"""#3592 slice 4c — ranking-pot-v2's page read: state, jobs, rebalances, the stored looks (v2's verdict, and whether
an invalidation cites them), and the shadow book's holdings with §5's per-entrant reasons (spec
``2026-10-03-3592-ranking-pot-v2.md`` §5 "Explainability", §7, §9 slice 4).

A reader over stored rows. It writes nothing, gates nothing and calls no broker; v2 has no executed book (sql/463), so
there are no positions, tickets or P&L marks — the holdings are the shadow book's own checkpoint (book 0).

**Outside v2's hash, deliberately** (as ``ranking_pot_readout_v2`` and the freeze): the name is not a
``ranking_pot_v2*`` root and no root imports it. It composes v1's page reader (``ranking_pot_status``), which is
outside v1's hash for the same reason; hashing a page reader would mint a new strategy version for a label change.
v1's ``page_declaration`` reads v1's id only, so neither page can pick up the other's declaration.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.services import ranking_pot_look as look
from app.services import ranking_pot_readout_v2 as ro2
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_status as s1
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_policy as policy
from app.services import ranking_pot_v2_step as st
from app.services.ai_trial_pack import canonical_sha256
from app.services.ai_trial_status import JobFire, job_fire
from app.services.ranking_pot_v2_declaration import load_declaration
from app.workers.scheduler import JOB_RANKING_POT_V2_REBALANCE, JOB_RANKING_POT_V2_STEP

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]


@dataclass(frozen=True)
class Look:
    look_id: int
    look_months: int
    endpoint: date
    #: ``detail.v2_verdict`` only (§7: readers of a v2 declaration never headline v1's verdict).
    v2_verdict: str
    harm: bool
    reasons: tuple[str, ...]
    #: Condition 6 (v1-reference) and 7 (turnover): ``None`` = unevaluable.
    reference_condition: bool | None
    turnover_condition: bool | None
    #: The invalidation row citing this look, if any; an invalidated look is shown as invalidated.
    invalidated_by: int | None
    invalidated_note: str | None


@dataclass(frozen=True)
class Holding:
    """A name the shadow book holds (``held``) or will enter at its next session (``pending``)."""

    instrument_id: int
    symbol: str | None
    state: str
    #: ``None`` for a pending entry (a slot is taken at the fill).
    slot: int | None
    entry_session: date
    entry_fill: Decimal | None
    invested: Decimal | None
    value: Decimal | None
    last_close: Decimal | None
    last_close_session: date | None
    stop_loss: Decimal | None
    take_profit: Decimal | None
    #: §5's per-entry reasons from the decision that entered it (``decision.v2_entries``); ``None`` = not stored.
    reasons: Mapping[str, Any] | None


@dataclass(frozen=True)
class PotStatusV2:
    strategy_id: str
    strategy_version: str
    build_complete: bool
    declaration: s1.Declaration | None
    jobs: tuple[JobFire, ...]
    rebalances: tuple[s1.Rebalance, ...]
    step: s1.StepState | None
    #: The shadow book's NAV (starts at 1) at ``step.latest_session``; ``None`` before the first step.
    shadow_nav: Decimal | None
    #: Empty with ``looks_withheld`` when a stored look's ``detail.v2`` fails to decode (the server log has it).
    looks: tuple[Look, ...]
    looks_withheld: bool
    #: Empty with ``holdings_withheld`` when the shadow's checkpoint or its stored reasons fail to decode.
    holdings: tuple[Holding, ...]
    holdings_withheld: bool


_LATEST_COMPLETED_SQL: Final = """
    SELECT d.declaration_id, d.doc, d.doc_sha256, d.frozen_at
      FROM ranking_pot_declarations d
     WHERE d.strategy_id = %s
       AND (SELECT e.to_state FROM ranking_pot_state_events e
             WHERE e.declaration_id = d.declaration_id ORDER BY e.event_id DESC LIMIT 1) = 'completed'
     ORDER BY d.declaration_id DESC
     LIMIT 1
"""

_ENTRIES_SQL: Final = """
    SELECT session, shadow -> 'decision' -> 'v2_entries'
      FROM ranking_pot_steps
     WHERE declaration_id = %s AND applied_attempt_id IS NOT NULL
     ORDER BY session
"""

#: The checkpoint is at the last stepped session, so the NAV is that session's step row (any step, not only an
#: applied rebalance's): both describe the book as it stands. The reasons come from decisions, hence applied rows only.
_SHADOW_SQL: Final = """
    SELECT c.state, (SELECT s.shadow ->> 'nav' FROM ranking_pot_steps s
                      WHERE s.declaration_id = c.declaration_id ORDER BY s.session DESC LIMIT 1)
      FROM ranking_pot_book_checkpoints c
     WHERE c.declaration_id = %s AND c.book = %s
"""


def page_declaration(conn: Conn) -> rb.PotDeclaration | None:
    """v2's live declaration, else its newest ``completed`` one (a finished seat keeps its page), as v1's."""
    live = load_declaration(conn)
    if live is not None:
        return live
    row = conn.execute(_LATEST_COMPLETED_SQL, (v2.STRATEGY_ID,)).fetchone()
    if row is None:
        return None
    if canonical_sha256(row[1]) != row[2]:
        raise rb.SnapshotIntegrityError(f"declaration {row[0]} does not hash to its doc_sha256")
    return rb.PotDeclaration(int(row[0]), row[1], str(row[2]), row[3], "completed")


def holdings(
    book: st.Book,
    entries: Sequence[tuple[date, Sequence[Mapping[str, Any]] | None]],
    symbols: Mapping[int, str],
) -> list[Holding]:
    """Book 0's positions (by slot) then its pending entries, each with the reasons of the latest applied decision
    at or before its entry session that entered it. ``entries`` is ascending by session; ``None`` = a decision
    stored without reasons."""

    def reasons_for(iid: int, entered_at: date) -> Mapping[str, Any] | None:
        found = None
        for session, rows in entries:
            if session > entered_at:
                break
            for r in rows or ():
                if int(r["instrument_id"]) == iid:
                    found = r
        return found

    out = [
        Holding(
            instrument_id=p.instrument_id,
            symbol=symbols.get(p.instrument_id),
            state="held",
            slot=p.slot,
            entry_session=p.entry_session,
            entry_fill=p.entry_fill,
            invested=p.invested,
            value=p.value,
            last_close=p.last_close,
            last_close_session=p.last_close_session,
            stop_loss=p.stop_loss,
            take_profit=p.take_profit,
            reasons=reasons_for(p.instrument_id, p.entry_session),
        )
        for p in sorted(book.state.positions, key=lambda p: p.slot)
    ]
    out += [
        Holding(
            instrument_id=e.instrument_id,
            symbol=symbols.get(e.instrument_id),
            state="pending",
            slot=None,
            entry_session=e.session,
            entry_fill=None,
            invested=None,
            value=None,
            last_close=None,
            last_close_session=None,
            stop_loss=None,
            take_profit=None,
            reasons=reasons_for(e.instrument_id, e.session),
        )
        for e in sorted(book.state.pending, key=lambda e: e.instrument_id)
    ]
    return out


def _looks(conn: Conn, declaration_id: int) -> tuple[tuple[Look, ...], bool]:
    try:
        rows = ro2.looks(conn, declaration_id)
    except rb.SnapshotIntegrityError:
        logger.exception("ranking pot v2: declaration %s has a look whose detail.v2 does not decode", declaration_id)
        return (), True
    return tuple(
        Look(
            look_id=int(r["look_id"]),
            look_months=int(r["look_months"]),
            endpoint=date.fromisoformat(r["endpoint"]),
            v2_verdict=r["v2_verdict"],
            harm=bool(r["harm"]),
            reasons=tuple(r["reasons"]),
            reference_condition=r["conditions"]["6_reference"],
            turnover_condition=r["conditions"]["7_turnover"],
            invalidated_by=None if r["invalidated"] is None else int(r["invalidated"]["look_id"]),
            invalidated_note=None if r["invalidated"] is None else r["invalidated"]["note"],
        )
        for r in rows
    ), False


def _holdings(conn: Conn, declaration_id: int) -> tuple[list[Holding], Decimal | None, bool]:
    """(holdings, shadow NAV, withheld). A checkpoint or reason that does not decode withholds the holdings (logged),
    as a malformed look withholds the looks, rather than failing the whole page."""
    row = conn.execute(_SHADOW_SQL, (declaration_id, st.SHADOW_BOOK)).fetchone()
    if row is None:  # not stepped yet
        return [], None, False
    nav = None if row[1] is None else Decimal(row[1])
    try:
        book = st.decode_book(row[0])
        entries = [(r[0], r[1]) for r in conn.execute(_ENTRIES_SQL, (declaration_id,)).fetchall()]
        ids = st.book_names(book.state)
        symbols = {
            int(r[0]): str(r[1])
            for r in conn.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%s)", (ids,)
            ).fetchall()
        }
        return holdings(book, entries, symbols), nav, False
    except KeyError, IndexError, TypeError, ValueError, ArithmeticError:
        logger.exception("ranking pot v2: declaration %s's shadow checkpoint does not decode", declaration_id)
        return [], nav, True


def load_status(conn: Conn, *, now: datetime | None = None) -> PotStatusV2:
    observed = now or datetime.now(UTC)
    base: dict[str, Any] = {
        "strategy_id": v2.STRATEGY_ID,
        "strategy_version": policy.STRATEGY_VERSION,
        "build_complete": policy.BUILD_COMPLETE,
        "jobs": tuple(
            job_fire(conn, name, observed) for name in (JOB_RANKING_POT_V2_REBALANCE, JOB_RANKING_POT_V2_STEP)
        ),
    }
    decl = page_declaration(conn)
    if decl is None:
        return PotStatusV2(
            **base,
            declaration=None,
            rebalances=(),
            step=None,
            shadow_nav=None,
            looks=(),
            looks_withheld=False,
            holdings=(),
            holdings_withheld=False,
        )
    d = decl.declaration_id
    head = conn.execute(s1._DECLARATION_SQL, (d,)).fetchone()
    assert head is not None  # page_declaration just read the row
    with conn.cursor(row_factory=dict_row) as cur:
        rebalances = tuple(
            s1.Rebalance(**r) for r in cur.execute(s1._REBALANCES_SQL, (d, s1.REBALANCE_LIMIT)).fetchall()
        )
    step = conn.execute(s1._STEP_SQL, {"d": d}).fetchone()
    assert step is not None  # one row by construction
    looks, withheld = _looks(conn, d)
    held, nav, held_withheld = _holdings(conn, d)
    return PotStatusV2(
        **base,
        declaration=s1.Declaration(
            declaration_id=d,
            frozen_at=decl.frozen_at,
            state=decl.state,
            state_reason=head[0],
            state_at=head[1],
            pot_capital=None,  # never activated (sql/463)
        ),
        rebalances=rebalances,
        step=s1.StepState(*step),
        shadow_nav=nav,
        looks=looks,
        looks_withheld=withheld,
        holdings=tuple(held),
        holdings_withheld=held_withheld,
    )


def load_readout(conn: Conn) -> s1.ReadoutView:
    """§7's readout at the latest stepped session (interim; a look's own figures are its stored row). An invariant
    violation withholds the figures (``reason = "invariant_violation"``, error in the server log), as v1's page."""
    decl = page_declaration(conn)
    if decl is None:
        conn.rollback()
        return s1.ReadoutView(None, None, None, "not_declared")
    t0 = look.first_target_session(conn, decl.declaration_id)
    row = conn.execute(
        "SELECT max(session) FROM ranking_pot_steps WHERE declaration_id = %s", (decl.declaration_id,)
    ).fetchone()
    endpoint = None if row is None else row[0]
    conn.rollback()  # readout opens its own REPEATABLE READ transaction on an idle connection
    if t0 is None or endpoint is None or endpoint < t0:
        return s1.ReadoutView(decl.declaration_id, None, None, "not_stepped")
    try:
        doc = ro2.readout(conn, decl, endpoint)
    except rb.SnapshotIntegrityError, ValueError:
        logger.exception(
            "ranking pot v2 readout: declaration %s at %s violates an invariant", decl.declaration_id, endpoint
        )
        conn.rollback()
        return s1.ReadoutView(decl.declaration_id, endpoint, None, "invariant_violation")
    return s1.ReadoutView(decl.declaration_id, endpoint, doc, None)


__all__ = ["Holding", "Look", "PotStatusV2", "holdings", "load_readout", "load_status", "page_declaration"]
