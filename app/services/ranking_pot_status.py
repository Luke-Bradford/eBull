"""#2842 slice 7 — the ranking pot's page read: state, jobs, rebalances, looks, and every executed-book position with
its §6 ticket (spec §10 slice 7).

A reader over stored rows. It writes nothing, gates nothing and calls no broker: P&L is the stored daily mark
(``broker_account_position_marks``, observed-USD snapshots only) and the booked closes (``trade_events``), each
dated, never a live quote.

Deliberately NOT in ``ranking_pot_policy.POLICY_MODULES``: nothing here decides, and hashing a page reader would mint a
new strategy version for a label change. The figures that measure the trial come from the hashed
``ranking_pot_readout.readout``, which ``load_readout`` calls unchanged.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from app.services import ranking_pot_look as look
from app.services import ranking_pot_policy
from app.services import ranking_pot_rebalance as rb
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.ai_trial_pack import canonical_sha256
from app.services.ai_trial_status import JobFire, job_fire
from app.services.ranking_pot_exec import HELD, Status, classify
from app.services.ranking_pot_readout import readout
from app.workers.scheduler import JOB_RANKING_POT_EXECUTE, JOB_RANKING_POT_REBALANCE, JOB_RANKING_POT_STEP

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")
#: Lifecycles no longer held (closed, refused, expired, failed) shown beside the held ones, newest first.
RECENT_LIMIT: Final = 25
REBALANCE_LIMIT: Final = 6


@dataclass(frozen=True)
class Declaration:
    declaration_id: int
    frozen_at: datetime
    #: Latest ``to_state``; ``None`` = no event (the freeze writes one, so this is a defect, shown as such).
    state: str | None
    state_reason: str | None
    state_at: datetime | None
    pot_capital: Decimal | None


@dataclass(frozen=True)
class Rebalance:
    month: date
    target_session: date
    outcome: str
    refusal: str | None
    fired_at: datetime
    #: The executed book's header for a decided month (``None`` = not decided, or decided before the executed book).
    executed_state: str | None
    entries_allowed: bool | None
    v1_active: bool | None


@dataclass(frozen=True)
class Look:
    look_months: int
    endpoint_session: date
    kind: str
    verdict: str
    harm: bool
    reasons: tuple[str, ...]
    computed_at: datetime


@dataclass(frozen=True)
class Position:
    lifecycle_id: int
    slot: int
    instrument_id: int
    symbol: str | None
    status: Status
    trade_status: str | None
    target_session: date
    funding_reason: str | None
    exit_session: date | None
    exit_reason: str | None
    amount: Decimal | None
    ask: Decimal | None
    quote_at: datetime | None
    sent_stop_loss: Decimal | None
    sent_take_profit: Decimal | None
    #: The latest change-only observation of the broker's held levels (sql/453); ``None`` = never observed.
    held_observed_at: datetime | None
    held_stop_loss: Decimal | None
    held_take_profit: Decimal | None
    held_no_stop_loss: bool | None
    held_no_take_profit: bool | None
    #: Σ ``realized_pnl_usd`` of the trade's booked closes; ``None`` = no close, or one without a figure.
    realized_pnl_usd: Decimal | None
    #: Σ the latest stored mark's ``unrealized_pnl`` over the trade's active positions, and that mark's date;
    #: ``None`` = no active position, or one with no mark.
    unrealized_pnl_usd: Decimal | None
    marked_on: date | None
    ticket: dict[str, Any]
    #: The stored ticket hashes to its ``ticket_sha256``; ``False`` is shown, never hidden.
    ticket_verified: bool


@dataclass(frozen=True)
class StepState:
    latest_session: date | None
    refusal_session: date | None
    refusal_reason: str | None
    refusal_at: datetime | None


@dataclass(frozen=True)
class PotStatus:
    strategy_id: str
    strategy_version: str
    build_complete: bool
    declaration: Declaration | None
    jobs: tuple[JobFire, ...]
    rebalances: tuple[Rebalance, ...]
    step: StepState | None
    looks: tuple[Look, ...]
    held: tuple[Position, ...]
    recent: tuple[Position, ...]


_DECLARATION_SQL: Final = """
    SELECT e.reason, e.at, a.pot_capital
      FROM ranking_pot_declarations d
      LEFT JOIN LATERAL (
          SELECT reason, at FROM ranking_pot_state_events
           WHERE declaration_id = d.declaration_id ORDER BY event_id DESC LIMIT 1
      ) e ON TRUE
      LEFT JOIN ranking_pot_activations a ON a.declaration_id = d.declaration_id
     WHERE d.declaration_id = %s
"""

_REBALANCES_SQL: Final = """
    SELECT a.month, a.target_session, a.outcome, a.refusal, a.fired_at,
           h.state AS executed_state, h.entries_allowed, h.v1_active
      FROM ranking_pot_rebalance_attempts a
      LEFT JOIN ranking_pot_exec_rebalances h ON h.attempt_id = a.attempt_id
     WHERE a.declaration_id = %s
     ORDER BY a.attempt_id DESC
     LIMIT %s
"""

_LOOKS_SQL: Final = """
    SELECT look_months, endpoint_session, kind, verdict, harm, reasons, computed_at
      FROM ranking_pot_looks WHERE declaration_id = %s ORDER BY look_id
"""

_STEP_SQL: Final = """
    SELECT (SELECT max(session) FROM ranking_pot_steps WHERE declaration_id = %(d)s),
           r.session, r.reason, r.fired_at
      FROM (SELECT 1) one
      LEFT JOIN LATERAL (
          SELECT session, reason, fired_at FROM ranking_pot_step_refusals
           WHERE declaration_id = %(d)s ORDER BY refusal_id DESC LIMIT 1
      ) r ON TRUE
"""

_POSITIONS_SQL: Final = """
    SELECT l.lifecycle_id, l.slot, l.instrument_id, i.symbol, l.ticket, l.ticket_sha256, a.target_session,
           fd.verdict AS funding_verdict, fd.reason_code AS funding_reason,
           t.strategy_trade_id, t.status AS trade_status,
           st.exit_session, st.reason AS exit_reason,
           s.amount, s.ask, s.quote_at, s.stop_loss_rate, s.take_profit_rate,
           lv.observed_at AS held_observed_at, lv.stop_loss_rate AS held_stop_loss,
           lv.take_profit_rate AS held_take_profit, lv.is_no_stop_loss, lv.is_no_take_profit
      FROM ranking_pot_exec_lifecycles l
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
      LEFT JOIN instruments i ON i.instrument_id = l.instrument_id
      LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
      LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
      LEFT JOIN ranking_pot_exec_exit_stamps st ON st.lifecycle_id = l.lifecycle_id
      LEFT JOIN ranking_pot_exec_submissions s ON s.lifecycle_id = l.lifecycle_id
      LEFT JOIN LATERAL (
          SELECT observed_at, stop_loss_rate, take_profit_rate, is_no_stop_loss, is_no_take_profit
            FROM ranking_pot_exec_level_observations o
           WHERE o.strategy_trade_id = t.strategy_trade_id
           ORDER BY o.observation_id DESC LIMIT 1
      ) lv ON TRUE
     WHERE l.declaration_id = %s
     ORDER BY l.lifecycle_id DESC
"""

#: One row per (trade, position); an ownership row per claim repeats a position, so the position is de-duplicated
#: and ``active`` is any of its rows (``ranking_pot_loss.read_pot_exposures``'s rule).
_OWNED_SQL: Final = """
    SELECT strategy_trade_id, broker_position_id, bool_or(status = 'active')
      FROM strategy_position_ownership
     WHERE strategy_trade_id = ANY(%s)
     GROUP BY strategy_trade_id, broker_position_id
"""

_CLOSES_SQL: Final = """
    SELECT position_id, realized_pnl_usd FROM trade_events
     WHERE event_kind = 'close' AND position_id = ANY(%s)
"""

#: Only marks from a snapshot whose account currency was OBSERVED as USD (sql/341: NULL = assumed), the executed
#: readout's ``not_usd`` rule.
_MARKS_SQL: Final = """
    SELECT DISTINCT ON (m.position_id) m.position_id, m.snapshot_date, m.unrealized_pnl
      FROM broker_account_position_marks m
      JOIN broker_account_equity_snapshots e
        ON e.environment = m.environment AND e.snapshot_date = m.snapshot_date
     WHERE m.environment = 'demo' AND m.position_id = ANY(%s) AND e.account_currency_id = ANY(%s)
     ORDER BY m.position_id, m.snapshot_date DESC
"""

_USD_CURRENCY_IDS: Final = sorted(k for k, v in DOCUMENTED_ACCOUNT_CURRENCIES.items() if v == "USD")


def _pnl(conn: Conn, trade_ids: list[int]) -> dict[int, tuple[Decimal | None, Decimal | None, date | None]]:
    """Per trade: (realized, unrealized, marked_on). A missing figure makes its sum ``None``, never a partial."""
    if not trade_ids:
        return {}
    owned = conn.execute(_OWNED_SQL, (trade_ids,)).fetchall()
    pids = sorted({int(r[1]) for r in owned})
    closes: dict[int, list[Decimal | None]] = defaultdict(list)
    for pid, realized in conn.execute(_CLOSES_SQL, (pids,)).fetchall():
        closes[int(pid)].append(realized)
    marks = {int(r[0]): (r[1], r[2]) for r in conn.execute(_MARKS_SQL, (pids, _USD_CURRENCY_IDS)).fetchall()}
    by_trade: dict[int, list[tuple[int, bool]]] = defaultdict(list)
    for trade, pid, active in owned:
        by_trade[int(trade)].append((int(pid), bool(active)))
    out: dict[int, tuple[Decimal | None, Decimal | None, date | None]] = {}
    for trade, positions in by_trade.items():
        booked = [v for pid, _ in positions for v in closes.get(pid, [])]
        known = [Decimal(v) for v in booked if v is not None]
        realized = sum(known, Decimal(0)) if booked and len(known) == len(booked) else None
        active = [pid for pid, is_active in positions if is_active]
        unrealized: Decimal | None = None
        marked_on: date | None = None
        if active and all(pid in marks and marks[pid][1] is not None for pid in active):
            unrealized = sum((Decimal(marks[pid][1]) for pid in active), Decimal(0))
            marked_on = min(marks[pid][0] for pid in active)
        out[trade] = (realized, unrealized, marked_on)
    return out


def _positions(conn: Conn, declaration_id: int, ny_today: date) -> tuple[list[Position], list[Position]]:
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_POSITIONS_SQL, (declaration_id,)).fetchall()
    pnl = _pnl(conn, [int(r["strategy_trade_id"]) for r in rows if r["strategy_trade_id"] is not None])
    held: list[Position] = []
    recent: list[Position] = []
    for r in rows:
        status = classify(
            funding_verdict=r["funding_verdict"],
            trade_status=r["trade_status"],
            target_session=r["target_session"],
            ny_today=ny_today,
        )
        realized, unrealized, marked_on = (
            pnl.get(int(r["strategy_trade_id"]), (None, None, None))
            if r["strategy_trade_id"] is not None
            else (None, None, None)
        )
        position = Position(
            lifecycle_id=int(r["lifecycle_id"]),
            slot=int(r["slot"]),
            instrument_id=int(r["instrument_id"]),
            symbol=r["symbol"],
            status=status,
            trade_status=r["trade_status"],
            target_session=r["target_session"],
            funding_reason=r["funding_reason"],
            exit_session=r["exit_session"],
            exit_reason=r["exit_reason"],
            amount=r["amount"],
            ask=r["ask"],
            quote_at=r["quote_at"],
            sent_stop_loss=r["stop_loss_rate"],
            sent_take_profit=r["take_profit_rate"],
            held_observed_at=r["held_observed_at"],
            held_stop_loss=r["held_stop_loss"],
            held_take_profit=r["held_take_profit"],
            held_no_stop_loss=r["is_no_stop_loss"],
            held_no_take_profit=r["is_no_take_profit"],
            realized_pnl_usd=realized,
            unrealized_pnl_usd=unrealized,
            marked_on=marked_on,
            ticket=dict(r["ticket"]),
            ticket_verified=canonical_sha256(r["ticket"]) == r["ticket_sha256"],
        )
        if status in HELD:
            held.append(position)
        elif len(recent) < RECENT_LIMIT:
            recent.append(position)
    held.sort(key=lambda p: p.slot)
    return held, recent


def load_status(conn: Conn, *, now: datetime | None = None) -> PotStatus:
    observed = now or datetime.now(UTC)
    jobs = tuple(
        job_fire(conn, name, observed)
        for name in (JOB_RANKING_POT_REBALANCE, JOB_RANKING_POT_STEP, JOB_RANKING_POT_EXECUTE)
    )
    base = {
        "strategy_id": rb.STRATEGY_ID,
        "strategy_version": ranking_pot_policy.STRATEGY_VERSION,
        "build_complete": ranking_pot_policy.BUILD_COMPLETE,
        "jobs": jobs,
    }
    decl = rb.load_declaration(conn)
    if decl is None:
        return PotStatus(**base, declaration=None, rebalances=(), step=None, looks=(), held=(), recent=())
    d = decl.declaration_id
    head = conn.execute(_DECLARATION_SQL, (d,)).fetchone()
    assert head is not None  # load_declaration just read the row
    with conn.cursor(row_factory=dict_row) as cur:
        rebalances = tuple(Rebalance(**r) for r in cur.execute(_REBALANCES_SQL, (d, REBALANCE_LIMIT)).fetchall())
        looks = tuple(
            Look(**(r | {"reasons": tuple(r["reasons"] or ())})) for r in cur.execute(_LOOKS_SQL, (d,)).fetchall()
        )
    step = conn.execute(_STEP_SQL, {"d": d}).fetchone()
    assert step is not None  # one row by construction
    held, recent = _positions(conn, d, observed.astimezone(_NEW_YORK).date())
    return PotStatus(
        **base,
        declaration=Declaration(
            declaration_id=d,
            frozen_at=decl.frozen_at,
            state=decl.state,
            state_reason=head[0],
            state_at=head[1],
            pot_capital=head[2],
        ),
        rebalances=rebalances,
        step=StepState(*step),
        looks=looks,
        held=tuple(held),
        recent=tuple(recent),
    )


@dataclass(frozen=True)
class ReadoutView:
    declaration_id: int | None
    endpoint: date | None
    #: ``None`` with ``reason`` when there is nothing to read, or the stored rows violate an invariant.
    readout: dict[str, Any] | None
    reason: str | None


def load_readout(conn: Conn) -> ReadoutView:
    """The §9.4 readout at the latest stepped session (an interim view; a look's own figures are its stored row).

    The readout raises on a malformed row or a violated invariant (spec §9.4: nothing partial); here that becomes
    ``reason = "invariant_violation"`` with the error in the server log, so the page says the figures are withheld
    rather than failing as a whole."""
    decl = rb.load_declaration(conn)
    if decl is None:
        conn.rollback()
        return ReadoutView(None, None, None, "not_declared")
    t0 = look.first_target_session(conn, decl.declaration_id)
    row = conn.execute(
        "SELECT max(session) FROM ranking_pot_steps WHERE declaration_id = %s", (decl.declaration_id,)
    ).fetchone()
    endpoint = None if row is None else row[0]
    conn.rollback()  # readout opens its own REPEATABLE READ transaction on an idle connection
    if t0 is None or endpoint is None or endpoint < t0:
        return ReadoutView(decl.declaration_id, None, None, "not_stepped")
    try:
        doc = readout(conn, decl, endpoint)
    except rb.SnapshotIntegrityError, ValueError:
        logger.exception(
            "ranking pot readout: declaration %s at %s violates an invariant", decl.declaration_id, endpoint
        )
        return ReadoutView(decl.declaration_id, endpoint, None, "invariant_violation")
    return ReadoutView(decl.declaration_id, endpoint, doc, None)


__all__ = [
    "RECENT_LIMIT",
    "REBALANCE_LIMIT",
    "Declaration",
    "Look",
    "PotStatus",
    "Position",
    "ReadoutView",
    "Rebalance",
    "StepState",
    "load_readout",
    "load_status",
]
