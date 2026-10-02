"""Ranking-pot-v1's executed book at a rebalance (#2842 slice 5a).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §4, "The executed book's decision". In
``executing`` or a halt, the rebalance job calls ``record_executed`` inside the transaction that inserted the
``decided`` attempt (``sql/449`` refuses any other transaction). It classifies every lifecycle from its signal's
funding decision and trade, applies ``ranking_pot.decide`` with the real order, assigns slots, and writes the header,
the §5.2 rows, the exit stamps, and one fired ``strategy_signals`` row, lifecycle and §6 ticket per entry. No broker
I/O: the loader and executor (5b) and the exits (5c) consume these rows.

Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.services import ranking_pot as pot
from app.services import ranking_pot_rebalance as rb
from app.services.ai_trial_pack import canonical_json, canonical_sha256
from app.services.scoring import family_weights

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")

Status = Literal["entry_pending", "expired", "refused", "open", "failed", "closed"]
#: Statuses that hold a slot (§5.1's held set; r3-99).
HELD: Final[frozenset[Status]] = frozenset({"entry_pending", "open"})
EXEC_STATES: Final = ("executing", "halted_loss", "halted_operator")
#: The book name on an executed ticket (§6).
EXECUTED_BOOK: Final = "executed"
#: ``strategy_signals.universe``: S₀ is the last pre-freeze scoring run's names, a survivor set (as v1's pack).
SIGNAL_UNIVERSE: Final = "survivor_only"

_ENTRY_PENDING_TRADE: Final = frozenset({"planned", "submitted", "reconcile_required"})
_OPEN_TRADE: Final = frozenset({"open", "closing"})


class ExecutedBookError(RuntimeError):
    """The executed book's stored state contradicts an invariant the database cannot check."""


def classify(
    *,
    funding_verdict: str | None,
    trade_status: str | None,
    target_session: Any,
    ny_today: Any,
) -> Status:
    """A lifecycle's status from its signal's funding decision and trade (spec §4, "Classification").

    No funding decision → ``entry_pending`` while the New York date is not past the target session, else
    ``expired`` (the loader refuses any other session, so nothing can authorise it later). An ``allocated`` decision
    with no trade yet holds its slot (fail closed). An unknown status raises.
    """
    if funding_verdict is None:
        if trade_status is not None:
            raise ExecutedBookError("a trade without a funding decision")
        return "entry_pending" if ny_today <= target_session else "expired"
    if funding_verdict == "rejected":
        return "refused"
    if funding_verdict != "allocated":
        raise ExecutedBookError(f"unknown funding verdict {funding_verdict!r}")
    if trade_status is None or trade_status in _ENTRY_PENDING_TRADE:
        return "entry_pending"
    if trade_status in _OPEN_TRADE:
        return "open"
    if trade_status == "failed":
        return "failed"
    if trade_status == "closed":
        return "closed"
    raise ExecutedBookError(f"unknown trade status {trade_status!r}")


@dataclass(frozen=True)
class Lifecycle:
    lifecycle_id: int
    instrument_id: int
    slot: int
    status: Status
    #: The raw trade status (``None`` = no trade): 5c reads exposure from it, never from ``status``.
    trade_status: str | None
    exit_stamped: bool


@dataclass(frozen=True)
class Entry:
    instrument_id: int
    slot: int
    replaces_lifecycle_id: int | None


@dataclass(frozen=True)
class ExecutedPlan:
    decision: pot.BookDecision
    held: tuple[Lifecycle, ...]
    closed_ids: frozenset[int]
    recently_exited: frozenset[int]
    #: (lifecycle, reason) stamped for exit at this rebalance's target session.
    stamps: tuple[tuple[Lifecycle, str], ...]
    #: In entry order.
    entries: tuple[Entry, ...]


def plan_executed(
    universes: pot.Universes,
    lifecycles: Sequence[Lifecycle],
    *,
    previous_closed: frozenset[int] | None,
    entries_allowed: bool,
    n: int,
) -> ExecutedPlan:
    """§5.1 for the executed book (pure). ``previous_closed`` is the previous header's ``closed`` set (``None`` when
    no header exists). Held lifecycles must have distinct names and slots within 1..N."""
    held = tuple(lc for lc in lifecycles if lc.status in HELD)
    if len({lc.instrument_id for lc in held}) != len(held):
        raise ExecutedBookError("a name is held by two lifecycles")
    slots = [lc.slot for lc in held]
    if len(set(slots)) != len(slots) or not all(1 <= s <= n for s in slots):
        raise ExecutedBookError(f"held slots are not distinct within 1..{n}: {sorted(slots)}")
    closed = {lc.lifecycle_id: lc.instrument_id for lc in lifecycles if lc.status == "closed"}
    recently_exited = frozenset(
        iid for lc_id, iid in closed.items() if previous_closed is None or lc_id not in previous_closed
    )
    decision = pot.decide(
        universes,
        pot.real_order(universes),
        [
            pot.Holding(lc.instrument_id, "open" if lc.status == "open" else "entry_pending", lc.exit_stamped)
            for lc in held
        ],
        recently_exited=recently_exited,
        entries_allowed=entries_allowed,
        n=n,
    )
    by_name = {lc.instrument_id: lc for lc in held}
    stamps = tuple((by_name[row.instrument_id], row.reason or "") for row in decision.rows if row.action == "exit")
    free = sorted(set(range(1, n + 1)) - set(slots))
    releasing = sorted((lc.slot, lc.lifecycle_id) for lc, _ in stamps)
    takers: Sequence[tuple[int, int | None]] = [*((s, None) for s in free), *releasing]
    if len(decision.entries) > len(takers):
        raise ExecutedBookError(f"{len(decision.entries)} entries for {len(takers)} slots")
    entries = tuple(Entry(iid, slot, replaces) for iid, (slot, replaces) in zip(decision.entries, takers, strict=False))
    return ExecutedPlan(decision, held, frozenset(closed), recently_exited, stamps, entries)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
_LIFECYCLES_SQL: Final = """
    SELECT l.lifecycle_id, l.instrument_id, l.slot, a.target_session,
           fd.verdict AS funding_verdict, t.status AS trade_status,
           (s.lifecycle_id IS NOT NULL) AS exit_stamped
      FROM ranking_pot_exec_lifecycles l
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
      LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
      LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
      LEFT JOIN ranking_pot_exec_exit_stamps s ON s.lifecycle_id = l.lifecycle_id
     WHERE l.declaration_id = %(d)s
     ORDER BY l.lifecycle_id
"""

#: §7.1's continuous guard, read at the rebalance (recorded; 5b re-checks it under locks).
V1_ACTIVE_SQL: Final = """
    SELECT EXISTS (
        SELECT 1 FROM ai_trial_declarations d
         WHERE (SELECT e.to_state FROM ai_trial_state_events e
                 WHERE e.declaration_id = d.declaration_id ORDER BY e.event_id DESC LIMIT 1) = 'active'
    )
"""


def read_lifecycles(conn: Conn, declaration_id: int, *, ny_today: Any) -> list[Lifecycle]:
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_LIFECYCLES_SQL, {"d": declaration_id}).fetchall()
    return [
        Lifecycle(
            lifecycle_id=int(r["lifecycle_id"]),
            instrument_id=int(r["instrument_id"]),
            slot=int(r["slot"]),
            status=classify(
                funding_verdict=r["funding_verdict"],
                trade_status=r["trade_status"],
                target_session=r["target_session"],
                ny_today=ny_today,
            ),
            trade_status=r["trade_status"],
            exit_stamped=bool(r["exit_stamped"]),
        )
        for r in rows
    ]


def previous_closed(conn: Conn, declaration_id: int) -> frozenset[int] | None:
    row = conn.execute(
        "SELECT closed_lifecycle_ids FROM ranking_pot_exec_rebalances WHERE declaration_id = %s "
        "ORDER BY attempt_id DESC LIMIT 1",
        (declaration_id,),
    ).fetchone()
    return None if row is None else frozenset(int(i) for i in row[0])


def _ticket(
    inputs: rb.SnapshotInputs,
    universes: pot.Universes,
    decl: rb.PotDeclaration,
    row: pot.DecisionRow,
    facts: pot.NameFacts,
) -> dict[str, Any]:
    iid = row.instrument_id
    score = inputs.scores[iid]
    used = inputs.theses.get(iid)
    atr = universes.atr[iid]
    h = pot.half_spread(facts.bid, facts.ask)
    if h is None:
        raise ExecutedBookError(f"{iid}: an F_t name without a valid quote")
    levels = pot.planned_levels(facts.bar_rows[-1]["close"], atr)
    return pot.entry_ticket(
        book=EXECUTED_BOOK,
        declaration_id=decl.declaration_id,
        instrument_id=iid,
        symbol=facts.symbol,
        row=row,
        own_score=score,
        family_weights=family_weights(score.model_version),
        order_donor_id=iid,
        order_score=universes.own_score[iid],
        thesis=None
        if used is None
        else pot.ThesisRef(used.thesis_id, (inputs.as_of - used.created_at).days, used.model, used.prompt_version),
        levels=None if isinstance(levels, str) else levels,
        atr=atr,
        expected_half_spread=h,
    )


@dataclass(frozen=True)
class ExecutedResult:
    state: str
    entries_allowed: bool
    v1_active: bool
    entries: int
    exits: int
    held: int

    @property
    def note(self) -> str:
        return (
            f"executed book ({self.state}, entries {'allowed' if self.entries_allowed else 'withheld'}"
            f"{', v1 active' if self.v1_active else ''}): held {self.held}, exits {self.exits}, entries {self.entries}"
        )


def record_executed(
    conn: Conn,
    decl: rb.PotDeclaration,
    attempt_id: int,
    prepared: rb.Prepared,
    *,
    state: str,
) -> ExecutedResult:
    """§4 step 4 for the executed book, inside the decided attempt's transaction (``sql/449``)."""
    if state not in EXEC_STATES:
        raise ValueError(f"no executed-book decision in state {state}")
    rb._assert_rebalance_transaction(conn)
    inputs = rb.decode_snapshot(prepared.snapshot)
    universes = prepared.universes
    n = int(decl.doc["terms"]["n"])
    v1_row = conn.execute(V1_ACTIVE_SQL).fetchone()
    v1_active = bool(v1_row and v1_row[0])
    entries_allowed = state == "executing" and not v1_active
    ny_today = inputs.as_of.astimezone(_NEW_YORK).date()
    lifecycles = read_lifecycles(conn, decl.declaration_id, ny_today=ny_today)
    plan = plan_executed(
        universes,
        lifecycles,
        previous_closed=previous_closed(conn, decl.declaration_id),
        entries_allowed=entries_allowed,
        n=n,
    )

    conn.execute(
        "INSERT INTO ranking_pot_exec_rebalances (attempt_id, declaration_id, state, entries_allowed, v1_active, "
        " held, closed_lifecycle_ids, detail) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            attempt_id,
            decl.declaration_id,
            state,
            entries_allowed,
            v1_active,
            Jsonb([[lc.lifecycle_id, lc.instrument_id, lc.slot, lc.status, lc.trade_status] for lc in plan.held]),
            sorted(plan.closed_ids),
            Jsonb(
                {
                    "held": len(plan.held),
                    "entries": len(plan.entries),
                    "exits": len(plan.stamps),
                    "slots_unfilled": plan.decision.slots_unfilled,
                    "recently_exited": sorted(plan.recently_exited),
                }
            ),
        ),
    )
    held_of = {lc.instrument_id: lc.lifecycle_id for lc in plan.held}
    decision_sql = (
        "INSERT INTO ranking_pot_exec_decisions (attempt_id, instrument_id, action, reason, r_rank, f_rank, "
        "lifecycle_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    for row in plan.decision.rows:
        if row.action != "enter":
            lc_id = None if row.action == "not_selected" else held_of[row.instrument_id]
            conn.execute(
                decision_sql, (attempt_id, row.instrument_id, row.action, row.reason, row.r_rank, row.f_rank, lc_id)
            )
    for lc, reason in plan.stamps:
        conn.execute(
            "INSERT INTO ranking_pot_exec_exit_stamps (lifecycle_id, declaration_id, attempt_id, exit_session, reason) "
            "VALUES (%s, %s, %s, %s, %s)",
            (lc.lifecycle_id, decl.declaration_id, attempt_id, inputs.target_session, reason),
        )

    facts = {f.instrument_id: f for f in inputs.facts}
    rows = {row.instrument_id: row for row in plan.decision.rows}
    for entry in plan.entries:
        row = rows[entry.instrument_id]
        f = facts[entry.instrument_id]
        if not f.bar_dates or f.bar_dates[-1] != inputs.last_session:
            raise ExecutedBookError(f"{entry.instrument_id}: an entrant without a last-session bar")
        ticket = json.loads(canonical_json(_ticket(inputs, universes, decl, row, f)))
        signal = conn.execute(
            "INSERT INTO strategy_signals (strategy_id, strategy_version, instrument_id, signal_bar_date, "
            " signal_kind, verdict, fill_bar_date, fill_price, universe, input_rule_set_versions) "
            "VALUES (%s, %s, %s, %s, 'entry', 'fired', %s, %s, %s, %s) RETURNING signal_id",
            (
                pot.STRATEGY_ID,
                decl.doc["strategy_version"],
                entry.instrument_id,
                inputs.last_session,
                inputs.target_session,
                f.bar_rows[-1]["close"],
                SIGNAL_UNIVERSE,
                Jsonb({"ranking_pot_policy": inputs.policy_hash}),
            ),
        ).fetchone()
        assert signal is not None
        lifecycle = conn.execute(
            "INSERT INTO ranking_pot_exec_lifecycles (attempt_id, declaration_id, instrument_id, slot, "
            " replaces_lifecycle_id, signal_id, ticket, ticket_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "RETURNING lifecycle_id, ticket",
            (
                attempt_id,
                decl.declaration_id,
                entry.instrument_id,
                entry.slot,
                entry.replaces_lifecycle_id,
                signal[0],
                Jsonb(ticket),
                canonical_sha256(ticket),
            ),
        ).fetchone()
        assert lifecycle is not None
        if canonical_sha256(lifecycle[1]) != canonical_sha256(ticket):
            raise rb.SnapshotIntegrityError(f"{entry.instrument_id}: the stored ticket does not hash as written")
        conn.execute(
            decision_sql, (attempt_id, row.instrument_id, row.action, row.reason, row.r_rank, row.f_rank, lifecycle[0])
        )

    stored = conn.execute(
        "SELECT count(*) FROM ranking_pot_exec_decisions WHERE attempt_id = %s", (attempt_id,)
    ).fetchone()
    if stored is None or int(stored[0]) != len(plan.decision.rows):
        raise ExecutedBookError(f"attempt {attempt_id}: stored decision rows differ from the decision")
    return ExecutedResult(
        state=state,
        entries_allowed=entries_allowed,
        v1_active=v1_active,
        entries=len(plan.entries),
        exits=len(plan.stamps),
        held=len(plan.held),
    )


__all__ = [
    "EXEC_STATES",
    "HELD",
    "Entry",
    "ExecutedBookError",
    "ExecutedPlan",
    "ExecutedResult",
    "Lifecycle",
    "classify",
    "plan_executed",
    "previous_closed",
    "read_lifecycles",
    "record_executed",
]
