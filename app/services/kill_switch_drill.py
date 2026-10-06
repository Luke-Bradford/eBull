"""The recorded kill-switch drill and its estimated time-to-flat (#3614 item 4).

Spec: ``docs/specs/ops/2026-10-06-3614-kill-drill-time-to-flat.md``. Two modes, chosen
by the switch's own state:

- **sandbox** (switch off): the real ``activate_kill_switch`` runs inside one
  non-autocommit transaction that is ALWAYS rolled back, and the three broker-entry
  chokepoints are evaluated against it inside that transaction. No other session ever
  sees the switch on. The drill has no code path that commits a kill-switch change.
- **observe** (switch already on, e.g. during an attended operator toggle): nothing is
  written to the kill row; the chokepoints are evaluated against the committed state.

Each chokepoint is evaluated through its own loader and decision, never through the
wrapper that records refusals, so the drill writes no ``decision_audit`` or rejection
rows. It calls no broker method. It commits only its own ``kill_switch_drill_*`` rows.

Lock order: the core mandate key, then the core submission key, then the kill row.
That is the executor's order (``core_submission_lock``: allocator, mandate,
submission) restricted to the keys the drill needs, and ``activate_kill_switch``'s
(submission, then row) — so the drill cannot deadlock against either. The spec lists
the mandate lock after the activation; it is taken first here for that reason.
"""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, Literal

import psycopg
from psycopg.pq import TransactionStatus

from app.services.bar_capture_certificate import next_session_open_utc
from app.services.execution_guard import decide_submission_controls, load_kill_switch
from app.services.market_calendar import us_market_status
from app.services.market_session_support import venue_calendar_for
from app.services.ops_monitor import activate_kill_switch
from app.services.runtime_config import RuntimeConfigCorrupt, get_runtime_config
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS
from app.services.strategy_core_mandate import CORE_MANDATE_ADVISORY_LOCK, load_core_mandate
from app.services.strategy_core_preflight import preflight_core_submission
from app.services.strategy_core_submission_gate import CORE_SUBMISSION_ADVISORY_LOCK
from app.services.strategy_paper_executor import decide_trading_enabled, load_trading_enabled_state

logger = logging.getLogger(__name__)

#: Session advisory lock, held on the lock connection for a run's whole life.
DRILL_ADVISORY_LOCK: Final = (3614, 4)
DRILL_ACTOR: Final = "kill-drill"

#: eToro's documented order-write allowance, shared across every writer
#: (``.claude/skills/data-sources/etoro-api.md`` § Stable facts).
ORDER_WRITES_PER_MINUTE: Final = 20

#: The only operator flatten route under kill (``operator_close``) has no frontend caller.
OPERATOR_SURFACE: Final = "api_only"

_SET_TIMEOUTS: Final = (
    "SET LOCAL lock_timeout = '2s'",
    "SET LOCAL statement_timeout = '5s'",
    "SET LOCAL transaction_timeout = '30s'",
)

Chokepoint = Literal["C1", "C2", "C3"]
ChokepointOutcome = Literal["kill_refused", "other_refusal", "allowed", "error", "not_applicable"]
EntryVerdict = Literal["passed", "incomplete", "failed", "not_run"]
RunFailure = Literal["kill_switch_row_missing", "sandbox_committed", "exception"]

C2_KILL_CODE: Final = "kill_switch_active_or_missing"
C2_EARLY_CODES: Final = frozenset({"runtime_config_corrupt", "auto_trading_disabled"})
C3_KILL_CODE: Final = "core_kill_switch_active_or_missing"
C3_EARLY_CODES: Final = frozenset({"core_runtime_config_corrupt", "core_auto_trading_disabled"})

#: ``strategy_trades`` states that still hold engine authority to act at the broker.
OUTSTANDING_TRADE_STATES: Final = ("planned", "submitted", "closing", "reconcile_required")


class DrillRefused(RuntimeError):
    """The sandbox precondition failed; nothing was written to the kill row."""


@dataclass(frozen=True)
class ChokepointResult:
    chokepoint: Chokepoint
    outcome: ChokepointOutcome
    refusal_code: str | None
    failed_rules: tuple[str, ...]
    drill_read_kill_active: bool | None
    error_detail: str | None
    evaluated_at: datetime


# ---------------------------------------------------------------------------
# Pure classification
# ---------------------------------------------------------------------------


def classify_c1(failed_rules: Sequence[str]) -> tuple[ChokepointOutcome, str | None, str | None]:
    """(outcome, refusal_code, error_detail) from C1's failed rule names.

    C1 has no early return, so its kill rule is always reached. ``kill_switch_config_corrupt``
    is only ever classified while the drill holds the row it just read, so it means the
    loader lost a row the drill can see: an error, not a refusal.
    """
    if "kill_switch_config_corrupt" in failed_rules:
        return "error", "kill_switch_config_corrupt", "C1 read no kill_switch row the drill can see"
    if "kill_switch" in failed_rules:
        return "kill_refused", "kill_switch", None
    return "allowed", None, None


def classify_c2(code: str | None) -> tuple[ChokepointOutcome, str | None, str | None]:
    if code == C2_KILL_CODE:
        return "kill_refused", code, None
    if code in C2_EARLY_CODES:
        return "other_refusal", code, None
    if code is None:
        return "allowed", None, None
    return "error", code, f"unknown C2 refusal code {code!r}"


def classify_c3(admitted: bool, code: str | None) -> tuple[ChokepointOutcome, str | None, str | None]:
    """A refusal after C3's kill check (or an admission) means the kill check let it through."""
    if code == C3_KILL_CODE:
        return "kill_refused", code, None
    if code in C3_EARLY_CODES:
        return "other_refusal", code, None
    return "allowed", None if admitted else code, None


def entry_verdict(results: Sequence[ChokepointResult], *, failed_after_evaluation: bool) -> EntryVerdict:
    if not results:
        return "not_run"
    outcomes = {r.outcome for r in results}
    if failed_after_evaluation or outcomes & {"allowed", "error"}:
        return "failed"
    if "other_refusal" in outcomes:
        return "incomplete"
    return "passed"


# ---------------------------------------------------------------------------
# Chokepoint evaluation (inside the caller's open transaction)
# ---------------------------------------------------------------------------


def _read_kill_active(conn: psycopg.Connection[Any]) -> bool | None:
    row = conn.execute("SELECT is_active FROM kill_switch WHERE id = TRUE").fetchone()
    return None if row is None else bool(row[0])


def _evaluate(
    conn: psycopg.Connection[Any],
    chokepoint: Chokepoint,
    probe: Callable[[], tuple[ChokepointOutcome, str | None, str | None, tuple[str, ...]]],
    now_fn: Callable[[], datetime],
) -> ChokepointResult:
    """Run one probe inside a savepoint, so a raising probe cannot abort the others."""
    drill_read = _read_kill_active(conn)
    try:
        with conn.transaction():
            outcome, code, detail, rules = probe()
    except Exception as exc:  # noqa: BLE001 - recorded as the chokepoint's error outcome
        outcome, code, detail, rules = "error", None, f"{type(exc).__name__}: {exc}", ()
    return ChokepointResult(chokepoint, outcome, code, rules, drill_read, detail, now_fn())


def evaluate_chokepoints(
    conn: psycopg.Connection[Any],
    *,
    core_instrument_id: int | None,
    now_fn: Callable[[], datetime],
) -> list[ChokepointResult]:
    """C1-C3 against the kill state ``conn`` sees. Requires an open transaction; writes nothing.

    C3 needs the core submission and mandate advisory locks held by this backend
    (``preflight_core_submission`` asserts it); the drill takes both before calling this.
    """

    def c1() -> tuple[ChokepointOutcome, str | None, str | None, tuple[str, ...]]:
        ks_row = load_kill_switch(conn)
        try:
            runtime, corrupt = get_runtime_config(conn), False
        except RuntimeConfigCorrupt:
            runtime, corrupt = None, True
        failed = tuple(r.rule for r in decide_submission_controls(ks_row, runtime, corrupt) if not r.passed)
        return (*classify_c1(failed), failed)

    def c2() -> tuple[ChokepointOutcome, str | None, str | None, tuple[str, ...]]:
        try:
            code = decide_trading_enabled(load_trading_enabled_state(conn))
        except RuntimeConfigCorrupt:
            code = "runtime_config_corrupt"
        return (*classify_c2(code), ())

    def c3() -> tuple[ChokepointOutcome, str | None, str | None, tuple[str, ...]]:
        assert core_instrument_id is not None
        verdict = preflight_core_submission(
            conn, core_instrument_id=core_instrument_id, action="buy_core", now=now_fn()
        )
        return (*classify_c3(verdict.admitted, verdict.reason_code), ())

    results = [_evaluate(conn, "C1", c1, now_fn), _evaluate(conn, "C2", c2, now_fn)]
    if core_instrument_id is None:
        results.append(ChokepointResult("C3", "not_applicable", None, (), _read_kill_active(conn), None, now_fn()))
    else:
        results.append(_evaluate(conn, "C3", c3, now_fn))
    return results


# ---------------------------------------------------------------------------
# Time-to-flat estimate (pure)
# ---------------------------------------------------------------------------

_NYSE = venue_calendar_for(US_EQUITY_ASSET_CLASS)
assert _NYSE is not None


def _ny_civil(d: date, t: Any) -> datetime:
    assert _NYSE is not None
    return datetime.combine(d, t, tzinfo=_NYSE.tz).astimezone(UTC)


def _session_close_utc(d: date) -> datetime | None:
    assert _NYSE is not None
    status = us_market_status(d)
    if status == "closed":
        return None
    return _ny_civil(d, _NYSE.half_day_close if status == "half_day" else _NYSE.session_close)


def close_opportunity(t0: datetime) -> datetime | None:
    """The earliest scheduled US session instant at or after ``t0``; None = ``session_unknown``."""
    assert _NYSE is not None
    d = t0.astimezone(_NYSE.tz).date()
    open_at = _ny_civil(d, _NYSE.session_open)
    close_at = _session_close_utc(d)
    if close_at is not None and open_at <= t0 < close_at:
        return t0
    if close_at is not None and t0 < open_at:
        return open_at
    return next_session_open_utc(d)


def last_close_created_at(opportunity: datetime, n_closes: int) -> datetime | None:
    """When the last of ``n_closes`` is created, slotted 20 per minute inside sessions.

    Slot j starts j minutes after ``opportunity``; a slot at or after its session's
    close moves to the next session's open, and later slots follow it.
    """
    assert n_closes > 0
    assert _NYSE is not None
    slot_at = opportunity
    for j in range(math.ceil(n_closes / ORDER_WRITES_PER_MINUTE)):
        if j:
            slot_at += timedelta(minutes=1)
        d = slot_at.astimezone(_NYSE.tz).date()
        close_at = _session_close_utc(d)
        if close_at is None or slot_at >= close_at:
            moved = next_session_open_utc(d)
            if moved is None:
                return None
            slot_at = moved
    return slot_at


@dataclass(frozen=True)
class BookPosition:
    ownership_id: int
    broker_position_id: int | None
    instrument_id: int
    broker_row_updated_at: datetime | None
    stop_present: bool | None
    target_present: bool | None
    broker_environment: str | None
    us_listed: bool


@dataclass(frozen=True)
class TimeToFlat:
    seconds: float | None
    null_reason: str | None
    opportunity_at: datetime | None


def estimate_time_to_flat(
    t0: datetime,
    positions: Sequence[BookPosition],
    *,
    outstanding_authority: int,
    close_resolution_max_s: float | None,
) -> TimeToFlat:
    """``created_at(last close) + R - t0`` for an ``operator_close`` of every engine position.

    An ESTIMATE under stated assumptions (spec § Estimated time-to-flat): the full write
    allowance is free at the opportunity and nothing else writes; R is an observed
    maximum, not a bound. ⚠ ``broker_environment`` NULL is accepted as demo: the entry
    orders of today's book predate #3189's column, and every engine entry method is
    demo-only (``place_demo_strategy_order`` refuses non-demo credentials). Only an
    explicit ``real`` is ``unsupported_route``.
    """
    ownerships = {p.ownership_id for p in positions}
    if not ownerships and not outstanding_authority:
        return TimeToFlat(0.0, None, None)
    opportunity = close_opportunity(t0)
    if close_resolution_max_s is None:
        return TimeToFlat(None, "no_close_observed", opportunity)
    if opportunity is None or not all(p.us_listed for p in positions):
        return TimeToFlat(None, "session_unknown", opportunity)
    if outstanding_authority:
        return TimeToFlat(None, "outstanding_authority", opportunity)
    if mapping_defects(positions) != (0, 0):
        return TimeToFlat(None, "mapping_defect", opportunity)
    if any(p.broker_environment == "real" for p in positions):
        return TimeToFlat(None, "unsupported_route", opportunity)
    last = last_close_created_at(opportunity, len(positions))
    if last is None:
        return TimeToFlat(None, "session_unknown", opportunity)
    return TimeToFlat((last - t0).total_seconds() + close_resolution_max_s, None, opportunity)


def mapping_defects(positions: Sequence[BookPosition]) -> tuple[int, int]:
    """(unmapped ownerships, multiply mapped ownerships)."""
    per_ownership: dict[int, int] = {}
    for p in positions:
        per_ownership[p.ownership_id] = per_ownership.get(p.ownership_id, 0) + (p.broker_position_id is not None)
    return (
        sum(1 for n in per_ownership.values() if n == 0),
        sum(1 for n in per_ownership.values() if n > 1),
    )


# ---------------------------------------------------------------------------
# Book snapshot (one REPEATABLE READ snapshot on the lock connection)
# ---------------------------------------------------------------------------

_POSITIONS_SQL: Final = """
SELECT o.ownership_id, bp.position_id, t.instrument_id, bp.updated_at,
       (bp.stop_loss_rate IS NOT NULL AND NOT bp.is_no_stop_loss),
       (bp.take_profit_rate IS NOT NULL AND NOT bp.is_no_take_profit),
       (SELECT ord.broker_environment FROM strategy_trade_orders sto
          JOIN orders ord ON ord.order_id = sto.order_id
         WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
         ORDER BY ord.order_id LIMIT 1),
       COALESCE(e.asset_class = %(us)s, false)
FROM strategy_position_ownership o
JOIN strategy_trades t ON t.strategy_trade_id = o.strategy_trade_id
LEFT JOIN broker_positions bp ON bp.position_id = o.broker_position_id
LEFT JOIN instruments i ON i.instrument_id = t.instrument_id
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
WHERE o.status = 'active'
ORDER BY o.ownership_id, bp.position_id
"""

_AUTHORITY_SQL: Final = """
SELECT 'strategy_trade', strategy_trade_id, status FROM strategy_trades WHERE status = ANY(%(states)s)
UNION ALL
SELECT 'order', order_id, status FROM orders WHERE execution_origin = 'strategy' AND status = 'submitted'
ORDER BY 1, 2
"""

_CLOSE_SAMPLES_SQL: Final = """
SELECT position_operation_id, trigger_code, status, created_at, resolved_at
FROM strategy_position_operations
WHERE operation_type = 'close'
ORDER BY position_operation_id
"""


@dataclass(frozen=True)
class CloseSample:
    position_operation_id: int
    trigger_code: str
    status: str
    created_at: datetime
    resolved_at: datetime | None


@dataclass(frozen=True)
class BookSnapshot:
    snapshot_at: datetime
    positions: tuple[BookPosition, ...]
    authority: tuple[tuple[str, int, str], ...]
    close_samples: tuple[CloseSample, ...]

    @property
    def applied_close_max_s(self) -> float | None:
        durations = [
            (s.resolved_at - s.created_at).total_seconds()
            for s in self.close_samples
            if s.status == "applied" and s.resolved_at is not None
        ]
        return max(durations) if durations else None


def take_book_snapshot(lock_conn: psycopg.Connection[Any]) -> BookSnapshot:
    """One read-only snapshot. ``lock_conn`` is autocommit; the block is a real transaction."""
    with lock_conn.transaction():
        lock_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        row = lock_conn.execute("SELECT clock_timestamp()").fetchone()
        assert row is not None
        positions = tuple(
            BookPosition(*r) for r in lock_conn.execute(_POSITIONS_SQL, {"us": US_EQUITY_ASSET_CLASS}).fetchall()
        )
        authority = tuple(
            (str(k), int(i), str(s))
            for k, i, s in lock_conn.execute(_AUTHORITY_SQL, {"states": list(OUTSTANDING_TRADE_STATES)}).fetchall()
        )
        samples = tuple(CloseSample(*r) for r in lock_conn.execute(_CLOSE_SAMPLES_SQL).fetchall())
    return BookSnapshot(row[0], positions, authority, samples)


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


@dataclass
class DrillRun:
    """Everything one run records. Mutated as the run proceeds, written once at the end."""

    run_token: uuid.UUID
    trigger: Literal["scheduled", "manual"]
    actor: str
    started_at: datetime
    scheduled_for: datetime | None = None
    job_run_id: int | None = None
    mode: Literal["sandbox", "observe"] | None = None
    kill_active_at_start: bool | None = None
    kill_active_at_verify: bool | None = None
    observed_activated_by: str | None = None
    observed_activated_at: datetime | None = None
    observe_outcome: str | None = None
    core_mandate_event_id: int | None = None
    core_mandate_revision: int | None = None
    core_instrument_id: int | None = None
    chokepoints: list[ChokepointResult] = field(default_factory=lambda: [])
    run_failure: RunFailure | None = None
    failure_detail: str | None = None
    snapshot: BookSnapshot | None = None
    finished_at: datetime | None = None
    event_id: int | None = None

    def fail(self, kind: RunFailure, detail: str) -> None:
        """Record a run failure. A later failure is appended, never replaces the first."""
        if self.run_failure is None:
            self.run_failure, self.failure_detail = kind, detail
        else:
            self.failure_detail = f"{self.failure_detail}; then {kind}: {detail}"

    @property
    def entry_verdict(self) -> EntryVerdict:
        return entry_verdict(self.chokepoints, failed_after_evaluation=self.run_failure is not None)

    @property
    def sandbox_token_reason(self) -> str:
        return f"kill drill sandbox {self.run_token}, rolled back"


def _begin_bounded(conn: psycopg.Connection[Any]) -> None:
    """Open the transaction and refuse unless it is a real one this run can roll back.

    ⚠ On an autocommit connection ``activate_kill_switch``'s ``conn.transaction()`` is a
    top-level transaction and COMMITS. Checked before any write.
    """
    if conn.autocommit:
        raise DrillRefused("drill connection is autocommit; the sandbox activation would commit")
    for stmt in _SET_TIMEOUTS:
        conn.execute(stmt)  # the first statement opens the transaction implicitly
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise DrillRefused(f"drill transaction not open: {conn.info.transaction_status!r}")


def _lock_core_keys(conn: psycopg.Connection[Any]) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", CORE_MANDATE_ADVISORY_LOCK)
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", CORE_SUBMISSION_ADVISORY_LOCK)


def _load_mandate(conn: psycopg.Connection[Any], run: DrillRun) -> None:
    mandate = load_core_mandate(conn)
    if mandate is not None and mandate.enabled and mandate.core_instrument_id is not None:
        run.core_mandate_event_id = mandate.event_id
        run.core_mandate_revision = mandate.revision
        run.core_instrument_id = mandate.core_instrument_id


def _cleanup(conn: psycopg.Connection[Any], lock_conn: psycopg.Connection[Any], run: DrillRun, *, verify: bool) -> None:
    """Roll back, then (sandbox) verify. A failure here is appended to the run's, never raised over it."""
    try:
        conn.rollback()
    except Exception as exc:  # noqa: BLE001 - appended to the run's failure
        run.fail("exception", f"rollback: {type(exc).__name__}: {exc}")
    if verify:
        try:
            _verify_rolled_back(lock_conn, run)
        except Exception as exc:  # noqa: BLE001 - appended to the run's failure
            run.fail("exception", f"verify: {type(exc).__name__}: {exc}")


def _sandbox(
    conn: psycopg.Connection[Any],
    lock_conn: psycopg.Connection[Any],
    run: DrillRun,
    now_fn: Callable[[], datetime],
) -> bool:
    """Returns True when the switch was already on and observe mode should run instead."""
    activation_attempted = False
    try:
        _begin_bounded(conn)
        _lock_core_keys(conn)
        row = conn.execute("SELECT is_active FROM kill_switch WHERE id = TRUE FOR UPDATE").fetchone()
        if row is None:
            run.fail("kill_switch_row_missing", "no kill_switch row at sandbox start")
            return False
        if bool(row[0]):
            return True
        run.mode, run.kill_active_at_start = "sandbox", False
        activation_attempted = True
        activate_kill_switch(conn, run.sandbox_token_reason, activated_by=DRILL_ACTOR)
        _load_mandate(conn, run)
        run.chokepoints = evaluate_chokepoints(conn, core_instrument_id=run.core_instrument_id, now_fn=now_fn)
    except Exception as exc:  # noqa: BLE001 - recorded as the run's failure
        logger.exception("kill drill %s sandbox failed", run.run_token)
        run.fail("exception", f"{type(exc).__name__}: {exc}")
    finally:
        _cleanup(conn, lock_conn, run, verify=activation_attempted)
    return False


def _verify_rolled_back(lock_conn: psycopg.Connection[Any], run: DrillRun) -> None:
    """Step 7: did any probe commit the sandbox activation? Looked up by this run's token."""
    committed = lock_conn.execute(
        "SELECT EXISTS (SELECT 1 FROM runtime_config_audit WHERE field = 'kill_switch' AND reason = %s)",
        (run.sandbox_token_reason,),
    ).fetchone()
    run.kill_active_at_verify = _read_kill_active(lock_conn)
    if committed is not None and committed[0]:
        run.fail("sandbox_committed", f"runtime_config_audit carries run token {run.run_token}")


def _observe(
    conn: psycopg.Connection[Any],
    lock_conn: psycopg.Connection[Any],
    run: DrillRun,
    now_fn: Callable[[], datetime],
) -> None:
    """The switch is already on: evaluate against the committed state. Writes nothing to the kill row."""
    run.mode, run.kill_active_at_start = "observe", True
    try:
        _begin_bounded(conn)
        _lock_core_keys(conn)
        row = conn.execute("SELECT is_active, activated_by, activated_at FROM kill_switch WHERE id = TRUE").fetchone()
        if row is None:
            run.fail("kill_switch_row_missing", "no kill_switch row at observe start")
        elif not row[0]:
            run.observe_outcome = "kill_changed_before_observe"
        else:
            run.observed_activated_by, run.observed_activated_at = row[1], row[2]
            _load_mandate(conn, run)
            run.chokepoints = evaluate_chokepoints(conn, core_instrument_id=run.core_instrument_id, now_fn=now_fn)
    except Exception as exc:  # noqa: BLE001 - recorded as the run's failure
        logger.exception("kill drill %s observe failed", run.run_token)
        run.fail("exception", f"{type(exc).__name__}: {exc}")
    finally:
        _cleanup(conn, lock_conn, run, verify=False)


def run_kill_switch_drill(
    connect: Callable[[], psycopg.Connection[Any]],
    *,
    trigger: Literal["scheduled", "manual"],
    actor: str,
    scheduled_for: datetime | None = None,
    job_run_id: int | None = None,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> DrillRun | None:
    """Run one drill and record it. None when another run holds the drill lock (nothing recorded).

    ``connect`` opens a NEW connection each call; the drill opens one lock connection and
    one evaluation connection and closes both. Raises if recording fails.
    """
    lock_conn = connect()
    try:
        lock_conn.autocommit = True
        lock_conn.execute("SET idle_session_timeout = '10min'")
        got = lock_conn.execute("SELECT pg_try_advisory_lock(%s, %s)", DRILL_ADVISORY_LOCK).fetchone()
        if got is None or not got[0]:
            return None
        try:
            run = DrillRun(uuid.uuid4(), trigger, actor, now_fn(), scheduled_for, job_run_id)
            try:
                # ⚠ Not ``with connect() as conn``: that block commits on a clean exit.
                conn = connect()
                try:
                    if _sandbox(conn, lock_conn, run, now_fn):
                        _observe(conn, lock_conn, run, now_fn)
                finally:
                    conn.close()
                if run.run_failure is None:
                    run.snapshot = take_book_snapshot(lock_conn)
            except Exception as exc:  # noqa: BLE001 - recorded as the run's failure
                logger.exception("kill drill %s failed", run.run_token)
                run.fail("exception", f"{type(exc).__name__}: {exc}")
            run.finished_at = now_fn()
            run.event_id = record_drill(lock_conn, run)
            return run
        finally:
            # Never raised over a recording failure; closing the session releases the lock anyway.
            try:
                lock_conn.execute("SELECT pg_advisory_unlock(%s, %s)", DRILL_ADVISORY_LOCK)
            except Exception:  # noqa: BLE001
                logger.exception("kill drill: unlock failed; the lock is released when the session closes")
    finally:
        lock_conn.close()


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


def _book_fields(run: DrillRun) -> dict[str, Any]:
    snap = run.snapshot
    if snap is None:
        return {
            "book_verdict": "not_run",
            "snapshot_at": None,
            "unmapped": None,
            "multi": None,
            "no_stop": None,
            "no_target": None,
            "estimate": None,
            "null_reason": "snapshot_unavailable",
            "samples_n": None,
            "not_applied": None,
            "r_max": None,
        }
    unmapped, multi = mapping_defects(snap.positions)
    no_stop = sum(1 for p in snap.positions if p.broker_position_id is not None and not p.stop_present)
    no_target = sum(1 for p in snap.positions if p.broker_position_id is not None and not p.target_present)
    r_max = snap.applied_close_max_s
    estimate = estimate_time_to_flat(
        snap.snapshot_at, snap.positions, outstanding_authority=len(snap.authority), close_resolution_max_s=r_max
    )
    applied = sum(1 for s in snap.close_samples if s.status == "applied" and s.resolved_at is not None)
    return {
        "book_verdict": "ok" if (unmapped, multi, no_stop, no_target) == (0, 0, 0, 0) else "defects",
        "snapshot_at": snap.snapshot_at,
        "unmapped": unmapped,
        "multi": multi,
        "no_stop": no_stop,
        "no_target": no_target,
        "estimate": estimate.seconds,
        "null_reason": estimate.null_reason,
        "samples_n": applied,
        "not_applied": len(snap.close_samples) - applied,
        "r_max": r_max,
        "opportunity_at": estimate.opportunity_at,
    }


def record_drill(lock_conn: psycopg.Connection[Any], run: DrillRun) -> int:
    """Write the event and its children in one transaction; returns the event id."""
    book = _book_fields(run)
    opportunity_at = book.pop("opportunity_at", None)
    with lock_conn.transaction():
        row = lock_conn.execute(
            """
            INSERT INTO kill_switch_drill_events (
                started_at, finished_at, scheduled_for, trigger, job_run_id, actor, run_token, mode,
                kill_active_at_start, kill_active_at_verify, observed_activated_by, observed_activated_at,
                observe_outcome, core_mandate_event_id, core_mandate_revision, core_instrument_id,
                entry_verdict, book_verdict, run_failure, failure_detail, snapshot_at,
                unmapped_ownerships, multiply_mapped_ownerships, positions_without_stop, positions_without_target,
                estimated_time_to_flat_s, estimate_null_reason, close_samples_n,
                close_samples_not_applied, close_resolution_max_s, operator_surface
            ) VALUES (
                %(started_at)s, %(finished_at)s, %(scheduled_for)s, %(trigger)s, %(job_run_id)s, %(actor)s,
                %(run_token)s, %(mode)s, %(kill_start)s, %(kill_verify)s, %(obs_by)s, %(obs_at)s,
                %(obs_outcome)s, %(mandate_id)s, %(mandate_rev)s, %(core_instrument_id)s,
                %(entry_verdict)s, %(book_verdict)s, %(run_failure)s, %(failure_detail)s, %(snapshot_at)s,
                %(unmapped)s, %(multi)s, %(no_stop)s, %(no_target)s, %(estimate)s, %(null_reason)s, %(samples_n)s,
                %(not_applied)s, %(r_max)s, %(surface)s
            )
            RETURNING kill_switch_drill_event_id
            """,
            {
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "scheduled_for": run.scheduled_for,
                "trigger": run.trigger,
                "job_run_id": run.job_run_id,
                "actor": run.actor,
                "run_token": run.run_token,
                "mode": run.mode,
                "kill_start": run.kill_active_at_start,
                "kill_verify": run.kill_active_at_verify,
                "obs_by": run.observed_activated_by,
                "obs_at": run.observed_activated_at,
                "obs_outcome": run.observe_outcome,
                "mandate_id": run.core_mandate_event_id,
                "mandate_rev": run.core_mandate_revision,
                "core_instrument_id": run.core_instrument_id,
                "entry_verdict": run.entry_verdict,
                "run_failure": run.run_failure,
                "failure_detail": run.failure_detail,
                "surface": OPERATOR_SURFACE,
                **book,
            },
        ).fetchone()
        assert row is not None
        event_id = int(row[0])
        for c in run.chokepoints:
            lock_conn.execute(
                """
                INSERT INTO kill_switch_drill_chokepoints (
                    event_id, chokepoint, outcome, refusal_code, failed_rules,
                    drill_read_kill_active, error_detail, evaluated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id,
                    c.chokepoint,
                    c.outcome,
                    c.refusal_code,
                    list(c.failed_rules),
                    c.drill_read_kill_active,
                    c.error_detail,
                    c.evaluated_at,
                ),
            )
        if run.snapshot is not None:
            _record_snapshot(lock_conn, event_id, run.snapshot, opportunity_at)
    return event_id


def _record_snapshot(
    lock_conn: psycopg.Connection[Any], event_id: int, snap: BookSnapshot, opportunity_at: datetime | None
) -> None:
    for p in snap.positions:
        opp = opportunity_at if p.us_listed else None
        lock_conn.execute(
            """
            INSERT INTO kill_switch_drill_positions (
                event_id, ownership_id, broker_position_id, instrument_id, broker_row_updated_at,
                stop_present, target_present, broker_environment, opportunity_at, session_unknown
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                p.ownership_id,
                p.broker_position_id,
                p.instrument_id,
                p.broker_row_updated_at,
                p.stop_present,
                p.target_present,
                p.broker_environment,
                opp,
                opp is None,
            ),
        )
    for kind, ref_id, state in snap.authority:
        lock_conn.execute(
            "INSERT INTO kill_switch_drill_authority (event_id, kind, ref_id, state) VALUES (%s, %s, %s, %s)",
            (event_id, kind, ref_id, state),
        )
    for s in snap.close_samples:
        lock_conn.execute(
            """
            INSERT INTO kill_switch_drill_close_samples (
                event_id, position_operation_id, trigger_code, status, created_at, resolved_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (event_id, s.position_operation_id, s.trigger_code, s.status, s.created_at, s.resolved_at),
        )


__all__ = [
    "DRILL_ADVISORY_LOCK",
    "BookPosition",
    "ChokepointResult",
    "DrillRefused",
    "DrillRun",
    "TimeToFlat",
    "classify_c1",
    "classify_c2",
    "classify_c3",
    "close_opportunity",
    "entry_verdict",
    "estimate_time_to_flat",
    "evaluate_chokepoints",
    "last_close_created_at",
    "mapping_defects",
    "record_drill",
    "run_kill_switch_drill",
    "take_book_snapshot",
]
