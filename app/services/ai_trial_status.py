"""#3514 — the AI trial's operator-visible readiness: every "nothing happened" state, named.

A day with no trial trade can mean the trial is not declared or not active, the decision job did
not run, the model abstained, every decision was refused, the run itself was refused, the legs
are waiting for the 15:00 UTC execute fire, or each leg was refused by the executor. This module
reads the stored rows and names which one it was, per target session, so none of them looks like
another or like a failure (the #2843 validity contract; #2218's "a no-op that reports success is
invisible").

Read-only. Every state is derived from a stored row:

* declaration and trial state — ``ai_trial_declarations`` + the latest ``ai_trial_state_events``
  (``ai_trial_run.load_declaration``, the loader the decision run itself uses);
* the decision per session — ``ai_trial_runs`` and its decisions / leg links (``decision_state``);
* the execution per session — each leg's ``strategy_funding_decisions`` row (``execution_states``);
* the two jobs' last fires — ``job_runs``; their next fires — the declared cadence;
* open legs and the loss-halt distance — ``ai_trial_halts``'s own loader and P&L rule, so this
  panel can never show a different loss from the one the halt acts on.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.services.ai_trial_deadline import TRIAL_ENTRY_TIME_UTC
from app.services.ai_trial_halts import (
    TRIAL_LOSS_LIMIT_USD,
    LegTrade,
    leg_losses,
    load_leg_trades,
    trade_pnl_usd,
)
from app.services.ai_trial_pack_reader import next_us_session
from app.services.ai_trial_readout import load_close_rows
from app.services.ai_trial_run import load_declaration
from app.services.market_calendar import latest_completed_us_session
from app.services.strategy_paper_executor import _NY
from app.workers.scheduler import (
    JOB_AI_TRIAL_DECISION_RUN,
    JOB_AI_TRIAL_EXECUTE,
    SCHEDULED_JOBS,
    compute_next_run,
)

Conn = psycopg.Connection[Any]

#: The decision job's daily fire (``scheduler``: ``Cadence.daily(hour=23, minute=30)``).
DECISION_FIRE_UTC: Final = time(23, 30)
#: Target sessions shown, newest first.
RECENT_SESSIONS: Final = 10

TrialState = Literal[
    "not_declared", "not_started", "active", "halted_harm", "halted_loss", "halted_mandate", "halted_operator"
]
DecisionState = Literal["not_run", "deciding", "refused", "abstained", "no_valid_plan", "legs_published"]
ExecutionState = Literal["submitted", "refused", "awaiting_execution", "not_run"]


# ---------------------------------------------------------------------------
# Pure derivations
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RunFacts:
    """One ``ai_trial_runs`` row and what it published."""

    session_date: date
    status: str
    refusal_reason: str | None
    decisions: int
    #: Refused decisions by ``reason_code``.
    decision_refusals: Mapping[str, int]
    #: ``ai_trial_leg_links`` rows (two per accepted decision: arm and control).
    legs: int


@dataclass(frozen=True)
class DecisionOutcome:
    state: DecisionState
    #: ``refused:<code>`` / ``legs_published:<n>``, else the state itself.
    label: str
    legs: int = 0
    refusal_reason: str | None = None
    decision_refusals: Mapping[str, int] = field(default_factory=dict)


def decision_state(run: RunFacts | None) -> DecisionOutcome:
    """What the decision run did for one target session.

    * no run row → ``not_run``;
    * ``claimed`` → ``deciding`` (the 15-minute lease is live, or the stale-claim sweep has not
      closed it yet);
    * ``refused`` → ``refused:<refusal_reason>`` — the whole run, before or after the model call;
    * ``decided`` with no decision → ``abstained`` (the model returned an empty list);
    * ``decided`` with decisions but no leg → ``no_valid_plan``: every decision was refused, and
      ``decision_refusals`` says why;
    * otherwise ``legs_published:<n>``.
    """
    if run is None:
        return DecisionOutcome("not_run", "not_run")
    if run.status == "claimed":
        return DecisionOutcome("deciding", "deciding")
    if run.status == "refused":
        reason = run.refusal_reason or "unknown"
        return DecisionOutcome("refused", f"refused:{reason}", refusal_reason=reason)
    refusals = dict(run.decision_refusals)
    if run.decisions == 0:
        return DecisionOutcome("abstained", "abstained")
    if run.legs == 0:
        return DecisionOutcome("no_valid_plan", "no_valid_plan", decision_refusals=refusals)
    return DecisionOutcome("legs_published", f"legs_published:{run.legs}", legs=run.legs, decision_refusals=refusals)


@dataclass(frozen=True)
class LegFunding:
    """One published leg and its funding decision (``verdict`` ``None`` = none yet)."""

    leg: str
    verdict: str | None
    reason_code: str | None


@dataclass(frozen=True)
class ExecutionOutcome:
    state: ExecutionState
    count: int
    reason: str | None = None

    @property
    def label(self) -> str:
        if self.state == "refused":
            return f"refused:{self.reason}×{self.count}"
        return f"{self.state}:{self.count}"


def execute_fired(session_date: date, execute_fires: Sequence[datetime]) -> bool:
    """Whether an ``ai_trial_execute`` run started on ``session_date`` (New York) at or after the
    frozen entry time — the only fire that acts on that session's legs (``run_trial_execution``
    returns ``session_closed`` before it)."""
    return any(
        fire.astimezone(_NY).date() == session_date and fire.astimezone(UTC).time() >= TRIAL_ENTRY_TIME_UTC
        for fire in execute_fires
    )


def execution_states(
    session_date: date, legs: Sequence[LegFunding], *, today: date, execute_fires: Sequence[datetime]
) -> list[ExecutionOutcome]:
    """What the execute job did with one session's published legs.

    ``submitted`` (allocated) and ``refused`` (by ``reason_code``) are the executor's persisted
    verdicts. A leg with no funding decision is ``awaiting_execution`` while its session's
    in-window fire is still ahead, and ``not_run`` once it is not: a later session day's fire
    refuses it ``decision_expired``, so a leg left here is a fire that did not reach it (or
    raised on it — the job's own run row counts those)."""
    submitted = sum(1 for leg in legs if leg.verdict == "allocated")
    refused: dict[str, int] = {}
    for leg in legs:
        if leg.verdict == "rejected":
            code = leg.reason_code or "unknown"
            refused[code] = refused.get(code, 0) + 1
    pending = sum(1 for leg in legs if leg.verdict is None)
    outcomes: list[ExecutionOutcome] = []
    if submitted:
        outcomes.append(ExecutionOutcome("submitted", submitted))
    outcomes.extend(ExecutionOutcome("refused", count, code) for code, count in sorted(refused.items()))
    if pending:
        waiting = session_date > today or (session_date == today and not execute_fired(session_date, execute_fires))
        outcomes.append(ExecutionOutcome("awaiting_execution" if waiting else "not_run", pending))
    return outcomes


def expected_decision_session(now: datetime) -> tuple[datetime, date]:
    """The latest decision fire at or before ``now`` and the session it targets (the claim's own
    rule, ``ai_trial_jobs.run_decision_job``)."""
    observed = now.astimezone(UTC)
    fire = datetime.combine(observed.date(), DECISION_FIRE_UTC, tzinfo=UTC)
    if fire > observed:
        fire -= timedelta(days=1)
    return fire, next_us_session(latest_completed_us_session(fire))


# ---------------------------------------------------------------------------
# Read model
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class JobFire:
    job_name: str
    next_fire_at: datetime
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_status: str | None = None
    #: The job's own outcome note (``job_runs.error_msg`` carries it on success too).
    last_note: str | None = None


@dataclass(frozen=True)
class SessionStatus:
    session_date: date
    decision: DecisionOutcome
    execution: list[ExecutionOutcome]


@dataclass(frozen=True)
class OpenLeg:
    leg: str
    pair_seq: int
    strategy_trade_id: int
    symbol: str
    #: ``strategy_trades.status``: submitted / open / closing / reconcile_required.
    trade_status: str
    entry_price: Decimal | None
    requested_stop: Decimal | None
    requested_target: Decimal | None
    #: The broker's own levels from the active owned position; ``None`` = not observed.
    broker_stop: Decimal | None
    broker_target: Decimal | None
    #: ``ai_trial_halts.trade_pnl_usd``; ``None`` = unmeasured.
    pnl_usd: Decimal | None
    exit_deadline_session: date | None


@dataclass(frozen=True)
class LegLossStatus:
    leg: str
    pnl_usd: Decimal
    unmeasured: int
    limit_usd: Decimal

    @property
    def headroom_usd(self) -> Decimal:
        """Loss still available before the halt (``limit − loss``); ≤ 0 means reached."""
        return self.limit_usd + self.pnl_usd


@dataclass(frozen=True)
class TrialStatus:
    state: TrialState
    declaration_id: int | None
    state_reason: str | None
    state_at: datetime | None
    decision_job: JobFire
    execute_job: JobFire
    sessions: list[SessionStatus]
    open_legs: list[OpenLeg]
    loss: list[LegLossStatus]


_STATE_EVENT_SQL: Final = """
    SELECT to_state, reason, at,
           (SELECT min(at) FROM ai_trial_state_events WHERE declaration_id = %(d)s) AS genesis_at
    FROM ai_trial_state_events WHERE declaration_id = %(d)s ORDER BY event_id DESC LIMIT 1
"""

_RUNS_SQL: Final = """
    SELECT r.run_id, r.session_date, r.status, r.refusal_reason,
           (SELECT count(*) FROM ai_trial_decisions d WHERE d.run_id = r.run_id),
           (SELECT coalesce(jsonb_object_agg(reason_code, n), '{}'::jsonb) FROM (
                SELECT reason_code, count(*) AS n FROM ai_trial_decisions d
                WHERE d.run_id = r.run_id AND d.verdict = 'refused' GROUP BY reason_code) x),
           (SELECT count(*) FROM ai_trial_leg_links l
              JOIN ai_trial_pairs p ON p.pair_id = l.pair_id
              JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id
             WHERE d.run_id = r.run_id)
    FROM ai_trial_runs r
    WHERE r.declaration_id = %s
    ORDER BY r.session_date DESC
    LIMIT %s
"""

_LEG_FUNDING_SQL: Final = """
    SELECT d.run_id, l.leg, fd.verdict, fd.reason_code
    FROM ai_trial_leg_links l
    JOIN ai_trial_pairs p ON p.pair_id = l.pair_id
    JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id
    LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
    WHERE d.run_id = ANY(%s)
    ORDER BY d.run_id, p.pair_seq, l.leg
"""

_LAST_JOB_RUN_SQL: Final = """
    SELECT DISTINCT ON (job_name) job_name, started_at, finished_at, status, error_msg
    FROM job_runs WHERE job_name = ANY(%s)
    ORDER BY job_name, started_at DESC
"""

_EXECUTE_FIRES_SQL: Final = """
    SELECT started_at FROM job_runs WHERE job_name = %s AND started_at >= %s
"""

_OPEN_LEGS_SQL: Final = """
    SELECT tl.leg, p.pair_seq, t.strategy_trade_id, i.symbol, t.status, t.exit_deadline_session,
           pf.stop_loss_rate, pf.take_profit_rate, bp.stop_loss_rate, bp.take_profit_rate,
           bp.is_no_stop_loss, bp.is_no_take_profit,
           i.currency = 'USD', entry.units, entry.average_price, q.bid, entry.filled_at, q.quoted_at
    FROM ai_trial_trade_links tl
    JOIN ai_trial_pairs p ON p.pair_id = tl.pair_id
    JOIN strategy_trades t ON t.strategy_trade_id = tl.strategy_trade_id
    JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id
    JOIN instruments i ON i.instrument_id = t.instrument_id
    LEFT JOIN strategy_entry_preflights pf ON pf.signal_id = fd.signal_id AND pf.verdict = 'allocated'
    LEFT JOIN strategy_position_ownership o ON o.strategy_trade_id = t.strategy_trade_id AND o.status = 'active'
    LEFT JOIN broker_positions bp ON bp.position_id = o.broker_position_id
    LEFT JOIN quotes q ON q.instrument_id = t.instrument_id
    LEFT JOIN LATERAL (
        SELECT sum(e.opening_units) AS units,
               sum(e.opening_units * e.average_price) / NULLIF(sum(e.opening_units), 0) AS average_price,
               min(e.execution_time) AS filled_at
        FROM strategy_trade_orders sto
        JOIN strategy_order_position_executions e ON e.order_id = sto.order_id
        WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
          AND e.opening_units > 0 AND e.average_price > 0
    ) entry ON TRUE
    WHERE p.declaration_id = %s AND t.status NOT IN ('closed', 'failed')
    ORDER BY p.pair_seq, tl.leg
"""


def _dec(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _job_fires(conn: Conn, now: datetime) -> tuple[JobFire, JobFire]:
    cadences = {job.name: job.cadence for job in SCHEDULED_JOBS}
    names = [JOB_AI_TRIAL_DECISION_RUN, JOB_AI_TRIAL_EXECUTE]
    last = {row[0]: row[1:] for row in conn.execute(_LAST_JOB_RUN_SQL, (names,)).fetchall()}
    fires = []
    for name in names:
        started, finished, status, note = last.get(name, (None, None, None, None))
        fires.append(JobFire(name, compute_next_run(cadences[name], now), started, finished, status, note))
    return fires[0], fires[1]


def _open_legs(conn: Conn, declaration_id: int) -> list[OpenLeg]:
    rows = conn.execute(_OPEN_LEGS_SQL, (declaration_id,)).fetchall()
    closes = load_close_rows(conn, [int(row[2]) for row in rows])
    legs: list[OpenLeg] = []
    for row in rows:
        (leg, pair_seq, trade_id, symbol, status, deadline, req_sl, req_tp, bp_sl, bp_tp, no_sl, no_tp) = row[:12]
        usd, units, average_price, bid, filled_at, bid_at = row[12:]
        pnl = trade_pnl_usd(
            LegTrade(
                leg=str(leg),
                status=str(status),
                usd=bool(usd),
                opened_units=_dec(units),
                average_price=_dec(average_price),
                bid=_dec(bid),
                closes=tuple(closes.get(int(trade_id), ())),
                filled_at=filled_at,
                bid_at=bid_at,
            )
        )
        legs.append(
            OpenLeg(
                leg=str(leg),
                pair_seq=int(pair_seq),
                strategy_trade_id=int(trade_id),
                symbol=str(symbol),
                trade_status=str(status),
                entry_price=_dec(average_price),
                requested_stop=_dec(req_sl),
                requested_target=_dec(req_tp),
                # eToro reports a removed level as a flag, not a null rate.
                broker_stop=None if no_sl else _dec(bp_sl),
                broker_target=None if no_tp else _dec(bp_tp),
                pnl_usd=pnl,
                exit_deadline_session=deadline,
            )
        )
    return legs


def _sessions(conn: Conn, declaration_id: int, *, now: datetime, genesis_at: datetime | None) -> list[SessionStatus]:
    runs = conn.execute(_RUNS_SQL, (declaration_id, RECENT_SESSIONS)).fetchall()
    run_ids = [int(row[0]) for row in runs]
    funding: dict[int, list[LegFunding]] = {}
    for run_id, leg, verdict, reason in conn.execute(_LEG_FUNDING_SQL, (run_ids,)).fetchall():
        funding.setdefault(int(run_id), []).append(LegFunding(str(leg), verdict, reason))
    oldest = min((row[1] for row in runs), default=now.astimezone(_NY).date())
    execute_fires = [
        row[0]
        for row in conn.execute(
            _EXECUTE_FIRES_SQL, (JOB_AI_TRIAL_EXECUTE, datetime.combine(oldest, time(0), tzinfo=_NY))
        ).fetchall()
    ]
    today = now.astimezone(_NY).date()
    sessions: list[SessionStatus] = []
    for run_id, session_date, status, refusal, decisions, refusals, legs in runs:
        facts = RunFacts(session_date, str(status), refusal, int(decisions), dict(refusals), int(legs))
        sessions.append(
            SessionStatus(
                session_date,
                decision_state(facts),
                execution_states(session_date, funding.get(int(run_id), []), today=today, execute_fires=execute_fires),
            )
        )
    # The session the latest decision fire targeted, when the trial was already running then and
    # that fire left no run row at all: the job did not run, or returned before its claim.
    fire, expected = expected_decision_session(now)
    if genesis_at is not None and genesis_at <= fire and all(s.session_date != expected for s in sessions):
        sessions.insert(0, SessionStatus(expected, decision_state(None), []))
    return sessions


def load_trial_status(conn: Conn, *, now: datetime | None = None) -> TrialStatus:
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    decision_job, execute_job = _job_fires(conn, observed)
    declaration = load_declaration(conn)
    if declaration is None:
        return TrialStatus("not_declared", None, None, None, decision_job, execute_job, [], [], [])
    declaration_id = declaration.declaration_id
    event = conn.execute(_STATE_EVENT_SQL, {"d": declaration_id}).fetchone()
    # No state event is NOT active (the loader's own fail-closed rule): the declaration was
    # frozen but never started.
    state: TrialState = "not_started" if event is None else event[0]
    genesis_at = None if event is None else event[3]
    losses = leg_losses(load_leg_trades(conn, declaration_id))
    loss = [LegLossStatus(item.leg, item.pnl_usd, item.unmeasured, TRIAL_LOSS_LIMIT_USD) for item in losses]
    return TrialStatus(
        state=state,
        declaration_id=declaration_id,
        state_reason=None if event is None else str(event[1]),
        state_at=None if event is None else event[2],
        decision_job=decision_job,
        execute_job=execute_job,
        sessions=_sessions(conn, declaration_id, now=observed, genesis_at=genesis_at),
        open_legs=_open_legs(conn, declaration_id),
        loss=loss,
    )


__all__ = [
    "DecisionOutcome",
    "ExecutionOutcome",
    "JobFire",
    "LegFunding",
    "LegLossStatus",
    "OpenLeg",
    "RunFacts",
    "SessionStatus",
    "TrialStatus",
    "decision_state",
    "execute_fired",
    "execution_states",
    "expected_decision_session",
    "load_trial_status",
]
