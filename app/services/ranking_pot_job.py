"""Ranking-pot-v1 monthly rebalance job (#2842 slice 4b-ii).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §4 steps 0–4: due, the pot's own scoring run,
the input gates and the published snapshot. Steps 2–3 are ``ranking_pot_rebalance.prepare``; this module sequences
them and owns WHEN a fire acts.

**The decision window (spec deviation, measured; §4 step 0 said "23:30 UTC").** A 23:30 UTC fire cannot pass its own
``price_daily_stale`` gate: the nightly full ``daily_candle_refresh`` finished between 23:52 and 06:50 UTC on its last
eight runs (``SELECT started_at, finished_at FROM job_runs WHERE job_name = 'daily_candle_refresh' AND finished_at -
started_at > interval '20 min' ORDER BY started_at``, 2026-09-22 → 10-01), and at 22:50 UTC on 10-01 ``price_daily``
held 1,144 of 3,925 S₀ bars. So the job fires hourly and acts only inside the window that follows a completed session
D: from ``WINDOW_OPENS`` on D until ``WINDOW_CLOSES`` the next calendar day (still before that day's open and the
15:00 UTC execute). Before ``BARS_WAIT_UNTIL`` a fire whose S₀ bars for D are below the gate's 95% does not score: it
records a ``price_daily_stale`` refusal (``detail.pre_score``), so every opportunity leaves a row (r3-88). From it on,
the fire scores and the gates decide; the first failing one is recorded. The waiting proxy counts S₀ names tradable
now, the gate counts those the run scored: the proxy only decides whether scoring is worth running. The §5.0
quote rule moves with it: ``as_of`` is the time read just after the snapshot transaction's first query, and
``is_eligible``'s 24 h quote age still admits the post-close quote.

**Books (spec deviation; the PR records it).** This slice publishes the decided snapshot. It decides no book:

- the executed book exists only in ``executing`` / ``halted_*``, which only slice 5's activation script reaches;
  until slice 5 lands its holdings reader and decision writer, a fire in any state but ``shadow_only`` raises
  (fail closed, visible as a failed job);
- the shadow and the controls are decided by slice 6's online step job when it steps the target session, from this
  snapshot and each book's ledger stepped through the last completed session (r3-41 by construction). Their
  decisions are a pure function of those two, so nothing is chosen after publication.

**Looks (§9.3, slice 6b).** Before the bar wait and scoring, a target session after a look endpoint whose look is not
stored (or whose wind-down event is not yet written) refuses ``look_pending`` (``ranking_pot_look.look_pending``).

**Scoring (§4 step 1).** ``compute_rankings`` on its own connection, committed before the snapshot transaction opens
(r3-80); the job runs on the ``db`` source lane, the scheduled scorer's, so the two never overlap. Each name's thesis
provenance is the one the scoring call consumed (``ScoreResult.thesis_used``, r3-95), never re-read.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from fractions import Fraction
from typing import Any, Final, Literal

import psycopg

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services.market_calendar import latest_completed_us_session
from app.services.scoring import compute_rankings

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

#: UTC, on the completed session's date.
WINDOW_OPENS: Final = time(23, 30)
#: UTC, the next calendar day: fires before this wait for the session's bars. 2 h 10 min after the latest measured
#: refresh finish (06:50), so the :40 fires at 09:40, 10:40 and 11:40 are three final opportunities.
BARS_WAIT_UNTIL: Final = time(9, 0)
#: UTC, the next calendar day: no fire acts at or after this (1 h 30 min before the open, 3 h before the execute).
WINDOW_CLOSES: Final = time(12, 0)
#: The scheduler's hourly fire minute. Here, not in the scheduler, so the policy hash covers it: which fire first
#: sees a passing state decides the snapshot (and the first snapshot the §9.2 seed). The scheduler's copy is pinned
#: to this by ``tests/test_ranking_pot_job.py``.
FIRE_MINUTE: Final = 40

Phase = Literal["closed", "waiting", "final"]


def window_phase(as_of: datetime) -> Phase:
    """Where ``as_of`` falls in the decision window of the last completed NYSE session (module docstring)."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    d = latest_completed_us_session(as_of)
    nxt = d + timedelta(days=1)
    at = as_of.astimezone(UTC)
    if not datetime.combine(d, WINDOW_OPENS, UTC) <= at < datetime.combine(nxt, WINDOW_CLOSES, UTC):
        return "closed"
    return "waiting" if at < datetime.combine(nxt, BARS_WAIT_UNTIL, UTC) else "final"


@dataclass(frozen=True)
class ScoringRun:
    scored_at: datetime
    #: Every scored name whose score consumed a thesis.
    theses: dict[int, rb.ThesisUsed]


def scoring_step(connect: Callable[[], AbstractContextManager[Conn]]) -> ScoringRun:
    """§4 step 1: one ordinary ``scores`` run, committed when its connection closes."""
    with connect() as sconn:
        result = compute_rankings(sconn)
    return ScoringRun(
        scored_at=result.run_at,
        theses={
            r.instrument_id: rb.ThesisUsed(
                r.thesis_used.thesis_id, r.thesis_used.created_at, r.thesis_used.model, r.thesis_used.prompt_version
            )
            for r in result.scored
            if r.thesis_used is not None
        },
    )


@dataclass(frozen=True)
class JobResult:
    note: str
    #: The attempt row this fire wrote (refused or decided), if any.
    attempt_id: int | None = None
    skipped_months: int = 0


def run_rebalance_job(
    conn: Conn,
    *,
    score: Callable[[], ScoringRun],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> JobResult:
    """One fire. ``conn`` must be autocommit: every write below is its own top-level transaction."""
    if not conn.autocommit:
        raise RuntimeError("run_rebalance_job needs an autocommit connection")
    as_of = now()
    decl = rb.load_declaration(conn)
    if decl is None:
        return JobResult("no ranking-pot declaration")
    if decl.state in (None, "winding_down", "completed"):
        return JobResult(f"no rebalance runs in state {decl.state}")

    history = rb.read_history(conn, decl.declaration_id)
    due = rb.plan(as_of, first=rb.first_month(decl.frozen_at), resolved=history.resolved)
    skipped = 0
    if due.skips:  # r3-71: every fire closes what can no longer be decided, in or out of the window
        with conn.transaction():
            skipped = rb.record_skips(conn, decl, due, history, as_of=as_of)
    if not due.due:
        return JobResult(f"target {due.target_session}: not due", skipped_months=skipped)
    phase = window_phase(as_of)
    if phase == "closed":
        return JobResult(f"target {due.target_session}: outside the decision window", skipped_months=skipped)
    if decl.state != "shadow_only":
        raise RuntimeError(
            f"ranking pot {decl.declaration_id} is {decl.state}: executed-book decisions are slice 5's, so this "
            "build refuses to rebalance outside shadow_only"
        )
    pending = look.look_pending(conn, decl.declaration_id, due.target_session)
    if pending is not None:
        # §9.3: no rebalance targets a session after a look endpoint until that look is stored (and its wind-down,
        # if any, written). Both only ever clear, so this pre-score check cannot go stale in the pass direction.
        with conn.transaction():
            attempt = rb.record_refused(
                conn, decl, due, rb.Refused("look_pending", pending), as_of=as_of, scored_at=None
            )
        return JobResult(f"target {due.target_session}: refused look_pending ({pending})", attempt, skipped)
    if phase == "waiting":
        barred, tradable = rb.bars_ready(conn, decl, as_of=as_of)
        if not (tradable > 0 and Fraction(barred, tradable) >= rb.BAR_COVERAGE_MIN):
            # Recorded, not silent (r3-88): every fire that reached a verdict on the month leaves its row.
            waiting = rb.Refused("price_daily_stale", {"pre_score": True, "barred": barred, "s0_tradable": tradable})
            with conn.transaction():
                attempt = rb.record_refused(conn, decl, due, waiting, as_of=as_of, scored_at=None)
            return JobResult(
                f"target {due.target_session}: {barred}/{tradable} S0 bars landed; retried next fire",
                attempt,
                skipped,
            )

    try:
        run = score()
    except Exception:
        logger.exception("ranking pot: scoring run failed")
        with conn.transaction():
            attempt = rb.record_refused(conn, decl, due, "scoring_failed", as_of=as_of, scored_at=None)
        return JobResult(f"target {due.target_session}: refused scoring_failed", attempt, skipped)

    s0 = set(decl.s0_ids)
    # An early exit below rolls the transaction back explicitly (``psycopg.Rollback``) rather than returning
    # through the block, which would COMMIT it: nothing is written before them today, and nothing must be.
    early: str | None = None
    with conn.transaction() as tx:
        rb.begin_rebalance(conn)
        # The first query fixes the REPEATABLE READ snapshot; `as_of` is read after it, so every quote the
        # snapshot can see is at or before `as_of` (`is_eligible` refuses a quote stamped after it).
        conn.execute("SELECT 1")
        snap_as_of = now()
        if rb.target_session(snap_as_of) != due.target_session or window_phase(snap_as_of) == "closed":
            early = f"target {due.target_session}: the window passed during scoring"
            raise psycopg.Rollback(tx)
        live = rb.load_declaration(conn)
        if live is None or live.declaration_id != decl.declaration_id or live.state != "shadow_only":
            early = f"declaration changed during the fire ({live and live.state}); retried next fire"
            raise psycopg.Rollback(tx)
        history = rb.read_history(conn, decl.declaration_id)
        if due.month in history.resolved:
            early = f"month {due.month} already resolved"
            raise psycopg.Rollback(tx)
        result = rb.prepare(
            conn,
            live,
            history,
            as_of=snap_as_of,
            scored_at=run.scored_at,
            theses={iid: t for iid, t in run.theses.items() if iid in s0},
        )
        if isinstance(result, rb.Refused):
            attempt = rb.record_refused(conn, live, due, result, as_of=snap_as_of, scored_at=run.scored_at)
            return JobResult(f"target {due.target_session}: refused {result.refusal}", attempt, skipped)
        attempt = rb.record_decided(conn, live, due, result, as_of=snap_as_of, scored_at=run.scored_at)
    if early is not None:
        return JobResult(early, None, skipped)
    return JobResult(
        f"target {due.target_session}: decided (R={result.detail['r_count']}, F={result.detail['f_count']}, "
        f"snapshot {result.snapshot_sha256[:12]})",
        attempt,
        skipped,
    )


__all__ = [
    "BARS_WAIT_UNTIL",
    "FIRE_MINUTE",
    "WINDOW_CLOSES",
    "WINDOW_OPENS",
    "JobResult",
    "ScoringRun",
    "run_rebalance_job",
    "scoring_step",
    "window_phase",
]
