"""Layer refresh adapters.

One adapter per in-DAG JOB_TO_LAYERS entry (13 total). Each adapter:

- Opens its own connections via the underlying legacy scheduler function
  (which already uses `_tracked_job` for `job_runs` audit).
- Wraps the legacy call in `JobLock` so contention with a still-scheduled
  cron fire becomes PREREQ_SKIP instead of a duplicate run.
- Reads outcome INSIDE the JobLock context to avoid race with a
  concurrent cron-triggered run writing a newer `job_runs` row.
- Returns ``Sequence[tuple[str, RefreshResult]]`` per spec §2.3.
  Single-layer adapters return one element; composite adapters return
  one element per emitted layer.

Phase 4 removes the scheduled cron triggers for these 13 jobs, ending
all contention. Until then, cross-locking keeps orchestrator-triggered
and cron-triggered runs serialized.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import psycopg

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.services.ops_monitor import record_job_skip
from app.services.sync_orchestrator.types import (
    PREREQ_SKIP_MARKER,
    LayerOutcome,
    ProgressCallback,
    RefreshResult,
    prereq_skip_reason,
)

# ---------------------------------------------------------------------------
# Shared helper: convert latest job_runs row → (LayerOutcome, row_count)
# ---------------------------------------------------------------------------


def _latest_job_outcome(job_name: str) -> tuple[LayerOutcome, int]:
    """Read the most recent job_runs row for `job_name` and map to
    LayerOutcome. Called INSIDE the JobLock context so a concurrent
    cron-triggered run cannot race a newer row in between."""
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute(
            """
            SELECT status, row_count, error_msg
            FROM job_runs
            WHERE job_name = %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (job_name,),
        ).fetchone()
    if row is None:
        return LayerOutcome.FAILED, 0
    status, row_count, error_msg = row
    if status == "success":
        return (
            LayerOutcome.SUCCESS if (row_count or 0) else LayerOutcome.NO_WORK,
            row_count or 0,
        )
    if status == "skipped" and error_msg is not None and error_msg.startswith(PREREQ_SKIP_MARKER):
        return LayerOutcome.PREREQ_SKIP, 0
    return LayerOutcome.FAILED, row_count or 0


def _run_with_lock(
    job_name: str,
    legacy_fn: Any,
) -> tuple[LayerOutcome, int] | str:
    """Run legacy_fn() under JobLock. Returns (outcome, row_count) on
    success-or-handled-failure; returns a PREREQ_SKIP reason string on
    JobAlreadyRunning contention."""
    try:
        with JobLock(settings.database_url, job_name):
            try:
                legacy_fn()
            except Exception:
                # _tracked_job recorded failure; re-raise so the
                # orchestrator records FAILED for the emit.
                raise
            return _latest_job_outcome(job_name)
    except JobAlreadyRunning:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(
                conn,
                job_name,
                prereq_skip_reason("legacy cron holder active"),
            )
        return "legacy cron holder active (JobLock busy)"


def _single_emit_result(
    layer_name: str,
    outcome: LayerOutcome,
    row_count: int,
    detail: str,
) -> list[tuple[str, RefreshResult]]:
    return [
        (
            layer_name,
            RefreshResult(
                outcome=outcome,
                row_count=row_count,
                items_processed=row_count,
                items_total=None,
                detail=detail,
                error_category=None,
            ),
        )
    ]


# ---------------------------------------------------------------------------
# Single-layer adapters
# ---------------------------------------------------------------------------


def _wrap_single(
    *,
    job_name: str,
    layer_name: str,
    legacy_fn: Any,
) -> Sequence[tuple[str, RefreshResult]]:
    """Common pattern for single-emit adapters."""
    result = _run_with_lock(job_name, legacy_fn)
    if isinstance(result, str):
        return _single_emit_result(layer_name, LayerOutcome.PREREQ_SKIP, 0, result)
    outcome, row_count = result
    return _single_emit_result(layer_name, outcome, row_count, f"{job_name}: {outcome.value}")


def refresh_universe(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import nightly_universe_sync

    return _wrap_single(
        job_name="nightly_universe_sync",
        layer_name="universe",
        legacy_fn=nightly_universe_sync,
    )


def refresh_cik_mapping(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import daily_cik_refresh

    return _wrap_single(
        job_name="daily_cik_refresh",
        layer_name="cik_mapping",
        legacy_fn=daily_cik_refresh,
    )


def refresh_candles(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import daily_candle_refresh

    return _wrap_single(
        job_name="daily_candle_refresh",
        layer_name="candles",
        legacy_fn=daily_candle_refresh,
    )


def refresh_fundamentals(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import daily_research_refresh

    return _wrap_single(
        job_name="daily_research_refresh",
        layer_name="fundamentals",
        legacy_fn=daily_research_refresh,
    )


def refresh_news(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import daily_news_refresh

    return _wrap_single(
        job_name="daily_news_refresh",
        layer_name="news",
        legacy_fn=daily_news_refresh,
    )


def refresh_thesis(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import daily_thesis_refresh

    return _wrap_single(
        job_name="daily_thesis_refresh",
        layer_name="thesis",
        legacy_fn=daily_thesis_refresh,
    )


def refresh_portfolio_sync(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import daily_portfolio_sync

    return _wrap_single(
        job_name="daily_portfolio_sync",
        layer_name="portfolio_sync",
        legacy_fn=daily_portfolio_sync,
    )


def refresh_cost_models(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import seed_cost_models

    return _wrap_single(
        job_name="seed_cost_models",
        layer_name="cost_models",
        legacy_fn=seed_cost_models,
    )


def refresh_fx_rates(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import fx_rates_refresh

    return _wrap_single(
        job_name="fx_rates_refresh",
        layer_name="fx_rates",
        legacy_fn=fx_rates_refresh,
    )


def refresh_weekly_reports(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import weekly_report

    return _wrap_single(
        job_name="weekly_report",
        layer_name="weekly_reports",
        legacy_fn=weekly_report,
    )


def refresh_monthly_reports(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    from app.workers.scheduler import monthly_report

    return _wrap_single(
        job_name="monthly_report",
        layer_name="monthly_reports",
        legacy_fn=monthly_report,
    )


# ---------------------------------------------------------------------------
# Composite adapters (spec §2.3.1)
# ---------------------------------------------------------------------------


def refresh_financial_facts_and_normalization(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    """Composite: daily_financial_facts emits (financial_facts,
    financial_normalization). Atomic — both emits share one outcome."""
    from app.workers.scheduler import daily_financial_facts

    result = _run_with_lock("daily_financial_facts", daily_financial_facts)
    if isinstance(result, str):
        skip = RefreshResult(
            outcome=LayerOutcome.PREREQ_SKIP,
            row_count=0,
            items_processed=0,
            items_total=None,
            detail=result,
            error_category=None,
        )
        return [("financial_facts", skip), ("financial_normalization", skip)]

    outcome, row_count = result
    return [
        (
            "financial_facts",
            RefreshResult(
                outcome=outcome,
                row_count=row_count,
                items_processed=row_count,
                items_total=None,
                detail="xbrl fetch",
                error_category=None,
            ),
        ),
        (
            "financial_normalization",
            RefreshResult(
                outcome=outcome,
                row_count=0,
                items_processed=0,
                items_total=None,
                detail="normalization pass",
                error_category=None,
            ),
        ),
    ]


def refresh_scoring_and_recommendations(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    """Composite: morning_candidate_review emits (scoring, recommendations).

    Does NOT call execute_approved_orders — that side effect lives
    only on the legacy scheduled path (spec §2.3.1). Calls the
    extracted compute_morning_recommendations() which handles the
    per-phase connection discipline.
    """
    from app.workers.scheduler import (
        JOB_MORNING_CANDIDATE_REVIEW,
        MorningComputeResult,
        _tracked_job,
        compute_morning_recommendations,
    )

    job_name = JOB_MORNING_CANDIDATE_REVIEW

    try:
        with JobLock(settings.database_url, job_name):
            # _tracked_job writes the job_runs row under the legacy
            # name; compute_morning_recommendations owns its own
            # connections per phase. No order execution.
            result: MorningComputeResult
            with _tracked_job(job_name) as tracker:
                result = compute_morning_recommendations()
                tracker.row_count = len(result.ranking_result.scored) + (
                    len(result.review_result.recommendations) if result.review_result is not None else 0
                )
            outcome, _ = _latest_job_outcome(job_name)
    except JobAlreadyRunning:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(
                conn,
                job_name,
                prereq_skip_reason("legacy cron holder active"),
            )
        skip = RefreshResult(
            outcome=LayerOutcome.PREREQ_SKIP,
            row_count=0,
            items_processed=0,
            items_total=None,
            detail="legacy cron holder active (JobLock busy)",
            error_category=None,
        )
        return [("scoring", skip), ("recommendations", skip)]

    scoring_count = len(result.ranking_result.scored)
    if result.review_result is None:
        rec_outcome = LayerOutcome.NO_WORK
        rec_count = 0
        rec_detail = "no eligible instruments to score"
    else:
        rec_outcome = outcome
        rec_count = len(result.review_result.recommendations)
        rec_detail = "recommendations pass"

    return [
        (
            "scoring",
            RefreshResult(
                outcome=outcome,
                row_count=scoring_count,
                items_processed=scoring_count,
                items_total=None,
                detail="scoring pass",
                error_category=None,
            ),
        ),
        (
            "recommendations",
            RefreshResult(
                outcome=rec_outcome,
                row_count=rec_count,
                items_processed=rec_count,
                items_total=None,
                detail=rec_detail,
                error_category=None,
            ),
        ),
    ]
