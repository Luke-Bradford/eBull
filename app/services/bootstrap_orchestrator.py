"""First-install bootstrap orchestrator.

Runs the 17-stage end-to-end first-install backfill described in
``docs/superpowers/specs/2026-05-07-first-install-bootstrap.md``.

Three phases:

1. **Phase A — init** (sequential, single thread): runs the universe
   sync (A1). Every Phase B stage depends on a populated
   ``instruments`` table.
2. **Phase B — lanes** (two threads in parallel): the eToro lane
   (E1: candle refresh) runs alongside the SEC lane (S1..S15:
   filer directories, CIK refresh, filing-events seed, manifest
   drain, typed parsers, ownership rollup, fundamentals).
3. **Phase C — finalize** (sequential): inspects per-stage outcomes
   and transitions ``bootstrap_state`` to ``complete`` or
   ``partial_error``.

Per-stage execution contract (every stage):

1. Pre-check stage status; skip if ``success``.
2. Mark stage ``running``.
3. Acquire ``JobLock(database_url, job_name)`` — same primitive
   that scheduled + manual paths use.
4. Invoke ``_INVOKERS[job_name]()``.
5. Catch exceptions; record ``error`` with truncated message.
6. On success record ``success`` + ``rows_processed``.

Bootstrap dispatches stage jobs by direct invocation, bypassing the
scheduler's prerequisite gate (intentional: bootstrap is the operator
forcing first-install work). The advisory lock is acquired so a
parallel manual / scheduled trigger cannot run twice simultaneously.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.services.bootstrap_state import (
    StageSpec,
    finalize_run,
    mark_stage_error,
    mark_stage_running,
    mark_stage_success,
    read_latest_run_with_stages,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage catalogue — single source of truth for which stages run.
# ---------------------------------------------------------------------------

# Job names registered in app/jobs/runtime.py:_INVOKERS that PR2 adds:
JOB_BOOTSTRAP_ORCHESTRATOR = "bootstrap_orchestrator"
JOB_BOOTSTRAP_FILINGS_HISTORY_SEED = "bootstrap_filings_history_seed"
JOB_SEC_FIRST_INSTALL_DRAIN = "sec_first_install_drain"

# These already exist as scheduled jobs but were not registered in
# _INVOKERS until PR2; we re-use the existing job-name constants so
# operator records / job_runs trail stays consistent.
JOB_DAILY_CIK_REFRESH = "daily_cik_refresh"
JOB_DAILY_FINANCIAL_FACTS = "daily_financial_facts"


def _spec(stage_key: str, stage_order: int, lane: str, job_name: str) -> StageSpec:
    return StageSpec(stage_key=stage_key, stage_order=stage_order, lane=lane, job_name=job_name)  # type: ignore[arg-type]


_BOOTSTRAP_STAGE_SPECS: tuple[StageSpec, ...] = (
    # Phase A (init, sequential)
    _spec("universe_sync", 1, "init", "nightly_universe_sync"),
    # Phase B — eToro lane
    _spec("candle_refresh", 2, "etoro", "daily_candle_refresh"),
    # Phase B — SEC lane (15 stages, sequential)
    _spec("cusip_universe_backfill", 3, "sec", "cusip_universe_backfill"),
    _spec("sec_13f_filer_directory_sync", 4, "sec", "sec_13f_filer_directory_sync"),
    _spec("sec_nport_filer_directory_sync", 5, "sec", "sec_nport_filer_directory_sync"),
    _spec("cik_refresh", 6, "sec", JOB_DAILY_CIK_REFRESH),
    _spec("filings_history_seed", 7, "sec", JOB_BOOTSTRAP_FILINGS_HISTORY_SEED),
    _spec("sec_first_install_drain", 8, "sec", JOB_SEC_FIRST_INSTALL_DRAIN),
    _spec("sec_def14a_bootstrap", 9, "sec", "sec_def14a_bootstrap"),
    _spec("sec_business_summary_bootstrap", 10, "sec", "sec_business_summary_bootstrap"),
    _spec("sec_insider_transactions_backfill", 11, "sec", "sec_insider_transactions_backfill"),
    _spec("sec_form3_ingest", 12, "sec", "sec_form3_ingest"),
    _spec("sec_8k_events_ingest", 13, "sec", "sec_8k_events_ingest"),
    _spec("sec_13f_quarterly_sweep", 14, "sec", "sec_13f_quarterly_sweep"),
    _spec("sec_n_port_ingest", 15, "sec", "sec_n_port_ingest"),
    _spec("ownership_observations_backfill", 16, "sec", "ownership_observations_backfill"),
    _spec("fundamentals_sync", 17, "sec", "fundamentals_sync"),
)


def get_bootstrap_stage_specs() -> tuple[StageSpec, ...]:
    """Public read-only accessor for the stage catalogue.

    The API endpoint that creates a new run imports this to seed
    ``bootstrap_stages`` rows. Lives in code (not the DB) because the
    catalogue is a deployable contract — adding / reordering stages
    is a code change with tests, not a runtime config change.
    """
    return _BOOTSTRAP_STAGE_SPECS


# ---------------------------------------------------------------------------
# Per-stage runner
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StageOutcome:
    stage_key: str
    success: bool
    error: str | None


def _run_one_stage(
    *,
    run_id: int,
    stage_key: str,
    job_name: str,
    invoker: Callable[[], None],
    database_url: str,
) -> _StageOutcome:
    """Execute one stage end-to-end with `JobLock` + bookkeeping.

    Exceptions inside the invoker are caught and recorded as
    ``error`` so the lane can proceed to the next stage. The only
    exceptions that escape this function are programmer errors
    (e.g. the bookkeeping query fails) — those propagate so the
    orchestrator surfaces them, but the lane runner catches a broad
    ``Exception`` to keep going.
    """
    with psycopg.connect(database_url) as conn:
        mark_stage_running(conn, run_id=run_id, stage_key=stage_key)
        conn.commit()

    try:
        with JobLock(database_url, job_name):
            invoker()
    except JobAlreadyRunning:
        message = (
            f"another instance of {job_name!r} holds the advisory lock; "
            "retry from the bootstrap panel after the other run completes"
        )
        with psycopg.connect(database_url) as conn:
            mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        logger.exception("bootstrap stage %s raised; lane continues", stage_key)
        with psycopg.connect(database_url) as conn:
            mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message)

    with psycopg.connect(database_url) as conn:
        mark_stage_success(conn, run_id=run_id, stage_key=stage_key)
        conn.commit()
    return _StageOutcome(stage_key=stage_key, success=True, error=None)


def _should_run(stage_status: str) -> bool:
    """Pre-check from the stage execution contract.

    On a fresh run every stage starts ``pending`` and runs. On a
    retry-failed pass, stages already in ``success`` are skipped so
    we touch only the affected stages.
    """
    return stage_status != "success"


# ---------------------------------------------------------------------------
# Lane runners
# ---------------------------------------------------------------------------


def _run_lane(
    *,
    run_id: int,
    lane_specs: Sequence[tuple[str, str, str, str, Callable[[], None]]],
    database_url: str,
    log_label: str,
) -> None:
    """Run a sequence of stages serially within a single lane.

    ``lane_specs`` is a sequence of
    ``(stage_key, job_name, lane, current_status, invoker)`` tuples.
    """
    logger.info("bootstrap %s lane: starting (%d stages)", log_label, len(lane_specs))
    for stage_key, job_name, _lane, status, invoker in lane_specs:
        if not _should_run(status):
            logger.info("bootstrap %s lane: skipping %s (already %s)", log_label, stage_key, status)
            continue
        outcome = _run_one_stage(
            run_id=run_id,
            stage_key=stage_key,
            job_name=job_name,
            invoker=invoker,
            database_url=database_url,
        )
        if outcome.success:
            logger.info("bootstrap %s lane: %s OK", log_label, stage_key)
        else:
            logger.warning("bootstrap %s lane: %s ERROR (%s)", log_label, stage_key, outcome.error)
    logger.info("bootstrap %s lane: done", log_label)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def run_bootstrap_orchestrator() -> None:
    """``_INVOKERS['bootstrap_orchestrator']`` — drive a queued run.

    Reads the latest ``bootstrap_runs`` row + its stages, runs Phase A
    sequentially, spawns two lane threads for Phase B in parallel,
    joins both, then finalises the run state.
    """
    # Lazy import: app.jobs.runtime imports app.services.bootstrap_orchestrator
    # via the orchestrator job invoker registration, and importing back the
    # other way at module load would be a circular import.
    from app.jobs.runtime import _INVOKERS

    database_url = settings.database_url

    with psycopg.connect(database_url) as conn:
        snapshot = read_latest_run_with_stages(conn)
    if snapshot is None:
        logger.error("bootstrap_orchestrator: no bootstrap_runs row found; nothing to do")
        return
    run_id = snapshot.run_id
    if snapshot.run_status != "running":
        logger.info(
            "bootstrap_orchestrator: latest run %d is %r; nothing to do",
            run_id,
            snapshot.run_status,
        )
        return

    by_lane: dict[str, list[tuple[str, str, str, str, Callable[[], None]]]] = {
        "init": [],
        "etoro": [],
        "sec": [],
    }
    for stage in sorted(snapshot.stages, key=lambda s: s.stage_order):
        invoker = _INVOKERS.get(stage.job_name)
        if invoker is None:
            logger.error(
                "bootstrap_orchestrator: stage %s has unknown job_name %r; marking error",
                stage.stage_key,
                stage.job_name,
            )
            with psycopg.connect(database_url) as conn:
                mark_stage_running(conn, run_id=run_id, stage_key=stage.stage_key)
                mark_stage_error(
                    conn,
                    run_id=run_id,
                    stage_key=stage.stage_key,
                    error_message=f"unknown job_name {stage.job_name!r}",
                )
                conn.commit()
            continue
        by_lane[stage.lane].append((stage.stage_key, stage.job_name, stage.lane, stage.status, invoker))

    logger.info(
        "bootstrap_orchestrator: run_id=%d phaseA=%d eToro=%d SEC=%d",
        run_id,
        len(by_lane["init"]),
        len(by_lane["etoro"]),
        len(by_lane["sec"]),
    )

    # Phase A — sequential init.
    _run_lane(
        run_id=run_id,
        lane_specs=by_lane["init"],
        database_url=database_url,
        log_label="init",
    )

    # If Phase A's only init stage errored, do not start Phase B.
    # Treat a missing post-init snapshot (e.g. transient DB
    # connectivity blip) as an init failure too — Phase B threads
    # would otherwise spawn against a run whose state we cannot
    # confirm, which would race the finalize step. Failing closed
    # here is the right default for a one-shot operator-driven run.
    init_failed = False
    with psycopg.connect(database_url) as conn:
        snap_after_init = read_latest_run_with_stages(conn)
    if snap_after_init is None:
        init_failed = True
        logger.warning(
            "bootstrap_orchestrator: post-init snapshot read returned None; "
            "treating as init failure and skipping Phase B"
        )
    else:
        for stage in snap_after_init.stages:
            if stage.lane == "init" and stage.status == "error":
                init_failed = True
                break

    if init_failed:
        logger.warning("bootstrap_orchestrator: Phase A init failed; skipping Phase B and finalising")
    else:
        etoro_thread = threading.Thread(
            target=_run_lane,
            kwargs={
                "run_id": run_id,
                "lane_specs": by_lane["etoro"],
                "database_url": database_url,
                "log_label": "eToro",
            },
            name="bootstrap-etoro-lane",
        )
        sec_thread = threading.Thread(
            target=_run_lane,
            kwargs={
                "run_id": run_id,
                "lane_specs": by_lane["sec"],
                "database_url": database_url,
                "log_label": "SEC",
            },
            name="bootstrap-sec-lane",
        )
        etoro_thread.start()
        sec_thread.start()
        etoro_thread.join()
        sec_thread.join()

    with psycopg.connect(database_url) as conn:
        terminal = finalize_run(conn, run_id=run_id)
    logger.info("bootstrap_orchestrator: run_id=%d finalised as %s", run_id, terminal)


# ---------------------------------------------------------------------------
# New invoker: bootstrap_filings_history_seed
# ---------------------------------------------------------------------------


# Historical depth window for the broad filings sweep. Two years
# matches what most operators want for first-install ranking; the
# practical depth is bounded by SEC submissions.json's inline
# ``recent`` block (typically ~12 months) since
# ``SecFilingsProvider.list_filings`` does not currently walk
# secondary submissions pages.
_FILINGS_HISTORY_DAYS = 730


def bootstrap_filings_history_seed() -> None:
    """``_INVOKERS['bootstrap_filings_history_seed']`` — broad filings sweep.

    Walks each CIK-mapped tradable instrument's submissions.json via
    ``refresh_filings`` with a 2-year window and no ``filing_types``
    filter, populating ``filing_events`` for every form type. The
    typed-form parsers later in the SEC lane (``sec_def14a_bootstrap``,
    ``sec_business_summary_bootstrap``,
    ``sec_insider_transactions_backfill``, etc.) read from
    ``filing_events`` and would otherwise no-op on a fresh DB.

    Bookkeeping: reuses ``_tracked_job`` from ``app.workers.scheduler``
    (the same context manager every scheduled job uses) so
    ``job_runs`` rows have a uniform shape regardless of whether the
    invoker was triggered by bootstrap dispatch or manual operator
    Run-now.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.filings import refresh_filings
    from app.workers.scheduler import _tracked_job  # type: ignore[attr-defined]

    with _tracked_job(JOB_BOOTSTRAP_FILINGS_HISTORY_SEED) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            cik_rows = conn.execute(
                """
                SELECT i.symbol, i.instrument_id::text
                  FROM external_identifiers ei
                  JOIN instruments i ON i.instrument_id = ei.instrument_id
                 WHERE ei.provider = 'sec'
                   AND ei.identifier_type = 'cik'
                   AND ei.is_primary = TRUE
                   AND i.is_tradable = TRUE
                """
            ).fetchall()

        if not cik_rows:
            logger.info("bootstrap_filings_history_seed: no CIK-mapped instruments; ensure daily_cik_refresh ran first")
            tracker.row_count = 0
            return

        instrument_ids = [row[1] for row in cik_rows]
        from_date = date.today() - timedelta(days=_FILINGS_HISTORY_DAYS)
        to_date = date.today()

        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = refresh_filings(
                provider=sec,
                provider_name="sec",
                identifier_type="cik",
                conn=conn,
                instrument_ids=instrument_ids,
                start_date=from_date,
                end_date=to_date,
                filing_types=None,
            )
        tracker.row_count = summary.filings_upserted
        logger.info(
            "bootstrap_filings_history_seed: instruments=%d filings_upserted=%d skipped=%d",
            summary.instruments_attempted,
            summary.filings_upserted,
            summary.instruments_skipped,
        )


# ---------------------------------------------------------------------------
# New invoker: sec_first_install_drain (zero-arg wrapper)
# ---------------------------------------------------------------------------


def _make_sec_http_get(sec_provider: object) -> Callable[[str, dict[str, str]], tuple[int, bytes]]:
    """Adapt ``SecFilingsProvider._http`` (a ``ResilientClient``) into
    an ``HttpGet = Callable[[str, dict[str, str]], tuple[int, bytes]]``.

    The drain / poll / rebuild call sites all consume this narrowed
    callable shape (see ``app/providers/implementations/sec_submissions.py``);
    the closure routes through the rate-limited shared client so SEC's
    10 req/s bucket is honoured.
    """

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        # ``_http`` is the ResilientClient wrapping the SEC httpx.Client
        # with the shared process-wide token bucket. ``.get(...)`` returns
        # a httpx.Response; the HttpGet contract is (status, body bytes).
        response = sec_provider._http.get(url, headers=headers)  # type: ignore[attr-defined]
        return response.status_code, response.content

    return _impl


def sec_first_install_drain_job() -> None:
    """``_INVOKERS['sec_first_install_drain']`` — zero-arg drain wrapper.

    The underlying ``run_first_install_drain`` takes an ``http_get``
    callable, ``follow_pagination``, etc. so it cannot be registered
    directly. This wrapper picks the bootstrap-default arguments
    (full universe scope, paginate enabled) and adapts the
    ``SecFilingsProvider._http`` ResilientClient into the
    ``HttpGet = Callable[[str, dict[str, str]], tuple[int, bytes]]``
    contract via ``_make_sec_http_get``.
    """
    from app.jobs.sec_first_install_drain import run_first_install_drain
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.workers.scheduler import _tracked_job  # type: ignore[attr-defined]

    with _tracked_job(JOB_SEC_FIRST_INSTALL_DRAIN) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_first_install_drain(
                conn,
                http_get=_make_sec_http_get(sec),  # type: ignore[arg-type]
                follow_pagination=True,
                use_bulk_zip=False,
                max_subjects=None,
            )
        tracker.row_count = stats.manifest_rows_upserted
        logger.info(
            "sec_first_install_drain: ciks_processed=%d skipped=%d manifest_rows=%d errors=%d",
            stats.ciks_processed,
            stats.ciks_skipped,
            stats.manifest_rows_upserted,
            stats.errors,
        )


__all__ = [
    "JOB_BOOTSTRAP_FILINGS_HISTORY_SEED",
    "JOB_BOOTSTRAP_ORCHESTRATOR",
    "JOB_DAILY_CIK_REFRESH",
    "JOB_DAILY_FINANCIAL_FACTS",
    "JOB_SEC_FIRST_INSTALL_DRAIN",
    "bootstrap_filings_history_seed",
    "get_bootstrap_stage_specs",
    "run_bootstrap_orchestrator",
    "sec_first_install_drain_job",
]


# Stage count assertion — pin so a future refactor that adds /
# removes a spec deliberately surfaces in code review and doesn't
# silently break the tests + frontend that hardcode "17 stages".
assert len(_BOOTSTRAP_STAGE_SPECS) == 17, (
    f"_BOOTSTRAP_STAGE_SPECS expected 17 stages, got {len(_BOOTSTRAP_STAGE_SPECS)}; "
    "update the spec, frontend, and stage_count tests in lockstep."
)
