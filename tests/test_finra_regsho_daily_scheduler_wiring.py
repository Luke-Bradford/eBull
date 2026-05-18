"""Wiring invariants for the FINRA RegSHO daily short volume refresh
(G6/#916 Phase 6 PR 12).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from app.jobs.runtime import _INVOKERS
from app.jobs.sources import MANUAL_TRIGGER_JOB_SOURCES, source_for
from app.workers.scheduler import (
    JOB_FINRA_REGSHO_DAILY_REFRESH,
    SCHEDULED_JOBS,
    _bootstrap_complete,
    finra_regsho_daily_refresh,
)


def test_job_name_constant_exported() -> None:
    assert JOB_FINRA_REGSHO_DAILY_REFRESH == "finra_regsho_daily_refresh"


def test_scheduled_jobs_contains_regsho_entry() -> None:
    matches = [j for j in SCHEDULED_JOBS if j.name == JOB_FINRA_REGSHO_DAILY_REFRESH]
    assert len(matches) == 1


def test_regsho_scheduled_job_cadence_and_gating() -> None:
    job = next(j for j in SCHEDULED_JOBS if j.name == JOB_FINRA_REGSHO_DAILY_REFRESH)
    assert job.source == "finra"
    assert job.cadence.kind == "daily"
    assert job.cadence.hour == 23
    assert job.cadence.minute == 0
    assert job.catch_up_on_boot is False
    assert job.prerequisite is _bootstrap_complete
    assert job.exempt_from_universal_bootstrap_gate is False
    # v1 manual-trigger surface is zero-param.
    assert job.params_metadata == ()


def test_invoker_registered_in_runtime() -> None:
    invoker = _INVOKERS[JOB_FINRA_REGSHO_DAILY_REFRESH]
    assert callable(invoker)
    assert invoker.__wrapped__ is finra_regsho_daily_refresh  # type: ignore[attr-defined]


def test_source_for_job_name_resolves_to_finra() -> None:
    assert source_for(JOB_FINRA_REGSHO_DAILY_REFRESH) == "finra"


def test_manual_trigger_sources_entry_present() -> None:
    assert MANUAL_TRIGGER_JOB_SOURCES[JOB_FINRA_REGSHO_DAILY_REFRESH] == "finra"


def test_finra_lane_now_has_two_jobs() -> None:
    """v1: both FINRA jobs live on the `finra` lane (disjoint from
    sec_rate). After PR 12 the lane has exactly two members.
    """
    finra_jobs = {j.name for j in SCHEDULED_JOBS if j.source == "finra"}
    assert finra_jobs >= {"finra_short_interest_refresh", "finra_regsho_daily_refresh"}


def test_shim_drives_tracker_row_count() -> None:
    """Direct shim invocation: mock ``run_finra_regsho_daily_refresh`` +
    ``psycopg.connect``; assert the shim sets ``tracker.row_count`` to
    the returned stats' ``total_upserted``. Catches the
    ``_tracked_job``-as-decorator misuse that Codex 1b r1 HIGH caught
    in the plan.
    """
    from app.services.finra_regsho_ingest import RegShoDailyIngestStats

    fake_stats = MagicMock()
    fake_stats.total_upserted = 42
    fake_stats.failed_files = 0
    fake_stats.daily_files = [
        RegShoDailyIngestStats(trade_date=date(2026, 5, 15), prefix="CNMS", rows_upserted=42),
    ]

    fake_conn_cm = MagicMock()
    fake_conn_cm.__enter__.return_value = MagicMock()
    fake_conn_cm.__exit__.return_value = False

    fake_tracker = MagicMock()
    fake_tracker_cm = MagicMock()
    fake_tracker_cm.__enter__.return_value = fake_tracker
    fake_tracker_cm.__exit__.return_value = False

    with (
        patch("app.workers.scheduler._tracked_job", return_value=fake_tracker_cm) as patched_tracked,
        patch("app.workers.scheduler.psycopg.connect", return_value=fake_conn_cm),
        patch(
            "app.jobs.finra_regsho_daily_refresh.run_finra_regsho_daily_refresh",
            return_value=fake_stats,
        ) as patched_run,
    ):
        finra_regsho_daily_refresh()

    patched_tracked.assert_called_once_with(JOB_FINRA_REGSHO_DAILY_REFRESH)
    patched_run.assert_called_once()
    assert fake_tracker.row_count == 42
