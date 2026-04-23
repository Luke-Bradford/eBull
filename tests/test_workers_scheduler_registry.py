"""Tests for the declared schedule registry and ``compute_next_run``.

Scope:
  - Registry shape: every entry has a unique name and a valid cadence.
  - Registry / ``_tracked_job`` consistency: every name constant referenced
    by a job function is also in ``SCHEDULED_JOBS`` (no drift).
  - ``compute_next_run`` semantics for hourly / daily / weekly cadences,
    including the strictly-greater-than-now boundary.

These are pure-Python tests — no DB, no network.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.workers import scheduler
from app.workers.scheduler import (
    SCHEDULED_JOBS,
    Cadence,
    compute_next_run,
)

# ---------------------------------------------------------------------------
# Registry shape
# ---------------------------------------------------------------------------


class TestRegistryShape:
    def test_names_are_unique(self) -> None:
        names = [job.name for job in SCHEDULED_JOBS]
        assert len(names) == len(set(names)), f"duplicate job names: {names}"

    def test_descriptions_are_non_empty(self) -> None:
        for job in SCHEDULED_JOBS:
            assert job.description.strip(), f"job {job.name} has empty description"

    def test_every_job_constant_is_scheduled_or_on_demand(self) -> None:
        # Every JOB_* constant must be either in SCHEDULED_JOBS, _INVOKERS,
        # or the internal-only set. Internal-only constants name jobs that
        # are called from within another scheduled job's body (via
        # _tracked_job for the audit row) but are not themselves exposed
        # to the scheduler or manual trigger UI.
        from app.jobs.runtime import _INVOKERS

        # Internal-only job names — called from within fundamentals_sync
        # (Chunk 3 of the 2026-04-19 research-tool refocus). They retain
        # JOB_* constants because the legacy function bodies still write
        # job_runs rows under those names for the audit trail.
        INTERNAL_ONLY = {
            "daily_cik_refresh",
            "daily_financial_facts",
            # Phase 1.2: news + thesis are on-demand; both functions
            # remain as internal helpers (news is a no-op stub pending
            # a concrete NewsProvider; thesis body is called from the
            # POST /instruments/{symbol}/thesis endpoint via
            # generate_thesis).
            "daily_news_refresh",
            "daily_thesis_refresh",
        }

        constants = {
            value for name, value in vars(scheduler).items() if name.startswith("JOB_") and isinstance(value, str)
        }
        registry_names = {job.name for job in SCHEDULED_JOBS}
        invoker_names = set(_INVOKERS.keys())
        unaccounted = constants - registry_names - invoker_names - INTERNAL_ONLY
        assert not unaccounted, f"JOB_* constants not in SCHEDULED_JOBS or _INVOKERS: {sorted(unaccounted)}"


# ---------------------------------------------------------------------------
# Daily candle job registration
# ---------------------------------------------------------------------------


class TestOrchestratorTriggers:
    """Phase 4: SCHEDULED_JOBS now carries two orchestrator triggers in
    place of the 12 legacy cron entries that mapped to non-empty
    JOB_TO_LAYERS values. daily_candle_refresh still exists as an
    _INVOKERS entry for POST /jobs/{name}/run, but no longer has its
    own cron schedule."""

    def test_orchestrator_full_sync_registered(self) -> None:
        names = [job.name for job in SCHEDULED_JOBS]
        assert "orchestrator_full_sync" in names

    def test_orchestrator_full_sync_cadence_daily_03_utc(self) -> None:
        job = next(j for j in SCHEDULED_JOBS if j.name == "orchestrator_full_sync")
        assert job.cadence.kind == "daily"
        assert job.cadence.hour == 3
        assert job.cadence.minute == 0

    def test_orchestrator_high_frequency_registered_every_5min(self) -> None:
        job = next(j for j in SCHEDULED_JOBS if j.name == "orchestrator_high_frequency_sync")
        assert job.cadence.kind == "every_n_minutes"
        assert job.cadence.interval_minutes == 5

    def test_daily_candle_refresh_still_invokable_via_invokers(self) -> None:
        from app.jobs.runtime import _INVOKERS

        assert "daily_candle_refresh" in _INVOKERS

    def test_daily_candle_refresh_no_longer_scheduled(self) -> None:
        names = [job.name for job in SCHEDULED_JOBS]
        assert "daily_candle_refresh" not in names


class TestFundamentalsSyncCadence:
    """fundamentals_sync cadence moved weekly→daily 02:30 UTC under #414.

    SEC publishes the nightly XBRL update around 22:00 ET (02:00 UTC).
    The previous Monday 05:00 UTC window missed the natural incremental
    and amplified seed lag (a missed Monday meant week-long staleness).
    Daily 02:30 UTC lands ~30 min after the publish window so new
    filings ingest the same night.
    """

    def test_fundamentals_sync_is_daily_02_30(self) -> None:
        job = next(j for j in SCHEDULED_JOBS if j.name == "fundamentals_sync")
        assert job.cadence.kind == "daily"
        assert job.cadence.hour == 2
        assert job.cadence.minute == 30

    def test_fundamentals_sync_does_not_catch_up_on_boot(self) -> None:
        job = next(j for j in SCHEDULED_JOBS if j.name == "fundamentals_sync")
        assert job.catch_up_on_boot is False


# ---------------------------------------------------------------------------
# Cadence validators
# ---------------------------------------------------------------------------


class TestCadenceValidators:
    @pytest.mark.parametrize("interval", [0, 31, 7])
    def test_every_n_minutes_invalid_raises(self, interval: int) -> None:
        with pytest.raises(ValueError, match="every_n_minutes"):
            Cadence.every_n_minutes(interval=interval)

    def test_every_n_minutes_label(self) -> None:
        assert Cadence.every_n_minutes(interval=5).label == "every 5m"

    @pytest.mark.parametrize("minute", [-1, 60, 75])
    def test_hourly_invalid_minute_raises(self, minute: int) -> None:
        with pytest.raises(ValueError, match="hourly minute"):
            Cadence.hourly(minute=minute)

    @pytest.mark.parametrize("hour", [-1, 24])
    def test_daily_invalid_hour_raises(self, hour: int) -> None:
        with pytest.raises(ValueError, match="daily hour"):
            Cadence.daily(hour=hour)

    @pytest.mark.parametrize("weekday", [-1, 7])
    def test_weekly_invalid_weekday_raises(self, weekday: int) -> None:
        with pytest.raises(ValueError, match="weekly weekday"):
            Cadence.weekly(weekday=weekday, hour=0)

    def test_label_hourly(self) -> None:
        assert Cadence.hourly(minute=5).label == "hourly at :05 UTC"

    def test_label_daily(self) -> None:
        assert Cadence.daily(hour=6, minute=30).label == "daily at 06:30 UTC"

    def test_label_weekly(self) -> None:
        # weekday=0 is Monday
        assert Cadence.weekly(weekday=0, hour=5).label == "weekly on Mon at 05:00 UTC"


# ---------------------------------------------------------------------------
# compute_next_run
# ---------------------------------------------------------------------------


_NOW = datetime(2026, 4, 7, 12, 30, 0, tzinfo=UTC)  # Tuesday 12:30 UTC


class TestComputeNextRun:
    def test_requires_aware_now(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            compute_next_run(Cadence.hourly(), datetime(2026, 4, 7, 12, 0, 0))

    # ---------- hourly ----------

    def test_hourly_next_is_within_one_hour(self) -> None:
        # cadence at minute 5; now is 12:30 → next is 13:05
        result = compute_next_run(Cadence.hourly(minute=5), _NOW)
        assert result == datetime(2026, 4, 7, 13, 5, 0, tzinfo=UTC)

    def test_hourly_strictly_after_now_when_on_boundary(self) -> None:
        # now is exactly 12:30; cadence at minute 30 → next is 13:30, not 12:30
        on_boundary = datetime(2026, 4, 7, 12, 30, 0, tzinfo=UTC)
        result = compute_next_run(Cadence.hourly(minute=30), on_boundary)
        assert result == datetime(2026, 4, 7, 13, 30, 0, tzinfo=UTC)

    def test_hourly_minute_after_current(self) -> None:
        # now is 12:30; cadence at minute 45 → next is 12:45 (same hour)
        result = compute_next_run(Cadence.hourly(minute=45), _NOW)
        assert result == datetime(2026, 4, 7, 12, 45, 0, tzinfo=UTC)

    # ---------- daily ----------

    def test_daily_today_in_future(self) -> None:
        # now 12:30; cadence at 18:00 → today 18:00
        result = compute_next_run(Cadence.daily(hour=18), _NOW)
        assert result == datetime(2026, 4, 7, 18, 0, 0, tzinfo=UTC)

    def test_daily_today_already_passed_rolls_to_tomorrow(self) -> None:
        # now 12:30; cadence at 06:00 → tomorrow 06:00
        result = compute_next_run(Cadence.daily(hour=6), _NOW)
        assert result == datetime(2026, 4, 8, 6, 0, 0, tzinfo=UTC)

    def test_daily_strictly_greater_on_boundary(self) -> None:
        on_boundary = datetime(2026, 4, 7, 6, 0, 0, tzinfo=UTC)
        result = compute_next_run(Cadence.daily(hour=6), on_boundary)
        assert result == datetime(2026, 4, 8, 6, 0, 0, tzinfo=UTC)

    # ---------- weekly ----------

    def test_weekly_later_this_week(self) -> None:
        # _NOW is Tuesday; ask for Friday (weekday=4) at 09:00
        result = compute_next_run(Cadence.weekly(weekday=4, hour=9), _NOW)
        assert result == datetime(2026, 4, 10, 9, 0, 0, tzinfo=UTC)

    def test_weekly_earlier_in_week_rolls_to_next_week(self) -> None:
        # _NOW is Tuesday; ask for Monday (weekday=0) at 05:00 → next Monday
        result = compute_next_run(Cadence.weekly(weekday=0, hour=5), _NOW)
        # Next Monday after 2026-04-07 (Tue) is 2026-04-13.
        assert result == datetime(2026, 4, 13, 5, 0, 0, tzinfo=UTC)

    def test_weekly_same_day_strict(self) -> None:
        # _NOW is Tuesday 12:30; ask for Tuesday at 09:00 → already passed → next week
        result = compute_next_run(Cadence.weekly(weekday=1, hour=9), _NOW)
        assert result == datetime(2026, 4, 14, 9, 0, 0, tzinfo=UTC)

    def test_weekly_same_day_future(self) -> None:
        # _NOW is Tuesday 12:30; ask for Tuesday at 18:00 → today
        result = compute_next_run(Cadence.weekly(weekday=1, hour=18), _NOW)
        assert result == datetime(2026, 4, 7, 18, 0, 0, tzinfo=UTC)

    def test_returns_utc_when_now_is_other_offset(self) -> None:
        # 12:30 UTC == 13:30 in +01:00; the cadence still resolves in UTC.
        plus_one = _NOW.astimezone(tz=__import__("datetime").timezone(timedelta(hours=1)))
        result = compute_next_run(Cadence.daily(hour=18), plus_one)
        assert result == datetime(2026, 4, 7, 18, 0, 0, tzinfo=UTC)
