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
            # Phase 1.2: news stays on-demand (no-op stub pending a
            # concrete NewsProvider). Thesis left this set in #1919
            # PR-B — ``thesis_refresh`` is a registered scheduled job.
            "daily_news_refresh",
        }

        constants = {
            value for name, value in vars(scheduler).items() if name.startswith("JOB_") and isinstance(value, str)
        }
        registry_names = {job.name for job in SCHEDULED_JOBS}
        invoker_names = set(_INVOKERS.keys())
        unaccounted = constants - registry_names - invoker_names - INTERNAL_ONLY
        assert not unaccounted, f"JOB_* constants not in SCHEDULED_JOBS or _INVOKERS: {sorted(unaccounted)}"


class TestThesisRefreshRegistration:
    """#1919 PR-B — thesis_refresh scheduled hourly on its own lane."""

    def _job(self) -> scheduler.ScheduledJob:
        return next(job for job in SCHEDULED_JOBS if job.name == scheduler.JOB_THESIS_REFRESH)

    def test_registered_hourly_own_lane(self) -> None:
        job = self._job()
        assert job.cadence.kind == "hourly"
        assert job.source == "llm_thesis"
        # A boot catch-up would fire a multi-minute LLM batch on every
        # dev-stack restart.
        assert job.catch_up_on_boot is False
        assert job.prerequisite is scheduler._llm_provider_resolvable

    def test_manual_trigger_registered(self) -> None:
        from app.jobs.runtime import _INVOKERS

        assert scheduler.JOB_THESIS_REFRESH in _INVOKERS


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


class TestOwnershipObservationsBackfill:
    """#909: one-shot legacy → ownership_*_observations backfill.

    Distinct from the daily ``ownership_observations_sync`` repair sweep:
    that sweep only refreshes ``_current`` and assumes ``_observations`` is
    already populated. The backfill is what populates ``_observations`` from
    legacy typed tables in the first place. Both jobs need to coexist
    until the legacy tables are dropped post-#905.
    """

    def test_backfill_registered_in_scheduled_jobs(self) -> None:
        names = [job.name for job in SCHEDULED_JOBS]
        assert "ownership_observations_backfill" in names

    def test_backfill_cadence_weekly_sunday_03_00(self) -> None:
        # Weekly Sunday 03:00 UTC: between sec_def14a_bootstrap
        # (Sun 02:30) and the daily ownership_observations_sync repair
        # sweep (03:30). Backfill populates observations first; the
        # repair sweep then sees zero drift on Sundays.
        job = next(j for j in SCHEDULED_JOBS if j.name == "ownership_observations_backfill")
        assert job.cadence.kind == "weekly"
        assert job.cadence.weekday == 6  # Sunday
        assert job.cadence.hour == 3
        assert job.cadence.minute == 0

    def test_backfill_does_not_catch_up_on_boot(self) -> None:
        # Backfill is heavy (full legacy-table re-scan); the operator
        # owns triggering it after a fresh clone, not the boot path.
        job = next(j for j in SCHEDULED_JOBS if j.name == "ownership_observations_backfill")
        assert job.catch_up_on_boot is False

    def test_backfill_invokable_via_invokers(self) -> None:
        from app.jobs.runtime import _INVOKERS

        assert "ownership_observations_backfill" in _INVOKERS

    def test_repair_sweep_and_backfill_coexist(self) -> None:
        # Both jobs must exist. The sweep keeps _current in sync with
        # observations; the backfill populates observations from legacy.
        names = {job.name for job in SCHEDULED_JOBS}
        assert "ownership_observations_sync" in names
        assert "ownership_observations_backfill" in names


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

    @pytest.mark.parametrize("month", [0, 13])
    def test_yearly_invalid_month_raises(self, month: int) -> None:
        with pytest.raises(ValueError, match="yearly month"):
            Cadence.yearly(month=month, day=1, hour=5)

    @pytest.mark.parametrize("day", [0, 29])
    def test_yearly_invalid_day_raises(self, day: int) -> None:
        with pytest.raises(ValueError, match="yearly day"):
            Cadence.yearly(month=4, day=day, hour=5)

    def test_label_yearly(self) -> None:
        assert Cadence.yearly(month=4, day=1, hour=5).label == "yearly on Apr 1 at 05:00 UTC"


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

    # ---------- yearly (#1303) ----------

    def test_yearly_this_year_already_passed_rolls_to_next_year(self) -> None:
        # _NOW is 2026-04-07; Apr 1 has passed → 2027-04-01.
        result = compute_next_run(Cadence.yearly(month=4, day=1, hour=5), _NOW)
        assert result == datetime(2027, 4, 1, 5, 0, 0, tzinfo=UTC)

    def test_yearly_later_this_year(self) -> None:
        # _NOW is 2026-04-07; Jun 15 is still ahead this year.
        result = compute_next_run(Cadence.yearly(month=6, day=15, hour=5), _NOW)
        assert result == datetime(2026, 6, 15, 5, 0, 0, tzinfo=UTC)

    def test_yearly_strictly_after_now_on_boundary(self) -> None:
        on_boundary = datetime(2026, 4, 1, 5, 0, 0, tzinfo=UTC)
        result = compute_next_run(Cadence.yearly(month=4, day=1, hour=5), on_boundary)
        assert result == datetime(2027, 4, 1, 5, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# N-CEN classifier annual schedule (#1303)
# ---------------------------------------------------------------------------


class TestNcenClassifierSchedule:
    def test_registered_in_scheduled_jobs(self) -> None:
        names = {job.name for job in SCHEDULED_JOBS}
        assert "ncen_classifier_yearly" in names

    def test_cadence_yearly_apr_1_05_00(self) -> None:
        job = next(j for j in SCHEDULED_JOBS if j.name == "ncen_classifier_yearly")
        assert job.cadence.kind == "yearly"
        assert job.cadence.month == 4
        assert job.cadence.day == 1
        assert job.cadence.hour == 5
        assert job.cadence.minute == 0

    def test_runs_on_sec_rate_lane(self) -> None:
        job = next(j for j in SCHEDULED_JOBS if j.name == "ncen_classifier_yearly")
        assert job.source == "sec_rate"

    def test_catches_up_on_boot(self) -> None:
        # Annual cadence: a missed Apr-1 fire costs a year of classification
        # drift, so catch up when genuinely overdue (#1303).
        job = next(j for j in SCHEDULED_JOBS if j.name == "ncen_classifier_yearly")
        assert job.catch_up_on_boot is True

    def test_not_exempt_from_bootstrap_gate(self) -> None:
        # Heavy SEC sweep — must wait for bootstrap to complete.
        job = next(j for j in SCHEDULED_JOBS if j.name == "ncen_classifier_yearly")
        assert job.exempt_from_universal_bootstrap_gate is False

    def test_invokable_via_invokers(self) -> None:
        from app.jobs.runtime import _INVOKERS

        assert "ncen_classifier_yearly" in _INVOKERS

    def test_admin_process_lane_is_ownership(self) -> None:
        # Classifies institutional filers → ownership domain, like the
        # adjacent 13F/N-PORT filer jobs. Without the explicit mapping it
        # would default to 'ops' (Codex #1303 review).
        from app.services.processes.scheduled_adapter import _lane_for

        assert _lane_for("ncen_classifier_yearly") == "ownership"

    def test_trigger_for_yearly_sets_month(self) -> None:
        from app.jobs.runtime import _trigger_for

        trigger = _trigger_for(Cadence.yearly(month=4, day=1, hour=5))
        # APScheduler CronTrigger stringifies its fields; month=4 + day=1 pin it.
        rendered = str(trigger)
        assert "month='4'" in rendered
        assert "day='1'" in rendered
        assert "hour='5'" in rendered
