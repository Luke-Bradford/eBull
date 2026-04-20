"""Unit test for the partial-XBRL-failure surfacing added in #353.

`execute_refresh` returns a `RefreshOutcome` whose `.failed` attribute
lists per-CIK failures without raising. Before #353, `daily_financial_facts()`
would merely log the failure count, so a day where 20% of SEC pulls crashed
left the tracked job status='success' and Admin health green.

The fix raises `RuntimeError` after the cascade + commits, so successful
CIKs' facts, rankings, and retry-queue writes all land first, but the job
itself fails and `fundamentals_sync` phase 1 sees the failure.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services.fundamentals import RefreshOutcome, RefreshPlan


def _install_tracked_job_cm(mod: object) -> MagicMock:
    """Replace _tracked_job with a context manager yielding a tracker mock."""
    tracker = MagicMock()
    tracker.row_count = 0
    tracked_cm = MagicMock()
    tracked_cm.__enter__.return_value = tracker
    tracked_cm.__exit__.return_value = None
    return tracked_cm


def test_daily_financial_facts_raises_when_outcome_has_failures() -> None:
    """With `outcome.failed` non-empty, the job must raise so
    fundamentals_sync phase 1 records a failure. The raise lands AFTER
    the facts commit, so successful CIKs' writes are preserved."""
    from app.workers import scheduler

    # Plan: one seed + one refresh. Outcome: one succeeded refresh + one
    # XBRL failure in the seed.
    plan = RefreshPlan(
        seeds=["0000000001"],
        refreshes=[("0000000002", "2026-04-18")],
        submissions_only_advances=[],
    )
    outcome = RefreshOutcome(
        seeded=0,
        refreshed=1,
        submissions_advanced=0,
        failed=[("0000000001", "HTTPError")],
    )

    conn = MagicMock()
    # Phase 2 lookup returns one instrument for the refreshed CIK.
    conn.execute.return_value.fetchall.return_value = [(42,)]

    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = conn
    fake_connect_cm.__exit__.return_value = None

    tracked_cm = _install_tracked_job_cm(scheduler)

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://stub/"
    stub_settings.sec_user_agent = "test"
    # No anthropic key → cascade block skipped, so the partial-failure raise
    # is the only thing standing between success and RuntimeError.
    stub_settings.anthropic_api_key = None

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job", return_value=tracked_cm),
        patch.object(scheduler.psycopg, "connect", return_value=fake_connect_cm),
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fundamentals_cls,
        patch("app.services.fundamentals.plan_refresh", return_value=plan),
        patch("app.services.fundamentals.execute_refresh", return_value=outcome),
        patch(
            "app.services.fundamentals.normalize_financial_periods",
            return_value=MagicMock(
                instruments_processed=1,
                periods_raw_upserted=0,
                periods_canonical_upserted=0,
            ),
        ),
    ):
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fundamentals_cls.return_value.__enter__.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="xbrl_failed=1"):
            scheduler.daily_financial_facts()

    # Committed state: facts for successful CIK + normalization landed
    # BEFORE the raise. Verify by counting commits: one after Phase 2
    # (normalization), zero after cascade (no anthropic key).
    assert conn.commit.call_count >= 1


def test_daily_financial_facts_raises_when_planner_has_skipped_ciks() -> None:
    """Planner-phase skips land in `plan.failed_plan_ciks` (separate from
    the executor's `outcome.failed`). Without surfacing them, a day where
    every submissions.json fetch returns None leaves the executor with
    nothing to do — `outcome.failed` is empty but the upstream fetch is
    broken and Admin health would still be green."""
    from app.workers import scheduler

    plan = RefreshPlan(
        seeds=[],
        refreshes=[],
        submissions_only_advances=[],
        failed_plan_ciks=["0000000009"],
    )
    outcome = RefreshOutcome(seeded=0, refreshed=0, submissions_advanced=0, failed=[])

    conn = MagicMock()
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = conn
    fake_connect_cm.__exit__.return_value = None

    tracked_cm = _install_tracked_job_cm(scheduler)

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://stub/"
    stub_settings.sec_user_agent = "test"
    stub_settings.anthropic_api_key = None

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job", return_value=tracked_cm),
        patch.object(scheduler.psycopg, "connect", return_value=fake_connect_cm),
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fundamentals_cls,
        patch("app.services.fundamentals.plan_refresh", return_value=plan),
        patch("app.services.fundamentals.execute_refresh", return_value=outcome),
    ):
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fundamentals_cls.return_value.__enter__.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="planner_skipped=1"):
            scheduler.daily_financial_facts()


def test_daily_financial_facts_combines_xbrl_and_cascade_failures() -> None:
    """When both channels fail, the single combined raise names both so
    diagnostics don't drop one signal. Previously the cascade-raise
    fired first and masked the XBRL failure."""
    from app.workers import scheduler

    plan = RefreshPlan(
        seeds=[],
        refreshes=[("0000000002", "2026-04-18")],
        submissions_only_advances=[],
    )
    outcome = RefreshOutcome(
        seeded=0,
        refreshed=1,
        submissions_advanced=0,
        failed=[("0000000001", "HTTPError")],
    )

    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [(42,)]
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = conn
    fake_connect_cm.__exit__.return_value = None

    tracked_cm = _install_tracked_job_cm(scheduler)

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://stub/"
    stub_settings.sec_user_agent = "test"
    stub_settings.anthropic_api_key = "sk-ant-stub"  # enable cascade

    # Stub a cascade that reports one per-instrument failure.
    cascade_outcome = MagicMock(
        instruments_considered=1,
        retries_drained=0,
        thesis_refreshed=0,
        rankings_recomputed=False,
        failed=[(42, "RuntimeError")],
    )

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job", return_value=tracked_cm),
        patch.object(scheduler.psycopg, "connect", return_value=fake_connect_cm),
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fundamentals_cls,
        patch("app.services.fundamentals.plan_refresh", return_value=plan),
        patch("app.services.fundamentals.execute_refresh", return_value=outcome),
        patch(
            "app.services.fundamentals.normalize_financial_periods",
            return_value=MagicMock(
                instruments_processed=1,
                periods_raw_upserted=0,
                periods_canonical_upserted=0,
            ),
        ),
        patch("app.services.refresh_cascade.cascade_refresh", return_value=cascade_outcome),
        patch("app.services.refresh_cascade.changed_instruments_from_outcome", return_value=[42]),
        patch("app.workers.scheduler.anthropic.Anthropic"),
    ):
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fundamentals_cls.return_value.__enter__.return_value = MagicMock()

        with pytest.raises(RuntimeError) as excinfo:
            scheduler.daily_financial_facts()
        # Single raise names BOTH channels — no signal masking.
        assert "xbrl_failed=1" in str(excinfo.value)
        assert "cascade_failed=1" in str(excinfo.value)


def test_daily_financial_facts_no_raise_when_outcome_clean() -> None:
    """Clean outcome (no failures) must not raise — this is the happy
    path and the regression guard against a too-eager raise condition."""
    from app.workers import scheduler

    plan = RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])
    outcome = RefreshOutcome(seeded=0, refreshed=0, submissions_advanced=0, failed=[])

    conn = MagicMock()
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = conn
    fake_connect_cm.__exit__.return_value = None

    tracked_cm = _install_tracked_job_cm(scheduler)

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://stub/"
    stub_settings.sec_user_agent = "test"
    stub_settings.anthropic_api_key = None

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job", return_value=tracked_cm),
        patch.object(scheduler.psycopg, "connect", return_value=fake_connect_cm),
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fundamentals_cls,
        patch("app.services.fundamentals.plan_refresh", return_value=plan),
        patch("app.services.fundamentals.execute_refresh", return_value=outcome),
    ):
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fundamentals_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.daily_financial_facts()  # must not raise
