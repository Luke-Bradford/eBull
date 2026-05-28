"""PR1c (#1064) — bespoke wrapper deletion + StageSpec.params extraction-equivalence.

Three behaviour groups:

1. **Deletion** — the three deleted symbols are gone from
   ``app.services.bootstrap_orchestrator``.
2. **StageSpec.params** — bootstrap stages 14, 15, 21 carry the
   exact params dict the deleted wrappers used to hardcode in their
   bodies. ``_resolve_dynamic_params`` materialises the dispatch-time
   13F cutoff sentinel.
3. **Promoted invoker contract** — ``filings_history_seed``,
   ``sec_first_install_drain``, ``sec_13f_quarterly_sweep`` accept the
   widened ``Mapping[str, Any]`` and honour the operator-tunable
   keys.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from app.services.bootstrap_orchestrator import (
    _BOOTSTRAP_13F_RECENCY_DAYS,
    _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF,
    _resolve_dynamic_params,
    get_bootstrap_stage_specs,
)

# ---------------------------------------------------------------------------
# 1. Deletion regression — wrappers + JOB_BOOTSTRAP_* constants gone.
# ---------------------------------------------------------------------------


class TestWrapperDeletion:
    """Three wrappers + their JOB_* constants must not be importable."""

    @pytest.mark.parametrize(
        "name",
        [
            "bootstrap_filings_history_seed",
            "sec_first_install_drain_job",
            "bootstrap_sec_13f_recent_sweep_job",
            "JOB_BOOTSTRAP_FILINGS_HISTORY_SEED",
            "JOB_BOOTSTRAP_SEC_13F_RECENT_SWEEP",
        ],
    )
    def test_deleted_symbol_is_gone(self, name: str) -> None:
        import app.services.bootstrap_orchestrator as orch

        assert not hasattr(orch, name), (
            f"PR1c (#1064) deleted {name!r}; reintroducing it would silently "
            "re-create the bespoke-wrapper duplication path."
        )


# ---------------------------------------------------------------------------
# 2. StageSpec.params — bootstrap stage 14 / 15 / 21 carry the right dicts.
# ---------------------------------------------------------------------------


class TestStageSpecParams:
    """Stages 14, 15, 21 populate the params dict the deleted wrappers hardcoded."""

    def _spec_by_key(self, key: str) -> Any:
        for spec in get_bootstrap_stage_specs():
            if spec.stage_key == key:
                return spec
        raise AssertionError(f"unknown stage_key {key!r}")

    def test_filings_history_seed_params_match_deleted_wrapper(self) -> None:
        from app.services.filings import SEC_INGEST_KEEP_FORMS

        spec = self._spec_by_key("filings_history_seed")
        assert spec.job_name == "filings_history_seed"
        assert spec.params["days_back"] == 730
        # Bootstrap stage carries the canonical three-tier allow-list as
        # an immutable tuple (frozen StageSpec compat).
        assert tuple(spec.params["filing_types"]) == tuple(sorted(SEC_INGEST_KEEP_FORMS))

    def test_sec_first_install_drain_params_match_deleted_wrapper(self) -> None:
        spec = self._spec_by_key("sec_first_install_drain")
        assert spec.job_name == "sec_first_install_drain"
        # Wrapper hardcoded ``max_subjects=None`` (full universe).
        # #1277 added ``use_bulk_zip=True`` (bootstrap-only via
        # JOB_INTERNAL_KEYS) — the dedicated sentinel
        # ``test_sec_first_install_drain_dispatches_use_bulk_zip_true``
        # in tests/test_bootstrap_orchestrator.py pins the flag's
        # presence; this assertion keeps tracking the full params dict
        # so a future spec edit that drops EITHER key is caught here.
        assert spec.params == {"max_subjects": None, "use_bulk_zip": True}

    def test_sec_13f_recent_sweep_params_match_deleted_wrapper(self) -> None:
        spec = self._spec_by_key("sec_13f_recent_sweep")
        # Stage 21 now dispatches the SCHEDULED ``sec_13f_quarterly_sweep`` body
        # with bootstrap-only overrides; the previous bespoke job name is gone.
        assert spec.job_name == "sec_13f_quarterly_sweep"
        # ``source_label`` rides as audit-only via JOB_INTERNAL_KEYS (PR1a).
        assert spec.params["source_label"] == "sec_edgar_13f_directory_bootstrap"
        # ``min_period_of_report`` is the dispatch-time sentinel — module-load
        # ``date.today()`` would freeze the cutoff in a long-lived process.
        assert spec.params["min_period_of_report"] == _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF


class TestResolveDynamicParams:
    """``_resolve_dynamic_params`` materialises the dispatch-time 13F cutoff."""

    def test_sentinel_resolves_to_today_minus_recency_days(self) -> None:
        out = _resolve_dynamic_params(
            {"min_period_of_report": _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF, "source_label": "x"}
        )
        assert out["min_period_of_report"] == date.today() - timedelta(days=_BOOTSTRAP_13F_RECENCY_DAYS)
        # Pass-through of non-sentinel keys.
        assert out["source_label"] == "x"

    def test_concrete_date_passes_through(self) -> None:
        fixed = date(2024, 1, 1)
        out = _resolve_dynamic_params({"min_period_of_report": fixed})
        assert out["min_period_of_report"] == fixed

    def test_no_min_period_pass_through(self) -> None:
        out = _resolve_dynamic_params({"days_back": 365})
        assert out == {"days_back": 365}

    def test_min_last_13f_hr_at_sentinel_resolves_to_utc_midnight(self) -> None:
        """#1010 — HR-recency cutoff materialises as UTC start-of-day
        ``today() - 380d``. Exact equality pins the invariant against
        future time-of-day regressions (Codex 1b)."""
        from datetime import UTC, datetime, time

        from app.services.bootstrap_orchestrator import (
            _PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF,
        )

        out = _resolve_dynamic_params({"min_last_13f_hr_at": _PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF})
        # Mirror the resolver: UTC date, not local ``date.today()``.
        # On a non-UTC dev host around the midnight boundary the two
        # can differ by one day (Codex 2 MEDIUM).
        expected = datetime.combine(
            datetime.now(tz=UTC).date() - timedelta(days=_BOOTSTRAP_13F_RECENCY_DAYS),
            time(0, 0),
            tzinfo=UTC,
        )
        assert out["min_last_13f_hr_at"] == expected

    def test_min_last_13f_hr_at_concrete_value_passes_through(self) -> None:
        """Concrete datetime ≠ sentinel → returned unchanged."""
        from datetime import UTC, datetime

        fixed = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        out = _resolve_dynamic_params({"min_last_13f_hr_at": fixed})
        assert out["min_last_13f_hr_at"] == fixed


# ---------------------------------------------------------------------------
# 3. Promoted invoker contract — params-aware bodies registered in _INVOKERS.
# ---------------------------------------------------------------------------


class TestPromotedInvokerRegistry:
    """Registered invokers are the new params-aware bodies, not adapters."""

    def test_filings_history_seed_registered_native(self) -> None:
        from app.jobs.runtime import _INVOKERS
        from app.workers import scheduler

        assert _INVOKERS["filings_history_seed"] is scheduler.filings_history_seed

    def test_sec_first_install_drain_registered_native(self) -> None:
        from app.jobs.runtime import _INVOKERS
        from app.workers import scheduler

        assert _INVOKERS["sec_first_install_drain"] is scheduler.sec_first_install_drain

    def test_sec_13f_quarterly_sweep_registered_native(self) -> None:
        """Migrated to native JobInvoker; no ``_adapt_zero_arg`` wrap."""
        from app.jobs.runtime import _INVOKERS
        from app.workers import scheduler

        assert _INVOKERS["sec_13f_quarterly_sweep"] is scheduler.sec_13f_quarterly_sweep


class TestSec13fSweepHonoursParams:
    """``sec_13f_quarterly_sweep`` body honours params dict — extraction equivalence."""

    def test_default_params_use_canonical_source_label_and_no_cutoff(self) -> None:
        """Empty params → standalone weekly sweep behaviour."""
        from unittest.mock import MagicMock, patch

        with (
            patch("app.workers.scheduler._tracked_job") as mock_tracker,
            patch("app.providers.implementations.sec_edgar.SecFilingsProvider"),
            patch("app.workers.scheduler.psycopg.connect"),
            patch("app.workers.scheduler.settings") as mock_settings,
            patch("app.services.institutional_holdings.list_directory_filer_ciks", return_value=[]),
            patch(
                "app.services.institutional_holdings.ingest_all_active_filers",
                return_value=[],
            ) as mock_ingest,
        ):
            mock_tracker.return_value.__enter__.return_value = MagicMock()
            mock_tracker.return_value.__exit__.return_value = False
            mock_settings.sec_user_agent = "test-agent"
            mock_settings.sec_13f_sweep_deadline_seconds = 3600
            mock_settings.database_url = "postgresql://stub/stub"

            from app.workers.scheduler import sec_13f_quarterly_sweep

            sec_13f_quarterly_sweep({})

        kwargs = mock_ingest.call_args.kwargs
        assert kwargs["source_label"] == "sec_edgar_13f_directory"
        assert kwargs["min_period_of_report"] is None

    def test_bootstrap_params_override_source_label_and_set_cutoff(self) -> None:
        """Bootstrap stage 21 dispatches with overrides → body honours both."""
        from unittest.mock import MagicMock, patch

        with (
            patch("app.workers.scheduler._tracked_job") as mock_tracker,
            patch("app.providers.implementations.sec_edgar.SecFilingsProvider"),
            patch("app.workers.scheduler.psycopg.connect"),
            patch("app.workers.scheduler.settings") as mock_settings,
            patch("app.services.institutional_holdings.list_directory_filer_ciks", return_value=[]),
            patch(
                "app.services.institutional_holdings.ingest_all_active_filers",
                return_value=[],
            ) as mock_ingest,
        ):
            mock_tracker.return_value.__enter__.return_value = MagicMock()
            mock_tracker.return_value.__exit__.return_value = False
            mock_settings.sec_user_agent = "test-agent"
            mock_settings.sec_13f_sweep_deadline_seconds = 3600
            mock_settings.database_url = "postgresql://stub/stub"

            cutoff = date(2025, 1, 1)
            from app.workers.scheduler import sec_13f_quarterly_sweep

            sec_13f_quarterly_sweep(
                {
                    "min_period_of_report": cutoff,
                    "source_label": "sec_edgar_13f_directory_bootstrap",
                }
            )

        kwargs = mock_ingest.call_args.kwargs
        assert kwargs["source_label"] == "sec_edgar_13f_directory_bootstrap"
        assert kwargs["min_period_of_report"] == cutoff

    def test_sweep_params_propagate_min_last_13f_hr_at_to_cohort_listing(self) -> None:
        """#1010 — ``sec_13f_quarterly_sweep`` body must thread
        ``min_last_13f_hr_at`` into ``list_directory_filer_ciks`` so
        the cohort filter actually runs."""
        from datetime import UTC, datetime
        from unittest.mock import MagicMock, patch

        with (
            patch("app.workers.scheduler._tracked_job") as mock_tracker,
            patch("app.providers.implementations.sec_edgar.SecFilingsProvider"),
            patch("app.workers.scheduler.psycopg.connect"),
            patch("app.workers.scheduler.settings") as mock_settings,
            patch(
                "app.services.institutional_holdings.list_directory_filer_ciks",
                return_value=[],
            ) as mock_list_ciks,
            patch(
                "app.services.institutional_holdings.ingest_all_active_filers",
                return_value=[],
            ),
        ):
            mock_tracker.return_value.__enter__.return_value = MagicMock()
            mock_tracker.return_value.__exit__.return_value = False
            mock_settings.sec_user_agent = "test-agent"
            mock_settings.sec_13f_sweep_deadline_seconds = 3600
            mock_settings.database_url = "postgresql://stub/stub"

            cutoff = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
            from app.workers.scheduler import sec_13f_quarterly_sweep

            sec_13f_quarterly_sweep({"min_last_13f_hr_at": cutoff})

        kwargs = mock_list_ciks.call_args.kwargs
        assert kwargs["min_last_13f_hr_at"] == cutoff

    def test_sweep_naive_datetime_min_last_13f_hr_at_gets_utc_tagged(self) -> None:
        """#1010 Codex 1b — naive datetime is tagged UTC defensively
        so psycopg never falls back to the server's local zone."""
        from datetime import UTC, datetime
        from unittest.mock import MagicMock, patch

        with (
            patch("app.workers.scheduler._tracked_job") as mock_tracker,
            patch("app.providers.implementations.sec_edgar.SecFilingsProvider"),
            patch("app.workers.scheduler.psycopg.connect"),
            patch("app.workers.scheduler.settings") as mock_settings,
            patch(
                "app.services.institutional_holdings.list_directory_filer_ciks",
                return_value=[],
            ) as mock_list_ciks,
            patch(
                "app.services.institutional_holdings.ingest_all_active_filers",
                return_value=[],
            ),
        ):
            mock_tracker.return_value.__enter__.return_value = MagicMock()
            mock_tracker.return_value.__exit__.return_value = False
            mock_settings.sec_user_agent = "test-agent"
            mock_settings.sec_13f_sweep_deadline_seconds = 3600
            mock_settings.database_url = "postgresql://stub/stub"

            naive = datetime(2025, 1, 1, 0, 0)  # noqa: DTZ001 — test of naive→UTC defensive tag
            from app.workers.scheduler import sec_13f_quarterly_sweep

            sec_13f_quarterly_sweep({"min_last_13f_hr_at": naive})

        kwargs = mock_list_ciks.call_args.kwargs
        passed = kwargs["min_last_13f_hr_at"]
        assert passed.tzinfo is UTC
        assert passed == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
