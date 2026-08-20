"""Tests for ``app.services.fundamentals.bootstrap`` (Stream A PR-C2
T1.2, #1233) — the bootstrap-only derivation entrypoint for S25
``fundamentals_sync``.

Three layers:

1. **Package-compat layer** — the package conversion from flat
   ``app/services/fundamentals.py`` → ``app/services/fundamentals/``
   directory must preserve every existing import. PR-C2 spec §1.7
   pinned option (b): existing body stays as ``__init__.py``, no
   re-export shim. The 45 import sites must continue to resolve.

2. **Pure-function layer** — invariants on the new
   ``FundamentalsSyncBootstrapResult`` dataclass + the constants.

3. **Integration layer** — the entrypoint against the real test DB
   with a small fixture cohort, verifying audit + normalize fire in
   order + the result telemetry is populated.
"""

from __future__ import annotations

import importlib
import inspect

import psycopg
import pytest

from app.services.fundamentals.bootstrap import (
    JOB_FUNDAMENTALS_SYNC_BOOTSTRAP,
    FundamentalsSyncBootstrapResult,
    fundamentals_sync_bootstrap,
    fundamentals_sync_bootstrap_invoker,
)


class TestPackageConversionPreservesImports:
    """Stream A PR-C2 spec §1.7 option (b) PINNED: existing
    ``fundamentals.py`` body moves verbatim to ``__init__.py`` so the
    45 import sites identified at spec-author time (§0.10 grep proof)
    continue to resolve without re-export shims."""

    def test_package_directory_exists(self) -> None:
        import app.services.fundamentals as pkg

        assert hasattr(pkg, "__path__"), "fundamentals must be a package, not a module"

    def test_bootstrap_submodule_resolves(self) -> None:
        bootstrap_mod = importlib.import_module("app.services.fundamentals.bootstrap")
        assert bootstrap_mod.JOB_FUNDAMENTALS_SYNC_BOOTSTRAP == "fundamentals_sync_bootstrap"

    @pytest.mark.parametrize(
        "name",
        [
            # Public surface — chosen sample from the 45-import set
            # captured in spec §0.10. If any of these fails to resolve,
            # the package conversion broke the public API contract.
            "refresh_financial_facts",
            "FactsRefreshSummary",
            "normalize_financial_periods",
            "NormalizationSummary",
            "plan_refresh",
            "execute_refresh",
            "RefreshPlan",
            "RefreshOutcome",
            "upsert_facts_for_instrument",
            "upsert_concept_catalog",
            "start_ingestion_run",
            "finish_ingestion_run",
            "persist_cik_timing",
        ],
    )
    def test_public_name_resolves_from_package_root(self, name: str) -> None:
        """Each public name MUST be importable as
        ``from app.services.fundamentals import <name>`` post-PR-C2.
        Regression sentinel — a future refactor that splits the
        package without re-exporting these names would break every
        existing caller."""
        from app.services import fundamentals as pkg

        assert hasattr(pkg, name), f"name {name!r} must be importable from app.services.fundamentals"


class TestFundamentalsSyncBootstrapResultInvariants:
    """Telemetry dataclass shape pin — every field counted in the
    audit-record path (``_record_bootstrap_audit_row``) must exist
    with int default 0 so a no-op invocation produces a well-formed
    audit row."""

    def test_dataclass_defaults_to_zero_counters(self) -> None:
        result = FundamentalsSyncBootstrapResult()
        assert result.audit_analysable == 0
        assert result.audit_insufficient == 0
        assert result.audit_fpi == 0
        assert result.audit_no_primary_sec_cik == 0
        assert result.audit_total_updated == 0
        # Data-integrity signal from coverage.audit_all_instruments —
        # added in pre-push review to surface Chunk B regression count
        # (Reviewer IMPORTANT).
        assert result.audit_null_anomalies == 0
        assert result.normalize_instruments_processed == 0
        assert result.normalize_periods_raw_upserted == 0
        assert result.normalize_periods_canonical_upserted == 0

    def test_job_name_constant_matches_invoker_registration(self) -> None:
        """The constant + the actual job_name used in
        ``_BOOTSTRAP_STAGE_SPECS`` MUST match — otherwise the
        orchestrator dispatch resolves to a missing ``_INVOKERS``
        entry and the stage hangs forever."""
        from app.services.bootstrap_orchestrator import _BOOTSTRAP_STAGE_SPECS

        fundamentals_spec = next(
            (s for s in _BOOTSTRAP_STAGE_SPECS if s.stage_key == "fundamentals_sync"),
            None,
        )
        assert fundamentals_spec is not None
        assert fundamentals_spec.job_name == JOB_FUNDAMENTALS_SYNC_BOOTSTRAP

    def test_invoker_accepts_optional_params_mapping(self) -> None:
        """JobInvoker contract (PR1b-2 #1064): invokers accept a
        params Mapping kwarg. The bootstrap invoker discards it but
        MUST accept it to satisfy the orchestrator dispatch signature."""
        sig = inspect.signature(fundamentals_sync_bootstrap_invoker)
        assert len(sig.parameters) == 1
        param = next(iter(sig.parameters.values()))
        assert param.default is None, "invoker param must default to None for zero-arg dispatch"


class TestFundamentalsSyncBootstrapIntegration:
    """End-to-end: the entrypoint against the real test DB. We can't
    seed a full bootstrap state in a unit test, but we CAN verify
    that:
      * the function runs cleanly against a DB with no
        ``financial_facts_raw`` rows (audit + normalize are both
        empty no-ops);
      * the returned result is a populated dataclass with the
        expected zero-counter shape;
      * the function is idempotent (second invocation produces the
        same result as the first)."""

    @pytest.mark.integration
    def test_empty_db_runs_cleanly_with_zero_counters(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM financial_facts_raw")
        ebull_test_conn.commit()

        result = fundamentals_sync_bootstrap(ebull_test_conn)

        assert isinstance(result, FundamentalsSyncBootstrapResult)
        assert result.normalize_instruments_processed == 0
        assert result.normalize_periods_raw_upserted == 0
        assert result.normalize_periods_canonical_upserted == 0
        # audit_* fields reflect the cohort being audited — could be
        # zero or positive depending on test-DB state; assert non-
        # negative not exact value (DB-global counter).
        assert result.audit_analysable >= 0
        assert result.audit_insufficient >= 0
        assert result.audit_fpi >= 0
        assert result.audit_no_primary_sec_cik >= 0

    @pytest.mark.integration
    def test_idempotent_on_repeat_invocation(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Second call returns equivalent telemetry (audit
        re-classifies the same cohort; normalize has no new facts
        so processes 0 instruments)."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM financial_facts_raw")
        ebull_test_conn.commit()

        first = fundamentals_sync_bootstrap(ebull_test_conn)
        second = fundamentals_sync_bootstrap(ebull_test_conn)

        # Audit cohort is stable across runs on the same DB.
        assert second.audit_analysable == first.audit_analysable
        assert second.audit_insufficient == first.audit_insufficient
        assert second.audit_fpi == first.audit_fpi
        assert second.audit_no_primary_sec_cik == first.audit_no_primary_sec_cik
        # Normalize is fully idempotent on identical input.
        assert second.normalize_instruments_processed == first.normalize_instruments_processed
        assert second.normalize_periods_raw_upserted == 0
        assert second.normalize_periods_canonical_upserted == 0
