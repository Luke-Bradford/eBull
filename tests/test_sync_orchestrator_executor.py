"""Tests for sync orchestrator executor.

DB-backed paths (_start_sync_run, _safe_run_and_finalize end-to-end)
use settings.database_url — the test DB. Pure-logic paths use mocks.
"""

from __future__ import annotations

from collections.abc import Mapping
from unittest.mock import MagicMock, patch

import pytest

from app.services.sync_orchestrator import executor
from app.services.sync_orchestrator.types import (
    ExecutionPlan,
    LayerOutcome,
    LayerPlan,
)


def _lp(
    name: str,
    emits: tuple[str, ...],
    deps: tuple[str, ...] = (),
    is_blocking: bool = True,
) -> LayerPlan:
    return LayerPlan(
        name=name,
        emits=emits,
        reason="stale",
        dependencies=deps,
        is_blocking=is_blocking,
        estimated_items=0,
    )


class TestBlockingDependencyFailed:
    def test_failed_blocking_dep_returns_skip_reason(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        upstream: Mapping[str, LayerOutcome] = {"universe": LayerOutcome.FAILED}
        # Patch LAYERS to mark universe blocking.
        with patch.object(
            executor,
            "_blocking_dependency_failed",
            wraps=executor._blocking_dependency_failed,
        ):
            reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is not None
        assert "universe" in reason
        assert "failed" in reason

    def test_dep_skipped_on_blocking_dep_returns_reason(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        upstream: Mapping[str, LayerOutcome] = {"universe": LayerOutcome.DEP_SKIPPED}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is not None
        assert "dep_skipped" in reason

    def test_prereq_skip_on_blocking_dep_returns_reason(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        upstream: Mapping[str, LayerOutcome] = {"universe": LayerOutcome.PREREQ_SKIP}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is not None
        assert "prerequisite" in reason

    def test_partial_on_blocking_dep_does_not_block(self) -> None:
        """PARTIAL is explicitly 'some items worked' — downstream runs."""
        plan = _lp("scoring", ("scoring",), deps=("thesis",))
        upstream: Mapping[str, LayerOutcome] = {"thesis": LayerOutcome.PARTIAL}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is None

    def test_failed_non_blocking_dep_does_not_block(self) -> None:
        """news is non-blocking; thesis can still run with news=FAILED."""
        plan = _lp(
            "daily_thesis_refresh",
            ("thesis",),
            deps=("fundamentals", "financial_normalization", "news"),
        )
        upstream: Mapping[str, LayerOutcome] = {
            "fundamentals": LayerOutcome.SUCCESS,
            "financial_normalization": LayerOutcome.SUCCESS,
            "news": LayerOutcome.FAILED,  # non-blocking
        }
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is None


class TestBuildUpstreamOutcomes:
    def test_in_run_deps_use_outcomes_map(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        outcomes: dict[str, LayerOutcome] = {"universe": LayerOutcome.SUCCESS}
        resolved = executor._build_upstream_outcomes(plan, outcomes)
        assert resolved["universe"] is LayerOutcome.SUCCESS

    def test_unplanned_dep_resolved_from_job_runs(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        outcomes: dict[str, LayerOutcome] = {}
        with patch.object(
            executor,
            "_last_counting_outcome_from_job_runs",
            return_value=LayerOutcome.SUCCESS,
        ) as m:
            resolved = executor._build_upstream_outcomes(plan, outcomes)
        m.assert_called_once_with("universe")
        assert resolved["universe"] is LayerOutcome.SUCCESS


class TestRunLayersLoopContract:
    """_run_layers_loop adapter-contract guards."""

    def test_adapter_returning_empty_list_marks_all_emits_failed(self, monkeypatch) -> None:
        plan_item = _lp(
            "daily_financial_facts",
            emits=("financial_facts", "financial_normalization"),
            deps=(),
        )
        exec_plan = ExecutionPlan(
            layers_to_refresh=(plan_item,),
            layers_skipped=(),
            estimated_duration=None,
        )
        outcomes: dict[str, LayerOutcome] = {}

        # Adapter that returns empty list — contract violation.
        def bad_adapter(**kwargs):
            return []

        from dataclasses import replace

        from app.services.sync_orchestrator import registry

        monkeypatch.setitem(
            registry.LAYERS,
            "financial_facts",
            replace(registry.LAYERS["financial_facts"], refresh=bad_adapter),
        )

        # Patch audit writers to no-op.
        for writer in (
            "_record_layer_started",
            "_record_layer_failed",
            "_record_layer_skipped",
            "_record_layer_result",
        ):
            monkeypatch.setattr(executor, writer, MagicMock())
        monkeypatch.setattr(
            executor,
            "_make_progress_callback",
            lambda *a, **kw: lambda *args, **kwargs: None,
        )

        executor._run_layers_loop(sync_run_id=1, plan=exec_plan, outcomes=outcomes)

        assert outcomes == {
            "financial_facts": LayerOutcome.FAILED,
            "financial_normalization": LayerOutcome.FAILED,
        }

    def test_adapter_raising_marks_all_emits_failed(self, monkeypatch) -> None:
        plan_item = _lp(
            "daily_financial_facts",
            emits=("financial_facts", "financial_normalization"),
            deps=(),
        )
        exec_plan = ExecutionPlan(
            layers_to_refresh=(plan_item,),
            layers_skipped=(),
            estimated_duration=None,
        )
        outcomes: dict[str, LayerOutcome] = {}

        def raising_adapter(**kwargs):
            raise RuntimeError("boom")

        from dataclasses import replace

        from app.services.sync_orchestrator import registry

        monkeypatch.setitem(
            registry.LAYERS,
            "financial_facts",
            replace(registry.LAYERS["financial_facts"], refresh=raising_adapter),
        )
        for writer in (
            "_record_layer_started",
            "_record_layer_failed",
            "_record_layer_skipped",
            "_record_layer_result",
        ):
            monkeypatch.setattr(executor, writer, MagicMock())
        monkeypatch.setattr(
            executor,
            "_make_progress_callback",
            lambda *a, **kw: lambda *args, **kwargs: None,
        )

        executor._run_layers_loop(sync_run_id=1, plan=exec_plan, outcomes=outcomes)

        assert outcomes == {
            "financial_facts": LayerOutcome.FAILED,
            "financial_normalization": LayerOutcome.FAILED,
        }


class TestCategorizeError:
    # _categorize_error replaced by classify_exception from exception_classifier.
    # Tests updated to FailureCategory values (behaviour change notes below):
    # - "db_constraint" → FailureCategory.DB_CONSTRAINT (same semantics)
    # - "unknown" → FailureCategory.INTERNAL_ERROR (KeyError was previously
    #   "unknown"; now bucketed as INTERNAL_ERROR — retriable, same effect)
    def test_integrity_error(self) -> None:
        import psycopg

        from app.services.sync_orchestrator.exception_classifier import classify_exception
        from app.services.sync_orchestrator.layer_types import FailureCategory

        exc = psycopg.errors.IntegrityError("fk violation")
        assert classify_exception(exc) is FailureCategory.DB_CONSTRAINT

    def test_unknown_fallback(self) -> None:
        from app.services.sync_orchestrator.exception_classifier import classify_exception
        from app.services.sync_orchestrator.layer_types import FailureCategory

        exc = KeyError("nope")
        assert classify_exception(exc) is FailureCategory.INTERNAL_ERROR


class TestSetExecutor:
    def test_submit_sync_raises_when_no_executor_set(self) -> None:
        executor._executor_ref = None
        with pytest.raises(RuntimeError, match="executor not set"):
            from app.services.sync_orchestrator.types import SyncScope

            executor.submit_sync(SyncScope.full(), trigger="manual")
