"""Tests for sync orchestrator types and constants."""

from __future__ import annotations

from app.services.sync_orchestrator.types import (
    PREREQ_SKIP_MARKER,
    LayerOutcome,
    LayerPlan,
    SyncAlreadyRunning,
    SyncScope,
    prereq_skip_reason,
)


class TestLayerOutcome:
    def test_enum_values(self) -> None:
        assert LayerOutcome.SUCCESS.value == "success"
        assert LayerOutcome.NO_WORK.value == "no_work"
        assert LayerOutcome.PARTIAL.value == "partial"
        assert LayerOutcome.FAILED.value == "failed"
        assert LayerOutcome.DEP_SKIPPED.value == "dep_skipped"
        assert LayerOutcome.PREREQ_SKIP.value == "prereq_skip"


class TestPrereqSkipReason:
    def test_marker_prefix(self) -> None:
        reason = prereq_skip_reason("no provider configured")
        assert reason.startswith(PREREQ_SKIP_MARKER)
        assert "no provider configured" in reason

    def test_marker_constant(self) -> None:
        assert PREREQ_SKIP_MARKER == "prereq_missing:"


class TestSyncScope:
    def test_full(self) -> None:
        scope = SyncScope.full()
        assert scope.kind == "full"
        assert scope.detail is None
        assert scope.force is False

    def test_layer(self) -> None:
        scope = SyncScope.layer("candles")
        assert scope.kind == "layer"
        assert scope.detail == "candles"

    def test_job_forces_by_default(self) -> None:
        scope = SyncScope.job("daily_candle_refresh")
        assert scope.kind == "job"
        assert scope.detail == "daily_candle_refresh"
        assert scope.force is True

    def test_job_force_false_supported(self) -> None:
        scope = SyncScope.job("daily_candle_refresh", force=False)
        assert scope.force is False

    def test_high_frequency(self) -> None:
        scope = SyncScope.high_frequency()
        assert scope.kind == "high_frequency"


class TestSyncAlreadyRunning:
    def test_carries_active_id(self) -> None:
        exc = SyncAlreadyRunning(SyncScope.full(), active_sync_run_id=42)
        assert exc.active_sync_run_id == 42
        assert "42" in str(exc)

    def test_default_no_active_id(self) -> None:
        exc = SyncAlreadyRunning(SyncScope.full())
        assert exc.active_sync_run_id is None


class TestLayerPlan:
    def test_composite_plan_accepts_explicit_dependency_tuple(self) -> None:
        plan = LayerPlan(
            name="morning_candidate_review",
            emits=("scoring", "recommendations"),
            reason="stale",
            dependencies=("thesis", "candles"),
            is_blocking=True,
            estimated_items=0,
        )
        assert plan.emits == ("scoring", "recommendations")
        assert plan.dependencies == ("thesis", "candles")
        assert plan.is_blocking is True

    def test_single_emit_plan(self) -> None:
        plan = LayerPlan(
            name="nightly_universe_sync",
            emits=("universe",),
            reason="stale",
            dependencies=(),
            is_blocking=True,
            estimated_items=50,
        )
        assert plan.emits == ("universe",)
        assert plan.dependencies == ()
