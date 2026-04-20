"""Tests for sync orchestrator planner."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.services.sync_orchestrator.planner import (
    _build_layer_plan,
    build_execution_plan,
)
from app.services.sync_orchestrator.types import SyncScope


def _make_conn_with_freshness(fresh_layers: set[str]) -> MagicMock:
    """Return a mock conn that, when LAYERS[name].is_fresh(conn) is called,
    produces (True, 'fresh') iff name is in fresh_layers."""
    from app.services.sync_orchestrator import registry

    saved: dict[str, object] = {}
    for name, layer in registry.LAYERS.items():
        saved[name] = layer.is_fresh

    def _make_predicate(layer_name: str):
        def _pred(conn):
            return (True, "fresh") if layer_name in fresh_layers else (False, "stale")

        return _pred

    # Patch by replacing the DataLayer dataclass fields (frozen, so
    # construct a copy and re-inject). Simpler: monkey-patch via dict.
    # But DataLayer is frozen. Use object.__setattr__ trick.
    for name, layer in registry.LAYERS.items():
        object.__setattr__(layer, "is_fresh", _make_predicate(name))
    return MagicMock()


@pytest.fixture(autouse=True)
def _restore_layer_predicates():
    """Restore is_fresh callables to their original freshness-module
    definitions after each test.

    Importing from `freshness` (not from LAYERS) guards against a
    previous test leaving LAYERS in a half-restored state — we always
    pin back to the real module-level predicate regardless of current
    LAYERS content."""
    from app.services.sync_orchestrator import freshness, registry

    originals = {
        "universe": freshness.universe_is_fresh,
        "candles": freshness.candles_is_fresh,
        "fundamentals": freshness.fundamentals_is_fresh,
        "scoring": freshness.scoring_is_fresh,
        "recommendations": freshness.recommendations_is_fresh,
        "portfolio_sync": freshness.portfolio_sync_is_fresh,
        "fx_rates": freshness.fx_rates_is_fresh,
        "cost_models": freshness.cost_models_is_fresh,
        "weekly_reports": freshness.weekly_reports_is_fresh,
        "monthly_reports": freshness.monthly_reports_is_fresh,
    }
    yield
    for name, pred in originals.items():
        object.__setattr__(registry.LAYERS[name], "is_fresh", pred)


class TestBuildLayerPlan:
    def test_single_layer_passes_dependencies_through(self) -> None:
        plan = _build_layer_plan("daily_candle_refresh", ("candles",), "stale")
        assert plan.emits == ("candles",)
        assert plan.dependencies == ("universe",)
        assert plan.is_blocking is True

    def test_composite_drops_intra_emit_edges(self) -> None:
        """morning_candidate_review emits (scoring, recommendations).
        scoring.deps = (candles, fundamentals); recommendations.deps = (scoring,).
        external = {candles, fundamentals, scoring} - {scoring, recommendations}
        = {candles, fundamentals}."""
        plan = _build_layer_plan(
            "morning_candidate_review",
            ("scoring", "recommendations"),
            "stale",
        )
        assert set(plan.dependencies) == {"candles", "fundamentals"}
        assert "scoring" not in plan.dependencies
        assert "recommendations" not in plan.dependencies


class TestBuildExecutionPlanFull:
    def test_all_fresh_yields_empty_refresh_set(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set(LAYERS.keys()))
        plan = build_execution_plan(MagicMock(), SyncScope.full())
        assert plan.layers_to_refresh == ()
        assert len(plan.layers_skipped) == 10

    def test_all_stale_yields_every_in_dag_layer(self) -> None:
        _make_conn_with_freshness(set())
        plan = build_execution_plan(MagicMock(), SyncScope.full())
        # 11 in-DAG jobs → 11 LayerPlan entries.
        assert len(plan.layers_to_refresh) == 9

    def test_topological_order_roots_first(self) -> None:
        _make_conn_with_freshness(set())
        plan = build_execution_plan(MagicMock(), SyncScope.full())
        order = [lp.name for lp in plan.layers_to_refresh]
        # universe comes before candles comes before morning_candidate_review.
        assert order.index("nightly_universe_sync") < order.index("daily_candle_refresh")
        assert order.index("daily_candle_refresh") < order.index("morning_candidate_review")


class TestBuildExecutionPlanHighFrequency:
    def test_high_frequency_includes_only_portfolio_and_fx(self) -> None:
        _make_conn_with_freshness(set())
        plan = build_execution_plan(MagicMock(), SyncScope.high_frequency())
        names = {lp.name for lp in plan.layers_to_refresh}
        assert names == {"daily_portfolio_sync", "fx_rates_refresh"}


class TestBuildExecutionPlanLayer:
    def test_layer_scope_includes_only_stale_deps(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS

        # Universe fresh, candles stale.
        _make_conn_with_freshness(set(LAYERS.keys()) - {"candles"})
        plan = build_execution_plan(MagicMock(), SyncScope.layer("candles"))
        names = {lp.name for lp in plan.layers_to_refresh}
        assert names == {"daily_candle_refresh"}

    def test_layer_scope_includes_stale_dep(self) -> None:
        # Both universe and candles stale.
        _make_conn_with_freshness(set())
        plan = build_execution_plan(MagicMock(), SyncScope.layer("candles"))
        names = {lp.name for lp in plan.layers_to_refresh}
        assert "nightly_universe_sync" in names
        assert "daily_candle_refresh" in names


class TestBuildExecutionPlanJobForce:
    def test_job_force_runs_target_when_fresh(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set(LAYERS.keys()))  # everything fresh
        plan = build_execution_plan(MagicMock(), SyncScope.job("daily_candle_refresh", force=True))
        names = {lp.name for lp in plan.layers_to_refresh}
        assert "daily_candle_refresh" in names

    def test_job_force_does_not_force_fresh_dependency(self) -> None:
        """force=True applies only to target; deps evaluated on freshness."""
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set(LAYERS.keys()))  # universe also fresh
        plan = build_execution_plan(MagicMock(), SyncScope.job("daily_candle_refresh", force=True))
        names = {lp.name for lp in plan.layers_to_refresh}
        assert "nightly_universe_sync" not in names

    def test_job_force_includes_stale_dependency(self) -> None:
        """force=True runs target; stale dep still planned via freshness."""
        from app.services.sync_orchestrator.registry import LAYERS

        # Candles fresh but universe stale → both planned (universe because stale).
        _make_conn_with_freshness(set(LAYERS.keys()) - {"universe"})
        plan = build_execution_plan(MagicMock(), SyncScope.job("daily_candle_refresh", force=True))
        names = {lp.name for lp in plan.layers_to_refresh}
        assert "nightly_universe_sync" in names
        assert "daily_candle_refresh" in names

    def test_job_force_composite_runs_all_emits(self) -> None:
        """force=True on a composite job runs both emits as one LayerPlan."""
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set(LAYERS.keys()))
        plan = build_execution_plan(MagicMock(), SyncScope.job("morning_candidate_review", force=True))
        morning_plans = [lp for lp in plan.layers_to_refresh if lp.name == "morning_candidate_review"]
        assert len(morning_plans) == 1
        assert morning_plans[0].emits == ("scoring", "recommendations")
