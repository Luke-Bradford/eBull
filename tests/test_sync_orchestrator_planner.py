"""Tests for sync orchestrator planner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services.sync_orchestrator.layer_types import LayerState
from app.services.sync_orchestrator.planner import (
    _build_layer_plan,
    build_execution_plan,
)
from app.services.sync_orchestrator.registry import JOB_TO_LAYERS
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


#: Layers the full DAG walk covers, and the jobs that emit them. Derived so a
#: new layer updates both counts by existing, not by somebody remembering to
#: bump a literal.
_IN_DAG_JOBS = {job for job, emits in JOB_TO_LAYERS.items() if emits}
_IN_DAG_LAYERS = {layer for emits in JOB_TO_LAYERS.values() for layer in emits}


class TestBuildExecutionPlanFull:
    def test_all_fresh_yields_empty_refresh_set(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set(LAYERS.keys()))
        plan = build_execution_plan(MagicMock(), SyncScope.full())
        assert plan.layers_to_refresh == ()
        # Every in-DAG layer, DERIVED from the registry rather than pinned to a
        # literal. The count was hand-maintained (9 → 10 → 11 → 12 → 13 across
        # #591 / #2009 / #2261 / #3040) and a hand-maintained count is a comment
        # that goes stale on the next append — prevention log: a test pinning a
        # generic capability must not couple to a churnable registry entry.
        assert len(plan.layers_skipped) == len(_IN_DAG_LAYERS)

    def test_all_stale_yields_every_in_dag_layer(self) -> None:
        _make_conn_with_freshness(set())
        plan = build_execution_plan(MagicMock(), SyncScope.full())
        # One LayerPlan per in-DAG JOB, not per layer: scoring + recommendations
        # collapse into one producing job (morning_candidate_review), which is
        # why this is derived from JOB_TO_LAYERS and is one fewer than the layer
        # count above.
        assert len(plan.layers_to_refresh) == len(_IN_DAG_JOBS)

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


class TestBehindScopeTargetStates:
    """#2274 — which LayerStates are DIRECT targets of scope='behind'.

    RETRYING was excluded on the strength of ``freshness-unification.md:78``
    ("orchestrator will re-fire with backoff"), a mechanism that was never
    built: ``RetryPolicy.backoff_seconds`` has no scheduling consumer. The
    exclusion made the gate non-monotone in failure count — 0 failures
    (DEGRADED) selected, 1..max_attempts-1 (RETRYING) not, >= max_attempts
    (ACTION_NEEDED) selected again.
    """

    @staticmethod
    def _plan_with_states(states: dict[str, LayerState]) -> set[str]:
        """Return the set of job names planned for scope='behind' under the
        given layer states. Every unnamed layer is HEALTHY."""
        from app.services.sync_orchestrator import planner
        from app.services.sync_orchestrator.registry import LAYERS

        full = {name: LayerState.HEALTHY for name in LAYERS}
        full.update(states)
        with patch.object(planner, "compute_layer_states_from_db", return_value=full):
            plan = build_execution_plan(MagicMock(), SyncScope.behind())
        return {lp.name for lp in plan.layers_to_refresh}

    @pytest.mark.parametrize(
        ("state", "selected"),
        [
            (LayerState.DEGRADED, True),
            (LayerState.RETRYING, True),  # #2274: was False
            (LayerState.ACTION_NEEDED, True),
            (LayerState.HEALTHY, False),
            (LayerState.RUNNING, False),
            (LayerState.DISABLED, False),
            (LayerState.SECRET_MISSING, False),
            (LayerState.CASCADE_WAITING, False),
        ],
    )
    def test_every_state_as_an_isolated_direct_target(self, state: LayerState, selected: bool) -> None:
        """All eight states, one at a time, on a layer with no unhealthy
        neighbours — so selection is attributable to the direct-target
        predicate and not to the closure arm."""
        planned = self._plan_with_states({"candles": state})
        assert ("daily_candle_refresh" in planned) is selected

    def test_retrying_is_planned_even_when_is_fresh_says_otherwise(self) -> None:
        """``behind`` bypasses the freshness re-filter (state selection is
        authoritative), so a RETRYING layer fires even with every predicate
        returning fresh. Pins the interaction the backoff design would have
        broken."""
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set(LAYERS.keys()))
        assert "daily_candle_refresh" in self._plan_with_states({"candles": LayerState.RETRYING})

    def test_retrying_upstream_still_planned_via_closure(self) -> None:
        """Regression pin for the pre-#2274 path: a RETRYING layer was already
        reachable as a transitive upstream of a DEGRADED target. That must keep
        working, and must not produce a duplicate plan entry now that the same
        layer is also a direct target."""
        from app.services.sync_orchestrator import planner
        from app.services.sync_orchestrator.registry import LAYERS

        full = {name: LayerState.HEALTHY for name in LAYERS}
        full["candles"] = LayerState.RETRYING
        full["scoring"] = LayerState.DEGRADED
        with patch.object(planner, "compute_layer_states_from_db", return_value=full):
            plan = build_execution_plan(MagicMock(), SyncScope.behind())
        names = [lp.name for lp in plan.layers_to_refresh]
        assert "daily_candle_refresh" in names
        assert "morning_candidate_review" in names
        assert len(names) == len(set(names)), f"duplicate plan entries: {names}"

    def test_behind_composite_job_runs_disabled_sibling(self) -> None:
        """⚠ PINS A PRE-EXISTING GAP, not desired behaviour.

        ``morning_candidate_review`` is the registry's only multi-emit job. A
        job is selected if ANY emit is a target, and the executor does not
        recheck the operator's enabled flag — so a DISABLED ``scoring`` is
        written anyway when ``recommendations`` is selected. That was already
        true for a DEGRADED recommendations; #2274 makes it reachable from a
        RETRYING one too. Asserted so the expansion is explicit and a future
        executor-side fix has a test to update rather than a silent behaviour
        to discover.
        """
        planned = self._plan_with_states(
            {"recommendations": LayerState.RETRYING, "scoring": LayerState.DISABLED}
        )
        assert "morning_candidate_review" in planned

    def test_non_behind_scopes_ignore_layer_state(self) -> None:
        """The state machine gates ``behind`` only. ``high_frequency`` selects
        by emit name and must be unaffected by a RETRYING candles layer."""
        from app.services.sync_orchestrator import planner
        from app.services.sync_orchestrator.registry import LAYERS

        _make_conn_with_freshness(set())
        full = {name: LayerState.HEALTHY for name in LAYERS}
        full["candles"] = LayerState.RETRYING
        with patch.object(planner, "compute_layer_states_from_db", return_value=full):
            plan = build_execution_plan(MagicMock(), SyncScope.high_frequency())
        assert {lp.name for lp in plan.layers_to_refresh} == {"daily_portfolio_sync", "fx_rates_refresh"}
