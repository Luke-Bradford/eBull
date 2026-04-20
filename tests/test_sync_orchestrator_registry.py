"""Tests for LAYERS and JOB_TO_LAYERS registries."""

from __future__ import annotations

from app.jobs.runtime import _INVOKERS
from app.services.sync_orchestrator.registry import JOB_TO_LAYERS, LAYERS


class TestLayerRegistry:
    def test_all_12_layers_present(self) -> None:
        # cik_mapping, financial_facts, financial_normalization retired in
        # Chunk 3 of the 2026-04-19 research-tool refocus; their work now
        # lives inside fundamentals_sync.
        expected = {
            "universe",
            "candles",
            "fundamentals",
            "news",
            "thesis",
            "scoring",
            "recommendations",
            "portfolio_sync",
            "fx_rates",
            "cost_models",
            "weekly_reports",
            "monthly_reports",
        }
        assert set(LAYERS.keys()) == expected

    def test_blocking_defaults_per_spec(self) -> None:
        """is_blocking=True for all data producers; False for news,
        portfolio_sync, fx_rates, weekly_reports, monthly_reports."""
        non_blocking = {
            "news",
            "portfolio_sync",
            "fx_rates",
            "weekly_reports",
            "monthly_reports",
        }
        for name, layer in LAYERS.items():
            expected_blocking = name not in non_blocking
            assert layer.is_blocking == expected_blocking, name

    def test_every_dep_is_a_known_layer(self) -> None:
        for name, layer in LAYERS.items():
            for dep in layer.dependencies:
                assert dep in LAYERS, f"{name} depends on unknown {dep}"

    def test_no_cycles(self) -> None:
        """DFS check: the DAG must be acyclic."""
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            assert name not in visiting, f"cycle at {name}"
            visiting.add(name)
            for dep in LAYERS[name].dependencies:
                visit(dep)
            visiting.remove(name)
            visited.add(name)

        for name in LAYERS:
            visit(name)


class TestJobToLayers:
    def test_every_key_is_a_real_invoker(self) -> None:
        for job_name in JOB_TO_LAYERS:
            assert job_name in _INVOKERS, job_name

    def test_every_emitted_layer_is_in_registry(self) -> None:
        for job_name, emits in JOB_TO_LAYERS.items():
            for layer in emits:
                assert layer in LAYERS, f"{job_name} emits unknown layer {layer}"

    def test_expected_mappings(self) -> None:
        assert JOB_TO_LAYERS["morning_candidate_review"] == (
            "scoring",
            "recommendations",
        )
        assert JOB_TO_LAYERS["weekly_report"] == ("weekly_reports",)
        assert JOB_TO_LAYERS["monthly_report"] == ("monthly_reports",)

    def test_outside_dag_jobs_are_empty_tuples(self) -> None:
        assert JOB_TO_LAYERS["execute_approved_orders"] == ()
        assert JOB_TO_LAYERS["monitor_positions"] == ()
        assert JOB_TO_LAYERS["retry_deferred_recommendations"] == ()
        assert JOB_TO_LAYERS["fundamentals_sync"] == ()
        assert JOB_TO_LAYERS["attribution_summary"] == ()
        assert JOB_TO_LAYERS["daily_tax_reconciliation"] == ()

    def test_every_layer_is_emitted_by_some_job(self) -> None:
        """Sanity: no orphan layer with no adapter."""
        emitted = {layer for emits in JOB_TO_LAYERS.values() for layer in emits}
        assert set(LAYERS.keys()) == emitted
