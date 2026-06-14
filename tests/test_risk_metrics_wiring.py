"""Wiring invariants for the risk-metrics orchestrator layer + manual job
(#591 PR-B, Tasks B4+B5).

Mirrors tests/test_finra_regsho_daily_scheduler_wiring.py, but risk_metrics
is orchestrator-driven, NOT a standalone cron — so this file asserts the
ABSENCE of a SCHEDULED_JOBS entry (a ScheduledJob row would double-fire
alongside the orchestrator DAG walk) and the PRESENCE of the DAG layer +
JOB_TO_LAYERS mapping + manual-trigger lane.
"""

from __future__ import annotations

from app.jobs.runtime import _INVOKERS
from app.jobs.sources import MANUAL_TRIGGER_JOB_SOURCES, source_for
from app.services.sync_orchestrator.adapters import refresh_risk_metrics
from app.services.sync_orchestrator.registry import (
    INIT_CHECKS,
    JOB_TO_LAYERS,
    LAYERS,
)
from app.workers.scheduler import (
    JOB_RISK_METRICS_REFRESH,
    SCHEDULED_JOBS,
    risk_metrics_refresh,
)


def test_job_name_constant_exported() -> None:
    assert JOB_RISK_METRICS_REFRESH == "risk_metrics_refresh"


def test_invoker_registered_in_runtime() -> None:
    invoker = _INVOKERS[JOB_RISK_METRICS_REFRESH]
    assert callable(invoker)
    assert invoker.__wrapped__ is risk_metrics_refresh  # type: ignore[attr-defined]


def test_source_for_job_name_resolves_to_risk_metrics() -> None:
    assert source_for(JOB_RISK_METRICS_REFRESH) == "risk_metrics"


def test_manual_trigger_sources_entry_present() -> None:
    assert MANUAL_TRIGGER_JOB_SOURCES[JOB_RISK_METRICS_REFRESH] == "risk_metrics"


def test_layer_present_with_candles_dependency() -> None:
    layer = LAYERS["risk_metrics"]
    assert layer.dependencies == ("candles",)
    assert layer.requires_layer_initialized == ("candles",)
    assert layer.is_blocking is False


def test_candles_init_check_present() -> None:
    # risk_metrics declares requires_layer_initialized=("candles",); the
    # pre-flight gate raises if a named dep has no INIT_CHECKS entry.
    assert "candles" in INIT_CHECKS


def test_job_to_layers_mapping() -> None:
    assert JOB_TO_LAYERS["risk_metrics_refresh"] == ("risk_metrics",)


def test_refresh_adapter_callable() -> None:
    assert callable(refresh_risk_metrics)


def test_not_in_scheduled_jobs() -> None:
    """Orchestrator-driven, NOT a standalone cron — a ScheduledJob row would
    double-fire alongside the DAG walk (Codex ckpt-1 design constraint)."""
    scheduled_names = {job.name for job in SCHEDULED_JOBS}
    assert JOB_RISK_METRICS_REFRESH not in scheduled_names
