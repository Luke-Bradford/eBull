from app.services.sync_orchestrator.cascade import ProblemGroup, collapse_cascades
from app.services.sync_orchestrator.layer_types import LayerState


def _graph() -> dict[str, tuple[str, ...]]:
    return {
        "universe": (),
        "cik_mapping": ("universe",),
        "candles": ("universe",),
        "financial_facts": ("cik_mapping",),
        "financial_normalization": ("financial_facts",),
        "fundamentals": ("universe",),
        "news": ("universe",),
        "thesis": ("fundamentals", "financial_normalization", "news"),
        "scoring": ("thesis", "candles"),
        "recommendations": ("scoring",),
    }


def test_single_root_collapses_transitive_downstream() -> None:
    states = {name: LayerState.CASCADE_WAITING for name in _graph()}
    states["universe"] = LayerState.ACTION_NEEDED
    groups = collapse_cascades(_graph(), states)
    assert len(groups) == 1
    assert groups[0].root == "universe"
    assert set(groups[0].affected) == {
        "cik_mapping", "candles", "financial_facts",
        "financial_normalization", "fundamentals", "news",
        "thesis", "scoring", "recommendations",
    }


def test_multiple_roots_produce_multiple_groups() -> None:
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.ACTION_NEEDED
    states["news"] = LayerState.SECRET_MISSING
    for name in ("financial_facts", "financial_normalization", "thesis", "scoring", "recommendations"):
        states[name] = LayerState.CASCADE_WAITING
    groups = collapse_cascades(_graph(), states)
    assert {g.root for g in groups} == {"cik_mapping", "news"}


def test_healthy_descendant_not_in_affected() -> None:
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.ACTION_NEEDED
    groups = collapse_cascades(_graph(), states)
    assert "financial_facts" not in groups[0].affected


def test_degraded_root_produces_no_group() -> None:
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.DEGRADED
    assert collapse_cascades(_graph(), states) == []


def test_retrying_root_produces_no_group() -> None:
    # RETRYING is self-healing; not a terminal cascade root.
    states = {name: LayerState.HEALTHY for name in _graph()}
    states["cik_mapping"] = LayerState.RETRYING
    assert collapse_cascades(_graph(), states) == []


def test_empty_dependency_map() -> None:
    assert collapse_cascades({}, {}) == []


def test_returns_frozen_problem_group() -> None:
    states = {"a": LayerState.ACTION_NEEDED}
    groups = collapse_cascades({"a": ()}, states)
    assert isinstance(groups[0], ProblemGroup)
    assert groups[0].root == "a"
    assert groups[0].affected == []
