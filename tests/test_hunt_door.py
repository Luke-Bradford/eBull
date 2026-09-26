"""#3385 slice 2b-i — the pure halves of the validation door: power, BY's stored p, pin
round-trips and the canonical document bytes."""

from __future__ import annotations

import json
import math
from typing import Any, Literal

import pytest

from app.services import hunt_door, hunt_inference
from app.services import hunt_harness as hh
from app.services.hunt_compute import CANONICAL_CELL, cell_key
from app.services.trial_register import (
    declaration_backed_evidence,
    parse_declaration_backed_evidence,
    parse_log_backed_evidence,
)
from tests.test_hunt_harness import trial_spec

# --- power ----------------------------------------------------------------------------------


def _series(n: int) -> list[float]:
    return [0.001 * math.sin(0.7 * i) + 0.0004 * ((i * 37) % 11 - 5) for i in range(n)]


def test_power_is_the_minimum_detectable_mean_at_the_target_lag() -> None:
    series = _series(400)
    statement = hunt_door.power_statement(series, target_observations=3000, h=5)
    lag = hunt_inference.hunt_lag(3000, 5)
    estimate = hunt_inference.hac_estimate(series, lag)
    assert isinstance(estimate, hunt_inference.HacEstimate)
    scale = math.sqrt(estimate.long_run_variance / 3000) * 252
    assert statement["lag"] == lag == 10
    assert statement["min_detectable_annual_mean_50"] == hunt_door.sig(3.0 * scale)
    assert statement["min_detectable_annual_mean_80"] == hunt_door.sig(3.8416 * scale)
    assert "Not power for DSR" in statement["statement"]
    assert "blocked" not in statement


@pytest.mark.parametrize(
    ("series", "target", "blocked"),
    [
        (None, 3000, "no_discovery_series"),
        ([], 3000, "no_discovery_series"),
        ([0.1, math.nan] * 50, 3000, "non_finite_discovery_series"),
        (_series(400), 15, "target_short_sample"),
        (_series(8), 3000, "target_lag_exceeds_discovery_series"),
        ([0.001] * 400, 3000, "discovery_degenerate_variance"),
    ],
)
def test_power_blocks_rather_than_guessing(series: list[float] | None, target: int, blocked: str) -> None:
    assert hunt_door.power_statement(series, target_observations=target, h=1)["blocked"] == blocked


def test_power_numbers_are_six_significant_figure_strings() -> None:
    assert hunt_door.sig(0.0123456789) == "0.0123457"
    with pytest.raises(ValueError):
        hunt_door.sig(math.inf)


# --- BY's p from a stored outcome -------------------------------------------------------------


def _cell(p: Any) -> dict[str, Any]:
    return {"p": p, "t_stat": 1.0}


def test_the_stored_screening_p_is_the_worst_base_cell() -> None:
    cells = {
        CANONICAL_CELL: _cell(0.01),
        cell_key("full_recovery", True, "base"): _cell(0.04),
        cell_key("full_recovery", True, "stress"): _cell(0.9),
    }
    assert hunt_door.stored_screening_p("computed", {"cells": cells}) == 0.04


def test_an_underflowed_p_screens_as_zero_and_a_refused_base_cell_as_one() -> None:
    assert hunt_door.stored_screening_p("computed", {"cells": {CANONICAL_CELL: _cell("< 1e-12")}}) == 0.0
    refused = {CANONICAL_CELL: _cell(0.001), cell_key("full_recovery", False, "base"): {"refused": "sparse_arm"}}
    assert hunt_door.stored_screening_p("refused", {"cells": refused}) == 1.0
    assert hunt_door.stored_screening_p("abandoned", {"cells": {CANONICAL_CELL: _cell(0.0)}}) == 1.0
    assert hunt_door.stored_screening_p("refused", {"grid": {"refused": "empty_grid"}}) == 1.0


def test_an_invalid_stored_p_is_an_infrastructure_error() -> None:
    with pytest.raises(hh.HuntHarnessError):
        hunt_door.stored_screening_p("computed", {"cells": {CANONICAL_CELL: _cell(1.5)}})


# --- pins and the document ------------------------------------------------------------------


def test_a_pin_round_trips_through_its_form() -> None:
    spec = trial_spec(split="validation")
    rebuilt = hh.TrialSpec.from_form(spec.form())
    assert rebuilt.spec_sha256 == spec.spec_sha256
    extra = {**spec.form(), "unexpected": 1}
    with pytest.raises(TypeError):
        hh.TrialSpec.from_form(extra)


def test_the_document_file_is_its_canonical_json(tmp_path: Any) -> None:
    doc = hh.canonical_form({"kind": hh.HUNT_DECLARATION_KIND, "b": 0.1, "a": [1, 2]})
    path = tmp_path / "doc.json"
    sha = hunt_door.write_declaration(path, doc)
    assert sha == hh.sha256_form(json.loads(path.read_bytes()))
    assert hunt_door.document_bytes(json.loads(path.read_bytes())) == path.read_bytes()


def test_register_evidence_parses_back() -> None:
    evidence = declaration_backed_evidence(declaration_path="docs/x.json", declaration_sha256="a" * 64, pinned_specs=3)
    parsed = parse_declaration_backed_evidence(evidence)
    assert parsed is not None and (parsed.declaration_path, parsed.pinned_specs) == ("docs/x.json", 3)
    assert parse_log_backed_evidence(evidence) is None
    assert parse_declaration_backed_evidence("free text") is None


# --- the validation readout's verdict (slice 2b-ii) ----------------------------------------


def _noise(seed: int, drift: float, count: int = 500) -> tuple[float, ...]:
    import numpy as np

    rng = np.random.default_rng(seed)
    return tuple(float(value) + drift for value in rng.normal(0.0, 0.01, count))


def _variance() -> hunt_inference.TrialSharpeVariance:
    members = [hunt_inference.VPopulationMember(i, _noise(100 + i, 0.0), False) for i in range(12)]
    variance = hunt_inference.trial_sharpe_variance(members)
    assert isinstance(variance, hunt_inference.TrialSharpeVariance)
    return variance


def _cell_form(series: tuple[float, ...]) -> dict[str, Any]:
    cell = hunt_inference.cell_statistics(series, h=1, entered_formations=range(len(series)))
    assert isinstance(cell, hunt_inference.CellStatistics)
    return hh.decode_form(hh.canonical_form(cell.form()))


def _all_cells(base: Any, stress: Any) -> dict[str, Any]:
    from app.services.r6_exclusion_trial import PROGRAMME_POLICIES

    costs: tuple[tuple[Literal["base", "stress"], Any], ...] = (("base", base), ("stress", stress))
    return {
        cell_key(policy.label, dividends, cost): form
        for policy in PROGRAMME_POLICIES
        for dividends in (True, False)
        for cost, form in costs
    }


def test_a_stored_cell_deflates_exactly_as_its_series_does() -> None:
    series = _noise(1, 0.003)
    form = _cell_form(series)
    cell = hunt_inference.CellStatistics.from_form(form)
    variance = _variance()
    live = hunt_inference.hunt_dsr(series, cell, variance=variance, declared_trials=300, trial_register_version="r")
    stored = hunt_inference.stored_hunt_dsr(form, variance=variance, declared_trials=300, trial_register_version="r")
    assert isinstance(live, hunt_inference.HuntDsr) and isinstance(stored, hunt_inference.HuntDsr)
    assert stored.result == live.result
    assert hunt_inference.TrialSharpeVariance.from_form(hh.decode_form(hh.canonical_form(variance.form()))) == variance


def test_a_strong_positive_outcome_passes_and_a_refused_base_cell_refuses() -> None:
    strong = _cell_form(_noise(2, 0.01))
    kwargs: dict[str, Any] = {"variance": _variance(), "declared_trials": 300, "trial_register_version": "r"}
    result, dsr = hunt_door.candidate_verdict("computed", {"cells": _all_cells(strong, strong)}, **kwargs)
    assert result.verdict is hunt_inference.Verdict.PASS and len(dsr) == 6
    weak_stress = _cell_form(_noise(3, -0.002))
    result, _ = hunt_door.candidate_verdict("computed", {"cells": _all_cells(strong, weak_stress)}, **kwargs)
    assert result.verdict is hunt_inference.Verdict.PASS_CONTINGENT
    cells = _all_cells(strong, strong)
    cells[CANONICAL_CELL] = {"refused": "sparse_arm", "detail": "x"}
    result, dsr = hunt_door.candidate_verdict("refused", {"cells": cells}, **kwargs)
    assert result.verdict is hunt_inference.Verdict.NOT_PASS_REFUSED
    assert dsr[CANONICAL_CELL.rpartition("|")[0]] == "refused: sparse_arm"
    for status, statistics in (("abandoned", {"cells": _all_cells(strong, strong)}), ("refused", {"grid": {}})):
        result, _ = hunt_door.candidate_verdict(status, statistics, **kwargs)
        assert result.verdict is hunt_inference.Verdict.NOT_PASS_REFUSED
