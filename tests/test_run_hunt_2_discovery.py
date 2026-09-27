"""#3448 — hunt 2's discovery flag (spec "Discovery flag → validation") and its frozen spec."""

from __future__ import annotations

from typing import Any

import pytest

from app.services import hunt_harness as hh
from app.services.hunt_compute import cell_key
from scripts import run_hunt_2_discovery as run

BASE = [cell_key(policy, dividends, "base") for policy in ("zero_recovery", "p2") for dividends in (True, False)]
ALL = sorted([*BASE, *(key.replace("|base", "|stress") for key in BASE)])
POWERED = {"min_detectable_annual_mean_80": "0.05"}


def _statistics(active: float | None = 0.001, excess: float | None = 0.09, positions: int = 10) -> dict[str, Any]:
    cells: dict[str, Any] = {key: {"mean": active} if active is not None else {"refused": "x"} for key in BASE}
    cells.update({key: {"mean": -1.0} for key in ALL if key.endswith("|stress")})
    per_trade = {key: {"arm": {"positions": positions, "mean": 0.0}, "control": {"positions": 9}} for key in ALL}
    tracker = {"cells": {key: {"excess_ann": excess} for key in ALL}}
    return {"cells": cells, "per_trade": per_trade, "tracker": tracker}


def test_all_four_conditions_met_declares_validation() -> None:
    flag = run.discovery_flag("computed", _statistics(), by_flagged=True, power=POWERED)
    assert flag["declare_validation"] is True
    assert all(c == {"met": True} for c in flag["conditions"].values())


@pytest.mark.parametrize(
    ("kwargs", "by_flagged", "power", "failing"),
    [
        ({}, False, POWERED, "by_flag"),
        ({"active": -0.0001}, True, POWERED, "arm_beats_control"),
        ({"excess": 0.0799}, True, POWERED, "corrected_bar"),
        ({"excess": None}, True, POWERED, "corrected_bar"),
        ({"positions": 0}, True, POWERED, "corrected_bar"),  # zero entered arm positions
        ({}, True, {"min_detectable_annual_mean_80": "0.0800001"}, "power_at_bar"),
        ({}, True, {"blocked": "no_discovery_series"}, "power_at_bar"),
    ],
)
def test_each_failing_condition_carries_its_own_label(
    kwargs: dict[str, Any], by_flagged: bool, power: dict[str, Any], failing: str
) -> None:
    flag = run.discovery_flag("computed", _statistics(**kwargs), by_flagged=by_flagged, power=power)
    assert flag["declare_validation"] is False
    assert flag["conditions"][failing]["label"] == run.CONDITION_LABELS[failing]
    assert all(c["met"] for name, c in flag["conditions"].items() if name != failing)


def test_the_stress_arm_binds_and_the_worst_cell_is_named() -> None:
    statistics = _statistics()
    stress = cell_key("p2", False, "stress")
    statistics["tracker"]["cells"][stress]["excess_ann"] = -0.2
    condition = run.discovery_flag("computed", statistics, by_flagged=True, power=POWERED)["conditions"][
        "corrected_bar"
    ]
    assert condition["met"] is False
    assert condition["worst_cell"] == {"cell": stress, "excess_ann": -0.2}


def test_a_refused_tracker_block_or_a_missing_cell_meets_nothing_economic() -> None:
    refused = _statistics()
    refused["tracker"] = {"refused": "tracker_gap:2009-03-02"}
    missing = _statistics()
    del missing["tracker"]["cells"][BASE[0]]
    for statistics in (refused, missing):
        flag = run.discovery_flag("computed", statistics, by_flagged=True, power=POWERED)
        assert flag["conditions"]["corrected_bar"]["met"] is False


def test_the_spec_is_the_frozen_trial() -> None:
    identity = hh.UniverseIdentity(
        universe="survivorship_free",
        selection_rule_version="v",
        validated_ids_sha256="1" * 64,
        archive_sha256="2" * 64,
        termination_identity_sha256="3" * 64,
    )
    spec = run.build_spec(identity)
    assert (spec.hunt_id, spec.family, spec.split) == ("hunt-2", "extreme_move_illiquid", "discovery")
    assert (spec.sign, spec.selection, spec.lag, spec.h) == (-1, 0.05, 1, 5)
    assert (spec.entry_point, spec.exit_point, spec.lane) == ("open", "close", "real_stock_long_x1")
    assert spec.survivor_bias_direction == "favours_arm"
    assert spec.signal_code_sha256 == hh.signal_code_sha256("extreme_move_illiquid:score")
    assert hh.timeline_refusal(spec) is None
    assert hh.HUNT_BUDGETS[spec.hunt_id] == 1
    assert "hunt-2" not in hh.HUNT_CLOSED


def test_power_reads_the_validation_calendar_only() -> None:
    statement = run.excess_power([0.001 * ((-1) ** i) + 0.0004 for i in range(4000)])
    assert statement["t_x"] == len(hh.split_grid("validation", lag=1, h=5).sessions)  # type: ignore[union-attr]
    assert float(statement["min_detectable_annual_mean_80"]) > 0.0
    assert run.excess_power(None) == {"blocked": "no_discovery_series"}


def test_a_refused_base_cell_fails_both_cell_conditions() -> None:
    conditions = run.discovery_flag("computed", _statistics(active=None), by_flagged=True, power=POWERED)["conditions"]
    assert conditions["arm_beats_control"]["met"] is False
    assert conditions["corrected_bar"]["met"] is False


def test_a_statistically_refused_cell_cannot_clear_the_bar() -> None:
    # Codex ckpt-2: a cell's books can compute (so it has an excess) while its statistics refuse.
    statistics = _statistics()
    statistics["cells"][cell_key("zero_recovery", True, "stress")] = {"refused": "degenerate_variance"}
    flag = run.discovery_flag("computed", statistics, by_flagged=True, power=POWERED)
    assert flag["conditions"]["corrected_bar"]["met"] is False


def test_hunt_2_validation_refuses_until_its_gate_is_built() -> None:
    from app.services import hunt_door

    assert hunt_door._gate_codes("hunt-2") == ["hunt_gate_not_implemented"]
    assert hunt_door._gate_codes("hunt-1") == []
