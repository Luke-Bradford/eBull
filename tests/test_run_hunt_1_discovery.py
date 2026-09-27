"""#3387 — hunt 1's discovery flag (spec "Discovery flag") and its frozen spec."""

from __future__ import annotations

from typing import Any

import pytest

from app.services import hunt_harness as hh
from app.services.hunt_compute import cell_key
from scripts import run_hunt_1_discovery as run

BASE = [cell_key(policy, dividends, "base") for policy in ("zero_recovery", "p2") for dividends in (True, False)]


def _statistics(active: float | None = 0.001, arm: float | None = 0.02, positions: int = 10) -> dict[str, Any]:
    cells: dict[str, Any] = {key: {"mean": active} if active is not None else {"refused": "x"} for key in BASE}
    cells[cell_key("zero_recovery", True, "stress")] = {"mean": -1.0}  # stress cells are not gated
    control = {"positions": 99, "mean": 0.0}
    per_trade = {key: {"arm": {"positions": positions, "mean": arm}, "control": control} for key in BASE}
    return {"cells": cells, "per_trade": per_trade}


def test_all_three_conditions_met_declares_validation() -> None:
    flag = run.discovery_flag("computed", _statistics(), by_flagged=True)
    assert flag["declare_validation"] is True
    assert all(c == {"met": True} for c in flag["conditions"].values())


@pytest.mark.parametrize(
    ("kwargs", "by_flagged", "failing"),
    [
        ({}, False, "by_flag"),
        ({"active": -0.0001}, True, "arm_beats_control"),
        ({"active": None}, True, "arm_beats_control"),  # a refused cell
        ({"arm": 0.0121}, True, "operator_bar"),
        ({"arm": None}, True, "operator_bar"),
        ({"positions": 0}, True, "operator_bar"),  # zero entered arm positions
    ],
)
def test_each_failing_condition_carries_its_own_label(kwargs: dict[str, Any], by_flagged: bool, failing: str) -> None:
    flag = run.discovery_flag("computed", _statistics(**kwargs), by_flagged=by_flagged)
    assert flag["declare_validation"] is False
    assert flag["conditions"][failing] == {"met": False, "label": run.CONDITION_LABELS[failing]}
    assert all(c["met"] for name, c in flag["conditions"].items() if name != failing)


def test_one_failing_base_cell_fails_the_condition() -> None:
    statistics = _statistics()
    statistics["per_trade"][BASE[-1]]["arm"]["mean"] = 0.01
    assert run.discovery_flag("computed", statistics, by_flagged=True)["conditions"]["operator_bar"]["met"] is False


def test_a_missing_readout_or_no_cells_meets_nothing() -> None:
    flag = run.discovery_flag("refused", {"grid": {}}, by_flagged=False)
    assert not any(c["met"] for c in flag["conditions"].values())
    statistics = _statistics()
    del statistics["per_trade"]
    assert run.discovery_flag("computed", statistics, by_flagged=True)["conditions"]["operator_bar"]["met"] is False


def test_the_spec_is_the_frozen_trial() -> None:
    identity = hh.UniverseIdentity(
        universe="survivorship_free",
        selection_rule_version="v",
        validated_ids_sha256="1" * 64,
        archive_sha256="2" * 64,
        termination_identity_sha256="3" * 64,
    )
    spec = run.build_spec(identity)
    assert (spec.hunt_id, spec.family, spec.split) == ("hunt-1", "extreme_move_illiquid", "discovery")
    assert (spec.sign, spec.selection, spec.lag, spec.h) == (-1, 0.05, 1, 5)
    assert (spec.entry_point, spec.exit_point, spec.lane) == ("close", "close", "real_stock_long_x1")
    assert spec.survivor_bias_direction == "favours_arm"
    assert spec.signal_code_sha256 == hh.signal_code_sha256("extreme_move_illiquid:score")
    assert hh.timeline_refusal(spec) is None
    assert hh.HUNT_BUDGETS[spec.hunt_id] == 1


#: The stored hunt-1 discovery outcome (hunt_trial_id 1, outcome_sha256 43895acf…), base cells only, as
#: ``--readout`` printed it on 2026-09-27. Every termination policy gave the same figures.
STORED_BASE = {
    True: {"active": 0.0015215045741797377, "arm": 0.0032513068802462923},
    False: {"active": 0.0015602159960924546, "arm": 0.00310990198993311},
}


def test_the_hunt_1_closure_matches_its_stored_readout() -> None:
    """Harness spec "Budget and closure": the closure's listed verdict against the stored outcome."""
    cells: dict[str, Any] = {}
    per_trade: dict[str, Any] = {}
    for policy in ("zero_recovery", "classified_best", "classified_worst"):
        for dividends, figures in STORED_BASE.items():
            key = cell_key(policy, dividends, "base")
            cells[key] = {"mean": figures["active"]}
            per_trade[key] = {"arm": {"positions": 146758, "mean": figures["arm"]}}
    flag = run.discovery_flag("computed", {"cells": cells, "per_trade": per_trade}, by_flagged=True)
    assert flag["declare_validation"] is False
    assert [name for name, c in flag["conditions"].items() if not c["met"]] == ["operator_bar"]
    closure = hh.HUNT_CLOSED["hunt-1"]
    assert closure.startswith("no demonstrated edge")
    assert run.CONDITION_LABELS["operator_bar"] in closure
