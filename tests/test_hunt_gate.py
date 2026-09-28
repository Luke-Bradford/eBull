"""#3454 slice B — the ``hunt_gate`` block and hunt 3's promotion rule, pure.

Spec ``docs/proposals/ta/2026-09-28-3454-hunt-3-spec.md``: "Flag → validation declaration"
(conditions 1-3 gate, stress and ``excess_power`` are reported) and "At validation, one look"
(verdict, base excess at the bar, canonical excess NW t above the threshold; an unavailable
input is "undetermined", a miss is "no demonstrated edge").
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pytest

from app.services import hunt_door, hunt_gate, trial_register
from app.services import hunt_harness as hh
from app.services.hunt_compute import CANONICAL_CELL, TRACKER_SOURCE

#: A canonical excess series with NW t far above 3, and one with t near 0.
STRONG = tuple(0.001 + 0.002 * math.sin(1.7 * i) for i in range(3000))
FLAT = tuple(0.002 * math.sin(1.7 * i) for i in range(3000))


def gate_statistics(
    *,
    excess: float = 0.17,
    stress: float = -0.27,
    mean: float = 0.0016,
    positions: int = 10,
    series: tuple[float, ...] = STRONG,
    drop: str | None = None,
) -> dict[str, Any]:
    """A stored outcome's shape as ``hunt_compute`` writes it, for the 12 cells."""
    cells: dict[str, Any] = {}
    tracker_cells: dict[str, Any] = {}
    per_trade: dict[str, Any] = {}
    for key, role in hunt_gate.CELL_ROLES.items():
        if key == drop:
            continue
        cells[key] = {"p": 0.0, "t_stat": 4.0, "mean": mean}
        tracker_cells[key] = {"excess_ann": excess if role == "gated" else stress}
        per_trade[key] = {"arm": {"positions": positions}}
    return {
        "cells": cells,
        "per_trade": per_trade,
        "tracker": {
            "source": TRACKER_SOURCE,
            "cells": tracker_cells,
            "canonical_excess_series": list(series),
        },
    }


def _block(statistics: dict[str, Any] | None = None, *, by_flagged: bool = True) -> tuple[dict[str, Any], list[str]]:
    return hunt_gate.gate_block(
        gate_statistics() if statistics is None else statistics,
        by_flagged=by_flagged,
        excess_power={"min_detectable_annual_mean_80": "0.209986"},
        significance_lag=10,
        inheritance={"harness_model_id": hh.HUNT_HARNESS_MODEL_ID},
        pins=hunt_gate.decision_pins(),
    )


def test_the_block_names_exactly_twelve_cells_six_gated() -> None:
    assert len(hunt_gate.CELL_ROLES) == 12
    assert sorted(hunt_gate.CELL_ROLES.values()).count("gated") == 6
    assert hunt_gate.CELL_ROLES[CANONICAL_CELL] == "gated"
    assert all(key.endswith("|base") for key in hunt_gate.BASE_CELLS)
    assert "hunt-3" in hunt_gate.GATED_HUNTS and "hunt-3" not in hunt_door.HUNTS_AWAITING_GATE


def test_hunt_2s_discovery_shape_meets_the_gate_with_stress_reported() -> None:
    block, codes = _block()
    assert codes == []
    assert set(block) == hunt_gate.BLOCK_KEYS
    assert all(value == -0.27 for value in block["reported"]["stress_excess_ann"].values())
    assert block["disclosure"] == hunt_gate.DISCLOSURE


@pytest.mark.parametrize(
    ("statistics", "by_flagged", "expected"),
    [
        (gate_statistics(), False, ["hunt_gate_condition_not_met:by_flag"]),
        (gate_statistics(mean=-0.001), True, ["hunt_gate_condition_not_met:arm_beats_control"]),
        (gate_statistics(excess=0.079), True, ["hunt_gate_condition_not_met:excess_at_bar"]),
        (gate_statistics(excess=math.nan), True, ["hunt_gate_condition_not_met:excess_at_bar"]),
        (gate_statistics(positions=0), True, ["hunt_gate_condition_not_met:excess_at_bar"]),
        (
            gate_statistics(drop=CANONICAL_CELL),
            True,
            [
                "hunt_gate_cell_keys",
                "hunt_gate_condition_not_met:arm_beats_control",
                "hunt_gate_condition_not_met:excess_at_bar",
            ],
        ),
    ],
)
def test_each_gating_condition_fails_closed(statistics: dict[str, Any], by_flagged: bool, expected: list[str]) -> None:
    assert _block(statistics, by_flagged=by_flagged)[1] == expected


def test_a_failing_stress_cell_never_blocks_the_freeze() -> None:
    assert _block(gate_statistics(stress=-5.0))[1] == []


def test_the_freeze_tells_stale_pins_from_stale_values() -> None:
    block, _ = _block()
    assert hunt_gate.freeze_codes(block, block) == []
    assert hunt_gate.freeze_codes(block, {**block, "decision_pins": {}}) == ["gate_decision_pins_stale"]
    assert hunt_gate.freeze_codes(block, {**block, "conditions": {}}) == ["hunt_gate_stale:conditions"]
    assert hunt_gate.freeze_codes(block, None) == ["gate_missing"]


def test_the_look_refuses_a_missing_malformed_or_stale_block() -> None:
    block, _ = _block()
    model = hh.HUNT_HARNESS_MODEL_ID
    assert hunt_gate.look_codes({"hunt_gate": block}, harness_model_id=model) == []
    assert hunt_gate.look_codes({}, harness_model_id=model) == ["hunt_gate_missing"]
    assert hunt_gate.look_codes({"hunt_gate": {**block, "extra": 1}}, harness_model_id=model) == ["hunt_gate_malformed"]
    moved = {**block["decision_pins"], "app/services/hunt_door.py": "0" * 64}
    assert hunt_gate.look_codes({"hunt_gate": {**block, "decision_pins": moved}}, harness_model_id=model) == [
        "hunt_gate_pin_stale:app/services/hunt_door.py"
    ]
    assert hunt_gate.look_codes({"hunt_gate": block}, harness_model_id="hunt-harness-v1+0") == ["hunt_gate_model_stale"]


_REGISTER_SOURCE = '''"""The register."""
HUNT_TRIAL_PREFIX: Final = "hunt-"
TRIAL_REGISTER_VERSION: Final = "r16"


def floor(trials):
    return [t for t in trials if not t.startswith(HUNT_TRIAL_PREFIX)]


TRIAL_REGISTER: Final = ("hunt-3-validation", "reregistration-3454")
'''


@pytest.mark.parametrize(
    ("old", "new", "pinned"),
    [
        ('"r16"', '"r17"', False),
        ('"reregistration-3454")', '"reregistration-3454", "hunt-3-validation-doc")', False),
        ('"""The register."""', '"""The register, re-worded."""', False),
        ("return [t", "# a comment\n    return [t", False),
        ('HUNT_TRIAL_PREFIX: Final = "hunt-"', 'HUNT_TRIAL_PREFIX: Final = "h-"', True),
        ("not t.startswith", "t.startswith", True),
    ],
)
def test_the_register_pin_covers_code_and_constants_but_not_entries(old: str, new: str, pinned: bool) -> None:
    before = hunt_gate.code_only_sha256_of_source(_REGISTER_SOURCE)
    assert _REGISTER_SOURCE.count(old) == 1
    after = hunt_gate.code_only_sha256_of_source(_REGISTER_SOURCE.replace(old, new))
    assert (after != before) is pinned


def test_the_live_register_pin_is_computed_from_its_source() -> None:
    assert hunt_gate.code_only_sha256(trial_register) == hunt_gate.decision_pins()[hunt_gate.CODE_ONLY_PIN]


def test_every_decision_module_is_pinned_and_present() -> None:
    pins = hunt_gate.decision_pins()
    assert set(pins) == {*hunt_gate.PINNED_FILES, hunt_gate.CODE_ONLY_PIN}
    assert "missing" not in pins.values()


# --- the one look's promotion ----------------------------------------------------------------


def _promote(verdict: str | None, statistics: dict[str, Any], reasons: tuple[str, ...] = ()) -> dict[str, Any]:
    block = hh.decode_form(hh.canonical_form(_block()[0]))
    return hunt_gate.promotion(block, verdict=verdict, reasons=reasons, statistics=statistics)


def test_a_pass_above_the_bar_and_significant_promotes_with_its_margins() -> None:
    result = _promote("PASS", gate_statistics())
    assert (result["promote"], result["closure"], result["failed"], result["unavailable"]) == (True, None, [], [])
    assert result["excess_t"]["t_stat"] > 3
    assert result["m_val"] == pytest.approx(0.17 - 0.08)
    assert set(result["m_val_by_cell"]) == set(hunt_gate.BASE_CELLS)


def test_pass_contingent_promotes_only_when_every_failure_is_stress() -> None:
    assert _promote("PASS_CONTINGENT", gate_statistics(), ("stress zero_recovery: t 1 <= 3.0",))["promote"]
    assert not _promote("PASS_CONTINGENT", gate_statistics(), ("zero_recovery: DSR 0.1 < 0.95",))["promote"]


@pytest.mark.parametrize(
    ("verdict", "statistics", "closure"),
    [
        ("UNDETERMINED", gate_statistics(), "no demonstrated edge"),
        ("FAIL_CONTROL", gate_statistics(), "no demonstrated edge"),
        ("PASS", gate_statistics(excess=0.07), "no demonstrated edge"),
        ("PASS", gate_statistics(series=FLAT), "no demonstrated edge"),
        ("NOT_PASS_REFUSED", gate_statistics(), "undetermined"),
        (None, {}, "undetermined"),
        ("PASS", gate_statistics(drop="zero_recovery|without_dividends|base"), "undetermined"),
        ("PASS", gate_statistics(series=()), "undetermined"),
        ("PASS", {**gate_statistics(), "tracker": {"refused": "tracker_gap:2010-01-04"}}, "undetermined"),
    ],
)
def test_a_miss_or_an_unavailable_input_does_not_promote(
    verdict: str | None, statistics: dict[str, Any], closure: str
) -> None:
    result = _promote(verdict, statistics)
    assert (result["promote"], result["closure"]) == (False, closure)


def test_the_bar_is_read_from_the_frozen_block_not_the_constant() -> None:
    block = hh.decode_form(hh.canonical_form({**_block()[0], "bar": 0.5}))
    result = hunt_gate.promotion(block, verdict="PASS", reasons=(), statistics=gate_statistics())
    assert not result["promote"] and result["m_val"] == pytest.approx(0.17 - 0.5)


# --- lineage and terminal classification --------------------------------------------------------


def test_the_lineage_is_matched_by_family_or_signal_hash() -> None:
    (signal_hash,) = hh.LAST_LOOK_LINEAGES["extreme_move_illiquid"]
    assert hh.lineage_of("extreme_move_illiquid", None) == "extreme_move_illiquid"
    assert hh.lineage_of("renamed_family", signal_hash) == "extreme_move_illiquid"
    assert hh.lineage_of("overnight_gap", "0" * 64) is None


def test_the_lineage_hash_is_hunt_2s_signal_module() -> None:
    assert hh.signal_code_sha256("extreme_move_illiquid:score") in hh.LAST_LOOK_LINEAGES["extreme_move_illiquid"]


@pytest.mark.parametrize(
    ("code", "substantive"),
    [
        ("hunt_gate_condition_not_met:excess_at_bar", True),
        ("hunt_gate_stale:conditions", True),
        ("inherited_discovery_invalid:outcome_sha256", True),
        ("pin_a76417b5ea50_stale_harness_model", True),
        ("gate_decision_pins_stale", False),
        ("declaration_numbers_stale", False),
        ("register_disagrees_with_log:hunt-3-validation_missing", False),
        ("document_not_canonical", False),
    ],
)
def test_only_substantive_refusals_close_a_gated_hunt(code: str, substantive: bool) -> None:
    assert hunt_door._substantive(code) is substantive


def test_every_pins_lineage_is_read_not_the_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review #3458: a later pin's lineage is not dropped."""
    from tests.test_hunt_harness import trial_spec

    monkeypatch.setattr(hh, "LAST_LOOK_LINEAGES", {"b_family": frozenset({"b" * 64}), "c_family": frozenset()})
    pins = [trial_spec(family="a_family"), trial_spec(family="c_family"), trial_spec(family="b_family", lag=2)]
    assert hunt_door._pin_lineages(pins) == ["b_family", "c_family"]
    assert hunt_door._pin_lineages([trial_spec(family="a_family")]) == [None]


def test_hunt_3_closed_on_its_immutable_readout_record() -> None:
    """The closure text matches the record the one look wrote (#3454 "At validation, one look")."""
    from scripts.run_hunt_3_validation import RECORD_PATH, RECORD_SHA256

    raw = json.loads((Path(__file__).resolve().parents[1] / RECORD_PATH).read_text())
    assert hunt_door.document_sha256(raw) == RECORD_SHA256
    record = hh.decode_form(raw)
    assert (record["hunt_id"], record["hunt_trial_id"], record["complete"]) == ("hunt-3", 3, True)
    assert record["outcome_sha256"] == "921b3a61cbd615dec98a715f9a2348287152a9ac5e06d4f9d8681ecfe85ce40b"
    assert (record["promote"], record["closure"], record["verdict"]) == (False, "no demonstrated edge", "UNDETERMINED")
    assert record["unavailable"] == []
    assert max(record["base_excess_ann"].values()) == pytest.approx(-0.3315, abs=5e-4)
    assert min(record["base_excess_ann"].values()) == pytest.approx(-0.3903, abs=5e-4)
    assert record["excess_t"]["t_stat"] == pytest.approx(-6.94, abs=5e-3)
    assert record["m_val"] == pytest.approx(-0.470, abs=5e-4)
    assert "hunt_trial_id 3" in hh.HUNT_CLOSED["hunt-3"] and "-6.94" in hh.HUNT_CLOSED["hunt-3"]
    assert set(hh.CLOSED_LINEAGES) == set(hh.LAST_LOOK_LINEAGES) == {"extreme_move_illiquid"}
