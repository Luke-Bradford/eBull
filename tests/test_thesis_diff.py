"""Fast-tier tests for the pure thesis-diff module (#2013).

No DB: field-class coverage, null transitions both directions, the
materiality boundary, memo section split edges, break-condition
normalization, and Decimal/float input parity.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.thesis_diff import (
    _MATERIAL_REL_MOVE,
    ThesisDiff,
    compute_thesis_diff,
)


def _row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "thesis_version": 1,
        "stance": "buy",
        "thesis_type": "value",
        "confidence_score": 0.6,
        "buy_zone_low": 90.0,
        "buy_zone_high": 100.0,
        "base_value": 120.0,
        "bull_value": 150.0,
        "bear_value": 80.0,
        "break_conditions_json": ["Margin collapse below 20%", "Debt/EBITDA > 1.5x"],
        "memo_markdown": "### Business\nSolid moat.\n### Valuation\nCheap vs peers.",
        "prompt_version": "v4",
        "model": "claude-sonnet-4-6",
    }
    base.update(overrides)
    return base


def _diff(prev_overrides: dict[str, object], curr_overrides: dict[str, object]) -> ThesisDiff:
    prev = _row(thesis_version=1, **prev_overrides)
    curr = _row(thesis_version=2, **curr_overrides)
    return compute_thesis_diff(prev, curr)


class TestNoChange:
    def test_identical_rows_not_material_empty_summary(self) -> None:
        d = _diff({}, {})
        assert d.material is False
        assert d.summary == ""
        assert d.stance is None
        assert d.thesis_type is None
        assert d.confidence is None
        assert d.targets == ()
        assert d.break_conditions_added == ()
        assert d.break_conditions_removed == ()
        assert d.memo_sections_changed == ()
        assert d.prev_version == 1
        assert d.curr_version == 2


class TestStanceAndType:
    def test_stance_change_is_material_and_summarized(self) -> None:
        d = _diff({"stance": "buy"}, {"stance": "hold"})
        assert d.stance is not None
        assert (d.stance.from_value, d.stance.to_value) == ("buy", "hold")
        assert d.material is True
        assert "stance buy→hold" in d.summary

    def test_thesis_type_change_alone_is_material(self) -> None:
        d = _diff({"thesis_type": "value"}, {"thesis_type": "turnaround"})
        assert d.thesis_type is not None
        assert d.material is True
        assert "type value→turnaround" in d.summary


class TestTargets:
    def test_null_to_value_is_added_and_material(self) -> None:
        d = _diff({"base_value": None}, {"base_value": 110.0})
        (t,) = d.targets
        assert (t.field, t.kind, t.from_value, t.to_value) == ("base_value", "added", None, 110.0)
        assert t.rel_move is None
        assert d.material is True
        assert "base added (110)" in d.summary

    def test_value_to_null_is_removed_and_material(self) -> None:
        d = _diff({"bear_value": 80.0}, {"bear_value": None})
        (t,) = d.targets
        assert (t.field, t.kind) == ("bear_value", "removed")
        assert d.material is True
        assert "bear removed" in d.summary

    def test_move_below_threshold_not_material(self) -> None:
        # 4.9% < _MATERIAL_REL_MOVE (5%)
        d = _diff({"base_value": 100.0}, {"base_value": 104.9})
        (t,) = d.targets
        assert t.kind == "moved"
        assert t.rel_move is not None and t.rel_move < _MATERIAL_REL_MOVE
        assert d.material is False
        # Summary still reports the (non-material) move for the pane detail.
        assert "base 100→104.9" in d.summary

    def test_move_at_threshold_is_material(self) -> None:
        d = _diff({"base_value": 100.0}, {"base_value": 105.1})
        assert d.material is True
        assert "(+5%)" in d.summary

    def test_move_from_zero_base_is_material_with_none_rel_move(self) -> None:
        d = _diff({"base_value": 0.0}, {"base_value": 10.0})
        (t,) = d.targets
        assert t.kind == "moved"
        assert t.rel_move is None
        assert d.material is True

    def test_downward_move_signed_percentage(self) -> None:
        d = _diff({"base_value": 120.0}, {"base_value": 98.0})
        assert "base 120→98 (-18%)" in d.summary

    def test_both_null_is_no_change(self) -> None:
        d = _diff({"bull_value": None}, {"bull_value": None})
        assert d.targets == ()
        assert d.material is False


class TestConfidence:
    def test_confidence_delta_never_material(self) -> None:
        d = _diff({"confidence_score": 0.6}, {"confidence_score": 0.2})
        assert d.confidence is not None
        assert d.confidence.delta == pytest.approx(-0.4)
        assert d.material is False
        assert d.summary == ""

    def test_confidence_null_side_yields_none_delta(self) -> None:
        d = _diff({"confidence_score": None}, {"confidence_score": 0.5})
        assert d.confidence is not None
        assert d.confidence.delta is None


class TestBreakConditions:
    def test_added_and_removed_with_normalization(self) -> None:
        d = _diff(
            {"break_conditions_json": ["Margin  collapse below 20%"]},
            {"break_conditions_json": ["margin collapse below 20%", "New CEO departs"]},
        )
        # Whitespace/case-normalized match: the margin condition is unchanged.
        assert d.break_conditions_added == ("New CEO departs",)
        assert d.break_conditions_removed == ()
        assert d.material is False  # break conditions are informational

    def test_null_payload_tolerated(self) -> None:
        d = _diff({"break_conditions_json": None}, {"break_conditions_json": ["X breaks"]})
        assert d.break_conditions_added == ("X breaks",)


class TestMemoSections:
    def test_added_removed_changed(self) -> None:
        d = _diff(
            {"memo_markdown": "### Business\nSolid moat.\n### Valuation\nCheap."},
            {"memo_markdown": "### Business\nMoat eroding.\n### Risks\nNew risks."},
        )
        assert d.memo_sections_added == ("Risks",)
        assert d.memo_sections_removed == ("Valuation",)
        assert d.memo_sections_changed == ("Business",)
        assert d.material is False  # memo sections are informational

    def test_whitespace_only_body_change_not_changed(self) -> None:
        d = _diff(
            {"memo_markdown": "### Business\nSolid  moat.\n"},
            {"memo_markdown": "### Business\nSolid moat."},
        )
        assert d.memo_sections_changed == ()

    def test_headingless_memo_diffs_as_body_pseudo_section(self) -> None:
        d = _diff(
            {"memo_markdown": "Plain memo without headings."},
            {"memo_markdown": "Different plain memo."},
        )
        assert d.memo_sections_changed == ("(body)",)

    def test_duplicate_headings_collapse_to_one_key(self) -> None:
        d = _diff(
            {"memo_markdown": "### Risks\nA.\n### Risks\nB."},
            {"memo_markdown": "### Risks\nA.\n### Risks\nB."},
        )
        assert d.memo_sections_changed == ()
        assert d.memo_sections_added == ()


class TestProvenanceAndInputTypes:
    def test_prompt_version_and_model_change_not_material(self) -> None:
        d = _diff(
            {"prompt_version": "v3", "model": "claude-sonnet-4-5"},
            {"prompt_version": "v4", "model": "claude-sonnet-4-6"},
        )
        assert d.prompt_version is not None
        assert d.model is not None
        assert d.material is False

    def test_decimal_inputs_match_float_inputs(self) -> None:
        d_dec = _diff(
            {"base_value": Decimal("120.000000")},
            {"base_value": Decimal("98.000000")},
        )
        d_flt = _diff({"base_value": 120.0}, {"base_value": 98.0})
        assert d_dec.targets == d_flt.targets
        assert d_dec.material is d_flt.material is True

    def test_decimal_equal_values_no_change(self) -> None:
        d = _diff({"base_value": Decimal("120.000000")}, {"base_value": 120.0})
        assert d.targets == ()
