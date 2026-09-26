"""#3389 slice (c): a family with no observed input is a default fill, never a verdict.

Pure: every family function marks its no-input path with ``NO_INPUT_NOTE``, and
``family_usability`` maps the notes to usable / missing / stale / quarantined.
"""

from __future__ import annotations

import pytest

from app.services.scoring import (
    _WEIGHT_MODES,
    FAMILIES,
    NO_INPUT_NOTE,
    _momentum_score,
    _quality_score,
    _sentiment_score,
    _turnaround_score,
    _value_score,
    family_evidence,
    family_usability,
)


class TestNoInputPathsAreMarked:
    def test_quality_with_no_input_is_marked_and_partial_is_not(self) -> None:
        score, notes = _quality_score(None, None, None, None, None)
        assert score == pytest.approx(0.25)  # the default fill that used to read as "poor quality"
        assert NO_INPUT_NOTE in notes
        _, partial = _quality_score(0.1, None, None, None, None)
        assert NO_INPUT_NOTE not in partial
        _, debt_only = _quality_score(None, None, None, None, 5.0)
        assert NO_INPUT_NOTE not in debt_only

    @pytest.mark.parametrize(
        ("kwargs", "missing"),
        [
            ({"base_value": 120.0, "bear_value": 80.0, "current_price": None}, True),
            ({"base_value": None, "bear_value": None, "current_price": 0.0}, True),
            ({"base_value": None, "bear_value": None, "current_price": 100.0}, True),
            ({"base_value": None, "bear_value": None, "current_price": 100.0, "pe_ratio": 15.0}, False),
            ({"base_value": 120.0, "bear_value": None, "current_price": 100.0}, False),
        ],
    )
    def test_value(self, kwargs: dict[str, float | None], missing: bool) -> None:
        _, notes = _value_score(**kwargs)  # type: ignore[arg-type]
        assert (NO_INPUT_NOTE in notes) is missing

    def test_momentum(self) -> None:
        _, none = _momentum_score(None, None, None)
        assert NO_INPUT_NOTE in none
        _, ta_all_none = _momentum_score(None, None, None, ta_indicators={"sma_200": None, "current_close": 10.0})
        assert NO_INPUT_NOTE in ta_all_none
        _, one_return = _momentum_score(0.05, None, None)
        assert NO_INPUT_NOTE not in one_return

    def test_sentiment(self) -> None:
        assert NO_INPUT_NOTE in _sentiment_score([])[1]
        assert NO_INPUT_NOTE in _sentiment_score([(None, 1.0)])[1]
        assert NO_INPUT_NOTE not in _sentiment_score([(0.2, 1.0)])[1]

    def test_turnaround_single_snapshots_are_not_a_trend(self) -> None:
        assert NO_INPUT_NOTE in _turnaround_score([], None, None)[1]
        assert NO_INPUT_NOTE in _turnaround_score([(0.1, 100.0)], None, None)[1]
        assert NO_INPUT_NOTE not in _turnaround_score([(0.1, 100.0), (0.05, 90.0)], None, None)[1]
        assert NO_INPUT_NOTE not in _turnaround_score([], 0.3, None)[1]
        assert NO_INPUT_NOTE not in _turnaround_score([], None, -5.0)[1]


class TestFamilyUsability:
    def test_missing_beats_stale_and_stale_needs_a_thesis_fed_family(self) -> None:
        notes = {family: [] for family in FAMILIES}
        notes["value"] = [NO_INPUT_NOTE]
        result = family_usability(
            notes, thesis_fed=frozenset({"value", "confidence"}), thesis_stale=True, thesis_quarantined=False
        )
        assert result == {
            "quality": "usable",
            "value": "missing",
            "turnaround": "usable",
            "momentum": "usable",
            "sentiment": "usable",
            "confidence": "stale",
        }

    def test_fresh_thesis_is_usable(self) -> None:
        notes = {family: [] for family in FAMILIES}
        result = family_usability(
            notes, thesis_fed=frozenset({"value", "confidence"}), thesis_stale=False, thesis_quarantined=False
        )
        assert set(result.values()) == {"usable"}

    def test_quarantined_thesis_explains_a_missing_thesis_family_only(self) -> None:
        notes = {family: [] for family in FAMILIES}
        notes["confidence"] = [NO_INPUT_NOTE]  # the rejected thesis carried the confidence
        notes["sentiment"] = [NO_INPUT_NOTE]  # unrelated absence stays "missing"
        # value found a fundamentals fallback, so it is usable despite the rejected thesis
        result = family_usability(notes, thesis_fed=frozenset(), thesis_stale=False, thesis_quarantined=True)
        assert result["confidence"] == "quarantined"
        assert result["sentiment"] == "missing"
        assert result["value"] == "usable"


class TestFamilyEvidence:
    def test_every_weighted_version_covers_every_family(self) -> None:
        for version, weights in _WEIGHT_MODES.items():
            evidence = family_evidence(version)
            assert evidence is not None
            assert set(evidence) == set(FAMILIES) == set(weights)
            assert {e.maturity for e in evidence.values()} == {"untested"}

    def test_unknown_version(self) -> None:
        assert family_evidence("v0-unknown") is None
