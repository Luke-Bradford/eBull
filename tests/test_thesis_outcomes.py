"""Pure-logic tests for the #2002 calibration-ledger capture predicates.

No DB: anchor parsing, data-anchored maturity, and the immature-pair
split (data_current / series_stalled / series_dead) are plain functions
over values — the house lean-test rule."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.thesis_outcomes import (
    HORIZONS,
    anchor_date_from_summary,
    classify_immature,
    is_mature,
)

_TODAY = date(2026, 7, 16)


def _summary(available: object = True, as_of: object = "2026-07-01") -> dict:
    return {"blocks": {"price_anchor": {"available": available, "as_of": as_of}}}


@pytest.mark.parametrize(
    ("summary", "expected"),
    [
        (_summary(), date(2026, 7, 1)),
        # ISO timestamp: only the date prefix is read (same as the DQ audit).
        (_summary(as_of="2026-07-01T12:00:00Z"), date(2026, 7, 1)),
        (_summary(available=False), None),
        (_summary(available=None), None),
        (_summary(as_of=None), None),
        (_summary(as_of="not-a-date"), None),
        ({"blocks": {}}, None),
        ({}, None),
        (None, None),
        ("price_anchor", None),
    ],
)
def test_anchor_date_from_summary(summary: object, expected: date | None) -> None:
    assert anchor_date_from_summary(summary) == expected


@pytest.mark.parametrize(
    ("anchor", "horizon", "max_price", "expected"),
    [
        # Maturity is data-anchored: the series must have printed AT or
        # PAST the due date; wall-clock never participates.
        (date(2026, 1, 5), 30, date(2026, 2, 4), True),
        (date(2026, 1, 5), 30, date(2026, 2, 5), True),
        (date(2026, 1, 5), 30, date(2026, 2, 3), False),
        (date(2026, 1, 5), 30, None, False),
        (date(2026, 1, 5), 365, date(2027, 1, 4), False),
        (date(2026, 1, 5), 365, date(2027, 1, 5), True),
    ],
)
def test_is_mature(anchor: date, horizon: int, max_price: date | None, expected: bool) -> None:
    assert is_mature(anchor, horizon, max_price) is expected


@pytest.mark.parametrize(
    ("tradable", "max_price", "due", "expected"),
    [
        # Live series, pair simply not due yet — the normal young-thesis case.
        (True, _TODAY, date(2027, 7, 1), "immature_data_current"),
        (True, date(2026, 7, 15), date(2026, 8, 8), "immature_data_current"),
        # Series stopped recently (delisted 5d ago), due within grace of the
        # last print — not yet provably dead.
        (False, date(2026, 7, 11), date(2026, 7, 18), "immature_series_stalled"),
        # Tradable but series stale just past grace, due still near last
        # print — stalled, recovers to data_current if ingest resumes.
        (True, date(2026, 6, 14), date(2026, 6, 20), "immature_series_stalled"),
        # Series ended far before the due print — dead; the print this pair
        # needs will never come.
        (False, date(2026, 4, 1), date(2026, 7, 1), "series_dead"),
        (True, date(2026, 6, 1), date(2027, 6, 1), "series_dead"),
        # No price series at all: absent data is a terminal verdict only
        # when the instrument is also untradable (Codex ckpt-2 Medium).
        (True, None, date(2026, 8, 1), "immature_series_stalled"),
        (False, None, date(2026, 8, 1), "series_dead"),
    ],
)
def test_classify_immature(tradable: bool, max_price: date | None, due: date, expected: str) -> None:
    assert classify_immature(is_tradable=tradable, max_price_date=max_price, due_date=due, today=_TODAY) == expected


def test_horizons_are_the_spec_set() -> None:
    """Schema CHECK pins (30, 90, 365); the constant must match."""
    assert HORIZONS == (30, 90, 365)


class TestAggregateScoreboard:
    """#2068 — pure cohort aggregation over plain values."""

    @staticmethod
    def _row(
        thesis_id: int = 1,
        model: str | None = "qwen3:14b",
        prompt_version: str | None = "v5",
        stance: str = "buy",
        base_value: Decimal | None = Decimal("110"),
        confidence_score: Decimal | None = Decimal("0.8"),
        is_tradable: bool = True,
        context_summary: object = None,
        max_price_date: date | None = date(2026, 7, 1),
    ) -> dict:
        return {
            "thesis_id": thesis_id,
            "model": model,
            "prompt_version": prompt_version,
            "stance": stance,
            "base_value": base_value,
            "confidence_score": confidence_score,
            "is_tradable": is_tradable,
            "context_summary": context_summary if context_summary is not None else _summary(),
            "max_price_date": max_price_date,
        }

    _TODAY = date(2026, 7, 10)

    def test_anchorless_thesis_counts_per_horizon(self) -> None:
        from app.services.thesis_outcomes import aggregate_scoreboard

        rows = [self._row(context_summary=_summary(available=False))]
        cohorts = aggregate_scoreboard(rows, {}, self._TODAY)
        assert len(cohorts) == 3
        assert all(c.total_theses == 1 and c.anchorless == 1 for c in cohorts)
        assert all(c.target_distance_mape is None for c in cohorts)

    def test_buy_hit_mape_and_brier(self) -> None:
        """base 110 vs realized 100 → MAPE 0.10; buy + positive return =
        hit; Brier (0.8 − 1)² = 0.04."""
        from app.services.thesis_outcomes import aggregate_scoreboard

        outcomes = {(1, 30): (Decimal("100"), Decimal("0.05"))}
        cohorts = aggregate_scoreboard([self._row()], outcomes, self._TODAY)
        c30 = next(c for c in cohorts if c.horizon_days == 30)
        assert c30.outcome_rows == 1
        assert c30.direction_claims == 1
        assert c30.stance_hit_rate == 1.0
        assert c30.target_distance_mape == pytest.approx(0.10)
        assert c30.conviction_brier == pytest.approx(0.04)

    def test_avoid_hit_on_negative_return(self) -> None:
        from app.services.thesis_outcomes import aggregate_scoreboard

        outcomes = {(1, 30): (Decimal("50"), Decimal("-0.2"))}
        cohorts = aggregate_scoreboard([self._row(stance="avoid", base_value=None)], outcomes, self._TODAY)
        c30 = next(c for c in cohorts if c.horizon_days == 30)
        assert c30.stance_hit_rate == 1.0
        assert c30.targets_absent == 1
        assert c30.target_distance_mape is None

    def test_zero_return_misses_both_directions(self) -> None:
        """Spec is strict > / < — a flat print is a miss for buy AND avoid."""
        from app.services.thesis_outcomes import aggregate_scoreboard

        outcomes = {(1, 30): (Decimal("100"), Decimal("0"))}
        for stance in ("buy", "avoid"):
            cohorts = aggregate_scoreboard([self._row(stance=stance)], outcomes, self._TODAY)
            c30 = next(c for c in cohorts if c.horizon_days == 30)
            assert c30.stance_hit_rate == 0.0

    def test_watch_excluded_from_direction_metrics_but_mape_counted(self) -> None:
        from app.services.thesis_outcomes import aggregate_scoreboard

        outcomes = {(1, 30): (Decimal("100"), Decimal("0.05"))}
        cohorts = aggregate_scoreboard([self._row(stance="watch")], outcomes, self._TODAY)
        c30 = next(c for c in cohorts if c.horizon_days == 30)
        assert c30.direction_claims == 0
        assert c30.stance_hit_rate is None
        assert c30.conviction_brier is None
        assert c30.target_distance_mape == pytest.approx(0.10)

    def test_null_confidence_counted_and_excluded_from_brier(self) -> None:
        from app.services.thesis_outcomes import aggregate_scoreboard

        outcomes = {(1, 30): (Decimal("100"), Decimal("0.05"))}
        cohorts = aggregate_scoreboard([self._row(confidence_score=None)], outcomes, self._TODAY)
        c30 = next(c for c in cohorts if c.horizon_days == 30)
        assert c30.confidence_absent == 1
        assert c30.conviction_brier is None
        assert c30.stance_hit_rate == 1.0  # hit-rate needs no confidence

    def test_uncaptured_pair_classified_immature(self) -> None:
        """Anchored thesis with no outcome row and a current series →
        immature_data_current (never classified ahead of the ledger)."""
        from app.services.thesis_outcomes import aggregate_scoreboard

        rows = [self._row(max_price_date=self._TODAY)]
        cohorts = aggregate_scoreboard(rows, {}, self._TODAY)
        assert all(c.immature_data_current == 1 for c in cohorts)

    def test_cohorts_split_by_model_and_prompt_version(self) -> None:
        from app.services.thesis_outcomes import aggregate_scoreboard

        rows = [
            self._row(thesis_id=1, model="qwen3:14b", prompt_version="v4"),
            self._row(thesis_id=2, model="qwen3:14b", prompt_version="v5"),
            self._row(thesis_id=3, model=None, prompt_version=None),
        ]
        cohorts = aggregate_scoreboard(rows, {}, self._TODAY)
        keys = {(c.model, c.prompt_version) for c in cohorts}
        assert keys == {("qwen3:14b", "v4"), ("qwen3:14b", "v5"), (None, None)}
        assert len(cohorts) == 9  # 3 cohorts x 3 horizons
        assert all(c.total_theses == 1 for c in cohorts)

    def test_coverage_counters_are_population_scoped(self) -> None:
        """Codex ckpt-2: targets_absent / confidence_absent /
        direction_claims count the whole cohort population — an immature
        thesis with no outcome row still contributes coverage."""
        from app.services.thesis_outcomes import aggregate_scoreboard

        rows = [
            self._row(thesis_id=1, stance="avoid", base_value=None, confidence_score=None, max_price_date=self._TODAY)
        ]
        cohorts = aggregate_scoreboard(rows, {}, self._TODAY)
        c30 = next(c for c in cohorts if c.horizon_days == 30)
        assert c30.outcome_rows == 0
        assert c30.direction_claims == 1
        assert c30.targets_absent == 1
        assert c30.confidence_absent == 1
        assert c30.immature_data_current == 1
        assert c30.stance_hit_rate is None
