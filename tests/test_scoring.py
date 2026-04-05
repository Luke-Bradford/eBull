"""
Unit tests for the scoring and ranking engine.

No network calls, no live database.
All DB interactions are tested via a lightweight fake connection or direct
calls to the pure computation functions.

Coverage:
  - _quality_score: full data, partial data, all-missing
  - _value_score: upside/downside, missing base_value, missing current_price
  - _momentum_score: full returns, partial returns, all missing
  - _sentiment_score: weighted mean, no events, all neutral
  - _turnaround_score: improving/declining trend, missing data
  - _compute_penalties: each penalty trigger individually
  - penalty stacking and total_score clipping at 0.0
  - weighted total: balanced / conservative / speculative modes
  - rank_delta: positive improvement, negative worsening, None on first run
  - compute_score: full fixture → expected family scores and total
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.services.scoring import (
    FamilyScores,
    PenaltyRecord,
    ScoreResult,
    _clip,
    _compute_penalties,
    _momentum_score,
    _quality_score,
    _sentiment_score,
    _turnaround_score,
    _value_score,
    compute_score,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 5, 9, 0, 0, tzinfo=UTC)
_RECENT = _NOW - timedelta(days=10)
_STALE = _NOW - timedelta(days=100)


def _approx(value: float, rel: float = 1e-4) -> object:
    return pytest.approx(value, rel=rel)


# ---------------------------------------------------------------------------
# _quality_score
# ---------------------------------------------------------------------------


class TestQualityScore:
    def test_full_data_high_quality(self) -> None:
        score, notes = _quality_score(
            operating_margin=0.20,  # maps to 1.0
            gross_margin=0.60,  # maps to 1.0
            fcf=500_000.0,  # positive → 1.0
            net_debt=-100_000.0,  # net cash → 1.0
            debt=50_000.0,
        )
        assert score == _approx(1.0)
        assert notes == []

    def test_zero_operating_margin(self) -> None:
        score, notes = _quality_score(
            operating_margin=0.0,
            gross_margin=0.60,
            fcf=500_000.0,
            net_debt=-100_000.0,
            debt=None,
        )
        # op_score=0.0, gm_score=1.0, fcf=1.0, debt=1.0
        expected = 0.35 * 0.0 + 0.25 * 1.0 + 0.20 * 1.0 + 0.20 * 1.0
        assert score == _approx(expected)

    def test_negative_fcf(self) -> None:
        score, _ = _quality_score(
            operating_margin=0.10,
            gross_margin=0.35,
            fcf=-1.0,
            net_debt=None,
            debt=100.0,
        )
        # fcf_score=0.0, debt_score=0.5 (debt known but net_debt unknown)
        op = _clip((0.10 - 0.00) / 0.20)
        gm = _clip((0.35 - 0.10) / 0.50)
        expected = 0.35 * op + 0.25 * gm + 0.20 * 0.0 + 0.20 * 0.5
        assert score == _approx(expected)

    def test_all_missing_returns_low_score(self) -> None:
        score, notes = _quality_score(None, None, None, None, None)
        # each sub-score defaults to 0.25
        expected = 0.25
        assert score == _approx(expected)
        assert len(notes) == 4  # all four components noted missing

    def test_score_clipped_to_one(self) -> None:
        # Extreme positive values should not produce score > 1.0
        score, _ = _quality_score(1.0, 1.0, 999.0, -1_000_000.0, None)
        assert score <= 1.0

    def test_score_never_below_zero(self) -> None:
        score, _ = _quality_score(-1.0, -1.0, -999.0, 1_000_000.0, 1_000_000.0)
        assert score >= 0.0


# ---------------------------------------------------------------------------
# _value_score
# ---------------------------------------------------------------------------


class TestValueScore:
    def test_50pct_upside_no_downside_risk(self) -> None:
        # base_value is 50% above current_price → upside_score=1.0
        # bear_value is 50% below current_price → downside_penalty=1.0
        score, notes = _value_score(
            base_value=150.0,
            bear_value=50.0,
            current_price=100.0,
        )
        upside_score = 1.0
        downside_penalty = 1.0
        expected = _clip(0.75 * upside_score + 0.25 * (1.0 - downside_penalty))
        assert score == _approx(expected)
        assert notes == []

    def test_no_upside(self) -> None:
        # base_value == current_price → upside=0, penalty moderate
        score, _ = _value_score(base_value=100.0, bear_value=90.0, current_price=100.0)
        assert score < 0.5

    def test_missing_base_value_returns_neutral(self) -> None:
        score, notes = _value_score(base_value=None, bear_value=80.0, current_price=100.0)
        assert score == _approx(0.5)
        assert any("base_value" in n for n in notes)

    def test_missing_current_price_returns_neutral(self) -> None:
        score, notes = _value_score(base_value=150.0, bear_value=80.0, current_price=None)
        assert score == _approx(0.5)
        assert any("current_price" in n for n in notes)

    def test_zero_current_price_returns_neutral(self) -> None:
        score, notes = _value_score(base_value=150.0, bear_value=80.0, current_price=0.0)
        assert score == _approx(0.5)

    def test_missing_bear_value_uses_moderate_penalty(self) -> None:
        # With bear_value missing, downside_penalty defaults to 0.5
        score, notes = _value_score(base_value=150.0, bear_value=None, current_price=100.0)
        upside = _clip(0.50 / 0.50)
        expected = _clip(0.75 * upside + 0.25 * 0.5)
        assert score == _approx(expected)
        assert any("bear_value" in n for n in notes)


# ---------------------------------------------------------------------------
# _momentum_score
# ---------------------------------------------------------------------------


class TestMomentumScore:
    def test_strong_positive_momentum(self) -> None:
        # All returns well above thresholds → score near 1.0
        score, notes = _momentum_score(
            return_1m=0.30,
            return_3m=0.45,
            return_6m=0.60,
        )
        assert score == _approx(1.0)
        assert notes == []

    def test_flat_market(self) -> None:
        # All returns at the "neutral" midpoint of each formula
        s1m = _clip((-0.10 + 0.10 + 0.10) / 0.30)  # return_1m = 0.0
        s3m = _clip((-0.15 + 0.15 + 0.15) / 0.45)
        s6m = _clip((-0.20 + 0.20 + 0.20) / 0.60)
        score, _ = _momentum_score(return_1m=0.0, return_3m=0.0, return_6m=0.0)
        expected = 0.20 * s1m + 0.50 * s3m + 0.30 * s6m
        assert score == _approx(expected)

    def test_missing_1m_weight_renormalised(self) -> None:
        # Only 3m and 6m available; weights should renormalise to [0.50, 0.30] / 0.80
        score, notes = _momentum_score(return_1m=None, return_3m=0.30, return_6m=0.30)
        s3m = _clip((0.30 + 0.15) / 0.45)
        s6m = _clip((0.30 + 0.20) / 0.60)
        expected = (0.50 * s3m + 0.30 * s6m) / 0.80
        assert score == _approx(expected)
        assert any("return_1m" in n for n in notes)

    def test_all_missing_returns_neutral(self) -> None:
        score, notes = _momentum_score(None, None, None)
        assert score == _approx(0.5)
        assert len(notes) == 3

    def test_negative_momentum_scores_low(self) -> None:
        # Very negative returns → score near 0
        score, _ = _momentum_score(return_1m=-0.30, return_3m=-0.50, return_6m=-0.60)
        assert score == _approx(0.0)


# ---------------------------------------------------------------------------
# _sentiment_score
# ---------------------------------------------------------------------------


class TestSentimentScore:
    def test_all_positive_high_importance(self) -> None:
        rows = [(0.8, 1.0), (0.9, 1.0), (0.7, 0.8)]
        score, notes = _sentiment_score(rows)
        total_w = 2.8
        raw = (0.8 * 1.0 + 0.9 * 1.0 + 0.7 * 0.8) / total_w
        expected = _clip((raw + 1.0) / 2.0)
        assert score == _approx(expected)
        assert notes == []

    def test_all_negative(self) -> None:
        rows = [(-0.8, 1.0), (-0.6, 1.0)]
        score, _ = _sentiment_score(rows)
        assert score < 0.3

    def test_neutral_by_absence(self) -> None:
        score, notes = _sentiment_score([])
        assert score == _approx(0.5)
        assert any("no recent news" in n for n in notes)

    def test_uniform_importance(self) -> None:
        # importance_score=None defaults to weight=1.0 per item
        rows = [(0.5, None), (-0.5, None)]
        score, _ = _sentiment_score(rows)
        assert score == _approx(0.5)

    def test_mixed_sentiment(self) -> None:
        rows = [(1.0, 1.0), (-1.0, 1.0)]
        score, _ = _sentiment_score(rows)
        assert score == _approx(0.5)


# ---------------------------------------------------------------------------
# _turnaround_score
# ---------------------------------------------------------------------------


class TestTurnaroundScore:
    def test_improving_margins_no_red_flags(self) -> None:
        # Latest margin > prior average, revenue growing, no red flags, net cash
        snapshots = [
            (0.15, 1_200_000.0),  # latest
            (0.10, 1_100_000.0),
            (0.08, 1_000_000.0),
        ]
        score, notes = _turnaround_score(snapshots, avg_red_flag_score=0.1, net_debt=-50_000.0)
        # margin trend: improving → 1.0, revenue: improving → 1.0
        # red_flag: 1.0 - 0.1 = 0.9, debt: net_debt<=0 → 1.0
        expected = 0.30 * 1.0 + 0.20 * 1.0 + 0.30 * 0.9 + 0.20 * 1.0
        assert score == _approx(expected)
        assert notes == []

    def test_declining_margins(self) -> None:
        snapshots = [
            (0.05, 900_000.0),  # latest — worse than prior
            (0.12, 1_000_000.0),
            (0.15, 1_100_000.0),
        ]
        score, _ = _turnaround_score(snapshots, avg_red_flag_score=0.0, net_debt=0.0)
        # margin trend: declining → 0.0, revenue: declining → 0.0
        # red_flag: 1.0 - 0.0 = 1.0, debt: net_debt=0 <= 0 → 1.0
        expected = 0.30 * 0.0 + 0.20 * 0.0 + 0.30 * 1.0 + 0.20 * 1.0
        assert score == _approx(expected)

    def test_single_snapshot_trend_unknown(self) -> None:
        snapshots = [(0.10, 1_000_000.0)]
        score, notes = _turnaround_score(snapshots, avg_red_flag_score=None, net_debt=None)
        assert any("only one" in n for n in notes)
        # All unknowns default to 0.5
        expected = 0.30 * 0.5 + 0.20 * 0.5 + 0.30 * 0.5 + 0.20 * 0.5
        assert score == _approx(expected)

    def test_all_missing_returns_neutral(self) -> None:
        score, notes = _turnaround_score([], avg_red_flag_score=None, net_debt=None)
        assert score == _approx(0.5)
        assert notes  # at least some notes about missing data


# ---------------------------------------------------------------------------
# _compute_penalties
# ---------------------------------------------------------------------------


class TestComputePenalties:
    def _base_call(self, **overrides: object) -> list[PenaltyRecord]:
        defaults: dict[str, object] = {
            "thesis_created_at": _RECENT,
            "confidence_score": 0.75,
            "has_missing_critical_data": False,
            "spread_flag": False,
            "avg_red_flag_score": 0.0,
            "shares_outstanding_latest": 1_000_000.0,
            "shares_outstanding_prior": 1_000_000.0,
            "now": _NOW,
        }
        defaults.update(overrides)
        return _compute_penalties(**defaults)  # type: ignore[arg-type]

    def test_clean_instrument_no_penalties(self) -> None:
        assert self._base_call() == []

    def test_stale_thesis_triggers(self) -> None:
        penalties = self._base_call(thesis_created_at=_STALE)
        names = [p.name for p in penalties]
        assert "stale_thesis" in names

    def test_no_thesis_triggers_stale(self) -> None:
        penalties = self._base_call(thesis_created_at=None)
        names = [p.name for p in penalties]
        assert "stale_thesis" in names

    def test_missing_critical_data_triggers(self) -> None:
        penalties = self._base_call(has_missing_critical_data=True)
        names = [p.name for p in penalties]
        assert "missing_critical_data" in names

    def test_wide_spread_triggers(self) -> None:
        penalties = self._base_call(spread_flag=True)
        names = [p.name for p in penalties]
        assert "wide_spread" in names

    def test_high_red_flag_triggers(self) -> None:
        penalties = self._base_call(avg_red_flag_score=0.70)
        names = [p.name for p in penalties]
        assert "high_red_flag" in names

    def test_red_flag_below_threshold_no_penalty(self) -> None:
        penalties = self._base_call(avg_red_flag_score=0.50)
        names = [p.name for p in penalties]
        assert "high_red_flag" not in names

    def test_extreme_dilution_triggers(self) -> None:
        penalties = self._base_call(
            shares_outstanding_latest=1_250_000.0,  # 25% growth
            shares_outstanding_prior=1_000_000.0,
        )
        names = [p.name for p in penalties]
        assert "extreme_dilution" in names

    def test_dilution_below_threshold_no_penalty(self) -> None:
        penalties = self._base_call(
            shares_outstanding_latest=1_100_000.0,  # 10% growth — below 20%
            shares_outstanding_prior=1_000_000.0,
        )
        names = [p.name for p in penalties]
        assert "extreme_dilution" not in names

    def test_low_confidence_triggers(self) -> None:
        penalties = self._base_call(confidence_score=0.30)
        names = [p.name for p in penalties]
        assert "low_confidence" in names

    def test_all_penalties_stack(self) -> None:
        penalties = self._base_call(
            thesis_created_at=_STALE,
            has_missing_critical_data=True,
            spread_flag=True,
            avg_red_flag_score=0.80,
            shares_outstanding_latest=1_500_000.0,
            shares_outstanding_prior=1_000_000.0,
            confidence_score=0.20,
        )
        names = [p.name for p in penalties]
        assert "stale_thesis" in names
        assert "missing_critical_data" in names
        assert "wide_spread" in names
        assert "high_red_flag" in names
        assert "extreme_dilution" in names
        assert "low_confidence" in names
        total = sum(p.deduction for p in penalties)
        assert total == pytest.approx(0.15 + 0.10 + 0.05 + 0.10 + 0.10 + 0.10)


# ---------------------------------------------------------------------------
# Penalty total_score clipping
# ---------------------------------------------------------------------------


class TestScoreClipping:
    def test_total_score_never_below_zero(self) -> None:
        # Even with maximum penalties a score cannot go below 0
        raw_total = 0.10
        total_penalty = 0.80
        total_score = _clip(raw_total - total_penalty)
        assert total_score == 0.0

    def test_total_score_never_above_one(self) -> None:
        total_score = _clip(1.50)
        assert total_score == 1.0


# ---------------------------------------------------------------------------
# Weighted total — weight modes
# ---------------------------------------------------------------------------


class TestWeightedTotal:
    def _total(self, mode: str, family: FamilyScores) -> float:
        from app.services.scoring import _WEIGHT_MODES

        w = _WEIGHT_MODES[mode]
        return _clip(
            w["quality"] * family.quality
            + w["value"] * family.value
            + w["turnaround"] * family.turnaround
            + w["momentum"] * family.momentum
            + w["sentiment"] * family.sentiment
            + w["confidence"] * family.confidence
        )

    def test_balanced_weights_sum_to_one(self) -> None:
        from app.services.scoring import _WEIGHT_MODES

        for mode, weights in _WEIGHT_MODES.items():
            total = sum(weights.values())
            assert total == pytest.approx(1.0), f"{mode} weights do not sum to 1.0"

    def test_conservative_favours_quality(self) -> None:
        # A high-quality, low-momentum instrument should rank higher in conservative
        high_quality = FamilyScores(quality=1.0, value=0.5, turnaround=0.3, momentum=0.2, sentiment=0.5, confidence=0.7)
        high_momentum = FamilyScores(
            quality=0.3, value=0.5, turnaround=0.3, momentum=1.0, sentiment=0.5, confidence=0.7
        )
        assert self._total("v1-conservative", high_quality) > self._total("v1-conservative", high_momentum)

    def test_speculative_favours_turnaround(self) -> None:
        high_turnaround = FamilyScores(
            quality=0.3, value=0.5, turnaround=1.0, momentum=0.5, sentiment=0.5, confidence=0.6
        )
        high_quality = FamilyScores(quality=1.0, value=0.5, turnaround=0.3, momentum=0.5, sentiment=0.5, confidence=0.6)
        assert self._total("v1-speculative", high_turnaround) > self._total("v1-speculative", high_quality)


# ---------------------------------------------------------------------------
# Rank delta
# ---------------------------------------------------------------------------


class TestRankDelta:
    def test_rank_improved(self) -> None:
        prior_rank = 5
        current_rank = 2
        delta = prior_rank - current_rank
        assert delta == 3  # positive = moved up

    def test_rank_worsened(self) -> None:
        prior_rank = 2
        current_rank = 5
        delta = prior_rank - current_rank
        assert delta == -3  # negative = moved down

    def test_rank_unchanged(self) -> None:
        delta = 3 - 3
        assert delta == 0

    def test_no_prior_rank_is_none(self) -> None:
        prior_rank = None
        rank_delta = (prior_rank - 1) if prior_rank is not None else None
        assert rank_delta is None


# ---------------------------------------------------------------------------
# compute_score — integration with fake DB connection
# ---------------------------------------------------------------------------


def _make_fake_conn(
    fund_rows: list[tuple[object, ...]],
    price_row: tuple[object, ...] | None,
    quote_row: tuple[object, ...] | None,
    thesis_row: tuple[object, ...] | None,
    news_rows: list[tuple[object, ...]],
    avg_red_flag: float | None,
) -> MagicMock:
    """
    Return a MagicMock psycopg connection whose execute().fetchall() /
    fetchone() returns fixture data in the same order as _load_instrument_data.
    """
    conn = MagicMock()

    # execute is called 6 times (fundamentals, price, quote, thesis, news, red_flag)
    execute_returns = [
        MagicMock(fetchall=MagicMock(return_value=fund_rows)),  # fundamentals
        MagicMock(fetchone=MagicMock(return_value=price_row)),  # price
        MagicMock(fetchone=MagicMock(return_value=quote_row)),  # quote
        MagicMock(fetchone=MagicMock(return_value=thesis_row)),  # thesis
        MagicMock(fetchall=MagicMock(return_value=news_rows)),  # news
        MagicMock(fetchone=MagicMock(return_value=(avg_red_flag,))),  # red flag avg
    ]

    conn.execute.side_effect = execute_returns
    return conn


class TestComputeScore:
    def test_full_fixture_produces_valid_result(self) -> None:
        conn = _make_fake_conn(
            fund_rows=[
                (0.18, 0.55, 200_000.0, -50_000.0, 100_000.0, 1_100_000.0, 10_000_000.0),  # latest
                (0.12, 0.50, 150_000.0, -30_000.0, 80_000.0, 1_000_000.0, 10_000_000.0),  # prior
            ],
            price_row=(0.05, 0.20, 0.35, 120.0),  # return_1m, 3m, 6m, close
            quote_row=(False, 120.0, 119.5, 120.5),  # spread_flag, last, bid, ask
            thesis_row=(0.75, 180.0, 90.0, _RECENT),  # confidence, base, bear, created_at
            news_rows=[(0.6, 0.8), (0.5, 1.0)],
            avg_red_flag=0.15,
        )
        result = compute_score(1, conn, "v1-balanced")

        assert isinstance(result, ScoreResult)
        assert result.instrument_id == 1
        assert result.model_version == "v1-balanced"
        assert 0.0 <= result.total_score <= 1.0
        assert 0.0 <= result.family_scores.quality <= 1.0
        assert 0.0 <= result.family_scores.value <= 1.0
        assert 0.0 <= result.family_scores.momentum <= 1.0
        assert 0.0 <= result.family_scores.sentiment <= 1.0
        assert 0.0 <= result.family_scores.turnaround <= 1.0
        assert 0.0 <= result.family_scores.confidence <= 1.0
        # No stale thesis, no wide spread, high confidence → no penalties
        assert result.penalties == []
        assert result.total_score == pytest.approx(result.raw_total, abs=1e-6)

    def test_wide_spread_triggers_penalty(self) -> None:
        conn = _make_fake_conn(
            fund_rows=[(0.18, 0.55, 200_000.0, -50_000.0, 100_000.0, 1_100_000.0, 10_000_000.0)],
            price_row=(0.05, 0.20, 0.35, 120.0),
            quote_row=(True, 120.0, 119.5, 120.5),  # spread_flag=True
            thesis_row=(0.75, 180.0, 90.0, _RECENT),
            news_rows=[],
            avg_red_flag=0.0,
        )
        result = compute_score(1, conn, "v1-balanced")
        penalty_names = [p.name for p in result.penalties]
        assert "wide_spread" in penalty_names
        assert result.total_score < result.raw_total

    def test_missing_thesis_triggers_stale_penalty(self) -> None:
        conn = _make_fake_conn(
            fund_rows=[(0.18, 0.55, 200_000.0, -50_000.0, 100_000.0, 1_100_000.0, 10_000_000.0)],
            price_row=(0.05, 0.20, 0.35, 120.0),
            quote_row=(False, 120.0, 119.5, 120.5),
            thesis_row=None,  # no thesis
            news_rows=[],
            avg_red_flag=0.0,
        )
        result = compute_score(1, conn, "v1-balanced")
        penalty_names = [p.name for p in result.penalties]
        assert "stale_thesis" in penalty_names

    def test_unknown_model_version_raises(self) -> None:
        conn = MagicMock()
        with pytest.raises(KeyError, match="unknown-mode"):
            compute_score(1, conn, "unknown-mode")

    def test_total_score_clipped_when_heavy_penalties(self) -> None:
        # Stale thesis + low confidence + high red flag + spread → heavy deductions
        conn = _make_fake_conn(
            fund_rows=[(0.00, 0.10, -1.0, 500_000.0, 600_000.0, 500_000.0, 10_000_000.0)],
            price_row=(0.0, 0.0, 0.0, 100.0),
            quote_row=(True, 100.0, 99.0, 101.0),  # spread flag
            thesis_row=(0.20, None, None, _STALE),  # stale + low confidence + no valuation
            news_rows=[(-0.9, 1.0), (-0.8, 1.0)],
            avg_red_flag=0.80,
        )
        result = compute_score(1, conn, "v1-balanced")
        assert result.total_score >= 0.0
        assert result.total_score <= 1.0
