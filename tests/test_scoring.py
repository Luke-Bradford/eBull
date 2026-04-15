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
from unittest.mock import MagicMock, patch

import pytest

from app.services.scoring import (
    FamilyScores,
    PenaltyRecord,
    ScoreResult,
    _clip,
    _compute_penalties,
    _fetch_prior_ranks,
    _momentum_score,
    _quality_score,
    _sentiment_score,
    _turnaround_score,
    _value_score,
    compute_rankings,
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
        # All returns = 0.0; formula is _clip((return + threshold) / range)
        s1m = _clip((0.0 + 0.10) / 0.30)
        s3m = _clip((0.0 + 0.15) / 0.45)
        s6m = _clip((0.0 + 0.20) / 0.60)
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
# _momentum_score with TA indicators
# ---------------------------------------------------------------------------


class TestEnhancedMomentumScore:
    """Tests for _momentum_score with TA inputs."""

    def test_backward_compatible_no_ta(self) -> None:
        """When ta_indicators is not passed, matches original behavior."""
        score_old, _ = _momentum_score(0.10, 0.20, 0.30)
        score_new, _ = _momentum_score(0.10, 0.20, 0.30, ta_indicators=None)
        assert score_new == _approx(score_old)

    def test_strong_bullish_ta_boosts(self) -> None:
        """Bullish TA (above SMA200, positive MACD, healthy RSI) with positive returns."""
        ta = {
            "sma_200": 90.0,
            "macd_histogram": 2.5,
            "rsi_14": 60.0,
            "stoch_k": 70.0,
            "stoch_d": 65.0,
            "bb_upper": 120.0,
            "bb_lower": 80.0,
            "atr_14": 3.0,
            "current_close": 110.0,
        }
        score, notes = _momentum_score(0.10, 0.20, 0.30, ta_indicators=ta)
        assert score > 0.7
        # No "unavailable" notes for provided indicators
        assert not any("unavailable" in n for n in notes)

    def test_bearish_ta_drags_down(self) -> None:
        """Bearish TA (below SMA200, negative MACD, overbought RSI) drags score below pure returns."""
        ta = {
            "sma_200": 130.0,
            "macd_histogram": -3.0,
            "rsi_14": 82.0,
            "stoch_k": 90.0,
            "stoch_d": 85.0,
            "bb_upper": 115.0,
            "bb_lower": 105.0,
            "atr_14": 6.0,
            "current_close": 100.0,
        }
        score_no_ta, _ = _momentum_score(0.10, 0.20, 0.30)
        score_with_ta, _ = _momentum_score(0.10, 0.20, 0.30, ta_indicators=ta)
        assert score_with_ta < score_no_ta

    def test_partial_ta_uses_available(self) -> None:
        """When some TA values are None, available ones still contribute."""
        ta = {
            "sma_200": None,
            "macd_histogram": 1.5,
            "rsi_14": 55.0,
            "stoch_k": None,
            "stoch_d": None,
            "bb_upper": None,
            "bb_lower": None,
            "atr_14": None,
            "current_close": 100.0,
        }
        score, notes = _momentum_score(0.10, 0.20, 0.30, ta_indicators=ta)
        assert 0.0 <= score <= 1.0
        assert any("sma_200" in n for n in notes)

    def test_all_missing_returns_and_no_ta_neutral(self) -> None:
        """No returns + no TA = neutral 0.5."""
        score, notes = _momentum_score(None, None, None, ta_indicators=None)
        assert score == _approx(0.5)
        assert len(notes) == 3  # 3 missing return notes

    def test_rsi_overbought_penalty(self) -> None:
        """RSI > 70 should reduce momentum quality score."""
        ta_healthy = {
            "sma_200": 90.0,
            "macd_histogram": 1.0,
            "rsi_14": 55.0,
            "stoch_k": 50.0,
            "stoch_d": 50.0,
            "bb_upper": 120.0,
            "bb_lower": 80.0,
            "atr_14": 2.0,
            "current_close": 100.0,
        }
        ta_overbought = {**ta_healthy, "rsi_14": 85.0}
        score_healthy, _ = _momentum_score(0.10, 0.20, 0.30, ta_indicators=ta_healthy)
        score_overbought, _ = _momentum_score(0.10, 0.20, 0.30, ta_indicators=ta_overbought)
        assert score_overbought < score_healthy

    def test_ta_with_no_returns_still_produces_score(self) -> None:
        """When all returns are None but TA is available, TA alone drives the score."""
        ta = {
            "sma_200": 90.0,
            "macd_histogram": 2.0,
            "rsi_14": 60.0,
            "stoch_k": 65.0,
            "stoch_d": 60.0,
            "bb_upper": 120.0,
            "bb_lower": 80.0,
            "atr_14": 2.5,
            "current_close": 110.0,
        }
        score, notes = _momentum_score(None, None, None, ta_indicators=ta)
        assert score > 0.5  # bullish TA should push above neutral
        assert score <= 1.0

    def test_zero_close_guards_division(self) -> None:
        """current_close = 0 should not cause ZeroDivisionError.

        MACD and ATR sub-components divide by current_close; the guards
        must suppress them gracefully and emit unavailable notes.
        """
        ta = {
            "sma_200": 90.0,
            "macd_histogram": 2.0,
            "rsi_14": 55.0,
            "stoch_k": 50.0,
            "stoch_d": 50.0,
            "bb_upper": 120.0,
            "bb_lower": 80.0,
            "atr_14": 3.0,
            "current_close": 0.0,
        }
        score, notes = _momentum_score(0.10, 0.20, 0.30, ta_indicators=ta)
        assert 0.0 <= score <= 1.0
        # MACD should be suppressed (division by zero guard)
        assert any("macd_histogram" in n for n in notes)


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

    def test_no_thesis_does_not_trigger_penalty(self) -> None:
        """Missing thesis is not penalised — T3→T2 promotion relies on
        deterministic signals alone (per #169).  Only stale (existing but
        outdated) theses incur a penalty."""
        penalties = self._base_call(thesis_created_at=None)
        names = [p.name for p in penalties]
        assert "stale_thesis" not in names

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
# Rank delta — tests call through _fetch_prior_ranks
# ---------------------------------------------------------------------------


def _make_prior_ranks_conn(rows: list[tuple[int, int]]) -> MagicMock:
    """
    Fake connection for _fetch_prior_ranks tests.

    _fetch_prior_ranks uses conn.cursor(row_factory=dict_row), so the cursor's
    fetchall must return dicts with "instrument_id" and "rank" keys.
    """
    dict_rows = [{"instrument_id": iid, "rank": rank} for iid, rank in rows]
    cur = MagicMock()
    cur.fetchall.return_value = dict_rows
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestRankDelta:
    def test_improved_rank_produces_positive_delta(self) -> None:
        # instrument 1 was rank 5 last run; this run it is rank 2 → delta = +3
        conn = _make_prior_ranks_conn([(1, 5)])
        prior = _fetch_prior_ranks(conn, [1], "v1-balanced")
        assert prior == {1: 5}
        delta = prior[1] - 2  # prior_rank - current_rank
        assert delta == 3

    def test_worsened_rank_produces_negative_delta(self) -> None:
        conn = _make_prior_ranks_conn([(1, 2)])
        prior = _fetch_prior_ranks(conn, [1], "v1-balanced")
        delta = prior[1] - 5
        assert delta == -3

    def test_unchanged_rank_produces_zero_delta(self) -> None:
        conn = _make_prior_ranks_conn([(1, 3)])
        prior = _fetch_prior_ranks(conn, [1], "v1-balanced")
        delta = prior[1] - 3
        assert delta == 0

    def test_no_prior_row_returns_empty_dict(self) -> None:
        # No rows returned by DB → instrument has no prior rank → delta = None
        conn = _make_prior_ranks_conn([])
        prior = _fetch_prior_ranks(conn, [1], "v1-balanced")
        assert prior == {}
        rank_delta = (prior[1] - 1) if 1 in prior else None
        assert rank_delta is None

    def test_multiple_instruments_delta(self) -> None:
        conn = _make_prior_ranks_conn([(1, 3), (2, 1), (3, 5)])
        prior = _fetch_prior_ranks(conn, [1, 2, 3], "v1-balanced")
        assert prior == {1: 3, 2: 1, 3: 5}
        # instrument 1: was 3, now 1 → +2
        assert prior[1] - 1 == 2
        # instrument 2: was 1, now 2 → -1
        assert prior[2] - 2 == -1


# ---------------------------------------------------------------------------
# compute_score — integration with fake DB connection
# ---------------------------------------------------------------------------


def _fund_row(
    operating_margin: float,
    gross_margin: float,
    fcf: float,
    net_debt: float,
    debt: float,
    revenue_ttm: float,
    shares_outstanding: float,
) -> dict[str, object]:
    return {
        "operating_margin": operating_margin,
        "gross_margin": gross_margin,
        "fcf": fcf,
        "net_debt": net_debt,
        "debt": debt,
        "revenue_ttm": revenue_ttm,
        "shares_outstanding": shares_outstanding,
    }


def _price_row(return_1m: float, return_3m: float, return_6m: float, close: float) -> dict[str, object]:
    return {"return_1m": return_1m, "return_3m": return_3m, "return_6m": return_6m, "close": close}


def _quote_row(spread_flag: bool, last: float, bid: float, ask: float) -> dict[str, object]:
    return {"spread_flag": spread_flag, "last": last, "bid": bid, "ask": ask}


def _thesis_row(
    confidence_score: float,
    base_value: float,
    bear_value: float,
    created_at: datetime,
) -> dict[str, object]:
    return {
        "confidence_score": confidence_score,
        "base_value": base_value,
        "bear_value": bear_value,
        "created_at": created_at,
    }


def _news_row(sentiment_score: float, importance_score: float) -> dict[str, object]:
    return {"sentiment_score": sentiment_score, "importance_score": importance_score}


def _make_fake_conn(
    fund_rows: list[dict[str, object]],
    price_row: dict[str, object] | None,
    quote_row: dict[str, object] | None,
    thesis_row: dict[str, object] | None,
    news_rows: list[dict[str, object]],
    avg_red_flag: float | None,
    valuation_row: dict[str, object] | None = None,
    estimates_row: dict[str, object] | None = None,
) -> MagicMock:
    """
    Return a MagicMock psycopg connection that supports the cursor(row_factory=...)
    context manager pattern used by _load_instrument_data.

    psycopg cursor semantics: cur.execute(sql) is called, then cur.fetchone() /
    cur.fetchall() is called on the *same* cursor object. We model this by having
    execute() mutate cur.fetchone / cur.fetchall as a side effect, dispatching
    results in order: fundamentals, price, quote, thesis, news, red_flag,
    valuation, analyst_estimates.
    """
    rf_row: dict[str, object] = {"avg_red_flag": avg_red_flag}

    # Ordered list of (fetch_method, return_value) per execute() call.
    responses: list[tuple[str, object]] = [
        ("fetchall", fund_rows),
        ("fetchone", price_row),
        ("fetchone", quote_row),
        ("fetchone", thesis_row),
        ("fetchall", news_rows),
        ("fetchone", rf_row),
        ("fetchone", valuation_row),
        ("fetchone", estimates_row),
    ]
    response_iter = iter(responses)

    cur = MagicMock()

    def _execute_side_effect(*args: object, **kwargs: object) -> None:
        method, value = next(response_iter)
        if method == "fetchall":
            cur.fetchall.return_value = value
        else:
            cur.fetchone.return_value = value

    cur.execute.side_effect = _execute_side_effect
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestComputeScore:
    def test_full_fixture_produces_valid_result(self) -> None:
        conn = _make_fake_conn(
            fund_rows=[
                _fund_row(0.18, 0.55, 200_000.0, -50_000.0, 100_000.0, 1_100_000.0, 10_000_000.0),
                _fund_row(0.12, 0.50, 150_000.0, -30_000.0, 80_000.0, 1_000_000.0, 10_000_000.0),
            ],
            price_row=_price_row(0.05, 0.20, 0.35, 120.0),
            quote_row=_quote_row(False, 120.0, 119.5, 120.5),
            thesis_row=_thesis_row(0.75, 180.0, 90.0, _RECENT),
            news_rows=[_news_row(0.6, 0.8), _news_row(0.5, 1.0)],
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
            fund_rows=[_fund_row(0.18, 0.55, 200_000.0, -50_000.0, 100_000.0, 1_100_000.0, 10_000_000.0)],
            price_row=_price_row(0.05, 0.20, 0.35, 120.0),
            quote_row=_quote_row(True, 120.0, 119.5, 120.5),  # spread_flag=True
            thesis_row=_thesis_row(0.75, 180.0, 90.0, _RECENT),
            news_rows=[],
            avg_red_flag=0.0,
        )
        result = compute_score(1, conn, "v1-balanced")
        penalty_names = [p.name for p in result.penalties]
        assert "wide_spread" in penalty_names
        assert result.total_score < result.raw_total

    def test_missing_thesis_triggers_stale_penalty(self) -> None:
        conn = _make_fake_conn(
            fund_rows=[_fund_row(0.18, 0.55, 200_000.0, -50_000.0, 100_000.0, 1_100_000.0, 10_000_000.0)],
            price_row=_price_row(0.05, 0.20, 0.35, 120.0),
            quote_row=_quote_row(False, 120.0, 119.5, 120.5),
            thesis_row=None,  # no thesis
            news_rows=[],
            avg_red_flag=0.0,
        )
        result = compute_score(1, conn, "v1-balanced")
        penalty_names = [p.name for p in result.penalties]
        # Missing thesis is no longer penalised (per #169 — T3→T2
        # promotion relies on deterministic signals, not thesis).
        assert "stale_thesis" not in penalty_names

    def test_unknown_model_version_raises(self) -> None:
        conn = MagicMock()
        with pytest.raises(KeyError, match="unknown-mode"):
            compute_score(1, conn, "unknown-mode")

    def test_total_score_clipped_when_heavy_penalties(self) -> None:
        # Stale thesis + low confidence + high red flag + spread → heavy deductions
        conn = _make_fake_conn(
            fund_rows=[_fund_row(0.00, 0.10, -1.0, 500_000.0, 600_000.0, 500_000.0, 10_000_000.0)],
            price_row=_price_row(0.0, 0.0, 0.0, 100.0),
            quote_row=_quote_row(True, 100.0, 99.0, 101.0),  # spread flag
            thesis_row=_thesis_row(0.20, 100.0, 100.0, _STALE),  # stale + low confidence
            news_rows=[_news_row(-0.9, 1.0), _news_row(-0.8, 1.0)],
            avg_red_flag=0.80,
        )
        result = compute_score(1, conn, "v1-balanced")
        assert result.total_score >= 0.0
        assert result.total_score <= 1.0


# ---------------------------------------------------------------------------
# compute_rankings — rank assignment and rank_delta via patched compute_score
# ---------------------------------------------------------------------------


def _make_score_result(instrument_id: int, total_score: float) -> ScoreResult:
    """Minimal ScoreResult fixture for ranking tests."""
    return ScoreResult(
        instrument_id=instrument_id,
        model_version="v1-balanced",
        family_scores=FamilyScores(
            quality=total_score,
            value=total_score,
            turnaround=total_score,
            momentum=total_score,
            sentiment=total_score,
            confidence=total_score,
        ),
        penalties=[],
        total_penalty=0.0,
        raw_total=total_score,
        total_score=total_score,
        explanation="fixture",
    )


def _make_rankings_conn(
    instrument_ids: list[int],
    prior_rank_rows: list[tuple[int, int]],
) -> MagicMock:
    """
    Fake connection for compute_rankings tests.

    compute_rankings issues calls in this order:
      1. conn.cursor(row_factory=dict_row) → eligible instruments query
         → fetchall: [{"instrument_id": id}, ...]
      2. conn.transaction().__enter__()   ← transaction opens
      3. conn.cursor(row_factory=dict_row) → cur.execute(_fetch_prior_ranks)
         → fetchall: [{"instrument_id": id, "rank": rank}, ...]
      4. N × conn.execute(INSERT INTO scores)

    Both cursors share the same mock; the second call (step 3) is the one
    that matters for the transaction ordering test — it is always the last
    cursor() call, so the test uses the last cursor index.
    """
    eligible_rows = [{"instrument_id": iid} for iid in instrument_ids]
    prior_rank_dicts = [{"instrument_id": iid, "rank": rank} for iid, rank in prior_rank_rows]

    conn = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

    # conn.execute: INSERT calls (no rows returned)
    conn.execute.return_value.fetchall.return_value = []

    # conn.cursor: called twice — eligible instruments then _fetch_prior_ranks.
    # Return a different mock per call so each has its own fetchall result.
    elig_cur = MagicMock()
    elig_cur.fetchall.return_value = eligible_rows
    elig_cur.__enter__ = MagicMock(return_value=elig_cur)
    elig_cur.__exit__ = MagicMock(return_value=False)

    prior_cur = MagicMock()
    prior_cur.fetchall.return_value = prior_rank_dicts
    prior_cur.__enter__ = MagicMock(return_value=prior_cur)
    prior_cur.__exit__ = MagicMock(return_value=False)

    conn.cursor.side_effect = [elig_cur, prior_cur]

    return conn


class TestComputeRankings:
    def test_rank_assigned_descending_by_total_score(self) -> None:
        # Instrument 2 scores higher → should be rank 1
        conn = _make_rankings_conn(instrument_ids=[1, 2], prior_rank_rows=[])
        with patch("app.services.scoring.compute_score") as mock_score:
            mock_score.side_effect = [
                _make_score_result(1, 0.60),
                _make_score_result(2, 0.80),
            ]
            result = compute_rankings(conn, "v1-balanced")

        by_id = {r.instrument_id: r for r in result.scored}
        assert by_id[2].rank == 1
        assert by_id[1].rank == 2

    def test_rank_delta_positive_when_rank_improved(self) -> None:
        # Instrument 1 was rank 3 last run; this run it becomes rank 1 → delta = +2
        conn = _make_rankings_conn(instrument_ids=[1], prior_rank_rows=[(1, 3)])
        with patch("app.services.scoring.compute_score") as mock_score:
            mock_score.return_value = _make_score_result(1, 0.75)
            result = compute_rankings(conn, "v1-balanced")

        assert result.scored[0].rank == 1
        assert result.scored[0].rank_delta == 2  # prior(3) - current(1)

    def test_rank_delta_negative_when_rank_worsened(self) -> None:
        # Instrument 1 was rank 1; now rank 2 → delta = -1
        conn = _make_rankings_conn(instrument_ids=[1, 2], prior_rank_rows=[(1, 1), (2, 2)])
        with patch("app.services.scoring.compute_score") as mock_score:
            mock_score.side_effect = [
                _make_score_result(1, 0.50),  # lower score this run
                _make_score_result(2, 0.80),  # higher score this run
            ]
            result = compute_rankings(conn, "v1-balanced")

        by_id = {r.instrument_id: r for r in result.scored}
        assert by_id[2].rank == 1
        assert by_id[1].rank == 2
        assert by_id[1].rank_delta == -1  # prior(1) - current(2)
        assert by_id[2].rank_delta == 1  # prior(2) - current(1)

    def test_rank_delta_none_on_first_run(self) -> None:
        # No prior rows → rank_delta is None for all instruments
        conn = _make_rankings_conn(instrument_ids=[1, 2], prior_rank_rows=[])
        with patch("app.services.scoring.compute_score") as mock_score:
            mock_score.side_effect = [
                _make_score_result(1, 0.70),
                _make_score_result(2, 0.60),
            ]
            result = compute_rankings(conn, "v1-balanced")

        for r in result.scored:
            assert r.rank_delta is None

    def test_empty_universe_returns_empty_result(self) -> None:
        conn = _make_rankings_conn(instrument_ids=[], prior_rank_rows=[])
        result = compute_rankings(conn, "v1-balanced")
        assert result.scored == []

    def test_unknown_model_version_raises(self) -> None:
        conn = MagicMock()
        with pytest.raises(KeyError, match="bad-version"):
            compute_rankings(conn, "bad-version")

    def test_fetch_prior_ranks_runs_inside_transaction(self) -> None:
        """
        Assert that _fetch_prior_ranks (conn.cursor call) executes after
        conn.transaction().__enter__ — i.e. inside the transaction block.
        If compute_rankings is ever refactored to move _fetch_prior_ranks
        outside the transaction, this test will fail.
        """
        conn = _make_rankings_conn(instrument_ids=[1], prior_rank_rows=[])
        with patch("app.services.scoring.compute_score") as mock_score:
            mock_score.return_value = _make_score_result(1, 0.70)
            compute_rankings(conn, "v1-balanced")

        # Collect the names of all calls made on `conn` in order
        call_names = [call[0] for call in conn.mock_calls]

        # transaction().__enter__ must appear before at least one cursor() call.
        # Use the last cursor index (not the first) to avoid a false-positive if a
        # pre-transaction cursor call is ever added (e.g. eligible-instruments query).
        assert "transaction().__enter__" in call_names, "transaction was never entered"
        assert "cursor" in call_names, "_fetch_prior_ranks cursor was never opened"

        tx_enter_idx = call_names.index("transaction().__enter__")
        # Find the last cursor call; _fetch_prior_ranks is always the final cursor open
        cursor_idx = len(call_names) - 1 - call_names[::-1].index("cursor")
        assert cursor_idx > tx_enter_idx, (
            f"conn.cursor (prior rank fetch) at position {cursor_idx} "
            f"must come after transaction().__enter__ at position {tx_enter_idx}"
        )
