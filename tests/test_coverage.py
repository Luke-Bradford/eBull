"""
Unit tests for the coverage tier management service.

No network calls, no database. All DB access is mocked.

Coverage:
  - _is_thesis_fresh: fresh, stale, no thesis, unknown frequency
  - _has_tier1_required_data: all present, each field missing
  - _evaluate_promotion: T3→T2, T2→T1, various failure conditions
  - _evaluate_demotion: T1→T2, T2→T3, various triggers, hysteresis bands
  - _enforce_tier1_cap: under cap, at cap, over cap, tiebreaking
  - review_coverage: end-to-end wiring, demotion-before-promotion ordering, cap interaction
  - override_tier: happy path, validation errors, cap enforcement
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from app.services.coverage import (
    DEMOTE_T1_TO_T2_SCORE,
    DEMOTE_T2_TO_T3_SCORE,
    PROMOTE_T2_TO_T1_CONFIDENCE,
    PROMOTE_T2_TO_T1_SCORE,
    PROMOTE_T3_TO_T2_SCORE,
    TIER_1_CAP,
    InstrumentSnapshot,
    ReviewResult,
    TierChange,
    _enforce_tier1_cap,
    _evaluate_demotion,
    _evaluate_promotion,
    _has_tier1_required_data,
    _is_thesis_fresh,
    override_tier,
    review_coverage,
    seed_coverage,
)

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Snapshot factory
# ---------------------------------------------------------------------------


def _snap(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    is_tradable: bool = True,
    current_tier: int = 2,
    review_frequency: str | None = "weekly",
    total_score: float | None = 0.80,
    thesis_stance: str | None = "buy",
    thesis_confidence: float | None = 0.75,
    thesis_created_at: datetime | None = None,
    has_fundamentals: bool = True,
    has_quote: bool = True,
    spread_flag: bool | None = False,
) -> InstrumentSnapshot:
    if thesis_created_at is None:
        thesis_created_at = _NOW - timedelta(days=1)  # fresh by default
    return InstrumentSnapshot(
        instrument_id=instrument_id,
        symbol=symbol,
        is_tradable=is_tradable,
        current_tier=current_tier,
        review_frequency=review_frequency,
        total_score=total_score,
        thesis_stance=thesis_stance,
        thesis_confidence=thesis_confidence,
        thesis_created_at=thesis_created_at,
        has_fundamentals=has_fundamentals,
        has_quote=has_quote,
        spread_flag=spread_flag,
    )


# ---------------------------------------------------------------------------
# _is_thesis_fresh
# ---------------------------------------------------------------------------


class TestIsThesisFresh:
    def test_fresh_weekly(self) -> None:
        created = _NOW - timedelta(days=3)
        assert _is_thesis_fresh(created, "weekly", _NOW) is True

    def test_stale_weekly(self) -> None:
        created = _NOW - timedelta(days=8)
        assert _is_thesis_fresh(created, "weekly", _NOW) is False

    def test_exactly_at_boundary_is_stale(self) -> None:
        created = _NOW - timedelta(days=7)
        assert _is_thesis_fresh(created, "weekly", _NOW) is False

    def test_fresh_daily(self) -> None:
        created = _NOW - timedelta(hours=12)
        assert _is_thesis_fresh(created, "daily", _NOW) is True

    def test_fresh_monthly(self) -> None:
        created = _NOW - timedelta(days=20)
        assert _is_thesis_fresh(created, "monthly", _NOW) is True

    def test_no_thesis_is_not_fresh(self) -> None:
        assert _is_thesis_fresh(None, "weekly", _NOW) is False

    def test_unknown_frequency_is_not_fresh(self) -> None:
        created = _NOW - timedelta(days=1)
        assert _is_thesis_fresh(created, "biannual", _NOW) is False

    def test_none_frequency_is_not_fresh(self) -> None:
        created = _NOW - timedelta(days=1)
        assert _is_thesis_fresh(created, None, _NOW) is False


# ---------------------------------------------------------------------------
# _has_tier1_required_data
# ---------------------------------------------------------------------------


class TestHasTier1RequiredData:
    def test_all_present(self) -> None:
        snap = _snap()
        ok, missing = _has_tier1_required_data(snap)
        assert ok is True
        assert missing == []

    def test_missing_thesis(self) -> None:
        snap = InstrumentSnapshot(
            instrument_id=1,
            symbol="AAPL",
            is_tradable=True,
            current_tier=2,
            review_frequency="weekly",
            total_score=0.80,
            thesis_stance=None,
            thesis_confidence=None,
            thesis_created_at=None,
            has_fundamentals=True,
            has_quote=True,
            spread_flag=False,
        )
        ok, missing = _has_tier1_required_data(snap)
        assert ok is False
        assert "thesis" in missing

    def test_missing_score(self) -> None:
        snap = _snap(total_score=None)
        ok, missing = _has_tier1_required_data(snap)
        assert ok is False
        assert "score" in missing

    def test_missing_fundamentals(self) -> None:
        snap = _snap(has_fundamentals=False)
        ok, missing = _has_tier1_required_data(snap)
        assert ok is False
        assert "fundamentals" in missing

    def test_not_tradable(self) -> None:
        snap = _snap(is_tradable=False)
        ok, missing = _has_tier1_required_data(snap)
        assert ok is False
        assert "tradable" in missing

    def test_missing_quote(self) -> None:
        snap = _snap(has_quote=False)
        ok, missing = _has_tier1_required_data(snap)
        assert ok is False
        assert "quote" in missing

    def test_multiple_missing(self) -> None:
        snap = _snap(total_score=None, has_fundamentals=False, has_quote=False)
        ok, missing = _has_tier1_required_data(snap)
        assert ok is False
        assert len(missing) == 3


# ---------------------------------------------------------------------------
# Promotion: T3 → T2
# ---------------------------------------------------------------------------


class TestPromotionT3ToT2:
    def test_qualifies(self) -> None:
        snap = _snap(current_tier=3, total_score=0.60)
        result = _evaluate_promotion(snap, _NOW)
        assert result is not None
        assert result.old_tier == 3
        assert result.new_tier == 2
        assert result.change_type == "promotion"

    def test_score_below_threshold_blocks(self) -> None:
        snap = _snap(current_tier=3, total_score=0.50)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_exactly_at_threshold_qualifies(self) -> None:
        snap = _snap(current_tier=3, total_score=PROMOTE_T3_TO_T2_SCORE)
        assert _evaluate_promotion(snap, _NOW) is not None

    def test_no_thesis_does_not_block(self) -> None:
        """T3→T2 does not require a thesis — deterministic signals suffice."""
        snap = InstrumentSnapshot(
            instrument_id=1,
            symbol="X",
            is_tradable=True,
            current_tier=3,
            review_frequency="weekly",
            total_score=0.60,
            thesis_stance=None,
            thesis_confidence=None,
            thesis_created_at=None,
            has_fundamentals=True,
            has_quote=True,
            spread_flag=False,
        )
        result = _evaluate_promotion(snap, _NOW)
        assert result is not None
        assert result.new_tier == 2

    def test_not_tradable_blocks(self) -> None:
        snap = _snap(current_tier=3, total_score=0.60, is_tradable=False)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_no_score_blocks(self) -> None:
        snap = _snap(current_tier=3, total_score=None)
        assert _evaluate_promotion(snap, _NOW) is None


# ---------------------------------------------------------------------------
# Promotion: T2 �� T1
# ---------------------------------------------------------------------------


class TestPromotionT2ToT1:
    def test_qualifies_all_conditions_met(self) -> None:
        snap = _snap(
            current_tier=2,
            total_score=0.75,
            thesis_stance="buy",
            thesis_confidence=0.70,
            thesis_created_at=_NOW - timedelta(days=2),
            review_frequency="weekly",
        )
        result = _evaluate_promotion(snap, _NOW)
        assert result is not None
        assert result.old_tier == 2
        assert result.new_tier == 1

    def test_score_below_threshold_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.65)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_exactly_at_score_threshold(self) -> None:
        snap = _snap(current_tier=2, total_score=PROMOTE_T2_TO_T1_SCORE)
        assert _evaluate_promotion(snap, _NOW) is not None

    def test_stance_hold_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, thesis_stance="hold")
        assert _evaluate_promotion(snap, _NOW) is None

    def test_stance_avoid_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, thesis_stance="avoid")
        assert _evaluate_promotion(snap, _NOW) is None

    def test_stance_watch_qualifies(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, thesis_stance="watch")
        assert _evaluate_promotion(snap, _NOW) is not None

    def test_low_confidence_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, thesis_confidence=0.50)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_exactly_at_confidence_threshold(self) -> None:
        snap = _snap(
            current_tier=2,
            total_score=0.75,
            thesis_confidence=PROMOTE_T2_TO_T1_CONFIDENCE,
        )
        assert _evaluate_promotion(snap, _NOW) is not None

    def test_stale_thesis_blocks(self) -> None:
        snap = _snap(
            current_tier=2,
            total_score=0.75,
            thesis_created_at=_NOW - timedelta(days=10),
            review_frequency="weekly",
        )
        assert _evaluate_promotion(snap, _NOW) is None

    def test_no_quote_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, has_quote=False)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_wide_spread_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, spread_flag=True)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_null_spread_flag_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, spread_flag=None)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_missing_fundamentals_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, has_fundamentals=False)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_not_tradable_blocks(self) -> None:
        snap = _snap(current_tier=2, total_score=0.75, is_tradable=False)
        assert _evaluate_promotion(snap, _NOW) is None

    def test_no_thesis_blocks(self) -> None:
        snap = InstrumentSnapshot(
            instrument_id=1,
            symbol="X",
            is_tradable=True,
            current_tier=2,
            review_frequency="weekly",
            total_score=0.80,
            thesis_stance=None,
            thesis_confidence=None,
            thesis_created_at=None,
            has_fundamentals=True,
            has_quote=True,
            spread_flag=False,
        )
        assert _evaluate_promotion(snap, _NOW) is None


# ---------------------------------------------------------------------------
# Promotion: T1 instruments cannot be promoted further
# ---------------------------------------------------------------------------


class TestPromotionT1NoOp:
    def test_tier1_returns_none(self) -> None:
        snap = _snap(current_tier=1, total_score=0.95)
        assert _evaluate_promotion(snap, _NOW) is None


# ---------------------------------------------------------------------------
# Demotion: T1 → T2
# ---------------------------------------------------------------------------


class TestDemotionT1ToT2:
    def test_low_score_triggers(self) -> None:
        snap = _snap(current_tier=1, total_score=0.55)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert result.old_tier == 1
        assert result.new_tier == 2
        assert "score=" in result.rationale

    def test_score_in_hysteresis_band_no_demotion(self) -> None:
        """Score between demotion (0.60) and promotion (0.70) thresholds — no demotion."""
        snap = _snap(current_tier=1, total_score=0.65)
        assert _evaluate_demotion(snap, _NOW) is None

    def test_exactly_at_demotion_threshold_no_demotion(self) -> None:
        """Score == 0.60 does not trigger demotion (threshold is <0.60)."""
        snap = _snap(current_tier=1, total_score=DEMOTE_T1_TO_T2_SCORE)
        assert _evaluate_demotion(snap, _NOW) is None

    def test_stale_thesis_triggers(self) -> None:
        snap = _snap(
            current_tier=1,
            total_score=0.75,
            thesis_created_at=_NOW - timedelta(days=10),
            review_frequency="weekly",
        )
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "thesis stale" in result.rationale

    def test_no_thesis_triggers(self) -> None:
        snap = InstrumentSnapshot(
            instrument_id=1,
            symbol="X",
            is_tradable=True,
            current_tier=1,
            review_frequency="weekly",
            total_score=0.75,
            thesis_stance=None,
            thesis_confidence=None,
            thesis_created_at=None,
            has_fundamentals=True,
            has_quote=True,
            spread_flag=False,
        )
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "no thesis" in result.rationale

    def test_stance_avoid_triggers(self) -> None:
        snap = _snap(current_tier=1, total_score=0.75, thesis_stance="avoid")
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "stance=avoid" in result.rationale

    def test_wide_spread_triggers(self) -> None:
        snap = _snap(current_tier=1, total_score=0.75, spread_flag=True)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "liquidity fails" in result.rationale

    def test_no_quote_triggers(self) -> None:
        snap = _snap(current_tier=1, total_score=0.75, has_quote=False)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "no quote" in result.rationale

    def test_missing_critical_data_triggers(self) -> None:
        snap = _snap(current_tier=1, total_score=0.75, has_fundamentals=False)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "critical data missing" in result.rationale

    def test_all_good_no_demotion(self) -> None:
        snap = _snap(current_tier=1, total_score=0.80)
        assert _evaluate_demotion(snap, _NOW) is None

    def test_multiple_triggers_all_listed(self) -> None:
        snap = _snap(
            current_tier=1,
            total_score=0.50,
            thesis_stance="avoid",
            spread_flag=True,
        )
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "score=" in result.rationale
        assert "stance=avoid" in result.rationale
        assert "liquidity fails" in result.rationale

    def test_none_score_demotes_via_missing_data(self) -> None:
        """A T1 instrument with total_score=None is demoted because score is required data."""
        snap = _snap(current_tier=1, total_score=None)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert result.new_tier == 2
        assert "critical data missing" in result.rationale
        assert "score" in result.rationale


# ---------------------------------------------------------------------------
# Demotion: T2 → T3
# ---------------------------------------------------------------------------


class TestDemotionT2ToT3:
    def test_low_score_triggers(self) -> None:
        snap = _snap(current_tier=2, total_score=0.40)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert result.new_tier == 3

    def test_score_in_hysteresis_band_no_demotion(self) -> None:
        """Score between demotion (0.45) and promotion (0.55) thresholds."""
        snap = _snap(current_tier=2, total_score=0.50)
        assert _evaluate_demotion(snap, _NOW) is None

    def test_exactly_at_demotion_threshold_no_demotion(self) -> None:
        snap = _snap(current_tier=2, total_score=DEMOTE_T2_TO_T3_SCORE)
        assert _evaluate_demotion(snap, _NOW) is None

    def test_no_thesis_triggers(self) -> None:
        snap = InstrumentSnapshot(
            instrument_id=1,
            symbol="X",
            is_tradable=True,
            current_tier=2,
            review_frequency="weekly",
            total_score=0.60,
            thesis_stance=None,
            thesis_confidence=None,
            thesis_created_at=None,
            has_fundamentals=True,
            has_quote=True,
            spread_flag=False,
        )
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "no thesis" in result.rationale

    def test_not_tradable_triggers(self) -> None:
        snap = _snap(current_tier=2, is_tradable=False)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "not tradable" in result.rationale

    def test_missing_fundamentals_and_quote_triggers(self) -> None:
        snap = _snap(current_tier=2, has_fundamentals=False, has_quote=False)
        result = _evaluate_demotion(snap, _NOW)
        assert result is not None
        assert "missing fundamentals and quote" in result.rationale

    def test_missing_only_fundamentals_no_demotion(self) -> None:
        """Missing just fundamentals is not severe enough for T2→T3."""
        snap = _snap(current_tier=2, total_score=0.60, has_fundamentals=False)
        assert _evaluate_demotion(snap, _NOW) is None

    def test_missing_only_quote_no_demotion(self) -> None:
        """Missing just quote is not severe enough for T2→T3."""
        snap = _snap(current_tier=2, total_score=0.60, has_quote=False)
        assert _evaluate_demotion(snap, _NOW) is None


# ---------------------------------------------------------------------------
# Demotion: T3 instruments cannot be demoted further
# ---------------------------------------------------------------------------


class TestDemotionT3NoOp:
    def test_tier3_returns_none(self) -> None:
        snap = _snap(current_tier=3, total_score=0.10)
        assert _evaluate_demotion(snap, _NOW) is None


# ---------------------------------------------------------------------------
# Hysteresis integration
# ---------------------------------------------------------------------------


class TestHysteresis:
    def test_t2_score_between_demote_and_promote_is_stable(self) -> None:
        """Score 0.50: above T2→T3 demote (0.45), below T3→T2 promote (0.55)."""
        snap = _snap(current_tier=2, total_score=0.50)
        assert _evaluate_demotion(snap, _NOW) is None
        assert _evaluate_promotion(snap, _NOW) is None

    def test_t1_score_between_demote_and_promote_is_stable(self) -> None:
        """Score 0.65: above T1→T2 demote (0.60), below T2→T1 promote (0.70)."""
        snap = _snap(current_tier=1, total_score=0.65)
        assert _evaluate_demotion(snap, _NOW) is None
        # T1 can't be promoted further, so this is moot but proves stability
        assert _evaluate_promotion(snap, _NOW) is None


# ---------------------------------------------------------------------------
# Tier 1 cap enforcement
# ---------------------------------------------------------------------------


class TestEnforceTier1Cap:
    def _make_change(self, iid: int, score: float, conf: float) -> TierChange:
        return TierChange(
            instrument_id=iid,
            old_tier=2,
            new_tier=1,
            change_type="promotion",
            rationale="test",
            evidence={
                "total_score": score,
                "thesis_confidence": conf,
                "thesis_created_at": (_NOW - timedelta(days=1)).isoformat(),
            },
        )

    def test_under_cap_all_approved(self) -> None:
        changes = [self._make_change(1, 0.80, 0.70)]
        approved, blocked = _enforce_tier1_cap(45, changes)
        assert len(approved) == 1
        assert len(blocked) == 0

    def test_at_cap_all_blocked(self) -> None:
        changes = [self._make_change(1, 0.80, 0.70)]
        approved, blocked = _enforce_tier1_cap(TIER_1_CAP, changes)
        assert len(approved) == 0
        assert len(blocked) == 1
        assert blocked[0].change_type == "blocked_promotion"
        assert blocked[0].old_tier == blocked[0].new_tier  # no change

    def test_partial_block_ranks_by_score(self) -> None:
        changes = [
            self._make_change(1, 0.75, 0.70),
            self._make_change(2, 0.90, 0.70),
            self._make_change(3, 0.80, 0.70),
        ]
        # Only 1 slot available
        approved, blocked = _enforce_tier1_cap(TIER_1_CAP - 1, changes)
        assert len(approved) == 1
        assert approved[0].instrument_id == 2  # highest score
        assert len(blocked) == 2

    def test_tiebreak_by_confidence(self) -> None:
        changes = [
            self._make_change(1, 0.80, 0.60),
            self._make_change(2, 0.80, 0.90),
        ]
        approved, blocked = _enforce_tier1_cap(TIER_1_CAP - 1, changes)
        assert approved[0].instrument_id == 2  # higher confidence breaks tie

    def test_zero_slots_all_blocked(self) -> None:
        changes = [
            self._make_change(1, 0.80, 0.70),
            self._make_change(2, 0.90, 0.80),
        ]
        approved, blocked = _enforce_tier1_cap(TIER_1_CAP, changes)
        assert len(approved) == 0
        assert len(blocked) == 2

    def test_blocked_records_have_correct_rationale(self) -> None:
        changes = [self._make_change(1, 0.75, 0.65)]
        _, blocked = _enforce_tier1_cap(TIER_1_CAP, changes)
        assert "cap" in blocked[0].rationale.lower()
        assert "0.75" in blocked[0].rationale

    def test_tiebreak_by_freshness(self) -> None:
        """When score and confidence tie, more recent thesis wins."""
        old_ts = (_NOW - timedelta(days=5)).isoformat()
        new_ts = (_NOW - timedelta(days=1)).isoformat()

        c1 = TierChange(
            instrument_id=1,
            old_tier=2,
            new_tier=1,
            change_type="promotion",
            rationale="test",
            evidence={"total_score": 0.80, "thesis_confidence": 0.70, "thesis_created_at": old_ts},
        )
        c2 = TierChange(
            instrument_id=2,
            old_tier=2,
            new_tier=1,
            change_type="promotion",
            rationale="test",
            evidence={"total_score": 0.80, "thesis_confidence": 0.70, "thesis_created_at": new_ts},
        )
        approved, blocked = _enforce_tier1_cap(TIER_1_CAP - 1, [c1, c2])
        assert len(approved) == 1
        assert approved[0].instrument_id == 2  # more recent thesis wins

    def test_tiebreak_none_freshness_ranked_last(self) -> None:
        """Instruments with no thesis_created_at are ranked below those with one."""
        c1 = TierChange(
            instrument_id=1,
            old_tier=2,
            new_tier=1,
            change_type="promotion",
            rationale="test",
            evidence={"total_score": 0.80, "thesis_confidence": 0.70, "thesis_created_at": None},
        )
        c2 = TierChange(
            instrument_id=2,
            old_tier=2,
            new_tier=1,
            change_type="promotion",
            rationale="test",
            evidence={
                "total_score": 0.80,
                "thesis_confidence": 0.70,
                "thesis_created_at": (_NOW - timedelta(days=1)).isoformat(),
            },
        )
        approved, _ = _enforce_tier1_cap(TIER_1_CAP - 1, [c1, c2])
        assert approved[0].instrument_id == 2


# ---------------------------------------------------------------------------
# review_coverage end-to-end (mocked DB)
# ---------------------------------------------------------------------------


class TestReviewCoverage:
    """
    Tests wire together _load_instruments_for_review, evaluation, cap, and writes.
    The DB is fully mocked — the actual query is tested via integration tests.
    """

    def _mock_conn_for_snapshots(self, snapshots: list[InstrumentSnapshot]) -> MagicMock:
        """
        Build a mock connection that returns the given snapshots from
        _load_instruments_for_review's query and tracks writes.
        """
        conn = MagicMock()
        self._writes: list[tuple[str, dict]] = []

        mock_cursor = MagicMock()
        rows = [
            {
                "instrument_id": s.instrument_id,
                "symbol": s.symbol,
                "is_tradable": s.is_tradable,
                "coverage_tier": s.current_tier,
                "review_frequency": s.review_frequency,
                "total_score": s.total_score,
                "thesis_stance": s.thesis_stance,
                "thesis_confidence": s.thesis_confidence,
                "thesis_created_at": s.thesis_created_at,
                "has_fundamentals": s.has_fundamentals,
                "has_quote": s.has_quote,
                "spread_flag": s.spread_flag,
            }
            for s in snapshots
        ]
        mock_cursor.fetchall.return_value = rows
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = mock_cursor

        def track_execute(sql: str, params: dict | None = None) -> MagicMock:
            if params is not None:
                self._writes.append((sql.strip(), dict(params)))
            return MagicMock()

        conn.execute.side_effect = track_execute
        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_promotion_t3_to_t2(self, _mock_now: MagicMock) -> None:
        snap = _snap(instrument_id=1, current_tier=3, total_score=0.60)
        conn = self._mock_conn_for_snapshots([snap])
        result = review_coverage(conn)
        assert len(result.promotions) == 1
        assert result.promotions[0].new_tier == 2

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_promotion_t3_to_t2_without_thesis(self, _mock_now: MagicMock) -> None:
        """T3→T2 does NOT require a thesis — deterministic signals suffice."""
        snap = InstrumentSnapshot(
            instrument_id=1,
            symbol="AAPL",
            is_tradable=True,
            current_tier=3,
            review_frequency=None,
            total_score=0.60,
            thesis_stance=None,
            thesis_confidence=None,
            thesis_created_at=None,
            has_fundamentals=True,
            has_quote=True,
            spread_flag=False,
        )
        conn = self._mock_conn_for_snapshots([snap])
        result = review_coverage(conn)
        assert len(result.promotions) == 1
        assert result.promotions[0].new_tier == 2
        assert "thesis" not in result.promotions[0].rationale.lower()

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_demotion_t1_to_t2(self, _mock_now: MagicMock) -> None:
        snap = _snap(instrument_id=1, current_tier=1, total_score=0.50)
        conn = self._mock_conn_for_snapshots([snap])
        result = review_coverage(conn)
        assert len(result.demotions) == 1
        assert result.demotions[0].new_tier == 2

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_demotion_frees_slot_for_promotion(self, _mock_now: MagicMock) -> None:
        """
        Demoting an instrument from T1 should free a slot,
        allowing a T2 instrument to be promoted.
        """
        demote_snap = _snap(instrument_id=1, current_tier=1, total_score=0.50)
        promote_snap = _snap(instrument_id=2, current_tier=2, total_score=0.80)
        # Fill remaining T1 slots to cap - 1 (so with instrument 1, we're at cap)
        t1_fillers = [_snap(instrument_id=100 + i, current_tier=1, total_score=0.75) for i in range(TIER_1_CAP - 1)]
        all_snaps = [demote_snap, promote_snap] + t1_fillers
        conn = self._mock_conn_for_snapshots(all_snaps)
        result = review_coverage(conn)
        assert len(result.demotions) == 1
        assert result.demotions[0].instrument_id == 1
        assert len(result.promotions) == 1
        assert result.promotions[0].instrument_id == 2

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_cap_blocks_excess_promotions(self, _mock_now: MagicMock) -> None:
        """When T1 is full and no demotions, promotions are blocked."""
        t1_fillers = [_snap(instrument_id=100 + i, current_tier=1, total_score=0.75) for i in range(TIER_1_CAP)]
        promote_snap = _snap(instrument_id=1, current_tier=2, total_score=0.80)
        all_snaps = t1_fillers + [promote_snap]
        conn = self._mock_conn_for_snapshots(all_snaps)
        result = review_coverage(conn)
        assert len(result.blocked) == 1
        assert result.blocked[0].instrument_id == 1
        assert len(result.promotions) == 0

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_no_instruments_returns_empty(self, _mock_now: MagicMock) -> None:
        conn = self._mock_conn_for_snapshots([])
        result = review_coverage(conn)
        assert result == ReviewResult(promotions=[], demotions=[], blocked=[], unchanged=0)

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_unchanged_count_correct(self, _mock_now: MagicMock) -> None:
        stable = _snap(instrument_id=1, current_tier=1, total_score=0.75)
        conn = self._mock_conn_for_snapshots([stable])
        result = review_coverage(conn)
        assert result.unchanged == 1
        assert result.promotions == []
        assert result.demotions == []

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_blocked_is_own_bucket(self, _mock_now: MagicMock) -> None:
        """promotions + demotions + blocked + unchanged == len(snapshots)."""
        t1_fillers = [_snap(instrument_id=100 + i, current_tier=1, total_score=0.75) for i in range(TIER_1_CAP)]
        promotable = _snap(instrument_id=1, current_tier=2, total_score=0.80)
        all_snaps = t1_fillers + [promotable]
        conn = self._mock_conn_for_snapshots(all_snaps)
        result = review_coverage(conn)
        # 51 total: 50 stable T1 + 1 blocked T2→T1
        assert len(result.blocked) == 1
        assert result.unchanged == 50  # 50 stable T1; blocked is separate bucket
        assert result.promotions == []
        assert result.demotions == []
        assert len(result.promotions) + len(result.demotions) + len(result.blocked) + result.unchanged == len(all_snaps)

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_demoted_instrument_not_also_promoted(self, _mock_now: MagicMock) -> None:
        """An instrument being demoted should not be evaluated for promotion."""
        # This instrument has a low score (T1→T2 demotion trigger) but would
        # also qualify for T2→T3 evaluation if re-evaluated. The service should
        # only demote once and not double-process.
        snap = _snap(instrument_id=1, current_tier=1, total_score=0.50)
        conn = self._mock_conn_for_snapshots([snap])
        result = review_coverage(conn)
        assert len(result.demotions) == 1
        assert len(result.promotions) == 0

    @patch("app.services.coverage._utcnow", return_value=_NOW)
    def test_writes_audit_for_all_changes(self, _mock_now: MagicMock) -> None:
        snap = _snap(instrument_id=1, current_tier=3, total_score=0.60)
        conn = self._mock_conn_for_snapshots([snap])
        review_coverage(conn)
        # Should have both an UPDATE (coverage.coverage_tier) and an INSERT (coverage_audit)
        update_calls = [(sql, params) for sql, params in self._writes if "UPDATE coverage" in sql]
        insert_calls = [(sql, params) for sql, params in self._writes if "INSERT INTO coverage_audit" in sql]
        assert len(update_calls) == 1
        assert len(insert_calls) == 1


# ---------------------------------------------------------------------------
# override_tier
# ---------------------------------------------------------------------------


class TestOverrideTier:
    def _mock_conn(
        self,
        coverage_row: dict[str, object] | None,
        t1_count: int = 10,
    ) -> MagicMock:
        conn = MagicMock()
        self._writes: list[tuple[str, dict]] = []

        call_count = 0

        def cursor_factory(*args: object, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            cur = MagicMock()
            cur.__enter__ = MagicMock(return_value=cur)
            cur.__exit__ = MagicMock(return_value=False)

            if call_count == 1:
                # First cursor: coverage + instrument lookup
                cur.fetchone.return_value = coverage_row
            elif call_count == 2:
                # Second cursor: T1 count check
                cur.fetchone.return_value = {"cnt": t1_count}
            return cur

        conn.cursor.side_effect = cursor_factory

        def track_execute(sql: str, params: dict | None = None) -> MagicMock:
            if params is not None:
                self._writes.append((sql.strip(), dict(params)))
            return MagicMock()

        conn.execute.side_effect = track_execute
        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_promote_t2_to_t1(self) -> None:
        conn = self._mock_conn({"coverage_tier": 2, "symbol": "AAPL"}, t1_count=10)
        change = override_tier(conn, instrument_id=1, new_tier=1, rationale="Strong thesis")
        assert change.old_tier == 2
        assert change.new_tier == 1
        assert change.change_type == "override"
        assert "Strong thesis" in change.rationale

    def test_demote_t1_to_t2(self) -> None:
        conn = self._mock_conn({"coverage_tier": 1, "symbol": "AAPL"})
        change = override_tier(conn, instrument_id=1, new_tier=2, rationale="Liquidity concern")
        assert change.old_tier == 1
        assert change.new_tier == 2

    def test_invalid_tier_raises(self) -> None:
        conn = MagicMock()
        with pytest.raises(ValueError, match="must be 1, 2, or 3"):
            override_tier(conn, instrument_id=1, new_tier=4, rationale="test")

    def test_empty_rationale_raises(self) -> None:
        conn = MagicMock()
        with pytest.raises(ValueError, match="non-empty"):
            override_tier(conn, instrument_id=1, new_tier=1, rationale="")

    def test_whitespace_rationale_raises(self) -> None:
        conn = MagicMock()
        with pytest.raises(ValueError, match="non-empty"):
            override_tier(conn, instrument_id=1, new_tier=1, rationale="   ")

    def test_no_coverage_row_raises(self) -> None:
        conn = self._mock_conn(None)
        with pytest.raises(ValueError, match="No coverage row"):
            override_tier(conn, instrument_id=999, new_tier=2, rationale="test")

    def test_same_tier_raises(self) -> None:
        conn = self._mock_conn({"coverage_tier": 2, "symbol": "AAPL"})
        with pytest.raises(ValueError, match="already at Tier 2"):
            override_tier(conn, instrument_id=1, new_tier=2, rationale="test")

    def test_t1_cap_enforced_on_override(self) -> None:
        conn = self._mock_conn({"coverage_tier": 2, "symbol": "AAPL"}, t1_count=TIER_1_CAP)
        with pytest.raises(ValueError, match="cap"):
            override_tier(conn, instrument_id=1, new_tier=1, rationale="test")

    def test_t1_cap_not_checked_for_non_t1_override(self) -> None:
        """Overriding to T2 or T3 should not check T1 cap."""
        conn = self._mock_conn({"coverage_tier": 1, "symbol": "AAPL"})
        # Should succeed regardless of T1 count
        change = override_tier(conn, instrument_id=1, new_tier=3, rationale="Delisting")
        assert change.new_tier == 3

    def test_writes_audit_record(self) -> None:
        conn = self._mock_conn({"coverage_tier": 2, "symbol": "AAPL"}, t1_count=10)
        override_tier(conn, instrument_id=1, new_tier=1, rationale="Operator decision")
        audit_writes = [(sql, params) for sql, params in self._writes if "INSERT INTO coverage_audit" in sql]
        assert len(audit_writes) == 1
        assert audit_writes[0][1]["change_type"] == "override"


# ---------------------------------------------------------------------------
# seed_coverage
# ---------------------------------------------------------------------------


class TestSeedCoverage:
    """
    Tests for the first-run bootstrap seeding function.
    """

    def _mock_conn(self, coverage_count: int, inserted_rows: int) -> MagicMock:
        """Build a mock connection for seed_coverage tests.

        The first cursor call returns the coverage count.
        The execute call (bulk INSERT) returns a result with rowcount.
        """
        conn = MagicMock()

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"cnt": coverage_count}
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = mock_cursor

        mock_result = MagicMock()
        mock_result.rowcount = inserted_rows
        conn.execute.return_value = mock_result

        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_seeds_when_empty(self) -> None:
        """Empty coverage table triggers INSERT of tradable instruments."""
        conn = self._mock_conn(coverage_count=0, inserted_rows=500)
        result = seed_coverage(conn)
        assert result.seeded == 500
        assert result.already_populated is False
        # Verify the INSERT was called
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO coverage" in sql
        assert "is_tradable = TRUE" in sql

    def test_noop_when_populated(self) -> None:
        """Non-empty coverage table skips seeding entirely."""
        conn = self._mock_conn(coverage_count=100, inserted_rows=0)
        result = seed_coverage(conn)
        assert result.seeded == 0
        assert result.already_populated is True
        # The bulk INSERT should not have been called
        conn.execute.assert_not_called()

    def test_seeds_zero_when_no_tradable_instruments(self) -> None:
        """Empty coverage + no tradable instruments = 0 seeded."""
        conn = self._mock_conn(coverage_count=0, inserted_rows=0)
        result = seed_coverage(conn)
        assert result.seeded == 0
        assert result.already_populated is False

    def test_runs_inside_transaction(self) -> None:
        """The count check and INSERT must be in the same transaction."""
        conn = self._mock_conn(coverage_count=0, inserted_rows=10)
        seed_coverage(conn)
        conn.transaction.assert_called_once()


class TestBootstrapMissingCoverageRows:
    """Tests for the post-bootstrap gap-filler for newly-added instruments."""

    def _mock_conn(self, inserted_rows: int) -> MagicMock:
        conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = inserted_rows
        conn.execute.return_value = mock_result
        conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
        conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_inserts_missing_rows(self) -> None:
        """Three newly-added tradable instruments without coverage rows
        get Tier 3 entries."""
        from app.services.coverage import bootstrap_missing_coverage_rows

        conn = self._mock_conn(inserted_rows=3)
        result = bootstrap_missing_coverage_rows(conn)
        assert result.bootstrapped == 3
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO coverage" in sql
        assert "NOT EXISTS" in sql
        assert "is_tradable = TRUE" in sql

    def test_new_rows_marked_filings_status_unknown(self) -> None:
        """#268 Chunk G: rows inserted by the post-bootstrap gap filler
        must be marked filings_status='unknown' so the weekly audit
        picks them up without the null_anomalies counter flagging them
        as data-integrity warnings."""
        from app.services.coverage import bootstrap_missing_coverage_rows

        conn = self._mock_conn(inserted_rows=2)
        bootstrap_missing_coverage_rows(conn)
        sql = conn.execute.call_args[0][0]
        # Assert column + literal pair positionally: the INSERT
        # column list must include filings_status AND the SELECT
        # projection must end with 'unknown' as its last element.
        # Checking independent substring presence would accept a
        # SQL body that placed 'unknown' in an unrelated clause.
        import re

        assert re.search(
            r"INSERT INTO coverage \(instrument_id,\s*coverage_tier,\s*filings_status\)",
            sql,
        ), "filings_status must appear in the INSERT column list"
        assert re.search(
            r"SELECT i\.instrument_id,\s*3,\s*'unknown'",
            sql,
        ), "'unknown' literal must be the third SELECT projection value"

    def test_noop_when_no_gaps(self) -> None:
        """Every tradable instrument already has coverage → zero inserts."""
        from app.services.coverage import bootstrap_missing_coverage_rows

        conn = self._mock_conn(inserted_rows=0)
        result = bootstrap_missing_coverage_rows(conn)
        assert result.bootstrapped == 0

    def test_runs_inside_transaction(self) -> None:
        """Insert must be atomic — wrapped in conn.transaction()."""
        from app.services.coverage import bootstrap_missing_coverage_rows

        conn = self._mock_conn(inserted_rows=5)
        bootstrap_missing_coverage_rows(conn)
        conn.transaction.assert_called_once()

    def test_raises_on_unknown_rowcount(self) -> None:
        """rowcount=-1 (server did not report command tag) must raise, not return silently."""
        from app.services.coverage import bootstrap_missing_coverage_rows

        conn = self._mock_conn(inserted_rows=-1)
        with pytest.raises(RuntimeError, match="command tag"):
            bootstrap_missing_coverage_rows(conn)
