"""
Tests for app.services.portfolio.

Structure:
  - TestEvaluateExit    — _evaluate_exit pure logic
  - TestEvaluateAdd     — _evaluate_add pure logic
  - TestEvaluateBuy     — _evaluate_buy pure logic
  - TestShouldPersistHold — HOLD deduplication logic
  - TestRunPortfolioReview — end-to-end via run_portfolio_review with fake DB
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from app.services.portfolio import (
    EXIT_RED_FLAG_THRESHOLD,
    MAX_ACTIVE_POSITIONS,
    MAX_INITIAL_POSITION_PCT,
    PositionState,
    _evaluate_add,
    _evaluate_buy,
    _evaluate_exit,
    _should_persist_hold,
    run_portfolio_review,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 6, 1, 9, 0, 0, tzinfo=UTC)


def _pos(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    sector: str | None = "Technology",
    current_units: float = 100.0,
    cost_basis: float = 1000.0,
    market_value: float = 1200.0,
    quote_is_fallback: bool = False,
) -> PositionState:
    return PositionState(
        instrument_id=instrument_id,
        symbol=symbol,
        sector=sector,
        current_units=current_units,
        cost_basis=cost_basis,
        market_value=market_value,
        quote_is_fallback=quote_is_fallback,
    )


def _thesis(
    stance: str = "buy",
    confidence_score: float = 0.75,
    buy_zone_low: float | None = 140.0,
    buy_zone_high: float | None = 160.0,
    base_value: float | None = 200.0,
    break_conditions_json: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "stance": stance,
        "confidence_score": confidence_score,
        "buy_zone_low": buy_zone_low,
        "buy_zone_high": buy_zone_high,
        "base_value": base_value,
        "break_conditions_json": break_conditions_json,
    }


def _score(
    total_score: float = 0.60,
    confidence_score: float = 0.70,
    rank: int = 1,
    score_id: int = 101,
    model_version: str = "v1-balanced",
) -> dict[str, Any]:
    return {
        "total_score": total_score,
        "confidence_score": confidence_score,
        "rank": rank,
        "score_id": score_id,
        "model_version": model_version,
    }


# ---------------------------------------------------------------------------
# TestEvaluateExit
# ---------------------------------------------------------------------------


class TestEvaluateExit:
    def test_no_thesis_no_exit(self) -> None:
        pos = _pos()
        should_exit, _ = _evaluate_exit(pos, {}, current_price=150.0)
        assert should_exit is False

    def test_break_conditions_with_high_red_flag_triggers_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {
            "thesis": _thesis(break_conditions_json=["Revenue declines >20%"]),
            "max_red_flag": EXIT_RED_FLAG_THRESHOLD,
        }
        should_exit, reason = _evaluate_exit(pos, details, current_price=150.0)
        assert should_exit is True
        assert "break" in reason.lower()

    def test_break_conditions_below_threshold_no_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {
            "thesis": _thesis(break_conditions_json=["Revenue declines >20%"]),
            "max_red_flag": EXIT_RED_FLAG_THRESHOLD - 0.01,
        }
        should_exit, _ = _evaluate_exit(pos, details, current_price=150.0)
        assert should_exit is False

    def test_break_conditions_no_red_flag_no_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {
            "thesis": _thesis(break_conditions_json=["Revenue declines >20%"]),
            # no max_red_flag key
        }
        should_exit, _ = _evaluate_exit(pos, details, current_price=150.0)
        assert should_exit is False

    def test_valuation_target_reached_triggers_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {"thesis": _thesis(base_value=200.0)}
        # Price exactly at base_value
        should_exit, reason = _evaluate_exit(pos, details, current_price=200.0)
        assert should_exit is True
        assert "valuation" in reason.lower()

    def test_price_below_target_no_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {"thesis": _thesis(base_value=200.0)}
        should_exit, _ = _evaluate_exit(pos, details, current_price=199.99)
        assert should_exit is False

    def test_no_base_value_no_valuation_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {"thesis": _thesis(base_value=None)}
        should_exit, _ = _evaluate_exit(pos, details, current_price=999.0)
        assert should_exit is False

    def test_no_current_price_no_valuation_exit(self) -> None:
        pos = _pos()
        details: dict[str, Any] = {"thesis": _thesis(base_value=200.0)}
        should_exit, _ = _evaluate_exit(pos, details, current_price=None)
        assert should_exit is False


# ---------------------------------------------------------------------------
# TestEvaluateAdd
# ---------------------------------------------------------------------------


class TestEvaluateAdd:
    def _base_positions(self, pos: PositionState) -> dict[int, PositionState]:
        return {pos.instrument_id: pos}

    def test_conviction_improved_via_score_triggers_add(self) -> None:
        pos = _pos(market_value=500.0)  # 5% of 10000 AUM
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            # confidence unchanged → only score delta triggers ADD
            "prev_thesis_confidence": 0.65,
        }
        latest_score = _score(total_score=0.70, confidence_score=0.65)
        prev_score_total = 0.60  # delta = 0.10 >= 0.05
        should_add, reason = _evaluate_add(
            pos,
            details,
            latest_score,
            prev_score_total,
            total_aum=10_000.0,
            positions=self._base_positions(pos),
        )
        assert should_add is True
        assert "score improved" in reason

    def test_conviction_improved_via_confidence_triggers_add(self) -> None:
        pos = _pos(market_value=500.0)
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            "prev_thesis_confidence": 0.60,
        }
        latest_score = _score(total_score=0.62, confidence_score=0.70)  # conf delta = 0.10
        prev_score_total = 0.61  # score delta = 0.01 < threshold
        should_add, reason = _evaluate_add(
            pos,
            details,
            latest_score,
            prev_score_total,
            total_aum=10_000.0,
            positions=self._base_positions(pos),
        )
        assert should_add is True
        assert "confidence improved" in reason

    def test_no_improvement_no_add(self) -> None:
        pos = _pos(market_value=500.0)
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            "prev_thesis_confidence": 0.70,
        }
        latest_score = _score(total_score=0.62, confidence_score=0.70)
        prev_score_total = 0.61  # score delta = 0.01, conf delta = 0.0 — both below threshold
        should_add, _ = _evaluate_add(
            pos,
            details,
            latest_score,
            prev_score_total,
            total_aum=10_000.0,
            positions=self._base_positions(pos),
        )
        assert should_add is False

    def test_position_at_max_full_no_add(self) -> None:
        # market_value = 10% of AUM → already at max
        pos = _pos(market_value=1_000.0)
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            "prev_thesis_confidence": 0.60,
        }
        latest_score = _score(total_score=0.80, confidence_score=0.80)
        should_add, _ = _evaluate_add(
            pos,
            details,
            latest_score,
            prev_score_total=0.60,
            total_aum=10_000.0,
            positions=self._base_positions(pos),
        )
        assert should_add is False

    def test_non_buy_stance_blocks_add(self) -> None:
        pos = _pos(market_value=500.0)
        details: dict[str, Any] = {
            "thesis": _thesis(stance="hold"),
            "prev_thesis_confidence": 0.60,
        }
        latest_score = _score(total_score=0.80, confidence_score=0.80)
        should_add, _ = _evaluate_add(
            pos,
            details,
            latest_score,
            prev_score_total=0.60,
            total_aum=10_000.0,
            positions=self._base_positions(pos),
        )
        assert should_add is False

    def test_high_red_flag_blocks_add(self) -> None:
        pos = _pos(market_value=500.0)
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            "prev_thesis_confidence": 0.60,
            "max_red_flag": EXIT_RED_FLAG_THRESHOLD,
        }
        latest_score = _score(total_score=0.80, confidence_score=0.80)
        should_add, _ = _evaluate_add(
            pos,
            details,
            latest_score,
            prev_score_total=0.60,
            total_aum=10_000.0,
            positions=self._base_positions(pos),
        )
        assert should_add is False

    def test_sector_cap_blocks_add(self) -> None:
        # pos1: Technology 20%, pos2: Technology 4% (both below full size individually)
        # Sector total = 24%. Adding MAX_INITIAL_POSITION_PCT (5%) would push to 29% > 25%.
        pos1 = _pos(instrument_id=1, symbol="A", sector="Technology", market_value=2_000.0)
        pos2 = _pos(instrument_id=2, symbol="B", sector="Technology", market_value=400.0)
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            "prev_thesis_confidence": 0.60,
        }
        latest_score = _score(total_score=0.80, confidence_score=0.80)
        should_add, reason = _evaluate_add(
            pos2,
            details,
            latest_score,
            prev_score_total=0.60,
            total_aum=10_000.0,
            positions={1: pos1, 2: pos2},
        )
        # Technology = (2000+400)/10000 = 24%; add_pct = min(10%-4%, 5%) = 5%
        # sector_after = 24% + 5% = 29% > 25% cap
        assert should_add is False
        assert "sector" in reason.lower()


# ---------------------------------------------------------------------------
# TestEvaluateBuy
# ---------------------------------------------------------------------------


class TestEvaluateBuy:
    def _no_positions(self) -> dict[int, PositionState]:
        return {}

    def test_valid_candidate_produces_buy(self) -> None:
        details: dict[str, Any] = {"thesis": _thesis(stance="buy"), "sector": "Technology"}
        latest_score = _score(total_score=0.65)
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is True
        assert "Entry candidate" in reason

    def test_score_below_min_blocks_buy(self) -> None:
        details: dict[str, Any] = {"thesis": _thesis(stance="buy")}
        latest_score = _score(total_score=0.30)  # below MIN_BUY_SCORE=0.35
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "min_buy_score" in reason

    def test_non_buy_stance_blocks_buy(self) -> None:
        details: dict[str, Any] = {"thesis": _thesis(stance="hold")}
        latest_score = _score(total_score=0.70)
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "stance" in reason

    def test_max_positions_blocks_buy(self) -> None:
        positions = {i: _pos(instrument_id=i, symbol=f"S{i}") for i in range(MAX_ACTIVE_POSITIONS)}
        details: dict[str, Any] = {"thesis": _thesis(stance="buy")}
        latest_score = _score(total_score=0.80)
        should_buy, reason = _evaluate_buy(
            99,
            "NEW",
            "Technology",
            details,
            latest_score,
            positions,
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "max_active_positions" in reason

    def test_sector_cap_blocks_buy_from_held_exposure(self) -> None:
        # 25% already held in Technology — adding another would breach cap
        pos = _pos(sector="Technology", market_value=25_000.0)
        details: dict[str, Any] = {"thesis": _thesis(stance="buy")}
        latest_score = _score(total_score=0.80)
        should_buy, reason = _evaluate_buy(
            2,
            "MSFT",
            "Technology",
            details,
            latest_score,
            {1: pos},
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "sector" in reason

    def test_sector_cap_blocks_second_buy_via_accumulator(self) -> None:
        # No held positions; first BUY approved (5% Tech pending).
        # Second candidate in same sector: 0% held + 5% pending + 5% new = 10% — passes.
        # Third candidate: 0% + 5% + 5% = 10% after second — still fine.
        # Use 21% already pending to test the cap fires correctly.
        details: dict[str, Any] = {"thesis": _thesis(stance="buy")}
        latest_score = _score(total_score=0.80)
        # Simulate 4 prior BUYs in Technology already pending (4 × 5% = 20%)
        # → 5th BUY would push to 25% exactly (passes >, not >=)
        # → 6th would push to 30% and be blocked
        should_buy_fifth, _ = _evaluate_buy(
            5,
            "E",
            "Technology",
            details,
            latest_score,
            {},
            total_aum=100_000.0,
            cash=50_000.0,
            pending_buy_count=4,
            pending_sector_pct={"Technology": 0.20},
        )
        assert should_buy_fifth is True  # 20% + 5% = 25%, exactly at cap (not >, so passes)

        should_buy_sixth, reason = _evaluate_buy(
            6,
            "F",
            "Technology",
            details,
            latest_score,
            {},
            total_aum=100_000.0,
            cash=50_000.0,
            pending_buy_count=5,
            pending_sector_pct={"Technology": 0.25},
        )
        assert should_buy_sixth is False  # 25% + 5% = 30% > 25%
        assert "sector" in reason

    def test_insufficient_cash_blocks_buy(self) -> None:
        details: dict[str, Any] = {"thesis": _thesis(stance="buy")}
        latest_score = _score(total_score=0.70)
        # AUM=100k → initial_alloc=5k; cash=1k < 5k
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=1_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "cash" in reason.lower()

    def test_unknown_cash_allows_buy_with_note(self) -> None:
        details: dict[str, Any] = {"thesis": _thesis(stance="buy")}
        latest_score = _score(total_score=0.70)
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=None,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is True
        assert "cash_check_deferred" in reason

    def test_severe_red_flag_blocks_buy(self) -> None:
        details: dict[str, Any] = {
            "thesis": _thesis(stance="buy"),
            "max_red_flag": EXIT_RED_FLAG_THRESHOLD,
        }
        latest_score = _score(total_score=0.80)
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "red flag" in reason.lower()

    def test_no_thesis_blocks_buy(self) -> None:
        details: dict[str, Any] = {}
        latest_score = _score(total_score=0.80)
        should_buy, reason = _evaluate_buy(
            1,
            "AAPL",
            "Technology",
            details,
            latest_score,
            self._no_positions(),
            total_aum=100_000.0,
            cash=10_000.0,
            pending_buy_count=0,
            pending_sector_pct={},
        )
        assert should_buy is False
        assert "thesis" in reason.lower()


# ---------------------------------------------------------------------------
# TestShouldPersistHold
# ---------------------------------------------------------------------------


class TestShouldPersistHold:
    def test_no_prior_recommendation_persists(self) -> None:
        assert _should_persist_hold(1, "some reason", {}) is True

    def test_prior_hold_same_rationale_suppressed(self) -> None:
        prior = {1: {"action": "HOLD", "rationale": "same reason"}}
        assert _should_persist_hold(1, "same reason", prior) is False

    def test_prior_hold_different_rationale_persists(self) -> None:
        prior = {1: {"action": "HOLD", "rationale": "old reason"}}
        assert _should_persist_hold(1, "new reason", prior) is True

    def test_prior_action_not_hold_persists(self) -> None:
        prior = {1: {"action": "BUY", "rationale": "same reason"}}
        assert _should_persist_hold(1, "same reason", prior) is True


# ---------------------------------------------------------------------------
# TestRunPortfolioReview — integration via fake DB
# ---------------------------------------------------------------------------
#
# run_portfolio_review calls these DB functions in order:
#   1. _load_positions      — cursor: positions JOIN instruments LEFT JOIN quotes
#   2. _load_cash           — cursor: cash_ledger
#   3. _load_ranked_scores  — cursor: scores
#   4. _load_instrument_details — 4 cursors (instruments, theses, prev_thesis, filing_events)
#   5. _load_prev_scores    — cursor: scores (prev)
#   6. _load_prior_recommendations — cursor: trade_recommendations
#   7. conn.transaction() + N×conn.execute(INSERT)
#
# We build per-call cursor mocks via side_effect on conn.cursor so each
# call gets its own MagicMock with the right fetchall/fetchone result.
# conn.execute handles only INSERTs (no rows returned).
# ---------------------------------------------------------------------------


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor_sequence: list[MagicMock]) -> MagicMock:
    """
    Build a fake psycopg connection.
    conn.cursor() calls consume cursor_sequence in order.
    conn.execute() returns a no-op mock (INSERTs only).
    conn.transaction() is a no-op context manager.
    """
    conn = MagicMock()
    conn.cursor.side_effect = cursor_sequence
    conn.execute.return_value = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


class TestRunPortfolioReview:
    def test_buy_recommended_for_unowned_ranked_candidate(self) -> None:
        """
        Single ranked instrument, not held, passes all buy checks → BUY.
        """
        cursors = [
            # 1. _load_positions — no open positions
            _make_cursor([]),
            # 2. _load_cash — known cash balance
            _make_cursor([{"balance": 50_000.0}]),
            # 3. _load_ranked_scores
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "score_id": 10,
                        "total_score": 0.70,
                        "confidence_score": 0.75,
                        "rank": 1,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    }
                ]
            ),
            # 4a. _load_instrument_details: instruments
            _make_cursor([{"instrument_id": 1, "symbol": "AAPL", "sector": "Technology"}]),
            # 4b. theses
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "thesis_version": 2,
                        "stance": "buy",
                        "confidence_score": 0.75,
                        "buy_zone_low": 140.0,
                        "buy_zone_high": 160.0,
                        "base_value": 200.0,
                        "break_conditions_json": None,
                    }
                ]
            ),
            # 4c. prev thesis confidence
            _make_cursor([]),
            # 4d. filing_events red flags
            _make_cursor([]),
            # 5. _load_prev_scores — no prev (held_ranked_ids is empty, so this is skipped)
            # 6. _load_prior_recommendations
            _make_cursor([]),
        ]
        conn = _make_conn(cursors)
        result = run_portfolio_review(conn, "v1-balanced")

        assert len(result.recommendations) == 1
        rec = result.recommendations[0]
        assert rec.action == "BUY"
        assert rec.instrument_id == 1
        assert rec.suggested_size_pct == MAX_INITIAL_POSITION_PCT
        assert rec.score_id == 10
        assert rec.cash_balance_known is True

    def test_hold_when_position_at_max_full_and_no_exit_trigger(self) -> None:
        """
        Held at 10% of AUM (max_full), no exit trigger, no conviction improvement → HOLD.
        """
        cursors = [
            # _load_positions — one position at max_full (market_value = 10% of 100k AUM)
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "symbol": "AAPL",
                        "sector": "Technology",
                        "current_units": 100.0,
                        "cost_basis": 8_000.0,
                        "quote_price": 100.0,
                        "quote_is_fallback": False,
                    }
                ]
            ),
            # _load_cash
            _make_cursor([{"balance": 90_000.0}]),
            # _load_ranked_scores
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "score_id": 10,
                        "total_score": 0.70,
                        "confidence_score": 0.75,
                        "rank": 1,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    }
                ]
            ),
            # _load_instrument_details: instruments
            _make_cursor([{"instrument_id": 1, "symbol": "AAPL", "sector": "Technology"}]),
            # theses
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "thesis_version": 2,
                        "stance": "buy",
                        "confidence_score": 0.75,
                        "buy_zone_low": 140.0,
                        "buy_zone_high": 160.0,
                        "base_value": 200.0,
                        "break_conditions_json": None,
                    }
                ]
            ),
            # prev thesis
            _make_cursor([{"instrument_id": 1, "confidence_score": 0.75}]),
            # filing_events
            _make_cursor([]),
            # _load_prev_scores
            _make_cursor([{"instrument_id": 1, "total_score": 0.70}]),
            # _load_prior_recommendations
            _make_cursor([]),
        ]
        conn = _make_conn(cursors)
        result = run_portfolio_review(conn, "v1-balanced")

        recs = {r.instrument_id: r for r in result.recommendations}
        assert recs[1].action == "HOLD"

    def test_exit_on_thesis_break(self) -> None:
        """
        Held position with break conditions and high red flag → EXIT.
        """
        cursors = [
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "symbol": "XYZ",
                        "sector": "Energy",
                        "current_units": 50.0,
                        "cost_basis": 5_000.0,
                        "quote_price": 90.0,
                        "quote_is_fallback": False,
                    }
                ]
            ),
            _make_cursor([{"balance": 10_000.0}]),
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "score_id": 5,
                        "total_score": 0.40,
                        "confidence_score": 0.50,
                        "rank": 3,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    }
                ]
            ),
            _make_cursor([{"instrument_id": 1, "symbol": "XYZ", "sector": "Energy"}]),
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "thesis_version": 1,
                        "stance": "buy",
                        "confidence_score": 0.50,
                        "buy_zone_low": None,
                        "buy_zone_high": None,
                        "base_value": 200.0,
                        "break_conditions_json": ["Revenue declines >20%"],
                    }
                ]
            ),
            _make_cursor([]),
            # red flag above threshold
            _make_cursor([{"instrument_id": 1, "max_red_flag": EXIT_RED_FLAG_THRESHOLD}]),
            _make_cursor([{"instrument_id": 1, "total_score": 0.55}]),
            _make_cursor([]),
        ]
        conn = _make_conn(cursors)
        result = run_portfolio_review(conn, "v1-balanced")

        recs = {r.instrument_id: r for r in result.recommendations}
        assert recs[1].action == "EXIT"
        assert "break" in recs[1].rationale.lower()

    def test_hold_not_in_ranked_list(self) -> None:
        """
        Held instrument not in ranked list (no fresh score) → HOLD with explanatory note.
        """
        cursors = [
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "symbol": "OLD",
                        "sector": "Utilities",
                        "current_units": 10.0,
                        "cost_basis": 1_000.0,
                        "quote_price": 110.0,
                        "quote_is_fallback": False,
                    }
                ]
            ),
            _make_cursor([{"balance": 5_000.0}]),
            # ranked_scores — instrument 1 is NOT here
            _make_cursor([]),
            # _load_instrument_details — called for held instrument
            _make_cursor([{"instrument_id": 1, "symbol": "OLD", "sector": "Utilities"}]),
            _make_cursor([]),  # theses
            _make_cursor([]),  # prev thesis
            _make_cursor([]),  # filing_events
            # _load_prev_scores — skipped (no held_ranked_ids)
            # _load_prior_recommendations
            _make_cursor([]),
        ]
        conn = _make_conn(cursors)
        result = run_portfolio_review(conn, "v1-balanced")

        assert len(result.recommendations) == 1
        rec = result.recommendations[0]
        assert rec.action == "HOLD"
        assert "not in current ranked list" in rec.rationale

    def test_redundant_hold_not_persisted(self) -> None:
        """
        Prior recommendation is HOLD with the same rationale → INSERT not called.
        """
        cursors = [
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "symbol": "AAPL",
                        "sector": "Technology",
                        "current_units": 50.0,
                        "cost_basis": 5_000.0,
                        "quote_price": 120.0,
                        "quote_is_fallback": False,
                    }
                ]
            ),
            _make_cursor([{"balance": 10_000.0}]),
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "score_id": 10,
                        "total_score": 0.60,
                        "confidence_score": 0.70,
                        "rank": 2,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    }
                ]
            ),
            _make_cursor([{"instrument_id": 1, "symbol": "AAPL", "sector": "Technology"}]),
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "thesis_version": 1,
                        "stance": "buy",
                        "confidence_score": 0.70,
                        "buy_zone_low": None,
                        "buy_zone_high": None,
                        "base_value": 200.0,
                        "break_conditions_json": None,
                    }
                ]
            ),
            # prev thesis: same confidence → no conviction improvement
            _make_cursor([{"instrument_id": 1, "confidence_score": 0.70}]),
            _make_cursor([]),  # filing_events
            # prev score: same total → no score improvement
            _make_cursor([{"instrument_id": 1, "total_score": 0.60}]),
            # prior recommendation: HOLD with identical rationale
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "action": "HOLD",
                        "rationale": "No action trigger met; score=0.600 rank=2",
                    }
                ]
            ),
        ]
        conn = _make_conn(cursors)
        run_portfolio_review(conn, "v1-balanced")

        # No INSERT should have been called (redundant HOLD suppressed)
        conn.execute.assert_not_called()

    def test_sector_concentration_enforced_across_buys(self) -> None:
        """
        Held position at 21% Tech + two unowned Tech candidates ranked 1 and 2.
        First candidate: 21% + 5% = 26% > 25% — blocked.
        Both candidates should be blocked by the sector cap.

        This tests that _evaluate_buy uses held exposure correctly and that
        no BUY is approved when held exposure already fills the sector headroom.
        """
        # AUM = 100k; held Tech position = 21k (21%)
        cursors = [
            _make_cursor(
                [
                    {
                        "instrument_id": 10,
                        "symbol": "HELD",
                        "sector": "Technology",
                        "current_units": 210.0,
                        "cost_basis": 20_000.0,
                        "quote_price": 100.0,
                        "quote_is_fallback": False,
                    }
                ]
            ),
            # cash: 79k → AUM = 21k + 79k = 100k
            _make_cursor([{"balance": 79_000.0}]),
            # ranked scores — two new Tech candidates
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "score_id": 11,
                        "total_score": 0.80,
                        "confidence_score": 0.80,
                        "rank": 1,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    },
                    {
                        "instrument_id": 2,
                        "score_id": 12,
                        "total_score": 0.75,
                        "confidence_score": 0.75,
                        "rank": 2,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    },
                ]
            ),
            # instruments — includes held instrument 10 and candidates 1, 2
            _make_cursor(
                [
                    {"instrument_id": 1, "symbol": "AAPL", "sector": "Technology"},
                    {"instrument_id": 2, "symbol": "MSFT", "sector": "Technology"},
                    {"instrument_id": 10, "symbol": "HELD", "sector": "Technology"},
                ]
            ),
            # theses
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "thesis_version": 1,
                        "stance": "buy",
                        "confidence_score": 0.80,
                        "buy_zone_low": 140.0,
                        "buy_zone_high": 160.0,
                        "base_value": 200.0,
                        "break_conditions_json": None,
                    },
                    {
                        "instrument_id": 2,
                        "thesis_version": 1,
                        "stance": "buy",
                        "confidence_score": 0.75,
                        "buy_zone_low": 300.0,
                        "buy_zone_high": 340.0,
                        "base_value": 400.0,
                        "break_conditions_json": None,
                    },
                    {
                        "instrument_id": 10,
                        "thesis_version": 1,
                        "stance": "buy",
                        "confidence_score": 0.70,
                        "buy_zone_low": None,
                        "buy_zone_high": None,
                        "base_value": 150.0,
                        "break_conditions_json": None,
                    },
                ]
            ),
            _make_cursor([]),  # prev thesis
            _make_cursor([]),  # filing_events
            # _load_prev_scores for held instrument 10 (it IS ranked)
            _make_cursor([]),
            # prior recs
            _make_cursor([]),
        ]
        conn = _make_conn(cursors)

        # Held Tech = 21k/100k = 21%; first BUY would push to 26% > 25% — blocked
        result = run_portfolio_review(conn, "v1-balanced")

        buy_recs = [r for r in result.recommendations if r.action == "BUY"]
        assert len(buy_recs) == 0  # both blocked by sector cap from held exposure

    def test_empty_universe_returns_empty_result(self) -> None:
        cursors = [
            _make_cursor([]),  # positions
            _make_cursor([{"balance": None}]),  # cash — unknown
            _make_cursor([]),  # ranked scores
        ]
        conn = _make_conn(cursors)
        result = run_portfolio_review(conn, "v1-balanced")

        assert result.recommendations == []
        assert result.cash is None

    def test_unknown_cash_does_not_block_buy(self) -> None:
        """
        Empty ledger (cash=None) → BUY still recommended with cash_check_deferred note.
        """
        cursors = [
            _make_cursor([]),
            _make_cursor([{"balance": None}]),  # unknown cash
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "score_id": 10,
                        "total_score": 0.70,
                        "confidence_score": 0.75,
                        "rank": 1,
                        "model_version": "v1-balanced",
                        "scored_at": _NOW,
                    }
                ]
            ),
            _make_cursor([{"instrument_id": 1, "symbol": "AAPL", "sector": "Technology"}]),
            _make_cursor(
                [
                    {
                        "instrument_id": 1,
                        "thesis_version": 1,
                        "stance": "buy",
                        "confidence_score": 0.75,
                        "buy_zone_low": 140.0,
                        "buy_zone_high": 160.0,
                        "base_value": 200.0,
                        "break_conditions_json": None,
                    }
                ]
            ),
            _make_cursor([]),
            _make_cursor([]),
            _make_cursor([]),
        ]
        conn = _make_conn(cursors)
        result = run_portfolio_review(conn, "v1-balanced")

        assert len(result.recommendations) == 1
        rec = result.recommendations[0]
        assert rec.action == "BUY"
        assert "cash_check_deferred" in rec.rationale
        assert rec.cash_balance_known is False
