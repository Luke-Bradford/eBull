"""Tests for app.services.return_attribution."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.return_attribution import (
    ATTRIBUTION_METHOD,
    SUMMARY_WINDOWS,
    ZERO,
    AttributionResult,
    SummaryResult,
    _compute_average_return,
    _compute_market_return,
    _compute_sector_return,
    _load_position_fills,
    _load_price_series,
    _load_recommendation_for_fills,
    _load_score_snapshot,
    _load_sector_peers,
    compute_attribution,
    compute_attribution_summary,
    persist_attribution,
    persist_attribution_summary,
)

_D = Decimal

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    """Mock cursor whose fetchall returns rows and fetchone returns first row."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    """Mock connection that dispenses cursors in sequence."""
    conn = MagicMock()
    conn.cursor.side_effect = cursors
    return conn


def _dt(d: date) -> datetime:
    """Convert a date to a UTC datetime at midnight."""
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def _fill(
    fill_id: int = 1,
    order_id: int = 10,
    filled_at: datetime | None = None,
    price: str = "100.00",
    units: str = "10.00",
    fees: str = "1.00",
    action: str = "BUY",
) -> dict[str, Any]:
    if filled_at is None:
        filled_at = _dt(date(2025, 1, 10))
    return {
        "fill_id": fill_id,
        "order_id": order_id,
        "filled_at": filled_at,
        "price": _D(price),
        "units": _D(units),
        "fees": _D(fees),
        "action": action,
    }


def _price_row(price_date: date, close: str) -> dict[str, Any]:
    return {"price_date": price_date, "close": _D(close)}


# ===========================================================================
# TestConstants
# ===========================================================================


class TestConstants:
    def test_attribution_method_value(self) -> None:
        assert ATTRIBUTION_METHOD == "sector_relative_v1"

    def test_zero_is_decimal(self) -> None:
        assert isinstance(ZERO, Decimal)
        assert ZERO == _D("0")

    def test_summary_windows_contains_expected_values(self) -> None:
        assert 30 in SUMMARY_WINDOWS
        assert 90 in SUMMARY_WINDOWS
        assert 365 in SUMMARY_WINDOWS


# ===========================================================================
# TestComputeAverageReturn
# ===========================================================================


class TestComputeAverageReturn:
    def test_positive_return(self) -> None:
        prices = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 10), "110"),
        ]
        result = _compute_average_return(prices)
        assert result == _D("0.1")

    def test_negative_return(self) -> None:
        prices = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 10), "90"),
        ]
        result = _compute_average_return(prices)
        assert result == _D("-0.1")

    def test_zero_return_on_flat_price(self) -> None:
        prices = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 10), "100"),
        ]
        result = _compute_average_return(prices)
        assert result == ZERO

    def test_empty_list_returns_zero(self) -> None:
        assert _compute_average_return([]) == ZERO

    def test_single_row_returns_zero(self) -> None:
        prices = [_price_row(date(2025, 1, 1), "100")]
        assert _compute_average_return(prices) == ZERO

    def test_zero_first_price_returns_zero(self) -> None:
        prices = [
            _price_row(date(2025, 1, 1), "0"),
            _price_row(date(2025, 1, 10), "100"),
        ]
        assert _compute_average_return(prices) == ZERO

    def test_uses_first_and_last_rows_only(self) -> None:
        # Middle rows are ignored
        prices = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 5), "50"),  # middle — ignored
            _price_row(date(2025, 1, 10), "120"),
        ]
        result = _compute_average_return(prices)
        # (120 - 100) / 100 = 0.2
        assert result == _D("0.2")


# ===========================================================================
# TestLoadPositionFills
# ===========================================================================


class TestLoadPositionFills:
    def test_returns_rows_from_cursor(self) -> None:
        rows = [_fill(fill_id=1), _fill(fill_id=2, action="EXIT")]
        cur = _make_cursor(rows)
        conn = _make_conn([cur])
        result = _load_position_fills(conn, instrument_id=42)
        assert len(result) == 2
        assert result[0]["fill_id"] == 1

    def test_empty_table_returns_empty_list(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = _load_position_fills(conn, instrument_id=99)
        assert result == []

    def test_passes_instrument_id_as_param(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        _load_position_fills(conn, instrument_id=77)
        execute_call = cur.execute.call_args
        params = execute_call[0][1]
        assert params["iid"] == 77


# ===========================================================================
# TestLoadPriceSeries
# ===========================================================================


class TestLoadPriceSeries:
    def test_returns_rows_ordered_by_date(self) -> None:
        rows = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 5), "105"),
        ]
        cur = _make_cursor(rows)
        conn = _make_conn([cur])
        result = _load_price_series(conn, 1, date(2025, 1, 1), date(2025, 1, 5))
        assert len(result) == 2

    def test_passes_correct_params(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        _load_price_series(conn, 55, date(2025, 1, 1), date(2025, 3, 1))
        params = cur.execute.call_args[0][1]
        assert params["iid"] == 55
        assert params["start"] == date(2025, 1, 1)
        assert params["end"] == date(2025, 3, 1)

    def test_empty_result_returns_empty_list(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = _load_price_series(conn, 1, date(2025, 1, 1), date(2025, 1, 31))
        assert result == []


# ===========================================================================
# TestLoadScoreSnapshot
# ===========================================================================


class TestLoadScoreSnapshot:
    def test_returns_row_when_exists(self) -> None:
        row = {
            "score_id": 5,
            "total_score": _D("0.75"),
            "quality_score": _D("0.80"),
            "value_score": _D("0.70"),
            "turnaround_score": _D("0.65"),
            "momentum_score": _D("0.60"),
            "sentiment_score": _D("0.55"),
            "confidence_score": _D("0.85"),
            "model_version": "v1-balanced",
        }
        cur = _make_cursor([row])
        conn = _make_conn([cur])
        result = _load_score_snapshot(conn, score_id=5)
        assert result is not None
        assert result["total_score"] == _D("0.75")

    def test_returns_none_when_not_found(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = _load_score_snapshot(conn, score_id=999)
        assert result is None

    def test_passes_score_id_as_param(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        _load_score_snapshot(conn, score_id=42)
        params = cur.execute.call_args[0][1]
        assert params["sid"] == 42


# ===========================================================================
# TestLoadSectorPeers
# ===========================================================================


class TestLoadSectorPeers:
    def test_returns_peer_instrument_ids(self) -> None:
        rows = [{"instrument_id": 2}, {"instrument_id": 3}]
        cur = _make_cursor(rows)
        conn = _make_conn([cur])
        result = _load_sector_peers(conn, instrument_id=1)
        assert result == [2, 3]

    def test_no_peers_returns_empty_list(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = _load_sector_peers(conn, instrument_id=1)
        assert result == []

    def test_passes_instrument_id_as_param(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        _load_sector_peers(conn, instrument_id=10)
        params = cur.execute.call_args[0][1]
        assert params["iid"] == 10


# ===========================================================================
# TestLoadRecommendationForFills
# ===========================================================================


class TestLoadRecommendationForFills:
    def test_returns_most_recent_executed_buy(self) -> None:
        row = {"recommendation_id": 7, "score_id": 3}
        cur = _make_cursor([row])
        conn = _make_conn([cur])
        result = _load_recommendation_for_fills(conn, instrument_id=1)
        assert result is not None
        assert result["recommendation_id"] == 7
        assert result["score_id"] == 3

    def test_returns_none_when_no_executed_buy(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = _load_recommendation_for_fills(conn, instrument_id=1)
        assert result is None

    def test_passes_instrument_id_as_param(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        _load_recommendation_for_fills(conn, instrument_id=55)
        params = cur.execute.call_args[0][1]
        assert params["iid"] == 55


# ===========================================================================
# TestComputeMarketReturn
# ===========================================================================


class TestComputeMarketReturn:
    def test_returns_average_of_tier1_returns(self) -> None:
        # Two Tier 1 instruments: one +20%, one +10% → average = 15%
        tier1_cursor = _make_cursor([{"instrument_id": 1}, {"instrument_id": 2}])
        prices_1 = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 31), "120"),
        ]
        prices_2 = [
            _price_row(date(2025, 1, 1), "200"),
            _price_row(date(2025, 1, 31), "220"),
        ]
        price_cur_1 = _make_cursor(prices_1)
        price_cur_2 = _make_cursor(prices_2)
        conn = _make_conn([tier1_cursor, price_cur_1, price_cur_2])
        result = _compute_market_return(conn, date(2025, 1, 1), date(2025, 1, 31))
        # 0.20 + 0.10 = 0.30, / 2 = 0.15
        assert result == _D("0.15")

    def test_returns_zero_when_no_tier1_instruments(self) -> None:
        tier1_cursor = _make_cursor([])
        conn = _make_conn([tier1_cursor])
        result = _compute_market_return(conn, date(2025, 1, 1), date(2025, 1, 31))
        assert result == ZERO

    def test_returns_zero_when_tier1_has_no_price_data(self) -> None:
        tier1_cursor = _make_cursor([{"instrument_id": 1}])
        # Only one price row — not enough for a return
        price_cur = _make_cursor([_price_row(date(2025, 1, 1), "100")])
        conn = _make_conn([tier1_cursor, price_cur])
        result = _compute_market_return(conn, date(2025, 1, 1), date(2025, 1, 31))
        assert result == ZERO


# ===========================================================================
# TestComputeSectorReturn
# ===========================================================================


class TestComputeSectorReturn:
    def test_returns_average_of_peer_returns(self) -> None:
        peers_cursor = _make_cursor([{"instrument_id": 2}, {"instrument_id": 3}])
        prices_2 = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 31), "110"),
        ]
        prices_3 = [
            _price_row(date(2025, 1, 1), "100"),
            _price_row(date(2025, 1, 31), "130"),
        ]
        price_cur_2 = _make_cursor(prices_2)
        price_cur_3 = _make_cursor(prices_3)
        conn = _make_conn([peers_cursor, price_cur_2, price_cur_3])
        result = _compute_sector_return(conn, instrument_id=1, start=date(2025, 1, 1), end=date(2025, 1, 31))
        # peer 2: +10%, peer 3: +30% → average = 20%
        assert result == _D("0.20")

    def test_returns_zero_when_no_peers(self) -> None:
        peers_cursor = _make_cursor([])
        conn = _make_conn([peers_cursor])
        result = _compute_sector_return(conn, instrument_id=1, start=date(2025, 1, 1), end=date(2025, 1, 31))
        assert result == ZERO

    def test_returns_zero_when_peers_have_no_price_data(self) -> None:
        peers_cursor = _make_cursor([{"instrument_id": 2}])
        # Only one price row — not enough for a return
        price_cur = _make_cursor([_price_row(date(2025, 1, 1), "100")])
        conn = _make_conn([peers_cursor, price_cur])
        result = _compute_sector_return(conn, instrument_id=1, start=date(2025, 1, 1), end=date(2025, 1, 31))
        assert result == ZERO


# ===========================================================================
# TestComputeAttribution
# ===========================================================================


class TestComputeAttribution:
    """Tests for compute_attribution using patched internal loaders."""

    def _run(
        self,
        fills: list[dict[str, Any]],
        market_return: Decimal = _D("0.05"),
        sector_return: Decimal = _D("0.08"),
        rec: dict[str, Any] | None = None,
        score_snap: dict[str, Any] | None = None,
    ) -> AttributionResult | None:
        conn = MagicMock()
        with (
            patch("app.services.return_attribution._load_position_fills", return_value=fills),
            patch("app.services.return_attribution._compute_market_return", return_value=market_return),
            patch("app.services.return_attribution._compute_sector_return", return_value=sector_return),
            patch("app.services.return_attribution._load_recommendation_for_fills", return_value=rec),
            patch("app.services.return_attribution._load_score_snapshot", return_value=score_snap),
        ):
            return compute_attribution(conn, instrument_id=1)

    def test_returns_none_when_no_fills(self) -> None:
        result = self._run(fills=[])
        assert result is None

    def test_returns_none_when_no_exit_fills(self) -> None:
        fills = [_fill(action="BUY")]
        result = self._run(fills=fills)
        assert result is None

    def test_happy_path_gross_return_computation(self) -> None:
        # Buy at 100, sell at 110 → 10% gross
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 10)), price="100", units="10", fees="1", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 3, 10)), price="110", units="10", fees="1", action="EXIT"),
        ]
        result = self._run(fills=fills)
        assert result is not None
        assert result.gross_return_pct == _D("0.1")

    def test_model_alpha_is_gross_minus_sector_return(self) -> None:
        # gross = 0.10, sector = 0.08 → model_alpha = 0.02
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 10)), price="100", units="10", fees="0", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 3, 10)), price="110", units="10", fees="0", action="EXIT"),
        ]
        result = self._run(fills=fills, sector_return=_D("0.08"))
        assert result is not None
        assert result.model_alpha_pct == _D("0.02")

    def test_timing_alpha_is_always_zero(self) -> None:
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 10)), price="100", units="10", fees="0", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 3, 10)), price="110", units="10", fees="0", action="EXIT"),
        ]
        result = self._run(fills=fills)
        assert result is not None
        assert result.timing_alpha_pct == ZERO

    def test_residual_closes_the_equation(self) -> None:
        """Components must sum to gross_return (residual is the closure term)."""
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 10)), price="100", units="10", fees="2", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 3, 10)), price="115", units="10", fees="1", action="EXIT"),
        ]
        result = self._run(fills=fills, market_return=_D("0.05"), sector_return=_D("0.08"))
        assert result is not None
        component_sum = (
            result.market_return_pct
            + (result.sector_return_pct - result.market_return_pct)
            + result.model_alpha_pct
            + result.timing_alpha_pct
            + result.cost_drag_pct
            + result.residual_pct
        )
        # Should equal gross_return — allow tiny Decimal precision difference
        assert abs(component_sum - result.gross_return_pct) < _D("0.000001")

    def test_cost_drag_is_fees_over_entry_cost(self) -> None:
        # entry_cost = 100 * 10 = 1000, total_fees = 5 (BUY) + 3 (EXIT) = 8
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 10)), price="100", units="10", fees="5", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 3, 10)), price="110", units="10", fees="3", action="EXIT"),
        ]
        result = self._run(fills=fills)
        assert result is not None
        # cost_drag = (5 + 3) / 1000 = 0.008
        assert result.cost_drag_pct == _D("0.008")

    def test_hold_days_computed_correctly(self) -> None:
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 1)), price="100", units="10", fees="0", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 4, 10)), price="110", units="10", fees="0", action="EXIT"),
        ]
        result = self._run(fills=fills)
        assert result is not None
        # Jan 1 to Apr 10 = 99 days
        assert result.hold_days == (date(2025, 4, 10) - date(2025, 1, 1)).days

    def test_entry_and_exit_fill_ids_assigned(self) -> None:
        fills = [
            _fill(fill_id=11, filled_at=_dt(date(2025, 1, 1)), action="BUY"),
            _fill(fill_id=22, filled_at=_dt(date(2025, 3, 1)), action="EXIT"),
        ]
        result = self._run(fills=fills)
        assert result is not None
        assert result.entry_fill_id == 11
        assert result.exit_fill_id == 22

    def test_recommendation_id_none_when_no_rec(self) -> None:
        fills = [
            _fill(fill_id=1, action="BUY"),
            _fill(fill_id=2, action="EXIT"),
        ]
        result = self._run(fills=fills, rec=None)
        assert result is not None
        assert result.recommendation_id is None

    def test_score_at_entry_populated_from_snapshot(self) -> None:
        rec = {"recommendation_id": 7, "score_id": 3}
        score_snap = {
            "score_id": 3,
            "total_score": _D("0.72"),
            "quality_score": _D("0.80"),
            "value_score": _D("0.70"),
            "turnaround_score": _D("0.65"),
            "momentum_score": _D("0.60"),
            "sentiment_score": _D("0.55"),
            "confidence_score": _D("0.85"),
            "model_version": "v1-balanced",
        }
        fills = [
            _fill(fill_id=1, action="BUY"),
            _fill(fill_id=2, action="EXIT"),
        ]
        result = self._run(fills=fills, rec=rec, score_snap=score_snap)
        assert result is not None
        assert result.score_at_entry == _D("0.72")
        assert result.recommendation_id == 7

    def test_score_at_entry_none_when_score_id_is_null(self) -> None:
        rec = {"recommendation_id": 7, "score_id": None}
        fills = [
            _fill(fill_id=1, action="BUY"),
            _fill(fill_id=2, action="EXIT"),
        ]
        result = self._run(fills=fills, rec=rec, score_snap=None)
        assert result is not None
        assert result.score_at_entry is None
        assert result.recommendation_id == 7

    def test_weighted_average_entry_price_with_multiple_buys(self) -> None:
        # Two BUY fills at different prices/sizes: 100@10 + 120@5 = 1600/15 ≈ 106.667
        fills = [
            _fill(fill_id=1, filled_at=_dt(date(2025, 1, 1)), price="100", units="10", fees="0", action="BUY"),
            _fill(fill_id=2, filled_at=_dt(date(2025, 1, 5)), price="120", units="5", fees="0", action="ADD"),
            _fill(fill_id=3, filled_at=_dt(date(2025, 3, 1)), price="130", units="15", fees="0", action="EXIT"),
        ]
        result = self._run(fills=fills)
        assert result is not None
        # avg_entry = 1600/15, avg_exit = 130
        # gross = (130 - 1600/15) / (1600/15)
        avg_entry = _D("1600") / _D("15")
        expected_gross = (_D("130") - avg_entry) / avg_entry
        assert abs(result.gross_return_pct - expected_gross) < _D("0.000001")

    def test_instrument_id_preserved_on_result(self) -> None:
        fills = [
            _fill(fill_id=1, action="BUY"),
            _fill(fill_id=2, action="EXIT"),
        ]
        conn = MagicMock()
        with (
            patch("app.services.return_attribution._load_position_fills", return_value=fills),
            patch("app.services.return_attribution._compute_market_return", return_value=ZERO),
            patch("app.services.return_attribution._compute_sector_return", return_value=ZERO),
            patch("app.services.return_attribution._load_recommendation_for_fills", return_value=None),
            patch("app.services.return_attribution._load_score_snapshot", return_value=None),
        ):
            result = compute_attribution(conn, instrument_id=42)
        assert result is not None
        assert result.instrument_id == 42

    def test_market_and_sector_return_zero_when_no_data(self) -> None:
        fills = [
            _fill(fill_id=1, action="BUY"),
            _fill(fill_id=2, action="EXIT"),
        ]
        result = self._run(fills=fills, market_return=ZERO, sector_return=ZERO)
        assert result is not None
        assert result.market_return_pct == ZERO
        assert result.sector_return_pct == ZERO


# ===========================================================================
# TestPersistAttribution
# ===========================================================================


class TestPersistAttribution:
    def _make_result(self, **overrides: Any) -> AttributionResult:
        defaults: dict[str, Any] = {
            "instrument_id": 1,
            "hold_start": date(2025, 1, 10),
            "hold_end": date(2025, 3, 10),
            "hold_days": 59,
            "gross_return_pct": _D("0.10"),
            "market_return_pct": _D("0.05"),
            "sector_return_pct": _D("0.08"),
            "model_alpha_pct": _D("0.02"),
            "timing_alpha_pct": ZERO,
            "cost_drag_pct": _D("0.002"),
            "residual_pct": _D("0.078"),
            "score_at_entry": _D("0.75"),
            "score_components": {"quality_score": 0.8},
            "entry_fill_id": 1,
            "exit_fill_id": 2,
            "recommendation_id": 7,
        }
        defaults.update(overrides)
        return AttributionResult(**defaults)

    def test_executes_insert_with_attribution_method(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = self._make_result()
        persist_attribution(conn, result)
        assert cur.execute.called
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO return_attribution" in sql

    def test_params_contain_all_required_fields(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = self._make_result()
        persist_attribution(conn, result)
        params = cur.execute.call_args[0][1]
        assert params["instrument_id"] == 1
        assert params["hold_days"] == 59
        assert params["gross_return_pct"] == _D("0.10")
        assert params["attribution_method"] == ATTRIBUTION_METHOD

    def test_null_score_components_passes_none(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = self._make_result(score_components=None)
        persist_attribution(conn, result)
        params = cur.execute.call_args[0][1]
        assert params["score_components"] is None

    def test_score_components_wrapped_in_jsonb(self) -> None:
        from psycopg.types.json import Jsonb

        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = self._make_result(score_components={"quality_score": 0.8})
        persist_attribution(conn, result)
        params = cur.execute.call_args[0][1]
        assert isinstance(params["score_components"], Jsonb)

    def test_computed_at_is_utc_datetime(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        result = self._make_result()
        persist_attribution(conn, result)
        params = cur.execute.call_args[0][1]
        computed_at = params["computed_at"]
        assert isinstance(computed_at, datetime)
        assert computed_at.tzinfo is not None


# ===========================================================================
# TestComputeAttributionSummary
# ===========================================================================


class TestComputeAttributionSummary:
    def test_happy_path_returns_summary_with_averages(self) -> None:
        row = {
            "positions_attributed": 5,
            "avg_gross_return_pct": _D("0.12"),
            "avg_market_return_pct": _D("0.05"),
            "avg_sector_return_pct": _D("0.08"),
            "avg_model_alpha_pct": _D("0.04"),
            "avg_timing_alpha_pct": _D("0"),
            "avg_cost_drag_pct": _D("0.003"),
        }
        cur = _make_cursor([row])
        conn = _make_conn([cur])
        result = compute_attribution_summary(conn, window_days=90)
        assert result.window_days == 90
        assert result.positions_attributed == 5
        assert result.avg_gross_return_pct == _D("0.12")
        assert result.avg_model_alpha_pct == _D("0.04")

    def test_empty_window_returns_zero_count_none_averages(self) -> None:
        row = {
            "positions_attributed": 0,
            "avg_gross_return_pct": None,
            "avg_market_return_pct": None,
            "avg_sector_return_pct": None,
            "avg_model_alpha_pct": None,
            "avg_timing_alpha_pct": None,
            "avg_cost_drag_pct": None,
        }
        cur = _make_cursor([row])
        conn = _make_conn([cur])
        result = compute_attribution_summary(conn, window_days=30)
        assert result.positions_attributed == 0
        assert result.avg_gross_return_pct is None
        assert result.avg_model_alpha_pct is None

    def test_none_row_returns_zero_count_none_averages(self) -> None:
        """If fetchone returns None (e.g. an unusual driver state), return defaults."""
        cur = MagicMock()
        cur.fetchone.return_value = None
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        conn = _make_conn([cur])
        result = compute_attribution_summary(conn, window_days=365)
        assert result.positions_attributed == 0
        assert result.avg_gross_return_pct is None

    def test_passes_window_days_as_param(self) -> None:
        row = {
            "positions_attributed": 0,
            "avg_gross_return_pct": None,
            "avg_market_return_pct": None,
            "avg_sector_return_pct": None,
            "avg_model_alpha_pct": None,
            "avg_timing_alpha_pct": None,
            "avg_cost_drag_pct": None,
        }
        cur = _make_cursor([row])
        conn = _make_conn([cur])
        compute_attribution_summary(conn, window_days=90)
        params = cur.execute.call_args[0][1]
        assert params["window_days"] == 90

    def test_averages_are_decimal_not_float(self) -> None:
        row = {
            "positions_attributed": 3,
            "avg_gross_return_pct": _D("0.07"),
            "avg_market_return_pct": _D("0.03"),
            "avg_sector_return_pct": _D("0.05"),
            "avg_model_alpha_pct": _D("0.02"),
            "avg_timing_alpha_pct": _D("0"),
            "avg_cost_drag_pct": _D("0.001"),
        }
        cur = _make_cursor([row])
        conn = _make_conn([cur])
        result = compute_attribution_summary(conn, window_days=30)
        assert isinstance(result.avg_gross_return_pct, Decimal)
        assert isinstance(result.avg_model_alpha_pct, Decimal)


# ===========================================================================
# TestPersistAttributionSummary
# ===========================================================================


class TestPersistAttributionSummary:
    def _make_summary(self, **overrides: Any) -> SummaryResult:
        defaults: dict[str, Any] = {
            "window_days": 90,
            "positions_attributed": 5,
            "avg_gross_return_pct": _D("0.12"),
            "avg_market_return_pct": _D("0.05"),
            "avg_sector_return_pct": _D("0.08"),
            "avg_model_alpha_pct": _D("0.04"),
            "avg_timing_alpha_pct": ZERO,
            "avg_cost_drag_pct": _D("0.003"),
        }
        defaults.update(overrides)
        return SummaryResult(**defaults)

    def test_executes_insert_into_summary_table(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        summary = self._make_summary()
        persist_attribution_summary(conn, summary)
        assert cur.execute.called
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO return_attribution_summary" in sql

    def test_params_contain_all_required_fields(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        summary = self._make_summary()
        persist_attribution_summary(conn, summary)
        params = cur.execute.call_args[0][1]
        assert params["window_days"] == 90
        assert params["positions_attributed"] == 5
        assert params["avg_gross_return_pct"] == _D("0.12")
        assert params["avg_model_alpha_pct"] == _D("0.04")

    def test_none_averages_passed_through_as_none(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        summary = self._make_summary(avg_gross_return_pct=None, avg_model_alpha_pct=None)
        persist_attribution_summary(conn, summary)
        params = cur.execute.call_args[0][1]
        assert params["avg_gross_return_pct"] is None
        assert params["avg_model_alpha_pct"] is None

    def test_computed_at_is_utc_datetime(self) -> None:
        cur = _make_cursor([])
        conn = _make_conn([cur])
        summary = self._make_summary()
        persist_attribution_summary(conn, summary)
        params = cur.execute.call_args[0][1]
        computed_at = params["computed_at"]
        assert isinstance(computed_at, datetime)
        assert computed_at.tzinfo is not None


# ===========================================================================
# TestAttributionResultDataclass
# ===========================================================================


class TestAttributionResultDataclass:
    def test_frozen_raises_on_mutation(self) -> None:
        result = AttributionResult(
            instrument_id=1,
            hold_start=date(2025, 1, 1),
            hold_end=date(2025, 3, 1),
            hold_days=59,
            gross_return_pct=_D("0.10"),
            market_return_pct=_D("0.05"),
            sector_return_pct=_D("0.08"),
            model_alpha_pct=_D("0.02"),
            timing_alpha_pct=ZERO,
            cost_drag_pct=_D("0.002"),
            residual_pct=_D("0.078"),
            score_at_entry=None,
            score_components=None,
            entry_fill_id=1,
            exit_fill_id=2,
            recommendation_id=None,
        )
        with pytest.raises((AttributeError, TypeError)):
            result.instrument_id = 999  # type: ignore[misc]


# ===========================================================================
# TestSummaryResultDataclass
# ===========================================================================


class TestSummaryResultDataclass:
    def test_frozen_raises_on_mutation(self) -> None:
        summary = SummaryResult(
            window_days=30,
            positions_attributed=0,
            avg_gross_return_pct=None,
            avg_market_return_pct=None,
            avg_sector_return_pct=None,
            avg_model_alpha_pct=None,
            avg_timing_alpha_pct=None,
            avg_cost_drag_pct=None,
        )
        with pytest.raises((AttributeError, TypeError)):
            summary.window_days = 90  # type: ignore[misc]
