"""Tests for app.services.tax_ledger — the legacy tax views (the matcher moved to tests/test_tax_matching.py)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from app.services.tax_ledger import (
    _cgt_rates_for_disposal,
    _compute_tax_year,
    _to_uk_date,
    available_tax_years,
    current_tax_year,
    disposals_for_tax_year,
    s104_pool_rows,
    tax_year_summary,
    valid_tax_year,
)

_D = Decimal
_UK = ZoneInfo("Europe/London")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor_sequence: list[MagicMock]) -> MagicMock:
    conn = MagicMock()
    conn.cursor.side_effect = cursor_sequence
    conn.execute.return_value = MagicMock()
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=None)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    return conn


# ===========================================================================
# TestToUkDate
# ===========================================================================


class TestToUkDate:
    def test_utc_midnight_same_uk_date_in_winter(self) -> None:
        # UTC midnight on 15 Jan (GMT, no offset) -> same UK date
        dt = datetime(2025, 1, 15, 0, 0, 0, tzinfo=UTC)
        assert _to_uk_date(dt) == date(2025, 1, 15)

    def test_utc_2330_during_bst_is_next_uk_date(self) -> None:
        # UTC 23:30 on 5 April during BST (UTC+1) -> 6 April UK time
        # BST runs from last Sunday of March to last Sunday of October
        dt = datetime(2025, 4, 5, 23, 30, 0, tzinfo=UTC)
        assert _to_uk_date(dt) == date(2025, 4, 6)

    def test_utc_0030_during_gmt_same_uk_date(self) -> None:
        # UTC 00:30 on 15 Jan (GMT) -> still 15 Jan UK time
        dt = datetime(2025, 1, 15, 0, 30, 0, tzinfo=UTC)
        assert _to_uk_date(dt) == date(2025, 1, 15)

    def test_utc_2230_during_bst_same_uk_date(self) -> None:
        # UTC 22:30 on 1 July (BST, UTC+1) -> 23:30 UK = still 1 July
        dt = datetime(2025, 7, 1, 22, 30, 0, tzinfo=UTC)
        assert _to_uk_date(dt) == date(2025, 7, 1)


# ===========================================================================
# TestComputeTaxYear
# ===========================================================================


class TestComputeTaxYear:
    def test_january_falls_in_prior_year(self) -> None:
        assert _compute_tax_year(date(2025, 1, 15)) == "2024/25"

    def test_april_5_falls_in_prior_year(self) -> None:
        assert _compute_tax_year(date(2025, 4, 5)) == "2024/25"

    def test_april_6_falls_in_current_year(self) -> None:
        assert _compute_tax_year(date(2025, 4, 6)) == "2025/26"

    def test_december_falls_in_current_year(self) -> None:
        assert _compute_tax_year(date(2025, 12, 31)) == "2025/26"

    def test_century_boundary(self) -> None:
        assert _compute_tax_year(date(2099, 6, 1)) == "2099/00"


# ===========================================================================
# TestCgtRateLookup
# ===========================================================================


class TestCgtRateLookup:
    def test_pre_budget_2024_25_uses_old_rates(self) -> None:
        basic, higher = _cgt_rates_for_disposal(date(2024, 9, 15))
        assert basic == _D("0.10")
        assert higher == _D("0.20")

    def test_post_budget_2024_25_uses_new_rates(self) -> None:
        basic, higher = _cgt_rates_for_disposal(date(2025, 1, 15))
        assert basic == _D("0.18")
        assert higher == _D("0.24")

    def test_2025_26_uses_current_rates(self) -> None:
        basic, higher = _cgt_rates_for_disposal(date(2025, 7, 1))
        assert basic == _D("0.18")
        assert higher == _D("0.24")

    def test_date_before_earliest_period_raises(self) -> None:
        with pytest.raises(RuntimeError, match="No CGT rate period"):
            _cgt_rates_for_disposal(date(2020, 1, 1))


# ===========================================================================
# TestTaxYearSummary — DB interaction
# ===========================================================================


class TestTaxYearSummary:
    def test_aggregates_gains_losses_and_exemption(self) -> None:
        # Cursor 1: aggregate query
        agg_cursor = _make_cursor(
            [
                {
                    "total_gains": _D("5000"),
                    "total_losses": _D("-1000"),
                    "net_gain": _D("4000"),
                    "cnt_same_day": 2,
                    "cnt_bnb": 1,
                    "cnt_s104": 3,
                    "cnt_total": 5,
                }
            ]
        )
        # Cursor 2: per-match CGT rows (all in 2025/26 -> 18%/24%)
        gain_cursor = _make_cursor(
            [
                {"gain_or_loss_gbp": _D("3000"), "disposal_uk_date": date(2025, 7, 1)},
                {"gain_or_loss_gbp": _D("2000"), "disposal_uk_date": date(2025, 9, 1)},
            ]
        )
        # Cursor 3: dividends
        div_cursor = _make_cursor([{"dividend_total": _D("200")}])

        conn = _make_conn([agg_cursor, gain_cursor, div_cursor])

        result = tax_year_summary(conn, "2025/26")

        assert result.tax_year == "2025/26"
        assert result.total_gains_gbp == _D("5000")
        assert result.total_losses_gbp == _D("-1000")
        assert result.net_gain_gbp == _D("4000")
        assert result.dividend_total_gbp == _D("200")
        assert result.disposals_same_day == 2
        assert result.disposals_bed_and_breakfast == 1
        assert result.disposals_s104 == 3
        assert result.disposals_total == 5

        # Annual exempt fully consumed: net 4000 > 3000 allowance -> 0 left.
        assert result.annual_exempt_gbp == _D("3000")
        assert result.exempt_remaining_gbp == _D("0")

        # CGT: net=4000, taxable=4000-3000=1000
        # Weighted basic: 3000*0.18 + 2000*0.18 = 900
        # Scale: 1000/5000 = 0.2
        # est_basic = 900 * 0.2 = 180
        assert result.estimated_cgt_basic_scenario == _D("180")
        # Weighted higher: 3000*0.24 + 2000*0.24 = 1200
        # est_higher = 1200 * 0.2 = 240
        assert result.estimated_cgt_higher_scenario == _D("240")

    def test_empty_tax_year_returns_zeros(self) -> None:
        agg_cursor = _make_cursor(
            [
                {
                    "total_gains": _D("0"),
                    "total_losses": _D("0"),
                    "net_gain": _D("0"),
                    "cnt_same_day": 0,
                    "cnt_bnb": 0,
                    "cnt_s104": 0,
                    "cnt_total": 0,
                }
            ]
        )
        gain_cursor = _make_cursor([])
        div_cursor = _make_cursor([{"dividend_total": _D("0")}])

        conn = _make_conn([agg_cursor, gain_cursor, div_cursor])
        result = tax_year_summary(conn, "2025/26")

        assert result.net_gain_gbp == _D("0")
        assert result.estimated_cgt_basic_scenario == _D("0")
        assert result.estimated_cgt_higher_scenario == _D("0")
        # No gains -> the full £3,000 allowance is unused.
        assert result.exempt_remaining_gbp == _D("3000")

    def test_2024_25_split_year_cgt_estimate(self) -> None:
        """Disposals spanning the 30 Oct 2024 rate change use different rates."""
        agg_cursor = _make_cursor(
            [
                {
                    "total_gains": _D("10000"),
                    "total_losses": _D("0"),
                    "net_gain": _D("10000"),
                    "cnt_same_day": 0,
                    "cnt_bnb": 0,
                    "cnt_s104": 2,
                    "cnt_total": 2,
                }
            ]
        )
        # One disposal pre-budget (10%), one post-budget (18%)
        gain_cursor = _make_cursor(
            [
                {
                    "gain_or_loss_gbp": _D("5000"),
                    "disposal_uk_date": date(2024, 9, 1),
                },
                {
                    "gain_or_loss_gbp": _D("5000"),
                    "disposal_uk_date": date(2025, 1, 15),
                },
            ]
        )
        div_cursor = _make_cursor([{"dividend_total": _D("0")}])

        conn = _make_conn([agg_cursor, gain_cursor, div_cursor])
        result = tax_year_summary(conn, "2024/25")

        # Weighted basic: 5000*0.10 + 5000*0.18 = 500+900 = 1400
        # Taxable: 10000 - 3000 = 7000, scale = 7000/10000 = 0.7
        # est_basic = 1400 * 0.7 = 980
        assert result.estimated_cgt_basic_scenario == _D("980")
        # Weighted higher: 5000*0.20 + 5000*0.24 = 1000+1200 = 2200
        # est_higher = 2200 * 0.7 = 1540
        assert result.estimated_cgt_higher_scenario == _D("1540")


# ===========================================================================
# TestTaxYearHelpers (#1905) — pure, fast tier
# ===========================================================================


class TestValidTaxYear:
    def test_current_and_supported_floor_valid(self) -> None:
        assert valid_tax_year(current_tax_year()) is True
        # 2024/25 is the first year the engine has CGT rate coverage for.
        assert valid_tax_year("2024/25") is True

    def test_impossible_suffix_rejected(self) -> None:
        # Shape passes \d{4}/\d{2} but 99 != (2026+1) % 100.
        assert valid_tax_year("2026/99") is False
        assert valid_tax_year("2026/28") is False

    def test_malformed_shape_rejected(self) -> None:
        for bad in ["202627", "26/27", "2026/7", "abcd/ef", "2026-27", "2026/2027", ""]:
            assert valid_tax_year(bad) is False

    def test_out_of_range_start_rejected(self) -> None:
        # Suffix arithmetic is correct, but the start year is out of bounds.
        assert valid_tax_year("2023/24") is False  # below the engine's rate-coverage floor
        assert valid_tax_year("1999/00") is False  # far below
        assert valid_tax_year("2099/00") is False  # far future


class TestCurrentTaxYear:
    def test_format_and_self_consistency(self) -> None:
        ty = current_tax_year()
        assert valid_tax_year(ty) is True
        start, suffix = ty.split("/")
        assert len(start) == 4 and len(suffix) == 2
        assert int(suffix) == (int(start) + 1) % 100


class TestDisposalsForTaxYear:
    def test_maps_rows_with_symbol_and_null_acquisition(self) -> None:
        cur = _make_cursor(
            [
                {
                    "match_id": 7,
                    "instrument_id": 100,
                    "symbol": "AAPL",
                    "disposal_tax_lot_id": 55,
                    "acquisition_tax_lot_id": 40,
                    "matching_rule": "s104_pool",
                    "matched_units": _D("10"),
                    "acquisition_cost_gbp": _D("800"),
                    "disposal_proceeds_gbp": _D("1200"),
                    "gain_or_loss_gbp": _D("400"),
                    "disposal_uk_date": date(2025, 7, 1),
                    "tax_year": "2025/26",
                    "matched_at": datetime(2025, 7, 2, 9, 0, 0, tzinfo=UTC),
                },
                {
                    "match_id": 8,
                    "instrument_id": 101,
                    "symbol": "TSLA",
                    "disposal_tax_lot_id": 56,
                    "acquisition_tax_lot_id": None,  # s104 disposal with no single acquisition lot
                    "matching_rule": "s104_pool",
                    "matched_units": _D("5"),
                    "acquisition_cost_gbp": _D("500"),
                    "disposal_proceeds_gbp": _D("450"),
                    "gain_or_loss_gbp": _D("-50"),
                    "disposal_uk_date": date(2025, 8, 1),
                    "tax_year": "2025/26",
                    "matched_at": datetime(2025, 8, 2, 9, 0, 0, tzinfo=UTC),
                },
            ]
        )
        conn = _make_conn([cur])
        rows = disposals_for_tax_year(conn, "2025/26")

        assert [r.symbol for r in rows] == ["AAPL", "TSLA"]
        assert rows[0].gain_or_loss_gbp == _D("400")
        assert rows[0].acquisition_tax_lot_id == 40
        assert rows[1].acquisition_tax_lot_id is None
        assert rows[1].gain_or_loss_gbp == _D("-50")

    def test_empty_year_returns_empty_list(self) -> None:
        conn = _make_conn([_make_cursor([])])
        assert disposals_for_tax_year(conn, "2025/26") == []


class TestS104PoolRows:
    def test_maps_pool_rows(self) -> None:
        cur = _make_cursor(
            [
                {
                    "instrument_id": 100,
                    "symbol": "AAPL",
                    "pool_units": _D("30"),
                    "pool_cost_gbp": _D("2400"),
                    "pool_avg_cost_gbp": _D("80"),
                    "updated_at": datetime(2025, 9, 1, 12, 0, 0, tzinfo=UTC),
                }
            ]
        )
        conn = _make_conn([cur])
        rows = s104_pool_rows(conn)
        assert len(rows) == 1
        assert rows[0].symbol == "AAPL"
        assert rows[0].pool_avg_cost_gbp == _D("80")

    def test_empty_returns_empty_list(self) -> None:
        conn = _make_conn([_make_cursor([])])
        assert s104_pool_rows(conn) == []


class TestAvailableTaxYears:
    def test_union_includes_current_and_sorts_desc(self) -> None:
        cur = _make_cursor([{"tax_year": "2024/25"}, {"tax_year": "2023/24"}])
        conn = _make_conn([cur])
        years = available_tax_years(conn)
        assert years == sorted(set(years), reverse=True)  # sorted desc, deduped
        assert current_tax_year() in years  # current always present
        assert "2024/25" in years and "2023/24" in years

    def test_empty_tables_yield_current_only(self) -> None:
        conn = _make_conn([_make_cursor([])])
        assert available_tax_years(conn) == [current_tax_year()]
