"""Tests for app.services.tax_ledger — UK disposal matching and tax views."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from app.services.tax_ledger import (
    TaxEventDirection,
    TaxLot,
    _cgt_rates_for_disposal,
    _compute_tax_year,
    _match_disposals_for_instrument,
    _to_uk_date,
    ingest_tax_events,
    run_disposal_matching,
    tax_year_summary,
)

_D = Decimal
_UK = ZoneInfo("Europe/London")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lot(
    tax_lot_id: int = 1,
    instrument_id: int = 100,
    event_time: datetime | None = None,
    event_type: str = "BUY",
    direction: TaxEventDirection = "acquisition",
    quantity: Decimal = _D("100"),
    cost_or_proceeds: Decimal = _D("1000"),
    amount_gbp: Decimal = _D("800"),
    tax_year: str = "2025/26",
    reference_fill_id: int | None = 1,
) -> TaxLot:
    if event_time is None:
        event_time = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)
    uk_date = _to_uk_date(event_time)
    return TaxLot(
        tax_lot_id=tax_lot_id,
        instrument_id=instrument_id,
        event_time=event_time,
        uk_date=uk_date,
        event_type=event_type,
        direction=direction,
        quantity=quantity,
        cost_or_proceeds=cost_or_proceeds,
        amount_gbp=amount_gbp,
        tax_year=tax_year,
        reference_fill_id=reference_fill_id,
    )


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
# TestSameDayMatching
# ===========================================================================


class TestSameDayMatching:
    def test_single_buy_single_sell_same_day(self) -> None:
        # Buy 100 @ 10 GBP, Sell 100 @ 15 GBP same day -> gain = 500
        t = datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC)
        buy = _lot(
            tax_lot_id=1,
            event_time=t,
            direction="acquisition",
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=t + timedelta(hours=2),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            reference_fill_id=2,
        )
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        m = matches[0]
        assert m.matching_rule == "same_day"
        assert m.matched_units == _D("100")
        assert m.acquisition_cost_gbp == _D("1000")
        assert m.disposal_proceeds_gbp == _D("1500")
        assert m.gain_or_loss_gbp == _D("500")
        assert pool.units == _D("0")

    def test_partial_same_day_match(self) -> None:
        # Buy 50 @ 10 GBP each, Sell 100 @ 15 GBP each same day
        # 50 matched same-day, 50 to S104 (but no pool -> unmatched warning)
        t = datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC)
        buy = _lot(
            tax_lot_id=1,
            event_time=t,
            quantity=_D("50"),
            amount_gbp=_D("500"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=t + timedelta(hours=2),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            reference_fill_id=2,
        )
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        assert matches[0].matching_rule == "same_day"
        assert matches[0].matched_units == _D("50")
        # 50 units unmatched (no pool, no 30-day buys)
        assert pool.units == _D("0")

    def test_multiple_buys_same_day(self) -> None:
        # Buy 30 @ cost 300 GBP, Buy 70 @ cost 840 GBP, Sell 100 @ 1500 GBP
        t = datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC)
        b1 = _lot(
            tax_lot_id=1,
            event_time=t,
            quantity=_D("30"),
            amount_gbp=_D("300"),
        )
        b2 = _lot(
            tax_lot_id=2,
            event_time=t + timedelta(hours=1),
            quantity=_D("70"),
            amount_gbp=_D("840"),
            reference_fill_id=2,
        )
        sell = _lot(
            tax_lot_id=3,
            event_time=t + timedelta(hours=3),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            reference_fill_id=3,
        )
        matches, _ = _match_disposals_for_instrument([b1, b2], [sell])
        assert len(matches) == 2
        assert all(m.matching_rule == "same_day" for m in matches)
        total_cost = sum(m.acquisition_cost_gbp for m in matches)
        total_proceeds = sum(m.disposal_proceeds_gbp for m in matches)
        assert total_cost == _D("1140")
        assert total_proceeds == _D("1500")

    def test_no_same_day_buys_skips_rule(self) -> None:
        # Buy yesterday, sell today -> no same-day match, goes to S104 pool
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 2, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            reference_fill_id=2,
        )
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        assert matches[0].matching_rule == "s104_pool"
        assert pool.units == _D("0")


# ===========================================================================
# TestBedAndBreakfast
# ===========================================================================


class TestBedAndBreakfast:
    def test_buy_within_30_days_after_sell(self) -> None:
        # Sell 100 @ 15 GBP, Buy 100 @ 12 GBP 5 days later
        sell = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
        )
        buy = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 6, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("100"),
            amount_gbp=_D("1200"),
            reference_fill_id=2,
        )
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        m = matches[0]
        assert m.matching_rule == "bed_and_breakfast"
        assert m.matched_units == _D("100")
        # Cost is at the 30-day acquisition price, not pool
        assert m.acquisition_cost_gbp == _D("1200")
        assert m.gain_or_loss_gbp == _D("300")
        # Buy was consumed by B&B, pool gets nothing from it
        assert pool.units == _D("0")

    def test_buy_on_day_30_is_included(self) -> None:
        sell = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("50"),
            amount_gbp=_D("750"),
        )
        buy = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 31, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("50"),
            amount_gbp=_D("500"),
            reference_fill_id=2,
        )
        matches, _ = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        assert matches[0].matching_rule == "bed_and_breakfast"

    def test_buy_on_day_31_is_excluded(self) -> None:
        sell = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("50"),
            amount_gbp=_D("750"),
        )
        buy = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 8, 1, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("50"),
            amount_gbp=_D("500"),
            reference_fill_id=2,
        )
        matches, _ = _match_disposals_for_instrument([buy], [sell])
        # Day 31 = not in 30-day window; no pool either (buy is after sell)
        # The buy enters pool after disposal processing but disposal already happened
        assert len(matches) == 0 or all(m.matching_rule != "bed_and_breakfast" for m in matches)

    def test_fifo_within_30_day_window(self) -> None:
        # Sell 100, Buy 60 @ 11/unit on day 5, Buy 60 @ 13/unit on day 10
        sell = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
        )
        b1 = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 6, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("60"),
            amount_gbp=_D("660"),
            reference_fill_id=2,
        )
        b2 = _lot(
            tax_lot_id=3,
            event_time=datetime(2025, 7, 11, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("60"),
            amount_gbp=_D("780"),
            reference_fill_id=3,
        )
        matches, pool = _match_disposals_for_instrument([b1, b2], [sell])
        assert len(matches) == 2
        assert all(m.matching_rule == "bed_and_breakfast" for m in matches)
        # First match: 60 units at 11/unit cost
        assert matches[0].matched_units == _D("60")
        assert matches[0].acquisition_cost_gbp == _D("660")
        # Second match: 40 units at 13/unit cost
        assert matches[1].matched_units == _D("40")
        # 40 * (780/60) = 40 * 13 = 520
        assert matches[1].acquisition_cost_gbp == _D("520")
        # Remaining 20 units from b2 enter pool
        assert pool.units == _D("20")

    def test_30_day_overrides_s104_pool(self) -> None:
        # Pool has 100 @ 10 avg from old buy
        # Sell 50 @ 15, Buy 50 @ 14 on day 3
        # Should match at 14 (bed_and_breakfast), not pool avg of 10
        old_buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("50"),
            amount_gbp=_D("750"),
            reference_fill_id=2,
        )
        new_buy = _lot(
            tax_lot_id=3,
            event_time=datetime(2025, 7, 4, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("50"),
            amount_gbp=_D("700"),
            reference_fill_id=3,
        )
        matches, pool = _match_disposals_for_instrument([old_buy, new_buy], [sell])
        assert len(matches) == 1
        m = matches[0]
        assert m.matching_rule == "bed_and_breakfast"
        assert m.acquisition_cost_gbp == _D("700")
        assert m.gain_or_loss_gbp == _D("50")
        # Pool: old_buy (100 @ 10) still in pool, new_buy consumed by B&B
        assert pool.units == _D("100")

    def test_partial_30_day_consumption_then_pool_entry(self) -> None:
        """A 30-day acquisition partially consumed by B&B rule;
        remaining units enter the S104 pool for a later disposal."""
        # Day 1: Buy 100 @ 10 GBP each (enters pool for disposal on day 50)
        old_buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        # Day 30: Sell 40 @ 15 GBP each
        sell1 = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("40"),
            amount_gbp=_D("600"),
            reference_fill_id=2,
        )
        # Day 35 (+5 from sell1): Buy 100 @ 12 GBP each
        # B&B claims 40 units from this buy for sell1
        future_buy = _lot(
            tax_lot_id=3,
            event_time=datetime(2025, 7, 6, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("100"),
            amount_gbp=_D("1200"),
            reference_fill_id=3,
        )
        # Day 60: Sell 80 @ 18 GBP each
        # Should use pool: old_buy (100 @ 10) + remaining future_buy (60 @ 12)
        sell2 = _lot(
            tax_lot_id=4,
            event_time=datetime(2025, 8, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("80"),
            amount_gbp=_D("1440"),
            reference_fill_id=4,
        )

        matches, pool = _match_disposals_for_instrument([old_buy, future_buy], [sell1, sell2])

        # sell1: 40 units matched via B&B at cost 12/unit = 480 GBP
        bnb_matches = [m for m in matches if m.matching_rule == "bed_and_breakfast"]
        assert len(bnb_matches) == 1
        assert bnb_matches[0].matched_units == _D("40")
        assert bnb_matches[0].acquisition_cost_gbp == _D("480")

        # sell2: 80 units from pool
        # Pool at time of sell2: old_buy 100 @ 10 + remaining future_buy 60 @ 12
        # Pool: 160 units, cost = 1000 + 720 = 1720, avg = 10.75
        pool_matches = [m for m in matches if m.matching_rule == "s104_pool"]
        assert len(pool_matches) == 1
        assert pool_matches[0].matched_units == _D("80")
        # 80 * (1720/160) = 80 * 10.75 = 860
        assert pool_matches[0].acquisition_cost_gbp == _D("860")

        # Remaining pool: 160 - 80 = 80 units
        assert pool.units == _D("80")


# ===========================================================================
# TestS104Pool
# ===========================================================================


class TestS104Pool:
    def test_single_buy_then_sell_uses_pool_avg(self) -> None:
        # Buy 100 @ 10 GBP, Sell 50 @ 15 GBP (next day)
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 2, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("50"),
            amount_gbp=_D("750"),
            reference_fill_id=2,
        )
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        m = matches[0]
        assert m.matching_rule == "s104_pool"
        assert m.matched_units == _D("50")
        # 50 * (1000/100) = 50 * 10 = 500
        assert m.acquisition_cost_gbp == _D("500")
        assert m.gain_or_loss_gbp == _D("250")
        assert pool.units == _D("50")

    def test_multiple_buys_pool_avg_recomputed(self) -> None:
        # Buy 100 @ 10, Buy 100 @ 20 -> pool avg 15
        # Sell 100 @ 18 -> gain = 100 * (18 - 15) = 300
        b1 = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        b2 = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("2000"),
            reference_fill_id=2,
        )
        sell = _lot(
            tax_lot_id=3,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1800"),
            reference_fill_id=3,
        )
        matches, pool = _match_disposals_for_instrument([b1, b2], [sell])
        assert len(matches) == 1
        assert matches[0].matching_rule == "s104_pool"
        # 100 * (3000/200) = 100 * 15 = 1500
        assert matches[0].acquisition_cost_gbp == _D("1500")
        assert matches[0].gain_or_loss_gbp == _D("300")
        assert pool.units == _D("100")

    def test_pool_depleted_to_zero(self) -> None:
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 2, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            reference_fill_id=2,
        )
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert pool.units == _D("0")
        assert pool.cost_gbp == _D("0")

    def test_pool_avg_unchanged_after_partial_sell(self) -> None:
        # Buy 200 @ 10 GBP each = pool: 200 units, cost 2000, avg 10
        # Sell 50 @ 15 -> pool: 150 units, cost 1500, avg still 10
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("200"),
            amount_gbp=_D("2000"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 2, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("50"),
            amount_gbp=_D("750"),
            reference_fill_id=2,
        )
        _, pool = _match_disposals_for_instrument([buy], [sell])
        assert pool.units == _D("150")
        assert pool.cost_gbp == _D("1500")
        assert pool.avg_cost_gbp == _D("10")


# ===========================================================================
# TestMixedDisposal — acceptance criterion
# ===========================================================================


class TestMixedDisposal:
    def test_disposal_uses_all_three_rules(self) -> None:
        """Acceptance test: a single disposal matched across same-day,
        bed-and-breakfast, and Section 104 pool rules."""
        # Day 1: Buy 100 @ 10 GBP each (will enter pool)
        old_buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
        )
        # Day 50: Sell 150 @ 20 GBP each
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 20, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("150"),
            amount_gbp=_D("3000"),
            reference_fill_id=2,
        )
        # Day 50: Buy 30 @ 18 GBP each (same day)
        same_day_buy = _lot(
            tax_lot_id=3,
            event_time=datetime(2025, 7, 20, 14, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("30"),
            amount_gbp=_D("540"),
            reference_fill_id=3,
        )
        # Day 55: Buy 50 @ 19 GBP each (30-day window)
        bnb_buy = _lot(
            tax_lot_id=4,
            event_time=datetime(2025, 7, 25, 10, 0, 0, tzinfo=UTC),
            direction="acquisition",
            quantity=_D("50"),
            amount_gbp=_D("950"),
            reference_fill_id=4,
        )

        matches, pool = _match_disposals_for_instrument([old_buy, same_day_buy, bnb_buy], [sell])

        assert len(matches) == 3
        by_rule = {m.matching_rule: m for m in matches}

        # Same-day: 30 units @ cost 18 -> gain = 30*(20-18) = 60
        sd = by_rule["same_day"]
        assert sd.matched_units == _D("30")
        assert sd.acquisition_cost_gbp == _D("540")
        assert sd.disposal_proceeds_gbp == _D("600")  # 30 * 20
        assert sd.gain_or_loss_gbp == _D("60")

        # B&B: 50 units @ cost 19 -> gain = 50*(20-19) = 50
        bnb = by_rule["bed_and_breakfast"]
        assert bnb.matched_units == _D("50")
        assert bnb.acquisition_cost_gbp == _D("950")
        assert bnb.disposal_proceeds_gbp == _D("1000")  # 50 * 20
        assert bnb.gain_or_loss_gbp == _D("50")

        # S104: 70 units @ pool avg 10 -> gain = 70*(20-10) = 700
        s104 = by_rule["s104_pool"]
        assert s104.matched_units == _D("70")
        assert s104.acquisition_cost_gbp == _D("700")
        assert s104.disposal_proceeds_gbp == _D("1400")  # 70 * 20
        assert s104.gain_or_loss_gbp == _D("700")

        # Total gain = 60 + 50 + 700 = 810
        total = sum(m.gain_or_loss_gbp for m in matches)
        assert total == _D("810")

        # Pool: old_buy had 100, 70 consumed by S104 -> 30 remaining
        assert pool.units == _D("30")


# ===========================================================================
# TestMultiYear
# ===========================================================================


class TestMultiYear:
    def test_gains_attributed_to_disposal_year(self) -> None:
        # Buy in 2024/25, Sell in 2025/26
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
            tax_year="2024/25",
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            tax_year="2025/26",
            reference_fill_id=2,
        )
        matches, _ = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        assert matches[0].matching_rule == "s104_pool"
        # Gain attributed to disposal's tax year
        assert matches[0].disposal_lot.tax_year == "2025/26"

    def test_s104_pool_carries_across_tax_years(self) -> None:
        # Buy 100 in 2024/25, Buy 100 in 2025/26, Sell 150 in 2025/26
        b1 = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("1000"),
            tax_year="2024/25",
        )
        b2 = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            amount_gbp=_D("2000"),
            tax_year="2025/26",
            reference_fill_id=2,
        )
        sell = _lot(
            tax_lot_id=3,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("150"),
            amount_gbp=_D("3000"),
            tax_year="2025/26",
            reference_fill_id=3,
        )
        matches, pool = _match_disposals_for_instrument([b1, b2], [sell])
        assert len(matches) == 1
        m = matches[0]
        assert m.matching_rule == "s104_pool"
        # Pool avg: (1000+2000)/200 = 15 per unit
        # 150 * 15 = 2250 cost
        assert m.acquisition_cost_gbp == _D("2250")
        assert m.gain_or_loss_gbp == _D("750")
        assert pool.units == _D("50")


# ===========================================================================
# TestFeeHandling
# ===========================================================================


class TestFeeHandling:
    def test_acquisition_cost_includes_fees_in_gbp(self) -> None:
        # gross=900, fees=100 -> cost_or_proceeds = 1000
        # FX rate 0.8 -> amount_gbp = 800
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("100"),
            cost_or_proceeds=_D("1000"),
            amount_gbp=_D("800"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 2, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            cost_or_proceeds=_D("1400"),
            amount_gbp=_D("1120"),
            reference_fill_id=2,
        )
        matches, _ = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        # Cost uses the GBP amount which already includes fees
        assert matches[0].acquisition_cost_gbp == _D("800")
        assert matches[0].disposal_proceeds_gbp == _D("1120")
        assert matches[0].gain_or_loss_gbp == _D("320")


# ===========================================================================
# TestEdgeCases
# ===========================================================================


class TestEdgeCases:
    def test_no_disposals_produces_empty_matches(self) -> None:
        buy = _lot(tax_lot_id=1, quantity=_D("100"), amount_gbp=_D("1000"))
        matches, pool = _match_disposals_for_instrument([buy], [])
        assert matches == []
        assert pool.units == _D("100")

    def test_no_acquisitions_before_disposal_warns(self) -> None:
        sell = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
        )
        matches, pool = _match_disposals_for_instrument([], [sell])
        # No acquisitions -> no matches possible, warning logged
        assert matches == []
        assert pool.units == _D("0")

    def test_disposal_larger_than_all_acquisitions(self) -> None:
        buy = _lot(
            tax_lot_id=1,
            event_time=datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
            quantity=_D("50"),
            amount_gbp=_D("500"),
        )
        sell = _lot(
            tax_lot_id=2,
            event_time=datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
            direction="disposal",
            event_type="EXIT",
            quantity=_D("100"),
            amount_gbp=_D("1500"),
            reference_fill_id=2,
        )
        # Should match 50 from pool, warn about remaining 50
        matches, pool = _match_disposals_for_instrument([buy], [sell])
        assert len(matches) == 1
        assert matches[0].matched_units == _D("50")
        assert pool.units == _D("0")


# ===========================================================================
# TestIngestion — DB interaction (mocked cursors)
# ===========================================================================

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


class TestIngestion:
    @patch("app.services.tax_ledger._utcnow", return_value=_NOW)
    def test_fills_ingested_with_fx_conversion(self, _mock: MagicMock) -> None:
        # Cursor 1: un-ingested fills query
        fill_rows = [
            {
                "fill_id": 1,
                "filled_at": datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
                "price": _D("10"),
                "units": _D("100"),
                "gross_amount": _D("1000"),
                "fees": _D("5"),
                "instrument_id": 42,
                "action": "BUY",
                "instrument_currency": "USD",
            }
        ]
        fills_cursor = _make_cursor(fill_rows)

        # Cursor 2: FX rate lookup
        fx_cursor = _make_cursor([{"rate": _D("0.80")}])

        # Cursor 3: count of total fill tax_lots (after write)
        count_cursor = _make_cursor([{"cnt": 1}])

        conn = _make_conn([fills_cursor, fx_cursor, count_cursor])

        result = ingest_tax_events(conn)

        assert result.fills_ingested == 1
        assert result.already_present == 0
        # Verify the INSERT was called
        assert conn.execute.call_count >= 1

    @patch("app.services.tax_ledger._utcnow", return_value=_NOW)
    def test_missing_fx_rate_raises(self, _mock: MagicMock) -> None:
        fill_rows = [
            {
                "fill_id": 1,
                "filled_at": datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
                "price": _D("10"),
                "units": _D("100"),
                "gross_amount": _D("1000"),
                "fees": _D("5"),
                "instrument_id": 42,
                "action": "BUY",
                "instrument_currency": "USD",
            }
        ]
        fills_cursor = _make_cursor(fill_rows)
        # FX rate not found
        fx_cursor = _make_cursor([])
        fx_cursor.fetchone.return_value = None

        conn = _make_conn([fills_cursor, fx_cursor])

        with pytest.raises(RuntimeError, match="Missing FX rate"):
            ingest_tax_events(conn)


# ===========================================================================
# TestMatchingPersistence — DB interaction
# ===========================================================================


class TestMatchingPersistence:
    @patch("app.services.tax_ledger._utcnow", return_value=_NOW)
    def test_matches_written_to_db(self, _mock: MagicMock) -> None:
        # Cursor: load tax_lots
        lots = [
            {
                "tax_lot_id": 1,
                "instrument_id": 42,
                "event_time": datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC),
                "event_type": "BUY",
                "direction": "acquisition",
                "quantity": _D("100"),
                "cost_or_proceeds": _D("1000"),
                "amount_gbp": _D("800"),
                "tax_year": "2025/26",
                "reference_fill_id": 1,
            },
            {
                "tax_lot_id": 2,
                "instrument_id": 42,
                "event_time": datetime(2025, 7, 1, 10, 0, 0, tzinfo=UTC),
                "event_type": "EXIT",
                "direction": "disposal",
                "quantity": _D("100"),
                "cost_or_proceeds": _D("1500"),
                "amount_gbp": _D("1200"),
                "tax_year": "2025/26",
                "reference_fill_id": 2,
            },
        ]
        lots_cursor = _make_cursor(lots)
        conn = _make_conn([lots_cursor])

        result = run_disposal_matching(conn, instrument_id=42)

        assert result.instruments_processed == 1
        assert result.matches_created == 1
        # DELETE + INSERT match + UPSERT pool = at least 3 executes
        assert conn.execute.call_count >= 3


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
