"""Pure-logic tests for EOD equity aggregation (#1594 PR-A)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.portfolio_eod import (
    PositionInput,
    PositionResult,
    compute_eod_equity,
    resolve_snapshot_date,
    summarise_marks,
)

# 1 USD = 0.80 GBP; no EUR pair on purpose (exercises the cross-rate skip).
RATES = {("USD", "GBP"): Decimal("0.80")}


def _pos(
    pid: int,
    units: str,
    ccy: str | None,
    close: str | None,
    *,
    amount: str = "0",
    open_rate: str = "0",
    is_buy: bool = True,
    open_conversion_rate: str = "1",
    mark_price_date: date | None = None,
) -> PositionInput:
    # Defaults amount=open_rate=0 reduce MTM (amount + units*(close-open_rate))
    # to units*close — the unleveraged-long identity the FX/counter tests assert.
    # The MTM-specific tests pass realistic amount/open_rate/is_buy.
    return PositionInput(
        position_id=pid,
        instrument_id=pid * 10,
        units=Decimal(units),
        native_ccy=ccy,
        close=Decimal(close) if close is not None else None,
        amount=Decimal(amount),
        open_rate=Decimal(open_rate),
        is_buy=is_buy,
        open_conversion_rate=Decimal(open_conversion_rate),
        mark_price_date=mark_price_date,
    )


class TestResolveSnapshotDate:
    def test_picks_latest_price_date(self) -> None:
        dates = [date(2025, 6, 10), date(2025, 6, 12), date(2025, 6, 11)]
        assert resolve_snapshot_date(dates, date(2025, 1, 1)) == date(2025, 6, 12)

    def test_falls_back_when_no_prices(self) -> None:
        assert resolve_snapshot_date([], date(2025, 6, 13)) == date(2025, 6, 13)


class TestComputeEodEquity:
    def test_priced_position_usd_to_gbp(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", "USD", "10")], [], "GBP", RATES)
        # 2 * 10 USD = 20 USD → 16 GBP.
        assert eq.positions_value == Decimal("16.00")
        assert eq.total_value == Decimal("16.00")
        assert (eq.positions_priced, eq.positions_no_price, eq.positions_no_fx) == (1, 0, 0)
        assert eq.position_results[0].price_status == "priced"
        assert eq.position_results[0].value_display == Decimal("16.00")

    def test_same_currency_no_conversion(self) -> None:
        eq = compute_eod_equity([_pos(1, "3", "GBP", "5")], [], "GBP", RATES)
        assert eq.positions_value == Decimal("15")
        assert eq.positions_priced == 1

    def test_mtm_long_uses_invested_plus_gain(self) -> None:
        # Long: amount 100 invested, open_rate 10, units 5, mark 12.
        # MTM = 100 + 5*(12-10) = 110 (GBP, same ccy). NOT units*mark (=60).
        eq = compute_eod_equity([_pos(1, "5", "GBP", "12", amount="100", open_rate="10")], [], "GBP", RATES)
        assert eq.positions_value == Decimal("110")

    def test_mtm_short_profits_on_fall(self) -> None:
        # Short: amount 100, open_rate 10, units 5, mark 8 → 100 + 5*(10-8) = 110.
        eq = compute_eod_equity(
            [_pos(1, "5", "GBP", "8", amount="100", open_rate="10", is_buy=False)], [], "GBP", RATES
        )
        assert eq.positions_value == Decimal("110")

    def test_preserves_strategy_unrealised_pnl_in_broker_usd(self) -> None:
        eq = compute_eod_equity(
            [
                _pos(
                    1,
                    "5",
                    "GBP",
                    "12",
                    amount="100",
                    open_rate="10",
                    open_conversion_rate="1.25",
                )
            ],
            [],
            "GBP",
            RATES,
        )
        assert eq.position_results[0].unrealised_pnl_usd == Decimal("12.50")

    def test_missing_display_fx_does_not_discard_known_usd_pnl(self) -> None:
        eq = compute_eod_equity(
            [_pos(1, "5", "EUR", "12", amount="100", open_rate="10", open_conversion_rate="1.10")],
            [],
            "GBP",
            RATES,
        )
        assert eq.position_results[0].price_status == "no_fx"
        assert eq.position_results[0].unrealised_pnl_usd == Decimal("11.00")

    def test_mtm_equals_units_close_for_unleveraged_long(self) -> None:
        # amount == units*open_rate → MTM collapses to units*mark (the v1 case).
        eq = compute_eod_equity([_pos(1, "2", "GBP", "10", amount="16", open_rate="8")], [], "GBP", RATES)
        assert eq.positions_value == Decimal("20")  # 16 + 2*(10-8) == 2*10

    def test_no_price_position_skipped_not_zeroed(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", "USD", None)], [], "GBP", RATES)
        assert eq.positions_value == Decimal("0")
        assert eq.positions_no_price == 1
        assert eq.position_results[0].price_status == "no_price"
        assert eq.position_results[0].value_display is None

    def test_missing_fx_pair_is_no_fx(self) -> None:
        # EUR→GBP has no direct/inverse pair in RATES → skipped as no_fx.
        eq = compute_eod_equity([_pos(1, "2", "EUR", "10")], [], "GBP", RATES)
        assert eq.positions_value == Decimal("0")
        assert eq.positions_no_fx == 1
        assert eq.position_results[0].price_status == "no_fx"

    def test_null_currency_is_no_fx(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", None, "10")], [], "GBP", RATES)
        assert eq.positions_no_fx == 1
        assert eq.position_results[0].price_status == "no_fx"

    def test_closed_set_counter_invariant(self) -> None:
        positions = [
            _pos(1, "2", "USD", "10"),  # priced
            _pos(2, "1", "USD", None),  # no_price
            _pos(3, "1", "EUR", "10"),  # no_fx
        ]
        eq = compute_eod_equity(positions, [], "GBP", RATES)
        assert eq.positions_total == 3
        assert eq.positions_priced + eq.positions_no_price + eq.positions_no_fx == eq.positions_total

    def test_cash_priced_and_skipped(self) -> None:
        eq = compute_eod_equity(
            [],
            [("USD", Decimal("100")), ("EUR", Decimal("50")), (None, Decimal("5"))],
            "GBP",
            RATES,
        )
        # USD 100 → 80 GBP; EUR + NULL cash dropped (no FX).
        assert eq.cash_value == Decimal("80.00")
        assert eq.cash_no_fx_currencies == 2
        assert eq.total_value == Decimal("80.00")

    def test_total_is_positions_plus_cash(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", "USD", "10")], [("USD", Decimal("100"))], "GBP", RATES)
        assert eq.positions_value == Decimal("16.00")
        assert eq.cash_value == Decimal("80.00")
        assert eq.total_value == Decimal("96.00")


SNAPSHOT = date(2025, 6, 12)
EARLIER = date(2025, 6, 10)


def _result(status: str, mark: date | None) -> PositionResult:
    return PositionResult(
        position_id=1,
        instrument_id=10,
        units=Decimal("1"),
        native_ccy="USD",
        close=Decimal("1"),
        value_display=Decimal("1") if status == "priced" else None,
        price_status=status,
        mark_price_date=mark,
    )


class TestMarkPriceDateIsCarriedThroughEveryBranch:
    """#2602 item 4 — the aggregate cannot see a single dropped construction site.

    ``summarise_marks`` reads only the ``priced`` rows, so a test on the bound
    alone would still pass if the ``no_price`` or ``no_fx`` branches lost the
    field. There are four ``PositionResult`` construction sites and they were
    edited separately, which is exactly when a positional field goes missing
    from one of them.
    """

    def test_priced_carries_the_mark_date(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", "USD", "10", mark_price_date=SNAPSHOT)], [], "GBP", RATES)
        assert [(r.price_status, r.mark_price_date) for r in eq.position_results] == [("priced", SNAPSHOT)]

    def test_no_fx_carries_the_mark_date(self) -> None:
        # EUR has no pair in RATES, so this prices natively and then fails to convert.
        eq = compute_eod_equity([_pos(1, "2", "EUR", "10", mark_price_date=EARLIER)], [], "GBP", RATES)
        assert [(r.price_status, r.mark_price_date) for r in eq.position_results] == [("no_fx", EARLIER)]

    def test_no_currency_carries_the_mark_date(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", None, "10", mark_price_date=EARLIER)], [], "GBP", RATES)
        assert [(r.price_status, r.mark_price_date) for r in eq.position_results] == [("no_fx", EARLIER)]

    def test_no_price_carries_the_absent_mark_date(self) -> None:
        eq = compute_eod_equity([_pos(1, "2", "USD", None)], [], "GBP", RATES)
        assert [(r.price_status, r.mark_price_date) for r in eq.position_results] == [("no_price", None)]


class TestSummariseMarks:
    def test_all_marks_on_the_session_bound_to_it_with_nothing_stale(self) -> None:
        marks = summarise_marks([_result("priced", SNAPSHOT), _result("priced", SNAPSHOT)], SNAPSHOT)
        assert (marks.oldest_mark_date, marks.stale_mark_positions) == (SNAPSHOT, 0)

    def test_the_bound_is_the_OLDEST_mark_not_the_newest(self) -> None:
        # The whole point: a total stamped SNAPSHOT whose worst input is two days
        # old is a two-day-old valuation. A max() here would report it as current.
        marks = summarise_marks([_result("priced", SNAPSHOT), _result("priced", EARLIER)], SNAPSHOT)
        assert (marks.oldest_mark_date, marks.stale_mark_positions) == (EARLIER, 1)

    def test_unpriced_and_unconvertible_positions_cannot_move_the_bound(self) -> None:
        # Neither contributed to positions_value, so neither may bound it. A
        # no_fx row is the trap: it HAS a mark date, just not one that is in the
        # total, so filtering on `mark_price_date is not None` alone is wrong.
        marks = summarise_marks(
            [_result("priced", SNAPSHOT), _result("no_fx", EARLIER), _result("no_price", None)],
            SNAPSHOT,
        )
        assert (marks.oldest_mark_date, marks.stale_mark_positions) == (SNAPSHOT, 0)

    def test_no_priced_position_leaves_the_bound_absent_rather_than_guessing(self) -> None:
        marks = summarise_marks([_result("no_price", None)], SNAPSHOT)
        assert (marks.oldest_mark_date, marks.stale_mark_positions) == (None, 0)

    def test_an_all_cash_snapshot_has_no_bound(self) -> None:
        marks = summarise_marks([], SNAPSHOT)
        assert (marks.oldest_mark_date, marks.stale_mark_positions) == (None, 0)

    def test_every_priced_mark_stale_counts_all_of_them(self) -> None:
        marks = summarise_marks([_result("priced", EARLIER), _result("priced", EARLIER)], SNAPSHOT)
        assert (marks.oldest_mark_date, marks.stale_mark_positions) == (EARLIER, 2)
