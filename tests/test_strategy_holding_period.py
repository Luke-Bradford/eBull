"""#2623 gap 1 — the holding-period statistic ("expected turnaround").

Spec: ``docs/proposals/ta/2026-08-14-strategy-holding-period.md``.

Pure cases only. The ledger round-trip, the recreated view and the `sql/347`
CHECKs live in ``test_strategy_holding_period_db.py`` — ``tests/conftest.py``
marks a whole MODULE ``db`` when its source touches psycopg, so co-locating them
would drop every case below off the fast push gate.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.strategy_statistics import METRIC_SET_ID, TradeReturns, _hold_percentiles


def _trades(pairs: list[tuple[date, date]]) -> TradeReturns:
    return TradeReturns(
        net_return_pct=tuple(1.0 for _ in pairs),
        entry_fill_date=tuple(entry for entry, _ in pairs),
        exit_bar_date=tuple(exit_bar for _, exit_bar in pairs),
        open_count=0,
        unpriced_count=0,
    )


class TestHoldDays:
    def test_duration_is_calendar_days_not_trading_bars(self) -> None:
        # Fri 2026-08-07 -> Mon 2026-08-10 is ONE trading bar and THREE calendar
        # days. The spec picks calendar days deliberately (§3.1), so a weekend
        # is the case that distinguishes the two units.
        trades = _trades([(date(2026, 8, 7), date(2026, 8, 10))])
        assert trades.hold_days == (3,)

    def test_a_same_day_close_holds_for_zero_days(self) -> None:
        assert _trades([(date(2026, 8, 10), date(2026, 8, 10))]).hold_days == (0,)


class TestTradeReturnsInvariants:
    def test_exit_axis_is_required_not_defaulted(self) -> None:
        # Same argument the existing `entry_fill_date` comment makes: a default
        # would let a caller silently produce a metric set with no holding
        # period and no error anywhere.
        with pytest.raises(TypeError, match="exit_bar_date"):
            TradeReturns(  # type: ignore[call-arg]
                net_return_pct=(1.0,),
                entry_fill_date=(date(2026, 8, 10),),
                open_count=0,
                unpriced_count=0,
            )

    def test_exit_axis_must_be_parallel_to_the_entry_axis(self) -> None:
        with pytest.raises(ValueError, match="positionally parallel"):
            TradeReturns(
                net_return_pct=(1.0, 2.0),
                entry_fill_date=(date(2026, 8, 10), date(2026, 8, 11)),
                exit_bar_date=(date(2026, 8, 12),),
                open_count=0,
                unpriced_count=0,
            )

    def test_an_exit_before_its_own_entry_is_refused(self) -> None:
        # A producer bug. Left through, it contributes a negative duration that
        # averages into a median which still looks entirely plausible.
        with pytest.raises(ValueError, match="exits .* before it enters"):
            _trades([(date(2026, 8, 10), date(2026, 8, 9))])

    def test_the_refusal_names_the_offending_trade(self) -> None:
        with pytest.raises(ValueError, match="trade 2 exits"):
            _trades(
                [
                    (date(2026, 8, 3), date(2026, 8, 4)),
                    (date(2026, 8, 3), date(2026, 8, 5)),
                    (date(2026, 8, 10), date(2026, 8, 9)),
                ]
            )


class TestPercentiles:
    def test_no_realised_trades_gives_three_nulls(self) -> None:
        assert _hold_percentiles(()) == (None, None, None)

    def test_a_single_trade_collapses_all_three(self) -> None:
        assert _hold_percentiles((7,)) == (7.0, 7.0, 7.0)

    def test_odd_sized_population(self) -> None:
        # [1,2,3,4,5]: p25 = 2, median = 3, p75 = 4 under linear interpolation.
        assert _hold_percentiles((1, 2, 3, 4, 5)) == (2.0, 3.0, 4.0)

    def test_even_sized_population_interpolates_rather_than_picking_a_rank(self) -> None:
        # [1,2,3,4]: linear gives p25 = 1.75, median = 2.5, p75 = 3.25. A
        # nearest-rank method would return whole numbers here, so this is the
        # case that pins the method (§3.2).
        assert _hold_percentiles((1, 2, 3, 4)) == (1.75, 2.5, 3.25)

    def test_duplicates_do_not_disturb_the_ordering(self) -> None:
        p25, median, p75 = _hold_percentiles((5, 5, 5, 5, 5))
        assert (p25, median, p75) == (5.0, 5.0, 5.0)

    def test_input_order_does_not_matter(self) -> None:
        assert _hold_percentiles((5, 1, 4, 2, 3)) == _hold_percentiles((1, 2, 3, 4, 5))

    def test_zero_holds_are_a_real_measurement_not_an_absence(self) -> None:
        # Every trade closing same-day is a legitimate 0, and must not read as
        # "not measured" — which is why the null case is keyed on an EMPTY
        # population rather than on a falsy median.
        assert _hold_percentiles((0, 0, 0)) == (0.0, 0.0, 0.0)


class TestMetricSetId:
    def test_bumped_to_v2_because_the_metric_set_gained_members(self) -> None:
        # A version denotes a RULE SET, not a row population (#2670): a row
        # carrying metrics `criterion7-v1` never defined cannot keep that stamp.
        # ⚠ This is also what makes a null holding period readable — `sql/347`
        # requires the triple on a v2 row with realised trades, so a legacy null
        # and a writer defect are distinguishable.
        assert METRIC_SET_ID == "criterion7-v2"
