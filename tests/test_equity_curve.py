"""Phase 5d — the sleeve equity curve and its sizing rule.

Pure tier: no database. ``build_equity_curve`` reads no series and resolves no
fill by design (§2.1), so every property below is expressible on a handful of
hand-built legs.

⚠ THE SPEC LITERALS ARE RESTATED, NOT IMPORTED — the #2240 S-3 lesson, *"a
reference that IMPORTS the constant it validates is a tautology"*. One bridge
test asserts the module agrees with them.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from app.services.equity_curve import (
    BENCHMARK_RULE_ID,
    CAPPED_TARGET_EXPOSURE_RULE_ID,
    ENTRY_WEIGHT_DRIFT_RULE_ID,
    MONTH_END_REBALANCE_RULE_ID,
    SIZING_RULE_ID,
    LegBook,
    build_buy_and_hold_curve,
    build_capped_target_exposure_curve,
    build_entry_weight_drift_curve,
    build_equity_curve,
    build_month_end_rebalanced_curve,
)

#: §5.4's declared v1 rule, transcribed from
#: ``docs/proposals/ta/2026-08-07-bounded-backtester.md``.
SPEC_SIZING_RULE = "equal_weight_concurrent_v1"

#: #2430's frozen research arm. It is deliberately not production evidence.
SPEC_ENTRY_WEIGHT_DRIFT_RULE = "entry_weight_drift_v1"
SPEC_MONTH_END_REBALANCE_RULE = "calendar_month_end_equal_weight_v1"
SPEC_CAPPED_TARGET_EXPOSURE_RULE = "capped_target_exposure_after_decision_close_v1"

#: #2426's benchmark rule, transcribed from
#: ``docs/proposals/ta/2026-08-08-buy-and-hold-benchmark.md`` §2.4.
SPEC_BENCHMARK_RULE = "equal_weight_buy_and_hold_v1"


def _leg(
    book: LegBook,
    *,
    entry: int,
    exit_: int,
    entry_price: float,
    exit_price: float,
    marks: list[float] | None = None,
    half_spread: float = 0.0,
    realised: bool = True,
) -> None:
    """Add one leg, defaulting the marks to a straight line between the fills.

    ⚠ The default is a CONVENIENCE for tests about cash flow, never a model: a
    test about marking behaviour passes its own array.
    """
    span = exit_ - entry + 1
    if marks is None:
        if span == 1:
            marks = [exit_price]
        else:
            step = (exit_price - entry_price) / (span - 1)
            marks = [entry_price + step * i for i in range(span)]
    book.add(
        entry_index=entry,
        exit_index=exit_,
        entry_price=entry_price,
        exit_price=exit_price,
        half_spread=half_spread,
        realised=realised,
        marks=marks,
    )


class TestSpecConstants:
    def test_the_sizing_rule_id_is_the_declared_one(self) -> None:
        assert SIZING_RULE_ID == SPEC_SIZING_RULE

    def test_the_drift_arm_has_a_distinct_declared_identity(self) -> None:
        assert ENTRY_WEIGHT_DRIFT_RULE_ID == SPEC_ENTRY_WEIGHT_DRIFT_RULE
        assert ENTRY_WEIGHT_DRIFT_RULE_ID != SIZING_RULE_ID

    def test_the_month_end_arm_has_a_distinct_declared_identity(self) -> None:
        assert MONTH_END_REBALANCE_RULE_ID == SPEC_MONTH_END_REBALANCE_RULE
        assert MONTH_END_REBALANCE_RULE_ID not in {SIZING_RULE_ID, ENTRY_WEIGHT_DRIFT_RULE_ID}

    def test_the_capped_exposure_engine_has_a_distinct_declared_identity(self) -> None:
        assert CAPPED_TARGET_EXPOSURE_RULE_ID == SPEC_CAPPED_TARGET_EXPOSURE_RULE
        assert CAPPED_TARGET_EXPOSURE_RULE_ID not in {
            SIZING_RULE_ID,
            ENTRY_WEIGHT_DRIFT_RULE_ID,
            MONTH_END_REBALANCE_RULE_ID,
        }


class TestLegBookRefuses:
    """⚠ THE BOOK RAISES. It is a writer-side shape, like ``StrategyResult`` and
    unlike ``check_promotable``: a caller assembling a malformed leg has a bug."""

    def test_a_close_before_its_open_is_refused(self) -> None:
        with pytest.raises(ValueError, match="before it opens"):
            LegBook().add(
                entry_index=5,
                exit_index=3,
                entry_price=10.0,
                exit_price=11.0,
                half_spread=0.0,
                realised=True,
                marks=[10.0],
            )

    def test_a_short_mark_array_is_refused_rather_than_silently_shortening_the_hold(self) -> None:
        """⚠ THE ONE THAT MATTERS. ``offset = mark_offset + (day - entry_index)``
        is unbounded in the inner loop for speed, so a short array would read the
        NEXT leg's marks — a valuation off another instrument's prices, which no
        aggregate would reveal."""
        with pytest.raises(ValueError, match="needs 4 marks, got 2"):
            LegBook().add(
                entry_index=0,
                exit_index=3,
                entry_price=10.0,
                exit_price=11.0,
                half_spread=0.0,
                realised=True,
                marks=[10.0, 11.0],
            )

    @pytest.mark.parametrize(("entry", "exit_"), [(0.0, 5.0), (5.0, 0.0), (-1.0, 5.0)])
    def test_a_non_positive_price_is_refused(self, entry: float, exit_: float) -> None:
        with pytest.raises(ValueError, match="prices must be positive"):
            LegBook().add(
                entry_index=0,
                exit_index=0,
                entry_price=entry,
                exit_price=exit_,
                half_spread=0.0,
                realised=True,
                marks=[1.0],
            )

    def test_a_negative_half_spread_is_refused(self) -> None:
        """A negative half-spread is a cost that IMPROVES a trade, which
        ``position_costing`` refuses one layer up and which would show here as a
        rebalance that manufactured cash."""
        with pytest.raises(ValueError, match="half_spread must be non-negative"):
            LegBook().add(
                entry_index=0,
                exit_index=0,
                entry_price=1.0,
                exit_price=1.0,
                half_spread=-0.01,
                realised=True,
                marks=[1.0],
            )


class TestAxisRefusals:
    def test_a_leg_past_the_end_of_the_axis_is_refused(self) -> None:
        book = LegBook()
        _leg(book, entry=0, exit_=9, entry_price=10.0, exit_price=12.0)
        with pytest.raises(ValueError, match="the axis is short"):
            build_equity_curve(book, date_count=5)

    def test_an_empty_axis_is_refused(self) -> None:
        with pytest.raises(ValueError, match="date_count must be"):
            build_equity_curve(LegBook(), date_count=0)

    def test_a_non_positive_starting_equity_is_refused(self) -> None:
        with pytest.raises(ValueError, match="starting_equity must be positive"):
            build_equity_curve(LegBook(), date_count=3, starting_equity=0.0)


class TestSingleLegConservation:
    """⚠⚠ THE CROSS-LAYER TEST. With ONE leg and no rebalance to pay for, the
    sleeve's total return must EQUAL that position's ``net_return_pct`` from
    ``position_costing`` — the two layers compute the same quantity by different
    routes, and a disagreement means one of them is wrong."""

    def test_one_leg_returns_exactly_its_own_net_return(self) -> None:
        book = LegBook()
        _leg(book, entry=1, exit_=4, entry_price=100.0, exit_price=125.0)
        curve = build_equity_curve(book, date_count=6, starting_equity=1.0)
        # position_costing's arithmetic, restated: (exit_net - entry_net) / entry_net.
        expected = (125.0 - 100.0) / 100.0
        assert curve.equity[-1] == pytest.approx(1.0 + expected)
        assert curve.rebalance_costs == 0.0

    def test_the_pot_is_idle_before_the_entry_and_after_the_exit(self) -> None:
        """§5.4: the denominator is the FULL allocated pot and cash earns 0, so
        the curve is flat outside the hold rather than undefined."""
        book = LegBook()
        _leg(book, entry=2, exit_=3, entry_price=100.0, exit_price=110.0)
        curve = build_equity_curve(book, date_count=6)
        assert curve.equity[0] == pytest.approx(1.0)
        assert curve.equity[1] == pytest.approx(1.0)
        assert curve.equity[4] == curve.equity[5] == pytest.approx(1.1)
        assert list(curve.open_count) == [0, 0, 1, 0, 0, 0]

    def test_a_same_bar_leg_is_legal(self) -> None:
        """``sql/256`` records ``bars_held = 0`` as legal — a tp/sl can be
        touched on the fill bar itself — so a leg that opens and closes on one
        date must price, not raise."""
        book = LegBook()
        _leg(book, entry=1, exit_=1, entry_price=50.0, exit_price=55.0)
        curve = build_equity_curve(book, date_count=3)
        assert curve.equity[-1] == pytest.approx(1.1)


class TestOrderWithinADate:
    def test_an_exit_frees_cash_before_a_same_date_entry_uses_it(self) -> None:
        """§3.2 rule 4 — *"exit before entry"*. Reversing the two would fund the
        entry out of cash the exit had not released, which shows up as a
        spurious short-funded entry rather than as an error."""
        book = LegBook()
        _leg(book, entry=0, exit_=2, entry_price=100.0, exit_price=100.0)
        _leg(book, entry=2, exit_=4, entry_price=100.0, exit_price=100.0)
        curve = build_equity_curve(book, date_count=5)
        assert curve.short_funded_entries == 0
        assert curve.equity[-1] == pytest.approx(1.0)


class TestLegOrderIndependence:
    def test_a_book_in_instrument_order_gives_the_same_curve_as_one_in_date_order(self) -> None:
        """⚠ ``build_positions`` emits per instrument, so the concatenated book
        is in INSTRUMENT order, not date order. A loop that assumed otherwise
        would open legs late and no aggregate would say so."""
        date_order = LegBook()
        _leg(date_order, entry=0, exit_=2, entry_price=10.0, exit_price=12.0)
        _leg(date_order, entry=1, exit_=3, entry_price=20.0, exit_price=19.0)

        instrument_order = LegBook()
        _leg(instrument_order, entry=1, exit_=3, entry_price=20.0, exit_price=19.0)
        _leg(instrument_order, entry=0, exit_=2, entry_price=10.0, exit_price=12.0)

        first = build_equity_curve(date_order, date_count=5)
        second = build_equity_curve(instrument_order, date_count=5)
        assert list(first.equity) == pytest.approx(list(second.equity))


class TestRebalance:
    def test_equal_weight_is_imposed_on_an_event_date(self) -> None:
        """Two legs open on the same date, so each must hold half the pot at
        that date's close."""
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 4)
        _leg(book, entry=0, exit_=3, entry_price=50.0, exit_price=50.0, marks=[50.0] * 4)
        curve = build_equity_curve(book, date_count=4)
        assert curve.invested[0] == pytest.approx(1.0)
        assert curve.open_count[0] == 2
        assert curve.event_dates == 2  # the shared open date and the shared close date

    def test_entries_on_the_SAME_date_are_sized_against_the_whole_basket(self) -> None:
        """⚠⚠ THE DENOMINATOR COUNTS TODAY'S WHOLE BASKET, not one entry at a
        time. Both entries are decided at the same instant on the same bar, so an
        allocator sizing today's basket knows how many names it is opening.
        Sizing them sequentially gives the FIRST 100% of a flat pot and reports
        every sibling as short-funded — an artefact of the loop, not a capital
        constraint, and it lands in criterion 9's census as a narrowing that
        never happened.
        """
        book = LegBook()
        for _ in range(4):
            _leg(book, entry=0, exit_=2, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3)
        curve = build_equity_curve(book, date_count=3)
        assert curve.short_funded_entries == 0
        assert curve.invested[0] == pytest.approx(1.0)

    def test_weights_DRIFT_between_event_dates_and_are_not_restored_daily(self) -> None:
        """⚠⚠ THE DISCRIMINATOR for §5.4's *"rebalanced ONLY on position
        open/close"*. Two legs open together; one doubles while the other is
        flat, over dates on which nothing opens or closes. A daily-rebalanced
        engine would trade every day and charge for it; this one must not.

        The pot ends at ``0.5 * 2.0 + 0.5 * 1.0 = 1.5``, which is the
        buy-and-hold answer, and a rebalancing engine gives something else.
        """
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=200.0, marks=[100.0, 133.0, 166.0, 200.0])
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 4)
        curve = build_equity_curve(book, date_count=4, starting_equity=1.0)
        assert curve.equity[-1] == pytest.approx(1.5)
        # Only the shared open and the shared close are events.
        assert curve.event_dates == 2

    def test_a_rebalance_charges_the_half_spread_and_costs_never_add_equity(self) -> None:
        """Criterion 2 applied to the trades the SIZING RULE creates. A
        rebalance that cost nothing would be a free trade, and a negative cost
        would manufacture equity."""
        book = LegBook()
        _leg(book, entry=0, exit_=5, entry_price=100.0, exit_price=100.0, marks=[100.0] * 6, half_spread=0.01)
        # A second leg opening later forces a rebalance of the first.
        _leg(book, entry=2, exit_=5, entry_price=100.0, exit_price=100.0, marks=[100.0] * 4, half_spread=0.01)
        curve = build_equity_curve(book, date_count=6)
        assert curve.rebalance_costs > 0.0
        assert curve.equity[-1] < 1.0

    def test_BOTH_sides_of_a_rebalance_are_charged(self) -> None:
        """⚠⚠ THE ONE THE LOOSE TEST ABOVE DOES NOT PIN. "rebalance_costs > 0"
        passes with either side free, because the other side still charges — a
        revert probe caught exactly that. The bounds below are derived from the
        stated rule (sell first at ``h``, then buy what cash allows), not from
        the implementation:

        Leg A takes the whole flat pot on date 0. Leg B opens on date 1, so that
        date's close rebalance sells ``0.5`` out of A — costing ``0.5h`` — and
        then buys ``(0.5 - 0.5h) / (1 + h)`` into B, costing less than another
        ``0.5h`` because the buy is capped by the cash the sale actually raised.

        - a free SELL side leaves only the buy charge, which is BELOW ``0.5h``;
        - a free BUY side leaves exactly ``0.5h``;
        - both charged is strictly between ``0.5h`` and ``1.0h``.

        So one strict two-sided bound discriminates all three.
        """
        half = 0.01
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 4, half_spread=half)
        _leg(book, entry=1, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3, half_spread=half)
        curve = build_equity_curve(book, date_count=4)
        assert 0.5 * half < curve.rebalance_costs < 1.0 * half

    def test_cash_never_goes_negative_which_is_what_sells_before_buys_buys(self) -> None:
        """⚠ A single-pass rebalance to ``equity / n`` leaves cash at MINUS the
        cost it just charged — arithmetically small, and leverage, which the
        project posture forbids outright. Selling first and capping buys at cash
        on hand makes ``cash >= 0`` hold by construction.

        ``equity - invested`` IS the cash, so asserting it never goes negative
        asserts exactly that.
        """
        book = LegBook()
        for entry in range(6):
            _leg(
                book,
                entry=entry,
                exit_=9,
                entry_price=100.0,
                exit_price=100.0,
                marks=[100.0] * (10 - entry),
                half_spread=0.02,
            )
        curve = build_equity_curve(book, date_count=10)
        cash = curve.equity - curve.invested
        assert min(cash) >= -1e-12, f"cash went to {min(cash)} — the rebalance borrowed"


class TestEntryWeightDriftArm:
    def test_simultaneous_entries_start_with_the_same_weights_as_production(self) -> None:
        book = LegBook()
        _leg(book, entry=0, exit_=2, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3)
        _leg(book, entry=0, exit_=2, entry_price=50.0, exit_price=50.0, marks=[50.0] * 3)
        production = build_equity_curve(book, date_count=3)
        drift = build_entry_weight_drift_curve(book, date_count=3)
        assert drift.equity.tolist() == pytest.approx(production.equity.tolist())
        assert drift.short_funded_entries == 0

    def test_a_later_entry_cannot_be_funded_by_selling_an_existing_winner(self) -> None:
        """The exact #2430 discriminator: production trims A at B's entry event;
        the drift arm cannot spend that synthetic sale and reports B as short
        funded. Signals, fills, marks and exit prices are otherwise identical."""
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=200.0, marks=[100.0, 200.0, 200.0, 200.0])
        _leg(book, entry=1, exit_=3, entry_price=100.0, exit_price=200.0, marks=[100.0, 150.0, 200.0])
        production = build_equity_curve(book, date_count=4)
        drift = build_entry_weight_drift_curve(book, date_count=4)
        assert drift.rebalance_costs == 0.0
        assert drift.short_funded_entries == 1
        assert drift.traded_notional.sum() < production.traded_notional.sum()
        assert drift.equity[-1] < production.equity[-1]


class TestMonthEndRebalanceArm:
    def test_it_defers_equalisation_from_an_entry_event_to_month_end(self) -> None:
        dates = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 31), date(2024, 2, 1))
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 4, half_spread=0.01)
        _leg(book, entry=1, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3, half_spread=0.01)
        production = build_equity_curve(book, date_count=len(dates))
        monthly = build_month_end_rebalanced_curve(book, dates=dates)
        assert production.traded_notional[1] > 0.0
        assert monthly.traded_notional[1] == 0.0
        assert monthly.traded_notional[2] > 0.0
        assert monthly.rebalance_costs > 0.0

    def test_it_does_not_invent_a_month_end_at_a_truncated_window_boundary(self) -> None:
        dates = (date(2024, 7, 1), date(2024, 7, 8))
        book = LegBook()
        _leg(
            book,
            entry=0,
            exit_=1,
            entry_price=100.0,
            exit_price=100.0,
            marks=[100.0] * 2,
            realised=False,
        )
        _leg(book, entry=1, exit_=1, entry_price=100.0, exit_price=100.0, marks=[100.0])
        monthly = build_month_end_rebalanced_curve(book, dates=dates)
        assert monthly.rebalance_costs == 0.0
        assert monthly.short_funded_entries == 1

    def test_it_refuses_an_unordered_calendar(self) -> None:
        with pytest.raises(ValueError, match="strictly increasing"):
            build_month_end_rebalanced_curve(
                LegBook(),
                dates=(date(2024, 1, 3), date(2024, 1, 2)),
            )


class TestHalts:
    def test_a_missing_bar_carries_the_previous_mark_forward_and_is_counted(self) -> None:
        """§3.3 — a halted name *"stays open to the next date on which its own
        series has a bar"*. ⚠ The gap is NOT interpolated: a fabricated bar is
        an invented observation, which is worse than a stale one."""
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0, math.nan, math.nan, 100.0])
        curve = build_equity_curve(book, date_count=4)
        assert curve.stale_marks == 2
        assert curve.equity[1] == pytest.approx(1.0)
        assert curve.equity[2] == pytest.approx(1.0)


class TestOpenAtWindowEnd:
    """⚠⚠ AN UNREALISED LEG IS NOT AN EXIT. Caught at Codex checkpoint 2: the
    engine bucketed it with the realised closes, liquidated it at its mark bar,
    and handed the notional to cash. §3.2 rule 5 gives it *"an unrealised mark"*
    and §3.4 keeps it IN exposure and ON the curve — there is no sale."""

    def test_an_open_position_is_marked_not_dropped(self) -> None:
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=100.0, exit_price=140.0, realised=False)
        curve = build_equity_curve(book, date_count=4)
        assert curve.equity[-1] == pytest.approx(1.4)
        assert curve.open_count[2] == 1

    def test_it_stays_OPEN_and_INVESTED_past_its_mark_bar(self) -> None:
        """Its instrument's last usable bar is date 1, and the window runs to
        date 3. ⚠ Liquidating it there would report ``open_count`` and
        ``invested`` as ZERO for the rest of the window — the bias toward
        positions that CLOSED that rule 5 exists to prevent, arriving through
        the exposure metric instead of through the win rate."""
        book = LegBook()
        _leg(book, entry=0, exit_=1, entry_price=100.0, exit_price=120.0, realised=False)
        curve = build_equity_curve(book, date_count=4)
        assert list(curve.open_count) == [1, 1, 1, 1]
        assert curve.invested[-1] == pytest.approx(1.2)
        assert curve.unrealised_held == 1

    def test_a_frozen_leg_is_NOT_traded_by_a_later_rebalance(self) -> None:
        """⚠⚠ A FROZEN LEG HAS NO BAR TO TRADE ON. Leg A's instrument stops at
        date 1; leg B opens on date 2, which is an event date. Including A in
        that rebalance's target sells half of it — a sale on a date its series
        does not reach — and hands the proceeds to B. Equalising only the
        TRADEABLE sleeve leaves A alone, so the rebalance charges nothing.
        """
        book = LegBook()
        _leg(
            book,
            entry=0,
            exit_=1,
            entry_price=100.0,
            exit_price=100.0,
            marks=[100.0, 100.0],
            half_spread=0.01,
            realised=False,
        )
        _leg(book, entry=2, exit_=4, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3, half_spread=0.01)
        curve = build_equity_curve(book, date_count=5)
        assert curve.rebalance_costs == 0.0
        assert curve.equity[-1] == pytest.approx(1.0)
        assert curve.unrealised_held == 1

    def test_its_notional_CANNOT_fund_a_same_date_entry(self) -> None:
        """⚠⚠ THE DISCRIMINATOR. Liquidating the unrealised leg puts its whole
        notional into cash, and a position opening that date then buys with
        money nobody received. Frozen, the entry is short-funded — which is the
        truth, because the capital is still committed to a name that cannot be
        sold.
        """
        book = LegBook()
        _leg(book, entry=0, exit_=2, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3, realised=False)
        _leg(book, entry=2, exit_=3, entry_price=100.0, exit_price=100.0, marks=[100.0] * 2)
        curve = build_equity_curve(book, date_count=4)
        assert curve.short_funded_entries == 1
        assert curve.open_count[-1] == 1
        assert curve.unrealised_held == 1


class TestExposureInputs:
    def test_invested_is_zero_on_an_idle_axis_and_the_pot_is_not(self) -> None:
        """§5.4: exposure divides invested capital-days by ALLOCATED capital-days,
        so an idle sleeve must report a live pot and no investment — not a zero
        denominator."""
        curve = build_equity_curve(LegBook(), date_count=4, starting_equity=2.0)
        assert list(curve.invested) == [0.0, 0.0, 0.0, 0.0]
        assert list(curve.equity) == [2.0, 2.0, 2.0, 2.0]
        assert curve.event_dates == 0


class TestBuyAndHoldComposition:
    """#2426 — the benchmark's own rule, whose defining property is not trading.

    ⚠ The bug these guard is not a wrong number, it is a wrong COMPOSITION: the
    benchmark used to be built by ``build_equity_curve`` and so inherited a
    sizing rule that rebalances. Every assertion below is about the absence of
    that rebalance, because a rebalanced comparator is not buy-and-hold (Blume &
    Stambaugh, JFE 12, 1983, 387-404).
    """

    def test_the_benchmark_rule_matches_the_spec_literal(self) -> None:
        assert BENCHMARK_RULE_ID == SPEC_BENCHMARK_RULE

    def test_it_is_not_the_sizing_rule(self) -> None:
        """⚠ The whole of #2426 in one line — they were the same id by omission."""
        assert BENCHMARK_RULE_ID != SIZING_RULE_ID

    def test_a_divergent_winner_is_never_trimmed_back_to_equal_weight(self) -> None:
        """⚠⚠ THE DEFECT ITSELF. Two legs, one 10x and one flat, and a third leg
        opening late so an event date falls between. ``build_equity_curve``
        re-equalises on that date and books the winner's gain into the loser;
        ``build_buy_and_hold_curve`` must not.
        """
        book = LegBook()
        _leg(book, entry=0, exit_=4, entry_price=1.0, exit_price=10.0, marks=[1.0, 4.0, 7.0, 10.0, 10.0])
        _leg(book, entry=0, exit_=4, entry_price=1.0, exit_price=1.0, marks=[1.0, 1.0, 1.0, 1.0, 1.0])
        _leg(book, entry=2, exit_=4, entry_price=1.0, exit_price=1.0, marks=[1.0, 1.0, 1.0])

        held = build_buy_and_hold_curve(book, date_count=5)
        rebalanced = build_equity_curve(book, date_count=5)

        # Each leg took exactly 1/3 and kept its own outcome: (10 + 1 + 1) / 3.
        assert held.equity[-1] == pytest.approx((10.0 + 1.0 + 1.0) / 3.0)
        assert rebalanced.equity[-1] != pytest.approx(held.equity[-1])

    def test_total_return_is_exactly_the_mean_gross_multiple(self) -> None:
        """⚠ An ALGEBRAIC identity, not a tolerance: with no rebalancing the pot
        is n independent 1/n sleeves, so the composition cannot be
        path-dependent. It is the assertion the full-population arm repeats.
        """
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=2.0, exit_price=5.0)
        _leg(book, entry=1, exit_=3, entry_price=4.0, exit_price=3.0)
        _leg(book, entry=2, exit_=3, entry_price=1.0, exit_price=1.5)

        curve = build_buy_and_hold_curve(book, date_count=4)
        expected = (5.0 / 2.0 + 3.0 / 4.0 + 1.5 / 1.0) / 3.0
        assert curve.equity[-1] == pytest.approx(expected, rel=1e-12)

    def test_it_charges_no_rebalance_cost_even_at_a_wide_spread(self) -> None:
        """⚠ The entry/exit round trip is already inside the two prices; a
        rebalance cost here would be a SECOND charge for a trade never made.
        """
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=1.0, exit_price=2.0, half_spread=0.25)
        _leg(book, entry=1, exit_=3, entry_price=1.0, exit_price=0.5, half_spread=0.25)
        curve = build_buy_and_hold_curve(book, date_count=4)
        assert curve.rebalance_costs == 0.0
        assert curve.short_funded_entries == 0

    def test_traded_notional_is_entries_plus_exits_and_nothing_else(self) -> None:
        book = LegBook()
        _leg(book, entry=0, exit_=2, entry_price=1.0, exit_price=3.0)
        _leg(book, entry=0, exit_=2, entry_price=1.0, exit_price=1.0)
        curve = build_buy_and_hold_curve(book, date_count=3)
        # 0.5 in per leg; out at 0.5 * 3.0 and 0.5 * 1.0.
        assert float(curve.traded_notional.sum()) == pytest.approx(0.5 + 0.5 + 1.5 + 0.5)

    def test_cash_is_never_negative_however_the_legs_are_priced(self) -> None:
        """⚠ Stronger than the strategy curve's cap and for a structural reason:
        total commitment is exactly ``n * (1/n)``, so no leg can be short-funded.
        """
        book = LegBook()
        for entry in range(5):
            _leg(book, entry=entry, exit_=5, entry_price=0.01, exit_price=100.0)
        curve = build_buy_and_hold_curve(book, date_count=6)
        cash = curve.equity - curve.invested
        assert float(cash.min()) >= -1e-12

    def test_an_unrealised_leg_is_refused_rather_than_guessed_at(self) -> None:
        """⚠ The strategy engine FREEZES such a leg to exclude it from the
        rebalance. With no rebalance there is nothing to exclude it from, so
        inventing a treatment would price a position nobody could sell.
        """
        book = LegBook()
        _leg(book, entry=0, exit_=2, entry_price=1.0, exit_price=2.0, realised=False)
        with pytest.raises(ValueError, match="unrealised"):
            build_buy_and_hold_curve(book, date_count=3)

    def test_an_empty_book_holds_the_pot_flat(self) -> None:
        curve = build_buy_and_hold_curve(LegBook(), date_count=4)
        assert list(curve.equity) == [1.0, 1.0, 1.0, 1.0]
        assert curve.event_dates == 0

    def test_a_halt_carries_the_previous_mark_and_is_counted(self) -> None:
        """§3.3 — a missing bar is not a return, and the carry-forward is
        reported. Matches ``build_equity_curve``'s treatment deliberately."""
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=1.0, exit_price=2.0, marks=[1.0, math.nan, math.nan, 2.0])
        curve = build_buy_and_hold_curve(book, date_count=4)
        assert curve.stale_marks == 2
        assert curve.equity[1] == pytest.approx(curve.equity[0])

    def test_event_dates_stay_truthful_rather_than_being_zeroed(self) -> None:
        """⚠ It counts concurrency CHANGES (criterion 8), whose meaning does not
        depend on whether a rebalance followed. Zeroing it to advertise "no
        rebalancing" would destroy a different measurement."""
        book = LegBook()
        _leg(book, entry=0, exit_=3, entry_price=1.0, exit_price=1.0)
        _leg(book, entry=2, exit_=3, entry_price=1.0, exit_price=1.0)
        curve = build_buy_and_hold_curve(book, date_count=4)
        # Opens on 0 and 2, both close on 3.
        assert curve.event_dates == 3


class TestCappedTargetExposureCurve:
    DATES = (
        date(2020, 1, 2),
        date(2020, 1, 3),
        date(2020, 1, 6),
        date(2020, 1, 7),
        date(2020, 1, 8),
    )

    @staticmethod
    def _rising_book(*, half_spread: float = 0.0, realised: bool = True) -> LegBook:
        book = LegBook()
        _leg(
            book,
            entry=0,
            exit_=4,
            entry_price=100.0,
            exit_price=146.41,
            marks=[100.0, 110.0, 121.0, 133.1, 146.41],
            half_spread=half_spread,
            realised=realised,
        )
        return book

    def test_a_new_target_applies_only_after_the_decision_bars_return(self) -> None:
        curve = build_capped_target_exposure_curve(
            self._rising_book(),
            dates=self.DATES,
            target_exposure_by_date={self.DATES[0]: 0.5, self.DATES[2]: 0.25},
        )

        # The first target trades after day 0's mark. Two subsequent 10% source
        # moves therefore add 5% each; day 2's lower target only affects day 3.
        assert list(curve.equity[:4]) == pytest.approx([1.0, 1.05, 1.105, 1.132625])
        assert list(curve.invested[:4]) == pytest.approx([0.5, 0.55, 0.27625, 0.303875])
        assert list(curve.traded_notional[:4]) == pytest.approx([0.5, 0.0, 0.32875, 0.0])

    def test_exposure_changes_charge_each_holding_spread_and_preserve_cash(self) -> None:
        book = LegBook()
        _leg(
            book,
            entry=0,
            exit_=2,
            entry_price=100.0,
            exit_price=100.0,
            marks=[100.0, 100.0, 100.0],
            half_spread=0.01,
        )
        curve = build_capped_target_exposure_curve(
            book,
            dates=self.DATES[:3],
            target_exposure_by_date={self.DATES[0]: 0.5, self.DATES[1]: 0.0},
        )

        assert list(curve.equity) == pytest.approx([0.995, 0.99, 0.99])
        assert list(curve.traded_notional) == pytest.approx([0.5, 0.5, 0.0])
        assert curve.rebalance_costs == pytest.approx(0.01)
        assert min(curve.equity - curve.invested) >= 0.0

    def test_intramonth_source_events_keep_the_last_target_and_net_once(self) -> None:
        book = LegBook()
        _leg(book, entry=0, exit_=4, entry_price=100.0, exit_price=100.0, marks=[100.0] * 5)
        _leg(book, entry=2, exit_=4, entry_price=100.0, exit_price=100.0, marks=[100.0] * 3)

        curve = build_capped_target_exposure_curve(
            book,
            dates=self.DATES,
            target_exposure_by_date={self.DATES[0]: 0.5},
        )

        assert curve.equity[2] == pytest.approx(1.0)
        assert curve.invested[2] == pytest.approx(0.5)
        # 0.25 buys the new leg and 0.25 sells the old one: the event is one
        # netted uniform rebalance under the unchanged 50% aggregate target.
        assert curve.traded_notional[2] == pytest.approx(0.5)

    @pytest.mark.parametrize(
        ("schedule", "message"),
        [
            ({}, "schedule is empty"),
            ({date(2019, 12, 31): 0.5}, "outside the curve date axis"),
            ({DATES[0]: -0.01}, "in \\[0, 1\\]"),
            ({DATES[0]: 1.01}, "in \\[0, 1\\]"),
            ({DATES[0]: math.nan}, "finite"),
            ({DATES[0]: True}, "not boolean"),
        ],
    )
    def test_invalid_or_levered_schedules_refuse(self, schedule: dict[date, float], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            build_capped_target_exposure_curve(
                self._rising_book(),
                dates=self.DATES,
                target_exposure_by_date=schedule,
            )

    def test_an_untradeable_frozen_leg_refuses_uniform_scaling(self) -> None:
        with pytest.raises(ValueError, match="untradeable frozen leg"):
            build_capped_target_exposure_curve(
                self._rising_book(realised=False),
                dates=self.DATES,
                target_exposure_by_date={self.DATES[0]: 0.5},
            )

    def test_a_non_increasing_axis_refuses(self) -> None:
        with pytest.raises(ValueError, match="strictly increasing"):
            build_capped_target_exposure_curve(
                self._rising_book(),
                dates=(self.DATES[0], self.DATES[0], *self.DATES[2:]),
                target_exposure_by_date={self.DATES[0]: 0.5},
            )
