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

import pytest

from app.services.equity_curve import SIZING_RULE_ID, LegBook, build_equity_curve

#: §5.4's declared v1 rule, transcribed from
#: ``docs/proposals/ta/2026-08-07-bounded-backtester.md``.
SPEC_SIZING_RULE = "equal_weight_concurrent_v1"


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
