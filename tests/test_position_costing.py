"""Phase 5b — charging the cost model against a built position.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.1, §3.2 rule 5,
§3.4; acceptance C2. Full-population figures are in
``scripts/verify_2240_cost_model.py``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.cost_model import COST_MODEL_ID, UNKNOWN_NOMINAL_PRICE_BAND, half_spread_for
from app.services.position_builder import Position
from app.services.position_costing import (
    CostedPosition,
    band_crossings,
)
from app.services.position_costing import (
    cost_position as _cost_position,
)
from app.services.position_costing import (
    cost_positions as _cost_positions,
)


def cost_position(position: Position) -> CostedPosition:
    """Synthetic fixtures use nominal, as-traded prices unless stated."""
    return _cost_position(position, price_basis="as_traded")


def cost_positions(positions: list[Position]) -> tuple[CostedPosition, ...]:
    return _cost_positions(positions, price_basis="as_traded")


def _position(**overrides: object) -> Position:
    fields: dict[str, object] = {
        "strategy_id": "s1",
        "strategy_version": "strategy-registry-v1+deadbeef",
        "instrument_id": 7,
        "entry_signal_id": 1,
        "entry_signal_bar_date": date(2024, 1, 1),
        "entry_fill_bar_date": date(2024, 1, 2),
        "entry_fill_price": Decimal("100"),
        "close_source": "signal_pair",
        "close_bar_date": date(2024, 1, 5),
        "close_price": Decimal("110"),
        "bars_held": 3,
        "open_reason": None,
        "mark_price": None,
    }
    fields.update(overrides)
    return Position(**fields)  # type: ignore[arg-type]


def _open_position(**overrides: object) -> Position:
    base: dict[str, object] = {
        "close_source": None,
        "close_bar_date": None,
        "close_price": None,
        "bars_held": None,
        "open_reason": "window_end",
        "mark_price": Decimal("110"),
    }
    base.update(overrides)
    return _position(**base)


class TestAClosedPosition:
    def test_both_sides_are_charged(self) -> None:
        """Acceptance C2(b), the closed half: the buy pays up and the sell pays
        down, and both are ADJUSTED PRICES rather than a cost subtracted from a
        return (§5.1)."""
        costed = cost_position(_position())
        h = half_spread_for(Decimal("100"))
        assert costed.entry_price_net == Decimal("100") * (1 + h)
        assert costed.exit_price_net == Decimal("110") * (1 - h)
        assert costed.exit_basis == "close"
        assert costed.uncosted_reason is None

    def test_the_net_return_is_computed_from_the_adjusted_prices(self) -> None:
        """⚠ NOT ``gross − 2h``. The half-spread is multiplicative on both the
        numerator and the denominator, so the two differ — and ``sql/256`` names
        its column GROSS precisely so nothing averages it as performance."""
        costed = cost_position(_position())
        h = half_spread_for(Decimal("100"))
        entry_net = Decimal("100") * (1 + h)
        exit_net = Decimal("110") * (1 - h)
        assert costed.net_return_pct == (exit_net - entry_net) / entry_net * 100
        assert costed.gross_return_pct == Decimal("10")

    def test_costs_never_improve_a_trade(self) -> None:
        for close in ("50", "99.9", "100", "100.1", "400"):
            costed = cost_position(_position(close_price=Decimal(close)))
            assert costed.net_return_pct is not None and costed.gross_return_pct is not None
            assert costed.net_return_pct < costed.gross_return_pct

    def test_a_small_gross_gain_becomes_a_net_loss(self) -> None:
        """⚠⚠ THE MEASUREMENT THAT MATTERS FOR S-1. Its median hold is ONE BAR
        (phase 5a, full population), so most of its trades are decided in
        exactly this region: a 0.2% gross move at the ``>=$100`` band's
        0.161% half-spread is a loss after the round trip."""
        costed = cost_position(_position(entry_fill_price=Decimal("100"), close_price=Decimal("100.2")))
        assert costed.gross_return_pct == Decimal("0.2")
        assert costed.net_return_pct is not None
        assert costed.net_return_pct < 0

    def test_the_model_id_travels_on_the_row(self) -> None:
        assert cost_position(_position()).cost_model_id == COST_MODEL_ID


class TestTheBandIsKeyedOnTheEntryPrice:
    """§5.1: *"The band is keyed on the ENTRY fill price, fixed for the life of
    the position. A position that crosses a band boundary does not re-key: the
    cost is a property of the trade, and re-keying mid-hold would make the cost
    depend on the outcome."*"""

    def test_a_position_crossing_a_boundary_keeps_its_entry_band(self) -> None:
        costed = cost_position(_position(entry_fill_price=Decimal("4"), close_price=Decimal("500")))
        assert costed.band_label == "<$5"
        assert costed.half_spread == half_spread_for(Decimal("4"))

    def test_the_exit_side_uses_the_entry_half_spread(self) -> None:
        """The one that would catch a re-key: the ``<$5`` and ``>=$100`` bands
        differ by ~4.5x, so a sell adjusted at the exit band is a visibly
        different number."""
        costed = cost_position(_position(entry_fill_price=Decimal("4"), close_price=Decimal("500")))
        h = half_spread_for(Decimal("4"))
        assert costed.exit_price_net == Decimal("500") * (1 - h)
        assert costed.exit_price_net != Decimal("500") * (1 - half_spread_for(Decimal("500")))

    def test_crossings_are_counted_not_prevented(self) -> None:
        rows = cost_positions(
            [
                _position(entry_fill_price=Decimal("4"), close_price=Decimal("500")),
                _position(entry_fill_price=Decimal("50"), close_price=Decimal("55")),
            ]
        )
        assert band_crossings(rows) == 1

    def test_an_unpriced_exit_cannot_cross(self) -> None:
        rows = cost_positions([_position(close_source="ambiguous", close_price=None)])
        assert band_crossings(rows) == 0


class TestSplitAdjustedPriceBasis:
    def test_it_uses_the_maximum_cost_band_regardless_of_adjusted_price(self) -> None:
        low = _cost_position(_position(entry_fill_price=Decimal("4")), price_basis="split_adjusted")
        high = _cost_position(_position(entry_fill_price=Decimal("500")), price_basis="split_adjusted")
        assert low.band_label == high.band_label == f"max:{UNKNOWN_NOMINAL_PRICE_BAND.label}"
        assert low.half_spread == high.half_spread == UNKNOWN_NOMINAL_PRICE_BAND.half_spread
        assert low.price_basis == high.price_basis == "split_adjusted"

    def test_adjusted_prices_do_not_claim_nominal_band_crossings(self) -> None:
        row = _cost_position(
            _position(entry_fill_price=Decimal("500"), close_price=Decimal("4")), price_basis="split_adjusted"
        )
        assert band_crossings([row]) == 0


class TestAnOpenPosition:
    def test_the_mark_is_charged_the_exit_side(self) -> None:
        """§3.2 rule 5: the mark is *"minus one side of the cost model (the exit
        that has not happened)"*. Leaving it gross would report an unrealised
        gain nobody could realise."""
        costed = cost_position(_open_position())
        h = half_spread_for(Decimal("100"))
        assert costed.exit_basis == "mark"
        assert costed.exit_price_gross == Decimal("110")
        assert costed.exit_price_net == Decimal("110") * (1 - h)

    def test_a_mark_is_distinguishable_from_a_realised_close(self) -> None:
        """⚠ 5d has to keep an open position OUT of the win rate and expectancy
        while keeping it IN exposure and on the equity curve (§3.4), which is
        impossible if the two look identical on the row."""
        assert cost_position(_open_position()).exit_basis == "mark"
        assert cost_position(_position()).exit_basis == "close"

    @pytest.mark.parametrize("reason", ["window_end", "unresolved_outcome", "close_bar_unfillable"])
    def test_every_open_reason_is_costed_the_same_way(self, reason: str) -> None:
        """These open positions have capital committed; §3.4 puts each in
        exposure. Only 5d cares WHY."""
        costed = cost_position(_open_position(open_reason=reason))
        assert costed.exit_basis == "mark"

    def test_a_series_break_position_cannot_carry_a_cross_scale_mark(self) -> None:
        with pytest.raises(ValueError, match="cannot be marked on the new price scale"):
            _open_position(open_reason="series_break")

    def test_an_open_position_with_no_mark_is_excluded_and_counted(self) -> None:
        costed = cost_position(_open_position(mark_price=None))
        assert costed.uncosted_reason == "no_mark"
        assert costed.net_return_pct is None
        assert costed.exit_basis is None

    def test_the_entry_side_is_still_charged_without_a_mark(self) -> None:
        """⚠ The entry HAPPENED. A position with no mark has an unknown return,
        not an uncommitted one — dropping its entry cost would make an
        unmarkable position look free."""
        costed = cost_position(_open_position(mark_price=None))
        assert costed.entry_price_net > Decimal("100")


class TestAnAmbiguousClose:
    """§3.4: *"the return is unknown, not zero. Recording zero is a treatment —
    it silently asserts break-even, which is favourable for a strategy whose
    ambiguous bars span a stop."*"""

    def test_it_carries_no_return(self) -> None:
        costed = cost_position(_position(close_source="ambiguous", close_price=None))
        assert costed.uncosted_reason == "ambiguous_close"
        assert costed.net_return_pct is None
        assert costed.gross_return_pct is None
        assert costed.exit_price_gross is None

    def test_it_is_not_recorded_as_break_even(self) -> None:
        costed = cost_position(_position(close_source="ambiguous", close_price=None))
        assert costed.net_return_pct != Decimal("0")

    def test_it_still_carries_a_band(self) -> None:
        """Capital was committed, so the trade has a cost basis even though it
        has no return — §3.4 keeps it IN exposure."""
        costed = cost_position(_position(close_source="ambiguous", close_price=None))
        assert costed.band_label == ">=$100"


class TestRowInvariants:
    def _costed(self, **overrides: object) -> CostedPosition:
        position = _position()
        h = half_spread_for(position.entry_fill_price)
        fields: dict[str, object] = {
            "position": position,
            "cost_model_id": COST_MODEL_ID,
            "price_basis": "as_traded",
            "band_label": ">=$100",
            "half_spread": h,
            "entry_price_net": position.entry_fill_price * (1 + h),
            "exit_price_gross": Decimal("110"),
            "exit_price_net": Decimal("110") * (1 - h),
            "exit_basis": "close",
            "gross_return_pct": Decimal("10"),
            "net_return_pct": Decimal("9"),
            "uncosted_reason": None,
        }
        fields.update(overrides)
        return CostedPosition(**fields)  # type: ignore[arg-type]

    def test_a_row_is_priced_or_unpriced_never_both(self) -> None:
        with pytest.raises(ValueError, match="both"):
            self._costed(uncosted_reason="no_mark")

    def test_a_row_is_priced_or_unpriced_never_neither(self) -> None:
        with pytest.raises(ValueError, match="neither"):
            self._costed(
                exit_basis=None,
                exit_price_gross=None,
                exit_price_net=None,
                gross_return_pct=None,
                net_return_pct=None,
            )

    def test_a_partial_costing_is_rejected(self) -> None:
        """⚠⚠ COUNTED, NOT ANDed — the 3c mirror defect (prevention log). Under
        a chain of ``is not None`` ANDs this row reads as "no exit" and is
        silently excluded from every statistic instead of raising."""
        with pytest.raises(ValueError, match="partial costing"):
            self._costed(net_return_pct=None)

    def test_a_priced_row_missing_only_its_gross_return_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="partial costing"):
            self._costed(gross_return_pct=None)

    def test_an_unpriced_row_carrying_a_stray_return_is_rejected(self) -> None:
        """The other side of the count: a row that says "no exit" while holding
        a return is the half that an ANDed guard admits."""
        with pytest.raises(ValueError, match="partial costing"):
            self._costed(
                exit_basis=None,
                uncosted_reason="ambiguous_close",
                exit_price_gross=None,
                exit_price_net=None,
                gross_return_pct=None,
            )

    def test_an_uncharged_entry_is_rejected(self) -> None:
        """Criterion 2's *"costs are non-zero on every position"*, asserted at
        the row rather than trusted from the constructor."""
        with pytest.raises(ValueError, match="does not exceed the gross fill"):
            self._costed(entry_price_net=Decimal("100"))

    def test_an_uncharged_exit_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="not below the gross exit"):
            self._costed(exit_price_net=Decimal("110"))

    def test_a_net_return_at_or_above_gross_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="costs cannot improve a trade"):
            self._costed(net_return_pct=Decimal("10"))

    def test_an_unknown_exit_basis_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown exit basis"):
            self._costed(exit_basis="guess")

    def test_an_unknown_uncosted_reason_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown uncosted reason"):
            self._costed(
                exit_basis=None,
                exit_price_gross=None,
                exit_price_net=None,
                gross_return_pct=None,
                net_return_pct=None,
                uncosted_reason="dunno",
            )

    def test_an_unknown_price_basis_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown price basis"):
            self._costed(price_basis="adjusted-ish")

    def test_a_band_label_that_disagrees_with_the_basis_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="does not match as_traded entry basis"):
            self._costed(band_label="max:<$5")

    def test_a_half_spread_that_disagrees_with_the_basis_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="does not match as_traded entry basis"):
            self._costed(half_spread=Decimal("0.01"), entry_price_net=Decimal("101"))


class TestCostPositionsPreservesOrder:
    def test_order_is_preserved(self) -> None:
        rows = cost_positions([_position(entry_signal_id=i, close_price=Decimal("110")) for i in range(5)])
        assert [row.position.entry_signal_id for row in rows] == [0, 1, 2, 3, 4]
