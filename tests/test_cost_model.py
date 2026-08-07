"""Phase 5b — the static cost model, as pure logic.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.1, acceptance
C2. The calibration figures and their drift against the live ``quotes``
snapshot live in ``scripts/verify_2240_cost_model.py`` — a hand-copied
statistic goes stale in the place a reader trusts most.

⚠ DB-free by design. ``cost_model`` reads no database; it is a frozen table and
four functions over it.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.cost_model import (
    BANDS,
    CARRY_BPS,
    CARRY_UNMODELLED,
    COST_MODEL_ID,
    FX_BPS,
    PriceBand,
    _check_bands_are_total,
    _check_half_spread,
    band_for,
    buy_price,
    half_spread_for,
    sell_price,
)

#: ⚠⚠ THE FROZEN TABLE, RESTATED HERE AS LITERALS AND NOT IMPORTED.
#:
#: Acceptance C2(c): *"the band table is … pinned by test"*. A test that
#: imported ``BANDS`` and compared it to itself is a tautology — the exact
#: defect the S-3 run hit (prevention log, #2240 S-3: *"a reference that IMPORTS
#: the constant it validates"*). These literals are the second copy, on purpose,
#: so a recalibration cannot land without a test failing and a human deciding
#: whether the ``cost_model_id`` should have moved with it.
SPEC_BANDS: tuple[tuple[str, str | None, str | None, str, int], ...] = (
    ("<$5", None, "5", "1.450", 76),
    ("$5-20", "5", "20", "0.571", 244),
    ("$20-100", "20", "100", "0.509", 625),
    (">=$100", "100", None, "0.322", 210),
)

SPEC_COST_MODEL_ID = "static-p75-insession-v1"


class TestTheFrozenTable:
    def test_the_cost_model_id_is_the_frozen_one(self) -> None:
        """⚠ A recalibration is a NEW id (§5.1), and a new id moves every
        strategy version. This assertion is what makes that a deliberate act."""
        assert COST_MODEL_ID == SPEC_COST_MODEL_ID

    def test_every_band_matches_the_frozen_calibration(self) -> None:
        assert len(BANDS) == len(SPEC_BANDS)
        for band, (label, lower, upper, p75, n) in zip(BANDS, SPEC_BANDS, strict=True):
            assert band.label == label
            assert band.lower == (None if lower is None else Decimal(lower))
            assert band.upper == (None if upper is None else Decimal(upper))
            assert band.p75_spread_pct == Decimal(p75)
            assert band.sample_size == n

    def test_the_half_spread_is_half_the_round_trip(self) -> None:
        for band in BANDS:
            assert band.half_spread_pct == band.p75_spread_pct / 2
            assert band.half_spread == band.p75_spread_pct / 200

    def test_the_bands_get_cheaper_as_the_price_rises(self) -> None:
        """Not a law of nature — a property of THIS calibration, asserted so a
        recalibration that inverts it is noticed rather than shipped. A penny
        stock costing less to trade than a $200 one would mean the measurement
        was of something other than liquidity."""
        spreads = [band.p75_spread_pct for band in BANDS]
        assert spreads == sorted(spreads, reverse=True)


class TestBandLookup:
    @pytest.mark.parametrize(
        ("price", "label"),
        [
            ("0.01", "<$5"),
            ("4.999999", "<$5"),
            ("5", "$5-20"),  # lower bound INCLUSIVE
            ("19.999999", "$5-20"),
            ("20", "$20-100"),
            ("99.999999", "$20-100"),
            ("100", ">=$100"),
            ("100000", ">=$100"),
        ],
    )
    def test_a_price_lands_in_the_band_that_claims_it(self, price: str, label: str) -> None:
        assert band_for(Decimal(price)).label == label

    @pytest.mark.parametrize("price", ["0", "-1", "-0.000001"])
    def test_a_non_positive_price_has_no_band(self, price: str) -> None:
        """⚠ It RAISES rather than defaulting to the cheapest or the dearest
        band. Both fills already require ``> 0``, so arriving here with one
        means the caller assembled a position from something that is not a
        price — and a silent default would price that trade anyway."""
        with pytest.raises(ValueError, match="must be > 0"):
            band_for(Decimal(price))

    def test_half_spread_for_reads_the_entry_price_band(self) -> None:
        assert half_spread_for(Decimal("50")) == Decimal("0.509") / 200


class TestTheBandTableIsTotal:
    """``_check_bands_are_total`` runs at IMPORT, so these exercise it directly
    against tables the module would refuse."""

    def _band(self, label: str, lower: str | None, upper: str | None) -> PriceBand:
        return PriceBand(
            label=label,
            lower=None if lower is None else Decimal(lower),
            upper=None if upper is None else Decimal(upper),
            p75_spread_pct=Decimal("1"),
            sample_size=10,
        )

    def test_a_gap_between_two_bands_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="not contiguous"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("b", "6", None)))

    def test_an_overlap_between_two_bands_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="not contiguous"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("b", "4", None)))

    def test_the_lowest_band_must_be_open_below(self) -> None:
        with pytest.raises(ValueError, match="open below"):
            _check_bands_are_total((self._band("a", "1", "5"), self._band("b", "5", None)))

    def test_the_highest_band_must_be_open_above(self) -> None:
        with pytest.raises(ValueError, match="open above"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("b", "5", "100")))

    def test_an_empty_table_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="no price bands"):
            _check_bands_are_total(())

    def test_a_duplicate_label_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate band label"):
            _check_bands_are_total((self._band("a", None, "5"), self._band("a", "5", None)))

    def test_the_shipped_table_passes_its_own_check(self) -> None:
        _check_bands_are_total(BANDS)


class TestBandInvariants:
    def _band(self, **overrides: object) -> PriceBand:
        fields: dict[str, object] = {
            "label": "b",
            "lower": Decimal("5"),
            "upper": Decimal("20"),
            "p75_spread_pct": Decimal("0.5"),
            "sample_size": 10,
        }
        fields.update(overrides)
        return PriceBand(**fields)  # type: ignore[arg-type]

    def test_a_zero_cost_band_is_rejected(self) -> None:
        """Criterion 2's *"honest model, not a fictional one"* — a band charging
        nothing is a free trade, which is the assumption the criterion exists to
        remove."""
        with pytest.raises(ValueError, match="must be > 0"):
            self._band(p75_spread_pct=Decimal("0"))

    def test_a_band_with_no_sample_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="sample_size"):
            self._band(sample_size=0)

    def test_an_inverted_band_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="does not exceed"):
            self._band(lower=Decimal("20"), upper=Decimal("5"))

    def test_a_non_positive_lower_bound_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="lower bound must be > 0"):
            self._band(lower=Decimal("0"))


class TestTheArithmetic:
    """§5.1 verbatim: *"a buy fills at ``fill_price × (1 + h)`` and a sell at
    ``fill_price × (1 − h)``"*."""

    def test_a_buy_pays_the_half_spread(self) -> None:
        assert buy_price(Decimal("100"), half_spread=Decimal("0.01")) == Decimal("101")

    def test_a_sell_pays_the_half_spread(self) -> None:
        assert sell_price(Decimal("100"), half_spread=Decimal("0.01")) == Decimal("99")

    def test_the_two_sides_are_symmetric_about_the_gross_price(self) -> None:
        gross, h = Decimal("37.5"), Decimal("0.0025")
        assert buy_price(gross, half_spread=h) - gross == gross - sell_price(gross, half_spread=h)

    def test_a_round_trip_at_an_unchanged_price_loses_money(self) -> None:
        """⚠ THE PROPERTY THE WHOLE STAGE EXISTS FOR. A flat trade is a losing
        trade once the spread is charged, and S-1's median hold is ONE BAR
        (measured, phase 5a), so this is not a rounding effect for it."""
        gross, h = Decimal("100"), half_spread_for(Decimal("100"))
        assert sell_price(gross, half_spread=h) < buy_price(gross, half_spread=h)

    @pytest.mark.parametrize("half_spread", ["0", "-0.01"])
    def test_a_non_positive_half_spread_is_rejected(self, half_spread: str) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            _check_half_spread(Decimal(half_spread))

    @pytest.mark.parametrize("half_spread", ["1", "1.5"])
    def test_a_half_spread_at_or_above_one_is_rejected(self, half_spread: str) -> None:
        """⚠ At ``h >= 1`` the sell side is zero or negative. A return computed
        from it is not a loss — it is nonsense that still divides cleanly and
        would be averaged into a result."""
        with pytest.raises(ValueError, match="must be < 1"):
            _check_half_spread(Decimal(half_spread))

    def test_the_price_functions_enforce_the_same_bound(self) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            buy_price(Decimal("100"), half_spread=Decimal("0"))
        with pytest.raises(ValueError, match="must be < 1"):
            sell_price(Decimal("100"), half_spread=Decimal("1"))


class TestCarryIsNullNotZero:
    """§5.1: *"Writing zero would be the #2286 shape: a value that is present
    and wrong beats a value that is absent and refused."*"""

    def test_carry_is_none(self) -> None:
        assert CARRY_BPS is None

    def test_fx_is_none(self) -> None:
        assert FX_BPS is None

    def test_the_unmodelled_marker_is_set(self) -> None:
        """⚠ The marker the promotion gate refuses on (§6 clause 4). It is
        DERIVED from the two constants above, so measuring carry flips it
        without anybody remembering to."""
        assert CARRY_UNMODELLED is True
