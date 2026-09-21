"""#2834 §7 item 2 slice B — the split-only correction, as pure logic.

No DB: the derivation is a function of stored stamps and the full-population
evidence lives in ``scripts/measure_2834_split_adjustment.py``. What is asserted
here is the arithmetic and the refusals — the parts a corpus scan cannot show
are right, only that they did not fire.
"""

from __future__ import annotations

import decimal
from decimal import Decimal

import pytest

from app.services.research_split_adjustment import (
    SPLIT_ADJUSTMENT_RULE_VERSION,
    SPLIT_CORRECTION_POLICY,
    StampsUnavailable,
    UncorrectableStamp,
    corrected_price,
    corrected_volume,
    require_correctable,
    split_scales,
)

ONE = Decimal(1)


def _factors(*values: str) -> list[Decimal | None]:
    return [Decimal(value) for value in values]


class TestTheScaleIsTheProductOfLaterFactors:
    def test_a_series_with_no_event_scales_to_one_everywhere(self) -> None:
        assert split_scales(_factors("1", "1", "1"), stamps_marker="vendor_supplied") == (ONE, ONE, ONE)

    def test_the_last_bar_always_scales_to_one(self) -> None:
        scales = split_scales(_factors("1", "4", "1"), stamps_marker="vendor_supplied")
        assert scales[-1] == ONE

    def test_the_stamped_bar_is_excluded_from_its_own_scale(self) -> None:
        # AAPL's 4:1 settles on 2020-08-31, the bar that FIRST prints the
        # post-split level. That bar is already on the new basis; the bar before
        # it is the one that needs the 4.
        closes = [Decimal("499.23"), Decimal("129.04"), Decimal("134.18")]
        scales = split_scales(_factors("1", "4", "1"), stamps_marker="vendor_supplied")
        assert scales == (Decimal(4), ONE, ONE)
        corrected = [corrected_price(close, scale) for close, scale in zip(closes, scales, strict=True)]
        assert corrected[0] == Decimal("124.8075")
        # The step the raw series carries (3.87x) is gone from the corrected one.
        assert corrected[0] is not None and corrected[1] is not None
        assert abs(corrected[0] / corrected[1] - 1) < Decimal("0.05")

    def test_several_events_compound(self) -> None:
        # Two forward splits after bar 0: its scale is the product, not the last.
        scales = split_scales(_factors("1", "7", "1", "4"), stamps_marker="vendor_supplied")
        assert scales == (Decimal(28), Decimal(4), Decimal(4), ONE)

    def test_a_reverse_split_scales_below_one(self) -> None:
        scales = split_scales(_factors("1", "0.1"), stamps_marker="vendor_supplied")
        assert scales == (Decimal("0.1"), ONE)
        # A 1:10 reverse INFLATES the pre-event level when uncorrected, which is
        # the direction `d15e680e` §3.5 showed pushes a name towards the top
        # decile. Correcting divides by 0.1, i.e. restores the ×10.
        assert corrected_price(Decimal("5"), scales[0]) == Decimal("50")

    def test_an_empty_series_has_no_scales(self) -> None:
        assert split_scales([], stamps_marker="vendor_supplied") == ()

    def test_the_first_bars_own_factor_is_never_applied_but_is_still_validated(self) -> None:
        # 33 stamps in the corpus sit on a series' first bar. They scale nothing
        # (no earlier bar exists) — but a bad one still has to fail.
        assert split_scales(_factors("4", "1"), stamps_marker="vendor_supplied") == (ONE, ONE)
        with pytest.raises(UncorrectableStamp):
            split_scales([Decimal("-1"), Decimal(1)], stamps_marker="vendor_supplied")


class TestTheMarkerIsAPreconditionNotADefault:
    @pytest.mark.parametrize("marker", ["absent", "", None, "vendor supplied", "VENDOR_SUPPLIED"])
    def test_anything_but_vendor_supplied_refuses(self, marker: str | None) -> None:
        with pytest.raises(StampsUnavailable):
            split_scales(_factors("1"), stamps_marker=marker)

    def test_absent_raises_rather_than_returning_a_neutral_scale(self) -> None:
        # The whole point: `COALESCE(split_factor, 1)` on the OTHER vendor's
        # 25.8M NULL bars would return a scale of 1 and look like a correct
        # no-op. A scale of 1 must never be REACHABLE from an absent marker.
        with pytest.raises(StampsUnavailable):
            require_correctable("absent")


class TestABadStampFailsClosed:
    def test_a_null_factor_inside_a_correctable_series_raises(self) -> None:
        with pytest.raises(UncorrectableStamp, match="marker and the bars disagree"):
            split_scales([Decimal(1), None], stamps_marker="vendor_supplied")

    @pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
    def test_a_non_finite_factor_raises_although_it_compares_above_zero(self, bad: str) -> None:
        # `Decimal("NaN") > 0` is False in Python but `'NaN'::numeric > 0` is
        # TRUE in Postgres, and `Infinity > 0` is true in both. A `> 0` test
        # alone admits at least one of them whichever side runs it.
        with pytest.raises(UncorrectableStamp, match="non-finite"):
            split_scales([Decimal(1), Decimal(bad)], stamps_marker="vendor_supplied")

    @pytest.mark.parametrize("bad", ["0", "-2"])
    def test_a_non_positive_factor_raises(self, bad: str) -> None:
        with pytest.raises(UncorrectableStamp, match="non-positive"):
            split_scales([Decimal(1), Decimal(bad)], stamps_marker="vendor_supplied")


class TestTheProductIsExactOrItRaises:
    def test_a_long_chain_of_wide_factors_stays_exact(self) -> None:
        # 58 events is the corpus maximum (measured 2026-09-21). Each factor
        # here carries 15 significant digits, so the exact product needs ~870 —
        # far past the 28-digit default context this would otherwise inherit.
        factor = Decimal("1.00000000000003")
        scales = split_scales([Decimal(1), *([factor] * 58)], stamps_marker="vendor_supplied")
        with decimal.localcontext() as context:
            context.prec = 10_000
            expected = Decimal(1)
            for _ in range(58):
                expected *= factor
        assert scales[0] == expected

    def test_the_ambient_context_cannot_round_the_product(self) -> None:
        factors: list[Decimal | None] = [Decimal("1.00000000000003")] * 20
        with decimal.localcontext() as context:
            context.prec = 5  # a caller with a hostile context
            scales = split_scales([Decimal(1), *factors], stamps_marker="vendor_supplied")
        assert len(str(scales[0]).replace(".", "").rstrip("0")) > 5


class TestPriceAndVolumeMoveOppositeWays:
    def test_close_times_volume_is_split_invariant(self) -> None:
        # `price_quarantine`'s T3 corroboration depends on this. Correcting one
        # side and not the other breaks the admit-back signal silently, which is
        # why the two functions exist as a pair.
        close, volume, scale = Decimal("499.23"), 46_907_479, Decimal(4)
        corrected_close = corrected_price(close, scale)
        corrected_vol = corrected_volume(volume, scale)
        assert corrected_close is not None and corrected_vol is not None
        assert corrected_close * corrected_vol == close * Decimal(volume)

    def test_volume_is_multiplied_not_divided(self) -> None:
        assert corrected_volume(1_000, Decimal(4)) == Decimal(4_000)

    def test_a_reverse_split_gives_a_fractional_pre_event_share_count(self) -> None:
        # 1:10 reverse — 1,000 post-split shares are 100 pre-split ones. Not
        # rounded: rounding would break the invariant above to tidy the type.
        assert corrected_volume(1_000, Decimal("0.1")) == Decimal("100.0")
        assert corrected_volume(5, Decimal("0.1")) == Decimal("0.5")

    def test_absent_levels_pass_through(self) -> None:
        # open/high/low are nullable in research_price_daily; close is not.
        assert corrected_price(None, Decimal(4)) is None
        assert corrected_volume(None, Decimal(4)) is None


class TestTheAppliersDoNotTrustTheirScale:
    """The appliers are public, so they cannot assume a `split_scales` output."""

    @pytest.mark.parametrize("bad", ["0", "-1", "NaN", "Infinity"])
    def test_a_scale_that_is_not_a_positive_finite_multiplier_raises(self, bad: str) -> None:
        for apply in (lambda s: corrected_price(Decimal(10), s), lambda s: corrected_volume(10, s)):
            with pytest.raises(UncorrectableStamp, match="positive finite multiplier"):
                apply(Decimal(bad))

    def test_a_nan_scale_does_not_propagate_silently(self) -> None:
        # Why the guard exists rather than letting the arithmetic speak, MEASURED
        # in this interpreter rather than reasoned from the float rules:
        #   * the division SUCCEEDS and returns NaN — no error at the site of
        #     the defect;
        #   * `Decimal("NaN") == 1` is False (quiet), but `Decimal("NaN") >= 1`
        #     RAISES InvalidOperation — unlike `float("nan") >= 1.0`, which is
        #     merely False.
        # So an unguarded NaN scale travels silently through the correction and
        # then explodes at whichever unrelated ORDERING comparison meets it
        # first — a MIN_CLOSE filter, a decile sort — with nothing pointing back
        # to the scale. Failing here makes the site of the defect the site of
        # the error.
        assert (Decimal(10) / Decimal("NaN")).is_nan()
        assert (Decimal("NaN") == Decimal(1)) is False
        with pytest.raises(decimal.InvalidOperation):
            _ = Decimal("NaN") >= Decimal(1)
        with pytest.raises(UncorrectableStamp):
            corrected_price(Decimal(10), Decimal("NaN"))


class TestThePolicyIsFrozenAndCarried:
    def test_the_policy_is_apply_all(self) -> None:
        assert SPLIT_CORRECTION_POLICY == "apply_all"

    def test_the_rule_version_moves_when_the_module_does(self) -> None:
        # §4 rule 11: identity is code + config + data contract. A consumer
        # hashes this in, so it has to be a function of the source rather than a
        # hand-maintained string.
        import hashlib
        from pathlib import Path

        import app.services.research_split_adjustment as module

        digest = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()[:12]
        assert SPLIT_ADJUSTMENT_RULE_VERSION == f"split-only-correction-v1+{digest}"

    def test_the_applied_rule_takes_no_adjudication_parameter(self) -> None:
        # §5's freeze is only meaningful if it is checkable, and the checkable
        # form is the SIGNATURE rather than a grep for words (the module's own
        # prose names every rejected arm, so a text scan would fail on the
        # documentation of the freeze). `apply_all` consumes stamps and a
        # coverage marker; a tolerance, a reference vendor or an adjudication
        # threshold would have to arrive as an argument, and this fails when one
        # does.
        import inspect

        parameters = inspect.signature(split_scales).parameters
        assert list(parameters) == ["factors", "stamps_marker"]
