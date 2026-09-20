"""#2840 — the confirmed-re-denomination adjustment probe's decision logic.

Pure-logic only: the register query and the provider fetch are exercised by running the probe,
not mocked here. What is pinned is every rule that a later edit could silently loosen in the
direction of this probe's own conclusion.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from scripts.probe_2840_confirmed_split_adjustment import (
    CLIFF_AT_EXPECTED,
    CLIFF_ELSEWHERE,
    INSUFFICIENT,
    MIN_LOG_RATIO,
    NO_CLIFF,
    PRICE_TOLERANCE,
    SHARE_RATIO_TOLERANCE,
    Redenomination,
    cliff_profile,
)
from scripts.probe_2840_intraday_adjustment_basis import is_split_scale


def _series(levels: list[float], *, start: date = date(2026, 1, 5)) -> dict[date, float]:
    """One close per consecutive day. Calendar realism is irrelevant to a level test."""
    return {start + timedelta(days=index): value for index, value in enumerate(levels)}


#: A bracket strictly INSIDE the 24-day series the tests build. ⚠ It used to be the whole of 2026,
#: which every one of these series fails to cover — and covering the bracket is now a
#: precondition, so the old brackets would make each test pass for the wrong reason.
BRACKET = (date(2026, 1, 10), date(2026, 1, 20))


def _event(share_ratio: float) -> Redenomination:
    pq = is_split_scale(share_ratio, limit=30, tolerance=SHARE_RATIO_TOLERANCE)
    assert pq is not None
    return Redenomination(
        symbol="TEST",
        instrument_id=1,
        share_ratio=share_ratio,
        ratio_pq=pq,
        old_filed=date(2026, 1, 1),
        new_filed=date(2026, 12, 31),
        period_end=date(2025, 12, 31),
        old_val=1_000_000.0,
        new_val=1_000_000.0 * share_ratio,
    )


class TestExpectedFactor:
    def test_shares_multiply_price_divides(self) -> None:
        # A forward 3:1 split triples the share count and thirds a NOMINAL price.
        assert _event(3.0).expected_price_factor == pytest.approx(1 / 3)

    def test_reverse_split_raises_price(self) -> None:
        assert _event(0.05).expected_price_factor == pytest.approx(20.0)

    def test_the_reciprocal_is_NOT_also_expected(self) -> None:
        """⚠ Accepting ``s`` as well as ``1/s`` would re-admit same-direction cases.

        It would also score HON — whose share count and delivered price BOTH halved — as a
        nominal delivery, when the whole point of that case is that they moved together and so
        the surviving step cannot be the split.
        """
        event = _event(0.5)
        assert event.expected_price_factor == pytest.approx(2.0)
        assert event.expected_price_factor != pytest.approx(0.5)


class TestCliffProfile:
    def test_a_nominal_series_shows_the_cliff(self) -> None:
        # 1-for-10 reverse split: shares x0.1, price x10 at the effective date.
        series = _series([2.0] * 12 + [20.0] * 12)
        profile = cliff_profile(series, expected_factor=10.0, bracket=BRACKET)
        assert profile["verdict"] == CLIFF_AT_EXPECTED

    def test_a_back_adjusted_series_shows_no_cliff(self) -> None:
        """The whole series already on the new basis — which is what back-adjustment looks like."""
        series = _series([20.0, 20.4, 19.8] * 8)
        profile = cliff_profile(series, expected_factor=10.0, bracket=BRACKET)
        assert profile["verdict"] == NO_CLIFF

    def test_a_single_corrupt_bar_is_NOT_a_cliff(self) -> None:
        """⚠ THE FALSIFIED v1 REGISTER IN ONE ASSERTION.

        2025-12-09 put one bar of 17 instruments at a tenth of both neighbours. An adjacent-close
        ratio calls that a 10:1 event; a geometric-mean level on each side does not.
        """
        levels = [20.0] * 12 + [20.0] * 12
        levels[12] = 2.0
        profile = cliff_profile(_series(levels), expected_factor=10.0, bracket=BRACKET)
        assert profile["verdict"] == NO_CLIFF

    def test_short_history_is_insufficient_not_no_cliff(self) -> None:
        """⚠ The direction that would inflate this probe's own conclusion, so it is pinned.

        An event nobody could look at must never be counted as an event with no cliff.
        """
        profile = cliff_profile(_series([20.0] * 8), expected_factor=10.0, bracket=BRACKET)
        assert profile["verdict"] == INSUFFICIENT

    def test_a_bracket_with_no_full_window_is_insufficient(self) -> None:
        series = _series([20.0] * 40, start=date(2026, 1, 5))
        # A bracket over the first three sessions can never have 10 bars on its left.
        profile = cliff_profile(series, expected_factor=10.0, bracket=(date(2026, 1, 5), date(2026, 1, 7)))
        assert profile["verdict"] == INSUFFICIENT

    def test_a_shift_outside_the_bracket_is_not_counted(self) -> None:
        """The bracket bounds where the EVENT may be; a level shift elsewhere is not this event.

        The series is long enough to COVER the later bracket, so this exercises the bracket test
        and not the coverage precondition — which would otherwise make it pass for another reason.
        """
        series = _series([2.0] * 12 + [20.0] * 48, start=date(2026, 1, 5))
        profile = cliff_profile(series, expected_factor=10.0, bracket=(date(2026, 2, 10), date(2026, 2, 20)))
        assert profile["verdict"] == NO_CLIFF

    def test_partial_bracket_coverage_is_insufficient_not_no_cliff(self) -> None:
        """⚠⚠ CODEX CHECKPOINT 2, and the finding that most inflated this probe's conclusion.

        A 1000-bar fetch against a 13-year bracket sees a flat tail and would have reported
        ``no_cliff`` for a window whose transition was never fetched at all.
        """
        series = _series([20.0] * 40, start=date(2026, 3, 1))
        profile = cliff_profile(series, expected_factor=10.0, bracket=(date(2025, 1, 1), date(2026, 4, 1)))
        assert profile["verdict"] == INSUFFICIENT
        assert "does not cover bracket" in profile["reason"]

    def test_an_intermediate_window_factor_is_not_a_match(self) -> None:
        """⚠⚠ CODEX CHECKPOINT 2 — overlapping windows manufacture every factor on the way.

        A real x10 step is seen partially by the windows either side of it, so some window reads
        x1.995. Scoring "any window matched" turned that x10 event into a NOMINAL DELIVERY at
        expected factor 2. The verdict reads the LARGEST shift, so the x10 is classified as a
        cliff at another factor — withheld from both tallies rather than counted as evidence.
        """
        series = _series([1.0] * 20 + [10.0] * 20, start=date(2026, 1, 1))
        profile = cliff_profile(series, expected_factor=2.0, bracket=(date(2026, 1, 11), date(2026, 1, 31)))
        assert profile["verdict"] == CLIFF_ELSEWHERE
        # The intermediate match is still REPORTED — it is diagnostic, it is simply not the verdict.
        assert profile["near_expected"]

    def test_the_any_cliff_bar_scales_with_the_event(self) -> None:
        """⚠ A FLAT BAR SWALLOWED THE REGISTER — 255 of 285 events landed in ``cliff_at_other``.

        A 25% level shift is ordinary for a sub-dollar small-cap. Against a 20x event it is
        nothing; against a 1.2x event it is larger than the event itself. The bar is therefore
        the event's own size, so ``no_cliff`` means "not even a shift as big as the one sought".
        """
        wobbly = _series([10.0] * 12 + [12.5] * 12)
        big_event = cliff_profile(wobbly, expected_factor=20.0, bracket=BRACKET)
        small_event = cliff_profile(wobbly, expected_factor=1.1, bracket=BRACKET)
        assert big_event["verdict"] == NO_CLIFF
        assert small_event["verdict"] == CLIFF_ELSEWHERE

    def test_the_match_band_is_the_price_tolerance(self) -> None:
        inside = _series([2.0] * 12 + [2.0 * 10.0 * (1 + PRICE_TOLERANCE / 2)] * 12)
        outside = _series([2.0] * 12 + [2.0 * 10.0 * (1 + PRICE_TOLERANCE * 4)] * 12)
        bracket = BRACKET
        assert cliff_profile(inside, expected_factor=10.0, bracket=bracket)["verdict"] == CLIFF_AT_EXPECTED
        assert cliff_profile(outside, expected_factor=10.0, bracket=bracket)["verdict"] != CLIFF_AT_EXPECTED


class TestTheRegistersAdmissionBandIsSymmetric:
    """⚠ The first cut was ``s > 1.5 OR s < 0.667`` and dropped an exact 3:2 while keeping 2:3."""

    def test_an_exact_three_for_two_is_admitted_in_both_directions(self) -> None:
        assert abs(math.log(1.5)) >= MIN_LOG_RATIO
        assert abs(math.log(2 / 3)) >= MIN_LOG_RATIO

    def test_the_band_is_a_log_distance_so_reciprocals_agree(self) -> None:
        for ratio in (1.2, 1.5, 2.0, 20.0):
            assert abs(math.log(ratio)) == pytest.approx(abs(math.log(1 / ratio)))


class TestTheShareToleranceIsTighterThanThePriceOne:
    def test_share_counts_are_integers_not_noisy_prices(self) -> None:
        assert SHARE_RATIO_TOLERANCE < PRICE_TOLERANCE

    def test_the_price_tolerance_makes_common_ratios_ambiguous_at_this_limit(self) -> None:
        """⚠ Why the share side does not reuse ``is_split_scale``'s 1% default.

        At ``limit=30`` a 1% band around 7:4 contains more than one coprime pair, so an exact and
        perfectly ordinary ratio is rejected as ambiguous. The tight band resolves it.
        """
        assert is_split_scale(7 / 4, limit=30, tolerance=0.01) is None
        assert is_split_scale(7 / 4, limit=30, tolerance=SHARE_RATIO_TOLERANCE) == (7, 4)

    def test_a_non_ratio_is_still_rejected_at_the_tight_band(self) -> None:
        assert is_split_scale(1.4142, limit=30, tolerance=SHARE_RATIO_TOLERANCE) is None
