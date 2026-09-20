"""#2840 — the intraday-adjustment probe's estimator and step test.

Pure-logic only, no DB and no provider call. The probe's decisions live in two functions and
one of them was already wrong once, so these tests exist to pin WHICH estimator is being used
and what the step test does and does not claim.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pytest

from scripts.probe_2840_intraday_adjustment_basis import (
    detect_step,
    is_split_scale,
    reference_window_start,
    session_ratios,
)


@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    close: float


def _session(day: date, closes: list[float]) -> list[_Bar]:
    opened = datetime(day.year, day.month, day.day, 13, 30, tzinfo=UTC)
    return [_Bar(opened + timedelta(minutes=30 * index), close) for index, close in enumerate(closes)]


class TestSessionRatios:
    def test_the_LAST_bar_is_the_default_estimator_not_the_median(self) -> None:
        """⚠ The whole reason the first version of this probe was wrong.

        A session's median bar sits somewhere inside the day's range, so on a volatile name its
        ratio against the daily close wanders by the intraday range — which was wider than the
        threshold and made the CONTROL fire.
        """
        day = date(2026, 9, 18)
        bars = _session(day, [90.0, 110.0, 100.0])
        ratios = session_ratios(bars, {day: 100.0})
        assert ratios == [(day, 1.0, 3)], "the default must read the session's final bar"

    def test_the_median_estimator_is_still_available_for_contrast(self) -> None:
        day = date(2026, 9, 18)
        bars = _session(day, [90.0, 110.0, 100.0])
        assert session_ratios(bars, {day: 100.0}, estimator="median")[0][1] == pytest.approx(1.0)
        # A skewed session shows the two estimators diverging, which is the point of keeping both.
        bars = _session(day, [50.0, 60.0, 100.0])
        assert session_ratios(bars, {day: 100.0})[0][1] == pytest.approx(1.0)
        assert session_ratios(bars, {day: 100.0}, estimator="median")[0][1] == pytest.approx(0.6)

    def test_the_last_bar_is_chosen_by_TIMESTAMP_not_by_input_order(self) -> None:
        day = date(2026, 9, 18)
        bars = list(reversed(_session(day, [50.0, 60.0, 100.0])))
        assert session_ratios(bars, {day: 100.0})[0][1] == pytest.approx(1.0)

    def test_a_session_with_no_daily_reference_is_dropped_not_defaulted(self) -> None:
        day = date(2026, 9, 18)
        assert session_ratios(_session(day, [100.0]), {}) == []
        assert session_ratios(_session(day, [100.0]), {day: 0.0}) == []

    def test_an_unknown_estimator_raises_rather_than_silently_picking_one(self) -> None:
        day = date(2026, 9, 18)
        with pytest.raises(ValueError, match="unknown estimator"):
            session_ratios(_session(day, [100.0]), {day: 100.0}, estimator="mean")


class TestIsSplitScale:
    def test_the_ratios_a_WHITELIST_missed_are_matched(self) -> None:
        """⚠ Codex checkpoint 1 reproduced both of these as "not a basis event".

        The first classifier listed 2, 3, 4, 5, 10 and reciprocals. A 3:2 split and a 15:1
        reverse split are real, documented ratios and both fell outside it.
        """
        assert is_split_scale(1.5) == (3, 2)
        assert is_split_scale(1 / 15) == (1, 15)
        assert is_split_scale(1.25) == (5, 4)
        assert is_split_scale(0.05) == (1, 20)

    def test_the_simplest_matching_ratio_wins(self) -> None:
        assert is_split_scale(2.0) == (2, 1)
        assert is_split_scale(0.5) == (1, 2)

    def test_a_factor_outside_the_limit_is_not_forced_into_a_ratio(self) -> None:
        assert is_split_scale(1.0324) is None
        assert is_split_scale(31.0) is None

    def test_a_match_is_a_RATIO_not_a_verdict(self) -> None:
        """An unambiguous match can still be an implausible split — the caller judges that.

        17:1 has no near neighbour under the limit, so it matches cleanly. Whether a 17:1 split
        is plausible for the instrument in hand is not this function's business, which is why it
        returns the ratio rather than a boolean.
        """
        assert is_split_scale(17.0) == (17, 1)

    def test_AMBIGUITY_rejects_a_match_rather_than_tightness(self) -> None:
        """⚠ This function was wrong twice, in opposite directions, and both are pinned here.

        A whitelist missed 3:2 and 15:1. Tightening to 20 bps to fix the resulting density then
        broke detection, because the factor is a ratio-of-ratios carrying the intraday-vs-daily
        mismatch on BOTH sides and the control's measured noise is ~1%. The tolerance therefore
        sits at the noise floor and ambiguity is what rejects a match.
        """
        # 19/17 is not a split: 9:8 is also within 1% of it, so the match is ambiguous.
        assert is_split_scale(19 / 17) is None
        # 2:1 has no near neighbour under the limit, so it survives realistic distortion.
        assert is_split_scale(2.0) == (2, 1)

    def test_a_genuine_split_DISTORTED_by_the_noise_floor_is_still_matched(self) -> None:
        """⚠ Codex checkpoint 2: at 20 bps a 2:1 transition distorted 0.5% returned None.

        That is the failure mode that matters — a real event classified as ordinary disagreement.
        """
        assert is_split_scale(0.4975) == (1, 2)
        assert is_split_scale(2.01) == (2, 1)
        assert is_split_scale(1.5 * 1.005) == (3, 2)


class TestDetectStep:
    def test_a_persistent_split_scale_shift_is_found_and_named(self) -> None:
        ratios = [
            (date(2026, 9, 14), 2.0, 13),
            (date(2026, 9, 15), 1.0, 13),
            (date(2026, 9, 16), 1.0, 13),
            (date(2026, 9, 17), 1.0, 13),
        ]
        step = detect_step(ratios)
        assert len(step["jumps"]) == 1
        jump = step["jumps"][0]
        assert jump["at"] == date(2026, 9, 15)
        assert jump["split_scale"] == (1, 2)
        assert jump["persisted"] is False, "one pre-jump session is not a settled level; the window needs two"

    def test_a_shift_with_a_SETTLED_LEVEL_ON_BOTH_SIDES_does_persist(self) -> None:
        ratios = [
            (date(2026, 9, 10), 2.0, 13),
            (date(2026, 9, 11), 2.0, 13),
            (date(2026, 9, 14), 1.0, 13),
            (date(2026, 9, 15), 1.0, 13),
        ]
        jumps = detect_step(ratios)["jumps"]
        assert len(jumps) == 1
        assert jumps[0]["persisted"] is True
        assert jumps[0]["split_scale"] == (1, 2)

    def test_NEITHER_EDGE_of_a_transient_spike_counts_as_a_level_shift(self) -> None:
        """⚠ Two Codex findings, one test.

        ckpt-1: `[1, 2, 1]` received the categorical nominal verdict at all. ckpt-2: checking
        persistence only FORWARD let the spike's RETURN edge pass — on `[1, 2, 1, 1]` the 2->1
        edge sees the tail `[1, 1]`, holds, matches 1:2, and the bad print becomes a positive
        result. A re-basing has a settled level on EACH side of one transition.
        """
        ratios = [
            (date(2026, 9, 11), 1.0, 13),
            (date(2026, 9, 14), 1.0, 13),
            (date(2026, 9, 15), 2.0, 13),
            (date(2026, 9, 16), 1.0, 13),
            (date(2026, 9, 17), 1.0, 13),
        ]
        jumps = detect_step(ratios)["jumps"]
        assert [jump["persisted"] for jump in jumps] == [False, False]
        excursion, ret = jumps
        assert excursion["held_before"] is True and excursion["held_after"] is False
        assert ret["held_before"] is False and ret["held_after"] is True
        assert ret["split_scale"] == (1, 2), "the return edge still matches a ratio; two-sided stability rejects it"

    def test_EVERY_above_tolerance_jump_is_classified_not_just_the_largest(self) -> None:
        """⚠ Codex reproduced `[2, 1, 0.13]` discarding the genuine 0.5x transition."""
        ratios = [
            (date(2026, 9, 14), 2.0, 13),
            (date(2026, 9, 15), 1.0, 13),
            (date(2026, 9, 16), 0.13, 13),
        ]
        jumps = detect_step(ratios)["jumps"]
        assert len(jumps) == 2
        assert jumps[0]["split_scale"] == (1, 2), "the smaller, genuine split-scale jump must survive"

    def test_the_largest_jump_is_over_ALL_pairs_not_only_those_above_tolerance(self) -> None:
        """Otherwise a quiet series reports 0.000000 and reads as having no variation at all —
        which is the opposite of the noise floor the control exists to establish."""
        ratios = [(date(2026, 9, 14), 1.0, 13), (date(2026, 9, 15), 1.005, 13)]
        step = detect_step(ratios, tolerance=0.02)
        assert step["jumps"] == []
        assert step["largest_jump"] == pytest.approx(0.005)
        assert step["largest_jump_above_tolerance"] == pytest.approx(0.0)

    def test_a_flat_series_reports_its_exact_deviation_rather_than_a_rounded_one(self) -> None:
        ratios = [(date(2026, 9, 14 + index), 1.0, 13) for index in range(4)]
        step = detect_step(ratios)
        assert step["max_abs_deviation"] == 0.0
        assert step["ratio_min"] == step["ratio_max"] == 1.0

    def test_the_PAIRED_span_is_reported_because_it_is_not_the_fetched_span(self) -> None:
        ratios = [(date(2026, 6, 1), 1.0, 13), (date(2026, 9, 18), 1.0, 13)]
        step = detect_step(ratios)
        assert (step["paired_first"], step["paired_last"]) == (date(2026, 6, 1), date(2026, 9, 18))
        assert step["sessions"] == 2, "two paired sessions can sit months apart; the count says so"

    def test_too_few_sessions_gives_a_REASON_not_a_verdict(self) -> None:
        assert detect_step([])["reason"] == "fewer than two paired sessions"
        assert detect_step([(date(2026, 9, 18), 1.0, 13)])["jumps"] == []

    def test_the_tolerance_is_a_parameter_so_the_control_can_set_it(self) -> None:
        """Measured 2026-09-20: AAPL, asserted to have no `adjustment_heal`, showed a largest
        adjacent jump of 0.0106 under the `last` estimator and 0.0219 under `median` — so 0.02 is
        meaningful for the first and not for the second."""
        ratios = [(date(2026, 9, 14), 1.0, 13), (date(2026, 9, 15), 1.015, 13)]
        assert detect_step(ratios, tolerance=0.02)["jumps"] == []
        assert len(detect_step(ratios, tolerance=0.01)["jumps"]) == 1


class TestNonFiniteGuards:
    def test_a_non_finite_provider_close_is_dropped_before_it_can_hide_a_jump(self) -> None:
        """⚠ Codex reproduced a leading NaN making a later 2x jump report "no step".

        The SQL guard covers the stored reference only; a non-finite value arriving from the
        PROVIDER has to be refused in Python.
        """
        day, later = date(2026, 9, 17), date(2026, 9, 18)
        bars = _session(day, [float("nan")]) + _session(later, [200.0])
        ratios = session_ratios(bars, {day: 100.0, later: 100.0})
        assert [ratio[0] for ratio in ratios] == [later], "the nan session must not enter the series"

    def test_a_non_finite_or_nonpositive_reference_is_dropped_too(self) -> None:
        day = date(2026, 9, 18)
        assert session_ratios(_session(day, [100.0]), {day: float("nan")}) == []
        assert session_ratios(_session(day, [100.0]), {day: -5.0}) == []
        assert session_ratios(_session(day, [-5.0]), {day: 100.0}) == []


class TestReferenceWindow:
    def test_the_lower_bound_FOLLOWS_the_interval_reach_rather_than_a_fixed_date(self) -> None:
        """⚠ Replaces a hardcoded `date(2025, 1, 1)` that had no stated rationale.

        The endpoint is count-based with no date anchor, so the furthest a request reaches back
        is `count` bars of `interval`. A fixed date silently under-fetches the daily reference for
        a long-reach interval — the failure is invisible because it looks like missing pairs.
        """
        today = date(2026, 9, 20)
        thirty = reference_window_start("ThirtyMinutes", 1000, today=today)
        four_hours = reference_window_start("FourHours", 1000, today=today)
        weekly = reference_window_start("OneWeek", 1000, today=today)
        assert thirty < today
        assert four_hours < thirty, "a wider interval reaches further back, so its reference must too"
        assert weekly < four_hours

    def test_an_unknown_interval_falls_back_to_the_WIDEST_reach(self) -> None:
        """Under-fetching is the silent failure; over-fetching is merely a bigger query."""
        today = date(2026, 9, 20)
        assert reference_window_start("SomeNewInterval", 1000, today=today) == reference_window_start(
            "OneWeek", 1000, today=today
        )

    def test_a_tiny_count_still_yields_a_usable_window(self) -> None:
        today = date(2026, 9, 20)
        assert reference_window_start("ThirtyMinutes", 1, today=today) < today
