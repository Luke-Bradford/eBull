"""Adjustment read path + segment model (#2261).

THE FACTOR-DIRECTION TESTS ARE THE POINT OF THIS FILE. Inverting the factor
moves every historical bar the wrong way by factor SQUARED and leaves a series
that is still internally consistent, so it is invisible on a chart and invisible
to any single-direction test that just asserts "the number changed". Both
directions are pinned here by asserting CONTINUITY across the effective date:
the last pre-adjustment bar and the first post-adjustment bar must land on the
same scale. An inverted factor turns that join into a factor**2 cliff.

Same inversion class Codex caught on the #2231 spec.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.price_adjustments import (
    Adjustment,
    SeriesBreak,
    adjusted_close,
    series_segments,
)

D = Decimal


class TestFactorDirection:
    def test_reverse_split_1_for_10_scales_history_UP(self) -> None:
        # 1:10 reverse split on 2024-01-10, F = 10. A $1 share becomes a $10
        # share; every bar before the effective date must read x10.
        adjustments = [Adjustment(effective_date=date(2024, 1, 10), factor=D(10))]
        before = adjusted_close(D("1.00"), date(2024, 1, 9), adjustments)
        on_day = adjusted_close(D("10.00"), date(2024, 1, 10), adjustments)
        assert before == D("10.00")
        assert on_day == D("10.00")
        # CONTINUITY: an inverted factor would give 0.10 vs 10.00 — a x100
        # (factor**2) cliff exactly at the join.
        assert before == on_day

    def test_forward_split_20_for_1_scales_history_DOWN(self) -> None:
        # 20:1 forward split on 2022-06-06, F = 0.05. AMZN ran ~$2,150 to ~$107.
        adjustments = [Adjustment(effective_date=date(2022, 6, 6), factor=D("0.05"))]
        before = adjusted_close(D("2140.00"), date(2022, 6, 3), adjustments)
        on_day = adjusted_close(D("107.00"), date(2022, 6, 6), adjustments)
        assert before == D("107.00")
        assert before == on_day

    def test_effective_date_bar_itself_is_already_on_the_new_scale(self) -> None:
        # STRICTLY BEFORE. An off-by-one that includes the effective date
        # double-applies the factor to the first post-split bar.
        adjustments = [Adjustment(effective_date=date(2024, 1, 10), factor=D(10))]
        assert adjusted_close(D("10.00"), date(2024, 1, 10), adjustments) == D("10.00")

    def test_factors_compound_across_multiple_actions(self) -> None:
        adjustments = [
            Adjustment(effective_date=date(2022, 6, 6), factor=D("0.05")),
            Adjustment(effective_date=date(2024, 1, 10), factor=D(10)),
        ]
        # A 2022-06-03 bar sits before BOTH: 2140 * 0.05 * 10 = 1070.
        assert adjusted_close(D("2140.00"), date(2022, 6, 3), adjustments) == D("1070.00")
        # A 2023 bar sits before only the reverse split.
        assert adjusted_close(D("107.00"), date(2023, 5, 1), adjustments) == D("1070.00")

    def test_no_adjustments_is_identity(self) -> None:
        assert adjusted_close(D("42.50"), date(2025, 1, 1), []) == D("42.50")


class TestSeriesSegments:
    """A single ``usable_from`` gate discards joinable segments (S7 §8)."""

    def test_no_breaks_is_one_segment(self) -> None:
        segments = series_segments(date(2020, 1, 1), date(2026, 1, 1), [])
        assert len(segments) == 1
        assert (segments[0].start, segments[0].end) == (date(2020, 1, 1), date(2026, 1, 1))

    def test_three_breaks_with_the_middle_one_resolved_keeps_the_joinable_pair(self) -> None:
        # The case a single gate date gets wrong: it would keep only the run
        # after the LAST break and throw the joinable pair away.
        breaks = [
            SeriesBreak(date(2021, 6, 1), D("0.1"), resolved=False),
            SeriesBreak(date(2022, 6, 1), D("0.1"), resolved=True),
            SeriesBreak(date(2023, 6, 1), D("0.1"), resolved=False),
        ]
        segments = series_segments(date(2020, 1, 1), date(2026, 1, 1), breaks)
        assert len(segments) == 3
        assert (segments[0].start, segments[0].end) == (date(2020, 1, 1), date(2021, 5, 31))
        # The middle segment spans the RESOLVED break — its two halves are
        # joinable through the adjustment factor, so they are one segment.
        assert (segments[1].start, segments[1].end) == (date(2021, 6, 1), date(2023, 5, 31))
        assert segments[1].resolved_breaks == (date(2022, 6, 1),)
        assert (segments[2].start, segments[2].end) == (date(2023, 6, 1), date(2026, 1, 1))

    def test_a_fully_resolved_instrument_is_one_segment(self) -> None:
        breaks = [SeriesBreak(date(2022, 6, 1), D("0.05"), resolved=True)]
        segments = series_segments(date(2020, 1, 1), date(2026, 1, 1), breaks)
        assert len(segments) == 1
        assert segments[0].resolved_breaks == (date(2022, 6, 1),)

    def test_break_outside_the_series_window_is_ignored(self) -> None:
        breaks = [SeriesBreak(date(2019, 1, 1), D("0.1"), resolved=False)]
        segments = series_segments(date(2020, 1, 1), date(2026, 1, 1), breaks)
        assert len(segments) == 1

    def test_empty_series_yields_no_segments(self) -> None:
        assert series_segments(date(2026, 1, 2), date(2026, 1, 1), []) == []
