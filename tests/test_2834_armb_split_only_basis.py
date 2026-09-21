"""Pure-logic tests for #2834's split-only construction. Refs #2834.

No DB. The measurement's evidence is the script's own full-population output;
what is pinned here is the arithmetic the verdict rests on, because each failure
direction is silent:

* the back-adjusting scale is half-open at the EVENT date. Including the event
  bar would divide an already-post-split close by its own factor, turning a 4:1
  into a 4x error in the opposite direction — and the series would still look
  plausible, because every level is positive and monotone;
* the cumulative product must run FUTURE-ward. Multiplying past factors instead
  would leave the most recent bars rescaled and the oldest untouched, i.e. an
  adjustment applied backwards in time;
* ``reference_is_unadjusted`` and ``implied_factor`` are what separate "the
  stamp is wrong" from "the other vendor is unadjusted here". Swapping them
  attributes a defect to the wrong producer, which is the mistake the residual
  classification exists to avoid.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from scripts.measure_2834_armb_split_only_basis import (
    EventCheck,
    SplitEvent,
    back_adjust_scale,
)


def _event(bar_date: date, factor: str, series_id: int = 1) -> SplitEvent:
    return SplitEvent(series_id=series_id, vendor_symbol="X", bar_date=bar_date, factor=Decimal(factor))


class TestBackAdjustScale:
    def test_one_event_scales_only_the_bars_before_it(self) -> None:
        segments = back_adjust_scale([_event(date(2020, 8, 31), "4")])
        assert segments == [(1, date.min, date(2020, 8, 31), Decimal(4))]

    def test_aapl_scale_reproduces_the_split_adjusted_close(self) -> None:
        """500.04 / 4 = 125.01 — the figure sql/251 recorded for the other vendor."""
        (_, _, _, scale), *rest = back_adjust_scale([_event(date(2020, 8, 31), "4")])
        assert not rest
        assert Decimal("500.04") / scale == Decimal("125.01")

    def test_two_events_compound_future_ward(self) -> None:
        segments = sorted(
            back_adjust_scale([_event(date(2000, 1, 3), "2"), _event(date(2010, 6, 1), "3")]),
            key=lambda seg: seg[1],
        )
        # Bars before the FIRST event carry both factors; bars between carry only
        # the later one; bars from the last event on carry none (no segment).
        assert segments == [
            (1, date.min, date(2000, 1, 3), Decimal(6)),
            (1, date(2000, 1, 3), date(2010, 6, 1), Decimal(3)),
        ]

    def test_a_reverse_split_scales_below_one(self) -> None:
        """A 1-for-10 reverse DIVIDES the raw level, so the scale is 0.1.

        Named because the 'error has a sign' story withdrawn on #3278 assumed
        forward splits only — the corpus carries 570 stamps of 0.1 alone.
        """
        assert back_adjust_scale([_event(date(2019, 5, 1), "0.1")])[0][3] == Decimal("0.1")

    def test_segments_are_per_series(self) -> None:
        segments = back_adjust_scale(
            [_event(date(2020, 1, 2), "2", series_id=7), _event(date(2020, 1, 2), "3", series_id=9)]
        )
        assert {(series_id, scale) for series_id, _, _, scale in segments} == {
            (7, Decimal(2)),
            (9, Decimal(3)),
        }

    def test_no_events_is_no_segments(self) -> None:
        assert back_adjust_scale([]) == []


class TestResidualClassification:
    def _check(self, *, factor: str, raw: str, ref: str) -> EventCheck:
        raw_ratio = Decimal(raw)
        ref_ratio = Decimal(ref)
        stamped = Decimal(factor)
        return EventCheck(
            event=_event(date(1996, 5, 9), factor),
            internal_error=Decimal(0),
            raw_error=abs(raw_ratio / ref_ratio - 1),
            split_error=abs(raw_ratio * stamped / ref_ratio - 1),
            raw_ratio=raw_ratio,
            ref_ratio=ref_ratio,
        )

    def test_a_clean_split_agrees_after_correction(self) -> None:
        """AAPL's shape: raw steps by 1/4, the reference does not step."""
        check = self._check(factor="4", raw="0.25", ref="1")
        assert check.agrees
        assert not check.raw_agrees
        assert not check.reference_is_unadjusted
        assert check.implied_factor == Decimal(4)

    def test_civb_shape_is_attributed_to_the_reference(self) -> None:
        """BOTH vendors print the raw 4:1 step, so the stamp is corroborated."""
        check = self._check(factor="4", raw="0.25", ref="0.25")
        assert not check.agrees
        assert check.raw_agrees
        assert check.reference_is_unadjusted
        assert check.implied_factor == Decimal(1)

    def test_coo_shape_is_a_wrong_magnitude(self) -> None:
        """A real 4:1 against a stamped 16 — the class B1 cannot see."""
        check = self._check(factor="16", raw="0.25", ref="1")
        assert not check.agrees
        assert not check.reference_is_unadjusted
        assert check.implied_factor == Decimal(4)

    def test_an_unserved_event_reaches_no_reference_verdict(self) -> None:
        check = EventCheck(
            event=_event(date(2001, 1, 2), "2"),
            internal_error=Decimal(0),
            raw_error=None,
            split_error=None,
            raw_ratio=Decimal("0.5"),
            ref_ratio=None,
        )
        assert not check.agrees
        assert not check.raw_agrees
        assert not check.reference_is_unadjusted
        assert check.implied_factor is None
        # ⚠ The internal check passing is NOT a verdict — 364 of 383 events the
        # reference rejects also pass it.
        assert check.internal_agrees
