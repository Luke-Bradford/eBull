"""Pure-logic tests for fx_history gap-range computation (#1594 PR-A)."""

from __future__ import annotations

from datetime import date

from app.services.fx_history import fetch_ranges, supported_targets


def _d(s: str) -> date:
    return date.fromisoformat(s)


class TestFetchRanges:
    def test_empty_table_fetches_whole_span(self) -> None:
        assert fetch_ranges(None, None, _d("2025-01-01"), _d("2025-06-01")) == [(_d("2025-01-01"), _d("2025-06-01"))]

    def test_fully_covered_fetches_nothing(self) -> None:
        # since/until both inside [min, max] → no gap.
        assert fetch_ranges(_d("2025-01-01"), _d("2025-12-31"), _d("2025-03-01"), _d("2025-09-01")) == []

    def test_forward_tail_only(self) -> None:
        # max_existing < until → fetch the forward tail (touching boundary).
        assert fetch_ranges(_d("2025-01-01"), _d("2025-06-01"), _d("2025-01-01"), _d("2025-06-10")) == [
            (_d("2025-06-01"), _d("2025-06-10"))
        ]

    def test_older_gap_only(self) -> None:
        # since < min_existing → fetch the older span.
        assert fetch_ranges(_d("2025-03-01"), _d("2025-06-01"), _d("2025-01-01"), _d("2025-06-01")) == [
            (_d("2025-01-01"), _d("2025-03-01"))
        ]

    def test_both_gaps(self) -> None:
        assert fetch_ranges(_d("2025-03-01"), _d("2025-04-01"), _d("2025-01-01"), _d("2025-06-01")) == [
            (_d("2025-01-01"), _d("2025-03-01")),
            (_d("2025-04-01"), _d("2025-06-01")),
        ]

    def test_until_before_since_is_empty(self) -> None:
        assert fetch_ranges(None, None, _d("2025-06-01"), _d("2025-01-01")) == []

    def test_single_day_forward(self) -> None:
        # Steady-state: floor already covered (since == min_existing), latest on
        # file is yesterday → fetch only [yesterday, today].
        assert fetch_ranges(_d("2025-01-01"), _d("2025-06-12"), _d("2025-01-01"), _d("2025-06-13")) == [
            (_d("2025-06-12"), _d("2025-06-13"))
        ]


class TestSupportedTargets:
    def test_excludes_base_sorted(self) -> None:
        # SUPPORTED_CURRENCIES = {GBP, USD, EUR}; USD base → [EUR, GBP].
        assert supported_targets("USD") == ["EUR", "GBP"]
