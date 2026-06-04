"""#1433 — the shared ownership period_end sanity window.

``period_end_within_bounds`` is the single source both the 13F and N-PORT
bulk ingesters use to reject a NULL or out-of-[1900, 2100) period_end before
it lands in the DEFAULT partition (mirrors the #1218 XBRL guard).
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.ownership_observations import (
    OWNERSHIP_PERIOD_END_MAX,
    OWNERSHIP_PERIOD_END_MIN,
    period_end_within_bounds,
)


def test_bounds_constants() -> None:
    assert OWNERSHIP_PERIOD_END_MIN == date(1900, 1, 1)
    assert OWNERSHIP_PERIOD_END_MAX == date(2100, 1, 1)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, False),
        (date(1899, 12, 31), False),  # below MIN
        (date(1900, 1, 1), True),  # inclusive MIN
        (date(2025, 9, 30), True),  # typical quarter-end
        (date(2099, 12, 31), True),  # below MAX
        (date(2100, 1, 1), False),  # exclusive MAX
        (date(6016, 9, 30), False),  # parser-bug junk (#1218 in-the-wild shape)
    ],
)
def test_period_end_within_bounds(value: date | None, expected: bool) -> None:
    assert period_end_within_bounds(value) is expected
