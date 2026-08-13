"""Pure-logic tests for the FCF-yield formula (#671).

The yield arithmetic is the only branch-bearing pure policy in the FCF-yield
path (the SQL window + suppression are exercised by dev-verify + the
full-population check in the PR). Table-test it here (no DB, fast tier).
"""

from decimal import Decimal

import pytest

from app.services.fcf_yield import fcf_yield_pct


@pytest.mark.parametrize(
    ("fcf_ttm", "market_cap", "expected"),
    [
        (Decimal("100"), Decimal("2000"), Decimal("5")),  # 100 / 2000 → 5%
        (Decimal("-50"), Decimal("1000"), Decimal("-5")),  # negative FCF → negative yield, NOT clamped
        (Decimal("0"), Decimal("1000"), Decimal("0")),  # zero FCF → 0%
        (None, Decimal("1000"), None),  # missing FCF → None
        (Decimal("100"), None, None),  # missing market cap → None
        (Decimal("100"), Decimal("0"), None),  # zero market cap → None (no ZeroDivision)
        (Decimal("100"), Decimal("-5"), None),  # negative market cap → None (fail-closed)
    ],
)
def test_fcf_yield_pct(fcf_ttm: Decimal | None, market_cap: Decimal | None, expected: Decimal | None) -> None:
    result = fcf_yield_pct(fcf_ttm, market_cap)
    if expected is None:
        assert result is None
    else:
        assert result == expected
