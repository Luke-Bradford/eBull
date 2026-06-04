"""Quote-mark primitives — the strictly-positive contract for prices.

A usable execution price / valuation mark is STRICTLY POSITIVE. eToro
persists ``quotes.last/bid/ask = 0.00`` for instruments with no recent
trade or a one-sided book, and ``broker_positions.open_rate`` carries no
positive CHECK constraint. A non-positive (or null) value must be treated
as *missing*, never used to price a synthetic fill at 0 (#1428 read-path,
#1439 execution-path). Mirrors the SQL ``NULLIF(GREATEST(x, 0), 0)``
contract and ``app/api/_helpers.resolve_quote_price``'s ``> 0`` rule.

Lives in a neutral service module so both the recommendation-execution
service (``app/services/order_client.py``) and the manual-order API route
(``app/api/orders.py``) share one source of truth without coupling the
API to the execution service.
"""

from __future__ import annotations

from decimal import Decimal


def positive_decimal_or_none(value: object) -> Decimal | None:
    """Return ``value`` as a strictly-positive ``Decimal``, else ``None``."""
    if value is None:
        return None
    dec = Decimal(str(value))
    return dec if dec > 0 else None
