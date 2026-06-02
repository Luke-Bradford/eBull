"""Shared helpers for API route modules."""

from __future__ import annotations


def parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def resolve_quote_price(
    last: float | None,
    bid: float | None,
    ask: float | None,
) -> float | None:
    """Return a usable live-quote price, or ``None`` if none is available.

    Rule: a usable mark is the trade ``last`` when strictly positive; else
    the bid/ask mid when the book is two-sided and both sides are positive.

    A non-positive ``last`` is treated as missing. eToro persists
    ``quotes.last = 0.00`` for instruments not freshly traded (bid/ask
    present, no recent trade). Using 0 as the mark values a position at 0 →
    fake −100% P&L (#1428). Callers supply their own downstream fallback
    (daily_close → cost basis / open_rate) when this returns ``None``.
    """
    if last is not None and last > 0:
        return last
    if bid is not None and bid > 0 and ask is not None and ask > 0:
        return (bid + ask) / 2.0
    return None


def parse_optional_int(row: dict[str, object], key: str) -> int | None:
    """Safely cast a nullable integer DB column to int."""
    val = row.get(key)
    if val is None:
        return None
    return int(val)  # type: ignore[arg-type]
