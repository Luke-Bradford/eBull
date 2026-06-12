"""Shared helpers for API route modules.

``parse_optional_float`` and ``resolve_quote_price`` moved to
``app/services/valuation.py`` with #1596 — the shared valuation
helper needs them and the service layer must not import from the API
layer. Re-exported here so existing API-module imports keep working.
"""

from __future__ import annotations

from app.services.valuation import parse_optional_float, resolve_quote_price

__all__ = ["parse_optional_float", "parse_optional_int", "resolve_quote_price"]


def parse_optional_int(row: dict[str, object], key: str) -> int | None:
    """Safely cast a nullable integer DB column to int."""
    val = row.get(key)
    if val is None:
        return None
    return int(val)  # type: ignore[arg-type]
