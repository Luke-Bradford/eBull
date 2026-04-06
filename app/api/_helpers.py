"""Shared helpers for API route modules."""

from __future__ import annotations


def parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def parse_optional_int(row: dict[str, object], key: str) -> int | None:
    """Safely cast a nullable integer DB column to int."""
    val = row.get(key)
    if val is None:
        return None
    return int(val)  # type: ignore[arg-type]
