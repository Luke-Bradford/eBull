"""Shared unresolved price-scale segmentation for strategy consumers.

``price_series_break.break_date`` is the first date at the new scale. Bars on
either side remain usable inside their own segment; indicators and positions
must not span the boundary. The loader is intentionally sparse and bounded to
the requested instruments.
"""

from __future__ import annotations

from bisect import bisect_left
from collections.abc import Mapping, Sequence
from datetime import date
from itertools import pairwise
from typing import Any

import psycopg

from app.services.indicator_series import BarSeries

_UNRESOLVED_BREAKS_SQL = """
    SELECT instrument_id, break_date
    FROM price_series_break
    WHERE instrument_id = ANY(%(instrument_ids)s)
      AND resolved_by IS NULL
    ORDER BY instrument_id, break_date
"""


def load_unresolved_breaks(
    conn: psycopg.Connection[Any], instrument_ids: Sequence[int]
) -> Mapping[int, tuple[date, ...]]:
    """Return unresolved scale transitions for only ``instrument_ids``."""
    ids = sorted(set(instrument_ids))
    if not ids:
        return {}
    rows = conn.execute(_UNRESOLVED_BREAKS_SQL, {"instrument_ids": ids}).fetchall()
    grouped: dict[int, list[date]] = {}
    for instrument_id, break_date in rows:
        grouped.setdefault(int(instrument_id), []).append(break_date)
    return {instrument_id: tuple(dates) for instrument_id, dates in grouped.items()}


def series_segment_bounds(
    series: BarSeries,
    *,
    unresolved_breaks: Sequence[date],
) -> tuple[tuple[int, int], ...]:
    """Half-open bar-index ranges separated by unresolved scale transitions."""
    ordered = tuple(unresolved_breaks)
    if tuple(sorted(ordered)) != ordered or len(set(ordered)) != len(ordered):
        raise ValueError("unresolved break dates must be unique and ordered")
    cuts = {0, len(series)}
    cuts.update(bisect_left(series.dates, break_date) for break_date in ordered)
    ordered_cuts = sorted(cuts)
    return tuple((start, end) for start, end in pairwise(ordered_cuts) if start < end)


def segment_for_index(
    series: BarSeries,
    *,
    index: int,
    unresolved_breaks: Sequence[date],
) -> tuple[BarSeries, int]:
    """Return the independent segment containing ``index`` and its local index."""
    if not 0 <= index < len(series):
        raise ValueError(f"index {index} is outside the {len(series)}-bar series")
    for start, end in series_segment_bounds(series, unresolved_breaks=unresolved_breaks):
        if start <= index < end:
            return BarSeries(dates=series.dates[start:end], rows=series.rows[start:end]), index - start
    raise RuntimeError(f"index {index} belongs to no price segment")


def segment_end_index(
    series: BarSeries,
    *,
    fill_index: int,
    unresolved_breaks: Sequence[date],
) -> int | None:
    """Return the last stored bar before the next break after ``fill_index``."""
    if not 0 <= fill_index < len(series):
        raise ValueError(f"fill_index {fill_index} is outside the {len(series)}-bar series")
    for start, end in series_segment_bounds(series, unresolved_breaks=unresolved_breaks):
        if start <= fill_index < end:
            return None if end == len(series) else end - 1
    raise RuntimeError(f"fill_index {fill_index} belongs to no price segment")


__all__ = [
    "load_unresolved_breaks",
    "segment_end_index",
    "segment_for_index",
    "series_segment_bounds",
]
