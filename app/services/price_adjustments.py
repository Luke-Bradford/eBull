"""
Price-adjustment read path and the series-segment model (#2261, phase 0a of
#2240; spec = the S7 verdict on #2247 §7 and §8).

This module does NOT detect adjustments. Design-doc decision 8: split
adjustment is an auditable TABLE, fed by whatever source establishes a factor
(#2231's XBRL detector when it lands, eToro, or the operator). Here we only
read it back, and read back the series breaks it may or may not resolve.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg


@dataclass(frozen=True)
class Adjustment:
    effective_date: date
    """First bar at the NEW scale."""

    factor: Decimal
    """Multiply bars STRICTLY BEFORE ``effective_date`` by this to reach the new scale."""

    adjustment_id: int | None = None


@dataclass(frozen=True)
class SeriesBreak:
    break_date: date
    """The bar at the new scale."""

    observed_ratio: Decimal
    resolved: bool


@dataclass(frozen=True)
class Segment:
    """A run of bars that share one unit regime and are therefore joinable.

    Adjacent segments are separated by an UNRESOLVED break: their prices are
    each internally valid but cannot be compared to one another, because no
    factor is known.
    """

    start: date
    end: date
    resolved_breaks: tuple[date, ...] = ()
    """Breaks INSIDE this segment — resolved, so an adjustment factor joins across them."""

    @property
    def days(self) -> int:
        return (self.end - self.start).days


def adjusted_close(close: Decimal, bar_date: date, adjustments: list[Adjustment]) -> Decimal:
    """Express ``close`` (as stored on ``bar_date``) in the CURRENT scale.

    ``close x product(factor)`` over adjustments with ``effective_date > bar_date``.

    DIRECTION. The factor is defined so that a bar STRICTLY BEFORE the effective
    date is multiplied by it:

        1:10 reverse split, F = 10   -> a stored $1 bar reads $10
        20:1 forward split, F = 0.05 -> a stored $2000 bar reads $100

    Inverting it moves every historical bar the wrong way by factor SQUARED and
    leaves a series that still looks internally consistent, so it is not visible
    on a chart. ``tests/test_price_adjustments.py`` pins BOTH directions by
    asserting the series is CONTINUOUS across the effective date — an inverted
    factor turns a continuous join into a factor**2 cliff.

    Caller supplies the adjustment list, because which rows are active depends
    on the replay pin (see ``load_adjustments``); this function is pure.
    """
    factor = Decimal(1)
    for adjustment in adjustments:
        if adjustment.effective_date > bar_date:
            factor *= adjustment.factor
    return close * factor


def series_segments(first_bar: date, last_bar: date, breaks: list[SeriesBreak]) -> list[Segment]:
    """Split ``[first_bar, last_bar]`` into joinable segments at UNRESOLVED breaks.

    WHY NOT A SINGLE ``usable_from`` GATE (S7 §8). An instrument with three
    breaks where the middle one resolves has TWO joinable segments and one
    stranded one. A single gate date keeps only the run after the last break and
    discards the joinable pair — which silently shrinks and biases the eligible
    universe, exactly what "marked, never dropped" exists to prevent.

    A backtest may use any single segment, or any run of segments joined by
    resolved adjustments (which is what a resolved break inside a segment is).
    """
    if last_bar < first_bar:
        return []
    ordered = sorted(breaks, key=lambda b: b.break_date)
    segments: list[Segment] = []
    start = first_bar
    resolved_inside: list[date] = []
    for series_break in ordered:
        if not (first_bar < series_break.break_date <= last_bar):
            # A break outside the series window says nothing about these bars.
            continue
        if series_break.resolved:
            resolved_inside.append(series_break.break_date)
            continue
        segments.append(Segment(start, series_break.break_date - timedelta(days=1), tuple(resolved_inside)))
        start = series_break.break_date
        resolved_inside = []
    segments.append(Segment(start, last_bar, tuple(resolved_inside)))
    return segments


def load_adjustments(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    *,
    pinned_adjustment_id: int | None = None,
    run_ts: datetime | None = None,
) -> list[Adjustment]:
    """Active adjustments for an instrument, optionally as of a pinned replay point.

    With no pin this is "what is true now" (``superseded_by IS NULL``). With a
    pin it is "what was true when that backtest ran" — rows are append-only and
    corrections supersede rather than update, so both are reconstructable, and
    ``superseded_by`` alone cannot do the second.
    """
    if pinned_adjustment_id is None:
        rows = conn.execute(
            """
            SELECT adjustment_id, effective_date, factor
            FROM price_adjustments
            WHERE instrument_id = %(iid)s AND superseded_by IS NULL
            ORDER BY effective_date
            """,
            {"iid": instrument_id},
        ).fetchall()
    else:
        if run_ts is None:
            raise ValueError("run_ts is required when pinning an adjustment snapshot")
        rows = conn.execute(
            """
            SELECT adjustment_id, effective_date, factor
            FROM price_adjustments
            WHERE instrument_id = %(iid)s
              AND adjustment_id <= %(pinned)s
              AND (superseded_at IS NULL OR superseded_at > %(run_ts)s)
            ORDER BY effective_date
            """,
            {"iid": instrument_id, "pinned": pinned_adjustment_id, "run_ts": run_ts},
        ).fetchall()
    return [Adjustment(effective_date=r[1], factor=Decimal(r[2]), adjustment_id=int(r[0])) for r in rows]


def load_breaks(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
) -> list[SeriesBreak]:
    rows = conn.execute(
        """
        SELECT break_date, observed_ratio, resolved_by
        FROM price_series_break
        WHERE instrument_id = %(iid)s
        ORDER BY break_date
        """,
        {"iid": instrument_id},
    ).fetchall()
    return [SeriesBreak(break_date=r[0], observed_ratio=Decimal(r[1]), resolved=r[2] is not None) for r in rows]
