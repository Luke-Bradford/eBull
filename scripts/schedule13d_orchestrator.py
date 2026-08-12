"""Gate-protected orchestration for the frozen Schedule 13D study.

The module has no CLI and cannot construct an outcome gate.  It only coordinates
the already reviewed source, price, challenger and report primitives after a
caller has crossed that boundary.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, LiteralString

import psycopg

from scripts.evaluate_2582_schedule13d_outcomes import (
    OutcomeGate,
    SourceEvent,
    load_initial_13g_price_windows,
    load_initial_13g_source_events,
    load_price_windows,
    load_random_time_price_windows,
    load_source_events,
    prepare_price_window_workspace,
)
from scripts.schedule13d_report import HistoricalFalsificationReport, build_historical_falsification_report

_CURRENT_SECTOR_SQL: LiteralString = """
SELECT i.instrument_id,
       coalesce(esi.name, 'provider_industry_id:' || i.sector) AS sector_label
FROM instruments i
LEFT JOIN etoro_stocks_industries esi
  ON esi.industry_id::text = i.sector
WHERE i.instrument_id = ANY(%(instrument_ids)s::bigint[])
  AND i.sector IS NOT NULL
ORDER BY i.instrument_id
"""


def load_current_sector_labels(conn: psycopg.Connection[Any], events: Sequence[SourceEvent]) -> dict[int, str]:
    """Load current provider sectors for concentration attribution only.

    These are explicitly not point-in-time classifications and cannot gate the
    candidate or define a rescuing subgroup.  A missing lookup label retains the
    stable provider-industry id rather than merging it into another sector.
    """

    instrument_ids = sorted({event.instrument_id for event in events if event.instrument_id is not None})
    if not instrument_ids:
        return {}
    rows = conn.execute(_CURRENT_SECTOR_SQL, {"instrument_ids": instrument_ids}).fetchall()
    return {int(instrument_id): str(label) for instrument_id, label in rows}


def evaluate_historical_falsification(
    conn: psycopg.Connection[Any], gate: OutcomeGate
) -> HistoricalFalsificationReport:
    """Evaluate every frozen arm once and return one aggregate report.

    The transaction is read-only for durable tables.  The shared price loader's
    bounded temporary table remains permissible, while accidental application
    writes fail closed at PostgreSQL.
    """

    prepare_price_window_workspace(conn)
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    try:
        source_events = load_source_events(conn)
        initial_13g_sources = load_initial_13g_source_events(conn)
        primary_windows = load_price_windows(conn, gate, source_events, population="primary")
        unfiltered_windows = load_price_windows(conn, gate, source_events, population="unfiltered")
        random_windows = load_random_time_price_windows(conn, gate, source_events)
        initial_13g_windows = load_initial_13g_price_windows(conn, gate, initial_13g_sources)
        sectors = load_current_sector_labels(conn, source_events)
        return build_historical_falsification_report(
            source_events=source_events,
            initial_13g_sources=initial_13g_sources,
            primary_windows=primary_windows,
            unfiltered_windows=unfiltered_windows,
            random_windows=random_windows,
            initial_13g_windows=initial_13g_windows,
            sector_by_instrument=sectors,
        )
    finally:
        conn.rollback()


__all__ = [
    "_CURRENT_SECTOR_SQL",
    "evaluate_historical_falsification",
    "load_current_sector_labels",
]
