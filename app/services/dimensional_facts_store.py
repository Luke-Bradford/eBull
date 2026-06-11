"""DB layer for dimensional XBRL facts (#554).

Writer: per-accession delete-then-insert (spec §D4 — rows are
immutable in normal operation; a rewash of an accession replaces its
rows in one transaction, stamping the new parser_version).

Reader: winning accession per (instrument, axis, METRIC) — not per
axis — so a 10-K/A that restates revenue but omits operating income
cannot regress the omitted metric to empty (spec §D4, Codex ckpt-1
HIGH). "Latest FY" is per metric kind: duration metrics take the
winner's max period_end among annual durations (330–400 days, which
excludes the quarterly/YTD contexts a 10-K also carries); the instant
metric (assets) takes the winner's max instant. Subtotal rows are
excluded — leaf rows sum to the consolidated figure (spec §D3).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.services.dimensional_facts import (
    DimensionalAxis,
    DimensionalFact,
    DimensionalMetric,
)

logger = logging.getLogger(__name__)

_METRICS: tuple[DimensionalMetric, ...] = ("revenue", "operating_income", "assets")


def replace_accession_rows(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
    form_type: str,
    filed_at: datetime,
    parser_version: str,
    facts: list[DimensionalFact],
) -> int:
    """Replace all dimensional-fact rows for one (instrument, accession).

    Runs in the caller's transaction (the sec_10k parser wraps this in
    its own savepoint). Returns the number of rows inserted.
    """
    conn.execute(
        "DELETE FROM instrument_dimensional_facts WHERE instrument_id = %s AND source_accession = %s",
        (instrument_id, source_accession),
    )
    if not facts:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO instrument_dimensional_facts (
                instrument_id, axis, member_qname, member_label, metric,
                unit, is_subtotal, period_start, period_end, val, decimals,
                source_accession, form_type, filed_at, parser_version
            ) VALUES (
                %(instrument_id)s, %(axis)s, %(member_qname)s, %(member_label)s,
                %(metric)s, %(unit)s, %(is_subtotal)s, %(period_start)s,
                %(period_end)s, %(val)s, %(decimals)s, %(source_accession)s,
                %(form_type)s, %(filed_at)s, %(parser_version)s
            )
            """,
            [
                {
                    "instrument_id": instrument_id,
                    "axis": f.axis,
                    "member_qname": f.member_qname,
                    "member_label": f.member_label,
                    "metric": f.metric,
                    "unit": f.unit,
                    "is_subtotal": f.is_subtotal,
                    "period_start": f.period_start,
                    "period_end": f.period_end,
                    "val": f.val,
                    "decimals": f.decimals,
                    "source_accession": source_accession,
                    "form_type": form_type,
                    "filed_at": filed_at,
                    "parser_version": parser_version,
                }
                for f in facts
            ],
        )
        # psycopg3 executemany rowcount is cumulative across the batch
        # (python-hygiene skill, empirically pinned in
        # test_sec_13f_dataset_ingest).
        return cur.rowcount


@dataclass(frozen=True)
class SegmentsReadResult:
    """Latest-FY leaf rows for one (instrument, axis), merged by member."""

    axis: DimensionalAxis
    period_end: date | None
    filed_at: datetime | None
    sources: dict[str, str]  # metric → winning accession
    rows: list[dict[str, Any]]  # member_qname, member_label, <metric>: Decimal|None


# Annual-duration window in days: catches 52/53-week fiscal years
# (AAPL-style) without admitting the quarterly/YTD contexts a 10-K
# also carries.
_ANNUAL_DURATION_SQL = "(f.period_start IS NULL OR (f.period_end - f.period_start) BETWEEN 330 AND 400)"

_METRIC_ROWS_SQL = f"""
WITH winner AS (
    -- Winner = latest accession HAVING eligible rows (annual-duration,
    -- non-subtotal). Selecting before filtering would let a 10-K/A
    -- carrying only quarterly/YTD or only subtotal rows win and blank
    -- the metric (#554 Codex pre-push finding).
    SELECT f.source_accession, f.filed_at
      FROM instrument_dimensional_facts f
     WHERE f.instrument_id = %(instrument_id)s
       AND f.axis = %(axis)s
       AND f.metric = %(metric)s
       AND NOT f.is_subtotal
       AND {_ANNUAL_DURATION_SQL}
     ORDER BY f.filed_at DESC, f.source_accession DESC
     LIMIT 1
),
target AS (
    -- Same eligibility filters as winner + main query: a CTE chain
    -- whose stages disagree on filters can anchor on a row the final
    -- join then excludes, returning zero rows despite eligible data
    -- (review-bot WARNING on PR #1588).
    SELECT MAX(f.period_end) AS period_end
      FROM instrument_dimensional_facts f
      JOIN winner w ON w.source_accession = f.source_accession
     WHERE f.instrument_id = %(instrument_id)s
       AND f.axis = %(axis)s
       AND f.metric = %(metric)s
       AND NOT f.is_subtotal
       AND {_ANNUAL_DURATION_SQL}
)
SELECT f.member_qname, f.member_label, f.val,
       f.period_end, w.source_accession, w.filed_at
  FROM instrument_dimensional_facts f
  JOIN winner w ON w.source_accession = f.source_accession
  JOIN target t ON t.period_end = f.period_end
 WHERE f.instrument_id = %(instrument_id)s
   AND f.axis = %(axis)s
   AND f.metric = %(metric)s
   AND NOT f.is_subtotal
   AND {_ANNUAL_DURATION_SQL}
 ORDER BY f.val DESC, f.member_qname
"""


def read_segments(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    axis: DimensionalAxis,
) -> SegmentsReadResult:
    """Latest-FY leaf rows for an axis, one merged row per member."""
    sources: dict[str, str] = {}
    period_end: date | None = None
    filed_at: datetime | None = None
    merged: dict[str, dict[str, Any]] = {}
    member_order: list[str] = []

    with conn.cursor(row_factory=dict_row) as cur:
        for metric in _METRICS:
            cur.execute(
                _METRIC_ROWS_SQL,
                {"instrument_id": instrument_id, "axis": axis, "metric": metric},
            )
            rows = cur.fetchall()
            if not rows:
                continue
            sources[metric] = rows[0]["source_accession"]
            # Revenue (always present when the axis has data, queried
            # first) pins the headline period; assets instants and
            # op-income durations from the same FY share it.
            if period_end is None:
                period_end = rows[0]["period_end"]
            if filed_at is None or rows[0]["filed_at"] > filed_at:
                filed_at = rows[0]["filed_at"]
            for row in rows:
                member = row["member_qname"]
                entry = merged.get(member)
                if entry is None:
                    entry = {
                        "member_qname": member,
                        "member_label": row["member_label"],
                        "revenue": None,
                        "operating_income": None,
                        "assets": None,
                    }
                    merged[member] = entry
                    member_order.append(member)
                entry[metric] = Decimal(row["val"]) if not isinstance(row["val"], Decimal) else row["val"]

    return SegmentsReadResult(
        axis=axis,
        period_end=period_end,
        filed_at=filed_at,
        sources=sources,
        rows=[merged[m] for m in member_order],
    )
