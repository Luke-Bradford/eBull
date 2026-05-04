"""Self-healing ownership-observations repair sweep (#873).

Spec §"Eliminate periodic re-scan jobs"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

Replaces the legacy ``ownership_observations_sync`` job. Runs weekly
(or on-demand) and ONLY against rows where the per-instrument
``ownership_*_current`` snapshot is staler than the per-instrument
max(ingested_at) of the corresponding observations partition. On a
healthy install this finds zero rows and exits in <100ms.

Predicate per category:

    WHERE c.refreshed_at < (
        SELECT MAX(o.ingested_at)
        FROM ownership_<category>_observations o
        WHERE o.instrument_id = c.instrument_id
    )

Note ``ingested_at`` is system-time (advances on every UPSERT
including DO UPDATE; #864 migration 119), distinct from valid-time
``known_from`` which doesn't advance on a re-ingest of the same
accession or a parser-version rewash.

Migration toward write-through (the rest of #873):

  - Each per-form ingester (def14a_ingest, sec_form4_ingest,
    institutional_holdings_ingest, blockholder_filings_ingest,
    treasury XBRL projection) wires a ``record_*_observation`` call
    + an immediate ``refresh_*_current(instrument_id)`` call inline.
  - The legacy nightly ``ownership_observations_sync`` job is
    retired in favour of this repair sweep.
  - Per-ingester write-through wiring is a sequence of follow-up
    PRs (873.A insiders, 873.B institutions, 873.C blockholders,
    873.D treasury+def14a) — too much blast radius for one PR.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from app.services.ownership_observations import (
    refresh_blockholders_current,
    refresh_def14a_current,
    refresh_insiders_current,
    refresh_institutions_current,
    refresh_treasury_current,
)

logger = logging.getLogger(__name__)


# Per-category (current_table, observations_table, refresh_callable).
# Pinned here so adding a new category means one edit, not a sweep.
_CATEGORIES: list[tuple[str, str, Callable[[psycopg.Connection[Any], int], int]]] = [
    (
        "ownership_insiders_current",
        "ownership_insiders_observations",
        lambda c, i: refresh_insiders_current(c, instrument_id=i),
    ),
    (
        "ownership_institutions_current",
        "ownership_institutions_observations",
        lambda c, i: refresh_institutions_current(c, instrument_id=i),
    ),
    (
        "ownership_blockholders_current",
        "ownership_blockholders_observations",
        lambda c, i: refresh_blockholders_current(c, instrument_id=i),
    ),
    (
        "ownership_treasury_current",
        "ownership_treasury_observations",
        lambda c, i: refresh_treasury_current(c, instrument_id=i),
    ),
    (
        "ownership_def14a_current",
        "ownership_def14a_observations",
        lambda c, i: refresh_def14a_current(c, instrument_id=i),
    ),
]


@dataclass(frozen=True)
class CategoryRepairStats:
    category: str
    drifted_instruments: int
    refreshed_rows: int


@dataclass(frozen=True)
class RepairSweepStats:
    per_category: list[CategoryRepairStats]

    @property
    def total_drifted(self) -> int:
        return sum(c.drifted_instruments for c in self.per_category)


def _drifted_instruments(conn: psycopg.Connection[Any], current_table: str, observations_table: str) -> list[int]:
    """Return instrument_ids whose _current is staler than the observations
    max(ingested_at) for that instrument. Empty on a healthy install."""
    # Both table names are module-local literals — never user input.
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT c.instrument_id
            FROM {current_table} c
            WHERE c.refreshed_at < (
                SELECT MAX(o.ingested_at)
                FROM {observations_table} o
                WHERE o.instrument_id = c.instrument_id
            )
            GROUP BY c.instrument_id
            """  # noqa: S608 — table names are literals
        )
        return [int(row[0]) for row in cur.fetchall()]


def run_observations_repair_sweep(
    conn: psycopg.Connection[Any],
) -> RepairSweepStats:
    """Repair-sweep tick: find drifted instruments per category and
    refresh _current for each. On a healthy install this is a series
    of MAX-vs-MAX comparisons that returns zero drifted rows; the
    expensive ``refresh_*_current`` work only runs when actual drift
    exists.
    """
    per_category: list[CategoryRepairStats] = []
    for current_table, observations_table, refresh_fn in _CATEGORIES:
        drifted = _drifted_instruments(conn, current_table, observations_table)
        refreshed_rows = 0
        for instrument_id in drifted:
            try:
                refreshed_rows += refresh_fn(conn, instrument_id)
            except Exception as exc:
                logger.warning(
                    "repair sweep: refresh failed category=%s instrument_id=%d: %s",
                    current_table,
                    instrument_id,
                    exc,
                )
        per_category.append(
            CategoryRepairStats(
                category=current_table,
                drifted_instruments=len(drifted),
                refreshed_rows=refreshed_rows,
            )
        )
        logger.info(
            "repair sweep %s: drifted=%d refreshed_rows=%d",
            current_table,
            len(drifted),
            refreshed_rows,
        )

    stats = RepairSweepStats(per_category=per_category)
    logger.info("repair sweep total drifted instruments: %d", stats.total_drifted)
    return stats
