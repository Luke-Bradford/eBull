"""Ownership observations + current refresh (#840 P1 Phase 1).

Two-layer ownership storage per the spec at
``docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md``
(Phase 1, §Data model design):

- **Layer 1 — observations** (per-category, append-only, immutable):
  ``ownership_<category>_observations``. Every ingested filing fact
  lands here with the full provenance block. Source of truth for
  history queries. Partitioned by ``period_end`` quarterly.
- **Layer 2 — _current** (per-category, mutable, deterministic):
  ``ownership_<category>_current``. Rebuilt by
  ``refresh_<category>_current(instrument_id)`` under a per-instrument
  Postgres advisory lock so concurrent refreshes cannot race (Codex
  plan-review finding #3). Read by the rollup endpoint after #840.E.

Two-axis dedup model:
  1. ``source`` priority chain — form4 > form3 > 13d > 13g > def14a > 13f > nport > ncsr.
  2. ``ownership_nature`` enum — direct | indirect | beneficial | voting | economic.

Dedup is applied ONLY within compatible natures. Cohen's GME 13D/A
(beneficial 75M) and his Form 4 (direct 38M) BOTH render under the
new model.

This is sub-PR A — foundation + insiders only. Institutions (#840.B),
blockholders (#840.C), and treasury+def14a (#840.D) follow the same
pattern in subsequent sub-PRs.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)


OwnershipNature = Literal["direct", "indirect", "beneficial", "voting", "economic"]
OwnershipSource = Literal[
    "form4", "form3", "13d", "13g", "def14a", "13f", "nport", "ncsr", "xbrl_dei", "10k_note", "finra_si", "derived"
]


# Source priority chain (lower = higher priority within same nature).
# Pinned here so a future sub-PR can't drift the chain across categories.
_SOURCE_PRIORITY: dict[OwnershipSource, int] = {
    "form4": 1,
    "form3": 2,
    "13d": 3,
    "13g": 3,
    "def14a": 4,
    "13f": 5,
    "nport": 6,
    "ncsr": 6,
    "xbrl_dei": 7,
    "10k_note": 8,
    "finra_si": 9,
    "derived": 10,
}


@dataclass(frozen=True)
class InsiderObservation:
    """Public dataclass mirroring one ``ownership_insiders_observations``
    row. Used by the round-trip helpers and tests; the writer takes
    discrete kwargs to keep the call site readable at the ingest
    boundary."""

    instrument_id: int
    holder_cik: str
    holder_name: str
    ownership_nature: OwnershipNature
    source: OwnershipSource
    source_document_id: str
    source_accession: str | None
    source_field: str | None
    source_url: str | None
    filed_at: datetime
    period_start: date | None
    period_end: date
    known_from: datetime
    known_to: datetime | None
    ingest_run_id: UUID
    shares: Decimal | None


# ---------------------------------------------------------------------------
# Insiders — record + refresh
# ---------------------------------------------------------------------------


def record_insider_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    holder_cik: str | None,
    holder_name: str,
    ownership_nature: OwnershipNature,
    source: OwnershipSource,
    source_document_id: str,
    source_accession: str | None,
    source_field: str | None,
    source_url: str | None,
    filed_at: datetime,
    period_start: date | None,
    period_end: date,
    ingest_run_id: UUID,
    shares: Decimal | None,
) -> None:
    """Append one observation. Idempotent on the natural key
    ``(instrument_id, holder_cik, ownership_nature, source, source_document_id, period_end)``
    via ON CONFLICT DO UPDATE — re-running an ingest on the same
    accession refreshes the row in place rather than appending a
    duplicate.

    The legacy ingester paths (insider_transactions, insider_initial_holdings)
    call this on every successful upsert so ``_current`` stays
    refreshable on demand. Backfill is the one-shot retro version of
    the same path (see ``scripts/backfill_840_insiders.py``)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_insiders_observations (
                instrument_id, holder_cik, holder_name, ownership_nature,
                source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id, shares
            ) VALUES (
                %(iid)s, %(cik)s, %(name)s, %(nature)s,
                %(source)s, %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s, %(shares)s
            )
            ON CONFLICT (instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)
            DO UPDATE SET
                holder_name = EXCLUDED.holder_name,
                source_accession = EXCLUDED.source_accession,
                source_field = EXCLUDED.source_field,
                source_url = EXCLUDED.source_url,
                filed_at = EXCLUDED.filed_at,
                period_start = EXCLUDED.period_start,
                shares = EXCLUDED.shares,
                ingest_run_id = EXCLUDED.ingest_run_id
            """,
            {
                "iid": instrument_id,
                "cik": holder_cik,
                "name": holder_name,
                "nature": ownership_nature,
                "source": source,
                "doc_id": source_document_id,
                "accession": source_accession,
                "field": source_field,
                "url": source_url,
                "filed_at": filed_at,
                "period_start": period_start,
                "period_end": period_end,
                "run_id": str(ingest_run_id),
                "shares": shares,
            },
        )


def refresh_insiders_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Deterministically rebuild ``ownership_insiders_current`` rows
    for one instrument from its observations.

    Atomicity (Codex plan-review finding #3): wrapped in a single
    transaction with a per-instrument ``pg_advisory_xact_lock`` so
    concurrent refreshes against the same instrument serialise. The
    UNIQUE PK on ``(instrument_id, holder_cik, ownership_nature)``
    is the second-line guard — if the lock is ever bypassed, a
    duplicate INSERT trips the constraint loudly.

    Dedup logic (per the two-axis spec): for each
    ``(holder_cik, ownership_nature)`` group, pick the highest-priority
    observation by ``source_priority ASC, period_end DESC, filed_at DESC``.
    Cross-nature observations NEVER dedup against each other —
    Cohen's direct Form 4 and beneficial 13D/A both produce ``_current``
    rows.

    Returns the number of ``_current`` rows after the refresh."""
    # Codex review for #840.A: explicit ``with conn.transaction()``
    # so the advisory lock and the DELETE/INSERT pair share one
    # transaction even if the caller has set ``conn.autocommit=True``
    # — without this, ``pg_advisory_xact_lock`` would release after
    # the SELECT and the DELETE/INSERT would land in separate
    # auto-committed transactions, reopening the race the lock is
    # meant to close.
    with conn.transaction(), conn.cursor() as cur:
        # Per-instrument advisory lock keyed on a stable hash of the
        # function name # instrument_id. ``pg_advisory_xact_lock`` is
        # transaction-scoped — released automatically at COMMIT/ROLLBACK.
        # The 2-arg form takes (int4, int4) and the 1-arg form takes
        # int8. Use the 1-arg form fed by a 64-bit composite (function
        # namespace hash XOR instrument_id) so both halves contribute
        # uniqueness and collisions across functions / instruments are
        # negligible. ``hashtextextended`` returns int8 directly.
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_insiders_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )

        # Replace-then-insert: clear stale rows for the instrument and
        # rebuild from observations under the same transaction.
        cur.execute(
            "DELETE FROM ownership_insiders_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        # DISTINCT ON ordering implements the source-priority chain.
        # Codex review for #840.A: deterministic final tie-breakers
        # (source, source_document_id) so equal-priority pairs (13d
        # vs 13g; same accession refiled) don't pick
        # nondeterministically across refresh runs.
        cur.execute(
            """
            INSERT INTO ownership_insiders_current (
                instrument_id, holder_cik, holder_name, holder_identity_key,
                ownership_nature, source, source_document_id, source_accession,
                source_url, filed_at, period_start, period_end, shares
            )
            SELECT DISTINCT ON (holder_identity_key, ownership_nature)
                instrument_id, holder_cik, holder_name, holder_identity_key,
                ownership_nature, source, source_document_id, source_accession,
                source_url, filed_at, period_start, period_end, shares
            FROM ownership_insiders_observations
            WHERE instrument_id = %s
              AND known_to IS NULL
            ORDER BY
                holder_identity_key,
                ownership_nature,
                CASE source
                    WHEN 'form4'    THEN 1
                    WHEN 'form3'    THEN 2
                    WHEN '13d'      THEN 3
                    WHEN '13g'      THEN 3
                    WHEN 'def14a'   THEN 4
                    WHEN '13f'      THEN 5
                    WHEN 'nport'    THEN 6
                    WHEN 'ncsr'     THEN 6
                    WHEN 'xbrl_dei' THEN 7
                    WHEN '10k_note' THEN 8
                    WHEN 'finra_si' THEN 9
                    ELSE 10
                END ASC,
                period_end DESC,
                filed_at DESC,
                source ASC,
                source_document_id ASC
            """,
            (instrument_id,),
        )

        cur.execute(
            "SELECT COUNT(*) FROM ownership_insiders_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def iter_insider_observations(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    holder_cik: str | None = None,
    limit: int = 1000,
) -> Iterator[dict[str, Any]]:
    """Yield observations for one instrument (and optional holder).
    Used by the history endpoint (#840.F) and by tests."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if holder_cik is not None:
            cur.execute(
                """
                SELECT instrument_id, holder_cik, holder_name, ownership_nature,
                       source, source_document_id, source_accession, source_url,
                       filed_at, period_start, period_end, known_from, known_to,
                       shares
                FROM ownership_insiders_observations
                WHERE instrument_id = %s AND holder_cik = %s
                ORDER BY period_end DESC, filed_at DESC
                LIMIT %s
                """,
                (instrument_id, holder_cik, limit),
            )
        else:
            cur.execute(
                """
                SELECT instrument_id, holder_cik, holder_name, ownership_nature,
                       source, source_document_id, source_accession, source_url,
                       filed_at, period_start, period_end, known_from, known_to,
                       shares
                FROM ownership_insiders_observations
                WHERE instrument_id = %s
                ORDER BY period_end DESC, filed_at DESC
                LIMIT %s
                """,
                (instrument_id, limit),
            )
        for row in cur.fetchall():
            yield dict(row)
