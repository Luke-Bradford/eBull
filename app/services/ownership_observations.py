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
                ingest_run_id = EXCLUDED.ingest_run_id,
                ingested_at = clock_timestamp()
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


# ---------------------------------------------------------------------------
# Institutions — record + refresh (#840.B)
# ---------------------------------------------------------------------------


def record_institution_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    filer_type: str | None,
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
    market_value_usd: Decimal | None,
    voting_authority: str | None,
    exposure_kind: Literal["EQUITY", "PUT", "CALL"] = "EQUITY",
) -> None:
    """Append one institution observation. Idempotent on
    ``(instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind)``.

    ``exposure_kind`` (Codex review for #840.B): 13F-HR can carry up
    to three legal rows per ``(accession, instrument)`` — equity, PUT,
    CALL. Pass ``'EQUITY'`` (default) for the standard equity position;
    ``'PUT'`` / ``'CALL'`` for option exposure rows. Without this axis,
    ON CONFLICT would collapse the three legal rows into one.

    Identity contract (Codex plan-review finding #2): the legacy
    ``institutional_holdings`` table joins to ``institutional_filers``
    via ``filer_id``. Backfill (#840.E-prep) MUST resolve filer_id →
    cik before calling this helper. ``filer_cik`` is the canonical
    identity in the new model — orphans (filer_id with no
    institutional_filers row) must be rejected at the call site, not
    silently dropped here.

    ``ownership_nature`` for 13F-HR: pass ``'economic'`` for the
    full reported position. ``'voting'`` is reserved for an explicit
    voting-authority overlay row that operator UI gains in a future
    phase. Mapping pinned here so the per-source default is
    consistent across the codebase."""
    if filer_cik is None or not filer_cik.strip():
        raise ValueError("record_institution_observation: filer_cik is required")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_institutions_observations (
                instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
                source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id,
                shares, market_value_usd, voting_authority, exposure_kind
            ) VALUES (
                %(iid)s, %(cik)s, %(name)s, %(ftype)s, %(nature)s,
                %(source)s, %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s,
                %(shares)s, %(mv)s, %(voting)s, %(exp)s
            )
            ON CONFLICT (instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind)
            DO UPDATE SET
                filer_name = EXCLUDED.filer_name,
                filer_type = EXCLUDED.filer_type,
                source_accession = EXCLUDED.source_accession,
                source_field = EXCLUDED.source_field,
                source_url = EXCLUDED.source_url,
                filed_at = EXCLUDED.filed_at,
                period_start = EXCLUDED.period_start,
                shares = EXCLUDED.shares,
                market_value_usd = EXCLUDED.market_value_usd,
                voting_authority = EXCLUDED.voting_authority,
                ingest_run_id = EXCLUDED.ingest_run_id,
                ingested_at = clock_timestamp()
            """,
            {
                "iid": instrument_id,
                "cik": filer_cik.strip(),
                "name": filer_name,
                "ftype": filer_type,
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
                "mv": market_value_usd,
                "voting": voting_authority,
                "exp": exposure_kind,
            },
        )


def refresh_institutions_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Deterministically rebuild ``ownership_institutions_current``
    rows for one instrument.

    Same atomicity contract as ``refresh_insiders_current``: explicit
    transaction + per-instrument advisory lock. Dedup picks the latest
    ``period_end`` per ``(filer_cik, ownership_nature)`` — within 13F
    there's no cross-source priority chain to apply (13F is the only
    source today; nport/ncsr in Phase 3 will need the chain). Final
    deterministic tie-breakers: ``filed_at DESC, source_document_id ASC``."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_institutions_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "DELETE FROM ownership_institutions_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        cur.execute(
            """
            INSERT INTO ownership_institutions_current (
                instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, market_value_usd, voting_authority, exposure_kind
            )
            SELECT DISTINCT ON (filer_cik, ownership_nature, exposure_kind)
                instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, market_value_usd, voting_authority, exposure_kind
            FROM ownership_institutions_observations
            WHERE instrument_id = %s
              AND known_to IS NULL
            ORDER BY
                filer_cik,
                ownership_nature,
                exposure_kind,
                period_end DESC,
                filed_at DESC,
                source_document_id ASC
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_institutions_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def resolve_filer_cik_or_raise(
    conn: psycopg.Connection[Any],
    *,
    filer_id: int,
) -> tuple[str, str, str | None]:
    """Resolve a legacy ``institutional_holdings.filer_id`` to
    ``(cik, name, filer_type)`` for the new observations API.

    Codex plan-review finding #2: backfill MUST validate parent rows
    exist before recording observations. Raises ``ValueError`` on an
    orphan filer_id so the operator sees a loud failure rather than a
    silent drop. Use this as the single resolution path so behaviour
    is consistent across backfill + live ingester write-through."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik, name, filer_type FROM institutional_filers WHERE filer_id = %s",
            (filer_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"institutional_filers row missing for filer_id={filer_id}; refusing to drop holding silently")
    cik = str(row[0])
    if not cik.strip():
        raise ValueError(f"institutional_filers.cik is empty for filer_id={filer_id}")
    return cik, str(row[1]), (str(row[2]) if row[2] is not None else None)


# ---------------------------------------------------------------------------
# Blockholders — record + refresh (#840.C)
# ---------------------------------------------------------------------------


def record_blockholder_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    reporter_cik: str,
    reporter_name: str,
    ownership_nature: OwnershipNature,
    submission_type: str,
    status_flag: str | None,
    source: OwnershipSource,
    source_document_id: str,
    source_accession: str | None,
    source_field: str | None,
    source_url: str | None,
    filed_at: datetime,
    period_start: date | None,
    period_end: date,
    ingest_run_id: UUID,
    aggregate_amount_owned: Decimal | None,
    percent_of_class: Decimal | None,
) -> None:
    """Append one 13D/G blockholder observation. Idempotent on the
    natural key.

    Identity (per #837 lesson): ``reporter_cik`` here is the PRIMARY
    filer (``blockholder_filers.cik``), NOT the per-row joint
    reporter. Backfill / write-through MUST resolve the primary
    filer first; joint reporters on the same accession collapse to
    one observation row per the SEC convention that joint filers
    claim the same beneficial ownership."""
    if reporter_cik is None or not reporter_cik.strip():
        raise ValueError("record_blockholder_observation: reporter_cik is required")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_blockholders_observations (
                instrument_id, reporter_cik, reporter_name, ownership_nature,
                submission_type, status_flag,
                source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id,
                aggregate_amount_owned, percent_of_class
            ) VALUES (
                %(iid)s, %(cik)s, %(name)s, %(nature)s,
                %(stype)s, %(sflag)s,
                %(source)s, %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s,
                %(amount)s, %(pct)s
            )
            ON CONFLICT (instrument_id, reporter_cik, ownership_nature, source, source_document_id, period_end)
            DO UPDATE SET
                reporter_name = EXCLUDED.reporter_name,
                submission_type = EXCLUDED.submission_type,
                status_flag = EXCLUDED.status_flag,
                source_accession = EXCLUDED.source_accession,
                source_field = EXCLUDED.source_field,
                source_url = EXCLUDED.source_url,
                filed_at = EXCLUDED.filed_at,
                period_start = EXCLUDED.period_start,
                aggregate_amount_owned = EXCLUDED.aggregate_amount_owned,
                percent_of_class = EXCLUDED.percent_of_class,
                ingest_run_id = EXCLUDED.ingest_run_id,
                ingested_at = clock_timestamp()
            """,
            {
                "iid": instrument_id,
                "cik": reporter_cik.strip(),
                "name": reporter_name,
                "nature": ownership_nature,
                "stype": submission_type,
                "sflag": status_flag,
                "source": source,
                "doc_id": source_document_id,
                "accession": source_accession,
                "field": source_field,
                "url": source_url,
                "filed_at": filed_at,
                "period_start": period_start,
                "period_end": period_end,
                "run_id": str(ingest_run_id),
                "amount": aggregate_amount_owned,
                "pct": percent_of_class,
            },
        )


def refresh_blockholders_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Deterministically rebuild ``ownership_blockholders_current``.

    Picks latest amendment per ``(reporter_cik, ownership_nature)``
    by ``filed_at DESC, period_end DESC``. Same atomicity contract as
    the other refresh helpers."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_blockholders_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "DELETE FROM ownership_blockholders_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        cur.execute(
            """
            INSERT INTO ownership_blockholders_current (
                instrument_id, reporter_cik, reporter_name, ownership_nature,
                submission_type, status_flag,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                aggregate_amount_owned, percent_of_class
            )
            SELECT DISTINCT ON (reporter_cik, ownership_nature)
                instrument_id, reporter_cik, reporter_name, ownership_nature,
                submission_type, status_flag,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                aggregate_amount_owned, percent_of_class
            FROM ownership_blockholders_observations
            WHERE instrument_id = %s
              AND known_to IS NULL
            ORDER BY
                reporter_cik,
                ownership_nature,
                filed_at DESC,
                period_end DESC,
                source_document_id ASC
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_blockholders_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Treasury — record + refresh (#840.D)
# ---------------------------------------------------------------------------


def record_treasury_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source: OwnershipSource,
    source_document_id: str,
    source_accession: str | None,
    source_field: str | None,
    source_url: str | None,
    filed_at: datetime,
    period_start: date | None,
    period_end: date,
    ingest_run_id: UUID,
    treasury_shares: Decimal | None,
) -> None:
    """Append one treasury observation. ``ownership_nature`` is fixed
    to ``'economic'`` (issuer-held shares — not Rule 13d-3 beneficial).
    Source is typically ``'xbrl_dei'`` (TreasuryStockShares /
    TreasuryStockCommonShares); ``'10k_note'`` for narrative
    fallbacks."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_treasury_observations (
                instrument_id, source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id, treasury_shares
            ) VALUES (
                %(iid)s, %(source)s, %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s, %(shares)s
            )
            ON CONFLICT (instrument_id, period_end, source_document_id)
            DO UPDATE SET
                source = EXCLUDED.source,
                source_accession = EXCLUDED.source_accession,
                source_field = EXCLUDED.source_field,
                source_url = EXCLUDED.source_url,
                filed_at = EXCLUDED.filed_at,
                period_start = EXCLUDED.period_start,
                treasury_shares = EXCLUDED.treasury_shares,
                ingest_run_id = EXCLUDED.ingest_run_id,
                ingested_at = clock_timestamp()
            """,
            {
                "iid": instrument_id,
                "source": source,
                "doc_id": source_document_id,
                "accession": source_accession,
                "field": source_field,
                "url": source_url,
                "filed_at": filed_at,
                "period_start": period_start,
                "period_end": period_end,
                "run_id": str(ingest_run_id),
                "shares": treasury_shares,
            },
        )


def refresh_treasury_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Latest non-null ``treasury_shares`` per instrument wins.
    ``WHERE treasury_shares IS NOT NULL`` so a NULL observation
    doesn't displace an earlier non-null value (e.g. a re-parse
    that lost the concept shouldn't blank out the column)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_treasury_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "DELETE FROM ownership_treasury_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        cur.execute(
            """
            INSERT INTO ownership_treasury_current (
                instrument_id, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end, treasury_shares
            )
            SELECT DISTINCT ON (instrument_id)
                instrument_id, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end, treasury_shares
            FROM ownership_treasury_observations
            WHERE instrument_id = %s
              AND known_to IS NULL
              AND treasury_shares IS NOT NULL
            ORDER BY
                instrument_id,
                period_end DESC,
                filed_at DESC,
                source_document_id ASC
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_treasury_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# DEF 14A — record + refresh (#840.D)
# ---------------------------------------------------------------------------


def record_def14a_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    holder_name: str,
    holder_role: str | None,
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
    percent_of_class: Decimal | None,
) -> None:
    """Append one DEF 14A bene-table observation. Identity is the
    normalised holder name (generated column ``holder_name_key`` =
    lower(trim(holder_name))) — DEF 14A doesn't carry CIK on the
    proxy so the resolver-to-CIK match happens at rollup-read time
    instead of here."""
    if not holder_name or not holder_name.strip():
        raise ValueError("record_def14a_observation: holder_name is required")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_def14a_observations (
                instrument_id, holder_name, holder_role, ownership_nature,
                source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id,
                shares, percent_of_class
            ) VALUES (
                %(iid)s, %(name)s, %(role)s, %(nature)s,
                %(source)s, %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s,
                %(shares)s, %(pct)s
            )
            ON CONFLICT (instrument_id, holder_name_key, ownership_nature, period_end, source_document_id)
            DO UPDATE SET
                holder_name = EXCLUDED.holder_name,
                holder_role = EXCLUDED.holder_role,
                source_accession = EXCLUDED.source_accession,
                source_field = EXCLUDED.source_field,
                source_url = EXCLUDED.source_url,
                filed_at = EXCLUDED.filed_at,
                period_start = EXCLUDED.period_start,
                shares = EXCLUDED.shares,
                percent_of_class = EXCLUDED.percent_of_class,
                ingest_run_id = EXCLUDED.ingest_run_id,
                ingested_at = clock_timestamp()
            """,
            {
                "iid": instrument_id,
                "name": holder_name,
                "role": holder_role,
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
                "pct": percent_of_class,
            },
        )


def refresh_def14a_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Latest proxy per (instrument, normalised holder name) wins."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_def14a_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "DELETE FROM ownership_def14a_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        cur.execute(
            """
            INSERT INTO ownership_def14a_current (
                instrument_id, holder_name, holder_name_key, holder_role, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, percent_of_class
            )
            SELECT DISTINCT ON (holder_name_key, ownership_nature)
                instrument_id, holder_name, holder_name_key, holder_role, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, percent_of_class
            FROM ownership_def14a_observations
            WHERE instrument_id = %s
              AND known_to IS NULL
              AND shares IS NOT NULL  -- prevent NULL re-parse displacing prior good value
            ORDER BY
                holder_name_key,
                ownership_nature,
                period_end DESC,
                filed_at DESC,
                source_document_id ASC
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_def14a_current WHERE instrument_id = %s",
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
