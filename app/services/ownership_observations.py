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
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

import psycopg
import psycopg.rows

# Pinned to the SEC series identifier shape ``S0000xxxxx``. Used by
# the fund-observation write-side guard (Codex pre-impl review #2 +
# #8) and asserted by the ``ownership_funds_observations`` /
# ``sec_fund_series`` CHECK constraints — this regex is the
# application-side mirror so a guard violation surfaces as a clean
# ``ValueError`` instead of a Postgres CHECK error rolling the
# whole transaction.
_FUND_SERIES_ID_RE = re.compile(r"^S[0-9]{9}$")

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
    """Diff-aware MERGE reconciler for ``ownership_insiders_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    UPDATE only when business cols IS DISTINCT FROM the new set; INSERT
    new rows; DELETE rows that fall out of the latest set (NOT MATCHED
    BY SOURCE scope-clamped to this instrument). ``refreshed_at`` is
    advanced on the UPDATE path only; the operator-visible drift
    watermark for repair-sweep lives in ``ownership_refresh_state``
    (§3.3 — separates write-side dead-tuple budget from watermark
    semantics so no-op refreshes do not trigger forever-loops in
    the repair sweep).

    Source-priority CASE chain preserved verbatim from the prior
    DELETE+INSERT implementation (Codex 1b HIGH-1: ``holder_identity_key``
    is a schema-generated PK column — appears in ON clause + INSERT col
    list + DISTINCT ON, but NEVER in diff tuple or UPDATE SET).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_insiders_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT MAX(ingested_at) FROM ownership_insiders_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_insiders_current AS tgt
            USING (
                SELECT DISTINCT ON (holder_identity_key, ownership_nature)
                    instrument_id, holder_cik, holder_name, holder_identity_key,
                    ownership_nature, source, source_document_id, source_accession,
                    source_url, filed_at, period_start, period_end, shares
                FROM ownership_insiders_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
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
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.holder_identity_key = src.holder_identity_key
               AND tgt.ownership_nature = src.ownership_nature
            WHEN MATCHED AND (
                tgt.holder_cik, tgt.holder_name,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares
            ) IS DISTINCT FROM (
                src.holder_cik, src.holder_name,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares
            ) THEN UPDATE SET
                holder_cik         = src.holder_cik,
                holder_name        = src.holder_name,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, holder_cik, holder_name, holder_identity_key,
                ownership_nature, source, source_document_id, source_accession,
                source_url, filed_at, period_start, period_end, shares
            ) VALUES (
                src.instrument_id, src.holder_cik, src.holder_name, src.holder_identity_key,
                src.ownership_nature, src.source, src.source_document_id, src.source_accession,
                src.source_url, src.filed_at, src.period_start, src.period_end, src.shares
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'insiders', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
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
    """Diff-aware MERGE reconciler for ``ownership_institutions_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    UPDATE only when business cols IS DISTINCT FROM the new set; INSERT
    new rows; DELETE rows that fall out of the latest set. ``refreshed_at``
    advanced on UPDATE path only. Drift watermark lives in
    ``ownership_refresh_state`` (§3.3).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
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
            "SELECT MAX(ingested_at) FROM ownership_institutions_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_institutions_current AS tgt
            USING (
                SELECT DISTINCT ON (filer_cik, ownership_nature, exposure_kind)
                    instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    shares, market_value_usd, voting_authority, exposure_kind
                FROM ownership_institutions_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
                ORDER BY
                    filer_cik,
                    ownership_nature,
                    exposure_kind,
                    period_end DESC,
                    filed_at DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.filer_cik = src.filer_cik
               AND tgt.ownership_nature = src.ownership_nature
               AND tgt.exposure_kind = src.exposure_kind
            WHEN MATCHED AND (
                tgt.filer_name, tgt.filer_type,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares, tgt.market_value_usd, tgt.voting_authority
            ) IS DISTINCT FROM (
                src.filer_name, src.filer_type,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.voting_authority
            ) THEN UPDATE SET
                filer_name         = src.filer_name,
                filer_type         = src.filer_type,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                market_value_usd   = src.market_value_usd,
                voting_authority   = src.voting_authority,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, market_value_usd, voting_authority, exposure_kind
            ) VALUES (
                src.instrument_id, src.filer_cik, src.filer_name, src.filer_type, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.voting_authority, src.exposure_kind
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'institutions', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
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
    """Diff-aware MERGE reconciler for ``ownership_blockholders_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    UPDATE only when business cols IS DISTINCT FROM the new set; INSERT
    new rows; DELETE rows that fall out of the latest set. ``refreshed_at``
    advanced on UPDATE path only. Drift watermark lives in
    ``ownership_refresh_state`` (§3.3).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
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
            "SELECT MAX(ingested_at) FROM ownership_blockholders_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_blockholders_current AS tgt
            USING (
                SELECT DISTINCT ON (reporter_cik, ownership_nature)
                    instrument_id, reporter_cik, reporter_name, ownership_nature,
                    submission_type, status_flag,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    aggregate_amount_owned, percent_of_class
                FROM ownership_blockholders_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
                ORDER BY
                    reporter_cik,
                    ownership_nature,
                    filed_at DESC,
                    period_end DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.reporter_cik = src.reporter_cik
               AND tgt.ownership_nature = src.ownership_nature
            WHEN MATCHED AND (
                tgt.reporter_name, tgt.submission_type, tgt.status_flag,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.aggregate_amount_owned, tgt.percent_of_class
            ) IS DISTINCT FROM (
                src.reporter_name, src.submission_type, src.status_flag,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.aggregate_amount_owned, src.percent_of_class
            ) THEN UPDATE SET
                reporter_name        = src.reporter_name,
                submission_type      = src.submission_type,
                status_flag          = src.status_flag,
                source               = src.source,
                source_document_id   = src.source_document_id,
                source_accession     = src.source_accession,
                source_url           = src.source_url,
                filed_at             = src.filed_at,
                period_start         = src.period_start,
                period_end           = src.period_end,
                aggregate_amount_owned = src.aggregate_amount_owned,
                percent_of_class     = src.percent_of_class,
                refreshed_at         = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, reporter_cik, reporter_name, ownership_nature,
                submission_type, status_flag,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                aggregate_amount_owned, percent_of_class
            ) VALUES (
                src.instrument_id, src.reporter_cik, src.reporter_name, src.ownership_nature,
                src.submission_type, src.status_flag,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.aggregate_amount_owned, src.percent_of_class
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'blockholders', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
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
    """Diff-aware MERGE reconciler for ``ownership_treasury_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    ``WHERE treasury_shares IS NOT NULL`` preserved — a NULL re-parse
    must not displace a prior non-null value. Single-column PK
    ``(instrument_id)`` — ON clause uses ``tgt.instrument_id =
    src.instrument_id`` (PG MERGE requires a column-to-column join
    condition; a bare constant predicate triggers FULL JOIN
    unsupported error). ``src.instrument_id`` is always ``%(iid)s``
    due to the USING WHERE clause so this is semantically equivalent.
    ``refreshed_at`` advanced on UPDATE path only. Drift watermark
    lives in ``ownership_refresh_state`` (§3.3).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
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
            "SELECT MAX(ingested_at) FROM ownership_treasury_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_treasury_current AS tgt
            USING (
                SELECT DISTINCT ON (instrument_id)
                    instrument_id, ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end, treasury_shares
                FROM ownership_treasury_observations
                WHERE instrument_id = %(iid)s
                  AND known_to IS NULL
                  AND treasury_shares IS NOT NULL
                ORDER BY
                    instrument_id,
                    period_end DESC,
                    filed_at DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = src.instrument_id
            WHEN MATCHED AND (
                tgt.ownership_nature,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.treasury_shares
            ) IS DISTINCT FROM (
                src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.treasury_shares
            ) THEN UPDATE SET
                ownership_nature   = src.ownership_nature,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                treasury_shares    = src.treasury_shares,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end, treasury_shares
            ) VALUES (
                src.instrument_id, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end, src.treasury_shares
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'treasury', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
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


_ESOP_HOLDER_NAME_SQL_REGEX = (
    r"\m(?:ESOP"
    r"|employee[[:space:]]+stock[[:space:]]+ownership[[:space:]]+plan"
    r"|401(?:[[:space:]]*\(?k\)?)?[[:space:]]+plan"
    r"|employee[[:space:]]+savings[[:space:]]+plan"
    r"|retirement[[:space:]]+savings[[:space:]]+plan"
    r"|profit[-[:space:]]sharing[[:space:]]+plan"
    r"|employee[[:space:]]+benefit[[:space:]]+plan"
    r"|company[[:space:]]+stock[[:space:]]+fund"
    r"|(?:savings|retirement|profit[-[:space:]]sharing)[[:space:]]+plan[[:space:]]+trust"
    r")\M"
)
"""SQL-side mirror of ``_ESOP_NAME_PATTERNS`` in
``app.providers.implementations.sec_def14a``. Used by
:func:`refresh_def14a_current` to filter out legacy ESOP-shape
observations that were ingested before the parser ESOP override
landed (#843). Without this defence, pre-existing observations with
``holder_role='principal'`` and an ESOP-pattern name would still
surface in the def14a slice alongside the new dedicated ESOP slice
— double-count. Codex pre-push review #843 round 3 caught this."""


def refresh_def14a_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Diff-aware MERGE reconciler for ``ownership_def14a_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    ESOP-shape rows excluded by 3-clause filter (shares IS NOT NULL +
    holder_role IS DISTINCT FROM 'esop' + holder_name !~* regex) —
    same semantics as the prior DELETE+INSERT implementation; regex
    bound via named placeholder ``%(esop_regex)s`` (psycopg3 cannot
    mix named + positional placeholders in one execute call — Codex 1b
    plan-rev2 MED-2).

    ``holder_name_key`` is a schema-generated PK column — appears in
    ON clause + INSERT col list + DISTINCT ON, but NEVER in diff tuple
    or UPDATE SET (Codex 1b plan-rev2 HIGH-1).

    ``refreshed_at`` advanced on UPDATE path only. Drift watermark
    lives in ``ownership_refresh_state`` (§3.3).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
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
            "SELECT MAX(ingested_at) FROM ownership_def14a_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_def14a_current AS tgt
            USING (
                SELECT DISTINCT ON (holder_name_key, ownership_nature)
                    instrument_id, holder_name, holder_name_key, holder_role, ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    shares, percent_of_class
                FROM ownership_def14a_observations
                WHERE instrument_id = %(iid)s
                  AND known_to IS NULL
                  AND shares IS NOT NULL
                  AND holder_role IS DISTINCT FROM 'esop'
                  AND holder_name !~* %(esop_regex)s
                ORDER BY
                    holder_name_key,
                    ownership_nature,
                    period_end DESC,
                    filed_at DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.holder_name_key = src.holder_name_key
               AND tgt.ownership_nature = src.ownership_nature
            WHEN MATCHED AND (
                tgt.holder_name, tgt.holder_role,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares, tgt.percent_of_class
            ) IS DISTINCT FROM (
                src.holder_name, src.holder_role,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.percent_of_class
            ) THEN UPDATE SET
                holder_name        = src.holder_name,
                holder_role        = src.holder_role,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                percent_of_class   = src.percent_of_class,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, holder_name, holder_name_key, holder_role, ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, percent_of_class
            ) VALUES (
                src.instrument_id, src.holder_name, src.holder_name_key, src.holder_role, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.percent_of_class
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id, "esop_regex": _ESOP_HOLDER_NAME_SQL_REGEX},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'def14a', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_def14a_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Funds — record + refresh (#917 — Phase 3 PR1, N-PORT)
# ---------------------------------------------------------------------------


def record_fund_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    fund_series_id: str,
    fund_series_name: str,
    fund_filer_cik: str,
    source_document_id: str,
    source_accession: str | None,
    source_field: str | None,
    source_url: str | None,
    filed_at: datetime,
    period_start: date | None,
    period_end: date,
    ingest_run_id: UUID,
    shares: Decimal,
    market_value_usd: Decimal | None,
    payoff_profile: str,
    asset_category: str,
) -> None:
    """Append one N-PORT fund-holding observation. Idempotent on
    ``(instrument_id, fund_series_id, period_end, source_document_id)``.

    Write-side guards (Codex pre-impl review #3, #4, #8 — moved into
    the helper so test seeders inherit the guards automatically per
    the prevention-log entry "Test seed mirrors must replicate
    production write-through guards"):

    * ``fund_series_id`` must match the SEC series-id regex. Filings
      missing a series_id are rejected upstream by the parser; this
      guard is the second-line catch.
    * ``asset_category`` must be ``'EC'`` (equity-common). N-PORT
      carries debt / preferred / derivative / cash positions in the
      same holdings array; only equity-common rows belong in the
      ownership decomposition.
    * ``payoff_profile`` must be ``'Long'``. A short fund position
      is a borrow artifact, not an ownership claim — it does NOT
      land in the ownership pie (per the spec §"Target chart
      decomposition").
    * ``shares`` must be positive. NULL / zero / negative are
      rejected at the helper boundary; the schema CHECK is the
      backstop.

    ``ownership_nature`` is fixed to ``'economic'`` and ``source`` to
    ``'nport'`` per the schema CHECK constraints — no per-call
    parameter so a buggy caller can't widen the CHECK by passing a
    different value.
    """
    if not _FUND_SERIES_ID_RE.match(fund_series_id):
        raise ValueError(
            f"record_fund_observation: invalid fund_series_id={fund_series_id!r} "
            "(expected SEC series identifier matching ^S[0-9]{9}$)"
        )
    if asset_category != "EC":
        raise ValueError(
            f"record_fund_observation: asset_category={asset_category!r} "
            "is not 'EC' (equity-common); refusing to record non-equity holding "
            "as ownership"
        )
    if payoff_profile != "Long":
        raise ValueError(
            f"record_fund_observation: payoff_profile={payoff_profile!r} "
            "is not 'Long'; short positions are memo overlays, not ownership "
            "rows (spec §Target chart decomposition)"
        )
    if shares is None or shares <= 0:
        raise ValueError(
            f"record_fund_observation: shares={shares!r} must be a positive "
            "Decimal — null/zero/negative are not ownership facts"
        )
    if not fund_filer_cik or not fund_filer_cik.strip():
        raise ValueError("record_fund_observation: fund_filer_cik is required")
    if not fund_series_name or not fund_series_name.strip():
        raise ValueError("record_fund_observation: fund_series_name is required")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_funds_observations (
                instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                ownership_nature,
                source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id,
                shares, market_value_usd, payoff_profile, asset_category
            ) VALUES (
                %(iid)s, %(sid)s, %(sname)s, %(fcik)s,
                'economic',
                'nport', %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s,
                %(shares)s, %(mv)s, %(payoff)s, %(asset)s
            )
            ON CONFLICT (instrument_id, fund_series_id, period_end, source_document_id)
            DO UPDATE SET
                fund_series_name = EXCLUDED.fund_series_name,
                fund_filer_cik = EXCLUDED.fund_filer_cik,
                source_accession = EXCLUDED.source_accession,
                source_field = EXCLUDED.source_field,
                source_url = EXCLUDED.source_url,
                filed_at = EXCLUDED.filed_at,
                period_start = EXCLUDED.period_start,
                shares = EXCLUDED.shares,
                market_value_usd = EXCLUDED.market_value_usd,
                payoff_profile = EXCLUDED.payoff_profile,
                asset_category = EXCLUDED.asset_category,
                ingest_run_id = EXCLUDED.ingest_run_id,
                ingested_at = clock_timestamp()
            """,
            {
                "iid": instrument_id,
                "sid": fund_series_id,
                "sname": fund_series_name,
                "fcik": fund_filer_cik.strip(),
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
                "payoff": payoff_profile,
                "asset": asset_category,
            },
        )


def refresh_funds_current(conn: psycopg.Connection[Any], *, instrument_id: int) -> int:
    """Diff-aware MERGE reconciler for ``ownership_funds_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    UPDATE only when business cols IS DISTINCT FROM the new set; INSERT
    new rows; DELETE rows that fall out of the latest set (NOT MATCHED
    BY SOURCE scope-clamped to this instrument via the ON clause AND
    the DELETE clause for defence-in-depth). ``refreshed_at`` is
    advanced on the UPDATE path only; the operator-visible drift
    watermark for repair-sweep lives in ``ownership_refresh_state``
    (§3.3 — separates write-side dead-tuple budget from watermark
    semantics so no-op refreshes do not trigger forever-loops in
    the repair sweep).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_funds_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT MAX(ingested_at) FROM ownership_funds_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_funds_current AS tgt
            USING (
                SELECT DISTINCT ON (fund_series_id)
                    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                    ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    shares, market_value_usd, payoff_profile, asset_category
                FROM ownership_funds_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
                ORDER BY
                    fund_series_id,
                    filed_at DESC,
                    period_end DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.fund_series_id = src.fund_series_id
            WHEN MATCHED AND (
                tgt.fund_series_name, tgt.fund_filer_cik, tgt.ownership_nature,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares, tgt.market_value_usd, tgt.payoff_profile, tgt.asset_category
            ) IS DISTINCT FROM (
                src.fund_series_name, src.fund_filer_cik, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.payoff_profile, src.asset_category
            ) THEN UPDATE SET
                fund_series_name   = src.fund_series_name,
                fund_filer_cik     = src.fund_filer_cik,
                ownership_nature   = src.ownership_nature,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                market_value_usd   = src.market_value_usd,
                payoff_profile     = src.payoff_profile,
                asset_category     = src.asset_category,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                ownership_nature, source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, market_value_usd, payoff_profile, asset_category
            ) VALUES (
                src.instrument_id, src.fund_series_id, src.fund_series_name, src.fund_filer_cik,
                src.ownership_nature, src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.payoff_profile, src.asset_category
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'funds', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_funds_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def record_esop_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    plan_name: str,
    plan_trustee_name: str | None,
    plan_trustee_cik: str | None,
    source_document_id: str,
    source_accession: str | None,
    source_field: str | None,
    source_url: str | None,
    filed_at: datetime,
    period_start: date | None,
    period_end: date,
    ingest_run_id: UUID,
    shares: Decimal,
    percent_of_class: Decimal | None,
) -> None:
    """Append one DEF-14A ESOP / employee-benefit-plan observation
    (#843). Idempotent on
    ``(instrument_id, plan_name, period_end, source_document_id)``.

    Write-side guards (mirror ``record_fund_observation`` shape):

    * ``shares`` must be positive — null/zero/negative are not
      ownership facts; the schema CHECK is the backstop.
    * ``plan_name`` must be non-empty after strip — the parser's
      plan_name extractor returns the canonicalised plan name; an
      empty value indicates a parser bug, not a legal-empty filing.

    ``ownership_nature`` is fixed to ``'beneficial'`` and ``source``
    to ``'def14a'`` per the schema CHECK constraints — no per-call
    parameter so a buggy caller can't widen the CHECK by passing a
    different value."""
    if shares is None or shares <= 0:
        raise ValueError(
            f"record_esop_observation: shares={shares!r} must be a positive "
            "Decimal — null/zero/negative are not ownership facts"
        )
    if not plan_name or not plan_name.strip():
        raise ValueError("record_esop_observation: plan_name is required (non-empty)")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ownership_esop_observations (
                instrument_id, plan_name, plan_trustee_name, plan_trustee_cik,
                ownership_nature,
                source, source_document_id, source_accession, source_field, source_url,
                filed_at, period_start, period_end, ingest_run_id,
                shares, percent_of_class
            ) VALUES (
                %(iid)s, %(plan)s, %(trustee)s, %(trustee_cik)s,
                'beneficial',
                'def14a', %(doc_id)s, %(accession)s, %(field)s, %(url)s,
                %(filed_at)s, %(period_start)s, %(period_end)s, %(run_id)s,
                %(shares)s, %(pct)s
            )
            ON CONFLICT (instrument_id, plan_name, period_end, source_document_id)
            DO UPDATE SET
                plan_trustee_name = EXCLUDED.plan_trustee_name,
                plan_trustee_cik = EXCLUDED.plan_trustee_cik,
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
                "plan": plan_name.strip(),
                "trustee": plan_trustee_name.strip() if plan_trustee_name else None,
                "trustee_cik": plan_trustee_cik.strip() if plan_trustee_cik else None,
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


def refresh_esop_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> int:
    """Diff-aware MERGE reconciler for ``ownership_esop_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    UPDATE only when business cols IS DISTINCT FROM the new set; INSERT
    new rows; DELETE rows that fall out of the latest set. ``refreshed_at``
    advanced on UPDATE path only. Drift watermark lives in
    ``ownership_refresh_state`` (§3.3).

    Dedup picks one row per ``plan_name`` ordered by
    ``filed_at DESC, period_end DESC, source_document_id ASC`` —
    DEF 14A amendments (DEFA14A) carry the same period_end as the
    original DEF 14A but are filed later, so ordering by
    ``filed_at DESC`` first ensures the amendment wins.

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_esop_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT MAX(ingested_at) FROM ownership_esop_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_esop_current AS tgt
            USING (
                SELECT DISTINCT ON (plan_name)
                    instrument_id, plan_name, plan_trustee_name, plan_trustee_cik,
                    ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    shares, percent_of_class
                FROM ownership_esop_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
                ORDER BY
                    plan_name,
                    filed_at DESC,
                    period_end DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.plan_name = src.plan_name
            WHEN MATCHED AND (
                tgt.plan_trustee_name, tgt.plan_trustee_cik, tgt.ownership_nature,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares, tgt.percent_of_class
            ) IS DISTINCT FROM (
                src.plan_trustee_name, src.plan_trustee_cik, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.percent_of_class
            ) THEN UPDATE SET
                plan_trustee_name  = src.plan_trustee_name,
                plan_trustee_cik   = src.plan_trustee_cik,
                ownership_nature   = src.ownership_nature,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                percent_of_class   = src.percent_of_class,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, plan_name, plan_trustee_name, plan_trustee_cik,
                ownership_nature,
                source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, percent_of_class
            ) VALUES (
                src.instrument_id, src.plan_name, src.plan_trustee_name, src.plan_trustee_cik,
                src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.percent_of_class
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'esop', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_esop_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def upsert_sec_fund_series(
    conn: psycopg.Connection[Any],
    *,
    fund_series_id: str,
    fund_series_name: str,
    fund_filer_cik: str,
    last_seen_period_end: date | None,
) -> None:
    """Idempotent upsert into ``sec_fund_series`` reference table.

    Called once per ingested N-PORT accession. ``last_seen_period_end``
    is monotonically advanced via ``GREATEST`` so an out-of-order
    re-ingest of an older filing doesn't regress the value. Series
    name is refreshed unconditionally — the most recent ingest wins
    so a fund rename propagates."""
    if not _FUND_SERIES_ID_RE.match(fund_series_id):
        raise ValueError(f"upsert_sec_fund_series: invalid fund_series_id={fund_series_id!r}")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_fund_series (
                fund_series_id, fund_series_name, fund_filer_cik,
                last_seen_period_end
            ) VALUES (
                %(sid)s, %(sname)s, %(fcik)s, %(period)s
            )
            ON CONFLICT (fund_series_id) DO UPDATE SET
                fund_series_name = EXCLUDED.fund_series_name,
                fund_filer_cik = EXCLUDED.fund_filer_cik,
                last_seen_period_end = GREATEST(
                    COALESCE(sec_fund_series.last_seen_period_end, '1900-01-01'),
                    COALESCE(EXCLUDED.last_seen_period_end, '1900-01-01')
                ),
                updated_at = NOW()
            """,
            {
                "sid": fund_series_id,
                "sname": fund_series_name,
                "fcik": fund_filer_cik.strip(),
                "period": last_seen_period_end,
            },
        )


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
