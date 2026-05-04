"""Atom feed fast-lane discovery job (#867).

Issue #867 / spec §"Layer 1 — getcurrent Atom feed (every 5 min)".

Reads the getcurrent Atom feed once per cycle, filters to the
in-universe set (CIKs already known to the freshness scheduler),
then UPSERTs ``sec_filing_manifest`` rows for matches. The
manifest worker (#869) picks up the new pending rows.

Fast lane semantics: low latency (5-min cycle, near-realtime),
NOT lossless. The daily-index reconciliation job (#868) is the
safety net.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from app.providers.implementations.sec_getcurrent import HttpGet, read_getcurrent
from app.services.sec_manifest import record_manifest_entry

logger = logging.getLogger(__name__)


# Resolver: cik -> (subject_type, subject_id, instrument_id) | None.
# The job needs to know whether a discovered CIK matches an issuer
# in our universe, an institutional filer we track, or a blockholder
# filer. Provided via dependency injection so tests can pass a fake
# without seeding the four candidate tables.
SubjectResolver = Callable[
    [psycopg.Connection[Any], str],
    "ResolvedSubject | None",
]


@dataclass(frozen=True)
class ResolvedSubject:
    subject_type: str  # 'issuer' | 'institutional_filer' | 'blockholder_filer'
    subject_id: str
    instrument_id: int | None


@dataclass(frozen=True)
class AtomLaneStats:
    feed_rows: int
    matched_in_universe: int
    upserted: int
    skipped_unmapped_form: int
    skipped_unknown_subject: int


def default_subject_resolver(conn: psycopg.Connection[Any], cik: str) -> ResolvedSubject | None:
    """Resolve a CIK against the universe.

    Order:
      1. Issuer? — instrument_sec_profile.cik
      2. Institutional filer? — institutional_filers.cik
      3. Blockholder filer? — blockholder_filers.cik
      4. None (out of universe; skip)
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id FROM instrument_sec_profile WHERE cik = %s LIMIT 1",
            (cik,),
        )
        row = cur.fetchone()
        if row is not None:
            return ResolvedSubject(
                subject_type="issuer",
                subject_id=str(int(row[0])),
                instrument_id=int(row[0]),
            )

        cur.execute(
            "SELECT 1 FROM institutional_filers WHERE cik = %s LIMIT 1",
            (cik,),
        )
        if cur.fetchone() is not None:
            return ResolvedSubject(
                subject_type="institutional_filer",
                subject_id=cik,
                instrument_id=None,
            )

        cur.execute(
            "SELECT 1 FROM blockholder_filers WHERE cik = %s LIMIT 1",
            (cik,),
        )
        if cur.fetchone() is not None:
            return ResolvedSubject(
                subject_type="blockholder_filer",
                subject_id=cik,
                instrument_id=None,
            )

    return None


def run_atom_fast_lane(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    subject_resolver: SubjectResolver = default_subject_resolver,
) -> AtomLaneStats:
    """One Atom-feed cycle. Read → filter → UPSERT.

    Atom feed entries that don't map to a manifest source enum (e.g.
    S-1, CORRESP) are skipped. Entries whose CIK is out-of-universe
    are skipped. Idempotent on re-discovery — record_manifest_entry's
    ON CONFLICT preserves any in-flight ingest_status.
    """
    feed_rows = 0
    matched = 0
    upserted = 0
    skipped_unmapped = 0
    skipped_unknown = 0

    for row in read_getcurrent(http_get):
        feed_rows += 1
        if row.source is None:
            skipped_unmapped += 1
            continue

        subject = subject_resolver(conn, row.cik)
        if subject is None:
            skipped_unknown += 1
            continue
        matched += 1

        try:
            record_manifest_entry(
                conn,
                row.accession_number,
                cik=row.cik,
                form=row.form,
                source=row.source,
                subject_type=subject.subject_type,  # type: ignore[arg-type]
                subject_id=subject.subject_id,
                instrument_id=subject.instrument_id,
                filed_at=row.filed_at,
                accepted_at=row.accepted_at,
                primary_document_url=row.primary_document_url,
                is_amendment=row.is_amendment,
            )
            upserted += 1
        except ValueError as exc:
            # Service-layer guards (issuer/instrument cross-check,
            # empty cik, empty subject_id). Log + continue rather
            # than abort the whole feed for one bad row.
            logger.warning(
                "atom fast lane: record_manifest_entry rejected accession=%s: %s",
                row.accession_number,
                exc,
            )

    logger.info(
        "atom fast lane: feed=%d matched=%d upserted=%d unmapped_form=%d unknown_subject=%d",
        feed_rows,
        matched,
        upserted,
        skipped_unmapped,
        skipped_unknown,
    )
    return AtomLaneStats(
        feed_rows=feed_rows,
        matched_in_universe=matched,
        upserted=upserted,
        skipped_unmapped_form=skipped_unmapped,
        skipped_unknown_subject=skipped_unknown,
    )
