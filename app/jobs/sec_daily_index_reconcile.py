"""Daily-index reconciliation job (#868).

Issue #868 / spec §"Layer 2 — Daily-index reconciliation".

Runs daily at 04:00 UTC. Streams the previous day's daily-index from
SEC, filters to (cik IN universe) + (source IN our set), and UPSERTs
manifest rows the Atom fast lane (#867) missed.

This is the SAFETY NET on top of the Atom feed. Atom is low-latency
but lossy on outage / very-old amendments; the daily-index covers
every accepted filing for that day so nothing falls through the
cracks.

One ~1 MB download covers every CIK + every form. Cheap.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg

from app.jobs.sec_atom_fast_lane import (
    ResolvedSubject,
    SubjectResolver,
    default_subject_resolver,
)
from app.providers.implementations.sec_daily_index import HttpGet, read_daily_index
from app.services.sec_manifest import record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReconcileStats:
    index_rows: int
    matched_in_universe: int
    upserted: int
    skipped_unmapped_form: int
    skipped_unknown_subject: int


def run_daily_index_reconcile(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    when: date | None = None,
    subject_resolver: SubjectResolver = default_subject_resolver,
) -> ReconcileStats:
    """Reconcile one day's filings against the manifest.

    ``when=None`` defaults to yesterday (UTC), which is the typical
    04:00-UTC schedule — yesterday's daily-index is reliably published
    by then.

    Idempotent on re-run for the same date — record_manifest_entry's
    ON CONFLICT preserves any in-flight ingest_status (e.g. an Atom-
    discovered row already in ``parsed`` is NOT downgraded to
    ``pending``).
    """
    if when is None:
        when = (datetime.now(tz=UTC) - timedelta(days=1)).date()

    index_rows = 0
    matched = 0
    upserted = 0
    skipped_unmapped = 0
    skipped_unknown = 0

    for row in read_daily_index(http_get, when):
        index_rows += 1
        if row.source is None:
            skipped_unmapped += 1
            continue

        subject: ResolvedSubject | None = subject_resolver(conn, row.cik)
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
            logger.warning(
                "daily-index reconcile: rejected accession=%s: %s",
                row.accession_number,
                exc,
            )

    logger.info(
        "daily-index reconcile %s: index=%d matched=%d upserted=%d unmapped=%d unknown=%d",
        when.isoformat(),
        index_rows,
        matched,
        upserted,
        skipped_unmapped,
        skipped_unknown,
    )
    return ReconcileStats(
        index_rows=index_rows,
        matched_in_universe=matched,
        upserted=upserted,
        skipped_unmapped_form=skipped_unmapped,
        skipped_unknown_subject=skipped_unknown,
    )
