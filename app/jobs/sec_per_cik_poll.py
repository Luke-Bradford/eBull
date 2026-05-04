"""Per-CIK scheduled polling — Layer 3 (#870).

Issue #870 / spec §"Layer 3 — Per-CIK submissions.json".

Hourly job that reads ``data_freshness_index`` for subjects whose
``expected_next_at`` has elapsed, calls ``check_freshness`` per CIK,
UPSERTs new manifest rows, and updates the scheduler outcome.

Layer 3 is the per-CIK reconcile path — fires only at predicted-next-
filing windows. AAPL's DEF 14A poll fires once a year; AAPL's 13F
poll never fires (issuer subject; AAPL doesn't file 13F).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

from app.providers.implementations.sec_submissions import HttpGet, check_freshness
from app.services.data_freshness import (
    record_poll_outcome,
    subjects_due_for_poll,
)
from app.services.sec_manifest import ManifestSource, record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PerCikPollStats:
    subjects_polled: int
    new_filings_recorded: int
    poll_errors: int


def run_per_cik_poll(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    source: ManifestSource | None = None,
    max_subjects: int = 100,
) -> PerCikPollStats:
    """One per-CIK poll cycle. For each subject due, call submissions.json
    and UPSERT manifest + scheduler.

    Pagination (``has_more_in_files`` for first-install / rebuild
    paths) is NOT followed here — that lives in the dedicated drain
    + rebuild jobs (#871, #872) which have their own throughput
    budgets. The per-CIK steady-state path uses only the recent array.
    """
    subjects_polled = 0
    new_filings_recorded = 0
    poll_errors = 0

    due = list(subjects_due_for_poll(conn, source=source, limit=max_subjects))

    for subject in due:
        if subject.cik is None:
            # FINRA universe singleton — no submissions.json poll
            continue
        subjects_polled += 1

        sources_to_check: set[ManifestSource] | None = {subject.source} if subject.source else None
        try:
            delta = check_freshness(
                http_get,
                cik=subject.cik,
                last_known_filing_id=subject.last_known_filing_id,
                sources=sources_to_check,
            )
        except Exception as exc:
            logger.warning(
                "per-cik poll: check_freshness raised for cik=%s source=%s: %s",
                subject.cik,
                subject.source,
                exc,
            )
            record_poll_outcome(
                conn,
                subject_type=subject.subject_type,
                subject_id=subject.subject_id,
                source=subject.source,
                outcome="error",
                error=f"{type(exc).__name__}: {exc}"[:500],
                cik=subject.cik,
                instrument_id=subject.instrument_id,
            )
            poll_errors += 1
            continue

        # UPSERT manifest rows for the new filings
        for row in delta.new_filings:
            if row.source is None:
                continue
            try:
                record_manifest_entry(
                    conn,
                    row.accession_number,
                    cik=row.cik,
                    form=row.form,
                    source=row.source,
                    subject_type=subject.subject_type,
                    subject_id=subject.subject_id,
                    instrument_id=subject.instrument_id,
                    filed_at=row.filed_at,
                    accepted_at=row.accepted_at,
                    primary_document_url=row.primary_document_url,
                    is_amendment=row.is_amendment,
                )
                new_filings_recorded += 1
            except ValueError as exc:
                logger.warning("per-cik poll: rejected accession=%s: %s", row.accession_number, exc)

        # Update scheduler outcome
        outcome: str = "new_data" if delta.new_filings else "current"
        last_known = delta.new_filings[0].accession_number if delta.new_filings else subject.last_known_filing_id
        last_filed = delta.last_filed_at if delta.last_filed_at else subject.last_known_filed_at
        record_poll_outcome(
            conn,
            subject_type=subject.subject_type,
            subject_id=subject.subject_id,
            source=subject.source,
            outcome=outcome,  # type: ignore[arg-type]
            last_known_filing_id=last_known,
            last_known_filed_at=last_filed,
            new_filings_since=len(delta.new_filings),
            cik=subject.cik,
            instrument_id=subject.instrument_id,
        )

    logger.info(
        "per-cik poll: subjects=%d new_filings=%d errors=%d",
        subjects_polled,
        new_filings_recorded,
        poll_errors,
    )
    return PerCikPollStats(
        subjects_polled=subjects_polled,
        new_filings_recorded=new_filings_recorded,
        poll_errors=poll_errors,
    )
