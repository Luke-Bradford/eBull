"""First-install drain (#871).

Issue #871 / spec §"Mode 1 — First-install drain".

Operator-triggered job for new installs and explicit drain requests.

Two paths (Codex review v2 finding 3):

  - **In-universe-only (default)**: per-CIK submissions.json for every
    CIK in the tradable universe. ~12k requests at 10 req/s = ~20 min.
    Cheap, precise, respects the universe scope.
  - **Bulk-zip**: download submissions.zip + companyfacts.zip once.
    Production / operator-explicit only — NOT the default local path.
    Out of scope for this PR; raises NotImplementedError. The
    in-universe path is sufficient for dev + small-capital live.

Crash-resume: idempotent — re-run drains the remaining pending /
unknown subjects. ``record_manifest_entry`` UPSERTs, so duplicate
discovery is a no-op.

Pagination: when a CIK's recent array doesn't cover its full history
(``has_more_in_files=True``), the drain follows the secondary pages
to capture older filings. The per-CIK steady-state poll (#870) does
NOT follow pagination; this is the dedicated batch-throughput path.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import psycopg

from app.jobs.sec_atom_fast_lane import ResolvedSubject
from app.providers.implementations.sec_submissions import (
    HttpGet,
    check_freshness,
    parse_submissions_page,
)
from app.services.sec_manifest import record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DrainStats:
    ciks_processed: int
    ciks_skipped: int
    secondary_pages_fetched: int
    manifest_rows_upserted: int
    errors: int


def _iter_in_universe_subjects(
    conn: psycopg.Connection[Any],
) -> Iterable[ResolvedSubject]:
    """Stream every (cik, subject) triple in the universe.

    Issuers from instrument_sec_profile, then institutional_filers,
    then blockholder_filers. Ordered for deterministic test runs;
    crash-resume relies on the manifest UPSERT idempotency, not
    iteration ordering.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik, instrument_id FROM instrument_sec_profile WHERE cik IS NOT NULL ORDER BY instrument_id"
        )
        for cik, instrument_id in cur.fetchall():
            yield (
                ResolvedSubject(
                    subject_type="issuer",
                    subject_id=str(int(instrument_id)),
                    instrument_id=int(instrument_id),
                ),
                cik,
            )  # type: ignore[misc]

        cur.execute("SELECT cik FROM institutional_filers ORDER BY filer_id")
        for (cik,) in cur.fetchall():
            yield (
                ResolvedSubject(
                    subject_type="institutional_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
                cik,
            )  # type: ignore[misc]

        cur.execute("SELECT cik FROM blockholder_filers ORDER BY filer_id")
        for (cik,) in cur.fetchall():
            yield (
                ResolvedSubject(
                    subject_type="blockholder_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
                cik,
            )  # type: ignore[misc]


def run_first_install_drain(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    use_bulk_zip: bool = False,
    follow_pagination: bool = True,
    max_subjects: int | None = None,
) -> DrainStats:
    """Drain manifest seeding from every CIK in the universe.

    ``use_bulk_zip=True`` raises NotImplementedError — see module
    docstring. Operator path will land in a follow-up PR if needed.

    ``max_subjects=None`` drains everything; pass an integer to bound
    a sample run. ``follow_pagination`` controls whether secondary
    submissions pages are fetched when ``has_more_in_files``.
    """
    if use_bulk_zip:
        raise NotImplementedError(
            "bulk-zip drain not yet implemented — use the default in-universe path "
            "or wait for the dedicated bulk-zip PR"
        )

    ciks_processed = 0
    ciks_skipped = 0
    secondary_pages_fetched = 0
    manifest_upserted = 0
    errors = 0

    for subject, cik in _iter_in_universe_subjects(conn):  # type: ignore[misc]
        if max_subjects is not None and ciks_processed >= max_subjects:
            break

        try:
            delta = check_freshness(
                http_get,
                cik=cik,
                last_known_filing_id=None,  # full drain — no watermark
            )
        except Exception as exc:
            logger.warning("first-install drain: check_freshness raised for cik=%s: %s", cik, exc)
            errors += 1
            continue

        ciks_processed += 1
        if not delta.new_filings:
            ciks_skipped += 1

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
                    subject_type=subject.subject_type,  # type: ignore[arg-type]
                    subject_id=subject.subject_id,
                    instrument_id=subject.instrument_id,
                    filed_at=row.filed_at,
                    accepted_at=row.accepted_at,
                    primary_document_url=row.primary_document_url,
                    is_amendment=row.is_amendment,
                )
                manifest_upserted += 1
            except ValueError as exc:
                logger.warning(
                    "first-install drain: rejected accession=%s for cik=%s: %s",
                    row.accession_number,
                    cik,
                    exc,
                )

        # Secondary-page pagination for full history
        if follow_pagination and delta.has_more_in_files:
            secondary_pages_fetched += _drain_secondary_pages(
                conn,
                http_get=http_get,
                cik=cik,
                subject=subject,
            )

    logger.info(
        "first-install drain: ciks=%d skipped=%d errors=%d secondary_pages=%d upserted=%d",
        ciks_processed,
        ciks_skipped,
        errors,
        secondary_pages_fetched,
        manifest_upserted,
    )
    return DrainStats(
        ciks_processed=ciks_processed,
        ciks_skipped=ciks_skipped,
        secondary_pages_fetched=secondary_pages_fetched,
        manifest_rows_upserted=manifest_upserted,
        errors=errors,
    )


def _drain_secondary_pages(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    cik: str,
    subject: ResolvedSubject,
) -> int:
    """Walk every ``filings.files[]`` page for one CIK.

    The primary submissions.json carries up to ~1000 most-recent
    filings inline. Older filings live in secondary pages named in
    ``files[]``. The drain follows them all once per CIK.

    Returns the count of pages fetched.
    """
    cik_padded = cik.zfill(10)
    primary_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    primary_status, primary_body = http_get(primary_url, _drain_headers())
    if primary_status != 200:
        return 0
    try:
        primary_payload = json.loads(primary_body)
    except json.JSONDecodeError:
        return 0

    files = (primary_payload.get("filings", {}) or {}).get("files", []) or []
    pages = 0
    for page_meta in files:
        name = page_meta.get("name") if isinstance(page_meta, dict) else None
        if not name:
            continue
        page_url = f"https://data.sec.gov/submissions/{name}"
        status, body = http_get(page_url, _drain_headers())
        if status != 200:
            continue
        rows, _ = parse_submissions_page(body, cik=cik_padded)
        pages += 1
        for row in rows:
            if row.source is None:
                continue
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
            except ValueError as exc:
                logger.warning(
                    "first-install drain (secondary): rejected accession=%s: %s",
                    row.accession_number,
                    exc,
                )
    return pages


def _drain_headers() -> dict[str, str]:
    return {
        "User-Agent": "eBull research/1.0 contact@example.com",
        "Accept-Encoding": "gzip, deflate",
    }
