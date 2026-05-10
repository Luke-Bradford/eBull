"""First-install drain (#871).

Issue #871 / spec §"Mode 1 — First-install drain".

Operator-triggered job for new installs and explicit drain requests.

Three paths:

  - **filing_events seed (default fast path, #1044)**: SELECT every
    issuer row from ``filing_events`` (already populated by C1.a +
    C1.b in the bulk path) and seed ``sec_filing_manifest`` from the
    cached payloads. No HTTP. ~15s for ~12k issuer events vs ~21min
    of per-CIK fetches.
  - **In-universe HTTP fallback**: per-CIK submissions.json for every
    CIK in the tradable universe. Used when ``filing_events`` is
    empty (e.g. fallback mode where the bulk path was bypassed).
  - **Bulk-zip**: download submissions.zip + companyfacts.zip once.
    Out of scope; raises NotImplementedError.

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
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.jobs.sec_atom_fast_lane import ResolvedSubject
from app.providers.implementations.sec_submissions import (
    HttpGet,
    check_freshness,
    parse_submissions_page,
)
from app.services.bootstrap_state import BootstrapStageCancelled
from app.services.data_freshness import seed_scheduler_from_manifest
from app.services.processes.bootstrap_cancel_signal import bootstrap_cancel_requested
from app.services.sec_manifest import is_amendment_form, map_form_to_source, record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DrainStats:
    ciks_processed: int
    ciks_skipped: int
    secondary_pages_fetched: int
    manifest_rows_upserted: int
    errors: int
    # Count of (subject_type, subject_id, source) triples written to
    # ``data_freshness_index`` after the drain. Without this seeding
    # step (#937), the steady-state per-CIK poll would silently no-op
    # because the scheduler had no rows to poll.
    scheduler_rows_seeded: int = 0
    # #1044: count of manifest rows seeded from filing_events without
    # any HTTP. Bulk path populates this; fallback path leaves it 0.
    rows_seeded_from_filing_events: int = 0


def seed_manifest_from_filing_events(
    conn: psycopg.Connection[Any],
) -> int:
    """Seed ``sec_filing_manifest`` from already-ingested ``filing_events``.

    Reads every issuer-scoped filing_events row joined to the best
    available SEC CIK identifier (primary preferred), calls
    ``record_manifest_entry`` per row.
    The bulk path (C1.a + C1.b) populates filing_events ahead of this
    drain stage, so the manifest can be seeded without ANY HTTP
    requests — replaces the ~21min per-CIK loop with a ~15s table
    walk. (#1044)

    Returns the number of manifest rows upserted.

    No-op if filing_events is empty (e.g. fallback mode bypassed the
    bulk path entirely); the caller should follow up with the per-CIK
    HTTP drain in that case.
    """
    upserted = 0
    skipped_unmapped_form = 0
    skipped_no_cik = 0
    skipped_non_issuer_source = 0
    with conn.cursor() as cur:
        # LATERAL JOIN with LIMIT 1 picks the highest-priority CIK
        # mapping per instrument: ORDER BY is_primary DESC selects the
        # primary mapping when one exists, else any non-primary one.
        # Codex pre-push MED for #1044 — naive `is_primary = TRUE`
        # would drop valid rows whose only SEC CIK mapping isn't
        # flagged primary.
        # Per-#1117 PR-B: filing_events fans out per share-class
        # sibling under sql/144, so two rows can carry the same
        # accession. sec_filing_manifest.accession_number is PK
        # (entity-level), so the seeder dedups by accession picking
        # the canonical (lowest instrument_id) sibling. Parser
        # fan-out at parse time covers per-sibling observations;
        # the manifest itself is one row per accession.
        cur.execute(
            """
            SELECT DISTINCT ON (fe.provider_filing_id)
                fe.instrument_id,
                fe.filing_date,
                fe.filing_type,
                fe.provider_filing_id,
                fe.primary_document_url,
                cik_map.identifier_value AS cik
            FROM filing_events fe
            JOIN LATERAL (
                SELECT identifier_value
                FROM external_identifiers ei
                WHERE ei.instrument_id = fe.instrument_id
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                ORDER BY ei.is_primary DESC, ei.external_identifier_id ASC
                LIMIT 1
            ) cik_map ON TRUE
            WHERE fe.provider = 'sec'
            ORDER BY fe.provider_filing_id, fe.instrument_id
            """
        )
        rows = cur.fetchall()
    for instrument_id, filing_date, filing_type, provider_filing_id, primary_doc_url, cik_raw in rows:
        if cik_raw is None or not str(cik_raw).strip():
            skipped_no_cik += 1
            continue
        # Use the canonical map_form_to_source from sec_manifest so
        # this drain stays in sync with the rest of the manifest
        # writers (Codex pre-push HIGH for #1044).
        source = map_form_to_source(filing_type) if filing_type else None
        if source is None:
            skipped_unmapped_form += 1
            continue
        # subject_type='issuer' is hard-coded below — must therefore
        # exclude non-issuer-scoped sources. 13F-HR / N-PORT / N-CSR
        # are filer-scoped (subject_id = filer CIK, instrument_id NULL)
        # and need a different code path. Filing_events does carry
        # 13F-HR / NPORT-P rows for instruments that ALSO have those
        # filings on file (e.g. fund families), so we must filter
        # them out at the seed boundary. PR #1051 review WARNING for
        # #1044.
        if source in ("sec_13f_hr", "sec_n_port", "sec_n_csr"):
            skipped_non_issuer_source += 1
            continue
        # Accession lives in fe.provider_filing_id — that's the
        # authoritative column. raw_payload_json mirrors it but can
        # legitimately drift on legacy rows. Codex pre-push MED.
        accession = provider_filing_id
        if not accession:
            continue
        cik_padded = str(cik_raw).strip().zfill(10)
        # filing_date is a date — record_manifest_entry takes filed_at
        # as datetime. Anchor at UTC midnight; the precise time is
        # carried only by the per-CIK HTTP path's accept_timestamp.
        filed_at = datetime.combine(filing_date, datetime.min.time(), tzinfo=UTC)
        # Use canonical is_amendment_form so DEFA14A and other non-/A
        # amendment proxies are flagged correctly. Codex pre-push MED.
        is_amendment = is_amendment_form(filing_type or "")
        try:
            record_manifest_entry(
                conn,
                str(accession),
                cik=cik_padded,
                form=str(filing_type or ""),
                source=source,
                subject_type="issuer",
                subject_id=str(int(instrument_id)),
                instrument_id=int(instrument_id),
                filed_at=filed_at,
                primary_document_url=primary_doc_url,
                is_amendment=is_amendment,
            )
            upserted += 1
        except ValueError as exc:
            logger.debug(
                "seed_manifest_from_filing_events: rejected accession=%s: %s",
                accession,
                exc,
            )
    if skipped_unmapped_form or skipped_no_cik or skipped_non_issuer_source:
        logger.info(
            "seed_manifest_from_filing_events: upserted=%d skipped_no_cik=%d "
            "skipped_unmapped_form=%d skipped_non_issuer_source=%d",
            upserted,
            skipped_no_cik,
            skipped_unmapped_form,
            skipped_non_issuer_source,
        )
    return upserted


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

    # Fast path (#1044): if filing_events has rows for the SEC
    # provider (populated by C1.a + C1.b in the bulk path), seed the
    # manifest from that table without any HTTP requests. The per-CIK
    # HTTP loop below still runs to cover non-issuer subjects
    # (institutional_filer + blockholder_filer) which filing_events
    # doesn't carry. Newer issuer filings published since the bulk
    # snapshot are picked up by the steady-state per-CIK poll
    # (#870), not this drain — running another full HTTP sweep here
    # would defeat the perf gain.
    rows_seeded_from_filing_events = seed_manifest_from_filing_events(conn)
    manifest_upserted += rows_seeded_from_filing_events
    if rows_seeded_from_filing_events > 0:
        logger.info(
            "first-install drain: seeded %d manifest rows from filing_events (no HTTP)",
            rows_seeded_from_filing_events,
        )

    skip_issuer_http = rows_seeded_from_filing_events > 0
    # PR3d #1064 — cancel-poll cadence. The drain iterates ~12k CIKs
    # at 10 req/s, ~21 minutes wall-clock. Polling for the bootstrap
    # cancel signal every 50 CIKs keeps observation latency under
    # ~5 seconds without flooding the DB. Outside a bootstrap dispatch
    # the helper short-circuits to False (contextvar unset), so
    # scheduled / manual triggers of this job are unaffected.
    _CANCEL_POLL_EVERY_N = 50
    for n, (subject, cik) in enumerate(_iter_in_universe_subjects(conn)):  # type: ignore[misc]
        if n % _CANCEL_POLL_EVERY_N == 0 and bootstrap_cancel_requested():
            raise BootstrapStageCancelled(
                f"first-install drain cancelled by operator after {ciks_processed} CIKs",
                stage_key="sec_first_install_drain",
            )
        if max_subjects is not None and ciks_processed >= max_subjects:
            break
        # #1044 fast-path: when filing_events seeded the issuer manifest
        # rows already, skip the per-CIK HTTP fetch for issuers. Non-
        # issuer subjects (institutional_filer + blockholder_filer)
        # still need the HTTP fetch — filing_events only carries
        # universe-mapped instruments.
        if skip_issuer_http and subject.subject_type == "issuer":
            ciks_skipped += 1
            continue

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

    # #937: seed the scheduler from manifest after the drain commits
    # rows. Without this, the per-CIK poll (#870) silently no-ops
    # post-drain because data_freshness_index is empty for the drained
    # scope. ``seed_scheduler_from_manifest`` is idempotent + UPSERTs
    # by (subject_type, subject_id, source) so re-runs are safe.
    #
    # Scope trade-off: ``seed_scheduler_from_manifest`` is full-table —
    # ``SELECT DISTINCT ON ... FROM sec_filing_manifest``. With
    # ``max_subjects=N`` (sample run) it still scans every prior
    # manifest row, not just this drain's. Acceptable here because the
    # drain runs rarely (first-install + explicit operator re-drain);
    # the full-scan + ON CONFLICT UPSERT is bounded at ~12k subjects ×
    # ~10 forms ≈ 120k rows, well under any pathological threshold. A
    # scoped variant is filed as a follow-up if the scale ever grows.
    scheduler_rows_seeded = seed_scheduler_from_manifest(conn)

    logger.info(
        "first-install drain: ciks=%d skipped=%d errors=%d secondary_pages=%d upserted=%d scheduler_seeded=%d",
        ciks_processed,
        ciks_skipped,
        errors,
        secondary_pages_fetched,
        manifest_upserted,
        scheduler_rows_seeded,
    )
    return DrainStats(
        ciks_processed=ciks_processed,
        ciks_skipped=ciks_skipped,
        secondary_pages_fetched=secondary_pages_fetched,
        manifest_rows_upserted=manifest_upserted,
        errors=errors,
        scheduler_rows_seeded=scheduler_rows_seeded,
        rows_seeded_from_filing_events=rows_seeded_from_filing_events,
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
