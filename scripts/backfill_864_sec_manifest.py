"""One-shot ``sec_filing_manifest`` backfill (#864).

Bootstraps the manifest from existing tombstone tables so the new
manifest-driven worker (#869) and freshness scheduler (#865) start
with full historical knowledge of every accession we've already seen.

Sources read:

  - ``def14a_ingest_log``                  → ``sec_def14a``  (issuer)
  - ``institutional_holdings_ingest_log``  → ``sec_13f_hr``  (institutional_filer)
  - ``insider_filings``                    → ``sec_form{3,4,5}`` (issuer)
  - ``blockholder_filings``                → ``sec_13d`` / ``sec_13g`` (issuer)
  - ``filing_raw_documents``               → infers source from
                                              ``document_kind`` for accessions
                                              not already covered above

For every (accession, source) tuple, derives:

  - ``ingest_status``: ``parsed``     when historical row indicates success
                       ``failed``     when historical row indicates failure
                       ``tombstoned`` when historical row is a give-up
                       ``pending``    when only ``filing_raw_documents`` knew
                                       about it (fetched but never parsed)
  - ``raw_status``:    ``stored``     when ``filing_raw_documents`` has the body
                       ``absent``     otherwise
  - ``parser_version``: copied from per-row provenance where available

Idempotent: ``record_manifest_entry`` is UPSERT; ``transition_status``
re-applies the same status as a no-op. Safe to re-run.

Run from repo root:

    uv run python -m scripts.backfill_864_sec_manifest --dry-run
    uv run python -m scripts.backfill_864_sec_manifest --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.config import settings
from app.services.sec_manifest import (
    ManifestSource,
    map_form_to_source,
    record_manifest_entry,
    transition_status,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _resolve_instrument_id_by_cik(cur: psycopg.Cursor[Any], cik: str) -> int | None:
    """Map issuer CIK → instrument_id; returns None when not in universe.

    The DEF 14A backfill needs this because the per-row table only
    carries the issuer's CIK, not the instrument_id directly. Returns
    None for non-tradable / non-universe CIKs — those manifest rows
    still get recorded under the issuer subject_type but with
    ``instrument_id=None``... wait — that's not allowed by the CHECK
    constraint. So those CIKs are SKIPPED with a debug log; the
    operator will pick them up on the next universe expansion."""
    cur.execute(
        """
        SELECT instrument_id
        FROM instrument_sec_profile
        WHERE cik = %s
        LIMIT 1
        """,
        (cik,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])


def _resolve_filer_cik_for_blockholder(cur: psycopg.Cursor[Any], filer_id: int) -> str | None:
    """Map blockholder_filings.filer_id → blockholder_filers.cik."""
    cur.execute(
        "SELECT cik FROM blockholder_filers WHERE filer_id = %s",
        (filer_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return str(row[0]) if row[0] else None


def backfill_def14a(conn: psycopg.Connection[Any], *, dry_run: bool) -> int:
    """One row per ``def14a_ingest_log`` entry → ``sec_def14a`` manifest.

    Maps ``status='success'`` → ``parsed``, ``'partial'`` → ``failed``,
    ``'failed'`` → ``failed``. The CIK → instrument_id resolution
    drops out-of-universe CIKs.
    """
    inserted = 0
    skipped_no_instrument = 0
    failed_resolves: list[str] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number, issuer_cik, status, error, fetched_at
            FROM def14a_ingest_log
            ORDER BY fetched_at ASC
            """
        )
        rows = cur.fetchall()
    logger.info("def14a backfill: %d source rows", len(rows))

    for accession, issuer_cik, status, error, fetched_at in rows:
        with conn.cursor() as cur:
            instrument_id = _resolve_instrument_id_by_cik(cur, issuer_cik)
        if instrument_id is None:
            skipped_no_instrument += 1
            failed_resolves.append(issuer_cik)
            continue

        if dry_run:
            inserted += 1
            continue

        record_manifest_entry(
            conn,
            accession,
            cik=issuer_cik,
            form="DEF 14A",
            source="sec_def14a",
            subject_type="issuer",
            subject_id=str(instrument_id),
            instrument_id=instrument_id,
            filed_at=fetched_at,
        )
        target_status = "parsed" if status == "success" else ("tombstoned" if status == "partial" else "failed")
        transition_status(
            conn,
            accession,
            ingest_status=target_status,
            error=error if target_status != "parsed" else None,
            last_attempted_at=fetched_at,
        )
        inserted += 1

    if failed_resolves:
        logger.info(
            "def14a backfill: %d rows skipped (issuer CIK not in instruments table); first 5: %s",
            skipped_no_instrument,
            failed_resolves[:5],
        )
    return inserted


def backfill_institutional_holdings(conn: psycopg.Connection[Any], *, dry_run: bool) -> int:
    """One row per ``institutional_holdings_ingest_log`` → ``sec_13f_hr``.

    Subject is the filer (not the issuer); instrument_id is NULL on
    13F rows because each accession spans many issuers in the body.
    """
    inserted = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number, filer_cik, status, error, fetched_at
            FROM institutional_holdings_ingest_log
            ORDER BY fetched_at ASC
            """
        )
        rows = cur.fetchall()
    logger.info("13F backfill: %d source rows", len(rows))

    for accession, filer_cik, status, error, fetched_at in rows:
        if dry_run:
            inserted += 1
            continue

        # Codex review: 13F ``filed_at`` is NOT ``period_of_report``
        # (which is the quarter-end, ~45 days before the actual filing).
        # Steady-state schedulers compute ``expected_next_at`` off
        # ``filed_at``; using period_of_report would skew the cadence
        # by ~45 days. ``fetched_at`` is the closest proxy in
        # ``institutional_holdings_ingest_log`` for when the filing
        # actually existed at SEC; the next steady-state poll cycle
        # (Atom + submissions.json) will UPSERT the precise filed_at.
        # ``period_of_report`` is preserved separately on the typed
        # ``institutional_holdings`` rows; not lost.
        record_manifest_entry(
            conn,
            accession,
            cik=filer_cik,
            form="13F-HR",
            source="sec_13f_hr",
            subject_type="institutional_filer",
            subject_id=filer_cik,
            instrument_id=None,
            filed_at=fetched_at,
        )
        target_status = "parsed" if status == "success" else ("tombstoned" if status == "partial" else "failed")
        transition_status(
            conn,
            accession,
            ingest_status=target_status,
            error=error if target_status != "parsed" else None,
            last_attempted_at=fetched_at,
        )
        inserted += 1
    return inserted


def backfill_insider_filings(conn: psycopg.Connection[Any], *, dry_run: bool) -> int:
    """One row per ``insider_filings`` → ``sec_form{3,4,5}``.

    The ``is_tombstone`` flag flips status to ``tombstoned``; otherwise
    presence of the row implies the parser ran successfully = ``parsed``
    (the legacy ingester did not insert insider_filings rows on parse
    failure, only tombstones).
    """
    inserted = 0
    skipped_no_form = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number, instrument_id, document_type, issuer_cik,
                   primary_document_url, fetched_at, parser_version, is_tombstone
            FROM insider_filings
            ORDER BY fetched_at ASC
            """
        )
        rows = cur.fetchall()
    logger.info("insider_filings backfill: %d source rows", len(rows))

    for (
        accession,
        instrument_id,
        document_type,
        issuer_cik,
        primary_document_url,
        fetched_at,
        parser_version,
        is_tombstone,
    ) in rows:
        source = map_form_to_source(document_type or "")
        if source not in {"sec_form3", "sec_form4", "sec_form5"}:
            skipped_no_form += 1
            continue
        if instrument_id is None:
            skipped_no_form += 1
            continue
        if dry_run:
            inserted += 1
            continue

        record_manifest_entry(
            conn,
            accession,
            cik=issuer_cik or "",
            form=document_type or "",
            source=source,
            subject_type="issuer",
            subject_id=str(instrument_id),
            instrument_id=int(instrument_id),
            filed_at=fetched_at,
            primary_document_url=primary_document_url,
        )
        target_status = "tombstoned" if is_tombstone else "parsed"
        transition_status(
            conn,
            accession,
            ingest_status=target_status,
            parser_version=str(parser_version) if parser_version is not None else None,
            last_attempted_at=fetched_at,
        )
        inserted += 1

    if skipped_no_form:
        logger.info("insider_filings backfill: %d rows skipped (unmapped form / null instrument_id)", skipped_no_form)
    return inserted


def backfill_blockholder_filings(conn: psycopg.Connection[Any], *, dry_run: bool) -> int:
    """One row per *distinct accession* in ``blockholder_filings`` →
    ``sec_13d`` or ``sec_13g``.

    The blockholder_filings table is per-reporter (joint filers
    produce multiple rows per accession). Manifest is per-accession,
    so we ``DISTINCT ON (accession_number)`` and pick the earliest
    ``filed_at`` for the manifest row.
    """
    inserted = 0
    skipped = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (accession_number)
                accession_number, submission_type, instrument_id, issuer_cik, filed_at
            FROM blockholder_filings
            ORDER BY accession_number, filed_at ASC
            """
        )
        rows = cur.fetchall()
    logger.info("blockholder_filings backfill: %d distinct accessions", len(rows))

    for accession, submission_type, instrument_id, issuer_cik, filed_at in rows:
        source = map_form_to_source(submission_type or "")
        if source not in {"sec_13d", "sec_13g"}:
            skipped += 1
            continue
        if instrument_id is None:
            # Issuer scoped — drop if we can't map to instrument_id;
            # it'll get repaired on the next universe expansion.
            skipped += 1
            continue
        if dry_run:
            inserted += 1
            continue

        record_manifest_entry(
            conn,
            accession,
            cik=issuer_cik or "",
            form=submission_type or "",
            source=source,
            subject_type="issuer",
            subject_id=str(instrument_id),
            instrument_id=int(instrument_id),
            filed_at=filed_at or datetime.now(tz=UTC),
        )
        transition_status(
            conn,
            accession,
            ingest_status="parsed",
            last_attempted_at=filed_at,
        )
        inserted += 1

    if skipped:
        logger.info("blockholder_filings backfill: %d accessions skipped (unmapped form / null instrument_id)", skipped)
    return inserted


# Mapping ``filing_raw_documents.document_kind`` → manifest ``source``
# for the residual sweep. Most accessions are already manifest-known
# via the per-table backfills above; this catches any gaps where the
# raw doc was fetched but the per-source ingest log never saw it.
_RAW_KIND_TO_SOURCE: dict[str, ManifestSource] = {
    "form4_xml": "sec_form4",
    "form3_xml": "sec_form3",
    "infotable_13f": "sec_13f_hr",
    "primary_doc_13dg": "sec_13d",
    "def14a_body": "sec_def14a",
}


def backfill_raw_documents(conn: psycopg.Connection[Any], *, dry_run: bool) -> int:
    """Mark ``raw_status='stored'`` on every manifest row whose body is
    in ``filing_raw_documents``, and add manifest rows for accessions
    that had a body fetched but no corresponding ingest_log entry
    (== orphaned raw doc → ``ingest_status='pending'``)."""
    promoted = 0
    with conn.cursor() as cur:
        # First pass: existing manifest rows whose body is on disk.
        cur.execute(
            """
            UPDATE sec_filing_manifest m
            SET raw_status = 'stored'
            WHERE raw_status = 'absent'
              AND EXISTS (
                  SELECT 1 FROM filing_raw_documents r
                  WHERE r.accession_number = m.accession_number
              )
            """
        )
        promoted = cur.rowcount

        # Second pass: orphans (body exists, no manifest row yet).
        cur.execute(
            """
            SELECT DISTINCT r.accession_number, r.document_kind, r.fetched_at, r.parser_version
            FROM filing_raw_documents r
            LEFT JOIN sec_filing_manifest m USING (accession_number)
            WHERE m.accession_number IS NULL
            """
        )
        orphans = cur.fetchall()

    logger.info(
        "raw_documents backfill: promoted %d existing manifest rows to raw_status='stored'; %d orphans to insert",
        promoted,
        len(orphans),
    )
    if dry_run:
        return promoted + len(orphans)

    # Orphans don't carry enough metadata to build a complete manifest
    # row (no cik, no instrument_id, no form code). Log them and
    # skip — these need a manual backfill pass against the SEC
    # discovery layer to recover the missing fields.
    if orphans:
        logger.warning(
            "raw_documents backfill: %d orphan accessions skipped"
            " — manifest row needs cik/instrument_id which is not in filing_raw_documents."
            " First 5 accessions: %s",
            len(orphans),
            [r[0] for r in orphans[:5]],
        )
    return promoted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit the writes (default is dry-run).")
    args = parser.parse_args()
    dry_run = not args.apply

    logger.info("backfill_864_sec_manifest: dry_run=%s db=%s", dry_run, settings.database_url[:40])

    with psycopg.connect(settings.database_url) as conn:
        conn.autocommit = False
        try:
            n_def14a = backfill_def14a(conn, dry_run=dry_run)
            n_13f = backfill_institutional_holdings(conn, dry_run=dry_run)
            n_insider = backfill_insider_filings(conn, dry_run=dry_run)
            n_block = backfill_blockholder_filings(conn, dry_run=dry_run)
            n_raw = backfill_raw_documents(conn, dry_run=dry_run)

            if dry_run:
                conn.rollback()
                logger.info("DRY RUN: would have inserted/transitioned counts")
            else:
                conn.commit()
                logger.info("backfill committed")

            logger.info(
                "summary: def14a=%d 13f=%d insider=%d block=%d raw=%d",
                n_def14a,
                n_13f,
                n_insider,
                n_block,
                n_raw,
            )
        except Exception:
            conn.rollback()
            logger.exception("backfill failed; rolled back")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
