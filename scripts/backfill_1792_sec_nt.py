"""Scoped seed + drain of historical NT 10-K / NT 10-Q into ``sec_nt`` (#1792).

Follow-up to #1015 (Form 12b-25 NT 10-K / NT 10-Q parser). The going-forward
``sec_nt`` source + the #1015 pilot (8 filings) are already live; this backfills
the ~4.2k historical NT 10-K / NT 10-Q ``filing_events`` (2016-2026) that predate
the source. Mirrors the ``sec_n_csr`` parser/backfill split (#1174/#1176).

Why a *scoped* script, not the universe seed (issue option 1):
``seed_manifest_from_filing_events`` walks every ``provider='sec'`` filing_event.
On a fully-ingested dev DB that resurrects ~78k un-manifested non-NT rows
(form4/8k/13g/...) as ``pending`` and saturates the shared 10 req/s manifest
worker for hours. This seeds ONLY the two forms that
``map_form_to_source`` routes to ``sec_nt`` (NT 10-K / NT 10-Q), then drains
them standalone, mirroring ``drain_554_sec10k.py``.

Convergence, not one-shot: the seed has no date/era predicate — it (re)seeds
ALL NT 10-K / NT 10-Q events, and the drain clears ALL pending ``sec_nt``. That
is intentional: #1792's done-criterion is ``sec_nt`` pending == 0, so a
going-forward NT notice that the daemon discovers mid-run draining here too is
correct, not scope creep. "Historical ~4.2k" is the motivation (today the whole
population predates the source); the operation stays correct as the population
grows because it is idempotent — ``record_manifest_entry`` never overwrites a
live row's ``ingest_status`` (S16 contract), so re-runs leave the #1015 pilot's
already-``parsed`` rows untouched and only insert genuinely-new accessions.

Concurrency: the drain shares oldest-pending rows with the running jobs-daemon
manifest worker. ``_dispatch_rows`` catches per-row transition failures
internally and ``transition_status`` no-ops ``parsed->parsed``, so the outer
chunk-level rollback/retry rarely fires — it is a defensive backstop for a
whole-chunk dispatch error. ``iter_pending`` does not row-lock, so daemon +
script can occasionally double-fetch one accession's body; writes are idempotent
so this only wastes a little of the shared 10 req/s budget — acceptable for a
one-off backfill (omit this script and let the daemon drain solo to avoid it).

Usage:
    uv run python scripts/backfill_1792_sec_nt.py              # seed then drain
    uv run python scripts/backfill_1792_sec_nt.py --seed-only  # create rows, let daemon drain
    uv run python scripts/backfill_1792_sec_nt.py --drain-only # drain existing pending
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.config import settings
from app.jobs.sec_manifest_worker import _dispatch_rows
from app.services.manifest_parsers import register_all_parsers
from app.services.sec_manifest import (
    is_amendment_form,
    iter_pending,
    map_form_to_source,
    record_manifest_entry,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("backfill_1792")

# The two forms map_form_to_source routes to ``sec_nt``. Kept explicit so a
# future routing change to other NT-* forms (NT 20-F / NT 11-K) is a conscious
# edit here, not a silent scope creep.
NT_FORMS = ("NT 10-K", "NT 10-Q")


def seed(conn: psycopg.Connection[Any]) -> int:
    """Insert ``sec_nt`` manifest rows for historical NT 10-K / NT 10-Q events.

    Mirrors ``seed_manifest_from_filing_events``'s CIK resolution (LATERAL
    LIMIT 1, primary-preferred) and per-accession dedup, but filtered to
    ``NT_FORMS`` so it cannot touch any other source.
    """
    with conn.cursor() as cur:
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
              AND fe.filing_type = ANY(%(forms)s)
              AND fe.provider_filing_id IS NOT NULL
            ORDER BY fe.provider_filing_id, fe.instrument_id
            """,
            {"forms": list(NT_FORMS)},
        )
        rows = cur.fetchall()

    upserted = 0
    skipped_no_cik = 0
    for instrument_id, filing_date, filing_type, accession, primary_doc_url, cik_raw in rows:
        if cik_raw is None or not str(cik_raw).strip():
            skipped_no_cik += 1
            continue
        source = map_form_to_source(filing_type) if filing_type else None
        if source != "sec_nt":
            # Defensive: the WHERE already restricts to NT_FORMS, but never
            # seed a row whose canonical routing disagrees with this drive.
            logger.warning(
                "skip accession=%s form=%r routed to %r (expected sec_nt)",
                accession,
                filing_type,
                source,
            )
            continue
        filed_at = datetime.combine(filing_date, datetime.min.time(), tzinfo=UTC)
        try:
            record_manifest_entry(
                conn,
                str(accession),
                cik=str(cik_raw).strip().zfill(10),
                form=str(filing_type),
                source="sec_nt",
                subject_type="issuer",
                subject_id=str(int(instrument_id)),
                instrument_id=int(instrument_id),
                filed_at=filed_at,
                primary_document_url=primary_doc_url,
                is_amendment=is_amendment_form(filing_type or ""),
                initial_ingest_status="pending",
            )
            upserted += 1
        except ValueError as exc:
            logger.debug("rejected accession=%s: %s", accession, exc)
    conn.commit()
    logger.info(
        "SEED COMPLETE: candidates=%d upserted=%d skipped_no_cik=%d",
        len(rows),
        upserted,
        skipped_no_cik,
    )
    return upserted


def drain(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Drain pending ``sec_nt`` manifest rows in chunks of 50 (S7 recipe)."""
    totals = {"parsed": 0, "tombstoned": 0, "failed": 0, "skipped": 0}
    chunks = 0
    consecutive_failures = 0
    started = time.monotonic()
    while True:
        rows = list(iter_pending(conn, source="sec_nt", limit=50))
        if not rows:
            break
        try:
            stats = _dispatch_rows(conn, rows, now=datetime.now(tz=UTC))
        except Exception:
            # Defensive backstop: _dispatch_rows handles per-row transition
            # races internally, so this fires only on a whole-chunk dispatch
            # error. Roll back the chunk (idempotent redo) and re-read.
            conn.rollback()
            consecutive_failures += 1
            logger.warning(
                "chunk dispatch failed (%d consecutive); re-reading",
                consecutive_failures,
                exc_info=True,
            )
            if consecutive_failures >= 5:
                raise
            time.sleep(5)
            continue
        consecutive_failures = 0
        conn.commit()
        chunks += 1
        totals["parsed"] += stats.parsed
        totals["tombstoned"] += stats.tombstoned
        totals["failed"] += stats.failed
        totals["skipped"] += stats.skipped_no_parser
        logger.info(
            "chunk %d done: %s (elapsed %.0fs)",
            chunks,
            totals,
            time.monotonic() - started,
        )
    logger.info("DRAIN COMPLETE: %s in %.0fs", totals, time.monotonic() - started)
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="NT 10-K / NT 10-Q backfill (#1792)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--seed-only", action="store_true", help="seed manifest, skip drain")
    group.add_argument("--drain-only", action="store_true", help="drain only, skip seed")
    args = parser.parse_args()

    register_all_parsers()
    with psycopg.connect(settings.database_url, application_name="backfill-1792-sec-nt") as conn:
        if not args.drain_only:
            seed(conn)
        if not args.seed_only:
            drain(conn)


if __name__ == "__main__":
    main()
