"""Scoped seed + drain of historical 424B prospectuses into ``sec_424b`` (#1974).

Child of #1816 (parser shipped in PR #1973, B2 volume gate in #1975/PR #1981).
The going-forward ``sec_424b`` source is live; this backfills the historical
424B ``filing_events`` that predate the source. Mirrors the ``sec_nt``
parser/backfill split (#1792 / ``backfill_1792_sec_nt.py``).

Scope = every form ``map_form_to_source`` routes to ``sec_424b``:
tier-1 bodies (424B1/B3/B4/B5/B7, ~43.7k) PLUS 424B2 (~152k). B2 is
volume-gated at parse time (#1975): the ~4.2k allowed B2 bodies parse on this
drive; the over-cap whale rows tombstone at ~zero cost (one COUNT on a
partial index, no SEC fetch) — so seeding B2 here is intended, not scope
creep.

Why a *scoped* script, not the universe seed: same reason as #1792 —
``seed_manifest_from_filing_events`` resurrects every un-manifested
``provider='sec'`` row across all sources.

Convergence, not one-shot: no date predicate; done-criterion is ``sec_424b``
pending == 0. ``record_manifest_entry`` never overwrites a live row's
``ingest_status`` (S16 contract), so re-runs leave already-``parsed``/
``tombstoned`` rows untouched and only insert genuinely-new accessions.

Concurrency: the drain shares oldest-pending rows with the running
jobs-daemon manifest worker (same caveats as #1792 — idempotent writes, the
occasional double-fetch only wastes a little of the SEC budget; the daemon
worker's fair-quota slice is ~0.7 req/s on top of this process's throttle).

Usage:
    uv run python scripts/backfill_1974_sec_424b.py --dry-run    # report-only
    uv run python scripts/backfill_1974_sec_424b.py              # seed then drain
    uv run python scripts/backfill_1974_sec_424b.py --seed-only  # create rows, let daemon drain
    uv run python scripts/backfill_1974_sec_424b.py --drain-only # drain existing pending
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
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
logger = logging.getLogger("backfill_1974")

# Every form map_form_to_source routes to ``sec_424b``. Kept explicit so a
# future routing change (e.g. mapping 424B8) is a conscious edit here, not a
# silent scope change mid-drive.
B424_FORMS = ("424B1", "424B2", "424B3", "424B4", "424B5", "424B7")

_CANDIDATES_SQL = """
    SELECT DISTINCT ON (fe.provider_filing_id)
        fe.instrument_id,
        fe.filing_date,
        fe.filing_type,
        fe.provider_filing_id,
        fe.primary_document_url,
        cik_map.identifier_value AS cik,
        (m.accession_number IS NOT NULL) AS already_manifested
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
    LEFT JOIN sec_filing_manifest m
        ON m.accession_number = fe.provider_filing_id
    WHERE fe.provider = 'sec'
      AND fe.filing_type = ANY(%(forms)s)
      AND fe.provider_filing_id IS NOT NULL
    ORDER BY fe.provider_filing_id, fe.instrument_id
"""


def _fetch_candidates(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(_CANDIDATES_SQL, {"forms": list(B424_FORMS)})
        return cur.fetchall()


def dry_run(conn: psycopg.Connection[Any]) -> None:
    """Report-only: what the seed WOULD do, by subtype. No writes."""
    rows = _fetch_candidates(conn)
    candidates: Counter[str] = Counter()
    manifested: Counter[str] = Counter()
    no_cik: Counter[str] = Counter()
    for _iid, _fdate, form, _acc, _url, cik, already in rows:
        candidates[form] += 1
        if already:
            manifested[form] += 1
        if cik is None or not str(cik).strip():
            no_cik[form] += 1
    logger.info("DRY RUN — seed population by subtype (no writes):")
    logger.info("%-8s %10s %12s %10s %12s", "subtype", "events", "manifested", "no_cik", "would_seed")
    for form in B424_FORMS:
        would = candidates[form] - manifested[form] - no_cik[form]
        logger.info(
            "%-8s %10d %12d %10d %12d",
            form,
            candidates[form],
            manifested[form],
            no_cik[form],
            max(would, 0),
        )
    total_would = sum(candidates.values()) - sum(manifested.values()) - sum(no_cik.values())
    logger.info(
        "TOTAL: events=%d manifested=%d no_cik=%d would_seed=%d",
        sum(candidates.values()),
        sum(manifested.values()),
        sum(no_cik.values()),
        max(total_would, 0),
    )


def seed(conn: psycopg.Connection[Any]) -> int:
    """Insert ``sec_424b`` manifest rows for historical 424B events.

    Mirrors ``backfill_1792_sec_nt.seed``: ``seed_manifest_from_filing_events``'s
    CIK resolution (LATERAL LIMIT 1, primary-preferred) + per-accession dedup,
    filtered to ``B424_FORMS`` so it cannot touch any other source.
    """
    rows = _fetch_candidates(conn)
    upserted = 0
    skipped_no_cik = 0
    for instrument_id, filing_date, filing_type, accession, primary_doc_url, cik_raw, _already in rows:
        if cik_raw is None or not str(cik_raw).strip():
            skipped_no_cik += 1
            continue
        source = map_form_to_source(filing_type) if filing_type else None
        if source != "sec_424b":
            # Defensive: the WHERE already restricts to B424_FORMS, but never
            # seed a row whose canonical routing disagrees with this drive.
            logger.warning(
                "skip accession=%s form=%r routed to %r (expected sec_424b)",
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
                source="sec_424b",
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
    """Drain pending ``sec_424b`` manifest rows in chunks of 50 (S7 recipe)."""
    totals = {"parsed": 0, "tombstoned": 0, "failed": 0, "skipped": 0}
    chunks = 0
    consecutive_failures = 0
    started = time.monotonic()
    while True:
        rows = list(iter_pending(conn, source="sec_424b", limit=50))
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
    parser = argparse.ArgumentParser(description="Historical 424B backfill (#1974)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", help="report seed population, no writes")
    group.add_argument("--seed-only", action="store_true", help="seed manifest, skip drain")
    group.add_argument("--drain-only", action="store_true", help="drain only, skip seed")
    args = parser.parse_args()

    register_all_parsers()
    with psycopg.connect(settings.database_url, application_name="backfill-1974-sec-424b") as conn:
        if args.dry_run:
            dry_run(conn)
            return
        if not args.drain_only:
            seed(conn)
        if not args.seed_only:
            drain(conn)


if __name__ == "__main__":
    main()
