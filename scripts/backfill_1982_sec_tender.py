"""Scoped seed + drain of historical tender / going-private schedules (#1982).

Seeds ``sec_tender`` manifest rows for every SC TO-T / SC TO-I / SC 14D9 /
SC 13E3 (+ /A) ``filing_events`` row (population ≈ 1,783, 2016-2026), then
drains them standalone. Mirrors ``backfill_1792_sec_nt.py`` (scoped seed, not
the universe seed — see that script's rationale) and adds ``--report``: the
full-population dry-run acceptance summary the #1982 spec gates on
(role-row yield, checkbox NULL-rate, price / expiration / recommendation
hit-rates by form).

Dual-attributed accessions (subject + offeror both in universe) exist as TWO
filing_events rows but ONE manifest row (accession is the PK) — the seed's
per-accession dedup keeps whichever party's row sorts first; the parser
derives BOTH parties' roles from the SGML header regardless (#1982 design).

Usage:
    uv run python scripts/backfill_1982_sec_tender.py              # seed then drain
    uv run python scripts/backfill_1982_sec_tender.py --seed-only  # create rows, let daemon drain
    uv run python scripts/backfill_1982_sec_tender.py --drain-only # drain existing pending
    uv run python scripts/backfill_1982_sec_tender.py --report     # acceptance report only
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
from app.services.tender_offers import IN_SCOPE_FORMS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("backfill_1982")


def seed(conn: psycopg.Connection[Any]) -> int:
    """Insert ``sec_tender`` manifest rows for historical schedule events.

    Mirrors ``seed_manifest_from_filing_events``'s CIK resolution (LATERAL
    LIMIT 1, primary-preferred) and per-accession dedup, filtered to the
    eight in-scope forms so it cannot touch any other source.
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
            {"forms": sorted(IN_SCOPE_FORMS)},
        )
        rows = cur.fetchall()

    upserted = 0
    skipped_no_cik = 0
    for instrument_id, filing_date, filing_type, accession, primary_doc_url, cik_raw in rows:
        if cik_raw is None or not str(cik_raw).strip():
            skipped_no_cik += 1
            continue
        source = map_form_to_source(filing_type) if filing_type else None
        if source != "sec_tender":
            logger.warning(
                "skip accession=%s form=%r routed to %r (expected sec_tender)",
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
                source="sec_tender",
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
    """Drain pending ``sec_tender`` manifest rows in chunks of 50 (S7 recipe)."""
    totals = {"parsed": 0, "tombstoned": 0, "failed": 0, "skipped": 0}
    chunks = 0
    consecutive_failures = 0
    started = time.monotonic()
    while True:
        rows = list(iter_pending(conn, source="sec_tender", limit=50))
        if not rows:
            break
        try:
            stats = _dispatch_rows(conn, rows, now=datetime.now(tz=UTC))
        except Exception:
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


def report(conn: psycopg.Connection[Any]) -> None:
    """Full-population acceptance report (#1982 spec gate)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT form, ingest_status, count(*)
            FROM sec_filing_manifest WHERE source = 'sec_tender'
            GROUP BY 1, 2 ORDER BY 1, 2
            """
        )
        print("\n== manifest outcome by form ==")
        for form, status, n in cur.fetchall():
            print(f"  {form:<12} {status:<12} {n}")

        cur.execute(
            """
            SELECT form,
                   count(*) AS rows,
                   count(DISTINCT accession_number) AS accessions,
                   count(*) FILTER (WHERE role = 'offeror') AS offeror_rows,
                   count(*) FILTER (WHERE is_third_party_tender IS NOT NULL) AS box_resolved,
                   count(offer_price_per_unit) AS with_price,
                   count(expiration_date) AS with_expiration,
                   count(board_recommendation) AS with_recommendation
            FROM tender_offer_events GROUP BY form ORDER BY form
            """
        )
        print("\n== typed-row yield by form ==")
        header = ("form", "rows", "accs", "offeror", "boxes", "price", "expir", "recomm")
        print(f"  {header[0]:<12} " + " ".join(f"{h:>7}" for h in header[1:]))
        for row in cur.fetchall():
            print(f"  {row[0]:<12} " + " ".join(f"{v:>7}" for v in row[1:]))

        cur.execute(
            """
            SELECT board_recommendation, count(*)
            FROM tender_offer_events
            WHERE form LIKE 'SC 14D9%%'
            GROUP BY 1 ORDER BY 2 DESC
            """
        )
        print("\n== 14D-9 recommendation distribution ==")
        for rec, n in cur.fetchall():
            print(f"  {rec or 'NULL':<10} {n}")


def main() -> None:
    parser = argparse.ArgumentParser(description="tender/going-private backfill (#1982)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--seed-only", action="store_true", help="seed manifest, skip drain")
    group.add_argument("--drain-only", action="store_true", help="drain only, skip seed")
    group.add_argument("--report", action="store_true", help="acceptance report only")
    args = parser.parse_args()

    register_all_parsers()
    with psycopg.connect(settings.database_url, application_name="backfill-1982-sec-tender") as conn:
        if args.report:
            report(conn)
            return
        if not args.drain_only:
            seed(conn)
        if not args.seed_only:
            drain(conn)
        report(conn)


if __name__ == "__main__":
    main()
