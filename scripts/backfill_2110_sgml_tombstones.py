"""One-shot (#2110): re-apply parsers over SGML-wrapped form3/4/5 raws.

Full-pop scan 2026-07-22 found 14,279 ``form3_xml``/``form4_xml``/
``form5_xml`` raw rows storing the ``.txt`` full-submission SGML
wrapper (master/daily-index discovery + legacy owner-stream fetches) —
11,331 of them tombstoned because ``parse_form_*_xml`` rejected the
wrapper. The parsers now unwrap the embedded ``<ownershipDocument>``
at the chokepoint (#2110), so re-applying the rewash spec over the
wrapped cohort converts tombstones back into typed rows (the entity
upsert sets ``is_tombstone = FALSE`` on conflict). Stored payloads are
NOT mutated — the raw store keeps fetched bytes verbatim.

Idempotent: re-running re-applies the same parses onto the same rows.
Rows that STILL return ``None`` post-unwrap (e.g. holdings-only Form 5
filings, by-design) keep their tombstone.

Usage::

    PYTHONPATH=. uv run python scripts/backfill_2110_sgml_tombstones.py --dry-run
    PYTHONPATH=. uv run python scripts/backfill_2110_sgml_tombstones.py
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.services import raw_filings, rewash_filings

logger = logging.getLogger("backfill_2110")

_KINDS = ("form3_xml", "form4_xml", "form5_xml")

_COHORT_SQL = """
SELECT accession_number
FROM filing_raw_documents
WHERE document_kind = %(kind)s
  AND payload IS NOT NULL
  AND left(payload, 200) LIKE '%%<SEC-DOCUMENT>%%'
ORDER BY accession_number
"""


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="count the cohort, apply nothing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    specs = rewash_filings.registered_specs()

    totals = {"applied": 0, "still_unparsed": 0, "failed": 0}
    with psycopg.connect(settings.database_url) as conn:
        for kind in _KINDS:
            accessions = [row[0] for row in conn.execute(_COHORT_SQL, {"kind": kind}).fetchall()]
            logger.info("%s: %d SGML-wrapped raw rows", kind, len(accessions))
            if args.dry_run:
                continue
            spec = specs[kind]
            for accession in accessions:
                raw_doc = raw_filings.read_raw(conn, accession_number=accession, document_kind=kind)  # type: ignore[arg-type]
                if raw_doc is None or raw_doc.payload is None:
                    continue
                try:
                    outcome = spec.apply_fn(conn, raw_doc)
                    conn.commit()
                except psycopg.OperationalError, psycopg.InterfaceError:
                    # Connection-level death: every subsequent apply would
                    # fail too — abort loudly (PR #2111 review lesson).
                    logger.exception("connection failure at accession=%s — aborting", accession)
                    raise
                except Exception:  # noqa: BLE001 — one bad accession must not abort the sweep
                    logger.exception("apply failed accession=%s kind=%s", accession, kind)
                    conn.rollback()
                    totals["failed"] += 1
                    continue
                # apply_fn contract (rewash_filings.RewashSpec): True =
                # typed upsert ran; False = parse returned None (deliberate
                # skip — e.g. holdings-only Form 5), tombstone stays.
                if outcome:
                    totals["applied"] += 1
                else:
                    totals["still_unparsed"] += 1

    logger.info("done: %s", totals)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
