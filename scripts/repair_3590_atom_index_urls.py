"""#3590 repair: re-queue manifest rows tombstoned on an EDGAR filing-INDEX page.

    PYTHONPATH=. uv run python -m scripts.repair_3590_atom_index_urls            # dry run (default)
    PYTHONPATH=. uv run python -m scripts.repair_3590_atom_index_urls --apply

The getcurrent Atom fast lane recorded each entry's filing-index page (``…/{acc}-index.htm``) as
``primary_document_url``; parsers fetched that HTML, stored it, failed to parse it and tombstoned the row.
A row is re-queued when it is ``tombstoned`` AND a stored ``filing_raw_documents`` row for its accession was
fetched from a filing-index page — i.e. the parser actually read the index. Tombstones decided before any
fetch (retention floor, missing instrument_id, 424B2 volume cap, latest-N cap) do not depend on the URL and
are left alone. For each re-queued row, in one transaction:

- an index-page ``primary_document_url`` becomes the complete-submission ``.txt``
  (``sec_manifest.complete_submission_url``, the daily-index URL shape);
- ``ingest_status = 'pending'``, ``next_retry_at`` and ``error`` cleared — the ``sec_rebuild`` reset.

The manifest worker then drains the rows; ``raw_filings.stored_body`` no longer hands back an index-page
body, so each re-drain fetches the real document. Idempotent: a second run finds nothing to do once the
worker has re-parsed the rows. Prints counts per source before writing.
"""

from __future__ import annotations

import argparse
from collections import Counter
from typing import Any

import psycopg

from app.config import settings
from app.services.sec_manifest import complete_submission_url, is_filing_index_url

# Prefilter only; ``is_filing_index_url`` is the predicate.
_CANDIDATES_SQL = """
    SELECT m.accession_number, m.source, m.primary_document_url,
           ARRAY(SELECT r.source_url FROM filing_raw_documents r
                  WHERE r.accession_number = m.accession_number AND r.source_url LIKE %(pat)s) AS body_urls
      FROM sec_filing_manifest m
     WHERE m.ingest_status = 'tombstoned'
       AND EXISTS (SELECT 1 FROM filing_raw_documents r
                    WHERE r.accession_number = m.accession_number AND r.source_url LIKE %(pat)s)
"""


def _plan(conn: psycopg.Connection[Any]) -> list[tuple[str, str, str | None, bool]]:
    """``(accession, source, new_url, url_rewritten)`` for every row to re-queue."""
    plan: list[tuple[str, str, str | None, bool]] = []
    for accession, source, url, body_urls in conn.execute(_CANDIDATES_SQL, {"pat": "%-index.htm%"}):
        if not any(is_filing_index_url(u) for u in body_urls):
            continue
        url_is_index = is_filing_index_url(url)
        plan.append((accession, source, complete_submission_url(url) if url_is_index else url, url_is_index))
    return plan


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write; default is a dry run")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            plan = _plan(conn)
            by_source = Counter(source for _, source, _, _ in plan)
            rewritten = Counter(source for _, source, _, url_rewritten in plan if url_rewritten)
            print(f"rows to re-queue: {len(plan)}")
            for source, n in sorted(by_source.items(), key=lambda kv: -kv[1]):
                print(f"  {source:<14} {n:>7}   (index-page manifest URL rewritten to .txt: {rewritten[source]})")
            if not args.apply:
                print("dry run — nothing written (pass --apply)")
                return
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    UPDATE sec_filing_manifest
                       SET primary_document_url = %s, ingest_status = 'pending', next_retry_at = NULL, error = NULL
                     WHERE accession_number = %s AND ingest_status = 'tombstoned'
                    """,
                    [(new, accession) for accession, _, new, _ in plan],
                )
            print(f"applied: {len(plan)} rows re-queued")


if __name__ == "__main__":
    main()
