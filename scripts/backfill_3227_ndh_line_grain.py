"""#3227 item 2 — populate the Table I line-grain columns on stored ``:NDH:`` rows.

    PYTHONPATH=. uv run python -m scripts.backfill_3227_ndh_line_grain            # dry run
    PYTHONPATH=. uv run python -m scripts.backfill_3227_ndh_line_grain --apply

Dry run by default. Nothing is written without ``--apply``.

## What it writes, and what it cannot touch

``sql/400`` adds ``security_title`` / ``direct_indirect`` / ``nature_of_ownership`` to
``ownership_insiders_observations``. This script fills them for rows already stored by
``sec_insider_dataset_ingest``'s ``:NDH:`` loop, from the cached DERA archives.

It writes **only those three columns**. No row is created or deleted, no §16 date gate is
re-adjudicated, no ``shares`` value moves, and ``ownership_insiders_current`` is not
refreshed. That is the whole reason it exists rather than a ``sec_rebuild`` of the insider
sources: a rebuild would re-run every gate and re-derive every value, so proving it changed
nothing else would be far more expensive than the change is worth.

## Why matching on ``source_document_id`` is sound

``source_document_id`` is ``{accession}:NDH:{NONDERIV_HOLDING_SK}``. The DERA Insider
readme documents ``NONDERIV_HOLDING_SK`` as the holding surrogate key under
``ACCESSION_NUMBER``, and our corpus conforms: over all 81 cached archives (2,367,536
rows) there are zero blank SKs, zero duplicate ``(ACCESSION_NUMBER, SK)`` pairs within an
archive, and zero ids appearing in more than one archive. So each id has exactly ONE
authoritative payload and ``UPDATE ... FROM`` cannot be ambiguous or archive-order
dependent.

⚠ One id maps to SEVERAL observation rows — the reporting-owner fan-out and the #1117
share-class instrument fan-out each replicate a source line. **Updating every replica is
correct, not a bug**: they all descend from the same source line, so they share one
payload. The reconciliation below reports the replication ratio rather than assuming it.

## ⚠ The denominator is the stored rows, NOT the corpus

``sec_insider_dataset_ingest``'s holdings loop begins
``if accn in accessions_with_transactions: continue``, so 1,840,672 of 2,367,536 cached
holding rows (77.75%) are never written at all. This script cannot invent them; it can only
fill columns on rows that exist. Coverage is reported against stored ``:NDH:`` rows and must
not be read as coverage of the filings. The 77.75% skip is a separate, larger defect in the
same writer, recorded on #3227.
"""

from __future__ import annotations

import argparse
import csv
import io
import zipfile
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.sec_insider_dataset_ingest import _parse_direct_indirect, _parse_text

_HOLDING_NAMES = ("NONDERIV_HOLDING.tsv", "NON_DERIV_HOLDING.tsv")


def _iter_tsv(zf: zipfile.ZipFile, *candidates: str) -> Any:
    names = zf.namelist()
    target: str | None = None
    for candidate in candidates:
        if candidate in names:
            target = candidate
            break
        nested = [n for n in names if n.endswith("/" + candidate)]
        if nested:
            target = nested[0]
            break
    if target is None:
        return iter(())
    fh = zf.open(target)
    return csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8", newline=""), delimiter="\t")


def _archives() -> list[Path]:
    base = resolve_data_dir() / "sec" / "bulk"
    if not base.is_dir():
        return []
    return sorted(p for p in base.iterdir() if p.name.startswith("insider_") and p.name.endswith(".zip"))


def _load_stored_ids(conn: psycopg.Connection[Any]) -> set[str]:
    """Every distinct ``:NDH:`` ``source_document_id`` currently stored.

    Loaded up front so the archive sweep keeps only rows it can actually match —
    holding the whole 2.37M-row corpus in memory to discard 88% of it is waste.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT source_document_id
              FROM ownership_insiders_observations
             WHERE source_document_id LIKE '%%:NDH:%%'
            """
        )
        return {row[0] for row in cur.fetchall()}


def _collect_payloads(stored: set[str]) -> tuple[dict[str, tuple[str | None, str | None, str | None]], int]:
    """Archive payload per stored document id, plus the corpus row count."""
    payloads: dict[str, tuple[str | None, str | None, str | None]] = {}
    corpus_rows = 0
    for archive in _archives():
        with zipfile.ZipFile(archive) as zf:
            for holding in _iter_tsv(zf, *_HOLDING_NAMES):
                corpus_rows += 1
                accn = (holding.get("ACCESSION_NUMBER") or "").strip()
                sk = (holding.get("NONDERIV_HOLDING_SK") or holding.get("NON_DERIV_HOLDING_SK") or "").strip() or "0"
                doc_id = f"{accn}:NDH:{sk}"
                if doc_id not in stored:
                    continue
                payloads[doc_id] = (
                    _parse_text(holding.get("SECURITY_TITLE")),
                    _parse_direct_indirect(holding.get("DIRECT_INDIRECT_OWNERSHIP")),
                    _parse_text(holding.get("NATURE_OF_OWNERSHIP")),
                )
    return payloads, corpus_rows


_STAGE_SQL = """
CREATE TEMP TABLE _stg_3227 (
    source_document_id  TEXT PRIMARY KEY,
    security_title      TEXT,
    direct_indirect     TEXT,
    nature_of_ownership TEXT
) ON COMMIT DROP
"""

_UPDATE_SQL = """
UPDATE ownership_insiders_observations AS o
   SET security_title      = s.security_title,
       direct_indirect     = s.direct_indirect,
       nature_of_ownership = s.nature_of_ownership
  FROM _stg_3227 AS s
 WHERE o.source_document_id = s.source_document_id
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write; omit for a dry run")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        stored = _load_stored_ids(conn)
        print(f"stored distinct :NDH: document ids   {len(stored):,}")

        payloads, corpus_rows = _collect_payloads(stored)
        print(f"cached corpus holding rows           {corpus_rows:,}")
        print(f"matched to an archive payload        {len(payloads):,}")

        unmatched = len(stored) - len(payloads)
        print(f"stored ids with NO archive payload   {unmatched:,}")
        if unmatched:
            print("  ^ expected non-zero only if an archive that fed a past ingest is no longer cached;")
            print("    `_delete_archive_after_success` removes an archive once its ingest succeeds.")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM ownership_insiders_observations WHERE source_document_id LIKE '%%:NDH:%%'"
            )
            row = cur.fetchone()
            target_rows = int(row[0]) if row else 0
        print(f"stored :NDH: observation rows        {target_rows:,}")
        if len(stored):
            print(f"replication ratio (rows / ids)       {target_rows / len(stored):.3f}")

        if not args.apply:
            print()
            print("DRY RUN — nothing written. Re-run with --apply.")
            return

        with conn.cursor() as cur:
            cur.execute(_STAGE_SQL)
            with cur.copy(
                "COPY _stg_3227 (source_document_id, security_title, direct_indirect, nature_of_ownership) FROM STDIN"
            ) as copy:
                for doc_id, (title, dio, nature) in payloads.items():
                    copy.write_row((doc_id, title, dio, nature))
            cur.execute(_UPDATE_SQL)
            updated = cur.rowcount
        conn.commit()

        print()
        print(f"rows updated                         {updated:,}")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)                                     AS ndh_rows,
                       count(security_title)                        AS with_title,
                       count(direct_indirect)                       AS with_dio,
                       count(nature_of_ownership)                   AS with_nature
                  FROM ownership_insiders_observations
                 WHERE source_document_id LIKE '%%:NDH:%%'
                """
            )
            stats = cur.fetchone()
        if stats:
            ndh, title, dio, nature = (int(v) for v in stats)
            print(f"  :NDH: rows                         {ndh:,}")
            print(f"  security_title populated           {title:,}  ({100.0 * title / ndh if ndh else 0:.2f}%)")
            print(f"  direct_indirect populated          {dio:,}  ({100.0 * dio / ndh if ndh else 0:.2f}%)")
            print(f"  nature_of_ownership populated      {nature:,}  ({100.0 * nature / ndh if ndh else 0:.2f}%)")
            print()
            print("  ⚠ nature_of_ownership is EXPECTED to be partial — it is populated on 72.66% of")
            print("    the cached corpus, because Instr. 5(b)(iii) asks for it on indirect lines.")


if __name__ == "__main__":
    main()
