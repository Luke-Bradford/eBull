"""Repair RegSHO rows that a CQS suffix symbol wrote onto a common stock (#3437).

Before #3437, ``TpC`` (a preferred line) upper-cased to ``TPC`` and was stored
against that instrument. Two shapes resulted, per (instrument, source document):

* the suffix line's facility union differed from the common's, so it is an
  EXTRA row (market not produced by any common line) -> deleted;
* the unions matched, so the primary key kept whichever line came last; when
  that was the suffix line, the stored figures are the preferred's -> restored
  to the common line's figures from the raw file.

Everything is re-derived from the stored raw payloads (``filing_raw_documents``,
kind ``finra_regsho_daily_txt``) with the ingest's own row parsers. A stored row
is touched only when it carries a suffix line's exact market and figures in the
same document, for an instrument that has ever held the upper-cased symbol.

``known_from`` policy: a restored row gets ``known_from = NOW()``, the same
stamp the ingest's own upsert writes whenever it rewrites a row's figures
(``finra_regsho_ingest.py``, ``ON CONFLICT ... known_from = NOW()``). The table
keeps no history, so the corrected figures are known from when they were
written. Deleted rows are derived data, re-derivable from the retained raw
files.

Also scans the stored short-interest files for suffix symbols, the population
check behind the SI ingest's guard.

Usage (dry run prints counts; ``--apply`` commits them in one transaction)::

    PYTHONPATH=. uv run python -m scripts.backfill_3437_regsho_suffix_rows [--apply]
"""

from __future__ import annotations

import argparse
import csv
import io
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.finra_regsho_ingest import (
    REGSHO_EXPECTED_HEADER,
    split_regsho_body_row,
    validate_regsho_body_fields,
)
from app.services.finra_short_interest_ingest import is_cqs_suffix_symbol, normalise_symbol

_Figures = tuple[Decimal, Decimal, Decimal]


def _parse_document(payload: str) -> list[tuple[str, str, _Figures]]:
    """(symbol, market, figures) for every valid body line."""
    lines = [ln.rstrip("\r") for ln in payload.split("\n")]
    if tuple(lines[0].split("|")) != REGSHO_EXPECTED_HEADER:
        # The ingest's own header contract: it refused to ingest any file that
        # fails it, so every stored row came from a file that passes (the dry
        # run's "of N" line counts them). A failure here means a corrupt store,
        # and the run aborts before anything is committed.
        raise ValueError(f"unexpected RegSHO header: {lines[0]!r}")
    rows: list[tuple[str, str, _Figures]] = []
    for line in lines[1:]:
        parts = split_regsho_body_row(line)
        if parts is None:
            continue
        row = validate_regsho_body_fields(parts)
        if row is None:
            continue
        rows.append((row.symbol, row.market, (row.short_volume, row.short_exempt_volume, row.total_volume)))
    return rows


def _candidate_instruments(conn: psycopg.Connection[Any]) -> tuple[dict[str, set[int]], set[tuple[str, int]]]:
    """Normalised symbol -> every instrument that has ever carried it, plus the
    (key, instrument) pairs whose identity is unresolved.

    Wider than the ingest resolver (tradable, current symbol only) so a row
    written for a since-renamed or delisted instrument is still found; the
    exact-figures match in ``main`` keeps the wider net from touching others.

    Unresolved: a US-listed instrument whose symbol reaches the key through a
    dot (``AXIA.PC``). On a US venue a dotted suffix marks a class or preferred
    listing, so it may be the very security the CQS suffix line reports, and
    its rows are left alone. A non-US dotted symbol (``AUB.PA``, Euronext
    Paris) is a venue suffix and collides like any other.
    """
    out: dict[str, set[int]] = {}
    unresolved: set[tuple[str, int]] = set()
    for iid, symbol, country in conn.execute(
        "SELECT s.instrument_id, s.symbol, e.country "
        "FROM (SELECT instrument_id, symbol FROM instruments "
        "      UNION SELECT instrument_id, symbol FROM instrument_symbol_history) s "
        "JOIN instruments i ON i.instrument_id = s.instrument_id "
        "LEFT JOIN exchanges e ON e.exchange_id = i.exchange"
    ):
        if not symbol:
            continue
        key = normalise_symbol(symbol)
        out.setdefault(key, set()).add(int(iid))
        if "." in symbol and country == "US":
            unresolved.add((key, int(iid)))
    return out, unresolved


def _scan_short_interest(conn: psycopg.Connection[Any]) -> tuple[int, int, int]:
    files = rows = suffix = 0
    for (payload,) in conn.execute(
        "SELECT payload FROM filing_raw_documents WHERE document_kind = 'finra_short_interest_csv'"
    ):
        files += 1
        for row in csv.DictReader(io.StringIO(payload), delimiter="|"):
            rows += 1
            suffix += is_cqs_suffix_symbol((row.get("symbolCode") or "").strip())
    return files, rows, suffix


@dataclass
class RepairStats:
    documents: int = 0
    documents_scanned: int = 0
    deleted: int = 0
    restored: int = 0
    left_unresolved: dict[int, int] = field(default_factory=dict)


def repair(conn: psycopg.Connection[Any]) -> RepairStats:
    """Apply the repair on ``conn`` without committing; the caller decides."""
    stats = RepairStats()
    candidates, unresolved = _candidate_instruments(conn)
    raw = conn.execute(
        "SELECT accession_number, payload FROM filing_raw_documents "
        "WHERE document_kind = 'finra_regsho_daily_txt' ORDER BY accession_number"
    ).fetchall()
    for accession, payload in raw:
        parsed = _parse_document(payload)
        # accession FINRA_REGSHO_<prefix>_<yyyymmdd> -> source_document_id <prefix>_<yyyymmdd>
        source_document_id = accession.removeprefix("FINRA_REGSHO_")
        common = {
            (normalise_symbol(symbol), market): figures
            for symbol, market, figures in parsed
            if not is_cqs_suffix_symbol(symbol)
        }
        touched = False
        for symbol, market, figures in parsed:
            if not is_cqs_suffix_symbol(symbol):
                continue
            key = normalise_symbol(symbol)
            iids = candidates.get(key)
            if not iids:
                continue
            # Only a stored row carrying this suffix line's exact figures can
            # have come from it; a row with the common's figures is correct.
            hits = conn.execute(
                "SELECT instrument_id FROM finra_regsho_daily_observations "
                "WHERE source_document_id = %s AND instrument_id = ANY(%s) AND market = %s "
                "AND short_volume = %s AND short_exempt_volume = %s AND total_volume = %s",
                (source_document_id, sorted(iids), market, *figures),
            ).fetchall()
            want = common.get((key, market))
            for (iid,) in hits:
                if want == figures:
                    continue  # the common line reported identical figures
                if (key, iid) in unresolved:
                    stats.left_unresolved[iid] = stats.left_unresolved.get(iid, 0) + 1
                    continue
                touched = True
                if want is None:
                    stats.deleted += 1
                    conn.execute(
                        "DELETE FROM finra_regsho_daily_observations "
                        "WHERE source_document_id = %s AND instrument_id = %s AND market = %s",
                        (source_document_id, iid, market),
                    )
                else:
                    stats.restored += 1
                    conn.execute(
                        "UPDATE finra_regsho_daily_observations "
                        "SET short_volume = %s, short_exempt_volume = %s, total_volume = %s, "
                        "known_from = NOW() "
                        "WHERE source_document_id = %s AND instrument_id = %s AND market = %s",
                        (*want, source_document_id, iid, market),
                    )
        stats.documents += touched
    stats.documents_scanned = len(raw)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="commit the repair (default: dry run)")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        stats = repair(conn)
        si_files, si_rows, si_suffix = _scan_short_interest(conn)
        if args.apply:
            conn.commit()
        else:
            conn.rollback()

    print(f"regsho documents repaired: {stats.documents} of {stats.documents_scanned}")
    print(f"rows deleted (suffix-only market): {stats.deleted}")
    print(f"rows restored to the common's figures: {stats.restored}")
    print(f"rows left, instrument identity unresolved (instrument_id: rows): {stats.left_unresolved}")
    print(f"short-interest files scanned: {si_files}, rows: {si_rows}, suffix-symbol rows: {si_suffix}")
    print("APPLIED" if args.apply else "DRY RUN (rolled back)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
