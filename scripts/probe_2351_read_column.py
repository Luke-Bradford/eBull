"""#2351 slice 3 — dry run of the class-column rule over the job's full population.

Runs ``compute_desired`` with a fetcher that refuses every network call (uncached covers
count as unresolved) inside a transaction that is rolled back, so it writes nothing. Prints
every desired ``other_common_class_column`` suppression with its witness title, the
suppressed sibling's row count per class letter, and the distinct header captions read.

Usage::

    PYTHONPATH=. uv run python -m scripts.probe_2351_read_column
"""

from __future__ import annotations

from collections import Counter

import httpx
import psycopg

from app.config import settings
from app.services.def14a_recipients import (
    REASON_CLASS_COLUMN,
    RunReport,
    compute_desired,
    load_population,
    load_row_locations,
    location_label,
    row_label,
)


def _no_fetch(url: str) -> str | None:
    raise httpx.TransportError(f"probe is offline: {url}")


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        symbols = {s.instrument_id: s.symbol for sibs in load_population(conn).values() for s in sibs}
        report = RunReport()
        desired = compute_desired(conn, _no_fetch, report).accessions
        hits = sorted(
            (k[0], symbols[k[0]], k[1], row[9]) for k, row in desired.items() if row[3] == REASON_CLASS_COLUMN
        )
        print(
            f"accessions={report.accessions} unresolved={report.accessions_unresolved}",
            f"no_date={report.accessions_no_date}",
        )
        print(f"{REASON_CLASS_COLUMN}: {len(hits)} (instrument, accession) pairs")
        for iid, symbol, accession, witness_title in hits:
            rows = [locs for _, locs in load_row_locations(conn, accession, [iid])[iid]]
            letters = Counter(row_label(locs) for locs in rows)
            captions = sorted({" / ".join(loc.captions) for locs in rows for loc in locs if location_label(loc)})
            print(f"  {symbol:10} {accession}  witness: {witness_title}")
            print(f"      rows by letter: {dict(letters)}; captions: {captions}")
        conn.rollback()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
