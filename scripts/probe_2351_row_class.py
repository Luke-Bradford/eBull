"""#2351 slice 3b — dry run of the V-shape row rule over the job's full population.

Runs ``compute_desired`` with a fetcher that refuses every network call (uncached covers
count as unresolved) inside a transaction that is rolled back, so it writes nothing.
Prints every desired ``other_common_class_row`` withhold: sibling, accession, holder,
the *Title of class* cell and the witness title.

Usage::

    PYTHONPATH=. uv run python -m scripts.probe_2351_row_class
"""

from __future__ import annotations

from collections import Counter

import httpx
import psycopg

from app.config import settings
from app.services.def14a_recipients import RunReport, compute_desired, load_population


def _no_fetch(url: str) -> str | None:
    raise httpx.TransportError(f"probe is offline: {url}")


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        symbols = {s.instrument_id: s.symbol for sibs in load_population(conn).values() for s in sibs}
        report = RunReport()
        rows = compute_desired(conn, _no_fetch, report).rows
        print(f"accessions={report.accessions} unresolved={report.accessions_unresolved}")
        print(f"other_common_class_row: {len(rows)} rows")
        print(dict(Counter(symbols[k[0]] for k in rows).most_common()))
        for (iid, accession, holder), row in sorted(rows.items(), key=lambda kv: (symbols[kv[0][0]], kv[0][1:])):
            print(f"  {symbols[iid]:8} {accession}  cell={row[11]!r}  witness={row[10]!r}  {holder[:60]!r}")
        conn.rollback()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
