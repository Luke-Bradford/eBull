"""Read-only census of the DEF 14A correct-zero release rule (#2173 / #2371).

Every population figure quoted on #2371 and in PR-body prose comes from here, so that a
number in a docstring can be re-derived instead of trusted. The previous rule's docstring
carried a hand-written "2 of 7,141 accessions … and NONE is mixed"; by 2026-09-17 the real
count was **0**, and nothing failed — "rare" and "dead" read identically once written down.

    PYTHONPATH=. uv run python -m scripts.audit_2371_correct_zero_release

⚠ **Imports the shipped predicate.** It does NOT restate the rule. A census that restates
the code it describes measures a proxy, and reports numbers that were never true of what
ships (precedent this same session: #2788's census quoted a population 326 holders larger
than the predicate selected).

⚠ Read-only: no write path exists in this module.
"""

from __future__ import annotations

import collections

import psycopg
import psycopg.rows

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services.rewash_filings import (
    name_is_not_a_beneficial_owner,
    no_stored_name_is_a_beneficial_owner,
)


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        # One REPEATABLE READ snapshot: the release count and the mixed count are quoted
        # side by side and must describe the same corpus.
        with snapshot_read(conn):
            with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
                cur.execute("SELECT accession_number, holder_name FROM def14a_beneficial_holdings")
                by_accession: dict[str, list[str | None]] = collections.defaultdict(list)
                for accession, name in cur.fetchall():
                    by_accession[accession].append(name)

    releases: list[str] = []
    mixed = blank_bearing = 0
    for accession, names in by_accession.items():
        if no_stored_name_is_a_beneficial_owner(names):
            releases.append(accession)
            continue
        cleaned = [(n or "").strip() for n in names]
        if any(not n for n in cleaned):
            blank_bearing += 1
            continue
        # MIXED is reported separately because it is the case the ALL (never ANY) rule
        # exists to protect: at least one row may be a genuine holder.
        hits = sum(name_is_not_a_beneficial_owner(n) for n in cleaned)
        if 0 < hits < len(cleaned):
            mixed += 1

    total_rows = sum(len(v) for v in by_accession.values())
    released_rows = sum(len(by_accession[a]) for a in releases)
    print(f"accessions with stored rows        {len(by_accession)}")
    print(f"stored rows                        {total_rows}")
    print(f"accessions the rule RELEASES       {len(releases)}")
    print(f"rows those carry                   {released_rows}")
    print(f"accessions MIXED (kept, correctly) {mixed}")
    print(f"accessions with a BLANK name (kept){blank_bearing}")
    print()
    print("released accessions and their stored names:")
    for accession in sorted(releases):
        names = [(n or "")[:48] for n in by_accession[accession]]
        print(f"  {accession}  {names}")
    print()
    print("⚠ The rule only fires when the re-parse ALSO yields zero rows. This census")
    print("  measures the predicate, not the joint condition — re-parse the bodies to")
    print("  size the actual effect.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
