"""One-shot eviction of out-of-window ``financial_facts_raw`` rows (#1218).

Mirrors the parser-side guard at
``app/providers/implementations/sec_fundamentals.py``::
``_classify_period_rejection`` so the DB and the parser disagree on
exactly nothing. Removes any historical bleed that pre-dates the
parser fix, defensively re-runnable if the bug ever re-surfaces.

Window: ``[1900-01-01, 2100-01-01)``. Also evicts ``period_start >
period_end`` (negative-duration parser-bug class). Spec:
``docs/superpowers/specs/2026-05-19-1218-parser-period-end.md``.

Usage:

  uv run python scripts/cleanup_1218_out_of_window_facts.py            # dry-run
  uv run python scripts/cleanup_1218_out_of_window_facts.py --apply    # delete

Default mode prints the affected row count + a 20-row sample without
deleting. ``--apply`` performs the delete in a single transaction and
prints the deleted count. Idempotent: a second ``--apply`` run on a
clean DB reports 0 and exits 0.
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg

# Keep aligned with ``_PERIOD_MIN`` / ``_PERIOD_MAX`` in
# ``app/providers/implementations/sec_fundamentals.py``. If the parser
# window moves, this predicate moves with it.
_WINDOW_PREDICATE = (
    "period_end   <  DATE '1900-01-01' "
    "OR period_end   >= DATE '2100-01-01' "
    "OR period_start <  DATE '1900-01-01' "
    "OR period_start >= DATE '2100-01-01' "
    "OR (period_start IS NOT NULL AND period_start > period_end)"
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Perform the delete. Without this flag the script is a dry-run.",
    )
    args = ap.parse_args(argv)

    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/ebull",
    )
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM financial_facts_raw WHERE {_WINDOW_PREDICATE}")
        row = cur.fetchone()
        assert row is not None
        n = int(row[0])
        if n == 0:
            print("0 out-of-window rows; nothing to do.")
            return 0

        cur.execute(
            "SELECT period_start, period_end, accession_number, form_type, concept "
            f"FROM financial_facts_raw WHERE {_WINDOW_PREDICATE} "
            "ORDER BY period_end LIMIT 20"
        )
        print(f"{n} out-of-window rows. Sample (up to 20):")
        for sample_row in cur.fetchall():
            print(" ", sample_row)

        if not args.apply:
            print("Dry-run. Re-run with --apply to delete.")
            return 0

        cur.execute(f"DELETE FROM financial_facts_raw WHERE {_WINDOW_PREDICATE}")
        conn.commit()
        print(f"Deleted {n} rows.")
        return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
