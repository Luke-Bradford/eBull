"""Operator CLI — discover and seed top active 13F-HR filers.

Usage::

    # Dry-run: print top-N candidates from the last 4 quarters
    uv run python -m scripts.seed_top_13f_filers --top-n 200

    # Apply via verification gate
    uv run python -m scripts.seed_top_13f_filers --top-n 200 --apply

Walks SEC's quarterly form.idx for the past N quarters, aggregates
13F-HR filing counts per CIK, takes the top-N, runs each through
the filer_seed_verification gate (PR #821), persists matches via
seed_filer.

Filing-count is a noisy proxy for AUM — the household-name top
managers (Vanguard, BlackRock, Fidelity, State Street) file once
per quarter and rank low. The output IS useful for surfacing
active filers the operator can hand-pick from. AUM-based ranking
needs primary_doc.xml fetches (tracked as a follow-up).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import psycopg

from app.config import settings
from app.services import filer_seed_verification
from app.services.institutional_holdings import seed_filer
from app.services.top_filer_discovery import aggregate_top_filers


def _last_n_quarters(today: date, n: int) -> list[tuple[int, int]]:
    """Return the last ``n`` (year, quarter) tuples ending at the
    quarter PRECEDING ``today`` — the current quarter's form.idx
    is incomplete until the quarter ends."""
    cur_q = (today.month - 1) // 3 + 1  # 1..4
    cur_year = today.year
    out: list[tuple[int, int]] = []
    y, q = cur_year, cur_q - 1
    if q == 0:
        q = 4
        y -= 1
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return out


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed top 13F-HR filers from SEC form.idx.")
    p.add_argument("--quarters", type=int, default=4, help="Number of quarters to aggregate.")
    p.add_argument("--top-n", type=int, default=200, help="Take this many filers from the count ranking.")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Persist verified filers via seed_filer. Default is dry-run.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    quarters = _last_n_quarters(date.today(), args.quarters)
    print(f"Aggregating 13F-HR filings across quarters: {quarters}")

    candidates = aggregate_top_filers(quarters, top_n=args.top_n)
    print(f"Top {len(candidates)} candidates by filing count:")
    for c in candidates:
        print(f"  {c.cik}  {c.filing_count:4d}  {c.latest_name}")

    if not args.apply:
        print("\nDry run — pass --apply to verify + persist.")
        return 0

    matched = 0
    drift = 0
    fetch_errors = 0
    inserted = 0
    skipped_existing = 0
    with psycopg.connect(settings.database_url) as conn:
        # Pre-load existing curated CIKs so the CLI doesn't
        # silently overwrite operator-curated display labels with
        # raw SEC names. Existing rows are operator's choice;
        # discovery only ADDS new rows. Codex pre-push review
        # caught the prior label-clobber bug.
        with conn.cursor() as cur:
            cur.execute("SELECT cik FROM institutional_filer_seeds")
            existing_ciks = {row[0] for row in cur.fetchall()}

        for c in candidates:
            if c.cik in existing_ciks:
                skipped_existing += 1
                continue
            result = filer_seed_verification.verify_seed(
                conn,
                cik=c.cik,
                expected_name=c.latest_name,
            )
            if result.status == "match":
                matched += 1
                seed_filer(
                    conn,
                    cik=c.cik,
                    label=c.latest_name,
                    expected_name=c.latest_name,
                )
                inserted += 1
            elif result.status == "drift":
                drift += 1
                print(f"  DRIFT  {c.cik}  expected={c.latest_name!r} sec={result.sec_name!r}")
            elif result.status == "fetch_error":
                fetch_errors += 1
                print(f"  FETCH_ERROR  {c.cik}  {result.detail}")
        conn.commit()

    print(
        f"\nSummary: existing_preserved={skipped_existing} matched={matched} drift={drift} "
        f"fetch_errors={fetch_errors} inserted={inserted}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
