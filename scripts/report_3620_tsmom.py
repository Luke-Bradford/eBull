"""#3620 slice 1: the census for the cross-asset TSMOM study — coverage, verdicts, refusals, start, E, stamp audit.

Spec: ``docs/research/2026-10-06-3620-cross-asset-tsmom.md`` §"Universe and eligibility", §Returns ("Distribution
capture is unverified") and §"Hold-out inspection, stated". It loads panel rows through E to decide coverage, and
prints no return value, signal, slot or statistic. The gated run path is slice 3 and does not exist yet.

    PYTHONPATH=. uv run python -m scripts.report_3620_tsmom --census
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any

import psycopg

from app.config import settings
from app.services.etf_total_return_reader import EtfMonthlyReturn, EtfVerdict, load_etf_total_return_panel
from app.services.total_return_reader import INTRADER_VENDOR, SWITCH_MONTH, add_months
from app.services.tsmom_etf import (
    CLASSES,
    COMPARATORS,
    COVERAGE_CAP,
    FUNDS,
    POOLS,
    FundCoverage,
    TsmomRefusal,
    build_census,
    fund_coverage,
    stamp_audit,
)

#: Per symbol: its Intrader series' first bar, and per year whether it has a January and a December bar.
_BARS_SQL = """
SELECT s.vendor_symbol, extract(year FROM d.bar_date)::int AS year,
       bool_or(extract(month FROM d.bar_date) = 1), bool_or(extract(month FROM d.bar_date) = 12), min(d.bar_date)
FROM research_price_daily d JOIN research_price_series s USING (series_id)
WHERE s.vendor = %(vendor)s AND s.vendor_symbol = ANY(%(symbols)s::text[])
GROUP BY 1, 2
"""

#: The distribution event: a row of the fund's Intrader series with a non-zero ``dividend`` (spec §Returns).
_STAMPS_SQL = """
SELECT s.vendor_symbol, d.bar_date
FROM research_price_daily d JOIN research_price_series s USING (series_id)
WHERE s.vendor = %(vendor)s AND s.vendor_symbol = ANY(%(symbols)s::text[]) AND d.dividend <> 0
"""


def census(conn: psycopg.Connection[Any]) -> int:
    symbols = sorted(set(FUNDS) | set(COMPARATORS))
    panel = load_etf_total_return_panel(conn, symbols, include_price_return=False)
    rows: dict[str, list[EtfMonthlyReturn]] = {}
    for row in panel.rows:
        rows.setdefault(row.symbol, []).append(row)
    print(f"panel version {panel.version}; {panel.nport_snapshot}")

    coverage: dict[str, FundCoverage] = {}
    refusals: list[str] = []
    for symbol in symbols:
        verdict = panel.verdicts.get(symbol, EtfVerdict.NO_INTRADER_SERIES)
        try:
            coverage[symbol] = fund_coverage(symbol, verdict, rows.get(symbol, []))
        except TsmomRefusal as exc:
            refusals.append(str(exc))

    first_bar: dict[str, Any] = {}
    full_years: dict[str, dict[int, bool]] = {}
    for symbol, year, has_jan, has_dec, first in conn.execute(
        _BARS_SQL, {"vendor": INTRADER_VENDOR, "symbols": symbols}
    ).fetchall():
        first_bar[symbol] = min(first, first_bar.get(symbol, first))
        full_years.setdefault(symbol, {})[int(year)] = bool(has_jan and has_dec)
    stamps: dict[str, list[Any]] = {}
    for symbol, bar_date in conn.execute(_STAMPS_SQL, {"vendor": INTRADER_VENDOR, "symbols": symbols}).fetchall():
        stamps.setdefault(symbol, []).append(bar_date)

    result = None
    if not refusals:
        try:
            result = build_census({s: coverage[s] for s in FUNDS}, {s: coverage[s] for s in COMPARATORS})
        except TsmomRefusal as exc:
            refusals.append(str(exc))

    print("\nsymbol  class               verdict                first_bar   first_m  last_m   first_eligible")
    class_of = {s: name for name, members in CLASSES.items() for s in members}
    for symbol in symbols:
        c = coverage.get(symbol)
        print(
            f"{symbol:<7} {class_of.get(symbol, 'comparator'):<19} "
            f"{panel.verdicts.get(symbol, EtfVerdict.NO_INTRADER_SERIES).value:<22} "
            f"{first_bar.get(symbol, '-')!s:<11} "
            + (
                f"{c.first_month[0]}-{c.first_month[1]:02d}  {c.last_month[0]}-{c.last_month[1]:02d}  "
                f"{c.first_eligible[0]}-{c.first_eligible[1]:02d}"
                if c
                else "-"
            )
        )

    print("\nstamp audit (Intrader distribution rows per year; full = bars in January and December)")
    for symbol in symbols:
        if symbol not in full_years:
            print(f"{symbol:<7} no Intrader series")
            continue
        # The last Intrader month the panel uses, never past E (pools stay on Intrader to E; the rest switch).
        end = result.end if result is not None else COVERAGE_CAP
        last_used = end if symbol in POOLS else min(end, add_months(SWITCH_MONTH, -1))
        audit = stamp_audit(symbol, stamps.get(symbol, []), full_years[symbol], last_used)
        flagged = ", ".join(str(y) for y in audit.flagged) or "none"
        years = " ".join(f"{y.year}:{y.count}{'' if y.full else 'p'}" for y in audit.years)
        print(f"{symbol:<7} {audit.status:<20} mode={audit.mode} flagged={flagged}  [{years}]")

    if result is not None:
        print(
            f"\nstart S = {result.start[0]}-{result.start[1]:02d}; E = {result.end[0]}-{result.end[1]:02d} "
            f"(limited by {', '.join(result.limiting)}); reported months S+1..E"
        )
        for name, members in result.start_constituents.items():
            print(f"  eligible at S, {name}: {', '.join(members) or '-'}")
    if refusals:
        print("\nREFUSED:")
        for line in refusals:
            print(f"  {line}")
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--census", action="store_true", help="print coverage, refusals, start, E and stamp audit")
    args = parser.parse_args(argv)
    if not args.census:
        parser.error("slice 1 has only --census; the gated run path is slice 3")
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        return census(conn)


if __name__ == "__main__":
    sys.exit(main())
