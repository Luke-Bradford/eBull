"""#3238 census: which cost basis does each backtest universe actually carry?

Read-only. Every figure is computed here and printed; none is written by hand
into prose, a comment or a docstring (``.claude/CLAUDE.md``: *"never hardcode a
derived statistic"*).

Run from a worktree::

    PYTHONPATH=. uv run python -m scripts.census_3238_cost_basis

What it establishes, in order:

1. The **pinned basis** per universe, taken from the engine's own
   ``universe_selection.vendor_for`` and the ``adjustment_basis`` stored on the
   admitted series — not from a vendor literal.
2. The **band mix** the nominal opens would select, for the universe whose
   archive is genuinely unadjusted. Bands and edges are read from
   ``cost_model.BANDS``; nothing here restates them.
3. The **coverage gap** — how much of the admitted set this census can and
   cannot speak for. A band mix over a partial population is a claim about the
   part, and saying which part is the point.

⚠ Reported quantities are named for their arithmetic. ``p75_spread_pct`` is a
ROUND TRIP; ``half_spread_pct`` is one side and is half of it
(``cost_model.PriceBand.half_spread_pct``). An earlier draft of #3238 labelled a
round-trip figure "half-spread" and was out by 2x. The RATIO between charged and
banded is unaffected, being a ratio of like quantities — which is exactly why the
error survived a read.

⚠ These are BAR-weighted, never trade-weighted. Backtest legs are not stored
(``strategy_promotion_evidence`` is empty — #3104's missing producer), so no
per-trade figure is derivable without a run. Nothing printed here describes the
effect on any verdict.
"""

from __future__ import annotations

import argparse
from decimal import Decimal

import psycopg
from psycopg import sql

from app.config import settings
from app.services.backtest_run import EVALUATION_WINDOW_START
from app.services.cost_model import BANDS, UNKNOWN_NOMINAL_PRICE_BAND
from app.services.research_corpus_ingest import RESEARCH_ARCHIVES
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import load_universe_selection, vendor_for

UNIVERSES = ("survivor_only", "survivorship_free")

#: The fill an entry is costed on. ``signal_ledger.resolve_fills``: *"the fill
#: price is that bar's OPEN"*. Measuring ``close`` here would describe a price
#: no leg is ever charged on.
FILL_COLUMN = "open"

#: Mirrors ``_OPPORTUNITY_SERIES_SQL``'s own usability filter, so the census
#: counts the bars a run could actually fill on rather than every stored row.
_BASIS_SQL = """
    SELECT adjustment_basis, count(*)
    FROM research_price_series
    WHERE series_id = ANY(%(ids)s)
    GROUP BY 1 ORDER BY 2 DESC
"""

#: ⚠ A LITERAL, not an f-string over ``FILL_COLUMN``. psycopg types ``execute``
#: to ``LiteralString`` and an f-string is a plain ``str``, so templating the
#: column name here fails ``pyright`` — and reaching for ``sql.Identifier`` to
#: get around that would be machinery for a column that never varies.
_BAND_COUNT_SQL = """
    SELECT count(*) AS bars,
           min(d.bar_date) AS first_bar,
           max(d.bar_date) AS last_bar,
           {buckets}
    FROM research_price_daily d
    WHERE d.series_id = ANY(%(ids)s)
      AND d.bar_date >= %(start)s
      AND d.open > 0
      AND d.open::text NOT IN ('NaN', 'Infinity', '-Infinity')
"""

_PAIRED_SQL = """
    SELECT count(*) FROM research_price_series a
    WHERE a.series_id = ANY(%(ids)s)
      AND a.instrument_id IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM research_price_series b
        WHERE b.instrument_id = a.instrument_id AND b.vendor <> a.vendor
      )
"""


def _bucket_sql() -> sql.Composed:
    """One COUNT per band, with the edges READ from ``BANDS``, never retyped.

    The edges travel as ``sql.Literal`` rather than through the query text, so a
    band table that ever gained a non-numeric bound could not reach the SQL as
    syntax.
    """
    parts: list[sql.Composable] = []
    for index, band in enumerate(BANDS):
        clauses: list[sql.Composable] = []
        if band.lower is not None:
            clauses.append(sql.SQL("d.open >= {}").format(sql.Literal(band.lower)))
        if band.upper is not None:
            clauses.append(sql.SQL("d.open < {}").format(sql.Literal(band.upper)))
        predicate = sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("TRUE")
        parts.append(sql.SQL("count(*) FILTER (WHERE {}) AS {}").format(predicate, sql.Identifier(f"band_{index}")))
    return sql.SQL(", ").join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=settings.database_url)
    args = parser.parse_args()

    print("cost model bands (p75_spread_pct is a ROUND TRIP; half is one side)")
    for band in BANDS:
        print(
            f"  {band.label:>8}  round trip {band.p75_spread_pct}%  half {band.half_spread_pct}%  n={band.sample_size}"
        )
    print(
        f"  charged to every leg today: {UNKNOWN_NOMINAL_PRICE_BAND.label} "
        f"round trip {UNKNOWN_NOMINAL_PRICE_BAND.p75_spread_pct}% "
        f"half {UNKNOWN_NOMINAL_PRICE_BAND.half_spread_pct}%"
    )
    print("\ndeclared archives")
    for archive in RESEARCH_ARCHIVES:
        print(f"  {archive.vendor:<38} {archive.adjustment_basis:<15} capture {archive.quarantine_as_of}")

    with psycopg.connect(args.database_url) as conn:
        conn.execute("SET statement_timeout = '1800s'")
        validated = load_validated_universe(conn)
        for universe in UNIVERSES:
            selection = load_universe_selection(conn, universe=universe, validated_ids=frozenset(validated))
            ids = [series.series_id for series in selection.admitted]
            print(f"\n=== {universe} ===")
            print(f"  pinned vendor      {vendor_for(universe)}")
            print(f"  admitted series    {len(ids):,}")
            if not ids:
                print("  no admitted series")
                continue

            stored = conn.execute(_BASIS_SQL, {"ids": ids}).fetchall()
            print(f"  stored basis       {[(row[0], int(row[1])) for row in stored]}")
            bases = {row[0] for row in stored}
            bandable = bases == {"unadjusted"}
            print(f"  bandable           {bandable}  (a nominal band needs a single unadjusted basis)")

            paired = conn.execute(_PAIRED_SQL, {"ids": ids}).fetchone()
            assert paired is not None
            print(
                f"  cross-archive pair {int(paired[0]):,} of {len(ids):,} admitted series have a "
                f"same-instrument series under the other vendor"
            )

            row = conn.execute(
                sql.SQL(_BAND_COUNT_SQL).format(buckets=_bucket_sql()),
                {"ids": ids, "start": EVALUATION_WINDOW_START},
            ).fetchone()
            assert row is not None
            bars = int(row[0])
            print(f"  usable {FILL_COLUMN} bars from {EVALUATION_WINDOW_START}: {bars:,} ({row[1]} .. {row[2]})")
            if not bars:
                continue
            counts = [int(value) for value in row[3:]]
            if sum(counts) != bars:
                raise RuntimeError(f"band buckets sum to {sum(counts):,}, not {bars:,} — the edges are not total")
            weighted = Decimal(0)
            for band, count in zip(BANDS, counts, strict=True):
                print(f"    {band.label:>8}  {count:>12,}  {100 * count / bars:6.2f}%")
                weighted += band.p75_spread_pct * count
            weighted /= bars
            charged = UNKNOWN_NOMINAL_PRICE_BAND.p75_spread_pct
            print(f"    bar-weighted round trip if banded  {weighted:.4f}%")
            print(f"    charged round trip today           {charged}%")
            print(f"    ratio                              {charged / weighted:.2f}x")
            if not bandable:
                print(
                    "    ⚠ NOT A DEFECT FOR THIS UNIVERSE — these prices are split-adjusted, so the numeric\n"
                    "      thresholds above are not nominal bands and the ratio is not an overcharge."
                )


if __name__ == "__main__":
    main()
