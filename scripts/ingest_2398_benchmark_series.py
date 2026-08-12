"""#2398 — load benchmark / sector series into the research corpus.

Run from repo root::

    uv run python -m scripts.ingest_2398_benchmark_series --load
    uv run python -m scripts.ingest_2398_benchmark_series --verify

Benchmarks are **comparators, not tradable candidates**. They are stored with
``instrument_id IS NULL``, which the ``research_price_series_resolution_evidenced``
CHECK ties to ``resolution_method IS NULL`` — so they cannot acquire an
instrument by accident, and ``load_validated_universe`` (which joins
``instruments``) cannot see them. That is the invariant this file exists to
protect; do not "helpfully" resolve them later.

Source: the icyDenev/Intrader mirror already on disk under
``var/research_corpus/mirrors``. Headerless CSV, column order verified against
the Deamoner mirror on a shared bar (SPY 1993-01-29) and against the last bar
(where ``adjclose == close``, i.e. the back-adjustment factor is 1.0)::

    date, open, high, low, close, volume, split_factor, dividend, adjclose

``close`` is the vendor's OHLC close; ``adjclose`` carries dividends — verified
on SPY: the factor runs 0.610791 -> 1.0 over 115 steps on a quarterly cadence.
**Use ``adj_close`` for returns and ``close`` for price levels** (#2400: a
back-adjusted level is meaningless, and a benchmark is no exception).
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.research_corpus_ingest import INTRADER_ARCHIVE

logger = logging.getLogger("ingest_2398")

#: ⚠ Corrected in #2597: this vendor's OHLC are **unadjusted**, and the stamp
#: here read ``split_adjusted`` from the day it shipped. Measured on the same
#: mirror this script reads — AAPL 2020-08-27 close is 500.04 (the traded level
#: four days before a 4:1 split) and its 1980-12-12 IPO bar is 28.75, against
#: Yahoo's split-adjusted 0.1283. The dividend AND split adjustment live in the
#: ninth column, stored as ``adj_close``, which is what the docstring above
#: already said and what #2400 tells consumers to read for returns. The
#: provenance now lives in one place, ``research_corpus_ingest.INTRADER_ARCHIVE``,
#: so a bulk load of this vendor and this script cannot disagree about it.
VENDOR = INTRADER_ARCHIVE.vendor
UPSTREAM_SOURCE = INTRADER_ARCHIVE.upstream_source
LICENCE = INTRADER_ARCHIVE.licence
ADJUSTMENT_BASIS = INTRADER_ARCHIVE.adjustment_basis

_MIRROR = Path("var/research_corpus/mirrors/icyDenev_Intrader/Data/Day")

#: The comparator set. Market, size/style, then the nine sector SPDRs, then
#: cross-asset. Ordered by what each unblocks — SPY alone gives beta and a
#: regime filter (#2398).
BENCHMARKS: tuple[str, ...] = (
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "VTI",
    "XLK",
    "XLF",
    "XLE",
    "XLV",
    "XLI",
    "XLY",
    "XLP",
    "XLU",
    "XLB",
    "TLT",
    "GLD",
)


def _dec(value: str) -> Decimal | None:
    try:
        d = Decimal(value)
    except InvalidOperation, ValueError:
        return None
    return d if d.is_finite() else None


def _read(symbol: str) -> list[tuple[date, Decimal, Decimal, Decimal, Decimal, int, Decimal]]:
    """Parse one mirror CSV. Rows failing any field are dropped, counted by the caller."""
    path = _MIRROR / f"{symbol}.csv"
    out: list[tuple[date, Decimal, Decimal, Decimal, Decimal, int, Decimal]] = []
    with path.open(newline="") as fh:
        for row in csv.reader(fh):
            if len(row) < 9:
                continue
            try:
                bar_date = datetime.strptime(row[0], "%Y-%m-%d").date()
            except ValueError:
                continue
            o, h, low, c, adj = (_dec(row[1]), _dec(row[2]), _dec(row[3]), _dec(row[4]), _dec(row[8]))
            if None in (o, h, low, c, adj):
                continue
            assert o is not None and h is not None and low is not None and c is not None and adj is not None
            if c <= 0 or adj <= 0:
                continue
            try:
                volume = int(row[5])
            except ValueError:
                continue
            out.append((bar_date, o, h, low, c, volume, adj))
    out.sort(key=lambda r: r[0])
    return out


def load(conn: psycopg.Connection[Any]) -> int:
    total = 0
    for symbol in BENCHMARKS:
        rows = _read(symbol)
        if not rows:
            logger.warning("%s: no usable rows — skipped", symbol)
            continue
        series_id = conn.execute(
            """
            INSERT INTO research_price_series
                (vendor, vendor_symbol, upstream_source, licence, adjustment_basis,
                 first_bar, last_bar, bar_count, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW(), NOW())
            ON CONFLICT (vendor, vendor_symbol) DO UPDATE
               SET first_bar=EXCLUDED.first_bar, last_bar=EXCLUDED.last_bar,
                   bar_count=EXCLUDED.bar_count, updated_at=NOW()
            RETURNING series_id
            """,
            (VENDOR, symbol, UPSTREAM_SOURCE, LICENCE, ADJUSTMENT_BASIS, rows[0][0], rows[-1][0], len(rows)),
        ).fetchone()
        if series_id is None:
            raise RuntimeError(f"{symbol}: series upsert returned no series_id")
        sid = int(series_id[0])
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO research_price_daily
                    (series_id, bar_date, open, high, low, close, volume, adj_close)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (series_id, bar_date) DO UPDATE
                   SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                       close=EXCLUDED.close, volume=EXCLUDED.volume, adj_close=EXCLUDED.adj_close
                """,
                [(sid, d, o, h, low, c, v, adj) for (d, o, h, low, c, v, adj) in rows],
            )
        conn.commit()
        total += len(rows)
        logger.info("%s: %d bars %s..%s (series_id=%d)", symbol, len(rows), rows[0][0], rows[-1][0], sid)
    return total


def verify(conn: psycopg.Connection[Any]) -> int:
    """Acceptance: loaded, dividend-adjusted, and NOT tradable.

    ⚠ Scoped to ``BENCHMARKS``, not to the whole vendor. #2597 bulk-loads the
    same mirror under the same vendor, so a vendor-wide check would (a) count
    22,879 series against 16 and (b) fail the ``factor <= 1`` assert on every
    reverse-split name, where Yahoo's back-adjusted close is legitimately ABOVE
    the raw close. Neither says anything about the comparators this verifies.
    """
    failures = 0
    rows = conn.execute(
        """
        SELECT s.vendor_symbol, s.bar_count, s.first_bar, s.last_bar, s.instrument_id, s.resolution_method
        FROM research_price_series s
        WHERE s.vendor = %s AND s.vendor_symbol = ANY(%s)
        ORDER BY s.vendor_symbol
        """,
        (VENDOR, list(BENCHMARKS)),
    ).fetchall()
    print(f"{'symbol':<8}{'bars':>8}  {'first':<12}{'last':<12}{'tradable?':>10}")
    for sym, bars, first, last, iid, rm in rows:
        leak = "LEAK" if iid is not None or rm is not None else "no"
        if leak == "LEAK":
            failures += 1
        print(f"{sym:<8}{bars:>8}  {str(first):<12}{str(last):<12}{leak:>10}")
    if len(rows) != len(BENCHMARKS):
        print(f"FAIL: expected {len(BENCHMARKS)} series, found {len(rows)}")
        failures += 1

    # dividend adjustment: factor must reach 1.0 at the last bar and never exceed it
    bad = conn.execute(
        """
        WITH f AS (
          SELECT s.vendor_symbol, d.adj_close/NULLIF(d.close,0) AS factor,
                 row_number() OVER (PARTITION BY d.series_id ORDER BY d.bar_date DESC) AS rn
          FROM research_price_daily d JOIN research_price_series s USING (series_id)
          WHERE s.vendor = %s AND s.vendor_symbol = ANY(%s) AND d.close > 0
        )
        SELECT count(*) FILTER (WHERE rn = 1 AND abs(factor - 1) > 1e-6),
               count(*) FILTER (WHERE factor > 1.000001)
        FROM f
        """,
        (VENDOR, list(BENCHMARKS)),
    ).fetchone()
    if bad is None:
        raise RuntimeError("adjustment check returned no row")
    if bad[0] or bad[1]:
        print(f"FAIL: latest_factor!=1 on {bad[0]} series; factor>1 on {bad[1]} bars")
        failures += 1
    else:
        print("\nadjustment: latest factor == 1.0 on every series, none > 1  OK")

    if failures:
        print(f"\n{failures} FAILURE(S)")
    else:
        print("\nall acceptance checks passed")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if not (args.load or args.verify):
        parser.error("pass --load and/or --verify")
    with psycopg.connect(settings.database_url) as conn:
        if args.load:
            n = load(conn)
            logger.info("loaded %d bars across %d symbols", n, len(BENCHMARKS))
        if args.verify:
            return verify(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
