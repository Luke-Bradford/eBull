"""#2282 stage 2b — download and load the HF daily-price archive into the research corpus.

Run from repo root:

    uv run python -m scripts.ingest_2282_research_archive --download
    uv run python -m scripts.ingest_2282_research_archive --load
    uv run python -m scripts.ingest_2282_research_archive --quarantine
    uv run python -m scripts.ingest_2282_research_archive --verify
    uv run python -m scripts.ingest_2282_research_archive --link-delistings

⚠ Long-running (~20-40 min end to end on 25.8M rows). Launch it with the
tool's own background mode — a ``nohup … &`` started inside an ordinary tool
call is killed when that call's process group is cleaned up, and a load that
dies part-way shows up only as a wrong bar count.

Idempotent at every stage: the series upsert is ON CONFLICT, the bar drain is
ON CONFLICT, and the quarantine replaces a series' verdicts wholesale.

``--verify`` is acceptance item 4 — the regression guard on the adjustment
basis. It compares this corpus against ``price_daily`` (eToro, split-adjusted)
over every overlapping instrument, not a hand-picked panel: if the archive's
OHLC were unadjusted, the two return series would diverge on every name that
split inside the overlap window.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import httpx
import psycopg

from app.config import settings
from app.services import research_corpus_ingest as ingest

logger = logging.getLogger("ingest_2282")

_HF_BASE = "https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price/resolve/main/data"
_SHARDS = [f"train-{i:05d}-of-00004.parquet" for i in range(4)]
_DEFAULT_CACHE = Path("var/research_corpus")


def download(cache: Path) -> list[Path]:
    """Fetch the four Parquet shards. ~525 MB total, ~30 s on a decent link.

    Plain HTTP against the public resolve URL rather than ``huggingface_hub``:
    the repo does not carry that dependency and a signed-URL redirect plus four
    GETs does not justify adding one.
    """
    cache.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for name in _SHARDS:
        target = cache / name
        paths.append(target)
        if target.exists() and target.stat().st_size > 0:
            logger.info("have %s (%.0f MB)", name, target.stat().st_size / 1e6)
            continue
        started = time.time()
        with httpx.stream("GET", f"{_HF_BASE}/{name}", follow_redirects=True, timeout=300.0) as response:
            response.raise_for_status()
            with target.open("wb") as handle:
                for chunk in response.iter_bytes(1 << 20):
                    handle.write(chunk)
        logger.info(
            "downloaded %s (%.0f MB in %.0fs)",
            name,
            target.stat().st_size / 1e6,
            time.time() - started,
        )
    return paths


def load(conn: psycopg.Connection[tuple], cache: Path) -> int:
    paths = [cache / name for name in _SHARDS]
    missing = [p.name for p in paths if not p.exists()]
    if missing:
        logger.error("missing shard(s): %s — run --download first", ", ".join(missing))
        return 1

    started = time.time()
    census = ingest.load_archive(conn, ingest.ParquetArchive(paths), provenance=ingest.HF_ARCHIVE)
    drift = ingest.census_drift(conn)

    print("\n=== stage 2b load census ===")
    print(f"  vendor                : {ingest.HF_ARCHIVE.vendor}")
    print(f"  upstream_source       : {ingest.HF_ARCHIVE.upstream_source}")
    print(f"  licence               : {ingest.HF_ARCHIVE.licence}")
    print(f"  adjustment_basis      : {ingest.HF_ARCHIVE.adjustment_basis}  (OHLC; adj_close is split+div)")
    print(f"  symbols seen          : {census.symbols_seen:,}")
    print(f"  series upserted       : {census.series_upserted:,}")
    print(f"    resolved            : {census.resolved_series:,}")
    print(f"    unresolved          : {census.unresolved_series:,}   <- eToro-listing-bias measure")
    print(f"    ambiguous symbols   : {len(census.ambiguous_symbols):,}")
    print(f"  bars loaded           : {census.bars_copied:,}")
    print(f"  rows without a close  : {census.rows_without_close:,}  (dropped, counted, no floor)")
    print(f"  duplicate vendor rows : {census.duplicate_bar_rows:,}")
    print(f"  {census.reuse_guard_note}")
    print(f"  census drift rows     : {drift}   <- MUST be 0")
    print(f"  elapsed               : {time.time() - started:.0f}s")

    if drift:
        logger.error("census drift is %d, expected 0 — the load is NOT done", drift)
        return 1
    return 0


def quarantine(conn: psycopg.Connection[tuple], as_of: date) -> int:
    started = time.time()
    census = ingest.run_quarantine(conn, as_of=as_of)
    print("\n=== stage 2b quarantine census ===")
    print(f"  series evaluated      : {census.series_evaluated:,}")
    print(f"  bars evaluated        : {census.bars_evaluated:,}")
    print(f"  transitions evaluated : {census.transitions_evaluated:,}")
    print(f"  bar verdicts stored   : {census.bar_verdicts_written:,}")
    print(f"  trans verdicts stored : {census.transition_verdicts_written:,}")
    print(f"  elapsed               : {time.time() - started:.0f}s")
    for row in conn.execute("SELECT * FROM research_quarantine_census").fetchall():
        print(f"  census view: {row}")
    return 0


def link_delistings(conn: psycopg.Connection[tuple]) -> int:
    """#2297 — wire the Form 25 register to the corpus. Writes dates, truncates nothing.

    Prints the NOT-covered side first, deliberately. A linkage that reports
    only what it matched reads as a completed guard; this one's headline
    finding is that the truncation set is empty by construction.
    """
    census = ingest.link_form25_delistings(conn)
    print("\n=== #2297 Form 25 delisting linkage ===")
    print(f"  overlapping series      : {census.overlap_series:,}")
    print(f"  suspension dates written: {census.suspension_dates_written:,}")
    print(f"  no suspension date (NULL, not back-filled): {census.no_suspension_date:,}")
    print(f"  conflicting symbols     : {census.conflicting or 'none'}")
    print("  overlap by rule provision:")
    for provision, n in sorted(census.by_provision.items()):
        print(f"    {provision:>14} : {n:,}")
    print(
        f"  identity unverified (series starts after a Form 25 on the same symbol): {len(census.identity_unverified):,}"
    )
    for symbol, filed, first_bar in census.identity_unverified:
        print(f"    {symbol:<8} filed {filed}  first bar {first_bar}")
    print(f"  terminating at/near the filing: {len(census.terminating):,}")
    for symbol, filed, last_bar in census.terminating:
        print(f"    {symbol:<8} filed {filed}  last bar {last_bar}")
    print(f"\n  coverage: {census.coverage_note}")
    return 0


_VERIFY_SQL = """
WITH overlap AS (
    SELECT s.series_id,
           s.instrument_id,
           r.bar_date,
           r.close                                            AS research_close,
           p.close                                            AS etoro_close,
           lag(r.close) OVER w                                AS research_prev,
           lag(p.close) OVER w                                AS etoro_prev
    FROM research_price_series s
    JOIN research_price_daily r ON r.series_id = s.series_id
    JOIN price_daily p
      ON p.instrument_id = s.instrument_id AND p.price_date = r.bar_date
    WHERE s.vendor = %(vendor)s
      AND s.instrument_id IS NOT NULL
    WINDOW w AS (PARTITION BY s.series_id ORDER BY r.bar_date)
),
rets AS (
    SELECT instrument_id,
           research_close / research_prev - 1 AS research_ret,
           etoro_close / etoro_prev - 1       AS etoro_ret,
           research_close / etoro_close       AS level_ratio
    FROM overlap
    WHERE research_prev > 0 AND etoro_prev > 0 AND etoro_close > 0
),
per_instrument AS (
    SELECT instrument_id,
           count(*)                                     AS n,
           corr(research_ret::float8, etoro_ret::float8) AS ret_corr,
           -- ⚠ MEDIAN, not mean. A level ratio is a ratio: one mis-levelled
           -- name at 30x drags a mean across 5,174 instruments into
           -- meaninglessness. The first draft of this query reported a mean
           -- level gap of +0.367 against an expected -0.0015 and looked like a
           -- failed adjustment-basis check; the median was +0.0020, which is
           -- the half-spread the reference predicts. The statistic was wrong,
           -- not the data.
           -- (No literal percent signs in this string: psycopg scans SQL
           -- comments for placeholders too, and a stray one raises
           -- "incomplete placeholder".)
           percentile_cont(0.5) WITHIN GROUP (ORDER BY level_ratio) AS med_level_ratio
    FROM rets
    GROUP BY instrument_id
    HAVING count(*) >= 60
),
scored AS (
    -- ⚠ ln() raises a domain error on zero and on negatives, and this corpus
    -- deliberately has no price floor: two loaded bars are already at
    -- close <= 0, and price_daily holds 154. They happen not to overlap TODAY,
    -- which is exactly what makes it a latent crash in the acceptance guard
    -- rather than a theoretical one.
    --
    -- Guarded here rather than by adding `research_close > 0` to the WHERE
    -- above, because that would be a narrowing gate on precisely the
    -- failed-company population the corpus exists to retain: FRCB's last bar
    -- is 0.0004 and a delisted name's final bars are the signal. NULL means
    -- "level not comparable for this instrument", and a NULL comparison
    -- excludes it from the FILTERs without removing any bar.
    SELECT n,
           ret_corr,
           med_level_ratio,
           ln(nullif(greatest(med_level_ratio, 0), 0)) AS log_level_ratio
    FROM per_instrument
)
SELECT count(*)                                        AS instruments,
       sum(n)                                          AS paired_bars,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ret_corr)::numeric, 4)
                                                       AS median_return_corr,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY med_level_ratio)::numeric, 6)
                                                       AS median_level_ratio,
       count(*) FILTER (WHERE abs(log_level_ratio) <= 0.01)
                                                       AS instruments_level_within_1pct,
       count(*) FILTER (WHERE ret_corr >= 0.90)        AS instruments_corr_at_least_0_90,
       count(*) FILTER (WHERE ret_corr >= 0.90 AND abs(log_level_ratio) > 0.10)
                                                       AS tail_agreeing_returns_offset_level,
       count(*) FILTER (WHERE ret_corr < 0.90)         AS tail_disagreeing_returns
FROM scored
"""


def verify(conn: psycopg.Connection[tuple]) -> int:
    """Acceptance item 4 — full-population regression guard on the adjustment basis.

    Every overlapping instrument, not a panel. An unadjusted research series
    against a split-adjusted ``price_daily`` would show up as a low return
    correlation on every name that split inside the overlap window; a
    dividend-adjusted close sitting in the ``close`` column would show up as a
    level ratio drifting away from 1 over time.

    The tail is REPORTED, not absorbed. Two signatures were characterised on
    the 2026-08-05 load and they are different problems:

    * **returns agree, level is offset by a constant factor** — the archive
      snapshot is frozen (last bar 2026-07-06) while ``price_daily`` keeps
      retro-adjusting, so any split after the snapshot rescales one side and
      not the other. ``FFAI`` is exactly 1/147.7 on all 162 overlapping bars
      including the last, at correlation 0.995.
    * **returns disagree** — the two sources are not describing the same
      series. Some of that is ticker reuse on the research side; some is on
      the eToro side (mean return sigma in this group is 37.8, which is not a
      plausible equity return series).

    Telling those apart per-instrument is a separate investigation into
    ``price_daily`` quality and is NOT this ticket.
    """
    row = conn.execute(_VERIFY_SQL, {"vendor": ingest.VENDOR}).fetchone()
    assert row is not None
    (
        instruments,
        paired,
        median_corr,
        median_level,
        within_1pct,
        corr_ok,
        tail_level,
        tail_returns,
    ) = row
    print("\n=== stage 2b verification vs price_daily (eToro) ===")
    print(f"  instruments compared          : {instruments:,}")
    print(f"  paired bars                   : {paired:,}")
    print(f"  median return correlation     : {median_corr}")
    print(f"  median level ratio (res/eToro): {median_level}")
    print(f"  level within +/-1%            : {within_1pct:,} ({100 * within_1pct / max(1, instruments):.1f}%)")
    print(f"  return corr >= 0.90           : {corr_ok:,} ({100 * corr_ok / max(1, instruments):.1f}%)")
    print(f"  tail, returns agree/level off : {tail_level:,}  (frozen-snapshot split epoch)")
    print(f"  tail, returns disagree        : {tail_returns:,}  (not the same series)")
    print(
        "\n  Reference (#2240 §0, TA spike): return correlation 0.963-0.996 and a\n"
        "  level gap of about -0.15% attributable to eToro's Bid half-spread.\n"
        "  A materially lower MEDIAN correlation would mean the archive's OHLC\n"
        "  are NOT split-adjusted as sql/251 claims."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--quarantine", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--link-delistings", action="store_true")
    parser.add_argument("--cache", type=Path, default=_DEFAULT_CACHE)
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.today(),
        help="Quarantine 'today' — sets which trailing bars count as provisional.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if not any((args.download, args.load, args.quarantine, args.verify, args.link_delistings)):
        parser.error("pick at least one of --download / --load / --quarantine / --verify / --link-delistings")

    if args.download:
        download(args.cache)

    if not any((args.load, args.quarantine, args.verify, args.link_delistings)):
        return 0

    with psycopg.connect(settings.database_url) as conn:
        if args.load and (rc := load(conn, args.cache)):
            return rc
        if args.quarantine and (rc := quarantine(conn, args.as_of)):
            return rc
        if args.verify and (rc := verify(conn)):
            return rc
        if args.link_delistings and (rc := link_delistings(conn)):
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
