"""#2282 stage 2b — download and load the HF daily-price archive into the research corpus.

Run from repo root:

    uv run python -m scripts.ingest_2282_research_archive --download
    uv run python -m scripts.ingest_2282_research_archive --load
    uv run python -m scripts.ingest_2282_research_archive --quarantine
    uv run python -m scripts.ingest_2282_research_archive --verify
    uv run python -m scripts.ingest_2282_research_archive --attribute
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

``--attribute`` explains ``--verify``'s tail (#2293). ⚠⚠ The low-correlation
tail is NOT a data-quality signal: it is a LIQUIDITY signal. A tape close
carries forward when a thin name does not trade, a broker quote moves every
day, and the gap between them is the same order as the daily return. It bands
the tail rate by archive dollar volume, crossed with overlap length, and
DERIVES its reading from that table — figures are printed, never written here,
and a corpus that produces the opposite gradient says so in the output.
"""

from __future__ import annotations

import argparse
import logging
import statistics
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
    # ⚠ Same explicit emptiness guard as the Intrader script. This archive
    # never measured its as_of, so it never had the implicit version either —
    # a --quarantine before --load printed a 0-series census, which is what a
    # healthy no-op prints. #3040.
    if ingest.loaded_series_count(conn, ingest.HF_ARCHIVE) == 0:
        # logger.error, matching this file's own `missing shard(s) … run
        # --download first` guard above — `print` here is for census OUTPUT, and
        # a refusal is not output. Same surface as the Intrader script's twin.
        logger.error("no bars loaded for %s — run --load first", ingest.HF_ARCHIVE.vendor)
        return 1

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
    print(f"  undated evidence written (date NULL, not back-filled): {census.undated_evidence_written:,}")
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


_ATTRIBUTE_SQL = """
WITH overlap AS (
    SELECT s.series_id,
           s.instrument_id,
           r.close  AS research_close,
           r.volume AS research_volume,
           p.close  AS etoro_close,
           lag(r.close) OVER w AS research_prev,
           lag(p.close) OVER w AS etoro_prev
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
           research_close,
           research_volume,
           (research_close / research_prev - 1)::float8 AS research_ret,
           (etoro_close / etoro_prev - 1)::float8       AS etoro_ret
    FROM overlap
    WHERE research_prev > 0 AND etoro_prev > 0 AND etoro_close > 0
)
SELECT instrument_id,
       count(*)                          AS n,
       corr(research_ret, etoro_ret)     AS ret_corr,
       -- The ARCHIVE's volume, deliberately. price_daily.volume is NULL for
       -- every bar of a large minority of instruments and for the median
       -- member of the tail itself, so banding on it would beg the question.
       percentile_cont(0.5) WITHIN GROUP (ORDER BY research_close * research_volume)
                                         AS med_dollar_volume
FROM rets
GROUP BY instrument_id
HAVING count(*) >= 60
"""

# Display bands for overlap length. These label the rows of a diagnostic table;
# no decision is taken on them, and nothing downstream reads them.
_OVERLAP_BANDS: tuple[tuple[str, int, int], ...] = (
    ("< 200", 0, 200),
    ("200-499", 200, 500),
    ("500-799", 500, 800),
    (">= 800", 800, 1 << 30),
)

# #2240 §0 / sql/251: the archive is split-adjusted, so a healthy pair correlates
# tightly. 0.90 is the cut --verify already reports its tail on; reused here so
# the two commands describe the same population rather than two similar ones.
_TAIL_CORR = 0.90


def attribute(conn: psycopg.Connection[tuple]) -> int:
    """#2293 — explain ``--verify``'s low-correlation tail.

    The ticket proposed three causes (frozen-snapshot split epoch, ticker reuse
    on the archive side, wrong bars in ``price_daily``). Measured on the full
    overlap, the tail is dominated by a FOURTH thing that is not a defect on
    either side: the two sources do not measure the same quantity on a name
    that barely trades. The archive close is a consolidated-tape last trade and
    carries forward through a no-trade session; the eToro close is a broker
    quote that moves daily. On a deep name the two coincide; on a thin one the
    difference is the same order as the daily return and the correlation
    collapses without either series being wrong.

    So this prints the tail rate against archive dollar volume, CROSSED with
    overlap length — because the two co-move, and reporting the liquidity
    gradient alone would not distinguish "thin names disagree" from "short
    series estimate a correlation badly". The gradient holds inside the
    longest-overlap band, which is what makes the liquidity reading the load-
    bearing one.

    ⚠ Band membership is not per-instrument attribution. A thin name can ALSO
    carry a genuine ``price_daily`` corporate-action break; the deep-quintile
    tail members are where such a break is separable from this artefact.
    """
    rows = conn.execute(_ATTRIBUTE_SQL, {"vendor": ingest.VENDOR}).fetchall()
    scored = [(int(r[0]), int(r[1]), float(r[2]), r[3]) for r in rows if r[2] is not None]
    banded = [(iid, n, c, float(dv)) for iid, n, c, dv in scored if dv is not None]
    banded.sort(key=lambda row: row[3])

    tail = [row for row in scored if row[2] < _TAIL_CORR]
    print("\n=== stage 2b tail attribution (#2293) ===")
    print(f"  instruments compared        : {len(scored):,}")
    print(f"  tail, return corr < {_TAIL_CORR}    : {len(tail):,}")
    print(f"  archive dollar volume usable: {len(banded):,}")
    if not banded:
        print("  nothing to band — no instrument has a usable archive volume")
        return 0

    size = len(banded) // 5
    quintile = {iid: min(4, i // size) if size else 0 for i, (iid, *_) in enumerate(banded)}
    by_id = {iid: (n, c, dv) for iid, n, c, dv in banded}

    def rate(members: list[int]) -> float | None:
        """Tail rate, or None when the cell is empty.

        A quintile CAN be empty — with fewer than five banded instruments
        ``size`` collapses and everything lands in Q1. That is a partially
        loaded corpus, not a bug, and it must print rather than raise.
        """
        if not members:
            return None
        return sum(1 for iid in members if by_id[iid][1] < _TAIL_CORR) / len(members)

    print(f"\n  {'quintile':<10}{'n':>8}{'corr<' + str(_TAIL_CORR):>10}{'rate':>8}{'median $vol/day':>20}")
    for q in range(5):
        members = [iid for iid, qq in quintile.items() if qq == q]
        if not members:
            print(f"  Q{q + 1:<9}{0:>8}{'-':>10}{'-':>8}{'-':>20}")
            continue
        bad = sum(1 for iid in members if by_id[iid][1] < _TAIL_CORR)
        med = statistics.median(by_id[iid][2] for iid in members)
        print(f"  Q{q + 1:<9}{len(members):>8}{bad:>10}{bad / len(members):>8.1%}{med:>20,.0f}")

    print("\n  tail rate by overlap length x archive-liquidity quintile")
    print(f"  {'overlap n':<12}" + "".join(f"{'Q' + str(q + 1):>14}" for q in range(5)))
    deepest_band = _OVERLAP_BANDS[-1]
    for label, lo, hi in _OVERLAP_BANDS:
        cells = []
        for q in range(5):
            members = [iid for iid, qq in quintile.items() if qq == q and lo <= by_id[iid][0] < hi]
            cell_rate = rate(members)
            if cell_rate is None:
                cells.append(f"{'-':>14}")
                continue
            bad = sum(1 for iid in members if by_id[iid][1] < _TAIL_CORR)
            cells.append(f"{f'{bad}/{len(members)} {cell_rate:.0%}':>14}")
        print(f"  {label:<12}" + "".join(cells))

    # ⚠ The reading is DERIVED, not printed as a constant. #2293's conclusion was
    # measured on one load; a later corpus can produce the opposite gradient, and a
    # fixed sentence would then explain a distribution it does not describe. This is
    # the "never hardcode a derived statistic" rule applied to the conclusion rather
    # than to a number.
    overall = [rate([iid for iid, qq in quintile.items() if qq == q]) for q in range(5)]
    lo_band, hi_band = overall[0], overall[4]
    deep = [
        rate([iid for iid, qq in quintile.items() if qq == q and deepest_band[1] <= by_id[iid][0] < deepest_band[2]])
        for q in (0, 4)
    ]
    print("\n  Reading, derived from the table above:")
    if lo_band is None or hi_band is None:
        print("    not enough banded instruments to compare the extreme quintiles.")
    elif lo_band > hi_band:
        print(f"    the tail concentrates in the THINNEST quintile ({lo_band:.1%} vs {hi_band:.1%} in the deepest).")
        if deep[0] is None or deep[1] is None:
            print(
                f"    ⚠ the '{deepest_band[0]}' overlap band cannot be compared, so "
                "a short-series\n    artefact is NOT excluded — treat the gradient as unconfirmed."
            )
        elif deep[0] > deep[1]:
            print(
                f"    it survives inside the '{deepest_band[0]}' overlap band "
                f"({deep[0]:.1%} vs {deep[1]:.1%}), so it is a\n    liquidity signal rather "
                "than a short-series estimation artefact. A low\n    correlation here is NOT "
                "by itself evidence of a defect on either side."
            )
        else:
            print(
                f"    ⚠ it does NOT survive inside the '{deepest_band[0]}' overlap band "
                f"({deep[0]:.1%} vs\n    {deep[1]:.1%}) — on this corpus the tail tracks overlap "
                "LENGTH, not liquidity.\n    #2293's reading does not apply; re-attribute before "
                "citing it."
            )
    else:
        print(
            f"    ⚠ the tail does NOT concentrate in the thinnest quintile ({lo_band:.1%} vs "
            f"{hi_band:.1%}\n    in the deepest). #2293's liquidity reading does not describe this "
            "corpus —\n    the tail needs re-attributing before any figure from it is cited."
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--quarantine", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--attribute", action="store_true")
    parser.add_argument("--link-delistings", action="store_true")
    parser.add_argument("--cache", type=Path, default=_DEFAULT_CACHE)
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=ingest.HF_ARCHIVE.quarantine_as_of,
        help=(
            "Quarantine 'today' — sets which trailing bars count as provisional. "
            "Defaults to this archive's declared quarantine_as_of so a manual run "
            "and the scheduled job cannot write different verdicts (#3040); pass a "
            "value only to investigate, and expect the next scheduled run to "
            "restore the declared policy."
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if not any((args.download, args.load, args.quarantine, args.verify, args.attribute, args.link_delistings)):
        parser.error(
            "pick at least one of --download / --load / --quarantine / --verify / --attribute / --link-delistings"
        )

    if args.download:
        download(args.cache)

    if not any((args.load, args.quarantine, args.verify, args.attribute, args.link_delistings)):
        return 0

    with psycopg.connect(settings.database_url) as conn:
        if args.load and (rc := load(conn, args.cache)):
            return rc
        if args.quarantine and (rc := quarantine(conn, args.as_of)):
            return rc
        if args.verify and (rc := verify(conn)):
            return rc
        if args.attribute and (rc := attribute(conn)):
            return rc
        if args.link_delistings and (rc := link_delistings(conn)):
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
