"""Phase 2 ticket 2c — acceptance harness for `indicator_series`.

Run from repo root:

    uv run python -m scripts.verify_2240_indicator_series --equivalence
    uv run python -m scripts.verify_2240_indicator_series --timing
    uv run python -m scripts.verify_2240_indicator_series --equivalence --timing

⚠ **Do NOT pipe this into `head`/`tail`.** A pipe buffers, so the flushed
progress lines go nowhere and the output file sits empty while the run is
perfectly healthy — that cost 7 minutes on 2026-08-05. Redirect to a file and
read the file. Same rule as `.claude/CLAUDE.md`'s "never pipe a gate command",
second symptom.

⚠ Every print here passes ``flush=True``, including the SUMMARY lines. The
first draft flushed only the progress counter, so a two-arm run
(``--equivalence --timing``) showed nothing at all between the arms — the
equivalence verdict sat in the buffer while the timing arm ran, which is the
same invisibility this docstring warns about, in the file that warns about it.

WHY THIS IS A COMMITTED SCRIPT AND NOT A NUMBER IN A PR
-------------------------------------------------------
Acceptance 2 and 5 of the spec are full-corpus figures. A figure written by
hand into prose goes stale silently the moment the derivation changes, and it
goes stale in the place a reader trusts most — so the repo's rule is to compute
it or omit it. This computes it.

Sister to `scripts/verify_2279_price_structure.py`.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from decimal import Decimal

import psycopg

from app.config import settings
from app.services import technical_analysis as ta
from app.services.indicator_series import (
    RULE_SET_VERSION,
    BarSeries,
    atr_series,
    bollinger_series,
    ema_series,
    macd_series,
    rsi_series,
    sma_series,
    stochastic_series,
)

logger = logging.getLogger("verify_2240")

# ⚠ RELATIVE, not absolute — and this was a real defect in the first draft.
#
# An absolute 1e-9 is magnitude-blind, and this corpus is not: `BINI` carries a
# close of 3.0e17 and `TOPS` 1.0e15. One ULP of float64 at 1e9 is already
# 1.2e-07, so at those magnitudes NO implementation can satisfy an absolute
# 1e-9 — including `technical_analysis` itself. A tolerance that the reference
# cannot meet either tests nothing or fails everything.
#
# Measured against an exact `math.fsum` reference, the streaming forms agree to
# ~1-2 ULP (relative ~2e-16) at every magnitude tried. `1e-12` relative is
# therefore four orders of magnitude of headroom over the observed error while
# still being far tighter than any real defect would produce — the reverted
# one-pass Bollinger variance sat at ~3e-08 relative.
REL_TOL = 1e-12
#: Absolute floor, so near-zero values (penny stocks, and the corpus has
#: closes at 1e-4) do not fail on a relative comparison against ~0.
ABS_FLOOR = 1e-12
#: The corpus is survivor-only — #2284 measured 0 of 259 known delisted names
#: served. Hard-coded here because this harness only ever reads that corpus;
#: a caller with a different one must state its own.
UNIVERSE = "survivor_only"
_MIN_BARS = 40


def _load(conn: psycopg.Connection[tuple], series_id: int) -> tuple[BarSeries, list[ta.OHLCVRow], list[Decimal]]:
    raw = conn.execute(
        "SELECT bar_date, open, high, low, close, volume FROM research_price_daily "
        "WHERE series_id = %s ORDER BY bar_date",
        (series_id,),
    ).fetchall()
    rows: list[ta.OHLCVRow] = [{"open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} for r in raw]
    series = BarSeries(dates=tuple(r[0] for r in raw), rows=tuple(rows))
    return series, rows, [r[4] for r in raw]


def _compare(series: BarSeries, rows: list[ta.OHLCVRow], closes: list[Decimal]) -> dict[str, int]:
    """Streamed last value vs the shipped batch function, per component."""
    out: dict[str, int] = {}

    def chk(name: str, streamed: float | None, batch: float | None) -> None:
        out.setdefault(name, 0)
        if batch is None and streamed is None:
            return
        if batch is None or streamed is None:
            out[name] += 1
            return
        if abs(streamed - batch) > max(ABS_FLOOR, REL_TOL * abs(batch)):
            out[name] += 1

    chk("sma_20", sma_series(series, universe=UNIVERSE, period=20).values[-1], ta.sma(closes, 20))
    chk("ema_12", ema_series(series, universe=UNIVERSE, period=12).values[-1], ta.ema(closes, 12))
    chk("rsi_14", rsi_series(series, universe=UNIVERSE).values[-1], ta.rsi(closes, 14))
    chk("atr_14", atr_series(series, universe=UNIVERSE).values[-1], ta.atr(rows, 14))

    macd = macd_series(series, universe=UNIVERSE)
    batch_macd = ta.macd(closes)
    for i, comp in enumerate(("line", "signal", "histogram")):
        chk(f"macd_{comp}", macd.components[comp][-1], None if batch_macd is None else batch_macd[i])

    bb = bollinger_series(series, universe=UNIVERSE)
    batch_bb = ta.bollinger_bands(closes)
    chk("bb_upper", bb.components["upper"][-1], None if batch_bb is None else batch_bb[0])
    chk("bb_lower", bb.components["lower"][-1], None if batch_bb is None else batch_bb[1])

    st = stochastic_series(series, universe=UNIVERSE)
    batch_st = ta.stochastic(rows)
    chk("stoch_k", st.components["k"][-1], None if batch_st is None else batch_st[0])
    chk("stoch_d", st.components["d"][-1], None if batch_st is None else batch_st[1])
    return out


def equivalence(conn: psycopg.Connection[tuple]) -> int:
    """Acceptance 2 — FULL corpus, every series, all seven indicators.

    ⚠ Not a sample, and the harness offers no sampling flag on purpose. The
    spec's own §3 benchmark was five series, and this phase exists because a
    sampled measurement produced a 27-point phantom edge (#2260).
    """
    ids = conn.execute(
        "SELECT series_id FROM research_price_series WHERE bar_count IS NOT NULL ORDER BY series_id"
    ).fetchall()
    totals: dict[str, int] = {}
    checked = skipped = 0
    started = time.perf_counter()

    for k, (series_id,) in enumerate(ids):
        series, rows, closes = _load(conn, series_id)
        if len(series) < _MIN_BARS:
            skipped += 1
            continue
        for name, bad in _compare(series, rows, closes).items():
            totals[name] = totals.get(name, 0) + bad
        checked += 1
        if (k + 1) % 1000 == 0:
            print(f"  {k + 1:,}/{len(ids):,} | checked={checked:,} mismatches={sum(totals.values())}", flush=True)

    print(f"\n=== acceptance 2 — equivalence ({RULE_SET_VERSION}) ===", flush=True)
    print(f"  series checked : {checked:,}   (skipped {skipped:,} with < {_MIN_BARS} bars)", flush=True)
    for name in sorted(totals):
        print(f"  {name:<16} {'OK' if totals[name] == 0 else f'*** {totals[name]} MISMATCH ***'}", flush=True)
    print(f"  total mismatches: {sum(totals.values())}", flush=True)
    print(f"  elapsed: {time.perf_counter() - started:.1f}s (includes reading the whole corpus)")

    # ⚠ The corpus carries no NULL OHLC, so the not_evaluable paths are NOT
    # exercised here at all. Say so rather than letting a clean sweep imply
    # coverage it does not have.
    nulls = conn.execute(
        "SELECT count(*) FROM research_price_daily WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL"
    ).fetchone()
    assert nulls is not None
    print(f"  ⚠ bars with a NULL OHLC field: {nulls[0]:,} — the not_evaluable paths are", flush=True)
    print("    unreachable on this corpus and are covered ONLY by tests/test_indicator_series.py", flush=True)
    return 0 if sum(totals.values()) == 0 else 1


def timing(conn: psycopg.Connection[tuple]) -> int:
    """Acceptance 5 — all seven indicators over the full corpus, together.

    ⚠ Together, not per-indicator. Per-indicator timing was the spec's first
    draft and does not answer phase 5's question, which is what a
    multi-indicator strategy pass costs.
    """
    ids = conn.execute(
        "SELECT series_id FROM research_price_series WHERE bar_count IS NOT NULL ORDER BY series_id"
    ).fetchall()
    bars_seen = 0
    compute = 0.0
    started = time.perf_counter()

    for (series_id,) in ids:
        series, rows, closes = _load(conn, series_id)
        if len(series) < _MIN_BARS:
            continue
        t0 = time.perf_counter()
        sma_series(series, universe=UNIVERSE, period=20)
        ema_series(series, universe=UNIVERSE, period=12)
        rsi_series(series, universe=UNIVERSE)
        atr_series(series, universe=UNIVERSE)
        macd_series(series, universe=UNIVERSE)
        bollinger_series(series, universe=UNIVERSE)
        stochastic_series(series, universe=UNIVERSE)
        compute += time.perf_counter() - t0
        bars_seen += len(series)

    wall = time.perf_counter() - started
    print(f"\n=== acceptance 5 — timing, all seven together ({RULE_SET_VERSION}) ===", flush=True)
    print(f"  bars           : {bars_seen:,}", flush=True)
    print(f"  COMPUTE only   : {compute:.1f}s      <- the acceptance figure (< 60s)", flush=True)
    print(f"  wall incl. I/O : {wall:.1f}s", flush=True)
    print(f"  verdict        : {'PASS' if compute < 60 else '*** FAIL ***'}", flush=True)
    return 0 if compute < 60 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equivalence", action="store_true", help="acceptance 2")
    parser.add_argument("--timing", action="store_true", help="acceptance 5")
    args = parser.parse_args(argv)
    if not (args.equivalence or args.timing):
        parser.error("pick at least one of --equivalence / --timing")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        if args.equivalence and (rc := equivalence(conn)):
            return rc
        if args.timing and (rc := timing(conn)):
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
