"""Full-population verification of S-4 volatility compression breakout (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_s4_volatility_breakout.py --all

⚠ NOTHING IS WRITTEN. Both arms read and print. Gate on the EXIT CODE — 0 means
every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

TWO ARMS, MEASURING DIFFERENT THINGS
------------------------------------
``--equivalence`` — every bar of BOTH corpora, ``research_price_daily`` and
``price_daily``, on RAW bars, against a re-derivation of the same rule.

⚠⚠ WHAT IS INDEPENDENTLY DERIVED HERE, AND WHAT IS NOT. Stating this precisely
matters more on S-4 than on S-1, because S-4's inputs are not all the same shape
and a blanket "compared against SQL" would be false:

- **The true range is SQL's**, from ``lag(close)`` and the bar's own high/low.
  This is where an OHLC alignment bug would live — a TR that reaches the wrong
  previous close, or reads high/low off the fill bar — and the SQL side has no
  index and no state, so a shared off-by-one would have to occur in both.
- **The prior-20 high is SQL's**, from ``max(close) OVER (ROWS BETWEEN 20
  PRECEDING AND 1 PRECEDING)``. §4's *"excluding t itself"* is a frame boundary
  on that side and a slice bound on ours, which is the cleanest possible
  independent statement of the same rule.
- **The compression rank is re-derived by a DIFFERENT ALGORITHM** —
  ``sorted(window).index(value)``, the position of the first occurrence in a
  sorted window, against the module's count-of-comparisons. Not SQL's: counting
  window values below a per-row value has no window-function form, and the
  array-unnest form is 100 comparisons on each of 32.5M bars.
- ⚠ **The Wilder recursion is RE-IMPLEMENTED in this script, not SQL-derived**,
  and that is the weakest link — a recursive CTE over 32.5M rows needs indexed
  temp tables per corpus. What it still catches is transcription and HORIZON
  errors (where the seed lands, where the tail refusal starts), because the
  re-implementation is fed SQL's true ranges. What it cannot catch is an error
  in the Wilder arithmetic itself, which is pinned instead by
  ``tests/test_indicator_series.py`` and by phase 2a's
  ``verify_2240_indicator_series.py``. ⚠ Phase 2a checks ATR at the LAST BAR of
  each series only; this arm checks EVERY bar, which is the gap it closes.

⚠ BOTH SIDES COMPUTE IN float64 IN THE SAME ORDER, so the ATR is expected to
agree BIT-FOR-BIT and the run asserts that on the full population rather than
assuming it. SQL casts to ``double precision`` before any arithmetic for exactly
this reason: left in ``numeric`` it would divide exactly where we divide
approximately, and every near-tie would need a tolerance. Any ATR disagreement is
therefore a logic difference, not a rounding one, and is counted as a hard
mismatch with no tie allowance. The only comparison left with two arithmetics is
``close > prior_high``, both ``numeric`` on the SQL side and ``float`` on ours —
those get the tie treatment S-1 established.

``--census`` — the §4.0 validated universe (US stocks ex-ETF) on MASKED bars, via
the fail-closed loader. Reports the verdict distribution and the refusal
breakdown by reason (criterion 9's "measure what you reject"), plus TWO
S-4-specific counts:

- the bars the 113-bar warm-up narrows away from a breakout leg that was already
  computable at bar 20; and
- ⚠ **the bars refused because a hole EARLIER in the series killed the ATR
  recursion**. This is S-3's blast radius in a second guise: Wilder smoothing
  carries state, so ``atr_series`` refuses every bar after a masked one rather
  than recovering, and S-4 inherits it. Counted, not asserted away.

⚠ The two arms are NOT comparable and the numbers must not be pooled: different
populations, different bars (raw vs masked), different purpose.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date
from decimal import Decimal

import psycopg
from psycopg import sql as pgsql

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries, atr_series
from app.services.research_price_structure_store import load_masked_series
from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_PERIOD,
    BREAKOUT_LOOKBACK,
    COMPRESSION_QUANTILE,
    COMPRESSION_WINDOW,
    S4_STRATEGY_ID,
    WARMUP_BARS,
    _source_hash,
    compression_rank_series,
    prior_high_close_series,
    s4_identity,
    s4_signals,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.technical_analysis import OHLCVRow

#: The research corpus is survivor-only (#2284: no free source serves the
#: delisted cohort), and ``price_daily`` is eToro's listing as it stands today.
#: Both are ``survivor_only`` and every figure below inherits that label (#2288).
UNIVERSE = "survivor_only"

#: ⚠ IMPORTED, never restated. Stage 5b froze the model, so the identity hash
#: records a real cost basis; a local literal here would be a second source of
#: truth for a value that is hashed into every stored strategy version.

#: Relative margin below which a disagreement is arithmetic, not logic.
TIE_TOLERANCE = 1e-9

#: ⚠ Read at IMPORT, microseconds after the strategy module is loaded, so it is
#: the hash of the code this process actually runs. See ``_stamped_version``.
_SOURCE_AT_IMPORT = _source_hash()


#: ⚠ Every arithmetic operand is cast to ``double precision`` BEFORE the
#: operation, not after — ``(high - low)::float8`` would subtract in ``numeric``
#: and round once, which is a different number from subtracting two float64s.
#: The whole bit-for-bit claim above rests on this line being written this way.
#:
#: ⚠ ``count(*)`` and ``count(close)`` are BOTH required on the 20-bar frame. The
#: first rejects a frame that has not filled yet, the second one holding a NULL
#: close. Either alone admits the other case, and they are different verdicts —
#: a short frame is warm-up, a holed one is a data gap (criterion 8).
_BAR_TEMPLATE = """
WITH b AS (
  SELECT {key} AS k,
         {date_column} AS d,
         open, high, low, close, volume,
         lag(close) OVER w AS prev_close,
         CASE WHEN count(*) OVER w20 = {lookback} AND count(close) OVER w20 = {lookback}
              THEN max(close) OVER w20 END AS prior_high
  FROM {table}
  WINDOW w AS (PARTITION BY {key} ORDER BY {date_column}),
         w20 AS (PARTITION BY {key} ORDER BY {date_column}
                 ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING)
)
SELECT k, d, open, high, low, close, volume, prior_high,
       CASE WHEN high IS NULL OR low IS NULL OR close IS NULL OR prev_close IS NULL THEN NULL
            ELSE greatest(high::float8 - low::float8,
                          abs(high::float8 - prev_close::float8),
                          abs(low::float8 - prev_close::float8)) END AS tr
FROM b
ORDER BY k, d
"""


def _bar_sql(table: str, key: str, date_column: str) -> pgsql.Composed:
    """Per-bar OHLCV plus Postgres' own true range and prior-20 high.

    Composed through ``psycopg.sql`` rather than an f-string: the table and key
    vary per corpus, and a frame offset cannot be a query parameter, so the two
    kinds of interpolation are separated — ``Identifier`` for names, ``Literal``
    for the lookback — instead of pasted into one string.
    """
    return pgsql.SQL(_BAR_TEMPLATE).format(
        table=pgsql.Identifier(table),
        key=pgsql.Identifier(key),
        date_column=pgsql.Identifier(date_column),
        lookback=pgsql.Literal(BREAKOUT_LOOKBACK),
    )


CORPORA = (
    ("research_price_daily", _bar_sql("research_price_daily", "series_id", "bar_date")),
    ("price_daily", _bar_sql("price_daily", "instrument_id", "price_date")),
)


def _wilder_from_true_ranges(trs: list[float | None]) -> tuple[list[float | None], int]:
    """Wilder ATR from SQL's true ranges, plus the index the tail refusal starts.

    ⚠ Re-implemented rather than SQL-derived — see the module docstring for what
    that does and does not buy. The HORIZON logic is the part worth having twice:
    the first NULL true range at or after index 1 ends the series for ATR
    purposes, because Wilder smoothing carries state and has no window for a hole
    to clear. Written here from §4's data requirement rather than copied.
    """
    n = len(trs)
    values: list[float | None] = [None] * n
    first_null = next((i for i in range(1, n) if trs[i] is None), None)
    horizon = n if first_null is None else first_null
    if horizon <= ATR_PERIOD:
        return values, horizon
    window = [t for t in trs[1 : ATR_PERIOD + 1] if t is not None]
    current = sum(window) / ATR_PERIOD
    values[ATR_PERIOD] = current
    for i in range(ATR_PERIOD + 1, horizon):
        tr = trs[i]
        assert tr is not None
        current = (current * (ATR_PERIOD - 1) + tr) / ATR_PERIOD
        values[i] = current
    return values, horizon


def _reference_verdicts(
    closes: list[Decimal | None],
    atr: list[float | None],
    horizon: int,
    prior_high: list[Decimal | None],
) -> list[str]:
    """The verdict per bar, from the re-derived inputs only.

    Mirrors 3a's runner precedence without sharing a line of its code: the last
    bar has no ``t+1``; then a data refusal; then warm-up; then the two strict
    comparisons.
    """
    n = len(closes)
    out: list[str] = []
    for i in range(n):
        if i == n - 1:
            out.append("not_evaluable")
            continue
        window_start = i - COMPRESSION_WINDOW + 1
        # A data refusal: the close itself, the ATR tail after a hole, or a
        # 20-bar frame that Postgres refused because it held a NULL close.
        holed = closes[i] is None or i >= horizon or (i >= BREAKOUT_LOOKBACK and prior_high[i] is None)
        if holed:
            out.append("not_evaluable")
            continue
        if window_start < 0 or atr[window_start] is None or atr[i] is None or i < BREAKOUT_LOOKBACK:
            out.append("not_evaluable")
            continue
        window = [v for v in atr[window_start : i + 1] if v is not None]
        current = atr[i]
        assert current is not None
        # ⚠ A DIFFERENT DERIVATION: in a sorted window the index of the first
        # occurrence of `current` IS the count of values strictly below it.
        rank = sorted(window).index(current) / len(window)
        close, high = closes[i], prior_high[i]
        assert close is not None and high is not None
        out.append("fired" if (rank < COMPRESSION_QUANTILE and close > high) else "not_fired")
    return out


def _relative(a: Decimal, b: Decimal) -> float:
    scale = max(abs(float(b)), 1e-12)
    return abs(float(a) - float(b)) / scale


class _Tally:
    """⚠ A COUNTER PLUS A BOUNDED SAMPLE, never a list padded to keep a count.

    The idiom this replaces appended an empty string past the printed cap purely
    so ``len()`` stayed right, which grows an unbounded list of nothing. Both
    kinds route through ``record`` so the count and the sample cannot drift apart.
    """

    SAMPLE_CAP = 20

    def __init__(self) -> None:
        self.series = 0
        self.bars = 0
        self.atr_mismatches = 0
        self.atr_sample: list[str] = []
        self.verdict_mismatches = 0
        self.verdict_sample: list[str] = []
        self.ties = 0
        self.max_tie_margin = 0.0
        self.min_real_margin = float("inf")

    def record_atr(self, problem: str) -> None:
        self.atr_mismatches += 1
        if len(self.atr_sample) < self.SAMPLE_CAP:
            self.atr_sample.append(problem)

    def record_verdict(self, problem: str) -> None:
        self.verdict_mismatches += 1
        if len(self.verdict_sample) < self.SAMPLE_CAP:
            self.verdict_sample.append(problem)


def _compare(
    key: int,
    dates: list[date],
    rows: list[OHLCVRow],
    trs: list[float | None],
    prior_high_sql: list[Decimal | None],
    tally: _Tally,
) -> None:
    """One series: strategy verdicts against the re-derivation, bar by bar."""
    lengths = {
        "dates": len(dates),
        "rows": len(rows),
        "trs": len(trs),
        "prior_high_sql": len(prior_high_sql),
    }
    # ⚠ ENFORCED, NOT ASSUMED. The comparison loop indexes all four by the same
    # `i`, and the alignment they rest on is a promise each producer keeps rather
    # than something any of them validates. A trimmed list would surface as an
    # IndexError naming the wrong thing instead of as the alignment defect this
    # arm exists to catch (#2240 round 3 on S-3).
    if len(set(lengths.values())) != 1:
        raise RuntimeError(f"series {key} is misaligned across the two derivations: {lengths}")

    series = BarSeries(dates=tuple(dates), rows=tuple(rows))
    signals = s4_signals(series, universe=UNIVERSE, masked_reason="quarantined_bar")
    closes = series.closes

    atr_ref, horizon = _wilder_from_true_ranges(trs)
    atr_python = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD).values
    prior_high_python = prior_high_close_series(series, universe=UNIVERSE).values

    tally.series += 1
    tally.bars += len(series)

    # ⚠ The ATR is compared BIT-FOR-BIT, values not verdicts. Both sides are
    # float64 in the same order, so any difference is logic, never rounding.
    for i in range(len(series)):
        if atr_ref[i] != atr_python[i] and tally.atr_mismatches < 10_000:
            tally.record_atr(f"{key} {dates[i]}: python={atr_python[i]!r} sql-derived={atr_ref[i]!r}")

    # The prior-20 high is Postgres' frame against our slice — exact on both.
    for i in range(len(series)):
        high_sql = prior_high_sql[i]
        want = None if high_sql is None else float(high_sql)
        if want != prior_high_python[i] and tally.atr_mismatches < 10_000:
            tally.record_atr(f"{key} {dates[i]} prior_high: python={prior_high_python[i]!r} sql={want!r}")

    expected = _reference_verdicts(closes, atr_ref, horizon, prior_high_sql)
    for i, want in enumerate(expected):
        got = signals[i].verdict
        if got == want:
            continue
        close, high = closes[i], prior_high_sql[i]
        margin = float("inf") if close is None or high is None else _relative(close, high)
        if margin < TIE_TOLERANCE:
            tally.ties += 1
            tally.max_tie_margin = max(tally.max_tie_margin, margin)
            continue
        tally.min_real_margin = min(tally.min_real_margin, margin)
        tally.record_verdict(f"{key} {dates[i]}: python={got} reference={want} margin={margin:.3e}")


def equivalence() -> int:
    """Every bar of both corpora: strategy verdict vs the re-derivation."""
    failures = 0
    for table, sql in CORPORA:
        started = time.monotonic()
        tally = _Tally()
        print(f"\n[{table}] streaming…", flush=True)
        with psycopg.connect(settings.database_url) as conn, conn.cursor(name=f"s4_{table}") as cur:
            cur.itersize = 50_000
            cur.execute(sql)
            current: int | None = None
            dates: list[date] = []
            rows: list[OHLCVRow] = []
            trs: list[float | None] = []
            prior_high_sql: list[Decimal | None] = []
            for row in cur:
                key = row[0]
                if key != current:
                    if current is not None:
                        _compare(current, dates, rows, trs, prior_high_sql, tally)
                        if tally.series % 500 == 0:
                            print(
                                f"  {tally.series} series, {tally.bars} bars, "
                                f"{tally.atr_mismatches} value / {tally.verdict_mismatches} verdict mismatches, "
                                f"{tally.ties} ties ({time.monotonic() - started:.0f}s)",
                                flush=True,
                            )
                    current, dates, rows, trs, prior_high_sql = key, [], [], [], []
                dates.append(row[1])
                rows.append({"open": row[2], "high": row[3], "low": row[4], "close": row[5], "volume": row[6]})
                prior_high_sql.append(row[7])
                trs.append(row[8])
            if current is not None:
                _compare(current, dates, rows, trs, prior_high_sql, tally)

        print(f"  series                {tally.series}")
        print(f"  bars                  {tally.bars}")
        print(f"  ATR + prior-high values compared {2 * tally.bars}")
        print(f"  VALUE MISMATCHES      {tally.atr_mismatches}")
        print(f"  verdicts compared     {tally.bars}")
        print(f"  VERDICT MISMATCHES    {tally.verdict_mismatches}")
        print(f"  ties (< {TIE_TOLERANCE:g})       {tally.ties}   max margin {tally.max_tie_margin:.3e}")
        if tally.atr_mismatches or tally.verdict_mismatches:
            if tally.verdict_mismatches:
                print(f"  smallest real margin {tally.min_real_margin:.3e}")
            for problem in tally.atr_sample + tally.verdict_sample:
                print("   ", problem)
            failures += 1
        print(f"  elapsed               {time.monotonic() - started:.1f}s", flush=True)
    return failures


def _stamped_version() -> str:
    """The strategy version, with the source pinned either side of reading it.

    ⚠ MEASURED, NOT HYPOTHETICAL (2026-08-06, S-1 on this ticket).
    ``_source_hash`` re-reads the strategy file at CALL time, while Python
    imported the module once at process start. The probe harness mutates that
    file, runs a test and restores it; running the two concurrently printed a
    strategy version belonging to an INJECTED DEFECT next to figures produced by
    the clean code. A start-of-run vs end-of-run comparison DOES NOT catch it —
    the mutation is transient and restores itself, so the two ends agree. The
    check has to BRACKET THE READ, against an anchor taken at import.
    """
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved before stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    version = s4_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved while stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    return version


def census() -> int:
    """Verdict + refusal distribution over the §4.0 validated universe, masked."""
    started = time.monotonic()
    print(f"\n[census] strategy {S4_STRATEGY_ID} version {_stamped_version()}", flush=True)

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        print(f"  validated universe {len(universe)} instruments (US stocks ex-ETF, §4.0)", flush=True)
        series_ids = [
            row[0]
            for row in conn.execute(
                "SELECT series_id FROM research_price_series WHERE instrument_id = ANY(%(ids)s) ORDER BY series_id",
                {"ids": list(universe)},
            ).fetchall()
        ]
        print(f"  research series in it {len(series_ids)}", flush=True)

        verdicts: Counter[str] = Counter()
        reasons: Counter[str] = Counter()
        bars = 0
        narrowed = 0
        tail_refused = 0
        series_with_a_hole = 0
        empty = 0
        range_masked = 0
        return_masked = 0
        for n, series_id in enumerate(series_ids, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            range_masked += masked.range_masked
            return_masked += masked.return_masked
            rows: list[OHLCVRow] = [
                {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}  # type: ignore[typeddict-item]
                for b in masked.bars
            ]
            series = BarSeries(dates=tuple(b.bar_date for b in masked.bars), rows=tuple(rows))
            bars += len(series)
            for signal in s4_signals(series, universe=UNIVERSE, masked_reason="quarantined_bar"):
                verdicts[signal.verdict] += 1
                if signal.reason is not None:
                    reasons[signal.reason] += 1

            atr = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD)
            prior_high = prior_high_close_series(series, universe=UNIVERSE)
            compression = compression_rank_series(atr, universe=UNIVERSE)

            # The narrowing, counted rather than asserted harmless: bars whose
            # breakout leg is warm AND evaluable while the compression window is
            # still filling. §3.1 makes evaluability a property of the STRATEGY,
            # so those bars are refused although one leg could have been judged.
            #
            # ⚠ COUNTED PER BAR, not as `WARMUP_BARS - BREAKOUT_LOOKBACK` per
            # series. The closed form is right only on a series with no holes,
            # and this arm runs on MASKED bars by definition — a hole inside the
            # 20-bar frame makes the breakout leg a data refusal rather than a
            # narrowed one, and the closed form counts it anyway. Same per-bar
            # shape as `verify_2240_s1_momentum.py`.
            narrowed += sum(
                1
                for i in range(len(series) - 1)
                if prior_high.values[i] is not None
                and compression.values[i] is None
                and i not in compression.not_evaluable_indices
            )

            # ⚠ S-4's OWN BIAS, with no S-1 equivalent: bars refused ONLY because
            # a masked bar EARLIER in the series killed the Wilder recursion,
            # while the 20-bar breakout frame had already recovered and the bar's
            # own OHLC is complete. All three of high/low/close are required —
            # without them the refusal is the bar's own, not inherited.
            if atr.not_evaluable_indices:
                series_with_a_hole += 1
                bad = set(atr.not_evaluable_indices)
                tail_refused += sum(
                    1
                    for i in range(WARMUP_BARS, len(series) - 1)
                    if i in bad
                    and prior_high.values[i] is not None
                    and rows[i]["close"] is not None
                    and rows[i]["high"] is not None
                    and rows[i]["low"] is not None
                )
            if n % 500 == 0:
                print(f"  {n}/{len(series_ids)} series, {bars} bars ({time.monotonic() - started:.0f}s)", flush=True)

    total = sum(verdicts.values())
    print(f"  series with bars  {len(series_ids) - empty}   (fail-closed empties: {empty})")
    print(f"  bars              {bars}")
    print(f"  masked fields     range {range_masked} · return {return_masked}")
    for verdict in ("fired", "not_fired", "not_evaluable"):
        count = verdicts[verdict]
        share = 100.0 * count / total if total else 0.0
        print(f"      {verdict:<16} {count:>12,}  {share:6.3f}%")
    for reason, count in sorted(reasons.items()):
        print(f"        reason {reason:<20} {count:>12,}")
    print(f"  bars narrowed by the shared warm-up:            {narrowed:,}")
    print(f"  series carrying a masked bar:                   {series_with_a_hole:,}")
    print(f"  bars refused by the ATR tail alone:             {tail_refused:,}")
    print(f"  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    # A census reports; it has no pass/fail of its own beyond running clean.
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equivalence", action="store_true", help="both corpora, raw bars, vs a re-derivation")
    parser.add_argument("--census", action="store_true", help="validated universe, masked bars, verdict distribution")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if not (args.equivalence or args.census or args.all):
        parser.error("pick at least one arm: --equivalence, --census or --all")

    failures = 0
    if args.equivalence or args.all:
        failures += equivalence()
    if args.census or args.all:
        failures += census()
    print(f"\nverdict: {'*** FAIL ***' if failures else 'PASS'}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
