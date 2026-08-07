"""Full-population verification of S-1 time-series momentum (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_s1_momentum.py --all

⚠ NOTHING IS WRITTEN. Both arms read and print. Gate on the EXIT CODE — 0 means
every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

TWO ARMS, MEASURING DIFFERENT THINGS
------------------------------------
``--equivalence`` — every bar of BOTH corpora, ``research_price_daily`` and
``price_daily``, on RAW bars. The strategy's verdict is compared against a
Postgres re-derivation of the same rule from window functions. ⚠ The two
derivations are independent, which is the point: the Python side streams a
running sum and indexes a ``BarSeries``; the SQL side has no index and no state.
A shared off-by-one would have to occur in both to go unnoticed. Same shape as
``verify_2240_signal_ledger_fills.py``, which checked 3c's fill arithmetic
against ``lead()``.

⚠ THE TWO SIDES USE DIFFERENT ARITHMETIC AND THAT IS DELIBERATE — Postgres
averages ``numeric`` exactly, ``sma_series`` accumulates ``float``. A verdict is
a comparison, so a bar where the close sits within a float ULP of its average
can legitimately fall either way. Those are reported as TIES with their margin
rather than folded into either column: calling them mismatches would be false,
and silently allowing them would hide a real one.

``--census`` — the §4.0 validated universe (US stocks ex-ETF) on MASKED bars,
via the fail-closed loader. Reports the verdict distribution and the refusal
breakdown by reason, which is criterion 9's "measure what you reject", plus the
count of bars the shared warm-up narrows away (see ``s1_signals``' docstring).

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
from app.services.indicator_series import BarSeries, sma_series
from app.services.research_price_structure_store import load_masked_series
from app.services.strategies.s1_time_series_momentum import (
    FAST_PERIOD,
    S1_STRATEGY_ID,
    SLOW_PERIOD,
    _source_hash,
    s1_identity,
    s1_signals,
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

#: Relative margin below which a disagreement is arithmetic, not logic. One part
#: in 1e-9 of the price is ~150x looser than float64's ~1e-16 and ~7 orders
#: tighter than any real crossing.
TIE_TOLERANCE = 1e-9

#: ⚠ Read at IMPORT, which is microseconds after the strategy module is loaded,
#: so it is the hash of the code this process actually runs. See
#: ``_stamped_version`` for why an end-of-run comparison is not enough.
_SOURCE_AT_IMPORT = _source_hash()


#: ⚠ ``count(close)`` and ``count(*)`` are BOTH required. The first rejects a
#: window holding a NULL close; the second rejects a window that has not filled
#: yet. Either alone admits the other case, and they are different verdicts —
#: a hole is a data gap, a short window is warm-up (criterion 8).
_BAR_TEMPLATE = """
SELECT {key},
       {date_column},
       close,
       CASE WHEN count(*) OVER w_fast = {fast} AND count(close) OVER w_fast = {fast}
            THEN avg(close) OVER w_fast END AS sma_fast,
       CASE WHEN count(*) OVER w_slow = {slow} AND count(close) OVER w_slow = {slow}
            THEN avg(close) OVER w_slow END AS sma_slow,
       open, high, low, volume
FROM {table}
WINDOW w_fast AS (PARTITION BY {key} ORDER BY {date_column}
                  ROWS BETWEEN {fast_offset} PRECEDING AND CURRENT ROW),
       w_slow AS (PARTITION BY {key} ORDER BY {date_column}
                  ROWS BETWEEN {slow_offset} PRECEDING AND CURRENT ROW)
ORDER BY {key}, {date_column}
"""


def _bar_sql(table: str, key: str, date_column: str) -> pgsql.Composed:
    """Per-bar closes plus Postgres' own SMA at each bar.

    Composed through ``psycopg.sql`` rather than an f-string: the table and key
    vary per corpus, and a frame offset cannot be a query parameter, so the two
    kinds of interpolation are separated — ``Identifier`` for names, ``Literal``
    for the periods — instead of pasted into one string.
    """
    return pgsql.SQL(_BAR_TEMPLATE).format(
        table=pgsql.Identifier(table),
        key=pgsql.Identifier(key),
        date_column=pgsql.Identifier(date_column),
        fast=pgsql.Literal(FAST_PERIOD),
        slow=pgsql.Literal(SLOW_PERIOD),
        fast_offset=pgsql.Literal(FAST_PERIOD - 1),
        slow_offset=pgsql.Literal(SLOW_PERIOD - 1),
    )


CORPORA = (
    ("research_price_daily", _bar_sql("research_price_daily", "series_id", "bar_date")),
    ("price_daily", _bar_sql("price_daily", "instrument_id", "price_date")),
)


def _sql_verdicts(
    closes: list[Decimal | None],
    fast: list[Decimal | None],
    slow: list[Decimal | None],
) -> list[tuple[str, str]]:
    """(entry, exit) per bar, from Postgres' averages only.

    Mirrors the strategy's contract without sharing a line of its code: the last
    bar has no ``t+1``; a bar missing either average is unevaluable on BOTH legs
    (the shared warm-up); otherwise the two strict comparisons decide.
    """
    n = len(closes)
    out: list[tuple[str, str]] = []
    for i in range(n):
        if i == n - 1:
            out.append(("not_evaluable", "not_evaluable"))
            continue
        close, fast_value, slow_value = closes[i], fast[i], slow[i]
        if close is None or fast_value is None or slow_value is None:
            out.append(("not_evaluable", "not_evaluable"))
            continue
        entry = "fired" if (close > slow_value and fast_value > slow_value) else "not_fired"
        exit_ = "fired" if close < fast_value else "not_fired"
        out.append((entry, exit_))
    return out


def _relative(a: Decimal, b: Decimal) -> float:
    scale = max(abs(float(b)), 1e-12)
    return abs(float(a) - float(b)) / scale


def _margin(
    kind: str,
    close: Decimal | None,
    fast: Decimal | None,
    slow: Decimal | None,
) -> float:
    """How close the deciding comparison was, relatively.

    A disagreement can only come from a comparison that was nearly an equality,
    so the smallest relative gap among the comparisons the leg makes is the
    figure that says whether the two arithmetics could legitimately differ.
    """
    if close is None or fast is None or slow is None:
        return float("inf")
    if kind == "exit":
        return _relative(close, fast)
    return min(_relative(close, slow), _relative(fast, slow))


class _Tally:
    def __init__(self) -> None:
        self.series = 0
        self.bars = 0
        self.mismatches: list[str] = []
        self.ties = 0
        self.max_tie_margin = 0.0
        self.min_real_margin = float("inf")


def _compare(
    key: int,
    dates: list[date],
    rows: list[OHLCVRow],
    fast_sql: list[Decimal | None],
    slow_sql: list[Decimal | None],
    tally: _Tally,
) -> None:
    series = BarSeries(dates=tuple(dates), rows=tuple(rows))
    signals = s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar")
    entries = [s for s in signals if s.kind == "entry"]
    exits = [s for s in signals if s.kind == "exit"]
    closes = series.closes
    expected = _sql_verdicts(closes, fast_sql, slow_sql)

    tally.series += 1
    tally.bars += len(series)
    for i, (want_entry, want_exit) in enumerate(expected):
        for kind, got, want in (("entry", entries[i].verdict, want_entry), ("exit", exits[i].verdict, want_exit)):
            if got == want:
                continue
            margin = _margin(kind, closes[i], fast_sql[i], slow_sql[i])
            if margin < TIE_TOLERANCE:
                tally.ties += 1
                tally.max_tie_margin = max(tally.max_tie_margin, margin)
                continue
            tally.min_real_margin = min(tally.min_real_margin, margin)
            if len(tally.mismatches) < 20:
                tally.mismatches.append(f"{key} {dates[i]} {kind}: python={got} sql={want} margin={margin:.3e}")
            else:  # keep counting past the printed sample
                tally.mismatches.append("")


def equivalence() -> int:
    """Every bar of both corpora: strategy verdict vs a SQL re-derivation."""
    failures = 0
    for table, sql in CORPORA:
        started = time.monotonic()
        tally = _Tally()
        print(f"\n[{table}] streaming…", flush=True)
        with psycopg.connect(settings.database_url) as conn, conn.cursor(name=f"s1_{table}") as cur:
            cur.itersize = 50_000
            cur.execute(sql)
            current: int | None = None
            dates: list[date] = []
            rows: list[OHLCVRow] = []
            fast_sql: list[Decimal | None] = []
            slow_sql: list[Decimal | None] = []
            for row in cur:
                key = row[0]
                if key != current:
                    if current is not None:
                        _compare(current, dates, rows, fast_sql, slow_sql, tally)
                        if tally.series % 500 == 0:
                            print(
                                f"  {tally.series} series, {tally.bars} bars, "
                                f"{len(tally.mismatches)} mismatches, {tally.ties} ties "
                                f"({time.monotonic() - started:.0f}s)",
                                flush=True,
                            )
                    current, dates, rows, fast_sql, slow_sql = key, [], [], [], []
                dates.append(row[1])
                rows.append({"open": row[5], "high": row[6], "low": row[7], "close": row[2], "volume": row[8]})
                fast_sql.append(row[3])
                slow_sql.append(row[4])
            if current is not None:
                _compare(current, dates, rows, fast_sql, slow_sql, tally)

        real = len(tally.mismatches)
        print(f"  series            {tally.series}")
        print(f"  bars              {tally.bars}")
        print(f"  verdicts compared {2 * tally.bars}")
        print(f"  MISMATCHES        {real}")
        print(f"  ties (< {TIE_TOLERANCE:g})   {tally.ties}   max margin {tally.max_tie_margin:.3e}")
        if real:
            print(f"  smallest real margin {tally.min_real_margin:.3e}")
            for problem in [m for m in tally.mismatches if m][:20]:
                print("   ", problem)
            failures += 1
        print(f"  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    return failures


def _stamped_version() -> str:
    """The strategy version, with the source pinned either side of reading it.

    ⚠ MEASURED, NOT HYPOTHETICAL (2026-08-06, this ticket). ``_source_hash``
    re-reads the strategy file at CALL time, while Python imported the module
    once at process start. ``scripts/probe_2240_s1_momentum.py`` mutates that
    file, runs a test and restores it; running the two concurrently printed a
    strategy version belonging to an INJECTED DEFECT next to figures produced by
    the clean code. The behaviour was clean and the stamp was a lie, which is
    the worse half — a version string is what a later reader keys the numbers on.

    ⚠ A start-of-run vs end-of-run comparison DOES NOT CATCH THIS, and that was
    the first fix written here (Codex, checkpoint 2). The real incident was a
    *transient* mutation that restored itself, so the two ends agree and the run
    reports success with the wrong stamp. The check has to sit where the read
    happens, against an anchor taken at import:

    - ``_SOURCE_AT_IMPORT`` is read microseconds after the module is imported,
      so it is the hash of the code that actually runs;
    - both checks below bracket the ONLY file read in this script (the version
      property), so a mutation cannot be active for the stamp without also
      being active for at least one check.

    A mutation active at import AND at the stamp is not contamination — it is a
    run of the mutated code, correctly stamped.
    """
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved before stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    version = s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved while stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    return version


def census() -> int:
    """Verdict + refusal distribution over the §4.0 validated universe, masked."""
    started = time.monotonic()
    print(f"\n[census] strategy {S1_STRATEGY_ID} version {_stamped_version()}", flush=True)

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

        verdicts: Counter[tuple[str, str]] = Counter()
        reasons: Counter[tuple[str, str]] = Counter()
        bars = 0
        narrowed = 0
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
            for signal in s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"):
                verdicts[(signal.kind, signal.verdict)] += 1
                if signal.reason is not None:
                    reasons[(signal.kind, signal.reason)] += 1
            # The narrowing, counted rather than asserted harmless: bars whose
            # fast average is warm and whose slow average is not. The exit leg
            # could have been judged there and is refused, because §3.1 makes
            # evaluability a property of the strategy, not of the leg.
            fast = sma_series(series, universe=UNIVERSE, period=FAST_PERIOD)
            slow = sma_series(series, universe=UNIVERSE, period=SLOW_PERIOD)
            narrowed += sum(
                1
                for i in range(len(series) - 1)
                if fast.values[i] is not None and slow.values[i] is None and i not in slow.not_evaluable_indices
            )
            if n % 500 == 0:
                print(f"  {n}/{len(series_ids)} series, {bars} bars ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"  series with bars  {len(series_ids) - empty}   (fail-closed empties: {empty})")
    print(f"  bars              {bars}")
    print(f"  masked fields     range {range_masked} · return {return_masked}")
    for kind in ("entry", "exit"):
        total = sum(count for (k, _), count in verdicts.items() if k == kind)
        print(f"  {kind}:")
        for verdict in ("fired", "not_fired", "not_evaluable"):
            count = verdicts[(kind, verdict)]
            share = 100.0 * count / total if total else 0.0
            print(f"      {verdict:<16} {count:>12,}  {share:6.3f}%")
        for reason, count in sorted(reasons.items()):
            if reason[0] == kind:
                print(f"        reason {reason[1]:<20} {count:>12,}")
    print(f"  exit bars narrowed by the shared warm-up: {narrowed:,}")
    print(f"  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    # A census reports; it has no pass/fail of its own beyond running clean.
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equivalence", action="store_true", help="both corpora, raw bars, vs a SQL re-derivation")
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
