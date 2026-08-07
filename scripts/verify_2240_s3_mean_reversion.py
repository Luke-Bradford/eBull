"""Full-population verification of S-3 mean reversion within trend (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_s3_mean_reversion.py --all

⚠ NOTHING IS WRITTEN to any permanent table. Both arms read and print; the
equivalence arm creates TEMP tables, which live in the session and vanish with
it. Gate on the EXIT CODE — 0 means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

TWO ARMS, MEASURING DIFFERENT THINGS
------------------------------------
``--equivalence`` — every bar of BOTH corpora, ``research_price_daily`` and
``price_daily``, on RAW bars. The strategy's verdict is compared against a
Postgres re-derivation of the same rule. Same shape as
``verify_2240_s1_momentum.py``, with one addition S-1 did not need: **Postgres
re-derives Wilder RSI itself**, through a recursive CTE, rather than being handed
the Python value.

⚠ THE TWO SIDES OF THE *RSI* COMPARISON AGREE BIT-FOR-BIT, AND THAT IS ASSERTED
ON THE FULL POPULATION, not assumed. Both compute ``(gain*13 + up)/14`` in
IEEE754 doubles in the same order, so the arithmetic is not what is being tested
— the RECURSION STRUCTURE and the BAR ALIGNMENT are. Any disagreement on an RSI
value is therefore a logic difference, not a rounding one, and it is counted as a
hard mismatch with no tie allowance.

⚠ ``_compare`` compares the RSI VALUES directly, bar by bar, rather than only the
verdicts they feed. The first version of this docstring rested on a THREE-SERIES
measurement (16,236 of 16,236 exactly equal), which is the sample-not-population
shape `.claude/CLAUDE.md` forbids; the claim is now produced by the run that
makes it. It is also what licenses the conditional tie in ``_margin`` — see there
for the entry-leg conjunction hole this closed.

⚠ THE TWO SIDES OF THE *SMA* COMPARISON DO NOT, deliberately — Postgres averages
``numeric`` exactly, ``sma_series`` accumulates ``float``. A bar whose close sits
within a float ULP of its 200-day average can legitimately fall either way, so
those are reported as TIES with their margin rather than folded into either
column. S-1's reasoning, unchanged.

⚠ THE RAW CORPORA HAVE NO NULL CLOSES, so this arm does NOT exercise the
hole-handling path — asserted at run time rather than assumed, because the
assertion is what makes the gap visible if a future ingest changes it. Masked
bars are the ``--census`` arm's job.

``--census`` — the §4.0 validated universe (US stocks ex-ETF) on MASKED bars, via
the fail-closed loader. Reports the verdict distribution and the refusal
breakdown by reason (criterion 9's "measure what you reject"), plus TWO
S-3-specific counts:

- the bars the shared warm-up narrows away from the exit leg, as S-1 reports; and
- ⚠ **the bars refused because a hole EARLIER in the series killed the RSI
  recursion** while the 200-day average had already recovered. This is S-3's own
  bias and has no S-1 equivalent: Wilder smoothing carries state, so a masked bar
  refuses every bar after it rather than a 200-bar window.

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
from typing import LiteralString

import psycopg
from psycopg import sql as pgsql

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries, rsi_series, sma_series
from app.services.research_price_structure_store import load_masked_series
from app.services.strategies.s3_mean_reversion_in_trend import (
    EXIT_THRESHOLD,
    OVERSOLD_THRESHOLD,
    RSI_PERIOD,
    S3_STRATEGY_ID,
    TREND_PERIOD,
    _source_hash,
    s3_identity,
    s3_signals,
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

#: Relative margin below which a disagreement is arithmetic, not logic. Applies
#: to the SMA comparison ONLY — see the module docstring on why the RSI
#: comparison gets no tie allowance.
TIE_TOLERANCE = 1e-9

#: ⚠ Read at IMPORT, microseconds after the strategy module is loaded, so it is
#: the hash of the code this process actually runs. See ``_stamped_version``.
_SOURCE_AT_IMPORT = _source_hash()


# ---------------------------------------------------------------------------
# Postgres' own re-derivation
# ---------------------------------------------------------------------------

#: Per-bar close-to-close deltas, in float8 so the recursion below is IEEE754
#: like Python's rather than scale-rounded ``numeric``.
_DELTA_TEMPLATE = """
CREATE TEMP TABLE s3_delta AS
SELECT {key} AS series_key, rn, delta FROM (
  SELECT {key},
         row_number() OVER (PARTITION BY {key} ORDER BY {date_column}) AS rn,
         close::float8 - lag(close::float8) OVER (PARTITION BY {key} ORDER BY {date_column}) AS delta
  FROM {table}
) t
"""

#: ⚠ LOCKSTEP, NOT PER-SERIES, and the difference is 150x.
#: A recursive CTE run once per series costs one index probe per bar — measured
#: at 0.45 ms/bar, i.e. ~3 hours over the 25.8M-bar research corpus. Advancing
#: EVERY series together makes the recursion depth the deepest series (16,236)
#: and the working set the number still alive, so each iteration is one batched
#: join instead of thousands of probes: 79 s for the same arithmetic.
#:
#: The seed is Wilder's: a SIMPLE average of the first ``period`` deltas, at
#: ``rn = period + 1`` (1-indexed) — which is Python's index ``period``. The
#: ``HAVING count(*) = period`` drops series too short to seed at all, and the
#: LEFT JOIN downstream turns that into the NULL the Python side also produces.
_WALK_TEMPLATE = """
CREATE TEMP TABLE s3_walk AS
WITH RECURSIVE seed AS (
  SELECT series_key, {seed_rn}::bigint AS rn,
         sum(greatest(delta, 0)) / {period_float} AS gain,
         sum(greatest(-delta, 0)) / {period_float} AS loss
  FROM s3_delta WHERE rn BETWEEN 2 AND {seed_rn}
  GROUP BY series_key HAVING count(*) = {period}
),
walk AS (
  SELECT series_key, rn, gain, loss FROM seed
  UNION ALL
  SELECT d.series_key, d.rn,
         (w.gain * {prior} + greatest(d.delta, 0)) / {period},
         (w.loss * {prior} + greatest(-d.delta, 0)) / {period}
  FROM walk w JOIN s3_delta d ON d.rn = w.rn + 1 AND d.series_key = w.series_key
)
SELECT * FROM walk
"""

#: ⚠ ``count(close)`` and ``count(*)`` are BOTH required on the trend window. The
#: first rejects a window holding a NULL close; the second rejects a window that
#: has not filled yet. Either alone admits the other case, and they are different
#: verdicts — a hole is a data gap, a short window is warm-up (criterion 8).
#:
#: The RSI value is built HERE, in SQL, including the flat-series convention, so
#: that no part of the comparison borrows the Python implementation.
#:
#: ⚠ The row number is materialised in a CTE rather than written inline in the
#: JOIN condition, because a window function is not allowed there. Noted so the
#: shape reads as a restriction rather than as a preference.
_BAR_TEMPLATE = """
WITH numbered AS (
  SELECT {key} AS series_key,
         {date_column} AS bar_day,
         close, open, high, low, volume,
         row_number() OVER (PARTITION BY {key} ORDER BY {date_column}) AS rn,
         CASE WHEN count(*) OVER w_trend = {trend} AND count(close) OVER w_trend = {trend}
              THEN avg(close) OVER w_trend END AS sma_trend
  FROM {table}
  WINDOW w_trend AS (PARTITION BY {key} ORDER BY {date_column}
                     ROWS BETWEEN {trend_offset} PRECEDING AND CURRENT ROW)
)
SELECT n.series_key, n.bar_day, n.close, n.sma_trend,
       CASE WHEN w.gain IS NULL THEN NULL
            WHEN w.gain = 0 AND w.loss = 0 THEN 50.0
            WHEN w.loss = 0 THEN 100.0
            ELSE 100.0 - 100.0 / (1.0 + w.gain / w.loss) END AS rsi,
       n.open, n.high, n.low, n.volume
FROM numbered n
LEFT JOIN s3_walk w ON w.series_key = n.series_key AND w.rn = n.rn
ORDER BY n.series_key, n.bar_day
"""


def _format(template: LiteralString, table: str, key: str, date_column: str) -> pgsql.Composed:
    """Compose through ``psycopg.sql`` rather than an f-string: the table and key
    vary per corpus, and neither a frame offset nor a recursion coefficient can
    be a query parameter, so names and periods are interpolated by kind instead
    of pasted into one string.

    ⚠ Every period goes through ``Literal``, never ``SQL(str(n))``. Both render
    the same digits, but ``SQL`` takes a ``LiteralString`` precisely so that a
    runtime-built fragment cannot be smuggled into a query — and
    ``SQL(str(anything))`` defeats that check for every future caller, not just
    this one. These values are module constants and are safe; the type is not.
    """
    return pgsql.SQL(template).format(
        table=pgsql.Identifier(table),
        key=pgsql.Identifier(key),
        date_column=pgsql.Identifier(date_column),
        period=pgsql.Literal(RSI_PERIOD),
        period_float=pgsql.Literal(float(RSI_PERIOD)),
        prior=pgsql.Literal(RSI_PERIOD - 1),
        seed_rn=pgsql.Literal(RSI_PERIOD + 1),
        trend=pgsql.Literal(TREND_PERIOD),
        trend_offset=pgsql.Literal(TREND_PERIOD - 1),
    )


CORPORA = (
    ("research_price_daily", "series_id", "bar_date"),
    ("price_daily", "instrument_id", "price_date"),
)


def _sql_verdicts(
    closes: list[Decimal | None],
    trend: list[Decimal | None],
    rsi: list[float | None],
) -> list[tuple[str, str]]:
    """(entry, exit) per bar, from Postgres' own indicators only.

    Mirrors the strategy's contract without sharing a line of its code: the last
    bar has no ``t+1``; a bar missing ANY of the three inputs is unevaluable on
    BOTH legs (the shared warm-up); otherwise the three strict comparisons decide.
    """
    n = len(closes)
    out: list[tuple[str, str]] = []
    for i in range(n):
        if i == n - 1:
            out.append(("not_evaluable", "not_evaluable"))
            continue
        close, trend_value, rsi_value = closes[i], trend[i], rsi[i]
        if close is None or trend_value is None or rsi_value is None:
            out.append(("not_evaluable", "not_evaluable"))
            continue
        entry = "fired" if (rsi_value < OVERSOLD_THRESHOLD and float(close) > float(trend_value)) else "not_fired"
        exit_ = "fired" if rsi_value > EXIT_THRESHOLD else "not_fired"
        out.append((entry, exit_))
    return out


def _relative(a: float, b: float) -> float:
    return abs(a - b) / max(abs(b), 1e-12)


def _margin(
    kind: str,
    close: Decimal | None,
    trend: Decimal | None,
    rsi_python: float | None,
    rsi_sql: float | None,
) -> float:
    """How close the deciding comparison was, relatively.

    ⚠ ONLY THE SMA COMPARISON EARNS A TIE, and the entry leg is a CONJUNCTION, so
    that is not automatic. An earlier version returned the close-vs-trend gap for
    any entry disagreement — which silently attributed the disagreement to the SMA
    comparison whenever the close happened to sit near its 200-day average. A real
    RSI walk defect landing on such a bar would have been reclassified as a float
    tie and swallowed, defeating the "no tie allowance for RSI" guarantee on
    exactly the leg where both comparisons live. (Review bot, PR #2322.)

    So the tie is CONDITIONAL on the RSI halves being provably identical at this
    bar. If they differ by so much as an ULP, the disagreement might be theirs and
    the margin is ``inf`` — a hard mismatch. The exit leg reads only the RSI, so it
    never earns a tie at all.
    """
    if kind == "exit" or close is None or trend is None:
        return float("inf")
    # ⚠ Not `!=` on two Nones: a bar where one side has a value and the other does
    # not is precisely the alignment defect this arm exists to find, so it must
    # not be excused either.
    if rsi_python is None or rsi_sql is None or rsi_python != rsi_sql:
        return float("inf")
    return _relative(float(close), float(trend))


#: How many failing bars are printed. Past this the run still COUNTS every one —
#: a truncated sample with an honest total, never a silently capped total.
_SAMPLE_LIMIT = 20


class _Tally:
    """Counts, plus a bounded sample of each failure kind.

    ⚠ The count and the sample are separate fields ON PURPOSE. S-1's script keeps
    one list and appends `""` past the cap so that `len()` stays the total, which
    grows an unbounded list of empty strings over a corpus-sized failure — flagged
    by the review bot on PR #2322. A counter says the same thing in 8 bytes.
    """

    def __init__(self) -> None:
        self.series = 0
        self.bars = 0
        self.mismatch_count = 0
        self.mismatch_sample: list[str] = []
        self.rsi_mismatch_count = 0
        self.rsi_mismatch_sample: list[str] = []
        self.ties = 0
        self.max_tie_margin = 0.0
        self.min_real_margin = float("inf")

    def record(self, kind: str, message: str) -> None:
        if kind == "rsi":
            self.rsi_mismatch_count += 1
            sample = self.rsi_mismatch_sample
        else:
            self.mismatch_count += 1
            sample = self.mismatch_sample
        if len(sample) < _SAMPLE_LIMIT:
            sample.append(message)


def _compare(
    key: int,
    dates: list[date],
    rows: list[OHLCVRow],
    trend_sql: list[Decimal | None],
    rsi_sql: list[float | None],
    tally: _Tally,
) -> None:
    series = BarSeries(dates=tuple(dates), rows=tuple(rows))
    signals = s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar")
    entries = [s for s in signals if s.kind == "entry"]
    exits = [s for s in signals if s.kind == "exit"]
    closes = series.closes
    expected = _sql_verdicts(closes, trend_sql, rsi_sql)

    # ⚠ THE RSI VALUES ARE COMPARED DIRECTLY, not merely through the verdicts they
    # produce. The module docstring's "bit-for-bit" claim rested on a THREE-SERIES
    # measurement taken before this script existed, which is the sample-not-
    # population shape `.claude/CLAUDE.md` forbids. Comparing them here turns it
    # into a full-population assertion — and it is what licenses the conditional
    # tie in `_margin`, since a tie is only sound where the RSI halves agree.
    rsi_python = rsi_series(series, universe=UNIVERSE, period=RSI_PERIOD).values

    # ⚠ THE ALIGNMENT IS ENFORCED, NOT ASSUMED — and the distinction is this
    # prevention log's own ("An independent verifier that is only ACCIDENTALLY
    # right", #2240 4a): an arm that leans on an invariant must either enforce it
    # or encode it, because a green run cannot tell you which it did.
    #
    # Every length here is equal BY CONSTRUCTION today — `BarSeries.__post_init__`
    # pins dates against rows, the streaming loop appends once per row to all four
    # lists, and `IndicatorSeries`' contract is `len(values) == len(input)`. But
    # that last one is a documented promise each `*_series` function keeps, not
    # something `IndicatorSeries` can validate (it holds no reference to the bars).
    # So a future indicator that returns a trimmed series would surface here as an
    # `IndexError` from a bare `[i]` — a stack trace naming the wrong thing —
    # rather than as the alignment defect this arm exists to catch. (Review bot,
    # PR #2322.)
    lengths = {
        "dates": len(dates),
        "rows": len(rows),
        "trend_sql": len(trend_sql),
        "rsi_sql": len(rsi_sql),
        "rsi_python": len(rsi_python),
        "entries": len(entries),
        "exits": len(exits),
        "expected": len(expected),
    }
    if len(set(lengths.values())) != 1:
        raise RuntimeError(
            f"series {key} is misaligned across the two derivations: {lengths} — "
            "every list must carry exactly one entry per bar before any of them is indexed"
        )

    tally.series += 1
    tally.bars += len(series)
    for i in range(len(series)):
        if rsi_python[i] != rsi_sql[i]:
            tally.record("rsi", f"{key} {dates[i]}: python={rsi_python[i]!r} sql={rsi_sql[i]!r}")

    for i, (want_entry, want_exit) in enumerate(expected):
        for kind, got, want in (("entry", entries[i].verdict, want_entry), ("exit", exits[i].verdict, want_exit)):
            if got == want:
                continue
            margin = _margin(kind, closes[i], trend_sql[i], rsi_python[i], rsi_sql[i])
            if margin < TIE_TOLERANCE:
                tally.ties += 1
                tally.max_tie_margin = max(tally.max_tie_margin, margin)
                continue
            tally.min_real_margin = min(tally.min_real_margin, margin)
            tally.record("verdict", f"{key} {dates[i]} {kind}: python={got} sql={want} margin={margin:.3e}")


def _assert_no_null_closes(conn: psycopg.Connection[object], table: str) -> None:
    """⚠ ASSERTED, NOT ASSUMED. The SQL re-derivation does not model the
    first-NULL-close truncation ``rsi_series`` performs, because the raw corpora
    have no NULL closes. If that ever changes, this arm would silently start
    comparing two different rules — so it refuses instead."""
    total, present = conn.execute(  # type: ignore[misc]
        pgsql.SQL("SELECT count(*), count(close) FROM {}").format(pgsql.Identifier(table))
    ).fetchone()
    if total != present:
        raise RuntimeError(
            f"{table} holds {total - present} NULL closes; this arm's SQL does not model RSI's "
            "first-NULL truncation, so its verdicts would diverge for a reason that is not a defect"
        )


def equivalence() -> int:
    """Every bar of both corpora: strategy verdict vs a SQL re-derivation."""
    failures = 0
    for table, key, date_column in CORPORA:
        started = time.monotonic()
        tally = _Tally()
        print(f"\n[{table}] re-deriving Wilder RSI in Postgres…", flush=True)
        with psycopg.connect(settings.database_url) as conn:
            _assert_no_null_closes(conn, table)
            conn.execute(_format(_DELTA_TEMPLATE, table, key, date_column))
            conn.execute(pgsql.SQL("CREATE INDEX ON s3_delta (rn, series_key)"))
            conn.execute(pgsql.SQL("ANALYZE s3_delta"))
            print(f"  deltas {time.monotonic() - started:.0f}s", flush=True)
            conn.execute(_format(_WALK_TEMPLATE, table, key, date_column))
            conn.execute(pgsql.SQL("CREATE INDEX ON s3_walk (series_key, rn)"))
            conn.execute(pgsql.SQL("ANALYZE s3_walk"))
            print(f"  walk   {time.monotonic() - started:.0f}s — streaming…", flush=True)

            with conn.cursor(name=f"s3_{table}") as cur:
                cur.itersize = 50_000
                cur.execute(_format(_BAR_TEMPLATE, table, key, date_column))
                current: int | None = None
                dates: list[date] = []
                rows: list[OHLCVRow] = []
                trend_sql: list[Decimal | None] = []
                rsi_sql: list[float | None] = []
                for row in cur:
                    row_key = row[0]
                    if row_key != current:
                        if current is not None:
                            _compare(current, dates, rows, trend_sql, rsi_sql, tally)
                            if tally.series % 500 == 0:
                                print(
                                    f"  {tally.series} series, {tally.bars} bars, "
                                    f"{tally.mismatch_count} mismatches, {tally.ties} ties "
                                    f"({time.monotonic() - started:.0f}s)",
                                    flush=True,
                                )
                        current, dates, rows, trend_sql, rsi_sql = row_key, [], [], [], []
                    dates.append(row[1])
                    rows.append({"open": row[5], "high": row[6], "low": row[7], "close": row[2], "volume": row[8]})
                    trend_sql.append(row[3])
                    rsi_sql.append(None if row[4] is None else float(row[4]))
                if current is not None:
                    _compare(current, dates, rows, trend_sql, rsi_sql, tally)

        real = tally.mismatch_count
        rsi_real = tally.rsi_mismatch_count
        print(f"  series            {tally.series}")
        print(f"  bars              {tally.bars}")
        print(f"  verdicts compared {2 * tally.bars}")
        print(f"  RSI VALUE MISMATCHES {rsi_real}   (exact equality, no tolerance)")
        print(f"  MISMATCHES        {real}")
        print(f"  ties (< {TIE_TOLERANCE:g})   {tally.ties}   max margin {tally.max_tie_margin:.3e}")
        if rsi_real:
            for problem in tally.rsi_mismatch_sample:
                print("   ", problem)
            failures += 1
        if real:
            print(f"  smallest real margin {tally.min_real_margin:.3e}")
            for problem in tally.mismatch_sample:
                print("   ", problem)
            failures += 1
        print(f"  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    return failures


def _stamped_version() -> str:
    """The strategy version, with the source pinned either side of reading it.

    ⚠ S-1's lesson, unchanged and it applies to every strategy in this
    directory: ``_source_hash`` re-reads the strategy file at CALL time, while
    Python imported the module once at process start. The probe harness mutates
    that file, runs a test and restores it — so a concurrent run stamps clean
    figures with an INJECTED source hash, and a start-vs-end comparison does NOT
    catch it because the probe restores the file. The checks have to bracket the
    read itself, against an anchor taken at import.
    """
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved before stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    version = s3_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved while stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    return version


def census() -> int:
    """Verdict + refusal distribution over the §4.0 validated universe, masked."""
    started = time.monotonic()
    print(f"\n[census] strategy {S3_STRATEGY_ID} version {_stamped_version()}", flush=True)

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
        rsi_tail_refused = 0
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
            for signal in s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"):
                verdicts[(signal.kind, signal.verdict)] += 1
                if signal.reason is not None:
                    reasons[(signal.kind, signal.reason)] += 1

            rsi = rsi_series(series, universe=UNIVERSE, period=RSI_PERIOD)
            trend = sma_series(series, universe=UNIVERSE, period=TREND_PERIOD)
            trend_holes = set(trend.not_evaluable_indices)
            rsi_holes = set(rsi.not_evaluable_indices)
            # The narrowing, counted rather than asserted harmless: bars whose
            # RSI is warm and whose trend average is not. The exit leg could have
            # been judged there and is refused, because §3.1 makes evaluability a
            # property of the strategy, not of the leg.
            narrowed += sum(
                1
                for i in range(len(series) - 1)
                if rsi.values[i] is not None and trend.values[i] is None and i not in trend_holes
            )
            # ⚠ S-3's OWN BIAS, with no S-1 equivalent: bars the 200-day average
            # has recovered on but the RSI recursion never will, because a hole
            # earlier in the series ended it. Every one of these is a decision
            # S-1 would have made and S-3 cannot.
            if rsi_holes:
                series_with_a_hole += 1
                rsi_tail_refused += sum(
                    1
                    for i in range(len(series) - 1)
                    if i in rsi_holes and i not in trend_holes and trend.values[i] is not None
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
    print(f"  exit bars narrowed by the shared warm-up:            {narrowed:,}")
    print(f"  series carrying at least one masked close:           {series_with_a_hole:,}")
    print(f"  bars refused by the RSI tail that the trend recovered: {rsi_tail_refused:,}")
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
