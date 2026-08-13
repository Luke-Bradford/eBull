"""Full-population verification of the buy-and-hold benchmark composition (#2426).

    PYTHONPATH=. uv run python scripts/verify_2426_benchmark.py --compositions
    PYTHONPATH=. uv run python scripts/verify_2426_benchmark.py --emit-version-map

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0 means
every assertion held, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

THE ARMS
--------
``--compositions`` — build the REAL benchmark book over the §4.0 validated
universe on the whole evaluation axis, then run the SAME legs through three
compositions and assert the properties criterion 7 needs. This is the table the
spec and `sql/275`'s header quote, and it is why they quote a command rather
than a hand-copied figure.

  A  ``equal_weight_concurrent_v1`` — what shipped. The strategy sizing rule,
     which re-imposes equal weight on every event date.
  B  ``equal_weight_buy_and_hold_v1`` — the fix. 1/N committed at each leg's own
     entry, held to its own exit, never rebalanced.
  C  the premise #2426 stated — the naive sum of per-instrument hold returns.
     Reported to FALSIFY it, not because anything computes it.

  P1  **The benchmark does not rebalance.** ``rebalance_costs == 0.0`` and
      ``Σ traded_notional`` equals ``Σ(entry allocations) + Σ(exit proceeds)``
      exactly. Entries and exits ARE trades; a rebalance is what must be absent.
  P2  **No leverage, ever.** ``equity - invested`` is cash and is never negative
      on any date. Stronger here than on the strategy curve: total commitment is
      exactly ``n × (starting_equity / n)``, so it holds by arithmetic.
  P3  **The composition identity is exact.** With no rebalancing, the portfolio's
      total return IS ``mean(exit/entry) − 1`` over the legs. This is algebra,
      not a plausibility check — it is the assertion that no path-dependence
      leaked into a composition that must not have any.
  P4  **The premise is falsified.** A ≠ C on identical legs. If the mechanism
      were summation the two would be equal.

``--emit-version-map`` — re-derive each stored row's ``result_version`` under the
completed identity payload. ⚠ It ASSERTS that it reproduces the row's stored
(old) hash from the stored columns BEFORE emitting the new one; without that
check an emitted literal would be a guess, and a wrong one makes the row
permanently unreadable through ``result_ledger._result_from_row``. Produced the
literals in `sql/275_strategy_result_benchmark_rule.sql`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from array import array
from typing import Any

import numpy as np
import psycopg

from app.config import settings
from app.services.backtest_run import _benchmark_book, _to_series, load_corpus
from app.services.equity_curve import LegBook, build_buy_and_hold_curve, build_equity_curve
from app.services.research_price_structure_store import load_masked_series
from app.services.strategy_result import RESULT_SET_ID

#: What produced every row stored before #2426 — the sizing rule the benchmark
#: silently inherited. ⚠ A literal rather than an import of ``SIZING_RULE_ID``:
#: this is a historical fact about 24 rows, and it must not move if the strategy
#: sizing rule is ever re-versioned.
LEGACY_BENCHMARK_RULE = "equal_weight_concurrent_v1"

_IDENTITY_SQL = """
    SELECT result_id, strategy_id, strategy_version, result_scope, namespace, ambiguity_arm,
           quarantine_arm, sizing_rule, cost_model_id, corpus_version, window_start, window_end,
           position_rule_set_version, outcome_rule_set_version, input_rule_set_version, result_version
    FROM strategy_results_store
    ORDER BY result_id
"""


def _version(payload: dict[str, str]) -> str:
    """``ResultIdentity.version``'s hash, over whatever payload it is handed.

    ⚠ Deliberately NOT a call into ``ResultIdentity`` — the point of this arm is
    to reproduce the hash of the OLD payload shape, which that class can no
    longer express. Keeping the digest here means the check cannot silently
    become a tautology against the current model.
    """
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"{RESULT_SET_ID}+{hashlib.sha256(encoded).hexdigest()[:12]}"


def _load_closes(conn: psycopg.Connection[Any], corpus: Any, started: float) -> dict[int, tuple[int, array[float]]]:
    """One dense close array per instrument, on the shared axis, as the run builds it."""
    closes_by_instrument: dict[int, tuple[int, array[float]]] = {}
    for n, (instrument_id, series_id) in enumerate(corpus.pairs, start=1):
        masked = load_masked_series(conn, series_id, arm="admitted")
        if not masked.bars:
            continue
        series = _to_series(masked.bars)
        indices = [corpus.axis_pos[when] for when in series.dates if when in corpus.axis_pos]
        if len(indices) < 2:
            continue
        first, last = indices[0], indices[-1]
        closes = [float("nan")] * (last - first + 1)
        for when, row in zip(series.dates, series.rows, strict=True):
            slot = corpus.axis_pos.get(when)
            close = row.get("close")
            if slot is not None and close is not None:
                closes[slot - first] = float(close)
        closes_by_instrument[instrument_id] = (first, array("d", closes))
        if n % 500 == 0:
            print(f"  loaded {n}/{len(corpus.pairs)} ({time.monotonic() - started:.0f}s)", flush=True)
    return closes_by_instrument


def _entry_and_exit_notional(book: LegBook, *, allocation: float) -> float:
    """What a non-rebalancing curve must have traded: one buy and one sell per leg."""
    total = 0.0
    for leg in range(len(book)):
        units = allocation / book.entry_price[leg]
        total += allocation + units * book.exit_price[leg]
    return total


def compositions() -> int:
    started = time.monotonic()
    with psycopg.connect(settings.database_url) as conn:
        corpus = load_corpus(conn)
        print(f"axis {len(corpus.axis)} dates {corpus.axis[0]} -> {corpus.axis[-1]}", flush=True)
        print(f"universe {len(corpus.universe)} instruments, {len(corpus.pairs)} series", flush=True)
        closes_by_instrument = _load_closes(conn, corpus, started)
    print(f"closes loaded for {len(closes_by_instrument)} instruments ({time.monotonic() - started:.0f}s)", flush=True)

    lo, hi = 0, len(corpus.axis) - 1
    book = _benchmark_book(
        instruments=frozenset(closes_by_instrument),
        raw_closes_by_instrument=closes_by_instrument,
        wealth_closes_by_instrument=closes_by_instrument,
        lo=lo,
        hi=hi,
    )
    n_legs = len(book)
    date_count = hi - lo + 1
    years = (corpus.axis[hi] - corpus.axis[lo]).days / 365.25
    print(f"\nbenchmark book: {n_legs} legs over {years:.2f} years", flush=True)

    shipped_curve = build_equity_curve(book, date_count=date_count)
    held_curve = build_buy_and_hold_curve(book, date_count=date_count)
    shipped = (float(shipped_curve.equity[-1]) - 1.0) * 100.0
    held = (float(held_curve.equity[-1]) - 1.0) * 100.0

    gross = np.asarray(book.exit_price, dtype=np.float64) / np.asarray(book.entry_price, dtype=np.float64)
    naive_sum = float((gross - 1.0).sum()) * 100.0

    print("\n--- A. as shipped (equal_weight_concurrent_v1, rebalanced on event dates) ---")
    print(f"  total return      {shipped:>22,.2f}%")
    print(f"  CAGR              {(float(shipped_curve.equity[-1]) ** (1.0 / years) - 1.0) * 100.0:>22.3f}%/yr")
    print(f"  event dates       {shipped_curve.event_dates:>22,} of {date_count:,}")
    print(f"  traded notional   {float(shipped_curve.traded_notional.sum()):>22,.2f}x the pot")
    print(f"  rebalance costs   {shipped_curve.rebalance_costs:>22,.4f} of a 1.0 pot")

    print("\n--- B. equal_weight_buy_and_hold_v1 (1/N at entry, held to exit, never rebalanced) ---")
    print(f"  total return      {held:>22,.2f}%")
    print(f"  CAGR              {(float(held_curve.equity[-1]) ** (1.0 / years) - 1.0) * 100.0:>22.3f}%/yr")
    print(f"  event dates       {held_curve.event_dates:>22,} of {date_count:,}")
    print(f"  traded notional   {float(held_curve.traded_notional.sum()):>22,.2f}x the pot")
    print(f"  rebalance costs   {held_curve.rebalance_costs:>22,.4f} of a 1.0 pot")

    print("\n--- C. #2426's premise: the sum of per-instrument hold returns ---")
    print(f"  Sigma return_pct  {naive_sum:>22,.2f}%")
    print(f"  ratio to A        {naive_sum / shipped:>22,.4f}x")

    a_cagr = (float(shipped_curve.equity[-1]) ** (1.0 / years) - 1.0) * 100.0
    b_cagr = (float(held_curve.equity[-1]) ** (1.0 / years) - 1.0) * 100.0
    print(f"\n  >>> rebalancing manufactured {a_cagr - b_cagr:.1f} points of annual return")

    order = np.argsort(gross)[::-1]
    print("\n--- leg census: gross multiple exit/entry ---")
    print(f"  median {float(np.median(gross)):,.4f}  mean {float(gross.mean()):,.2f}  max {float(gross.max()):,.2f}")
    for rank, leg in enumerate(order[:10], start=1):
        idx = int(leg)
        bars = book.exit_index[idx] - book.entry_index[idx] + 1
        print(f"  #{rank:2d} x{gross[idx]:>16,.2f}  entry {book.entry_price[idx]:>13.8f} bars {bars:>6d}")
    print(f"  top-10 legs are {float(gross[order[:10]].sum()) / float(gross.sum()) * 100.0:.2f}% of Sigma(gross)")

    failures: list[str] = []

    # P1 — the benchmark does not rebalance.
    if held_curve.rebalance_costs != 0.0:
        failures.append(f"P1 rebalance_costs is {held_curve.rebalance_costs}, not 0.0")
    expected_notional = _entry_and_exit_notional(book, allocation=1.0 / n_legs)
    actual_notional = float(held_curve.traded_notional.sum())
    if abs(actual_notional - expected_notional) > 1e-6 * max(1.0, expected_notional):
        failures.append(f"P1 traded notional {actual_notional:,.6f} against entries+exits {expected_notional:,.6f}")

    # P2 — no leverage on any date.
    cash = held_curve.equity - held_curve.invested
    if float(cash.min()) < -1e-9:
        failures.append(f"P2 cash reaches {float(cash.min()):.12f} — the benchmark borrowed")

    # P3 — the composition identity, exactly.
    identity = float(gross.mean())
    if abs(float(held_curve.equity[-1]) - identity) > 1e-9 * max(1.0, identity):
        failures.append(f"P3 final equity {float(held_curve.equity[-1]):,.9f} against mean(gross) {identity:,.9f}")

    # P4 — the premise is falsified.
    if abs(naive_sum - shipped) < 1e-6 * abs(shipped):
        failures.append("P4 A equals C — the mechanism WOULD be summation after all")

    print("\n--- assertions ---")
    for name, ok in (
        ("P1 no rebalance (zero cost, notional = entries + exits)", not any(f.startswith("P1") for f in failures)),
        ("P2 cash never negative on any date", not any(f.startswith("P2") for f in failures)),
        ("P3 total return == mean(exit/entry) - 1, exactly", not any(f.startswith("P3") for f in failures)),
        ("P4 the summing premise is falsified (A != C)", not any(f.startswith("P4") for f in failures)),
    ):
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    for failure in failures:
        print(f"  !! {failure}")

    print(f"\ndone in {time.monotonic() - started:.0f}s")
    return 1 if failures else 0


def emit_version_map() -> int:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_IDENTITY_SQL).fetchall()

    failures: list[str] = []
    print(f"-- {len(rows)} stored rows")
    for row in rows:
        (
            result_id,
            strategy_id,
            strategy_version,
            result_scope,
            namespace,
            ambiguity_arm,
            quarantine_arm,
            sizing_rule,
            cost_model_id,
            corpus_version,
            window_start,
            window_end,
            position_rule_set_version,
            outcome_rule_set_version,
            input_rule_set_version,
            stored_version,
        ) = row
        legacy = {
            "strategy_id": str(strategy_id),
            "strategy_version": str(strategy_version),
            "result_scope": str(result_scope),
            "namespace": str(namespace),
            "ambiguity_arm": str(ambiguity_arm),
            "quarantine_arm": str(quarantine_arm),
            "sizing_rule": str(sizing_rule),
            "cost_model_id": str(cost_model_id),
            "corpus_version": str(corpus_version),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "position_rule_set_version": str(position_rule_set_version),
            "outcome_rule_set_version": str(outcome_rule_set_version),
            "input_rule_set_version": str(input_rule_set_version),
        }
        # ⚠⚠ THE GUARD. If the OLD payload does not reproduce the OLD hash, the
        # payload shape here is wrong and every NEW hash it emits is a guess.
        derived = _version(legacy)
        if derived != stored_version:
            failures.append(f"result_id {result_id}: derived {derived} against stored {stored_version}")
            continue
        completed = dict(legacy, benchmark_rule=LEGACY_BENCHMARK_RULE)
        print(f"    ('{stored_version}', '{_version(completed)}'),")

    if failures:
        print("\n!! the legacy payload does not reproduce the stored hash — emitted map is NOT usable")
        for failure in failures:
            print(f"  !! {failure}")
        return 1
    print(f"-- every one of the {len(rows)} old hashes reproduced from its stored columns before emitting")
    return 0


def profile() -> int:
    """Which term dominates ``build_buy_and_hold_curve``? Counted, not argued.

    ⚠ Answers the #2428 review NITPICK, which read the two
    ``[leg for leg in open_legs if leg not in done]`` filters as the hot loop.
    They are not: the mark-and-value loop is ``sum(open_count)`` — one iteration
    per open position per day — while the filters run ONLY on a date where
    something closes. This arm reports the split so the claim is re-measurable
    rather than a number rotting in a docstring.

    ⚠ Synthetic book at full-corpus SHAPE (5,266 legs over a 16,236-date axis),
    not the corpus itself: the question is about the algorithm, and this arm must
    not need a 300-second database read to answer it. Seeded, so it repeats.
    """
    import random

    dates, legs = 16236, 5266
    random.seed(20260808)
    book = LegBook()
    for _ in range(legs):
        entry = random.randint(0, dates - 2)
        exit_ = random.randint(entry + 1, dates - 1)
        span = exit_ - entry + 1
        book.add(
            entry_index=entry,
            exit_index=exit_,
            entry_price=10.0,
            exit_price=12.0,
            half_spread=0.001,
            realised=True,
            marks=[10.0 + i * 0.0001 for i in range(span)],
        )

    started = time.monotonic()
    curve = build_buy_and_hold_curve(book, date_count=dates)
    elapsed = time.monotonic() - started

    opens = [0] * dates
    closes = [0] * dates
    for leg in range(len(book)):
        opens[book.entry_index[leg]] += 1
        closes[book.exit_index[leg]] += 1
    filter_iterations = 0
    open_now = 0
    for day in range(dates):
        open_now += opens[day]
        if closes[day]:
            filter_iterations += open_now
        open_now -= closes[day]
    mark_iterations = int(curve.open_count.sum())
    total = mark_iterations + filter_iterations

    print(f"legs {legs:,}  dates {dates:,}  build {elapsed:.2f}s")
    print(f"  mark/valuation loop iterations : {mark_iterations:>12,}")
    print(f"  open_legs filter iterations    : {filter_iterations:>12,}")
    print(f"  filter share of the two        : {filter_iterations / total * 100:>11.1f}%")
    if filter_iterations >= mark_iterations:
        print("  !! the filter is now the dominant term — the docstring's cost note is stale")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compositions", action="store_true", help="A/B/C over the full population")
    parser.add_argument("--emit-version-map", action="store_true", help="re-derive the stored result versions")
    parser.add_argument("--profile", action="store_true", help="which loop dominates the buy-and-hold curve")
    args = parser.parse_args()
    if not (args.compositions or args.emit_version_map or args.profile):
        parser.error("pick an arm: --compositions, --emit-version-map or --profile")
    status = 0
    if args.compositions:
        status |= compositions()
    if args.emit_version_map:
        status |= emit_version_map()
    if args.profile:
        status |= profile()
    return status


if __name__ == "__main__":
    sys.exit(main())
