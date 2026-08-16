"""Revert-probe the phase-5d equity-curve and metric-set invariant tests (#2240).

    uv run python scripts/probe_2240_statistics.py

Sister to ``scripts/probe_2240_cost_model.py`` and
``scripts/probe_2240_result_model.py``, whose five guards apply unchanged and
whose strict runner is IMPORTED rather than copied a fifth time (#2357 owns the
shared extraction):

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is not guarded by either.** ``NOT CAUGHT`` has three causes
   and the triage order is selector → fixture → code.
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** A syntax break in the
   injected source exits 4 and would read as a catch.
5. ⚠ **Run a BASELINE first**, so "the mutation broke it" and "it was already
   broken" are distinguishable.

⚠ TWO SOURCE FILES: ``equity_curve`` is the engine and ``strategy_statistics``
the metrics over it. The harness restores both regardless of which one a probe
touched.

⚠ NOT A TEST, and it must never become one: it mutates tracked source files on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2240_statistics.py``. Phase
4b's lesson: a concurrent run stamps its output with the INJECTED source, and a
start-vs-end check misses it because the probe restores the file.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``_MIN_ALLOCATION``.** It is the zero floor below which an allocation is not
worth a division, and reverting it to a negative number produces no behaviour
change on any reachable input — an allocation is ``min(target, cash)`` with both
non-negative.

⚠ **The ``EquityCurve.__post_init__`` length check.** It guards a shape only
``build_equity_curve`` constructs, and every array there is allocated from the
same ``date_count``; there is no reachable path that builds a ragged one. The
test file constructs one by hand, which is what keeps the guard honest.

⚠ **``METRIC_SET_ID``'s value.** Nothing branches on it; it is stamped and
reported. ``sql/263``'s ``NOT NULL`` + non-empty CHECK is what keeps it present.

⚠⚠ **THE SEEDED ``effective_sample_size`` BRANCH** (#2695, Codex checkpoint).
The probe below covers the no-seed half only. Substituting the nominal trade
count in the SEEDED half is a criterion 3 violation with no test that observes
it: ``test_a_declared_seed_fills_the_sample_size_and_the_interval`` asserts
``is not None`` and ``> 0.0``, and a nominal *n* satisfies both. Closing it
needs an assertion that discriminates an effective *n* from a nominal one —
``effective_sample_size < trade_count`` is the obvious candidate and is NOT
universally true of a block bootstrap, so it wants deriving rather than
assuming. Stated here rather than probed, because a probe over a test that
cannot fail would report ``NOT CAUGHT`` for a real defect and read as noise.
"""

from __future__ import annotations

import sys
from pathlib import Path

# ⚠⚠ The docstring above invokes this file by PATH, which puts ``scripts/`` on
# sys.path and NOT the repo root — so the cross-script import below raises
# ModuleNotFoundError under the exact command this file documents. Prepending
# the root makes both that form and ``-m scripts.<name>`` work (#2357/#2695).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

CURVE = Path("app/services/equity_curve.py")
STATS = Path("app/services/strategy_statistics.py")
SOURCES = (CURVE, STATS)

CURVE_TESTS = "tests/test_equity_curve.py"
STATS_TESTS = "tests/test_strategy_statistics.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        "the sizing rule id renamed without the rule moving",
        CURVE,
        CURVE_TESTS,
        [('SIZING_RULE_ID: Final = "equal_weight_concurrent_v1"', 'SIZING_RULE_ID: Final = "equal_weight_v2"')],
        "test_the_sizing_rule_id_is_the_declared_one",
    ),
    (
        # ⚠⚠ §3.2 RULE 4 INVERTED. Entries before exits funds a new position out
        # of cash a same-day exit has not released, which shows up as a spurious
        # short-funded entry rather than as an error.
        "entries processed BEFORE same-date exits (§3.2 rule 4 inverted)",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        closed_today = [leg for leg in closing_now if realised[leg] and int(entry_index[leg]) < day]",
                "        closed_today = []",
            )
        ],
        "test_an_exit_frees_cash_before_a_same_date_entry_uses_it",
    ),
    (
        # ⚠⚠ THE SAME-BAR SPLIT. Applying "exit before entry" to a leg's OWN
        # exit closes it before it opens, so it never leaves the open set — and
        # then reads marks past the end of its own array.
        "the same-bar split removed (a bars_held=0 leg closed before it opened)",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        same_bar = [leg for leg in closing_now if realised[leg] and int(entry_index[leg]) == day]",
                "        same_bar = []",
            )
        ],
        "test_a_same_bar_leg_is_legal",
    ),
    (
        # ⚠⚠ THE CODEX CHECKPOINT-2 DEFECT, injected. Bucketing an unrealised
        # leg with the realised closes liquidates it at its mark bar: it leaves
        # `open_count` and `invested` for the rest of the window, and its
        # notional lands in cash where it can fund a same-date entry.
        "an unrealised leg treated as an EXIT instead of frozen at its mark",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        closed_today = [leg for leg in closing_now if realised[leg] and int(entry_index[leg]) "
                "< day]\n"
                "        same_bar = [leg for leg in closing_now if realised[leg] and int(entry_index[leg]) == day]\n"
                "        freezing_today = [leg for leg in closing_now if not realised[leg]]",
                "        closed_today = [leg for leg in closing_now if int(entry_index[leg]) < day]\n"
                "        same_bar = [leg for leg in closing_now if int(entry_index[leg]) == day]\n"
                "        freezing_today = []",
            )
        ],
        "test_its_notional_CANNOT_fund_a_same_date_entry",
    ),
    (
        # ⚠ A frozen leg cannot be traded, so equalising the WHOLE concurrent
        # set sets a target it can never move to and forces every tradeable leg
        # to absorb the shortfall — a different sizing rule wearing the declared
        # rule's id.
        "the rebalance target computed over frozen legs it cannot trade",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        tradeable = [leg for leg in open_legs if leg not in frozen]",
                "        tradeable = list(open_legs)",
            )
        ],
        "test_a_frozen_leg_is_NOT_traded_by_a_later_rebalance",
    ),
    (
        # ⚠⚠ LEVERAGE. Spending the full desired amount rather than what is on
        # hand takes cash negative — arithmetically small and forbidden outright
        # by the project posture.
        "the rebalance buy no longer capped by cash (leverage)",
        CURVE,
        CURVE_TESTS,
        [("                spend = min(wanted, cash / (1.0 + half_spread[leg]))", "                spend = wanted")],
        "test_cash_never_goes_negative",
    ),
    (
        # ⚠⚠ §5.4's "rebalanced ONLY on position open/close". Rebalancing every
        # bar is a different — and much busier — strategy, and it charges
        # turnover the declared rule never incurs.
        # ⚠ RE-ANCHORED (#2695). The predicate is now `rebalance_now`, which is
        # `(rebalance_events and event) or day in scheduled_rebalance_indices` —
        # the month-end arm added a second legitimate trigger. The DEFECT is
        # unchanged and still spans both arms: dropping the predicate entirely
        # rebalances on every bar under either configuration.
        "the rebalance made unconditional (every bar, not only on an open/close)",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        if rebalance_now and tradeable:\n            held = 0.0",
                "        if tradeable:\n            held = 0.0",
            )
        ],
        "test_weights_DRIFT_between_event_dates_and_are_not_restored_daily",
    ),
    (
        "the rebalance sell side charging nothing (a free trade)",
        CURVE,
        CURVE_TESTS,
        [("                    charge = sold * half_spread[leg]", "                    charge = 0.0")],
        "test_BOTH_sides_of_a_rebalance_are_charged",
    ),
    (
        # ⚠ §3.3's halt. Overwriting the mark with the `nan` fabricates a
        # valuation of nothing, and every arithmetic downstream becomes NaN
        # silently.
        # ⚠⚠ RE-ANCHORED TO THE SHARED/FLAT LOOKUP JOIN (#2772). The carry
        # decision remains common to both strategy representations, while
        # `build_buy_and_hold_curve` has its own copy and probe below. Anchoring
        # on the representation join makes this mutation hit the production
        # strategy walker exactly once rather than one storage branch.
        # A mark-carry defect in the BENCHMARK moves
        # `return_vs_buy_and_hold_pct` on every result just as surely.
        "a missing bar overwriting the mark with NaN instead of carrying it forward",
        CURVE,
        CURVE_TESTS,
        [
            (
                "            if marks_by_source:\n"
                "                source = int(mark_source[leg])\n"
                "                offset = day - int(marks_first_by_source[source])\n"
                "                mark = marks_by_source[source][offset]\n"
                "            else:\n"
                "                offset = int(mark_offset[leg]) + (day - int(entry_index[leg]))\n"
                "                mark = marks[offset]\n"
                "            if np.isnan(mark):\n"
                "                stale_marks += 1\n"
                "            else:\n"
                "                last_price[leg] = mark",
                "            if marks_by_source:\n"
                "                source = int(mark_source[leg])\n"
                "                offset = day - int(marks_first_by_source[source])\n"
                "                mark = marks_by_source[source][offset]\n"
                "            else:\n"
                "                offset = int(mark_offset[leg]) + (day - int(entry_index[leg]))\n"
                "                mark = marks[offset]\n"
                "            last_price[leg] = mark",
            )
        ],
        "test_a_missing_bar_carries_the_previous_mark_forward_and_is_counted",
    ),
    (
        # ⚠⚠ CLASS 2 (#2695): the branch no probe named. `build_buy_and_hold_curve`
        # carries its own copy of §3.3's halt handling, and it had no probe at
        # all — the duplicate was only visible because the STRATEGY curve's
        # anchor started matching twice. A NaN mark overwriting the benchmark's
        # last price makes `buy_and_hold_return_pct` NaN, which propagates into
        # `return_vs_buy_and_hold_pct` on every result computed against it.
        "a missing bar overwriting the BENCHMARK's mark with NaN (build_buy_and_hold_curve)",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        for leg in open_legs:\n"
                "            offset = int(mark_offset[leg]) + (day - int(entry_index[leg]))\n"
                "            mark = marks[offset]\n"
                "            if np.isnan(mark):\n"
                "                stale_marks += 1\n"
                "            else:\n"
                "                last_price[leg] = mark",
                "        for leg in open_legs:\n"
                "            offset = int(mark_offset[leg]) + (day - int(entry_index[leg]))\n"
                "            mark = marks[offset]\n"
                "            last_price[leg] = mark",
            )
        ],
        "test_a_halt_carries_the_previous_mark_and_is_counted",
    ),
    (
        # ⚠⚠ THE BASKET DENOMINATOR. Sizing today's entries one at a time gives
        # the first 100% of a flat pot and reports every sibling as
        # short-funded — a narrowing that never happened, landing in criterion
        # 9's census.
        "today's entries sized one at a time instead of against the whole basket",
        CURVE,
        CURVE_TESTS,
        [("        basket = len(open_legs) + len(opened_today)", "        basket = len(open_legs) + 1")],
        "test_entries_on_the_SAME_date_are_sized_against_the_whole_basket",
    ),
    (
        # ⚠ Without it a leg reads past the end of its own mark slice into the
        # NEXT leg's — a valuation off another instrument's prices, which no
        # aggregate reveals.
        "the mark-array length check removed",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        if len(marks) != span:\n"
                "            raise ValueError(\n"
                '                f"leg spanning indices {entry_index}..{exit_index} needs {span} marks, got '
                '{len(marks)} — a short "\n'
                '                "mark array would silently shorten the hold"\n'
                "            )\n",
                "",
            )
        ],
        "test_a_short_mark_array_is_refused_rather_than_silently_shortening_the_hold",
    ),
    (
        "the reversed-index refusal removed",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        if exit_index < entry_index:\n"
                '            raise ValueError(f"leg closes at index {exit_index} before it opens at {entry_index}")\n',
                "",
            )
        ],
        "test_a_close_before_its_open_is_refused",
    ),
    (
        "the short-axis refusal removed (the tail of the curve silently truncated)",
        CURVE,
        CURVE_TESTS,
        [
            (
                "    if n_legs and int(exit_index.max()) >= date_count:\n"
                "        raise ValueError(\n"
                '            f"a leg closes at index {int(exit_index.max())} on a {date_count}-date axis — the axis '
                'is short, and "\n'
                '            "silently truncating it would drop the tail of the curve"\n'
                "        )\n",
                "",
            )
        ],
        "test_a_leg_past_the_end_of_the_axis_is_refused",
    ),
    (
        "the non-positive-price refusal removed",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        if entry_price <= 0.0 or exit_price <= 0.0:\n"
                '            raise ValueError(f"leg prices must be positive, got entry {entry_price} exit '
                '{exit_price}")\n',
                "",
            )
        ],
        "test_a_non_positive_price_is_refused",
    ),
    (
        "the negative-half-spread refusal removed (a cost that IMPROVES a trade)",
        CURVE,
        CURVE_TESTS,
        [
            (
                "        if half_spread < 0.0:\n"
                '            raise ValueError(f"half_spread must be non-negative, got {half_spread}")\n',
                "",
            )
        ],
        "test_a_negative_half_spread_is_refused",
    ),
    # --- strategy_statistics ------------------------------------------------
    (
        # ⚠ A two-date axis spans ONE interval. Counting endpoints annualises it
        # as if it held two years of observations.
        "the annualisation counting endpoints instead of intervals",
        STATS,
        STATS_TESTS,
        [
            (
                "    return (len(dates) - 1) / (span_days / DAYS_PER_YEAR)",
                "    return len(dates) / (span_days / DAYS_PER_YEAR)",
            )
        ],
        "test_it_is_derived_from_the_axis_and_is_not_252",
    ),
    (
        "the year shortened to 365 days (the leap-day drift back on the CAGR exponent)",
        STATS,
        STATS_TESTS,
        [("DAYS_PER_YEAR: Final = 365.25", "DAYS_PER_YEAR: Final = 365.0")],
        "test_the_year_is_gregorian_not_365",
    ),
    (
        # ⚠ The POPULATION standard deviation, which inflates Sharpe on a short
        # series and converges on a long one — so a fixture would not see it and
        # a corpus run would not either.
        "the volatility computed with ddof=0 instead of the sample deviation",
        STATS,
        STATS_TESTS,
        [("        volatility = float(np.std(returns, ddof=1))", "        volatility = float(np.std(returns))")],
        "test_sharpe_matches_the_stdlib_formula_with_a_zero_risk_free_rate",
    ),
    (
        # ⚠⚠ THE ONE THAT IS ROUTINELY CONFUSED. Dividing by the LOSING count is
        # a different statistic that rewards a strategy for rarely losing twice
        # over, and it is always the more flattering of the two.
        "the downside deviation divided by the LOSING periods instead of all of them",
        STATS,
        STATS_TESTS,
        [
            (
                "        downside_deviation = float(math.sqrt(float(np.sum(downside**2)) / len(returns)))",
                "        downside_deviation = float(math.sqrt(float(np.sum(downside**2)) / len(downside)))",
            )
        ],
        "test_sortino_divides_by_ALL_periods_not_just_the_losing_ones",
    ),
    (
        # ⚠ From the START rather than from the running peak. A path that
        # finishes above where it started reports NO drawdown at all.
        "the drawdown measured from the start instead of the running peak",
        STATS,
        STATS_TESTS,
        [("    peak = np.maximum.accumulate(equity)", "    peak = np.full_like(equity, equity[0])")],
        "test_it_is_the_deepest_fall_from_a_RUNNING_peak",
    ),
    (
        "the turnover no longer halved into round trips (twice the trading reported)",
        STATS,
        STATS_TESTS,
        [
            (
                "    turnover = (traded / 2.0 / mean_equity / years) if mean_equity > 0.0 and years > 0.0 else 0.0",
                "    turnover = (traded / mean_equity / years) if mean_equity > 0.0 and years > 0.0 else 0.0",
            )
        ],
        "test_turnover_halves_the_traded_notional_into_round_trips",
    ),
    (
        # ⚠⚠ THE NOMINAL n. Criterion 3 says "no bare percentage and no nominal
        # n is reported anywhere", so this fills the field with the exact number
        # the criterion forbids — wearing the name of the one it requires, and
        # clearing the promotion gate's refusal while it does.
        # ⚠ RE-ANCHORED AND RENAMED (#2695), because the old name outlived its
        # invariant. Stage 5e's block bootstrap now fills the field when a
        # `bootstrap_seed` is declared, so the shipped rule is no longer "always
        # null" — it is "null unless a bootstrap ran", and this probe proves only
        # the NO-SEED half. Crediting it with criterion 3's full "no nominal n is
        # reported anywhere" would be exactly the stale claim #2695 is about.
        #
        # ⚠⚠ The injected defect is BROADER than the observation: `float(
        # trade_count) or None` corrupts the seeded branch too, and nothing
        # catches that — `test_a_declared_seed_fills_the_sample_size_and_the
        # _interval` asserts only `is not None` and `> 0.0`, both of which the
        # nominal count satisfies. Recorded in WHAT IS NOT PROBED above rather
        # than papered over with a second anchor; closing it needs a test that
        # discriminates an effective n from a nominal one, which does not exist.
        "the no-bootstrap effective sample size filled with the nominal trade count",
        STATS,
        STATS_TESTS,
        [
            (
                "        effective_sample_size=bootstrap.effective_sample_size if bootstrap else None,",
                "        effective_sample_size=float(trade_count) or None,",
            )
        ],
        "test_the_effective_sample_size_is_NULL_WITHOUT_A_DECLARED_SEED",
    ),
    (
        # ⚠ §5.4: exposure is invested capital-days over ALLOCATED capital-days.
        # Dividing by the invested days makes every sleeve 100% exposed.
        "exposure divided by the INVESTED days instead of the allocated pot",
        STATS,
        STATS_TESTS,
        [
            (
                "    exposure = (invested_capital_days / allocated_capital_days * 100.0) "
                "if allocated_capital_days > 0.0 else 0.0",
                "    exposure = (invested_capital_days / invested_capital_days * 100.0) "
                "if invested_capital_days > 0.0 else 0.0",
            )
        ],
        "test_exposure_is_capital_days_and_NOT_a_bar_count",
    ),
    (
        # ⚠ A profit factor of 0.0 where the denominator was empty is a number
        # standing in for "there was nothing to divide by" — the state
        # ``StrategyMetrics`` refuses and ``sql/263`` CHECKs.
        "a zero profit factor standing in for an empty denominator",
        STATS,
        STATS_TESTS,
        [
            (
                "    profit_factor = (gains / losses) if losses > 0.0 else None",
                "    profit_factor = (gains / losses) if losses > 0.0 else 0.0",
            )
        ],
        "test_profit_factor_is_NULL_exactly_when_there_is_no_losing_trade",
    ),
    (
        "the drawdown sign guard removed (a positive drawdown reads as a good result)",
        STATS,
        STATS_TESTS,
        [
            (
                "        if self.max_drawdown_pct > 0.0:\n"
                "            raise ValueError(\n"
                '                f"max_drawdown_pct {self.max_drawdown_pct} is positive — a drawdown is a fall from '
                'a running peak "\n'
                '                "and is reported as a non-positive number, so a sign flip cannot read as a good '
                'result"\n'
                "            )\n",
                "",
            )
        ],
        "test_a_positive_drawdown_is_refused",
    ),
    (
        "the profit-factor / losing-count consistency guard removed",
        STATS,
        STATS_TESTS,
        [
            (
                "        if (self.profit_factor is None) != (self.losing_trade_count == 0):\n"
                "            raise ValueError(\n"
                '                f"profit_factor {self.profit_factor!r} against {self.losing_trade_count} losing '
                'trades: it is null "\n'
                "                \"exactly when the denominator is empty, and never as a stand-in for 'not "
                "computed'\"\n"
                "            )\n",
                "",
            )
        ],
        "test_a_null_profit_factor_with_losing_trades_is_refused",
    ),
    (
        # ⚠⚠ RETARGETED AFTER A `NOT CAUGHT`. The original probe removed a
        # `final_equity <= 0` special case and the test still passed — correctly,
        # because `0.0 ** x == 0.0` in Python, so the general formula already
        # returns -100% and the branch was dead code. The reachable failure is
        # the NEGATIVE base, which returns a COMPLEX number.
        "the negative-final-equity refusal removed (CAGR returns a complex number)",
        STATS,
        STATS_TESTS,
        [
            (
                "    if final_equity < 0.0:\n"
                "        raise ValueError(\n"
                '            f"final equity {final_equity} is negative — a negative base under a fractional '
                'exponent returns a "\n'
                '            "COMPLEX number, and the sleeve cannot borrow (equity_curve caps every buy at cash '
                'on hand)"\n'
                "        )\n",
                "",
            )
        ],
        "test_a_NEGATIVE_final_equity_raises_rather_than_returning_a_COMPLEX_number",
    ),
    (
        "the benchmark exact-date check removed (equal-length windows subtracted as one)",
        STATS,
        STATS_TESTS,
        [
            (
                "        if buy_and_hold.dates != dates:\n"
                "            raise ValueError(\n"
                '                "benchmark dates differ from the strategy dates — equal curve lengths do not prove '
                'the same "\n'
                '                "measurement window"\n'
                "            )\n",
                "",
            )
        ],
        "test_equal_length_curves_on_different_dates_are_refused",
    ),
]


def main() -> int:
    originals = {source: source.read_text() for source in SOURCES}
    failures: list[str] = []
    try:
        for name, source, tests, edits, selector in PROBES:
            count = selected(tests, selector)
            if count == 0:
                failures.append(f"{name}: selector {selector!r} names no test — probe proves nothing")
                print(f"  {'*** NO SUCH TEST ***':<24} {name}", flush=True)
                continue
            mutated = originals[source]
            bad_anchor = False
            for old, new in edits:
                occurrences = mutated.count(old)
                if occurrences != 1:
                    failures.append(
                        f"{name}: anchor occurs {occurrences} times, expected exactly 1 — probe proves nothing"
                    )
                    bad_anchor = True
                    break
                mutated = mutated.replace(old, new)
            if bad_anchor:
                print(f"  {'*** BAD ANCHOR ***':<24} {name}", flush=True)
                continue
            rc_baseline = run([tests], selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<24} {name}  (exit {rc_baseline})", flush=True)
                continue
            source.write_text(mutated)
            rc = run([tests], selector)
            source.write_text(originals[source])
            if rc == PYTEST_TEST_FAILED:
                verdict = "CAUGHT"
            elif rc == PYTEST_PASSED:
                verdict = "*** NOT CAUGHT ***"
                failures.append(name)
            else:
                verdict = f"*** HARNESS FAULT {rc} ***"
                failures.append(f"{name}: pytest exit {rc} is not a test result — the mutation was never evaluated")
            print(f"  {verdict:<24} {name}  ({count} test{'' if count == 1 else 's'})", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        for source, text in originals.items():
            source.write_text(text)

    rc_suite = run([CURVE_TESTS, STATS_TESTS], "test_")
    suite = "PASS" if rc_suite == PYTEST_PASSED else f"*** FAIL (exit {rc_suite}) ***"
    print(f"\n  restored suite: {suite}", flush=True)
    if rc_suite != PYTEST_PASSED:
        failures.append(f"restored suite exits {rc_suite}")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
