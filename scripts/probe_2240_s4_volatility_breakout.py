"""Revert-probe the S-4 invariant tests (#2240).

    uv run python scripts/probe_2240_s4_volatility_breakout.py

Sister to ``scripts/probe_2240_s1_momentum.py``, whose two guards apply unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **Three of the four declared inputs are individually redundant**, and no probe
is written for them rather than one being contrived that reports ``CAUGHT`` for
something else. ``s4_signals`` declares close, ATR, compression and prior-high;
deleting any of the first, second or fourth changes NO verdict, because
``atr_series`` already refuses on a NULL close, and ``compression_rank_series``
already refuses on every ATR refusal — while the prior-high window's refusals are
a strict subset of the tail ATR refuses anyway. They are declared because §3.1
makes evaluability a property of the STRATEGY and an undeclared input is a guard
that exists only as a property of a different module, which is exactly the
reasoning S-1 records for its own close declaration. The COMPRESSION input is not
redundant (it carries the 113-bar warm-up that ATR alone does not) and IS probed.

⚠ **The final bar's** ``no_fill_bar`` **refusal** lives in
``strategy_registry.evaluate`` (3a), not here, and is probed with that module.

⚠ **The exit bracket.** ``ATR_STOP_MULTIPLE`` / ``ATR_TARGET_MULTIPLE`` /
``MAX_HOLD_BARS`` are not read by any code path in this module — S-4 has no exit
leg (see its docstring), and their consumer is ``outcome_resolver.ExitLevels``.
So the only thing that can hold them to the rule is the identity hash, and the
probe that matters is the one that drops one from ``S4_PARAMS``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# ⚠ The gate constants live ONCE, in the 5b reference harness. A second
# hand-written copy of "exit 1 means the test failed" is how this file's
# gate drifted from it in the first place (#2357).
from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED

SRC = Path("app/services/strategies/s4_volatility_compression_breakout.py")
TESTS = "tests/test_strategy_s4.py"

CONDITION = "        return rank < COMPRESSION_QUANTILE and close > highest_prior_close"

#: (what the injected defect IS, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        # ⚠ NOT the flat fixture, and not the rising one. Both are degenerate on
        # BOTH conjuncts at once, so relaxing either leaves the other false and
        # the probe reports NOT CAUGHT — the S-3 lesson, already in the
        # prevention log. Each operator gets its own exact-equality fixture.
        "setup leg relaxed to <= (fires with the ATR exactly ON the quartile)",
        [(CONDITION, "        return rank <= COMPRESSION_QUANTILE and close > highest_prior_close")],
        "test_a_compression_rank_exactly_on_the_quartile_does_not_fire",
    ),
    (
        "breakout leg relaxed to >= (fires with the close exactly ON the prior high)",
        [(CONDITION, "        return rank < COMPRESSION_QUANTILE and close >= highest_prior_close")],
        "test_a_close_exactly_on_the_prior_high_does_not_fire",
    ),
    (
        # Without the setup leg S-4 is "buy any 20-bar breakout", not a
        # compression strategy. Only detectable on bars where the breakout leg
        # is true and the setup leg is false — the quadrant the cycle fixture
        # asserts it covers.
        "the compression setup leg dropped entirely",
        [(CONDITION, "        return close > highest_prior_close")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        "the breakout leg dropped entirely",
        [(CONDITION, "        return rank < COMPRESSION_QUANTILE")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        # §4: *"computed on bars <= t"*. Dropping `t` from its own window also
        # drops the divisor to 99, so a new ATR high scores 1.0 instead of 0.99.
        "the compression window excludes the ranked bar",
        [("        window = atr.values[low : index + 1]", "        window = atr.values[low:index]")],
        "test_the_compression_rank_can_never_reach_one or test_the_window_includes_the_ranked_bar",
    ),
    (
        # §4's own parenthesis: *"including it makes the condition partly
        # self-referential"*. Under a strict `>` the rule then never fires at
        # all — it fails silently rather than loudly, which is why it is probed.
        "the breakout window includes the signal bar",
        [
            (
                "        window = closes[low:index]  # ⚠ excludes `index` itself.",
                "        window = closes[low : index + 1]",
            )
        ],
        "test_the_breakout_window_excludes_the_signal_bar",
    ),
    (
        # ⚠ Tie handling is FORCED, not chosen: counting `<=` makes two bars with
        # identical ATRs in one window rank differently by position.
        "the rank counts ties as below (<= instead of <)",
        [
            (
                "        below = sum(1 for value in window if value is not None and value < current)",
                "        below = sum(1 for value in window if value is not None and value <= current)",
            )
        ],
        "test_a_constant_atr_ramp_fires_every_warm_bar",
    ),
    (
        # The one input declaration that is NOT redundant — it carries the
        # 113-bar warm-up that the raw ATR (warm at 14) does not.
        "the compression input dropped from the declared set",
        [
            (
                "        StrategyInput(series=compression, reason=masked_reason),\n",
                "",
            )
        ],
        "test_the_first_evaluable_bar_is_the_warm_up_boundary",
    ),
    (
        "the ATR period changed away from Wilder's 14",
        [("ATR_PERIOD = 14", "ATR_PERIOD = 20")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        "the compression window shortened from §4's 100 bars",
        [("COMPRESSION_WINDOW = 100", "COMPRESSION_WINDOW = 50")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        "the breakout lookback shortened from §4's 20 bars",
        [("BREAKOUT_LOOKBACK = 20", "BREAKOUT_LOOKBACK = 10")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        # S-4 has no exit leg and cannot have one; emitting under the exit kind
        # would put entry decisions on the ledger's exit key.
        "the single leg emitted under the exit kind",
        [('n_bars=len(series), kind="entry")', 'n_bars=len(series), kind="exit")')],
        "test_every_signal_is_an_entry",
    ),
    (
        "a blank cost-model declaration accepted into the identity hash",
        [("    if not cost_model_id.strip():", "    if False:")],
        "test_a_blank_cost_model_id_is_rejected",
    ),
    (
        # Criterion 11 requires identity to cover CODE. A constant hash means an
        # edited rule inherits the prior track record.
        "the source hash frozen to a constant",
        [
            (
                "    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]",
                '    return "0" * 12',
            )
        ],
        "test_the_source_hash_is_this_strategys_own_source",
    ),
    (
        "the compression window dropped from the hashed parameters",
        [('    "compression_window": COMPRESSION_WINDOW,\n', "")],
        "test_the_identity_carries_every_parameter_of_the_rule",
    ),
    (
        # ⚠ THE PROBE THAT MATTERS MOST ON THIS MODULE. Nothing in the code reads
        # the stop multiple, so nothing except the identity hash would notice it
        # drifting away from §4's rule — S-3's MAX_HOLD_BARS lesson, applied to a
        # whole exit bracket rather than half of one.
        "the ATR stop multiple dropped from the hashed parameters",
        [('    "atr_stop_multiple": ATR_STOP_MULTIPLE,\n', "")],
        "test_the_identity_carries_every_parameter_of_the_rule",
    ),
]


def run(selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def main() -> int:
    original = SRC.read_text()
    failures: list[str] = []
    try:
        for name, edits, selector in PROBES:
            mutated = original
            bad_anchor = False
            for old, new in edits:
                count = mutated.count(old)
                if count != 1:
                    failures.append(f"{name}: anchor occurs {count} times, expected exactly 1 — probe proves nothing")
                    bad_anchor = True
                    break
                mutated = mutated.replace(old, new)
            if bad_anchor:
                continue
            # ⚠⚠ BASELINE FIRST — assert the selected test PASSES on unmutated
            # source before mutating anything. Without it a probe cannot tell
            # "the mutation broke the test" from "the test was already broken",
            # and the second reads as CAUGHT (prevention log, #2214).
            rc_baseline = run(selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<20} {name}  (exit {rc_baseline})", flush=True)
                continue
            SRC.write_text(mutated)
            rc = run(selector)
            SRC.write_text(original)
            if rc == PYTEST_TEST_FAILED:
                verdict = "CAUGHT"
            elif rc == PYTEST_PASSED:
                verdict = "*** NOT CAUGHT ***"
                failures.append(name)
            else:
                # ⚠⚠ NOT a catch. 2/3/4/5 are interrupted / internal error /
                # usage error / no tests collected — a mutation that leaves the
                # source unparseable exits 4 and was never evaluated.
                verdict = f"*** HARNESS FAULT {rc} ***"
                failures.append(f"{name}: pytest exit {rc} is not a test result — probe proves nothing")
            print(f"  {verdict:<20} {name}", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        SRC.write_text(original)

    rc_suite = run("test_")
    print(f"\n  restored suite: {'PASS' if rc_suite == 0 else '*** FAIL ***'}", flush=True)
    if rc_suite:
        failures.append("restored suite does not pass")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
