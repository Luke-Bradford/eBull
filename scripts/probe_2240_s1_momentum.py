"""Revert-probe the S-1 invariant tests (#2240).

    uv run python scripts/probe_2240_s1_momentum.py

Sister to ``scripts/probe_2240_outcome_ledger.py`` and
``scripts/probe_2240_outcome_resolver.py``, whose two guards apply unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **The declared close input.** ``s1_signals`` declares the bar close as an
input alongside the two averages, and deleting that declaration changes NO
verdict — an ``sma_series`` window always contains its own index, so a NULL
close already makes both averages unevaluable at that bar. The declaration is
there because the strategy READS the close and the guard should not be a
property of a different module; it is **not detectable by any fixture**, so no
probe is written rather than one being contrived that reports ``CAUGHT`` for
something else. Stated here so the gap is a decision, not an oversight.

⚠ **The final bar's** ``no_fill_bar`` **refusal.** It lives in
``strategy_registry.evaluate`` (3a), not here, and is probed with that module.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path("app/services/strategies/s1_time_series_momentum.py")
TESTS = "tests/test_strategy_s1.py"

#: (what the injected defect IS, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        # ⚠ NOT the flat fixture. On a flat series BOTH comparisons are
        # equalities, so relaxing one leaves the conjunction false and the
        # probe reports NOT CAUGHT — measured, and the reason
        # `CLOSE_ON_SLOW` exists.
        "entry's close comparison relaxed to >= (fires with the close ON the average)",
        [
            (
                "        return close > slow_value and fast_value > slow_value",
                "        return close >= slow_value and fast_value > slow_value",
            )
        ],
        "test_a_close_sitting_exactly_on_the_slow_average_does_not_enter",
    ),
    (
        "entry's trend comparison relaxed to >= (fires with the averages EQUAL)",
        [
            (
                "        return close > slow_value and fast_value > slow_value",
                "        return close > slow_value and fast_value >= slow_value",
            )
        ],
        "test_averages_exactly_equal_do_not_enter_however_high_the_close",
    ),
    (
        # §4's rationale for the second conjunct is the whole strategy: without
        # it S-1 is "buy anything above its 200-day", not a trend follower.
        # ⚠ Only detectable on bars where `sma_50 <= sma_200` while the close is
        # above it — which is why the fixture asserts that quadrant exists.
        "entry drops the sma_50 > sma_200 trend filter",
        [
            (
                "        return close > slow_value and fast_value > slow_value",
                "        return close > slow_value",
            )
        ],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        "exit's comparison relaxed to <= (exits on a flat market)",
        [("        return close < fast_value", "        return close <= fast_value")],
        "test_a_flat_series_fires_neither_leg",
    ),
    (
        # The judgement call in the module: both legs declare the same inputs,
        # so the strategy has ONE warm-up. Per-leg declarations make the same
        # bar live for the exit and warming for the entry.
        "the exit leg declares only its own inputs (per-leg evaluability)",
        [
            (
                '        *evaluate(exit_, inputs=inputs, n_bars=n_bars, kind="exit"),',
                '        *evaluate(exit_, inputs=inputs[:2], n_bars=n_bars, kind="exit"),',
            )
        ],
        "test_both_legs_share_the_slow_warm_up",
    ),
    (
        # `signal_kind` is in the ledger's uniqueness key precisely so the two
        # legs coexist on one bar. Emitting both as entries collides.
        "both legs emitted under the entry kind",
        [('n_bars=n_bars, kind="exit"),', 'n_bars=n_bars, kind="entry"),')],
        "test_both_legs_coexist_on_one_bar",
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
        "a lookback dropped from the hashed parameters",
        [
            (
                'S1_PARAMS: Mapping[str, object] = {"fast_period": FAST_PERIOD, "slow_period": SLOW_PERIOD}',
                'S1_PARAMS: Mapping[str, object] = {"fast_period": FAST_PERIOD}',
            )
        ],
        "test_the_identity_carries_the_two_lookbacks_and_nothing_else",
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
            SRC.write_text(mutated)
            rc = run(selector)
            SRC.write_text(original)
            verdict = "CAUGHT" if rc != 0 else "*** NOT CAUGHT ***"
            print(f"  {verdict:<20} {name}", flush=True)
            if rc == 0:
                failures.append(name)
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
