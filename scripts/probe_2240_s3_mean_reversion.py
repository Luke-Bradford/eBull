"""Revert-probe the S-3 invariant tests (#2240).

    uv run python scripts/probe_2240_s3_mean_reversion.py

Sister to ``scripts/probe_2240_s1_momentum.py``, whose two guards apply
unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

⚠ THIS HARNESS MUTATES A TRACKED SOURCE FILE, so it must not run concurrently
with anything that reads it — including a full-population sweep of this same
strategy. Phase 4b's lesson: a concurrent run stamps its rows with the INJECTED
source hash, and a start-vs-end check misses it because the probe restores the
file. Run the probes and the ``--census`` sweep one after the other, never at
once.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **The declared close input.** ``s3_signals`` declares the bar close as an
input alongside the RSI and the trend average, and deleting that declaration
changes NO verdict — an ``sma_series`` window always contains its own index and
``rsi_series`` refuses everything from the first NULL onward, so a missing close
is already unevaluable at that bar through both. It is **not detectable by any
fixture**, so no probe is written rather than one being contrived that reports
``CAUGHT`` for something else. Stated here so the gap is a decision, not an
oversight. (Same gap S-1 records, for the same reason.)

⚠ **The final bar's** ``no_fill_bar`` **refusal** lives in
``strategy_registry.evaluate`` (3a) and is probed with that module.

⚠ **``MAX_HOLD_BARS`` is not probed as a RULE**, because this module does not
evaluate it — §4's *"or 10 bars elapsed"* is position state and is enforced by
``outcome_resolver``. What IS probed is that it cannot fall out of the identity
hash, which is the failure mode available here.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path("app/services/strategies/s3_mean_reversion_in_trend.py")
TESTS = "tests/test_strategy_s3.py"

#: (what the injected defect IS, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        # ⚠ NOT the flat fixture, and not `FALLING` either. A flat series makes
        # the RSI exactly 50 and the close exactly equal to the average — so
        # relaxing EITHER entry comparison leaves the conjunction false, and the
        # probe reports NOT CAUGHT. `RSI_ON_THRESHOLD` puts this one comparison
        # exactly on its boundary while the other is strictly true.
        "entry's oversold comparison relaxed to <= (fires with RSI exactly 30)",
        [
            (
                "        return rsi_value < OVERSOLD_THRESHOLD and close > trend_value",
                "        return rsi_value <= OVERSOLD_THRESHOLD and close > trend_value",
            )
        ],
        "test_an_rsi_of_exactly_thirty_does_not_enter",
    ),
    (
        "entry's trend comparison relaxed to >= (fires with the close ON the average)",
        [
            (
                "        return rsi_value < OVERSOLD_THRESHOLD and close > trend_value",
                "        return rsi_value < OVERSOLD_THRESHOLD and close >= trend_value",
            )
        ],
        "test_a_close_sitting_exactly_on_the_trend_average_does_not_enter",
    ),
    (
        # §4's rationale for the second conjunct is the whole strategy: without
        # it S-3 is "buy anything oversold", i.e. the falling knife the trend
        # filter exists to refuse. `FALLING` is RSI 0 on every bar with the close
        # below its average throughout, so a dropped filter fires the lot.
        "entry drops the close > sma_200 trend filter (the falling-knife guard)",
        [
            (
                "        return rsi_value < OVERSOLD_THRESHOLD and close > trend_value",
                "        return rsi_value < OVERSOLD_THRESHOLD",
            )
        ],
        "test_a_falling_ramp_fires_neither_leg",
    ),
    (
        # ⚠ The MIRROR of the probe above: dropping the OTHER conjunct. A single
        # fixture cannot see both, and a dropped-conjunct defect is the one that
        # silently doubles the trade count.
        "entry drops the RSI oversold condition (buys any bar in an uptrend)",
        [
            (
                "        return rsi_value < OVERSOLD_THRESHOLD and close > trend_value",
                "        return close > trend_value",
            )
        ],
        "test_a_rising_ramp_never_enters_and_always_exits",
    ),
    (
        "exit's comparison relaxed to >= (exits on a flat market, RSI exactly 50)",
        [
            (
                "        return rsi_value > EXIT_THRESHOLD",
                "        return rsi_value >= EXIT_THRESHOLD",
            )
        ],
        "test_an_rsi_of_exactly_fifty_does_not_exit",
    ),
    (
        # The judgement call in the module, inherited from S-1 and wider here:
        # both legs declare the same inputs, so the strategy has ONE warm-up at
        # 200 bars. Per-leg declarations make bars 14..198 live for the exit and
        # warming for the entry.
        "the exit leg declares only its own inputs (per-leg evaluability)",
        [
            (
                '        *evaluate(exit_, inputs=inputs, n_bars=n_bars, kind="exit"),',
                '        *evaluate(exit_, inputs=(inputs[1],), n_bars=n_bars, kind="exit"),',
            )
        ],
        "test_both_legs_share_the_trend_warm_up",
    ),
    (
        # `signal_kind` is in the ledger's uniqueness key precisely so the two
        # legs coexist on one bar. Emitting both as entries collides.
        "both legs emitted under the entry kind",
        [('n_bars=n_bars, kind="exit"),', 'n_bars=n_bars, kind="entry"),')],
        "test_both_legs_coexist_on_one_bar",
    ),
    (
        # ⚠⚠ THE PROBE THAT PAID. This reported NOT CAUGHT on the first run,
        # because `tests/test_strategy_s3.py` fed the naive reference the
        # module's OWN `RSI_PERIOD` — so shifting it shifted the reference too
        # and every bar still "matched". The test file now carries §4's numbers
        # as `SPEC_*` literals and the reference is independent of the code.
        # A period of 13 shifts every RSI value and the warm-up boundary with it.
        "the RSI period shifted by one",
        [("RSI_PERIOD = 14", "RSI_PERIOD = 13")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        # Same class: a threshold that drifts off §4's number is a different
        # strategy wearing this one's identity.
        "the oversold threshold loosened from 30 to 35",
        [("OVERSOLD_THRESHOLD = 30.0", "OVERSOLD_THRESHOLD = 35.0")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        "the trend lookback shortened from 200 to 100",
        [("TREND_PERIOD = 200", "TREND_PERIOD = 100")],
        "test_every_bar_matches_the_naive_reference",
    ),
    (
        "the exit threshold raised from 50 to 60",
        [("EXIT_THRESHOLD = 50.0", "EXIT_THRESHOLD = 60.0")],
        "test_every_bar_matches_the_naive_reference",
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
        # ⚠ THE ONE THAT MATTERS MOST HERE. `max_hold_bars` is the half of §4's
        # exit this module does not evaluate, so nothing else in the code would
        # notice it going missing — the identity hash is its only anchor.
        "the max-hold cap dropped from the hashed parameters",
        [('    "max_hold_bars": MAX_HOLD_BARS,\n', "")],
        "test_the_identity_carries_every_constant_the_rule_reads",
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
