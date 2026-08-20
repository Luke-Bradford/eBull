"""Revert-probe the phase-5e-5a quarantine-sensitivity invariant tests (#2240).

    uv run python scripts/probe_2240_quarantine_sensitivity.py

Sister to ``scripts/probe_2240_walk_forward.py``; the five guards in its header
apply unchanged and the strict runner is IMPORTED rather than copied:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is not guarded by either.** Triage ``NOT CAUGHT`` in the
   order selector → fixture → code.
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** A syntax break exits 4 and a
   pytest USAGE error exits 4 as well; both would read as a catch.
5. ⚠ **Run a BASELINE first**, so "the mutation broke it" and "it was already
   broken" are distinguishable.

⚠ NOT A TEST. It mutates tracked source files on disk; CI does not run it.
Everything probed here is pure-tier, so no database is needed.

⚠ MUST NOT RUN CONCURRENTLY with
``scripts/verify_2240_quarantine_sensitivity.py``, which imports both modules — a
concurrent run would be stamped with injected source.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``SENSITIVITY_MODEL_ID``'s string and ``SPEC_CRITERION7_METRICS``' order.**
Nothing branches on either. The bridge test pins the metric NAMES against an
independently transcribed set, which is the check that actually applies; a
probe deleting a name from the tuple is the same assertion by a slower route.

⚠⚠ **The "which bars differ between the arms" count in the verify script.** It
lives in the SWEEP, not in a module, and its correct value is a property of the
corpus rather than of a rule — a probe could only compare it against itself.
The full-population run gates on it being non-zero, which is what makes an arm
that silently stopped admitting anything visible.

⚠ **The promotion refusal's WIRING** (``quarantine_arms_not_compared``). It is
one boolean and one append; ``tests/test_strategy_result.py`` asserts both the
refusal's presence and — separately — the ABSENCE of a materiality twin, which
is the part a probe cannot express.
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

SENSITIVITY = Path("app/services/quarantine_sensitivity.py")
STORE = Path("app/services/research_price_structure_store.py")
SOURCES = (SENSITIVITY, STORE)

TESTS = "tests/test_quarantine_sensitivity.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE ARM MADE A NO-OP. The admitted arm masks exactly what the
        # masked arm does, so both metric sets are identical, every delta is
        # 0.0, and the run reports "the quarantine costs nothing" — the
        # strongest possible conclusion, from an arm that never ran.
        "the admitted arm masking the flagged fields as well",
        STORE,
        TESTS,
        [
            (
                "                high=high if (range_usable or admit) else None,\n"
                "                low=low if (range_usable or admit) else None,\n"
                "                close=close if (return_usable or admit) else None,",
                "                high=high if range_usable else None,\n"
                "                low=low if range_usable else None,\n"
                "                close=close if return_usable else None,",
            )
        ],
        "test_the_admitted_arm_keeps_them_at_their_stored_values",
    ),
    (
        # ⚠⚠ THE CENSUS ZEROED IN THE ADMITTED ARM. `*_flagged` moving with the
        # arm makes the sensitivity arm report its own exclusion as empty: an
        # arm whose job is to measure what masking cost, printing "nothing was
        # masked".
        "the flagged counts following the arm instead of the verdict",
        STORE,
        TESTS,
        [
            (
                "        if not range_usable:\n"
                "            range_flagged += 1\n"
                "        if not return_usable:\n"
                "            return_flagged += 1",
                "        if not range_usable and not admit:\n"
                "            range_flagged += 1\n"
                "        if not return_usable and not admit:\n"
                "            return_flagged += 1",
            )
        ],
        "test_the_arms_agree_on_what_was_flagged_and_differ_on_what_was_masked",
    ),
    (
        # ⚠ THE OVERLAP DOUBLE-COUNTED. Every return-flagged bar on this corpus
        # is also range-flagged, so summing the two verdicts inflates criterion
        # 9's share by up to 2x — in the direction that makes the exclusion look
        # bigger, which is not the flattering direction and is still wrong.
        "the flagged BAR count summed across the two verdicts",
        STORE,
        TESTS,
        [
            (
                "        if not (range_usable and return_usable):\n            bars_flagged += 1",
                "        if not range_usable:\n            bars_flagged += 1\n"
                "        if not return_usable:\n            bars_flagged += 1",
            )
        ],
        "test_flagged_bars_are_counted_once_not_summed_across_verdicts",
    ),
    (
        # ⚠⚠ A ONE-SIDED NULL SUBTRACTED AS A ZERO. "The admitted arm gained a
        # losing trade, so profit_factor became computable" then prints as
        # "unchanged" — the most interesting finding rendered as the least.
        "a metric absent in one arm reported as a zero delta",
        SENSITIVITY,
        TESTS,
        [
            (
                "        if left_f is None and right_f is None:\n"
                '            state: DeltaState = "both_null"\n'
                "        elif left_f is None:\n"
                '            state = "masked_null"\n'
                "        elif right_f is None:\n"
                '            state = "admitted_null"\n'
                "        else:\n"
                '            state = "measured"',
                "        if left_f is None and right_f is None:\n"
                '            state: DeltaState = "both_null"\n'
                "        elif left_f is None:\n"
                "            left_f = 0.0\n"
                '            state = "measured"\n'
                "        elif right_f is None:\n"
                "            right_f = 0.0\n"
                '            state = "measured"\n'
                "        else:\n"
                '            state = "measured"',
            )
        ],
        "test_a_metric_null_in_one_arm_has_no_delta_and_is_not_zero",
    ),
    (
        # ⚠ THE DELTA REVERSED. `masked - admitted` reads as "admitting would
        # reduce the Sharpe" when it raises it. Every sign in the report flips,
        # and nothing about the table's shape says which way round it is.
        "the delta taken as masked minus admitted",
        SENSITIVITY,
        TESTS,
        [
            (
                'delta=None if state != "measured" else right_f - left_f',
                'delta=None if state != "measured" else left_f - right_f',
            )
        ],
        "test_a_delta_is_the_admitted_value_minus_the_masked_one",
    ),
    (
        # ⚠ A RELATIVE CHANGE ON A ZERO BASE. Python raises here rather than
        # returning an infinity, so the defect is a crash mid-report — but the
        # shape being guarded is the one where a metric that was 0.0 acquires a
        # meaningless percentage beside the others.
        "the relative change computed against a zero base",
        SENSITIVITY,
        TESTS,
        [
            (
                "        if self.delta is None or self.masked in (None, 0.0):",
                "        if self.delta is None or self.masked is None:",
            )
        ],
        "test_a_relative_change_on_a_zero_base_is_none_not_an_infinity",
    ),
    (
        # ⚠⚠ THE CONTROLLED-EXPERIMENT CHECK REMOVED. Two arms that read
        # different populations produce a delta attributed to handling when it
        # is a difference in what was read. It is the one check that makes every
        # number below it mean what it says.
        "the arms allowed to disagree on the population they read",
        SENSITIVITY,
        TESTS,
        [("            if left != right:", "            if False:")],
        "test_arms_that_read_different_populations_are_refused",
    ),
    (
        # ⚠ THE TRADE DELTA FORCED POSITIVE. Admitting bars can LOSE trades, and
        # an absolute value would hide exactly the case worth reading — the
        # quarantine having removed a trade the strategy would otherwise not
        # have taken.
        "the trade delta reported as a magnitude",
        SENSITIVITY,
        TESTS,
        [
            (
                "        return self.admitted.trades - self.masked.trades",
                "        return abs(self.admitted.trades - self.masked.trades)",
            )
        ],
        "test_the_trade_delta_is_signed",
    ),
    (
        # ⚠ CRITERION 8'S CLOSED VOCABULARY OPENED. A free-text reason cannot be
        # counted against the criterion's list, so a census carrying one reports
        # a total nobody can attribute.
        "an unknown not_evaluable reason code admitted into the census",
        SENSITIVITY,
        TESTS,
        [("        unknown = set(self.not_evaluable) - NOT_EVALUABLE_REASONS", "        unknown = set()")],
        "test_an_unknown_reason_code_is_refused",
    ),
    (
        # ⚠ THE SHARE FABRICATED ON AN EMPTY READ. A run whose coverage join
        # matched nothing would print "0.000000% of bars flagged" — a reassuring
        # number produced by having read no bars at all.
        "a zero-bar read reporting a zero share instead of none",
        SENSITIVITY,
        TESTS,
        [
            (
                "        return None if self.bars == 0 else 100.0 * self.bars_flagged / self.bars",
                "        return 0.0 if self.bars == 0 else 100.0 * self.bars_flagged / self.bars",
            )
        ],
        "test_an_empty_read_has_no_share_rather_than_a_zero_one",
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
                print(f"  {'*** NO SUCH TEST ***':<20} {name}", flush=True)
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
                print(f"  {'*** BAD ANCHOR ***':<20} {name}", flush=True)
                continue
            rc_baseline = run([tests], selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<20} {name}  (exit {rc_baseline})", flush=True)
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
            print(f"  {verdict:<20} {name}  ({count} test{'' if count == 1 else 's'})", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt.
        for source, text in originals.items():
            source.write_text(text)

    rc_suite = run([TESTS], "test_")
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
