"""Revert-probe the phase-5e-5b random-entry-cohort invariant tests (#2240).

    PYTHONPATH=. uv run python scripts/probe_2240_random_entry_cohort.py

Sister to ``scripts/probe_2240_quarantine_sensitivity.py``; the five guards in
its header apply unchanged and the strict runner is IMPORTED rather than copied:

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
``scripts/verify_2240_random_entry_cohort.py``, which imports this module — a
concurrent run would compute a 1,000-member cohort under injected source.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``COHORT_MODEL_ID``'s string.** Nothing branches on it; it is a stamp. The
bridge test asserts it is non-empty, which is the only property that has a
consequence — a blank one would let two constructions share a row.

⚠⚠ **The PLACEMENT MEASURE itself** — that the gaps are sorted iid uniforms
rather than a uniform composition. There is no source rule to violate, so there
is no defect to inject: both are valid null constructions and the module
declares which it uses. What IS probed is every property the measure has to
have whatever it is (non-overlap, in-bounds, count and multiset preserved).

⚠ **``--prepare``'s eligible-bar table.** It lives in the verify script, not in
a module, and its correct value is a property of the corpus rather than of a
rule. The full-population ``--properties`` arm asserts it over every series,
which is the check that actually applies.

⚠ **The three promotion refusals' WIRING.** ``tests/test_strategy_result.py``
asserts each code fires on its own condition AND that both synthetic failures
are reported together, which is the part a probe of a single append cannot
express.
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

COHORT = Path("app/services/random_entry_cohort.py")
SOURCES = (COHORT,)

TESTS = "tests/test_random_entry_cohort.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE SHUFFLE REMOVED. Every random entry keeps the hold the strategy
        # chose in that slot, so the sequence of holding periods — which is a
        # fact about what the signal saw — travels into the null. The multiset
        # test still passes, because the identity IS a permutation.
        "the holding periods kept in the strategy's own order",
        COHORT,
        TESTS,
        [("    permuted = rng.permutation(holds)", "    permuted = holds")],
        "test_the_holds_do_not_keep_the_strategys_own_order",
    ),
    (
        # ⚠⚠ THE GAPS LEFT UNSORTED. Sorting is the ONLY thing making the gaps
        # non-negative, so unsorted draws let positions overlap inside one
        # instrument — §3.1's pyramiding rule, violated silently, and each
        # overlap is capital committed twice.
        "the leading gaps left unsorted, so positions overlap",
        COHORT,
        TESTS,
        [
            (
                "    leading = np.sort(rng.integers(0, free + 1, size=count))",
                "    leading = rng.integers(0, free + 1, size=count)",
            )
        ],
        "test_positions_never_overlap",
    ),
    (
        # ⚠ THE PREFIX SUM MADE INCLUSIVE. Every position is offset by its OWN
        # hold as well as its predecessors', so the last leg runs off the end of
        # the eligible space — trading a bar the strategy could never reach.
        "an inclusive prefix sum, pushing every leg past its slot",
        COHORT,
        TESTS,
        [
            (
                "    consumed = np.concatenate(([0], np.cumsum(permuted[:-1])))",
                "    consumed = np.cumsum(permuted)",
            )
        ],
        "test_every_leg_lands_inside_the_eligible_space",
    ),
    (
        # ⚠ THE SLACK OFF BY ONE. `eligible` counts bars and the last valid
        # ordinal is `eligible - 1`; using the count lets the final exit land one
        # bar past the end, which is an IndexError in the cohort arm and a
        # silently wrong bound here.
        "the slack computed off the bar COUNT rather than the last ordinal",
        COHORT,
        TESTS,
        [("    return (eligible - 1) - int(holds.sum())", "    return eligible - int(holds.sum())")],
        "test_every_leg_lands_inside_the_eligible_space",
    ),
    (
        # ⚠⚠ THE CONTRADICTION ABSORBED. A series whose holds do not fit gets
        # its slack clamped to zero and its positions stacked into whatever room
        # there is — the cohort still reports a matched trade count while trading
        # a shape the strategy never could.
        "a series that cannot carry its holds silently clamped instead of refused",
        COHORT,
        TESTS,
        [
            (
                "    if free < 0:\n        raise ValueError(",
                "    if False:\n        raise ValueError(",
            )
        ],
        "test_a_series_that_cannot_carry_its_holds_is_refused",
    ),
    (
        # ⚠⚠ THE ENTRY COST TURNED INTO A SUBSIDY. A buy filling BELOW the open
        # pays the operator to trade, and it does so identically for every one of
        # the 1,000 members — so the null distribution shifts up and the real
        # strategy's Sharpe stops standing out for a reason that has nothing to
        # do with either.
        "the entry side of the cost model applied as a discount",
        COHORT,
        TESTS,
        [("    return opens * (1.0 + half_spreads)", "    return opens * (1.0 - half_spreads)")],
        "test_the_entry_side_is_charged_up_and_the_exit_side_down or test_the_float_form_agrees",
    ),
    (
        # ⚠⚠ §9's FIRST THRESHOLD MADE ONE-SIDED, UPPER BOUND DROPPED. An
        # interval sitting entirely BELOW zero now passes — which is exactly
        # what a cost-dragged random cohort on this corpus produces, so it is
        # the half most likely to be reached in practice.
        "the cohort-mean threshold ignoring the upper end of its interval",
        COHORT,
        TESTS,
        [
            (
                "        return self.mean_return_ci_low_pct <= 0.0 <= self.mean_return_ci_high_pct",
                "        return self.mean_return_ci_low_pct <= 0.0",
            )
        ],
        "test_an_interval_excluding_zero_from_below_fails_too",
    ),
    (
        # ⚠⚠ THE SAME THRESHOLD ONE-SIDED THE OTHER WAY: an interval entirely
        # ABOVE zero passes, which is the harness finding edge in noise. ⚠ TWO
        # PROBES AND NOT ONE, because each mutation is only visible in the half
        # it drops — a single probe would leave the other direction unpinned,
        # and this threshold is two-sided precisely because both are failures.
        "the cohort-mean threshold ignoring the lower end of its interval",
        COHORT,
        TESTS,
        [
            (
                "        return self.mean_return_ci_low_pct <= 0.0 <= self.mean_return_ci_high_pct",
                "        return self.mean_return_ci_high_pct >= 0.0",
            )
        ],
        "test_an_interval_excluding_zero_fails_the_first_threshold",
    ),
    (
        # ⚠ §9's SECOND THRESHOLD RELAXED TO `>=`. §9 says "must EXCEED", and a
        # strategy sitting exactly ON the 950th random member's Sharpe is
        # indistinguishable from it — admitting it is admitting noise.
        "the Sharpe threshold admitting equality",
        COHORT,
        TESTS,
        [
            (
                "        return self.strategy_sharpe > self.cohort_sharpe_threshold",
                "        return self.strategy_sharpe >= self.cohort_sharpe_threshold",
            )
        ],
        "test_a_sharpe_equal_to_the_threshold_does_not_exceed_it",
    ),
    (
        # ⚠⚠ A THIRD THRESHOLD SMUGGLED INTO THE VERDICT. §9's acceptance names
        # exactly two; the return percentile is reported so a reader can judge
        # them. Folding it in makes this module the author of an acceptance
        # criterion, and a spec amendment would be invisible as a code change.
        "the reported return percentile folded into the verdict",
        COHORT,
        TESTS,
        [
            (
                "        return self.mean_return_ci_contains_zero and self.sharpe_exceeds_cohort",
                "        return (\n"
                "            self.mean_return_ci_contains_zero\n"
                "            and self.sharpe_exceeds_cohort\n"
                "            and self.return_exceeds_cohort\n"
                "        )",
            )
        ],
        "test_the_return_percentile_is_reported_and_does_not_enter_the_verdict",
    ),
    (
        # ⚠⚠ THE DUPLICATE-MEMBER CHECK REMOVED. A shard re-run that left its
        # old output in place counts some draws twice, which NARROWS the null
        # distribution the real strategy has to beat — the one direction that
        # makes a strategy look better and leaves every printed figure plausible.
        "duplicated cohort members admitted into the null distribution",
        COHORT,
        TESTS,
        [("    if len(seen) != len(members):", "    if False:")],
        "test_a_duplicated_member_is_refused",
    ),
    (
        # ⚠ THE EXACT MATCH GIVEN A TOLERANCE. One trade of slack absorbs
        # exactly the failure the permutation can have — a series whose holds
        # were dropped — and the cohort still reports itself matched.
        "a one-trade tolerance on the exact trade-count match",
        COHORT,
        TESTS,
        [
            (
                "rel_tol=0.0, abs_tol=1e-9)",
                "rel_tol=0.0, abs_tol=1.0)",
            )
        ],
        "test_one_member_short_by_a_single_trade_breaks_the_match",
    ),
    (
        # ⚠⚠ EVERY MEMBER GIVEN THE SAME STREAM. An empty spawn key makes all
        # 1,000 members identical: the cohort's interval collapses to a point,
        # its 95th percentile becomes its only value, and the run still prints a
        # complete §9 verdict off a null distribution of one.
        "every cohort member drawing the same stream",
        COHORT,
        TESTS,
        [
            (
                "    return np.random.SeedSequence(entropy=COHORT_ROOT_SEED, spawn_key=(index,))",
                "    return np.random.SeedSequence(entropy=COHORT_ROOT_SEED, spawn_key=())",
            )
        ],
        "test_two_members_draw_different_placements",
    ),
    (
        # ⚠⚠ THE COHORT CUT INTERPOLATED. NumPy's default puts the 95th
        # percentile of a 1,000-member cohort BETWEEN the 950th and 951st sorted
        # members — a value no member achieved — so a strategy that beat every
        # draw at or below the declared rank is refused against a number the null
        # never produced. This is the defect Codex found at checkpoint 2: the
        # module's own header declared the order statistic and the code did not.
        "the cohort percentile interpolated between two members",
        COHORT,
        TESTS,
        [
            (
                '    return float(np.percentile(values, percentile, method="inverted_cdf"))',
                "    return float(np.percentile(values, percentile))",
            )
        ],
        "test_the_threshold_is_an_order_statistic_and_not_an_interpolation",
    ),
    (
        # ⚠ THE INTERVAL COLLAPSED TO ONE TAIL. Both ends read the LOW
        # percentile, so the reported interval sits entirely below the mean —
        # and "contains zero" then answers a question about the lower tail
        # alone, which is §9's first threshold silently made one-sided again by
        # a different route.
        "both interval ends taken from the same tail",
        COHORT,
        TESTS,
        [
            (
                "    low, high = np.percentile(means, [tail, 100.0 - tail])",
                "    low, high = np.percentile(means, [tail, tail])",
            )
        ],
        "test_the_interval_brackets_the_mean",
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
