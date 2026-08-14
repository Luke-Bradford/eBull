"""Revert-probe the phase-5e-3 Deflated-Sharpe invariant tests (#2240).

    uv run python scripts/probe_2240_deflated_sharpe.py

Sister to ``scripts/probe_2240_block_bootstrap.py``; the five guards in its
header apply unchanged and the strict runner is IMPORTED rather than copied:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is not guarded by either.** Triage ``NOT CAUGHT`` in the
   order selector → fixture → code.
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** A syntax break exits 4 and a
   pytest USAGE error exits 4 as well; both would read as a catch under a
   "non-zero means failed" rule (the #2214 false 4/4).
5. ⚠ **Run a BASELINE first**, so "the mutation broke it" and "it was already
   broken" are distinguishable.

⚠ THREE SOURCE FILES: ``deflated_sharpe`` is the construction, ``trial_register``
the declaration it counts, and ``strategy_result`` the row that binds the two.

⚠ NOT A TEST. It mutates tracked source files on disk; CI does not run it.
Everything probed here is pure-tier, so no database is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2240_statistics.py``, which
imports all three — a concurrent run would be stamped with injected source.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``DSR_MODEL_ID``'s value.** Nothing branches on it; ``sql/266``'s non-empty
CHECK and the literal in ``tests/test_deflated_sharpe.py`` keep it honest.

⚠ **``EULER_MASCHERONI``.** It is ``numpy.euler_gamma`` rather than a typed
literal precisely so there is no transcription to get wrong. Perturbing it does
move the paper's reference values, but the probe would be testing numpy.

⚠⚠ **``MIN_MEASURED_TRIALS``'s early return, and this is a FINDING not a
judgement call** — the same shape as 5e-2's ``MIN_CLUSTERS``. A probe replacing
``measured_trials < MIN_MEASURED_TRIALS`` with ``if False`` was written and ran
**CAUGHT**, because unlike ``MIN_CLUSTERS`` this guard is NOT shadowed by a
later one: ``trial_sharpe_variance`` is supplied by the caller, so a single
measured trial does not force a zero variance the way a single cluster forces a
zero bootstrap variance. It is probed below and it is load-bearing.

⚠ **The ``trial_register`` CONTENTS.** Adding or removing an entry changes ``M``
and so every DSR, but the register is a DECLARATION — its correctness is
evidence, not arithmetic, and no mutation of it can be "wrong" in a way a test
should encode. What IS probed is the machinery that keeps it honest: the
undeclared-key refusal and the ddof.
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

DSR = Path("app/services/deflated_sharpe.py")
REGISTER = Path("app/services/trial_register.py")
RESULT = Path("app/services/strategy_result.py")
SOURCES = (DSR, REGISTER, RESULT)

DSR_TESTS = "tests/test_deflated_sharpe.py"
REGISTER_TESTS = "tests/test_trial_register.py"
RESULT_TESTS = "tests/test_strategy_result.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE STAGE'S WHOLE POINT. `T` becomes the nominal trade count, which
        # is the number criterion 3 forbids reporting anywhere. The DSR still
        # computes and still looks entirely plausible — it is simply confident
        # on evidence the block bootstrap already showed was not there.
        "the nominal trade count used as the sample length instead of the ESS",
        DSR,
        DSR_TESTS,
        [
            (
                "    statistic = (sharpe - threshold) * math.sqrt(effective_sample_size - 1.0) "
                "/ math.sqrt(variance_term)",
                "    statistic = (sharpe - threshold) * math.sqrt(moments.trade_count - 1.0) "
                "/ math.sqrt(variance_term)",
            )
        ],
        "test_a_nominal_n_overstates_the_deflated_sharpe or test_the_stored_sample_length_is_the_effective_one",
    ),
    (
        # ⚠⚠ THE DEFLATION REMOVED. Subtracting nothing turns the DSR back into
        # a plain Probabilistic Sharpe Ratio — the multiple-testing correction
        # gone, wearing the name of the statistic that applies it.
        "the multiple-testing threshold not subtracted",
        DSR,
        DSR_TESTS,
        [("    statistic = (sharpe - threshold) *", "    statistic = (sharpe - 0.0) *")],
        "test_deflated_sharpe_matches_the_paper",
    ),
    (
        # ⚠ Excess kurtosis where the paper's `(y4 - 1)/4` expects the raw
        # fourth moment. Shrinks the denominator and inflates every DSR by a
        # silent constant — and a Normal fixture would still look fine.
        "excess kurtosis fed to the raw-kurtosis term",
        DSR,
        DSR_TESTS,
        [
            (
                "    variance_term = 1.0 - moments.skewness * sharpe + (moments.kurtosis - 1.0) / 4.0 * sharpe**2",
                "    variance_term = 1.0 - moments.skewness * sharpe + (moments.kurtosis - 3.0 - 1.0) "
                "/ 4.0 * sharpe**2",
            )
        ],
        "test_the_paper_s_normal_returns_counterfactual or test_deflated_sharpe_matches_the_paper",
    ),
    (
        # ⚠ The skew term dropped. Equation (2) without it is the Normal-returns
        # special case, which is exactly the inflation the paper's example is
        # built to show (N=88 vs N=46 at the same DSR).
        "the skewness term dropped from the standard error",
        DSR,
        DSR_TESTS,
        [("1.0 - moments.skewness * sharpe +", "1.0 - 0.0 * sharpe +")],
        "test_deflated_sharpe_matches_the_paper",
    ),
    (
        # ⚠⚠ Equation (9) ignored, so `M` is used where `N` belongs. A.3 opens
        # with exactly this: "using M instead of N will overstate E[max{SR_n}]".
        "the trial correlation ignored, so M is used as N",
        DSR,
        DSR_TESTS,
        [
            (
                "    return average_correlation + (1.0 - average_correlation) * declared_trials",
                "    return float(declared_trials)",
            )
        ],
        "test_perfect_correlation_collapses_to_one_trial or test_more_correlation_means_a_lower_threshold",
    ),
    (
        # ⚠ The Euler-Mascheroni weighting collapsed to the plain upper
        # quantile. Equation (5) is a weighted pair of quantiles, not one.
        "the expected-maximum weighting reduced to a single quantile",
        DSR,
        DSR_TESTS,
        [
            (
                "    max_z = (1.0 - EULER_MASCHERONI) * _NORMAL.inv_cdf(tail) "
                "+ EULER_MASCHERONI * _NORMAL.inv_cdf(tail_over_e)",
                "    max_z = _NORMAL.inv_cdf(tail)",
            )
        ],
        "test_expected_max_sharpe_matches_the_paper or test_deflated_sharpe_matches_the_paper",
    ),
    (
        # ⚠ A.3's positive-definite bound loosened to the naive (-1, 1]. At M=3
        # the real bound is -0.5, and a matrix that was never a correlation
        # matrix would pass.
        "the average-correlation bound loosened to minus one",
        DSR,
        DSR_TESTS,
        [("    lower_bound = -1.0 / (declared_trials - 1)", "    lower_bound = -1.0")],
        "test_a_correlation_below_the_positive_definite_bound_is_refused",
    ),
    (
        # ⚠ Equation (8) divided by M^2 rather than M(M-1) — i.e. averaging over
        # the diagonal too, which pulls every correlation toward 1/M.
        "the average correlation divided by M squared instead of M(M-1)",
        DSR,
        DSR_TESTS,
        [
            (
                "    return float(2.0 * upper.sum() / (size * (size - 1)))",
                "    return float(2.0 * upper.sum() / (size * size))",
            )
        ],
        "test_average_correlation_is_the_mean_off_diagonal",
    ),
    (
        # ⚠⚠ NOT a dead branch — see the header. With one measured trial the
        # caller's `trial_sharpe_variance` is still whatever it was, so nothing
        # downstream refuses, and V[{SR_n}] would be a variance over one point.
        "a single measured trial admitted, so V[SR_n] is a variance over one point",
        DSR,
        DSR_TESTS,
        [("    if measured_trials < MIN_MEASURED_TRIALS:", "    if False:")],
        "test_one_measured_trial_has_no_sharpe_variance",
    ),
    (
        # ⚠⚠ CODEX P2 AT CHECKPOINT 2. Perfectly correlated trials give N_hat=1,
        # where `Z^-1[1 - 1/N]` is `Z^-1[0] = -inf`. Without the conversion the
        # helper's RAISE escapes into the caller instead of failing closed —
        # and two register entries running one rule over two corpora is exactly
        # that shape, so it is reachable rather than theoretical.
        "an out-of-range implied trial count raising instead of refusing",
        DSR,
        DSR_TESTS,
        [
            (
                "    if not 1.0 < independent <= declared_trials:\n        return None",
                "    if False:\n        return None",
            )
        ],
        "test_perfectly_correlated_trials_refuse_rather_than_raise "
        "or test_negatively_correlated_trials_refuse_rather_than_raise",
    ),
    (
        # ⚠⚠ THE UPPER HALF OF THAT BOUND ON ITS OWN. The first version of this
        # guard was `independent <= 1.0`, which fixes only the rho==1 end and
        # leaves any NEGATIVE correlation raising — the review bot's BLOCKING
        # finding on PR #2372. This probe reverts to exactly that half, so a
        # future edit cannot silently drop the upper bound again.
        "the N-hat bound narrowed back to its lower end only",
        DSR,
        DSR_TESTS,
        [("    if not 1.0 < independent <= declared_trials:", "    if not 1.0 < independent:")],
        "test_negatively_correlated_trials_refuse_rather_than_raise",
    ),
    (
        # ⚠ A ZERO standard error admitted, so eq. (2) divides by `sqrt(0)`.
        # Surfaces as a crash rather than a wrong number — the refusal is what
        # makes it a stated state instead of a traceback.
        # ⚠⚠ ZERO, not negative: once the moment guard enforces Pearson's
        # `y4 >= y3^2 + 1`, the bracket's discriminant is always <= 0 and it can
        # only TOUCH zero, never go below. The strictly-negative case is
        # unreachable, which is why the named test sits on the Pearson boundary.
        "a zero variance term admitted into the square root",
        DSR,
        DSR_TESTS,
        [("    if variance_term <= 0.0 or not math.isfinite(variance_term):", "    if False:")],
        "test_a_zero_standard_error_is_refused",
    ),
    (
        # ⚠ Kurtosis validated as excess rather than raw, so a Normal's 0.0
        # would be accepted under a column every reader takes as raw.
        "the raw-kurtosis guard removed from TradeMoments",
        DSR,
        DSR_TESTS,
        [("        if self.kurtosis < floor:", "        if False:")],
        "test_excess_kurtosis_is_refused_at_construction",
    ),
    (
        # ⚠⚠ The kurtosis bound loosened back to `> 0` — the first review
        # NITPICK on PR #2372. `y4 >= 1` for any real distribution, so (0, 1) is
        # impossible, and the looser bound admitted all of it while the message
        # beside it claimed otherwise.
        "the kurtosis bound loosened from the Pearson floor back to 0",
        DSR,
        DSR_TESTS,
        [("        if self.kurtosis < floor:", "        if self.kurtosis <= 0.0:")],
        "test_a_kurtosis_between_zero_and_one_is_refused",
    ),
    (
        # ⚠⚠ The SKEW TIE dropped — the second review NITPICK on PR #2372. A
        # bare `>= 1` floor is right for y3 = 0 and wrong for every other skew:
        # Pearson gives `y4 >= y3^2 + 1`, so at y3 = 2 the floor is 5 and a
        # kurtosis of 1 beside it is impossible. This probe keeps the guard but
        # unties it from the skew, which is the version that passed review once.
        "the kurtosis floor untied from the skewness",
        DSR,
        DSR_TESTS,
        [("        floor = self.skewness**2 + 1.0", "        floor = 1.0")],
        "test_a_kurtosis_below_the_skewness_floor_is_refused",
    ),
    (
        # ⚠⚠ A COVARIANCE matrix is square AND symmetric, so both earlier guards
        # pass it. Without the unit-diagonal check eq. (8) averages its
        # off-diagonal covariances into something shaped like a correlation and
        # nothing downstream can tell. Review NITPICK on PR #2372.
        "a covariance matrix admitted as a correlation matrix",
        DSR,
        DSR_TESTS,
        [("    if not np.allclose(np.diag(matrix), 1.0):", "    if False:")],
        "test_a_covariance_matrix_is_refused",
    ),
    (
        # ⚠ The moments taken with a sample standard deviation, double-counting
        # the small-sample correction equation (2) already applies via sqrt(T-1).
        "the trade Sharpe's denominator taken with ddof=1",
        DSR,
        DSR_TESTS,
        [("    sigma = float(returns.std(ddof=0))", "    sigma = float(returns.std(ddof=1))")],
        # ⚠ NOT the paper's reference arm — that builds `TradeMoments` DIRECTLY
        # and never runs this denominator, so the probe came back NOT CAUGHT for
        # a selector fault rather than a missing guard. And a full-population
        # fixture cannot see it either: `sqrt(n/(n-1))` at 3.1M trades is
        # 1.00000016. The pin has to be a sample small enough for ddof to move
        # the answer, which is what the named test is.
        "test_the_sharpe_denominator_is_the_population_deviation",
    ),
    (
        # ⚠⚠ THE CHECK THAT KEEPS M HONEST. A measured trial the register never
        # declared is a trial missing from the count — skipping the key silently
        # is precisely the under-count criterion 6 calls decorative.
        "a measured trial absent from the register silently ignored",
        REGISTER,
        REGISTER_TESTS,
        [("        if unknown:", "        if False:")],
        "test_an_undeclared_measured_trial_is_refused",
    ),
    (
        # ⚠ V[{SR_n}] as a population variance. The trials RUN are a sample of
        # the trials that could have been run, and a population variance
        # understates the spread — hence SR_0, hence a flattering DSR.
        "the trial Sharpe variance taken as a population variance",
        REGISTER,
        REGISTER_TESTS,
        [("        return statistics.variance(values)", "        return statistics.pvariance(values)")],
        "test_two_measured_trials_give_a_sample_variance",
    ),
    (
        # ⚠ Duplicate trial ids admitted, which inflates M by counting one
        # search twice — the conservative direction, but still a wrong number.
        "duplicate trial ids admitted into the register",
        REGISTER,
        REGISTER_TESTS,
        [("        if len(ids) != len(set(ids)):", "        if False:")],
        "test_duplicate_trial_ids_are_refused",
    ),
    (
        # ⚠⚠ ONE SAMPLE SIZE, ONE COLUMN. Without this the row could be deflated
        # against one effective sample size and stored declaring another, and
        # the ledger round trip would silently swap the first for the second.
        "a DSR whose sample size disagrees with the metric set admitted",
        RESULT,
        RESULT_TESTS,
        [
            (
                "            if self.deflated.effective_sample_size != self.metrics.effective_sample_size:",
                "            if False:",
            )
        ],
        "test_a_sample_size_the_metric_set_does_not_carry_is_refused",
    ),
    (
        # ⚠ The stored trial count allowed to describe a different correction
        # from the one computed — `sql/266`'s all-or-nothing, at assembly time.
        "a stored trial count disagreeing with the computed DSR admitted",
        RESULT,
        RESULT_TESTS,
        [("            if self.trial_count != self.deflated.declared_trials:", "            if False:")],
        "test_a_disagreeing_trial_count_is_refused or test_a_missing_trial_count_beside_a_dsr_is_refused",
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

    rc_suite = run([DSR_TESTS, REGISTER_TESTS, RESULT_TESTS], "test_")
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
