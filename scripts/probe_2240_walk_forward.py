"""Revert-probe the phase-5e-4 purged-walk-forward invariant tests (#2240).

    PYTHONPATH=. uv run python scripts/probe_2240_walk_forward.py

Sister to ``scripts/probe_2240_deflated_sharpe.py``; the five guards in its
header apply unchanged and the strict runner is IMPORTED rather than copied:

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

⚠ NOT A TEST. It mutates a tracked source file on disk; CI does not run it.
Everything probed here is pure-tier, so no database is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2240_walk_forward.py``, which
imports the module — a concurrent run would be stamped with injected source.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``FOLD_COUNT``'s value and ``WALK_FORWARD_MODEL_ID``'s string.** Nothing
branches on either; ``tests/test_walk_forward.py`` pins both against transcribed
literals, which is the check that actually applies.

⚠ **``census``' arithmetic.** It is a counting wrapper over ``role`` by
construction — there is no second copy of the rule to break. Every defect that
could reach it is a defect in ``role``, and those are probed below.

⚠⚠ **The "unlabelled at window end" exclusion**, which lives in the VERIFY
SCRIPT rather than the module. It is a population choice, not arithmetic: a
probe admitting open positions with an end index at the axis end would change
every measured embargo, and no pure-tier test can say the resulting number is
wrong — only the full-population run can, and it reports the excluded count for
exactly that reason.
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

WALK = Path("app/services/walk_forward.py")
SOURCES = (WALK,)

TESTS = "tests/test_walk_forward.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ §5.2'S WEIGHTING INVERTED. Every date contributes equally, so three
        # of four folds land in the thin 1960s-2000s era carrying almost no
        # observations and the fourth is the whole modern corpus. The split
        # still looks like a split.
        "the fold cut weighted by DATE instead of by BAR",
        WALK,
        TESTS,
        [
            ("    total = sum(bar_counts)", "    total = len(bar_counts)"),
            ("        cumulative += count", "        cumulative += 1"),
        ],
        "test_the_cut_follows_bars_not_dates",
    ),
    (
        # ⚠ §5.2's selection rule is "strictly exceeds". `>=` moves every fold
        # edge by a date against a rule this repo already fixed once for the
        # hold-out boundary.
        "the fold boundary taken where the cumulative REACHES the target, not exceeds it",
        WALK,
        TESTS,
        [
            (
                "        while target < fold_count and cumulative * fold_count > total * target:",
                "        while target < fold_count and cumulative * fold_count >= total * target:",
            )
        ],
        "test_the_boundary_date_starts_the_next_fold",
    ),
    (
        # ⚠⚠ THE CLAMP REMOVED. A degenerate axis then produces repeated edges,
        # and a repeated edge is an EMPTY fold — a fold whose training set is
        # the entire sample and whose test set is nothing.
        "the non-empty-fold clamp dropped",
        WALK,
        TESTS,
        [("        edges.append(min(max(boundary, lowest), highest))", "        edges.append(boundary)")],
        "test_a_single_dense_date_still_yields_non_empty_folds",
    ),
    (
        # ⚠⚠ THE PURGE WRITTEN ON ENDPOINTS. A trade opened before the fold and
        # closed after it spans the fold entirely — every price the fold owns is
        # inside its label window — and an endpoint test calls it training data.
        # This is the branch §5.3's "label window overlaps" wording exists for.
        "the purge testing the close endpoint instead of the label INTERVAL",
        WALK,
        TESTS,
        [
            (
                "    if start_index <= fold.last_index and end_index >= fold.first_index:",
                "    if fold.first_index <= end_index <= fold.last_index:",
            )
        ],
        "test_role_verdicts",
    ),
    (
        # ⚠ The embargo window half-open on the RIGHT, so an `h`-bar embargo
        # covers `h - 1` dates. Off by one in the direction that leaks, every
        # fold, silently.
        "the embargo window one date short",
        WALK,
        TESTS,
        [
            (
                "    if fold.last_index < start_index <= fold.last_index + embargo_bars:",
                "    if fold.last_index < start_index < fold.last_index + embargo_bars:",
            )
        ],
        "test_role_verdicts",
    ),
    (
        # ⚠⚠ THE EMBARGO KEYED ON THE CLOSE. It is the ENTRY that inherits the
        # test fold's information through serial correlation; an observation
        # that opens inside the embargo and closes past it would go straight
        # into training.
        "the embargo keyed on the observation's close instead of its entry",
        WALK,
        TESTS,
        [
            (
                "    if fold.last_index < start_index <= fold.last_index + embargo_bars:",
                "    if fold.last_index < end_index <= fold.last_index + embargo_bars:",
            )
        ],
        "test_role_verdicts",
    ),
    (
        # ⚠⚠ BRANCH ORDER. Purging before testing steals every observation that
        # opens inside the fold and closes after it — they are the test set's
        # own, and the fold would silently shrink.
        "the purge evaluated before the test-membership branch",
        WALK,
        TESTS,
        [
            (
                "    if start_index >= fold.first_index and start_index <= fold.last_index:\n"
                '        return "test"\n'
                "    if start_index <= fold.last_index and end_index >= fold.first_index:\n"
                '        return "purged"',
                "    if start_index <= fold.last_index and end_index >= fold.first_index:\n"
                '        return "purged"\n'
                "    if start_index >= fold.first_index and start_index <= fold.last_index:\n"
                '        return "test"',
            )
        ],
        "test_role_verdicts",
    ),
    (
        # ⚠⚠ THE PEEK §5.3 REJECTED. Measuring over the PRE-purge candidates
        # uses the length of a trade whose label window reaches into the test
        # fold — i.e. how long that fold's own prices took to resolve a
        # position. It is the *measured p99* defect wearing a p100's clothes.
        "the embargo measured over the pre-purge population",
        WALK,
        TESTS,
        [
            (
                '        if role(start, end, fold=fold, embargo_bars=0) != "train":',
                '        if role(start, end, fold=fold, embargo_bars=0) == "test":',
            )
        ],
        "test_the_embargo_is_the_widest_span_wholly_outside_the_fold",
    ),
    (
        # ⚠ The span counted as a bar COUNT rather than a DISPLACEMENT. One date
        # too many on every embargo — small, permanent, and invisible.
        "the label span counted inclusively instead of as a displacement",
        WALK,
        TESTS,
        [("        widest = max(widest, end - start)", "        widest = max(widest, end - start + 1)")],
        "test_the_span_is_a_displacement_not_a_bar_count",
    ),
    (
        # ⚠⚠ THE BOUND INVERTED. `min` against a zero seed is always zero, so
        # every fold gets no embargo at all and the whole stage is a no-op that
        # reports success.
        "the embargo taken as the narrowest span instead of the widest",
        WALK,
        TESTS,
        [("        widest = max(widest, end - start)", "        widest = min(widest, end - start)")],
        "test_the_embargo_is_the_widest_span_wholly_outside_the_fold",
    ),
    # --- stage 5e-5c: the STORED split's shape -----------------------------
    (
        # ⚠⚠ A THREE-FOLD SPLIT STORED AS A COMPLETE ONE. The rows are each
        # individually correct and the set is a cross-validation that stopped
        # early — which is exactly what it would not look like on a read.
        "the stored split no longer required to be complete",
        WALK,
        TESTS,
        [("        if len(self.folds) != FOLD_COUNT:", "        if False:")],
        "test_a_split_that_is_not_four_folds_is_refused",
    ),
    (
        "a fold's index no longer required to be its position in the split",
        WALK,
        TESTS,
        [("            if record.fold.index != position:", "            if False:")],
        "test_a_fold_whose_index_is_not_its_position_is_refused",
    ),
    (
        # ⚠ A gap at the front is training data that no fold ever tested — it
        # never enters the purge, the embargo or any census, and every count
        # below it still adds up.
        "the split no longer required to start at the axis front",
        WALK,
        TESTS,
        [("        if self.folds[0].fold.first_index != 0:", "        if False:")],
        "test_a_split_that_does_not_start_at_the_axis_front_is_refused",
    ),
    (
        "the folds no longer required to be contiguous",
        WALK,
        TESTS,
        [("            if later.fold.first_index != earlier.fold.last_index + 1:", "            if False:")],
        "test_a_gap_between_two_folds_is_refused",
    ),
    (
        # ⚠⚠ THE SECOND AXIS. Indices and dates are stored side by side and only
        # one of them is derived from the other's axis; dropping the date check
        # lets a split whose dates came from a different corpus pass as one
        # whose indices are contiguous.
        "the fold DATES no longer required to agree with the fold indices",
        WALK,
        TESTS,
        [("            if later.first_date <= earlier.last_date:", "            if False:")],
        "test_index_contiguous_folds_that_overlap_in_TIME_are_refused",
    ),
    (
        # ⚠⚠ THE CHECK THAT CATCHES TWO RUNS. Every fold classifies every
        # observation, so their totals are equal by construction; without this
        # a split assembled from two sweeps stores four internally-consistent
        # censuses of different populations.
        "the folds no longer required to count one population",
        WALK,
        TESTS,
        [("        if len(totals) > 1:", "        if False:")],
        "test_folds_counting_different_populations_are_refused",
    ),
    (
        "a fold record accepting a last date before its first",
        WALK,
        TESTS,
        [("        if self.last_date < self.first_date:", "        if False:")],
        "test_fold_record_refuses_impossible_fields",
    ),
    (
        # ⚠ The #2286 shape: a PRESENT-but-empty construction id. NOT NULL does
        # not catch it and neither does anything downstream.
        "a blank construction id accepted on a stored split",
        WALK,
        TESTS,
        [("        if not self.model_id:", "        if False:")],
        "test_a_blank_model_id_is_refused",
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
