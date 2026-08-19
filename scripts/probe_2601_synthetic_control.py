"""Revert-probe the #2601 synthetic-control orchestration invariants.

    PYTHONPATH=. uv run python scripts/probe_2601_synthetic_control.py

Sister to ``scripts/probe_2240_random_entry_cohort.py``, which probes the
CONSTRUCTION (``random_entry_cohort``). This one probes the ORCHESTRATION — the
placement space, the total-return carry and the compact shared-mark mapping — and the five
guards in that script's header apply unchanged:

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

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠⚠ **The per-member trade-count equality in ``run_cohort``.** Removing it needs
a placement that produces the wrong count to be observable, and the permutation
preserves the count by construction — so there is no fixture in which the guard
fires and the mutation is therefore unobservable rather than uncaught. The
property it protects is probed one layer down, in
``probe_2240_random_entry_cohort``'s multiset and slack probes, which are where
a dropped hold would actually originate.

⚠ **``CONTROL_NAMESPACE`` and ``HOLDOUT_CONTROL_REASON``'s strings.** Nothing
branches on the reason; it is a declaration written to the run log. The
namespace IS branched on, and the branch is probed below.

⚠ **``PLACEMENT_SPACE_ID``'s string.** A stamp, like ``COHORT_MODEL_ID``. The
integration test asserts the value travels onto the result; a different string
would be a different declared space, not a defect in this code.
"""

from __future__ import annotations

import sys
from pathlib import Path

# ⚠⚠ Invoking this file by PATH puts ``scripts/`` on sys.path and NOT the repo
# root, so the cross-script import below raises ModuleNotFoundError under the
# exact command the #2772 handover documents. Prepending the root makes both
# that form and ``-m scripts.<name>`` work — the same fix #2357/#2695 already
# applied to the sibling probes, missed here (#2772).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

CONTROL = Path("app/services/synthetic_control_run.py")
SOURCES = (CONTROL,)

TESTS = "tests/test_synthetic_control_run.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE ENTRY-LEG KEY DROPPED. An EXIT verdict says nothing about
        # whether a position could be OPENED on the following bar, and S-1/S-3
        # emit both legs on every bar — so counting exits widens the placement
        # space over bars the strategy was structurally cold on, and every
        # member draws from a null the real sleeve was never exposed to.
        "the placement space keyed on any leg rather than the entry leg",
        CONTROL,
        TESTS,
        [
            (
                '        if row.signal_kind != "entry" or row.not_evaluable_reason is not None:',
                "        if row.not_evaluable_reason is not None:",
            )
        ],
        "test_the_cold_prefix_is_excluded_because_the_rows_say_it_is",
    ),
    (
        # ⚠⚠ EVALUABILITY IGNORED. The warm-up prefix becomes placeable, so a
        # member opens on bars where the strategy's own verdict was
        # `insufficient_warmup`. This is the defect the measured-rather-than-
        # declared space exists to make impossible, injected directly.
        "the warm-up admitted into the placement space",
        CONTROL,
        TESTS,
        [
            (
                '        if row.signal_kind != "entry" or row.not_evaluable_reason is not None:',
                '        if row.signal_kind != "entry":',
            )
        ],
        "test_a_wider_warm_up_narrows_the_space_it_is_read_from",
    ),
    (
        # ⚠⚠ THE FILL MOVED ONTO THE SIGNAL BAR. `resolve_fills`' own rule is
        # `fill_index = signal_index + 1, always`; filling on the signal bar is
        # look-ahead — the member acts on a decision at a price the decision was
        # taken from — and it does so for every one of the 1,000 members, so the
        # null gets an advantage the real sleeve does not have.
        "the cohort filling on the signal bar rather than the bar after it",
        CONTROL,
        TESTS,
        [("        fill_index = signal_index + 1", "        fill_index = signal_index")],
        "test_the_cold_prefix_is_excluded_because_the_rows_say_it_is",
    ),
    (
        # ⚠⚠ THE IN-SAMPLE RESTRICTION REMOVED, WHICH IS CRITERION 5. A member
        # of an IN-SAMPLE control opens on a withheld bar: the null is then
        # priced partly off the hold-out, and the in-sample Sharpe is compared
        # against a distribution that has seen data the strategy has not.
        "hold-out bars admitted into the in-sample placement space",
        CONTROL,
        TESTS,
        [
            (
                "        if namespace_for_position(when, when) != CONTROL_NAMESPACE:",
                "        if False:",
            )
        ],
        "test_a_hold_out_bar_is_never_placed_into_the_in_sample_null",
    ),
    (
        # ⚠⚠ THE TOTAL-RETURN CARRY DROPPED. The sleeve's legs are priced
        # `net * wealth_close / raw_close` by `_absorb`; a cohort priced off the
        # raw open is a PRICE-return null under a TOTAL-return strategy, so the
        # whole dividend stream is attributed to the edge. Silent, one-directional
        # and exactly the shape §9's comparison cannot survive.
        "the cohort priced off the raw open, ignoring the total-return carry",
        CONTROL,
        TESTS,
        [
            (
                "        adjusted.append(float(bar_open) * wealth_close / raw_close)",
                "        adjusted.append(float(bar_open))",
            )
        ],
        "test_the_placement_price_is_carried_onto_the_total_return_basis",
    ),
    (
        # ⚠ A BAR WITH NO PRICE MADE PLACEABLE. `_absorb` excludes a real leg
        # whose endpoint has no usable close; admitting one here puts a `nan`
        # into the member's mark path, which propagates silently rather than
        # raising.
        "a bar with no usable close admitted as an entry",
        CONTROL,
        TESTS,
        [("    return bool(np.isfinite(value)) and value > 0.0", "    return True")],
        "test_a_bar_whose_close_is_missing_is_not_placeable",
    ),
    (
        # ⚠⚠ EVERY COMPACT LEG POINTS AT SERIES ZERO. The optimization is valid
        # only because it removes duplicate storage while retaining the exact
        # per-series mark source. A wrong source can still produce finite curves
        # and therefore needs the reference differential to catch it.
        "the compact member reads every leg from the first series' marks",
        CONTROL,
        TESTS,
        [
            (
                "        mark_source[cursor:end] = source",
                "        mark_source[cursor:end] = 0",
            )
        ],
        "test_compact_shared_marks_are_exactly_equivalent_to_the_reference_book",
    ),
    (
        # ⚠⚠ #2775. ``ru_maxrss`` is a process LIFETIME high-water mark, so the
        # per-worker allowance used to be a difference of two PARENT readings —
        # which is zero from the second cohort of an invocation onward, and the
        # memory arm of the scale gate then admitted a fan-out on a measurement
        # of nothing. Deleting the fresh-child measurement restores exactly that
        # collapse, and nothing about the resulting run looks wrong.
        "the per-worker memory figure taken from the flat base instead of a fresh child",
        CONTROL,
        TESTS,
        [
            (
                "    per_worker_unique_bytes = SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES\n"
                "    if measure_child:\n"
                "        child_peak_bytes, shared_block_bytes = _measure_child_member_peak(inputs, index=0)\n"
                "        per_worker_unique_bytes = max("
                "child_peak_bytes - shared_block_bytes, SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES)",
                "    per_worker_unique_bytes = SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES",
            )
        ],
        "test_the_memory_projection_measures_a_FRESH_CHILD_ON_EVERY_COHORT",
    ),
    (
        # ⚠⚠ #2775, second defect. Building the shared inputs costs the PARENT
        # about twice ``shared_input_bytes`` — ``_shared_member_inputs``
        # concatenates the per-series arrays into four contiguous temporaries
        # AND allocates equally sized SharedMemory blocks to copy them into, and
        # the temporaries stay referenced for the pool's lifetime. Reading the
        # parent peak BEFORE the child probe builds those blocks under-states it
        # by a whole copy of the corpus, which near the ceiling is the difference
        # between admitting and refusing.
        "the parent peak read before the shared inputs it has to pay for",
        CONTROL,
        TESTS,
        [
            (
                "        per_worker_unique_bytes = max("
                "child_peak_bytes - shared_block_bytes, SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES)\n"
                "    parent_peak_bytes = _peak_rss_bytes()",
                "        per_worker_unique_bytes = max("
                "child_peak_bytes - shared_block_bytes, SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES)\n"
                "    parent_peak_bytes = 0",
            )
        ],
        "test_the_parent_peak_is_read_AFTER_the_shared_inputs_are_built",
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
