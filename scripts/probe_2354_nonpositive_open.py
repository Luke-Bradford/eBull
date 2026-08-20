"""Revert-probe #2354's invariant tests — the non-positive open (Refs #2240).

    uv run python scripts/probe_2354_nonpositive_open.py

⚠⚠ THE RUNNER IS IMPORTED, NOT COPIED. ``run``, ``selected`` and the two
exit-code constants come from ``scripts.probe_2240_cost_model``, which is the
hardened harness (#2357 tracks extracting it). Copying it again would be another
place for the loose-gate defect to survive its own fix.

The five guards it brings, all live here:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is guarded separately.** ``NOT CAUGHT`` has four causes and
   the triage order is selector → fixture → code → *the layer below already
   provides the property* (prevention log, #2240 5e-5c).
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** A mutation that leaves the
   source unparseable exits 4 and would read as ``CAUGHT``.
5. ⚠ **Baseline first** — the selected test must PASS on unmutated source.

⚠⚠ THREE SOURCE FILES, AND THAT IS THE SHAPE OF THE FIX. The defect had a
producing layer (the loader carried a zero open through) and a consuming layer
(the writer treated any non-``None`` open as a price), and the prevention log's
entry on it says why both are needed: *"an obligation written in the docstring
of the module that CANNOT discharge it is the weakest form of a rule there is …
if the producing layer cannot enforce it, the consuming layer needs a runtime
refusal."* The registry is the third because the split needs a code to split
INTO, and a vocabulary that drifts from the SQL CHECK writes rows nothing reads.

⚠ TWO-SIDED PREDICATES GET ONE PROBE PER SIDE, and each probe's selector is the
test the DROPPED side makes wrong (prevention log, #2240 5e-5b — pairing a
one-sided mutation with the test that cannot see it reports NOT CAUGHT on
correct code). Both ``open_ is not None and open_ > 0`` and ``fill_open is None
or fill_open <= 0`` are probed on each half.

⚠ NOT A TEST, and it must never become one: it mutates tracked source files on
disk. CI does not run it. Everything selected here is pure-tier, so no database
is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2354_nonpositive_open.py`` or
any sibling verify script — a concurrent run reads the INJECTED source.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **sql/270's CHECK.** Reverting it means editing a migration that has already
been applied, so the mutated DDL never reaches the database and the probe would
report on a schema that does not exist. ``tests/test_strategy_registry.py``
pins the Python Literal against the migration TEXT, and that pin is probed here
instead — which is the check that actually binds.

⚠ **``price_structure.StructureBar.open`` widening to ``Decimal | None``.** It
is an annotation; reverting it changes no runtime behaviour, and what it guards
is ``pyright``, not ``pytest``. The pre-push hook runs ``pyright`` and that is
the gate that catches a re-narrowing.
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

WRITER = Path("app/services/signal_ledger.py")
LOADER = Path("app/services/research_price_structure_store.py")
REGISTRY = Path("app/services/strategy_registry.py")
SOURCES = (WRITER, LOADER, REGISTRY)

WRITER_TESTS = "tests/test_signal_ledger.py"
LOADER_TESTS = "tests/test_quarantine_sensitivity.py"
REGISTRY_TESTS = "tests/test_strategy_registry.py"
ALL_TESTS = [WRITER_TESTS, LOADER_TESTS, REGISTRY_TESTS]

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE DEFECT ITSELF, restored verbatim: the branch as it stood before
        # #2354. `open = 0` is not None, so it becomes `fill_price = 0` on a
        # fired row and every reader downstream then refuses it.
        "the zero-open half of the fill refusal removed (the #2354 defect, verbatim)",
        WRITER,
        WRITER_TESTS,
        [("if fill_open is None or fill_open <= 0:", "if fill_open is None:")],
        "test_a_zero_open_is_refused_and_never_becomes_a_fill_price",
    ),
    (
        # The OTHER side of the same predicate. Dropping the None guard does not
        # fail quietly — `None <= 0` raises — and the selector is the test that
        # supplies a NULL open, not the one that supplies a zero.
        "the NULL-open half of the fill refusal removed",
        WRITER,
        WRITER_TESTS,
        [("if fill_open is None or fill_open <= 0:", "if fill_open <= 0:")],
        "test_a_fill_bar_with_no_open_price_is_unusable_fill_price",
    ),
    (
        # ⚠ The bound narrowed to the value the corpus HAPPENS to hold. No
        # negative open is stored today, so this passes every test written
        # against real data and fails only the one written against the rule.
        "the refusal narrowed from <= 0 to == 0, admitting a negative open",
        WRITER,
        WRITER_TESTS,
        [("if fill_open is None or fill_open <= 0:", "if fill_open is None or fill_open == 0:")],
        "test_a_negative_open_is_refused",
    ),
    (
        # Criterion 8's collapse, re-introduced. The refusal still fires and no
        # bad row is stored — only the COUNT is wrong, which is precisely the
        # failure "measure what you reject" exists to prevent.
        "the two refusals collapsed back into one reason code",
        WRITER,
        WRITER_TESTS,
        [
            (
                'verdict, reason = "not_evaluable", "unusable_fill_price"',
                'verdict, reason = "not_evaluable", "no_fill_bar"',
            )
        ],
        "test_the_two_refusals_are_different_facts",
    ),
    (
        "the series edge mislabelled as an unpriceable bar (the codes swapped)",
        WRITER,
        WRITER_TESTS,
        [
            (
                '        if fill_index >= n_bars:\n            verdict, reason = "not_evaluable", "no_fill_bar"',
                "        if fill_index >= n_bars:\n"
                '            verdict, reason = "not_evaluable", "unusable_fill_price"',
            )
        ],
        "test_last_bar_is_no_fill_bar_not_a_fill",
    ),
    (
        # The producing layer, reverted to what it did before #2354.
        "the loader's open mask removed — a zero open handed to every consumer",
        LOADER,
        LOADER_TESTS,
        [("open=open_ if (admit or (open_ is not None and open_ > 0)) else None,", "open=open_,")],
        "test_a_zero_open_is_masked_even_though_no_verdict_names_it",
    ),
    (
        "the loader's bound weakened from > 0 to >= 0, so a zero open survives",
        LOADER,
        LOADER_TESTS,
        [
            (
                "open=open_ if (admit or (open_ is not None and open_ > 0)) else None,",
                "open=open_ if (admit or (open_ is not None and open_ >= 0)) else None,",
            )
        ],
        "test_a_zero_open_is_masked_even_though_no_verdict_names_it",
    ),
    (
        "the loader's NULL guard removed — the None half of the same predicate",
        LOADER,
        LOADER_TESTS,
        [
            (
                "open=open_ if (admit or (open_ is not None and open_ > 0)) else None,",
                "open=open_ if (admit or open_ > 0) else None,",
            )
        ],
        "test_a_null_open_stays_none_and_is_not_compared_against_zero",
    ),
    (
        # ⚠ The arm, not the rule. C9 defines the sensitivity arm as "admitted at
        # their stored values"; masking the open under it makes the arm measure
        # something other than the exclusion it exists to price.
        "the admitted arm masking the open too, so it no longer admits stored values",
        LOADER,
        LOADER_TESTS,
        [
            (
                "open=open_ if (admit or (open_ is not None and open_ > 0)) else None,",
                "open=open_ if (open_ is not None and open_ > 0) else None,",
            )
        ],
        "test_the_admitted_arm_still_admits_the_stored_open",
    ),
    (
        "the tenth code dropped from the vocabulary while the writer still emits it",
        REGISTRY,
        REGISTRY_TESTS,
        [('    "thin_cross_section",\n    "unusable_fill_price",\n]', '    "thin_cross_section",\n]')],
        "test_unusable_fill_price_is_ours_too",
    ),
    (
        # ⚠ Silent in the direction that matters: the code still works, and a
        # reader is told criterion 8 declared it. `PARENT_REASON_CODES` would
        # grow to eight and the parent set is what the spec is quoted against.
        "the tenth code claimed as the parent's (dropped from OUR_ADDITIONAL_REASON_CODES)",
        REGISTRY,
        REGISTRY_TESTS,
        [
            (
                'frozenset({"no_fill_bar", "thin_cross_section", "unusable_fill_price"})',
                'frozenset({"no_fill_bar", "thin_cross_section"})',
            )
        ],
        "test_parent_codes_are_the_derived_set_minus_ours",
    ),
    (
        # The pin that keeps the Python Literal and the applied CHECK together.
        # Reverting it to the superseded migration is the edit a "the test names
        # a file that moved" fix would make without noticing the schema.
        "the vocabulary pinned against the superseded migration",
        REGISTRY,
        REGISTRY_TESTS,
        [('    "unusable_fill_price",\n]', "]")],
        "test_sql_reason_codes_match_the_python_vocabulary",
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

    rc_suite = run(ALL_TESTS, "test_")
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
