"""Revert-probe the §4.0 validated-universe invariant tests (#2605).

    uv run python scripts/probe_2605_validated_universe.py

⚠⚠ THE RUNNER IS IMPORTED, NOT COPIED — ``run``, ``selected`` and the two exit
codes come from ``scripts.probe_2240_cost_model``, the same harness the phase-5c
probes reuse. #2357 tracks extracting it; a fresh copy here would be a fourth
place for the loose-gate defect to survive its fix.

WHAT IS BEING PROBED, AND WHY IT NEEDED A PROBE
------------------------------------------------
``check_promotable`` refuses ``instrument_outside_validated_universe`` by
subtracting ``validated_universe_ids`` from the evaluated set, and the sole
production writer fills that set from ``load_validated_universe``. The refusal
is therefore exactly as strong as three predicates in one ``WHERE`` clause —
and before #2605 none of them had a test. Each probe below deletes one predicate
and asserts a test notices.

⚠ THESE ARE DB-BACKED TESTS. ``docker compose --profile test up -d
postgres-test`` must be running or every probe reports a harness fault, not a
miss.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. The ``finally`` restores on ``KeyboardInterrupt`` too.
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

UNIVERSE = Path("app/services/strategies/validated_universe.py")
SOURCES = (UNIVERSE,)

TESTS = "tests/test_validated_universe.py"

#: (what the injected defect IS, source file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE #2605 DEFECT. Drop this predicate and every non-US venue joins
        # the validated universe — so a UK-listed name becomes promotable
        # evidence with no refusal raised and no label changed.
        "the US-venue cut dropped (non-US instruments admitted to the validated universe)",
        UNIVERSE,
        [("      AND e.asset_class = %(asset_class)s\n", "")],
        "test_universe_admits_us_stocks_and_excludes_every_other_axis",
    ),
    (
        # §4.0's homogeneity cut: a fund's price path is a basket's, not an
        # issuer's, and S-2 ranks across the set.
        "the Stocks-type cut dropped (ETFs ranked against common stocks)",
        UNIVERSE,
        [("      AND i.instrument_type_id = %(instrument_type_id)s\n", "")],
        "test_universe_admits_us_stocks_and_excludes_every_other_axis",
    ),
    (
        # ⚠ The module's own docstring says do NOT quietly drop this to make a
        # number look better — it would widen the population without changing
        # the `survivor_only` label, which is worse than the bias.
        "is_tradable dropped (the population widens, the survivorship label does not)",
        UNIVERSE,
        [("    WHERE i.is_tradable\n", "    WHERE TRUE\n")],
        "test_universe_admits_us_stocks_and_excludes_every_other_axis",
    ),
    (
        "the ascending order reversed (callers that iterate the tuple reorder their work)",
        UNIVERSE,
        [("    ORDER BY i.instrument_id\n", "    ORDER BY i.instrument_id DESC\n")],
        "test_universe_is_returned_ascending",
    ),
    (
        # An empty lookup must RAISE. Returning nobody reads downstream as
        # "no instrument is outside the validated universe" — fail-open.
        "the missing-anchor assertion loosened (a vanished Stocks type yields an empty universe)",
        UNIVERSE,
        [("    if len(rows) != 1:", "    if len(rows) > 1:")],
        "test_stocks_type_id_raises_when_the_lookup_does_not_resolve",
    ),
    (
        "the ambiguous-anchor assertion loosened (two Stocks rows silently pick one)",
        UNIVERSE,
        [("    if len(rows) != 1:", "    if len(rows) < 1:")],
        "test_stocks_type_id_raises_when_the_description_is_ambiguous",
    ),
]


def main() -> int:
    originals = {source: source.read_text() for source in SOURCES}
    failures: list[str] = []
    try:
        for name, source, edits, selector in PROBES:
            count = selected(TESTS, selector)
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
            rc_baseline = run([TESTS], selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<24} {name}  (exit {rc_baseline})", flush=True)
                continue
            source.write_text(mutated)
            rc = run([TESTS], selector)
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
