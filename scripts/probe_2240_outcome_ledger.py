"""Revert-probe the phase-4b invariant tests (#2240).

Run from repo root (the DB probes need the test cluster up):

    docker compose --profile test up -d postgres-test
    uv run python scripts/probe_2240_outcome_ledger.py

Spec: ``docs/proposals/ta/2026-08-06-outcome-ledger.md`` §5. Sister to
``scripts/probe_2240_outcome_resolver.py`` (4a), whose two guards apply
unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything — `X` → `A if False
   else X` passes guard 1 and proves nothing (prevention log, #2240 4a).

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **CASCADE.** The behaviour lives in ``sql/256``, and the test database is
built from a migrated template — editing the migration file does not change an
already-built template, so a source-edit probe would report NOT CAUGHT for a
constraint that is present and working. Its test
(``test_deleting_a_signal_removes_its_outcomes``) is exercised against the real
FK instead, which is the evidence that matters. Stated rather than left as a
gap somebody re-derives.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# ⚠ The gate constants live ONCE, in the 5b reference harness. A second
# hand-written copy of "exit 1 means the test failed" is how this file's
# gate drifted from it in the first place (#2357).
from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED

SRC = Path("app/services/outcome_ledger.py")
PURE = "tests/test_outcome_ledger.py"
DB = "tests/test_outcome_ledger_db.py"

#: (what the injected defect IS, [(anchor, replacement), ...], test file, -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str, str]] = [
    (
        # ⚠ THE 3c DEFECT, injected. On a booked outcome the ANDed form still
        # rejects a half location, so only the unresolved case can see it —
        # which is exactly why that case had to be written down rather than
        # inferred from what the writer emits.
        "exit location ANDed instead of counted (admits half a location)",
        [
            (
                "        located = (self.exit_bar_date is not None) + (self.bars_held is not None)\n"
                '        if located != (0 if self.outcome == "unresolved" else 2):',
                "        located = self.exit_bar_date is not None and self.bars_held is not None\n"
                '        if located != (self.outcome != "unresolved"):',
            )
        ],
        PURE,
        "test_a_half_exit_location_on_an_unresolved_row",
    ),
    (
        "booked pair ANDed instead of counted (admits half a pair)",
        [
            (
                "        booked = (self.exit_price is not None) + (self.gross_return_pct is not None)\n"
                "        if booked != (2 if self.outcome in _BOOKED else 0):",
                "        booked = self.exit_price is not None and self.gross_return_pct is not None\n"
                "        if booked != (self.outcome in _BOOKED):",
            )
        ],
        PURE,
        "test_half_a_booked_pair_on_an_ambiguous_outcome or test_half_a_booked_pair_on_an_unresolved_outcome",
    ),
    (
        "a blank version accepted as a key member",
        [
            (
                "        if not self.rule_set_version or not self.input_rule_set_version:",
                "        if False:",
            )
        ],
        PURE,
        "test_a_blank_version_is_present_and_meaningless",
    ),
    (
        "a stored fill date the corpus no longer holds lands on the nearest bar",
        [
            (
                "        return series.dates.index(fill_bar_date)",
                "        return next((i for i, d in enumerate(series.dates) if d >= fill_bar_date), 0)",
            )
        ],
        PURE,
        "test_a_date_the_corpus_no_longer_holds_raises",
    ),
    (
        "the INSERT stops requiring a FIRED parent",
        [
            (
                "      AND s.signal_kind = 'entry'\n      AND s.verdict = 'fired'\n"
                "      AND (%(exit_bar_date)s::date IS NULL",
                "      AND s.signal_kind = 'entry'\n      AND (%(exit_bar_date)s::date IS NULL",
            )
        ],
        DB,
        # ⚠ The selector is the UNRESOLVED case, not the general one. With a
        # BOOKED outcome the surviving `exit_bar_date >= s.fill_bar_date` clause
        # refuses a not_fired parent anyway (its fill date is NULL), so
        # `test_it_raises_and_writes_nothing` reports CAUGHT for a defect it did
        # not detect. This probe reported NOT CAUGHT until that case was
        # written — which is the harness doing its job.
        "test_an_unresolved_outcome_cannot_attach_to_an_unfilled_parent",
    ),
    (
        "the INSERT stops requiring the exit to follow the fill",
        [
            (
                "\n      AND (%(exit_bar_date)s::date IS NULL OR %(exit_bar_date)s::date >= s.fill_bar_date)",
                "",
            )
        ],
        DB,
        "test_an_exit_before_its_fill_is_refused",
    ),
    (
        # ⚠ A short count is the SILENT half of the parent predicate: zero rows
        # insert, the writer returns 0, and the caller reports success having
        # dropped an outcome.
        "a shortfall returned as a count instead of raised",
        [("        if written != len(rows):", "        if False:")],
        DB,
        "test_a_partial_batch_raises_rather_than_returning_short",
    ),
    (
        # The anti-join keyed on the signal alone: any outcome at ANY version
        # hides the fill, so a resolver or quarantine bump can never re-resolve.
        "the pending anti-join drops the version predicates",
        [
            (
                "          AND o.rule_set_version = %(rule_set_version)s\n"
                "          AND o.input_rule_set_version = %(input_rule_set_version)s\n",
                "",
            )
        ],
        DB,
        "test_a_fill_resolved_at_a_different_version_is_pending_again",
    ),
]


def run(test_file: str, selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", test_file, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def main() -> int:
    original = SRC.read_text()
    failures: list[str] = []
    try:
        for name, edits, test_file, selector in PROBES:
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
            rc_baseline = run(test_file, selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<20} {name}  (exit {rc_baseline})", flush=True)
                continue
            SRC.write_text(mutated)
            rc = run(test_file, selector)
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

    rc_pure = run(PURE, "test_")
    rc_db = run(DB, "test_")
    print(f"\n  restored pure suite: {'PASS' if rc_pure == 0 else '*** FAIL ***'}", flush=True)
    print(f"  restored db suite  : {'PASS' if rc_db == 0 else '*** FAIL ***'}", flush=True)
    if rc_pure or rc_db:
        failures.append("restored suite does not pass")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
