"""Revert-probe the phase-5e-1 result-ledger invariant tests (#2240).

    PYTHONPATH=. uv run python scripts/probe_2240_result_ledger.py

⚠⚠ THE RUNNER IS IMPORTED, NOT COPIED — ``run``, ``selected`` and the two
exit-code constants come from ``scripts.probe_2240_cost_model``, the harness
#2214 hardened. #2357 tracks extracting it to ``scripts/probe_harness.py``; a
fifth verbatim copy would be a fifth place for the loose-gate defect to survive
its own fix.

The five guards it brings, all live here:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing and reports ``CAUGHT``
   for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is guarded separately.** Triage order for ``NOT CAUGHT`` is
   selector → fixture → code.
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** Exit 4 is a pytest USAGE
   error and 2 is a collection error; both would read as ``CAUGHT``.
5. ⚠ **Baseline first** — the selected test must PASS on unmutated source.

⚠ HALF THESE PROBES ARE DB-TIER, which is new for this family and is why each
probe names its own test file. ``run`` already passes ``-n 0``, so the template
race that bit stage 5c does not apply — but the test Postgres must be up::

    docker compose --profile test up -d postgres-test

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠⚠ **``sql/264``'s VIEW FILTER, CHECK OPTION and TRIGGER.** They are DDL, and
reverting one means editing a migration that has already been applied — the
mutated DDL would never reach the database, so the probe would report ``NOT
CAUGHT`` for a defect it never injected. Stage 5c reached the same conclusion
for the ``strategy_results`` constraints.

They are not unguarded. Each is exercised directly against a real Postgres in
``tests/test_strategy_holdout_namespace.py``, and one of those tests is itself a
revert probe of the trigger:
``test_the_two_counts_read_different_relations`` DISABLES it, writes the row it
was refusing, and asserts the gate then fires — the only in-repo test that
injects a schema-level defect and observes the consequence.

⚠ **The float ↔ NUMERIC boundary IS probed** (two entries below) and is the one
that would otherwise fail silently: a lossy conversion stores a number that is
not the number that was computed, and every downstream reader agrees with the
stored one.
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

LEDGER = Path("app/services/result_ledger.py")
SOURCES = (LEDGER,)

PURE_TESTS = "tests/test_result_ledger.py"
DB_TESTS = "tests/test_strategy_holdout_namespace.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    # --- the storage boundary ---------------------------------------------
    (
        # ⚠⚠ THE SILENT ONE. Six decimal places looks generous and truncates
        # every metric; nothing downstream can tell a rounded Sharpe from a
        # computed one, because the stored value IS what every reader sees.
        "the float→NUMERIC conversion made lossy (metrics stored rounded)",
        LEDGER,
        PURE_TESTS,
        [
            (
                "    return None if value is None else Decimal(repr(value))",
                '    return None if value is None else Decimal(f"{value:.6f}")',
            )
        ],
        "test_every_float_survives_the_trip",
    ),
    (
        # Round-trips through float() just fine, so the round-trip test alone
        # does NOT catch it — Decimal(0.1) is 55 significant digits and would
        # store as such. The discriminating assertion is the separate one.
        "Decimal(value) for Decimal(repr(value)) — the full binary expansion stored",
        LEDGER,
        PURE_TESTS,
        [
            (
                "    return None if value is None else Decimal(repr(value))",
                "    return None if value is None else Decimal(value)",
            )
        ],
        "test_repr_and_not_the_binary_expansion",
    ),
    # --- the access record must actually record ---------------------------
    (
        "the blank-field check removed (an access record with no actor and no purpose)",
        LEDGER,
        PURE_TESTS,
        [
            (
                '        for field_name in ("strategy_id", "strategy_version", "accessed_by", "purpose"):\n'
                "            if not getattr(self, field_name):\n",
                "        for field_name in ():\n            if not getattr(self, field_name):\n",
            )
        ],
        "test_a_blank_identity_field_is_refused",
    ),
    (
        # ⚠ sql/264's CHECK mirrored in Python. An unversioned evaluate record
        # matches no row through the trigger, so the mistake surfaces as a
        # confusing refusal three statements later instead of here.
        "the evaluate-names-its-result check removed",
        LEDGER,
        PURE_TESTS,
        [('        if self.access_kind == "evaluate" and self.result_version is None:', "        if False:")],
        "test_an_evaluate_must_name_the_result_it_authorises",
    ),
    (
        "a blank result_version read as absent rather than refused",
        LEDGER,
        PURE_TESTS,
        [("        if self.result_version is not None and not self.result_version:", "        if False:")],
        "test_a_blank_result_version_is_refused",
    ),
    # --- the two writers --------------------------------------------------
    (
        "the in-sample writer's namespace guard removed",
        LEDGER,
        PURE_TESTS,
        [('    if result.identity.namespace != "in_sample":', "    if False:")],
        "test_in_sample_writer_refuses_a_hold_out_result",
    ),
    (
        "the hold-out writer's namespace guard removed (an in-sample write logs a hold-out access)",
        LEDGER,
        PURE_TESTS,
        [('    if result.identity.namespace != "hold_out":', "    if False:")],
        "test_hold_out_writer_refuses_an_in_sample_result",
    ),
    # --- criterion 5's records --------------------------------------------
    (
        # ⚠ The DEFECT here is caught by the DATABASE, not by this module: the
        # trigger refuses the row. The probe is what proves the test would
        # notice, rather than passing because nothing was stored either way.
        "the hold-out write stopped recording its evaluation",
        LEDGER,
        DB_TESTS,
        [
            (
                "    record_holdout_access(\n"
                "        conn,\n"
                "        HoldoutAccess(\n"
                "            strategy_id=result.identity.strategy_id,\n",
                "    _unused = (\n"
                "        conn,\n"
                "        HoldoutAccess(\n"
                "            strategy_id=result.identity.strategy_id,\n",
            )
        ],
        "test_an_exploratory_select_cannot_see_a_hold_out_result",
    ),
    (
        "the hold-out READ stopped recording its access",
        LEDGER,
        DB_TESTS,
        [
            (
                "    record_holdout_access(\n"
                "        conn,\n"
                "        HoldoutAccess(\n"
                "            strategy_id=strategy_id,\n",
                "    _unused = (\n        conn,\n        HoldoutAccess(\n            strategy_id=strategy_id,\n",
            )
        ],
        "test_a_read_is_recorded_even_when_it_returns_nothing",
    ),
    (
        # ⚠⚠ THE TAUTOLOGY. Count both numbers off the access log and
        # `recorded_accesses < holdout_evaluations` can never be true, so the
        # gate's refusal becomes unreachable — passing for the wrong reason
        # rather than failing.
        "both gate counts read off the same relation (the unrecorded-access refusal goes dead)",
        LEDGER,
        DB_TESTS,
        [
            (
                "    evaluations = conn.execute(_COUNT_HOLDOUT_RESULTS, params).fetchone()",
                "    evaluations = conn.execute(_COUNT_EVALUATE_ACCESSES, params).fetchone()",
            )
        ],
        "test_the_two_counts_read_different_relations",
    ),
    (
        "the hold-out read stopped scoping to the withheld side (in-sample rows returned too)",
        LEDGER,
        DB_TESTS,
        [("      AND namespace = 'hold_out'\n    ORDER BY", "    ORDER BY")],
        "test_the_holdout_read_returns_only_the_withheld_side",
    ),
    # --- the 39-column mapping --------------------------------------------
    (
        # ⚠ Two same-typed NUMERIC columns swapped on the WRITE side. Every
        # constraint still passes, every count is right, and the stored Sharpe
        # is the Sortino.
        "sharpe and sortino swapped when writing",
        LEDGER,
        DB_TESTS,
        [('        "sharpe": _numeric(metrics.sharpe),', '        "sharpe": _numeric(metrics.sortino),')],
        "test_the_round_trip_preserves_the_whole_result",
    ),
    (
        # The same defect on the READ side, which the write-side probe cannot
        # reach: the two lists are independent and only the round trip pins both.
        "losing_trade_count and losing_period_count swapped when reading",
        LEDGER,
        DB_TESTS,
        [
            (
                "        losing_trade_count=int(losing_trade_count),  # type: ignore[arg-type]\n"
                "        losing_period_count=int(losing_period_count),  # type: ignore[arg-type]\n",
                "        losing_trade_count=int(losing_period_count),  # type: ignore[arg-type]\n"
                "        losing_period_count=int(losing_trade_count),  # type: ignore[arg-type]\n",
            )
        ],
        "test_the_round_trip_preserves_the_whole_result",
    ),
    (
        "the identity-hash mismatch refusal removed (a row may claim an identity that is not its own)",
        LEDGER,
        DB_TESTS,
        [("    if identity.version != result_version:", "    if False:")],
        "test_a_stored_row_whose_hash_does_not_describe_it_is_refused",
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

    rc_suite = run([PURE_TESTS, DB_TESTS], "test_")
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
