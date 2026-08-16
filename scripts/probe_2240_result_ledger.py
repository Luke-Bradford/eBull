"""Revert-probe the phase-5e-1 result-ledger invariant tests (#2240).

    uv run python scripts/probe_2240_result_ledger.py

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

⚠⚠ **`store_walk_forward_folds`' own `conn.transaction()` is NOT probed, and the
NOT CAUGHT that established it is the evidence.** The probe removing it ran and
`test_the_split_writer_is_atomic_on_an_autocommit_connection` still passed —
triaged per the rule above (selector → fixture → code) and the answer was the
third one: there is no observable defect to inject. Measured on psycopg
**3.3.3** (2026-08-08) — autocommit connection, temp table with a primary key,
an `executemany` whose THIRD statement violates it — the two rows before it do
**not** survive, because `executemany` runs the batch in its own transaction.
The wrapper stays as defence in depth (see the writer's docstring); a probe for
it would report CAUGHT or NOT CAUGHT about the DRIVER, not about our code. ⚠ The
sibling probe on `store_in_sample_arm_pair` is a different case and IS here: two
separate `execute` calls have no such batching, and it is CAUGHT.
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

LEDGER = Path("app/services/result_ledger.py")
SOURCES = (LEDGER,)

PURE_TESTS = "tests/test_result_ledger.py"
DB_TESTS = "tests/test_strategy_holdout_namespace.py"
FOLD_TESTS = "tests/test_strategy_result_folds.py"

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
        # ⚠ RE-ANCHORED (#2695). The two counts were two statements when this
        # probe was written; they are now the two sub-SELECTs of
        # `_COUNT_HOLDOUT_EVALUATIONS_AND_ACCESSES` — one statement, one
        # snapshot, so a concurrent write cannot be straddled. Same defect,
        # injected inside the merged statement: point the ACCESS count at the
        # results store and both numbers come off one relation.
        "both gate counts read off the same relation (the unrecorded-access refusal goes dead)",
        LEDGER,
        DB_TESTS,
        # ⚠ TWO ONE-LINE EDITS, not one four-line span (review NITPICK, PR #2700).
        # The relation and its predicate are what make the count read the access
        # log; the two `strategy_id` / `strategy_version` lines between them are
        # shared verbatim with the sibling subquery and identify nothing. Keeping
        # them in the anchor would have let a rename of either kill this probe
        # without touching the rule — the decay #2695 exists to stop. Both lines
        # below are unique on their own indentation.
        [
            ("           FROM strategy_holdout_accesses\n", "           FROM strategy_results_store\n"),
            ("            AND access_kind = 'evaluate')", "            AND namespace = 'hold_out')"),
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
    # --- stage 5e-5c: the arm pair and the walk-forward split --------------
    (
        # ⚠ The order matters because the ADMITTED arm is never the number to
        # quote (sql/267). Dropping the label check lets a caller store the pair
        # backwards, and every stored figure then reads as the other arm's.
        "the arm labels no longer checked (the pair may be stored backwards)",
        LEDGER,
        DB_TESTS,
        [
            (
                '    if masked.identity.quarantine_arm != "masked" or admitted.identity.quarantine_arm != "admitted":',
                "    if False:",
            )
        ],
        "test_mislabelled_arms_are_refused",
    ),
    (
        # ⚠⚠ THE PAIR'S WHOLE POINT. Without this, two results differing in the
        # SCOPE (or the window, or the cost model) can be stored as criterion
        # 9's pair, and the delta between them measures that difference instead
        # of the quarantine handling.
        "the arm pair no longer required to be one measurement",
        LEDGER,
        DB_TESTS,
        [("    if expected != admitted.identity:", "    if False:")],
        "test_arms_differing_in_anything_but_the_arm_are_refused",
    ),
    (
        # ⚠ The sibling is the identity with the ARM FLIPPED. Reading the same
        # identity twice counts one row and the pair can never read as
        # compared — the gate's criterion-9 refusal then never clears.
        "the sibling identity computed without flipping the arm",
        LEDGER,
        DB_TESTS,
        [
            (
                '    sibling = replace(identity, quarantine_arm=("admitted" if identity.quarantine_arm == "masked" '
                'else "masked"))',
                "    sibling = identity",
            )
        ],
        "test_both_arms_land_and_the_pair_reads_as_compared",
    ),
    (
        # ⚠ ONE arm is not a comparison. `>= 1` makes a lone masked result read
        # as compared, which clears criterion 9's refusal on evidence that does
        # not exist.
        "a single stored arm accepted as a comparison",
        LEDGER,
        DB_TESTS,
        [("    return int(row[0]) == 2", "    return int(row[0]) >= 1")],
        "test_a_lone_arm_does_not_read_as_compared",
    ),
    (
        # ⚠ Presence is a fact about the withheld side, so looking at it is an
        # access. Dropping the record makes a hold-out look invisible to
        # criterion 5's log.
        "the hold-out pair check stopped recording its look",
        LEDGER,
        DB_TESTS,
        [('    if identity.namespace == "hold_out":', "    if False:")],
        "test_reading_the_pair_state_on_the_hold_out_records_the_look",
    ),
    (
        # ⚠⚠ THE PAIR'S ATOMICITY. Without its own transaction the guarantee is
        # the CALLER's connection mode, and on an autocommit connection the
        # masked arm commits before the admitted one is refused — the lone-arm
        # state this API exists to make unreachable. Found by Codex at
        # checkpoint 2, and the test below exists because of it.
        "the arm pair writing outside its own transaction",
        LEDGER,
        DB_TESTS,
        [
            (
                "    with conn.transaction():\n"
                "        return (store_in_sample_result(conn, masked), store_in_sample_result(conn, admitted))",
                "    return (store_in_sample_result(conn, masked), store_in_sample_result(conn, admitted))",
            )
        ],
        "test_the_pair_writer_is_atomic_on_an_autocommit_connection",
    ),
    (
        # ⚠ A write happens under TODAY's construction. Without the guard a
        # split can be stored under any label, including one whose fold count
        # or embargo rule differs from the rows beside it.
        "the walk-forward writer accepting a construction it did not implement",
        LEDGER,
        FOLD_TESTS,
        [("    if split.model_id != WALK_FORWARD_MODEL_ID:", "    if False:")],
        "test_the_writer_refuses_a_construction_it_did_not_implement",
    ),
    (
        # ⚠ One split is one construction. Without the check a mixed row set is
        # returned as a single split under whichever id the set happened to pop.
        "a split assembled from two constructions returned as one",
        LEDGER,
        FOLD_TESTS,
        [("    if len(model_ids) > 1:", "    if False:")],
        "test_a_split_whose_rows_declare_two_constructions_is_refused_on_read",
    ),
    (
        # ⚠⚠ THE 13-COLUMN MAPPING, one position out. The purge and the embargo
        # are different leaks of very different sizes (§5.3), and a stored pair
        # that swapped them would report the finding backwards while every
        # CHECK on the table still passes.
        "purged and embargoed counts swapped when writing the split",
        LEDGER,
        FOLD_TESTS,
        [
            (
                '                    "purged_count": record.census.purged,\n'
                '                    "embargoed_count": record.census.embargoed,\n',
                '                    "purged_count": record.census.embargoed,\n'
                '                    "embargoed_count": record.census.purged,\n',
            )
        ],
        "test_a_split_round_trips_through_the_table",
    ),
    (
        # The same defect on the READ side, which the write-side probe cannot
        # reach — the statement and the unpacking are independent, and only the
        # round trip pins both.
        "the two fold DATE bounds swapped when reading the split",
        LEDGER,
        FOLD_TESTS,
        [
            (
                "        first_date=first_date,  # type: ignore[arg-type]\n"
                "        last_date=last_date,  # type: ignore[arg-type]\n",
                "        first_date=last_date,  # type: ignore[arg-type]\n"
                "        last_date=first_date,  # type: ignore[arg-type]\n",
            )
        ],
        "test_a_split_round_trips_through_the_table",
    ),
    (
        "the synthetic-control match policy omitted from the durable row",
        LEDGER,
        DB_TESTS,
        [('                "synthetic_control_match_policy_id": match.policy_id,\n', "")],
        "test_a_result_carrying_the_synthetic_control_survives_the_round_trip",
    ),
    (
        "durable synthetic-control match evidence discarded on read",
        LEDGER,
        DB_TESTS,
        [("            match_quality=match_quality,\n", "            match_quality=None,\n")],
        "test_a_result_carrying_the_synthetic_control_survives_the_round_trip",
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

    rc_suite = run([PURE_TESTS, DB_TESTS, FOLD_TESTS], "test_")
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
