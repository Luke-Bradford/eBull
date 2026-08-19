"""Revert-probe the phase-5c result-model invariant tests (#2240).

    uv run python scripts/probe_2240_result_model.py

⚠⚠ THE RUNNER IS IMPORTED, NOT COPIED. ``run``, ``selected`` and the two exit-code
constants come from ``scripts.probe_2240_cost_model``, which is the harness
#2214's entry hardened and #2357 tracks sweeping across the siblings. A fourth
verbatim copy would be a fourth place for the loose-gate defect to survive the
fix — and the defect history of this exact harness is in the prevention log
twice. Extracting it to a shared ``scripts/probe_harness.py`` is #2357's, not
this diff's.

The five guards it brings, all live here:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is guarded separately.** ``NOT CAUGHT`` has three causes and
   the triage order is selector → fixture → code (prevention log, #2240 S-2).
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** A mutation that leaves the
   source unparseable exits 4 and would read as ``CAUGHT``.
5. ⚠ **Baseline first** — the selected test must PASS on unmutated source.

⚠ TWO SOURCE FILES, and the second is deliberate. ``strategy_result`` holds the
frozen literals; ``verify_2240_result_model`` holds ``_derive_boundary``, which
is the INDEPENDENT re-derivation those literals are checked against. A reference
nobody probes is the prevention log's *"independent verifier that is only
ACCIDENTALLY right"* — if it conflates the selection rule with the split rule it
agrees with a module doing the same, and ``--frozen`` reports PASS on a leak.

⚠ NOT A TEST, and it must never become one: it mutates tracked source files on
disk. CI does not run it. Everything selected here is pure-tier, so no database
is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2240_result_model.py`` or any
sibling verify script. Phase 4b's lesson: a concurrent run stamps its output
with the INJECTED source hash, and a start-vs-end check misses it because the
probe restores the file.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **The ``strategy_results`` CONSTRAINTS.** They live in SQL, and reverting one
means editing a migration that has already been applied — the mutated DDL would
never reach the database. ``tests/test_strategy_results_table.py`` exercises
them against a real Postgres instead, which is the check that actually binds.

⚠ **``CURRENT_RESULT_PROVENANCE``.** Reported, not enforced; nothing branches on
it, so there is no behaviour to revert.
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

MODEL = Path("app/services/strategy_result.py")
VERIFY = Path("scripts/verify_2240_result_model.py")
SOURCES = (MODEL, VERIFY)

TESTS = "tests/test_strategy_result.py"

#: (what the injected defect IS, source file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE 4,021-BAR LEAK. `>=` makes the boundary date the FIRST HOLD-OUT
        # bar; `>` moves its own bars into training. 0.02% of the corpus and
        # invisible in every summary statistic.
        "the boundary date's own bars moved into training (>= became >)",
        MODEL,
        [
            (
                '    return "hold_out" if bar_date >= HOLDOUT_BOUNDARY else "in_sample"',
                '    return "hold_out" if bar_date > HOLDOUT_BOUNDARY else "in_sample"',
            )
        ],
        "test_the_boundary_bar_itself_is_hold_out",
    ),
    (
        "the frozen boundary silently walked forward one day",
        MODEL,
        [("HOLDOUT_BOUNDARY = date(2021, 6, 29)", "HOLDOUT_BOUNDARY = date(2021, 6, 30)")],
        "test_the_boundary_is_the_spec_boundary",
    ),
    (
        # §5.2: a signal decided in-sample and filled on the withheld side needs
        # a price from that side. Assigning it to the signal's own namespace
        # imports a hold-out price into a training number.
        "the purge removed — a straddling signal assigned to its signal's side",
        MODEL,
        [('    return "purged"', "    return signal_side")],
        "test_a_signal_decided_in_sample_but_filled_on_the_boundary_is_purged",
    ),
    (
        # ⚠ An open position is marked at the END of the evaluation window,
        # which is on the withheld side. "No close" is not "no span".
        "an open position credited to the in-sample arm",
        MODEL,
        [
            (
                '    if close_bar_date is None:\n        return "hold_out"\n',
                '    if close_bar_date is None:\n        return "in_sample"\n',
            )
        ],
        "test_an_open_position_entered_in_sample_is_hold_out",
    ),
    (
        "a position's namespace keyed on its ENTRY instead of its close (spanning ignored)",
        MODEL,
        [("    return namespace_for_bar(close_bar_date)", "    return namespace_for_bar(entry_fill_bar_date)")],
        "test_a_position_spanning_the_boundary_is_hold_out",
    ),
    (
        # Review NITPICK, PR #2360. Unreachable through `position_builder`, but
        # the function is public and takes two bare dates — and its sibling
        # `namespace_for_signal` already refuses ITS corrupt pair.
        "the reversed entry/close refusal removed (a backwards position answers on the close)",
        MODEL,
        [
            (
                "    if close_bar_date < entry_fill_bar_date:\n"
                "        raise ValueError(\n"
                '            f"position closes {close_bar_date} before its entry fill {entry_fill_bar_date} — '
                'a reversed pair has "\n'
                '            "no namespace, and answering on the close alone would be a verdict with no signal '
                'attached"\n'
                "        )\n",
                "",
            )
        ],
        "test_a_close_before_its_entry_raises",
    ),
    (
        # ⚠ The discriminator: sql/256 says `bars_held = 0` IS LEGAL, so a
        # same-bar open-and-close is a real trade. `<=` would reject it.
        "the reversed-pair guard made inclusive (a legal same-bar trade is refused)",
        MODEL,
        [("    if close_bar_date < entry_fill_bar_date:", "    if close_bar_date <= entry_fill_bar_date:")],
        "test_a_same_bar_open_and_close_is_allowed",
    ),
    (
        "the promotable allowlist widened to admit survivor_only",
        MODEL,
        [
            (
                'PROMOTABLE_UNIVERSE_BASES: frozenset[str] = frozenset({"survivorship_free"})',
                'PROMOTABLE_UNIVERSE_BASES: frozenset[str] = frozenset({"survivorship_free", "survivor_only"})',
            )
        ],
        "test_survivor_only_is_refused",
    ),
    (
        # ⚠⚠ THE FAIL-CLOSED INVERSION. A denylist enumerating the bad values
        # lets every value nobody anticipated — including a typo — straight
        # through. The allowlist is one member and everything else refuses.
        "the basis allowlist turned into a denylist (a typo'd basis promotes)",
        MODEL,
        [
            (
                "    elif universe_basis not in PROMOTABLE_UNIVERSE_BASES:",
                '    elif universe_basis == "survivor_only":',
            )
        ],
        "test_an_unrecognised_basis_is_refused_not_raised",
    ),
    (
        "the carry refusal removed (§5.1's unmodelled carry promotes)",
        MODEL,
        [('    if carry_unmodelled:\n        refusals.append("carry_unmodelled")\n', "")],
        "test_carry_unmodelled_is_refused",
    ),
    (
        # ⚠ `set() - anything` is empty, so folding the empty case into the
        # subset test makes a result over NO instruments pass vacuously.
        "the empty-evaluated-set guard folded into the subset test (no evidence promotes)",
        MODEL,
        [
            (
                "    if not candidate.evaluated_instrument_ids and not candidate.evaluated_series_ids:\n"
                '        refusals.append("no_instruments_evaluated")\n'
                "    elif candidate.evaluated_instrument_ids - candidate.validated_universe_ids:",
                "    if candidate.evaluated_instrument_ids - candidate.validated_universe_ids:",
            )
        ],
        "test_an_empty_evaluated_set_is_refused_rather_than_passing_vacuously",
    ),
    (
        "the holdout evidence-window registry replay removed (an invented label promotes)",
        MODEL,
        [
            (
                '    if identity.namespace == "hold_out":\n',
                '    if False and identity.namespace == "hold_out":\n',
            )
        ],
        "test_holdout_axis_requires_the_exact_registered_evidence_window",
    ),
    (
        "the §4.0 universe membership check removed",
        MODEL,
        [
            (
                "    elif candidate.evaluated_instrument_ids - candidate.validated_universe_ids:\n"
                '        refusals.append("instrument_outside_validated_universe")\n',
                "",
            )
        ],
        "test_an_instrument_outside_the_validated_universe_is_refused",
    ),
    (
        "the never-evaluated hold-out admitted (< 1 became < 0)",
        MODEL,
        [("    if holdout_evaluations < 1:", "    if holdout_evaluations < 0:")],
        "test_a_never_evaluated_holdout_is_refused",
    ),
    (
        # ⚠⚠ THIS INJECTS CRITERION 5'S LITERAL WORDING — "evaluated MORE THAN
        # ONCE without a recorded access" — which lets a single unrecorded look
        # at the hold-out through. That is the same governance failure, just the
        # first one, and it is why the shipped rule is stricter than the words.
        "the access rule weakened to criterion 5's literal wording (one unrecorded look passes)",
        MODEL,
        [
            (
                "    if recorded_accesses < holdout_evaluations:",
                "    if holdout_evaluations > 1 and recorded_accesses < holdout_evaluations:",
            )
        ],
        "test_an_evaluation_without_a_recorded_access_is_refused",
    ),
    (
        "the Deflated Sharpe refusal removed (criterion 6)",
        MODEL,
        [('    if deflated_sharpe is None:\n        refusals.append("deflated_sharpe_not_computed")\n', "")],
        "test_a_missing_deflated_sharpe_is_refused",
    ),
    (
        "the trial-count refusal removed (an undeclared count promotes)",
        MODEL,
        [('    if trial_count is None:\n        refusals.append("trial_count_undeclared")\n', "")],
        "test_an_undeclared_trial_count_is_refused_even_with_a_deflated_sharpe",
    ),
    (
        # ⚠ "Not measured" and "measured and bad" are different states.
        # Collapsing them lets an uncomputed comparison read as a clean one.
        "the uncompared-ambiguity state collapsed into the material one",
        MODEL,
        [
            (
                "    if candidate.ambiguity_material is None:\n"
                '        refusals.append("ambiguity_arms_not_compared")\n'
                "    elif candidate.ambiguity_material:",
                "    if candidate.ambiguity_material:",
            )
        ],
        "test_an_uncompared_ambiguity_pair_is_refused",
    ),
    (
        # ⚠ ANCHORED ON WHAT FOLLOWS, not on what precedes. `return
        # tuple(refusals)` has FOUR sites since #2639 split the gate into
        # per-criterion helpers, and the one this probe means is
        # `check_promotable`'s own. Trailing context survives a new refusal
        # block being appended INSIDE the function — which is how the gate
        # grows — whereas anchoring on the preceding statement would die on
        # exactly that edit (#2695).
        "the gate short-circuited to its first refusal",
        MODEL,
        [
            (
                "    return tuple(refusals)\n\n\ndef is_promotable(candidate: PromotionCandidate) -> bool:",
                "    return tuple(refusals[:1])\n\n\ndef is_promotable(candidate: PromotionCandidate) -> bool:",
            )
        ],
        "test_every_refusal_is_returned_not_just_the_first",
    ),
    (
        "is_promotable inverted",
        MODEL,
        [("    return not check_promotable(candidate)", "    return bool(check_promotable(candidate))")],
        "test_a_fully_clean_candidate_is_promotable",
    ),
    (
        # C11's named case: "a sizing change that did not move the version would
        # let a different strategy inherit a track record".
        "the sizing rule dropped from the result identity hash",
        MODEL,
        [('            "sizing_rule": self.sizing_rule,\n', "")],
        "test_the_sizing_rule_moves_it",
    ),
    (
        "the ambiguity arm dropped from the result identity hash (§3.4's two arms collide)",
        MODEL,
        [('            "ambiguity_arm": self.ambiguity_arm,\n', "")],
        "test_the_ambiguity_arm_moves_it",
    ),
    (
        # The #2286 shape: a present-but-empty identity field is not caught by
        # NOT NULL and silently merges two results into one bucket.
        "the blank-identity-field refusal removed",
        MODEL,
        [
            (
                "            if not getattr(self.identity, field_name):\n"
                '                raise ValueError(f"{field_name} is blank — a present-but-empty identity field '
                'merges two results")\n',
                "            pass\n",
            )
        ],
        "test_a_blank_identity_field_is_refused",
    ),
    (
        "`purged` admitted as a result namespace (§5.2 gives a purged signal a result)",
        MODEL,
        [
            (
                "RESULT_NAMESPACES: frozenset[str] = frozenset(get_args(ResultNamespace))",
                'RESULT_NAMESPACES: frozenset[str] = frozenset(get_args(ResultNamespace)) | {"purged"}',
            )
        ],
        "test_purged_is_not_a_result_namespace",
    ),
    (
        "a zero trial count admitted (criterion 6 counts abandoned branches, so zero is unreachable)",
        MODEL,
        [
            (
                "        if self.trial_count is not None and self.trial_count < 1:",
                "        if self.trial_count is not None and self.trial_count < 0:",
            )
        ],
        "test_a_zero_trial_count_is_refused",
    ),
    (
        # ⚠⚠ THE SAME 4,021-BAR DEFECT, IN THE VERIFIER. If the reference
        # conflates the two rules it agrees with a module that does the same,
        # and `--frozen` reports PASS on the leak.
        "the verifier conflating the selection rule with the split rule",
        VERIFY,
        [("            in_sample = running - count", "            in_sample = running")],
        "test_the_boundary_date_s_own_bars_are_hold_out",
    ),
    (
        "the verifier's boundary selection made non-strict (an exact 75% empties the in-sample arm)",
        VERIFY,
        [("        if Decimal(running) > threshold:", "        if Decimal(running) >= threshold:")],
        "test_the_selection_rule_needs_a_STRICT_exceedance",
    ),
    (
        "the corpus version stripped of its frozen last bar (a re-freeze becomes invisible)",
        MODEL,
        [
            (
                "CORPUS_VERSION = f\"{'+'.join(CORPUS_VENDORS)}@{CORPUS_FROZEN_LAST_BAR.isoformat()}\"",
                "CORPUS_VERSION = f\"{'+'.join(CORPUS_VENDORS)}\"",
            )
        ],
        "test_the_corpus_version_names_the_vendor_and_the_frozen_last_bar",
    ),
    (
        "legacy synthetic controls admitted without durable match evidence",
        MODEL,
        [('        refusals.append("synthetic_control_match_evidence_missing")\n', "")],
        "test_a_legacy_control_without_match_evidence_is_refused",
    ),
    (
        "an unknown synthetic-control match policy admitted",
        MODEL,
        [('            refusals.append("synthetic_control_match_policy_unrecognised")\n', "")],
        "test_an_unknown_match_policy_is_refused",
    ),
    (
        "a mismatched synthetic-control population admitted",
        MODEL,
        [('            refusals.append("synthetic_control_population_mismatch")\n', "")],
        "test_every_match_dimension_is_checked_independently",
    ),
    (
        "a synthetic-control exposure residual admitted",
        MODEL,
        [('            refusals.append("synthetic_control_exposure_mismatch")\n', "")],
        "test_every_match_dimension_is_checked_independently",
    ),
    (
        "a synthetic-control turnover residual admitted",
        MODEL,
        [('            refusals.append("synthetic_control_turnover_mismatch")\n', "")],
        "test_every_match_dimension_is_checked_independently",
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
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
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
