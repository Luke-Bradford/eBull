"""Revert-probe the S-2 and cross-sectional-contract invariant tests (#2240).

    uv run python scripts/probe_2240_s2_cross_sectional.py

Sister to ``scripts/probe_2240_s1_momentum.py``, whose two guards apply
unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**

⚠ NOT A TEST, and it must never become one: it mutates tracked source files on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

⚠ TWO SOURCE FILES. Half of S-2's invariants live in the *contract*
(``strategy_registry``) rather than in the strategy — the thin-cross-section
refusal, the participants-must-be-offered check, evaluability-before-ranking and
the date grouping. Probing only the strategy would leave the half that S-5/S-6
will inherit unprobed.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **The ``score`` must be a declared input.** It IS probed (the check is one
line), but note what the probe demonstrates: deleting the check does not change
a verdict on any fixture where the score is also reachable through another
declared input. The test that catches it constructs a member whose score is NOT
declared, which is the only shape where the two differ.

⚠ **"The panel calendar, not the member's own"** has no one-line inversion:
``rebalance_dates`` is handed a calendar by its caller, so the reading lives in
``s2_signals``/the census, not in a branch. The probe below instead deletes the
YEAR from the month comparison, which is the failure the reading is exposed to.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

STRATEGY = Path("app/services/strategies/s2_cross_sectional_momentum.py")
REGISTRY = Path("app/services/strategy_registry.py")
S2_TESTS = "tests/test_strategy_s2.py"
REGISTRY_TESTS = "tests/test_strategy_registry.py"

#: (what the injected defect IS, [(file, anchor, replacement), ...], test file, -k selector)
PROBES: list[tuple[str, list[tuple[Path, str, str]], str, str]] = [
    (
        "the skip month dropped — the score runs to t instead of t-21",
        [(STRATEGY, "        recent = closes[index - SKIP_BARS]", "        recent = closes[index]")],
        S2_TESTS,
        "test_matches_the_naive_reference_at_every_bar",
    ),
    (
        "the eligibility gate loosened by one bar",
        [(STRATEGY, "        if index < ELIGIBILITY_BARS - 1:", "        if index < ELIGIBILITY_BARS - 2:")],
        S2_TESTS,
        "test_the_eligibility_boundary_is_exact",
    ),
    (
        "the non-positive close guard dropped (a zero denominator, a negative return that ranks like a winner)",
        [
            (
                STRATEGY,
                "        if past is None or recent is None or past <= 0.0 or recent <= 0.0:",
                "        if past is None or recent is None:",
            )
        ],
        S2_TESTS,
        "test_a_non_positive_window_close_is_refused_not_divided_by",
    ),
    (
        "a holed lookback window reported as warm-up rather than a data gap",
        [(STRATEGY, "            unevaluable.append(index)", "            pass")],
        S2_TESTS,
        "test_a_masked_window_close_is_a_data_gap",
    ),
    (
        "§9 Q3's price floor dropped — sub-$1 tick-quantised names back in the ranking",
        [
            (
                STRATEGY,
                "            if when in panel_rebalance_dates and (close := closes[index]) is not None "
                "and close >= MIN_CLOSE",
                "            if when in panel_rebalance_dates and closes[index] is not None",
            )
        ],
        S2_TESTS,
        "test_a_sub_dollar_close_is_not_a_decision_bar",
    ),
    (
        "the price floor made strict — a name at exactly $1.00 excluded",
        [(STRATEGY, "and close >= MIN_CLOSE", "and close > MIN_CLOSE")],
        S2_TESTS,
        "test_the_floor_is_inclusive_at_one_dollar",
    ),
    (
        "the decision bar's own close no longer declared — a quarantined bar gets ranked",
        [
            (
                STRATEGY,
                "            StrategyInput(series=_close_input(series, universe=universe), reason=close_reason),\n",
                "",
            )
        ],
        S2_TESTS,
        "test_a_masked_close_at_the_decision_bar_refuses_the_bar",
    ),
    (
        "the decile cut rounded up instead of down",
        [(STRATEGY, "    count = len(scores) // DECILE", "    count = -(-len(scores) // DECILE)")],
        S2_TESTS,
        "test_the_cut_is_a_floor",
    ),
    (
        "the tie-break dropped — the cut becomes dict-insertion order",
        [
            (
                STRATEGY,
                "    ordered = sorted(scores.items(), key=lambda item: (-item[1], item[0]))",
                "    ordered = sorted(scores.items(), key=lambda item: -item[1])",
            )
        ],
        S2_TESTS,
        # ⚠ NOT `test_insertion_order_does_not_change_the_answer`, which was the
        # first selector here and reported NOT CAUGHT: its scores are DISTINCT,
        # so a stable sort orders them identically with or without the
        # tie-break. Only the all-equal fixture, built highest-id-first,
        # separates the two.
        "test_ties_break_on_the_lower_instrument_id",
    ),
    (
        "the rebalance trigger fires on every bar, not on the month change",
        [
            (
                STRATEGY,
                "        if (when.year, when.month) != (previous.year, previous.month)",
                "        if when != previous",
            )
        ],
        S2_TESTS,
        "test_only_the_first_bar_of_each_month",
    ),
    (
        "the year dropped from the month comparison — a gap year reads as the same month",
        [
            (
                STRATEGY,
                "        if (when.year, when.month) != (previous.year, previous.month)",
                "        if when.month != previous.month",
            )
        ],
        S2_TESTS,
        "test_the_same_month_a_year_apart_is_a_change",
    ),
    (
        "the panel's minimum cross-section dropped to one — a decile of six names",
        [(STRATEGY, "        min_participants=MIN_CROSS_SECTION,", "        min_participants=1,")],
        S2_TESTS,
        "test_a_thin_cross_section_is_refused_not_reported_as_not_fired",
    ),
    (
        "the source hash frozen to a constant (an edited rule inherits the track record)",
        [
            (
                STRATEGY,
                "    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]",
                '    return "0" * 12',
            )
        ],
        S2_TESTS,
        "test_the_source_hash_is_this_strategys_own_source",
    ),
    (
        "the price floor dropped from the hashed parameters",
        [(STRATEGY, '    "min_close": MIN_CLOSE,\n', "")],
        S2_TESTS,
        "test_params_carry_every_constant_into_the_identity",
    ),
    (
        "a blank cost-model declaration accepted into the identity hash",
        [(STRATEGY, "    if not cost_model_id.strip():", "    if False:")],
        S2_TESTS,
        "test_a_blank_cost_model_is_rejected",
    ),
    # ---- the contract half ----
    (
        "the contract stops requiring the score to be a declared input",
        [
            (
                REGISTRY,
                "        if not any(declared.series is self.score for declared in self.inputs):",
                "        if False:",
            )
        ],
        REGISTRY_TESTS,
        "test_the_score_must_be_declared_as_an_input",
    ),
    (
        "evaluability no longer checked before ranking",
        [(REGISTRY, "        reason = _unevaluable_reason_at(member.inputs, index)", "        reason = None")],
        REGISTRY_TESTS,
        "test_an_unevaluable_input_refuses_the_bar_before_it_is_ranked",
    ),
    (
        "the last bar of a member becomes a decidable bar",
        [(REGISTRY, "        if index == n - 1:", "        if False:")],
        REGISTRY_TESTS,
        "test_the_last_bar_is_no_fill_even_at_a_decision_bar",
    ),
    (
        "the thin-cross-section refusal removed — the panel ranks whatever it has",
        [(REGISTRY, "        if len(scores) < min_participants:", "        if False:")],
        REGISTRY_TESTS,
        "test_a_thin_cross_section_is_refused_and_select_is_never_called",
    ),
    (
        "a winner that never participated is accepted instead of raising",
        [(REGISTRY, "        if unknown:", "        if False:")],
        REGISTRY_TESTS,
        "test_a_winner_that_did_not_participate_raises",
    ),
    (
        "the cross-section keyed on how many bars a member has contributed, not on the DATE",
        [
            (
                REGISTRY,
                "        scores[member.dates[index]] = value",
                "        scores[member.dates[len(scores)]] = value",
            )
        ],
        REGISTRY_TESTS,
        "test_members_are_grouped_by_date_not_by_index",
    ),
]


def run(tests: str, selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", tests, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def main() -> int:
    originals = {path: path.read_text() for path in (STRATEGY, REGISTRY)}
    failures: list[str] = []
    try:
        for name, edits, tests, selector in PROBES:
            mutated = dict(originals)
            bad_anchor = False
            for path, old, new in edits:
                count = mutated[path].count(old)
                if count != 1:
                    failures.append(f"{name}: anchor occurs {count} times, expected exactly 1 — probe proves nothing")
                    bad_anchor = True
                    break
                mutated[path] = mutated[path].replace(old, new)
            if bad_anchor:
                continue
            for path, text in mutated.items():
                path.write_text(text)
            rc = run(tests, selector)
            for path, text in originals.items():
                path.write_text(text)
            verdict = "CAUGHT" if rc != 0 else "*** NOT CAUGHT ***"
            print(f"  {verdict:<20} {name}", flush=True)
            if rc == 0:
                failures.append(name)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        for path, text in originals.items():
            path.write_text(text)

    rc_suite = run(f"{S2_TESTS} {REGISTRY_TESTS}".split()[0], "test_")
    rc_registry = run(REGISTRY_TESTS, "test_")
    print(f"\n  restored suites: {'PASS' if rc_suite == 0 and rc_registry == 0 else '*** FAIL ***'}", flush=True)
    if rc_suite or rc_registry:
        failures.append("restored suite does not pass")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
