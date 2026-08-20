"""Revert-probe the phase-5a position-construction invariant tests (#2240).

    uv run python scripts/probe_2240_position_builder.py

Sister to ``scripts/probe_2240_s1_momentum.py`` / ``probe_2240_s3_mean_reversion.py``,
whose guards apply unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything.
3. ⚠ **The SELECTOR is not guarded by either.** ``NOT CAUGHT`` has three causes
   and the triage order is selector → fixture → code (prevention log, #2240
   S-2): a ``-k`` naming a test whose fixture happens to agree with the defect
   proves nothing either.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

⚠ THIS HARNESS MUTATES A TRACKED SOURCE FILE, so it must not run concurrently
with anything that reads it — including ``scripts/verify_2240_position_builder.py``.
Phase 4b's lesson: a concurrent run stamps its output with the INJECTED source
hash, and a start-vs-end check misses it because the probe restores the file.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``_bar_index``'s corpus-drift refusal.** Deleting it makes ``index.get``
return ``None`` and the next line raises a ``TypeError`` instead — the test
still fails, but on a crash rather than on the guard, so the probe would report
``CAUGHT`` for something that is not the invariant. The two tests that pin it
(``test_a_fill_date_absent_from_the_series_raises``,
``test_a_stored_exit_date_absent_from_the_series_raises``) stand on their own.

⚠ **The window-end mark's VALUE.** ``_mark_price`` walking forwards instead of
backwards is caught by ``test_a_close_after_the_window_end_leaves_the_position_open_and_marked``
and is probed; its lower bound is probed separately. There is no third
behaviour in that function to delete.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# ⚠⚠ The docstring above invokes this file by PATH, which puts ``scripts/`` on
# sys.path and NOT the repo root — so the cross-script import below raises
# ModuleNotFoundError under the exact command this file documents. Prepending
# the root makes both that form and ``-m scripts.<name>`` work (#2357).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ⚠ The gate constants live ONCE, in the 5b reference harness. A second
# hand-written copy of "exit 1 means the test failed" is how this file's
# gate drifted from it in the first place (#2357).
from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED  # noqa: E402

SRC = Path("app/services/position_builder.py")
TESTS = "tests/test_position_builder.py"

# ⚠ `noqa: E501` appears on several anchors below and is not laziness: an
# anchor is a VERBATIM copy of a source line, and wrapping it to satisfy the
# line limit would make it match nothing — which the harness would then
# report as a bad anchor rather than as a caught defect.
#: (what the injected defect IS, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        # ⚠ THE PYRAMIDING COLLAPSE — §3.1. Entries are STATES, so without this
        # a trend opens a position every bar and multiplies every downstream
        # statistic by the length of the run.
        "the open-position suppression removed entirely (pyramiding restored)",
        [
            (
                "            if holding and (open_until is None or entry.fill_bar_date < open_until):\n"
                "                superseded += 1\n"
                "                continue\n",
                "",
            )
        ],
        "test_entries_while_open_are_superseded_and_counted",
    ),
    (
        # ⚠ §3.2 rule 4 — same-bar ordering is exit BEFORE entry. `<=` suppresses
        # the entry that fills on the bar the previous hold closed on.
        "the suppression bound relaxed to <= (an entry on the close bar is swallowed)",
        [
            (
                "            if holding and (open_until is None or entry.fill_bar_date < open_until):",
                "            if holding and (open_until is None or entry.fill_bar_date <= open_until):",
            )
        ],
        "test_an_entry_on_the_close_bar_opens_a_new_position",
    ),
    (
        # The mirror of the above, on the OTHER side of the same rule: C1 must
        # be STRICTLY after the entry fill, or a stateless exit leg closes the
        # position it just opened at zero bars held.
        "C1's search relaxed to bisect_left (an exit closes the position opened that bar)",
        [
            (
                "                after = bisect_right(exit_dates, entry.fill_bar_date)",
                "                after = bisect_left(exit_dates, entry.fill_bar_date)",
            ),
            ("from bisect import bisect_right", "from bisect import bisect_left, bisect_right"),
        ],
        "test_an_exit_on_the_entry_fill_bar_does_not_close_it",
    ),
    (
        # ⚠ §3.2 rule 5 is only satisfiable if `unresolved` beats C3. Without
        # this the resolver's refusal is overridden and the expiry return is
        # booked over bars it just said it could not judge.
        "an unresolved outcome no longer suppresses the max-hold close",
        [
            (
                "            if regime.max_hold_bars is not None and not unresolved:",
                "            if regime.max_hold_bars is not None:",
            )
        ],
        "test_unresolved_leaves_the_position_open_and_suppresses_the_max_hold",
    ),
    (
        # §3.2 C3: for S-4 the resolver's `expired` and position construction's
        # max-hold are redundant BY DESIGN, so "a disagreement is a failure, not
        # a tie-break". Deleting the equality check silently picks one of two
        # numbers that cannot both be right.
        # ⚠ RE-ANCHORED (#2779). The check used to run over every tied candidate
        # and now runs over the redundant pair only, so the anchor moved from the
        # price set to the redundant subset. The DEFECT is unchanged.
        "the same-bar price-agreement check deleted (a disagreement becomes a tie-break)",
        [("    if len({candidate.price for candidate in redundant}) > 1:", "    if False:")],
        "test_a_disagreement_about_the_same_bar_raises",
    ),
    (
        # ⚠⚠ THE OTHER HALF OF #2779, and the one that cost 13.6 hours. Widening
        # the redundant set back to every tied candidate makes an ordinary
        # level-vs-signal_pair tie fatal again — they are different exit rules,
        # not two readings of one window, so they have no reason to agree.
        "the redundancy check widened back to every tied source",
        [
            (
                "    redundant = [candidate for candidate in tied "
                'if candidate.redundant_with_max_hold or candidate.source == "max_hold"]',  # noqa: E501
                "    redundant = list(tied)",
            )
        ],
        "test_a_LEVEL_TOUCH_tying_with_a_signal_pair_exit_resolves_by_precedence",
    ),
    (
        # ⚠ `tp_hit` / `sl_hit` are intraday touches, not a recomputation of the
        # max-hold window. Marking them redundant would demand they agree with a
        # max-hold close they have no relationship to.
        "a tp/sl touch marked redundant with the max-hold expiry",
        [
            (
                '                            redundant_with_max_hold=outcome.outcome == "expired",',
                "                            redundant_with_max_hold=True,",
            )
        ],
        # ⚠ The selector must build BOTH a level and a max-hold candidate on one
        # date, or the redundant set is a single element and this mutation is
        # invisible. The hybrid fixture has no max-hold leg and let it through.
        "test_a_tp_touch_landing_on_the_max_hold_bar_is_NOT_a_redundancy_failure",
    ),
    (
        "the tie label taken from position construction rather than the resolver",
        [
            (
                '_SOURCE_PRECEDENCE: tuple[CloseSource, ...] = ("level", "ambiguous", "max_hold", "calendar", "signal_pair")',  # noqa: E501
                '_SOURCE_PRECEDENCE: tuple[CloseSource, ...] = ("max_hold", "calendar", "level", "ambiguous", "signal_pair")',  # noqa: E501
            )
        ],
        "test_expired_and_the_max_hold_bar_agree",
    ),
    (
        # ⚠ "Not resolved yet" and "resolved as nothing" are different states,
        # and the second is the one that quietly books returns. The injected
        # version is the plausible defect, not a crash: treat the absence as an
        # unresolved outcome and move on.
        "a missing outcome treated as unresolved instead of refused",
        [
            (
                "                if outcome is None:\n"
                "                    raise ValueError(\n"
                '                        f"signal {entry.signal_id} is a level-based entry with no outcome at the pinned "\n'  # noqa: E501
                '                        "version pair — resolve it before building positions rather than falling through to "\n'  # noqa: E501
                '                        "the max-hold close"\n'
                "                    )\n",
                "                if outcome is None:\n"
                "                    outcome = ResolvedOutcome(\n"
                "                        signal_id=entry.signal_id,\n"
                '                        rule_set_version="probe",\n'
                '                        input_rule_set_version="probe",\n'
                '                        outcome="unresolved",\n'
                "                        exit_bar_date=None,\n"
                "                        exit_price=None,\n"
                "                    )\n",
            )
        ],
        "test_a_missing_outcome_raises_rather_than_falling_through_to_max_hold",
    ),
    (
        # ⚠ §3.2 rule 1. Two resolver versions coexist by design (sql/256 keys
        # on the pair), so an unpinned set double-counts every signal once per
        # version present.
        "the outcome version-pin check deleted",
        [
            (
                "        if outcome_pin is not None and (\n"
                "            outcome.rule_set_version != outcome_pin.rule_set_version\n"
                "            or outcome.input_rule_set_version != outcome_pin.input_rule_set_version\n"
                "        ):",
                "        if False:",
            )
        ],
        "test_an_outcome_at_another_version_pin_raises",
    ),
    (
        "the duplicate-outcome check deleted (one signal resolved twice merges silently)",
        [("        if outcome.signal_id in by_signal:", "        if False:")],
        "test_two_outcomes_for_one_signal_raise",
    ),
    (
        "the level-based regime no longer required to declare its version pin",
        [("    if regime.level_based and outcome_pin is None:", "    if False:")],
        "test_a_level_based_regime_without_a_pin_raises",
    ),
    (
        "exits accepted for a regime that declares no exit leg",
        [("    if exits and not regime.signal_pair:", "    if False:")],
        "test_exits_without_a_declared_exit_leg_raise",
    ),
    (
        "outcomes accepted for a regime that is not level-based",
        [("    if outcomes and not regime.level_based:", "    if False:")],
        "test_outcomes_without_a_level_based_regime_raise",
    ),
    (
        # §5.2's purge, applied to the FILL date: acting on the signal needs a
        # price from the withheld side, so the entry belongs to neither
        # namespace.
        "the window purge deleted (an entry filling outside the window still trades)",
        [("        if not window.contains(entry.fill_bar_date):", "        if False:")],
        "test_an_entry_filling_outside_the_window_is_purged",
    ),
    (
        # §3.2 rule 5 — dropping a close that lands past the window end biases
        # toward positions that closed, and positions close faster in trending
        # regimes.
        "a close past the window end accepted as this namespace's close",
        [
            (
                "            candidates = [candidate for candidate in candidates if candidate.when <= limit]",
                "            candidates = list(candidates)",
            )
        ],
        "test_a_close_after_the_window_end_leaves_the_position_open_and_marked",
    ),
    (
        # A mark taken from before the fill is not an unrealised return, it is a
        # fabricated one.
        "the mark's lower bound deleted (an open position marked from before its own fill)",
        [
            (
                "        if when < not_before:\n            return None\n",
                "",
            )
        ],
        "test_a_mark_that_cannot_be_taken_is_counted_never_invented",
    ),
    (
        # §3.3 — a name reselected at consecutive rebalances is ONE hold. Without
        # this the position closes and reopens every month and stage 5b charges
        # two sides of the cost model for a hold that never ended.
        "the reselection check deleted (a consecutive hold closes and reopens monthly)",
        [
            (
                "                    if (instrument_id, when) in reselected:\n                        continue\n",
                "",
            )
        ],
        "test_a_reselected_name_is_one_hold_not_two",
    ),
    (
        "the calendar close moved onto the rebalance bar itself (a same-bar fill)",
        [
            (
                "    index = bisect_right(series.dates, when)",
                "    index = bisect_left(series.dates, when)",
            ),
            ("from bisect import bisect_right", "from bisect import bisect_left, bisect_right"),
        ],
        "test_a_name_dropped_at_the_next_rebalance_closes_at_that_fill",
    ),
    (
        # Criterion 8 / spec §9 C8: the panel-vs-instrument calendar divergence
        # is a narrowing phase 5 introduces, so it is reported rather than
        # inferred.
        "the halted-at-rebalance counter deleted",
        [
            (
                "                    if when not in bar_index:\n                        halted += 1\n",
                "",
            )
        ],
        "test_a_halt_across_the_rebalance_is_counted_and_closes_at_the_next_own_bar",
    ),
    (
        # ⚠⚠ CODEX CHECKPOINT 2's P2. A declared `max_hold_bars` ENDS the hold
        # by construction, so a later exit signal must not book it. Without the
        # ceiling, an unpriceable expiry bar lets S-3 run 13 bars against a
        # declared 10, priced off a bar the strategy could never have reached.
        "the max-hold / calendar ceiling deleted (a later source books a forced-closed hold)",
        [
            (
                "            limit = window.end if ceiling is None else min(ceiling, window.end)",
                "            limit = window.end",
            )
        ],
        "test_an_unpriceable_expiry_bar_does_not_let_a_later_exit_book_the_trade",
    ),
    (
        # The mirror: the ceiling must BOUND the hold, not suppress every later
        # trade in the instrument. Treating an unbookable hold as eternal drops
        # the rest of that series' history over one masked bar.
        #
        # ⚠⚠ RE-ANCHORED (#2357). The original anchor was the one-line
        # ``open_until = ceiling if open_reason == "close_bar_unfillable" else None``.
        # A later ``series_break`` arm turned that into a three-way conditional and
        # the formatter split it across lines, so the anchor stopped matching and
        # this invariant has been UNPROBED ever since — silently, because nothing
        # re-ran the harness. Anchored on the ``close_bar_unfillable`` arm alone so
        # the mutation stays scoped to THIS invariant rather than also deleting the
        # ``series_break`` arm, which is a different rule with its own tests.
        "an unbookable hold left open forever instead of ending at its ceiling",
        [
            (
                '                    ceiling\n                    if open_reason == "close_bar_unfillable"\n',
                '                    None\n                    if open_reason == "close_bar_unfillable"\n',
            )
        ],
        "test_an_unbookable_hold_does_not_suppress_the_rest_of_history",
    ),
    (
        # The THIRD arm of the same expression, and the one that had no probe at
        # all (#2689). It post-dates this harness: when the probe above was
        # written the expression was a two-way `ceiling if ... else None`, so
        # `series_break` was added without anything asking whether it was
        # covered. ⚠ Nothing in the harness can notice that — the guards check
        # that an anchor matches and that a replacement changes behaviour, and
        # both are silent about a branch no probe names.
        #
        # A `series_break` hold must suppress entries only until the series
        # RESUMES (`unresolved_until`), not forever. Deleting the bound sends the
        # arm to None, which the suppression check reads as "open indefinitely"
        # and drops the whole next segment of that instrument's history.
        "a series-break hold suppressing the resumed segment instead of ending at its resume boundary",
        [
            (
                '                    else unresolved_until\n                    if open_reason == "series_break"\n',
                '                    else None\n                    if open_reason == "series_break"\n',
            )
        ],
        "test_a_series_break_outcome_is_unmarked_but_does_not_suppress_the_new_segment",
    ),
    (
        # ⚠ MEASURED: 16 bars across 9 series carry `open = 0`, all already
        # quarantined on both axes. A fill at 0 is not a trade — it makes every
        # downstream return infinite.
        "the non-positive fill-price refusal deleted",
        [
            (
                "        if self.fill_price <= 0:\n"
                '            raise ValueError(f"signal {self.signal_id}: fill_price must be > 0, got {self.fill_price}")\n',  # noqa: E501
                "",
            )
        ],
        "test_a_zero_fill_price_is_refused",
    ),
    (
        "the unfillable-close-bar counter deleted",
        [
            (
                "                    if expiry_open is None:\n                        unfillable += 1\n",
                "                    if expiry_open is None:\n                        pass\n",
            )
        ],
        "test_an_expiry_bar_with_no_open_is_counted_not_guessed",
    ),
    (
        # ⚠⚠ THE MIRROR DEFECT ITSELF (prevention log, #2240 3c). `a is not None
        # and b is not None` reads as "has a location" and admits HALF one — and
        # only an OPEN position carrying a stray bars_held discriminates the two
        # expressions, which is why that fixture exists.
        "the close-location nullity check ANDed instead of counted",
        [
            (
                "        located = (self.close_bar_date is not None) + (self.bars_held is not None)",
                "        located = 2 if (self.close_bar_date is not None and self.bars_held is not None) else 0",
            )
        ],
        "test_an_open_position_may_not_carry_a_stray_bars_held",
    ),
    (
        # `sql/256`: bars_held = 0 is LEGAL — a level touched on the fill bar
        # itself. A strict inequality here rejects the resolver's own output.
        "the close-after-fill bound tightened to strict (rejects a legal 0-bar hold)",
        [
            (
                "            if self.close_bar_date < self.entry_fill_bar_date:",
                "            if self.close_bar_date <= self.entry_fill_bar_date:",
            )
        ],
        "test_a_level_touched_on_the_fill_bar_is_zero_bars_held",
    ),
    (
        "the ambiguous-carries-no-price rule deleted",
        [
            (
                '            if (self.close_price is None) != (self.close_source == "ambiguous"):',
                "            if False:",
            )
        ],
        "test_only_ambiguous_may_close_without_a_price or test_ambiguous_may_not_carry_a_price",
    ),
    (
        "a regime declaring no close source at all accepted",
        [
            (
                "        if not (self.signal_pair or self.level_based or self.max_hold_bars or self.rebalance_dates):",
                "        if False:",
            )
        ],
        "test_a_regime_with_no_close_source_raises",
    ),
    (
        "the entry fill/signal ordering check deleted (the look-ahead sql/255 exists to prevent)",
        [
            (
                # ⚠ Anchored on the MESSAGE, not on the comparison: `EntryFill`
                # and `ExitFill` carry the identical guard, so the comparison
                # alone occurs twice and the harness refuses it.
                "        if self.fill_bar_date <= self.signal_bar_date:\n"
                "            raise ValueError(\n"
                '                f"signal {self.signal_id}: fill_bar_date {self.fill_bar_date} is not after signal_bar_date "\n',  # noqa: E501
                "        if False:\n"
                "            raise ValueError(\n"
                '                f"signal {self.signal_id}: fill_bar_date {self.fill_bar_date} is not after signal_bar_date "\n',  # noqa: E501
            )
        ],
        "test_a_fill_on_or_before_its_signal_bar_raises",
    ),
    (
        "the rule-set version frozen to a constant",
        [
            (
                "    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]",
                '    return "0" * 12',
            )
        ],
        "test_the_rule_set_version_hashes_this_modules_own_source",
    ),
]


def run(selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def selected(selector: str) -> int:
    """How many tests the selector actually names. ⚠ A selector matching ZERO
    tests makes ``pytest`` exit non-zero, which the harness would read as
    ``CAUGHT`` — the third cause of a meaningless probe (see the header)."""
    result = subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "-k", selector, "-p", "no:randomly", "-n", "0", "--collect-only"],
        capture_output=True,
        text=True,
    )
    # ⚠ `-q --collect-only` prints one `path: <count>` line per file, NOT one
    # line per test id. Counting `"::" in line` returns 0 for every selector,
    # which this harness reports as "names no test" — which is how the miscount
    # was found rather than silently passing.
    total = 0
    for line in result.stdout.splitlines():
        head, _, tail = line.partition(": ")
        if head == TESTS and tail.strip().isdigit():
            total += int(tail.strip())
    return total


def main() -> int:
    original = SRC.read_text()
    failures: list[str] = []
    try:
        for name, edits, selector in PROBES:
            count = selected(selector)
            if count == 0:
                failures.append(f"{name}: selector {selector!r} names no test — probe proves nothing")
                print(f"  {'*** NO SUCH TEST ***':<20} {name}", flush=True)
                continue
            mutated = original
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
            # ⚠⚠ BASELINE FIRST — assert the selected test PASSES on unmutated
            # source before mutating anything. Without it a probe cannot tell
            # "the mutation broke the test" from "the test was already broken",
            # and the second reads as CAUGHT (prevention log, #2214).
            rc_baseline = run(selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<20} {name}  (exit {rc_baseline})", flush=True)
                continue
            SRC.write_text(mutated)
            rc = run(selector)
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
            print(f"  {verdict:<20} {name}  ({count} test{'' if count == 1 else 's'})", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        SRC.write_text(original)

    rc_suite = run("test_")
    print(f"\n  restored suite: {'PASS' if rc_suite == 0 else '*** FAIL ***'}", flush=True)
    if rc_suite:
        failures.append("restored suite does not pass")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
