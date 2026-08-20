"""Revert-probes for §3.2's backtest-run invariant tests (#2394, #2240).

    PYTHONPATH=. uv run python scripts/probe_2394_backtest_run.py

⚠ NOTHING IS WRITTEN TO THE DATABASE. This edits source files in place, runs one
targeted test, and restores them — including on failure. Gate on the EXIT CODE:
0 means every probe was CAUGHT by the test it targets, 1 means at least one
defect passed the suite unnoticed.

WHY THIS EXISTS
---------------
A test asserting a value can pass because the value is right OR because the
assertion never runs. Each probe below injects the exact defect one test claims
to guard, and requires that test to FAIL. A probe that changes nothing proves
nothing, so every substitution asserts it matched exactly once before applying
it — the prevention-log lesson from a probe that silently matched no text.

⚠ THREE OF THESE DEFECTS WERE REAL AND WERE FOUND BY THE TESTS THEMSELVES, not
by review: the hold-out pairing accepting a whitespace-only purpose, the
degenerate-trial guard testing a standard deviation that is never exactly zero
in binary floating point, and a namespace whose block bootstrap could not run
storing a row with a null effective sample size. Their probes are the record
that each fix is now load-bearing.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SERVICE = REPO / "app" / "services" / "backtest_run.py"
SCHEDULER = REPO / "app" / "workers" / "scheduler.py"
WALK_FORWARD = REPO / "app" / "services" / "walk_forward.py"

_TESTS = "tests/test_backtest_run.py"


@dataclass(frozen=True)
class Probe:
    """One injected defect and the test that must catch it."""

    name: str
    path: Path
    old: str
    new: str
    test: str


PROBES: tuple[Probe, ...] = (
    Probe(
        name="a whitespace-only hold-out purpose passes the pairing check (#2286 shape)",
        path=SERVICE,
        old="        if value is not None and value.strip()\n",
        new="        if value\n",
        test=f"{_TESTS}::TestHoldoutPairing::test_one_of_two_refuses",
    ),
    Probe(
        name="the degenerate-trial guard tests a standard deviation instead of the spread",
        path=SERVICE,
        old="if float(np.ptp(panel[index])) == 0.0)",
        new="if float(np.std(panel[index])) == 0.0)",
        test=(f"{_TESTS}::TestDeflateGroup::test_a_constant_return_series_refuses_and_is_not_read_as_uncorrelated"),
    ),
    Probe(
        name="a trial with no effective sample size counts toward MIN_MEASURED_TRIALS",
        path=SERVICE,
        old="        if outcome.moments is not None and outcome.metrics.effective_sample_size is not None\n",
        new="        if outcome.moments is not None\n",
        test=f"{_TESTS}::TestDeflateGroup::test_a_trial_with_no_effective_sample_size_does_not_count",
    ),
    Probe(
        name="an undeclared trial id is dropped instead of refusing the deflation",
        path=SERVICE,
        old="    undeclared = sorted(set(usable) - TRIAL_REGISTER.trial_ids)\n",
        new="    undeclared = []\n",
        test=f"{_TESTS}::TestDeflateGroup::test_an_undeclared_trial_id_refuses_rather_than_raising",
    ),
    Probe(
        name="the in-sample axis is allowed to reach the frozen hold-out boundary",
        path=SERVICE,
        old='    if namespace == "in_sample" and corpus.axis[hi] >= HOLDOUT_BOUNDARY:\n',
        new='    if namespace == "in_sample" and corpus.axis[hi] > EVALUATION_WINDOW_END:\n',
        test=f"{_TESTS}::TestNamespaceAxis::test_an_in_sample_position_closing_on_the_boundary_raises",
    ),
    Probe(
        name="a namespace whose block bootstrap did not run still produces a row",
        path=SERVICE,
        old="    if metrics.effective_sample_size is None:\n        raise RuntimeError(",
        new="    if False:\n        raise RuntimeError(",
        test=(
            f"{_TESTS}::TestNamespaceAxis"
            "::test_a_namespace_whose_bootstrap_cannot_run_refuses_rather_than_storing_a_nominal_n"
        ),
    ),
    Probe(
        name="the namespace axis is padded back to the corpus start",
        path=SERVICE,
        old="    lo, hi = book.first_index, book.last_index\n",
        new="    lo, hi = 0, book.last_index\n",
        test=f"{_TESTS}::TestNamespaceAxis::test_the_axis_is_the_span_of_the_namespaces_own_legs",
    ),
    Probe(
        name="a level-based entry that stopped refusing is silently treated as excluded",
        path=SERVICE,
        old="""        if refusal is None:
            raise RuntimeError(""",
        new="""        if False:
            raise RuntimeError(""",
        test=f"{_TESTS}::TestRunnableStrategies::test_a_level_based_entry_that_stops_refusing_raises",
    ),
    Probe(
        name="an `ambiguous` close on a non-level arm is not noticed",
        path=SERVICE,
        old='    count = measurement.close_sources.get("ambiguous", 0)\n',
        new="    count = 0\n",
        test=f"{_TESTS}::TestAmbiguityCensus::test_one_ambiguous_close_falsifies_the_single_measurement_claim",
    ),
    Probe(
        name="a runnable strategy that produced no row does not fail the run",
        path=SERVICE,
        old="    missing = sorted(strategy_id for strategy_id in report.runnable if not produced[strategy_id])\n",
        new="    missing = []\n",
        test=f"{_TESTS}::TestRowCompleteness::test_a_runnable_strategy_with_no_row_fails",
    ),
    Probe(
        name="a short arm passes because only the strategy set is checked",
        path=SERVICE,
        old="    if report.rows_written != expected:\n",
        new="    if False:\n",
        test=f"{_TESTS}::TestRowCompleteness::test_a_short_arm_fails_even_when_every_strategy_appears",
    ),
    Probe(
        name="an in-sample-only run is not expected to refuse on the never-evaluated hold-out",
        path=SERVICE,
        old='    if not holdout_requested:\n        expected.add("holdout_never_evaluated")\n',
        new="    if False:\n        pass\n",
        test=f"{_TESTS}::TestExpectedRefusals::test_in_sample_run_adds_holdout_never_evaluated",
    ),
    Probe(
        name="a missing Deflated Sharpe is expected to refuse on only one of criterion 6's two codes",
        path=SERVICE,
        old='        expected.update({"deflated_sharpe_not_computed", "trial_count_undeclared"})\n',
        new='        expected.update({"deflated_sharpe_not_computed"})\n',
        test=f"{_TESTS}::TestExpectedRefusals::test_no_dsr_adds_both_criterion_6_refusals",
    ),
    Probe(
        name="two planned rows sharing a result_version reach the INSERT",
        path=SERVICE,
        old="    if len(set(versions)) != len(versions):\n",
        new="    if False:\n",
        test=f"{_TESTS}::TestPlannedIdentities::test_two_planned_rows_sharing_a_version_raise_without_touching_the_database",
    ),
    Probe(
        name="the benchmark opens at the instrument's own first bar rather than the namespace axis",
        path=SERVICE,
        old="        start = max(lo, first_axis_index)\n",
        new="        start = first_axis_index\n",
        test=f"{_TESTS}::TestBenchmarkBook::test_a_leg_is_clipped_to_the_axis_rather_than_dropped",
    ),
    Probe(
        name="the benchmark covers every loaded instrument, not the namespace's own",
        path=SERVICE,
        old="    for instrument_id in sorted(instruments):\n",
        new="    for instrument_id in sorted(closes_by_instrument):\n",
        test=f"{_TESTS}::TestBenchmarkBook::test_only_the_namespaces_own_instruments_get_a_leg",
    ),
    Probe(
        name="the benchmark is charged no cost, so every strategy looks worse by the cost model",
        path=SERVICE,
        old="        half = half_spread_for(Decimal(repr(entry_close)))\n",
        new="        half = Decimal(0)\n",
        test=f"{_TESTS}::TestBenchmarkBook::test_the_benchmark_is_charged_the_same_cost_model",
    ),
    Probe(
        name="the leg book is re-based by the wrong sign",
        path=SERVICE,
        old="        entry_index=[index - offset for index in book.entry_index],\n",
        new="        entry_index=[index + offset for index in book.entry_index],\n",
        test=f"{_TESTS}::TestShiftedLegBook::test_indices_shift_and_the_large_arrays_are_shared",
    ),
    Probe(
        name="a non-fired ledger row reaches the position builder",
        path=SERVICE,
        old='        if row.verdict != "fired":\n            continue\n',
        new="        if False:\n            continue\n",
        test=f"{_TESTS}::TestFills::test_only_fired_rows_reach_the_builder",
    ),
    Probe(
        name="the scheduler passes a blank param through as an empty string",
        path=SCHEDULER,
        old="    text = str(value).strip()\n    return text or None\n",
        new="    return str(value)\n",
        test=f"{_TESTS}::TestOptionalStr::test_blank_is_none",
    ),
    # -----------------------------------------------------------------------
    # Criterion 5's split, wired into the run (#2240 follow-on to §3.2)
    # -----------------------------------------------------------------------
    Probe(
        name="a split is cut over an empty in-sample population",
        path=SERVICE,
        old="    if not starts:\n        raise ValueError(",
        new="    if False:\n        raise ValueError(",
        test=f"{_TESTS}::TestInSampleSplit::test_a_population_with_no_closed_observation_refuses",
    ),
    Probe(
        name="mismatched label-window arrays are cut instead of refused",
        path=SERVICE,
        old='    if len(starts) != len(ends):\n        raise ValueError(f"{len(starts)} label-window starts',
        new='    if False:\n        raise ValueError(f"{len(starts)} label-window starts',
        test=f"{_TESTS}::TestInSampleSplit::test_mismatched_label_window_arrays_refuse",
    ),
    Probe(
        name="the fold's bar count is taken from the population rather than the axis",
        path=SERVICE,
        old="                bar_count=sum(bar_counts[fold.first_index : fold.last_index + 1]),\n",
        new="                bar_count=len(starts),\n",
        test=f"{_TESTS}::TestInSampleSplit::test_the_geometry_does_not_move_with_the_population",
    ),
    Probe(
        name="a fold's stored first_date does not describe its stored first_index",
        path=SERVICE,
        old="                first_date=axis[fold.first_index],\n",
        new="                first_date=axis[fold.last_index],\n",
        test=f"{_TESTS}::TestInSampleSplit::test_the_axis_is_cut_into_four_contiguous_blocks_carrying_their_dates",
    ),
    Probe(
        name="the embargo is measured over the PRE-purge candidates (the circularity AFML ch. 7 forbids)",
        path=WALK_FORWARD,
        old='        if role(start, end, fold=fold, embargo_bars=0) != "train":\n            continue\n',
        new="        if False:\n            continue\n",
        test=f"{_TESTS}::TestInSampleSplit::test_the_embargo_is_measured_off_the_post_purge_training_side",
    ),
    Probe(
        name="a second measurement of one arm silently overwrites the first arm's split",
        path=SERVICE,
        old="        if key in splits:\n",
        new="        if False:\n",
        test=(f"{_TESTS}::TestCutSplits::test_two_measurements_of_one_arm_are_refused_rather_than_overwritten"),
    ),
    Probe(
        name="an in-sample row with no split reaches the write instead of being refused up front",
        path=SERVICE,
        old="            if (strategy_id, result.identity.quarantine_arm) not in splits\n",
        new="            if False\n",
        test=(f"{_TESTS}::TestCutSplits::test_a_pending_in_sample_row_with_no_split_is_refused_before_any_insert"),
    ),
    Probe(
        name="every arm is handed the FIRST arm's split, so the two censuses stop differing",
        path=SERVICE,
        old="        splits[key] = split\n",
        new="        splits[key] = next(iter(splits.values()), split)\n",
        test=f"{_TESTS}::TestCutSplits::test_each_arm_keeps_its_own_census",
    ),
    Probe(
        name="a measurement with no in-sample namespace still gets a split cut for it",
        path=SERVICE,
        old=(
            '        outcome = measurement.namespaces.get("in_sample")\n'
            "        if outcome is None:\n            continue\n"
        ),
        new=(
            '        outcome = measurement.namespaces.get("in_sample")\n'
            "        if outcome is None:\n"
            "            outcome = next(iter(measurement.namespaces.values()), None)\n"
            "        if outcome is None:\n            raise RuntimeError('no namespace')\n"
        ),
        test=f"{_TESTS}::TestCutSplits::test_a_hold_out_only_measurement_contributes_no_split",
    ),
    Probe(
        name="the hold-out book accumulates criterion 5's label windows too",
        path=SERVICE,
        old="        if self.records_label_windows and realised:\n",
        new="        if realised:\n",
        test=f"{_TESTS}::TestLabelWindowCollection::test_a_hold_out_book_records_nothing",
    ),
    Probe(
        name="an unrealised leg's unresolved label window reaches the embargo measurement",
        path=SERVICE,
        old="        if self.records_label_windows and realised:\n",
        new="        if self.records_label_windows:\n",
        test=f"{_TESTS}::TestLabelWindowCollection::test_an_unrealised_leg_contributes_no_label_window",
    ),
    Probe(
        name="an in-sample book holding an open position is measured rather than refused",
        path=SERVICE,
        old='    if namespace == "in_sample" and book.open_at_end:\n',
        new="    if False:\n",
        test=f"{_TESTS}::TestNamespaceAxis::test_an_in_sample_namespace_holding_an_open_position_raises",
    ),
    # ⚠ TWO-SIDED PREDICATE, ONE PROBE PER SIDE. ``wanted`` decides both "an
    # in-sample row carries the whole split" and "a hold-out row carries none";
    # a single probe would leave whichever side it did not move unproven.
    Probe(
        name="an in-sample row's fold shortfall is not noticed",
        path=SERVICE,
        old='        wanted = FOLD_COUNT if row.namespace == "in_sample" else 0\n',
        new='        wanted = row.folds_written if row.namespace == "in_sample" else 0\n',
        test=f"{_TESTS}::TestRowCompleteness::test_an_in_sample_row_without_its_whole_split_fails",
    ),
    Probe(
        name="a hold-out row carrying folds is not noticed",
        path=SERVICE,
        old='        wanted = FOLD_COUNT if row.namespace == "in_sample" else 0\n',
        new='        wanted = FOLD_COUNT if row.namespace == "in_sample" else row.folds_written\n',
        test=f"{_TESTS}::TestRowCompleteness::test_a_hold_out_row_carrying_folds_fails",
    ),
)


def _run(test: str) -> int:
    """The targeted test's exit code.

    ⚠ NOT PIPED. A pipe returns the pipe's status, which reads a failure as a
    success — the defect this repo has re-committed twice.
    """
    return subprocess.run(
        [sys.executable, "-m", "pytest", test, "-q", "--no-header", "-p", "no:cacheprovider", "-p", "no:randomly"],
        cwd=REPO,
        capture_output=True,
        text=True,
    ).returncode


def main() -> int:
    failures: list[str] = []
    for probe in PROBES:
        original = probe.path.read_text()
        count = original.count(probe.old)
        if count != 1:
            failures.append(f"NO-OP PROBE: {probe.name!r} matched its target {count} times, expected exactly 1")
            print(f"  ✗ {probe.name}: target matched {count} times — the probe would prove nothing", flush=True)
            continue
        probe.path.write_text(original.replace(probe.old, probe.new))
        try:
            code = _run(probe.test)
        finally:
            probe.path.write_text(original)
        if code == 0:
            failures.append(f"NOT CAUGHT: {probe.name!r} passed {probe.test}")
            print(f"  ✗ {probe.name}: the test PASSED with the defect present", flush=True)
        else:
            print(f"  ✓ {probe.name}: caught (exit {code})", flush=True)

    print(f"\nprobes {len(PROBES)}   caught {len(PROBES) - len(failures)}   missed {len(failures)}")
    for failure in failures:
        print(f"  {failure}")

    baseline = _run(_TESTS)
    print(f"restored suite exit {baseline}")
    return 0 if not failures and baseline == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
