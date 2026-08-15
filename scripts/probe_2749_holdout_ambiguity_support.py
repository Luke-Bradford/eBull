"""Adversarial revert probes for #2749's holdout ambiguity composition.

    uv run python scripts/probe_2749_holdout_ambiguity_support.py

The harness mutates tracked source temporarily and restores it in ``finally``.
A defect counts as caught only when the unmodified selected test passes and the
one-behaviour mutation exits pytest with an assertion failure (code 1).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

REPLAY = Path("app/services/strategy_result_ambiguity.py")
RUNNER = Path("app/services/backtest_run.py")
SOURCES = (REPLAY, RUNNER)
PURE_TESTS = "tests/test_strategy_result_ambiguity.py"
RUNNER_TESTS = "tests/test_backtest_run.py"

PROBES: list[tuple[str, Path, str, str, str, str]] = [
    (
        "a favourable support id accepted from multiple candidates",
        REPLAY,
        "    if candidate_count != 1 or support_id is None:",
        "    if candidate_count < 1 or support_id is None:",
        PURE_TESTS,
        "test_a_favourable_id_cannot_be_selected_without_exactly_one_candidate",
    ),
    (
        "favourable support allowed to override authoritative local evidence",
        REPLAY,
        '    if local_refusals != ("ambiguity_arms_not_compared",) or support_record is None:',
        "    if support_record is None:",
        PURE_TESTS,
        "test_support_cannot_override_an_authoritative_local_state",
    ),
    (
        "an in-sample cohort threshold applied to holdout Sharpes",
        RUNNER,
        "    if result.identity.namespace == CONTROL_NAMESPACE and best_sharpe is not None "
        "and worst_sharpe is not None:",
        "    if best_sharpe is not None and worst_sharpe is not None:",
        RUNNER_TESTS,
        "test_a_combined_run_never_applies_the_in_sample_cohort_to_holdout_sharpes",
    ),
]


def main() -> int:
    originals = {source: source.read_text() for source in SOURCES}
    failures: list[str] = []
    try:
        for name, source, old, new, test_file, selector in PROBES:
            if selected(test_file, selector) == 0:
                failures.append(f"{name}: selector names no test")
                print(f"  {'*** NO SUCH TEST ***':<24} {name}", flush=True)
                continue
            count = originals[source].count(old)
            if count != 1:
                failures.append(f"{name}: anchor occurs {count} times, expected 1")
                print(f"  {'*** BAD ANCHOR ***':<24} {name}", flush=True)
                continue
            baseline = run([test_file], selector)
            if baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exits {baseline}")
                print(f"  {'*** BAD BASELINE ***':<24} {name}", flush=True)
                continue
            source.write_text(originals[source].replace(old, new))
            caught = run([test_file], selector) == PYTEST_TEST_FAILED
            source.write_text(originals[source])
            if caught:
                print(f"  {'CAUGHT':<24} {name}", flush=True)
            else:
                failures.append(f"{name}: mutation was not caught by exit 1")
                print(f"  {'*** NOT CAUGHT ***':<24} {name}", flush=True)
    finally:
        for source, original in originals.items():
            source.write_text(original)

    restored = all(run([test_file], selector) == PYTEST_PASSED for _, _, _, _, test_file, selector in PROBES)
    print(f"\nrestored suite: {'PASS' if restored else '*** FAIL ***'}", flush=True)
    if not restored:
        failures.append("restored suite fails")
    if failures:
        print("\nUNCAUGHT:\n" + "\n".join(f"  {failure}" for failure in failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
