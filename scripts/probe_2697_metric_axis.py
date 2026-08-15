"""Adversarial revert probes for #2697's fixed metric-axis contract.

    uv run python scripts/probe_2697_metric_axis.py

The harness temporarily restores the two outcome-selected axis constructions
named by acceptance criterion 10. A probe is caught only when its exact named
test passes before mutation and exits pytest with code 1 after mutation. Every
source is restored in ``finally``.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

BACKTEST_RUN = Path("app/services/backtest_run.py")
SYNTHETIC_CONTROL_RUN = Path("app/services/synthetic_control_run.py")
BACKTEST_TESTS = "tests/test_backtest_run.py"
CONTROL_TESTS = "tests/test_synthetic_control_run.py"

PROBES: list[tuple[str, Path, str, str, str, str]] = [
    (
        "the strategy metric axis is truncated to its own first and last leg",
        BACKTEST_RUN,
        "    lo = corpus.axis_pos[dates[0]]\n    hi = corpus.axis_pos[dates[-1]]\n",
        (
            "    if book.first_index is None or book.last_index is None:\n"
            "        return None\n"
            "    lo = book.first_index\n"
            "    hi = book.last_index\n"
            "    dates = corpus.axis[lo : hi + 1]\n"
        ),
        BACKTEST_TESTS,
        "test_moving_only_the_first_firing_and_last_exit_cannot_move_axis_identity",
    ),
    (
        "each synthetic member is truncated to its random first and last leg",
        SYNTHETIC_CONTROL_RUN,
        "    curve = build_equity_curve(book, date_count=len(inputs.axis))\n",
        (
            "    low = min(book.entry_index)\n"
            "    high = max(book.exit_index)\n"
            "    member_axis = inputs.axis[low : high + 1]\n"
            "    book = book.rebased(low)\n"
            "    curve = build_equity_curve(book, date_count=len(member_axis))\n"
            "    inputs = _MemberInputs(\n"
            "        placements=inputs.placements,\n"
            "        axis=member_axis,\n"
            "        benchmark=inputs.benchmark,\n"
            "        expected_trade_count=inputs.expected_trade_count,\n"
            "    )\n"
        ),
        CONTROL_TESTS,
        "test_every_member_is_measured_on_the_complete_fixed_axis",
    ),
]


def main() -> int:
    sources = {path for _, path, *_ in PROBES}
    originals = {source: source.read_text() for source in sources}
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
