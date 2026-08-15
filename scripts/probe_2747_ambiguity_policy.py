"""Adversarial revert probes for #2747's §3.4 ambiguity policy.

    uv run python scripts/probe_2747_ambiguity_policy.py

This mutates tracked sources temporarily and restores them in ``finally``. A
probe counts only when its unmodified baseline passes and its one-behaviour
mutation produces pytest exit 1; syntax/collection/usage failures do not count.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

POLICY = Path("app/services/strategy_ambiguity_policy.py")
REPLAY = Path("app/services/strategy_result_ambiguity.py")
IDENTITY = Path("app/services/strategy_result.py")
SOURCES = (POLICY, REPLAY, IDENTITY)

PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        "the favourable arm's larger null margin used as the shared ambiguity ceiling",
        POLICY,
        "tests/test_strategy_result_ambiguity.py",
        [("    return min(margins)", "    return max(margins)")],
        "test_the_weaker_positive_arm_margin_is_the_shared_threshold",
    ),
    (
        "an arm gap equal to its evidence margin made material",
        REPLAY,
        "tests/test_strategy_result_ambiguity.py",
        [("    return gap > record.cohort_gap_threshold", "    return gap >= record.cohort_gap_threshold")],
        "test_the_comparison_is_strict_at_the_boundary",
    ),
    (
        "the ambiguity rule dropped from the v3 result hash",
        IDENTITY,
        "tests/test_strategy_result.py",
        [
            (
                "        if self.ambiguity_rule_version != LEGACY_AMBIGUITY_RULE_VERSION:\n"
                '            fields["ambiguity_rule_version"] = self.ambiguity_rule_version\n',
                "",
            )
        ],
        "test_a_successor_ambiguity_rule_moves_the_v3_hash",
    ),
]


def main() -> int:
    originals = {source: source.read_text() for source in SOURCES}
    failures: list[str] = []
    try:
        for name, source, test_file, edits, selector in PROBES:
            if selected(test_file, selector) == 0:
                failures.append(f"{name}: selector names no test")
                print(f"  {'*** NO SUCH TEST ***':<24} {name}", flush=True)
                continue
            mutated = originals[source]
            for old, new in edits:
                count = mutated.count(old)
                if count != 1:
                    failures.append(f"{name}: anchor occurs {count} times, expected 1")
                    print(f"  {'*** BAD ANCHOR ***':<24} {name}", flush=True)
                    break
                mutated = mutated.replace(old, new)
            else:
                baseline = run([test_file], selector)
                if baseline != PYTEST_PASSED:
                    failures.append(f"{name}: baseline exits {baseline}")
                    print(f"  {'*** BAD BASELINE ***':<24} {name}", flush=True)
                    continue
                source.write_text(mutated)
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

    restored = all(run([test_file], selector) == PYTEST_PASSED for _, _, test_file, _, selector in PROBES)
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
