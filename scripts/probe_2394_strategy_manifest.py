"""Revert-probes for the strategy manifest's invariant tests (#2394 §2).

    PYTHONPATH=. uv run python scripts/probe_2394_strategy_manifest.py

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
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "app" / "services" / "strategy_manifest.py"


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
        name="a strategy exists in the tree but is not registered",
        path=MANIFEST,
        old="""        S4_STRATEGY_ID: StrategyEntry(
            strategy_id=S4_STRATEGY_ID,
            identity=s4_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s4_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s4_signals,
        ),
""",
        new="",
        test="tests/test_strategy_manifest.py::TestManifestIsComplete::test_every_strategy_module_is_registered",
    ),
    Probe(
        name="an adapter drops the caller's reason code and defaults it",
        path=MANIFEST,
        old="    return s4_signals(series, universe=universe, masked_reason=masked_reason)",
        new='    return s4_signals(series, universe=universe, masked_reason="quarantined_bar")',
        test=(
            "tests/test_strategy_manifest.py::TestUniformInvocationEqualsTheDirectCall"
            "::test_the_reason_code_reaches_the_verdict"
        ),
    ),
    Probe(
        name="an adapter forwards to the wrong strategy's function",
        path=MANIFEST,
        old="    return s3_signals(series, universe=universe, close_reason=masked_reason)",
        new="    return s1_signals(series, universe=universe, close_reason=masked_reason)",
        test=(
            "tests/test_strategy_manifest.py::TestUniformInvocationEqualsTheDirectCall"
            "::test_close_reason_strategies_match"
        ),
    ),
    Probe(
        name="S-3's hold cap is dropped from its exit regime",
        path=MANIFEST,
        old=(
            "    return ExitRegime(signal_pair=True, level_based=False, "
            "max_hold_bars=S3_MAX_HOLD_BARS, rebalance_dates=None)"
        ),
        new="    return ExitRegime(signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=None)",
        test="tests/test_strategy_manifest.py::TestExitRegimeTableIsExecutable::test_regime_matches_the_spec_table",
    ),
    Probe(
        name="a per-series strategy silently accepts a rebalance calendar",
        path=MANIFEST,
        old="""    if decision_dates is not None:
        raise ValueError(""",
        new="""    if False:
        raise ValueError(""",
        test=(
            "tests/test_strategy_manifest.py::TestExitRegimeTableIsExecutable"
            "::test_a_per_series_strategy_refuses_a_calendar"
        ),
    ),
    Probe(
        name="a strategy declares a leg it does not emit",
        path=MANIFEST,
        old="""            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s4_exit_regime,""",
        new="""            strategy_class="per_series",
            signal_kinds=frozenset({"entry", "exit"}),
            exit_regime=_s4_exit_regime,""",
        test=(
            "tests/test_strategy_manifest.py::TestUniformInvocationEqualsTheDirectCall"
            "::test_emitted_kinds_are_the_declared_kinds"
        ),
    ),
    Probe(
        name="the completeness walk stops reading the strategy modules",
        path=REPO / "tests" / "test_strategy_manifest.py",
        old="            if path.name in cls._NOT_STRATEGIES:\n                continue",
        new="            if True:\n                continue",
        test="tests/test_strategy_manifest.py::TestManifestIsComplete::test_the_walk_finds_the_strategies",
    ),
)


def _run(test: str) -> int:
    """The targeted test's exit code. ⚠ Not piped — a pipe returns the pipe's
    status, which is the defect this repo has re-committed twice."""
    return subprocess.run(
        [sys.executable, "-m", "pytest", test, "-q", "--no-header", "-p", "no:cacheprovider"],
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

    baseline = _run("tests/test_strategy_manifest.py")
    print(f"restored suite exit {baseline}")
    return 0 if not failures and baseline == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
