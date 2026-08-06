"""Revert-probe the #2333 invariant tests — strategy identity covers the indicator rule set.

Run from repo root (the DB probe needs the test cluster up):

    docker compose --profile test up -d postgres-test
    uv run python scripts/probe_2333_strategy_identity_input_rule_sets.py

Sister to ``scripts/probe_2240_outcome_ledger.py``, whose two guards apply
unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything — ``X`` → ``A if False
   else X`` passes guard 1 and proves nothing (prevention log, #2240 4a).

⚠ NOT A TEST, and it must never become one: it mutates tracked source files on
disk. CI does not run it.

⚠ This harness edits ``app/services/strategy_registry.py``, whose source hash is
part of ``StrategyIdentity.version``. Do NOT run it beside a full-population
sweep — the #2240 S-1 lesson: a concurrent run would stamp its rows with an
INJECTED version, and a start-vs-end hash check does not see it because the
mutation is restored before the sweep ends.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **sql/257's shape CHECK.** Same reason ``probe_2240_outcome_ledger.py`` gives
for CASCADE: the test database is built from a migrated template, so editing the
migration file does not change an already-built template and a source-edit probe
would report NOT CAUGHT for a constraint that is present and working. It is
exercised against the REAL constraint instead — nine cases in
``tests/test_strategy_signals_ledger.py`` (empty object, blank/whitespace/
non-string/null version, array, scalar, explicit NULL, and the column omitted
entirely), each of which was confirmed rejected on the dev cluster before the
migration landed.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REGISTRY = Path("app/services/strategy_registry.py")
LEDGER = Path("app/services/signal_ledger.py")

REGISTRY_TESTS = "tests/test_strategy_registry.py"
LEDGER_TESTS = "tests/test_signal_ledger.py"
LEDGER_DB_TESTS = "tests/test_signal_ledger_writer_db.py"

#: (what the injected defect IS, source file, [(anchor, replacement), ...], test file, -k selector)
PROBES: list[tuple[str, Path, list[tuple[str, str]], str, str]] = [
    (
        # ⚠ THE #2333 DEFECT ITSELF, re-injected: the state main was in.
        "the indicator rule set is dropped from the identity hash",
        REGISTRY,
        [('                "input_rule_sets": dict(self.input_rule_set_versions),\n', "")],
        REGISTRY_TESTS,
        "test_the_indicator_rule_set_changes_the_version",
    ),
    (
        # The registry names no rule set, so nothing a strategy reads is
        # covered — the omission this constant exists to make impossible.
        "INPUT_RULE_SETS emptied, so a strategy's indicators are uncovered",
        REGISTRY,
        [
            (
                '    {\n        "indicator_series": INDICATOR_SERIES_RULE_SET_VERSION,\n    }\n',
                "    {}\n",
            )
        ],
        REGISTRY_TESTS,
        "test_every_versioned_pipeline_a_strategy_reads_is_in_the_hash",
    ),
    (
        "the registry constant becomes mutable, so any importer can retag every strategy",
        REGISTRY,
        [("INPUT_RULE_SETS: Mapping[str, str] = MappingProxyType(\n", "INPUT_RULE_SETS: Mapping[str, str] = dict(\n")],
        REGISTRY_TESTS,
        "test_the_registry_constant_is_read_only",
    ),
    (
        # A second source of truth: the stored column stops being the object
        # the version hash was built from.
        "the writer invents its own rule-set mapping instead of reading the identity's",
        LEDGER,
        [
            (
                "                input_rule_set_versions=identity.input_rule_set_versions,\n",
                '                input_rule_set_versions={"indicator_series": "indicator-series-v1+000000000000"},\n',
            )
        ],
        LEDGER_TESTS,
        "test_input_rule_set_versions_come_from_the_identity",
    ),
    (
        "an empty rule-set mapping accepted (the column is present and records nothing)",
        LEDGER,
        [
            (
                "        if not isinstance(self.input_rule_set_versions, Mapping) or not self.input_rule_set_versions:",
                "        if False:",
            )
        ],
        LEDGER_TESTS,
        # ⚠ Narrowed to the two cases this line owns. A bare `test_rejects`
        # runs the whole matrix, so the harness would report CAUGHT if ANY
        # unrelated case failed — a probe is only evidence about the test it
        # actually names.
        "test_rejects and empty and mapping",
    ),
    (
        # ⚠ The narrow half. `not version` alone still rejects "", so ONLY the
        # whitespace case can see this — which is why that case exists rather
        # than being inferred from the empty-string one.
        "a whitespace-only version accepted, diverging from sql/257's CHECK",
        LEDGER,
        [
            (
                "            if not isinstance(version, str) or not version.strip():",
                "            if not isinstance(version, str) or not version:",
            )
        ],
        LEDGER_TESTS,
        "test_rejects and whitespace",
    ),
    (
        "the field gets a default, so a writer can forget it",
        LEDGER,
        [
            ("from decimal import Decimal\n", "from decimal import Decimal\nfrom types import MappingProxyType\n"),
            (
                "    input_rule_set_versions: Mapping[str, str]\n",
                "    input_rule_set_versions: Mapping[str, str] = "
                'MappingProxyType({"indicator_series": "forgotten"})\n',
            ),
        ],
        LEDGER_TESTS,
        "test_input_rule_set_versions_has_no_default",
    ),
    (
        "the INSERT stops writing the column",
        LEDGER,
        [
            (
                "        fill_price, universe, input_rule_set_versions\n",
                "        fill_price, universe\n",
            ),
            (
                "        %(fill_price)s, %(universe)s, %(input_rule_set_versions)s\n",
                "        %(fill_price)s, %(universe)s\n",
            ),
        ],
        LEDGER_DB_TESTS,
        "test_stored_fill_price_is_open_of_the_next_bar_in_price_daily",
    ),
]


def run(test_file: str, selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", test_file, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def main() -> int:
    originals = {path: path.read_text() for path in (REGISTRY, LEDGER)}
    failures: list[str] = []
    try:
        for name, src, edits, test_file, selector in PROBES:
            mutated = originals[src]
            bad_anchor = False
            for old, new in edits:
                count = mutated.count(old)
                if count != 1:
                    failures.append(f"{name}: anchor occurs {count} times, expected exactly 1 — probe proves nothing")
                    bad_anchor = True
                    break
                mutated = mutated.replace(old, new)
            if bad_anchor:
                continue
            src.write_text(mutated)
            rc = run(test_file, selector)
            src.write_text(originals[src])
            verdict = "CAUGHT" if rc != 0 else "*** NOT CAUGHT ***"
            print(f"  {verdict:<20} {name}", flush=True)
            if rc == 0:
                failures.append(name)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident — and here the mutated file's hash IS a stored version.
        for path, text in originals.items():
            path.write_text(text)

    restored = {
        REGISTRY_TESTS: run(REGISTRY_TESTS, "test_"),
        LEDGER_TESTS: run(LEDGER_TESTS, "test_"),
        LEDGER_DB_TESTS: run(LEDGER_DB_TESTS, "test_"),
    }
    print("", flush=True)
    for test_file, rc in restored.items():
        print(f"  restored {test_file}: {'PASS' if rc == 0 else '*** FAIL ***'}", flush=True)
    if any(restored.values()):
        failures.append("restored suite does not pass")

    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
