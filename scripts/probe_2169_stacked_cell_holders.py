"""Revert-probe the one-``<tr>``-N-holders invariant tests (#2169).

    PYTHONPATH=. uv run python scripts/probe_2169_stacked_cell_holders.py

Same two guards as ``scripts/probe_2240_s1_momentum.py``:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**

⚠ **The verdict gates on exit code 1, not on "non-zero"** (#2214, 2026-08-07):
pytest exits **4** on a USAGE error, which is not a caught defect but reads as
one under a ``rc != 0`` test — that reported a false 4/4 CAUGHT. Anything other
than 0 or 1 is printed as a harness fault. For the same reason the subprocess
passes ``-n 0`` rather than ``-p no:xdist``, which conflicts with this repo's
``addopts``.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier; no database is needed.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``_cell_segments`` itself.** Every gate consumes it, so any mutation of it
fails several tests at once and the probe attributes the catch to whichever
selector ran — it discriminates nothing. Its behaviour is pinned by the gate
probes below, each of which needs it correct to fail for the stated reason.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path("app/providers/implementations/sec_def14a.py")
TESTS = "tests/test_sec_def14a_parser.py"

#: (what the injected defect IS, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        "the expansion is not wired into the row loop at all (pre-#2169 behaviour)",
        [
            (
                "        raw_rows.extend(split if split is not None else [source_row])",
                "        raw_rows.append(source_row)",
            )
        ],
        "test_one_tr_holding_two_holders_yields_two_rows",
    ),
    (
        # Gate 1. Without the veto the split runs on the OTHER column's count
        # and both amounts are dropped into blanked cells.
        "a partially-parsing value stack no longer vetoes the split",
        [
            (
                '    if "veto" in (shares_state, percent_state):\n        return None',
                "    if False:\n        return None",
            )
        ],
        "test_a_partially_numeric_value_stack_is_not_split",
    ),
    (
        # Gate 2. Two amounts against three percents: whichever column is
        # consulted first silently wins and the name blocks align to it.
        "the two value columns no longer have to agree on the holder count",
        [
            (
                "        if len(share_segs) != len(percent_segs):\n            return None",
                "        if False:\n            return None",
            )
        ],
        "test_value_columns_that_disagree_on_the_count_are_not_split",
    ),
    (
        # Gate 3, attacked at the construction rather than the comparison:
        # treating EVERY line as its own holder is the obvious alternative
        # split, and it is what #2140 D5's flatten exists to prevent.
        "the name cell splits on every line instead of on blank lines",
        [
            (
                "        if line.strip():\n            current.append(line.strip())\n        elif current:",
                "        if line.strip():\n            blocks.append(line.strip())\n        elif current:",
            )
        ],
        "test_a_name_cell_with_no_blank_line_separator_is_not_split",
    ),
    (
        # The NAME side driving the count is the whole hazard of this ticket —
        # #2140 D5 measured 704 rows / 117 instruments split across two holder
        # identities by an interior render wrap.
        "the holder count falls back to the NAME blocks when no value column stacks",
        [
            (
                "    else:\n        return None\n\n    name_blocks =",
                "    else:\n        count = len(_stacked_name_blocks(cells[name_idx]))\n\n    name_blocks =",
            )
        ],
        "test_a_wrapped_name_over_a_single_amount_is_not_split",
    ),
    (
        "an unaligned non-value cell is carried through whole instead of blanked",
        [
            (
                '            row.append(segments[ordinal] if len(segments) == count else "")',
                "            row.append(segments[ordinal] if len(segments) == count else cell)",
            )
        ],
        "test_a_stacked_row_distributes_every_aligned_cell_by_ordinal",
    ),
    (
        "the percent column alone can no longer evidence the split",
        [
            (
                '    elif percent_state == "stack":\n        count = len(percent_segs)',
                "    elif False:\n        count = len(percent_segs)",
            )
        ],
        "test_a_percent_only_stack_splits_when_the_amounts_are_absent",
    ),
    (
        # The floor is what keeps the expansion inert on the ordinary corpus:
        # at < 1 a single-value row "stacks" at count 1 and every row in every
        # Item 403 table goes through the split path.
        "the two-line floor drops to one, so an ordinary row is a stack of one",
        [("    if len(segments) < 2 or not any", "    if len(segments) < 1 or not any")],
        "test_an_ordinary_single_holder_row_is_never_split",
    ),
]


def run(selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def main() -> int:
    original = SRC.read_text()
    failures: list[str] = []
    try:
        for name, edits, selector in PROBES:
            mutated = original
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
            SRC.write_text(mutated)
            rc = run(selector)
            SRC.write_text(original)
            if rc == 1:
                verdict = "CAUGHT"
            elif rc == 0:
                verdict = "*** NOT CAUGHT ***"
                failures.append(name)
            else:
                verdict = f"*** HARNESS FAULT rc={rc} ***"
                failures.append(f"{name}: pytest exited {rc} (usage/collection error, not a catch)")
            print(f"  {verdict:<28} {name}", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        SRC.write_text(original)

    rc_suite = run("TestStackedCellHolders")
    print(f"\n  restored suite: {'PASS' if rc_suite == 0 else f'*** rc={rc_suite} ***'}", flush=True)
    if rc_suite:
        failures.append("restored suite does not pass")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
