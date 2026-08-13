"""Revert-probe the phase-4a invariant tests (#2240).

Run from repo root:

    uv run python scripts/probe_2240_outcome_resolver.py

Spec: ``docs/proposals/ta/2026-08-06-outcome-resolver.md`` acceptance 1-11 name
the probes explicitly ("revert-probed by making the walk return ``tp_hit`` on
that bar and asserting the test fails"). This is that harness.

WHY THIS IS COMMITTED
---------------------
An assertion nobody has watched fail is decoration, and the only evidence it is
load-bearing is a run that broke the code and watched the test catch it. Keeping
the harness in ``/tmp`` means the next person to touch
``app/services/outcome_resolver.py`` has to re-derive nine injections from prose.
Same reason ``scripts/probe_def14a_high_identity_escape.py`` is committed.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it, and `-m "not db"` never collects it.

TWO GUARDS, AND WHY EACH IS NEEDED
----------------------------------
1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected — green proving the
   opposite of what it claims. Already a repo lesson; asserted, not trusted.

2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 catches
   matching nothing; it says nothing about whether the replacement changes
   behaviour. This harness's own first draft replaced ``return
   "window_truncated"`` with ``return None if False else "window_truncated"`` —
   anchor unique, file changed, test still passed, because the replacement
   returns the same value. It reported ``NOT CAUGHT``, which READS as a hole in
   the test rather than a hole in the probe, and the natural next move (write a
   redundant test, or weaken the code until the probe "works") makes things
   worse. The tell is a replacement that keeps the original expression anywhere
   inside it. See ``docs/review-prevention-log.md`` and
   ``.claude/skills/engineering/test-quality.md``.

⚠ **Some invariants cannot be broken at a single site**, so a probe takes a LIST
of (anchor, replacement) pairs. "Truncation must not be absorbed into
``expired``" needs the window clamp AND the exit-index clamp changed together —
either alone still refuses. That a probe needs two sites is itself evidence the
two guards are load-bearing together, worth knowing before somebody deletes one
as redundant.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SRC = Path("app/services/outcome_resolver.py")
TESTS = "tests/test_outcome_resolver.py"

#: (what the injected defect IS, [(anchor, replacement), ...], pytest -k selector)
PROBES: list[tuple[str, list[tuple[str, str]], str]] = [
    (
        "ambiguity resolved favourably",
        [
            (
                "        touched_stop, touched_target = low <= stop, high >= target\n"
                "        if touched_stop and touched_target:",
                "        touched_stop, touched_target = low <= stop, high >= target\n        if False:",
            )
        ],
        "test_one_bar_spanning_both_levels_is_ambiguous or "
        "test_ambiguity_is_not_resolved_favourably_at_any_tp_distance",
    ),
    (
        "expiry booked at the window's last close (same-bar fill)",
        [
            (
                "    exit_index = fill_index + levels.max_hold_bars\n",
                "    exit_index = fill_index + levels.max_hold_bars - 1\n",
            )
        ],
        "test_expiry_fills_at_the_open_after_the_window_not_the_last_close",
    ),
    (
        # ⚠ TWO sites. Clamping only the walk still runs off the exit bar and
        # refuses; clamping only the exit index still refuses inside the walk.
        # The invariant lives in both, which is why the probe changes both.
        "truncation absorbed into expired (window clamped to the corpus edge)",
        [
            (
                "    for index in range(fill_index, fill_index + levels.max_hold_bars):",
                "    for index in range(fill_index, min(fill_index + levels.max_hold_bars, n_bars)):",
            ),
            (
                "    exit_index = fill_index + levels.max_hold_bars\n",
                "    exit_index = min(fill_index + levels.max_hold_bars, n_bars - 1)\n",
            ),
        ],
        "test_a_window_running_off_the_series_is_unresolved_not_expired or "
        "test_expiry_needs_the_bar_after_the_window_or_it_is_truncated",
    ),
    (
        "gap-through no longer resolves at the open",
        [("        if bar_open <= stop:", "        if False:")],
        "test_gap_below_the_stop_resolves_at_the_open_even_when_the_high_clears_the_target",
    ),
    (
        "masking made whole-bar instead of per-field",
        [
            (
                '    exit_open = series.rows[exit_index].get("open")\n    if exit_open is None:',
                '    _r = series.rows[exit_index]\n    exit_open = _r.get("open")\n'
                '    if exit_open is None or _r.get("high") is None or _r.get("low") is None:',
            )
        ],
        "test_masking_is_per_field_a_masked_range_bar_still_serves_as_an_expiry_exit",
    ),
    (
        "declared reason collapsed into the NULL fallback",
        [
            (
                '        return _unresolved(masked_bar_reasons.get(index, "missing_bar_data"))',
                '        return _unresolved("missing_bar_data")',
            )
        ],
        "test_a_declared_reason_beats_the_null_fallback",
    ),
    (
        "segment boundary ignored",
        [
            (
                "        if segment_end_index is not None and index > segment_end_index:\n"
                '            return "series_break"',
                "        if False:\n            pass",
            )
        ],
        "test_a_window_crossing_the_segment_end_is_unresolved",
    ),
    (
        "fill bar excluded from the window",
        [
            (
                "    for index in range(fill_index, fill_index + levels.max_hold_bars):",
                "    for index in range(fill_index + 1, fill_index + levels.max_hold_bars + 1):",
            )
        ],
        "test_the_fill_bar_is_inside_the_window",
    ),
    (
        "entry price re-read instead of validated against the fill bar",
        [("    if entry_price != fill_open:", "    if False:")],
        "test_an_entry_price_disagreeing_with_the_fill_bars_open_raises",
    ),
]


def run(selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", TESTS, "-q", "-k", selector, "-p", "no:randomly"],
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
            verdict = "CAUGHT" if rc != 0 else "*** NOT CAUGHT ***"
            print(f"  {verdict:<20} {name}", flush=True)
            if rc == 0:
                failures.append(name)
    finally:
        # ⚠ Restored even on KeyboardInterrupt / an exception mid-probe. A
        # harness that can leave a tracked source file mutated is one Ctrl-C
        # away from a defect committed by accident.
        SRC.write_text(original)

    rc = run("test_")
    print(f"\n  restored suite: {'PASS' if rc == 0 else '*** FAIL ***'}", flush=True)
    if rc != 0:
        failures.append("restored suite does not pass")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
