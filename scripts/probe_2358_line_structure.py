"""Revert-probe the block-level line-structure invariants (#2358).

    PYTHONPATH=. uv run python scripts/probe_2358_line_structure.py

Same two guards as ``scripts/probe_2169_stacked_cell_holders.py``:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**

⚠ **The verdict gates on exit code 1, not on "non-zero"**: pytest exits **4** on
a USAGE error and **2** on a collection error, neither of which is a caught
defect but both of which read as one under a ``rc != 0`` test. Anything other
than 0 or 1 is printed as a harness fault.

⚠ NOT A TEST, and it must never become one: it mutates a tracked source file on
disk. CI does not run it. Everything here is pure-tier; no database is needed.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``_BLOCK_ELEMENTS`` / ``_HTML_TAG_RE``.** Emptying either makes almost every
test in the file fail at once, so the probe would attribute the catch to
whichever selector happened to run and would discriminate nothing. What each
element list has to achieve is pinned by the four probes below instead, each of
which names the specific line structure it destroys.
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
        # The defect itself, restored: <br> back to a space on the line grid.
        "the line grid is not built at all — every tag is a space again (pre-#2358)",
        [
            (
                "_strip_inline_html(inner, block_breaks=True) if _LINE_STRUCTURE_TAG_RE.search(inner) else flat",
                "flat",
            )
        ],
        "test_br_stacked_amounts_do_not_glue_into_one_number",
    ),
    (
        # The short-circuit's own precondition. Inverted, the line pass is
        # skipped on precisely the cells that need it.
        "the line pass is skipped on cells that DO carry line-structure markup",
        [
            (
                "if _LINE_STRUCTURE_TAG_RE.search(inner) else flat",
                "if not _LINE_STRUCTURE_TAG_RE.search(inner) else flat",
            )
        ],
        "test_br_stacked_amounts_do_not_glue_into_one_number",
    ),
    (
        # The split reading the FLAT row is the state main is in: the stack is
        # invisible there, so the two holders never separate.
        "the holder split reads the flat grid instead of the line grid",
        [
            (
                "        split = _split_stacked_holder_row(line_row, name_idx=name_idx",
                "        split = _split_stacked_holder_row(source_row, name_idx=name_idx",
            )
        ],
        "test_br_stacked_amounts_do_not_glue_into_one_number",
    ),
    (
        # Without the collapse the one-holder-N-classes residue keeps parsing
        # its two class amounts as one 12-digit number.
        "a stacked value cell the split declined is left glued",
        [
            (
                "        raw_rows.append(_collapse_stacked_value_cells(source_row, line_row))",
                "        raw_rows.append(source_row)",
            )
        ],
        "test_one_holder_across_two_classes_reads_as_the_first_class",
    ),
    (
        # The stack gate is what keeps the collapse off the NAME column without
        # a special case for it — relaxed to "any multi-line cell", a wrapped
        # holder name is truncated to its first line (#2140 D5: 704 rows / 117
        # instruments split across two identities by exactly that shape).
        "the collapse fires on any multi-line cell instead of a pure value stack",
        [
            (
                '_value_stack_state(segments, _is_whole_share_segment) != "stack" or',
                "len(segments) < 2 or",
            )
        ],
        "test_a_footnote_line_above_the_amount_is_not_read_as_the_amount",
    ),
    (
        # The corrective precondition. Without it the collapse RESURRECTS rows
        # main drops, and the full-population A/B showed the one real instance
        # is a mangled two-caption holder identity.
        "the collapse resurrects rows that never parsed instead of only correcting",
        [
            (
                "        if index >= len(flat_row) or _parse_share_count(flat_row[index]) is None:",
                "        if index >= len(flat_row):",
            )
        ],
        "test_a_row_that_never_parsed_is_not_resurrected_by_the_collapse",
    ),
    (
        # The pin. Injecting the line structure into `rows` is the obvious
        # "simplification" — and it re-cuts every SCT name cell and every
        # multi-line Item 403 caption.
        "the line structure leaks into the flat grid the SCT path and the scorer read",
        [
            (
                "            flat = _strip_inline_html(inner)",
                "            flat = _strip_inline_html(inner, block_breaks=True)",
            )
        ],
        "test_the_flat_grid_the_sct_path_reads_carries_no_tag_derived_newline or "
        "test_a_multi_line_header_caption_still_matches_its_prescribed_phrase",
    ),
    (
        # Codex checkpoint 2's finding. A block whose only content is a <br> is
        # an EMPTY block; without the pre-pass it reads as adjacent boundaries
        # and two stacked owners merge into one holder identity.
        "a break-only block is no longer recognised as a blank line",
        [("        raw = _EMPTY_BLOCK_RE.sub(_BLANK_LINE, raw)\n", "")],
        "test_br_stacked_amounts_do_not_glue_into_one_number",
    ),
    (
        # Same shape, rendered without the enclosing block.
        "a run of consecutive breaks is no longer a blank line",
        [("        raw = _BREAK_RUN_RE.sub(_BLANK_LINE, raw)\n", "")],
        "test_an_empty_block_is_a_blank_line_and_adjacent_breaks_are_one",
    ),
    (
        # The blank-line separator #2169 splits on is an EMPTY BLOCK. Widening
        # the run to swallow the space between two sentinels erases it.
        "the sentinel run widens over whitespace, erasing the empty-block blank line",
        [(r'rf"\n*{_BREAK_SENTINEL}[\n{_BREAK_SENTINEL}]*"', r'rf"[\s]*{_BREAK_SENTINEL}[\s{_BREAK_SENTINEL}]*"')],
        "test_an_empty_block_is_a_blank_line_and_adjacent_breaks_are_one",
    ),
    (
        # The opposite error: not collapsing at all, so `</p><p>` and a
        # trailing `<br/>` before a close tag fabricate a blank line that the
        # holder split then reads as a second owner.
        "adjacent tag breaks are no longer collapsed, fabricating a blank line",
        [
            (
                '        decoded = _SENTINEL_RUN_RE.sub(_BREAK_SENTINEL, decoded).replace(_BREAK_SENTINEL, "\\n")',
                '        decoded = decoded.replace(_BREAK_SENTINEL, "\\n")',
            )
        ],
        "test_an_empty_block_is_a_blank_line_and_adjacent_breaks_are_one",
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

    rc_suite = run("TestBlockLevelLineStructure or TestStackedCellHolders")
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
