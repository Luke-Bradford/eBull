"""Revert-probes for #2376 -- inject each defect the new tests guard, confirm CAUGHT.

A test that passes proves nothing until the defect it guards has been shown to
make it fail. Each probe edits the parser source in place, runs ONLY the tests
that should notice, and restores it in a ``finally``. A byte-for-byte copy is
also parked at ``_BACKUP`` for the one case ``finally`` cannot cover -- a SIGKILL
mid-probe, which leaves the injected defect on disk with every later gate
passing against it.

Three harness rules, all learned the hard way in this repo:

* every replacement asserts ``source.count(old) == 1`` first -- a probe whose
  pattern silently matches nothing reports CAUGHT for a mutation that was never
  applied;
* the verdict is taken from pytest's exit code 1 (tests failed) specifically,
  NOT from "non-zero". Exit code 4 is a usage error and means no test was
  evaluated at all, which also reads as non-zero (#2335's false 4/4).
* the mutation and the run BOTH sit inside the ``try``, so the restore covers a
  partial write and a Ctrl-C as well as a raising subprocess.

⚠ Do not ``git add`` while this is running -- it stages whichever defect is
injected at that instant, and the gates all read the working tree, which the
harness has already restored by the time they run.

    PYTHONPATH=. uv run python -m scripts.probe_2376_revert
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

_TARGET = Path("app/providers/implementations/sec_def14a.py")
_BACKUP = Path("/tmp/2376_sec_def14a_backup.py")
_TESTS = "tests/test_sec_def14a_parser.py"

# (label, old, new, -k expression selecting the tests that must notice)
_PROBES: tuple[tuple[str, str, str, str], ...] = (
    (
        "layout grid stops expanding colspan (caption covers one column only)",
        "            for slot in range(column, column + colspan):\n                row[slot] = text",
        "            row[column] = text",
        "test_layout_grid_matches_the_html_table_model or test_bare_percent_is_recovered_end_to_end",
    ),
    (
        "layout grid stops reading self-closing <td/> cells",
        "        for match in _ANY_CELL_RE.finditer(tr_match.group(1)):",
        "        for match in _CELL_RE.finditer(tr_match.group(1)):",
        "test_layout_grid_matches_the_html_table_model or test_bare_percent_is_recovered_end_to_end",
    ),
    (
        "percent caption test drops the strong-AMOUNT exclusion",
        "    if any(keyword in lowered for keyword in _STRONG_SHARES_KEYWORDS):\n        return False\n",
        "",
        "test_merged_amount_and_percent_caption_is_not_a_percent_column",
    ),
    (
        "attestation no longer requires a percent caption at all",
        "    if not percent_columns:\n        return {}",
        "    if not percent_columns:\n        percent_columns = {max(max(r) for r in rows if r)}",
        "test_a_table_with_no_percent_caption_recovers_nothing",
    ),
    (
        "ambiguous name-prefix collisions are guessed instead of dropped",
        "    for key in ambiguous:\n        found.pop(key, None)",
        "    ambiguous.clear()",
        "test_an_ambiguous_name_prefix_is_dropped_rather_than_guessed"
        " or test_a_repeated_class_label_collides_with_itself_and_drops_out",
    ),
    (
        "the row is keyed on its FIRST text cell, so 229.403 column 1 shadows the holder",
        "            if len(name_keys) == 2:\n                break",
        "            if len(name_keys) == 1:\n                break",
        "test_a_leading_title_of_class_column_does_not_shadow_the_holder",
    ),
    (
        "the rescue overwrites a percent another rescue already found",
        "        if percent is None and attest_percent and table.table_html:",
        "        if attest_percent and table.table_html:",
        "test_an_existing_percent_is_never_overwritten",
    ),
    (
        "the rescue is allowed to run during the ELIGIBILITY probe",
        "        attest_percent=rows is not None,",
        "        attest_percent=True,",
        "test_the_rescue_cannot_change_which_tables_are_selected",
    ),
    (
        "a dual-class table's two percent runs are pooled instead of refused",
        "    if max(percent_columns) - min(percent_columns) + 1 != len(percent_columns):\n        return {}",
        "    percent_columns = set(percent_columns)",
        "test_a_dual_class_table_attests_nothing",
    ),
    (
        "the caption scan stops at the FIRST percent-captioned row",
        "            percent_columns |= columns\n"
        '            captions |= {_FOOTNOTE_RE.sub("", row[column]).strip().lower() for column in columns}\n'
        "            header_index = max(header_index, index)",
        "            percent_columns |= columns\n"
        "            header_index = max(header_index, index)\n"
        "            break",
        "test_two_distinct_percent_captions_attest_nothing",
    ),
    (
        "a threshold phrase like '5% Beneficial owners' counts as a caption",
        "    if _THRESHOLD_LABEL_RE.search(lowered):\n        return False",
        "    pass",
        "test_a_threshold_phrase_is_not_a_column_caption or test_bare_percent_is_recovered_end_to_end",
    ),
    (
        "the threshold guard tests only the SIGN, so '5 Percent Beneficial Owners' is a caption",
        r"(?:\d|\bfive\b|\bten\b)\s*(?:%|percent\b)",
        r"\d\s*%",
        "test_a_threshold_phrase_spelled_as_a_word_is_not_a_caption_either",
    ),
    (
        "a '1*' threshold cell is read as a flat 1 instead of being declined",
        '            if "*" in text and any(character.isdigit() for character in text):\n'
        "                percent = None\n"
        "                break",
        "            pass",
        "test_a_digit_beside_an_asterisk_states_a_threshold_not_a_holding",
    ),
    (
        "a percent equal to the row's own share count is accepted",
        "            if percent is not None and shares is not None and percent == shares:\n"
        "                percent = None",
        "            pass",
        "test_a_percent_equal_to_the_share_count_is_the_same_cell_twice",
    ),
)


def _run(selector: str) -> int:
    return subprocess.run(
        ["uv", "run", "pytest", _TESTS, "-q", "-p", "no:randomly", "-k", selector],
        capture_output=True,
        text=True,
    ).returncode


def main() -> int:
    original = _TARGET.read_text()
    shutil.copy(_TARGET, _BACKUP)

    baseline = _run("LayoutAttested")
    if baseline != 0:
        print(f"BASELINE NOT GREEN (exit {baseline}) -- fix that before probing")
        return 1
    print("baseline: LayoutAttested green\n")

    caught = 0
    for label, old, new, selector in _PROBES:
        source = _TARGET.read_text()
        occurrences = source.count(old)
        if occurrences != 1:
            print(f"NOT APPLIED  {label}\n             pattern occurs {occurrences}x, expected 1")
            _TARGET.write_text(original)
            continue
        # The restore is the ONLY thing between a crash here and a permanently
        # mutated parser, so it goes in `finally` and the mutation goes inside
        # the `try` with it (review WARNING on PR #2405).
        try:
            _TARGET.write_text(source.replace(old, new))
            exit_code = _run(selector)
        finally:
            _TARGET.write_text(original)
        assert _TARGET.read_text() == original, "restore failed"
        # 1 == tests ran and failed. 4 == usage error, nothing evaluated.
        verdict = "CAUGHT" if exit_code == 1 else f"NOT CAUGHT (exit {exit_code})"
        caught += exit_code == 1
        print(f"{verdict:24} {label}")

    restored = _run("LayoutAttested")
    print(f"\nrestored baseline: {'green' if restored == 0 else f'BROKEN (exit {restored})'}")
    print(f"{caught}/{len(_PROBES)} CAUGHT")
    return 0 if caught == len(_PROBES) and restored == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
