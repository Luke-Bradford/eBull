"""Revert-probes for #2176 — inject each defect the new tests guard.

Three injections, because the guard has three distinct ways to be wrong and one
probe would only prove the first:

  A. the per-row test is ABSENT      -> the junk rows are stored again
  B. the test matches a SUBSTRING    -> it eats the 229.403(b) Instruction 5 row
  C. the test is the POSITIVE one    -> it eats genuine holders (#2176 §2)
  D. class DESIGNATORS count as words -> 'Class B ...' escapes where 'Class A
     ...' does not, and the per-row prune then lifts an all-junk table over
     the identity floor (Codex checkpoint 2)

Each probe names the tests that MUST fail and the tests that MUST still pass —
a probe whose whole file goes red proves much less than one that moves exactly
the cases it targets.

⚠ Gate on exit code 1. pytest exits 4 on a USAGE error, which looks like a pass
here if the check is ``!= 0`` — it reported a false 4/4 CAUGHT on #2214. Runs
with ``-n 0`` for the same reason (``addopts`` carries ``-n``).

    PYTHONPATH=. uv run python scripts/probe_2176_item403_row_owner.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_PARSER = Path("app/providers/implementations/sec_def14a.py")
_TESTS = "tests/test_sec_def14a_parser.py"
_CLASS = "TestItem403RowIsABeneficialOwner"

_GUARD = "        if _is_instrument_not_owner(holder_name):\n"
_DESIGNATOR = '    if "class" in words or "series" in words:\n'

_PROBES = (
    (
        "A: guard ABSENT",
        _GUARD,
        "        if False:  # PROBE A — per-row owner test removed\n",
        (
            "test_a_presentation_total_row_is_not_stored_as_a_holder",
            "test_a_title_of_class_value_in_the_name_column_is_not_a_holder",
            "test_dropping_class_rows_lifts_a_genuine_table_over_the_identity_floor",
        ),
        (
            "test_the_instruction_5_group_row_survives",
            "test_a_bare_entity_name_without_a_corporate_designator_survives",
        ),
    ),
    (
        "B: guard widened to a SUBSTRING match on the aggregate noun",
        _GUARD,
        '        if "total" in holder_name.lower():  # PROBE B — substring, not whole-name\n',
        ("test_the_instruction_5_group_row_survives",),
        (
            "test_a_presentation_total_row_is_not_stored_as_a_holder",
            "test_a_bare_entity_name_without_a_corporate_designator_survives",
        ),
    ),
    (
        "C: guard inverted to the POSITIVE identity predicate",
        _GUARD,
        "        if not _is_beneficial_owner_identity(holder_name):  # PROBE C — positive test per row\n",
        ("test_a_bare_entity_name_without_a_corporate_designator_survives",),
        ("test_a_presentation_total_row_is_not_stored_as_a_holder",),
    ),
    (
        "D: class DESIGNATOR letters counted as ordinary words",
        _DESIGNATOR,
        "    if False:  # PROBE D — designator not stripped\n",
        ("test_a_class_designator_letter_does_not_rescue_a_title_of_class_row",),
        (
            "test_a_real_entity_whose_name_contains_series_is_not_a_title_of_class",
            "test_a_presentation_total_row_is_not_stored_as_a_holder",
        ),
    ),
)


def _pytest(node_ids: tuple[str, ...]) -> int:
    cmd = ["uv", "run", "pytest", _TESTS, "-q", "-n", "0", "-p", "no:randomly", "-k", " or ".join(node_ids)]
    return subprocess.run(cmd, capture_output=True, text=True).returncode


def main() -> int:
    original = _PARSER.read_text()
    for label, anchor in (("guard", _GUARD), ("designator", _DESIGNATOR)):
        # A probe that silently matches nothing proves nothing.
        if original.count(anchor) != 1:
            print(f"ABORT: {label} anchor appears {original.count(anchor)} times, expected exactly 1")
            return 2

    caught = 0
    try:
        for label, old, new, must_fail, must_pass in _PROBES:
            _PARSER.write_text(original.replace(old, new))
            fail_rc = _pytest(must_fail)
            pass_rc = _pytest(must_pass)
            ok = fail_rc == 1 and pass_rc == 0
            caught += ok
            verdict = "CAUGHT" if ok else "NOT CAUGHT"
            print(f"  probe {label}")
            print(f"    targeted rc={fail_rc} (want 1)  bystanders rc={pass_rc} (want 0)  -> {verdict}")
    finally:
        _PARSER.write_text(original)

    restored_rc = subprocess.run(
        ["uv", "run", "pytest", _TESTS, "-q", "-n", "0", "-k", _CLASS],
        capture_output=True,
        text=True,
    ).returncode
    print(f"\nrestored suite rc={restored_rc} (want 0)")
    print(f"{caught}/{len(_PROBES)} CAUGHT")
    return 0 if caught == len(_PROBES) and restored_rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
