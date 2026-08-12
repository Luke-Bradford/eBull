"""#2614 — a sealed outcome opener must call a declaration check, or say why not.

#2599 put its gate at the ledger chokepoint and documented that this closed
"every path we have written". It did not: three scripts open sealed outcomes
without touching the ledger at all, because they compute their own statistics
from raw price windows and store no result row. The premise that opening an
outcome always goes through the ledger was never checked.

⚠ THIS IS A TEST AND NOT A CONVENTION ON PURPOSE. "Remember to call the helper"
is exactly what failed five consecutive times before #2599 existed, so the rule
that a new sealed opener must be gated is enforced by something that fails.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Final

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"

#: Any of these being CALLED means the script consults #2599's declaration.
_GATE_CALLS: Final[frozenset[str]] = frozenset(
    {
        "require_outcome_access",
        "require_outcome_gate",
        "verify_outcome_access_provenance",
    }
)

#: ⚠ EXPLICIT, REASONED, AND NOT EMPTY — the two openers that ran BEFORE
#: `TRIAL_REGISTER_CUTOFF` (2026-08-12 07:00Z). #2599 does not retroactively
#: invalidate them (`sql/333`: "a trial with no row here behaves exactly as it
#: did before this migration") and #2600's reconstruction already charged both to
#: the register, so their searches are counted. Re-gating them is follow-up work,
#: and this mapping is the visible record of that debt rather than a silent
#: omission.
_PRE_CUTOFF_UNGATED: Final[dict[str, str]] = {
    "verify_2476_pead_outcomes.py": (
        "ran before TRIAL_REGISTER_CUTOFF; charged as pead-historical-sue-net-income-v1 (8, exact)"
    ),
    "verify_2480_insider_outcomes.py": (
        "ran before TRIAL_REGISTER_CUTOFF; charged as insider-purchase-forward-returns-first-look-2026-08-09 (4, exact)"
    ),
}


def _sealed_outcome_scripts() -> list[Path]:
    return sorted(set(_SCRIPTS.glob("evaluate_*.py")) | set(_SCRIPTS.glob("*_outcomes.py")))


def _called_names(source: str) -> set[str]:
    """Every name appearing in CALL position anywhere in the module.

    ⚠ AN AST WALK, NOT A SUBSTRING SEARCH. `"require_outcome_access" in source`
    passes on a mention in a comment, a docstring, an unused import or a dead
    branch — a test that certifies precisely the thing it exists to catch.
    """
    called: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name):
            called.add(func.id)
        elif isinstance(func, ast.Attribute):
            called.add(func.attr)
    return called


def test_the_glob_still_finds_the_known_sealed_openers() -> None:
    """A glob that matches nothing would make every assertion below vacuous."""

    names = {path.name for path in _sealed_outcome_scripts()}
    assert {
        "evaluate_2582_schedule13d_outcomes.py",
        "run_2582_schedule13d_outcomes.py",
        *_PRE_CUTOFF_UNGATED,
    } <= names


def test_every_sealed_outcome_script_calls_a_declaration_check_or_is_allowlisted() -> None:
    ungated = [
        path.name
        for path in _sealed_outcome_scripts()
        if path.name not in _PRE_CUTOFF_UNGATED and not (_called_names(path.read_text()) & _GATE_CALLS)
    ]
    assert ungated == [], (
        f"sealed outcome openers with no declaration check: {ungated}. A script that reads forward outcome "
        f"windows must call one of {sorted(_GATE_CALLS)}, or be added to _PRE_CUTOFF_UNGATED with the reason."
    )


def test_the_allowlist_names_only_scripts_that_still_exist() -> None:
    """An allowlist entry for a deleted script silently widens the exemption."""

    present = {path.name for path in _sealed_outcome_scripts()}
    assert set(_PRE_CUTOFF_UNGATED) <= present


def test_c4_is_gated_rather_than_allowlisted() -> None:
    """The trial this ticket exists for must be on the enforced side."""

    assert "evaluate_2582_schedule13d_outcomes.py" not in _PRE_CUTOFF_UNGATED
    called = _called_names((_SCRIPTS / "evaluate_2582_schedule13d_outcomes.py").read_text())
    assert "require_outcome_access" in called
    assert "verify_outcome_access_provenance" in called
