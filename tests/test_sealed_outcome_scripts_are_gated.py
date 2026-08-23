"""#2614 — a sealed outcome opener must call a declaration check, or say why not.

#2599 put its gate at the ledger chokepoint and documented that this closed
"every path we have written". It did not: three scripts open sealed outcomes
without touching the ledger at all, because they compute their own statistics
from raw price windows and store no result row. The premise that opening an
outcome always goes through the ledger was never checked.

⚠ THIS IS A TEST AND NOT A CONVENTION ON PURPOSE. "Remember to call the helper"
is exactly what failed five consecutive times before #2599 existed, so the rule
that a new sealed opener must be gated is enforced by something that fails.

⚠⚠ #2830 — MEMBERSHIP IS NOW DECIDED BY PROVENANCE, NOT BY FILENAME. Until this
was fixed the verdict was an AST walk (chosen over a substring search precisely
because a substring "passes on a mention in a comment, a docstring, an unused
import or a dead branch") while MEMBERSHIP was a filename glob — the rigour
applied to the wrong half. `scripts/measure_2827_gross_vs_net.py` opens the
withheld side and was invisible to this file purely because it is called
`measure_*`. It was gated because Codex caught it at checkpoint 2, not because
anything here fired.

Same shape as the scan-basis arc (#2803/#2806/#2807/#2809/#2811/#2814), whose
recorded lesson is *"guards key on AST PROVENANCE, not naming"*.

⚠ THE DOOR IS `_resolve_invocation_window`, AND THAT IS A SOURCE RULE RATHER
THAN A GUESS. `app/services/backtest_run.py:3528` makes the hold-out reachable
only through a registered `evidence_window_id` WITH `holdout_requested` — it
raises on either alone. So "this module opens the withheld side" is exactly
"it passes one of those two to a backtest entry point", and that is what
`_opens_sealed_outcome` looks for.

⚠⚠ THREE ACCEPTED GATE FORMS, NOT ONE, AND THE THIRD IS EASY TO MISS.
`run_backtest` does not require a script-level gate call: it takes the
`holdout_purpose` / `holdout_accessed_by` audit pair, `_check_holdout_pairing`
enforces both-or-neither, and the engine writes the access row itself. A rule
accepting only the script-level doors would have failed
`run_2825_decisive_holdout_evidence.py`, `verify_2429_total_return.py` and
`benchmark_2488_evidence_refresh.py` — all three correctly audited. Measured
before this change shipped; reporting them as ungated would have been three
false findings.
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

#: ⚠ EMPTY SINCE #2616, AND EMPTY IS THE STATE WORTH KEEPING. The two pre-cutoff
#: openers (`verify_2476_pead_outcomes.py`, `verify_2480_insider_outcomes.py`)
#: were the visible record of a debt — first looks charged by #2600's
#: reconstruction, re-runs ungated. #2616 gated their re-runs through
#: `scripts/sealed_rerun_gate.py` (a NEW register entry per re-run, a frozen
#: #2599 declaration, a `read` access row), so the rule now holds with no
#: exemption. A new entry here needs the pre-cutoff justification the old ones
#: had; nothing that runs after `TRIAL_REGISTER_CUTOFF` qualifies.
#:
#: ⚠ The retired insider entry mis-attributed its charge to
#: `insider-purchase-forward-returns-first-look-2026-08-09` — the register
#: charges that run as `form4-code-p-opportunistic-purchase-v1` (7, exact); the
#: first-look entry is `scripts/verify_2437_insider_forward_returns.py`'s.
_PRE_CUTOFF_UNGATED: Final[dict[str, str]] = {}


#: Backtest entry points that can resolve the hold-out namespace. Anything that
#: reaches the withheld side goes through one of these — `_resolve_invocation_window`
#: is the door itself, the others accept `evidence_window_id` and pass it down.
_BACKTEST_ENTRY_POINTS: Final[frozenset[str]] = frozenset(
    {
        "run_backtest",
        "evaluate_arm",
        "evaluate_level_arms",
        "evaluate_strategy",
        "_resolve_invocation_window",
    }
)

#: Supplied TOGETHER to a backtest entry point, these make the engine write the
#: access row itself (`_check_holdout_pairing`, `backtest_run.py:3497`). This is a
#: real gate, not a bypass — see the module docstring's third-form warning.
_AUDIT_FIELDS: Final[frozenset[str]] = frozenset({"holdout_purpose", "holdout_accessed_by"})

#: Records the look without asserting a declaration permits it. Accepted
#: deliberately: #2829 shows the ten live strategies cannot pass
#: `require_outcome_access` at all, so accepting only the strict door would force
#: either a false declaration or a permanent allowlist entry — both worse than
#: the thing being prevented.
_ACCESS_RECORD_CALLS: Final[frozenset[str]] = frozenset({"record_holdout_access"})


def _callee_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _is_not_literal(node: ast.expr, value: object) -> bool:
    """True unless the argument is written as exactly that literal.

    ``evidence_window_id=None`` and ``holdout_requested=False`` are the explicit
    in-sample spellings and must not select the module; anything else — a
    variable, a constant, an expression — is treated as possibly opening.
    """
    return not (isinstance(node, ast.Constant) and node.value is value)


def _opens_sealed_outcome(tree: ast.Module) -> bool:
    """Does this module ask a backtest entry point for the withheld side?

    ⚠ The callee is checked, not just the keyword name. A bare
    ``evidence_window_id=...`` keyword match also fires on dataclass
    construction and on row parsing — measured while building this, where it
    wrongly selected `verify_2697_metric_axis_derivation.py`, a module whose own
    docstring says it "reads provenance only, never result performance".
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _callee_name(node) not in _BACKTEST_ENTRY_POINTS:
            continue
        for keyword in node.keywords:
            if keyword.arg == "holdout_requested" and _is_not_literal(keyword.value, False):
                return True
            if keyword.arg == "evidence_window_id" and _is_not_literal(keyword.value, None):
                return True
    return False


def _passes_holdout_audit_fields(tree: ast.Module) -> bool:
    """Both audit fields supplied to one backtest entry point — the engine gate."""
    return any(
        _AUDIT_FIELDS <= {keyword.arg for keyword in node.keywords if keyword.arg is not None}
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _callee_name(node) in _BACKTEST_ENTRY_POINTS
    )


def _sealed_outcome_scripts() -> list[Path]:
    """Provenance first, with the historical glob kept as a floor.

    ⚠ The glob stays in the UNION rather than being replaced. It costs nothing,
    and dropping it would silently narrow coverage if the provenance rule ever
    misses a shape — a narrowing this test could not report on itself.
    """
    selected = set(_SCRIPTS.glob("evaluate_*.py")) | set(_SCRIPTS.glob("*_outcomes.py"))
    for path in _SCRIPTS.glob("*.py"):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover - a script that cannot parse cannot open anything
            continue
        if _opens_sealed_outcome(tree):
            selected.add(path)
    return sorted(selected)


def _is_gated(source: str) -> bool:
    """Any of the three accepted forms. See the module docstring."""
    tree = ast.parse(source)
    called = _called_names(source)
    return bool(called & (_GATE_CALLS | _ACCESS_RECORD_CALLS)) or _passes_holdout_audit_fields(tree)


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
        "verify_2476_pead_outcomes.py",
        "verify_2480_insider_outcomes.py",
    } <= names


def test_every_sealed_outcome_script_calls_a_declaration_check_or_is_allowlisted() -> None:
    ungated = [
        path.name
        for path in _sealed_outcome_scripts()
        if path.name not in _PRE_CUTOFF_UNGATED and not _is_gated(path.read_text())
    ]
    assert ungated == [], (
        f"sealed outcome openers with no declaration check: {ungated}. A script that reads forward outcome "
        f"windows must call one of {sorted(_GATE_CALLS | _ACCESS_RECORD_CALLS)}, or pass "
        f"{sorted(_AUDIT_FIELDS)} together to a backtest entry point, or be added to _PRE_CUTOFF_UNGATED "
        f"with the reason."
    )


def test_selection_is_by_provenance_and_not_by_filename() -> None:
    """#2830's regression: the opener this guard could not see.

    `measure_2827_gross_vs_net.py` matches NEITHER glob — it is not `evaluate_*`
    and not `*_outcomes` — and opens the withheld side. Before #2830 it was
    invisible here purely because of its name.
    """
    selected = {path.name for path in _sealed_outcome_scripts()}
    assert "measure_2827_gross_vs_net.py" in selected, (
        "the provenance rule no longer selects the opener #2830 was filed for; membership has regressed to naming"
    )
    assert not any(
        name.startswith("evaluate_") or name.endswith("_outcomes.py") for name in {"measure_2827_gross_vs_net.py"}
    ), "this assertion is vacuous unless the witness really is outside both globs"


def test_the_provenance_rule_admits_more_than_the_glob() -> None:
    """A rule that selects exactly the glob would be the old bug wearing an AST.

    ⚠ Asserts a STRICT superset, not a count. Pinning "8 scripts" would fail on
    every unrelated script added later, which trains the next reader to edit the
    number rather than look at what changed.
    """
    glob_only = set(_SCRIPTS.glob("evaluate_*.py")) | set(_SCRIPTS.glob("*_outcomes.py"))
    selected = set(_sealed_outcome_scripts())

    assert glob_only < selected, "provenance selection must be a strict superset of the historical glob"


def test_the_engine_audit_pair_counts_as_a_gate() -> None:
    """The third accepted form, pinned because omitting it produces FALSE findings.

    `run_backtest` writes the access row itself when both audit fields are
    supplied, so these scripts are gated without any script-level gate call. A
    rule that recognised only `_GATE_CALLS` would report all three as ungated —
    which is what a first cut of #2830 did before the pairing was checked.
    """
    for name in (
        "run_2825_decisive_holdout_evidence.py",
        "verify_2429_total_return.py",
        "benchmark_2488_evidence_refresh.py",
    ):
        source = (_SCRIPTS / name).read_text()
        assert not (_called_names(source) & (_GATE_CALLS | _ACCESS_RECORD_CALLS)), (
            f"{name} now calls a script-level gate, so it no longer witnesses the audit-pair form"
        )
        assert _passes_holdout_audit_fields(ast.parse(source)), name
        assert _is_gated(source), name


def test_an_explicit_in_sample_call_is_not_selected() -> None:
    """`evidence_window_id=None` / `holdout_requested=False` are the in-sample spellings.

    Without this the rule would select every backtest caller and the gate would
    stop discriminating.
    """
    in_sample = ast.parse(
        "run_backtest(conn, evidence_window_id=None, holdout_requested=False)\n",
    )
    opening = ast.parse("run_backtest(conn, evidence_window_id='primary-2022-plus')\n")

    assert not _opens_sealed_outcome(in_sample)
    assert _opens_sealed_outcome(opening)


def test_the_keyword_alone_does_not_select_a_non_backtest_call() -> None:
    """The callee is load-bearing — a bare keyword match selects row parsing.

    Measured while building #2830: matching `evidence_window_id=` anywhere
    wrongly selected `verify_2697_metric_axis_derivation.py`, which constructs a
    dataclass from stored columns and reads provenance only.
    """
    dataclass_construction = ast.parse("StoredRow(result_id=1, evidence_window_id=str(row[12]))\n")

    assert not _opens_sealed_outcome(dataclass_construction)


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


def test_the_precutoff_openers_are_gated_rather_than_allowlisted() -> None:
    """#2616's acceptance: the allowlist is empty because both openers are gated.

    ⚠ BOTH names asserted, not either: `require_outcome_gate` writes the access
    row a re-run charges, and `verify_outcome_access_provenance` is what proves
    it COMMITTED with the declaration frozen strictly before it. A script
    calling only one has half a gate.
    """

    assert _PRE_CUTOFF_UNGATED == {}
    for name in ("verify_2476_pead_outcomes.py", "verify_2480_insider_outcomes.py"):
        called = _called_names((_SCRIPTS / name).read_text())
        assert "require_outcome_gate" in called, name
        assert "verify_outcome_access_provenance" in called, name
