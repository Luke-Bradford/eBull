"""Every paper-entry refusal the executor can emit has operator-facing copy (#2844).

#2844's acceptance clause 2 is *"Guard refuses over-bound entries; refusal visible on
/strategies"*. The refusal was reaching the screen and was not readable: `REFUSAL_LABELS`
described the *allocation-readiness* vocabulary and none of the *paper-entry-preflight*
one, so 73 of the executor's 76 codes -- `sandbox_exceeded`, the operator's own
assigned-capital boundary, among them -- fell through to a fallback that de-underscored
the identifier and presented it in the same slot as a written sentence.

WHY THIS GUARD IS KEYED ON CODES, WHEN THE PREVENTION LOG SAYS NOT TO
---------------------------------------------------------------------------
`docs/review-prevention-log.md` ("A guard keyed on a MANY-TO-MANY PROJECTION of what it
guards is evadable", #2625) killed a guard keyed on `check_promotable`'s refusal codes,
because the thing it needed to constrain was the gate's *inputs* and the codes were a
projection of them: a new input could reuse an existing code and stay green.

That reasoning does not transfer, and the log's own general test is why. It says: *ask
what the guard's assertion would look like if the defect were present.* Here the defect
is "a code reaches the operator with no copy", and the code string is not a projection of
that -- it IS the thing displayed. Add an unlabelled code and this assertion fails. So
the code is the index, not the observable.

WHY THE EXTRACTOR SELF-TESTS
---------------------------------------------------------------------------
`test_extractor_finds_every_emission_shape` exists because successive hand-written
extractions of this vocabulary undercounted it -- at 32, then 77, then 83 -- each time by
matching a *syntactic shape* and each time missing a different one. A guard that silently
under-reports is worse than none: it reports full coverage of the codes it happened to
see. So the shapes are pinned by anchor, and a new emission style has to either be found
by the extractor or fail that test.

⚠ It earned its place immediately: on first run it caught a FOURTH miss in the extractor
below -- a bare `return SANDBOX_EXCEEDED`, where the refusal is a Name rather than a
literal. Without the anchor, coverage would have reported green while the one code
#2844 exists for was still absent.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_EXECUTOR = _REPO / "app" / "services" / "strategy_paper_executor.py"
_PAGE = _REPO / "frontend" / "src" / "pages" / "StrategiesPage.tsx"

_CODE_SHAPED = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)+")

#: String constants that sit in a refusal position but are not refusal codes.
#: ⚠ Keep this SHORT and justified per entry. It is the extractor's only escape
#: hatch, and a code silenced here is a code the operator never gets copy for.
_NOT_REASON_CODES = frozenset(
    {
        "harness_validation",  # a deployment PURPOSE, compared against; the code is harness_validation_only
        "reason_code",  # the field name, passed to _require_text as its own label
        # `_component_amount` returns `(amount, basis)`, so the COST_BASIS_* constants
        # land in the same tuple-return position as a refusal. They name which broker
        # field the money came out of and are never written to `reason_code`.
        "broker_preflight_amount",
        "broker_preflight_value",
    }
)

#: One anchor per emission shape the executor actually uses. See the module docstring.
_SHAPE_ANCHORS = {
    "signal_not_fired_entry": 'tuple return -- `return None, "<code>", False`',
    "instrument_not_tradable": "the `checks` (predicate, code) table",
    "costs_missing": "bare return from a `-> str` helper",
    "preflight_unavailable": 'BoolOp under a keyword -- `reason_code=reason or "<code>"`',
    "strategy_not_capital_candidate": "IfExp assigned to `reason_code`",
    "sandbox_exceeded": "a Name referring to a constant in another module",
    "deployment_currency_unsupported": "a Name referring to a module-level constant",
    "all_paper_entry_gates_passed": "keyword on the ALLOCATED path, not a refusal",
}


def _resolved_names() -> dict[str, str]:
    """Module-level and imported string constants the executor emits by Name.

    Imported here rather than at module scope so this file stays free of anything
    that would attract the collection-time `db` marker (which is module-scoped:
    one DB-touching import evicts every test in the file from the push gate).
    """
    from app.services import strategy_paper_executor as executor

    return {
        name: value
        for name, value in vars(executor).items()
        if name.isupper() and isinstance(value, str) and _CODE_SHAPED.fullmatch(value)
    }


def _harvested_strings() -> set[str]:
    """Every code-shaped string sitting in a position that can reach `reason_code`.

    Deliberately over-inclusive: within each position that matters, the WHOLE
    sub-expression is walked rather than shape-matched. A false positive costs one
    label; a false negative ships a code the operator cannot read. `_NOT_REASON_CODES`
    is subtracted by `_executor_reason_codes`, and pinned by
    `test_the_non_code_escape_hatch_stays_minimal`.
    """
    tree = ast.parse(_EXECUTOR.read_text())
    names = _resolved_names()
    found: set[str] = set()

    def harvest(node: ast.AST) -> None:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and _CODE_SHAPED.fullmatch(sub.value):
                found.add(sub.value)
            elif isinstance(sub, ast.Name) and sub.id in names:
                found.add(names[sub.id])

    for node in ast.walk(tree):
        # (a) anything reaching a `reason_code=` keyword, however it is wrapped.
        if isinstance(node, ast.keyword) and node.arg == "reason_code" and node.value is not None:
            harvest(node.value)
        # (b) anything assigned to a local named `reason_code`.
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "reason_code" for target in node.targets
        ):
            harvest(node.value)
        # (c) refusals returned by the `-> str` preflight helpers, bare or in a tuple.
        if isinstance(node, ast.Return) and node.value is not None:
            if isinstance(node.value, (ast.Constant, ast.Name, ast.BoolOp, ast.IfExp)):
                harvest(node.value)
            elif isinstance(node.value, ast.Tuple):
                for element in node.value.elts:
                    if isinstance(element, (ast.Constant, ast.Name)):
                        harvest(element)
        # (d) the `checks` table of `(predicate, code)` pairs in `_load_intent`.
        if isinstance(node, ast.Tuple) and len(node.elts) == 2 and isinstance(node.elts[1], (ast.Constant, ast.Name)):
            harvest(node.elts[1])

    return found


def _executor_reason_codes() -> set[str]:
    return _harvested_strings() - _NOT_REASON_CODES


def _assert_parse_is_plausible(body: str, labels: dict[str, str]) -> None:
    """Every `key: "value"` line in the map was actually parsed.

    Counting the entries the naive way -- lines that look like a map entry at ANY indent --
    and comparing against what the strict regex captured turns a reformat from a quiet
    under-count into a named failure.
    """
    entry_lines = len(re.findall(r'^\s*[a-z0-9_]+: "', body, re.M))
    assert len(labels) == entry_lines, (
        f"REFUSAL_LABELS parse is under-counting: {entry_lines} entry line(s) present, "
        f"{len(labels)} captured. The map's formatting has moved away from what this "
        "guard parses -- fix the regex, do not lower the expectation."
    )


def _frontend_labels() -> dict[str, str]:
    """`REFUSAL_LABELS`, parsed out of the page.

    ⚠ The per-entry regex assumes a 2-space indent, so a reformat of the map could make
    it parse FEWER entries than exist. In the two coverage tests that direction is loud
    -- fewer labels means a bigger `codes - labels` difference and a failure -- but it is
    silent in `test_labels_are_sentences_not_restated_identifiers`, which only inspects
    what it parsed. `_assert_parse_is_plausible` closes that gap for every caller at once
    rather than each test re-checking.
    """
    source = _PAGE.read_text()
    match = re.search(r"const REFUSAL_LABELS: Record<string, string> = \{(.*?)\n\};", source, re.S)
    assert match is not None, "REFUSAL_LABELS is no longer declared in the shape this guard parses"
    body = match.group(1)
    labels = dict(re.findall(r'^\s{2}([a-z0-9_]+): "((?:[^"\\]|\\.)*)"', body, re.M))
    _assert_parse_is_plausible(body, labels)
    return labels


def test_extractor_finds_every_emission_shape() -> None:
    """The extractor is not silently under-reporting.

    Without this, `test_every_executor_refusal_has_operator_copy` passes vacuously the
    moment a shape stops being recognised -- which is how the gap it guards survived.
    """
    codes = _executor_reason_codes()
    unfound = {code: shape for code, shape in _SHAPE_ANCHORS.items() if code not in codes}
    assert not unfound, f"the extractor stopped recognising an emission shape: {unfound}"


def test_the_non_code_escape_hatch_stays_minimal() -> None:
    """Every `_NOT_REASON_CODES` entry still suppresses something the walk finds.

    The allow-list is the one way to silence a string the extractor picks up, so it is
    also the one way to silence a real refusal code. Nothing keeps it honest by itself.
    This asserts each entry is still EARNING its place: delete the executor string an
    entry was written for and the entry fails here rather than lingering, ready to
    swallow a future code that happens to reuse the name.

    ⚠ It found one on its first run -- `broker_rejected`, which appears only in a
    `Literal[...]` annotation and so was never harvested. It was suppressing nothing and
    is removed.
    """
    stale = sorted(_NOT_REASON_CODES - _harvested_strings())
    assert not stale, (
        f"{len(stale)} entr(y/ies) in _NOT_REASON_CODES no longer match anything the extractor "
        f"finds, so they only suppress future codes: {stale}. Remove them."
    )


def test_every_executor_refusal_has_operator_copy() -> None:
    labels = _frontend_labels()
    missing = sorted(_executor_reason_codes() - labels.keys())
    assert not missing, (
        f"{len(missing)} paper-entry reason code(s) reach StrategiesPage with no operator copy "
        f"and would render as a bare identifier: {missing}. Add an entry to REFUSAL_LABELS in "
        "frontend/src/pages/StrategiesPage.tsx, under the PAPER ENTRY PREFLIGHT section."
    )


def test_every_position_operation_trigger_has_operator_copy() -> None:
    """Triggers are keyed on the SCHEMA's own list, not on a scrape.

    Unlike the executor's refusal vocabulary -- which is declared nowhere, which is why
    the extractor above has to exist -- the trigger vocabulary IS declared, by
    `strategy_position_operations_trigger_code_check`. So this guard reads the
    constraint instead of restating it, and a migration adding a seventh trigger
    without copy fails here.
    """
    constraint = (_REPO / "sql" / "292_strategy_operator_position_close.sql").read_text()
    match = re.search(r"trigger_code IN \((.*?)\)", constraint, re.S)
    assert match is not None, "the trigger_code CHECK is no longer in the shape this guard parses"
    declared = set(re.findall(r"'([a-z0-9_]+)'", match.group(1)))
    assert declared, "parsed no trigger codes out of the CHECK constraint"

    source = _PAGE.read_text()
    block = re.search(r"const TRIGGER_LABELS: Record<string, string> = \{(.*?)\n\};", source, re.S)
    assert block is not None, "TRIGGER_LABELS is no longer declared in the shape this guard parses"
    labelled = set(re.findall(r"^\s{2}([a-z0-9_]+):", block.group(1), re.M))

    undescribed = sorted(declared - labelled)
    assert not undescribed, f"position-operation trigger(s) with no operator copy: {undescribed}"
    invented = sorted(labelled - declared)
    assert not invented, f"TRIGGER_LABELS describes trigger(s) the schema forbids: {invented}"


def test_labels_are_sentences_not_restated_identifiers() -> None:
    """A label that is just the de-underscored code is the defect wearing a map entry.

    The fallback was removed precisely so an undescribed code stays visible as an
    identifier. Re-adding the disguise as a hand-written entry would restore it.
    """
    disguised = sorted(
        code for code, label in _frontend_labels().items() if label.strip().lower() == code.replace("_", " ")
    )
    assert not disguised, f"these labels only restate their code: {disguised}"
