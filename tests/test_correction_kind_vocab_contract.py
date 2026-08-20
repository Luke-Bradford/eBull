"""The ``CorrectionApplied.kind`` vocabulary must agree across all three declarations.

Pure-logic (no DB, no app boot). #2229 shipped a new kind in the service layer and the
full service-level test suite stayed green while **every rollup that fired the new
correction 500'd**, because ``_CorrectionAppliedModel.kind`` is a closed Pydantic
``Literal`` that is only evaluated at serialization time. Nothing tied the two together.

This is the recurring class the prevention log calls "contract-field wired into one model
not its sibling": the fix is not another test for the specific kind, it is a test that
enumerates what the service actually emits and requires the consumers to keep up.

Three declarations, one vocabulary:
  * producer — ``app/services/ownership_rollup.py``, every ``CorrectionApplied(kind=...)``
  * API      — ``app/api/instruments.py``, ``_CorrectionAppliedModel.kind`` Literal
  * frontend — ``frontend/src/api/ownership.ts``, ``OwnershipCorrectionKind``

The producer side is read by AST rather than by importing and introspecting, so a kind
that is only reachable on a rare data path still counts: what matters is that the string
exists in the code, not that some fixture happened to trigger it.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import get_args

from app.api.instruments import _CorrectionAppliedModel

_REPO = Path(__file__).resolve().parents[1]
_SERVICE = _REPO / "app" / "services" / "ownership_rollup.py"
_FRONTEND = _REPO / "frontend" / "src" / "api" / "ownership.ts"


def _kind_expressions() -> list[ast.expr]:
    """The ``kind=`` argument expression of every ``CorrectionApplied(...)`` call."""
    tree = ast.parse(_SERVICE.read_text())
    out: list[ast.expr] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name != "CorrectionApplied":
            continue
        out.extend(kw.value for kw in node.keywords if kw.arg == "kind")
    return out


def _resolve(expr: ast.expr) -> set[str]:
    """String literals this expression can EVALUATE to.

    Deliberately not ``ast.walk`` — one call site is
    ``"def14a_restates_institution" if src == "def14a" else "institutional_family_collapse"``,
    and a blind walk also picks up ``"def14a"``, which is a comparison operand in the
    test, never a kind. Only value positions count: a constant, or both branches of a
    conditional. Anything else resolves empty and is caught by
    :func:`test_every_kind_argument_is_statically_readable`.
    """
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return {expr.value}
    if isinstance(expr, ast.IfExp):
        return _resolve(expr.body) | _resolve(expr.orelse)
    return set()


def _emitted_kinds() -> set[str]:
    """Every kind the service can construct a ``CorrectionApplied`` with."""
    return {kind for expr in _kind_expressions() for kind in _resolve(expr)}


def _api_kinds() -> set[str]:
    annotation = _CorrectionAppliedModel.model_fields["kind"].annotation
    return set(get_args(annotation))


def _frontend_kinds() -> set[str]:
    src = _FRONTEND.read_text()
    match = re.search(r"export type OwnershipCorrectionKind\s*=(.*?);", src, re.DOTALL)
    assert match is not None, "OwnershipCorrectionKind union not found in ownership.ts"
    return set(re.findall(r'"([a-z0-9_]+)"', match.group(1)))


def test_service_emits_at_least_one_kind() -> None:
    """Guard the guard: if the AST walk silently under-collected, every assertion below
    would pass vacuously — the ``x <> ALL('{}')`` failure mode in test form. This caught
    the first version of ``_emitted_kinds`` missing a conditional-expression call site."""
    assert len(_emitted_kinds()) >= 6


def test_every_kind_argument_is_statically_readable() -> None:
    """No ``CorrectionApplied(kind=...)`` may hide its value behind a name the AST walk
    cannot resolve — otherwise the coverage assertions below go quietly blind to it."""
    opaque = [ast.dump(expr) for expr in _kind_expressions() if not _resolve(expr)]
    assert not opaque, f"kind= argument(s) with no statically resolvable string: {opaque}"


def test_api_literal_covers_every_kind_the_service_emits() -> None:
    """A kind missing here 500s the ownership-rollup endpoint on exactly the
    instruments the correction fires for, while service-layer tests stay green."""
    missing = _emitted_kinds() - _api_kinds()
    assert not missing, (
        f"{sorted(missing)} emitted by ownership_rollup.CorrectionApplied but absent from "
        f"_CorrectionAppliedModel.kind in app/api/instruments.py — the endpoint will 500."
    )


def test_frontend_union_covers_every_kind_the_service_emits() -> None:
    missing = _emitted_kinds() - _frontend_kinds()
    assert not missing, (
        f"{sorted(missing)} emitted by the service but absent from OwnershipCorrectionKind "
        f"in frontend/src/api/ownership.ts."
    )


def test_no_declared_kind_is_unreachable() -> None:
    """The reverse direction: a kind declared in the API or the frontend that the service
    never emits is dead vocabulary — either a rename that left a stale entry behind, or a
    producer that was deleted without its contract."""
    emitted = _emitted_kinds()
    stale_api = _api_kinds() - emitted
    stale_fe = _frontend_kinds() - emitted
    assert not stale_api, f"declared in the API Literal but never emitted: {sorted(stale_api)}"
    assert not stale_fe, f"declared in the frontend union but never emitted: {sorted(stale_fe)}"
