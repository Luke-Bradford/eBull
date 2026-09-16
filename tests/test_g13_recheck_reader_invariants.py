"""Static AST invariants for #1155 G13 — Layer 3 recheck-reader wiring.

Integration coverage of both reader paths already lives in
``tests/test_sec_per_cik_poll.py`` (``TestG13RecheckPath``) and the
hourly cadence is asserted in ``tests/test_layer_123_wiring.py``
(``test_layer3_per_cik_poll_registered``).

These tests are the *static* guarantee that a future refactor of
``run_per_cik_poll`` cannot silently drop one of the two reader
paths. Without them, removing the ``subjects_due_for_recheck`` call
or import would leave only behavioural tests as the safety net —
which would still pass if the dead import lingered or the call site
was swapped for an equivalent-shape stub.

Closes G13 row in
``.claude/skills/data-engineer/etl-endpoint-coverage.md`` §7.

**Scope-walk discipline (from #1193 review feedback):** any
intra-function AST check in this file MUST use the
``_RunPerCikPollVisitor`` (or a visitor that mirrors its nested-
scope skips). Naive ``ast.walk(stmt)`` recurses into nested
``FunctionDef`` / ``AsyncFunctionDef`` / ``Lambda`` / ``ClassDef``
bodies, producing an *asymmetric* invariant — a legitimately-added
nested helper would false-trigger one check while another remained
silent. Symmetric scope exclusion across all visitors is non-
negotiable.
"""

from __future__ import annotations

import ast
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parent.parent / "app" / "jobs" / "sec_per_cik_poll.py"

# ⚠ Renamed by #3109 from ``subjects_due_for_*``. The budget unit moved from
# rows to CIKs, and the readers now return an eager ``list`` of per-CIK batches
# rather than a lazy iterator of rows. The G13 invariant they exist to protect
# is unchanged: both lanes must still be read and drained inside
# ``run_per_cik_poll``'s own body.
_REQUIRED_READERS = ("ciks_due_for_poll", "ciks_due_for_recheck")


def _module_tree() -> ast.Module:
    return ast.parse(_MODULE_PATH.read_text())


def _run_per_cik_poll_node(tree: ast.Module) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "run_per_cik_poll":
            return node
    raise AssertionError("app/jobs/sec_per_cik_poll.py::run_per_cik_poll not found — refactored?")


class TestG13ReaderImports:
    """Both readers must be imported at module level from
    ``app.services.data_freshness``. A drift here means a refactor
    has split or rerouted the freshness-index access surface — the
    matrix entry needs revisiting."""

    def test_both_readers_imported_from_data_freshness(self) -> None:
        tree = _module_tree()
        imported_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "app.services.data_freshness":
                for alias in node.names:
                    imported_names.add(alias.name)
        missing = [name for name in _REQUIRED_READERS if name not in imported_names]
        assert not missing, (
            f"run_per_cik_poll module missing data_freshness imports: {missing}. "
            "Both reader paths are required by #1155 G13."
        )


class _RunPerCikPollVisitor(ast.NodeVisitor):
    """Single visitor that collects *both* intra-function invariants
    under one nested-scope-skipping traversal:

    1. ``consumed`` — reader-call names whose return value is actually
       used: wrapped in an eager materialiser (``list`` / ``tuple`` /
       ``set``), directly iterated (``for``, ``async for``,
       ``yield from``), or **bound to a name that a later for-loop in
       the same body iterates**. A bare ``ciks_due_for_recheck(...)``
       whose result is dropped does NOT satisfy this.

       ⚠ The bound-then-iterated case was added by #3109. The readers
       now return an eager ``list`` of per-CIK batches, so wrapping the
       call in ``list(...)`` would be redundant noise — but the thing
       this invariant actually protects is "the lane is drained", not
       "an iterator is materialised". Accepting only the materialiser
       form would have forced dead code to satisfy a test. The binding
       is tracked through ``ast.IfExp`` too, because the budget guard
       is written ``x = reader(...) if budget > 0 else []``.
    2. ``rebinds`` — reader names that appear as the target of an
       ``ast.Assign`` / ``ast.AnnAssign`` inside the function. A local
       stub (``subjects_due_for_recheck = lambda: iter([])``) would
       otherwise leave the dead module import in place while defeating
       the consumed-call invariant.

    Both invariants share the same scope-skipping logic. Splitting
    them across two visitors would have introduced an asymmetric
    invariant (#1193 review WARNING): a legitimately-added nested
    helper that locally reuses a name would false-trigger one check
    but not the other.

    Nested ``FunctionDef`` / ``AsyncFunctionDef`` / ``Lambda`` /
    ``ClassDef`` scopes are NOT recursed into — a dead nested helper
    must not be able to satisfy *or violate* the invariant for the
    outer function.
    """

    _MATERIALISERS = frozenset({"list", "tuple", "set"})

    def __init__(self, watch_names: tuple[str, ...]) -> None:
        self._watch: frozenset[str] = frozenset(watch_names)
        self.consumed: set[str] = set()
        self.rebinds: list[str] = []
        # local variable name -> reader it was assigned from
        self._bound_from: dict[str, str] = {}
        # local variable names a for-loop iterates
        self._iterated: set[str] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        return  # skip nested function bodies

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        return

    @staticmethod
    def _inner_call_name(node: ast.AST) -> str | None:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            return node.func.id
        return None

    def _record_consumed(self, node: ast.AST) -> None:
        inner = self._inner_call_name(node)
        if inner is not None and inner in self._watch:
            self.consumed.add(inner)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        # Pattern: list(<reader>(...)) / tuple(...) / set(...)
        if isinstance(node.func, ast.Name) and node.func.id in self._MATERIALISERS and node.args:
            self._record_consumed(node.args[0])
        self.generic_visit(node)

    def _reader_in(self, node: ast.AST) -> str | None:
        """Reader name behind an expression, seeing through ``IfExp``.

        The budget guard is ``reader(...) if budget > 0 else []``, so the
        call sits in one branch of a conditional rather than at the top.
        """
        inner = self._inner_call_name(node)
        if inner is not None:
            return inner if inner in self._watch else None
        if isinstance(node, ast.IfExp):
            return self._reader_in(node.body) or self._reader_in(node.orelse)
        return None

    def visit_For(self, node: ast.For) -> None:  # noqa: N802
        self._record_consumed(node.iter)
        if isinstance(node.iter, ast.Name):
            self._iterated.add(node.iter.id)
        self.generic_visit(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802
        self._record_consumed(node.iter)
        self.generic_visit(node)

    def visit_YieldFrom(self, node: ast.YieldFrom) -> None:  # noqa: N802
        self._record_consumed(node.value)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in self._watch:
                self.rebinds.append(target.id)
            elif isinstance(target, ast.Name):
                reader = self._reader_in(node.value)
                if reader is not None:
                    self._bound_from[target.id] = reader
        self.generic_visit(node)

    def resolve_bound_then_iterated(self) -> None:
        """Fold ``x = reader(...)`` + ``for _ in x:`` into ``consumed``.

        Called once after the walk, because the binding and the loop are
        separate statements and either may be visited first.
        """
        for name, reader in self._bound_from.items():
            if name in self._iterated:
                self.consumed.add(reader)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802
        if isinstance(node.target, ast.Name) and node.target.id in self._watch:
            self.rebinds.append(node.target.id)
        self.generic_visit(node)


def _walk_run_per_cik_poll() -> _RunPerCikPollVisitor:
    """Run the shared visitor against ``run_per_cik_poll``'s body."""
    tree = _module_tree()
    fn = _run_per_cik_poll_node(tree)
    visitor = _RunPerCikPollVisitor(_REQUIRED_READERS)
    for stmt in fn.body:
        visitor.visit(stmt)
    visitor.resolve_bound_then_iterated()
    return visitor


class TestG13ReaderCalls:
    """Both readers must be *called and consumed* inside
    ``run_per_cik_poll``'s own body (not in a nested helper) and must
    NOT be locally rebound to a stub. A stale import, discarded-result
    call, or same-name stub would let the recheck path silently die.

    Both assertions share one ``_RunPerCikPollVisitor`` traversal so
    the nested-scope skip is applied symmetrically (see #1193 review
    feedback)."""

    def test_both_readers_called_and_consumed_in_run_per_cik_poll(self) -> None:
        visitor = _walk_run_per_cik_poll()
        missing = [name for name in _REQUIRED_READERS if name not in visitor.consumed]
        assert not missing, (
            f"run_per_cik_poll body missing CONSUMED reader calls: {missing}. "
            "Both reader paths must (a) be called directly inside the function "
            "(not in a nested helper) and (b) have their iterator drained via "
            "list() / tuple() / set() / for-loop / yield from. Per #1155 G13; "
            "see app/jobs/sec_per_cik_poll.py:195-198 for the wired pattern."
        )

    def test_reader_names_not_locally_rebound(self) -> None:
        """Reject any ``ast.Assign`` / ``ast.AnnAssign`` inside
        ``run_per_cik_poll`` that rebinds one of the reader names.

        Without this, a future refactor could leave the import in
        place and stub the reader locally (``subjects_due_for_recheck
        = lambda *a, **k: iter([])``) and the consumed-call invariant
        would still pass while production silently dropped the path.
        """
        visitor = _walk_run_per_cik_poll()
        assert not visitor.rebinds, (
            f"run_per_cik_poll locally rebinds reader name(s): {visitor.rebinds}. "
            "A local stub would defeat the consumed-call invariant while "
            "leaving the dead module import in place. Per #1155 G13."
        )

    def test_run_per_cik_poll_returns_PerCikPollStats(self) -> None:
        """Stats dataclass carries the ``recheck_*`` counters — proves
        the caller-visible contract surfaces both reader outcomes.
        A future change that flattens the dataclass would defeat the
        operator's ability to see recheck-lane drain rates."""
        tree = _module_tree()
        fn = _run_per_cik_poll_node(tree)
        assert isinstance(fn.returns, ast.Name) and fn.returns.id == "PerCikPollStats", (
            "run_per_cik_poll return annotation drift — PerCikPollStats must remain "
            "the caller-visible surface so recheck_* counters are observable."
        )

        stats_cls: ast.ClassDef | None = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "PerCikPollStats":
                stats_cls = node
                break
        assert stats_cls is not None, "PerCikPollStats dataclass not found in module"

        field_names: set[str] = set()
        for stmt in stats_cls.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                field_names.add(stmt.target.id)
        for required in ("recheck_subjects_polled", "recheck_new_filings_recorded"):
            assert required in field_names, (
                f"PerCikPollStats missing field '{required}' — recheck-lane drain "
                "rate would no longer be operator-visible. See #1155 G13."
            )
