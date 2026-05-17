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
"""

from __future__ import annotations

import ast
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parent.parent / "app" / "jobs" / "sec_per_cik_poll.py"
_REQUIRED_READERS = ("subjects_due_for_poll", "subjects_due_for_recheck")


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


class _ConsumedReaderVisitor(ast.NodeVisitor):
    """Collects reader-call names whose return value is *consumed*.

    A bare ``subjects_due_for_recheck(...)`` whose result is discarded
    would still match a naive ``ast.walk`` scan; this visitor only
    records the call when its return value is wrapped in an eager
    materialiser (``list`` / ``tuple`` / ``set``) or directly iterated
    (``for x in reader(...)``, ``yield from reader(...)``). That
    matches the bounded-budget drain pattern the production code uses.

    Nested ``FunctionDef`` / ``AsyncFunctionDef`` / ``Lambda`` /
    ``ClassDef`` scopes are NOT recursed into — a dead nested helper
    must not be able to satisfy the invariant for the outer function.
    """

    _MATERIALISERS = frozenset({"list", "tuple", "set"})

    def __init__(self) -> None:
        self.consumed: set[str] = set()

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

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        # Pattern: list(<reader>(...)) / tuple(...) / set(...)
        if isinstance(node.func, ast.Name) and node.func.id in self._MATERIALISERS and node.args:
            inner = self._inner_call_name(node.args[0])
            if inner is not None:
                self.consumed.add(inner)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:  # noqa: N802
        inner = self._inner_call_name(node.iter)
        if inner is not None:
            self.consumed.add(inner)
        self.generic_visit(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802
        inner = self._inner_call_name(node.iter)
        if inner is not None:
            self.consumed.add(inner)
        self.generic_visit(node)

    def visit_YieldFrom(self, node: ast.YieldFrom) -> None:  # noqa: N802
        inner = self._inner_call_name(node.value)
        if inner is not None:
            self.consumed.add(inner)
        self.generic_visit(node)


class TestG13ReaderCalls:
    """Both readers must be *called and consumed* inside
    ``run_per_cik_poll``'s own body (not in a nested helper). A
    stale import or a discarded-result call would let the recheck
    path silently die."""

    def test_both_readers_called_and_consumed_in_run_per_cik_poll(self) -> None:
        tree = _module_tree()
        fn = _run_per_cik_poll_node(tree)
        visitor = _ConsumedReaderVisitor()
        for stmt in fn.body:
            visitor.visit(stmt)
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
        tree = _module_tree()
        fn = _run_per_cik_poll_node(tree)
        rebinds: list[str] = []
        for stmt in fn.body:
            for node in ast.walk(stmt):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id in _REQUIRED_READERS:
                            rebinds.append(target.id)
                elif isinstance(node, ast.AnnAssign):
                    target = node.target
                    if isinstance(target, ast.Name) and target.id in _REQUIRED_READERS:
                        rebinds.append(target.id)
        assert not rebinds, (
            f"run_per_cik_poll locally rebinds reader name(s): {rebinds}. "
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
