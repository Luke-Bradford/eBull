"""#3592 slice 3b — ranking-pot-v2's policy hash scope (spec §8 v5) and isolation from v1's.

The module list is recomputed here by a static walk of ``app/services`` and pinned EQUAL to the policy module's
literal list; no executed-book module may sit in C, the pot code v2 calls through.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from app.services import ranking_pot_policy as v1_policy
from app.services import ranking_pot_v2_policy as policy
from app.services.ranking_pot_sim import K_CONTROLS

SERVICES = Path(policy.__file__).resolve().parent
PKG = "app.services"
#: §8 v5: a ``ranking_pot*`` module whose name marks it executed-book.
EXECUTED_BOOK = re.compile(r"^ranking_pot.*(_exec|_executor$|_intent$|_activation$|_loss$|_exits$|_held_levels$)")


def _module_file(name: str) -> Path:
    path = SERVICES / f"{name}.py"
    if not path.is_file():
        raise AssertionError(f"{PKG}.{name} is not a module file (packages and re-exports are outside the contract)")
    return path


def imports_of(name: str, *, strict: bool = True) -> frozenset[str]:
    """§8 v5's walk contract: every import statement in the module (any depth, conservative), in the three absolute
    forms. ``strict`` (C and its one hop): a relative import, ``importlib``, ``__import__`` or an ``app.services`` name
    that is not a module file fails. Lenient (the full closure, (v)): subpackages are leaves —
    ``test_no_subpackage_reaches_a_pot_module`` keeps that safe."""
    tree = ast.parse(_module_file(name).read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level and strict:
                raise AssertionError(f"{name}: relative import")
            mod = node.module or ""
            if strict and (mod == "importlib" or mod.startswith("importlib.")):
                raise AssertionError(f"{name}: importlib")
            if mod == PKG:
                out.update(a.name for a in node.names)
            elif mod.startswith(PKG + "."):
                out.add(mod.split(".")[2])
        elif isinstance(node, ast.Import):
            for a in node.names:
                if strict and (a.name == "importlib" or a.name.startswith("importlib.")):
                    raise AssertionError(f"{name}: importlib")
                if a.name.startswith(PKG + "."):
                    out.add(a.name.split(".")[2])
        elif strict and isinstance(node, ast.Name) and node.id == "__import__":
            raise AssertionError(f"{name}: __import__")
    if not strict:
        return frozenset(d for d in out if (SERVICES / f"{d}.py").is_file())
    for dep in out:
        _module_file(dep)
    return frozenset(out)


def _closure(roots: frozenset[str], *, pot_only: bool) -> frozenset[str]:
    seen: set[str] = set()
    todo = list(roots)
    while todo:
        m = todo.pop()
        if m in seen:
            continue
        seen.add(m)
        todo.extend(d for d in imports_of(m, strict=pot_only) if not pot_only or d.startswith("ranking_pot"))
    return frozenset(seen)


def roots() -> frozenset[str]:
    return frozenset(p.stem for p in SERVICES.glob("ranking_pot_v2*.py"))


def expected_modules() -> tuple[str, ...]:
    c = _closure(roots(), pot_only=True)
    one_hop = {d for m in c for d in imports_of(m)}
    full_pots = {m for m in _closure(roots(), pot_only=False) if m.startswith("ranking_pot")}
    scorer = {"scoring", *(f.removesuffix(".py") for f in policy.SCORER_IMPORTS)}
    return tuple(sorted(f"{m}.py" for m in scorer | c | one_hop | full_pots))


def test_policy_modules_are_exactly_the_v5_union() -> None:
    assert policy.POLICY_MODULES == expected_modules()


def test_no_executed_book_module_is_in_c() -> None:
    c = _closure(roots(), pot_only=True)
    executed = {m for m in c if EXECUTED_BOOK.match(m)}
    # A module whose own restricted closure reaches executed-book code is barred too (v1's job, step, readout).
    via = {m for m in c if any(EXECUTED_BOOK.match(d) for d in _closure(frozenset({m}), pot_only=True))}
    assert (executed, via) == (set(), set())
    for name in ("ranking_pot_job", "ranking_pot_step", "ranking_pot_readout"):
        assert any(EXECUTED_BOOK.match(d) for d in _closure(frozenset({name}), pot_only=True)), name


def test_the_freeze_stays_outside_the_hash() -> None:
    """§8 v6: the freeze imports the trial register (bumped often) and the result ledger; hashed, one register bump
    would refuse every later rebalance as ``ranking_drift``. No hashed module may import it, so neither may a root."""
    assert not (SERVICES / "ranking_pot_v2_freeze.py").exists()  # a v2-prefixed name would make it a root
    assert "ranking_pot_freeze_v2" not in _closure(roots(), pot_only=False)
    hashed = {name.removesuffix(".py") for name in policy.POLICY_MODULES}
    assert not hashed & {"ranking_pot_freeze_v2", "ranking_pot_freeze", "trial_register", "result_ledger"}


def test_scorer_imports_are_exactly_scoring_py_in_repo_imports() -> None:
    assert policy.SCORER_IMPORTS == tuple(sorted(f"{m}.py" for m in imports_of("scoring")))


def test_the_walk_contract_refuses_dynamic_and_relative_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tests.test_ranking_pot_v2_policy as me

    monkeypatch.setattr(me, "SERVICES", tmp_path)
    for i, body in enumerate(
        (
            "from . import x\n",
            "import importlib\n",
            "def f():\n    __import__('os')\n",
            "from app.services import nope\n",
        )
    ):
        (tmp_path / f"m{i}.py").write_text(body)
        with pytest.raises(AssertionError):
            imports_of(f"m{i}")
    (tmp_path / "ok.py").write_text("def f():\n    if False:\n        from app.services.dep import y\n")
    (tmp_path / "dep.py").write_text("")
    assert imports_of("ok") == {"dep"}  # function-level and dead-branch imports count


def test_hash_matches_disk_and_moves_with_a_hashed_module(tmp_path: Path) -> None:
    assert policy.policy_manifest_now().digest() == policy.RANKING_POT_V2_POLICY_HASH
    for name in policy.POLICY_MODULES:
        (tmp_path / name).write_bytes((SERVICES / name).read_bytes())
    assert policy.policy_manifest_now(tmp_path).digest() == policy.RANKING_POT_V2_POLICY_HASH
    (tmp_path / "ranking_pot_v2.py").write_bytes((SERVICES / "ranking_pot_v2.py").read_bytes() + b"\n")
    assert policy.policy_manifest_now(tmp_path).digest() != policy.RANKING_POT_V2_POLICY_HASH


def test_v1_constants_are_reused_unchanged_and_v2_terms_match_sql_463() -> None:
    assert (policy.FAMILY, policy.FAMILY_ALPHA, policy.LOOK_MONTHS, policy.HARM_ALPHA) == (
        v1_policy.FAMILY,
        v1_policy.FAMILY_ALPHA,
        v1_policy.LOOK_MONTHS,
        v1_policy.HARM_ALPHA,
    )
    assert [policy.per_look_alpha(m) for m in (1, 2, 3)] == [v1_policy.per_look_alpha(m) for m in (1, 2, 3)]
    assert (policy.EXECUTION, policy.BOOK_COUNT) == ("none", K_CONTROLS + 3)
    assert policy.BUILD_COMPLETE is False


def test_no_subpackage_reaches_a_pot_module() -> None:
    """The lenient walk stops at ``app.services`` subpackages; none of them names a pot module today."""
    offenders = [
        str(p.relative_to(SERVICES))
        for p in SERVICES.glob("*/**/*.py")
        if "ranking_pot" in p.read_text(encoding="utf-8")
    ]
    assert offenders == []


def test_v1_and_v2_hash_disjoint_pot_modules() -> None:
    """v1's hash never covers a v2 module, so building v2 cannot drift v1; v2 hashes no v1 executed-book module
    through C (``test_no_executed_book_module_is_in_c``)."""
    assert not any(m.startswith("ranking_pot_v2") for m in v1_policy.POLICY_MODULES)
    assert "ranking_pot_policy" not in roots()  # stems, as roots() returns
