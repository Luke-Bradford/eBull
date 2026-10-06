"""#2645 — the unattended loop must not be able to mutate broker state.

`.autonomy/loop_prompt.md` claimed the unattended loop "is run with NO broker
credentials configured, so the order client fails closed". It is not. `.env` is
absent from the loop worktree, but neither half of the credential path lives
there: the database-URL setting defaults to the shared dev Postgres, which holds
the `broker_credentials` rows, and the decryption root secret resolves to
`platformdirs.user_data_dir("eBull")` — a machine-wide OS directory. #2644 read
demo credentials and completed informational preflight calls from that worktree.

Two layers are tested here, and the order matters:

1. `refuse_broker_mutation_if_unattended` — an EXECUTION-TIME refusal raised
   before any network I/O. This is the real control. A push-time check could not
   have stopped #2644's probe, which ran long before anything was pushed.
2. The static scans — that every mutating `EtoroBrokerProvider` method calls the
   guard, that no informational one does, and that no script reaches a mutating
   method at all. These keep layer 1 wired as the code moves; they are not
   themselves the safety layer.

⚠ WHAT THIS DOES NOT DO. The guard lives in the repo the loop can edit, so it
constrains a confused run rather than a determined one, and the AST scans see
call sites only — not `getattr` dispatch, not a raw POST to an order endpoint,
not a shell script. The prohibition in `.autonomy/hard_rules.md` stays first.
"""

from __future__ import annotations

import ast
import inspect
import os
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Final

import pytest

from app.providers.broker import BrokerProvider
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security import unattended_guard
from app.security.unattended_guard import (
    UnattendedExecutionRefused,
    UnmergedCodeRefused,
    is_linked_worktree,
    refuse_broker_mutation_if_unattended,
    unmerged_code_reason,
)

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
_SCRIPTS: Final[Path] = _REPO_ROOT / "scripts"
_ETORO: Final[Path] = _REPO_ROOT / "app" / "providers" / "implementations" / "etoro_broker.py"

_GUARD_CALL: Final[str] = "refuse_broker_mutation_if_unattended"

#: Broker methods that change order or position state at the broker. Calling any
#: of these from a script is the thing `.autonomy/hard_rules.md` forbids
#: ("never execute/approve/simulate a trade... never close a position"), whether
#: the environment is demo or real — a demo fill is still a persisted write.
_MUTATING: Final[frozenset[str]] = frozenset(
    {
        "close_demo_strategy_position",
        "close_position",
        "edit_demo_strategy_position",
        "place_demo_core_order",
        "place_demo_strategy_order",
        "place_order",
    }
)

#: Read-only broker methods. Enumerated NOT because the scan needs them, but so
#: the two sets can be checked to partition the live `BrokerProvider` surface —
#: see `test_every_broker_method_is_classified`.
#:
#: ⚠ `check_instrument_eligibility` and `get_what_if_costs` are POSTs, and they
#: share the `_http_write` transport with the mutating calls. That is exactly why
#: the guard is per-method rather than wrapped around the transport: refusing at
#: `_http_write` would block the informational preflight decode #2644 completed
#: correctly, which is the work this ticket found had been wrongly ruled out.
_INFORMATIONAL: Final[frozenset[str]] = frozenset(
    {
        "check_instrument_eligibility",
        "get_account_risk_snapshot",
        "get_close_order",
        "get_order_status",
        "get_portfolio",
        "get_trade_history",
        "get_what_if_costs",
        "lookup_order",
    }
)

#: ⚠ EMPTY, AND EMPTY IS THE STATE WORTH KEEPING. Measured 2026-08-13 across all
#: 192 files under `scripts/`: zero call a mutating broker method. An entry here
#: would mean an unattended run reaches the order path, which is the condition
#: this file exists to prevent — it needs an operator decision, not a commit.
_EXEMPT_SCRIPTS: Final[dict[str, str]] = {}


def _called_names(source: str) -> set[str]:
    """Every name appearing in CALL position anywhere in the source.

    ⚠ AN AST WALK, NOT A SUBSTRING SEARCH. `"place_order" in source` also
    matches a comment, a docstring, an unused import or a dead branch, so it
    would fail on scripts that merely discuss the order path — and this file's
    own module docstring names several of these methods.
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


def _etoro_methods() -> dict[str, ast.FunctionDef]:
    tree = ast.parse(_ETORO.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "EtoroBrokerProvider":
            return {b.name: b for b in node.body if isinstance(b, ast.FunctionDef)}
    raise AssertionError("EtoroBrokerProvider not found — this scan is measuring nothing")


# --------------------------------------------------------------------------
# Layer 1 — the execution-time refusal
# --------------------------------------------------------------------------


def test_guard_refuses_from_a_linked_worktree(tmp_path: Path) -> None:
    """A linked worktree's `.git` is a FILE holding a `gitdir:` pointer."""
    (tmp_path / ".git").write_text("gitdir: /somewhere/.git/worktrees/x\n")
    assert is_linked_worktree(tmp_path) is True


def test_guard_allows_the_main_checkout(tmp_path: Path) -> None:
    """The main checkout's `.git` is a DIRECTORY.

    Verified against the operator's real trading checkout on 2026-08-13:
    the main checkout's `.git` is a directory, and `git worktree list`
    names it the main worktree — so this guard cannot brick the order path there.
    """
    (tmp_path / ".git").mkdir()
    assert is_linked_worktree(tmp_path) is False


def test_guard_allows_a_checkout_with_no_git_at_all(tmp_path: Path) -> None:
    """Only POSITIVE detection refuses.

    An installed package or container image has no `.git`. Refusing there would
    add a new way for the operator's own order path to break, in exactly the
    environments this can never be exercised — for no gain, because the loop is
    not one of them.
    """
    assert is_linked_worktree(tmp_path) is False


def test_refusal_names_the_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(unattended_guard, "is_linked_worktree", lambda *_a, **_k: True)
    with pytest.raises(UnattendedExecutionRefused, match="place_order"):
        refuse_broker_mutation_if_unattended("place_order")


def test_guard_is_a_no_op_in_the_main_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(unattended_guard, "is_linked_worktree", lambda *_a, **_k: False)
    refuse_broker_mutation_if_unattended("place_order")


def test_place_order_refuses_end_to_end_when_armed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drive the SHIPPED method, not the guard function, with the guard armed.

    ``tests/conftest.py`` pins the refusal off for the whole suite so the result
    does not depend on which checkout it runs in. That is safe only because this
    test re-arms it and proves the refusal still fires through
    ``EtoroBrokerProvider.place_order`` — otherwise the disarm would be a
    fail-open default that silently voids everything below it (#2647).

    It also proves the refusal lands BEFORE any network I/O: ``_http_write`` is
    replaced with an object that fails the test if it is touched at all.
    """
    monkeypatch.setattr(unattended_guard, "is_linked_worktree", lambda *_a, **_k: True)

    class _NeverCalled:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"refusal must precede all network I/O, but _http_write.{name} was reached")

    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
        broker._http_write = _NeverCalled()  # type: ignore[assignment]
        with pytest.raises(UnattendedExecutionRefused, match="place_order"):
            broker.place_order(
                instrument_id=1001,
                action="BUY",
                amount=Decimal("100"),
                units=None,
            )


# --------------------------------------------------------------------------
# Layer 2 — static scans keeping layer 1 wired
# --------------------------------------------------------------------------


def test_every_mutating_etoro_method_calls_the_guard() -> None:
    methods = _etoro_methods()
    missing = sorted(
        name for name in _MUTATING if name in methods and _GUARD_CALL not in _called_names(ast.unparse(methods[name]))
    )
    assert not missing, (
        f"mutating EtoroBrokerProvider method(s) do not call {_GUARD_CALL}(): {missing}. "
        "An unattended run would reach the broker through them (#2645)."
    )


def test_informational_etoro_methods_do_not_call_the_guard() -> None:
    """The guard must not creep onto the read path.

    #2645's other half is that `#2598` was wrongly classified loop-ineligible on
    the false premise that credentials were unreachable. Informational preflight
    from a worktree is legitimate and was how the truth surfaced; a guard that
    spread to `get_what_if_costs` would re-impose the error it corrects.
    """
    methods = _etoro_methods()
    over_guarded = sorted(
        name for name in _INFORMATIONAL if name in methods and _GUARD_CALL in _called_names(ast.unparse(methods[name]))
    )
    assert not over_guarded, (
        f"informational method(s) refuse from a worktree, blocking legitimate preflight: {over_guarded}"
    )


def test_no_script_calls_a_mutating_broker_method() -> None:
    offenders: dict[str, list[str]] = {}
    for path in sorted(_SCRIPTS.rglob("*.py")):
        rel = path.relative_to(_REPO_ROOT).as_posix()
        if rel in _EXEMPT_SCRIPTS:
            continue
        hits = sorted(_called_names(path.read_text()) & _MUTATING)
        if hits:
            offenders[rel] = hits
    assert not offenders, (
        "script(s) call a broker method that mutates order/position state, which an "
        f"unattended run must never reach (#2645): {offenders}"
    )


def test_every_broker_method_is_classified() -> None:
    """A guard over a hand-written list is only as wide as the list.

    Recorded as its own failure mode in `docs/review-prevention-log.md` — "a
    chokepoint closes the doors that EXIST" (#2614). `_MUTATING` is hand-written,
    so a broker method added later is silently uncovered by BOTH scans above.
    Asserting the two sets partition the live surface turns that silence into a
    failing test naming the unclassified method.
    """
    live = {
        name for name in vars(BrokerProvider) if not name.startswith("_") and callable(getattr(BrokerProvider, name))
    }
    classified = _MUTATING | _INFORMATIONAL
    assert live == classified, (
        "BrokerProvider's public surface no longer matches the mutating/informational "
        f"split. Unclassified: {sorted(live - classified)}. "
        f"Classified but gone: {sorted(classified - live)}. "
        "Add each new method to _MUTATING or _INFORMATIONAL in this file."
    )


def test_mutating_and_informational_are_disjoint() -> None:
    assert not (_MUTATING & _INFORMATIONAL)


# --------------------------------------------------------------------------
# #3614 — entries refuse unmerged or modified code; exits never do
# --------------------------------------------------------------------------

#: Methods that OPEN exposure. Every other `_MUTATING` method exits, closes or edits SL/TP, and
#: "Exits and protective actions are never blocked" (`.claude/CLAUDE.md`).
_ENTRIES: Final[frozenset[str]] = frozenset({"place_demo_core_order", "place_demo_strategy_order", "place_order"})
_ENTRY_GUARD_CALL: Final[str] = "refuse_entry_if_unmerged_code"


def _run_git(root: Path, *args: str) -> None:
    """Scrubbed of GIT_*: the pre-push hook exports GIT_DIR, and `git -C` does not override it, so an
    unscrubbed `init`/`config`/`commit` here rewrites the REAL repository (it did, once: core.bare,
    user.*, and refs/remotes/origin/main — #2658's trap, re-learned in #3614)."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, env=env)


def _repo(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    _run_git(root, "init", "-q", "-b", "main")
    _run_git(root, "config", "user.email", "t@example.com")
    _run_git(root, "config", "user.name", "t")
    (root / "a.py").write_text("x = 1\n")
    _run_git(root, "add", "a.py")
    _run_git(root, "commit", "-q", "-m", "one")
    _run_git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
    return root


def _commit(root: Path, text: str) -> None:
    (root / "a.py").write_text(text)
    _run_git(root, "commit", "-qam", "next")


def test_merged_clean_checkout_is_allowed(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    assert unmerged_code_reason(root) is None
    (root / "notes.md").write_text("untracked\n")
    assert unmerged_code_reason(root) is None


def test_a_checkout_behind_origin_main_is_allowed(tmp_path: Path) -> None:
    """A fetch advances origin/main before anyone re-detaches; older merged code is reviewed code."""
    root = _repo(tmp_path)
    _commit(root, "x = 2\n")
    _run_git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
    _run_git(root, "checkout", "-q", "--detach", "HEAD~1")
    assert unmerged_code_reason(root) is None


def test_an_unmerged_commit_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    _commit(root, "x = 2\n")
    assert (reason := unmerged_code_reason(root)) is not None and "is not on origin/main" in reason


def test_a_modified_tracked_file_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    (root / "a.py").write_text("x = 3\n")
    assert unmerged_code_reason(root) == "tracked files are modified: ['a.py']"


def test_unreadable_git_state_refuses(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    _run_git(root, "update-ref", "-d", "refs/remotes/origin/main")
    assert (reason := unmerged_code_reason(root)) is not None and reason.startswith("git merge-base failed")


def test_a_hook_exported_git_dir_does_not_redirect_the_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Under a git hook GIT_DIR names the hook's repository; the guard must still read `root`."""
    root = _repo(tmp_path / "checked")
    _commit(root, "x = 2\n")
    other = _repo(tmp_path / "hook")
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    assert (reason := unmerged_code_reason(root)) is not None and "is not on origin/main" in reason


@pytest.mark.parametrize("failing_call", [0, 1, 2])
def test_a_hung_or_missing_git_refuses_at_every_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failing_call: int
) -> None:
    """Each of the three git reads (status, merge-base, rev-parse) fails closed, not with a raw exception."""
    root = _repo(tmp_path)
    _commit(root, "x = 2\n")  # unmerged, so all three calls are reached
    real = unattended_guard._git
    calls = iter(range(3))

    def flaky(where: Path, *args: str) -> subprocess.CompletedProcess[str]:
        if next(calls) == failing_call:
            raise subprocess.TimeoutExpired(["git", *args], 5)
        return real(where, *args)

    monkeypatch.setattr(unattended_guard, "_git", flaky)
    assert (reason := unmerged_code_reason(root)) is not None and reason.startswith("git state unreadable")


def test_no_git_checkout_is_not_checked(tmp_path: Path) -> None:
    assert unmerged_code_reason(tmp_path) is None


@pytest.mark.parametrize("operation", sorted(_ENTRIES))
def test_entries_refuse_end_to_end_when_armed(monkeypatch: pytest.MonkeyPatch, operation: str) -> None:
    """Re-arm the conftest disarm and drive the shipped method; the refusal precedes all network I/O."""
    monkeypatch.setattr(unattended_guard, "unmerged_code_reason", lambda *_a, **_k: "HEAD abc is not on origin/main")

    class _NeverCalled:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"refusal must precede all network I/O, but _http_write.{name} was reached")

    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
        broker._http_write = _NeverCalled()  # type: ignore[assignment]
        method = getattr(broker, operation)
        with pytest.raises(UnmergedCodeRefused, match=operation):
            method(**dict.fromkeys(inspect.signature(method).parameters))


def test_only_entry_methods_call_the_entry_guard() -> None:
    methods = _etoro_methods()
    calling = {name for name, node in methods.items() if _ENTRY_GUARD_CALL in _called_names(ast.unparse(node))}
    assert calling == _ENTRIES
    assert _ENTRIES < _MUTATING


@pytest.mark.parametrize(
    ("path", "marker"),
    [
        ("app/services/strategy_core_executor.py", "mark_core_submission_entered"),
        ("app/services/strategy_paper_executor.py", "mark_entry_verb_entered"),
    ],
)
def test_executors_refuse_before_their_entered_marker(path: str, marker: str) -> None:
    """A refusal after the marker reads as "may have reached the broker"; before it, provably unsent."""
    tree = ast.parse((_REPO_ROOT / path).read_text())
    checked = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        calls = [
            (n.lineno, n.func.id if isinstance(n.func, ast.Name) else getattr(n.func, "attr", ""))
            for n in ast.walk(node)
            if isinstance(n, ast.Call)
        ]
        marks = [line for line, name in calls if name == marker]
        if not marks or not any(name in _ENTRIES for _, name in calls):
            continue
        guards = [line for line, name in calls if name == _ENTRY_GUARD_CALL]
        assert guards and min(guards) < min(marks), f"{path}:{node.name} must refuse before {marker}"
        checked += 1
    assert checked, f"{path}: no entry function found — this scan is measuring nothing"
