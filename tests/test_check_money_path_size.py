"""Money-path size cap (#3613 item 5), against throwaway git repos."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from scripts import check_money_path_size as cap

_LISTED = "app/services/order_client.py"


def _run(repo: Path, *args: str) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env |= {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    return subprocess.run(["git", *args], cwd=repo, env=env, check=True, capture_output=True, text=True).stdout


def _write(repo: Path, rel: str, lines: int, prefix: str = "x") -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{prefix}{i}\n" for i in range(lines)))


def _commit(repo: Path) -> str:
    _run(repo, "add", "-A")
    _run(repo, "commit", "-q", "-m", "c")
    return _run(repo, "rev-parse", "HEAD").strip()


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A repo where every MONEY_PATH pattern matches one file, at one line each."""
    _run(tmp_path, "init", "-q")
    for pattern in cap.MONEY_PATH:
        rel = pattern.removeprefix(":(top,glob)").replace("*", "x")
        _write(tmp_path, rel, 1)
    _commit(tmp_path)
    # A pre-push hook's GIT_DIR must not redirect the check to the real repo.
    monkeypatch.setenv("GIT_DIR", "/nonexistent/hook.git")
    return tmp_path


def _churn(repo: Path, base: str, head: str) -> tuple[int, list[str]]:
    return cap.evaluate(cap.measure(base, head, repo))


def test_400_passes_and_401_fails(repo: Path) -> None:
    base = _run(repo, "rev-parse", "HEAD").strip()
    _write(repo, _LISTED, 1 + 200)  # +200 added, the original line kept
    _write(repo, "app/services/execution_guard.py", 1 + 200)
    at_cap = _commit(repo)
    assert _churn(repo, base, at_cap) == (400, [])

    _write(repo, "app/services/execution_guard.py", 1 + 201)
    over = _commit(repo)
    total, reasons = _churn(repo, base, over)
    assert total == 401
    assert reasons == [f"401 lines changed on the money path; the cap is {cap.MAX_CHURN}"]


def test_deletions_count(repo: Path) -> None:
    _write(repo, _LISTED, 300)
    base = _commit(repo)
    _write(repo, _LISTED, 300, prefix="y")  # replace every line
    head = _commit(repo)
    assert _churn(repo, base, head)[0] == 600


def test_unlisted_paths_are_not_counted(repo: Path) -> None:
    base = _run(repo, "rev-parse", "HEAD").strip()
    _write(repo, "tests/test_order_client.py", 5000)
    _write(repo, "app/services/strategy_result_regime_cohorts.py", 5000)
    # `*` must not cross `/`: a nested executor is outside the inventory.
    _write(repo, "app/services/sub/strategy_executor.py", 5000)
    head = _commit(repo)
    assert _churn(repo, base, head) == (0, [])


def test_glob_pattern_covers_a_new_executor(repo: Path) -> None:
    base = _run(repo, "rev-parse", "HEAD").strip()
    _write(repo, "app/services/brand_new_executor.py", 401)
    head = _commit(repo)
    assert _churn(repo, base, head)[0] == 401


def test_a_rename_is_charged_as_delete_plus_add(repo: Path) -> None:
    _write(repo, _LISTED, 250)
    base = _commit(repo)
    # A pure move, which git's default rename detection would report as 0/0.
    _run(repo, "mv", _LISTED, "app/services/order_client_x_executor.py")
    head = _commit(repo)
    assert _churn(repo, base, head)[0] == 500


def test_binary_change_fails_closed(repo: Path) -> None:
    base = _run(repo, "rev-parse", "HEAD").strip()
    (repo / _LISTED).write_bytes(b"\x00\x01\x02")
    head = _commit(repo)
    total, reasons = _churn(repo, base, head)
    assert total == 0
    assert reasons == [f"binary change has no line count: {_LISTED}"]


def test_unmatched_pattern_is_reported(repo: Path) -> None:
    _run(repo, "rm", "-q", "app/services/engine_book_risk.py")
    head = _commit(repo)
    assert cap.unmatched_patterns(head, repo) == [":(top,glob)app/services/engine_book_risk.py"]


def test_every_pattern_matches_in_the_fixture(repo: Path) -> None:
    assert cap.unmatched_patterns("HEAD", repo) == []


def test_unknown_base_is_an_error_not_zero(repo: Path) -> None:
    with pytest.raises(cap.CheckError):
        cap.resolve_commit("no-such-ref", repo)


@pytest.mark.parametrize("record", ["12\t3", "a\t3\tf.py", "-\t3\tf.py"])
def test_malformed_numstat_is_an_error(record: str) -> None:
    with pytest.raises(cap.CheckError):
        cap.parse_numstat_z(record + "\0")


def test_numstat_keeps_tabs_in_paths() -> None:
    assert cap.parse_numstat_z("1\t2\tweird\tname.py\0") == [cap.FileChurn("weird\tname.py", 1, 2)]
