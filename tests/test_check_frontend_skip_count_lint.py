"""Unit test for scripts/check_frontend_skip_count.sh (#990).

The gate is wired into ``.githooks/pre-push`` and ``.github/workflows/
ci.yml`` so a push that adds a ``describe.skip`` / ``it.skip`` block
beyond the stored baseline is rejected. Contracts pinned here:

1. The current tree passes (regression: a developer who adds a skip
   without bumping the baseline sees this fail locally).
2. The gate returns non-zero + names the offending file when a
   synthetic skip pushes the count over the baseline.
3. The gate returns zero when the count is at or below the baseline
   (an explicit bump legitimises the skip).
4. A missing / non-integer baseline file is a hard error (fail-closed).

Cases 2-4 use the gate's optional positional args (src dir, baseline
file) to point it at a synthetic fixture rather than the real tree.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / "scripts" / "check_frontend_skip_count.sh"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(GUARD), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_gate_passes_on_clean_tree() -> None:
    """The real frontend/src tree must pass against its committed
    baseline. A new skip without a baseline bump fails this."""
    result = _run()
    assert result.returncode == 0, (
        f"skip-count gate failed on clean tree:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )


def test_gate_fails_when_count_exceeds_baseline(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.test.ts").write_text(
        "describe.skip('x', () => { it.skip('y', () => {}) })\n",
        encoding="utf-8",
    )
    baseline = tmp_path / "baseline.txt"
    baseline.write_text("0\n", encoding="utf-8")

    result = _run(str(src), str(baseline))
    assert result.returncode == 1
    assert "exceeds baseline" in result.stderr
    assert "a.test.ts" in result.stderr


def test_gate_passes_when_count_within_baseline(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.test.ts").write_text(
        "it.skip('justified', () => {})\n",
        encoding="utf-8",
    )
    baseline = tmp_path / "baseline.txt"
    baseline.write_text("1\n", encoding="utf-8")

    result = _run(str(src), str(baseline))
    assert result.returncode == 0, result.stderr


def test_gate_fails_on_missing_baseline(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    result = _run(str(src), str(tmp_path / "does_not_exist.txt"))
    assert result.returncode == 1
    assert "missing baseline" in result.stderr


def test_gate_fails_on_non_integer_baseline(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    baseline = tmp_path / "baseline.txt"
    baseline.write_text("not-a-number\n", encoding="utf-8")
    result = _run(str(src), str(baseline))
    assert result.returncode == 1
    assert "non-negative integer" in result.stderr
