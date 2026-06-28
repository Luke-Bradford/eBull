"""Unit test for scripts/check_shellcheck.sh (#1257).

The gate runs shellcheck (``-S warning``) recursively over
``scripts/**/*.sh`` (top level + subdirs like ``scripts/autonomy/`` and
``scripts/perf_bench/`` — the old top-level-only glob silently ungated
them, which let a shell bug ship in the autonomy supervisor, #1801) and
is wired into both ``.githooks/pre-push`` and ``.github/workflows/ci.yml``.
This test pins three contracts:

1. The current ``scripts/**/*.sh`` tree passes — a regression test, so any
   developer who introduces an SC2034 / SC2261 / SC2046-class bug in a
   shell script (including a subdir one) sees this fail locally before the
   push gate fires.
2. The gate returns non-zero when pointed at a synthetic script with a
   warning-level finding (acceptance: "a deliberately-broken check_*.sh
   fails CI").
3. The gate returns zero on a clean synthetic script.

The gate accepts optional positional args (a target file list); the
fixture tests pass a temp script so they never touch the real tree.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
GATE = REPO_ROOT / "scripts" / "check_shellcheck.sh"

# The gate self-resolves shellcheck (system binary or `uv run`); skip
# only if neither is reachable so the suite stays green on a bare box.
_HAVE_SHELLCHECK = shutil.which("shellcheck") is not None or shutil.which("uv") is not None


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(GATE), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.skipif(not _HAVE_SHELLCHECK, reason="shellcheck unavailable (no binary, no uv)")
def test_gate_passes_on_clean_tree() -> None:
    """scripts/**/*.sh (recursive — incl. subdirs) must be shellcheck-clean at -S warning."""
    result = _run()
    assert result.returncode == 0, (
        f"shellcheck gate failed on the real scripts/ tree:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )


@pytest.mark.skipif(not _HAVE_SHELLCHECK, reason="shellcheck unavailable (no binary, no uv)")
def test_gate_fails_on_warning_level_finding(tmp_path: Path) -> None:
    """A synthetic script with a warning-level finding must trip the gate."""
    bad = tmp_path / "check_synthetic_bad.sh"
    # SC2261: two competing stderr redirects on one `find` — the exact
    # bug class #1257 fixed in check_13f_hr_retention.sh / check_nport.
    bad.write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nfind app 2>/dev/null -name '*.py' -exec grep x {} \\; 2>/dev/null\n"
    )
    result = _run(str(bad))
    assert result.returncode != 0, (
        f"gate did not fail on a script with a warning-level finding:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )


@pytest.mark.skipif(not _HAVE_SHELLCHECK, reason="shellcheck unavailable (no binary, no uv)")
def test_gate_passes_on_clean_synthetic_script(tmp_path: Path) -> None:
    """A clean synthetic script must pass."""
    good = tmp_path / "check_synthetic_good.sh"
    good.write_text('#!/usr/bin/env bash\nset -euo pipefail\necho "hello"\n')
    result = _run(str(good))
    assert result.returncode == 0, (
        f"gate failed on a clean synthetic script:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
