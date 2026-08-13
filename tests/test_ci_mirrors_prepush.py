"""#1329 — pre-push hook <-> ci.yml chokepoint-lint parity guard.

`scripts/check_ci_mirrors_prepush.sh` fails if any `bash scripts/check_*.sh`
lint runs in only one of `.githooks/pre-push` / `.github/workflows/ci.yml`.
Without it, a `--no-verify` push of a regression guarded by a pre-push-only
lint lands green at CI (the #1382 / #1387 drift class).

Two tests:
  * live parity holds on the real repo files (exit 0);
  * the guard actually catches drift when fed a mismatched pair (exit 1) —
    this is the automated form of the #1329 acceptance criterion
    ("a deliberately-broken chokepoint fails CI").

Pure subprocess + tmp files — no DB, no network.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPARATOR = REPO_ROOT / "scripts" / "check_ci_mirrors_prepush.sh"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(COMPARATOR), *args],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def test_live_parity_holds() -> None:
    result = _run()
    assert result.returncode == 0, f"pre-push hook and ci.yml lint sets have drifted:\n{result.stdout}\n{result.stderr}"


def test_guard_detects_drift(tmp_path: Path) -> None:
    # Hook runs two lints; ci mirrors only one -> the second is
    # pre-push-only, the exact gap the guard exists to catch.
    hook = tmp_path / "pre-push"
    hook.write_text(
        "bash scripts/check_alpha.sh\nbash scripts/check_beta.sh\n",
        encoding="utf-8",
    )
    ci = tmp_path / "ci.yml"
    ci.write_text("run: bash scripts/check_alpha.sh\n", encoding="utf-8")

    result = _run(str(hook), str(ci))
    assert result.returncode == 1, "guard should fail on hook-only lint"
    assert "check_beta.sh" in result.stderr


def test_guard_detects_ci_only_drift(tmp_path: Path) -> None:
    # The reverse direction: a lint in ci.yml with no pre-push counterpart.
    hook = tmp_path / "pre-push"
    hook.write_text("bash scripts/check_alpha.sh\n", encoding="utf-8")
    ci = tmp_path / "ci.yml"
    ci.write_text(
        "run: bash scripts/check_alpha.sh\nrun: bash scripts/check_gamma.sh\n",
        encoding="utf-8",
    )

    result = _run(str(hook), str(ci))
    assert result.returncode == 1, "guard should fail on ci-only lint"
    assert "check_gamma.sh" in result.stderr
