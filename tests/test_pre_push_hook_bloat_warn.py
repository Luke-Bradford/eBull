"""Pre-push hook bloat-warn gates (#1208 Phase 4).

Two checks:

1. Drift gate — the hook's hardcoded `DB_SIZE_WARN_BYTES=10737418240`
   matches `app.services.postgres_health.DB_SIZE_WARN_BYTES`. Codex 1a
   WARNING #3 regression: the two MUST stay aligned because the
   operator-visible threshold (endpoint) and the push-time
   threshold (hook) are documented as the same signal.
2. Syntax check — `bash -n .githooks/pre-push` exits 0. Codex 1a
   WARNING #11 regression: catches double-quote/backtick injection
   bugs (the original v1 hook would have triggered command
   substitution on every push).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from app.services.postgres_health import DB_SIZE_WARN_BYTES

_HOOK_PATH = Path(__file__).resolve().parents[1] / ".githooks" / "pre-push"


def test_pre_push_hook_threshold_matches_db_size_warn() -> None:
    """The hook's literal threshold must equal `DB_SIZE_WARN_BYTES`."""
    hook_text = _HOOK_PATH.read_text(encoding="utf-8")
    match = re.search(r"^DB_SIZE_WARN_BYTES=(\d+)\b", hook_text, flags=re.MULTILINE)
    assert match is not None, (
        "expected `DB_SIZE_WARN_BYTES=<int>` in .githooks/pre-push; "
        "if the assignment moves, update this regex to match the new shape"
    )
    hook_value = int(match.group(1))
    assert hook_value == DB_SIZE_WARN_BYTES, (
        f"hook value {hook_value} != "
        f"app.services.postgres_health.DB_SIZE_WARN_BYTES {DB_SIZE_WARN_BYTES} — "
        "update one to match the other; the two are documented as a single source of truth"
    )


def test_pre_push_hook_syntax() -> None:
    """`bash -n` syntax-validates the hook without executing it."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not on PATH; cannot syntax-check the hook")
    result = subprocess.run(
        [bash, "-n", str(_HOOK_PATH)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"bash -n {_HOOK_PATH} failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"


def test_pre_push_hook_avoids_backtick_in_echo() -> None:
    """Codex 1a BLOCKING #2 regression: backticks inside a
    double-quoted echo trigger command substitution at hook-run time.
    Assert no backticks appear inside any double-quoted echo line in
    the warning block. Single-quoted echoes are fine.
    """
    hook_text = _HOOK_PATH.read_text(encoding="utf-8")
    # Capture any double-quoted echo line and reject if it contains a
    # backtick. Single-quoted echoes pass through.
    for lineno, line in enumerate(hook_text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped.startswith("echo "):
            continue
        # Find the first quote character to determine quote style.
        body = stripped[len("echo ") :].lstrip()
        if not body:
            continue
        if body[0] == "'":
            continue  # single-quoted — safe
        if body[0] == '"' and "`" in body:
            raise AssertionError(
                f"line {lineno}: double-quoted echo contains a backtick "
                "(would trigger command substitution at hook-run time):\n"
                f"  {line}"
            )
