"""Unit test for scripts/check_external_identifiers_inserts.sh (#1173).

The lint guard is wired into ``.githooks/pre-push`` + ``ci.yml`` so a
push that introduces an ``INSERT INTO external_identifiers`` site
without ``is_primary`` in the column list is rejected. Mirrors
tests/test_check_instruments_inserts_lint.py (the is_tradable guard).

Pins six contracts:

1. The current tree passes (regression test — every existing writer
   already sets is_primary; a developer who adds a violating INSERT
   sees this fail locally before the push gate fires).
2. A synthetic INSERT omitting is_primary trips the guard + names the
   offending file.
3. The same INSERT with is_primary in the column list passes.
4. A multi-line INSERT whose column list spans several lines still
   passes when is_primary lives inside the window.
5. A positional INSERT (no column list) is flagged (#1173 hardening).
6. is_primary appearing only in an inline comment does NOT satisfy the
   guard (#1173 hardening).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / "scripts" / "check_external_identifiers_inserts.sh"


def test_guard_passes_on_clean_tree() -> None:
    """The current source tree must pass — every external_identifiers
    writer already lists is_primary explicitly."""
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Lint guard failed on clean tree:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}\n"
        f"Every INSERT INTO external_identifiers must list is_primary explicitly."
    )


def test_guard_fails_on_synthetic_violation(tmp_path: Path) -> None:
    """A synthetic INSERT without is_primary must trip the guard."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    violation = tests_dir / "synthetic_bad.py"
    violation.write_text(
        '"""Intentional violation for lint guard test."""\n'
        'BAD_SQL = """\n'
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_value)\n"
        "VALUES (1, 'sec', '0000320193')\n"
        '"""\n'
    )
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1, (
        f"Expected guard to fail; got returncode={result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    assert "synthetic_bad.py" in result.stdout, f"Expected violation filename in output:\n{result.stdout}"
    assert "is_primary" in result.stdout, f"Expected guard to mention is_primary in failure message:\n{result.stdout}"


def test_guard_passes_when_is_primary_present(tmp_path: Path) -> None:
    """An INSERT that includes is_primary in the column list passes."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    clean = tests_dir / "synthetic_good.py"
    clean.write_text(
        '"""Compliant INSERT for lint guard test."""\n'
        'GOOD_SQL = """\n'
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_value, is_primary)\n"
        "VALUES (1, 'sec', '0000320193', TRUE)\n"
        '"""\n'
    )
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Expected guard to pass; got returncode={result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )


def test_guard_window_covers_multiline_inserts(tmp_path: Path) -> None:
    """A multi-line INSERT whose column list spans lines still passes
    when is_primary lives inside the window."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    multiline = tests_dir / "synthetic_multiline.py"
    multiline.write_text(
        '"""Multi-line INSERT — is_primary lives several lines down."""\n'
        'SQL = """\n'
        "INSERT INTO external_identifiers (\n"
        "    instrument_id,\n"
        "    provider,\n"
        "    identifier_value,\n"
        "    is_primary\n"
        ") VALUES (1, 'sec', '0000320193', TRUE)\n"
        '"""\n'
    )
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_guard_flags_positional_insert(tmp_path: Path) -> None:
    """A positional INSERT (no column list) can't set is_primary → fail.
    #1173 Codex hardening."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    positional = tests_dir / "synthetic_positional.py"
    positional.write_text(
        '"""Positional INSERT — no column list at all."""\n'
        'BAD = """\n'
        "INSERT INTO external_identifiers VALUES (1, 'sec', '0000320193', TRUE)\n"
        '"""\n'
    )
    result = subprocess.run(["bash", str(GUARD)], cwd=tmp_path, capture_output=True, text=True, check=False)
    assert result.returncode == 1, (
        f"Expected positional INSERT to fail; got {result.returncode}.\nSTDOUT: {result.stdout}"
    )
    assert "synthetic_positional.py" in result.stdout, result.stdout


def test_guard_rejects_is_primary_only_in_comment(tmp_path: Path) -> None:
    """is_primary appearing only in an inline ``--`` comment inside the
    column list must NOT satisfy the guard. #1173 Codex hardening."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    commented = tests_dir / "synthetic_commented.py"
    commented.write_text(
        '"""is_primary only in a comment — must still fail."""\n'
        'BAD = """\n'
        "INSERT INTO external_identifiers (\n"
        "    instrument_id,\n"
        "    provider,  -- is_primary intentionally defaulted\n"
        "    identifier_value\n"
        ") VALUES (1, 'sec', '0000320193')\n"
        '"""\n'
    )
    result = subprocess.run(["bash", str(GUARD)], cwd=tmp_path, capture_output=True, text=True, check=False)
    assert result.returncode == 1, (
        f"Expected comment-only is_primary to fail; got {result.returncode}.\nSTDOUT: {result.stdout}"
    )
    assert "synthetic_commented.py" in result.stdout, result.stdout
