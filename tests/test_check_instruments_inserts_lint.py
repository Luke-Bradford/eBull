"""Unit test for scripts/check_instruments_inserts.sh (#1233 §6.2).

The lint guard is wired into ``.githooks/pre-push`` so a push that
introduces an ``INSERT INTO instruments`` site without ``is_tradable``
in the column list is rejected. This test pins three contracts:

1. The current tree passes (a regression test — any developer who
   adds a violating INSERT will see this test fail locally before
   the push gate fires).
2. The guard returns non-zero + names the offending file when a
   synthetic violation is introduced in a temp directory.
3. The guard returns zero when the same synthetic INSERT includes
   ``is_tradable`` in the column list.

The shell script lives at the project root; the test invokes it via
``bash`` with the synthetic dir as the only ``SEARCH_DIRS`` candidate
(by writing it under a ``tests`` subdir that's part of the guard's
default search list).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / "scripts" / "check_instruments_inserts.sh"


def test_guard_passes_on_clean_tree() -> None:
    """The current source tree must pass the guard. Any developer who
    adds a violating INSERT (forgotten is_tradable) sees this fail."""
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Lint guard failed on clean tree:\n"
        f"STDOUT: {result.stdout}\n"
        f"STDERR: {result.stderr}\n"
        f"Every INSERT INTO instruments must list is_tradable explicitly."
    )


def test_guard_fails_on_synthetic_violation(tmp_path: Path) -> None:
    """A synthetic INSERT without is_tradable must trip the guard."""
    # The guard walks SEARCH_DIRS=(app tests sql scripts) relative to
    # CWD. We point CWD at tmp_path with a synthetic ``tests`` subdir
    # so the guard finds the violation but doesn't see the real repo.
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    violation = tests_dir / "synthetic_bad.py"
    violation.write_text(
        '"""Intentional violation for lint guard test."""\n'
        'BAD_SQL = """\n'
        "INSERT INTO instruments (instrument_id, symbol, company_name)\n"
        "VALUES (1, 'TEST', 'Test')\n"
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
    assert "is_tradable" in result.stdout, f"Expected guard to mention is_tradable in failure message:\n{result.stdout}"


def test_guard_passes_when_is_tradable_present(tmp_path: Path) -> None:
    """An INSERT that includes is_tradable in the column list passes."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    clean = tests_dir / "synthetic_good.py"
    clean.write_text(
        '"""Compliant INSERT for lint guard test."""\n'
        'GOOD_SQL = """\n'
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)\n"
        "VALUES (1, 'TEST', 'Test', TRUE)\n"
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
    """An INSERT statement whose column list is split across the
    opener line and a later line still passes when is_tradable lives
    inside the 30-line window. Pins the WINDOW_LINES contract."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    multiline = tests_dir / "synthetic_multiline.py"
    multiline.write_text(
        '"""Multi-line INSERT — is_tradable lives 5 lines down."""\n'
        'SQL = """\n'
        "INSERT INTO instruments (\n"
        "    instrument_id,\n"
        "    symbol,\n"
        "    company_name,\n"
        "    is_tradable\n"
        ") VALUES (1, 'TEST', 'Test', TRUE)\n"
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


def test_guard_is_executable() -> None:
    """The script must have +x set so ``.githooks/pre-push`` can invoke
    it via ``bash scripts/...`` without permission errors."""
    assert GUARD.exists(), f"Guard script not found at {GUARD}"
    # bash invocation works regardless of +x, but +x signals operator
    # intent. Don't enforce mode bits on Windows checkouts where git
    # may not preserve them.
    import os
    import sys

    if sys.platform != "win32":
        mode = os.stat(GUARD).st_mode & 0o777
        assert mode & 0o100, f"Guard script not executable: mode={oct(mode)}"


def test_guard_case_insensitive_catches_lowercase(tmp_path: Path) -> None:
    """Codex 1a hardening: lowercase ``insert into instruments`` must
    also trip the guard. Catches the false-negative where a developer
    writes SQL in lowercase to dodge the literal-case match."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    violation = tests_dir / "synthetic_lowercase.py"
    violation.write_text(
        '"""Lowercase SQL bypass attempt."""\n'
        'BAD_SQL = """\n'
        "insert into instruments (instrument_id, symbol, company_name)\n"
        "values (1, 'TEST', 'Test')\n"
        '"""\n'
    )
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "synthetic_lowercase.py" in result.stdout


def test_guard_schema_qualified_catches_public_prefix(tmp_path: Path) -> None:
    """Codex 1a hardening: ``INSERT INTO public.instruments`` must
    also trip the guard. Catches the false-negative where a schema
    prefix sneaks past the literal-name match."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    violation = tests_dir / "synthetic_schema.py"
    violation.write_text(
        '"""Schema-qualified bypass attempt."""\n'
        'BAD_SQL = """\n'
        "INSERT INTO public.instruments (instrument_id, symbol, company_name)\n"
        "VALUES (1, 'TEST', 'Test')\n"
        '"""\n'
    )
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "synthetic_schema.py" in result.stdout


def test_guard_ignores_is_tradable_outside_column_list(tmp_path: Path) -> None:
    """Codex 1a hardening: ``is_tradable`` mentioned only in an
    ``ON CONFLICT`` clause or a trailing comment — but missing from
    the actual column list — must still fail. The guard slices the
    column list (between the first ``(`` and matching ``)`` after
    ``instruments``) and checks ``is_tradable`` lives THERE."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    violation = tests_dir / "synthetic_decoy.py"
    violation.write_text(
        '"""Decoy is_tradable outside column list."""\n'
        'BAD_SQL = """\n'
        "INSERT INTO instruments (instrument_id, symbol, company_name)\n"
        "VALUES (1, 'TEST', 'Test')\n"
        "ON CONFLICT (instrument_id) DO UPDATE SET\n"
        "  is_tradable = EXCLUDED.is_tradable\n"
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
        f"Expected guard to fail; got returncode={result.returncode}.\nSTDOUT: {result.stdout}"
    )
    assert "synthetic_decoy.py" in result.stdout


def test_guard_ignores_pattern_in_docstring_prose(tmp_path: Path) -> None:
    """The guard must not flag prose / docstrings that quote the
    pattern. Codex 1a hardening: bail silently when no ``(`` is
    found within 5 lines of the opener (definitely not a real SQL
    statement)."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    prose = tests_dir / "synthetic_docstring.py"
    prose.write_text(
        '"""Docstring that quotes the pattern in passing.\n'
        "\n"
        "Prevention: ``INSERT INTO instruments`` fixtures must supply\n"
        "is_tradable.  We supply it explicitly even though it has a\n"
        "default.\n"
        '"""\n'
        "\n"
        "def f() -> None:\n"
        "    pass\n"
    )
    result = subprocess.run(
        ["bash", str(GUARD)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Guard should not flag docstring prose; got returncode={result.returncode}.\nSTDOUT: {result.stdout}"
    )


@pytest.mark.skip(reason="documentation — pins the prevention-log line that motivates this guard")
def test_documents_prevention_log_link() -> None:
    """See docs/review-prevention-log.md §'INSERT INTO instruments
    fixtures must supply is_tradable' for the original incident.

    The prevention-log entry called for the gate in ``tests/fixtures/``
    only; #1233 §6.2 extends it to the whole tree via this script.
    """
