"""Meta-test for scripts/check_n_csr_retention.sh (#1233 §4.12 / PR8).

Pins the lint guard against the failure modes the PR8 plan + Codex
1a/1b/1c reviews identified:

1. Clean source tree passes.
2. Duplicating the helper definition trips invariant A.
3. Removing the ``N_CSR_RETENTION_DAYS:`` annotated assignment trips
   invariant A.
4. Inlining ``timedelta(days=730)`` inside ``bootstrap_n_csr_drain``
   trips invariant B.
5. Removing the ``n_csr_retention_cutoff(`` call inside
   ``bootstrap_n_csr_drain`` trips invariant B.
6. Moving the parser gate AFTER ``_fetch_ixbrl(`` trips invariant D.
7. Removing the parser gate entirely trips invariant D.
8. The guard script is executable.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / "scripts" / "check_n_csr_retention.sh"
SRC_PARSER = REPO_ROOT / "app/services/manifest_parsers/sec_n_csr.py"
SRC_BOOTSTRAP = REPO_ROOT / "app/jobs/sec_first_install_drain.py"


def _stage_tree(tmp_path: Path) -> None:
    """Copy the two guarded files under their real paths so the
    guard's hardcoded paths resolve when invoked with ``cwd=tmp_path``."""
    for rel in (
        "app/services/manifest_parsers/sec_n_csr.py",
        "app/jobs/sec_first_install_drain.py",
    ):
        src = REPO_ROOT / rel
        dst = tmp_path / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dst)


def _run_guard(cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(GUARD)],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def test_guard_is_executable() -> None:
    import os

    assert os.access(GUARD, os.X_OK), f"{GUARD} is not executable; chmod +x is required for the pre-push wire."


def test_guard_passes_on_clean_tree() -> None:
    """The current source tree must pass the guard."""
    result = _run_guard(REPO_ROOT)
    assert result.returncode == 0, f"Lint guard failed on clean tree:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"


def test_guard_fails_on_duplicate_helper(tmp_path: Path) -> None:
    """Adding a second ``def n_csr_retention_cutoff(`` trips A."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/manifest_parsers/sec_n_csr.py"
    text = target.read_text()
    patched = text + "\n\ndef n_csr_retention_cutoff(now):\n    return now\n"
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "expected exactly 1 'def n_csr_retention_cutoff(" in result.stderr


def test_guard_fails_when_constant_assignment_removed(tmp_path: Path) -> None:
    """Stripping the ``N_CSR_RETENTION_DAYS:`` annotated assignment
    trips A (assignment line is the only reference that counts)."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/manifest_parsers/sec_n_csr.py"
    text = target.read_text()
    patched = text.replace("N_CSR_RETENTION_DAYS: int = 730", "_N_CSR_RETENTION_DAYS = 730", 1)
    assert patched != text, "expected the annotated assignment substring in source"
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "column-0 'N_CSR_RETENTION_DAYS:'" in result.stderr


def test_guard_fails_on_inlined_730d_math(tmp_path: Path) -> None:
    """Inlining ``timedelta(days=730)`` inside ``bootstrap_n_csr_drain``
    trips invariant B even when the helper call is still present."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/jobs/sec_first_install_drain.py"
    text = target.read_text()
    patched = text.replace(
        "cutoff = n_csr_retention_cutoff()",
        ("cutoff = n_csr_retention_cutoff()\n    _shadow = datetime.now(UTC) - timedelta(days=730)"),
        1,
    )
    assert patched != text, "expected the cutoff line in source"
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "forbidden inlined 'timedelta(days=730)' math" in result.stderr


def test_guard_fails_when_helper_call_missing(tmp_path: Path) -> None:
    """Removing the ``n_csr_retention_cutoff(`` call inside
    ``bootstrap_n_csr_drain`` trips invariant B."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/jobs/sec_first_install_drain.py"
    text = target.read_text()
    patched = text.replace(
        "cutoff = n_csr_retention_cutoff()",
        "cutoff = datetime.now(UTC)  # bypass cap",
        1,
    )
    assert patched != text, "expected helper call in source"
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "shared helper not wired" in result.stderr


def test_guard_fails_when_parser_gate_after_fetch(tmp_path: Path) -> None:
    """Moving the gate after ``_fetch_ixbrl(`` trips invariant D."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/manifest_parsers/sec_n_csr.py"
    text = target.read_text()
    # Strip the gate block in its current location...
    gate_block = '    if not n_csr_within_retention(filed_at):\n        return _tombstoned("outside_retention")\n'
    assert gate_block in text, "expected current gate block in source"
    stripped = text.replace(gate_block, "", 1)
    # ...and re-insert it AFTER the first ``_fetch_ixbrl(`` line.
    fetch_marker = "        ixbrl_bytes = _fetch_ixbrl(ixbrl_url)\n"
    assert fetch_marker in stripped, "expected fetch marker in source"
    moved = stripped.replace(
        fetch_marker,
        fetch_marker + gate_block,
        1,
    )
    assert moved != text, "no-op patch"
    target.write_text(moved)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "must short-circuit BEFORE the HTTP fetch" in result.stderr


def test_guard_fails_when_parser_gate_removed(tmp_path: Path) -> None:
    """Removing the gate entirely trips invariant D."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/manifest_parsers/sec_n_csr.py"
    text = target.read_text()
    gate_block = '    if not n_csr_within_retention(filed_at):\n        return _tombstoned("outside_retention")\n'
    assert gate_block in text, "expected current gate block in source"
    target.write_text(text.replace(gate_block, "", 1))

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "missing n_csr_within_retention(...) pre-fetch gate" in result.stderr
