"""Meta-test for scripts/check_form4_retention.sh (#1233 §4.3 PR4).

Pins the lint guard against every failure mode Codex 1b/1c/1d flagged
during the PR4 plan review:

1. Clean source tree passes.
2. Removing one ``%(retention_cutoff)s`` binding from
   ``insider_transactions.py`` while leaving the SQL block intact
   breaks parity → guard fails.
3. Adding a new ``filing_type IN ('4', '4/A')`` SQL block without a
   corresponding ``%(retention_cutoff)s`` binding breaks parity →
   guard fails.
4. Removing the manifest-worker ``form4_within_retention(`` gate
   from ``insider_345.py:_parse_form4`` → guard fails.
5. Removing both ``filed_at.date() < retention_cutoff`` predicates
   from the bulk-dataset ingester (one per write loop) → guard fails.
6. ``form4_retention_cutoff(`` does NOT count call-sites that live on
   ``def`` lines or inside docstrings (Codex 1d).
7. The guard script is executable.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / "scripts" / "check_form4_retention.sh"
SRC_INSIDER_TXNS = REPO_ROOT / "app/services/insider_transactions.py"
SRC_MANIFEST_FORM4 = REPO_ROOT / "app/services/manifest_parsers/insider_345.py"
SRC_BULK_DATASET = REPO_ROOT / "app/services/sec_insider_dataset_ingest.py"


def _stage_tree(tmp_path: Path) -> None:
    """Copy the three guarded files into the temp tree under their
    real-repo paths so the guard's hardcoded paths resolve when
    invoked with ``cwd=tmp_path``."""
    for rel_src in (
        "app/services/insider_transactions.py",
        "app/services/manifest_parsers/insider_345.py",
        "app/services/sec_insider_dataset_ingest.py",
    ):
        src = REPO_ROOT / rel_src
        dst = tmp_path / rel_src
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


def test_guard_passes_on_clean_tree() -> None:
    """The current source tree must pass the guard. Any developer who
    adds a Form 4 SQL block without ``%(retention_cutoff)s`` or strips
    a manifest/bulk gate sees this fail."""
    result = _run_guard(REPO_ROOT)
    assert result.returncode == 0, f"Lint guard failed on clean tree:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"


def test_guard_fails_when_retention_cutoff_binding_removed(
    tmp_path: Path,
) -> None:
    """Remove one ``%(retention_cutoff)s`` from
    ``insider_transactions.py`` (keep the SQL block intact). Parity
    breaks: 4 blocks vs 3 bindings → guard fails."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/insider_transactions.py"
    text = target.read_text()
    patched = text.replace(
        "AND fe.filing_date >= %(retention_cutoff)s",
        "AND fe.filing_date >= '2023-01-01'::date",
        1,  # exactly one occurrence
    )
    assert patched != text, "Expected the marker substring in the source file"
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1, (
        f"Expected parity failure; got rc={result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    assert "parity broken" in result.stderr


def test_guard_fails_when_new_chokepoint_added_without_binding(
    tmp_path: Path,
) -> None:
    """Inject a NEW ``filing_type IN ('4', '4/A')`` SQL block without
    a corresponding ``%(retention_cutoff)s`` binding. Parity breaks:
    5 blocks vs 4 bindings → guard fails."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/insider_transactions.py"
    text = target.read_text()
    new_block = (
        "\n# Synthetic new chokepoint added without retention predicate.\n"
        "_BAD_SQL = '''\n"
        "SELECT fe.instrument_id\n"
        "FROM filing_events fe\n"
        "JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id\n"
        "WHERE fe.provider = 'sec'\n"
        "  AND fe.filing_type IN ('4', '4/A')\n"
        "'''\n"
    )
    target.write_text(text + new_block)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "parity broken" in result.stderr or "expected 4" in result.stderr


def test_guard_fails_when_manifest_gate_stripped(tmp_path: Path) -> None:
    """Remove the only ``form4_within_retention(`` call in the
    manifest-worker parser → guard fails."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/manifest_parsers/insider_345.py"
    text = target.read_text()
    # Replace the call with a harmless no-op so the file still parses.
    patched = text.replace("form4_within_retention(filed_at.date())", "True")
    assert patched != text, "Expected the marker call in the source file"
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "pre-fetch gate" in result.stderr


def test_guard_fails_when_bulk_dataset_loop_gates_stripped(
    tmp_path: Path,
) -> None:
    """Remove the ``filed_at.date() < retention_cutoff`` predicate from
    both bulk-dataset write loops → guard fails. The predicate is the
    Form-4-only retention gate; without it Form 4 rows from any era
    flow through to ``record_insider_observation``."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/sec_insider_dataset_ingest.py"
    text = target.read_text()
    patched = text.replace("filed_at.date() < retention_cutoff", "False")
    assert patched != text
    target.write_text(patched)

    result = _run_guard(tmp_path)
    assert result.returncode == 1
    assert "per write loop" in result.stderr


def test_guard_excludes_def_and_docstring_mentions(tmp_path: Path) -> None:
    """A file consisting ONLY of a ``def form4_retention_cutoff(`` line
    plus a docstring referencing the symbol should report 0 call-sites
    — proves the ``def`` exclusion + comment exclusion work (Codex
    1d). We exercise this by writing a stub bulk-dataset file with
    those exact contents and asserting the guard rejects it (because
    the required 1 call-site + 2 loop-call-sites are absent)."""
    _stage_tree(tmp_path)
    target = tmp_path / "app/services/sec_insider_dataset_ingest.py"
    target.write_text(
        '"""Stub. References form4_retention_cutoff in this docstring only."""\n'
        "\n"
        "def form4_retention_cutoff():\n"
        "    pass\n"
        "\n"
        "# Comment mentioning form4_retention_cutoff() should not count.\n"
    )
    result = _run_guard(tmp_path)
    assert result.returncode == 1
    # The cutoff-call count comes back as zero (def line excluded,
    # docstring excluded, comment excluded).
    assert "expected exactly 1 form4_retention_cutoff" in result.stderr


def test_guard_is_executable() -> None:
    import os
    import stat

    mode = os.stat(GUARD).st_mode
    assert mode & stat.S_IXUSR, "scripts/check_form4_retention.sh must be executable"
