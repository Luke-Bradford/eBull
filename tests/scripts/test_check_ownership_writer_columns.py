"""#1256 — tests for scripts/_check_ownership_writer_columns.py.

10 happy paths cover all real helpers (7 single + 3 batch) via the actual
source file. 7 negative tests cover each regression class invariant I axes
exist to prevent (Codex iter-1+2 BLOCKING/IMPORTANT fold):

* drop-column-from-update-set         → I.a
* wrong-prefix (tgt on RHS / src on LHS) → shape-gate + I.b
* LHS-RHS-name-mismatch               → I.b
* duplicated-col-in-update-set        → I.d
* duplicated-col-in-diff-tuple        → I.d
* refreshed_at-leaks-into-diff        → I.c
* update-assignment-mismatch          → I.e
* shape-violation-inline-comment      → shape-gate

Each negative test uses ``--source-text`` (no temp files needed).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LINT_SCRIPT = REPO_ROOT / "scripts" / "_check_ownership_writer_columns.py"
OWNERSHIP_OBSERVATIONS = REPO_ROOT / "app" / "services" / "ownership_observations.py"


def _run(args: list[str], source_text: str | None = None) -> subprocess.CompletedProcess[str]:
    """Invoke the lint script with the given args + optional --source-text."""
    cmd = [sys.executable, str(LINT_SCRIPT), *args]
    if source_text is not None:
        cmd.extend(["--source-text", source_text])
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


# ---------------------------------------------------------------------------
# Happy paths: all 10 real helpers pass against the real source file.
# ---------------------------------------------------------------------------

REAL_HELPERS = [
    "refresh_insiders_current",
    "refresh_institutions_current",
    "refresh_blockholders_current",
    "refresh_treasury_current",
    "refresh_def14a_current",
    "refresh_funds_current",
    "refresh_esop_current",
    "refresh_insiders_current_batch",
    "refresh_institutions_current_batch",
    "refresh_funds_current_batch",
]


@pytest.mark.parametrize("function_name", REAL_HELPERS)
def test_real_helper_passes(function_name: str) -> None:
    """Each of 10 real helpers passes all 5 invariant axes."""
    result = _run(["--function", function_name, str(OWNERSHIP_OBSERVATIONS)])
    assert result.returncode == 0, (
        f"Real helper {function_name} unexpectedly failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_real_source_coverage_report_lists_all_10() -> None:
    """Coverage report finds all 10 expected helpers + reports them PASS."""
    result = _run(["--coverage-report", str(OWNERSHIP_OBSERVATIONS)])
    assert result.returncode == 0, result.stderr
    assert "10 functions covered (expected 10)" in result.stdout


# ---------------------------------------------------------------------------
# Minimal helper-body template for negative tests
# ---------------------------------------------------------------------------


# Smallest possible function-body shape that the lint can parse. Real helpers
# have ~80 lines of Python wrapping the MERGE; for negative tests we only need
# the MERGE block to be embedded inside something starting with ``def NAME(``.
def _template(diff_lhs: str, diff_rhs: str, update_set: str) -> str:
    return f'''def refresh_test_current(conn, *, instrument_id):
    """Synthetic helper for invariant-I lint tests."""
    cur.execute("""
        MERGE INTO ownership_test_current AS tgt
        USING (...) AS src
        ON tgt.instrument_id = %(iid)s
        WHEN MATCHED AND (
{diff_lhs}
        ) IS DISTINCT FROM (
{diff_rhs}
        ) THEN UPDATE SET
{update_set}
        WHEN NOT MATCHED BY TARGET THEN INSERT (...) VALUES (...)
        WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
        """, {{"iid": instrument_id}})
'''


# Canonical valid 2-col helper for baseline reference.
_VALID_LHS = "            tgt.shares, tgt.filed_at"
_VALID_RHS = "            src.shares, src.filed_at"
_VALID_UPDATE = """            shares       = src.shares,
            filed_at     = src.filed_at,
            refreshed_at = now()"""


def test_baseline_template_passes() -> None:
    """The canonical template itself passes — confirms negative tests below
    diverge from a known-good baseline."""
    source = _template(_VALID_LHS, _VALID_RHS, _VALID_UPDATE)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Negative tests — each fires a specific invariant axis
# ---------------------------------------------------------------------------


def test_drop_column_from_update_set_fails_I_a() -> None:
    """I.a: dropping a business col from UPDATE SET while keeping in diff."""
    update_set_missing_filed_at = """            shares       = src.shares,
            refreshed_at = now()"""
    source = _template(_VALID_LHS, _VALID_RHS, update_set_missing_filed_at)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "I.a" in result.stderr
    assert "filed_at" in result.stderr


def test_wrong_prefix_src_on_lhs_fails_shape_gate() -> None:
    """Shape gate: src.* on LHS (should be tgt.*) — Codex iter-2 BLOCKING-1."""
    bad_lhs = "            src.shares, src.filed_at"  # all src on LHS
    source = _template(bad_lhs, _VALID_RHS, _VALID_UPDATE)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "shape violation" in result.stderr
    assert "diff-tuple LHS" in result.stderr


def test_lhs_rhs_name_mismatch_fails_I_b() -> None:
    """I.b: LHS and RHS have same count but different column names."""
    bad_rhs = "            src.shares, src.period_end"  # filed_at → period_end
    update_set_for_period_end = """            shares       = src.shares,
            period_end   = src.period_end,
            refreshed_at = now()"""
    source = _template(_VALID_LHS, bad_rhs, update_set_for_period_end)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "I.b" in result.stderr


def test_duplicated_col_in_update_set_fails_I_d() -> None:
    """I.d: same col appears twice in UPDATE SET."""
    update_set_dup = """            shares       = src.shares,
            shares       = src.shares,
            filed_at     = src.filed_at,
            refreshed_at = now()"""
    source = _template(_VALID_LHS, _VALID_RHS, update_set_dup)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "I.d" in result.stderr
    assert "shares" in result.stderr


def test_duplicated_col_in_diff_tuple_fails_I_d() -> None:
    """I.d: same col appears twice in diff LHS."""
    dup_lhs = "            tgt.shares, tgt.shares, tgt.filed_at"
    dup_rhs = "            src.shares, src.shares, src.filed_at"
    source = _template(dup_lhs, dup_rhs, _VALID_UPDATE)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "I.d" in result.stderr


def test_refreshed_at_leaks_into_diff_fails_I_c() -> None:
    """I.c: refreshed_at must NEVER appear in diff LHS or RHS."""
    bad_lhs = "            tgt.shares, tgt.filed_at, tgt.refreshed_at"
    bad_rhs = "            src.shares, src.filed_at, src.refreshed_at"
    update_set_with_refreshed = """            shares       = src.shares,
            filed_at     = src.filed_at,
            refreshed_at = now()"""
    source = _template(bad_lhs, bad_rhs, update_set_with_refreshed)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "I.c" in result.stderr


def test_update_assignment_mismatch_fails_I_e() -> None:
    """I.e: UPDATE SET pair `foo = src.bar` (LHS != RHS-after-prefix-strip)."""
    update_set_swap = """            shares       = src.filed_at,
            filed_at     = src.shares,
            refreshed_at = now()"""
    source = _template(_VALID_LHS, _VALID_RHS, update_set_swap)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "I.e" in result.stderr


def test_shape_violation_inline_comment_fails() -> None:
    """Shape gate: inline comment in UPDATE SET line."""
    update_with_comment = """            shares       = src.shares,  -- inline
            filed_at     = src.filed_at,
            refreshed_at = now()"""
    source = _template(_VALID_LHS, _VALID_RHS, update_with_comment)
    result = _run(["--function", "refresh_test_current"], source_text=source)
    assert result.returncode == 2
    assert "shape violation" in result.stderr


# ---------------------------------------------------------------------------
# CLI contract: refusal paths
# ---------------------------------------------------------------------------


def test_missing_function_arg_errors() -> None:
    """--function required (unless --coverage-report)."""
    result = _run([], source_text="x")
    assert result.returncode != 0
    assert "either source_file or --source-text" in result.stderr or "required" in result.stderr


def test_mutual_exclusion_source_text_vs_file() -> None:
    """--source-text and source_file are mutually exclusive."""
    cmd = [sys.executable, str(LINT_SCRIPT), "--function", "x", "--source-text", "y", str(OWNERSHIP_OBSERVATIONS)]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert "mutually exclusive" in result.stderr
