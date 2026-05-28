"""#1346 — tests for scripts/check_jit_off_in_refresh_helpers.sh.

Invariants:

* I1 — the helper file must contain EXACTLY 10
  ``cur.execute("SET LOCAL jit = off")`` statements (one per refresh
  helper transaction).
* I2 — every ``with conn.transaction(), conn.cursor() as cur:`` opener
  must be IMMEDIATELY followed by the jit=off statement (allowing only
  blank / comment lines in between, and stopping at the first
  executable line).

Happy path: real helper file passes.
Negative paths: synthesised tiny fixtures exercise each I1/I2 failure.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LINT_SCRIPT = REPO_ROOT / "scripts" / "check_jit_off_in_refresh_helpers.sh"


def _run(helper_path: Path | None = None) -> subprocess.CompletedProcess[str]:
    cmd = ["bash", str(LINT_SCRIPT)]
    if helper_path is not None:
        cmd.append(str(helper_path))
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


# Template body that exhibits I1 + I2 compliance for a single helper.
def _good_block(name: str) -> str:
    return (
        f"def {name}(conn, *, instrument_id):\n"
        "    with conn.transaction(), conn.cursor() as cur:\n"
        '        cur.execute("SET LOCAL jit = off")\n'
        "        cur.execute(\n"
        '            "SELECT pg_advisory_xact_lock(1)"\n'
        "        )\n"
    )


def _ten_good_helpers() -> str:
    return "\n".join(_good_block(f"refresh_helper_{i}") for i in range(10))


def test_real_helper_file_passes() -> None:
    """The shipped app/services/ownership_observations.py passes both gates."""
    result = _run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert "10/10 helper transactions have jit=off" in result.stdout


def test_synthetic_ten_good_blocks_pass(tmp_path: Path) -> None:
    fixture = tmp_path / "synthetic.py"
    fixture.write_text(_ten_good_helpers())
    result = _run(fixture)
    assert result.returncode == 0, result.stdout + result.stderr


def test_i1_too_few_jit_off_statements_fails(tmp_path: Path) -> None:
    """9 jit=off lines (one helper missing) → I1 fail."""
    body = _ten_good_helpers()
    # Drop ONE jit=off line.
    body = body.replace(
        '        cur.execute("SET LOCAL jit = off")\n',
        "",
        1,
    )
    fixture = tmp_path / "synthetic.py"
    fixture.write_text(body)
    result = _run(fixture)
    assert result.returncode != 0
    assert "FAIL (I1)" in result.stderr
    assert "found 9" in result.stderr


def test_i1_extra_jit_off_statements_fails(tmp_path: Path) -> None:
    """11 jit=off lines → I1 fail (extra means unrelated SQL drift)."""
    body = _ten_good_helpers()
    body += '        cur.execute("SET LOCAL jit = off")\n'  # spurious extra
    fixture = tmp_path / "synthetic.py"
    fixture.write_text(body)
    result = _run(fixture)
    assert result.returncode != 0
    assert "FAIL (I1)" in result.stderr
    assert "found 11" in result.stderr


def test_i2_jit_off_not_first_executable_fails(tmp_path: Path) -> None:
    """jit=off must precede the first cur.execute(advisory_lock)."""
    body = ""
    for i in range(10):
        body += (
            f"def refresh_helper_{i}(conn, *, instrument_id):\n"
            "    with conn.transaction(), conn.cursor() as cur:\n"
            "        cur.execute(\n"
            '            "SELECT pg_advisory_xact_lock(1)"\n'
            "        )\n"
            '        cur.execute("SET LOCAL jit = off")\n'
            "\n"
        )
    fixture = tmp_path / "synthetic.py"
    fixture.write_text(body)
    result = _run(fixture)
    assert result.returncode != 0
    assert "FAIL (I2)" in result.stderr


def test_i2_allows_comment_lines_before_jit_off(tmp_path: Path) -> None:
    """A leading comment line inside the block is fine; jit=off counts as first executable."""
    body = ""
    for i in range(10):
        body += (
            f"def refresh_helper_{i}(conn, *, instrument_id):\n"
            "    with conn.transaction(), conn.cursor() as cur:\n"
            "        # explanatory comment about the advisory lock\n"
            '        cur.execute("SET LOCAL jit = off")\n'
            "        cur.execute(\n"
            '            "SELECT pg_advisory_xact_lock(1)"\n'
            "        )\n"
        )
    fixture = tmp_path / "synthetic.py"
    fixture.write_text(body)
    result = _run(fixture)
    assert result.returncode == 0, result.stdout + result.stderr


def test_missing_helper_file_errors(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.py"
    result = _run(missing)
    assert result.returncode != 0
    assert "helper file not found" in result.stderr
