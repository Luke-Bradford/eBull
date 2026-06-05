"""Acceptance tests for ``scripts/check_pooled_conn_across_http.py``.

Spec: #1472 PR2 (connection-lifetime audit) +
docs/proposals/infra/2026-06-04-db-connection-discipline.md §PR2.

The guard enforces that a route declaring ``conn = Depends(get_conn)``
(which holds the pooled connection for its whole body) does NOT reach an
external-provider HTTP client (eToro / SEC EDGAR) in that body — a conn
pinned across slow external I/O stalls a small pool (block-then-
PoolTimeout, ``max_waiting=0``).

Pins:
1. Positive — the real ``app/api/*.py`` tree is clean (exit 0): V1
   (intraday-candles) + V2 (validate-stored) were fixed to release the
   conn before the external call, and the two KNOWN-DEFERRED routes
   (#1492) are on the script's ALLOWLIST.
2. Negative — a synthetic violator (``Depends(get_conn)`` + a
   ``SecFilingsProvider`` reference) trips the guard (exit 1 + the
   function name printed).
3. The fixture's DB-only ``clean_route`` is NOT flagged (no external
   marker).
4. Allowlist — the two deferred routes in ``app/api/instruments.py`` are
   not flagged even though they reference ``SecFilingsProvider``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PY = REPO_ROOT / "scripts" / "check_pooled_conn_across_http.py"
VIOLATOR_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "lint" / "pooled_conn_across_http_violator.py"


def _run(*extra_paths: str | Path) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(SCRIPT_PY)] + [str(p) for p in extra_paths]
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)


def test_real_api_tree_is_clean() -> None:
    """The production app/api tree passes (V1/V2 fixed, V3/V4 allowlisted)."""
    result = _run()
    assert result.returncode == 0, (
        f"guard flagged a real route — a Depends(get_conn) route holds the "
        f"pooled conn across an external HTTP call:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_violator_fixture_is_flagged() -> None:
    """A synthetic Depends(get_conn) + SecFilingsProvider route trips the guard."""
    result = _run(VIOLATOR_FIXTURE)
    assert result.returncode == 1, f"expected exit 1, got {result.returncode}\n{result.stdout}"
    assert "violating_route" in result.stdout
    assert "pooled-conn-across-HTTP violation" in result.stderr


def test_keyword_form_depends_is_flagged() -> None:
    """``Depends(dependency=get_conn)`` (keyword form) is also detected."""
    result = _run(VIOLATOR_FIXTURE)
    assert result.returncode == 1
    assert "kw_violating_route" in result.stdout


def test_clean_route_in_fixture_not_flagged() -> None:
    """The fixture's DB-only route (no external marker) is not reported."""
    result = _run(VIOLATOR_FIXTURE)
    assert "clean_route" not in result.stdout


def test_allowlisted_deferred_routes_not_flagged() -> None:
    """The two KNOWN-DEFERRED routes (#1492) in instruments.py are allowlisted."""
    result = _run(REPO_ROOT / "app" / "api" / "instruments.py")
    assert result.returncode == 0, f"allowlist not honoured:\n{result.stdout}"
    assert "get_instrument_8k_filing_body" not in result.stdout
    assert "get_instrument_business_sections" not in result.stdout
