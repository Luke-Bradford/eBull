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
   (intraday-candles), V2 (validate-stored), and V3/V4 (8-K body +
   business-sections lazy-fill, fixed in #1492) all release the pooled
   conn before the external call — none take ``Depends(get_conn)`` while
   referencing an external client.
2. Negative — a synthetic violator (``Depends(get_conn)`` + a
   ``SecFilingsProvider`` reference) trips the guard (exit 1 + the
   function name printed).
3. The fixture's DB-only ``clean_route`` is NOT flagged (no external
   marker).
4. Re-armed — the ALLOWLIST is empty (#1492 removed V3/V4), and the two
   former-deferred routes pass on their own (fetch-first), not via a
   waiver.
"""

from __future__ import annotations

import importlib.util
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
    """The production app/api tree passes (V1-V4 all fixed; allowlist empty)."""
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


def test_former_deferred_routes_pass_unallowlisted() -> None:
    """The two former-deferred routes (#1492) now pass on their own merit —
    fetch-first, no ``Depends(get_conn)`` — not via a waiver."""
    result = _run(REPO_ROOT / "app" / "api" / "instruments.py")
    assert result.returncode == 0, f"a former-deferred route regressed:\n{result.stdout}"
    assert "get_instrument_8k_filing_body" not in result.stdout
    assert "get_instrument_business_sections" not in result.stdout


def test_allowlist_is_empty() -> None:
    """#1492 re-armed the tripwire: the ALLOWLIST carries no waivers, so a
    future ``Depends(get_conn)``-shaped regression on either route trips."""
    spec = importlib.util.spec_from_file_location("check_pooled_conn_across_http", SCRIPT_PY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.ALLOWLIST == frozenset()
