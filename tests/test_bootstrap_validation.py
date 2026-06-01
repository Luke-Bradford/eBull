"""Tests for app.services.bootstrap_validation (#1419, P4).

The terminal bootstrap validation gate. Covers each check in isolation plus
the invoker's verdict mapping:

* ``_count_at_least`` — the LIMIT-bounded counting mechanism P6 calibrates.
* ``_check_row_floors`` — absolute floor pass / hard-fail.
* ``_check_panel_render`` — render OK / unresolved-warns / none-render-raises
  (rollup mocked; the real clean-pass is P6's DoD 8-12).
* ``_check_cross_source`` — clean / mild-oversub-warns / gross-raises.
* ``run_bootstrap_validation`` — verdict persisted to
  ``bootstrap_runs.validation_gate_status`` (passed / warned / failed_<id>) and
  a hard breach re-raises (→ stage error → partial_error).

Per-check connection note: the check functions take a connection (so tests pass
``ebull_test_conn`` + temp tables), but ``run_bootstrap_validation`` opens its
OWN connection — so the invoker tests mock the check functions rather than seed
data a separate connection could not see.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

import app.services.bootstrap_validation as bv
from app.services.bootstrap_validation import BootstrapValidationError, run_bootstrap_validation


def _bind_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> str:
    """Point ``settings.database_url`` at the worker's private test DB so the
    invoker's own ``psycopg.connect`` writes the verdict there (not the dev DB —
    test_db_isolation feedback memory)."""
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", url)
    return url


def _fake_rollup(
    *,
    state: str = "green",
    shares: Decimal | None = Decimal(1000),
    pct_known: Decimal = Decimal("0.50"),
    oversub: bool = False,
) -> Any:
    """A minimal stand-in for OwnershipRollup carrying only the attributes the
    panel + reconciliation checks read. Typed ``Any`` so the duck-typed
    namespace satisfies the ``OwnershipRollup``-typed check signatures."""
    return SimpleNamespace(
        banner=SimpleNamespace(state=state),
        shares_outstanding=shares,
        concentration=SimpleNamespace(pct_outstanding_known=pct_known),
        residual=SimpleNamespace(oversubscribed=oversub),
    )


def _make_running_run(conn: psycopg.Connection[tuple]) -> int:
    row = conn.execute("INSERT INTO bootstrap_runs (status) VALUES ('running') RETURNING id").fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# _count_at_least — bounded-count mechanism
# ---------------------------------------------------------------------------


def test_count_at_least_bounded_and_accurate_shortfall(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.execute("CREATE TEMP TABLE _vt_count (x int)")
    conn.execute("INSERT INTO _vt_count VALUES (1), (2), (3)")

    # floor met: got is capped at floor (LIMIT bounds the scan).
    assert bv._count_at_least(conn, "_vt_count", 1) == (True, 1)
    assert bv._count_at_least(conn, "_vt_count", 3) == (True, 3)
    # floor NOT met: got is the TRUE total (LIMIT floor > total returns all).
    assert bv._count_at_least(conn, "_vt_count", 5) == (False, 3)

    conn.execute("DELETE FROM _vt_count")
    assert bv._count_at_least(conn, "_vt_count", 1) == (False, 0)


# ---------------------------------------------------------------------------
# _check_row_floors
# ---------------------------------------------------------------------------


def test_check_row_floors_raises_when_table_empty(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    conn.execute("CREATE TEMP TABLE _vt_floor (x int)")  # empty
    monkeypatch.setattr(bv, "_ROW_FLOORS", {"_vt_floor": 1})

    with pytest.raises(BootstrapValidationError) as exc_info:
        bv._check_row_floors(conn)
    assert exc_info.value.check_id == "row_floor"
    assert "_vt_floor" in str(exc_info.value)


def test_check_row_floors_passes_when_floor_met(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    conn.execute("CREATE TEMP TABLE _vt_floor (x int)")
    conn.execute("INSERT INTO _vt_floor VALUES (1)")
    monkeypatch.setattr(bv, "_ROW_FLOORS", {"_vt_floor": 1})

    bv._check_row_floors(conn)  # no raise


# ---------------------------------------------------------------------------
# _check_panel_render (rollup mocked)
# ---------------------------------------------------------------------------


def test_check_panel_render_all_render(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bv, "_resolve_instrument", lambda conn, sym: (1, sym))
    monkeypatch.setattr(bv, "get_ownership_rollup", lambda conn, symbol, instrument_id: _fake_rollup())

    warnings: list[str] = []
    rendered = bv._check_panel_render(ebull_test_conn, warnings)
    assert len(rendered) == len(bv._PANEL)
    assert warnings == []


def test_check_panel_render_unresolved_instrument_warns_not_fails(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The last panel symbol is not in the universe; the rest render fine.
    def fake_resolve(conn: object, sym: str) -> tuple[int, str] | None:
        return None if sym == bv._PANEL[-1] else (1, sym)

    monkeypatch.setattr(bv, "_resolve_instrument", fake_resolve)
    monkeypatch.setattr(bv, "get_ownership_rollup", lambda conn, symbol, instrument_id: _fake_rollup())

    warnings: list[str] = []
    rendered = bv._check_panel_render(ebull_test_conn, warnings)
    assert len(rendered) == len(bv._PANEL) - 1
    assert any("not in universe" in w for w in warnings)


def test_check_panel_render_none_render_raises(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bv, "_resolve_instrument", lambda conn, sym: (1, sym))
    monkeypatch.setattr(
        bv,
        "get_ownership_rollup",
        lambda conn, symbol, instrument_id: _fake_rollup(state="no_data", shares=None),
    )

    warnings: list[str] = []
    with pytest.raises(BootstrapValidationError) as exc_info:
        bv._check_panel_render(ebull_test_conn, warnings)
    assert exc_info.value.check_id == "panel"


def test_check_panel_render_missing_shares_does_not_render(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    # banner OK but shares_outstanding absent → not rendered.
    monkeypatch.setattr(bv, "_resolve_instrument", lambda conn, sym: (1, sym))
    monkeypatch.setattr(
        bv,
        "get_ownership_rollup",
        lambda conn, symbol, instrument_id: _fake_rollup(state="amber", shares=None),
    )
    monkeypatch.setattr(bv, "_MIN_PANEL_RENDERS", 1)

    warnings: list[str] = []
    with pytest.raises(BootstrapValidationError) as exc_info:
        bv._check_panel_render(ebull_test_conn, warnings)
    assert exc_info.value.check_id == "panel"


# ---------------------------------------------------------------------------
# _check_cross_source (rollups mocked)
# ---------------------------------------------------------------------------


def test_check_cross_source_clean() -> None:
    warnings: list[str] = []
    bv._check_cross_source([("AAPL", _fake_rollup(pct_known=Decimal("0.60")))], warnings)
    assert warnings == []


def test_check_cross_source_mild_oversubscription_warns() -> None:
    warnings: list[str] = []
    bv._check_cross_source([("GME", _fake_rollup(pct_known=Decimal("1.10"), oversub=True))], warnings)
    assert any("oversubscribed" in w for w in warnings)


def test_check_cross_source_gross_oversubscription_raises() -> None:
    warnings: list[str] = []
    with pytest.raises(BootstrapValidationError) as exc_info:
        bv._check_cross_source([("XYZ", _fake_rollup(pct_known=Decimal("2.00"), oversub=True))], warnings)
    assert exc_info.value.check_id == "reconciliation"


def test_check_cross_source_threshold_is_inclusive_boundary() -> None:
    # Exactly at the bound is NOT a breach (strictly greater fails).
    warnings: list[str] = []
    bv._check_cross_source([("EDGE", _fake_rollup(pct_known=bv._MAX_PCT_OUTSTANDING_KNOWN, oversub=False))], warnings)
    assert warnings == []


# ---------------------------------------------------------------------------
# run_bootstrap_validation — verdict mapping (checks mocked, real run row)
# ---------------------------------------------------------------------------


def _read_verdict(conn: psycopg.Connection[tuple], run_id: int) -> str | None:
    conn.rollback()  # drop any stale read-snapshot so we see the invoker's commit
    row = conn.execute("SELECT validation_gate_status FROM bootstrap_runs WHERE id = %s", (run_id,)).fetchone()
    assert row is not None
    return row[0]


def test_invoker_persists_passed_verdict(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    _bind_settings_to_test_db(monkeypatch)
    conn = ebull_test_conn
    run_id = _make_running_run(conn)
    conn.commit()

    monkeypatch.setattr(bv, "_check_row_floors", lambda c: None)
    monkeypatch.setattr(bv, "_check_panel_render", lambda c, w: [])
    monkeypatch.setattr(bv, "_check_cross_source", lambda r, w: None)

    with active_bootstrap_run(run_id, "bootstrap_validation"):
        run_bootstrap_validation()

    assert _read_verdict(conn, run_id) == "passed"


def test_invoker_persists_warned_verdict(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    _bind_settings_to_test_db(monkeypatch)
    conn = ebull_test_conn
    run_id = _make_running_run(conn)
    conn.commit()

    def _panel_with_warning(c: object, w: list[str]) -> list[object]:
        w.append("panel: HD not in universe (skipped)")
        return []

    monkeypatch.setattr(bv, "_check_row_floors", lambda c: None)
    monkeypatch.setattr(bv, "_check_panel_render", _panel_with_warning)
    monkeypatch.setattr(bv, "_check_cross_source", lambda r, w: None)

    with active_bootstrap_run(run_id, "bootstrap_validation"):
        run_bootstrap_validation()

    assert _read_verdict(conn, run_id) == "warned"


def test_invoker_persists_failed_verdict_and_reraises(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    _bind_settings_to_test_db(monkeypatch)
    conn = ebull_test_conn
    run_id = _make_running_run(conn)
    conn.commit()

    def _floors_fail(c: object) -> None:
        raise BootstrapValidationError("row_floor", "filing_events has 0 rows, want >= 1")

    monkeypatch.setattr(bv, "_check_row_floors", _floors_fail)

    # The hard breach re-raises so _run_one_stage marks the stage error
    # (→ finalize_run → partial_error).
    with active_bootstrap_run(run_id, "bootstrap_validation"):
        with pytest.raises(BootstrapValidationError) as exc_info:
            run_bootstrap_validation()
    assert exc_info.value.check_id == "row_floor"

    # The verdict column records WHICH check failed (informational; the gate is
    # the stage-error path, not this column).
    assert _read_verdict(conn, run_id) == "failed_row_floor"


def test_invoker_no_active_run_does_not_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    # Outside active_bootstrap_run the contextvar is unset → verdict not
    # persisted, but the checks still run (and pass when mocked).
    _bind_settings_to_test_db(monkeypatch)
    monkeypatch.setattr(bv, "_check_row_floors", lambda c: None)
    monkeypatch.setattr(bv, "_check_panel_render", lambda c, w: [])
    monkeypatch.setattr(bv, "_check_cross_source", lambda r, w: None)

    run_bootstrap_validation()  # no raise, no run_id to persist
