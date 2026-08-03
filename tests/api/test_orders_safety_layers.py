"""Regression: POST /portfolio/orders manual BUY/ADD must honour
safety_layers_enabled — disabled layers block the request with 403.

The safety-layer check fires before any quote or instrument lookup so
these tests do not need to seed an instrument row.  The layer state is
toggled via the existing /sync/layers/{name}/enabled endpoint (which
also hits the dev DB via the shared connection pool), so no direct
psycopg.connect call against settings.database_url is required.

Kill-switch isolation (#2212)
-----------------------------
``clean_client`` drives the real app against the dev DB, and the order path
checks the kill switch FIRST by design (``app/api/orders.py:503`` — "Safety
checks first — kill switch blocks everything"), before the layer gate at :511.
The dev DB carries a deliberately-active kill switch ("autonomy loop unattended
— block any order path", activated_by=monitor, 2026-06-28), so end-to-end both
tests got a 403 for the *kill-switch* reason and never reached the layer gate —
the assertion they exist to make was unreachable, and had been since that switch
was thrown.

The fix patches ``get_kill_switch_status`` as the orders module resolves it, for
the duration of these two tests only. It never writes to ``kill_switch`` —
clearing the operator's standing block to make a test pass would disable every
order path in dev. Kill-switch enforcement itself keeps its own coverage:
tests/test_execution_guard.py::TestCheckKillSwitch (active switch fails, missing
row fails closed) and tests/test_ops_monitor.py::(activate/deactivate/status).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def _kill_switch_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the order path's kill-switch probe report inactive.

    Read-only: patches the symbol, not the row.
    """
    import app.api.orders as orders

    def _inactive(_conn: Any) -> dict[str, Any]:
        return {
            "is_active": False,
            "activated_at": None,
            "activated_by": None,
            "reason": None,
        }

    monkeypatch.setattr(orders, "get_kill_switch_status", _inactive)


@pytest.mark.integration
@pytest.mark.usefixtures("_kill_switch_inactive")
def test_manual_buy_blocked_when_fx_rates_disabled(clean_client: TestClient) -> None:
    clean_client.post(
        "/sync/layers/fx_rates/enabled",
        json={"enabled": False, "reason": "test", "changed_by": "pytest"},
    )
    try:
        resp = clean_client.post(
            "/portfolio/orders",
            json={"instrument_id": 999004, "action": "BUY", "amount": 100},
        )
    finally:
        clean_client.post("/sync/layers/fx_rates/enabled", json={"enabled": True})

    assert resp.status_code == 403, resp.text
    body = resp.text.lower()
    assert "fx_rates" in body or "safety" in body or "disabled" in body


@pytest.mark.integration
@pytest.mark.usefixtures("_kill_switch_inactive")
def test_manual_buy_blocked_when_portfolio_sync_disabled(clean_client: TestClient) -> None:
    clean_client.post(
        "/sync/layers/portfolio_sync/enabled",
        json={"enabled": False, "reason": "test", "changed_by": "pytest"},
    )
    try:
        resp = clean_client.post(
            "/portfolio/orders",
            json={"instrument_id": 999005, "action": "BUY", "amount": 100},
        )
    finally:
        clean_client.post("/sync/layers/portfolio_sync/enabled", json={"enabled": True})

    assert resp.status_code == 403, resp.text
    body = resp.text.lower()
    assert "portfolio_sync" in body or "safety" in body or "disabled" in body
