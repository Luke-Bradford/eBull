"""Regression: POST /portfolio/orders manual BUY/ADD must honour
safety_layers_enabled — disabled layers block the request with 403.

The safety-layer check fires before any quote or instrument lookup so
these tests do not need to seed an instrument row.  The layer state is
toggled via the existing /sync/layers/{name}/enabled endpoint (which
also hits the dev DB via the shared connection pool), so no direct
psycopg.connect call against settings.database_url is required.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.mark.integration
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
