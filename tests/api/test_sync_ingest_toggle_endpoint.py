"""Tests for the ingest-toggle endpoint (#414 design goal F).

Operator pause/resume for scheduled jobs that are not orchestrator
layers. Uses the same ``layer_enabled`` table underneath — absent row
counts as enabled, POST False flips to disabled, POST True restores.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.integration
def test_post_ingest_enabled_happy_path(clean_client: TestClient) -> None:
    try:
        resp = clean_client.post("/sync/ingest/fundamentals_ingest/enabled", json={"enabled": False})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["key"] == "fundamentals_ingest"
        assert body["is_enabled"] is False
        assert "fundamentals" in body["display_name"].lower()

        resp2 = clean_client.post("/sync/ingest/fundamentals_ingest/enabled", json={"enabled": True})
        assert resp2.status_code == 200
        assert resp2.json()["is_enabled"] is True
    finally:
        clean_client.post("/sync/ingest/fundamentals_ingest/enabled", json={"enabled": True})


@pytest.mark.integration
def test_post_ingest_enabled_unknown_key_404(clean_client: TestClient) -> None:
    resp = clean_client.post("/sync/ingest/not_a_real_key/enabled", json={"enabled": False})
    assert resp.status_code == 404
    assert "unknown ingest key" in resp.json()["detail"]


def test_post_ingest_enabled_requires_auth() -> None:
    from app.api.sync import router as sync_router
    from app.db import get_conn

    def _mock_conn():  # type: ignore[return]
        yield MagicMock()

    bare = FastAPI()
    bare.include_router(sync_router)
    bare.dependency_overrides[get_conn] = _mock_conn
    with TestClient(bare) as client:
        resp = client.post("/sync/ingest/fundamentals_ingest/enabled", json={"enabled": False})
    assert resp.status_code in {401, 403}, resp.text
