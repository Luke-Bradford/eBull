"""HTTP-layer tests for GET /portfolio/value-history (#204, #1594).

The data-path logic moved into pure helpers when PR-B rewrote the
endpoint on the ``trade_events`` ledger (#1594): the formula, FX
carry-forward, persisted overlay, and units timeline are table-tested in
``tests/test_portfolio_value_history.py``; the SQL wiring (closed-position
history, overlay, markers) in ``tests/test_value_history_db.py``. What
remains here is the request-layer contract that needs the FastAPI stack.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_value_history_rejects_unknown_range(client: TestClient) -> None:
    """Unknown range → FastAPI 422 from the ``Literal`` param validator.
    Pins that we don't silently default to '1y' on unexpected input.
    Overrides get_conn with a noop since dep resolution runs alongside
    param validation in FastAPI."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_conn] = _conn
    try:
        resp = client.get("/portfolio/value-history?range=2y")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 422
