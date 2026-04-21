"""Tests for the alerts API (#315 Phase 3)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.operators import AmbiguousOperatorError, NoOperatorError


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _install_conn(
    fetchone_returns: list[object] | None = None,
    fetchall_returns: list[object] | None = None,
    rowcount: int = 1,
) -> MagicMock:
    """Stub DB whose cursor feeds fetchone/fetchall in the order supplied."""
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    if fetchone_returns is not None:
        cur.fetchone.side_effect = list(fetchone_returns)
    if fetchall_returns is not None:
        cur.fetchall.return_value = fetchall_returns
    cur.rowcount = rowcount
    conn.cursor.return_value = cur
    conn.commit = MagicMock()

    def _dep() -> Iterator[MagicMock]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return cur


def test_get_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 503


def test_get_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch(
        "app.api.alerts.sole_operator_id",
        side_effect=AmbiguousOperatorError(),
    ):
        _install_conn()
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 501
