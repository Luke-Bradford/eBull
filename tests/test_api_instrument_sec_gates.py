"""Tests for the SEC-coverage gates on per-instrument API handlers (#503 PR 2).

The four handlers below previously returned data joined to
SEC-derived tables without checking that the instrument actually
has a current SEC CIK in ``external_identifiers``. After
migration 066 purged orphan rows, the API still needed an
explicit gate so a future bogus-CIK regression doesn't leak
again.

Each handler is exercised with two instrument shapes:

  * with_sec_cik=True  → 200 OK (or 404 only when the underlying
                          data fetch is itself empty)
  * with_sec_cik=False → 404 with detail "no SEC coverage"
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


def _conn_for_handler(*, has_cik: bool, instrument_id: int = 42, symbol: str = "AAPL") -> MagicMock:
    """Build a stub conn that replies to:

    1. The handler's instrument-resolve query (``WHERE UPPER(symbol) = …``)
       returning a single row.
    2. The ``_has_sec_cik`` query (returning a row when ``has_cik=True``,
       else ``None``).
    3. Any subsequent service-layer fetch (returns empty / None — we
       only care that the gate fires; the handler's body shouldn't run
       on the 404 path).
    """
    instrument_row = {"instrument_id": instrument_id, "symbol": symbol}
    cik_row: tuple[int] | None = (1,) if has_cik else None
    queue: list[object] = [
        instrument_row,  # resolve symbol
        cik_row,  # _has_sec_cik
    ]

    def _next_cursor(*_args: object, **_kwargs: object) -> MagicMock:
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None
        if queue:
            payload = queue.pop(0)
            cur.fetchone.return_value = payload
        else:
            cur.fetchone.return_value = None
        cur.fetchall.return_value = []
        return cur

    conn = MagicMock()
    conn.cursor.side_effect = _next_cursor
    return conn


def _install(conn: MagicMock) -> None:
    from app.db import get_conn

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override


def _clear() -> None:
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


@pytest.mark.parametrize(
    "endpoint",
    [
        "/instruments/AAPL/eight_k_filings",
        "/instruments/AAPL/dividends",
        "/instruments/AAPL/insider_summary",
        "/instruments/AAPL/insider_transactions",
    ],
)
def test_no_sec_cik_returns_404_no_sec_coverage(client: TestClient, endpoint: str) -> None:
    """The four newly-gated handlers must 404 (with the 'no SEC
    coverage' detail) when the instrument has no primary SEC CIK
    — so post-migration-066 a re-introduction of orphan rows
    can't leak through the API."""
    conn = _conn_for_handler(has_cik=False)
    _install(conn)
    try:
        resp = client.get(endpoint)
    finally:
        _clear()
    assert resp.status_code == 404
    assert "SEC coverage" in resp.json()["detail"]


@pytest.mark.parametrize(
    "endpoint",
    [
        "/instruments/AAPL/eight_k_filings",
        "/instruments/AAPL/dividends",
        "/instruments/AAPL/insider_summary",
        "/instruments/AAPL/insider_transactions",
    ],
)
def test_with_sec_cik_does_not_short_circuit_with_no_sec_coverage(client: TestClient, endpoint: str) -> None:
    """Companion: when the instrument DOES have a SEC CIK, the
    gate must NOT fire. Without this, a future regression that
    inverts the predicate (e.g. ``not _has_sec_cik`` swapped) or
    aliases the wrong flag (Codex round 1 finding on PR #506)
    would pass the negative test alone. The downstream service
    layer is not stubbed here — the response may still 5xx /
    return empty for other reasons — but the response MUST NOT
    carry the gate's "SEC coverage" 404 detail."""
    conn = _conn_for_handler(has_cik=True)
    _install(conn)
    try:
        resp = client.get(endpoint)
    finally:
        _clear()
    if resp.status_code == 404:
        assert "SEC coverage" not in resp.json().get("detail", "")
