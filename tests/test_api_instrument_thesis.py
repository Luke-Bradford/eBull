"""Tests for POST /instruments/{symbol}/thesis (Phase 2.4 + #1919 force/provider).

Uses stub providers for the LLM + DB so no real LLM calls happen.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.theses import get_llm_client
from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    app.dependency_overrides.pop(get_llm_client, None)


def _install_llm_stub() -> MagicMock:
    stub = MagicMock()
    stub.provider_name = "openai_compatible"
    stub.model = "test-model"
    app.dependency_overrides[get_llm_client] = lambda: stub
    return stub


def _install_db(
    *,
    instrument_row: dict | None,
    cached_thesis: dict | None = None,
    fresh_thesis: dict | None = None,
) -> None:
    """Stub DB with three sequential fetchone calls:
    1. instrument lookup
    2. cache check (cached_thesis or None)
    3. fresh thesis re-read after generate_thesis (if reached)
    """

    def _conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.side_effect = [instrument_row, cached_thesis, fresh_thesis]
        conn_mock.cursor.return_value = cur_mock
        # commit is a no-op for the tests.
        conn_mock.commit = MagicMock()
        conn_mock.transaction = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _conn


def _clear_db() -> None:
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _thesis_row(**overrides) -> dict:
    base = {
        "thesis_id": 101,
        "instrument_id": 42,
        "thesis_version": 3,
        "thesis_type": "long",
        "stance": "buy",
        "confidence_score": 0.72,
        "buy_zone_low": 180.0,
        "buy_zone_high": 195.0,
        "base_value": 200.0,
        "bull_value": 230.0,
        "bear_value": 160.0,
        "break_conditions_json": ["sales growth < 5%", "margin contraction"],
        "memo_markdown": "# Bull case\nSolid moat.",
        "critic_json": {"counter": "sector headwinds"},
        "created_at": datetime.now(UTC) - timedelta(hours=2),
    }
    base.update(overrides)
    return base


def test_get_llm_client_maps_unconfigured_provider_to_503() -> None:
    """The dependency maps LLMProviderNotConfigured (anthropic provider
    configured without ANTHROPIC_API_KEY) to a fixed-string 503."""
    from fastapi import HTTPException

    from app.services.llm_client import LLMProviderNotConfigured

    conn = MagicMock()
    with (
        patch("app.api.theses.make_llm_client", side_effect=LLMProviderNotConfigured("no key")),
        pytest.raises(HTTPException) as exc_info,
    ):
        get_llm_client(conn)
    assert exc_info.value.status_code == 503
    assert "LLM provider not configured" in str(exc_info.value.detail)


def test_get_llm_client_maps_corrupt_config_to_503() -> None:
    from fastapi import HTTPException

    from app.services.runtime_config import RuntimeConfigCorrupt

    conn = MagicMock()
    with (
        patch("app.api.theses.make_llm_client", side_effect=RuntimeConfigCorrupt("row missing")),
        pytest.raises(HTTPException) as exc_info,
    ):
        get_llm_client(conn)
    assert exc_info.value.status_code == 503


def test_thesis_unknown_symbol_returns_404(client: TestClient) -> None:
    _install_llm_stub()
    _install_db(instrument_row=None)
    try:
        resp = client.post("/instruments/NOTREAL/thesis")
    finally:
        _clear_db()
    assert resp.status_code == 404


def test_thesis_empty_symbol_returns_400(client: TestClient) -> None:
    _install_llm_stub()
    _install_db(instrument_row=None)
    try:
        resp = client.post("/instruments/%20/thesis")
    finally:
        _clear_db()
    assert resp.status_code == 400


def test_thesis_cache_hit_returns_cached_without_llm(client: TestClient) -> None:
    """Within 24h of the last thesis, the endpoint returns the cached row
    without calling generate_thesis / the LLM."""
    _install_llm_stub()
    cached = _thesis_row()
    _install_db(
        instrument_row={"instrument_id": 42},
        cached_thesis=cached,
    )
    with patch("app.api.theses.generate_thesis") as gen_mock:
        try:
            resp = client.post("/instruments/AAPL/thesis")
        finally:
            _clear_db()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cached"] is True
    assert body["thesis"]["thesis_id"] == 101
    assert body["thesis"]["stance"] == "buy"
    gen_mock.assert_not_called()


def test_thesis_cache_miss_generates_and_returns_fresh(client: TestClient) -> None:
    """No thesis within the 24h window → generate_thesis is called, the
    fresh row is re-read and returned."""
    _install_llm_stub()
    fresh = _thesis_row(
        thesis_id=202,
        thesis_version=4,
        memo_markdown="# Fresh\nUpdated thesis.",
        created_at=datetime.now(UTC),
    )
    _install_db(
        instrument_row={"instrument_id": 42},
        cached_thesis=None,  # cache miss
        fresh_thesis=fresh,
    )
    with patch("app.api.theses.generate_thesis") as gen_mock:
        try:
            resp = client.post("/instruments/AAPL/thesis")
        finally:
            _clear_db()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cached"] is False
    assert body["thesis"]["thesis_id"] == 202
    assert body["thesis"]["memo_markdown"].startswith("# Fresh")
    gen_mock.assert_called_once()
    assert gen_mock.call_args.kwargs["trigger"] == "manual"


def test_thesis_force_bypasses_cache(client: TestClient) -> None:
    """?force=true (#1919) skips the 24h cache query entirely and
    regenerates. The DB stub therefore sees only two fetchone calls:
    symbol lookup, then the post-generation re-read — the `cached_thesis`
    slot below is consumed by the RE-READ, proving the cache query never
    ran (a cache hit on this row would have returned cached=true)."""
    _install_llm_stub()
    fresh = _thesis_row(thesis_id=303, created_at=datetime.now(UTC))
    _install_db(
        instrument_row={"instrument_id": 42},
        cached_thesis=fresh,
    )
    with patch("app.api.theses.generate_thesis") as gen_mock:
        try:
            resp = client.post("/instruments/AAPL/thesis?force=true")
        finally:
            _clear_db()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cached"] is False
    assert body["thesis"]["thesis_id"] == 303
    gen_mock.assert_called_once()
    assert gen_mock.call_args.kwargs["trigger"] == "manual"


def test_thesis_generate_failure_returns_502(client: TestClient) -> None:
    """When generate_thesis raises (LLM outage, DB transient, etc.)
    the endpoint surfaces 502 rather than letting the 500 propagate."""
    _install_llm_stub()
    _install_db(
        instrument_row={"instrument_id": 42},
        cached_thesis=None,
    )
    with patch(
        "app.api.theses.generate_thesis",
        side_effect=RuntimeError("llm is down"),
    ):
        try:
            resp = client.post("/instruments/AAPL/thesis")
        finally:
            _clear_db()

    assert resp.status_code == 502
    assert "thesis generation failed" in resp.json()["detail"]
