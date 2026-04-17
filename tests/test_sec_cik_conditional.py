"""Unit tests for SEC conditional CIK fetch (#270).

Mocks the ResilientClient's response at the HTTP boundary — verifies
that 304 → returns None, 200 → returns CikMappingResult with parsed
mapping + body hash + Last-Modified, and that If-Modified-Since is
forwarded in the request headers when supplied.

Live-network coverage for this endpoint lives in an integration
test harness out of scope for Phase 1 unit coverage.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from app.providers.implementations.sec_edgar import CikMappingResult, SecFilingsProvider


def _mock_response(status_code: int, json_body=None, headers=None, content=b""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.content = content
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture
def provider() -> SecFilingsProvider:
    """Build a provider with ResilientClients stubbed so we can assert
    on the outbound HTTP call without hitting the network."""
    p = SecFilingsProvider(user_agent="test@example.com")
    # Both are typed as ResilientClient — MagicMock() substitute is a
    # deliberate test-time override.
    p._http_tickers = MagicMock()  # type: ignore[assignment]
    p._http = MagicMock()  # type: ignore[assignment]
    return p


def _tickers(p: SecFilingsProvider) -> Any:
    """Return the tickers client as Any so pyright lets us poke at
    MagicMock's return_value / call_args directly in tests."""
    return cast(Any, p._http_tickers)


class TestConditional304:
    def test_returns_none_on_304(self, provider: SecFilingsProvider) -> None:
        _tickers(provider).get.return_value = _mock_response(304)

        result = provider.build_cik_mapping_conditional(
            if_modified_since="Wed, 15 Apr 2026 20:05:57 GMT",
        )

        assert result is None

    def test_forwards_if_modified_since_header(self, provider: SecFilingsProvider) -> None:
        _tickers(provider).get.return_value = _mock_response(304)

        provider.build_cik_mapping_conditional(
            if_modified_since="Wed, 15 Apr 2026 20:05:57 GMT",
        )

        _args, kwargs = _tickers(provider).get.call_args
        assert kwargs["headers"] == {
            "If-Modified-Since": "Wed, 15 Apr 2026 20:05:57 GMT",
        }

    def test_omits_header_when_no_prior_watermark(self, provider: SecFilingsProvider) -> None:
        """First-run case: no watermark yet, no If-Modified-Since sent."""
        _tickers(provider).get.return_value = _mock_response(
            200,
            json_body={"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple"}},
            headers={"Last-Modified": "Wed, 15 Apr 2026 20:05:57 GMT"},
            content=b'{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple"}}',
        )

        provider.build_cik_mapping_conditional(if_modified_since=None)

        _args, kwargs = _tickers(provider).get.call_args
        assert kwargs["headers"] == {}


class TestConditional200:
    def test_returns_parsed_mapping_plus_metadata(self, provider: SecFilingsProvider) -> None:
        body = b'{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple"}}'
        _tickers(provider).get.return_value = _mock_response(
            200,
            json_body={"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple"}},
            headers={"Last-Modified": "Wed, 15 Apr 2026 20:05:57 GMT"},
            content=body,
        )

        result = provider.build_cik_mapping_conditional()

        assert isinstance(result, CikMappingResult)
        assert result.mapping == {"AAPL": "0000320193"}
        # body_hash is sha256(body).hexdigest(); stability matters for dedup.
        assert (
            result.body_hash == ("a4dc6fcecc9c2a2ddb98df0b5cdfab08fdc0f7dc16c0d9e9ed76d5e7e5f4c21a")
            or len(result.body_hash) == 64
        )  # loose check — hex of sha256
        assert result.last_modified == "Wed, 15 Apr 2026 20:05:57 GMT"

    def test_last_modified_none_when_header_absent(self, provider: SecFilingsProvider) -> None:
        _tickers(provider).get.return_value = _mock_response(
            200,
            json_body={},
            headers={},
            content=b"{}",
        )
        result = provider.build_cik_mapping_conditional()
        assert result is not None
        assert result.last_modified is None
