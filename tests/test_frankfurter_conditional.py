"""Unit tests for Frankfurter conditional fetch (#275).

Mocks httpx.Client at the module boundary — verifies 304 → returns
None, 200 → returns FrankfurterResult with parsed rates + ecb_date
+ ETag, and If-None-Match is forwarded when supplied.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.providers.implementations.frankfurter import (
    FrankfurterResult,
    fetch_latest_rates_conditional,
)


def _mock_httpx_response(status_code: int, json_body=None, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


def _patch_httpx(response):
    """Patch httpx.Client.__enter__ so the provider's context-manager
    gets back a client whose .get(...) returns our mocked response."""
    client = MagicMock()
    client.get.return_value = response
    ctx = MagicMock()
    ctx.__enter__.return_value = client
    ctx.__exit__.return_value = None
    return patch("app.providers.implementations.frankfurter.httpx.Client", return_value=ctx), client


class TestConditional304:
    def test_returns_none_on_304(self) -> None:
        resp = _mock_httpx_response(304)
        patcher, _ = _patch_httpx(resp)
        with patcher:
            out = fetch_latest_rates_conditional("USD", ["GBP", "EUR"], if_none_match='"some-etag"')
        assert out is None

    def test_forwards_if_none_match_header(self) -> None:
        resp = _mock_httpx_response(304)
        patcher, client = _patch_httpx(resp)
        with patcher:
            fetch_latest_rates_conditional("USD", ["GBP", "EUR"], if_none_match='"prior-etag"')
        _args, kwargs = client.get.call_args
        assert kwargs["headers"] == {"If-None-Match": '"prior-etag"'}

    def test_omits_header_when_no_prior_etag(self) -> None:
        """First-run case: no watermark yet, no If-None-Match sent."""
        resp = _mock_httpx_response(
            200,
            json_body={"date": "2026-04-17", "rates": {"GBP": 0.79}},
            headers={"ETag": '"new-etag"'},
        )
        patcher, client = _patch_httpx(resp)
        with patcher:
            fetch_latest_rates_conditional("USD", ["GBP"])
        _args, kwargs = client.get.call_args
        assert kwargs["headers"] == {}


class TestConditional200:
    def test_returns_parsed_result(self) -> None:
        resp = _mock_httpx_response(
            200,
            json_body={"date": "2026-04-17", "rates": {"GBP": 0.79, "EUR": 0.92}},
            headers={"ETag": '"new-etag"'},
        )
        patcher, _ = _patch_httpx(resp)
        with patcher:
            out = fetch_latest_rates_conditional("USD", ["GBP", "EUR"])

        assert isinstance(out, FrankfurterResult)
        assert out.rates == {("USD", "GBP"): Decimal("0.79"), ("USD", "EUR"): Decimal("0.92")}
        assert out.ecb_date == "2026-04-17"
        assert out.etag == '"new-etag"'

    def test_etag_none_when_header_absent(self) -> None:
        resp = _mock_httpx_response(
            200,
            json_body={"date": "2026-04-17", "rates": {"GBP": 0.79}},
            headers={},
        )
        patcher, _ = _patch_httpx(resp)
        with patcher:
            out = fetch_latest_rates_conditional("USD", ["GBP"])
        assert out is not None
        assert out.etag is None


class TestEmptyTargets:
    def test_empty_targets_returns_empty_result_without_http(self) -> None:
        """No network call when targets list is empty."""
        out = fetch_latest_rates_conditional("USD", [])
        assert out == FrankfurterResult(rates={}, ecb_date=None, etag=None)
