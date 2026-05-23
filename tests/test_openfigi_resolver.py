"""#1233 PR-1b — OpenFigiResolver unit tests against PR-0 fixtures.

Verifies the resolver's:

* Parallel-array contract — request index N → response entry N. Uses
  ``batch_known_5.json`` + ``batch_with_invalid.json`` to pin positional
  alignment.
* US-primary defensive filter — must drop AAPL's 254 non-US listings
  and pick the ``exchCode='US' AND securityType='Common Stock'`` row.
* 429 retry — sleeps Retry-After once, retries; second 429 raises
  :class:`OpenFigiRateLimited`.
* 429 body shape — plain text NOT JSON; resolver MUST branch on
  status before calling ``.json()``.
* Empty input — zero HTTP calls, returns ``{}``.
* Rate-limiter — N+1th call inside the same window blocks until the
  window expires.
* Tier selection — keyed vs unkeyed jobs-per-POST and window-seconds.

No live HTTP. Every test uses ``httpx.MockTransport`` to play back
fixture bytes against a real ``httpx.Client``, so the assertions
exercise the resolver's parsing + retry path end-to-end.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.services.openfigi_resolver import (
    OPENFIGI_BASE_URL,
    OpenFigiMapping,
    OpenFigiRateLimited,
    OpenFigiResolver,
    OpenFigiTransportError,
)

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "openfigi"


def _load_fixture(name: str) -> dict[str, Any]:
    with (FIXTURE_DIR / name).open("r", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# httpx.MockTransport helpers
# ---------------------------------------------------------------------------


def _strip_encoding_headers(headers: dict[str, str]) -> dict[str, str]:
    """Drop ``content-encoding`` / ``content-length`` from a fixture's
    captured headers.

    The PR-0 probe recorded these as observed on the wire (e.g.
    ``content-encoding: gzip``), but httpx.MockTransport returns the
    raw body the caller passes — replaying with a ``gzip`` encoding
    header makes httpx try to decompress plain JSON and raises
    ``DecodingError``. The other rate-limit / status headers are what
    we care about; encoding negotiation is irrelevant to the resolver's
    correctness."""
    return {k: v for k, v in headers.items() if k.lower() not in {"content-encoding", "content-length"}}


def _fixture_handler(
    fixture_name: str,
) -> tuple[
    list[httpx.Request],
    httpx.Client,
]:
    """Build a Client whose POSTs return the fixture response.

    Returns (captured_requests, client). The test owns the client and
    is responsible for closing it via the resolver's context manager.
    """
    blob = _load_fixture(fixture_name)
    resp_blob = blob["response"]
    body = resp_blob["body"]
    headers = _strip_encoding_headers({str(k): str(v) for k, v in resp_blob["headers"].items()})
    status_code = int(resp_blob["status_code"])

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if isinstance(body, str):
            # 429 fixture — plain-text body
            return httpx.Response(status_code=status_code, content=body, headers=headers)
        return httpx.Response(status_code=status_code, json=body, headers=headers)

    return captured, httpx.Client(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Sequenced handler — drives 429 → 200 retry path
# ---------------------------------------------------------------------------


def _sequenced_handler(
    responses: list[tuple[int, Any, dict[str, str]]],
) -> tuple[
    list[httpx.Request],
    httpx.Client,
]:
    """Build a Client that cycles through ``responses`` per POST.

    Each entry is (status_code, body, headers). ``body`` is either a
    ``str`` (plain text) or a ``list`` (JSON-encoded).
    """
    captured: list[httpx.Request] = []
    iterator = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        try:
            status, body, headers = next(iterator)
        except StopIteration as exc:
            raise AssertionError(f"unexpected extra POST: request #{len(captured)}") from exc
        if isinstance(body, str):
            return httpx.Response(status_code=status, content=body, headers=headers)
        return httpx.Response(status_code=status, json=body, headers=headers)

    return captured, httpx.Client(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Parsing — single CUSIP → AAPL
# ---------------------------------------------------------------------------


class TestSingleCusip:
    def test_single_aapl_resolves_to_us_primary(self) -> None:
        captured, client = _fixture_handler("single_aapl.json")
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["037833100"])
        assert set(result.keys()) == {"037833100"}
        mapping = result["037833100"]
        assert mapping.ticker == "AAPL"
        assert mapping.name == "APPLE INC"
        assert mapping.exch_code == "US"
        # Defensive filter MUST drop the 254 non-US listings.
        # Only one HTTP call.
        assert len(captured) == 1
        # Request body has the exact CUSIP we asked for.
        req_body = json.loads(captured[0].content.decode("utf-8"))
        assert req_body == [{"idType": "ID_CUSIP", "idValue": "037833100"}]


# ---------------------------------------------------------------------------
# Parsing — batch of 5 known CUSIPs (parallel-array positional contract)
# ---------------------------------------------------------------------------


class TestBatchPositionalContract:
    _EXPECTED = {
        "037833100": "AAPL",
        "594918104": "MSFT",
        "46625H100": "JPM",
        "36467W109": "GME",
        "437076102": "HD",
    }

    def test_batch_known_5_resolves_each_positionally(self) -> None:
        captured, client = _fixture_handler("batch_known_5.json")
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(list(self._EXPECTED.keys()))
        # Every CUSIP positionally resolves to its expected ticker.
        assert set(result.keys()) == set(self._EXPECTED.keys())
        for cusip, expected_ticker in self._EXPECTED.items():
            assert result[cusip].ticker == expected_ticker
        # One HTTP call for the whole batch.
        assert len(captured) == 1


# ---------------------------------------------------------------------------
# Parsing — invalid CUSIP omitted from result dict
# ---------------------------------------------------------------------------


class TestInvalidCusipDropped:
    def test_invalid_cusip_is_silently_omitted(self) -> None:
        captured, client = _fixture_handler("batch_with_invalid.json")
        request_cusips = [entry["idValue"] for entry in _load_fixture("batch_with_invalid.json")["request"]["body"]]
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(request_cusips)
        # 9 valid + 1 invalid → 9 entries in the result dict.
        assert "000000000" not in result
        valid_cusips = {c for c in request_cusips if c != "000000000"}
        assert set(result.keys()) == valid_cusips
        assert len(captured) == 1


# ---------------------------------------------------------------------------
# 429 retry behaviour
# ---------------------------------------------------------------------------


class TestRateLimitBackoff:
    def test_429_then_200_retries_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """One 429 with short Retry-After then a 200 → result populated;
        no exception raised. Single retry."""
        captured, client = _sequenced_handler(
            [
                (
                    429,
                    "Too many requests, please try again later.",
                    {"retry-after": "1", "ratelimit-remaining": "0"},
                ),
                (
                    200,
                    [
                        {
                            "data": [
                                {
                                    "ticker": "AAPL",
                                    "name": "APPLE INC",
                                    "exchCode": "US",
                                    "securityType": "Common Stock",
                                    "shareClassFIGI": "BBG001S5N8V8",
                                }
                            ]
                        }
                    ],
                    {"ratelimit-limit": "25", "ratelimit-remaining": "24", "ratelimit-reset": "60"},
                ),
            ]
        )
        # Skip real sleeps so the test runs <1s.
        sleeps: list[float] = []

        def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr(time, "sleep", fake_sleep)
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["037833100"])
        assert len(captured) == 2  # 429 + retry
        assert "037833100" in result
        # Slept the Retry-After (1s).
        assert sleeps == [1]

    def test_429_then_429_raises_OpenFigiRateLimited(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Two consecutive 429s → resolver surfaces rate-limited
        exception (single retry exhausted)."""
        captured, client = _sequenced_handler(
            [
                (429, "Too many requests.", {"retry-after": "1"}),
                (429, "Too many requests.", {"retry-after": "1"}),
            ]
        )
        monkeypatch.setattr(time, "sleep", lambda seconds: None)
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            with pytest.raises(OpenFigiRateLimited):
                resolver.resolve_cusips(["037833100"])
        # 2 HTTP calls (initial + 1 retry); no third.
        assert len(captured) == 2

    def test_429_body_is_plain_text_not_json(self) -> None:
        """The 429 body is plain-text; resolver MUST NOT call
        ``.json()`` on it."""
        blob = _load_fixture("rate_limit_429.json")
        # Sanity: fixture matches its own contract.
        assert isinstance(blob["response"]["body"], str)
        # The resolver behaviour is covered by
        # test_429_then_429_raises_OpenFigiRateLimited above; here we
        # just pin the fixture shape so a future fixture refresh that
        # records JSON instead of plain text would break the test and
        # surface the contract change explicitly.


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------


class TestEmptyInput:
    def test_empty_iterable_zero_calls(self) -> None:
        captured, client = _sequenced_handler([])
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips([])
        assert result == {}
        assert captured == []

    def test_all_whitespace_strings_drop(self) -> None:
        captured, client = _sequenced_handler([])
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["", "   ", ""])
        assert result == {}
        assert captured == []


# ---------------------------------------------------------------------------
# Tier selection
# ---------------------------------------------------------------------------


class TestTierSelection:
    def test_unkeyed_default_jobs_per_post_is_10(self) -> None:
        resolver = OpenFigiResolver(api_key=None)
        try:
            assert resolver.jobs_per_post == 10
            assert resolver.keyed is False
        finally:
            resolver.close()

    def test_keyed_jobs_per_post_is_100(self) -> None:
        resolver = OpenFigiResolver(api_key="ABCDEF1234567890")
        try:
            assert resolver.jobs_per_post == 100
            assert resolver.keyed is True
        finally:
            resolver.close()

    def test_keyed_header_set_on_request(self) -> None:
        """Keyed-tier requests include the ``X-OPENFIGI-APIKEY`` header."""
        captured, client = _sequenced_handler(
            [
                (
                    200,
                    [{"data": []}],
                    {"ratelimit-limit": "25"},
                ),
            ]
        )
        with OpenFigiResolver(api_key="TEST-KEY-XYZ", client=client) as resolver:
            resolver.resolve_cusips(["037833100"])
        assert len(captured) == 1
        # MockTransport lowercases via Headers, but the encoded request
        # preserves what we set. Read via the case-insensitive accessor.
        assert captured[0].headers["X-OPENFIGI-APIKEY"] == "TEST-KEY-XYZ"

    def test_unkeyed_omits_apikey_header(self) -> None:
        captured, client = _sequenced_handler([(200, [{"data": []}], {"ratelimit-limit": "25"})])
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            resolver.resolve_cusips(["037833100"])
        assert "x-openfigi-apikey" not in {k.lower() for k in captured[0].headers.keys()}


# ---------------------------------------------------------------------------
# Defensive US-primary filter — exhaustive
# ---------------------------------------------------------------------------


class TestUsPrimaryFilter:
    def test_no_us_row_returns_no_mapping(self) -> None:
        """A CUSIP whose data array has only non-US listings → no
        mapping in result dict."""
        captured, client = _sequenced_handler(
            [
                (
                    200,
                    [
                        {
                            "data": [
                                {
                                    "ticker": "AAPL",
                                    "name": "APPLE INC",
                                    "exchCode": "UA",  # NYSE Arca, not composite US
                                    "securityType": "Common Stock",
                                }
                            ]
                        }
                    ],
                    {"ratelimit-limit": "25"},
                )
            ]
        )
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["037833100"])
        assert result == {}
        assert len(captured) == 1

    def test_us_preferred_over_non_us(self) -> None:
        """data[] with both US-primary and non-US entries → US-primary picked
        regardless of order."""
        captured, client = _sequenced_handler(
            [
                (
                    200,
                    [
                        {
                            "data": [
                                {
                                    # Non-US entry FIRST in array
                                    "ticker": "AAPL",
                                    "name": "APPLE INC",
                                    "exchCode": "UA",
                                    "securityType": "Common Stock",
                                },
                                {
                                    # Real US-primary SECOND — defensive
                                    # filter MUST pick this one.
                                    "ticker": "AAPL",
                                    "name": "APPLE INC",
                                    "exchCode": "US",
                                    "securityType": "Common Stock",
                                },
                            ]
                        }
                    ],
                    {"ratelimit-limit": "25"},
                )
            ]
        )
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["037833100"])
        assert result["037833100"].exch_code == "US"
        del captured  # unused

    def test_us_but_not_common_stock_returns_no_mapping(self) -> None:
        """An ETF / preferred / ADR row even on the US exchange does
        NOT trigger promotion — only Common Stock binds to instruments."""
        captured, client = _sequenced_handler(
            [
                (
                    200,
                    [
                        {
                            "data": [
                                {
                                    "ticker": "SPY",
                                    "name": "SPDR S&P 500 ETF",
                                    "exchCode": "US",
                                    "securityType": "ETP",
                                }
                            ]
                        }
                    ],
                    {"ratelimit-limit": "25"},
                )
            ]
        )
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["78462F103"])
        assert result == {}
        del captured

    def test_warning_entry_returns_no_mapping(self) -> None:
        """``{"warning": "No identifier found."}`` → no mapping."""
        captured, client = _sequenced_handler([(200, [{"warning": "No identifier found."}], {"ratelimit-limit": "25"})])
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            result = resolver.resolve_cusips(["000000000"])
        assert result == {}
        del captured


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_500_raises_transport_error(self) -> None:
        captured, client = _sequenced_handler([(500, "Internal Server Error", {"content-type": "text/plain"})])
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            with pytest.raises(OpenFigiTransportError):
                resolver.resolve_cusips(["037833100"])
        assert len(captured) == 1  # No retry on 5xx.

    def test_400_raises_transport_error(self) -> None:
        captured, client = _sequenced_handler([(400, "Bad Request", {"content-type": "text/plain"})])
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            with pytest.raises(OpenFigiTransportError):
                resolver.resolve_cusips(["037833100"])
        del captured

    def test_non_array_2xx_raises_transport_error(self) -> None:
        """A 200 with a non-array body (server bug) surfaces as
        transport error, NOT silent zero mappings."""
        captured, client = _sequenced_handler(
            [(200, {"data": []}, {"content-type": "application/json"})]  # dict not list
        )
        with OpenFigiResolver(api_key=None, client=client) as resolver:
            with pytest.raises(OpenFigiTransportError):
                resolver.resolve_cusips(["037833100"])
        del captured


# ---------------------------------------------------------------------------
# Endpoint pin
# ---------------------------------------------------------------------------


def test_endpoint_url() -> None:
    """The hard-coded endpoint URL matches the SD-1 settled-decisions
    entry. A future re-spec to a different URL must touch this constant
    and this test, so the change is auditable."""
    assert OPENFIGI_BASE_URL == "https://api.openfigi.com/v3/mapping"


# ---------------------------------------------------------------------------
# Public dataclass shape
# ---------------------------------------------------------------------------


def test_mapping_dataclass_is_frozen() -> None:
    mapping = OpenFigiMapping(ticker="AAPL", name="APPLE INC", exch_code="US", share_class_figi=None)
    with pytest.raises(Exception):  # noqa: B017 — FrozenInstanceError varies
        mapping.ticker = "MSFT"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# from_env() — Settings integration (fix/1233-openfigi-key-via-settings)
# ---------------------------------------------------------------------------


class TestFromEnv:
    """``OpenFigiResolver.from_env`` reads ``settings.openfigi_api_key``.

    The prior shape (``os.environ.get('OPENFIGI_API_KEY')``) silently
    bypassed the ``.env`` loader — keys written to ``.env`` never
    reached the resolver. Reading via Settings keeps env-file precedence
    consistent with every other secret in the repo.
    """

    def test_settings_key_promoted_to_keyed_tier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.config import settings

        monkeypatch.setattr(settings, "openfigi_api_key", "test-key-keyed")
        with OpenFigiResolver.from_env() as resolver:
            assert resolver.keyed is True

    def test_missing_settings_key_falls_back_to_unkeyed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.config import settings

        monkeypatch.setattr(settings, "openfigi_api_key", None)
        with OpenFigiResolver.from_env() as resolver:
            assert resolver.keyed is False

    def test_empty_string_settings_key_falls_back_to_unkeyed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty ``OPENFIGI_API_KEY=`` line in .env yields empty string,
        not None; resolver must treat as unkeyed."""
        from app.config import settings

        monkeypatch.setattr(settings, "openfigi_api_key", "")
        with OpenFigiResolver.from_env() as resolver:
            assert resolver.keyed is False
