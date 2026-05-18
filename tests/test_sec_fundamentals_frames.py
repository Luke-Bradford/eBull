"""Tests for ``SecFundamentalsProvider.fetch_frame`` — G11 (2026-05-18,
US-ETL completion plan §2 Phase 4 PR 8).

The frames primitive lands as a thin HTTP wrapper over
``https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json``.
No production consumer in v1 — these tests pin the public surface
contract so future sector-aggregate / cross-sectional consumer tickets
(latent: #594 peer-comparison radar + sector heatmap) can wire it
without re-discovering edge cases.

Spec:
  docs/superpowers/specs/2026-05-18-g11-frames-api-consumer.md
Plan:
  docs/superpowers/plans/2026-05-18-g11-frames-api-consumer-plan.md
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from app.providers.implementations import sec_edgar
from app.providers.implementations.sec_fundamentals import (
    _MIN_REQUEST_INTERVAL_S,
    SecFundamentalsProvider,
)
from app.providers.resilient_client import ResilientClient

_UA = "ebull-test/0.0 test@example.com"
_BASE = "https://data.sec.gov"


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _make_provider() -> SecFundamentalsProvider:
    return SecFundamentalsProvider(user_agent=_UA)


def _rewire_transport(
    provider: SecFundamentalsProvider,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    preserve_throttle: bool = True,
    max_retries: int = 0,
) -> None:
    """Swap the provider's `_client` + `_http` for a MockTransport pair.

    Mirrors the helper in ``tests/test_sec_fundamentals_companyconcept.py``
    — duplicated here intentionally per plan §1 (two test files, no
    shared helper module yet; revisit when a third lands).
    """
    provider._client.close()  # noqa: SLF001
    provider._client = httpx.Client(  # noqa: SLF001
        base_url=_BASE,
        headers={"User-Agent": _UA, "Accept": "application/json"},
        transport=httpx.MockTransport(handler),
    )
    if preserve_throttle:
        provider._http = ResilientClient(  # noqa: SLF001
            provider._client,  # noqa: SLF001
            min_request_interval_s=_MIN_REQUEST_INTERVAL_S,
            shared_last_request=sec_edgar._PROCESS_RATE_LIMIT_CLOCK,
            shared_throttle_lock=sec_edgar._PROCESS_RATE_LIMIT_LOCK,
            max_retries=max_retries,
        )
    else:
        provider._http = ResilientClient(  # noqa: SLF001
            provider._client,  # noqa: SLF001
            min_request_interval_s=0.0,
            max_retries=max_retries,
        )


def _make_frames_payload(
    *,
    taxonomy: str = "us-gaap",
    tag: str = "Revenues",
    ccp: str = "CY2024",
    uom: str = "USD",
    data: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "taxonomy": taxonomy,
        "tag": tag,
        "ccp": ccp,
        "uom": uom,
        "label": tag,
        "description": "",
        "pts": len(data) if data else 0,
        "data": data or [],
    }


# ----------------------------------------------------------------------
# Test 1 — URL builder
# ----------------------------------------------------------------------


def test_fetch_frame_url() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            json=_make_frames_payload(tag="Assets", ccp="CY2024Q1I", uom="USD"),
        )

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_frame("us-gaap", "Assets", "USD", "CY2024Q1I")

    assert len(captured) == 1
    assert captured[0].url.host == "data.sec.gov"
    assert captured[0].url.path == "/api/xbrl/frames/us-gaap/Assets/USD/CY2024Q1I.json"


# ----------------------------------------------------------------------
# Test 2 — 404 returns None
# ----------------------------------------------------------------------


def test_fetch_frame_404_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    assert provider.fetch_frame("us-gaap", "Revenues", "USD", "CY2024") is None


# ----------------------------------------------------------------------
# Test 3 — 5xx raises (request= attached, max_retries=0)
# ----------------------------------------------------------------------


def test_fetch_frame_5xx_raises() -> None:
    target = "/api/xbrl/frames/us-gaap/Revenues/USD/CY2024.json"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            request=httpx.Request("GET", f"{_BASE}{target}"),
        )

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    with pytest.raises(httpx.HTTPStatusError):
        provider.fetch_frame("us-gaap", "Revenues", "USD", "CY2024")


# ----------------------------------------------------------------------
# Test 4 — taxonomy validation (G10 literals duplicated verbatim)
# ----------------------------------------------------------------------


_BAD_TAXONOMY = [
    "Us-Gaap",
    "",
    "us gaap",
    "us/gaap",
    "-bad",
    "us-gaap\n",
    "us-gaap ",
    "\nus-gaap",
    "us-gaap-",
    "-",
]

_GOOD_TAXONOMY = [
    "us-gaap",
    "dei",
    "srt",
    "ifrs-full",
    "invest",
    "country",
]


@pytest.mark.parametrize("taxonomy", _BAD_TAXONOMY)
def test_fetch_frame_rejects_malformed_taxonomy(taxonomy: str) -> None:
    provider = _make_provider()
    _rewire_transport(
        provider,
        lambda r: httpx.Response(200, json=_make_frames_payload()),
        preserve_throttle=False,
    )

    with pytest.raises(ValueError, match="invalid taxonomy"):
        provider.fetch_frame(taxonomy, "Revenues", "USD", "CY2024")


@pytest.mark.parametrize("taxonomy", _GOOD_TAXONOMY)
def test_fetch_frame_accepts_legitimate_taxonomy(taxonomy: str) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_make_frames_payload(taxonomy=taxonomy))

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_frame(taxonomy, "Revenues", "USD", "CY2024")

    assert len(captured) == 1
    assert f"/{taxonomy}/Revenues/" in captured[0].url.path


# ----------------------------------------------------------------------
# Test 5 — tag validation (G10 literals duplicated verbatim)
# ----------------------------------------------------------------------


_BAD_TAG = [
    "Revenues/Q1",
    "Revenues ",
    "",
    "123Revenues",
    "Revenu€s",
    "Revenues\n",
]

_GOOD_TAG = [
    "Revenues",
    "EntityCommonStockSharesOutstanding",
    "my_custom_concept",
]


@pytest.mark.parametrize("tag", _BAD_TAG)
def test_fetch_frame_rejects_malformed_tag(tag: str) -> None:
    provider = _make_provider()
    _rewire_transport(
        provider,
        lambda r: httpx.Response(200, json=_make_frames_payload()),
        preserve_throttle=False,
    )

    with pytest.raises(ValueError, match="invalid tag"):
        provider.fetch_frame("us-gaap", tag, "USD", "CY2024")


@pytest.mark.parametrize("tag", _GOOD_TAG)
def test_fetch_frame_accepts_legitimate_tag(tag: str) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_make_frames_payload(tag=tag))

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_frame("us-gaap", tag, "USD", "CY2024")

    assert len(captured) == 1
    assert f"/{tag}/USD/" in captured[0].url.path


# ----------------------------------------------------------------------
# Test 6 — unit validation (token-per-token grammar)
# ----------------------------------------------------------------------


_BAD_UNIT = [
    "",
    "USD ",  # trailing space
    "US D",  # interior space
    "USD\n",  # trailing newline
    "123USD",  # leading digit
    "USD€",  # non-ASCII
    "USD/shares",  # slash — SEC uses -per- not /
    "/USD",  # leading slash
    "USD-per-",  # trailing dash; empty denominator
    "USD-per",  # bare -per; missing denominator
    "USD--per-shares",  # double dash; empty interior token
]

_GOOD_UNIT = [
    "USD",
    "USD-per-shares",
    "shares",
    "pure",
    "GBP",
    "EUR",
    "Y",
    "Y-per-shares",
    "usd",  # lowercase admitted — primitive is general
]


@pytest.mark.parametrize("unit", _BAD_UNIT)
def test_fetch_frame_rejects_malformed_unit(unit: str) -> None:
    provider = _make_provider()
    _rewire_transport(
        provider,
        lambda r: httpx.Response(200, json=_make_frames_payload()),
        preserve_throttle=False,
    )

    with pytest.raises(ValueError, match="invalid unit"):
        provider.fetch_frame("us-gaap", "Revenues", unit, "CY2024")


@pytest.mark.parametrize("unit", _GOOD_UNIT)
def test_fetch_frame_accepts_legitimate_unit(unit: str) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_make_frames_payload(uom=unit))

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_frame("us-gaap", "Revenues", unit, "CY2024")

    assert len(captured) == 1
    assert f"/Revenues/{unit}/" in captured[0].url.path


# ----------------------------------------------------------------------
# Test 7 — period validation
# ----------------------------------------------------------------------


_BAD_PERIOD = [
    "cy2024",  # lowercase
    "CY24",  # 2-digit year
    "CY2024Q5",  # Q5 invalid
    "CY2024Q0",  # Q0 invalid
    "CY2024Q1A",  # A not I
    "CY2024Q1I ",  # trailing space
    "CY2024Q1I\n",  # newline
    "",
    "FY2024",  # FY prefix
    "CY2024I",  # annual-instantaneous not a valid frame per SEC docs
]

_GOOD_PERIOD = [
    "CY2024",
    "CY2024Q1",
    "CY2024Q4",
    "CY2024Q1I",
    "CY2024Q4I",
]


@pytest.mark.parametrize("period", _BAD_PERIOD)
def test_fetch_frame_rejects_malformed_period(period: str) -> None:
    provider = _make_provider()
    _rewire_transport(
        provider,
        lambda r: httpx.Response(200, json=_make_frames_payload()),
        preserve_throttle=False,
    )

    with pytest.raises(ValueError, match="invalid period"):
        provider.fetch_frame("us-gaap", "Revenues", "USD", period)


@pytest.mark.parametrize("period", _GOOD_PERIOD)
def test_fetch_frame_accepts_legitimate_period(period: str) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_make_frames_payload(ccp=period))

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_frame("us-gaap", "Revenues", "USD", period)

    assert len(captured) == 1
    assert captured[0].url.path.endswith(f"/{period}.json")


# ----------------------------------------------------------------------
# Test 8 — payload parsing
# ----------------------------------------------------------------------


def test_fetch_frame_returns_parsed_payload() -> None:
    fixture = _make_frames_payload(
        taxonomy="us-gaap",
        tag="Revenues",
        ccp="CY2024",
        uom="USD",
        data=[
            {
                "accn": "0000320193-24-000123",
                "cik": 320193,
                "entityName": "Apple Inc.",
                "loc": "US-CA",
                "end": "2024-09-28",
                "val": 391035000000,
                "fy": 2024,
                "fp": "FY",
                "form": "10-K",
                "filed": "2024-11-01",
            }
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=fixture)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    result = provider.fetch_frame("us-gaap", "Revenues", "USD", "CY2024")

    assert result is not None
    assert result["taxonomy"] == "us-gaap"
    assert result["tag"] == "Revenues"
    assert result["ccp"] == "CY2024"
    assert result["uom"] == "USD"
    assert result["pts"] == 1
    assert len(result["data"]) == 1
    assert result["data"][0]["cik"] == 320193
    assert result["data"][0]["val"] == 391035000000
    assert result["data"][0]["entityName"] == "Apple Inc."


# ----------------------------------------------------------------------
# Test 9 — rate-limit clock sharing
# ----------------------------------------------------------------------


def test_fetch_frame_shares_rate_limit_clock_identity() -> None:
    provider = _make_provider()
    try:
        assert (
            provider._http._last_request_at  # noqa: SLF001
            is sec_edgar._PROCESS_RATE_LIMIT_CLOCK  # noqa: SLF001
        )
        assert (
            provider._http._throttle_lock  # noqa: SLF001
            is sec_edgar._PROCESS_RATE_LIMIT_LOCK  # noqa: SLF001
        )
    finally:
        provider._client.close()  # noqa: SLF001


def test_fetch_frame_throttles_back_to_back_calls() -> None:
    """Back-to-back fetch_frame calls observe delta ≥
    _MIN_REQUEST_INTERVAL_S via the shared rate-limit clock.
    """
    call_times: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_times.append(time.monotonic())
        return httpx.Response(200, json=_make_frames_payload())

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=True)

    try:
        sec_edgar._PROCESS_RATE_LIMIT_CLOCK[0] = 0.0  # noqa: SLF001
        provider.fetch_frame("us-gaap", "Revenues", "USD", "CY2024")
        provider.fetch_frame("us-gaap", "NetIncomeLoss", "USD", "CY2024")

        assert len(call_times) == 2
        delta = call_times[1] - call_times[0]
        assert delta >= _MIN_REQUEST_INTERVAL_S, (
            f"expected delta >= {_MIN_REQUEST_INTERVAL_S:.3f}s (got {delta:.3f}s) — rate-limit clock not engaged"
        )
    finally:
        provider._client.close()  # noqa: SLF001
        sec_edgar._PROCESS_RATE_LIMIT_CLOCK[0] = 0.0  # noqa: SLF001


# Silence unused-import warning — keep `logging` for symmetry with G10.
_ = logging
