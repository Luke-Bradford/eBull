"""Tests for ``SecFundamentalsProvider.fetch_concept`` +
``extract_concept_facts`` — G10 (2026-05-17, US-ETL completion plan
§2 Phase 4 PR 7).

The companyconcept primitive lands as a thin HTTP wrapper over
``https://data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json``.
There is no production consumer in v1 — these tests pin the public
surface contract so future single-tag refresh paths (e.g. #435
dilution-tracker per-CIK shares-outstanding topup) can wire it
without re-discovering edge cases.

Spec:
  docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md
Plan:
  docs/superpowers/plans/2026-05-17-g10-companyconcept-api-consumer-plan.md
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from decimal import Decimal
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

    `preserve_throttle=True` (default) — re-uses the shared
    ``_PROCESS_RATE_LIMIT_CLOCK`` + ``_PROCESS_RATE_LIMIT_LOCK`` so
    the rate-limit gate continues to engage end-to-end (used by
    test 10 sub-test 2).

    `preserve_throttle=False` — sets ``min_request_interval_s=0.0``
    so unit tests do not block on the rate-limit gate (used by tests
    1-9 that exercise the HTTP contract).

    `max_retries=0` (default) — disables ``ResilientClient`` retry/
    backoff so a stubbed 5xx fixture fires once and propagates
    cleanly (Codex 2 r1 LOW-3 ownership; without this, the 5xx
    test sleeps ~7 s through the default exponential backoff
    schedule).
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


def _make_payload(
    *,
    tag: str,
    taxonomy: str = "us-gaap",
    entries: list[dict[str, Any]] | None = None,
    units: dict[str, Any] | None = None,
    include_units: bool = True,
) -> dict[str, Any]:
    """Construct a companyconcept-shaped response payload.

    Use `units=` to inject custom unit structure (e.g. USD/shares for
    the float-boundary test). Use `entries=` for the common
    single-unit USD case. `include_units=False` omits the key
    entirely (covers test 9 missing-units branch).
    """
    body: dict[str, Any] = {
        "cik": 320193,
        "taxonomy": taxonomy,
        "tag": tag,
        "label": tag,
        "description": "",
        "entityName": "Apple Inc.",
    }
    if not include_units:
        return body
    if units is not None:
        body["units"] = units
    else:
        body["units"] = {"USD": entries or []}
    return body


# ----------------------------------------------------------------------
# Test 1 — URL builder zero-pads CIK
# ----------------------------------------------------------------------


def test_fetch_concept_url_zero_pads_cik() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_make_payload(tag="Revenues", entries=[]))

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_concept("320193", "us-gaap", "Revenues")

    assert len(captured) == 1
    assert captured[0].url.host == "data.sec.gov"
    assert captured[0].url.path == "/api/xbrl/companyconcept/CIK0000320193/us-gaap/Revenues.json"


# ----------------------------------------------------------------------
# Test 2 — 404 returns None
# ----------------------------------------------------------------------


def test_fetch_concept_404_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    assert provider.fetch_concept("320193", "us-gaap", "Revenues") is None


# ----------------------------------------------------------------------
# Test 3 — 5xx raises (with Request attached so raise_for_status fires
# cleanly per Codex 1b r1 MED-3)
# ----------------------------------------------------------------------


def test_fetch_concept_5xx_raises() -> None:
    target_path = "/api/xbrl/companyconcept/CIK0000320193/us-gaap/Revenues.json"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            request=httpx.Request("GET", f"{_BASE}{target_path}"),
        )

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    with pytest.raises(httpx.HTTPStatusError):
        provider.fetch_concept("320193", "us-gaap", "Revenues")


# ----------------------------------------------------------------------
# Test 4 — taxonomy validation
# ----------------------------------------------------------------------


_BAD_TAXONOMY = [
    "Us-Gaap",  # uppercase
    "",  # empty
    "us gaap",  # space
    "us/gaap",  # slash
    "-bad",  # leading dash
    "us-gaap\n",  # trailing newline — fullmatch discipline
    "us-gaap ",  # trailing space
    "\nus-gaap",  # leading newline
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
def test_fetch_concept_rejects_malformed_taxonomy(taxonomy: str) -> None:
    provider = _make_provider()
    _rewire_transport(
        provider,
        lambda r: httpx.Response(200, json=_make_payload(tag="Revenues", entries=[])),
        preserve_throttle=False,
    )

    with pytest.raises(ValueError, match="invalid taxonomy"):
        provider.fetch_concept("320193", taxonomy, "Revenues")


@pytest.mark.parametrize("taxonomy", _GOOD_TAXONOMY)
def test_fetch_concept_accepts_legitimate_taxonomy(taxonomy: str) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            json=_make_payload(tag="Revenues", taxonomy=taxonomy, entries=[]),
        )

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    result = provider.fetch_concept("320193", taxonomy, "Revenues")

    assert result is not None
    assert len(captured) == 1
    assert f"/{taxonomy}/Revenues.json" in captured[0].url.path


# ----------------------------------------------------------------------
# Test 5 — tag validation
# ----------------------------------------------------------------------


_BAD_TAG = [
    "Revenues/Q1",  # slash
    "Revenues ",  # trailing space
    "",  # empty
    "123Revenues",  # leading digit
    "Revenu€s",  # non-ASCII
    "Revenues\n",  # trailing newline
]

_GOOD_TAG = [
    "Revenues",
    "EntityCommonStockSharesOutstanding",
    "my_custom_concept",
]


@pytest.mark.parametrize("tag", _BAD_TAG)
def test_fetch_concept_rejects_malformed_tag(tag: str) -> None:
    provider = _make_provider()
    _rewire_transport(
        provider,
        lambda r: httpx.Response(200, json=_make_payload(tag="Revenues", entries=[])),
        preserve_throttle=False,
    )

    with pytest.raises(ValueError, match="invalid tag"):
        provider.fetch_concept("320193", "us-gaap", tag)


@pytest.mark.parametrize("tag", _GOOD_TAG)
def test_fetch_concept_accepts_legitimate_tag(tag: str) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_make_payload(tag=tag, entries=[]))

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    provider.fetch_concept("320193", "us-gaap", tag)

    assert len(captured) == 1
    assert captured[0].url.path.endswith(f"/{tag}.json")


# ----------------------------------------------------------------------
# Test 6 — extractor reuse with integer USD revenue fixture
# (Codex 1b r1 MED-2 ownership — float-boundary moved to test 7)
# ----------------------------------------------------------------------


def test_extract_concept_facts_reuses_section_extractor() -> None:
    payload = _make_payload(
        tag="Revenues",
        entries=[
            {
                "end": "2024-09-28",
                "val": 391035000000,
                "accn": "0000320193-24-000123",
                "fy": 2024,
                "fp": "FY",
                "form": "10-K",
                "filed": "2024-11-01",
            },
            {
                "start": "2025-03-30",
                "end": "2025-06-28",
                "val": 85777000000,
                "accn": "0000320193-25-000045",
                "fy": 2025,
                "fp": "Q3",
                "form": "10-Q",
                "filed": "2025-08-01",
            },
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    facts = provider.extract_concept_facts("AAPL", "320193", "us-gaap", "Revenues")

    assert len(facts) == 2
    fy = next(f for f in facts if f.form_type == "10-K")
    q3 = next(f for f in facts if f.form_type == "10-Q")
    assert fy.concept == "Revenues"
    assert fy.taxonomy == "us-gaap"
    assert fy.unit == "USD"
    assert fy.val == Decimal("391035000000")
    assert q3.val == Decimal("85777000000")
    assert q3.fiscal_period == "Q3"


# ----------------------------------------------------------------------
# Test 7 — Decimal(str) boundary on float val (USD/shares EPS)
# Pins prevention-log #1174.
# ----------------------------------------------------------------------


def test_extract_concept_facts_decimal_str_boundary() -> None:
    payload = _make_payload(
        tag="EarningsPerShareDiluted",
        units={
            "USD/shares": [
                {
                    "end": "2024-09-28",
                    "val": 3.7,  # non-binary-representable float
                    "accn": "0000320193-24-000123",
                    "fy": 2024,
                    "fp": "FY",
                    "form": "10-K",
                    "filed": "2024-11-01",
                }
            ]
        },
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    facts = provider.extract_concept_facts("AAPL", "320193", "us-gaap", "EarningsPerShareDiluted")

    assert len(facts) == 1
    assert facts[0].unit == "USD/shares"
    # Decimal(str(3.7)) == Decimal("3.7"); Decimal(3.7) would be
    # Decimal("3.7000000000000001776...") — the str() boundary is
    # what prevention-log #1174 enforces.
    assert facts[0].val == Decimal("3.7")


# ----------------------------------------------------------------------
# Test 8 — extract empty on 404
# ----------------------------------------------------------------------


def test_extract_concept_facts_empty_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    facts = provider.extract_concept_facts("AAPL", "320193", "us-gaap", "Revenues")

    assert facts == []


# ----------------------------------------------------------------------
# Test 9 — extract empty on missing units + warning
# ----------------------------------------------------------------------


def test_extract_concept_facts_empty_on_missing_units(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_make_payload(tag="Revenues", include_units=False),
        )

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    with caplog.at_level(logging.WARNING):
        facts = provider.extract_concept_facts("AAPL", "320193", "us-gaap", "Revenues")

    assert facts == []
    assert any(
        "missing or non-dict units" in rec.message and "320193" in rec.message and "Revenues" in rec.message
        for rec in caplog.records
    )


# ----------------------------------------------------------------------
# Test 9a — taxonomy-mismatch warning branch
# (Codex 2 r1 LOW-2 ownership — pins the synthesis-contract warning
# that fires when SEC response taxonomy differs from request)
# ----------------------------------------------------------------------


def test_extract_concept_facts_logs_warning_on_taxonomy_mismatch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = _make_payload(
        tag="Revenues",
        taxonomy="srt",  # response taxonomy differs from request
        entries=[
            {
                "end": "2024-09-28",
                "val": 391035000000,
                "accn": "0000320193-24-000123",
                "fy": 2024,
                "fp": "FY",
                "form": "10-K",
                "filed": "2024-11-01",
            }
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=False)

    with caplog.at_level(logging.WARNING):
        facts = provider.extract_concept_facts("AAPL", "320193", "us-gaap", "Revenues")

    # Source-of-truth is the REQUEST taxonomy — emitted facts carry
    # 'us-gaap' even though response said 'srt'.
    assert len(facts) == 1
    assert facts[0].taxonomy == "us-gaap"
    assert any(
        "response taxonomy 'srt' differs from request 'us-gaap'" in rec.message
        and "320193" in rec.message
        and "Revenues" in rec.message
        for rec in caplog.records
    )


# ----------------------------------------------------------------------
# Test 10 — rate-limit clock sharing (identity + behaviour)
# ----------------------------------------------------------------------


def test_fetch_concept_shares_rate_limit_clock_identity() -> None:
    """Identity sub-test — pins the §3.4 invariant: the provider's
    HTTP client is bound to the process-wide shared clock + lock.
    """
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


def test_fetch_concept_throttles_back_to_back_calls() -> None:
    """Behaviour sub-test — rebuild ``_http`` around a MockTransport
    while preserving the shared clock + lock; back-to-back
    ``fetch_concept`` calls observe a delta ≥
    ``_MIN_REQUEST_INTERVAL_S`` between them.

    Teardown (Codex 1b r1 LOW-4 ownership): close the swapped
    client + reset ``_PROCESS_RATE_LIMIT_CLOCK[0]`` so the shared
    mutation does not bleed into the rest of ``uv run pytest``.
    """
    call_times: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_times.append(time.monotonic())
        return httpx.Response(
            200,
            json=_make_payload(tag="Revenues", entries=[]),
        )

    provider = _make_provider()
    _rewire_transport(provider, handler, preserve_throttle=True)

    try:
        # Reset shared clock so the first call is unimpeded.
        sec_edgar._PROCESS_RATE_LIMIT_CLOCK[0] = 0.0  # noqa: SLF001
        provider.fetch_concept("320193", "us-gaap", "Revenues")
        provider.fetch_concept("320193", "us-gaap", "NetIncomeLoss")

        assert len(call_times) == 2
        delta = call_times[1] - call_times[0]
        assert delta >= _MIN_REQUEST_INTERVAL_S, (
            f"expected delta >= {_MIN_REQUEST_INTERVAL_S:.3f}s (got {delta:.3f}s) — rate-limit clock not engaged"
        )
    finally:
        provider._client.close()  # noqa: SLF001
        # Restore baseline so subsequent unit tests are not slowed.
        sec_edgar._PROCESS_RATE_LIMIT_CLOCK[0] = 0.0  # noqa: SLF001
