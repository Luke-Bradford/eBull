"""Fixture-shape assertions for the OpenFIGI probe captures (PR-0 of #1233).

No HTTP calls. The probe under ``scripts/probe_openfigi.py`` issues live
requests against ``api.openfigi.com``; these tests only validate that the
fixtures it produced match the contract PR-1b's resolver will rely on.

If a future probe run records a fixture that breaks one of these
assertions, EITHER OpenFIGI's contract changed (re-spec PR-1b) OR the
probe regressed (fix the probe). Do not silence these tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "openfigi"

# Per-row keys we depend on for PR-1b's resolver. If any of these go
# missing from a successful entry the resolver will fail at runtime.
_REQUIRED_MAPPING_KEYS: frozenset[str] = frozenset({"ticker", "name", "exchCode", "securityType"})

# Headers we depend on for rate-limit reasoning. All lower-case
# because httpx normalises header names.
_REQUIRED_RATE_LIMIT_HEADERS_2XX: frozenset[str] = frozenset(
    {"ratelimit-limit", "ratelimit-remaining", "ratelimit-reset"}
)

# Authoritative CUSIP → US-primary-ticker mapping for the probed CUSIPs.
# Pins the parallel-array positional contract — any positional drift in
# the recorded fixture would point a CUSIP at the wrong ticker.
_EXPECTED_TICKER: dict[str, str] = {
    "037833100": "AAPL",
    "594918104": "MSFT",
    "46625H100": "JPM",
    "36467W109": "GME",
    "437076102": "HD",
    "67066G104": "NVDA",
    "88160R101": "TSLA",
    "023135106": "AMZN",
    "02079K305": "GOOGL",
    "30303M102": "META",
}


def _us_primary_ticker(entry: dict[str, Any]) -> str | None:
    """Mirror PR-1b's defensive pick: first US common-stock entry's ticker."""
    if "data" not in entry or not isinstance(entry["data"], list):
        return None
    for mapping in entry["data"]:
        if mapping.get("exchCode") == "US" and mapping.get("securityType") == "Common Stock":
            ticker = mapping.get("ticker")
            return ticker if isinstance(ticker, str) else None
    return None


def _load(name: str) -> dict[str, Any]:
    path = FIXTURE_DIR / name
    assert path.exists(), f"fixture missing: {path}"
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


# ----------------------- envelope sanity -----------------------------


@pytest.mark.parametrize(
    "fixture",
    [
        "single_aapl.json",
        "batch_known_5.json",
        "batch_with_invalid.json",
        "rate_limit_429.json",
    ],
)
def test_fixture_envelope(fixture: str) -> None:
    """Every fixture has the captured-request + captured-response envelope."""
    blob = _load(fixture)
    assert blob["scenario"], "missing scenario"
    assert blob["captured_at"], "missing captured_at"
    assert blob["request"]["method"] == "POST"
    assert blob["request"]["url"] == "https://api.openfigi.com/v3/mapping"
    assert isinstance(blob["request"]["body"], list)
    assert isinstance(blob["response"]["status_code"], int)
    assert isinstance(blob["response"]["headers"], dict)
    assert isinstance(blob["elapsed_ms"], int)
    assert blob["elapsed_ms"] >= 0


@pytest.mark.parametrize(
    "fixture",
    [
        "single_aapl.json",
        "batch_known_5.json",
        "batch_with_invalid.json",
        "rate_limit_429.json",
    ],
)
def test_request_body_is_cusip_idtype(fixture: str) -> None:
    """Every request item declares ID_CUSIP — the only flow eBull is permitted."""
    blob = _load(fixture)
    body = blob["request"]["body"]
    assert body, f"empty request body in {fixture}"
    for entry in body:
        assert entry["idType"] == "ID_CUSIP"
        assert isinstance(entry["idValue"], str)
        assert len(entry["idValue"]) == 9, f"CUSIP must be 9 chars, got {entry!r}"


# ----------------------- success-path shape --------------------------


def test_single_aapl_resolves_to_aapl_ticker() -> None:
    """Single CUSIP → first data entry has ticker AAPL on US exchange."""
    blob = _load("single_aapl.json")
    assert blob["response"]["status_code"] == 200
    body = blob["response"]["body"]
    assert isinstance(body, list) and len(body) == 1
    entry = body[0]
    assert "data" in entry and entry["data"], "AAPL CUSIP must return non-empty data"
    # All required keys present on every mapping row.
    for mapping in entry["data"]:
        missing = _REQUIRED_MAPPING_KEYS - mapping.keys()
        assert not missing, f"mapping missing required keys: {missing}"
    # The US-primary common-stock filter must locate AAPL.
    us_primaries = [m for m in entry["data"] if m.get("exchCode") == "US" and m.get("securityType") == "Common Stock"]
    assert us_primaries, "no US common-stock entry for AAPL"
    assert us_primaries[0]["ticker"] == "AAPL"


def test_batch_of_five_parallel_to_input() -> None:
    """Response is positional-parallel to request — 5 in, 5 out, each CUSIP → expected ticker."""
    blob = _load("batch_known_5.json")
    assert blob["response"]["status_code"] == 200
    body = blob["response"]["body"]
    request = blob["request"]["body"]
    assert len(body) == len(request) == 5
    # Each request CUSIP must positionally resolve to its known US-primary ticker.
    # Catches positional drift OR a wrong-but-valid CUSIP swap.
    for i, (req_item, entry) in enumerate(zip(request, body, strict=True)):
        cusip = req_item["idValue"]
        expected = _EXPECTED_TICKER.get(cusip)
        assert expected is not None, f"unexpected CUSIP at index {i}: {cusip}"
        actual = _us_primary_ticker(entry)
        assert actual == expected, f"index {i} CUSIP {cusip}: expected ticker {expected}, got {actual!r}"


def test_batch_with_invalid_isolates_failure() -> None:
    """9 valid + 1 invalid CUSIP — exactly one warning entry, others pin to known tickers."""
    blob = _load("batch_with_invalid.json")
    assert blob["response"]["status_code"] == 200
    body = blob["response"]["body"]
    request = blob["request"]["body"]
    assert len(body) == len(request) == 10
    # The deliberately invalid CUSIP is the LAST item in the request.
    assert request[-1]["idValue"] == "000000000"
    # The corresponding response entry is a warning, NOT data.
    invalid_entry = body[-1]
    assert "data" not in invalid_entry, f"invalid CUSIP should not have data: {invalid_entry!r}"
    assert "warning" in invalid_entry, f"expected warning key: {invalid_entry!r}"
    assert isinstance(invalid_entry["warning"], str)
    # The other 9 must positionally resolve to their expected tickers.
    for i, (req_item, entry) in enumerate(zip(request[:-1], body[:-1], strict=True)):
        cusip = req_item["idValue"]
        expected = _EXPECTED_TICKER.get(cusip)
        assert expected is not None, f"unexpected CUSIP at index {i}: {cusip}"
        actual = _us_primary_ticker(entry)
        assert actual == expected, f"index {i} CUSIP {cusip}: expected ticker {expected}, got {actual!r}"


# ----------------------- rate-limit headers --------------------------


@pytest.mark.parametrize(
    "fixture",
    ["single_aapl.json", "batch_known_5.json", "batch_with_invalid.json"],
)
def test_2xx_emits_ratelimit_headers(fixture: str) -> None:
    """Every 2xx call exposes the IETF draft RateLimit-* headers we rely on."""
    blob = _load(fixture)
    assert blob["response"]["status_code"] == 200
    headers = {k.lower(): v for k, v in blob["response"]["headers"].items()}
    missing = _REQUIRED_RATE_LIMIT_HEADERS_2XX - headers.keys()
    assert not missing, f"{fixture}: missing rate-limit headers: {missing}"
    # `ratelimit-limit: 25` is the contract for the unkeyed tier.
    assert headers["ratelimit-limit"] == "25"
    assert int(headers["ratelimit-remaining"]) >= 0


# ----------------------- 429 contract --------------------------------


def test_rate_limit_429_shape() -> None:
    """The 429 fixture carries Retry-After + non-JSON body."""
    blob = _load("rate_limit_429.json")
    assert blob["response"]["status_code"] == 429
    headers = {k.lower(): v for k, v in blob["response"]["headers"].items()}
    assert "retry-after" in headers, "429 must include Retry-After header"
    retry_after = int(headers["retry-after"])
    assert retry_after > 0
    # ratelimit-remaining must be 0 at the point we trip 429.
    assert headers.get("ratelimit-remaining") == "0"
    # 429 body is plain text, NOT JSON — string is the empirical shape.
    body = blob["response"]["body"]
    assert isinstance(body, str), f"expected plain-text 429 body, got {type(body).__name__}"
    assert "too many requests" in body.lower()
