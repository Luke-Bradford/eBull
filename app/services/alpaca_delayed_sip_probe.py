"""Bounded, read-only contract probe for Alpaca delayed historical SIP data.

This is research-source qualification for #2520, not a production ingestion
provider.  It deliberately writes no database rows and never returns raw bars:
the result contains only counts, bounds, hashes, matched reference identities
and rate-limit headers.  A successful probe proves the configured account
contract; docs and marketing copy do not.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final

import httpx

DATA_BASE_URL: Final = "https://data.alpaca.markets"
TRADING_BASE_URL: Final = "https://paper-api.alpaca.markets"
MAX_HTTP_CALLS: Final = 12
MAX_PAGES_PER_SCENARIO: Final = 2
SOURCE_VERSION: Final = "alpaca_delayed_sip_probe_v1"

_RATE_HEADERS: Final = (
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "retry-after",
)


class ProbeRefusal(RuntimeError):
    """The source contract could not be proven without weakening it."""


class _CallBudget:
    def __init__(self, maximum: int = MAX_HTTP_CALLS) -> None:
        self.maximum = maximum
        self.used = 0

    def take(self) -> None:
        if self.used >= self.maximum:
            raise ProbeRefusal(f"HTTP call budget exhausted at {self.maximum}")
        self.used += 1


def credential_headers(*, key_id: str, secret_key: str) -> dict[str, str]:
    """Build authentication headers after rejecting empty/placeholder secrets."""
    key_id = key_id.strip()
    secret_key = secret_key.strip()
    if not key_id or not secret_key:
        raise ProbeRefusal("APCA_API_KEY_ID and APCA_API_SECRET_KEY are both required")
    if key_id.startswith("<") or secret_key.startswith("<"):
        raise ProbeRefusal("placeholder Alpaca credentials are not accepted")
    return {
        "APCA-API-KEY-ID": key_id,
        "APCA-API-SECRET-KEY": secret_key,
        "User-Agent": "eBull/2520 delayed-SIP source probe",
    }


def _safe_error(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:240]
    if isinstance(body, Mapping):
        return str(body.get("message") or body.get("error") or sorted(body))[:240]
    return type(body).__name__


def _get_json(
    client: httpx.Client,
    budget: _CallBudget,
    url: str,
    *,
    params: Mapping[str, str | int | float],
) -> tuple[Any, Mapping[str, str]]:
    budget.take()
    try:
        response = client.get(url, params=params, timeout=20.0)
    except httpx.RequestError as exc:
        raise ProbeRefusal(f"transport failure for {url}: {exc.__class__.__name__}") from exc
    if response.status_code != 200:
        raise ProbeRefusal(f"{url} returned HTTP {response.status_code}: {_safe_error(response)}")
    try:
        return response.json(), response.headers
    except ValueError as exc:
        raise ProbeRefusal(f"{url} returned non-JSON HTTP 200") from exc


def _decimal(value: object, *, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ProbeRefusal(f"bar field {field} is not numeric") from exc
    if not parsed.is_finite():
        raise ProbeRefusal(f"bar field {field} is not finite")
    return parsed


def validate_bars(rows: object) -> tuple[dict[str, object], ...]:
    """Validate Alpaca's compact bar schema without retaining vendor payloads."""
    if not isinstance(rows, list):
        raise ProbeRefusal("bar response 'bars' must be a list")
    validated: list[dict[str, object]] = []
    previous: datetime | None = None
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ProbeRefusal("every bar must be an object")
        try:
            stamp = datetime.fromisoformat(str(raw["t"]).replace("Z", "+00:00"))
            open_ = _decimal(raw["o"], field="o")
            high = _decimal(raw["h"], field="h")
            low = _decimal(raw["l"], field="l")
            close = _decimal(raw["c"], field="c")
        except KeyError as exc:
            raise ProbeRefusal(f"bar is missing {exc.args[0]!r}") from exc
        except (TypeError, ValueError) as exc:
            raise ProbeRefusal("bar timestamp has an invalid type") from exc
        if stamp.tzinfo is None:
            raise ProbeRefusal("bar timestamp must carry an offset")
        stamp = stamp.astimezone(UTC)
        raw_volume = raw["v"]
        if isinstance(raw_volume, bool) or not isinstance(raw_volume, int):
            raise ProbeRefusal("bar volume must be an integer")
        volume = raw_volume
        if previous is not None and stamp <= previous:
            raise ProbeRefusal("bars are not strictly ascending")
        if min(open_, high, low, close) <= 0:
            raise ProbeRefusal("bar OHLC must be positive")
        if high < max(open_, close, low) or low > min(open_, close, high):
            raise ProbeRefusal("bar violates OHLC bounds")
        if volume < 0:
            raise ProbeRefusal("bar volume must be non-negative")
        previous = stamp
        validated.append(
            {"t": stamp.isoformat(), "o": str(open_), "h": str(high), "l": str(low), "c": str(close), "v": volume}
        )
    return tuple(validated)


def _digest(rows: tuple[dict[str, object], ...]) -> str:
    encoded = json.dumps(rows, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _bar_summary(symbol: str, rows: tuple[dict[str, object], ...]) -> dict[str, object]:
    return {
        "symbol": symbol,
        "rows": len(rows),
        "first": rows[0]["t"] if rows else None,
        "last": rows[-1]["t"] if rows else None,
        "sha256": _digest(rows),
    }


def fetch_bar_scenario(
    client: httpx.Client,
    budget: _CallBudget,
    *,
    symbol: str,
    start: str,
    end: str,
    adjustment: str = "raw",
    asof: str,
    limit: int = 100,
    max_pages: int = MAX_PAGES_PER_SCENARIO,
) -> tuple[dict[str, object], dict[str, str]]:
    """Fetch at most two pages with exact SIP/raw/as-of provenance."""
    if adjustment not in {"raw", "split"}:
        raise ValueError("probe adjustment must be raw or split")
    if not 1 <= limit <= 10_000:
        raise ValueError("limit must be between 1 and 10000")
    params: dict[str, str | int | float] = {
        "timeframe": "1Min",
        "start": start,
        "end": end,
        "limit": limit,
        "adjustment": adjustment,
        "asof": asof,
        "feed": "sip",
        "sort": "asc",
    }
    all_rows: list[dict[str, object]] = []
    seen_tokens: set[str] = set()
    captured_headers: dict[str, str] = {}
    pages = 0
    while pages < max_pages:
        body, headers = _get_json(
            client,
            budget,
            f"{DATA_BASE_URL}/v2/stocks/{symbol}/bars",
            params=params,
        )
        if not isinstance(body, Mapping):
            raise ProbeRefusal("bar response must be an object")
        page = validate_bars(body.get("bars"))
        if all_rows and page and str(page[0]["t"]) <= str(all_rows[-1]["t"]):
            raise ProbeRefusal("bar pagination overlapped or regressed")
        all_rows.extend(page)
        pages += 1
        captured_headers.update({name: headers[name] for name in _RATE_HEADERS if name in headers})
        token_value = body.get("next_page_token")
        if token_value in {None, ""}:
            break
        token = str(token_value)
        if token in seen_tokens:
            raise ProbeRefusal("bar pagination token repeated")
        seen_tokens.add(token)
        params["page_token"] = token
    else:
        raise ProbeRefusal(f"{symbol} scenario exceeded the {max_pages}-page bound")
    summary = _bar_summary(symbol, tuple(all_rows))
    summary.update({"pages": pages, "feed": "sip", "adjustment": adjustment, "asof": asof})
    return summary, captured_headers


def _require_tsla_forward_split(body: object) -> dict[str, object]:
    """Prove the filtered response contains the expected action, not just keys."""
    if not isinstance(body, Mapping):
        raise ProbeRefusal("corporate_actions response must be an object")
    actions = body.get("corporate_actions")
    if not isinstance(actions, Mapping):
        raise ProbeRefusal("corporate_actions response is missing its action collection")
    forward_splits = actions.get("forward_splits")
    if not isinstance(forward_splits, list) or not forward_splits:
        raise ProbeRefusal("TSLA scenario returned no forward split")
    matching = [row for row in forward_splits if isinstance(row, Mapping) and row.get("symbol") == "TSLA"]
    if not matching:
        raise ProbeRefusal("forward-split response did not contain TSLA")
    row = matching[0]
    new_rate = _decimal(row.get("new_rate"), field="new_rate")
    old_rate = _decimal(row.get("old_rate"), field="old_rate")
    if new_rate <= old_rate or old_rate <= 0:
        raise ProbeRefusal("TSLA forward split does not increase shares")
    return {
        "name": "corporate_actions",
        "forward_splits": len(forward_splits),
        "symbol": "TSLA",
        "new_rate": str(new_rate),
        "old_rate": str(old_rate),
        "ex_date": row.get("ex_date"),
    }


def _require_inactive_twtr(body: object) -> dict[str, object]:
    if not isinstance(body, Mapping):
        raise ProbeRefusal("inactive_asset response must be an object")
    if body.get("symbol") != "TWTR" or body.get("status") != "inactive":
        raise ProbeRefusal("TWTR did not resolve as an inactive asset")
    return {
        "name": "inactive_asset",
        "symbol": "TWTR",
        "status": "inactive",
        "exchange": body.get("exchange"),
    }


@dataclass(frozen=True)
class _BarScenario:
    symbol: str
    start: str
    end: str
    asof: str
    limit: int = 100


def _require_rows(summary: Mapping[str, object], *, name: str) -> None:
    rows = summary.get("rows")
    if not isinstance(rows, int) or rows == 0:
        raise ProbeRefusal(f"{name} scenario returned no SIP bars")


def run_probe(client: httpx.Client) -> dict[str, object]:
    """Run the frozen <=12-call qualification panel and return compact evidence."""
    budget = _CallBudget()
    checks: list[dict[str, object]] = []
    rate_headers: dict[str, str] = {}

    scenarios = (
        _BarScenario("AAPL", "2016-01-04T14:30:00Z", "2016-01-04T14:33:00Z", "2016-01-04", 2),
        _BarScenario("AAPL", "2026-07-01T13:30:00Z", "2026-07-01T13:36:00Z", "2026-07-01"),
        _BarScenario("JPM", "2026-07-01T13:30:00Z", "2026-07-01T13:36:00Z", "2026-07-01"),
        _BarScenario("TWTR", "2022-10-20T13:30:00Z", "2022-10-20T13:36:00Z", "2022-10-20"),
        _BarScenario("SPY", "2025-11-28T14:30:00Z", "2025-11-28T18:05:00Z", "2025-11-28"),
    )
    for scenario in scenarios:
        summary, headers = fetch_bar_scenario(
            client,
            budget,
            symbol=scenario.symbol,
            start=scenario.start,
            end=scenario.end,
            asof=scenario.asof,
            limit=scenario.limit,
        )
        _require_rows(summary, name=scenario.symbol)
        checks.append(summary)
        rate_headers.update(headers)

    adjustment_hashes: dict[str, str] = {}
    for adjustment in ("raw", "split"):
        summary, headers = fetch_bar_scenario(
            client,
            budget,
            symbol="TSLA",
            start="2022-08-24T13:30:00Z",
            end="2022-08-24T13:36:00Z",
            adjustment=adjustment,
            asof="2022-08-24",
        )
        _require_rows(summary, name=f"TSLA {adjustment}")
        adjustment_hashes[adjustment] = str(summary["sha256"])
        checks.append(summary)
        rate_headers.update(headers)
    if adjustment_hashes["raw"] == adjustment_hashes["split"]:
        raise ProbeRefusal("TSLA raw and split-adjusted samples were identical across the declared split")

    calendar, headers = _get_json(
        client,
        budget,
        f"{TRADING_BASE_URL}/v2/calendar",
        params={"start": "2025-11-28", "end": "2025-11-28"},
    )
    if not isinstance(calendar, list) or len(calendar) != 1 or not isinstance(calendar[0], Mapping):
        raise ProbeRefusal("early-close calendar scenario did not return exactly one session")
    close = str(calendar[0].get("close", ""))
    if close != "13:00":
        raise ProbeRefusal(f"2025-11-28 calendar did not report the expected early close: {close!r}")
    checks.append({"name": "early_close_calendar", "date": "2025-11-28", "close": close})
    rate_headers.update({name: headers[name] for name in _RATE_HEADERS if name in headers})

    actions, headers = _get_json(
        client,
        budget,
        f"{DATA_BASE_URL}/v1/corporate-actions",
        params={
            "symbols": "TSLA",
            "types": "forward_split",
            "start": "2022-08-20",
            "end": "2022-08-31",
            "limit": 10,
            "sort": "asc",
        },
    )
    checks.append(_require_tsla_forward_split(actions))
    rate_headers.update({name: headers[name] for name in _RATE_HEADERS if name in headers})

    inactive, headers = _get_json(
        client,
        budget,
        f"{TRADING_BASE_URL}/v2/assets/TWTR",
        params={},
    )
    checks.append(_require_inactive_twtr(inactive))
    rate_headers.update({name: headers[name] for name in _RATE_HEADERS if name in headers})

    return {
        "status": "qualified",
        "source_version": SOURCE_VERSION,
        "calls_used": budget.used,
        "call_budget": budget.maximum,
        "checks": checks,
        "rate_limit_headers": rate_headers,
    }


__all__ = [
    "MAX_HTTP_CALLS",
    "ProbeRefusal",
    "credential_headers",
    "fetch_bar_scenario",
    "run_probe",
    "validate_bars",
]
