"""Read-only eToro calls for the #3381 slice 3 perishables recorder.

Three endpoints on three documented quotas (``etoro_quota_lanes``):

* ``GET /api/v1/market-data/instruments/rates`` — lane F, the shared market-data pool.
* ``POST /api/v2/trading/info/demo/eligibility`` — lane B, dedicated 20/min.
* ``POST /api/v2/trading/info/demo/costs`` — lane C, dedicated 20/min.

Unlike ``EtoroMarketDataProvider.get_quotes`` and ``EtoroBrokerProvider``'s preflight methods, which
chunk, drop and parse, every method here makes exactly ONE request and returns the response AS SERVED
(status, decoded body, receipt time), because the recorder stores the body and decides what each status
means (``docs/proposals/etl/2026-09-25-3381-perishables-recorder.md``). Retryable statuses (429, 5xx) are
retried by ``ResilientClient`` and raise ``httpx.HTTPStatusError`` once exhausted; transport errors raise.

The two POSTs are informational ("what would this cost", "may I trade this"): nothing here places,
edits or closes anything, which is why ``refuse_broker_mutation_if_unattended`` does not apply (see the
``EtoroBrokerProvider.get_what_if_costs`` docstring). Demo only: the recorder's rows describe the demo
account, and the real-environment preflight paths are recorded drift (``KNOWN_PATH_DRIFT``).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Final
from uuid import uuid4

import httpx

from app.config import settings
from app.providers.broker import BrokerWhatIfOrder
from app.providers.implementations.etoro_broker import what_if_request_body
from app.providers.implementations.etoro_request_log import attempt_observer
from app.providers.resilient_client import ResilientClient

#: Lane F spacing, the same floor as ``EtoroMarketDataProvider``'s client.
_ETORO_RATES_INTERVAL_S: Final = 1.1
#: Lanes B and C spacing, the same floor as ``EtoroBrokerProvider``'s write client (20 per rolling 60 s).
_ETORO_PREFLIGHT_INTERVAL_S: Final = 3.5
#: The endpoint's documented maximum is 100 ids; the market-data provider sends 50 (``_RATES_BATCH_SIZE``).
RATES_BATCH_SIZE: Final = 50
#: The eligibility endpoint's documented maximum.
ELIGIBILITY_BATCH_SIZE: Final = 100

RATES_PATH: Final = "/api/v1/market-data/instruments/rates"
ELIGIBILITY_PATH: Final = "/api/v2/trading/info/demo/eligibility"
WHAT_IF_PATH: Final = "/api/v2/trading/info/demo/costs"


@dataclass(frozen=True)
class RawResponse:
    """One response as served: status, decoded body (JSON, else the text) and its receipt time."""

    status: int
    body: Any
    observed_at: datetime


def _decode(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _paced_client(client: httpx.Client, interval_s: float, src: str) -> ResilientClient:
    return ResilientClient(
        client,
        min_request_interval_s=interval_s,
        shared_last_request=[0.0],
        shared_throttle_lock=threading.Lock(),
        on_attempt=attempt_observer(src, "demo"),
    )


class EtoroPerishablesProvider:
    """One paced client per quota lane. Use as a context manager."""

    def __init__(self, api_key: str, user_key: str, env: str = "demo") -> None:
        if env != "demo":
            raise ValueError("the perishables recorder records the demo account only")
        self._client = httpx.Client(
            base_url=settings.etoro_base_url,
            headers={"x-api-key": api_key, "x-user-key": user_key, "Content-Type": "application/json"},
            timeout=30.0,
        )
        # Separate clocks: B and C are separate dedicated quotas, and F is a different pool.
        self._http_rates = _paced_client(self._client, _ETORO_RATES_INTERVAL_S, "perishables_rates")
        self._http_eligibility = _paced_client(self._client, _ETORO_PREFLIGHT_INTERVAL_S, "perishables_eligibility")
        self._http_what_if = _paced_client(self._client, _ETORO_PREFLIGHT_INTERVAL_S, "perishables_what_if")

    def __enter__(self) -> EtoroPerishablesProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def get_rates(self, instrument_ids: list[int]) -> RawResponse:
        """One rates request for at most ``RATES_BATCH_SIZE`` ids."""
        if not 1 <= len(instrument_ids) <= RATES_BATCH_SIZE:
            raise ValueError(f"a rates request carries 1..{RATES_BATCH_SIZE} ids")
        # Inline query string: httpx would percent-encode the commas, which eToro rejects
        # (``EtoroMarketDataProvider.get_quotes``).
        ids = ",".join(str(i) for i in instrument_ids)
        response = self._http_rates.get(f"{RATES_PATH}?instrumentIds={ids}", headers={"x-request-id": str(uuid4())})
        return RawResponse(response.status_code, _decode(response), datetime.now(UTC))

    def post_eligibility(self, instrument_ids: list[int]) -> RawResponse:
        """One eligibility request for at most ``ELIGIBILITY_BATCH_SIZE`` ids."""
        if not 1 <= len(instrument_ids) <= ELIGIBILITY_BATCH_SIZE:
            raise ValueError(f"an eligibility request carries 1..{ELIGIBILITY_BATCH_SIZE} ids")
        response = self._http_eligibility.post(
            ELIGIBILITY_PATH,
            json={"instrumentIds": instrument_ids, "currency": "USD"},
            headers={"x-request-id": str(uuid4())},
        )
        return RawResponse(response.status_code, _decode(response), datetime.now(UTC))

    def post_what_if(self, order: BrokerWhatIfOrder) -> RawResponse:
        """One what-if cost request for *order* (informational; places nothing)."""
        response = self._http_what_if.post(
            WHAT_IF_PATH, json=what_if_request_body(order), headers={"x-request-id": str(uuid4())}
        )
        return RawResponse(response.status_code, _decode(response), datetime.now(UTC))


__all__ = [
    "ELIGIBILITY_BATCH_SIZE",
    "RATES_BATCH_SIZE",
    "EtoroPerishablesProvider",
    "RawResponse",
]
