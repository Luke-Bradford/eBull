"""
eToro broker provider.

Thin adapter for the eToro write API.  No domain logic, no DB access.
Raw responses are returned as-is for the service layer to persist.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import TracebackType
from typing import Any

import httpx

from app.providers.broker import BrokerOrderResult, BrokerProvider, OrderStatus

logger = logging.getLogger(__name__)

_ETORO_BASE_URL = "https://api.etoro.com"


class EtoroBrokerProvider(BrokerProvider):
    """
    eToro write API client.

    Callers must supply the API key (loaded from the encrypted
    broker_credentials store).  Use as a context manager:

        with EtoroBrokerProvider(api_key=...) as broker:
            result = broker.place_order(...)
    """

    def __init__(self, api_key: str) -> None:
        self._client = httpx.Client(
            base_url=_ETORO_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    def __enter__(self) -> EtoroBrokerProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client. Prefer using as a context manager."""
        self._client.close()

    # ------------------------------------------------------------------
    # BrokerProvider implementation
    # ------------------------------------------------------------------

    def place_order(
        self,
        instrument_id: int,
        action: str,
        amount: Decimal | None,
        units: Decimal | None,
    ) -> BrokerOrderResult:
        body: dict[str, Any] = {
            "instrumentId": instrument_id,
            "action": action,
        }
        if amount is not None:
            body["amount"] = str(amount)
        if units is not None:
            body["units"] = str(units)

        try:
            response = self._client.post("/v1/orders", json=body)
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPStatusError as exc:
            raw = _safe_json(exc.response)
            logger.error(
                "eToro place_order failed: status=%d body=%s",
                exc.response.status_code,
                raw,
            )
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload=raw,
            )
        except httpx.HTTPError as exc:
            logger.error("eToro place_order network error: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": str(exc)},
            )

        return _normalise_order_response(raw)

    def close_position(self, instrument_id: int) -> BrokerOrderResult:
        try:
            response = self._client.post(
                f"/v1/positions/{instrument_id}/close",
            )
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPStatusError as exc:
            raw = _safe_json(exc.response)
            logger.error(
                "eToro close_position failed: status=%d body=%s",
                exc.response.status_code,
                raw,
            )
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload=raw,
            )
        except httpx.HTTPError as exc:
            logger.error("eToro close_position network error: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": str(exc)},
            )

        return _normalise_order_response(raw)

    def get_order_status(self, broker_order_ref: str) -> BrokerOrderResult:
        try:
            response = self._client.get(f"/v1/orders/{broker_order_ref}")
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPStatusError as exc:
            raw = _safe_json(exc.response)
            logger.error(
                "eToro get_order_status failed: status=%d body=%s",
                exc.response.status_code,
                raw,
            )
            return BrokerOrderResult(
                broker_order_ref=broker_order_ref,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload=raw,
            )
        except httpx.HTTPError as exc:
            logger.error("eToro get_order_status network error: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=broker_order_ref,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": str(exc)},
            )

        return _normalise_order_response(raw)


# ------------------------------------------------------------------
# Normalisers — pure functions, no I/O
# ------------------------------------------------------------------

_STATUS_MAP: dict[str, OrderStatus] = {
    "Filled": "filled",
    "filled": "filled",
    "Pending": "pending",
    "pending": "pending",
    "Rejected": "rejected",
    "rejected": "rejected",
    "Failed": "failed",
    "failed": "failed",
    "Executed": "filled",
    "executed": "filled",
}


def _first_present(*keys: str, source: dict[str, Any]) -> Any:
    """Return the value of the first key present (not None) in source."""
    for k in keys:
        val = source.get(k)
        if val is not None:
            return val
    return None


def _normalise_order_response(raw: dict[str, Any]) -> BrokerOrderResult:
    """Map eToro order response to BrokerOrderResult."""
    ref = _first_present("OrderId", "orderId", "order_id", source=raw)
    raw_status = _first_present("Status", "status", source=raw) or "pending"
    status: OrderStatus = _STATUS_MAP.get(str(raw_status), "pending")

    filled_price: Decimal | None = None
    filled_units: Decimal | None = None
    fees = Decimal("0")

    raw_price = _first_present("ExecutionPrice", "execution_price", "price", source=raw)
    raw_units = _first_present("ExecutedUnits", "executed_units", "units", source=raw)
    raw_fees = _first_present("Fees", "fees", source=raw)

    if raw_price is not None:
        filled_price = Decimal(str(raw_price))
    if raw_units is not None:
        filled_units = Decimal(str(raw_units))
    if raw_fees is not None:
        fees = Decimal(str(raw_fees))

    return BrokerOrderResult(
        broker_order_ref=str(ref) if ref is not None else None,
        status=status,
        filled_price=filled_price,
        filled_units=filled_units,
        fees=fees,
        raw_payload=raw,
    )


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    """Extract JSON from an error response, falling back to text."""
    try:
        return response.json()  # type: ignore[no-any-return]
    except Exception:
        return {"raw_text": response.text}
