"""
eToro broker provider.

Thin adapter for the eToro trading API.  No domain logic, no DB access.
Raw responses are returned as-is for the service layer to persist.

Auth: three-header scheme (x-api-key, x-user-key, x-request-id).
Base URL: https://public-api.etoro.com (configurable via settings.etoro_base_url).
Trading endpoints are environment-scoped: /demo/ prefix for demo, no prefix for real.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import TracebackType
from typing import Any

import httpx

from app.config import settings
from app.providers.broker import BrokerOrderResult, BrokerProvider, OrderStatus

logger = logging.getLogger(__name__)

# Map eToro statusID values to our OrderStatus.
# Populated from documented API responses. Edge-case status values
# may need live validation — unknown statuses default to "pending".
_STATUS_MAP: dict[str, OrderStatus] = {
    "Executed": "filled",
    "Filled": "filled",
    "Pending": "pending",
    "Rejected": "rejected",
    "Failed": "failed",
    "Cancelled": "rejected",
}


class EtoroBrokerProvider(BrokerProvider):
    """
    eToro trading API client.

    Callers must supply both ``api_key`` and ``user_key`` (loaded from
    the encrypted broker_credentials store).  Use as a context manager:

        with EtoroBrokerProvider(
            api_key=..., user_key=..., env="demo",
        ) as broker:
            result = broker.place_order(...)
    """

    def __init__(self, api_key: str, user_key: str, env: str = "demo") -> None:
        self._env = env
        self._client = httpx.Client(
            base_url=settings.etoro_base_url,
            headers={
                "x-api-key": api_key,
                "x-user-key": user_key,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

        # Environment-scoped path prefixes for trading endpoints.
        # Demo: /api/v1/trading/execution/demo/...
        # Real: /api/v1/trading/execution/...
        env_segment = f"/{env}" if env == "demo" else ""
        self._exec_prefix = f"/api/v1/trading/execution{env_segment}"
        self._info_prefix = f"/api/v1/trading/info{env_segment}"

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

    def _request_headers(self) -> dict[str, str]:
        """Per-request headers — fresh UUID for x-request-id."""
        from uuid import uuid4

        return {"x-request-id": str(uuid4())}

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
        # EXIT should never reach place_order — the service layer routes
        # EXIT to close_position. Guard against misrouting.
        if action == "EXIT":
            logger.error(
                "EXIT action reached place_order for instrument %d — this is a routing error in the service layer",
                instrument_id,
            )
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": "EXIT action must use close_position, not place_order"},
            )

        # Determine endpoint and amount field based on order type.
        if units is not None:
            endpoint = f"{self._exec_prefix}/market-open-orders/by-units"
            body: dict[str, Any] = {
                "InstrumentID": instrument_id,
                "IsBuy": True,  # v1 is long-only
                "Leverage": 1,  # v1 is no-leverage
                "AmountInUnits": float(units),
                "StopLossRate": None,
                "TakeProfitRate": None,
                "IsTslEnabled": False,
                "IsNoStopLoss": True,
                "IsNoTakeProfit": True,
            }
        else:
            endpoint = f"{self._exec_prefix}/market-open-orders/by-amount"
            body = {
                "InstrumentID": instrument_id,
                "IsBuy": True,
                "Leverage": 1,
                "Amount": float(amount) if amount is not None else 0,
                "StopLossRate": None,
                "TakeProfitRate": None,
                "IsTslEnabled": False,
                "IsNoStopLoss": True,
                "IsNoTakeProfit": True,
            }

        try:
            response = self._client.post(
                endpoint,
                json=body,
                headers=self._request_headers(),
            )
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
                raw_payload={"_ebull_action": action, **raw},
            )
        except httpx.HTTPError as exc:
            logger.error("eToro place_order network error: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"_ebull_action": action, "error": str(exc)},
            )

        # Preserve the domain action in raw_payload for audit trail.
        # eToro only has IsBuy — our BUY/ADD distinction is eBull-specific.
        raw["_ebull_action"] = action
        return _normalise_open_order_response(raw)

    def close_position(self, instrument_id: int) -> BrokerOrderResult:
        # Step 1: Resolve instrument_id → positionId via portfolio lookup.
        # The eToro close endpoint requires a positionId, not an instrumentId.
        # clientPortfolio.positions[] has both instrumentID and positionID.
        position_id = self._resolve_position_id(instrument_id)
        if position_id is None:
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={
                    "error": f"No open position found for instrument {instrument_id}",
                },
            )

        # Step 2: Close the position.
        body: dict[str, Any] = {
            "InstrumentID": instrument_id,
            "UnitsToDeduct": None,  # close entire position
        }

        try:
            response = self._client.post(
                f"{self._exec_prefix}/market-close-orders/positions/{position_id}",
                json=body,
                headers=self._request_headers(),
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

        return _normalise_close_order_response(raw)

    def get_order_status(self, broker_order_ref: str) -> BrokerOrderResult:
        try:
            response = self._client.get(
                f"{self._info_prefix}/orders/{broker_order_ref}",
                headers=self._request_headers(),
            )
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

        return _normalise_order_info_response(raw, broker_order_ref)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_position_id(self, instrument_id: int) -> int | None:
        """Look up the open positionID for an instrument via the portfolio endpoint.

        Returns None if no open position exists or the lookup fails.
        """
        try:
            response = self._client.get(
                f"{self._info_prefix}/portfolio",
                headers=self._request_headers(),
            )
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPError as exc:
            logger.error("eToro portfolio lookup failed: %s", exc)
            return None

        # Response shape: { clientPortfolio: { positions: [...] } }
        portfolio = raw.get("clientPortfolio") or {}
        positions: list[dict[str, Any]] = portfolio.get("positions") or []

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            if pos.get("instrumentID") == instrument_id:
                pos_id = pos.get("positionID")
                if pos_id is not None:
                    return int(pos_id)

        logger.warning(
            "No open position found for instrument %d in portfolio (%d positions checked)",
            instrument_id,
            len(positions),
        )
        return None


# ------------------------------------------------------------------
# Normalisers — pure functions, no I/O
# ------------------------------------------------------------------


def _normalise_open_order_response(raw: dict[str, Any]) -> BrokerOrderResult:
    """Normalise an eToro open-order response to BrokerOrderResult.

    Open order returns ``orderForOpen`` with ``orderID``, ``statusID``,
    ``instrumentID``.
    """
    order_data = raw.get("orderForOpen") or raw
    return _build_result(order_data, raw)


def _normalise_close_order_response(raw: dict[str, Any]) -> BrokerOrderResult:
    """Normalise an eToro close-order response to BrokerOrderResult.

    Close order returns ``orderForClose`` with ``positionID``, ``orderID``,
    ``statusID``, ``instrumentID``.
    """
    order_data = raw.get("orderForClose") or raw
    return _build_result(order_data, raw)


def _normalise_order_info_response(
    raw: dict[str, Any],
    broker_order_ref: str,
) -> BrokerOrderResult:
    """Normalise an eToro order-info response to BrokerOrderResult.

    Order info returns ``orderID``, ``statusID``, ``instrumentID``,
    ``amount``, ``units``, and ``positions[]`` with ``positionID``.
    """
    return _build_result(raw, raw, fallback_ref=broker_order_ref)


def _build_result(
    order_data: dict[str, Any],
    raw_payload: dict[str, Any],
    *,
    fallback_ref: str | None = None,
) -> BrokerOrderResult:
    """Build a BrokerOrderResult from normalised eToro order data."""
    ref = order_data.get("orderID")
    raw_status = order_data.get("statusID")
    status: OrderStatus = _STATUS_MAP.get(str(raw_status), "pending") if raw_status is not None else "pending"

    filled_price: Decimal | None = None
    filled_units: Decimal | None = None
    fees = Decimal("0")

    raw_price = order_data.get("executionPrice")
    raw_units = order_data.get("units")
    raw_fees = order_data.get("fees")

    if raw_price is not None:
        filled_price = Decimal(str(raw_price))
    if raw_units is not None:
        filled_units = Decimal(str(raw_units))
    if raw_fees is not None:
        fees = Decimal(str(raw_fees))

    return BrokerOrderResult(
        broker_order_ref=str(ref) if ref is not None else fallback_ref,
        status=status,
        filled_price=filled_price,
        filled_units=filled_units,
        fees=fees,
        raw_payload=raw_payload,
    )


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    """Extract JSON from an error response, falling back to text."""
    try:
        return response.json()  # type: ignore[no-any-return]
    except Exception:
        return {"raw_text": response.text}
