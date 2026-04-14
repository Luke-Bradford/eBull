"""
eToro broker provider.

Thin adapter for the eToro trading API.  No domain logic, no DB access.
Raw responses are returned as-is for the service layer to persist.

Auth: three-header scheme (x-api-key, x-user-key, x-request-id).
Base URL: https://public-api.etoro.com (configurable via settings.etoro_base_url).
Trading endpoints are environment-scoped: /demo/ prefix for demo, no prefix for real.
"""

from __future__ import annotations

import decimal
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Any

import httpx

from app.config import settings
from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerOrderResult,
    BrokerPortfolio,
    BrokerPosition,
    BrokerProvider,
    OrderStatus,
)
from app.providers.resilient_client import ResilientClient

logger = logging.getLogger(__name__)

_RAW_PAYLOAD_DIR = Path("data/raw/etoro_broker")


def _persist_raw(tag: str, payload: bytes) -> None:
    """Write raw API response bytes to disk before normalisation.

    Raises ``OSError`` on disk-level failures (permission denied, disk full)
    so the caller can decide whether to abort or continue.  Non-OS exceptions
    are logged and swallowed.
    """
    try:
        _RAW_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = _RAW_PAYLOAD_DIR / f"{tag}_{ts}.json"
        path.write_bytes(payload)
    except OSError:
        raise
    except Exception:
        logger.warning("Failed to persist raw payload for tag=%s", tag, exc_info=True)


# Actions the service layer is allowed to send to place_order.
# EXIT is routed to close_position by the service layer and must never
# reach here. HOLD does not produce broker calls at all.
_ALLOWED_PLACE_ORDER_ACTIONS = frozenset({"BUY", "ADD"})

# Map eToro statusID values to our OrderStatus.
# Populated from documented API responses. Edge-case status values
# may need live validation — unknown statuses default to "pending".
# eToro rate limits: 60 GET/min (read), 20 POST/min (write).
# Read: 1.1s interval ≈ 55/min (~8% headroom).
# Write: 3.5s interval ≈ 17/min (~15% headroom).
_ETORO_READ_INTERVAL_S = 1.1
_ETORO_WRITE_INTERVAL_S = 3.5

_STATUS_MAP: dict[str, OrderStatus] = {
    "Executed": "filled",
    "Filled": "filled",
    "Pending": "pending",
    "Rejected": "rejected",
    "Failed": "failed",
    "Cancelled": "rejected",
}


class PortfolioParseError(Exception):
    """Raised when a mirrors[] row cannot be parsed safely.

    Directly subclasses Exception (NOT ValueError / TypeError /
    KeyError / decimal.DecimalException) so the outer parse loop can
    distinguish it from incidental exceptions and re-raise. Never
    swallowed by any `except (KeyError, ValueError, TypeError,
    decimal.DecimalException)` block.

    See spec §2.2.1 for the hierarchy rationale and §2.3.3 for the
    strict-raise sync contract that depends on it.
    """


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
        # Separate throttle rates for reads (GET 60/min) and writes (POST 20/min).
        # Both share the same _last_request_at timestamp so interleaved
        # GET+POST calls cannot exceed the API's combined rate limit.
        shared_ts: list[float] = [0.0]
        self._http_read = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_READ_INTERVAL_S,
            shared_last_request=shared_ts,
        )
        self._http_write = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_WRITE_INTERVAL_S,
            shared_last_request=shared_ts,
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
        # Reject unrecognised actions before any HTTP call.
        if action not in _ALLOWED_PLACE_ORDER_ACTIONS:
            logger.error(
                "Unrecognised action %r for instrument %d — "
                "only BUY/ADD are valid for place_order (EXIT routes to close_position)",
                action,
                instrument_id,
            )
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": f"Unrecognised action {action!r} for place_order"},
            )

        # Exactly one of amount/units must be provided and positive.
        if amount is not None and units is not None:
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": "Both amount and units provided — supply exactly one"},
            )
        if amount is None and units is None:
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": "Neither amount nor units provided"},
            )
        order_value = units if units is not None else amount
        if order_value is not None and order_value <= 0:
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": f"Order value must be positive, got {order_value}"},
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
            # units is None, and the guard above rejects both-None,
            # so amount is guaranteed non-None here.
            if amount is None:  # pragma: no cover — unreachable after guard
                raise RuntimeError("amount must be non-None when units is None")
            endpoint = f"{self._exec_prefix}/market-open-orders/by-amount"
            body = {
                "InstrumentID": instrument_id,
                "IsBuy": True,
                "Leverage": 1,
                "Amount": float(amount),
                "StopLossRate": None,
                "TakeProfitRate": None,
                "IsTslEnabled": False,
                "IsNoStopLoss": True,
                "IsNoTakeProfit": True,
            }

        try:
            response = self._http_write.post(
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
                raw_payload={"_ebull_action": action, "error": f"Network error: {exc}"},
            )
        except ValueError as exc:
            logger.error("eToro place_order non-JSON response: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"_ebull_action": action, "error": f"Non-JSON response: {exc}"},
            )

        # Preserve the domain action in raw_payload for audit trail.
        # eToro only has IsBuy — our BUY/ADD distinction is eBull-specific.
        raw["_ebull_action"] = action
        return _normalise_open_order_response(raw)

    def close_position(self, instrument_id: int) -> BrokerOrderResult:
        # Step 1: Resolve instrument_id → positionId via portfolio lookup.
        # The eToro close endpoint requires a positionId, not an instrumentId.
        # clientPortfolio.positions[] has both instrumentID and positionID.
        position_id, failure_reason = self._resolve_position_id(instrument_id)
        if position_id is None:
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": failure_reason},
            )

        # Step 2: Close the position.
        body: dict[str, Any] = {
            "InstrumentID": instrument_id,
            "UnitsToDeduct": None,  # close entire position
        }

        try:
            response = self._http_write.post(
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
                raw_payload={"error": f"Network error: {exc}"},
            )
        except ValueError as exc:
            logger.error("eToro close_position non-JSON response: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": f"Non-JSON response: {exc}"},
            )

        return _normalise_close_order_response(raw)

    def get_order_status(self, broker_order_ref: str) -> BrokerOrderResult:
        try:
            response = self._http_read.get(
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
                raw_payload={"error": f"Network error: {exc}"},
            )
        except ValueError as exc:
            logger.error("eToro get_order_status non-JSON response: %s", exc)
            return BrokerOrderResult(
                broker_order_ref=broker_order_ref,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={"error": f"Non-JSON response: {exc}"},
            )

        return _normalise_order_info_response(raw, broker_order_ref)

    # ------------------------------------------------------------------
    # Portfolio reads
    # ------------------------------------------------------------------

    def get_portfolio(self) -> BrokerPortfolio:
        """Fetch open positions and available cash from the eToro portfolio endpoint.

        Raises on HTTP or network errors (caller should handle).
        """
        response = self._http_read.get(
            f"{self._info_prefix}/portfolio",
            headers=self._request_headers(),
        )
        try:
            _persist_raw("etoro_portfolio", response.content)
        except OSError:
            logger.error(
                "Failed to persist raw portfolio payload — continuing with response",
                exc_info=True,
            )
        response.raise_for_status()
        raw = response.json()

        portfolio = raw.get("clientPortfolio") or {}
        raw_positions: list[dict[str, Any]] = portfolio.get("positions") or []
        credit = portfolio.get("credit")
        raw_mirrors: list[Any] = portfolio.get("mirrors") or []

        positions: list[BrokerPosition] = []
        for idx, pos in enumerate(raw_positions):
            if not isinstance(pos, dict):
                continue
            iid = pos.get("instrumentID")
            if iid is None:
                continue
            try:
                positions.append(_parse_direct_position(pos))
            except (KeyError, ValueError, TypeError, decimal.DecimalException) as exc:
                raise PortfolioParseError(f"Failed to parse position[{idx}] (instrument {iid}): {exc}") from exc

        return BrokerPortfolio(
            positions=positions,
            available_cash=Decimal(str(credit)) if credit is not None else Decimal("0"),
            raw_payload=raw,
            mirrors=tuple(_parse_mirrors_payload(raw_mirrors)),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_position_id(self, instrument_id: int) -> tuple[int | None, str]:
        """Look up the open positionID for an instrument via the portfolio endpoint.

        Returns (position_id, "") on success, or (None, reason) on failure.
        The reason distinguishes network/HTTP errors from missing positions.
        """
        try:
            response = self._http_read.get(
                f"{self._info_prefix}/portfolio",
                headers=self._request_headers(),
            )
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPStatusError as exc:
            raw_body = _safe_json(exc.response)
            logger.error(
                "eToro portfolio lookup failed: status=%d body=%s",
                exc.response.status_code,
                raw_body,
            )
            return None, f"Portfolio lookup failed: HTTP {exc.response.status_code}"
        except httpx.HTTPError as exc:
            logger.error("eToro portfolio lookup network error: %s", exc)
            return None, f"Portfolio lookup failed: {exc}"
        except ValueError as exc:
            logger.error("eToro portfolio non-JSON response: %s", exc)
            return None, "Portfolio lookup failed: non-JSON response"

        # Response shape: { clientPortfolio: { positions: [...] } }
        portfolio = raw.get("clientPortfolio") or {}
        positions: list[dict[str, Any]] = portfolio.get("positions") or []

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            if pos.get("instrumentID") == instrument_id:
                pos_id = pos.get("positionID")
                if pos_id is not None:
                    return int(pos_id), ""

        logger.warning(
            "No open position found for instrument %d in portfolio (%d positions checked)",
            instrument_id,
            len(positions),
        )
        return None, f"No open position found for instrument {instrument_id}"


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


def _parse_direct_position(payload: dict[str, Any]) -> BrokerPosition:
    """Parse a top-level portfolio position payload into BrokerPosition.

    Pure normaliser — no I/O, no instance state.  Follows the same
    pattern as ``_parse_mirror_position`` but for direct holdings.

    eToro's /portfolio endpoint returns ``openRate`` for the entry price
    and does NOT include a current price field.  We set current_price =
    open_price as a neutral placeholder so the PnL aggregation
    ``(current_price - open_price) * units`` evaluates to zero.  The
    portfolio API computes live unrealised PnL from the ``quotes`` table
    on read, so this placeholder is never surfaced to the dashboard.
    """

    def _opt_decimal(key: str) -> Decimal | None:
        value = payload.get(key)
        if value is None:
            return None
        return Decimal(str(value))

    open_rate = Decimal(str(payload["openRate"]))
    units = Decimal(str(payload["units"]))

    # initialAmountInDollars may be absent on very old positions;
    # fall back to amount, then to units * open_rate.
    raw_initial = payload.get("initialAmountInDollars")
    if raw_initial is not None:
        initial_amount = Decimal(str(raw_initial))
    else:
        raw_amount = payload.get("amount")
        initial_amount = Decimal(str(raw_amount)) if raw_amount is not None else units * open_rate

    return BrokerPosition(
        instrument_id=int(payload["instrumentID"]),
        units=units,
        open_price=open_rate,
        current_price=open_rate,
        raw_payload=payload,
        position_id=int(payload["positionID"]),
        is_buy=bool(payload.get("isBuy", True)),
        amount=Decimal(str(payload.get("amount", 0))),
        initial_amount_in_dollars=initial_amount,
        open_conversion_rate=Decimal(str(payload.get("openConversionRate", 1))),
        open_date_time=_parse_iso_datetime(payload["openDateTime"]) if "openDateTime" in payload else None,
        initial_units=_opt_decimal("initialUnits"),
        stop_loss_rate=_opt_decimal("stopLossRate"),
        take_profit_rate=_opt_decimal("takeProfitRate"),
        is_no_stop_loss=bool(payload.get("isNoStopLoss", True)),
        is_no_take_profit=bool(payload.get("isNoTakeProfit", True)),
        leverage=int(payload.get("leverage", 1)),
        is_tsl_enabled=bool(payload.get("isTslEnabled", False)),
        total_fees=Decimal(str(payload.get("totalFees", 0))),
    )


def _parse_mirror_position(payload: dict[str, Any]) -> BrokerMirrorPosition:
    """Parse a nested copy-mirror position payload into a typed dataclass.

    Pure normaliser — no I/O, no instance state. Required fields
    raise KeyError on absence; numeric fields go through
    Decimal(str(value)) and raise decimal.InvalidOperation
    (a subclass of decimal.DecimalException) on non-numeric input.
    The caller (_parse_mirror) wraps both exception types in a
    PortfolioParseError with position-index attribution.

    openConversionRate is required — see spec §2.2.2 and the
    74/198 non-USD positions on demo mirror 15712187 that would
    otherwise be AUM-nonsense.
    """

    def _opt_decimal(key: str) -> Decimal | None:
        value = payload.get(key)
        if value is None:
            return None
        return Decimal(str(value))

    return BrokerMirrorPosition(
        position_id=int(payload["positionID"]),
        parent_position_id=int(payload["parentPositionID"]),
        instrument_id=int(payload["instrumentID"]),
        is_buy=bool(payload["isBuy"]),
        units=Decimal(str(payload["units"])),
        amount=Decimal(str(payload["amount"])),
        initial_amount_in_dollars=Decimal(str(payload["initialAmountInDollars"])),
        open_rate=Decimal(str(payload["openRate"])),
        open_conversion_rate=Decimal(str(payload["openConversionRate"])),
        open_date_time=_parse_iso_datetime(payload["openDateTime"]),
        take_profit_rate=_opt_decimal("takeProfitRate"),
        stop_loss_rate=_opt_decimal("stopLossRate"),
        total_fees=Decimal(str(payload.get("totalFees", "0"))),
        leverage=int(payload.get("leverage", 1)),
        raw_payload=payload,
    )


def _parse_iso_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string from an eToro payload.

    eToro returns `2026-04-10T00:00:00Z`; Python's fromisoformat
    below 3.11 rejects the trailing `Z`, so we normalise to `+00:00`.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _parse_mirror(payload: dict[str, Any]) -> BrokerMirror:
    """Parse a top-level copy-trading mirror payload.

    Nested positions are iterated under an inner try/except that
    wraps (KeyError, ValueError, TypeError, decimal.DecimalException)
    in PortfolioParseError with mirror_id + position index
    attribution. See spec §2.2.2 for why the inner wrap is mandatory
    — without it, a single malformed nested position degrades to a
    top-level error message that cannot tell the operator *which*
    row failed.

    Top-level numeric/string extraction may also raise
    (KeyError / ValueError / TypeError / DecimalException); those
    propagate up to the outer get_portfolio loop where §2.2.2's
    fallback wrap catches and re-raises as PortfolioParseError
    keyed on the mirror_id alone.
    """
    raw_positions = payload.get("positions") or []
    parsed_positions: list[BrokerMirrorPosition] = []
    for idx, pos in enumerate(raw_positions):
        try:
            parsed_positions.append(_parse_mirror_position(pos))
        except (KeyError, ValueError, TypeError, decimal.DecimalException) as exc:
            raise PortfolioParseError(f"Mirror {payload.get('mirrorID')!r} position[{idx}]: {exc}") from exc

    def _opt_decimal(key: str) -> Decimal | None:
        value = payload.get(key)
        if value is None:
            return None
        return Decimal(str(value))

    def _opt_int(key: str) -> int | None:
        value = payload.get(key)
        if value is None:
            return None
        return int(value)

    return BrokerMirror(
        mirror_id=int(payload["mirrorID"]),
        parent_cid=int(payload["parentCID"]),
        parent_username=str(payload["parentUsername"]),
        initial_investment=Decimal(str(payload["initialInvestment"])),
        deposit_summary=Decimal(str(payload.get("depositSummary", "0"))),
        withdrawal_summary=Decimal(str(payload.get("withdrawalSummary", "0"))),
        available_amount=Decimal(str(payload["availableAmount"])),
        closed_positions_net_profit=Decimal(str(payload["closedPositionsNetProfit"])),
        stop_loss_percentage=_opt_decimal("stopLossPercentage"),
        stop_loss_amount=_opt_decimal("stopLossAmount"),
        mirror_status_id=_opt_int("mirrorStatusID"),
        mirror_calculation_type=_opt_int("mirrorCalculationType"),
        pending_for_closure=bool(payload.get("pendingForClosure", False)),
        started_copy_date=_parse_iso_datetime(payload["startedCopyDate"]),
        positions=tuple(parsed_positions),
        raw_payload=payload,
    )


def _parse_mirrors_payload(
    raw_mirrors: Sequence[Any],
) -> list[BrokerMirror]:
    """Parse clientPortfolio.mirrors[] into a list of BrokerMirror.

    Implements the outer top-level loop from spec §2.2.2:

    1. Rows that are not dicts, or dicts with no `mirrorID` key, are
       logged and skipped (the ONLY surviving log-and-skip path —
       they cannot collide with any known local row, so silent skip
       is safe).
    2. Rows with a recognisable `mirrorID` are parsed via
       `_parse_mirror`. Any failure raises PortfolioParseError —
       log-and-skip on a known mirror_id would look like a
       disappearance to §2.3.4's soft-close and silently destroy
       the local row.
    3. PortfolioParseError raised by the nested-position wrap inside
       `_parse_mirror` is re-raised unchanged so the caller sees the
       `position[idx]` attribution.
    4. Any other exception escaping `_parse_mirror` (KeyError,
       ValueError, TypeError, decimal.DecimalException) is
       fallback-wrapped in PortfolioParseError with mirror_id-only
       attribution.
    """
    mirrors: list[BrokerMirror] = []
    for m in raw_mirrors:
        if not isinstance(m, dict) or "mirrorID" not in m:
            logger.warning("Skipping unrecognisable mirrors[] element: %r", m)
            continue

        try:
            mirrors.append(_parse_mirror(m))
        except PortfolioParseError:
            raise
        except (KeyError, ValueError, TypeError, decimal.DecimalException) as exc:
            raise PortfolioParseError(f"Failed to parse mirror {m.get('mirrorID')!r}: {exc}") from exc
    return mirrors


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
