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
import hashlib
import json
import logging
import re
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import Any
from uuid import UUID, uuid4

import httpx

from app.config import settings
from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerClosedTrade,
    BrokerCloseOrderDetail,
    BrokerCoreOrder,
    BrokerCoreOrderSubmission,
    BrokerCostComponent,
    BrokerDirectPositionInvestment,
    BrokerEligibilityResponse,
    BrokerInstrumentEligibility,
    BrokerInstrumentInvestment,
    BrokerLeverageConfig,
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerOrderDetail,
    BrokerOrderLookupError,
    BrokerOrderNotFound,
    BrokerOrderResult,
    BrokerOrderSubmission,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerPortfolio,
    BrokerPosition,
    BrokerPositionCloseSubmission,
    BrokerPositionEditSubmission,
    BrokerPositionExecution,
    BrokerPositionMutationError,
    BrokerPositionMutationUncertain,
    BrokerProvider,
    BrokerStrategyOrder,
    BrokerWhatIfCostResponse,
    BrokerWhatIfOrder,
    OrderParams,
    OrderStatus,
)
from app.providers.implementations.etoro_quota_lanes import CALL_SITES, LANES, min_interval_for_stamps
from app.providers.implementations.etoro_request_log import attempt_observer
from app.providers.resilient_client import ResilientClient
from app.security.unattended_guard import refuse_broker_mutation_if_unattended

logger = logging.getLogger(__name__)


# Actions the service layer is allowed to send to place_order.
# EXIT is routed to close_position by the service layer and must never
# reach here. HOLD does not produce broker calls at all.
_ALLOWED_PLACE_ORDER_ACTIONS = frozenset({"BUY", "ADD"})

# 4xx codes that do NOT prove the submission failed. A request timeout, a
# conflict, a too-early and an exhausted-retry throttle all leave the order's
# fate unknown, so the caller must park the attempt rather than re-submit it
# (#2942). Every other 4xx is a rejection: the broker answered and said no.
_UNCERTAIN_4XX_STATUSES = frozenset({408, 409, 425, 429})


def _submission_outcome_is_uncertain(status_code: int) -> bool:
    """True when an HTTP status leaves an order-writing request unresolved."""
    return status_code >= 500 or status_code in _UNCERTAIN_4XX_STATUSES


def _exact_json_decimal(value: Decimal, field: str = "amount") -> float:
    """Return a JSON-native number only when its emitted decimal is unchanged.

    ``field`` names the offending value in the error.  It is not decoration: a core
    body now carries three Decimals and a message saying "amount" for a rejected
    ``stopLossRate`` would send the reader to the wrong one.
    """
    encoded = float(value)
    if Decimal(str(encoded)) != value:
        raise BrokerOrderSubmissionError(f"core order {field} cannot be represented exactly in the broker JSON payload")
    return encoded


# Map eToro statusID values to our OrderStatus.
# Populated from documented API responses. Edge-case status values
# may need live validation — unknown statuses default to "pending".
# eToro rate limits: 60 GET/min (read), 20 POST/min (write).
# Read: 1.1s interval ≈ 55/min (~8% headroom).
# Write: 3.5s interval ≈ 17/min (~15% headroom).
_ETORO_READ_INTERVAL_S = 1.1
_ETORO_WRITE_INTERVAL_S = 3.5

#: The lane-G trade-history call site, so the pacing below is DERIVED from the documented
#: budget rather than chosen next to it.  `etoro_quota_lanes` is a pure data module
#: (stdlib imports only) and is already an indirect import here via `etoro_request_log`.
#:
#: ⚠ Matched on (module, method) and required to be UNIQUE — unlike the lane-B lookup in
#: `strategy_core_eligibility`, which matches the method name alone.  `place_order`
#: already proves one method name can carry two `CallSite` rows, so a `next(...)` would
#: silently take the first of a future duplicate pair.
_HISTORY_CALL_SITES = [
    site
    for site in CALL_SITES
    if site.module == "app/providers/implementations/etoro_broker.py" and site.method == "get_trade_history"
]
if len(_HISTORY_CALL_SITES) != 1:  # pragma: no cover - a lane-map rename, caught at import
    raise RuntimeError(
        f"etoro_quota_lanes.CALL_SITES has {len(_HISTORY_CALL_SITES)} `get_trade_history` entries, "
        "expected exactly 1; _ETORO_HISTORY_INTERVAL_S cannot be derived from the documented budget"
    )
_HISTORY_CALL_SITE = _HISTORY_CALL_SITES[0]

# Minimum spacing between trade-history requests, in seconds -- DERIVED, not chosen.
#
# `get_trade_history` paginates in a `while True` loop, so one logical call can issue
# back-to-back requests; on `_http_read`'s 1.1s floor that is 54.5/min against a lane
# whose conservative reading is 20/min.  #2946 step 3 item 1 gave it its own client at
# this floor and deleted the `ACCEPTED_FLOOR_EXCEPTIONS` entry that recorded the gap.
#
# `min_interval_for_stamps` turns a budget into a spacing under the ROLLING-window
# counting rule -- `floor(60/i) + 1`, not `60/i`.  The `- 1` is one request of HEADROOM
# and is the one constructed choice here: lane G's membership is UNENUMERATED, and two
# of its known members draw on it OUTSIDE this client (`edit_demo_strategy_position` on
# `_http_write`, and the unthrottled `/api/v1/me` in `KNOWN_UNTHROTTLED`), so the one
# caller that can burst must not spend the last documented request.  60 / 18 = 3.33s.
#
# ⚠ The margin is a POLICY choice, not a derivation: nothing measures that one reserved
# request is enough to cover the rest of lane G.  It matches lane B so the two reserve
# alike.  See `docs/proposals/execution/2026-09-13-lane-g-history-floor.md`.
_ETORO_HISTORY_INTERVAL_S = min_interval_for_stamps(
    LANES[_HISTORY_CALL_SITE.lane].window_s,
    _HISTORY_CALL_SITE.conservative_per_minute - 1,
)

#: Per-phase HTTP timeout handed to ``httpx.Client``.
#:
#: ⚠ ``httpx`` applies this to connect, read, write and pool INDIVIDUALLY -- it is not a
#: total wall-clock cap on one attempt.  Named rather than inline because
#: ``strategy_core_broker_preflight`` derives its account-risk age bound from it, and a
#: coupling test asserts the two agree; an unnamed literal makes that derivation
#: unverifiable and lets a retune invalidate it silently.
_ETORO_HTTP_TIMEOUT_S = 30.0

_STATUS_MAP: dict[str, OrderStatus] = {
    "Executed": "filled",
    "Filled": "filled",
    "Pending": "pending",
    "Rejected": "rejected",
    "Failed": "failed",
    "Cancelled": "rejected",
}


#: A status that is only digits (optionally signed / decimal) is one of the
#: undocumented integer codes described below, whether the transport handed it
#: to us as an ``int`` or as a quoted string.
_NUMERIC_STATUS_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")


def _map_textual_order_status(raw_status: Any) -> OrderStatus:
    """Map a submit-ack order status, ABSTAINING on a numeric one.

    ``_STATUS_MAP`` keys are textual, but the contract types every ack status as
    an opaque integer with **no enum** and no code table --
    ``OrderForOpen.statusId``, ``OrderForClose.statusId``,
    ``OrderForCloseInfoResponse.statusID``
    (``tests/fixtures/etoro/openapi_v1.375.0.json``). A numeric status therefore
    cannot resolve to filled/rejected without inventing the mapping, so it
    resolves to ``pending``: acknowledged, outcome unknown.

    ⚠ The outcome belongs to ``get_demo_close_order``, which derives it from the
    response SHAPE (``errorCode`` -> rejected, non-empty ``positions[]`` ->
    filled) and keeps the integer as opaque evidence. Copy that, not a map.

    Rationale and the live evidence: #3007, #2961, and the "status typed as an
    opaque integer must abstain" entry in ``docs/review-prevention-log.md``.
    """
    if raw_status is None:
        return "pending"
    # `bool` needs no member here -- it subclasses `int`, so a JSON `true`
    # lands in this branch too, which is the right answer for it.
    if isinstance(raw_status, (int, float, Decimal)):
        return "pending"
    text = str(raw_status).strip()
    if _NUMERIC_STATUS_RE.match(text):
        return "pending"
    return _STATUS_MAP.get(text, "pending")


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


class TradeHistoryParseError(Exception):
    """Raised when a trade-history row cannot be parsed safely.

    Same design as PortfolioParseError: directly subclasses Exception
    so it is never swallowed by the incidental-exception handlers
    inside the parse loop.
    """


class ClosedPositionEventCountParseError(Exception):
    """Raised when a closed-position-event count row cannot be parsed safely."""


@dataclass(frozen=True)
class ClosedPositionEventCount:
    """One ``(closeYear, assetType)`` bucket from the closed-position-event counter.

    Provider-local rather than on ``BrokerProvider``: the endpoint is eToro's,
    has no env segment, and nothing in the domain layer consumes it (#2993).
    """

    close_year: int
    asset_type: str
    closed_position_events: int


class TradingPreflightParseError(Exception):
    """Raised when a trading preflight response cannot be trusted.

    Eligibility and cost data are safety inputs.  A partial best-effort parse
    could turn an absent restriction or fee into permission, so malformed
    responses fail the whole preflight instead of dropping fields or rows.
    """


class OrderDetailParseError(BrokerOrderLookupError):
    """The exact-order response cannot safely establish position identity."""


def _order_body_common(
    instrument_id: int,
    params: OrderParams | None,
) -> dict[str, Any]:
    """Build the common fields for an eToro open-order request body."""
    p = params or OrderParams()
    return {
        "InstrumentID": instrument_id,
        "IsBuy": True,  # v1 is long-only
        "Leverage": p.leverage,
        "StopLossRate": float(p.stop_loss_rate) if p.stop_loss_rate is not None else None,
        "TakeProfitRate": float(p.take_profit_rate) if p.take_profit_rate is not None else None,
        "IsTslEnabled": p.is_tsl_enabled,
        "IsNoStopLoss": p.stop_loss_rate is None,
        "IsNoTakeProfit": p.take_profit_rate is None,
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
            timeout=_ETORO_HTTP_TIMEOUT_S,
        )
        # Separate throttle rates for reads (GET 60/min) and writes (POST 20/min).
        # Both share the same _last_request_at timestamp so interleaved
        # GET+POST calls cannot exceed the API's combined rate limit.
        #
        # ⚠⚠ #2946: sharing the CLOCK without sharing the LOCK is the #726
        # check-and-write race, reintroduced across the two instances that share it.
        # `ResilientClient.__init__` states the contract (`resilient_client.py:86-95`):
        # *"providers sharing a `shared_last_request` list also need to share this lock
        # to keep the throttle atomic across instances"*.  Measured before the fix: four
        # concurrent acquisitions across the pair produced a gap of 0.000s against a
        # 0.5s floor, i.e. two requests fired at once.  Five sibling call sites already
        # pass the lock (`etoro.py:130`, `finra_regsho.py:87`,
        # `finra_short_interest.py:99`, `sec_edgar.py:315,323`); this was the only eToro
        # provider that did not.
        #
        # ⚠ Accepted cost: `_throttle_and_stamp` sleeps INSIDE the lock, so a write
        # holding it across its 3.5s sleep blocks a read that owed 1.1s -- up to 2.4s of
        # head-of-line blocking per write.  Deliberate: it is OVER-restriction (the safe
        # direction), writes are per-submission while reads are the frequent path, and a
        # 429's `Retry-After` costs more than 2.4s.  A reserve-then-sleep-outside gate
        # would remove the blocking and was REJECTED -- it lets a research burst book the
        # shared horizon arbitrarily far ahead, starving reconciliation behind the queue,
        # which is unbounded and strictly worse.  See the spec.
        shared_ts: list[float] = [0.0]
        shared_throttle_lock = threading.Lock()
        # #2946 step 3 item 3: per-ATTEMPT observers, wired at CONSTRUCTION.  Wrapping
        # `.get()` / `.post()` instead would count logical calls and miss every retry --
        # and a retry places its own stamp in a rolling-window quota, which is the
        # quantity the instrumentation exists to observe.
        self._http_read = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_READ_INTERVAL_S,
            shared_last_request=shared_ts,
            shared_throttle_lock=shared_throttle_lock,
            on_attempt=attempt_observer("broker_read", env),
        )
        self._http_write = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_WRITE_INTERVAL_S,
            shared_last_request=shared_ts,
            shared_throttle_lock=shared_throttle_lock,
            on_attempt=attempt_observer("broker_write", env),
        )
        # Trade history only (#2946 step 3 item 1).  A third client rather than a raised
        # read floor: `_http_read` also carries lanes D and E at 1.0s, and slowing order
        # lookups to 3.3s to pace a paginator would be a quota cost with no quota reason.
        #
        # ⚠ Joins the SAME clock and lock as the other two.  Its own clock would be a
        # second independent budget against one user key -- precisely the failure this
        # item exists to remove.  The lock bounds STAMP spacing, not dispatch: it is
        # released before `send()`, so two requests can be in flight at once.
        self._http_history = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_HISTORY_INTERVAL_S,
            shared_last_request=shared_ts,
            shared_throttle_lock=shared_throttle_lock,
            on_attempt=attempt_observer("broker_history", env),
        )

        # Environment-scoped path prefixes for trading endpoints.
        # Demo: /api/v1/trading/execution/demo/...
        # Real: /api/v1/trading/execution/...
        env_segment = f"/{env}" if env == "demo" else ""
        self._exec_prefix = f"/api/v1/trading/execution{env_segment}"
        self._info_prefix = f"/api/v1/trading/info{env_segment}"
        self._v2_info_prefix = f"/api/v2/trading/info/{env}"

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

    def _request_headers(self, request_id: UUID | None = None) -> dict[str, str]:
        """Return a caller-owned idempotency UUID or a fresh request identity."""
        return {"x-request-id": str(request_id or uuid4())}

    def _submit(
        self,
        *,
        operation: str,
        endpoint: str,
        body: dict[str, Any],
        request_id: UUID | None,
        normalise: Callable[[dict[str, Any]], BrokerOrderResult],
        extra_payload: dict[str, Any] | None = None,
    ) -> BrokerOrderResult:
        """POST one order-writing request and classify its outcome honestly.

        The distinction that matters is REJECTED vs UNCERTAIN (#2942). Before
        this existed both collapsed into ``status="failed"``, so a transport
        failure on an order that actually landed was booked as a terminal
        failure and its position went unowned.

        Uncertain: transport error, 5xx, and 408/409/425/429 -- a timeout, a
        conflict or an exhausted-retry throttle does not prove the request
        failed. Also any response we cannot parse, including a 200 whose body
        is not the documented object: we have no idea what happened.

        Rejected (today's ``status="failed"`` return): every other 4xx. The
        broker answered and the answer was no.

        ``ResilientClient`` retries 429/5xx with this same headers dict, so all
        retries inside one call share one ``x-request-id``.
        """
        extra = extra_payload or {}
        try:
            response = self._http_write.post(
                endpoint,
                json=body,
                headers=self._request_headers(request_id),
            )
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPStatusError as exc:
            error_body = _safe_json(exc.response)
            status_code = exc.response.status_code
            if _submission_outcome_is_uncertain(status_code):
                logger.error(
                    "eToro %s uncertain: status=%d body=%s",
                    operation,
                    status_code,
                    error_body,
                )
                raise BrokerOrderSubmissionUncertain(
                    f"{operation} returned HTTP {status_code} — outcome unknown, do not re-submit",
                    raw_payload={**extra, "http_status": status_code, **error_body},
                ) from exc
            logger.error("eToro %s failed: status=%d body=%s", operation, status_code, error_body)
            return BrokerOrderResult(
                broker_order_ref=None,
                status="failed",
                filled_price=None,
                filled_units=None,
                fees=Decimal("0"),
                raw_payload={**extra, **error_body},
            )
        except httpx.HTTPError as exc:
            logger.error("eToro %s network error: %s", operation, exc)
            raise BrokerOrderSubmissionUncertain(
                f"{operation} transport failure — outcome unknown, do not re-submit",
                raw_payload={**extra, "error": f"Network error: {exc}"},
            ) from exc
        except ValueError as exc:
            logger.error("eToro %s non-JSON response: %s", operation, exc)
            raise BrokerOrderSubmissionUncertain(
                f"{operation} returned a non-JSON response — outcome unknown, do not re-submit",
                raw_payload={**extra, "error": f"Non-JSON response: {exc}"},
            ) from exc

        if not isinstance(raw, dict):
            logger.error("eToro %s returned a non-object body: %r", operation, raw)
            raise BrokerOrderSubmissionUncertain(
                f"{operation} returned a non-object body — outcome unknown, do not re-submit",
                raw_payload={**extra, "raw_json": raw},
            )
        raw.update(extra)
        try:
            return normalise(raw)
        except (decimal.DecimalException, TypeError, ValueError, AttributeError) as exc:
            # A 200 we cannot read is not a success and not a rejection. The
            # order may well exist at the broker.
            logger.error("eToro %s response could not be normalised: %s", operation, exc)
            raise BrokerOrderSubmissionUncertain(
                f"{operation} response could not be normalised — outcome unknown, do not re-submit",
                raw_payload=raw,
            ) from exc

    # ------------------------------------------------------------------
    # BrokerProvider implementation
    # ------------------------------------------------------------------

    def place_order(
        self,
        instrument_id: int,
        action: str,
        amount: Decimal | None,
        units: Decimal | None,
        params: OrderParams | None = None,
        *,
        request_id: UUID | None = None,
    ) -> BrokerOrderResult:
        refuse_broker_mutation_if_unattended("place_order")
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
                **_order_body_common(instrument_id, params),
                "AmountInUnits": float(units),
            }
        else:
            # units is None, and the guard above rejects both-None,
            # so amount is guaranteed non-None here.
            if amount is None:  # pragma: no cover — unreachable after guard
                raise RuntimeError("amount must be non-None when units is None")
            endpoint = f"{self._exec_prefix}/market-open-orders/by-amount"
            body = {
                **_order_body_common(instrument_id, params),
                "Amount": float(amount),
            }

        # ``_ebull_action`` preserves the domain action in raw_payload for the
        # audit trail: eToro only has IsBuy — our BUY/ADD distinction is
        # eBull-specific. It is attached on every outcome, including uncertain.
        return self._submit(
            operation="place_order",
            endpoint=endpoint,
            body=body,
            request_id=request_id,
            normalise=_normalise_open_order_response,
            extra_payload={"_ebull_action": action},
        )

    def place_demo_strategy_order(
        self,
        order: BrokerStrategyOrder,
        *,
        request_id: UUID,
    ) -> BrokerOrderSubmission:
        """Submit the v2 paper-MVP order; no real endpoint can be selected.

        The generic v1 writer remains for manual behavior. Automated strategy
        code must call this method and cannot inherit ``self._env`` into a real
        path by mistake.
        """
        refuse_broker_mutation_if_unattended("place_demo_strategy_order")
        if self._env != "demo":
            raise BrokerOrderSubmissionError("strategy paper orders require demo credentials")
        body: dict[str, Any] = {
            "action": "open",
            "transaction": "buy",
            "instrumentId": order.instrument_id,
            "settlementType": "real",
            "orderType": "mkt",
            "leverage": 1,
            "amount": float(order.amount),
            "orderCurrency": "usd",
            "stopLossRate": float(order.stop_loss_rate),
            "takeProfitRate": float(order.take_profit_rate),
            "stopLossType": "fixed",
        }
        try:
            response = self._http_write.post(
                "/api/v2/trading/execution/demo/orders",
                json=body,
                headers=self._request_headers(request_id),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # The broker explicitly rejected a 4xx request. A 5xx may have
            # accepted it before failing, so only the latter is uncertain.
            if 400 <= exc.response.status_code < 500:
                raise BrokerOrderSubmissionError(
                    f"demo strategy order rejected with HTTP {exc.response.status_code}"
                ) from exc
            raise BrokerOrderSubmissionUncertain("demo strategy order returned a server error") from exc
        except httpx.HTTPError as exc:
            raise BrokerOrderSubmissionUncertain("demo strategy order transport failed") from exc
        try:
            raw = response.json()
        except ValueError as exc:
            raise BrokerOrderSubmissionUncertain("demo strategy order returned non-JSON data") from exc
        if not isinstance(raw, dict):
            raise BrokerOrderSubmissionUncertain("demo strategy order response must be an object")
        try:
            broker_order_id = int(raw["orderId"])
            reference_id = UUID(str(raw["referenceId"]))
            token = UUID(str(raw["token"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise BrokerOrderSubmissionUncertain("demo strategy order response identity is malformed") from exc
        if broker_order_id <= 0 or reference_id != request_id:
            raise BrokerOrderSubmissionUncertain("demo strategy order response identity does not match intent")
        return BrokerOrderSubmission(str(broker_order_id), reference_id, token)

    def place_demo_core_order(
        self,
        order: BrokerCoreOrder,
        *,
        request_id: UUID,
    ) -> BrokerCoreOrderSubmission:
        """Submit one demo-only underlying core buy, carrying its stop and target.

        ⚠⚠ This docstring used to read "with no SL/TP fields", and that was the whole
        of #3284: a position opened here was naked until some later cycle noticed.
        The operator's decision (2026-09-21) is a stop and target at ALL times, so the
        rates are part of the submitted body and :class:`BrokerCoreOrder` requires them.

        ⚠ ``stopLossType: "fixed"`` is sent explicitly rather than left to the broker's
        default.  Precedent is ``place_demo_strategy_order`` above, which posts the same
        ``settlementType: "real"`` body to the SAME endpoint with the same three fields
        -- so this is our own measured field set for this endpoint, not an inference.
        The core stop is fixed by definition: it is a function of the entry price and
        does not trail.
        """
        refuse_broker_mutation_if_unattended("place_demo_core_order")
        if self._env != "demo":
            raise BrokerOrderSubmissionError("core orders require demo credentials")
        body: dict[str, Any] = {
            "action": "open",
            "transaction": "buy",
            "instrumentId": order.instrument_id,
            "settlementType": "real",
            "orderType": "mkt",
            "leverage": 1,
            "amount": _exact_json_decimal(order.amount),
            "orderCurrency": "usd",
            "stopLossRate": _exact_json_decimal(order.stop_loss_rate, "stopLossRate"),
            "takeProfitRate": _exact_json_decimal(order.take_profit_rate, "takeProfitRate"),
            "stopLossType": "fixed",
        }
        try:
            response = self._http_write.post(
                "/api/v2/trading/execution/demo/orders",
                json=body,
                headers=self._request_headers(request_id),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500:
                raise BrokerOrderSubmissionError(
                    f"demo core order rejected with HTTP {exc.response.status_code}"
                ) from exc
            raise BrokerOrderSubmissionUncertain("demo core order returned a server error") from exc
        except httpx.HTTPError as exc:
            raise BrokerOrderSubmissionUncertain("demo core order transport failed") from exc
        try:
            raw = response.json()
        except ValueError as exc:
            raise BrokerOrderSubmissionUncertain("demo core order returned non-JSON data") from exc
        if not isinstance(raw, dict):
            raise BrokerOrderSubmissionUncertain("demo core order response must be an object")
        try:
            broker_order_id = int(raw["orderId"])
            reference_id = UUID(str(raw["referenceId"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise BrokerOrderSubmissionUncertain("demo core order response identity is malformed") from exc
        if broker_order_id <= 0 or reference_id != request_id:
            raise BrokerOrderSubmissionUncertain("demo core order response identity does not match intent")
        try:
            canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise BrokerOrderSubmissionUncertain("demo core order response cannot be fingerprinted") from exc
        return BrokerCoreOrderSubmission(
            broker_order_ref=str(broker_order_id),
            reference_id=reference_id,
            response_digest=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )

    def close_position(
        self,
        position_id: int,
        units_to_deduct: Decimal | None = None,
        *,
        instrument_id: int | None = None,
        request_id: UUID | None = None,
    ) -> BrokerOrderResult:
        """Close one position by broker position id.

        ``InstrumentID`` is documented REQUIRED on the close body
        (api-reference/trading--demo/close-demo-position-by-units, re-read
        2026-09-13); it was previously omitted entirely, so every close would
        have been rejected (#2942).
        """
        refuse_broker_mutation_if_unattended("close_position")
        if instrument_id is None:
            raise ValueError("close_position requires instrument_id — eToro documents InstrumentID as required")
        body: dict[str, Any] = {
            "InstrumentID": instrument_id,
            "UnitsToDeduct": float(units_to_deduct) if units_to_deduct is not None else None,
        }
        return self._submit(
            operation="close_position",
            endpoint=f"{self._exec_prefix}/market-close-orders/positions/{position_id}",
            body=body,
            request_id=request_id,
            normalise=_normalise_close_order_response,
        )

    def edit_demo_strategy_position(
        self,
        *,
        position_id: int,
        stop_loss_rate: Decimal,
        take_profit_rate: Decimal | None,
        request_id: UUID,
        persist_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> BrokerPositionEditSubmission:
        """Strict v2 adapter for automated demo-only position edits."""
        refuse_broker_mutation_if_unattended("edit_demo_strategy_position")
        if self._env != "demo":
            raise BrokerPositionMutationError("strategy position edits require demo credentials")
        if position_id <= 0 or stop_loss_rate <= 0:
            raise BrokerPositionMutationError("position id and stop rate must be positive")
        body: dict[str, Any] = {
            "stopLossRate": float(stop_loss_rate),
            "stopLossType": "fixed",
        }
        if take_profit_rate is not None:
            if take_profit_rate <= 0:
                raise BrokerPositionMutationError("take-profit rate must be positive")
            body["takeProfitRate"] = float(take_profit_rate)
        raw: dict[str, Any] | None = None
        try:
            response = self._http_write.patch(
                f"/api/v2/trading/demo/positions/{position_id}",
                json=body,
                headers=self._request_headers(request_id),
            )
            raw = _safe_json(response)
            if persist_response is not None:
                persist_response(raw)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            error_raw = raw or _safe_json(exc.response)
            if 400 <= exc.response.status_code < 500:
                raise BrokerPositionMutationError(
                    f"demo position edit rejected with HTTP {exc.response.status_code}",
                    raw_payload=error_raw,
                ) from exc
            raise BrokerPositionMutationUncertain(
                "demo position edit returned a server error", raw_payload=error_raw
            ) from exc
        except httpx.HTTPError as exc:
            raise BrokerPositionMutationUncertain("demo position edit transport failed") from exc
        assert raw is not None
        try:
            operation_id = UUID(str(raw["operationId"]))
            returned_position_id = int(raw["positionId"])
            reference_id = UUID(str(raw["referenceId"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise BrokerPositionMutationUncertain(
                "demo position edit response identity is malformed", raw_payload=raw
            ) from exc
        if returned_position_id != position_id or reference_id != request_id:
            raise BrokerPositionMutationUncertain(
                "demo position edit response identity does not match intent", raw_payload=raw
            )
        return BrokerPositionEditSubmission(operation_id, returned_position_id, reference_id, raw)

    def close_demo_strategy_position(
        self,
        *,
        position_id: int,
        instrument_id: int,
        request_id: UUID,
        persist_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> BrokerPositionCloseSubmission:
        """Strict v1 adapter for an exact, whole demo-position close."""
        refuse_broker_mutation_if_unattended("close_demo_strategy_position")
        if self._env != "demo":
            raise BrokerPositionMutationError("strategy position closes require demo credentials")
        if position_id <= 0 or instrument_id <= 0:
            raise BrokerPositionMutationError("position and instrument ids must be positive")
        raw: dict[str, Any] | None = None
        try:
            response = self._http_write.post(
                f"/api/v1/trading/execution/demo/market-close-orders/positions/{position_id}",
                json={"InstrumentID": instrument_id, "UnitsToDeduct": None},
                headers=self._request_headers(request_id),
            )
            raw = _safe_json(response)
            if persist_response is not None:
                persist_response(raw)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            error_raw = raw or _safe_json(exc.response)
            if 400 <= exc.response.status_code < 500:
                raise BrokerPositionMutationError(
                    f"demo position close rejected with HTTP {exc.response.status_code}",
                    raw_payload=error_raw,
                ) from exc
            raise BrokerPositionMutationUncertain(
                "demo position close returned a server error", raw_payload=error_raw
            ) from exc
        except httpx.HTTPError as exc:
            raise BrokerPositionMutationUncertain("demo position close transport failed") from exc
        assert raw is not None
        try:
            order = raw["orderForClose"]
            broker_order_ref = int(order["orderID"])
            returned_position_id = int(order["positionID"])
        except (KeyError, TypeError, ValueError) as exc:
            raise BrokerPositionMutationUncertain(
                "demo position close response identity is malformed", raw_payload=raw
            ) from exc
        if broker_order_ref <= 0 or returned_position_id != position_id:
            raise BrokerPositionMutationUncertain(
                "demo position close response identity does not match intent", raw_payload=raw
            )
        return BrokerPositionCloseSubmission(str(broker_order_ref), returned_position_id, raw)

    def get_demo_close_order(
        self,
        *,
        order_id: str,
        persist_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> BrokerCloseOrderDetail:
        """Resolve a close order without inferring success from disappearance."""
        if self._env != "demo" or not order_id.isdigit() or int(order_id) <= 0:
            raise BrokerPositionMutationError("valid demo close-order identity is required")
        raw: dict[str, Any] | None = None
        try:
            response = self._http_read.get(
                f"/api/v1/trading/info/demo/close-orders/{order_id}",
                headers=self._request_headers(),
            )
            raw = _safe_json(response)
            if persist_response is not None:
                persist_response(raw)
            response.raise_for_status()
            returned_order_id = int(raw["orderID"])
            raw_positions = raw.get("positions") or []
            if not isinstance(raw_positions, list):
                raise TypeError("positions must be a list")
            if any(not isinstance(row, dict) for row in raw_positions):
                raise TypeError("every affected position must be an object")
            position_ids = tuple(int(row["positionID"]) for row in raw_positions)
            error_code = raw.get("errorCode")
            raw_status = str(raw.get("statusID", "unknown"))
            reference = raw.get("referenceID")
            reference_id = UUID(str(reference)) if reference else None
        except httpx.HTTPStatusError as exc:
            raise BrokerPositionMutationError(
                f"demo close-order lookup failed with HTTP {exc.response.status_code}",
                raw_payload=raw or _safe_json(exc.response),
            ) from exc
        except httpx.HTTPError as exc:
            raise BrokerPositionMutationUncertain("demo close-order lookup transport failed") from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise BrokerPositionMutationUncertain(
                "demo close-order lookup response is malformed", raw_payload=raw
            ) from exc
        assert raw is not None
        if returned_order_id != int(order_id):
            raise BrokerPositionMutationUncertain("demo close-order lookup identity does not match", raw_payload=raw)
        status: OrderStatus
        if error_code not in (None, 0, "0"):
            status = "rejected"
        elif position_ids:
            status = "filled"
        else:
            status = "pending"
        return BrokerCloseOrderDetail(order_id, status, raw_status, position_ids, reference_id, raw)

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

    def lookup_order(
        self,
        *,
        order_id: str | None = None,
        reference_id: str | None = None,
    ) -> BrokerOrderDetail:
        """Resolve v2 order detail and every exact position execution.

        eToro requires exactly one of ``orderId`` or ``referenceId``. The
        latter is the X-Request-Id used for the idempotent submission, so it
        closes the response-before-persist crash gap.
        """
        if (order_id is None) == (reference_id is None):
            raise ValueError("exactly one of order_id or reference_id is required")
        if order_id is not None:
            try:
                numeric_order_id = int(order_id)
            except ValueError as exc:
                raise ValueError("order_id must be a positive integer") from exc
            if numeric_order_id <= 0:
                raise ValueError("order_id must be a positive integer")
            params: dict[str, str | int] = {"orderId": numeric_order_id}
        else:
            assert reference_id is not None
            try:
                canonical_reference = str(UUID(reference_id))
            except ValueError as exc:
                raise ValueError("reference_id must be a UUID") from exc
            params = {"referenceId": canonical_reference}

        try:
            response = self._http_read.get(
                f"{self._v2_info_prefix}/orders:lookup",
                params=params,
                headers=self._request_headers(),
            )
            response.raise_for_status()
            raw = response.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise BrokerOrderNotFound("broker order was not found") from exc
            raise BrokerOrderLookupError(f"broker order lookup returned HTTP {exc.response.status_code}") from exc
        except httpx.HTTPError as exc:
            raise BrokerOrderLookupError(f"broker order lookup transport error: {exc}") from exc
        except ValueError as exc:
            raise BrokerOrderLookupError("broker order lookup returned non-JSON data") from exc
        if not isinstance(raw, dict):
            raise OrderDetailParseError("order detail response must be an object")
        return _parse_order_detail(raw, reference_id=reference_id)

    def check_instrument_eligibility(
        self,
        instrument_ids: Sequence[int],
    ) -> BrokerEligibilityResponse:
        """Call the current v2 account-specific eligibility endpoint.

        This is intentionally a non-persisting capability slice.  The caller
        decides whether an observed response is worth storing after coverage,
        change frequency, and byte cost have been measured.
        """
        ids = tuple(instrument_ids)
        if not ids:
            raise ValueError("instrument_ids must not be empty")
        if len(ids) > 100:
            raise ValueError("eToro eligibility accepts at most 100 instruments")
        if any(instrument_id <= 0 for instrument_id in ids):
            raise ValueError("instrument_ids must all be positive")
        if len(set(ids)) != len(ids):
            raise ValueError("instrument_ids must not contain duplicates")

        response = self._http_write.post(
            f"{self._v2_info_prefix}/eligibility",
            json={"instrumentIds": list(ids), "currency": "USD"},
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict):
            raise TradingPreflightParseError("eligibility response must be an object")
        return _parse_eligibility_response(raw)

    def get_what_if_costs(self, order: BrokerWhatIfOrder) -> BrokerWhatIfCostResponse:
        """Call the current v2 what-if endpoint without placing an order.

        ⚠ Informational on BOTH arms.  Pricing a close is not closing anything -- this is
        ``/trading/info/{demo/}costs``, whose whole contract is "what would this cost";
        the close endpoint is a different path entirely.  That is why this method is not
        covered by ``refuse_broker_mutation_if_unattended`` and must not become so: #2645's
        other half was that ruling informational work out was itself the error.
        """
        body: dict[str, Any] = {
            "action": order.action,
            "transaction": order.transaction,
            "instrumentId": order.instrument_id,
            "settlementType": order.settlement_type,
            "orderType": order.order_type,
            "leverage": order.leverage,
            "orderCurrency": order.order_currency.lower(),
        }
        if order.position_ids:
            # Required by the close arm, rejected on the open arm; `BrokerWhatIfOrder`
            # enforces both directions, so the presence of the tuple IS the arm.
            body["positionIds"] = list(order.position_ids)
        if order.amount is not None:
            body["amount"] = float(order.amount)
        else:
            body["units"] = float(order.units) if order.units is not None else None

        response = self._http_write.post(
            f"{self._v2_info_prefix}/costs",
            json=body,
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict):
            raise TradingPreflightParseError("what-if cost response must be an object")
        return _parse_what_if_cost_response(raw)

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

    def get_account_risk_snapshot(self) -> BrokerAccountRiskSnapshot:
        """Fetch and strictly derive the official demo P&L risk totals."""
        if self._env != "demo":
            raise TradingPreflightParseError("strategy account risk requires demo credentials")
        response = self._http_read.get(
            "/api/v1/trading/info/demo/pnl",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict):
            raise TradingPreflightParseError("account P&L response must be an object")
        return _parse_account_risk_snapshot(raw, observed_at=datetime.now(UTC))

    def get_trade_history(self, min_date: datetime, page_size: int = 200) -> Sequence[BrokerClosedTrade]:
        """Fetch all closed-trade rows with closeTimestamp >= min_date.

        The endpoint filters on the CLOSE timestamp (empirically probed,
        #1593 spec §0) and paginates offset-style; we loop while pages
        come back full, collecting everything before returning so the
        service layer can group slices per position.

        ⚠ Every page goes through ``_http_history``, NOT ``_http_read`` — the
        loop is the only place in this provider where one logical call issues
        unbounded back-to-back requests, and lane G's conservative reading is
        20/min (#2946 step 3 item 1).  A page that switched clients would pace
        the first request and nothing after it.

        ⚠ Returns the WHOLE window, never a truncated prefix: ledger §4's
        synthesized-open transform sums the slices of a never-seen position and
        is correct only because all of them are in the batch.  A page cap here
        would be a ledger-completeness decision, not a quota one.

        ⚠⚠ The DATE range is deliberately not windowed, and #2991 is where that
        was decided against the document rather than from first principles.  The
        operation asks callers to keep each request's lookback under a year and
        to "advance `minDate` per batch" for longer history; it exposes no
        upper-bound date parameter, so each request runs to the present and
        advancing `minDate` only shrinks the result.  A deep backfill from
        `HISTORY_EPOCH` therefore cannot comply, and was observed to be served
        anyway.  Do not add windowing here without first re-pinning the spec —
        `tests/test_2991_history_lookback_contract.py` fails if `maxDate` ever
        appears, which is the signal that it has become buildable.

        Env segment placement differs from the other info endpoints:
        /api/v1/trading/info/trade/demo/history (demo) vs
        /api/v1/trading/info/trade/history (real).

        Raises on HTTP or network errors (caller should handle).
        """
        env_segment = "/demo" if self._env == "demo" else ""
        path = f"/api/v1/trading/info/trade{env_segment}/history"

        trades: list[BrokerClosedTrade] = []
        page = 1
        while True:
            response = self._http_history.get(
                path,
                params={
                    "minDate": min_date.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "page": str(page),
                    "pageSize": str(page_size),
                },
                headers=self._request_headers(),
            )
            response.raise_for_status()
            try:
                rows = response.json()
            except ValueError as exc:  # 200 with a non-JSON body
                raise TradeHistoryParseError(f"trade history page {page}: response body is not JSON: {exc}") from exc
            if not isinstance(rows, list):
                raise TradeHistoryParseError(
                    f"trade history page {page}: expected a JSON array, got {type(rows).__name__}"
                )
            for idx, row in enumerate(rows):
                if not isinstance(row, dict):
                    raise TradeHistoryParseError(f"trade history page {page} row {idx}: expected an object")
                try:
                    trades.append(_parse_closed_trade(row))
                except (KeyError, ValueError, TypeError, decimal.DecimalException) as exc:
                    raise TradeHistoryParseError(
                        f"trade history page {page} row {idx} (position {row.get('positionId')}): {exc}"
                    ) from exc
            if len(rows) < page_size:
                return trades
            page += 1

    def get_closed_position_event_counts(self) -> tuple[ClosedPositionEventCount, ...]:
        """Closed-position event counts per (close year, asset type).

        ⚠⚠ NOT an environment-scoped figure, and not yet established to be one
        that can be compared against ``get_trade_history``.  The operation takes
        no ``env`` segment and no parameters: it answers for the caller's own
        gateway-issued GCID, which spans the real and demo customer ids that
        ``/api/v1/me`` reports separately.  On the dev demo account it returns
        counts across years in which this account did not exist.  #2993 holds
        the undetermined-scope evidence and the experiment that would settle it;
        until it does, do NOT read this as a completeness oracle for the ledger.

        Exists so that reading it is METERED.  ``etoro_quota_lanes.CALL_SITES``
        records the endpoints this repo reaches, and
        ``etoro_request_log`` classifies an observed request by matching that
        table — a caller reaching this path outside the provider is recorded as
        ``unclassified`` and its draw on lane G goes uncounted.

        Raises on HTTP or network errors (caller should handle).
        """
        response = self._http_read.get(
            "/api/v1/data/positions/closed-events/history",
            headers=self._request_headers(),
        )
        response.raise_for_status()
        try:
            rows = response.json()
        except ValueError as exc:  # 200 with a non-JSON body
            raise ClosedPositionEventCountParseError(f"response body is not JSON: {exc}") from exc
        if not isinstance(rows, list):
            raise ClosedPositionEventCountParseError(f"expected a JSON array, got {type(rows).__name__}")
        counts: list[ClosedPositionEventCount] = []
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                raise ClosedPositionEventCountParseError(f"row {idx}: expected an object")
            try:
                events = int(row["closedPositionEvents"])
                # A count cannot be negative. This is the field's definition, not a
                # chosen bound — and without it a negative parses as a valid figure and
                # drags a total (#2993's comparison) toward agreement.
                if events < 0:
                    raise ValueError(f"closedPositionEvents must be >= 0, got {events}")
                counts.append(
                    ClosedPositionEventCount(
                        close_year=int(row["closeYear"]),
                        asset_type=str(row["assetType"]),
                        closed_position_events=events,
                    )
                )
            except (KeyError, ValueError, TypeError) as exc:
                raise ClosedPositionEventCountParseError(f"row {idx}: {exc}") from exc
        return tuple(counts)


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
    """Normalise an eToro close-order ACKNOWLEDGEMENT to BrokerOrderResult.

    ⚠ This is an acknowledgement, not a fill. Measured live on demo (#3007,
    attended session 2026-09-22) -- the verbatim body of a full close::

        {"orderForClose": {"positionID": 3602456774, "instrumentID": 3434,
                           "orderID": 383127337, "orderType": 19,
                           "statusID": 1, "CID": 20661956,
                           "openDateTime": "2026-09-22T14:45:00.6711552Z",
                           "lastUpdate": "2026-09-22T14:45:00.6711552Z"},
         "token": "..."}

    No ``executionPrice``, no ``units``, no ``unitsToDeduct`` (we send
    ``UnitsToDeduct: None`` for a full close), and a numeric ``statusID``. So
    the result is always ``status='pending'`` with ``filled_price`` and
    ``filled_units`` at ``None``, and ``execute_order``'s fill guard correctly
    persists nothing.

    The outcome is resolved by looking the order up afterwards -- see
    ``get_demo_close_order``, which derives status from the response SHAPE and
    is the only path allowed to declare a close filled. ⚠ A 404 there does NOT
    mean the broker never received it: the same order id 404'd immediately
    after submission and reported ``broker_status=3`` with the affected
    position seconds later (#2961). The legacy EXIT path in
    ``app/services/order_client.py`` does not yet call it -- that completion
    path is the open half of #3007.
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


def _required_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise TradingPreflightParseError(f"{key} must be a boolean")
    return value


def _optional_decimal(payload: dict[str, Any], key: str) -> Decimal | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except decimal.DecimalException as exc:
        raise TradingPreflightParseError(f"{key} must be numeric or null") from exc


def _parse_eligibility_response(raw: dict[str, Any]) -> BrokerEligibilityResponse:
    """Strictly parse the documented v2 eligibility response."""
    currency = raw.get("currency")
    rows = raw.get("eligibilities")
    missing_ids = raw.get("notFoundInstrumentIds")
    missing_symbols = raw.get("notFoundSymbols")
    if not isinstance(currency, str) or not currency:
        raise TradingPreflightParseError("eligibility currency must be a non-empty string")
    if not isinstance(rows, list) or not isinstance(missing_ids, list) or not isinstance(missing_symbols, list):
        raise TradingPreflightParseError("eligibility response arrays are missing or malformed")

    parsed: list[BrokerInstrumentEligibility] = []
    try:
        for row in rows:
            if not isinstance(row, dict):
                raise TradingPreflightParseError("eligibilities entries must be objects")
            leverage_rows = row.get("leverageConfigs")
            if not isinstance(leverage_rows, list):
                raise TradingPreflightParseError("leverageConfigs must be an array")
            leverage_configs: list[BrokerLeverageConfig] = []
            for config in leverage_rows:
                if not isinstance(config, dict):
                    raise TradingPreflightParseError("leverageConfigs entries must be objects")
                values = config.get("leverageValues")
                if not isinstance(values, list) or any(not isinstance(value, int) for value in values):
                    raise TradingPreflightParseError("leverageValues must be an integer array")
                allow_edit_stop_loss = config.get("allowEditStopLoss")
                allow_edit_take_profit = config.get("allowEditTakeProfit")
                allow_stop_loss_take_profit = config.get("allowStopLossTakeProfit")
                leverage_configs.append(
                    BrokerLeverageConfig(
                        settlement_type=str(config["settlementType"]),
                        direction=str(config["direction"]),
                        leverage_values=tuple(values),
                        min_position_amount=_optional_decimal(config, "minPositionAmount"),
                        allow_edit_stop_loss=allow_edit_stop_loss if isinstance(allow_edit_stop_loss, bool) else None,
                        allow_edit_take_profit=allow_edit_take_profit
                        if isinstance(allow_edit_take_profit, bool)
                        else None,
                        allow_stop_loss_take_profit=allow_stop_loss_take_profit
                        if isinstance(allow_stop_loss_take_profit, bool)
                        else None,
                        raw_payload=dict(config),
                    )
                )
            symbol = row.get("symbol")
            if symbol is not None and not isinstance(symbol, str):
                raise TradingPreflightParseError("symbol must be a string or null")
            parsed.append(
                BrokerInstrumentEligibility(
                    instrument_id=int(row["instrumentId"]),
                    symbol=symbol,
                    min_position_exposure=_optional_decimal(row, "minPositionExposure"),
                    max_units_per_order=_optional_decimal(row, "maxUnitsPerOrder"),
                    allow_open_position=_required_bool(row, "allowOpenPosition"),
                    allow_close_position=_required_bool(row, "allowClosePosition"),
                    allow_partial_close_position=_required_bool(row, "allowPartialClosePosition"),
                    allow_trailing_stop_loss=_required_bool(row, "allowTrailingStopLoss"),
                    leverage_configs=tuple(leverage_configs),
                    raw_payload=dict(row),
                )
            )
        return BrokerEligibilityResponse(
            currency=currency,
            eligibilities=tuple(parsed),
            not_found_instrument_ids=tuple(int(value) for value in missing_ids),
            not_found_symbols=tuple(str(value) for value in missing_symbols),
            raw_payload=raw,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TradingPreflightParseError(f"malformed eligibility response: {exc}") from exc


def _parse_what_if_cost_response(raw: dict[str, Any]) -> BrokerWhatIfCostResponse:
    """Strictly parse the documented v2 cost response without closing its vocabulary."""
    rows = raw.get("costs")
    if not isinstance(rows, list):
        raise TradingPreflightParseError("costs must be an array")
    symbol = raw.get("symbol")
    if symbol is not None and not isinstance(symbol, str):
        raise TradingPreflightParseError("symbol must be a string or null")
    try:
        costs: list[BrokerCostComponent] = []
        for row in rows:
            if not isinstance(row, dict):
                raise TradingPreflightParseError("cost entries must be objects")
            cost_type = row["costType"]
            currency = row["currency"]
            if not isinstance(cost_type, str) or not cost_type or not isinstance(currency, str) or not currency:
                raise TradingPreflightParseError("cost type and currency must be non-empty strings")
            amount = _optional_decimal(row, "amount")
            value = _optional_decimal(row, "value")
            if amount is None and value is None:
                raise TradingPreflightParseError("cost row must carry amount or value")
            costs.append(
                BrokerCostComponent(
                    cost_type=cost_type,
                    amount=amount,
                    value=value,
                    currency=currency,
                    raw_payload=dict(row),
                )
            )
        last_updated = datetime.fromisoformat(str(raw["lastUpdated"]).replace("Z", "+00:00"))
        if last_updated.tzinfo is None:
            raise TradingPreflightParseError("lastUpdated must include a timezone")
        return BrokerWhatIfCostResponse(
            instrument_id=int(raw["instrumentId"]),
            symbol=symbol,
            costs=tuple(costs),
            last_updated=last_updated,
            raw_payload=raw,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TradingPreflightParseError(f"malformed what-if cost response: {exc}") from exc


def _order_detail_decimal(payload: dict[str, Any], key: str) -> Decimal | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except decimal.DecimalException as exc:
        raise OrderDetailParseError(f"{key} must be numeric or null") from exc


def _order_detail_time(payload: dict[str, Any], key: str) -> datetime | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise OrderDetailParseError(f"{key} must be an ISO timestamp or null") from exc
    if parsed.tzinfo is None:
        raise OrderDetailParseError(f"{key} must include a timezone")
    return parsed


def _parse_order_detail(raw: dict[str, Any], *, reference_id: str | None) -> BrokerOrderDetail:
    """Strictly parse the documented v2 ``orders:lookup`` response."""
    try:
        order_id = int(raw["orderId"])
        status_object = raw["status"]
        asset = raw["asset"]
        execution_rows = raw["positionExecutions"]
        if order_id <= 0:
            raise OrderDetailParseError("orderId must be positive")
        if not isinstance(status_object, dict) or not isinstance(asset, dict):
            raise OrderDetailParseError("status and asset must be objects")
        broker_status = status_object.get("name")
        if not isinstance(broker_status, str) or not broker_status:
            raise OrderDetailParseError("status.name must be a non-empty string")
        instrument_id = int(asset["instrumentId"])
        if instrument_id <= 0:
            raise OrderDetailParseError("asset.instrumentId must be positive")
        if not isinstance(execution_rows, list):
            raise OrderDetailParseError("positionExecutions must be an array")

        executions: list[BrokerPositionExecution] = []
        position_ids: set[int] = set()
        for row in execution_rows:
            if not isinstance(row, dict):
                raise OrderDetailParseError("positionExecutions entries must be objects")
            position_id = int(row["positionId"])
            state = row["state"]
            if position_id <= 0 or position_id in position_ids:
                raise OrderDetailParseError("positionExecutions positionId values must be positive and unique")
            if not isinstance(state, str) or not state:
                raise OrderDetailParseError("positionExecutions state must be non-empty")
            position_ids.add(position_id)
            opening = row.get("openingData")
            if opening is not None and not isinstance(opening, dict):
                raise OrderDetailParseError("positionExecutions openingData must be an object or null")
            opening_data = opening or {}
            remaining_units = _order_detail_decimal(row, "remainingUnits")
            opening_units = _order_detail_decimal(opening_data, "units")
            average_price = _order_detail_decimal(opening_data, "avgPrice")
            execution_time = _order_detail_time(opening_data, "executionTime")
            fees = _order_detail_decimal(opening_data, "fees")
            if opening_units is None or opening_units <= 0:
                raise OrderDetailParseError("openingData.units must be positive for a position execution")
            if average_price is None or average_price <= 0:
                raise OrderDetailParseError("openingData.avgPrice must be positive for a position execution")
            if execution_time is None:
                raise OrderDetailParseError("openingData.executionTime is required for a position execution")
            if remaining_units is None or remaining_units < 0:
                raise OrderDetailParseError("remainingUnits must be non-negative for a position execution")
            if fees is None or fees < 0:
                raise OrderDetailParseError("openingData.fees must be non-negative for a position execution")
            executions.append(
                BrokerPositionExecution(
                    position_id=position_id,
                    state=state,
                    remaining_units=remaining_units,
                    opening_units=opening_units,
                    average_price=average_price,
                    execution_time=execution_time,
                    fees=fees,
                    raw_payload=dict(row),
                )
            )

        normalised_status: OrderStatus = _STATUS_MAP.get(broker_status, "pending")
        return BrokerOrderDetail(
            broker_order_ref=str(order_id),
            reference_id=reference_id,
            status=normalised_status,
            broker_status=broker_status,
            instrument_id=instrument_id,
            position_executions=tuple(executions),
            last_update=_order_detail_time(raw, "lastUpdate"),
            raw_payload=raw,
        )
    except OrderDetailParseError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise OrderDetailParseError(f"malformed order detail response: {exc}") from exc


def _parse_account_risk_snapshot(
    raw: dict[str, Any],
    *,
    observed_at: datetime,
) -> BrokerAccountRiskSnapshot:
    """Apply eToro's published cash/invested/equity formulas exactly.

    The live v1 endpoint wraps the formula inputs in ``clientPortfolio``.
    Keep that envelope strict so an HTTP-200 error or a future response-shape
    change cannot be mistaken for a zero/healthy account.
    """

    def _array(parent: dict[str, Any], key: str) -> list[Any]:
        value = parent.get(key)
        if not isinstance(value, list):
            raise TradingPreflightParseError(f"account P&L {key} must be an array")
        return value

    def _row(value: Any, label: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise TradingPreflightParseError(f"account P&L {label} row must be an object")
        return value

    def _money(parent: dict[str, Any], key: str) -> Decimal:
        if key not in parent or isinstance(parent[key], bool):
            raise TradingPreflightParseError(f"account P&L {key} is required")
        try:
            value = Decimal(str(parent[key]))
        except decimal.DecimalException as exc:
            raise TradingPreflightParseError(f"account P&L {key} must be numeric") from exc
        if not value.is_finite():
            raise TradingPreflightParseError(f"account P&L {key} must be finite")
        return value

    def _instrument_id(parent: dict[str, Any]) -> int:
        documented = parent.get("instrumentId")
        legacy = parent.get("instrumentID")
        if documented is not None and legacy is not None and documented != legacy:
            raise TradingPreflightParseError("account P&L instrument id aliases disagree")
        raw_id = documented if documented is not None else legacy
        if raw_id is None:
            raise TradingPreflightParseError("account P&L instrument id is required")
        try:
            value = int(raw_id)
        except (TypeError, ValueError) as exc:
            raise TradingPreflightParseError("account P&L instrument id is required") from exc
        if value <= 0:
            raise TradingPreflightParseError("account P&L instrument id must be positive")
        return value

    def _position_id(parent: dict[str, Any]) -> int:
        def validated(value: object) -> int:
            # The portal schema says integer. Do not let bool or int(1.2) become
            # an exact ownership identity by Python coercion.
            if isinstance(value, bool) or not isinstance(value, int):
                raise TradingPreflightParseError("account P&L position id must be an integer")
            if value <= 0 or value > 9_223_372_036_854_775_807:
                raise TradingPreflightParseError("account P&L position id is outside signed BIGINT range")
            return value

        has_documented = "positionId" in parent
        has_legacy = "positionID" in parent
        if not has_documented and not has_legacy:
            raise TradingPreflightParseError("account P&L position id must be an integer")
        documented = validated(parent["positionId"]) if has_documented else None
        legacy = validated(parent["positionID"]) if has_legacy else None
        if documented is not None and legacy is not None and documented != legacy:
            raise TradingPreflightParseError("account P&L position id aliases disagree")
        if documented is not None:
            return documented
        assert legacy is not None
        return legacy

    def _position_units(row: dict[str, Any]) -> Decimal:
        """Read the position's CURRENT quantity, failing closed on anything unusable.

        The portal documents ``units`` on ``clientPortfolio.positions[]`` -- "Number of
        units in the position" -- and the same ``TradingRealAdminApi_Position`` schema
        backs both the portfolio and the P&L response, so it is present on this payload.

        ⚠ This fails closed rather than carrying ``None`` like ``_account_currency_id``
        does, and the difference is deliberate: an open position with no positive
        quantity is not a state this endpoint can legitimately report, and the value is
        a DIVISOR for the derived mark (#3068). A zero or absent one would either raise
        deep inside a consumer or -- worse -- be quietly coerced and produce a mark that
        looks like an observation. ``_instrument_id`` already fails closed on the same
        "documented, required, must be positive" grounds.
        """
        if "units" not in row:
            raise TradingPreflightParseError("account P&L position units is required")
        # ⚠ Separate branch from absence on purpose: a present-but-bool `units` is
        # response drift, not an omission, and `Decimal(str(True))` is `Decimal('True')`
        # -- which raises anyway, but under a message that would send a reader looking
        # for a missing field. `bool` is a subclass of `int`, so it must be excluded
        # before any numeric coercion.
        if isinstance(row["units"], bool):
            raise TradingPreflightParseError("account P&L position units must be numeric")
        try:
            value = Decimal(str(row["units"]))
        except decimal.DecimalException as exc:
            raise TradingPreflightParseError("account P&L position units must be numeric") from exc
        if not value.is_finite():
            raise TradingPreflightParseError("account P&L position units must be finite")
        if value <= 0:
            raise TradingPreflightParseError("account P&L position units must be positive")
        return value

    def _pnl_operand(pnl_row: dict[str, Any], key: str) -> Decimal:
        """Read a positive finite ``unrealizedPnL`` operand, failing closed (#3068).

        ``closeRate`` and ``closeConversionRate`` are comparand operands -- the v2
        reconciliation multiplies our units by the first and the result by the second --
        so they get exactly the treatment ``units`` gets, and for the same reason: a
        defaulted or coerced operand produces a number that looks like an observation.
        ``bool`` is excluded before any numeric coercion because it is an ``int``
        subclass, and a present-but-bool value is response drift rather than an omission.
        """
        if key not in pnl_row:
            raise TradingPreflightParseError(f"account P&L position {key} is required")
        if isinstance(pnl_row[key], bool):
            raise TradingPreflightParseError(f"account P&L position {key} must be numeric")
        try:
            value = Decimal(str(pnl_row[key]))
        except decimal.DecimalException as exc:
            raise TradingPreflightParseError(f"account P&L position {key} must be numeric") from exc
        if not value.is_finite():
            raise TradingPreflightParseError(f"account P&L position {key} must be finite")
        if value <= 0:
            raise TradingPreflightParseError(f"account P&L position {key} must be positive")
        return value

    def _asset_currency_id(pnl_row: dict[str, Any]) -> int:
        """Read the currency the close rate is quoted in, failing closed (#3068).

        Unlike the ACCOUNT currency id -- which is carried as ``None`` so that its
        absence cannot take the paper executor's cash checks down -- this one is a
        precondition of using ``closeRate`` at all: a price whose currency is unknown
        cannot be compared against a local close whose currency is known. Refusing the
        parse is narrower than it looks, because the whole object is absent on the
        non-P&L endpoint and present-and-populated on this one.
        """
        if "assetCurrencyId" not in pnl_row:
            raise TradingPreflightParseError("account P&L position assetCurrencyId is required")
        value = pnl_row["assetCurrencyId"]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TradingPreflightParseError("account P&L position assetCurrencyId must be an integer")
        if value <= 0:
            raise TradingPreflightParseError("account P&L position assetCurrencyId must be positive")
        return value

    def _pnl_timestamp(pnl_row: dict[str, Any]) -> datetime | None:
        """When the broker struck this mark, or ``None``. Never raises (#3068).

        ⚠ Deliberately the ONLY lenient field in this row. It is evidence and not an
        operand -- no verdict reads it -- so failing the whole snapshot over an
        unparseable timestamp would lose the comparand to protect a diagnostic. Absence
        is logged nowhere per-position on purpose: the value is identical across
        positions in every observed response, so a per-row warning would be noise.
        """
        raw = pnl_row.get("timestamp")
        if not isinstance(raw, str):
            return None
        try:
            parsed = _parse_iso_datetime(raw)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

    def _is_partially_altered(row: dict[str, Any]) -> bool:
        if "isPartiallyAltered" not in row:
            raise TradingPreflightParseError("account P&L position isPartiallyAltered is required")
        value = row["isPartiallyAltered"]
        if not isinstance(value, bool):
            raise TradingPreflightParseError("account P&L position isPartiallyAltered must be a boolean")
        return value

    def _is_buy(row: dict[str, Any]) -> bool:
        """Read a direct position's DIRECTION, failing closed on absence.

        The portal documents ``isBuy`` on ``clientPortfolio.positions[]`` as
        "true for long (buy) positions, false for short (sell) positions", and
        it was present on 7/7 positions of the live demo account (2026-08-14).
        ⚠ All seven were ``true``, so the short branch is specified and
        unit-tested but UNOBSERVED.

        This fails closed where ``_account_currency_id`` deliberately does not,
        and the difference is not inconsistency: that field is not a formula
        input, whereas direction decides which side of the sleeve a position
        lands on.  Defaulting it would silently book a short as a long.
        """
        if "isBuy" not in row:
            raise TradingPreflightParseError("account P&L position isBuy is required")
        value = row["isBuy"]
        if not isinstance(value, bool):
            raise TradingPreflightParseError("account P&L position isBuy must be a boolean")
        return value

    def _account_currency_id(parent: dict[str, Any]) -> int | None:
        """Read the reported account currency id, or None when absent.

        The portal documents ``accountCurrencyId`` on this response ("Currency
        ID of the account (1 = USD)"), so absence is drift worth logging -- but
        it is not a formula input, and failing the whole parse over it would
        take the paper executor's cash checks down with it.  Absence is carried
        as None and refused where it matters, at the F-0 evidence writer
        (#2602 item 2).  A PRESENT but malformed value is response drift of the
        kind sql/324's fail-closed posture exists for, so that does raise.
        """
        if "accountCurrencyId" not in parent:
            logger.warning(
                "account P&L response carried no accountCurrencyId; "
                "the account base currency is unobserved for this snapshot"
            )
            return None
        value = parent["accountCurrencyId"]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TradingPreflightParseError("account P&L accountCurrencyId must be an integer")
        return value

    try:
        portfolio = raw.get("clientPortfolio")
        if not isinstance(portfolio, dict):
            raise TradingPreflightParseError("account P&L clientPortfolio must be an object")
        account_currency_id = _account_currency_id(portfolio)
        credit = _money(portfolio, "credit")
        positions = _array(portfolio, "positions")
        mirrors = _array(portfolio, "mirrors")
        open_orders = _array(portfolio, "ordersForOpen")
        orders = _array(portfolio, "orders")
        investments: dict[int, Decimal] = {}
        # The DIRECT-position half, kept apart from `investments` on purpose: the
        # total-invested formula folds mirrors and pending orders in, and a core
        # sleeve is neither (#2704).
        direct_long_value: dict[int, Decimal] = {}
        direct_long_count: dict[int, int] = {}
        direct_short_count: dict[int, int] = {}
        direct_positions: list[BrokerDirectPositionInvestment] = []
        direct_position_ids: set[int] = set()
        total_invested = Decimal("0")
        unrealized = Decimal("0")

        for item in positions:
            row = _row(item, "positions")
            amount = _money(row, "amount")
            pnl_row = _row(row.get("unrealizedPnL"), "positions.unrealizedPnL")
            pnl = _money(pnl_row, "pnL")
            instrument_id = _instrument_id(row)
            position_id = _position_id(row)
            if position_id in direct_position_ids:
                raise TradingPreflightParseError("account P&L direct position ids must be unique")
            direct_position_ids.add(position_id)
            is_buy = _is_buy(row)
            is_partially_altered = _is_partially_altered(row)
            units = _position_units(row)
            market_value = amount + pnl
            direct_positions.append(
                BrokerDirectPositionInvestment(
                    position_id=position_id,
                    instrument_id=instrument_id,
                    is_buy=is_buy,
                    units=units,
                    amount=amount,
                    unrealized_pnl=pnl,
                    market_value=market_value,
                    is_partially_altered=is_partially_altered,
                    close_rate=_pnl_operand(pnl_row, "closeRate"),
                    close_conversion_rate=_pnl_operand(pnl_row, "closeConversionRate"),
                    asset_currency_id=_asset_currency_id(pnl_row),
                    pnl_timestamp=_pnl_timestamp(pnl_row),
                )
            )
            total_invested += amount
            unrealized += pnl
            investments[instrument_id] = investments.get(instrument_id, Decimal("0")) + amount
            if is_buy:
                # `amount + pnL` is this position's contribution to the equity
                # identity, and for a long holding that IS its market value --
                # cross-checked against our independent quote feed (#2704).
                direct_long_value[instrument_id] = direct_long_value.get(instrument_id, Decimal("0")) + market_value
                direct_long_count[instrument_id] = direct_long_count.get(instrument_id, 0) + 1
            else:
                direct_short_count[instrument_id] = direct_short_count.get(instrument_id, 0) + 1

        for item in mirrors:
            mirror = _row(item, "mirrors")
            closed_profit = _money(mirror, "closedPositionsNetProfit")
            total_invested += _money(mirror, "availableAmount") - closed_profit
            unrealized += closed_profit
            for item_position in _array(mirror, "positions"):
                row = _row(item_position, "mirrors.positions")
                amount = _money(row, "amount")
                pnl = _money(_row(row.get("unrealizedPnL"), "mirrors.positions.unrealizedPnL"), "pnL")
                instrument_id = _instrument_id(row)
                total_invested += amount
                unrealized += pnl
                investments[instrument_id] = investments.get(instrument_id, Decimal("0")) + amount

        pending_amount = Decimal("0")
        for item in open_orders:
            row = _row(item, "ordersForOpen")
            mirror_id = int(row.get("mirrorID", row.get("mirrorId", 0)))
            if mirror_id != 0:
                continue
            amount = _money(row, "amount")
            external = _money(row, "totalExternalCosts")
            instrument_id = _instrument_id(row)
            pending_amount += amount
            total_invested += amount + external
            investments[instrument_id] = investments.get(instrument_id, Decimal("0")) + amount + external

        for item in orders:
            row = _row(item, "orders")
            amount = _money(row, "amount")
            instrument_id = _instrument_id(row)
            pending_amount += amount
            total_invested += amount
            investments[instrument_id] = investments.get(instrument_id, Decimal("0")) + amount

        available_cash = credit - pending_amount
        equity = available_cash + total_invested + unrealized
        if available_cash < 0 or total_invested < 0 or equity <= 0 or any(value < 0 for value in investments.values()):
            raise TradingPreflightParseError("account P&L derived risk totals are outside safe bounds")
        return BrokerAccountRiskSnapshot(
            available_cash=available_cash,
            total_invested=total_invested,
            unrealized_pnl=unrealized,
            equity=equity,
            instrument_investments=tuple(
                BrokerInstrumentInvestment(
                    instrument_id,
                    amount,
                    direct_long_value.get(instrument_id, Decimal("0")),
                    direct_long_count.get(instrument_id, 0),
                    direct_short_count.get(instrument_id, 0),
                )
                for instrument_id, amount in sorted(investments.items())
            ),
            direct_positions=tuple(direct_positions),
            observed_at=observed_at,
            raw_payload=raw,
            account_currency_id=account_currency_id,
            pending_order_amount=pending_amount,
        )
    except TradingPreflightParseError:
        raise
    except (KeyError, TypeError, ValueError, decimal.DecimalException) as exc:
        raise TradingPreflightParseError(f"malformed account P&L response: {exc}") from exc


def _required_position_bool(payload: dict[str, Any], key: str) -> bool:
    """A boolean the position cannot be stored without. Raises rather than defaults.

    ``bool()`` on a JSON value is a coercion, not a read: it turns ``0``, ``""`` and
    ``None`` into ``False`` and every other value into ``True``, so a payload that sent
    the wrong TYPE would be stored as a confident direction. #3068.

    ⚠ Raises ``ValueError``, NOT ``TradingPreflightParseError``, so this is deliberately
    not the module's existing ``_required_bool``. ``get_portfolio`` wraps
    ``(KeyError, ValueError, TypeError, DecimalException)`` into a ``PortfolioParseError``
    carrying the position index and instrument id; a ``TradingPreflightParseError`` is
    none of those, so it would escape that wrap, lose the attribution, and surface a
    preflight-shaped error out of a portfolio sync.
    """
    if key not in payload:
        raise ValueError(f"position {key} is required")
    value = payload[key]
    if not isinstance(value, bool):
        raise ValueError(f"position {key} must be a boolean")
    return value


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
        # ⚠⚠ FAILS CLOSED, and used to not (#3068). This read `bool(payload.get("isBuy",
        # True))`: an ABSENT direction became LONG, and `bool()` coerced any truthy value
        # into one. A default is not a measurement -- and `broker_positions.is_buy` is
        # what the end-of-day valuation signs its mark-to-market with, and what the v2
        # reconciliation compares against the broker's OWN `isBuy` as a two-endpoint
        # check. That check is vacuous if one side can be a default.
        #
        # `_parse_account_risk_snapshot._is_buy` already refuses on the same grounds
        # ("defaulting it would silently book a short as a long"); this is the sibling
        # that was missed. `isBuy` is documented on the shared `TradingRealAdminApi_
        # Position` schema and was present on 7/7 live demo positions (2026-09-15), so
        # this cannot fire on a well-formed payload.
        is_buy=_required_position_bool(payload, "isBuy"),
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
        settlement_type_id=_opt_settlement_type_id(payload),
    )


#: An integral decimal string, with optional sign and no fractional part.
#: `settlementTypeID` is an enumeration, so "1.5" is malformed input, not a
#: value to round (Codex ckpt-2, #2602 item 3).
_INTEGRAL_STRING = re.compile(r"-?\d+")

#: Range of `broker_positions.settlement_type_id` (SMALLINT, `sql/367`). Mirrored
#: here because the parser is the only writer on the live path, and handing the
#: upsert an out-of-range value trades one unreadable label for the entire sync.
_SMALLINT_MIN = -32768
_SMALLINT_MAX = 32767


def _opt_settlement_type_id(payload: dict[str, Any]) -> int | None:
    """``settlementTypeID`` as an int, or ``None`` if absent/unreadable (#2602 item 3).

    Never raises. This is evidence about the position, not a field the position
    depends on: refusing to parse the whole position because its investment type
    is malformed would lose the holding itself over a label. ``bool`` is excluded
    explicitly — it is an ``int`` subclass in Python, so ``isinstance(True, int)``
    would quietly store ``1`` ("Real Asset") for a payload that sent ``true``.

    ⚠ Only INTEGRAL values are accepted, because this is an enumeration and
    ``int()`` truncates. A payload sending ``1.5`` is malformed, and coercing it
    would present it to the operator as ``1`` — "Real Asset", an ownership claim
    the broker never made. Unreadable must stay unreadable (Codex ckpt-2).
    ``1.0`` is admitted: JSON has one number type, so a whole value can legally
    arrive as a float.

    ⚠⚠ Values outside ``SMALLINT`` are dropped, because the storage column is
    ``SMALLINT`` (``sql/367``). Returning one would push the overflow down into
    ``_upsert_broker_positions``, where it aborts the WHOLE portfolio sync — every
    position lost over one unreadable label, which is precisely the outcome the
    first paragraph promises not to have. The migration's backfill already
    range-guards for the same reason; this is its runtime twin (Codex ckpt-2).
    """
    value = payload.get("settlementTypeID")
    if value is None or isinstance(value, bool):
        return None
    parsed: int | None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        parsed = int(value) if value.is_integer() else None
    elif isinstance(value, str) and _INTEGRAL_STRING.fullmatch(value.strip()):
        parsed = int(value)
    else:
        parsed = None
    if parsed is None or not (_SMALLINT_MIN <= parsed <= _SMALLINT_MAX):
        return None
    return parsed


def _parse_closed_trade(payload: dict[str, Any]) -> BrokerClosedTrade:
    """Parse one trade-history row into BrokerClosedTrade.

    Pure normaliser — no I/O, no instance state. Required fields
    (positionId, instrumentId, units, openTimestamp, closeTimestamp)
    raise KeyError on absence; numerics go through Decimal(str(value)).
    Optional ids: eToro uses 0 as the "none" sentinel for
    socialTradeId / parentPositionId / orderId — preserved as-is here
    (the service layer decides sentinel semantics; raw_payload keeps
    the evidence either way).
    """

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

    return BrokerClosedTrade(
        position_id=int(payload["positionId"]),
        instrument_id=int(payload["instrumentId"]),
        is_buy=bool(payload.get("isBuy", True)),
        units=Decimal(str(payload["units"])),
        open_rate=_opt_decimal("openRate"),
        open_timestamp=_parse_iso_datetime(payload["openTimestamp"]),
        close_rate=_opt_decimal("closeRate"),
        close_timestamp=_parse_iso_datetime(payload["closeTimestamp"]),
        net_profit=_opt_decimal("netProfit"),
        fees=_opt_decimal("fees"),
        investment=_opt_decimal("investment"),
        initial_investment=_opt_decimal("initialInvestment"),
        leverage=int(payload.get("leverage", 1)),
        order_id=_opt_int("orderId"),
        social_trade_id=_opt_int("socialTradeId"),
        parent_position_id=_opt_int("parentPositionId"),
        raw_payload=payload,
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
    status: OrderStatus = _map_textual_order_status(order_data.get("statusID"))

    filled_price: Decimal | None = None
    filled_units: Decimal | None = None
    fees = Decimal("0")

    # ⚠ `units` only. A close ack may carry `unitsToDeduct`, which the contract
    # describes as "the number of units closed in this order" -- but it is the
    # deduction we ASKED for, echoed back before anything executed: the live
    # ack's `openDateTime` equals its `lastUpdate`, and the same order was
    # still a 404 on the lookup route seconds later (#2961). Reading it as
    # `filled_units` books a fill the broker has not made. Executed units come
    # from the lookup, never from the acknowledgement.
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
        value = response.json()
        return dict(value) if isinstance(value, dict) else {"raw_json": value}
    except Exception:
        return {"raw_text": response.text}
