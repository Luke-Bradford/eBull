# Order Entry with SL/TP — Phase 2a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the operator to place orders with stop-loss/take-profit parameters and close individual positions by position_id, with full audit trail and kill-switch enforcement.

**Architecture:** Extend `BrokerProvider.place_order()` to accept optional SL/TP parameters; change `close_position(instrument_id)` to `close_position(position_id)` for per-position granularity. Add two REST endpoints (`POST /portfolio/orders`, `POST /portfolio/positions/{position_id}/close`) that enforce kill-switch, config flags, and validation before calling the broker. On fill, write to both `broker_positions` and `positions` tables. Frontend modals are Phase 2b (separate plan).

**Tech Stack:** Python 3.14, FastAPI, psycopg3, pydantic, pytest, httpx (eToro HTTP client)

**Settled decisions preserved:**
- Provider design rule: providers are thin adapters — no DB lookups, no domain logic
- Safety invariant: `close_position()` ONLY via operator UI or EXIT recommendation
- Kill switch checked before any order
- `enable_live_trading` checked — demo mode produces synthetic fills
- Guard auditability: one `decision_audit` row per order attempt

**Prevention log entries respected:**
- Read-then-write in same transaction (position update after fill)
- Missing data on hard-rule path fails closed (no quote → reject, not guess)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `app/providers/broker.py` | Modify | Add `OrderParams` dataclass; update `place_order()` and `close_position()` signatures |
| `app/providers/implementations/etoro_broker.py` | Modify | Wire SL/TP params to eToro API body; change `close_position` to accept `position_id` |
| `app/api/orders.py` | Create | Two endpoints: place order + close position — validation, guards, broker call, persistence |
| `app/main.py` | Modify | Register `orders_router` |
| `tests/test_broker_provider.py` | Modify | Update `place_order` and `close_position` call signatures in existing tests |
| `tests/test_orders_api.py` | Create | REST endpoint tests for both order and close flows |
| `tests/test_order_client.py` | Modify | Update `close_position` mock signature |

---

### Task 1: Extend BrokerProvider interface with SL/TP and position-level close

**Files:**
- Modify: `app/providers/broker.py:24-68` (add `OrderParams` dataclass)
- Modify: `app/providers/broker.py:138-158` (update method signatures)
- Test: `tests/test_broker_provider.py` (update call sites)

- [ ] **Step 1: Add `OrderParams` dataclass to `app/providers/broker.py`**

Add after the `BrokerOrderResult` dataclass (after line 34):

```python
@dataclass(frozen=True)
class OrderParams:
    """Optional parameters for order placement.

    All fields are optional — omitting them preserves the current
    behaviour (no SL, no TP, leverage 1).
    """

    stop_loss_rate: Decimal | None = None
    take_profit_rate: Decimal | None = None
    is_tsl_enabled: bool = False
    leverage: int = 1
```

- [ ] **Step 2: Update `place_order` signature**

Change the abstract method at `app/providers/broker.py:138-151`:

```python
@abstractmethod
def place_order(
    self,
    instrument_id: int,
    action: str,
    amount: Decimal | None,
    units: Decimal | None,
    params: OrderParams | None = None,
) -> BrokerOrderResult:
    """
    Place an order with the broker.

    Exactly one of amount or units should be provided.
    params: optional SL/TP and leverage settings. None = broker defaults.
    Returns the broker's response, including fill details if immediately filled.
    """
```

- [ ] **Step 3: Update `close_position` signature**

Change the abstract method at `app/providers/broker.py:153-158`:

```python
@abstractmethod
def close_position(
    self,
    position_id: int,
    units_to_deduct: Decimal | None = None,
) -> BrokerOrderResult:
    """
    Close an existing position by broker position ID.

    units_to_deduct: if provided, partial close. None = close entire position.
    Returns the broker's response with fill details.
    """
```

- [ ] **Step 4: Update all callers of `close_position` in order_client.py**

In `app/services/order_client.py:513-514`, the EXIT path calls `broker.close_position(instrument_id)`. This needs to resolve `instrument_id → position_id` before calling the broker. Add a helper and update the call:

```python
# Add near top of file, after _load_cash:
def _load_position_id_for_exit(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> int | None:
    """Return the broker position_id for an instrument, or None if not found.

    For EXIT via recommendation, the instrument may have multiple broker_positions.
    We close the oldest (earliest open_date_time) — this matches FIFO semantics.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT position_id FROM broker_positions
            WHERE instrument_id = %(iid)s AND units > 0
            ORDER BY open_date_time ASC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return int(row["position_id"])
```

Update the live-mode EXIT call at line ~513:

```python
if action == "EXIT":
    exit_position_id = _load_position_id_for_exit(conn, instrument_id)
    if exit_position_id is None:
        # No broker_position found — fall back to instrument-level
        # close for backwards compat with pre-024 positions.
        broker_result = broker.close_position(instrument_id)
    else:
        broker_result = broker.close_position(exit_position_id)
```

Wait — this breaks the interface. The old `close_position(instrument_id)` accepted instrument_id; the new one accepts position_id. The order_client is the only live-mode caller. Let me reconsider.

**Revised approach:** Keep the interface change clean. The `order_client` EXIT path already loads position data. After the interface change, it passes position_id. For the backwards-compat case (no broker_positions rows), we fall back to the old _resolve_position_id path inside the eToro provider.

Actually, the cleanest approach: `close_position(position_id)` always takes a position_id. The caller is responsible for resolving it. For the recommendation-driven EXIT path, `order_client` resolves from `broker_positions`. For the UI-driven close, the frontend already has the `position_id`.

```python
# In order_client.py, replace the EXIT broker call:
if action == "EXIT":
    exit_pos_id = _load_position_id_for_exit(conn, instrument_id)
    if exit_pos_id is None:
        # Pre-024 position without broker_positions row.
        # Fail — cannot close without a position_id.
        logger.error(
            "EXIT for instrument_id=%d: no broker_positions row found",
            instrument_id,
        )
        broker_result = BrokerOrderResult(
            broker_order_ref=None,
            status="failed",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"error": f"No broker_positions row for instrument {instrument_id}"},
        )
    else:
        broker_result = broker.close_position(exit_pos_id)
```

- [ ] **Step 5: Run tests to see what breaks**

Run: `uv run pytest tests/test_order_client.py tests/test_broker_provider.py -v`
Expected: Some failures where `close_position` is called with `instrument_id` — update those mocks.

- [ ] **Step 6: Fix broken test mocks**

In any test that mocks `broker.close_position(instrument_id=...)`, change to `broker.close_position(position_id=...)` and ensure the test sets up `broker_positions` data or mocks `_load_position_id_for_exit`.

- [ ] **Step 7: Run all tests**

Run: `uv run pytest -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add app/providers/broker.py app/services/order_client.py tests/
git commit -m "feat: extend BrokerProvider with OrderParams and position-level close"
```

---

### Task 2: Wire SL/TP params in EtoroBrokerProvider

**Files:**
- Modify: `app/providers/implementations/etoro_broker.py:169-302` (place_order)
- Modify: `app/providers/implementations/etoro_broker.py:304-369` (close_position)
- Test: `tests/test_broker_provider.py`

- [ ] **Step 1: Write failing test — SL/TP flows to request body**

In `tests/test_broker_provider.py`, add:

```python
def test_place_order_passes_sl_tp_to_request_body(mock_http_write, provider):
    """SL/TP params appear in the eToro request body."""
    mock_http_write.post.return_value = _mock_open_order_response()

    params = OrderParams(
        stop_loss_rate=Decimal("140.00"),
        take_profit_rate=Decimal("200.00"),
        is_tsl_enabled=True,
        leverage=2,
    )
    provider.place_order(
        instrument_id=1,
        action="BUY",
        amount=Decimal("100"),
        units=None,
        params=params,
    )

    call_kwargs = mock_http_write.post.call_args
    body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
    assert body["StopLossRate"] == 140.00
    assert body["TakeProfitRate"] == 200.00
    assert body["IsTslEnabled"] is True
    assert body["Leverage"] == 2
    assert body["IsNoStopLoss"] is False
    assert body["IsNoTakeProfit"] is False
```

- [ ] **Step 2: Run test — verify it fails**

Run: `uv run pytest tests/test_broker_provider.py::test_place_order_passes_sl_tp_to_request_body -v`
Expected: FAIL (params kwarg not accepted or SL/TP still hard-coded)

- [ ] **Step 3: Update `place_order` in EtoroBrokerProvider**

In `app/providers/implementations/etoro_broker.py`, update the method signature and body construction:

```python
def place_order(
    self,
    instrument_id: int,
    action: str,
    amount: Decimal | None,
    units: Decimal | None,
    params: OrderParams | None = None,
) -> BrokerOrderResult:
```

Replace the hard-coded SL/TP block in both the by-units and by-amount body dicts. Extract a helper:

```python
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
```

Then in place_order:
```python
if units is not None:
    endpoint = f"{self._exec_prefix}/market-open-orders/by-units"
    body = {**_order_body_common(instrument_id, params), "AmountInUnits": float(units)}
else:
    endpoint = f"{self._exec_prefix}/market-open-orders/by-amount"
    body = {**_order_body_common(instrument_id, params), "Amount": float(amount)}
```

- [ ] **Step 4: Update `close_position` in EtoroBrokerProvider**

Change signature and remove `_resolve_position_id` call:

```python
def close_position(
    self,
    position_id: int,
    units_to_deduct: Decimal | None = None,
) -> BrokerOrderResult:
    body: dict[str, Any] = {
        "UnitsToDeduct": float(units_to_deduct) if units_to_deduct is not None else None,
    }

    try:
        response = self._http_write.post(
            f"{self._exec_prefix}/market-close-orders/positions/{position_id}",
            json=body,
            headers=self._request_headers(),
        )
        # ... rest unchanged
```

Remove the `InstrumentID` from the body (the eToro endpoint identifies the position via the URL path). Keep `_resolve_position_id` as a private helper — it's no longer called from `close_position` but may be useful for other lookups.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_broker_provider.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add app/providers/implementations/etoro_broker.py app/providers/broker.py tests/test_broker_provider.py
git commit -m "feat: wire SL/TP params and position-level close to eToro API"
```

---

### Task 3: Create orders REST API

**Files:**
- Create: `app/api/orders.py`
- Modify: `app/main.py` (register router)
- Test: `tests/test_orders_api.py`

This is the core task. Two endpoints:
1. `POST /portfolio/orders` — place a new order (BUY/ADD with optional SL/TP)
2. `POST /portfolio/positions/{position_id}/close` — close a specific position

Both endpoints:
- Require auth (`require_session_or_service_token`)
- Check kill switch
- Check `enable_live_trading` (demo mode if false)
- Validate inputs
- Call broker
- Persist order + fill + position update in one transaction
- Write `decision_audit` row
- Return structured result

- [ ] **Step 1: Write failing test for place order endpoint**

Create `tests/test_orders_api.py`:

```python
"""Tests for POST /portfolio/orders and POST /portfolio/positions/{id}/close.

Test strategy:
  Mock broker provider via dependency override.
  Mock DB via FastAPI dependency override (same pattern as test_api_portfolio).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.providers.broker import BrokerOrderResult, BrokerProvider, OrderParams
from app.services.runtime_config import RuntimeConfig

_NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)

_DEFAULT_CONFIG = RuntimeConfig(
    enable_auto_trading=False,
    enable_live_trading=False,
    display_currency="USD",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock psycopg.Connection."""
    cur = MagicMock()
    result_iter = iter(cursor_results)

    def _on_execute(*_args: Any, **_kwargs: Any) -> None:
        rows = next(result_iter)
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows

    cur.execute.side_effect = _on_execute
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur

    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx

    return conn


def _with_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    conn = _mock_conn(cursor_results)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn


def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn([])


app.dependency_overrides.setdefault(get_conn, _fallback_conn)
client = TestClient(app)


class TestPlaceOrder:
    """POST /portfolio/orders."""

    def setup_method(self) -> None:
        self._patches = [
            patch("app.api.orders.get_runtime_config", return_value=_DEFAULT_CONFIG),
        ]
        for p in self._patches:
            p.start()

    def teardown_method(self) -> None:
        for p in self._patches:
            p.stop()
        app.dependency_overrides[get_conn] = _fallback_conn

    def test_rejects_missing_instrument_id(self) -> None:
        resp = client.post("/portfolio/orders", json={"action": "BUY", "amount": 100})
        assert resp.status_code == 422

    def test_rejects_missing_action(self) -> None:
        resp = client.post("/portfolio/orders", json={"instrument_id": 1, "amount": 100})
        assert resp.status_code == 422

    def test_rejects_both_amount_and_units(self) -> None:
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY", "amount": 100, "units": 5},
        )
        assert resp.status_code == 400

    def test_rejects_neither_amount_nor_units(self) -> None:
        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 1, "action": "BUY"},
        )
        assert resp.status_code == 400
```

- [ ] **Step 2: Run test — verify it fails**

Run: `uv run pytest tests/test_orders_api.py -v`
Expected: FAIL (module not found or 404)

- [ ] **Step 3: Create `app/api/orders.py` with place order endpoint**

```python
"""Order entry and position close endpoints.

POST /portfolio/orders         — place a new order (BUY/ADD) with optional SL/TP
POST /portfolio/positions/{position_id}/close — close a specific broker position

Safety:
  - Kill switch checked before any broker call
  - enable_live_trading checked — demo mode returns synthetic fills
  - All DB writes (order, fill, position, cash_ledger, audit) in one transaction
  - Every attempt writes a decision_audit row (success or failure)

This module does NOT route through execution_guard or trade_recommendations.
Those remain the automated pipeline path. These endpoints are the operator's
direct manual trading path — separate audit trail, separate safety checks.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.providers.broker import BrokerOrderResult, BrokerProvider, OrderParams
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/portfolio",
    tags=["orders"],
    dependencies=[Depends(require_session_or_service_token)],
)

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class PlaceOrderRequest(BaseModel):
    instrument_id: int
    action: str  # "BUY" or "ADD"
    amount: float | None = None
    units: float | None = None
    stop_loss_rate: float | None = None
    take_profit_rate: float | None = None
    is_tsl_enabled: bool = False
    leverage: int = 1


class ClosePositionRequest(BaseModel):
    units_to_deduct: float | None = None  # None = close entire position


class OrderResponse(BaseModel):
    order_id: int
    status: str  # "filled", "pending", "failed"
    broker_order_ref: str | None
    filled_price: float | None
    filled_units: float | None
    fees: float
    explanation: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ALLOWED_ACTIONS = {"BUY", "ADD"}


def _get_broker(conn: psycopg.Connection[Any]) -> BrokerProvider | None:
    """Resolve the broker provider from app state.

    Returns None in demo mode (enable_live_trading=False).
    In live mode, the broker is loaded from the lifespan-initialised
    app state. If not available, raises 503.
    """
    # The broker provider is set on the connection's app state during lifespan.
    # For now, return None — the endpoint handles demo mode internally.
    # Live mode wiring will be added when enable_live_trading is turned on.
    return None


def _check_kill_switch(conn: psycopg.Connection[Any]) -> None:
    """Raise 403 if the kill switch is active."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT is_active, reason FROM kill_switch ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=503, detail="Kill switch not configured")
    if row["is_active"]:
        raise HTTPException(
            status_code=403,
            detail=f"Kill switch is active: {row['reason']}",
        )


def _load_quote_price(
    conn: psycopg.Connection[Any], instrument_id: int
) -> Decimal | None:
    """Return the latest quote price, or None."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT last FROM quotes WHERE instrument_id = %(iid)s",
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None or row["last"] is None:
        return None
    return Decimal(str(row["last"]))


def _synthetic_fill(
    instrument_id: int,
    action: str,
    quote_price: Decimal | None,
    amount: Decimal | None,
    units: Decimal | None,
) -> BrokerOrderResult:
    """Demo-mode synthetic fill (same logic as order_client)."""
    price = quote_price if quote_price is not None else Decimal("0")
    if units is not None:
        fill_units = units
    elif amount is not None and price > 0:
        fill_units = (amount / price).quantize(Decimal("0.000001"))
    else:
        fill_units = Decimal("0")

    return BrokerOrderResult(
        broker_order_ref=f"DEMO-{instrument_id}-{action}",
        status="filled",
        filled_price=price,
        filled_units=fill_units,
        fees=Decimal("0"),
        raw_payload={
            "demo": True,
            "instrument_id": instrument_id,
            "action": action,
            "price": str(price),
            "units": str(fill_units),
        },
    )


def _persist_order_and_fill(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    action: str,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
    broker_result: BrokerOrderResult,
    now: datetime,
    params: OrderParams | None,
) -> tuple[int, int | None]:
    """Persist order + fill + position + cash in one transaction. Returns (order_id, fill_id)."""
    with conn.transaction():
        # Insert order row
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                INSERT INTO orders
                    (instrument_id, action, order_type,
                     requested_amount, requested_units,
                     status, broker_order_ref, raw_payload_json, created_at)
                VALUES
                    (%(iid)s, %(action)s, 'market',
                     %(amt)s, %(units)s,
                     %(status)s, %(ref)s, %(payload)s, %(now)s)
                RETURNING order_id
                """,
                {
                    "iid": instrument_id,
                    "action": action,
                    "amt": requested_amount,
                    "units": requested_units,
                    "status": broker_result.status,
                    "ref": broker_result.broker_order_ref,
                    "payload": Jsonb(broker_result.raw_payload),
                    "now": now,
                },
            )
            order_row = cur.fetchone()
        assert order_row is not None
        order_id = int(order_row["order_id"])

        fill_id: int | None = None
        fp = broker_result.filled_price
        fu = broker_result.filled_units

        if broker_result.status == "filled" and fp is not None and fu is not None and fu > 0:
            gross = fp * fu
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    INSERT INTO fills (order_id, filled_at, price, units, gross_amount, fees)
                    VALUES (%(oid)s, %(at)s, %(p)s, %(u)s, %(g)s, %(f)s)
                    RETURNING fill_id
                    """,
                    {
                        "oid": order_id,
                        "at": now,
                        "p": fp,
                        "u": fu,
                        "g": gross,
                        "f": broker_result.fees,
                    },
                )
                fill_row = cur.fetchone()
            assert fill_row is not None
            fill_id = int(fill_row["fill_id"])

            # Update positions table (same upsert as order_client)
            if action in ("BUY", "ADD"):
                new_cost = fp * fu
                conn.execute(
                    """
                    INSERT INTO positions
                        (instrument_id, open_date, avg_cost, current_units,
                         cost_basis, source, updated_at)
                    VALUES
                        (%(iid)s, %(date)s, %(price)s, %(units)s,
                         %(cost)s, 'ebull', %(now)s)
                    ON CONFLICT (instrument_id) DO UPDATE SET
                        current_units = positions.current_units + EXCLUDED.current_units,
                        cost_basis    = positions.cost_basis + EXCLUDED.cost_basis,
                        avg_cost      = (positions.cost_basis + EXCLUDED.cost_basis)
                                        / NULLIF(positions.current_units + EXCLUDED.current_units, 0),
                        source        = CASE
                            WHEN positions.current_units <= 0 THEN EXCLUDED.source
                            ELSE positions.source
                        END,
                        updated_at    = EXCLUDED.updated_at
                    """,
                    {
                        "iid": instrument_id,
                        "date": now.date(),
                        "price": fp,
                        "units": fu,
                        "cost": new_cost,
                        "now": now,
                    },
                )

            # Cash ledger entry
            if action in ("BUY", "ADD"):
                cash_amount = -(gross + broker_result.fees)
                event_type = "order_buy"
            else:
                cash_amount = gross - broker_result.fees
                event_type = "order_sell"
            conn.execute(
                """
                INSERT INTO cash_ledger (event_time, event_type, amount, currency, note)
                VALUES (%(time)s, %(type)s, %(amount)s, 'USD', %(note)s)
                """,
                {
                    "time": now,
                    "type": event_type,
                    "amount": cash_amount,
                    "note": f"manual {action} via UI",
                },
            )

        # Write audit row
        conn.execute(
            """
            INSERT INTO decision_audit
                (decision_time, instrument_id, stage,
                 pass_fail, explanation, evidence_json)
            VALUES
                (%(dt)s, %(iid)s, 'manual_order',
                 %(pf)s, %(expl)s, %(ev)s)
            """,
            {
                "dt": now,
                "iid": instrument_id,
                "pf": "PASS" if fill_id is not None else "FAIL",
                "expl": f"manual {action}: status={broker_result.status}",
                "ev": Jsonb({
                    "order_id": order_id,
                    "fill_id": fill_id,
                    "params": {
                        "stop_loss_rate": str(params.stop_loss_rate) if params and params.stop_loss_rate else None,
                        "take_profit_rate": str(params.take_profit_rate) if params and params.take_profit_rate else None,
                    } if params else None,
                    "raw_payload": broker_result.raw_payload,
                }),
            },
        )

    return order_id, fill_id


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/orders", response_model=OrderResponse)
def place_order(
    body: PlaceOrderRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OrderResponse:
    """Place a manual order (BUY/ADD) with optional SL/TP.

    This is the operator's direct trading path — no recommendation required.
    Kill switch and config flags are checked. Demo mode returns synthetic fills.
    """
    # Validate action
    if body.action not in _ALLOWED_ACTIONS:
        raise HTTPException(status_code=400, detail=f"Invalid action: {body.action!r}. Must be BUY or ADD.")

    # Validate exactly one of amount/units
    if body.amount is not None and body.units is not None:
        raise HTTPException(status_code=400, detail="Provide exactly one of amount or units, not both.")
    if body.amount is None and body.units is None:
        raise HTTPException(status_code=400, detail="Provide exactly one of amount or units.")

    # Validate positive values
    if body.amount is not None and body.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be positive.")
    if body.units is not None and body.units <= 0:
        raise HTTPException(status_code=400, detail="units must be positive.")

    # Safety checks
    _check_kill_switch(conn)
    config = get_runtime_config(conn)

    # Build broker params
    params = OrderParams(
        stop_loss_rate=Decimal(str(body.stop_loss_rate)) if body.stop_loss_rate is not None else None,
        take_profit_rate=Decimal(str(body.take_profit_rate)) if body.take_profit_rate is not None else None,
        is_tsl_enabled=body.is_tsl_enabled,
        leverage=body.leverage,
    )

    amount = Decimal(str(body.amount)) if body.amount is not None else None
    units = Decimal(str(body.units)) if body.units is not None else None
    now = datetime.now(tz=UTC)

    # Call broker or demo mode
    if config.enable_live_trading:
        raise HTTPException(status_code=501, detail="Live trading not yet wired — use demo mode.")
    else:
        quote_price = _load_quote_price(conn, body.instrument_id)
        broker_result = _synthetic_fill(
            instrument_id=body.instrument_id,
            action=body.action,
            quote_price=quote_price,
            amount=amount,
            units=units,
        )

    # Persist
    order_id, fill_id = _persist_order_and_fill(
        conn,
        instrument_id=body.instrument_id,
        action=body.action,
        requested_amount=amount,
        requested_units=units,
        broker_result=broker_result,
        now=now,
        params=params,
    )

    return OrderResponse(
        order_id=order_id,
        status=broker_result.status,
        broker_order_ref=broker_result.broker_order_ref,
        filled_price=float(broker_result.filled_price) if broker_result.filled_price else None,
        filled_units=float(broker_result.filled_units) if broker_result.filled_units else None,
        fees=float(broker_result.fees),
        explanation=f"{'demo ' if not config.enable_live_trading else ''}{body.action} "
        f"{'filled' if fill_id else broker_result.status}",
    )


@router.post(
    "/positions/{position_id}/close",
    response_model=OrderResponse,
)
def close_position(
    position_id: int,
    body: ClosePositionRequest | None = None,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OrderResponse:
    """Close a specific broker position by position_id.

    Safety invariant: this is one of exactly two code paths that may close
    a position (the other is EXIT via execution_guard → order_client).
    """
    _check_kill_switch(conn)
    config = get_runtime_config(conn)

    # Look up the position to get instrument_id and units
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, units, amount, open_rate
            FROM broker_positions
            WHERE position_id = %(pid)s AND units > 0
            """,
            {"pid": position_id},
        )
        pos_row = cur.fetchone()

    if pos_row is None:
        raise HTTPException(status_code=404, detail=f"Position {position_id} not found or already closed.")

    instrument_id = int(pos_row["instrument_id"])
    units_to_deduct = None
    if body and body.units_to_deduct is not None:
        units_to_deduct = Decimal(str(body.units_to_deduct))

    now = datetime.now(tz=UTC)

    # Call broker or demo mode
    if config.enable_live_trading:
        raise HTTPException(status_code=501, detail="Live trading not yet wired — use demo mode.")
    else:
        quote_price = _load_quote_price(conn, instrument_id)
        close_units = units_to_deduct if units_to_deduct else Decimal(str(pos_row["units"]))
        broker_result = _synthetic_fill(
            instrument_id=instrument_id,
            action="EXIT",
            quote_price=quote_price,
            amount=None,
            units=close_units,
        )

    # Persist close
    order_id, fill_id = _persist_order_and_fill(
        conn,
        instrument_id=instrument_id,
        action="EXIT",
        requested_amount=None,
        requested_units=units_to_deduct or Decimal(str(pos_row["units"])),
        broker_result=broker_result,
        now=now,
        params=None,
    )

    return OrderResponse(
        order_id=order_id,
        status=broker_result.status,
        broker_order_ref=broker_result.broker_order_ref,
        filled_price=float(broker_result.filled_price) if broker_result.filled_price else None,
        filled_units=float(broker_result.filled_units) if broker_result.filled_units else None,
        fees=float(broker_result.fees),
        explanation=f"{'demo ' if not config.enable_live_trading else ''}close position {position_id}",
    )
```

- [ ] **Step 4: Register the router in `app/main.py`**

Add import and include_router:

```python
from app.api.orders import router as orders_router
# ...
app.include_router(orders_router)
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_orders_api.py -v`
Expected: PASS for validation tests

- [ ] **Step 6: Add happy-path tests**

Add to `tests/test_orders_api.py`:

```python
    def test_demo_buy_order_returns_synthetic_fill(self) -> None:
        """Demo mode BUY returns a filled order with synthetic price."""
        # cursor results: kill_switch, quote
        ks_row = {"is_active": False, "reason": ""}
        quote_row = {"last": 150.0}
        order_row = {"order_id": 1}
        fill_row = {"fill_id": 1}
        _with_conn([
            [ks_row],      # kill switch check
            [quote_row],   # quote price lookup
            [order_row],   # INSERT orders RETURNING
            [fill_row],    # INSERT fills RETURNING
        ])

        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 42, "action": "BUY", "amount": 1500.0},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "filled"
        assert body["filled_price"] == 150.0
        assert body["order_id"] == 1

    def test_kill_switch_blocks_order(self) -> None:
        """Active kill switch returns 403."""
        ks_row = {"is_active": True, "reason": "emergency stop"}
        _with_conn([[ks_row]])

        resp = client.post(
            "/portfolio/orders",
            json={"instrument_id": 42, "action": "BUY", "amount": 1500.0},
        )
        assert resp.status_code == 403
        assert "kill switch" in resp.json()["detail"].lower()
```

- [ ] **Step 7: Run full test suite**

Run: `uv run pytest -v`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add app/api/orders.py app/main.py tests/test_orders_api.py
git commit -m "feat: add POST /portfolio/orders and POST /portfolio/positions/{id}/close"
```

---

### Task 4: Wire broker_positions on fill

**Files:**
- Modify: `app/api/orders.py` (add broker_positions INSERT after fill)
- Test: `tests/test_orders_api.py`

When a BUY/ADD fill succeeds, the new endpoint should also write to `broker_positions` so the position detail drill-through shows the individual trade.

- [ ] **Step 1: Write failing test**

```python
    def test_buy_fill_writes_to_broker_positions(self) -> None:
        """A filled BUY should INSERT into broker_positions."""
        ks_row = {"is_active": False, "reason": ""}
        quote_row = {"last": 150.0}
        order_row = {"order_id": 1}
        fill_row = {"fill_id": 1}
        conn = _with_conn([
            [ks_row], [quote_row], [order_row], [fill_row],
        ])

        resp = client.post(
            "/portfolio/orders",
            json={
                "instrument_id": 42,
                "action": "BUY",
                "amount": 1500.0,
                "stop_loss_rate": 140.0,
                "take_profit_rate": 200.0,
            },
        )
        assert resp.status_code == 200

        # Verify broker_positions INSERT was called
        all_sql = [
            call.args[0] if call.args else call.kwargs.get("query", "")
            for call in conn.execute.call_args_list
        ]
        bp_inserts = [s for s in all_sql if "broker_positions" in str(s)]
        assert len(bp_inserts) >= 1, f"Expected broker_positions INSERT, got: {all_sql}"
```

- [ ] **Step 2: Add broker_positions INSERT to `_persist_order_and_fill`**

Inside the `if action in ("BUY", "ADD"):` block after the positions upsert, add:

```python
            # Write to broker_positions for drill-through visibility.
            # In demo mode, use the order_id as a synthetic position_id
            # since there's no real broker positionId.
            synthetic_position_id = order_id  # demo placeholder
            conn.execute(
                """
                INSERT INTO broker_positions
                    (position_id, instrument_id, is_buy, units, amount,
                     initial_amount_in_dollars, open_rate, open_conversion_rate,
                     open_date_time, stop_loss_rate, take_profit_rate,
                     is_no_stop_loss, is_no_take_profit,
                     leverage, is_tsl_enabled, total_fees,
                     source, raw_payload, updated_at)
                VALUES
                    (%(pid)s, %(iid)s, TRUE, %(units)s, %(amount)s,
                     %(amount)s, %(price)s, 1,
                     %(now)s, %(sl)s, %(tp)s,
                     %(no_sl)s, %(no_tp)s,
                     %(leverage)s, %(tsl)s, %(fees)s,
                     'ebull', %(payload)s, %(now)s)
                ON CONFLICT (position_id) DO UPDATE SET
                    units = EXCLUDED.units,
                    amount = EXCLUDED.amount,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "pid": synthetic_position_id,
                    "iid": instrument_id,
                    "units": fu,
                    "amount": gross,
                    "price": fp,
                    "now": now,
                    "sl": params.stop_loss_rate if params else None,
                    "tp": params.take_profit_rate if params else None,
                    "no_sl": params is None or params.stop_loss_rate is None,
                    "no_tp": params is None or params.take_profit_rate is None,
                    "leverage": params.leverage if params else 1,
                    "tsl": params.is_tsl_enabled if params else False,
                    "fees": broker_result.fees,
                    "payload": Jsonb(broker_result.raw_payload),
                },
            )
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_orders_api.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/api/orders.py tests/test_orders_api.py
git commit -m "feat: write broker_positions on manual order fill"
```

---

### Task 5: Close-position endpoint tests

**Files:**
- Test: `tests/test_orders_api.py`

- [ ] **Step 1: Add close position tests**

```python
class TestClosePosition:
    """POST /portfolio/positions/{position_id}/close."""

    def setup_method(self) -> None:
        self._patches = [
            patch("app.api.orders.get_runtime_config", return_value=_DEFAULT_CONFIG),
        ]
        for p in self._patches:
            p.start()

    def teardown_method(self) -> None:
        for p in self._patches:
            p.stop()
        app.dependency_overrides[get_conn] = _fallback_conn

    def test_404_for_unknown_position(self) -> None:
        """Unknown position_id returns 404."""
        ks_row = {"is_active": False, "reason": ""}
        _with_conn([[ks_row], []])  # kill switch, empty position lookup

        resp = client.post("/portfolio/positions/9999/close", json={})
        assert resp.status_code == 404

    def test_demo_close_returns_filled(self) -> None:
        """Demo mode close returns a filled synthetic response."""
        ks_row = {"is_active": False, "reason": ""}
        pos_row = {"instrument_id": 42, "units": 10.0, "amount": 1500.0, "open_rate": 150.0}
        quote_row = {"last": 160.0}
        order_row = {"order_id": 1}
        fill_row = {"fill_id": 1}
        _with_conn([
            [ks_row], [pos_row], [quote_row], [order_row], [fill_row],
        ])

        resp = client.post("/portfolio/positions/5001/close", json={})
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "filled"
        assert body["filled_units"] == 10.0

    def test_kill_switch_blocks_close(self) -> None:
        """Active kill switch returns 403 for close too."""
        ks_row = {"is_active": True, "reason": "halt"}
        _with_conn([[ks_row]])

        resp = client.post("/portfolio/positions/5001/close", json={})
        assert resp.status_code == 403
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_orders_api.py -v`
Expected: PASS

- [ ] **Step 3: Run full suite + pre-push checks**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_orders_api.py
git commit -m "test: add close-position endpoint tests"
```

---

### Task 6: Pre-push Codex review and final cleanup

- [ ] **Step 1: Run Codex review**

```bash
codex.cmd review --base main
```

Address any findings.

- [ ] **Step 2: Run full pre-push checklist**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

All must pass.

- [ ] **Step 3: Final commit if any cleanup needed**

---

## Scope explicitly excluded

- **Frontend modals** (OrderEntryModal, ClosePositionModal) — Phase 2b, separate plan
- **Limit orders** — Phase 4
- **SL/TP editing on existing positions** — Phase 3 (blocked on API discovery)
- **Live broker wiring** — the endpoints return 501 for `enable_live_trading=True` until the broker dependency injection is wired through FastAPI
- **Position-level EXIT from execution_guard** — the automated pipeline continues to use `order_client.py`; this plan adds the manual path only
