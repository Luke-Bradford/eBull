"""Manual order endpoints.

POST /portfolio/orders       — place a manual BUY/ADD order (operator UI)
POST /portfolio/positions/{position_id}/close — close a specific broker position

Both endpoints:
  - Require auth via require_session_or_service_token (router dependency).
  - Check the kill switch before any order/close.
  - Check runtime config for enable_live_trading (demo mode if false).
  - Validate inputs.
  - Call broker or create a synthetic fill in demo mode.
  - Persist order + fill + position + cash_ledger + audit in one transaction.

Safety invariant:
  close_position is reachable ONLY via the operator UI (this endpoint) or
  EXIT recommendation (order_client.py).  No other code path may close.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.providers.broker import BrokerOrderResult
from app.services.ops_monitor import get_kill_switch_status
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
    action: Literal["BUY", "ADD"]
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
# Internal helpers
# ---------------------------------------------------------------------------

STAGE: str = "manual_order"


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _check_kill_switch(conn: psycopg.Connection[Any]) -> None:
    """Fail-closed kill-switch check.  Must be called before any order/close.

    Delegates to the shared ``get_kill_switch_status`` service so the
    kill-switch query logic lives in one place.
    """
    ks = get_kill_switch_status(conn)
    if ks["is_active"]:
        reason = ks.get("reason", "")
        raise HTTPException(
            status_code=403,
            detail=f"Kill switch is active: {reason}",
        )


def _synthetic_fill(
    instrument_id: int,
    action: str,
    quote_price: Decimal | None,
    amount: Decimal | None,
    units: Decimal | None,
) -> BrokerOrderResult:
    """Build a synthetic BrokerOrderResult for demo mode."""
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
            "note": "synthetic fill — no real API call"
            + ("" if quote_price is not None else "; no quote available, price=0"),
        },
    )


def _load_latest_quote_price(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> Decimal | None:
    """Return the latest quote last-price, or None if unavailable."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT last FROM quotes WHERE instrument_id = %(iid)s ORDER BY quoted_at DESC LIMIT 1",
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None or row["last"] is None:
        return None
    return Decimal(str(row["last"]))


def _persist_order_and_fill(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    action: str,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
    broker_result: BrokerOrderResult,
    params: PlaceOrderRequest | None,
    now: datetime,
    close_position_id: int | None = None,
) -> int:
    """Persist order + fill + position + broker_positions + cash_ledger + audit.

    All writes happen inside a single transaction for atomicity.
    close_position_id: if closing a specific broker_positions row, zero its units.
    Returns the order_id.
    """
    order_status = broker_result.status
    with conn.transaction():
        # 1. INSERT order
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                INSERT INTO orders
                    (instrument_id, action, order_type,
                     requested_amount, requested_units,
                     status, broker_order_ref, raw_payload_json, created_at)
                VALUES
                    (%(iid)s, %(action)s, %(otype)s,
                     %(amt)s, %(units)s,
                     %(status)s, %(ref)s, %(payload)s, %(now)s)
                RETURNING order_id
                """,
                {
                    "iid": instrument_id,
                    "action": action,
                    "otype": "market",
                    "amt": requested_amount,
                    "units": requested_units,
                    "status": order_status,
                    "ref": broker_result.broker_order_ref,
                    "payload": Jsonb(broker_result.raw_payload),
                    "now": now,
                },
            )
            order_row = cur.fetchone()
        if order_row is None:
            raise RuntimeError("orders INSERT returned no row")
        order_id: int = int(order_row["order_id"])

        fp = broker_result.filled_price
        fu = broker_result.filled_units

        # 2. If filled with positive units, persist fill + position + cash
        if order_status == "filled" and fp is not None and fu is not None and fu > 0:
            gross = fp * fu

            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    INSERT INTO fills
                        (order_id, filled_at, price, units, gross_amount, fees)
                    VALUES
                        (%(oid)s, %(filled_at)s, %(price)s, %(units)s, %(gross)s, %(fees)s)
                    RETURNING fill_id
                    """,
                    {
                        "oid": order_id,
                        "filled_at": now,
                        "price": fp,
                        "units": fu,
                        "gross": gross,
                        "fees": broker_result.fees,
                    },
                )

            if action in ("BUY", "ADD"):
                # 3. Upsert positions (same pattern as order_client.py)
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
                            WHEN positions.current_units <= 0
                                THEN EXCLUDED.source
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

                # 4. Insert into broker_positions
                synthetic_position_id = order_id
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

            elif action == "EXIT":
                # Update positions for EXIT
                conn.execute(
                    """
                    UPDATE positions SET
                        current_units  = current_units - %(units)s,
                        realized_pnl   = realized_pnl
                                         + (%(price)s - COALESCE(avg_cost, 0)) * %(units)s,
                        updated_at     = %(now)s
                    WHERE instrument_id = %(iid)s
                    """,
                    {
                        "iid": instrument_id,
                        "units": fu,
                        "price": fp,
                        "now": now,
                    },
                )

                # Zero out the broker_positions row so it can't be closed again.
                if close_position_id is not None:
                    conn.execute(
                        """
                        UPDATE broker_positions SET
                            units      = units - %(units)s,
                            updated_at = %(now)s
                        WHERE position_id = %(pid)s
                        """,
                        {
                            "units": fu,
                            "pid": close_position_id,
                            "now": now,
                        },
                    )

            # 5. Cash ledger entry
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
                    "note": f"manual {action} fill",
                },
            )

        # 6. decision_audit row
        passed = order_status == "filled" and fp is not None and fu is not None and fu > 0
        conn.execute(
            """
            INSERT INTO decision_audit
                (decision_time, instrument_id, stage,
                 pass_fail, explanation, evidence_json)
            VALUES
                (%(dt)s, %(iid)s, %(stage)s,
                 %(pf)s, %(expl)s, %(ev)s)
            """,
            {
                "dt": now,
                "iid": instrument_id,
                "stage": STAGE,
                "pf": "PASS" if passed else "FAIL",
                "expl": f"manual order: status={order_status} ref={broker_result.broker_order_ref}",
                "ev": Jsonb({"order_id": order_id, "raw_payload": broker_result.raw_payload}),
            },
        )

    return order_id


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/orders", response_model=OrderResponse)
def place_order(
    body: PlaceOrderRequest,
    conn: psycopg.Connection[Any] = Depends(get_conn),
) -> OrderResponse:
    """Place a manual BUY/ADD order with optional SL/TP."""
    # Safety checks first — kill switch blocks everything.
    _check_kill_switch(conn)
    config = get_runtime_config(conn)

    # Validation
    if body.amount is not None and body.units is not None:
        raise HTTPException(status_code=400, detail="Provide amount or units, not both")
    if body.amount is None and body.units is None:
        raise HTTPException(status_code=400, detail="Provide amount or units")

    if config.enable_live_trading:
        raise HTTPException(status_code=501, detail="Live trading not yet wired — use demo mode.")

    # Demo mode: synthetic fill
    amount_d = Decimal(str(body.amount)) if body.amount is not None else None
    units_d = Decimal(str(body.units)) if body.units is not None else None
    quote_price = _load_latest_quote_price(conn, body.instrument_id)

    # Fail closed: amount-based orders need a quote to compute units.
    if amount_d is not None and quote_price is None:
        raise HTTPException(
            status_code=422,
            detail=f"No quote available for instrument {body.instrument_id} — cannot compute units from amount.",
        )

    broker_result = _synthetic_fill(
        instrument_id=body.instrument_id,
        action=body.action,
        quote_price=quote_price,
        amount=amount_d,
        units=units_d,
    )

    now = _utcnow()
    order_id = _persist_order_and_fill(
        conn,
        instrument_id=body.instrument_id,
        action=body.action,
        requested_amount=amount_d,
        requested_units=units_d,
        broker_result=broker_result,
        params=body,
        now=now,
    )

    return OrderResponse(
        order_id=order_id,
        status=broker_result.status,
        broker_order_ref=broker_result.broker_order_ref,
        filled_price=float(broker_result.filled_price) if broker_result.filled_price is not None else None,
        filled_units=float(broker_result.filled_units) if broker_result.filled_units is not None else None,
        fees=float(broker_result.fees),
        explanation=f"Demo {body.action}: price={broker_result.filled_price} units={broker_result.filled_units}",
    )


@router.post("/positions/{position_id}/close", response_model=OrderResponse)
def close_position(
    position_id: int,
    body: ClosePositionRequest | None = None,
    conn: psycopg.Connection[Any] = Depends(get_conn),
) -> OrderResponse:
    """Close a specific broker position (full or partial)."""
    _check_kill_switch(conn)
    config = get_runtime_config(conn)

    if config.enable_live_trading:
        raise HTTPException(status_code=501, detail="Live trading not yet wired — use demo mode.")

    # Look up the broker position
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT instrument_id, units, amount, open_rate FROM broker_positions "
            "WHERE position_id = %(pid)s AND units > 0",
            {"pid": position_id},
        )
        pos_row = cur.fetchone()
    if pos_row is None:
        raise HTTPException(status_code=404, detail=f"Position {position_id} not found or already closed.")

    instrument_id: int = int(pos_row["instrument_id"])
    position_units = Decimal(str(pos_row["units"]))
    close_units = position_units
    if body is not None and body.units_to_deduct is not None:
        close_units = Decimal(str(body.units_to_deduct))
        if close_units > position_units:
            raise HTTPException(
                status_code=400,
                detail=f"units_to_deduct ({close_units}) exceeds position units ({position_units})",
            )

    open_rate = Decimal(str(pos_row["open_rate"]))

    # Demo mode: synthetic fill at the open_rate (no live quote needed for close)
    broker_result = _synthetic_fill(
        instrument_id=instrument_id,
        action="EXIT",
        quote_price=open_rate,
        amount=None,
        units=close_units,
    )

    now = _utcnow()
    order_id = _persist_order_and_fill(
        conn,
        instrument_id=instrument_id,
        action="EXIT",
        requested_amount=None,
        requested_units=close_units,
        broker_result=broker_result,
        params=None,
        now=now,
        close_position_id=position_id,
    )

    return OrderResponse(
        order_id=order_id,
        status=broker_result.status,
        broker_order_ref=broker_result.broker_order_ref,
        filled_price=float(broker_result.filled_price) if broker_result.filled_price is not None else None,
        filled_units=float(broker_result.filled_units) if broker_result.filled_units is not None else None,
        fees=float(broker_result.fees),
        explanation=f"Demo EXIT: price={broker_result.filled_price} units={broker_result.filled_units}",
    )
