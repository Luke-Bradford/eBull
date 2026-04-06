"""
Order client.

Responsibilities:
  - Accept a guard-approved recommendation (PASS verdict only).
  - Place the order via the broker provider, or generate a synthetic fill
    in demo mode (enable_live_trading=False).
  - Persist every order attempt to the ``orders`` table with the raw broker
    response — success or failure.
  - On a successful fill: persist to ``fills``, update ``positions``,
    and record a ``cash_ledger`` entry.
  - Update ``decision_audit`` with execution outcome.

This is the only module that talks to the broker write API.
All decision logic lives upstream (portfolio manager → execution guard).

Demo mode:
  When ``enable_live_trading`` is False the service never makes a real HTTP
  call.  It logs the would-be request and produces a synthetic fill using
  the latest quote price from the DB.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.config import Settings
from app.providers.broker import BrokerOrderResult, BrokerProvider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OrderOutcome = Literal["filled", "pending", "failed"]

# Default position size used for BUY when the recommendation has no units.
# The portfolio manager provides suggested_size_pct as a fraction of AUM;
# the guard already validated cash.  When both amount and units are absent,
# this default prevents a zero-size order.
_DEFAULT_ORDER_TYPE = "market"

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExecuteResult:
    """Returned by execute_order."""

    order_id: int
    outcome: OrderOutcome
    broker_order_ref: str | None
    fill_id: int | None
    explanation: str


# ---------------------------------------------------------------------------
# DB loaders (read-only; called before any transaction)
# ---------------------------------------------------------------------------


def _load_approved_recommendation(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
) -> dict[str, Any]:
    """
    Load a recommendation that has been approved by the execution guard.

    Raises ValueError if:
      - the recommendation_id does not exist (programmer error)
      - the recommendation status is not 'approved' (caller violated contract)
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT recommendation_id, instrument_id, action,
                   target_entry, suggested_size_pct, model_version, status
            FROM trade_recommendations
            WHERE recommendation_id = %(rid)s
            """,
            {"rid": recommendation_id},
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"recommendation_id={recommendation_id} not found")
    if row["status"] != "approved":
        raise ValueError(f"recommendation_id={recommendation_id} status={row['status']!r}; expected 'approved'")
    return dict(row)


def _load_latest_quote_price(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> Decimal | None:
    """Return the latest quote last-price, or None if unavailable."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT last
            FROM quotes
            WHERE instrument_id = %(iid)s
            ORDER BY quoted_at DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None or row["last"] is None:
        return None
    return Decimal(str(row["last"]))


def _load_position_units(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> Decimal:
    """Return current_units for a position, or 0 if no position exists."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT current_units
            FROM positions
            WHERE instrument_id = %(iid)s
            """,
            {"iid": instrument_id},
        )
        # positions.instrument_id is PRIMARY KEY — at most one row.
        row = cur.fetchone()
    if row is None or row["current_units"] is None:
        return Decimal("0")
    return Decimal(str(row["current_units"]))


# ---------------------------------------------------------------------------
# Demo-mode synthetic fill
# ---------------------------------------------------------------------------


def _synthetic_fill(
    instrument_id: int,
    action: str,
    quote_price: Decimal | None,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
) -> BrokerOrderResult:
    """
    Build a synthetic BrokerOrderResult for demo mode.

    Uses the latest quote price.  If no price is available, the fill is
    produced at Decimal("0") with a note in the payload — this lets demo
    runs proceed without real market data while making the issue visible.
    """
    price = quote_price if quote_price is not None else Decimal("0")

    if requested_units is not None:
        units = requested_units
    elif requested_amount is not None and price > 0:
        units = (requested_amount / price).quantize(Decimal("0.000001"))
    else:
        units = Decimal("0")

    return BrokerOrderResult(
        broker_order_ref=f"DEMO-{instrument_id}-{action}",
        status="filled",
        filled_price=price,
        filled_units=units,
        fees=Decimal("0"),
        raw_payload={
            "demo": True,
            "instrument_id": instrument_id,
            "action": action,
            "price": str(price),
            "units": str(units),
            "note": "synthetic fill — no real API call"
            + ("" if quote_price is not None else "; no quote available, price=0"),
        },
    )


# ---------------------------------------------------------------------------
# DB writers (all inside a single transaction)
# ---------------------------------------------------------------------------


def _persist_order(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    recommendation_id: int,
    decision_id: int,
    action: str,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
    status: str,
    broker_order_ref: str | None,
    raw_payload: dict[str, Any],
    now: datetime,
) -> int:
    """Insert an orders row and return the order_id."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO orders
                (instrument_id, recommendation_id, decision_id,
                 action, order_type, requested_amount, requested_units,
                 status, broker_order_ref, raw_payload_json, created_at)
            VALUES
                (%(iid)s, %(rid)s, %(did)s,
                 %(action)s, %(otype)s, %(amt)s, %(units)s,
                 %(status)s, %(ref)s, %(payload)s, %(now)s)
            RETURNING order_id
            """,
            {
                "iid": instrument_id,
                "rid": recommendation_id,
                "did": decision_id,
                "action": action,
                "otype": _DEFAULT_ORDER_TYPE,
                "amt": requested_amount,
                "units": requested_units,
                "status": status,
                "ref": broker_order_ref,
                "payload": Jsonb(raw_payload),
                "now": now,
            },
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("orders INSERT returned no row")
    return int(row["order_id"])


def _persist_fill(
    conn: psycopg.Connection[Any],
    order_id: int,
    price: Decimal,
    units: Decimal,
    fees: Decimal,
    now: datetime,
) -> int:
    """Insert a fills row and return the fill_id."""
    gross_amount = price * units
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
                "price": price,
                "units": units,
                "gross": gross_amount,
                "fees": fees,
            },
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("fills INSERT returned no row")
    return int(row["fill_id"])


def _update_position_buy(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    filled_price: Decimal,
    filled_units: Decimal,
    now: datetime,
) -> None:
    """
    Upsert the position for a BUY/ADD fill.

    New position: set open_date, avg_cost, current_units, cost_basis.
    Existing position: add units, recompute avg_cost and cost_basis.
    """
    new_cost = filled_price * filled_units
    conn.execute(
        """
        INSERT INTO positions
            (instrument_id, open_date, avg_cost, current_units, cost_basis, updated_at)
        VALUES
            (%(iid)s, %(date)s, %(price)s, %(units)s, %(cost)s, %(now)s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            current_units = positions.current_units + EXCLUDED.current_units,
            cost_basis    = positions.cost_basis + EXCLUDED.cost_basis,
            avg_cost      = (positions.cost_basis + EXCLUDED.cost_basis)
                            / NULLIF(positions.current_units + EXCLUDED.current_units, 0),
            updated_at    = EXCLUDED.updated_at
        """,
        {
            "iid": instrument_id,
            "date": now.date(),
            "price": filled_price,
            "units": filled_units,
            "cost": new_cost,
            "now": now,
        },
    )


def _update_position_exit(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    filled_price: Decimal,
    filled_units: Decimal,
    now: datetime,
) -> None:
    """
    Update the position for an EXIT fill.

    Subtracts filled_units and computes realized P&L based on avg_cost.
    """
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
            "units": filled_units,
            "price": filled_price,
            "now": now,
        },
    )


def _record_cash_ledger(
    conn: psycopg.Connection[Any],
    action: str,
    gross_amount: Decimal,
    fees: Decimal,
    now: datetime,
) -> None:
    """
    Record the cash impact of a fill.

    BUY/ADD: cash outflow (negative amount).
    EXIT:    cash inflow (positive amount), minus fees.
    """
    if action in ("BUY", "ADD"):
        amount = -(gross_amount + fees)
        event_type = "order_buy"
    else:
        amount = gross_amount - fees
        event_type = "order_sell"

    conn.execute(
        """
        INSERT INTO cash_ledger (event_time, event_type, amount, currency, note)
        VALUES (%(time)s, %(type)s, %(amount)s, 'USD', %(note)s)
        """,
        {
            "time": now,
            "type": event_type,
            "amount": amount,
            "note": f"{action} fill",
        },
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def execute_order(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
    decision_id: int,
    settings: Settings,
    broker: BrokerProvider | None = None,
) -> ExecuteResult:
    """
    Execute a guard-approved order.

    Steps:
      1. Load the approved recommendation (raises if not found or not approved).
      2. Determine order parameters from the recommendation.
      3. If live mode: call the broker provider.
         If demo mode: generate a synthetic fill.
      4. Persist the order row with raw broker response.
      5. If filled: persist fill, update position, record cash ledger entry.
      All DB writes in steps 4-5 are inside a single transaction.

    No external I/O is performed inside any DB transaction.

    Raises ValueError if:
      - recommendation_id does not exist
      - recommendation status is not 'approved'
      - live mode but no broker provider supplied
    """
    now = _utcnow()

    # --- Step 1: load and validate recommendation ---
    rec = _load_approved_recommendation(conn, recommendation_id)
    instrument_id: int = int(rec["instrument_id"])
    action: str = str(rec["action"])

    # --- Step 2: determine order parameters ---
    requested_amount: Decimal | None = None
    requested_units: Decimal | None = None

    if rec["target_entry"] is not None and rec["suggested_size_pct"] is not None:
        requested_amount = Decimal(str(rec["target_entry"])) * Decimal(str(rec["suggested_size_pct"]))
    if action == "EXIT":
        requested_units = _load_position_units(conn, instrument_id)

    # --- Step 3: call broker or demo mode ---
    is_live = settings.enable_live_trading

    if is_live:
        if broker is None:
            raise ValueError("enable_live_trading is True but no broker provider supplied")
        if action == "EXIT":
            broker_result = broker.close_position(instrument_id)
        else:
            broker_result = broker.place_order(
                instrument_id=instrument_id,
                action=action,
                amount=requested_amount,
                units=requested_units,
            )
    else:
        quote_price = _load_latest_quote_price(conn, instrument_id)
        broker_result = _synthetic_fill(
            instrument_id=instrument_id,
            action=action,
            quote_price=quote_price,
            requested_amount=requested_amount,
            requested_units=requested_units,
        )
        logger.info(
            "demo mode: instrument_id=%d action=%s price=%s units=%s",
            instrument_id,
            action,
            broker_result.filled_price,
            broker_result.filled_units,
        )

    # --- Step 4 + 5: persist (all DB writes in one transaction) ---
    # All external I/O (broker call) is already complete above.
    order_status = broker_result.status
    fill_id: int | None = None

    with conn.transaction():
        order_id = _persist_order(
            conn,
            instrument_id=instrument_id,
            recommendation_id=recommendation_id,
            decision_id=decision_id,
            action=action,
            requested_amount=requested_amount,
            requested_units=requested_units,
            status=order_status,
            broker_order_ref=broker_result.broker_order_ref,
            raw_payload=broker_result.raw_payload,
            now=now,
        )

        filled_price = broker_result.filled_price
        filled_units = broker_result.filled_units

        if order_status == "filled" and filled_price is not None and filled_units is not None:
            fill_id = _persist_fill(
                conn,
                order_id=order_id,
                price=filled_price,
                units=filled_units,
                fees=broker_result.fees,
                now=now,
            )

            if action in ("BUY", "ADD"):
                _update_position_buy(
                    conn,
                    instrument_id=instrument_id,
                    filled_price=filled_price,
                    filled_units=filled_units,
                    now=now,
                )
            elif action == "EXIT":
                _update_position_exit(
                    conn,
                    instrument_id=instrument_id,
                    filled_price=filled_price,
                    filled_units=filled_units,
                    now=now,
                )

            gross_amount = filled_price * filled_units
            _record_cash_ledger(conn, action, gross_amount, broker_result.fees, now)

        # Update recommendation status to reflect execution outcome
        if order_status == "filled":
            exec_status = "executed"
        elif order_status == "pending":
            exec_status = "execution_pending"
        else:
            exec_status = "execution_failed"
        conn.execute(
            """
            UPDATE trade_recommendations
            SET status = %(status)s
            WHERE recommendation_id = %(rid)s
            """,
            {"status": exec_status, "rid": recommendation_id},
        )

    # --- Build explanation ---
    if order_status == "filled":
        explanation = (
            f"order filled: price={broker_result.filled_price} "
            f"units={broker_result.filled_units} "
            f"ref={broker_result.broker_order_ref}"
        )
    elif order_status == "pending":
        explanation = f"order pending: ref={broker_result.broker_order_ref}"
    else:
        explanation = f"order {order_status}: {broker_result.raw_payload}"

    outcome: OrderOutcome
    if order_status == "filled":
        outcome = "filled"
    elif order_status == "pending":
        outcome = "pending"
    else:
        outcome = "failed"

    logger.info(
        "execute_order: recommendation_id=%d instrument_id=%d action=%s outcome=%s order_id=%d",
        recommendation_id,
        instrument_id,
        action,
        outcome,
        order_id,
    )

    return ExecuteResult(
        order_id=order_id,
        outcome=outcome,
        broker_order_ref=broker_result.broker_order_ref,
        fill_id=fill_id,
        explanation=explanation,
    )
