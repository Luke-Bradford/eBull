"""
Order client.

Responsibilities:
  - Accept a guard-approved recommendation (PASS verdict only).
  - Re-check the operator's standing trading authority (kill switch,
    ``enable_auto_trading``) against live state immediately before taking
    entry authority, and refuse if it changed since approval (#2943).
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
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.providers.broker import (
    BrokerOrderResult,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    OrderParams,
)
from app.security.unattended_guard import UnattendedExecutionRefused
from app.services.execution_guard import decide_submission_controls, load_kill_switch
from app.services.quote_marks import directional_fill_price, positive_decimal_or_none
from app.services.return_attribution import compute_attribution, persist_attribution
from app.services.runtime_config import RuntimeConfig, get_runtime_config
from app.services.trade_events import enqueue_post_trade_sync
from app.services.transaction_cost import (
    estimate_cost,
    get_transaction_cost_config,
    load_instrument_cost,
    missing_cost_components,
    record_estimated_cost,
    spread_pct_to_bps,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OrderOutcome = Literal["filled", "pending", "failed"]

_DEFAULT_ORDER_TYPE = "market"

# decision_audit.stage for every row this module writes.
#
# Must stay equal to the "order_client" literal in the audit filter vocabulary
# (app/api/audit.py::Stage, frontend/src/api/types.ts::AuditStage). It read
# "order_execution" until #2943, which no row in dev had ever been written with
# — the writer and the only filter value that selects it had never agreed, so
# an operator filtering the trail for order-client rows got an empty list.
STAGE: str = "order_client"

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
                   target_entry, suggested_size_pct, model_version, status,
                   stop_loss_rate, take_profit_rate
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
    """Return the latest strictly-positive quote last-price, or None."""
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
    if row is None:
        return None
    return positive_decimal_or_none(row["last"])


def _load_quote_for_execution(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    """Load full quote data for execution: last, bid, ask, spread_pct."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT last, bid, ask, spread_pct
            FROM quotes
            WHERE instrument_id = %(iid)s
            ORDER BY quoted_at DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


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


@dataclass(frozen=True)
class ExitLot:
    """The single broker lot an EXIT recommendation will close (#3006)."""

    position_id: int
    units: Decimal


def _load_exit_lot(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> ExitLot | None:
    """Return the broker lot an EXIT will close, or None if there is none.

    For EXIT via recommendation, the instrument may have multiple
    ``broker_positions``. We close the oldest (earliest ``open_date_time``) —
    this matches FIFO semantics. ``position_id`` breaks ties so the choice is
    deterministic when two lots share a timestamp.

    Two filters beyond ``units > 0``, both added by #3006:

    ``position_id > 0`` — ``_persist_broker_position`` writes ``-order_id`` as a
    synthetic id for every eBull-originated BUY/ADD fill, live branch included,
    because the broker's real position id is not in the order response. Those
    rows describe OUR record of a fill, not a position the broker can close;
    posting to ``…/market-close-orders/positions/-123`` addresses nothing. The
    sign is the documented partition between the two namespaces (see
    ``_persist_broker_position``), so requiring a positive id selects exactly
    the broker-assigned lots.

    ``is_buy`` — this path's accounting assumes a long throughout:
    ``_update_position_exit`` accrues ``(price - avg_cost) * units`` and
    ``_record_cash_ledger`` CREDITS cash on EXIT, which is a sale, not a
    buy-to-close. Shorting is permitted for research and paper trading, so
    ``portfolio_sync`` can import a short lot; selected by age it would be closed
    and booked against a long basis. Returning None instead lands on the caller's
    existing audited failure path, which is fail-closed — it refuses to submit,
    it does not de-risk the short.

    The lot's ``units`` come back with it so the caller can record what it
    actually asked the broker to close rather than the aggregate ledger position.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT position_id, units FROM broker_positions
            WHERE instrument_id = %(iid)s
              AND units > 0
              AND is_buy
              AND position_id > 0
            ORDER BY open_date_time ASC, position_id ASC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ExitLot(position_id=int(row["position_id"]), units=Decimal(str(row["units"])))


def _load_cash(conn: psycopg.Connection[Any]) -> Decimal | None:
    """Return current cash balance, or None if the ledger is empty."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT SUM(amount) AS balance FROM cash_ledger")
        row = cur.fetchone()
    if row is None or row["balance"] is None:
        return None
    return Decimal(str(row["balance"]))


# ---------------------------------------------------------------------------
# Demo-mode synthetic fill
# ---------------------------------------------------------------------------


def _synthetic_fill(
    instrument_id: int,
    action: str,
    quote_price: Decimal | None,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
    params: OrderParams | None = None,
    bid: Decimal | None = None,
    ask: Decimal | None = None,
) -> BrokerOrderResult:
    """Build a synthetic BrokerOrderResult for demo mode.

    Uses bid/ask for realistic pricing when available:
    - BUY fills at ask, EXIT fills at bid (worst-case execution)
    - Fees = 0. The half-spread cost of crossing the spread is already
      embedded in the bid/ask execution price; charging fees on top
      would double-count the spread in the cash ledger
      (`gross_amount + fees` debit on BUY, `gross_amount - fees` credit
      on EXIT). See issue #255.
    Falls back to last price with zero fees when bid/ask unavailable.
    """
    # Determine fill price: BUY at ask, EXIT at bid, fallback to last.
    # A non-positive book side / last is treated as missing (#1439): a 0.00
    # ask must not override a valid last and price the fill at 0. The rule
    # is owned by quote_marks.directional_fill_price (#1465) so the manual
    # order route applies the identical pricing; None (no usable side) is
    # coerced to 0 here to preserve this path's existing zero-price guards.
    price = directional_fill_price(action, quote_price, bid, ask) or Decimal("0")

    # Fail-closed for demo EXIT with no quote (#241).
    #
    # For BUY/ADD with amount-based sizing the units calculation below
    # produces 0 when price=0, the outer guard ``fu > 0`` skips
    # persistence, and the recommendation ends in a failed state. That
    # is correct.
    #
    # For EXIT, ``requested_units`` is loaded from the existing
    # position, so units is NON-ZERO even when price=0. The outer
    # guard would let _persist_fill record a sale at zero, the cash
    # ledger would credit 0, _update_position_exit would deduct the
    # position to zero, and the report would log a realised loss
    # equal to the position's open cost basis. Bail explicitly here
    # — leave the position open and the next quote refresh can retry.
    if price == 0 and action == "EXIT":
        return BrokerOrderResult(
            broker_order_ref=f"DEMO-{instrument_id}-{action}",
            status="failed",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={
                "demo": True,
                "instrument_id": instrument_id,
                "action": action,
                "error": "no quote available for EXIT — cannot price fill",
            },
        )

    if requested_units is not None:
        units = requested_units
    elif requested_amount is not None and price > 0:
        units = (requested_amount / price).quantize(Decimal("0.000001"))
    else:
        units = Decimal("0")

    # Synthetic fills price at bid/ask when available, which already
    # contains the half-spread vs mid. No additional fee — see #255.
    fees = Decimal("0")

    payload: dict[str, Any] = {
        "demo": True,
        "instrument_id": instrument_id,
        "action": action,
        "price": str(price),
        "units": str(units),
        "fees": str(fees),
        "note": "synthetic fill — no real API call"
        + ("" if quote_price is not None or ask is not None else "; no quote available, price=0"),
    }
    if params is not None:
        if params.stop_loss_rate is not None:
            payload["stop_loss_rate"] = str(params.stop_loss_rate)
        if params.take_profit_rate is not None:
            payload["take_profit_rate"] = str(params.take_profit_rate)

    return BrokerOrderResult(
        broker_order_ref=f"DEMO-{instrument_id}-{action}",
        status="filled",
        filled_price=price,
        filled_units=units,
        fees=fees,
        raw_payload=payload,
    )


# ---------------------------------------------------------------------------
# DB writers (all inside a single transaction)
# ---------------------------------------------------------------------------


_SUBMITTED_INTENT_PAYLOAD: dict[str, Any] = {"intent": "submitted_pre_broker_call"}


def _persist_submitted_intent(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    recommendation_id: int,
    decision_id: int,
    action: str,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
    now: datetime,
) -> tuple[int, UUID]:
    """Insert a durable order-intent row BEFORE the broker call (#243).

    The row carries ``status='submitted'``, a sentinel ``raw_payload_json`` so
    a reconciler can find rows whose broker call never returned, and — since
    #2942 — the durable ``recommendation_request_id`` that is sent as
    ``x-request-id`` and never rotated. The caller MUST ``conn.commit()`` after
    this returns and BEFORE issuing the external broker call — otherwise the
    intent stays inside the implicit transaction and a process crash erases it
    along with everything else, UUID included.

    ``idx_orders_recommendation_open_attempt`` makes this INSERT the CLAIM: at
    most one unresolved attempt may exist per recommendation. A second attempt
    raises ``psycopg.errors.UniqueViolation``, which the caller translates to
    ``PriorSubmissionUnresolvedError``. The claim lives in the database because
    the failure mode is a restarted or concurrent second pass, which no
    application-level ``SELECT … WHERE status IN (…)`` can exclude.
    """
    request_id = uuid4()
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO orders
                (instrument_id, recommendation_id, decision_id,
                 action, order_type, requested_amount, requested_units,
                 status, broker_order_ref, raw_payload_json, created_at,
                 recommendation_request_id)
            VALUES
                (%(iid)s, %(rid)s, %(did)s,
                 %(action)s, %(otype)s, %(amt)s, %(units)s,
                 'submitted', NULL, %(payload)s, %(now)s,
                 %(request_id)s)
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
                "payload": Jsonb(_SUBMITTED_INTENT_PAYLOAD),
                "now": now,
                "request_id": request_id,
            },
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("orders INSERT (submitted intent) returned no row")
    return int(row["order_id"]), request_id


def _update_order_with_broker_result(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    status: str,
    broker_order_ref: str | None,
    raw_payload: dict[str, Any],
) -> None:
    """Update the pre-call ``submitted`` row with the broker response (#243).

    Raises ``RuntimeError`` if the UPDATE matched zero rows — without
    this check, ``order_id`` would silently flow forward as a foreign
    key into fills / cost records / positions and corrupt referential
    integrity. PR #637 review BLOCKING.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
            SET status = %(status)s,
                broker_order_ref = %(ref)s,
                raw_payload_json = %(payload)s
            WHERE order_id = %(oid)s
            """,
            {
                "oid": order_id,
                "status": status,
                "ref": broker_order_ref,
                "payload": Jsonb(raw_payload),
            },
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"_update_order_with_broker_result: expected to update exactly "
                f"1 orders row for order_id={order_id}, matched {cur.rowcount}. "
                f"Either the pre-call intent INSERT was lost or order_id is "
                f"stale — refusing to advance to fill/cost/position writes."
            )


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
            -- Reset source on reopen: if the existing row is fully
            -- closed (current_units <= 0) this BUY is reopening it
            -- under eBull, so source flips to 'ebull'. Otherwise
            -- preserve the existing source — an eBull ADD into an
            -- already-open broker_sync position shouldn't claim
            -- ownership of the original external open.
            --
            -- Evaluation order: in Postgres ON CONFLICT DO UPDATE,
            -- every SET expression reads from the *pre-update* row
            -- snapshot — SET is not a sequential assignment.  So
            -- `positions.current_units` in this CASE WHEN refers to
            -- the value BEFORE the `current_units = ...` assignment
            -- above, regardless of SET ordering.  See
            -- https://www.postgresql.org/docs/current/sql-insert.html
            -- (ON CONFLICT DO UPDATE — "existing row" semantics).
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
            "price": filled_price,
            "units": filled_units,
            "cost": new_cost,
            "now": now,
        },
    )


def _persist_broker_position(
    conn: psycopg.Connection[Any],
    order_id: int,
    instrument_id: int,
    filled_price: Decimal,
    filled_units: Decimal,
    fees: Decimal,
    order_params: OrderParams | None,
    raw_payload: dict[str, Any],
    now: datetime,
) -> None:
    """Insert a broker_positions row for an eBull-originated BUY/ADD fill.

    Uses ``-order_id`` as the synthetic ``position_id`` (#227). The
    sign partitions the synthetic-id namespace from real
    broker-assigned position_ids — eToro's positionID is unsigned in
    practice, so a negative synthetic id can never collide with one
    pulled in by a future portfolio sync. Pairs with the matching
    convention in ``app/api/orders._persist_order_and_fill``.

    ⚠ Since #3006 this row is NOT visible to ``_load_exit_lot``, which
    requires ``position_id > 0``. That is deliberate and corrects the
    previous behaviour: a synthetic id is our record of a fill, not a
    handle the broker can close, so an EXIT that selected one would post
    to a position that does not exist. The cost is that an EXIT
    recommendation following an eBull BUY must wait for the next broker
    sync to learn the real lot id; the previous alternative was a
    guaranteed-invalid close request.

    ON CONFLICT: if the same synthetic id ever recurs (e.g. a re-run
    of the same order), update units/amount/updated_at.
    """
    gross = filled_price * filled_units
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
            open_rate = EXCLUDED.open_rate,
            open_conversion_rate = EXCLUDED.open_conversion_rate,
            total_fees = EXCLUDED.total_fees,
            raw_payload = EXCLUDED.raw_payload,
            updated_at = EXCLUDED.updated_at
        """,
        {
            "pid": -order_id,
            "iid": instrument_id,
            "units": filled_units,
            "amount": gross,
            "price": filled_price,
            "now": now,
            "sl": order_params.stop_loss_rate if order_params else None,
            "tp": order_params.take_profit_rate if order_params else None,
            "no_sl": order_params is None or order_params.stop_loss_rate is None,
            "no_tp": order_params is None or order_params.take_profit_rate is None,
            "leverage": order_params.leverage if order_params else 1,
            "tsl": order_params.is_tsl_enabled if order_params else False,
            "fees": fees,
            "payload": Jsonb(raw_payload),
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


def _maybe_trigger_attribution(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    current_units_after: Decimal,
) -> None:
    """Compute and persist return attribution if the position is fully closed.

    Called after an EXIT fill updates the position. If current_units_after is
    zero (or negative due to rounding), the position is closed and attribution
    is computed.

    Errors are logged and swallowed — attribution is best-effort and must
    never abort the order execution path.
    """
    if current_units_after > Decimal("0"):
        return

    try:
        # Savepoint isolates attribution from the outer transaction.
        # If a DB error occurs inside, the savepoint rolls back and the
        # outer transaction stays healthy for cash_ledger / rec status writes.
        with conn.transaction():
            result = compute_attribution(conn, instrument_id)
            if result is not None:
                persist_attribution(conn, result)
                logger.info(
                    "execute_order: attribution computed for instrument_id=%d gross=%.4f alpha=%.4f",
                    instrument_id,
                    result.gross_return_pct,
                    result.model_alpha_pct,
                )
    except Exception:
        logger.error(
            "execute_order: attribution failed for instrument_id=%d",
            instrument_id,
            exc_info=True,
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


def describe_exit_completion(
    *,
    lot_units_selected: Decimal,
    units_closed: Decimal,
    units_open_after: Decimal,
) -> dict[str, Any]:
    """Describe how much of the POSITION an EXIT actually closed (#3006).

    An EXIT recommendation expresses an instrument-wide intent, but the broker
    call closes ONE lot. Without this the audit trail records a successful order
    and says nothing about the exposure still open, so a partial de-risk is
    indistinguishable from a complete one.

    Pure on purpose: the caller supplies the three quantities it has already
    measured, and it is only called on a path where all three are known — a
    close the broker acknowledged without reporting units tells us nothing about
    completion and must not be described as if it did.

    ``units_open_after`` is the LOCAL ledger's ``positions.current_units`` after
    the fill was applied. It is our record of remaining exposure, not a broker
    observation; the two can disagree until the next portfolio sync.
    """
    return {
        "exit_scope": "lot",
        "lot_units_selected": str(lot_units_selected),
        "units_closed": str(units_closed),
        "units_open_after": str(units_open_after),
        "position_fully_closed": units_open_after <= Decimal("0"),
    }


def _write_execution_audit(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    recommendation_id: int,
    order_id: int,
    passed: bool,
    explanation: str,
    raw_payload: dict[str, Any],
    now: datetime,
    exit_completion: dict[str, Any] | None = None,
) -> None:
    """
    Write a decision_audit row recording the execution outcome.

    Uses the same PASS/FAIL vocabulary as the execution guard so the
    pass_fail column is semantically consistent across stages.  The
    detailed execution status goes into explanation.

    ``exit_completion`` carries ``describe_exit_completion``'s reading when the
    order was an EXIT that produced a fill; it is absent on every other path
    because completion is unknown there (#3006).
    """
    evidence: dict[str, Any] = {"order_id": order_id, "raw_payload": raw_payload}
    if exit_completion is not None:
        evidence["exit_completion"] = exit_completion
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id, stage,
             pass_fail, explanation, evidence_json)
        VALUES
            (%(dt)s, %(iid)s, %(rid)s, %(stage)s,
             %(pf)s, %(expl)s, %(ev)s)
        """,
        {
            "dt": now,
            "iid": instrument_id,
            "rid": recommendation_id,
            "stage": STAGE,
            "pf": "PASS" if passed else "FAIL",
            "expl": explanation,
            "ev": Jsonb(evidence),
        },
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class SafetyLayerDisabledError(RuntimeError):
    """Raised when BUY/ADD execution is attempted with a safety-critical
    layer (fx_rates or portfolio_sync) disabled by the operator."""


class TransactionCostUnavailableError(RuntimeError):
    """BUY/ADD cannot execute while a cost component is unestablished."""


class SubmissionControlsRevokedError(RuntimeError):
    """The operator's standing trading authority no longer holds (#2943).

    Raised when the kill switch or ``enable_auto_trading`` changed between
    guard approval and submission. Carries ``failed_rules`` so the caller can
    log which control closed without re-deriving it from the message.
    """

    def __init__(self, message: str, failed_rules: list[str]) -> None:
        super().__init__(message)
        self.failed_rules = failed_rules


class PriorSubmissionUnresolvedError(RuntimeError):
    """This recommendation already holds an unresolved submission claim (#2942).

    An earlier attempt reached (or may have reached) the broker and its fate is
    not yet known. Submitting again would create a SECOND economic order, so
    the path refuses. The recommendation stays ``approved``: the refusal is a
    statement about the outstanding attempt, not about the recommendation's
    merit.
    """

    def __init__(self, message: str, order_id: int | None = None) -> None:
        super().__init__(message)
        self.order_id = order_id


class BrokerSubmissionUncertainError(RuntimeError):
    """The broker call neither succeeded nor was refused (#2942).

    A transport failure, a 5xx, a 408/409/425/429 or an unreadable response
    leaves the order's fate unknown — it may already exist at the broker. The
    intent row is parked at ``status='uncertain'`` with the response evidence,
    which keeps the claim held so nothing re-submits.
    """

    def __init__(self, message: str, order_id: int) -> None:
        super().__init__(message)
        self.order_id = order_id


def _assert_submission_controls(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
    instrument_id: int,
    runtime: RuntimeConfig,
    now: datetime,
) -> None:
    """Refuse submission when the operator's trading authority has changed.

    Applies to EVERY action, EXIT included — which preserves the established
    exit policy rather than tightening it: the guard already evaluates these
    same controls on every action (`execution_guard` module docstring, "All
    actions"). "EXIT is never blocked" governs thesis / coverage / spread,
    not the kill switch.

    On refusal this COMMITS a ``decision_audit`` FAIL row before raising.
    The commit is required, not incidental: the scheduler calls this inside
    ``with connect_job() as conn``, which rolls back on exception, so an
    uncommitted audit row would vanish exactly when it matters. The write is
    the only one outstanding at this point in ``execute_order`` (the intent
    INSERT happens later), so committing publishes nothing else — the same
    connection-ownership contract the intent commit relies on.

    The recommendation is deliberately left ``approved``: the controls are a
    statement about NOW, not about the recommendation's merit, so the next
    scheduler pass re-evaluates them and refuses again for as long as they
    hold. Revoking the approval instead would discard a still-valid EXIT.
    """
    # runtime_corrupt=False is not an assumption: ``runtime`` is typed
    # ``RuntimeConfig``, not ``RuntimeConfig | None``, so corruption cannot
    # reach here as a value. ``get_runtime_config`` either returns a valid
    # config or raises ``RuntimeConfigCorrupt``, which propagates out of
    # execute_order (Step 3) and never gets this far. The guard passes the
    # flag because it collects the corruption as a rule result instead of
    # raising; this caller has no such state to report.
    results = decide_submission_controls(
        load_kill_switch(conn),
        runtime,
        runtime_corrupt=False,
    )
    failed = [r for r in results if not r.passed]
    if not failed:
        return

    failed_rules = [r.rule for r in failed]
    explanation = "Submission refused — " + "; ".join(r.detail or r.rule for r in failed)
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id, stage,
             pass_fail, explanation, evidence_json)
        VALUES
            (%(dt)s, %(iid)s, %(rid)s, %(stage)s,
             %(pf)s, %(expl)s, %(ev)s)
        """,
        {
            "dt": now,
            "iid": instrument_id,
            "rid": recommendation_id,
            "stage": STAGE,
            "pf": "FAIL",
            "expl": explanation,
            "ev": Jsonb([{"rule": r.rule, "passed": r.passed, "detail": r.detail} for r in results]),
        },
    )
    conn.commit()

    logger.warning(
        "execute_order: submission refused for recommendation_id=%d rules=%s",
        recommendation_id,
        failed_rules,
    )
    raise SubmissionControlsRevokedError(explanation, failed_rules)


def _assert_safety_layers_enabled_for_buy_add(
    conn: psycopg.Connection[Any],
    action: str,
) -> None:
    """Second-line defence: execute_order re-checks safety_layers_enabled
    for BUY/ADD. The guard rule at evaluate_recommendation time is the
    first line; this catches the window where a rec is approved, then
    the operator disables a safety layer before execution fires.
    EXIT is never gated — emergency de-risk must always be possible.
    """
    if action not in ("BUY", "ADD"):
        return
    from app.services.layer_enabled import is_layer_enabled

    disabled = [name for name in ("fx_rates", "portfolio_sync") if not is_layer_enabled(conn, name)]
    if disabled:
        raise SafetyLayerDisabledError(
            f"{' + '.join(disabled)} disabled — BUY/ADD execution aborted; re-enable the layer to proceed.",
        )


def _assert_transaction_cost_complete_for_buy_add(
    conn: psycopg.Connection[Any], action: str, instrument_id: int
) -> None:
    """Re-check cost provenance immediately before a possible broker call."""
    if action not in ("BUY", "ADD"):
        return
    missing = missing_cost_components(load_instrument_cost(conn, instrument_id))
    if missing:
        raise TransactionCostUnavailableError(
            f"BUY/ADD execution aborted: costs not established for {', '.join(missing)}"
        )


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


_CLAIM_INDEX = "idx_orders_recommendation_open_attempt"


def _write_refusal_audit(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    recommendation_id: int,
    explanation: str,
    evidence: dict[str, Any],
    now: datetime,
) -> None:
    """Record a submission refusal and COMMIT it.

    The commit is load-bearing, not incidental: every caller of this helper
    raises immediately afterwards, and ``connect_job`` rolls back on a raising
    path — so an uncommitted audit row would vanish exactly when it matters
    (#2943's lesson, re-applied here).

    ⚠ The commit is NOT guaranteed to publish only this row. Every caller has
    already resolved the order row for the same refusal, and
    ``_release_claim_after_pre_io_refusal`` leaves its ``UPDATE orders SET
    status='refused'`` outstanding when it calls here — so that UPDATE lands on
    this commit. That is intended: the resolved status and its audit row are
    one refusal and must become visible together or not at all. What the
    caller contract actually requires is the #243 one it inherits — do not pass
    a connection carrying UNRELATED uncommitted writes.
    """
    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id, stage,
             pass_fail, explanation, evidence_json)
        VALUES
            (%(dt)s, %(iid)s, %(rid)s, %(stage)s, 'FAIL', %(expl)s, %(ev)s)
        """,
        {
            "dt": now,
            "iid": instrument_id,
            "rid": recommendation_id,
            "stage": STAGE,
            "expl": explanation,
            "ev": Jsonb(evidence),
        },
    )
    conn.commit()


def _claim_submission(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    recommendation_id: int,
    decision_id: int,
    action: str,
    requested_amount: Decimal | None,
    requested_units: Decimal | None,
    now: datetime,
) -> tuple[int, UUID]:
    """Take the single submission claim for this recommendation (#2942).

    Returns the committed ``(order_id, request_id)``. The commit happens here
    so the durable intent and its request identity survive a crash during the
    broker call that follows.

    Raises ``PriorSubmissionUnresolvedError`` when an unresolved attempt
    already exists. That is the whole point of the ticket: an interrupted
    submission previously left the recommendation ``approved``, and the next
    scheduler pass minted a fresh intent and a fresh ``x-request-id`` and
    submitted a SECOND economic order.
    """
    try:
        order_id, request_id = _persist_submitted_intent(
            conn,
            instrument_id=instrument_id,
            recommendation_id=recommendation_id,
            decision_id=decision_id,
            action=action,
            requested_amount=requested_amount,
            requested_units=requested_units,
            now=now,
        )
    except psycopg.errors.UniqueViolation as exc:
        # Match the claim index by name. Catching every UniqueViolation here
        # would silently reinterpret an unrelated constraint failure as "a
        # prior attempt exists", which is a different and wrong story.
        if exc.diag.constraint_name != _CLAIM_INDEX:
            raise
        # A UniqueViolation aborts the transaction: no further statement can
        # run on this connection until it is rolled back, including the audit
        # INSERT below.
        conn.rollback()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT order_id, status, recommendation_request_id
                FROM orders
                WHERE recommendation_id = %(rid)s
                  AND status IN ('submitted', 'pending', 'uncertain')
                """,
                {"rid": recommendation_id},
            )
            outstanding = cur.fetchone()
        held_order_id = int(outstanding["order_id"]) if outstanding is not None else None
        explanation = (
            f"Submission refused — recommendation {recommendation_id} already holds an "
            f"unresolved submission claim (order_id={held_order_id}); resolve it before re-submitting"
        )
        _write_refusal_audit(
            conn,
            instrument_id=instrument_id,
            recommendation_id=recommendation_id,
            explanation=explanation,
            evidence={
                "refusal": "prior_submission_unresolved",
                "held_order_id": held_order_id,
                "held_status": str(outstanding["status"]) if outstanding is not None else None,
                "held_request_id": (
                    str(outstanding["recommendation_request_id"])
                    if outstanding is not None and outstanding["recommendation_request_id"] is not None
                    else None
                ),
            },
            now=now,
        )
        logger.error(
            "execute_order: refusing recommendation_id=%d — unresolved attempt order_id=%s",
            recommendation_id,
            held_order_id,
        )
        raise PriorSubmissionUnresolvedError(explanation, held_order_id) from exc

    conn.commit()
    return order_id, request_id


def _release_claim_after_pre_io_refusal(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    instrument_id: int,
    recommendation_id: int,
    reason: str,
    now: datetime,
) -> None:
    """Release the claim for a refusal that provably never reached the broker.

    ``refuse_broker_mutation_if_unattended`` raises at the TOP of every
    mutating provider method, before credentials are read and before a request
    is built — so no order can exist at the broker and holding the claim would
    park the recommendation permanently, including after the operator does
    exactly what the refusal message asks and re-runs from the main checkout.

    The intent row is resolved to ``status='refused'`` rather than deleted: the
    attempt happened and belongs in the audit trail, and ``refused`` is outside
    ``idx_orders_recommendation_open_attempt``'s predicate so the claim lifts.
    The recommendation stays ``approved``.

    ⚠ This applies ONLY to an exception whose contract is "raised before any
    I/O". An arbitrary exception out of a broker call may have left a request
    on the wire and must stay uncertain.
    """
    conn.execute(
        """
        UPDATE orders
        SET status = 'refused', raw_payload_json = %(payload)s
        WHERE order_id = %(oid)s
        """,
        {"oid": order_id, "payload": Jsonb({"refusal": "pre_io_refusal", "detail": reason})},
    )
    _write_refusal_audit(
        conn,
        instrument_id=instrument_id,
        recommendation_id=recommendation_id,
        explanation=f"Submission refused before broker I/O — order_id={order_id}: {reason}",
        evidence={"refusal": "pre_io_refusal", "order_id": order_id, "detail": reason},
        now=now,
    )
    logger.warning(
        "execute_order: recommendation_id=%d order_id=%d refused before broker I/O — %s",
        recommendation_id,
        order_id,
        reason,
    )


def _park_uncertain_submission(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    instrument_id: int,
    recommendation_id: int,
    exc: BrokerOrderSubmissionUncertain,
    now: datetime,
) -> None:
    """Park an attempt whose outcome the broker never told us, and COMMIT.

    The order goes to ``status='uncertain'`` carrying the response evidence,
    which keeps ``idx_orders_recommendation_open_attempt`` held so nothing
    re-submits. The recommendation goes to ``execution_pending`` rather than
    ``execution_failed``: the previous behaviour booked an order that may well
    have landed as a terminal failure, leaving its position unowned.

    No fill, position or cash-ledger write happens on this path — we do not
    know that anything executed.
    """
    with conn.transaction():
        conn.execute(
            """
            UPDATE orders
            SET status = 'uncertain', raw_payload_json = %(payload)s
            WHERE order_id = %(oid)s
            """,
            {"oid": order_id, "payload": Jsonb(exc.raw_payload)},
        )
        conn.execute(
            "UPDATE trade_recommendations SET status = 'execution_pending' WHERE recommendation_id = %(rid)s",
            {"rid": recommendation_id},
        )
    _write_refusal_audit(
        conn,
        instrument_id=instrument_id,
        recommendation_id=recommendation_id,
        explanation=f"Submission outcome unknown — order_id={order_id} parked as uncertain: {exc}",
        evidence={"refusal": "broker_submission_uncertain", "order_id": order_id, "response": exc.raw_payload},
        now=now,
    )
    logger.error(
        "execute_order: recommendation_id=%d order_id=%d parked uncertain — %s",
        recommendation_id,
        order_id,
        exc,
    )


def execute_order(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
    decision_id: int,
    broker: BrokerProvider | None = None,
) -> ExecuteResult:
    """
    Execute a guard-approved order.

    Steps:
      1. Load the approved recommendation (raises if not found or not approved).
      2. Determine order parameters from the recommendation.
      3. **Live mode only (#243)**: INSERT a durable ``status='submitted'``
         intent row and ``conn.commit()`` so a process crash mid-broker-call
         leaves a row a reconciler can chase. Then call the broker.
         **Demo mode**: generate a synthetic fill (no external side effect).
      4. Live: UPDATE the pre-call intent row with the broker response.
         Demo / live-EXIT-no-position: INSERT a fresh order row.
      5. If filled: persist fill, update position, record cash ledger entry.
      DB writes in steps 4-5 are inside a single transaction.

    No external I/O is performed inside any DB transaction.

    **Connection ownership contract (#243)**: live-mode paths issue a
    ``conn.commit()`` between the intent INSERT and the broker call.
    Callers MUST therefore pass a connection that does NOT carry
    unrelated uncommitted writes — the commit would publish them
    too. The current scheduler caller (app/workers/scheduler.py)
    uses a fresh per-order pool connection, which satisfies this.
    Do not call ``execute_order`` inside a caller-owned outer
    transaction.

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
    _assert_safety_layers_enabled_for_buy_add(conn, action)
    _assert_transaction_cost_complete_for_buy_add(conn, action, instrument_id)

    # --- Step 2: determine order parameters ---
    # requested_amount is the dollar amount to invest.
    # Units: cash (USD) * suggested_size_pct (fraction) = dollar amount (USD).
    requested_amount: Decimal | None = None
    requested_units: Decimal | None = None

    if action == "EXIT":
        requested_units = _load_position_units(conn, instrument_id)
    elif rec["suggested_size_pct"] is not None:
        cash = _load_cash(conn)
        if cash is not None and cash > 0:
            requested_amount = cash * Decimal(str(rec["suggested_size_pct"]))

    # --- Step 2b: build OrderParams from recommendation SL/TP ---
    # SL/TP are set by the entry timing service (Phase 0) for BUY/ADD recs.
    # For EXIT recs, both are NULL — no SL/TP on a close-position order.
    # Explicit key access: crash loudly if columns are missing from the query
    # (would indicate a code bug, not a data issue).
    order_params: OrderParams | None = None
    sl_raw = rec["stop_loss_rate"]
    tp_raw = rec["take_profit_rate"]
    if sl_raw is not None or tp_raw is not None:
        order_params = OrderParams(
            stop_loss_rate=Decimal(str(sl_raw)) if sl_raw is not None else None,
            take_profit_rate=Decimal(str(tp_raw)) if tp_raw is not None else None,
        )
    if action in ("BUY", "ADD") and order_params is None:
        logger.warning(
            "execute_order: BUY/ADD rec=%d has no SL/TP — timing may not have run",
            recommendation_id,
        )

    # --- Step 3: call broker or demo mode ---
    # Read live-mode flag from runtime_config (DB-backed source of truth).
    # Any RuntimeConfigCorrupt propagates: we will NOT default to demo mode
    # silently when live mode could have been intended (or the reverse).
    # Callers must have already passed execution_guard, which fails closed
    # on the same condition.
    runtime = get_runtime_config(conn)

    # #2943: re-evaluate the operator's standing trading authority against
    # LIVE state, immediately before any entry authority is taken. The guard
    # checked these at approval time, which may have been minutes or days
    # ago; an operator who has since hit the kill switch or turned auto
    # trading off must not have a stale approval fire behind them. Same
    # function the guard runs, so the two rule sets cannot drift.
    _assert_submission_controls(conn, recommendation_id, instrument_id, runtime, now)

    is_live = runtime.enable_live_trading

    quote_data: dict[str, Any] | None = None
    submitted_order_id: int | None = None
    # #3006: set only on the live EXIT path, and only once a lot is resolved.
    exit_lot: ExitLot | None = None
    exit_completion: dict[str, Any] | None = None

    if is_live:
        if broker is None:
            raise ValueError("enable_live_trading is True but no broker provider supplied")
        if action == "EXIT":
            exit_lot = _load_exit_lot(conn, instrument_id)
            if exit_lot is None:
                # Pre-024 position without broker_positions row, or no
                # broker-closeable LONG lot — a synthetic-id row or a short
                # lot is not something this path can close (#3006).
                logger.error(
                    "EXIT for instrument_id=%d: no broker-closeable long lot found",
                    instrument_id,
                )
                broker_result = BrokerOrderResult(
                    broker_order_ref=None,
                    status="failed",
                    filled_price=None,
                    filled_units=None,
                    fees=Decimal("0"),
                    raw_payload={
                        "error": (
                            f"No broker-closeable long broker_positions row for instrument {instrument_id}"
                        )
                    },
                )
            else:
                # #3006: record what is actually asked of the broker. Step 2
                # sized this from ``positions.current_units``, the aggregate
                # across every open lot, while the call below closes exactly
                # ONE lot — so the aggregate described a request the broker
                # never received.
                #
                # ⚠ Descriptive only. ``close_position`` is called with
                # ``units_to_deduct=None``, i.e. close the lot WHOLE; sending a
                # units figure derived from a possibly-stale ``broker_positions``
                # row would only add a rejection mode. ⚠
                # ``broker_positions.units`` is numeric(20,8) and
                # ``orders.requested_units`` is numeric(18,6), so this copy
                # rounds — acceptable because nothing consumes it as a control
                # input.
                requested_units = exit_lot.units
                exit_pos_id = exit_lot.position_id
                # #243: persist the order intent BEFORE the broker
                # side effect, then commit so a crash mid-call leaves
                # a durable ``status='submitted'`` row that a
                # reconciler can chase against the broker.
                # #2942: that row now also carries the durable
                # ``x-request-id`` and takes the single submission claim.
                submitted_order_id, request_id = _claim_submission(
                    conn,
                    instrument_id=instrument_id,
                    recommendation_id=recommendation_id,
                    decision_id=decision_id,
                    action=action,
                    requested_amount=requested_amount,
                    requested_units=requested_units,
                    now=now,
                )
                try:
                    broker_result = broker.close_position(
                        exit_pos_id,
                        instrument_id=instrument_id,
                        request_id=request_id,
                    )
                except UnattendedExecutionRefused as exc:
                    _release_claim_after_pre_io_refusal(
                        conn,
                        order_id=submitted_order_id,
                        instrument_id=instrument_id,
                        recommendation_id=recommendation_id,
                        reason=str(exc),
                        now=now,
                    )
                    raise
                except BrokerOrderSubmissionUncertain as exc:
                    _park_uncertain_submission(
                        conn,
                        order_id=submitted_order_id,
                        instrument_id=instrument_id,
                        recommendation_id=recommendation_id,
                        exc=exc,
                        now=now,
                    )
                    raise BrokerSubmissionUncertainError(str(exc), submitted_order_id) from exc
        else:
            # #243: durable order intent before the broker call.
            # #2942: it carries the request identity and the claim.
            submitted_order_id, request_id = _claim_submission(
                conn,
                instrument_id=instrument_id,
                recommendation_id=recommendation_id,
                decision_id=decision_id,
                action=action,
                requested_amount=requested_amount,
                requested_units=requested_units,
                now=now,
            )
            try:
                broker_result = broker.place_order(
                    instrument_id=instrument_id,
                    action=action,
                    amount=requested_amount,
                    units=requested_units,
                    params=order_params,
                    request_id=request_id,
                )
            except UnattendedExecutionRefused as exc:
                _release_claim_after_pre_io_refusal(
                    conn,
                    order_id=submitted_order_id,
                    instrument_id=instrument_id,
                    recommendation_id=recommendation_id,
                    reason=str(exc),
                    now=now,
                )
                raise
            except BrokerOrderSubmissionUncertain as exc:
                _park_uncertain_submission(
                    conn,
                    order_id=submitted_order_id,
                    instrument_id=instrument_id,
                    recommendation_id=recommendation_id,
                    exc=exc,
                    now=now,
                )
                raise BrokerSubmissionUncertainError(str(exc), submitted_order_id) from exc
    else:
        quote_data = _load_quote_for_execution(conn, instrument_id)
        # Floor last/bid/ask to strictly-positive (#1439): a 0.00 row is not
        # a usable mark and must never price a synthetic fill at 0.
        quote_price = positive_decimal_or_none(quote_data.get("last")) if quote_data else None
        broker_result = _synthetic_fill(
            instrument_id=instrument_id,
            action=action,
            quote_price=quote_price,
            requested_amount=requested_amount,
            requested_units=requested_units,
            params=order_params,
            bid=positive_decimal_or_none(quote_data.get("bid")) if quote_data else None,
            ask=positive_decimal_or_none(quote_data.get("ask")) if quote_data else None,
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
        if submitted_order_id is not None:
            # #243 live path: pre-call intent already exists. UPDATE
            # the same row with the broker response so reconciliation
            # keys on a single durable identity.
            order_id = submitted_order_id
            _update_order_with_broker_result(
                conn,
                order_id=order_id,
                status=order_status,
                broker_order_ref=broker_result.broker_order_ref,
                raw_payload=broker_result.raw_payload,
            )
        else:
            # Demo path (no external side effect to lose) or live
            # EXIT with no broker_positions row (broker not called).
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

        # Record estimated cost for BUY/ADD only (entry cost is meaningless
        # for EXIT orders).  Best-effort: any failure here must not block the
        # order or abort the enclosing transaction.  The savepoint ensures a
        # DB error during cost recording rolls back only the cost INSERT,
        # leaving the outer transaction intact for _persist_fill.
        if action in ("BUY", "ADD"):
            try:
                with conn.transaction():
                    cost_config = get_transaction_cost_config(conn)
                    cost_model_row = load_instrument_cost(conn, instrument_id)
                    if cost_model_row is not None:
                        s_bps = cost_model_row["spread_bps"]
                        o_rate = cost_model_row["overnight_rate"]
                        fx_bps = cost_model_row["fx_markup_bps"]
                    else:
                        # No cost_model row — fall back to quote spread_pct.
                        # In live mode quote_data is not loaded for the fill,
                        # so load it here for cost recording.
                        qd = quote_data if quote_data is not None else _load_quote_for_execution(conn, instrument_id)
                        if qd is not None and qd.get("spread_pct") is not None:
                            s_bps = spread_pct_to_bps(Decimal(str(qd["spread_pct"])))
                        else:
                            s_bps = None
                        o_rate = Decimal("0")
                        fx_bps = Decimal("0")

                    if s_bps is not None:
                        cost_est = estimate_cost(
                            spread_bps=s_bps,
                            overnight_rate=o_rate,
                            fx_markup_bps=fx_bps,
                            hold_days=cost_config["default_hold_days"],
                            max_total_cost_bps=cost_config["max_total_cost_bps"],
                            min_return_vs_cost_ratio=cost_config["min_return_vs_cost_ratio"],
                            expected_return_pct=None,
                        )
                        record_estimated_cost(
                            conn,
                            order_id=order_id,
                            recommendation_id=recommendation_id,
                            instrument_id=instrument_id,
                            estimate=cost_est,
                        )
            except Exception:
                logger.warning(
                    "cost recording failed for order_id=%d — continuing without cost record",
                    order_id,
                    exc_info=True,
                )

        fp = broker_result.filled_price
        fu = broker_result.filled_units

        # Guard: a fill must have a strictly-positive price AND units to be
        # persisted. A zero-unit fill (demo mode, no quote) is not a real
        # fill; neither is a zero-PRICE fill — a units-based demo BUY/ADD with
        # no usable bid/ask/last would otherwise persist free holdings at
        # price 0 (#1439, prevention-log "Zero-value fills persisted as real
        # fills" #68). _synthetic_fill only fail-closes zero-price EXIT; this
        # guard closes the same hole for every action.
        if order_status == "filled" and fp is not None and fp > 0 and fu is not None and fu > 0:
            fill_id = _persist_fill(
                conn,
                order_id=order_id,
                price=fp,
                units=fu,
                fees=broker_result.fees,
                now=now,
            )

            if action in ("BUY", "ADD"):
                _update_position_buy(
                    conn,
                    instrument_id=instrument_id,
                    filled_price=fp,
                    filled_units=fu,
                    now=now,
                )
                _persist_broker_position(
                    conn,
                    order_id=order_id,
                    instrument_id=instrument_id,
                    filled_price=fp,
                    filled_units=fu,
                    fees=broker_result.fees,
                    order_params=order_params,
                    raw_payload=broker_result.raw_payload,
                    now=now,
                )
            elif action == "EXIT":
                _update_position_exit(
                    conn,
                    instrument_id=instrument_id,
                    filled_price=fp,
                    filled_units=fu,
                    now=now,
                )
                # Check if position is fully closed → trigger attribution
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(
                        "SELECT current_units FROM positions WHERE instrument_id = %(iid)s",
                        {"iid": instrument_id},
                    )
                    pos_row = cur.fetchone()
                units_after = Decimal(str(pos_row["current_units"])) if pos_row else Decimal("0")
                _maybe_trigger_attribution(conn, instrument_id, units_after)

                # #3006: an EXIT closes ONE lot but expresses an
                # instrument-wide intent. Record how much of the position is
                # still open so a partial de-risk is visible in the audit trail
                # instead of reading as a completed one. Only on the live path,
                # where a lot was resolved — demo has no lot to be partial about.
                if exit_lot is not None:
                    exit_completion = describe_exit_completion(
                        lot_units_selected=exit_lot.units,
                        units_closed=fu,
                        units_open_after=units_after,
                    )
                    if not exit_completion["position_fully_closed"]:
                        logger.warning(
                            "execute_order: EXIT recommendation_id=%d instrument_id=%d closed lot %d "
                            "(%s units) but %s units remain open — the position is NOT fully exited",
                            recommendation_id,
                            instrument_id,
                            exit_lot.position_id,
                            fu,
                            units_after,
                        )

            gross_amount = fp * fu
            _record_cash_ledger(conn, action, gross_amount, broker_result.fees, now)

        # Update recommendation status to reflect execution outcome
        if fill_id is not None:
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

        # Write execution outcome to decision_audit (every path, success or failure).
        # pass_fail uses PASS/FAIL vocabulary consistent with the execution guard.
        # Detailed status goes in explanation.
        _write_execution_audit(
            conn,
            instrument_id=instrument_id,
            recommendation_id=recommendation_id,
            order_id=order_id,
            passed=exec_status == "executed",
            explanation=f"status={exec_status} order_status={order_status} broker_ref={broker_result.broker_order_ref}",
            raw_payload=broker_result.raw_payload,
            now=now,
            exit_completion=exit_completion,
        )

        # Filled trade → queue an immediate portfolio sync so the
        # broker-observed event lands in trade_events within seconds
        # (#1593). Same transaction: NOTIFY fires only at commit.
        if fill_id is not None:
            enqueue_post_trade_sync(conn, requested_by="execute_order")

    # --- Build explanation ---
    if order_status == "filled" and fill_id is not None:
        explanation = (
            f"order filled: price={broker_result.filled_price} "
            f"units={broker_result.filled_units} "
            f"ref={broker_result.broker_order_ref}"
        )
        # #3006: never let an instrument-wide EXIT read as complete when it
        # closed one lot and left exposure open.
        if exit_completion is not None and not exit_completion["position_fully_closed"]:
            explanation += f"; POSITION NOT FULLY EXITED — {exit_completion['units_open_after']} units still open"
    elif order_status == "filled" and fill_id is None:
        explanation = "order reported filled but zero units — no fill persisted"
    elif order_status == "pending":
        explanation = f"order pending: ref={broker_result.broker_order_ref}"
    else:
        explanation = f"order {order_status}: {broker_result.raw_payload}"

    outcome: OrderOutcome
    if fill_id is not None:
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
