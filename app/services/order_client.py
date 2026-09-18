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
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Final, Literal, LiteralString
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.providers.broker import (
    BrokerOrderLookupError,
    BrokerOrderNotFound,
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
from app.services.strategy_order_reconciliation import (
    StrategyReconciliationError,
    classify_broker_order_status,
)
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

# The scale of ``orders.requested_units`` (numeric(18,6)). ``broker_positions.units``
# is numeric(20,8), so an EXIT lot can carry two decimals the order row cannot
# hold; #3006 logs the loss rather than letting it happen silently.
_REQUESTED_UNITS_STEP = Decimal("0.000001")

# The scale of ``broker_positions.units`` / ``.amount`` (numeric(20,8)). A fill
# is normalised to this before it is compared against, or subtracted from, a
# STORED lot — see ``_deduct_closed_exit_lot`` and #3017.
_BROKER_UNITS_STEP = Decimal("0.00000001")

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


#: The lots a recommendation EXIT could close at the broker, before ownership.
#: ⚠ Shared by :func:`_load_exit_lot` and :func:`_engine_owned_long_lot_count`
#: rather than written twice: the second reports what the first REFUSED, so a
#: divergence would not fail — it would quietly describe a different set to the
#: operator than the one that was actually excluded (review NITPICK on PR #3026).
#: Contains no caller-supplied text; ``%(iid)s`` stays a bound parameter.
#: The ``bp.`` alias is load-bearing, not cosmetic — ``own`` and ``bp`` both
#: carry a position id, so an unqualified reference inside the correlated
#: ``EXISTS`` below would resolve to the wrong one.
_BROKER_CLOSEABLE_LONG_LOT_SQL = """bp.instrument_id = %(iid)s
          AND bp.units > 0
          AND bp.is_buy
          AND bp.position_id > 0"""

#: The lot is claimed by the strategy engine RIGHT NOW (#3025). ``active`` only —
#: a ``released`` row means the engine has given the lot up, and matching on the
#: row's mere existence would strand every lot the engine ever touched.
_ENGINE_OWNED_SQL = """EXISTS (
              SELECT 1 FROM strategy_position_ownership own
              WHERE own.broker_position_id = bp.position_id
                AND own.status = 'active'
          )"""


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

    A third filter, added by #3025:

    ``NOT EXISTS`` an ACTIVE ``strategy_position_ownership`` row — the lot belongs
    to the strategy engine, and this path must not close it. The blindness was
    one-directional: ``strategy_engine_capital`` joins that table in five places
    so the engine only ever counts what it owns, and
    ``strategy_position_manager`` closes by exact ``broker_position_id``, so the
    engine can never reach a legacy lot; nothing in ``order_client`` read the
    table at all. ``position_id > 0`` partitions synthetic ids from
    broker-assigned ones and says nothing about ownership, so an engine lot
    imported by ``portfolio_sync`` satisfied every filter above.

    ⚠ Closing one would be the #2979 wedge reached from the other side: the
    broker no longer carries the position, nothing here releases the ownership
    row, and ``resolve_engine_capital_usage`` then refuses the join for every
    later ``execute_core_rebalance`` cycle.

    ⚠ ``status='active'`` only. A ``released`` row means the engine has given the
    lot up, so excluding it too would over-narrow.
    ``strategy_position_ownership.broker_position_id`` is ``UNIQUE`` and
    ``CHECK (> 0)``, so the anti-join is exact and cannot fan out.

    The lot's ``units`` come back with it so the caller can record what it
    actually asked the broker to close rather than the aggregate ledger position.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT position_id, units FROM broker_positions bp
            WHERE {_BROKER_CLOSEABLE_LONG_LOT_SQL}
              AND NOT {_ENGINE_OWNED_SQL}
            ORDER BY bp.open_date_time ASC, bp.position_id ASC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ExitLot(position_id=int(row["position_id"]), units=Decimal(str(row["units"])))


def _engine_owned_long_lot_count(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> int:
    """How many otherwise-closeable long lots #3025's filter excluded.

    Called ONLY when :func:`_load_exit_lot` returns ``None``, so the success path
    still costs one query. It exists so the refusal names the actual cause:
    "every candidate lot is engine-owned" and "there is no broker-closeable long
    lot at all" are different operator situations with different fixes, and
    #3003 settled that a backstop must not pre-empt the specific diagnosis.

    Shares :data:`_BROKER_CLOSEABLE_LONG_LOT_SQL` and :data:`_ENGINE_OWNED_SQL`
    with :func:`_load_exit_lot` and differs from it ONLY by dropping the ``NOT``,
    so the two cannot drift into disagreeing about what was excluded — which
    would make the operator-facing count describe a different set from the one
    actually refused.
    """
    row = conn.execute(
        f"""
        SELECT count(*) FROM broker_positions bp
        WHERE {_BROKER_CLOSEABLE_LONG_LOT_SQL}
          AND {_ENGINE_OWNED_SQL}
        """,
        {"iid": instrument_id},
    ).fetchone()
    assert row is not None
    return int(row[0])


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
    broker_env: str | None,
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

    #2942 half 2: the row is stamped ``recommendation_submission_phase =
    'claim_committed'`` here, and ``mark_recommendation_submission_entered``
    moves it to ``'broker_verb_entered'`` in a SEPARATE commit immediately
    before the provider call. The separation is the whole mechanism — folding
    the marker into this transaction would make every committed row read as
    "may have reached the broker" and prove nothing.

    #3189 finding 4(b): ``broker_env`` is recorded HERE and not from the
    response, because the environment is a property of the ATTEMPT — it has to
    survive the crash this row exists to survive. ``None`` writes NULL, which
    the poller reads as "not recorded" and polls exactly as it does today.
    """
    request_id = uuid4()
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO orders
                (instrument_id, recommendation_id, decision_id,
                 action, order_type, requested_amount, requested_units,
                 status, broker_order_ref, raw_payload_json, created_at,
                 recommendation_request_id, recommendation_submission_phase,
                 broker_environment)
            VALUES
                (%(iid)s, %(rid)s, %(did)s,
                 %(action)s, %(otype)s, %(amt)s, %(units)s,
                 'submitted', NULL, %(payload)s, %(now)s,
                 %(request_id)s, 'claim_committed',
                 %(broker_env)s)
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
                "broker_env": broker_env,
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

    Subtracts filled_units, withdraws the disposed share of the cost pool, and
    computes realized P&L based on avg_cost.

    **Source rule — HMRC s104 part disposal from an average-cost pool**, which
    this repo already implements for the tax layer in
    ``app/services/tax_ledger.py`` (``_match_disposals``): the cost withdrawn is
    the PROPORTIONAL fraction of the pool, ``(u / U) * cost``, not ``avg * u``.
    The fraction is used deliberately — ``avg = cost / U`` can be
    non-terminating, so multiplying it back re-introduces a rounding error that
    accumulates over repeated disposals. Full depletion takes the exact
    remainder (here: zero) rather than a computed value, which is what
    guarantees cost conservation over a sequence of part disposals.

    ``avg_cost`` is deliberately NOT recomputed. Under an average-cost pool a
    part disposal leaves the per-unit cost unchanged, and the arithmetic agrees:
    ``cost_basis * (1 - u/U) = avg * (U - u) = avg * units_after``. The
    ``realized_pnl`` line below already denominates the disposal at ``avg_cost``,
    so this is the treatment the statement was half-applying, not a new choice
    (#3008).

    ⚠ That identity holds to ROUNDING, not exactly, and the bound is measured:
    each ``round(…, 6)`` can move the basis by up to one unit in the column's
    last place, so after ``n`` part disposals the gap against
    ``avg_cost * current_units`` is at most ``n`` ULP (1.2e-6 after six, in
    ``tests/test_3008_partial_exit_cost_basis_db.py``). It does not compound into
    the final state: full depletion takes the exact remainder. For scale, the
    positions held on dev already sit up to 5e-4 from the same identity, because
    ``avg_cost`` is itself stored rounded — so this is well inside the noise the
    table already carries.

    ⚠ Until #3008 this statement moved ``current_units`` and left ``cost_basis``
    at its full original value, so a partial EXIT broke
    ``cost_basis = avg_cost * current_units`` — the invariant every row in the
    table actually satisfies. The stale-high basis then inflated ``avg_cost`` on
    the next ADD (``_update_position_buy`` recomputes it from
    ``positions.cost_basis``) and overstated the no-quote market-value fallback
    in ``app/services/portfolio.py`` and the ``pnl_pct`` denominator in
    ``app/services/reporting.py``. ``portfolio_sync`` does not repair it: for an
    existing local position it refreshes units and P&L only, by design.

    ⚠ Every SET expression reads the PRE-update row, so ``current_units`` inside
    the ``cost_basis`` expression is the value before the subtraction above it.
    SET is not sequential assignment — see
    https://www.postgresql.org/docs/current/sql-update.html.
    ⚠ Raises ``RuntimeError`` if the UPDATE matched zero rows (#3013). Without
    it, a missing ``positions`` row is silent and everything downstream proceeds
    as though the disposal booked: ``_persist_fill`` has already written the
    fill, ``_record_cash_ledger`` CREDITS the full proceeds, the post-fill read
    returns no row so ``units_after`` defaults to zero and
    ``_maybe_trigger_attribution`` reads that as "fully closed", and the
    recommendation goes ``executed`` with a PASS audit row. The ledger would
    book a disposal of a position it never held and credit the cash.

    The raise aborts the caller's enclosing ``with conn.transaction()``, so the
    fill, the cash credit and the recommendation status roll back together and
    the order is left for a reconciler — the correct outcome when the ledger
    cannot account for what the broker did. Same guard, and the same reasoning,
    as ``_update_order_with_broker_result``.
    """
    result = conn.execute(
        """
        UPDATE positions SET
            current_units  = current_units - %(units)s,
            cost_basis     = CASE
                -- Full (or over-) disposal: take the exact remainder. Also the
                -- guard that makes the division below unreachable at
                -- current_units = 0.
                WHEN current_units <= %(units)s THEN 0
                ELSE round(
                    cost_basis - (%(units)s / current_units) * cost_basis, 6
                )
            END,
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
    if result.rowcount != 1:
        raise RuntimeError(
            f"_update_position_exit: expected to update exactly 1 positions row "
            f"for instrument_id={instrument_id}, matched {result.rowcount}. "
            f"The broker closed a lot the ledger has no position for — refusing "
            f"to credit the proceeds or mark the recommendation executed."
        )


def _deduct_closed_exit_lot(
    conn: psycopg.Connection[Any],
    *,
    position_id: int,
    filled_units: Decimal,
    now: datetime,
) -> None:
    """Deduct a filled EXIT from the ``broker_positions`` lot it closed (#3020).

    Until this existed the EXIT path resolved a lot, closed it at the broker and
    left the mirror row untouched, so for up to one sync interval the closed
    position was still listed as open by ``app/api/portfolio.py`` (both trade
    lists select ``WHERE bp.units > 0``) and was still the FIFO-oldest row
    ``_load_exit_lot`` would hand to the NEXT EXIT — a close addressed to a
    position the broker no longer has.

    ``enqueue_post_trade_sync`` does queue a refresh, but it is asynchronous and
    best-effort: its own docstring says the scheduled tick (5 min) covers a lost
    enqueue, and a broker outage extends that indefinitely.

    The lock-and-re-read is the sibling's, and so is its reason (#245,
    ``app/api/orders.py``): two closes that both read ``units > 0`` outside the
    transaction would both build a fill, and the loser's UPDATE would match zero
    rows while its fill, cash and audit rows still committed.

    ⚠⚠ ``SKIP LOCKED``, and it is the whole reason this cannot deadlock. The
    tempting claim — "``positions`` first, then ``broker_positions``, matching
    ``portfolio_sync``" — is FALSE for the case this function exists to serve.
    The sync's order is: the upsert loop over the aggregated broker positions
    writes ``positions``; then ``_upsert_broker_positions`` writes the mirror and
    DELETEs lots absent from the payload; then the ``for row in local_rows`` loop
    returns to ``positions`` to zero the instruments that disappeared. An
    instrument whose last lot an EXIT just
    closed is exactly one that disappears, so the sync reaches its mirror row
    BEFORE its ``positions`` row while this transaction holds ``positions`` and
    would be waiting on the mirror — a cycle. Postgres would abort one side, and
    if it aborts this one the fill, cash credit and audit roll back for a close
    the broker has already executed. ``SKIP LOCKED`` means this transaction never
    WAITS on a mirror row, so no cycle can form; a contended row is left to the
    sync that holds it, which is the right outcome for a best-effort mirror
    write. (Codex, checkpoint 2.)

    ⚠ A mismatch WARNS and returns; it does not raise, and the difference from
    ``_update_position_exit`` is deliberate. ``positions`` is the ledger — a
    missed update there credits cash for a position never held, so it must abort
    the transaction. ``broker_positions`` is the broker MIRROR, replaced wholesale
    by the next sync, so a failed deduction is self-healing; raising would roll
    back a durable record of a fill the broker has already executed, which is
    strictly worse than a stale row that heals itself.

    ⚠ #3017's lesson, applied where it actually bites: the fill is normalised to
    the column's own grain ONCE, in Python, before either the guard or the
    statement sees it. A SQL-side cast alone would not be enough — the
    fewer-units guard below is a Python comparison between a raw fill and a
    STORED value, and a 9-dp fill against an 8-dp lot fails it on an exact whole
    close, silently leaving the closed lot selectable. ``ROUND_HALF_UP`` because
    that is how Postgres rounded the stored value on the way in.
    """
    filled_units = filled_units.quantize(_BROKER_UNITS_STEP, rounding=ROUND_HALF_UP)
    with conn.cursor() as lock_cur:
        lock_cur.execute(
            "SELECT units FROM broker_positions WHERE position_id = %(pid)s FOR UPDATE SKIP LOCKED",
            {"pid": position_id},
        )
        locked_row = lock_cur.fetchone()
    if locked_row is None:
        # Deliberately one branch for two causes: the row is gone, or a
        # concurrent sync holds it. Both mean "the mirror is not ours to move
        # right now", both are resolved by that same sync, and distinguishing
        # them would cost a second read that could only be stale by the time it
        # returned.
        logger.warning(
            "EXIT fill closed broker lot %d but its broker_positions row is absent or "
            "locked by a concurrent sync — mirror left to the next portfolio sync",
            position_id,
        )
        return
    locked_units = Decimal(str(locked_row[0]))
    if locked_units < filled_units:
        logger.warning(
            "EXIT fill closed broker lot %d for %s units but the row holds only %s — "
            "not deducting; mirror left to the next portfolio sync",
            position_id,
            filled_units,
            locked_units,
        )
        return

    result = conn.execute(
        """
        UPDATE broker_positions SET
            amount     = CASE
                WHEN units > 0
                THEN amount * (1 - %(units)s / units)
                ELSE 0
            END,
            units      = units - %(units)s,
            updated_at = %(now)s
        WHERE position_id = %(pid)s
          AND units >= %(units)s
        """,
        {"units": filled_units, "pid": position_id, "now": now},
    )
    if result.rowcount != 1:
        logger.warning(
            "EXIT fill deduction on broker lot %d matched %d rows despite the locked "
            "re-read — mirror left to the next portfolio sync",
            position_id,
            result.rowcount,
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


class ConcurrentSubmissionInFlightError(RuntimeError):
    """Another session is mid-submission for this recommendation (#2942 half 2).

    Raised when ``RECOMMENDATION_SUBMISSION_ADVISORY_LOCK`` cannot be taken.
    The path must NOT fall through to ``_claim_submission``: the other session
    may not have committed its claim yet, so the unique index would not stop
    this one and the result would be the second economic order the claim exists
    to prevent.
    """


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
    broker_env: str | None,
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
            broker_env=broker_env,
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

    ⚠ #2942 half 2: the row already reads
    ``recommendation_submission_phase='broker_verb_entered'`` by the time this
    runs, because the marker commits before the provider call and the guard
    raises inside it. That is deliberate, not an oversight — the marker is not
    pushed past the guard, because #2961 measured that placement as buying
    microseconds for a change to the ``BrokerProvider`` protocol. Nothing is
    lost: this path resolves the row to a terminal status directly, so the
    marker never has to answer for it.
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


#: #2942 half 2. Session-scoped advisory-lock namespace for the live
#: recommendation submission span. Keyed PER RECOMMENDATION, deliberately: a
#: global evidence key would let one unrelated in-flight submission starve
#: every other recommendation's reconciliation, a blast radius with no
#: relationship to the feature (#2961's second-half finding). Follows the
#: repo's ``(ticket, discriminator)`` key convention — cf.
#: ``CORE_MANDATE_ADVISORY_LOCK = (2603, 1)``.
RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS = 2942


def _concurrent_submission_error(recommendation_id: int, consequence: str) -> ConcurrentSubmissionInFlightError:
    """One place the "someone else holds the key" wording lives.

    Both raise sites mean the same thing about the world and differ only in what
    they were about to do, so the shared clause is built here and the caller
    supplies its own consequence (bot NITPICK, PR #3165).
    """
    return ConcurrentSubmissionInFlightError(
        f"another session is submitting recommendation {recommendation_id}; {consequence}"
    )


@contextmanager
def _recommendation_submission_try_lock(conn: psycopg.Connection[Any], recommendation_id: int) -> Iterator[bool]:
    """Take the per-recommendation submission key without waiting (#2942 half 2).

    ⚠⚠ What success proves, exactly: **no OTHER session holds the key**. Advisory
    locks are reentrant, so ``execute_order`` — which holds it across the claim,
    the marker and the provider call — succeeds on its own nested try inside
    ``terminalise_unsubmitted_recommendation_attempt``. That is correct rather
    than a hole: the proof happens at ``execute_order``'s own acquire, which
    fails if another session holds the key, and the lock is held continuously
    from that instant to the end of the span. But it is a reasoned exemption and
    is asserted by test, not assumed.

    The release decrements the reference count, so an outer holder keeps the key.

    ⚠ Session-scoped, so it survives the ``conn.commit()`` between the claim and
    the broker call — which is the entire reason it can bound that window. It
    also outlives the request if it is not released, and ``execute_order``'s
    connection comes from a pool, so every exit path releases in ``finally``.

    ⚠ A FAILED release only logs, deliberately (bot NITPICK, PR #3165). Raising
    there would replace the body's exception — the one that says what actually
    went wrong — with a cleanup error. The residual is bounded and points the
    safe way: the key stays held until that backend goes away, and a later
    submission for the same recommendation then *refuses*
    (``ConcurrentSubmissionInFlightError``) rather than placing a second order.
    No metrics surface exists in this module to emit to; the ``logger.error``
    matches the landed core equivalent (``_core_submission_try_lock``), and
    inventing a second reporting channel for one line is not this slice's work.
    """
    key = (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, recommendation_id)
    acquired = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", key).fetchone()
    conn.commit()
    if acquired != (True,):
        yield False
        return
    try:
        yield True
    finally:
        # Only READS can be outstanding here — every write inside the span
        # commits (the claim, the marker, a refusal audit) — so this rollback
        # discards nothing. It exists so the unlock below cannot run inside a
        # dirty or aborted transaction and silently fail to release the key.
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        # Never let a release failure replace the body's exception, and never
        # leave the key held on the way out of a pooled connection.
        try:
            released = conn.execute("SELECT pg_advisory_unlock(%s, %s)", key).fetchone()
            conn.commit()
            if released != (True,):
                logger.error(
                    "recommendation submission advisory lock ownership was lost for recommendation_id=%d",
                    recommendation_id,
                )
        except Exception:
            logger.exception("releasing the recommendation submission advisory lock failed")


def mark_recommendation_submission_entered(conn: psycopg.Connection[Any], *, order_id: int) -> None:
    """Commit, before the broker verb, that it is about to be entered (#2942 half 2).

    ⚠ ``'broker_verb_entered'`` means *"may subsequently have been entered"*.
    A marker bounds the instant it COMMITS, never the statement after it: the
    unattended guard, the credential read, body construction and
    ``ResilientClient``'s throttle and shared-lock wait all sit on the
    unprovable side. The name says what it proves, not what it precedes.

    ⚠ Its own commit, separate from the claim's. Folding the two together would
    make every committed row read the same and the discriminator would be
    vacuous.
    """
    conn.execute(
        """
        UPDATE orders
        SET recommendation_submission_phase = 'broker_verb_entered'
        WHERE order_id = %(oid)s
        """,
        {"oid": order_id},
    )
    conn.commit()


def terminalise_unsubmitted_recommendation_attempt(
    conn: psycopg.Connection[Any],
    *,
    recommendation_id: int,
    now: datetime,
) -> int | None:
    """Release a claim whose broker verb was provably never entered (#2942 half 2).

    Returns the released ``order_id``, or ``None`` when there is nothing to
    release — which is the ordinary case and every pre-existing row.

    **The evidence is our own write ordering, never a broker observation.**
    ``recommendation_submission_phase = 'claim_committed'`` means
    ``mark_recommendation_submission_entered`` did not commit, and it commits
    before the provider call — so the verb was never entered and no order can
    exist at the broker. Reference-keyed recovery cannot establish this: measured
    2026-09-17 on demo, a v2 order that FILLED echoed our ``referenceId`` and
    ``orders:lookup?referenceId=`` still returned 404 (#2961).

    **Two conditions, and the marker alone is not enough.** Between the claim
    commit and the marker commit a LIVE submitter's row reads exactly
    ``'claim_committed'`` too, so releasing on the marker alone would lift the
    claim under a submission that then places a real order — this ticket's own
    defect, reintroduced by its fix. The caller holds
    ``RECOMMENDATION_SUBMISSION_ADVISORY_LOCK`` across the whole span and
    Postgres drops it when the backend dies, so the try-lock below is the second
    condition. It is taken unconditionally rather than after a candidacy read:
    ``execute_order`` holds this same key across every live submission anyway, so
    a pre-lock read would save nothing and would race its own result. (#2961's
    "candidacy before the lock" rule is about a GLOBAL key whose blast radius
    reaches unrelated arms; this key is one recommendation.)

    The row is resolved to ``status='refused'`` rather than deleted — the attempt
    happened and belongs in the audit trail — reusing the status
    ``_release_claim_after_pre_io_refusal`` already gives an attempt that provably
    never reached the broker. ``'refused'`` is outside
    ``idx_orders_recommendation_open_attempt``'s predicate, so the claim lifts and
    the recommendation (still ``approved``) submits on the next pass.

    ⚠ Zero broker calls on every path.

    ⚠ No "requires an idle connection" precondition, unlike #2961's core
    equivalent. That one is called from a reconciler holding a clean connection;
    this one is called from ``execute_order``, whose step-1/2 reads have already
    opened a transaction by the time it runs, so the assertion could never hold.
    The contract that does apply is #243's, already on ``execute_order``: do not
    pass a connection carrying UNRELATED uncommitted writes, because the lock
    acquire commits.
    """
    with _recommendation_submission_try_lock(conn, recommendation_id) as idle:
        if not idle:
            raise _concurrent_submission_error(recommendation_id, "no claim of theirs can be shown unsubmitted")
        # ONE statement, under the lock: selecting and then updating would open a
        # window in which the marker moves to 'broker_verb_entered' between the
        # two, and the UPDATE would then release a claim whose verb HAD been
        # entered. The predicate IS the discriminator, so it belongs in the write.
        released = conn.execute(
            """
            UPDATE orders
            SET status = 'refused', raw_payload_json = %(payload)s
            WHERE recommendation_id = %(rid)s
              AND status IN ('submitted', 'pending', 'uncertain')
              AND recommendation_submission_phase = 'claim_committed'
            RETURNING order_id, instrument_id
            """,
            {
                "rid": recommendation_id,
                "payload": Jsonb(
                    {
                        "refusal": "never_submitted",
                        "detail": (
                            "recommendation_submission_phase stayed 'claim_committed', so the "
                            "broker verb was never entered and no order can exist"
                        ),
                    }
                ),
            },
        ).fetchone()
        if released is None:
            # Nothing to release. The connection carries no write, so the
            # context manager's rollback on the way out costs nothing.
            return None
        order_id = int(released[0])
        instrument_id = int(released[1])
        # ⚠ This COMMITS, and the UPDATE above rides on that commit — the
        # resolved status and its audit row are one refusal and must become
        # visible together. It also has to happen before the context manager
        # exits: that path rolls back a non-idle connection.
        _write_refusal_audit(
            conn,
            instrument_id=instrument_id,
            recommendation_id=recommendation_id,
            explanation=(f"Claim released — order_id={order_id} was committed but its broker verb was never entered"),
            evidence={"refusal": "never_submitted", "order_id": order_id},
            now=now,
        )
        logger.warning(
            "execute_order: recommendation_id=%d order_id=%d claim released — never submitted",
            recommendation_id,
            order_id,
        )
        return order_id


def execute_order(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
    decision_id: int,
    broker: BrokerProvider | None = None,
    broker_env: str | None = None,
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
         #2942 half 2 brackets that span with a session-scoped advisory lock
         and a write-ordering marker, so a crash BEFORE the broker verb is
         distinguishable from one after it and does not park the claim for ever.
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

    ``broker_env`` is the eToro environment ``broker`` was constructed with. It
    is recorded on the durable intent row (#3189 finding 4b) so the pending-order
    poller can prove an order belongs to the environment it is talking to —
    broker order ids are namespaced per environment, so a demo lookup of a live
    id can return a well-formed demo order and the identity guard never fires.

    ⚠ ``None`` is a real value, not a missing argument: it means no broker
    environment was recorded, which is exactly what a synthetic fill and every
    pre-existing row are. The poller polls those as it does today. It is
    therefore defaulted rather than required — but the live caller passes it,
    and a live row written without it is the only way this can go quietly wrong.

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
        # #2942 half 2. The evidence lock. Session-scoped and held across the
        # claim commit, the marker commit and the provider call, so a row still
        # reading 'claim_committed' while this key is free is one whose submitter
        # is gone -- Postgres drops the key when the backend dies. Released as
        # soon as the provider call returns: from that instant the row reads
        # 'broker_verb_entered' and is no longer terminalisable by anyone.
        with _recommendation_submission_try_lock(conn, recommendation_id) as submission_idle:
            if not submission_idle:
                raise _concurrent_submission_error(
                    recommendation_id, "refusing rather than risking a second economic order"
                )
            # Release a claim whose broker verb was provably never entered, so a
            # process death inside the window below does not park this
            # recommendation for ever. Returns None in the ordinary case.
            terminalise_unsubmitted_recommendation_attempt(conn, recommendation_id=recommendation_id, now=now)
            if action == "EXIT":
                exit_lot = _load_exit_lot(conn, instrument_id)
                if exit_lot is None:
                    # Pre-024 position without broker_positions row, or no
                    # broker-closeable LONG lot — a synthetic-id row or a short
                    # lot is not something this path can close (#3006) — or every
                    # candidate lot belongs to the strategy engine (#3025).
                    # Distinguish the last case: it is the only one an operator
                    # fixes by closing through the engine rather than by waiting
                    # for a sync.
                    engine_owned = _engine_owned_long_lot_count(conn, instrument_id)
                    if engine_owned:
                        error = (
                            f"All {engine_owned} broker-closeable long broker_positions rows for "
                            f"instrument {instrument_id} are owned by the strategy engine "
                            f"(strategy_position_ownership.status='active'); a recommendation EXIT "
                            f"must not close an engine-owned lot — close it through the engine"
                        )
                    else:
                        error = f"No broker-closeable long broker_positions row for instrument {instrument_id}"
                    logger.error("EXIT for instrument_id=%d: %s", instrument_id, error)
                    broker_result = BrokerOrderResult(
                        broker_order_ref=None,
                        status="failed",
                        filled_price=None,
                        filled_units=None,
                        fees=Decimal("0"),
                        raw_payload={"error": error, "engine_owned_long_lots": engine_owned},
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
                    # row would only add a rejection mode.
                    requested_units = exit_lot.units
                    exit_pos_id = exit_lot.position_id
                    # ``broker_positions.units`` is numeric(20,8) and
                    # ``orders.requested_units`` is numeric(18,6), so the column
                    # rounds this on the way in. Tolerable — nothing consumes it as
                    # a control input — but a SILENT loss of precision in an audit
                    # record is the part worth refusing, so say so when it actually
                    # happens rather than only in a comment. Decimal compares by
                    # VALUE, so a lot that is merely stored with trailing zeros
                    # (1305.05709600) does not trip this; one with genuine 7th/8th
                    # decimals does.
                    if requested_units != requested_units.quantize(_REQUESTED_UNITS_STEP):
                        logger.warning(
                            "execute_order: EXIT lot %d units %s do not survive the "
                            "numeric(18,6) orders column — recording %s",
                            exit_lot.position_id,
                            requested_units,
                            requested_units.quantize(_REQUESTED_UNITS_STEP),
                        )
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
                        broker_env=broker_env,
                        now=now,
                    )
                    # #2942 half 2: its OWN commit, immediately before the verb.
                    # Everything after this line -- the unattended guard, the
                    # credential read, body construction, ResilientClient's
                    # throttle -- is on the unprovable side, which is why the
                    # value means "may subsequently have been entered".
                    mark_recommendation_submission_entered(conn, order_id=submitted_order_id)
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
                    broker_env=broker_env,
                    now=now,
                )
                # #2942 half 2: its OWN commit, immediately before the verb.
                # See the EXIT arm above for why the marker cannot claim more
                # than the instant it commits.
                mark_recommendation_submission_entered(conn, order_id=submitted_order_id)
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
                # #3020: the mirror row for the lot the broker just closed.
                # Only on the live path — a demo EXIT resolves no lot, and its
                # `broker_positions` rows carry synthetic negative ids that
                # `_load_exit_lot` already excludes. AFTER `positions`, to keep
                # the lock order `portfolio_sync` uses.
                if exit_lot is not None:
                    _deduct_closed_exit_lot(
                        conn,
                        position_id=exit_lot.position_id,
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


# ---------------------------------------------------------------------------
# Pending-order reconciliation (#2942 half 2, slice B)
# ---------------------------------------------------------------------------

#: Verdicts the poller can reach for one pending recommendation order. Exactly
#: one of them releases the claim — ``terminalised_rejected``, where the broker
#: has told us the order has ceased to exist. Everything else fails closed.
PendingOrderVerdict = Literal[
    "terminalised_rejected",
    "still_pending",
    "filled_not_booked",
    "not_found",
    "lookup_error",
    "unsafe_status",
    "identity_mismatch",
    "environment_mismatch",
    "ref_not_pollable",
    "lock_busy",
    "no_longer_pending",
    "poll_error",
]

#: The two verdicts this module BRANCHES on, named once (review NITPICK, PR #3193).
#:
#: ⚠ The stated risk was drift on a rename; the measured one is sharper.
#: `verdict` is a ``PendingOrderVerdict``, so pyright rejects a misspelled
#: ASSIGNMENT (`verdict = "filled_not_bookd"` → 1 error) but is SILENT on a
#: misspelled COMPARISON (`verdict == "terminalised_rejectd"` → 0 errors), which
#: is an always-false branch that no gate catches. Comparing against a typed
#: constant makes the typo a name error instead (measured: reportUndefinedVariable).
_TERMINALISED_REJECTED: Final[PendingOrderVerdict] = "terminalised_rejected"
_FILLED_NOT_BOOKED: Final[PendingOrderVerdict] = "filled_not_booked"

#: Verdicts that describe a PERMANENT property of the row, so re-asking cannot
#: change the answer. Parking them is what stops an unresolvable order polling
#: the broker hourly for ever (PR #3168 WARNING). Deliberately NOT here:
#: ``not_found`` and ``lookup_error`` (a transport failure is not a fact about
#: the order), ``unsafe_status`` (an unsettled partial fill can still progress
#: to Filled) and ``identity_mismatch`` (a statement about the ANSWER, not about
#: the row — #3189 finding 4). ⚠ Parked is not resolved — see ``sql/395``.
#: ⚠ ``poll_error`` and ``lock_busy`` are deliberately absent for the same
#: reason: both are statements about THIS attempt, not about the row (#3189).
_PARKING_POLL_VERDICTS: dict[str, str] = {
    "filled_not_booked": "filled_unbooked",
    "ref_not_pollable": "ref_not_pollable",
}

#: Which orders the poller is willing to ask about. ONE definition, shared by
#: the batch selection and by the scheduler's prerequisite count — the two
#: disagreeing means either the job fires for nothing or never fires while work
#: waits, and ``test_count_matches_what_the_poller_would_select`` guards it.
#: A static literal: nothing interpolates into it.
_POLLABLE_ORDER_PREDICATE: Final[LiteralString] = """
    recommendation_id IS NOT NULL
    AND status = 'pending'
    AND broker_order_ref IS NOT NULL
    AND recommendation_poll_parked_reason IS NULL
"""


@dataclass(frozen=True)
class PendingOrderPollResult:
    """One poll of one pending recommendation order."""

    order_id: int
    recommendation_id: int
    verdict: PendingOrderVerdict
    broker_status: str | None = None


def pending_order_verdict(reconciliation_state: str) -> PendingOrderVerdict:
    """Map a reconciliation state to what this poller does about it.

    Pure, so the whole verdict table can be asserted without a database or a
    broker. Raises rather than guessing: an unmapped state must never advance an
    order, because the only advancing verdict releases a submission claim.
    """
    if reconciliation_state == "rejected":
        return "terminalised_rejected"
    if reconciliation_state == "pending":
        return "still_pending"
    if reconciliation_state == "resolved":
        # The broker filled it. We deliberately do NOT book the fill here —
        # see ``_record_unbooked_fill`` for why, and what would unblock it.
        return "filled_not_booked"
    raise ValueError(f"unmapped reconciliation state: {reconciliation_state}")


def count_pending_recommendation_orders(conn: psycopg.Connection[Any]) -> int:
    """How many recommendation orders are waiting on a broker verdict.

    The scheduler's prerequisite reads this so a dormant path spends no lane
    time and no share of the eToro request budget.

    ⚠ Its own ``scalar_row`` cursor, not ``conn.execute(...).fetchone()[0]``.
    Callers hand over whatever connection they already hold and this repo's
    read paths routinely set ``row_factory=dict_row``, under which ``row[0]``
    raises ``KeyError: 0``. Caught by running it against the dev DB, not by a
    test — every test here builds its own connection.
    """
    with conn.cursor(row_factory=psycopg.rows.scalar_row) as cur:
        cur.execute(f"SELECT count(*) FROM orders WHERE {_POLLABLE_ORDER_PREDICATE}")  # noqa: S608
        count = cur.fetchone()
    return int(count or 0)


def _stamp_polled(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    now: datetime,
    park_reason: str | None = None,
) -> None:
    """Record that we ASKED, and COMMIT.

    ⚠ ``recommendation_last_polled_at`` is written on every attempt path,
    including the ones that change nothing. That is the point: it is the rotation
    key, and #2948 established that ordering a bounded backlog on a key that does
    not move for a non-terminal row is an absorbing state rather than a delay.

    ⚠⚠ MONOTONIC (#3189, Codex checkpoint 2). The key only ever moves FORWARD.
    Two pollers overlap by design — the module's own docstring names "a boot
    catch-up racing the scheduled fire" — and each carries the ``now`` it
    captured when its batch began. So a run that took the lock at ``T1`` can
    finish and stamp *after* a later run stamped ``lock_busy`` at ``T2 > T1``; a
    plain assignment would move the key BACKWARD and re-postpone every row
    stamped between them, which is the fairness this whole slice exists to
    provide. ``GREATEST`` ignores NULL arguments (verified on PG 17.9) rather
    than propagating it like most functions, so the never-polled case still
    takes ``now``.

    ``park_reason`` stops the asking. It is set only for a verdict that is a
    PERMANENT property of the row (``sql/395``), never for a transport failure —
    otherwise a blip would silently retire a live order from reconciliation.
    Parking does not touch ``status``, so the submission claim stays held.
    """
    conn.execute(
        """
        UPDATE orders
        SET recommendation_last_polled_at = GREATEST(recommendation_last_polled_at, %(now)s),
            recommendation_poll_parked_reason = COALESCE(
                recommendation_poll_parked_reason, %(park)s)
        WHERE order_id = %(oid)s
        """,
        {"now": now, "oid": order_id, "park": park_reason},
    )
    conn.commit()


def _terminalise_rejected_order(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    instrument_id: int,
    recommendation_id: int,
    broker_order_ref: str,
    broker_status: str,
    raw_payload: dict[str, Any],
    now: datetime,
) -> bool:
    """Release the claim on an order the broker says has ceased to exist.

    ``rejected`` is outside ``idx_orders_recommendation_open_attempt``'s
    predicate, so writing it lifts the claim and the recommendation becomes
    proposable again. The recommendation goes to ``execution_failed``: an
    attempt was made and the broker refused it, which is a different fact from
    ``approved`` (never attempted) and from ``execution_pending`` (outstanding).

    No fill, position or cash write happens — by construction there is nothing
    to book, which is exactly why this is the one verdict safe to terminalise
    unattended.

    The UPDATEs are left outstanding for ``_write_refusal_audit``'s commit, the
    pattern that helper documents: the resolved status and its audit row are one
    event and must become visible together or not at all.

    ⚠⚠ BOTH WRITES ARE COMPARE-AND-SET (#3189 finding 5). The caller re-reads
    the row under the lock, then spends a broker round-trip OUTSIDE any
    transaction, and only then arrives here — so the state the decision was
    taken on is not the state being written, and this is the one verdict that
    RELEASES a submission claim. Guarding it is the module's own promoted rule
    (``sql-correctness.md``, "Single-row UPDATE must verify rowcount"; the
    prevention-log entry that promoted it was written against
    ``_update_order_with_broker_result`` in this same file).

    Returns ``True`` when the order was terminalised. ``False`` means the CAS
    matched nothing — the row is no longer the ``pending`` row carrying
    ``broker_order_ref`` that was looked up — in which case **nothing is
    written and the claim is not released**; the caller reports
    ``no_longer_pending``. ⚠ ``rowcount == 1`` and not ``!= 0`` deliberately:
    psycopg v3 reports ``-1`` when the server gives no count, and for both
    guards here that sentinel must fall to the fail-closed side.

    ⚠ The recommendation guard RAISES rather than returning, because a miss
    there is a different fact: the order CAS just held, so the row *was* the
    pending order for this recommendation, and its recommendation nevertheless
    is not ``execution_pending``. Writing half of a claim release — order
    resolved, recommendation saying something else — is worse than failing.
    The raise leaves both UPDATEs uncommitted (they are one implicit
    transaction until ``_write_refusal_audit`` commits), the lock context
    manager's ``finally`` rolls them back, and #3189 finding 7's per-row
    containment turns it into ``poll_error`` + a degraded run rather than a
    wedged batch. Posture follows the prevention log's ledger/mirror rule:
    both tables are ledger on the claim path, so a failed write aborts.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
            SET status = 'rejected',
                raw_payload_json = %(payload)s,
                recommendation_last_polled_at = %(now)s
            WHERE order_id = %(oid)s
              AND status = 'pending'
              AND broker_order_ref = %(ref)s
            """,
            {"oid": order_id, "payload": Jsonb(raw_payload), "now": now, "ref": broker_order_ref},
        )
        terminalised = cur.rowcount == 1
    if not terminalised:
        # Explicit, even though a zero-row UPDATE wrote nothing: the lock
        # context manager's `finally` documents "only READS can be outstanding
        # here", and leaving a DML statement open would quietly falsify it.
        conn.rollback()
        logger.warning(
            "reconcile_pending_recommendation_orders: order_id=%d was no longer the pending order for ref=%s "
            "when the broker's %s verdict came back; nothing written and the claim stays held",
            order_id,
            broker_order_ref,
            broker_status,
        )
        return False
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE trade_recommendations SET status = 'execution_failed' "
            "WHERE recommendation_id = %(rid)s AND status = 'execution_pending'",
            {"rid": recommendation_id},
        )
        demoted = cur.rowcount == 1
    if not demoted:
        raise RuntimeError(
            f"terminalising order_id={order_id} released the claim on recommendation {recommendation_id} "
            f"but the recommendation is not execution_pending; refusing to write half a claim release"
        )
    _write_refusal_audit(
        conn,
        instrument_id=instrument_id,
        recommendation_id=recommendation_id,
        explanation=(
            f"Pending order resolved by the broker as {broker_status} — order_id={order_id} "
            f"terminalised and the submission claim released"
        ),
        evidence={
            "refusal": "broker_rejected_pending_order",
            "order_id": order_id,
            "broker_status": broker_status,
            "response": raw_payload,
        },
        now=now,
    )
    logger.warning(
        "reconcile_pending_recommendation_orders: recommendation_id=%d order_id=%d rejected by broker (%s)",
        recommendation_id,
        order_id,
        broker_status,
    )
    return True


def _record_unbooked_fill(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    instrument_id: int,
    recommendation_id: int,
    broker_status: str,
    raw_payload: dict[str, Any],
    now: datetime,
) -> None:
    """Record that the broker filled an order our books have not booked.

    ⛔ **Deliberately does not book the fill, and the claim stays held.**
    Booking a late fill is ``_persist_fill`` -> ``_update_position_buy`` /
    ``_update_position_exit`` -> ``_persist_broker_position`` ->
    ``_deduct_closed_exit_lot`` -> ``_record_cash_ledger`` -> attribution ->
    ``enqueue_post_trade_sync``, plus estimated-cost recording — a path that
    closes over ``order_params``, ``quote_data`` and ``exit_lot``, locals that
    exist only at submission time. Two blockers, neither a preference:

    * **EXIT has no persisted lot.** ``_load_exit_lot`` resolves it at
      submission and nothing stores which lot was closed, so re-selecting at
      poll time is exactly the instrument/time/FIFO guess for broker ownership
      that #2942 forbids.
    * **It cannot be dev-verified.** Producing a real pending->filled
      recommendation order needs an attended demo session.

    So this leaves the status quo (claim held, nothing double-submits) plus the
    two things the status quo lacked: an ERROR log and a durable audit row.
    **Unblock for the booking slice: one attended pending->fill observation on a
    recommendation-origin order** — the same observation #2965 needs.

    ⚠ Written ONCE, then parked. The order is terminal at the broker, so
    re-asking every hour could only spend a shared eToro read and append another
    identical audit row for ever (PR #3168 WARNING). ``sql/395``'s park is what
    bounds it, and it deliberately leaves ``status='pending'`` — a terminal
    status would release the claim on an order that demonstrably executed.
    """
    conn.execute(
        """
        UPDATE orders
        SET recommendation_last_polled_at = %(now)s,
            recommendation_poll_parked_reason = %(park)s
        WHERE order_id = %(oid)s
        """,
        {"now": now, "oid": order_id, "park": _PARKING_POLL_VERDICTS["filled_not_booked"]},
    )
    _write_refusal_audit(
        conn,
        instrument_id=instrument_id,
        recommendation_id=recommendation_id,
        explanation=(
            f"Broker reports order_id={order_id} as {broker_status} but the fill is NOT booked — "
            f"late-fill booking is not implemented (#2942 slice C); the submission claim stays held"
        ),
        evidence={
            "refusal": "pending_order_filled_not_booked",
            "order_id": order_id,
            "broker_status": broker_status,
            "response": raw_payload,
        },
        now=now,
    )
    logger.error(
        "reconcile_pending_recommendation_orders: recommendation_id=%d order_id=%d is %s at the broker "
        "and is NOT booked locally — fills/positions/cash are behind the broker for this instrument",
        recommendation_id,
        order_id,
        broker_status,
    )


def reconcile_pending_recommendation_orders(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    env: str,
    limit: int = 20,
    now: datetime | None = None,
) -> tuple[PendingOrderPollResult, ...]:
    """Ask the broker about recommendation orders stuck at ``pending`` (#2942).

    ``execute_order`` writes ``orders.status='pending'`` when the broker
    acknowledges without filling, and until this consumer existed nothing looked
    at that row again — while the claim index kept the recommendation
    unsubmittable for ever, including when the broker rejected the order
    asynchronously a second later.

    ⚠⚠ **``lookup_order``, never ``get_order_status``.** The latter catches
    ``HTTPStatusError``, ``httpx.HTTPError`` and ``ValueError`` and returns
    ``status='failed'`` for all three, so it cannot distinguish "the broker
    rejected this order" from "the network dropped". Terminalising on that would
    release the claim on a live order after one blip and permit the second
    economic order this ticket exists to prevent. ``lookup_order`` raises.

    ⚠ ``status='uncertain'`` is NOT reachable from here and that is structural,
    not an omission: ``broker_order_ref`` is written only by
    ``_update_order_with_broker_result`` on the successful-response path, so a
    parked uncertain row carries no broker order id — and demo
    ``orders:lookup?referenceId=`` 404s even for an order that filled (#2961,
    measured 2026-09-17).

    No broker state is mutated: every call is a read, so
    ``refuse_broker_mutation_if_unattended`` is not reached and must not be.
    """
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    if conn.info.transaction_status != TransactionStatus.IDLE:
        # Broker I/O must never run inside a DB transaction, and
        # ``_recommendation_submission_try_lock`` commits on acquire.
        raise RuntimeError("pending-order reconciliation requires an idle connection")
    at = now or _utcnow()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            # ⚠ #2948: the rotation key is load-bearing. Ordering a bounded
            # backlog on keys that never move for a non-terminal row is an
            # ABSORBING STATE, not a delay — once the first `limit` rows are
            # stuck, the row at `limit + 1` is never visited again.
            f"""
            SELECT order_id, recommendation_id, instrument_id, broker_order_ref
            FROM orders
            WHERE {_POLLABLE_ORDER_PREDICATE}
            ORDER BY recommendation_last_polled_at ASC NULLS FIRST, order_id
            LIMIT %(limit)s
            """,  # noqa: S608
            {"limit": limit},
        )
        due = cur.fetchall()
    conn.commit()

    results: list[PendingOrderPollResult] = []
    for row in due:
        # ⚠⚠ PER-ROW CONTAINMENT (#3189 finding 7). Without it one unexpected
        # raise aborts the whole batch before any later row is attempted, and
        # because the raising row never stamps, the next fire selects the same
        # head and raises again — the rotation key's absorbing state reached
        # through an exception instead of through contention. Every path
        # `_poll_one_pending_order` handles deliberately already stamps; this
        # catches the ones nobody predicted, which is the only kind that gets
        # here.
        try:
            results.append(_poll_one_pending_order(conn, broker=broker, env=env, row=row, now=at))
        # ⚠ `Exception`, and it must NEVER widen to `BaseException`. Breadth is
        # the point here — the whole value is catching what nobody predicted —
        # but `KeyboardInterrupt` and `SystemExit` are a shutdown in progress,
        # and containing those would keep polling the broker through a SIGTERM
        # drain instead of letting the batch stop.
        except Exception:
            order_id = int(row["order_id"])
            # ⚠ No rollback here, and that is checked rather than assumed. A
            # raise that left a failed transaction would make `_stamp_polled`
            # fail too — but every path in `_poll_one_pending_order` runs inside
            # `_recommendation_submission_try_lock`, whose `finally` already
            # rolls back on the way out precisely so the unlock cannot run
            # inside a transaction. A defensive `conn.rollback()` was written
            # here first and REMOVED: a revert-probe deleting it could not be
            # made to fail, because the connection is already clean by the time
            # this block runs.
            try:
                _stamp_polled(conn, order_id=order_id, now=at)
            except Exception:
                # A stamp that cannot be written means the connection is gone,
                # which is not a per-row problem and must not be swallowed into
                # a per-row verdict.
                logger.exception(
                    "reconcile_pending_recommendation_orders: order_id=%d could not be stamped after an "
                    "unexpected failure; aborting the batch",
                    order_id,
                )
                raise
            logger.exception(
                "reconcile_pending_recommendation_orders: order_id=%d raised unexpectedly; stamped and "
                "continuing with the rest of the batch",
                order_id,
            )
            results.append(PendingOrderPollResult(order_id, int(row["recommendation_id"]), "poll_error"))
    return tuple(results)


def _poll_one_pending_order(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    env: str,
    row: dict[str, Any],
    now: datetime,
) -> PendingOrderPollResult:
    """Poll and resolve exactly one order, under this recommendation's key.

    The lock is the SAME per-recommendation key ``execute_order`` holds across
    its claim, marker and provider call. Reusing it rather than minting a second
    one keeps one lock discipline over this recommendation's whole lifecycle,
    and it is per-recommendation so a wedged row cannot starve the others
    (#2961's blast-radius finding).

    ⚠ The row is re-read UNDER the lock. It was selected before it, and a
    concurrent poller (a boot catch-up racing the scheduled fire) may already
    have resolved it — re-polling a settled order would spend request budget to
    learn nothing, and acting on the stale status would be worse.
    """
    order_id = int(row["order_id"])
    recommendation_id = int(row["recommendation_id"])
    instrument_id = int(row["instrument_id"])

    with _recommendation_submission_try_lock(conn, recommendation_id) as acquired:
        if not acquired:
            # ⚠⚠ STAMP BEFORE RETURNING (#3189 finding 7). `lock_busy` is an
            # attempt path — we asked and were told "someone else has this
            # recommendation" — so `_stamp_polled`'s invariant applies to it
            # exactly as it does to `lookup_error`. Without the stamp the row
            # keeps its old (or NULL) rotation key and re-occupies the head of
            # the bounded window on every fire, so `limit` contended rows
            # starve every order behind them permanently. That is #2948's
            # absorbing state reached through contention instead of through a
            # stuck status.
            #
            # ⚠ Safe to write here: the try-lock committed before yielding
            # False, so the connection is idle, and the session holding the key
            # is doing broker I/O OUTSIDE a transaction by this module's own
            # rule — so this UPDATE contends for the row lock only briefly, and
            # the holder's own `_stamp_polled` simply overwrites ours later.
            #
            # ⚠ NOT parked: `lock_busy` is a statement about this attempt, not
            # a permanent property of the row.
            _stamp_polled(conn, order_id=order_id, now=now)
            return PendingOrderPollResult(order_id, recommendation_id, "lock_busy")

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, broker_order_ref, broker_environment FROM orders WHERE order_id = %(oid)s",
                {"oid": order_id},
            )
            current = cur.fetchone()
        conn.commit()
        if current is None or current["status"] != "pending" or current["broker_order_ref"] is None:
            return PendingOrderPollResult(order_id, recommendation_id, "no_longer_pending")

        # ⚠⚠ BROKER ORDER IDS ARE NAMESPACED PER ENVIRONMENT (#3189 finding 4b).
        # This job is deliberately not demo-gated — refusing to look at a
        # live-environment order would leave exactly the row that matters most
        # wedged — so a demo-configured run can reach a row submitted to `real`.
        # Looking that id up on the demo API does not merely fail: it can return
        # a real, well-formed DEMO order that happens to carry the same id, and
        # the identity guard below then passes because the id is the one we
        # asked for. So the environment is compared BEFORE the lookup, and it
        # comes from the row rather than from the answer.
        #
        # ⚠ NULL is pollable, as it was before this column existed: every
        # pre-existing row and every synthetic fill carries NULL, and refusing
        # them would wedge every outstanding order the moment the column landed.
        #
        # ⚠ Stamped and NOT parked. "This process talks to another environment"
        # is a property of the deployment, not of the row — a `real`-env run
        # must find the row waiting, unparked, and resolve it.
        row_env = current["broker_environment"]
        if row_env is not None and str(row_env) != env:
            _stamp_polled(conn, order_id=order_id, now=now)
            logger.warning(
                "reconcile_pending_recommendation_orders: order_id=%d was submitted to broker environment %r "
                "and this process is configured for %r — not looked up",
                order_id,
                str(row_env),
                env,
            )
            return PendingOrderPollResult(order_id, recommendation_id, "environment_mismatch")

        ref = str(current["broker_order_ref"])
        # ``lookup_order`` raises ValueError on a non-positive-integer order id.
        # A recommendation order can carry a ref shape we cannot look up (a v1
        # submission echo, a demo synthetic id), and that is a permanent
        # property of the row, not a transient failure — so it is PARKED and
        # reported rather than retried hourly for ever.
        if not ref.isdigit() or int(ref) <= 0:
            _stamp_polled(
                conn,
                order_id=order_id,
                now=now,
                park_reason=_PARKING_POLL_VERDICTS["ref_not_pollable"],
            )
            logger.warning(
                "reconcile_pending_recommendation_orders: order_id=%d has non-pollable broker_order_ref=%r",
                order_id,
                ref,
            )
            return PendingOrderPollResult(order_id, recommendation_id, "ref_not_pollable")

        try:
            detail = broker.lookup_order(order_id=ref)
        except BrokerOrderNotFound:
            # ⚠ NOT terminalised. A 404 on an id the broker itself issued is
            # unexplained, and the safe reading of an unexplained answer is that
            # the order may still exist. Releasing the claim here would be the
            # #2942 defect reintroduced by its own fix.
            _stamp_polled(conn, order_id=order_id, now=now)
            logger.warning(
                "reconcile_pending_recommendation_orders: order_id=%d ref=%s not found at the broker; claim stays held",
                order_id,
                ref,
            )
            return PendingOrderPollResult(order_id, recommendation_id, "not_found")
        except BrokerOrderLookupError as exc:
            _stamp_polled(conn, order_id=order_id, now=now)
            logger.warning(
                "reconcile_pending_recommendation_orders: order_id=%d lookup failed — %s",
                order_id,
                exc,
            )
            return PendingOrderPollResult(order_id, recommendation_id, "lookup_error")

        # ⚠⚠ THE RESPONSE MUST DESCRIBE THE ORDER WE ASKED ABOUT (#3189
        # finding 4). Nothing in the classification below reads the identity of
        # what came back, so a wrong or ambiguous broker answer resolves
        # whichever local row happened to be polled — terminalising or parking
        # an order on a status that belongs to a different one.
        #
        # The same contradiction is already refused one subsystem over, against
        # the same eToro contract: `strategy_order_reconciliation.py` raises
        # "broker order instrument differs from durable intent" and "broker
        # order id differs from the previously reconciled id". This is that
        # rule carried to the poller, which had neither.
        #
        # ⚠ It cannot fire on a well-formed response: `_parse_order_detail`
        # raises unless `asset.instrumentId` is present and positive, and sets
        # `broker_order_ref = str(orderId)`. Equality fires only when the broker
        # genuinely answered about another order.
        #
        # ⚠ NOT parked, for the reason `not_found` and `unsafe_status` are not:
        # it is a statement about the ANSWER, not a permanent property of the
        # row, and a later correct response must still be readable.
        #
        # ⚠⚠ NUMERIC comparison, not textual (Codex checkpoint 2, round 2).
        # `broker_order_ref` is a TEXT column and the pollability check above
        # accepts any positive digit string, so a row carrying `"00123"` is
        # looked up as `orderId=123` and answered with `broker_order_ref="123"`.
        # A textual compare would call that a mismatch — refusing a response the
        # broker got exactly right, for ever, and degrading every run while it
        # did. This is the enumerate-what-a-narrowing-gate-REJECTS rule: the
        # only rejection wanted here is a DIFFERENT order.
        returned_ref = detail.broker_order_ref
        ref_matches = returned_ref.isdigit() and int(returned_ref) == int(ref)
        if not ref_matches or detail.instrument_id != instrument_id:
            _stamp_polled(conn, order_id=order_id, now=now)
            logger.error(
                "reconcile_pending_recommendation_orders: order_id=%d asked the broker about ref=%s "
                "instrument_id=%d and was answered about ref=%s instrument_id=%d — nothing advanced",
                order_id,
                ref,
                instrument_id,
                detail.broker_order_ref,
                detail.instrument_id,
            )
            return PendingOrderPollResult(order_id, recommendation_id, "identity_mismatch", detail.broker_status)

        try:
            # The broker status vocabulary is fixed once, in the strategy
            # reconciler, against the same eToro order contract (#2451/#2965).
            # Imported rather than restated: a second copy is a magic-string
            # duplicate of a typed counterpart, and the two would drift.
            state, _order_status = classify_broker_order_status(detail.broker_status)
            verdict = pending_order_verdict(state)
        except (StrategyReconciliationError, ValueError) as exc:
            # An unsettled partial fill (#2965) or a status outside the
            # documented vocabulary. Never advance the order on a status we
            # cannot read.
            _stamp_polled(conn, order_id=order_id, now=now)
            logger.error(
                "reconcile_pending_recommendation_orders: order_id=%d broker_status=%r cannot be acted on — %s",
                order_id,
                detail.broker_status,
                exc,
            )
            return PendingOrderPollResult(order_id, recommendation_id, "unsafe_status", detail.broker_status)

        # ⚠⚠ A REJECTED ORDER THAT CARRIES POSITION EXECUTIONS IS A
        # CONTRADICTION, AND THIS IS THE ONE VERDICT THAT RELEASES THE CLAIM
        # (#3189 finding 3). `classify_broker_order_status` reads the status
        # word only; the detail can still name executions the broker actually
        # made. Terminalising on the word alone would leave a partial execution
        # unbooked AND lift the submission claim, so a second economic order
        # for the same recommendation becomes possible — the #2942 defect
        # reintroduced through a different door.
        #
        # The same contradiction is already refused one subsystem over, against
        # the same eToro contract (#2451/#2965):
        # `strategy_order_reconciliation.py` raises
        # "rejected broker order unexpectedly has position executions". This is
        # that rule, carried to the poller, which had the unsafe half.
        #
        # ⚠⚠ It resolves to `filled_not_booked`, and stamping alone is NOT
        # enough (Codex checkpoint 2, P1). A bare `_stamp_polled` leaves the row
        # selectable, so the contradiction spends a broker read every hour —
        # and, worse, a LATER `Rejected` that omitted the executions would reach
        # `_terminalise_rejected_order` and release the claim despite the
        # earlier proof of an economic execution. The observation has to be
        # durable, not just this attempt.
        #
        # `_record_unbooked_fill` is that mechanism and it already exists:
        # it parks (`sql/395`), which removes the row from
        # `_POLLABLE_ORDER_PREDICATE` so a later omission can never terminalise
        # it; it deliberately leaves `status='pending'`, so the claim stays
        # held; and it writes a `decision_audit` row naming the broker status,
        # which is what keeps the trade path auditable. Reusing it also means
        # the executions are not the only record that something happened.
        #
        # The verdict is the honest one: positions exist and we have not booked
        # them, whatever word the status carried.
        if verdict == _TERMINALISED_REJECTED and detail.position_executions:
            logger.error(
                "reconcile_pending_recommendation_orders: order_id=%d broker_status=%r is rejected but carries "
                "%d position execution(s); parking with the claim held rather than terminalising",
                order_id,
                detail.broker_status,
                len(detail.position_executions),
            )
            verdict = _FILLED_NOT_BOOKED

        if verdict == _TERMINALISED_REJECTED:
            # ⚠ The CAS can miss (#3189 finding 5): the row was re-read under
            # the lock, but the broker round-trip since then ran outside any
            # transaction. A miss means nothing was written and the claim is
            # still held — the same fact the pre-lookup check reports, so the
            # same verdict, which is deliberately not parked and not stamped
            # (a non-`pending` row is outside `_POLLABLE_ORDER_PREDICATE`, so
            # it cannot occupy the rotation head).
            if not _terminalise_rejected_order(
                conn,
                order_id=order_id,
                instrument_id=instrument_id,
                recommendation_id=recommendation_id,
                broker_order_ref=ref,
                broker_status=detail.broker_status,
                raw_payload=detail.raw_payload,
                now=now,
            ):
                return PendingOrderPollResult(order_id, recommendation_id, "no_longer_pending", detail.broker_status)
        elif verdict == _FILLED_NOT_BOOKED:
            _record_unbooked_fill(
                conn,
                order_id=order_id,
                instrument_id=instrument_id,
                recommendation_id=recommendation_id,
                broker_status=detail.broker_status,
                raw_payload=detail.raw_payload,
                now=now,
            )
        else:
            _stamp_polled(conn, order_id=order_id, now=now)
        return PendingOrderPollResult(order_id, recommendation_id, verdict, detail.broker_status)
