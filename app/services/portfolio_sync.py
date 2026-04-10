"""Portfolio sync — reconcile local positions and cash against the broker.

Fetches the broker's current portfolio (open positions + available cash)
and reconciles against the local ``positions`` and ``cash_ledger`` tables.

This is a **read-from-broker, write-to-local-DB** operation. It never
places orders or modifies broker state.

Reconciliation rules:

* **Broker position exists locally**: update ``current_units`` and
  ``unrealized_pnl`` from the broker snapshot.
* **Broker position is new locally**: insert a new ``positions`` row.
  The position was opened outside eBull (manual trade, copy trading).
* **Local position absent from broker**: the position was closed outside
  eBull. Zero out ``current_units`` and log a warning.
* **Cash**: record a ``broker_sync`` event in ``cash_ledger`` with the
  delta between the broker's reported available cash and the local
  ``SUM(amount)`` from ``cash_ledger``.  If delta is zero (within a
  tolerance), no event is recorded.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.providers.broker import BrokerPortfolio

logger = logging.getLogger(__name__)

# Cash deltas smaller than this are considered rounding noise.
_CASH_SYNC_TOLERANCE = Decimal("0.01")


@dataclass
class PortfolioSyncResult:
    """Summary of a portfolio sync run."""

    positions_updated: int
    positions_opened_externally: int
    positions_closed_externally: int
    cash_delta: Decimal
    broker_cash: Decimal
    local_cash: Decimal


def sync_portfolio(
    conn: psycopg.Connection[Any],
    portfolio: BrokerPortfolio,
    now: datetime | None = None,
) -> PortfolioSyncResult:
    """Reconcile local state against a broker portfolio snapshot.

    Must be called inside a transaction (autocommit=False, the default).
    The caller is responsible for committing.
    """
    if now is None:
        now = datetime.now(UTC)

    updated = 0
    opened_externally = 0
    closed_externally = 0

    # Build a lookup of broker positions by instrument_id.
    broker_positions = {bp.instrument_id: bp for bp in portfolio.positions}

    # Fetch all local positions with units > 0.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        local_rows = cur.execute(
            """
            SELECT instrument_id, current_units
            FROM positions
            WHERE current_units > 0
            """
        ).fetchall()
    local_instrument_ids = {row["instrument_id"] for row in local_rows}

    # 1. Upsert broker positions into local state.
    for bp in portfolio.positions:
        unrealized = (bp.current_price - bp.open_price) * bp.units
        if bp.instrument_id in local_instrument_ids:
            # Existing local position — update from broker.
            conn.execute(
                """
                UPDATE positions SET
                    current_units  = %(units)s,
                    unrealized_pnl = %(upnl)s,
                    updated_at     = %(now)s
                WHERE instrument_id = %(iid)s
                """,
                {
                    "iid": bp.instrument_id,
                    "units": bp.units,
                    "upnl": unrealized,
                    "now": now,
                },
            )
            updated += 1
        else:
            # New position from broker — opened externally.
            conn.execute(
                """
                INSERT INTO positions
                    (instrument_id, open_date, avg_cost, current_units,
                     cost_basis, unrealized_pnl, updated_at)
                VALUES
                    (%(iid)s, %(date)s, %(price)s, %(units)s,
                     %(cost)s, %(upnl)s, %(now)s)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    current_units  = EXCLUDED.current_units,
                    avg_cost       = EXCLUDED.avg_cost,
                    cost_basis     = EXCLUDED.cost_basis,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    updated_at     = EXCLUDED.updated_at
                """,
                {
                    "iid": bp.instrument_id,
                    "date": now.date(),
                    "price": bp.open_price,
                    "units": bp.units,
                    "cost": bp.open_price * bp.units,
                    "upnl": unrealized,
                    "now": now,
                },
            )
            opened_externally += 1
            logger.warning(
                "Position for instrument %d found on broker but not locally — "
                "opened externally (units=%.4f, open_price=%.4f)",
                bp.instrument_id,
                bp.units,
                bp.open_price,
            )

    # 2. Zero out local positions absent from broker.
    for row in local_rows:
        iid = row["instrument_id"]
        if iid not in broker_positions:
            conn.execute(
                """
                UPDATE positions SET
                    current_units  = 0,
                    unrealized_pnl = 0,
                    updated_at     = %(now)s
                WHERE instrument_id = %(iid)s
                """,
                {"iid": iid, "now": now},
            )
            closed_externally += 1
            logger.warning(
                "Local position for instrument %d not found on broker — closed externally, zeroing units",
                iid,
            )

    # 3. Reconcile cash.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        local_cash_row = cur.execute("SELECT COALESCE(SUM(amount), 0) AS total FROM cash_ledger").fetchone()
    local_cash = Decimal(str(local_cash_row["total"])) if local_cash_row else Decimal("0")
    broker_cash = portfolio.available_cash
    cash_delta = broker_cash - local_cash

    if abs(cash_delta) > _CASH_SYNC_TOLERANCE:
        conn.execute(
            """
            INSERT INTO cash_ledger (event_time, event_type, amount, currency, note)
            VALUES (%(time)s, 'broker_sync', %(amount)s, 'USD', %(note)s)
            """,
            {
                "time": now,
                "amount": cash_delta,
                "note": f"Broker sync: broker={broker_cash}, local={local_cash}, delta={cash_delta}",
            },
        )
        logger.info(
            "Cash reconciliation: broker=%.2f local=%.2f delta=%.2f",
            broker_cash,
            local_cash,
            cash_delta,
        )

    return PortfolioSyncResult(
        positions_updated=updated,
        positions_opened_externally=opened_externally,
        positions_closed_externally=closed_externally,
        cash_delta=cash_delta,
        broker_cash=broker_cash,
        local_cash=local_cash,
    )
