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
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.providers.broker import BrokerPortfolio, BrokerPosition

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


@dataclass
class _AggregatedPosition:
    """Broker positions for the same instrument, aggregated."""

    instrument_id: int
    units: Decimal
    avg_open_price: Decimal
    unrealized_pnl: Decimal
    earliest_open_date_raw: str | None
    raw_payloads: list[dict[str, Any]]


def _aggregate_by_instrument(
    positions: Sequence[BrokerPosition],
) -> dict[int, _AggregatedPosition]:
    """Group broker positions by instrument_id and aggregate.

    eToro can return multiple open positionIDs for the same instrumentID.
    We sum units, compute weighted-average open price, compute total
    unrealised PnL from the raw rows, and keep the earliest open date.
    """
    from collections import defaultdict

    buckets: dict[int, list[BrokerPosition]] = defaultdict(list)
    for bp in positions:
        buckets[bp.instrument_id].append(bp)

    result: dict[int, _AggregatedPosition] = {}
    for iid, group in buckets.items():
        _zero = Decimal("0")
        total_units = sum((bp.units for bp in group), start=_zero)
        total_cost = sum((bp.open_price * bp.units for bp in group), start=_zero)
        total_pnl = sum(
            ((bp.current_price - bp.open_price) * bp.units for bp in group),
            start=_zero,
        )
        avg_price = total_cost / total_units if total_units > 0 else _zero

        # Earliest open date among the raw payloads (if any provide it).
        open_dates: list[str] = []
        for bp in group:
            raw_open = bp.raw_payload.get("openDateTime")
            if isinstance(raw_open, str):
                open_dates.append(raw_open)
        open_dates.sort()

        result[iid] = _AggregatedPosition(
            instrument_id=iid,
            units=total_units,
            avg_open_price=avg_price,
            unrealized_pnl=total_pnl,
            earliest_open_date_raw=open_dates[0] if open_dates else None,
            raw_payloads=[bp.raw_payload for bp in group],
        )
    return result


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

    # Aggregate broker positions by instrument_id.  eToro can return
    # multiple positionIDs for the same instrument; we must reconcile
    # against aggregated totals, not individual rows.
    broker_positions = _aggregate_by_instrument(portfolio.positions)

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
    for agg in broker_positions.values():
        if agg.instrument_id in local_instrument_ids:
            # Existing local position — update from broker.
            # Only refresh units and PnL; leave avg_cost/cost_basis
            # untouched — for eBull-originated positions, the local
            # cost basis is authoritative for tax-lot and P&L history.
            conn.execute(
                """
                UPDATE positions SET
                    current_units  = %(units)s,
                    unrealized_pnl = %(upnl)s,
                    updated_at     = %(now)s
                WHERE instrument_id = %(iid)s
                """,
                {
                    "iid": agg.instrument_id,
                    "units": agg.units,
                    "upnl": agg.unrealized_pnl,
                    "now": now,
                },
            )
            updated += 1
        else:
            # New position from broker — opened externally.
            # Use the earliest open date from the aggregated broker
            # payloads; fall back to sync time if unavailable.
            open_date = now.date()
            if agg.earliest_open_date_raw is not None:
                try:
                    open_date = datetime.fromisoformat(agg.earliest_open_date_raw).date()
                except ValueError:
                    pass
            conn.execute(
                """
                INSERT INTO positions
                    (instrument_id, open_date, avg_cost, current_units,
                     cost_basis, unrealized_pnl, source, updated_at)
                VALUES
                    (%(iid)s, %(date)s, %(price)s, %(units)s,
                     %(cost)s, %(upnl)s, 'broker_sync', %(now)s)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    current_units  = EXCLUDED.current_units,
                    avg_cost       = EXCLUDED.avg_cost,
                    cost_basis     = EXCLUDED.cost_basis,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    open_date      = EXCLUDED.open_date,
                    -- Reset source on reopen: if the existing row is
                    -- fully closed (current_units <= 0) this conflict
                    -- path is reopening it externally, so the new
                    -- opener ('broker_sync') becomes the source.
                    -- Otherwise preserve the existing source (adds to
                    -- an already-open position shouldn't flip
                    -- ownership).
                    source         = CASE
                        WHEN positions.current_units <= 0
                            THEN EXCLUDED.source
                        ELSE positions.source
                    END,
                    updated_at     = EXCLUDED.updated_at
                """,
                {
                    "iid": agg.instrument_id,
                    "date": open_date,
                    "price": agg.avg_open_price,
                    "units": agg.units,
                    "cost": agg.avg_open_price * agg.units,
                    "upnl": agg.unrealized_pnl,
                    "now": now,
                },
            )
            opened_externally += 1
            logger.warning(
                "Position for instrument %d found on broker but not locally — "
                "opened externally (units=%.4f, avg_open_price=%.4f)",
                agg.instrument_id,
                agg.units,
                agg.avg_open_price,
            )

    # 2. Zero out local positions absent from broker.
    #
    # Guard: if the broker returned zero positions but we have local open
    # positions, treat this as a likely API failure (auth lapse, stale
    # session, or transient error returning HTTP 200 with an empty body)
    # rather than a legitimate "user liquidated everything" event.
    # Raising here marks the job as failed in `job_runs`, alerting the
    # operator, and prevents silent data loss on the positions table.
    # Legitimate liquidation is expected to be a per-position event, not
    # a whole-portfolio wipe in a single cycle.
    if not broker_positions and local_rows:
        raise RuntimeError(
            f"Broker returned empty positions but {len(local_rows)} local "
            f"position(s) exist — refusing to zero out local state. "
            f"Likely an upstream API failure (auth, session, or transient)."
        )

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
