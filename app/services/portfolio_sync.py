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
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
import psycopg.types.json

from app.providers.broker import (
    BrokerMirror,
    BrokerPortfolio,
    BrokerPosition,
)

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
    mirrors_upserted: int = 0
    mirrors_closed: int = 0
    mirror_positions_upserted: int = 0
    broker_positions_upserted: int = 0
    broker_positions_deleted: int = 0


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


def _upsert_broker_positions(
    conn: psycopg.Connection[Any],
    broker_positions: Sequence[BrokerPosition],
    now: datetime,
) -> tuple[int, int]:
    """Upsert individual broker positions into the ``broker_positions`` table.

    Writes one row per eToro positionID — preserving per-position SL/TP,
    leverage, fees, and the full raw payload.  Positions that disappeared
    from the broker payload are deleted.

    Returns (upserted, deleted).

    Safety: if the broker returned zero positions but local rows exist,
    the caller's guard (in ``sync_portfolio``) will have already raised
    before this function is called.
    """
    upserted = 0

    # Collect position_ids from the broker payload.  Skip positions
    # without a position_id (backwards-compat with test fixtures).
    broker_position_ids: list[int] = []
    for bp in broker_positions:
        if bp.position_id is None:
            continue
        broker_position_ids.append(bp.position_id)

        conn.execute(
            """
            INSERT INTO broker_positions (
                position_id, instrument_id, is_buy, units, initial_units,
                amount, initial_amount_in_dollars, open_rate,
                open_conversion_rate, open_date_time,
                stop_loss_rate, take_profit_rate,
                is_no_stop_loss, is_no_take_profit,
                leverage, is_tsl_enabled, total_fees,
                source, raw_payload, updated_at
            )
            VALUES (
                %(position_id)s, %(instrument_id)s, %(is_buy)s, %(units)s,
                %(initial_units)s, %(amount)s, %(initial_amount_in_dollars)s,
                %(open_rate)s, %(open_conversion_rate)s, %(open_date_time)s,
                %(stop_loss_rate)s, %(take_profit_rate)s,
                %(is_no_stop_loss)s, %(is_no_take_profit)s,
                %(leverage)s, %(is_tsl_enabled)s, %(total_fees)s,
                %(source)s, %(raw_payload)s, %(now)s
            )
            ON CONFLICT (position_id) DO UPDATE SET
                units                    = EXCLUDED.units,
                initial_units            = EXCLUDED.initial_units,
                amount                   = EXCLUDED.amount,
                stop_loss_rate           = EXCLUDED.stop_loss_rate,
                take_profit_rate         = EXCLUDED.take_profit_rate,
                is_no_stop_loss          = EXCLUDED.is_no_stop_loss,
                is_no_take_profit        = EXCLUDED.is_no_take_profit,
                leverage                 = EXCLUDED.leverage,
                is_tsl_enabled           = EXCLUDED.is_tsl_enabled,
                total_fees               = EXCLUDED.total_fees,
                -- Preserve source: if position was created by eBull, keep
                -- 'ebull' even when sync refreshes it.
                source                   = CASE
                    WHEN broker_positions.source = 'ebull'
                        THEN broker_positions.source
                    ELSE EXCLUDED.source
                END,
                raw_payload              = EXCLUDED.raw_payload,
                updated_at               = EXCLUDED.updated_at
            """,
            {
                "position_id": bp.position_id,
                "instrument_id": bp.instrument_id,
                "is_buy": bp.is_buy,
                "units": bp.units,
                "initial_units": bp.initial_units,
                "amount": bp.amount,
                "initial_amount_in_dollars": bp.initial_amount_in_dollars,
                "open_rate": bp.open_price,
                "open_conversion_rate": bp.open_conversion_rate,
                "open_date_time": bp.open_date_time or now,
                "stop_loss_rate": bp.stop_loss_rate,
                "take_profit_rate": bp.take_profit_rate,
                "is_no_stop_loss": bp.is_no_stop_loss,
                "is_no_take_profit": bp.is_no_take_profit,
                "leverage": bp.leverage,
                "is_tsl_enabled": bp.is_tsl_enabled,
                "total_fees": bp.total_fees,
                "source": "broker_sync",
                "raw_payload": psycopg.types.json.Jsonb(bp.raw_payload),
                "now": now,
            },
        )
        upserted += 1

    # Delete positions that disappeared from the broker payload.
    # These were closed externally (eToro UI, SL/TP trigger, manual).
    # eBull did not initiate the close — we are observing reality.
    #
    # When broker_position_ids is empty (all positions lacked
    # position_id, or no positions at all), we still run the delete
    # to clean up any stale rows from a previous sync cycle.  The
    # `!= ALL('{}'::bigint[])` predicate matches all rows, which is
    # the correct behaviour: if the broker reports nothing, nothing
    # should remain in broker_positions.
    deleted = 0
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            DELETE FROM broker_positions
            WHERE position_id != ALL(%(ids)s)
            RETURNING position_id, instrument_id
            """,
            {"ids": broker_position_ids},
        )
        for row in cur:
            deleted += 1
            logger.info(
                "broker_positions: position %d (instrument %d) disappeared "
                "from broker — deleted (closed externally: eToro UI, SL/TP "
                "trigger, or manual)",
                row["position_id"],
                row["instrument_id"],
            )

    return upserted, deleted


def _sync_mirrors(
    conn: psycopg.Connection[Any],
    mirrors: Sequence[BrokerMirror],
    now: datetime,
) -> tuple[int, int, int]:
    """Upsert copy_traders/copy_mirrors/copy_mirror_positions from a
    freshly-parsed mirror payload. Returns
    ``(mirrors_upserted, mirror_positions_upserted, mirrors_closed)``.

    Must be called inside the caller's transaction — this function
    never commits. Caller owns rollback on any raise.

    Disappearance handling is split by scope:

    - **Total disappearance** (payload empty AND active local rows
      exist) is handled by the caller-side pre-write guard in
      ``sync_portfolio``, which raises BEFORE any writes. Placing
      it there means a rollback does not silently discard already-
      written position/cash state from the same sync cycle.
    - **Partial disappearance** (payload non-empty, some mirrors
      absent) is handled here as a soft-close step (§2.3.4) after
      the per-mirror upsert loop.

    Single-writer serialisation is guaranteed by JobRuntime's
    APScheduler+JobLock stack (spec §2.3.1); _sync_mirrors does not
    take its own advisory lock.
    """
    mirrors_upserted = 0
    mirror_positions_upserted = 0
    mirrors_closed = 0

    for mirror in mirrors:
        # 1. Upsert the trader row (parent_cid is the identity spine).
        conn.execute(
            """
            INSERT INTO copy_traders (
                parent_cid, parent_username, first_seen_at, updated_at
            ) VALUES (
                %(cid)s, %(username)s, %(now)s, %(now)s
            )
            ON CONFLICT (parent_cid) DO UPDATE SET
                parent_username = EXCLUDED.parent_username,
                updated_at = EXCLUDED.updated_at
            """,
            {
                "cid": mirror.parent_cid,
                "username": mirror.parent_username,
                "now": now,
            },
        )

        # 2. Upsert the mirror row. active=TRUE, closed_at=NULL on
        #    every row the payload contains — re-copy of a
        #    previously-closed mirror_id flips those back to live.
        conn.execute(
            """
            INSERT INTO copy_mirrors (
                mirror_id, parent_cid, initial_investment,
                deposit_summary, withdrawal_summary,
                available_amount, closed_positions_net_profit,
                stop_loss_percentage, stop_loss_amount,
                mirror_status_id, mirror_calculation_type,
                pending_for_closure, started_copy_date,
                active, closed_at, raw_payload, updated_at
            ) VALUES (
                %(mirror_id)s, %(parent_cid)s, %(initial_investment)s,
                %(deposit_summary)s, %(withdrawal_summary)s,
                %(available_amount)s, %(closed_positions_net_profit)s,
                %(stop_loss_percentage)s, %(stop_loss_amount)s,
                %(mirror_status_id)s, %(mirror_calculation_type)s,
                %(pending_for_closure)s, %(started_copy_date)s,
                TRUE, NULL, %(raw_payload)s, %(now)s
            )
            ON CONFLICT (mirror_id) DO UPDATE SET
                parent_cid                  = EXCLUDED.parent_cid,
                initial_investment          = EXCLUDED.initial_investment,
                deposit_summary             = EXCLUDED.deposit_summary,
                withdrawal_summary          = EXCLUDED.withdrawal_summary,
                available_amount            = EXCLUDED.available_amount,
                closed_positions_net_profit = EXCLUDED.closed_positions_net_profit,
                stop_loss_percentage        = EXCLUDED.stop_loss_percentage,
                stop_loss_amount            = EXCLUDED.stop_loss_amount,
                mirror_status_id            = EXCLUDED.mirror_status_id,
                mirror_calculation_type     = EXCLUDED.mirror_calculation_type,
                pending_for_closure         = EXCLUDED.pending_for_closure,
                started_copy_date           = EXCLUDED.started_copy_date,
                active                      = TRUE,
                closed_at                   = NULL,
                raw_payload                 = EXCLUDED.raw_payload,
                updated_at                  = EXCLUDED.updated_at
            """,
            {
                "mirror_id": mirror.mirror_id,
                "parent_cid": mirror.parent_cid,
                "initial_investment": mirror.initial_investment,
                "deposit_summary": mirror.deposit_summary,
                "withdrawal_summary": mirror.withdrawal_summary,
                "available_amount": mirror.available_amount,
                "closed_positions_net_profit": mirror.closed_positions_net_profit,
                "stop_loss_percentage": mirror.stop_loss_percentage,
                "stop_loss_amount": mirror.stop_loss_amount,
                "mirror_status_id": mirror.mirror_status_id,
                "mirror_calculation_type": mirror.mirror_calculation_type,
                "pending_for_closure": mirror.pending_for_closure,
                "started_copy_date": mirror.started_copy_date,
                "raw_payload": psycopg.types.json.Jsonb(mirror.raw_payload),
                "now": now,
            },
        )
        mirrors_upserted += 1

        # 3a. Evict nested positions that have closed since the last
        #     sync. Passing the new IDs as a single array parameter
        #     sidesteps the empty-list SQL parser error and exploits
        #     Postgres's `position_id <> ALL('{}')` === TRUE semantics
        #     to correctly delete every existing row when the payload
        #     has zero positions for this mirror.
        current_position_ids = [int(p.position_id) for p in mirror.positions]
        conn.execute(
            """
            DELETE FROM copy_mirror_positions
            WHERE mirror_id = %(mirror_id)s
              AND position_id <> ALL(%(position_ids)s::bigint[])
            """,
            {
                "mirror_id": mirror.mirror_id,
                "position_ids": current_position_ids,
            },
        )

        # 3b. Upsert every position in the payload.
        for pos in mirror.positions:
            conn.execute(
                """
                INSERT INTO copy_mirror_positions (
                    mirror_id, position_id, parent_position_id,
                    instrument_id, is_buy, units, amount,
                    initial_amount_in_dollars, open_rate,
                    open_conversion_rate, open_date_time,
                    take_profit_rate, stop_loss_rate,
                    total_fees, leverage, raw_payload, updated_at
                ) VALUES (
                    %(mirror_id)s, %(position_id)s, %(parent_position_id)s,
                    %(instrument_id)s, %(is_buy)s, %(units)s, %(amount)s,
                    %(initial_amount)s, %(open_rate)s,
                    %(open_conversion_rate)s, %(open_date_time)s,
                    %(take_profit_rate)s, %(stop_loss_rate)s,
                    %(total_fees)s, %(leverage)s, %(raw_payload)s,
                    %(now)s
                )
                ON CONFLICT (mirror_id, position_id) DO UPDATE SET
                    parent_position_id        = EXCLUDED.parent_position_id,
                    instrument_id             = EXCLUDED.instrument_id,
                    is_buy                    = EXCLUDED.is_buy,
                    units                     = EXCLUDED.units,
                    amount                    = EXCLUDED.amount,
                    initial_amount_in_dollars = EXCLUDED.initial_amount_in_dollars,
                    open_rate                 = EXCLUDED.open_rate,
                    open_conversion_rate      = EXCLUDED.open_conversion_rate,
                    open_date_time            = EXCLUDED.open_date_time,
                    take_profit_rate          = EXCLUDED.take_profit_rate,
                    stop_loss_rate            = EXCLUDED.stop_loss_rate,
                    total_fees                = EXCLUDED.total_fees,
                    leverage                  = EXCLUDED.leverage,
                    raw_payload               = EXCLUDED.raw_payload,
                    updated_at                = EXCLUDED.updated_at
                """,
                {
                    "mirror_id": mirror.mirror_id,
                    "position_id": pos.position_id,
                    "parent_position_id": pos.parent_position_id,
                    "instrument_id": pos.instrument_id,
                    "is_buy": pos.is_buy,
                    "units": pos.units,
                    "amount": pos.amount,
                    "initial_amount": pos.initial_amount_in_dollars,
                    "open_rate": pos.open_rate,
                    "open_conversion_rate": pos.open_conversion_rate,
                    "open_date_time": pos.open_date_time,
                    "take_profit_rate": pos.take_profit_rate,
                    "stop_loss_rate": pos.stop_loss_rate,
                    "total_fees": pos.total_fees,
                    "leverage": pos.leverage,
                    "raw_payload": psycopg.types.json.Jsonb(pos.raw_payload),
                    "now": now,
                },
            )
            mirror_positions_upserted += 1

    # 4. Partial-disappearance soft-close (§2.3.4).
    #
    # Total disappearance (payload empty AND active local rows
    # exist) is handled by the caller-side pre-write guard in
    # `sync_portfolio` — it raises BEFORE any writes happen so
    # position/cash work is not silently rolled back. Here we
    # only handle the partial case: mirrors that have disappeared
    # from a NON-EMPTY payload are soft-closed.
    payload_mirror_ids = [int(m.mirror_id) for m in mirrors]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT mirror_id FROM copy_mirrors WHERE active = TRUE")
        active_local_ids = {int(r["mirror_id"]) for r in cur.fetchall()}

    disappeared_ids = sorted(active_local_ids - set(payload_mirror_ids))

    if disappeared_ids:
        conn.execute(
            """
            UPDATE copy_mirrors
               SET active = FALSE,
                   closed_at = %(now)s,
                   updated_at = %(now)s
             WHERE mirror_id = ANY(%(disappeared_ids)s::bigint[])
               AND active = TRUE
            """,
            {
                "now": now,
                "disappeared_ids": disappeared_ids,
            },
        )
        for mirror_id in disappeared_ids:
            logger.info(
                "mirror %d disappeared from payload — marked closed",
                mirror_id,
            )
        mirrors_closed = len(disappeared_ids)

    return mirrors_upserted, mirror_positions_upserted, mirrors_closed


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

    # Pre-write mirror guard (§2.3.4).
    #
    # Symmetric with the position guard below, but hoisted above
    # every write: if the broker returned an empty mirrors[] list
    # while we have active local mirror rows, refuse the whole
    # sync cycle before any positions, cash, or mirror upserts run.
    # Placing this here (rather than inside _sync_mirrors at step
    # 4) means the raise does not roll back already-written
    # position/cash state — nothing is written yet. Suspicious
    # broker state for mirrors implies the entire payload should
    # not be trusted for this cycle; the operator investigates
    # before the next run is allowed to touch any state.
    if not portfolio.mirrors:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            row = cur.execute("SELECT COUNT(*) AS n FROM copy_mirrors WHERE active = TRUE").fetchone()
        active_mirror_count = int(row["n"]) if row else 0
        if active_mirror_count > 0:
            raise RuntimeError(
                "Broker returned empty mirrors[] but "
                f"{active_mirror_count} active local mirror(s) exist — "
                "refusing to soft-close en masse. Likely upstream API "
                "regression; investigate before manual cleanup."
            )

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
                    --
                    -- Evaluation order: in Postgres ON CONFLICT DO
                    -- UPDATE, every SET expression reads from the
                    -- *pre-update* row snapshot — SET is not a
                    -- sequential assignment.  So `positions.current_units`
                    -- in this CASE WHEN refers to the value BEFORE the
                    -- `current_units = EXCLUDED.current_units` assignment
                    -- above, regardless of SET ordering.  See
                    -- https://www.postgresql.org/docs/current/sql-insert.html
                    -- (ON CONFLICT DO UPDATE — "existing row" semantics).
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

    # Upsert individual broker positions into broker_positions table.
    # Placed AFTER both guards (mirror + position) so that a suspicious
    # broker payload raises before any writes are committed.
    broker_positions_upserted, broker_positions_deleted = _upsert_broker_positions(
        conn,
        portfolio.positions,
        now,
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

    # 4. Reconcile copy-trading mirrors (spec §2.3).
    mirrors_upserted, mirror_positions_upserted, mirrors_closed = _sync_mirrors(conn, portfolio.mirrors, now)

    return PortfolioSyncResult(
        positions_updated=updated,
        positions_opened_externally=opened_externally,
        positions_closed_externally=closed_externally,
        cash_delta=cash_delta,
        broker_cash=broker_cash,
        local_cash=local_cash,
        mirrors_upserted=mirrors_upserted,
        mirrors_closed=mirrors_closed,
        mirror_positions_upserted=mirror_positions_upserted,
        broker_positions_upserted=broker_positions_upserted,
        broker_positions_deleted=broker_positions_deleted,
    )
