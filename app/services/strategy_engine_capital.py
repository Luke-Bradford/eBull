"""One exact observation of capital committed inside the shared strategy pot.

The paper pool is the operator's allocation authority.  Broker account cash is
only an independent ability-to-pay ceiling because the account also contains
manual holdings.  This module owns the database population and joins exact core
ownership to one live P&L snapshot; callers must not recreate either half.

Spec: ``docs/proposals/ta/2026-08-24-core-assigned-capital-boundary.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import psycopg
import psycopg.rows

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.strategy_capital_sandbox import CapitalMode, SandboxHeadroom, sandbox_headroom
from app.services.strategy_core_arc_sql import core_arm_joins, core_arm_present

_ZERO = Decimal("0")
_TERMINAL_TRADES = frozenset({"closed", "failed"})
_TERMINAL_RECONCILIATION = frozenset({"resolved", "rejected"})


class EngineCapitalObservationError(RuntimeError):
    """The shared capital population or its exact broker join is incomplete."""


@dataclass(frozen=True)
class EngineCapitalAuthority:
    pool_event_id: int
    enabled: bool
    capital_limit: Decimal
    capital_mode: CapitalMode
    epoch_started_at: datetime
    realised_delta: Decimal
    alpha_committed: Decimal
    alpha_working: Decimal
    core_pending_committed: Decimal
    core_active_recorded_committed: Decimal
    core_active_position_ids: tuple[int, ...]


@dataclass(frozen=True)
class EngineCapitalUsage:
    authority: EngineCapitalAuthority
    headroom: SandboxHeadroom
    committed: Decimal
    working: Decimal
    core_market_value: Decimal
    core_active_committed: Decimal


def _money(value: object, *, label: str, positive: bool = False) -> Decimal:
    try:
        money = Decimal(str(value))
    except Exception as exc:
        raise EngineCapitalObservationError(f"{label} is not numeric") from exc
    if not money.is_finite() or money < _ZERO or (positive and money <= _ZERO):
        raise EngineCapitalObservationError(f"{label} is outside safe bounds")
    return money


def _load_realised_delta(conn: psycopg.Connection[Any], *, epoch: datetime) -> Decimal:
    """The existing F-0 exact-owned close rule, widened to both strategy arms."""
    row = conn.execute(
        f"""
        WITH eligible_trades AS (
            SELECT trade.strategy_trade_id
            FROM strategy_trades trade
            LEFT JOIN strategy_funding_decisions funding
              ON funding.funding_decision_id=trade.funding_decision_id
             AND funding.verdict='allocated'
            LEFT JOIN strategy_deployments deployment
              ON deployment.deployment_id=funding.deployment_id
             AND deployment.mode='paper'
            {core_arm_joins("trade")}
            WHERE trade.created_at >= %s
              AND (deployment.deployment_id IS NOT NULL
                   OR {core_arm_present("trade")})
        ), owned AS (
            SELECT ownership.broker_position_id,ownership.status
            FROM strategy_position_ownership ownership
            JOIN eligible_trades trade USING (strategy_trade_id)
        ), closes AS (
            SELECT owned.broker_position_id,owned.status,
                   count(event.event_id) AS close_count,
                   count(event.event_id) FILTER (WHERE event.realized_pnl_usd IS NULL) AS missing_pnl,
                   COALESCE(sum(event.realized_pnl_usd),0) AS realised
            FROM owned
            LEFT JOIN trade_events event
              ON event.position_id=owned.broker_position_id AND event.event_kind='close'
            GROUP BY owned.broker_position_id,owned.status
        )
        SELECT COALESCE(sum(realised),0),
               count(*) FILTER (WHERE missing_pnl > 0),
               count(*) FILTER (WHERE status='released' AND close_count=0)
        FROM closes
        """,
        (epoch,),
    ).fetchone()
    assert row is not None
    if int(row[1]) or int(row[2]):
        raise EngineCapitalObservationError("exact-owned realised P&L is incomplete")
    value = Decimal(str(row[0]))
    if not value.is_finite():
        raise EngineCapitalObservationError("exact-owned realised P&L is not finite")
    return value


def load_engine_capital_authority(conn: psycopg.Connection[Any]) -> EngineCapitalAuthority | None:
    """Load the current pool and every cash authority it has issued.

    ``None`` means no paper-pool event has ever established an assigned pot.
    """
    pool = conn.execute(
        """
        SELECT latest.strategy_paper_pool_event_id,latest.enabled,latest.capital_limit,
               latest.capital_mode,first_event.changed_at
        FROM LATERAL (
            SELECT strategy_paper_pool_event_id,enabled,capital_limit,capital_mode
            FROM strategy_paper_pool_events
            ORDER BY strategy_paper_pool_event_id DESC LIMIT 1
        ) latest
        CROSS JOIN LATERAL (
            SELECT changed_at FROM strategy_paper_pool_events
            ORDER BY strategy_paper_pool_event_id LIMIT 1
        ) first_event
        """
    ).fetchone()
    if pool is None:
        return None
    pool_event_id = int(pool[0])
    capital_limit = _money(pool[2], label="paper pool capital limit")
    mode = str(pool[3])
    if mode not in {"fixed", "compound"}:
        raise EngineCapitalObservationError("paper pool capital mode is unsupported")
    capital_mode = cast(CapitalMode, mode)
    epoch = cast(datetime, pool[4])

    pre_epoch_funding = conn.execute(
        """
        SELECT count(*)
        FROM strategy_funding_decisions funding
        JOIN strategy_deployments deployment
          ON deployment.deployment_id=funding.deployment_id AND deployment.mode='paper'
        LEFT JOIN strategy_trades trade ON trade.funding_decision_id=funding.funding_decision_id
        WHERE funding.verdict='allocated' AND funding.decided_at < %s
          AND (trade.strategy_trade_id IS NULL
               OR trade.status NOT IN ('closed','failed')
               OR EXISTS (
                   SELECT 1 FROM strategy_position_ownership ownership
                   WHERE ownership.strategy_trade_id=trade.strategy_trade_id
                     AND ownership.status='active'
               ))
        """,
        (epoch,),
    ).fetchone()
    assert pre_epoch_funding is not None
    if int(pre_epoch_funding[0]):
        raise EngineCapitalObservationError("a non-terminal strategy lifecycle predates the assigned pot")

    # Core trades have no funding decision, so their epoch guard remains rooted
    # at the trade. Signal-arm lifecycles are guarded above from the allocation
    # itself, including the valid pending state where no trade exists yet.
    pre_epoch_core = conn.execute(
        f"""
        SELECT count(*)
        FROM strategy_trades trade
        {core_arm_joins("trade")}
        WHERE trade.created_at < %s
          AND {core_arm_present("trade")}
          AND (trade.status NOT IN ('closed','failed') OR EXISTS (
              SELECT 1 FROM strategy_position_ownership ownership
              WHERE ownership.strategy_trade_id=trade.strategy_trade_id
                AND ownership.status='active'
          ))
        """,
        (epoch,),
    ).fetchone()
    assert pre_epoch_core is not None
    if int(pre_epoch_core[0]):
        raise EngineCapitalObservationError("a non-terminal strategy lifecycle predates the assigned pot")

    alpha_rows = conn.execute(
        """
        SELECT funding.funding_decision_id,funding.amount,
               trade.strategy_trade_id,trade.status,
               count(DISTINCT ownership.ownership_id) FILTER (WHERE ownership.status='active') AS active_owned
        FROM strategy_funding_decisions funding
        JOIN strategy_deployments deployment
          ON deployment.deployment_id=funding.deployment_id AND deployment.mode='paper'
        LEFT JOIN strategy_trades trade ON trade.funding_decision_id=funding.funding_decision_id
        LEFT JOIN strategy_position_ownership ownership
          ON ownership.strategy_trade_id=trade.strategy_trade_id
        WHERE funding.verdict='allocated' AND funding.decided_at >= %s
        GROUP BY funding.funding_decision_id,funding.amount,trade.strategy_trade_id,trade.status
        ORDER BY funding.funding_decision_id
        """,
        (epoch,),
    ).fetchall()
    alpha_committed = _ZERO
    alpha_working = _ZERO
    for row in alpha_rows:
        amount = _money(row[1], label=f"funding decision {row[0]} amount", positive=True)
        trade_id = row[2]
        if trade_id is None:
            alpha_committed += amount
            continue
        status = str(row[3])
        active_owned = int(row[4])
        terminal = status in _TERMINAL_TRADES
        if terminal and active_owned:
            raise EngineCapitalObservationError(f"paper trade {trade_id} terminal state is inconsistent")
        if not terminal:
            alpha_committed += amount
            if active_owned:
                alpha_working += amount

    core_rows = conn.execute(
        f"""
        SELECT trade.strategy_trade_id,trade.status,trade.instrument_id,
               count(DISTINCT link.order_id) FILTER (WHERE link.purpose='entry') AS entry_count,
               min(entry.instrument_id) FILTER (WHERE link.purpose='entry') AS entry_instrument,
               min(entry.action) FILTER (WHERE link.purpose='entry') AS entry_action,
               min(entry.order_type) FILTER (WHERE link.purpose='entry') AS entry_type,
               min(entry.execution_origin) FILTER (WHERE link.purpose='entry') AS entry_origin,
               min(entry.requested_amount) FILTER (WHERE link.purpose='entry') AS requested_amount,
               min(recon.state) FILTER (WHERE link.purpose='entry') AS recon_state,
               array_agg(DISTINCT ownership.broker_position_id ORDER BY ownership.broker_position_id)
                 FILTER (WHERE ownership.status='active') AS active_position_ids,
               min(core_intent.action) AS intent_action,
               min(core_mandate.core_mandate_event_id) AS paper_mandate_id
        FROM strategy_trades trade
        {core_arm_joins("trade")}
        LEFT JOIN strategy_trade_orders link ON link.strategy_trade_id=trade.strategy_trade_id
        LEFT JOIN orders entry ON entry.order_id=link.order_id
        LEFT JOIN strategy_order_reconciliation_state recon ON recon.order_id=entry.order_id
        LEFT JOIN strategy_position_ownership ownership
          ON ownership.strategy_trade_id=trade.strategy_trade_id
        WHERE trade.created_at >= %s AND {core_arm_present("trade")}
        GROUP BY trade.strategy_trade_id,trade.status,trade.instrument_id
        ORDER BY trade.strategy_trade_id
        """,
        (epoch,),
    ).fetchall()
    core_pending = _ZERO
    core_active_recorded = _ZERO
    active_ids: list[int] = []
    for row in core_rows:
        trade_id = int(row[0])
        status = str(row[1])
        entry_count = int(row[3])
        recon_state = None if row[9] is None else str(row[9])
        owned_ids = tuple(int(value) for value in (row[10] or []))
        if (
            row[11] != "buy_core"
            or row[12] is None
            or entry_count != 1
            or row[4] != row[2]
            or row[5] != "BUY"
            or row[6] != "MARKET"
            or row[7] != "strategy"
            or recon_state is None
        ):
            raise EngineCapitalObservationError(f"core trade {trade_id} entry authority is incomplete")
        terminal = status in _TERMINAL_TRADES
        if terminal:
            if recon_state not in _TERMINAL_RECONCILIATION or owned_ids:
                raise EngineCapitalObservationError(f"core trade {trade_id} terminal state is inconsistent")
            continue
        if recon_state != "resolved":
            if owned_ids:
                raise EngineCapitalObservationError(f"core trade {trade_id} owns positions before entry resolution")
            core_pending += _money(row[8], label=f"core trade {trade_id} requested amount", positive=True)
            continue
        if not owned_ids:
            raise EngineCapitalObservationError(f"resolved core trade {trade_id} has no active exact ownership")
        core_active_recorded += _money(row[8], label=f"core trade {trade_id} requested amount", positive=True)
        active_ids.extend(owned_ids)
    if len(active_ids) != len(set(active_ids)):
        raise EngineCapitalObservationError("active core ownership ids are duplicated")

    return EngineCapitalAuthority(
        pool_event_id=pool_event_id,
        enabled=bool(pool[1]),
        capital_limit=capital_limit,
        capital_mode=capital_mode,
        epoch_started_at=epoch,
        realised_delta=_load_realised_delta(conn, epoch=epoch),
        alpha_committed=alpha_committed,
        alpha_working=alpha_working,
        core_pending_committed=core_pending,
        core_active_recorded_committed=core_active_recorded,
        core_active_position_ids=tuple(sorted(active_ids)),
    )


def resolve_engine_capital_usage(
    authority: EngineCapitalAuthority,
    snapshot: BrokerAccountRiskSnapshot,
    *,
    core_instrument_id: int | None,
) -> EngineCapitalUsage:
    """Join active core ownership to the same snapshot used for execution."""
    snapshot_currency = (
        None
        if snapshot.account_currency_id is None
        else DOCUMENTED_ACCOUNT_CURRENCIES.get(snapshot.account_currency_id)
    )
    if snapshot_currency != "USD":
        raise EngineCapitalObservationError("broker account currency is not observed as USD")
    positions = {row.position_id: row for row in snapshot.direct_positions}
    if len(positions) != len(snapshot.direct_positions):
        raise EngineCapitalObservationError("broker snapshot repeats a direct position id")
    core_committed = _ZERO
    core_market_value = _ZERO
    if authority.core_active_position_ids and core_instrument_id is None:
        raise EngineCapitalObservationError("active core ownership has no configured instrument")
    for position_id in authority.core_active_position_ids:
        row = positions.get(position_id)
        if row is None:
            raise EngineCapitalObservationError(f"active core position {position_id} is absent from broker snapshot")
        if row.instrument_id != core_instrument_id:
            raise EngineCapitalObservationError(f"active core position {position_id} belongs to another instrument")
        if not row.is_buy:
            raise EngineCapitalObservationError(f"active core position {position_id} is short")
        if row.is_partially_altered:
            raise EngineCapitalObservationError(f"active core position {position_id} is partially altered")
        core_committed += _money(row.amount, label=f"active core position {position_id} amount")
        if not row.market_value.is_finite() or row.market_value < _ZERO:
            raise EngineCapitalObservationError(f"active core position {position_id} market value is invalid")
        core_market_value += row.market_value

    committed = authority.alpha_committed + authority.core_pending_committed + core_committed
    working = authority.alpha_working + core_committed
    headroom = sandbox_headroom(
        capital_limit=authority.capital_limit,
        capital_mode=authority.capital_mode,
        realised_delta=authority.realised_delta,
        committed=committed,
    )
    return EngineCapitalUsage(authority, headroom, committed, working, core_market_value, core_committed)


__all__ = [
    "EngineCapitalAuthority",
    "EngineCapitalObservationError",
    "EngineCapitalUsage",
    "load_engine_capital_authority",
    "resolve_engine_capital_usage",
]
