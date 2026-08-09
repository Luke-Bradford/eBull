"""Bounded recurring paper-strategy operating loop (#2450).

The cycle reconciles uncertain orders, refreshes five current health blocks,
repairs/manages exact owned positions, then evaluates a small newest-first batch
of funded candidates.  Health rows are updated in place and unchanged position
polls add no rows.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerProvider
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_order_reconciliation import enforce_reconciliation_slo, reconcile_backlog
from app.services.strategy_paper_executor import execute_fired_paper_signal
from app.services.strategy_position_manager import manage_owned_position

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategyPaperCycleResult:
    reconciled_orders: int
    managed_positions: int
    evaluated_signals: int
    active_health_blocks: int


def _set_block(conn: psycopg.Connection[Any], *, source: str, active: bool, reason: str) -> None:
    conn.execute(
        """
        INSERT INTO strategy_execution_blocks (source,active,reason,blocked_at,cleared_at,updated_at)
        VALUES (%s,%s,%s,CASE WHEN %s THEN now() ELSE NULL END,
                CASE WHEN %s THEN NULL ELSE now() END,now())
        ON CONFLICT (source) DO UPDATE SET
          active=EXCLUDED.active, reason=EXCLUDED.reason,
          blocked_at=CASE
            WHEN EXCLUDED.active AND NOT strategy_execution_blocks.active THEN now()
            WHEN EXCLUDED.active THEN strategy_execution_blocks.blocked_at
            ELSE NULL END,
          cleared_at=CASE WHEN EXCLUDED.active THEN NULL ELSE now() END,
          updated_at=now()
        """,
        (source, active, reason, active, active),
    )


def refresh_strategy_health(
    conn: psycopg.Connection[Any], *, broker: BrokerProvider, now: datetime | None = None
) -> int:
    """Refresh bounded current health state for enabled paper deployments."""
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT min(p.max_reconciliation_age_seconds) AS reconciliation_age,
                   min(p.max_scan_age_seconds) AS scan_age,
                   min(p.max_quote_age_seconds) AS quote_age,
                   min(p.max_drawdown_pct) AS drawdown_limit,
                   array_agg(d.deployment_id ORDER BY d.deployment_id) AS deployment_ids
            FROM strategy_deployments d
            JOIN strategy_execution_policies p ON p.deployment_id=d.deployment_id
            WHERE d.mode='paper' AND d.enabled
            """
        )
        policy = cur.fetchone()
    assert policy is not None
    if policy["reconciliation_age"] is None:
        conn.commit()
        with conn.transaction():
            for source in (
                "order_reconciliation",
                "scan_freshness",
                "quote_freshness",
                "broker_availability",
                "drawdown",
            ):
                _set_block(
                    conn,
                    source=source,
                    active=False,
                    reason="no enabled paper strategy deployment requires this health gate",
                )
        return 0

    reconciliation = enforce_reconciliation_slo(conn, max_unresolved_seconds=int(policy["reconciliation_age"]))
    conn.commit()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (
                     WHERE w.updated_at IS NULL OR w.updated_at < %(cutoff)s
                   ) AS stale,
                   count(*) AS total
            FROM strategy_deployments d
            LEFT JOIN strategy_scan_watermark w
              ON w.strategy_id=d.strategy_id AND w.strategy_version=d.strategy_version
            WHERE d.mode='paper' AND d.enabled
            """,
            {"cutoff": observed_at - timedelta(seconds=int(policy["scan_age"]))},
        )
        scan = cur.fetchone()
        assert scan is not None
        cur.execute(
            """
            SELECT count(*) FILTER (
                     WHERE q.quoted_at IS NULL OR q.quoted_at < %(cutoff)s
                   ) AS stale,
                   count(*) AS total
            FROM strategy_position_ownership own
            JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
            JOIN strategy_funding_decisions fd ON fd.funding_decision_id=t.funding_decision_id
            JOIN strategy_deployments d ON d.deployment_id=fd.deployment_id
            LEFT JOIN quotes q ON q.instrument_id=t.instrument_id
            WHERE own.status='active' AND d.mode='paper' AND d.enabled
            """,
            {"cutoff": observed_at - timedelta(seconds=int(policy["quote_age"]))},
        )
        quotes = cur.fetchone()
        assert quotes is not None

    # End the read transaction before the broker network round-trip. Risk state
    # is read afresh afterwards in the transaction that persists the decision.
    conn.rollback()

    risk = None
    try:
        risk = broker.get_account_risk_snapshot()
        broker_active = risk.observed_at < observed_at - timedelta(seconds=int(policy["quote_age"]))
        broker_reason = "broker account-risk snapshot stale" if broker_active else "broker account-risk probe healthy"
    except Exception:
        logger.warning("strategy broker account-risk probe unavailable", exc_info=True)
        broker_active = True
        broker_reason = "broker account-risk probe unavailable"

    scan_active = int(scan["stale"]) > 0
    quote_active = int(quotes["stale"]) > 0
    high_water = Decimal("0")
    drawdown = Decimal("100")
    with conn.transaction():
        if risk is None:
            drawdown_active = True
            drawdown_reason = "drawdown unavailable because broker risk is unavailable"
        else:
            drawdown_row = conn.execute(
                "SELECT equity_high_water FROM strategy_paper_account_risk_state WHERE id=true"
            ).fetchone()
            high_water = max(Decimal(str(drawdown_row[0])) if drawdown_row else risk.equity, risk.equity)
            drawdown = (high_water - risk.equity) / high_water * Decimal("100") if high_water > 0 else Decimal("100")
            drawdown_active = drawdown > Decimal(str(policy["drawdown_limit"]))
            drawdown_reason = (
                f"account drawdown {drawdown}% exceeds configured paper limit"
                if drawdown_active
                else "account drawdown is within configured paper limit"
            )
        if risk is not None and not broker_active:
            conn.execute(
                """
                INSERT INTO strategy_paper_account_risk_state (
                    id,equity_high_water,last_equity,last_drawdown_pct,observed_at
                ) VALUES (true,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                  equity_high_water=GREATEST(
                    strategy_paper_account_risk_state.equity_high_water,EXCLUDED.last_equity
                  ),
                  last_equity=EXCLUDED.last_equity,
                  last_drawdown_pct=(
                    GREATEST(strategy_paper_account_risk_state.equity_high_water,EXCLUDED.last_equity)
                    - EXCLUDED.last_equity
                  ) / GREATEST(
                    strategy_paper_account_risk_state.equity_high_water,EXCLUDED.last_equity
                  ) * 100,
                  observed_at=EXCLUDED.observed_at
                WHERE EXCLUDED.observed_at >= strategy_paper_account_risk_state.observed_at
                """,
                (high_water, risk.equity, drawdown, risk.observed_at),
            )
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO strategy_paper_deployment_risk_state (
                      deployment_id,equity_high_water,last_equity,last_drawdown_pct,
                      max_drawdown_pct,observed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (deployment_id) DO UPDATE SET
                      equity_high_water=GREATEST(
                        strategy_paper_deployment_risk_state.equity_high_water,EXCLUDED.last_equity
                      ),
                      last_equity=EXCLUDED.last_equity,
                      last_drawdown_pct=(
                        GREATEST(strategy_paper_deployment_risk_state.equity_high_water,EXCLUDED.last_equity)
                        - EXCLUDED.last_equity
                      ) / GREATEST(
                        strategy_paper_deployment_risk_state.equity_high_water,EXCLUDED.last_equity
                      ) * 100,
                      max_drawdown_pct=GREATEST(
                        strategy_paper_deployment_risk_state.max_drawdown_pct,
                        (GREATEST(
                          strategy_paper_deployment_risk_state.equity_high_water,EXCLUDED.last_equity
                        ) - EXCLUDED.last_equity) / GREATEST(
                          strategy_paper_deployment_risk_state.equity_high_water,EXCLUDED.last_equity
                        ) * 100
                      ),
                      observed_at=EXCLUDED.observed_at
                    WHERE EXCLUDED.observed_at >= strategy_paper_deployment_risk_state.observed_at
                    """,
                    [
                        (deployment_id, risk.equity, risk.equity, Decimal("0"), Decimal("0"), risk.observed_at)
                        for deployment_id in policy["deployment_ids"]
                    ],
                )
        _set_block(
            conn,
            source="scan_freshness",
            active=scan_active,
            reason=(
                f"{scan['stale']} of {scan['total']} enabled strategy scans are stale"
                if scan_active
                else "enabled strategy scans are current"
            ),
        )
        _set_block(
            conn,
            source="quote_freshness",
            active=quote_active,
            reason=(
                f"{quotes['stale']} of {quotes['total']} owned-position quotes are stale"
                if quote_active
                else "owned-position quotes are current"
            ),
        )
        _set_block(conn, source="broker_availability", active=broker_active, reason=broker_reason)
        _set_block(conn, source="drawdown", active=drawdown_active, reason=drawdown_reason)
    return sum((reconciliation.active_block, scan_active, quote_active, broker_active, drawdown_active))


def run_strategy_paper_cycle(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    signal_limit: int = 5,
    reconciliation_limit: int = 20,
    position_limit: int = 5,
    now: datetime | None = None,
    strategy_versions: Sequence[str] | None = None,
) -> StrategyPaperCycleResult:
    """Run one bounded demo-only strategy lifecycle cycle."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("strategy paper cycle requires an idle connection")
    if signal_limit <= 0 or reconciliation_limit <= 0 or position_limit <= 0:
        raise ValueError("strategy paper cycle limits must be positive")
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)

    reconciled = reconcile_backlog(conn, broker=broker, limit=reconciliation_limit)
    active_blocks = refresh_strategy_health(conn, broker=broker, now=observed_at)
    conn.commit()

    # Rotate the bounded ownership batch by five-minute slot. A fixed
    # ``ORDER BY ... LIMIT`` would protect the same oldest positions forever
    # and starve later positions once the sleeve grows past the cap.
    position_offset = (int(observed_at.timestamp()) // 300) * position_limit
    owned = conn.execute(
        """
        WITH active AS (
          SELECT own.strategy_trade_id,own.broker_position_id,
                 row_number() OVER (ORDER BY own.ownership_id) AS ordinal,
                 count(*) OVER () AS total
          FROM strategy_position_ownership own
          JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
          JOIN strategy_funding_decisions fd ON fd.funding_decision_id=t.funding_decision_id
          JOIN strategy_deployments d ON d.deployment_id=fd.deployment_id
          WHERE own.status='active' AND d.mode='paper'
        )
        SELECT strategy_trade_id,broker_position_id
        FROM active
        ORDER BY mod(ordinal-1-%s+total,total)
        LIMIT %s
        """,
        (position_offset, position_limit),
    ).fetchall()
    conn.commit()
    managed = 0
    for trade_id, position_id in owned:
        manage_owned_position(
            conn,
            broker=broker,
            strategy_trade_id=int(trade_id),
            broker_position_id=int(position_id),
            now=observed_at,
        )
        managed += 1

    versions = (
        list(strategy_versions)
        if strategy_versions is not None
        else [
            entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
            for entry in STRATEGY_MANIFEST.values()
        ]
    )
    candidates = conn.execute(
        """
        SELECT s.signal_id
        FROM strategy_signals s
        JOIN strategy_deployments d
          ON d.strategy_id=s.strategy_id AND d.strategy_version=s.strategy_version
         AND d.mode='paper' AND d.enabled
        JOIN strategy_execution_policies p ON p.deployment_id=d.deployment_id
        JOIN LATERAL (
          SELECT max(sp.promoted_at) AS paper_at
          FROM strategy_promotions sp
          WHERE sp.strategy_id=s.strategy_id
            AND sp.strategy_version=s.strategy_version
            AND sp.to_stage='paper_enabled'
        ) promotion ON s.created_at >= promotion.paper_at
        LEFT JOIN strategy_funding_decisions fd ON fd.signal_id=s.signal_id
        WHERE s.signal_kind='entry' AND s.verdict='fired'
          AND s.strategy_version=ANY(%s) AND fd.signal_id IS NULL
        ORDER BY s.signal_id DESC
        LIMIT %s
        """,
        (versions, signal_limit),
    ).fetchall()
    conn.commit()
    for (signal_id,) in candidates:
        execute_fired_paper_signal(conn, broker=broker, signal_id=int(signal_id), now=observed_at)
    return StrategyPaperCycleResult(len(reconciled), managed, len(candidates), active_blocks)


__all__ = ["StrategyPaperCycleResult", "refresh_strategy_health", "run_strategy_paper_cycle"]
