"""Read models for strategy attribution and exact-owned P&L (#2453).

The strategy surface deliberately derives from existing compact ledgers.  It
does not snapshot quotes, copy broker payloads, or write periodic P&L rows.
Manual positions are excluded structurally: a broker position contributes only
when its exact id is present in ``strategy_position_ownership``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.valuation import resolve_quote_price


@dataclass(frozen=True)
class StrategyAttribution:
    fired_entries: int = 0
    funded_entries: int = 0
    rejected_entries: int = 0
    resolved_entries: int = 0
    winning_entries: int = 0
    win_rate: Decimal | None = None
    median_days_to_outcome: Decimal | None = None
    signals_last_30_days: int = 0
    shadow_average_return_pct: Decimal | None = None
    funded_shadow_average_return_pct: Decimal | None = None
    rejected_shadow_average_return_pct: Decimal | None = None
    opportunity_gap_pct: Decimal | None = None
    funded_capture_rate: Decimal | None = None
    filled_entries: int = 0
    broker_rejected_entries: int = 0
    fill_rate: Decimal | None = None
    broker_rejection_rate: Decimal | None = None
    average_slippage_pct: Decimal | None = None
    average_stressed_cost_usd: Decimal | None = None
    max_observed_account_drawdown_pct: Decimal | None = None


@dataclass(frozen=True)
class StrategyPnl:
    currency: str = "USD"
    strategy_trade_count: int = 0
    owned_position_count: int = 0
    active_position_count: int = 0
    close_event_count: int = 0
    invested_capital: Decimal | None = Decimal("0")
    realised_pnl: Decimal | None = Decimal("0")
    unrealised_pnl: Decimal | None = Decimal("0")
    total_pnl: Decimal | None = Decimal("0")
    observed_fees: Decimal | None = Decimal("0")
    complete: bool = True
    incomplete_reasons: tuple[str, ...] = ()
    # Completed close events remain usable for bankroll accounting while a
    # different newly allocated order is awaiting reconciliation. The public
    # realised_pnl stays conservative and may still be unknown in that state.
    reconciled_realised_pnl: Decimal = Decimal("0")


@dataclass(frozen=True)
class StrategyControlState:
    stage: str | None = None
    pinned_evidence_ready: bool = False
    deployment_id: int | None = None
    capital_limit: Decimal = Decimal("0")
    currency: str = "USD"
    enabled: bool = False
    revision: int | None = None
    reserved_capital: Decimal = Decimal("0")
    policy_configured: bool = False
    max_drawdown_limit_pct: Decimal | None = None
    ticket_sizing_mode: str | None = None
    ticket_fraction: Decimal | None = None
    fixed_ticket_amount: Decimal | None = None
    max_ticket_amount: Decimal | None = None


@dataclass(frozen=True)
class StrategyEntryBlockState:
    global_kill_active: bool
    global_kill_reason: str | None
    global_kill_activated_at: datetime | None
    global_kill_activated_by: str | None
    execution_block_reasons: tuple[str, ...]
    auto_trading_enabled: bool
    live_trading_enabled: bool

    @property
    def new_entries_blocked(self) -> bool:
        return self.global_kill_active or bool(self.execution_block_reasons) or not self.auto_trading_enabled


_ATTRIBUTION_SQL = """
    WITH entry_execution AS (
        SELECT sto.strategy_trade_id,
               SUM(e.opening_units * e.average_price)
                   / NULLIF(SUM(e.opening_units), 0) AS average_price
        FROM strategy_trade_orders sto
        JOIN strategy_order_position_executions e ON e.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
          AND e.opening_units > 0 AND e.average_price > 0
        GROUP BY sto.strategy_trade_id
    ), entry_order AS (
        SELECT DISTINCT ON (sto.strategy_trade_id)
               sto.strategy_trade_id, o.status
        FROM strategy_trade_orders sto
        JOIN orders o ON o.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
        ORDER BY sto.strategy_trade_id, sto.linked_at DESC, sto.order_id DESC
    )
    SELECT s.strategy_id, s.strategy_version,
           COUNT(*) AS fired_entries,
           COUNT(*) FILTER (WHERE fd.verdict = 'allocated') AS funded_entries,
           COUNT(*) FILTER (WHERE fd.verdict IS DISTINCT FROM 'allocated') AS rejected_entries,
           COUNT(*) FILTER (WHERE o.gross_return_pct IS NOT NULL) AS resolved_entries,
           COUNT(*) FILTER (WHERE o.gross_return_pct > 0) AS winning_entries,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY (o.exit_bar_date - s.fill_bar_date)
           ) FILTER (WHERE o.gross_return_pct IS NOT NULL AND o.exit_bar_date IS NOT NULL)
               AS median_days_to_outcome,
           COUNT(*) FILTER (WHERE s.signal_bar_date >= CURRENT_DATE - 30) AS signals_last_30_days,
           AVG(o.gross_return_pct) AS shadow_average_return_pct,
           AVG(o.gross_return_pct) FILTER (WHERE fd.verdict = 'allocated')
               AS funded_shadow_average_return_pct,
           AVG(o.gross_return_pct) FILTER (WHERE fd.verdict IS DISTINCT FROM 'allocated')
               AS rejected_shadow_average_return_pct,
           COUNT(*) FILTER (WHERE fd.verdict = 'allocated' AND ee.average_price IS NOT NULL)
               AS filled_entries,
           COUNT(*) FILTER (
               WHERE fd.verdict = 'allocated'
                 AND ee.average_price IS NULL
                 AND eo.status = 'rejected'
           )
               AS broker_rejected_entries,
           AVG(((ee.average_price - s.fill_price) / NULLIF(s.fill_price, 0)) * 100)
               FILTER (WHERE fd.verdict = 'allocated' AND ee.average_price IS NOT NULL)
               AS average_slippage_pct,
           AVG(pre.stressed_cost_amount) FILTER (WHERE pre.stressed_cost_amount IS NOT NULL)
               AS average_stressed_cost_usd,
           MAX(pre.account_drawdown_pct) AS max_observed_account_drawdown_pct
    FROM strategy_signals s
    LEFT JOIN strategy_outcomes o
      ON o.signal_id = s.signal_id
     AND o.rule_set_version = %(outcome_version)s
     AND o.input_rule_set_version = %(input_version)s
    LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = s.signal_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN entry_execution ee ON ee.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN entry_order eo ON eo.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN strategy_entry_preflights pre ON pre.signal_id = s.signal_id
    WHERE s.verdict = 'fired' AND s.signal_kind = 'entry'
      AND s.strategy_version = ANY(%(versions)s)
    GROUP BY s.strategy_id, s.strategy_version
"""


def load_attribution(
    conn: psycopg.Connection[Any],
    *,
    versions: Sequence[str],
    outcome_version: str,
    input_version: str,
) -> dict[tuple[str, str], StrategyAttribution]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            _ATTRIBUTION_SQL,
            {
                "versions": versions,
                "outcome_version": outcome_version,
                "input_version": input_version,
            },
        )
        rows = cur.fetchall()
    result: dict[tuple[str, str], StrategyAttribution] = {}
    for row in rows:
        fired = int(row["fired_entries"])
        funded = int(row["funded_entries"])
        funded_shadow = row["funded_shadow_average_return_pct"]
        rejected_shadow = row["rejected_shadow_average_return_pct"]
        resolved = int(row["resolved_entries"])
        winners = int(row["winning_entries"])
        filled = int(row["filled_entries"])
        broker_rejected = int(row["broker_rejected_entries"])
        result[(str(row["strategy_id"]), str(row["strategy_version"]))] = StrategyAttribution(
            fired_entries=fired,
            funded_entries=funded,
            rejected_entries=int(row["rejected_entries"]),
            resolved_entries=resolved,
            winning_entries=winners,
            win_rate=Decimal(winners) / Decimal(resolved) if resolved else None,
            median_days_to_outcome=(
                Decimal(str(row["median_days_to_outcome"])) if row["median_days_to_outcome"] is not None else None
            ),
            signals_last_30_days=int(row["signals_last_30_days"]),
            shadow_average_return_pct=row["shadow_average_return_pct"],
            funded_shadow_average_return_pct=funded_shadow,
            rejected_shadow_average_return_pct=rejected_shadow,
            opportunity_gap_pct=(
                rejected_shadow - funded_shadow if rejected_shadow is not None and funded_shadow is not None else None
            ),
            funded_capture_rate=Decimal(funded) / Decimal(fired) if fired else None,
            filled_entries=filled,
            broker_rejected_entries=broker_rejected,
            fill_rate=Decimal(filled) / Decimal(funded) if funded else None,
            broker_rejection_rate=Decimal(broker_rejected) / Decimal(funded) if funded else None,
            average_slippage_pct=row["average_slippage_pct"],
            average_stressed_cost_usd=row["average_stressed_cost_usd"],
            max_observed_account_drawdown_pct=row["max_observed_account_drawdown_pct"],
        )
    return result


_OWNED_LIFECYCLE_SQL = """
    WITH closes AS (
        SELECT position_id, COUNT(*) AS close_event_count,
               SUM(realized_pnl_usd) AS realised_pnl,
               SUM(fees_usd) AS fees,
               BOOL_AND(realized_pnl_usd IS NOT NULL) AS pnl_complete,
               BOOL_AND(fees_usd IS NOT NULL) AS fees_complete
        FROM trade_events
        WHERE event_kind = 'close'
        GROUP BY position_id
    )
    SELECT s.strategy_id, s.strategy_version, t.strategy_trade_id, t.status AS trade_status,
           own.ownership_id, own.broker_position_id, own.status AS ownership_status,
           bp.position_id AS active_broker_position_id, bp.is_buy, bp.units, bp.amount,
           bp.open_rate, bp.open_conversion_rate, bp.total_fees,
           q.last, q.bid, q.ask, pd.close AS daily_close,
           COALESCE(c.close_event_count, 0) AS close_event_count,
           c.realised_pnl, c.fees, c.pnl_complete, c.fees_complete
    FROM strategy_funding_decisions fd
    JOIN strategy_signals s ON s.signal_id = fd.signal_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN strategy_position_ownership own ON own.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN broker_positions bp ON bp.position_id = own.broker_position_id
    LEFT JOIN quotes q ON q.instrument_id = bp.instrument_id
    LEFT JOIN LATERAL (
        SELECT p.close
        FROM price_daily p
        WHERE p.instrument_id = bp.instrument_id AND p.close IS NOT NULL
        ORDER BY p.price_date DESC
        LIMIT 1
    ) pd ON TRUE
    LEFT JOIN closes c ON c.position_id = own.broker_position_id
    WHERE fd.verdict = 'allocated' AND s.strategy_version = ANY(%(versions)s)
    ORDER BY s.strategy_id, s.strategy_version, t.strategy_trade_id, own.ownership_id
"""


def _mark(row: dict[str, Any]) -> Decimal | None:
    price = resolve_quote_price(
        float(row["last"]) if row["last"] is not None else None,
        float(row["bid"]) if row["bid"] is not None else None,
        float(row["ask"]) if row["ask"] is not None else None,
    )
    if price is not None:
        return Decimal(str(price))
    daily = row["daily_close"]
    return Decimal(str(daily)) if daily is not None and Decimal(str(daily)) > 0 else None


def load_owned_pnl(conn: psycopg.Connection[Any], *, versions: Sequence[str]) -> dict[tuple[str, str], StrategyPnl]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_OWNED_LIFECYCLE_SQL, {"versions": versions})
        rows = list(cur.fetchall())

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["strategy_id"]), str(row["strategy_version"]))].append(row)

    result: dict[tuple[str, str], StrategyPnl] = {}
    for key, strategy_rows in grouped.items():
        realised = Decimal("0")
        unrealised = Decimal("0")
        invested = Decimal("0")
        fees = Decimal("0")
        reasons: set[str] = set()
        ownership_count = 0
        active_count = 0
        close_count = 0
        trade_ids: set[int] = set()
        for row in strategy_rows:
            if row["strategy_trade_id"] is None:
                reasons.add("funding_not_reconciled_to_trade")
                continue
            trade_ids.add(int(row["strategy_trade_id"]))
            ownership_id = row["ownership_id"]
            if ownership_id is None:
                if row["trade_status"] != "failed":
                    reasons.add("trade_not_reconciled_to_position")
                continue
            ownership_count += 1
            row_close_count = int(row["close_event_count"])
            close_count += row_close_count
            if row_close_count:
                if row["pnl_complete"] is True:
                    realised += Decimal(str(row["realised_pnl"]))
                else:
                    reasons.add("realised_pnl_missing_from_history")
                if row["fees_complete"] is True:
                    fees += Decimal(str(row["fees"]))
                else:
                    reasons.add("fees_missing_from_history")
            elif row["ownership_status"] == "released":
                reasons.add("released_position_missing_close_history")

            if row["ownership_status"] != "active":
                continue
            active_count += 1
            if row["active_broker_position_id"] is None:
                reasons.add("active_position_missing_from_broker_snapshot")
                continue
            invested += Decimal(str(row["amount"]))
            mark = _mark(row)
            if mark is None:
                reasons.add("active_position_mark_unavailable")
                continue
            units = Decimal(str(row["units"]))
            open_rate = Decimal(str(row["open_rate"]))
            conversion = Decimal(str(row["open_conversion_rate"]))
            direction = Decimal("1") if bool(row["is_buy"]) else Decimal("-1")
            unrealised += direction * units * (mark - open_rate) * conversion

        realised_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "realised_pnl_missing_from_history",
            "released_position_missing_close_history",
        }.intersection(reasons)
        unrealised_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "active_position_missing_from_broker_snapshot",
            "active_position_mark_unavailable",
        }.intersection(reasons)
        invested_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "active_position_missing_from_broker_snapshot",
        }.intersection(reasons)
        fees_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "released_position_missing_close_history",
            "fees_missing_from_history",
        }.intersection(reasons)
        realised_value = realised if realised_known else None
        unrealised_value = unrealised if unrealised_known else None
        result[key] = StrategyPnl(
            strategy_trade_count=len(trade_ids),
            owned_position_count=ownership_count,
            active_position_count=active_count,
            close_event_count=close_count,
            invested_capital=invested if invested_known else None,
            realised_pnl=realised_value,
            unrealised_pnl=unrealised_value,
            total_pnl=(
                realised_value + unrealised_value
                if realised_value is not None and unrealised_value is not None
                else None
            ),
            observed_fees=fees if fees_known else None,
            complete=not reasons,
            incomplete_reasons=tuple(sorted(reasons)),
            reconciled_realised_pnl=realised,
        )
    return result


def realised_pnl_for_keys(
    pnl_by_strategy: dict[tuple[str, str], StrategyPnl],
    keys: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], Decimal] | None:
    """Project reconciled realised values, failing closed on any unknown."""
    result: dict[tuple[str, str], Decimal] = {}
    for key in keys:
        pnl = pnl_by_strategy.get(key, StrategyPnl())
        if {
            "realised_pnl_missing_from_history",
            "released_position_missing_close_history",
        }.intersection(pnl.incomplete_reasons):
            return None
        result[key] = pnl.reconciled_realised_pnl
    return result


def load_paper_realised_pnl(conn: psycopg.Connection[Any]) -> dict[tuple[str, str], Decimal] | None:
    """Return exact-owned realised P&L for every paper deployment.

    Old strategy versions remain part of the shared pot after they are retired;
    limiting this calculation to the current manifest would make realised gains
    or losses disappear from the capital base. ``None`` is fail-closed whenever
    any deployed lifecycle cannot be reconciled completely.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT strategy_id,strategy_version
        FROM strategy_deployments
        WHERE mode='paper'
        """
    ).fetchall()
    keys = [(str(row[0]), str(row[1])) for row in rows]
    pnl_by_strategy = load_owned_pnl(conn, versions=sorted({version for _strategy_id, version in keys}))
    return realised_pnl_for_keys(pnl_by_strategy, keys)


_CONTROL_SQL = """
    WITH current_stage AS (
        SELECT DISTINCT ON (strategy_id, strategy_version)
               strategy_id, strategy_version, to_stage
        FROM strategy_promotions
        WHERE strategy_version = ANY(%(versions)s)
        ORDER BY strategy_id, strategy_version, promotion_id DESC
    ), strategy_keys AS (
        SELECT strategy_id, strategy_version FROM current_stage
        UNION
        SELECT strategy_id, strategy_version
        FROM strategy_deployments
        WHERE strategy_version = ANY(%(versions)s) AND mode = 'paper'
    ), pinned AS (
        SELECT p.strategy_id, p.strategy_version,
               COUNT(*) AS result_count,
               COUNT(*) FILTER (WHERE
                   r.expectancy_ci_low_pct IS NOT NULL
                   AND r.namespace = 'hold_out'
                   AND r.window_start >= DATE '2022-01-01'
                   AND r.universe_basis = 'survivorship_free'
                   AND r.carry_unmodelled = false
                   AND r.fx_unmodelled = false
                   AND r.trial_count IS NOT NULL
                   AND r.deflated_sharpe IS NOT NULL
                   AND r.effective_sample_size IS NOT NULL
                   AND r.synthetic_control_passed = true
               ) AS qualified_result_count
        FROM strategy_promotions p
        JOIN strategy_promotion_results pr ON pr.promotion_id = p.promotion_id
        JOIN strategy_results_store r ON r.result_id = pr.result_id
        WHERE p.strategy_version = ANY(%(versions)s)
        GROUP BY p.strategy_id, p.strategy_version
    ), reserved AS (
        SELECT fd.deployment_id, COALESCE(SUM(fd.amount), 0) AS amount
        FROM strategy_funding_decisions fd
        LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
        WHERE fd.verdict = 'allocated'
          AND (t.strategy_trade_id IS NULL OR t.status NOT IN ('closed', 'failed'))
        GROUP BY fd.deployment_id
    )
    SELECT keys.strategy_id, keys.strategy_version, cs.to_stage,
           COALESCE(pin.result_count, 0) AS result_count,
           COALESCE(pin.qualified_result_count, 0) AS qualified_result_count,
           d.deployment_id, d.capital_limit, d.currency, d.enabled, d.revision,
           COALESCE(res.amount, 0) AS reserved_capital,
           (pol.deployment_id IS NOT NULL) AS policy_configured,
           pol.max_drawdown_pct AS max_drawdown_limit_pct,
           pol.ticket_sizing_mode, pol.ticket_fraction,
           pol.fixed_ticket_amount, pol.max_ticket_amount
    FROM strategy_keys keys
    LEFT JOIN current_stage cs USING (strategy_id, strategy_version)
    LEFT JOIN pinned pin USING (strategy_id, strategy_version)
    LEFT JOIN strategy_deployments d
      ON d.strategy_id = keys.strategy_id AND d.strategy_version = keys.strategy_version
     AND d.mode = 'paper'
    LEFT JOIN reserved res ON res.deployment_id = d.deployment_id
    LEFT JOIN strategy_execution_policies pol ON pol.deployment_id = d.deployment_id
"""


def load_control_state(
    conn: psycopg.Connection[Any], *, versions: Sequence[str]
) -> dict[tuple[str, str], StrategyControlState]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_CONTROL_SQL, {"versions": versions})
        rows = cur.fetchall()
    return {
        (str(row["strategy_id"]), str(row["strategy_version"])): StrategyControlState(
            stage=str(row["to_stage"]) if row["to_stage"] is not None else None,
            pinned_evidence_ready=(
                int(row["result_count"]) > 0 and int(row["qualified_result_count"]) == int(row["result_count"])
            ),
            deployment_id=int(row["deployment_id"]) if row["deployment_id"] is not None else None,
            capital_limit=Decimal(str(row["capital_limit"] or 0)),
            currency=str(row["currency"] or "USD"),
            enabled=bool(row["enabled"]),
            revision=int(row["revision"]) if row["revision"] is not None else None,
            reserved_capital=Decimal(str(row["reserved_capital"])),
            policy_configured=bool(row["policy_configured"]),
            max_drawdown_limit_pct=(
                Decimal(str(row["max_drawdown_limit_pct"])) if row["max_drawdown_limit_pct"] is not None else None
            ),
            ticket_sizing_mode=(str(row["ticket_sizing_mode"]) if row["ticket_sizing_mode"] is not None else None),
            ticket_fraction=(Decimal(str(row["ticket_fraction"])) if row["ticket_fraction"] is not None else None),
            fixed_ticket_amount=(
                Decimal(str(row["fixed_ticket_amount"])) if row["fixed_ticket_amount"] is not None else None
            ),
            max_ticket_amount=(
                Decimal(str(row["max_ticket_amount"])) if row["max_ticket_amount"] is not None else None
            ),
        )
        for row in rows
    }


def load_entry_block_state(conn: psycopg.Connection[Any]) -> StrategyEntryBlockState:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT is_active, reason, activated_at, activated_by FROM kill_switch ORDER BY id DESC LIMIT 1")
        kill = cur.fetchone()
        cur.execute("SELECT reason FROM strategy_execution_blocks WHERE active ORDER BY source")
        blocks = tuple(str(row["reason"]) for row in cur.fetchall())
        cur.execute("SELECT enable_auto_trading, enable_live_trading FROM runtime_config ORDER BY id DESC LIMIT 1")
        runtime = cur.fetchone()
    # Missing singleton rows are fail-closed and visible rather than treated as
    # disabled-but-healthy configuration.
    if kill is None:
        return StrategyEntryBlockState(True, "kill switch state unavailable", None, None, blocks, False, False)
    if runtime is None:
        blocks = (*blocks, "runtime configuration unavailable")
    elif not bool(runtime["enable_auto_trading"]):
        blocks = (*blocks, "automatic trading disabled")
    return StrategyEntryBlockState(
        global_kill_active=bool(kill["is_active"]),
        global_kill_reason=str(kill["reason"]) if kill["reason"] is not None else None,
        global_kill_activated_at=kill["activated_at"],
        global_kill_activated_by=(str(kill["activated_by"]) if kill["activated_by"] is not None else None),
        execution_block_reasons=blocks,
        auto_trading_enabled=bool(runtime["enable_auto_trading"]) if runtime is not None else False,
        live_trading_enabled=bool(runtime["enable_live_trading"]) if runtime is not None else False,
    )


__all__ = [
    "StrategyAttribution",
    "StrategyControlState",
    "StrategyEntryBlockState",
    "StrategyPnl",
    "load_attribution",
    "load_control_state",
    "load_entry_block_state",
    "load_owned_pnl",
]
