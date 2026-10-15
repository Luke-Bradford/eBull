"""Momentum book (#3822) — the entry intent: authorise one formed entry and price its protective levels.

Read-only: it loads the signal, its formation, the paper deployment and execution policy, the shared pool's
mandate, the stored quote and the halt feed, and returns either a ``MomentumIntent`` (the shape the shared paper
helpers size and submit from) or the refusal code. Nothing here touches the broker or writes a row; the executor
(``momentum_book_executor``) does both.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final, Literal, cast
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from app.services import momentum_book as mb
from app.services.strategy_base_currency import DEPLOYMENT_CURRENCY_UNSUPPORTED, SUPPORTED_DEPLOYMENT_CURRENCIES
from app.services.strategy_control_plane import EFFECTIVE_MAX_CONCURRENT_SQL, registered_strategy_purpose
from app.services.strategy_halt_identity import INSTRUMENT_HALT_SYMBOL_SQL
from app.services.strategy_paper_executor import _age_ok, _session_is_open, protective_rates

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")
CURRENCY: Final = "USD"


@dataclass(frozen=True)
class MomentumIntent:
    """One entry, authorised and priced: the fields ``_risk_and_amount`` and the shared helpers read."""

    signal_id: int
    strategy_id: str
    strategy_version: str
    instrument_id: int
    symbol: str
    deployment_id: int
    currency: str
    deployment_limit: Decimal
    pool_limit: Decimal
    capital_mode: Literal["fixed", "compound"]
    pool_reserved: Decimal
    mandate_max_drawdown_pct: Decimal
    mandate_max_loss_per_position_pct: Decimal
    mandate_max_daily_loss_pct: Decimal
    mandate_active_risk_budget_pct: Decimal
    mandate_cash_reserve_pct: Decimal
    mandate_max_concurrent_positions: int
    policy_revision: int
    max_ticket_amount: Decimal
    fixed_ticket_amount: Decimal
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_quote_age_seconds: int
    max_halt_feed_age_seconds: int
    max_cost_age_seconds: int
    max_reconciliation_age_seconds: int
    max_instrument_exposure_pct: Decimal
    max_portfolio_exposure_pct: Decimal
    max_drawdown_pct: Decimal
    cost_stress_multiplier: Decimal
    quote_at: datetime
    ask: Decimal
    halt_feed_at: datetime
    reserved: Decimal
    formation_id: int
    formation_month: date
    rank: int
    stop_rate: Decimal
    take_rate: Decimal


_INTENT_SQL: Final = f"""
    SELECT s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id,
           i.symbol, i.is_tradable, e.asset_class,
           me.formation_id, me.rank, f.formation_month, f.target_session,
           d.deployment_id, d.capital_limit, d.enabled, d.currency,
           pool.enabled AS pool_enabled, pool.capital_limit AS pool_limit, pool.capital_mode, pool.risk_profile,
           pool.max_portfolio_drawdown_pct AS mandate_max_drawdown_pct,
           pool.max_loss_per_position_pct AS mandate_max_loss_per_position_pct,
           pool.max_daily_loss_pct AS mandate_max_daily_loss_pct,
           pool.active_risk_budget_pct AS mandate_active_risk_budget_pct,
           pool.cash_reserve_pct AS mandate_cash_reserve_pct,
           pool.max_concurrent_positions AS mandate_max_concurrent_positions,
           p.revision AS policy_revision, p.max_ticket_amount, p.fixed_ticket_amount,
           p.stop_loss_pct, p.take_profit_pct,
           p.max_quote_age_seconds, p.max_halt_feed_age_seconds, p.max_cost_age_seconds,
           p.max_reconciliation_age_seconds, p.max_instrument_exposure_pct,
           p.max_portfolio_exposure_pct, p.max_drawdown_pct, p.cost_stress_multiplier,
           q.quoted_at, q.ask, q.spread_flag,
           h.fetched_at AS halt_feed_at,
           EXISTS (
               SELECT 1 FROM strategy_market_halts mh
               WHERE mh.source = 'nasdaq_trader_rss'
                 AND mh.symbol = {INSTRUMENT_HALT_SYMBOL_SQL} AND mh.resumed_at IS NULL
           ) AS is_halted,
           EXISTS (SELECT 1 FROM strategy_execution_blocks b WHERE b.active) AS execution_blocked,
           (
               SELECT COALESCE(SUM(fd.amount), 0)
               FROM strategy_funding_decisions fd
               LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
               WHERE fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
                 AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
           ) AS reserved,
           (
               SELECT COALESCE(SUM(fd.amount), 0)
               FROM strategy_funding_decisions fd
               JOIN strategy_deployments reserved_d
                 ON reserved_d.deployment_id = fd.deployment_id AND reserved_d.mode = 'paper'
               LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
               WHERE fd.verdict = 'allocated'
                 AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
           ) AS pool_reserved
    FROM strategy_signals s
    JOIN instruments i ON i.instrument_id = s.instrument_id
    LEFT JOIN exchanges e ON e.exchange_id = i.exchange
    LEFT JOIN momentum_book_entries me ON me.signal_id = s.signal_id
    LEFT JOIN momentum_book_formations f ON f.formation_id = me.formation_id
    LEFT JOIN strategy_deployments d
      ON d.strategy_id = s.strategy_id AND d.strategy_version = s.strategy_version AND d.mode = 'paper'
    LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
    LEFT JOIN LATERAL (
        SELECT enabled, capital_limit, capital_mode, risk_profile,
               max_portfolio_drawdown_pct, max_loss_per_position_pct, max_daily_loss_pct,
               active_risk_budget_pct, cash_reserve_pct,
               {EFFECTIVE_MAX_CONCURRENT_SQL} AS max_concurrent_positions
        FROM strategy_paper_pool_events
        ORDER BY strategy_paper_pool_event_id DESC
        LIMIT 1
    ) pool ON true
    LEFT JOIN quotes q ON q.instrument_id = s.instrument_id
    LEFT JOIN strategy_halt_feed_state h ON h.source = 'nasdaq_trader_rss'
    WHERE s.signal_id = %(signal_id)s AND s.signal_kind = 'entry' AND s.verdict = 'fired'
"""


def _d(value: object) -> Decimal:
    return Decimal(str(value))


def _positive(value: object) -> bool:
    return value is not None and _d(value).is_finite() and _d(value) > 0


def load_intent(conn: Conn, *, signal_id: int, now: datetime) -> tuple[MomentumIntent | None, str | None, bool]:
    """Authorise one entry, or return the refusal code. The bool says the halt SQL ran (same contract as the paper
    and pot loaders)."""
    with conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(_INTENT_SQL, {"signal_id": signal_id}).fetchone()
    if row is None:
        return None, "signal_not_fired_entry", False
    if row["strategy_id"] != mb.STRATEGY_ID or registered_strategy_purpose(str(row["strategy_id"])) != "demo_trial":
        return None, "strategy_not_momentum_book", True
    if row["formation_id"] is None:
        return None, "momentum_entry_unformed", True
    today = now.astimezone(_NEW_YORK).date()
    if row["target_session"] < today:
        return None, "decision_expired", True
    if row["target_session"] > today:
        return None, "decision_not_yet_due", True
    checks = (
        (bool(row["is_tradable"]), "instrument_not_tradable"),
        (row["asset_class"] == "us_equity", "unsupported_market_session"),
        (row["deployment_id"] is not None, "paper_deployment_missing"),
        (row["pool_limit"] is not None, "paper_pool_unconfigured"),
        (bool(row["pool_enabled"]), "paper_pool_disabled"),
        (row["risk_profile"] is not None and row["risk_profile"] != "unconfigured", "portfolio_mandate_unconfigured"),
        (
            all(
                row[f] is not None
                for f in (
                    "mandate_max_drawdown_pct",
                    "mandate_max_loss_per_position_pct",
                    "mandate_max_daily_loss_pct",
                    "mandate_active_risk_budget_pct",
                    "mandate_cash_reserve_pct",
                    "mandate_max_concurrent_positions",
                )
            ),
            "portfolio_mandate_incomplete",
        ),
        (bool(row["enabled"]), "paper_deployment_disabled"),
        (row["currency"] in SUPPORTED_DEPLOYMENT_CURRENCIES, DEPLOYMENT_CURRENCY_UNSUPPORTED),
        (row["currency"] == CURRENCY, "momentum_currency_not_usd"),
        (row["policy_revision"] is not None, "execution_policy_missing"),
        (_positive(row["fixed_ticket_amount"]), "momentum_ticket_unconfigured"),
        (not bool(row["execution_blocked"]), "execution_block_active"),
        (row["quoted_at"] is not None and row["ask"] is not None, "quote_missing"),
        (_positive(row["ask"]), "quote_ask_invalid"),
        (not bool(row["spread_flag"]), "quote_spread_flagged"),
        (row["halt_feed_at"] is not None, "halt_feed_missing"),
        (not bool(row["is_halted"]), "instrument_halted"),
    )
    for passed, reason in checks:
        if not passed:
            return None, reason, True
    quote_at = cast(datetime, row["quoted_at"])
    halt_feed_at = cast(datetime, row["halt_feed_at"])
    if not _age_ok(quote_at, now=now, max_seconds=int(row["max_quote_age_seconds"])):
        return None, "quote_stale", True
    if not _age_ok(halt_feed_at, now=now, max_seconds=int(row["max_halt_feed_age_seconds"])):
        return None, "halt_feed_stale", True
    if not _session_is_open(now):
        return None, "market_session_closed", True
    ask = _d(row["ask"])
    stop_pct, take_pct = _d(row["stop_loss_pct"]), _d(row["take_profit_pct"])
    stop_rate, take_rate = protective_rates(ask, stop_pct, take_pct)
    return (
        MomentumIntent(
            signal_id=signal_id,
            strategy_id=str(row["strategy_id"]),
            strategy_version=str(row["strategy_version"]),
            instrument_id=int(row["instrument_id"]),
            symbol=str(row["symbol"]),
            deployment_id=int(row["deployment_id"]),
            currency=str(row["currency"]),
            deployment_limit=_d(row["capital_limit"]),
            pool_limit=_d(row["pool_limit"]),
            capital_mode="fixed",
            pool_reserved=_d(row["pool_reserved"]),
            mandate_max_drawdown_pct=_d(row["mandate_max_drawdown_pct"]),
            mandate_max_loss_per_position_pct=_d(row["mandate_max_loss_per_position_pct"]),
            mandate_max_daily_loss_pct=_d(row["mandate_max_daily_loss_pct"]),
            mandate_active_risk_budget_pct=_d(row["mandate_active_risk_budget_pct"]),
            mandate_cash_reserve_pct=_d(row["mandate_cash_reserve_pct"]),
            mandate_max_concurrent_positions=int(row["mandate_max_concurrent_positions"]),
            policy_revision=int(row["policy_revision"]),
            max_ticket_amount=_d(row["max_ticket_amount"]),
            fixed_ticket_amount=_d(row["fixed_ticket_amount"]),
            stop_loss_pct=stop_pct,
            take_profit_pct=take_pct,
            max_quote_age_seconds=int(row["max_quote_age_seconds"]),
            max_halt_feed_age_seconds=int(row["max_halt_feed_age_seconds"]),
            max_cost_age_seconds=int(row["max_cost_age_seconds"]),
            max_reconciliation_age_seconds=int(row["max_reconciliation_age_seconds"]),
            max_instrument_exposure_pct=_d(row["max_instrument_exposure_pct"]),
            max_portfolio_exposure_pct=_d(row["max_portfolio_exposure_pct"]),
            max_drawdown_pct=_d(row["max_drawdown_pct"]),
            cost_stress_multiplier=_d(row["cost_stress_multiplier"]),
            quote_at=quote_at,
            ask=ask,
            halt_feed_at=halt_feed_at,
            reserved=_d(row["reserved"]),
            formation_id=int(row["formation_id"]),
            formation_month=cast(date, row["formation_month"]),
            rank=int(row["rank"]),
            stop_rate=stop_rate,
            take_rate=take_rate,
        ),
        None,
        True,
    )


__all__ = ["CURRENCY", "MomentumIntent", "load_intent"]
