"""#3471 slice 2c-i — the step-0 capacity preview (spec §8 "Start gate").

Before any model call, the run is refused (``trial_capacity_unavailable:<reason>``) when the
capacity arithmetic could not admit ONE half-ticket pair. It is a best-effort preview, not a
guarantee: state can move between the preview and execution, and an execution-time refusal
is a broken pair.

* **Inputs:** the current pool and its mandate, the #2844 engine capital authority, the latest
  account-risk snapshot, the local pending-order and mandate observations, and each leg's
  deployment and policy.
* **Preview terms (frozen):** a 25% stop, which is adverse for loss-at-stop capacity, and zero
  existing instrument exposure, which is favourable.
* **Joint admission:** the shared terms (pool, cash, portfolio exposure, active risk, cash
  reserve, concurrency) must hold BOTH legs' tickets; the per-leg terms (deployment,
  instrument, loss at stop, max ticket, leg slots) must each hold one.

The arithmetic is the paper path's own (``strategy_paper_executor._capacities``), so the preview
and the executor cannot drift apart. The DB half is read-only: it never calls
``_observe_local_mandate_risk``, which advances the account high-water mark.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any, Final

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services.ai_trial_executor import TRIAL_MAX_CONCURRENT_PER_LEG
from app.services.ai_trial_intent import TRIAL_CAPITAL_MODE, TRIAL_TICKET_USD
from app.services.strategy_capital_sandbox import SANDBOX_EXCEEDED, sandbox_bound
from app.services.strategy_core_mandate import load_core_mandate
from app.services.strategy_engine_capital import (
    EngineCapitalObservationError,
    load_engine_capital_authority,
    resolve_engine_capital_usage,
)
from app.services.strategy_monitoring import load_paper_realised_pnl
from app.services.strategy_paper_executor import (
    _CENT,
    _MANDATE_OBSERVATION_SQL,
    _NY,
    _PENDING_RISK_SQL,
    StrategyPaperExecutionError,
    _capacities,
)

#: §8 "Start gate" preview terms, frozen by construction.
PREVIEW_STOP_PCT: Final = Decimal("25")
PREVIEW_TICKET: Final = TRIAL_TICKET_USD["half"]
REFUSAL_PREFIX: Final = "trial_capacity_unavailable:"


@dataclass(frozen=True)
class PreviewShared:
    """The account-, pool- and mandate-level facts every leg shares."""

    within_bound: bool
    pool_base: Decimal
    committed: Decimal
    # Non-core committed only: the active-risk budget's charge (#3471 §8, 2026-09-29).
    active_committed: Decimal
    equity: Decimal
    total_invested: Decimal
    available_cash: Decimal
    pending_total: Decimal
    drawdown_pct: Decimal
    open_lifecycles: int
    daily_realised_pnl: Decimal
    mandate_max_drawdown_pct: Decimal
    mandate_max_loss_per_position_pct: Decimal
    mandate_max_daily_loss_pct: Decimal
    mandate_active_risk_budget_pct: Decimal
    mandate_cash_reserve_pct: Decimal
    mandate_max_concurrent_positions: int


@dataclass(frozen=True)
class PreviewLeg:
    """One leg's deployment and execution policy."""

    deployment_base: Decimal
    deployment_reserved: Decimal
    open_trades: int
    max_ticket_amount: Decimal
    max_portfolio_exposure_pct: Decimal
    max_instrument_exposure_pct: Decimal
    max_drawdown_pct: Decimal


def _whole_cents(value: Decimal) -> Decimal:
    return value.quantize(_CENT, rounding=ROUND_DOWN)


def start_gate_reason(
    shared: PreviewShared,
    legs: Sequence[PreviewLeg],
    *,
    ticket: Decimal = PREVIEW_TICKET,
    stop_pct: Decimal = PREVIEW_STOP_PCT,
) -> str | None:
    """``None`` when one pair of ``ticket``-sized legs is admissible, else the refusal.

    Refusal order follows ``_risk_and_amount``: sandbox, concurrency, daily loss, drawdown,
    then the capacity terms. Every refusal carries ``REFUSAL_PREFIX``.
    """

    def refuse(reason: str) -> str:
        return REFUSAL_PREFIX + reason

    if not legs:
        return refuse("no_legs")
    if not shared.within_bound:
        return refuse(SANDBOX_EXCEEDED)
    if shared.open_lifecycles + len(legs) > shared.mandate_max_concurrent_positions:
        return refuse("portfolio_concurrency_limit")
    if shared.daily_realised_pnl <= -(shared.pool_base * shared.mandate_max_daily_loss_pct / Decimal("100")):
        return refuse("portfolio_daily_loss_limit")
    shared_room: dict[str, Decimal] = {}
    for leg in legs:
        if shared.drawdown_pct >= leg.max_drawdown_pct:
            return refuse("account_drawdown_limit")
        if shared.drawdown_pct >= shared.mandate_max_drawdown_pct:
            return refuse("portfolio_drawdown_limit")
        if leg.open_trades >= TRIAL_MAX_CONCURRENT_PER_LEG:
            return refuse("trial_leg_slots_full")
        capacities = _capacities(
            pool_base=shared.pool_base,
            committed=shared.committed,
            active_committed=shared.active_committed,
            deployment_base=leg.deployment_base,
            deployment_reserved=leg.deployment_reserved,
            equity=shared.equity,
            total_invested=shared.total_invested,
            available_cash=shared.available_cash,
            pending_total=shared.pending_total,
            pending_instrument=Decimal("0"),
            current_instrument=Decimal("0"),
            max_portfolio_exposure_pct=leg.max_portfolio_exposure_pct,
            max_instrument_exposure_pct=leg.max_instrument_exposure_pct,
            mandate_cash_reserve_pct=shared.mandate_cash_reserve_pct,
            mandate_active_risk_budget_pct=shared.mandate_active_risk_budget_pct,
            mandate_max_loss_per_position_pct=shared.mandate_max_loss_per_position_pct,
            stop_loss_pct=stop_pct,
        )
        if isinstance(capacities, str):
            return refuse(capacities)
        per_leg = (
            ("max_ticket_amount", leg.max_ticket_amount),
            ("deployment_remaining", capacities.deployment_remaining),
            ("instrument_exposure", capacities.instrument),
            ("loss_at_stop", capacities.loss_at_stop),
        )
        for name, room in per_leg:
            if _whole_cents(room) < ticket:
                return refuse(name)
        for name, room in (
            ("pool_remaining", capacities.pool_remaining),
            ("available_cash", capacities.cash),
            ("portfolio_exposure", capacities.portfolio),
            ("active_risk", capacities.active_risk),
            ("cash_reserve", capacities.cash_reserve),
        ):
            shared_room[name] = min(shared_room.get(name, room), room)
    need = ticket * len(legs)
    for name, room in shared_room.items():
        if _whole_cents(room) < need:
            return refuse(name)
    return None


_LEGS_SQL = """
    SELECT d.strategy_id, d.strategy_version, d.capital_limit, d.enabled,
           p.max_ticket_amount, p.max_portfolio_exposure_pct, p.max_instrument_exposure_pct,
           p.max_drawdown_pct,
           (
               SELECT COALESCE(SUM(fd.amount), 0)
               FROM strategy_funding_decisions fd
               LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
               WHERE fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
                 AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
           ) AS reserved,
           (
               -- ai_trial_executor._open_leg_trades' population, counted in this transaction.
               SELECT count(*)
               FROM strategy_funding_decisions fd
               LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
               WHERE fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
                 AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
           ) AS open_trades
    FROM ai_trial_declarations dcl
    JOIN strategy_deployments d
      ON d.strategy_id IN (dcl.strategy_id, dcl.strategy_id || '-control')
     AND d.strategy_version = dcl.strategy_version AND d.mode = 'paper'
    LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
    WHERE dcl.declaration_id = %s
"""

_POOL_SQL = """
    SELECT enabled, max_portfolio_drawdown_pct, max_loss_per_position_pct, max_daily_loss_pct,
           active_risk_budget_pct, cash_reserve_pct, max_concurrent_positions
    FROM strategy_paper_pool_events
    ORDER BY strategy_paper_pool_event_id DESC
    LIMIT 1
"""


def preview_trial_capacity(
    conn: psycopg.Connection[Any],
    *,
    declaration_id: int,
    risk: BrokerAccountRiskSnapshot,
    now: datetime,
) -> str | None:
    """Read the step-0 inputs (read-only, one transaction) and return ``start_gate_reason``.

    Requires an idle connection, as the executors do, so the transaction it opens and closes
    is its own and never a caller's.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("the trial capacity preview requires an idle connection")
    with conn.transaction():
        return _preview(conn, declaration_id=declaration_id, risk=risk, now=now)


def _preview(
    conn: psycopg.Connection[Any],
    *,
    declaration_id: int,
    risk: BrokerAccountRiskSnapshot,
    now: datetime,
) -> str | None:
    pool = conn.execute(_POOL_SQL).fetchone()
    if pool is None or not bool(pool[0]) or any(value is None for value in pool[1:]):
        return REFUSAL_PREFIX + "paper_pool_unavailable"
    # The same exception boundary as `_risk_and_amount`: a shared-capital observation that
    # refuses is a named refusal here, never an exception that aborts the caller.
    try:
        authority = load_engine_capital_authority(conn)
        if authority is None or not authority.enabled:
            return REFUSAL_PREFIX + SANDBOX_EXCEEDED
        core_mandate = load_core_mandate(conn)
        usage = resolve_engine_capital_usage(
            authority,
            risk,
            core_instrument_id=None if core_mandate is None else core_mandate.core_instrument_id,
        )
    except EngineCapitalObservationError as exc:
        return REFUSAL_PREFIX + exc.reason_code
    pending = conn.execute(_PENDING_RISK_SQL, (None,)).fetchone()
    market_date = now.astimezone(_NY).date()
    day_start = datetime.combine(market_date, time.min, tzinfo=_NY)
    mandate_row = conn.execute(
        _MANDATE_OBSERVATION_SQL, (day_start.astimezone(UTC), (day_start + timedelta(days=1)).astimezone(UTC))
    ).fetchone()
    high_water_row = conn.execute(
        "SELECT equity_high_water FROM strategy_paper_account_risk_state WHERE id = true"
    ).fetchone()
    if pending is None or mandate_row is None:  # pragma: no cover - aggregate SELECTs always return a row
        raise StrategyPaperExecutionError("pending or mandate observation was unavailable")
    high_water = max(Decimal(str(high_water_row[0])) if high_water_row else risk.equity, risk.equity)
    drawdown = (high_water - risk.equity) / high_water * Decimal("100") if high_water > 0 else Decimal("100")

    realised = load_paper_realised_pnl(conn)
    if realised is None:
        return REFUSAL_PREFIX + "realised_pnl_incomplete"
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_LEGS_SQL, (declaration_id,))
        rows = cur.fetchall()
    if len({row["strategy_id"] for row in rows}) != 2 or len(rows) != 2:
        return REFUSAL_PREFIX + "paper_deployment_missing"
    legs: list[PreviewLeg] = []
    for row in rows:
        if not bool(row["enabled"]):
            return REFUSAL_PREFIX + "paper_deployment_disabled"
        if row["max_ticket_amount"] is None:
            return REFUSAL_PREFIX + "execution_policy_missing"
        legs.append(
            PreviewLeg(
                # §8 caps: the leg's base is fixed-mode whatever the pool's mode (the loader's
                # TRIAL_CAPITAL_MODE), exactly as the executor sizes it.
                deployment_base=sandbox_bound(
                    capital_limit=Decimal(str(row["capital_limit"])),
                    capital_mode=TRIAL_CAPITAL_MODE,
                    realised_delta=realised.get((row["strategy_id"], row["strategy_version"]), Decimal("0")),
                ),
                deployment_reserved=Decimal(str(row["reserved"])),
                open_trades=int(row["open_trades"]),
                max_ticket_amount=Decimal(str(row["max_ticket_amount"])),
                max_portfolio_exposure_pct=Decimal(str(row["max_portfolio_exposure_pct"])),
                max_instrument_exposure_pct=Decimal(str(row["max_instrument_exposure_pct"])),
                max_drawdown_pct=Decimal(str(row["max_drawdown_pct"])),
            )
        )
    shared = PreviewShared(
        within_bound=usage.headroom.within_bound,
        pool_base=usage.headroom.bound,
        committed=usage.committed,
        active_committed=authority.alpha_committed,
        equity=risk.equity,
        total_invested=risk.total_invested,
        available_cash=risk.available_cash,
        pending_total=Decimal(str(pending[0])),
        drawdown_pct=drawdown,
        open_lifecycles=int(mandate_row[0]),
        daily_realised_pnl=Decimal(str(mandate_row[1])),
        mandate_max_drawdown_pct=Decimal(str(pool[1])),
        mandate_max_loss_per_position_pct=Decimal(str(pool[2])),
        mandate_max_daily_loss_pct=Decimal(str(pool[3])),
        mandate_active_risk_budget_pct=Decimal(str(pool[4])),
        mandate_cash_reserve_pct=Decimal(str(pool[5])),
        mandate_max_concurrent_positions=int(pool[6]),
    )
    return start_gate_reason(shared, legs)


__all__ = [
    "PREVIEW_STOP_PCT",
    "PREVIEW_TICKET",
    "REFUSAL_PREFIX",
    "PreviewLeg",
    "PreviewShared",
    "preview_trial_capacity",
    "start_gate_reason",
]
