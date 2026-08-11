"""Portfolio stress mechanics for the frozen C-2 extreme-shock candidate.

This module does not discover signals.  It accepts the already-frozen trade paths
and answers the different question the original per-trade study could not answer:
what happens when clustered signals compete for one unleveraged capital pot?

The implementation is deliberately pure and has no persistence side effects.  The
historical interval was searched, so its output is development/stress evidence only.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import Final

UNKNOWN_SECTOR: Final = "__unknown__"


@dataclass(frozen=True)
class ShockTradePath:
    """One frozen signal and its causal marked path, before costs."""

    trade_id: str
    series_id: int
    entry_date: date
    exit_date: date
    sector: str | None
    # End-of-session cumulative gross return on the short notional.  The final
    # item is the executable stop/timeout return and must be on ``exit_date``.
    cumulative_returns: tuple[tuple[date, float], ...]

    def __post_init__(self) -> None:
        if not self.trade_id:
            raise ValueError("trade_id must not be blank")
        if not self.cumulative_returns:
            raise ValueError("cumulative_returns must not be empty")
        dates = tuple(item[0] for item in self.cumulative_returns)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("path dates must be unique and increasing")
        if dates[0] != self.entry_date or dates[-1] != self.exit_date:
            raise ValueError("path must span entry_date through exit_date")
        if any(not isfinite(item[1]) for item in self.cumulative_returns):
            raise ValueError("path returns must be finite")


@dataclass(frozen=True)
class PortfolioStressConfig:
    """Frozen capital/risk policy; fractions are proportions of current equity."""

    per_name_cap: float
    gross_cap: float = 1.0
    sector_cap: float | None = 0.25
    round_trip_cost: float = 0.005
    carry_cost: float = 0.00574

    def __post_init__(self) -> None:
        if not 0 < self.per_name_cap <= 1:
            raise ValueError("per_name_cap must be in (0, 1]")
        if not 0 < self.gross_cap <= 1:
            raise ValueError("gross_cap must be in (0, 1]; C-2 does not permit leverage")
        if self.sector_cap is not None and not 0 < self.sector_cap <= self.gross_cap:
            raise ValueError("sector_cap must be in (0, gross_cap] or None")
        if self.round_trip_cost < 0 or self.carry_cost < 0:
            raise ValueError("costs must not be negative")


@dataclass(frozen=True)
class PortfolioStressResult:
    per_name_cap: float
    sector_cap: float | None
    start_date: date
    end_date: date
    candidate_trades: int
    funded_trades: int
    unfunded_trades: int
    ending_return: float
    annualized_return: float
    max_drawdown: float
    worst_day: float
    expected_shortfall_5: float
    max_concurrent: int
    max_gross_exposure: float
    max_sector_exposure: float | None
    unknown_sector_funded_pct: float
    capital_weighted_trade_return: float
    annual_returns: tuple[tuple[int, float], ...]
    stress_date: date | None
    one_name_loss_stressed_max_drawdown: float
    one_name_loss_stressed_ending_return: float


@dataclass
class _Position:
    trade: ShockTradePath
    notional: float
    previous_net_return: float = 0.0


def _maximum_drawdown(equities: list[float]) -> float:
    peak = equities[0]
    worst = 0.0
    for value in equities:
        peak = max(peak, value)
        if peak > 0:
            worst = min(worst, value / peak - 1.0)
    return worst


def _annualized_return(start: date, end: date, ending_equity: float) -> float:
    years = max((end - start).days / 365.25, 1 / 365.25)
    if ending_equity <= 0:
        return -1.0
    return ending_equity ** (1.0 / years) - 1.0


def _allocate_batch(
    candidates: list[ShockTradePath],
    *,
    equity: float,
    active: dict[str, _Position],
    config: PortfolioStressConfig,
) -> dict[str, float]:
    """Allocate one entry batch symmetrically, independent of input order.

    All candidates have the same frozen signal and no causal rank score.  Equal
    pro-rata allocation is therefore the only non-invented tie-break.  Unknown
    sectors share one conservative concentration bucket.
    """

    if equity <= 0 or not candidates:
        return {}
    existing_gross = sum(position.notional for position in active.values())
    global_room = max(0.0, equity * config.gross_cap - existing_gross)
    if global_room <= 0:
        return {}

    sector_used: dict[str, float] = defaultdict(float)
    for position in active.values():
        sector_used[position.trade.sector or UNKNOWN_SECTOR] += position.notional
    counts = Counter(candidate.sector or UNKNOWN_SECTOR for candidate in candidates)
    equal_global = global_room / len(candidates)
    per_name = equity * config.per_name_cap
    allocations: dict[str, float] = {}
    for candidate in sorted(candidates, key=lambda item: item.trade_id):
        amount = min(per_name, equal_global)
        if config.sector_cap is not None:
            key = candidate.sector or UNKNOWN_SECTOR
            sector_room = max(0.0, equity * config.sector_cap - sector_used[key])
            amount = min(amount, sector_room / counts[key])
        if amount > 0:
            allocations[candidate.trade_id] = amount
    return allocations


def _net_mark(trade: ShockTradePath, index: int, config: PortfolioStressConfig) -> float:
    gross = trade.cumulative_returns[index][1]
    steps = len(trade.cumulative_returns)
    elapsed_fraction = (index + 1) / steps
    cost = config.round_trip_cost / 2 + config.carry_cost * elapsed_fraction
    if index == steps - 1:
        cost += config.round_trip_cost / 2
    return gross - cost


def simulate_extreme_shock_portfolio(
    trades: list[ShockTradePath],
    config: PortfolioStressConfig,
) -> PortfolioStressResult:
    """Simulate one unleveraged, compounding pot and a simultaneous -100% shock."""

    if not trades:
        raise ValueError("at least one trade is required")
    ids = [trade.trade_id for trade in trades]
    if len(ids) != len(set(ids)):
        raise ValueError("trade_id values must be unique")

    entries: dict[date, list[ShockTradePath]] = defaultdict(list)
    marks: dict[date, list[tuple[ShockTradePath, int]]] = defaultdict(list)
    for trade in trades:
        entries[trade.entry_date].append(trade)
        for index, (mark_date, _) in enumerate(trade.cumulative_returns):
            marks[mark_date].append((trade, index))
    calendar = sorted(set(entries) | set(marks))

    equity = 1.0
    active: dict[str, _Position] = {}
    equities = [equity]
    daily_returns: list[float] = []
    gross_exposures: list[float] = []
    concurrent: list[int] = []
    max_sector_exposure: float | None = 0.0 if config.sector_cap is not None else None
    funded = 0
    unknown_funded = 0
    funded_notional = 0.0
    terminal_pnl = 0.0
    peak_gross_date: date | None = None
    peak_gross_ratio = -1.0
    peak_position_notional = 0.0

    for current_date in calendar:
        allocations = _allocate_batch(entries.get(current_date, []), equity=equity, active=active, config=config)
        for trade in entries.get(current_date, []):
            notional = allocations.get(trade.trade_id)
            if notional is None:
                continue
            active[trade.trade_id] = _Position(trade=trade, notional=notional)
            funded += 1
            funded_notional += notional
            unknown_funded += int(trade.sector is None)

        gross = sum(position.notional for position in active.values())
        gross_ratio = gross / equity if equity > 0 else float("inf")
        gross_exposures.append(gross_ratio)
        concurrent.append(len(active))
        if gross_ratio > peak_gross_ratio:
            peak_gross_date = current_date
            peak_gross_ratio = gross_ratio
            peak_position_notional = max((position.notional for position in active.values()), default=0.0)

        if max_sector_exposure is not None and equity > 0:
            by_sector: dict[str, float] = defaultdict(float)
            for position in active.values():
                by_sector[position.trade.sector or UNKNOWN_SECTOR] += position.notional
            max_sector_exposure = max(
                max_sector_exposure,
                max((value / equity for value in by_sector.values()), default=0.0),
            )

        prior_equity = equity
        current_marks = sorted(marks.get(current_date, []), key=lambda item: item[0].trade_id)
        for trade, index in current_marks:
            position = active.get(trade.trade_id)
            if position is None:
                continue
            net = _net_mark(trade, index, config)
            equity += position.notional * (net - position.previous_net_return)
            position.previous_net_return = net
        daily_returns.append(equity / prior_equity - 1.0 if prior_equity > 0 else -1.0)
        equities.append(equity)

        for trade, index in current_marks:
            if index == len(trade.cumulative_returns) - 1:
                position = active.pop(trade.trade_id, None)
                if position is not None:
                    terminal_pnl += position.notional * position.previous_net_return

    sorted_days = sorted(daily_returns)
    tail_count = max(1, int(len(sorted_days) * 0.05))
    observed_drawdown = _maximum_drawdown(equities)

    # Structural jump test: on the most crowded day, one largest allocation loses
    # its full notional in addition to the observed path.  This is not a probability
    # forecast; it asks whether the sizing policy survives the declared scenario.
    stress_equities = list(equities)
    if peak_gross_date is not None and peak_position_notional > 0:
        stress_index = calendar.index(peak_gross_date) + 1
        for index in range(stress_index, len(stress_equities)):
            stress_equities[index] -= peak_position_notional
    stressed_ending = stress_equities[-1]
    returns_by_year: dict[int, float] = defaultdict(lambda: 1.0)
    for current_date, daily_return in zip(calendar, daily_returns, strict=True):
        returns_by_year[current_date.year] *= 1.0 + daily_return

    return PortfolioStressResult(
        per_name_cap=config.per_name_cap,
        sector_cap=config.sector_cap,
        start_date=calendar[0],
        end_date=calendar[-1],
        candidate_trades=len(trades),
        funded_trades=funded,
        unfunded_trades=len(trades) - funded,
        ending_return=equity - 1.0,
        annualized_return=_annualized_return(calendar[0], calendar[-1], equity),
        max_drawdown=observed_drawdown,
        worst_day=min(daily_returns),
        expected_shortfall_5=sum(sorted_days[:tail_count]) / tail_count,
        max_concurrent=max(concurrent),
        max_gross_exposure=max(gross_exposures),
        max_sector_exposure=max_sector_exposure,
        unknown_sector_funded_pct=unknown_funded / funded if funded else 0.0,
        capital_weighted_trade_return=terminal_pnl / funded_notional if funded_notional else 0.0,
        annual_returns=tuple((year, growth - 1.0) for year, growth in sorted(returns_by_year.items())),
        stress_date=peak_gross_date,
        one_name_loss_stressed_max_drawdown=_maximum_drawdown(stress_equities),
        one_name_loss_stressed_ending_return=stressed_ending - 1.0,
    )


__all__ = [
    "PortfolioStressConfig",
    "PortfolioStressResult",
    "ShockTradePath",
    "simulate_extreme_shock_portfolio",
]
