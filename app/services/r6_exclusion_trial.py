"""Frozen measurement mechanics for preregistered R6 arm #2908."""

from __future__ import annotations

import csv
import math
import statistics
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Final, Literal

from app.services.r6_dilution_exclusion import NsiInput, assign_nsi_portfolios, exclusions
from app.services.r6_pit_bundle import R6PitBundle

HALF_SPREAD: Final = 0.00725
WINDOW_END: Final = date(2024, 9, 27)
REFERENCE_MEMBER: Final = "inv_monthly_2025/portf_nsi_monthly_2025.csv"
TerminationCase = Literal["best", "worst"]


@dataclass(frozen=True)
class PriceBar:
    day: date
    adjusted_open: float
    adjusted_close: float


@dataclass(frozen=True)
class PriceSeries:
    symbol: str
    bars: tuple[PriceBar, ...]
    invalid_rows: int

    @property
    def by_date(self) -> dict[date, PriceBar]:
        return {bar.day: bar for bar in self.bars}


@dataclass(frozen=True)
class RebalanceEvent:
    day: date
    pre_cost_wealth: float
    traded_notional: float
    spread_cost: float
    target_count: int
    censored_holdings: int


@dataclass(frozen=True)
class PortfolioResult:
    total_return: float
    events: tuple[RebalanceEvent, ...]

    @property
    def traded_notional_over_initial_capital(self) -> float:
        return sum(event.traded_notional for event in self.events)

    @property
    def spread_cost_over_initial_capital(self) -> float:
        return sum(event.spread_cost for event in self.events)


@dataclass(frozen=True)
class FactorValidation:
    months: int
    window_start: str
    window_end: str
    correlation: float
    alpha: float
    beta: float
    lag_correlation: float
    lead_correlation: float
    passed: bool


def read_price_series(path: Path) -> PriceSeries:
    bars: list[PriceBar] = []
    invalid = 0
    with path.open(encoding="utf-8", errors="strict", newline="") as handle:
        for row in csv.reader(handle):
            if len(row) != 9:
                invalid += 1
                continue
            try:
                day = date.fromisoformat(row[0])
                raw_open = float(row[1])
                raw_close = float(row[4])
                adjusted_close = float(row[8])
            except ValueError:
                invalid += 1
                continue
            if not all(math.isfinite(value) and value > 0 for value in (raw_open, raw_close, adjusted_close)):
                invalid += 1
                continue
            bars.append(
                PriceBar(
                    day=day,
                    adjusted_open=raw_open * adjusted_close / raw_close,
                    adjusted_close=adjusted_close,
                )
            )
    days = [bar.day for bar in bars]
    if days != sorted(set(days)):
        raise RuntimeError(f"valid price bars are duplicate or unordered: {path}")
    if not bars:
        raise RuntimeError(f"price series has no valid bars: {path}")
    return PriceSeries(symbol=path.stem.upper(), bars=tuple(bars), invalid_rows=invalid)


def load_required_prices(bundle: R6PitBundle, price_dir: Path) -> dict[str, PriceSeries]:
    symbols = sorted({row.symbol for row in bundle.records})
    return {symbol: read_price_series(price_dir / f"{symbol}.csv") for symbol in symbols}


def signal_sets(bundle: R6PitBundle, prices: dict[str, PriceSeries]) -> dict[datetime, dict[str, frozenset[str]]]:
    result: dict[datetime, dict[str, frozenset[str]]] = {}
    for formation in sorted({row.formation_close for row in bundle.records}):
        inputs = tuple(
            NsiInput(
                symbol=row.symbol,
                exchange=row.exchange,
                current_shares=row.current_shares,
                prior_shares=row.prior_shares,
                red_flag_scores=row.red_flag_scores,
                red_flag_history_complete=row.red_flag_history_complete,
            )
            for row in bundle.records_at(formation)
        )
        portfolios = assign_nsi_portfolios(inputs, nyse_exchange_names=frozenset({"NYSE"}))
        excluded = exclusions(inputs, portfolios)
        executable = frozenset(
            row.symbol
            for row in inputs
            if any(
                bar.day > formation.date() and (bar.day - formation.date()).days <= 7 for bar in prices[row.symbol].bars
            )
        )
        result[formation] = {
            "full": executable,
            **{name: executable - symbols for name, symbols in excluded.items()},
            "dilution_excluded": excluded["dilution"] & executable,
            "red_flag_excluded": excluded["red_flag"] & executable,
            "union_excluded": excluded["union"] & executable,
        }
    return result


def _execution_day(formation: datetime, members: frozenset[str], prices: dict[str, PriceSeries]) -> date:
    observed: set[date] = set()
    for symbol in members:
        later = [bar.day for bar in prices[symbol].bars if bar.day > formation.date()]
        if not later or (later[0] - formation.date()).days > 7:
            raise RuntimeError(f"target {symbol} has no admissible post-formation fill")
        observed.add(later[0])
    if len(observed) != 1:
        raise RuntimeError(f"formation {formation.isoformat()} has asynchronous entry sessions: {sorted(observed)}")
    return next(iter(observed))


def _event_value(series: PriceSeries, day: date, *, field: Literal["open", "close"], case: TerminationCase) -> float:
    exact = next((bar for bar in series.bars if bar.day == day), None)
    if exact is not None:
        return exact.adjusted_open if field == "open" else exact.adjusted_close
    prior = [bar for bar in series.bars if bar.day < day]
    if prior:
        # A halted holding is no more executable than a terminated one on the
        # rebalance session. Bound it instead of inventing an in-session fill:
        # stale last close in the best case, zero in the governing worst case.
        return prior[-1].adjusted_close if case == "best" else 0.0
    raise RuntimeError(f"{series.symbol} has no price at or before required session {day}")


def _target_value(
    pre_cost_wealth: float,
    current: dict[str, float],
    target: frozenset[str],
    half_spread: float,
) -> float:
    if not target:
        raise RuntimeError("portfolio target is empty")

    def residual(value: float) -> float:
        traded = sum(abs(value - current.get(symbol, 0.0)) for symbol in target)
        traded += sum(amount for symbol, amount in current.items() if symbol not in target)
        return len(target) * value + half_spread * traded - pre_cost_wealth

    lower = 0.0
    upper = pre_cost_wealth / len(target)
    for _ in range(100):
        middle = (lower + upper) / 2
        if residual(middle) > 0:
            upper = middle
        else:
            lower = middle
    return (lower + upper) / 2


def simulate_portfolio(
    *,
    schedule: tuple[tuple[datetime, frozenset[str]], ...],
    prices: dict[str, PriceSeries],
    case: TerminationCase,
    half_spread: float,
    window_end: date = WINDOW_END,
) -> PortfolioResult:
    holdings: dict[str, float] = {}
    cash = 1.0
    events: list[RebalanceEvent] = []
    for formation, target in schedule:
        day = _execution_day(formation, target, prices)
        current = {
            symbol: shares * _event_value(prices[symbol], day, field="open", case=case)
            for symbol, shares in holdings.items()
        }
        censored = sum(day not in prices[symbol].by_date for symbol in holdings)
        pre_cost = cash + sum(current.values())
        target_value = _target_value(pre_cost, current, target, half_spread)
        traded = sum(abs(target_value - current.get(symbol, 0.0)) for symbol in target)
        traded += sum(amount for symbol, amount in current.items() if symbol not in target)
        cost = half_spread * traded
        if not math.isclose(len(target) * target_value + cost, pre_cost, rel_tol=1e-10, abs_tol=1e-12):
            raise RuntimeError("rebalance cash conservation failed")
        target_prices = {symbol: _event_value(prices[symbol], day, field="open", case=case) for symbol in target}
        holdings = {symbol: target_value / target_prices[symbol] for symbol in target}
        cash = 0.0
        events.append(RebalanceEvent(day, pre_cost, traded, cost, len(target), censored))

    final_mid = sum(
        shares * _event_value(prices[symbol], window_end, field="close", case=case)
        for symbol, shares in holdings.items()
    )
    final_traded = final_mid
    final_cost = half_spread * final_traded
    final_censored = sum(window_end not in prices[symbol].by_date for symbol in holdings)
    events.append(RebalanceEvent(window_end, final_mid, final_traded, final_cost, 0, final_censored))
    return PortfolioResult(total_return=final_mid - final_cost - 1.0, events=tuple(events))


def month_pairs(start: tuple[int, int], end: tuple[int, int]) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    year, month = start
    while (year, month) <= end:
        result.append((year, month))
        month += 1
        if month == 13:
            year += 1
            month = 1
    return tuple(result)


def _last_bar_on_or_before(series: PriceSeries, cutoff: date) -> PriceBar | None:
    eligible = [bar for bar in series.bars if bar.day <= cutoff]
    return eligible[-1] if eligible else None


def _monthly_return(series: PriceSeries, start_session: date, end_session: date) -> float | None:
    start = _last_bar_on_or_before(series, start_session)
    end = _last_bar_on_or_before(series, end_session)
    if start is None or end is None or end.day <= start.day:
        return None
    if series.bars[-1].day < end_session:
        return -1.0
    return end.adjusted_close / start.adjusted_close - 1.0


def constructed_nsi_factor(
    bundle: R6PitBundle,
    prices: dict[str, PriceSeries],
    *,
    calendar_symbol: str = "A",
) -> dict[tuple[int, int], float]:
    calendar = prices[calendar_symbol]
    formations = sorted({row.formation_close for row in bundle.records})
    assignments: dict[datetime, tuple[frozenset[str], frozenset[str]]] = {}
    for formation in formations:
        rows = tuple(
            NsiInput(
                row.symbol,
                row.exchange,
                row.current_shares,
                row.prior_shares,
                row.red_flag_scores,
                row.red_flag_history_complete,
            )
            for row in bundle.records_at(formation)
        )
        ranked = assign_nsi_portfolios(rows, nyse_exchange_names=frozenset({"NYSE"}))
        assignments[formation] = (
            frozenset(symbol for symbol, rank in ranked.items() if rank == 1),
            frozenset(symbol for symbol, rank in ranked.items() if rank == 10),
        )

    sessions_by_month: dict[tuple[int, int], list[date]] = defaultdict(list)
    for bar in calendar.bars:
        if date(2022, 6, 1) <= bar.day <= WINDOW_END:
            sessions_by_month[(bar.day.year, bar.day.month)].append(bar.day)
    output: dict[tuple[int, int], float] = {}
    for year, month in month_pairs((2022, 7), (2024, 9)):
        formation = max(value for value in formations if value.date() < date(year, month, 28))
        low, high = assignments[formation]
        prior_month = (year - 1, 12) if month == 1 else (year, month - 1)
        start_session = sessions_by_month[prior_month][-1]
        end_session = sessions_by_month[(year, month)][-1]
        low_returns = [
            value
            for symbol in low
            if (value := _monthly_return(prices[symbol], start_session, end_session)) is not None
        ]
        high_returns = [
            value
            for symbol in high
            if (value := _monthly_return(prices[symbol], start_session, end_session)) is not None
        ]
        if not low_returns or not high_returns:
            raise RuntimeError(f"empty Nsi factor leg for {year:04d}-{month:02d}")
        output[(year, month)] = statistics.fmean(high_returns) - statistics.fmean(low_returns)
    return output


def read_global_q_nsi(path: Path) -> dict[tuple[int, int], float]:
    legs: dict[tuple[int, int], dict[int, float]] = defaultdict(dict)
    with zipfile.ZipFile(path) as archive, archive.open(REFERENCE_MEMBER) as raw:
        import io

        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
        if reader.fieldnames != ["year", "month", "rank_NSI", "nstocks", "ret_vw"]:
            raise RuntimeError(f"unexpected global-q Nsi header: {reader.fieldnames}")
        for row in reader:
            key = (int(row["year"]), int(row["month"]))
            rank = int(row["rank_NSI"])
            value = float(row["ret_vw"]) / 100.0
            if rank in legs[key]:
                raise RuntimeError(f"duplicate global-q Nsi rank {rank} at {key}")
            legs[key][rank] = value
    return {key: values[10] - values[1] for key, values in legs.items() if 1 in values and 10 in values}


def _pearson(left: list[float], right: list[float]) -> float:
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    denominator = math.sqrt(sum((x - left_mean) ** 2 for x in left) * sum((y - right_mean) ** 2 for y in right))
    if denominator == 0:
        raise RuntimeError("factor correlation has zero variance")
    return numerator / denominator


def validate_factor(ours: dict[tuple[int, int], float], reference: dict[tuple[int, int], float]) -> FactorValidation:
    keys = sorted(set(ours) & set(reference))
    if len(keys) < 24:
        raise RuntimeError(f"factor validation needs at least 24 overlapping months, got {len(keys)}")
    left = [ours[key] for key in keys]
    right = [reference[key] for key in keys]
    correlation = _pearson(left, right)
    right_mean = statistics.fmean(right)
    left_mean = statistics.fmean(left)
    variance = sum((value - right_mean) ** 2 for value in right)
    beta = sum((x - right_mean) * (y - left_mean) for x, y in zip(right, left, strict=True)) / variance
    alpha = left_mean - beta * right_mean
    lag = _pearson(left[1:], right[:-1])
    lead = _pearson(left[:-1], right[1:])
    passed = correlation >= 0.20 and beta > 0 and abs(correlation) >= max(abs(lag), abs(lead))
    return FactorValidation(
        months=len(keys),
        window_start=f"{keys[0][0]:04d}-{keys[0][1]:02d}",
        window_end=f"{keys[-1][0]:04d}-{keys[-1][1]:02d}",
        correlation=correlation,
        alpha=alpha,
        beta=beta,
        lag_correlation=lag,
        lead_correlation=lead,
        passed=passed,
    )


def haircut_net_return(
    *,
    strategy_gross: float,
    strategy_net: float,
    buy_hold_gross: float,
    haircut: float,
) -> float:
    edge = strategy_gross - buy_hold_gross
    adjusted_edge = edge if edge <= 0 else edge * (1 - haircut)
    full_strategy_cost_drag = strategy_gross - strategy_net
    return buy_hold_gross + adjusted_edge - full_strategy_cost_drag


__all__ = [
    "HALF_SPREAD",
    "WINDOW_END",
    "FactorValidation",
    "PortfolioResult",
    "constructed_nsi_factor",
    "haircut_net_return",
    "load_required_prices",
    "read_global_q_nsi",
    "read_price_series",
    "signal_sets",
    "simulate_portfolio",
    "validate_factor",
]
