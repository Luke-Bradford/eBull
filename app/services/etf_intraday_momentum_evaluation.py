"""Read-only retained-data census for the frozen ETF intraday trial (#2502).

This is deliberately not a backtest runner or a catalogue publisher.  It reads
the bars already retained for the exact active-universe members, applies the
pre-registered candidate unchanged, and reports gross candle-proxy evidence.
No row is written and no result from here can make a strategy allocatable.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, time, timedelta
from decimal import Decimal
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg

from app.services.block_bootstrap import BootstrapResult, block_bootstrap_expectancy, cluster_by_date
from app.services.etf_intraday_momentum_candidate import (
    CANDIDATE_VERSION,
    MIN_COMPLETE_PRIMARY_SESSIONS,
    PRIMARY_SYMBOL,
    ROBUSTNESS_SYMBOLS,
    CandidateRefusal,
    is_eligible_source,
    opening_signal,
    resolve_gross_feasibility,
)
from app.services.market_calendar import us_market_status
from app.services.strategy_observation_storage import IntradayBar

_NY: Final = ZoneInfo("America/New_York")
_SYMBOLS: Final = (PRIMARY_SYMBOL, *ROBUSTNESS_SYMBOLS)
_BOOTSTRAP_SEED: Final = 2502


@dataclass(frozen=True)
class ReturnSummary:
    observations: int
    expectancy_pct: float | None
    hit_rate_pct: float | None
    profit_factor: float | None
    worst_return_pct: float | None
    expected_shortfall_5_pct: float | None
    bootstrap: BootstrapResult | None


@dataclass(frozen=True)
class SymbolEvaluation:
    symbol: str
    first_session: date | None
    last_session: date | None
    candidate_sessions: int
    complete_outcomes: int
    fired_long: int
    fired_cadence_pct: float | None
    refusals: tuple[tuple[str, int], ...]
    long_only: ReturnSummary
    always_long: ReturnSummary
    published_signed: ReturnSummary


@dataclass(frozen=True)
class CandidateEvaluation:
    candidate_version: str
    universe_version: str
    symbols: tuple[SymbolEvaluation, ...]
    promotion_refusals: tuple[str, ...]

    @property
    def primary(self) -> SymbolEvaluation:
        return next(item for item in self.symbols if item.symbol == PRIMARY_SYMBOL)


def _full_sessions(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        if us_market_status(current) == "open":
            yield current
        current += timedelta(days=1)


def _summarise(returns: Sequence[float], dates: Sequence[date], *, seed: int) -> ReturnSummary:
    if not returns:
        return ReturnSummary(0, None, None, None, None, None, None)
    positives = sum(value for value in returns if value > 0.0)
    losses = abs(sum(value for value in returns if value < 0.0))
    tail_size = max(1, math.ceil(len(returns) * 0.05))
    bootstrap = block_bootstrap_expectancy(cluster_by_date(returns, dates), seed=seed)
    return ReturnSummary(
        observations=len(returns),
        expectancy_pct=sum(returns) / len(returns),
        hit_rate_pct=100.0 * sum(value > 0.0 for value in returns) / len(returns),
        profit_factor=positives / losses if losses > 0.0 else None,
        worst_return_pct=min(returns),
        expected_shortfall_5_pct=sum(sorted(returns)[:tail_size]) / tail_size,
        bootstrap=bootstrap,
    )


def evaluate_symbol(symbol: str, bars: Sequence[IntradayBar]) -> SymbolEvaluation:
    """Evaluate one symbol without inspecting or mutating any external state."""
    normalised = symbol.strip().upper()
    if normalised not in _SYMBOLS:
        raise ValueError(f"symbol {normalised!r} is outside the frozen candidate")
    relevant = [bar for bar in bars if bar.timeframe == "30m" and is_eligible_source(bar.source)]
    if not relevant:
        empty = _summarise([], [], seed=0)
        return SymbolEvaluation(normalised, None, None, 0, 0, 0, None, (), empty, empty, empty)

    by_point: dict[tuple[date, time], IntradayBar] = {}
    local_dates: list[date] = []
    for bar in relevant:
        local = bar.bar_time.astimezone(_NY)
        key = (local.date(), local.time().replace(tzinfo=None))
        if key in by_point:
            raise ValueError(f"duplicate retained {normalised} 30m bar at {key}")
        by_point[key] = bar
        local_dates.append(local.date())

    first, last = min(local_dates), max(local_dates)
    sessions = tuple(_full_sessions(first, last))
    refusals: Counter[str] = Counter()
    fired_returns: list[float] = []
    fired_dates: list[date] = []
    always_returns: list[float] = []
    complete_dates: list[date] = []
    signed_returns: list[float] = []

    for session in sessions:
        opening = by_point.get((session, time(9, 30)))
        final = by_point.get((session, time(15, 30)))
        if opening is None:
            refusals["missing_opening_bar"] += 1
            continue
        if final is None:
            refusals["missing_last_half_hour_bar"] += 1
            continue

        prior_date = session - timedelta(days=1)
        while prior_date >= first and us_market_status(prior_date) == "closed":
            prior_date -= timedelta(days=1)
        prior = by_point.get((prior_date, time(15, 30))) if prior_date >= first else None
        if prior is None:
            refusals["missing_prior_full_session_close"] += 1
            continue

        try:
            signal = opening_signal(symbol=normalised, prior_close_bar=prior, opening_bar=opening)
            outcome = resolve_gross_feasibility(signal, last_half_hour_bar=final)
        except CandidateRefusal:
            # Required-point absence has stable codes above. Any remaining
            # structural refusal is one operational class; exception prose is
            # deliberately not a dimension because it may contain row detail.
            refusals["candidate_structural_refusal"] += 1
            continue

        last_return = float((final.close / final.open - Decimal(1)) * Decimal(100))
        always_returns.append(last_return)
        complete_dates.append(session)
        signed_returns.append(last_return if signal.published_side == "long" else -last_return)
        if signal.adaptation_verdict == "fired_long":
            fired_returns.append(float(outcome.gross_return * Decimal(100)))
            fired_dates.append(session)

    complete = len(complete_dates)
    fired = len(fired_dates)
    return SymbolEvaluation(
        symbol=normalised,
        first_session=first,
        last_session=last,
        candidate_sessions=len(sessions),
        complete_outcomes=complete,
        fired_long=fired,
        fired_cadence_pct=100.0 * fired / complete if complete else None,
        refusals=tuple(sorted(refusals.items())),
        long_only=_summarise(fired_returns, fired_dates, seed=_BOOTSTRAP_SEED),
        always_long=_summarise(always_returns, complete_dates, seed=_BOOTSTRAP_SEED + 1),
        published_signed=_summarise(signed_returns, complete_dates, seed=_BOOTSTRAP_SEED + 2),
    )


def evaluate_candidate(
    universe_version: str, bars_by_symbol: Mapping[str, Sequence[IntradayBar]]
) -> CandidateEvaluation:
    symbols = tuple(evaluate_symbol(symbol, bars_by_symbol.get(symbol, ())) for symbol in _SYMBOLS)
    primary = symbols[0]
    refusals = [
        "historical_entry_exit_quotes_unavailable",
        "published_short_leg_not_executable",
        "prospective_outcome_interval_missing",
    ]
    if primary.complete_outcomes < MIN_COMPLETE_PRIMARY_SESSIONS:
        refusals.insert(0, "sample_immature")
    if primary.long_only.bootstrap is None:
        refusals.append("effective_sample_size_not_computed")
    return CandidateEvaluation(CANDIDATE_VERSION, universe_version, symbols, tuple(refusals))


def load_retained_bars(conn: psycopg.Connection[Any]) -> tuple[str, dict[str, tuple[IntradayBar, ...]]]:
    """Read exact resolved active-universe members; never fall back by symbol."""
    versions = conn.execute(
        """SELECT universe_version FROM strategy_intraday_universe_versions
           WHERE status = 'active' ORDER BY universe_version"""
    ).fetchall()
    if len(versions) != 1:
        raise RuntimeError(f"expected exactly one active intraday universe, found {len(versions)}")
    version = str(versions[0][0])
    rows = conn.execute(
        """
        WITH resolved AS (
            SELECT member.symbol,
                   array_agg(instrument.instrument_id ORDER BY instrument.instrument_id)
                       FILTER (WHERE instrument.instrument_id IS NOT NULL) AS instrument_ids
            FROM strategy_intraday_universe_members AS member
            LEFT JOIN instruments AS instrument
              ON instrument.symbol = member.symbol AND instrument.is_tradable = true
            WHERE member.universe_version = %(version)s
              AND member.timeframe = '30m'
              AND member.symbol = ANY(%(symbols)s)
            GROUP BY member.symbol
        )
        SELECT resolved.symbol, bar.bar_time, bar.instrument_id,
               bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source
        FROM resolved
        JOIN strategy_intraday_bars AS bar
          ON bar.instrument_id = resolved.instrument_ids[1]
         AND bar.timeframe = '30m'
         AND bar.source LIKE 'etoro/%%/nyse_rth'
        WHERE cardinality(resolved.instrument_ids) = 1
        ORDER BY resolved.symbol, bar.bar_time
        """,
        {"version": version, "symbols": list(_SYMBOLS)},
    ).fetchall()
    grouped: defaultdict[str, list[IntradayBar]] = defaultdict(list)
    for symbol, stamp, instrument_id, open_, high, low, close, volume, source in rows:
        grouped[str(symbol)].append(
            IntradayBar(
                "30m",
                stamp,
                int(instrument_id),
                Decimal(str(open_)),
                Decimal(str(high)),
                Decimal(str(low)),
                Decimal(str(close)),
                None if volume is None else Decimal(str(volume)),
                str(source),
            )
        )
    return version, {symbol: tuple(values) for symbol, values in grouped.items()}


def evaluate_retained_candidate(conn: psycopg.Connection[Any]) -> CandidateEvaluation:
    version, bars = load_retained_bars(conn)
    return evaluate_candidate(version, bars)


__all__ = [
    "CandidateEvaluation",
    "ReturnSummary",
    "SymbolEvaluation",
    "evaluate_candidate",
    "evaluate_retained_candidate",
    "evaluate_symbol",
    "load_retained_bars",
]
