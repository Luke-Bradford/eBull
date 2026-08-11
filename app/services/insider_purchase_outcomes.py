"""Sealed monthly portfolio outcomes for preregistered candidate #2480."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from random import Random
from statistics import mean, median
from typing import Any, Final

import psycopg

from app.services.block_bootstrap import BootstrapResult, block_bootstrap_expectancy, cluster_by_date
from app.services.cost_model import buy_price, half_spread_for, sell_price
from app.services.insider_purchase_candidate import PRIMARY_START, ClassifiedPurchase, InsiderClass
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.research_comparator_snapshot import SNAPSHOT_ID
from app.services.sector_classification import resolve_sector_spdr
from app.services.strategy_result import CORPUS_FROZEN_LAST_BAR, CORPUS_VENDORS

CONTROL_SEED: Final = 2480001
BOOTSTRAP_SEED: Final = 2480
MIN_ENTRY_PRICE: Final = Decimal("5")
MIN_MEDIAN_DOLLAR_VOLUME: Final = Decimal("10000000")


@dataclass(frozen=True)
class FirmMonthSignal:
    issuer_cik: str
    instrument_id: int
    insider_class: InsiderClass
    signal_year: int
    signal_month: int
    accession_numbers: tuple[str, ...]
    disclosed_value: Decimal
    latest_acceptance: datetime | None

    @property
    def signal_date(self) -> date:
        return date(self.signal_year, self.signal_month, 1)


@dataclass(frozen=True)
class FirmMonthWindow:
    signal: FirmMonthSignal
    series_id: int | None
    target_month_complete: bool
    entry_date: date | None
    entry_open: Decimal | None
    exit_date: date | None
    exit_close: Decimal | None
    holding_sessions: int
    holding_usable: bool | None
    prior_close: Decimal | None
    prior_close_usable: bool | None
    prior_sessions: int
    valid_liquidity_sessions: int
    median_dollar_volume: Decimal | None


@dataclass(frozen=True)
class FirmMonthOutcome:
    signal: FirmMonthSignal
    entry_date: date
    exit_date: date
    net_return_pct: float
    short_net_return_pct: float
    gross_return_pct: float
    weight_value: Decimal
    median_dollar_volume: Decimal
    market_relative_pct: float | None
    short_market_relative_pct: float | None
    sector_relative_pct: float | None
    short_sector_relative_pct: float | None
    sector_symbol: str | None


@dataclass(frozen=True)
class MonthlyPortfolioReturn:
    entry_date: date
    opportunistic_pct: float
    routine_pct: float
    spread_pct: float
    equal_weight_spread_pct: float
    market_relative_spread_pct: float | None
    sector_relative_spread_pct: float | None
    opportunistic_firms: int
    routine_firms: int
    unique_firms: int
    minimum_median_dollar_volume: Decimal
    maximum_single_firm_weight_pct: float


@dataclass(frozen=True)
class PortfolioEvaluation:
    firm_outcomes: tuple[FirmMonthOutcome, ...]
    monthly_returns: tuple[MonthlyPortfolioReturn, ...]
    bootstrap: BootstrapResult | None
    refusals: Mapping[str, int]


def build_firm_month_signals(classified: Sequence[ClassifiedPurchase]) -> tuple[FirmMonthSignal, ...]:
    grouped: dict[tuple[str, int, InsiderClass, int, int], list[ClassifiedPurchase]] = defaultdict(list)
    for item in classified:
        observation = item.observation
        if observation.instrument_id is None:
            continue
        key = (
            observation.issuer_cik,
            observation.instrument_id,
            item.insider_class,
            observation.filed_date.year,
            observation.filed_date.month,
        )
        grouped[key].append(item)

    signals: list[FirmMonthSignal] = []
    for (issuer_cik, instrument_id, insider_class, year, month), items in grouped.items():
        acceptances = [item.observation.accepted_at for item in items if item.observation.accepted_at is not None]
        signals.append(
            FirmMonthSignal(
                issuer_cik=issuer_cik,
                instrument_id=instrument_id,
                insider_class=insider_class,
                signal_year=year,
                signal_month=month,
                accession_numbers=tuple(sorted({item.observation.accession_number for item in items})),
                disclosed_value=sum(
                    (item.observation.disclosed_value for item in items),
                    start=Decimal("0"),
                ),
                latest_acceptance=max(acceptances, default=None),
            )
        )
    return tuple(
        sorted(
            signals,
            key=lambda item: (item.signal_year, item.signal_month, item.issuer_cik, item.insider_class),
        )
    )


def build_matched_control_signals(
    signals: Sequence[FirmMonthSignal], *, seed: int = CONTROL_SEED
) -> tuple[tuple[FirmMonthSignal, ...], Mapping[str, int]]:
    """Move each firm to another month in the same quarter before prices read."""
    rng = Random(seed)
    output: list[FirmMonthSignal] = []
    counters: Counter[str] = Counter()
    treated = {(signal.issuer_cik, signal.signal_year, signal.signal_month) for signal in signals}
    cells: dict[tuple[str, InsiderClass, int, int], list[FirmMonthSignal]] = defaultdict(list)
    for signal in signals:
        cells[(signal.issuer_cik, signal.insider_class, signal.signal_year, (signal.signal_month - 1) // 3)].append(
            signal
        )
    for (issuer_cik, _insider_class, year, quarter), cell_signals in sorted(cells.items()):
        quarter_start = quarter * 3 + 1
        alternatives = [
            month for month in range(quarter_start, quarter_start + 3) if (issuer_cik, year, month) not in treated
        ]
        rng.shuffle(alternatives)
        ordered_signals = sorted(cell_signals, key=lambda item: (item.signal_month, item.accession_numbers))
        matched = min(len(ordered_signals), len(alternatives))
        counters["control_cell_unmatched_signals"] += len(ordered_signals) - matched
        for signal, control_month in zip(ordered_signals[:matched], alternatives[:matched], strict=True):
            output.append(
                replace(
                    signal,
                    signal_month=control_month,
                    accession_numbers=tuple(f"control:{item}" for item in signal.accession_numbers),
                    latest_acceptance=None,
                )
            )
    counters["matched_control_firm_months"] = len(output)
    counters["input_signal_firm_months"] = len(signals)
    return tuple(sorted(output, key=lambda item: (item.signal_date, item.issuer_cik, item.insider_class))), dict(
        counters
    )


_CREATE_TEMP_SIGNALS = """
    CREATE TEMP TABLE insider_trial_signals (
        event_index    INTEGER PRIMARY KEY,
        instrument_id BIGINT NOT NULL,
        signal_year   INTEGER NOT NULL,
        signal_month  INTEGER NOT NULL
    ) ON COMMIT DROP
"""


_WINDOW_SQL = """
    WITH event_series AS (
        SELECT e.*, s.series_id,
               (make_date(e.signal_year, e.signal_month, 1) + interval '1 month')::date AS target_start,
               (make_date(e.signal_year, e.signal_month, 1) + interval '2 months')::date AS target_end
        FROM insider_trial_signals e
        LEFT JOIN research_price_series s
          ON s.instrument_id = e.instrument_id
         AND s.vendor = %(corpus_vendor)s
    ), target_raw AS (
        SELECT e.event_index, d.bar_date, d.open, d.close,
               coalesce(q.return_usable, TRUE) AS return_usable
        FROM event_series e
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = e.series_id
         AND cov.rule_set_version = %(quarantine_version)s
        JOIN research_price_daily d
          ON d.series_id = e.series_id
         AND d.bar_date >= e.target_start
         AND d.bar_date < e.target_end
         AND d.bar_date <= %(frontier)s
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(quarantine_version)s
    ), target_rows AS (
        SELECT event_index, bar_date, open, close,
               row_number() OVER (PARTITION BY e.event_index ORDER BY e.bar_date) AS rn_asc,
               row_number() OVER (PARTITION BY e.event_index ORDER BY e.bar_date DESC) AS rn_desc
        FROM target_raw e
        WHERE e.return_usable
    ), target AS (
        SELECT event_index,
               max(bar_date) FILTER (WHERE rn_asc = 1) AS entry_date,
               max(open) FILTER (WHERE rn_asc = 1) AS entry_open,
               max(bar_date) FILTER (WHERE rn_desc = 1) AS exit_date,
               max(close) FILTER (WHERE rn_desc = 1) AS exit_close,
               count(*) AS holding_sessions,
               TRUE AS holding_usable
        FROM target_rows GROUP BY event_index
    ), prior_raw AS (
        SELECT e.event_index, d.bar_date, d.close, d.volume,
               coalesce(q.return_usable, TRUE) AS return_usable
        FROM event_series e
        JOIN target t USING (event_index)
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = e.series_id
         AND cov.rule_set_version = %(quarantine_version)s
        JOIN research_price_daily d
          ON d.series_id = e.series_id
         AND d.bar_date < t.entry_date
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(quarantine_version)s
    ), prior_ranked AS (
        SELECT event_index, bar_date, close, volume, return_usable,
               row_number() OVER (PARTITION BY e.event_index ORDER BY e.bar_date DESC) AS rn
        FROM prior_raw e
    ), prior AS (
        SELECT event_index,
               max(close) FILTER (WHERE rn = 1) AS prior_close,
               bool_and(return_usable) FILTER (WHERE rn = 1) AS prior_close_usable,
               count(*) FILTER (WHERE rn <= 20) AS prior_sessions,
               count(*) FILTER (
                   WHERE rn <= 20 AND return_usable AND close > 0 AND volume IS NOT NULL AND volume > 0
               ) AS valid_liquidity_sessions,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY close * volume) FILTER (
                   WHERE rn <= 20 AND return_usable AND close > 0 AND volume IS NOT NULL AND volume > 0
               ) AS median_dollar_volume
        FROM prior_ranked GROUP BY event_index
    )
    SELECT e.event_index, e.series_id,
           e.target_end <= date_trunc('month', CAST(%(frontier)s AS date))::date AS target_month_complete,
           t.entry_date, t.entry_open, t.exit_date, t.exit_close,
           coalesce(t.holding_sessions, 0), t.holding_usable,
           p.prior_close, p.prior_close_usable, coalesce(p.prior_sessions, 0),
           coalesce(p.valid_liquidity_sessions, 0), p.median_dollar_volume
    FROM event_series e
    LEFT JOIN target t USING (event_index)
    LEFT JOIN prior p USING (event_index)
    ORDER BY e.event_index
"""


def load_windows(conn: psycopg.Connection[Any], signals: Sequence[FirmMonthSignal]) -> tuple[FirmMonthWindow, ...]:
    conn.execute("DROP TABLE IF EXISTS insider_trial_signals")
    conn.execute(_CREATE_TEMP_SIGNALS)
    with conn.cursor() as cursor:
        with cursor.copy(
            "COPY insider_trial_signals (event_index, instrument_id, signal_year, signal_month) FROM STDIN"
        ) as copy:
            for index, signal in enumerate(signals):
                copy.write_row((index, signal.instrument_id, signal.signal_year, signal.signal_month))
    rows = conn.execute(
        _WINDOW_SQL,
        {
            "corpus_vendor": CORPUS_VENDORS[0],
            "quarantine_version": QUARANTINE_RULE_SET_VERSION,
            "frontier": CORPUS_FROZEN_LAST_BAR,
        },
    ).fetchall()
    return tuple(
        FirmMonthWindow(
            signal=signals[int(row[0])],
            series_id=None if row[1] is None else int(row[1]),
            target_month_complete=bool(row[2]),
            entry_date=row[3],
            entry_open=None if row[4] is None else Decimal(row[4]),
            exit_date=row[5],
            exit_close=None if row[6] is None else Decimal(row[6]),
            holding_sessions=int(row[7]),
            holding_usable=row[8],
            prior_close=None if row[9] is None else Decimal(row[9]),
            prior_close_usable=row[10],
            prior_sessions=int(row[11]),
            valid_liquidity_sessions=int(row[12]),
            median_dollar_volume=None if row[13] is None else Decimal(row[13]),
        )
        for row in rows
    )


def _eligible_window(window: FirmMonthWindow) -> str | None:
    if not window.target_month_complete:
        return "incomplete_target_month_at_corpus_frontier"
    if window.entry_date is None or window.entry_open is None or window.series_id is None:
        return "price_series_or_entry_missing"
    if window.entry_date < PRIMARY_START:
        return "entry_before_primary_start"
    if window.exit_date is None or window.exit_close is None:
        return "incomplete_target_month"
    if window.entry_date.month != ((window.signal.signal_month % 12) + 1):
        return "entry_not_in_next_calendar_month"
    if not window.holding_usable or not window.prior_close_usable:
        return "quarantined_price_window"
    if window.entry_open < MIN_ENTRY_PRICE or window.exit_close <= 0:
        return "nonpositive_or_sub_five_dollar_fill"
    if window.prior_sessions != 20 or window.valid_liquidity_sessions != 20:
        return "incomplete_prior_liquidity_window"
    if window.median_dollar_volume is None or window.median_dollar_volume < MIN_MEDIAN_DOLLAR_VOLUME:
        return "median_dollar_volume_below_floor"
    if window.prior_close is None or window.prior_close <= 0:
        return "prior_close_missing"
    if not window.signal.disclosed_value.is_finite() or window.signal.disclosed_value <= 0:
        return "disclosed_purchase_value_invalid"
    acceptance = window.signal.latest_acceptance
    if acceptance is not None and acceptance.date() >= window.entry_date:
        return "acceptance_not_before_formation"
    return None


def _instrument_context(conn: psycopg.Connection[Any]) -> dict[int, str | None]:
    rows = conn.execute(
        """
        SELECT s.instrument_id, p.sic
        FROM research_price_series s
        LEFT JOIN instrument_sec_profile p ON p.instrument_id = s.instrument_id
        WHERE s.vendor = %s
          AND s.instrument_id IS NOT NULL
        """,
        (CORPUS_VENDORS[0],),
    ).fetchall()
    return {
        int(row[0]): (classification.spdr_symbol if (classification := resolve_sector_spdr(row[1])) else None)
        for row in rows
    }


def _comparators(conn: psycopg.Connection[Any]) -> dict[str, dict[date, tuple[Decimal, Decimal]]]:
    rows = conn.execute(
        """
        SELECT s.vendor_symbol, d.bar_date, d.open, d.close
        FROM research_price_series s JOIN research_price_daily d USING (series_id)
        WHERE s.comparator_snapshot_id = %s
        ORDER BY s.vendor_symbol, d.bar_date
        """,
        (SNAPSHOT_ID,),
    ).fetchall()
    output: dict[str, dict[date, tuple[Decimal, Decimal]]] = defaultdict(dict)
    for symbol, bar_date, open_price, close_price in rows:
        output[str(symbol)][bar_date] = (Decimal(open_price), Decimal(close_price))
    return dict(output)


def _weighted(values: Sequence[tuple[float, Decimal]]) -> float:
    total = sum((weight for _, weight in values), start=Decimal("0"))
    if total <= 0:
        raise ValueError("portfolio weight denominator is not positive")
    return sum(value * float(weight / total) for value, weight in values)


def _maximum_weight_pct(outcomes: Sequence[FirmMonthOutcome]) -> float:
    total = sum((item.weight_value for item in outcomes), start=Decimal("0"))
    if total <= 0:
        raise ValueError("portfolio weight denominator is not positive")
    return float(max(item.weight_value for item in outcomes) / total * 100)


def _portfolio_months(
    outcomes: Sequence[FirmMonthOutcome], refusals: Counter[str]
) -> tuple[MonthlyPortfolioReturn, ...]:
    grouped: dict[tuple[date, InsiderClass], list[FirmMonthOutcome]] = defaultdict(list)
    for outcome in outcomes:
        signal = outcome.signal
        target_year = signal.signal_year + (1 if signal.signal_month == 12 else 0)
        target_month = 1 if signal.signal_month == 12 else signal.signal_month + 1
        grouped[(date(target_year, target_month, 1), signal.insider_class)].append(outcome)
    dates = sorted({entry_date for entry_date, _ in grouped})
    monthly: list[MonthlyPortfolioReturn] = []
    for entry_date in dates:
        opportunistic = grouped.get((entry_date, "opportunistic"), [])
        routine = grouped.get((entry_date, "routine"), [])
        if not opportunistic or not routine:
            refusals["primary_month_missing_one_cohort"] += 1
            continue
        opp_return = _weighted([(item.net_return_pct, item.weight_value) for item in opportunistic])
        routine_return = _weighted([(item.net_return_pct, item.weight_value) for item in routine])
        routine_short = _weighted([(item.short_net_return_pct, item.weight_value) for item in routine])
        opp_equal = mean(item.net_return_pct for item in opportunistic)
        routine_short_equal = mean(item.short_net_return_pct for item in routine)
        market_pairs_opp = [
            (item.market_relative_pct, item.weight_value)
            for item in opportunistic
            if item.market_relative_pct is not None
        ]
        market_pairs_routine = [
            (item.short_market_relative_pct, item.weight_value)
            for item in routine
            if item.short_market_relative_pct is not None
        ]
        sector_pairs_opp = [
            (item.sector_relative_pct, item.weight_value)
            for item in opportunistic
            if item.sector_relative_pct is not None
        ]
        sector_pairs_routine = [
            (item.short_sector_relative_pct, item.weight_value)
            for item in routine
            if item.short_sector_relative_pct is not None
        ]
        market_spread = (
            _weighted([(float(value), weight) for value, weight in market_pairs_opp])
            + _weighted([(float(value), weight) for value, weight in market_pairs_routine])
            if len(market_pairs_opp) == len(opportunistic) and len(market_pairs_routine) == len(routine)
            else None
        )
        sector_spread = (
            _weighted([(float(value), weight) for value, weight in sector_pairs_opp])
            + _weighted([(float(value), weight) for value, weight in sector_pairs_routine])
            if len(sector_pairs_opp) == len(opportunistic) and len(sector_pairs_routine) == len(routine)
            else None
        )
        monthly.append(
            MonthlyPortfolioReturn(
                entry_date=entry_date,
                opportunistic_pct=opp_return,
                routine_pct=routine_return,
                spread_pct=opp_return + routine_short,
                equal_weight_spread_pct=opp_equal + routine_short_equal,
                market_relative_spread_pct=market_spread,
                sector_relative_spread_pct=sector_spread,
                opportunistic_firms=len(opportunistic),
                routine_firms=len(routine),
                unique_firms=len({item.signal.issuer_cik for item in [*opportunistic, *routine]}),
                minimum_median_dollar_volume=min(item.median_dollar_volume for item in [*opportunistic, *routine]),
                maximum_single_firm_weight_pct=max(
                    _maximum_weight_pct(opportunistic),
                    _maximum_weight_pct(routine),
                ),
            )
        )
    return tuple(monthly)


def evaluate_portfolios(conn: psycopg.Connection[Any], signals: Sequence[FirmMonthSignal]) -> PortfolioEvaluation:
    windows = load_windows(conn, signals)
    sectors = _instrument_context(conn)
    comparators = _comparators(conn)
    refusals: Counter[str] = Counter()
    outcomes: list[FirmMonthOutcome] = []
    for window in windows:
        reason = _eligible_window(window)
        if reason is not None:
            refusals[reason] += 1
            continue
        if (
            window.entry_date is None
            or window.entry_open is None
            or window.exit_date is None
            or window.exit_close is None
            or window.prior_close is None
            or window.median_dollar_volume is None
        ):
            raise RuntimeError("ineligible insider window escaped the refusal gate")
        gross = float((window.exit_close / window.entry_open - 1) * 100)
        half_spread = half_spread_for(window.entry_open)
        net = float(
            (
                sell_price(window.exit_close, half_spread=half_spread)
                / buy_price(window.entry_open, half_spread=half_spread)
                - 1
            )
            * 100
        )
        entry_proceeds = sell_price(window.entry_open, half_spread=half_spread)
        short_net = float(
            (entry_proceeds - buy_price(window.exit_close, half_spread=half_spread)) / entry_proceeds * 100
        )
        spy_entry = comparators.get("SPY", {}).get(window.entry_date)
        spy_exit = comparators.get("SPY", {}).get(window.exit_date)
        market_relative = None
        short_market_relative = None
        if spy_entry is None or spy_exit is None:
            refusals["market_comparator_session_missing"] += 1
        else:
            market_return = float((spy_exit[1] / spy_entry[0] - 1) * 100)
            market_relative = net - market_return
            short_market_relative = short_net + market_return
        sector_symbol = sectors.get(window.signal.instrument_id)
        sector_relative = None
        short_sector_relative = None
        if sector_symbol is None:
            refusals["sector_mapping_missing"] += 1
        else:
            sector_entry = comparators.get(sector_symbol, {}).get(window.entry_date)
            sector_exit = comparators.get(sector_symbol, {}).get(window.exit_date)
            if sector_entry is None or sector_exit is None:
                refusals["sector_comparator_session_missing"] += 1
            else:
                sector_return = float((sector_exit[1] / sector_entry[0] - 1) * 100)
                sector_relative = net - sector_return
                short_sector_relative = short_net + sector_return
        outcomes.append(
            FirmMonthOutcome(
                signal=window.signal,
                entry_date=window.entry_date,
                exit_date=window.exit_date,
                net_return_pct=net,
                short_net_return_pct=short_net,
                gross_return_pct=gross,
                weight_value=window.signal.disclosed_value,
                median_dollar_volume=window.median_dollar_volume,
                market_relative_pct=market_relative,
                short_market_relative_pct=short_market_relative,
                sector_relative_pct=sector_relative,
                short_sector_relative_pct=short_sector_relative,
                sector_symbol=sector_symbol,
            )
        )
    monthly = _portfolio_months(outcomes, refusals)
    bootstrap = block_bootstrap_expectancy(
        cluster_by_date([item.spread_pct for item in monthly], [item.entry_date for item in monthly]),
        seed=BOOTSTRAP_SEED,
    )
    refusals["input_firm_month_signals"] = len(signals)
    refusals["eligible_firm_month_outcomes"] = len(outcomes)
    refusals["complete_primary_months"] = len(monthly)
    return PortfolioEvaluation(
        firm_outcomes=tuple(outcomes),
        monthly_returns=monthly,
        bootstrap=bootstrap,
        refusals=dict(sorted(refusals.items())),
    )


def maximum_drawdown_pct(monthly: Sequence[MonthlyPortfolioReturn]) -> float | None:
    if not monthly:
        return None
    equity = peak = 1.0
    worst = 0.0
    for item in monthly:
        equity *= 1 + item.spread_pct / 100
        peak = max(peak, equity)
        worst = min(worst, equity / peak - 1)
    return worst * 100


def expected_shortfall_5_pct(monthly: Sequence[MonthlyPortfolioReturn]) -> float | None:
    if not monthly:
        return None
    ordered = sorted(item.spread_pct for item in monthly)
    return mean(ordered[: max(1, (len(ordered) + 19) // 20)])


def profit_factor(monthly: Sequence[MonthlyPortfolioReturn]) -> float | None:
    gains = sum(max(item.spread_pct, 0) for item in monthly)
    losses = -sum(min(item.spread_pct, 0) for item in monthly)
    return gains / losses if losses else None


def median_firm_count(monthly: Sequence[MonthlyPortfolioReturn]) -> float | None:
    return median(item.unique_firms for item in monthly) if monthly else None


__all__ = [
    "BOOTSTRAP_SEED",
    "CONTROL_SEED",
    "FirmMonthOutcome",
    "FirmMonthSignal",
    "MonthlyPortfolioReturn",
    "PortfolioEvaluation",
    "build_firm_month_signals",
    "build_matched_control_signals",
    "evaluate_portfolios",
    "expected_shortfall_5_pct",
    "maximum_drawdown_pct",
    "median_firm_count",
    "profit_factor",
]
