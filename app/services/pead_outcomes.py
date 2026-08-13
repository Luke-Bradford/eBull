"""Causal price windows and after-cost event outcomes for #2476.

This module is separate from :mod:`pead_candidate` so source construction can
be reviewed and run without opening the sealed return interval.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from random import Random
from statistics import mean, median
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg

from app.services.block_bootstrap import BootstrapResult, block_bootstrap_expectancy, cluster_by_date
from app.services.cost_model import buy_price, half_spread_for, sell_price
from app.services.pead_candidate import TriggeredSueEvent
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.research_comparator_snapshot import SNAPSHOT_ID
from app.services.sector_classification import resolve_sector_spdr
from app.services.strategy_result import CORPUS_FROZEN_LAST_BAR, CORPUS_VENDORS

PRIMARY_START: Final = date(2022, 1, 1)
MIN_ENTRY_PRICE: Final = Decimal("5")
MIN_MEDIAN_DOLLAR_VOLUME: Final = Decimal("10000000")
BOOTSTRAP_SEED: Final = 2476
CONTROL_SEED: Final = 2476001
MARKET_TIMEZONE: Final = ZoneInfo("America/New_York")
REGULAR_MARKET_OPEN: Final = time(9, 30)


@dataclass(frozen=True)
class PriceWindow:
    event_index: int
    instrument_id: int
    filed_date: date
    side: str
    accession_number: str
    series_id: int | None
    entry_date: date | None
    entry_open: Decimal | None
    exit_5_date: date | None
    exit_5_close: Decimal | None
    exit_5_return_usable: bool | None
    exit_20_date: date | None
    exit_20_close: Decimal | None
    exit_20_return_usable: bool | None
    exit_40_date: date | None
    exit_40_close: Decimal | None
    exit_40_return_usable: bool | None
    exit_62_date: date | None
    exit_62_close: Decimal | None
    entry_return_usable: bool | None
    exit_62_return_usable: bool | None
    prior_sessions: int
    valid_liquidity_sessions: int
    median_dollar_volume: Decimal | None


@dataclass(frozen=True)
class EventOutcome:
    instrument_id: int
    issuer_cik: str
    accession_number: str
    side: str
    entry_date: date
    exit_date: date
    gross_return_pct: float
    net_return_pct: float
    net_return_5_pct: float | None
    net_return_20_pct: float | None
    net_return_40_pct: float | None
    market_relative_net_return_pct: float | None
    sector_relative_net_return_pct: float | None
    sector_symbol: str | None


@dataclass(frozen=True)
class OutcomeSummary:
    outcomes: tuple[EventOutcome, ...]
    bootstrap: BootstrapResult | None
    refusals: Mapping[str, int]

    @property
    def win_rate_pct(self) -> float | None:
        if not self.outcomes:
            return None
        return sum(item.net_return_pct > 0 for item in self.outcomes) / len(self.outcomes) * 100

    @property
    def expectancy_pct(self) -> float | None:
        return mean(item.net_return_pct for item in self.outcomes) if self.outcomes else None

    @property
    def profit_factor(self) -> float | None:
        gains = sum(max(item.net_return_pct, 0.0) for item in self.outcomes)
        losses = -sum(min(item.net_return_pct, 0.0) for item in self.outcomes)
        return gains / losses if losses > 0 else None

    @property
    def worst_trade_pct(self) -> float | None:
        return min((item.net_return_pct for item in self.outcomes), default=None)

    @property
    def expected_shortfall_5_pct(self) -> float | None:
        if not self.outcomes:
            return None
        ordered = sorted(item.net_return_pct for item in self.outcomes)
        tail_count = max(1, (len(ordered) + 19) // 20)
        return mean(ordered[:tail_count])


_CREATE_TEMP_EVENTS = """
    CREATE TEMP TABLE pead_trial_events (
        event_index      INTEGER PRIMARY KEY,
        instrument_id    BIGINT NOT NULL,
        filed_date       DATE NOT NULL,
        entry_not_before DATE NOT NULL,
        side             TEXT NOT NULL CHECK (side IN ('long','short')),
        accession_number TEXT NOT NULL
    ) ON COMMIT DROP
"""

_PRICE_WINDOWS_SQL = """
    WITH event_series AS (
        SELECT e.*, s.series_id
        FROM pead_trial_events e
        LEFT JOIN research_price_series s
          ON s.instrument_id = e.instrument_id
         AND s.vendor = %(corpus_vendor)s
    ), forward AS (
        SELECT e.event_index, d.bar_date, d.open, d.close,
               coalesce(q.return_usable, TRUE) AS return_usable,
               row_number() OVER (PARTITION BY e.event_index ORDER BY d.bar_date) AS rn
        FROM event_series e
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = e.series_id
         AND cov.rule_set_version = %(quarantine_version)s
        JOIN research_price_daily d
          ON d.series_id = e.series_id
         AND d.bar_date >= e.entry_not_before
         AND d.bar_date <= %(frontier)s
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(quarantine_version)s
    ), pivots AS (
        SELECT event_index,
               max(bar_date) FILTER (WHERE rn = 1) AS entry_date,
               max(open) FILTER (WHERE rn = 1) AS entry_open,
               bool_and(return_usable) FILTER (WHERE rn = 1) AS entry_return_usable,
               max(bar_date) FILTER (WHERE rn = 5) AS exit_5_date,
               max(close) FILTER (WHERE rn = 5) AS exit_5_close,
               bool_and(return_usable) FILTER (WHERE rn <= 5) AS exit_5_return_usable,
               max(bar_date) FILTER (WHERE rn = 20) AS exit_20_date,
               max(close) FILTER (WHERE rn = 20) AS exit_20_close,
               bool_and(return_usable) FILTER (WHERE rn <= 20) AS exit_20_return_usable,
               max(bar_date) FILTER (WHERE rn = 40) AS exit_40_date,
               max(close) FILTER (WHERE rn = 40) AS exit_40_close,
               bool_and(return_usable) FILTER (WHERE rn <= 40) AS exit_40_return_usable,
               max(bar_date) FILTER (WHERE rn = 62) AS exit_62_date,
               max(close) FILTER (WHERE rn = 62) AS exit_62_close,
               bool_and(return_usable) FILTER (WHERE rn <= 62) AS exit_62_return_usable
        FROM forward
        GROUP BY event_index
    ), prior_ranked AS (
        SELECT e.event_index, d.close, d.volume,
               coalesce(q.return_usable, TRUE) AS return_usable,
               row_number() OVER (PARTITION BY e.event_index ORDER BY d.bar_date DESC) AS rn
        FROM event_series e
        JOIN pivots p USING (event_index)
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = e.series_id
         AND cov.rule_set_version = %(quarantine_version)s
        JOIN research_price_daily d
          ON d.series_id = e.series_id
         AND d.bar_date < p.entry_date
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(quarantine_version)s
    ), liquidity AS (
        SELECT event_index,
               count(*) FILTER (WHERE rn <= 20) AS prior_sessions,
               count(*) FILTER (
                   WHERE rn <= 20 AND return_usable AND close > 0 AND volume IS NOT NULL AND volume > 0
               ) AS valid_liquidity_sessions,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY close * volume) FILTER (
                   WHERE rn <= 20 AND return_usable AND close > 0 AND volume IS NOT NULL AND volume > 0
               ) AS median_dollar_volume
        FROM prior_ranked
        GROUP BY event_index
    )
    SELECT e.event_index, e.instrument_id, e.filed_date, e.side, e.accession_number,
           e.series_id, p.entry_date, p.entry_open,
           p.exit_5_date, p.exit_5_close, p.exit_5_return_usable,
           p.exit_20_date, p.exit_20_close, p.exit_20_return_usable,
           p.exit_40_date, p.exit_40_close, p.exit_40_return_usable,
           p.exit_62_date, p.exit_62_close,
           p.entry_return_usable, p.exit_62_return_usable,
           coalesce(l.prior_sessions, 0), coalesce(l.valid_liquidity_sessions, 0),
           l.median_dollar_volume
    FROM event_series e
    LEFT JOIN pivots p USING (event_index)
    LEFT JOIN liquidity l USING (event_index)
    ORDER BY e.event_index
"""


def load_price_windows(
    conn: psycopg.Connection[Any],
    events: Sequence[TriggeredSueEvent],
) -> tuple[PriceWindow, ...]:
    selected = [item for item in events if item.side is not None and item.event.observation.filed_date >= PRIMARY_START]
    conn.execute("DROP TABLE IF EXISTS pead_trial_events")
    conn.execute(_CREATE_TEMP_EVENTS)
    with conn.cursor() as cursor:
        with cursor.copy(
            "COPY pead_trial_events "
            "(event_index, instrument_id, filed_date, entry_not_before, side, accession_number) FROM STDIN"
        ) as copy:
            for index, item in enumerate(selected):
                observation = item.event.observation
                copy.write_row(
                    (
                        index,
                        observation.instrument_id,
                        observation.filed_date,
                        earliest_entry_date(observation.filed_date, observation.accepted_at),
                        item.side,
                        observation.accession_number,
                    )
                )
    rows = conn.execute(
        _PRICE_WINDOWS_SQL,
        {
            "corpus_vendor": CORPUS_VENDORS[0],
            "quarantine_version": QUARANTINE_RULE_SET_VERSION,
            "frontier": CORPUS_FROZEN_LAST_BAR,
        },
    ).fetchall()
    return tuple(
        PriceWindow(
            event_index=int(row[0]),
            instrument_id=int(row[1]),
            filed_date=row[2],
            side=str(row[3]),
            accession_number=str(row[4]),
            series_id=None if row[5] is None else int(row[5]),
            entry_date=row[6],
            entry_open=None if row[7] is None else Decimal(row[7]),
            exit_5_date=row[8],
            exit_5_close=None if row[9] is None else Decimal(row[9]),
            exit_5_return_usable=row[10],
            exit_20_date=row[11],
            exit_20_close=None if row[12] is None else Decimal(row[12]),
            exit_20_return_usable=row[13],
            exit_40_date=row[14],
            exit_40_close=None if row[15] is None else Decimal(row[15]),
            exit_40_return_usable=row[16],
            exit_62_date=row[17],
            exit_62_close=None if row[18] is None else Decimal(row[18]),
            entry_return_usable=row[19],
            exit_62_return_usable=row[20],
            prior_sessions=int(row[21]),
            valid_liquidity_sessions=int(row[22]),
            median_dollar_volume=None if row[23] is None else Decimal(row[23]),
        )
        for row in rows
    )


def earliest_entry_date(filed_date: date, accepted_at: datetime | None) -> date:
    """Return the first calendar date whose regular open follows knowledge.

    The daily corpus has no intraday bars. An exact SEC acceptance strictly
    before 09:30 New York time may use that date's open; acceptance at or after
    the open advances the lower bound one calendar day. Missing acceptance
    uses the preregistered conservative end-of-filed-date boundary. The price
    join then selects the first actual market session on or after this date.
    """
    if accepted_at is None:
        return filed_date + timedelta(days=1)
    if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
        raise ValueError("accepted_at must be timezone-aware")
    market_time = accepted_at.astimezone(MARKET_TIMEZONE)
    if market_time.timetz().replace(tzinfo=None) < REGULAR_MARKET_OPEN:
        return market_time.date()
    return market_time.date() + timedelta(days=1)


def _eligible_window(window: PriceWindow) -> str | None:
    if window.series_id is None or window.entry_date is None or window.entry_open is None:
        return "price_series_or_entry_missing"
    if window.exit_62_date is None or window.exit_62_close is None:
        return "incomplete_62_session_outcome"
    if not window.entry_return_usable or not window.exit_62_return_usable:
        return "quarantined_primary_horizon"
    if window.entry_open < MIN_ENTRY_PRICE:
        return "entry_price_below_floor"
    if window.prior_sessions != 20 or window.valid_liquidity_sessions != 20:
        return "incomplete_prior_liquidity_window"
    if window.median_dollar_volume is None or window.median_dollar_volume < MIN_MEDIAN_DOLLAR_VOLUME:
        return "median_dollar_volume_below_floor"
    if window.exit_62_close <= 0:
        return "non_positive_fill_price"
    return None


def _net_return_pct(side: str, entry: Decimal, exit_price: Decimal) -> tuple[float, float]:
    half_spread = half_spread_for(entry)
    if side == "long":
        gross = exit_price / entry - 1
        net = sell_price(exit_price, half_spread=half_spread) / buy_price(entry, half_spread=half_spread) - 1
    elif side == "short":
        gross = (entry - exit_price) / entry
        entry_proceeds = sell_price(entry, half_spread=half_spread)
        net = (entry_proceeds - buy_price(exit_price, half_spread=half_spread)) / entry_proceeds
    else:
        raise ValueError(f"unknown PEAD side {side!r}")
    return float(gross * 100), float(net * 100)


def _diagnostic_net_return_pct(
    side: str,
    entry: Decimal,
    exit_price: Decimal | None,
    return_usable: bool | None,
) -> float | None:
    if exit_price is None or not return_usable or exit_price <= 0:
        return None
    return _net_return_pct(side, entry, exit_price)[1]


def _load_instrument_context(conn: psycopg.Connection[Any]) -> tuple[dict[int, str], dict[int, str | None]]:
    rows = conn.execute(
        """
        SELECT s.instrument_id,
               lpad(e.identifier_value, 10, '0') AS cik,
               p.sic
        FROM research_price_series s
        JOIN external_identifiers e
          ON e.instrument_id = s.instrument_id
         AND e.provider = 'sec'
         AND e.identifier_type = 'cik'
         AND e.is_primary
        LEFT JOIN instrument_sec_profile p ON p.instrument_id = s.instrument_id
        WHERE s.vendor = %s
        """,
        (CORPUS_VENDORS[0],),
    ).fetchall()
    cik_by_instrument = {int(row[0]): str(row[1]) for row in rows}
    sector_by_instrument = {
        int(row[0]): (classification.spdr_symbol if (classification := resolve_sector_spdr(row[2])) else None)
        for row in rows
    }
    return cik_by_instrument, sector_by_instrument


def _load_comparators(conn: psycopg.Connection[Any]) -> dict[str, dict[date, tuple[Decimal, Decimal]]]:
    rows = conn.execute(
        """
        SELECT s.vendor_symbol, d.bar_date, d.open, d.close
        FROM research_price_series s
        JOIN research_price_daily d USING (series_id)
        WHERE s.comparator_snapshot_id = %s
        ORDER BY s.vendor_symbol, d.bar_date
        """,
        (SNAPSHOT_ID,),
    ).fetchall()
    output: dict[str, dict[date, tuple[Decimal, Decimal]]] = defaultdict(dict)
    for symbol, bar_date, open_price, close_price in rows:
        output[str(symbol)][bar_date] = (Decimal(open_price), Decimal(close_price))
    return dict(output)


def evaluate_outcomes(conn: psycopg.Connection[Any], events: Sequence[TriggeredSueEvent]) -> OutcomeSummary:
    windows = load_price_windows(conn, events)
    cik_by_instrument, sector_by_instrument = _load_instrument_context(conn)
    comparators = _load_comparators(conn)
    refusals: Counter[str] = Counter()

    # One issuer filing can fan out to multiple listed share classes. Select the
    # most liquid eligible class using only the pre-entry window.
    grouped: dict[tuple[str, str, str], list[PriceWindow]] = defaultdict(list)
    for window in windows:
        reason = _eligible_window(window)
        if reason is not None:
            refusals[reason] += 1
            continue
        cik = cik_by_instrument.get(window.instrument_id)
        if cik is None:
            refusals["issuer_cik_missing"] += 1
            continue
        grouped[(cik, window.accession_number, window.side)].append(window)

    outcomes: list[EventOutcome] = []
    for (cik, accession, side), candidates in grouped.items():
        candidates.sort(key=lambda item: (item.median_dollar_volume or Decimal("0"), -item.instrument_id), reverse=True)
        window = candidates[0]
        refusals["share_class_duplicates_suppressed"] += len(candidates) - 1
        if (
            window.entry_date is None
            or window.exit_62_date is None
            or window.entry_open is None
            or window.exit_62_close is None
        ):
            raise RuntimeError("an ineligible PEAD price window escaped the refusal gate")
        gross, net = _net_return_pct(side, window.entry_open, window.exit_62_close)
        net_5 = _diagnostic_net_return_pct(side, window.entry_open, window.exit_5_close, window.exit_5_return_usable)
        net_20 = _diagnostic_net_return_pct(side, window.entry_open, window.exit_20_close, window.exit_20_return_usable)
        net_40 = _diagnostic_net_return_pct(side, window.entry_open, window.exit_40_close, window.exit_40_return_usable)

        spy_entry = comparators.get("SPY", {}).get(window.entry_date)
        spy_exit = comparators.get("SPY", {}).get(window.exit_62_date)
        market_relative = None
        if spy_entry is None or spy_exit is None:
            refusals["market_comparator_session_missing"] += 1
        else:
            market_return = float((spy_exit[1] / spy_entry[0] - 1) * 100)
            market_relative = net - market_return if side == "long" else net + market_return

        sector_symbol = sector_by_instrument.get(window.instrument_id)
        sector_relative = None
        if sector_symbol is None:
            refusals["sector_mapping_missing"] += 1
        else:
            sector_entry = comparators.get(sector_symbol, {}).get(window.entry_date)
            sector_exit = comparators.get(sector_symbol, {}).get(window.exit_62_date)
            if sector_entry is None or sector_exit is None:
                refusals["sector_comparator_session_missing"] += 1
            else:
                sector_return = float((sector_exit[1] / sector_entry[0] - 1) * 100)
                sector_relative = net - sector_return if side == "long" else net + sector_return

        outcomes.append(
            EventOutcome(
                instrument_id=window.instrument_id,
                issuer_cik=cik,
                accession_number=accession,
                side=side,
                entry_date=window.entry_date,
                exit_date=window.exit_62_date,
                gross_return_pct=gross,
                net_return_pct=net,
                net_return_5_pct=net_5,
                net_return_20_pct=net_20,
                net_return_40_pct=net_40,
                market_relative_net_return_pct=market_relative,
                sector_relative_net_return_pct=sector_relative,
                sector_symbol=sector_symbol,
            )
        )
    outcomes.sort(key=lambda item: (item.entry_date, item.issuer_cik, item.side))
    bootstrap = None
    if outcomes:
        clusters = cluster_by_date(
            [item.net_return_pct for item in outcomes],
            [item.entry_date for item in outcomes],
        )
        bootstrap = block_bootstrap_expectancy(clusters, seed=BOOTSTRAP_SEED)
    refusals["input_signal_events"] = len(windows)
    refusals["eligible_issuer_events"] = len(outcomes)
    return OutcomeSummary(outcomes=tuple(outcomes), bootstrap=bootstrap, refusals=dict(sorted(refusals.items())))


def build_matched_control_events(
    events: Sequence[TriggeredSueEvent],
    *,
    seed: int = CONTROL_SEED,
) -> tuple[tuple[TriggeredSueEvent, ...], Mapping[str, int]]:
    """Select a deterministic one-for-one middle-SUE filing control.

    Controls are matched without replacement on filing calendar quarter and
    fiscal quarter. They inherit the signal's long/short side so direction and
    cost arithmetic have the same composition, but their own SUE is inside the
    causal 10th/90th-percentile thresholds. Selection reads no price or return.
    """
    rng = Random(seed)
    pools: dict[tuple[int, int], list[TriggeredSueEvent]] = defaultdict(list)
    signals: list[TriggeredSueEvent] = []
    for event in events:
        if event.side is None:
            key = (event.event.calendar_quarter, event.event.observation.fiscal_quarter)
            pools[key].append(event)
        elif event.event.observation.filed_date >= PRIMARY_START:
            signals.append(event)
    for pool in pools.values():
        pool.sort(
            key=lambda item: (
                item.event.observation.filed_date,
                item.event.observation.instrument_id,
                item.event.observation.accession_number,
            )
        )
        rng.shuffle(pool)

    controls: list[TriggeredSueEvent] = []
    refusals: Counter[str] = Counter()
    for signal in signals:
        key = (signal.event.calendar_quarter, signal.event.observation.fiscal_quarter)
        pool = pools.get(key)
        if not pool:
            refusals["matched_control_cell_exhausted"] += 1
            continue
        control = pool.pop()
        controls.append(replace(control, side=signal.side))
    controls.sort(key=lambda item: (item.event.observation.filed_date, item.event.observation.instrument_id))
    refusals["matched_control_events"] = len(controls)
    refusals["signal_events_to_match"] = len(signals)
    return tuple(controls), dict(sorted(refusals.items()))


def concurrency_counts(outcomes: Sequence[EventOutcome]) -> tuple[int, float | None]:
    """Return maximum and median open event counts over active dates."""
    if not outcomes:
        return 0, None
    deltas: dict[date, int] = defaultdict(int)
    for item in outcomes:
        deltas[item.entry_date] += 1
        deltas[item.exit_date + timedelta(days=1)] -= 1
    active = 0
    observed: list[int] = []
    previous: date | None = None
    for when in sorted(deltas):
        if previous is not None and when > previous and active > 0:
            observed.extend([active] * (when - previous).days)
        active += deltas[when]
        previous = when
    return max(observed, default=0), (float(median(observed)) if observed else None)


def segment_outcomes(outcomes: Sequence[EventOutcome], start: date, end: date) -> tuple[EventOutcome, ...]:
    return tuple(item for item in outcomes if start <= item.entry_date <= end)


def median_time_to_outcome_days(outcomes: Sequence[EventOutcome]) -> float | None:
    if not outcomes:
        return None
    return float(median((item.exit_date - item.entry_date).days for item in outcomes))


__all__ = [
    "BOOTSTRAP_SEED",
    "CONTROL_SEED",
    "EventOutcome",
    "OutcomeSummary",
    "PriceWindow",
    "build_matched_control_events",
    "concurrency_counts",
    "evaluate_outcomes",
    "earliest_entry_date",
    "load_price_windows",
    "median_time_to_outcome_days",
    "segment_outcomes",
]
