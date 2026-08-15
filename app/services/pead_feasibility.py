"""Outcome-free arrival and dependence census for PEAD follow-up #2493.

This module may inspect the signal entry open and the twenty sessions strictly
before entry.  It deliberately has no exit-price or return field: the opened
#2476 result is motivation, never a planning sample for the prospective trial.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Final

import psycopg

from app.services.market_calendar import us_market_status
from app.services.pead_candidate import TriggeredSueEvent
from app.services.pead_outcomes import (
    MIN_ENTRY_PRICE,
    MIN_MEDIAN_DOLLAR_VOLUME,
    PRIMARY_START,
    earliest_entry_date,
)
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.strategy_result import CORPUS_FROZEN_LAST_BAR, CORPUS_VENDORS

HOLD_SESSIONS: Final = 62


@dataclass(frozen=True)
class PreOutcomeWindow:
    event_index: int
    instrument_id: int
    accession_number: str
    series_id: int | None
    entry_date: date | None
    entry_open: Decimal | None
    entry_return_usable: bool | None
    prior_sessions: int
    valid_liquidity_sessions: int
    median_dollar_volume: Decimal | None


@dataclass(frozen=True)
class EligiblePeadEvent:
    instrument_id: int
    accession_number: str
    entry_date: date


_CREATE_TEMP_EVENTS = """
    CREATE TEMP TABLE pead_feasibility_events (
        event_index      INTEGER PRIMARY KEY,
        instrument_id    BIGINT NOT NULL,
        entry_not_before DATE NOT NULL,
        accession_number TEXT NOT NULL
    ) ON COMMIT DROP
"""


# Every close selected here is constrained strictly before the selected entry.
# The only on/after-entry value is the entry open itself.
_PRE_OUTCOME_WINDOWS_SQL = """
    WITH event_series AS (
        SELECT e.*, s.series_id
        FROM pead_feasibility_events e
        LEFT JOIN research_price_series s
          ON s.instrument_id = e.instrument_id
         AND s.vendor = %(corpus_vendor)s
    ), entries AS (
        SELECT e.event_index, picked.entry_date, picked.entry_open,
               picked.entry_return_usable
        FROM event_series e
        LEFT JOIN LATERAL (
            SELECT d.bar_date AS entry_date, d.open AS entry_open,
                   coalesce(q.return_usable, TRUE) AS entry_return_usable
            FROM research_price_quarantine_coverage cov
            JOIN research_price_daily d
              ON d.series_id = cov.series_id
             AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
            LEFT JOIN research_bar_quarantine q
              ON q.series_id = d.series_id
             AND q.bar_date = d.bar_date
             AND q.rule_set_version = %(quarantine_version)s
            WHERE cov.series_id = e.series_id
              AND cov.rule_set_version = %(quarantine_version)s
              AND d.bar_date >= e.entry_not_before
              AND d.bar_date <= %(frontier)s
            ORDER BY d.bar_date
            LIMIT 1
        ) picked ON TRUE
    ), prior_ranked AS (
        SELECT e.event_index, d.close, d.volume,
               coalesce(q.return_usable, TRUE) AS return_usable,
               row_number() OVER (PARTITION BY e.event_index ORDER BY d.bar_date DESC) AS rn
        FROM event_series e
        JOIN entries p USING (event_index)
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
    SELECT e.event_index, e.instrument_id, e.accession_number, e.series_id,
           p.entry_date, p.entry_open, p.entry_return_usable,
           coalesce(l.prior_sessions, 0), coalesce(l.valid_liquidity_sessions, 0),
           l.median_dollar_volume
    FROM event_series e
    LEFT JOIN entries p USING (event_index)
    LEFT JOIN liquidity l USING (event_index)
    ORDER BY e.event_index
"""


def load_pre_outcome_windows(
    conn: psycopg.Connection[Any], events: Sequence[TriggeredSueEvent]
) -> tuple[PreOutcomeWindow, ...]:
    selected = [item for item in events if item.side == "long" and item.event.observation.filed_date >= PRIMARY_START]
    conn.execute("DROP TABLE IF EXISTS pead_feasibility_events")
    conn.execute(_CREATE_TEMP_EVENTS)
    with conn.cursor() as cursor:
        with cursor.copy(
            "COPY pead_feasibility_events (event_index, instrument_id, entry_not_before, accession_number) FROM STDIN"
        ) as copy:
            for index, item in enumerate(selected):
                observation = item.event.observation
                copy.write_row(
                    (
                        index,
                        observation.instrument_id,
                        earliest_entry_date(observation.filed_date, observation.accepted_at),
                        observation.accession_number,
                    )
                )
    rows = conn.execute(
        _PRE_OUTCOME_WINDOWS_SQL,
        {
            "corpus_vendor": CORPUS_VENDORS[0],
            "quarantine_version": QUARANTINE_RULE_SET_VERSION,
            "frontier": CORPUS_FROZEN_LAST_BAR,
        },
    ).fetchall()
    return tuple(
        PreOutcomeWindow(
            event_index=int(row[0]),
            instrument_id=int(row[1]),
            accession_number=str(row[2]),
            series_id=None if row[3] is None else int(row[3]),
            entry_date=row[4],
            entry_open=None if row[5] is None else Decimal(row[5]),
            entry_return_usable=row[6],
            prior_sessions=int(row[7]),
            valid_liquidity_sessions=int(row[8]),
            median_dollar_volume=None if row[9] is None else Decimal(row[9]),
        )
        for row in rows
    )


def eligible_events(
    windows: Sequence[PreOutcomeWindow],
) -> tuple[tuple[EligiblePeadEvent, ...], Mapping[str, int]]:
    """Apply entry-known gates and suppress alternative share classes."""
    refusals: Counter[str] = Counter()
    grouped: dict[str, list[PreOutcomeWindow]] = defaultdict(list)
    for window in windows:
        if window.series_id is None or window.entry_date is None or window.entry_open is None:
            refusals["price_series_or_entry_missing"] += 1
        elif not window.entry_return_usable:
            refusals["quarantined_entry"] += 1
        elif window.entry_open < MIN_ENTRY_PRICE:
            refusals["entry_price_below_floor"] += 1
        elif window.prior_sessions != 20 or window.valid_liquidity_sessions != 20:
            refusals["incomplete_prior_liquidity_window"] += 1
        elif window.median_dollar_volume is None or window.median_dollar_volume < MIN_MEDIAN_DOLLAR_VOLUME:
            refusals["median_dollar_volume_below_floor"] += 1
        else:
            grouped[window.accession_number].append(window)

    output: list[EligiblePeadEvent] = []
    for accession, candidates in grouped.items():
        candidates.sort(
            key=lambda item: (item.median_dollar_volume or Decimal("0"), -item.instrument_id),
            reverse=True,
        )
        chosen = candidates[0]
        if chosen.entry_date is None:
            raise RuntimeError("eligible PEAD feasibility row has no entry date")
        refusals["share_class_duplicates_suppressed"] += len(candidates) - 1
        output.append(
            EligiblePeadEvent(
                instrument_id=chosen.instrument_id,
                accession_number=accession,
                entry_date=chosen.entry_date,
            )
        )
    output.sort(key=lambda item: (item.entry_date, item.accession_number))
    refusals["input_long_signal_rows"] = len(windows)
    refusals["eligible_issuer_events"] = len(output)
    return tuple(output), dict(sorted(refusals.items()))


def purged_date_count(
    entry_dates: Sequence[date], session_dates: Sequence[date], *, hold_sessions: int = HOLD_SESSIONS
) -> int:
    """Greedy maximum date count with at least ``hold_sessions`` session indices apart."""
    if hold_sessions < 1:
        raise ValueError("hold_sessions must be positive")
    index = {session: offset for offset, session in enumerate(sorted(set(session_dates)))}
    selected = 0
    next_allowed = -1
    for entry_date in sorted(set(entry_dates)):
        position = index.get(entry_date)
        if position is None:
            raise ValueError(f"entry date {entry_date} is absent from the declared session calendar")
        if position >= next_allowed:
            selected += 1
            next_allowed = position + hold_sessions
    return selected


def market_session_dates(first: date, last: date) -> tuple[date, ...]:
    """Return the repo's declared NYSE sessions over a closed interval."""
    if first > last:
        raise ValueError("first session boundary must not follow last")
    result: list[date] = []
    current = first
    while current <= last:
        if us_market_status(current) != "closed":
            result.append(current)
        current += timedelta(days=1)
    return tuple(result)


__all__ = [
    "EligiblePeadEvent",
    "HOLD_SESSIONS",
    "PreOutcomeWindow",
    "_PRE_OUTCOME_WINDOWS_SQL",
    "eligible_events",
    "load_pre_outcome_windows",
    "market_session_dates",
    "purged_date_count",
]
