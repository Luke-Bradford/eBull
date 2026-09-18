"""Resolve immutable opportunity forecasts against subsequent price paths.

Each forecast owns the bracket and horizon that informed capital ranking.  This
service observes that exact decision prospectively, using the shared causal
daily-bar resolver.  It stores one terminal row and no polling heartbeats;
immature windows remain absent and are retried by a bounded round-robin scan.
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from app.services.indicator_series import BarSeries
from app.services.outcome_ledger import (
    bar_was_deleted,
    fill_price_is_superseded,
    locate_fill_index,
    raw_bar_probe,
)
from app.services.outcome_resolver import RULE_SET_VERSION as PATH_RULE_SET_VERSION
from app.services.outcome_resolver import ExitLevels, Outcome, UnresolvedReason, resolve_outcome
from app.services.price_masked_bars import MASKED_REASON, QUARANTINE_RULE_SET_VERSION, load_masked_bars
from app.services.price_segments import load_unresolved_breaks, segment_end_index

logger = logging.getLogger(__name__)

DEFAULT_BATCH_LIMIT: Final = 1_000
RESOLVER_ID: Final = "forecast-outcome-resolver-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


# This service owns forecast-to-bracket mapping; the shared resolver owns OHLC
# touch ordering and timeout fills. Both identities are required: otherwise an
# edit to gap/touch semantics could reinterpret an old forecast under the same
# supposedly immutable outcome key.
RESOLVER_VERSION: Final = f"{RESOLVER_ID}+{_code_hash()}+path-{PATH_RULE_SET_VERSION}"

_READ_CURSOR_SQL = "SELECT last_forecast_id FROM strategy_forecast_outcome_cursor WHERE id=TRUE"
_WRITE_CURSOR_SQL = """
    INSERT INTO strategy_forecast_outcome_cursor (id,last_forecast_id,updated_at)
    VALUES (TRUE,%s,now())
    ON CONFLICT (id) DO UPDATE
    SET last_forecast_id=EXCLUDED.last_forecast_id,updated_at=now()
"""
_SELECT_SQL = """
    SELECT f.forecast_id,s.instrument_id,s.fill_bar_date,s.fill_price,
           f.target_barrier_pct,f.stop_barrier_pct,f.horizon_market_days
    FROM strategy_opportunity_forecasts f
    JOIN strategy_signals s ON s.signal_id=f.signal_id
    LEFT JOIN strategy_opportunity_forecast_outcomes o
      ON o.forecast_id=f.forecast_id
     AND o.resolver_version=%(resolver_version)s
     AND o.input_rule_set_version=%(input_rule_set_version)s
    WHERE s.verdict='fired'
      AND s.signal_kind='entry'
      AND s.fill_bar_date IS NOT NULL
      AND s.fill_price IS NOT NULL
      AND f.target_barrier_pct IS NOT NULL
      AND f.stop_barrier_pct IS NOT NULL
      AND o.forecast_outcome_id IS NULL
      AND f.forecast_id > %(after_forecast_id)s
      AND (%(at_or_before_forecast_id)s::bigint IS NULL
           OR f.forecast_id <= %(at_or_before_forecast_id)s)
    ORDER BY f.forecast_id
    LIMIT %(limit)s
"""
_INSERT_SQL = """
    INSERT INTO strategy_opportunity_forecast_outcomes (
        forecast_id,resolver_version,input_rule_set_version,outcome,reason,
        exit_bar_date,exit_price,market_bars_held,gross_return_pct
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (forecast_id,resolver_version,input_rule_set_version) DO NOTHING
    RETURNING forecast_outcome_id
"""


@dataclass(frozen=True)
class PendingForecast:
    forecast_id: int
    instrument_id: int
    fill_bar_date: date
    fill_price: Decimal
    target_barrier_pct: Decimal
    stop_barrier_pct: Decimal
    horizon_market_days: int


@dataclass(frozen=True)
class ForecastOutcomeRow:
    forecast_id: int
    outcome: str
    reason: str | None
    exit_bar_date: date | None
    exit_price: Decimal | None
    market_bars_held: int | None
    gross_return_pct: Decimal | None

    def __post_init__(self) -> None:
        terminal = {"target_first", "stop_first", "timeout", "ambiguous", "unresolved"}
        if self.outcome not in terminal:
            raise ValueError(f"unknown forecast outcome {self.outcome!r}")
        if (self.outcome == "unresolved") != (self.reason is not None):
            raise ValueError("a reason is required exactly when a forecast outcome is unresolved")
        located = (self.exit_bar_date is not None) + (self.market_bars_held is not None)
        if located != (0 if self.outcome == "unresolved" else 2):
            raise ValueError("unresolved outcomes have no exit location; all other outcomes require one")
        priced = (self.exit_price is not None) + (self.gross_return_pct is not None)
        if priced != (2 if self.outcome in {"target_first", "stop_first", "timeout"} else 0):
            raise ValueError("only target, stop and timeout outcomes carry both an exit price and return")

    @classmethod
    def from_outcome(cls, forecast_id: int, outcome: Outcome) -> ForecastOutcomeRow:
        mapped = {
            "tp_hit": "target_first",
            "sl_hit": "stop_first",
            "expired": "timeout",
            "ambiguous": "ambiguous",
            "unresolved": "unresolved",
        }
        return cls(
            forecast_id=forecast_id,
            outcome=mapped[outcome.outcome],
            reason=outcome.reason,
            exit_bar_date=outcome.exit_bar_date,
            exit_price=outcome.exit_price,
            market_bars_held=outcome.bars_held,
            gross_return_pct=outcome.gross_return_pct,
        )


@dataclass(frozen=True)
class ForecastOutcomeResolutionReport:
    selected: int = 0
    written: int = 0
    immature: int = 0
    ambiguous: int = 0
    outcomes: Mapping[str, int] = field(default_factory=dict)


def _masked_reasons(rows: Sequence[Mapping[str, object]]) -> dict[int, UnresolvedReason]:
    return {
        index: MASKED_REASON
        for index, row in enumerate(rows)
        if any(row.get(field) is None for field in ("open", "high", "low", "close"))
    }


def _unresolved_forecast_row(forecast_id: int, reason: UnresolvedReason) -> ForecastOutcomeRow:
    """One recorded refusal, so a corpus disagreement cannot abort the batch.

    ⚠ `signal_bar_absent` has no counterpart here and `sql/399` deliberately
    omits it from this ledger's CHECK: the forecast path locates no signal index.
    """
    return ForecastOutcomeRow.from_outcome(
        forecast_id,
        Outcome(
            outcome="unresolved",
            resolution_method="daily_bar",
            rule_set_version=PATH_RULE_SET_VERSION,
            reason=reason,
        ),
    )


def _resolve_forecast(
    forecast: PendingForecast,
    *,
    series: BarSeries,
    unresolved_breaks: Sequence[date],
    raw_bar_exists: Callable[[date], bool] | None = None,
    masked_bar_reasons: Mapping[int, UnresolvedReason] | None = None,
) -> ForecastOutcomeRow | None:
    """Return one terminal observation, or ``None`` until the horizon matures."""
    # ⚠⚠ Recorded, not raised (#3189 finding 10) — see the signal resolver's
    # `_resolve_fill` for the full reasoning. Same guard intent (no silent
    # re-read of a rebuilt corpus), same batch-wedge blast radius if it escapes.
    try:
        fill_index = locate_fill_index(series, forecast.fill_bar_date)
    except ValueError:
        # ⚠⚠ A date the loader could not find is DELETED only if the raw corpus
        # no longer holds it; `load_masked_bars` is fail-closed at the instrument
        # level, so its silence is not evidence (Codex ckpt-2). Unknown or still
        # present => pending, retried once the corpus is visible again. See
        # `bar_was_deleted` for why neither the returned span nor the coverage
        # window can stand in for this.
        if not bar_was_deleted(raw_bar_exists, forecast.fill_bar_date):
            return None
        return _unresolved_forecast_row(forecast.forecast_id, "fill_bar_absent")
    # ⚠⚠ This path reads the SAME stored `strategy_signals.fill_price` as the
    # signal resolver, so it carries the same defect (#2414) — and one step
    # worse: the bracket below is a PERCENTAGE OF that price, so a superseded
    # fill silently puts both barriers on the old scale before `resolve_outcome`
    # rejects the pair. Refuse first, with the same closed code.
    if fill_price_is_superseded(series, fill_index=fill_index, fill_price=forecast.fill_price):
        return ForecastOutcomeRow.from_outcome(
            forecast.forecast_id,
            Outcome(
                outcome="unresolved",
                resolution_method="daily_bar",
                rule_set_version=PATH_RULE_SET_VERSION,
                reason="fill_price_superseded",
            ),
        )
    hundred = Decimal("100")
    levels = ExitLevels(
        take_profit=forecast.fill_price * (Decimal("1") + forecast.target_barrier_pct / hundred),
        stop_loss=forecast.fill_price * (Decimal("1") - forecast.stop_barrier_pct / hundred),
        max_hold_bars=forecast.horizon_market_days,
    )
    # ⚠ The fill bar survives but its open does not load. `resolve_outcome`
    # would raise; here it is one recorded refusal (#3189 finding 10). Placed
    # immediately before the resolver and after the bracket is built, matching
    # the signal resolver: nothing that reaches a terminal state today may be
    # relabelled, and an unbuildable bracket is a contract breach that stays
    # loud.
    if series.rows[fill_index].get("open") is None:
        return _unresolved_forecast_row(forecast.forecast_id, "fill_bar_open_absent")
    outcome = resolve_outcome(
        series=series,
        fill_index=fill_index,
        entry_price=forecast.fill_price,
        levels=levels,
        masked_bar_reasons=masked_bar_reasons if masked_bar_reasons is not None else _masked_reasons(series.rows),
        segment_end_index=segment_end_index(
            series,
            fill_index=fill_index,
            unresolved_breaks=unresolved_breaks,
        ),
    )
    if outcome.outcome == "unresolved" and outcome.reason == "window_truncated":
        return None
    return ForecastOutcomeRow.from_outcome(forecast.forecast_id, outcome)


def _read_cursor(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(_READ_CURSOR_SQL).fetchone()
    return 0 if row is None else int(row[0])


def _write_cursor(conn: psycopg.Connection[Any], last_forecast_id: int) -> None:
    conn.execute(_WRITE_CURSOR_SQL, (last_forecast_id,))


def _select(
    conn: psycopg.Connection[Any],
    *,
    after_forecast_id: int,
    limit: int,
    at_or_before_forecast_id: int | None = None,
) -> list[PendingForecast]:
    rows = conn.execute(
        _SELECT_SQL,
        {
            "resolver_version": RESOLVER_VERSION,
            "input_rule_set_version": QUARANTINE_RULE_SET_VERSION,
            "after_forecast_id": after_forecast_id,
            "at_or_before_forecast_id": at_or_before_forecast_id,
            "limit": limit,
        },
    ).fetchall()
    return [
        PendingForecast(
            forecast_id=int(row[0]),
            instrument_id=int(row[1]),
            fill_bar_date=row[2],
            fill_price=Decimal(row[3]),
            target_barrier_pct=Decimal(row[4]),
            stop_barrier_pct=Decimal(row[5]),
            horizon_market_days=int(row[6]),
        )
        for row in rows
    ]


def _select_round_robin(conn: psycopg.Connection[Any], *, cursor: int, limit: int) -> list[PendingForecast]:
    forecasts = _select(conn, after_forecast_id=cursor, limit=limit)
    if len(forecasts) < limit and cursor > 0:
        forecasts.extend(
            _select(
                conn,
                after_forecast_id=0,
                at_or_before_forecast_id=cursor,
                limit=limit - len(forecasts),
            )
        )
    return forecasts


def _store(conn: psycopg.Connection[Any], rows: Sequence[ForecastOutcomeRow]) -> int:
    written = 0
    for row in rows:
        inserted = conn.execute(
            _INSERT_SQL,
            (
                row.forecast_id,
                RESOLVER_VERSION,
                QUARANTINE_RULE_SET_VERSION,
                row.outcome,
                row.reason,
                row.exit_bar_date,
                row.exit_price,
                row.market_bars_held,
                row.gross_return_pct,
            ),
        ).fetchone()
        written += inserted is not None
    return written


def run_forecast_outcome_resolution(
    conn: psycopg.Connection[Any], *, batch_limit: int = DEFAULT_BATCH_LIMIT
) -> ForecastOutcomeResolutionReport:
    if not conn.autocommit:
        raise ValueError("run_forecast_outcome_resolution needs an autocommit connection")
    if batch_limit < 1:
        raise ValueError(f"batch_limit must be positive, got {batch_limit}")

    cursor = _read_cursor(conn)
    forecasts = _select_round_robin(conn, cursor=cursor, limit=batch_limit)
    by_instrument: dict[int, list[PendingForecast]] = {}
    for forecast in forecasts:
        by_instrument.setdefault(forecast.instrument_id, []).append(forecast)
    breaks = load_unresolved_breaks(conn, sorted(by_instrument))
    rows: list[ForecastOutcomeRow] = []
    immature = 0
    for instrument_id, instrument_forecasts in sorted(by_instrument.items()):
        series = load_masked_bars(conn, instrument_id).series
        masked = _masked_reasons(series.rows)
        raw_bar_exists = raw_bar_probe(conn, instrument_id)
        for forecast in instrument_forecasts:
            row = _resolve_forecast(
                forecast,
                series=series,
                unresolved_breaks=breaks.get(instrument_id, ()),
                raw_bar_exists=raw_bar_exists,
                masked_bar_reasons=masked,
            )
            if row is None:
                immature += 1
            else:
                rows.append(row)
    with conn.transaction():
        written = _store(conn, rows)
        if forecasts:
            # The final ID is deliberately not max(...). After a wrap the last
            # row is in the lower range; persisting it lets the next run move
            # through that range. Persisting the maximum would wrap to the same
            # low rows repeatedly and starve IDs between the two ranges.
            _write_cursor(conn, forecasts[-1].forecast_id)
    outcomes = Counter(row.outcome for row in rows)
    report = ForecastOutcomeResolutionReport(
        selected=len(forecasts),
        written=written,
        immature=immature,
        ambiguous=outcomes["ambiguous"],
        outcomes=dict(sorted(outcomes.items())),
    )
    logger.info(
        "strategy_forecast_outcome_resolution: selected=%d written=%d immature=%d ambiguous=%d limit=%d",
        report.selected,
        report.written,
        report.immature,
        report.ambiguous,
        batch_limit,
    )
    return report


__all__ = [
    "DEFAULT_BATCH_LIMIT",
    "ForecastOutcomeResolutionReport",
    "RESOLVER_VERSION",
    "run_forecast_outcome_resolution",
]
