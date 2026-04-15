"""
Return attribution service.

Decomposes the gross return of a closed position into additive components:

  gross_return = market_return
               + (sector_return - market_return)   [sector tilt]
               + model_alpha                        [stock selection within sector]
               + timing_alpha                       [v1 placeholder, always 0]
               + cost_drag                          [fees as fraction of cost basis]
               + residual                           [arithmetic closure term]

Attribution method: ``sector_relative_v1``

Design:
  - All arithmetic uses Decimal — never float.
  - Caller owns the connection; this module never opens or closes connections.
  - Functions are append-only writers: they INSERT new rows, never UPDATE.
  - When sector or market peer data are unavailable the corresponding component
    is zero, and the residual absorbs the difference.
  - timing_alpha is always ZERO in v1. The field exists for future TA-based
    entry/exit timing attribution.

Issue #202.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

ATTRIBUTION_METHOD = "sector_relative_v1"
ZERO = Decimal("0")
SUMMARY_WINDOWS: tuple[int, ...] = (30, 90, 365)

# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AttributionResult:
    """Attribution decomposition for a single closed position."""

    instrument_id: int
    hold_start: date
    hold_end: date
    hold_days: int
    gross_return_pct: Decimal
    market_return_pct: Decimal
    sector_return_pct: Decimal
    model_alpha_pct: Decimal
    timing_alpha_pct: Decimal
    cost_drag_pct: Decimal
    residual_pct: Decimal
    score_at_entry: Decimal | None
    score_components: dict[str, Any] | None
    entry_fill_id: int | None
    exit_fill_id: int | None
    recommendation_id: int | None


@dataclass(frozen=True)
class SummaryResult:
    """Rolling-window aggregate of attribution components."""

    window_days: int
    positions_attributed: int
    avg_gross_return_pct: Decimal | None
    avg_market_return_pct: Decimal | None
    avg_sector_return_pct: Decimal | None
    avg_model_alpha_pct: Decimal | None
    avg_timing_alpha_pct: Decimal | None
    avg_cost_drag_pct: Decimal | None


# ---------------------------------------------------------------------------
# Internal DB loaders
# ---------------------------------------------------------------------------


def _load_position_fills(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> list[dict[str, Any]]:
    """Load all fills for an instrument, joined to orders for action.

    Returns fills ordered by filled_at ascending.
    Each row: fill_id, order_id, filled_at, price, units, fees, action.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT f.fill_id,
                   f.order_id,
                   f.filled_at,
                   f.price,
                   f.units,
                   f.fees,
                   o.action
            FROM fills f
            JOIN orders o ON o.order_id = f.order_id
            WHERE o.instrument_id = %(iid)s
              AND f.units > 0
            ORDER BY f.filled_at ASC
            """,
            {"iid": instrument_id},
        )
        return cur.fetchall()


def _load_price_series(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Load price_daily rows for an instrument between two dates (inclusive).

    Returns rows ordered by price_date ascending.
    Each row: price_date, close.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT price_date, close
            FROM price_daily
            WHERE instrument_id = %(iid)s
              AND price_date BETWEEN %(start)s AND %(end)s
              AND close IS NOT NULL
            ORDER BY price_date ASC
            """,
            {"iid": instrument_id, "start": start_date, "end": end_date},
        )
        return cur.fetchall()


def _load_score_snapshot(
    conn: psycopg.Connection[Any],
    score_id: int,
) -> dict[str, Any] | None:
    """Load a single scores row by primary key.

    Returns None if the score_id does not exist.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT score_id,
                   total_score,
                   quality_score,
                   value_score,
                   turnaround_score,
                   momentum_score,
                   sentiment_score,
                   confidence_score,
                   model_version
            FROM scores
            WHERE score_id = %(sid)s
            """,
            {"sid": score_id},
        )
        return cur.fetchone()


def _load_sector_peers(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> list[int]:
    """Return instrument_ids in the same sector, excluding the subject instrument.

    Only instruments in coverage (any tier) with a known non-NULL sector are
    returned. Returns an empty list if the instrument has no sector or no peers.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT i2.instrument_id
            FROM instruments i1
            JOIN instruments i2
              ON i2.sector = i1.sector
             AND i2.instrument_id <> i1.instrument_id
             AND i2.sector IS NOT NULL
            WHERE i1.instrument_id = %(iid)s
              AND i1.sector IS NOT NULL
            """,
            {"iid": instrument_id},
        )
        rows = cur.fetchall()
        return [int(r["instrument_id"]) for r in rows]


def _load_recommendation_for_fills(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    """Load the most recent executed BUY or ADD recommendation for an instrument.

    Returns None if no executed buy/add recommendation exists.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT recommendation_id, score_id
            FROM trade_recommendations
            WHERE instrument_id = %(iid)s
              AND action IN ('BUY', 'ADD')
              AND status = 'executed'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Internal computation helpers
# ---------------------------------------------------------------------------


def _compute_average_return(prices: Sequence[dict[str, Any]]) -> Decimal:
    """Compute (last_close - first_close) / first_close from an ordered price list.

    Returns ZERO when fewer than two price rows are available or when
    first_close is zero (avoids division by zero).
    """
    if len(prices) < 2:
        return ZERO
    first = Decimal(str(prices[0]["close"]))
    last = Decimal(str(prices[-1]["close"]))
    if first == ZERO:
        return ZERO
    return (last - first) / first


def _compute_market_return(
    conn: psycopg.Connection[Any],
    start: date,
    end: date,
) -> Decimal:
    """Average return of all Tier 1 instruments over the hold period.

    Returns ZERO when no Tier 1 instruments have price data for the period.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT c.instrument_id
            FROM coverage c
            WHERE c.coverage_tier = 1
            """,
        )
        rows = cur.fetchall()

    tier1_ids = [int(r["instrument_id"]) for r in rows]
    if not tier1_ids:
        return ZERO

    returns: list[Decimal] = []
    for iid in tier1_ids:
        prices = _load_price_series(conn, iid, start, end)
        ret = _compute_average_return(prices)
        # Only include instruments that had actual price data for the period
        if len(prices) >= 2:
            returns.append(ret)

    if not returns:
        return ZERO
    return sum(returns, ZERO) / Decimal(len(returns))


def _compute_sector_return(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    start: date,
    end: date,
) -> Decimal:
    """Average return of same-sector instruments over the hold period.

    Excludes the subject instrument itself. Returns ZERO when no sector
    peers have price data for the period.
    """
    peer_ids = _load_sector_peers(conn, instrument_id)
    if not peer_ids:
        return ZERO

    returns: list[Decimal] = []
    for iid in peer_ids:
        prices = _load_price_series(conn, iid, start, end)
        if len(prices) >= 2:
            returns.append(_compute_average_return(prices))

    if not returns:
        return ZERO
    return sum(returns, ZERO) / Decimal(len(returns))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_attribution(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> AttributionResult | None:
    """Compute return attribution for a closed position.

    Returns None if:
      - No fills exist for the instrument.
      - No EXIT fills exist (position not yet closed).

    All monetary arithmetic uses Decimal. Market and sector returns are
    computed from price_daily; ZERO is used as a fallback when data are absent.
    """
    fills = _load_position_fills(conn, instrument_id)
    if not fills:
        return None

    entry_fills = [f for f in fills if str(f["action"]) in ("BUY", "ADD")]
    exit_fills = [f for f in fills if str(f["action"]) == "EXIT"]

    if not exit_fills:
        return None

    # Weighted average entry price (price * units weighted)
    entry_cost: Decimal = sum(
        (Decimal(str(f["price"])) * Decimal(str(f["units"])) for f in entry_fills),
        ZERO,
    )
    entry_units: Decimal = sum(
        (Decimal(str(f["units"])) for f in entry_fills),
        ZERO,
    )
    if entry_units == ZERO:
        return None
    avg_entry_price: Decimal = entry_cost / entry_units

    # Weighted average exit price
    exit_proceeds: Decimal = sum(
        (Decimal(str(f["price"])) * Decimal(str(f["units"])) for f in exit_fills),
        ZERO,
    )
    exit_units: Decimal = sum(
        (Decimal(str(f["units"])) for f in exit_fills),
        ZERO,
    )
    if exit_units == ZERO:
        return None
    avg_exit_price: Decimal = exit_proceeds / exit_units

    if avg_entry_price == ZERO:
        return None

    gross_return: Decimal = (avg_exit_price - avg_entry_price) / avg_entry_price

    # Hold period — use fill dates as date objects
    hold_start = min(f["filled_at"] for f in entry_fills)
    hold_end = max(f["filled_at"] for f in exit_fills)
    # filled_at may be datetime (timezone-aware) or date; normalise to date
    hold_start_date: date = hold_start.date() if isinstance(hold_start, datetime) else hold_start
    hold_end_date: date = hold_end.date() if isinstance(hold_end, datetime) else hold_end
    hold_days = (hold_end_date - hold_start_date).days

    # Cost drag: total fees / total entry cost
    total_fees: Decimal = sum(
        (Decimal(str(f["fees"])) for f in fills),
        ZERO,
    )
    cost_drag: Decimal = total_fees / entry_cost if entry_cost > ZERO else ZERO

    # Market and sector benchmark returns
    market_return = _compute_market_return(conn, hold_start_date, hold_end_date)
    sector_return = _compute_sector_return(conn, instrument_id, hold_start_date, hold_end_date)

    # Stock selection alpha vs sector
    model_alpha = gross_return - sector_return
    timing_alpha = ZERO  # v1 placeholder

    # Residual: arithmetic closure so components sum to gross_return
    # gross = market + (sector - market) + model_alpha + timing_alpha + cost_drag + residual
    # Simplifies to: residual = gross - (sector + model_alpha + timing_alpha + cost_drag)
    residual = gross_return - (sector_return + model_alpha + timing_alpha + cost_drag)

    # Score snapshot at entry
    rec = _load_recommendation_for_fills(conn, instrument_id)
    score_at_entry: Decimal | None = None
    score_components: dict[str, Any] | None = None
    recommendation_id: int | None = None
    score_id_for_entry: int | None = None

    if rec is not None:
        recommendation_id = int(rec["recommendation_id"])
        raw_score_id = rec["score_id"]
        if raw_score_id is not None:
            score_id_for_entry = int(raw_score_id)

    if score_id_for_entry is not None:
        snap = _load_score_snapshot(conn, score_id_for_entry)
        if snap is not None:
            raw_total = snap["total_score"]
            score_at_entry = Decimal(str(raw_total)) if raw_total is not None else None
            numeric_keys = (
                "quality_score",
                "value_score",
                "turnaround_score",
                "momentum_score",
                "sentiment_score",
                "confidence_score",
            )
            score_components = {}
            for k in numeric_keys:
                if k in snap:
                    score_components[k] = float(snap[k]) if snap[k] is not None else None
            if "model_version" in snap:
                score_components["model_version"] = snap["model_version"]

    # fill_id references: first entry fill and last exit fill
    entry_fill_id = int(entry_fills[0]["fill_id"]) if entry_fills else None
    exit_fill_id = int(exit_fills[-1]["fill_id"]) if exit_fills else None

    return AttributionResult(
        instrument_id=instrument_id,
        hold_start=hold_start_date,
        hold_end=hold_end_date,
        hold_days=hold_days,
        gross_return_pct=gross_return,
        market_return_pct=market_return,
        sector_return_pct=sector_return,
        model_alpha_pct=model_alpha,
        timing_alpha_pct=timing_alpha,
        cost_drag_pct=cost_drag,
        residual_pct=residual,
        score_at_entry=score_at_entry,
        score_components=score_components,
        entry_fill_id=entry_fill_id,
        exit_fill_id=exit_fill_id,
        recommendation_id=recommendation_id,
    )


def persist_attribution(
    conn: psycopg.Connection[Any],
    result: AttributionResult,
) -> None:
    """INSERT a single attribution result into return_attribution.

    Caller is responsible for the surrounding transaction if needed.
    """
    components_jsonb = Jsonb(result.score_components) if result.score_components is not None else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO return_attribution (
                instrument_id,
                hold_start,
                hold_end,
                hold_days,
                gross_return_pct,
                market_return_pct,
                sector_return_pct,
                model_alpha_pct,
                timing_alpha_pct,
                cost_drag_pct,
                residual_pct,
                score_at_entry,
                score_components,
                entry_fill_id,
                exit_fill_id,
                recommendation_id,
                attribution_method,
                computed_at
            ) VALUES (
                %(instrument_id)s,
                %(hold_start)s,
                %(hold_end)s,
                %(hold_days)s,
                %(gross_return_pct)s,
                %(market_return_pct)s,
                %(sector_return_pct)s,
                %(model_alpha_pct)s,
                %(timing_alpha_pct)s,
                %(cost_drag_pct)s,
                %(residual_pct)s,
                %(score_at_entry)s,
                %(score_components)s,
                %(entry_fill_id)s,
                %(exit_fill_id)s,
                %(recommendation_id)s,
                %(attribution_method)s,
                %(computed_at)s
            )
            """,
            {
                "instrument_id": result.instrument_id,
                "hold_start": result.hold_start,
                "hold_end": result.hold_end,
                "hold_days": result.hold_days,
                "gross_return_pct": result.gross_return_pct,
                "market_return_pct": result.market_return_pct,
                "sector_return_pct": result.sector_return_pct,
                "model_alpha_pct": result.model_alpha_pct,
                "timing_alpha_pct": result.timing_alpha_pct,
                "cost_drag_pct": result.cost_drag_pct,
                "residual_pct": result.residual_pct,
                "score_at_entry": result.score_at_entry,
                "score_components": components_jsonb,
                "entry_fill_id": result.entry_fill_id,
                "exit_fill_id": result.exit_fill_id,
                "recommendation_id": result.recommendation_id,
                "attribution_method": ATTRIBUTION_METHOD,
                "computed_at": datetime.now(tz=UTC),
            },
        )


def compute_attribution_summary(
    conn: psycopg.Connection[Any],
    window_days: int,
) -> SummaryResult:
    """Aggregate return_attribution rows within the rolling window.

    Returns a SummaryResult with None averages when no rows exist in the window.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS positions_attributed,
                AVG(gross_return_pct)  AS avg_gross_return_pct,
                AVG(market_return_pct) AS avg_market_return_pct,
                AVG(sector_return_pct) AS avg_sector_return_pct,
                AVG(model_alpha_pct)   AS avg_model_alpha_pct,
                AVG(timing_alpha_pct)  AS avg_timing_alpha_pct,
                AVG(cost_drag_pct)     AS avg_cost_drag_pct
            FROM return_attribution
            WHERE computed_at >= NOW() - (%(window_days)s || ' days')::INTERVAL
            """,
            {"window_days": window_days},
        )
        row = cur.fetchone()

    if row is None:
        return SummaryResult(
            window_days=window_days,
            positions_attributed=0,
            avg_gross_return_pct=None,
            avg_market_return_pct=None,
            avg_sector_return_pct=None,
            avg_model_alpha_pct=None,
            avg_timing_alpha_pct=None,
            avg_cost_drag_pct=None,
        )

    count = int(row["positions_attributed"])

    def _to_decimal(val: object) -> Decimal | None:
        return Decimal(str(val)) if val is not None else None

    return SummaryResult(
        window_days=window_days,
        positions_attributed=count,
        avg_gross_return_pct=_to_decimal(row["avg_gross_return_pct"]),
        avg_market_return_pct=_to_decimal(row["avg_market_return_pct"]),
        avg_sector_return_pct=_to_decimal(row["avg_sector_return_pct"]),
        avg_model_alpha_pct=_to_decimal(row["avg_model_alpha_pct"]),
        avg_timing_alpha_pct=_to_decimal(row["avg_timing_alpha_pct"]),
        avg_cost_drag_pct=_to_decimal(row["avg_cost_drag_pct"]),
    )


def persist_attribution_summary(
    conn: psycopg.Connection[Any],
    result: SummaryResult,
) -> None:
    """INSERT a single summary result into return_attribution_summary.

    Caller is responsible for the surrounding transaction if needed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO return_attribution_summary (
                window_days,
                positions_attributed,
                avg_gross_return_pct,
                avg_market_return_pct,
                avg_sector_return_pct,
                avg_model_alpha_pct,
                avg_timing_alpha_pct,
                avg_cost_drag_pct,
                computed_at
            ) VALUES (
                %(window_days)s,
                %(positions_attributed)s,
                %(avg_gross_return_pct)s,
                %(avg_market_return_pct)s,
                %(avg_sector_return_pct)s,
                %(avg_model_alpha_pct)s,
                %(avg_timing_alpha_pct)s,
                %(avg_cost_drag_pct)s,
                %(computed_at)s
            )
            """,
            {
                "window_days": result.window_days,
                "positions_attributed": result.positions_attributed,
                "avg_gross_return_pct": result.avg_gross_return_pct,
                "avg_market_return_pct": result.avg_market_return_pct,
                "avg_sector_return_pct": result.avg_sector_return_pct,
                "avg_model_alpha_pct": result.avg_model_alpha_pct,
                "avg_timing_alpha_pct": result.avg_timing_alpha_pct,
                "avg_cost_drag_pct": result.avg_cost_drag_pct,
                "computed_at": datetime.now(tz=UTC),
            },
        )
