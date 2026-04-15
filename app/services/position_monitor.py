"""Position monitor service — intraday SL/TP/thesis-break detection.

Checks all open positions against the latest quotes and thesis data to
surface breaches that the daily 05:30 broker sync would otherwise miss.

Design choices:
  - READ-ONLY: no state mutations, no orders placed.
  - NULL SL/TP/red_flag = skip that check (never block on missing data).
  - filter WHERE current_units > 0 to exclude liquidated positions.
  - LATERAL joins for quotes/theses/broker_positions to avoid JOIN fan-out.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Mirror portfolio.EXIT_RED_FLAG_THRESHOLD — exported as Decimal for
# consistent comparison against Decimal values returned from the DB.
EXIT_RED_FLAG_THRESHOLD = Decimal("0.80")

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

AlertType = Literal["sl_breach", "tp_breach", "thesis_break"]


@dataclass(frozen=True)
class MonitorAlert:
    """A single position health alert."""

    instrument_id: int
    symbol: str
    alert_type: AlertType
    detail: str
    current_bid: Decimal | None = None


@dataclass(frozen=True)
class MonitorResult:
    """Aggregate result returned by check_position_health."""

    positions_checked: int
    alerts: tuple[MonitorAlert, ...] = ()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_position_health(conn: psycopg.Connection[Any]) -> MonitorResult:
    """Check all open positions against latest quotes and thesis data.

    For each open position:
      - sl_breach:    bid is not None AND sl is not None AND bid < sl
      - tp_breach:    bid is not None AND tp is not None AND bid >= tp
      - thesis_break: red_flag is not None AND red_flag >= EXIT_RED_FLAG_THRESHOLD

    Returns a MonitorResult with the count of positions checked and any alerts
    raised. NULL SL/TP/red_flag values are silently skipped — never block on
    missing data (prevention log: missing data on hard-rule path).

    This function is read-only. It neither places orders nor mutates any state.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                p.instrument_id,
                i.symbol,
                bp.stop_loss_rate,
                bp.take_profit_rate,
                q.bid,
                t.red_flag_score
            FROM positions p
            JOIN instruments i USING (instrument_id)
            -- Latest broker_positions row for this position's SL/TP.
            -- LATERAL prevents JOIN fan-out (prevention log: JOIN fan-out inflates
            -- aggregates — broker_positions has multiple rows per instrument).
            LEFT JOIN LATERAL (
                SELECT stop_loss_rate, take_profit_rate
                FROM broker_positions
                WHERE instrument_id = p.instrument_id
                ORDER BY updated_at DESC
                LIMIT 1
            ) bp ON TRUE
            -- Latest quote bid/ask.
            LEFT JOIN LATERAL (
                SELECT bid
                FROM quotes
                WHERE instrument_id = p.instrument_id
                ORDER BY quoted_at DESC
                LIMIT 1
            ) q ON TRUE
            -- Latest thesis red_flag_score.
            LEFT JOIN LATERAL (
                SELECT red_flag_score
                FROM theses
                WHERE instrument_id = p.instrument_id
                ORDER BY created_at DESC
                LIMIT 1
            ) t ON TRUE
            WHERE p.current_units > 0
            """
        )
        rows = cur.fetchall()

    alerts: list[MonitorAlert] = []

    for row in rows:
        instrument_id: int = row["instrument_id"]
        symbol: str = row["symbol"]

        bid_raw = row["bid"]
        sl_raw = row["stop_loss_rate"]
        tp_raw = row["take_profit_rate"]
        red_flag_raw = row["red_flag_score"]

        bid = Decimal(str(bid_raw)) if bid_raw is not None else None
        sl = Decimal(str(sl_raw)) if sl_raw is not None else None
        tp = Decimal(str(tp_raw)) if tp_raw is not None else None
        red_flag = Decimal(str(red_flag_raw)) if red_flag_raw is not None else None

        # SL breach: current bid has fallen below the stop-loss rate.
        if bid is not None and sl is not None and bid < sl:
            alerts.append(
                MonitorAlert(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alert_type="sl_breach",
                    detail=f"bid={bid} < stop_loss={sl}",
                    current_bid=bid,
                )
            )

        # TP breach: current bid has reached or exceeded the take-profit rate.
        if bid is not None and tp is not None and bid >= tp:
            alerts.append(
                MonitorAlert(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alert_type="tp_breach",
                    detail=f"bid={bid} >= take_profit={tp}",
                    current_bid=bid,
                )
            )

        # Thesis break: red flag score at or above the exit threshold.
        if red_flag is not None and red_flag >= EXIT_RED_FLAG_THRESHOLD:
            alerts.append(
                MonitorAlert(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alert_type="thesis_break",
                    detail=f"red_flag={red_flag} >= threshold={EXIT_RED_FLAG_THRESHOLD}",
                    current_bid=bid,
                )
            )

    return MonitorResult(positions_checked=len(rows), alerts=tuple(alerts))
