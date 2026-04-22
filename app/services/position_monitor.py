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


@dataclass(frozen=True)
class PersistStats:
    """Aggregate stats from one persist_position_alerts invocation."""

    opened: int
    resolved: int
    unchanged: int


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


def persist_position_alerts(
    conn: psycopg.Connection[Any],
    result: MonitorResult,
) -> PersistStats:
    """Reconcile open breach episodes against the current MonitorResult.

    Contract: for each (instrument_id, alert_type) pair:
      - current breach AND no open episode    -> INSERT new row
      - current breach AND open episode       -> no-op (still breaching)
      - no current breach AND open episode    -> UPDATE resolved_at = now()
      - no current breach AND no open episode -> no-op

    Runs inside a single ``conn.transaction()`` block — caller MUST NOT
    hold an outer transaction, because psycopg v3 treats nested
    ``conn.transaction()`` as a savepoint and the outer commit path is
    the caller's responsibility. ``monitor_positions_job`` opens a
    fresh connection, invokes ``check_position_health`` (read-only, no
    BEGIN), then calls this writer — the ``conn.transaction()`` block
    here IS the outer transaction and commits on clean exit.

    Concurrency: the INSERT path tolerates partial-unique-index
    conflicts via ``ON CONFLICT DO NOTHING``. The resolve path runs
    ``WHERE resolved_at IS NULL`` so a row resolved by a concurrent
    writer between the diff read and the UPDATE is a silent no-op.
    Both guards are defensive — the scheduler serialises
    ``monitor_positions_job`` via ``max_instances=1`` + per-job
    ``threading.Lock`` (app/jobs/runtime.py:224,243).
    """
    current: dict[tuple[int, str], MonitorAlert] = {(a.instrument_id, a.alert_type): a for a in result.alerts}

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT instrument_id, alert_type FROM position_alerts WHERE resolved_at IS NULL")
            open_pairs: set[tuple[int, str]] = {
                (int(row["instrument_id"]), str(row["alert_type"])) for row in cur.fetchall()
            }

            to_open = set(current.keys()) - open_pairs
            to_resolve = open_pairs - set(current.keys())
            unchanged = len(open_pairs & set(current.keys()))

            opened = 0
            for key in to_open:
                alert = current[key]
                cur.execute(
                    """
                    INSERT INTO position_alerts
                        (instrument_id, alert_type, detail, current_bid)
                    VALUES (%(instrument_id)s, %(alert_type)s, %(detail)s, %(current_bid)s)
                    ON CONFLICT (instrument_id, alert_type) WHERE resolved_at IS NULL
                    DO NOTHING
                    """,
                    {
                        "instrument_id": alert.instrument_id,
                        "alert_type": alert.alert_type,
                        "detail": alert.detail,
                        "current_bid": alert.current_bid,
                    },
                )
                # rowcount == 1 on insert, 0 on ON CONFLICT DO NOTHING (race backstop).
                if cur.rowcount == 1:
                    opened += 1

            resolved = 0
            for instrument_id, alert_type in to_resolve:
                cur.execute(
                    """
                    UPDATE position_alerts
                    SET resolved_at = now()
                    WHERE instrument_id = %(instrument_id)s
                      AND alert_type = %(alert_type)s
                      AND resolved_at IS NULL
                    """,
                    {"instrument_id": instrument_id, "alert_type": alert_type},
                )
                if cur.rowcount == 1:
                    resolved += 1

    return PersistStats(opened=opened, resolved=resolved, unchanged=unchanged)
