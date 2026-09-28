"""#3471 slice 2c-ii — spec §8 "Per-trade deadline": the session arithmetic behind
``strategy_trades.exit_deadline_session`` (``sql/435``).

- **Horizon.** NYSE sessions counted from the leg's own fill session, which is session 0
  (§4 "Horizon"). The deadline is session ``horizon_days``.
- **Exit time.** The first position cycle at or after 15:00 UTC on the deadline session
  (§8). 15:00 UTC is inside the regular session in both EDT and EST, including a 13:00 ET
  half day. Once due it stays due, so a missed deadline (downtime, a refused close) is
  retried on every later cycle.

Writer: ``strategy_order_reconciliation._apply_detail``, in the same UPDATE that opens the
trade. Reader: ``strategy_position_manager.manage_owned_position``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg

from app.services.market_calendar import us_market_status

_NY: Final = ZoneInfo("America/New_York")

#: §8: "the exit fires at the first position cycle at or after 15:00 UTC on the deadline session".
TRIAL_EXIT_TIME_UTC: Final = time(15, 0)


def _next_session(after: date) -> date:
    # ⚠ Not moved into `market_calendar`: that module versions itself by the SHA of its own
    # bytes (`RULE_SET_VERSION`), so any edit there re-keys every frozen hash that embeds it.
    candidate = after + timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate += timedelta(days=1)
    return candidate


def fill_session(executed_at: datetime) -> date:
    """The NYSE session a fill belongs to: its New York civil date, or the next session when
    that date is not one (a weekend or holiday extended-hours fill)."""
    if executed_at.tzinfo is None or executed_at.utcoffset() is None:
        raise ValueError("executed_at must be timezone-aware")
    local = executed_at.astimezone(_NY).date()
    return local if us_market_status(local) != "closed" else _next_session(local)


def exit_deadline_session(fill: date, horizon_days: int) -> date:
    """Session ``horizon_days`` counted from ``fill`` as session 0."""
    if us_market_status(fill) == "closed":
        raise ValueError(f"{fill} is not a NYSE session")
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    session = fill
    for _ in range(horizon_days):
        session = _next_session(session)
    return session


def deadline_exit_due(deadline: date, observed_at: datetime) -> bool:
    """True from 15:00 UTC on the deadline session onwards."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return observed_at >= datetime.combine(deadline, TRIAL_EXIT_TIME_UTC, tzinfo=UTC)


def trial_exit_deadline(conn: psycopg.Connection[Any], *, strategy_trade_id: int, entry_order_id: int) -> date | None:
    """The deadline for a trial trade whose entry order just resolved, or None for a trade that
    is not a trial leg.

    The fill time is the STORED execution time (immutable once recorded) of the entry order's
    earliest position execution, so a re-reconciliation recomputes the same session.
    """
    horizon = conn.execute(
        "SELECT p.horizon_days FROM ai_trial_trade_links l JOIN ai_trial_pairs p ON p.pair_id = l.pair_id "
        "WHERE l.strategy_trade_id = %s",
        (strategy_trade_id,),
    ).fetchone()
    if horizon is None:
        return None
    filled = conn.execute(
        "SELECT min(execution_time) FROM strategy_order_position_executions WHERE order_id = %s",
        (entry_order_id,),
    ).fetchone()
    if filled is None or filled[0] is None:
        raise ValueError(f"trial trade {strategy_trade_id} resolved with no recorded execution time")
    return exit_deadline_session(fill_session(filled[0]), int(horizon[0]))


__all__ = [
    "TRIAL_EXIT_TIME_UTC",
    "deadline_exit_due",
    "exit_deadline_session",
    "fill_session",
    "trial_exit_deadline",
]
