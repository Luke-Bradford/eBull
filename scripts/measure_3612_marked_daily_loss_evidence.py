"""#3612 slice 2 spec evidence (docs/proposals/risk/2026-10-12-3612-marked-daily-loss.md), demo account.

Four read-only censuses over every stored row from ``--since`` (default 2026-09-15, the first stored
position marks, #3068) to ``--until`` (default: yesterday, New York):

1. **Close-capture lag.** For each NYSE session P, the lag from P's regular close (16:00 New York, 13:00 on
   a half day) to the first successful ``daily_portfolio_sync`` run started on P's New York date at or
   after it. This is the delay a capture taken on that run would carry.
2. **Snapshot continuity.** Every UTC date in range with no ``broker_account_equity_snapshots`` row, and
   every row whose stored mark count differs from its declared long + short count.
3. **After-close movement by instrument.** For each stored mark taken on a NYSE session's UTC date after
   that session's close, whether its ``close_rate`` equals ``price_daily.close`` for the session. A name
   that does not trade after the close shows no inequality.
4. **Unit conservation over consecutive snapshots.** Population: positions in either endpoint's marks,
   positions with a ``trade_events`` row in ``(o1, o2]`` and engine ownership rows claimed in ``(o1, o2]``.
   Checks ``units(d1) + open units - close units = units(d2)`` (0 where absent), and that every close has
   ``realized_pnl_usd``. Reported per population group.

Run: PYTHONPATH=. uv run python scripts/measure_3612_marked_daily_loss_evidence.py [--since D] [--until D]
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import psycopg

from app.config import settings
from app.services.market_calendar import us_market_status

_NY = ZoneInfo("America/New_York")

_FIRST_RUN_SQL = """
SELECT min(started_at) FROM job_runs
WHERE job_name = 'daily_portfolio_sync' AND status = 'success' AND started_at >= %s AND started_at < %s
"""

_SNAPSHOTS_SQL = """
SELECT s.snapshot_date, s.observed_at,
       s.official_direct_long_positions + s.official_direct_short_positions AS declared,
       (SELECT count(*) FROM broker_account_position_marks m
         WHERE m.environment = s.environment AND m.snapshot_date = s.snapshot_date) AS marks
FROM broker_account_equity_snapshots s
WHERE s.environment = 'demo' AND s.snapshot_date BETWEEN %s AND %s
ORDER BY s.snapshot_date
"""

_MARKS_VS_CLOSE_SQL = """
SELECT m.instrument_id, i.symbol, m.close_rate, pd.close
FROM broker_account_position_marks m
JOIN instruments i ON i.instrument_id = m.instrument_id
LEFT JOIN price_daily pd ON pd.instrument_id = m.instrument_id AND pd.price_date = %s
WHERE m.environment = 'demo' AND m.snapshot_date = %s
"""

_CONSERVATION_SQL = """
WITH pop AS (
    SELECT position_id, 'endpoint' AS grp FROM broker_account_position_marks
     WHERE environment = 'demo' AND snapshot_date IN (%(d1)s, %(d2)s)
    UNION
    SELECT position_id, 'event' FROM trade_events WHERE executed_at > %(o1)s AND executed_at <= %(o2)s
    UNION
    SELECT broker_position_id, 'ownership' FROM strategy_position_ownership
     WHERE claimed_at > %(o1)s AND claimed_at <= %(o2)s
)
SELECT p.position_id, array_agg(DISTINCT p.grp ORDER BY p.grp),
       COALESCE((SELECT units FROM broker_account_position_marks
                  WHERE environment = 'demo' AND snapshot_date = %(d1)s AND position_id = p.position_id), 0),
       COALESCE((SELECT units FROM broker_account_position_marks
                  WHERE environment = 'demo' AND snapshot_date = %(d2)s AND position_id = p.position_id), 0),
       COALESCE((SELECT sum(CASE e.event_kind WHEN 'open' THEN e.units ELSE -e.units END)
                   FROM trade_events e
                  WHERE e.position_id = p.position_id AND e.executed_at > %(o1)s AND e.executed_at <= %(o2)s
                    AND e.event_kind IN ('open', 'close')), 0),
       (SELECT count(*) FROM trade_events e
         WHERE e.position_id = p.position_id AND e.executed_at > %(o1)s AND e.executed_at <= %(o2)s
           AND e.event_kind = 'close' AND e.realized_pnl_usd IS NULL)
FROM pop p
WHERE p.position_id IS NOT NULL
GROUP BY p.position_id
"""


def _regular_close(day: date) -> datetime:
    return datetime.combine(day, time(13, 0) if us_market_status(day) == "half_day" else time(16, 0), tzinfo=_NY)


def _sessions(since: date, until: date) -> list[date]:
    return [
        since + timedelta(days=i)
        for i in range((until - since).days + 1)
        if us_market_status(since + timedelta(days=i)) != "closed"
    ]


def close_capture_lag(conn: psycopg.Connection[Any], since: date, until: date) -> None:
    lags: list[tuple[float, date]] = []
    missing: list[date] = []
    for day in _sessions(since, until):
        end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=_NY)
        row = conn.execute(_FIRST_RUN_SQL, (_regular_close(day), end)).fetchone()
        if row is None or row[0] is None:
            missing.append(day)
        else:
            lags.append(((row[0] - _regular_close(day)).total_seconds() / 60, day))
    lags.sort()
    print(f"[1] sessions {len(lags) + len(missing)}, with a run after the close {len(lags)}, without {len(missing)}")
    if lags:
        print(
            f"    lag minutes: min {lags[0][0]:.1f}, median {lags[len(lags) // 2][0]:.1f}, "
            f"max {lags[-1][0]:.1f} ({lags[-1][1]}); over 15 min: {sum(1 for lag, _ in lags if lag > 15)}"
        )
    for day in missing:
        print(f"    no run after the close on {day}")


def snapshot_continuity(conn: psycopg.Connection[Any], since: date, until: date) -> None:
    rows = conn.execute(_SNAPSHOTS_SQL, (since, until)).fetchall()
    present = {r[0] for r in rows}
    days = [since + timedelta(days=i) for i in range((until - since).days + 1)]
    gaps = [day for day in days if day not in present]
    mismatched = [r for r in rows if r[2] is None or r[2] != r[3]]
    print(f"[2] UTC dates {(until - since).days + 1}, with a snapshot {len(present)}, missing {len(gaps)}")
    for day in gaps:
        print(f"    missing {day}")
    print(f"    declared count NULL or different from stored marks: {len(mismatched)}")
    for r in mismatched:
        print(f"    {r[0]} declared {r[2]} marks {r[3]}")


def after_close_movement(conn: psycopg.Connection[Any], since: date, until: date) -> None:
    rows = conn.execute(_SNAPSHOTS_SQL, (since, until)).fetchall()
    total: Counter[str] = Counter()
    moved: Counter[str] = Counter()
    no_bar = 0
    considered = 0
    for snapshot_date, observed_at, _declared, _marks in rows:
        if us_market_status(snapshot_date) == "closed" or observed_at <= _regular_close(snapshot_date):
            continue
        considered += 1
        for _instrument_id, symbol, rate, close in conn.execute(_MARKS_VS_CLOSE_SQL, (snapshot_date, snapshot_date)):
            if close is None:
                no_bar += 1
                continue
            total[symbol] += 1
            if close != rate:
                moved[symbol] += 1
    print(f"[3] session-date snapshots taken after the close: {considered}; marks with no price_daily bar: {no_bar}")
    for symbol in sorted(total):
        print(f"    {symbol:10} marks {total[symbol]:3}  close_rate != session close {moved[symbol]:3}")


def unit_conservation(conn: psycopg.Connection[Any], since: date, until: date) -> None:
    snaps = conn.execute(_SNAPSHOTS_SQL, (since, until)).fetchall()
    by_group: Counter[str] = Counter()
    broken: list[str] = []
    pairs = 0
    for (d1, o1, _a, _b), (d2, o2, _c, _d) in zip(snaps, snaps[1:], strict=False):
        pairs += 1
        params = {"d1": d1, "d2": d2, "o1": o1, "o2": o2}
        for position_id, groups, u1, u2, net, missing_pnl in conn.execute(_CONSERVATION_SQL, params):
            key = "+".join(groups)
            by_group[key] += 1
            if u1 + net != u2 or missing_pnl:
                broken.append(
                    f"{d1}->{d2} position {position_id} ({key}): {u1} + {net} vs {u2}, null pnl {missing_pnl}"
                )
    print(f"[4] snapshot pairs {pairs}; position-pairs by population group: {dict(sorted(by_group.items()))}")
    print(f"    failing conservation or with a NULL-P&L close: {len(broken)}")
    for line in broken:
        print(f"    {line}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", type=date.fromisoformat, default=date(2026, 9, 15))
    parser.add_argument("--until", type=date.fromisoformat, default=None)
    args = parser.parse_args()
    until: date = args.until or datetime.now(UTC).astimezone(_NY).date() - timedelta(days=1)
    print(f"range {args.since} .. {until}")
    with psycopg.connect(settings.database_url) as conn:
        close_capture_lag(conn, args.since, until)
        snapshot_continuity(conn, args.since, until)
        after_close_movement(conn, args.since, until)
        unit_conservation(conn, args.since, until)


if __name__ == "__main__":
    main()
