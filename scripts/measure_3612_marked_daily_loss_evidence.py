"""#3612 slice 2 spec evidence (docs/proposals/risk/2026-10-12-3612-marked-daily-loss.md), demo account.

Five read-only censuses over every stored row from ``--since`` (default 2026-09-15, the first stored
position marks, #3068) to ``--until`` (default: yesterday, New York):

1. **Job-start lag after the close.** For each NYSE session P, the lag from P's regular close (16:00 New
   York, 13:00 on a half day) to the START of the first successful ``daily_portfolio_sync`` run on P's New
   York date at or after it. A successful run is not a successful capture, and the snapshot is observed
   after the run starts; this bounds neither from above.
2. **Snapshot continuity.** Every UTC date in range with no ``broker_account_equity_snapshots`` row, and
   every row whose stored mark count differs from its declared long + short count.
3. **Retained-row price comparison by instrument.** For each stored mark taken on a NYSE session's UTC
   date after that session's close, whether its ``close_rate`` equals today's ``price_daily.close`` for
   the session. ``price_daily`` is back-adjusted and overwritten (sql/479 header), so an inequality is not
   by itself after-close movement. Every mark is counted: missing instrument metadata and missing bars are
   reported, and the counts reconcile.
4. **Unit conservation over consecutive snapshots.** Population: positions in either endpoint's marks,
   positions with a ``trade_events`` row in ``(o1, o2]``, engine ownership rows claimed in ``(o1, o2]``, and
   every engine ownership row claimed by ``o2`` (``owned_by_o2``, a superset of the gate's population E).
   Each position's window runs from its mark time at d1 to its mark time at d2 (``pnl_timestamp``, else the
   snapshot's ``observed_at`` where the position is absent), as the gate cuts it. Checks
   ``units(d1) + open units - close units = units(d2)`` and ``amount(d1) + open investment_usd - close
   investment_usd = amount(d2)`` (0 where absent), and that every close has ``realized_pnl_usd`` and zero
   ``fees_usd``. Reports events falling between a mark time and its snapshot's receipt time.
5. **Point claims.** The live pool limits; R times of day; the first stored mark date; the full event and
   ownership history of every position failing section 4; the closes of positions present in the preceding
   snapshot, with ``realised - U_S * closed/held - (price - close_rate) * closed * conversion``; and the
   age of each stored mark's ``pnl_timestamp`` against its snapshot's ``observed_at``.

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

_RUN_COUNT_SQL = """
SELECT count(*) FROM job_runs
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
LEFT JOIN instruments i ON i.instrument_id = m.instrument_id
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
    UNION
    SELECT broker_position_id, 'owned_by_o2' FROM strategy_position_ownership WHERE claimed_at <= %(o2)s
)
SELECT p.position_id, array_agg(DISTINCT p.grp ORDER BY p.grp),
       COALESCE(a.units, 0), COALESCE(b.units, 0),
       COALESCE((SELECT sum(CASE e.event_kind WHEN 'open' THEN e.units ELSE -e.units END)
                   FROM trade_events e
                  WHERE e.position_id = p.position_id AND e.executed_at > w.t1 AND e.executed_at <= w.t2), 0),
       (SELECT count(*) FROM trade_events e
         WHERE e.position_id = p.position_id AND e.executed_at > w.t1 AND e.executed_at <= w.t2
           AND e.event_kind = 'close' AND (e.realized_pnl_usd IS NULL OR e.fees_usd IS DISTINCT FROM 0)),
       COALESCE(a.amount, 0), COALESCE(b.amount, 0),
       COALESCE((SELECT sum(CASE e.event_kind WHEN 'open' THEN e.investment_usd ELSE -e.investment_usd END)
                   FROM trade_events e
                  WHERE e.position_id = p.position_id AND e.executed_at > w.t1 AND e.executed_at <= w.t2), 0),
       (SELECT count(*) FROM trade_events e
         WHERE e.position_id = p.position_id
           AND ((e.executed_at > w.t1 AND e.executed_at <= %(o1)s)
                OR (e.executed_at > w.t2 AND e.executed_at <= %(o2)s)))
FROM pop p
LEFT JOIN broker_account_position_marks a
  ON a.environment = 'demo' AND a.snapshot_date = %(d1)s AND a.position_id = p.position_id
LEFT JOIN broker_account_position_marks b
  ON b.environment = 'demo' AND b.snapshot_date = %(d2)s AND b.position_id = p.position_id
CROSS JOIN LATERAL (SELECT COALESCE(a.pnl_timestamp, %(o1)s) AS t1, COALESCE(b.pnl_timestamp, %(o2)s) AS t2) w
WHERE p.position_id IS NOT NULL
GROUP BY p.position_id, a.units, b.units, a.amount, b.amount, w.t1, w.t2
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
    runs: list[int] = []
    missing: list[date] = []
    for day in _sessions(since, until):
        end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=_NY)
        row = conn.execute(_FIRST_RUN_SQL, (_regular_close(day), end)).fetchone()
        count = conn.execute(_RUN_COUNT_SQL, (_regular_close(day), end)).fetchone()
        runs.append(int(count[0]) if count else 0)
        if row is None or row[0] is None:
            missing.append(day)
        else:
            lags.append(((row[0] - _regular_close(day)).total_seconds() / 60, day))
    lags.sort()
    print(f"[1] sessions {len(lags) + len(missing)}, with a successful run started after the close {len(lags)}")
    if lags:
        print(
            f"    job-start lag minutes: min {lags[0][0]:.1f}, median {lags[len(lags) // 2][0]:.1f}, "
            f"max {lags[-1][0]:.1f} ({lags[-1][1]}); over 15 min: {sum(1 for lag, _ in lags if lag > 15)}"
        )
    if runs:
        ordered = sorted(runs)
        print(f"    successful runs per session after the close: min {ordered[0]}, median {ordered[len(ordered) // 2]}")
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
    no_metadata = 0
    seen = 0
    considered = 0
    for snapshot_date, observed_at, _declared, _marks in rows:
        if us_market_status(snapshot_date) == "closed" or observed_at <= _regular_close(snapshot_date):
            continue
        considered += 1
        for instrument_id, symbol, rate, close in conn.execute(_MARKS_VS_CLOSE_SQL, (snapshot_date, snapshot_date)):
            seen += 1
            if symbol is None:
                no_metadata += 1
                symbol = f"id {instrument_id}"
            if close is None:
                no_bar += 1
                continue
            total[symbol] += 1
            if close != rate:
                moved[symbol] += 1
    print(
        f"[3] session-date snapshots taken after the close: {considered}; marks {seen} = compared "
        f"{sum(total.values())} + no price_daily bar {no_bar}; of them with no instrument metadata: {no_metadata}"
    )
    for symbol in sorted(total):
        print(f"    {symbol:10} marks {total[symbol]:3}  close_rate != session close {moved[symbol]:3}")


def unit_conservation(conn: psycopg.Connection[Any], since: date, until: date) -> list[int]:
    snaps = conn.execute(_SNAPSHOTS_SQL, (since, until)).fetchall()
    by_group: Counter[str] = Counter()
    broken: list[str] = []
    failing: list[int] = []
    amount_broken: list[str] = []
    straddling = 0
    pairs = 0
    for (d1, o1, _a, _b), (d2, o2, _c, _d) in zip(snaps, snaps[1:], strict=False):
        pairs += 1
        params = {"d1": d1, "d2": d2, "o1": o1, "o2": o2}
        for position_id, groups, u1, u2, net, bad_close, a1, a2, inv, straddle in conn.execute(
            _CONSERVATION_SQL, params
        ):
            key = "+".join(groups)
            by_group[key] += 1
            straddling += int(straddle)
            if u1 + net != u2 or bad_close:
                failing.append(int(position_id))
                broken.append(
                    f"{d1}->{d2} position {position_id} ({key}): units {u1} + {net} vs {u2}, bad close {bad_close}"
                )
            if a1 + inv != a2:
                amount_broken.append(f"{d1}->{d2} position {position_id} ({key}): amount {a1} + {inv} vs {a2}")
    print(f"[4] snapshot pairs {pairs}; position-pairs by population group: {dict(sorted(by_group.items()))}")
    print(f"    events between a position's mark time and its snapshot's receipt time: {straddling}")
    print(f"    failing unit conservation, or with a close lacking realised or zero fees: {len(broken)}")
    for line in broken:
        print(f"    {line}")
    print(f"    failing cost-basis conservation (amount + open - close investment_usd): {len(amount_broken)}")
    for line in amount_broken:
        print(f"    {line}")
    return failing


_POOL_SQL = """
SELECT risk_profile, max_daily_loss_pct, capital_limit FROM strategy_paper_pool_events
ORDER BY strategy_paper_pool_event_id DESC LIMIT 1
"""

_R_TIMES_SQL = """
SELECT min((observed_at AT TIME ZONE 'America/New_York')::time),
       max((observed_at AT TIME ZONE 'America/New_York')::time),
       (SELECT min(snapshot_date) FROM broker_account_position_marks WHERE environment = 'demo')
FROM broker_account_equity_snapshots s
WHERE s.environment = 'demo' AND s.snapshot_date BETWEEN %s AND %s AND extract(isodow FROM s.snapshot_date) < 6
  AND (s.observed_at AT TIME ZONE 'America/New_York')::time >= time '16:00'
"""

_HISTORY_SQL = """
SELECT 'event', e.event_kind, e.executed_at, e.units, e.realized_pnl_usd, e.instrument_id
FROM trade_events e WHERE e.position_id = %(p)s
UNION ALL
SELECT 'ownership', o.broker_position_id::text, o.claimed_at, NULL, NULL, NULL
FROM strategy_position_ownership o WHERE o.broker_position_id = %(p)s
UNION ALL
SELECT 'mark', m.snapshot_date::text, NULL, m.units, NULL, m.instrument_id
FROM broker_account_position_marks m WHERE m.environment = 'demo' AND m.position_id = %(p)s
ORDER BY 3 NULLS LAST
"""

_IDENTITY_SQL = """
WITH s AS (
    SELECT snapshot_date AS d, observed_at AS o, lead(observed_at) OVER (ORDER BY snapshot_date) AS o2
    FROM broker_account_equity_snapshots WHERE environment = 'demo' AND snapshot_date BETWEEN %s AND %s
)
SELECT e.position_id, s.o, LEAST(s.o2, %s), e.executed_at, e.fees_usd, m.asset_currency_id, m.total_fees,
       e.realized_pnl_usd - m.unrealized_pnl * e.units / m.units
         - (e.price - m.close_rate) * e.units * m.close_conversion_rate AS residual
FROM s
JOIN broker_account_position_marks m ON m.environment = 'demo' AND m.snapshot_date = s.d
JOIN trade_events e ON e.position_id = m.position_id AND e.event_kind = 'close'
 AND e.executed_at > s.o AND e.executed_at <= LEAST(s.o2, %s)
ORDER BY e.executed_at
"""

_PNL_AGE_SQL = """
SELECT count(*), count(m.pnl_timestamp),
       min(extract(epoch FROM s.observed_at - m.pnl_timestamp)),
       max(extract(epoch FROM s.observed_at - m.pnl_timestamp)),
       count(*) FILTER (WHERE m.total_fees = 0), count(*) FILTER (WHERE m.total_fees <> 0),
       count(*) FILTER (WHERE m.total_fees IS NULL),
       count(*) FILTER (WHERE m.asset_currency_id = 1), count(*) FILTER (WHERE m.asset_currency_id <> 1),
       count(*) FILTER (WHERE m.asset_currency_id IS NULL),
       count(*) FILTER (WHERE m.is_buy), count(*) FILTER (WHERE NOT m.is_buy), count(*) FILTER (WHERE m.is_buy IS NULL)
FROM broker_account_position_marks m
JOIN broker_account_equity_snapshots s USING (environment, snapshot_date)
WHERE m.environment = 'demo' AND m.snapshot_date BETWEEN %s AND %s
"""

_FEES_SPLIT_SQL = """
SELECT format('marks: first date with total_fees present %s, NULL total_fees on or after it %s',
              min(snapshot_date) FILTER (WHERE total_fees IS NOT NULL),
              count(*) FILTER (WHERE total_fees IS NULL AND snapshot_date >= (
                  SELECT min(snapshot_date) FROM broker_account_position_marks
                   WHERE environment = 'demo' AND total_fees IS NOT NULL)))
FROM broker_account_position_marks WHERE environment = 'demo'
UNION ALL
SELECT format('trade_events %s: %s rows, fees_usd NULL %s, zero %s', event_kind, count(*),
              count(*) FILTER (WHERE fees_usd IS NULL), count(*) FILTER (WHERE fees_usd = 0))
FROM trade_events GROUP BY event_kind
"""

_EVENTS_AUDIT_SQL = """
SELECT count(*),
       count(*) FILTER (WHERE units IN ('NaN', 'Infinity', '-Infinity') OR price IN ('NaN', 'Infinity', '-Infinity')
                        OR fees_usd IN ('NaN', 'Infinity', '-Infinity')
                        OR realized_pnl_usd IN ('NaN', 'Infinity', '-Infinity')),
       count(*) FILTER (WHERE fees_usd = 0), count(*) FILTER (WHERE fees_usd <> 0),
       count(*) FILTER (WHERE fees_usd IS NULL),
       count(*) FILTER (WHERE event_kind = 'close'),
       count(*) FILTER (WHERE event_kind = 'close' AND realized_pnl_usd IS NULL)
FROM trade_events
"""


def point_claims(conn: psycopg.Connection[Any], since: date, until: date, failing: list[int]) -> None:
    print(
        f"[5] latest pool event (risk_profile, max_daily_loss_pct, capital_limit): {conn.execute(_POOL_SQL).fetchone()}"
    )
    r = conn.execute(_R_TIMES_SQL, (since, until)).fetchone()
    if r is not None:
        print(f"    weekday snapshots after 16:00 New York: earliest {r[0]}, latest {r[1]}; first stored mark {r[2]}")
    for position_id in failing:
        print(f"    history of position {position_id} (kind, detail, at, units, realised, instrument):")
        for row in conn.execute(_HISTORY_SQL, {"p": position_id}):
            print(f"      {row}")
    cutoff = datetime.combine(until + timedelta(days=1), time.min, tzinfo=UTC)
    rows = conn.execute(_IDENTITY_SQL, (since, until, cutoff, cutoff)).fetchall()
    worst = max((abs(row[7]) for row in rows), default=None)
    print(f"    closes of positions in the preceding snapshot: {len(rows)}; max |residual| {worst}")
    for row in rows:
        print(
            f"      position {row[0]} in ({row[1]:%Y-%m-%d %H:%M}Z, {row[2]:%Y-%m-%d %H:%M}Z]"
            f" at {row[3]:%Y-%m-%d %H:%M}Z: fees {row[4]}, asset currency {row[5]}, anchor total_fees {row[6]},"
            f" residual {row[7]:.4f}"
        )
    age = conn.execute(_PNL_AGE_SQL, (since, until)).fetchone()
    if age is not None:
        print(f"    marks {age[0]}, with pnl_timestamp {age[1]}, age vs observed_at {age[2]}..{age[3]} s")
        print(f"    total_fees zero {age[4]} / non-zero {age[5]} / NULL {age[6]}")
        print(f"    asset currency USD {age[7]} / other {age[8]} / NULL {age[9]}")
        print(f"    is_buy true {age[10]} / false {age[11]} / NULL {age[12]}")
    fees = conn.execute(_FEES_SPLIT_SQL).fetchall()
    for row in fees:
        print(f"    {row[0]}")
    ev = conn.execute(_EVENTS_AUDIT_SQL).fetchone()
    if ev is not None:
        print(
            f"    trade_events {ev[0]}: non-finite units/price/fees/realised {ev[1]}; fees_usd zero {ev[2]}"
            f" / non-zero {ev[3]} / NULL {ev[4]}; closes {ev[5]} with NULL realised {ev[6]}"
        )


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
        failing = unit_conservation(conn, args.since, until)
        point_claims(conn, args.since, until, failing)


if __name__ == "__main__":
    main()
