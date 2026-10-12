"""#3612 slice 2 spec evidence: daily-loss anchor coverage per NYSE session (demo).

For every NYSE session D from 2026-09-16 to the last completed one, derives the spec's anchor:
R = the ``broker_account_equity_snapshots`` row dated the UTC day before D's New York midnight,
P = ``latest_completed_us_session(D 00:00 NY)``. Prints, per D: R's time (New York), declared vs
stored mark count, the number of demo ``trade_events`` in the quiet interval
(min(R, P's regular close), D 00:00 NY] (all demo positions, a superset of the engine book),
how many R marks equal ``price_daily``'s close for P, and the USD reprice R -> P close summed over
R's marks. ``price_daily`` stands in for the live P close here because this is retrospective; the
gate itself fetches the bar live.

Run: PYTHONPATH=. uv run python scripts/measure_3612_anchor_coverage.py
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import psycopg

from app.config import settings
from app.services.market_calendar import latest_completed_us_session, us_market_status

_NY = ZoneInfo("America/New_York")
_FIRST = date(2026, 9, 16)

_ANCHOR_SQL = """
SELECT s.observed_at, s.currency, s.official_direct_long_positions + s.official_direct_short_positions,
       (SELECT count(*) FROM broker_account_position_marks m
         WHERE m.environment = s.environment AND m.snapshot_date = s.snapshot_date)
FROM broker_account_equity_snapshots s
WHERE s.environment = 'demo' AND s.snapshot_date = %s
"""

_MARKS_SQL = """
SELECT m.position_id, m.units, m.close_rate, m.close_conversion_rate, pd.close
FROM broker_account_position_marks m
LEFT JOIN price_daily pd ON pd.instrument_id = m.instrument_id AND pd.price_date = %s
WHERE m.environment = 'demo' AND m.snapshot_date = %s
"""

_QUIET_SQL = "SELECT count(*) FROM trade_events WHERE executed_at > %s AND executed_at <= %s"


def main() -> None:
    today = datetime.now(UTC).astimezone(_NY).date()
    sessions = [
        _FIRST + timedelta(days=i)
        for i in range((today - _FIRST).days + 1)
        if us_market_status(_FIRST + timedelta(days=i)) != "closed"
    ]
    print("D          R_date     R_ny   P          declared marks quiet equal_close reprice_usd")
    with psycopg.connect(settings.database_url) as conn:
        for d in sessions:
            start = datetime.combine(d, time.min, tzinfo=_NY)
            r_date = start.astimezone(UTC).date() - timedelta(days=1)
            p = latest_completed_us_session(start)
            p_close = datetime.combine(p, time(13, 0) if us_market_status(p) == "half_day" else time(16, 0), tzinfo=_NY)
            anchor = conn.execute(_ANCHOR_SQL, (r_date,)).fetchone()
            if anchor is None:
                print(f"{d} {r_date} MISSING")
                continue
            observed_at, currency, declared, marks = anchor
            quiet = conn.execute(_QUIET_SQL, (min(observed_at, p_close), start)).fetchone()
            rows = conn.execute(_MARKS_SQL, (p, r_date)).fetchall()
            equal = sum(1 for r in rows if r[4] is not None and r[4] == r[2])
            reprice = sum(
                (r[1] * r[3] * (r[4] - r[2]) for r in rows if r[4] is not None),
                Decimal("0"),
            )
            print(
                f"{d} {r_date} {observed_at.astimezone(_NY):%H:%M}  {p} {declared!s:>8} {marks:>5}"
                f" {quiet[0] if quiet else '?':>5} {equal:>5}/{len(rows):<5} {reprice:>11.2f} {currency}"
            )


if __name__ == "__main__":
    main()
