"""#3612 slice 2 spec evidence: unit conservation between consecutive stored account snapshots (demo).

For every position present in either of two consecutive ``broker_account_equity_snapshots`` rows:
``units(d1) + open units - close units`` over ``trade_events`` executed in ``(observed_at(d1), observed_at(d2)]``
must equal ``units(d2)`` (0 where absent). Prints the pair count and how many conserve.

Run: PYTHONPATH=. uv run python scripts/measure_3612_unit_conservation.py
"""

from __future__ import annotations

import psycopg

from app.config import settings

_SQL = """
WITH snaps AS (
    SELECT snapshot_date AS d1, observed_at AS o1,
           lead(snapshot_date) OVER w AS d2, lead(observed_at) OVER w AS o2
    FROM broker_account_equity_snapshots
    WHERE environment = 'demo' AND snapshot_date >= '2026-09-15'
    WINDOW w AS (ORDER BY snapshot_date)
),
pairs AS (
    SELECT DISTINCT s.d1, s.o1, s.d2, s.o2, m.position_id
    FROM snaps s
    JOIN broker_account_position_marks m
      ON m.environment = 'demo' AND m.snapshot_date IN (s.d1, s.d2)
    WHERE s.d2 IS NOT NULL
)
SELECT p.d1, p.position_id,
       COALESCE(a.units, 0) AS u1,
       COALESCE(b.units, 0) AS u2,
       COALESCE(SUM(e.units) FILTER (WHERE e.event_kind = 'open'), 0)
         - COALESCE(SUM(e.units) FILTER (WHERE e.event_kind = 'close'), 0) AS net
FROM pairs p
LEFT JOIN broker_account_position_marks a
  ON a.environment = 'demo' AND a.snapshot_date = p.d1 AND a.position_id = p.position_id
LEFT JOIN broker_account_position_marks b
  ON b.environment = 'demo' AND b.snapshot_date = p.d2 AND b.position_id = p.position_id
LEFT JOIN trade_events e
  ON e.position_id = p.position_id AND e.executed_at > p.o1 AND e.executed_at <= p.o2
GROUP BY p.d1, p.position_id, a.units, b.units
"""


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SQL).fetchall()
    broken = [r for r in rows if r[2] + r[4] != r[3]]
    print(f"pairs {len(rows)} conserved {len(rows) - len(broken)}")
    for d1, position_id, u1, u2, net in broken:
        print(f"  {d1} position {position_id}: {u1} + {net} != {u2}")


if __name__ == "__main__":
    main()
