"""Re-resolve unresolved instruments on trade_events rows (#1593 spec §17).

``trade_events.instrument_id`` is NULL when the eToro instrument id was
absent from the ``instruments`` universe at ingest time (delisted /
removed instruments in deep history). After a universe sync introduces
the missing rows, this runbook stamps the FK from the always-kept
``etoro_instrument_id`` — instruments.instrument_id IS the eToro id, so
resolution is a pure existence join. No fetch, no delete.

Operator usage::

    uv run python -m app.runbooks.trade_events_reresolve_instruments [--apply]

Default mode is dry-run (prints the resolvable rows; no DB writes).
"""

from __future__ import annotations

import argparse

import psycopg
import psycopg.rows

from app.config import settings

_CANDIDATES_SQL = """
    SELECT te.event_id, te.position_id, te.event_kind, te.etoro_instrument_id,
           i.symbol
    FROM trade_events te
    JOIN instruments i ON i.instrument_id = te.etoro_instrument_id
    WHERE te.instrument_id IS NULL
    ORDER BY te.event_id
"""

_APPLY_SQL = """
    UPDATE trade_events te
    SET instrument_id = te.etoro_instrument_id
    FROM instruments i
    WHERE te.instrument_id IS NULL
      AND i.instrument_id = te.etoro_instrument_id
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write the resolved ids (default: dry-run)")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            rows = cur.execute(_CANDIDATES_SQL).fetchall()
        if not rows:
            print("nothing to resolve — no trade_events rows with NULL instrument_id matching the universe")
            return 0
        for row in rows:
            print(
                f"event {row['event_id']} (position {row['position_id']} {row['event_kind']}): "
                f"etoro_instrument_id {row['etoro_instrument_id']} -> {row['symbol']}"
            )
        if not args.apply:
            print(f"dry-run: {len(rows)} row(s) resolvable — re-run with --apply to write")
            return 0
        result = conn.execute(_APPLY_SQL)
        conn.commit()
        print(f"applied: {result.rowcount} row(s) resolved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
