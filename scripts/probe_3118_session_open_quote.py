"""#3118 — does the provider's price stamp advance at the venue open?

    PYTHONPATH=. uv run python -m scripts.probe_3118_session_open_quote --instrument-id 3417

Read-only. Samples ``quotes.quoted_at`` for one instrument once a minute and
reports when it first carries a stamp at or after that venue's session open.

⚠ WHY IT EXISTS.  The #3118 census can show where the HOURLY lane's fetches are
and what they returned; it has no sample between the last pre-open fetch and the
first in-session one, by construction.  So the one thing the spec could not
settle is whether eToro's ``date`` for an RTH instrument advances AT the open or
some minutes into it — which decides whether the new five-minute producer
actually closes the window or merely narrows it.

⚠ It observes; it does not fetch.  The advance it is waiting for is
``core_candidate_quote_refresh``'s, so a run with that job stopped proves
nothing about the provider.
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, date, datetime

import psycopg

from app.config import settings
from app.services.market_session_support import venue_calendar_for

_ROW = """
SELECT i.symbol, e.asset_class, q.quoted_at, now()
FROM instruments i
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
LEFT JOIN quotes q ON q.instrument_id = i.instrument_id
WHERE i.instrument_id = %(instrument_id)s
"""


def _session_open_utc(asset_class: str | None, on: date) -> datetime | None:
    calendar = venue_calendar_for(asset_class)
    if calendar is None:
        return None
    return datetime.combine(on, calendar.session_open, tzinfo=calendar.tz).astimezone(UTC)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument-id", type=int, default=3417)
    parser.add_argument("--minutes", type=int, default=70, help="how long to keep sampling")
    args = parser.parse_args()

    open_utc: datetime | None = None
    deadline = time.monotonic() + args.minutes * 60
    while time.monotonic() < deadline:
        with psycopg.connect(settings.database_url) as conn:
            row = conn.execute(_ROW, {"instrument_id": args.instrument_id}).fetchone()
        if row is None:
            print(f"instrument {args.instrument_id} is unknown", flush=True)
            return
        symbol, asset_class, quoted_at, now = row
        if open_utc is None:
            open_utc = _session_open_utc(asset_class, now.date())
            print(
                f"{symbol} ({asset_class}) — session open {open_utc:%H:%M}Z"
                if open_utc
                else f"{symbol} ({asset_class}) — no venue calendar",
                flush=True,
            )
        if quoted_at is None:
            print(f"{now:%H:%M:%SZ}  no quote row", flush=True)
        else:
            age = (now - quoted_at).total_seconds()
            print(f"{now:%H:%M:%SZ}  quoted_at={quoted_at:%Y-%m-%d %H:%M:%SZ}  age={age:,.0f}s", flush=True)
            if open_utc is not None and quoted_at >= open_utc:
                lag = (quoted_at - open_utc).total_seconds()
                print(
                    f"ADVANCED — first stamp at or after the open is {quoted_at:%H:%M:%SZ}, "
                    f"{lag:,.0f}s after it; observed at {now:%H:%M:%SZ}",
                    flush=True,
                )
                return
        time.sleep(60)
    print("deadline reached with no stamp at or after the open", flush=True)


if __name__ == "__main__":
    main()
