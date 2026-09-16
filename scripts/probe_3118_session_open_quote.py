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
from app.services.market_session_support import venue_calendar_for, venue_local_now

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

    announced = False
    deadline = time.monotonic() + args.minutes * 60
    while time.monotonic() < deadline:
        # ⚠ A FRESH connection per sample, deliberately, and not an oversight
        # (review NITPICK).  This loop runs for over an hour between one-row
        # SELECTs, and the dev profile's connection budget has ZERO headroom —
        # measured 2026-09-16 while building #3118: usable 27, demand 27
        # (`_dev_profile_connection_demand() + CONNECTION_BUDGET_RESERVE`).
        # Holding one idle connection open for 70 minutes to save ~70 cheap
        # connects is the wrong side of that trade, and it would also have to
        # grow reconnect handling to survive a restart of the very jobs process
        # this probe is watching.
        with psycopg.connect(settings.database_url) as conn:
            row = conn.execute(_ROW, {"instrument_id": args.instrument_id}).fetchone()
        if row is None:
            print(f"instrument {args.instrument_id} is unknown", flush=True)
            return
        symbol, asset_class, quoted_at, now = row
        # ⚠ RECOMPUTED every sample, and keyed on the VENUE-LOCAL date (review
        # NITPICK).  Computing it once straddles a date boundary with a stale
        # open; computing it from `now.date()` is wrong in a second way, because
        # that is the UTC date — at 00:30Z the UTC day has already rolled while
        # New York is still the previous evening, so the open would jump a day
        # early.  `venue_local_now` is the same helper `decide_core_preflight`
        # uses to report a refusal's venue time.
        local_now = venue_local_now(asset_class, now)
        open_utc = _session_open_utc(asset_class, local_now.date())
        if not announced:
            announced = True
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
