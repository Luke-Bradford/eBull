"""#3118 — does the provider's price stamp advance at the venue open?

    PYTHONPATH=. uv run python -m scripts.probe_3118_session_open_quote --instrument-id 3417

Read-only. Samples ``quotes.quoted_at`` for one instrument once a minute and
reports when it first carries a stamp at or after that venue's session open.

⚠ WHY IT EXISTS.  The #3118 census can show where the HOURLY lane's fetches are
and what they returned; it has no sample between the last pre-open fetch and the
first in-session one, by construction.  So the one thing the spec could not
settle is whether eToro's ``date`` for an RTH instrument advances AT the open or
some minutes into it — which decides whether the five-minute producer closes the
window or merely narrows it.

⚠ It observes; it does not fetch.  The advance it waits for is
``core_candidate_quote_refresh``'s, so a run with that job stopped proves nothing
about the provider.

⚠⚠ WHAT IT CANNOT ESTABLISH, and it matters because its output is evidence
(Codex ckpt-3 on PR #3121):

* **Not the provider's FIRST post-open stamp.** ``quotes`` is one MUTABLE row per
  instrument.  The producer runs every five minutes and this polls every minute,
  so the observed stamp is the first one that SURVIVED to a poll, which is an
  upper bound on the true first stamp and not the stamp itself.
* **Not, on its own, that the price is real.** ``etoro.py:708-715`` substitutes
  ``datetime.now(UTC)`` when the provider's ``date`` is absent or unparseable, so
  a stamp inside the session is consistent with a manufactured one.  The census
  in ``scripts/census_3118_core_quote_freshness.py`` is what shows the provider
  does supply ``date`` for this cohort.
* **Nothing about a venue it has no calendar for**, and nothing on a non-trading
  day — both are refused up front rather than waited through.

⚠ It REQUIRES a first sample strictly BEFORE the target open.  Started later, it
cannot witness a transition: the stamp would already be inside the session and
"advanced" would be a statement about the clock, not about the provider.
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, date, datetime, timedelta

import psycopg

from app.config import settings
from app.services.market_session_support import venue_calendar_for, venue_local_now

#: A stamp further into the future than this is a clock or provider fault, not
#: freshness.  Mirrors ``strategy_core_preflight._FUTURE_SKEW`` rather than
#: minting a second tolerance: a probe that accepted a future stamp would report
#: ADVANCED before the open and the reader could not tell.
_FUTURE_SKEW = timedelta(seconds=5)

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


def _sample(instrument_id: int) -> tuple[str, str | None, datetime | None, datetime] | None:
    """One read, on its own short-lived connection.

    ⚠ A FRESH connection per sample is deliberate.  The dev profile's connection
    budget has no unallocated modelled capacity — measured 2026-09-16 while
    building #3118: ``usable 27, demand 27`` (``_dev_profile_connection_demand()
    + CONNECTION_BUDGET_RESERVE`` against ``max_connections −
    superuser_reserved_connections``).  This script is not in that model, so
    holding a slot for the whole run would sit outside a budget with nothing
    spare; connecting per sample returns the slot between reads.

    ⚠ It is NOT justified by "a jobs restart would drop the connection" — that
    was claimed on PR #3121 and is false: this process's connection to Postgres
    is independent of the jobs child that ``dev_reload`` respawns.
    """
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute(_ROW, {"instrument_id": instrument_id}).fetchone()
    if row is None:
        return None
    symbol, asset_class, quoted_at, now = row
    # ⚠⚠ BOTH converted to UTC before anything reads or subtracts them. psycopg
    # returns a timestamptz in the CONNECTION's timezone, so printing one with a
    # literal "Z" is an assertion about the connection, not a check — and across
    # a DST fold the naive subtraction below is wrong by an hour. Dev's TimeZone
    # is Etc/UTC, which makes this latent here and wrong elsewhere.
    return (
        str(symbol),
        asset_class,
        None if quoted_at is None else quoted_at.astimezone(UTC),
        now.astimezone(UTC),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument-id", type=int, default=3417)
    parser.add_argument("--minutes", type=int, default=70, help="how long to keep sampling")
    args = parser.parse_args()
    if args.minutes <= 0:
        raise SystemExit("--minutes must be positive")

    first = _sample(args.instrument_id)
    if first is None:
        raise SystemExit(f"instrument {args.instrument_id} is unknown")
    symbol, asset_class, quoted_at, now = first

    calendar = venue_calendar_for(asset_class)
    if calendar is None:
        raise SystemExit(f"{symbol} ({asset_class}) — no venue calendar; there is no open to wait for")

    # ⚠ The TARGET session is pinned ONCE, from the venue-local date at START.
    # Recomputing it per sample would silently re-target after local midnight,
    # and the run would then report an advance against a different day's open.
    local_now = venue_local_now(asset_class, now)
    session_day = local_now.date()
    status = calendar.status(session_day)
    if status != "open":
        raise SystemExit(f"{symbol}: {calendar.calendar_id} is {status!r} on {session_day} — not a trading day")
    open_utc = _session_open_utc(asset_class, session_day)
    assert open_utc is not None  # calendar is not None, checked above

    if now >= open_utc:
        raise SystemExit(
            f"{symbol}: started at {now:%H:%M:%S} UTC, after the {open_utc:%H:%M} UTC open — "
            "a first advance cannot be witnessed from here; start before the open"
        )
    print(f"{symbol} ({asset_class}) — {calendar.calendar_id} opens {open_utc:%H:%M} UTC on {session_day}", flush=True)

    deadline = time.monotonic() + args.minutes * 60
    while True:
        sample = _sample(args.instrument_id)
        if sample is None:
            raise SystemExit(f"instrument {args.instrument_id} disappeared mid-run")
        _, _, quoted_at, now = sample
        if quoted_at is None:
            print(f"{now:%H:%M:%S} UTC  no quote row", flush=True)
        else:
            age = (now - quoted_at).total_seconds()
            print(f"{now:%H:%M:%S} UTC  quoted_at={quoted_at:%Y-%m-%d %H:%M:%S} UTC  age={age:,.0f}s", flush=True)
            if quoted_at > now + _FUTURE_SKEW:
                # A future stamp is a fault, and without this guard it would
                # satisfy the >= open test and report ADVANCED before the open.
                print(
                    f"REFUSED — quoted_at is {-age:,.0f}s in the FUTURE; this is a clock or provider fault", flush=True
                )
                return
            if quoted_at >= open_utc:
                lag = (quoted_at - open_utc).total_seconds()
                print(
                    f"ADVANCED — first stamp at or after the open is {quoted_at:%H:%M:%S} UTC, "
                    f"{lag:,.0f}s after it; observed at {now:%H:%M:%S} UTC. "
                    "⚠ an UPPER BOUND on the provider's first post-open stamp, not the stamp itself",
                    flush=True,
                )
                return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(60.0, remaining))
    print(f"deadline reached with no stamp at or after {open_utc:%H:%M} UTC", flush=True)


if __name__ == "__main__":
    main()
