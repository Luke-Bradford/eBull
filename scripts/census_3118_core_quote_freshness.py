"""#3118 — how fresh the core candidates' quotes actually are, hour by hour.

    PYTHONPATH=. uv run python -m scripts.census_3118_core_quote_freshness

Reads ``strategy_core_quote_observations`` (sql/366), the hourly immutable lane
``quotes_refresh`` writes on its own tick, and reports the share of ticks at
which each core candidate's quote was fresh enough to be an observation at all.

⚠ NOTHING HERE IS TYPED IN.  The bound, the policy version and the refresh
cadence are read from the modules that own them (``strategy_core_preflight``,
``app.workers.scheduler.SCHEDULED_JOBS``) and the venue session from
``market_session_support.venue_calendar_for``, so a re-cadenced job or a moved
constant changes this output instead of leaving it quietly wrong.

⚠ WHAT THE LANE CAN AND CANNOT SHOW.  It samples once an hour, at the refresh
job's fire minute.  So it establishes where the fetches ARE and what they
returned; it can say nothing about any moment between two fetches, which is
exactly the window the session-open gap below lives in.  The gap is therefore
derived from the cadence and the venue calendar, and labelled as derived.

``observation_status`` vocabulary is ``strategy_core_quote_observation``'s own:
``observed`` / ``missing`` (the provider returned no quote for the id) /
``invalid`` (it returned one that failed a shape or age guard, with the guard
named in ``refusal_reason``).
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta

import psycopg

from app.config import settings
from app.services.market_session_support import venue_calendar_for
from app.services.strategy_core_mandate import load_core_mandate
from app.services.strategy_core_preflight import (
    CORE_MAX_QUOTE_AGE_SECONDS,
    CORE_PREFLIGHT_POLICY_VERSION,
)
from app.workers.scheduler import JOB_QUOTES_REFRESH, SCHEDULED_JOBS

_CANDIDATES = """
SELECT o.instrument_id,
       i.symbol,
       e.asset_class,
       count(*)                                                AS ticks,
       count(*) FILTER (WHERE o.observation_status = 'observed') AS observed
FROM strategy_core_quote_observations o
JOIN instruments i ON i.instrument_id = o.instrument_id
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
GROUP BY 1, 2, 3
ORDER BY 1
"""

_REASONS = """
SELECT instrument_id, observation_status, refusal_reason, count(*)
FROM strategy_core_quote_observations
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC
"""

_BY_HOUR = """
SELECT extract(hour FROM sample_bucket AT TIME ZONE 'UTC')::int      AS utc_hour,
       count(*)                                                      AS ticks,
       count(*) FILTER (WHERE observation_status = 'observed')        AS observed
FROM strategy_core_quote_observations
WHERE instrument_id = %(instrument_id)s
GROUP BY 1
ORDER BY 1
"""

_PER_DAY = """
SELECT (sample_bucket AT TIME ZONE 'UTC')::date AS utc_day,
       min(sample_bucket)                       AS first_observed,
       max(sample_bucket)                       AS last_observed,
       count(*)                                 AS observed
FROM strategy_core_quote_observations
WHERE instrument_id = %(instrument_id)s AND observation_status = 'observed'
GROUP BY 1
ORDER BY 1
"""


def _refresh_fire_minute() -> tuple[str, int | None]:
    """``quotes_refresh``'s registered cadence kind and fire minute."""
    cadence = next(job.cadence for job in SCHEDULED_JOBS if job.name == JOB_QUOTES_REFRESH)
    return str(cadence.kind), (int(cadence.minute) if cadence.kind == "hourly" else None)


def _open_gap(asset_class: str | None, *, on: date, fire_minute: int | None) -> str:
    """The stretch after the venue open that no scheduled fetch can cover.

    DERIVED, not observed: the session open comes from the venue calendar and
    the fetch instants from the cadence, so this is arithmetic over two
    published rules rather than a measurement.  Returns a human line.
    """
    calendar = venue_calendar_for(asset_class)
    if calendar is None:
        return f"  no venue calendar for asset_class={asset_class!r} — session open undefined here"
    if fire_minute is None:
        return "  quotes_refresh is not hourly — the per-hour fire instant is not defined"
    open_local = datetime.combine(on, calendar.session_open, tzinfo=calendar.tz)
    open_utc = open_local.astimezone(UTC)
    fire = open_utc.replace(minute=fire_minute, second=0, microsecond=0)
    if fire < open_utc:
        fire += timedelta(hours=1)
    gap_s = int((fire - open_utc).total_seconds())
    return (
        f"  {calendar.calendar_id}: open {open_utc:%H:%M}Z on {on} — "
        f"first fetch at or after it {fire:%H:%M}Z — "
        f"uncovered {gap_s}s ({gap_s / 60:.0f} min) by the HOURLY lane alone; "
        f"preflight bound {CORE_MAX_QUOTE_AGE_SECONDS}s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instrument-id",
        type=int,
        default=None,
        help="instrument to profile hour-by-hour; default is the enabled core mandate's",
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=date.today(),
        help="civil date used to resolve the venue open to UTC (DST-sensitive)",
    )
    args = parser.parse_args()

    kind, fire_minute = _refresh_fire_minute()
    # ⚠ The bound stopped following this script's subject in #3157: it is derived from
    # `core_candidate_quote_refresh` (300 s, one lost fire tolerated), not from the
    # hourly lane whose coverage this census measures. Printed together on purpose —
    # the gap below is what the hourly lane alone leaves, and the bound is what a
    # verdict is held to; reading the second as the first was the #3118 confusion.
    print(f"preflight quote bound  {CORE_MAX_QUOTE_AGE_SECONDS}s  ({CORE_PREFLIGHT_POLICY_VERSION})")
    print(f"{JOB_QUOTES_REFRESH} cadence  kind={kind} minute={fire_minute}")
    print()

    with psycopg.connect(settings.database_url) as conn, conn.cursor() as cur:
        candidates = cur.execute(_CANDIDATES).fetchall()
        if not candidates:
            print("strategy_core_quote_observations is EMPTY — the lane has never run")
            return
        print("candidate            ticks  observed   share  asset_class")
        by_class: dict[int, str | None] = {}
        for instrument_id, symbol, asset_class, ticks, observed in candidates:
            by_class[int(instrument_id)] = asset_class
            share = f"{observed / ticks:6.1%}" if ticks else "     -"
            print(f"  {instrument_id:>6} {str(symbol):<10} {ticks:>5} {observed:>9} {share}  {asset_class}")

        print("\nrefusal reasons")
        for instrument_id, status, reason, count in cur.execute(_REASONS).fetchall():
            print(f"  {instrument_id:>6} {status:<9} {str(reason):<24} {count:>5}")

        target = args.instrument_id
        if target is None:
            # The mandate reader rather than a second copy of its SQL: "latest
            # revision" is its rule, and a hand-rolled ORDER BY here would be a
            # second definition of which revision is current.
            mandate = load_core_mandate(conn)
            target = None if mandate is None else mandate.core_instrument_id
        if target is None:
            print("\nno configured core mandate and no --instrument-id — skipping the per-hour profile")
            return

        print(f"\ninstrument {target} — observed/ticks by UTC hour")
        for utc_hour, ticks, observed in cur.execute(_BY_HOUR, {"instrument_id": target}).fetchall():
            if ticks:
                print(f"  {utc_hour:02d}:00  {observed:>3}/{ticks:<3}")

        print(f"\ninstrument {target} — observed window per UTC day")
        for utc_day, first_observed, last_observed, observed in cur.execute(
            _PER_DAY, {"instrument_id": target}
        ).fetchall():
            print(f"  {utc_day}  {first_observed:%H:%M}Z .. {last_observed:%H:%M}Z  n={observed}")

    print("\nsession-open gap (DERIVED from the venue calendar and the cadence, not sampled)")
    print(_open_gap(by_class.get(target), on=args.as_of, fire_minute=fire_minute))


if __name__ == "__main__":
    main()
