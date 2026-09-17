"""#3157 — the realised age of the core cohort's quote, sampled at arbitrary minutes.

    PYTHONPATH=. uv run python -m scripts.census_3157_core_quote_age --minutes 20

Read-only.  Samples ``now() - quotes.quoted_at`` for every instrument in
``CORE_QUOTE_REFRESH_SCOPE_SQL`` — the cohort ``core_candidate_quote_refresh``
maintains and the only cohort ``decide_core_preflight`` can be about — and prints
the per-instrument age distribution plus the maximum over the run.

⚠ WHY IT EXISTS, and why the #3118 lanes could not answer it.
``strategy_core_quote_observations`` samples on the hour at ``:23``, the same
minute ``quotes_refresh`` fires, so it is phase-locked to a DIFFERENT producer and
reads ~1 s of age at every tick whatever the five-minute job does.  And a
producer's inter-arrival census bounds the age from above without measuring it:
at a perfect 300 s cadence the age at a random instant is uniform on [0, 300).
The bound in ``strategy_core_preflight`` is evaluated at an arbitrary minute, so
that is the distribution it has to survive.

⚠ WHAT IT CANNOT ESTABLISH.

* **Not that the stamp is provider-supplied.**  ``etoro.py`` substitutes
  ``datetime.now(UTC)`` when the provider's ``date`` is absent, which would
  manufacture freshness.  #3118 ruled that out for THIS cohort by sub-second
  ordering against the fetching job's own ``started_at``; nothing here re-proves it.
* **Not a bound on future behaviour.**  It is a sample of one session.
* **Nothing out of session**, which is why the session state is printed beside
  every sample: ``core_market_session_closed`` precedes ``core_quote_stale`` in
  ``_decide``, so an out-of-session age never reaches the bound.
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime

import psycopg

from app.config import settings
from app.services.market_session_support import venue_session_is_open
from app.services.strategy_core_eligibility import CORE_ELIGIBILITY_PASS_VERDICT
from app.workers.scheduler import CORE_QUOTE_REFRESH_SCOPE_SQL

_AGE_SQL = """
SELECT i.instrument_id, i.symbol, e.asset_class,
       extract(epoch FROM now() - q.quoted_at) AS age_s,
       q.quoted_at
FROM instruments i
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
LEFT JOIN quotes q ON q.instrument_id = i.instrument_id
WHERE i.instrument_id = ANY(%(ids)s)
ORDER BY i.symbol
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument("--every-seconds", type=int, default=30)
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        cohort = conn.execute(CORE_QUOTE_REFRESH_SCOPE_SQL, {"pass_verdict": CORE_ELIGIBILITY_PASS_VERDICT}).fetchall()
    ids = [int(r[0]) for r in cohort]
    print(f"cohort: {len(ids)} instruments — {', '.join(str(r[1]) for r in cohort)}", flush=True)

    # symbol -> [(age_s, in_session)]
    samples: dict[str, list[tuple[float, bool]]] = {}
    deadline = time.monotonic() + args.minutes * 60
    while True:
        now = datetime.now(UTC)
        with psycopg.connect(settings.database_url) as conn:
            rows = conn.execute(_AGE_SQL, {"ids": ids}).fetchall()
        line = []
        for _iid, symbol, asset_class, age_s, quoted_at in rows:
            if age_s is None or quoted_at is None:
                line.append(f"{symbol}=NO_QUOTE")
                continue
            in_session = venue_session_is_open(asset_class, now) if asset_class else False
            samples.setdefault(symbol, []).append((float(age_s), in_session))
            line.append(f"{symbol}={float(age_s):.0f}{'*' if in_session else ''}")
        print(f"{now.strftime('%H:%M:%SZ')}  " + "  ".join(line), flush=True)
        if time.monotonic() >= deadline:
            break
        time.sleep(args.every_seconds)

    print("\n=== age seconds, * = venue session OPEN ===", flush=True)
    print(f"{'symbol':10} {'n':>4} {'n_open':>7} {'min':>7} {'p50':>7} {'max':>7} {'max_open':>9}")
    for symbol, vals in sorted(samples.items()):
        ages = sorted(a for a, _ in vals)
        open_ages = sorted(a for a, o in vals if o)
        print(
            f"{symbol:10} {len(ages):>4} {len(open_ages):>7} {ages[0]:>7.0f} "
            f"{ages[len(ages) // 2]:>7.0f} {ages[-1]:>7.0f} "
            f"{(f'{open_ages[-1]:.0f}' if open_ages else '-'):>9}"
        )
    all_open = [a for vals in samples.values() for a, o in vals if o]
    if all_open:
        print(f"\nmax in-session age over the run: {max(all_open):.0f}s ({len(all_open)} samples)")


if __name__ == "__main__":
    main()
