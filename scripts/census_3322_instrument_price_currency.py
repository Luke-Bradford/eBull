"""Read-only census for #3322: what the production price-currency classifier would do.

Fetches live eToro rates (informational endpoint, no broker mutation) for every
tradable instrument on a GBP/EUR venue and reports, per quote-age bucket, the best
match residual, how many quotes match 0 / 1 / >1 candidates, and what
``classify_price_currency`` would derive per venue currency. Writes nothing.

    PYTHONPATH=. uv run python -m scripts.census_3322_instrument_price_currency
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta

import psycopg

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.services.instrument_price_currency import (
    RECLASSIFIABLE_VENUE_CURRENCIES,
    TOLERANCE,
    classify_price_currency,
    load_fx_reference,
)
from app.workers.scheduler import _load_etoro_credentials

_BUCKETS = (
    (timedelta(days=1), "<=1d"),
    (timedelta(days=4), "<=4d"),
    (timedelta(days=7), "<=7d"),
    (timedelta(days=30), "<=30d"),
)


def _bucket(age: timedelta) -> str:
    return next((label for bound, label in _BUCKETS if age <= bound), ">30d")


def main() -> None:
    now = datetime.now(UTC)
    with psycopg.connect(settings.database_url) as conn:
        fx = load_fx_reference(conn)
        rows = conn.execute(
            """
            SELECT i.instrument_id, e.currency
              FROM instruments i JOIN exchanges e ON e.exchange_id = i.exchange
             WHERE i.is_tradable AND e.currency = ANY(%s)
            """,
            (list(RECLASSIFIABLE_VENUE_CURRENCIES),),
        ).fetchall()
    if fx is None:
        raise SystemExit("live_fx_rates lacks USD→EUR/GBP")
    creds = _load_etoro_credentials("census_3322")
    if creds is None:
        raise SystemExit("no eToro credentials")
    venue = dict(rows)
    ids = list(venue)
    residuals: dict[str, list[float]] = defaultdict(list)
    match_counts: Counter[tuple[str, int]] = Counter()
    derived: Counter[tuple[str, str]] = Counter()
    reasons: Counter[str] = Counter()
    with EtoroMarketDataProvider(api_key=creds[0], user_key=creds[1], env=settings.etoro_env) as provider:
        for start in range(0, len(ids), 100):
            batch = provider.get_quotes(ids[start : start + 100])
            now = datetime.now(UTC)
            for q in batch:
                ccy, reason = classify_price_currency(q.conversion_rate, q.timestamp, fx, now)
                reasons[reason] += 1
                if ccy is not None:
                    derived[(venue[q.instrument_id], ccy)] += 1
                if q.conversion_rate is None or q.conversion_rate <= 0:
                    continue
                dists = [abs(q.conversion_rate / usd - 1) for usd in fx.usd_per_unit.values()]
                b = _bucket(now - q.timestamp)
                residuals[b].append(float(min(dists)))
                match_counts[(b, sum(d <= TOLERANCE for d in dists))] += 1
    print(f"instruments={len(ids)} fx_quoted_at={fx.quoted_at.isoformat()} tolerance={TOLERANCE}")
    for b in [label for _, label in _BUCKETS] + [">30d"]:
        v = sorted(residuals.get(b, []))
        if v:
            print(f"{b:6} n={len(v):5} p50={v[len(v) // 2]:.4f} max={v[-1]:.4f}")
    for (b, n), c in sorted(match_counts.items()):
        print(f"matches {b:6} candidates={n} count={c}")
    print("reasons", dict(reasons))
    for (v, c), n in sorted(derived.items()):
        print(f"derive venue={v} -> {c}: {n}")


if __name__ == "__main__":
    main()
