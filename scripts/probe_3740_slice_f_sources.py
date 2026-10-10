"""Reproduce the measured premises of #3740 slice F's spec (read-only).

    PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_sources

Spec: ``docs/research/2026-10-10-3740-slice-f-forward-capture.md`` §"Measured premises".

One informational eToro GET (the closing-price snapshot, no order or position path) and
read-only SQL. Every count is printed with its population, because the tables it reads are
written by live jobs and any number copied into prose is stale the moment it is typed.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import platformdirs
import psycopg

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.workers.scheduler import _load_etoro_credentials  # pyright: ignore[reportPrivateUsage]

CLOSING_PRICE_PATH = "/api/v1/market-data/instruments/history/closing-price"
PWB_VENDOR = "paperswithbacktest/Stocks-Daily-Price@2026-09-09"
# eToro exchange ids (``exchanges`` table): 4 Nasdaq, 5 NYSE, 33 Regular Trading Hours.
US_EXCHANGES = ("4", "5", "33")
STOCKS_TYPE = 5
PWB_COMMITS_URL = "https://huggingface.co/api/datasets/paperswithbacktest/Stocks-Daily-Price/commits/main"
# Where ``app/services/sec_bulk_refresh.py`` keeps its single overwritten copy of each zip.
SEC_BULK_DIR = Path(platformdirs.user_data_dir("eBull")) / "sec" / "bulk"


def _quantile(sorted_values: list[float], q: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


def _closing_prices() -> list[dict[str, Any]]:
    creds = _load_etoro_credentials("probe_3740_slice_f_sources")
    if creds is None:
        raise SystemExit("no eToro credentials")
    with EtoroMarketDataProvider(api_key=creds[0], user_key=creds[1], env=settings.etoro_env) as provider:
        response = provider._http.get(CLOSING_PRICE_PATH, headers=provider._request_headers())  # pyright: ignore[reportPrivateUsage]
        response.raise_for_status()
    print(f"closing-price: {len(response.content)} bytes sha256 {hashlib.sha256(response.content).hexdigest()}")
    rows: list[dict[str, Any]] = json.loads(response.content)
    return rows


def main() -> None:
    rows = _closing_prices()
    if not rows:
        raise SystemExit("closing-price response is empty: no measurement")
    by_id = {row["instrumentId"]: row for row in rows}
    daily_dates = Counter(row["closingPrices"]["daily"]["date"][:10] for row in rows)
    print(f"instruments: {len(rows)}; closingPrices.daily.date top 5: {daily_dates.most_common(5)}")
    rolled_to = daily_dates.most_common(1)[0][0]

    with psycopg.connect(settings.database_url) as conn:
        latest = conn.execute("SELECT max(price_date) FROM price_daily WHERE price_date > %s", (rolled_to,)).fetchone()
        official_date = latest[0] if latest else None
        closes = dict(
            conn.execute(
                "SELECT instrument_id, close::float8 FROM price_daily WHERE price_date = %s", (official_date,)
            ).fetchall()
        )
        ratios = sorted(
            by_id[iid]["officialClosingPrice"] / close - 1
            for iid, close in closes.items()
            if iid in by_id and close > 0 and by_id[iid]["officialClosingPrice"] > 0
        )
        if not ratios:
            raise SystemExit(f"no instrument has both an official close and a price_daily close on {official_date}")
        print(
            f"officialClosingPrice vs price_daily.close on {official_date}: n={len(ratios)} "
            f"p1={_quantile(ratios, 0.01):+.4f} p10={_quantile(ratios, 0.10):+.4f} "
            f"p50={_quantile(ratios, 0.50):+.4f} p90={_quantile(ratios, 0.90):+.4f} "
            f"p99={_quantile(ratios, 0.99):+.4f} |r|>2%={sum(1 for r in ratios if abs(r) > 0.02)}"
        )

        candidates, with_pwb = conn.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM research_price_series s
                       WHERE s.vendor = %(vendor)s AND s.instrument_id = i.instrument_id))
            FROM instruments i
            WHERE i.exchange = ANY(%(exchanges)s) AND i.instrument_type_id = %(stocks)s AND i.is_tradable
            """,
            {"vendor": PWB_VENDOR, "exchanges": list(US_EXCHANGES), "stocks": STOCKS_TYPE},
        ).fetchone() or (0, 0)
        print(f"tradable type-5 instruments on exchanges {US_EXCHANGES}: {candidates}")
        print(f"... with a {PWB_VENDOR} series: {with_pwb}")

        markers = conn.execute(
            """
            SELECT count(*) FROM instruments i
            WHERE i.exchange = ANY(%(exchanges)s) AND i.instrument_type_id = %(stocks)s
              AND (i.company_name ILIKE '%%adr%%' OR i.company_name ILIKE '%%ads%%')
            """,
            {"exchanges": list(US_EXCHANGES), "stocks": STOCKS_TYPE},
        ).fetchone()
        print(f"type-5 instruments on exchanges {US_EXCHANGES} whose name contains ADR/ADS: {markers}")

    for name in ("companyfacts.zip", "submissions.zip"):
        path = SEC_BULK_DIR / name
        if path.exists():
            stat = path.stat()
            print(f"{path}: {stat.st_size} bytes, mtime {datetime.fromtimestamp(stat.st_mtime, UTC).isoformat()}")
        else:
            print(f"{path}: absent")

    with httpx.Client(timeout=30) as client:
        reply = client.get(PWB_COMMITS_URL)
        reply.raise_for_status()
        commits = reply.json()
    for commit in commits[:12]:
        print(f"PWB commit {commit['id'][:12]} {commit['date']} {commit['title']}")


if __name__ == "__main__":
    main()
