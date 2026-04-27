"""One-shot deepening of price_daily history (#603).

Bumps every Tier 1/2 instrument's candle history to the new 1000-bar
ceiling (≈4 calendar years of trading-day prices, the most a single
eToro fetch can deliver).

Why this script exists rather than waiting for the scheduler to
auto-deepen: ``refresh_market_data`` runs in incremental mode for any
instrument with a recent bar, which means an instrument seeded under
the old 400-bar policy keeps its 400 bars even after the default is
raised to 1000. Forcing a one-time backfill closes the gap; the
scheduler resumes incremental mode the next day.

Run from the repo root:

    uv run python scripts/rebackfill_candles_5y.py --apply

Defaults to dry-run (logs what it would do, no API calls). Provide
``--apply`` to actually fetch. ``--limit N`` caps the number of
instruments processed in this run, useful if you want to spread the
deepening across multiple sessions or smoke-test against a few
instruments first.

Rate limit: eToro allows 60 GET/min/key. With ~600 Tier 1/2
instruments at 1.1s per fetch (provider's built-in throttle), the
full sweep takes ~12 minutes. The provider client handles 429s with
backoff so the script does not need to manage rate limits itself.

Quotes are skipped (``skip_quotes=True``) so this script does not
shadow the hourly fx_rates_refresh job's quote freshness.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.services.broker_credentials import (
    CredentialNotFound,
    load_credential_for_provider_use,
)
from app.services.market_data import refresh_market_data
from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _load_instruments(conn: psycopg.Connection, limit: int | None) -> list[tuple[int, str]]:  # type: ignore[type-arg]
    """Held positions ∪ Tier 1 + Tier 2 tradable instruments.

    Held names are included even when they are not in T1/T2 because
    ``_candles_fetch_count`` will NOT deepen a short-but-fresh history
    on its own — once an instrument has any incremental-window-fresh
    bar, the function stays in 3-bar incremental mode regardless of
    how short the underlying series is. Without held names here, a
    delisted-but-still-held position seeded under the old 400-bar
    policy would never extend.
    """
    if limit is not None:
        rows = conn.execute(
            """
            SELECT instrument_id, symbol FROM (
                SELECT i.instrument_id, i.symbol
                FROM instruments i
                JOIN positions p ON p.instrument_id = i.instrument_id
                WHERE p.current_units > 0
                UNION
                SELECT i.instrument_id, i.symbol
                FROM instruments i
                JOIN coverage c ON c.instrument_id = i.instrument_id
                WHERE i.is_tradable = TRUE
                  AND c.coverage_tier IN (1, 2)
            ) AS u
            ORDER BY symbol, instrument_id
            LIMIT %(lim)s
            """,
            {"lim": int(limit)},
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT instrument_id, symbol FROM (
                SELECT i.instrument_id, i.symbol
                FROM instruments i
                JOIN positions p ON p.instrument_id = i.instrument_id
                WHERE p.current_units > 0
                UNION
                SELECT i.instrument_id, i.symbol
                FROM instruments i
                JOIN coverage c ON c.instrument_id = i.instrument_id
                WHERE i.is_tradable = TRUE
                  AND c.coverage_tier IN (1, 2)
            ) AS u
            ORDER BY symbol, instrument_id
            """
        ).fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually fetch (default: dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="Cap on instruments processed")
    args = parser.parse_args()

    try:
        with psycopg.connect(settings.database_url) as conn:
            op_id = sole_operator_id(conn)
            api_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="api_key",
                environment=settings.etoro_env,
                caller="rebackfill_candles_5y",
            )
            conn.commit()
            user_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="user_key",
                environment=settings.etoro_env,
                caller="rebackfill_candles_5y",
            )
            conn.commit()
    except (NoOperatorError, AmbiguousOperatorError) as exc:
        logger.error("operator lookup failed: %s", exc)
        return 1
    except CredentialNotFound as exc:
        logger.error("eToro credentials missing: %s", exc)
        return 1

    with psycopg.connect(settings.database_url) as conn:
        instruments = _load_instruments(conn, args.limit)
        if not instruments:
            logger.info("No Tier 1/2 instruments matched — nothing to deepen.")
            return 0

        logger.info("Selected %d instruments for deepening.", len(instruments))
        if not args.apply:
            logger.info("DRY RUN — pass --apply to actually fetch. Sample (first 5):")
            for iid, sym in instruments[:5]:
                logger.info("  %d %s", iid, sym)
            return 0

        with EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider:
            summary = refresh_market_data(
                provider,
                conn,
                instruments,
                skip_quotes=True,
                force_backfill=True,
            )

    logger.info(
        "Deepening complete: instruments=%d candles_upserted=%d features=%d",
        summary.instruments_refreshed,
        summary.candle_rows_upserted,
        summary.features_computed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
