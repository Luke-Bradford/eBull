"""Bounded, non-trading census of eToro v2 eligibility and what-if costs.

The script refuses every environment except ``demo``. It performs informational
POST requests only; it creates no orders and writes no observation tables. The
normal credential-access audit still runs, as it must for every decryption.

Run from the repository root::

    uv run python scripts/verify_2437_trading_preflight.py
    uv run python scripts/verify_2437_trading_preflight.py --apply --limit 8

The default is a DB-only dry run showing the deterministic liquidity-spread
cohort. ``--apply`` makes one batched eligibility request and, for each returned
LONG/SHORT configuration, one current cost request at leverage 1.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.providers.broker import BrokerInstrumentEligibility, BrokerWhatIfOrder
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import CredentialNotFound, load_credential_for_provider_use
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS, resolve_stocks_type_id

logger = logging.getLogger(__name__)

_SETTLEMENT_TYPES = {
    "CFD": "cfd",
    "REAL": "real",
    "REALFUTURES": "realFutures",
    "MARGINTRADE": "marginTrade",
}


def _load_cohort(conn: psycopg.Connection[Any], limit: int) -> list[tuple[int, str, Decimal]]:
    """Select deterministic points across the latest dollar-volume distribution."""
    instrument_type_id = resolve_stocks_type_id(conn)
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT i.instrument_id, i.symbol,
                   (l.close * l.volume)::numeric AS dollar_volume,
                   percent_rank() OVER (ORDER BY l.close * l.volume, i.instrument_id) AS pct
            FROM instruments i
            JOIN exchanges e ON e.exchange_id = i.exchange
            CROSS JOIN LATERAL (
                SELECT p.close, p.volume
                FROM price_daily p
                WHERE p.instrument_id = i.instrument_id
                  AND p.close > 0 AND p.volume > 0
                ORDER BY p.price_date DESC
                LIMIT 1
            ) l
            WHERE i.is_tradable = TRUE
              AND i.instrument_type_id = %(instrument_type_id)s
              AND e.asset_class = %(asset_class)s
        ), targets AS (
            SELECT generate_series(0, %(limit_minus_one)s) AS slot
        )
        SELECT DISTINCT ON (r.instrument_id)
               r.instrument_id, r.symbol, r.dollar_volume
        FROM targets x
        CROSS JOIN LATERAL (
            SELECT instrument_id, symbol, dollar_volume, pct
            FROM ranked
            ORDER BY abs(pct - (x.slot::numeric / GREATEST(%(limit_minus_one)s, 1))), instrument_id
            LIMIT 1
        ) r
        ORDER BY r.instrument_id
        """,
        {
            "limit_minus_one": limit - 1,
            "instrument_type_id": instrument_type_id,
            "asset_class": US_EQUITY_ASSET_CLASS,
        },
    ).fetchall()
    return [(int(row[0]), str(row[1]), Decimal(str(row[2]))) for row in rows]


def _load_demo_credentials() -> tuple[str, str]:
    with psycopg.connect(settings.database_url) as conn:
        ensure_broker_key_loaded(conn)
        operator_id = sole_operator_id(conn)
        api_key = load_credential_for_provider_use(
            conn,
            operator_id=operator_id,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="verify_2437_trading_preflight",
        )
        conn.commit()
        user_key = load_credential_for_provider_use(
            conn,
            operator_id=operator_id,
            provider="etoro",
            label="user_key",
            environment="demo",
            caller="verify_2437_trading_preflight",
        )
        conn.commit()
    return api_key, user_key


def _cost_orders(row: BrokerInstrumentEligibility) -> list[BrokerWhatIfOrder]:
    orders: list[BrokerWhatIfOrder] = []
    for config in row.leverage_configs:
        settlement = _SETTLEMENT_TYPES.get(config.settlement_type.upper())
        direction = config.direction.upper()
        if settlement is None or direction not in {"LONG", "SHORT"} or 1 not in config.leverage_values:
            continue
        minimum = config.min_position_amount or row.min_position_exposure or Decimal("100")
        amount = max(minimum, Decimal("100"))
        orders.append(
            BrokerWhatIfOrder(
                instrument_id=row.instrument_id,
                transaction="buy" if direction == "LONG" else "sellShort",
                settlement_type=settlement,  # type: ignore[arg-type]
                amount=amount,
            )
        )
    return orders


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Call demo informational endpoints")
    parser.add_argument("--limit", type=int, default=8, help="Cohort size, 1..100 (default 8)")
    args = parser.parse_args()
    if not 1 <= args.limit <= 100:
        parser.error("--limit must be between 1 and 100")
    if settings.etoro_env != "demo":
        parser.error("refusing: ETORO_ENV must be demo for this probe")

    with psycopg.connect(settings.database_url) as conn:
        cohort = _load_cohort(conn, args.limit)
    print(
        json.dumps(
            {
                "mode": "apply" if args.apply else "dry_run",
                "cohort": [
                    {"instrument_id": instrument_id, "symbol": symbol, "dollar_volume": str(dollar_volume)}
                    for instrument_id, symbol, dollar_volume in cohort
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if not args.apply:
        return 0

    try:
        api_key, user_key = _load_demo_credentials()
    except (CredentialNotFound, NoOperatorError, AmbiguousOperatorError) as exc:
        logger.error("cannot load demo credentials: %s", exc)
        return 1

    with EtoroBrokerProvider(api_key=api_key, user_key=user_key, env="demo") as broker:
        eligibility = broker.check_instrument_eligibility([instrument_id for instrument_id, _, _ in cohort])
        cost_rows: list[dict[str, object]] = []
        for row in eligibility.eligibilities:
            for order in _cost_orders(row):
                result = broker.get_what_if_costs(order)
                cost_rows.append(
                    {
                        "instrument_id": result.instrument_id,
                        "transaction": order.transaction,
                        "settlement_type": order.settlement_type,
                        "amount": str(order.amount),
                        "last_updated": result.last_updated.isoformat(),
                        "costs": [
                            asdict(cost)
                            | {
                                "amount": str(cost.amount) if cost.amount is not None else None,
                                "value": str(cost.value) if cost.value is not None else None,
                            }
                            for cost in result.costs
                        ],
                    }
                )

    report = {
        "requested": len(cohort),
        "resolved": len(eligibility.eligibilities),
        "not_found_instrument_ids": list(eligibility.not_found_instrument_ids),
        "eligibility": [
            {
                "instrument_id": row.instrument_id,
                "symbol": row.symbol,
                "allow_open_position": row.allow_open_position,
                "allow_close_position": row.allow_close_position,
                "allow_partial_close_position": row.allow_partial_close_position,
                "allow_trailing_stop_loss": row.allow_trailing_stop_loss,
                "min_position_exposure": str(row.min_position_exposure),
                "max_units_per_order": str(row.max_units_per_order),
                "directions": [
                    {
                        "settlement_type": config.settlement_type,
                        "direction": config.direction,
                        "leverage_values": list(config.leverage_values),
                        "min_position_amount": str(config.min_position_amount),
                    }
                    for config in row.leverage_configs
                ],
            }
            for row in eligibility.eligibilities
        ],
        "cost_preflights": cost_rows,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
