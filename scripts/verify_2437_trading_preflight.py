"""Bounded, non-trading census of eToro v2 eligibility and what-if costs.

The script refuses every environment except ``demo``. It performs informational
POST requests only; it creates no orders and writes no observation tables. The
normal credential-access audit still runs, as it must for every decryption.

Run from the repository root::

    PYTHONPATH=. uv run python scripts/verify_2437_trading_preflight.py
    PYTHONPATH=. uv run python scripts/verify_2437_trading_preflight.py --apply --limit 8

The default is a DB-only dry run showing a deterministic, type-balanced cohort
across US-equity Stocks and ETFs. ``--apply`` makes one batched eligibility
request, then bounded cost requests for eligible long-real and x1 short-CFD
arms at 1x and 10x their base ticket. No cost row is execution-usable unless it
uses the documented monetary ``amount`` field, USD, and a current timestamp.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import psycopg

from app.config import settings
from app.providers.broker import BrokerInstrumentEligibility, BrokerWhatIfCostResponse, BrokerWhatIfOrder
from app.providers.implementations.etoro_broker import EtoroBrokerProvider, TradingPreflightParseError
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import CredentialNotFound, load_credential_for_provider_use
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS

logger = logging.getLogger(__name__)

CENSUS_VERSION = "etoro-preflight-v2"
COHORT_TYPE_DESCRIPTIONS = ("Stocks", "ETF")
COST_SCALE_MULTIPLIERS = (Decimal("1"), Decimal("10"))
COST_FRESHNESS_MAX_AGE = timedelta(hours=24)
MAX_COST_REQUESTS_PER_RUN = 20  # Dedicated endpoint budget: 20 requests / 60 seconds.

_SETTLEMENT_TYPES = {
    "CFD": "cfd",
    "REAL": "real",
    "REALFUTURES": "realFutures",
    "MARGINTRADE": "marginTrade",
}


@dataclass(frozen=True)
class CohortMember:
    instrument_id: int
    symbol: str
    instrument_type: str
    dollar_volume: Decimal
    local_is_tradable: bool


def _canonical_payload_bytes(payload: object) -> int:
    """Parsed payload size under a reproducible canonical JSON encoding."""
    return len(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def _resolve_type_ids(conn: psycopg.Connection[Any]) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT description, instrument_type_id
        FROM etoro_instrument_types
        WHERE description = ANY(%(descriptions)s)
        ORDER BY description, instrument_type_id
        """,
        {"descriptions": list(COHORT_TYPE_DESCRIPTIONS)},
    ).fetchall()
    grouped: dict[str, list[int]] = defaultdict(list)
    for description, instrument_type_id in rows:
        grouped[str(description)].append(int(instrument_type_id))
    for description in COHORT_TYPE_DESCRIPTIONS:
        if len(grouped[description]) != 1:
            raise RuntimeError(
                f"etoro_instrument_types.description = {description!r} resolved to "
                f"{len(grouped[description])} rows; the cohort has no stable anchor"
            )
    return {description: grouped[description][0] for description in COHORT_TYPE_DESCRIPTIONS}


def _type_quotas(limit: int) -> dict[str, int]:
    """Split the total bound deterministically, favouring Stocks for odd limits."""
    base, remainder = divmod(limit, len(COHORT_TYPE_DESCRIPTIONS))
    return {
        description: base + (1 if index < remainder else 0)
        for index, description in enumerate(COHORT_TYPE_DESCRIPTIONS)
    }


def _load_cohort(conn: psycopg.Connection[Any], limit: int) -> list[CohortMember]:
    """Select deterministic points across each type's latest liquidity distribution."""
    type_ids = _resolve_type_ids(conn)
    cohort: list[CohortMember] = []
    for description, quota in _type_quotas(limit).items():
        if quota == 0:
            continue
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT i.instrument_id, i.symbol, i.is_tradable,
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
                SELECT generate_series(0, %(quota_minus_one)s) AS slot
            )
            SELECT DISTINCT ON (r.instrument_id)
                   r.instrument_id, r.symbol, r.dollar_volume, r.is_tradable
            FROM targets x
            CROSS JOIN LATERAL (
                SELECT instrument_id, symbol, dollar_volume, is_tradable, pct
                FROM ranked
                ORDER BY abs(pct - (x.slot::numeric / GREATEST(%(quota_minus_one)s, 1))), instrument_id
                LIMIT 1
            ) r
            ORDER BY r.instrument_id
            """,
            {
                "quota_minus_one": quota - 1,
                "instrument_type_id": type_ids[description],
                "asset_class": US_EQUITY_ASSET_CLASS,
            },
        ).fetchall()
        cohort.extend(
            CohortMember(
                instrument_id=int(row[0]),
                symbol=str(row[1]),
                instrument_type=description,
                dollar_volume=Decimal(str(row[2])),
                local_is_tradable=bool(row[3]),
            )
            for row in rows
        )
    return sorted(cohort, key=lambda row: (COHORT_TYPE_DESCRIPTIONS.index(row.instrument_type), row.instrument_id))


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


def _base_cost_orders(row: BrokerInstrumentEligibility) -> list[BrokerWhatIfOrder]:
    """Return only the two arms this spike is authorised to characterise."""
    orders: list[BrokerWhatIfOrder] = []
    for config in row.leverage_configs:
        settlement = _SETTLEMENT_TYPES.get(config.settlement_type.upper())
        direction = config.direction.upper()
        is_long_real = direction == "LONG" and settlement == "real"
        is_short_cfd = direction == "SHORT" and settlement == "cfd"
        if (not is_long_real and not is_short_cfd) or 1 not in config.leverage_values:
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
    return sorted(orders, key=lambda order: (order.transaction, order.settlement_type))


def _cost_orders(row: BrokerInstrumentEligibility) -> list[tuple[str, Decimal, BrokerWhatIfOrder]]:
    if not row.allow_open_position:
        return []
    orders: list[tuple[str, Decimal, BrokerWhatIfOrder]] = []
    for base in _base_cost_orders(row):
        assert base.amount is not None
        for multiplier in COST_SCALE_MULTIPLIERS:
            arm_id = f"{CENSUS_VERSION}:{base.instrument_id}:{base.transaction}:{base.settlement_type}:{multiplier}x"
            orders.append(
                (
                    arm_id,
                    multiplier,
                    BrokerWhatIfOrder(
                        instrument_id=base.instrument_id,
                        transaction=base.transaction,
                        settlement_type=base.settlement_type,
                        amount=base.amount * multiplier,
                    ),
                )
            )
    return orders


def _interleave_cost_arms(
    eligibilities: tuple[BrokerInstrumentEligibility, ...],
    instrument_types: dict[int, str],
) -> list[tuple[str, Decimal, BrokerWhatIfOrder]]:
    """Round-robin complete scaling pairs across types before applying the cap."""
    groups: dict[str, list[list[tuple[str, Decimal, BrokerWhatIfOrder]]]] = defaultdict(list)
    for row in eligibilities:
        arms = _cost_orders(row)
        for index in range(0, len(arms), len(COST_SCALE_MULTIPLIERS)):
            groups[instrument_types[row.instrument_id]].append(arms[index : index + len(COST_SCALE_MULTIPLIERS)])
    interleaved: list[tuple[str, Decimal, BrokerWhatIfOrder]] = []
    max_groups = max((len(rows) for rows in groups.values()), default=0)
    for index in range(max_groups):
        for description in COHORT_TYPE_DESCRIPTIONS:
            if index < len(groups[description]):
                interleaved.extend(groups[description][index])
    return interleaved


def _bounded_cost_arms(
    arms: list[tuple[str, Decimal, BrokerWhatIfOrder]],
    max_requests: int,
) -> list[tuple[str, Decimal, BrokerWhatIfOrder]]:
    if not 2 <= max_requests <= MAX_COST_REQUESTS_PER_RUN:
        raise ValueError(f"max_requests must be between 2 and {MAX_COST_REQUESTS_PER_RUN}")
    if max_requests % len(COST_SCALE_MULTIPLIERS):
        raise ValueError("max_requests must preserve complete scaling pairs")
    return arms[:max_requests]


def _fetch_cost(
    broker: EtoroBrokerProvider,
    order: BrokerWhatIfOrder,
) -> tuple[BrokerWhatIfCostResponse | None, str | None]:
    """Keep one malformed arm from destroying the bounded population census."""
    try:
        return broker.get_what_if_costs(order), None
    except (httpx.HTTPError, TradingPreflightParseError, json.JSONDecodeError) as exc:
        return None, type(exc).__name__


def _freshness(last_updated: datetime, observed_at: datetime) -> tuple[Decimal, str]:
    age = Decimal(str((observed_at - last_updated).total_seconds()))
    if age < 0:
        return age, "future_timestamp_blocking"
    if age > Decimal(str(COST_FRESHNESS_MAX_AGE.total_seconds())):
        return age, "stale_blocking"
    return age, "within_24h"


def _cost_response_usable(result: BrokerWhatIfCostResponse, freshness_status: str) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if freshness_status != "within_24h":
        blockers.append(freshness_status)
    if not result.costs:
        blockers.append("empty_cost_breakdown")
    for cost in result.costs:
        if cost.currency.upper() != "USD":
            blockers.append(f"unknown_currency:{cost.currency}")
        if cost.amount is None:
            blockers.append(f"undocumented_value_semantics:{cost.cost_type}")
    return not blockers, sorted(set(blockers))


def _classify_scaling(cost_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Classify exact 1x/10x relationships without claiming undocumented units."""
    grouped: dict[tuple[object, ...], list[tuple[Decimal, Decimal]]] = defaultdict(list)
    for row in cost_rows:
        multiplier = Decimal(str(row["multiplier"]))
        for cost in row["costs"]:  # type: ignore[union-attr]
            assert isinstance(cost, dict)
            for field in ("amount", "value"):
                raw_value = cost[field]
                if raw_value is not None:
                    grouped[
                        (
                            row["instrument_id"],
                            row["transaction"],
                            row["settlement_type"],
                            cost["cost_type"],
                            cost["currency"],
                            field,
                        )
                    ].append((multiplier, Decimal(str(raw_value))))

    classifications: list[dict[str, object]] = []
    for key, observations in sorted(grouped.items(), key=lambda item: tuple(str(part) for part in item[0])):
        values = dict(observations)
        base = values.get(Decimal("1"))
        scaled = values.get(Decimal("10"))
        if base is None or scaled is None:
            relationship = "incomplete_blocking"
        elif base == 0 and scaled == 0:
            relationship = "zero_only_uninformative"
        elif scaled == base * Decimal("10"):
            relationship = "ticket_proportional"
        elif scaled == base:
            relationship = "ticket_invariant"
        else:
            relationship = "other_blocking"
        classifications.append(
            {
                "instrument_id": key[0],
                "transaction": key[1],
                "settlement_type": key[2],
                "cost_type": key[3],
                "currency": key[4],
                "field": key[5],
                "equation": "cost(k * base_ticket) = k * cost(base_ticket)" if base is not None else None,
                "relationship": relationship,
                "observations": [
                    {"multiplier": str(multiplier), "cost": str(value)} for multiplier, value in sorted(observations)
                ],
                "execution_semantics": (
                    "documented_order_currency_amount" if key[5] == "amount" else "undocumented_blocking"
                ),
            }
        )
    return classifications


def _coverage(eligibilities: tuple[BrokerInstrumentEligibility, ...]) -> dict[str, object]:
    direction_counts: Counter[str] = Counter()
    settlement_counts: Counter[str] = Counter()
    leverage_counts: Counter[str] = Counter()
    for row in eligibilities:
        for config in row.leverage_configs:
            direction_counts[config.direction] += 1
            settlement_counts[config.settlement_type] += 1
            for leverage in config.leverage_values:
                leverage_counts[str(leverage)] += 1
    return {
        "direction_config_counts": dict(sorted(direction_counts.items())),
        "settlement_config_counts": dict(sorted(settlement_counts.items())),
        "leverage_config_counts": dict(sorted(leverage_counts.items(), key=lambda item: int(item[0]))),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Call demo informational endpoints")
    parser.add_argument("--limit", type=int, default=8, help="Total cohort size, 2..100 (default 8)")
    parser.add_argument(
        "--max-cost-requests",
        type=int,
        default=MAX_COST_REQUESTS_PER_RUN,
        help=f"Hard request cap, 2..{MAX_COST_REQUESTS_PER_RUN} (default {MAX_COST_REQUESTS_PER_RUN})",
    )
    args = parser.parse_args()
    if not 2 <= args.limit <= 100:
        parser.error("--limit must be between 2 and 100 so both Stocks and ETFs are represented")
    if not 2 <= args.max_cost_requests <= MAX_COST_REQUESTS_PER_RUN:
        parser.error(f"--max-cost-requests must be between 2 and {MAX_COST_REQUESTS_PER_RUN}")
    if args.max_cost_requests % len(COST_SCALE_MULTIPLIERS):
        parser.error("--max-cost-requests must be divisible by the number of scaling arms (2)")
    if settings.etoro_env != "demo":
        parser.error("refusing: ETORO_ENV must be demo for this probe")

    with psycopg.connect(settings.database_url) as conn:
        cohort = _load_cohort(conn, args.limit)
    cohort_counts = Counter(row.instrument_type for row in cohort)
    dry_report = {
        "census_version": CENSUS_VERSION,
        "mode": "apply" if args.apply else "dry_run",
        "cohort_definition": {
            "asset_class": US_EQUITY_ASSET_CLASS,
            "instrument_types": list(COHORT_TYPE_DESCRIPTIONS),
            "selection": "equal type quota; nearest deterministic latest-dollar-volume quantiles",
            "requested_total": args.limit,
            "selected_total": len(cohort),
            "selected_by_type": dict(sorted(cohort_counts.items())),
        },
        "request_budget": {
            "eligibility_requests": 1,
            "cost_requests_max": args.max_cost_requests,
            "cost_endpoint_documented_limit_per_60s": MAX_COST_REQUESTS_PER_RUN,
            "scaling_multipliers": [str(value) for value in COST_SCALE_MULTIPLIERS],
        },
        "cohort": [
            {
                "instrument_id": row.instrument_id,
                "symbol": row.symbol,
                "instrument_type": row.instrument_type,
                "dollar_volume": str(row.dollar_volume),
                "local_is_tradable": row.local_is_tradable,
            }
            for row in cohort
        ],
    }
    if not args.apply:
        print(json.dumps(dry_report, indent=2, sort_keys=True))
        return 0

    try:
        api_key, user_key = _load_demo_credentials()
    except (CredentialNotFound, NoOperatorError, AmbiguousOperatorError) as exc:
        logger.error("cannot load demo credentials: %s", exc)
        return 1

    observed_at = datetime.now(UTC)
    with EtoroBrokerProvider(api_key=api_key, user_key=user_key, env="demo") as broker:
        eligibility = broker.check_instrument_eligibility([row.instrument_id for row in cohort])
        all_arms = _interleave_cost_arms(
            eligibility.eligibilities,
            {row.instrument_id: row.instrument_type for row in cohort},
        )
        selected_arms = _bounded_cost_arms(all_arms, args.max_cost_requests)
        cost_rows: list[dict[str, object]] = []
        cost_errors: list[dict[str, object]] = []
        for arm_id, multiplier, order in selected_arms:
            result, error_type = _fetch_cost(broker, order)
            if result is None:
                cost_errors.append(
                    {
                        "arm_id": arm_id,
                        "instrument_id": order.instrument_id,
                        "transaction": order.transaction,
                        "settlement_type": order.settlement_type,
                        "multiplier": str(multiplier),
                        "error_type": error_type,
                    }
                )
                continue
            received_at = datetime.now(UTC)
            age_seconds, freshness_status = _freshness(result.last_updated, received_at)
            usable, blockers = _cost_response_usable(result, freshness_status)
            cost_rows.append(
                {
                    "arm_id": arm_id,
                    "instrument_id": result.instrument_id,
                    "transaction": order.transaction,
                    "settlement_type": order.settlement_type,
                    "multiplier": str(multiplier),
                    "ticket_amount_usd": str(order.amount),
                    "last_updated": result.last_updated.isoformat(),
                    "received_at": received_at.isoformat(),
                    "age_seconds_at_receipt": str(age_seconds),
                    "freshness_status": freshness_status,
                    "canonical_response_bytes": _canonical_payload_bytes(result.raw_payload),
                    "execution_usable": usable,
                    "execution_blockers": blockers,
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

    cohort_by_id = {row.instrument_id: row for row in cohort}
    returned_by_id = {row.instrument_id: row for row in eligibility.eligibilities}
    mismatch_ids = sorted(
        instrument_id
        for instrument_id, member in cohort_by_id.items()
        if member.local_is_tradable
        and (instrument_id not in returned_by_id or not returned_by_id[instrument_id].allow_open_position)
    )
    cost_types = Counter(
        str(cost["cost_type"])
        for row in cost_rows
        for cost in row["costs"]  # type: ignore[union-attr]
        if isinstance(cost, dict)
    )
    currencies = Counter(
        str(cost["currency"])
        for row in cost_rows
        for cost in row["costs"]  # type: ignore[union-attr]
        if isinstance(cost, dict)
    )
    report = dry_report | {
        "observed_at": observed_at.isoformat(),
        "population": {
            "requested": len(cohort),
            "resolved": len(eligibility.eligibilities),
            "refused_open": sum(not row.allow_open_position for row in eligibility.eligibilities),
            "not_found_instrument_ids": list(eligibility.not_found_instrument_ids),
            "not_found_symbols": list(eligibility.not_found_symbols),
            "local_tradable_broker_open_mismatch_count": len(mismatch_ids),
            "local_tradable_broker_open_mismatch_ids": mismatch_ids,
        },
        "eligibility_canonical_response_bytes": _canonical_payload_bytes(eligibility.raw_payload),
        "eligibility_coverage": _coverage(eligibility.eligibilities),
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
        "cost_population": {
            "eligible_arms": len(all_arms),
            "requested_arms": len(selected_arms),
            "skipped_by_rate_budget": len(all_arms) - len(selected_arms),
            "resolved_arms": len(cost_rows),
            "errored_arms": len(cost_errors),
            "execution_usable_arms": sum(bool(row["execution_usable"]) for row in cost_rows),
            "canonical_response_bytes": sum(
                value for row in cost_rows if isinstance((value := row["canonical_response_bytes"]), int)
            ),
            "cost_type_row_counts": dict(sorted(cost_types.items())),
            "currency_row_counts": dict(sorted(currencies.items())),
        },
        "cost_errors": cost_errors,
        "cost_scaling": _classify_scaling(cost_rows),
        "cost_preflights": cost_rows,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
