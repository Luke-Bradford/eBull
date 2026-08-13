"""#2603 item 2 -- observe and record core-instrument eligibility proofs.

Two modes, both READ-ONLY against the broker:

``prove SYMBOL [SYMBOL ...]``
    One informational eligibility request per instrument, each recorded as one
    append-only row in ``strategy_core_eligibility_proofs``.  This is also the
    revalidation instrument: a proof ages out after
    ``CORE_ELIGIBILITY_MAX_AGE`` and re-proving is re-running this.

``census``
    The full-population measurement the spec's figures come from.  Reproduces
    them rather than trusting a number written down, per
    ``.claude/CLAUDE.md``'s "never hardcode a derived statistic" rule.

⚠ Only ``POST /trading/info/{env}/eligibility`` is called.  Nothing here submits,
sizes or authorises an order, and no position or order state is touched.

⚠ A transport failure records NOTHING.  Storing "we could not ask" as an
observation would turn absence of evidence into broker evidence.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from typing import Any
from uuid import UUID

import psycopg

from app.config import settings
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.broker_settlement_arms import select_underlying_long_arms
from app.services.operators import sole_operator_id
from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_REQUEST_CURRENCY,
    evaluate_core_eligibility,
    record_core_eligibility_proof,
)

logger = logging.getLogger("prove_2603_core_eligibility")

PROVIDER = "etoro"
RECORDED_BY = "prove_2603_core_eligibility"
# eToro documents the eligibility endpoint at 20 requests/minute DEDICATED and at
# most 100 ids per request (live portal 2026-08-13).  3.2s between requests keeps
# a 128-batch census inside that budget with headroom.
BATCH_SIZE = 100
REQUEST_INTERVAL_S = 3.2


def _credentials(conn: psycopg.Connection[Any], *, operator_id: UUID, environment: str) -> tuple[str, str, UUID, UUID]:
    """Plaintext keys plus the credential-row ids the proof is attributed to."""
    ensure_broker_key_loaded(conn)
    api_key = load_credential_for_provider_use(
        conn,
        operator_id=operator_id,
        provider=PROVIDER,
        label="api_key",
        environment=environment,
        caller=RECORDED_BY,
    )
    conn.commit()
    user_key = load_credential_for_provider_use(
        conn,
        operator_id=operator_id,
        provider=PROVIDER,
        label="user_key",
        environment=environment,
        caller=RECORDED_BY,
    )
    conn.commit()
    rows = conn.execute(
        """
        SELECT label, id FROM broker_credentials
        WHERE operator_id = %s AND provider = %s AND environment = %s
          AND revoked_at IS NULL AND label IN ('api_key','user_key')
        """,
        (operator_id, PROVIDER, environment),
    ).fetchall()
    live = {str(label): value for label, value in rows}
    return api_key, user_key, live["api_key"], live["user_key"]


def _prove(args: argparse.Namespace) -> int:
    with psycopg.connect(settings.database_url) as conn:
        operator_id = sole_operator_id(conn)
        api_key, user_key, api_key_id, user_key_id = _credentials(
            conn, operator_id=operator_id, environment=args.environment
        )
        rows = conn.execute(
            "SELECT instrument_id, symbol FROM instruments WHERE symbol = ANY(%s)",
            (list(args.symbols),),
        ).fetchall()
        conn.commit()
        found = {str(symbol): int(instrument_id) for instrument_id, symbol in rows}
        missing = [symbol for symbol in args.symbols if symbol not in found]
        if missing:
            logger.error("unknown symbols (not in `instruments`): %s", ", ".join(missing))
            return 1

        results: list[dict[str, object]] = []
        for symbol in args.symbols:
            instrument_id = found[symbol]
            # One instrument per request, deliberately: the proof digests the
            # WHOLE response, which is only sound when the response is about
            # exactly this instrument.
            with EtoroBrokerProvider(api_key=api_key, user_key=user_key, env=args.environment) as broker:
                response = broker.check_instrument_eligibility([instrument_id])
            assessment = evaluate_core_eligibility(
                response,
                instrument_id=instrument_id,
                requested_currency=CORE_ELIGIBILITY_REQUEST_CURRENCY,
            )
            with conn.transaction():
                proof_id = record_core_eligibility_proof(
                    conn,
                    assessment=assessment,
                    instrument_id=instrument_id,
                    operator_id=operator_id,
                    provider=PROVIDER,
                    environment=args.environment,
                    api_key_credential_id=api_key_id,
                    user_key_credential_id=user_key_id,
                    recorded_by=RECORDED_BY,
                )
            results.append(
                {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "proof_id": proof_id,
                    "verdict": assessment.verdict,
                    "reason_code": assessment.reason_code,
                    "qualifying_arm_count": assessment.qualifying_arm_count,
                    "settlement_type": assessment.settlement_type,
                    "min_position_amount": str(assessment.min_position_amount),
                    "min_position_exposure": str(assessment.min_position_exposure),
                    "response_digest": assessment.response_digest[:16] + "...",
                }
            )
            time.sleep(REQUEST_INTERVAL_S)

    print(json.dumps({"environment": args.environment, "proofs": results}, indent=2))
    return 0


def _census(args: argparse.Namespace) -> int:
    """Measure the whole tradable universe's settlement arms.

    Reports the arm vocabulary per instrument type, and for ETFs the currency
    split -- the two figures the spec cites.  Every number printed here is
    computed at run time; none is written down anywhere.
    """
    with psycopg.connect(settings.database_url) as conn:
        operator_id = sole_operator_id(conn)
        api_key, user_key, _, _ = _credentials(conn, operator_id=operator_id, environment=args.environment)
        universe = conn.execute(
            """
            SELECT instrument_id, symbol, instrument_type_id, upper(currency)
            FROM instruments WHERE is_tradable ORDER BY instrument_id
            """
        ).fetchall()
        type_names = dict(conn.execute("SELECT instrument_type_id, name FROM etoro_instrument_types").fetchall())
        conn.commit()

    meta = {int(row[0]): row for row in universe}
    ids = sorted(meta)
    resolved: Counter[int] = Counter()
    underlying: Counter[int] = Counter()
    arms: Counter[str] = Counter()
    etf_by_ccy: Counter[str] = Counter()
    etf_underlying_by_ccy: Counter[str] = Counter()
    not_found = 0

    for start in range(0, len(ids), BATCH_SIZE):
        chunk = ids[start : start + BATCH_SIZE]
        with EtoroBrokerProvider(api_key=api_key, user_key=user_key, env=args.environment) as broker:
            response = broker.check_instrument_eligibility(chunk)
        not_found += len(response.not_found_instrument_ids)
        for row in response.eligibilities:
            _, _, type_id, currency = meta[row.instrument_id]
            type_id = int(type_id)
            currency = str(currency)
            resolved[type_id] += 1
            for arm in row.leverage_configs:
                arms[f"{type_names.get(type_id, type_id)}:{arm.settlement_type}/{arm.direction}"] += 1
            is_underlying = bool(select_underlying_long_arms(row))
            if is_underlying:
                underlying[type_id] += 1
            if type_id == 6:
                etf_by_ccy[currency] += 1
                if is_underlying:
                    etf_underlying_by_ccy[currency] += 1
        logger.info("resolved %d/%d", sum(resolved.values()), len(ids))
        time.sleep(REQUEST_INTERVAL_S)

    print(
        json.dumps(
            {
                "environment": args.environment,
                "requested": len(ids),
                "resolved": sum(resolved.values()),
                "not_found": not_found,
                "by_type": {
                    str(type_names.get(type_id, type_id)): {
                        "resolved": count,
                        "with_underlying_long_x1_arm": underlying[type_id],
                    }
                    for type_id, count in sorted(resolved.items())
                },
                "etf_by_currency": {
                    currency: {
                        "resolved": count,
                        "with_underlying_long_x1_arm": etf_underlying_by_ccy[currency],
                    }
                    for currency, count in etf_by_ccy.most_common()
                },
                "arm_vocabulary": dict(sorted(arms.items())),
            },
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment",
        default="demo",
        choices=("demo", "real"),
        help="broker environment the proof is attributed to (default: demo)",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    prove = sub.add_parser("prove", help="record one proof per symbol")
    prove.add_argument("symbols", nargs="+")
    prove.set_defaults(func=_prove)
    census = sub.add_parser("census", help="measure the whole tradable universe")
    census.set_defaults(func=_census)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
