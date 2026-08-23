"""#2838 stage-1 leg 1 -- what settlement arms does eToro offer on the vol ETPs?

READ-ONLY against the broker.  The only call is
``POST /api/v2/trading/info/{env}/eligibility``, an informational endpoint;
nothing here submits, sizes, prices or authorises an order, and no position or
order state is touched.

## The question this answers, and the one it does not

#2838's stage 1 gates the whole short-VIX carry family on a fee measurement, and
splits into three legs:

1. **eligibility** -- is there any non-CFD settlement for SVXY/SVOL, or is the UK
   account restricted to an x1 long CFD?  ``presumed no under PRIIPs``, which is
   a premise, not a measurement.  **This script is that leg.**
2. **a ~$50 demo x1 long CFD held 7 nights spanning a Friday**, reading
   ``overnightFee`` / ``overWeekendFee`` off the position.  Not run here: opening
   a position is a broker mutation, and demo fills are persisted writes.
3. **the what-if endpoint as a cross-check.**  ⚠ Already known to be
   **undecodable**, and this script does not re-run it.
   ``.claude/skills/data-sources/etoro-api.md`` records ``markup`` and
   ``overnightFee`` reading ``0.0`` on all 28 observations to date, x1 short CFDs
   included -- and an all-zero component cannot distinguish "0 dollars" from
   "0 percent".  Leg 3 cannot pass or fail the 2%/yr bar in either direction.

So leg 1 is decisive in exactly ONE direction.  If a ``real`` long x1 arm exists
the instrument can be held at full value, there is no CFD financing to measure,
and stage 1 passes without leg 2.  If every arm is ``cfd``, the premise is
confirmed and leg 2 remains the only instrument that can settle the fee -- which
makes the stage-1 bar operator-gated rather than merely unmeasured.

## Why this does not write a proof row

``scripts/prove_2603_core_eligibility.py`` asks the same question of the broker
and records the answer in ``strategy_core_eligibility_proofs``.  That table is
not a neutral log: ``app/workers/scheduler.py``'s ``quotes_refresh`` selects
instruments whose LATEST proof verdict passes, so writing one here would enrol
these ETPs in a scheduled job's working set as a side effect of a feasibility
question.  A research probe must not move an operational set, so this prints.

The "what counts as the underlying" definition is NOT re-implemented -- it is
imported from ``app.services.broker_settlement_arms``, which is where #2603 put
it precisely so a second caller could not drift from it.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg

from app.config import settings
from app.providers.broker import BrokerInstrumentEligibility
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.broker_settlement_arms import is_underlying_long_arm
from app.services.operators import sole_operator_id

logger = logging.getLogger("probe_2838_vol_carry_settlement_arms")

PROVIDER = "etoro"
CALLER = "probe_2838_vol_carry_settlement_arms"
# eToro documents the eligibility endpoint at 20 requests/minute DEDICATED
# (live portal 2026-08-13, mirrored in the etoro-api skill).  One request per
# symbol at 3.2s apart stays inside that with headroom.
REQUEST_INTERVAL_S = 3.2

# SVXY and SVOL are the vehicles #2838 names.  VXX and UVXY are the long-vol
# side of the same term structure and are requested as CONTEXT: if the whole
# vol-ETP shelf reads identically, "CFD-only" is a property of the shelf rather
# than of the two names the ticket happens to have picked.
DEFAULT_SYMBOLS = ("SVXY", "SVOL", "VXX", "UVXY")


def _credentials(conn: psycopg.Connection[Any], *, operator_id: UUID, environment: str) -> tuple[str, str]:
    ensure_broker_key_loaded(conn)
    api_key = load_credential_for_provider_use(
        conn,
        operator_id=operator_id,
        provider=PROVIDER,
        label="api_key",
        environment=environment,
        caller=CALLER,
    )
    conn.commit()
    user_key = load_credential_for_provider_use(
        conn,
        operator_id=operator_id,
        provider=PROVIDER,
        label="user_key",
        environment=environment,
        caller=CALLER,
    )
    conn.commit()
    return api_key, user_key


def _describe(row: BrokerInstrumentEligibility) -> dict[str, object]:
    """Every arm the response carried, plus the leg-1 verdict over them.

    The full vocabulary is reported rather than the qualifying arm alone.
    #2838 asks whether ANY non-CFD settlement exists, so an arm that is `real`
    but short, or `real` but leveraged, is evidence about the shelf even though
    it does not qualify -- and collapsing to the qualifying arm would answer a
    narrower question while looking like an answer to the wider one.
    """
    arms = [
        {
            "settlement_type": arm.settlement_type,
            "direction": arm.direction,
            "leverage_values": list(arm.leverage_values),
            "min_position_amount": None if arm.min_position_amount is None else str(arm.min_position_amount),
            "is_underlying_long_x1": is_underlying_long_arm(arm),
        }
        for arm in row.leverage_configs
    ]
    settlement_types = sorted({arm.settlement_type.strip().lower() for arm in row.leverage_configs})
    return {
        "instrument_id": row.instrument_id,
        "symbol": row.symbol,
        "allow_open_position": row.allow_open_position,
        "allow_close_position": row.allow_close_position,
        "min_position_exposure": None if row.min_position_exposure is None else str(row.min_position_exposure),
        "settlement_types_offered": settlement_types,
        # The leg-1 question, stated as the two facts that answer it.
        "has_underlying_long_x1_arm": any(arm["is_underlying_long_x1"] for arm in arms),
        "cfd_only": settlement_types == ["cfd"],
        "arms": arms,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment",
        default="demo",
        choices=("demo", "real"),
        help="broker environment to ask (default: demo)",
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        default=list(DEFAULT_SYMBOLS),
        help=f"symbols to measure (default: {' '.join(DEFAULT_SYMBOLS)})",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    symbols = [symbol.upper() for symbol in (args.symbols or list(DEFAULT_SYMBOLS))]

    with psycopg.connect(settings.database_url) as conn:
        operator_id = sole_operator_id(conn)
        api_key, user_key = _credentials(conn, operator_id=operator_id, environment=args.environment)
        rows = conn.execute(
            "SELECT instrument_id, symbol FROM instruments WHERE symbol = ANY(%s)",
            (symbols,),
        ).fetchall()
        conn.commit()

    found = {str(symbol): int(instrument_id) for instrument_id, symbol in rows}
    missing = [symbol for symbol in symbols if symbol not in found]
    if missing:
        logger.error("unknown symbols (not in `instruments`): %s", ", ".join(missing))
        return 1

    measured: list[dict[str, object]] = []
    not_found: list[str] = []
    for symbol in symbols:
        instrument_id = found[symbol]
        with EtoroBrokerProvider(api_key=api_key, user_key=user_key, env=args.environment) as broker:
            response = broker.check_instrument_eligibility([instrument_id])
        if response.not_found_instrument_ids:
            # The broker not knowing an instrument our universe lists is a real
            # observation about the shelf, and is NOT the same as "no arms".
            not_found.append(symbol)
        for row in response.eligibilities:
            measured.append({"requested_symbol": symbol, **_describe(row)})
        time.sleep(REQUEST_INTERVAL_S)

    underlying = [row["requested_symbol"] for row in measured if row["has_underlying_long_x1_arm"]]
    cfd_only = [row["requested_symbol"] for row in measured if row["cfd_only"]]
    print(
        json.dumps(
            {
                # Stamped so a redirected capture is datable evidence rather
                # than an undated JSON.  Eligibility is an account-and-moment
                # observation: the shelf can change without our noticing.
                "observed_at": datetime.now(UTC).isoformat(),
                "environment": args.environment,
                "requested": symbols,
                "not_found_by_broker": not_found,
                # The leg-1 readout, in the ticket's own terms.
                "with_underlying_long_x1_arm": underlying,
                "cfd_only": cfd_only,
                "instruments": measured,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
