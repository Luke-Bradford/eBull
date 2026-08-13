"""Decide what eToro's undocumented what-if ``value`` field is DENOMINATED IN.

Refs #2598. Informational demo endpoints only — this never places, modifies or
closes an order.

⚠⚠ SCALING ALONE CANNOT PROVE A UNIT, which is why #2446's census stopped short.
A component that doubles when the ticket doubles is proportional to notional;
that is equally true of a monetary amount and of a rate applied to notional. The
1x/10x arms in ``verify_2437_trading_preflight`` establish proportionality and
then have nothing left to say.

What settles it is an INDEPENDENT measurement of the same quantity. We hold one:
``quotes`` stores an observed ``bid``/``ask``/``spread_pct`` per instrument from
our own feed, produced by a code path that has never read a what-if response. If
``value`` is a monetary USD amount then ``value / ticket_amount`` is a spread
fraction and must land on the separately observed quoted spread. If ``value``
were a rate, that ratio would be smaller by four orders of magnitude and would
match nothing.

⚠ THE $100 TICKET IS THE CONTROL, NOT A SECOND SAMPLE. Costs come back rounded
to 0.01 USD, so on a $100 ticket the quantum alone is 1.0 bp and every tight
instrument reads ~1.0 bp regardless of its real spread. The agreement therefore
has to appear at $1,000 and be ABSENT at $100 — a run where both ticket sizes
agree with the quote is measuring something other than what this claims to.

Run (dev, demo credentials, ~2 requests per target within the dedicated
20/60s lane):

    PYTHONPATH=. uv run python -m scripts.verify_2598_preflight_quote_crosscheck
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from typing import Any, Final

import httpx
import psycopg

from app.config import settings
from app.providers.broker import BrokerWhatIfOrder
from app.providers.implementations.etoro_broker import EtoroBrokerProvider, TradingPreflightParseError
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.operators import sole_operator_id

#: Instruments carried BOTH by the quote panel and by the demo broker, spanning
#: a large-cap stock, two ETFs and a thinner name. Deliberately not the
#: dollar-volume cohort ``verify_2437_trading_preflight`` selects: that cohort
#: has zero overlap with ``quotes`` (measured 2026-08-12), so it cannot be
#: cross-checked against anything at all.
DEFAULT_TARGETS: Final[tuple[int, ...]] = (1001, 3000, 3017, 9660)

#: The control ticket and the measuring ticket. See the module docstring: the
#: first exists to show the rounding quantum swamping the signal.
TICKETS: Final[tuple[Decimal, ...]] = (Decimal("100"), Decimal("1000"))

#: eToro returns costs rounded to a whole cent.
COST_QUANTUM_USD: Final = Decimal("0.01")


def _load_demo_credentials(conn: psycopg.Connection[Any]) -> tuple[str, str]:
    ensure_broker_key_loaded(conn)
    operator_id = sole_operator_id(conn)
    keys: list[str] = []
    for label in ("api_key", "user_key"):
        keys.append(
            load_credential_for_provider_use(
                conn,
                operator_id=operator_id,
                provider="etoro",
                label=label,
                environment="demo",
                caller="verify_2598_preflight_quote_crosscheck",
            )
        )
        conn.commit()
    return keys[0], keys[1]


def _observed_quotes(conn: psycopg.Connection[Any], targets: tuple[int, ...]) -> dict[int, tuple[Any, ...]]:
    rows = conn.execute(
        """
        SELECT q.instrument_id, i.symbol, q.bid, q.ask, q.spread_pct, q.quoted_at
        FROM quotes q JOIN instruments i USING (instrument_id)
        WHERE q.instrument_id = ANY(%(ids)s)
        """,
        {"ids": list(targets)},
    ).fetchall()
    return {int(row[0]): row for row in rows}


def _probe(broker: EtoroBrokerProvider, quotes: dict[int, tuple[Any, ...]], targets: tuple[int, ...]) -> list[dict]:
    observations: list[dict] = []
    for instrument_id in targets:
        quote = quotes.get(instrument_id)
        for ticket in TICKETS:
            record: dict[str, Any] = {
                "instrument_id": instrument_id,
                "symbol": None if quote is None else quote[1],
                "transaction": "buy",
                "settlement_type": "real",
                "ticket_amount_usd": str(ticket),
                "rounding_floor_bps": float(COST_QUANTUM_USD / ticket * 10000),
            }
            try:
                result = broker.get_what_if_costs(
                    BrokerWhatIfOrder(
                        instrument_id=instrument_id,
                        transaction="buy",
                        settlement_type="real",
                        amount=ticket,
                    )
                )
            # ⚠ Narrowed to the set the sibling census already established
            # (``verify_2437_trading_preflight._fetch_cost``) rather than a bare
            # ``Exception``: one malformed arm must not destroy the run, but an
            # unexpected failure class should surface rather than be recorded as
            # a data point. The MESSAGE is retained alongside the type — a type
            # name alone cannot tell a 429 from a 400 after the fact.
            except (httpx.HTTPError, TradingPreflightParseError, json.JSONDecodeError) as exc:
                record["error"] = type(exc).__name__
                record["error_detail"] = str(exc)[:300]
                observations.append(record)
                continue

            # ⚠ A COST ROW THAT IS ABSENT IS NOT A ZERO COST. Measured on AAPL at
            # a $100 ticket: the real spread is below the 0.01 USD quantum and
            # eToro OMITS the marketSpread row rather than sending 0.0. Coercing
            # the gap to zero would price the tightest names as free.
            #
            # ⚠⚠ MEMBERSHIP AND VALUE ARE TESTED SEPARATELY AND MUST STAY THAT
            # WAY. The rounding claim above rests on the row being ABSENT, and a
            # single `rows.get(...) is not None` cannot tell an omitted row from
            # a row present with a null value — it would report the second as
            # the first and corroborate the claim with the wrong observation.
            rows = {cost.cost_type: cost.value for cost in result.costs}
            spread_present = "marketSpread" in rows
            spread_value = rows.get("marketSpread")
            record["cost_rows"] = {name: (None if value is None else str(value)) for name, value in rows.items()}
            record["market_spread_row_present"] = spread_present
            record["market_spread_value_null"] = spread_present and spread_value is None
            record["implied_bps_if_monetary"] = (
                None if spread_value is None else round(float(spread_value) / float(ticket) * 10000, 2)
            )
            record["observed_quote_bps"] = None if quote is None else round(float(quote[4]) * 100, 2)
            record["quote_bid"] = None if quote is None else str(quote[2])
            record["quote_ask"] = None if quote is None else str(quote[3])
            record["quoted_at"] = None if quote is None else quote[5].isoformat()
            record["cost_last_updated"] = result.last_updated.isoformat()
            observations.append(record)
    return observations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instrument-id",
        type=int,
        action="append",
        help="Override a target; repeatable. Must be present in `quotes` or the cross-check is vacuous.",
    )
    args = parser.parse_args(argv)
    targets = tuple(args.instrument_id) if args.instrument_id else DEFAULT_TARGETS

    if settings.etoro_env != "demo":
        parser.error("refusing: ETORO_ENV must be demo for this probe")

    with psycopg.connect(settings.database_url) as conn:
        api_key, user_key = _load_demo_credentials(conn)
        quotes = _observed_quotes(conn, targets)
        conn.rollback()

    missing = [instrument_id for instrument_id in targets if instrument_id not in quotes]
    if missing:
        sys.stderr.write(
            f"warning: no observed quote for {missing} — those rows cannot corroborate anything and are reported "
            "with a null observed_quote_bps rather than dropped\n"
        )

    observations = _probe(EtoroBrokerProvider(api_key, user_key, env="demo"), quotes, targets)
    sys.stdout.write(json.dumps(observations, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
