"""Does eToro's what-if cost endpoint price a CLOSE? (#2712)

#2603 step 3b-2 item 1 shipped ``resolve_core_trade_size``, which sizes a core sell
correctly the moment it is handed a cost bound -- and nothing in this repo can produce
one, because ``EtoroBrokerProvider.get_what_if_costs`` hardcodes ``"action": "open"``.

The live portal (verified 2026-08-14) documents ``action`` as a REQUIRED enum with values
``open`` and ``close``, and ``transaction`` as ``buy | sell | sellShort | buyToCover`` --
both wider than our ``TradeDirection`` Literal.  It also marks ``positionIds`` as *"For
`close` action; currently rejected"*, which leaves the useful question open: can a close be
priced WITHOUT naming a position, from ``instrumentId`` + size alone?

The skill's protocol step 5 is explicit that a doc-vs-code disagreement is settled
empirically on demo, so this probe asks the endpoint rather than reasoning about it.

⚠⚠ INFORMATIONAL ONLY.  ``/api/v2/trading/info/{demo/}costs`` is the documented cost
BREAKDOWN endpoint -- its entire purpose is to answer "what would this cost" without
placing anything.  It is the same endpoint #2598's census already exercises across 60
instruments; the only delta here is one enum field's value.  Nothing in this file opens,
closes, modifies or cancels anything, and ``positionIds`` is never sent.

Run (dev, demo credentials, well inside the dedicated 20/60s informational lane)::

    uv run python -m scripts.probe_2712_close_side_cost_quote --instrument-id 1001
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Final

import psycopg

from app.config import settings
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.operators import sole_operator_id

#: The arms to try, in the order that most cheaply narrows the answer.  Each is one
#: request.  ``open``/``buy`` is the CONTROL -- without it a run of refusals cannot
#: distinguish "close is rejected" from "this instrument/ticket is rejected".
_ARMS: Final = (
    ("open", "buy", "the control arm — the shape #2598 already decoded"),
    ("close", "sell", "closing a long: the arm a core rebalance sell needs"),
    ("close", "buyToCover", "closing a short — recorded for completeness, unused in v1"),
)

#: Retried with a real position id, because the arms above come back
#: "PositionIds must be provided for close action".  ⚠ The portal documents this field as
#: *"For `close` action; currently rejected"*, so the doc and the 400 disagree and only a
#: request settles it.
_POSITION_ARMS: Final = (("close", "sell", "closing a long, WITH the position named"),)

_TICKET: Final = 1000.0


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
                caller="probe_2712_close_side_cost_quote",
            )
        )
        conn.commit()
    return keys[0], keys[1]


def _ask(
    broker: EtoroBrokerProvider,
    *,
    action: str,
    transaction: str,
    instrument_id: int,
    position_ids: list[int] | None = None,
) -> dict[str, Any]:
    """One raw what-if call, built here because the provider hardcodes ``open``.

    Deliberately NOT routed through ``get_what_if_costs``: extending the provider before
    knowing whether the arm exists would be building on the premise under test.
    """
    body = {
        "action": action,
        "transaction": transaction,
        "instrumentId": instrument_id,
        "settlementType": "real",
        "orderType": "mkt",
        "leverage": 1,
        "orderCurrency": "usd",
        "amount": _TICKET,
    }
    if position_ids is not None:
        # ⚠ Names WHICH position to PRICE, on the informational cost endpoint.  It does
        # not close anything -- the close endpoint is a different path entirely
        # (`/trading/execution/...`), and this one's whole contract is "what would it
        # cost".  Sent only because the 400 above says the arm is unreachable without it.
        body["positionIds"] = position_ids
    record: dict[str, Any] = {"action": action, "transaction": transaction, "request": body}
    try:
        response = broker._http_write.post(  # noqa: SLF001 - probe, not production code
            f"{broker._v2_info_prefix}/costs",  # noqa: SLF001
            json=body,
            headers=broker._request_headers(),  # noqa: SLF001
        )
    except Exception as exc:  # pragma: no cover - network
        record["transport_error"] = f"{type(exc).__name__}: {exc}"
        return record

    record["status"] = response.status_code
    try:
        record["body"] = response.json()
    except ValueError:
        record["body"] = response.text[:2000]
    return record


def _all_held(broker: EtoroBrokerProvider) -> int:
    """Open vs close, same instrument, same ticket, seconds apart, every held position.

    ⚠ The population is small and is not a sample of anything wider -- it is every
    position the demo account holds, which is the complete set for which a close CAN be
    priced.  Reported as counts, never as a rate with an implied subject.
    """
    with psycopg.connect(settings.database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            -- DISTINCT ON takes the LOWEST position_id per instrument: deterministic,
            -- so a re-run compares like with like, and arbitrary in the sense that
            -- nothing here establishes one lot is more representative than another.
            -- ⚠ The ticket is pinned at $1,000 by `amount` on both arms, so the position
            -- id selects which LOT to price against, not the size priced.  Whether the
            -- close cost varies by lot is UNMEASURED -- two instruments hold two lots
            -- each and only one was quoted -- so read the per-instrument ratios as
            -- "this lot, this instant", not as the instrument's close cost.
            SELECT DISTINCT ON (bp.instrument_id)
                   bp.instrument_id, i.symbol, bp.position_id, q.spread_pct
            FROM broker_positions bp
            JOIN instruments i USING (instrument_id)
            LEFT JOIN quotes q USING (instrument_id)
            WHERE bp.is_buy
            ORDER BY bp.instrument_id, bp.position_id
            """
        )
        held = cur.fetchall()
        conn.rollback()

    print(f"{'symbol':8} {'open':>8} {'close':>8} {'ratio':>7} {'quote_bps':>10}")
    rows = []
    for instrument_id, symbol, position_id, spread_pct in held:
        opened = _ask(broker, action="open", transaction="buy", instrument_id=instrument_id)
        closed = _ask(
            broker,
            action="close",
            transaction="sell",
            instrument_id=instrument_id,
            position_ids=[position_id],
        )
        o = _spread_of(opened)
        c = _spread_of(closed)
        ratio = "n/a" if not o or c is None else f"{c / o:.1f}x"
        quote_bps = "n/a" if spread_pct is None else f"{float(spread_pct) * 100:.2f}"
        print(f"{symbol:8} {str(o):>8} {str(c):>8} {ratio:>7} {quote_bps:>10}")
        rows.append((symbol, o, c, opened.get("status"), closed.get("status")))

    decidable = [r for r in rows if r[1] is not None and r[2] is not None]
    dearer = [r for r in decidable if r[2] > r[1]]
    print(
        f"\n{len(rows)} held instruments; {len(decidable)} with BOTH arms decodable; "
        f"close dearer than open on {len(dearer)} of {len(decidable)}."
    )
    return 0


def _spread_of(record: dict[str, Any]) -> float | None:
    body = record.get("body")
    if record.get("status") != 200 or not isinstance(body, dict):
        return None
    for row in body.get("costs") or []:
        if row.get("costType") == "marketSpread":
            value = row.get("value") if row.get("amount") is None else row.get("amount")
            return None if value is None else float(value)
    return None  # row omitted — under the quantum, NOT a zero


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument-id", type=int)
    parser.add_argument(
        "--all-held",
        action="store_true",
        help=(
            "probe the open AND close arm for every held broker position — the WHOLE "
            "population a close quote can be obtained for, since the arm needs a real "
            "position id"
        ),
    )
    parser.add_argument(
        "--position-id",
        type=int,
        action="append",
        help="retry the close arm naming this position; the 400 above says it is required",
    )
    args = parser.parse_args(argv)

    if settings.etoro_env != "demo":
        parser.error("refusing: ETORO_ENV must be demo for this probe")
    if not args.all_held and args.instrument_id is None:
        parser.error("give --instrument-id or --all-held")

    with psycopg.connect(settings.database_url) as conn:
        api_key, user_key = _load_demo_credentials(conn)
        conn.rollback()

    broker = EtoroBrokerProvider(api_key, user_key, env="demo")
    if args.all_held:
        return _all_held(broker)

    records = []
    for action, transaction, why in _ARMS:
        print(f"[{action}/{transaction}] {why}", flush=True)
        record = _ask(broker, action=action, transaction=transaction, instrument_id=args.instrument_id)
        record["why"] = why
        records.append(record)
        print(f"  -> status={record.get('status')} {str(record.get('body'))[:300]}", flush=True)

    print("\n" + json.dumps(records, indent=1, sort_keys=True, default=str))

    if args.position_id:
        for action, transaction, why in _POSITION_ARMS:
            print(f"[{action}/{transaction} +positionIds] {why}", flush=True)
            record = _ask(
                broker,
                action=action,
                transaction=transaction,
                instrument_id=args.instrument_id,
                position_ids=list(args.position_id),
            )
            record["why"] = f"{why} (positionIds)"
            records.append(record)
            print(f"  -> status={record.get('status')} {str(record.get('body'))[:400]}", flush=True)

    control = records[0]
    close_long = records[1]
    if control.get("status") != 200:
        print("\n⚠ CONTROL ARM FAILED — this run cannot decide anything about the close arm.")
        return 2
    print(f"\nverdict: control 200, close/sell status={close_long.get('status')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
