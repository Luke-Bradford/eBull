"""#3616 slice 3c: does eToro's v2 order lookup carry the evidence the tax ledger needs, for every order we hold?

Read-only. For every distinct broker order id in ``trade_events`` (sync ``orderID`` and history ``orderId``), calls the
documented ``orders:lookup`` operation (``trading--demo/get-order-information-and-position-details``) and records:

- the HTTP status, so retention is measured rather than assumed;
- ``asset.settlementType`` (``cfd``/``real``/...), ``asset.leverage``, ``asset.side``, ``transaction``, ``action``;
- whether ``accountId`` and ``gcid`` are present and equal across orders (values are never printed or stored in the
  summary: they identify the account holder);
- per position execution: ``positionId``, ``state``, ``remainingUnits``, ``openingData.units``/``executionTime``/
  ``fees``/``taxes``/``avgPrice``/``avgConversionRate``, ``investedAmountCurrency``;
- and whether every ``trade_events`` position of the order appears among its executions.

The raw bodies go to ``var/research/3616/slice3c/order_lookup_raw.json`` (gitignored: they carry account identifiers).
The summary printed to stdout carries counts and order-level shapes only.

``--checks`` reads the saved bodies (no network) and prints, per order, the checks the slice 3c spec freezes: unit
conservation (execution units = closed units + units held in ``broker_positions``), the consideration corroboration
``|units x openRate x openConversion - consideration| / consideration``, the proceeds corroboration
``|units x closeRate - (investment + netProfit)| / proceeds`` for USD-priced close pieces, and per-indicator counts of
observed and non-zero charge values.

Run (dev, demo credentials, one GET per order at <= 1 per 1.5 s, inside the documented 60/60 s quota)::

    PYTHONPATH=. uv run python -m scripts.measure_3616_slice3c_order_lookup
    PYTHONPATH=. uv run python -m scripts.measure_3616_slice3c_order_lookup --checks
"""

from __future__ import annotations

import json
import pathlib
import time
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.operators import sole_operator_id

_CALLER = "measure_3616_slice3c_order_lookup"
_OUT = pathlib.Path("var/research/3616/slice3c/order_lookup_raw.json")

_ORDERS_SQL = """
SELECT COALESCE(te.order_id, (te.raw_payload->>'orderID')::bigint) AS order_id,
       te.position_id, te.source, te.event_kind, te.units, te.executed_at, i.symbol,
       te.raw_payload->>'settlementTypeID' AS settlement_type_id
FROM trade_events te
LEFT JOIN instruments i USING (instrument_id)
ORDER BY 1, te.executed_at
"""


def _credentials(conn: psycopg.Connection[Any]) -> tuple[str, str]:
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
                caller=_CALLER,
            )
        )
        conn.commit()
    return keys[0], keys[1]


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_ORDERS_SQL).fetchall()
        api_key, user_key = _credentials(conn)

    events: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    for row in rows:
        events[row[0]].append(row[1:])

    raw: dict[str, Any] = {}
    with EtoroBrokerProvider(api_key, user_key, env="demo") as broker:
        for order_id in sorted(events):
            response = broker._http_read.get(  # noqa: SLF001 — raw body wanted even where the strict parser refuses
                f"{broker._v2_info_prefix}/orders:lookup",  # noqa: SLF001
                params={"orderId": order_id},
                headers=broker._request_headers(),  # noqa: SLF001
            )
            try:
                body: Any = response.json()
            except ValueError:
                body = {"_non_json": response.text[:500]}
            raw[str(order_id)] = {"http_status": response.status_code, "body": body}
            time.sleep(1.5)

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    _OUT.write_text(json.dumps(raw, indent=2, default=str))

    statuses = Counter(entry["http_status"] for entry in raw.values())
    account_ids = {json.dumps(e["body"].get("accountId")) for e in raw.values() if e["http_status"] == 200}
    gcids = {json.dumps(e["body"].get("gcid")) for e in raw.values() if e["http_status"] == 200}
    print(f"orders={len(raw)} http_status={dict(statuses)}")
    print(f"distinct accountId among 200s={len(account_ids)} distinct gcid={len(gcids)}")
    for order_id in sorted(events):
        entry = raw[str(order_id)]
        symbols = sorted({e[5] or "?" for e in events[order_id]})
        stored = sorted({(e[0], e[1], e[2]) for e in events[order_id]})
        stored_positions = {e[0] for e in events[order_id]}
        sync_types = sorted({e[6] for e in events[order_id] if e[6] is not None})
        print(f"\norder {order_id} {symbols} stored settlementTypeID={sync_types} http={entry['http_status']}")
        print(f"  stored events: {stored}")
        body = entry["body"]
        if entry["http_status"] != 200 or not isinstance(body, dict):
            print(f"  body: {json.dumps(body)[:300]}")
            continue
        asset = body.get("asset") or {}
        status = body.get("status") or {}
        print(
            f"  action={body.get('action')} transaction={body.get('transaction')} status={status.get('id')}/"
            f"{status.get('name')} settlementType={asset.get('settlementType')} leverage={asset.get('leverage')} "
            f"side={asset.get('side')} orderCurrency={body.get('orderCurrency')} totalCosts={body.get('totalCosts')} "
            f"requestTime={body.get('requestTime')}"
        )
        executions = body.get("positionExecutions") or []
        seen: set[int] = set()
        for ex in executions:
            opening = ex.get("openingData") or {}
            seen.add(int(ex.get("positionId")))
            print(
                f"  exec pos={ex.get('positionId')} state={ex.get('state')} remaining={ex.get('remainingUnits')} "
                f"invested={ex.get('investedAmountCurrency')} units={opening.get('units')} "
                f"execTime={opening.get('executionTime')} openTime={opening.get('openTime')} "
                f"avgPrice={opening.get('avgPrice')} avgConv={opening.get('avgConversionRate')} "
                f"fees={opening.get('fees')} taxes={opening.get('taxes')} orderId={opening.get('orderId')}"
            )
        units = sum((Decimal(str((ex.get("openingData") or {}).get("units") or 0)) for ex in executions), Decimal(0))
        print(
            f"  executions={len(executions)} sum_opening_units={units} "
            f"stored_positions_missing_from_executions={sorted(stored_positions - seen)} "
            f"executions_not_stored={sorted(seen - stored_positions)}"
        )


_CHECK_SQL = """
SELECT COALESCE(te.order_id, (te.raw_payload->>'orderID')::bigint), te.position_id, te.source, te.event_kind, te.units,
       te.raw_payload, i.symbol
FROM trade_events te
LEFT JOIN instruments i USING (instrument_id)
"""


def _d(value: Any) -> Decimal:
    return Decimal(str(value))


def checks() -> None:
    """Per order: product sources, every charge indicator separately, unit and money conservation, per-position
    binding of the lookup's ``state``/``remainingUnits`` to stored closes and holdings, copy/parent ids, and the two
    plausibility ratios printed in full. Ends with population totals and denominators."""
    saved = json.loads(_OUT.read_text())
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_CHECK_SQL).fetchall()
        held = {
            r[0]: (_d(r[1]), _d(r[2]), r[3])
            for r in conn.execute(
                "SELECT position_id, units, amount, total_fees FROM broker_positions WHERE position_id > 0"
            )
        }
        archived_fees = dict(
            conn.execute(
                "SELECT DISTINCT ON (position_id) position_id, total_fees FROM broker_positions_closed "
                "WHERE position_id > 0 ORDER BY position_id, closed_detected_at DESC"
            ).fetchall()
        )
        conversion_keys = conn.execute(
            "SELECT count(*) FROM trade_events, jsonb_object_keys(raw_payload) k "
            "WHERE source = 'etoro_history' AND k ILIKE '%conversion%'"
        ).fetchone()
    by_order: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    for row in rows:
        by_order[row[0]].append(row)
    stored_positions = {row[1] for row in rows}
    print(f"broker_positions real-id rows={len(held)}; not in trade_events={sorted(set(held) - stored_positions)}")
    print(f"history payload keys containing 'conversion': {conversion_keys[0] if conversion_keys else None}")
    totals: Counter[str] = Counter()
    worst_acq = Decimal(0)
    worst_proceeds = Decimal(0)
    for order_id, events in sorted(by_order.items()):
        entry = saved[str(order_id)]
        body = entry["body"] if entry["http_status"] == 200 else None
        sync = [e[5] for e in events if e[2] == "etoro_sync"]
        history = [e[5] for e in events if e[2] == "etoro_history"]
        closes = [e for e in events if e[3] == "close"]
        positions = sorted({e[1] for e in events})
        out = [f"{order_id} {events[0][6]:8} lookup={'Y' if body else 'N'} positions={len(positions)}"]
        totals["orders"] += 1
        totals["close_rows"] += len(closes)
        totals["close_fees_zero"] += sum(1 for e in closes if _d(e[5]["fees"]) == 0)
        totals["history_rows"] += len(history)
        totals["history_social_parent_zero"] += sum(
            1 for p in history if int(p["socialTradeId"]) == 0 and int(p["parentPositionId"]) == 0
        )
        totals["sync_rows"] += len(sync)
        totals["sync_mirror_parent_zero"] += sum(
            1 for p in sync if int(p["mirrorID"]) == 0 and int(p["parentPositionID"]) == 0
        )
        for p in sync:
            for key in ("totalExternalFees", "totalExternalTaxes"):
                totals[f"sync {key} observed"] += 1
                totals[f"sync {key} non-zero"] += _d(p[key]) != 0
        for pid in {e[1] for e in events}:
            for label, source in (
                ("held total_fees", {k: v[2] for k, v in held.items()}),
                ("archived total_fees", archived_fees),
            ):
                if pid in source:
                    totals[f"{label} observed"] += 1
                    totals[f"{label} non-zero"] += _d(source[pid]) != 0
        out.append(f"sync_settlement={sorted({p['settlementTypeID'] for p in sync})}")
        charges: dict[str, Decimal] = {}
        if sync:
            charges["sync_totalExternalFees"] = sum((_d(p["totalExternalFees"]) for p in sync), Decimal(0))
            charges["sync_totalExternalTaxes"] = sum((_d(p["totalExternalTaxes"]) for p in sync), Decimal(0))
        for p in positions:
            if p in held:
                charges.setdefault("held_totalFees", Decimal(0))
                charges["held_totalFees"] += _d(held[p][2])
            if p in archived_fees:
                charges.setdefault("archived_totalFees", Decimal(0))
                charges["archived_totalFees"] += _d(archived_fees[p])
        units: Decimal | None = None
        rate = conversion = None
        if body is not None:
            asset = body["asset"]
            out.append(
                f"lookup settlement={asset['settlementType']} currency={asset['currency']} "
                f"orderCurrency={body['orderCurrency']} action={body['action']} instrument_match="
                f"{all(int(asset['instrumentId']) == int(e[5].get('instrumentId', e[5].get('instrumentID'))) for e in events)}"  # noqa: E501
            )
            openings = {json.dumps(e["openingData"], sort_keys=True) for e in body["positionExecutions"]}
            parsed = [json.loads(o) for o in openings]
            out.append(f"distinct_openings={len(parsed)}")
            charges["lookup_totalCosts"] = _d(body["totalCosts"])
            charges["lookup_fees"] = sum((_d(o["fees"]) for o in parsed), Decimal(0))
            charges["lookup_taxes"] = sum((_d(o["taxes"]) for o in parsed), Decimal(0))
            totals["lookup totalCosts observed"] += 1
            totals["lookup totalCosts non-zero"] += _d(body["totalCosts"]) != 0
            for x in body["positionExecutions"]:
                for key in ("fees", "taxes"):
                    totals[f"lookup execution {key} observed"] += 1
                    totals[f"lookup execution {key} non-zero"] += _d(x["openingData"][key]) != 0
            if len(parsed) == 1:
                units = _d(parsed[0]["units"])
                rate, conversion = _d(parsed[0]["avgPrice"]), _d(parsed[0]["avgConversionRate"])
            execution_positions = {int(x["positionId"]) for x in body["positionExecutions"]}
            out.append(f"executions_cover_positions={execution_positions == set(positions)}")
            binding = []
            for x in body["positionExecutions"]:
                pid = int(x["positionId"])
                closed_units = sum((e[4] for e in closes if e[1] == pid), Decimal(0))
                if x["state"] == "closed":
                    binding.append(closed_units == _d(x["remainingUnits"]) and pid not in held)
                else:
                    binding.append(pid in held and held[pid][0] == _d(x["remainingUnits"]))
            out.append(f"position_state_binding={all(binding)}")
        elif sync and len(positions) == 1:
            units = _d(sync[0]["initialUnits"])
            rate, conversion = _d(sync[0]["openRate"]), _d(sync[0]["openConversionRate"])
        out.append("charges=" + ",".join(f"{k}:{v}" for k, v in sorted(charges.items())))
        considerations = {_d(p["initialAmountInDollars"]) for p in sync} | {_d(p["initialInvestment"]) for p in history}
        if units is None:
            out.append("execution units unestablished")
            print(" ".join(out))
            continue
        closed_total = sum((e[4] for e in closes), Decimal(0))
        held_units = sum((held[p][0] for p in positions if p in held), Decimal(0))
        out.append(f"units_conserved={units == closed_total + held_units}")
        if len(considerations) == 1:
            consideration = next(iter(considerations))
            invested = sum((_d(e[5]["investment"]) for e in closes), Decimal(0)) + sum(
                (held[p][1] for p in positions if p in held), Decimal(0)
            )
            out.append(f"money_conserved={invested == consideration}")
            acq = abs(units * rate * conversion - consideration) / consideration  # type: ignore[operator]
            worst_acq = max(worst_acq, acq)
            out.append(f"acq_rel={acq}")
        else:
            out.append(f"considerations={sorted(considerations)}")
        if body is not None and str(body["asset"]["currency"]).upper() == "USD":
            for e in closes:
                proceeds = _d(e[5]["investment"]) + _d(e[5]["netProfit"])
                rel = abs(_d(e[5]["units"]) * _d(e[5]["closeRate"]) - proceeds) / proceeds
                worst_proceeds = max(worst_proceeds, rel)
                out.append(f"close_rel={rel}")
        print(" ".join(out))
    print(f"totals: {dict(totals)}")
    print(
        f"max acquisition plausibility gap={worst_acq} "
        f"max USD proceeds plausibility gap (lookup USD only)={worst_proceeds}"
    )


_FACT_QUERIES: tuple[tuple[str, str], ...] = (
    (
        "trade_events by source/kind",
        "SELECT source, event_kind, count(*), count(order_id) FROM trade_events GROUP BY 1, 2",
    ),
    ("trade_events recorded_at range", "SELECT min(recorded_at), max(recorded_at) FROM trade_events"),
    ("broker_credentials by environment", "SELECT environment, label, count(*) FROM broker_credentials GROUP BY 1, 2"),
    (
        "access log: rows, rows without a credential, environments, first row",
        "SELECT count(*), count(*) FILTER (WHERE l.credential_id IS NULL), array_agg(DISTINCT c.environment), "
        "min(l.accessed_at) FROM broker_credential_access_log l "
        "LEFT JOIN broker_credentials c ON c.id = l.credential_id",
    ),
    ("orders.broker_environment", "SELECT broker_environment, count(*) FROM orders GROUP BY 1"),
    (
        "account equity snapshots",
        "SELECT environment, count(*), count(DISTINCT snapshot_date) FROM broker_account_equity_snapshots GROUP BY 1",
    ),
    (
        "tax tables",
        "SELECT (SELECT count(*) FROM tax_lots), (SELECT count(*) FROM fx_rates), (SELECT count(*) FROM fills)",
    ),
    (
        "private-stream events: CID equal to / different from the sync CID",
        "WITH s AS (SELECT DISTINCT raw_payload->>'CID' AS cid FROM trade_events WHERE source = 'etoro_sync') "
        "SELECT message_type, count(*), count(*) FILTER (WHERE content->>'CID' IN (SELECT cid FROM s)), "
        "count(*) FILTER (WHERE content->>'CID' IS NOT NULL AND content->>'CID' NOT IN (SELECT cid FROM s)) "
        "FROM broker_private_events GROUP BY 1 ORDER BY 1",
    ),
    (
        "private-stream distinct CIDs other than the sync CID",
        "WITH s AS (SELECT DISTINCT raw_payload->>'CID' AS cid FROM trade_events WHERE source = 'etoro_sync') "
        "SELECT count(DISTINCT content->>'CID') FROM broker_private_events "
        "WHERE content->>'CID' NOT IN (SELECT cid FROM s)",
    ),
    (
        "fills by their order's broker reference prefix and environment",
        "SELECT left(o.broker_order_ref, 5), o.broker_environment, count(*) FROM fills f "
        "JOIN orders o USING (order_id) GROUP BY 1, 2",
    ),
    (
        "held real-id positions: rows, amount = initial_amount_in_dollars, leverage 1",
        "SELECT count(*), count(*) FILTER (WHERE amount = initial_amount_in_dollars), count(*) FILTER (WHERE "
        "leverage = 1) FROM broker_positions WHERE position_id > 0",
    ),
    (
        "distinct sync CIDs (count only)",
        "SELECT count(DISTINCT raw_payload->>'CID'), count(*) FILTER (WHERE "
        "raw_payload->>'CID' IS NULL) FROM trade_events WHERE source = 'etoro_sync'",
    ),
)


def facts() -> None:
    """DB facts the spec cites, as counts and dates only (no identifier is printed)."""
    saved = json.loads(_OUT.read_text())
    with psycopg.connect(settings.database_url) as conn:
        for label, sql in _FACT_QUERIES:
            print(f"{label}: {conn.execute(sql).fetchall()}")  # type: ignore[arg-type]  # constant SQL
        sync_cids = {
            r[0]
            for r in conn.execute("SELECT DISTINCT raw_payload->>'CID' FROM trade_events WHERE source = 'etoro_sync'")
        }
        placed = dict(
            conn.execute(
                "SELECT COALESCE(order_id, (raw_payload->>'orderID')::bigint), min(executed_at)::date "
                "FROM trade_events WHERE event_kind = 'open' GROUP BY 1"
            ).fetchall()
        )
    found = [e["body"] for e in saved.values() if e["http_status"] == 200]
    account_ids = [b.get("accountId") for b in found]
    print(
        f"lookup accountId: found={len(found)} missing={account_ids.count(None)} distinct={len(set(account_ids))} "
        f"equal_to_the_one_sync_CID={len(sync_cids) == 1 and {str(a) for a in account_ids} == sync_cids}"
    )
    shape: Counter[str] = Counter()
    for order_id, entry in saved.items():
        if entry["http_status"] != 200:
            continue
        b = entry["body"]
        a = b["asset"]
        shape[
            f"status={b['status'].get('name')} transaction={b['transaction']} action={b['action']} side={a['side']} "
            f"leverage={a['leverage']} settlementType={a['settlementType']} currency={a['currency']} "
            f"orderCurrency={b['orderCurrency']} totalCosts=fees+taxes:"
            f"{_d(b['totalCosts']) == sum((_d(x['openingData']['fees']) + _d(x['openingData']['taxes']) for x in b['positionExecutions'][:1]), Decimal(0))}"  # noqa: E501
        ] += 1
        shape[f"addedFunds present={'addedFunds' in b}"] += 1
        for x in b["positionExecutions"]:
            o = x["openingData"]
            for key in ("markup", "marketSpread"):
                shape[f"{a['settlementType']} execution {key} non-zero={_d(o[key]) != 0}"] += 1
            shape["executions"] += 1
            shape["executions with openingData.orderId = order id"] += int(x["openingData"]["orderId"]) == int(order_id)
    for key, count in sorted(shape.items()):
        print(f"lookup shape: {count} x {key}")
    with psycopg.connect(settings.database_url) as conn:
        currencies = dict(
            conn.execute(
                "SELECT DISTINCT COALESCE(te.order_id, (te.raw_payload->>'orderID')::bigint), i.currency "
                "FROM trade_events te JOIN instruments i USING (instrument_id)"
            ).fetchall()
        )
    pairs = Counter(
        (str(e["body"]["asset"]["currency"]).upper(), currencies.get(int(o)))
        for o, e in saved.items()
        if e["http_status"] == 200
    )
    print(f"lookup asset.currency vs instruments.currency (orders): {dict(pairs)}")
    by_status: dict[int, list[str]] = defaultdict(list)
    for order_id, entry in saved.items():
        by_status[entry["http_status"]].append(str(placed.get(int(order_id))))
    for status, dates in sorted(by_status.items()):
        print(f"lookup HTTP {status}: {len(dates)} orders, open dates {min(dates)} .. {max(dates)}")


if __name__ == "__main__":
    import sys

    if "--checks" in sys.argv:
        checks()
    elif "--facts" in sys.argv:
        facts()
    else:
        main()
