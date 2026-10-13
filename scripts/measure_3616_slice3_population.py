"""#3616 slice 3: the account's broker trade record, measured as tax events (spec
``docs/specs/metrics/2026-10-13-3616-account-disposals-tax-ledger.md``, "Facts established first").

Measurement only, read-only. It groups ``trade_events`` by the broker's ``orderId`` (one acquisition per order),
resolves each order's settlement type from every stored observation of its positions, and checks the order's units
and USD amounts for completeness. With ``--boe-csv`` it also prints the publication-gap distribution of a Bank of
England XUDLUSS download.

    PYTHONPATH=. uv run python scripts/measure_3616_slice3_population.py [--boe-csv <XUDLUSS.csv>] --out <file.json>

The BoE file comes from the public database, no account (dates DD/Mon/YYYY):

    curl -s -A "Mozilla/5.0" -o XUDLUSS.csv "https://www.bankofengland.co.uk/boeapps/database/\
_iadb-fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/1975&Dateto=12/Oct/2026&SeriesCodes=XUDLUSS&CSVF=TN\
&UsingCodes=Y&VPD=Y&VFD=N"
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows

from app.config import settings

_EVENTS_SQL = """
SELECT t.event_id, t.position_id, t.instrument_id, i.symbol, t.event_kind, t.source, t.units, t.executed_at,
       t.raw_payload
FROM trade_events t
JOIN instruments i USING (instrument_id)
ORDER BY t.executed_at, t.event_id
"""

_SETTLEMENT_SQL = """
SELECT position_id, settlement_type_id, units, initial_units, 'broker_positions' AS src FROM broker_positions
UNION ALL
SELECT position_id, settlement_type_id, units, initial_units, 'broker_positions_closed' FROM broker_positions_closed
"""


def _order_id(payload: dict[str, Any]) -> int | None:
    value = payload.get("orderID", payload.get("orderId"))
    return None if value is None else int(value)


def _dec(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def measure(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        events = cur.execute(_EVENTS_SQL).fetchall()
        broker_rows = cur.execute(_SETTLEMENT_SQL).fetchall()

    settlement: dict[int, set[int]] = defaultdict(set)
    held_units: dict[int, Decimal] = {}
    for row in broker_rows:
        if row["settlement_type_id"] is not None:
            settlement[row["position_id"]].add(int(row["settlement_type_id"]))
        if row["src"] == "broker_positions":
            held_units[row["position_id"]] = Decimal(str(row["units"]))

    by_order: dict[int | None, list[dict[str, Any]]] = defaultdict(list)
    for ev in events:
        by_order[_order_id(ev["raw_payload"])].append(ev)

    orders: list[dict[str, Any]] = []
    for order_id, evs in by_order.items():
        positions = sorted({e["position_id"] for e in evs})
        types: set[int] = set()
        for e in evs:
            raw = e["raw_payload"]
            if raw.get("settlementTypeID") is not None:
                types.add(int(raw["settlementTypeID"]))
        for p in positions:
            types |= settlement.get(p, set())
        sync_opens = [e for e in evs if e["event_kind"] == "open" and e["source"] == "etoro_sync"]
        closes = [e for e in evs if e["event_kind"] == "close"]
        initial_units = {_dec(e["raw_payload"].get("initialUnits")) for e in sync_opens} - {None}
        initial_usd = {_dec(e["raw_payload"].get("initialAmountInDollars")) for e in sync_opens} | {
            _dec(e["raw_payload"].get("initialInvestment")) for e in closes
        }
        initial_usd -= {None}
        disposed = sum((Decimal(str(e["units"])) for e in closes), Decimal(0))
        held = sum((held_units.get(p, Decimal(0)) for p in positions), Decimal(0))
        piece_usd = sum((_dec(e["raw_payload"].get("investment")) or Decimal(0) for e in closes), Decimal(0))
        if len(initial_units) == 1:
            acquired = next(iter(initial_units))
            units_rule = "sync_initialUnits"
        else:
            acquired = disposed + held
            units_rule = "sum_of_pieces" if not initial_units else "conflicting_initialUnits"
        orders.append(
            {
                "order_id": order_id,
                "symbol": evs[0]["symbol"],
                "instrument_id": evs[0]["instrument_id"],
                "positions": positions,
                "opened_at": min(e["executed_at"] for e in evs).isoformat(),
                "settlement_types": sorted(types),
                "character": (
                    "unestablished"
                    if not types
                    else "real"
                    if types == {1}
                    else "conflict"
                    if len(types) > 1
                    else "excluded_non_real"
                ),
                "is_buy": sorted({bool(e["raw_payload"].get("isBuy")) for e in evs}),
                "leverage": sorted({int(e["raw_payload"].get("leverage", 0)) for e in evs}),
                "units_rule": units_rule,
                "acquired_units": str(acquired),
                "disposed_units": str(disposed),
                "held_units": str(held),
                "units_reconcile": acquired == disposed + held,
                "initial_usd": sorted(str(v) for v in initial_usd),
                "pieces_plus_held_usd_closed_only": str(piece_usd) if held == 0 else None,
                "close_pieces": len(closes),
                "nonzero_close_fees": sum(1 for e in closes if (_dec(e["raw_payload"].get("fees")) or 0) != 0),
            }
        )

    pieces = [e for e in events if e["event_kind"] == "close"]
    piece_character = Counter()
    order_character = {o["order_id"]: o["character"] for o in orders}
    for e in pieces:
        piece_character[order_character[_order_id(e["raw_payload"])]] += 1
    return {
        "events_by_source_kind": {
            f"{s}/{k}": n for (s, k), n in sorted(Counter((e["source"], e["event_kind"]) for e in events).items())
        },
        "events_without_order_id": sum(1 for e in events if _order_id(e["raw_payload"]) is None),
        "orders": len(orders),
        "orders_by_character": dict(sorted(Counter(o["character"] for o in orders).items())),
        "close_pieces_by_character": dict(sorted(piece_character.items())),
        "orders_units_not_reconciled": [o["order_id"] for o in orders if not o["units_reconcile"]],
        "orders_with_multiple_positions": [o["order_id"] for o in orders if len(o["positions"]) > 1],
        "detail": sorted(orders, key=lambda o: o["opened_at"]),
    }


def boe_gaps(path: Path) -> dict[str, Any]:
    with path.open(newline="") as fh:
        rows = list(csv.reader(fh))[1:]
    days = [datetime.strptime(r[0], "%d %b %Y").date() for r in rows]
    gaps = Counter((b - a).days for a, b in zip(days, days[1:], strict=False))
    widest = max(gaps)
    return {
        "publications": len(days),
        "first": days[0].isoformat(),
        "last": days[-1].isoformat(),
        "gap_days_histogram": {str(k): gaps[k] for k in sorted(gaps)},
        "widest_gap_days": widest,
        "latest_widest_gaps": [
            [a.isoformat(), b.isoformat()] for a, b in zip(days, days[1:], strict=False) if (b - a).days == widest
        ][-3:],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--boe-csv", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        result: dict[str, Any] = {"measured_on": date.today().isoformat(), "population": measure(conn)}
    if args.boe_csv is not None:
        result["boe_xudluss"] = boe_gaps(args.boe_csv)
    args.out.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(json.dumps({k: v for k, v in result["population"].items() if k != "detail"}, indent=2, default=str))
    if "boe_xudluss" in result:
        print(json.dumps(result["boe_xudluss"], indent=2))


if __name__ == "__main__":
    main()
