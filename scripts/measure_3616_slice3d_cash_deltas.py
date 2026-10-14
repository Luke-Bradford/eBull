"""#3616 slice 3d-1 fact-finding: does the account's daily credit agree with the trade record?

Read-only, dev DB, one REPEATABLE READ transaction over the ledger scope's environment
(``trade_ledger_scope``). Numbers are parsed from JSON text: decimals as ``Decimal``,
integer tokens accepted as money, booleans and anything else rejected. A missing or
invalid operand makes its flow UNDEFINED; nothing defaults to zero. Prints dates,
amounts, counts and instrument symbols, never an account identifier.

An *interval* is ``(start.observed_at, end.observed_at]`` between consecutive
``broker_account_equity_snapshots`` rows of the environment. *Credit* is ``available_cash +
official_pending_order_amount`` (eToro's "Calculate Available Cash" guide); a row with a
NULL pending amount has unknown credit, and an interval touching one is reported on
``available_cash`` and labelled so. The *trade flow* counts each order's stated initial
investment once, negatively, at the order's earliest open event, and each close row's
``investment + netProfit`` positively. *Charges* are the latest ``found`` lookup's
``totalCosts`` of each order opened in the interval and each close row's ``fees``; the
residual is printed both without and with them, because their cash semantics are an
assumption (spec fact 3). A residual is exact: zero means equal at stored precision.

    uv run python -m scripts.measure_3616_slice3d_cash_deltas [run_id]

``run_id`` defaults to the latest published run of the ledger scope; its policy and
watermarks are printed beside the current ones, since events are read as they are now.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings

_D0 = Decimal(0)


def _money(payload: Any, key: str) -> Decimal | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, Decimal) and value.is_finite():
        return value
    return None


def _opening(payload: dict[str, Any]) -> Decimal | None:
    """The stated initial investment: the field present decides; invalid or conflicting is undefined."""
    present = [k for k in ("initialAmountInDollars", "initialInvestment") if k in payload]
    values = {_money(payload, k) for k in present}
    return values.pop() if len(values) == 1 else None


def _order(payload: dict[str, Any]) -> str | None:
    """The payload's order id, or None when absent (undefined, never merged with other id-less events)."""
    value = payload.get("orderID", payload.get("orderId"))
    return None if value is None else str(value)


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        scope = conn.execute("SELECT environment FROM trade_ledger_scope").fetchone()
        if scope is None:
            print("no ledger scope")
            return
        env = scope[0]
        wanted = int(sys.argv[1]) if len(sys.argv) > 1 else None
        # Explicit or default, the run must be a published run of the ledger scope.
        run_row = conn.execute(
            "SELECT r.run_id, r.evidence_policy_id, r.trade_events_max_event_id, r.lookups_max_id, "
            "r.observations_max_id FROM tax_evidence_runs r JOIN trade_ledger_scope s "
            "ON s.environment = r.environment AND s.account_id = r.account_id "
            "WHERE r.state = 'published' AND (%(run)s::bigint IS NULL OR r.run_id = %(run)s::bigint) "
            "ORDER BY r.run_id DESC LIMIT 1",
            {"run": wanted},
        ).fetchone()
        if run_row is None:
            print("no published run")
            return
        run_id = run_row[0]
        current = conn.execute(
            "SELECT (SELECT max(event_id) FROM trade_events), (SELECT max(lookup_id) FROM tax_order_lookups), "
            "(SELECT max(observation_id) FROM trade_event_observations)"
        ).fetchone()
        snaps = conn.execute(
            "SELECT snapshot_date, observed_at, available_cash, official_pending_order_amount "
            "FROM broker_account_equity_snapshots WHERE environment = %s ORDER BY observed_at",
            (env,),
        ).fetchall()
        raw_events = conn.execute(
            "SELECT e.event_id, e.event_kind, e.executed_at, e.raw_payload::text, i.symbol FROM trade_events e "
            "LEFT JOIN instruments i USING (instrument_id) WHERE e.environment = %s ORDER BY e.executed_at, e.event_id",
            (env,),
        ).fetchall()
        subjects = conn.execute(
            "SELECT outcome, trade_event_ids FROM tax_evidence_subjects WHERE run_id = %s", (run_id,)
        ).fetchall()
        lots = conn.execute(
            "SELECT l.direction, l.trade_event_ids, i.symbol, l.uk_date, l.usd_amount, l.execution_evidence "
            "FROM tax_evidence_lots l JOIN instruments i USING (instrument_id) WHERE l.run_id = %s "
            "ORDER BY l.executed_at, l.lot_key",
            (run_id,),
        ).fetchall()
        classified = conn.execute(
            "SELECT i.symbol, s.execution_evidence, l.execution_evidence, "
            "cardinality(s.total_fees_unobserved_position_ids), s.total_fees_unobserved_position_ids IS NULL, "
            "l.direction, l.uk_date, l.usd_amount FROM tax_evidence_subjects s JOIN tax_evidence_lots l "
            "ON l.run_id = s.run_id AND l.subject = s.subject "
            "LEFT JOIN instruments i ON i.instrument_id = s.instrument_id "
            "WHERE s.run_id = %s AND s.outcome = 'admitted' ORDER BY l.executed_at, l.lot_key",
            (run_id,),
        ).fetchall()
        bodies = conn.execute(
            "SELECT DISTINCT ON (order_id) order_id, body_text FROM tax_order_lookups "
            "WHERE environment = %s AND outcome = 'found' AND lookup_id <= %s ORDER BY order_id, lookup_id DESC",
            (env, run_row[3]),
        ).fetchall()

    print(f"environment={env} run={run_id} policy={run_row[1]}")
    print(f"run watermarks events/lookups/observations={run_row[2]}/{run_row[3]}/{run_row[4]}")
    print(f"current maxima events/lookups/observations={current[0]}/{current[1]}/{current[2]}" if current else "")
    events = [
        (eid, kind, at, json.loads(text, parse_float=Decimal), symbol)
        for eid, kind, at, text, symbol in raw_events
        if eid <= run_row[2]
    ]
    outcome_of = {eid: outcome for outcome, ids in subjects for eid in ids}
    total_costs = {str(o): _money(json.loads(b, parse_float=Decimal), "totalCosts") for o, b in bodies}
    _snapshot_facts(snaps)
    _close_facts(events)
    windows = _intervals(snaps, events, outcome_of, total_costs)
    _lots(lots, windows, {e[0]: e[2] for e in events})
    _classes(classified)


def _classes(rows: list[Any]) -> None:
    """Publication classes of the spec, from the run's stored subject and lot fields."""
    print("\n[publication classes]")
    totals: Counter[str] = Counter()
    for symbol, evidence, lot_evidence, unobserved, unobserved_null, direction, uk_date, usd in rows:
        if (
            evidence not in ("lookup", "sync_only")
            or lot_evidence != evidence
            or unobserved_null
            or direction not in ("acquisition", "disposal")
        ):
            cls, reasons = "unavailable", []
        else:
            reasons = ["spread_inclusion_assumed"] if direction == "acquisition" else ["disposal_formula_assumed"]
            if direction == "acquisition" and evidence == "sync_only":
                reasons += ["execution_assumed", "lookup_evidence_unobserved"]
            if unobserved:
                reasons.append("sync_charges_unobserved")
            cls = "provisional" if reasons else "final_eligible"
        totals[f"{direction}:{cls}"] += 1
        print(f"{direction},{symbol},{uk_date},{usd},{evidence},unobserved_positions={unobserved},{cls},{reasons}")
    print(f"lots={len(rows)} classes={dict(sorted(totals.items()))}")


def _credit(row: Any) -> Decimal | None:
    return None if row[3] is None else row[2] + row[3]


def _snapshot_facts(snaps: list[Any]) -> None:
    print("\n[snapshots]")
    known = [s for s in snaps if s[3] is not None]
    print(f"rows={len(snaps)} first_observed_at={snaps[0][1] if snaps else None}")
    print(f"pending_known={len(known)} pending_nonzero={sum(1 for s in known if s[3] != 0)}")
    print(f"first_pending_known_observed_at={known[0][1] if known else None}")
    gaps = [(str(a[0]), str(b[0])) for a, b in zip(snaps, snaps[1:], strict=False) if (b[0] - a[0]).days > 1]
    print(f"gaps_spanning_missing_dates={gaps} distinct_snapshot_dates={len({s[0] for s in snaps})}")


def _close_facts(events: list[Any]) -> None:
    closes = [(p, sym) for _, kind, _, p, sym in events if kind == "close"]
    keys = sorted({k for p, _ in closes for k in p})
    print(f"\n[events] total={len(events)} opens={len(events) - len(closes)} closes={len(closes)}")
    print(f"close_rows_by_symbol={dict(sorted(Counter(sym for _, sym in closes).items()))}")
    print(f"close_payload_keys={keys}")
    opens = {(p.get("positionID", p.get("positionId")), _order(p)) for _, kind, _, p, _ in events if kind == "open"}
    bound = sum(1 for p, _ in closes if (p.get("positionId"), _order(p)) in opens)
    print(f"closes_whose_positionId_and_orderId_match_an_open_event={bound} of {len(closes)}")


def _intervals(snaps: list[Any], events: list[Any], outcome_of: dict[int, str], total_costs: dict) -> list[Any]:
    first_open: dict[str, Any] = {}
    open_amounts: dict[str, set[Decimal | None]] = {}
    unbound_opens: list[Any] = []  # opens with no order id: their interval's flow is undefined
    for _, kind, at, payload, _ in events:
        if kind == "open":
            order = _order(payload)
            if order is None:
                unbound_opens.append(at)
                continue
            first_open.setdefault(order, at)
            amount = _opening(payload)
            open_amounts.setdefault(order, set()).add(amount)
    print("\n[intervals holding events]")
    print(
        "start,end,credit_basis,d_cash,raw_events,orders_opened,orders_participating,close_rows,symbols,outcomes,"
        "flow,charges(order:amount),residual_without_charges,residual_with_charges"
    )
    windows = []
    zero_with = zero_without = 0
    eventless: Counter[str] = Counter()
    inside_total = 0
    for start, end in zip(snaps, snaps[1:], strict=False):
        inside = [e for e in events if start[1] < e[2] <= end[1]]
        c0, c1 = _credit(start), _credit(end)
        basis = "credit" if c0 is not None and c1 is not None else "available_cash"
        d_cash = (c1 - c0) if c0 is not None and c1 is not None else end[2] - start[2]
        if not inside:
            if basis == "available_cash":
                eventless["credit_unknown"] += 1
            else:
                eventless["credit_unchanged" if d_cash == 0 else "credit_moved"] += 1
            continue
        inside_total += len(inside)
        orders = sorted({o for o, at in first_open.items() if start[1] < at <= end[1]})
        flow: Decimal | None = None if any(start[1] < at <= end[1] for at in unbound_opens) else _D0
        charges: Decimal | None = _D0
        charge_parts = []
        for o in orders:
            amounts = open_amounts[o]
            (stated,) = amounts if len(amounts) == 1 else (None,)
            flow = None if flow is None or stated is None else flow - stated
            # No found lookup, or no valid totalCosts in it: the charge is unknown.
            cost = total_costs.get(o)
            if cost is None:
                charges = None
                charge_parts.append(f"{o}:unknown")
            else:
                if cost != 0:
                    charge_parts.append(f"{o}:{cost}")
                if charges is not None:
                    charges += cost
        for _, kind, _, payload, _ in inside:
            if kind != "close":
                continue
            inv, pnl, fee = _money(payload, "investment"), _money(payload, "netProfit"), _money(payload, "fees")
            flow = None if flow is None or inv is None or pnl is None else flow + inv + pnl
            charges = None if charges is None or fee is None else charges + fee
        without = None if flow is None else d_cash - flow
        with_ = None if flow is None or charges is None else d_cash - flow + charges
        zero_without += without == 0
        zero_with += with_ == 0
        windows.append((start[1], end[1], with_))
        symbols = dict(sorted(Counter(e[4] for e in inside).items()))
        outcomes = dict(sorted(Counter(outcome_of.get(e[0], "none") for e in inside).items()))
        participating = {_order(e[3]) for e in inside}
        print(
            f"{start[0]},{end[0]},{basis},{d_cash},{len(inside)},{len(orders)},{len(participating)},"
            f"{sum(1 for e in inside if e[1] == 'close')},{symbols},{outcomes},{flow},"
            f"{charge_parts},{without},{with_}"
        )
        if len(participating) == 1:
            parts = []
            for e in inside:
                part = f"{e[1]}:initial={_opening(e[3])}"
                if e[1] == "close":
                    part += f",investment={_money(e[3], 'investment')},netProfit={_money(e[3], 'netProfit')}"
                parts.append(part)
            print(f"    single_order {symbols} components={parts}")
    print(f"intervals={max(len(snaps) - 1, 0)} with_events={len(windows)} events_inside={inside_total}")
    print(f"residual_zero_without_charges={zero_without} residual_zero_with_charges={zero_with}")
    print(f"eventless_intervals={dict(sorted(eventless.items()))}")
    covered = {e[0] for e in events for a, b in zip(snaps, snaps[1:], strict=False) if a[1] < e[2] <= b[1]}
    outside = [(e[1], str(e[2]), e[4]) for e in events if e[0] not in covered]
    print(f"events_outside_intervals={len(outside)} first_snapshot_at={snaps[0][1] if snaps else None}")
    for item in outside:
        print(f"    outside {item}")
    return windows


def _lots(lots: list[Any], windows: list[Any], event_at: dict[int, Any]) -> None:
    print("\n[lots of the run, placed by their earliest ledger event]")
    counts: Counter[str] = Counter()
    for direction, ids, symbol, uk_date, usd, evidence in lots:
        at = min(event_at[i] for i in ids)
        hit = [w for w in windows if w[0] < at <= w[1]]
        status = "outside" if not hit else ("zero" if hit[0][2] == 0 else "residual")
        counts[f"{direction}:{status}"] += 1
        print(f"{direction},{symbol},{uk_date},{usd},{evidence},{status},{hit[0][2] if hit else None}")
    print(f"lots={len(lots)} by_direction_status={dict(sorted(counts.items()))}")


if __name__ == "__main__":
    main()
