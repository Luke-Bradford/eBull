"""#3616 slice 3d-3b fact-finding: what the stored record shows of distributions and position income.

Read-only, dev DB, one REPEATABLE READ transaction over the ledger scope's environment
(``trade_ledger_scope``). Prints dates, amounts, counts, symbols and position ids, never an
account identifier. Payload keys are matched case-insensitively, as 3c's derivation matches them.

0. ``[held_real]``: held ``real`` positions (``settlementTypeID`` 1), each with whether the ledger scope's
   ``trade_events`` hold it.
1. ``[distribution]``: one declared distribution, read from the issuer's SEC filing and passed
   in below (``DISTRIBUTION``). Every position of its instrument opened on or before the record
   date: direction, leverage, product, units at open and now, its close events, and every
   retained sync ``totalFees`` (stored row, then each observation of the position).
2. ``[cash]``: ``available_cash`` across the snapshots up to the payment date, and every interval
   between consecutive snapshots with known credit (``available_cash +
   official_pending_order_amount``): how many hold no trade event, and of those how many changed.
3. ``[total_fees]``: every retained sync open payload of the scope (stored rows of the environment,
   then observations attached to them; unscoped observations counted), by product
   (``settlementTypeID``): payloads, with ``totalFees`` zero, non-zero, absent, or not a number.
4. ``[open_reason]``: ``openPositionActionType`` and ``orderType`` over the same payloads.
5. ``[runs]``: evidence and matching runs with their policies.
6. ``[observations]``: sync open observations by kind and by the keys that differ from the stored
   row (union of both documents' keys), and ``trade_history_fetches`` by outcome.

    uv run python -m scripts.measure_3616_slice3d3b_dividends
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings

#: Icahn Enterprises L.P., 8-K 0001104659-26-090743, Exhibit 99.1 (read 2026-10-14): "$0.50 per depositary
#: unit, which will be paid on or about September 23, 2026 ... to depositary unitholders of record at the close
#: of business on August 17, 2026"; no timely election is "deemed to have elected to receive the distribution
#: in additional depositary units".
DISTRIBUTION = {"symbol": "IEP", "per_unit": Decimal("0.50"), "record": date(2026, 8, 17), "paid": date(2026, 9, 23)}

_ABSENT = object()
_CONFLICT = object()


def _get(payload: Any, key: str) -> Any:
    """The value of ``key`` matched case-insensitively. Two spellings conflict when their values differ; numbers are
    compared as numbers (``0`` and ``0.0`` agree), and a boolean never equals a number."""
    if not isinstance(payload, dict):
        return _ABSENT
    values = [v for k, v in payload.items() if k.lower() == key.lower()]
    if not values:
        return _ABSENT

    def norm(v: Any) -> Any:
        if isinstance(v, bool):
            return ("bool", v)
        if isinstance(v, int | Decimal):
            return ("number", Decimal(v))
        return (type(v).__name__, v)

    first = norm(values[0])
    return values[0] if all(norm(v) == first for v in values) else _CONFLICT


def _label(value: Any) -> str:
    return "absent" if value is _ABSENT else "conflict" if value is _CONFLICT else repr(value)


def _fee_class(value: Any) -> str:
    if value is _ABSENT:
        return "absent"
    if value is _CONFLICT:
        return "conflict"
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        return "not_a_number"
    return "zero" if value == 0 else "non_zero"


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET TIME ZONE 'UTC'")
        scope = conn.execute("SELECT environment, account_id FROM trade_ledger_scope").fetchone()
        if scope is None:
            print("no ledger scope")
            return
        env, account = scope[0], scope[1]
        print(f"environment {env}")

        def cid_matches(payload: Any) -> bool:
            """The portfolio row's ``CID`` equals the scope's account (printed as a boolean, never the id)."""
            cid = _get(payload, "CID")
            return cid is not _ABSENT and cid is not _CONFLICT and str(cid) == str(account)

        def loads(text: str | None) -> Any:
            return None if text is None else json.loads(text, parse_float=Decimal)

        # Every retained sync open payload of the scope: (position_id, origin, payload).
        payloads: list[tuple[int, str, Any]] = [
            (r[0], "stored", loads(r[1]))
            for r in conn.execute(
                "SELECT position_id, raw_payload::text FROM trade_events WHERE environment = %s "
                "AND source = 'etoro_sync' AND event_kind = 'open'",
                (env,),
            ).fetchall()
        ]
        payloads += [
            (r[0], f"observation:{r[1]}", loads(r[2]))
            for r in conn.execute(
                "SELECT o.position_id, o.kind, o.observed_raw::text FROM trade_event_observations o "
                "JOIN trade_events e ON e.event_id = o.event_id AND e.environment = %s "
                "WHERE o.source = 'etoro_sync' AND o.event_kind = 'open' ORDER BY o.observation_id",
                (env,),
            ).fetchall()
        ]
        # Unscoped observations (event_id NULL) attach at derivation by position or order id (3c Design 2); their
        # count bounds what this enumeration could miss.
        unscoped = conn.execute(
            "SELECT count(*) FROM trade_event_observations WHERE event_id IS NULL AND source = 'etoro_sync'"
        ).fetchone()
        in_scope = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT position_id FROM trade_events WHERE environment = %s", (env,)
            ).fetchall()
        }

        # 3c Design 2: every committed sync writes one trade_history_fetches row in the transaction that writes
        # broker_positions, so the latest row identifies the refresh the portfolio tables hold.
        latest = conn.execute(
            "SELECT environment, account_id, outcome FROM trade_history_fetches ORDER BY fetch_id DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            print("portfolio refresh provenance: no fetch row (unverified)")
        else:
            print(
                f"portfolio refresh provenance: environment_is_scope {latest[0] == env} "
                f"account_is_scope {str(latest[1]) == str(account)} outcome {latest[2]}"
            )

        print("\n[held_real]")
        for row in conn.execute(
            "SELECT i.symbol, p.position_id, p.open_date_time, p.raw_payload::text FROM broker_positions p "
            "JOIN instruments i USING (instrument_id) WHERE p.settlement_type_id = 1 ORDER BY 1, 3"
        ).fetchall():
            print(
                f"{row[0]} position {row[1]} opened {row[2]:%Y-%m-%d} in_ledger_scope_events {row[1] in in_scope} "
                f"row_cid_is_scope_account {cid_matches(loads(row[3]))}"
            )

        print("\n[distribution]")
        d = DISTRIBUTION
        print(f"{d['symbol']} {d['per_unit']}/unit record {d['record']} paid on or about {d['paid']}")
        held = conn.execute(
            """
            SELECT p.position_id, p.is_buy, p.leverage, p.settlement_type_id, p.initial_units, p.units,
                   p.open_date_time, p.raw_payload::text, 'held'
              FROM broker_positions p JOIN instruments i USING (instrument_id)
             WHERE i.symbol = %(s)s AND p.open_date_time::date <= %(r)s
            UNION ALL
            SELECT c.position_id, c.is_buy, c.leverage, c.settlement_type_id, c.initial_units, c.units,
                   c.open_date_time, c.raw_payload::text, 'closed'
              FROM broker_positions_closed c JOIN instruments i USING (instrument_id)
             WHERE i.symbol = %(s)s AND c.open_date_time::date <= %(r)s
             ORDER BY 1
            """,
            {"s": d["symbol"], "r": d["record"]},
        ).fetchall()
        due_units = Decimal(0)
        for pid, is_buy, leverage, settlement, initial, units, opened, raw, state in held:
            closes = conn.execute(
                "SELECT count(*) FROM trade_events WHERE position_id = %s AND event_kind = 'close' "
                "AND environment = %s",
                (pid, env),
            ).fetchone()
            fees = [(origin, _label(_get(p, "totalFees"))) for p_id, origin, p in payloads if p_id == pid]
            n_closes = closes[0] if closes else None
            if n_closes == 0:
                due_units += initial
            print(
                f"position {pid} portfolio_state {state} in_ledger_scope_events {pid in in_scope} "
                f"row_cid_is_scope_account "
                f"{cid_matches(loads(raw))} is_buy {is_buy} leverage {leverage} "
                f"settlementTypeID {settlement} opened {opened:%Y-%m-%d} initial_units {initial} units {units} "
                f"close_events {n_closes} totalFees {fees}"
            )
        unit = d["per_unit"]
        print(f"sum of initial units of positions with no recorded scoped close event {due_units}")
        print(f"at {unit}/unit {due_units * unit}")

        print("\n[cash]")
        snaps = conn.execute(
            "SELECT observed_at, available_cash, official_pending_order_amount FROM broker_account_equity_snapshots "
            "WHERE environment = %s ORDER BY observed_at",
            (env,),
        ).fetchall()
        if not snaps:
            print("no snapshots")
        upto = [s for s in snaps if s[0].date() <= d["paid"]]
        first_last = f"{snaps[0][0]:%Y-%m-%d} to {snaps[-1][0]:%Y-%m-%d}" if snaps else "none"
        print(
            f"snapshots {len(snaps)} from {first_last}; up to {d['paid']}: "
            f"{len(upto)}, distinct available_cash {sorted({s[1] for s in upto})}"
        )
        prev = None
        for t, cash, _ in upto:
            if prev is not None and cash != prev[1]:
                print(f"available_cash change {prev[0]:%Y-%m-%dT%H:%MZ} -> {t:%Y-%m-%dT%H:%MZ}: {prev[1]} -> {cash}")
            prev = (t, cash)
        first_known = next((s[0] for s in snaps if s[2] is not None), None)
        print(
            f"first snapshot with known credit {first_known:%Y-%m-%dT%H:%MZ}" if first_known else "credit never known"
        )
        known = eventless = eventless_changed = 0
        for (t0, c0, p0), (t1, c1, p1) in zip(snaps, snaps[1:], strict=False):
            if p0 is None or p1 is None:
                continue
            known += 1
            change = (c1 + p1) - (c0 + p0)
            events = conn.execute(
                "SELECT count(*) FROM trade_events WHERE environment = %s AND executed_at > %s AND executed_at <= %s",
                (env, t0, t1),
            ).fetchone()
            n_events = events[0] if events else None
            if n_events == 0:
                eventless += 1
                eventless_changed += change != 0
            if t0.date() <= d["paid"] <= t1.date():
                print(f"interval {t0:%Y-%m-%dT%H:%MZ} -> {t1:%Y-%m-%dT%H:%MZ}: change {change}, events {n_events}")
        print(f"known-credit intervals {known}; without events {eventless}; of those changed {eventless_changed}")

        print("\n[total_fees]")
        by_product: Counter[tuple[str, str]] = Counter()
        for _, _, p in payloads:
            by_product[(_label(_get(p, "settlementTypeID")), _fee_class(_get(p, "totalFees")))] += 1
        n_unscoped = unscoped[0] if unscoped else None
        print(f"retained sync open payloads {len(payloads)}; unscoped sync observations, attached or not {n_unscoped}")
        for (product, cls), n in sorted(by_product.items()):
            print(f"settlementTypeID {product} totalFees {cls}: {n}")

        history = [
            loads(r[0])
            for r in conn.execute(
                "SELECT raw_payload::text FROM trade_events WHERE environment = %s AND source = 'etoro_history'",
                (env,),
            ).fetchall()
        ]
        for key in ("totalFees", "openPositionActionType", "orderType"):
            present = sum(_get(p, key) is not _ABSENT for p in history)
            print(f"history payloads {len(history)}; carrying {key}: {present}")

        print("\n[open_reason]")
        reasons: Counter[tuple[str, str, str]] = Counter()
        for _, origin, p in payloads:
            reasons[
                (origin.split(":")[0], _label(_get(p, "openPositionActionType")), _label(_get(p, "orderType")))
            ] += 1
        for (origin, action, order_type), n in sorted(reasons.items()):
            print(f"{origin} openPositionActionType {action} orderType {order_type}: {n}")

        print("\n[runs]")
        for row in conn.execute(
            "SELECT run_id, evidence_policy_id, state FROM tax_evidence_runs WHERE environment = %s ORDER BY run_id",
            (env,),
        ).fetchall():
            print(f"evidence run {row[0]} {row[1]} {row[2]}")
        for row in conn.execute(
            "SELECT m.matching_run_id, m.evidence_run_id, m.class_policy_id, m.character_policy_id, m.state "
            "FROM tax_matching_runs m JOIN tax_evidence_runs r ON r.run_id = m.evidence_run_id "
            "WHERE r.environment = %s ORDER BY m.matching_run_id",
            (env,),
        ).fetchall():
            print(f"matching run {row[0]} evidence {row[1]} {row[2]} {row[3]} {row[4]}")

        print("\n[observations]")
        for row in conn.execute(
            """
            -- Per observation row o: the keys of either document whose values differ (correlated on o by design).
            SELECT o.kind, (SELECT array_agg(k ORDER BY k)
                              FROM (SELECT jsonb_object_keys(o.observed_normalised)
                                    UNION SELECT jsonb_object_keys(o.stored_normalised)) AS keys(k)
                             WHERE o.observed_normalised -> k IS DISTINCT FROM o.stored_normalised -> k) AS differs,
                   count(*)
              FROM trade_event_observations o
             WHERE o.source = 'etoro_sync' AND o.event_kind = 'open'
               AND EXISTS (SELECT 1 FROM trade_events e WHERE e.event_id = o.event_id AND e.environment = %s)
             GROUP BY 1, 2 ORDER BY 1, 2
            """,
            (env,),
        ).fetchall():
            print(f"sync open {row[0]} differing from the stored row in {row[1]}: {row[2]}")
        for row in conn.execute(
            "SELECT outcome, count(*), min(portfolio_fetched_at), max(portfolio_fetched_at) FROM trade_history_fetches "
            "WHERE environment = %s GROUP BY 1 ORDER BY 1",
            (env,),
        ).fetchall():
            print(f"fetches {row[0]}: {row[1]} from {row[2]:%Y-%m-%dT%H:%MZ} to {row[3]:%Y-%m-%dT%H:%MZ}")


if __name__ == "__main__":
    main()
