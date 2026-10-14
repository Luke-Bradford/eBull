"""#3616 slice 3d-2 fact-finding: the population statutory matching would act on.

Read-only, dev DB, one REPEATABLE READ transaction. Reads one published run of the ledger
scope (``trade_ledger_scope``): its subjects and lots, grouped by the tax security of spec
Design 2 (``tax_security_v1``): an instrument's ``canonical_instrument_id`` when its symbol is
the canonical row's symbol plus ``.RTH`` (settled decision #819), decided per connected
component of canonical pointers and ``X``/``X.RTH`` symbol pairs: established when the component
has exactly one row with no canonical pointer and every other row points to it with its symbol
plus ``.RTH``; *unestablished* otherwise; an id with no row is *unplaceable*. Each component is
printed with its raw rows. Prints per security its admitted lots by London date, its unresolved
and excluded subjects, the history fetch the run read, and the legacy tax tables' row counts.
Sums run in a precision-60 context with ``Inexact`` and ``Rounded`` trapped, so a printed total
is exact or the script raises. Prints dates, amounts, counts and symbols, never an account
identifier.

    uv run python -m scripts.measure_3616_slice3d2_population [run_id]

``run_id`` defaults to the latest published run of the ledger scope.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from decimal import Decimal, Inexact, Rounded, localcontext
from typing import Any

import psycopg

from app.config import settings


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        # Set before the first statement, so the whole read is one snapshot (review-prevention-log, isolation level).
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        wanted = int(sys.argv[1]) if len(sys.argv) > 1 else None
        run = conn.execute(
            "SELECT r.run_id, r.evidence_policy_id, r.history_fetch_ok_id, r.history_fetch_latest_id, "
            "r.environment, r.account_id "
            "FROM tax_evidence_runs r JOIN trade_ledger_scope s "
            "ON s.environment = r.environment AND s.account_id = r.account_id "
            "WHERE r.state = 'published' AND (%(run)s::bigint IS NULL OR r.run_id = %(run)s::bigint) "
            "ORDER BY r.run_id DESC LIMIT 1",
            {"run": wanted},
        ).fetchone()
        if run is None:
            print("no published run")
            return
        run_id, policy, ok_id, latest_id, env, account = run
        print(f"[run] run_id={run_id} evidence_policy_id={policy} history_fetch_ok_id={ok_id} latest_id={latest_id}")
        for fid in sorted({x for x in (ok_id, latest_id) if x is not None}):
            f = conn.execute(
                "SELECT outcome, min_date, started_at, finished_at, row_count FROM trade_history_fetches "
                "WHERE fetch_id = %s",
                (fid,),
            ).fetchone()
            print(f"[fetch {fid}] {f}")
        first_ok = conn.execute(
            "SELECT min(fetch_id), min(started_at), count(*) FROM trade_history_fetches "
            "WHERE outcome = 'ok' AND environment = %s AND account_id = %s AND fetch_id <= %s",
            (env, account, ok_id),
        ).fetchone()
        print(f"[ok fetches of the scope up to the run's] first_id, first_started_at, count = {first_ok}")

        instruments: dict[int, tuple[str, int | None]] = {
            r[0]: (r[1], r[2])
            for r in conn.execute("SELECT instrument_id, symbol, canonical_instrument_id FROM instruments").fetchall()
        }

        by_symbol: dict[str, list[int]] = defaultdict(list)
        for iid, (symbol, _) in instruments.items():
            by_symbol[symbol.upper()].append(iid)

        pointed_from: dict[int, list[int]] = defaultdict(list)
        for iid, (_, canonical) in instruments.items():
            if canonical is not None:
                pointed_from[canonical].append(iid)

        def neighbours(iid: int) -> set[int]:
            """Design 2 edges: the canonical pointer either way, and symbol X <-> X.RTH either way."""
            symbol, canonical = instruments[iid]
            sym = symbol.upper()
            out = set(pointed_from.get(iid, []))
            if canonical is not None and canonical in instruments:
                out.add(canonical)
            out.update(by_symbol.get(f"{sym}.RTH", []))
            if sym.endswith(".RTH"):
                out.update(by_symbol.get(sym[: -len(".RTH")], []))
            out.discard(iid)
            return out

        def component(iid: int) -> frozenset[int]:
            seen, todo = {iid}, [iid]
            while todo:
                for v in neighbours(todo.pop()):
                    if v not in seen:
                        seen.add(v)
                        todo.append(v)
            return frozenset(seen)

        def security(iid: int | None) -> str:
            """Spec Design 2. An instrument with no row is unplaceable: no security is safe."""
            if iid is None or iid not in instruments:
                return "unplaceable (every security withheld)"
            members = component(iid)
            bases = [m for m in members if instruments[m][1] is None]
            if len(bases) == 1:
                base = bases[0]
                base_sym = instruments[base][0].upper()
                if all(
                    m == base or (instruments[m][1] == base and instruments[m][0].upper() == f"{base_sym}.RTH")
                    for m in members
                ):
                    return f"{base}:{instruments[base][0]}"
            return f"unestablished({iid}:{instruments[iid][0]} in component {sorted(members)})"

        subjects = conn.execute(
            "SELECT subject, outcome, product, instrument_id, reasons, execution_evidence, "
            "total_fees_unobserved_position_ids FROM tax_evidence_subjects WHERE run_id = %s "
            "ORDER BY subject",
            (run_id,),
        ).fetchall()
        lots = conn.execute(
            "SELECT instrument_id, direction, uk_date, tax_year, units, usd_amount, fx_status, amount_gbp, lot_key, "
            "execution_evidence, subject "
            "FROM tax_evidence_lots WHERE run_id = %s ORDER BY instrument_id, executed_at, lot_key",
            (run_id,),
        ).fetchall()

        by_sec: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
        mapped = 0
        for s in subjects:
            sec = security(s[3])
            by_sec[sec][s[1]].append(s)
            if s[3] in instruments and instruments[s[3]][1] is not None:
                mapped += 1
        for lot in lots:
            by_sec[security(lot[0])]["lots"].append(lot)
        print(f"[subjects] {len(subjects)}; on an instrument with a canonical row: {mapped}; lots: {len(lots)}")
        named = {s[3] for s in subjects if s[3] is not None} | {lot[0] for lot in lots}
        for comp in sorted({component(i) for i in named if i in instruments}, key=min):
            rows = ", ".join(f"{m}:{instruments[m][0]}->{instruments[m][1]}" for m in sorted(comp))
            print(f"[component] {rows} => {security(min(comp))}")

        for sec in sorted(by_sec):
            g = by_sec[sec]
            unresolved = g.get("unresolved", [])
            print(
                f"\n[security {sec}] admitted={len(g.get('admitted', []))} unresolved={len(unresolved)} "
                f"excluded={len(g.get('excluded_non_share_product', []))} lots={len(g.get('lots', []))}"
            )
            for outcome in ("admitted", "unresolved", "excluded_non_share_product"):
                for s in g.get(outcome, []):
                    print(
                        f"  {outcome} {s[0]} instrument={s[3]} product={s[2]} reasons={s[4]} "
                        f"execution_evidence={s[5]} total_fees_unobserved={len(s[6])}"
                    )
            for lot in g.get("lots", []):
                print(
                    f"  lot {lot[8]} instrument={lot[0]} {lot[1]} {lot[2]} execution_evidence={lot[9]} "
                    f"subject={lot[10]}"
                )
            days: dict[tuple[Any, str], list[Any]] = defaultdict(list)
            for lot in g.get("lots", []):
                days[(lot[2], lot[1])].append(lot)
            for (day, direction), group in sorted(days.items()):
                with localcontext() as ctx:
                    ctx.prec = 60
                    ctx.traps[Inexact] = True
                    ctx.traps[Rounded] = True
                    units = sum((x[4] for x in group), Decimal(0))
                    gbp = None if any(x[7] is None for x in group) else sum((x[7] for x in group), Decimal(0))
                print(
                    f"  {day} {group[0][3]} {direction}: pieces={len(group)} units={units} amount_gbp={gbp} "
                    f"fx={sorted({x[6] for x in group})}"
                )

        legacy = conn.execute(
            "SELECT (SELECT count(*) FROM tax_lots), (SELECT count(*) FROM disposal_matches), "
            "(SELECT count(*) FROM s104_pool)"
        ).fetchone()
        print(f"\n[legacy] tax_lots, disposal_matches, s104_pool = {legacy}")


if __name__ == "__main__":
    main()
