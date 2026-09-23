"""#2182 part B — two REAL arms of the canonical merge, full population, rolled back.

Per instrument, inside one REPEATABLE READ transaction that is always rolled back:
  arm A = ``normalize_financial_periods`` with every row's balance sheet treated as
          presented, which makes the merge SQL identical to main's overwrite-all;
  arm B = this branch unchanged.
Each arm runs in its own savepoint on the same snapshot, then the canonical rows of
the two arms are diffed cell by cell. Nothing is committed.

Scope: instruments with at least one ``balance_sheet_presented=False`` row. For every
other instrument the two arms run byte-identical SQL.

Needs ``sql/416`` applied (additive column, DEFAULT TRUE).

    PYTHONPATH=. uv run python -m scripts.ab_2182_p2_merge_arms > /tmp/ab_2182_arms.txt 2>&1
"""

from __future__ import annotations

from collections import Counter
from typing import Any
from unittest import mock

import psycopg
from psycopg import IsolationLevel

import app.services.fundamentals as fundamentals
from app.config import settings

_FACTS_SQL = """
SELECT concept, unit, period_start, period_end, val, frame, form_type, fiscal_year,
       fiscal_period, accession_number, filed_date
FROM financial_facts_raw
WHERE instrument_id = %(iid)s AND fiscal_year IS NOT NULL AND fiscal_period IS NOT NULL
ORDER BY period_end, concept
"""


def _canonical(conn: psycopg.Connection[Any], iid: int) -> dict[tuple[Any, str], dict[str, Any]]:
    cur = conn.execute("SELECT * FROM financial_periods WHERE instrument_id = %s AND superseded_at IS NULL", (iid,))
    names = [d.name for d in cur.description or []]
    rows = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
    return {(r["period_end_date"], r["period_type"]): r for r in rows}


def _arm(conn: psycopg.Connection[Any], iid: int, *, force_presented: bool) -> dict[tuple[Any, str], dict[str, Any]]:
    with conn.transaction(force_rollback=True):
        if force_presented:
            with mock.patch.object(fundamentals, "_fy_balance_sheet_is_presented", return_value=True):
                fundamentals.normalize_financial_periods(conn, [iid])
        else:
            fundamentals.normalize_financial_periods(conn, [iid])
        return _canonical(conn, iid)


def main() -> None:
    c = Counter[str]()
    inst = Counter[str]()
    samples: list[str] = []
    ignore = {"updated_at", "created_at", "normalized_at"}
    with psycopg.connect(settings.database_url) as conn:
        iids = [r[0] for r in conn.execute("SELECT DISTINCT instrument_id FROM financial_facts_raw ORDER BY 1")]
        conn.commit()
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        for n, iid in enumerate(iids):
            with conn.transaction(force_rollback=True):
                facts = [fundamentals.FactRow(*r) for r in conn.execute(_FACTS_SQL, {"iid": iid})]
                if not any(not p.balance_sheet_presented for p in fundamentals._derive_periods_from_facts(facts)):
                    continue
                inst["in_scope"] += 1
                a = _arm(conn, iid, force_presented=True)
                b = _arm(conn, iid, force_presented=False)
                if a.keys() != b.keys():
                    inst["row_set_differs"] += 1
                differs = False
                for key in a.keys() & b.keys():
                    for col, av in a[key].items():
                        if col in ignore:
                            continue
                        bv = b[key][col]
                        if av == bv:
                            continue
                        differs = True
                        if av is None:
                            kind = "B_keeps_A_nulls"
                        elif bv is None:
                            kind = "B_nulls_A_keeps"
                        else:
                            kind = "both_nonnull_differ"
                            if col == "shareholders_equity":
                                parent = conn.execute(
                                    "SELECT count(*) FROM financial_facts_raw WHERE instrument_id=%s "
                                    "AND concept='StockholdersEquity' AND period_start IS NULL AND period_end=%s",
                                    (iid, key[0]),
                                ).fetchone()
                                c[f"equity_differ:raw_has_parent_concept={bool(parent and parent[0])}"] += 1
                                if len(samples) < 30:
                                    samples.append(f"{iid} {key} A(raw)={av} B(canon)={bv}")
                        c[f"{kind}:{col}"] += 1
                        group = "preserved" if col in fundamentals._PRESERVED_WHEN_UNPRESENTED_COLUMNS else "OTHER"
                        c[f"{kind}:{group}"] += 1
                if differs:
                    inst["canonical_differs"] += 1
            if n % 250 == 0:
                print(f"progress {n}/{len(iids)} {dict(inst)}", flush=True)
    print("instruments:", dict(inst))
    for k, v in sorted(c.items()):
        print(f"{k}\t{v}")
    print("\n".join(samples))


if __name__ == "__main__":
    main()
