"""#2182 part B — full-population measurement of the P−2 balance-sheet overwrite.

Read-only. For every instrument in ``financial_facts_raw``, derive periods with the
current code, take the rows whose ``balance_sheet_presented`` is False, and compare
their balance-sheet cells with the canonical ``financial_periods`` row at the same
key. Arm A = main's merge (overwrite every cell with raw); arm B = this branch's
merge (keep canonical, fill a NULL from raw). Both arms are evaluated on the SAME
derived raw row and the SAME canonical row, so the only difference is the merge rule.

    PYTHONPATH=. uv run python -m scripts.ab_2182_p2_balance_sheet > /tmp/ab_2182_b.txt 2>&1
"""

from __future__ import annotations

from collections import Counter

import psycopg
from psycopg import sql

from app.config import settings
from app.services.fundamentals import (
    _PRESERVED_WHEN_UNPRESENTED_COLUMNS,
    FactRow,
    _derive_periods_from_facts,
)

_FACTS_SQL = """
SELECT concept, unit, period_start, period_end, val, frame, form_type, fiscal_year,
       fiscal_period, accession_number, filed_date
FROM financial_facts_raw
WHERE instrument_id = %(iid)s AND fiscal_year IS NOT NULL AND fiscal_period IS NOT NULL
ORDER BY period_end, concept
"""


def main() -> None:
    cols = sorted(_PRESERVED_WHEN_UNPRESENTED_COLUMNS)
    c = Counter[str]()
    inst = Counter[str]()
    samples: list[str] = []
    restore: set[int] = set()
    with psycopg.connect(settings.database_url) as conn:
        iids = [r[0] for r in conn.execute("SELECT DISTINCT instrument_id FROM financial_facts_raw ORDER BY 1")]
        for n, iid in enumerate(iids):
            facts = [FactRow(*r) for r in conn.execute(_FACTS_SQL, {"iid": iid})]
            if not facts:
                continue
            rows = [p for p in _derive_periods_from_facts(facts) if not p.balance_sheet_presented]
            if not rows:
                continue
            inst["with_unpresented_row"] += 1
            hit = set()
            for p in rows:
                c[f"unpresented_{p.period_type}"] += 1
                canon = conn.execute(
                    sql.SQL(
                        "SELECT {} FROM financial_periods "
                        "WHERE instrument_id=%s AND period_end_date=%s AND period_type=%s AND superseded_at IS NULL"
                    ).format(sql.SQL(", ").join(map(sql.Identifier, cols))),
                    (iid, p.period_end_date, p.period_type),
                ).fetchone()
                other = conn.execute(
                    "SELECT period_end_date, total_assets IS NOT NULL FROM financial_periods WHERE instrument_id=%s "
                    "AND fiscal_year=%s AND period_type=%s AND period_end_date<>%s AND superseded_at IS NULL",
                    (iid, p.fiscal_year, p.period_type, p.period_end_date),
                ).fetchall()
                for o_end, o_bs in other:
                    gap = abs((o_end - p.period_end_date).days)
                    c[f"phase_b_sibling:{'bs' if o_bs else 'no_bs'}:{'<=7d' if gap <= 7 else '>7d'}"] += 1
                if canon is None:
                    c["no_canonical_row"] += 1
                    continue
                if canon[cols.index("total_assets")] is None:
                    c["canon_total_assets_null"] += 1
                    restore.add(iid)
                row_clobbered = False
                for col, cv in zip(cols, canon, strict=True):
                    rv = getattr(p, col)
                    if cv is not None and rv is None:
                        c[f"A_nulls:{col}"] += 1
                        row_clobbered = True
                    elif cv is not None and rv is not None and cv != rv:
                        c[f"A_changes:{col}"] += 1
                        if len(samples) < 40:
                            samples.append(f"{iid} {p.period_type} {p.period_end_date} {col} canon={cv} raw={rv}")
                    elif cv is None and rv is not None:
                        c[f"both_fill:{col}"] += 1
                if row_clobbered:
                    c["rows_A_clobbers"] += 1
                    hit.add(iid)
            if hit:
                inst["A_clobbers"] += 1
            if n % 500 == 0:
                print(f"progress {n}/{len(iids)}", flush=True)
    print("instruments:", dict(inst))
    for k, v in sorted(c.items()):
        print(f"{k}\t{v}")
    print("\n".join(samples))
    # Instruments whose unpresented row already lost its balance sheet: the input
    # to scripts.force_refresh_fundamentals (full companyfacts re-fetch).
    with open("/tmp/ab_2182_restore_ids.txt", "w") as fh:
        fh.write("\n".join(str(i) for i in sorted(restore)))
    print(f"restore candidates: {len(restore)} instruments -> /tmp/ab_2182_restore_ids.txt")


if __name__ == "__main__":
    main()
