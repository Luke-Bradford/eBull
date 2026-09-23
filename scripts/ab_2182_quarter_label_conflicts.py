"""#2182 item 3 — full-population A/B of quarter fiscal-label conflict resolution.

Read-only. Derives periods for every instrument in ``financial_facts_raw`` with the
checkout's own code and writes one line per derived row, plus a per-instrument
count of quarter keys ``(period_type, period_end)`` held by more than one row (each
such key is a guaranteed ``CardinalityViolation`` in the canonical merge). Run it
from a main checkout and from this branch, then diff the two files:

    PYTHONPATH=. uv run python -m scripts.ab_2182_quarter_label_conflicts /tmp/ab_main.tsv
    PYTHONPATH=. uv run python -m scripts.ab_2182_quarter_label_conflicts /tmp/ab_branch.tsv
"""

from __future__ import annotations

import sys
from collections import Counter

import psycopg

from app.config import settings
from app.services.fundamentals import FactRow, _derive_periods_from_facts

_FACTS_SQL = """
SELECT concept, unit, period_start, period_end, val, frame, form_type, fiscal_year,
       fiscal_period, accession_number, filed_date  -- column names = FactRow fields
FROM financial_facts_raw
WHERE instrument_id = %(iid)s AND fiscal_year IS NOT NULL AND fiscal_period IS NOT NULL
ORDER BY period_end, concept
"""


def main(out_path: str) -> None:
    conflicted = 0
    with psycopg.connect(settings.database_url) as conn, open(out_path, "w") as out:
        iids = [r[0] for r in conn.execute("SELECT DISTINCT instrument_id FROM financial_facts_raw ORDER BY 1")]
        for n, iid in enumerate(iids):
            cur = conn.execute(_FACTS_SQL, {"iid": iid})
            names = [d.name for d in cur.description or []]
            facts = [FactRow(**dict(zip(names, r, strict=True))) for r in cur]
            if not facts:
                continue
            rows = _derive_periods_from_facts(facts)
            keys = Counter((p.period_type, p.period_end_date) for p in rows if p.period_type != "FY")
            dup = sum(1 for v in keys.values() if v > 1)
            if dup:
                conflicted += 1
                out.write(f"CONFLICT\t{iid}\t{dup}\n")
            for p in sorted(rows, key=lambda p: (p.period_type, p.period_end_date, p.fiscal_year)):
                out.write(f"ROW\t{iid}\t{sorted(vars(p).items())}\n")
            if n % 500 == 0:
                print(f"progress {n}/{len(iids)}", flush=True)
    print(f"instruments={len(iids)} with_conflicting_quarter_keys={conflicted}")


if __name__ == "__main__":
    main(sys.argv[1])
