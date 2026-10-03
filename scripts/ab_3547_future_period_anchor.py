"""#3547 — paired full-population A/B: future-context facts excluded from period derivation.

Read-only. One process, one REPEATABLE READ snapshot per instrument, so the jobs
daemon cannot ingest between the arms. Control is ``origin/main``'s
``app/services/fundamentals/__init__.py`` loaded verbatim from ``git show`` (not a
simulation); treatment is this checkout's module.

Arms (full-population-ab skill):
  1. control vs treatment derived rows, keyed (instrument, period_type, period_end);
  2. treatment vs STORED ``financial_periods`` (current, sec_edgar);
  3. mechanism: every lost/gained key with its filed_date, every changed field.

    PYTHONPATH=. uv run python -m scripts.ab_3547_future_period_anchor /tmp/ab_3547

Writes ``<prefix>.lost.tsv`` / ``.gained.tsv`` / ``.changed.tsv`` and prints the summary.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path
from types import ModuleType

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

_STORED_SQL = """
SELECT period_type, period_end_date
FROM financial_periods
WHERE instrument_id = %(iid)s AND superseded_at IS NULL AND source = 'sec_edgar'
"""


def _load_control() -> ModuleType:
    src = subprocess.run(
        ["git", "show", "origin/main:app/services/fundamentals/__init__.py"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    path = Path(tempfile.mkdtemp()) / "fundamentals_control.py"
    path.write_text(src)
    spec = importlib.util.spec_from_file_location("fundamentals_control", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fundamentals_control"] = mod
    spec.loader.exec_module(mod)
    return mod


def _survivors(rows: list) -> dict:
    """Mirror the canonical merge: ONE row per fiscal label survives.

    ``_canonical_merge_instrument`` keys on (fiscal_year, fiscal_quarter,
    period_type) and keeps filed_date DESC, period_end_date DESC, source_ref ASC
    (source priority is constant — every derived row is sec_edgar). A derived-row
    diff cannot see a label collision; this can.
    """
    by_label: dict = {}
    for p in sorted(rows, key=lambda p: p.source_ref or ""):
        k = (p.fiscal_year, p.fiscal_quarter, p.period_type)
        cur = by_label.get(k)
        rank = (p.filed_date or date.min, p.period_end_date)
        if cur is None or rank > (cur.filed_date or date.min, cur.period_end_date):
            by_label[k] = p
    return {(p.period_type, p.period_end_date): p for p in by_label.values()}


def main(prefix: str) -> None:
    control = _load_control()
    assert "_fact_period_ended_by_filing" not in vars(control), "control must be origin/main"
    c = Counter()
    changed_fields: Counter[str] = Counter()
    stored_future_after = 0
    with (
        psycopg.connect(settings.database_url) as conn,
        open(f"{prefix}.lost.tsv", "w") as lost_f,
        open(f"{prefix}.gained.tsv", "w") as gained_f,
        open(f"{prefix}.changed.tsv", "w") as changed_f,
        open(f"{prefix}.survivors.tsv", "w") as surv_f,
    ):
        iids = [r[0] for r in conn.execute("SELECT DISTINCT instrument_id FROM financial_facts_raw ORDER BY 1")]
        conn.commit()
        for n, iid in enumerate(iids):
            with conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur = conn.execute(_FACTS_SQL, {"iid": iid})
                names = [d.name for d in cur.description or []]
                rows = cur.fetchall()
                stored = {(r[0], r[1]) for r in conn.execute(_STORED_SQL, {"iid": iid})}
            if not rows:
                continue
            c["instruments"] += 1
            facts_t = [FactRow(**dict(zip(names, r, strict=True))) for r in rows]
            facts_c = [control.FactRow(**dict(zip(names, r, strict=True))) for r in rows]
            if any(f.period_end > f.filed_date for f in facts_t):
                c["instruments_with_future_facts"] += 1
            ctl_rows = control._derive_periods_from_facts(facts_c)
            trt_rows = _derive_periods_from_facts(facts_t)
            ctl = {(p.period_type, p.period_end_date): p for p in ctl_rows}
            trt = {(p.period_type, p.period_end_date): p for p in trt_rows}
            # Arm 3 — post-merge survivors (label collisions).
            s_ctl, s_trt = _survivors(ctl_rows), _survivors(trt_rows)
            for k in s_ctl.keys() - s_trt.keys():
                p = s_ctl[k]
                phantom = p.filed_date is not None and p.period_end_date > p.filed_date
                c["survivor_lost_phantom" if phantom else "survivor_lost_NOT_phantom"] += 1
                if not phantom:
                    surv_f.write(f"LOST\t{iid}\t{k[0]}\t{k[1]}\t{p.fiscal_year}\t{p.filed_date}\n")
            for k in s_trt.keys() - s_ctl.keys():
                c["survivor_gained"] += 1
                surv_f.write(f"GAINED\t{iid}\t{k[0]}\t{k[1]}\t{s_trt[k].fiscal_year}\t{s_trt[k].filed_date}\n")
            c["control_rows"] += len(ctl)
            c["treatment_rows"] += len(trt)
            touched = False
            for k in ctl.keys() - trt.keys():
                p = ctl[k]
                phantom = p.filed_date is not None and p.period_end_date > p.filed_date
                c["lost_phantom" if phantom else "lost_NOT_phantom"] += 1
                lost_f.write(f"{iid}\t{k[0]}\t{k[1]}\t{p.fiscal_year}\t{p.filed_date}\t{phantom}\n")
                touched = True
            for k in trt.keys() - ctl.keys():
                p = trt[k]
                c["gained"] += 1
                c["gained_future"] += int(p.filed_date is not None and p.period_end_date > p.filed_date)
                gained_f.write(f"{iid}\t{k[0]}\t{k[1]}\t{p.fiscal_year}\t{p.filed_date}\t{k in stored}\n")
                touched = True
            for k in ctl.keys() & trt.keys():
                a, b = vars(ctl[k]), vars(trt[k])
                diff = sorted(f for f in a if a[f] != b[f])
                if diff:
                    c["changed_rows"] += 1
                    changed_fields.update(diff)
                    changed_f.write(f"{iid}\t{k[0]}\t{k[1]}\t{diff}\t{[(a[f], b[f]) for f in diff]}\n")
                    touched = True
            c["instruments_touched"] += int(touched)
            # Arm 2 — treatment vs stored.
            c["stored_rows"] += len(stored)
            c["stored_not_in_treatment"] += len(stored - trt.keys())
            c["treatment_not_in_stored"] += len(trt.keys() - stored)
            stored_future_after += sum(
                1 for p in trt.values() if p.filed_date is not None and p.period_end_date > p.filed_date
            )
            if n % 1000 == 0:
                print(f"progress {n}/{len(iids)}", flush=True)
    for k in sorted(c):
        print(f"{k}={c[k]}")
    print(f"treatment_rows_with_period_end_after_filed={stored_future_after}")
    print(f"changed_fields={dict(changed_fields.most_common())}")


if __name__ == "__main__":
    main(sys.argv[1])
