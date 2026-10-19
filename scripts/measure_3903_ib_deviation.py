"""#3903: how far TTM ``NetIncomeLoss`` (the view's P/E numerator) sits from JKP's NI* on today's population.

Spec: ``docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md`` ("The numerator is a proxy"). Read-only.

NI* is JKP Table 5's IB, else NI − ``xido*`` (#3609 Amendment 2b): per quarter, ``IncomeLossFromContinuingOperations``
when filed, else ``NetIncomeLoss`` − DO − XI, where DO is, per quarter, the parent discontinued-operations concept,
else the consolidated one, and an unfiled DO or XI counts as zero. That last step is a simplification of the panel's
witness rules (``app/services/factor_panel.py`` ``companion``), stated, not hidden.

Facts come from the local SEC ``companyfacts.zip`` (path, size and SHA-256 printed). For each complete-TTM row of
``instrument_valuation`` with a positive ``market_cap_live``, the four window quarters are sql/220's. A quarter's value
for a concept is the latest-filed fact ending on the quarter's ``period_end_date`` with a duration of 80–100 days;
else, for a quarter whose FY fact (350–380 days) ends there, FY minus the latest-filed 9-month fact (260–285 days)
sharing the FY start. A quarter with neither is unresolved for that concept.

Validation first: the same rule applied to ``NetIncomeLoss`` is compared with the stored quarterly ``net_income``.

Prints, over windows whose NI* resolved in all four quarters:
1. the NetIncomeLoss rule against the stored quarters (agreement within 0.5%);
2. NI* TTM ÷ ``net_income_ttm`` in buckets, and which branch each window used;
3. sign disagreements: ``net_income_ttm > 0`` with NI* ≤ 0, and the reverse (with symbols);
4. the proposed P/E's relative change if NI* replaced ``net_income_ttm`` (|Δ| > 5%, 10%, 25%).

    PYTHONPATH=. uv run python scripts/measure_3903_ib_deviation.py
"""

from __future__ import annotations

import json
import zipfile
from collections import Counter
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.security.master_key import resolve_data_dir

NI = "NetIncomeLoss"
IB = "IncomeLossFromContinuingOperations"
DO = (
    "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity",
    "IncomeLossFromDiscontinuedOperationsNetOfTax",
)
XI = "ExtraordinaryItemNetOfTax"

_ROWS = """
    WITH d AS (
        SELECT DISTINCT ON (instrument_id, period_end_date) *
        FROM financial_periods
        WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
        ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
    ), r AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY period_end_date DESC) AS rn FROM d
    )
    SELECT v.instrument_id, i.symbol, v.net_income_ttm, v.market_cap_live, lpad(ei.identifier_value, 10, '0') AS cik10,
           array_agg(r.period_end_date ORDER BY r.period_end_date) AS ends,
           array_agg(r.net_income ORDER BY r.period_end_date) AS stored_ni
    FROM instrument_valuation v
    JOIN instruments i USING (instrument_id)
    JOIN external_identifiers ei ON ei.instrument_id = v.instrument_id AND ei.provider = 'sec'
         AND ei.identifier_type = 'cik' AND ei.is_primary
    JOIN r ON r.instrument_id = v.instrument_id AND r.rn <= 4
    WHERE v.is_complete_ttm AND v.market_cap_live > 0 AND v.net_income_ttm IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
"""


def _days(f: dict[str, Any]) -> int:
    return (date.fromisoformat(f["end"]) - date.fromisoformat(f["start"])).days


def _latest(facts: list[dict[str, Any]]) -> Decimal | None:
    if not facts:
        return None
    return Decimal(str(max(facts, key=lambda f: (f["filed"], f["accn"]))["val"]))


def quarter_value(facts: list[dict[str, Any]], end: date) -> Decimal | None:
    e = end.isoformat()
    durational = [f for f in facts if f.get("start") and f["end"] == e]
    q = _latest([f for f in durational if 80 <= _days(f) <= 100])
    if q is not None:
        return q
    fy = [f for f in durational if 350 <= _days(f) <= 380]
    if not fy:
        return None
    fy_latest = max(fy, key=lambda f: (f["filed"], f["accn"]))
    nine = _latest([f for f in facts if f.get("start") == fy_latest["start"] and 260 <= _days(f) <= 285])
    return None if nine is None else Decimal(str(fy_latest["val"])) - nine


def ni_star_ttm(gaap: dict[str, Any], ends: list[date]) -> tuple[Decimal, set[str]] | None:
    """NI* summed over the window's quarters (module docstring's rule) and the branches used; None if a quarter has
    neither IB nor NI."""

    def usd(concept: str) -> list[dict[str, Any]]:
        return gaap.get(concept, {}).get("units", {}).get("USD", [])

    ni_facts, ib_facts, xi_facts = usd(NI), usd(IB), usd(XI)
    do_by_concept = [usd(c) for c in DO]
    total = Decimal(0)
    used: set[str] = set()
    for end in ends:
        ib_q = quarter_value(ib_facts, end)
        if ib_q is not None:
            total += ib_q
            used.add("IB")
            continue
        ni_q = quarter_value(ni_facts, end)
        if ni_q is None:
            return None
        # DO per quarter: the parent concept, else the consolidated one (an issuer can switch between quarters).
        do_q = next((v for v in (quarter_value(f, end) for f in do_by_concept) if v is not None), None)
        xi_q = quarter_value(xi_facts, end)
        # The branch label records a FILED DO or XI, zero included; an unfiled one counts as zero.
        used.add("NI-xido" if do_q is not None or xi_q is not None else "NI")
        total += ni_q - (do_q or Decimal(0)) - (xi_q or Decimal(0))
    return total, used


def bucket(x: Decimal) -> str:
    """Closed bands at both edges of the centre bucket: 0.95 and 1.05 are in ``[0.95,1.05]``."""
    if x < Decimal("0.75"):
        return "<0.75"
    if x < Decimal("0.95"):
        return "[0.75,0.95)"
    if x <= Decimal("1.05"):
        return "[0.95,1.05]"
    if x <= Decimal("1.25"):
        return "(1.05,1.25]"
    return ">1.25"


def main() -> None:
    path = resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip"
    sha = (
        (path.with_name(path.name + ".sha256")).read_text().strip()
        if path.with_name(path.name + ".sha256").exists()
        else None
    )
    print(f"#3903 IB deviation; {path} ({path.stat().st_size} bytes, sha256 file {sha})")
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(_ROWS).fetchall()
        conn.rollback()
    print(f"complete-TTM rows with a published cap and a net_income_ttm: {len(rows)}")

    validation: Counter[str] = Counter()
    branch: Counter[str] = Counter()
    ratios: dict[str, Decimal] = {}
    unresolved: Counter[str] = Counter()
    flips_pos_to_nonpos: list[str] = []
    flips_nonpos_to_pos: list[str] = []
    with zipfile.ZipFile(path) as z:
        names = set(z.namelist())
        for r in rows:
            member = f"CIK{r['cik10']}.json"
            if member not in names:
                unresolved["no companyfacts member"] += 1
                continue
            gaap = json.loads(z.read(member)).get("facts", {}).get("us-gaap", {})
            ni_facts = gaap.get(NI, {}).get("units", {}).get("USD", [])
            for end, stored in zip(r["ends"], r["stored_ni"], strict=True):
                ni_q = quarter_value(ni_facts, end)
                if stored is not None and ni_q is not None:
                    agree = abs(ni_q - stored) <= Decimal("0.005") * max(abs(stored), Decimal(1))
                    validation["agree" if agree else "differ"] += 1
                else:
                    validation["unresolved"] += 1
            star = ni_star_ttm(gaap, r["ends"])
            if star is None:
                unresolved["a quarter with neither IB nor NI"] += 1
                continue
            total, used = star
            branch["+".join(sorted(used))] += 1
            ni_ttm = r["net_income_ttm"]
            if ni_ttm > 0 and total <= 0:
                flips_pos_to_nonpos.append(r["symbol"])
            elif ni_ttm <= 0 < total:
                flips_nonpos_to_pos.append(r["symbol"])
            elif ni_ttm > 0:
                ratios[r["symbol"]] = total / ni_ttm

    print("\n1. NetIncomeLoss rule vs stored quarterly net_income:", dict(validation))
    print("   unresolved windows:", dict(unresolved))
    print(f"\n2. NI* TTM / net_income_ttm over {len(ratios)} windows (both positive):")
    print("  ", dict(sorted(Counter(bucket(x) for x in ratios.values()).items())))
    print("   branches:", dict(branch.most_common()))
    print(f"\n3. net_income_ttm > 0, NI* <= 0: {len(flips_pos_to_nonpos)} {sorted(flips_pos_to_nonpos)}")
    print(f"   net_income_ttm <= 0, NI* > 0: {len(flips_nonpos_to_pos)} {sorted(flips_nonpos_to_pos)}")
    print("\n4. proposed P/E relative change if NI* replaced net_income_ttm (= net_income_ttm / NI* − 1):")
    for th in ("0.05", "0.10", "0.25"):
        n = sum(1 for x in ratios.values() if abs(1 / x - 1) > Decimal(th))
        print(f"   |change| > {th}: {n}")
    worst = sorted(ratios.items(), key=lambda kv: abs(1 / kv[1] - 1), reverse=True)[:15]
    print("   largest:", [(s, round(float(1 / x - 1), 3)) for s, x in worst])


if __name__ == "__main__":
    main()
