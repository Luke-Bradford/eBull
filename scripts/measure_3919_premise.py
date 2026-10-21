"""#3919 premise: which stock items ``financial_periods_ttm`` would change under JKP's annual/quarterly combine.

Spec: ``docs/specs/fundamentals/2026-10-21-3919-ttm-stock-items-jkp-combine.md`` ("Problem"). Read-only.

The deployed view (sql/508) takes its twelve stock items from the latest Q1-Q4 row only. JKP's
``combine_ann_qtr_chars`` (bkelly-lab/ReplicationCrisis at 67174c7f170b, GlobalFactors/accounting_chars.sas:1220-1244)
takes, per item, the quarterly value when the annual one is missing or the quarterly is non-missing and newer, else the
annual one; each series contributes its latest row (the row-wise validity window, accounting_chars.sas:1071-1075).

Prints:

1. how each instrument's latest FY end sits against its latest quarter end, the annual rows past the 18-month bound,
   the quarters past it (left to #3920), and the currency mismatches between the two latest rows;
2. per item, the instruments whose value changes, split by cause (newer FY / NULL quarter filled from the latest FY /
   equal-end tie), and how many change to NULL (the combine never should); then each changed instrument once, under
   its cause, which is a property of its two latest rows (the date relation between them);
3. the equity tie table (FY and quarter ending together, equity differing), with the annual row's usability and the
   combined value, and the quarter groups the view's dedup cannot order (same period end and ``filed_date``);
4. the named rows from the issue, and the changed rows with a published cap.

    PYTHONPATH=. uv run python scripts/measure_3919_premise.py
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Final, LiteralString, cast

import psycopg

from app.config import settings

STOCK_ITEMS: Final = (
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash",
    "long_term_debt",
    "short_term_debt",
    "shares_outstanding",
    "inventory",
    "receivables",
    "payables",
    "goodwill",
    "ppe_net",
)
NAMED: Final = ("NTNX", "HRB", "MCFT", "PURR")


def _latest(types: str) -> str:
    # One row per period end (latest filed wins, the view's own dedup), then the latest period end per instrument.
    return f"""
        SELECT DISTINCT ON (instrument_id) instrument_id, period_end_date AS pe, reported_currency AS cur,
               CURRENT_DATE > (date_trunc('month', period_end_date + interval '18 months')
                               + interval '1 month - 1 day')::date AS expired,
               {", ".join(STOCK_ITEMS)}
        FROM (
            SELECT DISTINCT ON (instrument_id, period_end_date) *
            FROM financial_periods
            WHERE period_type IN ({types}) AND superseded_at IS NULL AND normalization_status = 'normalized'
            ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
        ) d
        ORDER BY instrument_id, period_end_date DESC"""


_PAIRS_SQL: Final = f"""
WITH a AS ({_latest("'FY'")}),
     q AS ({_latest("'Q1','Q2','Q3','Q4'")})
SELECT t.instrument_id, i.symbol, t.ttm_end, q.pe AS q_pe, a.pe AS a_pe, q.cur AS q_cur, a.cur AS a_cur,
       q.expired AS q_expired, a.expired AS a_expired, v.market_cap_live IS NOT NULL AS has_cap,
       {", ".join(f"t.{c} AS view_{c}, q.{c} AS q_{c}, a.{c} AS a_{c}" for c in STOCK_ITEMS)}
FROM financial_periods_ttm t
JOIN instruments i USING (instrument_id)
JOIN q USING (instrument_id)
LEFT JOIN a USING (instrument_id)
LEFT JOIN instrument_valuation v USING (instrument_id)
"""


# Each group says whether it can win the view's dedup: its period end is the instrument's latest quarter end and its
# filed_date is the greatest there (DESC NULLS LAST). Only such a group can make the selected stock row ambiguous.
_QUARTER_TIES_SQL: Final = """
WITH elig AS (
    SELECT instrument_id, period_end_date, filed_date
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
),
winner AS (
    SELECT DISTINCT ON (instrument_id) instrument_id, period_end_date, filed_date
    FROM elig
    ORDER BY instrument_id, period_end_date DESC, filed_date DESC NULLS LAST
)
SELECT g.instrument_id, g.period_end_date, g.filed_date, g.n,
       EXISTS (SELECT 1 FROM winner w
               WHERE w.instrument_id = g.instrument_id AND w.period_end_date = g.period_end_date
                 AND w.filed_date IS NOT DISTINCT FROM g.filed_date) AS can_win
FROM (
    SELECT instrument_id, period_end_date, filed_date, count(*) AS n
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
    GROUP BY instrument_id, period_end_date, filed_date
    HAVING count(*) > 1
) g
ORDER BY 1, 2
"""


def _is_nan(v: Any) -> bool:
    return isinstance(v, Decimal) and v.is_nan()


def annual_usable(r: dict[str, Any]) -> bool:
    """Spec "Construction" 1: an annual row supplies items only within 18 months and in the quarter's currency."""
    return (
        r["a_pe"] is not None
        and not r["a_expired"]
        and r["a_cur"] is not None
        and r["q_cur"] is not None
        and r["a_cur"] == r["q_cur"]
    )


def combine(q_val: Any, q_pe: Any, a_val: Any, a_pe: Any) -> Any:
    """accounting_chars.sas:1235: ``if missing(ann) or (not missing(qtr) and datadate_q > datadate) then ann = qtr``.

    Called only for a usable annual row, so ``a_pe`` is set; ``q_pe`` is set on every row (inner join on ``q``)."""
    if a_val is None or (q_val is not None and q_pe > a_pe):
        return q_val
    return a_val


def combined(r: dict[str, Any], c: str) -> Any:
    if not annual_usable(r):
        return r[f"q_{c}"]
    return combine(r[f"q_{c}"], r["q_pe"], r[f"a_{c}"], r["a_pe"])


def cause(r: dict[str, Any]) -> str:
    # Only a usable annual row can change a value (``combined`` returns the quarter's otherwise, and section 1 has
    # proved that equals the view's), so a changed row without one is a script defect: fail rather than misattribute.
    if not annual_usable(r):
        raise RuntimeError(f"instrument {r['instrument_id']} changed with no usable annual row")
    return "newer_fy" if r["a_pe"] > r["q_pe"] else "tie" if r["a_pe"] == r["q_pe"] else "fill"


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        cur = conn.execute(cast(LiteralString, _PAIRS_SQL))
        names = [d.name for d in cur.description or []]
        rows = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
        ties = conn.execute(_QUARTER_TIES_SQL).fetchall()
        conn.rollback()

    print(f"1. financial_periods_ttm rows: {len(rows)}")
    print(f"   latest FY end > latest quarter end: {sum(1 for r in rows if r['a_pe'] and r['a_pe'] > r['q_pe'])}")
    print(f"   latest FY end = latest quarter end: {sum(1 for r in rows if r['a_pe'] and r['a_pe'] == r['q_pe'])}")
    print(f"   latest FY end < latest quarter end: {sum(1 for r in rows if r['a_pe'] and r['a_pe'] < r['q_pe'])}")
    print(f"   no FY row: {sum(1 for r in rows if r['a_pe'] is None)}")
    behind = sum(1 for r in rows if r["a_pe"] and r["a_expired"] and r["a_pe"] < r["q_pe"])
    print(
        f"   latest FY past the 18-month bound: {sum(1 for r in rows if r['a_pe'] and r['a_expired'])}"
        f" (of them behind the quarter: {behind})"
    )
    print(f"   latest quarter past the 18-month bound (#3920, unchanged): {sum(1 for r in rows if r['q_expired'])}")
    print(
        f"   FY/quarter reported_currency differs or NULL: "
        f"{sum(1 for r in rows if r['a_pe'] and (r['a_cur'] != r['q_cur'] or r['a_cur'] is None))}"
    )
    bad_end = sum(1 for r in rows if r["ttm_end"] != r["q_pe"])
    print(f"   view quarter end != latest quarter end (must be 0): {bad_end}")
    # The script's quarter row must be the view's: same dedup clause, so only a same-filed_date tie could differ.
    off = [r["instrument_id"] for r in rows if any(r[f"view_{c}"] != r[f"q_{c}"] for c in STOCK_ITEMS)]
    print(f"   view stock items != the script's latest-quarter row (must be 0): {len(off)} {off[:10]}")
    # numeric NaN is no SQL NULL, so combine() would treat it as a value; JKP has no such state. Refuse it.
    nan = [r["instrument_id"] for r in rows for x in ("q", "a") for c in STOCK_ITEMS if _is_nan(r[f"{x}_{c}"])]
    print(f"   numeric NaN in a latest-quarter or latest-FY stock item (must be 0): {len(nan)} {nan[:10]}")
    if off or nan or bad_end:
        # Every later figure attributes a change to the combine; with a different quarter row it would not be.
        print("   FAIL: a must-be-0 check above is not 0; no attribution is printed")
        return 1

    print("2. per item: changed = newer FY + NULL-quarter fill + tie; to_null must be 0")
    changed: dict[int, dict[str, Any]] = {}
    for c in STOCK_ITEMS:
        n: dict[str, int] = {"newer_fy": 0, "fill": 0, "tie": 0}
        to_null = 0
        for r in rows:
            new = combined(r, c)
            if new == r[f"view_{c}"]:
                continue
            changed[r["instrument_id"]] = r
            to_null += new is None
            n[cause(r)] += 1
        print(
            f"   {c}: {sum(n.values())} = {n['newer_fy']} newer FY + {n['fill']} fill + {n['tie']} tie;"
            f" to_null {to_null}"
        )
    # An instrument's cause is a property of its two latest rows, so every changed item of it has the same one.
    by_cause = {k: sum(1 for r in changed.values() if cause(r) == k) for k in ("newer_fy", "tie", "fill")}
    print(f"   instruments with any change: {len(changed)} = {by_cause}")

    print("3. equal-end equity ties with differing values:")
    for r in rows:
        if r["a_pe"] == r["q_pe"] and r["a_shareholders_equity"] != r["q_shareholders_equity"]:
            print(
                f"   {r['symbol']} {r['q_pe']} quarter={r['q_shareholders_equity']} annual={r['a_shareholders_equity']}"
                f" annual_usable={annual_usable(r)} combined={combined(r, 'shareholders_equity')}"
            )
    print(f"   quarter (instrument, period end, filed_date) groups with more than one row: {len(ties)}")
    for t in ties:
        print(f"     instrument {t[0]} {t[1]} filed {t[2]}: {t[3]} rows, can win the view's dedup: {t[4]}")
    print(f"   of them able to win the dedup (ambiguous stock row): {sum(1 for t in ties if t[4])}")

    print("4. named rows (view -> combine):")
    for r in rows:
        if r["symbol"] in NAMED:
            eq = combined(r, "shareholders_equity")
            print(
                f"   {r['symbol']}: quarter {r['q_pe']} FY {r['a_pe']} equity {r['view_shareholders_equity']} -> {eq}"
            )
    pub = [r for r in changed.values() if r["has_cap"]]
    print(
        f"   changed rows with a published cap: {len(pub)} = "
        f"{ {k: sum(1 for r in pub if cause(r) == k) for k in ('newer_fy', 'tie', 'fill')} }"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
