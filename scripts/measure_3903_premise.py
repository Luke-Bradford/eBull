"""#3903 premise measurements: ``instrument_valuation``'s per-share P/E and P/B against the total/ME form.

Spec: ``docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md``. Read-only; every read shares one REPEATABLE READ
snapshot. "Proposed" is the spec's construction evaluated over the deployed view's own columns:

- P/E (complete-TTM arm) = ``market_cap_live / net_income_ttm`` when ``net_income_ttm`` is finite and positive;
- P/E (legacy arm) = NULL;
- P/B (legacy arm) = ``market_cap_live / shareholders_equity`` of the ``financial_periods`` row the latest
  ``fundamentals_snapshot`` row was written from (its ``as_of_date``, the write-through's dedupe), when finite and
  positive. ``book_value × shares_outstanding`` is printed beside it: ``book_value`` is stored at 4 decimals.

"The window" is sql/220's: the latest four deduplicated normalized quarters, adjacent within 330 days.

Prints:

1. P/E, complete-TTM arm: today's published/NULL against the proposal's published/refused, refusals by cause;
2. over rows published both ways, today ÷ proposed in buckets, and the count outside [0.5, 2] by direct predicate;
3. the discrepancy inventory: every row published both ways outside [0.8, 1.25], by first matching candidate-cause
   predicate (``inventory_cause``);
4. today-published P/E whose ``net_income_ttm`` is not positive (a positive per-share sum beside a loss);
5. ``net_income_ttm`` NULL across every complete-TTM row, by cap outcome;
6. named cases: view operands, then each window quarter's operands;
7. the legacy arm: P/E and P/B today against proposed, and the reconstructed equity against the period row's
   ``shareholders_equity``;
8. non-finite operands (``NaN``, ``±Infinity``) in the columns the construction divides by;
9. the numerator's concepts in ``financial_facts_raw``, and the #3609 Amendment 2b DO-branch counts;
10. dividend yield (out of scope, coverage observations for #3903): today's DPS yield ÷ the paid-cash yield in
    buckets, and the non-NULL member counts of each window.

    PYTHONPATH=. uv run python scripts/measure_3903_premise.py
"""

from __future__ import annotations

import json
import subprocess
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings

NAMED = ("BKNG", "NFLX", "ORLY", "APH", "ALB", "TPL", "AAPL", "GME", "MSFT", "JPM", "HD")
WINDOW_NAMED = ("BKNG", "NFLX", "TPL", "ALB", "APH")
#: Inventory thresholds, by construction (spec "Problem"): a 1.5x spread of weighted diluted shares inside one window
#: is beyond a year's ordinary issuance; 20% is the agreement band's edge.
SPREAD = Decimal("1.5")
BAND = (Decimal("0.8"), Decimal("1.25"))

_TTM = """
    SELECT i.symbol, v.instrument_id, v.pe_ratio, v.market_cap_live, v.market_cap_reason, v.net_income_ttm,
           v.valuation_price, v.market_cap_pit, t.eps_diluted_ttm, v.dividend_yield, t.dps_declared_ttm,
           t.dividends_paid_ttm,
           CASE WHEN v.market_cap_live IS NOT NULL AND v.net_income_ttm > 0 AND v.net_income_ttm < 'Infinity'
                THEN v.market_cap_live / v.net_income_ttm END AS pe_proposed
    FROM instrument_valuation v
    JOIN instruments i USING (instrument_id)
    JOIN financial_periods_ttm t USING (instrument_id)
    WHERE v.is_complete_ttm
"""

_LEGACY = """
    SELECT i.symbol, v.instrument_id, v.pe_ratio, v.pb_ratio, v.market_cap_live, v.market_cap_reason,
           fs.as_of_date, fs.book_value, fs.shares_outstanding, fp.shareholders_equity AS period_equity,
           CASE WHEN fp.shareholders_equity > 0 AND fp.shareholders_equity < 'Infinity'
                THEN v.market_cap_live / fp.shareholders_equity END AS pb_proposed
    FROM instrument_valuation v
    JOIN instruments i USING (instrument_id)
    LEFT JOIN LATERAL (
        SELECT fs0.as_of_date, fs0.book_value, fs0.shares_outstanding FROM fundamentals_snapshot fs0
        WHERE fs0.instrument_id = v.instrument_id ORDER BY fs0.as_of_date DESC LIMIT 1
    ) fs ON TRUE
    -- The write-through's own row (``quarters`` dedupe: latest filed per period end).
    LEFT JOIN LATERAL (
        SELECT fp0.shareholders_equity FROM financial_periods fp0
        WHERE fp0.instrument_id = v.instrument_id AND fp0.period_end_date = fs.as_of_date
          AND fp0.period_type IN ('Q1','Q2','Q3','Q4') AND fp0.superseded_at IS NULL
          AND fp0.normalization_status = 'normalized'
        ORDER BY fp0.filed_date DESC NULLS LAST LIMIT 1
    ) fp ON TRUE
    WHERE v.is_complete_ttm IS NULL
"""

# sql/220's window, one row per quarter.
_WINDOW = """
    WITH d AS (
        SELECT DISTINCT ON (instrument_id, period_end_date) *
        FROM financial_periods
        WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
        ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
    ), r AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY period_end_date DESC) AS rn FROM d
    ), ok AS (
        SELECT instrument_id FROM r WHERE rn <= 4 GROUP BY instrument_id
        HAVING COUNT(*) = 4 AND MAX(period_end_date) - MIN(period_end_date) <= 330
    )
    SELECT r.instrument_id, r.period_end_date, r.period_type, r.months_covered, r.filed_date, r.net_income,
           r.eps_diluted, r.shares_diluted, r.dps_declared, r.dividends_paid
    FROM r JOIN ok USING (instrument_id) WHERE r.rn <= 4
    ORDER BY r.instrument_id, r.period_end_date
"""

_CONCEPTS = """
    SELECT c.concept, count(r.concept) AS n
    FROM unnest(%(concepts)s::text[]) AS c(concept)
    LEFT JOIN financial_facts_raw r ON r.concept = c.concept AND r.taxonomy = 'us-gaap'
    GROUP BY c.concept ORDER BY c.concept
"""
#: The IB / ``xido*`` concepts of the #3609 panel (``app/services/factor_panel.py`` IB, XI, XIDO_*), and NI.
CONCEPTS = (
    "NetIncomeLoss",
    "IncomeLossFromContinuingOperations",
    "IncomeLossFromDiscontinuedOperationsNetOfTax",
    "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity",
    "ExtraordinaryItemNetOfTax",
)
MEASUREMENT_2B = Path(__file__).resolve().parents[1] / "docs" / "research" / "3609-amendment-2b-measurement.json"

_NONFINITE = """
    SELECT
        (SELECT count(*) FROM financial_periods_ttm WHERE net_income_ttm IN ('NaN', 'Infinity', '-Infinity'))
            AS net_income_ttm,
        (SELECT count(*) FROM fundamentals_snapshot WHERE book_value IN ('NaN', 'Infinity', '-Infinity')) AS book_value,
        (SELECT count(*) FROM fundamentals_snapshot WHERE shares_outstanding IN ('NaN', 'Infinity', '-Infinity'))
            AS shares_outstanding,
        (SELECT count(*) FROM financial_periods_ttm WHERE shareholders_equity IN ('NaN', 'Infinity', '-Infinity'))
            AS shareholders_equity
"""


def bucket(x: Decimal) -> str:
    if x < Decimal("0.5"):
        return "<0.5"
    if x < BAND[0]:
        return "[0.5,0.8)"
    if x <= BAND[1]:
        return "[0.8,1.25]"
    if x <= 2:
        return "(1.25,2]"
    return ">2"


def pe_cause(r: dict[str, Any]) -> str:
    if r["market_cap_reason"] is not None:
        return f"cap:{r['market_cap_reason']}"
    if r["net_income_ttm"] is None:
        return "net_income_ttm NULL"
    return "net_income_ttm <= 0"


def inventory_cause(r: dict[str, Any], quarters: list[dict[str, Any]]) -> str:
    """The first candidate-cause predicate a disagreement outside the band matches, else ``untagged``.

    Precedence: window shares spread, then an EPS sign opposing its net income, then the EPS × shares product, then
    the PIT count. The predicates overlap, so the order sets the counts. A tag is a candidate, not a verified cause.
    """
    shares = [q["shares_diluted"] for q in quarters if q["shares_diluted"]]
    if shares and max(shares) / min(shares) >= SPREAD:
        return "window shares spread >= 1.5x"
    if any(
        q["eps_diluted"] is not None and q["net_income"] and q["eps_diluted"] * q["net_income"] < 0 for q in quarters
    ):
        return "a quarter's EPS sign opposes its net income"
    products = [
        q["eps_diluted"] * q["shares_diluted"] for q in quarters if q["eps_diluted"] is not None and q["shares_diluted"]
    ]
    if len(products) == 4 and r["net_income_ttm"]:
        ratio = sum(products) / r["net_income_ttm"]
        if not BAND[0] <= ratio <= BAND[1]:
            return "sum(EPS x diluted shares) vs net_income_ttm outside the band"
    pit = r["market_cap_pit"] or {}
    if shares and pit.get("shares_reported") and pit.get("split_factor"):
        effective = Decimal(str(pit["shares_reported"])) * Decimal(str(pit["split_factor"]))
        latest = quarters[-1]["shares_diluted"] or shares[-1]
        if not BAND[0] <= effective / latest <= BAND[1]:
            return "PIT effective count vs latest diluted shares outside the band"
    return "untagged"


def main() -> None:
    git = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=False)
    head = git.stdout.strip() if git.returncode == 0 else "unknown (git rev-parse failed)"
    print(f"#3903 premise, {datetime.now(UTC):%Y-%m-%d %H:%MZ}, code {head}")
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor(row_factory=dict_row) as cur:
            ttm = cur.execute(_TTM).fetchall()
            legacy = cur.execute(_LEGACY).fetchall()
            windows: dict[int, list[dict[str, Any]]] = {}
            for q in cur.execute(_WINDOW).fetchall():
                windows.setdefault(q["instrument_id"], []).append(q)
            nonfinite = cur.execute(_NONFINITE).fetchone()
            concepts = cur.execute(_CONCEPTS, {"concepts": list(CONCEPTS)}).fetchall()
            build = cur.execute(
                "SELECT max(build_id) AS b FROM market_cap_pit_builds WHERE status = 'complete'"
            ).fetchone()
        conn.rollback()
    print(
        f"complete-TTM rows {len(ttm)}, legacy rows {len(legacy)}, "
        f"latest complete PIT build {build['b'] if build else None}"
    )

    print("\n1. P/E, complete-TTM arm: today x proposed")
    cells: Counter[str] = Counter()
    for r in ttm:
        today, proposed = r["pe_ratio"] is not None, r["pe_proposed"] is not None
        if today and proposed:
            cells["kept"] += 1
        elif proposed:
            cells["gained"] += 1
        elif today:
            cells[f"lost, {pe_cause(r)}"] += 1
        else:
            cells[f"NULL both, {pe_cause(r)}"] += 1
    for k, n in sorted(cells.items()):
        print(f"   {k}: {n}")
    gained_eps = Counter(
        "eps_diluted_ttm NULL" if r["eps_diluted_ttm"] is None else "eps_diluted_ttm <= 0"
        for r in ttm
        if r["pe_ratio"] is None and r["pe_proposed"] is not None
    )
    print(f"   gained, by today's cause: {dict(gained_eps)}")

    print("\n2. today / proposed over rows published both ways")
    both = [r for r in ttm if r["pe_ratio"] is not None and r["pe_proposed"] is not None]
    ratios = {r["instrument_id"]: r["pe_ratio"] / r["pe_proposed"] for r in both}
    print(f"   {len(both)} rows: {dict(sorted(Counter(bucket(x) for x in ratios.values()).items()))}")
    print(f"   ratio < 0.5 or > 2: {sum(1 for x in ratios.values() if x < Decimal('0.5') or x > 2)}")

    print("\n3. discrepancy inventory (outside [0.8, 1.25])")
    out = [r for r in both if not BAND[0] <= ratios[r["instrument_id"]] <= BAND[1]]
    causes = Counter(inventory_cause(r, windows.get(r["instrument_id"], [])) for r in out)
    print(f"   {len(out)} rows")
    for k, n in causes.most_common():
        print(f"   {k}: {n}")
    for r in sorted(out, key=lambda r: r["symbol"]):
        if inventory_cause(r, windows.get(r["instrument_id"], [])) == "untagged":
            print(f"      untagged {r['symbol']}: today {r['pe_ratio']:.2f} proposed {r['pe_proposed']:.2f}")

    print("\n4. today-published P/E with net_income_ttm <= 0")
    neg = [r for r in ttm if r["pe_ratio"] is not None and r["net_income_ttm"] is not None and r["net_income_ttm"] <= 0]
    print(f"   {len(neg)} rows; P/E <= 10 (value-family pe_score 1.0): {sum(1 for r in neg if r['pe_ratio'] <= 10)}")

    print("\n5. net_income_ttm NULL, every complete-TTM row, by cap outcome")
    ni_null = [r for r in ttm if r["net_income_ttm"] is None]
    print(
        f"   {len(ni_null)} rows: "
        f"{dict(Counter(r['market_cap_reason'] or 'cap published' for r in ni_null).most_common())}"
    )
    print(f"   of which P/E published today: {sum(1 for r in ni_null if r['pe_ratio'] is not None)}")

    print("\n6. named cases")
    by_symbol = {r["symbol"]: r for r in ttm} | {r["symbol"]: r for r in legacy}
    for s in NAMED:
        r = by_symbol.get(s)
        if r is None:
            print(f"   {s}: no view row")
            continue
        print(
            f"   {s}: pe today {r['pe_ratio']} eps_diluted_ttm {r.get('eps_diluted_ttm')} "
            f"net_income_ttm {r.get('net_income_ttm')} market_cap_live {r['market_cap_live']} "
            f"reason {r['market_cap_reason']} pe proposed {r.get('pe_proposed')}"
        )
    for s in WINDOW_NAMED:
        r = by_symbol.get(s)
        if r is None:
            continue
        quarters = windows.get(r["instrument_id"], [])
        products = [
            q["eps_diluted"] * q["shares_diluted"]
            for q in quarters
            if q["eps_diluted"] is not None and q["shares_diluted"]
        ]
        print(f"   {s} window: sum(EPS x diluted shares) {sum(products) if len(products) == 4 else None}")
        for q in quarters:
            print(
                f"      {q['period_end_date']} {q['period_type']} months {q['months_covered']} filed {q['filed_date']} "
                f"net_income {q['net_income']} eps_diluted {q['eps_diluted']} shares_diluted {q['shares_diluted']}"
            )

    print("\n7. legacy arm")
    print(f"   pe_ratio published today: {sum(1 for r in legacy if r['pe_ratio'] is not None)}")
    pb_today = [r for r in legacy if r["pb_ratio"] is not None]
    pb_both = [r for r in pb_today if r["pb_proposed"] is not None]
    print(
        f"   pb_ratio published today {len(pb_today)}, "
        f"proposed {sum(1 for r in legacy if r['pb_proposed'] is not None)}"
    )
    print(f"   lost: {dict(Counter(r['market_cap_reason'] or 'equity' for r in pb_today if r['pb_proposed'] is None))}")
    print(f"   gained: {sum(1 for r in legacy if r['pb_ratio'] is None and r['pb_proposed'] is not None)}")
    pb_ratios = Counter(bucket(r["pb_ratio"] / r["pb_proposed"]) for r in pb_both)
    print(f"   today / proposed over {len(pb_both)}: {dict(sorted(pb_ratios.items()))}")
    for r in pb_both:
        if not BAND[0] <= r["pb_ratio"] / r["pb_proposed"] <= BAND[1]:
            print(f"      {r['symbol']}: today {r['pb_ratio']:.3f} proposed {r['pb_proposed']:.3f}")
    recon = [r for r in legacy if r["book_value"] and r["shares_outstanding"] and r["period_equity"]]
    errs = [
        abs(r["book_value"] * r["shares_outstanding"] - r["period_equity"]) / abs(r["period_equity"]) for r in recon
    ]
    print(
        f"   reconstructed equity vs period row over {len(recon)} (of {sum(1 for r in legacy if r['book_value'])} "
        f"with book_value): max relative error {max(errs) if errs else None}, "
        f"> 1e-6: {sum(1 for e in errs if e > Decimal('1e-6'))}"
    )

    print("\n8. non-finite operands")
    print(f"   {dict(nonfinite) if nonfinite else None}")

    print("\n9. the numerator's concepts in financial_facts_raw (us-gaap rows)")
    for c in concepts:
        print(f"   {c['concept']}: {c['n']}")
    adopted = json.loads(MEASUREMENT_2B.read_text())["counts"]["ni_adopted"]
    print(f"   #3609 Amendment 2b ni_adopted ({MEASUREMENT_2B.name}):")
    for k in (
        "values using the fallback (name-months)",
        "  DO branch: do_parent",
        "  DO branch: do_consolidated_proxy",
        "  DO branch: do_consolidated_minus_nci",
    ):
        print(f"      {k.strip()}: {adopted[k]}")

    print("\n10. dividend yield (coverage observations)")
    dy = [r for r in ttm if r["dividend_yield"] is not None and r["market_cap_live"] and r["dividends_paid_ttm"]]
    dy_buckets = Counter(
        bucket(r["dividend_yield"] / (abs(r["dividends_paid_ttm"]) / r["market_cap_live"] * 100)) for r in dy
    )
    print(f"   DPS yield / paid-cash yield over {len(dy)}: {dict(sorted(dy_buckets.items()))}")
    pos_dps = [r for r in ttm if r["dps_declared_ttm"] is not None and r["dps_declared_ttm"] > 0]
    dps_n = Counter(
        sum(1 for q in windows.get(r["instrument_id"], []) if q["dps_declared"] is not None) for r in pos_dps
    )
    print(f"   dps_declared_ttm > 0: {len(pos_dps)} rows; non-NULL members: {dict(sorted(dps_n.items()))}")
    paid = [r for r in ttm if r["dividends_paid_ttm"] is not None and r["dividends_paid_ttm"] != 0]
    paid_n = Counter(
        sum(1 for q in windows.get(r["instrument_id"], []) if q["dividends_paid"] is not None) for r in paid
    )
    print(f"   dividends_paid_ttm <> 0: {len(paid)} rows; non-NULL members: {dict(sorted(paid_n.items()))}")


if __name__ == "__main__":
    main()
