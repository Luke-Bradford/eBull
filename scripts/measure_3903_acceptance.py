"""#3903 acceptance: ``instrument_valuation.pe_ratio`` as market cap / TTM net income (sql/506).

Spec: ``docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md`` ("Acceptance"). The arms are #3850's: each is a
TEMPORARY view named ``instrument_valuation`` shadowing the deployed one for this connection only; the control is
sql/505's body (``sql/rollback/506_…``), the treatment sql/506's. Both are read in one REPEATABLE READ transaction
that is rolled back: nothing is written.

Prints:

1. wiring over the full population: the row set; every published ``pe_ratio`` against ``market_cap_live /
   net_income_ttm`` in ``Decimal``; every NULL decision against the construction's guards; every other column
   against the control;
2. the A/B of ``pe_ratio``: kept, gained by today's cause, lost by cause;
3. the scorer, both arms (its bulk load, overlay and ``_score_from_data``, no persistence): value-family score changes
   by path and fallback components, transitions into and out of the no-input result, and rank moves attributed to the
   instrument's own P/E input or to peers;
4. independent values: BKNG and APH expected NULL; for each name in ``REFERENCE`` the exact identity
   ours ÷ external = (V/P) × (N/A) × (A·E/I), with A the window's mean weighted diluted share count read from the
   local SEC ``companyfacts.zip`` (the build's Amendment 1: the external TTM EPS is built on it), and the spec's pass
   rule; then the treatment-input check over the flagged population;
5. latency of the per-instrument and bulk reads, both arms.

Exits non-zero when a wiring check, an own-input attribution, an expected NULL, the identity, the reference rule or
the 2x latency bound fails. The treatment-input check lists defect candidates and does not fail (spec item 4).

    PYTHONPATH=. uv run python scripts/measure_3903_acceptance.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.security.master_key import resolve_data_dir
from scripts.measure_3850_acceptance import REL_TOL, _best, _close, _dec, _ranks, _score, view_body
from scripts.measure_3903_ib_deviation import NI, ni_star_ttm, quarter_value
from scripts.measure_3903_premise import _WINDOW, inventory_cause

_SQL: Final = Path(__file__).resolve().parents[1] / "sql"
MIGRATION: Final = _SQL / "506_instrument_valuation_pe_total_form.sql"
ROLLBACK: Final = _SQL / "rollback" / "506_instrument_valuation_pe_total_form.sql"
LATENCY_BOUND: Final = 2.0
BAND: Final = (Decimal("0.8"), Decimal("1.25"))
FACTOR_BAND: Final = (Decimal("0.85"), Decimal("1.15"))
REFERENCE_TOLERANCE: Final = Decimal("0.15")
#: Spec acceptance item 4: each expected-NULL name with the cause the spec gives for it.
EXPECTED_NULL: Final[dict[str, Callable[[dict[str, Any]], bool]]] = {
    "BKNG": lambda t: t["net_income_ttm"] is None and t["market_cap_reason"] is None,
    "APH": lambda t: t["market_cap_reason"] is not None,
}
DILUTED: Final = "WeightedAverageNumberOfDilutedSharesOutstanding"

AVAILABLE_TO_COMMON: Final = "NetIncomeLossAvailableToCommonStockholdersBasic"

#: Spec acceptance item 4: (external price, its date, external P/E (TTM)) from the stockanalysis.com statistics pages,
#: read 2026-10-19 between 15:18 and 15:36 EDT (intraday quotes, market open). The page rounds EPS to cents, so E is
#: taken as price / P/E (both as shown) rather than the rounded EPS; for AVO (EPS $0.02) the two differ by 10%.
#: The sample is the panel; NFLX, ORLY and TPL (split windows); ALB (numerator); and the five untagged rows with the
#: largest |log(today / proposed)| as of 2026-10-19 (section 4 prints today's five beside them).
REFERENCE_SOURCE: Final = "stockanalysis.com/stocks/<ticker>/statistics/, read 2026-10-19 15:18-15:36 EDT"
_READ: Final = date(2026, 10, 19)
REFERENCE: Final[dict[str, tuple[Decimal, date, Decimal]]] = {
    symbol: (Decimal(price), _READ, Decimal(pe))
    for symbol, price, pe in (
        ("AAPL", "342.61", "39.30"),
        ("GME", "26.27", "17.23"),
        ("MSFT", "534.31", "29.77"),
        ("JPM", "333.75", "14.27"),
        ("HD", "310.01", "21.66"),
        ("NFLX", "69.77", "22.22"),
        ("ORLY", "88.77", "28.22"),
        ("TPL", "344.47", "43.89"),
        ("ALB", "94.76", "194.40"),
        ("TOON", "0.5000", "3.70"),
        ("SCHL", "39.54", "15.72"),
        ("AVO", "11.80", "544.37"),
        ("EIG", "51.42", "142.11"),
        ("OSS", "6.09", "117.48"),
    )
}

_ONE_SQL = (
    "SELECT pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield, debt_equity_ratio, market_cap_live, current_price, fcf_ttm "
    "FROM instrument_valuation WHERE instrument_id = %(id)s"
)
_BULK_SQL = (
    "SELECT instrument_id, pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield, debt_equity_ratio, market_cap_live, "
    "current_price, fcf_ttm FROM instrument_valuation WHERE instrument_id = ANY(%(ids)s::bigint[])"
)
_ELIGIBLE_SQL = """
    SELECT DISTINCT i.instrument_id
    FROM instruments i
    JOIN coverage c ON c.instrument_id = i.instrument_id
    WHERE i.is_tradable = TRUE
      AND c.filings_status = 'analysable'
      AND (
          EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
          OR EXISTS (SELECT 1 FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id)
          OR EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
      )
    ORDER BY i.instrument_id
"""
_CIK_SQL = (
    "SELECT instrument_id, lpad(identifier_value, 10, '0') FROM external_identifiers "
    "WHERE provider = 'sec' AND identifier_type = 'cik' AND is_primary"
)


def expected_pe(t: dict[str, Any]) -> Decimal | None:
    """Spec "Construction" item 1–2 from the treatment row's own operands."""
    cap, ni = _dec(t["market_cap_live"]), _dec(t["net_income_ttm"])
    if t["is_complete_ttm"] is not True or cap is None or ni is None or ni.is_nan() or not ni.is_finite() or ni <= 0:
        return None
    return cap / ni


def lost_cause(t: dict[str, Any]) -> str:
    if t["is_complete_ttm"] is not True:
        return "legacy arm"
    if t["market_cap_reason"] is not None:
        return f"cap:{t['market_cap_reason']}"
    if t["net_income_ttm"] is None:
        return "net_income_ttm NULL"
    return "net_income_ttm <= 0 or non-finite"


def _arm(conn: psycopg.Connection[Any], body: str, *, now: datetime, eligible: list[int], probe: int) -> dict[str, Any]:
    conn.execute("DROP VIEW IF EXISTS pg_temp.instrument_valuation")
    conn.execute("CREATE TEMP VIEW instrument_valuation AS " + body)  # type: ignore[arg-type]  # a reviewed file
    with conn.cursor(row_factory=dict_row) as cur:
        rows = {int(r["instrument_id"]): r for r in cur.execute("SELECT * FROM instrument_valuation").fetchall()}
    latency = (_best(conn, _ONE_SQL, {"id": probe}, 5), _best(conn, _BULK_SQL, {"ids": eligible}, 3))
    scored, bulk = _score(conn, eligible, now)
    return {"rows": rows, "latency": latency, "scored": scored, "bulk": bulk}


def _facts(z: zipfile.ZipFile, cik10: str) -> dict[str, Any]:
    try:
        return json.loads(z.read(f"CIK{cik10}.json")).get("facts", {})
    except KeyError:
        return {}


def _units(facts: dict[str, Any], taxonomy: str, concept: str, unit: str) -> list[dict[str, Any]]:
    return facts.get(taxonomy, {}).get(concept, {}).get("units", {}).get(unit, [])


def companyfacts_ttm(facts: dict[str, Any], concept: str, ends: list[date]) -> Decimal | None:
    """``concept`` (USD) summed over the window's quarters by the IB-deviation script's rule; None if unresolved."""
    values = [quarter_value(_units(facts, "us-gaap", concept, "USD"), e) for e in ends]
    present = [v for v in values if v is not None]
    return sum(present, Decimal(0)) if len(present) == len(values) else None


def _days(f: dict[str, Any]) -> int:
    return (date.fromisoformat(f["end"]) - date.fromisoformat(f["start"])).days


def _latest_fact(facts: list[dict[str, Any]]) -> dict[str, Any] | None:
    return max(facts, key=lambda f: (f["filed"], f["accn"])) if facts else None


def diluted_shares(facts: dict[str, Any], end: date) -> tuple[Decimal, str] | None:
    """The filing's weighted diluted share count for the quarter ending ``end``, and how it was read.

    The 80–100 day context when filed. Else, for a fiscal Q4, the duration-weighted difference of the fiscal-year and
    nine-month averages, (FY × d_FY − 9M × d_9M) / (d_FY − d_9M): a weighted average is additive over time only when
    weighted by duration (spec Amendment 1). It is an approximation, because ASC 260 tests each period's dilution on
    its own. None when neither resolves.
    """
    units = [f for f in _units(facts, "us-gaap", DILUTED, "shares") if f.get("start")]
    at_end = [f for f in units if f["end"] == end.isoformat()]
    quarter = _latest_fact([f for f in at_end if 80 <= _days(f) <= 100])
    if quarter is not None:
        return Decimal(str(quarter["val"])), "3m"
    fy = _latest_fact([f for f in at_end if 350 <= _days(f) <= 380])
    if fy is None:
        return None
    nine = _latest_fact([f for f in units if f["start"] == fy["start"] and 260 <= _days(f) <= 285])
    if nine is None:
        return None
    d_fy, d_nine = _days(fy), _days(nine)
    value = (Decimal(str(fy["val"])) * d_fy - Decimal(str(nine["val"])) * d_nine) / (d_fy - d_nine)
    return value, "FY-9M duration-weighted"


def filed_count(facts: dict[str, Any], pit: dict[str, Any]) -> Decimal | None:
    """The filing fact the PIT row's count claims: the cover (dei) or balance-sheet (us-gaap) count at its context
    date in its accession."""
    taxonomy, concept = (
        ("dei", "EntityCommonStockSharesOutstanding")
        if pit.get("shares_concept") == "cover"
        else ("us-gaap", "CommonStockSharesOutstanding")
    )
    hits = [
        f
        for f in _units(facts, taxonomy, concept, "shares")
        if f["end"] == pit.get("context_date") and f["accn"] == pit.get("accession")
    ]
    fact = _latest_fact(hits)
    return None if fact is None else Decimal(str(fact["val"]))


def _reference_row(
    z: zipfile.ZipFile,
    symbol: str,
    price: Decimal,
    price_date: date,
    pe_external: Decimal,
    by_symbol: dict[str | None, int],
    treat: dict[int, dict[str, Any]],
    windows: dict[int, list[dict[str, Any]]],
    ciks: dict[int, str],
) -> list[str]:
    """Spec acceptance item 4 (Amendment 1) for one reference name; returns its failures."""
    iid = by_symbol.get(symbol)
    if iid is None or treat[iid]["pe_ratio"] is None:
        print(f"   {symbol}: no published P/E")
        return [f"reference {symbol} not published"]
    t = treat[iid]
    v, cap, ni = _dec(t["valuation_price"]), _dec(t["market_cap_live"]), _dec(t["net_income_ttm"])
    assert v is not None and cap is not None and ni is not None
    pit: dict[str, Any] = t["market_cap_pit"] or {}
    eps = price / pe_external
    n = cap / v
    ends = [q["period_end_date"] for q in windows.get(iid, [])]
    facts = _facts(z, ciks.get(iid, ""))
    counts = [diluted_shares(facts, e) for e in ends]
    ratio = (cap / ni) / pe_external
    print(
        f"   {symbol}: ours {cap / ni:.3f} (session {t['session']}, V {v}, N {n:.0f}, I {ni}) | external "
        f"{pe_external} ({price_date}, P {price}, E = P / PE {eps:.6f})"
    )
    if not counts or any(c is None for c in counts):
        print("      a window quarter's weighted diluted count is unresolved:", list(zip(ends, counts, strict=True)))
        return [f"reference {symbol}: A unresolved"]
    resolved = [c for c in counts if c is not None]
    big_a = sum((c for c, _ in resolved), Decimal(0)) / len(resolved)
    if big_a <= 0:
        return [f"reference {symbol}: A not positive"]
    print(
        "      window diluted counts",
        [(str(e), f"{c:.0f}", how) for e, (c, how) in zip(ends, resolved, strict=True)],
        f"A {big_a:.0f}; external implied count I / E {ni / eps:.0f}",
    )
    price_f, count_f, earn_f = v / price, n / big_a, big_a * eps / ni
    identity = price_f * count_f * earn_f
    print(
        f"      ratio {ratio:.4f} = price {price_f:.4f} x count {count_f:.4f} x earnings {earn_f:.4f} "
        f"(identity {identity:.4f})"
    )
    failures: list[str] = []
    if not _close(identity, ratio):
        failures.append(f"identity {symbol}")
    # A reference row's own treatment input: stored I against the filing; a mismatch passes only when the external
    # earnings corroborate I (the earnings factor inside the band).
    filed_ni = companyfacts_ttm(facts, NI, ends)
    if filed_ni is not None and abs(ni - filed_ni) > Decimal("0.005") * max(abs(filed_ni), Decimal(1)):
        corroborated = FACTOR_BAND[0] <= earn_f <= FACTOR_BAND[1]
        print(
            f"      stored I {ni} vs companyfacts {filed_ni}: {'corroborated externally' if corroborated else 'FAIL'}"
        )
        if not corroborated:
            failures.append(f"reference {symbol}: treatment input I unreconciled")
    if abs(ratio - 1) <= REFERENCE_TOLERANCE:
        return failures
    # A miss: every factor outside the band must be accounted for by a filing figure, and the residual after dividing
    # the accounted adjustments out must be inside the tolerance.
    adjustment = Decimal(1)
    notes: list[str] = []
    unaccounted: list[str] = []
    if not FACTOR_BAND[0] <= price_f <= FACTOR_BAND[1]:
        unaccounted.append("price (a date mismatch is never divided out)")
    if not FACTOR_BAND[0] <= count_f <= FACTOR_BAND[1]:
        filed = filed_count(facts, pit)
        reported = _dec(pit.get("shares_reported"))
        split = _dec(pit.get("split_factor"))
        valid = (
            filed is not None
            and reported is not None
            and split == 1
            and abs(reported - filed) <= Decimal("0.005") * filed
        )
        if valid:
            adjustment *= count_f
            notes.append(f"count: N {n:.0f} is the filed {pit.get('shares_concept')} count {filed} (split 1) vs A")
        else:
            unaccounted.append(f"count (filed {filed}, reported {reported}, split {split})")
    if not FACTOR_BAND[0] <= earn_f <= FACTOR_BAND[1]:
        common = companyfacts_ttm(facts, AVAILABLE_TO_COMMON, ends)
        if common is not None and common > 0 and FACTOR_BAND[0] <= big_a * eps / common <= FACTOR_BAND[1]:
            adjustment *= common / ni
            notes.append(
                f"earnings: C/I divided out (C {AVAILABLE_TO_COMMON} {common}); A·E/C {big_a * eps / common:.4f}"
            )
        else:
            unaccounted.append(f"earnings ({AVAILABLE_TO_COMMON} {common})")
    residual = ratio / adjustment
    ok = not unaccounted and abs(residual - 1) <= REFERENCE_TOLERANCE
    print(
        f"      miss {ratio - 1:+.1%}; accounted: {notes or 'none'}; unaccounted: {unaccounted or 'none'}; "
        f"residual {residual:.4f} -> {'accounted' if ok else 'FAIL'}"
    )
    if not ok:
        failures.append(f"reference {symbol} miss unaccounted")
    return failures


def main() -> int:
    failures: list[str] = []
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
        symbols: dict[int, str] = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
        ciks: dict[int, str] = dict(conn.execute(_CIK_SQL).fetchall())
        eligible = [int(r[0]) for r in conn.execute(_ELIGIBLE_SQL).fetchall()]
        probe = conn.execute(
            "SELECT instrument_id FROM instruments WHERE symbol = 'AAPL' ORDER BY is_primary_listing DESC, "
            "instrument_id LIMIT 1"
        ).fetchone()
        now = datetime.now(UTC)
        arms = {
            name: _arm(conn, view_body(path), now=now, eligible=eligible, probe=probe[0] if probe else 0)
            for name, path in (("control", ROLLBACK), ("treatment", MIGRATION))
        }
        windows: dict[int, list[dict[str, Any]]] = {}
        with conn.cursor(row_factory=dict_row) as cur:
            for q in cur.execute(_WINDOW).fetchall():
                windows.setdefault(int(q["instrument_id"]), []).append(q)
        conn.rollback()
    control, treat = arms["control"]["rows"], arms["treatment"]["rows"]
    scored_c, bulk_c = arms["control"]["scored"], arms["control"]["bulk"]
    scored_t, bulk_t = arms["treatment"]["scored"], arms["treatment"]["bulk"]

    # 1. wiring
    print("\n1. wiring")
    print("   rows control", len(control), "treatment", len(treat), "| same id set", set(control) == set(treat))
    if set(control) != set(treat):
        failures.append("row set differs")
    bad_value = bad_null = bad_other = 0
    for iid, t in treat.items():
        want = expected_pe(t)
        if (want is None) != (t["pe_ratio"] is None):
            bad_null += 1
            if bad_null <= 5:
                print("   NULL decision mismatch", symbols.get(iid), t["pe_ratio"], want)
        elif want is not None and not _close(_dec(t["pe_ratio"]), want):
            bad_value += 1
            if bad_value <= 5:
                print("   value mismatch", symbols.get(iid), t["pe_ratio"], want)
        c = control.get(iid)
        if c is not None:
            for col, value in c.items():
                if col != "pe_ratio" and value != t[col]:
                    bad_other += 1
                    if bad_other <= 5:
                        print("   unchanged-column mismatch", symbols.get(iid), col, value, t[col])
    for name, n in (
        ("published values off the formula", bad_value),
        ("NULL decisions off the guards", bad_null),
        ("other columns differing from control", bad_other),
    ):
        print("  ", name, n)
        if n:
            failures.append(name)
    print(f"   relative tolerance {REL_TOL}")

    # 2. A/B
    print("\n2. A/B of pe_ratio")
    ab: Counter[str] = Counter()
    for iid, t in treat.items():
        old, new = control[iid]["pe_ratio"], t["pe_ratio"]
        if old is not None and new is not None:
            ab["kept"] += 1
        elif new is not None:
            ab["gained"] += 1
        elif old is not None:
            ab[f"lost, {lost_cause(t)}"] += 1
    for k, n in sorted(ab.items()):
        print(f"   {k}: {n}")
    both = {
        iid: _dec(control[iid]["pe_ratio"]) / _dec(t["pe_ratio"])  # type: ignore[operator]
        for iid, t in treat.items()
        if control[iid]["pe_ratio"] is not None and t["pe_ratio"] is not None
    }
    out = [iid for iid, x in both.items() if not BAND[0] <= x <= BAND[1]]
    tags = Counter(
        inventory_cause({**treat[iid], "pe_ratio": control[iid]["pe_ratio"]}, windows.get(iid, [])) for iid in out
    )
    print(f"   today / proposed outside [0.8, 1.25]: {len(out)} of {len(both)}; tags {dict(tags.most_common())}")

    # 3. scorer
    print("\n3. scorer (no persistence)")
    dropped, added = scored_c.keys() - scored_t.keys(), scored_t.keys() - scored_c.keys()
    print("   scored", len(scored_c), "->", len(scored_t), "| dropped", len(dropped), "| added", len(added))
    if dropped or added:
        failures.append("scorer result set differs")
    segments: Counter[str] = Counter()
    for iid in scored_c.keys() & scored_t.keys():
        vc_score, vt_score = scored_c[iid].family_scores.value, scored_t[iid].family_scores.value
        if vc_score == vt_score:
            continue
        data = bulk_t.get(iid) or {}
        thesis = data.get("thesis_row")
        path = "primary" if thesis is not None and thesis["base_value"] is not None else "fallback"
        vrow_c = (bulk_c.get(iid) or {}).get("valuation_row") or {}
        vrow_t = data.get("valuation_row") or {}
        comps = "+".join(
            k
            for k, present in (("pe", vrow_t.get("pe_ratio") is not None), ("fcf", vrow_t.get("fcf_yield") is not None))
            if present
        )
        miss_c = (scored_c[iid].family_usability or {}).get("value") == "missing"
        miss_t = (scored_t[iid].family_usability or {}).get("value") == "missing"
        transition = "into no-input" if miss_t and not miss_c else "out of no-input" if miss_c and not miss_t else ""
        pe_moved = (
            "pe gained"
            if vrow_c.get("pe_ratio") is None
            else "pe lost"
            if vrow_t.get("pe_ratio") is None
            else "pe changed"
        )
        segments[f"{path} | {pe_moved} | treatment comps {comps or 'none'} {transition}".strip()] += 1
    for k, n in segments.most_common():
        print(f"   value-family change: {k}: {n}")
    rc, rt = _ranks(scored_c), _ranks(scored_t)
    attributed: Counter[str] = Counter()
    unexplained: list[str] = []
    for iid in rc.keys() & rt.keys():
        if rc[iid] == rt[iid] and scored_c[iid].total_score == scored_t[iid].total_score:
            continue
        if scored_c[iid].total_score == scored_t[iid].total_score:
            attributed["peers"] += 1
            continue
        vc = (bulk_c.get(iid) or {}).get("valuation_row") or {}
        vt = (bulk_t.get(iid) or {}).get("valuation_row") or {}
        if vc.get("pe_ratio") != vt.get("pe_ratio"):
            attributed["own P/E input"] += 1
        else:
            unexplained.append(symbols.get(iid, str(iid)))
    print(
        "   rank moves", dict(attributed.most_common()), "| own-score moves without an own P/E change", len(unexplained)
    )
    if unexplained:
        failures.append("unexplained own-input rank moves")
        print("   ", unexplained[:20])

    # 4. independent values
    print("\n4. independent values")
    by_symbol = {symbols.get(iid): iid for iid in treat}
    for symbol, cause in EXPECTED_NULL.items():
        iid = by_symbol.get(symbol)
        if iid is None:
            print(f"   expected NULL {symbol}: no view row")
            failures.append(f"{symbol} expected NULL: no row")
            continue
        t = treat[iid]
        ok = t["pe_ratio"] is None and cause(t)
        print(
            f"   expected NULL {symbol}: pe_ratio {t['pe_ratio']} net_income_ttm {t['net_income_ttm']} "
            f"reason {t['market_cap_reason']} -> {'ok' if ok else 'FAIL'}"
        )
        if not ok:
            failures.append(f"{symbol} expected NULL")
    path = resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip"
    print(f"   reference: {REFERENCE_SOURCE}; filing operands: {path}")
    untagged = sorted(
        (
            iid
            for iid in out
            if inventory_cause({**treat[iid], "pe_ratio": control[iid]["pe_ratio"]}, windows.get(iid, [])) == "untagged"
        ),
        key=lambda i: abs(float(both[i].ln())),
        reverse=True,
    )[:5]
    top5 = [str(symbols.get(i)) for i in untagged]
    print("   the five untagged rows with the largest |log(today / proposed)|:", top5)
    if missing := [s_ for s_ in top5 if s_ not in REFERENCE]:
        failures.append(f"untagged top five without a reference: {missing}")
    with zipfile.ZipFile(path) as z:
        for symbol, (price, price_date, pe_external) in REFERENCE.items():
            failures.extend(_reference_row(z, symbol, price, price_date, pe_external, by_symbol, treat, windows, ciks))

        # Treatment-input check over the flagged population: the band outliers and the NI* <= 0 < net_income_ttm rows.
        print("   treatment-input check (stored net_income_ttm vs companyfacts NetIncomeLoss TTM, > 0.5%):")
        flagged: dict[int, str] = {iid: "band outlier" for iid in out}
        unresolved: Counter[str] = Counter()
        for iid, t in treat.items():
            if t["pe_ratio"] is None or iid in flagged:
                continue
            ends = [q["period_end_date"] for q in windows.get(iid, [])]
            star = ni_star_ttm(_facts(z, ciks.get(iid, "")).get("us-gaap", {}), ends) if ends else None
            if star is None:
                unresolved["NI* unresolved (no IB or NI in a quarter, or no window)"] += 1
            elif star[0] <= 0:
                flagged[iid] = "NI* <= 0 < net_income_ttm"
        candidates: list[str] = []
        for iid, why in sorted(flagged.items()):
            ends = [q["period_end_date"] for q in windows.get(iid, [])]
            stored = _dec(treat[iid]["net_income_ttm"])
            filed = companyfacts_ttm(_facts(z, ciks.get(iid, "")), NI, ends) if ends else None
            if stored is None or filed is None:
                print(f"      unresolved {symbols.get(iid)} ({why}): stored {stored}, companyfacts {filed}")
                continue
            if abs(stored - filed) > Decimal("0.005") * max(abs(filed), Decimal(1)):
                candidates.append(f"{symbols.get(iid)} ({why}) stored {stored} companyfacts {filed}")
        print(
            f"   flagged {len(flagged)} ({dict(Counter(flagged.values()))}); NI* not resolved for "
            f"{sum(unresolved.values())} published rows outside the outliers; defect candidates {len(candidates)}"
        )
        for c in candidates:
            print("     ", c)

    # 5. latency
    print("\n5. latency (best of n)")
    lat_c, lat_t = arms["control"]["latency"], arms["treatment"]["latency"]
    for name, c_s, t_s in (("per-instrument", lat_c[0], lat_t[0]), ("compute_rankings bulk read", lat_c[1], lat_t[1])):
        ratio_l = t_s / c_s if c_s else float("inf")
        print(f"   {name}: control {c_s * 1000:.1f} ms, treatment {t_s * 1000:.1f} ms, ratio {ratio_l:.2f}")
        if ratio_l > LATENCY_BOUND:
            failures.append(f"latency {name}")

    print("\nRESULT:", "PASS" if not failures else "FAIL " + "; ".join(failures))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
