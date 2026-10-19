"""#3903 dividend-yield premise: ``instrument_valuation.dividend_yield`` (TTM DPS ÷ ``current_price``) against the
total/ME form (TTM dividends paid ÷ ``market_cap_live``).

Read-only; every database read shares one REPEATABLE READ snapshot. Filing facts that ``financial_facts_raw`` does
not hold (it stores tracked concepts only) are read from the local SEC ``companyfacts.zip``, whose path, size and
SHA-256 file are printed. "The window" is sql/220's: the latest four deduplicated normalized quarters, adjacent
within 330 days. "Published" means a complete-TTM row with ``market_cap_live`` set.

Prints:

1. populations: today's published yield against the total form's, over complete-TTM rows;
2. sign of the stored ``dividends_paid`` members;
3. ``dividends_paid`` member coverage in windows with a nonzero member, and every NULL member classified against
   ``financial_facts_raw`` (``classify_null_member``): the quarter's value derivable from the filed facts and
   nonzero (a missed fact), derivable and zero, or not derivable;
4. numerator composition from ``companyfacts.zip``: the concept that supplies ``dividends_paid`` on each
   paid-active window's latest annual fact, and whether the same accession files a preferred or
   noncontrolling-interest dividend concept, with the taxonomy's own description of each concept;
5. windows with a DPS but no ``dividends_paid`` member: the cash-flow dividend concepts the issuer files in
   ``companyfacts.zip`` that sql's tracked list does not read;
6. DPS windows that straddle a share-basis change (weighted diluted shares spread >= 1.5x, the P/E inventory's
   predicate), and today's yield ÷ the total form's in buckets, factored as (V/P) x (DPS x N / D); every row outside
   [0.8, 1.25] tagged with its first matching candidate cause (``inventory_cause``);
7. the proxy's deviation: stored ``dividends_paid_ttm`` ÷ the common-only TTM rebuilt from ``companyfacts.zip``
   (``common_ttm``);
8. named cases;
9. 8-K ``dividend_events`` coverage of the paid-active published instruments (an ex-date in the last 365 days).

    PYTHONPATH=. uv run python scripts/measure_3903_dividend_premise.py
"""

from __future__ import annotations

import json
import subprocess
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.providers.implementations.sec_fundamentals import TRACKED_CONCEPTS
from app.security.master_key import resolve_data_dir

PAID = tuple(TRACKED_CONCEPTS["dividends_paid"])
#: Dividend-payment concepts that are not to common holders. Names are us-gaap's; descriptions print from the zip.
NOT_COMMON = (
    "PaymentsOfDividendsPreferredStockAndPreferenceStock",
    "PaymentsOfDividendsMinorityInterest",
    "DividendsPreferredStock",
    "DividendsPreferredStockCash",
)
#: The inventory predicate shared with ``measure_3903_premise.SPREAD``.
SPREAD = Decimal("1.5")
BAND = (Decimal("0.8"), Decimal("1.25"))
NAMED = ("FAST", "IBKR", "AAPL", "MSFT", "JPM", "HD", "GME", "KO", "O", "T")
#: A fact "ends at" a quarter end within this many days (52/53-week calendars end on a weekday near month end).
END_SLACK = 3

_ROWS = """
    SELECT i.symbol, v.instrument_id, v.dividend_yield, v.current_price, v.valuation_price, v.market_cap_live,
           v.market_cap_reason, t.dps_declared_ttm, t.dividends_paid_ttm, lpad(ei.identifier_value, 10, '0') AS cik10
    FROM instrument_valuation v
    JOIN instruments i USING (instrument_id)
    JOIN financial_periods_ttm t USING (instrument_id)
    LEFT JOIN external_identifiers ei ON ei.instrument_id = v.instrument_id AND ei.provider = 'sec'
         AND ei.identifier_type = 'cik' AND ei.is_primary
    WHERE v.is_complete_ttm
"""

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
    SELECT r.instrument_id, r.period_start_date, r.period_end_date, r.period_type, r.shares_diluted, r.dps_declared,
           r.dividends_paid
    FROM r JOIN ok USING (instrument_id) WHERE r.rn <= 4
    ORDER BY r.instrument_id, r.period_end_date
"""

_EIGHT_K = """
    SELECT count(*) AS paid_active,
           count(*) FILTER (WHERE EXISTS (
               SELECT 1 FROM dividend_events e
               WHERE e.instrument_id = v.instrument_id AND e.ex_date >= current_date - 365
           )) AS with_ex_date
    FROM instrument_valuation v
    JOIN financial_periods_ttm t USING (instrument_id)
    WHERE v.is_complete_ttm AND v.market_cap_live IS NOT NULL AND t.dividends_paid_ttm > 0
"""

_FACTS = """
    SELECT instrument_id, concept, period_start, period_end, val, accession_number, filed_date
    FROM financial_facts_raw
    WHERE taxonomy = 'us-gaap' AND concept = ANY(%(c)s) AND instrument_id = ANY(%(i)s) AND period_start IS NOT NULL
"""


def _near(a: date, b: date) -> bool:
    return abs((a - b).days) <= END_SLACK


def classify_null_member(facts: list[dict[str, Any]], end: date, prev_end: date | None) -> str:
    """Classify a NULL ``dividends_paid`` member ending ``end`` from the issuer's tracked-concept facts.

    In concept priority order: a fact of 80-100 days ending at ``end`` is the quarter's value; otherwise a fact
    ending at ``end`` minus the same-concept, same-start fact ending at ``prev_end`` (a year-to-date difference).
    The latest-filed fact wins within a span. "missed" = derivable and nonzero; "zero" = derivable and zero;
    "underivable: ..." otherwise, split by whether any fact ends at ``end``."""
    any_ending = False
    for concept in PAID:
        cf = [f for f in facts if f["concept"] == concept]
        ending = sorted(
            (f for f in cf if _near(f["period_end"], end)), key=lambda f: (f["filed_date"], f["accession_number"])
        )
        if not ending:
            continue
        any_ending = True
        quarterly = [f for f in ending if 80 <= (f["period_end"] - f["period_start"]).days <= 100]
        if quarterly:
            return "missed" if quarterly[-1]["val"] != 0 else "zero"
        if prev_end is None:
            continue
        for f in reversed(ending):
            prior = sorted(
                (g for g in cf if g["period_start"] == f["period_start"] and _near(g["period_end"], prev_end)),
                key=lambda g: (g["filed_date"], g["accession_number"]),
            )
            if prior:
                return "missed" if f["val"] - prior[-1]["val"] != 0 else "zero"
            if f["period_start"] > prev_end:  # a fact starting after the prior quarter is this quarter alone
                return "missed" if f["val"] != 0 else "zero"
    return "underivable: a fact ends at the quarter end" if any_ending else "underivable: no fact ends there"


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


def total_yield(r: dict[str, Any]) -> Decimal | None:
    d, cap = r["dividends_paid_ttm"], r["market_cap_live"]
    if d is None or d == 0 or cap is None or cap <= 0:
        return None
    return abs(d) / cap * 100


def inventory_cause(
    window: list[dict[str, Any]], straddles: bool, stored_vs_common: tuple[Decimal, str] | None, long_span: bool
) -> str:
    """The first candidate cause, in this precedence, for a row whose two yields disagree. A tag is a candidate, not
    a verified cause: the predicates overlap and the order sets the counts."""
    n_dps = sum(1 for q in window if q["dps_declared"] is not None)
    n_paid = sum(1 for q in window if q["dividends_paid"] is not None)
    dps = [q["dps_declared"] for q in window if q["dps_declared"] is not None]
    if straddles:
        return "1 diluted shares spread >= 1.5x (share-basis change)"
    if any(d < 0 for d in dps):
        return "1a a negative DPS member (a derived Q4)"
    positive = [d for d in dps if d > 0]
    if positive and max(positive) > 50 * min(positive):
        return "1b a DPS member over 50x the window's smallest (a scale error)"
    if n_dps < n_paid:
        return "2 fewer DPS members than paid members"
    if n_paid < n_dps:
        return "3 fewer paid members than DPS members"
    if stored_vs_common is not None and not Decimal("0.95") <= stored_vs_common[0] <= Decimal("1.05"):
        return "4 stored paid TTM outside 5% of the common-only TTM"
    if long_span:
        return "5 window span over 365 days"
    return "untagged"


def _usd_facts(gaap: dict[str, Any], concept: str) -> list[dict[str, Any]]:
    return gaap.get(concept, {}).get("units", {}).get("USD", [])


def _annual(facts: list[dict[str, Any]], since: date) -> list[dict[str, Any]]:
    out = []
    for f in facts:
        if not f.get("start"):
            continue
        s, e = date.fromisoformat(f["start"]), date.fromisoformat(f["end"])
        if 350 <= (e - s).days <= 380 and e >= since:
            out.append(f)
    return out


def _quarter(facts: list[dict[str, Any]], end: date) -> Decimal | None:
    """One quarter's flow ending ``end`` from companyfacts duration facts: an 80-100 day fact, else a fact ending at
    ``end`` minus the same-start fact ending 80-100 days earlier (a year-to-date difference). Latest filed wins."""
    ending = sorted(
        (f for f in facts if f.get("start") and _near(date.fromisoformat(f["end"]), end)),
        key=lambda f: (f["filed"], f["accn"]),
    )
    for f in reversed(ending):
        if 80 <= (date.fromisoformat(f["end"]) - date.fromisoformat(f["start"])).days <= 100:
            return Decimal(str(f["val"]))
    for f in reversed(ending):
        prior = sorted(
            (
                g
                for g in facts
                if g.get("start") == f["start"] and 80 <= (end - date.fromisoformat(g["end"])).days <= 100
            ),
            key=lambda g: (g["filed"], g["accn"]),
        )
        if prior:
            return Decimal(str(f["val"])) - Decimal(str(prior[-1]["val"]))
    return None


def common_ttm(gaap: dict[str, Any], ends: list[date]) -> tuple[Decimal, str] | None:
    """TTM cash dividends to common holders of the parent over the window's quarters, by source rule order:
    ``PaymentsOfDividendsCommonStock``; else the partnership per-unit-holder concepts; else ``PaymentsOfDividends``
    minus the filed preferred and noncontrolling-interest payment concepts (an unfiled one counts as zero). The
    branch is chosen per window by the first concept with a fact at any window quarter. A quarter with no fact in the
    branch is zero (the sporadic class). None when no branch has a fact."""
    for branch, concepts in (
        ("common", ("PaymentsOfDividendsCommonStock",)),
        ("partnership", PAID[2:]),
        ("generic less preferred and NCI", ("PaymentsOfDividends",)),
    ):
        for c in concepts:
            facts = _usd_facts(gaap, c)
            qs = [_quarter(facts, e) for e in ends]
            if all(q is None for q in qs):
                continue
            total = sum((q for q in qs if q is not None), Decimal(0))
            if c == "PaymentsOfDividends":
                for nc in NOT_COMMON[:2]:
                    total -= sum(
                        (q for q in (_quarter(_usd_facts(gaap, nc), e) for e in ends) if q is not None), Decimal(0)
                    )
            return total, branch
    return None


def main() -> None:
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    zpath = resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip"
    shafile = zpath.with_name(zpath.name + ".sha256")
    sha = shafile.read_text().strip()[:12] if shafile.exists() else None
    print(f"#3903 dividend premise; {datetime.now(UTC):%Y-%m-%d %H:%MZ}; code {head}")
    print(f"companyfacts: {zpath} ({zpath.stat().st_size} bytes, sha256 file {sha})")

    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(_ROWS).fetchall()
            win: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for q in cur.execute(_WINDOW).fetchall():
                win[q["instrument_id"]].append(q)
            ids = sorted(win)
            eight_k = cur.execute(_EIGHT_K).fetchone()
            facts: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for f in cur.execute(_FACTS, {"c": list(PAID), "i": ids}).fetchall():
                facts[f["instrument_id"]].append(f)
        conn.rollback()

    pub = [r for r in rows if r["market_cap_live"] is not None]
    sym_of = {r["instrument_id"]: r["symbol"] for r in rows}
    print(f"\ncomplete-TTM rows {len(rows)}; with a published cap {len(pub)}")

    # 1. populations
    print("\n1. today's dividend_yield against the total form (dividends_paid_ttm <> 0 over market_cap_live)")
    m: Counter[str] = Counter()
    for r in rows:
        today = r["dividend_yield"] is not None
        if r["market_cap_live"] is None:
            m[f"cap refused ({r['market_cap_reason']}); today {'published' if today else 'NULL'}"] += 1
            continue
        tot = total_yield(r) is not None
        m[f"today {'published' if today else 'NULL'}, total {'published' if tot else 'NULL'}"] += 1
    for k, v in sorted(m.items()):
        print(f"   {k}: {v}")
    zero_paid = sum(1 for r in pub if r["dividends_paid_ttm"] == 0)
    null_paid = sum(1 for r in pub if r["dividends_paid_ttm"] is None)
    print(f"   published-cap rows: dividends_paid_ttm = 0 {zero_paid}, NULL {null_paid}")

    # 2. sign
    signs: Counter[str] = Counter()
    for w in win.values():
        for q in w:
            v = q["dividends_paid"]
            signs["NULL" if v is None else ("positive" if v > 0 else ("zero" if v == 0 else "negative"))] += 1
    print(f"\n2. dividends_paid window members by sign: {dict(signs)}")
    pub_ids = {r["instrument_id"] for r in pub}
    neg_ttm = sorted(r["symbol"] for r in pub if r["dividends_paid_ttm"] is not None and r["dividends_paid_ttm"] < 0)
    neg_member = sorted(
        r["symbol"]
        for r in pub
        if r["dividends_paid_ttm"] is not None
        and r["dividends_paid_ttm"] > 0
        and any(q["dividends_paid"] is not None and q["dividends_paid"] < 0 for q in win.get(r["instrument_id"], []))
    )
    print(f"   published-cap rows with dividends_paid_ttm < 0: {len(neg_ttm)} {neg_ttm}")
    print(f"   with a positive TTM beside a negative member: {len(neg_member)} {neg_member}")
    spans = Counter()
    long_span = []
    for iid, w in win.items():
        if (
            iid not in pub_ids
            or not any(q["dividends_paid"] not in (None, 0) for q in w)
            or w[0]["period_start_date"] is None
        ):
            continue
        days = (w[-1]["period_end_date"] - w[0]["period_start_date"]).days + 1
        spans["> 371 days" if days > 371 else ("366-371 days" if days > 365 else "<= 365 days")] += 1
        if days > 365:
            long_span.append(sym_of[iid])
    print(f"   paid-active windows by span (first quarter's start to last quarter's end, inclusive): {dict(spans)}")
    print(f"   spans over 365 days: {sorted(long_span)}")

    # 3. coverage and NULL members
    active = {i: w for i, w in win.items() if i in pub_ids and any(q["dividends_paid"] not in (None, 0) for q in w)}
    nn = Counter(sum(1 for q in w if q["dividends_paid"] is not None) for w in active.values())
    print(
        f"\n3. published windows with a nonzero dividends_paid member: {len(active)}; "
        f"non-NULL members {dict(sorted(nn.items()))}"
    )
    cls: Counter[str] = Counter()
    win_cls: Counter[str] = Counter()
    missed_ids: list[int] = []
    for iid, w in active.items():
        kinds = []
        for k, q in enumerate(w):
            if q["dividends_paid"] is not None:
                continue
            prev = w[k - 1]["period_end_date"] if k else None
            if prev is None:
                prev = q["period_end_date"] - timedelta(days=91)
            kinds.append(classify_null_member(facts.get(iid, []), q["period_end_date"], prev))
        cls.update(kinds)
        if kinds:
            wk = (
                "a missed member"
                if "missed" in kinds
                else (
                    "underivable member(s)"
                    if any(k.startswith("underivable") for k in kinds)
                    else "all NULL members zero"
                )
            )
            win_cls[wk] += 1
            if wk == "a missed member":
                missed_ids.append(iid)
    print(f"   NULL members: {dict(cls)}")
    print(f"   windows with a NULL member: {dict(win_cls)}")
    sym = sym_of
    print(f"   windows with a missed member: {sorted(sym[i] for i in missed_ids)}")

    # 4-5. companyfacts
    by_id = {r["instrument_id"]: r for r in rows}
    composition: Counter[str] = Counter()
    not_common_rows: list[tuple[str, str, Decimal, Decimal]] = []
    untracked: Counter[str] = Counter()
    dps_only = [
        i
        for i, w in win.items()
        if i in pub_ids
        and any(q["dps_declared"] not in (None, 0) for q in w)
        and all(q["dividends_paid"] is None for q in w)
    ]
    descriptions: dict[str, str] = {}
    dev: Counter[str] = Counter()
    dev_branch: Counter[str] = Counter()
    dev_ratio: dict[str, tuple[Decimal, str]] = {}
    since = date(2024, 6, 1)
    with zipfile.ZipFile(zpath) as z:
        names = set(z.namelist())

        def gaap_of(iid: int) -> dict[str, Any] | None:
            cik = by_id[iid]["cik10"]
            member = f"CIK{cik}.json"
            if cik is None or member not in names:
                return None
            return json.loads(z.read(member)).get("facts", {}).get("us-gaap", {})

        for iid in sorted(active):
            gaap = gaap_of(iid)
            if gaap is None:
                composition["no companyfacts member"] += 1
                continue
            for c in (*PAID, *NOT_COMMON):
                if c in gaap and c not in descriptions:
                    descriptions[c] = gaap[c].get("description") or ""
            supplier = next((c for c in PAID if _annual(_usd_facts(gaap, c), since)), None)
            if supplier is None:
                composition["no annual tracked fact since 2024-06"] += 1
                continue
            latest = max(_annual(_usd_facts(gaap, supplier), since), key=lambda f: (f["end"], f["filed"]))
            composition[f"supplier {supplier}"] += 1
            for c in NOT_COMMON:
                same = [
                    f
                    for f in _usd_facts(gaap, c)
                    if f.get("accn") == latest["accn"]
                    and f.get("start") == latest["start"]
                    and f["end"] == latest["end"]
                ]
                if same and Decimal(str(same[-1]["val"])) != 0:
                    composition[f"supplier {supplier} + nonzero {c}"] += 1
                    if supplier == "PaymentsOfDividends":
                        not_common_rows.append(
                            (sym[iid], c, Decimal(str(latest["val"])), Decimal(str(same[-1]["val"])))
                        )
        for iid in sorted(active):
            gaap = gaap_of(iid)
            stored = by_id[iid]["dividends_paid_ttm"]
            if gaap is None:
                dev["no companyfacts member"] += 1
                continue
            ct = common_ttm(gaap, [q["period_end_date"] for q in win[iid]])
            if ct is None:
                dev["no fact in any branch"] += 1
                continue
            val, branch = ct
            dev_branch[branch] += 1
            if val <= 0:
                dev["common TTM <= 0"] += 1
                continue
            x = abs(stored) / val
            dev_ratio[sym[iid]] = (x, branch)
        for c in ("PaymentsOfOrdinaryDividends", "PaymentsOfCapitalDistribution"):
            for iid in dps_only:
                gaap = gaap_of(iid)
                if gaap and c in gaap:
                    descriptions[c] = gaap[c].get("description") or ""
                    break
        for iid in dps_only:
            gaap = gaap_of(iid)
            if gaap is None:
                untracked["no companyfacts member"] += 1
                continue
            found = sorted(
                c
                for c in gaap
                if ("Dividend" in c or "Distribution" in c)
                and ("Payment" in c or "Paid" in c)
                and c not in PAID
                and c not in NOT_COMMON
                and _annual(_usd_facts(gaap, c), since)
            )
            untracked[" + ".join(found) if found else "none"] += 1

    print(f"\n4. numerator composition over {len(active)} paid-active windows (latest annual fact since {since}):")
    for k, v in composition.most_common():
        print(f"   {k}: {v}")
    for s, c, tot, nc in sorted(not_common_rows):
        print(f"   {s}: {c} {nc} beside the supplier's {tot} (share {nc / tot if tot else 'n/a'})")
    print("   taxonomy descriptions (companyfacts 'description', verbatim):")
    for c, d in sorted(descriptions.items()):
        print(f"   - {c}: {d}")
    print(
        f"\n5. published windows with a DPS and no dividends_paid member: {len(dps_only)}; "
        "untracked paid concepts filed:"
    )
    for k, v in untracked.most_common():
        print(f"   {k}: {v}")

    # 6. split straddle and agreement
    straddle = []
    for iid, w in win.items():
        if iid not in pub_ids or not any(q["dps_declared"] not in (None, 0) for q in w):
            continue
        sh = [q["shares_diluted"] for q in w if q["shares_diluted"]]
        if len(sh) >= 2 and max(sh) / min(sh) >= SPREAD:
            straddle.append(sym[iid])
    print(f"\n6. DPS windows with weighted diluted shares spread >= {SPREAD}x: {len(straddle)} {sorted(straddle)}")
    both = [r for r in pub if r["dividend_yield"] is not None and total_yield(r) is not None]
    ratio: dict[str, tuple[Decimal, Decimal, Decimal]] = {}
    for r in both:
        ty = total_yield(r)
        assert ty is not None
        n = r["market_cap_live"] / r["valuation_price"]
        vp = r["valuation_price"] / r["current_price"]
        dnd = r["dps_declared_ttm"] * n / abs(r["dividends_paid_ttm"])
        ratio[r["symbol"]] = (r["dividend_yield"] / ty, vp, dnd)
    print(
        f"   rows publishing both: {len(both)}; today ÷ total: "
        f"{dict(sorted(Counter(bucket(x[0]) for x in ratio.values()).items()))}"
    )
    print(
        f"   price factor V/P outside [0.95,1.05]: "
        f"{sum(1 for x in ratio.values() if not Decimal('0.95') <= x[1] <= Decimal('1.05'))}"
    )
    print(f"   DPS x N / D: {dict(sorted(Counter(bucket(x[2]) for x in ratio.values()).items()))}")
    outside = sorted(((s, x) for s, x in ratio.items() if not BAND[0] <= x[0] <= BAND[1]), key=lambda t: t[1][0])
    id_of = {r["symbol"]: r["instrument_id"] for r in both}
    tags: Counter[str] = Counter()
    for s, (x, vp, dnd) in outside:
        tag = inventory_cause(win.get(id_of[s], []), s in straddle, dev_ratio.get(s), s in long_span)
        tags[tag] += 1
        print(f"   {s}: today/total {x:.3f}  V/P {vp:.3f}  DPS*N/D {dnd:.3f}  [{tag}]")
    print(f"   {len(outside)} outside [0.8,1.25] by first matching candidate cause: {dict(tags.most_common())}")

    # 7. the proxy's deviation
    print(f"\n7. stored dividends_paid_ttm ÷ companyfacts common-only TTM (``common_ttm``) over {len(active)} windows:")
    print(f"   unresolved: {dict(dev)}; branch: {dict(dev_branch)}")
    print(f"   all: {dict(sorted(Counter(bucket(x) for x, _ in dev_ratio.values()).items()))}")
    for b in dev_branch:
        print(f"   {b}: {dict(sorted(Counter(bucket(x) for x, bb in dev_ratio.values() if bb == b).items()))}")
    tight = sum(1 for x, _ in dev_ratio.values() if Decimal("0.95") <= x <= Decimal("1.05"))
    print(f"   within [0.95,1.05]: {tight} of {len(dev_ratio)}")
    for s, (x, b) in sorted(dev_ratio.items(), key=lambda t: t[1][0]):
        if not BAND[0] <= x <= BAND[1]:
            print(f"   outside band: {s} {x:.3f} ({b})")

    # 8. named
    print("\n8. named cases")
    for r in rows:
        if r["symbol"] in NAMED:
            print(
                f"   {r['symbol']}: yield {r['dividend_yield']} total {total_yield(r)} dps_ttm {r['dps_declared_ttm']} "
                f"paid_ttm {r['dividends_paid_ttm']} cap {r['market_cap_live']} reason {r['market_cap_reason']}"
            )
            for q in win.get(r["instrument_id"], []):
                print(
                    f"      {q['period_end_date']} {q['period_type']} dps {q['dps_declared']} "
                    f"paid {q['dividends_paid']} diluted {q['shares_diluted']}"
                )

    assert eight_k is not None
    print(
        f"\n9. paid-active published instruments: {eight_k['paid_active']}; with an 8-K ex-date in 365 days: "
        f"{eight_k['with_ex_date']}"
    )


if __name__ == "__main__":
    main()
