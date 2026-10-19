"""#3903 dividend-yield premise: ``instrument_valuation.dividend_yield`` (TTM DPS ÷ ``current_price``) against the
spec's total/ME form.

Spec: ``docs/specs/fundamentals/2026-10-19-3903-dividend-yield-total-form.md``. Read-only; every database read shares
one REPEATABLE READ snapshot. Filing facts that ``financial_facts_raw`` does not hold (it stores tracked concepts only)
are read from the local SEC ``companyfacts.zip``, whose path, size and SHA-256 file are printed. "The window" is
sql/220's: the latest four deduplicated normalized quarters, adjacent within 330 days.

"Proposed" (``proposed_yield``) is the spec's construction over stored columns: Σ of the window's four
``dividends_paid`` members ÷ ``market_cap_live`` × 100, when the cap is published, all four members are non-NULL and
none is negative; zero publishes as zero. Otherwise NULL, with the first failing condition as its cause.

Prints:

1. today's yield against the proposed one over every complete-TTM row, with each NULL's cause;
2. sign and member counts of the stored ``dividends_paid`` members;
3. every NULL member of an active window (one with a nonzero member) classified against ``financial_facts_raw``
   (``classify_member``), windows by category and cap eligibility, and each missed amount;
4. the rule's own check: ``priority_value`` (the normaliser's concept priority) against every stored non-NULL member
   of an active window or proposed publication, as of the member's filed_date, and the matching candidate concept;
5. the common-only TTM rebuilt per quarter from companyfacts (``common_ttm``) against the stored members, for active
   windows and for every proposed publication with each exclusion counted; both forms against the rebuilt yield; the
   rows where the proposal is further from it than today, with their member checks; every published window outside
   [0.95, 1.05]; the zero publications;
6. windows with a special distribution (``PaymentsOfCapitalDistribution``) in a window quarter;
7. rows today publishes and the proposal does not (published cap), by cause and untracked concept;
8. the discrepancy inventory over rows publishing a positive yield both ways, with the identity
   today ÷ proposed = (V/P) × (DPS × N / D) asserted per row (``inventory_cause``), the window span distribution and
   proposed publications with a member interval outside 80-100 days;
9. proposed publications whose stored price series starts under 365 days before the session;
10. 8-K ``dividend_events`` with an ex-date in the 365 days to today;
11. named cases;
12. the us-gaap descriptions of the concepts involved, verbatim from companyfacts.

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

#: The normaliser's priority for ``dividends_paid`` (first concept with a value wins).
PAID = tuple(TRACKED_CONCEPTS["dividends_paid"])
GENERIC = "PaymentsOfDividends"
COMMON = "PaymentsOfDividendsCommonStock"
PARTNERSHIP = tuple(c for c in PAID if c not in (GENERIC, COMMON))
#: Cash paid to holders other than the parent's common: subtracted from the generic concept when filed.
PAID_NOT_COMMON = ("PaymentsOfDividendsPreferredStockAndPreferenceStock", "PaymentsOfDividendsMinorityInterest")
#: Preferred dividends DECLARED (us-gaap: "paid and unpaid"): not a cash amount, so never subtracted; flagged.
DECLARED_PREFERRED = ("DividendsPreferredStock", "DividendsPreferredStockCash")
SPECIAL = "PaymentsOfCapitalDistribution"
UNTRACKED = ("PaymentsOfOrdinaryDividends", SPECIAL)
DESCRIBED = (*PAID, *PAID_NOT_COMMON, *DECLARED_PREFERRED, *UNTRACKED)
SPREAD = Decimal("1.5")
BAND = (Decimal("0.8"), Decimal("1.25"))
TIGHT = (Decimal("0.95"), Decimal("1.05"))
NAMED = ("AAPL", "GME", "MSFT", "JPM", "HD", "KO", "FAST", "O", "CEG", "PAYC", "EFC", "BJRI")
#: A fact's start or end matches a quarter boundary within this many days (52/53-week calendars).
SLACK = 3

_ROWS = """
    SELECT i.symbol, v.instrument_id, v.dividend_yield, v.current_price, v.valuation_price, v.market_cap_live,
           v.market_cap_reason, v.session, t.dps_declared_ttm, t.dividends_paid_ttm,
           lpad(ei.identifier_value, 10, '0') AS cik10,
           (SELECT min(p.price_date) FROM price_daily p WHERE p.instrument_id = v.instrument_id) AS first_bar
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
           r.dividends_paid, r.filed_date
    FROM r JOIN ok USING (instrument_id) WHERE r.rn <= 4
    ORDER BY r.instrument_id, r.period_end_date
"""

_EIGHT_K = """
    SELECT DISTINCT instrument_id FROM dividend_events
    WHERE ex_date > current_date - 365 AND ex_date <= current_date AND instrument_id = ANY(%(i)s)
"""

_FACTS = """
    SELECT instrument_id, concept, period_start, period_end, val, accession_number, filed_date
    FROM financial_facts_raw
    WHERE taxonomy = 'us-gaap' AND unit = 'USD' AND concept = ANY(%(c)s) AND instrument_id = ANY(%(i)s)
"""

#: A fact: start (None for an undated fact), end, val, filed, accn.
Fact = dict[str, Any]


def bounds(window: list[dict[str, Any]], counter: Counter[str]) -> list[tuple[date, date]]:
    """Each member's [start, end]. A NULL ``period_start_date`` takes the previous member's end + 1 day; the first
    member's, its end - 90 days. ``counter`` counts both substitutions."""
    out = []
    for k, q in enumerate(window):
        start = q["period_start_date"]
        if start is None:
            start = (
                window[k - 1]["period_end_date"] + timedelta(days=1) if k else q["period_end_date"] - timedelta(days=90)
            )
            counter["first member" if k == 0 else "later member"] += 1
        out.append((start, q["period_end_date"]))
    return out


def _near(a: date, b: date) -> bool:
    return abs((a - b).days) <= SLACK


def _latest(facts: list[Fact]) -> Fact | None:
    return max(facts, key=lambda f: (f["filed"], f["accn"])) if facts else None


def quarter_value(facts: list[Fact], qstart: date, qend: date) -> tuple[str, Decimal | None]:
    """One concept's flow for the quarter [``qstart``, ``qend``] (boundaries matched within ``SLACK`` days):
    ("value", v), ("unresolved", None) or ("no fact", None).

    1. The latest-filed dated fact spanning the quarter.
    2. Else a contemporaneous year-to-date difference: per start before ``qstart``, the latest-filed fact ending at
       ``qend`` minus the latest same-start fact ending the day before ``qstart`` FILED NO LATER than it (so a later
       restatement of the earlier period is never subtracted from an older cumulative). The newest cumulative wins.
    "unresolved" = some fact (dated or not) ends at ``qend`` and neither forms; "no fact" = none ends there."""
    ending = [f for f in facts if _near(f["end"], qend)]
    if not ending:
        return "no fact", None
    dated = [f for f in ending if f["start"] is not None]
    direct = _latest([f for f in dated if _near(f["start"], qstart)])
    if direct is not None:
        return "value", direct["val"]
    by_start: dict[date, list[Fact]] = defaultdict(list)
    for f in dated:
        if f["start"] < qstart - timedelta(days=SLACK):
            by_start[f["start"]].append(f)
    best: tuple[date, str, Decimal] | None = None
    for start, fs in by_start.items():
        cum = _latest(fs)
        assert cum is not None
        prior = _latest(
            [
                g
                for g in facts
                if g["start"] == start and _near(g["end"], qstart - timedelta(days=1)) and g["filed"] <= cum["filed"]
            ]
        )
        if prior is not None and (best is None or (cum["filed"], cum["accn"]) > (best[0], best[1])):
            best = (cum["filed"], cum["accn"], cum["val"] - prior["val"])
    if best is None:
        return "unresolved", None
    return "value", best[2]


def priority_value(
    by_concept: dict[str, list[Fact]], qstart: date, qend: date, as_of: date | None = None
) -> tuple[str, Decimal | None, str | None]:
    """The normaliser's priority applied to a quarter: the first concept in ``PAID`` with a value. ("unresolved", …)
    when none has one and any is unresolved; else ("no fact", …). ``as_of`` keeps only facts filed on or before it
    (the stored member's own vintage)."""
    unresolved = False
    for c in PAID:
        facts = by_concept.get(c, [])
        if as_of is not None:
            facts = [f for f in facts if f["filed"] <= as_of]
        k, v = quarter_value(facts, qstart, qend)
        if k == "value":
            return "value", v, c
        unresolved |= k == "unresolved"
    return ("unresolved" if unresolved else "no fact"), None, None


def classify_member(by_concept: dict[str, list[Fact]], qstart: date, qend: date) -> tuple[str, Decimal | None]:
    """A NULL stored member: "missed" (derivable, nonzero), "zero" (derivable, zero), "unresolved" or "no fact"
    ("no tracked USD fact ends at the quarter end": absence from our tracked facts, not proof of no payment)."""
    k, v, _ = priority_value(by_concept, qstart, qend)
    if k == "value":
        assert v is not None
        return ("missed" if v != 0 else "zero"), v
    return k, None


def common_quarter(gaap: dict[str, list[Fact]], qstart: date, qend: date) -> tuple[str, Decimal, str, set[str]]:
    """Cash dividends to the parent's common holders for one quarter: ``COMMON``; else the partnership concepts; else
    ``GENERIC`` less the filed ``PAID_NOT_COMMON`` amounts. Returns (state, value, branch, flags). State "value";
    "no fact" (none of these concepts nor ``PaymentsOfOrdinaryDividends`` has a fact ending at the quarter end: value
    0, branch "none"); "unresolved" (a payment fact ends there and does not derive, a filed deduction is unresolved,
    preferred dividends are declared with no paid amount, or only an ordinary-dividend fact exists). The partnership
    branch is a proxy: its concepts do not establish the recipient class."""
    unresolved = False
    for branch, concepts in (("common", (COMMON,)), ("partnership proxy", PARTNERSHIP)):
        for c in concepts:
            k, v = quarter_value(gaap.get(c, []), qstart, qend)
            if k == "value":
                assert v is not None
                return "value", v, branch, set()
            unresolved |= k == "unresolved"
    k, v = quarter_value(gaap.get(GENERIC, []), qstart, qend)
    if k == "value":
        assert v is not None
        flags: set[str] = set()
        paid_pref_nonzero = False
        for c in PAID_NOT_COMMON:
            kk, vv = quarter_value(gaap.get(c, []), qstart, qend)
            if kk == "unresolved":
                return "unresolved", Decimal(0), "", set()
            if kk == "value":
                assert vv is not None
                v -= vv
                paid_pref_nonzero |= c == PAID_NOT_COMMON[0] and vv != 0
        for c in DECLARED_PREFERRED:
            kk, vv = quarter_value(gaap.get(c, []), qstart, qend)
            if kk == "value" and vv and not paid_pref_nonzero:
                # Preferred dividends declared with no paid amount to deduct: composition unknown.
                return "unresolved", Decimal(0), "", set()
        return "value", v, "generic less filed deductions", flags
    unresolved |= k == "unresolved"
    # A payment concept this rebuild does not classify (ordinary dividends to all holders) makes the quarter unknown.
    kk, _ = quarter_value(gaap.get(UNTRACKED[0], []), qstart, qend)
    unresolved |= kk != "no fact"
    return ("unresolved" if unresolved else "no fact"), Decimal(0), "none", set()


def common_ttm(
    gaap: dict[str, list[Fact]], quarters: list[tuple[date, date]]
) -> tuple[Decimal, list[str], set[str], int]:
    """Σ ``common_quarter`` over the window: (total, per-quarter branch, flags, unresolved quarter count)."""
    total, branches, flags, unresolved = Decimal(0), [], set(), 0
    for qs, qe in quarters:
        state, v, branch, fl = common_quarter(gaap, qs, qe)
        if state == "unresolved":
            unresolved += 1
            branches.append("unresolved")
            continue
        total += v
        branches.append(branch)
        flags |= fl
    return total, branches, flags, unresolved


def proposed_yield(r: dict[str, Any], window: list[dict[str, Any]]) -> tuple[Decimal | None, str]:
    """The spec's construction over stored columns, with the first failing condition as the NULL's cause.

    Preconditions, as in the view: ``r`` is a complete-TTM row (``_ROWS``) and ``window`` is sql/220's window, which
    ``_WINDOW`` returns only when it passes the shape guard (four rows, end dates within 330 days)."""
    if r["market_cap_live"] is None:
        return None, f"cap refused ({r['market_cap_reason']})"
    if len(window) != 4:
        return None, "no window"
    paid = [q["dividends_paid"] for q in window]
    if any(p is None for p in paid):
        return None, "fewer than four observed members" if any(p is not None for p in paid) else "no observed member"
    if any(not p.is_finite() for p in paid):
        return None, "a non-finite member"
    if any(p < 0 for p in paid):
        return None, "a negative member"
    if r["market_cap_live"] <= 0:
        return None, "cap not positive"
    return sum(paid, Decimal(0)) / r["market_cap_live"] * 100, "published"


def inventory_cause(window: list[dict[str, Any]], tight_miss: bool, long_span: bool) -> str:
    """First matching observational predicate for a row whose two yields disagree; a candidate, not a cause."""
    dps = [q["dps_declared"] for q in window if q["dps_declared"] is not None and q["dps_declared"].is_finite()]
    sh = [q["shares_diluted"] for q in window if q["shares_diluted"] and q["shares_diluted"].is_finite()]
    positive = [d for d in dps if d > 0]
    if len(sh) >= 2 and max(sh) / min(sh) >= SPREAD:
        return "1 weighted diluted shares spread >= 1.5x"
    if any(d < 0 for d in dps):
        return "2 a negative DPS member"
    if positive and max(positive) > 50 * min(positive):
        return "3 largest positive DPS member over 50x the smallest positive one"
    if any(q["dps_declared"] is None and q["dividends_paid"] is not None and q["dividends_paid"] > 0 for q in window):
        return "4 a quarter with a positive paid member and a NULL DPS member"
    if tight_miss:
        return "5 stored paid sum outside [0.95,1.05] of the rebuilt common-only TTM"
    if long_span:
        return "6 window span over 365 days"
    return "untagged"


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


def _cf_facts(raw: dict[str, Any], concept: str) -> list[Fact]:
    return [
        {
            "start": date.fromisoformat(f["start"]) if f.get("start") else None,
            "end": date.fromisoformat(f["end"]),
            "val": Decimal(str(f["val"])),
            "filed": date.fromisoformat(f["filed"]),
            "accn": f["accn"],
        }
        for f in raw.get(concept, {}).get("units", {}).get("USD", [])
    ]


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
            by_id = {r["instrument_id"]: r for r in rows}
            win: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for q in cur.execute(_WINDOW).fetchall():
                if q["instrument_id"] in by_id:
                    win[q["instrument_id"]].append(q)
            raw: dict[int, dict[str, list[Fact]]] = defaultdict(lambda: defaultdict(list))
            for f in cur.execute(_FACTS, {"c": list(PAID), "i": sorted(win)}).fetchall():
                raw[f["instrument_id"]][f["concept"]].append(
                    {
                        "start": f["period_start"],
                        "end": f["period_end"],
                        "val": f["val"],
                        "filed": f["filed_date"],
                        "accn": f["accession_number"],
                    }
                )
            prop = {r["instrument_id"]: proposed_yield(r, win.get(r["instrument_id"], [])) for r in rows}
            pos_ids = [i for i, (y, _) in prop.items() if y is not None and y > 0]
            eight_k = {r["instrument_id"] for r in cur.execute(_EIGHT_K, {"i": pos_ids}).fetchall()}
        conn.rollback()

    sym = {r["instrument_id"]: r["symbol"] for r in rows}
    approximated: Counter[str] = Counter()
    qb = {iid: bounds(w, approximated) for iid, w in win.items()}
    pub_ids = {i for i, (y, _) in prop.items() if y is not None}
    capped = {i for i, r in by_id.items() if r["market_cap_live"] is not None}
    print(f"\ncomplete-TTM rows {len(rows)}; with a sql/220 window {len(win)}; with a published cap {len(capped)}")
    print(f"NULL period_start_date substituted in those windows (``bounds``): {dict(approximated)}")

    # 1
    print("\n1. today's dividend_yield against the proposed one, every complete-TTM row:")
    m: Counter[str] = Counter()
    for r in rows:
        y, cause = prop[r["instrument_id"]]
        today = "published" if r["dividend_yield"] is not None else "NULL"
        out = ("published zero" if y == 0 else "published") if y is not None else f"NULL: {cause}"
        m[f"today {today}; proposed {out}"] += 1
    for k, v in sorted(m.items()):
        print(f"   {k}: {v}")

    # 2
    signs: Counter[str] = Counter()
    for w in win.values():
        for q in w:
            v = q["dividends_paid"]
            signs[
                "NULL"
                if v is None
                else (
                    "non-finite" if not v.is_finite() else ("positive" if v > 0 else ("zero" if v == 0 else "negative"))
                )
            ] += 1
    print(f"\n2. dividends_paid members of the {len(win)} windows by sign: {dict(signs)}")
    active = {i for i, w in win.items() if any(q["dividends_paid"] is not None and q["dividends_paid"] != 0 for q in w)}
    nn = Counter(sum(1 for q in win[i] if q["dividends_paid"] is not None) for i in active)
    print(f"   windows with a nonzero member (``active``): {len(active)}; non-NULL members: {dict(sorted(nn.items()))}")
    neg = sorted(
        sym[i]
        for i in win
        if any(
            q["dividends_paid"] is not None and q["dividends_paid"].is_finite() and q["dividends_paid"] < 0
            for q in win[i]
        )
    )
    print(f"   windows with a negative member: {len(neg)} {neg}")

    # 3
    print("\n3. NULL members of active windows, against financial_facts_raw (USD, dated and undated facts):")
    cls: Counter[str] = Counter()
    win_cls: Counter[str] = Counter()
    missed: dict[int, tuple[list[Decimal], int]] = {}
    for iid in sorted(active):
        kinds: list[str] = []
        amounts: list[Decimal] = []
        for q, (qs, qe) in zip(win[iid], qb[iid], strict=True):
            if q["dividends_paid"] is not None:
                continue
            k, v = classify_member(raw[iid], qs, qe)
            kinds.append(k)
            if k == "missed":
                assert v is not None
                amounts.append(v)
        cls.update(kinds)
        if not kinds:
            continue
        if "missed" in kinds:
            cat = "a missed member"
            missed[iid] = (amounts, sum(1 for k in kinds if k == "unresolved"))
        elif "unresolved" in kinds:
            cat = "an unresolved member, none missed"
        elif "zero" in kinds:
            cat = "no-fact and derivable-zero members only"
        else:
            cat = "no-fact members only"
        win_cls[cat] += 1
        win_cls[f"{cat}, cap published"] += iid in capped
    print(f"   members: {dict(cls)}")
    for k, v in sorted(win_cls.items()):
        print(f"   windows: {k}: {v}")
    for iid, (amounts, unres) in sorted(missed.items(), key=lambda t: sym[t[0]]):
        stored = by_id[iid]["dividends_paid_ttm"]
        print(
            f"   {sym[iid]}: missed {[str(a) for a in amounts]}; stored sum {stored}; stored + missed "
            f"{(stored or Decimal(0)) + sum(amounts, Decimal(0))}; other NULL members unresolved {unres}"
        )

    # companyfacts pass over every window
    descriptions: dict[str, str] = {}
    check: Counter[str] = Counter()
    member_check: dict[int, list[str]] = defaultdict(list)
    candidate: Counter[str] = Counter()
    rebuilt: dict[int, tuple[Decimal, list[str], set[str], int]] = {}
    no_cf: set[int] = set()
    special: set[int] = set()
    lost = {i for i in capped if by_id[i]["dividend_yield"] is not None and prop[i][0] is None}
    lost_untracked: Counter[str] = Counter()
    with zipfile.ZipFile(zpath) as z:
        names = set(z.namelist())
        for iid in sorted(set(win) | lost):
            cik = by_id[iid]["cik10"]
            if cik is None or f"CIK{cik}.json" not in names:
                no_cf.add(iid)
                if iid in lost:
                    lost_untracked[f"{prop[iid][1]}; no companyfacts member"] += 1
                continue
            g = json.loads(z.read(f"CIK{cik}.json")).get("facts", {}).get("us-gaap", {})
            for c in DESCRIBED:
                if c in g and c not in descriptions:
                    descriptions[c] = g[c].get("description") or ""
            gaap = {c: _cf_facts(g, c) for c in DESCRIBED}
            qs = qb.get(iid, [])
            if iid in active or iid in pub_ids:
                for q, (q0, q1) in zip(win[iid], qs, strict=True):
                    if q["dividends_paid"] is None:
                        continue
                    k, v, c = priority_value(gaap, q0, q1, q["filed_date"])
                    if k != "value":
                        check[k] += 1
                        member_check[iid].append(f"{q1} {k}")
                        continue
                    assert v is not None
                    st = q["dividends_paid"]
                    ok = abs(v - st) <= Decimal("0.005") * max(abs(st), Decimal(1))
                    check["agree" if ok else "differ"] += 1
                    member_check[iid].append(f"{q1} stored {st} rule {v} {'agree' if ok else 'DIFFER'}")
                    if ok:
                        candidate[str(c)] += 1
            if qs:
                rebuilt[iid] = common_ttm(gaap, qs)
                if any(has_value(gaap, SPECIAL, a, b) for a, b in qs):
                    special.add(iid)
            if iid in lost:
                found = sorted(c for c in UNTRACKED if any(has_value(gaap, c, a, b) for a, b in qs))
                lost_untracked[f"{prop[iid][1]}; untracked filed in window: {' + '.join(found) or 'none'}"] += 1

    print(
        "\n4. the rule's own check: ``priority_value`` on facts filed by each member's filed_date, against every "
        "stored "
        "non-NULL member of active windows and proposed publications (0.5%):"
    )
    print(f"   {dict(check)}; matching candidate concept: {dict(candidate.most_common())}")

    # 5
    print("\n5. the rebuilt common-only TTM (``common_ttm``):")

    def compared(ids: set[int]) -> tuple[dict[int, Decimal], Counter[str]]:
        ratios: dict[int, Decimal] = {}
        excl: Counter[str] = Counter()
        for i in ids:
            if i in no_cf:
                excl["no companyfacts member"] += 1
                continue
            total, _, _, unres = rebuilt[i]
            d = sum((q["dividends_paid"] for q in win[i] if q["dividends_paid"] is not None), Decimal(0))
            if unres:
                excl["a quarter unresolved"] += 1
            elif total == 0 and d == 0:
                excl["stored and rebuilt both zero"] += 1
            elif total <= 0:
                excl["rebuilt total <= 0, stored not"] += 1
            else:
                ratios[i] = d / total
        return ratios, excl

    for label, ids in (("active windows", active), ("proposed-published rows", pub_ids)):
        ratios, excl = compared(ids)
        tight = sum(1 for x in ratios.values() if TIGHT[0] <= x <= TIGHT[1])
        wholly = [i for i in ratios if rebuilt[i][1] == ["common"] * 4]
        w_tight = sum(1 for i in wholly if TIGHT[0] <= ratios[i] <= TIGHT[1])
        print(
            f"   {label}: {len(ids)}; compared {len(ratios)}; excluded {dict(excl)}; "
            f"stored/rebuilt in [0.95,1.05] {tight}; buckets "
            f"{dict(sorted(Counter(bucket(x) for x in ratios.values()).items()))}"
        )
        print(f"      all four quarters on {COMMON}: {len(wholly)} windows, {w_tight} in [0.95,1.05]")
    pub_ratios, _ = compared(pub_ids)
    for i in sorted(pub_ratios, key=lambda i: pub_ratios[i]):
        if not TIGHT[0] <= pub_ratios[i] <= TIGHT[1]:
            t, b, f, _ = rebuilt[i]
            print(
                f"   published, outside [0.95,1.05]: {sym[i]} stored {by_id[i]['dividends_paid_ttm']} rebuilt {t} "
                f"ratio {pub_ratios[i]:.3f} quarters {b} flags {sorted(f)}"
            )
    print(
        f"   composition flags over compared published rows: "
        f"{dict(Counter(f for i in pub_ratios for f in rebuilt[i][2]))}"
    )
    # Both forms against the rebuilt yield, per row.
    both_ref = [i for i in pub_ratios if by_id[i]["dividend_yield"] is not None and (prop[i][0] or 0) > 0]
    acc: Counter[str] = Counter()
    worse: list[tuple[str, Decimal, Decimal]] = []
    for i in both_ref:
        ref = rebuilt[i][0] / by_id[i]["market_cap_live"] * 100
        t_x, p_x = by_id[i]["dividend_yield"] / ref, (prop[i][0] or Decimal(0)) / ref
        acc["today in [0.95,1.05]"] += TIGHT[0] <= t_x <= TIGHT[1]
        acc["proposed in [0.95,1.05]"] += TIGHT[0] <= p_x <= TIGHT[1]
        acc["today in [0.8,1.25]"] += BAND[0] <= t_x <= BAND[1]
        acc["proposed in [0.8,1.25]"] += BAND[0] <= p_x <= BAND[1]
        if abs(p_x.ln()) > abs(t_x.ln()) and not TIGHT[0] <= p_x <= TIGHT[1]:
            worse.append((sym[i], t_x, p_x))
    print(
        f"   rows publishing a positive yield both ways and compared: {len(both_ref)}; "
        f"against the rebuilt yield: {dict(acc)}"
    )
    print(
        f"   rows where the proposal is further from the rebuilt yield than today, and outside [0.95,1.05]: "
        f"{len(worse)}"
    )
    sym_to_id = {v: k for k, v in sym.items()}
    for s_, t_x, p_x in sorted(worse, key=lambda t: -abs(t[2].ln())):
        print(f"      {s_}: today/rebuilt {t_x:.3f} proposed/rebuilt {p_x:.3f}; members {member_check[sym_to_id[s_]]}")
    mismatch = sorted(sym[i] for i in pub_ids if any("DIFFER" in m_ for m_ in member_check.get(i, [])))
    print(
        f"   proposed-published rows with a stored member that differs from the rule (section 4): {len(mismatch)} "
        f"{mismatch}"
    )
    gained_mismatch = sorted(s_ for s_ in mismatch if by_id[sym_to_id[s_]]["dividend_yield"] is None)
    print(f"   of them today NULL (newly exposed): {len(gained_mismatch)} {gained_mismatch}")
    unresolved_pub = sorted(sym[i] for i in pub_ids if i not in pub_ratios)
    print(f"   proposed publications without a resolved comparison: {len(unresolved_pub)} {unresolved_pub}")
    zero_pub = sorted(i for i in pub_ids if prop[i][0] == 0)
    print(
        f"   zero publications: {len(zero_pub)}; rebuilt total per row: "
        f"{
            [
                (
                    sym[i],
                    str(rebuilt[i][0]) if i in rebuilt else 'no companyfacts',
                    rebuilt[i][3] if i in rebuilt else None,
                )
                for i in zero_pub
            ]
        }"
    )

    # 6
    sp_pub = sorted(sym[i] for i in special if i in pub_ids)
    print(
        f"\n6. windows with a nonzero {SPECIAL} amount in a window quarter: {len(special)}; "
        f"of them active {len(special & active)}; proposed-published {len(sp_pub)} {sp_pub}"
    )

    # 7
    print("\n7. today published, proposed NULL with a published cap, by cause and untracked concept:")
    for k, v in lost_untracked.most_common():
        print(f"   {k}: {v}")
    print(f"   total {sum(lost_untracked.values())} of {len(lost)}")

    # 8
    both = [r for r in rows if r["dividend_yield"] is not None and (prop[r["instrument_id"]][0] or 0) > 0]
    ratio: dict[int, tuple[Decimal, Decimal, Decimal]] = {}
    for r in both:
        iid = r["instrument_id"]
        y = prop[iid][0]
        assert y is not None
        n = r["market_cap_live"] / r["valuation_price"]
        d = sum((q["dividends_paid"] for q in win[iid]), Decimal(0))
        vp = r["valuation_price"] / r["current_price"]
        dnd = r["dps_declared_ttm"] * n / d
        x = r["dividend_yield"] / y
        assert abs(x - vp * dnd) <= Decimal("1e-9") * x, (r["symbol"], x, vp * dnd)
        ratio[iid] = (x, vp, dnd)
    vps = sorted(v[1] for v in ratio.values())
    print(
        f"\n8. rows publishing a positive yield both ways: {len(both)}; today ÷ proposed: "
        f"{dict(sorted(Counter(bucket(x[0]) for x in ratio.values()).items()))}; identity asserted on every row; "
        f"V/P min {vps[0]:.4f} max {vps[-1]:.4f}, outside [0.95,1.05] "
        f"{sum(1 for v in vps if not TIGHT[0] <= v <= TIGHT[1])}"
    )
    tight_miss = {i for i, x in pub_ratios.items() if not TIGHT[0] <= x <= TIGHT[1]}
    span = {i: (qb[i][-1][1] - qb[i][0][0]).days + 1 for i in win}
    tags: Counter[str] = Counter()
    for iid, (x, vp, dnd) in sorted(ratio.items(), key=lambda t: t[1][0]):
        if BAND[0] <= x <= BAND[1]:
            continue
        tag = inventory_cause(win[iid], iid in tight_miss, span[iid] > 365)
        tags[tag] += 1
        print(f"   {sym[iid]}: today/proposed {x:.3f}  V/P {vp:.4f}  DPS*N/D {dnd:.3f}  [{tag}]")
    print(f"   outside [0.8,1.25]: {sum(tags.values())}, by first matching predicate: {dict(sorted(tags.items()))}")

    def span_bucket(d: int) -> str:
        if d < 330:
            return "< 330"
        if d < 365:
            return "330-364"
        if d == 365:
            return "365"
        return "366-371" if d <= 371 else "> 371"

    print(
        f"   window span (first start to last end, inclusive), active windows: "
        f"{dict(sorted(Counter(span_bucket(span[i]) for i in active).items()))}"
    )
    odd = sorted(f"{sym[i]}:{span[i]}" for i in active if span[i] < 330 or span[i] > 371)
    print(f"   active windows spanning < 330 or > 371 days: {odd}")
    bad_iv = sorted(
        sym[i]
        for i in pub_ids
        if any(
            q["period_start_date"] is None or not 80 <= (q["period_end_date"] - q["period_start_date"]).days + 1 <= 100
            for q in win[i]
        )
    )
    print(
        f"   proposed-published rows with a member whose stored interval is NULL or outside 80-100 days: "
        f"{len(bad_iv)} {bad_iv}"
    )

    # 9-10
    short = sorted(
        sym[i]
        for i in pub_ids
        if by_id[i]["first_bar"] is None or (by_id[i]["session"] - by_id[i]["first_bar"]).days < 365
    )
    print(
        f"\n9. proposed-published rows whose stored price_daily series starts under 365 days before the session "
        f"(stored bars, not listing history): {len(short)}"
    )
    print(
        f"\n10. proposed-published positive rows: {len(pos_ids)}; with an 8-K ex-date in the 365 days to today: "
        f"{len(eight_k)}"
    )

    # 11
    print("\n11. named cases")
    for r in rows:
        if r["symbol"] in NAMED:
            iid = r["instrument_id"]
            print(
                f"   {r['symbol']}: today {r['dividend_yield']} proposed {prop[iid]} dps_ttm {r['dps_declared_ttm']} "
                f"paid_ttm {r['dividends_paid_ttm']} cap {r['market_cap_live']} reason {r['market_cap_reason']}"
            )
            for q in win.get(iid, []):
                print(
                    f"      {q['period_start_date']}..{q['period_end_date']} {q['period_type']} "
                    f"dps {q['dps_declared']} paid {q['dividends_paid']} diluted {q['shares_diluted']}"
                )

    print("\n12. us-gaap descriptions (companyfacts 'description', verbatim):")
    for c, d in sorted(descriptions.items()):
        print(f"   - {c}: {d}")


def has_value(gaap: dict[str, list[Fact]], concept: str, qstart: date, qend: date) -> bool:
    """A nonzero value of ``concept`` for the quarter."""
    k, v = quarter_value(gaap.get(concept, []), qstart, qend)
    return k == "value" and v is not None and v != 0


if __name__ == "__main__":
    main()
