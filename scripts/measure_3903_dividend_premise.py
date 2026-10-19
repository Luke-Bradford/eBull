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
3. every NULL member of a window with a nonzero member, classified against ``financial_facts_raw``
   (``classify_member``), and for each window with a derivable nonzero member its signed amounts and corrected sum;
4. the rule's own check: ``quarter_value`` in the normaliser's concept priority against every stored non-NULL member,
   and the concept that supplies each agreeing member;
5. the common-only TTM (``common_ttm``) rebuilt per quarter from companyfacts against the stored sum, over every
   complete-TTM window with a nonzero member, and over the proposed-published subset; only windows with no
   unresolved quarter enter the ratio; every window outside [0.95, 1.05] listed with both amounts;
6. special distributions (``PaymentsOfCapitalDistribution``) in the window quarters;
7. rows today publishes and the proposal does not, by cause, with the untracked payment concepts filed in their window
   quarters;
8. the discrepancy inventory over rows publishing both: today ÷ proposed, the identity
   today ÷ proposed = (V/P) × (DPS × N / D) asserted per row, and each row outside [0.8, 1.25] tagged with its first
   matching observational predicate (``inventory_cause``);
9. security price history shorter than 365 days before the session, over proposed-published rows;
10. 8-K ``dividend_events`` with an ex-date in the 365 days to today, over proposed-published positive rows;
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
           r.dividends_paid
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
      AND period_start IS NOT NULL
"""

Fact = dict[str, Any]


def bounds(window: list[dict[str, Any]]) -> list[tuple[date, date]]:
    """Each member's [start, end]. A NULL ``period_start_date`` takes the previous member's end + 1 day; the first
    member's, its end - 90 days. ``APPROXIMATED`` counts both substitutions."""
    out = []
    for k, q in enumerate(window):
        start = q["period_start_date"]
        if start is None:
            start = (
                window[k - 1]["period_end_date"] + timedelta(days=1) if k else q["period_end_date"] - timedelta(days=90)
            )
            APPROXIMATED["first member" if k == 0 else "later member"] += 1
        out.append((start, q["period_end_date"]))
    return out


APPROXIMATED: Counter[str] = Counter()


def _near(a: date, b: date) -> bool:
    return abs((a - b).days) <= SLACK


def _latest(facts: list[Fact]) -> Fact | None:
    return max(facts, key=lambda f: (f["filed"], f["accn"])) if facts else None


def quarter_value(facts: list[Fact], qstart: date, qend: date) -> tuple[str, Decimal | None]:
    """One concept's flow for the quarter [``qstart``, ``qend``]: ("value", v), ("unresolved", None) or
    ("absent", None).

    Candidates: the latest-filed fact spanning the quarter; and, per span that ends at ``qend`` and starts before
    ``qstart`` (a year-to-date or annual fact), its latest-filed fact minus the latest-filed same-start fact ending the
    day before ``qstart``. The candidate whose newest operand was filed last wins; a direct fact wins a tie.
    "unresolved" = a fact ends at ``qend`` but no candidate forms; "absent" = no fact of the concept ends there."""
    ending = [f for f in facts if _near(f["end"], qend)]
    if not ending:
        return "absent", None
    cands: list[tuple[date, int, Decimal]] = []
    direct = _latest([f for f in ending if _near(f["start"], qstart)])
    if direct is not None:
        cands.append((direct["filed"], 1, direct["val"]))
    by_start: dict[date, list[Fact]] = defaultdict(list)
    for f in ending:
        if f["start"] < qstart - timedelta(days=SLACK):
            by_start[f["start"]].append(f)
    for start, fs in by_start.items():
        cum = _latest(fs)
        prior = _latest([g for g in facts if g["start"] == start and _near(g["end"], qstart - timedelta(days=1))])
        if cum is not None and prior is not None:
            cands.append((max(cum["filed"], prior["filed"]), 0, cum["val"] - prior["val"]))
    if not cands:
        return "unresolved", None
    return "value", max(cands, key=lambda c: (c[0], c[1]))[2]


def priority_value(
    by_concept: dict[str, list[Fact]], qstart: date, qend: date
) -> tuple[str, Decimal | None, str | None]:
    """The normaliser's reading of a quarter: the first concept in ``PAID`` with a value. ("unresolved", …) when no
    concept has one and any is unresolved; else ("absent", …)."""
    unresolved = False
    for c in PAID:
        k, v = quarter_value(by_concept.get(c, []), qstart, qend)
        if k == "value":
            return "value", v, c
        unresolved |= k == "unresolved"
    return ("unresolved" if unresolved else "absent"), None, None


def classify_member(by_concept: dict[str, list[Fact]], qstart: date, qend: date) -> tuple[str, Decimal | None]:
    """A NULL stored member: "missed" (derivable, nonzero), "zero" (derivable, zero), "unresolved" or "absent"."""
    k, v, _ = priority_value(by_concept, qstart, qend)
    if k == "value":
        assert v is not None
        return ("missed" if v != 0 else "zero"), v
    return k, None


def common_quarter(gaap: dict[str, list[Fact]], qstart: date, qend: date) -> tuple[str, Decimal, str, set[str]]:
    """Cash dividends to the parent's common holders for one quarter: ``COMMON``; else the partnership concepts; else
    ``GENERIC`` less the filed ``PAID_NOT_COMMON`` amounts. Returns (state, value, branch, flags); state "absent" =
    no concept has a fact ending at the quarter end (value 0), "unresolved" = some fact ends there but none derives."""
    unresolved = False
    for branch, concepts in (("common", (COMMON,)), ("partnership", PARTNERSHIP)):
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
        paid_pref = False
        for c in PAID_NOT_COMMON:
            kk, vv = quarter_value(gaap.get(c, []), qstart, qend)
            if kk == "value":
                assert vv is not None
                v -= vv
                paid_pref |= c == PAID_NOT_COMMON[0] and vv != 0
            elif kk == "unresolved":
                flags.add("a deduction unresolved")
        for c in DECLARED_PREFERRED:
            kk, vv = quarter_value(gaap.get(c, []), qstart, qend)
            if kk == "value" and vv and not paid_pref:
                flags.add("preferred declared, no paid-preferred fact")
        return "value", v, "generic less paid preferred and NCI", flags
    unresolved |= k == "unresolved"
    return ("unresolved" if unresolved else "absent"), Decimal(0), "", set()


def common_ttm(
    gaap: dict[str, list[Fact]], quarters: list[tuple[date, date]]
) -> tuple[Decimal, set[str], set[str], int]:
    """Σ ``common_quarter`` over the window: (total, branches used, flags, unresolved quarter count)."""
    total, branches, flags, unresolved = Decimal(0), set(), set(), 0
    for qs, qe in quarters:
        state, v, branch, fl = common_quarter(gaap, qs, qe)
        if state == "unresolved":
            unresolved += 1
            continue
        total += v
        if branch:
            branches.add(branch)
        flags |= fl
    return total, branches, flags, unresolved


def proposed_yield(r: dict[str, Any], window: list[dict[str, Any]]) -> tuple[Decimal | None, str]:
    """The spec's construction over stored columns, with the first failing condition as the NULL's cause."""
    if r["market_cap_live"] is None:
        return None, f"cap refused ({r['market_cap_reason']})"
    paid = [q["dividends_paid"] for q in window]
    if len(window) != 4:
        return None, "no window"
    if any(p is None for p in paid):
        return None, "fewer than four observed members" if any(p is not None for p in paid) else "no observed member"
    if any(p < 0 or not p.is_finite() for p in paid):
        return None, "a negative or non-finite member"
    if r["market_cap_live"] <= 0:
        return None, "cap not positive"
    return sum(paid, Decimal(0)) / r["market_cap_live"] * 100, "published"


def inventory_cause(window: list[dict[str, Any]], tight_miss: bool, long_span: bool) -> str:
    """First matching observational predicate for a row whose two yields disagree; a candidate, not a cause."""
    dps = [q["dps_declared"] for q in window if q["dps_declared"] is not None]
    sh = [q["shares_diluted"] for q in window if q["shares_diluted"]]
    positive = [d for d in dps if d > 0]
    if len(sh) >= 2 and max(sh) / min(sh) >= SPREAD:
        return "1 weighted diluted shares spread >= 1.5x"
    if any(d < 0 for d in dps):
        return "2 a negative DPS member"
    if positive and max(positive) > 50 * min(positive):
        return "3 largest positive DPS member over 50x the smallest positive one"
    if any(q["dps_declared"] is None and (q["dividends_paid"] or 0) > 0 for q in window):
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
    out = []
    for f in raw.get(concept, {}).get("units", {}).get("USD", []):
        if f.get("start"):
            out.append(
                {
                    "start": date.fromisoformat(f["start"]),
                    "end": date.fromisoformat(f["end"]),
                    "val": Decimal(str(f["val"])),
                    "filed": date.fromisoformat(f["filed"]),
                    "accn": f["accn"],
                }
            )
    return out


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
    by_id = {r["instrument_id"]: r for r in rows}

    qb = {iid: bounds(w) for iid, w in win.items()}
    print(f"NULL period_start_date substituted (``bounds``): {dict(APPROXIMATED)}")

    def quarters(iid: int) -> list[tuple[date, date]]:
        return qb.get(iid, [])

    # 1
    print(f"\ncomplete-TTM rows {len(rows)}; with a published cap {sum(1 for r in rows if r['market_cap_live'])}")
    print("\n1. today's dividend_yield against the proposed one, by the proposal's outcome:")
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
            signs["NULL" if v is None else ("positive" if v > 0 else ("zero" if v == 0 else "negative"))] += 1
    print(f"\n2. dividends_paid members of every complete window by sign: {dict(signs)}")
    active = {i for i, w in win.items() if i in by_id and any((q["dividends_paid"] or 0) != 0 for q in w)}
    nn = Counter(sum(1 for q in win[i] if q["dividends_paid"] is not None) for i in active)
    print(f"   complete-TTM windows with a nonzero member: {len(active)}; non-NULL members: {dict(sorted(nn.items()))}")
    neg = sorted(sym[i] for i in active if any((q["dividends_paid"] or 0) < 0 for q in win[i]))
    print(f"   with a negative member: {len(neg)} {neg}")

    # 3
    print("\n3. NULL members of windows with a nonzero member, against financial_facts_raw (USD):")
    cls: Counter[str] = Counter()
    win_cls: Counter[str] = Counter()
    missed: dict[int, list[Decimal]] = {}
    for iid in sorted(active):
        kinds = []
        for q, (qs, qe) in zip(win[iid], qb[iid], strict=True):
            if q["dividends_paid"] is not None:
                continue
            k, v = classify_member(raw[iid], qs, qe)
            kinds.append(k)
            if k == "missed":
                assert v is not None
                missed.setdefault(iid, []).append(v)
        cls.update(kinds)
        if kinds:
            win_cls[
                "a missed member"
                if "missed" in kinds
                else ("an unresolved member" if "unresolved" in kinds else "absent or zero only")
            ] += 1
    print(f"   members: {dict(cls)}")
    print(f"   windows with a NULL member: {dict(win_cls)}")
    for iid, amounts in sorted(missed.items(), key=lambda t: sym[t[0]]):
        stored = by_id[iid]["dividends_paid_ttm"]
        corrected = (stored or Decimal(0)) + sum(amounts, Decimal(0))
        print(f"   {sym[iid]}: missed {[str(a) for a in amounts]}; stored sum {stored}; corrected {corrected}")

    # 4-7 need companyfacts
    names: set[str] = set()
    descriptions: dict[str, str] = {}
    check: Counter[str] = Counter()
    supplier: Counter[str] = Counter()
    cmp_all: dict[int, tuple[Decimal, Decimal, set[str], set[str]]] = {}
    unresolved_windows: Counter[str] = Counter()
    special: list[str] = []
    lost_untracked: Counter[str] = Counter()
    with zipfile.ZipFile(zpath) as z:
        names = set(z.namelist())

        def gaap_of(iid: int) -> dict[str, list[Fact]] | None:
            cik = by_id[iid]["cik10"]
            if cik is None or f"CIK{cik}.json" not in names:
                return None
            g = json.loads(z.read(f"CIK{cik}.json")).get("facts", {}).get("us-gaap", {})
            for c in DESCRIBED:
                if c in g and c not in descriptions:
                    descriptions[c] = g[c].get("description") or ""
            return {c: _cf_facts(g, c) for c in (*PAID, *PAID_NOT_COMMON, *DECLARED_PREFERRED, *UNTRACKED)}

        lost = [
            i
            for i in by_id
            if by_id[i]["dividend_yield"] is not None and prop[i][0] is None and by_id[i]["market_cap_live"] is not None
        ]
        for iid in sorted(active | set(lost)):
            gaap = gaap_of(iid)
            if gaap is None:
                unresolved_windows["no companyfacts member"] += 1
                continue
            qs = quarters(iid) if iid in win else []
            if iid in active:
                for q, (q0, q1) in zip(win[iid], qb[iid], strict=True):
                    if q["dividends_paid"] is None:
                        continue
                    k, v, c = priority_value(gaap, q0, q1)
                    if k != "value":
                        check[k] += 1
                        continue
                    assert v is not None
                    s = q["dividends_paid"]
                    ok = abs(v - s) <= Decimal("0.005") * max(abs(s), Decimal(1))
                    check["agree" if ok else "differ"] += 1
                    if ok:
                        supplier[str(c)] += 1
                total, branches, flags, unres = common_ttm(gaap, qs)
                if unres:
                    unresolved_windows["a quarter unresolved"] += 1
                elif total <= 0:
                    unresolved_windows["rebuilt total <= 0"] += 1
                elif by_id[iid]["dividends_paid_ttm"] is not None:
                    cmp_all[iid] = (by_id[iid]["dividends_paid_ttm"], total, branches, flags)
                if any(has_value(gaap, SPECIAL, a, b) for a, b in qs):
                    special.append(sym[iid])
            if iid in lost:
                found = sorted(c for c in UNTRACKED if any(has_value(gaap, c, a, b) for a, b in qs))
                lost_untracked[f"{prop[iid][1]}; untracked filed in window: {' + '.join(found) or 'none'}"] += 1

    print("\n4. the rule's own check, stored non-NULL members of windows with a nonzero member (0.5%):", dict(check))
    print(f"   supplying concept of agreeing members: {dict(supplier.most_common())}")

    print("\n5. stored paid sum ÷ rebuilt common-only TTM (``common_ttm``):")
    print(f"   windows with a nonzero member {len(active)}; excluded: {dict(unresolved_windows)}")
    pub_ids = {i for i, (y, _) in prop.items() if y is not None}
    for label, ids in (("all complete-TTM", set(cmp_all)), ("proposed-published", set(cmp_all) & pub_ids)):
        ratios = {i: cmp_all[i][0] / cmp_all[i][1] for i in ids}
        tight = sum(1 for x in ratios.values() if TIGHT[0] <= x <= TIGHT[1])
        print(
            f"   {label}: {len(ratios)} windows; in [0.95,1.05] {tight}; buckets "
            f"{dict(sorted(Counter(bucket(x) for x in ratios.values()).items()))}"
        )
        only_common = [i for i in ids if cmp_all[i][2] == {"common"}]
        oc_tight = sum(1 for i in only_common if TIGHT[0] <= ratios[i] <= TIGHT[1])
        print(f"      every quarter on {COMMON}: {len(only_common)} windows, {oc_tight} in [0.95,1.05]")
    # Both forms against the rebuilt common-only yield (common_ttm / market_cap_live), where both publish.
    acc: Counter[str] = Counter()
    for i in cmp_all:
        r = by_id[i]
        if r["market_cap_live"] is None or r["dividend_yield"] is None or prop[i][0] is None:
            continue
        ref = cmp_all[i][1] / r["market_cap_live"] * 100
        acc["rows"] += 1
        acc["today in [0.95,1.05] of rebuilt"] += TIGHT[0] <= r["dividend_yield"] / ref <= TIGHT[1]
        acc["proposed in [0.95,1.05] of rebuilt"] += TIGHT[0] <= prop[i][0] / ref <= TIGHT[1]
        acc["today in [0.8,1.25] of rebuilt"] += BAND[0] <= r["dividend_yield"] / ref <= BAND[1]
        acc["proposed in [0.8,1.25] of rebuilt"] += BAND[0] <= prop[i][0] / ref <= BAND[1]
    print(f"   rows where today and the proposal both publish, against the rebuilt yield: {dict(acc)}")
    flagged = Counter(f for i in cmp_all for f in cmp_all[i][3])
    print(f"   composition flags: {dict(flagged)}")
    tight_miss = {i for i in cmp_all if not TIGHT[0] <= cmp_all[i][0] / cmp_all[i][1] <= TIGHT[1]}
    for i in sorted(tight_miss, key=lambda i: cmp_all[i][0] / cmp_all[i][1]):
        s, t, b, f = cmp_all[i]
        print(
            f"   outside [0.95,1.05]: {sym[i]} stored {s} rebuilt {t} ratio {s / t:.3f} "
            f"branches {sorted(b)} flags {sorted(f)} {'published' if i in pub_ids else 'not published'}"
        )

    print(
        f"\n6. windows with a nonzero member and a {SPECIAL} amount in a window quarter: "
        f"{len(special)} {sorted(special)}"
    )

    print("\n7. today published, proposed NULL with a published cap, by cause and untracked concept:")
    for k, v in lost_untracked.most_common():
        print(f"   {k}: {v}")

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
    print(
        f"\n8. rows publishing both (positive proposed): {len(both)}; today ÷ proposed: "
        f"{dict(sorted(Counter(bucket(x[0]) for x in ratio.values()).items()))}; identity asserted on every row"
    )
    long_span = {i for i in win if (qb[i][-1][1] - qb[i][0][0]).days + 1 > 365}
    tags: Counter[str] = Counter()
    for iid, (x, vp, dnd) in sorted(ratio.items(), key=lambda t: t[1][0]):
        if BAND[0] <= x <= BAND[1]:
            continue
        tag = inventory_cause(win[iid], iid in tight_miss, iid in long_span)
        tags[tag] += 1
        print(f"   {sym[iid]}: today/proposed {x:.3f}  V/P {vp:.4f}  DPS*N/D {dnd:.3f}  [{tag}]")
    print(f"   outside [0.8,1.25]: {sum(tags.values())}, by first matching predicate: {dict(sorted(tags.items()))}")
    spans = Counter((qb[i][-1][1] - qb[i][0][0]).days + 1 for i in active)
    print(
        f"   window span in days (first start to last end, inclusive), windows with a nonzero member: "
        f"{dict(sorted(spans.items()))}"
    )

    # 9-10
    short = sorted(
        sym[i]
        for i in pub_ids
        if by_id[i]["first_bar"] is None or (by_id[i]["session"] - by_id[i]["first_bar"]).days < 365
    )
    print(f"\n9. proposed-published rows with under 365 days of price history before the session: {len(short)} {short}")
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
                    f"dps {q['dps_declared']} "
                    f"paid {q['dividends_paid']} diluted {q['shares_diluted']}"
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
