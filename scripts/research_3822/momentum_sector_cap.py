# ruff: noqa: E501
"""Does the skill's sector cap help the #3822 momentum book? Quick test on the same archive as momentum_backtest.py.

Rule (`.claude/skills/quant/portfolio-construction-and-risk.md`, "Neutralise unintended bets"): cap any sector at about
2x its benchmark weight; NULL-sector names are their own bucket. Sector = FF-12 group of the point-in-time SIC from the
#3609 factor panel (`load_ff12`, pinned Siccodes12). Benchmark weight = the group's ME share of the formation's
500-name universe, the same-universe cap reference #3740 step 3 uses (`vw_factor_book.sector_caps`), because no SPY
constituent weights are held. For an equal-weight book of N names a group may hold at most floor(2 x share x N) names;
the book fills down the ranking, skipping names whose group is full.

`--covered-only` drops universe names without a usable panel ME (finite, positive) from both arms. In the default run
an uncovered name with a SIC still uses its group's cap, but a name with no panel row lands in Unclassified, whose cap
is funded only by covered NULL-SIC names, so the capped arm rarely holds it. Both arms share momentum_backtest.py's
return treatment (a missing or out-of-range next-month return counts as 0%); the run prints how often a held name hits it.

Input: the panel extract written by `--extract` (series_id -> [ME, SIC] per formation month). The panel ends 2024-07;
later formations reuse the latest panel month.
"""

import gzip
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from statistics import mean

import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from app.services.factor_book_reference import load_ff12  # noqa: E402
from app.services.vw_factor_book import SECTOR_CAP_MULTIPLE  # noqa: E402

PANEL_DIR = Path.home() / "Library/Application Support/eBull/research/factor_panel_3609"
PANEL_RUNS = ("2026-10-09-af7889cc-stageA", "2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f")
EXTRACT = Path("/tmp/msc/panel.json")
LINK = Path.home() / "Library/Application Support/eBull/research/security_linkage_3361/2026-10-05-805bc9e6/series"
UNCLASSIFIED = "Unclassified"

if "--extract" in sys.argv:
    extract: dict[str, dict[int, list]] = {}
    for panel_run in PANEL_RUNS:  # stage B rows overwrite stage A rows for the same month
        for line in gzip.open(PANEL_DIR / panel_run / "rows.jsonl.gz", "rt"):
            r = json.loads(line)
            if r.get("series_id") is None:
                continue
            me = (r.get("me") or {}).get("value")
            extract.setdefault(r["M"][:7], {})[r["series_id"]] = [float(me) if me else None, r.get("sic")]
    EXTRACT.parent.mkdir(exist_ok=True)
    EXTRACT.write_text(json.dumps(extract))

panel = {k: {int(s): v for s, v in rows.items()} for k, rows in json.loads(EXTRACT.read_text()).items()}
panel_months = sorted(panel)
ff12 = load_ff12()

linked = set()
for p in LINK.iterdir():
    doc = json.loads(p.read_text())
    if not doc.get("collision") and doc.get("observations"):
        linked.add(int(p.stem))

SQL = """
with d as (
  select series_id, bar_date, close::float c, coalesce(split_factor,1)::float sf, coalesce(dividend,0)::float dv,
         close::float * coalesce(volume,0)::float dvol,
         date_trunc('month', bar_date)::date m
  from research_price_daily
  where series_id = any(%s) and bar_date >= '2012-06-01'
)
select series_id, m,
       (array_agg(c order by bar_date desc))[1] last_close,
       exp(sum(ln(nullif(sf,0)))) split_prod,
       sum(dv) divs,
       avg(dvol) adv
from d group by 1, 2
"""
with psycopg.connect(settings.database_url) as c:
    rows = c.execute(SQL, (list(linked),)).fetchall()
    spy = c.execute(
        "select date_trunc('month', bar_date)::date m, (array_agg(adj_close::float order by bar_date desc))[1] "
        "from research_price_daily where series_id=7694 group by 1 order by 1"
    ).fetchall()

by = defaultdict(dict)
for sid, m, lc, sp, dv, adv in rows:
    by[sid][m] = (lc, sp or 1.0, dv or 0.0, adv or 0.0)
months = sorted({m for s in by.values() for m in s})
ret = defaultdict(dict)
for sid, mm in by.items():
    ks = sorted(mm)
    for a, b in zip(ks, ks[1:]):
        if (b.year * 12 + b.month) - (a.year * 12 + a.month) != 1:
            continue
        pa, pb = mm[a][0], mm[b]
        if pa > 0:
            r = (pb[0] * pb[1] + pb[2]) / pa - 1
            if -1 < r < 5:
                ret[sid][b] = r
spym = dict(spy)


def has_me(row) -> bool:
    return bool(row) and row[0] is not None and math.isfinite(row[0]) and row[0] > 0


def group_of(row) -> str:
    return ff12.industry(row[1]) if row and isinstance(row[1], int) else UNCLASSIFIED


def ranked(m, UNIV=500):
    """(score desc, sid) for the radj universe at formation month m, plus each name's group and the per-group cap."""
    idx = months.index(m)
    hist = months[idx - 12 : idx + 1]
    cands = []
    for sid, mm in by.items():
        if m not in mm or mm[m][0] < 5:
            continue
        adv = mean(by[sid].get(x, (0, 1, 0, 0))[3] for x in months[idx - 2 : idx + 1])
        rs = [ret[sid].get(x) for x in hist[1:-1]]
        if any(r is None for r in rs):
            continue
        cands.append((adv, sid, rs))
    cands.sort(reverse=True)
    scored = []
    for _, sid, rs in cands[:UNIV]:
        mu = mean(rs)
        sd = math.sqrt(sum((r - mu) ** 2 for r in rs) / (len(rs) - 1)) or 1e-9
        scored.append(((math.prod(1 + r for r in rs) - 1) / sd, sid))
    scored.sort(reverse=True)
    key = max((k for k in panel_months if k <= m.strftime("%Y-%m")), default=None)
    prow = panel.get(key, {})
    groups = {sid: group_of(prow.get(sid)) for _, sid in scored}
    me_by = defaultdict(float)
    for _, sid in scored:
        row = prow.get(sid)
        if row is not None and has_me(row):
            me_by[groups[sid]] += row[0]
    total = sum(me_by.values())
    shares = {g: v / total for g, v in me_by.items()} if total else {}
    coverage = sum(1 for _, sid in scored if has_me(prow.get(sid))) / len(scored)
    if COVERED_ONLY:  # both arms on the names that carry a panel ME, so the cap reference covers the whole universe
        scored = [(g, sid) for g, sid in scored if has_me(prow.get(sid))]
    return scored, groups, shares, coverage


def run(N, COST, capped, start=date(2014, 10, 1), end=date(2024, 9, 1), stats=None):
    nav, w_prev, navs = 1.0, {}, []
    ms = [m for m in months if start <= m <= end]
    for i, m in enumerate(ms[:-1]):
        nxt = ms[i + 1]
        if months.index(m) < 13:
            continue
        scored, groups, shares, coverage = RANKS[m]
        if capped:
            limit = {g: math.floor(SECTOR_CAP_MULTIPLE * s * N + 1e-9) for g, s in shares.items()}
            used, top = Counter(), []
            for _, sid in scored:
                g = groups[sid]
                if used[g] < limit.get(g, 0):
                    top.append(sid)
                    used[g] += 1
                if len(top) == N:
                    break
        else:
            top = [sid for _, sid in scored[:N]]
        if stats is not None:
            plain = [sid for _, sid in scored[:N]]
            stats.append(
                {
                    "m": m,
                    "swapped": len(set(plain) - set(top)),
                    "largest": Counter(groups[s] for s in plain).most_common(1)[0],
                    "largest_capped": Counter(groups[s] for s in top).most_common(1)[0][1],
                    "size": len(top),
                    "coverage": coverage,
                    "zero_plain": sum(1 for s in plain if nxt not in ret[s]),
                    "zero_capped": sum(1 for s in top if nxt not in ret[s]),
                }
            )
        w = {sid: 1 / len(top) for sid in top}
        turn = sum(abs(w.get(s, 0) - w_prev.get(s, 0)) for s in set(w) | set(w_prev))
        nav *= 1 - turn * COST
        nav *= 1 + sum(w[s] * ret[s].get(nxt, 0.0) for s in top)
        tot = sum(w[s] * (1 + ret[s].get(nxt, 0.0)) for s in top)
        w_prev = {s: w[s] * (1 + ret[s].get(nxt, 0.0)) / tot for s in top}
        navs.append((nxt, nav))
    yrs = len(navs) / 12
    cagr = navs[-1][1] ** (1 / yrs) - 1
    peak, mdd = 1.0, 0.0
    for _, v in navs:
        peak = max(peak, v)
        mdd = max(mdd, 1 - v / peak)
    spy_nav = spym[navs[-1][0]] / spym[months[months.index(navs[0][0]) - 1]]
    worst = min(navs[k][1] / (navs[k - 1][1] if k else 1.0) - 1 for k in range(len(navs)))
    base = spym[months[months.index(navs[0][0]) - 1]]
    spy_peak, spy_dd = 1.0, 0.0
    for d, _ in navs:
        spy_peak = max(spy_peak, spym[d] / base)
        spy_dd = max(spy_dd, 1 - spym[d] / base / spy_peak)
    return cagr, mdd, spy_nav ** (1 / yrs) - 1, worst, spy_dd


COVERED_ONLY = "--covered-only" in sys.argv
print("universe:", "names with a panel ME only" if COVERED_ONLY else "all 500")
RANKS = {m: ranked(m) for m in months if date(2014, 10, 1) <= m <= date(2024, 9, 1) and months.index(m) >= 13}
SPANS = (
    (date(2014, 10, 1), date(2024, 9, 1)),
    (date(2014, 10, 1), date(2019, 12, 1)),
    (date(2019, 12, 1), date(2024, 9, 1)),
)
stats: list = []
run(30, 0.001, True, stats=stats)
cov = sorted(st["coverage"] for st in stats)
print(f"formations: {len(stats)} | universe names with a usable ME: min {cov[0]:.1%} median {cov[len(cov) // 2]:.1%}")
print(
    f"cap binds in {sum(1 for st in stats if st['swapped'])} of {len(stats)} formations; held slots swapped:"
    f" {sum(st['swapped'] for st in stats)}, max {max(st['swapped'] for st in stats)};"
    f" capped books under 30 names: {sum(1 for st in stats if st['size'] < 30)}"
)
print(
    "largest uncapped group per formation (ties: first seen):", Counter(st["largest"][0] for st in stats).most_common(4)
)
print(
    "names in the largest group, mean uncapped / capped:",
    f"{mean(st['largest'][1] for st in stats):.1f} / {mean(st['largest_capped'] for st in stats):.1f};",
    f"max {max(st['largest'][1] for st in stats)} / {max(st['largest_capped'] for st in stats)}",
)
print(
    "held name-months with no next-month return (counted 0%), uncapped / capped:",
    sum(st["zero_plain"] for st in stats),
    "/",
    sum(st["zero_capped"] for st in stats),
    "of",
    30 * len(stats),
)
for N in (30,):
    for COST in (0.001, 0.002):
        for capped in (False, True):
            out = []
            for a, b in SPANS:
                cg, dd, sp, worst, spy_dd = run(N, COST, capped, a, b)
                out.append(
                    f"{a.year}-{b.year}: {cg:6.2%} vs SPY {sp:6.2%} dd {dd:4.1%} (SPY {spy_dd:4.1%}) worst-mo {worst:5.1%}"
                )
            print(f"{'capped' if capped else 'plain '} top{N} cost {COST:.1%} | " + " | ".join(out))
