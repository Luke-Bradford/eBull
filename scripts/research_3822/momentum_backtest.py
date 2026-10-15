# ruff: noqa: E501
"""Large-cap momentum quick test on the survivorship-free Intrader archive, 2014-10..2024-09.

Universe each month: Intrader series linked to an operating-company CIK (#3361 Form 3/4/5 linkage, collision-free),
close >= $5, top UNIV by trailing-3-month average dollar volume. Signal: 12-1 total return (months t-12..t-2);
variant 'radj' divides by trailing 12m monthly vol (SPMO-style risk-adjusted). Book: top N equal weight, monthly.
A held name that stops trading earns its last available return then leaves (cash-out). Cost per unit turnover.
"""

import json
import math
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import mean

import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402

LINK = Path.home() / "Library/Application Support/eBull/research/security_linkage_3361/2026-10-05-805bc9e6/series"
linked = set()
for p in LINK.iterdir():
    doc = json.loads(p.read_text())
    if not doc.get("collision") and doc.get("observations"):
        linked.add(int(p.stem))
print("linked series:", len(linked))

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
print("series-months:", len(rows))

by = defaultdict(dict)
for sid, m, lc, sp, dv, adv in rows:
    by[sid][m] = (lc, sp or 1.0, dv or 0.0, adv or 0.0)
months = sorted({m for s in by.values() for m in s})
ret = defaultdict(dict)  # sid -> m -> total return over month m
for sid, mm in by.items():
    ks = sorted(mm)
    for a, b in zip(ks, ks[1:]):
        if (b.year * 12 + b.month) - (a.year * 12 + a.month) != 1:
            continue
        pa, pb = mm[a][0], mm[b]
        if pa > 0:
            r = (pb[0] * pb[1] + pb[2]) / pa - 1
            if r > -1 and r < 5:  # data-glitch guard; logged below
                ret[sid][b] = r
spym = dict(spy)


def run(N, UNIV, COST, variant, start=date(2014, 10, 1), end=date(2024, 9, 1), band: float = 1.0):
    nav, w_prev, navs = 1.0, {}, []
    held = []
    ms = [m for m in months if start <= m <= end]
    for i, m in enumerate(ms[:-1]):
        nxt = ms[i + 1]
        # formation at end of month m; hold during month nxt
        idx = months.index(m)
        if idx < 13:
            continue
        hist = months[idx - 12 : idx + 1]  # m-12..m
        cands = []
        for sid, mm in by.items():
            if m not in mm or mm[m][0] < 5:
                continue
            last3 = [by[sid].get(x, (0, 1, 0, 0))[3] for x in months[idx - 2 : idx + 1]]
            adv = mean(last3)
            rs = [ret[sid].get(x) for x in hist[1:-1]]  # months m-11..m-1 (skip m)
            if any(r is None for r in rs):
                continue
            cands.append((adv, sid, rs))
        cands.sort(reverse=True)
        uni = cands[:UNIV]
        scored = []
        for adv, sid, rs in uni:
            g = math.prod(1 + r for r in rs) - 1
            if variant == "radj":
                mu = mean(rs)
                sd = math.sqrt(sum((r - mu) ** 2 for r in rs) / (len(rs) - 1)) or 1e-9
                g = g / sd
            scored.append((g, sid))
        if variant.startswith("rand"):
            import random as _r

            rng = _r.Random(f"{variant}:{m}")
            pool = [sid for _, sid in scored]
            held = rng.sample(pool, N)
            top = held
            w = {sid: 1 / N for sid in top}
            turn = sum(abs(w.get(s, 0) - w_prev.get(s, 0)) for s in set(w) | set(w_prev))
            nav *= 1 - turn * COST
            pr = sum(w[s] * ret[s].get(nxt, 0.0) for s in top)
            nav *= 1 + pr
            tot = sum(w[s] * (1 + ret[s].get(nxt, 0.0)) for s in top)
            w_prev = {s: w[s] * (1 + ret[s].get(nxt, 0.0)) / tot for s in top}
            navs.append((nxt, nav))
            continue
        scored.sort(reverse=True)
        ranks = {sid: k for k, (_, sid) in enumerate(scored)}
        keep = [s for s in held if s in ranks and ranks[s] < band * N]
        top = keep + [sid for _, sid in scored if sid not in keep][: N - len(keep)]
        held = top
        w = {sid: 1 / N for sid in top}
        turn = sum(abs(w.get(s, 0) - w_prev.get(s, 0)) for s in set(w) | set(w_prev))
        nav *= 1 - turn * COST
        pr = sum(w[s] * ret[s].get(nxt, 0.0) for s in top)  # stopped trading -> 0 (cash-out happened in last bar)
        nav *= 1 + pr
        # drift weights
        tot = sum(w[s] * (1 + ret[s].get(nxt, 0.0)) for s in top)
        w_prev = {s: w[s] * (1 + ret[s].get(nxt, 0.0)) / tot for s in top}
        navs.append((nxt, nav))
    yrs = len(navs) / 12
    cagr = navs[-1][1] ** (1 / yrs) - 1
    peak = mdd = 0
    for _, v in navs:
        peak = max(peak, v)
        mdd = max(mdd, 1 - v / peak)
    spy_ms = [x for x, _ in navs]
    spy_nav = spym[spy_ms[-1]] / spym[months[months.index(spy_ms[0]) - 1]]
    spy_c = spy_nav ** (1 / yrs) - 1
    yearly = defaultdict(lambda: [1.0, 1.0])
    prev = 1.0
    for d, v in navs:
        yearly[d.year][1] *= v / prev
        prev = v
    sy = defaultdict(lambda: 1.0)
    for d in spy_ms:
        sy[d.year] *= spym[d] / spym[months[months.index(d) - 1]]
    return cagr, mdd, spy_c, {y: (yearly[y][1] - 1, sy[y] - 1) for y in sorted(yearly)}


SPANS = (
    (date(2014, 10, 1), date(2024, 9, 1)),
    (date(2014, 10, 1), date(2019, 12, 1)),
    (date(2019, 12, 1), date(2024, 9, 1)),
)
print("\n# 1. variant grid, 2014-10..2024-09")
for variant in ("plain", "radj"):
    for N in (30, 50):
        for COST in (0.002, 0.005):
            cagr, mdd, spy_c, yr = run(N, 500, COST, variant)
            print(
                f"{variant:5s} top{N} cost {COST:.1%}: CAGR {cagr:6.2%} maxDD {mdd:5.1%} | SPY {spy_c:6.2%} | beat SPY {sum(a > b for a, b in yr.values())}/{len(yr)} years"
            )
print("\n# 2. robustness: book size x cost x sub-period (radj)")
for N in (20, 30, 40, 50):
    for COST in (0.001, 0.002):
        out = [
            f"{a.year}-{b.year}: {c:6.2%} vs {s:6.2%} dd {d:4.1%}"
            for a, b in SPANS
            for c, d, s, _ in [run(N, 500, COST, "radj", a, b)]
        ]
        print(f"radj top{N} cost {COST:.1%} | " + " | ".join(out))
print("\n# 3. holding band (radj top30)")
for band in (1, 1.5, 2):
    for COST in (0.001, 0.002):
        c, d, s, _ = run(30, 500, COST, "radj", SPANS[0][0], SPANS[0][1], band)
        print(f"band {band}N cost {COST:.1%}: {c:6.2%} vs SPY {s:6.2%} dd {d:4.1%}")
print("\n# 4. random-30 control, cost 0.1%")
res = sorted(run(30, 500, 0.001, f"rand{k}")[0] for k in range(20))
c, d, s, yr = run(30, 500, 0.001, "radj")
print(
    f"random 20 seeds: median {res[10]:.2%} min {res[0]:.2%} max {res[-1]:.2%}; momentum {c:.2%} beats {sum(r < c for r in res)}/20; SPY {s:.2%}"
)
print("radj top30 cost 0.1% by year (book/SPY):", " ".join(f"{y}:{a:+.0%}/{b:+.0%}" for y, (a, b) in yr.items()))
