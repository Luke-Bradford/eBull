# ruff: noqa: E501
"""SPY 1993-2026: research adj_close to 2024-09-27, then eToro SPY (3000) close + 1.25%/yr dividend accrual.
Faber 10-month SMA trend, monthly, cash earns 0 (conservative), borrow cost on L>1 = 5%/yr flat, 5 bp per unit traded."""

import sys
from datetime import date

import psycopg

sys.path.insert(0, ".")
from app.config import settings

with psycopg.connect(settings.database_url) as c:
    a = c.execute(
        "select bar_date, adj_close::float from research_price_daily where series_id=7694 order by 1"
    ).fetchall()
    b = c.execute(
        "select price_date, close::float from price_daily where instrument_id=3000 and price_date > %s order by 1",
        (a[-1][0],),
    ).fetchall()
rets = [(d, p / q - 1) for (d, p), (_, q) in zip(a[1:], a[:-1])]
prev = a[-1][1]
# bridge: eToro close on the research end date
bridge = None
with psycopg.connect(settings.database_url) as c:
    bridge = c.execute(
        "select close::float from price_daily where instrument_id=3000 and price_date=%s", (a[-1][0],)
    ).fetchone()
assert bridge, "no eToro close on the splice date"
prev = bridge[0]
for d, p in b:
    rets.append((d, p / prev - 1 + 0.0125 / 252))
    prev = p


def run(L, start, end, trend_on, borrow=0.05):
    idx = nav = peak = 1.0
    mdd = 0.0
    w = 0.0
    month = None
    me = []
    yearly = {}
    n0 = None
    for d, r in rets:
        if d.month != month:
            if month is not None:
                me.append(idx)
            month = d.month
            if d >= start:
                up = (len(me) < 10) or (me[-1] > sum(me[-10:]) / 10)
                nw = L * (1.0 if (up or not trend_on) else 0.0)
                nav *= 1 - abs(nw - w) * 0.0005
                w = nw
        idx *= 1 + r
        if d < start or d > end:
            continue
        if n0 is None:
            n0 = d
        nav *= 1 + w * r - max(w - 1, 0) * borrow / 252
        peak = max(peak, nav)
        mdd = max(mdd, 1 - nav / peak)
        yearly.setdefault(d.year, [nav, nav])
        yearly[d.year][1] = nav
    yrs = (end - n0).days / 365.25
    return nav ** (1 / yrs) - 1, mdd, {y: v[1] / v[0] - 1 for y, v in yearly.items()}


for s, e in ((date(1994, 1, 1), date(2026, 10, 14)), (date(2022, 1, 1), date(2026, 10, 14))):
    print(f"\n== {s}..{e}  CAGR / maxDD")
    for name, L, t in (("SPY buy&hold", 1.0, False), ("trend x1.0", 1.0, True), ("trend x1.5", 1.5, True)):
        cg, dd, yr = run(L, s, e, t)
        print(f"  {name:14s} {cg:7.2%} {dd:7.1%}")
_, _, bh = run(1.0, date(2019, 1, 1), date(2026, 10, 14), False)
_, _, t1 = run(1.0, date(2019, 1, 1), date(2026, 10, 14), True)
_, _, t15 = run(1.5, date(2019, 1, 1), date(2026, 10, 14), True)
print("\nyear   SPY    trend1.0  trend1.5")
for y in sorted(bh):
    print(f"{y}  {bh[y]:7.1%}  {t1[y]:7.1%}  {t15[y]:7.1%}")
