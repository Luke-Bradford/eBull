"""Scratch: vol-targeted and trend-filtered market exposure vs buy-and-hold (French daily, snapshot 60).

Rules fixed ex ante from the literature, no tuning:
- vol target (Moreira & Muir 2017 style): weight = min(L, target / sigma_prev), sigma_prev = annualised std of the
  previous 21 daily market returns, target = annualised std of ALL daily returns before the rebalance (expanding,
  no look-ahead). Monthly rebalance.
- trend (Faber 2007): long when the month-end total-return index is above its 10-month average of month-end
  values, else cash. Monthly.
- leverage (w > 1) pays cash + spread on the borrowed part. Cost 5 bp per unit of weight traded at rebalance.
"""

import math
import sys
from datetime import date

import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from scripts.measure_3612_degross_ladder import load_market  # noqa: E402

COST = 0.0005


def run(days, weight_fn, spread):
    idx, nav, peak, mdd = 1.0, 1.0, 1.0, 0.0
    w, month = 0.0, None
    navs = []
    hist = []  # daily market returns seen so far
    me_index = []  # month-end market index values
    yearly = {}
    rets = []
    for i, (d, m, c) in enumerate(days):
        if d.month != month:
            if month is not None:
                me_index.append(idx)
            month = d.month
            new_w = weight_fn(hist, me_index, idx)
            nav *= 1 - abs(new_w - w) * COST
            w = new_w
        borrow = max(w - 1.0, 0.0)
        r = w * m + (1 - w) * c - borrow * spread / 252
        nav *= 1 + r
        rets.append(r - c)
        idx *= 1 + m
        hist.append(m)
        peak = max(peak, nav)
        mdd = max(mdd, 1 - nav / peak)
        yearly.setdefault(d.year, [nav / (1 + r), nav])
        yearly[d.year][1] = nav
        navs.append((d, nav))
    years = (days[-1][0] - days[0][0]).days / 365.25
    cagr = nav ** (1 / years) - 1
    mu = sum(rets) / len(rets)
    sd = math.sqrt(sum((x - mu) ** 2 for x in rets) / (len(rets) - 1))
    worst = min(v[1] / v[0] - 1 for v in yearly.values())
    vol = sd * math.sqrt(252)
    return cagr, vol, mu * 252 / vol, mdd, worst


def buy_hold(hist, me, idx):
    return 1.0


def vol_target(cap):
    def f(hist, me, idx):
        if len(hist) < 252:
            return 1.0
        recent = hist[-21:]
        m = sum(recent) / 21
        s = math.sqrt(sum((x - m) ** 2 for x in recent) / 20) * math.sqrt(252)
        mu = sum(hist) / len(hist)
        tgt = math.sqrt(sum((x - mu) ** 2 for x in hist) / (len(hist) - 1)) * math.sqrt(252)
        return min(cap, tgt / s) if s > 0 else cap

    return f


def trend(hist, me, idx):
    if len(me) < 10:
        return 1.0
    return 1.0 if me[-1] > sum(me[-10:]) / 10 else 0.0


def both(cap):
    vt = vol_target(cap)

    def f(hist, me, idx):
        return vt(hist, me, idx) * trend(hist, me, idx)

    return f


with psycopg.connect(settings.database_url) as conn:
    days = load_market(conn, 60)

rules = [
    ("buy & hold 1.0x", buy_hold),
    ("trend 10m SMA (no lev)", trend),
    ("vol target cap 1.0x", vol_target(1.0)),
    ("vol target cap 1.5x", vol_target(1.5)),
    ("vol target cap 2.0x", vol_target(2.0)),
    ("vol+trend cap 1.5x", both(1.5)),
    ("vol+trend cap 2.0x", both(2.0)),
]
for label, start in (("1927-2021", date(1927, 1, 1)), ("1993-2021", date(1993, 1, 1)), ("2000-2021", date(2000, 1, 1))):
    for spread in (0.03, 0.06):
        print(f"\n== {label}, borrow = cash + {spread:.0%}  (CAGR / vol / Sharpe / maxDD / worst year)")
        # Warm-up: feed full history to rules, but measure from `start`.
        for name, fn in rules:
            # Simulate whole series, then slice by re-running from start with history pre-seeded.
            pre = [d for d in days if d[0] < start]
            post = [d for d in days if d[0] >= start]

            def seeded(hist, me, idx, fn=fn, pre=pre):
                return fn(pre_hist + hist, pre_me + me, idx)

            pre_hist = [m for _, m, _ in pre]
            idx0, pre_me, month = 1.0, [], None
            for d, m, _ in pre:
                if d.month != month:
                    if month is not None:
                        pre_me.append(idx0)
                    month = d.month
                idx0 *= 1 + m
            pre_me = [v / idx0 for v in pre_me]  # rescale so the post index starts at 1.0
            cagr, vol, sh, mdd, worst = run(post, seeded, spread)
            print(f"  {name:26s} {cagr:7.2%} {vol:6.1%} {sh:5.2f} {mdd:7.1%} {worst:7.1%}")


def trend_lev(L):
    def f(hist, me, idx):
        return L * trend(hist, me, idx)

    return f


print("\n\n######## trend filter with leverage while in the market")
rules = [("buy & hold 1.0x", buy_hold)] + [(f"trend x{L}", trend_lev(L)) for L in (1.0, 1.25, 1.5, 2.0)]
for label, start in (("1927-2021", date(1927, 1, 1)), ("1993-2021", date(1993, 1, 1)), ("2000-2021", date(2000, 1, 1))):
    for spread in (0.03, 0.06):
        print(f"\n== {label}, borrow = cash + {spread:.0%}  (CAGR / vol / Sharpe / maxDD / worst year)")
        for name, fn in rules:
            pre = [d for d in days if d[0] < start]
            post = [d for d in days if d[0] >= start]
            pre_hist = [m for _, m, _ in pre]
            idx0, pre_me, month = 1.0, [], None
            for d, m, _ in pre:
                if d.month != month:
                    if month is not None:
                        pre_me.append(idx0)
                    month = d.month
                idx0 *= 1 + m
            pre_me = [v / idx0 for v in pre_me]

            def seeded_lev(hist, me, idx, fn=fn, pre_hist=pre_hist, pre_me=pre_me):
                return fn(pre_hist + hist, pre_me + me, idx)

            cagr, vol, sh, mdd, worst = run(post, seeded_lev, spread)
            print(f"  {name:26s} {cagr:7.2%} {vol:6.1%} {sh:5.2f} {mdd:7.1%} {worst:7.1%}")
