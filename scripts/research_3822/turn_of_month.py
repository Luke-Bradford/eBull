# ruff: noqa: E501
"""Turn-of-month (McConnell & Xu 2008): window = last trading day of month + first 3 of next.
Report mean daily market return in vs out of window per era; and overlay: 1x always + extra 1x inside window
(borrow cash+3% on the extra, 5 bp per unit traded = 4 units/month round trip)."""

import math
import sys

import psycopg

sys.path.insert(0, ".")
from app.config import settings
from scripts.measure_3612_degross_ladder import load_market

with psycopg.connect(settings.database_url) as c:
    days = load_market(c, 60)
    a = c.execute(
        "select bar_date, adj_close::float from research_price_daily where series_id=7694 order by 1"
    ).fetchall()
spy = [(d, p / q - 1, 0.0) for (d, p), (_, q) in zip(a[1:], a[:-1])]


def windows(ds):
    inw = [False] * len(ds)
    for i in range(len(ds)):
        last = i + 1 < len(ds) and ds[i + 1][0].month != ds[i][0].month
        k = 0
        j = i
        while j > 0 and ds[j - 1][0].month == ds[i][0].month:
            j -= 1
            k += 1  # k = index within month
        inw[i] = last or k < 3
    return inw


def stats(ds, lo, hi, label):
    inw = windows(ds)
    sel = [(r, w, c) for (d, r, c), w in zip(ds, inw) if lo <= d.year <= hi]
    i = [r for r, w, _ in sel if w]
    o = [r for r, w, _ in sel if not w]
    mi, mo = sum(i) / len(i), sum(o) / len(o)
    sd = math.sqrt(sum((x - mi) ** 2 for x in i) / (len(i) - 1))
    t = mi / (sd / math.sqrt(len(i)))
    # overlay
    nav = 1.0
    prev = False
    for r, w, c in sel:
        if w != prev:
            nav *= 1 - 0.0005
            prev = w
        L = 2.0 if w else 1.0
        nav *= 1 + L * r - (L - 1) * c - (L - 1) * 0.03 / 252
    bh = 1.0
    for r, _, _ in sel:
        bh *= 1 + r
    yrs = hi - lo + 1
    print(
        f"{label} {lo}-{hi}: in-window {mi * 1e4:5.1f} bp/day (t={t:4.1f}, n={len(i)}), out {mo * 1e4:5.1f} bp/day; "
        f"share of return in window {sum(i) / (sum(i) + sum(o)):5.0%}; B&H {bh ** (1 / yrs) - 1:6.2%}/yr vs overlay {nav ** (1 / yrs) - 1:6.2%}/yr"
    )


for lo, hi in ((1927, 1986), (1987, 2005), (2006, 2021)):
    stats(days, lo, hi, "French mkt")
stats(spy, 2006, 2023, "SPY      ")
stats(spy, 2014, 2023, "SPY      ")
