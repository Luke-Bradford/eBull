# ruff: noqa: E501
import sys
from datetime import date

import psycopg

sys.path.insert(0, ".")
from app.config import settings

SYMS = [
    "SPY",
    "QQQ",
    "RSP",
    "SPMO",
    "XMMO",
    "SPHQ",
    "SPLV",
    "RPV",
    "RPG",
    "VTV",
    "VUG",
    "IWD",
    "IWF",
    "SCHD",
    "IWM",
    "MNA",
    "BTAL",
    "DBMF",
    "KMLM",
    "GLD",
    "TLT",
    "SVOL",
    "AVUV",
    "BIL",
]
with psycopg.connect(settings.database_url) as c:
    ids = dict(
        c.execute(
            "select vendor_symbol, series_id from research_price_series where vendor='icyDenev/Intrader' and vendor_symbol = any(%s)",
            (SYMS,),
        ).fetchall()
    )
    tr = {}
    for s, sid in ids.items():
        rows = c.execute(
            "select bar_date, close::float, coalesce(split_factor,1)::float, coalesce(dividend,0)::float from research_price_daily where series_id=%s order by 1",
            (sid,),
        ).fetchall()
        idx, out = 1.0, {rows[0][0]: 1.0}
        for p, q in zip(rows, rows[1:]):
            idx *= (q[1] * q[2] + q[3]) / p[1]
            out[q[0]] = idx
        tr[s] = out


def stats(s, a, b):
    d = sorted(x for x in tr[s] if a <= x <= b)
    if not d or (d[0] - a).days > 10 or (b - d[-1]).days > 10:
        return None
    v = [tr[s][x] for x in d]
    yrs = (d[-1] - d[0]).days / 365.25
    peak = mdd = 0
    for x in v:
        peak = max(peak, x)
        mdd = max(mdd, 1 - x / peak)
    return (v[-1] / v[0]) ** (1 / yrs) - 1, mdd


for a, b in (
    (date(2006, 4, 1), date(2024, 9, 27)),
    (date(2016, 1, 4), date(2024, 9, 27)),
    (date(2021, 1, 4), date(2024, 9, 27)),
):
    print(f"\n== {a}..{b}   CAGR  maxDD   (vs SPY)")
    base = stats("SPY", a, b)
    assert base is not None
    for s in SYMS:
        r = stats(s, a, b)
        if r:
            print(f"  {s:5s} {r[0]:7.2%} {r[1]:6.1%}  {r[0] - base[0]:+6.2%}")
