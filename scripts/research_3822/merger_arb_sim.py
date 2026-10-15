# ruff: noqa: E501
"""Merger-arbitrage quick test v2. Rules fixed before results (see ma_sim.py header) plus:
- CIK -> price series via the #3361 Form 3/4/5 linkage (series whose observations carry the CIK), then Form 25 symbol,
  then submissions tickers. The series must have a bar within 5 days after the announcement and close >= $1.
- Announcement: tender = earliest SC 14D9-C/SC TO-C by the CIK within 45d before its first SC 14D9 (else the SC 14D9
  date); merger = earliest DEFA14A/425 within 90d before its first PREM14A/PREM14C (else that date).
- Returns split-adjusted (close*split/prev close) plus dividends.
"""

import json
import math
import sys
from bisect import bisect_right
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median

import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402

S = sys.argv[1]
COST = float(sys.argv[2])
SLOTS = int(sys.argv[3])
KIND = sys.argv[4]  # tender | merger | both
HOLD = 365
START, END, DATA_END = date(2014, 10, 1), date(2024, 6, 30), date(2024, 9, 27)
LINK = Path.home() / "Library/Application Support/eBull/research/security_linkage_3361/2026-10-05-805bc9e6/series"

filings = json.load(open(f"{S}/ma_filings.json"))


def deals_of(cik, rows):
    out = []
    tender_trig = [date.fromisoformat(f) for fm, f, _ in rows if fm == "SC 14D9"]
    tender_pre = [date.fromisoformat(f) for fm, f, _ in rows if fm in ("SC 14D9-C", "SC TO-C")]
    merger_trig = [date.fromisoformat(f) for fm, f, _ in rows if fm in ("PREM14A", "PREM14C")]
    merger_pre = [date.fromisoformat(f) for fm, f, _ in rows if fm in ("DEFA14A", "425")]
    last = None
    events = sorted([(d, "tender") for d in tender_trig] + [(d, "merger") for d in merger_trig])
    for d, kind in events:
        if last and (d - last).days <= HOLD:
            continue
        pre = tender_pre if kind == "tender" else merger_pre
        win = 45 if kind == "tender" else 90
        early = [p for p in pre if d - timedelta(days=win) <= p <= d]
        ann = min(early) if early else d
        if not (START <= ann <= END):
            continue
        last = d
        out.append((cik, ann, kind))
    return out


deals = [x for cik, d in filings.items() for x in deals_of(cik, d["filings"])]
if KIND != "both":
    deals = [x for x in deals if x[2] == KIND]
print(f"{KIND} deal events: {len(deals)}")

cik_series = defaultdict(set)
for p in LINK.iterdir():
    doc = json.loads(p.read_text())
    if doc.get("collision"):
        continue
    for o in doc.get("observations", []):
        cik_series[o["cik"]].add(int(p.stem))

with psycopg.connect(settings.database_url) as c:
    meta = {
        sid: (sym, fb, lb)
        for sid, sym, fb, lb in c.execute(
            "select series_id, vendor_symbol, first_bar, last_bar from research_price_series where vendor='icyDenev/Intrader'"
        )
    }
    by_sym = {v[0]: k for k, v in meta.items()}
    f25 = defaultdict(list)
    for cik, sym in c.execute(
        "select lpad(issuer_cik,10,'0'), resolved_symbol from sec_form25_common_equity_delistings where resolved_symbol is not null"
    ):
        f25[cik].append(sym)
    spy_rows = c.execute(
        "select bar_date, adj_close::float from research_price_daily where series_id=7694 order by 1"
    ).fetchall()

    trades, miss = [], defaultdict(int)
    for cik, ann, kind in deals:
        cands = list(cik_series.get(cik, ())) + [
            by_sym[s] for s in f25.get(cik, []) + filings[cik]["tickers"] if s in by_sym
        ]
        hit = None
        for sid in dict.fromkeys(cands):
            sym, fb, lb = meta[sid]
            if fb > ann or lb <= ann:
                continue
            bars = c.execute(
                "select bar_date, close::float, coalesce(split_factor,1)::float, coalesce(dividend,0)::float "
                "from research_price_daily where series_id=%s and bar_date > %s and bar_date <= %s order by 1",
                (sid, ann, min(ann + timedelta(days=HOLD + 10), DATA_END)),
            ).fetchall()
            if not bars or (bars[0][0] - ann).days > 5 or bars[0][1] < 1.0:
                continue
            hit = (sym, lb, bars)
            break
        if not hit:
            miss["no_series" if not cands else "no_price_match"] += 1
            continue
        sym, lb, bars = hit
        entry = bars[0][0]
        limit = entry + timedelta(days=HOLD)
        ended = lb < DATA_END - timedelta(days=5) and lb <= limit
        path_raw = [b for b in bars if b[0] <= (lb if ended else limit)]
        idx, path = 1.0, [(entry, 1.0)]
        for prev, cur in zip(path_raw, path_raw[1:]):
            idx *= (cur[1] * cur[2] + cur[3]) / prev[1]
            path.append((cur[0], idx))
        trades.append(
            dict(
                sym=sym,
                kind=kind,
                entry=entry,
                exit=path[-1][0],
                ret=idx - 1 - COST,
                ended=ended,
                days=(path[-1][0] - entry).days,
                path=path,
            )
        )
print("matched:", len(trades), dict(miss))

for k in ("tender", "merger"):
    ts = [t for t in trades if t["kind"] == k]
    if not ts:
        continue
    print(f"\n[{k}] trades {len(ts)}, ended within {HOLD}d: {sum(t['ended'] for t in ts) / len(ts):.0%}")
    for lab, sub in (
        ("all", ts),
        ("ended", [t for t in ts if t["ended"]]),
        ("not ended", [t for t in ts if not t["ended"]]),
    ):
        if sub:
            r = [t["ret"] for t in sub]
            print(
                f"  {lab:10s} n={len(sub):4d} mean {mean(r):7.2%} median {median(r):7.2%} win {sum(x > 0 for x in r) / len(r):4.0%} "
                f"p5 {sorted(r)[len(r) // 20]:7.2%} worst {min(r):7.2%} mean days {mean(t['days'] for t in sub):4.0f}"
            )

cal = [d for d, _ in spy_rows if START <= d <= DATA_END]
spy = dict(spy_rows)
events = sorted(trades, key=lambda t: t["entry"])
px = {id(t): ([d for d, _ in t["path"]], [v for _, v in t["path"]]) for t in trades}


def mark(t, d):
    ds, vs = px[id(t)]
    return vs[max(bisect_right(ds, d) - 1, 0)]


cash, open_pos, navs, ei, skipped, invested = 1.0, [], [], 0, 0, []
for d in cal:
    keep = []
    for t, u in open_pos:
        if d >= t["exit"]:
            cash += u * t["path"][-1][1] * (1 - COST / 2)
        else:
            keep.append((t, u))
    open_pos = keep
    nav = cash + sum(u * mark(t, d) for t, u in open_pos)
    while ei < len(events) and events[ei]["entry"] <= d:
        t = events[ei]
        ei += 1
        if len(open_pos) < SLOTS and nav / SLOTS <= cash:
            cash -= nav / SLOTS
            open_pos.append((t, nav / SLOTS * (1 - COST / 2)))
        else:
            skipped += 1
    nav = cash + sum(u * mark(t, d) for t, u in open_pos)
    invested.append(1 - cash / nav)
    navs.append((d, nav))

yrs = (cal[-1] - cal[0]).days / 365.25
cagr = navs[-1][1] ** (1 / yrs) - 1
peak = mdd = 0
for _, v in navs:
    peak = max(peak, v)
    mdd = max(mdd, 1 - v / peak)
pr = [navs[i][1] / navs[i - 1][1] - 1 for i in range(1, len(navs))]
sr = [spy[cal[i]] / spy[cal[i - 1]] - 1 for i in range(1, len(cal))]
mp, ms = mean(pr), mean(sr)
cov = sum((a - mp) * (b - ms) for a, b in zip(pr, sr)) / (len(pr) - 1)
vp, vs = (math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) for xs, m in ((pr, mp), (sr, ms)))
print(
    f"\nPORTFOLIO [{KIND}] {SLOTS} slots, cost {COST:.1%}: CAGR {cagr:.2%}, vol {vp * math.sqrt(252):.1%}, maxDD {mdd:.1%}, "
    f"beta {cov / vs**2:.2f}, corr {cov / (vp * vs):.2f}, avg invested {mean(invested):.0%}, skipped {skipped}"
)
print(f"SPY CAGR {(spy[cal[-1]] / spy[cal[0]]) ** (1 / yrs) - 1:.2%}")
yearly = {}
for d, v in navs:
    yearly.setdefault(d.year, [v, v])
    yearly[d.year][1] = v
print("years:", " ".join(f"{y}:{b / a - 1:+.1%}" for y, (a, b) in yearly.items()))
