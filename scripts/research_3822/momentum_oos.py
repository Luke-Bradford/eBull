# ruff: noqa: E501
"""Momentum book out of sample: the #3822 rule on the stretch after the backtest, 2024-10..2026-08 (#2437 item 1).

The backtest (`momentum_backtest.py`) stops at 2024-09 because the survivorship-free Intrader archive does. This runs
the same rule (risk-adjusted 12-1, top 500 by trailing 3-month dollar volume, close >= $5, top 30 equal weight,
monthly, cost per unit turnover) on the PWB `Stocks-Daily-Price@2026-09-09` snapshot.

SURVIVOR-ONLY: the PWB universe is the series linked to an instrument the live book could hold today (tradable,
canonical, US stock; the `momentum_book_job._CANDIDATES_SQL` predicate). A name delisted before 2026-09 is absent, so
the book, the random control and the universe all carry survivorship; SPY does not.

Returns are total returns from `adj_close`. SPY is Yahoo's monthly adjclose (no account; PWB is a Yahoo scrape too),
cross-checked against eToro SPY month-end closes. The last PWB bar is 2026-09-09, so September 2026 is not scored.

Stop rule (#2437, 2026-10-16): if the out-of-sample stretch lags SPY by more than the backtest's worst lag over a
window of the same length, recommend stopping the book. The lag is SPY's cumulative return minus the book's.

Run: PYTHONPATH=. uv run python scripts/research_3822/momentum_oos.py
"""

import json
import math
import random
import sys
import urllib.request
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean, median

import psycopg
from platformdirs import user_data_dir

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS, resolve_stocks_type_id  # noqa: E402

PWB = "paperswithbacktest/Stocks-Daily-Price@2026-09-09"
N, UNIV = 30, 500
SEEDS = 50
OOS = (date(2024, 10, 1), date(2026, 8, 1))
BACKTEST = (date(2014, 11, 1), date(2024, 9, 1))  # holding months; formation from 2014-10, as momentum_backtest.py

INTRADER_SQL = """
with d as (
  select series_id, bar_date, close::float c, coalesce(split_factor,1)::float sf, coalesce(dividend,0)::float dv,
         close::float * coalesce(volume,0)::float dvol, date_trunc('month', bar_date)::date m
  from research_price_daily where series_id = any(%s) and bar_date >= '2012-06-01'
)
select series_id, m, (array_agg(c order by bar_date desc))[1], exp(sum(ln(nullif(sf,0)))), sum(dv), avg(dvol)
from d group by 1, 2
"""

PWB_SQL = """
with live as (
  select i.instrument_id
  from instruments i join exchanges e on e.exchange_id = i.exchange
  where i.is_tradable and e.asset_class = %(asset_class)s and i.instrument_type_id = %(type_id)s
    and i.canonical_instrument_id is null
), s as (
  select distinct on (rs.instrument_id) rs.series_id, rs.instrument_id
  from research_price_series rs join live using (instrument_id)
  where rs.vendor = %(vendor)s
  order by rs.instrument_id, rs.bar_count desc, rs.series_id
), d as (
  select series_id, bar_date, close::float c, adj_close::float a, close::float * coalesce(volume,0)::float dvol,
         date_trunc('month', bar_date)::date m
  from research_price_daily join s using (series_id) where bar_date >= '2012-06-01'
)
select series_id, m, (array_agg(c order by bar_date desc))[1], (array_agg(a order by bar_date desc))[1], avg(dvol)
from d group by 1, 2
"""


def months_apart(a: date, b: date) -> int:
    return (b.year * 12 + b.month) - (a.year * 12 + a.month)


def load_intrader(c):
    link = Path(user_data_dir("eBull")) / "research/security_linkage_3361/2026-10-05-805bc9e6/series"
    linked = []
    for p in link.iterdir():
        doc = json.loads(p.read_text())
        if not doc.get("collision") and doc.get("observations"):
            linked.append(int(p.stem))
    by: dict = defaultdict(dict)  # sid -> m -> (filter close, adv)
    raw: dict = defaultdict(dict)
    for sid, m, lc, sp, dv, adv in c.execute(INTRADER_SQL, (linked,)).fetchall():
        by[sid][m] = (lc, adv or 0.0)
        raw[sid][m] = (lc, sp or 1.0, dv or 0.0)
    ret: dict = defaultdict(dict)
    for sid, mm in raw.items():
        ks = sorted(mm)
        for a, b in zip(ks, ks[1:]):
            if months_apart(a, b) == 1 and mm[a][0] > 0:
                r = (mm[b][0] * mm[b][1] + mm[b][2]) / mm[a][0] - 1
                if -1 < r < 5:
                    ret[sid][b] = r
    spy = dict(
        c.execute(
            "select date_trunc('month', bar_date)::date, (array_agg(adj_close::float order by bar_date desc))[1] "
            "from research_price_daily where series_id = 7694 group by 1"
        ).fetchall()
    )
    return by, ret, spy, len(linked)


def load_pwb(c):
    rows = c.execute(
        PWB_SQL, {"asset_class": US_EQUITY_ASSET_CLASS, "type_id": resolve_stocks_type_id(c), "vendor": PWB}
    ).fetchall()
    by: dict = defaultdict(dict)
    adj: dict = defaultdict(dict)
    for sid, m, lc, a, adv in rows:
        by[sid][m] = (lc, adv or 0.0)
        adj[sid][m] = a
    ret: dict = defaultdict(dict)
    for sid, mm in adj.items():
        ks = sorted(mm)
        for a, b in zip(ks, ks[1:]):
            if months_apart(a, b) == 1 and mm[a] and mm[a] > 0:
                r = mm[b] / mm[a] - 1
                if -1 < r < 5:
                    ret[sid][b] = r
    return by, ret


def yahoo_spy() -> dict:
    """Month -> SPY month-end adjclose, from Yahoo's public chart endpoint (no account)."""
    url = "https://query2.finance.yahoo.com/v8/finance/chart/SPY?period1=1719792000&period2=1792108800&interval=1mo&events=div"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        r = json.load(urllib.request.urlopen(req, timeout=30))["chart"]["result"][0]
        rows = zip(r["timestamp"], r["indicators"]["quote"][0]["close"], r["indicators"]["adjclose"][0]["adjclose"])
    except (KeyError, IndexError, TypeError) as e:
        raise SystemExit(f"Yahoo chart response for SPY is not the expected shape: {e!r}") from e
    out = {}
    for t, cl, a in rows:
        d = datetime.fromtimestamp(t, UTC).date()
        # Monthly bars only (the trailing intramonth row repeats the current month); a null close is skipped, and a
        # scored month it leaves missing fails loudly in main().
        if d.day == 1 and cl is not None and a is not None:
            out[d] = (float(a), float(cl))
    return out


def run(by, ret, start, end, cost, variant="radj", audit=None):
    """Monthly returns of the book over holding months start..end (formation at the prior month end).

    A held name with no return for the holding month earns 0% (the backtest's cash-out); ``audit`` collects each one.
    """
    months = sorted({m for s in by.values() for m in s})
    pos = {m: i for i, m in enumerate(months)}
    out, w_prev = [], {}
    for nxt in [m for m in months if start <= m <= end]:
        idx = pos[nxt] - 1
        m = months[idx]
        if idx < 13:
            continue
        hist = months[idx - 11 : idx]  # m-11..m-1 (skip m)
        cands = []
        for sid, mm in by.items():
            if m not in mm or mm[m][0] < 5:
                continue
            rs = [ret[sid].get(x) for x in hist]
            if any(r is None for r in rs):
                continue
            adv = mean(by[sid].get(x, (0, 0.0))[1] for x in months[idx - 2 : idx + 1])
            cands.append((adv, sid, rs))
        cands.sort(reverse=True)
        uni = cands[:UNIV]
        if variant.startswith("rand"):
            top = random.Random(f"{variant}:{m}").sample([sid for _, sid, _ in uni], N)
        else:
            scored = []
            for _, sid, rs in uni:
                mu = mean(rs)
                sd = math.sqrt(sum((r - mu) ** 2 for r in rs) / (len(rs) - 1)) or 1e-9
                scored.append(((math.prod(1 + r for r in rs) - 1) / sd, sid))
            top = [sid for _, sid in sorted(scored, reverse=True)[:N]]
        w = {sid: 1 / N for sid in top}
        turn = sum(abs(w.get(s, 0) - w_prev.get(s, 0)) for s in set(w) | set(w_prev))
        if audit is not None:
            audit.extend((nxt, s) for s in top if nxt not in ret[s])
        pr = sum(w[s] * ret[s].get(nxt, 0.0) for s in top)
        out.append((nxt, (1 - turn * cost) * (1 + pr) - 1))
        tot = sum(w[s] * (1 + ret[s].get(nxt, 0.0)) for s in top)
        w_prev = {s: w[s] * (1 + ret[s].get(nxt, 0.0)) / tot for s in top}
    return out


def cum(rs) -> float:
    return math.prod(1 + r for r in rs) - 1


def ann(rs) -> float:
    return (1 + cum(rs)) ** (12 / len(rs)) - 1


def max_dd(rs) -> float:
    nav = peak = 1.0
    dd = 0.0
    for r in rs:
        nav *= 1 + r
        peak = max(peak, nav)
        dd = max(dd, 1 - nav / peak)
    return dd


def spy_rets(levels: dict, ms) -> list:
    keys = sorted(levels)
    missing = [m for m in ms if m not in levels or keys.index(m) == 0]
    if missing:
        raise SystemExit(f"SPY levels missing for scored months or their prior month: {missing}")
    return [levels[m] / levels[keys[keys.index(m) - 1]] - 1 for m in ms]


def worst_lag(book, spy, k):
    """Worst (SPY cumulative - book cumulative) over every k-month window, with its window start."""
    return max((cum(spy[i : i + k]) - cum(book[i : i + k]), i) for i in range(len(book) - k + 1))


def main() -> None:
    with psycopg.connect(settings.database_url) as c:
        ib, iret, ispy, nlinked = load_intrader(c)
        pb, pret = load_pwb(c)
        etoro_spy = dict(
            c.execute(
                "select date_trunc('month', price_date)::date, (array_agg(close::float order by price_date desc))[1] "
                "from price_daily where instrument_id = 3000 and price_date >= '2024-09-01' group by 1"
            ).fetchall()
        )
        live_uni = c.execute(
            "select f.formation_month, u.universe from momentum_book_universes u join momentum_book_formations f using (formation_id) order by 1 desc limit 1"
        ).fetchone()
        pwb_instr = {
            r[0]
            for r in c.execute(
                "select distinct instrument_id from research_price_series where vendor = %s and instrument_id is not null",
                (PWB,),
            ).fetchall()
        }

    ys = yahoo_spy()
    ymaps = {m: a for m, (a, _) in ys.items()}
    diffs = [abs(ys[m][1] / etoro_spy[m] - 1) for m in etoro_spy if m in ys and m <= OOS[1]]
    if not diffs:
        raise SystemExit("SPY cross-check: no month overlaps Yahoo and eToro price_daily")
    print(f"Intrader linked series: {nlinked}; PWB live-linked series: {len(pb)}")
    print(f"SPY cross-check, Yahoo close vs eToro month-end close: {len(diffs)} months, max |diff| {max(diffs):.3%}")
    if live_uni:
        uni_ids = live_uni[1] if isinstance(live_uni[1], list) else list(live_uni[1])
        ids = {int(x["instrument_id"]) if isinstance(x, dict) else int(x) for x in uni_ids}
        print(f"live formation {live_uni[0]}: {len(ids)} universe names, {len(ids & pwb_instr)} have a PWB series")

    # 1. Backtest reference: the worst lag over windows the length of the out-of-sample stretch.
    bt = run(ib, iret, *BACKTEST, 0.001)
    bt_ms = [m for m, _ in bt]
    bt_book = [r for _, r in bt]
    bt_spy = spy_rets(ispy, bt_ms)
    print(
        f"\n# backtest {bt_ms[0]:%Y-%m}..{bt_ms[-1]:%Y-%m}, 0.1%/side: book {ann(bt_book):.2%}/yr vs SPY {ann(bt_spy):.2%}/yr"
    )

    cash_outs: list = []
    oos = {cost: run(pb, pret, *OOS, cost, audit=cash_outs if cost == 0.001 else None) for cost in (0.001, 0.002)}
    o_ms = [m for m, _ in oos[0.001]]
    k = len(o_ms)
    for kk in (k, 24):
        lag, i = worst_lag(bt_book, bt_spy, kk)
        print(f"backtest worst {kk}-month lag vs SPY: {lag:+.2%} cumulative (window from {bt_ms[i]:%Y-%m})")
    ref_lag, _ = worst_lag(bt_book, bt_spy, k)

    # 2. Out of sample.
    spy_o = spy_rets(ymaps, o_ms)
    rand = {s: [r for _, r in run(pb, pret, *OOS, 0.001, f"rand{s}")] for s in range(SEEDS)}
    print(f"\n# out of sample {o_ms[0]:%Y-%m}..{o_ms[-1]:%Y-%m} ({k} months), PWB survivor-only")
    for cost, rs in oos.items():
        b = [r for _, r in rs]
        print(f"book {cost:.1%}/side: cum {cum(b):+.2%}, {ann(b):+.2%}/yr, maxDD {max_dd(b):.1%}")
    print(f"held name-months with no holding-month return (earn 0%): {len(cash_outs)} of {30 * k}: {cash_outs}")
    rc = sorted(cum(v) for v in rand.values())
    book = [r for _, r in oos[0.001]]
    print(f"SPY: cum {cum(spy_o):+.2%}, {ann(spy_o):+.2%}/yr, maxDD {max_dd(spy_o):.1%}")
    print(
        f"random-30 ({SEEDS} seeds, 0.1%/side): cum median {median(rc):+.2%}, range {rc[0]:+.2%}..{rc[-1]:+.2%}; book beats {sum(x < cum(book) for x in rc)}/{SEEDS}"
    )
    print("\nyear  months  book    SPY     random-30 median")
    for y in sorted({m.year for m in o_ms}):
        ix = [i for i, m in enumerate(o_ms) if m.year == y]
        by_r = median(cum([rand[s][i] for i in ix]) for s in rand)
        print(f"{y}  {len(ix):>6}  {cum([book[i] for i in ix]):+.2%}  {cum([spy_o[i] for i in ix]):+.2%}  {by_r:+.2%}")
    print("\nmonth    book    SPY")
    for m, b, s in zip(o_ms, book, spy_o):
        print(f"{m:%Y-%m}  {b:+.2%}  {s:+.2%}")

    # 3. Cross-dataset gap: the same survivor-only PWB universe over the backtest decade, against the survivorship-free
    # Intrader result. It mixes survivorship, universe composition and return construction; it does not isolate survivorship.
    pw_bt = [r for _, r in run(pb, pret, *BACKTEST, 0.001)]
    pw_rand = sorted(ann([r for _, r in run(pb, pret, *BACKTEST, 0.001, f"rand{s}")]) for s in range(20))
    in_rand = sorted(ann([r for _, r in run(ib, iret, *BACKTEST, 0.001, f"rand{s}")]) for s in range(20))
    print(f"\n# cross-dataset gap, {BACKTEST[0]:%Y-%m}..{BACKTEST[1]:%Y-%m}, 0.1%/side")
    print(
        f"book: PWB survivor-only {ann(pw_bt):.2%}/yr vs Intrader survivorship-free {ann(bt_book):.2%}/yr -> gap {100 * (ann(pw_bt) - ann(bt_book)):+.2f} pts/yr"
    )
    print(f"random-30 median (20 seeds): PWB {median(pw_rand):.2%}/yr vs Intrader {median(in_rand):.2%}/yr")

    lag = cum(spy_o) - cum(book)
    verdict = "triggers the stop rule" if lag > ref_lag else "does not trigger the stop rule"
    print(
        f"\nstop rule: out-of-sample lag {lag:+.2%} vs backtest worst {k}-month lag {ref_lag:+.2%} -> this survivor-only replay {verdict}"
    )
    # How large a bias in the book's favour would have to be for the rule to fire: the book's cumulative return at which it would lag SPY by ref_lag.
    fire_at = cum(spy_o) - ref_lag
    print(
        f"the rule fires only if the true book returned below {fire_at:+.2%} cumulative ({(1 + fire_at) ** (12 / k) - 1:+.2%}/yr), against {cum(book):+.2%} measured"
    )


if __name__ == "__main__":
    main()
