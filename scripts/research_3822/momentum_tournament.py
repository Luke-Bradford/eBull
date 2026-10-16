# ruff: noqa: E501
"""Momentum-family tournament (#2437 item 2, 2026-10-16): three published price-only signals against the live book.

Every candidate runs through `momentum_oos.run()`: the book's universe (top 500 by trailing 3-month dollar volume,
close >= $5, 11 monthly returns), top 30 equal weight, monthly, 0.1% per unit turnover. Only the ranking changes.

- **52wk:** George & Hwang (2004): price / highest price over the past 12 months, month-end close against the highest
  daily close (split-adjusted, price only). Ties at 1.0 are broken by the book's signal.
- **industry:** Moskowitz & Grinblatt (1999): industries ranked by their equal-weighted 6-month return (months m-5..m,
  no skip), top 3 held. Industry = FF-12 of the SIC (French's committed `Siccodes12`; M&G's own 20-industry table is
  not held). The 30 names come from the top 3 industries by the book's signal. An industry needs 5 universe names.
- **residual:** Blitz, Huij & Martens (2011): each name's 36 monthly excess returns (m-35..m) regressed on the
  Fama-French three factors; score = sum of residuals m-11..m-1 / their standard deviation.

Windows: the survivorship-free Intrader backtest, 2014-11..2024-09 (point-in-time SIC from the #3609 panel), and
the survivor-only PWB stretch 2024-10..2026-08 (current SIC, `instrument_sec_profile`). Each is scored against SPY and
the same 20 random-30 paths. Rule (#2437): a candidate goes to demo only if it beats SPY and the book out of sample.

Run: PYTHONPATH=. uv run python scripts/research_3822/momentum_tournament.py
"""

import gzip
import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import mean, median

import numpy as np
import psycopg
from platformdirs import user_data_dir

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from app.services.factor_book_reference import load_ff12  # noqa: E402
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS, resolve_stocks_type_id  # noqa: E402
from scripts.research_3822.momentum_oos import (  # noqa: E402
    BACKTEST,
    OOS,
    PWB,
    N,
    ann,
    cum,
    max_dd,
    months_apart,
    radj_score,
    run,
    spy_rets,
    yahoo_spy,
)

RESEARCH = Path(user_data_dir("eBull")) / "research"
LINK = RESEARCH / "security_linkage_3361/2026-10-05-805bc9e6/series"
PANEL_RUNS = ("2026-10-09-af7889cc-stageA", "2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f")
SIC_CACHE = Path("/tmp/momentum_tournament/panel_sic.json")
SEEDS = 20
FRENCH_SNAPSHOT = 39  # french_five_factor_monthly, observations to 2026-08-31, fetched 2026-10-04
TOP_INDUSTRIES, MIN_INDUSTRY_NAMES = 3, 5
SPLITS = ((date(2014, 11, 1), date(2019, 12, 1)), (date(2020, 1, 1), date(2024, 9, 1)))

# Month-end close, the month's highest close in month-end split basis (a bar's close divided by the split factors
# that take effect after it in the month), split product, dividends and average dollar volume.
INTRADER_SQL = """
with d as (
  select series_id, bar_date, close::float c, coalesce(split_factor,1)::float sf, coalesce(dividend,0)::float dv,
         close::float * coalesce(volume,0)::float dvol, date_trunc('month', bar_date)::date m
  from research_price_daily where series_id = any(%s) and bar_date >= '2011-06-01'
), e as (
  select *, exp(coalesce(sum(ln(nullif(sf,0))) over (partition by series_id, m order by bar_date desc
                                                    rows between unbounded preceding and 1 preceding), 0)) later_sf
  from d
)
select series_id, m, (array_agg(c order by bar_date desc))[1], max(c / later_sf),
       exp(sum(ln(nullif(sf,0)))), sum(dv), avg(dvol)
from e group by 1, 2
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
  from research_price_daily join s using (series_id) where bar_date >= '2021-06-01'
)
select series_id, m, (array_agg(c order by bar_date desc))[1], max(c), (array_agg(a order by bar_date desc))[1], avg(dvol)
from d group by 1, 2
"""

PWB_SIC_SQL = """
select distinct on (rs.instrument_id) rs.series_id, p.sic
from research_price_series rs join instrument_sec_profile p using (instrument_id)
where rs.vendor = %(vendor)s and p.sic ~ '^[0-9]{1,4}$'
order by rs.instrument_id, rs.bar_count desc, rs.series_id
"""


def contiguous_returns(level: dict, fn) -> dict:
    out: dict = defaultdict(dict)
    for sid, mm in level.items():
        ks = sorted(mm)
        for a, b in zip(ks, ks[1:]):
            if months_apart(a, b) == 1:
                r = fn(mm[a], mm[b])
                if r is not None and -1 < r < 5:
                    out[sid][b] = r
    return out


def load_intrader(c):
    linked = []
    for p in LINK.iterdir():
        doc = json.loads(p.read_text())
        if not doc.get("collision") and doc.get("observations"):
            linked.append(int(p.stem))
    by: dict = defaultdict(dict)
    raw: dict = defaultdict(dict)
    for sid, m, lc, mx, sp, dv, adv in c.execute(INTRADER_SQL, (linked,)).fetchall():
        by[sid][m] = (lc, adv or 0.0)
        raw[sid][m] = (lc, mx, sp or 1.0, dv or 0.0)
    ret = contiguous_returns(raw, lambda a, b: (b[0] * b[2] + b[3]) / a[0] - 1 if a[0] > 0 else None)
    pret = contiguous_returns(raw, lambda a, b: b[0] * b[2] / a[0] - 1 if a[0] > 0 else None)
    # Price-only level per series and each month's high on that level. A missing or filtered return starts a new
    # segment at 1.0; the 52-week ratio only compares months within one segment.
    high: dict = defaultdict(dict)
    for sid, mm in raw.items():
        level, seg = None, 0
        for m in sorted(mm):
            r = pret[sid].get(m)
            if level is not None and r is not None:
                level *= 1 + r
            else:
                level, seg = 1.0, seg + 1
            lc, mx = mm[m][0], mm[m][1]
            high[sid][m] = (level, level * mx / lc if lc > 0 else None, seg)
    spy = dict(
        c.execute(
            "select date_trunc('month', bar_date)::date, (array_agg(adj_close::float order by bar_date desc))[1] "
            "from research_price_daily where series_id = 7694 group by 1"
        ).fetchall()
    )
    return by, ret, high, spy


def load_pwb(c):
    rows = c.execute(
        PWB_SQL, {"asset_class": US_EQUITY_ASSET_CLASS, "type_id": resolve_stocks_type_id(c), "vendor": PWB}
    ).fetchall()
    by: dict = defaultdict(dict)
    raw: dict = defaultdict(dict)
    high: dict = defaultdict(dict)
    for sid, m, lc, mx, a, adv in rows:
        by[sid][m] = (lc, adv or 0.0)
        raw[sid][m] = a
        high[sid][m] = (lc, mx, 0)  # PWB closes are split-adjusted, so they are one level already
    ret = contiguous_returns(raw, lambda a, b: b / a - 1 if a and b and a > 0 else None)
    return by, ret, high


def panel_sic() -> dict:
    """'YYYY-MM' -> series -> point-in-time SIC from the #3609 factor panel (stage B overwrites stage A)."""
    if not SIC_CACHE.exists():
        out: dict = {}
        for panel_run in PANEL_RUNS:
            for line in gzip.open(RESEARCH / "factor_panel_3609" / panel_run / "rows.jsonl.gz", "rt"):
                r = json.loads(line)
                if r.get("series_id") is not None:
                    out.setdefault(r["M"][:7], {})[r["series_id"]] = r.get("sic")
        SIC_CACHE.parent.mkdir(exist_ok=True)
        SIC_CACHE.write_text(json.dumps(out))
    return {k: {int(s): v for s, v in rows.items()} for k, rows in json.loads(SIC_CACHE.read_text()).items()}


def french(c) -> dict:
    """Month -> (Mkt-RF, SMB, HML, RF) from the pinned French five-factor monthly snapshot (its SMB is the five-factor
    construction, not the three-factor file's)."""
    row = c.execute(
        "select dataset_key, parse_status, last_observation from reference_data_snapshots where snapshot_id = %s",
        (FRENCH_SNAPSHOT,),
    ).fetchone()
    if not row or row[0] != "french_five_factor_monthly" or row[1] != "accepted":
        raise SystemExit(f"French snapshot {FRENCH_SNAPSHOT} is not an accepted five-factor monthly snapshot: {row}")
    sid = FRENCH_SNAPSHOT
    f: dict = defaultdict(dict)
    for key, d, v in c.execute(
        "select series_key, observation_date, value::float from reference_data_observations where snapshot_id = %s",
        (sid,),
    ).fetchall():
        f[d.replace(day=1)][key] = v
    return {m: (v["Mkt-RF"], v["SMB"], v["HML"], v["RF"]) for m, v in f.items() if len(v) >= 4}


def window(m: date, k: int) -> list:
    """The k months ending at m, oldest first."""
    out = []
    y, mo = m.year, m.month
    for _ in range(k):
        out.append(date(y, mo, 1))
        y, mo = (y - 1, 12) if mo == 1 else (y, mo - 1)
    return out[::-1]


def rank_52wk(high, diag: list):
    def rank(uni, m):
        out = []
        for _, sid, rs in uni:
            hs = [high[sid].get(x) for x in window(m, 12)]
            if any(h is None or h[1] is None or h[2] != hs[-1][2] for h in hs):
                continue
            out.append(((hs[-1][0] / max(h[1] for h in hs), radj_score(rs)), sid))
        ratios = sorted((score[0] for score, _ in out), reverse=True)
        diag.append((sum(r >= 0.9999 for r in ratios), ratios[29] if len(ratios) >= 30 else None))
        return out

    return rank


def rank_industry(ret, industry_of, coverage: list):
    def rank(uni, m):
        members: dict = defaultdict(list)
        coverage.append(sum(industry_of(sid, m) is not None for _, sid, _ in uni) / len(uni))
        for _, sid, rs in uni:
            ind = industry_of(sid, m)
            r6 = [ret[sid].get(x) for x in window(m, 6)]
            if ind is not None and all(r is not None for r in r6):
                members[ind].append((sid, rs, r6))
        # Industry return = an equal-weighted portfolio of its members, rebalanced monthly, compounded over 6 months.
        scored = sorted(
            (
                (float(np.prod([1 + mean(x[2][k] for x in v) for k in range(6)]) - 1), ind)
                for ind, v in members.items()
                if len(v) >= MIN_INDUSTRY_NAMES
            ),
            reverse=True,
        )
        top = {ind for _, ind in scored[:TOP_INDUSTRIES]}
        return [(radj_score(rs), sid) for ind in top for sid, rs, _ in members[ind]]

    return rank


def rank_residual(ret, ff):
    def rank(uni, m):
        months = window(m, 36)
        if any(x not in ff for x in months):
            raise SystemExit(f"French factors missing for {[x for x in months if x not in ff]}")
        X = np.array([[1.0, *ff[x][:3]] for x in months])
        rf = np.array([ff[x][3] for x in months])
        out = []
        for _, sid, _ in uni:
            ys = [ret[sid].get(x) for x in months]
            if any(y is None for y in ys):
                continue
            y = np.array(ys) - rf
            beta, *_ = np.linalg.lstsq(X, y, rcond=None)
            e = (y - X @ beta)[24:35]  # m-11..m-1
            sd = float(np.std(e, ddof=1)) or 1e-9
            out.append((float(e.sum()) / sd, sid))
        return out

    return rank


def main() -> None:
    ff12 = load_ff12()
    with psycopg.connect(settings.database_url) as c:
        ib, iret, ihigh, ispy = load_intrader(c)
        pb, pret, phigh = load_pwb(c)
        pwb_sic = {sid: int(s) for sid, s in c.execute(PWB_SIC_SQL, {"vendor": PWB}).fetchall()}
        ff = french(c)
    psic = panel_sic()
    panel_months = sorted(psic)
    ys = {m: a for m, (a, _) in yahoo_spy().items()}

    def intrader_industry(sid, m):
        keys = [k for k in panel_months if k <= f"{m:%Y-%m}"]
        sic = psic[keys[-1]].get(sid) if keys else None
        return ff12.industry(sic) if isinstance(sic, int) else None

    def pwb_industry(sid, m):
        sic = pwb_sic.get(sid)
        return ff12.industry(sic) if sic is not None else None

    datasets = {
        "backtest (Intrader, survivorship-free)": (ib, iret, ihigh, intrader_industry, BACKTEST, ispy),
        "out of sample (PWB, survivor-only)": (pb, pret, phigh, pwb_industry, OOS, ys),
    }
    for label, (by, ret, high, ind, (start, end), spy_levels) in datasets.items():
        cands = {
            "book (risk-adj 12-1)": None,
            "52wk high": rank_52wk(high, diag52 := []),
            "industry momentum": rank_industry(ret, ind, coverage := []),
            "residual momentum": rank_residual(ret, ff),
        }
        res, audits = {}, {}
        for name, rank in cands.items():
            turns: list = []
            sizes: list = []
            cash_outs: list = []

            def sized(uni, m, rank=rank, sizes=sizes):
                out = rank(uni, m) if rank else [(radj_score(rs), sid) for _, sid, rs in uni]
                sizes.append(min(N, len(out)))
                return out

            rs = run(by, ret, start, end, 0.001, rank=sized, turns=turns, audit=cash_outs)
            res[name] = (rs, mean(turns[1:]))
            audits[name] = (min(sizes), len(cash_outs), N * len(rs))
        ms = [m for m, _ in res["book (risk-adj 12-1)"][0]]
        spy = spy_rets(spy_levels, ms)
        rand = [[r for _, r in run(by, ret, start, end, 0.001, f"rand{s}")] for s in range(SEEDS)]
        print(f"\n# {label}, {ms[0]:%Y-%m}..{ms[-1]:%Y-%m}, 0.1%/side")
        hdr = "cumulative / per year"
        print(f"{'':24s} {hdr:>22s}  maxDD  turnover/mo  " + "  ".join(f"{a:%Y}-{b:%Y}" for a, b in SPLITS))
        rows = [(n, [r for _, r in rs], t) for n, (rs, t) in res.items()]
        rows += [("SPY", spy, None), ("random-30 median", None, None)]
        for name, rs, t in rows:
            if rs is None:
                cagrs = sorted(ann(r) for r in rand)
                cums = sorted(cum(r) for r in rand)
                print(f"{name:24s} {median(cums):+8.2%} / {median(cagrs):+7.2%}")
                continue
            subs = []
            for a, b in SPLITS:
                ix = [i for i, m in enumerate(ms) if a <= m <= b]
                subs.append(f"{ann([rs[i] for i in ix]):+7.2%}" if ix else "      -")
            tt = f"{t:10.1%}" if t is not None else " " * 10
            print(f"{name:24s} {cum(rs):+8.2%} / {ann(rs):+7.2%}  {max_dd(rs):5.1%}  {tt}  " + "  ".join(subs))
        print(f"universe names with an industry: median {median(coverage):.1%}, minimum {min(coverage):.1%}")
        ties = [d[0] for d in diag52]
        thirtieth = [d[1] for d in diag52 if d[1] is not None]
        print(
            f"52wk high: names at their 12-month closing high per formation {min(ties)}..{max(ties)} (median {median(ties)}); "
            f"30th name's ratio {min(thirtieth):.4f}..{max(thirtieth):.4f}"
        )
        for name, (least, outs, slots) in audits.items():
            print(f"{name}: fewest names held {least}; held name-months with no return (earn 0%) {outs} of {slots}")
        book = cum(rows[0][1])
        for name, rs, _ in rows[1:4]:
            beats_rand = sum(cum(r) < cum(rs) for r in rand)
            print(
                f"{name}: beats SPY {cum(rs) > cum(spy)}, beats book {cum(rs) > book}, beats {beats_rand}/{SEEDS} random paths; "
                f"years beating SPY {sum(cum([rs[i] for i, m in enumerate(ms) if m.year == y]) > cum([spy[i] for i, m in enumerate(ms) if m.year == y]) for y in {m.year for m in ms})}/{len({m.year for m in ms})}"
            )


if __name__ == "__main__":
    main()
