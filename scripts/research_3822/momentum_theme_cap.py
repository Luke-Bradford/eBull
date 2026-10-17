# ruff: noqa: E501
"""Does a theme-level cap help the #3822 momentum book? Quick test for #3842, same harness as momentum_oos.py.

The 2026-06 formation lost 21.7% in July in the PWB replay (-23.5% in the eToro-close replay on #3822), led by one
theme, AI hardware and semis, which spans FF-12 groups, so the FF-12 sector cap could not target it (#3839). This
groups names by what they did, not by SIC.

Method, fixed before the first run except where marked:
- Returns: daily, over the 126 sessions ending at the formation month's last bar (the half-year window used
  elsewhere here, e.g. Bollinger's 126-session extreme and the #3609 liquidity window; no rule fixes it for clustering). Intrader: (close x split
  factor + dividend) / prior close, the monthly construction of momentum_oos.load_intrader; PWB: `adj_close` ratios.
  A return is recorded only across consecutive calendar sessions (corrected after Codex checkpoint 1).
- Correlation: over each pair's common sessions, with deviations from each name's own full-window mean (not the
  pair's common-session mean, so it approximates Pearson on the overlap); missing sessions are never treated as zero returns (corrected
  after checkpoint 1; the first run zero-filled them). A name with fewer than 100 returns in the window, or none that
  vary, is its own group.
- Distance and grouping: Mantegna's d = sqrt(2(1 - rho)) (Mantegna 1999, Eur. Phys. J. B 11:193). Mantegna's own
  hierarchy is single linkage (the minimum spanning tree); this uses average linkage (Lance-Williams), a choice, cut
  at K = 12 groups. No rule here fixes K; 12 matches the FF-12 group count of the rejected sector cap. K = 6 and
  K = 24 are printed as sensitivities and decide nothing.
- Cap: adapted from the risk skill's "about 2x benchmark weight" (`portfolio-construction-and-risk.md`): at most
  floor(2 x share x 30) names per group, filled down the ranking, where share is the group's NAME share of the
  500-name universe (momentum_sector_cap.py used market-cap share). Floored at 1, a local exception: a group under a
  1/60 share is never excluded outright.
- Decision (#3842): adopt only if the capped book's max drawdown is lower AND its annual return is at least the
  book's, in BOTH the backtest (Intrader, survivorship-free, 2014-11..2024-09) and the 2024-10..2026-08 stretch (PWB,
  survivor-only). That stretch is out of sample for the book but not for this cap, which was designed after its crash.

Both arms share momentum_oos.run(): 0.1% per side, a held name with no holding-month return earns 0%.

Run: PYTHONPATH=. uv run python -W ignore::RuntimeWarning scripts/research_3822/momentum_theme_cap.py
"""

import math
import sys
from collections import Counter, defaultdict
from datetime import date

import numpy as np
import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from scripts.research_3822.momentum_oos import (  # noqa: E402
    BACKTEST,
    OOS,
    N,
    ann,
    load_intrader,
    load_pwb,
    max_dd,
    radj_score,
    run,
)

WINDOW, MIN_OBS, K = 126, 100, 12
COST = 0.001

INTRADER_DAILY_SQL = """
select series_id, bar_date, close::float, coalesce(split_factor,1)::float, coalesce(dividend,0)::float
from research_price_daily where series_id = any(%s) and bar_date between %s and %s order by series_id, bar_date
"""
PWB_DAILY_SQL = """
select series_id, bar_date, adj_close::float, 1.0::float, 0.0::float
from research_price_daily where series_id = any(%s) and bar_date between %s and %s order by series_id, bar_date
"""


def daily_returns(rows) -> tuple[list[date], dict]:
    """series -> {session: return}, and the session calendar (every date any loaded series has a bar on).

    A return is recorded only when the series' previous bar is the calendar's previous session, so a move across a
    missing session is never counted as a one-day return; the session is missing instead.
    """
    rows = list(rows)
    days = sorted({d for _, d, *_ in rows})
    pos = {d: i for i, d in enumerate(days)}
    out: dict = defaultdict(dict)
    prev: dict = {}
    for sid, d, c, sf, dv in rows:
        pd_, p = prev.get(sid, (None, None))
        if p and p > 0 and c is not None and pos[d] - pos[pd_] == 1:
            r = (c * sf + dv) / p - 1
            if -1 < r < 5:
                out[sid][d] = r
        prev[sid] = (d, c)
    return days, out


def average_linkage(dist: np.ndarray, k: int) -> list[int]:
    """Agglomerative average-linkage clustering cut at k groups; returns a group label per point. Pure."""
    n = len(dist)
    d = dist.astype(float).copy()
    np.fill_diagonal(d, np.inf)
    size = np.ones(n)
    label = list(range(n))
    for _ in range(n - k):
        i, j = np.unravel_index(np.argmin(d), d.shape)
        i, j = (i, j) if i < j else (j, i)
        merged = (size[i] * d[i] + size[j] * d[j]) / (size[i] + size[j])  # Lance-Williams, average linkage
        d[i], d[:, i] = merged, merged
        d[i, i] = np.inf
        d[j], d[:, j] = np.inf, np.inf
        size[i] += size[j]
        label = [i if lab == j else lab for lab in label]
    return [int(x) for x in label]


def groups_for(uni_sids, end_day_idx: int, days, rets, k: int, stats: list) -> dict:
    """sid -> group id for one formation's universe."""
    win = days[max(0, end_day_idx - WINDOW + 1) : end_day_idx + 1]
    mat = np.array([[rets[s].get(d, np.nan) for d in win] for s in uni_sids])
    obs = np.isfinite(mat)
    xc = np.where(obs, mat - np.nanmean(np.where(obs, mat, np.nan), axis=1, keepdims=True), 0.0)
    ok = (obs.sum(axis=1) >= MIN_OBS) & ((xc**2).sum(axis=1) > 0)
    idx = np.flatnonzero(ok)
    x, m_ = xc[idx], obs[idx].astype(float)
    # Correlation over each pair's common sessions, deviations from each name's own window mean (approximate Pearson): sum x_i x_j over sessions
    # both observe, over sqrt(sum x_i^2 on j's sessions x sum x_j^2 on i's sessions). Missing sessions are never zeros.
    a = (x**2) @ m_.T
    den = np.sqrt(a * a.T)
    rho = np.clip(np.divide(x @ x.T, den, out=np.zeros_like(den), where=den > 0), -1, 1)
    overlap = m_ @ m_.T
    stats.append((int((~ok).sum()), int(overlap.min()) if len(idx) else 0))
    lab = average_linkage(np.sqrt(2 * (1 - rho)), min(k, len(idx)))
    out = {uni_sids[i]: ("g", lab[p]) for p, i in enumerate(idx)}
    for i in np.flatnonzero(~ok):
        out[uni_sids[i]] = ("solo", uni_sids[i])
    return out


def make_rank(days, rets, k: int, log: list, show=None):
    """A run() rank hook: the book's own score, with names past their group's cap pushed below every other name."""
    day_idx = {d: i for i, d in enumerate(days)}

    def rank(uni, m):
        sids = [sid for _, sid, _ in uni]
        nxt = date(m.year + m.month // 12, m.month % 12 + 1, 1)
        end = max(i for d, i in day_idx.items() if d < nxt)
        stats: list = []
        grp = groups_for(sids, end, days, rets, k, stats)
        share = Counter(grp.values())
        cap = {g: max(1, math.floor(2 * n / len(sids) * N)) for g, n in share.items()}
        scored = sorted(((radj_score(rs), sid) for _, sid, rs in uni), reverse=True)
        held: Counter = Counter()
        picked = []
        for _score, sid in scored:
            g = grp[sid]
            if held[g] < cap[g]:
                held[g] += 1
                picked.append(sid)
            if len(picked) == N:
                break
        uncapped = Counter(grp[sid] for _, sid in scored[:N])
        big = max(share, key=lambda g: share[g])  # the universe's largest group, tracked by identity
        log.append(
            (
                m,
                len(set(picked) - {sid for _, sid in scored[:N]}),
                uncapped[big],
                held[big],
                share[big] / len(sids),
                cap[big],
                sum(1 for g in share if g[0] == "g"),
                stats[0][0],
                stats[0][1],
            )
        )
        if show and m == show[0]:
            for g, n in uncapped.most_common():
                names = [show[1].get(sid, sid) for _, sid in scored[:N] if grp[sid] == g]
                print(
                    f"  {m:%Y-%m} formation, group {g}: universe share {share[g] / len(sids):.0%}, cap {cap[g]}, book {n}: {names}"
                )
        keep = set(picked)
        return [(s if sid in keep else -1e18, sid) for s, sid in scored]

    return rank


def summary(rs) -> str:
    worst = min(rs, key=lambda x: x[1])
    vals = [r for _, r in rs]
    return f"{ann(vals):+.2%}/yr, maxDD {max_dd(vals):.1%}, worst month {worst[1]:+.1%} ({worst[0]:%Y-%m})"


def main() -> None:
    with psycopg.connect(settings.database_url) as c:
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        ib, iret, _, _ = load_intrader(c)
        pb, pret = load_pwb(c)
        # Daily bars only for names that ever enter a 500-name universe: a first pass records the universes.
        seen: dict = {"bt": set(), "oos": set()}

        def recorder(key):
            def rank(uni, m):
                seen[key].update(sid for _, sid, _ in uni)
                return [(radj_score(rs), sid) for _, sid, rs in uni]

            return rank

        book = {
            "bt": run(ib, iret, *BACKTEST, COST, rank=recorder("bt")),
            "oos": run(pb, pret, *OOS, COST, rank=recorder("oos")),
        }
        bt_days, bt_rets = daily_returns(
            c.execute(INTRADER_DAILY_SQL, (sorted(seen["bt"]), date(2014, 1, 1), date(2024, 9, 30)))
        )
        oo_days, oo_rets = daily_returns(
            c.execute(PWB_DAILY_SQL, (sorted(seen["oos"]), date(2024, 1, 1), date(2026, 8, 31)))
        )
        symbols = dict(
            c.execute(
                "select series_id, vendor_symbol from research_price_series where series_id = any(%s)",
                (sorted(seen["oos"]),),
            ).fetchall()
        )

    print(f"universe names: backtest {len(seen['bt'])}, out of sample {len(seen['oos'])}")
    windows = {"bt": (ib, iret, BACKTEST, bt_days, bt_rets), "oos": (pb, pret, OOS, oo_days, oo_rets)}
    verdict = {}
    for k in (K, 6, 24):
        for key, (by, ret, span, days, rets) in windows.items():
            log: list = []
            show = (date(2026, 6, 1), symbols) if key == "oos" and k in (K, 24) else None
            name = "backtest 2014-11..2024-09" if key == "bt" else "out of sample 2024-10..2026-08"
            print(f"\n# K={k}, {name}")
            capped = run(by, ret, *span, COST, rank=make_rank(days, rets, k, log, show))
            b, cp = [r for _, r in book[key]], [r for _, r in capped]
            print(f"book:   {summary(book[key])}")
            print(f"capped: {summary(capped)}")
            print(
                f"cap binds in {sum(x[1] > 0 for x in log)} of {len(log)} formations, displacing {sum(x[1] for x in log)} slots"
            )
            print(
                f"universe's largest group: share mean {np.mean([x[4] for x in log]):.0%}, min {min(x[4] for x in log):.0%}; "
                f"cap {N} (share >= 50%) in {sum(x[5] >= N for x in log)} of {len(log)} formations; "
                f"its book names mean {np.mean([x[2] for x in log]):.1f} -> {np.mean([x[3] for x in log]):.1f}"
            )
            print(
                f"groups per formation {min(x[6] for x in log)}..{max(x[6] for x in log)} plus solo names (mean {np.mean([x[7] for x in log]):.1f}); "
                f"min pair overlap {min(x[8] for x in log)} sessions"
            )
            if key == "oos":
                jul = date(2026, 7, 1)
                print(
                    f"2026-06 formation, held July 2026: book {dict(book[key])[jul]:+.1%}, capped {dict(capped)[jul]:+.1%}"
                )
            if k == K:
                verdict[key] = max_dd(cp) < max_dd(b) and ann(cp) >= ann(b)
                print(
                    f"decision inputs: maxDD book {max_dd(b):.6f} capped {max_dd(cp):.6f}; ann book {ann(b):.6f} capped {ann(cp):.6f}"
                )
    print(
        f"\ndecision rule (K={K}): backtest {'passes' if verdict['bt'] else 'fails'}, out of sample {'passes' if verdict['oos'] else 'fails'} -> {'ADOPT' if all(verdict.values()) else 'do not adopt'}"
    )


if __name__ == "__main__":
    main()
