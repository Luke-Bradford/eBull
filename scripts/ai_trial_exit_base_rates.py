"""Exit map: what (stop k*ATR, target m*R, horizon h) does to RANDOM entries on our universe.

#3471 spec v6 §16.5: this is the supervisor's ``exit_map.py`` (2026-09-29 ~10:15Z), committed
with lint-only changes as the exact DEFINITION of the §16.1 horizon stop floors: each floor is
this script's MAE p50 (in ATR14) rounded UP to 0.5 (5d 1.0 / 10d 1.5 / 20d 2.0 —
``app/services/ai_trial_plan.HORIZON_STOP_FLOOR_ATR``). Its output as measured is checked in
beside it (``ai_trial_exit_base_rates.out``) and test-pinned to the floors
(``tests/test_ai_trial_plan.py``). The floors are a noise HEURISTIC, not a calibrated stop
probability (r1-53).

⚠ Look-ahead, labelled: the universe filter (median close >= $3, median dollar volume >= $1M)
is taken over each name's WHOLE window, so it uses observations after each entry. That is the
floors' method as measured; the §16.5 setup library applies its universe point-in-time instead.

Entry = close of day t (ATR14 at t). Walk t+1..t+h on daily high/low.
Same-day stop+target touch -> counted as STOP (conservative). Otherwise exit at horizon close.
Universe: price >= $3, median dollar volume >= $1M over the window. Entries every 5th session.

Run: ``PYTHONPATH=. uv run python scripts/ai_trial_exit_base_rates.py > scripts/ai_trial_exit_base_rates.out``
"""

import collections
import statistics as st

import numpy as np
import psycopg

from app.config import settings

START, END = "2023-01-01", "2026-09-25"
STOPS = [1.0, 1.5, 2.0, 3.0, 4.0]
RRS = [1.5, 2.0, 3.0]
HORIZONS = [5, 10, 20]
COST_RT_PCT = 0.30  # 0.15%/side stock CFD tariff, spread excluded

with psycopg.connect(settings.database_url) as c:
    rows = c.execute(
        """
        select instrument_id, price_date, high, low, close, volume
        from price_daily
        where price_date between %s and %s and close is not null and high is not null
          and low is not null
        order by instrument_id, price_date
        """,
        (START, END),
    ).fetchall()

series = collections.defaultdict(list)
for iid, d, h, lo, cl, vol in rows:
    series[iid].append((float(h), float(lo), float(cl), float(vol) if vol is not None else np.nan))


def wilder_atr(hi, lo, cl, n=14):
    tr = np.maximum(hi[1:] - lo[1:], np.maximum(abs(hi[1:] - cl[:-1]), abs(lo[1:] - cl[:-1])))
    atr = np.full(len(cl), np.nan)
    if len(tr) < n:
        return atr
    atr[n] = tr[:n].mean()
    for i in range(n + 1, len(cl)):
        atr[i] = (atr[i - 1] * (n - 1) + tr[i - 1]) / n
    return atr


res = collections.defaultdict(list)  # key -> list of (outcome, ret_pct, ret_R)
mae_atr, mfe_atr = collections.defaultdict(list), collections.defaultdict(list)
n_names = 0
for iid, s in series.items():
    if len(s) < 300:
        continue
    arr = np.array(s)
    closes = arr[:, 2]
    dv = closes * arr[:, 3]
    if np.median(closes) < 3 or np.isnan(dv).all() or np.nanmedian(dv) < 1e6:
        continue
    atrs = wilder_atr(arr[:, 0], arr[:, 1], closes)
    n_names += 1
    for t in range(30, len(arr) - 21, 5):
        c0, atr = closes[t], atrs[t]
        if c0 < 3 or not atr > 0 or atr / c0 > 0.25:
            continue
        for h in HORIZONS:
            win = arr[t + 1 : t + 1 + h]
            mae_atr[h].append((c0 - win[:, 1].min()) / atr)
            mfe_atr[h].append((win[:, 0].max() - c0) / atr)
            for k in STOPS:
                stop = c0 - k * atr
                for m in RRS:
                    tgt = c0 + m * k * atr
                    out, px = "time", win[-1, 2]
                    for hi, lo in win[:, :2]:
                        if lo <= stop:
                            out, px = "stop", stop
                            break
                        if hi >= tgt:
                            out, px = "target", tgt
                            break
                    r = (px - c0) / c0 * 100
                    res[(h, k, m)].append((out, r, (px - c0) / (k * atr)))

print(f"names={n_names} window={START}..{END}")
for h in HORIZONS:
    a, f = np.array(mae_atr[h]), np.array(mfe_atr[h])
    print(
        f"\nhorizon {h}d  n={len(a)}  MAE(ATR) p50={np.median(a):.2f} p80={np.percentile(a, 80):.2f} "
        f"p90={np.percentile(a, 90):.2f} | MFE(ATR) p50={np.median(f):.2f} p80={np.percentile(f, 80):.2f}"
    )
    print(" stop  RR | %stop %tgt %time | avg win%  avg loss% | mean gross%  mean net%  mean R")
    for k in STOPS:
        for m in RRS:
            v = res[(h, k, m)]
            outs = collections.Counter(o for o, _, _ in v)
            n = len(v)
            wins = [r for _, r, _ in v if r > 0]
            losses = [r for _, r, _ in v if r <= 0]
            mean = st.fmean(r for _, r, _ in v)
            pct = {o: outs[o] / n * 100 for o in ("stop", "target", "time")}
            print(
                f" {k:>3.1f} {m:>4.1f} | {pct['stop']:5.1f} {pct['target']:5.1f} {pct['time']:5.1f} "
                f"| {st.fmean(wins):7.2f} {st.fmean(losses):8.2f} "
                f"| {mean:9.2f} {mean - COST_RT_PCT:9.2f} {st.fmean(x for _, _, x in v):7.2f}"
            )
