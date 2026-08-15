"""The quant-methods skill's measured panel (#2437, 2026-08-15). Read-only.

Reproduces every MEASURED figure in `.claude/skills/market-technician/quant-methods.md`:

  A. Corwin-Schultz (JF 2012, authors' SAS constants, no overnight adjustment) and
     Abdi-Ranaldo two-day-corrected CHL (RFS 2017, eq. 11) spread ESTIMATES from
     daily bars, each compared against the ACTUAL quoted spread (`quotes.spread_pct`).
  B. Kaufman Efficiency Ratio ER(10) last-bar cross-section.
  C. Parkinson vs close-to-close volatility ratio.

Panel: every instrument present in `quotes` with bid > 0 and ask > bid and at least
WINDOW usable trailing bars in `price_daily` (a usable bar has high/low/close > 0 and
high >= low; bars failing that are dropped, which shortens the window rather than
interpolating). Estimates use the trailing WINDOW bars ending at each instrument's
latest stored bar at run time — the panel therefore drifts as bars accrue, which is
why the skill quotes the run date next to every figure.

Run:  uv run python scripts/verify_2437_quant_methods_panel.py
"""

from __future__ import annotations

import math

import numpy as np
import psycopg

from app.config import settings

WINDOW = 60
ER_PERIOD = 10

#: Corwin-Schultz denominator constant, 3 - 2*sqrt(2), per their own SAS code.
_CS_CONST = 3 - 2 * math.sqrt(2)

_SQL = """
with ranked as (
  select p.instrument_id, p.price_date, p.high, p.low, p.close,
         row_number() over (partition by p.instrument_id order by p.price_date desc) rn
  from price_daily p
  join quotes q on q.instrument_id = p.instrument_id
  where q.bid > 0 and q.ask > q.bid
    and p.high > 0 and p.low > 0 and p.close > 0 and p.high >= p.low
)
select instrument_id, high, low, close from ranked where rn <= %(n)s
order by instrument_id, price_date
"""


def corwin_schultz(highs: np.ndarray, lows: np.ndarray) -> float:
    """Mean over the window's overlapping 2-day pairs; negative pair estimates
    set to zero (the authors' primary daily estimator). ⚠ No overnight
    adjustment — stated in the skill; adding it is a declared change."""
    log_range_sq = np.log(highs / lows) ** 2
    estimates = []
    for t in range(len(highs) - 1):
        beta = log_range_sq[t] + log_range_sq[t + 1]  # plain sum — no 1/2 factor
        gamma = math.log(max(highs[t], highs[t + 1]) / min(lows[t], lows[t + 1])) ** 2
        alpha = (math.sqrt(2 * beta) - math.sqrt(beta)) / _CS_CONST - math.sqrt(gamma / _CS_CONST)
        spread = 2 * (math.exp(alpha) - 1) / (1 + math.exp(alpha))
        estimates.append(max(spread, 0.0))
    return float(np.mean(estimates)) if estimates else float("nan")


def abdi_ranaldo(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    """Two-day-corrected CHL (their eq. 11): mean_t sqrt(max(4(c_t-eta_t)(c_t-eta_{t+1}), 0)),
    eta = (h + l)/2 in log prices."""
    h, lo, c = np.log(highs), np.log(lows), np.log(closes)
    eta = (h + lo) / 2
    vals = 4 * (c[:-1] - eta[:-1]) * (c[:-1] - eta[1:])
    return float(np.sqrt(np.maximum(vals, 0)).mean())


def efficiency_ratio(closes: np.ndarray, n: int) -> float:
    c = closes[-(n + 1) :]
    path = np.abs(np.diff(c)).sum()
    return float(abs(c[-1] - c[0]) / path) if path > 0 else float("nan")


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.corrcoef(np.argsort(np.argsort(a)), np.argsort(np.argsort(b)))[0, 1])


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SQL, {"n": WINDOW}).fetchall()
        quoted_spread = dict(
            conn.execute(
                "select instrument_id, spread_pct from quotes where bid > 0 and ask > bid and spread_pct is not null"
            ).fetchall()
        )

    by_inst: dict[int, list[tuple[float, float, float]]] = {}
    for iid, h, lo, c in rows:
        by_inst.setdefault(iid, []).append((float(h), float(lo), float(c)))

    cs_est, chl_est, quoted, ers, park_ratio = [], [], [], [], []
    for iid, bars in by_inst.items():
        if len(bars) < WINDOW:
            continue
        arr = np.array(bars[-WINDOW:])
        highs, lows, closes = arr[:, 0], arr[:, 1], arr[:, 2]
        er = efficiency_ratio(closes, ER_PERIOD)
        if np.isfinite(er):
            ers.append(er)
        parkinson = np.sqrt(np.mean(np.log(highs / lows) ** 2) / (4 * math.log(2)))
        close_close = np.std(np.diff(np.log(closes)))
        if close_close > 0:
            park_ratio.append(parkinson / close_close)
        if quoted_spread.get(iid) is not None:
            cs = corwin_schultz(highs, lows)
            chl = abdi_ranaldo(highs, lows, closes)
            if np.isfinite(cs) and np.isfinite(chl):
                cs_est.append(cs * 100)
                chl_est.append(chl * 100)
                quoted.append(float(quoted_spread[iid]))

    cs, chl, qt = np.array(cs_est), np.array(chl_est), np.array(quoted)
    top = qt >= np.percentile(qt, 75)
    bottom = qt <= np.percentile(qt, 25)
    print(f"A. spread estimators vs quoted spread_pct: n={len(qt)}")
    for name, est in (("CS ", cs), ("CHL", chl)):
        print(
            f"   {name} spearman={_spearman(est, qt):.3f} pearson={np.corrcoef(est, qt)[0, 1]:.3f}"
            f" median_est={np.median(est):.3f}% (quoted median {np.median(qt):.3f}%)"
            f" top-quartile-mean={est[top].mean():.3f}% bottom={est[bottom].mean():.3f}%"
        )

    e = np.array(ers)
    print(
        f"B. ER({ER_PERIOD}) last-bar cross-section: n={len(e)}"
        f" quartiles={np.round(np.percentile(e, [25, 50, 75]), 4)} share>0.5={(e > 0.5).mean() * 100:.1f}%"
    )

    p = np.array(park_ratio)
    print(
        f"C. Parkinson/close-close sigma ratio: n={len(p)}"
        f" median={np.median(p):.3f} IQR={np.round(np.percentile(p, [25, 75]), 4)}"
    )


if __name__ == "__main__":
    main()
