"""Run the cost-aware viability gate over stored results, in the documented order.

Encodes `.claude/skills/quant/cost-aware-viability.md` so the rules are EXECUTED
rather than remembered. Read-only.

⚠ Reads `expectancy_per_trade_pct` and `profit_factor` to decide, never `cagr_pct`
or annualised `sharpe`. On 2026-08-21 s8 posted +45.0% CAGR and Sortino 4.16 on a
per-trade expectancy of -0.96% — skew 36.5, kurtosis 1,976. The compounded figures
were real and were the wrong ones to read.

Usage:
    PYTHONPATH=. uv run python -m scripts.check_strategy_viability [--since-hours N]
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.cost_model import BANDS

#: Novy-Marx & Velikov (2016), RFS 29(1). Turnover above this rarely survives costs.
MAX_MONTHLY_TURNOVER_PCT = 50.0

_SQL = """
SELECT strategy_id,
       avg(turnover_annualised)       AS turnover,
       avg(expectancy_per_trade_pct)  AS expectancy,
       avg(profit_factor)             AS profit_factor,
       avg(dsr_trade_sharpe)          AS trade_sharpe,
       avg(dsr_expected_max_sharpe)   AS bar,
       avg(cagr_pct)                  AS cagr,
       avg(dsr_skewness)              AS skew,
       avg(dsr_kurtosis)              AS kurtosis
FROM strategy_results_store
WHERE created_at > now() - make_interval(hours => %(hours)s)
  AND namespace = 'hold_out'
GROUP BY 1
ORDER BY 3 DESC
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-hours", type=int, default=24)
    args = parser.parse_args()

    cheapest = min(band.p75_spread_pct for band in BANDS)
    dearest = max(band.p75_spread_pct for band in BANDS)

    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SQL, {"hours": args.since_hours}).fetchall()

    if not rows:
        print(f"no hold_out results in the last {args.since_hours}h")
        return 1

    print(f"round-trip cost band: {cheapest}% (>=$100) .. {dearest}% (<$5)\n")
    header = f"{'strategy':36}{'turn/mo':>9}{'exp%/trade':>12}{'PF':>7}{'tradeSR':>9}{'bar':>8}  verdict"
    print(header)
    print("-" * (len(header) + 34))

    viable = 0
    for sid, turnover, expectancy, pf, trade_sr, bar, cagr, skew, kurt in rows:
        per_month = float(turnover) / 12 * 100
        refusals: list[str] = []
        # 1. turnover — the pre-backtest gate
        if per_month > MAX_MONTHLY_TURNOVER_PCT:
            refusals.append(f"turnover {per_month:.0f}%/mo > {MAX_MONTHLY_TURNOVER_PCT:.0f}%")
        # 2. break-even — one trade must clear one round trip
        if Decimal(str(expectancy)) <= 0:
            refusals.append(f"expectancy {float(expectancy):+.2f}%/trade <= 0")
        # 3. profit factor
        if float(pf) < 1.0:
            refusals.append(f"profit factor {float(pf):.2f} < 1.0")
        # 4. deflation
        if trade_sr is not None and bar is not None and float(trade_sr) <= float(bar):
            refusals.append(f"trade Sharpe {float(trade_sr):+.3f} <= bar {float(bar):.3f}")

        verdict = "VIABLE" if not refusals else "; ".join(refusals)
        viable += not refusals
        print(
            f"{sid:36}{per_month:>8.0f}%{float(expectancy):>12.2f}{float(pf):>7.2f}"
            f"{float(trade_sr):>9.3f}{float(bar):>8.3f}  {verdict}"
        )
        # ⚠ Say it out loud when the compounded figures disagree with the per-trade
        # ones — that disagreement IS the trap this script exists for.
        if float(cagr) > 0 and float(expectancy) <= 0:
            print(
                f"{'':36}⚠ CAGR {float(cagr):+.1f}% is positive on a NEGATIVE per-trade edge "
                f"(skew {float(skew):.0f}, kurtosis {float(kurt):.0f}) — a right-tail payoff, not an edge"
            )

    print(f"\n{viable}/{len(rows)} viable")
    return 0 if viable else 2


if __name__ == "__main__":
    sys.exit(main())
