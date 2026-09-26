"""Recorded round-trip spread by Amihud-illiquidity tercile, ≥ $5 names (#3387 route-A spec).

The operator's route-A bar (#2437, 2026-09-26) is net per-trade expectancy ≥ 1% after the tariff and the
RECORDED spread. This prints the recorded spread that bar is set against, for the domain the hunt-1 trial
selects from, overall and by the frozen cost band the harness would charge.

Domain (APPROXIMATES the trial's, a construction): validated-universe names whose latest bar is on ``--as-of``,
with 21 bars in the 60 days to it (20 returns, every close and volume > 0), close ≥ ``--floor``. Unlike the trial
it checks no session gaps and no OHLC validity, and breaks ties by instrument_id rather than series_id.
Illiquidity = Amihud (2002): mean of |r| / (close × volume) over those 20 returns. Terciles are cut over the
whole domain BEFORE any spread is joined (ties broken by instrument_id, never by spread); the top third by
illiquidity is "illiquid". Spread = 100 × (ask − bid) / mid from ONE perishables snapshot (``--snapshot``), so
each instrument has at most one capture; coverage per tercile is printed.

⚠ Reproducibility is partial: the snapshot's rows are append-only (sql/426) but the validated universe and
``price_daily`` can change. The printed domain size is a drift tell, not a guarantee.

    PYTHONPATH=. uv run python -m scripts.hunt_recorded_spread_census
"""

from __future__ import annotations

import argparse
import statistics
from datetime import date
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.cost_model import cost_band_for
from app.services.strategies.validated_universe import load_validated_universe

_DOMAIN_SQL = """
with bars as (
  select instrument_id, price_date, close, volume,
         row_number() over (partition by instrument_id order by price_date desc) as k,
         lag(close) over (partition by instrument_id order by price_date) as prev_close
  from price_daily
  where instrument_id = any(%(ids)s) and price_date <= %(as_of)s and price_date >= %(as_of)s - 60),
last21 as (
  select instrument_id, price_date, close, volume, prev_close, k
  from bars where k <= 21),
qual as (
  select instrument_id,
         avg(abs(close / nullif(prev_close, 0) - 1) / nullif(close * volume, 0)) filter (where k <= 20) as illiq,
         max(close) filter (where k = 1) as px,
         max(price_date) filter (where k = 1) as last_date,
         count(*) filter (where close > 0 and volume > 0 and prev_close > 0 and k <= 20) as good,
         count(*) filter (where close > 0 and volume > 0 and k = 21) as base_ok
  from last21 group by instrument_id)
select instrument_id, illiq, px from qual
where last_date = %(as_of)s and good = 20 and base_ok = 1 and px >= %(floor)s
"""

_SPREAD_SQL = """
select instrument_id, 100 * (ask - bid) / ((ask + bid) / 2)
from etoro_rate_observations
where snapshot_id = %(snapshot)s and instrument_id = any(%(ids)s) and bid > 0 and ask >= bid
"""


def _q(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


def _row(label: str, spreads: list[float], domain: int) -> str:
    if not spreads:
        return f"{label:<26} {domain:<6} 0"
    return (
        f"{label:<26} {domain:<6} {len(spreads):<6} {statistics.fmean(spreads):.3f}  "
        f"{statistics.median(spreads):.3f}   {_q(spreads, 0.75):.3f}  {_q(spreads, 0.90):.3f}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recorded round-trip spread by Amihud-illiquidity tercile.")
    parser.add_argument("--snapshot", type=int, default=1, help="etoro_perishable_snapshots id (1 = 2026-09-25 17:22Z)")
    parser.add_argument("--as-of", default="2026-09-24", help="last session of the illiquidity window")
    parser.add_argument("--floor", type=float, default=5.0)
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        ids = list(load_validated_universe(conn))
        domain = conn.execute(
            _DOMAIN_SQL, {"ids": ids, "as_of": date.fromisoformat(args.as_of), "floor": args.floor}
        ).fetchall()
        quotes = conn.execute(_SPREAD_SQL, {"snapshot": args.snapshot, "ids": [r[0] for r in domain]}).fetchall()
    spread = dict(quotes)
    if len(spread) != len(quotes):
        print(f"refused: snapshot {args.snapshot} has more than one quote for some instrument")
        return 1
    # Most illiquid first; ties by instrument_id so the cut never depends on the measured spread.
    ranked = sorted(domain, key=lambda r: (-float(r[1]), r[0]))
    n = len(ranked)
    print(f"validated universe {len(ids)}; domain (20 returns to {args.as_of}, close >= {args.floor}): {n}")
    if n < 3:
        return 1
    cut = -(-n // 3)  # ceil(n / 3) names in the illiquid third, as the trial cuts it
    thirds = {
        "illiquid": ranked[:cut],
        "middle": ranked[cut : cut + (n - cut) // 2],
        "liquid": ranked[cut + (n - cut) // 2 :],
    }
    print(
        "group                      domain with   mean   median  p75    p90   (round-trip spread %, one capture each)"
    )
    for label, rows in thirds.items():
        print(_row(label, [float(spread[r[0]]) for r in rows if r[0] in spread], len(rows)))
    by_band: dict[str, list[tuple[int, float]]] = {}
    for instrument_id, _, px in thirds["illiquid"]:
        band = cost_band_for(Decimal(px), price_basis="as_traded")
        by_band.setdefault(f"illiquid {band.label} ({band.p75_spread_pct})", []).append((instrument_id, float(px)))
    for label, members in sorted(by_band.items()):
        print(_row(label, [float(spread[i]) for i, _ in members if i in spread], len(members)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
