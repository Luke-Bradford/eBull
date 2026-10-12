"""#3612: does a drawdown de-gross rule earn its cost on the engine pot? Measurement only.

A one-factor proxy of the pot, not an engine replay: the pot holds a risky fraction ``e0`` of its NAV in the US
market and the rest in cash. ``e0 = 0.5`` is today's core mandate (``core_target_pct`` 50, mandate event 35);
``e0 = 0.8`` adds the 30% active-risk budget (pool event 5) at a beta of 1, the pot fully deployed. Exposure rules:

* ``today``: the present behaviour, and the baseline every excess is measured against. Rebalance to ``e0`` while the
  pot's drawdown is under the 25% limit; from the limit, buys are refused (``portfolio_drawdown_limit`` in
  ``strategy_paper_executor``; the core's refusal in ``strategy_core_executor``), so exposure is the lower of the
  drifted weight and ``e0``. Added after the first run, once the core's refusal was read;
* ``static``: ``e0`` every day, rebalancing through any fall (reference, frozen before the first run);
* ``ladder``: ``e0 x f(DD)``, with ``f`` = 1, 0.75, 0.5, 0.25 at pot drawdowns from the high-water mark below 10%,
  15%, 20% and from 20%. Thresholds are the example in ``quant/portfolio-construction-and-risk.md``; the cut sizes
  have no source rule and are fixed by construction. Exposure follows the current drawdown both ways;
* ``gz``: Grossman & Zhou (1993): risky amount proportional to the cushion ``W - (1 - D) M`` above the drawdown floor,
  ``M`` the high-water mark, i.e. fraction ``m (1 - (1 - D) M / W)``. ``D`` = 0.25, the mandate's
  ``max_portfolio_drawdown_pct``. ``m = e0 / D`` (the pot is exactly ``e0`` invested at its high-water mark) and the
  cap at ``e0`` (no leverage) are calibration by construction, not from the paper. With continuous prices and trading,
  a non-negative cash return and no costs the floor always holds. Rebalanced daily with no costs, it fails only when
  one session's market fall exceeds ``1 / m = D / e0`` (cash return non-negative). Costs lower that threshold: a fee
  can breach the floor itself, or shrink the cushion so a smaller fall breaches it.

The ladder and gz rules keep today's refusal: from the limit they never buy. The ladder and gz rules and the static
reference were frozen before the first run; the refusal on the ladder and gz was added after checkpoint 1.

Each rule observes the close of day t - 1 and trades at that close (a same-close fill); a next-session fill is not
measured. Rebalancing from the drifted weight costs ``c x |target - drifted|`` of NAV, charged before the day's
return (an approximation of a cash-funded fee). The opening position is endowed at no cost. Cash earns RF.
Drawdown is from the running high-water mark of this simulated NAV, with no capital flows.

Data: Kenneth French's daily three factors (``french_three_factor_daily``), parsed by
``reference_data._parse_french_zip`` from percent to decimal returns. Market = ``Mkt-RF + RF`` (Fama-French market:
value-weighted CRSP firms, dividends included), cash = ``RF`` (one-month T-bill). The snapshot is pinned by argument.
Sample ends 2021-12-31, before the ``primary-2022-plus`` holdout. Each sample starts at an anchor close whose own
return is not used, and is annualised over calendar days from that anchor (the NYSE traded Saturdays until 1952).
SPY's dividend-adjusted close (research series 7694) is the objective's benchmark over 1993-01-29 .. 2021-12-31.

    PYTHONPATH=. uv run python scripts/measure_3612_degross_ladder.py --snapshot-id 60 --out <file.json>
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import psycopg

SAMPLE_END = date(2021, 12, 31)
SPY_ERA_ANCHOR = date(1993, 1, 29)
# The first date of French's daily file (F-F_Research_Data_Factors_daily).
FRENCH_DAILY_START = date(1926, 7, 1)
MAX_DRAWDOWN = 0.25
LADDER = ((0.10, 1.0), (0.15, 0.75), (0.20, 0.5))
LADDER_FLOOR = 0.25
EXPOSURES = (0.5, 0.8)
COSTS = (0.0, 0.0005, 0.0015)
REFERENCE_COST = 0.0005
TARGET_GRID = tuple(round(0.01 * k, 2) for k in range(1, 81))  # 0.01 .. 0.80
SPY_SERIES_ID = 7694
CRISES = {
    "1929-32": (date(1929, 9, 3), date(1932, 7, 8)),
    "1937-38": (date(1937, 3, 6), date(1938, 3, 31)),
    "1973-74": (date(1973, 1, 11), date(1974, 12, 6)),
    "1987": (date(1987, 8, 25), date(1987, 12, 4)),
    "2000-02": (date(2000, 3, 24), date(2002, 10, 9)),
    "2007-09": (date(2007, 10, 9), date(2009, 3, 9)),
    "2020": (date(2020, 2, 19), date(2020, 3, 23)),
}

Day = tuple[date, float, float]  # (date, market return, cash return)
Rule = Callable[[float, float, float], float]  # (drawdown, W / M, drifted weight) -> risky fraction


def _years(anchor: date, last: date) -> float:
    return (last - anchor).days / 365.25


def with_entry_stop(rule: Rule) -> Rule:
    """Today's refusal: from the limit, never buy (a sale is still allowed)."""
    return lambda dd, ratio, drifted: (
        rule(dd, ratio, drifted) if dd < MAX_DRAWDOWN else min(rule(dd, ratio, drifted), drifted)
    )


def static_rule(e0: float) -> Rule:
    return lambda _dd, _ratio, _drifted: e0


def ladder_rule(e0: float) -> Rule:
    def rule(dd: float, _ratio: float, _drifted: float) -> float:
        for bound, factor in LADDER:
            if dd < bound:
                return e0 * factor
        return e0 * LADDER_FLOOR

    return with_entry_stop(rule)


def gz_rule(e0: float) -> Rule:
    m = e0 / MAX_DRAWDOWN

    def rule(_dd: float, ratio: float, _drifted: float) -> float:
        return min(e0, max(0.0, m * (1.0 - (1.0 - MAX_DRAWDOWN) / ratio)))

    return with_entry_stop(rule)


RULES: dict[str, Callable[[float], Rule]] = {
    "today": lambda e0: with_entry_stop(static_rule(e0)),
    "static": static_rule,
    "ladder": ladder_rule,
    "gz": gz_rule,
}


@dataclass(frozen=True)
class Readout:
    rule: str
    e0: float
    cost: float
    anchor: str
    end: str
    annual_return_pct: float
    excess_vs_today_bp: float | None
    max_drawdown_pct: float
    days_beyond_limit: int
    mean_exposure: float
    annual_turnover: float
    # Worst drawdown from the running high-water mark on any day inside the window; the peak may precede it.
    crisis_drawdown_pct: dict[str, float | None]


def simulate(anchor: date, days: Sequence[Day], rule: Rule, cost: float) -> dict[str, Any]:
    """Pot NAV path over ``days`` (ascending, all after ``anchor``), invested at the anchor close."""
    nav = peak = 1.0
    weight = rule(0.0, 1.0, 0.0)
    traded = exposure_sum = 0.0
    # Worst drawdown observed at each close: after its return, and after the fee of the rebalance made there.
    worst: dict[date, float] = {}
    previous = anchor
    for day, mkt, rf in days:
        target = rule(1.0 - nav / peak, nav / peak, weight)
        trade = abs(target - weight)
        traded += trade
        fee = cost * trade
        nav *= 1.0 - fee
        if fee > 0:
            # The rebalance is made at the previous close; a fee can breach the floor on its own.
            worst[previous] = min(worst.get(previous, 0.0), nav / peak - 1.0)
        exposure_sum += target
        growth = 1.0 + target * mkt + (1.0 - target) * rf
        weight = target * (1.0 + mkt) / growth
        nav *= growth
        peak = max(peak, nav)
        worst[day] = nav / peak - 1.0
        previous = day
    path = sorted(worst.items())
    years = _years(anchor, days[-1][0])
    return {
        "annual": nav ** (1.0 / years) - 1.0,
        "path": path,
        "mean_exposure": exposure_sum / len(days),
        "turnover": traded / years,
    }


def readout(name: str, e0: float, cost: float, anchor: date, days: Sequence[Day], baseline: float | None):
    run = simulate(anchor, days, RULES[name](e0), cost)
    path = run["path"]
    crisis: dict[str, float | None] = {}
    for label, (lo, hi) in CRISES.items():
        # A crisis is reported only when the sample covers the whole window.
        inside = [dd for d, dd in path if lo <= d <= hi]
        covered = anchor < lo and path[-1][0] >= hi and inside
        crisis[label] = round(min(inside) * 100, 2) if covered else None
    return Readout(
        rule=name,
        e0=e0,
        cost=cost,
        anchor=anchor.isoformat(),
        end=days[-1][0].isoformat(),
        annual_return_pct=round(run["annual"] * 100, 3),
        excess_vs_today_bp=None if baseline is None else round((run["annual"] - baseline) * 1e4, 1),
        max_drawdown_pct=round(min(dd for _, dd in path) * 100, 2),
        days_beyond_limit=sum(dd < -MAX_DRAWDOWN for _, dd in path),
        mean_exposure=round(run["mean_exposure"], 4),
        annual_turnover=round(run["turnover"], 3),
        crisis_drawdown_pct=crisis,
    ), run["annual"]


def load_market(conn: psycopg.Connection[Any], snapshot_id: int) -> list[Day]:
    meta = conn.execute(
        """
        SELECT s.dataset_key, s.parse_status, s.missing_count, s.row_count, count(o.*),
               count(DISTINCT o.series_key), count(DISTINCT o.observation_date)
        FROM reference_data_snapshots s LEFT JOIN reference_data_observations o USING (snapshot_id)
        WHERE s.snapshot_id = %s GROUP BY 1, 2, 3, 4
        """,
        (snapshot_id,),
    ).fetchone()
    assert meta is not None and meta[:3] == ("french_three_factor_daily", "accepted", 0), f"{snapshot_id}: {meta}"
    # Every stored row is the parse's, and every parsed date carries every series (no partial dates).
    assert meta[3] == meta[4] == meta[5] * meta[6], f"snapshot {snapshot_id} rows: {meta[3:]}"
    rows = conn.execute(
        """
        SELECT observation_date,
               max(value) FILTER (WHERE series_key = 'Mkt-RF'), max(value) FILTER (WHERE series_key = 'RF'),
               count(*) FILTER (WHERE series_key = 'Mkt-RF'), count(*) FILTER (WHERE series_key = 'RF')
        FROM reference_data_observations
        WHERE snapshot_id = %s AND series_key IN ('Mkt-RF', 'RF') AND observation_date <= %s
        GROUP BY observation_date ORDER BY observation_date
        """,
        (snapshot_id, SAMPLE_END),
    ).fetchall()
    out: list[Day] = []
    for day, excess, rf, n_excess, n_rf in rows:
        assert n_excess == 1 and n_rf == 1, f"{day}: {n_excess} Mkt-RF, {n_rf} RF"
        mkt, cash = float(excess) + float(rf), float(rf)
        assert math.isfinite(mkt) and math.isfinite(cash) and mkt > -1.0, f"{day}: {excess}, {rf}"
        out.append((day, mkt, cash))
    assert out, f"snapshot {snapshot_id} has no rows to {SAMPLE_END}"
    assert out[0][0] == FRENCH_DAILY_START and out[-1][0] == SAMPLE_END, f"{out[0][0]}..{out[-1][0]}"
    assert SPY_ERA_ANCHOR in {r[0] for r in out}, "the SPY-era anchor is not a session in the snapshot"
    return out


def spy_annual(conn: psycopg.Connection[Any]) -> float:
    first, last = conn.execute(
        """
        SELECT (SELECT adj_close FROM research_price_daily WHERE series_id = %(s)s AND bar_date = %(a)s),
               (SELECT adj_close FROM research_price_daily WHERE series_id = %(s)s AND bar_date = %(b)s)
        """,
        {"s": SPY_SERIES_ID, "a": SPY_ERA_ANCHOR, "b": SAMPLE_END},
    ).fetchone() or (None, None)
    symbol = conn.execute(
        "SELECT vendor_symbol FROM research_price_series WHERE series_id = %s", (SPY_SERIES_ID,)
    ).fetchone()
    assert symbol == ("SPY",), f"series {SPY_SERIES_ID} is {symbol}"
    # adj_close is the split- and dividend-adjusted close (sql/251).
    assert first is not None and last is not None, "SPY adj_close missing at an endpoint"
    assert all(math.isfinite(float(v)) and float(v) > 0 for v in (first, last)), f"SPY endpoints {first}, {last}"
    return (float(last) / float(first)) ** (1.0 / _years(SPY_ERA_ANCHOR, SAMPLE_END)) - 1.0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", type=int, required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        market = load_market(conn, args.snapshot_id)
        spy = spy_annual(conn)
    samples = {
        "full": (market[0][0], market[1:]),
        "spy_era": (SPY_ERA_ANCHOR, [r for r in market if r[0] > SPY_ERA_ANCHOR]),
    }
    results: list[dict[str, Any]] = []
    best_safe_target: dict[str, Any] = {}
    sample_facts: dict[str, Any] = {}
    for sample, (anchor, days) in samples.items():
        worst = min(days, key=lambda r: r[1])
        years = _years(anchor, days[-1][0])
        sample_facts[sample] = {
            "anchor": anchor.isoformat(),
            "sessions": len(days),
            "market_annual_pct": (math.prod(1.0 + r[1] for r in days) ** (1.0 / years) - 1.0) * 100,
            "worst_session": [worst[0].isoformat(), worst[1]],
            "min_cash_return": min(r[2] for r in days),
            "negative_cash_sessions": sum(r[2] < 0 for r in days),
            # Coverage witness: no historical calendar is invented; the longest gap must be a known closure.
            "longest_gap": max(
                ((b[0] - a[0]).days, a[0].isoformat(), b[0].isoformat())
                for a, b in zip([(anchor, 0.0, 0.0), *days], days, strict=False)
            ),
            "gz_breach_session_fall": {str(e0): MAX_DRAWDOWN / e0 for e0 in EXPOSURES},
        }
        for e0 in EXPOSURES:
            for cost in COSTS:
                base, base_annual = readout("today", e0, cost, anchor, days, None)
                results.append({"sample": sample, **asdict(base)})
                for name in ("static", "ladder", "gz"):
                    r, _ = readout(name, e0, cost, anchor, days, base_annual)
                    results.append({"sample": sample, **asdict(r)})
        # The highest-return target with no session beyond the floor, compared unrounded.
        safe = [
            pair
            for pair in (readout("today", e0, REFERENCE_COST, anchor, days, None) for e0 in TARGET_GRID)
            if pair[0].days_beyond_limit == 0
        ]
        best_safe_target[sample] = asdict(max(safe, key=lambda pair: pair[1])[0]) if safe else None
    payload = {
        "snapshot_id": args.snapshot_id,
        "spy_era_spy_annual_pct": spy * 100,
        "samples": sample_facts,
        "best_target_holding_limit_under_today": best_safe_target,
        "results": results,
    }
    with open(args.out, "w") as fh:
        json.dump(payload, fh, indent=1)
    print(json.dumps({k: v for k, v in payload.items() if k != "results"}, indent=1))
    for r in results:
        print(
            f"{r['sample']:7} e0={r['e0']} c={r['cost']:.4f} {r['rule']:6} ann={r['annual_return_pct']:7.3f}% "
            f"xs={r['excess_vs_today_bp']} mdd={r['max_drawdown_pct']}% >25%d={r['days_beyond_limit']} "
            f"exp={r['mean_exposure']} to={r['annual_turnover']} crisis={r['crisis_drawdown_pct']}"
        )


if __name__ == "__main__":
    main()
