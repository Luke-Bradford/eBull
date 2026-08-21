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

import psycopg

from app.config import settings
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import BANDS
from app.services.strategy_manifest import STRATEGY_MANIFEST

#: Novy-Marx & Velikov (2016), RFS 29(1). Turnover above this rarely survives costs.
MAX_MONTHLY_TURNOVER_PCT = 50.0

#: ⚠⚠ ``evidence_window_id`` IS IN THE GROUP BY, AND IT IS LOAD-BEARING.
#: A registered evidence window is a MEASUREMENT WINDOW, so two of them are two
#: different measurements of the same strategy — averaging across them produces
#: a number that describes neither. This query grouped on ``strategy_id`` alone
#: until 2026-08-21, and on that day the dev database held two complete 40-row
#: hold-out runs written four hours apart:
#:
#:   primary-2022-plus  2022-01-01..2024-09-27  40 rows  13:56 UTC
#:   rolling-36m        2021-09-28..2024-09-27  40 rows  18:04 UTC
#:
#: Both are legitimate and neither is a double-write — ``evidence_window_id`` is
#: part of the result identity, so ``assert_no_existing_results`` correctly let
#: the second run store beside the first. What was wrong was reading them
#: together. Measured that day, the blend moved every strategy: s8's expectancy
#: read -0.868 mixed against -0.778 (primary-2022-plus) and -0.958
#: (rolling-36m), and s2 read -6.902 against -6.282 and -7.523.
#:
#: ⚠ It changed no VERDICT on that data, because all ten fail in both windows.
#: That is luck, not safety: the mixed figure is closer to the bar than the
#: better window's on every row, so the blend is capable of failing a strategy
#: one window passes, and of passing one that only the kinder window supports.
#:
#: Same defect family as ``sql/256``'s header — *"EVERY AGGREGATE OVER THIS
#: TABLE MUST PIN ONE (rule_set_version, input_rule_set_version) PAIR … an
#: unpinned count counts one trade twice"*. The key differs; the failure does not.
_SQL = """
SELECT evidence_window_id,
       strategy_id,
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
  AND (%(window)s::text IS NULL OR evidence_window_id = %(window)s::text)
GROUP BY 1, 2
ORDER BY 1, 4 DESC
"""


def _verdict_block(window: str, rows: list[tuple], *, header: str) -> int:
    """Print one evidence window's table. Returns how many strategies passed.

    ⚠ ONE WINDOW PER BLOCK AND NEVER A MERGED ONE. See ``_SQL``: two registered
    windows are two measurements, and a strategy that clears the bar in one and
    fails in the other has told you something a single averaged row destroys.
    """
    print(f"\nevidence window: {window}")
    print(header)
    print("-" * (len(header) + 34))

    measured = {row[1] for row in rows}
    # ⚠ EVERY MANIFEST STRATEGY IS REPORTED, NOT ONLY THE ONES WITH ROWS. A gate
    # that omits a strategy with no evidence reads as "not shown" when it means
    # "not measured", and absent-is-not-pass is the whole point of a gate.
    missing = sorted(set(STRATEGY_MANIFEST) - measured)

    viable = 0
    for _window, sid, turnover, expectancy, pf, trade_sr, bar, cagr, skew, kurt in rows:
        # ⚠ `avg()` over an all-NULL column returns NULL. Refuse on the missing
        # figure rather than crashing the report — a viability gate that dies on
        # sparse data tells you nothing about the strategies that DID measure.
        required = {
            "turnover": turnover,
            "expectancy_per_trade_pct": expectancy,
            "profit_factor": pf,
            "dsr_trade_sharpe": trade_sr,
            "dsr_expected_max_sharpe": bar,
        }
        absent = sorted(name for name, value in required.items() if value is None)
        if absent:
            print(f"{sid:36}{'':>9}{'':>12}{'':>7}{'':>9}{'':>8}  NO VERDICT — null: {', '.join(absent)}")
            continue

        per_month = float(turnover) / 12 * 100
        refusals: list[str] = []
        # 1. turnover — the pre-backtest gate
        if per_month > MAX_MONTHLY_TURNOVER_PCT:
            refusals.append(f"turnover {per_month:.0f}%/mo > {MAX_MONTHLY_TURNOVER_PCT:.0f}%")
        # 2. break-even — one trade must clear one round trip
        if float(expectancy) <= 0:
            refusals.append(f"expectancy {float(expectancy):+.2f}%/trade <= 0")
        # 3. profit factor
        if float(pf) < 1.0:
            refusals.append(f"profit factor {float(pf):.2f} < 1.0")
        # 4. deflation
        if float(trade_sr) <= float(bar):
            refusals.append(f"trade Sharpe {float(trade_sr):+.3f} <= bar {float(bar):.3f}")

        verdict = "VIABLE" if not refusals else "; ".join(refusals)
        viable += not refusals
        print(
            f"{sid:36}{per_month:>8.0f}%{float(expectancy):>12.2f}{float(pf):>7.2f}"
            f"{float(trade_sr):>9.3f}{float(bar):>8.3f}  {verdict}"
        )
        # ⚠ Say it out loud when the compounded figures disagree with the per-trade
        # ones — that disagreement IS the trap this script exists for.
        if cagr is not None and float(cagr) > 0 and float(expectancy) <= 0:
            skew_txt = f"{float(skew):.0f}" if skew is not None else "?"
            kurt_txt = f"{float(kurt):.0f}" if kurt is not None else "?"
            print(
                f"{'':36}⚠ CAGR {float(cagr):+.1f}% is positive on a NEGATIVE per-trade edge "
                f"(skew {skew_txt}, kurtosis {kurt_txt}) — a right-tail payoff, not an edge"
            )

    for sid in missing:
        print(f"{sid:36}{'':>9}{'':>12}{'':>7}{'':>9}{'':>8}  NO EVIDENCE — no hold_out rows in window")

    print(f"  {viable}/{len(STRATEGY_MANIFEST)} viable in {window} ({len(rows)} measured, {len(missing)} unmeasured)")
    return viable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-hours", type=int, default=24)
    parser.add_argument(
        "--evidence-window",
        default=None,
        help="restrict to one registered evidence window id; default reports every window separately",
    )
    args = parser.parse_args()

    cheapest = min(band.p75_spread_pct for band in BANDS)
    dearest = max(band.p75_spread_pct for band in BANDS)

    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SQL, {"hours": args.since_hours, "window": args.evidence_window}).fetchall()

    if not rows:
        scope = f" for evidence window {args.evidence_window}" if args.evidence_window else ""
        print(f"no hold_out results in the last {args.since_hours}h for universe {BACKTEST_UNIVERSE}{scope}")
        return 1

    print(f"round-trip cost band: {cheapest}% (>=$100) .. {dearest}% (<$5)")
    header = f"{'strategy':36}{'turn/mo':>9}{'exp%/trade':>12}{'PF':>7}{'tradeSR':>9}{'bar':>8}  verdict"

    by_window: dict[str, list[tuple]] = {}
    for row in rows:
        by_window.setdefault(row[0], []).append(row)
    if len(by_window) > 1:
        # ⚠ SAID OUT LOUD, not silently handled. The previous version averaged
        # these together, and a reader of that table had no way to know two
        # measurement windows had been blended into one number.
        print(
            f"\n⚠ {len(by_window)} evidence windows in this period: {', '.join(sorted(by_window))} — "
            "reported SEPARATELY. Averaging across them describes neither; pass --evidence-window to pin one."
        )

    total_viable = 0
    for window in sorted(by_window):
        total_viable += _verdict_block(window, by_window[window], header=header)

    print(f"\n{total_viable} viable strategy-window pair(s) across {len(by_window)} window(s)")
    return 0 if total_viable else 2


if __name__ == "__main__":
    sys.exit(main())
