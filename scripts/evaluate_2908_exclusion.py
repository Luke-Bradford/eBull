"""Open and report every outcome in preregistered R6 arm #2908."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

from app.services.r6_exclusion_trial import (
    HALF_SPREAD,
    WINDOW_END,
    PortfolioResult,
    constructed_nsi_factor,
    haircut_net_return,
    load_required_prices,
    read_global_q_nsi,
    signal_sets,
    simulate_portfolio,
    validate_factor,
)
from app.services.r6_pit_bundle import load_r6_pit_bundle


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _verify_mirror(root: Path, expected_commit: str) -> None:
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"], check=True, capture_output=True, text=True
    ).stdout.strip()
    if commit != expected_commit or dirty:
        raise RuntimeError(f"price mirror is not the declared clean commit: commit={commit}, dirty={bool(dirty)}")


def _portfolio_payload(result: PortfolioResult) -> dict[str, Any]:
    value = dataclasses.asdict(result)
    value["traded_notional_over_initial_capital"] = result.traded_notional_over_initial_capital
    value["spread_cost_over_initial_capital"] = result.spread_cost_over_initial_capital
    return value


def _json_default(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"unsupported JSON result type: {type(value).__name__}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--price-mirror", type=Path, required=True)
    parser.add_argument("--price-mirror-commit", required=True)
    parser.add_argument("--global-q", type=Path, required=True)
    parser.add_argument("--global-q-sha256", required=True)
    parser.add_argument("--acknowledge-open-preregistered-outcomes", action="store_true")
    args = parser.parse_args()
    if not args.acknowledge_open_preregistered_outcomes:
        parser.error("--acknowledge-open-preregistered-outcomes is required")
    if _sha256(args.global_q) != args.global_q_sha256:
        raise RuntimeError("global-q source digest moved")
    _verify_mirror(args.price_mirror, args.price_mirror_commit)
    bundle = load_r6_pit_bundle(args.manifest, expected_manifest_sha256=args.manifest_sha256)
    prices = load_required_prices(bundle, args.price_mirror / "Data" / "Day")
    validation = validate_factor(constructed_nsi_factor(bundle, prices), read_global_q_nsi(args.global_q))
    if not validation.passed:
        raise RuntimeError("published-factor identity gate failed; arm outcomes remain sealed as a construction bug")

    sets = signal_sets(bundle, prices)
    formations = tuple(sorted(sets))
    schedule_names = ("full", "dilution", "red_flag", "union")
    schedules = {name: tuple((formation, sets[formation][name]) for formation in formations) for name in schedule_names}
    schedules["buy_hold"] = ((formations[0], sets[formations[0]]["full"]),)

    outcomes: dict[str, Any] = {}
    for case in ("best", "worst"):
        measured: dict[str, dict[str, Any]] = {}
        for name, schedule in schedules.items():
            gross = simulate_portfolio(schedule=schedule, prices=prices, case=case, half_spread=0.0)
            net = simulate_portfolio(schedule=schedule, prices=prices, case=case, half_spread=HALF_SPREAD)
            measured[name] = {
                "gross": _portfolio_payload(gross),
                "net": _portfolio_payload(net),
            }
        buy_hold_gross = measured["buy_hold"]["gross"]["total_return"]
        buy_hold_net = measured["buy_hold"]["net"]["total_return"]
        for name in ("dilution", "red_flag", "union"):
            strategy_gross = measured[name]["gross"]["total_return"]
            strategy_net = measured[name]["net"]["total_return"]
            haircut_results: dict[str, object] = {}
            for haircut in (0.15, 0.58):
                adjusted = haircut_net_return(
                    strategy_gross=strategy_gross,
                    strategy_net=strategy_net,
                    buy_hold_gross=buy_hold_gross,
                    haircut=haircut,
                )
                haircut_results[f"{haircut:.2f}"] = {
                    "absolute_net_return": adjusted,
                    "excess_vs_buy_hold_net": adjusted - buy_hold_net,
                    "passed": adjusted > 0 and adjusted > buy_hold_net,
                }
            measured[name]["haircuts"] = haircut_results
        outcomes[case] = measured

    overlap: dict[str, object] = {}
    population: dict[str, object] = {}
    for formation in formations:
        dilution = sets[formation]["dilution_excluded"]
        red_flag = sets[formation]["red_flag_excluded"]
        union = dilution | red_flag
        overlap[formation.isoformat()] = {
            "dilution_excluded": len(dilution),
            "intersection": len(dilution & red_flag),
            "jaccard": 0.0 if not union else len(dilution & red_flag) / len(union),
            "red_flag_excluded": len(red_flag),
            "union_excluded": len(union),
        }
        population[formation.isoformat()] = {
            name: len(sets[formation][name]) for name in ("full", "dilution", "red_flag", "union")
        }

    primary_58 = outcomes["worst"]["dilution"]["haircuts"]["0.58"]["passed"]
    primary_15 = outcomes["worst"]["dilution"]["haircuts"]["0.15"]["passed"]
    verdict = "PASS_ROBUST" if primary_58 else "PASS_CONTINGENT" if primary_15 else "FAIL"
    print(
        json.dumps(
            {
                "factor_validation": dataclasses.asdict(validation),
                "haircut_rule": (
                    "scale positive gross excess vs buy-and-hold; never rescue non-positive gross excess; "
                    "subtract full strategy cost drag"
                ),
                "invalid_price_rows_skipped": sum(series.invalid_rows for series in prices.values()),
                "manifest_sha256": args.manifest_sha256,
                "outcomes": outcomes,
                "overlap": overlap,
                "population": population,
                "price_mirror_commit": args.price_mirror_commit,
                "primary_arm": "dilution",
                "round_trip_loss": 1 - (1 - HALF_SPREAD) / (1 + HALF_SPREAD),
                "spread_half": HALF_SPREAD,
                "termination_cases": ["best", "worst"],
                "verdict": verdict,
                "window": {"formation_start": formations[0].isoformat(), "outcome_end": WINDOW_END.isoformat()},
            },
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
