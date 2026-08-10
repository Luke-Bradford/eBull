"""Open and report #2480's preregistered sealed outcome interval once."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from statistics import mean

import psycopg

from app.config import settings
from app.services.block_bootstrap import BootstrapResult, block_bootstrap_expectancy, cluster_by_date
from app.services.insider_purchase_candidate import build_source
from app.services.insider_purchase_outcomes import (
    BOOTSTRAP_SEED,
    MonthlyPortfolioReturn,
    PortfolioEvaluation,
    build_firm_month_signals,
    build_matched_control_signals,
    evaluate_portfolios,
    expected_shortfall_5_pct,
    maximum_drawdown_pct,
    median_firm_count,
    profit_factor,
)


def _subtract_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 - months
    year, zero_month = divmod(absolute, 12)
    return date(year, zero_month + 1, 1)


def _print_segment(label: str, monthly: Sequence[MonthlyPortfolioReturn], seed: int) -> None:
    bootstrap = block_bootstrap_expectancy(
        cluster_by_date([item.spread_pct for item in monthly], [item.entry_date for item in monthly]), seed=seed
    )
    ci = "unavailable" if bootstrap is None else f"[{bootstrap.ci_low_pct:+.3f},{bootstrap.ci_high_pct:+.3f}]"
    expectancy = "—" if not monthly else f"{mean(item.spread_pct for item in monthly):+.3f}%"
    wins = "—" if not monthly else f"{sum(item.spread_pct > 0 for item in monthly) / len(monthly) * 100:.1f}%"
    pf = profit_factor(monthly)
    drawdown = maximum_drawdown_pct(monthly)
    tail = expected_shortfall_5_pct(monthly)
    eq = "—" if not monthly else f"{mean(item.equal_weight_spread_pct for item in monthly):+.3f}%"
    firms = median_firm_count(monthly)
    print(
        f"{label:<22} months={len(monthly):>2} expectancy={expectancy:>9} CI={ci:<18} "
        f"wins={wins:>6} PF={'—' if pf is None else f'{pf:.3f}':>6} "
        f"MDD={'—' if drawdown is None else f'{drawdown:+.2f}%':>8} "
        f"ES5={'—' if tail is None else f'{tail:+.2f}%':>8} EW={eq:>9} "
        f"median firms={'—' if firms is None else f'{firms:.1f}'}"
    )


def _paired_control_difference(
    evidence: PortfolioEvaluation, control: PortfolioEvaluation
) -> tuple[int, float | None, BootstrapResult | None]:
    evidence_by_month = {item.entry_date: item.spread_pct for item in evidence.monthly_returns}
    control_by_month = {item.entry_date: item.spread_pct for item in control.monthly_returns}
    dates = sorted(evidence_by_month.keys() & control_by_month.keys())
    values = [evidence_by_month[item] - control_by_month[item] for item in dates]
    bootstrap = block_bootstrap_expectancy(cluster_by_date(values, dates), seed=BOOTSTRAP_SEED + 99)
    return len(values), (mean(values) if values else None), bootstrap


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--last-quarter", required=True)
    parser.add_argument("--open-sealed-holdout", action="store_true")
    args = parser.parse_args(argv)
    if not args.open_sealed_holdout:
        parser.error("pass --open-sealed-holdout only after source and outcome code are frozen and reviewed")
    archives = sorted(args.archive_dir.glob("*_form345.zip"))

    with psycopg.connect(settings.database_url) as conn:
        source = build_source(conn, archives, expected_last_quarter=args.last_quarter)
        signals = build_firm_month_signals(source.classified)
        controls, control_match = build_matched_control_signals(signals)
        evidence = evaluate_portfolios(conn, signals)
        control = evaluate_portfolios(conn, controls)
        conn.rollback()

    if not evidence.monthly_returns:
        raise RuntimeError("the preregistered outcome population is empty")
    last_entry = max(item.entry_date for item in evidence.monthly_returns)
    print("#2480 sealed outcome report")
    print(f"archive manifest SHA-256: {source.archive_manifest_sha256}")
    print(f"latest complete portfolio month: {last_entry:%Y-%m}")
    print(f"source-classified observations: {len(source.source_classified):,}")
    print(f"research-resolved classified observations: {len(source.classified):,}")
    print(f"deduplicated firm-month signals: {len(signals):,}")
    print("\nmonthly opportunistic-minus-routine value-weight returns")
    _print_segment("2022+ primary", evidence.monthly_returns, BOOTSTRAP_SEED)
    _print_segment("matched random", control.monthly_returns, BOOTSTRAP_SEED + 1)
    for months in (36, 24):
        start = _subtract_months(last_entry, months)
        _print_segment(
            f"trailing {months} months",
            [item for item in evidence.monthly_returns if start <= item.entry_date <= last_entry],
            BOOTSTRAP_SEED + months,
        )
    for year in sorted({item.entry_date.year for item in evidence.monthly_returns}):
        _print_segment(
            str(year),
            [item for item in evidence.monthly_returns if item.entry_date.year == year],
            BOOTSTRAP_SEED + year,
        )

    paired_n, paired_mean, paired_bootstrap = _paired_control_difference(evidence, control)
    paired_ci = (
        "unavailable"
        if paired_bootstrap is None
        else f"[{paired_bootstrap.ci_low_pct:+.3f},{paired_bootstrap.ci_high_pct:+.3f}]"
    )
    print(
        f"\nprimary minus matched-control spread: months={paired_n} "
        f"expectancy={'—' if paired_mean is None else f'{paired_mean:+.3f}%'} CI={paired_ci}"
    )
    print("declared monthly portfolio turnover: 200% per cohort (one full entry + exit)")
    print("\nsource exclusions and census")
    for reason, count in source.refusals.items():
        print(f"  {reason:<50} {count:>12,}")
    print("\noutcome refusals")
    for reason, count in evidence.refusals.items():
        print(f"  {reason:<50} {count:>12,}")
    print("\ncontrol matching")
    for reason, count in control_match.items():
        print(f"  {reason:<50} {count:>12,}")
    print("\ncontrol outcome refusals")
    for reason, count in control.refusals.items():
        print(f"  {reason:<50} {count:>12,}")
    print("\npromotion refusals: survivor_only, total_return_dividends_absent, live_exact_acceptance_required,")
    print("                    bracket_not_defined, forward_shadow_absent, broker_cost_contract_unproven")
    return 0


if __name__ == "__main__":
    sys.exit(main())
