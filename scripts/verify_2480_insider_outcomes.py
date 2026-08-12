"""Open and report #2480's preregistered sealed outcome interval.

⚠⚠ GATED SINCE #2616. The first look ran before ``TRIAL_REGISTER_CUTOFF`` and
was charged by #2600's reconstruction as ``form4-code-p-opportunistic-purchase-v1``
(7 searches, exact) — NOT as ``insider-purchase-forward-returns-first-look-2026-08-09``,
which is ``scripts/verify_2437_insider_forward_returns.py``'s distinct first-look
construction; #2614's table and #2616's opening report both mis-attributed it.
That entry does not pre-pay for another look: any re-run must name a NEW register
entry (``--rerun-trial-id``), present a frozen #2599 declaration
(``scripts/freeze_2616_precutoff_declarations.py``), and writes a ``read`` access
row before any outcome is read. See ``scripts/sealed_rerun_gate.py``.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Final

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
from app.services.result_ledger import verify_outcome_access_provenance
from scripts.sealed_rerun_gate import SealedTrialIdentity, require_outcome_gate, require_outcome_gate_preconditions

STRATEGY_ID: Final = "form4-code-p-opportunistic-purchase"
STRATEGY_VERSION: Final = "form4-code-p-opportunistic-purchase-v1"

#: This trial's contract is its preregistration document, digest-frozen like
#: C-4's JSON contract. A deliberate edit must update this digest in review.
SEALED_TRIAL: Final = SealedTrialIdentity(
    strategy_id=STRATEGY_ID,
    strategy_version=STRATEGY_VERSION,
    prereg_doc=Path("docs/proposals/ta/2026-08-10-insider-purchase-preregistration.md"),
    prereg_sha256="27b0361dee73d033137a435f81ef0a43d4ffec20ecb5e294caac91056769603a",
    original_trial_id="form4-code-p-opportunistic-purchase-v1",
    rerun_trial_id_prefix="form4-code-p-opportunistic-purchase",
    accessed_by="scripts/verify_2480_insider_outcomes.py",
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
    ess = "—" if bootstrap is None else f"{bootstrap.effective_sample_size:.1f}"
    expectancy = "—" if not monthly else f"{mean(item.spread_pct for item in monthly):+.3f}%"
    wins = "—" if not monthly else f"{sum(item.spread_pct > 0 for item in monthly) / len(monthly) * 100:.1f}%"
    pf = profit_factor(monthly)
    drawdown = maximum_drawdown_pct(monthly)
    tail = expected_shortfall_5_pct(monthly)
    eq = "—" if not monthly else f"{mean(item.equal_weight_spread_pct for item in monthly):+.3f}%"
    firms = median_firm_count(monthly)
    worst = "—" if not monthly else f"{min(item.spread_pct for item in monthly):+.2f}%"
    market = [item.market_relative_spread_pct for item in monthly if item.market_relative_spread_pct is not None]
    sector = [item.sector_relative_spread_pct for item in monthly if item.sector_relative_spread_pct is not None]
    market_text = "—" if not market else f"{mean(market):+.3f}%/{len(market)}"
    sector_text = "—" if not sector else f"{mean(sector):+.3f}%/{len(sector)}"
    maximum_firms = max((item.unique_firms for item in monthly), default=0)
    minimum_liquidity = min((item.minimum_median_dollar_volume for item in monthly), default=None)
    maximum_weight = max((item.maximum_single_firm_weight_pct for item in monthly), default=None)
    print(
        f"{label:<22} months={len(monthly):>2} ESS={ess:>5} expectancy={expectancy:>9} CI={ci:<18} "
        f"wins={wins:>6} PF={'—' if pf is None else f'{pf:.3f}':>6} "
        f"MDD={'—' if drawdown is None else f'{drawdown:+.2f}%':>8} "
        f"ES5={'—' if tail is None else f'{tail:+.2f}%':>8} worst={worst:>8} EW={eq:>9} "
        f"vsSPY={market_text:>12} vsSector={sector_text:>12} "
        f"firms median/max={'—' if firms is None else f'{firms:.1f}'}/{maximum_firms} "
        f"minADV={'—' if minimum_liquidity is None else f'${minimum_liquidity:,.0f}'} "
        f"maxWeight={'—' if maximum_weight is None else f'{maximum_weight:.1f}%'}"
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
    parser.add_argument(
        "--rerun-trial-id",
        help="the trial_register entry charging THIS look; the original "
        "form4-code-p-opportunistic-purchase-v1 entry covers only the pre-cutoff look already taken",
    )
    args = parser.parse_args(argv)
    if not args.open_sealed_holdout:
        parser.error("pass --open-sealed-holdout only after source and outcome code are frozen and reviewed")
    # ⚠ The database-free refusals fire before a connection is opened (#2616).
    require_outcome_gate_preconditions(SEALED_TRIAL, args.rerun_trial_id)
    archives = sorted(args.archive_dir.glob("*_form345.zip"))

    with psycopg.connect(settings.database_url) as conn:
        gate = require_outcome_gate(conn, SEALED_TRIAL, trial_id=args.rerun_trial_id)
        # ⚠ The look is logged even if the evaluation below dies — and the
        # provenance re-check immediately after is what proves it COMMITTED
        # and that the declaration was frozen strictly before it.
        conn.commit()
        verify_outcome_access_provenance(
            conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=gate.declaration_id,
            access_id=gate.access_id,
        )
        print(
            f"re-run charged to register entry {gate.rerun_trial_id} "
            f"(declaration {gate.declaration_id}, access {gate.access_id})"
        )
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
    print("\nmonthly opportunistic-minus-routine disclosed-purchase-value-weighted returns")
    _print_segment("2022+ primary", evidence.monthly_returns, BOOTSTRAP_SEED)
    _print_segment("matched random", control.monthly_returns, BOOTSTRAP_SEED + 1)
    for months in (36, 24):
        start = _subtract_months(last_entry, months - 1)
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
    primary_effect_pass = evidence.bootstrap is not None and evidence.bootstrap.ci_low_pct > 0
    matched_control_pass = paired_bootstrap is not None and paired_bootstrap.ci_low_pct > 0
    full_years = {
        year: [item for item in evidence.monthly_returns if item.entry_date.year == year]
        for year in range(2022, last_entry.year)
    }
    stability_pass = bool(full_years) and all(
        len(items) >= 10 and mean(item.spread_pct for item in items) > 0 for items in full_years.values()
    )
    print("\nexplicit gates")
    print(f"  primary positive lower CI:            {'PASS' if primary_effect_pass else 'FAIL'}")
    print(f"  primary beats matched control by CI:  {'PASS' if matched_control_pass else 'FAIL'}")
    print(f"  completed-year sign stability:        {'PASS' if stability_pass else 'FAIL'}")
    print("  tail threshold:                       BLOCKED — no capital-trial bound registered")
    print("  point-in-time universe:               BLOCKED — survivor-only price corpus")
    print("  live cost/borrow contract:             BLOCKED — historical estimate only")
    print("  bracket and forward shadow:            BLOCKED — separate trial not defined")
    print("  PROMOTION:                             BLOCKED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
