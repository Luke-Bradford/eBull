"""Open and report #2476's sealed recent return interval.

⚠⚠ GATED SINCE #2616. The first look ran before ``TRIAL_REGISTER_CUTOFF`` and
was charged by #2600's reconstruction as ``pead-historical-sue-net-income-v1``
(8 searches, exact). That entry does not pre-pay for another look: any re-run
must name a NEW register entry (``--rerun-trial-id``), present a frozen #2599
declaration (``scripts/freeze_2616_precutoff_declarations.py``), and writes a
``read`` access row before any outcome is read. See
``scripts/sealed_rerun_gate.py`` for the rules and why each exists.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Final

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.block_bootstrap import block_bootstrap_expectancy, cluster_by_date
from app.services.pead_candidate import build_archive_source, expand_instrument_alternatives
from app.services.pead_outcomes import (
    BOOTSTRAP_SEED,
    EventOutcome,
    OutcomeSummary,
    build_matched_control_events,
    concurrency_counts,
    evaluate_outcomes,
    median_time_to_outcome_days,
)
from app.services.result_ledger import verify_outcome_access_provenance
from scripts.sealed_rerun_gate import SealedTrialIdentity, require_outcome_gate, require_outcome_gate_preconditions

STRATEGY_ID: Final = "pead-historical-sue-net-income"
STRATEGY_VERSION: Final = "pead-historical-sue-net-income-v1"

#: This trial's contract is its preregistration document, digest-frozen like
#: C-4's JSON contract. A deliberate edit must update this digest in review.
SEALED_TRIAL: Final = SealedTrialIdentity(
    strategy_id=STRATEGY_ID,
    strategy_version=STRATEGY_VERSION,
    prereg_doc=Path("docs/proposals/ta/2026-08-10-pead-preregistration.md"),
    prereg_sha256="dc5aa8034372e678b7cc1e798a68a4ce2018aaf7c7226383dbbf09b880bb9126",
    original_trial_id="pead-historical-sue-net-income-v1",
    rerun_trial_id_prefix="pead-historical-sue-net-income",
    accessed_by="scripts/verify_2476_pead_outcomes.py",
)


def _subtract_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 - months
    year, zero_month = divmod(absolute, 12)
    month = zero_month + 1
    day = min(value.day, 28)
    return date(year, month, day)


def _summary(outcomes: Sequence[EventOutcome], seed: int) -> OutcomeSummary:
    bootstrap = None
    if outcomes:
        bootstrap = block_bootstrap_expectancy(
            cluster_by_date(
                [item.net_return_pct for item in outcomes],
                [item.entry_date for item in outcomes],
            ),
            seed=seed,
        )
    return OutcomeSummary(outcomes=tuple(outcomes), bootstrap=bootstrap, refusals={})


def _print_segment(label: str, outcomes: Sequence[EventOutcome], seed: int) -> None:
    summary = _summary(outcomes, seed)
    market = [
        item.market_relative_net_return_pct for item in outcomes if item.market_relative_net_return_pct is not None
    ]
    sector = [
        item.sector_relative_net_return_pct for item in outcomes if item.sector_relative_net_return_pct is not None
    ]
    interval = (
        "unavailable"
        if summary.bootstrap is None
        else f"[{summary.bootstrap.ci_low_pct:+.3f}, {summary.bootstrap.ci_high_pct:+.3f}]"
    )
    expectancy = "—" if summary.expectancy_pct is None else f"{summary.expectancy_pct:+.3f}%"
    win_rate = "—" if summary.win_rate_pct is None else f"{summary.win_rate_pct:.2f}%"
    profit_factor = "—" if summary.profit_factor is None else f"{summary.profit_factor:.3f}"
    worst = "—" if summary.worst_trade_pct is None else f"{summary.worst_trade_pct:+.2f}%"
    expected_shortfall = (
        "—" if summary.expected_shortfall_5_pct is None else f"{summary.expected_shortfall_5_pct:+.2f}%"
    )
    market_mean = "—" if not market else f"{mean(market):+.3f}%"
    sector_mean = "—" if not sector else f"{mean(sector):+.3f}%"
    date_clusters = len({item.entry_date for item in outcomes})
    maximum_concurrent, median_concurrent = concurrency_counts(outcomes)
    median_open = "—" if median_concurrent is None else f"{median_concurrent:.1f}"
    holding_days = median_time_to_outcome_days(outcomes)
    holding = "—" if holding_days is None else f"{holding_days:.0f}d"
    diagnostics: list[str] = []
    for horizon, attribute in ((5, "net_return_5_pct"), (20, "net_return_20_pct"), (40, "net_return_40_pct")):
        values = [value for item in outcomes if (value := getattr(item, attribute)) is not None]
        diagnostics.append(f"d{horizon}={'—' if not values else f'{mean(values):+.3f}%'}")
    print(
        f"{label:<22} events={len(outcomes):>5,} dates={date_clusters:>4,} expectancy={expectancy:>9} "
        f"CI={interval:<19} "
        f"wins={win_rate:>7} PF={profit_factor:>6} worst={worst:>9} ES5={expected_shortfall:>9} "
        f"vsSPY={market_mean:>9} vsSector={sector_mean:>9} hold={holding} "
        f"concurrency={median_open}/{maximum_concurrent} {' '.join(diagnostics)}"
    )


def _print_equal_gross_primary(outcomes: Sequence[EventOutcome], seed: int) -> None:
    """Report 50% long / 50% short expectancy with a conservative joint CI."""
    long = [item for item in outcomes if item.side == "long"]
    short = [item for item in outcomes if item.side == "short"]
    if not long or not short:
        print("equal-gross primary    unavailable — one side has no retained outcomes")
        return
    long_bootstrap = block_bootstrap_expectancy(
        cluster_by_date([item.net_return_pct for item in long], [item.entry_date for item in long]),
        seed=seed,
        confidence=0.975,
    )
    short_bootstrap = block_bootstrap_expectancy(
        cluster_by_date([item.net_return_pct for item in short], [item.entry_date for item in short]),
        seed=seed + 1,
        confidence=0.975,
    )
    point = (mean(item.net_return_pct for item in long) + mean(item.net_return_pct for item in short)) / 2
    if long_bootstrap is None or short_bootstrap is None:
        interval = "unavailable"
    else:
        # Bonferroni: two 97.5% marginal intervals provide at least 95% joint
        # coverage without pretending the long and short arms are independent.
        low = (long_bootstrap.ci_low_pct + short_bootstrap.ci_low_pct) / 2
        high = (long_bootstrap.ci_high_pct + short_bootstrap.ci_high_pct) / 2
        interval = f"[{low:+.3f}, {high:+.3f}]"
    dates = len({item.entry_date for item in outcomes})
    print(
        f"equal-gross primary    long={len(long):,} short={len(short):,} dates={dates:,} "
        f"expectancy={point:+.3f}% CI={interval} (Bonferroni 95% joint)"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--open-sealed-holdout",
        action="store_true",
        help="required acknowledgement: this reads the preregistered recent outcomes",
    )
    parser.add_argument(
        "--rerun-trial-id",
        help="the trial_register entry charging THIS look; the original "
        "pead-historical-sue-net-income-v1 entry covers only the pre-cutoff look already taken",
    )
    args = parser.parse_args(argv)
    if not args.open_sealed_holdout:
        parser.error("pass --open-sealed-holdout only after the source and outcome code are frozen and tested")
    # ⚠ The database-free refusals fire before a connection is opened (#2616).
    require_outcome_gate_preconditions(SEALED_TRIAL, args.rerun_trial_id)

    archive = resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip"
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
        source, loaded = build_archive_source(conn, archive)
        control_events, control_match = build_matched_control_events(source.triggers)
        signal_events = expand_instrument_alternatives(source.triggers, source.instrument_alternatives)
        expanded_controls = expand_instrument_alternatives(control_events, source.instrument_alternatives)
        evaluated = evaluate_outcomes(conn, signal_events)
        control_evaluated = evaluate_outcomes(conn, expanded_controls)
        conn.rollback()  # discard the temporary event relation explicitly

    outcomes = evaluated.outcomes
    if not outcomes:
        raise RuntimeError("the preregistered outcome population is empty")
    last_entry = max(item.entry_date for item in outcomes)
    print("#2476 sealed outcome report")
    print(f"companyfacts SHA-256: {loaded.archive_sha256}")
    print(f"latest complete entry: {last_entry}")
    print(f"source SUE events: {len(source.sue_events):,}; classified: {len(source.triggers):,}")
    print()
    _print_equal_gross_primary(outcomes, BOOTSTRAP_SEED + 100)
    _print_segment("2022+ all", outcomes, BOOTSTRAP_SEED)
    _print_segment("2022+ long", [item for item in outcomes if item.side == "long"], BOOTSTRAP_SEED + 1)
    _print_segment("2022+ short", [item for item in outcomes if item.side == "short"], BOOTSTRAP_SEED + 2)
    _print_segment("matched control", control_evaluated.outcomes, BOOTSTRAP_SEED + 3)
    for months in (36, 24):
        start = _subtract_months(last_entry, months)
        _print_segment(
            f"trailing {months} months",
            [item for item in outcomes if start <= item.entry_date <= last_entry],
            BOOTSTRAP_SEED + months,
        )
    for year in sorted({item.entry_date.year for item in outcomes}):
        _print_segment(
            str(year),
            [item for item in outcomes if item.entry_date.year == year],
            BOOTSTRAP_SEED + year,
        )

    print("\nsource exclusions and census")
    for reason, count in source.refusals.items():
        print(f"  {reason:<42} {count:>10,}")
    print("\noutcome refusals")
    for reason, count in evaluated.refusals.items():
        print(f"  {reason:<42} {count:>10,}")
    print("\ncontrol matching")
    for reason, count in control_match.items():
        print(f"  {reason:<42} {count:>10,}")
    print("\ncontrol outcome refusals")
    for reason, count in control_evaluated.refusals.items():
        print(f"  {reason:<42} {count:>10,}")
    side_counts = Counter(item.side for item in outcomes)
    print(f"\nretained outcomes: long={side_counts['long']:,} short={side_counts['short']:,}")
    print("promotion refusals: portfolio_drawdown_not_measured, exposure_not_measured,")
    print("                    turnover_not_measured, dollar_capacity_not_measured, carry_unmodelled, survivor_only")
    return 0


if __name__ == "__main__":
    sys.exit(main())
