"""Measure S-E's 10-month-SMA overlay against its frozen contract. Refs #2837.

Contract: ``docs/proposals/ta/2026-08-22-se-ma-overlay-preregistration.md``
(`se-ma-overlay-2026-08-22`). The rule itself is ``app/services/se_ma_overlay.py``;
this is the impure half — the chain read, the distribution yield, the regime map
and the readout.

⚠⚠ THIS OPENS OUTCOMES, SO IT GOES THROUGH THE #2599 DOOR.
``require_outcome_access`` refuses ``preregistration_not_frozen`` until
``scripts/freeze_2837_se_overlay_declaration.py`` has run, and it writes the
access row that criterion 5 counts. This script stores nothing in
``strategy_results_store`` — it computes its own statistics from raw price
windows — which is exactly the shape #2614 found the ledger could not intercept
by itself, so the call here is explicit rather than incidental.

⚠ READ ONLY against the corpus, in one REPEATABLE READ snapshot, so the chain,
the yield segment and the regime map all observe one state of the world.

Run, in this order and only in this order:

    PYTHONPATH=. uv run python -m scripts.freeze_2837_se_overlay_declaration --dry-run
    PYTHONPATH=. uv run python -m scripts.freeze_2837_se_overlay_declaration
    PYTHONPATH=. uv run python -m scripts.measure_2837_se_overlay
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from typing import Final, TextIO

import psycopg

from app.config import settings
from app.services.market_regime_provider import (
    CHAIN_FALLBACK,
    CHAIN_SEAM,
    MarketRegimeProvider,
    load_research_closes,
)
from app.services.prereg_contract import declaration_refusals
from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    load_preregistration,
    require_outcome_access,
)
from app.services.se_ma_overlay import (
    CHAIN_BARS,
    CHAIN_FIRST_BAR,
    CHAIN_LAST_BAR,
    MAX_DRAWDOWN_RATIO_BAR,
    MIN_CAGR_DELTA_PP,
    OFFSETS,
    ArmResult,
    OverlayRefused,
    RegimeCohort,
    march_2020_detail,
    regime_cohorts,
    simulate_arm,
)
from app.services.trial_register import TRIAL_REGISTER
from scripts.freeze_2837_se_overlay_declaration import STRATEGY_ID, STRATEGY_VERSION

TRIAL_ID: Final = "se-ma-overlay-2026-08-22"


def _open_the_gate(conn: psycopg.Connection[tuple]) -> int:
    """#2599's door. Refuses unless the declaration was frozen first.

    ⚠ THE CALLER OWNS THE COMMIT — the same contract C-4's gate documents.
    ``require_outcome_access`` writes in this transaction and does not commit,
    and the measurement afterwards opens with ``SET TRANSACTION ISOLATION
    LEVEL ... READ ONLY``, which is only valid as a transaction's first
    statement. Committing the access first also keeps the look logged if the
    measurement then dies.
    """
    if TRIAL_REGISTER.trial_for_declaration(STRATEGY_ID, STRATEGY_VERSION) is None:
        raise OverlayRefused(
            f"no trial in {TRIAL_REGISTER.version} claims {STRATEGY_ID}@{STRATEGY_VERSION}; criterion 6's M does "
            "not count this search, so the look must not happen"
        )
    frozen = load_preregistration(conn, STRATEGY_ID, STRATEGY_VERSION)
    if frozen is None:
        raise PreregDeclarationRefused(STRATEGY_ID, STRATEGY_VERSION, ("preregistration_not_frozen",))
    refusals = declaration_refusals(frozen.declaration)
    if refusals:
        raise PreregDeclarationRefused(STRATEGY_ID, STRATEGY_VERSION, tuple(str(code) for code in refusals))
    # `read`, with a NULL result_version: sql/264's
    # `strategy_holdout_accesses_evaluate_names_a_result` requires an `evaluate`
    # access to name a result row, and this study writes none.
    return require_outcome_access(
        conn,
        HoldoutAccess(
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            result_version=None,
            access_kind="read",
            accessed_by="scripts/measure_2837_se_overlay.py",
            purpose=f"open S-E's preregistered drawdown-insurance measurement under {TRIAL_ID}",
        ),
    )


def _assert_chain_extent(chain: list[tuple[date, float]]) -> None:
    """Contract §3 — the frozen extent, refused rather than absorbed.

    ⚠ A corpus refresh that extends either segment changes the tested span. A
    contract that silently absorbs that means something different each time it
    runs, and the difference is invisible in the output.
    """
    actual = (chain[0][0], chain[-1][0], len(chain))
    expected = (CHAIN_FIRST_BAR, CHAIN_LAST_BAR, CHAIN_BARS)
    if actual != expected:
        raise OverlayRefused(
            f"spy_chain_v1 extent moved: got first={actual[0]} last={actual[1]} bars={actual[2]}, contract froze "
            f"first={expected[0]} last={expected[1]} bars={expected[2]}"
        )


def _distribution_yield_pp(conn: psycopg.Connection[tuple]) -> tuple[float, int, date, date]:
    """Contract §3.1 — SPY's realised annual distribution yield, MEASURED.

    ⚠ SOURCED, NOT CHOSEN. It is the fallback segment's own annualised
    ``adj_close``-minus-``close`` growth over the pre-seam span: the one place in
    this corpus where the same instrument carries both a distribution-adjusted
    and a price-only series. The primary segment carries no ``adj_close`` at all,
    which is why the chain is price-return and why this charge exists.

    ⚠ Applying a 1993–2022 yield to the 2022–2026 tail OVERSTATES the recent
    drag, because SPY's yield fell over the span. Overstating it makes §8's
    second leg harder, which is the conservative direction.
    """
    vendor, vendor_symbol = CHAIN_FALLBACK
    rows = conn.execute(
        """
        SELECT d.bar_date, d.close, d.adj_close
        FROM research_price_daily d
        JOIN research_price_series s USING (series_id)
        WHERE s.vendor = %s AND s.vendor_symbol = %s
          AND d.bar_date < %s
          AND d.close IS NOT NULL AND d.adj_close IS NOT NULL
          AND d.close > 0 AND d.adj_close > 0
        ORDER BY d.bar_date
        """,
        (vendor, vendor_symbol, CHAIN_SEAM),
    ).fetchall()
    if len(rows) < 2:
        raise OverlayRefused(
            f"the {vendor}/{vendor_symbol} segment carries {len(rows)} rows with both close and adj_close before "
            f"{CHAIN_SEAM}; §3.1's yield cannot be sourced and the dividend drag cannot be charged"
        )
    first, last = rows[0], rows[-1]
    years = (last[0] - first[0]).days / 365.25
    total_return = (float(last[2]) / float(first[2])) ** (1.0 / years)
    price_return = (float(last[1]) / float(first[1])) ** (1.0 / years)
    return ((total_return - price_return) * 100.0, len(rows), first[0], last[0])


def _render(result: ArmResult, cohorts: Sequence[RegimeCohort], out: TextIO) -> None:
    ratio = result.drawdown_ratio
    out.write(f"\n=== offset +{result.offset} chain bars ===\n")
    out.write(f"span                 {result.first_execution} .. {result.last_execution}  ({result.years:.2f}y)\n")
    out.write(f"decisions / flips    {len(result.evaluation_dates)} / {result.flips}\n")
    out.write(f"time in cash         {result.fraction_in_cash * 100:.2f}%\n")
    out.write(f"dividend drag        {result.dividend_drag_pp:.4f} pp/yr\n")
    out.write(f"seam-spanning SMAs   {result.seam_spanning_windows}\n")
    out.write(f"exits re-entered <30d {result.reentries_within_30_days}\n")
    out.write(
        f"CGT charges          {len(result.tax_charges)}  total £{sum(c.tax_gbp for c in result.tax_charges):,.0f}\n"
    )
    out.write("\n  LEG 1 — drawdown insurance\n")
    out.write(f"    overlay max DD     {result.overlay_max_drawdown_pct:.2f}%\n")
    out.write(f"    buy-and-hold max DD {result.benchmark_max_drawdown_pct:.2f}%\n")
    out.write(
        f"    ratio               {'n/a' if ratio is None else f'{ratio:.3f}'}  (bar <= {MAX_DRAWDOWN_RATIO_BAR:.3f})"
    )
    out.write(f"   -> {'PASS' if result.drawdown_leg_passes else 'FAIL'}\n")
    out.write("\n  LEG 2 — bounded CAGR cost\n")
    out.write(f"    overlay CAGR       {result.overlay_cagr_pct:.3f}%  (before drag)\n")
    out.write(f"    buy-and-hold CAGR  {result.benchmark_cagr_pct:.3f}%\n")
    out.write(f"    delta after drag   {result.net_cagr_delta_pp:+.3f} pp/yr  (bar >= {MIN_CAGR_DELTA_PP:+.1f})")
    out.write(f"   -> {'PASS' if result.cagr_leg_passes else 'FAIL'}\n")
    out.write(f"\n  ARM VERDICT          {'PASS' if result.passes else 'FAIL'}\n")
    out.write(f"\n  episodes >=15%: overlay {result.overlay_episodes_over_class}, ")
    out.write(f"buy-and-hold {result.benchmark_episodes_over_class}\n")
    for label, episodes in (("overlay", result.overlay_worst_episodes), ("buy&hold", result.benchmark_worst_episodes)):
        for episode in episodes:
            recovery = "UNRECOVERED" if episode.unrecovered else str(episode.recovery_date)
            out.write(
                f"    {label:9s} -{episode.depth_pct:6.2f}%  peak {episode.peak_date}  "
                f"trough {episode.trough_date}  recovered {recovery}\n"
            )
    out.write("\n  per-regime cohorts (descriptive only — never in the signal or the bar)\n")
    for cohort in cohorts:
        out.write(
            f"    {cohort.regime:22s} n={cohort.intervals:4d} cash={cohort.months_in_cash:4d} "
            f"overlay {cohort.overlay_mean_return_pct:+7.3f}%  b&h {cohort.benchmark_mean_return_pct:+7.3f}%\n"
        )
    out.write("\n  §7 symmetric sensitivity (both arms liquidated and taxed)\n")
    out.write(
        f"    overlay £{result.symmetric_overlay_terminal_gbp:,.0f}   "
        f"buy-and-hold £{result.symmetric_benchmark_terminal_gbp:,.0f}\n"
    )
    detail = march_2020_detail(result)
    out.write(f"\n  March-2020 window ({len(detail)} decisions, predeclared fields)\n")
    for evaluation, execution, position, overlay_equity, benchmark_equity in detail:
        out.write(
            f"    eval {evaluation}  exec {execution}  position {'HELD' if position else 'CASH'}  "
            f"overlay £{overlay_equity:,.0f}  b&h £{benchmark_equity:,.0f}\n"
        )


def main(argv: list[str] | None = None) -> int:
    """⚠⚠ THERE IS NO --skip-gate, AND ITS ABSENCE IS DELIBERATE.

    The first draft had one, "to exercise the plumbing before the declaration is
    frozen". That is a documented bypass of the single gate this whole design
    rests on — and using it would itself have been the first look the
    preregistration exists to precede, which is the defect wearing the costume of
    a debugging aid. The pure arithmetic is covered by
    ``tests/test_2837_se_ma_overlay.py`` against synthetic chains; the two
    database helpers read source characteristics, not outcomes, and can be
    exercised directly.
    """
    argparse.ArgumentParser(description=__doc__).parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        access_id = _open_the_gate(conn)
        conn.commit()
        sys.stdout.write(f"#2599 access recorded: access_id={access_id}, trial={TRIAL_ID}\n")

        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        chain = load_research_closes(conn)
        _assert_chain_extent(chain)
        yield_pp, yield_rows, yield_first, yield_last = _distribution_yield_pp(conn)
        regime_provider = MarketRegimeProvider.load_research(conn)
        conn.rollback()

    sys.stdout.write(
        f"\nspy_chain_v1: {len(chain)} bars, {chain[0][0]} .. {chain[-1][0]} (extent matches the frozen contract)\n"
        f"§3.1 distribution yield: {yield_pp:.4f} pp/yr, sourced from {yield_rows} "
        f"{CHAIN_FALLBACK[0]} bars {yield_first} .. {yield_last}\n"
    )

    verdicts: list[bool] = []
    for offset in OFFSETS:
        result = simulate_arm(chain, offset=offset, seam=CHAIN_SEAM, dividend_yield_pp=yield_pp)
        cohorts = regime_cohorts(result, [value for value in regime_provider.for_dates(result.evaluation_dates).values])
        _render(result, cohorts, sys.stdout)
        verdicts.append(result.passes)

    sys.stdout.write("\n" + "=" * 72 + "\n")
    # ⚠ ALL THREE, never the best. One offset passing is not a pass — §5 and §8.
    sys.stdout.write(f"S-E VERDICT: {'PASS' if all(verdicts) else 'FAIL'}  (arms: {verdicts})\n")
    if not all(verdicts):
        sys.stdout.write("§11: a fail is terminal. No smaller lookback, no band, no different proxy — each is a NEW\n")
        sys.stdout.write("declared search charging the register again. Record the lesson on #2837 and close it.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["TRIAL_ID", "main"]
