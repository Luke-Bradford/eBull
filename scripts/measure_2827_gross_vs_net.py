"""#2827 step 1 — is the loss the SIGNAL or the SPREAD? Measure gross, do not infer it.

⚠⚠ WHY THIS EXISTS. #2827 reported that ten structurally different strategies all
lose ``-0.83%`` to ``-1.76%`` per trade, observed that this is about one round-trip
spread, and concluded that they *"break even gross and lose the spread net"*. That
conclusion was computed from ``turnover x band spread`` — an INFERENCE from two
aggregates, never a reading of an uncosted return. The issue says so itself:
*"Gross expectancy is inferred, not measured … do that before building anything."*
This script does it.

WHAT IT MEASURES, AND WHY IT NEEDS A CORPUS PASS
------------------------------------------------
``strategy_results_store`` holds NET aggregates only, and the backtest's per-trade
population is never persisted — ``sql/256``'s ``gross_return_pct`` belongs to the
forward outcome resolver (199 rows), not to a backtest arm. So gross has to come
out of the same pass that produced net.

⚠ The MEAN could have been derived instead. The half-spread is multiplicative and
identical on every backtest trade, so ``(1 + net) = (1 + gross) x (1 - h)/(1 + h)``
exactly, and the mean of one fixes the mean of the other. **The profit factor
cannot be derived**: costs move the win/loss boundary off zero, so trades with a
net return between ``1/k - 1`` and ``0`` change sides. Since profit factor is one
of the two metrics ``cost-aware-viability.md`` allows a decision to rest on, the
pass is the only honest route — and it lets the derived mean be CHECKED rather
than asserted (``identity gap`` below, which must be ~0).

⚠ READ-ONLY BY CONSTRUCTION. It calls ``evaluate_level_arms`` / ``evaluate_arm``
directly rather than ``run_backtest``, so nothing is written and the trial
register is not charged. That is not only hygiene: ``run_backtest`` would refuse
outright — ``assert_no_existing_results`` blocks a second row at an identity that
2026-08-21's run already stored.

THE BAND SENSITIVITY, AND WHAT IT IS NOT
----------------------------------------
Every backtest trade is charged the ``<$5`` band (1.450% round trip) whatever its
price, because ``cost_band_for`` refuses to let a split-adjusted price select a
nominal threshold (``cost_model``: *"an adverse sensitivity suitable for
falsification, not a claim that the resulting cost is the historical quote"*). So
the reported net is priced at the DEAREST calibrated band for the whole panel.
The table below re-prices the measured gross population at each band.

⚠⚠ THOSE ROWS ARE A SENSITIVITY, NOT A RESULT. A cheaper band is only reachable
by knowing each entry's NOMINAL price, which this corpus does not carry. Quoting a
cheaper-band figure as the strategy's performance is fitting the cost model to the
answer, which ``cost-aware-viability.md`` names as the thing not to do. What the
row is FOR is bounding how much of the verdict the band rule owns.

Usage::

    PYTHONPATH=. uv run python -m scripts.measure_2827_gross_vs_net \
        --strategy s8-range-mean-reversion [--strategy ...]
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg

from app.config import settings
from app.services.backtest_run import (
    BACKTEST_UNIVERSE,
    QUARANTINE_ARM_ORDER,
    ArmMeasurement,
    NamespaceMeasurement,
    _regime_for,
    _resolve_invocation_window,
    evaluate_arm,
    evaluate_level_arms,
    load_corpus,
    runnable_strategies,
)
from app.services.cost_model import BANDS, COST_MODEL_ID
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import ResultNamespace

#: The window 2026-08-21's decisive run measured, so the gross figures here are
#: comparable to the net figures already stored rather than to a fresh span.
WINDOW = "primary-2022-plus"

#: ⚠⚠ THE AUDIT FIELDS ARE WRITTEN, NOT DECORATIVE. This script is a SEALED
#: OUTCOME OPENER: it computes its own statistics off the withheld side and
#: stores no result row, so no ledger write-path gate can intercept it — the
#: exact shape #2614 found three of. ``backtest_run``'s header states the
#: invariant it would otherwise break: *"criterion 5's whole mechanism is that
#: looking at it is rare, deliberate and logged"*. ``main`` therefore records a
#: ``read`` access per strategy through #2599's paved door and COMMITS it
#: BEFORE the corpus is read.
#:
#: ⚠ The ordering is the governance, not a detail. An access recorded after the
#: look would be a log written by whoever already knows the answer, which is the
#: fabrication ``record_holdout_access``'s trial lock exists to order against.
PURPOSE = "measure gross vs net per-trade return to falsify #2827's cost theory before building on it"
ACCESSED_BY = "agent, operator-directed"


@dataclass(frozen=True)
class Summary:
    """One arm's gross and net trade population, reduced to the deciding pair."""

    label: str
    trade_count: int
    net_expectancy: float
    net_profit_factor: float | None
    gross_expectancy: float
    gross_profit_factor: float | None
    #: Measured minus derived, in percentage points. ⚠ The derivation is exact
    #: arithmetic on a single-valued ``h``, so anything above float noise means
    #: the charge was NOT single-valued and every "one round trip" reading in
    #: this report is wrong.
    identity_gap: float
    half_spreads: tuple[float, ...]


def _expectancy(returns: Sequence[float]) -> float:
    return sum(returns) / len(returns) if returns else 0.0


def _profit_factor(returns: Sequence[float]) -> float | None:
    """Gross wins / gross losses. ``None`` when nothing lost — a real state."""
    gains = sum(value for value in returns if value > 0.0)
    losses = -sum(value for value in returns if value < 0.0)
    return (gains / losses) if losses > 0.0 else None


def _repriced(gross: Sequence[float], *, half_spread: float) -> list[float]:
    """The gross population re-charged at another ``h``, per ``position_costing``.

    ⚠ MULTIPLICATIVE, matching §5.1: a buy fills at ``x(1+h)`` and a sell at
    ``x(1-h)``, so the round trip scales ``(1+r)`` rather than subtracting from
    ``r``. Subtracting would be a second, different cost model.
    """
    factor = (1.0 - half_spread) / (1.0 + half_spread)
    return [(1.0 + value / 100.0) * factor * 100.0 - 100.0 for value in gross]


def _summarise(label: str, measurement: NamespaceMeasurement) -> Summary:
    """Reduce one namespace measurement to the gross/net pair and its check.

    ⚠ NET COMES OFF ``metrics``, NOT off a second reduction of the trade list.
    ``compute_metrics`` is what produced every stored net figure, so recomputing
    expectancy here would compare gross against a number no result row carries —
    the two-numbers-joined-on-an-axis-neither-names defect.
    """
    gross = measurement.gross_returns
    metrics = measurement.metrics
    half_spreads = tuple(sorted(measurement.half_spreads))
    # The derived counterpart of the measured mean, per the module header. Exact
    # only while ``h`` is single-valued, which ``half_spreads`` reports.
    if len(half_spreads) == 1:
        k = (1.0 + half_spreads[0]) / (1.0 - half_spreads[0])
        derived = (1.0 + metrics.expectancy_per_trade_pct / 100.0) * k * 100.0 - 100.0
        gap = _expectancy(gross) - derived
    else:
        gap = float("nan")
    return Summary(
        label=label,
        trade_count=metrics.trade_count,
        net_expectancy=metrics.expectancy_per_trade_pct,
        net_profit_factor=metrics.profit_factor,
        gross_expectancy=_expectancy(gross),
        gross_profit_factor=_profit_factor(gross),
        identity_gap=gap,
        half_spreads=half_spreads,
    )


def _arms_for(
    conn: psycopg.Connection[Any],
    strategy_id: str,
    *,
    corpus: Any,
    namespaces: Sequence[ResultNamespace],
    progress: Any,
) -> list[ArmMeasurement]:
    """Every arm of one strategy, routed exactly as ``run_backtest`` routes it.

    ⚠ THE ROUTING IS COPIED, NOT SIMPLIFIED. A level-based entry, and ANY entry
    on a corpus carrying terminating admissions, needs ``evaluate_level_arms`` —
    the two ambiguity projections price a terminating series differently, so a
    single shared pass would stamp one measurement onto two arms that are not
    equal. ``survivorship_free`` always carries terminations, so on the live
    universe every strategy takes that branch; the other is kept so a
    survivor-only corpus measures the same way it is evaluated.
    """
    entry = STRATEGY_MANIFEST[strategy_id]
    identity = entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    regime = _regime_for(entry, corpus.axis)
    arms: list[ArmMeasurement] = []
    for quarantine in QUARANTINE_ARM_ORDER:
        if regime.level_based or corpus.termination:
            arms.extend(
                evaluate_level_arms(
                    conn,
                    entry,
                    corpus=corpus,
                    quarantine_arm=quarantine,
                    identity=identity,
                    namespaces=namespaces,
                    progress=progress,
                )
            )
        else:
            arms.append(
                evaluate_arm(
                    conn,
                    entry,
                    corpus=corpus,
                    quarantine_arm=quarantine,
                    ambiguity_arm=None,
                    identity=identity,
                    namespaces=namespaces,
                    progress=progress,
                )
            )
    return arms


def _print_arm(summary: Summary) -> None:
    net_pf = f"{summary.net_profit_factor:.3f}" if summary.net_profit_factor is not None else "n/a"
    gross_pf = f"{summary.gross_profit_factor:.3f}" if summary.gross_profit_factor is not None else "n/a"
    print(
        f"  {summary.label:34}{summary.trade_count:>8}"
        f"{summary.net_expectancy:>11.3f}{net_pf:>8}"
        f"{summary.gross_expectancy:>13.3f}{gross_pf:>10}"
        f"{summary.identity_gap:>13.2e}  h={summary.half_spreads}",
        flush=True,
    )


def _print_band_table(label: str, gross: Sequence[float]) -> None:
    """Re-price one arm's measured gross population at every calibrated band.

    ⚠⚠ A SENSITIVITY, NOT A RESULT — see the module header. The corpus cannot
    say which band a trade belonged to; this bounds how much of the verdict the
    max-band rule owns, and nothing more.
    """
    print(f"\n  band sensitivity on {label} (SENSITIVITY, not a result — the corpus cannot assign a band):")
    print(f"    {'band':12}{'round trip%':>13}{'exp%/trade':>12}{'PF':>8}")
    for band in sorted(BANDS, key=lambda item: item.p75_spread_pct):
        repriced = _repriced(gross, half_spread=float(band.half_spread))
        pf = _profit_factor(repriced)
        pf_text = f"{pf:.3f}" if pf is not None else "n/a"
        marker = "  <- charged" if band.p75_spread_pct == max(item.p75_spread_pct for item in BANDS) else ""
        print(
            f"    {band.label:12}{float(band.p75_spread_pct):>13.3f}{_expectancy(repriced):>12.3f}{pf_text:>8}{marker}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", action="append", required=True, help="manifest strategy id; repeatable")
    parser.add_argument("--limit", type=int, default=None, help="corpus series cap, for a smoke run")
    args = parser.parse_args()

    runnable, _ = runnable_strategies(STRATEGY_MANIFEST)
    unknown = [sid for sid in args.strategy if sid not in runnable]
    if unknown:
        raise SystemExit(f"not runnable manifest strategies: {unknown}; runnable today: {list(runnable)}")

    window, namespaces = _resolve_invocation_window(
        holdout_requested=True,
        evidence_window_id=WINDOW,
        universe=BACKTEST_UNIVERSE,
    )
    started = time.monotonic()
    last = [started]

    def progress(event: object) -> None:
        now = time.monotonic()
        if now - last[0] < 60:
            return
        last[0] = now
        print(f"[{now - started:7.0f}s] {event}", flush=True)

    with psycopg.connect(settings.database_url) as conn:
        print(f"window {WINDOW} {window} namespaces={list(namespaces)} universe={BACKTEST_UNIVERSE}", flush=True)
        # ⚠ EVERY strategy's access is recorded and COMMITTED before ANY corpus
        # read. Not one per strategy interleaved with its pass: a refusal on the
        # fourth strategy would otherwise leave three looks already taken, and
        # criterion 5's log is meant to be the thing that precedes the look.
        #
        # ⚠⚠ `record_holdout_access` AND NOT `require_outcome_access`, WHICH IS
        # THE DOOR A NEW EVALUATOR IS OTHERWISE SUPPOSED TO USE. Measured
        # 2026-08-21: NONE of the ten manifest strategies has a frozen #2599
        # declaration at its current version — `strategy_preregistration_
        # declarations` holds five rows and every one belongs to another trial
        # (c4-schedule13d, form4-code-p, pead-historical-sue, two mt1 arms). So
        # `require_outcome_access` refuses all ten with
        # `preregistration_not_frozen`, verified by running it.
        #
        # The resolution is #2599's own non-retroactivity clause, not a waiver:
        # `record_holdout_access` *"leaves a trial with no declaration alone,
        # because #2599 does not retroactively invalidate the trials that
        # predate it"*, and it is the SAME door `run_backtest` used to produce
        # the very rows this script decomposes. Freezing a declaration now would
        # be worse than useless — the look has already been taken, which is what
        # `supersession_trial_already_exposed` exists to refuse.
        #
        # ⚠ The undeclared state of all ten is a FINDING, not a fact about this
        # script, and it is ticketed rather than left in this comment.
        for strategy_id in args.strategy:
            identity = STRATEGY_MANIFEST[strategy_id].identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
            access_id = record_holdout_access(
                conn,
                HoldoutAccess(
                    strategy_id=strategy_id,
                    strategy_version=identity.version,
                    # ⚠ `read`, not `evaluate`. An `evaluate` record authorises
                    # ONE result row and must name its `result_version`; this
                    # writes no row, so naming one would attribute the look to a
                    # row it did not produce.
                    access_kind="read",
                    accessed_by=ACCESSED_BY,
                    purpose=PURPOSE,
                ),
            )
            print(f"criterion-5 access {access_id} recorded for {strategy_id} @ {identity.version}", flush=True)
        conn.commit()
        corpus = load_corpus(conn, universe_basis=BACKTEST_UNIVERSE, limit=args.limit, evaluation_window=window)
        print(f"corpus: {len(corpus.pairs)} series, axis {corpus.axis[0]}..{corpus.axis[-1]}", flush=True)
        for strategy_id in args.strategy:
            arm_started = time.monotonic()
            arms = _arms_for(conn, strategy_id, corpus=corpus, namespaces=namespaces, progress=progress)
            print(f"\n{strategy_id}  ({time.monotonic() - arm_started:.0f}s)", flush=True)
            print(
                f"  {'arm':34}{'trades':>8}{'net exp%':>11}{'netPF':>8}"
                f"{'gross exp%':>13}{'grossPF':>10}{'identity gap':>13}",
                flush=True,
            )
            pooled: list[float] = []
            for arm in arms:
                for measurement in arm.namespaces.values():
                    label = f"{arm.label}/{measurement.namespace}"
                    _print_arm(_summarise(label, measurement))
                    pooled.extend(measurement.gross_returns)
            if pooled:
                _print_band_table(f"{strategy_id} (all arms pooled, {len(pooled)} trades)", pooled)
        # ⚠ The MEASUREMENT is read-only; the access records above are not, and
        # were committed deliberately before the look. This discards the read
        # transaction only. Stated because the connection context manager
        # commits on a clean exit by default.
        conn.rollback()

    print(f"\ntotal {time.monotonic() - started:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
