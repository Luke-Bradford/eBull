"""#3238 — full-population A/B: does deriving the cost basis move ONLY the charge?

    PYTHONPATH=. uv run python -m scripts.ab_3238_cost_basis --strategy s1-time-series-momentum

⚠ READ-ONLY. No write, no migration, no broker call, no result row. Both arms
run in memory and nothing is persisted; ``run_backtest``'s collision check and
ledger writer are deliberately not on this path.

THE TWO ARMS
------------
Both are the REAL runner — ``evaluate_level_arms`` / ``evaluate_arm``, the
functions that produce every stored result — over the SAME ``_Corpus`` object.
Neither arm is reconstructed from ``cost_position`` directly, which is what
made the first draft of this design invalid: a component comparison does not
exercise the runner, and the runner is what the fix changes.

- **treatment** — ``load_corpus`` as shipped. Its ``cost_price_basis`` comes
  from ``_resolve_liquidity_policy``, so this arm exercises the resolver under
  test rather than a hardcoded ``as_traded``.
- **control** — ``replace(corpus, cost_price_basis="split_adjusted")``. That is
  exactly the literal #3238 removes, applied through the shipped code path, so
  the control is the OLD BEHAVIOUR REPRODUCED and not simulated.

⚠⚠ ``liquidity_policy`` IS DELIBERATELY NOT REPLACED. It feeds the
entry-liquidity and exit-gap diagnostics as well as the basis; swapping it
would move those too, and their movement would be indistinguishable from the
effect being measured. Replacing the derived field alone is what makes
"only the charge moved" a testable claim.

WHAT IS ASSERTED, AND WHY EACH ONE IS THE RIGHT CHECK
----------------------------------------------------
The gate is that the two arms differ in the CHARGE and in nothing else:

- ``gross_returns`` EQUAL ELEMENT BY ELEMENT over the whole tuple. This is the
  per-leg comparison. The tuple is appended in one deterministic corpus sweep,
  so equal length plus element-wise equality means every leg matched AND kept
  its position — strictly stronger than a set difference, which would hide a
  reordering and, under duplicate values, a swap.
- ``daily_returns`` keys equal — the realised entry-date population.
- ``position_count``, ``metrics.trade_count``, ``termination_census`` and
  ``label_starts`` / ``label_ends`` equal. ``position_count`` counts positions
  §3.4 excluded as uncosted too, so an exclusion that moved shows up here even
  though it never reached a return.
- the synthetic control's ``matched_trade_count`` and ``unmatchable`` reasons
  equal, with the same seed — the null must permute the same holds into the
  same placements and differ only in what it is charged for them.

And that the charge DID move:

- ``half_spreads`` — reported as a set per arm, never asserted to change
  cardinality. A correct run may legitimately hold one band (every entry in the
  same band) or none (no realised leg), so 1 -> >1 is a diagnostic and not a
  gate.
- net expectancy per trade and profit factor, per namespace AND per regime
  cohort — the decision metrics (``cost-aware-viability.md``; CAGR, Sharpe and
  Sortino are banned as decision metrics and are not reported here as such).

⚠ EXPECTED DIRECTION, DECLARED BEFORE THE RUN: net expectancy rises on the
treatment arm for the strategy AND for its buy-and-hold comparator AND for its
synthetic control, because all three are overcharged today. A run in which the
strategy improves while the comparator or the null does not is a FAILED A/B —
it means a charge site was missed — and is not a promising result.

SCOPE BOUNDS, STATED RATHER THAN LEFT TO A READER
-------------------------------------------------
⚠ ``--limit`` makes this NOT a full-population figure and the report says so.
⚠ The cohort size defaults BELOW ``SPEC_COHORT_SIZE``: this is an arm-vs-arm
  comparison, not a promotion, so the null needs enough members to prove its
  charge moved and its placement did not — not the production cohort. The
  number used is printed.
⚠ ``in_sample`` ONLY. A hold-out pass is an access against withheld data under
  criterion 5 and needs declaration authority this diagnostic does not have.
⚠ On ``survivorship_free`` the termination map is non-empty, so every strategy
  routes through ``evaluate_level_arms`` and the ``evaluate_arm`` call site is
  NOT exercised by this run. That site is reachable only on ``survivor_only``,
  which is split-adjusted and therefore cannot change basis — so the fix there
  is correct-but-dormant, and this script reports that rather than implying a
  coverage it does not have.
⚠ Each arm re-reads bars from the database; the corpus object carries ids and a
  calendar, not prices. The ``gross_returns`` equality assertion doubles as the
  detector for that — a mid-run ingest would break it.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from typing import Any

import psycopg

from app.config import settings
from app.services import backtest_run
from app.services.backtest_run import (
    QUARANTINE_ARM_ORDER,
    ArmMeasurement,
    NamespaceMeasurement,
    _Corpus,
    _regime_for,
    evaluate_arm,
    evaluate_level_arms,
    load_corpus,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result_identity import BACKTEST_UNIVERSE

#: Enough members to prove the null's charge moved while its placement did not.
#: ⚠ NOT ``SPEC_COHORT_SIZE``: see the scope bounds in the header.
DEFAULT_COHORT_SIZE = 24


def _arms(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    corpus: _Corpus,
    regime_provider: MarketRegimeProvider,
    cohort_size: int | None,
) -> dict[str, ArmMeasurement]:
    """Every ``(quarantine, ambiguity)`` arm this strategy produces, keyed."""
    entry = STRATEGY_MANIFEST[strategy_id]
    identity = entry.identity(universe=corpus.universe_basis, cost_model_id=COST_MODEL_ID)
    out: dict[str, ArmMeasurement] = {}
    for quarantine_arm in QUARANTINE_ARM_ORDER:
        if _regime_for(entry, corpus.axis).level_based or corpus.termination:
            measured = evaluate_level_arms(
                conn,
                entry,
                corpus=corpus,
                quarantine_arm=quarantine_arm,
                identity=identity,
                namespaces=("in_sample",),
                cohort_size=cohort_size,
                regime_provider=regime_provider,
            )
        else:
            measured = (
                evaluate_arm(
                    conn,
                    entry,
                    corpus=corpus,
                    quarantine_arm=quarantine_arm,
                    ambiguity_arm=None,
                    identity=identity,
                    namespaces=("in_sample",),
                    cohort_size=cohort_size,
                    regime_provider=regime_provider,
                ),
            )
        for arm in measured:
            # ⚠ The key carries BOTH quarantine and ambiguity. Identical
            # name/namespace/date tuples recur across those arms, so a key that
            # omitted them would silently compare one arm against another.
            out[f"{quarantine_arm}/{arm.ambiguity_arm or 'shared'}"] = arm
    return out


def _unchanged(control: NamespaceMeasurement, treatment: NamespaceMeasurement) -> list[str]:
    """Everything that MUST be identical. Returns the violations, not a bool."""
    broken: list[str] = []
    if control.gross_returns != treatment.gross_returns:
        n = min(len(control.gross_returns), len(treatment.gross_returns))
        first = next(
            (i for i in range(n) if control.gross_returns[i] != treatment.gross_returns[i]),
            None,
        )
        broken.append(
            f"gross_returns differ: {len(control.gross_returns):,} vs {len(treatment.gross_returns):,} legs, "
            f"first positional mismatch at {first}"
        )
    if control.daily_returns.keys() != treatment.daily_returns.keys():
        only_control = sorted(set(control.daily_returns) - set(treatment.daily_returns))
        only_treatment = sorted(set(treatment.daily_returns) - set(control.daily_returns))
        broken.append(f"entry-date population differs: -{len(only_control)} +{len(only_treatment)}")
    if control.position_count != treatment.position_count:
        broken.append(f"position_count {control.position_count:,} vs {treatment.position_count:,}")
    if control.metrics.trade_count != treatment.metrics.trade_count:
        broken.append(f"trade_count {control.metrics.trade_count:,} vs {treatment.metrics.trade_count:,}")
    if dict(control.termination_census) != dict(treatment.termination_census):
        broken.append(f"termination_census {dict(control.termination_census)} vs {dict(treatment.termination_census)}")
    if (list(control.label_starts), list(control.label_ends)) != (
        list(treatment.label_starts),
        list(treatment.label_ends),
    ):
        broken.append("criterion-5 label windows differ")
    if {cohort.regime for cohort in control.regime_cohorts} != {cohort.regime for cohort in treatment.regime_cohorts}:
        broken.append("regime cohort population differs")
    return broken


def _charge_report(control: NamespaceMeasurement, treatment: NamespaceMeasurement) -> dict[str, Any]:
    """What the charge did — reported, never gated on a direction."""
    by_regime = {}
    control_regimes = {cohort.regime: cohort for cohort in control.regime_cohorts}
    for cohort in treatment.regime_cohorts:
        before = control_regimes.get(cohort.regime)
        if before is None:
            continue
        by_regime[str(cohort.regime)] = {
            "trades": cohort.trade_count,
            "expectancy_per_trade_pct": [
                _num(before.expectancy_pct),
                _num(cohort.expectancy_pct),
            ],
            "profit_factor": [_num(before.profit_factor), _num(cohort.profit_factor)],
        }
    return {
        "half_spreads_control": sorted(control.half_spreads),
        "half_spreads_treatment": sorted(treatment.half_spreads),
        "expectancy_per_trade_pct": [
            _num(control.metrics.expectancy_per_trade_pct),
            _num(treatment.metrics.expectancy_per_trade_pct),
        ],
        "profit_factor": [_num(control.metrics.profit_factor), _num(treatment.metrics.profit_factor)],
        "return_vs_buy_and_hold_pct": [
            _num(control.metrics.return_vs_buy_and_hold_pct),
            _num(treatment.metrics.return_vs_buy_and_hold_pct),
        ],
        "total_return_pct": [_num(control.metrics.total_return_pct), _num(treatment.metrics.total_return_pct)],
        "buy_and_hold_return_pct": [
            _num(control.metrics.buy_and_hold_return_pct),
            _num(treatment.metrics.buy_and_hold_return_pct),
        ],
        "by_regime": by_regime,
    }


def _num(value: Any) -> float | None:
    return None if value is None else float(value)


def _cohort_report(control: ArmMeasurement, treatment: ArmMeasurement) -> tuple[dict[str, Any], list[str]]:
    """The null's own before/after, plus the invariants its placement must keep."""
    broken: list[str] = []
    if (control.cohort is None) != (treatment.cohort is None):
        broken.append(f"one arm produced a cohort and the other did not ({control.cohort_refusal or ''!r})")
        return {}, broken
    if control.cohort is None or treatment.cohort is None:
        return {"not_run": control.cohort_refusal or treatment.cohort_refusal}, broken
    before, after = control.cohort, treatment.cohort
    if before.residual.strategy_trade_count != after.residual.strategy_trade_count:
        broken.append(
            f"cohort matched {before.residual.strategy_trade_count:,} vs {after.residual.strategy_trade_count:,} trades"
        )
    if dict(before.unmatchable) != dict(after.unmatchable):
        broken.append("cohort unmatchable reasons differ")
    return {
        "members": after.control.cohort_size,
        "matched_trades": after.residual.strategy_trade_count,
        "control_mean_return_pct": [before.control.mean_return_pct, after.control.mean_return_pct],
        "strategy_return_pct": [before.control.strategy_return_pct, after.control.strategy_return_pct],
        "cohort_mean_trade_count": [before.residual.cohort_mean_trade_count, after.residual.cohort_mean_trade_count],
        "mean_ci_contains_zero": [
            before.control.mean_return_ci_contains_zero,
            after.control.mean_return_ci_contains_zero,
        ],
        "sharpe_exceeds_cohort": [before.control.sharpe_exceeds_cohort, after.control.sharpe_exceeds_cohort],
    }, broken


def _charge_moved(control: NamespaceMeasurement, treatment: NamespaceMeasurement) -> list[str]:
    """Every charge CONSUMER must move, or a site was missed (ckpt-2, #3238).

    ⚠⚠ THE HEADER DECLARES AN UNCHANGED BENCHMARK OR NULL A FAILED A/B, so this
    has to be a GATE and not a printed number. Reporting the three movements and
    exiting 0 regardless is how a partial fix ships wearing a green tick: the
    strategy gets cheaper, its comparator does not, and
    ``return_vs_buy_and_hold_pct`` improves for a reason nobody measured.

    ⚠ ``buy_and_hold_return_pct`` is read DIRECTLY and not inferred from
    ``return_vs_buy_and_hold_pct``, which is the difference of two moving
    numbers and can sit still while both of its terms move.

    ⚠ NOT A DIRECTION TEST. It asserts the charge MOVED, not that it fell — a
    direction assertion on a population whose entries all sit in the maximum
    band would reject a correct run.
    """
    unmoved: list[str] = []
    if not control.half_spreads:
        # No realised leg: nothing was charged on either side, so "it moved" is
        # not a question this namespace can answer either way.
        return unmoved
    if control.half_spreads == treatment.half_spreads:
        unmoved.append(
            f"the STRATEGY's charge did not move: both arms charged {sorted(control.half_spreads)} — "
            "the cost_positions call sites are still on one basis"
        )
    if control.metrics.buy_and_hold_return_pct == treatment.metrics.buy_and_hold_return_pct:
        unmoved.append(
            "the BUY-AND-HOLD COMPARATOR's charge did not move "
            f"({control.metrics.buy_and_hold_return_pct}) — _benchmark_book is still on the maximum band, so "
            "return_vs_buy_and_hold_pct is flattering the strategy"
        )
    return unmoved


def _null_moved(control: ArmMeasurement, treatment: ArmMeasurement) -> list[str]:
    """The synthetic control is the third consumer and the easiest to forget."""
    if control.cohort is None or treatment.cohort is None:
        return []
    if control.cohort.control.mean_return_pct == treatment.cohort.control.mean_return_pct:
        return [
            "the SYNTHETIC CONTROL's charge did not move "
            f"({control.cohort.control.mean_return_pct}) — the null is still costed at the maximum band while "
            "the strategy is not, which manufactures a synthetic_control_passed the run did not earn"
        ]
    return []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", required=True, choices=sorted(STRATEGY_MANIFEST))
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="smoke slice; the report marks itself NOT full-population when set",
    )
    parser.add_argument("--cohort-size", type=int, default=DEFAULT_COHORT_SIZE)
    parser.add_argument("--no-cohort", action="store_true", help="skip the synthetic control entirely")
    args = parser.parse_args()

    cohort_size = None if args.no_cohort else args.cohort_size
    with psycopg.connect(settings.database_url) as conn:
        treatment_corpus = load_corpus(conn, universe_basis=BACKTEST_UNIVERSE, limit=args.limit)
        if treatment_corpus.cost_price_basis != "as_traded":
            raise RuntimeError(
                f"the {BACKTEST_UNIVERSE} corpus resolved cost basis "
                f"{treatment_corpus.cost_price_basis!r}, so the treatment arm would be identical to the control "
                "and this A/B would report a false green — check _resolve_liquidity_policy's warnings"
            )
        control_corpus = replace(treatment_corpus, cost_price_basis="split_adjusted")
        regime_provider = MarketRegimeProvider.load_research(conn)

        header = {
            "universe": treatment_corpus.universe_basis,
            "strategy": args.strategy,
            "cost_model_id": COST_MODEL_ID,
            "series": len(treatment_corpus.pairs),
            "full_population": args.limit is None,
            "cohort_size": cohort_size,
            "spec_cohort_size": backtest_run.SPEC_COHORT_SIZE,
            "control_basis": control_corpus.cost_price_basis,
            "treatment_basis": treatment_corpus.cost_price_basis,
            "evaluate_arm_site_exercised": not (treatment_corpus.termination != {}),
        }
        print(json.dumps({"header": header}, indent=2, default=str), flush=True)
        if args.limit is not None:
            print(f"⚠ --limit {args.limit} — NOT a full-population figure", flush=True)

        control_arms = _arms(
            conn,
            strategy_id=args.strategy,
            corpus=control_corpus,
            regime_provider=regime_provider,
            cohort_size=cohort_size,
        )
        treatment_arms = _arms(
            conn,
            strategy_id=args.strategy,
            corpus=treatment_corpus,
            regime_provider=regime_provider,
            cohort_size=cohort_size,
        )

    if control_arms.keys() != treatment_arms.keys():
        raise RuntimeError(f"arm sets differ: {sorted(control_arms)} vs {sorted(treatment_arms)}")

    violations: list[str] = []
    unmoved: list[str] = []
    report: dict[str, Any] = {}
    for key in sorted(control_arms):
        control, treatment = control_arms[key], treatment_arms[key]
        if control.namespaces.keys() != treatment.namespaces.keys():
            violations.append(f"{key}: namespace sets differ")
            continue
        arm_report: dict[str, Any] = {}
        for namespace in sorted(control.namespaces):
            broken = _unchanged(control.namespaces[namespace], treatment.namespaces[namespace])
            violations.extend(f"{key}/{namespace}: {reason}" for reason in broken)
            unmoved.extend(
                f"{key}/{namespace}: {reason}"
                for reason in _charge_moved(control.namespaces[namespace], treatment.namespaces[namespace])
            )
            arm_report[namespace] = _charge_report(control.namespaces[namespace], treatment.namespaces[namespace])
        cohort, cohort_broken = _cohort_report(control, treatment)
        violations.extend(f"{key}/cohort: {reason}" for reason in cohort_broken)
        unmoved.extend(f"{key}/cohort: {reason}" for reason in _null_moved(control, treatment))
        arm_report["synthetic_control"] = cohort
        if dict(control.close_sources) != dict(treatment.close_sources):
            violations.append(f"{key}: close_sources differ")
        report[key] = arm_report

    print(json.dumps({"arms": report}, indent=2, default=str), flush=True)
    if violations:
        print(f"\n⛔ {len(violations)} INVARIANT VIOLATION(S) — the change moved more than the charge:", flush=True)
        for reason in violations:
            print(f"  - {reason}", flush=True)
    if unmoved:
        print(f"\n⛔ {len(unmoved)} CHARGE CONSUMER(S) DID NOT MOVE — a partial fix flatters the comparison:")
        for reason in unmoved:
            print(f"  - {reason}", flush=True)
    if violations or unmoved:
        raise SystemExit(1)
    print(
        "\n✅ every leg, hold, gross return, exclusion and termination is identical; "
        "the strategy, its buy-and-hold comparator and its synthetic control ALL re-priced."
    )


if __name__ == "__main__":
    main()
