"""#3471 §9 readout of the AI-discretionary-v1 demo trial. Read-only; never calls the model (§10).

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §9. The arithmetic lives in
``app/services/ai_trial_readout.py``; this prints it.

Before the cohort readout is due (§9 "When the readout runs") it prints the censuses and the harm
looks only — no primary number is computed early.

Usage::

    PYTHONPATH=. uv run python -m scripts.ai_trial_readout [--version v1] [--json out.json]
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.ai_trial_pair_lifecycle import LEGS
from app.services.ai_trial_readout import Readout, ReadoutUnavailable, compute_readout
from app.services.ai_trial_run import TRIAL_STRATEGY_VERSION


def _jsonable(value: Any) -> Any:
    if isinstance(value, date | datetime):
        return value.isoformat()
    raise TypeError(f"not JSON-serialisable: {type(value).__name__}")


def render(readout: Readout) -> str:
    lines = [
        f"AI-discretionary-v1 readout — declaration {readout.declaration_id} ({readout.strategy_version}), "
        f"as of {readout.as_of.isoformat()}",
        f"cohort: {readout.cohort.status} — {readout.cohort.detail}",
        f"pairs: {readout.pair_states}  broken: {readout.broken_reasons}  unvalued legs: {readout.unvalued_reasons}",
        f"runs: {readout.run_census}  decisions: {readout.decision_census}",
        f"model cost: ${readout.model_cost_usd_total:.2f} total, per run {readout.model_cost_usd_per_run}",
        f"restated close rows: {readout.restated_rows}  close rows with a nonzero fee: {readout.fee_rows}",
        f"control-pool size by pair_seq: {readout.pool_sizes}",
        f"fill vs ask (%, + = filled above the priced ask): {readout.fill_vs_ask}",
    ]
    for look in readout.harm_looks:
        lines.append(
            f"harm look {look.k}: units {look.units}, clusters {look.clusters}, p(<0) {look.p_less}, "
            f"threshold {look.threshold:.6f}, halts {look.halts}, flows final {look.flows_final}"
            + (f" ({look.skipped})" if look.skipped else "")
        )
    if readout.primary is None:
        lines.append("primary: not computed — the cohort readout is not due")
        return "\n".join(lines)
    p = readout.primary
    lines += [
        f"PRIMARY: units {p.units}, clusters {p.clusters}, mean d {p.mean_d} pp, sign-flip p {p.p}",
        f"  verdict: {p.verdict}",
        f"O12 mechanical-exits only: {readout.mechanical_only}",
        f"arm: {readout.arm}",
        f"arm mean net interval (house C3 block bootstrap; coverage NOT reliable, no profitability claim): "
        f"{readout.arm_interval}",
        f"control: {readout.control}",
        f"capital-weighted d: {readout.capital_weighted_d_pct} pp",
        f"per regime: {readout.per_regime}",
        f"per confidence (not a calibration): {readout.per_confidence}",
        f"exit labels: {readout.exit_labels}  order parity: {readout.order_parity}",
        f"exploratory units (after the cohort): {readout.exploratory_units}",
        "SPY references (unmeasured approximations of executable returns, close-based):",
        f"  per pair (arm's own sessions, no spread): {readout.spy_per_pair}",
        f"  capital level (buy-and-hold on the arm's capital, one round-trip spread): {readout.spy_capital}",
        f"turnover (Σ opened ÷ mean committed, per 20 sessions): {readout.turnover}",
        f"exposure at the decision (not matched between legs, §9): {readout.exposure}",
        "Dividends are not in the closed-trade history, so not in the net.",
    ]
    plans = readout.plans
    if plans is None:
        return "\n".join(lines)
    lines += [
        "v6 structure plans (§16.7, §16.11(b)) — descriptive, small cells, no test, not like-for-like "
        "with the library:",
        f"  library {plans.library_sha256}: {plans.library_caveat}",
    ]
    for leg in LEGS:
        mix, cal = plans.exit_mix[leg], plans.calibration[leg]
        lines += [
            f"  {leg} exit mix over {mix.legs} legs: stop {mix.pct_stop} / target {mix.pct_target} / time "
            f"{mix.pct_time} %; library holdout (same weights) {mix.library_pct_stop} / "
            f"{mix.library_pct_target} / {mix.library_pct_time} %; other exits {mix.other}",
            *(
                f"    {c.setup_type} {c.horizon_days}d: {c.legs} legs, target hit {c.pct_target} % vs library "
                f"holdout {c.library_pct_target} %; other {c.other}"
                for c in mix.cells
            ),
            f"  {leg} calibration (not d): mean net R {cal.mean_net_r} over {cal.legs} legs vs library train "
            f"{cal.library_train_mean_net_r} / holdout {cal.library_holdout_mean_net_r}; counted {cal.counted}",
            *(
                f"    {c.setup_type} {c.horizon_days}d: {c.legs} legs, mean net R {c.mean_net_r} vs "
                f"{c.library_train_mean_net_r} / {c.library_holdout_mean_net_r}; counted {c.counted}"
                for c in cal.cells
            ),
            f"  {leg} geometry (cohort units): {plans.geometry[leg]}",
        ]
    lines += [
        f"  pairs {plans.pairs}, pool size {plans.pool_size}, self-draws {plans.self_draws}, "
        f"singleton-self pools {plans.singleton_self_pools}",
        f"  plan_invalidated per leg {plans.plan_invalidated}; pool exhausted per setup {plans.exhausted_by_setup}",
        f"  d by response position: {plans.d_by_response_position}",
        f"  d by selection contrast (secondary: 'contrast'): {plans.d_by_contrast}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3471 §9 readout of the AI-discretionary-v1 demo trial")
    parser.add_argument("--version", default=TRIAL_STRATEGY_VERSION, help="the declared strategy version")
    parser.add_argument("--json", type=Path, help="also write the readout as JSON here")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        try:
            readout = compute_readout(conn, strategy_version=args.version)
        except ReadoutUnavailable as exc:
            print(f"no readout: {exc}")
            return 0
    print(render(readout))
    if args.json:
        args.json.write_text(json.dumps(dataclasses.asdict(readout), default=_jsonable, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
