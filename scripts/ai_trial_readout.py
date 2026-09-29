"""#3471 §9 readout of the AI-discretionary-v1 demo trial. Read-only; never calls the model (§10).

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §9. The arithmetic lives in
``app/services/ai_trial_readout.py``; this prints it.

Before the cohort readout is due (§9 "When the readout runs") it prints the censuses and the harm
looks only — no primary number is computed early.

Usage::

    PYTHONPATH=. uv run python -m scripts.ai_trial_readout [--json out.json]
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
from app.services.ai_trial_readout import Readout, ReadoutUnavailable, compute_readout


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
    ]
    for look in readout.harm_looks:
        lines.append(
            f"harm look {look.k}: units {look.units}, clusters {look.clusters}, p(<0) {look.p_less}, "
            f"threshold {look.threshold:.6f}, halts {look.halts}" + (f" ({look.skipped})" if look.skipped else "")
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
        f"control: {readout.control}",
        f"capital-weighted d: {readout.capital_weighted_d_pct} pp",
        f"per regime: {readout.per_regime}",
        f"per confidence (not a calibration): {readout.per_confidence}",
        f"exit labels: {readout.exit_labels}  order parity: {readout.order_parity}",
        f"exploratory units (after the cohort): {readout.exploratory_units}",
        "Not computed: SPY references, exposure, turnover, fill-versus-ask gap (see ai_trial_readout).",
        "Dividends are not in the closed-trade history, so not in the net.",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3471 §9 readout of the AI-discretionary-v1 demo trial")
    parser.add_argument("--json", type=Path, help="also write the readout as JSON here")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        try:
            readout = compute_readout(conn)
        except ReadoutUnavailable as exc:
            print(f"no readout: {exc}")
            return 0
    print(render(readout))
    if args.json:
        args.json.write_text(json.dumps(dataclasses.asdict(readout), default=_jsonable, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
