"""Hunt 2's one discovery look, and its flag (#3448).

Spec ``docs/proposals/ta/2026-09-27-3448-hunt-2-declaration.md``: "The trial" fixes every
field of :func:`build_spec`; "Discovery flag → validation" fixes :func:`discovery_flag`.
``HUNT_BUDGETS`` gives hunt 2 exactly one discovery row, so ``--run`` is the whole search:
any registered row counts, whatever its outcome, and nothing is re-screened.

    PYTHONPATH=. uv run python -m scripts.run_hunt_2_discovery            # refusals only, registers nothing
    PYTHONPATH=. uv run python -m scripts.run_hunt_2_discovery --run --by "<session>"
    PYTHONPATH=. uv run python -m scripts.run_hunt_2_discovery --readout  # the stored outcome's flag
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any, Final

import psycopg

from app.config import settings
from app.services import hunt_door
from app.services import hunt_harness as hh
from app.services.hunt_inference import StatRefused

HUNT_ID: Final = "hunt-2"
SIGNAL_ID: Final = "extreme_move_illiquid:score"
#: SC1: annualised net excess over the tracker, in every base AND stress cell (spec "The bar").
EXCESS_BAR: Final = 0.08

CONDITION_LABELS: Final = {
    "by_flag": "undetermined",
    "arm_beats_control": "arm does not beat control",
    "corrected_bar": "below the corrected bar (point estimate)",
    "power_at_bar": "underpowered at the bar",
}


def build_spec(universe_identity: hh.UniverseIdentity) -> hh.TrialSpec:
    """``extreme_loser_reversal_illiquid_next_open_v1``: hunt 1's trial with next-open entry."""
    signal_sha256 = hh.signal_code_sha256(SIGNAL_ID)
    cost_model_id = hh.running_cost_model_id("real_stock_long_x1")
    if signal_sha256 is None or cost_model_id is None:
        raise SystemExit(f"signal {SIGNAL_ID} or the lane's cost identity is missing")
    return hh.TrialSpec(
        hunt_id=HUNT_ID,
        family="extreme_move_illiquid",
        split="discovery",
        signal_id=SIGNAL_ID,
        signal_code_sha256=signal_sha256,
        sign=-1,
        selection=0.05,
        lag=1,
        h=5,
        entry_point="open",
        exit_point="close",
        constants={"history": 26, "amihud_sessions": 20, "formation": 5, "min_close": 5.0, "illiquid_divisor": 3},
        weighting="equal",
        lane="real_stock_long_x1",
        universe_identity=universe_identity,
        calendar_identity=hh.calendar_identity(),
        cost_model_id=cost_model_id,
        harness_model_id=hh.HUNT_HARNESS_MODEL_ID,
        survivor_bias_direction="favours_arm",
        survivor_bias_reason=(
            "illiquid losers are the likeliest to terminate; every series trading before 2013-06-21 survives "
            "to it, so failed firms are missing from both books. Declared, not corrected"
        ),
        mechanism=(
            "liquidity provision: non-informational selling in illiquid names clears at a concession that "
            "reverts within days (Avramov, Chordia & Goyal 2006; Nagel 2012); predicts the arm beats the control "
            "and clears 8%/yr annualised net excess over SPY in every base and stress cell"
        ),
        competing_explanation=(
            "print noise at the t+1 open (a stale or bid-side first print); informed selling drifts on "
            "(#2481's continuation); real post-loss spreads exceed the charge"
        ),
    )


def excess_power(excess_series: Sequence[float] | None) -> dict[str, Any]:
    """Condition 4's input: the door's power formula on the discovery excess series, at the
    validation grid's length (only the validation calendar is read)."""
    grid = hh.split_grid("validation", lag=1, h=5)
    if isinstance(grid, StatRefused):
        return {"blocked": f"target_{grid.reason}"}
    return hunt_door.power_statement(excess_series, target_observations=len(grid.sessions), h=5)


def discovery_flag(
    status: str, statistics: Mapping[str, Any], *, by_flagged: bool, power: Mapping[str, Any]
) -> dict[str, Any]:
    """The spec's four conditions. A refused cell, a missing readout, a non-finite input or
    zero entered arm positions is the condition not met."""
    cells = statistics.get("cells")
    cells = cells if isinstance(cells, Mapping) else {}
    tracker = statistics.get("tracker")
    tracker = tracker if isinstance(tracker, Mapping) else {}
    tracker_cells = tracker.get("cells")
    tracker_cells = tracker_cells if isinstance(tracker_cells, Mapping) else {}
    per_trade = statistics.get("per_trade")
    per_trade = per_trade if isinstance(per_trade, Mapping) else {}

    base = sorted(key for key in cells if key.endswith("|base"))
    active = {
        key: cells[key].get("mean") if isinstance(cells[key], Mapping) and "refused" not in cells[key] else None
        for key in base
    }
    excess: dict[str, Any] = {}
    for key in sorted(cells):
        entry, trades = tracker_cells.get(key), per_trade.get(key)
        arm = trades.get("arm") if isinstance(trades, Mapping) else None
        entered = isinstance(arm, Mapping) and bool(arm.get("positions"))
        # A cell whose statistics refused is unavailable, even though its books computed.
        usable = isinstance(cells[key], Mapping) and "refused" not in cells[key]
        value = entry.get("excess_ann") if isinstance(entry, Mapping) else None
        excess[key] = value if usable and entered and isinstance(value, float) else None
    mde = power.get("min_detectable_annual_mean_80")
    met = {
        "by_flag": status != "abandoned" and by_flagged,
        "arm_beats_control": bool(base) and all(isinstance(v, float) and v > 0.0 for v in active.values()),
        "corrected_bar": len(excess) == 2 * len(base) > 0
        and all(isinstance(v, float) and v >= EXCESS_BAR for v in excess.values()),
        "power_at_bar": isinstance(mde, str) and float(mde) <= EXCESS_BAR,
    }
    worst = min(excess.items(), key=lambda item: (item[1] is not None, item[1] or 0.0)) if excess else None
    conditions: dict[str, Any] = {}
    for name, ok in met.items():
        conditions[name] = {"met": ok} if ok else {"met": False, "label": CONDITION_LABELS[name]}
    if not met["corrected_bar"] and worst is not None:
        conditions["corrected_bar"]["worst_cell"] = {"cell": worst[0], "excess_ann": worst[1]}
    return {
        "status": status,
        "conditions": conditions,
        "active_mean_by_base_cell": active,
        "excess_ann_by_cell": excess,
        "power_at_bar": dict(power),
        "declare_validation": all(met.values()),
    }


_DISCOVERY_ROW: Final = """
    SELECT hunt_trial_id FROM hunt_trials WHERE hunt_id = %(hunt)s AND split = 'discovery' AND purpose = 'evaluate'
"""


def _readout(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    row = conn.execute(_DISCOVERY_ROW, {"hunt": HUNT_ID}).fetchone()
    if row is None:
        return {"readout": "no hunt-2 discovery row"}
    trial_id = int(row[0])
    outcome = hh.read_outcome(conn, trial_id)
    by, p_values = hunt_door.discovery_by_readout(conn)
    conn.commit()
    tracker = outcome.statistics.get("tracker")
    series = tracker.get("canonical_excess_series") if isinstance(tracker, Mapping) else None
    flag = discovery_flag(
        outcome.status, outcome.statistics, by_flagged=trial_id in by.flagged, power=excess_power(series)
    )
    header = {k: v for k, v in tracker.items() if k not in ("cells", "canonical_excess_series")} if tracker else {}
    return {
        "hunt_trial_id": trial_id,
        "outcome_sha256": outcome.outcome_sha256,
        "by": {"m": by.m, "c_m": by.c_m, "q": by.q, "p": p_values.get(trial_id)},
        **flag,
        "tracker": header,
        "tracker_cells": tracker.get("cells") if isinstance(tracker, Mapping) else None,
        "per_trade": outcome.statistics.get("per_trade", {}),
        "regimes": outcome.statistics.get("regimes"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hunt 2's one discovery look (#3448).")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--run", action="store_true", help="register and compute the one discovery row")
    mode.add_argument("--readout", action="store_true", help="print the stored outcome's flag")
    parser.add_argument("--by", help="who registers the search (required with --run)")
    args = parser.parse_args(argv)
    if args.run and not args.by:
        parser.error("--run needs --by")
    with psycopg.connect(settings.database_url) as conn:
        if args.readout:
            print(json.dumps(_readout(conn), indent=2, default=str))
            return 0
        spec = build_spec(hh.read_universe_identity(conn, "survivorship_free"))
        conn.commit()
        print(f"spec_sha256 {spec.spec_sha256}\ncandidate_sha256 {spec.candidate_sha256}")
        if not args.run:
            with hh.hunt_programme_lock(conn):
                refusal = hh.refusal_before_registration(conn, spec)
                conn.commit()
            print(f"refusal before registration: {refusal}")
            return 0 if refusal is None else 1
        result = hh.evaluate(conn, spec, registered_by=args.by)
        if isinstance(result, hh.HuntRefused):
            print(f"refused: {result.reason}: {result.detail}", file=sys.stderr)
            return 1
        print(json.dumps(_readout(conn), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
