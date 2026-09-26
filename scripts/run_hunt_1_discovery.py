"""Hunt 1's one discovery look, and its flag (#3387).

Spec ``docs/proposals/ta/2026-09-26-3387-hunt-1-route-a-spec.md``: "The trial" fixes every
field of :func:`build_spec`; "Discovery flag" fixes :func:`discovery_flag`. ``HUNT_BUDGETS``
gives hunt 1 exactly one discovery row, so ``--run`` is the whole search: any registered
row counts, whatever its outcome, and nothing is re-screened.

    PYTHONPATH=. uv run python -m scripts.run_hunt_1_discovery            # refusals only, registers nothing
    PYTHONPATH=. uv run python -m scripts.run_hunt_1_discovery --run --by "<session>"
    PYTHONPATH=. uv run python -m scripts.run_hunt_1_discovery --readout  # the stored outcome's flag
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from typing import Any, Final

import psycopg

from app.config import settings
from app.services import hunt_door
from app.services import hunt_harness as hh

HUNT_ID: Final = "hunt-1"
SIGNAL_ID: Final = "extreme_move_illiquid:score"
#: Condition 3: arm per-trade net mean ≥ 1.22% in every base cell (spec "Recorded spreads":
#: 1.01·q(0.322)/q(0.539) − 1 = 1.2194%, q(x) = (1 − x/2)/(1 + x/2), rounded up).
ARM_PER_TRADE_NET_BAR: Final = 0.0122

CONDITION_LABELS: Final = {
    "by_flag": "undetermined",
    "arm_beats_control": "arm does not beat control",
    "operator_bar": "below the operator bar (point estimate)",
}


def build_spec(universe_identity: hh.UniverseIdentity) -> hh.TrialSpec:
    """``extreme_loser_reversal_illiquid_v1``, field for field from the spec's trial table."""
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
        entry_point="close",
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
            "reverts within days (Avramov, Chordia & Goyal 2006; Nagel 2012)"
        ),
        competing_explanation=(
            "pre-decimal tick bounce (most of discovery precedes 2001 decimalisation); informed selling drifts "
            "on (#2481's continuation); real post-loss spreads exceed the charge"
        ),
    )


def discovery_flag(status: str, statistics: Mapping[str, Any], *, by_flagged: bool) -> dict[str, Any]:
    """The spec's three conditions, each over every base cell. A refused cell, a missing
    readout or zero entered arm positions is the condition not met."""
    cells = statistics.get("cells")
    per_trade = statistics.get("per_trade")
    cells = cells if isinstance(cells, Mapping) else {}
    per_trade = per_trade if isinstance(per_trade, Mapping) else {}
    base = sorted(key for key in cells if key.endswith("|base"))
    active: dict[str, Any] = {}
    arm: dict[str, Any] = {}
    for key in base:
        cell = cells[key]
        mean = cell.get("mean") if isinstance(cell, Mapping) and "refused" not in cell else None
        active[key] = mean
        entry = per_trade.get(key)
        book = entry.get("arm") if isinstance(entry, Mapping) else None
        arm[key] = book.get("mean") if isinstance(book, Mapping) and book.get("positions") else None
    met = {
        "by_flag": status != "abandoned" and by_flagged,
        "arm_beats_control": bool(base) and all(isinstance(v, float) and v > 0.0 for v in active.values()),
        "operator_bar": bool(base) and all(isinstance(v, float) and v >= ARM_PER_TRADE_NET_BAR for v in arm.values()),
    }
    return {
        "status": status,
        "conditions": {
            name: {"met": ok, **({} if ok else {"label": CONDITION_LABELS[name]})} for name, ok in met.items()
        },
        "active_mean_by_base_cell": active,
        "arm_per_trade_net_by_base_cell": arm,
        "declare_validation": all(met.values()),
    }


_DISCOVERY_ROW: Final = """
    SELECT hunt_trial_id FROM hunt_trials WHERE hunt_id = %(hunt)s AND split = 'discovery' AND purpose = 'evaluate'
"""


def _discovery_trial_id(conn: psycopg.Connection[Any]) -> int | None:
    row = conn.execute(_DISCOVERY_ROW, {"hunt": HUNT_ID}).fetchone()
    return None if row is None else int(row[0])


def _readout(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    trial_id = _discovery_trial_id(conn)
    if trial_id is None:
        return {"readout": "no hunt-1 discovery row"}
    outcome = hh.read_outcome(conn, trial_id)
    by, p_values = hunt_door.discovery_by_readout(conn)
    conn.commit()
    flag = discovery_flag(outcome.status, outcome.statistics, by_flagged=trial_id in by.flagged)
    per_trade = outcome.statistics.get("per_trade", {})
    return {
        "hunt_trial_id": trial_id,
        "outcome_sha256": outcome.outcome_sha256,
        "by": {"m": by.m, "c_m": by.c_m, "q": by.q, "p": p_values.get(trial_id)},
        **flag,
        "per_trade": per_trade,
        "regimes": outcome.statistics.get("regimes"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hunt 1's one discovery look (#3387).")
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
