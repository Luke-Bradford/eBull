"""The ``hunt_gate`` block a gated hunt's validation declaration carries, and its promotion rule (#3454).

Spec ``docs/proposals/ta/2026-09-28-3454-hunt-3-spec.md``: "Flag → validation declaration"
fixes :func:`gate_block` (gating conditions 1-3 on the inherited discovery outcome, the stress
cells and ``excess_power`` reported, never gating); "At validation, one look" fixes
:func:`promotion`; "Hunt 3's identity" fixes :func:`decision_pins`.

The door (``hunt_door``) builds the block into the declaration and the freeze recomputes it.
``hunt_harness``'s membership check refuses the look, and the door refuses the readout, when
the frozen block is missing, malformed, or its decision pins no longer match the code
(:func:`look_codes`). The readout reads the bar and the t threshold from the FROZEN block,
never from these constants.

Pure except :func:`decision_pins`, which reads the pinned files. Imports nothing from
``hunt_harness`` or ``hunt_door`` (both import this module).
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any, Final

from app.services import hunt_inference, trial_register
from app.services.hunt_compute import CANONICAL_CELL, TRACKER_SOURCE, cell_key
from app.services.hunt_inference import StatRefused, Verdict
from app.services.r6_exclusion_trial import PROGRAMME_POLICIES

REPO_ROOT: Final = Path(__file__).resolve().parents[2]

#: Hunts whose validation declaration must carry a ``hunt_gate`` block: its freeze, look and
#: readout each refuse without a present, well-formed and current one (spec "Enforcement").
GATED_HUNTS: Final = frozenset({"hunt-3"})
HUNT_GATE_VERSION: Final = "hunt-gate-v1"
#: SC1's bar: annualised excess over the tracker, in every BASE cell (R1: stress is reported).
EXCESS_BAR: Final = 0.08
#: The validation excess series' significance threshold (condition 4 as amended).
T_THRESHOLD: Final = hunt_inference.T_BAR

BASE_CELLS: Final = tuple(
    sorted(cell_key(policy.label, dividends, "base") for policy in PROGRAMME_POLICIES for dividends in (True, False))
)
STRESS_CELLS: Final = tuple(
    sorted(cell_key(policy.label, dividends, "stress") for policy in PROGRAMME_POLICIES for dividends in (True, False))
)
CELL_ROLES: Final[Mapping[str, str]] = {
    **{key: "gated" for key in BASE_CELLS},
    **{key: "reported" for key in STRESS_CELLS},
}

#: The spec's disclosure, verbatim ("The disclosure registered with hunt 3").
DISCLOSURE: Final = (
    "Hunt 3 is hunt 2's construction (hunt_trial_id 2, candidate unchanged) re-registered after hunt 2's discovery "
    "look. Two discovery gates changed after that look, because of the shape of its result. The stress cells "
    "(1.450% round trip) are reported, not gated. The pre-look excess-power condition (MDE80 <= 0.08) became a "
    "post-look requirement: the validation excess over SPY must be significant at t > 3. Hunt 2 failed both old "
    "gates. Hunt 3's discovery evidence tests nothing. The validation window (2009-01-02 -> 2021-06-28) is the "
    "test. It is a previously examined window: the #3384 census lists 13 prior labels on it, including the s4 and "
    "s8 reversion runs and section 2.8's autocorrelation. Those are charged through M_inh. No hunt has read this "
    "candidate's validation returns."
)

#: Whole-file sha256 pins: every module that decides whether the declaration is accepted and
#: what the readout promotes. The statistics (NW, ``hunt_lag``) sit inside the model id.
PINNED_FILES: Final = (
    "scripts/run_hunt_3_validation.py",
    "app/services/hunt_gate.py",
    "app/services/hunt_door.py",
    "app/services/hunt_harness.py",
    "app/services/result_ledger.py",
    "app/services/prereg_contract.py",
    "app/services/strategy_result.py",
)
#: Pinned as CODE only: its register entries name the declaration's hash, so a whole-file
#: pin would be circular. The entries reach the declaration through M, which the freeze recomputes.
CODE_ONLY_PIN: Final = "app/services/trial_register.py#code"

_TRACKER_IDENTITY_KEYS: Final = (
    "source",
    "identity_sha256",
    "first_row",
    "first_return",
    "last_return",
    "sessions_used",
    "sessions_before_inception",
)
BLOCK_KEYS: Final = frozenset(
    {
        "version",
        "bar",
        "t_threshold",
        "cells",
        "conditions",
        "reported",
        "significance",
        "trackers",
        "disclosure",
        "inheritance",
        "decision_pins",
    }
)

#: A refusal code that closes the hunt (spec "Flag drift"). ``decision_pins`` drift at the
#: FREEZE is procedural (``gate_decision_pins_stale``: nothing was looked at; rebuild the
#: document from the merged code); from the freeze through the readout it is substantive.
SUBSTANTIVE_PREFIX: Final = "hunt_gate_"


def code_only_sha256(module: ModuleType) -> str:
    """sha256 over the source of ``module``'s own functions and classes and its compiled
    patterns, keyed by name. Data (register entries, version strings) is excluded."""
    parts: dict[str, str] = {}
    for name, value in sorted(vars(module).items()):
        if isinstance(value, re.Pattern):
            parts[name] = f"{value.pattern}\x00{value.flags}"
        elif (inspect.isfunction(value) or inspect.isclass(value)) and value.__module__ == module.__name__:
            parts[name] = inspect.getsource(value)
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()


def decision_pins(repo_root: Path = REPO_ROOT) -> dict[str, str]:
    """The decision procedure's pins as they stand now; a missing file pins as ``missing``."""
    pins = {
        path: hashlib.sha256((repo_root / path).read_bytes()).hexdigest() if (repo_root / path).is_file() else "missing"
        for path in PINNED_FILES
    }
    pins[CODE_ONLY_PIN] = code_only_sha256(trial_register)
    return pins


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite(value: Any) -> float | None:
    return (
        float(value)
        if isinstance(value, float | int) and not isinstance(value, bool) and math.isfinite(value)
        else None
    )


def cell_excess(statistics: Mapping[str, Any]) -> dict[str, float | None]:
    """``excess_ann`` per stored cell: ``None`` (unavailable) when the cell's statistics
    refused, its arm entered no position, or the value is missing or non-finite."""
    cells = _mapping(statistics.get("cells"))
    tracker_cells = _mapping(_mapping(statistics.get("tracker")).get("cells"))
    per_trade = _mapping(statistics.get("per_trade"))
    excess: dict[str, float | None] = {}
    for key in sorted(cells):
        usable = isinstance(cells[key], Mapping) and "refused" not in cells[key]
        arm = _mapping(per_trade.get(key)).get("arm")
        entered = isinstance(arm, Mapping) and bool(arm.get("positions"))
        value = _finite(_mapping(tracker_cells.get(key)).get("excess_ann"))
        excess[key] = value if usable and entered else None
    return excess


def tracker_utilisation(statistics: Mapping[str, Any]) -> Any:
    """The stored descriptive utilisation shares (the forward readout reports them beside discovery's)."""
    return _mapping(statistics.get("tracker")).get("utilisation")


def _key_codes(statistics: Mapping[str, Any]) -> list[str]:
    cells = set(_mapping(statistics.get("cells")))
    tracker_cells = set(_mapping(_mapping(statistics.get("tracker")).get("cells")))
    return [] if cells == tracker_cells == set(CELL_ROLES) else [f"{SUBSTANTIVE_PREFIX}cell_keys"]


def gate_block(
    statistics: Mapping[str, Any],
    *,
    by_flagged: bool,
    excess_power: Mapping[str, Any],
    significance_lag: int,
    inheritance: Mapping[str, Any],
    pins: Mapping[str, str],
) -> tuple[dict[str, Any], list[str]]:
    """The block, evaluated on the inherited discovery outcome, and its substantive codes.

    Gating (a refused cell, a non-finite input, a missing key or zero entered positions is
    not met): 1. BY flag; 2. active mean > 0 in every base cell; 3. ``excess_ann`` ≥ the bar
    in every base cell. Reported only: every stress cell's ``excess_ann`` and ``excess_power``.
    """
    codes = _key_codes(statistics)
    cells = _mapping(statistics.get("cells"))
    excess = cell_excess(statistics)
    active = {
        key: None if "refused" in _mapping(cells.get(key)) else _finite(_mapping(cells.get(key)).get("mean"))
        for key in BASE_CELLS
    }
    base_excess = {key: excess.get(key) for key in BASE_CELLS}
    met = {
        "by_flag": by_flagged,
        "arm_beats_control": all(value is not None and value > 0.0 for value in active.values()),
        "excess_at_bar": all(value is not None and value >= EXCESS_BAR for value in base_excess.values()),
    }
    codes += [f"{SUBSTANTIVE_PREFIX}condition_not_met:{name}" for name, ok in met.items() if not ok]
    tracker = _mapping(statistics.get("tracker"))
    block = {
        "version": HUNT_GATE_VERSION,
        "bar": EXCESS_BAR,
        "t_threshold": T_THRESHOLD,
        "cells": dict(CELL_ROLES),
        "conditions": {
            "by_flag": {"met": met["by_flag"]},
            "arm_beats_control": {"met": met["arm_beats_control"], "active_mean": active},
            "excess_at_bar": {"met": met["excess_at_bar"], "excess_ann": base_excess},
        },
        "reported": {
            "stress_excess_ann": {key: excess.get(key) for key in STRESS_CELLS},
            "excess_power": dict(excess_power),
        },
        "significance": {
            "cell": CANONICAL_CELL,
            "series": "tracker.canonical_excess_series",
            "estimator": "hunt_inference.hac_estimate (Newey-West, Bartlett)",
            "lag_rule": "hunt_inference.hunt_lag(validation grid sessions, h)",
            "lag": significance_lag,
        },
        "trackers": {
            "discovery": {key: tracker[key] for key in _TRACKER_IDENTITY_KEYS if key in tracker},
            "validation": {"source": TRACKER_SOURCE},
        },
        "disclosure": DISCLOSURE,
        "inheritance": dict(inheritance),
        "decision_pins": dict(pins),
    }
    return block, codes


def freeze_codes(recomputed: Mapping[str, Any], frozen: Any) -> list[str]:
    """The freeze's comparison of the document's block with its recomputation, per key."""
    if not isinstance(frozen, Mapping):
        # A document built without the block: rebuild it (procedural, nothing was looked at).
        return ["gate_missing"]
    codes: list[str] = []
    for key in sorted(set(recomputed) | set(frozen)):
        if recomputed.get(key) == frozen.get(key):
            continue
        codes.append("gate_decision_pins_stale" if key == "decision_pins" else f"{SUBSTANTIVE_PREFIX}stale:{key}")
    return codes


def look_codes(doc: Mapping[str, Any], *, harness_model_id: str, repo_root: Path = REPO_ROOT) -> list[str]:
    """From the freeze through the readout: the frozen block must be present and well-formed,
    its decision pins must match the code, and its harness model must be the running one."""
    block = doc.get("hunt_gate")
    if not isinstance(block, Mapping):
        return [f"{SUBSTANTIVE_PREFIX}missing"]
    if set(block) != BLOCK_KEYS or block.get("version") != HUNT_GATE_VERSION:
        return [f"{SUBSTANTIVE_PREFIX}malformed"]
    frozen_pins = _mapping(block.get("decision_pins"))
    current = decision_pins(repo_root)
    codes = [
        f"{SUBSTANTIVE_PREFIX}pin_stale:{path}"
        for path in sorted(set(frozen_pins) | set(current))
        if frozen_pins.get(path) != current.get(path)
    ]
    if _mapping(block.get("inheritance")).get("harness_model_id") != harness_model_id:
        codes.append(f"{SUBSTANTIVE_PREFIX}model_stale")
    return codes


def promotion(
    block: Mapping[str, Any], *, verdict: str | None, reasons: Sequence[str], statistics: Mapping[str, Any]
) -> dict[str, Any]:
    """The one look's promotion rule, read against the FROZEN (decoded) block.

    All of: a ``PASS``, or a ``PASS_CONTINGENT`` whose failures are all stress failures;
    ``excess_ann`` ≥ the bar in every base cell; the canonical excess series' NW t above the
    threshold. A refused, missing or non-finite input is not met, and makes the closure
    ``undetermined`` rather than ``no demonstrated edge``. M_val,c = ``excess_ann``_c − bar.
    """
    bar, threshold = float(block["bar"]), float(block["t_threshold"])
    lag = int(block["significance"]["lag"])
    unavailable: list[str] = []
    failed: list[str] = []

    if verdict is None or verdict == Verdict.NOT_PASS_REFUSED:
        unavailable.append(f"verdict: {verdict or 'no outcome'}")
    elif verdict == Verdict.PASS_CONTINGENT and all(reason.startswith("stress ") for reason in reasons):
        pass
    elif verdict != Verdict.PASS:
        failed.append(f"verdict: {verdict}")

    keys = set(_mapping(statistics.get("cells")))
    if keys != set(_mapping(block.get("cells"))) or _key_codes(statistics):
        unavailable.append("cell_keys")
    tracker = _mapping(statistics.get("tracker"))
    if "refused" in tracker:
        unavailable.append(f"tracker: {tracker['refused']}")
    elif tracker.get("source") != _mapping(_mapping(block.get("trackers")).get("validation")).get("source"):
        unavailable.append("tracker: source differs from the frozen one")
    excess = cell_excess(statistics)
    base = {key: excess.get(key) for key, role in sorted(_mapping(block.get("cells")).items()) if role == "gated"}
    for key, value in base.items():
        if value is None:
            unavailable.append(f"excess_ann unavailable: {key}")
        elif not value >= bar:
            failed.append(f"excess_ann {value} < {bar}: {key}")
    margins = {key: None if value is None else value - bar for key, value in base.items()}

    series = tracker.get("canonical_excess_series")
    estimate: Any = (
        hunt_inference.hac_estimate([float(v) for v in series], lag)
        if isinstance(series, Sequence) and series
        else StatRefused("short_sample", "no canonical excess series")
    )
    if isinstance(estimate, StatRefused):
        unavailable.append(f"excess t: {estimate.reason}")
        excess_t: dict[str, Any] = {"refused": estimate.reason}
    else:
        excess_t = {
            "t_stat": estimate.t_stat,
            "standard_error_ann": estimate.standard_error * 252,
            "mean_ann": estimate.mean * 252,
            "observations": estimate.observations,
            "lag": estimate.lag,
        }
        if not estimate.t_stat > threshold:
            failed.append(f"excess t {estimate.t_stat} <= {threshold}")

    promote = not unavailable and not failed
    closure = None if promote else ("undetermined" if unavailable else "no demonstrated edge")
    finite_margins = [value for value in margins.values() if value is not None]
    return {
        "promote": promote,
        "closure": closure,
        "verdict": verdict,
        "verdict_reasons": list(reasons),
        "unavailable": unavailable,
        "failed": failed,
        "base_excess_ann": base,
        "stress_excess_ann": {key: excess.get(key) for key in STRESS_CELLS},
        "excess_t": excess_t,
        "m_val_by_cell": margins,
        "m_val": min(finite_margins) if len(finite_margins) == len(margins) and margins else None,
    }


__all__ = [
    "BASE_CELLS",
    "BLOCK_KEYS",
    "CELL_ROLES",
    "CODE_ONLY_PIN",
    "DISCLOSURE",
    "EXCESS_BAR",
    "GATED_HUNTS",
    "HUNT_GATE_VERSION",
    "PINNED_FILES",
    "STRESS_CELLS",
    "SUBSTANTIVE_PREFIX",
    "T_THRESHOLD",
    "cell_excess",
    "code_only_sha256",
    "decision_pins",
    "freeze_codes",
    "gate_block",
    "look_codes",
    "promotion",
    "tracker_utilisation",
]
