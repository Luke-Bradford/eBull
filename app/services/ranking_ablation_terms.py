"""#1822 route F: the terms sidecar, the declaration it pins, and its register entry.

Spec ``docs/proposals/ta/2026-09-28-1822-ranking-ablation.md`` (v8), "Declaration (v8)".
``sql/333``'s row has fixed columns, so route F's construction lives in a **terms sidecar**
the row pins by digest:

- **semantic terms**, regenerated from the code on every run (:func:`semantic_terms`);
- **frozen facts**, computed once from the dev DB when the sidecar is generated and never
  recomputed: the forward-shadow floor, its derivation, and the grid facts it comes from.

A sidecar is canonical JSON stored per digest at ``SIDECAR_DIR/terms-<sha256>.json`` and never
overwritten. Generation refuses terms that an existing sidecar already declares, so the
regenerated semantic terms always select at most one sidecar; ``--freeze`` and ``--readout``
refuse unless they select exactly one. Changed terms or facts give a new sidecar, identity,
root declaration and register entry; the earlier ones stay.

Pure apart from reading and writing sidecar files. Nothing here reads a price or a return.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Final

from app.services import market_calendar
from app.services import ranking_ablation as ra
from app.services import ranking_ablation_reader as reader
from app.services.cost_model import COST_MODEL_ID
from app.services.hunt_harness import HUNT_HARNESS_MODEL_ID, HUNT_TARIFF
from app.services.market_regime import REGIME_RULE_VERSION
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.r6_exclusion_trial import PROGRAMME_POLICIES, ZERO_RECOVERY
from app.services.series_termination import TERMINATION_RULE_VERSION
from app.services.strategies.validated_universe import STOCKS_TYPE_DESCRIPTION
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister, declaration_backed_evidence

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
#: Repo-relative, because the register entry's evidence names it.
SIDECAR_RELATIVE_DIR: Final = "docs/proposals/ta/1822-route-f"
SIDECAR_DIR: Final = REPO_ROOT / SIDECAR_RELATIVE_DIR
_SIDECAR_NAME: Final = re.compile(r"terms-(?P<sha>[0-9a-f]{64})\.json")

STRATEGY_ID: Final = "ranking-ablation-1822-route-f"
DECLARED_BY: Final = "scripts/run_1822_ablation_readout.py --freeze (#1822)"
#: The one of ``PreregPurpose``'s two values that cannot promote.
PREREG_PURPOSE: Final = "falsification_only"
#: Conservative (#2288: "an unlabelled result is treated as survivor_only"): the lane filter and
#: the Q-suffix symbol are read at readout time, so survivorship-free is not claimed.
UNIVERSE_BASIS: Final = "survivor_only"
#: Long x1 US equities in a USD lane, as #2901 declares for the same lane: no overnight
#: financing, no conversion event. A bespoke contract owns its stamps.
CARRY_UNMODELLED: Final = False
FX_UNMODELLED: Final = False

# --- The evaluation inventory: every book, evaluation and population is charged. ---
BOOKS: Final = ("arm_full", *(f"arm_-{family}" for family in ra.FAMILY_ORDER), "control")
#: Four cells, then the two non-canonical termination policies re-evaluating the canonical cell.
CELLS: Final = ("canonical", "t3_excluded", "as_traded", "verbatim_bar_rule")
CANONICAL_POLICY: Final = ZERO_RECOVERY.label
EVALUATIONS: Final = (
    *CELLS,
    *(f"canonical@{policy.label}" for policy in PROGRAMME_POLICIES if policy.label != CANONICAL_POLICY),
)
POPULATIONS: Final = ("pooled", "prospective")

#: ⚠ sql/333 caps the derivation column at 1000 characters.
_DERIVATION_LIMIT: Final = 1000


def canonical_bytes(payload: Any) -> bytes:
    """The sidecar encoding: sorted keys, compact separators, ASCII, one trailing newline."""
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("ascii")


def _normalised(payload: Any) -> Any:
    """``payload`` as it reads back from a sidecar, so a regenerated value compares equal."""
    return json.loads(canonical_bytes(payload))


def semantic_terms() -> dict[str, Any]:
    """Route F's construction, regenerated from the code. Every change here is a new declaration."""
    inventory = {"books": list(BOOKS), "evaluations": list(EVALUATIONS), "populations": list(POPULATIONS)}
    return _normalised(
        {
            "construction_revision": ra.CONSTRUCTION_REVISION,
            "model_version": ra.MODEL_VERSION,
            "h": ra.H,
            "fraction": ra.FRACTION,
            "lag": ra.LAG,
            "annualisation": ra.ANNUALISATION,
            "tie_rule": "hunt_evaluator.select_arm: every tie at the cut is included",
            "family_order": list(ra.FAMILY_ORDER),
            "weights": dict(ra.weights()),
            "lane": {"instrument_type": STOCKS_TYPE_DESCRIPTION, "currency": reader.LANE_CURRENCY},
            "witness": {"job_name": reader.WITNESS_JOB, "status": reader.WITNESS_STATUS},
            "cutoff": {"provisional_window_days": PROVISIONAL_WINDOW_DAYS, "rule": "latest session strictly before"},
            "cells": list(CELLS),
            "cost_basis": {"canonical": "split_adjusted", "as_traded": "as_traded"},
            "termination_policies": [policy.identity() for policy in PROGRAMME_POLICIES],
            "canonical_termination_policy": CANONICAL_POLICY,
            "nw_lag": {"canonical": "hunt_lag(T, h)", "sensitivity": "2 * hunt_lag(T, h)"},
            "rule_identities": {
                "cost_model": COST_MODEL_ID,
                "hunt_harness": HUNT_HARNESS_MODEL_ID,
                "hunt_tariff": None if HUNT_TARIFF is None else HUNT_TARIFF.cost_model_id(),
                "structural_refusal_policy": STRUCTURAL_REFUSAL_POLICY_VERSION,
                "price_quarantine": QUARANTINE_RULE_SET_VERSION,
                "market_calendar": market_calendar.RULE_SET_VERSION,
                "series_termination": TERMINATION_RULE_VERSION,
                "market_regime": REGIME_RULE_VERSION,
            },
            "sql": dict(reader.ROUTE_F_SQL),
            "inventory": inventory,
            "searches": searches(inventory),
        }
    )


def searches(inventory: Mapping[str, Sequence[Any]]) -> int:
    """The register charge: the product of the inventory lists' lengths."""
    return math.prod(len(values) for values in inventory.values())


# ---------------------------------------------------------------------------
# Frozen facts: the forward-shadow floor, by construction
# ---------------------------------------------------------------------------


class FloorNotPositive(ValueError):
    """``floor_not_positive``: the evidence supply at the fact date is empty; nothing is frozen."""


def frozen_facts(*, fact_date: date, entry_sessions: Iterable[date], first_entry: date, g_k: date) -> dict[str, Any]:
    """The floor, its derivation and the grid facts it comes from (spec "Row", "Floor").

    ``entry_sessions`` are the mapped witnessed entry sessions; only those in
    [``first_entry``, ``g_k``] count. No price is read to produce any of this.
    """
    on_grid = sorted({day for day in entry_sessions if first_entry <= day <= g_k})
    dates = len(on_grid)
    weeks = math.ceil((g_k - first_entry).days / 7)
    if dates < 1 or weeks < 1:
        raise FloorNotPositive(f"floor_not_positive: dates={dates}, weeks={weeks}")
    derivation = (
        "NOT a power calculation: route F is a descriptive readout and none exists. Fixed BY CONSTRUCTION "
        "(the #2901/#2840 precedent: the floor equals the evidence supply) from the calendar and job_runs at "
        f"the fact date {fact_date}, no price read: dates = distinct entry sessions on the grid "
        f"[{first_entry}, G_k {g_k}] with a mapped witnessed {ra.MODEL_VERSION} run = {dates}; weeks = "
        f"ceil(({g_k} - {first_entry}) days / 7) = {weeks}. It is the evidence supply at the fact date, not "
        "readout 1's grid, which may hold more runs. It gates nothing: the declaration is falsification_only."
    )
    if len(derivation) > _DERIVATION_LIMIT:
        raise ValueError(f"derivation is {len(derivation)} characters; sql/333 caps it at {_DERIVATION_LIMIT}")
    return _normalised(
        {
            "fact_date": fact_date.isoformat(),
            "first_entry_session": first_entry.isoformat(),
            "g_k": g_k.isoformat(),
            "entry_sessions_on_grid": [day.isoformat() for day in on_grid],
            "min_independent_decision_dates": dates,
            "min_calendar_weeks": weeks,
            "derivation": derivation,
        }
    )


# ---------------------------------------------------------------------------
# Sidecars
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Sidecar:
    #: Repo-relative when under :data:`REPO_ROOT`; the register evidence names this.
    path: str
    sha256: str
    semantic_terms: Mapping[str, Any]
    frozen_facts: Mapping[str, Any]


class SidecarRefused(ValueError):
    """A sidecar that is malformed, or whose generation would break the one-to-one selection."""


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def load_sidecars(directory: Path = SIDECAR_DIR) -> tuple[Sidecar, ...]:
    """Every sidecar in ``directory``, integrity-checked: name digest = content digest, canonical bytes."""
    if not directory.is_dir():
        return ()
    loaded: list[Sidecar] = []
    for path in sorted(directory.iterdir()):
        match = _SIDECAR_NAME.fullmatch(path.name)
        if match is None:
            continue
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        if sha != match["sha"]:
            raise SidecarRefused(f"{path.name}: content sha256 {sha} does not match its name")
        payload = json.loads(raw)
        if canonical_bytes(payload) != raw:
            raise SidecarRefused(f"{path.name}: not canonical JSON")
        if set(payload) != {"semantic_terms", "frozen_facts"}:
            raise SidecarRefused(f"{path.name}: sections {sorted(payload)}")
        loaded.append(Sidecar(_display_path(path), sha, payload["semantic_terms"], payload["frozen_facts"]))
    return tuple(loaded)


def select_sidecar(terms: Mapping[str, Any], sidecars: Iterable[Sidecar]) -> Sidecar | str:
    """The one sidecar declaring ``terms``, or ``no_sidecar_for_terms`` / ``ambiguous_sidecars``."""
    matches = [sidecar for sidecar in sidecars if sidecar.semantic_terms == terms]
    if not matches:
        return "no_sidecar_for_terms"
    if len(matches) > 1:
        return "ambiguous_sidecars"
    return matches[0]


def write_sidecar(terms: Mapping[str, Any], facts: Mapping[str, Any], directory: Path = SIDECAR_DIR) -> Sidecar:
    """Write a new sidecar; refuse terms an existing sidecar already declares."""
    if any(existing.semantic_terms == terms for existing in load_sidecars(directory)):
        raise SidecarRefused("terms_already_declared: a sidecar with these semantic terms exists")
    raw = canonical_bytes({"semantic_terms": terms, "frozen_facts": facts})
    sha = hashlib.sha256(raw).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"terms-{sha}.json"
    with path.open("xb") as handle:  # never overwritten
        handle.write(raw)
    return Sidecar(_display_path(path), sha, _normalised(terms), _normalised(facts))


# ---------------------------------------------------------------------------
# Identity, declaration and register entry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Identity:
    strategy_id: str
    strategy_version: str
    contract_version: str
    trial_id: str


def identity(sha256: str) -> Identity:
    """The full-digest identity (spec "Identity")."""
    return Identity(
        strategy_id=STRATEGY_ID,
        strategy_version=f"{ra.MODEL_VERSION}+{sha256}",
        contract_version=f"1822-route-f-terms-{sha256}",
        trial_id=f"{STRATEGY_ID}-{sha256}",
    )


def build_declaration(sidecar: Sidecar) -> PreregDeclaration:
    """The declaration the sidecar pins. Stamps from this module; the floor from the frozen facts."""
    ident = identity(sidecar.sha256)
    facts = sidecar.frozen_facts
    return PreregDeclaration(
        strategy_id=ident.strategy_id,
        strategy_version=ident.strategy_version,
        contract_version=ident.contract_version,
        prereg_purpose=PREREG_PURPOSE,
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=UNIVERSE_BASIS,
        declared_carry_unmodelled=CARRY_UNMODELLED,
        declared_fx_unmodelled=FX_UNMODELLED,
        expected_structural_refusals=structural_promotion_refusals(
            universe_basis=UNIVERSE_BASIS, carry_unmodelled=CARRY_UNMODELLED, fx_unmodelled=FX_UNMODELLED
        ),
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=int(facts["min_independent_decision_dates"]),
            min_calendar_weeks=int(facts["min_calendar_weeks"]),
            derivation=str(facts["derivation"]),
        ),
        declared_by=DECLARED_BY,
    )


def expected_register_entry(sidecar: Sidecar) -> DeclaredTrial:
    """The one ``DeclaredTrial`` a sidecar must have (spec "Trial register")."""
    ident = identity(sidecar.sha256)
    charged = searches(sidecar.semantic_terms["inventory"])
    return DeclaredTrial(
        trial_id=ident.trial_id,
        description=(
            f"#1822 route F: the v1.5 family ablation over stored scores under terms sidecar {sidecar.sha256[:12]}"
            f"…, {charged} evaluations (books x evaluations x populations)."
        ),
        evidence=declaration_backed_evidence(
            declaration_path=sidecar.path, declaration_sha256=sidecar.sha256, pinned_specs=charged
        ),
        exactness=TrialExactness.EXACT,
        searches=charged,
        declared_for=(ident.strategy_id, ident.strategy_version),
    )


def register_refusals(sidecars: Sequence[Sidecar], register: TrialRegister) -> tuple[str, ...]:
    """Every sidecar has exactly one matching entry, and every route F entry has its sidecar."""
    refusals: list[str] = []
    by_id: dict[str, list[DeclaredTrial]] = {}
    for trial in register.trials:
        if trial.trial_id.startswith(f"{STRATEGY_ID}-"):
            by_id.setdefault(trial.trial_id, []).append(trial)
    named = set()
    for sidecar in sidecars:
        expected = expected_register_entry(sidecar)
        named.add(expected.trial_id)
        entries = by_id.get(expected.trial_id, [])
        if len(entries) != 1:
            refusals.append(f"{sidecar.path}: {len(entries)} register entries")
            continue
        (entry,) = entries
        for field in ("declared_for", "searches", "exactness", "evidence"):
            if getattr(entry, field) != getattr(expected, field):
                refusals.append(f"{sidecar.path}: register entry {field} differs")
    refusals.extend(f"{trial_id}: no sidecar" for trial_id in sorted(set(by_id) - named))
    return tuple(refusals)


__all__ = [
    "BOOKS",
    "CANONICAL_POLICY",
    "CELLS",
    "EVALUATIONS",
    "POPULATIONS",
    "SIDECAR_DIR",
    "SIDECAR_RELATIVE_DIR",
    "STRATEGY_ID",
    "FloorNotPositive",
    "Identity",
    "Sidecar",
    "SidecarRefused",
    "build_declaration",
    "canonical_bytes",
    "expected_register_entry",
    "frozen_facts",
    "identity",
    "load_sidecars",
    "register_refusals",
    "searches",
    "select_sidecar",
    "semantic_terms",
    "write_sidecar",
]
