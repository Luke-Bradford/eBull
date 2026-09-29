"""``AI_TRIAL_POLICY_HASH`` — the #3471 drift check (spec §9 "Runtime checks", obligation O6).

The declaration freezes every §3–§8 constant. O6: the hash covers the constants AND the source
bytes of the trial modules, and a run whose recomputed hash differs from the declared one is
refused ``policy_drift``. Hashing the files covers both at once, because every frozen constant is
defined in one of them:

* ``ai_trial_pack`` / ``ai_trial_pack_reader`` — shortlist, pack, indicators, knowledge time;
* ``ai_trial_decision`` — the §5 schema, the §6 validator and the §7 draw;
* ``ai_trial_levels`` — the v6 §16.1 structure levels and setup detectors;
* ``ai_trial_prompt`` / ``ai_trial_invocation`` — the frozen prompts, the model id, the argv,
  the env and the §4 limits;
* ``ai_trial_intent`` — the loader and the §8 tickets;
* ``ai_trial_start_gate`` — the step-0 preview terms.

⚠ Execution plumbing is deliberately NOT hashed by its bytes (``ai_trial_executor``,
``ai_trial_deadline``, ``ai_trial_protection``, ``ai_trial_pair_lifecycle``, the position manager,
the run publisher): a fix there would otherwise mint a new strategy version. The frozen terms
those modules DO define are hashed by VALUE instead (``FROZEN_CONSTANTS``, Codex ckpt-2 on the
publisher), as is the scorer's default model version, which picks the §3.1 scores run.

Not hashed: the paper path's shared safety gates the loader and the preview reuse by design (the
halt SQL, the mandate SQL, the sandbox codes) — §8 keeps them identical to the paper path, not
frozen for the trial — and the house functions the spec freezes by NAME (``atr_series``, the
market calendar).

Same idiom as ``market_regime._code_hash`` (read the FILE, never ``inspect.getsource``), but the
whole sha256: a declaration pins it, so a truncation would only weaken the pin.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from app.services.ai_trial_deadline import (
    TRIAL_ENTRY_TIME_UTC,
    TRIAL_EXIT_TIME_UTC,
    TRIAL_MAX_POSITION_AGE_SECONDS,
)
from app.services.ai_trial_executor import TRIAL_COST_CAP_PCT, TRIAL_MAX_CONCURRENT_PER_LEG
from app.services.ai_trial_halts import TRIAL_LOSS_HALT_PCT
from app.services.ai_trial_pair_lifecycle import (
    LABEL_CLASSIFIER_VERSION,
    TRIAL_CENSOR_SESSIONS,
    TRIAL_UNRESOLVED_SESSIONS,
)
from app.services.ai_trial_readout import (
    COHORT_SESSIONS,
    FLOW_WINDOW_SESSIONS,
    HARM_ALPHA,
    HARM_LOOK_EVERY,
    HARM_MIN_CLUSTERS,
    MECHANICAL_EXITS,
    MIN_CLUSTERS,
    MIN_UNITS,
    READOUT_WAIT_SESSIONS,
)
from app.services.ai_trial_stats import EXACT_MAX_CLUSTERS, MONTE_CARLO_FLIPS
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.strategy_position_manager import TRIAL_REPAIR_GRACE

_SERVICES: Final = Path(__file__).resolve().parent

#: Sorted; the digest is over ``name\\0sha256(bytes)\\n`` lines, so a rename is a change too.
POLICY_MODULES: Final = (
    "ai_trial_decision.py",
    "ai_trial_intent.py",
    "ai_trial_invocation.py",
    "ai_trial_levels.py",
    "ai_trial_pack.py",
    "ai_trial_pack_reader.py",
    "ai_trial_plan.py",
    "ai_trial_prompt.py",
    "ai_trial_start_gate.py",
)


#: §3–§8 / O10 / O11 terms and the §9 stopping rules defined outside ``POLICY_MODULES``, hashed by
#: ``repr`` (a set as a sorted tuple: a frozenset's repr order follows the per-process string hash).
FROZEN_CONSTANTS: Final[dict[str, object]] = {
    "ai_trial_deadline.TRIAL_ENTRY_TIME_UTC": TRIAL_ENTRY_TIME_UTC,
    "ai_trial_deadline.TRIAL_EXIT_TIME_UTC": TRIAL_EXIT_TIME_UTC,
    "ai_trial_deadline.TRIAL_MAX_POSITION_AGE_SECONDS": TRIAL_MAX_POSITION_AGE_SECONDS,
    "ai_trial_executor.TRIAL_COST_CAP_PCT": TRIAL_COST_CAP_PCT,
    "ai_trial_executor.TRIAL_MAX_CONCURRENT_PER_LEG": TRIAL_MAX_CONCURRENT_PER_LEG,
    "ai_trial_halts.TRIAL_LOSS_HALT_PCT": TRIAL_LOSS_HALT_PCT,
    "ai_trial_pair_lifecycle.LABEL_CLASSIFIER_VERSION": LABEL_CLASSIFIER_VERSION,
    "ai_trial_pair_lifecycle.TRIAL_CENSOR_SESSIONS": TRIAL_CENSOR_SESSIONS,
    "ai_trial_pair_lifecycle.TRIAL_UNRESOLVED_SESSIONS": TRIAL_UNRESOLVED_SESSIONS,
    "ai_trial_readout.COHORT_SESSIONS": COHORT_SESSIONS,
    "ai_trial_readout.FLOW_WINDOW_SESSIONS": FLOW_WINDOW_SESSIONS,
    "ai_trial_readout.HARM_ALPHA": HARM_ALPHA,
    "ai_trial_readout.HARM_LOOK_EVERY": HARM_LOOK_EVERY,
    "ai_trial_readout.HARM_MIN_CLUSTERS": HARM_MIN_CLUSTERS,
    "ai_trial_readout.MECHANICAL_EXITS": tuple(sorted(MECHANICAL_EXITS)),
    "ai_trial_readout.MIN_CLUSTERS": MIN_CLUSTERS,
    "ai_trial_readout.MIN_UNITS": MIN_UNITS,
    "ai_trial_readout.READOUT_WAIT_SESSIONS": READOUT_WAIT_SESSIONS,
    "ai_trial_stats.EXACT_MAX_CLUSTERS": EXACT_MAX_CLUSTERS,
    "ai_trial_stats.MONTE_CARLO_FLIPS": MONTE_CARLO_FLIPS,
    "scoring._DEFAULT_MODEL_VERSION": _DEFAULT_MODEL_VERSION,
    "strategy_position_manager.TRIAL_REPAIR_GRACE": TRIAL_REPAIR_GRACE,
}


@dataclass(frozen=True)
class PolicyManifest:
    """What ``policy_hash`` hashes, read once: each module's sha256 and each constant's ``repr``.
    The #3471 declaration stores both, so the document and its hash describe one snapshot."""

    module_sha256: dict[str, str]
    constant_repr: dict[str, str]

    def digest(self) -> str:
        digest = hashlib.sha256()
        for name in sorted(self.module_sha256):
            digest.update(f"{name}\0{self.module_sha256[name]}\n".encode())
        for name in sorted(self.constant_repr):
            digest.update(f"{name}\0{self.constant_repr[name]}\n".encode())
        return digest.hexdigest()


def policy_manifest(
    root: Path = _SERVICES,
    modules: tuple[str, ...] = POLICY_MODULES,
    constants: dict[str, object] = FROZEN_CONSTANTS,
) -> PolicyManifest:
    return PolicyManifest(
        {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in modules},
        {name: repr(value) for name, value in constants.items()},
    )


def policy_hash(
    root: Path = _SERVICES,
    modules: tuple[str, ...] = POLICY_MODULES,
    constants: dict[str, object] = FROZEN_CONSTANTS,
) -> str:
    return policy_manifest(root, modules, constants).digest()


AI_TRIAL_POLICY_HASH: Final = policy_hash()
