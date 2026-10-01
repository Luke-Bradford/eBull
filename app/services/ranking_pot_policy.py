"""Ranking-pot-v1 policy hash and frozen trial terms (#2842 slice 4a; spec §8, §9.3).

``RANKING_POT_POLICY_HASH`` is the §8 drift check: a rebalance whose process hash differs from the declared one
refuses ``ranking_drift``. Same construction as ``ai_trial_policy`` (whose parameterised ``policy_manifest`` this
reuses, unedited): sha256 of each module FILE's bytes plus the ``repr`` of constants defined elsewhere.

Hashed by bytes (§8): the scorer ``scoring.py`` and its direct in-repo imports (a test pins this list to the file's
own import statements, so a new import cannot slip out of the hash), ``market_calendar``, ``indicator_series``,
``ai_trial_pack`` (``is_eligible``, ``build_bar_series``), and the pot's own modules. Each later slice appends its
module (rebalance, loader, executor wrapper, exits, readout) here; the freeze happens after the last slice lands.

Residual, stated (v1's, spec §8 / r3-90..92): transitive imports beyond the scorer's direct ones, package versions,
runtime configuration and database functions are not hashed; shared non-pot modules (``strategy_paper_executor``,
the position manager, the broker provider) are deliberately not hashed, so a fix there mints no version.

The §8/§9.3 terms that have no other home yet are defined HERE, so they are frozen by this file's bytes.
"""

from __future__ import annotations

from fractions import Fraction
from pathlib import Path
from typing import Final

from app.services.ai_trial_policy import PolicyManifest, policy_manifest
from app.services.scoring import _DEFAULT_MODEL_VERSION

_SERVICES: Final = Path(__file__).resolve().parent

#: The scorer's direct in-repo imports (§8), function-level ones included: ``risk_metrics`` is imported inside
#: the scorer for ``RISK_METRICS_VERSION`` and the spec's list omitted it. ``test_ranking_pot_declaration`` pins
#: this to ``scoring.py``'s import statements.
SCORER_IMPORTS: Final = (
    "instrument_analytics.py",
    "risk_metrics.py",
    "sector_classification.py",
    "thesis_subject_identity.py",
    "xbrl_derived_stats.py",
)

#: Sorted; the digest is over ``name\\0sha256(bytes)\\n`` lines, so a rename is a change too.
POLICY_MODULES: Final = tuple(
    sorted(
        (
            "scoring.py",
            *SCORER_IMPORTS,
            "market_calendar.py",
            "indicator_series.py",
            "ai_trial_pack.py",
            "ranking_pot.py",
            "ranking_pot_sim.py",
            "ranking_pot_policy.py",
        )
    )
)

#: Hashed by value: the scores run the pot produces and consumes (§4 step 1).
FROZEN_CONSTANTS: Final[dict[str, object]] = {
    "scoring._DEFAULT_MODEL_VERSION": _DEFAULT_MODEL_VERSION,
}

# ---------------------------------------------------------------------------
# §8 family spending and §9.3 looks (frozen by this file's bytes)
# ---------------------------------------------------------------------------
STRATEGY_VERSION: Final = "v1"
FAMILY: Final = "ranking-pot"
#: §8: the family's total efficacy budget; the m-th declaration spends ``FAMILY_ALPHA * 2^-m`` over its looks.
FAMILY_ALPHA: Final = Fraction(1, 20)
#: §9.3 look endpoints, months after the first rebalance's target session.
LOOK_MONTHS: Final = (12, 24)
#: §9.3 harm: ``p_down`` at or below this at a look winds the trial down (a stopping rule, not an efficacy claim).
HARM_ALPHA: Final = Fraction(1, 40)


def declaration_alpha(family_seq: int) -> Fraction:
    """§8: the total efficacy alpha the ``family_seq``-th declaration of the family may spend."""
    if family_seq < 1:
        raise ValueError(f"family_seq starts at 1, not {family_seq}")
    return FAMILY_ALPHA / 2**family_seq


def per_look_alpha(family_seq: int) -> Fraction:
    """§9.3 condition 1: the declaration's alpha split equally over its looks (v1, m = 1: 0.0125)."""
    return declaration_alpha(family_seq) / len(LOOK_MONTHS)


def policy_manifest_now(root: Path = _SERVICES) -> PolicyManifest:
    """Read the hashed modules from disk NOW (the freeze compares this with the import-time hash)."""
    return policy_manifest(root, POLICY_MODULES, FROZEN_CONSTANTS)


RANKING_POT_POLICY_HASH: Final = policy_manifest_now().digest()


__all__ = [
    "FAMILY",
    "FAMILY_ALPHA",
    "FROZEN_CONSTANTS",
    "HARM_ALPHA",
    "LOOK_MONTHS",
    "POLICY_MODULES",
    "RANKING_POT_POLICY_HASH",
    "SCORER_IMPORTS",
    "STRATEGY_VERSION",
    "declaration_alpha",
    "per_look_alpha",
    "policy_manifest_now",
]
