"""Ranking-pot-v2 policy hash and frozen trial terms (#3592 slice 3b; spec ``2026-10-03-3592-ranking-pot-v2.md`` §8).

``RANKING_POT_V2_POLICY_HASH`` is v2's drift check, built as v1's (``ai_trial_policy.policy_manifest``, reused
unedited): sha256 of each listed module FILE's bytes plus the ``repr`` of constants defined elsewhere.

The list is §8 v5's exact union, by construction: (i) the scorer and ``SCORER_IMPORTS``; (ii) the roots, every
``ranking_pot_v2*.py``; (iii) C, the roots' closure over imports INTO ``ranking_pot*`` modules only; (iv) the other
``app.services`` modules a module in C imports directly (one hop); (v) every ``ranking_pot*`` module in the roots'
full transitive closure. ``tests/test_ranking_pot_v2_policy.py`` recomputes it by static walk and pins it equal, and
asserts no executed-book module is in C. Each later slice's modules join through that test.

Residual, stated in §8: modules outside the union, package versions, runtime configuration and database functions are
not hashed. This module deliberately does not import v1's ``ranking_pot_policy``; the v1 constants v2 reuses are copied
below and pinned equal to v1's by test, so they are frozen by THIS file's bytes.
"""

from __future__ import annotations

from fractions import Fraction
from pathlib import Path
from typing import Final

from app.services.ai_trial_policy import PolicyManifest, policy_manifest
from app.services.ranking_pot_sim import K_CONTROLS
from app.services.scoring import _DEFAULT_MODEL_VERSION

_SERVICES: Final = Path(__file__).resolve().parent

#: ``scoring.py``'s direct in-repo imports; pinned to its import statements by test (v1's ``SCORER_IMPORTS``).
SCORER_IMPORTS: Final = (
    "instrument_analytics.py",
    "risk_metrics.py",
    "sector_classification.py",
    "thesis_subject_identity.py",
    "xbrl_derived_stats.py",
)

#: Sorted; the digest is over ``name\\0sha256(bytes)\\n`` lines, so a rename is a change too.
POLICY_MODULES: Final = (
    "ai_trial_pack.py",
    "ai_trial_policy.py",
    "indicator_series.py",
    "instrument_analytics.py",
    "market_calendar.py",
    "ranking_pot.py",
    "ranking_pot_exit_rule.py",
    "ranking_pot_held_levels.py",
    "ranking_pot_sim.py",
    "ranking_pot_v2.py",
    "ranking_pot_v2_inputs.py",
    "ranking_pot_v2_policy.py",
    "risk_metrics.py",
    "scoring.py",
    "sector_classification.py",
    "thesis_subject_identity.py",
    "xbrl_derived_stats.py",
)

#: Hashed by value: the scores run v2 produces and consumes (v1 spec §4 step 1).
FROZEN_CONSTANTS: Final[dict[str, object]] = {
    "scoring._DEFAULT_MODEL_VERSION": _DEFAULT_MODEL_VERSION,
}

# ---------------------------------------------------------------------------
# §8 terms (frozen by this file's bytes)
# ---------------------------------------------------------------------------
STRATEGY_VERSION: Final = "v1"
#: Set True by slice 4, the last slice. While False the v2 freeze ``--apply`` refuses ``build_incomplete`` (v1's
#: reason: a declaration frozen on a partial build could never be measured). v1's flag is not read.
BUILD_COMPLETE: Final = False
#: §4: no executed book; sql/463 refuses any other value for ``ranking-pot-v2`` and enforces it by trigger.
EXECUTION: Final = "none"
#: §4: books 0 (shadow), 1..K (controls), K + 1 (no-SL/TP variant), K + 2 (the v1-reference book); sql/463 requires it.
BOOK_COUNT: Final = K_CONTROLS + 3
#: v1 spec §8 / §9.3, reused unchanged (pinned equal to ``ranking_pot_policy``'s by test).
FAMILY: Final = "ranking-pot"
FAMILY_ALPHA: Final = Fraction(1, 20)
LOOK_MONTHS: Final = (12, 24)
HARM_ALPHA: Final = Fraction(1, 40)


def declaration_alpha(family_seq: int) -> Fraction:
    """v1 spec §8: the total efficacy alpha the ``family_seq``-th declaration of the family may spend."""
    if family_seq < 1:
        raise ValueError(f"family_seq starts at 1, not {family_seq}")
    return FAMILY_ALPHA / 2**family_seq


def per_look_alpha(family_seq: int) -> Fraction:
    """v1 spec §9.3 condition 1: the declaration's alpha split equally over its looks."""
    return declaration_alpha(family_seq) / len(LOOK_MONTHS)


def policy_manifest_now(root: Path = _SERVICES) -> PolicyManifest:
    """Read the hashed modules from disk NOW (the freeze compares this with the import-time hash)."""
    return policy_manifest(root, POLICY_MODULES, FROZEN_CONSTANTS)


RANKING_POT_V2_POLICY_HASH: Final = policy_manifest_now().digest()


__all__ = [
    "BOOK_COUNT",
    "BUILD_COMPLETE",
    "EXECUTION",
    "FAMILY",
    "FAMILY_ALPHA",
    "FROZEN_CONSTANTS",
    "HARM_ALPHA",
    "LOOK_MONTHS",
    "POLICY_MODULES",
    "RANKING_POT_V2_POLICY_HASH",
    "SCORER_IMPORTS",
    "STRATEGY_VERSION",
    "declaration_alpha",
    "per_look_alpha",
    "policy_manifest_now",
]
