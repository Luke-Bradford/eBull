"""Immutable preregistration terms for the MT-1/S-8 controlled trial.

The terms live in application code because both the one-time freeze CLI and
the paved evaluator must independently reconstruct them.  ``DECLARED_BY``
intentionally retains the original script identity so this extraction leaves
the already-frozen declaration digests byte-for-byte unchanged.
"""

from __future__ import annotations

import math
from typing import Final

from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity
from app.services.strategy_mt1_trial import TRIAL_CONTRACT_VERSION
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals

DECLARED_BY: Final = "scripts/freeze_2437_mt1_declarations.py (#2437)"
UNIVERSE_BASIS: Final = "survivorship_free"

_Z_0975: Final = 1.959963984540054
_Z_08: Final = 0.8416212335729143
_STANDARDISED_EFFECT: Final = 0.5
_POWER_DERIVED_MONTHS: Final = math.ceil(((_Z_0975 + _Z_08) / _STANDARDISED_EFFECT) ** 2)
MIN_FORWARD_DECISION_DATES: Final = max(36, _POWER_DERIVED_MONTHS)
MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(MIN_FORWARD_DECISION_DATES * 365.25 / (12 * 7))

_FORWARD_SHADOW_DERIVATION: Final = (
    "Frozen preregistration power floor: standardised paired monthly effect=0.5, alpha=0.05 two-sided, "
    "power=0.8; z_0.975=1.959963984540054 and z_0.8=0.8416212335729143. "
    "n=ceil(((1.959963984540054+0.8416212335729143)/0.5)^2)=32 independent decision months; raised to "
    "36 months to cover three complete calendar years. Calendar duration="
    "ceil(36x365.25/(12x7))=157 weeks. Prospective inference still uses the preregistered paired "
    "moving-block bootstrap; this floor does not assert monthly independence."
)


def _build(*, control: bool) -> PreregDeclaration:
    identity = (
        s8_control_identity(universe=UNIVERSE_BASIS, cost_model_id=COST_MODEL_ID)
        if control
        else mt1_identity(universe=UNIVERSE_BASIS, cost_model_id=COST_MODEL_ID)
    )
    expected = structural_promotion_refusals(
        universe_basis=UNIVERSE_BASIS,
        carry_unmodelled=CARRY_UNMODELLED,
        fx_unmodelled=FX_UNMODELLED,
    )
    return PreregDeclaration(
        strategy_id=identity.strategy_id,
        strategy_version=identity.version,
        contract_version=TRIAL_CONTRACT_VERSION,
        prereg_purpose="falsification_only" if control else "capital_candidate",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=UNIVERSE_BASIS,
        declared_carry_unmodelled=CARRY_UNMODELLED,
        declared_fx_unmodelled=FX_UNMODELLED,
        expected_structural_refusals=expected,
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=MIN_FORWARD_DECISION_DATES,
            min_calendar_weeks=MIN_FORWARD_CALENDAR_WEEKS,
            derivation=_FORWARD_SHADOW_DERIVATION,
        ),
        declared_by=DECLARED_BY,
    )


def build_mt1_declaration() -> PreregDeclaration:
    return _build(control=False)


def build_s8_control_declaration() -> PreregDeclaration:
    return _build(control=True)


def build_declarations() -> tuple[PreregDeclaration, PreregDeclaration]:
    return build_mt1_declaration(), build_s8_control_declaration()


__all__ = [
    "DECLARED_BY",
    "MIN_FORWARD_CALENDAR_WEEKS",
    "MIN_FORWARD_DECISION_DATES",
    "UNIVERSE_BASIS",
    "build_declarations",
    "build_mt1_declaration",
    "build_s8_control_declaration",
]
