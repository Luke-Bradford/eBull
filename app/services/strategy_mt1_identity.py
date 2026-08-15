"""Distinct identities for MT-1 and its S-8 negative control (#2437).

Neither identity is placed in ``STRATEGY_MANIFEST`` yet.  The ordinary runner
would apply ``equal_weight_concurrent_v1`` and persist an unscaled result, which
is not MT-1.  They become runnable only through the dedicated four-arm path
that builds the source S-10/S-8 books, applies the causal overlay, and enforces
the preregistered paired evaluator.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Final

from app.services.equity_curve import CAPPED_TARGET_EXPOSURE_RULE_ID
from app.services.indicator_series import Universe
from app.services.strategies.s8_range_mean_reversion import s8_identity
from app.services.strategies.s10_relative_strength_leader import s10_identity
from app.services.strategy_mt1_books import BOOK_RULE_VERSION
from app.services.strategy_mt1_trial import (
    NEGATIVE_CONTROL_TRIAL_ID,
    TRIAL_CONTRACT_VERSION,
    TRIAL_EVALUATOR_VERSION,
    TRIAL_ID,
)
from app.services.strategy_registry import StrategyIdentity
from app.services.strategy_volatility_overlay import RULE_SET_VERSION as OVERLAY_RULE_SET_VERSION

MT1_STRATEGY_ID: Final = TRIAL_ID
S8_CONTROL_STRATEGY_ID: Final = NEGATIVE_CONTROL_TRIAL_ID


def _source_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def _identity(
    *,
    strategy_id: str,
    source_strategy_version: str,
    decision_clock_strategy_version: str,
    universe: Universe,
    cost_model_id: str,
) -> StrategyIdentity:
    if not cost_model_id.strip():
        raise ValueError("cost_model_id must be a non-empty declaration; pass app.services.cost_model.COST_MODEL_ID")
    return StrategyIdentity(
        strategy_id=strategy_id,
        params={
            "source_strategy_version": source_strategy_version,
            "decision_clock_strategy_version": decision_clock_strategy_version,
            "overlay_rule_set_version": OVERLAY_RULE_SET_VERSION,
            "target_exposure_rule": CAPPED_TARGET_EXPOSURE_RULE_ID,
            "four_arm_book_rule_version": BOOK_RULE_VERSION,
            "trial_contract_version": TRIAL_CONTRACT_VERSION,
            "trial_evaluator_version": TRIAL_EVALUATOR_VERSION,
        },
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def mt1_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The capped S-10 candidate, distinct from every S-10 result."""
    source = s10_identity(universe=universe, cost_model_id=cost_model_id)
    return _identity(
        strategy_id=MT1_STRATEGY_ID,
        source_strategy_version=source.version,
        decision_clock_strategy_version=source.version,
        universe=universe,
        cost_model_id=cost_model_id,
    )


def s8_control_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The capped S-8 falsification control, distinct from every S-8 result."""
    source = s8_identity(universe=universe, cost_model_id=cost_model_id)
    clock = s10_identity(universe=universe, cost_model_id=cost_model_id)
    return _identity(
        strategy_id=S8_CONTROL_STRATEGY_ID,
        source_strategy_version=source.version,
        decision_clock_strategy_version=clock.version,
        universe=universe,
        cost_model_id=cost_model_id,
    )


__all__ = [
    "MT1_STRATEGY_ID",
    "S8_CONTROL_STRATEGY_ID",
    "mt1_identity",
    "s8_control_identity",
]
