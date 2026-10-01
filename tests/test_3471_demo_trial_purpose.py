"""#3471 slice 2a — the ``demo_trial`` purpose at every capital and promotion chokepoint (spec §8).

Pure half of the enumeration test: the checks here all run before any database read, so a
``None`` connection proves they refuse without reaching one. The DB half (deployments and the
standard paper loader) is ``test_3471_demo_trial_purpose_db.py``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, cast

import pytest

from app.services.strategy_control_plane import (
    _ADVANCING_STAGES,
    StrategyControlError,
    promote_strategy,
    registered_strategy_purpose,
)
from app.services.strategy_live_gate import live_gate_refusals
from app.services.strategy_manifest import DEMO_TRIAL_STRATEGY_IDS, STRATEGY_MANIFEST
from tests.test_prereg_declaration_gate import _facts

_NO_DB = cast(Any, None)


def test_trial_legs_resolve_to_demo_trial_and_are_not_manifest_entries() -> None:
    assert DEMO_TRIAL_STRATEGY_IDS == {
        "ai-discretionary-v1",
        "ai-discretionary-v1-control",
        "ai-discretionary-fund-v1",
        "ai-discretionary-fund-v1-control",
        "ranking-pot-v1",
    }
    assert not DEMO_TRIAL_STRATEGY_IDS & STRATEGY_MANIFEST.keys()
    for strategy_id in DEMO_TRIAL_STRATEGY_IDS:
        assert registered_strategy_purpose(strategy_id) == "demo_trial"
    assert registered_strategy_purpose("ai-discretionary-v2") is None


def test_advancing_stages_are_every_stage_but_the_risk_reducing_two() -> None:
    assert _ADVANCING_STAGES == {
        "research_candidate",
        "historical_validated",
        "forward_observation",
        "paper_enabled",
        "live_enabled",
    }


@pytest.mark.parametrize("strategy_id", sorted(DEMO_TRIAL_STRATEGY_IDS))
@pytest.mark.parametrize("to_stage", sorted(_ADVANCING_STAGES))
def test_promote_refuses_every_advancing_stage_before_any_read(strategy_id: str, to_stage: Any) -> None:
    with pytest.raises(StrategyControlError):
        promote_strategy(
            _NO_DB,
            strategy_id=strategy_id,
            strategy_version="v1",
            to_stage=to_stage,
            promoted_by="operator",
            reason="must not advance",
            evidence_ref="result:test",
            result_ids=(1,),
        )


def test_live_gate_refuses_a_demo_trial_purpose() -> None:
    # Every fact passes, so the purpose code leads (the missing policy follows it).
    refusals = live_gate_refusals(
        purpose=registered_strategy_purpose("ai-discretionary-v1"),
        policy=None,
        declaration=None,
        facts=_facts(),
        requested_capital=Decimal("100"),
    )
    assert refusals[0] == "strategy_not_capital_candidate"
