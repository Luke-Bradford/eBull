"""#3515 slice 3b-ii — the trial version descriptor reproduces v1 exactly (fund-v1 spec §7)."""

from __future__ import annotations

import dataclasses

import pytest

from app.services import ai_trial_run
from app.services.ai_trial_fund_blocks import FUND_SYSTEM_PROMPT
from app.services.ai_trial_fund_pack import build_fund_pack
from app.services.ai_trial_fund_policy import (
    FUND_FROZEN_CONSTANTS,
    FUND_POLICY_HASH,
    FUND_POLICY_MODULES,
    fund_policy_manifest,
)
from app.services.ai_trial_guard import decision_json_schema
from app.services.ai_trial_invocation import InvocationResult, build_argv
from app.services.ai_trial_pack import canonical_sha256
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH, FROZEN_CONSTANTS, POLICY_MODULES
from app.services.ai_trial_prompt import SYSTEM_PROMPT, SYSTEM_PROMPT_SHA256
from app.services.ai_trial_run import RunEnvironment, _provenance
from app.services.ai_trial_version import FUND_V1, TRIAL_VERSIONS, V1, trial_version
from app.services.strategy_manifest import DEMO_TRIAL_STRATEGY_IDS

ENV = RunEnvironment(executable="/usr/local/bin/claude", cli_version="2.1.280", git_sha="b" * 40, source_env={})


def test_v1_descriptor_reproduces_the_values_v1_bound_as_constants() -> None:
    assert (V1.arm_strategy_id, V1.control_strategy_id, V1.strategy_version) == (
        "ai-discretionary-v1",
        "ai-discretionary-v1-control",
        "v1",
    )
    assert V1.policy_hash == AI_TRIAL_POLICY_HASH
    assert V1.system_prompt == SYSTEM_PROMPT
    assert V1.system_prompt_sha256 == SYSTEM_PROMPT_SHA256
    assert (ai_trial_run.TRIAL_ARM_STRATEGY_ID, ai_trial_run.TRIAL_STRATEGY_VERSION) == ("ai-discretionary-v1", "v1")


def test_the_descriptor_and_fund_modules_are_not_hashed_by_v1() -> None:
    # Adding a version must never move v1's policy_hash (fund-v1 spec §0 rule 1).
    assert not {
        "ai_trial_version.py",
        "ai_trial_pack_build.py",
        "ai_trial_fund_pack.py",
        "ai_trial_fund_policy.py",
        "ai_trial_fund_blocks.py",
    } & set(POLICY_MODULES)


def test_every_registered_leg_is_a_demo_trial_leg() -> None:
    legs = {leg for version in TRIAL_VERSIONS for leg in version.leg_strategy_ids}
    assert legs <= DEMO_TRIAL_STRATEGY_IDS
    assert len({(v.arm_strategy_id, v.strategy_version) for v in TRIAL_VERSIONS}) == len(TRIAL_VERSIONS)


def test_an_unregistered_version_has_no_descriptor() -> None:
    assert trial_version("ai-discretionary-v1", "v1") is V1
    with pytest.raises(LookupError):
        trial_version("ai-discretionary-v1", "v2")
    with pytest.raises(LookupError):
        trial_version("ai-discretionary-fund-v1", "v2")
    assert trial_version("ai-discretionary-fund-v1", "v1") is FUND_V1


def test_fund_v1_descriptor() -> None:
    assert FUND_V1.leg_strategy_ids == ("ai-discretionary-fund-v1", "ai-discretionary-fund-v1-control")
    assert FUND_V1.system_prompt == FUND_SYSTEM_PROMPT != SYSTEM_PROMPT
    assert FUND_V1.policy_hash == FUND_POLICY_HASH != AI_TRIAL_POLICY_HASH
    assert FUND_V1.build_pack is build_fund_pack


def test_the_fund_manifest_covers_v1s_and_the_shared_modules_slice_3_changed() -> None:
    # §7: v1's hashed modules, fund-v1's own, the shared non-hashed modules, v1's constants.
    assert set(POLICY_MODULES) < set(FUND_POLICY_MODULES)
    assert {
        "ai_trial_fund_blocks.py",
        "ai_trial_fund_pack.py",
        "ai_trial_run.py",
        "ai_trial_jobs.py",
        "ai_trial_halts.py",
        "ai_trial_readout.py",
        "ai_trial_version.py",
        "ai_trial_pack_build.py",
        "ai_trial_wind_down.py",
    } <= set(FUND_POLICY_MODULES)
    assert FROZEN_CONSTANTS.items() <= FUND_FROZEN_CONSTANTS.items()
    assert "mdna_extraction.extractor_id" in FUND_FROZEN_CONSTANTS
    assert fund_policy_manifest().digest() == FUND_POLICY_HASH


def test_v1_run_provenance_is_unchanged() -> None:
    result = InvocationResult(None, "", None, {"num_turns": 1, "total_cost_usd": 0.5}, {}, 0, b"", b"", 10)
    values = _provenance(ENV, V1, result=result)
    assert values["policy_hash"] == AI_TRIAL_POLICY_HASH
    assert values["system_prompt_sha256"] == SYSTEM_PROMPT_SHA256
    assert values["argv_sha256"] == canonical_sha256(
        build_argv(
            ENV.executable, model_id="claude-opus-5-5", system_prompt=SYSTEM_PROMPT, json_schema=decision_json_schema()
        )
    )


def test_provenance_follows_the_version() -> None:
    other = dataclasses.replace(V1, policy_hash="f" * 64, system_prompt=SYSTEM_PROMPT + "\nmore")
    values = _provenance(ENV, other)
    assert values["policy_hash"] == "f" * 64
    assert values["system_prompt_sha256"] == other.system_prompt_sha256 != SYSTEM_PROMPT_SHA256
