"""#3515 slice 3b-ii — the trial version descriptor (fund-v1 spec §7 "Version identity").

Spec: ``docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md`` §0, §7.

v1's identity used to be bound in the non-hashed modules as module constants (the arm id, the
policy hash, the system prompt, the pack builder). A descriptor carries them instead, so the
decision run, the jobs, the halts and the readout serve any registered version, and every query
keys on the descriptor's ids or the declaration id: persisted rows of one version never reach
another.

* ``V1`` reproduces today's values exactly (pinned by ``tests/test_ai_trial_version.py``).
* ``FUND_V1`` (slice 3b-iii): fund-v1's ids, ``FUND_POLICY_HASH``, ``FUND_SYSTEM_PROMPT``,
  ``build_fund_pack`` and (slice 2c) the §6 post-call budget halt. Registered, so it has code; it
  runs only once its own declaration is frozen, and its job's start gate refuses while v1 is not
  wound down (§0 rule 2).
* This module is NOT one of ``ai_trial_policy.POLICY_MODULES``: adding a version here cannot
  move v1's ``policy_hash``. v1's hashed modules are called as they are, with the descriptor's
  values passed as arguments (fund-v1 spec §0 rule 1, §7).
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

import psycopg

from app.services.ai_trial_fund_blocks import FUND_SYSTEM_PROMPT, post_call_halt_reason
from app.services.ai_trial_fund_pack import build_fund_pack
from app.services.ai_trial_fund_policy import FUND_POLICY_HASH
from app.services.ai_trial_pack_build import BuiltPack, PackBuilder, PackRefusal
from app.services.ai_trial_pack_reader import AccountContext, IntradayFetch, Step1, assemble_pack
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH
from app.services.ai_trial_prompt import SYSTEM_PROMPT

Conn = psycopg.Connection[Any]


@dataclass(frozen=True)
class TrialVersion:
    """One trial version's identity. The control leg's id is always the arm's plus ``-control``
    (v1 spec §8 "two legs, two identities"; ``sql/442``'s ``strategy_id`` CHECK holds the arm)."""

    arm_strategy_id: str
    strategy_version: str
    #: The code's policy hash, computed at import; a declaration whose stored hash differs is
    #: refused ``policy_drift`` (v1 spec O6).
    policy_hash: str
    system_prompt: str
    build_pack: PackBuilder
    #: After the model call: ``(refusal_reason, usage) -> reason`` to refuse the run AND halt the trial
    #: (``halted_operator``), or ``None``. v1 has none; fund-v1's is its §6 budget rule.
    post_call_halt: Callable[[str | None, object], str | None] | None = None

    @property
    def control_strategy_id(self) -> str:
        return self.arm_strategy_id + "-control"

    @property
    def leg_strategy_ids(self) -> tuple[str, str]:
        return self.arm_strategy_id, self.control_strategy_id

    @property
    def system_prompt_sha256(self) -> str:
        return hashlib.sha256(self.system_prompt.encode("utf-8")).hexdigest()


def _v1_pack(
    conn: Conn, *, step1: Step1, account: AccountContext, fetch_intraday: IntradayFetch, declaration: Any
) -> BuiltPack:
    return BuiltPack(assemble_pack(conn, step1=step1, account=account, fetch_intraday=fetch_intraday))


V1: Final = TrialVersion(
    arm_strategy_id="ai-discretionary-v1",
    strategy_version="v1",
    policy_hash=AI_TRIAL_POLICY_HASH,
    system_prompt=SYSTEM_PROMPT,
    build_pack=_v1_pack,
)

FUND_V1: Final = TrialVersion(
    arm_strategy_id="ai-discretionary-fund-v1",
    strategy_version="v1",
    policy_hash=FUND_POLICY_HASH,
    system_prompt=FUND_SYSTEM_PROMPT,
    build_pack=build_fund_pack,
    post_call_halt=post_call_halt_reason,
)

#: Every version the engine serves. A declaration of any other ``(arm id, version)`` has no code.
TRIAL_VERSIONS: Final[tuple[TrialVersion, ...]] = (V1, FUND_V1)


def trial_version(arm_strategy_id: str, strategy_version: str) -> TrialVersion:
    """The registered descriptor, or ``LookupError``: an unregistered version fails closed."""
    for version in TRIAL_VERSIONS:
        if (version.arm_strategy_id, version.strategy_version) == (arm_strategy_id, strategy_version):
            return version
    raise LookupError(f"no registered trial version {arm_strategy_id}/{strategy_version}")


__all__ = [
    "FUND_V1",
    "TRIAL_VERSIONS",
    "V1",
    "BuiltPack",
    "PackBuilder",
    "PackRefusal",
    "TrialVersion",
    "trial_version",
]
