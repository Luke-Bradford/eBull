"""#3515 slice 3b-iii — fund-v1's policy manifest and hash (fund-v1 spec §7 "Declaration and manifest").

fund-v1's hash covers what v1's does (v1's ten hashed modules and ``FROZEN_CONSTANTS``) plus:
fund-v1's own modules, the shared non-hashed modules slice 3 changed (fund-v1's code runs
through them, so a change to one is a change to fund-v1), and fund-v1's constants. A
fund-v1 declaration whose stored hash differs is refused ``policy_drift`` exactly as v1's is.

⚠ v1's own ``policy_hash`` is untouched: this module is not in ``ai_trial_policy.POLICY_MODULES``
and adds nothing to ``FROZEN_CONSTANTS`` (spec §0 rule 1).

``INPUT_TOKEN_CEILING`` is frozen here (slice 2b). The budget fixture's sha and measured pair join once
the fixture passes the ceiling (spec §6, measured 2026-10-01: it does not yet); nothing is frozen before
then, so the hash moving is expected.
"""

from __future__ import annotations

from typing import Final

from app.services.ai_trial_fund_blocks import (
    AMENDMENT_FORMS,
    FACTS_PER_REPORT_MAX,
    INPUT_TOKEN_CEILING,
    MAX_CLAIM_TO_SNAPSHOT,
    MDNA_MAX_CHARS,
    MDNA_MIN_SUBSTANTIVE_CHARS,
    MIN_BLOCK_SHARE,
    ORIGINAL_FORMS,
    K,
)
from app.services.ai_trial_policy import FROZEN_CONSTANTS, POLICY_MODULES, PolicyManifest, policy_manifest
from app.services.mdna_extraction import extractor_id

#: v1's hashed modules, fund-v1's own, and the shared non-hashed modules slice 3 changed.
FUND_POLICY_MODULES: Final = tuple(
    sorted(
        {
            *POLICY_MODULES,
            # fund-v1's own.
            "ai_trial_fund_blocks.py",
            "ai_trial_fund_pack.py",
            # Shared, not hashed by v1; fund-v1's run, halts, readout and start gate go through them.
            "ai_trial_halts.py",
            "ai_trial_jobs.py",
            "ai_trial_pack_build.py",
            "ai_trial_readout.py",
            "ai_trial_run.py",
            "ai_trial_version.py",
            "ai_trial_wind_down.py",
        }
    )
)

FUND_FROZEN_CONSTANTS: Final[dict[str, object]] = {
    **FROZEN_CONSTANTS,
    "ai_trial_fund_blocks.AMENDMENT_FORMS": AMENDMENT_FORMS,
    "ai_trial_fund_blocks.FACTS_PER_REPORT_MAX": FACTS_PER_REPORT_MAX,
    "ai_trial_fund_blocks.INPUT_TOKEN_CEILING": INPUT_TOKEN_CEILING,
    "ai_trial_fund_blocks.K": K,
    "ai_trial_fund_blocks.MAX_CLAIM_TO_SNAPSHOT": MAX_CLAIM_TO_SNAPSHOT,
    "ai_trial_fund_blocks.MDNA_MAX_CHARS": MDNA_MAX_CHARS,
    "ai_trial_fund_blocks.MDNA_MIN_SUBSTANTIVE_CHARS": MDNA_MIN_SUBSTANTIVE_CHARS,
    "ai_trial_fund_blocks.MIN_BLOCK_SHARE": MIN_BLOCK_SHARE,
    "ai_trial_fund_blocks.ORIGINAL_FORMS": ORIGINAL_FORMS,
    # The MD&A text depends on the extractor that produced it (#3518 §4.3).
    "mdna_extraction.extractor_id": extractor_id(),
}


def fund_policy_manifest() -> PolicyManifest:
    return policy_manifest(modules=FUND_POLICY_MODULES, constants=FUND_FROZEN_CONSTANTS)


FUND_POLICY_HASH: Final = fund_policy_manifest().digest()

__all__ = ["FUND_FROZEN_CONSTANTS", "FUND_POLICY_HASH", "FUND_POLICY_MODULES", "fund_policy_manifest"]
