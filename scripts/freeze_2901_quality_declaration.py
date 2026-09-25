"""Freeze #2901's #2599 preregistration declaration. Run ONCE, after this merges and before the run.

Contract: ``docs/proposals/ta/2026-09-25-2901-quality-declaration.md`` (the frozen declaration), which
fixes everything else the run applies. Follows ``scripts/freeze_2840_sh_regime_gate_declaration.py``,
whose preamble warnings apply here verbatim and are not restated: this is a separate script from the
run; ``--dry-run`` first; run it from ``main`` once ``r6-2901-quality-gpa-2026-09-25`` has merged
(``freeze_preregistration`` refuses a declaration the in-tree register does not claim); no number below
is chosen here.

Two differences from that script, both so the row frozen is exactly the one the declaration document
publishes: it refuses any digest other than ``EXPECTED_DECLARATION_SHA256``, and it has no
policy-divergence override.

The identity is imported from the runner, which writes the holdout-access row under it, so the two
cannot drift apart.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from typing import Final

import psycopg

from app.config import settings
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration, load_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from scripts._prereg_freeze_guard import assert_policy_version_merged, policy_version_report
from scripts.run_2901_quality_trial import STRATEGY_ID, STRATEGY_VERSION

CONTRACT_VERSION: Final = "r6-2901-quality-declaration-2026-09-25"
#: The digest ``docs/proposals/ta/2026-09-25-2901-quality-declaration.md`` publishes.
EXPECTED_DECLARATION_SHA256: Final = "9235ab28186b4a33b59f4580df1af89a2c371ed51724dac5e925e612c4582cc4"
DECLARED_BY: Final = "scripts/freeze_2901_quality_declaration.py (#2901)"

#: The universe is the Intrader mirror with delisted series kept and terminations applied under
#: #3362's programme policies; the construction spec's residuals R1/R5/R6 qualify it and the
#: declaration repeats them.
UNIVERSE_BASIS: Final = "survivorship_free"
#: Long x1 US equities in a USD lane: no overnight financing exists, and USD in / held / out has
#: no conversion event. The one-off GBP funding conversion is reported as a sensitivity, never
#: charged in the verdict (declaration spec, "Capital boundary"). A bespoke contract owns its
#: stamps (``freeze_preregistration``'s manifest-only check), so they are declared, not read.
CARRY_UNMODELLED: Final = False
FX_UNMODELLED: Final = False

# --- The forward-shadow floor. Schedule facts only; no outcome is read. ---
#
# The pass leg's evidence supply is the 11 complete holding-year cohorts (formations 2013-2023),
# each from one annual decision date X(D). By construction (#2840's precedent: the floor equals
# the evidence supply behind the pass leg), a forward record matches it at 11 decision dates
# spanning X(2013) to X(2024) of the frozen schedule (runner --census-only output).
_FIRST_COHORT_X: Final = date(2013, 7, 1)
_LAST_COHORT_END_X: Final = date(2024, 7, 1)
_COMPLETE_COHORTS: Final = 11
MIN_FORWARD_DECISION_DATES: Final = _COMPLETE_COHORTS
MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil((_LAST_COHORT_END_X - _FIRST_COHORT_X).days / 7)

#: ⚠ sql/333 caps this column at 1000 characters.
_FORWARD_SHADOW_DERIVATION: Final = (
    "NOT a power calculation; none is published for an annual cross-sectional cohort test and none is "
    "invented. Fixed BY CONSTRUCTION from the frozen schedule (no outcome read): the pass leg rests on "
    f"{_COMPLETE_COHORTS} complete holding-year cohorts, one annual decision date each, so dates = "
    f"{MIN_FORWARD_DECISION_DATES}; weeks = ceil(({_LAST_COHORT_END_X} - {_FIRST_COHORT_X}) days / 7) = "
    f"{MIN_FORWARD_CALENDAR_WEEKS}. Limits: (1) this identity is a historical backtest and never runs "
    "forward; a PASS_ROBUST opens a paper ticket whose forward procedure is a different strategy_version "
    "with its own declaration and floor (declaration spec, 'Capital boundary'); (2) annual cohorts are few "
    "and not shown independent; (3) clearing the floor is necessary, not sufficient."
)


def build_declaration() -> PreregDeclaration:
    """#2901's declaration. Every stamp is the run's actual state.

    ``capital_candidate`` because the verdict can open a paper-deployment ticket
    (``PASS_ROBUST``), and ``structural_promotion_refusals`` over these stamps is
    empty. The PIT registry's refusal of ``R6RankingIdentity.QUALITY`` is a further
    gate that the paper ticket carries; it is not a structural refusal code.
    """
    return PreregDeclaration(
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        contract_version=CONTRACT_VERSION,
        prereg_purpose="capital_candidate",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=UNIVERSE_BASIS,
        declared_carry_unmodelled=CARRY_UNMODELLED,
        declared_fx_unmodelled=FX_UNMODELLED,
        expected_structural_refusals=structural_promotion_refusals(
            universe_basis=UNIVERSE_BASIS, carry_unmodelled=CARRY_UNMODELLED, fx_unmodelled=FX_UNMODELLED
        ),
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=MIN_FORWARD_DECISION_DATES,
            min_calendar_weeks=MIN_FORWARD_CALENDAR_WEEKS,
            derivation=_FORWARD_SHADOW_DERIVATION,
        ),
        declared_by=DECLARED_BY,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print the declaration and its digest, write nothing")
    args = parser.parse_args(argv)
    declaration = build_declaration()
    summary: dict[str, object] = {**declaration.digest_payload, "declaration_sha256": declaration.sha256}
    if args.dry_run:
        sys.stdout.write(json.dumps({**summary, **policy_version_report(), "outcome": "dry_run"}, sort_keys=True))
        sys.stdout.write("\n")
        return 0

    if declaration.sha256 != EXPECTED_DECLARATION_SHA256:
        sys.stderr.write(
            json.dumps(
                {
                    "outcome": "digest_not_published",
                    "digest": declaration.sha256,
                    "published": EXPECTED_DECLARATION_SHA256,
                }
            )
            + "\n"
        )
        return 1
    summary.update(assert_policy_version_merged(allow_divergence=False))
    with psycopg.connect(settings.database_url) as conn:
        try:
            declaration_id = freeze_preregistration(conn, declaration)
        except psycopg.errors.UniqueViolation:
            conn.rollback()
            stored = load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
            if stored is not None and stored.declaration_sha256 == declaration.sha256:
                outcome = {**summary, "outcome": "already_frozen_identical", "declaration_id": stored.declaration_id}
                sys.stdout.write(json.dumps(outcome, sort_keys=True) + "\n")
                return 0
            conflict = {
                "outcome": "conflicting_declaration_already_frozen",
                "stored_declaration_sha256": None if stored is None else stored.declaration_sha256,
                "would_have_frozen_sha256": declaration.sha256,
            }
            sys.stderr.write(json.dumps(conflict, sort_keys=True) + "\n")
            return 1
        except PreregDeclarationRefused as refused:
            conn.rollback()
            sys.stderr.write(json.dumps({"outcome": "refused", "refusals": list(refused.refusals)}) + "\n")
            return 1
        conn.commit()
    sys.stdout.write(json.dumps({**summary, "outcome": "frozen", "declaration_id": declaration_id}, sort_keys=True))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
