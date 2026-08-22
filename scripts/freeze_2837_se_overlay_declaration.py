"""Freeze S-E's #2599 preregistration declaration. Run ONCE, before outcomes open.

Contract: ``docs/proposals/ta/2026-08-22-se-ma-overlay-preregistration.md``.
Refs #2837, #2832. Follows ``scripts/freeze_2582_schedule13d_declaration.py``.

⚠⚠ A SEPARATE SCRIPT FOR THE ONLY REASON THAT MATTERS: A DECLARATION FROZEN BY
THE THING THAT OPENS THE OUTCOMES DECLARES NOTHING.
``scripts/measure_2837_se_overlay.py`` could freeze this row itself and every
field would be identical. It would also be worthless — the declaration's entire
force is that it predates the look. So freezing lives here, runs separately and
earlier, and the measurement's own gate refuses until it has.

⚠⚠ THIS CANNOT BE RUN "ANYTIME" — THE FREEZE IS COUPLED TO THE POLICY VERSION.
The row records ``STRUCTURAL_REFUSAL_POLICY_VERSION``, and
``prereg_contract.declaration_refusals`` returns
``structural_refusal_policy_superseded`` the moment that string stops matching
the current constant. ``sql/333`` bars UPDATE and DELETE, so no corrected row
can replace it: recovery is a NEW ``strategy_version``, which changes the
trial's identity, strands the old trial forever and charges the shared register
again. Run ``--dry-run`` first, read ``structural_refusal_policy_version`` in its
output, and freeze only when no change to that policy is in flight.

⚠⚠ AND IT CANNOT BE RUN BEFORE ITS REGISTER ENTRY IS ON MAIN.
``freeze_preregistration`` refuses a declaration no ``DeclaredTrial`` claims, and
``_prereg_freeze_guard`` refuses a tree whose policy version is not main's. So
the ordering is: merge ``se-ma-overlay-2026-08-22`` into
``app/services/trial_register.py`` → then run this from a tree that is main.

⚠ NO NUMBER BELOW IS CHOSEN HERE. The stamps are the contract's §2, the refusal
list is COMPUTED by ``structural_promotion_refusals`` rather than spelled out,
and the forward-shadow floor is derived by construction from #2837's own
declared priors — see ``_FORWARD_SHADOW_DERIVATION``.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Final

import psycopg

from app.config import settings
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration, load_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from scripts._prereg_freeze_guard import assert_policy_version_merged, policy_version_report

STRATEGY_ID: Final = "se-ma-overlay-drawdown-insurance"
STRATEGY_VERSION: Final = "se-ma-overlay-drawdown-insurance-v1"
CONTRACT_VERSION: Final = "se-ma-overlay-2026-08-22"
DECLARED_BY: Final = "scripts/freeze_2837_se_overlay_declaration.py (#2837)"

#: #2837's own declared priors, quoted from the issue text BEFORE any look:
#: "spy_chain_v1 ... 1993→2026, ~404 month-ends" and "max DD + 3 worst drawdowns
#: vs buy-and-hold on the same chain (~7 episodes >=15%)".
#:
#: ⚠ THESE ARE PRIORS, NOT MEASUREMENTS OF THIS RUN. Deriving the floor from the
#: chain's realised drawdowns would be reading an outcome to size the gate that
#: authorises reading outcomes.
_TICKET_MONTH_ENDS: Final = 404
_TICKET_EPISODES_OVER_CLASS: Final = 7

#: The contract's §9 readout unit: the three worst drawdowns.
_READOUT_EPISODES: Final = 3

MIN_FORWARD_DECISION_DATES: Final = math.ceil(_READOUT_EPISODES * _TICKET_MONTH_ENDS / _TICKET_EPISODES_OVER_CLASS)
#: Months to weeks with the constant ``freeze_2616_precutoff_declarations.py``
#: already uses for a monthly-formation candidate.
MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(MIN_FORWARD_DECISION_DATES * 365.25 / (12 * 7))

#: ⚠ sql/333 caps this column at 1000 characters. The longer reading is §10 of
#: the contract; this is the arithmetic plus the three limits that keep it from
#: being read as a power calculation.
_FORWARD_SHADOW_DERIVATION: Final = (
    "NOT a power calculation — none is published for a drawdown-ratio claim and none is invented. Fixed BY "
    "CONSTRUCTION from #2837's own pre-look priors: spy_chain_v1 carries ~"
    f"{_TICKET_MONTH_ENDS} month-ends and ~{_TICKET_EPISODES_OVER_CLASS} drawdown episodes >=15%, and the "
    f"contract's readout unit is the {_READOUT_EPISODES} worst drawdowns. dates = ceil("
    f"{_READOUT_EPISODES}x{_TICKET_MONTH_ENDS}/{_TICKET_EPISODES_OVER_CLASS}) = {MIN_FORWARD_DECISION_DATES}; "
    f"weeks = ceil({MIN_FORWARD_DECISION_DATES}x365.25/(12x7)) = {MIN_FORWARD_CALENDAR_WEEKS} (~14.5y), the "
    "month->week constant freeze_2616_precutoff_declarations.py uses. Three limits, stated: (1) three episodes "
    "are EXPECTED in that span, not assured, and one starting inside it may end unrecovered; (2) the dates are "
    "NOT statistically independent (10-month SMAs share 9 inputs, positions persist) and episodes cluster by "
    "regime, so 404/7 is a long-run average, not an exchangeable arrival rate; (3) clearing this floor would be "
    "necessary and nowhere near sufficient. falsification_only regardless, so it gates nothing already ungated."
)


def build_declaration() -> PreregDeclaration:
    """S-E's declaration. Every stamp is the contract's §2, read not decided.

    ⚠ ``single_index_proxy`` is the honest basis for one spliced index chain: it
    is not a survivorship-free cross-section, so ``PROMOTABLE_UNIVERSE_BASES``
    refuses it — which is the ticket's *"never a strategy promotion, never in the
    strategy funnel"* obtained structurally rather than by convention.

    ⚠ ``carry_unmodelled`` because cash below the SMA earns ZERO (contract §4.9);
    cash yield is carry. ``fx_unmodelled`` because the chain is USD and the
    account is GBP with no frozen 1993→2026 rate series. Both are the run's
    actual state, not a pessimistic label.

    ⚠ ``falsification_only`` follows from those stamps rather than being chosen:
    a ``capital_candidate`` purpose over a non-empty recomputed refusal list is
    exactly what ``ineligible_trial_not_declared_falsification`` refuses.
    """
    universe_basis = "single_index_proxy"
    carry_unmodelled = True
    fx_unmodelled = True
    return PreregDeclaration(
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        contract_version=CONTRACT_VERSION,
        prereg_purpose="falsification_only",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=universe_basis,
        declared_carry_unmodelled=carry_unmodelled,
        declared_fx_unmodelled=fx_unmodelled,
        # ⚠ COMPUTED, never written out. A hand-typed list is a second copy of
        # the refusal policy that drifts the first time the policy moves.
        expected_structural_refusals=structural_promotion_refusals(
            universe_basis=universe_basis, carry_unmodelled=carry_unmodelled, fx_unmodelled=fx_unmodelled
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build and print the declaration and its digest without writing it",
    )
    parser.add_argument(
        "--allow-policy-divergence",
        action="store_true",
        help=(
            "freeze even though this tree's STRUCTURAL_REFUSAL_POLICY_VERSION is not the one on origin/main, "
            "or that ref could not be refreshed. The override is recorded in the output."
        ),
    )
    args = parser.parse_args(argv)
    declaration = build_declaration()
    summary: dict[str, object] = {**declaration.digest_payload, "declaration_sha256": declaration.sha256}
    if args.dry_run:
        sys.stdout.write(json.dumps({**summary, **policy_version_report(), "outcome": "dry_run"}, sort_keys=True))
        sys.stdout.write("\n")
        return 0

    summary.update(assert_policy_version_merged(allow_divergence=args.allow_policy_divergence))
    with psycopg.connect(settings.database_url) as conn:
        try:
            declaration_id = freeze_preregistration(conn, declaration)
        except psycopg.errors.UniqueViolation:
            # ⚠ A bare UniqueViolation is two situations wearing one error, and
            # they want opposite operator actions: a retry after an uncertain
            # commit is fine, and a second DIFFERENT declaration is the
            # fabrication sql/333's UNIQUE constraint exists to stop.
            conn.rollback()
            stored = load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
            if stored is not None and stored.declaration_sha256 == declaration.sha256:
                sys.stdout.write(
                    json.dumps(
                        {**summary, "outcome": "already_frozen_identical", "declaration_id": stored.declaration_id},
                        sort_keys=True,
                    )
                    + "\n"
                )
                return 0
            sys.stderr.write(
                json.dumps(
                    {
                        "outcome": "conflicting_declaration_already_frozen",
                        "stored_declaration_sha256": None if stored is None else stored.declaration_sha256,
                        "would_have_frozen_sha256": declaration.sha256,
                        "note": "declarations are immutable; different terms mean a new strategy_version",
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            return 1
        except PreregDeclarationRefused as refused:
            conn.rollback()
            sys.stderr.write(
                json.dumps({"outcome": "refused", "refusals": list(refused.refusals)}, sort_keys=True) + "\n"
            )
            return 1
        conn.commit()
    sys.stdout.write(json.dumps({**summary, "outcome": "frozen", "declaration_id": declaration_id}, sort_keys=True))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONTRACT_VERSION",
    "DECLARED_BY",
    "MIN_FORWARD_CALENDAR_WEEKS",
    "MIN_FORWARD_DECISION_DATES",
    "STRATEGY_ID",
    "STRATEGY_VERSION",
    "build_declaration",
    "main",
]
