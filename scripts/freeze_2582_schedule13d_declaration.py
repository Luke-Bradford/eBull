"""Freeze C-4's #2599 preregistration declaration. Run ONCE, before outcomes open.

Spec: ``docs/proposals/ta/2026-08-12-c4-declaration-gate-binding.md``. Refs #2614.

⚠⚠ THIS IS A SEPARATE SCRIPT FOR THE ONLY REASON THAT MATTERS: A DECLARATION
FROZEN BY THE THING THAT OPENS THE OUTCOMES DECLARES NOTHING.

The gate could freeze this row itself and every field would be identical. It
would also be worthless — the declaration's entire force is that it predates the
look, and a row written by the evaluator is a description of a run already under
way. That is #2599's checkpoint-1 defect (*"a caller can construct a favourable
declaration after seeing/reading outcomes"*) rebuilt with the constructor moved
one frame. So freezing lives here, is run separately and earlier, and
``require_outcome_gate`` refuses until it has been.

⚠ NO NUMBER BELOW IS CHOSEN HERE. The stamps come from the digest-frozen
contract, the refusal list is COMPUTED by ``structural_promotion_refusals``
rather than spelled out, and the forward-shadow floor is derived by construction
from the contract's own power calculation and a measured, outcome-free arrival
rate — see ``_FORWARD_SHADOW_DERIVATION`` and the test that re-derives it.

⚠⚠ THIS CANNOT BE RUN "ANYTIME" — THE FREEZE IS COUPLED TO THE POLICY VERSION
---------------------------------------------------------------------------
That advice was given to the operator in session on 2026-08-12 and was wrong
(#2631). The row records ``STRUCTURAL_REFUSAL_POLICY_VERSION``, and
``declaration_refusals`` returns ``structural_refusal_policy_superseded`` the
moment that string stops matching the current constant — permanently, because
``sql/333`` bars UPDATE and DELETE and holds the identity key, so no corrected
row can replace it.

**Recovery cost, in full**: a new ``strategy_version``. That is not merely a
rename — it changes the trial's identity, the old trial stays inaccessible
forever, and re-running charges the shared trial register again (#2600), raising
the deflated-Sharpe bar for every other candidate. There is no cheaper path.

So: run ``--dry-run`` first, read ``structural_refusal_policy_version`` in its
output, and freeze only when no change to the structural refusal policy is in
flight. ``scripts/_prereg_freeze_guard.py`` refuses automatically when this
tree's constant is not the one on ``origin/main`` — and states plainly which
case it cannot see.
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
from scripts.evaluate_2582_schedule13d_outcomes import STRATEGY_ID, STRATEGY_VERSION
from scripts.verify_2582_schedule13d_preregistration import load_and_verify

#: The contract's frozen power calculation, asserted by
#: ``verify_2582_schedule13d_preregistration.py``: ``round(7.84888 x (10.0/1.0)^2)``
#: at alpha 0.05 two-sided and power 0.8, where ``7.84888 = (z_0.975 + z_0.8)^2``.
_MINIMUM_EFFECTIVE_EVENTS: Final = 785

#: C-4's own primary population, measured 2026-08-12 on the FULL population (no
#: sample) and entirely outcome-free — source events and their public filing
#: dates, no price bar. Reproduce with:
#:
#:   PYTHONPATH=. uv run python -c "
#:   import psycopg; from app.config import settings
#:   from scripts.evaluate_2582_schedule13d_outcomes import load_source_events
#:   with psycopg.connect(settings.database_url) as c:
#:       c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
#:       ev=[e for e in load_source_events(c) if e.primary_source_refusal is None]; c.rollback()
#:   d=sorted({e.public_filing_date for e in ev})
#:   print(len(ev), len(d), (d[-1]-d[0]).days)"
_MEASURED_CLEAN_EVENTS: Final = 963
_MEASURED_DISTINCT_FILING_DATES: Final = 331
_MEASURED_SPAN_DAYS: Final = 547

#: ⚠ ``ceil`` both times, and BOTH ARE LOWER BOUNDS — the honest direction. The
#: contract's 785 is an EFFECTIVE sample size whose own definition
#: (``min_raw_n, sample_return_variance / pigeonhole_bootstrap_mean_variance``)
#: makes effective <= raw, so 785 raw events is the smallest raw count that could
#: satisfy it. Clustering by issuer and entry session pushes the true requirement
#: up, never down.
MIN_FORWARD_DECISION_DATES: Final = math.ceil(
    _MINIMUM_EFFECTIVE_EVENTS * _MEASURED_DISTINCT_FILING_DATES / _MEASURED_CLEAN_EVENTS
)
MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(
    MIN_FORWARD_DECISION_DATES * _MEASURED_SPAN_DAYS / (_MEASURED_DISTINCT_FILING_DATES * 7)
)

_FORWARD_SHADOW_DERIVATION: Final = (
    f"Contract power calculation: {_MINIMUM_EFFECTIVE_EVENTS} effective events = round(7.84888 x (10.0/1.0)^2), "
    "alpha 0.05 two-sided, power 0.8 (verify_2582_schedule13d_preregistration.py). Measured full-population "
    f"outcome-free arrival 2026-08-12: {_MEASURED_CLEAN_EVENTS} clean 13D events over "
    f"{_MEASURED_DISTINCT_FILING_DATES} distinct public filing dates spanning {_MEASURED_SPAN_DAYS} days "
    "(2024-12-18..2026-06-18), reproduced by scripts/freeze_2582_schedule13d_declaration.py's module docstring "
    f"command. dates = ceil(785 x {_MEASURED_DISTINCT_FILING_DATES} / {_MEASURED_CLEAN_EVENTS}) = "
    f"{MIN_FORWARD_DECISION_DATES}; weeks = ceil({MIN_FORWARD_DECISION_DATES} x {_MEASURED_SPAN_DAYS} / "
    f"({_MEASURED_DISTINCT_FILING_DATES} x 7)) = {MIN_FORWARD_CALENDAR_WEEKS}. Both are LOWER bounds: 785 is an "
    "effective sample size and effective <= raw, so clustering raises the requirement, never lowers it."
)

DECLARED_BY: Final = "scripts/freeze_2582_schedule13d_declaration.py (#2614)"


def build_declaration() -> PreregDeclaration:
    """C-4's declaration, every field traced to the digest-frozen contract.

    ⚠ ``prereg_purpose`` is ``falsification_only`` because the contract already
    says so twice — ``decision: historical_falsification_only`` and
    ``acceptance.historical_archive_can_promote_capital: false``. The declaration
    records what the contract states; it does not decide it.

    ⚠ ``survivor_only`` is not a guess about the ``paperswithbacktest`` corpus. It
    follows from the contract's OWN eligibility rule —
    ``current_is_tradable_required: true`` and
    ``historical_security_identity_limit: current_snapshot_only_...`` — which
    restricts the population to instruments that survived to today. #2288's rule
    ("an unlabelled result is treated as survivor_only, never as validated") and
    the one-member ``PROMOTABLE_UNIVERSE_BASES`` allowlist point the same way.

    ⚠ ``carry_unmodelled=True`` is read off the stamp's own definition
    (``strategy_result.py``: "carry and FX are NULL, not zero"). The contract's
    ``position`` block charges a flat ``round_trip_adverse_cost_bps: 50`` and
    names no carry, borrow or FX term.
    """

    contract, _digest = load_and_verify()
    if contract["candidate_id"] != STRATEGY_ID or contract["contract_version"] != STRATEGY_VERSION:
        raise ValueError(
            f"contract identity moved: {contract['candidate_id']}/{contract['contract_version']} "
            f"is not {STRATEGY_ID}/{STRATEGY_VERSION}"
        )
    if contract["decision"] != "historical_falsification_only":
        raise ValueError(f"contract decision is {contract['decision']!r}, not historical_falsification_only")
    if contract["acceptance"]["historical_archive_can_promote_capital"] is not False:
        raise ValueError("contract now claims the historical archive can promote capital; re-read the purpose")

    universe_basis = "survivor_only"
    carry_unmodelled = True
    # #2363 split the cost refusal; FX is unmodelled on the same stamp.
    fx_unmodelled = True
    return PreregDeclaration(
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        contract_version=str(contract["contract_version"]),
        prereg_purpose="falsification_only",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=universe_basis,
        declared_carry_unmodelled=carry_unmodelled,
        declared_fx_unmodelled=fx_unmodelled,
        # ⚠ COMPUTED, never written out. A hand-typed list is a second copy of
        # the refusal policy that drifts the first time the policy moves — and
        # `declaration_refusals` would then refuse this row with
        # `expected_structural_refusals_mismatch`, which is the check working.
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
            "freeze even though this tree's STRUCTURAL_REFUSAL_POLICY_VERSION is not the one on "
            "origin/main, or that ref could not be refreshed. The override is recorded in the output; "
            "see scripts/_prereg_freeze_guard.py for why the default refuses."
        ),
    )
    args = parser.parse_args(argv)
    declaration = build_declaration()
    # ⚠ THE FULL DIGEST PAYLOAD, NOT A HAND-PICKED SUBSET (#2631). The subset
    # this replaced omitted `structural_refusal_policy_version`, which is the
    # field that decides whether the freeze is still valid tomorrow.
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
            # ⚠ A BARE UniqueViolation IS TWO DIFFERENT SITUATIONS WEARING ONE
            # ERROR, and they want opposite operator actions: a retry after an
            # uncertain commit is fine, and a second DIFFERENT declaration is the
            # fabrication sql/333's UNIQUE constraint exists to stop. Distinguish
            # them by digest rather than making the operator guess.
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
            stored_digest = None if stored is None else stored.declaration_sha256
            sys.stderr.write(
                json.dumps(
                    {
                        "outcome": "conflicting_declaration_already_frozen",
                        "stored_declaration_sha256": stored_digest,
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
    "DECLARED_BY",
    "MIN_FORWARD_CALENDAR_WEEKS",
    "MIN_FORWARD_DECISION_DATES",
    "build_declaration",
    "main",
]
