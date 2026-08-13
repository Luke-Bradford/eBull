"""Freeze #2599 declarations for the two pre-cutoff sealed openers. Run ONCE each.

Refs #2616. Follows ``scripts/freeze_2582_schedule13d_declaration.py`` (#2614):
a declaration frozen by the thing that opens the outcomes declares nothing, so
freezing lives here, runs separately and earlier, and each script's
``require_outcome_gate`` refuses ``preregistration_not_frozen`` until it has.

⚠⚠ THE TICKET'S POWER-CALCULATION PREMISE WAS CHECKED AND IS FALSE. #2616's
scope says the forward-shadow floor is "derived from each candidate's own
preregistration power calculation (both have one)". Neither
``docs/proposals/ta/2026-08-10-pead-preregistration.md`` nor
``docs/proposals/ta/2026-08-10-insider-purchase-preregistration.md`` contains
one — no alpha, no power, no minimum-N arithmetic (checked 2026-08-12; C-4's
contract, by contrast, froze ``round(7.84888 x (10.0/1.0)^2) = 785``). Where a
declared formulation does not exist the rule is to fix one BY CONSTRUCTION and
freeze the constants, not to invent a citation.

⚠ THE CONSTRUCTION, stated once and shared by both floors: the smallest forward
sample at which the candidate's OWN preregistered primary gate — a positive
lower 95% confidence bound — reaches power 0.8 if the sealed run's point
estimate is the true effect. That is ``n_obs x (se_obs x (z_0.975 + z_0.8) /
effect)^2`` with the same standard constants C-4's contract froze
(``7.84888 = 2.8016^2``), every input a published figure in the sealed result
document. Both sealed runs FAILED that gate, so both floors are enormous; that
is the arithmetic's verdict, recorded rather than smoothed — a forward shadow
that could not detect the claimed effect validates nothing, and both trials are
``falsification_only`` regardless.

⚠⚠ THIS CANNOT BE RUN "ANYTIME" — see the same section in
``scripts/freeze_2582_schedule13d_declaration.py`` (#2631). The frozen row
records ``STRUCTURAL_REFUSAL_POLICY_VERSION``; when that constant moves, the
declaration is refused ``structural_refusal_policy_superseded`` forever and
``sql/333`` permits no repair. Recovery is a new ``strategy_version`` — a new
trial identity, the old one inaccessible, and another charge against the shared
trial register. Read the ``--dry-run`` output's ``structural_refusal_policy_version``
before committing to a freeze.
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
from scripts.sealed_rerun_gate import SealedTrialIdentity, verify_preregistration_document
from scripts.verify_2476_pead_outcomes import SEALED_TRIAL as PEAD_SEALED_TRIAL
from scripts.verify_2480_insider_outcomes import SEALED_TRIAL as INSIDER_SEALED_TRIAL

#: z(0.975) + z(0.8) — alpha 0.05 two-sided at power 0.8; squared is the
#: 7.84888 C-4's contract froze. The one non-candidate constant in either floor.
_Z_SUM: Final = 2.8016

DECLARED_BY: Final = "scripts/freeze_2616_precutoff_declarations.py (#2616)"


def _required_forward_samples(*, observed: int, mean_pct: float, ci_low_pct: float, ci_high_pct: float) -> int:
    """The construction from the module docstring, in one place for both trials.

    ⚠ ``ceil``, and the CI half-width is read as ``1.96 x SE``. For the pead arm
    the interval is a Bonferroni JOINT interval, so that read OVERSTATES the
    standard error — which RAISES the floor, the strict direction for a floor.
    """

    standard_error = (ci_high_pct - ci_low_pct) / (2 * 1.96)
    return math.ceil(observed * (standard_error * _Z_SUM / mean_pct) ** 2)


# --- pead-historical-sue-net-income-v1 ---------------------------------------
#: Frozen from docs/proposals/ta/2026-08-10-pead-result.md §"Preregistered
#: primary result": +0.440% net mean per 62-session event, date-clustered
#: Bonferroni 95% joint CI [-2.866%, +3.763%], 2,427 issuer-deduplicated events
#: over 508 entry dates. Window: 2022-01-01 .. the frozen corpus frontier
#: 2026-07-08 = 1,649 days.
_PEAD_SEALED_EVENTS: Final = 2427
_PEAD_SEALED_ENTRY_DATES: Final = 508
_PEAD_SEALED_NET_MEAN_PCT: Final = 0.440
_PEAD_SEALED_CI_LOW_PCT: Final = -2.866
_PEAD_SEALED_CI_HIGH_PCT: Final = 3.763
_PEAD_PRIMARY_SPAN_DAYS: Final = 1649

PEAD_MIN_FORWARD_EVENTS: Final = _required_forward_samples(
    observed=_PEAD_SEALED_EVENTS,
    mean_pct=_PEAD_SEALED_NET_MEAN_PCT,
    ci_low_pct=_PEAD_SEALED_CI_LOW_PCT,
    ci_high_pct=_PEAD_SEALED_CI_HIGH_PCT,
)
PEAD_MIN_FORWARD_DECISION_DATES: Final = math.ceil(
    PEAD_MIN_FORWARD_EVENTS * _PEAD_SEALED_ENTRY_DATES / _PEAD_SEALED_EVENTS
)
PEAD_MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(
    PEAD_MIN_FORWARD_DECISION_DATES * _PEAD_PRIMARY_SPAN_DAYS / (_PEAD_SEALED_ENTRY_DATES * 7)
)

#: ⚠ sql/333 caps this column at 1000 characters; the DB test caught the first
#: draft over it. The longer reading lives in this module's docstring.
_PEAD_FORWARD_SHADOW_DERIVATION: Final = (
    "No power calculation exists in 2026-08-10-pead-preregistration.md (checked 2026-08-12; #2616's premise "
    "corrected). Constructed floor: smallest forward sample at which the prereg's own primary gate (positive "
    "lower 95% CI bound) reaches power 0.8 if the sealed point estimate is true; alpha 0.05 two-sided, "
    "z_0.975+z_0.8=2.8016 (C-4's constants; 7.84888=2.8016^2). Inputs: 2026-08-10-pead-result.md "
    f"§'Preregistered primary result' — {_PEAD_SEALED_NET_MEAN_PCT:+.3f}%/event, Bonferroni 95% joint CI "
    f"[{_PEAD_SEALED_CI_LOW_PCT:+.3f}%, {_PEAD_SEALED_CI_HIGH_PCT:+.3f}%], {_PEAD_SEALED_EVENTS} events / "
    f"{_PEAD_SEALED_ENTRY_DATES} entry dates / {_PEAD_PRIMARY_SPAN_DAYS} days (2022-01-01..2026-07-08). "
    f"SE=(3.763+2.866)/3.92=1.6911%; events=ceil(2427x(1.6911x2.8016/0.440)^2)={PEAD_MIN_FORWARD_EVENTS}; "
    f"dates=ceil(events x 508/2427)={PEAD_MIN_FORWARD_DECISION_DATES}; weeks=ceil(dates x 1649/(508x7))="
    f"{PEAD_MIN_FORWARD_CALENDAR_WEEKS}. Conservative both ways: the joint half-width read as 1.96xSE "
    "overstates SE, raising the floor; a true effect below the point estimate raises the genuine "
    "requirement. The size records the sealed effect cannot be validated forward; falsification_only."
)


# --- form4-code-p-opportunistic-purchase-v1 ----------------------------------
#: Frozen from docs/proposals/ta/2026-08-10-insider-purchase-result.md §"Sealed
#: result": +1.192% per month primary spread, block-bootstrap 95% CI
#: [-1.452%, +4.215%], 49 complete primary portfolio months. Formation is
#: monthly, so a decision date IS a month; weeks convert at 365.25/(12 x 7).
_INSIDER_SEALED_MONTHS: Final = 49
_INSIDER_SEALED_MEAN_PCT: Final = 1.192
_INSIDER_SEALED_CI_LOW_PCT: Final = -1.452
_INSIDER_SEALED_CI_HIGH_PCT: Final = 4.215

INSIDER_MIN_FORWARD_DECISION_DATES: Final = _required_forward_samples(
    observed=_INSIDER_SEALED_MONTHS,
    mean_pct=_INSIDER_SEALED_MEAN_PCT,
    ci_low_pct=_INSIDER_SEALED_CI_LOW_PCT,
    ci_high_pct=_INSIDER_SEALED_CI_HIGH_PCT,
)
INSIDER_MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(INSIDER_MIN_FORWARD_DECISION_DATES * 365.25 / (12 * 7))

#: ⚠ sql/333 caps this column at 1000 characters — same note as the pead floor.
_INSIDER_FORWARD_SHADOW_DERIVATION: Final = (
    "No power calculation exists in 2026-08-10-insider-purchase-preregistration.md (checked 2026-08-12; "
    "#2616's premise corrected). Constructed floor: smallest forward sample at which the prereg's own "
    "primary gate (positive lower 95% CI bound) reaches power 0.8 if the sealed point estimate is true; "
    "alpha 0.05 two-sided, z_0.975+z_0.8=2.8016 (C-4's constants; 7.84888=2.8016^2). Inputs: "
    "2026-08-10-insider-purchase-result.md §'Sealed result' — primary spread "
    f"{_INSIDER_SEALED_MEAN_PCT:+.3f}%/month, block-bootstrap 95% CI [{_INSIDER_SEALED_CI_LOW_PCT:+.3f}%, "
    f"{_INSIDER_SEALED_CI_HIGH_PCT:+.3f}%], {_INSIDER_SEALED_MONTHS} complete portfolio months. "
    f"SE=(4.215+1.452)/3.92=1.4457%; months=ceil(49x(1.4457x2.8016/1.192)^2)={INSIDER_MIN_FORWARD_DECISION_DATES}. "
    f"Formation is monthly, so decision dates = months; weeks=ceil(months x 365.25/(12x7))="
    f"{INSIDER_MIN_FORWARD_CALENDAR_WEEKS}. Conservative: a true effect below the point estimate raises the "
    "genuine requirement. A floor this size records the sealed effect cannot realistically be validated "
    "forward — the run failed its own gate; falsification_only."
)


def _build(
    *,
    trial: SealedTrialIdentity,
    contract_version: str,
    forward_shadow: ForwardShadowFloor,
) -> PreregDeclaration:
    """One declaration, every field traced to a frozen artefact.

    ⚠ ``prereg_purpose`` is ``falsification_only`` because both preregistrations
    already say so in substance: the pead document defines "no capital sizing
    path" and stamps survivor-only as a promotion refusal; the insider document
    calls its survivor-only corpus "a permanent promotion refusal for this
    trial". A ``capital_candidate`` purpose over these stamps would also be
    incoherent by ``declaration_refusals``'s own recomputation, which is the
    check working.

    ⚠ ``survivor_only`` and ``carry_unmodelled=True`` are read off the
    documents, not chosen: pead §"Universe, prices and costs" ("survivor-only is
    stamped and blocks promotion", "Short carry is not zero: it is unavailable
    and blocks promotion"); insider §"Frozen source population" ("The price
    corpus is survivor-only...") — and neither outcome service charges any carry
    term, per the standing cost model whose CARRY_BPS is None (#2363).
    """

    verify_preregistration_document(trial)
    universe_basis = "survivor_only"
    carry_unmodelled = True
    # #2363 split the cost refusal; FX is unmodelled on the same stamp.
    fx_unmodelled = True
    return PreregDeclaration(
        strategy_id=trial.strategy_id,
        strategy_version=trial.strategy_version,
        contract_version=contract_version,
        prereg_purpose="falsification_only",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=universe_basis,
        declared_carry_unmodelled=carry_unmodelled,
        declared_fx_unmodelled=fx_unmodelled,
        # ⚠ COMPUTED, never written out — a hand-typed list is a second copy of
        # the refusal policy that drifts the first time the policy moves.
        expected_structural_refusals=structural_promotion_refusals(
            universe_basis=universe_basis, carry_unmodelled=carry_unmodelled, fx_unmodelled=fx_unmodelled
        ),
        forward_shadow=forward_shadow,
        declared_by=DECLARED_BY,
    )


def build_pead_declaration() -> PreregDeclaration:
    return _build(
        trial=PEAD_SEALED_TRIAL,
        contract_version="pead-preregistration-2026-08-10",
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=PEAD_MIN_FORWARD_DECISION_DATES,
            min_calendar_weeks=PEAD_MIN_FORWARD_CALENDAR_WEEKS,
            derivation=_PEAD_FORWARD_SHADOW_DERIVATION,
        ),
    )


def build_insider_declaration() -> PreregDeclaration:
    return _build(
        trial=INSIDER_SEALED_TRIAL,
        contract_version="insider-purchase-preregistration-2026-08-10",
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=INSIDER_MIN_FORWARD_DECISION_DATES,
            min_calendar_weeks=INSIDER_MIN_FORWARD_CALENDAR_WEEKS,
            derivation=_INSIDER_FORWARD_SHADOW_DERIVATION,
        ),
    )


def _freeze_one(conn: psycopg.Connection[tuple], declaration: PreregDeclaration) -> tuple[dict[str, object], bool]:
    """Freeze one declaration; returns (report, ok). Same outcomes as #2614's script."""

    # ⚠ The full digest payload, not a hand-picked subset (#2631) — same reason
    # as scripts/freeze_2582_schedule13d_declaration.py.
    summary: dict[str, object] = {**declaration.digest_payload, "declaration_sha256": declaration.sha256}
    try:
        declaration_id = freeze_preregistration(conn, declaration)
    except psycopg.errors.UniqueViolation:
        # ⚠ Two situations wearing one error, wanting opposite operator actions:
        # a retry after an uncertain commit is fine; a second DIFFERENT
        # declaration is the fabrication sql/333's UNIQUE constraint stops.
        conn.rollback()
        stored = load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
        if stored is not None and stored.declaration_sha256 == declaration.sha256:
            return {**summary, "outcome": "already_frozen_identical", "declaration_id": stored.declaration_id}, True
        return {
            **summary,
            "outcome": "conflicting_declaration_already_frozen",
            "stored_declaration_sha256": None if stored is None else stored.declaration_sha256,
            "note": "declarations are immutable; different terms mean a new strategy_version",
        }, False
    except PreregDeclarationRefused as refused:
        conn.rollback()
        return {**summary, "outcome": "refused", "refusals": list(refused.refusals)}, False
    conn.commit()
    return {**summary, "outcome": "frozen", "declaration_id": declaration_id}, True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="build and print both declarations without writing either"
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
    declarations = (build_pead_declaration(), build_insider_declaration())
    if args.dry_run:
        report = policy_version_report()
        for declaration in declarations:
            sys.stdout.write(
                json.dumps(
                    {
                        **declaration.digest_payload,
                        "declaration_sha256": declaration.sha256,
                        **report,
                        "outcome": "dry_run",
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        return 0

    # ⚠ ONCE, BEFORE EITHER WRITE. Both declarations carry the same constant, so
    # a per-declaration check would refuse the second having already frozen the
    # first — half a batch under a policy version the guard rejects.
    policy = assert_policy_version_merged(allow_divergence=args.allow_policy_divergence)
    all_ok = True
    with psycopg.connect(settings.database_url) as conn:
        for declaration in declarations:
            report, ok = _freeze_one(conn, declaration)
            all_ok = all_ok and ok
            (sys.stdout if ok else sys.stderr).write(json.dumps({**report, **policy}, sort_keys=True) + "\n")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DECLARED_BY",
    "INSIDER_MIN_FORWARD_CALENDAR_WEEKS",
    "INSIDER_MIN_FORWARD_DECISION_DATES",
    "PEAD_MIN_FORWARD_CALENDAR_WEEKS",
    "PEAD_MIN_FORWARD_DECISION_DATES",
    "PEAD_MIN_FORWARD_EVENTS",
    "build_insider_declaration",
    "build_pead_declaration",
    "main",
]
