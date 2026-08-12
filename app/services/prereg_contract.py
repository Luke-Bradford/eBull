"""#2599 — the preregistration declaration and its coherence rules.

Spec: ``docs/proposals/ta/2026-08-12-preregistration-declaration-gate.md``.
Storage: ``sql/333``. Enforcement: ``app/services/result_ledger.py``
(``record_holdout_access`` / ``require_outcome_access``) and
``app/services/strategy_live_gate.py`` (the forward-shadow floor).

⚠⚠ WHAT THIS CLOSES, IN ONE SENTENCE
---------------------------------------------------------------------------
The runtime funding gate already refuses capital to a survivor-only,
carry-unmodelled result (``strategy_paper_executor.py:283,294``). The RESEARCH
side had no equivalent, so between 2026-08-10 and 2026-08-12 five sealed trials
opened outcomes that were structurally unpromotable before their first outcome
was read — each charging the shared trial register, raising the deflated-Sharpe
bar for every future candidate at no possible promotion benefit.

A declared falsification run stays legitimate and STILL charges the register;
any look at the data must. What becomes impossible is *incidental* burn: sealing
a trial that cannot promote without anyone having said so at freeze time.

⚠ THIS MODULE IS PURE. It holds the vocabulary and the rules; the frozen row and
every database touch live in ``result_ledger``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Final, Literal, get_args

from app.services.strategy_result import (
    STRUCTURAL_REFUSAL_POLICY_VERSION,
    PromotionRefusal,
    structural_promotion_refusals,
)

PreregPurpose = Literal["capital_candidate", "falsification_only"]
PREREG_PURPOSES: Final[frozenset[str]] = frozenset(get_args(PreregPurpose))

#: ⚠ SEPARATE FROM ``StrategyResult.purpose``, which is a closed two-value
#: vocabulary about what a RESULT is (``harness_validation`` /
#: ``capital_candidate``) and is deliberately NOT extended. This one is about
#: what a TRIAL declared itself to be before it ran. The overlap in the
#: ``capital_candidate`` spelling is real and intentional: a trial that declares
#: itself a capital candidate is the only one whose results may claim to be one.

DeclarationRefusal = Literal[
    #: The frozen expectation was computed under a policy version that is no
    #: longer current. ⚠ Refused rather than re-interpreted, which is the shape
    #: `trial_register_superseded` already sets: a frozen artefact means what it
    #: meant under its own policy, and silently re-reading it under a new one is
    #: how a stale expectation becomes an approval.
    "structural_refusal_policy_superseded",
    #: The declared list disagrees with the list recomputed from the declared
    #: stamps. Either the stamps or the list was edited; both are the writer's
    #: bug and neither is safe to guess at.
    "expected_structural_refusals_mismatch",
    #: The substantive rule. ⚠ READS THE RECOMPUTED LIST, NEVER THE DECLARED
    #: ONE — see `declaration_refusals`.
    "ineligible_trial_not_declared_falsification",
    "forward_shadow_floor_not_positive",
    "forward_shadow_derivation_missing",
]
DECLARATION_REFUSALS: Final[frozenset[str]] = frozenset(get_args(DeclarationRefusal))


@dataclass(frozen=True)
class ForwardShadowFloor:
    """#2437's promotion-contract floor: N decision-dates AND M calendar weeks.

    ⚠ NO DEFAULTS, AND NO VALUE IS CHOSEN ANYWHERE IN THIS MODULE. Both numbers
    are frozen from the candidate's own power calculation and arrive with the
    ``derivation`` that names it. Picking a central default would be the #2600
    padded-floor defect verbatim — a floor no artefact evidences.

    ⚠ DECISION *DATES*, NOT SIGNALS. The live gate's existing
    ``min_forward_resolved_signals`` cannot tell twenty signals on one day from
    twenty days of evidence. The narrow claim made here is that a distinct-date
    count cannot be inflated by same-day fan-out — NOT that the dates are
    statistically independent of each other, which they are not, and which is
    what stage 5e-2's block bootstrap exists to handle.
    """

    min_independent_decision_dates: int
    min_calendar_weeks: int
    derivation: str


@dataclass(frozen=True)
class PreregDeclaration:
    """One frozen preregistration declaration. Mirrors ``sql/333``.

    ⚠ THIS IS A READ-BACK OF A STORED ROW, NOT AN ARGUMENT TO A GATE. The first
    draft of #2599 passed one of these into the access helper; Codex checkpoint
    1 killed it — *"a caller can construct a favourable declaration after
    seeing/reading outcomes"*. Constructing one is how you FREEZE a declaration;
    every enforcement path loads it from the table by trial identity.

    ⚠ Validation MIRRORS ``sql/333``'s CHECKs rather than deferring to them —
    the same deliberate duplication ``HoldoutAccess`` documents. A malformed
    declaration fails here naming the field, and the constraints stay as the
    backstop for any writer that bypasses this class.
    """

    strategy_id: str
    strategy_version: str
    contract_version: str
    prereg_purpose: PreregPurpose
    structural_refusal_policy_version: str
    declared_universe_basis: str
    declared_carry_unmodelled: bool
    expected_structural_refusals: tuple[str, ...]
    forward_shadow: ForwardShadowFloor
    declared_by: str

    def __post_init__(self) -> None:
        if self.prereg_purpose not in PREREG_PURPOSES:
            raise ValueError(
                f"unknown prereg_purpose {self.prereg_purpose!r}; must be one of {sorted(PREREG_PURPOSES)}"
            )
        for field_name in (
            "strategy_id",
            "strategy_version",
            "contract_version",
            "structural_refusal_policy_version",
            "declared_universe_basis",
            "declared_by",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} must be non-empty on a preregistration declaration")
        unknown = set(self.expected_structural_refusals) - set(get_args(PromotionRefusal))
        if unknown:
            raise ValueError(
                f"expected_structural_refusals names codes outside the promotion vocabulary: {sorted(unknown)}"
            )
        if len(set(self.expected_structural_refusals)) != len(self.expected_structural_refusals):
            raise ValueError("expected_structural_refusals contains a duplicate code")

    @property
    def recomputed_structural_refusals(self) -> tuple[PromotionRefusal, ...]:
        """What the declared stamps produce under TODAY's policy."""
        return structural_promotion_refusals(
            universe_basis=self.declared_universe_basis,
            carry_unmodelled=self.declared_carry_unmodelled,
        )

    @property
    def sha256(self) -> str:
        """Digest over canonical JSON of the declared fields.

        ⚠ Canonical means ``sort_keys`` and compact separators, and the refusal
        list is SORTED here — so two declarations that differ only in key or
        list order hash the same, which is what stops a re-ordered copy reading
        as a different declaration. Same freezing pattern
        ``scripts/verify_2582_schedule13d_preregistration.py`` uses on a file.
        """
        payload = {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "contract_version": self.contract_version,
            "prereg_purpose": self.prereg_purpose,
            "structural_refusal_policy_version": self.structural_refusal_policy_version,
            "declared_universe_basis": self.declared_universe_basis,
            "declared_carry_unmodelled": self.declared_carry_unmodelled,
            "expected_structural_refusals": sorted(self.expected_structural_refusals),
            "min_independent_decision_dates": self.forward_shadow.min_independent_decision_dates,
            "min_calendar_weeks": self.forward_shadow.min_calendar_weeks,
            "forward_shadow_derivation": self.forward_shadow.derivation,
            "declared_by": self.declared_by,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def declaration_refusals(declaration: PreregDeclaration) -> tuple[DeclarationRefusal, ...]:
    """Every reason this declaration may not authorise an outcome look.

    Pure, returns ALL refusals rather than the first, and empty means coherent.

    ⚠ "Coherent" is not "promotable". A ``falsification_only`` declaration over
    survivor-only stamps returns EMPTY here and is meant to: it is a legitimate
    trial that said out loud what it was. The promotion gate still refuses its
    results, which is the whole arrangement.
    """
    refusals: list[DeclarationRefusal] = []

    if declaration.structural_refusal_policy_version != STRUCTURAL_REFUSAL_POLICY_VERSION:
        refusals.append("structural_refusal_policy_superseded")

    recomputed = declaration.recomputed_structural_refusals
    if sorted(declaration.expected_structural_refusals) != sorted(recomputed):
        refusals.append("expected_structural_refusals_mismatch")

    # ⚠ RECOMPUTED, NOT DECLARED. Reading the declared list here would let a
    # writer that lied by declaring `[]` on survivor-only stamps downgrade a
    # purpose violation into a bare mismatch — the substantive refusal would
    # vanish exactly in the case it exists for.
    if recomputed and declaration.prereg_purpose == "capital_candidate":
        refusals.append("ineligible_trial_not_declared_falsification")

    floor = declaration.forward_shadow
    if floor.min_independent_decision_dates <= 0 or floor.min_calendar_weeks <= 0:
        refusals.append("forward_shadow_floor_not_positive")
    if not floor.derivation.strip():
        refusals.append("forward_shadow_derivation_missing")

    return tuple(refusals)


def is_coherent(declaration: PreregDeclaration) -> bool:
    """``declaration_refusals`` with the reasons discarded.

    Named so the reason-losing is visible at the call site: an enforcement path
    must use ``declaration_refusals``, because "refused" with no code gives an
    operator nothing to act on.
    """
    return not declaration_refusals(declaration)


__all__ = [
    "DECLARATION_REFUSALS",
    "PREREG_PURPOSES",
    "DeclarationRefusal",
    "ForwardShadowFloor",
    "PreregDeclaration",
    "PreregPurpose",
    "declaration_refusals",
    "is_coherent",
]
