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
from dataclasses import dataclass, fields
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

#: #2634 — the ONLY thing a supersession is permitted to repair. Other reasons
#: to re-declare are exactly the adaptivity supersession exists to forbid, so
#: widening this is a migration (``sql/337`` CHECKs the same value) and a
#: visible act rather than a free-text field nobody reads.
SupersessionReason = Literal["structural_refusal_policy_superseded"]
SUPERSESSION_REASONS: Final[frozenset[str]] = frozenset(get_args(SupersessionReason))

SupersessionRefusal = Literal[
    #: The predecessor already names the current policy version. Nothing is
    #: stranded, so the chain link would be a row nobody needs — and every extra
    #: revision is another thing an auditor has to read.
    "supersession_not_required",
    #: The successor does not name the current policy version. Superseding into
    #: another stale version repairs nothing and hides that it repaired nothing.
    "supersession_policy_not_current",
    #: ⚠ THE SUBSTANTIVE RULE. The declared terms moved. A trial that wants
    #: different terms is a different trial — a new ``strategy_version``.
    "supersession_terms_changed",
    #: No declaration exists for the trial; there is nothing to supersede.
    "supersession_nothing_frozen",
    #: The trial has already been looked at, by the access ledger's reckoning.
    "supersession_trial_already_exposed",
    #: A ``hold_out`` result row exists with or without an access row — exposure
    #: the ledger cannot see, because it predates the chokepoint or bypassed it.
    "supersession_trial_has_holdout_results",
    #: Lost a concurrent race: another supersession linked to the same
    #: predecessor first. The UNIQUE backstop fired rather than a raw driver
    #: error escaping.
    "supersession_predecessor_already_superseded",
]
SUPERSESSION_REFUSALS: Final[frozenset[str]] = frozenset(get_args(SupersessionRefusal))

#: The ONLY declared fields a superseding declaration may change.
#:
#: ⚠⚠ THE PARTITION MUST STAY EXHAUSTIVE OR IT ROTS. A field added to
#: ``PreregDeclaration`` and named in neither half would become silently mutable
#: through supersession — the same defect shape as a digest input nobody added
#: to ``digest_payload`` (#2631). ``tests/test_2634_prereg_supersession.py``
#: compares this set against the dataclass's own fields for that reason.
#:
#: - ``structural_refusal_policy_version`` is the field being repaired;
#: - ``expected_structural_refusals`` is recomputed under the new policy, and
#:   ``expected_structural_refusals_mismatch`` already pins it to the declared
#:   stamps — which terms-identity holds fixed;
#: - ``declared_by`` names whoever re-declared. The original declarer is not
#:   lost: the predecessor row is immutable and still in the chain.
SUPERSESSION_MUTABLE_FIELDS: Final[frozenset[str]] = frozenset(
    {"structural_refusal_policy_version", "expected_structural_refusals", "declared_by"}
)


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
    #: #2363's FX half. ⚠ REQUIRED, not defaulted: a declaration that could not
    #: name FX separately could not pre-declare the refusal set its run will
    #: produce, which is the whole function of freezing one.
    declared_fx_unmodelled: bool
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
            fx_unmodelled=self.declared_fx_unmodelled,
        )

    @property
    def digest_payload(self) -> dict[str, object]:
        """Exactly what ``sha256`` hashes — every declared field, nothing else.

        ⚠ A PROPERTY RATHER THAN AN INLINE DICT SO THE FREEZE SCRIPTS CAN PRINT
        IT (#2631). Their ``--dry-run`` is the operator's only look at an
        irreversible write, and it used to hand-list a SUBSET: the policy
        version — the one field that decides whether the freeze is still valid
        tomorrow — was absent from both. Printing this payload makes the
        dry-run's coverage a consequence of the digest's rather than a second
        list to maintain.

        ⚠ That is not self-enforcing on its own: a field added to the dataclass
        and forgotten here would be missing from the digest AND the dry-run
        together. ``tests/test_2631_freeze_policy_guard.py`` compares the
        dataclass's fields against these keys for that reason.

        A fresh dict each call — a cached one a caller mutated would make the
        printed payload and the hashed payload disagree.
        """
        return {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "contract_version": self.contract_version,
            "prereg_purpose": self.prereg_purpose,
            "structural_refusal_policy_version": self.structural_refusal_policy_version,
            "declared_universe_basis": self.declared_universe_basis,
            "declared_carry_unmodelled": self.declared_carry_unmodelled,
            "declared_fx_unmodelled": self.declared_fx_unmodelled,
            "expected_structural_refusals": sorted(self.expected_structural_refusals),
            "min_independent_decision_dates": self.forward_shadow.min_independent_decision_dates,
            "min_calendar_weeks": self.forward_shadow.min_calendar_weeks,
            "forward_shadow_derivation": self.forward_shadow.derivation,
            "declared_by": self.declared_by,
        }

    @property
    def sha256(self) -> str:
        """Digest over canonical JSON of the declared fields.

        ⚠ Canonical means ``sort_keys`` and compact separators, and the refusal
        list is SORTED in ``digest_payload`` — so two declarations that differ
        only in key or list order hash the same, which is what stops a
        re-ordered copy reading as a different declaration. Same freezing
        pattern ``scripts/verify_2582_schedule13d_preregistration.py`` uses on a
        file.
        """
        return hashlib.sha256(
            json.dumps(self.digest_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


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


@dataclass(frozen=True)
class Supersession:
    """#2634 — the terms under which one declaration replaces another.

    ⚠ SEPARATE FROM ``PreregDeclaration`` AND DELIBERATELY OUTSIDE THE DIGEST.
    The digest freezes the DECLARED TERMS, and those are byte-identical across a
    chain by construction (``SUPERSESSION_MUTABLE_FIELDS``) — folding the repair
    metadata in would make two revisions of the same terms hash differently and
    turn ``digest_intact`` into a check on the wrong thing. The attestation is
    protected instead by ``sql/333``'s immutability trigger, which bars UPDATE
    and DELETE on the row it is written to.
    """

    reason: SupersessionReason
    #: ⚠ A CLAIM, NOT A PROOF, and the spec says so out loud. A zero count in
    #: ``strategy_holdout_accesses`` is a cheap disqualifier that can never
    #: establish non-access: a direct ``SELECT`` leaves no row, a rolled-back
    #: transaction removes its own, and outcomes may already sit in a signed
    #: artifact, an export, a log or another database. This sentence is what
    #: carries the rest of the weight, and naming a person is the point of it.
    attestation: str

    def __post_init__(self) -> None:
        if self.reason not in SUPERSESSION_REASONS:
            raise ValueError(
                f"unknown supersession reason {self.reason!r}; must be one of {sorted(SUPERSESSION_REASONS)}"
            )
        # ⚠ ``strip``, not truthiness: an attestation of three spaces is
        # non-empty and says nothing. ``sql/337`` CHECKs ``btrim`` for the same
        # reason — this one names the field, that one binds a writer that
        # bypasses this class.
        if not self.attestation.strip():
            raise ValueError("supersession_attestation must be a non-empty statement of no exposure")
        if len(self.attestation) > 2000:
            raise ValueError("supersession_attestation is longer than the 2000 characters sql/337 permits")


def supersession_refusals(
    predecessor: PreregDeclaration, successor: PreregDeclaration
) -> tuple[SupersessionRefusal | DeclarationRefusal, ...]:
    """Every reason this re-declaration may not replace that one. Pure.

    Returns ALL refusals rather than the first, and empty means the pair is a
    legal supersession — subject to the database-side checks
    (``supersession_nothing_frozen``, exposure) that ``result_ledger`` owns
    because they read rows.

    ⚠⚠ TERMS-IDENTITY IS THE WHOLE SAFETY ARGUMENT. The worry a re-declaration
    path raises is an author who has seen sample counts, missingness or corpus
    composition re-declaring more favourably. Under this rule there is nothing
    to re-declare: purpose, stamps and both floors are exactly the
    predecessor's, so a supersession can repair the policy-version string and
    can express nothing else. What wants different terms is a different trial.

    ⚠ The successor's own coherence is checked here too, so a successor that is
    malformed in its own right fails naming why rather than being accepted into
    a chain and refused at the next look.
    """
    refusals: list[SupersessionRefusal | DeclarationRefusal] = []

    if predecessor.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION:
        refusals.append("supersession_not_required")
    if successor.structural_refusal_policy_version != STRUCTURAL_REFUSAL_POLICY_VERSION:
        refusals.append("supersession_policy_not_current")

    if _changed_terms(predecessor, successor):
        refusals.append("supersession_terms_changed")

    refusals.extend(declaration_refusals(successor))
    return tuple(refusals)


def changed_supersession_terms(predecessor: PreregDeclaration, successor: PreregDeclaration) -> tuple[str, ...]:
    """The invariant fields that differ, for the refusal's message.

    Public because "the terms changed" with no field name gives an operator
    nothing to act on, and the code vocabulary is closed — so the detail travels
    in the exception text instead of as five more codes that all mean *mint a
    new strategy_version*.
    """
    return _changed_terms(predecessor, successor)


def _changed_terms(predecessor: PreregDeclaration, successor: PreregDeclaration) -> tuple[str, ...]:
    return tuple(
        field.name
        for field in fields(PreregDeclaration)
        if field.name not in SUPERSESSION_MUTABLE_FIELDS
        and getattr(predecessor, field.name) != getattr(successor, field.name)
    )


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
    "SUPERSESSION_MUTABLE_FIELDS",
    "SUPERSESSION_REASONS",
    "SUPERSESSION_REFUSALS",
    "DeclarationRefusal",
    "ForwardShadowFloor",
    "PreregDeclaration",
    "PreregPurpose",
    "Supersession",
    "SupersessionReason",
    "SupersessionRefusal",
    "changed_supersession_terms",
    "declaration_refusals",
    "is_coherent",
    "supersession_refusals",
]
