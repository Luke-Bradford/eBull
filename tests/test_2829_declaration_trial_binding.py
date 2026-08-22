"""Bind a preregistration declaration to the trial that counts it (#2829).

`TRIAL_REGISTER` (criterion 6's ``M``, feeding the DSR) and
`strategy_preregistration_declarations` were not joinable, so "search #275" was an
assertion no code could check. These tests pin the join, the invariants that keep it
from inflating ``M``, and the two things the change deliberately does NOT do.

Spec: ``docs/proposals/ta/2026-08-22-declaration-trial-binding.md``
"""

from __future__ import annotations

import pytest

from app.services.trial_register import (
    TRIAL_REGISTER,
    DeclaredTrial,
    TrialExactness,
    TrialRegister,
)

#: The five rows in ``strategy_preregistration_declarations`` on 2026-08-22, and the
#: measurement that motivated an explicit mapping: the matching ``trial_id`` was
#: ``strategy_version`` twice, ``strategy_id`` twice, and ``strategy_id + "-v1"`` once.
#: ⚠ Three join rules across five rows — there is no convention to infer.
_STORED_DECLARATIONS: tuple[tuple[str, str, str], ...] = (
    ("c4-schedule13d-public-catalyst", "schedule13d-public-catalyst-v1", "c4-schedule13d-public-catalyst-v1"),
    (
        "form4-code-p-opportunistic-purchase",
        "form4-code-p-opportunistic-purchase-v1",
        "form4-code-p-opportunistic-purchase-v1",
    ),
    ("pead-historical-sue-net-income", "pead-historical-sue-net-income-v1", "pead-historical-sue-net-income-v1"),
    (
        "mt1-capped-volatility-managed-relative-strength-v1",
        "strategy-registry-v1+32970feefa00",
        "mt1-capped-volatility-managed-relative-strength-v1",
    ),
    (
        "mt1-s8-capped-volatility-negative-control-v1",
        "strategy-registry-v1+b83c3e4fc997",
        "mt1-s8-capped-volatility-negative-control-v1",
    ),
)


def _trial(trial_id: str, declares: tuple[tuple[str, str], ...] = ()) -> DeclaredTrial:
    return DeclaredTrial(
        trial_id=trial_id,
        description="d",
        evidence="e",
        exactness=TrialExactness.EXACT,
        declares=declares,
    )


class TestTheShippedMapping:
    @pytest.mark.parametrize(("strategy_id", "strategy_version", "trial_id"), _STORED_DECLARATIONS)
    def test_every_stored_declaration_is_claimed_by_its_trial(
        self, strategy_id: str, strategy_version: str, trial_id: str
    ) -> None:
        trial = TRIAL_REGISTER.trial_for_declaration(strategy_id, strategy_version)
        assert trial is not None, f"{strategy_id}@{strategy_version} is claimed by no trial"
        assert trial.trial_id == trial_id

    def test_the_mapping_is_sparse_and_that_is_correct(self) -> None:
        """⚠ Most trials claim nothing, and an empty ``declares`` is not an omission.

        Many entries are research SESSIONS that no declaration corresponds to. A
        test that demanded a mapping on every trial would be demanding five rows'
        worth of declarations for thirty trials.
        """
        claiming = [trial for trial in TRIAL_REGISTER.trials if trial.declares]
        assert len(claiming) == len(_STORED_DECLARATIONS)
        assert TRIAL_REGISTER.trial_for_declaration("short-horizon-search-session-2026-08-09", "whatever") is None


class TestTheInvariantsThatProtectM:
    def test_a_pair_claimed_by_two_trials_is_refused(self) -> None:
        """One declaration counted twice inflates ``M`` silently — the same
        failure shape as a duplicate ``trial_id``, one level down."""
        pair = (("alpha", "v1"),)
        with pytest.raises(ValueError, match="claimed by both"):
            TrialRegister(version="v", trials=(_trial("one", pair), _trial("two", pair)))

    def test_a_pair_repeated_inside_one_trial_is_refused(self) -> None:
        """⚠ The cross-trial check builds a set, so a pair repeated INSIDE one
        tuple would pass it while still leaving the register unable to say how
        many times it counted that declaration."""
        with pytest.raises(ValueError, match="declares .* twice"):
            _trial("one", (("alpha", "v1"), ("alpha", "v1")))

    @pytest.mark.parametrize("bad", ["", "   ", "x" * 201])
    def test_an_identity_the_declarations_table_could_not_hold_is_refused(self, bad: str) -> None:
        """Mirrors ``sql/333``'s identity CHECK (1-200 chars, non-blank). A pair
        the table could never hold can never match a row, so it fails SILENT — as
        a declaration this register does not claim — rather than loudly."""
        with pytest.raises(ValueError, match="sql/333"):
            _trial("one", ((bad, "v1"),))

    def test_a_malformed_pair_is_refused(self) -> None:
        with pytest.raises(ValueError, match="strategy_id, strategy_version"):
            DeclaredTrial(
                trial_id="one",
                description="d",
                evidence="e",
                exactness=TrialExactness.EXACT,
                declares=(("alpha",),),  # type: ignore[arg-type]
            )


class TestWhatThisChangeDeliberatelyDoesNotMove:
    def test_M_is_unchanged_so_the_register_version_is_not_bumped(self) -> None:
        """⚠⚠ THE LOAD-BEARING CALL, pinned as literals on purpose.

        ``TRIAL_REGISTER_VERSION`` is stored beside every DSR to answer "which
        population was this deflated against", and ``strategy_result.py`` refuses
        ``trial_register_superseded`` when a stored version or ``declared_trials``
        disagrees with today's. 220 stored results carry
        ``trial-register-2026-08-15-r7``, so bumping is the DESTRUCTIVE option,
        not the safe one — it would flip all 220 to refused for a change that
        added no trial and moved no ``searches`` value.

        These two literals are what keeps that argument checkable: an edit that
        actually moves ``M`` fails here and forces the bump conversation at the
        moment it is due.
        """
        assert TRIAL_REGISTER.declared_count == 274
        assert len(TRIAL_REGISTER.trials) == 30

    def test_the_declaration_refusal_vocabulary_is_untouched(self) -> None:
        """⚠ The first draft added ``trial_not_in_register`` here. Codex ckpt-1
        killed it: ``declaration_refusals`` is pure and consulted by supersession
        REPAIR and live-gate REASSESSMENT, so a member here would strand already
        frozen artefacts — and ``sql/333``'s header is titled "NO RETROACTIVE
        INVALIDATION". The gate went to freeze time instead.

        Pinned as an exact set so a later author who reaches for the easy
        placement has to read that argument first.
        """
        from app.services.prereg_contract import DECLARATION_REFUSALS

        assert DECLARATION_REFUSALS == frozenset(
            {
                "structural_refusal_policy_superseded",
                "expected_structural_refusals_mismatch",
                "ineligible_trial_not_declared_falsification",
                "forward_shadow_floor_not_positive",
                "forward_shadow_derivation_missing",
            }
        )
