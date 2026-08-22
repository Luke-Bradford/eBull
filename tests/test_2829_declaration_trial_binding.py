"""Bind a preregistration declaration to the trial that counts it (#2829).

`TRIAL_REGISTER` (criterion 6's ``M``, feeding the DSR) and
`strategy_preregistration_declarations` were not joinable, so "search #275" was an
assertion no code could check. These tests pin the join, the invariants that keep it
from inflating ``M``, and the two things the change deliberately does NOT do.

Spec: ``docs/proposals/ta/2026-08-22-declaration-trial-binding.md``
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import freeze_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
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


def _trial(trial_id: str, declared_for: tuple[str, str] | None = None) -> DeclaredTrial:
    return DeclaredTrial(
        trial_id=trial_id,
        description="d",
        evidence="e",
        exactness=TrialExactness.EXACT,
        declared_for=declared_for,
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
        """⚠ Most trials claim nothing, and a ``None`` ``declared_for`` is not an omission.

        Many entries are research SESSIONS that no declaration corresponds to. A
        test that demanded a mapping on every trial would be demanding five rows'
        worth of declarations for thirty trials.
        """
        claiming = [trial for trial in TRIAL_REGISTER.trials if trial.declared_for is not None]
        assert len(claiming) == len(_STORED_DECLARATIONS)
        assert TRIAL_REGISTER.trial_for_declaration("short-horizon-search-session-2026-08-09", "whatever") is None


class TestTheInvariantsThatProtectM:
    def test_a_pair_claimed_by_two_trials_is_refused(self) -> None:
        """One declaration counted twice inflates ``M`` silently — the same
        failure shape as a duplicate ``trial_id``, one level down."""
        pair = ("alpha", "v1")
        with pytest.raises(ValueError, match="claimed by both"):
            TrialRegister(version="v", trials=(_trial("one", pair), _trial("two", pair)))

    def test_one_trial_can_claim_at_most_one_declaration(self) -> None:
        """⚠⚠ THE INVARIANT CODEX CKPT-2 FORCED, and it is structural rather than
        checked.

        The first draft let a trial claim a TUPLE of pairs. A new search could
        then be added to an existing trial's list -- which the freeze gate's own
        error message suggested -- passing the gate while ``searches`` stayed put
        and ``declared_count`` never moved. That is the exact under-count the gate
        exists to prevent, sailing through it.

        One trial to one declaration makes "this declaration has its own counted
        trial" true by the type, so appending a claim necessarily appends a trial
        and necessarily moves ``M``.
        """
        # Behavioural, not a type assertion: a list of pairs is what the first
        # draft accepted, and `(("a", "v1"), ("b", "v2"))` even has len 2, so it
        # slips past a shape check and is caught only because its elements are
        # tuples rather than strings.
        with pytest.raises(ValueError, match="must be a non-blank string"):
            _trial("one", (("alpha", "v1"), ("beta", "v2")))  # type: ignore[arg-type]

    @pytest.mark.parametrize("bad", ["", "   ", "x" * 201])
    def test_an_identity_the_declarations_table_could_not_hold_is_refused(self, bad: str) -> None:
        """Mirrors ``sql/333``'s identity CHECK (1-200 chars, non-blank). A pair
        the table could never hold can never match a row, so it fails SILENT — as
        a declaration this register does not claim — rather than loudly."""
        with pytest.raises(ValueError, match="sql/333"):
            _trial("one", (bad, "v1"))

    def test_a_malformed_pair_is_refused(self) -> None:
        with pytest.raises(ValueError, match="strategy_id, strategy_version"):
            DeclaredTrial(
                trial_id="one",
                description="d",
                evidence="e",
                exactness=TrialExactness.EXACT,
                declared_for=("alpha",),  # type: ignore[arg-type]
            )


class TestTheFreezeGate:
    """⚠ DB-free by design: the check fires before ``freeze_preregistration``
    touches its connection, which is what these lean on — the same property
    ``tests/test_2720_freeze_stamp_validation.py`` documents for the cost-stamp
    check directly above it."""

    @staticmethod
    def _declaration(strategy_id: str, strategy_version: str) -> PreregDeclaration:
        # ⚠ A NON-manifest strategy_id on purpose: the cost-stamp check above
        # this gate is scoped to STRATEGY_MANIFEST, and a manifest id would fail
        # there first and never reach the gate under test.
        return PreregDeclaration(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            contract_version="test-contract-v1",
            prereg_purpose="falsification_only",
            structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
            declared_universe_basis="survivor_only",
            declared_carry_unmodelled=False,
            declared_fx_unmodelled=False,
            expected_structural_refusals=("universe_basis_not_survivorship_free",),
            forward_shadow=ForwardShadowFloor(
                min_independent_decision_dates=40,
                min_calendar_weeks=12,
                derivation="tests/test_2829_declaration_trial_binding.py",
            ),
            declared_by="tests/test_2829_declaration_trial_binding.py",
        )

    def test_a_declaration_no_trial_claims_cannot_be_frozen(self) -> None:
        """The substantive rule: M must already count the search.

        Without it a trial freezes, runs, and is deflated against a population
        that excludes it — and the row is immutable, so the mistake costs a
        supersession to even acknowledge.
        """
        declaration = self._declaration("some-unregistered-trial", "v1")
        with pytest.raises(ValueError, match="does not count the search"):
            freeze_preregistration(cast("Any", object()), declaration)

    def test_the_refusal_names_the_pair_and_where_to_fix_it(self) -> None:
        """A refusal a reader cannot act on sends them to read the gate's source."""
        declaration = self._declaration("some-unregistered-trial", "v1")
        with pytest.raises(ValueError) as excinfo:
            freeze_preregistration(cast("Any", object()), declaration)
        message = str(excinfo.value)
        assert "some-unregistered-trial@v1" in message
        assert "app/services/trial_register.py" in message

    def test_a_claimed_declaration_reaches_the_connection(self) -> None:
        """The positive arm, and it must be present or the test above passes for
        a register that claims NOTHING.

        A bare ``object()`` has no ``execute``, so getting past the gate raises
        ``AttributeError`` — a different failure from the ``ValueError`` the gate
        produces, which is exactly what distinguishes "admitted" from "refused"
        without a database.
        """
        strategy_id, strategy_version, _ = _STORED_DECLARATIONS[0]
        with pytest.raises(AttributeError):
            freeze_preregistration(cast("Any", object()), self._declaration(strategy_id, strategy_version))


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
