"""S-H arm 1's register entry and preregistration declaration (#2840 step 2).

Contract: ``docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md``.

Pure tier, no DB. Everything asserted here is a property of the declaration the
freeze script WOULD write, checked before it writes it — ``sql/333`` bars UPDATE
and DELETE, so a wrong row is only recoverable by minting a new strategy version
and charging the shared trial register twice.
"""

from __future__ import annotations

from pathlib import Path

from app.services.cost_model import CARRY_UNMODELLED, FX_UNMODELLED
from app.services.prereg_contract import PREREG_PURPOSES, declaration_refusals
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import structural_promotion_refusals
from app.services.strategy_result_identity import BACKTEST_UNIVERSE, COST_MODEL_ID
from app.services.strategy_signal_scan import SCAN_UNIVERSE
from app.services.trial_register import TRIAL_REGISTER, TrialExactness
from scripts.freeze_2840_sh_regime_gate_declaration import (
    CONTRACT_VERSION,
    MIN_FORWARD_CALENDAR_WEEKS,
    MIN_FORWARD_DECISION_DATES,
    STRATEGY_ID,
    STRATEGY_VERSION,
    build_declaration,
)

TRIAL_ID = "sh-volatile-regime-gate-2026-08-22"

#: The regime-series priors the floor is built from, restated here so the test
#: fails if the script's arithmetic is edited without the derivation moving.
WINDOW_BEAR_VOLATILE_DATES = 14
CHAIN_BEAR_VOLATILE_DAYS = 155
CHAIN_VOLATILE_DAYS = CHAIN_BEAR_VOLATILE_DAYS + 150


class TestTheRegisterEntry:
    def test_the_trial_is_declared_before_any_outcome_is_opened(self) -> None:
        trial = next(trial for trial in TRIAL_REGISTER.trials if trial.trial_id == TRIAL_ID)
        assert trial.exactness is TrialExactness.EXACT
        # ⚠ 1, not 4. The four readout cells are a fragility screen the pass bar
        # requires jointly, so no favourable cell can be selected between.
        assert trial.searches == 1
        assert "2026-08-22-sh-volatile-regime-gated-breakout.md" in trial.evidence

    def test_the_trial_claims_the_declaration_the_freeze_script_writes(self) -> None:
        """``freeze_preregistration`` refuses a declaration no trial claims."""
        trial = next(trial for trial in TRIAL_REGISTER.trials if trial.trial_id == TRIAL_ID)
        assert trial.declared_for == (STRATEGY_ID, STRATEGY_VERSION)

    def test_the_declared_identity_is_the_research_corpus_one(self) -> None:
        """⚠⚠ The two universes are two trials, and only one is being declared.

        Pasting the wrong hash would freeze a declaration for the scan universe,
        which is not what the exploration measures — and nothing downstream would
        report the mismatch, because the row would simply never be loaded.
        """
        assert BACKTEST_UNIVERSE == "survivorship_free"
        assert SCAN_UNIVERSE == "survivor_only"
        entry = STRATEGY_MANIFEST[STRATEGY_ID]
        scan_version = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        assert STRATEGY_VERSION != scan_version
        assert STRATEGY_VERSION == entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version


class TestTheDeclaration:
    def test_the_carry_and_fx_stamps_are_read_from_the_cost_model(self) -> None:
        """⚠⚠ The draft declared ``(True, True)`` off the cost model's NAME.

        ``carry-fx-structural-zero`` reads as "not modelled" and means the
        opposite — ``structural_zero`` is a closure state saying the cost does
        not exist for this lane. ``freeze_preregistration`` refuses a manifest
        strategy whose stamps cannot match what ``backtest_run`` writes, which
        is what caught it before a row was burned.
        """
        declaration = build_declaration()
        assert (declaration.declared_carry_unmodelled, declaration.declared_fx_unmodelled) == (
            CARRY_UNMODELLED,
            FX_UNMODELLED,
        )
        assert (CARRY_UNMODELLED, FX_UNMODELLED) == (False, False)

    def test_falsification_only_is_a_choice_and_the_test_says_which(self) -> None:
        """⚠ NOT forced. With the real stamps the refusal list is EMPTY, so
        ``ineligible_trial_not_declared_falsification`` does not fire and
        ``capital_candidate`` would be accepted.

        It is declared anyway because no stored corpus window can confirm this
        hypothesis — every pinned window contains the cohorts that generated it
        — so the authorised run can only kill the candidate.
        """
        declaration = build_declaration()
        assert declaration.prereg_purpose in PREREG_PURPOSES
        assert declaration.expected_structural_refusals == ()
        assert declaration.prereg_purpose == "falsification_only"

    def test_the_declaration_would_pass_the_freeze_time_stamp_gate(self) -> None:
        """The gate that refused the draft, exercised without touching the DB."""
        assert declaration_refusals(build_declaration()) == ()

    def test_the_expected_refusals_are_recomputed_not_transcribed(self) -> None:
        declaration = build_declaration()
        assert declaration.expected_structural_refusals == structural_promotion_refusals(
            universe_basis=declaration.declared_universe_basis,
            carry_unmodelled=declaration.declared_carry_unmodelled,
            fx_unmodelled=declaration.declared_fx_unmodelled,
        )

    def test_the_contract_version_names_a_spec_that_exists(self) -> None:
        """The contract version names the doc, and the doc is the frozen readout."""
        assert CONTRACT_VERSION == "sh-volatile-regime-gate-2026-08-22"
        spec = Path("docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md")
        assert spec.is_file()
        assert "Readout and abort bar" in spec.read_text(encoding="utf-8")

    def test_the_digest_is_stable_across_builds(self) -> None:
        assert build_declaration().sha256 == build_declaration().sha256


class TestTheForwardShadowFloor:
    def test_the_floor_supplies_at_least_the_window_s_own_bear_volatile_dates(self) -> None:
        """⚠ The pass leg is bear_volatile, so THAT is what has to accumulate.

        S-11 fires in both volatile regimes, so a total-date floor only clears the
        bear leg if the chain's mix carries it there.
        """
        expected_bear = MIN_FORWARD_DECISION_DATES * CHAIN_BEAR_VOLATILE_DAYS / CHAIN_VOLATILE_DAYS
        assert expected_bear >= WINDOW_BEAR_VOLATILE_DATES

    def test_the_floor_is_denominated_in_dates_not_trades(self) -> None:
        """⚠⚠ 508 trades were 14 decision dates across 485 instruments.

        A floor set from the trade count would be inflated by same-day fan-out —
        the failure ``ForwardShadowFloor``'s own docstring names.
        """
        assert MIN_FORWARD_DECISION_DATES == 28
        assert MIN_FORWARD_DECISION_DATES < 508

    def test_the_calendar_leg_is_derived_from_the_chain_s_own_span(self) -> None:
        assert MIN_FORWARD_CALENDAR_WEEKS == 161

    def test_the_derivation_fits_the_column_and_states_its_limits(self) -> None:
        floor = build_declaration().forward_shadow
        # sql/333 caps the column at 1000 characters.
        assert len(floor.derivation) <= 1000
        assert "NOT a power calculation" in floor.derivation
        assert str(MIN_FORWARD_DECISION_DATES) in floor.derivation
        assert str(MIN_FORWARD_CALENDAR_WEEKS) in floor.derivation
