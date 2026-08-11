"""Phase 5e-3 — criterion 6's declared trial count (#2240)."""

from __future__ import annotations

import pytest

from app.services.trial_register import (
    TRIAL_REGISTER,
    TRIAL_REGISTER_VERSION,
    DeclaredTrial,
    TrialRegister,
)


def _trial(trial_id: str) -> DeclaredTrial:
    return DeclaredTrial(trial_id=trial_id, description="d", evidence="e")


class TestTheShippedDeclaration:
    def test_the_register_is_stamped_with_its_version(self) -> None:
        assert TRIAL_REGISTER.version == TRIAL_REGISTER_VERSION

    def test_every_declared_trial_carries_its_evidence(self) -> None:
        """⚠ An entry nobody can trace is indistinguishable from one invented."""
        for trial in TRIAL_REGISTER.trials:
            assert trial.evidence
            assert trial.description

    def test_the_count_exceeds_the_shipped_strategies(self) -> None:
        """⚠⚠ CRITERION 6 NAMES THE WRONG ANSWER EXPLICITLY.

        An undeclared trial count *"does not default to the number of shipped
        strategies"*. Four strategies ship; a register that counted only those
        would be the exact failure the criterion describes, so this asserts the
        register is strictly larger rather than merely non-empty.
        """
        # ⚠ Named explicitly rather than matched by prefix: a future trial
        # called "sma-crossover" would silently join a `startswith("s")` set and
        # make this assertion pass for the wrong reason.
        shipped = {
            "s1-time-series-momentum",
            "s2-cross-sectional-momentum",
            "s3-mean-reversion-in-trend",
            "s4-volatility-compression-breakout",
        }
        assert shipped <= TRIAL_REGISTER.trial_ids
        assert TRIAL_REGISTER.declared_count > len(shipped)

    def test_the_known_2026_08_09_search_floor_is_counted(self) -> None:
        """The documented 101-arm session must not collapse to one declaration."""
        family = next(
            trial for trial in TRIAL_REGISTER.trials if trial.trial_id == "short-horizon-search-session-2026-08-09"
        )
        assert family.searches == 101
        assert TRIAL_REGISTER.declared_count == 114

    def test_the_discarded_arms_are_counted(self) -> None:
        """A rejected result is still a search of the data.

        #2260's arms were run and their conclusion withdrawn. Dropping them
        would undercount M, which raises the DSR — the flattering direction.
        """
        assert any(trial.startswith("rsi30") for trial in TRIAL_REGISTER.trial_ids)

    def test_the_inconclusive_pead_trial_is_counted(self) -> None:
        assert "pead-historical-sue-net-income-v1" in TRIAL_REGISTER.trial_ids

    def test_the_rejected_insider_purchase_trial_is_counted(self) -> None:
        assert "form4-code-p-opportunistic-purchase-v1" in TRIAL_REGISTER.trial_ids

    def test_designed_but_unevaluated_rules_are_absent(self) -> None:
        """⚠ S-5 and S-6 are specified and blocked on #2279, never run.

        A rule that never touched price data cannot have produced a chance
        winner, so counting it would inflate M and understate the DSR. The
        register's header states this test; this pins it.
        """
        assert not any(trial.startswith(("s5", "s6")) for trial in TRIAL_REGISTER.trial_ids)


class TestSharpeVariance:
    def test_two_measured_trials_give_a_sample_variance(self) -> None:
        register = TrialRegister(version="v", trials=(_trial("a"), _trial("b"), _trial("c")))
        # ⚠ ddof=1 — the trials run are a SAMPLE of the trials that could have
        # been. Computed here from the definition, not read back from us.
        assert register.sharpe_variance({"a": 0.1, "b": 0.3}) == pytest.approx(0.02)

    def test_one_measured_trial_has_no_variance(self) -> None:
        register = TrialRegister(version="v", trials=(_trial("a"), _trial("b")))
        assert register.sharpe_variance({"a": 0.1}) is None

    def test_no_measured_trials_has_no_variance(self) -> None:
        register = TrialRegister(version="v", trials=(_trial("a"),))
        assert register.sharpe_variance({}) is None

    def test_an_undeclared_measured_trial_is_refused(self) -> None:
        """⚠⚠ THE CHECK THAT KEEPS M HONEST.

        A trial we measured but never declared is a trial missing from the
        count. Skipping the key silently would hide precisely the under-count
        criterion 6 calls decorative.
        """
        register = TrialRegister(version="v", trials=(_trial("a"), _trial("b")))
        with pytest.raises(ValueError, match="undeclared trials"):
            register.sharpe_variance({"a": 0.1, "ghost": 0.2})


class TestRegisterInvariants:
    def test_duplicate_trial_ids_are_refused(self) -> None:
        with pytest.raises(ValueError, match="not distinct"):
            TrialRegister(version="v", trials=(_trial("a"), _trial("a")))

    def test_a_blank_version_is_refused(self) -> None:
        with pytest.raises(ValueError, match="version is blank"):
            TrialRegister(version="", trials=())

    @pytest.mark.parametrize("field", ["trial_id", "description", "evidence"])
    def test_a_blank_field_is_refused(self, field: str) -> None:
        kwargs = {"trial_id": "a", "description": "d", "evidence": "e", field: ""}
        with pytest.raises(ValueError, match="is blank"):
            DeclaredTrial(**kwargs)  # type: ignore[arg-type]

    @pytest.mark.parametrize("searches", [0, -1, 1.5, True])
    def test_an_invalid_search_multiplicity_is_refused(self, searches: object) -> None:
        with pytest.raises(ValueError, match="searches must be a positive integer"):
            DeclaredTrial(trial_id="a", description="d", evidence="e", searches=searches)  # type: ignore[arg-type]
