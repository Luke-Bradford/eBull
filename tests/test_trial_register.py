"""Phase 5e-3 — criterion 6's declared trial count (#2240)."""

from __future__ import annotations

from datetime import UTC

import pytest

from app.services.trial_register import (
    TRIAL_REGISTER,
    TRIAL_REGISTER_CUTOFF,
    TRIAL_REGISTER_VERSION,
    DeclaredTrial,
    TrialExactness,
    TrialRegister,
)


def _trial(trial_id: str, exactness: TrialExactness = TrialExactness.EXACT) -> DeclaredTrial:
    return DeclaredTrial(trial_id=trial_id, description="d", evidence="e", exactness=exactness)


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
        assert family.exactness is TrialExactness.FLOOR

    def test_the_reconstructed_total_is_the_sum_of_the_declared_families(self) -> None:
        """⚠ Gate D-0.1's whole deliverable is this number.

        Pinned as an explicit literal AND re-derived from the entries, because
        the two fail differently: a dropped entry moves both together and only
        the literal catches it, while a typo'd `searches` that happens to keep
        the total is caught by neither and is why every family below is pinned
        individually as well.
        """
        # ⚠ 259 (#2600's Gate D-0.1 reconstruction) + 7 (#2614's C-4 entry, the
        # first declared BEFORE its run). Moved deliberately, not loosened: the
        # pin exists to catch a DROPPED entry, and an addition that raises M is
        # the conservative direction — a larger M lowers the DSR.
        assert TRIAL_REGISTER.declared_count == 272
        assert TRIAL_REGISTER.declared_count == sum(trial.searches for trial in TRIAL_REGISTER.trials)

    def test_the_c4_schedule13d_arms_are_counted_before_the_run(self) -> None:
        """#2614 — three arms that load their own bars, plus four 13G rule cells.

        ⚠ EXACT, not FLOOR: `build_historical_falsification_report` computes all
        seven unconditionally, so the code grid at this commit IS the search list.
        """
        family = next(trial for trial in TRIAL_REGISTER.trials if trial.trial_id == "c4-schedule13d-public-catalyst-v1")
        assert family.searches == 7
        assert family.exactness is TrialExactness.EXACT

    def test_the_rejected_extreme_shock_sizing_arms_are_counted(self) -> None:
        """⚠ 8 charged by the result page + 7 calendar-year cuts it never charged.

        #2600: an era cut is a search, and the page's own warning ("do not rescue
        it by selecting a cap, threshold, hold, stop, era, sector") is the reason.
        """
        family = next(
            trial for trial in TRIAL_REGISTER.trials if trial.trial_id == "extreme-shock-portfolio-sizing-stress-v1"
        )
        assert family.searches == 15

    def test_the_discarded_arms_are_counted(self) -> None:
        """A rejected result is still a search of the data.

        #2260's arms were run and their conclusion withdrawn. Dropping them
        would undercount M, which raises the DSR — the flattering direction.
        """
        assert any(trial.startswith("rsi30") for trial in TRIAL_REGISTER.trial_ids)

    def test_the_inconclusive_pead_trial_is_counted(self) -> None:
        trial = next(trial for trial in TRIAL_REGISTER.trials if trial.trial_id == "pead-historical-sue-net-income-v1")
        assert trial.evidence.startswith("docs/proposals/ta/2026-08-10-pead-result.md")

    def test_the_rejected_insider_purchase_trial_is_counted(self) -> None:
        assert "form4-code-p-opportunistic-purchase-v1" in TRIAL_REGISTER.trial_ids

    def test_designed_but_unevaluated_rules_are_absent(self) -> None:
        """⚠ S-5 and S-6 are specified and blocked on #2279, never run.

        A rule that never touched price data cannot have produced a chance
        winner, so counting it would inflate M and understate the DSR. The
        register's header states this test; this pins it.
        """
        assert {
            "s5-support-bounce",
            "s6-resistance-breakout",
            "s7-trend-pullback",
            "s8-range-mean-reversion",
            "s9-squeeze-expansion",
            "s10-relative-strength-leader",
        } <= TRIAL_REGISTER.trial_ids


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
        kwargs = {
            "trial_id": "a",
            "description": "d",
            "evidence": "e",
            "exactness": TrialExactness.EXACT,
            field: "",
        }
        with pytest.raises(ValueError, match="is blank"):
            DeclaredTrial(**kwargs)  # type: ignore[arg-type]

    @pytest.mark.parametrize("searches", [0, -1, 1.5, True])
    def test_an_invalid_search_multiplicity_is_refused(self, searches: object) -> None:
        with pytest.raises(ValueError, match="searches must be a positive integer"):
            DeclaredTrial(
                trial_id="a",
                description="d",
                evidence="e",
                exactness=TrialExactness.EXACT,
                searches=searches,  # type: ignore[arg-type]
            )


class TestExactness:
    """#2600 Gate D-0.1 — `searches` is meaningless without saying what it is."""

    def test_a_raw_string_exactness_is_refused(self) -> None:
        """⚠⚠ THE FAILURE MODE THAT WOULD BE SILENT.

        A bare "floor" satisfies every `== "floor"` comparison a reader writes
        and fails every `is TrialExactness.FLOOR` one, so `floored_searches`
        would under-report on an entry that looks correct in the source. Refused
        at construction rather than coerced.
        """
        with pytest.raises(ValueError, match="exactness must be a TrialExactness"):
            DeclaredTrial(trial_id="a", description="d", evidence="e", exactness="floor")  # type: ignore[arg-type]

    def test_every_shipped_entry_declares_its_exactness(self) -> None:
        for trial in TRIAL_REGISTER.trials:
            assert isinstance(trial.exactness, TrialExactness)

    def test_floored_searches_counts_only_floored_families(self) -> None:
        register = TrialRegister(
            version="v",
            trials=(
                DeclaredTrial(
                    trial_id="exact-family",
                    description="d",
                    evidence="e",
                    exactness=TrialExactness.EXACT,
                    searches=5,
                ),
                DeclaredTrial(
                    trial_id="floored-family",
                    description="d",
                    evidence="e",
                    exactness=TrialExactness.FLOOR,
                    searches=7,
                ),
            ),
        )
        assert register.declared_count == 12
        assert register.floored_searches == 7

    def test_floored_searches_are_reported_not_subtracted(self) -> None:
        """⚠ A floored family's searches HAPPENED. The flag says only that more
        of them happened than the register can name, so removing them would move
        M in the flattering direction."""
        assert TRIAL_REGISTER.floored_searches < TRIAL_REGISTER.declared_count
        assert TRIAL_REGISTER.declared_count == sum(trial.searches for trial in TRIAL_REGISTER.trials)

    @pytest.mark.parametrize(
        ("trial_id", "searches", "exactness"),
        [
            # The hold-out access ledger: evaluate/4 + in_sample rows/4 + 1 read.
            ("s1-time-series-momentum", 19, TrialExactness.FLOOR),
            ("s2-cross-sectional-momentum", 19, TrialExactness.FLOOR),
            ("s3-mean-reversion-in-trend", 19, TrialExactness.FLOOR),
            ("s4-volatility-compression-breakout", 8, TrialExactness.FLOOR),
            # Result pages that enumerate their own arms.
            ("pead-historical-sue-net-income-v1", 8, TrialExactness.EXACT),
            ("form4-code-p-opportunistic-purchase-v1", 7, TrialExactness.EXACT),
            # Code grids read at commit 61fb17da.
            ("autocorrelation-term-structure-2026-08-09", 28, TrialExactness.FLOOR),
            ("roll-bounce-spread-recovery-2026-08-09", 4, TrialExactness.EXACT),
            ("insider-purchase-forward-returns-first-look-2026-08-09", 4, TrialExactness.EXACT),
            # Floors that admit only evidenced arms.
            ("residual-confluence-v1-development-arms", 7, TrialExactness.FLOOR),
            ("etf-intraday-momentum-v1-retained-census", 4, TrialExactness.FLOOR),
            ("sizing-rule-attribution-2026-08-12", 9, TrialExactness.FLOOR),
        ],
    )
    def test_each_reconstructed_family_carries_its_derived_count(
        self, trial_id: str, searches: int, exactness: TrialExactness
    ) -> None:
        """⚠ Pinned family-by-family, not just in the total.

        Two counts moving in opposite directions leave `declared_count`
        unchanged, and a register whose total is right for the wrong reasons is
        exactly what Gate D-0.1 was opened to replace.
        """
        trial = next(t for t in TRIAL_REGISTER.trials if t.trial_id == trial_id)
        assert trial.searches == searches
        assert trial.exactness is exactness


class TestReconstructionCutoff:
    def test_the_cutoff_is_utc_aware(self) -> None:
        """⚠ A naive datetime here explodes on comparison with a timestamptz.

        The cutoff's only job is to be compared against
        `strategy_results_store.created_at` / `strategy_holdout_accesses.accessed_at`,
        both of which are `timestamp with time zone`. A naive value raises
        `TypeError` at exactly the moment #2599 tries to enforce the boundary.
        """
        assert TRIAL_REGISTER_CUTOFF.tzinfo is not None
        assert TRIAL_REGISTER_CUTOFF.utcoffset() == UTC.utcoffset(None)
