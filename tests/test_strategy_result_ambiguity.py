"""#2625 — the frozen §3.4 ambiguity record and the verdict re-derived from it.

Pure-logic throughout: the record is a dataclass, the verdict and the refusals
are pure functions, and none of it needs Postgres. The table's own CHECK
constraints are exercised against the real database in the PR's evidence table,
not here — a fixture cannot prove what a constraint does.
"""

from __future__ import annotations

import math
from dataclasses import replace

import pytest

from app.services.random_entry_cohort import SyntheticControl
from app.services.strategy_result_ambiguity import (
    AMBIGUITY_RULE_VERSION,
    LEGACY_AMBIGUITY_RULE_VERSION,
    AmbiguityRecord,
    ambiguity_promotion_refusals,
    ambiguity_verdict,
    composed_holdout_ambiguity_refusals,
    exact_ambiguity_support_id,
    matched_control_margin,
    record_sha256,
)


def _arms(best: float | None, worst: float | None, threshold: float | None = None) -> AmbiguityRecord:
    return AmbiguityRecord(
        ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
        comparison_basis="arm_sharpes",
        best_case_sharpe=best,
        worst_case_sharpe=worst,
        cohort_gap_threshold=threshold,
    )


_SHARED = AmbiguityRecord(
    ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
    comparison_basis="shared_measurement",
)


def _control(*, strategy: float, threshold: float) -> SyntheticControl:
    return SyntheticControl(
        model_id="permuted-entry-uniform-gap-v1",
        cohort_size=1000,
        root_seed=20260808,
        mean_return_pct=0.0,
        mean_return_ci_low_pct=-0.1,
        mean_return_ci_high_pct=0.1,
        sharpe_percentile=95.0,
        cohort_sharpe_threshold=threshold,
        strategy_sharpe=strategy,
        cohort_return_threshold_pct=1.0,
        strategy_return_pct=2.0,
    )


class TestMatchedControlMargin:
    def test_the_weaker_positive_arm_margin_is_the_shared_threshold(self) -> None:
        best = _control(strategy=0.8, threshold=0.3)
        worst = _control(strategy=0.6, threshold=0.4)
        assert matched_control_margin(best, worst, best_case_sharpe=0.8, worst_case_sharpe=0.6) == pytest.approx(0.2)

    @pytest.mark.parametrize("missing", ["best", "worst"])
    def test_a_missing_control_leaves_the_pair_not_compared(self, missing: str) -> None:
        best = None if missing == "best" else _control(strategy=0.8, threshold=0.3)
        worst = None if missing == "worst" else _control(strategy=0.6, threshold=0.4)
        assert matched_control_margin(best, worst, best_case_sharpe=0.8, worst_case_sharpe=0.6) is None

    @pytest.mark.parametrize("threshold", [0.6, 0.7])
    def test_a_non_positive_arm_margin_leaves_the_pair_not_compared(self, threshold: float) -> None:
        best = _control(strategy=0.8, threshold=0.3)
        worst = _control(strategy=0.6, threshold=threshold)
        assert matched_control_margin(best, worst, best_case_sharpe=0.8, worst_case_sharpe=0.6) is None

    @pytest.mark.parametrize(
        ("field", "changed"),
        [("model_id", "other"), ("cohort_size", 999), ("root_seed", 1), ("sharpe_percentile", 90.0)],
    )
    def test_unlike_control_metadata_is_an_integrity_failure(self, field: str, changed: object) -> None:
        best = _control(strategy=0.8, threshold=0.3)
        worst = replace(_control(strategy=0.6, threshold=0.4), **{field: changed})
        with pytest.raises(ValueError, match="ambiguity controls disagree"):
            matched_control_margin(best, worst, best_case_sharpe=0.8, worst_case_sharpe=0.6)

    def test_a_control_for_another_arm_is_an_integrity_failure(self) -> None:
        with pytest.raises(ValueError, match="do not describe"):
            matched_control_margin(
                _control(strategy=0.7, threshold=0.3),
                _control(strategy=0.6, threshold=0.4),
                best_case_sharpe=0.8,
                worst_case_sharpe=0.6,
            )


class TestRecordValidation:
    """The record refuses states that would make its own verdict meaningless."""

    @pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
    def test_a_non_finite_sharpe_is_refused(self, bad: float) -> None:
        # ⚠ NaN is the one that matters: it compares false against everything,
        # so a NaN pair would silently read as "the arms differ" while carrying
        # no information at all.
        with pytest.raises(ValueError, match="must be finite"):
            _arms(bad, 1.0)
        with pytest.raises(ValueError, match="must be finite"):
            _arms(1.0, bad)

    def test_a_negative_threshold_is_refused(self) -> None:
        # A gap cannot be below zero, and a negative one would report every
        # comparison material rather than being merely lenient.
        with pytest.raises(ValueError, match="cannot be negative"):
            _arms(1.0, 2.0, -0.1)

    def test_a_shared_measurement_carries_no_numbers(self) -> None:
        # Otherwise one verdict has two canonical forms and the hash stops
        # identifying the record.
        with pytest.raises(ValueError, match="carries no arm Sharpes"):
            AmbiguityRecord(
                ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
                comparison_basis="shared_measurement",
                best_case_sharpe=1.0,
            )

    def test_an_empty_rule_version_is_refused(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            AmbiguityRecord(ambiguity_rule_version="", comparison_basis="arm_sharpes")

    def test_an_unknown_basis_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown comparison_basis"):
            AmbiguityRecord(ambiguity_rule_version=AMBIGUITY_RULE_VERSION, comparison_basis="invented")  # type: ignore[arg-type]


class TestHash:
    def test_the_hash_is_stable_across_equal_records(self) -> None:
        assert record_sha256(_arms(0.6, 0.4)) == record_sha256(_arms(0.6, 0.4))

    def test_every_field_moves_the_hash(self) -> None:
        base = record_sha256(_arms(0.6, 0.4, 0.1))
        assert record_sha256(_arms(0.61, 0.4, 0.1)) != base
        assert record_sha256(_arms(0.6, 0.41, 0.1)) != base
        assert record_sha256(_arms(0.6, 0.4, 0.2)) != base
        assert record_sha256(_SHARED) != base

    def test_a_shared_record_does_not_hash_like_an_empty_arm_record(self) -> None:
        # ⚠ The two differ ONLY in the basis, which is exactly the field a
        # Sharpe-only encoding would have dropped.
        assert record_sha256(_SHARED) != record_sha256(_arms(None, None))


class TestVerdict:
    """Every state ``_ambiguity_material_for`` can reach, plus §3.4's threshold."""

    def test_a_shared_measurement_is_never_material(self) -> None:
        assert ambiguity_verdict(_SHARED) is False

    @pytest.mark.parametrize(
        ("best", "worst", "threshold"),
        [
            pytest.param(0.9, 0.1, 0.1, id="complete_arms_a_material_gap_would_beat"),
            pytest.param(0.9, None, None, id="one_arm_absent_not_compared_would_beat"),
        ],
    )
    def test_the_basis_outranks_the_sharpes(
        self, best: float | None, worst: float | None, threshold: float | None
    ) -> None:
        """A ``shared_measurement`` record is ``False`` whatever numbers it
        carries — on a record neither validator would let you build.

        ⚠ OUTCOME PRECEDENCE, NOT READ ORDER. This pins that the basis DECIDES,
        not that it is read first: an implementation computing the gap and
        consulting the basis last would satisfy it. The read-order claim is not
        testable from outside the function and is not made here.

        ⚠ THE STATE IS FORBIDDEN TWICE, which is why it must be forced.
        ``__post_init__`` refuses a ``shared_measurement`` record carrying
        Sharpes (``test_a_shared_measurement_carries_no_numbers``), and so does
        the table's ``strategy_result_ambiguity_shared_carries_no_measurements``
        CHECK, so ``load_result_ambiguity`` cannot surface one either. Written
        the obvious way — ``ambiguity_verdict(_SHARED)`` — this test asserts the
        same expression as the one above it and exercises no ordering at all.
        ``object.__setattr__`` mutates the frozen instance in place without
        re-running ``__post_init__``.

        ⚠⚠ BOTH CASES ARE LOAD-BEARING, and one alone is a false sense of
        coverage. Each is chosen so that a DIFFERENT non-basis branch would
        return a DIFFERENT verdict if it got there first:

        - complete arms, gap 0.8 beyond a 0.1 threshold → the comparison branch
          would say ``True``. Catches the basis check falling to the bottom.
        - one arm absent → the unpriced branch would say ``None``. Catches the
          basis check falling BELOW the missing-Sharpe guard but above the gap
          maths — a one-line move the complete-arm case sails straight through,
          because its Sharpes are both present.

        Blast radius today is zero BY CONSTRUCTION, and that is the property
        being defended rather than a weakness of the test: the record is
        unreachable, so the only way this can regress is an edit to
        ``ambiguity_verdict`` itself, which is ordinary live code that touches
        neither validator. Same reasoning as the NULL-coercion fix on this
        branch.
        """
        forced = AmbiguityRecord(
            ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
            comparison_basis="shared_measurement",
        )
        object.__setattr__(forced, "best_case_sharpe", best)
        object.__setattr__(forced, "worst_case_sharpe", worst)
        object.__setattr__(forced, "cohort_gap_threshold", threshold)

        assert ambiguity_verdict(forced) is False

    def test_equal_arms_prove_a_zero_gap_without_a_threshold(self) -> None:
        assert ambiguity_verdict(_arms(0.5, 0.5)) is False

    def test_unequal_arms_are_not_compared_without_a_threshold(self) -> None:
        # The state every row this runner writes is in: §3.4 needs the random
        # cohort's gap and no cohort is attached, so the honest verdict is
        # "not compared" and the gate stays closed.
        assert ambiguity_verdict(_arms(0.6, 0.4)) is None

    @pytest.mark.parametrize("pair", [(None, 0.4), (0.6, None), (None, None)])
    def test_an_unpriced_arm_is_not_compared(self, pair: tuple[float | None, float | None]) -> None:
        assert ambiguity_verdict(_arms(*pair)) is None

    def test_a_gap_inside_the_threshold_is_not_material(self) -> None:
        assert ambiguity_verdict(_arms(0.6, 0.4, 0.5)) is False

    def test_a_gap_beyond_the_threshold_is_material(self) -> None:
        assert ambiguity_verdict(_arms(0.6, 0.4, 0.1)) is True

    def test_the_comparison_is_strict_at_the_boundary(self) -> None:
        """§3.4 says "differ by MORE than", so a gap exactly equal to the
        threshold is not material.

        ⚠⚠ THE VALUES ARE BINARY-EXACT ON PURPOSE, and the obvious ones are
        not. This assertion was first written as ``_arms(0.6, 0.4, 0.2)``, which
        proves nothing: ``abs(0.6 - 0.4)`` is ``0.19999999999999996``, so the
        gap never reaches the threshold and BOTH ``>`` and ``>=`` return False.
        A revert-probe flipping the operator left the test passing. 0.5, 0.25
        and 0.25 are exactly representable, so the comparison really is made at
        equality.
        """
        assert abs(0.5 - 0.25) == 0.25, "the boundary values must be binary-exact or this test is vacuous"
        assert ambiguity_verdict(_arms(0.5, 0.25, 0.25)) is False

    def test_the_gap_is_absolute(self) -> None:
        # Which arm is larger is not a §3.4 question; the two orderings must
        # give one verdict.
        assert ambiguity_verdict(_arms(0.4, 0.6, 0.1)) is ambiguity_verdict(_arms(0.6, 0.4, 0.1))


class TestRefusals:
    """Four distinguishable states, four distinct outcomes."""

    def test_an_absent_record_refuses_unrecorded(self) -> None:
        assert ambiguity_promotion_refusals(None) == ("ambiguity_verdict_unrecorded",)

    def test_an_uncompared_record_refuses_not_compared(self) -> None:
        assert ambiguity_promotion_refusals(_arms(0.6, 0.4)) == ("ambiguity_arms_not_compared",)

    def test_a_material_record_refuses_material(self) -> None:
        assert ambiguity_promotion_refusals(_arms(0.6, 0.4, 0.1)) == ("ambiguity_material",)

    def test_an_immaterial_record_refuses_nothing(self) -> None:
        assert ambiguity_promotion_refusals(_arms(0.6, 0.4, 0.5)) == ()
        assert ambiguity_promotion_refusals(_SHARED) == ()

    def test_absent_and_not_compared_are_different_refusals(self) -> None:
        # ⚠⚠ THE ASYMMETRY IS THE ASSERTION. "no record" and "measured but
        # unjudged" are different states, and collapsing them is how a row
        # nobody measured passes as a row that was measured and found fine —
        # the same collapse `check_promotable` refuses between "not measured"
        # and "measured and bad".
        assert ambiguity_promotion_refusals(None) != ambiguity_promotion_refusals(_arms(0.6, 0.4))

    def test_an_unrecognised_rule_version_refuses(self) -> None:
        # Fail closed is an allowlist: a record frozen under a future §3.4 is
        # refused, never reinterpreted under today's semantics.
        stale = AmbiguityRecord(
            ambiguity_rule_version="ambiguity-verdict-2099-v9",
            comparison_basis="arm_sharpes",
            best_case_sharpe=0.6,
            worst_case_sharpe=0.4,
            cohort_gap_threshold=0.5,
        )
        assert "ambiguity_rule_unrecognised" in ambiguity_promotion_refusals(stale)

    def test_the_pre_threshold_rule_is_no_longer_replayed_as_current(self) -> None:
        legacy = AmbiguityRecord(
            ambiguity_rule_version=LEGACY_AMBIGUITY_RULE_VERSION,
            comparison_basis="shared_measurement",
        )
        assert ambiguity_promotion_refusals(legacy) == ("ambiguity_rule_unrecognised",)

    def test_an_unrecognised_version_still_reports_the_verdict_clause(self) -> None:
        # ALL refusals, not the first — the `check_promotable` contract.
        stale = AmbiguityRecord(
            ambiguity_rule_version="ambiguity-verdict-2099-v9",
            comparison_basis="arm_sharpes",
            best_case_sharpe=0.6,
            worst_case_sharpe=0.4,
        )
        assert set(ambiguity_promotion_refusals(stale)) == {
            "ambiguity_rule_unrecognised",
            "ambiguity_arms_not_compared",
        }


class TestHoldoutComposition:
    """#2749 — only a measured-but-unjudged holdout may use exact support."""

    def test_exact_immaterial_support_closes_the_uncompared_holdout_clause(self) -> None:
        assert composed_holdout_ambiguity_refusals(_arms(0.7, 0.4), _arms(0.7, 0.4, 0.5)) == ()

    def test_exact_material_support_blocks_the_holdout(self) -> None:
        assert composed_holdout_ambiguity_refusals(_arms(0.7, 0.4), _arms(0.7, 0.4, 0.1)) == ("ambiguity_material",)

    def test_missing_support_leaves_the_local_not_compared_refusal(self) -> None:
        assert composed_holdout_ambiguity_refusals(_arms(0.7, 0.4), None) == ("ambiguity_arms_not_compared",)

    @pytest.mark.parametrize(
        ("local", "expected"),
        [
            (None, ("ambiguity_verdict_unrecorded",)),
            (_SHARED, ()),
            (_arms(0.7, 0.4, 0.1), ("ambiguity_material",)),
        ],
    )
    def test_support_cannot_override_an_authoritative_local_state(
        self,
        local: AmbiguityRecord | None,
        expected: tuple[str, ...],
    ) -> None:
        favourable = _arms(0.7, 0.4, 0.5)
        assert composed_holdout_ambiguity_refusals(local, favourable) == expected

    def test_an_unrecognised_support_rule_remains_fail_closed(self) -> None:
        stale = replace(_arms(0.7, 0.4, 0.5), ambiguity_rule_version="ambiguity-verdict-2099-v9")
        assert composed_holdout_ambiguity_refusals(_arms(0.7, 0.4), stale) == ("ambiguity_rule_unrecognised",)

    @pytest.mark.parametrize(
        ("candidate_count", "support_id"),
        [(0, None), (0, 41), (1, None), (2, None), (2, 41)],
    )
    def test_a_favourable_id_cannot_be_selected_without_exactly_one_candidate(
        self,
        candidate_count: int,
        support_id: int | None,
    ) -> None:
        assert exact_ambiguity_support_id(candidate_count, support_id) is None

    def test_exactly_one_candidate_and_one_id_is_accepted(self) -> None:
        assert exact_ambiguity_support_id(1, 41) == 41
