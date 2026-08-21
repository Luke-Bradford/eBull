"""#2770 — the operator promotion path's rules, tested without Postgres.

Every rule that decides whether a promotion may proceed is a pure function here, so
these are fast-tier tests. The DB-backed behaviour (the loader's SQL, the endpoint) is
covered separately; what matters most is that the MATRIX rule and the assessment rule
are table-tested, because those are the two places a cherry-picked denominator or a
stale verdict would get in.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.services.strategy_control_plane import _NEXT_STAGE, Stage
from app.services.strategy_operator_promotion import (
    EXPECTED_ARMS,
    EXPECTED_EVIDENCE_COMBINATIONS,
    AssessmentCandidate,
    EvidenceRow,
    action_target,
    allowed_operator_action,
    evidence_identities,
    evidence_refusal_summary,
    next_operator_action_view,
    operator_targets,
    prospective_assessment_ref,
    recent_evidence_ref,
    recent_evidence_refusals,
    select_latest_rows,
    select_prospective_assessment,
    weakening_refusals,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS

_AS_OF = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


def _complete_matrix() -> list[EvidenceRow]:
    """One row per declared (window, ambiguity, quarantine), ids 1..24."""
    rows: list[EvidenceRow] = []
    result_id = 1
    for window_id in RECENT_EVIDENCE_WINDOWS:
        for ambiguity, quarantine in EXPECTED_ARMS:
            rows.append(EvidenceRow(window_id, ambiguity, quarantine, result_id))
            result_id += 1
    return rows


def _candidate(**overrides: object) -> AssessmentCandidate:
    base: dict[str, object] = {
        "assessment_id": 1,
        "policy_id": "assessment-policy-v1",
        "checked_at": _AS_OF - timedelta(days=1),
        "passed": True,
        "max_assessment_age_days": 30,
    }
    base.update(overrides)
    return AssessmentCandidate(**base)  # type: ignore[arg-type]


class TestTheDeclaredDenominator:
    def test_the_matrix_is_six_windows_times_four_arms(self) -> None:
        assert len(RECENT_EVIDENCE_WINDOWS) == 6
        assert len(EXPECTED_ARMS) == 4
        assert EXPECTED_EVIDENCE_COMBINATIONS == 24
        assert len(_complete_matrix()) == 24

    def test_a_complete_matrix_refuses_nothing(self) -> None:
        assert recent_evidence_refusals(_complete_matrix()) == ()

    def test_no_evidence_at_all_names_every_window_once(self) -> None:
        refusals = recent_evidence_refusals([])
        assert len(refusals) == len(RECENT_EVIDENCE_WINDOWS)
        assert all(item.startswith("recent_evidence_window_missing:") for item in refusals)

    def test_a_wholly_absent_window_is_one_refusal_not_five(self) -> None:
        """The refusal list should describe the damage, not scale with it."""
        rows = [row for row in _complete_matrix() if row.window_id != "year-2023"]
        refusals = recent_evidence_refusals(rows)
        assert refusals == ("recent_evidence_window_missing:year-2023",)

    def test_one_absent_arm_names_that_arm(self) -> None:
        rows = [
            row
            for row in _complete_matrix()
            if not (
                row.window_id == "year-2023" and (row.ambiguity_arm, row.quarantine_arm) == ("worst_case", "masked")
            )
        ]
        assert recent_evidence_refusals(rows) == ("recent_evidence_arm_missing:year-2023/worst_case/masked",)

    def test_a_five_of_six_matrix_is_a_refusal_not_a_partial_pass(self) -> None:
        """There is no 'close enough' tier — five windows is a CHOSEN denominator."""
        rows = [row for row in _complete_matrix() if row.window_id != "rolling-24m"]
        assert recent_evidence_refusals(rows) != ()

    def test_an_undeclared_window_refuses_even_when_the_six_are_complete(self) -> None:
        rows = [*_complete_matrix(), EvidenceRow("year-2025", "best_case", "masked", 99)]
        assert recent_evidence_refusals(rows) == ("recent_evidence_window_unknown:year-2025",)

    def test_an_undeclared_arm_refuses(self) -> None:
        rows = [*_complete_matrix(), EvidenceRow("year-2023", "median_case", "masked", 99)]
        assert recent_evidence_refusals(rows) == ("recent_evidence_arm_unknown:year-2023/median_case/masked",)

    def test_a_duplicate_identity_refuses_when_it_reaches_the_rule(self) -> None:
        """The loader resolves duplicates; this asserts it happened, not that it will."""
        rows = [*_complete_matrix(), EvidenceRow("year-2023", "best_case", "masked", 999)]
        assert recent_evidence_refusals(rows) == ("recent_evidence_arm_duplicate:year-2023/best_case/masked",)

    def test_refusals_come_back_sorted_so_two_runs_diff_cleanly(self) -> None:
        rows = [row for row in _complete_matrix() if row.window_id not in {"year-2022", "primary-2022-plus"}]
        refusals = recent_evidence_refusals(rows)
        assert list(refusals) == sorted(refusals)


class TestLatestWinsPerIdentity:
    """A re-run ADDS a row; the store is not unique on the window/arm identity."""

    def test_the_higher_result_id_supersedes(self) -> None:
        older = EvidenceRow("year-2023", "best_case", "masked", 10)
        newer = EvidenceRow("year-2023", "best_case", "masked", 40)
        assert select_latest_rows([older, newer]) == (newer,)
        assert select_latest_rows([newer, older]) == (newer,)

    def test_resolution_turns_a_duplicated_matrix_back_into_a_complete_one(self) -> None:
        rerun = [
            EvidenceRow(row.window_id, row.ambiguity_arm, row.quarantine_arm, row.result_id + 100)
            for row in _complete_matrix()
        ]
        resolved = select_latest_rows([*_complete_matrix(), *rerun])
        assert len(resolved) == EXPECTED_EVIDENCE_COMBINATIONS
        assert recent_evidence_refusals(resolved) == ()
        assert min(row.result_id for row in resolved) > 100

    def test_distinct_identities_are_not_collapsed(self) -> None:
        rows = _complete_matrix()
        assert len(select_latest_rows(rows)) == len(rows)


class TestEvidenceReference:
    def test_it_is_deterministic_and_order_insensitive(self) -> None:
        rows = _complete_matrix()
        first = recent_evidence_ref(strategy_id="s6", strategy_version="v1", rows=rows)
        second = recent_evidence_ref(strategy_id="s6", strategy_version="v1", rows=list(reversed(rows)))
        assert first == second

    def test_one_swapped_result_id_changes_it(self) -> None:
        rows = _complete_matrix()
        swapped = [*rows[:-1], EvidenceRow(rows[-1].window_id, rows[-1].ambiguity_arm, rows[-1].quarantine_arm, 4242)]
        assert recent_evidence_ref(strategy_id="s6", strategy_version="v1", rows=rows) != recent_evidence_ref(
            strategy_id="s6", strategy_version="v1", rows=swapped
        )

    def test_the_strategy_identity_is_part_of_it(self) -> None:
        rows = _complete_matrix()
        assert recent_evidence_ref(strategy_id="s6", strategy_version="v1", rows=rows) != recent_evidence_ref(
            strategy_id="s7", strategy_version="v1", rows=rows
        )

    def test_the_digest_is_not_truncated(self) -> None:
        ref = recent_evidence_ref(strategy_id="s6", strategy_version="v1", rows=_complete_matrix())
        prefix, digest = ref.split("+", 1)
        assert prefix == "recent-evidence-v1"
        assert len(digest) == 64

    def test_the_assessment_reference_names_both_halves(self) -> None:
        candidate = _candidate(assessment_id=7, policy_id="assessment-policy-v2")
        assert prospective_assessment_ref(candidate) == "prospective-assessment-v1+7@assessment-policy-v2"


class TestTheActionGraphCannotDriftFromTheStageGraph:
    def test_every_stage_offers_at_most_one_operator_successor(self) -> None:
        for stage in _NEXT_STAGE:
            assert len(operator_targets(stage)) <= 1, stage

    def test_the_ordered_walk_is_exactly_the_declared_one(self) -> None:
        walk: list[tuple[Stage | None, str]] = []
        stage: Stage | None = None
        while (action := allowed_operator_action(stage)) is not None:
            walk.append((stage, action))
            stage = action_target(action)
        assert walk == [
            (None, "register_research_candidate"),
            ("research_candidate", "validate_historical"),
            ("historical_validated", "start_forward_observation"),
            ("forward_observation", "enable_paper"),
        ]

    def test_paper_enabled_is_terminal_for_this_path(self) -> None:
        """`live_enabled` is the dedicated gate's, and must not be reachable here."""
        assert allowed_operator_action("paper_enabled") is None
        assert "live_enabled" not in operator_targets("paper_enabled")

    def test_the_lifecycle_stages_offer_nothing(self) -> None:
        assert allowed_operator_action("paused") is None
        assert allowed_operator_action("retired") is None

    def test_every_action_target_is_reachable_from_exactly_one_stage(self) -> None:
        reached = [
            (stage, action_target(action))
            for stage in _NEXT_STAGE
            if (action := allowed_operator_action(stage)) is not None
        ]
        targets = [target for _stage, target in reached]
        assert sorted(targets) == sorted(set(targets))


class TestForwardObservationMayNotWeakenTheEvidence:
    _A = ("year-2023", "best_case", "masked")
    _B = ("year-2023", "worst_case", "masked")
    _C = ("year-2024", "best_case", "masked")

    def test_the_same_coverage_passes(self) -> None:
        assert weakening_refusals(previously_covered=[self._A, self._B], now_covered=[self._A, self._B]) == ()

    def test_wider_coverage_passes(self) -> None:
        assert weakening_refusals(previously_covered=[self._A], now_covered=[self._A, self._B]) == ()

    def test_a_dropped_combination_refuses_and_names_it(self) -> None:
        assert weakening_refusals(previously_covered=[self._A, self._B], now_covered=[self._A]) == (
            "recent_evidence_weakened:year-2023/worst_case/masked",
        )

    def test_no_prior_promotion_refuses_nothing(self) -> None:
        assert weakening_refusals(previously_covered=[], now_covered=[self._A]) == ()

    def test_a_re_run_that_replaces_every_result_id_is_not_weakening(self) -> None:
        """⚠⚠ THE REGRESSION THIS RULE EXISTS TO AVOID (Codex ckpt-2).

        Comparing RESULT IDS would read a routine `refresh_recent` re-run as "all 24
        dropped": `select_latest_rows` returns the new ids, the old ones are gone from
        the bundle, and because the store is append-only with `ON DELETE RESTRICT`
        pins, that verdict is permanent. One re-run would block
        `start_forward_observation` forever.
        """
        pinned = _complete_matrix()
        rerun = [
            EvidenceRow(row.window_id, row.ambiguity_arm, row.quarantine_arm, row.result_id + 1000) for row in pinned
        ]
        assert {row.result_id for row in pinned}.isdisjoint({row.result_id for row in rerun})
        assert (
            weakening_refusals(previously_covered=evidence_identities(pinned), now_covered=evidence_identities(rerun))
            == ()
        )

    def test_a_shrunken_declared_window_set_is_caught(self) -> None:
        """The one case completeness cannot see: both matrices "complete", one smaller."""
        pinned = evidence_identities(_complete_matrix())
        shrunk = evidence_identities([row for row in _complete_matrix() if row.window_id != "year-2024"])
        assert len(weakening_refusals(previously_covered=pinned, now_covered=shrunk)) == len(EXPECTED_ARMS)


class TestProspectiveAssessmentSelection:
    def test_a_fresh_pass_is_chosen(self) -> None:
        chosen, refusals = select_prospective_assessment(
            policy_present=True, candidates=[_candidate()], as_of=_AS_OF, forward_started_at=None
        )
        assert refusals == ()
        assert chosen is not None and chosen.assessment_id == 1

    def test_no_policy_is_distinct_from_no_assessment(self) -> None:
        assert select_prospective_assessment(
            policy_present=False, candidates=[], as_of=_AS_OF, forward_started_at=None
        )[1] == ("prospective_assessment_policy_missing",)
        assert select_prospective_assessment(policy_present=True, candidates=[], as_of=_AS_OF, forward_started_at=None)[
            1
        ] == ("prospective_assessment_missing",)

    def test_a_failed_assessment_refuses(self) -> None:
        chosen, refusals = select_prospective_assessment(
            policy_present=True, candidates=[_candidate(passed=False)], as_of=_AS_OF, forward_started_at=None
        )
        assert chosen is None
        assert refusals == ("prospective_assessment_not_passed",)

    def test_an_expired_pass_refuses(self) -> None:
        stale = _candidate(checked_at=_AS_OF - timedelta(days=31), max_assessment_age_days=30)
        assert select_prospective_assessment(
            policy_present=True, candidates=[stale], as_of=_AS_OF, forward_started_at=None
        )[1] == ("prospective_assessment_stale",)

    def test_the_overview_s_five_second_future_tolerance_is_inherited(self) -> None:
        """One predicate, or the card says 'fresh' where the transaction says 'stale'."""
        just_ahead = _candidate(checked_at=_AS_OF + timedelta(seconds=4))
        too_far_ahead = _candidate(checked_at=_AS_OF + timedelta(seconds=6))
        assert (
            select_prospective_assessment(
                policy_present=True, candidates=[just_ahead], as_of=_AS_OF, forward_started_at=None
            )[0]
            is not None
        )
        assert select_prospective_assessment(
            policy_present=True, candidates=[too_far_ahead], as_of=_AS_OF, forward_started_at=None
        )[1] == ("prospective_assessment_stale",)

    def test_an_assessment_predating_forward_observation_refuses(self) -> None:
        """Otherwise all four advances run back-to-back on backtest evidence alone."""
        candidate = _candidate(checked_at=_AS_OF - timedelta(days=10))
        chosen, refusals = select_prospective_assessment(
            policy_present=True,
            candidates=[candidate],
            as_of=_AS_OF,
            forward_started_at=_AS_OF - timedelta(days=5),
        )
        assert chosen is None
        assert refusals == ("prospective_assessment_predates_forward_observation",)

    def test_an_assessment_after_forward_observation_is_accepted(self) -> None:
        candidate = _candidate(checked_at=_AS_OF - timedelta(days=1))
        chosen, _ = select_prospective_assessment(
            policy_present=True,
            candidates=[candidate],
            as_of=_AS_OF,
            forward_started_at=_AS_OF - timedelta(days=5),
        )
        assert chosen is not None

    def test_the_most_recent_passing_assessment_wins(self) -> None:
        older = _candidate(assessment_id=1, checked_at=_AS_OF - timedelta(days=3))
        newer = _candidate(assessment_id=2, checked_at=_AS_OF - timedelta(days=1))
        chosen, _ = select_prospective_assessment(
            policy_present=True, candidates=[older, newer], as_of=_AS_OF, forward_started_at=None
        )
        assert chosen is not None and chosen.assessment_id == 2

    def test_a_failing_newer_assessment_does_not_hide_a_passing_older_one(self) -> None:
        """`passed` filters first, so a later failure does not veto — it just is not chosen."""
        passing = _candidate(assessment_id=1, checked_at=_AS_OF - timedelta(days=3))
        failing = _candidate(assessment_id=2, checked_at=_AS_OF - timedelta(days=1), passed=False)
        chosen, refusals = select_prospective_assessment(
            policy_present=True, candidates=[passing, failing], as_of=_AS_OF, forward_started_at=None
        )
        assert refusals == ()
        assert chosen is not None and chosen.assessment_id == 1


class TestTheReadSurface:
    def test_the_action_is_named_alongside_its_refusals(self) -> None:
        action, refusals = next_operator_action_view(
            stage="research_candidate",
            purpose="capital_candidate",
            evidence_refusals=["recent_evidence_window_missing x6"],
        )
        assert action == "validate_historical"
        assert refusals == ("recent_evidence_window_missing x6",)

    def test_a_harness_control_is_told_why_it_cannot_advance(self) -> None:
        action, refusals = next_operator_action_view(
            stage="research_candidate", purpose="harness_validation", evidence_refusals=[]
        )
        assert action == "validate_historical"
        assert refusals == ("strategy_not_capital_candidate",)

    def test_registering_a_research_candidate_needs_no_purpose(self) -> None:
        """`research_candidate` is not an evidence stage; the primitive permits it."""
        action, refusals = next_operator_action_view(
            stage=None, purpose="harness_validation", evidence_refusals=["ignored"]
        )
        assert action == "register_research_candidate"
        assert refusals == ()

    def test_paper_enable_does_not_report_backtest_matrix_refusals(self) -> None:
        """Its evidence is the prospective assessment, not the backtest matrix."""
        action, refusals = next_operator_action_view(
            stage="forward_observation",
            purpose="capital_candidate",
            evidence_refusals=["recent_evidence_window_missing:year-2023"],
        )
        assert action == "enable_paper"
        assert refusals == ()

    def test_paper_enable_reports_a_known_assessment_refusal(self) -> None:
        """Otherwise the button is enabled for a guaranteed 409 (Codex ckpt-2)."""
        action, refusals = next_operator_action_view(
            stage="forward_observation",
            purpose="capital_candidate",
            evidence_refusals=["recent_evidence_window_missing:year-2023"],
            assessment_refusals=["prospective_assessment_stale"],
        )
        assert action == "enable_paper"
        assert refusals == ("prospective_assessment_stale",)

    def test_earlier_steps_ignore_assessment_refusals(self) -> None:
        """A stale prospective assessment does not block historical validation."""
        _action, refusals = next_operator_action_view(
            stage="research_candidate",
            purpose="capital_candidate",
            evidence_refusals=[],
            assessment_refusals=["prospective_assessment_missing"],
        )
        assert refusals == ()

    def test_a_terminal_stage_offers_nothing(self) -> None:
        assert next_operator_action_view(stage="retired", purpose="capital_candidate", evidence_refusals=[]) == (
            None,
            (),
        )

    def test_an_unrecognised_stage_does_not_raise(self) -> None:
        """A read surface must not blow up on a value it cannot classify."""
        assert next_operator_action_view(stage="something_new", purpose="capital_candidate", evidence_refusals=[]) == (
            None,
            (),
        )


class TestRefusalSummary:
    def test_twenty_four_arm_refusals_collapse_to_one_line(self) -> None:
        refusals = [f"recent_evidence_arm_missing:w{index}/best_case/masked" for index in range(24)]
        assert evidence_refusal_summary(refusals) == ("recent_evidence_arm_missing x24",)

    def test_a_single_refusal_keeps_its_detail(self) -> None:
        assert evidence_refusal_summary(["recent_evidence_window_missing:year-2023"]) == (
            "recent_evidence_window_missing:year-2023",
        )

    def test_classes_are_reported_separately(self) -> None:
        assert evidence_refusal_summary(
            [
                "recent_evidence_window_missing:year-2023",
                "recent_evidence_window_missing:year-2024",
                "recent_evidence_arm_missing:year-2022/best_case/masked",
            ]
        ) == ("recent_evidence_arm_missing:year-2022/best_case/masked", "recent_evidence_window_missing x2")


class TestTheEndpointCannotBeHandedADenominator:
    def test_the_request_model_exposes_no_evidence_fields(self) -> None:
        """Structural: a browser must not be able to name its own result ids."""
        from app.api.strategies import StrategyAdvanceRequest

        assert set(StrategyAdvanceRequest.model_fields) == {"action", "reason"}

    @pytest.mark.parametrize("field", ["result_ids", "strategy_version", "to_stage", "evidence_ref"])
    def test_the_forbidden_inputs_are_absent_by_name(self, field: str) -> None:
        from app.api.strategies import StrategyAdvanceRequest

        assert field not in StrategyAdvanceRequest.model_fields
