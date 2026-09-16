"""#2500 slice 1 — the pure baseline SELECTION rule (no database)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.services.strategy_monitoring_baseline import (
    MISSING_ENVELOPE_COMPONENTS,
    NO_EVIDENCE_STAGE_PROMOTION,
    NO_PINNED_RESULTS,
    NO_PROMOTION_FOR_VERSION,
    BaselinePromotionCandidate,
    select_baseline_promotion,
)

_AT = datetime(2026, 9, 16, tzinfo=UTC)


def _candidate(stage: str, promotion_id: int, *, result_ids: tuple[int, ...] = (1,)) -> BaselinePromotionCandidate:
    return BaselinePromotionCandidate(to_stage=stage, promotion_id=promotion_id, promoted_at=_AT, result_ids=result_ids)


def test_no_promotion_at_all_is_its_own_reason() -> None:
    choice = select_baseline_promotion(())
    assert choice.candidate is None
    assert choice.refusals == (NO_PROMOTION_FOR_VERSION,)


def test_forward_observation_is_preferred_over_historical_validated() -> None:
    # Ordered historical-first on purpose: the rule must not depend on input order.
    choice = select_baseline_promotion(
        (
            _candidate("historical_validated", 1, result_ids=(10,)),
            _candidate("forward_observation", 2, result_ids=(20,)),
        )
    )
    assert choice.refusals == ()
    assert choice.candidate is not None
    assert (choice.candidate.to_stage, choice.candidate.result_ids) == ("forward_observation", (20,))


def test_historical_validated_is_used_when_no_forward_stage_exists() -> None:
    choice = select_baseline_promotion((_candidate("historical_validated", 1, result_ids=(10,)),))
    assert choice.candidate is not None
    assert choice.candidate.to_stage == "historical_validated"


@pytest.mark.parametrize("stage", ["paper_enabled", "live_enabled", "paused", "retired", "research_candidate"])
def test_a_non_evidence_stage_is_never_the_baseline_even_when_it_is_the_latest(stage: str) -> None:
    """``enable_paper`` pins no result ids by design, so the deploying promotion carries
    no envelope — and ``paused``/``retired`` must not silently become the baseline just
    by being most recent."""
    choice = select_baseline_promotion((_candidate(stage, 9, result_ids=()),))
    assert choice.candidate is None
    assert choice.refusals == (NO_EVIDENCE_STAGE_PROMOTION,)


def test_a_non_evidence_stage_does_not_hide_an_evidence_stage() -> None:
    choice = select_baseline_promotion(
        (_candidate("paper_enabled", 3, result_ids=()), _candidate("forward_observation", 2, result_ids=(20,)))
    )
    assert choice.candidate is not None
    assert choice.candidate.promotion_id == 2


def test_an_evidence_stage_with_no_pinned_results_refuses_rather_than_falling_back() -> None:
    """Falling back would monitor against the statement the empty promotion superseded.

    ``promote_strategy`` refuses to create such a row, so reaching this clause means the
    stored row is not what the transition rule says it is.
    """
    choice = select_baseline_promotion(
        (_candidate("forward_observation", 2, result_ids=()), _candidate("historical_validated", 1, result_ids=(10,)))
    )
    assert choice.candidate is None
    assert choice.refusals == (NO_PINNED_RESULTS,)


def test_missing_envelope_components_names_what_2505_does_not_carry() -> None:
    """A closed list, so the monitor's build surface cannot drift into 'the envelope is complete'."""
    assert MISSING_ENVELOPE_COMPONENTS == (
        "firing_interarrival_range",
        "broker_fill_rejection_range",
        "resolved_outcome_maturity_range",
        "checkpoint_plan_and_error_budget",
    )
