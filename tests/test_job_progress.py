"""#2218 — the progress verdict. Pure, no database.

Every case below is drawn from a real run shape rather than an imagined one:
the two defects that motivated the ticket (#2213 `cusip_resolver_post_bulk_sweep`
at 100% `api_errors`, #2214 `ncen_classifier_yearly` writing zero rows) and the
healthy shapes that must NOT be degraded, which is where a naive "zero rows =
alarm" rule would have produced noise.
"""

from __future__ import annotations

from app.services.job_progress import JobProgress, degradation_reason


def test_no_progress_reported_is_judged_exactly_as_before() -> None:
    # The opt-in property. Every job that has not been wired must be inert,
    # otherwise this change silently reclassifies the whole scheduler.
    assert degradation_reason(None) is None


def test_saw_nothing_did_nothing_is_healthy() -> None:
    # ⚠ The case a "zero rows written = failure" rule gets wrong, and the
    # reason the ticket says not to build one. A sweep with an empty queue is
    # the normal state of most jobs most of the time.
    progress = JobProgress(candidates_seen=0, outcomes={"promoted": 0}, errors={"api_errors": 0})
    assert degradation_reason(progress) is None


def test_saw_work_and_did_some_is_healthy() -> None:
    progress = JobProgress(
        candidates_seen=120,
        outcomes={"promoted": 3, "no_instrument_match": 117},
        errors={"api_errors": 0},
    )
    assert degradation_reason(progress) is None


def test_a_non_universe_outcome_still_counts_as_progress() -> None:
    # Most bulk CUSIPs resolve to something outside our universe. If
    # `no_instrument_match` were treated as a miss, every healthy run of
    # `cusip_resolver_post_bulk_sweep` would read degraded — the false-alarm
    # direction, which trains the operator to ignore the signal.
    progress = JobProgress(candidates_seen=500, outcomes={"promoted": 0, "no_instrument_match": 500})
    assert degradation_reason(progress) is None


def test_all_errored_is_degraded() -> None:
    # #2213's actual shape: the resolver raised, every CUSIP was bound to the
    # error counter, and the job recorded success / row_count 0 for seven
    # weeks.
    progress = JobProgress(
        candidates_seen=44_195,
        outcomes={"promoted": 0, "no_instrument_match": 0},
        errors={"api_errors": 44_195},
    )
    reason = degradation_reason(progress)
    assert reason is not None
    assert "api_errors=44195" in reason


def test_partial_errors_still_degrade() -> None:
    # ⚠ Deliberate, and the interesting half: a run that did SOME work and
    # errored the rest is degraded. The seven-week stall was a 100%-error run,
    # but the same failure at 20% hides the same defect at a smaller scale and
    # is the shape that precedes it.
    progress = JobProgress(candidates_seen=100, outcomes={"promoted": 80}, errors={"api_errors": 20})
    assert degradation_reason(progress) is not None


def test_saw_candidates_but_no_terminal_outcome_is_the_silent_stall() -> None:
    # #2214's shape: filers seen, nothing classified, nothing errored either.
    progress = JobProgress(
        candidates_seen=11_464,
        outcomes={"classifications_written": 0, "no_ncen_found": 0},
        errors={"parse_failures": 0, "fetch_failures": 0},
    )
    reason = degradation_reason(progress)
    assert reason is not None
    assert "11464" in reason


def test_errors_outrank_the_stall_reason() -> None:
    # Both conditions hold here. The error detail is the more actionable of
    # the two — "it errored on 5 things" points somewhere, "it produced
    # nothing" does not — so it must be the reported reason.
    progress = JobProgress(candidates_seen=5, outcomes={"promoted": 0}, errors={"api_errors": 5})
    reason = degradation_reason(progress)
    assert reason is not None
    assert reason.startswith("errors reported")


def test_unknown_candidate_count_cannot_trigger_the_stall_rule() -> None:
    # `None` is not zero and is not "many". A job that does not count its
    # candidates cannot be judged stalled without inventing an alarm out of an
    # absence, which is the same error class as back-filling a missing date.
    progress = JobProgress(candidates_seen=None, outcomes={"promoted": 0})
    assert degradation_reason(progress) is None


def test_progress_serialises_with_its_three_axes_intact() -> None:
    # `progress_json` is the evidence #2213 needed and did not have. The
    # shape is asserted because a reader querying for the next stall will
    # query these keys.
    progress = JobProgress(candidates_seen=7, outcomes={"done": 1}, errors={"boom": 2})
    assert progress.as_json() == {
        "candidates_seen": 7,
        "outcomes": {"done": 1},
        "errors": {"boom": 2},
    }


def test_progress_serialises_optional_population_context_without_changing_default_shape() -> None:
    progress = JobProgress(
        candidates_seen=10,
        outcomes={"usable": 8},
        context={"provider_session": "2026-08-10", "population_status": "partial"},
    )
    assert progress.as_json()["context"] == {
        "provider_session": "2026-08-10",
        "population_status": "partial",
    }


def test_negative_counts_are_not_treated_as_opposites() -> None:
    """Codex ckpt-3. A negative count is nonsense in either bucket, but under
    truthiness it degraded on the error side and read as PROGRESS on the
    outcome side — the same bad value pointing two different ways.
    """
    assert degradation_reason(JobProgress(errors={"api_errors": -1})) is None
    assert degradation_reason(JobProgress(candidates_seen=5, outcomes={"done": -1})) is not None
