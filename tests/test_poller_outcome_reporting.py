"""#3111 slice 6 — the two pollers' #2218 progress verdict.

Pure-logic: ``progress_for`` on both jobs is a total function of its stats
object, and the verdict is ``degradation_reason`` over the result. That is the
whole risk-bearing decision, so no DB is needed for it — the counter plumbing
that FEEDS these stats is exercised separately in
``tests/test_sec_per_cik_poll.py`` against a real connection.

The table below is spec §10i's acceptance matrix, one case per row.

⚠⚠ The two cases this file exists for are **2 and 3**: a poll that finds
nothing is HEALTHY. Wiring the discovery count as the outcome bucket instead
would have degraded 1,940 of ``sec_per_cik_poll``'s 1,961 successful runs and
5,698 of ``expected_filings_poller``'s 5,701 (full population, ``job_runs``,
spec §10c). Those two assertions are the ones that must not be relaxed.
"""

from __future__ import annotations

import pytest

from app.jobs.expected_filings_poller import PollStats
from app.jobs.expected_filings_poller import progress_for as expected_progress_for
from app.jobs.sec_per_cik_poll import PerCikPollStats
from app.jobs.sec_per_cik_poll import progress_for as per_cik_progress_for
from app.services.job_progress import degradation_reason


def _per_cik(**kw: int) -> PerCikPollStats:
    base: dict[str, int] = {
        "subjects_polled": 0,
        "new_filings_recorded": 0,
        "poll_errors": 0,
        "recheck_subjects_polled": 0,
        "recheck_new_filings_recorded": 0,
        "recheck_poll_errors": 0,
        "manifest_rejected": 0,
    }
    base.update(kw)
    return PerCikPollStats(**base)  # type: ignore[arg-type]


# --- sec_per_cik_poll -----------------------------------------------------


@pytest.mark.parametrize(
    ("case", "stats", "degraded"),
    [
        ("1: no due subjects", _per_cik(), False),
        ("2: polled, no discovery", _per_cik(subjects_polled=66), False),
        ("3: polled, discovery", _per_cik(subjects_polled=66, new_filings_recorded=4), False),
        ("4: every fetch failed", _per_cik(subjects_polled=66, poll_errors=66), True),
        ("5: one batch of 3 failed", _per_cik(subjects_polled=66, poll_errors=3), True),
        ("6: recheck-only, clean", _per_cik(recheck_subjects_polled=34), False),
        (
            "7: recheck errors only",
            _per_cik(subjects_polled=66, recheck_subjects_polled=34, recheck_poll_errors=2),
            True,
        ),
        ("10: accession rejected", _per_cik(subjects_polled=66, manifest_rejected=1), True),
        ("11: two subjects reject one accession", _per_cik(subjects_polled=66, manifest_rejected=2), True),
    ],
)
def test_per_cik_acceptance_matrix(case: str, stats: PerCikPollStats, degraded: bool) -> None:
    assert (degradation_reason(per_cik_progress_for(stats)) is not None) is degraded, case


def test_per_cik_a_clean_poll_finding_nothing_is_healthy() -> None:
    """Matrix row 2 — the 98.9% case, asserted on its own so it cannot be
    relaxed by editing a parametrize row.

    ⚠ The discriminator is that ``polled`` and NOT ``new_filings`` is the
    outcome bucket. Both stats below have zero discovery.
    """
    progress = per_cik_progress_for(_per_cik(subjects_polled=66, recheck_subjects_polled=34))

    assert progress.candidates_seen == 100
    assert progress.outcomes == {"polled": 100}
    assert progress.context["new_filings"] == 0
    assert degradation_reason(progress) is None


def test_per_cik_polled_is_the_complement_of_subject_denominated_errors() -> None:
    """``polled = seen - poll_errors - recheck_poll_errors``, and NOT minus
    ``manifest_rejected``, which is in a different unit (spec §10e)."""
    progress = per_cik_progress_for(
        _per_cik(
            subjects_polled=10,
            recheck_subjects_polled=5,
            poll_errors=3,
            recheck_poll_errors=2,
            manifest_rejected=7,
        )
    )

    assert progress.candidates_seen == 15
    assert progress.outcomes == {"polled": 10}  # 15 - 3 - 2, NOT 15 - 3 - 2 - 7
    assert progress.errors["manifest_rejected"] == 7


def test_per_cik_lanes_have_separate_error_buckets() -> None:
    """§7's recorded gap: recheck errors used to fold into ``poll_errors``, so a
    per-lane success count was uncomputable."""
    progress = per_cik_progress_for(
        _per_cik(subjects_polled=10, recheck_subjects_polled=5, poll_errors=1, recheck_poll_errors=4)
    )

    assert progress.errors["poll_fetch_failed"] == 1
    assert progress.errors["recheck_fetch_failed"] == 4
    reason = degradation_reason(progress)
    assert reason is not None
    assert "poll_fetch_failed=1" in reason
    assert "recheck_fetch_failed=4" in reason


# --- expected_filings_poller ----------------------------------------------


@pytest.mark.parametrize(
    ("case", "stats", "degraded"),
    [
        ("1: no due rows", PollStats(subjects_polled=0, fulfilled=0, poll_errors=0), False),
        ("2: polled, nothing filed yet", PollStats(subjects_polled=2, fulfilled=0, poll_errors=0), False),
        ("3: polled, fulfilled", PollStats(subjects_polled=2, fulfilled=1, poll_errors=0), False),
        ("4: every probe failed", PollStats(subjects_polled=2, fulfilled=0, poll_errors=2), True),
        ("5: one of two probes failed", PollStats(subjects_polled=2, fulfilled=1, poll_errors=1), True),
        (
            "13: fulfilled but refresh failed",
            PollStats(subjects_polled=1, fulfilled=1, poll_errors=0, fundamentals_refresh_failed=1),
            True,
        ),
        (
            "14: fulfilled, refresh clean, companyfacts lag",
            PollStats(subjects_polled=1, fulfilled=1, poll_errors=0, fundamentals_refresh_failed=0),
            False,
        ),
    ],
)
def test_expected_filings_acceptance_matrix(case: str, stats: PollStats, degraded: bool) -> None:
    assert (degradation_reason(expected_progress_for(stats)) is not None) is degraded, case


def test_expected_filings_an_open_window_with_no_filing_is_healthy() -> None:
    """Matrix row 2 — the 99.9% case. An expectation whose issuer has not filed
    yet is the steady state, not a stall."""
    progress = expected_progress_for(PollStats(subjects_polled=2, fulfilled=0, poll_errors=0))

    assert progress.candidates_seen == 2
    assert progress.outcomes == {"polled": 2}
    assert progress.context["fulfilled"] == 0
    assert degradation_reason(progress) is None


def test_expected_filings_companyfacts_lag_is_not_an_error() -> None:
    """Matrix row 14. The poller's own spec
    (docs/specs/etl/2026-06-28-expected-filings-poller.md:149-151) makes XBRL
    lagging the filing an expected state the daily backstop resolves — so only
    RETURNED refresh failures count, never "no new facts"."""
    lagging = expected_progress_for(PollStats(subjects_polled=1, fulfilled=1, poll_errors=0))
    assert degradation_reason(lagging) is None


# --- the contract both rely on --------------------------------------------


def test_zero_valued_error_buckets_are_emitted_and_inert() -> None:
    """Emitted so "this job measured zero errors" is sayable at the read
    boundary (slice 5 distinguishes ``None`` from a measured zero), and inert
    because ``degradation_reason`` filters on ``n > 0``."""
    progress = per_cik_progress_for(_per_cik(subjects_polled=1))

    assert progress.errors == {"poll_fetch_failed": 0, "recheck_fetch_failed": 0, "manifest_rejected": 0}
    assert progress.as_json()["errors"] == progress.errors
    assert degradation_reason(progress) is None


def test_no_bucket_name_appears_in_both_outcomes_and_errors() -> None:
    """``JobProgress``'s contract: "a job must NOT put a bucket here that it
    also reports as an error"."""
    for progress in (
        per_cik_progress_for(_per_cik(subjects_polled=9, poll_errors=1, manifest_rejected=1)),
        expected_progress_for(PollStats(subjects_polled=9, fulfilled=1, poll_errors=1, fundamentals_refresh_failed=1)),
    ):
        assert not set(progress.outcomes) & set(progress.errors)
