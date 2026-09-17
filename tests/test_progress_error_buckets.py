"""#3111 slice 5 — the second error counter reaching the Errored column.

Pure-logic only, deliberately in its own module: the `db` marker is applied
per-MODULE at collection, so putting the one SQL round-trip here would evict
every table case below from the `-m "not db"` push tier. That round-trip lives
in `tests/test_scheduled_adapter.py` beside the rest of the adapter's DB tests.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.jobs.sec_manifest_worker import WorkerStats
from app.services.job_progress import JobProgress, degradation_reason
from app.services.processes.scheduled_adapter import _progress_error_buckets

# ---------------------------------------------------------------------------
# Extraction — the `n > 0` predicate, and NULL-is-not-zero
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        # sql/254: "NULL means the job does not report progress, which is NOT
        # the same as reporting zero". Four different ways of not reporting,
        # all None — never {}.
        (None, None),
        ("not-a-dict", None),
        ({}, None),
        ({"errors": None}, None),
        ({"errors": "boom"}, None),
        ({"candidates_seen": 5, "outcomes": {"parsed": 5}}, None),
        # Reported, nothing positive -> {} . This is the 245-row case in the
        # dev corpus, most of it the {"failed": 0} shape scheduler.py:3464
        # seeds before a sweep runs. Rendering a key as an error here would
        # flag 239 clean successes.
        ({"errors": {}}, {}),
        ({"errors": {"failed": 0}}, {}),
        ({"errors": {"failed": 0, "dispatch_errors": 0}}, {}),
        # The one positive run in 133,952 (daily_candle_refresh 133470).
        ({"errors": {"failed": 1}}, {"failed": 1}),
        # Multiple buckets survive SEPARATELY. Summing them is the fold this
        # slice exists to avoid.
        (
            {"errors": {"failed": 3, "dispatch_errors": 2}},
            {"failed": 3, "dispatch_errors": 2},
        ),
        # `n > 0`, not truthiness — degradation_reason's own predicate and its
        # own recorded reason: truthiness makes {"api_errors": -1} degrade
        # while {"done": -1} reads as progress.
        ({"errors": {"a": 2, "b": 0, "c": -1}}, {"a": 2}),
        # A bool is not a count, even though Python says `True > 0`.
        ({"errors": {"flag": True, "a": 1}}, {"a": 1}),
        ({"errors": {"flag": False}}, {}),
        # Non-int values dropped rather than crashing the History tab. The
        # corpus has 0 non-numeric and 0 non-integral values across all
        # 133,952 rows, so this drops nothing any producer can emit.
        ({"errors": {"a": "5", "b": None, "c": [1], "d": {"n": 1}, "e": 4}}, {"e": 4}),
    ],
)
def test_bucket_extraction(payload: Any, expected: dict[str, int] | None) -> None:
    assert _progress_error_buckets(payload) == expected


def test_none_and_empty_are_distinct_answers() -> None:
    """The distinction sql/254 spent a column comment on must survive the read.

    Both render `—`, so a test that only checked the rendering would pass with
    them collapsed. Assert the values themselves, and assert they are not equal
    to each other — `{} == {}` but `None != {}`, and an `assert x == {}` alone
    would not catch a helper that returned `{}` for an unreported run.
    """
    unreported = _progress_error_buckets(None)
    reported_clean = _progress_error_buckets({"errors": {"failed": 0}})
    assert unreported is None
    assert reported_clean == {}
    assert unreported != reported_clean


# ---------------------------------------------------------------------------
# The cell and the verdict must not disagree
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "errors",
    [
        {"failed": 1},
        {"failed": 3, "dispatch_errors": 2},
        {"a": 2, "b": 0, "c": -1},
        {"api_errors": 7},
    ],
)
def test_every_map_that_degrades_yields_a_non_empty_cell(errors: dict[str, int]) -> None:
    """A run flagged `degraded` for errors must show what flagged it.

    The Errored cell and the Status cell are computed from the SAME map by two
    different functions. If they applied different predicates an operator would
    see `degraded` beside `—` — which is exactly the slice-4 defect, in a new
    place. This pins them together rather than trusting the comment.
    """
    reason = degradation_reason(JobProgress(errors=errors))
    assert reason is not None and reason.startswith("errors reported:")
    assert _progress_error_buckets({"errors": errors})


def test_degraded_for_no_terminal_outcome_correctly_shows_nothing() -> None:
    """`degradation_reason`'s SECOND rule degrades a run with NO errors.

    Saw candidates, produced no outcome. Such a run is `degraded` with an empty
    error map and the Errored column is right to stay `—`: nothing errored. The
    spec's first draft claimed a run could only degrade through errors; it was
    false, and this is the case that falsified it.
    """
    progress = JobProgress(candidates_seen=40, outcomes={"parsed": 0}, errors={})
    assert degradation_reason(progress) is not None
    assert _progress_error_buckets(progress.as_json()) == {}


# ---------------------------------------------------------------------------
# The gate behind the buckets-first display rule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("failed", "dispatch_errors"),
    [(0, 0), (1, 0), (0, 1), (3, 2), (11, 7)],
)
def test_manifest_buckets_account_for_the_whole_scalar(failed: int, dispatch_errors: int) -> None:
    """Buckets-first is safe only while the buckets ARE the scalar.

    The History cell renders `JobProgress.errors` buckets in preference to
    `rows_errored`, which loses nothing ONLY because `sec_manifest_worker` —
    the single `rows_errored` producer in the tree (`JobTelemetryAggregator`
    is instantiated once, at `scheduler.py:8093`) — pins
    `agg.rows_errored == failed + dispatch_errors` (`sec_manifest_worker.py:760`)
    and puts those same two counters in its `JobProgress` (`:367`).

    The moment a bucket appears that is NOT part of that total, the scalar
    carries information the cell would hide and the display rule needs two
    labelled segments instead of a precedence. That is the spec's §9c revisit
    trigger, and this is it as a gate rather than as a sentence someone has to
    remember to re-read.
    """
    stats = WorkerStats(
        rows_processed=failed + dispatch_errors + 4,
        parsed=3,
        tombstoned=1,
        failed=failed,
        skipped_no_parser=0,
        dispatch_errors=dispatch_errors,
    )
    buckets = stats.to_job_progress().errors
    assert set(buckets) == {"failed", "dispatch_errors"}
    assert sum(buckets.values()) == failed + dispatch_errors
