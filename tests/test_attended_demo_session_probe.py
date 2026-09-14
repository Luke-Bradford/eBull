"""The attended-session observer's pure classification (#2961 #2993).

Pure by design — no DB, no broker, so this stays in the `-m "not db"` push tier. The
parts worth pinning are the two places the observer turns a raw observation into a
sentence somebody will later quote:

* `classify_lookup_pair` — the protocol's P3 table. The failure that matters is
  collapsing `error` into `not_found`, which would manufacture evidence for #2961 out
  of an outage.
* `diff_event_counts` — #2993's counter delta. The failure that matters is diffing the
  TOTAL, which hides a +1 in one asset bucket cancelling a −1 in another.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.probe_attended_demo_session import (
    classify_lookup_pair,
    diff_event_counts,
    resolve_history_min_date,
)


@pytest.mark.parametrize(
    ("reference", "order", "expected"),
    [
        ("found", "found", "reference_key_present"),
        ("not_found", "found", "reference_key_absent_or_lagging"),
        ("not_found", "not_found", "v1_absent_from_v2_index"),
        ("found", "not_found", "contradictory"),
    ],
)
def test_every_p3_row_is_reachable(reference: str, order: str, expected: str) -> None:
    assert classify_lookup_pair(reference, order)["reading"] == expected


@pytest.mark.parametrize(
    ("reference", "order"),
    [
        ("error", "found"),
        ("found", "error"),
        ("error", "error"),
        ("error", "not_found"),
        ("not_found", "error"),
    ],
)
def test_an_error_on_either_arm_is_inconclusive_not_a_not_found(reference: str, order: str) -> None:
    """⚠ THE LOAD-BEARING ONE.

    A 500, a 429 or a parse failure licenses no conclusion. Folding any of them into
    `not_found` would let an outage read as "the broker has no record of this order",
    which is precisely the sentence #2961 would build a fail-closed discriminator on.
    """
    assert classify_lookup_pair(reference, order)["reading"] == "inconclusive"


def test_a_deliberately_unrun_arm_reads_as_single_arm_not_as_a_defect() -> None:
    """`absent` arises from the negative-control phase, which runs no orderId arm.

    ⚠ Distinct from `inconclusive`. Both decline to pick a P3 row, but one says "this
    run did not ask" and the other says "this run asked and could not tell" — and the
    artefact is read by an operator deciding whether to re-take the measurement. The
    2026-09-14 arm-N observation is exactly this shape and is a clean pass.
    """
    result = classify_lookup_pair("not_found", "absent")
    assert result["reading"] == "single_arm"
    assert "negative-control" in result["note"]


def test_an_error_still_outranks_a_missing_arm() -> None:
    """A real error must not be softened to `single_arm` just because a peer is absent."""
    assert classify_lookup_pair("error", "absent")["reading"] == "inconclusive"


def test_every_reading_states_what_remains_uncontrolled() -> None:
    """A reading without its residual ambiguity is how a 404 becomes a verdict.

    Asserted structurally rather than by eyeballing the table: a future row added
    without a note would otherwise ship silently.
    """
    for reference, order in [("found", "found"), ("not_found", "found"), ("not_found", "not_found")]:
        note = classify_lookup_pair(reference, order)["note"]
        assert len(note) > 40, f"{reference}/{order} has no stated residual ambiguity"


def test_counter_delta_is_per_bucket_not_on_the_total() -> None:
    """⚠ A +1 in one asset bucket cancelling a −1 in another nets to zero.

    #2993's discriminator is phrased on "the current-year figure", and the counter is
    bucketed by `(closeYear, assetType)`. A total-only diff reports the one number a
    reader would quote and it is the wrong one.
    """
    before = [
        {"closeYear": 2026, "assetType": "Stocks", "closedPositionEvents": 5},
        {"closeYear": 2026, "assetType": "Crypto", "closedPositionEvents": 3},
    ]
    after = [
        {"closeYear": 2026, "assetType": "Stocks", "closedPositionEvents": 6},
        {"closeYear": 2026, "assetType": "Crypto", "closedPositionEvents": 2},
    ]
    deltas = {(d["closeYear"], d["assetType"]): d["delta"] for d in diff_event_counts(before, after)}
    assert deltas[(2026, "Stocks")] == 1
    assert deltas[(2026, "Crypto")] == -1
    assert sum(d for d in deltas.values() if d is not None) == 0


def test_the_expected_single_close_shows_as_plus_one_on_its_bucket() -> None:
    before = [{"closeYear": 2026, "assetType": "Stocks", "closedPositionEvents": 5}]
    after = [{"closeYear": 2026, "assetType": "Stocks", "closedPositionEvents": 6}]
    assert diff_event_counts(before, after) == [
        {
            "closeYear": 2026,
            "assetType": "Stocks",
            "before": 5,
            "after": 6,
            "delta": 1,
            "bucket_appeared": False,
            "bucket_vanished": False,
        }
    ]


def test_an_appearing_bucket_is_flagged_rather_than_read_as_a_delta() -> None:
    """The first close of the year creates the bucket; `delta` is not computable.

    Defaulting the missing side to 0 would report `delta=1` — indistinguishable from an
    existing bucket incrementing, and it is not the same observation.
    """
    after_only = diff_event_counts([], [{"closeYear": 2026, "assetType": "Stocks", "closedPositionEvents": 1}])
    assert after_only[0]["bucket_appeared"] is True
    assert after_only[0]["delta"] is None


def test_a_vanishing_bucket_is_flagged() -> None:
    """A counter correction can remove a bucket; that is recordable, not an error."""
    before_only = diff_event_counts([{"closeYear": 2021, "assetType": "Commodities", "closedPositionEvents": 27}], [])
    assert before_only[0]["bucket_vanished"] is True
    assert before_only[0]["delta"] is None


def test_a_relative_history_window_is_marked_not_comparable() -> None:
    """⚠ THE OTHER LOAD-BEARING ONE.

    `now - history_days` is re-evaluated per invocation, so the baseline and close
    phases do NOT read the identical range the protocol promises. A trade near the lower
    boundary can leave the range for that reason alone and show up in the #2993 diff as
    a difference the session did not cause. The flag is what stops that being invisible.
    """
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    resolved, comparable = resolve_history_min_date(history_min_date=None, history_days=90, now=now)
    assert comparable is False
    assert resolved == datetime(2026, 6, 22, 12, 0, tzinfo=UTC)


def test_the_same_relative_window_moves_between_two_invocations() -> None:
    """The defect itself, pinned — otherwise the flag above reads as pedantry."""
    first, _ = resolve_history_min_date(
        history_min_date=None, history_days=90, now=datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    )
    second, _ = resolve_history_min_date(
        history_min_date=None, history_days=90, now=datetime(2026, 9, 20, 14, 30, tzinfo=UTC)
    )
    assert first != second


def test_an_absolute_cutoff_is_comparable_and_is_stable() -> None:
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    later = datetime(2026, 9, 20, 18, 0, tzinfo=UTC)
    first, comparable = resolve_history_min_date(history_min_date="2026-06-22T12:00:00+00:00", history_days=90, now=now)
    second, _ = resolve_history_min_date(history_min_date="2026-06-22T12:00:00+00:00", history_days=90, now=later)
    assert comparable is True
    assert first == second


def test_a_naive_absolute_cutoff_is_read_as_utc() -> None:
    """The broker read is UTC-denominated; a naive local reading would silently shift it."""
    resolved, _ = resolve_history_min_date(
        history_min_date="2026-06-22T12:00:00", history_days=90, now=datetime(2026, 9, 20, tzinfo=UTC)
    )
    assert resolved == datetime(2026, 6, 22, 12, 0, tzinfo=UTC)


def test_unchanged_counter_reports_zero_rather_than_dropping_the_bucket() -> None:
    """Unchanged is an OUTCOME (#2993 reading 1), so it must survive into the report."""
    rows = [{"closeYear": 2026, "assetType": "Stocks", "closedPositionEvents": 5}]
    assert diff_event_counts(rows, rows) == [
        {
            "closeYear": 2026,
            "assetType": "Stocks",
            "before": 5,
            "after": 5,
            "delta": 0,
            "bucket_appeared": False,
            "bucket_vanished": False,
        }
    ]
