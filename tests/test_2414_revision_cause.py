"""#2414 — the pure half of revision-cause attribution.

No DB: ``revision_cause`` is a pure function and ``_candles_fetch_count``'s only
DB interaction is one ``MAX(price_date)`` read, which a double supplies. The
branch-attribution end of this lives in
``tests/test_market_data_bar_revision_counter_db.py``, which drives the real
loop — conservation cannot validate attribution (labelling everything
``stale_reobservation`` satisfies the sum), so the labels are pinned there
against executed paths and here against the decision rules alone.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from app.services.market_data import (
    _INCREMENTAL_FETCH_BARS,
    FETCH_REASON_INCREMENTAL,
    FETCH_REASON_INITIAL_BACKFILL,
    FETCH_REASON_STALE_REOBSERVATION,
    REVISION_CAUSE_ADJUSTMENT_HEAL,
    REVISION_CAUSE_FORCE_BACKFILL,
    _candles_fetch_count,
    revision_cause,
)


def _conn_returning(latest: date | None) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None if latest is None else (latest,)
    return conn


# ---------------------------------------------------------------------------
# revision_cause
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("adjustment_detected", "force_backfill", "fetch_reason", "expected"),
    [
        # The three fetch reasons pass straight through when neither override fires.
        (False, False, FETCH_REASON_INCREMENTAL, FETCH_REASON_INCREMENTAL),
        (False, False, FETCH_REASON_STALE_REOBSERVATION, FETCH_REASON_STALE_REOBSERVATION),
        (False, False, FETCH_REASON_INITIAL_BACKFILL, FETCH_REASON_INITIAL_BACKFILL),
        # ⚠ ORDERING TRAP 1. A heal is reachable ONLY from the incremental
        # branch, so its fetch reason is ALWAYS `incremental`. Returning the
        # fetch reason first would silently label every heal as an ordinary
        # 3-bar correction — the branch with the opposite meaning.
        (True, False, FETCH_REASON_INCREMENTAL, REVISION_CAUSE_ADJUSTMENT_HEAL),
        # ⚠ ORDERING TRAP 2. `force_backfill` never consults
        # `_candles_fetch_count`, so it has no reason to report and arrives as
        # the empty string. Falling through to it would emit a blank cause key.
        (False, True, "", REVISION_CAUSE_FORCE_BACKFILL),
        # Unreachable today (`:733` requires `not force_backfill`), and resolved
        # rather than raised: a telemetry helper that can abort a refresh is a
        # worse failure than a mislabelled counter.
        (True, True, "", REVISION_CAUSE_ADJUSTMENT_HEAL),
    ],
)
def test_revision_cause_resolves_every_branch_in_priority_order(
    adjustment_detected: bool,
    force_backfill: bool,
    fetch_reason: str,
    expected: str,
) -> None:
    assert (
        revision_cause(
            adjustment_detected=adjustment_detected,
            force_backfill=force_backfill,
            fetch_reason=fetch_reason,
        )
        == expected
    )


def test_revision_cause_never_emits_an_empty_key_for_a_forced_run() -> None:
    """The blank fetch reason must not escape into a counter key.

    `bars_revised_by_cause` is read as a mapping of named causes; an empty key
    is indistinguishable on the admin surface from a missing one.
    """
    assert revision_cause(adjustment_detected=False, force_backfill=True, fetch_reason="") != ""


# ---------------------------------------------------------------------------
# _candles_fetch_count
# ---------------------------------------------------------------------------


def test_no_prior_bars_is_initial_backfill_not_stale() -> None:
    count, reason = _candles_fetch_count(_conn_returning(None), 1, default=1000, today=date(2026, 9, 15))
    assert (count, reason) == (1000, FETCH_REASON_INITIAL_BACKFILL)


def test_a_gap_inside_the_window_is_incremental() -> None:
    count, reason = _candles_fetch_count(_conn_returning(date(2026, 9, 12)), 1, default=1000, today=date(2026, 9, 15))
    assert (count, reason) == (_INCREMENTAL_FETCH_BARS, FETCH_REASON_INCREMENTAL)


def test_the_window_boundary_is_inclusive() -> None:
    """``gap_days > _INCREMENTAL_FETCH_BARS`` — exactly 3 stays incremental.

    Pinned because the reason now travels with the count: an off-by-one here
    would mislabel every boundary instrument as well as mis-sizing its fetch.
    """
    at_edge = _candles_fetch_count(_conn_returning(date(2026, 9, 12)), 1, default=1000, today=date(2026, 9, 15))
    past_edge = _candles_fetch_count(_conn_returning(date(2026, 9, 11)), 1, default=1000, today=date(2026, 9, 15))
    assert at_edge[1] == FETCH_REASON_INCREMENTAL
    assert past_edge[1] == FETCH_REASON_STALE_REOBSERVATION


def test_a_stale_gap_is_stale_reobservation_even_when_the_default_is_three() -> None:
    """⚠⚠ THE CASE THAT MADE THE OLD INFERENCE WRONG.

    The caller used to derive the reason by comparing the returned count against
    ``_INCREMENTAL_FETCH_BARS``. When ``lookback_days`` is itself 3, the
    stale-gap fallback returns 3 too — so the comparison reports a four-year
    re-observation as a 3-bar correction, which is the one confusion the whole
    attribution exists to prevent. The reason now comes from the function that
    made the decision, so the counts colliding no longer matters.
    """
    count, reason = _candles_fetch_count(
        _conn_returning(date(2025, 1, 1)),
        1,
        default=_INCREMENTAL_FETCH_BARS,
        today=date(2026, 9, 15),
    )
    assert count == _INCREMENTAL_FETCH_BARS
    assert reason == FETCH_REASON_STALE_REOBSERVATION
    # And the cause built from it is the deep one, not the shallow one.
    assert (
        revision_cause(adjustment_detected=False, force_backfill=False, fetch_reason=reason)
        == FETCH_REASON_STALE_REOBSERVATION
    )
