"""#2414 — the pure half of revision-cause attribution.

No DB: ``revision_cause`` is a pure function. Its input ``fetch_reason`` comes
from ``_candles_fetch_count``, whose own tests stay with the rest of that
function's suite in ``tests/test_market_data.py::TestCandlesFetchCount`` rather
than being restated here. The branch-attribution end of this lives in
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
    FETCH_REASON_INCREMENTAL,
    FETCH_REASON_INITIAL_BACKFILL,
    FETCH_REASON_STALE_REOBSERVATION,
    REVISION_CAUSE_ADJUSTMENT_HEAL,
    REVISION_CAUSE_FORCE_BACKFILL,
    REVISION_CAUSE_UNKNOWN,
    FetchReason,
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
        (False, True, None, REVISION_CAUSE_FORCE_BACKFILL),
        # Unreachable today (`:733` requires `not force_backfill`), and resolved
        # rather than raised: a telemetry helper that can abort a refresh is a
        # worse failure than a mislabelled counter.
        (True, True, None, REVISION_CAUSE_ADJUSTMENT_HEAL),
        # A non-forced fetch with no reason is unreachable from the one call
        # site, and is NAMED rather than guessed: it must surface in the census
        # as unattributed, never be folded into a real cause.
        (False, False, None, REVISION_CAUSE_UNKNOWN),
    ],
)
def test_revision_cause_resolves_every_branch_in_priority_order(
    adjustment_detected: bool,
    force_backfill: bool,
    fetch_reason: FetchReason | None,
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


def test_no_input_combination_produces_an_empty_or_unnamed_key() -> None:
    """Every result must be a NAMED cause.

    `bars_revised_by_cause` is read as a mapping of named causes, so an empty or
    unrecognised key is indistinguishable on the admin surface from a cause that
    genuinely did not fire. The `Literal` return type makes a typo a pyright
    error; this pins the runtime side over the whole input space.
    """
    named = {
        FETCH_REASON_INCREMENTAL,
        FETCH_REASON_STALE_REOBSERVATION,
        FETCH_REASON_INITIAL_BACKFILL,
        REVISION_CAUSE_ADJUSTMENT_HEAL,
        REVISION_CAUSE_FORCE_BACKFILL,
        REVISION_CAUSE_UNKNOWN,
    }
    reasons: list[FetchReason | None] = [
        FETCH_REASON_INCREMENTAL,
        FETCH_REASON_STALE_REOBSERVATION,
        FETCH_REASON_INITIAL_BACKFILL,
        None,
    ]
    for adjustment in (True, False):
        for forced in (True, False):
            for reason in reasons:
                cause = revision_cause(
                    adjustment_detected=adjustment,
                    force_backfill=forced,
                    fetch_reason=reason,
                )
                assert cause in named, (adjustment, forced, reason, cause)
