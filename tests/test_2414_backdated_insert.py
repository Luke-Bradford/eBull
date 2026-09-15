"""#2414 item 2 — the pure half of backdated-insert classification.

No DB: the classification is a comparison inside ``_upsert_candles``, and the only
DB behaviour it depends on (``RETURNING (xmax = 0)`` telling an INSERT from a DO
UPDATE) is already pinned by the revision suite against a real cluster. What is
NOT pinned anywhere else, and is what actually goes wrong, is the *frontier
policy*: fixed vs advancing, and the ``None`` case.

The DB-backed end lives in ``tests/test_market_data_backdated_insert_record_db.py``.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.providers.market_data import OHLCVBar
from app.services import market_data
from app.services.market_data import RevisionCause, _upsert_candles

_REF = date(2026, 1, 30)


def _bar(price_date: date, close: str = "100") -> OHLCVBar:
    c = Decimal(close)
    return OHLCVBar(price_date=price_date, open=c, high=c, low=c, close=c, volume=1000)


def _conn(was_insert: list[bool | None]) -> MagicMock:
    """A conn whose successive ``RETURNING`` rows are scripted.

    ``True`` = the bar was inserted, ``False`` = it overwrote a stored value,
    ``None`` = the ``IS DISTINCT FROM`` guard blocked it (no row returned).
    """
    conn = MagicMock()
    cursors = []
    for flag in was_insert:
        cur = MagicMock()
        cur.fetchone.return_value = None if flag is None else (flag,)
        cursors.append(cur)
    conn.execute.side_effect = cursors
    return conn


def test_no_prior_history_makes_no_bar_backdated() -> None:
    """The initial-backfill case, and the reason it is not merely "rare".

    A 1000-bar seed of a brand-new instrument is 1000 inserts and must produce
    ZERO rows: nothing had been committed, so nothing had been decided against.
    Counting them would make the table's first sweep report ~12k instruments'
    entire history as a corpus movement.
    """
    days = [date(2026, 1, 5) + timedelta(days=i) for i in range(4)]
    outcome = _upsert_candles(
        _conn([True] * 4),
        1,
        [_bar(d) for d in days],
        reference_date=_REF,
        frontier_before=None,
    )
    assert outcome.inserted == 4
    assert outcome.backdated_insert_dates == ()


def test_a_bar_below_the_frontier_is_backdated_and_one_above_is_not() -> None:
    frontier = date(2026, 1, 20)
    bars = [_bar(date(2026, 1, 10)), _bar(date(2026, 1, 25))]
    outcome = _upsert_candles(_conn([True, True]), 1, bars, reference_date=_REF, frontier_before=frontier)
    assert outcome.inserted == 2
    assert outcome.backdated_insert_dates == (date(2026, 1, 10),)


def test_a_bar_on_the_frontier_cannot_be_an_insert_so_is_never_backdated() -> None:
    """Equality is unreachable in production, and the code must not rely on that.

    A bar dated exactly on the frontier CONFLICTS, so the real writer gets a
    revision or a no-op. This drives the impossible case anyway — scripted as an
    insert — to pin that the comparison is strict: a frontier bar reclassified as
    backdated would make every ordinary incremental fetch emit a row.
    """
    frontier = date(2026, 1, 20)
    outcome = _upsert_candles(_conn([True]), 1, [_bar(frontier)], reference_date=_REF, frontier_before=frontier)
    assert outcome.inserted == 1
    assert outcome.backdated_insert_dates == ()


def test_the_frontier_is_fixed_and_not_advanced_by_bars_landing_in_this_call() -> None:
    """The policy decision, and the ONLY test that can catch it going the other way.

    Payload order ``[frontier + 3, frontier + 1]``: under a FIXED frontier both are
    forward extensions and nothing is recorded. Under a frontier advanced as bars
    land, the first insert moves it to ``+3`` and ``+1`` is then "backdated" — so
    the table would be recording provider PAYLOAD ORDER, which is not a property
    of the corpus. Nothing inside an uncommitted transaction is visible to any
    reader, so a bar that arrives second was never behind committed history.

    ⚠ A ``[frontier + 3, frontier − 1]`` payload cannot detect this: the second
    bar is backdated under BOTH policies. That was the first draft of this test
    and it would have passed against the bug.
    """
    frontier = date(2026, 1, 20)
    bars = [_bar(frontier + timedelta(days=3)), _bar(frontier + timedelta(days=1))]
    outcome = _upsert_candles(_conn([True, True]), 1, bars, reference_date=_REF, frontier_before=frontier)
    assert outcome.inserted == 2
    assert outcome.backdated_insert_dates == ()


def test_the_frontier_is_not_advanced_from_an_empty_series_either() -> None:
    """The ``None`` companion to the case above.

    An advancing implementation seeded from ``None`` would set the frontier to the
    first inserted bar and call every LOWER bar in the same payload backdated.
    """
    bars = [_bar(date(2026, 1, 25)), _bar(date(2026, 1, 10))]
    outcome = _upsert_candles(_conn([True, True]), 1, bars, reference_date=_REF, frontier_before=None)
    assert outcome.backdated_insert_dates == ()


def test_revisions_and_no_ops_are_never_counted_as_backdated_inserts() -> None:
    """The two non-insert outcomes, below the frontier, where the bug would hide.

    A revision below the frontier is the ``price_daily_revision`` class and must
    not appear in both logs; a guard-blocked no-op mutated nothing at all.
    """
    frontier = date(2026, 1, 20)
    bars = [_bar(date(2026, 1, 8)), _bar(date(2026, 1, 9)), _bar(date(2026, 1, 10))]
    outcome = _upsert_candles(
        # revision, no-op, insert — all three below the frontier.
        _conn([False, None, True]),
        1,
        bars,
        reference_date=_REF,
        frontier_before=frontier,
    )
    assert outcome.revised == 1
    assert outcome.inserted == 1
    assert outcome.revised_bar_dates == (date(2026, 1, 8),)
    assert outcome.backdated_insert_dates == (date(2026, 1, 10),)


def test_backdated_dates_are_a_subset_of_the_inserted_count() -> None:
    """The conservation law between the two insert kinds.

    ``len(backdated_insert_dates) <= inserted`` must hold for every input, or the
    census can report more backdated inserts than there were inserts.
    """
    frontier = date(2026, 1, 20)
    for flags, dates in (
        ([True, True, True], [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]),
        ([True, False, None], [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]),
        ([None, None], [date(2026, 1, 1), date(2026, 2, 1)]),
    ):
        outcome = _upsert_candles(
            _conn(list(flags)),
            1,
            [_bar(d) for d in dates],
            reference_date=_REF,
            frontier_before=frontier,
        )
        assert len(outcome.backdated_insert_dates) <= outcome.inserted


def test_recording_nothing_issues_no_statement() -> None:
    """An empty list must not reach ``executemany`` at all.

    Same guard the revision writer has. Without it, every one of ~12k instruments
    on an ordinary sweep pays a round trip to insert zero rows.
    """
    conn = MagicMock()
    market_data._record_backdated_inserts(conn, 1, [], frontier_before=date(2026, 1, 20), cause="incremental")
    conn.cursor.assert_not_called()


def test_the_write_branch_vocabulary_is_shared_with_the_revision_log() -> None:
    """The two audit tables must agree on `cause`, and a member added to one
    must not silently become unstoreable in the other.

    ``sql/388``'s CHECK is a copy of ``sql/387``'s, so this pins the Python side
    of both against one list. The SQL side is pinned by
    ``test_2414_revision_cause.py``'s sibling assertion plus the migration's own
    CHECK rejecting an unknown value at write time.
    """
    from typing import get_args

    assert set(get_args(RevisionCause)) == {
        "initial_backfill",
        "stale_reobservation",
        "incremental",
        "adjustment_heal",
        "force_backfill",
        "unknown",
    }


@pytest.mark.parametrize("cause", ["initial_backfill", "incremental", "force_backfill"])
def test_every_cause_is_storable_for_a_backdated_insert(cause: str) -> None:
    """``initial_backfill`` included, and that is the point.

    It LOOKS impossible — no prior history means no frontier means no backdated
    bar — and it is only race-impossible, not impossible: ``_candles_fetch_count``
    and the frontier read are separate statements, so a concurrent writer
    committing history between them produces exactly this pair. The writer must
    not refuse it, because refusing would abort a price write over a telemetry
    disagreement.
    """
    conn = MagicMock()
    typed: Any = cause
    market_data._record_backdated_inserts(conn, 1, [date(2026, 1, 10)], frontier_before=date(2026, 1, 20), cause=typed)
    conn.cursor.assert_called_once()


def test_an_impossible_classification_keeps_the_bars_and_drops_the_audit_rows(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Review round 1: the invariant is RESOLVED, not asserted, and not raised.

    ``backdated_insert_dates`` non-empty with ``frontier_before is None`` is
    unreachable — ``_upsert_candles`` can only classify a bar as backdated by
    comparing against a frontier. Three ways to handle the unreachable state, and
    only one is right:

    * bare ``assert`` — stripped under ``-O``, so ``None`` reaches a NOT NULL
      column (the review's finding);
    * ``raise`` — inside the bar write's transaction, so a telemetry disagreement
      destroys a price write, which is the exact thing ``sql/388``'s header
      refuses a cross-column CHECK for;
    * log at ERROR and skip the audit write — bars kept, nothing silent.

    This pins the third, because the second is the tempting fix for the first.
    """
    import logging

    from app.services.market_data import refresh_market_data

    conn = MagicMock()
    cursor = MagicMock()
    # Drives `_candles_are_fresh` (stale -> fetch) AND `_observed_frontier`, which
    # returns None here: the impossible pairing this test exists to reach.
    cursor.fetchone.side_effect = [(date(2020, 1, 1),), (date(2020, 1, 1),), (None,), (None,)] + [(None,)] * 20
    conn.execute.return_value = cursor
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=ctx)
    ctx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = ctx

    provider = MagicMock()
    provider.get_daily_candles.return_value = [MagicMock()]

    outcome = market_data.CandleUpsertOutcome(
        inserted=1,
        revised=0,
        revision_age_days={},
        revision_max_age_days=None,
        revised_bar_dates=(),
        backdated_insert_dates=(date(2019, 12, 31),),
    )
    with (
        pytest.MonkeyPatch.context() as mp,
        caplog.at_level(logging.ERROR, logger="app.services.market_data"),
    ):
        mp.setattr(market_data, "_upsert_candles", lambda *a, **k: outcome)
        mp.setattr(market_data, "_observed_frontier", lambda *a, **k: None)
        mp.setattr(market_data, "_compute_and_store_features", lambda *a, **k: 0)
        recorded: list[object] = []
        mp.setattr(market_data, "_record_backdated_inserts", lambda *a, **k: recorded.append(a))
        summary = refresh_market_data(provider, conn, instruments=[(42, "AAPL")], skip_quotes=True)

    assert recorded == [], "the audit write must be skipped, not fed a None frontier"
    # The bars survived: the instrument is NOT counted as failed.
    assert summary.candles_failed == 0
    assert any("#2414" in r.getMessage() for r in caplog.records), "the drop must not be silent"
