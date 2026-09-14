"""#2833 verdict-bound SQL, exercised against a real Postgres.

Separate module ON PURPOSE. `tests/conftest.py::pytest_collection_modifyitems` applies
the `db` marker per MODULE, so a single DB test living beside the pure ones would evict
all 21 of them from the `-m "not db"` push gate. Keeping it here leaves the pure file
on the gate and this file on the deliberate `-m db` run.
"""

from datetime import UTC, date, datetime, time

import psycopg
import pytest

from app.services.strategy_core_quote_observation import SOURCE
from app.services.strategy_core_selection import load_core_selection


@pytest.mark.db
def test_the_window_close_date_is_the_fifth_common_date_not_the_latest(
    ebull_test_conn: psycopg.Connection[tuple],
    two_seeded_instrument_ids: tuple[int, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one mechanism a mocked row cannot check, and where the real bug was.

    Codex checkpoint 2 caught the first draft anchoring a closed window on
    ``max(common_dates)``: with a sixth observation, "Earliest verdict" would advance
    past a boundary that had already opened. The fix is an ``OFFSET`` subquery, so the
    guard has to execute the SQL -- every other test here feeds the rows in by hand and
    would pass against either query.
    """
    monkeypatch.setattr(
        "app.services.strategy_core_selection.CORE_SELECTION_CANDIDATE_IDS",
        two_seeded_instrument_ids,
    )
    # Six common weekday dates: Mon 14th through Mon 21st September 2026.
    common = [date(2026, 9, d) for d in (14, 15, 16, 17, 18, 21)]
    for instrument_id in two_seeded_instrument_ids:
        for day in common:
            bucket = datetime.combine(day, time(15, 0), tzinfo=UTC)
            # `strategy_core_quote_observation_shape` requires the whole quote on an
            # `observed` row, and `source` is pinned by regex -- so the real SOURCE
            # constant is used rather than a literal that would drift from it.
            ebull_test_conn.execute(
                "INSERT INTO strategy_core_quote_observations "
                "(instrument_id, sample_bucket, observed_at, quote_at, bid, ask, spread_bps, "
                " observation_status, source) "
                "VALUES (%s, %s, %s, %s, 100, 100.02, 2, 'observed', %s)",
                (instrument_id, bucket, bucket, bucket, SOURCE),
            )
    ebull_test_conn.commit()

    selection = load_core_selection(ebull_test_conn, now=datetime(2026, 9, 21, 20, 0, tzinfo=UTC))

    assert selection.observed_trading_days == 6
    # The FIFTH common date is 2026-09-18, so the boundary is the 19th. `max()` would
    # give 2026-09-21 and a boundary of the 22nd -- three days after the verdict opened.
    assert selection.earliest_possible_verdict_at == datetime(2026, 9, 19, tzinfo=UTC)
