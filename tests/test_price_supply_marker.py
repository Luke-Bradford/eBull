"""The "eligible but supply-less" signal (#2262).

⚠⚠ eToro returns HTTP 200 WITH NOTHING NEW for a supply-less instrument. It does
not error, does not 404, does not raise. So a marker keyed on HTTP status or on
an exception never fires for any of the ~108 affected instruments, and the ONLY
observable is whether ``MAX(price_date)`` moved. ``series_advanced`` is that
whole signal, which is why it is a pure function with a table test rather than a
line buried in the refresh loop.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.market_data import series_advanced

_D1 = date(2026, 8, 3)
_D2 = date(2026, 8, 4)


class TestSeriesAdvanced:
    @pytest.mark.parametrize(
        ("before", "after", "expected"),
        [
            # The 200-with-nothing case: fetched, series unchanged. This is the
            # 81 PRICED instruments S6's probe could not see, because it sampled
            # the unpriced set.
            (_D1, _D1, False),
            # A real advance resets the counter.
            (_D1, _D2, True),
            # First bar ever — the 27 unpriced gate-passers, when supply exists.
            (None, _D2, True),
            # Still nothing at all: the 27 when supply does not exist.
            (None, None, False),
            # Series went backwards (a corrective delete, or a rewritten
            # history). Not an advance — hence `>` and not `!=`. Reading it as
            # an advance would reset the counter and hide a supply-less name
            # forever.
            (_D2, _D1, False),
            # Bars vanished entirely. Also not an advance.
            (_D2, None, False),
        ],
    )
    def test_table(self, before: date | None, after: date | None, expected: bool) -> None:
        assert series_advanced(before, after) is expected


class TestExclusionExpires:
    """The supply-less exclusion must EXPIRE, not LATCH.

    A job scope predicate that permanently drops an instrument once it looks
    supply-less is a seeder — the #2254 defect in a new place. A relisted or
    newly-supplied instrument would never come back on its own, and nothing
    would report that it had been dropped.
    """

    def test_scope_sql_rechecks_on_an_interval(self) -> None:
        from app.workers.scheduler import _T3_CANDLE_SELECT, _T3_SUPPLY_LESS_RECHECK

        assert "supply_recheck" in _T3_CANDLE_SELECT
        assert "last_attempt_at <" in _T3_CANDLE_SELECT
        assert _T3_SUPPLY_LESS_RECHECK == "7 days"

    def test_scope_sql_admits_instruments_with_no_supply_row_yet(self) -> None:
        # LEFT JOIN + `s.instrument_id IS NULL`: an instrument never attempted
        # must not be excluded by a marker it cannot have.
        from app.workers.scheduler import _T3_CANDLE_SELECT

        assert "s.instrument_id IS NULL" in _T3_CANDLE_SELECT


class TestSeedGateIsPriceShaped:
    """Design decision 9 — price eligibility is defined on the PRICE path.

    ``coverage_tier`` and ``fundamentals_snapshot`` are SEC-fed, so a seeding
    gate keyed on them makes the price universe US-filer-only while presenting
    as "the market".
    """

    def test_seed_arm_no_longer_reads_fundamentals(self) -> None:
        from app.workers.scheduler import _T3_CANDLE_SELECT

        assert "fundamentals_snapshot" not in _T3_CANDLE_SELECT

    def test_unknown_asset_class_stays_gated(self) -> None:
        # The one thing this narrowing still rejects (194 instruments): the
        # operator curates the exchange row first, via #503 PR 4.
        from app.workers.scheduler import _T3_CANDLE_SELECT

        assert "e.asset_class IS NOT NULL" in _T3_CANDLE_SELECT
        assert "e.asset_class <> 'unknown'" in _T3_CANDLE_SELECT

    def test_batch_cap_covers_the_admitted_population(self) -> None:
        # Measured 2026-08-04 on the full dev population: the scope query
        # returns 10,483 rows under the new predicate. A cap below that binds on
        # the first run and leaves an unknown remainder stale while the run
        # still reports success.
        from app.workers.scheduler import _T3_CANDLE_BATCH_SIZE

        assert _T3_CANDLE_BATCH_SIZE >= 10_483
