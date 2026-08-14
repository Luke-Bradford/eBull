"""§3.1 daily signal scan — the decisions, as pure tests.

Spec: ``docs/proposals/ta/2026-08-08-strategy-signal-scan.md``. Refs #2240, #2394.

Everything here runs without a database. The scan's DB half (frontier from the
corpus, the write, the watermark round-trip, the re-run no-op) is exercised
against the live dev corpus by ``scripts/verify_2394_signal_scan.py --all``,
which also removes what it wrote — a terminal ledger is not somewhere to leave
test rows.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.signal_ledger import LedgerRow
from app.services.strategies.s2_cross_sectional_momentum import S2_STRATEGY_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_registry import StrategySignal
from app.services.strategy_signal_scan import (
    FRONTIER_MODAL_SHARE_FLOOR,
    SCAN_UNIVERSE,
    Frontier,
    _PendingMember,
    _Plan,
    _resolve_cross_section,
    assert_census_complete,
    choose_frontier,
    write_window_indices,
)


def _bars(*prices: str) -> BarSeries:
    """A minimal ascending series — one bar per price, opens equal to closes."""
    return BarSeries(
        dates=tuple(date(2026, 1, day) for day in range(1, len(prices) + 1)),
        rows=tuple(
            {
                "open": Decimal(price),
                "high": Decimal(price),
                "low": Decimal(price),
                "close": Decimal(price),
                "volume": 1_000,
            }
            for price in prices
        ),
    )


class TestChooseFrontier:
    """The frontier is the MODAL last bar, never ``max(price_date)`` (spec §3)."""

    def test_modal_not_max(self) -> None:
        """The measured shape: a handful of instruments run ahead of the corpus.

        On the day the spec was written 7 instruments carried a bar at
        2026-08-08 and 5,783 did not. Keying on the maximum evaluates a date most
        of the universe is missing and manufactures thousands of terminal
        refusals out of a refresh still in flight.
        """
        last_bars = {i: date(2026, 8, 7) for i in range(20)}
        last_bars.update({100: date(2026, 8, 8), 101: date(2026, 8, 8)})
        frontier = choose_frontier(last_bars)
        assert frontier is not None
        assert frontier.bar_date == date(2026, 8, 7)
        assert (frontier.modal_count, frontier.loadable) == (20, 22)

    def test_ties_break_on_the_later_date(self) -> None:
        frontier = choose_frontier({1: date(2026, 8, 6), 2: date(2026, 8, 7)})
        assert frontier is not None
        assert frontier.bar_date == date(2026, 8, 7)

    def test_empty_population_is_none_not_an_exception(self) -> None:
        assert choose_frontier({}) is None

    def test_floor_is_exact_at_two_thirds(self) -> None:
        """⚠ The boundary is the case a floor exists for, so it is integer maths.

        A float ``2/3`` makes 2 of 3 a question about binary representation.
        """
        assert FRONTIER_MODAL_SHARE_FLOOR == (2, 3)
        assert Frontier(bar_date=date(2026, 8, 7), modal_count=2, loadable=3).meets_floor
        assert not Frontier(bar_date=date(2026, 8, 7), modal_count=1999, loadable=3000).meets_floor
        assert Frontier(bar_date=date(2026, 8, 7), modal_count=2000, loadable=3000).meets_floor


class TestWriteWindow:
    """Which of an instrument's bars this run may write (spec §2 + §3.1)."""

    def test_cold_start_writes_one_bar_not_the_history(self) -> None:
        """⚠⚠ Spec §11: the scan does not backfill.

        With no watermark the "strictly after" bound is vacuous, so an unbounded
        reading of the rule would write the instrument's ENTIRE history —
        deriving past signals from today's stored bars, which is the look-ahead
        phase 5 spent itself removing.
        """
        dates = tuple(date(2026, 1, day) for day in range(1, 11))
        assert list(write_window_indices(dates, watermark=None, frontier=date(2026, 1, 10))) == [8]

    def test_arrears_never_includes_the_last_bar(self) -> None:
        """The last bar has no ``t+1``; a decision there is unrewritable."""
        dates = tuple(date(2026, 1, day) for day in range(1, 6))
        for watermark in (None, date(2025, 12, 31), date(2026, 1, 1), date(2026, 1, 3)):
            window = write_window_indices(dates, watermark=watermark, frontier=date(2026, 1, 5))
            assert len(dates) - 1 not in window

    def test_same_frontier_rerun_writes_nothing(self) -> None:
        """Acceptance 2 — the no-op is the watermark's, not an ``ON CONFLICT``."""
        dates = tuple(date(2026, 1, day) for day in range(1, 6))
        # ⚠ The re-run no-op is `run_signal_scan`'s `watermark == frontier`
        # short-circuit, which never reaches this function. Here the frontier has
        # ALREADY moved past the watermark, which is the case that must write.
        assert list(write_window_indices(dates, watermark=date(2026, 1, 5), frontier=date(2026, 1, 5))) == []

    def test_a_gap_is_caught_up_whole(self) -> None:
        """Acceptance 3 — the next run with a moved frontier writes the gap.

        An instrument that missed sessions gets every unwritten bar in the
        window, not just the newest, so a gap does not silently drop a day of its
        record.
        """
        dates = tuple(date(2026, 1, day) for day in range(1, 11))
        window = write_window_indices(dates, watermark=date(2026, 1, 5), frontier=date(2026, 1, 10))
        assert list(window) == [4, 5, 6, 7, 8]

    def test_watermark_ahead_of_the_series_writes_nothing(self) -> None:
        dates = tuple(date(2026, 1, day) for day in range(1, 6))
        assert list(write_window_indices(dates, watermark=date(2026, 6, 1), frontier=date(2026, 1, 5))) == []

    def test_single_bar_series_has_no_write_date(self) -> None:
        assert list(write_window_indices((date(2026, 1, 1),), watermark=None, frontier=date(2026, 1, 1))) == []
        assert list(write_window_indices((), watermark=None, frontier=date(2026, 1, 1))) == []

    def test_a_bar_arriving_mid_scan_is_left_for_tomorrow(self) -> None:
        """⚠⚠ The Codex checkpoint-2 finding, pinned.

        ``daily_candle_refresh`` writes a new last bar between the span query and
        this instrument's load. The window must still be the bar before the
        FRONTIER — skipping the instrument would advance the watermark past a bar
        that was never written, and no later run could reach back for it.
        """
        dates = tuple(date(2026, 1, day) for day in range(1, 12))  # gained 2026-01-11
        window = write_window_indices(dates, watermark=date(2026, 1, 9), frontier=date(2026, 1, 10))
        assert [dates[index] for index in window] == [date(2026, 1, 9)]

    def test_the_day_after_a_completed_run_writes_the_watermark_bar_itself(self) -> None:
        """⚠⚠ ``>=``, not ``>`` — the bound spec §3.1 states as "strictly after".

        The watermark names the FRONTIER the last run completed, and that run
        wrote bars strictly BEFORE it. So the frontier bar is the first one still
        owed. Under ``>`` this window is empty and every run after the first
        writes nothing at all.
        """
        dates = tuple(date(2026, 1, day) for day in range(1, 6))
        window = write_window_indices(dates, watermark=date(2026, 1, 4), frontier=date(2026, 1, 5))
        assert [dates[index] for index in window] == [date(2026, 1, 4)]

    def test_consecutive_runs_write_each_bar_exactly_once(self) -> None:
        """The abutting-window property the ``>=`` bound rests on.

        Run N covers ``[lower, frontier_N)`` and run N+1 covers
        ``[frontier_N, frontier_N+1)``. Under a ledger with no ``ON CONFLICT`` a
        one-bar overlap is an aborted batch, and a one-bar gap is unrecoverable.
        """
        dates = tuple(date(2026, 1, day) for day in range(1, 8))
        written: list[date] = []
        watermark: date | None = None
        for frontier_index in range(4, len(dates)):
            frontier = dates[frontier_index]
            window = write_window_indices(dates, watermark=watermark, frontier=frontier)
            written.extend(dates[index] for index in window)
            watermark = frontier
        assert written == [date(2026, 1, 4), date(2026, 1, 5), date(2026, 1, 6)]
        assert len(set(written)) == len(written)

    def test_a_series_that_fell_behind_still_stops_short_of_its_own_last_bar(self) -> None:
        """The opposite movement: the frontier bound must not override the arrears one."""
        dates = tuple(date(2026, 1, day) for day in range(1, 8))  # last bar 2026-01-07
        window = write_window_indices(dates, watermark=date(2026, 1, 5), frontier=date(2026, 1, 10))
        assert [dates[index] for index in window] == [date(2026, 1, 5), date(2026, 1, 6)]


class TestCensusGate:
    """Spec §9 — a short leg fails the run rather than under-reporting it."""

    def _census(self, entry_kinds: tuple[str, ...], count: int) -> dict:
        return {kind: {(kind, "not_fired", ""): count} for kind in entry_kinds}

    def test_complete_census_passes(self) -> None:
        entry = STRATEGY_MANIFEST["s1-time-series-momentum"]
        assert_census_complete(
            entry, self._census(("entry", "exit"), 7), 7, instruments_evaluated=7, eligible_instruments=7
        )

    def test_short_leg_raises(self) -> None:
        """A leg missing rows means an eligible instrument produced no verdict.

        Nothing downstream can see that: an absent row indexes to nothing, so
        zero coverage is only detectable against the expected count.
        """
        entry = STRATEGY_MANIFEST["s1-time-series-momentum"]
        census = self._census(("entry", "exit"), 7)
        census["exit"] = {("exit", "not_fired", ""): 6}
        with pytest.raises(RuntimeError, match="exit censused 6 rows against 7"):
            assert_census_complete(entry, census, 7, instruments_evaluated=7, eligible_instruments=7)

    def test_undeclared_leg_raises(self) -> None:
        """The mirror of a short leg: rows nothing filtering on the manifest reads."""
        entry = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
        with pytest.raises(RuntimeError, match="emitted \\['exit'\\]"):
            assert_census_complete(
                entry, self._census(("entry", "exit"), 3), 3, instruments_evaluated=3, eligible_instruments=3
            )

    def test_an_unevaluated_eligible_instrument_raises(self) -> None:
        """⚠⚠ The check ``expected_per_leg`` CANNOT make.

        ``expected_per_leg`` is summed over the windows the scan computed, so an
        instrument the loader could not return lowers the expectation and the row
        count together and the census agrees with itself. Only the eligible
        population — counted before any bar was loaded — sees the hole.
        """
        entry = STRATEGY_MANIFEST["s1-time-series-momentum"]
        with pytest.raises(RuntimeError, match="evaluated 6 instruments against 7 eligible"):
            assert_census_complete(
                entry, self._census(("entry", "exit"), 6), 6, instruments_evaluated=6, eligible_instruments=7
            )

    def test_an_empty_window_is_not_a_hole(self) -> None:
        """⚠ The review's WARNING on the first version, pinned.

        An instrument rejoining the frontier after a stale spell has no bar at or
        after the watermark to write, and a series that shrank mid-scan may be
        fully caught up. Both evaluate fine and produce nothing, and gating on
        "produced a row" would abort a healthy batch for either.
        """
        entry = STRATEGY_MANIFEST["s1-time-series-momentum"]
        assert_census_complete(
            entry, self._census(("entry", "exit"), 5), 5, instruments_evaluated=7, eligible_instruments=7
        )


class TestCrossSectionalResolution:
    """S-2's deferred verdicts, and the trimmed slice they are resolved against.

    ⚠⚠ THE SLICE IS THE PART WORTH TESTING. A member's decision-bar verdict is
    unknowable until every member has been staged, by which point the streaming
    pass has moved off that instrument — so the scan keeps the window bars plus
    the ONE bar after them, and re-bases the signal indices onto it. If the
    re-basing is off by one, ``resolve_fills`` books the fill from the wrong bar
    and the row is wrong-but-plausible, which nothing downstream can detect.
    """

    def _plan(self) -> _Plan:
        entry = STRATEGY_MANIFEST[S2_STRATEGY_ID]
        identity = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID)
        return _Plan(entry=entry, identity=identity, version=identity.version, watermark=None)

    def _pending(self) -> _PendingMember:
        # Window bar 2026-01-01, then its fill bar 2026-01-02. This is exactly
        # what a one-bar window slices to: `series[-2:]`.
        return _PendingMember(
            series=_bars("10", "11"),
            window_dates=(date(2026, 1, 1),),
            decided={},
            participating=frozenset({date(2026, 1, 1)}),
            admissible_dates=None,
            mandatory_dates=None,
        )

    def test_winner_fills_at_the_next_bar_open(self) -> None:
        plan = self._plan()
        out: list[LedgerRow] = []
        scores = {date(2026, 1, 1): {instrument: float(instrument) for instrument in range(1, 21)}}
        _resolve_cross_section(plan, leg="entry", pending={20: self._pending()}, scores=scores, out=out)
        assert len(out) == 1
        row = out[0]
        assert (row.verdict, row.signal_bar_date) == ("fired", date(2026, 1, 1))
        # ⚠ The fill is the NEXT bar's OPEN — 11, the second bar of the slice —
        # not the signal bar's own open (10). An off-by-one in the re-basing
        # shows up here and nowhere else.
        assert (row.fill_bar_date, row.fill_price) == (date(2026, 1, 2), Decimal("11"))

    def test_thin_cross_section_is_not_fired(self) -> None:
        """Criterion 8: a panel of six has no decile to be in the top of.

        Reporting ``not_fired`` would be the exact prohibition — a
        data-availability fact wearing a rule verdict's clothes.
        """
        plan = self._plan()
        out: list[LedgerRow] = []
        scores = {date(2026, 1, 1): {1: 1.0, 20: 2.0}}
        _resolve_cross_section(plan, leg="entry", pending={20: self._pending()}, scores=scores, out=out)
        assert (out[0].verdict, out[0].not_evaluable_reason) == ("not_evaluable", "thin_cross_section")
        assert (out[0].fill_bar_date, out[0].fill_price) == (None, None)

    def test_non_decision_bar_keeps_its_staged_verdict(self) -> None:
        """*"Everything else is an ordinary not_fired … It is a verdict, not an

        absence."* So S-2 writes for the whole eligible population every scan
        day, with a handful of rebalance days a year carrying any ``fired``.
        """
        plan = self._plan()
        pending = _PendingMember(
            series=_bars("10", "11"),
            window_dates=(date(2026, 1, 1),),
            decided={date(2026, 1, 1): StrategySignal(verdict="not_fired", signal_index=0)},
            participating=frozenset(),
            admissible_dates=None,
            mandatory_dates=None,
        )
        out: list[LedgerRow] = []
        _resolve_cross_section(plan, leg="entry", pending={20: pending}, scores={}, out=out)
        assert (out[0].verdict, out[0].signal_bar_date) == ("not_fired", date(2026, 1, 1))

    def test_a_selector_naming_a_non_participant_raises(self) -> None:
        """Mirrors ``evaluate_cross_sectional``: honouring it would fire a signal
        on a bar the runner already judged unevaluable."""
        entry = STRATEGY_MANIFEST[S2_STRATEGY_ID]
        identity = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID)
        rogue = _Plan(
            entry=type(entry)(
                strategy_id=entry.strategy_id,
                purpose=entry.purpose,
                identity=entry.identity,
                strategy_class="cross_sectional",
                signal_kinds=entry.signal_kinds,
                exit_regime=entry.exit_regime,
                decision_calendar=entry.decision_calendar,
                member=entry.member,
                select=lambda when, scores: frozenset({999_999}),
                min_participants=entry.min_participants,
            ),
            identity=identity,
            version=identity.version,
            watermark=None,
        )
        scores = {date(2026, 1, 1): {instrument: float(instrument) for instrument in range(1, 21)}}
        with pytest.raises(ValueError, match="did not participate"):
            _resolve_cross_section(rogue, leg="entry", pending={20: self._pending()}, scores=scores, out=[])
