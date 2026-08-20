"""Phase 5a — position construction, as pure logic.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §3. The
full-population figures — position counts, the ``superseded_open_position``
share, the close-source distribution — live in
``scripts/verify_2240_position_builder.py``, because a hand-copied statistic
goes stale in the place a reader trusts most.

⚠ DB-free by design. ``build_positions`` reads no database; the repo's stated
default is to extract the decision into a pure function and table-test it, and
there is no new SQL mechanism here to integration-test.
"""

from __future__ import annotations

import hashlib
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.indicator_series import BarSeries
from app.services.position_builder import (
    CLOSE_SOURCES,
    OPEN_REASONS,
    OUR_ADDITIONAL_OPEN_REASONS,
    RULE_SET_ID,
    RULE_SET_VERSION,
    SPEC_OPEN_REASONS,
    EntryFill,
    ExitFill,
    ExitRegime,
    OutcomePin,
    Position,
    PositionSet,
    ResolvedOutcome,
    Window,
    build_positions,
)
from app.services.strategies.s3_mean_reversion_in_trend import MAX_HOLD_BARS as S3_MAX_HOLD_BARS
from app.services.strategies.s4_volatility_compression_breakout import MAX_HOLD_BARS as S4_MAX_HOLD_BARS
from app.services.technical_analysis import OHLCVRow

_D0 = date(2026, 1, 5)
_INSTRUMENT = 42
_PIN = OutcomePin(rule_set_version="outcome-resolver-v1+abc", input_rule_set_version="price-quarantine-v1+def")


def _d(index: int) -> date:
    return _D0 + timedelta(days=index)


def _bar(open_: Decimal | None, close: Decimal | None) -> OHLCVRow:
    """One bar. ⚠ BOTH arguments are required and both accept ``None``: the
    columns are NULLABLE and ``load_masked_series`` deliberately writes ``None``
    into a masked field, so a default would make "not given" and "masked"
    indistinguishable — which is the defect this file tests for elsewhere."""
    return {
        "open": open_,  # type: ignore[typeddict-item]
        "high": open_,  # type: ignore[typeddict-item]
        "low": open_,  # type: ignore[typeddict-item]
        "close": close,  # type: ignore[typeddict-item]
        "volume": 1_000,
    }


def _series(
    n: int = 20,
    *,
    opens: dict[int, Decimal | None] | None = None,
    closes: dict[int, Decimal | None] | None = None,
) -> BarSeries:
    """``n`` bars whose open and close are both ``100 + index``, with overrides."""
    overrides_open = opens or {}
    overrides_close = closes or {}
    rows = [
        _bar(
            overrides_open[index] if index in overrides_open else Decimal(100 + index),
            overrides_close[index] if index in overrides_close else Decimal(100 + index),
        )
        for index in range(n)
    ]
    return BarSeries(dates=tuple(_d(i) for i in range(n)), rows=tuple(rows))


def _entry(signal: int, fill_index: int, *, instrument: int = _INSTRUMENT) -> EntryFill:
    return EntryFill(
        signal_id=signal,
        instrument_id=instrument,
        signal_bar_date=_d(fill_index - 1),
        fill_bar_date=_d(fill_index),
        fill_price=Decimal(100 + fill_index),
    )


def _exit(fill_index: int, *, instrument: int = _INSTRUMENT) -> ExitFill:
    return ExitFill(
        instrument_id=instrument,
        signal_bar_date=_d(fill_index - 1),
        fill_bar_date=_d(fill_index),
        fill_price=Decimal(100 + fill_index),
    )


_SIGNAL_PAIR = ExitRegime(signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=None)


def _build(
    *,
    entries: list[EntryFill],
    exits: list[ExitFill] | None = None,
    outcomes: list[ResolvedOutcome] | None = None,
    outcome_pin: OutcomePin | None = None,
    series: dict[int, BarSeries] | None = None,
    regime: ExitRegime = _SIGNAL_PAIR,
    window: Window | None = None,
) -> PositionSet:
    # ⚠ `is None`, not `or`: an EMPTY series map is a real test case (the
    # builder must refuse it) and `series or {...}` would substitute the
    # default for it.
    if series is None:
        series = {_INSTRUMENT: _series()}
    return build_positions(
        strategy_id="s1-time-series-momentum",
        strategy_version="strategy-registry-v1+deadbeef",
        entries=entries,
        exits=exits or [],
        outcomes=outcomes or [],
        outcome_pin=outcome_pin,
        series=series,
        regime=regime,
        window=window or Window(start=_d(0), end=_d(19)),
    )


class TestSignalPairClose:
    def test_c1_closes_at_the_next_exit_fill(self) -> None:
        built = _build(entries=[_entry(1, 2)], exits=[_exit(5)])
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date, position.close_price) == (
            "signal_pair",
            _d(5),
            Decimal(105),
        )
        assert position.bars_held == 3
        assert position.open_reason is None and position.mark_price is None

    def test_an_exit_before_the_entry_does_not_close_it(self) -> None:
        built = _build(entries=[_entry(1, 5)], exits=[_exit(2)])
        (position,) = built.positions
        assert position.close_source is None and position.open_reason == "window_end"

    def test_an_exit_on_the_entry_fill_bar_does_not_close_it(self) -> None:
        """⚠ C1 is STRICTLY after (§3.2): an exit sharing the entry's own fill
        bar closes the OLDER position, never the one opened that bar."""
        built = _build(entries=[_entry(1, 5)], exits=[_exit(5), _exit(9)])
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date) == ("signal_pair", _d(9))


class TestPyramidingIsCollapsed:
    def test_entries_while_open_are_superseded_and_counted(self) -> None:
        """§3.1 — entries are STATES, so a trend fires one every bar."""
        built = _build(entries=[_entry(n, n) for n in (2, 3, 4, 5)], exits=[_exit(8)])
        assert len(built.positions) == 1
        assert built.superseded_open_position == 3
        assert built.positions[0].entry_fill_bar_date == _d(2)

    def test_an_entry_on_the_close_bar_opens_a_new_position(self) -> None:
        """⚠ §3.2 rule 4 — same-bar ordering is exit BEFORE entry. The exit at
        bar 5 closes the older hold; the entry filling at bar 5 opens a new
        one, which then takes the NEXT exit."""
        built = _build(entries=[_entry(1, 2), _entry(2, 5)], exits=[_exit(5), _exit(9)])
        assert [(p.entry_fill_bar_date, p.close_bar_date) for p in built.positions] == [
            (_d(2), _d(5)),
            (_d(5), _d(9)),
        ]
        assert built.superseded_open_position == 0

    def test_an_open_position_suppresses_every_later_entry(self) -> None:
        built = _build(entries=[_entry(1, 2), _entry(2, 9)], exits=[])
        assert len(built.positions) == 1
        assert built.superseded_open_position == 1

    def test_a_second_instrument_holds_its_own_position(self) -> None:
        built = _build(
            entries=[_entry(1, 2), _entry(2, 3, instrument=7)],
            exits=[],
            series={_INSTRUMENT: _series(), 7: _series()},
        )
        assert {p.instrument_id for p in built.positions} == {_INSTRUMENT, 7}
        assert built.superseded_open_position == 0


class TestLevelClose:
    def _level_regime(self, *, max_hold: int | None = 4) -> ExitRegime:
        return ExitRegime(signal_pair=False, level_based=True, max_hold_bars=max_hold, rebalance_dates=None)

    def _outcome(
        self,
        outcome: str,
        *,
        at: int | None,
        price: Decimal | None,
        signal: int = 1,
        reason: str | None = None,
        unresolved_until: int | None = None,
    ) -> ResolvedOutcome:
        return ResolvedOutcome(
            signal_id=signal,
            rule_set_version=_PIN.rule_set_version,
            input_rule_set_version=_PIN.input_rule_set_version,
            outcome=outcome,  # type: ignore[arg-type]
            exit_bar_date=None if at is None else _d(at),
            exit_price=price,
            reason=("missing_bar_data" if outcome == "unresolved" and reason is None else reason),  # type: ignore[arg-type]
            unresolved_until_bar_date=None if unresolved_until is None else _d(unresolved_until),
        )

    def test_tp_hit_closes_at_the_stored_exit(self) -> None:
        built = _build(
            entries=[_entry(1, 2)],
            outcomes=[self._outcome("tp_hit", at=4, price=Decimal("110"))],
            outcome_pin=_PIN,
            regime=self._level_regime(),
        )
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date, position.close_price) == (
            "level",
            _d(4),
            Decimal("110"),
        )
        assert position.bars_held == 2

    def test_a_level_touched_on_the_fill_bar_is_zero_bars_held(self) -> None:
        """``sql/256`` says ``bars_held = 0`` is legal — the resolver scans from
        the fill bar inclusive, so a gap through the stop resolves there."""
        built = _build(
            entries=[_entry(1, 2)],
            outcomes=[self._outcome("sl_hit", at=2, price=Decimal("95"))],
            outcome_pin=_PIN,
            regime=self._level_regime(),
        )
        (position,) = built.positions
        assert (position.close_bar_date, position.bars_held) == (_d(2), 0)

    def test_ambiguous_closes_the_bar_with_no_price(self) -> None:
        built = _build(
            entries=[_entry(1, 2)],
            outcomes=[self._outcome("ambiguous", at=3, price=None)],
            outcome_pin=_PIN,
            regime=self._level_regime(),
        )
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date, position.close_price) == ("ambiguous", _d(3), None)

    def test_unresolved_leaves_the_position_open_and_suppresses_the_max_hold(self) -> None:
        """⚠ §3.2 rule 5 is only satisfiable if ``unresolved`` beats C3: booking
        the expiry return would report a result over bars the resolver just
        said it could not judge."""
        built = _build(
            entries=[_entry(1, 2)],
            outcomes=[self._outcome("unresolved", at=None, price=None)],
            outcome_pin=_PIN,
            regime=self._level_regime(),
        )
        (position,) = built.positions
        assert position.close_source is None
        assert position.open_reason == "unresolved_outcome"
        assert position.mark_price == Decimal(119)

    def test_a_series_break_outcome_is_unmarked_but_does_not_suppress_the_new_segment(self) -> None:
        built = _build(
            entries=[_entry(1, 2), _entry(2, 6)],
            outcomes=[
                self._outcome(
                    "unresolved",
                    at=None,
                    price=None,
                    reason="series_break",
                    unresolved_until=6,
                ),
                self._outcome("tp_hit", at=8, price=Decimal("110"), signal=2),
            ],
            outcome_pin=_PIN,
            regime=self._level_regime(),
        )
        first, second = built.positions
        assert first.open_reason == "series_break"
        assert first.mark_price is None
        assert (second.entry_fill_bar_date, second.close_bar_date) == (_d(6), _d(8))
        assert built.superseded_open_position == 0
        assert built.marks_unavailable == 1

    def test_expired_and_the_max_hold_bar_agree(self) -> None:
        """For S-4 the two are redundant BY DESIGN (§3.2 C3), and the label the
        agreement carries is the resolver's."""
        built = _build(
            entries=[_entry(1, 2)],
            outcomes=[self._outcome("expired", at=6, price=Decimal(106))],
            outcome_pin=_PIN,
            regime=self._level_regime(),
        )
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date, position.bars_held) == ("level", _d(6), 4)

    def test_a_disagreement_about_the_same_bar_raises(self) -> None:
        """§3.2: *a disagreement is a failure, not a tie-break*."""
        with pytest.raises(ValueError, match="close sources disagree"):
            _build(
                entries=[_entry(1, 2)],
                outcomes=[self._outcome("expired", at=6, price=Decimal("999"))],
                outcome_pin=_PIN,
                regime=self._level_regime(),
            )

    def test_a_missing_outcome_raises_rather_than_falling_through_to_max_hold(self) -> None:
        with pytest.raises(ValueError, match="no outcome at the pinned"):
            _build(entries=[_entry(1, 2)], outcomes=[], outcome_pin=_PIN, regime=self._level_regime())

    def test_an_outcome_at_another_version_pin_raises(self) -> None:
        """§3.2 rule 1 — two resolver versions coexist by design, so an unpinned
        join double-counts every signal once per version present."""
        stray = ResolvedOutcome(
            signal_id=1,
            rule_set_version="outcome-resolver-v1+OTHER",
            input_rule_set_version=_PIN.input_rule_set_version,
            outcome="tp_hit",
            exit_bar_date=_d(4),
            exit_price=Decimal("110"),
        )
        with pytest.raises(ValueError, match="does not match the pin"):
            _build(entries=[_entry(1, 2)], outcomes=[stray], outcome_pin=_PIN, regime=self._level_regime())

    def test_two_outcomes_for_one_signal_raise(self) -> None:
        outcome = self._outcome("tp_hit", at=4, price=Decimal("110"))
        with pytest.raises(ValueError, match="two outcomes for signal"):
            _build(entries=[_entry(1, 2)], outcomes=[outcome, outcome], outcome_pin=_PIN, regime=self._level_regime())


class TestMaxHoldClose:
    _REGIME = ExitRegime(signal_pair=True, level_based=False, max_hold_bars=4, rebalance_dates=None)

    def test_c3_exits_at_the_open_of_the_expiry_bar(self) -> None:
        """``entry fill index + max_hold_bars``, at that bar's OPEN — the same
        arithmetic ``outcome_resolver`` uses for ``expired``."""
        built = _build(entries=[_entry(1, 2)], exits=[], regime=self._REGIME)
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date, position.close_price) == (
            "max_hold",
            _d(6),
            Decimal(106),
        )

    def test_an_earlier_exit_signal_wins(self) -> None:
        built = _build(entries=[_entry(1, 2)], exits=[_exit(4)], regime=self._REGIME)
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date) == ("signal_pair", _d(4))

    def test_an_expiry_bar_past_the_series_end_leaves_it_open(self) -> None:
        built = _build(entries=[_entry(1, 17)], exits=[], regime=self._REGIME)
        (position,) = built.positions
        assert position.open_reason == "window_end"
        assert built.close_bar_unfillable == 0

    def test_an_expiry_bar_with_no_open_is_counted_not_guessed(self) -> None:
        built = _build(
            entries=[_entry(1, 2)],
            exits=[],
            series={_INSTRUMENT: _series(opens={6: None})},
            regime=self._REGIME,
        )
        (position,) = built.positions
        assert position.open_reason == "close_bar_unfillable"
        assert built.close_bar_unfillable == 1

    def test_an_unpriceable_expiry_bar_does_not_let_a_later_exit_book_the_trade(self) -> None:
        """⚠⚠ A declared ``max_hold_bars`` ENDS the position by construction, so
        no later source may close it. Without the ceiling the exit at bar 9
        books a 7-bar hold against a declared maximum of 4 — priced off a bar
        the strategy could never have reached. (Codex, checkpoint 2.)"""
        built = _build(
            entries=[_entry(1, 2)],
            exits=[_exit(9)],
            series={_INSTRUMENT: _series(opens={6: None})},
            regime=self._REGIME,
        )
        (position,) = built.positions
        assert (position.close_source, position.open_reason) == (None, "close_bar_unfillable")
        assert built.close_bar_unfillable == 1

    def test_an_exit_before_the_expiry_bar_still_wins(self) -> None:
        """The ceiling bounds the hold; it does not suppress an earlier close."""
        built = _build(
            entries=[_entry(1, 2)],
            exits=[_exit(4)],
            series={_INSTRUMENT: _series(opens={6: None})},
            regime=self._REGIME,
        )
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date) == ("signal_pair", _d(4))

    def test_an_unbookable_hold_does_not_suppress_the_rest_of_history(self) -> None:
        """⚠ The hold ENDED at the ceiling; only its price is unknown. Treating
        it as open forever would drop every later trade in that instrument over
        one masked bar — a narrowing far larger than the defect."""
        built = _build(
            entries=[_entry(1, 2), _entry(2, 9)],
            exits=[],
            series={_INSTRUMENT: _series(opens={6: None})},
            regime=self._REGIME,
        )
        first, second = built.positions
        assert first.open_reason == "close_bar_unfillable"
        assert (second.close_source, second.close_bar_date) == ("max_hold", _d(13))
        assert built.superseded_open_position == 0


class TestCalendarClose:
    def _regime(self, *rebalances: int) -> ExitRegime:
        return ExitRegime(
            signal_pair=False,
            level_based=False,
            max_hold_bars=None,
            rebalance_dates=frozenset(_d(i) for i in rebalances),
        )

    def test_a_name_dropped_at_the_next_rebalance_closes_at_that_fill(self) -> None:
        """⚠ No exit signal exists for a drop-out (S-2 has one leg), which is
        why the close clause cannot be *the entry that supersedes it*."""
        built = _build(entries=[_entry(1, 2)], regime=self._regime(1, 8))
        (position,) = built.positions
        assert (position.close_source, position.close_bar_date, position.close_price) == (
            "calendar",
            _d(9),
            Decimal(109),
        )

    def test_a_reselected_name_is_one_hold_not_two(self) -> None:
        """§3.3 — the second rebalance's entry is superseded AND the calendar
        close does not fire, so no cost is charged for a hold that never ended."""
        built = _build(entries=[_entry(1, 2), _entry(2, 9)], regime=self._regime(1, 8, 14))
        (position,) = built.positions
        assert built.superseded_open_position == 1
        assert (position.entry_fill_bar_date, position.close_bar_date) == (_d(2), _d(15))

    def test_dropped_then_reselected_is_two_positions(self) -> None:
        built = _build(entries=[_entry(1, 2), _entry(2, 15)], regime=self._regime(1, 8, 14))
        assert [(p.entry_fill_bar_date, p.close_bar_date) for p in built.positions] == [(_d(2), _d(9)), (_d(15), None)]
        assert built.superseded_open_position == 0

    def test_a_halt_across_the_rebalance_is_counted_and_closes_at_the_next_own_bar(self) -> None:
        """§3.3 — the position stays open to the next date on which its OWN
        series has a bar, and the panel/instrument divergence is counted."""
        dates = tuple(_d(i) for i in range(20) if i not in {8, 9, 10})
        series = BarSeries(dates=dates, rows=tuple(_bar(Decimal(100 + d.day), Decimal(100 + d.day)) for d in dates))
        built = _build(entries=[_entry(1, 2)], series={_INSTRUMENT: series}, regime=self._regime(1, 8))
        (position,) = built.positions
        assert built.halted_at_rebalance == 1
        assert (position.close_source, position.close_bar_date) == ("calendar", _d(11))


class TestWindow:
    def test_an_entry_filling_outside_the_window_is_purged(self) -> None:
        """§5.2 — the purge is applied to the FILL date, because acting on the
        signal needs a price from the withheld side."""
        built = _build(entries=[_entry(1, 2), _entry(2, 15)], exits=[], window=Window(start=_d(10), end=_d(19)))
        assert [p.entry_fill_bar_date for p in built.positions] == [_d(15)]
        assert built.entries_outside_window == 1

    def test_a_close_after_the_window_end_leaves_the_position_open_and_marked(self) -> None:
        built = _build(entries=[_entry(1, 2)], exits=[_exit(9)], window=Window(start=_d(0), end=_d(6)))
        (position,) = built.positions
        assert (position.close_source, position.open_reason) == (None, "window_end")
        assert position.mark_price == Decimal(106)

    def test_a_mark_that_cannot_be_taken_is_counted_never_invented(self) -> None:
        masked: dict[int, Decimal | None] = dict.fromkeys(range(2, 20))
        built = _build(
            entries=[_entry(1, 2)],
            exits=[],
            series={_INSTRUMENT: _series(closes=masked)},
        )
        (position,) = built.positions
        assert position.mark_price is None
        assert built.marks_unavailable == 1


class TestCallerContract:
    def test_exits_without_a_declared_exit_leg_raise(self) -> None:
        regime = ExitRegime(signal_pair=False, level_based=False, max_hold_bars=4, rebalance_dates=None)
        with pytest.raises(ValueError, match="declares no exit leg"):
            _build(entries=[_entry(1, 2)], exits=[_exit(5)], regime=regime)

    def test_outcomes_without_a_level_based_regime_raise(self) -> None:
        outcome = ResolvedOutcome(
            signal_id=1,
            rule_set_version=_PIN.rule_set_version,
            input_rule_set_version=_PIN.input_rule_set_version,
            outcome="tp_hit",
            exit_bar_date=_d(4),
            exit_price=Decimal("110"),
        )
        with pytest.raises(ValueError, match="not level-based"):
            _build(entries=[_entry(1, 2)], outcomes=[outcome], outcome_pin=_PIN)

    def test_a_level_based_regime_without_a_pin_raises(self) -> None:
        regime = ExitRegime(signal_pair=False, level_based=True, max_hold_bars=None, rebalance_dates=None)
        with pytest.raises(ValueError, match="must declare the outcome version pin"):
            _build(entries=[_entry(1, 2)], regime=regime)

    def test_a_missing_series_raises(self) -> None:
        with pytest.raises(ValueError, match="no series supplied"):
            _build(entries=[_entry(1, 2)], series={})

    def test_a_fill_date_absent_from_the_series_raises(self) -> None:
        with pytest.raises(ValueError, match="must come from the same corpus"):
            _build(entries=[_entry(1, 2)], series={_INSTRUMENT: _series(2)})

    def test_a_stored_exit_date_absent_from_the_series_raises(self) -> None:
        short = BarSeries(
            dates=tuple(_d(i) for i in range(20) if i != 5),
            rows=tuple(_bar(Decimal(100 + i), Decimal(100 + i)) for i in range(20) if i != 5),
        )
        with pytest.raises(ValueError, match="close_bar_date"):
            _build(entries=[_entry(1, 2)], exits=[_exit(5)], series={_INSTRUMENT: short})

    def test_a_regime_with_no_close_source_raises(self) -> None:
        with pytest.raises(ValueError, match="no close source declared"):
            ExitRegime(signal_pair=False, level_based=False, max_hold_bars=None, rebalance_dates=None)

    def test_an_empty_rebalance_calendar_is_not_the_same_as_none(self) -> None:
        with pytest.raises(ValueError, match="rebalance_dates is empty"):
            ExitRegime(signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=frozenset())

    def test_a_fill_on_or_before_its_signal_bar_raises(self) -> None:
        with pytest.raises(ValueError, match="is not after signal_bar_date"):
            EntryFill(
                signal_id=1,
                instrument_id=_INSTRUMENT,
                signal_bar_date=_d(5),
                fill_bar_date=_d(5),
                fill_price=Decimal("100"),
            )

    def test_a_zero_fill_price_is_refused(self) -> None:
        """⚠ MEASURED, not hypothetical: the research corpus holds 16 bars
        across 9 series whose stored ``open`` is 0 — all already quarantined on
        both axes, and all carried through unmasked by ``load_masked_series``.
        A fill at 0 is not a trade; ``outcome_resolver`` refuses the same state
        for the same reason (``entry_price`` divides ``gross_return_pct``)."""
        with pytest.raises(ValueError, match="fill_price must be > 0"):
            EntryFill(
                signal_id=1,
                instrument_id=_INSTRUMENT,
                signal_bar_date=_d(4),
                fill_bar_date=_d(5),
                fill_price=Decimal("0"),
            )

    def test_an_exit_fill_on_or_before_its_signal_bar_raises(self) -> None:
        with pytest.raises(ValueError, match="is not after it"):
            ExitFill(
                instrument_id=_INSTRUMENT,
                signal_bar_date=_d(5),
                fill_bar_date=_d(4),
                fill_price=Decimal("100"),
            )


class TestRowInvariants:
    """The dataclasses mirror ``sql/255`` / ``sql/256``'s shape rules, so a bad
    row fails at construction with a message naming the field."""

    def _position(self, **overrides: object) -> Position:
        fields: dict[str, object] = {
            "strategy_id": "s1",
            "strategy_version": "v1",
            "instrument_id": _INSTRUMENT,
            "entry_signal_id": 1,
            "entry_signal_bar_date": _d(1),
            "entry_fill_bar_date": _d(2),
            "entry_fill_price": Decimal("100"),
            "close_source": "signal_pair",
            "close_bar_date": _d(5),
            "close_price": Decimal("105"),
            "bars_held": 3,
            "open_reason": None,
            "mark_price": None,
        }
        fields.update(overrides)
        return Position(**fields)  # type: ignore[arg-type]

    def test_a_position_is_closed_or_open_never_both(self) -> None:
        with pytest.raises(ValueError, match="both"):
            self._position(open_reason="window_end")

    def test_a_position_is_closed_or_open_never_neither(self) -> None:
        with pytest.raises(ValueError, match="neither"):
            self._position(close_source=None, close_bar_date=None, close_price=None, bars_held=None)

    def test_a_half_close_location_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="partial close location"):
            self._position(bars_held=None)

    def test_an_open_position_may_not_carry_a_stray_bars_held(self) -> None:
        """⚠⚠ THE ONE THAT PINS *COUNTED*, NOT *ANDed* — the 3c mirror defect
        (prevention log, #2240 3c). The closed-side case above raises under
        either expression, so it does not discriminate them: only an OPEN
        position carrying half a location does. ``a is not None and b is not
        None`` scores False here, matches "no location", and passes."""
        with pytest.raises(ValueError, match="partial close location"):
            self._position(
                close_source=None, close_bar_date=None, close_price=None, bars_held=3, open_reason="window_end"
            )

    def test_only_ambiguous_may_close_without_a_price(self) -> None:
        with pytest.raises(ValueError, match="a price is absent exactly for ambiguous"):
            self._position(close_price=None)

    def test_ambiguous_may_not_carry_a_price(self) -> None:
        with pytest.raises(ValueError, match="a price is absent exactly for ambiguous"):
            self._position(close_source="ambiguous")

    def test_a_closed_position_carries_no_mark(self) -> None:
        with pytest.raises(ValueError, match="realised close, not a mark"):
            self._position(mark_price=Decimal("104"))

    def test_a_close_before_the_fill_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="closes .* before its fill"):
            self._position(close_bar_date=_d(1), bars_held=0)

    def test_an_unresolved_outcome_may_not_carry_a_location(self) -> None:
        with pytest.raises(ValueError, match="an unresolved outcome has none"):
            ResolvedOutcome(
                signal_id=1,
                rule_set_version="a",
                input_rule_set_version="b",
                outcome="unresolved",
                exit_bar_date=_d(4),
                exit_price=None,
                reason="missing_bar_data",
            )

    def test_a_series_break_outcome_requires_its_resume_boundary(self) -> None:
        with pytest.raises(ValueError, match="a boundary is required exactly for series_break"):
            ResolvedOutcome(
                signal_id=1,
                rule_set_version="a",
                input_rule_set_version="b",
                outcome="unresolved",
                exit_bar_date=None,
                exit_price=None,
                reason="series_break",
            )

    def test_a_booked_outcome_must_carry_a_price(self) -> None:
        with pytest.raises(ValueError, match="a price exists exactly for"):
            ResolvedOutcome(
                signal_id=1,
                rule_set_version="a",
                input_rule_set_version="b",
                outcome="tp_hit",
                exit_bar_date=_d(4),
                exit_price=None,
            )

    def test_a_blank_version_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="both key members must be stated"):
            ResolvedOutcome(
                signal_id=1,
                rule_set_version="",
                input_rule_set_version="b",
                outcome="ambiguous",
                exit_bar_date=_d(4),
                exit_price=None,
            )


class TestSpecConstants:
    """⚠ LITERALS, not imports. A reference that imports the constant it
    validates is a tautology (prevention log, #2240 S-3): it would agree with
    any value the module happened to hold. These are the spec's own numbers,
    typed out from ``2026-08-07-bounded-backtester.md`` §3's table."""

    SPEC_CLOSE_SOURCES = {"signal_pair", "level", "max_hold", "calendar", "ambiguous"}
    #: #2721 step 3 — emitted by ``backtest_run``'s termination rule, never by
    #: the builder itself; declared apart like the open-reason additions.
    OUR_ADDITIONAL_CLOSE_SOURCES = {"series_termination"}
    #: ⚠ The spec's §3.2 rule 5 names exactly these two. The implementation's
    #: explicitly declared additions are deliberately absent here, so adopting
    #: a spec reason later cannot silently land on our side of the line.
    SPEC_OPEN_REASONS = {"unresolved_outcome", "window_end"}
    SPEC_S3_MAX_HOLD_BARS = 10
    SPEC_S4_MAX_HOLD_BARS = 40

    def test_the_close_source_vocabulary_is_the_specs(self) -> None:
        assert set(CLOSE_SOURCES) == self.SPEC_CLOSE_SOURCES | self.OUR_ADDITIONAL_CLOSE_SOURCES

    def test_the_open_reason_vocabulary_is_the_specs_plus_ours(self) -> None:
        assert set(SPEC_OPEN_REASONS) == self.SPEC_OPEN_REASONS
        assert set(OPEN_REASONS) == self.SPEC_OPEN_REASONS | set(OUR_ADDITIONAL_OPEN_REASONS)
        assert "termination_price_unlocatable" in OUR_ADDITIONAL_OPEN_REASONS

    def test_the_shipped_max_holds_are_the_specs(self) -> None:
        assert (S3_MAX_HOLD_BARS, S4_MAX_HOLD_BARS) == (self.SPEC_S3_MAX_HOLD_BARS, self.SPEC_S4_MAX_HOLD_BARS)

    def test_the_rule_set_version_hashes_this_modules_own_source(self) -> None:
        """Criterion 11 makes the execution assumption part of identity, and
        this module's constructions live nowhere else.

        ⚠ Recomputed here from the FILE. Comparing against the module's own
        helper would hold just as well when both are a constant, which is the
        one defect this guards."""
        import app.services.position_builder as builder

        expected = hashlib.sha256(Path(builder.__file__).read_bytes()).hexdigest()[:12]
        assert RULE_SET_VERSION == f"{RULE_SET_ID}+{expected}"
