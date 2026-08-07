"""S-1 time-series momentum — the catalogue's first strategy (#2240).

Spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §4
(S-1), §3.5, §4.0, §5 criteria 4/8/11. Registry: phase 3a.

⚠ THE EXPECTED VERDICTS ARE DERIVED FROM A NAIVE REFERENCE, NOT HAND-WRITTEN.
``_reference_sma`` re-adds the whole window at every bar. ``sma_series`` carries
a running sum with roll-off. A shared off-by-one or a window-boundary error
would have to occur in both to pass, which a table of literals cannot say.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import (
    FAST_PERIOD,
    S1_PARAMS,
    S1_STRATEGY_ID,
    SLOW_PERIOD,
    s1_identity,
    s1_signals,
)
from app.services.strategy_registry import StrategySignal
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"


def _bars(closes: Sequence[float | None]) -> BarSeries:
    """One bar per close. ``None`` is a MASKED close, as ``load_masked_series``
    produces — the field is present and empty, not absent."""
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(c)),
            "high": None if c is None else Decimal(str(c + 1)),
            "low": None if c is None else Decimal(str(c - 1)),
            "close": None if c is None else Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    start = date(2020, 1, 1)
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _reference_sma(closes: Sequence[float | None], period: int, index: int) -> float | None:
    """Naive SMA at ``index``, or None when the window is short or holed."""
    if index + 1 < period:
        return None
    window = closes[index - period + 1 : index + 1]
    if any(value is None for value in window):
        return None
    return sum(v for v in window if v is not None) / period


def _reference_verdicts(closes: Sequence[float | None]) -> list[tuple[str, str | None, str, str | None]]:
    """(entry verdict, entry reason, exit verdict, exit reason) per bar, from
    the naive reference. Mirrors 3a's runner: the last bar has no ``t+1``, a
    holed input is the caller's reason, a cold input is warm-up, and only then
    is the condition asked."""
    out: list[tuple[str, str | None, str, str | None]] = []
    for i, close in enumerate(closes):
        if i == len(closes) - 1:
            out.append(("not_evaluable", "no_fill_bar", "not_evaluable", "no_fill_bar"))
            continue
        fast = _reference_sma(closes, FAST_PERIOD, i)
        slow = _reference_sma(closes, SLOW_PERIOD, i)
        # A hole is any None a WARM window can see, plus the close itself. A
        # window that has not filled yet is warm-up, not a hole, and the two
        # carry different bias implications (criterion 8).
        fast_holed = i + 1 >= FAST_PERIOD and any(closes[j] is None for j in range(i - FAST_PERIOD + 1, i + 1))
        slow_holed = i + 1 >= SLOW_PERIOD and any(closes[j] is None for j in range(i - SLOW_PERIOD + 1, i + 1))
        if close is None or fast_holed or slow_holed:
            out.append(("not_evaluable", REASON, "not_evaluable", REASON))
            continue
        if fast is None or slow is None:
            out.append(("not_evaluable", "insufficient_warmup", "not_evaluable", "insufficient_warmup"))
            continue
        assert close is not None
        entry = "fired" if (close > slow and fast > slow) else "not_fired"
        exit_ = "fired" if close < fast else "not_fired"
        out.append((entry, None, exit_, None))
    return out


def _split(signals: list[StrategySignal]) -> tuple[list[StrategySignal], list[StrategySignal]]:
    entries = [s for s in signals if s.kind == "entry"]
    exits = [s for s in signals if s.kind == "exit"]
    return entries, exits


def _run(closes: Sequence[float | None]) -> tuple[list[StrategySignal], list[StrategySignal]]:
    return _split(s1_signals(_bars(closes), universe=UNIVERSE, close_reason=REASON))


N = 260
RISING: list[float | None] = [100.0 + i for i in range(N)]
FALLING: list[float | None] = [1_000.0 - i for i in range(N)]
FLAT: list[float | None] = [100.0] * N


def _ramps(*legs: tuple[int, float, float]) -> list[float | None]:
    """Piecewise-linear closes: ``(bars, start, step)`` per leg."""
    out: list[float | None] = []
    for bars, start, step in legs:
        out.extend(start + step * k for k in range(bars))
    return out


#: Up, down, up, down — chosen because it covers ALL FOUR combinations of the
#: two entry conjuncts (asserted below), including the ``close > sma_200`` /
#: ``sma_50 <= sma_200`` case a dropped conjunct would silently fire on. Prices
#: stay above 100 throughout: a fixture that goes negative would test arithmetic
#: no instrument can produce.
CYCLE: list[float | None] = _ramps((240, 100.0, 1.0), (80, 340.0, -2.0), (140, 180.0, 1.0), (80, 320.0, -1.5))


class TestTheRule:
    """§4: entry ``close > sma_200 and sma_50 > sma_200``; exit ``close < sma_50``."""

    @pytest.mark.parametrize("closes", [RISING, FALLING, FLAT, CYCLE], ids=["rising", "falling", "flat", "cycle"])
    def test_every_bar_matches_the_naive_reference(self, closes: Sequence[float | None]) -> None:
        entries, exits = _run(closes)
        expected = _reference_verdicts(closes)
        assert len(entries) == len(exits) == len(closes)
        for i, (entry_verdict, entry_reason, exit_verdict, exit_reason) in enumerate(expected):
            assert (entries[i].verdict, entries[i].reason) == (entry_verdict, entry_reason), f"entry bar {i}"
            assert (exits[i].verdict, exits[i].reason) == (exit_verdict, exit_reason), f"exit bar {i}"

    def test_a_rising_ramp_enters_and_never_exits(self) -> None:
        entries, exits = _run(RISING)
        assert all(s.verdict == "fired" for s in entries[SLOW_PERIOD - 1 : -1])
        assert not any(s.verdict == "fired" for s in exits)

    def test_a_falling_ramp_exits_and_never_enters(self) -> None:
        entries, exits = _run(FALLING)
        assert all(s.verdict == "fired" for s in exits[SLOW_PERIOD - 1 : -1])
        assert not any(s.verdict == "fired" for s in entries)

    def test_a_flat_series_fires_neither_leg(self) -> None:
        """Both comparisons are STRICT, as §4 writes them: on a flat series
        ``close == sma_50 == sma_200``, so a ``>=`` on either leg would fire
        every warm bar instead of none."""
        entries, exits = _run(FLAT)
        warm = slice(SLOW_PERIOD - 1, -1)
        assert [s.verdict for s in entries[warm]] == ["not_fired"] * (N - SLOW_PERIOD)
        assert [s.verdict for s in exits[warm]] == ["not_fired"] * (N - SLOW_PERIOD)

    def test_the_cycle_fixture_exercises_both_outcomes_on_both_legs(self) -> None:
        """Guards the table test above from passing vacuously — a fixture where
        one leg never fires proves nothing about that leg."""
        entries, exits = _run(CYCLE)
        for signals in (entries, exits):
            verdicts = {s.verdict for s in signals}
            assert {"fired", "not_fired"} <= verdicts

    def test_the_cycle_fixture_covers_all_four_entry_combinations(self) -> None:
        """⚠ WITHOUT THIS THE ENTRY TABLE CANNOT SEE A DROPPED CONJUNCT.

        ``close > sma_200`` alone and the full two-clause rule agree on every
        bar EXCEPT those where ``sma_50 <= sma_200``, so a fixture missing that
        quadrant passes an entry rule with the trend filter deleted."""
        combinations = set()
        for i in range(SLOW_PERIOD - 1, len(CYCLE) - 1):
            close = CYCLE[i]
            fast = _reference_sma(CYCLE, FAST_PERIOD, i)
            slow = _reference_sma(CYCLE, SLOW_PERIOD, i)
            assert close is not None and fast is not None and slow is not None
            combinations.add((close > slow, fast > slow))
        assert combinations == {(True, True), (True, False), (False, True), (False, False)}


#: ⚠ CONSTRUCTED SO EXACTLY ONE ENTRY COMPARISON IS AN EQUALITY.
#:
#: A flat series makes BOTH comparisons equalities, so relaxing either one alone
#: still leaves the conjunction false — a `>=` defect survives it. (Measured:
#: `scripts/probe_2240_s1_momentum.py` reported NOT CAUGHT twice before these
#: existed.) Each fixture below puts one comparison exactly on the boundary
#: while the other is strictly true, which is the only shape that can see it.
#:
#: All values are integers and every window sum stays far inside 2**53, so the
#: equalities below are EXACT in float64 rather than approximately equal.
#:
#: close(199) == sma_200(199) == 100, while sma_50(199) = 109.8.
#: 49*90 + 101*100 + 49*110 + 100 = 20,000 over 200 bars.
CLOSE_ON_SLOW: list[float | None] = [*([90.0] * 49), *([100.0] * 101), *([110.0] * 49), 100.0, 100.0]

#: sma_50(199) == sma_200(199) == 124.5 (a period-50 saw-tooth, and 200 is four
#: whole periods), while close(199) = 149.
AVERAGES_EQUAL: list[float | None] = [100.0 + (i % 50) for i in range(201)]

BOUNDARY_INDEX = 199


class TestStrictComparisons:
    """§4 writes both entry comparisons with ``>``. A ``>=`` on either is only
    visible where THAT comparison is an equality and the other is not."""

    def test_a_close_sitting_exactly_on_the_slow_average_does_not_enter(self) -> None:
        closes = CLOSE_ON_SLOW
        i = BOUNDARY_INDEX
        fast = _reference_sma(closes, FAST_PERIOD, i)
        slow = _reference_sma(closes, SLOW_PERIOD, i)
        # ⚠ Asserted, not assumed. If the fixture ever drifts off the boundary
        # the test would still pass while testing nothing.
        assert closes[i] == slow and fast is not None and slow is not None and fast > slow
        entries, _ = _run(closes)
        assert entries[i].verdict == "not_fired"

    def test_averages_exactly_equal_do_not_enter_however_high_the_close(self) -> None:
        closes = AVERAGES_EQUAL
        i = BOUNDARY_INDEX
        fast = _reference_sma(closes, FAST_PERIOD, i)
        slow = _reference_sma(closes, SLOW_PERIOD, i)
        close = closes[i]
        assert fast == slow and slow is not None and close is not None and close > slow
        entries, _ = _run(closes)
        assert entries[i].verdict == "not_fired"


class TestWarmUp:
    """§4: *"Needs >=200 bars as-of the decision date"* — one warm-up, both legs."""

    def test_both_legs_share_the_slow_warm_up(self) -> None:
        """⚠ THE NARROWING. Bar 100 has a warm ``sma_50`` and a cold
        ``sma_200``, so the exit condition IS computable there. It is refused
        anyway, because per-leg evaluability would make the same bar live for
        one leg and warming for the other — §3.1's branch-dependent evaluability
        one level up."""
        entries, exits = _run(RISING)
        cold = FAST_PERIOD + 10
        assert cold < SLOW_PERIOD - 1
        assert (entries[cold].verdict, entries[cold].reason) == ("not_evaluable", "insufficient_warmup")
        assert (exits[cold].verdict, exits[cold].reason) == ("not_evaluable", "insufficient_warmup")

    def test_the_first_evaluable_bar_is_the_slow_period_boundary(self) -> None:
        entries, _ = _run(RISING)
        assert entries[SLOW_PERIOD - 2].verdict == "not_evaluable"
        assert entries[SLOW_PERIOD - 1].verdict != "not_evaluable"

    def test_a_series_shorter_than_the_slow_period_is_never_evaluable(self) -> None:
        entries, exits = _run(RISING[:150])
        assert {s.verdict for s in entries} == {"not_evaluable"}
        assert {s.verdict for s in exits} == {"not_evaluable"}
        assert {s.reason for s in entries} == {"insufficient_warmup", "no_fill_bar"}


class TestMissingClose:
    """Criterion 8: a data gap and a real absence must stay distinguishable."""

    def test_a_masked_close_records_the_callers_reason_not_warm_up(self) -> None:
        closes = list(RISING)
        holed = 210
        closes[holed] = None
        entries, exits = _run(closes)
        assert (entries[holed].verdict, entries[holed].reason) == ("not_evaluable", REASON)
        assert (exits[holed].verdict, exits[holed].reason) == ("not_evaluable", REASON)

    def test_the_hole_propagates_across_the_whole_slow_window(self) -> None:
        """Not just the holed bar: every window containing it is unevaluable,
        which is what makes the refusal count a bias measurement rather than a
        single dropped row."""
        closes = list(RISING)
        closes[10] = None
        entries, _ = _run(closes)
        assert entries[10 + SLOW_PERIOD - 1].reason == REASON
        assert entries[10 + SLOW_PERIOD].reason != REASON

    def test_a_reason_outside_the_closed_vocabulary_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown reason code"):
            s1_signals(_bars(RISING), universe=UNIVERSE, close_reason="looked_wrong")  # type: ignore[arg-type]


class TestLastBar:
    def test_the_final_bar_is_no_fill_bar_on_both_legs(self) -> None:
        """§3.5.1 has no ``t+1`` to fill at, so no decision on the last bar can
        be acted on — whichever way the condition would have gone."""
        entries, exits = _run(RISING)
        assert (entries[-1].verdict, entries[-1].reason) == ("not_evaluable", "no_fill_bar")
        assert (exits[-1].verdict, exits[-1].reason) == ("not_evaluable", "no_fill_bar")


class TestCausality:
    """Criterion 4: every value at bar ``t`` uses only bars <= ``t``. #2260
    candidate 1, and the likeliest of the four."""

    def test_a_bars_verdict_is_unchanged_when_later_bars_are_removed(self) -> None:
        full_entries, full_exits = _run(CYCLE)
        # +2 so `index` is never the truncated series' own last bar, which is
        # `no_fill_bar` by construction and would compare a refusal against a
        # verdict.
        for index in range(SLOW_PERIOD - 1, len(CYCLE) - 1, 7):
            truncated_entries, truncated_exits = _run(CYCLE[: index + 2])
            assert truncated_entries[index].verdict == full_entries[index].verdict, f"entry {index}"
            assert truncated_exits[index].verdict == full_exits[index].verdict, f"exit {index}"

    def test_the_truncation_sweep_covers_bars_of_both_verdicts(self) -> None:
        """A sweep over a stretch where nothing changes cannot detect a
        look-ahead, so the fixture is asserted to vary across it."""
        full_entries, _ = _run(CYCLE)
        swept = [full_entries[i].verdict for i in range(SLOW_PERIOD - 1, len(CYCLE) - 1, 7)]
        assert {"fired", "not_fired"} <= set(swept)


class TestIdentity:
    """Criterion 11: identity = code + config + data contract."""

    def test_a_blank_cost_model_id_is_rejected(self) -> None:
        """``NOT NULL`` does not catch present-but-empty (#2286), and neither
        does ``str``. No cost model exists yet, so the placeholder must be said
        out loud rather than defaulted to nothing."""
        with pytest.raises(ValueError, match="non-empty"):
            s1_identity(universe=UNIVERSE, cost_model_id="   ")

    def test_the_version_moves_with_the_universe(self) -> None:
        a = s1_identity(universe="survivor_only", cost_model_id=COST_MODEL_ID)
        b = s1_identity(universe="survivorship_free", cost_model_id=COST_MODEL_ID)
        assert a.version != b.version

    def test_the_version_moves_with_the_cost_model(self) -> None:
        a = s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        b = s1_identity(universe=UNIVERSE, cost_model_id="static-p75-v1")
        assert a.version != b.version

    def test_the_identity_carries_the_two_lookbacks_and_nothing_else(self) -> None:
        identity = s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        assert identity.strategy_id == S1_STRATEGY_ID
        assert dict(identity.params) == {"fast_period": 50, "slow_period": 200}
        assert dict(S1_PARAMS) == dict(identity.params)

    def test_the_source_hash_is_this_strategys_own_source(self) -> None:
        """⚠ Recomputed here from the FILE, not compared against the module's
        own helper: ``source_hash == _source_hash()`` holds just as well when
        both are a constant, which is the one defect this guards."""
        import app.services.strategies.s1_time_series_momentum as s1

        expected = hashlib.sha256(Path(s1.__file__).read_bytes()).hexdigest()[:12]
        assert s1_identity(universe=UNIVERSE, cost_model_id="x").source_hash == expected


class TestLedgerRoundTrip:
    """The two legs have to survive 3c's writer, which is where the fill is
    resolved and where a same-bar collision would surface."""

    def test_both_legs_coexist_on_one_bar(self) -> None:
        series = _bars(CYCLE)
        signals = s1_signals(series, universe=UNIVERSE, close_reason=REASON)
        rows = resolve_fills(
            signals,
            series=series,
            identity=s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=1,
        )
        assert len(rows) == 2 * len(CYCLE)
        keys = {(row.signal_bar_date, row.signal_kind) for row in rows}
        assert len(keys) == len(rows)

    def test_every_fired_row_fills_at_the_next_bars_open(self) -> None:
        series = _bars(CYCLE)
        rows = resolve_fills(
            s1_signals(series, universe=UNIVERSE, close_reason=REASON),
            series=series,
            identity=s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=1,
        )
        fired = [row for row in rows if row.verdict == "fired"]
        assert fired
        by_date = {d: i for i, d in enumerate(series.dates)}
        for row in fired:
            index = by_date[row.signal_bar_date]
            assert row.fill_bar_date == series.dates[index + 1]
            assert row.fill_price == series.rows[index + 1]["open"]
