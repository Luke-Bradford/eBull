"""S-3 mean reversion within trend — the catalogue's second strategy (#2240).

Spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §4
(S-3), §3.5, §4.0, §5 criteria 4/8/11. Registry: phase 3a.

⚠ THE EXPECTED VERDICTS ARE DERIVED FROM A NAIVE REFERENCE, NOT HAND-WRITTEN.
``_reference_rsi`` re-runs Wilder from the seed at every bar and
``_reference_sma`` re-adds the whole window; the shipped ``rsi_series`` /
``sma_series`` carry state forward. A window-boundary error or an off-by-one in
the seed index would have to occur in both to pass, which a table of literals
cannot say.

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
from app.services.outcome_resolver import ExitLevels
from app.services.signal_ledger import resolve_fills
from app.services.strategies import s3_mean_reversion_in_trend as s3_module
from app.services.strategies.s3_mean_reversion_in_trend import (
    MAX_HOLD_BARS,
    S3_PARAMS,
    S3_STRATEGY_ID,
    s3_identity,
    s3_signals,
)
from app.services.strategy_registry import StrategySignal
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"

#: ⚠⚠ §4's NUMBERS, WRITTEN OUT HERE AND DELIBERATELY *NOT* IMPORTED FROM THE
#: MODULE UNDER TEST.
#:
#: An earlier draft of this file imported ``RSI_PERIOD`` etc. and fed them to the
#: naive reference. That makes the reference a function of the code it is meant
#: to check: shift ``RSI_PERIOD`` to 13 and the reference shifts with it, so
#: every bar still "matches" and a silently different strategy passes.
#: ``scripts/probe_2240_s3_mean_reversion.py`` reported NOT CAUGHT for exactly
#: that mutation, which is what these literals are here to close.
#:
#: The rule, verbatim: *"Signal: rsi_14(t) < 30 and close(t) > sma_200(t) …
#: Exit: rsi_14(t) > 50, or 10 bars elapsed, whichever first."*
SPEC_RSI_PERIOD = 14
SPEC_OVERSOLD = 30.0
SPEC_EXIT = 50.0
SPEC_TREND_PERIOD = 200
SPEC_MAX_HOLD = 10


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


def _reference_rsi(closes: Sequence[float | None], period: int, index: int) -> float | None:
    """Naive Wilder RSI at ``index``, recomputed from the seed every call.

    ⚠ Returns None as soon as ANY close at or before ``index`` is missing, which
    is not a choice this reference makes — Wilder smoothing carries state across
    every bar, so a hole has no window to clear and the recursion cannot resume.
    ``sma_series`` recovers; ``rsi_series`` does not, and that asymmetry is the
    thing ``TestMissingClose`` exists to pin.

    Flat-series convention (50.0 when gain and loss are both zero, 100.0 when
    there are no losses) is ``technical_analysis.rsi``'s, inherited deliberately
    and restated here so the reference is not just a call to the code under test.
    """
    if index < period or any(value is None for value in closes[: index + 1]):
        return None
    gain = 0.0
    loss = 0.0
    for i in range(1, period + 1):
        a, b = closes[i], closes[i - 1]
        assert a is not None and b is not None
        gain += max(a - b, 0.0)
        loss += max(b - a, 0.0)
    gain /= period
    loss /= period
    for i in range(period + 1, index + 1):
        a, b = closes[i], closes[i - 1]
        assert a is not None and b is not None
        gain = (gain * (period - 1) + max(a - b, 0.0)) / period
        loss = (loss * (period - 1) + max(b - a, 0.0)) / period
    if gain == 0.0 and loss == 0.0:
        return 50.0
    if loss == 0.0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + gain / loss)


def _reference_verdicts(closes: Sequence[float | None]) -> list[tuple[str, str | None, str, str | None]]:
    """(entry verdict, entry reason, exit verdict, exit reason) per bar, from the
    naive reference. Mirrors 3a's runner: the last bar has no ``t+1``, a holed
    input is the caller's reason, a cold input is warm-up, and only then is the
    condition asked."""
    out: list[tuple[str, str | None, str, str | None]] = []
    first_hole = next((i for i, value in enumerate(closes) if value is None), None)
    for i, close in enumerate(closes):
        if i == len(closes) - 1:
            out.append(("not_evaluable", "no_fill_bar", "not_evaluable", "no_fill_bar"))
            continue
        rsi = _reference_rsi(closes, SPEC_RSI_PERIOD, i)
        trend = _reference_sma(closes, SPEC_TREND_PERIOD, i)
        # A hole is any None the RSI recursion has already passed (i.e. every
        # bar from the first one onward), plus any None a WARM sma window can
        # see, plus the close itself. A window that has not filled yet is
        # warm-up, not a hole — criterion 8 keeps the two apart.
        rsi_holed = first_hole is not None and i >= first_hole
        trend_holed = i + 1 >= SPEC_TREND_PERIOD and any(
            closes[j] is None for j in range(i - SPEC_TREND_PERIOD + 1, i + 1)
        )
        if close is None or rsi_holed or trend_holed:
            out.append(("not_evaluable", REASON, "not_evaluable", REASON))
            continue
        if rsi is None or trend is None:
            out.append(("not_evaluable", "insufficient_warmup", "not_evaluable", "insufficient_warmup"))
            continue
        entry = "fired" if (rsi < SPEC_OVERSOLD and close > trend) else "not_fired"
        exit_ = "fired" if rsi > SPEC_EXIT else "not_fired"
        out.append((entry, None, exit_, None))
    return out


def _split(signals: list[StrategySignal]) -> tuple[list[StrategySignal], list[StrategySignal]]:
    entries = [s for s in signals if s.kind == "entry"]
    exits = [s for s in signals if s.kind == "exit"]
    return entries, exits


def _run(closes: Sequence[float | None]) -> tuple[list[StrategySignal], list[StrategySignal]]:
    return _split(s3_signals(_bars(closes), universe=UNIVERSE, close_reason=REASON))


def _ramps(*legs: tuple[int, float, float]) -> list[float | None]:
    """Piecewise-linear closes: ``(bars, start, step)`` per leg."""
    out: list[float | None] = []
    for bars, start, step in legs:
        out.extend(start + step * k for k in range(bars))
    return out


N = 260
RISING: list[float | None] = [100.0 + i for i in range(N)]
FALLING: list[float | None] = [1_000.0 - i for i in range(N)]
FLAT: list[float | None] = [100.0] * N

#: Up, down, up, down. Chosen because it produces BOTH verdicts on BOTH legs
#: (asserted below) — an uptrend deep enough to hold ``close > sma_200`` through
#: a fall sharp enough to drive RSI under 30, which is precisely S-3's setup and
#: is not something a monotone ramp ever shows.
CYCLE: list[float | None] = _ramps((240, 100.0, 1.0), (80, 340.0, -2.0), (140, 180.0, 1.0), (80, 320.0, -1.5))


def _ramp_then_drop(drop: float, *, bars: int = 400, base: float = 1_000.0, step: float = 3.0) -> list[float | None]:
    """A constant ``+step`` ramp of ``bars`` bars, then ONE bar down by ``drop``,
    then a flat tail so the drop bar is never the series' own last bar.

    ⚠ THIS SHAPE IS WHAT MAKES THE BOUNDARY EXACT IN FLOAT64, and it is chosen,
    not found. A constant ``+3`` ramp gives Wilder a seed that is the mean of 14
    identical deltas — exactly ``3.0`` — and zero loss. One down bar then puts
    ``gain = 39/14`` and ``loss = drop/14``, so ``drop = 91`` makes the ratio
    exactly ``3/7`` and the RSI exactly ``30.0``, while ``drop = 297`` makes
    ``close`` exactly equal to ``sma_200``. Every close is an integer and every
    window sum stays far inside ``2**53``, so these are exact equalities rather
    than approximate ones. Asserted in each test, never assumed.
    """
    closes: list[float | None] = [base + step * i for i in range(bars)]
    last = base + step * (bars - 1)
    closes.append(last - drop)
    closes.extend([last - drop] * 3)
    return closes


#: ⚠ CONSTRUCTED SO EXACTLY ONE COMPARISON IS AN EQUALITY — one fixture per
#: operator, because a fixture that sits on BOTH boundaries pins NEITHER: relax
#: one ``<``/``>`` and the other conjunct still reads False, so the revert probe
#: reports NOT CAUGHT. (Same defect ``scripts/probe_2240_s1_momentum.py``
#: measured on S-1 before its two fixtures existed.)
#:
#: ``rsi_14(400) == 30.0`` exactly, while ``close(400) = 2106 > sma_200 = 1901.03``.
RSI_ON_THRESHOLD: list[float | None] = _ramp_then_drop(91.0)

#: ``close(400) == sma_200(400) == 1900.0`` exactly, while ``rsi_14 = 11.6 < 30``.
CLOSE_ON_TREND: list[float | None] = _ramp_then_drop(297.0)

#: Neither boundary — the same shape dropped far enough that BOTH conjuncts are
#: strictly true. Without it the two boundary fixtures would be consistent with a
#: rule that never fires at all.
DEEP_DIP: list[float | None] = _ramp_then_drop(120.0)

BOUNDARY_INDEX = 400


class TestTheRule:
    """§4: entry ``rsi_14 < 30 and close > sma_200``; exit ``rsi_14 > 50``."""

    @pytest.mark.parametrize(
        "closes",
        [RISING, FALLING, FLAT, CYCLE, DEEP_DIP],
        ids=["rising", "falling", "flat", "cycle", "deep_dip"],
    )
    def test_every_bar_matches_the_naive_reference(self, closes: Sequence[float | None]) -> None:
        entries, exits = _run(closes)
        expected = _reference_verdicts(closes)
        assert len(entries) == len(exits) == len(closes)
        for i, (entry_verdict, entry_reason, exit_verdict, exit_reason) in enumerate(expected):
            assert (entries[i].verdict, entries[i].reason) == (entry_verdict, entry_reason), f"entry bar {i}"
            assert (exits[i].verdict, exits[i].reason) == (exit_verdict, exit_reason), f"exit bar {i}"

    def test_a_rising_ramp_never_enters_and_always_exits(self) -> None:
        """A monotone rise has no losses, so RSI is 100 throughout: never
        oversold, always above the exit threshold."""
        entries, exits = _run(RISING)
        assert not any(s.verdict == "fired" for s in entries)
        assert all(s.verdict == "fired" for s in exits[SPEC_TREND_PERIOD - 1 : -1])

    def test_a_falling_ramp_fires_neither_leg(self) -> None:
        """RSI is 0 — oversold — but ``close < sma_200`` throughout, so the trend
        filter refuses it. This is the whole point of that conjunct: §4 calls it
        what distinguishes S-3 from *"catching a terminal decline"*, and a
        dropped trend filter fires on every bar here."""
        entries, exits = _run(FALLING)
        assert not any(s.verdict == "fired" for s in entries)
        assert not any(s.verdict == "fired" for s in exits)

    def test_a_dip_inside_an_uptrend_enters(self) -> None:
        entries, _ = _run(DEEP_DIP)
        assert entries[BOUNDARY_INDEX].verdict == "fired"

    def test_the_cycle_fixture_exercises_both_outcomes_on_both_legs(self) -> None:
        entries, exits = _run(CYCLE)
        assert {"fired", "not_fired"} <= {s.verdict for s in entries}
        assert {"fired", "not_fired"} <= {s.verdict for s in exits}


class TestStrictComparisons:
    """§4 writes all three comparisons strictly. A ``<=``/``>=`` on any one is
    only visible where THAT comparison is an equality and the others are not."""

    def test_an_rsi_of_exactly_thirty_does_not_enter(self) -> None:
        closes = RSI_ON_THRESHOLD
        i = BOUNDARY_INDEX
        rsi = _reference_rsi(closes, SPEC_RSI_PERIOD, i)
        trend = _reference_sma(closes, SPEC_TREND_PERIOD, i)
        close = closes[i]
        # ⚠ Asserted, not assumed. If the fixture ever drifts off the boundary
        # the test would still pass while testing nothing.
        assert rsi == SPEC_OVERSOLD
        assert trend is not None and close is not None and close > trend
        entries, _ = _run(closes)
        assert entries[i].verdict == "not_fired"

    def test_a_close_sitting_exactly_on_the_trend_average_does_not_enter(self) -> None:
        closes = CLOSE_ON_TREND
        i = BOUNDARY_INDEX
        rsi = _reference_rsi(closes, SPEC_RSI_PERIOD, i)
        trend = _reference_sma(closes, SPEC_TREND_PERIOD, i)
        close = closes[i]
        assert close == trend
        assert rsi is not None and rsi < SPEC_OVERSOLD
        entries, _ = _run(closes)
        assert entries[i].verdict == "not_fired"

    def test_an_rsi_of_exactly_fifty_does_not_exit(self) -> None:
        """The flat-series convention makes this boundary exactly reachable:
        ``technical_analysis.rsi`` returns 50.0 when average gain and loss are
        both zero. The exit is a SINGLE comparison, so a flat fixture does pin it
        — unlike either entry conjunct."""
        i = SPEC_TREND_PERIOD + 10
        assert _reference_rsi(FLAT, SPEC_RSI_PERIOD, i) == SPEC_EXIT
        _, exits = _run(FLAT)
        assert exits[i].verdict == "not_fired"


class TestWarmUp:
    """One warm-up, both legs — at the 200 bars ``sma_200`` implies."""

    def test_both_legs_share_the_trend_warm_up(self) -> None:
        """⚠ THE NARROWING, and it is wider than S-1's. Bar 100 has a warm
        ``rsi_14`` (from bar 14) and a cold ``sma_200``, so the exit condition IS
        computable there. It is refused anyway, because per-leg evaluability
        would make the same bar live for one leg and warming for the other —
        §3.1's branch-dependent evaluability one level up."""
        entries, exits = _run(RISING)
        cold = SPEC_RSI_PERIOD + 10
        assert cold < SPEC_TREND_PERIOD - 1
        assert (entries[cold].verdict, entries[cold].reason) == ("not_evaluable", "insufficient_warmup")
        assert (exits[cold].verdict, exits[cold].reason) == ("not_evaluable", "insufficient_warmup")

    def test_the_narrowed_span_is_the_rsi_boundary_to_the_trend_boundary(self) -> None:
        """The exact bars the shared warm-up costs the exit leg — 185 of them,
        against S-1's 150. ``--census`` counts these on the full population."""
        _, exits = _run(RISING)
        narrowed = [i for i in range(len(RISING) - 1) if exits[i].reason == "insufficient_warmup"]
        assert narrowed == list(range(SPEC_TREND_PERIOD - 1))
        computable_but_refused = [i for i in narrowed if i >= SPEC_RSI_PERIOD]
        assert computable_but_refused == list(range(SPEC_RSI_PERIOD, SPEC_TREND_PERIOD - 1))
        assert len(computable_but_refused) == 185

    def test_the_first_evaluable_bar_is_the_trend_period_boundary(self) -> None:
        entries, _ = _run(RISING)
        assert entries[SPEC_TREND_PERIOD - 2].verdict == "not_evaluable"
        assert entries[SPEC_TREND_PERIOD - 1].verdict != "not_evaluable"

    def test_a_series_shorter_than_the_trend_period_is_never_evaluable(self) -> None:
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

    def test_a_hole_refuses_the_whole_tail_not_a_two_hundred_bar_window(self) -> None:
        """⚠ S-3's ONE STRUCTURAL DIFFERENCE FROM S-1, and it is a property of
        Wilder smoothing rather than a decision made here: RSI carries state
        across every bar, so there is no window for a hole to clear. S-1's
        equivalent test asserts the refusal ENDS one bar past the window; here it
        never ends. The bias this creates is counted by ``--census``, not
        asserted away."""
        closes = list(RISING)
        closes[10] = None
        entries, _ = _run(closes)
        assert entries[10 + SPEC_TREND_PERIOD].reason == REASON
        assert all(s.reason == REASON for s in entries[10:-1])

    def test_a_reason_outside_the_closed_vocabulary_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown reason code"):
            s3_signals(_bars(RISING), universe=UNIVERSE, close_reason="looked_wrong")  # type: ignore[arg-type]


class TestLastBar:
    def test_the_final_bar_is_no_fill_bar_on_both_legs(self) -> None:
        """§3.5.1 has no ``t+1`` to fill at, so no decision on the last bar can
        be acted on — whichever way the condition would have gone."""
        entries, exits = _run(CYCLE)
        assert (entries[-1].verdict, entries[-1].reason) == ("not_evaluable", "no_fill_bar")
        assert (exits[-1].verdict, exits[-1].reason) == ("not_evaluable", "no_fill_bar")


class TestCausality:
    """Criterion 4: every value at bar ``t`` uses only bars <= ``t``. #2260
    candidate 1, and the one this strategy is nearest to."""

    def test_a_bars_verdict_is_unchanged_when_later_bars_are_removed(self) -> None:
        full_entries, full_exits = _run(CYCLE)
        # +2 so `index` is never the truncated series' own last bar, which is
        # `no_fill_bar` by construction and would compare a refusal against a
        # verdict.
        for index in range(SPEC_TREND_PERIOD - 1, len(CYCLE) - 1, 7):
            truncated_entries, truncated_exits = _run(CYCLE[: index + 2])
            assert truncated_entries[index].verdict == full_entries[index].verdict, f"entry {index}"
            assert truncated_exits[index].verdict == full_exits[index].verdict, f"exit {index}"

    def test_the_truncation_sweep_covers_bars_of_both_verdicts(self) -> None:
        """A sweep over a stretch where nothing changes cannot detect a
        look-ahead, so the fixture is asserted to vary across it — on BOTH legs,
        since they are truncated together."""
        full_entries, full_exits = _run(CYCLE)
        swept = range(SPEC_TREND_PERIOD - 1, len(CYCLE) - 1, 7)
        assert {"fired", "not_fired"} <= {full_entries[i].verdict for i in swept}
        assert {"fired", "not_fired"} <= {full_exits[i].verdict for i in swept}


class TestSpecConstants:
    """⚠ The bridge between §4's prose and the module's constants.

    Everything else in this file reasons in ``SPEC_*`` literals, so that the
    naive reference cannot move when the code does. That independence has a
    price: nothing would then notice the module quietly using a different
    number. This class is that notice, and it is the ONLY place the module's
    constants are read as values.
    """

    def test_the_modules_constants_are_the_specs_numbers(self) -> None:
        assert s3_module.RSI_PERIOD == SPEC_RSI_PERIOD
        assert s3_module.OVERSOLD_THRESHOLD == SPEC_OVERSOLD
        assert s3_module.EXIT_THRESHOLD == SPEC_EXIT
        assert s3_module.TREND_PERIOD == SPEC_TREND_PERIOD
        assert s3_module.MAX_HOLD_BARS == SPEC_MAX_HOLD

    def test_the_warm_up_requirement_is_the_trend_lookback(self) -> None:
        """§4 gives S-3 no explicit bar count; the rule names ``sma_200``, so the
        requirement is derived from the lookback rather than restated."""
        assert s3_module.WARMUP_BARS == SPEC_TREND_PERIOD


class TestIdentity:
    """Criterion 11: identity = code + config + data contract."""

    def test_a_blank_cost_model_id_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            s3_identity(universe=UNIVERSE, cost_model_id="   ")

    def test_the_version_moves_with_the_universe(self) -> None:
        a = s3_identity(universe="survivor_only", cost_model_id=COST_MODEL_ID)
        b = s3_identity(universe="survivorship_free", cost_model_id=COST_MODEL_ID)
        assert a.version != b.version

    def test_the_version_moves_with_the_cost_model(self) -> None:
        a = s3_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        b = s3_identity(universe=UNIVERSE, cost_model_id="static-p75-v1")
        assert a.version != b.version

    def test_the_identity_is_distinct_from_s1s(self) -> None:
        from app.services.strategies.s1_time_series_momentum import s1_identity

        s1 = s1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        s3 = s3_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        assert s3.strategy_id == S3_STRATEGY_ID != s1.strategy_id
        assert s3.version != s1.version

    def test_the_identity_carries_every_constant_the_rule_reads(self) -> None:
        """⚠ Including ``max_hold_bars``, which ``s3_signals`` does NOT evaluate.
        §4's exit is *"rsi_14 > 50, or 10 bars elapsed, whichever first"*; the
        second half is position state and cannot be a per-bar verdict, so it is
        carried as a hashed parameter instead of being dropped."""
        identity = s3_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        assert dict(identity.params) == {
            "rsi_period": 14,
            "oversold_threshold": 30.0,
            "exit_threshold": 50.0,
            "trend_period": 200,
            "max_hold_bars": 10,
        }
        assert dict(S3_PARAMS) == dict(identity.params)

    def test_the_source_hash_is_this_strategys_own_source(self) -> None:
        """⚠ Recomputed here from the FILE, not compared against the module's own
        helper: ``source_hash == _source_hash()`` holds just as well when both
        are a constant, which is the one defect this guards."""
        import app.services.strategies.s3_mean_reversion_in_trend as s3

        expected = hashlib.sha256(Path(s3.__file__).read_bytes()).hexdigest()[:12]
        assert s3_identity(universe=UNIVERSE, cost_model_id="x").source_hash == expected


class TestMaxHoldIsDeclaredNotDropped:
    """§4's *"or 10 bars elapsed"* — the half of the exit that is not a signal."""

    def test_the_cap_is_consumable_by_the_phase_4a_resolver(self) -> None:
        """⚠ The point of this test is that ``MAX_HOLD_BARS`` is not a dangling
        number. It is declared here and enforced there, so it is exercised
        against the field that enforces it: ``ExitLevels`` validates
        ``max_hold_bars >= 1`` and is what ``resolve_outcome`` counts bars
        against."""
        levels = ExitLevels(take_profit=Decimal("110"), stop_loss=Decimal("90"), max_hold_bars=MAX_HOLD_BARS)
        assert levels.max_hold_bars == SPEC_MAX_HOLD

    def test_no_signal_carries_hold_information(self) -> None:
        """The bar-count cap must not have leaked into the signal stream — a
        ``StrategySignal`` has an index, a kind and a reason, and nothing that
        could express "and hold for 10 bars"."""
        entries, exits = _run(CYCLE)
        for signal in (*entries, *exits):
            assert set(vars(signal)) == {"verdict", "signal_index", "kind", "reason"}


class TestLedgerRoundTrip:
    """The two legs have to survive 3c's writer, which is where the fill is
    resolved and where a same-bar collision would surface."""

    def _rows(self, closes: Sequence[float | None]) -> tuple[BarSeries, list[object]]:
        series = _bars(closes)
        rows = resolve_fills(
            s3_signals(series, universe=UNIVERSE, close_reason=REASON),
            series=series,
            identity=s3_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=1,
        )
        return series, list(rows)

    def test_both_legs_coexist_on_one_bar(self) -> None:
        _, rows = self._rows(CYCLE)
        assert len(rows) == 2 * len(CYCLE)
        keys = {(row.signal_bar_date, row.signal_kind) for row in rows}  # type: ignore[attr-defined]
        assert len(keys) == len(rows)

    def test_every_fired_row_fills_at_the_next_bars_open(self) -> None:
        series, rows = self._rows(CYCLE)
        fired = [row for row in rows if row.verdict == "fired"]  # type: ignore[attr-defined]
        assert fired
        by_date = {d: i for i, d in enumerate(series.dates)}
        for row in fired:
            index = by_date[row.signal_bar_date]  # type: ignore[attr-defined]
            assert row.fill_bar_date == series.dates[index + 1]  # type: ignore[attr-defined]
            assert row.fill_price == series.rows[index + 1]["open"]  # type: ignore[attr-defined]
