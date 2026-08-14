"""S-6 — resistance breakout with volume confirmation (#2437).

Modules under test: ``app/services/strategies/s6_resistance_breakout.py``,
``app/services/market_context.py``, and the ``LevelScan`` hoist in
``app/services/price_levels.py``.

⚠ THE EXPECTED VALUES ARE CONSTRUCTED, NOT IMPORTED, wherever a constant is the
thing under test. ``test_strategy_manifest``'s module docstring states the rule:
*"a reference that imports the constant it validates is a tautology"*. So the
volume-boundary tests build a fixture at exactly 1.2x and exactly below it from
literals, rather than asserting ``ratio >= VOLUME_MULTIPLE``.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pytest

from app.services.indicator_series import BarSeries, atr_series
from app.services.market_context import BENCHMARK_SYMBOL, MarketContext
from app.services.market_regime import Regime
from app.services.price_levels import LevelScan, levels_at, swing_pivots
from app.services.strategies.s6_resistance_breakout import (
    PERMITTED_REGIMES,
    S6_PARAMS,
    S6_STRATEGY_ID,
    prior_close_series,
    s6_exit_bracket,
    s6_identity,
    s6_signals,
    volume_ratio_series,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"
START = date(2020, 1, 1)


def _bars(
    highs: Sequence[float | None],
    lows: Sequence[float | None],
    closes: Sequence[float | None],
    volumes: Sequence[int | None],
) -> BarSeries:
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(c)),
            "high": None if h is None else Decimal(str(h)),
            "low": None if lo is None else Decimal(str(lo)),
            "close": None if c is None else Decimal(str(c)),
            "volume": v,
        }  # type: ignore[typeddict-item]
        for h, lo, c, v in zip(highs, lows, closes, volumes, strict=True)
    ]
    return BarSeries(dates=tuple(START + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _all_bull(series: BarSeries) -> MarketContext:
    """Every session classified ``bull_quiet``, so the regime gate never binds."""
    return MarketContext(
        regime_by_date={when: Regime.BULL_QUIET for when in series.dates},
        first_classified=series.dates[0],
        benchmark_instrument_id=1,
        benchmark_symbol=BENCHMARK_SYMBOL,
    )


class TestVolumeWindowBoundary:
    """⚠ THE WINDOW EXCLUDES ``t`` AND THE COMPARISON IS ``>=``.

    Both are stated in the module docstring and neither is observable from a
    happy-path fixture, so they are pinned at the boundary. The exclusion is
    what stops a breakout bar's own volume inflating the average it must clear.
    """

    @staticmethod
    def _series(volumes: Sequence[int]) -> BarSeries:
        n = len(volumes)
        return _bars([10.0] * n, [9.0] * n, [9.5] * n, list(volumes))

    def test_the_average_excludes_the_bar_itself(self) -> None:
        # Twenty bars of 100, then one of 1,000. Excluding t: 1000/100 = 10.0.
        # Including t would give 1000 / ((20*100 + 1000)/21) = 7.0.
        series = self._series([100] * 20 + [1_000])
        ratio = volume_ratio_series(series, universe=UNIVERSE).values[20]
        assert ratio == pytest.approx(10.0)

    def test_exactly_the_multiple_qualifies_and_a_hair_under_does_not(self) -> None:
        """§3 says ``volume(t) >= 1.2 x``, so 120 on an average of 100 fires."""
        at_the_bound = self._series([100] * 20 + [120])
        just_under = self._series([100] * 20 + [119])
        assert volume_ratio_series(at_the_bound, universe=UNIVERSE).values[20] == pytest.approx(1.2)
        assert volume_ratio_series(just_under, universe=UNIVERSE).values[20] == pytest.approx(1.19)

    def test_the_first_full_window_is_index_twenty(self) -> None:
        series = self._series([100] * 30)
        values = volume_ratio_series(series, universe=UNIVERSE).values
        assert values[19] is None, "index 19 has only 19 prior bars — warm-up"
        assert values[20] is not None

    def test_a_missing_volume_is_a_data_refusal_not_warm_up(self) -> None:
        """⚠ Criterion 8: a present-and-empty field is a gap, not a warm window."""
        volumes: list[int | None] = [100] * 30
        volumes[5] = None
        n = len(volumes)
        series = _bars([10.0] * n, [9.0] * n, [9.5] * n, volumes)
        result = volume_ratio_series(series, universe=UNIVERSE)
        # Bars 20..25 have index 5 inside their trailing window.
        assert set(range(20, 26)) <= set(result.not_evaluable_indices)
        assert 26 not in result.not_evaluable_indices

    def test_an_all_zero_window_is_refused_rather_than_dividing(self) -> None:
        """A halted name must not report every later bar as an infinite surge."""
        series = self._series([0] * 20 + [5])
        assert 20 in volume_ratio_series(series, universe=UNIVERSE).not_evaluable_indices


class TestPriorCloseSeries:
    def test_index_zero_is_warm_up_not_a_refusal(self) -> None:
        series = _bars([10.0] * 4, [9.0] * 4, [9.5] * 4, [100] * 4)
        result = prior_close_series(series, universe=UNIVERSE)
        assert result.values[0] is None
        assert 0 not in result.not_evaluable_indices
        assert result.values[1] == pytest.approx(9.5)

    def test_a_masked_previous_close_refuses_the_next_bar(self) -> None:
        series = _bars([10.0] * 4, [9.0] * 4, [9.5, None, 9.5, 9.5], [100] * 4)
        result = prior_close_series(series, universe=UNIVERSE)
        assert 2 in result.not_evaluable_indices
        assert 3 not in result.not_evaluable_indices


class TestRegimeGate:
    """⚠ THE THREE STATES OF ``gate_series``, WHICH IS THE WHOLE POINT OF IT."""

    @staticmethod
    def _dates(n: int) -> tuple[date, ...]:
        return tuple(START + timedelta(days=i) for i in range(n))

    def test_a_permitted_regime_is_one_and_a_refused_one_is_zero_not_none(self) -> None:
        """⚠ A refused regime is a VERDICT. Reporting it unevaluable would delete
        the denominator that shows the gate working (spec §0 rule 2)."""
        dates = self._dates(3)
        context = MarketContext(
            regime_by_date={dates[0]: Regime.BULL_QUIET, dates[1]: Regime.BEAR_QUIET, dates[2]: Regime.BULL_VOLATILE},
            first_classified=dates[0],
            benchmark_instrument_id=1,
            benchmark_symbol="SPY",
        )
        gate = context.gate_series(dates, allowed=frozenset({Regime.BULL_QUIET}), universe=UNIVERSE)
        assert gate.values == (1.0, 0.0, 0.0)
        assert gate.not_evaluable_indices == ()

    def test_before_the_first_classified_bar_is_warm_up_after_it_is_a_gap(self) -> None:
        """⚠⚠ THE SPLIT THE ELEVENTH REASON CODE EXISTS FOR. Both are ``None``;
        only one is listed, and the registry reads the difference."""
        dates = self._dates(4)
        context = MarketContext(
            regime_by_date={dates[1]: Regime.BULL_QUIET},
            first_classified=dates[1],
            benchmark_instrument_id=1,
            benchmark_symbol="SPY",
        )
        gate = context.gate_series(dates, allowed=frozenset({Regime.BULL_QUIET}), universe=UNIVERSE)
        assert gate.values == (None, 1.0, None, None)
        assert 0 not in gate.not_evaluable_indices, "before the benchmark started — warm-up"
        assert {2, 3} <= set(gate.not_evaluable_indices), "after it started, with no session — a hole"

    def test_an_empty_allowed_set_is_rejected(self) -> None:
        dates = self._dates(2)
        context = MarketContext(
            regime_by_date={dates[0]: Regime.BULL_QUIET},
            first_classified=dates[0],
            benchmark_instrument_id=1,
            benchmark_symbol="SPY",
        )
        with pytest.raises(ValueError, match="can never fire"):
            context.gate_series(dates, allowed=frozenset(), universe=UNIVERSE)


class TestLevelScanReproducesTheScalarForm:
    """⚠ The hoist is a PERFORMANCE change and must not be a behavioural one.

    ``levels_at`` now builds a ``LevelScan`` and calls ``at``, so there is one
    code path — but the equivalence that made the hoist legal (a pivot's verdict
    depends only on its own +/- 5 bars, never on where the observer stands) is
    the claim, and it is asserted rather than argued.
    """

    @staticmethod
    def _wavy(n: int = 120) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        i = np.arange(n, dtype=float)
        highs = 100.0 + 5.0 * np.sin(i / 3.0) + 0.4 * np.sin(i / 11.0)
        lows = highs - 2.0
        volumes = np.full(n, 1_000.0)
        return highs, lows, volumes

    def test_the_hoisted_and_scalar_forms_agree_at_every_index(self) -> None:
        highs, lows, volumes = self._wavy()
        scan = LevelScan.build(highs=highs, lows=lows, volumes=volumes)
        seen = 0
        for index in range(highs.size):
            hoisted = scan.at(atr=1.5, index=index)
            scalar = levels_at(highs=highs, lows=lows, volumes=volumes, atr=1.5, index=index)
            assert hoisted == scalar
            seen += len(hoisted)
        assert seen > 0, "a fixture with no levels would make this vacuous"

    def test_a_pivot_is_never_reported_before_it_is_confirmed(self) -> None:
        """⚠⚠ The lookahead this whole construction exists to prevent. The last
        candidate is ``index - 5``, so a pivot at ``index`` is unknowable."""
        highs, lows, _ = self._wavy()
        pivots = swing_pivots(highs, lows)
        assert pivots.high_indices, "no pivots detected — the fixture proves nothing"
        for index in range(20, highs.size):
            live = LevelScan.build(highs=highs, lows=lows, volumes=None).at(atr=1.5, index=index)
            for level in live:
                assert level.last_touch_index <= index - pivots.half_window

    def test_ragged_inputs_raise_rather_than_returning_nothing(self) -> None:
        with pytest.raises(ValueError, match="must align"):
            LevelScan.build(highs=np.zeros(5), lows=np.zeros(4), volumes=None)


class TestS6Fires:
    """A constructed breakout, and the four ways it is refused."""

    @staticmethod
    def _breakout_series(*, final_volume: int = 5_000, final_close: float = 108.0) -> BarSeries:
        """Three touches of a ~105 resistance, then a close through it.

        Built rather than sampled: the level has to be a real 3-touch cluster or
        the fixture proves nothing about the rule.
        """
        highs: list[float] = []
        lows: list[float] = []
        closes: list[float] = []
        for i in range(60):
            # Three symmetric peaks at 105, each isolated by 5 quiet bars.
            if i in (15, 30, 45):
                high = 105.0
            elif i % 15 in (7, 8):
                high = 99.0
            else:
                high = 101.0 + (i % 3) * 0.2
            highs.append(high)
            lows.append(high - 3.0)
            closes.append(high - 1.5)
        highs.append(final_close + 1.0)
        lows.append(final_close - 2.0)
        closes.append(final_close)
        highs.append(final_close + 1.0)
        lows.append(final_close - 2.0)
        closes.append(final_close)
        volumes = [1_000] * 60 + [final_volume, 1_000]
        return _bars(highs, lows, closes, volumes)

    def test_the_fixture_fires(self) -> None:
        series = self._breakout_series()
        signals = s6_signals(series, universe=UNIVERSE, masked_reason=REASON, market=_all_bull(series))
        assert any(signal.verdict == "fired" for signal in signals), (
            "the fixture never fires, so every refusal test below would pass vacuously"
        )

    def test_thin_volume_refuses_the_same_bar(self) -> None:
        """1,000 against a 1,000 average is a ratio of 1.0, under the 1.2 bar."""
        series = self._breakout_series(final_volume=1_000)
        signals = s6_signals(series, universe=UNIVERSE, masked_reason=REASON, market=_all_bull(series))
        assert not any(signal.verdict == "fired" for signal in signals)

    def test_no_close_through_the_level_refuses(self) -> None:
        series = self._breakout_series(final_close=102.0)
        signals = s6_signals(series, universe=UNIVERSE, masked_reason=REASON, market=_all_bull(series))
        assert not any(signal.verdict == "fired" for signal in signals)

    def test_a_refused_regime_is_not_fired_not_unevaluable(self) -> None:
        """⚠ The bar was JUDGED and declined — it must not vanish into a refusal."""
        series = self._breakout_series()
        bear = MarketContext(
            regime_by_date={when: Regime.BEAR_QUIET for when in series.dates},
            first_classified=series.dates[0],
            benchmark_instrument_id=1,
            benchmark_symbol="SPY",
        )
        signals = s6_signals(series, universe=UNIVERSE, masked_reason=REASON, market=bear)
        assert not any(signal.verdict == "fired" for signal in signals)
        assert signals[60].verdict == "not_fired"
        assert signals[60].reason is None

    def test_a_missing_benchmark_session_refuses_with_the_eleventh_code(self) -> None:
        """⚠ NOT ``not_fired``. The strategy could not decide, and the reason
        names whose data was missing."""
        series = self._breakout_series()
        holed = MarketContext(
            regime_by_date={when: Regime.BULL_QUIET for when in series.dates if when != series.dates[60]},
            first_classified=series.dates[0],
            benchmark_instrument_id=1,
            benchmark_symbol="SPY",
        )
        signals = s6_signals(series, universe=UNIVERSE, masked_reason=REASON, market=holed)
        assert signals[60].verdict == "not_evaluable"
        assert signals[60].reason == "missing_market_context"

    def test_the_permitted_set_is_bull_quiet_only(self) -> None:
        """⚠ §3 excludes ``bull_volatile`` BY NAME — breakouts into a Bulge are
        the classic false-break regime. Pinned as a literal, not imported."""
        assert {regime.value for regime in PERMITTED_REGIMES} == {"bull_quiet"}


class TestS6ExitBracket:
    """⚠⚠ THE STOP IS ANCHORED TO THE LEVEL, THE TARGET TO THE ENTRY."""

    def test_the_stop_is_measured_from_the_level_and_the_target_from_the_entry(self) -> None:
        series = TestS6Fires._breakout_series()
        signal_index = 60
        entry_price = Decimal("108.5")
        target, stop, max_hold = s6_exit_bracket(
            series, signal_index=signal_index, entry_price=entry_price, universe=UNIVERSE
        )
        atr = atr_series(series, universe=UNIVERSE, period=14).values[signal_index]
        assert atr is not None
        assert target == entry_price + Decimal("3.0") * Decimal(str(atr))
        assert max_hold == 40

        # ⚠ The stop is NOT entry-anchored, and this is the assertion that says
        # so: adding the one ATR back must land exactly on a live RESISTANCE
        # level, which no entry-relative stop could do.
        implied_level = float(stop) + atr
        live = LevelScan.build(
            highs=series.array_highs,
            lows=series.array_lows,
            volumes=np.array([float(row["volume"] or 0) for row in series.rows]),
        ).at(atr=atr, index=signal_index)
        resistances = [level.price for level in live if level.kind == "resistance"]
        assert any(implied_level == pytest.approx(price) for price in resistances), (
            f"stop+ATR = {implied_level} is not one of the live resistances {resistances}"
        )
        assert float(stop) < float(entry_price) - atr, "an entry-anchored 1xATR stop would sit higher"

    def test_a_bracket_is_refused_for_a_bar_that_cannot_have_fired(self) -> None:
        """⚠ Asking for a bracket where no level exists is a caller bug, not a
        default. Silently returning an entry-anchored stop would be S-4's rule
        wearing S-6's name."""
        n = 40
        flat = _bars([10.0] * n, [9.0] * n, [9.5] * n, [100] * n)
        with pytest.raises(ValueError, match="no live resistance"):
            s6_exit_bracket(flat, signal_index=30, entry_price=Decimal("9.5"), universe=UNIVERSE)

    def test_the_manifest_converts_every_refusal_into_the_resolver_vocabulary(self) -> None:
        """The adapter must not leak ``ValueError`` into the outcome pipeline."""
        n = 40
        flat = _bars([10.0] * n, [9.0] * n, [9.5] * n, [100] * n)
        entry = STRATEGY_MANIFEST[S6_STRATEGY_ID]
        assert entry.exit_levels is not None
        assert (
            entry.exit_levels(flat, signal_index=30, entry_price=Decimal("9.5"), universe=UNIVERSE)
            == "unorderable_exit_levels"
        )


class TestSegmentLocalStateIsPreservedAcrossTheSignalExitBoundary:
    """⚠⚠ CODEX CHECKPOINT-2 P1, MEASURED AND REBUTTED — and pinned so it stays so.

    The claim: ``segmented_signals`` evaluates S-6 inside a price-scale segment
    with fresh indicator state, while the outcome resolver later calls
    ``s6_exit_bracket`` with the FULL series — so a signal that fired on
    segment-local ATR and levels would get a bracket built from pre-break
    history, and could even get a spurious ``unorderable_exit_levels``.

    It does not happen, because BOTH consumers segment before calling:

    * ``strategy_outcome_resolution._resolve_one`` calls ``segment_for_index``
      and passes ``(signal_segment, local_signal_index)``;
    * ``backtest_run._exit_levels_for_entries`` walks ``series_segment_bounds``,
      rebuilds a ``BarSeries`` per segment and passes the local index.

    Segmentation is the CALLER's job and is uniform across S-4 and S-6, so the
    bracket sees exactly the bars the signal did. That is a property of code this
    branch does not own, which is precisely why it is asserted here rather than
    argued: if either caller ever stops segmenting, this fails.
    """

    @staticmethod
    def _series_with_a_scale_break() -> tuple[BarSeries, date]:
        """A breakout series preceded by history at a 10x different price scale."""
        tail = TestS6Fires._breakout_series()
        n_pre = 40
        pre_highs = [1_000.0 + (i % 5) for i in range(n_pre)]
        highs = pre_highs + [float(row["high"]) for row in tail.rows]  # type: ignore[arg-type]
        lows = [h - 30.0 for h in pre_highs] + [float(row["low"]) for row in tail.rows]  # type: ignore[arg-type]
        closes = [h - 15.0 for h in pre_highs] + [float(row["close"]) for row in tail.rows]  # type: ignore[arg-type]
        volumes: list[int | None] = [1_000] * n_pre + [row["volume"] for row in tail.rows]
        joined = _bars(highs, lows, closes, volumes)
        return joined, joined.dates[n_pre]

    def test_the_bracket_uses_the_segment_the_signal_fired_in(self) -> None:
        from app.services.price_segments import segment_for_index

        series, break_date = self._series_with_a_scale_break()
        segment, local_index = segment_for_index(series, index=100, unresolved_breaks=[break_date])
        entry_price = Decimal("108.5")

        segment_bracket = s6_exit_bracket(segment, signal_index=local_index, entry_price=entry_price, universe=UNIVERSE)
        full_bracket = s6_exit_bracket(series, signal_index=100, entry_price=entry_price, universe=UNIVERSE)

        # ⚠ The two DISAGREE, which is what makes the caller's segmentation
        # load-bearing rather than decorative. A fixture where they matched
        # would assert nothing.
        assert segment_bracket != full_bracket, (
            "the pre-break history changed neither ATR nor the level, so this fixture cannot "
            "detect a caller that forgot to segment"
        )

    def test_both_consumers_pass_a_segment_local_series(self) -> None:
        """⚠ Read from the SOURCE, not from a docstring — the rebuttal above is
        only true while these two call sites keep doing it."""
        from pathlib import Path

        from app.services import backtest_run, strategy_outcome_resolution

        resolution = Path(strategy_outcome_resolution.__file__).read_text()
        assert "segment_for_index(" in resolution
        assert "entry.exit_levels(\n        signal_segment," in resolution

        backtest = Path(backtest_run.__file__).read_text()
        assert "for start, end in series_segment_bounds(series, unresolved_breaks=unresolved_breaks):" in backtest
        assert "signal_series = BarSeries(dates=series.dates[start:end], rows=series.rows[start:end])" in backtest


class TestS6Identity:
    def test_the_benchmark_and_the_regime_domain_are_in_the_identity(self) -> None:
        """⚠⚠ Neither travels via ``INPUT_RULE_SETS``, which names the CLASSIFIER
        and not the series it ran over. An S-6 gated on QQQ would otherwise share
        this one's version with an identical source hash."""
        assert S6_PARAMS["benchmark_symbol"] == "SPY"
        assert S6_PARAMS["permitted_regimes"] == ["bull_quiet"]

    def test_a_blank_cost_model_is_refused(self) -> None:
        with pytest.raises(ValueError, match="non-empty declaration"):
            s6_identity(universe=UNIVERSE, cost_model_id="   ")


class TestManifestWiring:
    def test_s6_declares_that_it_requires_a_market_context(self) -> None:
        assert STRATEGY_MANIFEST[S6_STRATEGY_ID].requires_market_context is True

    def test_no_other_strategy_claims_to(self) -> None:
        gated = {key for key, entry in STRATEGY_MANIFEST.items() if entry.requires_market_context}
        assert gated == {S6_STRATEGY_ID}

    def test_the_adapter_refuses_rather_than_scanning_ungated(self) -> None:
        """⚠ FAIL-CLOSED IN THE RIGHT DIRECTION. Running S-6 without its gate
        produces MORE signals, in markets its rule excludes by name."""
        entry = STRATEGY_MANIFEST[S6_STRATEGY_ID]
        assert entry.signals is not None
        series = TestS6Fires._breakout_series()
        with pytest.raises(ValueError, match="gated on the market regime"):
            entry.signals(series, universe=UNIVERSE, masked_reason=REASON, market=None)

    def test_the_uniform_call_equals_the_direct_call(self) -> None:
        entry = STRATEGY_MANIFEST[S6_STRATEGY_ID]
        assert entry.signals is not None
        series = TestS6Fires._breakout_series()
        market = _all_bull(series)
        via_manifest = entry.signals(series, universe=UNIVERSE, masked_reason=REASON, market=market)
        assert via_manifest == s6_signals(series, universe=UNIVERSE, masked_reason=REASON, market=market)
        assert any(signal.verdict == "fired" for signal in via_manifest)


class TestBenchmarkFreshnessIsSomebodysJob:
    """⚠ The #1818 prevention entry, enforced rather than restated.

    *"when a read path pins a specific symbol/entity, that symbol must appear in
    … the ingest scope that maintains its data — and a test should pin the
    cross-reference"*. SPX500 froze for weeks because nothing did this.
    """

    def test_the_benchmark_symbol_is_in_the_refresh_scope(self) -> None:
        from app.workers.scheduler import BENCHMARK_SYMBOLS

        assert BENCHMARK_SYMBOL in BENCHMARK_SYMBOLS, (
            f"{BENCHMARK_SYMBOL} gates every regime-aware strategy but is not in the candle-refresh scope, "
            "so its series would freeze silently and the gate would go blind"
        )
