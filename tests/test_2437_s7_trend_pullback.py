"""S-7 trend pullback (#2437) — the first strategy with a stop and no target.

⚠ DB-free by design: everything here is a walk over a bar array, per the repo's
stated default of extracting the decision into a pure function and table-testing
it.

THE FIXTURE IS VALIDATED, NOT TRUSTED. A monotonic uptrend never dips RSI below
40 (no losses at all reads RSI 100), so the pullback's depth and the recovery's
size were tuned against ``rsi_series`` itself and the tests assert the fixture's
own premises (RSI crossed, trend gates hold) before asserting the verdict —
S-8's recorded pattern.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries, OHLCVRow, atr_series, rsi_series, sma_series
from app.services.market_regime import Regime, RegimeSeries
from app.services.strategies.s7_trend_pullback import (
    ATR_STOP_MULTIPLE,
    MAX_HOLD_BARS,
    PERMITTED_REGIMES,
    RSI_REENTRY_THRESHOLD,
    S7_PARAMS,
    S7_STRATEGY_ID,
    s7_exit_bracket,
    s7_identity,
    s7_signals,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST

U = "survivor_only"


def _bars(closes: list[float], *, start: date = date(2020, 1, 1)) -> BarSeries:
    """Closes with a half-point range either side; open tracks the close."""
    rows: list[OHLCVRow] = [
        {
            "open": Decimal(str(c)),
            "high": Decimal(str(c + 0.5)),
            "low": Decimal(str(c - 0.5)),
            "close": Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _regime(n: int, value: Regime | None = Regime.BULL_QUIET) -> RegimeSeries:
    return RegimeSeries(values=(value,) * n)


#: 210 rising bars, a 6-bar pullback deep enough to drag Wilder RSI below 40,
#: then a recovery whose first bar crosses back above it. Cross at index 216.
_UP_BARS = 210
_DIP_BARS = 6
_CROSS = _UP_BARS + _DIP_BARS


def _pullback() -> list[float]:
    closes = [100 + 0.5 * i for i in range(_UP_BARS)]
    top = closes[-1]
    closes.extend(top - 1.5 * j for j in range(1, _DIP_BARS + 1))
    bottom = closes[-1]
    closes.extend(bottom + 2.0 * j for j in range(1, 8))
    return closes


class TestS7FiresOnTheRuleAsWritten:
    @staticmethod
    def _entry_verdicts(closes: list[float], regime: RegimeSeries | None = None) -> list[str]:
        signals = s7_signals(
            _bars(closes),
            universe=U,
            masked_reason="quarantined_bar",
            regime=regime if regime is not None else _regime(len(closes)),
        )
        return [s.verdict for s in signals[: len(closes)]]

    def test_the_fixture_actually_crosses(self) -> None:
        """The premise, asserted rather than trusted — see the module docstring."""
        series = _bars(_pullback())
        rsi = rsi_series(series, universe=U, period=14)
        now, prior = rsi.values[_CROSS], rsi.values[_CROSS - 1]
        assert now is not None and now > RSI_REENTRY_THRESHOLD
        assert prior is not None and prior <= RSI_REENTRY_THRESHOLD
        sma50 = sma_series(series, universe=U, period=50).values[_CROSS]
        sma200 = sma_series(series, universe=U, period=200).values[_CROSS]
        assert sma50 is not None and sma200 is not None and sma50 > sma200
        close = series.float_closes[_CROSS]
        assert close is not None and close > sma200

    def test_it_fires_on_the_crossing_bar(self) -> None:
        assert self._entry_verdicts(_pullback())[_CROSS] == "fired"

    def test_it_does_not_fire_while_rsi_is_still_below(self) -> None:
        verdicts = self._entry_verdicts(_pullback())
        assert verdicts[_CROSS - 1] == "not_fired"

    def test_it_does_not_refire_while_rsi_stays_above(self) -> None:
        """⚠ The crossing is an EDGE, not a state — a state re-fires on every
        bar of the recovery (S-6's measured defect, one strategy over)."""
        verdicts = self._entry_verdicts(_pullback())
        assert verdicts[_CROSS + 1 :].count("fired") == 0

    def test_a_monotonic_uptrend_never_fires(self) -> None:
        """No pullback, no signal: RSI never leaves the top of its range."""
        assert "fired" not in self._entry_verdicts([100 + 0.5 * i for i in range(230)])

    def test_a_refused_regime_blocks_it(self) -> None:
        assert Regime.BEAR_QUIET not in PERMITTED_REGIMES
        closes = _pullback()
        assert "fired" not in self._entry_verdicts(closes, _regime(len(closes), Regime.BEAR_QUIET))

    def test_bull_volatile_is_permitted(self) -> None:
        """⚠ Unlike S-6: a Bulge is hostile to breakouts, not to buying a dip
        inside an intact uptrend. §3 states the pair outright."""
        closes = _pullback()
        assert self._entry_verdicts(closes, _regime(len(closes), Regime.BULL_VOLATILE))[_CROSS] == "fired"

    def test_an_unknown_regime_is_not_evaluable_not_not_fired(self) -> None:
        """#2437's contract, inherited by every strategy in the set."""
        closes = _pullback()
        regime = RegimeSeries(
            values=tuple(None if i == _CROSS else Regime.BULL_QUIET for i in range(len(closes))),
            not_evaluable_indices=(_CROSS,),
        )
        signals = s7_signals(_bars(closes), universe=U, masked_reason="quarantined_bar", regime=regime)
        assert (signals[_CROSS].verdict, signals[_CROSS].reason) == ("not_evaluable", "missing_market_context")

    def test_below_the_trend_sma_never_fires(self) -> None:
        """An RSI recovery inside a downtrend is not a pullback — there is no
        trend to pull back FROM, and the 200-SMA gate says so."""
        closes = [300 - 0.8 * i for i in range(210)]
        bottom = closes[-1]
        closes.extend(bottom + 2.0 * j for j in range(1, 8))
        assert "fired" not in self._entry_verdicts(closes)


class TestS7ExitLeg:
    def test_it_fires_below_the_50_sma_and_not_above(self) -> None:
        """⚠ The exit leg declares the same four instrument inputs as the entry
        (S-3's uniformity rule), so it is live only from the 200-SMA's warmup —
        the fixture must run past bar 199 before the verdicts mean anything."""
        closes = [100 + 0.5 * i for i in range(210)]
        closes.extend(closes[-1] - 3.0 * j for j in range(1, 41))
        series = _bars(closes)
        sma50 = sma_series(series, universe=U, period=50)
        signals = s7_signals(series, universe=U, masked_reason="quarantined_bar", regime=_regime(len(closes)))
        exits = signals[len(closes) :]
        assert all(s.kind == "exit" for s in exits)
        judged = [i for i, s in enumerate(exits) if s.verdict != "not_evaluable"]
        below = [i for i in judged if (v := sma50.values[i]) is not None and closes[i] < v]
        above = [i for i in judged if (v := sma50.values[i]) is not None and closes[i] > v]
        assert below and above, "fixture must exercise both sides of the 50-SMA"
        assert all(exits[i].verdict == "fired" for i in below)
        assert all(exits[i].verdict == "not_fired" for i in above)

    def test_the_exit_is_evaluable_without_the_regime(self) -> None:
        """⚠⚠ THE POINT OF THE ASYMMETRIC INPUT DECLARATION. A missing benchmark
        session refuses the ENTRY (`missing_market_context`) and must NOT refuse
        the exit verdict for a position that is already open — `close < 50-SMA`
        reads nothing from the benchmark."""
        closes = _pullback()
        regime = RegimeSeries(
            values=tuple(None for _ in closes),
            not_evaluable_indices=tuple(range(len(closes))),
        )
        signals = s7_signals(_bars(closes), universe=U, masked_reason="quarantined_bar", regime=regime)
        entries, exits = signals[: len(closes)], signals[len(closes) :]
        assert entries[_CROSS].verdict == "not_evaluable"
        assert entries[_CROSS].reason == "missing_market_context"
        assert exits[_CROSS].verdict in {"fired", "not_fired"}


class TestS7Bracket:
    def test_the_stop_is_entry_minus_two_atr_and_there_is_no_target(self) -> None:
        closes = _pullback()
        series = _bars(closes)
        atr = atr_series(series, universe=U, period=14).values[_CROSS]
        assert atr is not None
        entry_price = Decimal("200")
        target, stop, max_hold = s7_exit_bracket(series, signal_index=_CROSS, entry_price=entry_price, universe=U)
        assert target is None
        assert stop == entry_price - Decimal(str(ATR_STOP_MULTIPLE * atr))
        assert max_hold == MAX_HOLD_BARS

    def test_the_adapter_builds_a_stop_only_bracket(self) -> None:
        entry = STRATEGY_MANIFEST[S7_STRATEGY_ID]
        assert entry.exit_levels is not None
        levels = entry.exit_levels(_bars(_pullback()), signal_index=_CROSS, entry_price=Decimal("200"), universe=U)
        assert not isinstance(levels, str)
        assert levels.take_profit is None
        assert levels.max_hold_bars == MAX_HOLD_BARS

    def test_a_nonpositive_stop_is_refused_not_raised(self) -> None:
        """A low-priced high-ATR name puts `entry - 2 x ATR` at or below zero;
        `ExitLevels` refuses that state, so the adapter must return the typed
        refusal rather than abort the whole outcome batch."""
        entry = STRATEGY_MANIFEST[S7_STRATEGY_ID]
        assert entry.exit_levels is not None
        result = entry.exit_levels(_bars(_pullback()), signal_index=_CROSS, entry_price=Decimal("0.5"), universe=U)
        assert result == "unorderable_exit_levels"

    def test_an_unevaluable_signal_bar_is_refused_not_raised(self) -> None:
        entry = STRATEGY_MANIFEST[S7_STRATEGY_ID]
        assert entry.exit_levels is not None
        result = entry.exit_levels(_bars(_pullback()), signal_index=3, entry_price=Decimal("100"), universe=U)
        assert result == "unorderable_exit_levels"


class TestS7ExitRegime:
    def test_the_first_hybrid_regime_declares_both_close_sources(self) -> None:
        """S-7's §3 shape: a stop (level) AND a `close < 50-SMA` rule (signal
        pair) AND a hold cap. `build_positions` evaluates all declared sources
        together and the earliest wins, so the combination needs no new code."""
        entry = STRATEGY_MANIFEST[S7_STRATEGY_ID]
        regime = entry.exit_regime(entry.decision_calendar(()))
        assert regime.signal_pair is True
        assert regime.level_based is True
        assert regime.max_hold_bars == MAX_HOLD_BARS
        assert regime.rebalance_dates is None
        assert entry.signal_kinds == frozenset({"entry", "exit"})


class TestS7Identity:
    def test_the_identity_names_this_strategy(self) -> None:
        identity = s7_identity(universe=U, cost_model_id="cost-v1")
        assert identity.strategy_id == S7_STRATEGY_ID
        assert identity.params == S7_PARAMS

    def test_an_empty_cost_model_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cost_model_id must be a non-empty declaration"):
            s7_identity(universe=U, cost_model_id="  ")

    def test_the_regime_rule_version_is_inside_the_identity(self) -> None:
        """⚠ Criterion 11: the same bars under a different regime boundary
        produce different signals, so the boundary is part of what this
        strategy IS."""
        assert "regime_rule_version" in S7_PARAMS
