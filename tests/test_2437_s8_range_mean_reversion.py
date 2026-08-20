"""S-8 range mean reversion, and the Wilder ADX it needs (#2437).

Two subjects, deliberately in one file: ``adx_series`` exists only because S-8
needs it, and testing an indicator apart from the rule that consumes it is how
an indicator ends up correct in isolation and wrong in use.

⚠⚠ THE ADX CROSS-CHECK IS AN INDEPENDENT IMPLEMENTATION, NOT A GOLDEN FILE.
``adx_series`` smooths with the AVERAGE form (matching ``atr_series``);
``_wilder_reference`` below smooths with the RUNNING-SUM form Wilder actually
publishes (``S = S_prev - S_prev/period + current``). The two are algebraically
equivalent for ``+DI``/``-DI`` because those are ratios, but they are written
independently and neither is derived from the other — so agreement between them
is evidence rather than a tautology. A golden file produced by this module would
have proved only that it still does what it did.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries, OHLCVRow, adx_series, bollinger_series
from app.services.market_regime import Regime, RegimeSeries
from app.services.strategies.s8_range_mean_reversion import (
    ADX_PERIOD,
    ADX_TREND_CEILING,
    MAX_HOLD_BARS,
    PERMITTED_REGIMES,
    S8_PARAMS,
    S8_STRATEGY_ID,
    WARMUP_BARS,
    s8_exit_bracket,
    s8_identity,
    s8_signals,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST

U = "survivor_only"


def _bars(rows: list[tuple[float, float, float]], *, start: date = date(2020, 1, 1)) -> BarSeries:
    """``(high, low, close)`` per bar; open tracks the close."""
    built: list[OHLCVRow] = [
        {
            "open": Decimal(str(c)),
            "high": Decimal(str(h)),
            "low": Decimal(str(low)),
            "close": Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for h, low, c in rows
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(rows))), rows=tuple(built))


def _trending(n: int, *, step: float = 1.0) -> list[tuple[float, float, float]]:
    return [(100 + i * step + 0.5, 100 + i * step - 0.5, 100 + i * step) for i in range(n)]


def _ranging(n: int, *, amplitude: float = 3.0) -> list[tuple[float, float, float]]:
    out = []
    for i in range(n):
        mid = 100 + amplitude * math.sin(i * math.pi / 6)
        out.append((mid + 0.5, mid - 0.5, mid))
    return out


# ---------------------------------------------------------------------------
# Wilder's ADX
# ---------------------------------------------------------------------------


def _wilder_reference(rows: list[tuple[float, float, float]], period: int) -> list[float | None]:
    """ADX in Wilder's own RUNNING-SUM smoothing, written from the book's steps.

    ⚠ Independent of ``adx_series`` on purpose — see the module docstring. The
    only thing shared is the definition, which is the thing being checked.
    """
    n = len(rows)
    tr: list[float] = [0.0] * n
    pdm: list[float] = [0.0] * n
    mdm: list[float] = [0.0] * n
    for i in range(1, n):
        high, low, _ = rows[i]
        prev_high, prev_low, prev_close = rows[i - 1]
        tr[i] = max(high - low, abs(high - prev_close), abs(low - prev_close))
        up, down = high - prev_high, prev_low - low
        if up > down and up > 0:
            pdm[i] = up
        if down > up and down > 0:
            mdm[i] = down

    out: list[float | None] = [None] * n
    if n <= 2 * period - 1:
        return out
    # Wilder seeds each smoothed SUM with the sum of the first `period` values.
    s_tr, s_p, s_m = sum(tr[1 : period + 1]), sum(pdm[1 : period + 1]), sum(mdm[1 : period + 1])
    dx: list[float | None] = [None] * n

    def _dx(t: float, p: float, m: float) -> float | None:
        if t <= 0:
            return None
        pdi, mdi = 100.0 * p / t, 100.0 * m / t
        return None if pdi + mdi <= 0 else 100.0 * abs(pdi - mdi) / (pdi + mdi)

    dx[period] = _dx(s_tr, s_p, s_m)
    for i in range(period + 1, n):
        s_tr = s_tr - s_tr / period + tr[i]
        s_p = s_p - s_p / period + pdm[i]
        s_m = s_m - s_m / period + mdm[i]
        dx[i] = _dx(s_tr, s_p, s_m)

    seed = dx[period : 2 * period]
    if any(v is None for v in seed):
        return out
    current = sum(v for v in seed if v is not None) / period
    out[2 * period - 1] = current
    for i in range(2 * period, n):
        value = dx[i]
        if value is None:
            return out
        current = (current * (period - 1) + value) / period
        out[i] = current
    return out


class TestAdxAgreesWithAnIndependentWilderImplementation:
    @pytest.mark.parametrize(
        "rows",
        [_trending(80), _trending(80, step=-1.0), _ranging(80), _ranging(80, amplitude=8.0)],
        ids=["uptrend", "downtrend", "range", "wide-range"],
    )
    def test_values_match(self, rows: list[tuple[float, float, float]]) -> None:
        ours = adx_series(_bars(rows), universe=U, period=ADX_PERIOD).values
        theirs = _wilder_reference(rows, ADX_PERIOD)
        assert len(ours) == len(theirs)
        for index, (mine, ref) in enumerate(zip(ours, theirs, strict=True)):
            if ref is None:
                assert mine is None, f"index {index}: we produced {mine} where Wilder's form has none"
            else:
                assert mine is not None
                assert mine == pytest.approx(ref, rel=1e-9), f"index {index}"

    def test_the_reference_actually_produced_values(self) -> None:
        """⚠ A comparison against an all-``None`` reference passes and proves
        nothing — the prevention-log lesson from a probe that matched nothing."""
        assert sum(1 for v in _wilder_reference(_trending(80), ADX_PERIOD) if v is not None) > 40


class TestAdxShape:
    def test_first_value_is_at_twice_the_period_less_one(self) -> None:
        """Wilder needs ``period`` smoothed bars, then ``period`` DX readings."""
        values = adx_series(_bars(_trending(80)), universe=U, period=ADX_PERIOD).values
        assert all(v is None for v in values[: 2 * ADX_PERIOD - 1])
        assert values[2 * ADX_PERIOD - 1] is not None
        assert WARMUP_BARS == 2 * ADX_PERIOD - 1

    def test_a_series_shorter_than_the_warmup_is_all_none(self) -> None:
        values = adx_series(_bars(_trending(20)), universe=U, period=ADX_PERIOD).values
        assert set(values) == {None}

    def test_values_stay_inside_zero_and_one_hundred(self) -> None:
        for rows in (_trending(120), _ranging(120, amplitude=12.0)):
            for value in adx_series(_bars(rows), universe=U, period=ADX_PERIOD).values:
                if value is not None:
                    assert 0.0 <= value <= 100.0

    def test_a_clean_trend_reads_higher_than_a_range(self) -> None:
        """⚠ The DIRECTION of the index, checked on the two cases it exists to
        separate. Not a threshold assertion — the constant belongs to Wilder,
        not to this test."""
        trend = [v for v in adx_series(_bars(_trending(120)), universe=U, period=ADX_PERIOD).values if v is not None]
        rng = [v for v in adx_series(_bars(_ranging(120)), universe=U, period=ADX_PERIOD).values if v is not None]
        assert trend[-1] > rng[-1]
        assert rng[-1] < ADX_TREND_CEILING < trend[-1]

    def test_a_masked_field_makes_everything_after_it_unevaluable(self) -> None:
        """⚠ Wilder smoothing is RECURSIVE: a hole does not spoil one value, it
        shifts every value after it. Matching ``atr_series``'s fail-closed
        horizon rather than resuming past the gap."""
        rows = _trending(80)
        series = _bars(rows)
        holed = BarSeries(
            dates=series.dates,
            rows=tuple(
                {**row, "high": None} if index == 50 else row  # type: ignore[typeddict-item]
                for index, row in enumerate(series.rows)
            ),
        )
        result = adx_series(holed, universe=U, period=ADX_PERIOD)
        assert 50 in result.not_evaluable_indices
        assert 79 in result.not_evaluable_indices
        assert all(result.values[i] is None for i in range(50, 80))

    def test_a_flat_series_has_no_defined_dx(self) -> None:
        """No range and no directional movement: ``+DI == -DI == 0``, which
        Wilder does not define. Reported as absent, never as ``ADX = 0`` — zero
        means "equal directional pressure", which is a measurement."""
        flat = [(100.0, 100.0, 100.0)] * 80
        result = adx_series(_bars(flat), universe=U, period=ADX_PERIOD)
        assert set(result.values) == {None}
        assert result.not_evaluable_indices != ()


# ---------------------------------------------------------------------------
# S-8
# ---------------------------------------------------------------------------


def _regime(n: int, value: Regime | None = Regime.BULL_QUIET) -> RegimeSeries:
    return RegimeSeries(values=(value,) * n)


def _excursion(n: int = 120) -> list[tuple[float, float, float]]:
    """A range with one dip below the lower band that turns up on the next bar.

    ⚠⚠ THE DIP IS SHALLOW ON PURPOSE, AND THE FIRST ATTEMPT AT THIS FIXTURE WAS
    NOT. A dip to 85 against a lower band of 88.65 cleared the band comfortably
    and then FAILED to fire, because the excursion itself is directional
    movement: ADX read **20.14** at the turn bar, just over Wilder's ceiling of
    20. That is the rule working — a violent drop is not a range excursion, it
    is the start of a trend, and S-8 declining it is the ADX gate earning its
    place. The fixture was made milder rather than the ceiling being moved.
    """
    rows = _ranging(n)
    rows[n - 12] = (88.6, 87.6, 88.0)
    rows[n - 11] = (88.8, 88.0, 88.4)
    return rows


class TestS8FiresOnTheRuleAsWritten:
    @staticmethod
    def _verdicts(rows: list[tuple[float, float, float]], regime: RegimeSeries | None = None) -> list[str]:
        series = _bars(rows)
        signals = s8_signals(
            series,
            universe=U,
            masked_reason="quarantined_bar",
            regime=regime if regime is not None else _regime(len(rows)),
        )
        return [s.verdict for s in signals]

    def test_it_fires_on_a_turn_up_from_below_the_lower_band(self) -> None:
        rows = _excursion()
        series = _bars(rows)
        bands = bollinger_series(series, universe=U, period=20, num_std=2.0)
        turn = len(rows) - 11
        lower = bands.components["lower"][turn]
        assert lower is not None and rows[turn][2] < lower, "fixture must actually sit below the band"
        assert rows[turn][2] > rows[turn - 1][2], "fixture must actually turn up"
        assert self._verdicts(rows)[turn] == "fired"

    def test_it_does_not_fire_while_still_falling(self) -> None:
        """⚠ THE LEG THAT SEPARATES THIS FROM A FALLING KNIFE. The dip bar is
        below the band and is DOWN on the day; only the bar that turns up
        fires."""
        rows = _excursion()
        dip = len(rows) - 12
        assert rows[dip][2] < rows[dip - 1][2]
        assert self._verdicts(rows)[dip] == "not_fired"

    def test_a_refused_regime_blocks_it(self) -> None:
        rows = _excursion()
        assert Regime.BULL_VOLATILE not in PERMITTED_REGIMES
        verdicts = self._verdicts(rows, _regime(len(rows), Regime.BULL_VOLATILE))
        assert "fired" not in verdicts

    def test_an_unknown_regime_is_not_evaluable_not_not_fired(self) -> None:
        """#2437's contract, inherited by every strategy in the set."""
        rows = _excursion()
        turn = len(rows) - 11
        regime = RegimeSeries(
            values=tuple(None if i == turn else Regime.BULL_QUIET for i in range(len(rows))),
            not_evaluable_indices=(turn,),
        )
        signals = s8_signals(_bars(rows), universe=U, masked_reason="quarantined_bar", regime=regime)
        assert (signals[turn].verdict, signals[turn].reason) == ("not_evaluable", "missing_market_context")

    def test_a_trending_series_never_fires(self) -> None:
        """⚠ The ADX gate, exercised through the rule rather than asserted on
        the indicator. A clean trend reads well above Wilder's 20."""
        assert "fired" not in self._verdicts(_trending(120))

    def test_a_masked_prior_close_is_a_data_reason_not_warmup(self) -> None:
        """⚠⚠ ``close(t) > close(t-1)`` reaches one bar BACK, so a masked
        ``close(t-1)`` makes bar ``t`` unjudgeable even though every input AT
        ``t`` is fine. Reading it inside the body would have stored `not_fired`
        — #2437's defect, one input over."""
        rows = _excursion()
        series = _bars(rows)
        hole = 60
        holed = BarSeries(
            dates=series.dates,
            rows=tuple(
                {**row, "close": None} if index == hole else row  # type: ignore[typeddict-item]
                for index, row in enumerate(series.rows)
            ),
        )
        signals = s8_signals(holed, universe=U, masked_reason="quarantined_bar", regime=_regime(len(rows)))
        assert (signals[hole + 1].verdict, signals[hole + 1].reason) == ("not_evaluable", "quarantined_bar")


class TestS8Bracket:
    def test_the_target_is_the_signal_bars_middle_band(self) -> None:
        rows = _excursion()
        series = _bars(rows)
        turn = len(rows) - 11
        middle = bollinger_series(series, universe=U, period=20, num_std=2.0).components["middle"][turn]
        assert middle is not None
        target, stop, max_hold = s8_exit_bracket(series, signal_index=turn, entry_price=Decimal("86"), universe=U)
        assert target == pytest.approx(Decimal(str(middle)))
        assert stop < Decimal("86")
        assert max_hold == MAX_HOLD_BARS

    def test_the_target_does_not_track_the_band_after_the_signal(self) -> None:
        """§3.5: levels are fixed at signal time and never move. A target that
        tracked the band would depend on bars after the entry."""
        rows = _excursion()
        series = _bars(rows)
        turn = len(rows) - 11
        first, _, _ = s8_exit_bracket(series, signal_index=turn, entry_price=Decimal("86"), universe=U)
        longer = _bars(rows + _ranging(10, amplitude=30.0))
        again, _, _ = s8_exit_bracket(longer, signal_index=turn, entry_price=Decimal("86"), universe=U)
        assert first == again

    def test_an_inverted_bracket_is_refused_not_raised(self) -> None:
        """⚠ Reachable: the target is anchored to the signal bar's band and the
        stop to the FILL, so a gap up through the band inverts them. The
        manifest adapter must refuse rather than store a backwards bracket."""
        rows = _excursion()
        turn = len(rows) - 11
        entry = STRATEGY_MANIFEST[S8_STRATEGY_ID]
        assert entry.exit_levels is not None
        result = entry.exit_levels(_bars(rows), signal_index=turn, entry_price=Decimal("1000"), universe=U)
        assert result == "unorderable_exit_levels"

    def test_an_unevaluable_signal_bar_is_refused_not_raised(self) -> None:
        entry = STRATEGY_MANIFEST[S8_STRATEGY_ID]
        assert entry.exit_levels is not None
        result = entry.exit_levels(_bars(_ranging(120)), signal_index=3, entry_price=Decimal("100"), universe=U)
        assert result == "unorderable_exit_levels"


class TestS8Identity:
    def test_the_identity_names_this_strategy(self) -> None:
        identity = s8_identity(universe=U, cost_model_id="cost-v1")
        assert identity.strategy_id == S8_STRATEGY_ID
        assert identity.params == S8_PARAMS

    def test_an_empty_cost_model_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cost_model_id must be a non-empty declaration"):
            s8_identity(universe=U, cost_model_id="  ")

    def test_the_regime_rule_version_is_inside_the_identity(self) -> None:
        """⚠ Criterion 11: the same bars under a different regime boundary
        produce different signals, so the boundary is part of what this
        strategy IS."""
        assert "regime_rule_version" in S8_PARAMS
        assert "adx_trend_ceiling" in S8_PARAMS
