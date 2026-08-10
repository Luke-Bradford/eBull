"""S-4 volatility compression breakout — the catalogue's third strategy (#2240).

Spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §4
(S-4), §3.5, §4.0, §5 criteria 4/8/11. Registry: phase 3a.

⚠ WHAT THE REFERENCE DERIVES INDEPENDENTLY, AND WHAT IT DOES NOT.
``_reference_verdicts`` re-derives the two legs by DIFFERENT ALGORITHMS from the
module's: the compression rank comes from ``sorted(window).index(value)`` (the
position of the first occurrence in a sorted window IS the count strictly below)
against the module's count-of-comparisons, and the prior high from ``max()`` over
an explicit slice. The refusal bookkeeping — which index is a data refusal, which
is warm-up, and their precedence — is written out here independently too.

⚠ The ATR itself is NOT independently derived, and saying so is the point. The
Wilder recursion has no naive alternative that is bit-exact, and an approximate
one would make every boundary fixture below unreliable. ``atr_series`` is pinned
by ``tests/test_indicator_series.py``; what is checked HERE is the composition on
top of it. The reference calls it with a LITERAL period rather than
``ATR_PERIOD``, so a changed constant still fails.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import app.services.strategies.s4_volatility_compression_breakout as s4_module
import app.services.strategy_exit_levels_batch as batch_module
import app.services.strategy_manifest as manifest_module
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries, IndicatorSeries, atr_series
from app.services.outcome_resolver import ExitLevels
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_PERIOD,
    BREAKOUT_LOOKBACK,
    COMPRESSION_QUANTILE,
    COMPRESSION_WINDOW,
    S4_PARAMS,
    S4_STRATEGY_ID,
    WARMUP_BARS,
    compression_rank_series,
    prior_high_close_series,
    s4_exit_bracket,
    s4_identity,
    s4_signals,
)
from app.services.strategy_exit_levels_batch import s4_exit_levels_batch
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_registry import StrategySignal
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"

#: ⚠⚠ §4's NUMBERS, TRANSCRIBED BY HAND. Nothing below imports the module's
#: constants for an expected value — the reference must not move when the code
#: does, or the comparison is a tautology. Measured on this branch:
#: `scripts/probe_2240_s4_volatility_breakout.py` reported `*** NOT CAUGHT ***`
#: for BOTH "window 100 -> 50" and "lookback 20 -> 10" while the reference
#: imported them. Same defect S-3 hit the same day (prevention log) — two of the
#: three strategies, which makes it the default mistake rather than a slip.
#:
#: The rule, verbatim: *"Setup: atr_14(t) sits in the bottom quartile of its own
#: trailing 100-bar distribution … Signal: close(t) > the highest close of bars
#: t-20 .. t-1 … Exit: stop at entry - 2 x atr_14(t), profit target at
#: entry + 3 x atr_14(t), hard max-hold 40 bars."*
#:
#: ⚠ Scope: this binds numbers an ASSERTION depends on. Using the module's
#: constant as scaffolding — sizing a window, indexing a warm-up boundary — is
#: fine, and `TestSpecConstants` is what keeps the two in step.
SPEC_ATR_PERIOD = 14
SPEC_COMPRESSION_WINDOW = 100
SPEC_COMPRESSION_QUANTILE = 0.25
SPEC_BREAKOUT_LOOKBACK = 20
SPEC_ATR_STOP_MULTIPLE = 2.0
SPEC_ATR_TARGET_MULTIPLE = 3.0
SPEC_MAX_HOLD_BARS = 40


def _bars(closes: Sequence[float | None], half_ranges: Sequence[float] | None = None) -> BarSeries:
    """One bar per close, ``high = close + h`` and ``low = close - h``.

    ``half_ranges`` is what lets a fixture drive the ATR directly — S-1 and S-3
    read closes only, so their helper could pin the range at 1.0. S-4's setup leg
    is a statement about bar RANGES, so a fixture that cannot vary them cannot
    exercise it. ``None`` is a MASKED bar, as ``load_masked_series`` produces:
    every field present and empty, not absent.
    """
    spans = [1.0] * len(closes) if half_ranges is None else list(half_ranges)
    assert len(spans) == len(closes), "half_ranges must align with closes"
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(c)),
            "high": None if c is None else Decimal(str(c + h)),
            "low": None if c is None else Decimal(str(c - h)),
            "close": None if c is None else Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c, h in zip(closes, spans, strict=True)
    ]
    start = date(2020, 1, 1)
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _reference_verdicts(
    closes: Sequence[float | None],
    half_ranges: Sequence[float] | None = None,
) -> list[tuple[str, str | None]]:
    """(verdict, reason) per bar, re-derived independently of the module.

    Mirrors 3a's runner precedence exactly: the last bar has no ``t+1``; then a
    data refusal on ANY declared input; then warm-up; and only then is the
    condition asked.
    """
    # ⚠ The SPEC_* literals, never the module's constants — see their comment.
    period = SPEC_ATR_PERIOD
    compression_window = SPEC_COMPRESSION_WINDOW
    breakout_lookback = SPEC_BREAKOUT_LOOKBACK
    quantile = SPEC_COMPRESSION_QUANTILE

    series = _bars(closes, half_ranges)
    atr = atr_series(series, universe=UNIVERSE, period=period)
    atr_bad = set(atr.not_evaluable_indices)
    n = len(closes)

    out: list[tuple[str, str | None]] = []
    for i in range(n):
        if i == n - 1:
            out.append(("not_evaluable", "no_fill_bar"))
            continue

        # --- data refusals, over every declared input -----------------------
        compression_window_holed = i + 1 >= compression_window and any(
            j in atr_bad for j in range(i - compression_window + 1, i + 1)
        )
        breakout_window_holed = i >= breakout_lookback and any(
            closes[j] is None for j in range(i - breakout_lookback, i)
        )
        if closes[i] is None or i in atr_bad or compression_window_holed or breakout_window_holed:
            out.append(("not_evaluable", REASON))
            continue

        # --- warm-up ---------------------------------------------------------
        window_ready = i + 1 >= compression_window and all(
            atr.values[j] is not None for j in range(i - compression_window + 1, i + 1)
        )
        if not window_ready or i < breakout_lookback:
            out.append(("not_evaluable", "insufficient_warmup"))
            continue

        # --- the rule --------------------------------------------------------
        window = [atr.values[j] for j in range(i - compression_window + 1, i + 1)]
        current = atr.values[i]
        assert current is not None
        # ⚠ A DIFFERENT DERIVATION of the same number: in a sorted window the
        # index of the first occurrence of `current` IS the count strictly below.
        rank = sorted(w for w in window if w is not None).index(current) / len(window)
        prior_high = max(w for w in closes[i - breakout_lookback : i] if w is not None)
        close = closes[i]
        assert close is not None
        fired = rank < quantile and close > prior_high
        out.append(("fired" if fired else "not_fired", None))
    return out


def _run(closes: Sequence[float | None], half_ranges: Sequence[float] | None = None) -> list[StrategySignal]:
    return s4_signals(_bars(closes, half_ranges), universe=UNIVERSE, masked_reason=REASON)


N = 300

#: Rising by a constant step with a constant bar range, so EVERY true range is
#: identical and the ATR is exactly constant. That makes it the degenerate
#: all-ties window the module docstring describes: rank is 0.0 on every bar, so
#: the setup leg holds everywhere and only the breakout leg decides.
RISING: list[float | None] = [100.0 + i for i in range(N)]

#: Constant price. Also a constant ATR (the range is fixed), so the setup leg
#: holds everywhere here too — and NOTHING fires, because a flat series cannot
#: put a close strictly above its own prior 20. This pair is the module
#: docstring's claim that the conjunction defuses the degenerate case.
FLAT: list[float | None] = [100.0] * N


def _ramps(*legs: tuple[int, float, float]) -> list[float]:
    """Piecewise-linear closes: ``(bars, start, step)`` per leg."""
    out: list[float] = []
    for bars, start, step in legs:
        out.extend(start + step * k for k in range(bars))
    return out


def _spans(*legs: tuple[int, float]) -> list[float]:
    """Piecewise-constant half-ranges: ``(bars, half_range)`` per leg."""
    out: list[float] = []
    for bars, half_range in legs:
        out.extend([half_range] * bars)
    return out


#: ⚠ THE FIXTURE THAT HAS TO COVER ALL FOUR CONJUNCT COMBINATIONS, asserted
#: below rather than hoped for. Volatile and quiet stretches drive the ATR up and
#: down so the compression rank crosses 0.25 in both directions, while the closes
#: rise and fall so the breakout leg does too. A fixture missing the
#: (compression false, breakout true) quadrant passes a rule with the setup leg
#: deleted, which is S-1's lesson applied to S-4's pair.
CYCLE_CLOSES: list[float | None] = [
    *_ramps((60, 100.0, 3.0), (60, 280.0, -3.0), (60, 100.0, 0.2), (60, 112.0, -0.2), (60, 100.0, 1.0))
]
CYCLE_SPANS: list[float] = _spans((60, 8.0), (60, 8.0), (60, 0.4), (60, 0.4), (60, 2.0))


#: Monotonically widening bar ranges against a FLAT close, so ``TR = 2h`` exactly
#: and the ATR is strictly increasing — every warm bar is then the maximum of its
#: own trailing window.
RISING_ATR_SPANS: list[float] = [1.0 + 0.1 * i for i in range(N)]

#: The bar the constructed quartile boundary lands on.
BOUNDARY_INDEX = 150


def _quartile_boundary() -> tuple[list[float | None], list[float]]:
    """A series whose bar 150 sits EXACTLY on the quartile — by construction.

    ⚠ CONSTRUCTED, NOT SEARCHED FOR, and the difference is robustness. Searching
    a wandering fixture for ``rank == 0.25`` finds knife-edge bars that a
    one-ULP change relocates (measured: the widest search over piecewise-constant
    volatility fixtures yielded ONE such bar with the breakout leg also true).
    This construction yields a whole INTERVAL of valid spike sizes instead.

    It works because a flat close makes the true range exactly ``2h``: with the
    close unchanged bar to bar, ``TR = max(2h, |Δ|+h) = 2h``. So a long stretch
    at constant ``h`` seeds Wilder on ``2h`` and holds it there EXACTLY —

        bars 0..124   h=5.0   -> TR 10 -> ATR exactly 10.0 from bar 14
        bars 125..149 h=0.5   -> TR 1  -> ATR decays monotonically to ~2.41
        bar  150      h=52.0  -> TR 104, and the close jumps 100 -> 105

    Bar 150's window is bars 51..150: seventy-four at exactly 10.0, twenty-five
    decaying, and bar 150 itself. Any spike putting ``atr(150)`` strictly between
    the highest quiet value (``131/14 = 9.3571…``) and the loud plateau (10.0)
    gives a below-count of exactly 25, hence ``rank = 25/100 = 0.25`` — exact in
    binary float. ``h = 52.0`` lands it at ~9.6677, roughly mid-interval, so the
    fixture has margin on both sides rather than sitting on an edge.

    ⚠ ``|Δ| = 5`` at the spike is well inside ``h = 52``, so the jump does not
    disturb ``TR = 2h`` — which is what keeps the arithmetic above exact.
    """
    closes: list[float | None] = []
    spans: list[float] = []
    for i in range(N):
        if i <= 124:
            closes.append(100.0)
            spans.append(5.0)
        elif i <= 149:
            closes.append(100.0)
            spans.append(0.5)
        elif i == BOUNDARY_INDEX:
            closes.append(105.0)
            spans.append(52.0)
        else:
            closes.append(105.0)
            spans.append(0.5)
    return closes, spans


class TestTheRule:
    """§4: ``atr_14(t)`` in the bottom quartile of its trailing 100, AND
    ``close(t)`` above the highest close of the prior 20."""

    @pytest.mark.parametrize(
        ("closes", "spans"),
        [(RISING, None), (FLAT, None), (CYCLE_CLOSES, CYCLE_SPANS)],
        ids=["rising", "flat", "cycle"],
    )
    def test_every_bar_matches_the_naive_reference(
        self, closes: Sequence[float | None], spans: Sequence[float] | None
    ) -> None:
        signals = _run(closes, spans)
        expected = _reference_verdicts(closes, spans)
        assert len(signals) == len(closes)
        for i, (verdict, reason) in enumerate(expected):
            assert (signals[i].verdict, signals[i].reason) == (verdict, reason), f"bar {i}"

    def test_the_cycle_fixture_exercises_both_outcomes(self) -> None:
        """Guards the table test from passing vacuously."""
        signals = _run(CYCLE_CLOSES, CYCLE_SPANS)
        assert {"fired", "not_fired"} <= {s.verdict for s in signals}

    def test_the_cycle_fixture_covers_all_four_conjunct_combinations(self) -> None:
        """⚠ WITHOUT THIS THE TABLE TEST CANNOT SEE A DROPPED CONJUNCT.

        Either leg alone agrees with the pair on every bar except those in its
        own off-quadrant, so a fixture missing a quadrant passes a rule with that
        leg deleted."""
        series = _bars(CYCLE_CLOSES, CYCLE_SPANS)
        atr = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD)
        compression = compression_rank_series(atr, universe=UNIVERSE)
        prior_high = prior_high_close_series(series, universe=UNIVERSE)
        closes = series.float_closes

        combinations = set()
        for i in range(WARMUP_BARS, len(CYCLE_CLOSES) - 1):
            rank, high, close = compression.values[i], prior_high.values[i], closes[i]
            assert rank is not None and high is not None and close is not None
            combinations.add((rank < COMPRESSION_QUANTILE, close > high))
        assert combinations == {(True, True), (True, False), (False, True), (False, False)}

    def test_a_constant_atr_ramp_fires_every_warm_bar(self) -> None:
        """⚠ THE DEGENERATE ALL-TIES WINDOW, asserted rather than argued.

        Every true range on ``RISING`` is identical, so all 100 window ATRs are
        equal and the count STRICTLY below today's is 0 — rank 0.0, setup holds.
        A ``<=`` count would make the rank 1.0 here and fire nothing."""
        series = _bars(RISING)
        atr = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD)
        warm = [v for v in atr.values[ATR_PERIOD:] if v is not None]
        assert len(set(warm)) == 1, "fixture drifted: the ATR is meant to be exactly constant"

        signals = _run(RISING)
        assert [s.verdict for s in signals[WARMUP_BARS:-1]] == ["fired"] * (N - WARMUP_BARS - 1)

    def test_a_flat_series_never_fires(self) -> None:
        """The other half of the same claim: the setup leg holds on every bar of
        a flat series too (constant ATR), and the CONJUNCTION is what stops it —
        a flat close cannot sit strictly above its own prior 20."""
        series = _bars(FLAT)
        atr = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD)
        compression = compression_rank_series(atr, universe=UNIVERSE)
        assert compression.values[WARMUP_BARS] == 0.0, "the setup leg is meant to HOLD here"

        signals = _run(FLAT)
        assert not any(s.verdict == "fired" for s in signals)


class TestCompressionRankRule:
    """The rank rule itself, on synthetic ATRs — exact, and free of Wilder drift.

    ⚠ "Bottom quartile" has no published formulation, so the module fixes it by
    construction (see its docstring). These are the tests that pin the
    construction: the k=25/k=26 boundary, and favourable tie handling.
    """

    @staticmethod
    def _atr(values: Sequence[float]) -> IndicatorSeries:
        return IndicatorSeries(tuple(values), UNIVERSE)

    def test_the_kth_smallest_of_a_hundred_is_in_the_quartile_up_to_k_25(self) -> None:
        """With 100 distinct values the k-th smallest has ``k-1`` strictly below,
        so ``rank = (k-1)/100`` and ``rank < 0.25`` holds for exactly k <= 25 —
        the bottom 25 of 100, which is what "bottom quartile" must mean."""
        ordered = [float(v) for v in range(1, COMPRESSION_WINDOW + 1)]
        for k, target in ((25, 25.0), (26, 26.0)):
            # Put the k-th smallest LAST so it is the ranked bar, keeping the
            # window's contents identical between the two cases.
            window = [v for v in ordered if v != target] + [target]
            rank = compression_rank_series(self._atr(window), universe=UNIVERSE).values[-1]
            assert rank == (k - 1) / COMPRESSION_WINDOW
        rank_25 = compression_rank_series(
            self._atr([v for v in ordered if v != 25.0] + [25.0]), universe=UNIVERSE
        ).values[-1]
        rank_26 = compression_rank_series(
            self._atr([v for v in ordered if v != 26.0] + [26.0]), universe=UNIVERSE
        ).values[-1]
        assert rank_25 is not None and rank_26 is not None
        assert rank_25 < COMPRESSION_QUANTILE, "the 25th smallest of 100 is in the bottom quartile"
        assert not rank_26 < COMPRESSION_QUANTILE, "the 26th smallest of 100 is not"

    def test_the_boundary_rank_is_exactly_representable(self) -> None:
        """``rank == 0.25`` iff exactly 25 of 100 sit below — an INTEGER
        condition, and ``25/100`` is exact in binary float. So the boundary
        fixtures below compare exactly rather than approximately."""
        window = [float(v) for v in range(1, COMPRESSION_WINDOW + 1)]
        window = [v for v in window if v != 26.0] + [26.0]
        assert compression_rank_series(self._atr(window), universe=UNIVERSE).values[-1] == COMPRESSION_QUANTILE

    def test_tied_values_all_receive_the_same_rank(self) -> None:
        """⚠ FORCED, NOT CHOSEN. Counting ``<=`` would make two bars with
        IDENTICAL ATRs in the same window rank differently according to their
        position, i.e. read arbitrary order as signal."""
        window = [1.0] * 40 + [float(v) for v in range(2, 62)]
        ranks = []
        for tied_position in range(3):
            rotated = [v for v in window if v != 1.0]
            rotated = [1.0] * 39 + rotated[: 21 + tied_position] + [1.0] + rotated[21 + tied_position :]
            assert len(rotated) == COMPRESSION_WINDOW
            ranks.append(compression_rank_series(self._atr(rotated), universe=UNIVERSE).values[-1])
        assert len(set(ranks)) == 1

    def test_the_window_includes_the_ranked_bar(self) -> None:
        """§4: *"computed on bars <= t"*. With ``t`` excluded the divisor and the
        contents both change; here the ranked value is the window MAXIMUM, so
        including it gives ``99/100`` and excluding it would give ``99/99``."""
        window = [float(v) for v in range(1, COMPRESSION_WINDOW)] + [1_000.0]
        assert compression_rank_series(self._atr(window), universe=UNIVERSE).values[-1] == 0.99


class TestStrictComparisons:
    """§4 writes both comparisons strictly. ⚠ ONE EXACT-EQUALITY FIXTURE PER
    OPERATOR — a fixture that is degenerate on both pins NEITHER, because
    relaxing one leaves the other conjunct false and the probe reports NOT
    CAUGHT. That is the S-3 lesson, already in the prevention log."""

    @staticmethod
    def _quiet_after_volatile() -> tuple[list[float | None], list[float]]:
        """A volatile first half then a long quiet tail, so the recent ATRs are
        the smallest of their trailing 100 and the setup leg holds in the tail."""
        closes: list[float | None] = [*_ramps((80, 100.0, 4.0), (80, 420.0, -4.0), (140, 100.0, 0.05))]
        spans = _spans((160, 12.0), (140, 0.2))
        return closes, spans

    def test_a_close_exactly_on_the_prior_high_does_not_fire(self) -> None:
        """Pins the breakout leg's ``>``. The setup leg is strictly true here, so
        a ``>=`` on the breakout is the only thing that could fire this bar."""
        closes, spans = self._quiet_after_volatile()
        i = 260
        prior = [c for c in closes[i - BREAKOUT_LOOKBACK : i] if c is not None]
        closes[i] = max(prior)

        series = _bars(closes, spans)
        atr = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD)
        compression = compression_rank_series(atr, universe=UNIVERSE)
        prior_high = prior_high_close_series(series, universe=UNIVERSE)
        rank = compression.values[i]
        # ⚠ Asserted, not assumed: if the fixture drifts off the boundary or out
        # of the compressed regime the test would pass while testing nothing.
        assert rank is not None and rank < COMPRESSION_QUANTILE, "setup leg must be strictly true"
        assert closes[i] == prior_high.values[i], "the breakout comparison must be an exact equality"

        assert _run(closes, spans)[i].verdict == "not_fired"

    def test_a_compression_rank_exactly_on_the_quartile_does_not_fire(self) -> None:
        """Pins the setup leg's ``<``, on a CONSTRUCTED boundary rather than a
        found one — see ``_quartile_boundary``. The breakout leg is strictly true
        here, so a ``<=`` on the setup is the only thing that could fire it."""
        closes, spans = _quartile_boundary()
        series = _bars(closes, spans)
        atr = atr_series(series, universe=UNIVERSE, period=ATR_PERIOD)
        compression = compression_rank_series(atr, universe=UNIVERSE)
        prior_high = prior_high_close_series(series, universe=UNIVERSE)
        i = BOUNDARY_INDEX

        # ⚠ Asserted, not assumed. Every one of these can drift silently, and
        # each would leave the test passing while testing nothing.
        window = [atr.values[j] for j in range(i - COMPRESSION_WINDOW + 1, i + 1)]
        assert {v for v in window[:74]} == {10.0}, "the loud plateau must be exactly flat"
        assert compression.values[i] == COMPRESSION_QUANTILE, "the setup comparison must be an exact equality"
        close, high = closes[i], prior_high.values[i]
        assert close is not None and high is not None and close > high, "breakout leg must be strictly true"

        assert _run(closes, spans)[i].verdict == "not_fired"


class TestWindowBoundaries:
    """§4 gives the two legs DIFFERENT boundaries on purpose."""

    def test_the_breakout_window_excludes_the_signal_bar(self) -> None:
        """Including ``t`` makes ``close(t) > max(...including close(t))``
        satisfiable only by a tie, i.e. never under a strict ``>`` — the rule
        would silently never fire rather than fail. Any fire at all falsifies
        the inclusive reading."""
        series = _bars(CYCLE_CLOSES, CYCLE_SPANS)
        prior_high = prior_high_close_series(series, universe=UNIVERSE)
        closes = series.float_closes
        i = WARMUP_BARS + 5
        assert prior_high.values[i] == max(c for c in closes[i - BREAKOUT_LOOKBACK : i] if c is not None)
        assert any(s.verdict == "fired" for s in _run(CYCLE_CLOSES, CYCLE_SPANS))

    def test_the_compression_rank_can_never_reach_one(self) -> None:
        """§4: *"computed on bars <= t"* — the window CONTAINS the ranked bar, so
        at most 99 of its 100 values can be strictly below and the rank tops out
        at 0.99. An exclusive window (``t-100 .. t-1``) has no such ceiling: its
        100 values are all other bars, so a new ATR high scores a full 1.0.

        Pinned on a strictly-rising ATR, where EVERY warm bar is its own window's
        maximum — so the ceiling is exercised on every bar rather than sampled.
        ``RISING_ATR_SPANS`` widens the bar range monotonically, and a flat close
        makes ``TR = 2h`` exactly, so the ATR is strictly increasing by
        construction (asserted below)."""
        atr = atr_series(_bars(FLAT, RISING_ATR_SPANS), universe=UNIVERSE, period=ATR_PERIOD)
        warm = [v for v in atr.values[ATR_PERIOD:] if v is not None]
        assert all(b > a for a, b in zip(warm, warm[1:], strict=False)), "fixture drifted: ATR must be rising"

        ranks = [v for v in compression_rank_series(atr, universe=UNIVERSE).values if v is not None]
        assert ranks
        assert set(ranks) == {0.99}, "every bar is its own window's maximum, so every rank is 99/100"


class TestWarmUp:
    """One warm-up for the whole strategy, derived from the rule."""

    def test_the_warm_up_is_the_atr_seed_plus_the_compression_window(self) -> None:
        assert WARMUP_BARS == ATR_PERIOD + COMPRESSION_WINDOW - 1 == 113

    def test_the_first_evaluable_bar_is_the_warm_up_boundary(self) -> None:
        signals = _run(RISING)
        assert (signals[WARMUP_BARS - 1].verdict, signals[WARMUP_BARS - 1].reason) == (
            "not_evaluable",
            "insufficient_warmup",
        )
        assert signals[WARMUP_BARS].verdict != "not_evaluable"

    def test_a_bar_with_a_warm_breakout_leg_but_a_cold_atr_is_still_refused(self) -> None:
        """⚠ THE NARROWING. Bar 40 has 20 prior closes, so the breakout leg IS
        computable there; it is refused anyway, because per-leg evaluability
        would make the same bar live for one leg and warming for the other."""
        cold = BREAKOUT_LOOKBACK + 20
        assert cold < WARMUP_BARS
        signals = _run(RISING)
        assert (signals[cold].verdict, signals[cold].reason) == ("not_evaluable", "insufficient_warmup")

    def test_a_series_shorter_than_the_warm_up_is_never_evaluable(self) -> None:
        signals = _run(RISING[:100])
        assert {s.verdict for s in signals} == {"not_evaluable"}
        assert {s.reason for s in signals} == {"insufficient_warmup", "no_fill_bar"}


class TestMissingBars:
    """Criterion 8: a data gap and a real absence must stay distinguishable."""

    def test_a_masked_bar_records_the_callers_reason_not_warm_up(self) -> None:
        closes = list(RISING)
        closes[200] = None
        signals = _run(closes)
        assert (signals[200].verdict, signals[200].reason) == ("not_evaluable", REASON)

    def test_a_masked_bar_refuses_the_whole_tail_not_a_window(self) -> None:
        """⚠ S-4's blast radius, inherited from Wilder smoothing exactly as S-3's
        is. ``atr_series`` carries state across every bar, so there is no window
        for the hole to clear — S-1 would have recovered 200 bars later."""
        closes = list(RISING)
        closes[150] = None
        signals = _run(closes)
        assert all(s.reason == REASON for s in signals[150:-1])
        assert signals[149].reason != REASON

    def test_a_missing_high_is_refused_rather_than_judged(self) -> None:
        """§4 requires COMPLETE OHLC. A bar with a good close and no high has a
        computable breakout leg and an uncomputable ATR."""
        series = _bars(RISING)
        rows = list(series.rows)
        rows[200] = {**rows[200], "high": None}  # type: ignore[typeddict-item]
        holed = BarSeries(dates=series.dates, rows=tuple(rows))
        signals = s4_signals(holed, universe=UNIVERSE, masked_reason="series_break")
        assert (signals[200].verdict, signals[200].reason) == ("not_evaluable", "series_break")

    def test_a_reason_outside_the_closed_vocabulary_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown reason code"):
            s4_signals(_bars(RISING), universe=UNIVERSE, masked_reason="looked_quiet")  # type: ignore[arg-type]


class TestNoExitLeg:
    """⚠ S-4's structural difference from S-1 and S-3, pinned so a later edit
    cannot add an exit leg without this failing. All three of §4's exit
    conditions are measured from the ENTRY, so none is a per-bar verdict."""

    def test_every_signal_is_an_entry(self) -> None:
        signals = _run(CYCLE_CLOSES, CYCLE_SPANS)
        assert {s.kind for s in signals} == {"entry"}
        assert len(signals) == len(CYCLE_CLOSES)

    def test_the_exit_parameters_are_carried_in_the_identity(self) -> None:
        assert dict(S4_PARAMS)["atr_stop_multiple"] == SPEC_ATR_STOP_MULTIPLE
        assert dict(S4_PARAMS)["atr_target_multiple"] == SPEC_ATR_TARGET_MULTIPLE
        assert dict(S4_PARAMS)["max_hold_bars"] == SPEC_MAX_HOLD_BARS


class TestExitLevels:
    """The bracket is fixed from signal-bar ATR around the next-open fill."""

    def test_constant_two_point_true_range_builds_the_declared_bracket(self) -> None:
        series = _bars(RISING)
        target, stop, max_hold = s4_exit_bracket(
            series,
            signal_index=150,
            entry_price=Decimal("300"),
            universe=UNIVERSE,
        )
        assert stop == Decimal("296.0")
        assert target == Decimal("306.0")
        assert max_hold == SPEC_MAX_HOLD_BARS

    def test_future_bars_cannot_move_a_fixed_bracket(self) -> None:
        full = _bars(RISING)
        truncated = BarSeries(dates=full.dates[:152], rows=full.rows[:152])
        kwargs = {"signal_index": 150, "entry_price": Decimal("300"), "universe": UNIVERSE}
        assert s4_exit_bracket(full, **kwargs) == s4_exit_bracket(truncated, **kwargs)

    def test_an_unorderable_nonpositive_stop_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not broker-orderable"):
            s4_exit_bracket(
                _bars(RISING),
                signal_index=150,
                entry_price=Decimal("3"),
                universe=UNIVERSE,
            )

    def test_manifest_adapter_turns_the_scalar_refusal_into_a_countable_reason(self) -> None:
        factory = STRATEGY_MANIFEST[S4_STRATEGY_ID].exit_levels
        assert factory is not None

        actual = factory(
            _bars(RISING),
            signal_index=150,
            entry_price=Decimal("3"),
            universe=UNIVERSE,
        )

        assert actual == "unorderable_exit_levels"

    def test_manifest_adapter_keeps_the_hashed_scalar_factory_as_exact_oracle(self) -> None:
        factory = STRATEGY_MANIFEST[S4_STRATEGY_ID].exit_levels
        assert factory is not None
        series = _bars(RISING)

        actual = factory(
            series,
            signal_index=150,
            entry_price=Decimal("300"),
            universe=UNIVERSE,
        )
        target, stop, max_hold = s4_exit_bracket(
            series,
            signal_index=150,
            entry_price=Decimal("300"),
            universe=UNIVERSE,
        )

        assert actual == ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)

    def test_manifest_adapter_does_not_swallow_an_unrelated_scalar_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        factory = STRATEGY_MANIFEST[S4_STRATEGY_ID].exit_levels
        assert factory is not None

        def fail_scalar(*args: object, **kwargs: object) -> tuple[Decimal, Decimal, int]:
            raise ValueError("unrelated scalar defect")

        monkeypatch.setattr(manifest_module, "s4_exit_bracket", fail_scalar)

        with pytest.raises(ValueError, match="unrelated scalar defect"):
            factory(
                _bars(RISING),
                signal_index=150,
                entry_price=Decimal("300"),
                universe=UNIVERSE,
            )

    def test_batch_matches_scalar_brackets_and_computes_atr_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        series = _bars(RISING)
        requests = (
            (150, Decimal("300")),
            (175, Decimal("325")),
            (200, Decimal("350")),
        )
        expected = tuple(
            s4_exit_bracket(
                series,
                signal_index=signal_index,
                entry_price=entry_price,
                universe=UNIVERSE,
            )
            for signal_index, entry_price in requests
        )
        calls = 0
        original = batch_module.atr_series

        def counted_atr(*args: object, **kwargs: object) -> IndicatorSeries:
            nonlocal calls
            calls += 1
            return original(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(batch_module, "atr_series", counted_atr)

        actual = s4_exit_levels_batch(series, requests=requests, universe=UNIVERSE)
        assert len(actual) == len(expected)
        assert (
            tuple(
                (item.take_profit, item.stop_loss, item.max_hold_bars)
                for item in actual
                if isinstance(item, ExitLevels)
            )
            == expected
        )
        assert calls == 1

    def test_batch_counts_an_unorderable_stop_without_hiding_other_levels(self) -> None:
        series = _bars(RISING)

        actual = s4_exit_levels_batch(
            series,
            requests=((150, Decimal("3")), (175, Decimal("325"))),
            universe=UNIVERSE,
        )

        assert actual[0] == "unorderable_exit_levels"
        assert isinstance(actual[1], ExitLevels)

    def test_batch_classifies_a_nonfinite_atr_as_an_unorderable_level(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        series = _bars(RISING)
        values: list[float | None] = [None] * len(series)
        values[150] = float("nan")
        monkeypatch.setattr(
            batch_module,
            "atr_series",
            lambda *args, **kwargs: IndicatorSeries(values=tuple(values), universe=UNIVERSE),
        )

        actual = s4_exit_levels_batch(
            series,
            requests=((150, Decimal("300")),),
            universe=UNIVERSE,
        )

        assert actual == ("unorderable_exit_levels",)


class TestLastBar:
    def test_the_final_bar_is_no_fill_bar(self) -> None:
        signals = _run(RISING)
        assert (signals[-1].verdict, signals[-1].reason) == ("not_evaluable", "no_fill_bar")


class TestCausality:
    """Criterion 4: every value at bar ``t`` uses only bars <= ``t``."""

    def test_a_bars_verdict_is_unchanged_when_later_bars_are_removed(self) -> None:
        full = _run(CYCLE_CLOSES, CYCLE_SPANS)
        for index in range(WARMUP_BARS, len(CYCLE_CLOSES) - 1, 7):
            truncated = _run(CYCLE_CLOSES[: index + 2], CYCLE_SPANS[: index + 2])
            assert truncated[index].verdict == full[index].verdict, f"bar {index}"

    def test_the_truncation_sweep_covers_bars_of_both_verdicts(self) -> None:
        """A sweep over a stretch where nothing changes cannot detect a
        look-ahead, so the fixture is asserted to vary across it."""
        full = _run(CYCLE_CLOSES, CYCLE_SPANS)
        swept = [full[i].verdict for i in range(WARMUP_BARS, len(CYCLE_CLOSES) - 1, 7)]
        assert {"fired", "not_fired"} <= set(swept)


class TestIdentity:
    """Criterion 11: identity = code + config + data contract."""

    def test_a_blank_cost_model_id_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            s4_identity(universe=UNIVERSE, cost_model_id="   ")

    def test_the_version_moves_with_the_universe(self) -> None:
        a = s4_identity(universe="survivor_only", cost_model_id=COST_MODEL_ID)
        b = s4_identity(universe="survivorship_free", cost_model_id=COST_MODEL_ID)
        assert a.version != b.version

    def test_the_version_moves_with_the_cost_model(self) -> None:
        a = s4_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        b = s4_identity(universe=UNIVERSE, cost_model_id="static-p75-v1")
        assert a.version != b.version

    def test_the_identity_carries_every_parameter_of_the_rule(self) -> None:
        identity = s4_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        assert identity.strategy_id == S4_STRATEGY_ID
        assert dict(identity.params) == {
            "atr_period": SPEC_ATR_PERIOD,
            "compression_window": SPEC_COMPRESSION_WINDOW,
            "compression_quantile": SPEC_COMPRESSION_QUANTILE,
            "breakout_lookback": SPEC_BREAKOUT_LOOKBACK,
            "atr_stop_multiple": SPEC_ATR_STOP_MULTIPLE,
            "atr_target_multiple": SPEC_ATR_TARGET_MULTIPLE,
            "max_hold_bars": SPEC_MAX_HOLD_BARS,
        }
        assert dict(S4_PARAMS) == dict(identity.params)

    def test_the_source_hash_is_this_strategys_own_source(self) -> None:
        """⚠ Recomputed here from the FILE, not compared against the module's own
        helper: ``source_hash == _source_hash()`` holds just as well when both
        are a constant, which is the one defect this guards."""
        import app.services.strategies.s4_volatility_compression_breakout as s4

        expected = hashlib.sha256(Path(s4.__file__).read_bytes()).hexdigest()[:12]
        assert s4_identity(universe=UNIVERSE, cost_model_id="x").source_hash == expected


class TestLedgerRoundTrip:
    """The signals have to survive 3c's writer, which is where the fill is
    resolved and where a same-bar fill would surface."""

    def test_every_fired_row_fills_at_the_next_bars_open(self) -> None:
        series = _bars(CYCLE_CLOSES, CYCLE_SPANS)
        rows = resolve_fills(
            s4_signals(series, universe=UNIVERSE, masked_reason=REASON),
            series=series,
            identity=s4_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=1,
        )
        fired = [row for row in rows if row.verdict == "fired"]
        assert fired
        by_date = {d: i for i, d in enumerate(series.dates)}
        for row in fired:
            index = by_date[row.signal_bar_date]
            assert row.fill_bar_date == series.dates[index + 1]
            assert row.fill_price == series.rows[index + 1]["open"]

    def test_one_row_per_bar_with_no_key_collision(self) -> None:
        series = _bars(CYCLE_CLOSES, CYCLE_SPANS)
        rows = resolve_fills(
            s4_signals(series, universe=UNIVERSE, masked_reason=REASON),
            series=series,
            identity=s4_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=1,
        )
        assert len(rows) == len(CYCLE_CLOSES)
        keys = {(row.signal_bar_date, row.signal_kind) for row in rows}
        assert len(keys) == len(rows)


class TestSpecConstants:
    """⚠ The bridge between §4's prose and the module's constants.

    Everything else in this file reasons in ``SPEC_*`` literals, so the reference
    cannot move when the code does. That independence has a price: nothing would
    then notice the module quietly using a different number. This class is that
    notice, and it is the ONLY place the module's constants are read as values.

    Same shape as ``tests/test_strategy_s3.py::TestSpecConstants``, which exists
    for the same incident.
    """

    def test_the_modules_constants_are_the_specs_numbers(self) -> None:
        assert s4_module.ATR_PERIOD == SPEC_ATR_PERIOD
        assert s4_module.COMPRESSION_WINDOW == SPEC_COMPRESSION_WINDOW
        assert s4_module.COMPRESSION_QUANTILE == SPEC_COMPRESSION_QUANTILE
        assert s4_module.BREAKOUT_LOOKBACK == SPEC_BREAKOUT_LOOKBACK
        assert s4_module.ATR_STOP_MULTIPLE == SPEC_ATR_STOP_MULTIPLE
        assert s4_module.ATR_TARGET_MULTIPLE == SPEC_ATR_TARGET_MULTIPLE
        assert s4_module.MAX_HOLD_BARS == SPEC_MAX_HOLD_BARS

    def test_the_warm_up_is_derived_from_the_specs_two_windows(self) -> None:
        assert s4_module.WARMUP_BARS == SPEC_ATR_PERIOD + SPEC_COMPRESSION_WINDOW - 1
