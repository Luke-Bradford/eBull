"""Phase 2 ticket 2a — the ten invariants of `indicator_series`.

Pure, no DB. Spec:
`docs/proposals/ta/2026-08-05-historical-indicator-recompute.md` §5.

⚠ Every fixture here is chosen to EXPRESS the defect it guards. Three fixtures
failed to discriminate on #2308 before one worked, and all three were caught by
revert-probes rather than review — a fixture too neutral to express a defect
passes against broken code, which is the expensive direction.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services import technical_analysis as ta
from app.services.indicator_series import (
    RULE_SET_ID,
    RULE_SET_VERSION,
    BarSeries,
    atr_series,
    bollinger_series,
    ema_series,
    macd_series,
    rsi_series,
    sma_series,
    stochastic_series,
)

U = "survivor_only"

# A series with BOTH gains and losses and a changing character — the shape that
# discriminates a full-series seed from a warm-up seed. A monotonic ramp does
# not (both average to the same number), which #2308 established the hard way.
_CLOSES = [
    100,
    99,
    101,
    98,
    97,
    99,
    96,
    95,
    97,
    94,
    93,
    95,
    92,
    91,
    93,
    85,
    87,
    84,
    83,
    86,
    88,
    91,
    89,
    92,
    95,
    94,
    97,
    99,
    98,
    101,
    103,
    102,
    105,
    107,
    106,
    109,
    111,
    110,
    113,
    115,
]


def _bars(closes: list[float] | list[int]) -> BarSeries:
    """OHLC around each close, wide enough that ATR and stochastic have range."""
    rows: list[ta.OHLCVRow] = [
        {
            "open": Decimal(str(c)),
            "high": Decimal(str(c + 1.5)),
            "low": Decimal(str(c - 1.5)),
            "close": Decimal(str(c)),
            "volume": 1_000,
        }
        for c in closes
    ]
    start = date(2024, 1, 1)
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


SERIES = _bars(_CLOSES)
CLOSES_D = [Decimal(str(c)) for c in _CLOSES]
ROWS = list(SERIES.rows)


# ---------------------------------------------------------------------------
# Invariant 1 — causality
# ---------------------------------------------------------------------------


class TestCausality:
    """Element i depends only on inputs 0..i.

    ⚠ This proves no-future-bars relative to the CURRENT batch implementation.
    It does NOT prove either implementation matches the published rule — a
    different claim, carried by the spec's §2 citations, not by this test.
    """

    @pytest.mark.parametrize(
        ("fn", "kwargs", "batch"),
        [
            (sma_series, {"period": 20}, lambda c: ta.sma(c, 20)),
            (ema_series, {"period": 12}, lambda c: ta.ema(c, 12)),
            (rsi_series, {"period": 14}, lambda c: ta.rsi(c, 14)),
        ],
    )
    def test_every_prefix_matches_the_batch_form(self, fn, kwargs, batch) -> None:  # type: ignore[no-untyped-def]
        for k in range(2, len(_CLOSES) + 1):
            streamed = fn(_bars(_CLOSES[:k]), universe=U, **kwargs).values[-1]
            expected = batch(CLOSES_D[:k])
            if expected is None:
                assert streamed is None, f"prefix {k}: batch None, series {streamed}"
            else:
                assert streamed is not None, f"prefix {k}: series None, batch {expected}"
                assert abs(streamed - expected) < 1e-9, f"prefix {k}: {streamed} != {expected}"

    def test_atr_every_prefix_matches_the_batch_form(self) -> None:
        for k in range(2, len(_CLOSES) + 1):
            streamed = atr_series(_bars(_CLOSES[:k]), universe=U).values[-1]
            expected = ta.atr(ROWS[:k], 14)
            if expected is None:
                assert streamed is None, f"prefix {k}"
            else:
                assert streamed is not None and abs(streamed - expected) < 1e-9, f"prefix {k}"


# ---------------------------------------------------------------------------
# Invariant 2 — equivalence with the batch form
# ---------------------------------------------------------------------------


class TestEquivalence:
    """⚠ The unit-test version. The BINDING check is the full-corpus sweep over
    all 7,693 series (acceptance 2) — resting this on a fixture would repeat the
    sampling error the whole phase exists to correct."""

    def test_last_value_matches_for_every_indicator(self) -> None:
        assert sma_series(SERIES, universe=U, period=20).values[-1] == pytest.approx(ta.sma(CLOSES_D, 20))
        assert ema_series(SERIES, universe=U, period=12).values[-1] == pytest.approx(ta.ema(CLOSES_D, 12))
        assert rsi_series(SERIES, universe=U).values[-1] == pytest.approx(ta.rsi(CLOSES_D, 14))
        assert atr_series(SERIES, universe=U).values[-1] == pytest.approx(ta.atr(ROWS, 14))

        macd = macd_series(SERIES, universe=U)
        batch_macd = ta.macd(CLOSES_D)
        assert batch_macd is not None
        for i, comp in enumerate(("line", "signal", "histogram")):
            assert macd.components[comp][-1] == pytest.approx(batch_macd[i])

        bb = bollinger_series(SERIES, universe=U)
        batch_bb = ta.bollinger_bands(CLOSES_D)
        assert batch_bb is not None
        assert bb.components["upper"][-1] == pytest.approx(batch_bb[0])
        assert bb.components["lower"][-1] == pytest.approx(batch_bb[1])

        st = stochastic_series(SERIES, universe=U)
        batch_st = ta.stochastic(ROWS)
        assert batch_st is not None
        assert st.components["k"][-1] == pytest.approx(batch_st[0])
        assert st.components["d"][-1] == pytest.approx(batch_st[1])


# ---------------------------------------------------------------------------
# Invariant 3 — alignment and warm-up
# ---------------------------------------------------------------------------


class TestAlignment:
    def test_length_always_equals_the_input(self) -> None:
        """An offset series is how an off-by-one enters a backtest."""
        n = len(SERIES)
        assert len(rsi_series(SERIES, universe=U)) == n
        assert len(atr_series(SERIES, universe=U)) == n
        assert len(sma_series(SERIES, universe=U, period=20)) == n
        assert len(macd_series(SERIES, universe=U)) == n
        assert len(bollinger_series(SERIES, universe=U)) == n
        assert len(stochastic_series(SERIES, universe=U)) == n

    def test_warm_up_is_none_not_a_value(self) -> None:
        """A seeded-but-unwarmed indicator is look-ahead in miniature."""
        sma20 = sma_series(SERIES, universe=U, period=20).values
        assert all(v is None for v in sma20[:19])
        assert sma20[19] is not None

        rsi = rsi_series(SERIES, universe=U, period=14).values
        assert all(v is None for v in rsi[:14])
        assert rsi[14] is not None


# ---------------------------------------------------------------------------
# Invariant 6 — input ordering, enforced by construction
# ---------------------------------------------------------------------------


class TestInputOrdering:
    """⚠ The look-ahead no causality fixture can catch. Newest-first input
    produces internally consistent numbers for a reversed timeline, so every
    value-equality test still passes while the backtest reads the future."""

    def test_descending_dates_rejected(self) -> None:
        rows = SERIES.rows
        start = date(2024, 1, 1)
        dates = tuple(start + timedelta(days=len(rows) - i) for i in range(len(rows)))
        with pytest.raises(ValueError, match="not ascending"):
            BarSeries(dates=dates, rows=rows)

    def test_duplicate_dates_rejected(self) -> None:
        rows = SERIES.rows[:3]
        with pytest.raises(ValueError, match="duplicate date"):
            BarSeries(dates=(date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 2)), rows=rows)

    def test_length_mismatch_rejected(self) -> None:
        with pytest.raises(ValueError, match="length mismatch"):
            BarSeries(dates=(date(2024, 1, 1),), rows=SERIES.rows[:2])

    def test_calendar_gaps_are_allowed_and_never_interpolated(self) -> None:
        """A missing session is normal; a fabricated bar to fill it is not."""
        rows = SERIES.rows[:5]
        gapped = BarSeries(
            dates=(date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 8), date(2024, 1, 9), date(2024, 1, 10)),
            rows=rows,
        )
        assert len(gapped) == 5


# ---------------------------------------------------------------------------
# Invariant 8 — NULL OHLC is not_evaluable, never coerced
# ---------------------------------------------------------------------------


class TestNotEvaluable:
    """Decision 5: a NULL that evaluates falsey makes 'could not evaluate'
    indistinguishable from 'did not fire', which silently corrupts a win-rate
    denominator."""

    @staticmethod
    def _with_null_close(index: int) -> BarSeries:
        rows = [dict(r) for r in SERIES.rows]
        rows[index]["close"] = None  # type: ignore[typeddict-item]
        return BarSeries(dates=SERIES.dates, rows=tuple(rows))  # type: ignore[arg-type]

    def test_window_indicator_reports_the_affected_indices(self) -> None:
        series = self._with_null_close(25)
        result = sma_series(series, universe=U, period=20)
        # Every window containing index 25 is unevaluable — 25 through 44,
        # bounded by the series length.
        assert 25 in result.not_evaluable_indices
        assert 30 in result.not_evaluable_indices
        assert all(result.values[i] is None for i in result.not_evaluable_indices)
        # ...and a window entirely before the NULL is unaffected.
        assert result.values[24] is not None

    def test_recursive_indicator_is_unevaluable_from_the_null_onward(self) -> None:
        """⚠ Unlike a rolling window, an EMA/RSI recursion has no window that
        rolls off — a NULL poisons everything after it, and pretending it
        recovers would be a fabricated value."""
        series = self._with_null_close(20)
        result = rsi_series(series, universe=U)
        assert result.not_evaluable_indices[0] == 20
        assert result.not_evaluable_indices[-1] == len(SERIES) - 1
        assert result.values[19] is not None

    def test_not_evaluable_is_distinguishable_from_warm_up(self) -> None:
        """Both are None in `values`; only one is listed. That distinction IS
        the invariant — without it the two collapse."""
        result = sma_series(self._with_null_close(25), universe=U, period=20)
        assert result.values[0] is None and 0 not in result.not_evaluable_indices  # warm-up
        assert result.values[25] is None and 25 in result.not_evaluable_indices  # unevaluable


# ---------------------------------------------------------------------------
# Invariant 10 — parameter validity
# ---------------------------------------------------------------------------


class TestParameterValidity:
    def test_non_positive_period_rejected(self) -> None:
        for period in (0, -1):
            with pytest.raises(ValueError, match="must be positive"):
                sma_series(SERIES, universe=U, period=period)

    def test_inverted_macd_pair_rejected(self) -> None:
        """An inverted pair sign-flips the histogram, which reads as a real
        signal and is invisible in any single value."""
        with pytest.raises(ValueError, match="fast must be <"):
            macd_series(SERIES, universe=U, fast=26, slow=12)

    def test_negative_num_std_rejected(self) -> None:
        with pytest.raises(ValueError, match="num_std"):
            bollinger_series(SERIES, universe=U, num_std=-1.0)


# ---------------------------------------------------------------------------
# Invariants 4, 5, 9 — provenance and the no-persistence contract
# ---------------------------------------------------------------------------


class TestProvenance:
    def test_universe_is_required_with_no_default(self) -> None:
        """#2288's labelling contract. A field with a default is a field a
        consumer can bypass — price_structure §6 makes the same argument."""
        with pytest.raises(TypeError):
            rsi_series(SERIES)  # type: ignore[call-arg]

    def test_every_result_carries_the_rule_set_version(self) -> None:
        assert rsi_series(SERIES, universe=U).rule_set_version == RULE_SET_VERSION
        assert macd_series(SERIES, universe=U).rule_set_version == RULE_SET_VERSION
        assert RULE_SET_VERSION.startswith(f"{RULE_SET_ID}+")

    def test_version_derives_from_module_source(self) -> None:
        """Following price_quarantine: over-invalidation is the deliberate
        trade, so a stored signal is visibly stale rather than silently mixed."""
        import hashlib
        from pathlib import Path

        import app.services.indicator_series as module

        expected = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()[:12]
        assert RULE_SET_VERSION == f"{RULE_SET_ID}+{expected}"


class TestNullPropagation:
    """[C2] Two contract violations Codex found at checkpoint 2, both cases
    where a NULL produced a `None` that was neither warm-up nor listed."""

    def test_atr_marks_a_bar_with_a_null_close_unevaluable(self) -> None:
        """⚠ TR reads high[i], low[i] and close[i-1] — NOT close[i] — so a bar
        with a null close still has a computable true range. It is refused
        anyway: emitting it would let a caller pair a real ATR with a missing
        close at the same index. `price_structure._atr_at` fails closed on any
        masked field for the same reason.
        """
        rows = [dict(r) for r in SERIES.rows]
        rows[25]["close"] = None  # type: ignore[typeddict-item]
        series = BarSeries(dates=SERIES.dates, rows=tuple(rows))  # type: ignore[arg-type]
        result = atr_series(series, universe=U)
        assert 25 in result.not_evaluable_indices
        assert result.values[25] is None

    def test_stochastic_d_inherits_k_unevaluability(self) -> None:
        """A NULL makes %K unevaluable at j, and every %D window containing j
        with it — for the following d_period - 1 bars. Those were previously a
        bare None: outside warm-up AND outside the list, which is the exact
        collapse the contract forbids."""
        rows = [dict(r) for r in SERIES.rows]
        rows[25]["high"] = None  # type: ignore[typeddict-item]
        series = BarSeries(dates=SERIES.dates, rows=tuple(rows))  # type: ignore[arg-type]
        result = stochastic_series(series, universe=U, period=14, d_period=3)

        listed = set(result.not_evaluable_indices)
        # Every index whose %D window touches an unevaluable %K is listed...
        assert {25, 26, 27} <= listed
        # ...and no index is left as an unexplained None outside warm-up.
        for i, value in enumerate(result.components["d"]):
            if value is None and i >= 14 + 3 - 1:
                assert i in listed, f"index {i}: None but neither warm-up nor listed"


class TestMultiSeriesAlignment:
    """[review] The length is DERIVED from the components, not carried beside
    them. A `_length` field that nothing validated let a direct construction
    produce a `__len__` disagreeing with the data — an alignment bug in the one
    object whose entire job is alignment."""

    def test_misaligned_components_rejected(self) -> None:
        from app.services.indicator_series import MultiIndicatorSeries

        with pytest.raises(ValueError, match="not aligned"):
            MultiIndicatorSeries(
                components={"a": (1.0, 2.0, 3.0), "b": (1.0, 2.0)},
                universe=U,
            )

    def test_len_tracks_the_components(self) -> None:
        assert len(macd_series(SERIES, universe=U)) == len(SERIES)
        assert len(bollinger_series(SERIES, universe=U)) == len(SERIES)
        assert len(stochastic_series(SERIES, universe=U)) == len(SERIES)


class TestWindowedAtrMatchesPriceStructure:
    """`atr_window_series` must reproduce `price_structure._atr_at` EXACTLY.

    ⚠ This is the test that makes ticket 2b safe, and it exists because the
    spec's original 2b was wrong: it proposed rewiring `_atr_at` onto
    `atr_series`, which is Wilder-smoothed from the series START and computes a
    DIFFERENT quantity. They agree at the seed and diverge after — index 25 on
    a 36-bar fixture gives 3.9610 (Wilder) vs 4.2500 (window). Swapping them
    would silently change every level tolerance, and with it which swings
    cluster into a level.
    """

    @staticmethod
    def _bars(masked: set[int]) -> tuple[list, BarSeries]:  # type: ignore[type-arg]
        from app.services.price_structure import StructureBar

        closes = [100.0]
        for i in range(120):
            closes.append(closes[-1] * (1.0 + (0.013 if i % 3 else -0.019)))

        def d(x: float) -> Decimal:
            return Decimal(str(round(x, 4)))

        structure = [
            StructureBar(
                bar_date=date(2024, 1, 1) + timedelta(days=i),
                open=d(c),
                high=None if i in masked else d(c * 1.01),
                low=None if i in masked else d(c * 0.99),
                close=None if i in masked else d(c),
                volume=1,
            )
            for i, c in enumerate(closes)
        ]
        rows = [{"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume} for b in structure]
        series = BarSeries(dates=tuple(b.bar_date for b in structure), rows=tuple(rows))  # type: ignore[arg-type]
        return structure, series

    @pytest.mark.parametrize("masked", [set(), {40}, {40, 90}])
    def test_matches_at_every_index(self, masked: set[int]) -> None:
        from app.services.indicator_series import atr_window_series
        from app.services.price_structure import _atr_at

        structure, series = self._bars(masked)
        streamed = atr_window_series(series, universe=U, period=14).values
        for i in range(len(structure)):
            reference = _atr_at(structure, i, 14)
            if reference is None:
                assert streamed[i] is None, f"index {i}: _atr_at None, series {streamed[i]}"
            else:
                assert streamed[i] is not None, f"index {i}: series None, _atr_at {reference}"
                assert abs(streamed[i] - reference) < 1e-9, f"index {i}"  # type: ignore[operator]

    def test_is_not_the_wilder_form(self) -> None:
        """⚠ Pins the distinction itself. If someone later 'simplifies' these
        into one function, this fails — which is the point."""
        from app.services.indicator_series import atr_series, atr_window_series

        _, series = self._bars(set())
        wilder = atr_series(series, universe=U, period=14).values
        window = atr_window_series(series, universe=U, period=14).values
        assert wilder[14] == pytest.approx(window[14])  # agree at the seed
        differing = sum(
            1
            for i in range(15, len(series))
            if wilder[i] is not None and window[i] is not None and abs(wilder[i] - window[i]) > 1e-9  # type: ignore[operator]
        )
        assert differing > 50, "the two ATR definitions should diverge after the seed"


class TestVectorisedWindowIndicators:
    """#2311 — bollinger and stochastic moved from Python loops to numpy.

    ⚠ REPRESENTATION CHANGED, DEFINITIONS DID NOT. Equivalence with
    `technical_analysis` is the invariant, and its binding check is the
    full-corpus sweep (acceptance 2). What lives HERE is everything that sweep
    structurally cannot see: the corpus carries zero NULL OHLC fields, so every
    unevaluable path below is covered by these fixtures and nothing else.

    Each of these is revert-probed by
    `scripts/probe_2311_indicator_vectorisation.py`.
    """

    @staticmethod
    def _flat_bars(n: int, *, null_close_at: int | None = None) -> BarSeries:
        """Bars whose high, low and close are all identical — the flat-range
        case the %K convention answers with 50.0."""
        rows: list[ta.OHLCVRow] = [
            {
                "open": Decimal("100"),
                "high": Decimal("100"),
                "low": Decimal("100"),
                "close": None if i == null_close_at else Decimal("100"),  # type: ignore[typeddict-item]
                "volume": 1_000,
            }
            for i in range(n)
        ]
        start = date(2024, 1, 1)
        return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(n)), rows=tuple(rows))

    def test_bollinger_every_prefix_matches_the_batch_form(self) -> None:
        """The prefix form, not just the last value — an off-by-one in the
        window offset moves every band by one bar and still matches at the end
        of a long series."""
        for k in range(2, len(_CLOSES) + 1):
            bands = bollinger_series(_bars(_CLOSES[:k]), universe=U, period=20).components
            expected = ta.bollinger_bands(CLOSES_D[:k], period=20)
            if expected is None:
                assert bands["upper"][-1] is None and bands["lower"][-1] is None, f"prefix {k}"
                continue
            assert bands["upper"][-1] == pytest.approx(expected[0], abs=1e-9), f"prefix {k}"
            assert bands["lower"][-1] == pytest.approx(expected[1], abs=1e-9), f"prefix {k}"

    def test_stochastic_every_prefix_matches_the_batch_form(self) -> None:
        """⚠ Compared on %D, because that is what `ta.stochastic` gates on: it
        needs k_period + d_period - 1 bars and returns both or neither, while
        %K here is legitimately defined d_period - 1 bars earlier."""
        for k in range(2, len(_CLOSES) + 1):
            comps = stochastic_series(_bars(_CLOSES[:k]), universe=U, period=14, d_period=3).components
            expected = ta.stochastic(ROWS[:k], 14, 3)
            if expected is None:
                assert comps["d"][-1] is None, f"prefix {k}"
                continue
            assert comps["k"][-1] == pytest.approx(expected[0], abs=1e-9), f"prefix {k}"
            assert comps["d"][-1] == pytest.approx(expected[1], abs=1e-9), f"prefix {k}"

    @pytest.mark.parametrize("n", [0, 1, 2, 13, 19])
    def test_a_series_shorter_than_the_window_is_warm_up_not_a_crash(self, n: int) -> None:
        """⚠ `sliding_window_view` RAISES when the window exceeds the array, so
        the length guard is load-bearing rather than cosmetic: a newly-listed
        instrument with 19 bars would abort a corpus sweep instead of reporting
        warm-up. The Python loops it replaced simply never entered the body."""
        series = _bars(_CLOSES[:n])
        bands = bollinger_series(series, universe=U, period=20)
        assert all(v is None for v in bands.components["middle"])
        assert bands.not_evaluable_indices == ()
        assert len(bands) == n

        # ⚠ period=20 here too, deliberately: at the default 14 an n=19 series
        # is WARM, not short, so the parametrisation would stop exercising the
        # guard on the very case it was added for. The first draft did exactly
        # that and the n=19 leg failed — correctly.
        stoch = stochastic_series(series, universe=U, period=20, d_period=3)
        assert all(v is None for v in stoch.components["k"])
        assert all(v is None for v in stoch.components["d"])
        assert stoch.not_evaluable_indices == ()
        assert len(stoch) == n

    def test_a_flat_window_with_a_null_close_is_unevaluable_not_fifty(self) -> None:
        """⚠ The flat-range convention must not outrank a missing close.

        Both conditions are true on this bar: the window high equals its low,
        AND the close is NULL. Taking the 50.0 branch would emit an oscillator
        reading for a bar that has no close — a fabricated observation at
        exactly the index a caller pairs with a real one. The Python form got
        this right structurally (it checked `close is None` before reaching the
        convention); the vectorised form has to say so explicitly.
        """
        series = self._flat_bars(20, null_close_at=19)
        result = stochastic_series(series, universe=U, period=14, d_period=3)
        assert result.components["k"][19] is None
        assert 19 in result.not_evaluable_indices
        # ...and the convention still applies where the close IS present.
        assert result.components["k"][18] == pytest.approx(50.0)

    def test_no_nan_ever_reaches_the_result_contract(self) -> None:
        """⚠ A NaN is strictly worse than a None here. `nan > x` and `nan < x`
        are BOTH False, so a NaN band answers "no" to every comparison a
        strategy makes and never announces itself — the vacuous-truth class
        decision 5 exists to prevent. NaN is the internal missing-marker; it
        must not survive the boundary."""
        import math

        rows = [dict(r) for r in SERIES.rows]
        rows[25]["close"] = None  # type: ignore[typeddict-item]
        rows[30]["high"] = None  # type: ignore[typeddict-item]
        series = BarSeries(dates=SERIES.dates, rows=tuple(rows))  # type: ignore[arg-type]

        for result in (
            bollinger_series(series, universe=U, period=20),
            stochastic_series(series, universe=U, period=14, d_period=3),
        ):
            for name, values in result.components.items():
                for i, value in enumerate(values):
                    assert value is None or not math.isnan(value), f"{name}[{i}] is NaN"
            listed = list(result.not_evaluable_indices)
            assert listed == sorted(set(listed)), "not_evaluable_indices must be sorted and unique"

    def test_bollinger_lists_every_window_containing_the_null_and_no_warm_up(self) -> None:
        """The warm-up prefix is None and NOT listed; a window containing the
        NULL is None and IS listed. Collapsing the two is the exact ambiguity
        `not_evaluable_indices` exists to remove."""
        rows = [dict(r) for r in SERIES.rows]
        rows[25]["close"] = None  # type: ignore[typeddict-item]
        series = BarSeries(dates=SERIES.dates, rows=tuple(rows))  # type: ignore[arg-type]
        result = bollinger_series(series, universe=U, period=20)

        # Windows ending at 25..39 all contain index 25; 40 bars in, that is all
        # of them to the end of the series.
        assert set(result.not_evaluable_indices) == set(range(25, len(SERIES)))
        assert result.components["upper"][25] is None
        assert result.components["middle"][24] is not None
        assert all(result.components["middle"][i] is None for i in range(19))


class TestBollingerNumericalStability:
    """Pins the two-pass variance against a "faster" one-pass rewrite.

    ⚠ THIS TEST EXISTS BECAUSE THE ONE-PASS FORM WAS SHIPPED AND REVERTED.
    `sumsq/n - mean^2` is O(1) instead of O(period), and a sample said it was
    safe: 48,707 bars from the three deepest corpus series gave a max band
    error of 2.4e-11 against the 1e-9 tolerance, a 40x margin.

    The FULL-CORPUS sweep then failed it — **193 mismatches on each band**
    across 7,354 series. The sample was three large caps; the corpus contains
    high-priced low-volatility names where mean^2 dwarfs the variance and the
    subtraction eats the significant digits.

    A high-price low-amplitude fixture reproduces it in milliseconds, so the
    next person tempted by the O(1) form finds out here rather than nine
    minutes into a corpus sweep.
    """

    @pytest.mark.parametrize(
        ("base", "amplitude"),
        [(10_000.0, 0.01), (100_000.0, 0.001)],
    )
    def test_matches_the_batch_form_where_one_pass_variance_fails(self, base: float, amplitude: float) -> None:
        closes = [base + (amplitude if i % 2 else -amplitude) + amplitude * 0.3 * (i % 7) for i in range(40)]
        series = _bars(closes)
        streamed = bollinger_series(series, universe=U, period=20).components
        batch = ta.bollinger_bands([Decimal(str(c)) for c in closes], period=20)
        assert batch is not None
        # The shipped two-pass form matches to 1e-9. The one-pass form is off by
        # 5.1e-06 at base=10,000 and 3.3e-03 at base=100,000 — thousands of
        # times the tolerance.
        assert streamed["upper"][-1] == pytest.approx(batch[0], abs=1e-9)
        assert streamed["lower"][-1] == pytest.approx(batch[1], abs=1e-9)


class TestRunningSumPrecision:
    """[review WARNING] A running sum is the same class of shortcut as the
    reverted one-pass Bollinger variance, and it shipped with no proof.

    Checked the same way, against an exact `math.fsum` reference over 20,000
    bars:

        base 1e2   abs 1.4e-14   rel 1.4e-16
        base 1e5   abs 4.4e-11   rel 4.4e-16
        base 1e9   abs 2.4e-07   rel 2.4e-16

    ⚠ The verdict is the opposite of the Bollinger one. Relative error is ~1-2
    ULP at every magnitude, so the accumulator is as accurate as float64
    allows. An earlier draft read the 2.4e-07 as drift and added periodic
    re-seeding; that was a misattribution — one ULP at 1e9 IS 1.2e-07, and
    `ta.sma` misses an absolute 1e-9 there too. The tolerance was the defect,
    and the harness now compares relatively.
    """

    @pytest.mark.parametrize("base", [1e2, 1e5, 1e9])
    def test_relative_agreement_holds_at_every_magnitude(self, base: float) -> None:
        import math

        amp, n = base * 1e-9, 3_000
        closes = [base + (amp if i % 2 else -amp) + amp * 0.3 * (i % 7) for i in range(n)]
        streamed = sma_series(_bars(closes), universe=U, period=20).values
        for i in range(19, n, 91):
            exact = math.fsum(closes[i - 19 : i + 1]) / 20
            assert streamed[i] is not None
            assert abs(streamed[i] - exact) <= 1e-12 * abs(exact), f"index {i}"  # type: ignore[operator]

    def test_bollinger_mean_too(self) -> None:
        import math

        base, n = 1e9, 2_000
        closes = [base + (1.0 if i % 2 else -1.0) + 0.3 * (i % 7) for i in range(n)]
        middle = bollinger_series(_bars(closes), universe=U, period=20).components["middle"]
        for i in range(19, n, 89):
            exact = math.fsum(closes[i - 19 : i + 1]) / 20
            assert middle[i] is not None
            assert abs(middle[i] - exact) <= 1e-12 * abs(exact), f"index {i}"  # type: ignore[operator]


# ---------------------------------------------------------------------------
# The conversion cache
# ---------------------------------------------------------------------------


class _CountingDecimal(Decimal):
    """A Decimal that records every `float()` taken of it.

    ⚠ Counts on the CLASS, so a test must reset before measuring.
    """

    conversions = 0

    def __float__(self) -> float:
        type(self).conversions += 1
        return super().__float__()


def _counting_bars(closes: list[int]) -> BarSeries:
    rows: list[ta.OHLCVRow] = [
        {
            "open": _CountingDecimal(str(c)),
            "high": _CountingDecimal(str(c + 1.5)),
            "low": _CountingDecimal(str(c - 1.5)),
            "close": _CountingDecimal(str(c)),
            "volume": 1_000,
        }
        for c in closes
    ]
    start = date(2024, 1, 1)
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


class TestConversionHappensOncePerField:
    """Decimal -> float conversion is O(bars), not O(bars x indicators).

    ⚠ THIS IS A CORRECTNESS-SHAPED TEST FOR A PERFORMANCE INVARIANT, and it
    exists because nothing else in this file would notice if the cache went
    away. Every value stays right without it — the only symptom is that the
    corpus sweep goes from ~35 s back towards the 305.6 s that made ticket 2a
    add the cache in the first place. A regression with no failing test is
    exactly the kind that survives review.

    Counting `__float__` rather than asserting `x is x` is deliberate: identity
    proves a value was memoised, not that the conversion happened ONCE. A cache
    populated per-call would satisfy identity and still be O(bars x indicators).

    ⚠ Also pins that nothing converts `open`. It is the one OHLC field with no
    float view, and the count would rise by `len(bars)` if a future indicator
    reached for it without adding one.
    """

    def test_seven_indicators_convert_each_field_exactly_once(self) -> None:
        bars = _counting_bars(_CLOSES)
        _CountingDecimal.conversions = 0

        sma_series(bars, universe=U, period=20)
        ema_series(bars, universe=U, period=12)
        rsi_series(bars, universe=U)
        atr_series(bars, universe=U)
        macd_series(bars, universe=U)
        bollinger_series(bars, universe=U)
        stochastic_series(bars, universe=U)

        # close, high, low — once each per bar. `open` is never converted.
        assert _CountingDecimal.conversions == 3 * len(_CLOSES)

    def test_the_ndarray_views_reuse_the_float_views(self) -> None:
        """`array_*` must build from the float cache, not re-convert Decimals."""
        bars = _counting_bars(_CLOSES)
        _ = bars.float_closes, bars.float_highs, bars.float_lows
        _CountingDecimal.conversions = 0

        _ = bars.array_closes, bars.array_highs, bars.array_lows

        assert _CountingDecimal.conversions == 0

    def test_the_cache_survives_on_a_frozen_instance(self) -> None:
        """The frozen dataclass must not defeat the memoisation.

        `cached_property` writes into `instance.__dict__`; `frozen=True` only
        overrides `__setattr__`. If a future edit adds `slots=True` there is no
        instance `__dict__` to write into and this fails at first ACCESS — the
        class definition itself stays legal, so nothing else would catch it.
        """
        bars = _counting_bars(_CLOSES)
        assert bars.float_closes is bars.float_closes
        assert bars.array_closes is bars.array_closes
        with pytest.raises(Exception):  # noqa: B017 - FrozenInstanceError, by construction
            bars.dates = ()  # type: ignore[misc]
