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
