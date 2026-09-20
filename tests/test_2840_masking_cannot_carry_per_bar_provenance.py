"""Masking is DELIBERATELY terminal, so it cannot carry a per-bar fact (#2840).

Why this test exists rather than a sentence in a doc: the per-series price-basis
carrier was drafted around "a bar whose basis we cannot certify is delivered
masked". That is unsound, and the reason is a measurement rather than an
argument — one masked close does not refuse one bar, it refuses every bar after
it, for the whole remaining series.

⚠ THIS PINS A DECISION, NOT A BUG. ``adx_series``' docstring states the rule:
*"Wilder smoothing is recursive: a gap does not affect one value, it shifts
every value after it. Resuming past a hole would produce numbers that look valid
and are not, so everything from the first unusable bar is not_evaluable."*
``atr_series`` implements it as ``unevaluable.extend(range(first_null, len(rows)))``.

So the test asserts the CONTRACT holds, and the carrier design has to live with
it: a per-bar provenance signal must be readable by a gate WITHOUT entering the
indicator recursion. If this test ever fails because masking became bar-local,
that is a change to the causality contract and the carrier decision is reopened.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from app.services.indicator_series import BarSeries, atr_series
from app.services.technical_analysis import OHLCVRow

BARS = 200
MASK_AT = 120
PERIOD = 14


def _row(index: int, *, close_masked: bool = False) -> OHLCVRow:
    # ⚠ A sawtooth rather than a ramp: a monotone series makes every true range
    # identical, which would hide an off-by-one in the recursion seed.
    #
    # ⚠ The ``type: ignore`` is this repo's existing idiom for a MASKED field —
    # ``OHLCVRow`` types ``close`` as a ``Decimal`` because a stored bar always
    # has one, and ``price_masked_bars.load_masked_bars`` substitutes ``None``
    # after the fact. ``tests/test_indicator_series.py`` builds a masked close
    # the same way; cited without a line number deliberately, because a test
    # file's line numbers move and a stale pointer is worse than none.
    base = Decimal(100 + index % 7)
    return {
        "open": base,
        "high": base + 2,
        "low": base - 2,
        "close": None if close_masked else base,  # type: ignore[typeddict-item]
        "volume": 1_000,
    }


def _series(*, mask_index: int | None) -> BarSeries:
    start = date(2024, 1, 1)
    return BarSeries(
        dates=tuple(start + timedelta(days=i) for i in range(BARS)),
        rows=tuple(_row(i, close_masked=(i == mask_index)) for i in range(BARS)),
    )


def _unevaluable(series: BarSeries) -> set[int]:
    result = atr_series(series, universe="survivorship_free", period=PERIOD)
    return {i for i, value in enumerate(result.values) if value is None}


def test_a_clean_series_is_unevaluable_only_through_its_warmup() -> None:
    """The control. Without it the treatment's count means nothing."""
    assert _unevaluable(_series(mask_index=None)) == set(range(PERIOD))


def test_one_masked_close_refuses_every_later_bar_and_never_recovers() -> None:
    """The measurement the carrier decision rests on.

    80 of the 80 bars at or after the mask are refused — not one, and not a
    window of ``PERIOD``.
    """
    clean = _unevaluable(_series(mask_index=None))
    masked = _unevaluable(_series(mask_index=MASK_AT))

    caused = masked - clean
    assert caused == set(range(MASK_AT, BARS))
    assert len(caused) == BARS - MASK_AT == 80
    # The tail specifically: the last bar of the series is still refused, so
    # there is no recovery horizon a caller could wait out.
    assert BARS - 1 in caused


def test_the_refusal_is_not_a_window_of_the_smoothing_period() -> None:
    """Rules out the reading that would make masking usable.

    If the damage were bounded by ``PERIOD``, a composer could mask an
    uncertified bar and lose ``PERIOD`` bars of evaluability. It is not bounded:
    the count scales with the DISTANCE TO THE END of the series, which is what
    makes the mechanism unusable as a per-bar carrier.
    """
    early = _unevaluable(_series(mask_index=MASK_AT)) - _unevaluable(_series(mask_index=None))
    later = _unevaluable(_series(mask_index=MASK_AT + 40)) - _unevaluable(_series(mask_index=None))

    assert len(early) - len(later) == 40
    assert len(early) > PERIOD and len(later) > PERIOD
