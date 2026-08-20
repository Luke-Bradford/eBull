"""S-5's batch exit factory must equal its scalar oracle, request for request.

Refs #2780. Module under test: ``app/services/strategy_exit_levels_batch.py``.

⚠⚠ A PERFORMANCE ADAPTER, SO EQUIVALENCE IS THE WHOLE CONTRACT. The scalar
``_s5_exit_levels`` remains the semantic owner — ``StrategyEntry`` refuses a
batch factory declared without it, precisely so there is always an oracle — and
the batch may share only immutable indicator work. Anything else it changed
would be a silent strategy change wearing a speed-up's name.

WHY IT EXISTS
-------------
``s5_exit_bracket`` derives the ATR, the swing-pivot scan and the volume array
from the WHOLE series and then reads one bar out of each. Evaluating fills one
at a time therefore rebuilt all three per signal. Measured on a 300-series s5
profile: ``atr_series`` 24,378 calls / 51.2s cumulative, ``_volumes`` 24,634
calls / 24.5s, ``swing_pivots`` 12.3s — about 38% of the evaluation, redundant.

⚠ The runner already had the seam: ``_exit_levels_for_entries`` groups entries by
segment and calls ``exit_levels_batch`` when a strategy declares one. S-4 has had
one since #2623. S-5 simply never got it.

Pure tier: no database.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services import strategy_exit_levels_batch as batch_module
from app.services.indicator_series import BarSeries, IndicatorSeries
from app.services.outcome_resolver import ExitLevels
from app.services.strategy_exit_levels_batch import s5_exit_levels_batch
from app.services.strategy_manifest import STRATEGY_MANIFEST

UNIVERSE = "survivorship_free"
_S5 = "s5-support-bounce"


def _wavy(n: int = 160) -> BarSeries:
    """A cyclical series that actually forms support and then wicks through it.

    ⚠⚠ THE FIXTURE IS THE HARD PART, AND THE FIRST ONE WAS SILENTLY USELESS. A
    drifting ramp never revisits a price, so no cluster reaches ``MIN_TOUCHES``
    (3) and EVERY request refuses — the batch and the scalar then agree on
    nothing but refusals, which proves neither the shared ATR nor the shared
    scan. The equality assertion passed; the bracket-count guard is what caught
    it.

    So this is built to satisfy the three conditions together: a repeating cycle
    so pivot lows recur and cluster; a small per-cycle jitter so the cluster mean
    sits between its members rather than exactly on one; and a deeper wick at the
    cycle turn so a query bar can have ``low < level <= close``, which is what
    ``_support_below`` requires — the level the bar wicked through and reclaimed.

    Measured on this construction: 3 of 140 candidate bars build a bracket, the
    rest refuse. That mix is the point — it exercises both paths.
    """
    rows = []
    for i in range(n):
        cycle = i % 20
        triangle = cycle / 10.0 if cycle <= 10 else (20 - cycle) / 10.0
        base = 100.0 + 8.0 * triangle
        jitter = ((i // 20) % 3) * 0.15
        low = base - 1.5 - (0.8 if cycle in (0, 1, 19) else 0.0) + jitter
        rows.append(
            {
                "open": Decimal(str(round(base, 4))),
                "high": Decimal(str(round(base + 1.5, 4))),
                "low": Decimal(str(round(low, 4))),
                "close": Decimal(str(round(base + 0.25, 4))),
                "volume": 1_000 + (i % 7) * 250,
            }
        )
    return BarSeries(
        dates=tuple(date(2024, 1, 1) + timedelta(days=i) for i in range(n)),
        rows=tuple(rows),  # type: ignore[arg-type]
    )


class TestBatchEqualsTheScalarOracle:
    def test_every_request_matches_the_registered_scalar_factory(self) -> None:
        """⚠ Compared against the MANIFEST's factory, not against
        ``s5_exit_bracket`` directly: the registered adapter is what the runner
        would otherwise call, and it carries the try/except and the orderable
        check that the bracket does not.
        """
        series = _wavy()
        entry = STRATEGY_MANIFEST[_S5]
        assert entry.exit_levels is not None
        assert entry.exit_levels_batch is not None, "S-5 must declare the batch factory"

        requests = tuple((index, Decimal(str(100 + index))) for index in range(20, len(series)))
        expected = tuple(
            entry.exit_levels(series, signal_index=index, entry_price=price, universe=UNIVERSE)
            for index, price in requests
        )
        actual = s5_exit_levels_batch(series, requests=requests, universe=UNIVERSE)

        assert len(actual) == len(expected)
        assert actual == expected, "the batch is a speed-up, so a single differing request is a strategy change"
        # The fixture must exercise the level path, or this compares refusals.
        built = sum(isinstance(item, ExitLevels) for item in actual)
        assert built >= 3, f"only {built} brackets built — the fixture is comparing refusals, not levels"

    def test_the_indicators_are_derived_ONCE_for_the_whole_batch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """⚠ The equality test above passes whether or not anything was shared —
        rebuilding per request is equally correct and equally slow. This is the
        half that pins the actual optimisation.
        """
        series = _wavy()
        atr_calls = 0
        volume_calls = 0
        real_atr = batch_module.atr_series
        real_volumes = batch_module._volumes

        def counted_atr(*args: object, **kwargs: object) -> IndicatorSeries:
            nonlocal atr_calls
            atr_calls += 1
            return real_atr(*args, **kwargs)  # type: ignore[arg-type]

        def counted_volumes(*args: object, **kwargs: object) -> object:
            nonlocal volume_calls
            volume_calls += 1
            return real_volumes(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(batch_module, "atr_series", counted_atr)
        monkeypatch.setattr(batch_module, "_volumes", counted_volumes)

        requests = tuple((index, Decimal(str(100 + index))) for index in range(20, 140))
        s5_exit_levels_batch(series, requests=requests, universe=UNIVERSE)

        assert atr_calls == 1, f"ATR derived {atr_calls} times for {len(requests)} requests"
        assert volume_calls == 1, f"volumes derived {volume_calls} times for {len(requests)} requests"

    def test_an_out_of_range_request_refuses_without_losing_the_others(self) -> None:
        """⚠ One bad bar must not abort the batch — the same property the scalar
        adapter's ``except ValueError, IndexError`` exists for. Positional output
        means the refusal has to land on ITS request and no other.
        """
        series = _wavy()
        entry = STRATEGY_MANIFEST[_S5]
        assert entry.exit_levels is not None
        requests = ((30, Decimal("130")), (999, Decimal("200")), (45, Decimal("145")))

        actual = s5_exit_levels_batch(series, requests=requests, universe=UNIVERSE)
        expected = tuple(
            entry.exit_levels(series, signal_index=index, entry_price=price, universe=UNIVERSE)
            for index, price in requests
        )

        assert actual[1] == "unorderable_exit_levels"
        assert actual == expected

    def test_duplicate_requests_stay_positionally_distinct(self) -> None:
        series = _wavy()
        requests = ((40, Decimal("140")), (40, Decimal("140")), (40, Decimal("999")))
        actual = s5_exit_levels_batch(series, requests=requests, universe=UNIVERSE)
        assert len(actual) == 3
        assert actual[0] == actual[1], "identical requests must produce identical brackets"
