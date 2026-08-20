"""The batch exit factories must equal their scalar oracles, request for request.

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
from app.services.strategy_exit_levels_batch import s5_exit_levels_batch, s6_exit_levels_batch
from app.services.strategy_manifest import STRATEGY_MANIFEST

UNIVERSE = "survivorship_free"

#: ⚠ Each entry names the MINIMUM brackets its fixture must build. A batch that
#: only ever returns refusals compares equal to its oracle while proving nothing
#: about the shared indicators — the first version of this file did exactly that.
BATCHED = (
    ("s5-support-bounce", s5_exit_levels_batch, 3),
    ("s6-resistance-breakout", s6_exit_levels_batch, 40),
)


def _wavy(n: int = 200) -> BarSeries:
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

    Measured on this construction: S-5 builds 3 brackets and S-6 builds 51, the
    rest refusing. That mix is the point — it exercises both paths.

    ⚠ Entry prices are taken from each bar's own close, not a constant. With a
    flat entry the level-anchored stop lands ABOVE the entry on most bars and
    `exit_levels_are_orderable` refuses — which reads as "no level found" and
    sent me looking at the fixture's shape when the fault was the test's.
    """
    rows = []
    for i in range(n):
        cycle = i % 20
        triangle = cycle / 10.0 if cycle <= 10 else (20 - cycle) / 10.0
        # ⚠ The late push is S-6's half of the fixture: its level must sit BELOW
        # the close (a broken resistance), which a bounded cycle never produces.
        base = 100.0 + 8.0 * triangle + max(0.0, (i - 140) * 0.45)
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
    """⚠ Parameterised over every batched strategy rather than duplicated per
    strategy: the contract is identical, and a copy is how one of them quietly
    stops being checked when the next is added.
    """

    @pytest.mark.parametrize(("strategy_id", "batch", "min_brackets"), BATCHED)
    def test_every_request_matches_the_registered_scalar_factory(
        self, strategy_id: str, batch: object, min_brackets: int
    ) -> None:
        """⚠ Compared against the MANIFEST's factory, not the raw bracket: the
        registered adapter is what the runner would otherwise call, and it
        carries the try/except and the orderable check the bracket does not.
        """
        series = _wavy()
        entry = STRATEGY_MANIFEST[strategy_id]
        assert entry.exit_levels is not None
        assert entry.exit_levels_batch is not None, f"{strategy_id} must declare the batch factory"

        requests = tuple((index, Decimal(str(series.float_closes[index]))) for index in range(20, len(series)))
        expected = tuple(
            entry.exit_levels(series, signal_index=index, entry_price=price, universe=UNIVERSE)
            for index, price in requests
        )
        actual = batch(series, requests=requests, universe=UNIVERSE)  # type: ignore[operator]

        assert len(actual) == len(expected)
        assert actual == expected, "the batch is a speed-up, so a single differing request is a strategy change"
        built = sum(isinstance(item, ExitLevels) for item in actual)
        assert built >= min_brackets, f"only {built} brackets built — comparing refusals, not levels"

    @pytest.mark.parametrize(("strategy_id", "batch", "min_brackets"), BATCHED)
    def test_the_indicators_are_derived_ONCE_for_the_whole_batch(
        self, strategy_id: str, batch: object, min_brackets: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """⚠ The equality test passes whether or not anything was shared —
        rebuilding per request is equally correct and equally slow. This is the
        half that pins the actual optimisation.
        """
        series = _wavy()
        calls = 0
        real_atr = batch_module.atr_series

        def counted_atr(*args: object, **kwargs: object) -> IndicatorSeries:
            nonlocal calls
            calls += 1
            return real_atr(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(batch_module, "atr_series", counted_atr)
        requests = tuple((index, Decimal(str(series.float_closes[index]))) for index in range(20, 160))
        batch(series, requests=requests, universe=UNIVERSE)  # type: ignore[operator]

        assert calls == 1, f"{strategy_id} derived ATR {calls} times for {len(requests)} requests"

    @pytest.mark.parametrize(("strategy_id", "batch", "min_brackets"), BATCHED)
    def test_an_out_of_range_request_refuses_without_losing_the_others(
        self, strategy_id: str, batch: object, min_brackets: int
    ) -> None:
        """⚠ One bad bar must not abort the batch — the property the scalar
        adapter's ``except ValueError, IndexError`` exists for. Positional output
        means the refusal has to land on ITS request and no other.
        """
        series = _wavy()
        entry = STRATEGY_MANIFEST[strategy_id]
        assert entry.exit_levels is not None
        requests = (
            (150, Decimal(str(series.float_closes[150]))),
            (9_999, Decimal("200")),
            (152, Decimal(str(series.float_closes[152]))),
        )
        actual = batch(series, requests=requests, universe=UNIVERSE)  # type: ignore[operator]
        expected = tuple(
            entry.exit_levels(series, signal_index=index, entry_price=price, universe=UNIVERSE)
            for index, price in requests
        )
        assert actual[1] == "unorderable_exit_levels"
        assert actual == expected

    @pytest.mark.parametrize(("strategy_id", "batch", "min_brackets"), BATCHED)
    def test_duplicate_requests_stay_positionally_distinct(
        self, strategy_id: str, batch: object, min_brackets: int
    ) -> None:
        series = _wavy()
        price = Decimal(str(series.float_closes[150]))
        actual = batch(series, requests=((150, price), (150, price)), universe=UNIVERSE)  # type: ignore[operator]
        assert len(actual) == 2
        assert actual[0] == actual[1], "identical requests must produce identical brackets"
