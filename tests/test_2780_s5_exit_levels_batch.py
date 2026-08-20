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
from app.services.strategy_exit_levels_batch import (
    s4_exit_levels_batch,
    s5_exit_levels_batch,
    s6_exit_levels_batch,
    s7_exit_levels_batch,
    s8_exit_levels_batch,
    s9_exit_levels_batch,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST

UNIVERSE = "survivorship_free"

#: ⚠ Each entry names the MINIMUM brackets its fixture must build. A batch that
#: only ever returns refusals compares equal to its oracle while proving nothing
#: about the shared indicators — the first version of this file did exactly that.
BATCHED = (
    ("s4-volatility-compression-breakout", s4_exit_levels_batch, 40),
    ("s5-support-bounce", s5_exit_levels_batch, 3),
    ("s6-resistance-breakout", s6_exit_levels_batch, 40),
    ("s7-trend-pullback", s7_exit_levels_batch, 40),
    ("s8-range-mean-reversion", s8_exit_levels_batch, 40),
    ("s9-squeeze-expansion", s9_exit_levels_batch, 40),
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
    def test_an_out_of_range_request_behaves_exactly_as_its_oracle_does(
        self, strategy_id: str, batch: object, min_brackets: int
    ) -> None:
        """⚠⚠ THE ASSERTION IS AGREEMENT, NOT UNIFORMITY, AND THAT DISTINCTION
        FOUND A REAL INCONSISTENCY.

        Five of the six refuse an out-of-range index as
        ``unorderable_exit_levels`` — the property #2437's refusal-surface test
        exists for, since an uncaught exception aborts the WHOLE outcome batch
        for one bad bar. **S-4 raises instead**, because ``s4_exit_levels_batch``
        validates the index up front and ``_s4_exit_levels`` does not catch it.
        S-4 is excluded from that refusal test on the grounds that it "has its
        own equivalence check", and that check never covered a bad index.

        Demanding uniformity here would have silently changed S-4's shipped
        behaviour to make a test pass. So this asserts what the adapter contract
        actually claims — the batch does whatever its oracle does — and the
        divergence between S-4 and its siblings is reported separately rather
        than papered over.
        """
        series = _wavy()
        entry = STRATEGY_MANIFEST[strategy_id]
        # ⚠ Bound to a local before the lambda. Narrowing an ATTRIBUTE does not
        # survive into a closure — the checker cannot assume it is still
        # non-None when the lambda runs — and the pre-push gate refused the
        # version that relied on it.
        scalar = entry.exit_levels
        assert scalar is not None
        bad = (9_999, Decimal("200"))

        def outcome(call: object) -> object:
            try:
                return ("returned", call())  # type: ignore[operator]
            except Exception as exc:  # noqa: BLE001 - the exception TYPE is the observation
                return ("raised", type(exc).__name__)

        oracle = outcome(lambda: scalar(series, signal_index=bad[0], entry_price=bad[1], universe=UNIVERSE))
        batched = outcome(lambda: batch(series, requests=(bad,), universe=UNIVERSE)[0])  # type: ignore[operator]
        assert batched == oracle, f"{strategy_id}: batch and oracle disagree on an out-of-range index"

    @pytest.mark.parametrize(("strategy_id", "batch", "min_brackets"), BATCHED)
    def test_a_bad_request_does_not_lose_its_neighbours(
        self, strategy_id: str, batch: object, min_brackets: int
    ) -> None:
        """Positional output: a refusal must land on ITS request and no other.

        ⚠ Bar 0 has no prior close and therefore no ATR. Five strategies refuse
        it; S-4 RAISES, because ``s4_exit_levels_batch`` has no ``try``/``except``
        at all — it raises for an out-of-range index, a non-finite or non-positive
        entry price, and a missing ATR, and refuses only for a non-finite ATR or
        an unorderable bracket. So S-4 aborts a whole outcome batch where its
        five siblings record one unresolved outcome.

        Not reachable today, because entries only fire on evaluable bars — but it
        is one masked bar from being reachable, and it is the exact failure mode
        #2437's refusal-surface test exists to prevent. Asserted as AGREEMENT with
        its own oracle rather than silently normalised, and reported separately.
        """
        series = _wavy()
        entry = STRATEGY_MANIFEST[strategy_id]
        assert entry.exit_levels is not None
        good_a = (150, Decimal(str(series.float_closes[150])))
        bad = (0, Decimal("100"))
        good_b = (152, Decimal(str(series.float_closes[152])))

        try:
            oracle = entry.exit_levels(series, signal_index=bad[0], entry_price=bad[1], universe=UNIVERSE)
        except Exception as exc:  # noqa: BLE001 - the raising strategies are the finding
            with pytest.raises(type(exc)):
                batch(series, requests=(good_a, bad, good_b), universe=UNIVERSE)  # type: ignore[operator]
            return

        actual = batch(series, requests=(good_a, bad, good_b), universe=UNIVERSE)  # type: ignore[operator]
        assert len(actual) == 3
        assert actual[1] == oracle, "an unevaluable bar must refuse exactly as its oracle does"
        assert actual[1] == "unorderable_exit_levels"

    @pytest.mark.parametrize(("strategy_id", "batch", "min_brackets"), BATCHED)
    def test_duplicate_requests_stay_positionally_distinct(
        self, strategy_id: str, batch: object, min_brackets: int
    ) -> None:
        series = _wavy()
        price = Decimal(str(series.float_closes[150]))
        actual = batch(series, requests=((150, price), (150, price)), universe=UNIVERSE)  # type: ignore[operator]
        assert len(actual) == 2
        assert actual[0] == actual[1], "identical requests must produce identical brackets"


def test_every_bracket_strategy_has_a_batch_factory_under_test() -> None:
    """⚠⚠ THE GUARD AGAINST THE NEXT S-5.

    S-5 went without the batch factory S-4 had since #2623, and nothing said so —
    the runner silently fell back to the per-signal path and the cost showed up
    only in a profile, 7.5 hours into a 13.6-hour run. A manifest entry that
    declares ``exit_levels`` and no ``exit_levels_batch`` is that state exactly.

    ⚠ It also requires the batched strategy to appear in ``BATCHED`` above, so a
    factory cannot be registered without an equivalence proof. Registering one is
    the easy half; proving it equals its oracle is the half that matters.
    """
    unbatched = sorted(
        strategy_id
        for strategy_id, entry in STRATEGY_MANIFEST.items()
        if entry.exit_levels is not None and entry.exit_levels_batch is None
    )
    assert unbatched == [], f"{unbatched} pay a whole-series indicator rebuild per signal"

    covered = {strategy_id for strategy_id, _, _ in BATCHED}
    declared = {strategy_id for strategy_id, entry in STRATEGY_MANIFEST.items() if entry.exit_levels_batch is not None}
    assert declared == covered, f"batch factories without an equivalence proof: {sorted(declared - covered)}"
