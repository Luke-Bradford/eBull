"""S-9 is a CONTROLLED comparison against S-4, and these pin what is controlled.

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §3 (S-9).

S-4 lost to buy-and-hold in every hold-out year (0 of 5, 2022-2026). S-9 keeps
its breakout leg IDENTICAL and changes exactly two things — the compression test
becomes Bollinger's published Squeeze, and a regime gate is added. That is only
an attributable experiment while the held-constant parts actually stay constant,
and "stay constant" is a property nothing enforces on its own: the two modules
deliberately do NOT share a symbol, because importing S-4's constant into S-9
would couple S-9's identity hash to S-4's module bytes and an unrelated comment
edit in S-4 would invalidate S-9's stored track record.

So the equality is asserted here rather than expressed as a shared import. If a
future edit moves either lookback, this fails and names the reason — which is
the whole point, because the alternative is a silently uncontrolled comparison
that still LOOKS like an experiment.
"""

from __future__ import annotations

from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_STOP_MULTIPLE as S4_STOP,
)
from app.services.strategies.s4_volatility_compression_breakout import (
    ATR_TARGET_MULTIPLE as S4_TARGET,
)
from app.services.strategies.s4_volatility_compression_breakout import (
    BREAKOUT_LOOKBACK as S4_BREAKOUT_LOOKBACK,
)
from app.services.strategies.s4_volatility_compression_breakout import (
    MAX_HOLD_BARS as S4_MAX_HOLD,
)
from app.services.strategies.s9_squeeze_expansion import (
    ATR_STOP_MULTIPLE as S9_STOP,
)
from app.services.strategies.s9_squeeze_expansion import (
    ATR_TARGET_MULTIPLE as S9_TARGET,
)
from app.services.strategies.s9_squeeze_expansion import (
    BREAKOUT_LOOKBACK as S9_BREAKOUT_LOOKBACK,
)
from app.services.strategies.s9_squeeze_expansion import (
    MAX_HOLD_BARS as S9_MAX_HOLD,
)
from app.services.strategies.s9_squeeze_expansion import (
    PERMITTED_REGIMES,
    prior_high_close_series,
)


class TestTheHeldConstantParts:
    """What must NOT differ, or the comparison attributes nothing."""

    def test_the_breakout_lookback_is_identical(self) -> None:
        """⚠ The shared leg. If these diverge, a difference in results can no
        longer be attributed to the compression test or the regime gate — the
        experiment silently becomes two unrelated strategies."""
        assert S9_BREAKOUT_LOOKBACK == S4_BREAKOUT_LOOKBACK

    def test_the_bracket_is_identical(self) -> None:
        """Stop, target and hold cap are held constant for the same reason.

        ⚠ A different bracket changes the OUTCOME of an identical signal, so a
        bracket difference would masquerade as a signal difference — the most
        misleading failure available here, because both strategies would still
        be firing on the same bars.
        """
        assert (S9_STOP, S9_TARGET, S9_MAX_HOLD) == (S4_STOP, S4_TARGET, S4_MAX_HOLD)


class TestTheChangedParts:
    """What must differ, or S-9 is a duplicate wearing a new id."""

    def test_s9_gates_on_regime_and_s4_does_not(self) -> None:
        """S-4 has no regime concept at all — it predates one. S-9 declaring a
        non-empty permitted set is what makes the gate a real second variable."""
        assert PERMITTED_REGIMES
        assert not hasattr(
            __import__(
                "app.services.strategies.s4_volatility_compression_breakout",
                fromlist=["PERMITTED_REGIMES"],
            ),
            "PERMITTED_REGIMES",
        )


class TestTheBreakoutWindowBoundary:
    """The prior-high window EXCLUDES the signal bar, exactly as S-4's does."""

    def test_the_window_excludes_the_signal_bar(self) -> None:
        """⚠ ``close(t) > max(closes INCLUDING close(t))`` is satisfiable only by
        a tie and is partly self-referential — S-4's own docstring says so. S-9
        inherits the boundary; this pins that it did not drift while being
        retyped rather than imported.

        Bars 0..19 are unevaluable (window not full). At bar 20 the window is
        bars 0..19, whose maximum close is 19.0 — NOT 20.0, which is bar 20's
        own close and would prove the window had swallowed the signal bar.
        """
        from datetime import date, timedelta
        from decimal import Decimal

        from app.services.indicator_series import BarSeries

        n = 30
        start = date(2024, 1, 1)
        rows = tuple(
            {
                "open": Decimal(str(i)),
                "high": Decimal(str(i)),
                "low": Decimal(str(i)),
                "close": Decimal(str(i)),
                "volume": 1_000,
            }
            for i in range(n)
        )
        series = BarSeries(dates=tuple(start + timedelta(days=i) for i in range(n)), rows=rows)  # type: ignore[arg-type]
        prior = prior_high_close_series(series, universe="survivor_only")

        assert prior.values[19] is None, "the window is not full before bar 20"
        assert prior.values[20] == 19.0, "bar 20's window must be bars 0..19, excluding bar 20 itself"
        assert prior.values[25] == 24.0
