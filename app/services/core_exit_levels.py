"""The core sleeve's broker-side stop and target, and where their sizes come from (#3284).

Operator decision, 2026-09-21: *"would expect a safety net of sl and tp in place at
all times for this to safe guard spikes"*.  This module is the one place the two
percentages live, and the one place that turns an entry price into the two absolute
rates eToro's API takes (``stopLossRate`` / ``takeProfitRate`` are levels, never
percentages).

⚠⚠ **This REVERSES the core arm's documented no-SL/TP exemption**, which was settled
with #2603 step 2 and written into ``strategy_position_manager.manage_owned_position``
as *"a stop on a benchmark holding sells the benchmark into a drawdown"*.  That
reasoning was sound and is not refuted here -- the operator has weighed it against
spike risk and chosen the stop.  A later session reading only the old comment would
restore the exemption as 'correct', which is why the reversal is recorded on #3284 and
#2437 as well as here.

⚠⚠ **The accepted cost, stated so it is not rediscovered as a defect.**  This is a
deterministic buy-and-hold BETA sleeve with **no re-entry rule**.  In a sustained bear
(2008: -55%; 2020: -34% over weeks) a -25% stop sells near the bottom and the sleeve
then sits in cash indefinitely.  That is the price of the spike guard and the operator
has accepted it.  Do NOT 'fix' it by tightening the stop, and do NOT tighten the target:
a near take-profit converts the sleeve into a market-timing strategy, which is what the
price-only steer cut (#2837).  Profit-taking is already handled by the rebalance band,
which trims above 55%.  A re-entry rule is a separate, evidence-gated decision.

Basis for the two percentages -- SPY daily closes, ``icyDenev/Intrader``, the same
(vendor, symbol) pair ``market_regime_provider.CHAIN_FALLBACK`` already pins.
**7,973 bars, 1993-01-29 -> 2024-09-27** (7,972 daily returns; the ticket's "7,972"
counts returns, this counts bars -- same series, different subject).  Reproduce with::

    WITH b AS (
      SELECT d.bar_date, d.close
      FROM research_price_daily d
      JOIN research_price_series s ON s.series_id = d.series_id
      WHERE s.vendor = 'icyDenev/Intrader' AND s.vendor_symbol = 'SPY'
    ), w AS (
      SELECT close / lag(close,  1) OVER (ORDER BY bar_date) - 1 AS r1,
             close / lag(close,  5) OVER (ORDER BY bar_date) - 1 AS r5,
             close / lag(close, 10) OVER (ORDER BY bar_date) - 1 AS r10,
             close / lag(close, 20) OVER (ORDER BY bar_date) - 1 AS r20
      FROM b
    )
    SELECT count(*) + 1 AS bars,
           round(min(r1) * 100, 2)                                        AS worst_1d,
           round((percentile_cont(0.001) WITHIN GROUP (ORDER BY r1))::numeric * 100, 2) AS p999,
           round(min(r5) * 100, 2)                                        AS worst_5d,
           round(min(r10) * 100, 2)                                       AS worst_10d,
           round(min(r20) * 100, 2)                                       AS worst_20d
    FROM w

which returns worst day **-10.94%** (2020-03-16), 99.9th-percentile day **-7.14%**,
worst 5-trading-day move **-19.79%**, worst 10-trading-day **-26.77%**, worst
20-trading-day **-31.39%**.

⚠ **"N-day" must be N days of MOVEMENT, and the window is easy to get wrong.** A
``ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`` high spans five BARS but only four days of
movement, and returns -17.80% rather than -19.79%.  ``lag(close, N)`` is used here
because it cannot express that error.

⚠⚠ **THE STOP IS OUTSIDE THE 1-DAY AND 5-DAY TAILS AND INSIDE THE 10-DAY ONE.**  This
is the fact most likely to be misread, so it is stated before the constants: at
**-26.77%** the worst 10-trading-day move in 31 years is BEYOND the -25% stop, and the
15- and 20-day figures (-28.70%, -31.39%) are further beyond it.  So the stop does not
merely risk firing in a sustained decline -- on the March 2020 sequence it **would have**
fired.  That is not a new objection: it is the accepted cost above, now with the number
attached.  A spike guard that survives every one-day and one-week move while yielding to
a month-long bear is the trade the operator made knowingly.

⚠ An earlier draft of this docstring reported the 10-day figure as -24.95% and concluded
the stop sat "outside every measured window".  Both were wrong -- -24.95% is the worst
NINE-day move, from a ``ROWS BETWEEN 9 PRECEDING AND CURRENT ROW`` window -- and they
were wrong in the reassuring direction, which is why the corrected numbers lead here.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Final

from app.services.strategy_core_preflight import CORE_MAX_QUOTE_AGE_SECONDS

#: Distance below the entry price at which the broker-side stop sits, in percent.
CORE_STOP_LOSS_PCT: Final[Decimal] = Decimal("25")

#: Distance above the entry price at which the broker-side target sits, in percent.
#: A spike guard, NOT a profit target -- see the module docstring.
CORE_TAKE_PROFIT_PCT: Final[Decimal] = Decimal("100")

#: The precision of a submitted rate.  Fixed BY CONSTRUCTION rather than from a
#: published rule: eToro quotes and echoes equity rates in cents, and submitting more
#: precision than the broker can store guarantees the echo never equals the intent.
CORE_EXIT_RATE_QUANTUM: Final[Decimal] = Decimal("0.01")

#: How far a broker-reported rate may sit from the derived one before it counts as a
#: real divergence.  One quantum, BY CONSTRUCTION: the broker cannot echo a value more
#: than one cent from what we sent without having changed it.  Without this the
#: comparison is exact equality, and a single cent of broker-side rounding would make
#: every cycle re-derive a gap that ``_prior_same_edit`` then refuses -- a position
#: reading "rejected" forever while actually being protected.
CORE_EXIT_RATE_TOLERANCE: Final[Decimal] = Decimal("0.01")

#: Freshness bound on the quote used to prove a stop sits below the market.  The core
#: arm has no ``strategy_execution_policies`` row to carry one (a mandate holding has no
#: deployment), so this arm needs a bound of its own -- and the core sleeve ALREADY HAS
#: ONE, derived and frozen with ``CORE_PREFLIGHT_POLICY_VERSION``.  Re-exported rather
#: than redefined: one core quote-freshness policy, named once.
#:
#: ⚠⚠ A first draft of this module defined its own 300 s bound "by construction, one
#: paper cycle".  That was wrong twice over, and Codex checkpoint 2 caught both.  300 s
#: is BELOW one producer period, which ``_freshness_bound`` documents as "a recurring
#: false refusal by construction" -- immediately before each refresh the newest possible
#: row is one full period old, so the repair would have refused
#: ``fixed_exit_quote_unsafe`` in the tail of every single cycle, leaving the position
#: unprotected exactly when it was asked to protect it.  And #3157 MEASURED that
#: ``core_candidate_quote_refresh`` loses fires (2 of 352 slots), which is what the
#: tolerated-miss term exists for; a by-construction bound invented here cannot know
#: that.  The general lesson: a "by construction" derivation is only honest once you
#: have grepped for an existing measured one.
CORE_EXIT_MAX_QUOTE_AGE_SECONDS: Final[int] = CORE_MAX_QUOTE_AGE_SECONDS

#: Bump when any constant above changes, so a stored operation's rates can be read
#: back against the policy that produced them.
CORE_EXIT_POLICY_VERSION: Final[str] = "core-exit-v1"


@dataclass(frozen=True)
class CoreExitLevels:
    """The two absolute rates for one core entry price."""

    stop_loss_rate: Decimal
    take_profit_rate: Decimal
    policy_version: str = CORE_EXIT_POLICY_VERSION


def core_exit_levels(entry_rate: Decimal) -> CoreExitLevels:
    """Derive the stop and target for a position opened at ``entry_rate``.

    ⚠ **Quantization rounds AWAY from the entry price, in both directions** -- the stop
    down, the target up.  One rule with one consequence: neither exit can fire before
    the declared move has actually happened.  Rounding the stop up would stop the
    sleeve out marginally inside -25%, and rounding the target down would take profit
    marginally inside +100%; both make the realised policy tighter than the declared
    one, which is the direction a reader would not expect from a "rounding" step.
    """
    if not entry_rate.is_finite() or entry_rate <= 0:
        raise ValueError("a core entry rate must be finite and positive")
    hundred = Decimal("100")
    stop = (entry_rate * (hundred - CORE_STOP_LOSS_PCT) / hundred).quantize(CORE_EXIT_RATE_QUANTUM, rounding=ROUND_DOWN)
    take = (entry_rate * (hundred + CORE_TAKE_PROFIT_PCT) / hundred).quantize(CORE_EXIT_RATE_QUANTUM, rounding=ROUND_UP)
    # Validates the CONSTANTS as much as the arithmetic, and deliberately not an
    # `assert`: `python -O` strips those, and a stop of 0 or a target below the stop is
    # a body eToro would either reject or -- worse -- accept.
    if stop <= 0 or take <= stop:
        raise ValueError("core exit levels must be positive with the target above the stop")
    return CoreExitLevels(stop_loss_rate=stop, take_profit_rate=take)


def core_exit_level_satisfied(*, observed: Decimal | None, desired: Decimal) -> bool:
    """Is a broker-reported rate close enough to the derived one to leave alone?

    ``None`` is never satisfied: an absent rate is an unprotected position, which is
    the whole subject of #3284.
    """
    if observed is None or not observed.is_finite():
        return False
    return abs(observed - desired) <= CORE_EXIT_RATE_TOLERANCE


__all__ = [
    "CORE_EXIT_MAX_QUOTE_AGE_SECONDS",
    "CORE_EXIT_POLICY_VERSION",
    "CORE_EXIT_RATE_QUANTUM",
    "CORE_EXIT_RATE_TOLERANCE",
    "CORE_STOP_LOSS_PCT",
    "CORE_TAKE_PROFIT_PCT",
    "CoreExitLevels",
    "core_exit_level_satisfied",
    "core_exit_levels",
]
