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

⚠⚠ **THE LEVELS ANSWER "HOW OFTEN WOULD THIS FIRE FROM A REAL ENTRY", NOT "HOW BIG CAN
A MOVE BE".**  Those are different questions and only the first is the one a stop answers.
An earlier draft of this module set -25% / +100% by placing the stop just outside the
worst observed MOVE (-10.94% in a day, -19.79% over five days).  The operator asked "are
the SL and TP realistic?" and the answer was no: swept over every bar as a hypothetical
entry, a **-25% stop fires on 35.7% of entries ever and 10.1% within a year**, which makes
it a BEAR-MARKET EXIT rather than a spike guard.  In 2022 it stops out near the October
low, and SPY then rose ~56% by 2024-09 (366 -> 572) with **no re-entry rule** to get back
in.  A -50% stop fires on **8.2%**.

Measured over SPY daily closes, ``icyDenev/Intrader`` -- the same (vendor, symbol) pair
``market_regime_provider.CHAIN_FALLBACK`` already pins -- **7,973 bars, 1993-01-29 ->
2024-09-27**.  Reproduce with::

    WITH b AS (
      SELECT d.bar_date, d.close, d.low
      FROM research_price_daily d
      JOIN research_price_series s ON s.series_id = d.series_id
      WHERE s.vendor = 'icyDenev/Intrader' AND s.vendor_symbol = 'SPY'
    ), f AS (
      SELECT close,
             min(close) OVER (ORDER BY bar_date
               ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS fmin,
             min(close) OVER (ORDER BY bar_date
               ROWS BETWEEN 1 FOLLOWING AND 252 FOLLOWING)       AS fmin_1y,
             min(low)   OVER (ORDER BY bar_date
               ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS fmin_low
      FROM b
    )
    SELECT count(*) AS entries,
           round(100.0 * count(*) FILTER (WHERE fmin     <= close * 0.75) / count(*), 1) AS pct25,
           round(100.0 * count(*) FILTER (WHERE fmin     <= close * 0.60) / count(*), 1) AS pct40,
           round(100.0 * count(*) FILTER (WHERE fmin     <= close * 0.50) / count(*), 1) AS pct50,
           round(100.0 * count(*) FILTER (WHERE fmin_1y  <= close * 0.75) / count(*), 1) AS pct25_1y,
           round(100.0 * count(*) FILTER (WHERE fmin_low <= close * 0.50) / count(*), 1) AS pct50_low
    FROM f

which returns 7,973 entries and fire rates **35.7%** (-25%), **23.0%** (-40%), **8.2%**
(-50%); -25% within one year **10.1%**; and -50% measured on the intraday LOW rather than
the close **9.3%**.

⚠ The low-based figure is the more faithful one -- a stop triggers on a touch, not on a
close -- and it is reported so the close-based headline is not mistaken for a bound.  It
does not change the ranking or the decision.

⚠⚠ **The accepted cost, stated so it is not rediscovered as a defect.**  This is a
deterministic buy-and-hold BETA sleeve with **no re-entry rule**, so ANY stop that fires
leaves it in cash indefinitely.  -50% keeps that to 8.2% of entries while still cutting a
genuine collapse.  Do NOT 'fix' it by tightening the stop -- that is the exact change the
operator already reversed once, and the 35.7% figure above is why.

⚠ **The target is a formality, not a control.**  +200% needs roughly eleven years at
historical drift, so its only realistic trigger is a BAD TICK -- where firing would sell
at a fake price.  That is the argument for placing a mandated target FAR away rather than
near: a near target converts the sleeve into a market-timing strategy, which the
price-only steer cut (#2837).  Profit-taking is the rebalance band's job; it trims above
55%.

"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Final

from app.services.strategy_core_preflight import CORE_MAX_QUOTE_AGE_SECONDS

#: Distance below the entry price at which the broker-side stop sits, in percent.
#: ⚠ -50%, NOT -25%.  See the module docstring: -25% fires on 35.7% of entries.
CORE_STOP_LOSS_PCT: Final[Decimal] = Decimal("50")

#: Distance above the entry price at which the broker-side target sits, in percent.
#: A spike guard, NOT a profit target -- see the module docstring.
CORE_TAKE_PROFIT_PCT: Final[Decimal] = Decimal("200")

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
CORE_EXIT_POLICY_VERSION: Final[str] = "core-exit-v2"


class CoreExitLevelsUnderivable(ValueError):
    """No valid stop/target pair exists for this anchor.

    A ``ValueError`` subclass rather than a new hierarchy, so every existing caller that
    catches ``ValueError`` keeps working.  It exists so the executor can catch THIS and
    not everything: a bare ``except ValueError`` around :func:`core_exit_levels` would
    silently map any future failure in it to the same operator-facing refusal, which is
    the reassuring direction (review bot, PR #3289).
    """


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
    sleeve out marginally inside -50%, and rounding the target down would take profit
    marginally inside +200%; both make the realised policy tighter than the declared
    one, which is the direction a reader would not expect from a "rounding" step.
    """
    if not entry_rate.is_finite() or entry_rate <= 0:
        raise CoreExitLevelsUnderivable("a core entry rate must be finite and positive")
    hundred = Decimal("100")
    stop = (entry_rate * (hundred - CORE_STOP_LOSS_PCT) / hundred).quantize(CORE_EXIT_RATE_QUANTUM, rounding=ROUND_DOWN)
    take = (entry_rate * (hundred + CORE_TAKE_PROFIT_PCT) / hundred).quantize(CORE_EXIT_RATE_QUANTUM, rounding=ROUND_UP)
    # Validates the CONSTANTS as much as the arithmetic, and deliberately not an
    # `assert`: `python -O` strips those, and a stop of 0 or a target below the stop is
    # a body eToro would either reject or -- worse -- accept.
    if stop <= 0 or take <= stop:
        raise CoreExitLevelsUnderivable("core exit levels must be positive with the target above the stop")
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
    "CoreExitLevelsUnderivable",
    "core_exit_level_satisfied",
    "core_exit_levels",
]
