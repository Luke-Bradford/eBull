"""The pot's return against an investable UCITS S&P 500 fund, and the active hurdle (#3616 slice 1).

Spec: ``docs/specs/metrics/2026-10-12-3616-investable-comparator.md``.

The comparator is the market-price return of ``CSPX.L`` (iShares Core S&P 500
UCITS ETF, USD Accumulating, IE00B5BMR087) as eToro quotes it, over the pot's
own time-weighted window.  It is NOT the S&P 500 total-return index that
``BENCHMARK_REFUSALS`` refuses; that refusal stands unchanged.

The hurdle is a counterfactual under the current mandate: core earns the
comparator, cash earns 0, the active sleeves sit at their full cap, and the pot
is bought and held over the window.  Over one buy-and-hold period the pot's
return is exactly the weighted sum of the sleeves', so the active sleeves match
the comparator at ``r_cmp · (1 − w_core) / w_active``, where ``w_core`` is the
core's share of the POT (``hurdle_weights``).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.services.strategy_control_plane import PaperPool, load_paper_pool
from app.services.strategy_core_mandate import CoreMandate, load_core_mandate
from app.services.strategy_wealth import StrategyWealthPoint, TimeWeightedReturns

#: Resolved by symbol at runtime, never by a fixed instrument id.  Bound to
#: ``scheduler.BENCHMARK_SYMBOLS`` by a test so its closes stay fresh (#1818).
COMPARATOR_SYMBOL: Final = "CSPX.L"
#: The unhedged USD class the CSPX ticker identifies, on eToro's LSE exchange id.
COMPARATOR_CURRENCY: Final = "USD"
COMPARATOR_EXCHANGE: Final = "7"
COMPARATOR_LABEL: Final = "iShares Core S&P 500 UCITS ETF (USD, Acc)"
COMPARATOR_BASIS: Final = "accumulating_ucits_market_price_return_usd"
#: A CHOSEN tolerance, not a calendar rule: the longest LSE closure (Thursday
#: close over Good Friday and Easter Monday) puts a date at most 4 days after
#: its latest close.
COMPARATOR_MAX_CLOSE_AGE: Final = timedelta(days=4)

ComparatorUnavailableReason = Literal[
    "comparator_instrument_missing",
    "comparator_instrument_mismatch",
    "pot_return_unavailable",
    "comparator_close_missing",
    "comparator_stale",
]
HurdleUnavailableReason = Literal[
    "mandate_unconfigured",
    "mandate_weights_invalid",
    "no_active_budget",
    "comparator_return_unavailable",
    "hurdle_infeasible",
]

_ONE: Final = Decimal("1")
_HUNDRED: Final = Decimal("100")


@dataclass(frozen=True)
class ComparatorInstrument:
    instrument_id: int
    currency: str | None
    exchange: str | None


@dataclass(frozen=True)
class Close:
    price_date: date
    close: Decimal


@dataclass(frozen=True)
class PotWindow:
    """S, E and the pot's cumulative return at E."""

    start: date
    end: date
    pot_return: Decimal


@dataclass(frozen=True)
class Hurdle:
    #: ``core_target_pct`` / 100, a share of the core allocator's sleeve.
    core_target_weight: Decimal | None = None
    #: The core's share of the POT: ``core_target_weight · (1 − active_weight)``.
    core_weight: Decimal | None = None
    active_weight: Decimal | None = None
    core_mandate_event_id: int | None = None
    pool_event_id: int | None = None
    multiplier: Decimal | None = None
    hurdle_return: Decimal | None = None
    required_active_excess: Decimal | None = None
    unavailable_reason: HurdleUnavailableReason | None = None


@dataclass(frozen=True)
class Comparator:
    hurdle: Hurdle
    symbol: str = COMPARATOR_SYMBOL
    instrument_id: int | None = None
    label: str = COMPARATOR_LABEL
    basis: str = COMPARATOR_BASIS
    window_start: date | None = None
    window_end: date | None = None
    start_close_date: date | None = None
    end_close_date: date | None = None
    comparator_return: Decimal | None = None
    unavailable_reason: ComparatorUnavailableReason | None = None
    engine_excess: Decimal | None = None


def pot_window(history: Sequence[StrategyWealthPoint], returns: TimeWeightedReturns) -> PotWindow | None:
    """The pot's TWR window, or None when the TWR summary is unavailable.

    E is the last complete point; S is the ``period_start`` of the first linked
    point.  S is not ``return_since``: from an unfunded base the first
    sub-period runs from the base date with the flow at its start (TWR spec,
    Rule 3), and the comparator has to cover it too.  After a chain break the
    summary is unavailable, so an earlier comparison is never revived.
    """
    if not returns.total_return_available:
        return None
    start = next((r.period_start for r in returns.points if r.cumulative_return is not None), None)
    last = next(
        (
            (point, twr)
            for point, twr in zip(reversed(history), reversed(returns.points), strict=True)
            if point.complete and point.pot_value is not None
        ),
        None,
    )
    if start is None or last is None or last[1].cumulative_return is None:
        return None
    return PotWindow(start=start, end=last[0].date, pot_return=last[1].cumulative_return)


def compare(
    instrument: ComparatorInstrument | None,
    window: PotWindow | None,
    start_close: Close | None,
    end_close: Close | None,
    hurdle_weights: Hurdle,
) -> Comparator:
    """Assemble the comparator; the first reason that applies wins, in the contract's order."""
    instrument_id = None if instrument is None else instrument.instrument_id
    dates: dict[str, Any] = {
        "window_start": None if window is None else window.start,
        "window_end": None if window is None else window.end,
        "start_close_date": None if start_close is None else start_close.price_date,
        "end_close_date": None if end_close is None else end_close.price_date,
    }
    reason: ComparatorUnavailableReason | None
    if instrument is None:
        reason = "comparator_instrument_missing"
    elif instrument.currency != COMPARATOR_CURRENCY or instrument.exchange != COMPARATOR_EXCHANGE:
        reason = "comparator_instrument_mismatch"
    elif window is None:
        reason = "pot_return_unavailable"
    elif start_close is None or end_close is None:
        reason = "comparator_close_missing"
    elif (
        window.start - start_close.price_date > COMPARATOR_MAX_CLOSE_AGE
        or window.end - end_close.price_date > COMPARATOR_MAX_CLOSE_AGE
        # Both endpoints on one row is no window at all.  Distinct rows with
        # equal closes are a genuine 0 return.
        or start_close.price_date == end_close.price_date
    ):
        reason = "comparator_stale"
    else:
        reason = None

    if reason is not None or window is None or start_close is None or end_close is None:
        return Comparator(
            hurdle=hurdle(hurdle_weights, None),
            instrument_id=instrument_id,
            unavailable_reason=reason,
            **dates,
        )
    comparator_return = end_close.close / start_close.close - _ONE
    return Comparator(
        hurdle=hurdle(hurdle_weights, comparator_return),
        instrument_id=instrument_id,
        comparator_return=comparator_return,
        engine_excess=window.pot_return - comparator_return,
        **dates,
    )


def hurdle_weights(core: CoreMandate | None, pool: PaperPool) -> Hurdle:
    """The current mandate's weights and the multiplier, before any comparator return.

    ``w_active`` is the cap on non-core committed capital as a share of
    ``pool_base`` (``strategy_paper_executor``), so it is a capital weight at
    full use, not a risk measure.

    ⚠ ``core_target_pct`` is NOT a share of the pot.  The allocator measures it
    over ``core_market_value + cash_balance`` (``strategy_core_allocator``), and
    that cash is the pot's sandbox headroom (``observe_core_sleeve``), which is
    net of everything committed, active included.  With the active sleeves at
    their cap the core's sleeve is ``1 − w_active`` of the pot, so its pot weight
    is ``core_target · (1 − w_active)``: 35%, not 50%, at the live 50/30.
    """
    mandate = pool.mandate
    target = None if core is None else core.core_target_pct / _HUNDRED
    w_active = None if mandate.active_risk_budget_pct is None else mandate.active_risk_budget_pct / _HUNDRED
    weights = Hurdle(
        core_target_weight=target,
        core_weight=None if target is None or w_active is None else target * (_ONE - w_active),
        active_weight=w_active,
        core_mandate_event_id=None if core is None else core.event_id,
        pool_event_id=pool.event_id,
    )
    w_core = weights.core_weight
    if (
        core is None
        or not core.enabled
        or pool.event_id is None
        or not mandate.configured
        or target is None
        or w_core is None
        or w_active is None
        or mandate.cash_reserve_pct is None
    ):
        return _with(weights, unavailable_reason="mandate_unconfigured")
    committable = _ONE - mandate.cash_reserve_pct / _HUNDRED
    if any(not 0 <= weight <= 1 for weight in (target, w_active)) or w_core + w_active > committable:
        return _with(weights, unavailable_reason="mandate_weights_invalid")
    if w_active == 0:
        return _with(weights, unavailable_reason="no_active_budget")
    return _with(weights, multiplier=(_ONE - w_core) / w_active)


def hurdle(weights: Hurdle, comparator_return: Decimal | None) -> Hurdle:
    """Apply the comparator return to the mandate's multiplier."""
    if weights.unavailable_reason is not None or weights.multiplier is None:
        return weights
    if comparator_return is None:
        return _with(weights, unavailable_reason="comparator_return_unavailable")
    hurdle_return = comparator_return * weights.multiplier
    if hurdle_return < -_ONE:
        # No active return below a total loss exists without leverage: the pot
        # would be ahead of the comparator whatever the active sleeves did.
        return _with(weights, unavailable_reason="hurdle_infeasible")
    return _with(
        weights,
        hurdle_return=hurdle_return,
        required_active_excess=hurdle_return - comparator_return,
    )


def _with(base: Hurdle, **changes: Any) -> Hurdle:
    return replace(base, **changes)


#: A row matching the USD/LSE class wins over any other row sharing the symbol,
#: so a mismatch is reported only when no matching row exists.
_INSTRUMENT_SQL = """
    SELECT instrument_id, currency, exchange
    FROM instruments
    WHERE symbol = %(symbol)s
    ORDER BY (currency = %(currency)s AND exchange = %(exchange)s) IS TRUE DESC,
             is_primary_listing DESC, instrument_id
    LIMIT 1
"""

#: ``price_daily.close`` is NUMERIC(18,6), which cannot hold ±Infinity but can
#: hold NaN, and in Postgres ``'NaN' > 0`` is TRUE, so NaN is excluded by name.
#: No row dated after ``on_or_before`` is read.
LATEST_VALID_CLOSE_SQL = """
    SELECT price_date, close
    FROM price_daily
    WHERE instrument_id = %(instrument_id)s
      AND price_date <= %(on_or_before)s
      AND close > 0
      AND close <> 'NaN'::numeric
    ORDER BY price_date DESC
    LIMIT 1
"""


def latest_valid_close(conn: psycopg.Connection[Any], instrument_id: int, on_or_before: date) -> Close | None:
    """The latest finite, positive close dated on or before ``on_or_before``."""
    row = conn.execute(
        LATEST_VALID_CLOSE_SQL, {"instrument_id": instrument_id, "on_or_before": on_or_before}
    ).fetchone()
    return None if row is None else Close(price_date=row[0], close=Decimal(str(row[1])))


def load_comparator(
    conn: psycopg.Connection[Any], history: Sequence[StrategyWealthPoint], returns: TimeWeightedReturns
) -> Comparator:
    row = conn.execute(
        _INSTRUMENT_SQL,
        {"symbol": COMPARATOR_SYMBOL, "currency": COMPARATOR_CURRENCY, "exchange": COMPARATOR_EXCHANGE},
    ).fetchone()
    instrument = None if row is None else ComparatorInstrument(int(row[0]), row[1], row[2])
    window = pot_window(history, returns)
    start_close = end_close = None
    if instrument is not None and window is not None:
        start_close = latest_valid_close(conn, instrument.instrument_id, window.start)
        end_close = latest_valid_close(conn, instrument.instrument_id, window.end)
    weights = hurdle_weights(load_core_mandate(conn), load_paper_pool(conn))
    return compare(instrument, window, start_close, end_close, weights)


__all__ = [
    "COMPARATOR_SYMBOL",
    "Close",
    "Comparator",
    "ComparatorInstrument",
    "ComparatorUnavailableReason",
    "Hurdle",
    "HurdleUnavailableReason",
    "PotWindow",
    "compare",
    "hurdle",
    "hurdle_weights",
    "latest_valid_close",
    "load_comparator",
    "pot_window",
]
