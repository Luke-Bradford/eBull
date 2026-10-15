"""Momentum book (#3822) — the live scorecard: the rule's book against SPY and against random 30-name baskets.

The evidence (``docs/research/2026-10-15-3822-momentum-evidence.md``) says the book should beat SPY and a random
30-name basket from its own universe, and that a sustained lag stops it. This module measures exactly that, live:

* **Periods.** Formation k holds from the close of its signal day (the session before its target session, the
  close every entry signal is priced at) to the next formation's signal-day close, or to the latest close while it
  is the current formation. A formation whose period has not seen a close yet is ``pending``.
* **Book.** The equal-weight price return of the formation's names over the period. Closes are read under the
  price-verdict contract (``docs/settled-decisions.md``, 2026-09-15): only bars inside an evaluated quarantine range
  and not marked return-unusable (clause 1), and no return across an unresolved series break (clause 2). A name is
  left out when its start close is missing or older than ``STALENESS``, or its return crosses a break; a name whose
  latest close is older than ``STALENESS`` at the period end is also left out and counted as ``names_stale`` (a
  stale mark is not scored as current; a name acquired mid-period lands here too, and the count says so).
* **SPY.** SPY's price return over the same period. eToro closes exclude dividends for both sides.
* **Control.** ``CONTROL_DRAWS`` baskets of ``BOOK_SIZE`` names, each drawn uniformly from the formation's recorded
  universe (``momentum_book_universes``) with ``random.Random(control_seed(draw, formation_month))``, valued like
  the book. Each draw is chained across periods, so the cumulative control is a distribution of whole paths.

This scores the RULE, gross of costs on every side: the control and SPY are not charged either. Executed fills,
spreads and refusals are the positions view's job, not this one. Descriptive only; nothing here enters a decision.
"""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import median
from typing import Any, Final

import psycopg

from app.services import momentum_book as mb
from app.services.market_calendar import us_market_status
from app.services.market_regime_provider import BENCHMARK_SYMBOL
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.price_segments import load_unresolved_breaks

Conn = psycopg.Connection[Any]

CONTROL_DRAWS: Final = 200
#: A start or end close older than this, against the period's own start or end date, is stale.
STALENESS: Final = timedelta(days=7)


class BenchmarkInstrumentError(LookupError):
    """SPY does not resolve to exactly one canonical instrument, so nothing can be scored against it."""


def control_seed(draw: int, formation_month: date) -> str:
    return f"{mb.STRATEGY_ID}:{mb.STRATEGY_VERSION}:control:{draw}:{formation_month.isoformat()}"


@dataclass(frozen=True)
class Period:
    formation_month: date
    start: date
    end: date
    book: tuple[int, ...]
    universe: tuple[int, ...]


#: Close date -> instrument id -> (the bar's date, close): the latest usable close on or before that date.
Closes = Mapping[date, Mapping[int, tuple[date, float]]]
#: Instrument id -> its unresolved series-break dates (``price_segments.load_unresolved_breaks``).
Breaks = Mapping[int, Sequence[date]]


@dataclass(frozen=True)
class BasketReturn:
    value: float | None
    priced: int
    #: Names whose latest close is older than ``STALENESS`` at the period end.
    stale: int


def basket_return(ids: Sequence[int], start: date, end: date, closes: Closes, breaks: Breaks) -> BasketReturn:
    """Equal-weight price return over ``start``..``end``."""
    returns = []
    stale = 0
    for iid in ids:
        first = closes.get(start, {}).get(iid)
        if first is None or first[0] < start - STALENESS:
            continue
        last = closes.get(end, {}).get(iid)
        if last is None or last[0] < end - STALENESS:
            stale += 1
            continue
        # A break dated d starts a new segment at bar d, so a return from first to last crosses it when
        # first < d <= last.
        if last[0] < first[0] or any(first[0] < d <= last[0] for d in breaks.get(iid, ())):
            continue
        returns.append(last[1] / first[1] - 1.0)
    return BasketReturn(sum(returns) / len(returns) if returns else None, len(returns), stale)


def percentile_of(value: float, draws: Sequence[float]) -> float:
    """Share of draws below ``value``, ties counted half."""
    below = sum(d < value for d in draws) + 0.5 * sum(d == value for d in draws)
    return below / len(draws)


@dataclass(frozen=True)
class PeriodScore:
    formation_month: date
    start: date
    end: date
    names_priced: int
    names_stale: int
    book_return: float | None
    spy_return: float | None
    control_median: float | None
    control_p05: float | None
    control_p95: float | None
    #: Share of control baskets that returned less than the book.
    book_percentile: float | None


@dataclass(frozen=True)
class Scorecard:
    periods: tuple[PeriodScore, ...]
    #: Chained over the scored periods; ``None`` until one period has a book, SPY and control return.
    book_cumulative: float | None
    spy_cumulative: float | None
    control_cumulative_median: float | None
    book_cumulative_percentile: float | None
    control_draws: int
    #: Formations with no close yet after their signal day.
    pending: tuple[date, ...]


def _quantile(sorted_values: Sequence[float], q: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, max(0, math.ceil(q * len(sorted_values)) - 1))]


def score(
    periods: Sequence[Period], closes: Closes, breaks: Breaks, *, spy_id: int, draws: int = CONTROL_DRAWS
) -> Scorecard:
    if draws < 1:
        raise ValueError(f"draws must be >= 1, got {draws}")
    scored: list[PeriodScore] = []
    pending: list[date] = []
    book_path = spy_path = 1.0
    control_paths = [1.0] * draws
    chained = 0
    for p in periods:
        if p.end <= p.start:
            pending.append(p.formation_month)
            continue
        basket = basket_return(p.book, p.start, p.end, closes, breaks)
        book = basket.value
        spy = basket_return((spy_id,), p.start, p.end, closes, breaks).value
        population = sorted(p.universe)
        control: list[float] = []
        for d in range(draws):
            if len(population) < mb.BOOK_SIZE:
                break
            r = basket_return(
                random.Random(control_seed(d, p.formation_month)).sample(population, mb.BOOK_SIZE),
                p.start,
                p.end,
                closes,
                breaks,
            ).value
            if r is None:
                break
            control.append(r)
        complete = book is not None and spy is not None and len(control) == draws
        ordered = sorted(control)
        scored.append(
            PeriodScore(
                formation_month=p.formation_month,
                start=p.start,
                end=p.end,
                names_priced=basket.priced,
                names_stale=basket.stale,
                book_return=book,
                spy_return=spy,
                control_median=median(control) if complete else None,
                control_p05=_quantile(ordered, 0.05) if complete else None,
                control_p95=_quantile(ordered, 0.95) if complete else None,
                book_percentile=percentile_of(book, control) if complete and book is not None else None,
            )
        )
        if complete and book is not None and spy is not None:
            chained += 1
            book_path *= 1 + book
            spy_path *= 1 + spy
            control_paths = [path * (1 + r) for path, r in zip(control_paths, control, strict=True)]
    cumulative = [c - 1 for c in control_paths]
    return Scorecard(
        periods=tuple(scored),
        book_cumulative=book_path - 1 if chained else None,
        spy_cumulative=spy_path - 1 if chained else None,
        control_cumulative_median=median(cumulative) if chained else None,
        book_cumulative_percentile=percentile_of(book_path - 1, cumulative) if chained else None,
        control_draws=draws,
        pending=tuple(pending),
    )


def _signal_day(target_session: date) -> date:
    d = target_session - timedelta(days=1)
    while us_market_status(d) == "closed":
        d -= timedelta(days=1)
    return d


_FORMATIONS_SQL: Final = """
    SELECT f.formation_month, f.target_session, f.book, u.universe
    FROM momentum_book_formations f
    LEFT JOIN momentum_book_universes u USING (formation_id)
    WHERE f.strategy_version = %s
    ORDER BY f.formation_month
"""

# The latest usable close on or before each date, per instrument. Fail-closed like `price_masked_bars._LOAD_SQL`
# (clause 1): only bars inside an evaluated quarantine range, never one marked return-unusable.
_CLOSES_SQL: Final = """
    SELECT d.day, i.id, c.price_date, c.close::float8
    FROM unnest(%(days)s::date[]) AS d(day)
    CROSS JOIN unnest(%(ids)s::bigint[]) AS i(id)
    CROSS JOIN LATERAL (
        SELECT p.price_date, p.close FROM price_daily p
        JOIN price_quarantine_coverage cov
          ON cov.instrument_id = p.instrument_id
         AND cov.rule_set_version = %(quarantine_version)s
         AND p.price_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN price_bar_quarantine q
          ON q.instrument_id = p.instrument_id AND q.price_date = p.price_date
         AND q.rule_set_version = %(quarantine_version)s
        WHERE p.instrument_id = i.id AND p.price_date <= d.day AND p.close IS NOT NULL AND p.close > 0
          AND COALESCE(q.return_usable, TRUE)
        ORDER BY p.price_date DESC LIMIT 1
    ) c
"""


def load_scorecard(conn: Conn) -> Scorecard:
    """Read the formations, their universes and the closes the periods need, and score them."""
    rows = conn.execute(_FORMATIONS_SQL, (mb.STRATEGY_VERSION,)).fetchall()
    # The canonical row only: SPY.RTH is the same fund and points at it.
    spy = conn.execute(
        "SELECT instrument_id FROM instruments WHERE symbol = %s AND canonical_instrument_id IS NULL",
        (BENCHMARK_SYMBOL,),
    ).fetchall()
    if len(spy) != 1:
        raise BenchmarkInstrumentError(f"expected one canonical {BENCHMARK_SYMBOL!r} instrument, found {len(spy)}")
    spy_id = int(spy[0][0])
    # The current period ends at SPY's latest usable close, read under the same mask as every other close.
    latest = conn.execute(
        _CLOSES_SQL, {"days": [date.max], "ids": [spy_id], "quarantine_version": QUARANTINE_RULE_SET_VERSION}
    ).fetchone()
    starts = [_signal_day(r[1]) for r in rows]
    latest_close = latest[2] if latest else None
    periods = []
    for k, (formation_month, _, book, universe) in enumerate(rows):
        end = starts[k + 1] if k + 1 < len(rows) else (latest_close or starts[k])
        periods.append(
            Period(
                formation_month=formation_month,
                start=starts[k],
                end=end,
                book=tuple(int(x["instrument_id"]) for x in book),
                # A formation whose universe is not recorded yet scores no control (the next job fire records it).
                universe=tuple(int(x) for x in universe or ()),
            )
        )
    ids = sorted({spy_id} | {i for p in periods for i in (*p.book, *p.universe)})
    days = sorted({d for p in periods for d in (p.start, p.end)})
    closes: dict[date, dict[int, tuple[date, float]]] = {}
    if ids and days:
        params = {"days": days, "ids": ids, "quarantine_version": QUARANTINE_RULE_SET_VERSION}
        for day, iid, bar_date, close in conn.execute(_CLOSES_SQL, params).fetchall():
            closes.setdefault(day, {})[int(iid)] = (bar_date, float(close))
    return score(periods, closes, load_unresolved_breaks(conn, ids), spy_id=spy_id)


__all__ = [
    "CONTROL_DRAWS",
    "BenchmarkInstrumentError",
    "Period",
    "PeriodScore",
    "Scorecard",
    "BasketReturn",
    "basket_return",
    "control_seed",
    "load_scorecard",
    "percentile_of",
    "score",
]
