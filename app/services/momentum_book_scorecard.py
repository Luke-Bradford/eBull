"""Momentum book (#3822) — the live scorecard: the rule's book against SPY and against random 30-name baskets.

The evidence (``docs/research/2026-10-15-3822-momentum-evidence.md``) says the book should beat SPY and a random
30-name basket from its own universe, and that a sustained lag stops it. This module measures exactly that, live:

* **Periods.** Formation k holds from the close of its signal day (the session before its target session, the
  close every entry signal is priced at) to the next formation's signal-day close, or to the latest close while it
  is the current formation. A formation whose period has not seen a close yet is ``pending``.
* **Book.** The equal-weight price return of the formation's names over the period. A name with no close on the
  start day (or in the 7 days before it) is left out; a name that stopped trading mid-period (an acquisition, a
  delisting) is valued at its last close in the period.
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

Conn = psycopg.Connection[Any]

CONTROL_DRAWS: Final = 200
#: A start close older than this is stale, and the name is left out of the period.
START_STALENESS: Final = timedelta(days=7)


def control_seed(draw: int, formation_month: date) -> str:
    return f"{mb.STRATEGY_ID}:{mb.STRATEGY_VERSION}:control:{draw}:{formation_month.isoformat()}"


@dataclass(frozen=True)
class Period:
    formation_month: date
    start: date
    end: date
    book: tuple[int, ...]
    universe: tuple[int, ...]


#: Close date -> instrument id -> (the bar's date, close): the latest close on or before that date.
Closes = Mapping[date, Mapping[int, tuple[date, float]]]


def basket_return(ids: Sequence[int], start: date, end: date, closes: Closes) -> tuple[float | None, int]:
    """Equal-weight price return over ``start``..``end`` and the number of names priced."""
    returns = []
    for iid in ids:
        first = closes.get(start, {}).get(iid)
        last = closes.get(end, {}).get(iid)
        if first is None or last is None or first[0] < start - START_STALENESS or last[0] < first[0]:
            continue
        returns.append(last[1] / first[1] - 1.0)
    return (sum(returns) / len(returns) if returns else None), len(returns)


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


def score(periods: Sequence[Period], closes: Closes, *, spy_id: int, draws: int = CONTROL_DRAWS) -> Scorecard:
    scored: list[PeriodScore] = []
    pending: list[date] = []
    book_path = spy_path = 1.0
    control_paths = [1.0] * draws
    chained = 0
    for p in periods:
        if p.end <= p.start:
            pending.append(p.formation_month)
            continue
        book, priced = basket_return(p.book, p.start, p.end, closes)
        spy, _ = basket_return((spy_id,), p.start, p.end, closes)
        population = sorted(p.universe)
        control: list[float] = []
        for d in range(draws):
            if len(population) < mb.BOOK_SIZE:
                break
            r, _ = basket_return(
                random.Random(control_seed(d, p.formation_month)).sample(population, mb.BOOK_SIZE),
                p.start,
                p.end,
                closes,
            )
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
                names_priced=priced,
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

# The latest close on or before each date, per instrument (one index probe per pair).
_CLOSES_SQL: Final = """
    SELECT d.day, i.id, c.price_date, c.close::float8
    FROM unnest(%(days)s::date[]) AS d(day)
    CROSS JOIN unnest(%(ids)s::bigint[]) AS i(id)
    CROSS JOIN LATERAL (
        SELECT price_date, close FROM price_daily
        WHERE instrument_id = i.id AND price_date <= d.day AND close IS NOT NULL AND close > 0
        ORDER BY price_date DESC LIMIT 1
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
        raise ValueError(f"expected one canonical {BENCHMARK_SYMBOL!r} instrument, found {len(spy)}")
    spy_id = int(spy[0][0])
    latest = conn.execute(
        "SELECT max(price_date) FROM price_daily WHERE instrument_id = %s AND close > 0", (spy_id,)
    ).fetchone()
    starts = [_signal_day(r[1]) for r in rows]
    latest_close = latest[0] if latest and latest[0] else None
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
        for day, iid, bar_date, close in conn.execute(_CLOSES_SQL, {"days": days, "ids": ids}).fetchall():
            closes.setdefault(day, {})[int(iid)] = (bar_date, float(close))
    return score(periods, closes, spy_id=spy_id)


__all__ = [
    "CONTROL_DRAWS",
    "Period",
    "PeriodScore",
    "Scorecard",
    "basket_return",
    "control_seed",
    "load_scorecard",
    "percentile_of",
    "score",
]
