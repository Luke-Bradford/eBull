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
* **Total return (#3844), beside price return, never instead of it.** Each name's dividends with an ex-date after
  its start bar and on or before its end bar are added to its end close, net of ``DIVIDEND_WITHHOLDING``, and not
  reinvested. Stocks read eToro's dividend calendar (``etoro_dividend_calendar``): a name's total is known only when
  its bars lie inside the captured days with no gap between them, so a period starting before the first capture or
  spanning a missing or unusable capture has none (``dividend_days_uncaptured`` says how many). The calendar's
  completeness against an independent source is not measured, so a name missing from it counts as paying nothing.
  SPY is not on that calendar: its ex-dates come from ``SPY_EX_DATES`` with no amounts, so SPY's total equals its
  price return when no ex-date falls inside the period, and is ``None`` when one does or the period runs past the
  schedule.
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
from app.services.etoro_dividend_calendar import load_calendar_dividends
from app.services.market_calendar import us_market_status
from app.services.market_regime_provider import BENCHMARK_SYMBOL
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.price_segments import load_unresolved_breaks

Conn = psycopg.Connection[Any]

CONTROL_DRAWS: Final = 200
#: US withholding on a UK account's dividends at the UK-US treaty rate (#3844).
DIVIDEND_WITHHOLDING: Final = 0.15
#: SPY's ex-dates in State Street's "Dividend Distribution Schedule, Calendar Year 2026" (Quarterly table, SPY
#: row), the potential excise distribution included:
#: https://www.ssga.com/library-content/products/fund-data/etfs/us/distribution/SPDR_Dividend_Distribution_Schedule.pdf
#: No amount source exists yet, so a period containing one of these scores no SPY total return.
SPY_EX_DATES: Final = (date(2026, 3, 20), date(2026, 6, 18), date(2026, 9, 18), date(2026, 12, 18), date(2026, 12, 31))
SPY_SCHEDULE_FROM: Final = date(2026, 1, 1)
SPY_SCHEDULE_THROUGH: Final = date(2026, 12, 31)
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
    #: ``momentum_book_universes.source``: 'formation', 'reconstructed' (rebuilt later from possibly healed closes,
    #: so the control may differ slightly from the set actually ranked), or ``None`` when not recorded yet.
    universe_source: str | None = None


#: Close date -> instrument id -> (the bar's date, close): the latest usable close on or before that date.
Closes = Mapping[date, Mapping[int, tuple[date, float]]]
#: Instrument id -> its unresolved series-break dates (``price_segments.load_unresolved_breaks``).
Breaks = Mapping[int, Sequence[date]]


@dataclass(frozen=True)
class Dividends:
    """Dividends known for ex-dates in ``known_from``..``known_through`` except on ``gaps``; elsewhere unknown."""

    #: Instrument id -> (ex-date, gross amount per share, or ``None`` when the date is known but the amount is not).
    events: Mapping[int, Sequence[tuple[date, float | None]]]
    known_from: date
    known_through: date
    #: Days inside the span whose ex-dates are not known (a missing or unusable calendar capture).
    gaps: frozenset[date] = frozenset()

    def net(self, iid: int, after: date, through: date) -> float | None:
        """Net dividends per share with an ex-date in (``after``, ``through``], or ``None`` when unknown."""
        if after + timedelta(days=1) < self.known_from or through > self.known_through:
            return None
        if any(after < gap <= through for gap in self.gaps):
            return None
        amounts = [a for ex, a in self.events.get(iid, ()) if after < ex <= through]
        if any(a is None for a in amounts):
            return None
        return sum(a for a in amounts if a is not None) * (1 - DIVIDEND_WITHHOLDING)


@dataclass(frozen=True)
class BasketReturn:
    value: float | None
    priced: int
    #: Names whose latest close is older than ``STALENESS`` at the period end.
    stale: int
    #: Equal-weight total return over the same names; ``None`` when any name's dividends are unknown.
    total: float | None = None


def basket_return(
    ids: Sequence[int],
    start: date,
    end: date,
    closes: Closes,
    breaks: Breaks,
    dividends: Dividends | None = None,
) -> BasketReturn:
    """Equal-weight price return over ``start``..``end``, and the total return when ``dividends`` is given."""
    returns = []
    totals: list[float | None] = []
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
        net = dividends.net(iid, first[0], last[0]) if dividends is not None else None
        totals.append(None if net is None else (last[1] + net) / first[1] - 1.0)
    total = None
    if totals and all(t is not None for t in totals):
        total = sum(t for t in totals if t is not None) / len(totals)
    return BasketReturn(sum(returns) / len(returns) if returns else None, len(returns), stale, total)


def percentile_of(value: float, draws: Sequence[float]) -> float:
    """Share of draws below ``value``, ties counted half."""
    below = sum(d < value for d in draws) + 0.5 * sum(d == value for d in draws)
    return below / len(draws)


@dataclass(frozen=True)
class PeriodScore:
    formation_month: date
    start: date
    end: date
    universe_source: str | None
    names_priced: int
    names_stale: int
    book_return: float | None
    spy_return: float | None
    control_median: float | None
    control_p05: float | None
    control_p95: float | None
    #: Share of control baskets that returned less than the book.
    book_percentile: float | None
    book_total_return: float | None = None
    spy_total_return: float | None = None
    control_total_median: float | None = None
    dividend_days_uncaptured: int | None = None


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
    #: Chained over the same periods as the price figures; ``None`` when any of them lacks a total.
    book_total_cumulative: float | None = None
    spy_total_cumulative: float | None = None


def _quantile(sorted_values: Sequence[float], q: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, max(0, math.ceil(q * len(sorted_values)) - 1))]


def score(
    periods: Sequence[Period],
    closes: Closes,
    breaks: Breaks,
    *,
    spy_id: int,
    draws: int = CONTROL_DRAWS,
    stock_dividends: Dividends | None = None,
    spy_dividends: Dividends | None = None,
    captured_days: Sequence[date] = (),
) -> Scorecard:
    if draws < 1:
        raise ValueError(f"draws must be >= 1, got {draws}")
    scored: list[PeriodScore] = []
    pending: list[date] = []
    book_path = spy_path = 1.0
    book_total_path: float | None = 1.0
    spy_total_path: float | None = 1.0
    control_paths = [1.0] * draws
    chained = 0
    captured = set(captured_days)
    for p in periods:
        if p.end <= p.start:
            pending.append(p.formation_month)
            continue
        basket = basket_return(p.book, p.start, p.end, closes, breaks, stock_dividends)
        book = basket.value
        spy_basket = basket_return((spy_id,), p.start, p.end, closes, breaks, spy_dividends)
        spy = spy_basket.value
        population = sorted(p.universe)
        control: list[float] = []
        control_totals: list[float | None] = []
        for d in range(draws):
            if len(population) < mb.BOOK_SIZE:
                break
            drawn = basket_return(
                random.Random(control_seed(d, p.formation_month)).sample(population, mb.BOOK_SIZE),
                p.start,
                p.end,
                closes,
                breaks,
                stock_dividends,
            )
            if drawn.value is None:
                break
            control.append(drawn.value)
            control_totals.append(drawn.total)
        complete = book is not None and spy is not None and len(control) == draws
        ordered = sorted(control)
        scored.append(
            PeriodScore(
                formation_month=p.formation_month,
                start=p.start,
                end=p.end,
                universe_source=p.universe_source,
                names_priced=basket.priced,
                names_stale=basket.stale,
                book_return=book,
                spy_return=spy,
                control_median=median(control) if complete else None,
                control_p05=_quantile(ordered, 0.05) if complete else None,
                control_p95=_quantile(ordered, 0.95) if complete else None,
                book_percentile=percentile_of(book, control) if complete and book is not None else None,
                book_total_return=basket.total,
                spy_total_return=spy_basket.total,
                control_total_median=(
                    median(t for t in control_totals if t is not None)
                    if complete and all(t is not None for t in control_totals)
                    else None
                ),
                dividend_days_uncaptured=(
                    sum(p.start + timedelta(days=k) not in captured for k in range(1, (p.end - p.start).days + 1))
                    if stock_dividends is not None
                    else None
                ),
            )
        )
        if complete and book is not None and spy is not None:
            chained += 1
            book_path *= 1 + book
            spy_path *= 1 + spy
            book_total_path = (
                None if book_total_path is None or basket.total is None else book_total_path * (1 + basket.total)
            )
            spy_total_path = (
                None if spy_total_path is None or spy_basket.total is None else spy_total_path * (1 + spy_basket.total)
            )
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
        book_total_cumulative=book_total_path - 1 if chained and book_total_path is not None else None,
        spy_total_cumulative=spy_total_path - 1 if chained and spy_total_path is not None else None,
    )


def _signal_day(target_session: date) -> date:
    d = target_session - timedelta(days=1)
    while us_market_status(d) == "closed":
        d -= timedelta(days=1)
    return d


_FORMATIONS_SQL: Final = """
    SELECT f.formation_month, f.target_session, f.book, u.universe, u.source
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
    for k, (formation_month, _, book, universe, universe_source) in enumerate(rows):
        end = starts[k + 1] if k + 1 < len(rows) else (latest_close or starts[k])
        periods.append(
            Period(
                formation_month=formation_month,
                start=starts[k],
                end=end,
                book=tuple(int(x["instrument_id"]) for x in book),
                # A formation whose universe is not recorded yet scores no control (the next job fire records it).
                universe=tuple(int(x) for x in universe or ()),
                universe_source=universe_source,
            )
        )
    ids = sorted({spy_id} | {i for p in periods for i in (*p.book, *p.universe)})
    days = sorted({d for p in periods for d in (p.start, p.end)})
    closes: dict[date, dict[int, tuple[date, float]]] = {}
    if ids and days:
        params = {"days": days, "ids": ids, "quarantine_version": QUARANTINE_RULE_SET_VERSION}
        for day, iid, bar_date, close in conn.execute(_CLOSES_SQL, params).fetchall():
            closes.setdefault(day, {})[int(iid)] = (bar_date, float(close))
    calendar = load_calendar_dividends(conn, since=min(starts)) if starts else None
    captured_days = calendar.captured_days if calendar is not None else ()
    stock_dividends = None
    if calendar is not None and captured_days:
        span = (captured_days[-1] - captured_days[0]).days + 1
        stock_dividends = Dividends(
            calendar.by_instrument,
            captured_days[0],
            captured_days[-1],
            frozenset(captured_days[0] + timedelta(days=k) for k in range(span)) - set(captured_days),
        )
    spy_dividends = Dividends({spy_id: [(ex, None) for ex in SPY_EX_DATES]}, SPY_SCHEDULE_FROM, SPY_SCHEDULE_THROUGH)
    return score(
        periods,
        closes,
        load_unresolved_breaks(conn, ids),
        spy_id=spy_id,
        stock_dividends=stock_dividends,
        spy_dividends=spy_dividends,
        captured_days=captured_days,
    )


__all__ = [
    "CONTROL_DRAWS",
    "DIVIDEND_WITHHOLDING",
    "SPY_EX_DATES",
    "BenchmarkInstrumentError",
    "Dividends",
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
