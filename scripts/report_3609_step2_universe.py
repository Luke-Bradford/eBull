"""#3609 step 2's universe and cutoff diagnostics (slice 3c-v(e)): printed, never gated, except the input refusals.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Diagnostics", "Universe" (PR #3666), and premise 3
(``scripts/measure_3609_step2_universe.py`` on the spec branch, which measured stage A's 80 formations). Pure
functions over the loader's :class:`~scripts.report_3609_step2.PanelMonth`, each formation's book universe, the
book's decisions and its base-cost trades.

* **Premise 3's table, on every formation:** admitted names; the ME of the 1,000th name; the top 1,000's family
  counts (a family is present when any member has a value) and SIC 6221 count; and, against JKP's NYSE-median
  cutoff (``nyse_p50``, USD millions, normalised to USD): names above it, the overlap with the top 1,000, and the top
  1,000's share of the above-cutoff ME. "Above" is ME > cutoff; "at or below" is its complement.
* **The book at or below the cutoff,** per formation: its post-trade holdings and weight there (weights as
  ``Decision.share``, so a formation holding nothing is all cash and weighs zero).
* **Notional classes,** per arm at base cost, by precedence: no ME at s(M) (the name left the admitted population);
  cutoff unavailable; above; at or below. The 2024-08 final liquidation is its own class. The classes partition the
  trades, so they reconcile to the book's total order notional.
* **Unavailable:** a formation with no published cutoff prints ``None`` in every cutoff-dependent field, and the
  coverage count says how many formations had one. A formation with no admitted name above its cutoff has no share.
* **Refusals:** a repeated, non-finite, non-positive or wrongly-united published cutoff refuses ``CUTOFF_INVALID``
  (every ``nyse_p50`` row in the snapshot is checked, published months off the grid included). An ME sum in the
  share that is not finite and positive refuses ``ME_INVALID``.

**Windows** are keyed by formation, so stage A is premise 3's grid: formations before 2021-05 (stage A's 80). Stage
B is formations 2021-05..2024-07 with the 2024-08 final liquidation, and pooled is all of them. A trade's window is
its ``Trade.month``, which is its formation's month (the final liquidation's is 2024-08). This differs from
``report_3609_step2_operations``'s return-month windows, where stage A's last month also carries the boundary
formation's trades.
"""

from __future__ import annotations

import gzip
import io
import json
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Any, Final

from app.services.factor_book import FAMILIES, BookRefusal
from app.services.factor_book_path import Decision, Month, Trade, TradeCategory, month_of
from app.services.factor_book_series import ARMS, SeriesRun
from app.services.strategy_result import AmbiguityArm
from scripts.build_3609_factor_panel import Frozen
from scripts.report_3609_step2 import PanelMonth
from scripts.report_3609_step2_verdict import BASE, STAGE_B_FORMATIONS

#: The JKP cutoff snapshot the report consumes, relative to the artefact.
NYSE_CUTOFFS: Final = f"inputs/{Frozen.snapshot('jkp_nyse_cutoffs')}"
CUTOFF_SERIES: Final = "nyse_p50"
#: JKP Documentation.pdf, "Market Equity": "quoted in million USD" (``sql/472_reference_jkp_cutoff_units.sql``).
CUTOFF_UNIT: Final = "usd_millions"
USD_PER_UNIT: Final = 1e6
#: Commodity contracts, the SIC commodity pools file under (premise 3).
COMMODITY_SIC: Final = 6221


class NotionalClass(StrEnum):
    """§"Diagnostics", Universe, notional classes, in precedence order."""

    NO_ME = "no_me_at_s_m"
    CUTOFF_UNAVAILABLE = "cutoff_unavailable"
    ABOVE = "above"
    AT_OR_BELOW = "at_or_below"
    FINAL_LIQUIDATION = "final_liquidation"


# --------------------------------------------------------------------------- cutoffs


def _lines(payload: bytes) -> Iterator[Any]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as handle:
        for line in handle:
            yield json.loads(line)


def read_cutoffs(payload: bytes) -> dict[date, float]:
    """``month end -> NYSE median ME in USD`` from the frozen snapshot's ``nyse_p50`` rows."""
    out: dict[date, float] = {}
    for key, day, value, unit in _lines(payload):
        if key != CUTOFF_SERIES:
            continue
        try:
            when, number = date.fromisoformat(day), float(value) * USD_PER_UNIT
        except (TypeError, ValueError) as exc:
            raise BookRefusal("CUTOFF_INVALID", f"{key} row {(day, value)!r} is not a date and a number") from exc
        if unit != CUTOFF_UNIT:
            raise BookRefusal("CUTOFF_INVALID", f"{key} {day} unit {unit!r}, expected {CUTOFF_UNIT}")
        if when in out:
            raise BookRefusal("CUTOFF_INVALID", f"{key} {day} is repeated")
        if not (math.isfinite(number) and number > 0):
            raise BookRefusal("CUTOFF_INVALID", f"{key} {day} is {value!r}: not finite and positive")
        out[when] = number
    return out


# --------------------------------------------------------------------------- per formation


@dataclass(frozen=True)
class UniverseMonth:
    """Premise 3's row for one formation, plus the book at or below the cutoff. ``None`` is unavailable."""

    formation: date
    admitted: int
    me_rank_last: float
    top_fam3: int
    top_fam2: int
    top_sic6221: int
    cutoff: float | None
    above: int | None
    shared: int | None
    top_at_or_below: int | None
    above_outside_top: int | None
    above_me_share_in_top: float | None
    book_holdings: int
    book_at_or_below: int | None
    book_weight_at_or_below: float | None


def _me_sum(values: Sequence[float], what: str, *, empty_ok: bool = False) -> float:
    """A finite ME sum; positive unless ``empty_ok``, where no names sum to a legitimate 0."""
    try:
        total = math.fsum(values)
    except OverflowError:  # fsum raises on an intermediate overflow rather than returning inf
        total = math.inf
    except ValueError:  # and on inf + -inf rather than returning nan
        total = math.nan
    if not (math.isfinite(total) and (total > 0 or (empty_ok and not values))):
        raise BookRefusal("ME_INVALID", f"{what} sums to {total!r}, not finite and positive")
    return total


def universe_month(
    month: PanelMonth, universe: Sequence[int], decision: Decision, cutoff: float | None
) -> UniverseMonth:
    """One formation's row. ``universe`` is the book universe (``factor_book.universe``), ``decision`` the book's
    decision at this formation, ``cutoff`` its published NYSE median in USD or ``None``."""
    if decision.formation != month.formation:
        raise ValueError(f"decision {decision.formation} is not formation {month.formation}")
    me = {name: panel.me for name, panel in month.admitted.items()}
    top = set(universe)
    if not top:
        raise ValueError(f"{month.formation}: the universe is empty")
    if not top <= me.keys() or not set(decision.targets) <= top:
        raise ValueError(f"{month.formation}: the universe or the book holds a name outside the admitted population")
    families = {
        name: sum(any(c in month.admitted[name].signed for c in members) for members in FAMILIES.values())
        for name in top
    }
    common = {
        "formation": month.formation,
        "admitted": len(me),
        "me_rank_last": min(me[name] for name in top),
        "top_fam3": sum(1 for n in families.values() if n == len(FAMILIES)),
        "top_fam2": sum(1 for n in families.values() if n >= 2),
        "top_sic6221": sum(1 for name in top if month.admitted[name].sic == COMMODITY_SIC),
        "book_holdings": len(decision.targets),
    }
    if cutoff is None:
        return UniverseMonth(**common, cutoff=None, above=None, shared=None, top_at_or_below=None,
                             above_outside_top=None, above_me_share_in_top=None, book_at_or_below=None,
                             book_weight_at_or_below=None)  # fmt: skip
    above = {name for name, value in me.items() if value > cutoff}
    share = None
    if above:
        # Under the book's rule the top 1,000 holds the largest name, so this overlap is never empty; for any other
        # universe an empty overlap is a share of 0, not invalid input.
        held = _me_sum([me[n] for n in above & top], f"{month.formation} above-cutoff ME in the top", empty_ok=True)
        share = held / _me_sum([me[n] for n in above], f"{month.formation} above-cutoff ME")
    below = [name for name in decision.targets if me[name] <= cutoff]
    return UniverseMonth(
        **common,
        cutoff=cutoff,
        above=len(above),
        shared=len(above & top),
        top_at_or_below=len(top - above),
        above_outside_top=len(above - top),
        above_me_share_in_top=share,
        book_at_or_below=len(below),
        book_weight_at_or_below=math.fsum(decision.share(1.0, name) for name in below),
    )


# --------------------------------------------------------------------------- notional classes


def notional_class(
    trade: Trade, me: Mapping[Month, Mapping[int, float]], cutoffs: Mapping[Month, float]
) -> NotionalClass:
    """``me`` and ``cutoffs`` are keyed by formation month: the admitted names' ME at s(M), and the published cutoff."""
    if trade.category is TradeCategory.FINAL_LIQUIDATION:
        return NotionalClass.FINAL_LIQUIDATION
    value = me[trade.month].get(trade.name)
    if value is None:
        return NotionalClass.NO_ME
    cutoff = cutoffs.get(trade.month)
    if cutoff is None:
        return NotionalClass.CUTOFF_UNAVAILABLE
    return NotionalClass.ABOVE if value > cutoff else NotionalClass.AT_OR_BELOW


@dataclass(frozen=True)
class Notional:
    by_class: Mapping[NotionalClass, float]
    total: float


def notional_by_class(
    trades: Sequence[Trade], me: Mapping[Month, Mapping[int, float]], cutoffs: Mapping[Month, float]
) -> Notional:
    by_class: dict[NotionalClass, list[float]] = {c: [] for c in NotionalClass}
    for trade in trades:
        by_class[notional_class(trade, me, cutoffs)].append(trade.notional)
    return Notional({c: math.fsum(v) for c, v in by_class.items()}, math.fsum(t.notional for t in trades))


# --------------------------------------------------------------------------- windows


STAGE_A: Final = "stage A"
STAGE_B: Final = "stage B"
POOLED: Final = "pooled"


def window_of(month: Month) -> str:
    """A formation's (or trade's) stage: before 2021-05 is stage A; from it on, the 2024-08 liquidation included, B."""
    return STAGE_A if month < STAGE_B_FORMATIONS[0] else STAGE_B


def in_window(month: Month, window: str) -> bool:
    return window == POOLED or window_of(month) == window


@dataclass(frozen=True)
class Range:
    #: Formations where the field is available, of ``formations``.
    covered: int
    formations: int
    low: float | None
    high: float | None


def summarise(rows: Sequence[UniverseMonth], window: str) -> dict[str, Range]:
    """Premise 3's min/max over the window's formations, per numeric field, on the formations where it is available."""
    chosen = [r for r in rows if in_window(month_of(r.formation), window)]
    fields = [f for f in UniverseMonth.__dataclass_fields__ if f != "formation"]
    out: dict[str, Range] = {}
    for name in fields:
        values = [float(v) for r in chosen if (v := getattr(r, name)) is not None]
        out[name] = Range(len(values), len(chosen), min(values, default=None), max(values, default=None))
    return out


# --------------------------------------------------------------------------- the whole block


@dataclass(frozen=True)
class UniverseDiagnostics:
    rows: tuple[UniverseMonth, ...]
    summaries: Mapping[str, Mapping[str, Range]]
    notional: Mapping[AmbiguityArm, Mapping[str, Notional]]


def diagnostics(
    panel: Sequence[PanelMonth],
    universes: Sequence[Sequence[int]],
    decisions: Sequence[Decision],
    run: SeriesRun,
    cutoffs: Mapping[date, float],
) -> UniverseDiagnostics:
    """Every figure in this module. ``universes`` and ``decisions`` are per formation, in ``panel``'s order;
    ``cutoffs`` is :func:`read_cutoffs`'s."""
    if not (len(panel) == len(universes) == len(decisions)):
        raise ValueError(f"{len(panel)} formations, {len(universes)} universes, {len(decisions)} decisions")
    rows = tuple(
        universe_month(month, universe, decision, cutoffs.get(month.formation))
        for month, universe, decision in zip(panel, universes, decisions, strict=True)
    )
    me = {month_of(m.formation): {name: p.me for name, p in m.admitted.items()} for m in panel}
    by_month = {month_of(m.formation): cutoffs[m.formation] for m in panel if m.formation in cutoffs}
    windows = (STAGE_A, STAGE_B, POOLED)
    notional: dict[AmbiguityArm, dict[str, Notional]] = {}
    for arm in ARMS:
        trades = run.book[(arm, BASE)].trades
        notional[arm] = {
            w: notional_by_class([t for t in trades if in_window(t.month, w)], me, by_month) for w in windows
        }
    return UniverseDiagnostics(rows, {w: summarise(rows, w) for w in windows}, notional)


__all__ = [
    "COMMODITY_SIC",
    "NYSE_CUTOFFS",
    "POOLED",
    "STAGE_A",
    "STAGE_B",
    "Notional",
    "NotionalClass",
    "Range",
    "UniverseDiagnostics",
    "UniverseMonth",
    "diagnostics",
    "notional_by_class",
    "notional_class",
    "read_cutoffs",
    "summarise",
    "universe_month",
    "window_of",
]
