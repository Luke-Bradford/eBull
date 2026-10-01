"""Ranking-pot-v1 pure core (#2842 slice 2).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md``: §5.0 universes, §2 breakpoint and MAX cut,
R/F ranks for ANY order (the real one or a §9.2 control's), §5.1 rules, §5.2 decision rows, §7.2 levels and
asserts, and the §6 ticket.

Everything here is pure. Slice 4's rebalance job reads the snapshot rows and hands them in; the snapshot stores
these inputs, never this module's outputs, so a replay re-derives every decision from the same bytes.

Fixed here by construction (each closes a spec Appendix A item; the PR lists them):

- Percentiles are NEAREST-RANK for both the cap breakpoint and the MAX cut (r3-86). A name AT the cut passes.
- Ties in an order break by the RECIPIENT's ``instrument_id`` ascending (r3-5), so the real book (where recipient
  and donor coincide) and every control break ties the same way.
- MAX and ATR14 are both read from the one latest-price-segment series the caller passes (r3-54): a single
  corporate-action basis. ATR14 is the house Wilder ``atr_series`` at the segment's last bar, which must be the last
  completed session, over a series ``ai_trial_pack.build_bar_series`` accepts (≥ 60 bars, as v1; r3-87).
- First-failed-rule evaluation order is the tuple order of ``HOLD_RULES`` / ``ENTRY_RULES`` (r3-105).
- A position whose exit was stamped at an EARLIER rebalance keeps its slot until it closes (r3-99); only exits
  stamped by this rebalance free a slot for this rebalance's entries ("exits first", §7.2).
- A name whose lifecycle closed since the previous decided rebalance (any exit reason, including a protective one)
  is not re-entered (r3-102, r3-104 ``recently_exited``).
- ``not_selected`` reasons are closed and frozen in precedence order: ``infeasible:<rule>`` > ``recently_exited`` >
  ``frank_out_of_band`` (r3-103) > ``entries_halted`` > ``band_full``.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final, Literal

from app.services.ai_trial_pack import ShortlistCandidate, build_bar_series, is_eligible
from app.services.indicator_series import BarSeries, atr_series
from app.services.market_calendar import us_market_status

STRATEGY_ID: Final = "ranking-pot-v1"
#: §2 — by construction.
N: Final = 25
HOLD_BAND_MULTIPLE: Final = 2
#: §2 — Fama & French (2008) microcap cut, adapted; Bali, Cakici & Whitelaw (2011) lottery guard, adapted.
BREAKPOINT_PERCENTILE: Final = 20
MAX_CUT_PERCENTILE: Final = 90
#: 22 closes on exactly the last 22 NYSE sessions → 21 close-to-close returns.
MAX_SESSIONS: Final = 22
#: §2 — operator rule 2026-09-28: SL = 3 × ATR14, TP = 2R; the asserted band is the operator's [1, 4] ATR and ≥ 1.5R.
ATR_STOP_MULTIPLE: Final = 3
TARGET_R_MULTIPLE: Final = 2
STOP_ATR_MIN: Final = 1
STOP_ATR_MAX: Final = 4
TARGET_R_MIN: Final = Fraction(3, 2)
ATR_PERIOD: Final = 14
ATR_UNIVERSE: Final = "survivor_only"  # as v1's `ai_trial_pack.indicators` passes it
#: §4 step 2 — minimum reference samples.
MIN_NYSE_CAPS: Final = 500
MIN_VALID_MAX: Final = 200
#: §6 — `scores` stores raw_total and total_score as NUMERIC(10,4) (sql/001, sql/008): each is rounded to half a
#: unit in the 4th place, so a reconciliation within one unit there is exact up to storage rounding.
RECONCILE_TOLERANCE: Final = Fraction(1, 10_000)
SCORE_CLIP: Final = (Fraction(0), Fraction(1))  # `scoring._clip` bounds
ENTRY_RULE_ID: Final = "ranking-pot-v1:enter-frank-le-N"
EXIT_RULE: Final = (
    "broker SL = entry − 3×ATR14, TP = entry + 6×ATR14; rank exit when R-rank > 2N or the name leaves R; no age exit"
)

HoldRule = Literal[
    "not_tradable",
    "not_us_equity",
    "score_not_positive",
    "completeness_insufficient",
    "filings_not_analysable",
    "cap_unavailable",
    "cap_below_breakpoint",
]
#: §5.0 R_t rules, in their frozen evaluation order (r3-105).
HOLD_RULES: Final[tuple[HoldRule, ...]] = (
    "not_tradable",
    "not_us_equity",
    "score_not_positive",
    "completeness_insufficient",
    "filings_not_analysable",
    "cap_unavailable",
    "cap_below_breakpoint",
)
EntryRule = Literal[
    "quote_ineligible",
    "max_unavailable",
    "max_above_cut",
    "bars_incomplete",
    "atr_unavailable",
    "stop_not_below_ask",
]
#: §5.0 F_t rules, in their frozen evaluation order (r3-105).
ENTRY_RULES: Final[tuple[EntryRule, ...]] = (
    "quote_ineligible",
    "max_unavailable",
    "max_above_cut",
    "bars_incomplete",
    "atr_unavailable",
    "stop_not_below_ask",
)
UniverseRefusal = Literal["breakpoint_unavailable", "max_cut_unavailable"]
Action = Literal["exit", "exit_pending", "hold", "enter", "not_selected"]
#: §5.2 row precedence, highest first.
ACTION_PRECEDENCE: Final[tuple[Action, ...]] = ("exit", "exit_pending", "hold", "enter", "not_selected")


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class NameFacts:
    """One S₀ name's own facts at the snapshot (§4 step 3).

    ``market_cap_usd`` is ``instrument_valuation.market_cap_live`` with the #1664 overlay applied as the scorer applies
    it, resolved by the caller; ``None`` when the overlay fails or yields no cap (fail closed, §2). ``bar_dates`` /
    ``bar_rows`` are the name's latest price segment of completed sessions, ascending, as stored.
    """

    instrument_id: int
    symbol: str
    is_tradable: bool
    asset_class: str | None
    total_score: Decimal | None
    completeness_tier: str | None
    filings_status: str | None
    market_cap_usd: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    quoted_at: datetime | None
    bar_dates: tuple[date, ...] = ()
    bar_rows: tuple[Mapping[str, Any], ...] = ()


def _finite_positive(value: Decimal | Fraction | float | None) -> bool:
    if value is None:
        return False
    if isinstance(value, Decimal):
        return value.is_finite() and value > 0
    if isinstance(value, Fraction):
        return value > 0
    return math.isfinite(value) and value > 0


def _exact(value: Decimal | float) -> Fraction:
    """The rational a stored value denotes; a float by its shortest round-trip form."""
    return Fraction(Decimal(repr(value)) if isinstance(value, float) else value)


def nearest_rank(values: Iterable[Fraction | Decimal], percentile: int) -> Fraction | Decimal | None:
    """The nearest-rank percentile: the ⌈p·n/100⌉-th smallest value (1-based, at least the first)."""
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(1, -(-percentile * len(ordered) // 100)) - 1]


def max_window_sessions(last_session: date) -> tuple[date, ...]:
    """The last ``MAX_SESSIONS`` NYSE sessions ending at ``last_session``, ascending."""
    out: list[date] = []
    d = last_session
    while len(out) < MAX_SESSIONS:
        if us_market_status(d) != "closed":
            out.append(d)
        d -= timedelta(days=1)
    return tuple(reversed(out))


def max_daily_return(facts: NameFacts, sessions: Sequence[date]) -> Fraction | None:
    """§2 MAX: the largest close-to-close return over ``sessions`` (exactly the last 22 NYSE sessions), or ``None``
    unless the segment's last 22 bars are exactly those sessions with every close finite and > 0."""
    if len(sessions) != MAX_SESSIONS or len(facts.bar_dates) < MAX_SESSIONS:
        return None
    if tuple(facts.bar_dates[-MAX_SESSIONS:]) != tuple(sessions):
        return None
    closes: list[Fraction] = []
    for row in facts.bar_rows[-MAX_SESSIONS:]:
        c = row.get("close")
        if not isinstance(c, Decimal | float) or not _finite_positive(c):
            return None
        closes.append(_exact(c))
    return max(closes[i] / closes[i - 1] - 1 for i in range(1, MAX_SESSIONS))


def atr14(facts: NameFacts, *, last_session: date) -> Fraction | None:
    """House Wilder ATR14 at the segment's last bar, or ``None`` unless ``build_bar_series`` accepts the segment
    (≥ 60 bars, ascending, last bar = ``last_session``, finite and range-consistent OHLC) and the value is > 0."""
    return _atr_of(build_bar_series(facts.bar_dates, facts.bar_rows, last_session=last_session))


def _atr_of(series: BarSeries | str) -> Fraction | None:
    if isinstance(series, str):
        return None
    if any(not _finite_positive(row[k]) for row in series.rows for k in ("open", "high", "low", "close")):
        return None  # r3-37: OHLC ordering alone admits zero or negative prices
    value = atr_series(series, universe=ATR_UNIVERSE, period=ATR_PERIOD).values[-1]
    if value is None or not _finite_positive(value):
        return None
    return _exact(value)


# ---------------------------------------------------------------------------
# §5.0 universes
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Universes:
    breakpoint: Decimal
    max_cut: Fraction
    #: R_t and F_t ⊆ R_t, identical in every book.
    r_ids: frozenset[int]
    f_ids: frozenset[int]
    #: S₀ names outside R_t → their first failed hold rule.
    hold_failure: Mapping[int, HoldRule]
    #: R_t names outside F_t → their first failed entry rule.
    entry_failure: Mapping[int, EntryRule]
    own_score: Mapping[int, Decimal]
    max_return: Mapping[int, Fraction]
    atr: Mapping[int, Fraction]
    #: Valid-MAX population size behind the cut (R ∩ quote-eligible).
    max_population: int
    nyse_cap_population: int


def _hold_failure(f: NameFacts, breakpoint: Decimal) -> HoldRule | None:
    checks: dict[HoldRule, bool] = {
        "not_tradable": f.is_tradable,
        "not_us_equity": f.asset_class == "us_equity",
        "score_not_positive": _finite_positive(f.total_score),
        "completeness_insufficient": f.completeness_tier is not None and f.completeness_tier != "insufficient_data",
        "filings_not_analysable": f.filings_status == "analysable",
        "cap_unavailable": _finite_positive(f.market_cap_usd),
        # Guarded: the dict is built eagerly, and ordering a NaN Decimal raises.
        "cap_below_breakpoint": f.market_cap_usd is not None
        and _finite_positive(f.market_cap_usd)
        and f.market_cap_usd >= breakpoint,
    }
    return next((rule for rule in HOLD_RULES if not checks[rule]), None)


def _quote_ok(f: NameFacts, as_of: datetime) -> bool:
    candidate = ShortlistCandidate(
        instrument_id=f.instrument_id,
        symbol=f.symbol,
        bid=f.bid,
        ask=f.ask,
        quoted_at=f.quoted_at,
        total_score=float(f.total_score) if f.total_score is not None else None,
        market_cap_usd=None,
    )
    return is_eligible(candidate, as_of=as_of)


def build_universes(
    facts: Sequence[NameFacts],
    *,
    nyse_caps: Sequence[Decimal | None],
    as_of: datetime,
    last_session: date,
) -> Universes | UniverseRefusal:
    """§5.0 R_t and F_t over the S₀ ``facts``.

    ``nyse_caps`` is the overlaid cap of every scored, tradable NYSE name in the run (§2), INCLUDING names outside S₀
    (r3-85: the snapshot stores this population); ``None`` entries are names whose overlay failed and are excluded.
    """
    ids = [f.instrument_id for f in facts]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate instrument_id among S₀ facts")
    caps = [c for c in nyse_caps if _finite_positive(c)]
    if len(caps) < MIN_NYSE_CAPS:
        return "breakpoint_unavailable"
    breakpoint = nearest_rank([c for c in caps if c is not None], BREAKPOINT_PERCENTILE)
    assert isinstance(breakpoint, Decimal)

    hold_failure: dict[int, HoldRule] = {}
    ranking: list[NameFacts] = []
    for f in facts:
        failed = _hold_failure(f, breakpoint)
        if failed is None:
            ranking.append(f)
        else:
            hold_failure[f.instrument_id] = failed

    sessions = max_window_sessions(last_session)
    quoted = {f.instrument_id for f in ranking if _quote_ok(f, as_of)}
    max_return = {f.instrument_id: m for f in ranking if (m := max_daily_return(f, sessions)) is not None}
    cut_population = [m for iid, m in max_return.items() if iid in quoted]
    if len(cut_population) < MIN_VALID_MAX:
        return "max_cut_unavailable"
    cut = nearest_rank(cut_population, MAX_CUT_PERCENTILE)
    assert isinstance(cut, Fraction)

    entry_failure: dict[int, EntryRule] = {}
    atr: dict[int, Fraction] = {}
    feasible: set[int] = set()
    for f in ranking:
        iid = f.instrument_id
        series = build_bar_series(f.bar_dates, f.bar_rows, last_session=last_session)
        a = _atr_of(series)
        if a is not None:
            atr[iid] = a
        checks: dict[EntryRule, bool] = {
            "quote_ineligible": iid in quoted,
            "max_unavailable": iid in max_return,
            "max_above_cut": iid in max_return and max_return[iid] <= cut,
            "bars_incomplete": not isinstance(series, str),
            "atr_unavailable": a is not None,
            "stop_not_below_ask": a is not None
            and f.ask is not None
            and _finite_positive(f.ask)
            and ATR_STOP_MULTIPLE * a < _exact(f.ask),
        }
        failed_entry: EntryRule | None = next((rule for rule in ENTRY_RULES if not checks[rule]), None)
        if failed_entry is None:
            feasible.add(iid)
        else:
            entry_failure[iid] = failed_entry

    return Universes(
        breakpoint=breakpoint,
        max_cut=cut,
        r_ids=frozenset(f.instrument_id for f in ranking),
        f_ids=frozenset(feasible),
        hold_failure=hold_failure,
        entry_failure=entry_failure,
        own_score={f.instrument_id: f.total_score for f in ranking if f.total_score is not None},
        max_return=max_return,
        atr=atr,
        max_population=len(cut_population),
        nyse_cap_population=len(caps),
    )


# ---------------------------------------------------------------------------
# Orders and ranks (§5.0, §9.2)
# ---------------------------------------------------------------------------
def rank_order(r_ids: Iterable[int], order_score: Callable[[int], Decimal | None]) -> tuple[int, ...]:
    """R_t ordered by ``order_score`` descending; ties by the recipient's ``instrument_id`` (r3-5). A name whose order
    score is missing, non-finite or ≤ 0 sorts after every other, by ``instrument_id`` (§9.2 missing donor)."""
    valid: list[tuple[Decimal, int]] = []
    missing: list[int] = []
    for iid in r_ids:
        score = order_score(iid)
        if score is not None and _finite_positive(score):
            valid.append((score, iid))
        else:
            missing.append(iid)
    valid.sort(key=lambda pair: (-pair[0], pair[1]))
    return tuple(iid for _, iid in valid) + tuple(sorted(missing))


def real_order(universes: Universes) -> tuple[int, ...]:
    """The shadow's and the executed book's order: every R member by its own score."""
    return rank_order(universes.r_ids, universes.own_score.get)


def control_order(
    universes: Universes, donor_of: Mapping[int, int], run_scores: Mapping[int, Decimal | None]
) -> tuple[int, ...]:
    """A §9.2 control's order: each R member takes its donor's score in this run (``donor_of`` is the control's
    bijection of S₀; ``run_scores`` every S₀ name's score in the run, absent or ``None`` when it has none)."""
    return rank_order(universes.r_ids, lambda iid: run_scores.get(donor_of[iid]))


@dataclass(frozen=True)
class Ranks:
    r_rank: Mapping[int, int]
    f_rank: Mapping[int, int]


def ranks(universes: Universes, order: Sequence[int]) -> Ranks:
    if len(order) != len(universes.r_ids) or set(order) != universes.r_ids:
        raise ValueError("order must be a permutation of R_t")
    r_rank = {iid: k for k, iid in enumerate(order, start=1)}
    f_order = [iid for iid in order if iid in universes.f_ids]
    return Ranks(r_rank=r_rank, f_rank={iid: k for k, iid in enumerate(f_order, start=1)})


# ---------------------------------------------------------------------------
# §5.1 rules and §5.2 rows
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Holding:
    """A held name in one book: ``open`` or an entry planned / submitted / uncertain (``entry_pending``).
    ``exit_stamped`` = it already carries its lifecycle's immutable exit stamp from an earlier rebalance."""

    instrument_id: int
    status: Literal["open", "entry_pending"]
    exit_stamped: bool = False


@dataclass(frozen=True)
class DecisionRow:
    instrument_id: int
    action: Action
    reason: str | None
    r_rank: int | None
    f_rank: int | None


@dataclass(frozen=True)
class BookDecision:
    rows: tuple[DecisionRow, ...]
    #: In entry order (ascending F-rank); slots fill in this order.
    entries: tuple[int, ...]
    #: Names stamped for exit by this rebalance.
    exits: tuple[int, ...]
    slots_unfilled: int
    #: Slots held through the rebalance: holds plus exits stamped at an earlier rebalance.
    occupied: int


def _sorted_rows(rows: Iterable[DecisionRow]) -> tuple[DecisionRow, ...]:
    """§5.2 precedence order, then ``instrument_id``."""
    return tuple(sorted(rows, key=lambda row: (ACTION_PRECEDENCE.index(row.action), row.instrument_id)))


def _check_holdings(holdings: Sequence[Holding], universes: Universes) -> None:
    ids = [h.instrument_id for h in holdings]
    if len(set(ids)) != len(ids):
        raise ValueError("a name is held twice in one book")
    known = universes.r_ids | universes.hold_failure.keys()
    if not set(ids) <= known:
        raise ValueError("a held name is outside S₀")


def decide(
    universes: Universes,
    order: Sequence[int],
    holdings: Sequence[Holding],
    *,
    recently_exited: frozenset[int],
    entries_allowed: bool,
    n: int = N,
) -> BookDecision:
    """§5.1 for one book under ``order`` (the real order or a control's). Returns exactly one §5.2 row per held name
    and per name with R-rank ≤ 2N.

    ``recently_exited``: names whose lifecycle in THIS book closed since the previous decided rebalance.
    ``entries_allowed``: ``False`` for the executed book outside ``executing``; ``True`` for the shadow and controls.
    """
    if n <= 0:
        raise ValueError("n must be positive")
    _check_holdings(holdings, universes)
    rk = ranks(universes, order)
    band = HOLD_BAND_MULTIPLE * n
    rows: dict[int, DecisionRow] = {}
    exits: list[int] = []
    occupied = 0

    for h in holdings:
        iid = h.instrument_id
        r, f = rk.r_rank.get(iid), rk.f_rank.get(iid)
        if h.exit_stamped:
            rows[iid] = DecisionRow(iid, "exit_pending", None, r, f)
            occupied += 1  # r3-99: the slot is released only when the close reconciles
        elif iid not in universes.r_ids:
            rows[iid] = DecisionRow(iid, "exit", f"ineligible:{universes.hold_failure[iid]}", None, None)
            exits.append(iid)
        elif r is not None and r > band:
            rows[iid] = DecisionRow(iid, "exit", "rerank_out_of_band", r, f)
            exits.append(iid)
        else:
            rows[iid] = DecisionRow(iid, "hold", None, r, f)
            occupied += 1

    capacity = max(0, n - occupied)
    entries: list[int] = []
    for iid in order:
        r = rk.r_rank[iid]
        if r > band:
            break
        if iid in rows:
            continue
        f = rk.f_rank.get(iid)
        reason: str | None
        if iid not in universes.f_ids:
            reason = f"infeasible:{universes.entry_failure[iid]}"
        elif iid in recently_exited:
            reason = "recently_exited"
        elif f is None or f > n:
            reason = "frank_out_of_band"
        elif not entries_allowed:
            reason = "entries_halted"
        elif len(entries) >= capacity:
            reason = "band_full"
        else:
            reason = None
        if reason is None:
            entries.append(iid)
            rows[iid] = DecisionRow(iid, "enter", None, r, f)
        else:
            rows[iid] = DecisionRow(iid, "not_selected", reason, r, f)

    ordered_rows = tuple(
        sorted(rows.values(), key=lambda row: (ACTION_PRECEDENCE.index(row.action), row.instrument_id))
    )
    return BookDecision(
        rows=ordered_rows,
        entries=tuple(entries),
        exits=tuple(exits),
        slots_unfilled=capacity - len(entries),
        occupied=occupied,
    )


def wind_down(holdings: Sequence[Holding]) -> tuple[DecisionRow, ...]:
    """§5.1 rule 3: the wind-down event stamps every unstamped held name's exit; a name already stamped keeps its
    stamp (one per lifecycle) and its original reason (r3-66). No ranking is read."""
    ids = [h.instrument_id for h in holdings]
    if len(set(ids)) != len(ids):
        raise ValueError("a name is held twice in one book")
    return _sorted_rows(
        DecisionRow(h.instrument_id, "exit_pending", None, None, None)
        if h.exit_stamped
        else DecisionRow(h.instrument_id, "exit", "wind_down", None, None)
        for h in holdings
    )


# ---------------------------------------------------------------------------
# §7.2 levels and asserts
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Levels:
    basis: Fraction
    stop_loss: Fraction
    take_profit: Fraction


def half_spread(bid: Decimal | None, ask: Decimal | None) -> Fraction | None:
    """h = (ask − bid) / (ask + bid): half the spread as a fraction of the mid (r3-45). ``None`` unless
    0 < bid ≤ ask, both finite."""
    if not (_finite_positive(bid) and _finite_positive(ask)):
        return None
    assert bid is not None and ask is not None
    b, a = _exact(bid), _exact(ask)
    if a < b:
        return None
    return (a - b) / (a + b)


def validate_levels(price: Fraction, stop_loss: Fraction, take_profit: Fraction, atr: Fraction) -> bool:
    """§7.2 asserts on the levels SENT (after any broker rounding): ``0 < SL < price < TP``,
    ``1 ≤ (price − SL)/ATR14 ≤ 4``, ``TP − price ≥ 1.5 (price − SL)``."""
    if not (atr > 0 and 0 < stop_loss < price < take_profit):
        return False
    risk = price - stop_loss
    return STOP_ATR_MIN <= risk / atr <= STOP_ATR_MAX and take_profit - price >= TARGET_R_MIN * risk


def planned_levels(basis: Decimal | Fraction, atr: Fraction) -> Levels | Literal["protective_levels_invalid"]:
    """The frozen policy, exactly (r3-123): SL = basis − 3·ATR14, TP = basis + 2R = basis + 6·ATR14. Levels are only
    ever produced here; ``validate_levels`` is the operator-band assert on what is sent."""
    b = basis if isinstance(basis, Fraction) else (_exact(basis) if _finite_positive(basis) else Fraction(0))
    if b <= 0 or atr <= 0:
        return "protective_levels_invalid"
    risk = ATR_STOP_MULTIPLE * atr
    levels = Levels(basis=b, stop_loss=b - risk, take_profit=b + TARGET_R_MULTIPLE * risk)
    if not validate_levels(levels.basis, levels.stop_loss, levels.take_profit, atr):
        return "protective_levels_invalid"
    return levels


def stop_loss_pct(price: Fraction, stop_loss: Fraction) -> Fraction:
    """§7.2: ``100 × (ask − SL) / ask`` of the validated levels."""
    if price <= 0:
        raise ValueError("price must be positive")
    return 100 * (price - stop_loss) / price


def basis_unchanged(snapshot_close: Decimal | None, current_close: Decimal | None) -> bool:
    """§7.2 split check: the snapshot's last close equals the current stored close for the same session, exactly.
    A missing or non-finite value on either side fails closed (r3-122) — the caller refuses ``basis_changed``.
    A residual (r3-121): equality detects a provider rewrite, not every corporate action."""
    if not (_finite_positive(snapshot_close) and _finite_positive(current_close)):
        return False
    assert snapshot_close is not None and current_close is not None
    return _exact(snapshot_close) == _exact(current_close)


# ---------------------------------------------------------------------------
# §6 ticket
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ScoreBreakdown:
    """A ``scores`` row as stored: family scores, ``raw_total``, ``total_score`` and ``penalties_json``."""

    model_version: str
    total_score: Decimal
    raw_total: Decimal | None
    families: Mapping[str, Decimal | None]
    penalties_json: Sequence[Mapping[str, Any]]


@dataclass(frozen=True)
class ThesisRef:
    thesis_id: int
    age_days: int
    model: str | None
    prompt_version: str | None


def _adjustments(penalties_json: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    penalties: list[dict[str, Any]] = []
    rewards: list[dict[str, Any]] = []
    for item in penalties_json:
        kind = item.get("kind", "penalty")  # pre-#1635 rows carry no kind and are penalties
        if kind == "reward":
            rewards.append({"name": item["name"], "addition": item["addition"]})
        elif kind == "penalty":
            penalties.append({"name": item["name"], "deduction": item["deduction"]})
        else:
            raise ValueError(f"unknown penalties_json kind {kind!r}")
    return penalties, rewards


def reconciles(score: ScoreBreakdown) -> bool:
    """§6: ``total_score = clip(raw_total − P + R)`` within storage rounding (r3-107)."""
    if score.raw_total is None:
        return False
    penalties, rewards = _adjustments(score.penalties_json)
    lo, hi = SCORE_CLIP
    value = (
        _exact(score.raw_total)
        - sum((_exact(p["deduction"]) for p in penalties), Fraction(0))
        + sum((_exact(r["addition"]) for r in rewards), Fraction(0))
    )
    return abs(max(lo, min(hi, value)) - _exact(score.total_score)) <= RECONCILE_TOLERANCE


def entry_ticket(
    *,
    book: str,
    declaration_id: int,
    instrument_id: int,
    symbol: str,
    row: DecisionRow,
    own_score: ScoreBreakdown,
    family_weights: Mapping[str, float] | None,
    order_donor_id: int,
    order_score: Decimal | None,
    thesis: ThesisRef | None,
    levels: Levels,
    atr: Fraction,
    expected_half_spread: Fraction,
) -> dict[str, Any]:
    """The §6 ticket for an ``enter`` row, written before any order. ``own_score`` is the RECIPIENT's score and
    factors; ``order_donor_id`` / ``order_score`` record the score that placed it in the order (the recipient itself
    in the real book, the donor in a control — r3-106). Planned levels use the snapshot close basis; actual levels
    are slice 5's, recorded before I/O."""
    if row.action != "enter" or row.instrument_id != instrument_id:
        raise ValueError("a ticket is written only for this name's enter row")
    penalties, rewards = _adjustments(own_score.penalties_json)
    return {
        "strategy_id": STRATEGY_ID,
        "rule_id": ENTRY_RULE_ID,
        "evidence_id": declaration_id,
        "rationale_class": "signal",
        "book": book,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "r_rank": row.r_rank,
        "f_rank": row.f_rank,
        "order": {"donor_instrument_id": order_donor_id, "score": order_score},
        "score": {
            "model_version": own_score.model_version,
            "total_score": own_score.total_score,
            "raw_total": own_score.raw_total,
            "families": dict(own_score.families),
            "family_weights": dict(family_weights) if family_weights is not None else None,
            "penalties": penalties,
            "rewards": rewards,
            "clip": [str(SCORE_CLIP[0]), str(SCORE_CLIP[1])],
            "reconciles": reconciles(own_score),
        },
        "thesis": None
        if thesis is None
        else {
            "thesis_id": thesis.thesis_id,
            "age_days": thesis.age_days,
            "model": thesis.model,
            "prompt_version": thesis.prompt_version,
        },
        "exit_rule": EXIT_RULE,
        "planned_levels": {
            "basis": str(levels.basis),
            "stop_loss": str(levels.stop_loss),
            "take_profit": str(levels.take_profit),
            "atr14": str(atr),
        },
        "expected_cost": {"half_spread_fraction": str(expected_half_spread)},
    }


__all__ = [
    "ACTION_PRECEDENCE",
    "ENTRY_RULES",
    "HOLD_RULES",
    "N",
    "BookDecision",
    "DecisionRow",
    "Holding",
    "Levels",
    "NameFacts",
    "ScoreBreakdown",
    "ThesisRef",
    "Universes",
    "atr14",
    "basis_unchanged",
    "build_universes",
    "control_order",
    "decide",
    "entry_ticket",
    "half_spread",
    "max_daily_return",
    "max_window_sessions",
    "nearest_rank",
    "planned_levels",
    "rank_order",
    "ranks",
    "real_order",
    "reconciles",
    "stop_loss_pct",
    "validate_levels",
    "wind_down",
]
