"""#3620 slice 1 — the pure rules of the cross-asset TSMOM study.

Spec: ``docs/research/2026-10-06-3620-cross-asset-tsmom.md``. It lists the source rule (Moskowitz, Ooi & Pedersen
2012) and every deviation from it; nothing here is tuned. Nothing here reads the database: the census script feeds
it panel rows, and slice 3's gated run will feed it the rest. No outcome is computed on real data before a
declaration exists, and this module has no entry point that could do so.

Months are ``(year, month)`` tuples (``total_return_reader.Month``). A fund's returns map month → decimal simple
return for that calendar month.
"""

from __future__ import annotations

import math
import random
import statistics
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Final, Literal

from app.services.etf_total_return_reader import EtfMonthlyReturn, EtfVerdict
from app.services.total_return_reader import Month, add_months, month_of

#: Spec §"Universe and eligibility": the house partition, also the primary segmentation. DBC is excluded (#3676).
CLASSES: Final[Mapping[str, tuple[str, ...]]] = {
    "us_equity": ("SPY", "QQQ", "IWM", "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"),
    "non_us_equity": ("EFA", "EEM", "VGK", "EWJ"),
    "treasuries": ("TLT", "IEF", "SHY"),
    "credit_inflation": ("AGG", "LQD", "HYG", "TIP"),
    "commodities_metals": ("GLD", "IAU", "SLV", "USO"),
    "real_estate": ("VNQ", "XLRE"),
}
FUNDS: Final[tuple[str, ...]] = tuple(sorted(s for funds in CLASSES.values() for s in funds))
#: The equity sub-book compared with AQR's ``TSMOM^EQ`` (spec §Outputs).
EQUITY_BOOK: Final[tuple[str, ...]] = tuple(sorted(CLASSES["us_equity"] + CLASSES["non_us_equity"]))
#: Step 0's B1/B2 funds (``report_3609_baselines``), loaded and coverage-checked from the start.
BASELINES: Final[Mapping[str, Mapping[str, float]]] = {
    "B1": {"SPY": 1.0},
    "B2a": {"SPY": 0.60, "AGG": 0.40},
    "B2b": {"VTI": 0.30, "EFA": 0.15, "EEM": 0.05, "VNQ": 0.20, "IEF": 0.15, "TIP": 0.15},
}
COMPARATORS: Final[tuple[str, ...]] = tuple(sorted({s for weights in BASELINES.values() for s in weights}))

#: Commodity pools read Intrader months only (price-return extension off).
POOLS: Final[frozenset[str]] = frozenset({"GLD", "IAU", "SLV", "USO"})
#: Spec §Returns: the panel each fund must carry. Any other verdict refuses.
ACCEPTED_VERDICTS: Final[Mapping[str, frozenset[EtfVerdict]]] = {
    "pool": frozenset({EtfVerdict.PRICE_RETURN_EXCLUDED}),
    "fund": frozenset({EtfVerdict.NPORT, EtfVerdict.NPORT_PROXY}),
}

#: Spec §"End E": the chosen coverage cap.
COVERAGE_CAP: Final[Month] = (2024, 8)
LOOKBACK_MONTHS: Final = 12
#: Step 0's ``B3_STALE_DAYS``: an Intrader month-end bar must fall within this many days of the calendar month-end.
STALE_DAYS: Final = 7
WEIGHT_TOLERANCE: Final = 1e-12
COST_TOLERANCE: Final = 1e-14
COST_MAX_ITERATIONS: Final = 100
DRAWS: Final = 1_000

Timing = Literal["lagged", "same_close"]
TIMINGS: Final[tuple[Timing, ...]] = ("lagged", "same_close")


class TsmomRefusal(RuntimeError):
    """A spec refusal. ``code`` names the rule; the run never continues past one."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


def _assert_partition() -> None:
    members = [s for funds in CLASSES.values() for s in funds]
    if len(members) != len(set(members)) or len(members) != 30:
        raise AssertionError("CLASSES must be six disjoint sets covering 30 funds")


_assert_partition()


# ---------------------------------------------------------------------------
# Validity (spec §Validity)
# ---------------------------------------------------------------------------


def check_wealth_return(label: str, month: Month, value: float) -> float:
    """A fund return or RF, compounded as wealth: finite and above −100%."""
    if not math.isfinite(value) or value <= -1.0:
        raise TsmomRefusal("invalid_value", f"{label} {month}: {value!r}")
    return value


def check_finite(label: str, month: Month, value: float) -> float:
    """A factor or AQR observation (long/short or excess): finite only."""
    if not math.isfinite(value):
        raise TsmomRefusal("invalid_value", f"{label} {month}: {value!r}")
    return value


def month_range(first: Month, last: Month) -> list[Month]:
    out: list[Month] = []
    month = first
    while month <= last:
        out.append(month)
        month = add_months(month, 1)
    return out


# ---------------------------------------------------------------------------
# Census: verdicts, coverage, start, E (spec §"Universe and eligibility")
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FundCoverage:
    """One fund's panel rows as read. Values and anchors are validated only through E (``validate_through``)."""

    symbol: str
    verdict: EtfVerdict
    first_month: Month
    last_month: Month
    #: Raw values; ``_need`` and ``validate_through`` check them where they are consumed.
    returns: Mapping[Month, float]
    #: Per month: the Intrader bars the return runs between (``None`` for N-PORT months).
    anchors: Mapping[Month, tuple[date | None, date | None]] = field(default_factory=dict)
    duplicates: frozenset[Month] = frozenset()

    @property
    def first_eligible(self) -> Month:
        return add_months(self.first_month, LOOKBACK_MONTHS - 1)


def fund_coverage(symbol: str, verdict: EtfVerdict, rows: Sequence[EtfMonthlyReturn]) -> FundCoverage:
    """One fund's panel months, refusing no rows or a wrong verdict.

    Nothing about individual rows is judged here: rows after E are untouched by the spec, and E is not known until
    every fund is read (``build_census`` → ``validate_through``).
    """
    if not rows:
        raise TsmomRefusal("no_panel_rows", symbol)
    expected = ACCEPTED_VERDICTS["pool" if symbol in POOLS else "fund"]
    if verdict not in expected:
        raise TsmomRefusal("verdict", f"{symbol}: {verdict.value}, expected one of {sorted(v.value for v in expected)}")
    returns: dict[Month, float] = {}
    anchors: dict[Month, tuple[date | None, date | None]] = {}
    duplicates: set[Month] = set()
    for row in rows:
        month = month_of(row.month)
        if month in returns:
            duplicates.add(month)
        returns[month] = row.total_return
        anchors[month] = (row.start_bar, row.end_bar)
    months = sorted(returns)
    return FundCoverage(symbol, verdict, months[0], months[-1], returns, anchors, frozenset(duplicates))


def _stale(anchor: date, month: Month) -> bool:
    """An Intrader month-end bar more than ``STALE_DAYS`` before ``month``'s last calendar day."""
    return (date_of(add_months(month, 1)) - anchor).days - 1 > STALE_DAYS


def validate_through(coverage: FundCoverage, end: Month) -> None:
    """Spec §Validity for every row up to ``end``: no duplicate, valid value, fresh closing AND opening anchors.

    The opening anchor is the previous month's last bar; for a series' first row it is not any row's closing
    anchor, so it is checked here against the preceding calendar month.
    """
    for month in sorted(m for m in coverage.returns if m <= end):
        if month in coverage.duplicates:
            raise TsmomRefusal("duplicate_month", f"{coverage.symbol} {month}")
        check_wealth_return(coverage.symbol, month, coverage.returns[month])
        opening, closing = coverage.anchors.get(month, (None, None))
        if closing is not None and _stale(closing, month):
            raise TsmomRefusal("stale_month_end", f"{coverage.symbol} {month}: closing bar {closing}")
        if opening is not None and _stale(opening, add_months(month, -1)):
            raise TsmomRefusal("stale_month_end", f"{coverage.symbol} {month}: opening bar {opening}")


def date_of(month: Month) -> date:
    """The first day of ``month``."""
    return date(month[0], month[1], 1)


def check_contiguous(coverage: FundCoverage, first: Month, end: Month) -> None:
    """Every month from ``first`` to ``end`` present (spec §Validity case 2)."""
    missing = [m for m in month_range(first, end) if m not in coverage.returns]
    if missing:
        raise TsmomRefusal("missing_month", f"{coverage.symbol}: {len(missing)} missing, first {missing[0]}")


@dataclass(frozen=True)
class Census:
    start: Month
    end: Month
    #: What set E: ``"cap"`` when the coverage cap did, then every fund or comparator whose last month is E.
    limiting: tuple[str, ...]
    #: Per class: the funds eligible at the start formation.
    start_constituents: Mapping[str, tuple[str, ...]]
    coverage: Mapping[str, FundCoverage]


def build_census(funds: Mapping[str, FundCoverage], comparators: Mapping[str, FundCoverage]) -> Census:
    """Start, E and every refusal in spec §"Universe and eligibility"."""
    if set(funds) != set(FUNDS):
        raise TsmomRefusal("universe", f"funds {sorted(set(FUNDS) ^ set(funds))} differ from the spec's 30")
    if set(comparators) != set(COMPARATORS):
        raise TsmomRefusal("universe", f"comparators {sorted(set(COMPARATORS) ^ set(comparators))} differ")
    starts: dict[str, Month] = {}
    for name, members in CLASSES.items():
        starts[name] = min(funds[s].first_eligible for s in members)
    start = max(starts.values())
    # Funds and comparators are judged separately: SPY is both, and one map must not hide the other's entry.
    candidates = [(c.last_month, s) for s, c in funds.items()] + [(c.last_month, s) for s, c in comparators.items()]
    earliest = min(m for m, _ in candidates)
    end = min(COVERAGE_CAP, earliest)
    at_end = tuple(sorted({s for m, s in candidates if m == end}))
    limiting = (("cap",) if end == COVERAGE_CAP else ()) + at_end
    if end < add_months(start, 2):
        raise TsmomRefusal("too_short", f"E {end} < S + 2 with S {start}")
    for symbol, coverage in funds.items():
        if coverage.first_eligible > add_months(end, -2):
            raise TsmomRefusal("never_eligible", f"{symbol}: first eligible {coverage.first_eligible}, E {end}")
        check_contiguous(coverage, coverage.first_month, end)
        validate_through(coverage, end)
    for symbol, coverage in comparators.items():
        if coverage.first_month > add_months(start, 1):
            raise TsmomRefusal("comparator_late", f"{symbol}: first month {coverage.first_month}, start {start}")
        check_contiguous(coverage, add_months(start, 1), end)
        validate_through(coverage, end)
    constituents = {
        name: tuple(s for s in members if funds[s].first_eligible <= start) for name, members in CLASSES.items()
    }
    return Census(start, end, limiting, constituents, {**comparators, **funds})


# ---------------------------------------------------------------------------
# Distribution-stamp screen (spec §Returns, "Distribution capture is unverified")
# ---------------------------------------------------------------------------

#: Fewer full years than this prints "insufficient history".
STAMP_MIN_FULL_YEARS: Final = 3


@dataclass(frozen=True)
class StampYear:
    year: int
    count: int
    #: Bars in both January and December of the year, inside the months the panel uses.
    full: bool


@dataclass(frozen=True)
class StampAudit:
    symbol: str
    years: tuple[StampYear, ...]
    mode: int | None
    status: str
    flagged: tuple[int, ...]


def stamp_audit(
    symbol: str, events: Iterable[date], has_jan_and_dec: Mapping[int, bool], last_used: Month
) -> StampAudit:
    """A screen, not validation: full years whose distribution count differs from the full-year mode.

    Years run from the series' first bar year to ``last_used``'s year (the last Intrader month the panel uses).
    That last year is full only when ``last_used`` is a December. Partial years are listed and never flagged.
    """
    if not has_jan_and_dec:
        return StampAudit(symbol, (), None, "no bars", ())
    cutoff = add_months(last_used, 1)
    counts: dict[int, int] = {}
    for day in events:
        if month_of(day) < cutoff:
            counts[day.year] = counts.get(day.year, 0) + 1
    years = tuple(
        StampYear(
            year,
            counts.get(year, 0),
            has_jan_and_dec.get(year, False) and (year < last_used[0] or last_used[1] == 12),
        )
        for year in range(min(has_jan_and_dec), last_used[0] + 1)
    )
    full = [y.count for y in years if y.full]
    if len(full) < STAMP_MIN_FULL_YEARS:
        return StampAudit(symbol, years, None, "insufficient history", ())
    tally = statistics.multimode(full)
    if len(tally) > 1:
        return StampAudit(symbol, years, None, "multimodal", ())
    mode = tally[0]
    return StampAudit(symbol, years, mode, "ok", tuple(y.year for y in years if y.full and y.count != mode))


# ---------------------------------------------------------------------------
# Signals, volatility, slots (spec §"Source rules and every deviation")
# ---------------------------------------------------------------------------


def lookback(t: Month) -> list[Month]:
    return month_range(add_months(t, -(LOOKBACK_MONTHS - 1)), t)


def tsmom_signal(returns: Sequence[float], rf: Sequence[float]) -> bool:
    """Hold iff Π(1 + r) − Π(1 + RF) > 0 over the 12 months (strictly positive)."""
    if len(returns) != LOOKBACK_MONTHS or len(rf) != LOOKBACK_MONTHS:
        raise ValueError("TSMOM needs exactly 12 returns and 12 RF values")
    return math.prod(1.0 + r for r in returns) - math.prod(1.0 + x for x in rf) > 0.0


def c4_signal(returns: Sequence[float], rf: Sequence[float]) -> bool:
    """Huang, Li, Wang & Zhou (2020) eq. 17 / Table 9: hold iff Σ(r − RF) ≥ 0 over the whole prefix."""
    if len(returns) != len(rf) or not returns:
        raise ValueError("C4 needs a non-empty prefix of paired returns and RF values")
    return sum(r - x for r, x in zip(returns, rf, strict=True)) >= 0.0


def volatility(returns: Sequence[float]) -> float:
    """Sample standard deviation (n − 1) of the 12 monthly returns, × √12."""
    if len(returns) != LOOKBACK_MONTHS:
        raise ValueError("volatility needs exactly 12 returns")
    return statistics.stdev(returns) * math.sqrt(12.0)


def slots(vols: Mapping[str, float]) -> dict[str, float]:
    """wᵢ = (1/σᵢ) / Σⱼ(1/σⱼ); refuses a σ that is ≤ 0 or non-finite (no floor)."""
    for symbol, sigma in vols.items():
        if not math.isfinite(sigma) or sigma <= 0.0:
            raise TsmomRefusal("invalid_volatility", f"{symbol}: {sigma!r}")
    inverse = {s: 1.0 / sigma for s, sigma in sorted(vols.items())}
    total = sum(inverse.values())
    return {s: v / total for s, v in inverse.items()}


@dataclass(frozen=True)
class Formation:
    """What a book knows at formation month ``t``: eligible funds, signals and slots."""

    month: Month
    eligible: tuple[str, ...]
    tsmom: Mapping[str, bool]
    c4: Mapping[str, bool]
    vol: Mapping[str, float]
    slot: Mapping[str, float]


def form(
    t: Month,
    book: Sequence[str],
    coverage: Mapping[str, FundCoverage],
    rf: Mapping[Month, float],
) -> Formation:
    """The formation at ``t`` for a book of ``book``'s funds; slots normalise over the book's eligible funds."""
    eligible = tuple(sorted(s for s in book if coverage[s].first_eligible <= t))
    window = lookback(t)
    tsmom: dict[str, bool] = {}
    c4: dict[str, bool] = {}
    vol: dict[str, float] = {}
    for symbol in eligible:
        returns = coverage[symbol].returns
        rets = [_need(returns, symbol, m) for m in window]
        rfs = [_need(rf, "RF", m) for m in window]
        tsmom[symbol] = tsmom_signal(rets, rfs)
        prefix = month_range(coverage[symbol].first_month, t)
        c4[symbol] = c4_signal([_need(returns, symbol, m) for m in prefix], [_need(rf, "RF", m) for m in prefix])
        vol[symbol] = volatility(rets)
    return Formation(t, eligible, tsmom, c4, vol, slots(vol) if vol else {})


def _need(values: Mapping[Month, float], label: str, month: Month) -> float:
    if month not in values:
        raise TsmomRefusal("missing_month", f"{label} {month}")
    return check_wealth_return(label, month, values[month])


# ---------------------------------------------------------------------------
# Weight builders (spec §"Controls and baselines"). Each returns risky weights; cash is the remainder.
# ---------------------------------------------------------------------------


def signal_weights(formation: Formation, signals: Mapping[str, bool]) -> dict[str, float]:
    """Long-or-cash: a fund holds its slot iff its signal is on."""
    return {s: w for s, w in formation.slot.items() if signals[s]}


def tsmom_weights(formation: Formation) -> dict[str, float]:
    return signal_weights(formation, formation.tsmom)


def c1_weights(formation: Formation) -> dict[str, float]:
    return dict(formation.slot)


def c1x_weights(formation: Formation, invested_share: float) -> dict[str, float]:
    """C1's slots scaled to TSMOM's invested share at the same formation."""
    return {s: w * invested_share for s, w in formation.slot.items()}


def c4_weights(formation: Formation) -> dict[str, float]:
    return signal_weights(formation, formation.c4)


def basket_weights(formation: Formation, chosen: Iterable[str]) -> dict[str, float]:
    """C3: the chosen funds at their full-book slots; unselected slots are cash."""
    return {s: formation.slot[s] for s in sorted(chosen)}


def check_weights(weights: Mapping[str, float]) -> None:
    for symbol, w in weights.items():
        if not math.isfinite(w) or w < 0.0:
            raise TsmomRefusal("invalid_weight", f"{symbol}: {w!r}")
    if sum(weights.values()) > 1.0 + WEIGHT_TOLERANCE:
        raise TsmomRefusal("invalid_weight", f"risky weights sum to {sum(weights.values())!r} > 1")


# ---------------------------------------------------------------------------
# Samplers (spec §"Controls and baselines", "Samplers (exact)")
# ---------------------------------------------------------------------------


def c2_offsets(timing: Timing, book: str, draw: int, lengths: Mapping[str, int]) -> dict[str, int]:
    """One offset per fund in symbol order; n ≤ 1 takes 0 with no RNG call."""
    rng = random.Random(f"3620:C2:{timing}:{book}:{draw}")
    out: dict[str, int] = {}
    for symbol in sorted(lengths):
        n = lengths[symbol]
        out[symbol] = 0 if n <= 1 else rng.randrange(1, n)
    return out


def circular_shift(sequence: Sequence[bool], offset: int) -> list[bool]:
    """``shifted[i] = seq[(i − offset) % n]``."""
    n = len(sequence)
    return [sequence[(i - offset) % n] for i in range(n)]


def c3_baskets(
    timing: Timing, book: str, draw: int, formations: Sequence[tuple[Month, Sequence[str], int]]
) -> dict[Month, list[str]]:
    """Per evaluated formation in ascending order: ``rng.sample(sorted(eligible), k)``."""
    rng = random.Random(f"3620:C3:{timing}:{book}:{draw}")
    out: dict[Month, list[str]] = {}
    for month, eligible, k in sorted(formations, key=lambda f: f[0]):
        out[month] = rng.sample(sorted(eligible), k)
    return out


# ---------------------------------------------------------------------------
# Timing (spec §Path)
# ---------------------------------------------------------------------------


def fill_month(decision: Month, timing: Timing) -> Month:
    return add_months(decision, 1) if timing == "lagged" else decision


def evaluated_formations(start: Month, end: Month, timing: Timing) -> list[Month]:
    """Formations whose fill produces at least one held month: S..E−2 (lagged) or S..E−1 (same-close)."""
    last = add_months(end, -2 if timing == "lagged" else -1)
    return month_range(start, last)


# ---------------------------------------------------------------------------
# The exact cost solve and the monthly path (spec §Path)
# ---------------------------------------------------------------------------


def solve_cost(
    values: Mapping[str, float], cash: float, targets: Mapping[str, float], half: Mapping[str, float]
) -> float:
    """C = Σᵢ hᵢ·|wᵢ(V − C) − vᵢ|, solved on V-normalised values by fixed-point iteration from C = 0.

    The map is a contraction with factor ≤ maxᵢ hᵢ < 1. Returns the cost in the caller's units.
    """
    nav = sum(values.values()) + cash
    if not nav > 0.0:
        raise TsmomRefusal("nav", f"non-positive NAV {nav!r}")
    assets = sorted(set(values) | set(targets))
    for symbol in assets:
        if not 0.0 <= half[symbol] < 1.0:
            raise TsmomRefusal("half_spread", f"{symbol}: {half[symbol]!r}")
    v = {s: values.get(s, 0.0) / nav for s in assets}
    w = {s: targets.get(s, 0.0) for s in assets}
    cost = 0.0
    for _ in range(COST_MAX_ITERATIONS):
        following = sum(half[s] * abs(w[s] * (1.0 - cost) - v[s]) for s in assets)
        if abs(following - cost) <= COST_TOLERANCE:
            return following * nav
        cost = following
    raise TsmomRefusal("cost_nonconvergence", f"no fixed point within {COST_MAX_ITERATIONS} iterations")


@dataclass(frozen=True)
class Trade:
    month: Month
    symbol: str
    #: Signed notional as a fraction of the pre-trade NAV (positive = buy).
    notional: float
    half_spread: float


@dataclass
class PathResult:
    #: Reported months S+1..E (spec §Path, "Initialisation and reported months").
    returns: dict[Month, float]
    #: Per calendar year: Σ one-way turnover over the counted fills.
    turnover: dict[int, float]
    trades: list[Trade] = field(default_factory=list)
    #: Per fill month: the cost as a fraction of pre-trade NAV.
    cost: dict[Month, float] = field(default_factory=dict)
    final_nav: float = 1.0


def simulate(
    *,
    start: Month,
    end: Month,
    fills: Mapping[Month, Mapping[str, float]],
    returns: Mapping[str, Mapping[Month, float]],
    cash_return: Mapping[Month, float] | None,
    entry_half_spread: Callable[[str, Month], float],
    cost_multiplier: float,
) -> PathResult:
    """One continuous self-financing monthly path: capital 1.0 in cash at S's close, liquidated at E's close.

    ``fills`` maps a fill month (S..E−1) to the risky target weights applied to post-cost NAV at that close.
    ``cash_return`` is ``None`` for the 0% convention, else RF per month. A position's band is fixed at entry and
    cleared when it is sold in full; a re-entry takes a fresh band.
    """
    if any(m < start or m >= end for m in fills):
        raise TsmomRefusal("fill_month", f"fills must lie in [{start}, {end})")
    held: dict[str, float] = {}
    band: dict[str, float] = {}
    cash = 1.0
    previous = 1.0
    first_fill = min(fills) if fills else None
    result = PathResult(returns={}, turnover={})
    for month in month_range(start, end):
        if month > start:
            for symbol in held:
                held[symbol] *= 1.0 + _need(returns[symbol], symbol, month)
            if cash_return is not None:
                cash *= 1.0 + check_wealth_return("cash", month, cash_return[month])
        pre = sum(held.values()) + cash
        if month == end:
            targets: Mapping[str, float] | None = {}
        elif month in fills:
            check_weights(fills[month])
            targets = {sym: w for sym, w in fills[month].items() if w > 0.0}
        else:
            targets = None
        if targets is not None:
            base = {s: band[s] for s in held}
            for symbol in targets:
                if symbol not in held:
                    base[symbol] = entry_half_spread(symbol, month)
            half = {s: b * cost_multiplier for s, b in base.items()}
            cost = solve_cost(held, cash, targets, half)
            post = pre - cost
            traded = 0.0
            charged = 0.0
            new_held: dict[str, float] = {}
            for symbol in sorted(base):
                after = targets.get(symbol, 0.0) * post
                delta = after - held.get(symbol, 0.0)
                if delta != 0.0:
                    result.trades.append(Trade(month, symbol, delta / pre, base[symbol]))
                traded += abs(delta)
                charged += half[symbol] * abs(delta)
                if after > 0.0:
                    new_held[symbol] = after
                    band.setdefault(symbol, base[symbol])
                else:
                    band.pop(symbol, None)
            if abs(charged - cost) > WEIGHT_TOLERANCE * max(pre, 1.0):
                raise TsmomRefusal("cost_reconciliation", f"{month}: charged {charged!r} vs solved {cost!r}")
            cash = post - sum(new_held.values())
            if cash < -WEIGHT_TOLERANCE:
                raise TsmomRefusal("negative_cash", f"{month}: {cash!r}")
            cash = max(cash, 0.0)
            held = new_held
            result.cost[month] = cost / pre
            if month != first_fill and month != end:
                result.turnover[month[0]] = result.turnover.get(month[0], 0.0) + traded / 2.0 / pre
        nav = sum(held.values()) + cash
        if month > start:
            result.returns[month] = nav / previous - 1.0
            previous = nav
    result.final_nav = previous
    if abs(math.prod(1.0 + r for r in result.returns.values()) - result.final_nav) > WEIGHT_TOLERANCE:
        raise TsmomRefusal("path_reconciliation", "compounded returns do not equal the final NAV")
    return result


def baseline_fills(
    weights: Mapping[str, float], start: Month, end: Month, timing: Timing
) -> dict[Month, dict[str, float]]:
    """B1/B2: bought at the first fill, rebalanced to target at each later December fill before E."""
    first = fill_month(start, timing)
    months = [first] + [m for m in month_range(add_months(first, 1), add_months(end, -1)) if m[1] == 12]
    return {m: dict(weights) for m in months}


__all__ = [
    "ACCEPTED_VERDICTS",
    "BASELINES",
    "CLASSES",
    "COMPARATORS",
    "COVERAGE_CAP",
    "EQUITY_BOOK",
    "FUNDS",
    "POOLS",
    "Census",
    "Formation",
    "FundCoverage",
    "PathResult",
    "Trade",
    "TsmomRefusal",
    "baseline_fills",
    "basket_weights",
    "build_census",
    "c1_weights",
    "c1x_weights",
    "c2_offsets",
    "c3_baskets",
    "c4_signal",
    "c4_weights",
    "check_contiguous",
    "check_finite",
    "check_wealth_return",
    "check_weights",
    "circular_shift",
    "evaluated_formations",
    "fill_month",
    "form",
    "fund_coverage",
    "lookback",
    "month_range",
    "simulate",
    "slots",
    "solve_cost",
    "stamp_audit",
    "StampAudit",
    "StampYear",
    "tsmom_signal",
    "tsmom_weights",
    "validate_through",
    "volatility",
]
