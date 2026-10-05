"""#3609 step 1: the panel's price characteristics, daily screen and holding returns, per series.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Price characteristics and daily data",
§"Returns and holdings" and §"Census" (liquidity tercile). Pure: the builder streams one series' bars at a time
and this module turns them into one ``FormationPrices`` per formation the series is priced at.

Monthly returns use ``total_return_reader.monthly_returns`` over the same month-end semantics (the last usable
bar of each calendar month). Every stage-A month is before ``SWITCH_MONTH``, so the reader's splice would take
the Intrader row anyway; calling the pure rule on a bounded read keeps the hold-out bound the reader's own
unbounded load does not have.

Spec readings adopted by Amendment 2 (§"Daily screen"), first implemented here by slice 3c:
- the ``adj_close/close`` ratio "moves more than 50%" when ``|ln(ratio_q / ratio_p)| > ln 1.5``, between
  consecutive admitted session bars, excused by a ``split_factor`` or ``dividend`` stamp dated in ``(p, q]``.
  An excused move the stamps' split factors do not explain within the same tolerance is counted, not screened;
- "a window containing a flagged bar" means a window containing a flagged pair's LATER bar: a pair whose later bar
  is after s(M) is not observable at s(M), so it flags s(M) in the census but does not screen the formation.

Readings that are ours, not the spec's text:
- the liquidity and volatility windows end at s(M) inclusive;
- a holding is ``observed`` when the holding month's last usable bar is on or after its last SPY session.
"""

from __future__ import annotations

import math
from bisect import bisect_left
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Final

import numpy as np

from app.services import total_return_reader as trr
from app.services.factor_panel import PanelError, SplitStamp
from app.services.series_termination import TerminationClass, terminal_value_fraction
from app.services.strategy_result import AmbiguityArm

ARMS: Final[tuple[AmbiguityArm, ...]] = ("best_case", "worst_case")
#: §"Daily screen".
SCREEN_RETURN_LOW: Final = -0.9
SCREEN_RETURN_HIGH: Final = 3.0
SCREEN_RATIO_MOVE: Final = 0.5
#: §"`ret_12_1`": all 11 of months t-11 .. t-1 (ours; JKP needs "enough").
MOMENTUM_MONTHS: Final = 11
#: §"`rvol_21d`": 21 sessions ending at s(M), at least 15 returns (JKP's residual minimum, by analogy).
RVOL_SESSIONS: Final = 21
RVOL_MIN_RETURNS: Final = 15
#: §"Census": mean ``close x volume`` over 126 sessions, at least 63 bars, else unclassified.
LIQUIDITY_SESSIONS: Final = 126
LIQUIDITY_MIN_BARS: Final = 63
#: §"Daily-monthly reconciliation".
RECONCILIATION_TOLERANCE: Final = 1e-6
#: The longest lookback, in sessions before s(M), that must be loaded.
LOOKBACK_SESSIONS: Final = max(LIQUIDITY_SESSIONS, RVOL_SESSIONS + 1)


@dataclass(frozen=True, slots=True)
class DailyBar:
    bar_date: date
    close: float
    adj_close: float
    volume: int | None
    #: ``split_factor`` not 1 or a positive ``dividend`` on this bar.
    stamped: bool
    #: Inside the series' quarantine coverage, ``return_usable``, finite positive ``close`` and ``adj_close``.
    usable: bool


class PriceMissing(StrEnum):
    INSUFFICIENT_MONTHS = "insufficient_months"
    SCREEN_FLAGGED = "screen_flagged"
    INSUFFICIENT_RETURNS = "insufficient_returns"


class HoldingStatus(StrEnum):
    OBSERVED = "observed"
    TERMINAL = "terminal"
    COVERAGE_EXIT = "coverage_exit"


class DailyMonthly(StrEnum):
    """Compounded daily returns against the reader's monthly return for the holding month."""

    AGREE = "agree"
    #: A session in the month has no daily return, or a month-end bar is not a SPY session.
    GAP = "gap"
    UNEXPLAINED = "unexplained"
    NO_MONTHLY = "no_monthly"


@dataclass(frozen=True, slots=True)
class PriceCharacteristic:
    value: float | None
    observations: int
    missing: PriceMissing | None


@dataclass(frozen=True, slots=True)
class Holding:
    status: HoldingStatus
    #: s(M) to the holding month's last usable bar; 0 when it has none.
    period_return: float
    end_bar: date | None
    by_arm: Mapping[AmbiguityArm, float]


@dataclass(frozen=True, slots=True)
class FormationPrices:
    adj_close: float
    ret_12_1: PriceCharacteristic
    rvol_21d: PriceCharacteristic
    #: Mean ``close x volume``; ``None`` when unclassified.
    dollar_volume: float | None
    dollar_volume_bars: int
    #: The liquidity window holds a screened pair's later bar (Amendment 2 counts check-3 failures with one).
    liquidity_screened: bool
    holding: Holding
    #: Month t's last usable bar is after s(M), so the reader's month t+1 row starts later than the holding.
    month_end_after_decision: bool
    daily_monthly: DailyMonthly


@dataclass(frozen=True)
class SeriesPrices:
    by_formation: Mapping[date, FormationPrices]
    #: Flagged bars per calendar year (§"Daily screen": printed per year, nothing deleted).
    flags_by_year: Counter[int]
    #: Stamp-excused ratio moves the stamps' split factors do not explain, per calendar year (Amendment 2).
    excused_unexplained_by_year: Counter[int] = field(default_factory=Counter)


@dataclass(frozen=True)
class SessionGrid:
    """The SPY sessions, the RF on each, and each formation's decision session index."""

    sessions: Sequence[date]
    rf: np.ndarray
    decisions: Mapping[date, int]

    @classmethod
    def build(cls, sessions: Sequence[date], rf: Mapping[date, float], decisions: Mapping[date, date]) -> SessionGrid:
        position = {day: i for i, day in enumerate(sessions)}
        index: dict[date, int] = {}
        for formation, session in decisions.items():
            i = position.get(session)
            if i is None:
                raise PanelError(f"decision session {session} is not a loaded SPY session")
            if i < LOOKBACK_SESSIONS:
                raise PanelError(f"sessions before {session} are not loaded far enough back for the lookbacks")
            index[formation] = i
        rf_array = np.array([rf.get(day, math.nan) for day in sessions], dtype=float)
        return cls(sessions, rf_array, index)


def _holding(
    formation: date,
    start_adj: float,
    month_ends: Mapping[trr.Month, trr.MonthEnd],
    holding_last_session: date,
    termination: tuple[TerminationClass, date] | None,
) -> Holding:
    """§"Returns and holdings": weights fixed at s(M); the month-(t+1) return and its status."""
    held = trr.add_months(trr.month_of(formation), 1)
    end = month_ends.get(held)
    period_return = 0.0 if end is None else end.adj_close / start_adj - 1.0
    end_bar = None if end is None else end.bar_date
    flat: dict[AmbiguityArm, float] = {arm: period_return for arm in ARMS}
    if end_bar is not None and end_bar >= holding_last_session:
        return Holding(HoldingStatus.OBSERVED, period_return, end_bar, flat)
    if termination is not None and trr.month_of(termination[1]) <= held:
        by_arm: dict[AmbiguityArm, float] = {
            arm: (1.0 + period_return) * terminal_value_fraction(termination[0], arm) - 1.0 for arm in ARMS
        }
        return Holding(HoldingStatus.TERMINAL, period_return, end_bar, by_arm)
    return Holding(HoldingStatus.COVERAGE_EXIT, period_return, end_bar, flat)


def series_prices(
    bars: Sequence[DailyBar],
    grid: SessionGrid,
    *,
    holding_last_session: Mapping[date, date],
    termination: tuple[TerminationClass, date] | None,
    split_stamps: Sequence[SplitStamp] = (),
) -> SeriesPrices:
    """Every price quantity for one series at each formation where it has a usable bar on s(M).

    ``bars`` ascending; ``holding_last_session`` maps each formation to its holding month's last SPY session;
    ``termination`` is the series' termination class and stored last bar, ``None`` for a live series.
    """
    sessions = grid.sessions
    n = len(sessions)
    position = {day: i for i, day in enumerate(sessions)}
    adj = np.full(n, math.nan)
    close = np.full(n, math.nan)
    dollar = np.full(n, math.nan)
    stamps = np.zeros(n + 1, dtype=np.int64)
    month_ends: dict[trr.Month, trr.MonthEnd] = {}
    for bar in bars:
        if bar.stamped:
            # A stamp on any bar, session or not, usable or not, counts at the first session on or after it.
            stamps[bisect_left(sessions, bar.bar_date)] += 1
        if not bar.usable:
            continue
        month_ends[trr.month_of(bar.bar_date)] = trr.MonthEnd(bar.bar_date, bar.adj_close, bar.close)
        i = position.get(bar.bar_date)
        if i is not None:
            adj[i], close[i] = bar.adj_close, bar.close
            if bar.volume is not None:
                dollar[i] = bar.close * bar.volume
    stamps_through = np.cumsum(stamps)
    log_split = np.zeros(n + 1)
    for stamp in split_stamps:
        log_split[bisect_left(sessions, stamp.day)] += math.log(float(stamp.factor))
    log_split_through = np.cumsum(log_split)

    # Daily returns exist only between admitted bars on adjacent sessions (NaN propagates otherwise).
    daily = np.full(n, math.nan)
    daily[1:] = adj[1:] / adj[:-1] - 1.0
    # ``flagged`` marks both bars of a screened pair (the census); ``screened_at`` marks the pair's LATER bar, the
    # session the screen becomes observable. A window ending at s(M) is screened only by pairs ending inside it,
    # so a jump on the first session after s(M) cannot reach back into the formation (no look-ahead).
    flagged = np.zeros(n, dtype=bool)
    screened_at = np.zeros(n, dtype=bool)
    with np.errstate(invalid="ignore"):
        extreme = (daily < SCREEN_RETURN_LOW) | (daily > SCREEN_RETURN_HIGH)
    flagged |= extreme
    flagged[:-1] |= extreme[1:]
    screened_at |= extreme
    admitted = np.flatnonzero(~np.isnan(adj))
    if len(admitted) > 1:
        ratio = adj[admitted] / close[admitted]
        moved = np.abs(np.log(ratio[1:] / ratio[:-1])) > math.log(1.0 + SCREEN_RATIO_MOVE)
        unstamped = stamps_through[admitted[1:]] - stamps_through[admitted[:-1]] == 0
        jump = moved & unstamped
        flagged[admitted[1:][jump]] = True
        flagged[admitted[:-1][jump]] = True
        screened_at[admitted[1:][jump]] = True
        # Intrader stamps new shares per old share, so a stamp f multiplies ``adj_close/close`` by f: signed.
        factor = log_split_through[admitted[1:]] - log_split_through[admitted[:-1]]
        unexplained = np.abs(np.log(ratio[1:] / ratio[:-1]) - factor) > math.log(1.0 + SCREEN_RATIO_MOVE)
        excused_unexplained = Counter(sessions[int(i)].year for i in admitted[1:][moved & ~unstamped & unexplained])
    else:
        excused_unexplained = Counter()
    flags_by_year = Counter(sessions[int(i)].year for i in np.flatnonzero(flagged))

    monthly = trr.monthly_returns(month_ends, field="adj_close", through=trr.month_of(sessions[-1]))
    excess = daily - grid.rf
    out: dict[date, FormationPrices] = {}
    for formation, k in grid.decisions.items():
        if math.isnan(adj[k]):
            continue
        month = trr.month_of(formation)

        past = [monthly.get(trr.add_months(month, -lag)) for lag in range(1, MOMENTUM_MONTHS + 1)]
        present = [r.value for r in past if r is not None]
        if len(present) == MOMENTUM_MONTHS:
            momentum = PriceCharacteristic(math.prod(1.0 + r for r in present) - 1.0, len(present), None)
        else:
            momentum = PriceCharacteristic(None, len(present), PriceMissing.INSUFFICIENT_MONTHS)

        window = slice(k - RVOL_SESSIONS + 1, k + 1)
        observed = ~np.isnan(daily[window])
        if np.isnan(grid.rf[window][observed]).any():
            raise PanelError(f"French daily RF missing inside the rvol_21d window ending {sessions[k]}")
        returns = excess[window][observed]
        if screened_at[window].any():
            volatility = PriceCharacteristic(None, len(returns), PriceMissing.SCREEN_FLAGGED)
        elif len(returns) < RVOL_MIN_RETURNS:
            volatility = PriceCharacteristic(None, len(returns), PriceMissing.INSUFFICIENT_RETURNS)
        else:
            volatility = PriceCharacteristic(float(np.std(returns, ddof=1)), len(returns), None)

        liquidity_window = slice(max(k - LIQUIDITY_SESSIONS + 1, 0), k + 1)
        traded = dollar[k - LIQUIDITY_SESSIONS + 1 : k + 1]
        traded = traded[~np.isnan(traded)]
        dollar_volume = float(traded.mean()) if len(traded) >= LIQUIDITY_MIN_BARS else None

        held = trr.add_months(month, 1)
        row = monthly.get(held)
        if row is None:
            reconciled = DailyMonthly.NO_MONTHLY
        else:
            start, end = position.get(row.start_bar), position.get(row.end_bar)
            segment = None if start is None or end is None else daily[start + 1 : end + 1]
            if segment is None or np.isnan(segment).any():
                reconciled = DailyMonthly.GAP
            else:
                compounded = float(np.prod(1.0 + segment)) - 1.0
                agree = abs(compounded - row.value) <= RECONCILIATION_TOLERANCE
                reconciled = DailyMonthly.AGREE if agree else DailyMonthly.UNEXPLAINED

        end_of_month = month_ends[month]
        out[formation] = FormationPrices(
            adj_close=float(adj[k]),
            ret_12_1=momentum,
            rvol_21d=volatility,
            dollar_volume=dollar_volume,
            dollar_volume_bars=len(traded),
            liquidity_screened=bool(screened_at[liquidity_window].any()),
            holding=_holding(formation, float(adj[k]), month_ends, holding_last_session[formation], termination),
            month_end_after_decision=end_of_month.bar_date > sessions[k],
            daily_monthly=reconciled,
        )
    return SeriesPrices(out, flags_by_year, excused_unexplained)


def liquidity_terciles(dollar_volume: Mapping[int, float | None], name_key: Mapping[int, int]) -> dict[int, int]:
    """Nearest-rank terciles (1 = least liquid) among one formation's classified names, ties by ``name_key``.

    Keys are series ids; an unclassified name (``None``) gets no tercile.
    """
    ranked = sorted((v, name_key[sid], sid) for sid, v in dollar_volume.items() if v is not None)
    n = len(ranked)
    cut1, cut2 = math.ceil(n / 3), math.ceil(2 * n / 3)
    return {sid: 1 if rank <= cut1 else 2 if rank <= cut2 else 3 for rank, (_, _, sid) in enumerate(ranked, start=1)}


def me_discontinuity(me_ratio: float, adj_ratio: float, *, low: float = 0.8, high: float = 1.25) -> float | None:
    """The ME ratio over the ``adj_close`` ratio when it falls outside ``[low, high]``, else ``None``.

    §"Market equity": the discontinuity census and the split reconciliation use the same band.
    """
    relative = me_ratio / adj_ratio
    return None if low <= relative <= high else relative
