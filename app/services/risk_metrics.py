"""
Risk-metrics pure-compute service (``risk_v1``).

Computes long-horizon risk metrics for ONE instrument over ONE window from a
price series, plus benchmark-relative metrics against SPY.

Design (mirrors ``app/services/return_attribution.py``):
  - Pure math. No DB, no I/O, no connection. The caller loads price series and
    persists results.
  - ``Decimal(str(x))`` coercion at every numeric boundary; ``ZERO = Decimal("0")``.
  - Frozen dataclasses for results. Module-level version/window constants.
  - One sanctioned FLOAT ISLAND: skew, excess kurtosis, and the var_5 percentile
    are computed in ``float`` via numpy (no scipy) and re-quantized to Decimal at
    the boundary with ``Decimal(str(round(v, 8)))``. Everything else stays
    end-to-end Decimal.

Math contracts:
  - Returns are SIMPLE: ``r = close[i]/close[i-1] - 1`` between *consecutive
    surviving* rows. A close is valid iff finite AND ``> 0``. An invalid row
    breaks the chain — NO gap-spanning synthetic return. The return is keyed to
    the LATER close's date.
  - Volatility annualizes the sample (n-1) std by ``sqrt(252)``.
  - CAGR is CALENDAR-time: ``(final/first)^(365/calendar_days) - 1``.
  - Drawdown uses a running peak: ``dd[i] = close[i]/peak[i] - 1`` (≤ 0).
  - Beta INTERSECTS dates between the instrument and SPY return series — never
    positional-zip (that mis-pairs across holiday gaps).
  - ``var_5`` is SIGNED (a loss is negative) — persisted as-is.

Issue #591 PR-B, Task B1.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Literal, get_args

import numpy as np
import psycopg
from psycopg import sql

from app.db.snapshot import snapshot_read
from app.workers.scheduler import BENCHMARK_SYMBOLS

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

RISK_METRICS_VERSION = "risk_v1"
TRADING_DAYS = 252
ZERO = Decimal("0")

# Boundaries are counted in RETURNS, not closes.
MIN_RETURNS_VOL_BETA = 60
MIN_RETURNS_ANNUALIZED = 252
MIN_OBS_MOMENTS = 250

# Calmar guard: a max-drawdown magnitude below this is treated as "no drawdown".
CALMAR_DD_EPSILON = Decimal("1e-9")

WINDOW_KEYS: tuple[str, ...] = ("1y", "3y", "full")

# Calendar-day lookback per window. ``full`` has NO lower bound (all closes).
# A close survives the slice iff its date is within ``lookback`` days of the
# as_of_date (and ≤ as_of_date, which the loaded series already guarantees).
WINDOW_LOOKBACK_DAYS: dict[str, int | None] = {"1y": 365, "3y": 1095, "full": None}

# The benchmark for beta / excess metrics. Resolved to an instrument_id at run
# time (primary-listing tiebreak) the same way the candle-refresh scope does.
SPY_SYMBOL = "SPY"

# Chunk size for the batched current-table rebuild (~15k total rows / run).
_REBUILD_BATCH = 500

RiskStatus = Literal[
    "ok",
    "insufficient_history",
    "partial_window",
    "benchmark_missing",
    "benchmark_insufficient_history",
    "invalid_price_chain",
    "stale",
]
RISK_STATUSES: frozenset[str] = frozenset(get_args(RiskStatus))

# Trailing-return lookback windows, in calendar days.
TRAILING_LOOKBACK_DAYS: dict[str, int] = {
    "1m": 30,
    "3m": 91,
    "6m": 182,
    "1y": 365,
}

# A (date, close) pair. close may be any numeric / float (NaN allowed upstream).
PricePoint = tuple[date, object]
# A (date, return) pair — return is always a Decimal.
ReturnPoint = tuple[date, Decimal]

_SQRT_TRADING_DAYS = Decimal(TRADING_DAYS).sqrt()


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DrawdownResult:
    """Running-peak drawdown over a close series."""

    max_drawdown: Decimal | None
    current_drawdown: Decimal | None
    peak_date: date | None
    trough_date: date | None


@dataclass(frozen=True)
class BetaResult:
    """OLS regression of instrument returns on benchmark returns (single regressor)."""

    beta: Decimal | None
    r2: Decimal | None
    n_obs: int


@dataclass(frozen=True)
class DistributionResult:
    """Return-distribution moments and tail stats (float island, quantized to Decimal)."""

    skew: Decimal | None
    excess_kurtosis: Decimal | None
    var_5: Decimal | None
    worst_day: Decimal | None
    best_day: Decimal | None
    n_obs: int
    low_sample: bool


@dataclass(frozen=True)
class WindowMetrics:
    """All risk metrics for ONE instrument over ONE window."""

    window_key: str
    n_returns: int
    annualized_vol: Decimal | None
    vol_status: RiskStatus
    cagr: Decimal | None
    cagr_status: RiskStatus
    max_drawdown: Decimal | None
    current_drawdown: Decimal | None
    drawdown_status: RiskStatus
    calmar: Decimal | None
    calmar_status: RiskStatus
    beta: Decimal | None
    r2: Decimal | None
    beta_n_obs: int
    beta_status: RiskStatus
    excess_cagr: Decimal | None
    excess_cagr_status: RiskStatus
    distribution: DistributionResult | None
    distribution_status: RiskStatus
    # Trailing-return scalars (calendar-lookback from as_of_date). These are
    # window-INDEPENDENT — identical across the 1y/3y/full rows — and computed
    # on the FULL series, NOT the window slice. excess_* are null without SPY.
    trailing_1m: Decimal | None
    trailing_3m: Decimal | None
    trailing_6m: Decimal | None
    trailing_1y: Decimal | None
    excess_trailing_1m: Decimal | None
    excess_trailing_3m: Decimal | None
    excess_trailing_6m: Decimal | None
    excess_trailing_1y: Decimal | None
    trailing_status: RiskStatus


# ---------------------------------------------------------------------------
# Validity + returns
# ---------------------------------------------------------------------------


def _valid_close(value: object) -> Decimal | None:
    """Return a positive finite close as Decimal, else None.

    A close is valid iff it is finite AND strictly greater than zero.
    NaN / inf / zero / negative are all invalid (and break the return chain).
    """
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except ArithmeticError, ValueError, TypeError:
        return None
    if not d.is_finite():
        return None
    if d <= ZERO:
        return None
    return d


def _count_invalid_closes(closes: Sequence[PricePoint]) -> int:
    """Count raw rows whose close is invalid (None / non-finite / ≤ 0).

    A single invalid close breaks the return chain (no gap-spanning return) and
    therefore costs up to two returns (the one into it and the one out of it).
    The caller uses this count to distinguish *invalid-driven* shortfalls
    (``invalid_price_chain``) from genuinely-short clean history
    (``insufficient_history`` / ``partial_window``).
    """
    return sum(1 for _, raw in closes if _valid_close(raw) is None)


def simple_returns(closes: Sequence[PricePoint]) -> list[ReturnPoint]:
    """Simple returns between *consecutive surviving* closes.

    ``r = close[i]/close[i-1] - 1`` only when BOTH the current and the
    immediately-preceding row are valid. An invalid row breaks the chain — no
    synthetic return spans the gap. The return is keyed to the LATER close's
    date.
    """
    out: list[ReturnPoint] = []
    prev: Decimal | None = None
    for d, raw in closes:
        cur = _valid_close(raw)
        if cur is None:
            prev = None  # break the chain; no gap-spanning return
            continue
        if prev is not None:
            out.append((d, cur / prev - Decimal(1)))
        prev = cur
    return out


# ---------------------------------------------------------------------------
# Volatility (shared sample std)
# ---------------------------------------------------------------------------


def _sample_std(returns: Sequence[Decimal]) -> Decimal | None:
    """Sample standard deviation (n-1 denominator). None for n < 2.

    SHARED by ``annualized_vol`` and ``distribution`` so the std is identical.
    """
    n = len(returns)
    if n < 2:
        return None
    mean = sum(returns, ZERO) / Decimal(n)
    sse = sum(((r - mean) ** 2 for r in returns), ZERO)
    variance = sse / Decimal(n - 1)
    return variance.sqrt()


def annualized_vol(returns: Sequence[Decimal]) -> Decimal | None:
    """Annualized volatility: sample std × sqrt(252). None for < 2 returns."""
    std = _sample_std(returns)
    if std is None:
        return None
    return std * _SQRT_TRADING_DAYS


# ---------------------------------------------------------------------------
# CAGR (calendar-time)
# ---------------------------------------------------------------------------


def cagr(closes: Sequence[PricePoint]) -> Decimal | None:
    """CALENDAR-time CAGR over the valid sub-chain of a close series.

    ``(final/first)^(365/calendar_days) - 1`` where
    ``calendar_days = (last_valid_date - first_valid_date).days``.

    Uses Decimal ``.ln()``/``.exp()`` for the fractional power. Returns None
    when there are fewer than two valid closes, when first close is non-positive,
    or when ``calendar_days == 0``.
    """
    valid: list[tuple[date, Decimal]] = []
    for d, raw in closes:
        c = _valid_close(raw)
        if c is not None:
            valid.append((d, c))
    if len(valid) < 2:
        return None
    first_date, first_close = valid[0]
    last_date, last_close = valid[-1]
    calendar_days = (last_date - first_date).days
    if calendar_days == 0:
        return None
    ratio = last_close / first_close
    exponent = Decimal(365) / Decimal(calendar_days)
    # ratio ** exponent via exp(exponent * ln(ratio)); ratio > 0 guaranteed.
    return (exponent * ratio.ln()).exp() - Decimal(1)


# ---------------------------------------------------------------------------
# Drawdown
# ---------------------------------------------------------------------------


def drawdown(closes: Sequence[PricePoint]) -> DrawdownResult:
    """Running-peak drawdown over the valid sub-chain.

    ``dd[i] = close[i]/peak[i] - 1`` (always ≤ 0). ``max_drawdown`` is the
    minimum (most negative) dd; ``current_drawdown`` is the last dd. Reports the
    peak date in effect at the trough and the trough date.
    """
    valid: list[tuple[date, Decimal]] = []
    for d, raw in closes:
        c = _valid_close(raw)
        if c is not None:
            valid.append((d, c))
    if not valid:
        return DrawdownResult(None, None, None, None)

    peak_close = valid[0][1]
    peak_date = valid[0][0]
    max_dd = ZERO
    max_dd_peak_date = valid[0][0]
    trough_date = valid[0][0]
    last_dd = ZERO

    for d, c in valid:
        if c > peak_close:
            peak_close = c
            peak_date = d
        dd = c / peak_close - Decimal(1)
        last_dd = dd
        if dd < max_dd:
            max_dd = dd
            max_dd_peak_date = peak_date
            trough_date = d

    return DrawdownResult(
        max_drawdown=max_dd,
        current_drawdown=last_dd,
        peak_date=max_dd_peak_date,
        trough_date=trough_date,
    )


# ---------------------------------------------------------------------------
# Calmar
# ---------------------------------------------------------------------------


def calmar(annualized_return: Decimal, max_drawdown: Decimal) -> Decimal | None:
    """``annualized_return / abs(max_drawdown)``.

    None when ``abs(max_drawdown) < CALMAR_DD_EPSILON`` (avoids div-by-near-zero).
    """
    dd_mag = abs(max_drawdown)
    if dd_mag < CALMAR_DD_EPSILON:
        return None
    return annualized_return / dd_mag


# ---------------------------------------------------------------------------
# OLS beta (date-intersection)
# ---------------------------------------------------------------------------


def ols_beta(
    inst_returns: Sequence[ReturnPoint],
    bench_returns: Sequence[ReturnPoint],
) -> BetaResult:
    """OLS beta of instrument returns on benchmark returns, single regressor.

    Builds ``{date: return}`` for both and regresses on the sorted INTERSECTION
    of dates (never positional-zip — that silently mis-pairs across holiday
    gaps). The aligned window therefore starts at ``max(first inst return date,
    first bench return date)`` implicitly.

    ``beta = cov(i, m) / var(m)``; ``r2 = corr(i, m)^2`` (closed form). Both
    covariance and variance use n-1 denominators (they cancel in beta but must
    match). Guards:
      - fewer than 2 aligned pairs → beta None, r2 None
      - ``var(m) == 0`` → beta None
      - ``var(i) == 0 or var(m) == 0`` → r2 None
    """
    inst_map = dict(inst_returns)
    bench_map = dict(bench_returns)
    shared = sorted(set(inst_map) & set(bench_map))
    n = len(shared)
    if n < 2:
        return BetaResult(beta=None, r2=None, n_obs=n)

    i_vals = [inst_map[d] for d in shared]
    m_vals = [bench_map[d] for d in shared]
    nd = Decimal(n)
    i_mean = sum(i_vals, ZERO) / nd
    m_mean = sum(m_vals, ZERO) / nd

    denom = Decimal(n - 1)
    cov = sum(((i - i_mean) * (m - m_mean) for i, m in zip(i_vals, m_vals, strict=True)), ZERO) / denom
    var_m = sum(((m - m_mean) ** 2 for m in m_vals), ZERO) / denom
    var_i = sum(((i - i_mean) ** 2 for i in i_vals), ZERO) / denom

    beta: Decimal | None = None if var_m == ZERO else cov / var_m

    r2: Decimal | None
    if var_i == ZERO or var_m == ZERO:
        r2 = None
    else:
        corr = cov / (var_i.sqrt() * var_m.sqrt())
        r2 = corr * corr

    return BetaResult(beta=beta, r2=r2, n_obs=n)


# ---------------------------------------------------------------------------
# Distribution (FLOAT ISLAND)
# ---------------------------------------------------------------------------


def _quantize8(value: float) -> Decimal:
    """Quantize a float-island scalar to 8dp Decimal at the persistence boundary."""
    return Decimal(str(round(value, 8)))


def distribution(returns: Sequence[Decimal]) -> DistributionResult:
    """Return-distribution moments + tail stats.

    FLOAT ISLAND: skew, excess kurtosis and the var_5 percentile are computed in
    float via numpy (two-pass: mean first, then central moments), then quantized
    to 8dp Decimal at the boundary. ``worst_day``/``best_day`` stay Decimal.

    - ``skew``: biased moment form ``m3 / m2^1.5``.
    - ``excess_kurtosis``: Fisher, biased moment form ``m4 / m2^2 - 3``.
    - ``var_5``: type-7 linear-interpolation 5th percentile (``h = 0.05*(n-1)``),
      SIGNED (a left-tail loss is negative — persisted as-is).
    - constant series (variance 0) → skew / kurt None (no div-by-zero).
    - ``low_sample`` True when ``n_obs < MIN_OBS_MOMENTS``.
    """
    n = len(returns)
    if n == 0:
        return DistributionResult(None, None, None, None, None, 0, True)

    worst = min(returns)
    best = max(returns)
    low_sample = n < MIN_OBS_MOMENTS

    arr = np.array([float(r) for r in returns], dtype=np.float64)

    # type-7 5th percentile (numpy default method='linear' == type-7). SIGNED.
    var_5_f = float(np.percentile(arr, 5.0, method="linear"))

    skew: Decimal | None = None
    excess_kurt: Decimal | None = None
    # Detect a constant series EXACTLY in the Decimal domain (the source of
    # truth). Float central moments suffer roundoff (a constant series yields
    # m2 ~1e-36, not 0) and would produce nonsense skew/kurt. When variance is
    # exactly zero the moments are undefined → None (no div-by-zero).
    if n >= 2 and worst != best:
        mean = float(arr.mean())
        centered = arr - mean
        m2 = float(np.mean(centered**2))
        if m2 > 0.0:
            m3 = float(np.mean(centered**3))
            m4 = float(np.mean(centered**4))
            skew = _quantize8(m3 / (m2**1.5))
            excess_kurt = _quantize8(m4 / (m2**2) - 3.0)

    return DistributionResult(
        skew=skew,
        excess_kurtosis=excess_kurt,
        var_5=_quantize8(var_5_f),
        worst_day=worst,
        best_day=best,
        n_obs=n,
        low_sample=low_sample,
    )


# ---------------------------------------------------------------------------
# Trailing returns
# ---------------------------------------------------------------------------


def _close_at(closes: Sequence[PricePoint], as_of: date) -> Decimal | None:
    """Valid close exactly at ``as_of`` if present, else the nearest valid close before it."""
    best: Decimal | None = None
    for d, raw in closes:
        if d > as_of:
            continue
        c = _valid_close(raw)
        if c is not None:
            best = c  # closes assumed ascending; last ≤ as_of wins
    return best


def trailing_return(
    closes: Sequence[PricePoint],
    as_of: date,
    lookback_days: int,
) -> Decimal | None:
    """``close[as_of] / close[nearest valid ≤ as_of - lookback_days] - 1``.

    None if there is no valid close at/after ``as_of`` or no valid close at or
    before ``as_of - lookback_days``.
    """
    end = _close_at(closes, as_of)
    if end is None:
        return None
    start_cutoff = as_of - timedelta(days=lookback_days)
    start = _close_at(closes, start_cutoff)
    if start is None or start == ZERO:
        return None
    return end / start - Decimal(1)


def excess_trailing_return(
    inst_closes: Sequence[PricePoint],
    spy_closes: Sequence[PricePoint],
    as_of: date,
    lookback_days: int,
) -> tuple[Decimal | None, RiskStatus]:
    """Instrument trailing return minus SPY trailing return over the same window.

    ``benchmark_missing`` when there is no SPY series at all. Otherwise computes
    the difference; None value if either side lacks far-enough history.
    """
    if not spy_closes:
        return None, "benchmark_missing"
    inst = trailing_return(inst_closes, as_of, lookback_days)
    spy = trailing_return(spy_closes, as_of, lookback_days)
    if inst is None:
        return None, "insufficient_history"
    if spy is None:
        return None, "benchmark_insufficient_history"
    return inst - spy, "ok"


# ---------------------------------------------------------------------------
# Excess CAGR (first-class)
# ---------------------------------------------------------------------------


def _aligned_window(
    inst_closes: Sequence[PricePoint],
    spy_closes: Sequence[PricePoint],
) -> tuple[list[PricePoint], list[PricePoint]]:
    """Restrict both series to the overlapping date span (intersection of extents).

    Window starts at ``max(first valid inst date, first valid spy date)`` and
    ends at ``min(last valid inst date, last valid spy date)``.
    """
    inst_valid = [(d, raw) for d, raw in inst_closes if _valid_close(raw) is not None]
    spy_valid = [(d, raw) for d, raw in spy_closes if _valid_close(raw) is not None]
    if not inst_valid or not spy_valid:
        return [], []
    start = max(inst_valid[0][0], spy_valid[0][0])
    end = min(inst_valid[-1][0], spy_valid[-1][0])
    if start > end:
        return [], []
    inst_w = [(d, raw) for d, raw in inst_valid if start <= d <= end]
    spy_w = [(d, raw) for d, raw in spy_valid if start <= d <= end]
    return inst_w, spy_w


def excess_cagr(
    inst_closes: Sequence[PricePoint],
    spy_closes: Sequence[PricePoint],
    window: str,  # noqa: ARG001 — label only; the caller pre-slices both series to the window
) -> tuple[Decimal | None, RiskStatus]:
    """``cagr(inst over aligned window) - cagr(SPY over aligned window)``.

    Window slicing happens UPSTREAM: :func:`compute_instrument_risk` slices both
    series to ``window_key`` before calling this, so the inst/spy passed here are
    already window-bounded and this just intersects their date extents. ``window``
    is retained as a label for caller symmetry.

    ``benchmark_missing`` when SPY is absent; ``benchmark_insufficient_history``
    when the aligned overlap is too short to compute either CAGR.
    """
    if not spy_closes:
        return None, "benchmark_missing"
    inst_w, spy_w = _aligned_window(inst_closes, spy_closes)
    if not inst_w or not spy_w:
        return None, "benchmark_insufficient_history"
    inst_cagr = cagr(inst_w)
    spy_cagr = cagr(spy_w)
    if inst_cagr is None:
        return None, "insufficient_history"
    if spy_cagr is None:
        return None, "benchmark_insufficient_history"
    return inst_cagr - spy_cagr, "ok"


# ---------------------------------------------------------------------------
# Per-metric status helpers
# ---------------------------------------------------------------------------


def vol_beta_status(n_returns: int, invalids_dropped: int = 0) -> RiskStatus:
    """Status for vol/beta given valid-return count and invalids dropped upstream.

    Trigger rule:
      - ``n_returns >= MIN_RETURNS_VOL_BETA`` → ``ok`` (a single chain break with
        enough valid returns remaining is fine, even if some invalids were
        dropped earlier).
      - ``n_returns < MIN_RETURNS_VOL_BETA`` AND ``invalids_dropped > 0`` →
        ``invalid_price_chain`` (invalids contributed to the shortfall).
      - ``n_returns < MIN_RETURNS_VOL_BETA`` AND no invalids → genuinely-short
        clean history → ``insufficient_history``.
    """
    if n_returns >= MIN_RETURNS_VOL_BETA:
        return "ok"
    if invalids_dropped > 0:
        return "invalid_price_chain"
    return "insufficient_history"


def annualized_status(n_returns: int, invalids_dropped: int = 0) -> RiskStatus:
    """Status for annualized metrics given valid-return count and invalids dropped.

    Trigger rule (mirrors :func:`vol_beta_status` against ``MIN_RETURNS_ANNUALIZED``):
      - ``n_returns >= MIN_RETURNS_ANNUALIZED`` → ``ok``.
      - below threshold AND ``invalids_dropped > 0`` → ``invalid_price_chain``.
      - below threshold AND no invalids → ``partial_window``.
    """
    if n_returns >= MIN_RETURNS_ANNUALIZED:
        return "ok"
    if invalids_dropped > 0:
        return "invalid_price_chain"
    return "partial_window"


def beta_status(
    aligned_n: int,
    spy_present: bool,
) -> RiskStatus:
    """Status for beta given aligned-pair count and benchmark presence."""
    if not spy_present:
        return "benchmark_missing"
    if aligned_n < MIN_RETURNS_VOL_BETA:
        return "benchmark_insufficient_history"
    return "ok"


def drawdown_status(valid_closes: int, invalids_dropped: int = 0) -> RiskStatus:
    """Status for drawdown given the count of valid closes in the chain.

    A drawdown is computable from ≥ 2 valid closes. It is NOT annualized
    (never ``partial_window``) and NOT benchmark-relative (never ``benchmark_*``).

    Trigger rule (mirrors :func:`vol_beta_status`'s invalids signal):
      - ``valid_closes >= 2`` → ``ok``.
      - below 2 AND ``invalids_dropped > 0`` → ``invalid_price_chain``
        (invalids dropped the usable chain below 2).
      - below 2 AND no invalids → ``insufficient_history``.
    """
    if valid_closes >= 2:
        return "ok"
    if invalids_dropped > 0:
        return "invalid_price_chain"
    return "insufficient_history"


def distribution_status(n_returns: int, invalids_dropped: int = 0) -> RiskStatus:
    """Status for the return-distribution moments given the valid-return count.

    The moments (skew / excess kurtosis / var_5) need ≥ 2 returns to exist and
    ≥ ``MIN_OBS_MOMENTS`` (250) returns to be reliable (the persisted form of
    :attr:`DistributionResult.low_sample`).

    Trigger rule:
      - ``n_returns >= MIN_OBS_MOMENTS`` → ``ok``.
      - ``2 <= n_returns < MIN_OBS_MOMENTS`` → ``partial_window`` (low-sample,
        unreliable moments).
      - ``n_returns < 2`` AND ``invalids_dropped > 0`` → ``invalid_price_chain``.
      - ``n_returns < 2`` AND no invalids → ``insufficient_history``.
    """
    if n_returns >= MIN_OBS_MOMENTS:
        return "ok"
    if n_returns >= 2:
        return "partial_window"
    if invalids_dropped > 0:
        return "invalid_price_chain"
    return "insufficient_history"


def trailing_status(
    inst_closes: Sequence[PricePoint],
    as_of: date,
    invalids_dropped: int = 0,
) -> RiskStatus:
    """Rollup status for trailing returns: is the SHORTEST horizon computable?

    Per-window null trailing values already encode which individual horizons are
    missing; this status is the rollup over the shortest (1m) window — if not
    even 1m is computable there is no trailing history at all.

    Trigger rule:
      - shortest (1m) trailing return computable → ``ok``.
      - not computable AND ``invalids_dropped > 0`` → ``invalid_price_chain``.
      - not computable AND no invalids → ``insufficient_history``.
    """
    shortest_lookback = TRAILING_LOOKBACK_DAYS["1m"]
    if trailing_return(inst_closes, as_of, shortest_lookback) is not None:
        return "ok"
    if invalids_dropped > 0:
        return "invalid_price_chain"
    return "insufficient_history"


# ---------------------------------------------------------------------------
# Window slicing
# ---------------------------------------------------------------------------


def _slice_window(
    closes: Sequence[PricePoint],
    as_of_date: date,
    window_key: str,
) -> list[PricePoint]:
    """Keep the closes that fall inside the ``window_key`` calendar lookback.

    A row survives iff ``as_of_date - lookback <= date <= as_of_date`` where
    ``lookback = WINDOW_LOOKBACK_DAYS[window_key]``. ``full`` has no lower bound
    (every close at/before ``as_of_date`` is kept). The loaded series is already
    ≤ as_of_date, but the upper bound is enforced here too so a stray future bar
    cannot leak into a window. Row order is preserved (callers assume ascending).
    """
    lookback = WINDOW_LOOKBACK_DAYS[window_key]
    if lookback is None:
        return [(d, raw) for d, raw in closes if d <= as_of_date]
    cutoff = as_of_date - timedelta(days=lookback)
    return [(d, raw) for d, raw in closes if cutoff <= d <= as_of_date]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def compute_instrument_risk(
    inst_closes: Sequence[PricePoint],
    spy_closes: Sequence[PricePoint],
    window_key: str,
    as_of_date: date,
) -> WindowMetrics:
    """Compute all risk metrics for ONE instrument over ONE window.

    The instrument and SPY series are FIRST sliced to ``window_key``'s calendar
    lookback (1y / 3y / full) via :func:`_slice_window`. Every standalone metric
    (vol, cagr, drawdown, calmar, distribution) is then computed on the sliced
    instrument series, and the benchmark metrics (beta, excess_cagr) on the
    sliced+aligned overlap with SPY. ``n_returns`` and ``window_days`` reflect
    the sliced window, so the three windows produce distinct metrics.

    EXCEPTION — trailing returns. ``trailing_*`` / ``excess_trailing_*`` are
    calendar-lookback from ``as_of_date`` and therefore window-INDEPENDENT; they
    are computed on the FULL (unsliced) series and are intentionally identical
    across the 1y/3y/full rows. ``trailing_status`` is likewise the full-series
    rollup. excess_trailing is null when SPY is absent.
    """
    # Slice BOTH series to the window first; all window metrics derive from these.
    inst_w = _slice_window(inst_closes, as_of_date, window_key)
    spy_w = _slice_window(spy_closes, as_of_date, window_key)

    inst_rets = simple_returns(inst_w)
    inst_ret_vals = [r for _, r in inst_rets]
    n_returns = len(inst_ret_vals)
    # Invalid closes break the return chain; track how many were dropped so a
    # sub-threshold metric can report invalid_price_chain (vs short-but-clean).
    # Scoped to the window — an invalid bar outside the window is irrelevant here.
    invalids_dropped = _count_invalid_closes(inst_w)

    vol = annualized_vol(inst_ret_vals)
    vol_st = vol_beta_status(n_returns, invalids_dropped)

    inst_cagr = cagr(inst_w)
    cagr_st = annualized_status(n_returns, invalids_dropped)

    dd = drawdown(inst_w)
    # Drawdown is computed off the VALID close sub-chain (≥ 2 needed). The total
    # windowed rows minus invalids gives the usable chain length.
    valid_closes = len(inst_w) - invalids_dropped
    dd_st = drawdown_status(valid_closes, invalids_dropped)

    calmar_val: Decimal | None = None
    if inst_cagr is not None and dd.max_drawdown is not None:
        calmar_val = calmar(inst_cagr, dd.max_drawdown)
    # Calmar shares the annualized min-obs threshold → reuse annualized_status so
    # invalid-driven shortfalls surface as invalid_price_chain there too.
    calmar_st = annualized_status(n_returns, invalids_dropped)

    dist = distribution(inst_ret_vals) if inst_ret_vals else None
    dist_st = distribution_status(n_returns, invalids_dropped)

    spy_present = bool(spy_closes)
    spy_rets = simple_returns(spy_w)
    beta_res = ols_beta(inst_rets, spy_rets)
    beta_st = beta_status(beta_res.n_obs, spy_present)

    # excess_cagr over the window-sliced aligned overlap. SPY presence is GLOBAL:
    # if SPY exists overall but has no rows inside this window, the shortfall is
    # benchmark_insufficient_history (not benchmark_missing). Pass spy_w (already
    # sliced) but force the missing-vs-insufficient distinction off the global
    # series so an empty window slice doesn't masquerade as "no benchmark".
    if not spy_present:
        xs_cagr, xs_cagr_st = None, "benchmark_missing"
    else:
        xs_cagr, xs_cagr_st = excess_cagr(inst_w, spy_w, window_key)
        if xs_cagr_st == "benchmark_missing":
            # spy_w empty within the window though SPY exists globally.
            xs_cagr_st = "benchmark_insufficient_history"

    # Trailing returns are window-INDEPENDENT: computed on the FULL series.
    trailing: dict[str, Decimal | None] = {}
    excess_trailing: dict[str, Decimal | None] = {}
    for key, lookback in TRAILING_LOOKBACK_DAYS.items():
        trailing[key] = trailing_return(inst_closes, as_of_date, lookback)
        xs_val, _ = excess_trailing_return(inst_closes, spy_closes, as_of_date, lookback)
        excess_trailing[key] = xs_val
    trailing_st = trailing_status(inst_closes, as_of_date, _count_invalid_closes(inst_closes))

    return WindowMetrics(
        window_key=window_key,
        n_returns=n_returns,
        annualized_vol=vol,
        vol_status=vol_st,
        cagr=inst_cagr,
        cagr_status=cagr_st,
        max_drawdown=dd.max_drawdown,
        current_drawdown=dd.current_drawdown,
        drawdown_status=dd_st,
        calmar=calmar_val,
        calmar_status=calmar_st,
        beta=beta_res.beta,
        r2=beta_res.r2,
        beta_n_obs=beta_res.n_obs,
        beta_status=beta_st,
        excess_cagr=xs_cagr,
        excess_cagr_status=xs_cagr_st,
        distribution=dist,
        distribution_status=dist_st,
        trailing_1m=trailing["1m"],
        trailing_3m=trailing["3m"],
        trailing_6m=trailing["6m"],
        trailing_1y=trailing["1y"],
        excess_trailing_1m=excess_trailing["1m"],
        excess_trailing_3m=excess_trailing["3m"],
        excess_trailing_6m=excess_trailing["6m"],
        excess_trailing_1y=excess_trailing["1y"],
        trailing_status=trailing_st,
    )


# ---------------------------------------------------------------------------
# DB persist layer (#591 PR-B, Task B3)
# ---------------------------------------------------------------------------
#
# READ phase under snapshot_read (REPEATABLE READ): one consistent snapshot so
# a concurrent candle refresh / correction cannot let two instruments see
# different price states. COMPUTE phase in memory (pure). WRITE phase in a
# separate READ COMMITTED transaction: content-dedup append into the
# append-only observations log, then a deterministic rebuild of the _current
# write-through from the winning observation.
#
# CRITICAL (prevention-log "Diff-aware writers" variant A): refreshed_at /
# computed_at are mutate-on-every-call timestamps. They go in the UPDATE SET
# clause ONLY — NEVER inside the IS DISTINCT FROM tuple (else now() always
# differs → every MATCHED row re-fires → bloat). The (as_of_date, computed_at)
# tuple comparison is what advances a fresh / corrected snapshot even when the
# rounded business values are unchanged.

# Business columns shared by the INSERT, the dedup compare, and the _current
# upsert IS DISTINCT FROM tuple. Single source of truth — ORDER IS LOAD-BEARING
# (it drives the param order below and the EXCLUDED tuple). Excludes the keys
# (instrument_id, as_of_date, metric_version, window_key) and the mutate-on-
# write timestamps (computed_at, refreshed_at).
_RISK_BUSINESS_COLS: tuple[str, ...] = (
    "cagr",
    "excess_cagr_vs_spy",
    "max_drawdown",
    "max_dd_peak_date",
    "max_dd_trough_date",
    "current_drawdown",
    "vol_annualized",
    "beta",
    "beta_r2",
    "skew",
    "excess_kurtosis",
    "var_5",
    "worst_day",
    "best_day",
    "calmar",
    "trailing_1m",
    "trailing_3m",
    "trailing_6m",
    "trailing_1y",
    "excess_trailing_1m",
    "excess_trailing_3m",
    "excess_trailing_6m",
    "excess_trailing_1y",
    "n_returns",
    "beta_n_obs",
    "benchmark_instrument_id",
    "window_days",
    "cagr_status",
    "vol_status",
    "beta_status",
    "drawdown_status",
    "distribution_status",
    "calmar_status",
    "trailing_status",
    "excess_cagr_status",
)


def load_close_series(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    end_date: date,
) -> list[tuple[date, Decimal | None]]:
    """Load ``(price_date, close)`` for an instrument up to and including ``end_date``.

    Closes are returned ASC with NULL closes KEPT in the list — the
    return-chain / status logic in :func:`compute_instrument_risk` must see the
    invalid rows (a NULL breaks the chain and feeds the ``invalid_price_chain``
    signal). Do NOT pre-filter. Non-null closes are coerced ``Decimal(str(...))``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price_date, close
            FROM price_daily
            WHERE instrument_id = %(iid)s
              AND price_date <= %(end)s
            ORDER BY price_date ASC
            """,
            {"iid": instrument_id, "end": end_date},
        )
        rows = cur.fetchall()
    out: list[tuple[date, Decimal | None]] = []
    for price_date, close in rows:
        out.append((price_date, None if close is None else Decimal(str(close))))
    return out


def _resolve_benchmark_instrument_ids(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Resolve the benchmark symbols to instrument_ids (primary-listing tiebreak).

    Mirrors the candle-refresh scope query (``daily_candle_refresh``): tradable
    rows only, one id per symbol, ``is_primary_listing DESC, instrument_id``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol) symbol, instrument_id
            FROM instruments
            WHERE symbol = ANY(%(symbols)s)
              AND is_tradable = TRUE
            ORDER BY symbol, is_primary_listing DESC, instrument_id
            """,
            {"symbols": sorted(BENCHMARK_SYMBOLS)},
        )
        return {str(symbol): int(iid) for symbol, iid in cur.fetchall()}


def _latest_valid_close_date(closes: Sequence[tuple[date, Decimal | None]]) -> date | None:
    """The date of the latest VALID (finite & > 0) close, else None.

    closes are ASC; the last valid one wins. This — NOT raw MAX(price_date) —
    is the ``as_of_date`` so a trailing NULL / bad bar does not advance the
    snapshot date past the last good price.
    """
    latest: date | None = None
    for d, raw in closes:
        if _valid_close(raw) is not None:
            latest = d
    return latest


def _count_valid_closes(closes: Sequence[tuple[date, Decimal | None]]) -> int:
    """Count rows whose close is valid (finite & > 0)."""
    return sum(1 for _, raw in closes if _valid_close(raw) is not None)


def _window_days(closes: Sequence[PricePoint]) -> int | None:
    """Calendar span (days) of the valid close sub-chain, else None for < 2 valid."""
    valid = [d for d, raw in closes if _valid_close(raw) is not None]
    if len(valid) < 2:
        return None
    return (valid[-1] - valid[0]).days


def _metrics_row_values(
    metrics: WindowMetrics,
    inst_closes: Sequence[tuple[date, Decimal | None]],
    spy_closes: Sequence[tuple[date, Decimal | None]],  # noqa: ARG001 — kept for caller symmetry
    as_of_date: date,
    benchmark_instrument_id: int | None,
) -> dict[str, object]:
    """Build the business-column value dict for one (instrument, window) row.

    Window-DEPENDENT evidence (peak/trough dates, window_days) is recomputed off
    the SAME window slice the metrics used (``metrics.window_key``), so it stays
    consistent with the now-sliced vol/cagr/drawdown. Trailing returns are taken
    directly from the window-INDEPENDENT WindowMetrics scalars (computed on the
    full series) — they are identical across window rows.
    """
    dist = metrics.distribution
    # peak/trough dates are not on WindowMetrics; recompute off the same window
    # slice the drawdown metric used (NOT the full series — that would make the
    # peak/trough disagree with the sliced max_drawdown).
    inst_w = _slice_window(inst_closes, as_of_date, metrics.window_key)
    dd = drawdown(inst_w)

    return {
        "cagr": metrics.cagr,
        "excess_cagr_vs_spy": metrics.excess_cagr,
        "max_drawdown": metrics.max_drawdown,
        "max_dd_peak_date": dd.peak_date,
        "max_dd_trough_date": dd.trough_date,
        "current_drawdown": metrics.current_drawdown,
        "vol_annualized": metrics.annualized_vol,
        "beta": metrics.beta,
        "beta_r2": metrics.r2,
        "skew": dist.skew if dist else None,
        "excess_kurtosis": dist.excess_kurtosis if dist else None,
        "var_5": dist.var_5 if dist else None,
        "worst_day": dist.worst_day if dist else None,
        "best_day": dist.best_day if dist else None,
        "calmar": metrics.calmar,
        "trailing_1m": metrics.trailing_1m,
        "trailing_3m": metrics.trailing_3m,
        "trailing_6m": metrics.trailing_6m,
        "trailing_1y": metrics.trailing_1y,
        "excess_trailing_1m": metrics.excess_trailing_1m,
        "excess_trailing_3m": metrics.excess_trailing_3m,
        "excess_trailing_6m": metrics.excess_trailing_6m,
        "excess_trailing_1y": metrics.excess_trailing_1y,
        "n_returns": metrics.n_returns,
        "beta_n_obs": metrics.beta_n_obs,
        "benchmark_instrument_id": benchmark_instrument_id,
        "window_days": _window_days(inst_w),
        "cagr_status": metrics.cagr_status,
        "vol_status": metrics.vol_status,
        "beta_status": metrics.beta_status,
        "drawdown_status": metrics.drawdown_status,
        "distribution_status": metrics.distribution_status,
        "calmar_status": metrics.calmar_status,
        "trailing_status": metrics.trailing_status,
        "excess_cagr_status": metrics.excess_cagr_status,
    }


def _coerce_db_value(value: object) -> object:
    """Coerce a business value to its DB boundary form. Decimal stays Decimal."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, str, date)):
        return value
    # Any stray numeric (e.g. float island that slipped through) -> Decimal.
    return Decimal(str(value))


@dataclass(frozen=True)
class _PendingRow:
    """One computed (instrument, as_of, window) row awaiting write."""

    instrument_id: int
    as_of_date: date
    window_key: str
    values: dict[str, object]


def compute_and_store_risk_metrics(conn: psycopg.Connection[Any]) -> int:
    """Compute + persist risk metrics for the scoped instrument universe.

    Scope = instruments with >= 2 valid closes (>= 1 return computable) UNION
    the resolved benchmark instrument_ids. For each, all WINDOW_KEYS are
    computed against the SPY series and written:

      1. content-dedup APPEND into instrument_risk_metrics_observations
         (insert a new row only if none exists for the key, or the business
         columns differ from the latest existing row);
      2. deterministic REBUILD of instrument_risk_metrics_current from the
         winning observation (latest as_of_date, then latest computed_at).

    Returns the number of observation rows written this run.
    """
    # ---- READ PHASE (REPEATABLE READ snapshot; no writes inside) ----------
    with snapshot_read(conn):
        benchmark_ids = _resolve_benchmark_instrument_ids(conn)
        spy_instrument_id = benchmark_ids.get(SPY_SYMBOL)

        with conn.cursor() as cur:
            # Instruments with >= 2 valid (finite & > 0) closes. Pre-filter in
            # SQL on the close-validity predicate so scope is bounded; the
            # Python validity check is the source of truth but this avoids
            # loading the whole universe.
            cur.execute(
                """
                SELECT instrument_id
                FROM price_daily
                WHERE close IS NOT NULL
                  AND close > 0
                GROUP BY instrument_id
                HAVING COUNT(*) >= 2
                """
            )
            scoped_ids = {int(r[0]) for r in cur.fetchall()}
        scoped_ids |= set(benchmark_ids.values())

        # End date for every series load: today (UTC date via Postgres).
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_DATE")
            current_date_row = cur.fetchone()
        assert current_date_row is not None
        current_date: date = current_date_row[0]

        spy_closes: list[tuple[date, Decimal | None]] = (
            load_close_series(conn, spy_instrument_id, current_date) if spy_instrument_id is not None else []
        )

        inst_series: dict[int, list[tuple[date, Decimal | None]]] = {}
        for iid in sorted(scoped_ids):
            inst_series[iid] = load_close_series(conn, iid, current_date)

    # ---- COMPUTE PHASE (in memory; pure) ----------------------------------
    pending: list[_PendingRow] = []
    for iid in sorted(scoped_ids):
        closes = inst_series.get(iid, [])
        # A benchmark with thin / no own history can still be in scope via the
        # UNION; skip it if it cannot produce even one return.
        if _count_valid_closes(closes) < 2:
            continue
        as_of_date = _latest_valid_close_date(closes)
        if as_of_date is None:
            continue
        # Guard: never let a future-dated bar advance the snapshot past today.
        if as_of_date > current_date:
            as_of_date = current_date
        for window_key in WINDOW_KEYS:
            metrics = compute_instrument_risk(closes, spy_closes, window_key, as_of_date)
            values = _metrics_row_values(metrics, closes, spy_closes, as_of_date, spy_instrument_id)
            pending.append(
                _PendingRow(
                    instrument_id=iid,
                    as_of_date=as_of_date,
                    window_key=window_key,
                    values=values,
                )
            )

    if not pending:
        return 0

    # ---- WRITE PHASE (separate READ COMMITTED transaction) ----------------
    written = 0
    touched_ids: list[int] = sorted({p.instrument_id for p in pending})
    with conn.transaction():
        for row in pending:
            if _append_observation_if_changed(conn, row):
                written += 1
        # Rebuild _current for the touched instruments, batched.
        for start in range(0, len(touched_ids), _REBUILD_BATCH):
            batch = touched_ids[start : start + _REBUILD_BATCH]
            _rebuild_current_for_batch(conn, batch)
    return written


def _append_observation_if_changed(conn: psycopg.Connection[Any], row: _PendingRow) -> bool:
    """Insert a new observation row iff it is new or its business columns differ.

    Reads the latest existing row for (instrument, as_of, version, window) and
    compares ONLY the business columns (never computed_at). Returns True if a
    row was inserted.
    """
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in _RISK_BUSINESS_COLS)
    select_q = sql.SQL(
        """
        SELECT {cols}
        FROM instrument_risk_metrics_observations
        WHERE instrument_id = %(iid)s
          AND as_of_date = %(asof)s
          AND metric_version = %(ver)s
          AND window_key = %(win)s
        ORDER BY computed_at DESC
        LIMIT 1
        """
    ).format(cols=col_idents)
    key_params: dict[str, object] = {
        "iid": row.instrument_id,
        "asof": row.as_of_date,
        "ver": RISK_METRICS_VERSION,
        "win": row.window_key,
    }
    with conn.cursor() as cur:
        cur.execute(select_q, key_params)
        latest = cur.fetchone()

    new_vals = [_coerce_db_value(row.values[c]) for c in _RISK_BUSINESS_COLS]
    if latest is not None:
        # Compare business columns. The SELECT returns DB-native types
        # (Decimal / int / date / str / None) which match the coerced new
        # values exactly, so equality is a faithful content compare.
        if list(latest) == new_vals:
            return False

    placeholders = sql.SQL(", ").join(sql.Placeholder(c) for c in _RISK_BUSINESS_COLS)
    insert_q = sql.SQL(
        """
        INSERT INTO instrument_risk_metrics_observations (
            instrument_id, as_of_date, metric_version, window_key, {cols}
        ) VALUES (
            %(iid)s, %(asof)s, %(ver)s, %(win)s, {vals}
        )
        """
    ).format(cols=col_idents, vals=placeholders)
    params: dict[str, object] = dict(key_params)
    for c in _RISK_BUSINESS_COLS:
        params[c] = _coerce_db_value(row.values[c])
    with conn.cursor() as cur:
        cur.execute(insert_q, params)
    return True


def _rebuild_current_for_batch(conn: psycopg.Connection[Any], instrument_ids: Sequence[int]) -> None:
    """Rebuild instrument_risk_metrics_current from the winning observation.

    DISTINCT ON picks the latest (as_of_date, computed_at) per
    (instrument, version, window). On conflict the row advances when the
    winning observation is newer ((as_of_date, computed_at) tuple) OR the
    business columns differ — refreshed_at / computed_at are SET only, never in
    the IS DISTINCT FROM tuple (prevention-log MERGE-bloat variant A).
    """
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in _RISK_BUSINESS_COLS)
    set_clause = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in _RISK_BUSINESS_COLS
    )
    business_lhs = sql.SQL(", ").join(
        sql.SQL("instrument_risk_metrics_current.{c}").format(c=sql.Identifier(c)) for c in _RISK_BUSINESS_COLS
    )
    business_rhs = sql.SQL(", ").join(sql.SQL("EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in _RISK_BUSINESS_COLS)
    rebuild_q = sql.SQL(
        """
        INSERT INTO instrument_risk_metrics_current (
            instrument_id, metric_version, window_key,
            {cols}, as_of_date, computed_at, refreshed_at
        )
        SELECT DISTINCT ON (instrument_id, metric_version, window_key)
            instrument_id, metric_version, window_key,
            {cols}, as_of_date, computed_at, NOW() AS refreshed_at
        FROM instrument_risk_metrics_observations
        WHERE instrument_id = ANY(%(ids)s)
          AND metric_version = %(ver)s
        ORDER BY instrument_id, metric_version, window_key,
                 as_of_date DESC, computed_at DESC
        ON CONFLICT (instrument_id, metric_version, window_key) DO UPDATE SET
            {set_clause},
            as_of_date = EXCLUDED.as_of_date,
            computed_at = EXCLUDED.computed_at,
            refreshed_at = NOW()
        WHERE (EXCLUDED.as_of_date, EXCLUDED.computed_at)
              > (instrument_risk_metrics_current.as_of_date, instrument_risk_metrics_current.computed_at)
           OR ({business_lhs}) IS DISTINCT FROM ({business_rhs})
        """
    ).format(
        cols=col_idents,
        set_clause=set_clause,
        business_lhs=business_lhs,
        business_rhs=business_rhs,
    )
    with conn.cursor() as cur:
        cur.execute(rebuild_q, {"ids": list(instrument_ids), "ver": RISK_METRICS_VERSION})
