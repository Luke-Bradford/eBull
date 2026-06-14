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
from typing import Literal, get_args

import numpy as np

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
    calmar: Decimal | None
    calmar_status: RiskStatus
    beta: Decimal | None
    r2: Decimal | None
    beta_n_obs: int
    beta_status: RiskStatus
    excess_cagr: Decimal | None
    excess_cagr_status: RiskStatus
    distribution: DistributionResult | None


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
    window: str,  # noqa: ARG001 — window kept for caller symmetry / future per-window slicing
) -> tuple[Decimal | None, RiskStatus]:
    """``cagr(inst over aligned window) - cagr(SPY over aligned window)``.

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


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def compute_instrument_risk(
    inst_closes: Sequence[PricePoint],
    spy_closes: Sequence[PricePoint],
    window_key: str,
    as_of_date: date,  # noqa: ARG001 — reserved for staleness checks at the persistence layer
) -> WindowMetrics:
    """Compute all risk metrics for ONE instrument over ONE window.

    Standalone metrics (vol, cagr, drawdown, calmar, distribution) use the
    instrument's full valid history within the window. Benchmark metrics (beta,
    excess_cagr) use the aligned-overlap window with SPY.
    """
    inst_rets = simple_returns(inst_closes)
    inst_ret_vals = [r for _, r in inst_rets]
    n_returns = len(inst_ret_vals)
    # Invalid closes break the return chain; track how many were dropped so a
    # sub-threshold metric can report invalid_price_chain (vs short-but-clean).
    invalids_dropped = _count_invalid_closes(inst_closes)

    vol = annualized_vol(inst_ret_vals)
    vol_st = vol_beta_status(n_returns, invalids_dropped)

    inst_cagr = cagr(inst_closes)
    cagr_st = annualized_status(n_returns, invalids_dropped)

    dd = drawdown(inst_closes)

    calmar_val: Decimal | None = None
    if inst_cagr is not None and dd.max_drawdown is not None:
        calmar_val = calmar(inst_cagr, dd.max_drawdown)
    # Calmar shares the annualized min-obs threshold → reuse annualized_status so
    # invalid-driven shortfalls surface as invalid_price_chain there too.
    calmar_st = annualized_status(n_returns, invalids_dropped)

    dist = distribution(inst_ret_vals) if inst_ret_vals else None

    spy_present = bool(spy_closes)
    spy_rets = simple_returns(spy_closes)
    beta_res = ols_beta(inst_rets, spy_rets)
    beta_st = beta_status(beta_res.n_obs, spy_present)

    xs_cagr, xs_cagr_st = excess_cagr(inst_closes, spy_closes, window_key)

    return WindowMetrics(
        window_key=window_key,
        n_returns=n_returns,
        annualized_vol=vol,
        vol_status=vol_st,
        cagr=inst_cagr,
        cagr_status=cagr_st,
        max_drawdown=dd.max_drawdown,
        current_drawdown=dd.current_drawdown,
        calmar=calmar_val,
        calmar_status=calmar_st,
        beta=beta_res.beta,
        r2=beta_res.r2,
        beta_n_obs=beta_res.n_obs,
        beta_status=beta_st,
        excess_cagr=xs_cagr,
        excess_cagr_status=xs_cagr_st,
        distribution=dist,
    )
