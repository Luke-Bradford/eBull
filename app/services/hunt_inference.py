"""The pattern-hunt harness's inference: HAC t, refusals, BY screen, DSR and verdicts.

#3385 slice 3a, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5), sections
"Inference" and "Verdicts". Pure: no database, no prices. It consumes an active series
a(d) = r_A(d) − r_C(d) on the fixed grid, which the evaluator (slice 3b) produces.

Every constant here is a MODEL constant. ``hunt_harness`` folds this module's code hash
into ``HUNT_HARNESS_MODEL_ID``, so editing anything below is a new model id and a new
trial identity for every spec evaluated under it.

Source rules (spec "Source rule"): Newey & West 1987/1994 (Bartlett HAC, lag rule);
Benjamini & Yekutieli 2001 Theorem 1.3 (step-up at q / c(m), c(m) = Σ 1/j); Bailey &
López de Prado 2014 equations (1), (2) through ``deflated_sharpe``. The 2h lag floor,
the block df, T_eff, the V[SR] floor and every refusal floor are **constructions** the
spec freezes; no published rule gives them.

⚠ A statistical refusal is an OUTCOME (``StatRefused``); a caller bug is a ``ValueError``
and an infrastructure error propagates. Every comparison that decides a verdict is made on
values a constructor has already proved finite, so a NaN can never fall through a ``>``
into the wrong branch (obligation 146).
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Literal

from app.services.deflated_sharpe import DeflatedSharpeResult, TradeMoments, deflated_sharpe, trade_moments
from app.services.r6_monthly_trial import HacEstimate, newey_west_lag, student_t_cdf

# ---------------------------------------------------------------------------
# Model constants (spec "Inference", "Verdicts")
# ---------------------------------------------------------------------------

#: T < SHORT_SAMPLE_LAGS · L refuses ``short_sample``.
SHORT_SAMPLE_LAGS: Final = 10
#: Fewer greedy, h-spaced formations with an entered arm position refuses ``sparse_arm``.
MIN_ARM_FORMATIONS: Final = 30
#: p below this is recorded as ``< 1e-12`` and enters BY as 0.
P_UNDERFLOW: Final = 1e-12
P_UNDERFLOW_LABEL: Final = "< 1e-12"
#: T_eff at or below this refuses ``short_effective_sample``.
MIN_EFFECTIVE_SAMPLE: Final = 30.0
#: Fewer distinct finite trial Sharpes refuses ``trial_population_too_small``.
MIN_V_POPULATION: Final = 10
#: BY's FDR level for the discovery screen.
BY_Q: Final = 0.05
#: Validation / holdout bars.
T_BAR: Final = 3.0
DSR_BAR: Final = 0.95
#: The DSR here is on the calendar-time active axis, not criterion 6's trade axis.
HUNT_DSR_AXIS_ID: Final = "hunt-calendar-active-dsr-v1"

StatRefusalReason = Literal[
    "short_sample",
    "sparse_arm",
    "degenerate_variance",
    "non_finite",
    "book_ruin",
    "bad_dividend",
    "empty_grid",
    "short_effective_sample",
    "trial_population_too_small",
    "dsr_not_computed",
]


@dataclass(frozen=True)
class StatRefused:
    """A cell whose statistic could not be computed. An outcome, never a number."""

    reason: StatRefusalReason
    detail: str

    def form(self) -> dict[str, str]:
        return {"refused": self.reason, "detail": self.detail}


def _finite_sum(terms: Iterable[float]) -> float | None:
    try:
        total = math.fsum(terms)
    except OverflowError, ValueError:
        return None
    return total if math.isfinite(total) else None


def _all_finite(values: Iterable[float]) -> bool:
    return all(math.isfinite(value) for value in values)


# ---------------------------------------------------------------------------
# HAC t at a caller-supplied lag
# ---------------------------------------------------------------------------


def hunt_lag(observations: int, h: int) -> int:
    """L = max(2h, ⌊4(T/100)^{2/9}⌋). The 2h floor covers the overlap of h cohorts (construction)."""
    if h < 1:
        raise ValueError(f"h must be >= 1, got {h}")
    return max(2 * h, newey_west_lag(observations))


def hac_estimate(active: Sequence[float], lag: int) -> HacEstimate | StatRefused:
    """mean / Newey–West SE at ``lag``: r6's estimator (Bartlett kernel, 1/T autocovariances
    centred on the mean, no prewhitening, no finite-sample adjustment).

    ⚠ ``r6_monthly_trial.hac_t`` fixes the lag at the Newey–West rule; the harness needs the
    2h floor, so the estimator is restated here and a test pins the two equal at r6's lag.
    """
    if lag < 0:
        raise ValueError(f"lag must be >= 0, got {lag}")
    count = len(active)
    if count < 2:
        return StatRefused("short_sample", f"a HAC estimate needs at least 2 observations, got {count}")
    if not _all_finite(active):
        return StatRefused("non_finite", "a non-finite active return")
    total = _finite_sum(active)
    if total is None:
        return StatRefused("non_finite", "the sum of the active returns is not finite")
    mean = total / count
    centred = [value - mean for value in active]
    sums = [_finite_sum(centred[t] * centred[t - shift] for t in range(shift, count)) for shift in range(lag + 1)]
    if not _all_finite(centred) or any(value is None for value in sums):
        return StatRefused("non_finite", "a HAC autocovariance is not finite")
    autocovariances = [value / count for value in sums if value is not None]
    variance = autocovariances[0]
    if variance == 0.0:
        return StatRefused("degenerate_variance", "γ̂₀ = 0: the active series is constant")
    long_run = _finite_sum(
        [variance, *(2.0 * (1.0 - shift / (lag + 1)) * autocovariances[shift] for shift in range(1, lag + 1))]
    )
    if long_run is None:
        return StatRefused("non_finite", "the HAC variance is not finite")
    if not long_run > 0.0:
        return StatRefused("degenerate_variance", f"the HAC variance is not positive: {long_run}")
    standard_error = math.sqrt(long_run / count)
    t_stat = mean / standard_error
    if not (standard_error > 0.0 and math.isfinite(t_stat)):
        return StatRefused("non_finite", "the HAC t is not finite")
    return HacEstimate(
        observations=count,
        lag=lag,
        mean=mean,
        variance=variance,
        long_run_variance=long_run,
        standard_error=standard_error,
        t_stat=t_stat,
    )


def sparse_arm_count(entered_formations: Iterable[int], h: int) -> int:
    """Formations with an entered arm position, counted greedily from the first, each at
    least h sessions after the last counted (construction; spec "Statistical refusals").

    ``entered_formations`` are session ordinals on one calendar.
    """
    if h < 1:
        raise ValueError(f"h must be >= 1, got {h}")
    count = 0
    last: int | None = None
    for ordinal in sorted(set(entered_formations)):
        if last is None or ordinal - last >= h:
            count += 1
            last = ordinal
    return count


def one_sided_p(t_stat: float, df: int) -> float:
    """P(T_df ≤ −t), clamped to [0, 1]. ``student_t_cdf`` subtracts from 0.5 and can return
    0 or a tiny negative deep in the tail (v3 finding 42), hence the clamp."""
    if not math.isfinite(t_stat):
        raise ValueError(f"t must be finite, got {t_stat}")
    p = student_t_cdf(-t_stat, df)
    if not math.isfinite(p):
        raise ValueError(f"student_t_cdf returned {p} at t={t_stat}, df={df}")
    return min(1.0, max(0.0, p))


@dataclass(frozen=True)
class CellStatistics:
    """One cell's inference. Constructed only from finite values."""

    observations: int
    lag: int
    df: int
    mean: float
    variance: float
    long_run_variance: float
    standard_error: float
    t_stat: float
    #: Clamped one-sided p; 0.0 when ``p_underflow``.
    p: float
    p_underflow: bool
    arm_formations: int

    def __post_init__(self) -> None:
        values = (self.mean, self.variance, self.long_run_variance, self.standard_error, self.t_stat, self.p)
        if not _all_finite(values):
            raise ValueError(f"cell statistics must be finite, got {values}")
        if not 0.0 <= self.p <= 1.0:
            raise ValueError(f"p must be in [0, 1], got {self.p}")

    def form(self) -> dict[str, object]:
        return {
            "observations": self.observations,
            "lag": self.lag,
            "df": self.df,
            "mean": self.mean,
            "variance": self.variance,
            "long_run_variance": self.long_run_variance,
            "standard_error": self.standard_error,
            "t_stat": self.t_stat,
            "p": P_UNDERFLOW_LABEL if self.p_underflow else self.p,
            "arm_formations": self.arm_formations,
        }


def cell_statistics(
    active: Sequence[float], *, h: int, entered_formations: Iterable[int]
) -> CellStatistics | StatRefused:
    """The per-cell statistics and refusals of spec "Inference", in a fixed order:
    non_finite → short_sample → sparse_arm → the HAC's own refusals."""
    count = len(active)
    if not _all_finite(active):
        return StatRefused("non_finite", "a non-finite active return")
    if count < 2:
        return StatRefused("short_sample", f"T = {count}")
    lag = hunt_lag(count, h)
    if count < SHORT_SAMPLE_LAGS * lag:
        return StatRefused("short_sample", f"T = {count} < {SHORT_SAMPLE_LAGS} · L = {SHORT_SAMPLE_LAGS * lag}")
    arm = sparse_arm_count(entered_formations, h)
    if arm < MIN_ARM_FORMATIONS:
        return StatRefused(
            "sparse_arm", f"{arm} h-spaced formations with an entered arm position < {MIN_ARM_FORMATIONS}"
        )
    estimate = hac_estimate(active, lag)
    if isinstance(estimate, StatRefused):
        return estimate
    df = count // lag - 1
    p = one_sided_p(estimate.t_stat, df)
    underflow = p < P_UNDERFLOW
    return CellStatistics(
        observations=count,
        lag=lag,
        df=df,
        mean=estimate.mean,
        variance=estimate.variance,
        long_run_variance=estimate.long_run_variance,
        standard_error=estimate.standard_error,
        t_stat=estimate.t_stat,
        p=0.0 if underflow else p,
        p_underflow=underflow,
        arm_formations=arm,
    )


# ---------------------------------------------------------------------------
# V[SR] over the hunt's discovery trials
# ---------------------------------------------------------------------------


def _moments(series: Sequence[float]) -> TradeMoments | None:
    """``trade_moments``, or ``None`` when its moments are degenerate. ⚠ A near-constant finite
    series can fail its Pearson check numerically and raise ``ValueError``, which is a
    statistical failure here, not a bug (Codex ckpt-2)."""
    try:
        return trade_moments(series)
    except ValueError:
        return None


@dataclass(frozen=True)
class VPopulationMember:
    """One discovery ``evaluate`` trial of the hunt with an outcome."""

    hunt_trial_id: int
    #: The canonical cell's stored active series; ``None`` when none was stored.
    active_series: tuple[float, ...] | None
    #: The canonical cell refused ``book_ruin``.
    ruined: bool


@dataclass(frozen=True)
class TrialSharpeVariance:
    variance: float
    measured_variance: float
    #: mean over the population of 1/T_i: the IID variance of a per-observation Sharpe under zero edge.
    floor: float
    trial_ids: tuple[int, ...]
    #: reason → count: ``no_series``, ``ruined``, ``non_finite``, ``zero_variance`` (no usable
    #: moments), ``duplicate``.
    excluded: Mapping[str, int]

    @property
    def measured_trials(self) -> int:
        return len(self.trial_ids)


def trial_sharpe_variance(members: Iterable[VPopulationMember]) -> TrialSharpeVariance | StatRefused:
    """V[SR_n] = max(ddof=1 variance of per-observation Sharpes, mean(1/T_i)).

    Identical series count once (the lowest trial id is kept); a missing, ruined,
    non-finite or zero-variance series is excluded and counted (spec "DSR").
    """
    excluded = {"no_series": 0, "ruined": 0, "non_finite": 0, "zero_variance": 0, "duplicate": 0}
    seen: set[tuple[float, ...]] = set()
    ids: list[int] = []
    sharpes: list[float] = []
    lengths: list[int] = []
    for member in sorted(members, key=lambda item: item.hunt_trial_id):
        series = member.active_series
        if member.ruined:
            excluded["ruined"] += 1
            continue
        if series is None:
            excluded["no_series"] += 1
            continue
        if not _all_finite(series):
            excluded["non_finite"] += 1
            continue
        if series in seen:
            excluded["duplicate"] += 1
            continue
        moments = _moments(series)
        if moments is None:
            excluded["zero_variance"] += 1
            continue
        if not math.isfinite(moments.sharpe):
            excluded["non_finite"] += 1
            continue
        seen.add(series)
        ids.append(member.hunt_trial_id)
        sharpes.append(moments.sharpe)
        lengths.append(len(series))
    if len(sharpes) < MIN_V_POPULATION:
        return StatRefused(
            "trial_population_too_small", f"{len(sharpes)} distinct finite trial Sharpes < {MIN_V_POPULATION}"
        )
    measured = statistics.variance(sharpes)
    floor = statistics.fmean(1.0 / length for length in lengths)
    if not (math.isfinite(measured) and math.isfinite(floor)):
        return StatRefused("non_finite", "the trial Sharpe variance is not finite")
    return TrialSharpeVariance(
        variance=max(measured, floor),
        measured_variance=measured,
        floor=floor,
        trial_ids=tuple(ids),
        excluded=dict(excluded),
    )


# ---------------------------------------------------------------------------
# DSR (validation and holdout candidates only)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HuntDsr:
    result: DeflatedSharpeResult
    effective_sample_size: float
    #: The axis this DSR lives on; ``result.model_id`` names the shared equations.
    axis_id: str = HUNT_DSR_AXIS_ID


def hunt_dsr(
    active: Sequence[float],
    cell: CellStatistics,
    *,
    variance: TrialSharpeVariance,
    declared_trials: int,
    trial_register_version: str,
) -> HuntDsr | StatRefused:
    """Equation (2) on the calendar-time active series, N̂ = M.

    T_eff = min(T, T · γ̂₀ / Ŝ_NW) at the cell's L (construction): negative dependence is
    not credited. ``average_correlation = 0.0`` makes equation (9) return N̂ = M, which A.3
    calls conservative for ρ ≥ 0.
    """
    if declared_trials < max(2, variance.measured_trials):
        raise ValueError(
            f"declared_trials {declared_trials} must be >= 2 and cover the {variance.measured_trials} measured trials"
        )
    # ⚠ Cells share the fixed grid, so a length check cannot catch a series from another
    # cell; recompute and compare exactly (the estimator is deterministic; Codex ckpt-2).
    recomputed = hac_estimate(active, cell.lag)
    if not isinstance(recomputed, HacEstimate) or (
        recomputed.observations,
        recomputed.mean,
        recomputed.variance,
        recomputed.long_run_variance,
    ) != (cell.observations, cell.mean, cell.variance, cell.long_run_variance):
        raise ValueError("the active series is not the one the cell statistics were computed on")
    moments = _moments(active)
    if moments is None:
        return StatRefused("degenerate_variance", "the active series has no Sharpe ratio")
    effective = min(float(cell.observations), cell.observations * cell.variance / cell.long_run_variance)
    if not math.isfinite(effective):
        return StatRefused("non_finite", "T_eff is not finite")
    if effective <= MIN_EFFECTIVE_SAMPLE:
        return StatRefused("short_effective_sample", f"T_eff = {effective} <= {MIN_EFFECTIVE_SAMPLE}")
    result = deflated_sharpe(
        moments,
        effective_sample_size=effective,
        trial_sharpe_variance=variance.variance,
        declared_trials=declared_trials,
        average_correlation=0.0,
        measured_trials=variance.measured_trials,
        trial_register_version=trial_register_version,
    )
    if result is None:
        return StatRefused("dsr_not_computed", "the shared deflated_sharpe returned None")
    return HuntDsr(result=result, effective_sample_size=effective)


# ---------------------------------------------------------------------------
# Benjamini–Yekutieli: the programme-wide discovery screen
# ---------------------------------------------------------------------------


def _validated_p[K: Hashable](p_values: Mapping[K, float], m: int) -> list[tuple[float, K]]:
    if not (isinstance(m, int) and not isinstance(m, bool)) or m < max(1, len(p_values)):
        raise ValueError(f"m = {m!r} must be an int >= 1 and >= the {len(p_values)} p-values given")
    for key, p in p_values.items():
        if not (math.isfinite(p) and 0.0 <= p <= 1.0):
            raise ValueError(f"p-value for {key!r} must be finite and in [0, 1], got {p}")
    return sorted((p, key) for key, p in p_values.items())


def step_up[K: Hashable](p_values: Mapping[K, float], *, m: int, level: float) -> frozenset[K]:
    """The linear step-up of BY 2001 procedure (1): the largest k with p₍ₖ₎ ≤ k·level/m;
    flag every p ≤ p₍ₖ₎, or nothing when no k qualifies.

    The ``m − len(p_values)`` untested members (inherited searches, missing outcomes)
    carry p = 1, which can never qualify since k·level/m < 1.
    """
    if not (math.isfinite(level) and 0.0 < level < 1.0):
        raise ValueError(f"level must be in (0, 1), got {level}")
    ordered = _validated_p(p_values, m)
    cut: float | None = None
    for rank, (p, _key) in enumerate(ordered, start=1):
        if p <= rank * level / m:
            cut = p
    if cut is None:
        return frozenset()
    return frozenset(key for key, p in p_values.items() if p <= cut)


def harmonic(m: int) -> float:
    """c(m) = Σ_{j=1}^m 1/j (BY 2001, Theorem 1.3)."""
    if m < 1:
        raise ValueError(f"m must be >= 1, got {m}")
    return math.fsum(1.0 / j for j in range(1, m + 1))


@dataclass(frozen=True)
class ByReadout:
    flagged: frozenset[Hashable]
    m: int
    c_m: float
    q: float


def by_screen[K: Hashable](p_values: Mapping[K, float], *, m: int, q: float = BY_Q) -> ByReadout:
    """BY 2001 Theorem 1.3: the step-up at q / c(m).

    ⚠ A screen deciding which candidates may be declared, not an FDR guarantee: the
    programme is sequential and adaptive and its inherited searches have no p-values
    (spec residual). Recompute on every readout; never cache a flag.
    """
    c_m = harmonic(m)
    return ByReadout(flagged=frozenset(step_up(p_values, m=m, level=q / c_m)), m=m, c_m=c_m, q=q)


def screening_p(base_cells: Mapping[str, CellStatistics | StatRefused] | None) -> float:
    """A trial's BY p: the largest base-cost p over its cells, so every cell must clear the
    screen; 1 when it has no outcome or any base cell refused."""
    if not base_cells:
        return 1.0
    if any(isinstance(cell, StatRefused) for cell in base_cells.values()):
        return 1.0
    return max(cell.p for cell in base_cells.values() if isinstance(cell, CellStatistics))


# ---------------------------------------------------------------------------
# Verdicts (validation and holdout)
# ---------------------------------------------------------------------------


class Verdict(StrEnum):
    """A candidate's verdict. ⚠ None of these means "no edge" (programme rule 3):
    ``UNDETERMINED`` is an underpowered positive, ``FAIL_CONTROL`` is a failure against the
    control on THIS construction. The hunt-level "no demonstrated edge" is ``HuntClosure``."""

    NOT_PASS_REFUSED = "NOT_PASS_REFUSED"
    PASS = "PASS"
    #: Base passes, stress does not. No capital, no holdout.
    PASS_CONTINGENT = "PASS_CONTINGENT"
    FAIL_CONTROL = "FAIL_CONTROL"
    UNDETERMINED = "UNDETERMINED"
    NOT_PASS_MIXED = "NOT_PASS_MIXED"

    @property
    def permits_holdout(self) -> bool:
        return self is Verdict.PASS


class HuntClosure(StrEnum):
    """A HUNT's terminal readout. ⚠ A statement about the search, never about a candidate:
    the closure lists every candidate's own ``Verdict`` beside it."""

    NO_DEMONSTRATED_EDGE = "no_demonstrated_edge"
    HOLDOUT_REPORTED = "holdout_reported"


def validation_closure(verdicts: Mapping[str, Verdict]) -> HuntClosure | None:
    """``NO_DEMONSTRATED_EDGE`` when a completed validation batch has no ``PASS``; ``None``
    when the hunt goes on to its holdout (it closes after that readout)."""
    if not verdicts:
        raise ValueError("a validation batch with no candidates cannot close a hunt through this path")
    return None if any(verdict.permits_holdout for verdict in verdicts.values()) else HuntClosure.NO_DEMONSTRATED_EDGE


@dataclass(frozen=True)
class BaseReadout:
    t_stat: float
    dsr: float
    mean: float

    def __post_init__(self) -> None:
        if not _all_finite((self.t_stat, self.dsr, self.mean)):
            raise ValueError(f"a base readout must be finite, got {self}")


@dataclass(frozen=True)
class StressReadout:
    t_stat: float
    mean: float

    def __post_init__(self) -> None:
        if not _all_finite((self.t_stat, self.mean)):
            raise ValueError(f"a stress readout must be finite, got {self}")


@dataclass(frozen=True)
class VerdictResult:
    verdict: Verdict
    #: Each failing cell and why; for ``NOT_PASS_REFUSED`` the refusal reasons.
    reasons: tuple[str, ...]


def _base_failures(name: str, cell: BaseReadout) -> list[str]:
    failures = []
    if not cell.t_stat > T_BAR:
        failures.append(f"{name}: t {cell.t_stat} <= {T_BAR}")
    if not cell.dsr >= DSR_BAR:
        failures.append(f"{name}: DSR {cell.dsr} < {DSR_BAR}")
    if not cell.mean > 0.0:
        failures.append(f"{name}: mean(a) {cell.mean} <= 0")
    return failures


def decide_verdict(
    base: Mapping[str, BaseReadout | StatRefused], stress: Mapping[str, StressReadout | StatRefused]
) -> VerdictResult:
    """Spec "Verdicts", first match wins. A refused stress cell is a failed stress cell."""
    if not base:
        raise ValueError("a verdict needs at least one base cell")
    if set(base) != set(stress):
        raise ValueError(f"base cells {sorted(base)} and stress cells {sorted(stress)} must be the same cells")
    refused = [f"{name}: {cell.reason}" for name, cell in sorted(base.items()) if isinstance(cell, StatRefused)]
    if refused:
        return VerdictResult(Verdict.NOT_PASS_REFUSED, tuple(refused))
    readouts = {name: cell for name, cell in base.items() if isinstance(cell, BaseReadout)}
    base_failures = [failure for name, cell in sorted(readouts.items()) for failure in _base_failures(name, cell)]
    stress_failures: list[str] = []
    for name, cell in sorted(stress.items()):
        if isinstance(cell, StatRefused):
            stress_failures.append(f"stress {name}: refused {cell.reason}")
            continue
        if not cell.t_stat > T_BAR:
            stress_failures.append(f"stress {name}: t {cell.t_stat} <= {T_BAR}")
        if not cell.mean > 0.0:
            stress_failures.append(f"stress {name}: mean(a) {cell.mean} <= 0")
    if not base_failures:
        if not stress_failures:
            return VerdictResult(Verdict.PASS, ())
        return VerdictResult(Verdict.PASS_CONTINGENT, tuple(stress_failures))
    if all(not cell.mean > 0.0 for cell in readouts.values()):
        return VerdictResult(Verdict.FAIL_CONTROL, tuple(base_failures))
    if all(cell.mean > 0.0 for cell in readouts.values()):
        return VerdictResult(Verdict.UNDETERMINED, tuple(base_failures))
    return VerdictResult(Verdict.NOT_PASS_MIXED, tuple(base_failures))


# ---------------------------------------------------------------------------
# Per-regime readout (descriptive; no t)
# ---------------------------------------------------------------------------


def regime_readout(active: Sequence[float], labels: Sequence[str]) -> dict[str, tuple[float, int]]:
    """mean(a) and session count per regime label of session d."""
    if len(active) != len(labels):
        raise ValueError(f"{len(active)} active returns but {len(labels)} regime labels")
    groups: dict[str, list[float]] = {}
    for value, label in zip(active, labels, strict=True):
        groups.setdefault(label, []).append(value)
    return {label: (math.fsum(values) / len(values), len(values)) for label, values in sorted(groups.items())}


__all__ = [
    "BY_Q",
    "DSR_BAR",
    "HUNT_DSR_AXIS_ID",
    "MIN_ARM_FORMATIONS",
    "MIN_EFFECTIVE_SAMPLE",
    "MIN_V_POPULATION",
    "P_UNDERFLOW",
    "SHORT_SAMPLE_LAGS",
    "T_BAR",
    "BaseReadout",
    "ByReadout",
    "CellStatistics",
    "HuntClosure",
    "HuntDsr",
    "StatRefused",
    "StressReadout",
    "TrialSharpeVariance",
    "VPopulationMember",
    "Verdict",
    "VerdictResult",
    "by_screen",
    "cell_statistics",
    "decide_verdict",
    "hac_estimate",
    "harmonic",
    "hunt_dsr",
    "hunt_lag",
    "one_sided_p",
    "regime_readout",
    "screening_p",
    "sparse_arm_count",
    "step_up",
    "trial_sharpe_variance",
    "validation_closure",
]
