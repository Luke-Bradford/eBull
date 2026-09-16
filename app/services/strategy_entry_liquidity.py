"""Per-leg entry-liquidity observation for #3104 slice 7a.

Spec: ``docs/proposals/ta/2026-09-16-promotion-evidence-capacity.md``.

⚠⚠ THIS MODULE DOES NOT PRODUCE ``capacity_usd`` AND MUST NOT BE READ AS DOING
SO. Two Codex checkpoint-1 passes each killed a different capacity formula; the
four blockers are recorded in the spec. What is landed here is the part that
**cannot be added later** — the per-leg observation itself, measured where
``_NamespaceBook`` still exists, because the book dies inside
``_measure_namespace`` (``backtest_run.py:1704``) and a later job over stored
results can never recover it.

⚠ The quantity is named for its arithmetic — ``close × volume``, meaned over a
causal window — and deliberately NOT "dollar volume", "turnover" or "liquidity".
``research_price_series.adjustment_basis`` describes the OHLC columns only
(``sql/251:32``), so the volume basis is unverified; the only available
cross-check is the other archive, and ``research_corpus_ingest.py:168-170``
rules that both are Yahoo redistributions and *"agreement between them is
circular, never corroborating"*. Even a verified price × shares would use the
CLOSING price for the whole session and would say nothing about spread or depth.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Final, Literal, get_args

import numpy as np

from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS
from app.services.research_corpus_ingest import RESEARCH_ARCHIVES
from app.services.strategy_decision_context import DEFINITION

#: ``sql/304`` (#2508): *"Mean ADV is the capacity convention; median volume is
#: the robust typical-day baseline."* The lookback and the statistic are
#: ``ContextDefinition``'s, reused rather than minted — ⚠ the EXCLUSION policy
#: below is NOT inherited with them and is declared separately.
LOOKBACK_SESSIONS: Final[int] = DEFINITION.volume_lookback_sessions

#: ``sql/305`` (#2508 / #2400): price and dollar-volume attribution requires a
#: directly observed unadjusted level. ``reconstructed_unadjusted`` is admitted
#: by that rule but unreachable — ``cost_model.py:451-453`` records *"The corpus
#: has no such factors"* — so it is not listed.
ELIGIBLE_ADJUSTMENT_BASES: Final[frozenset[str]] = frozenset({"unadjusted"})

ExclusionReason = Literal[
    "basis_ineligible",
    "window_short",
    "scale_break_spanned",
    "provisional_bar",
    "close_unusable",
    "volume_unusable",
    "non_finite",
]

#: ⚠ BUMPED ON A RULE CHANGE, NEVER ON A COMMENT — a literal rather than a hash
#: of this module's source, matching ``LEDGER_MEASUREMENT_RULE_VERSION`` on
#: ``strategy_promotion_evidence_measure``, which is the record this
#: measurement rides on.
#:
#: ⚠ A first version hashed ``inspect.getsource(causal_close_volume_means)``.
#: That moved the stamp on a comment-only edit (review NITPICK), which makes a
#: version change stop meaning anything — the reader cannot tell a re-worded
#: docstring from a changed estimator. Bump this deliberately when the
#: lookback, the eligible bases, the precedence or the arithmetic move.
ENTRY_LIQUIDITY_RULE_VERSION: Final[str] = "entry-liquidity-2026-09-16-v1"

#: ⚠ FROZEN PRECEDENCE, and it is load-bearing. A window routinely fails several
#: rules at once; counting every hit would break ``measured + excluded ==
#: realised``, which is the one invariant that makes the exclusion counts
#: auditable. Earliest match wins.
EXCLUSION_PRECEDENCE: Final[tuple[ExclusionReason, ...]] = get_args(ExclusionReason)


@dataclass(frozen=True)
class ArchivePolicy:
    """The pinned per-vendor facts this measurement needs, and nothing else."""

    adjustment_basis: str
    #: ``as_of - PROVISIONAL_WINDOW_DAYS``. ⚠ Derived from the archive's PINNED
    #: ``quarantine_as_of``, never from today or from the run's end: a
    #: diagnostic whose value moves with the wall clock is not reproducible.
    provisional_from: date

    @property
    def eligible(self) -> bool:
        return self.adjustment_basis in ELIGIBLE_ADJUSTMENT_BASES


def archive_policy_for(vendor: str) -> ArchivePolicy | None:
    """The pinned policy for ``vendor``, or ``None`` if we hold no provenance.

    ⚠ ``None`` is WITHHOLDING, not eligibility. An unknown vendor has no
    declared adjustment basis, and ``sql/305`` refuses an undeclared one exactly
    as it refuses a split-adjusted one.
    """
    for archive in RESEARCH_ARCHIVES:
        if archive.vendor == vendor:
            return ArchivePolicy(
                adjustment_basis=archive.adjustment_basis,
                provisional_from=archive.quarantine_as_of - timedelta(days=PROVISIONAL_WINDOW_DAYS),
            )
    return None


def causal_close_volume_means(
    *,
    dates: Sequence[date],
    closes: Sequence[Decimal | float | None],
    volumes: Sequence[Decimal | float | int | None],
    provisional_from: date,
    lookback: int = LOOKBACK_SESSIONS,
) -> tuple[tuple[float, ...], tuple[ExclusionReason | None, ...]]:
    """Per bar index: the causal mean of ``close × volume``, or why there is none.

    The window is the ``lookback`` completed sessions ending **at and
    including** index ``i``. ⚠ Inclusive because ``signal_ledger.py:9-11``
    carries §3.5 — *"Signal on the close of bar t → fill at the OPEN of bar
    t+1"* — so bar ``t`` has closed when the signal is formed and its volume is
    known. Excluding it would be an extra lag, not caution.

    ⚠ A failing session invalidates the whole WINDOW rather than dropping a
    TERM. A mean over 17 of 20 sessions is a different estimator and
    ``volume_lookback_sessions`` is a declared 20. This diverges from
    ``strategy_decision_context``, which permits a zero mean and carries
    ``zero_volume_frequency`` alongside; the divergence is a decision taken here
    and not something inherited with that module's constants.

    ⚠ ``closes`` arrives ALREADY MASKED FOR THE ARM — ``_apply_arm`` sets a
    quarantined close to ``None`` under ``masked`` and passes it through under
    ``admitted`` — so the usability test below is arm-correct by construction
    and the diagnostic follows the arm it is measured on. Per-field masking
    survives: a ``range_usable = False`` bar keeps its close and stays usable
    here, which is the point of ``StructureBar``'s per-field masking.

    ⚠ Bad terms are zeroed BEFORE the windowed sum, so a non-finite input can
    never poison a later clean window.
    """
    if lookback < 1:
        raise ValueError(f"lookback must be positive, got {lookback}")
    count = len(dates)
    if not (len(closes) == len(volumes) == count):
        raise ValueError(
            f"dates/closes/volumes carry {count}/{len(closes)}/{len(volumes)} entries — they are positionally parallel"
        )
    if count == 0:
        return (), ()

    close_arr = np.array([_finite_positive(value) for value in closes], dtype=np.float64)
    volume_arr = np.array([_finite_positive(value) for value in volumes], dtype=np.float64)
    close_bad = np.isnan(close_arr)
    volume_bad = np.isnan(volume_arr)
    provisional = np.fromiter((day >= provisional_from for day in dates), dtype=bool, count=count)

    product = np.where(close_bad | volume_bad, 0.0, close_arr * volume_arr)

    means: list[float] = [0.0] * count
    reasons: list[ExclusionReason | None] = [None] * count
    if count < lookback:
        short: list[ExclusionReason | None] = ["window_short"] * count
        return tuple(means), tuple(short)

    # Exact windowed mean — no cumsum, so no catastrophic cancellation on a
    # long series of ~1e11-scale products.
    windows = np.lib.stride_tricks.sliding_window_view(product, lookback)
    window_means: np.ndarray[Any, Any] = windows.mean(axis=1)
    provisional_hits: Any = np.lib.stride_tricks.sliding_window_view(provisional, lookback).any(axis=1)
    close_hits: Any = np.lib.stride_tricks.sliding_window_view(close_bad, lookback).any(axis=1)
    volume_hits: Any = np.lib.stride_tricks.sliding_window_view(volume_bad, lookback).any(axis=1)

    for index in range(count):
        if index < lookback - 1:
            reasons[index] = "window_short"
            continue
        slot = index - (lookback - 1)
        # Frozen precedence, applied literally.
        if provisional_hits[slot]:
            reasons[index] = "provisional_bar"
            continue
        if close_hits[slot]:
            reasons[index] = "close_unusable"
            continue
        if volume_hits[slot]:
            reasons[index] = "volume_unusable"
            continue
        value = float(window_means[slot])
        if not np.isfinite(value) or value <= 0.0:
            reasons[index] = "non_finite"
            continue
        means[index] = value
    return tuple(means), tuple(reasons)


def _finite_positive(value: Decimal | float | int | None) -> float:
    """``_usable_close`` / ``_usable_volume``'s predicate, on this module's inputs.

    ⚠ NOT "reused verbatim" — ``price_quarantine``'s helpers take a ``Bar`` and
    return ``Decimal | None``, while ``_absorb`` holds ``OHLCVRow`` mappings.
    The PREDICATE is identical (``None`` or ``<= 0`` is unusable); the carrier
    is not, and pretending otherwise would hide a type mismatch.

    ⚠ Non-finite is rejected here, BEFORE any comparison that a ``Decimal``
    NaN would raise on rather than answer.
    """
    if value is None:
        return float("nan")
    numeric = float(value)
    if not np.isfinite(numeric) or numeric <= 0.0:
        return float("nan")
    return numeric


def window_spans_break(
    *,
    dates: Sequence[date],
    index: int,
    unresolved_breaks: Sequence[date],
    lookback: int = LOOKBACK_SESSIONS,
) -> bool:
    """Does the window ending at ``index`` cross an unresolved price-scale break?

    ``price_segments.py:1-6``: *"``price_series_break.break_date`` is the first
    date at the new scale. Bars on either side remain usable inside their own
    segment; indicators and positions must not span the boundary."* A 20-bar
    rolling mean is an indicator, so a break whose first new-scale bar falls
    strictly after the window's first bar and at or before its last one makes
    the window span two scales.
    """
    if not unresolved_breaks or index < lookback - 1:
        return False
    window_start = dates[index - (lookback - 1)]
    window_end = dates[index]
    ordered = sorted(unresolved_breaks)
    left = bisect_right(ordered, window_start)
    right = bisect_right(ordered, window_end)
    return right > left


def bar_index_for(dates: Sequence[date], when: date) -> int | None:
    """The index of ``when`` in an ascending ``dates``, or ``None`` if absent."""
    position = bisect_left(dates, when)
    if position < len(dates) and dates[position] == when:
        return position
    return None


@dataclass(frozen=True)
class EntryLiquidityMeasurement:
    """One namespace's entry-liquidity observations, or the reason there are none.

    ⚠⚠ ALWAYS PRESENT, never ``None``. Withholding is expressed INSIDE this
    object so that ``measured + excluded == realised`` survives it — a ``None``
    measurement would take the exclusion counts with it, which is precisely the
    accounting the diagnostic exists to provide.
    """

    rule_version: str
    lookback_sessions: int
    #: The run's resolved basis, or ``None`` where it could not be resolved —
    #: an unknown vendor, or an admitted set spanning more than one basis.
    adjustment_basis: str | None
    realised_leg_count: int
    measured_leg_count: int
    excluded: Mapping[ExclusionReason, int]
    #: How many MEASURED legs sit on a series ``price_series_break`` cannot
    #: describe at all — an unlinked survivorship-free series, whose name key is
    #: ``-series_id`` (#2721 step 3).
    #:
    #: ⚠⚠ A CAVEAT CARRIED WITH THE NUMBER, NOT AN EXCLUSION, and the reasoning
    #: is consistency rather than convenience. ``price_series_break.instrument_id
    #: REFERENCES instruments`` (``sql/246:103``), so the table describes the
    #: LIVE corpus; ``backtest_run`` already passes ``()`` for a negative key
    #: everywhere else, which is how those legs came to be OPENED. A diagnostic
    #: that excluded them would apply a STRICTER scale-break standard than the
    #: positions it describes, and would then describe a population the run did
    #: not trade — on ``survivorship_free`` that is 17,707 of 22,879 series.
    #:
    #: ⚠ Reported rather than assumed away: "no break record" and "no break" are
    #: different statements, and this count is which one applies.
    unlinked_series_leg_count: int
    #: ⚠ NULLABLE TOGETHER. Undefined when nothing was measured, and an invented
    #: zero is what #2505's "missing is not zero" rule exists to prevent.
    minimum: Decimal | None
    p05: Decimal | None
    p50: Decimal | None
    binding_name_key: int | None
    binding_signal_date: date | None

    def __post_init__(self) -> None:
        if self.realised_leg_count < 0 or self.measured_leg_count < 0:
            raise ValueError("leg counts cannot be negative")
        unknown = set(self.excluded) - set(EXCLUSION_PRECEDENCE)
        if unknown:
            raise ValueError(f"unknown exclusion reasons {sorted(unknown)}")
        if any(value < 0 for value in self.excluded.values()):
            raise ValueError("exclusion counts cannot be negative")
        if not 0 <= self.unlinked_series_leg_count <= self.measured_leg_count:
            raise ValueError(
                f"{self.unlinked_series_leg_count} unlinked-series legs against {self.measured_leg_count} measured — "
                "the caveat counts a SUBSET of the measured population"
            )
        counted = self.measured_leg_count + sum(self.excluded.values())
        if counted != self.realised_leg_count:
            raise ValueError(
                f"{self.measured_leg_count} measured + {sum(self.excluded.values())} excluded = {counted} "
                f"against {self.realised_leg_count} realised legs — every leg carries exactly one verdict"
            )
        summary = (self.minimum, self.p05, self.p50, self.binding_name_key, self.binding_signal_date)
        if self.measured_leg_count == 0:
            if any(field is not None for field in summary):
                raise ValueError("a measurement with no measured legs cannot carry a summary")
        elif any(field is None for field in summary):
            raise ValueError("a measurement with measured legs must carry a complete summary")


def summarise(
    *,
    values: Sequence[float],
    name_keys: Sequence[int],
    signal_dates: Sequence[date],
    exit_dates: Sequence[date],
    realised_leg_count: int,
    excluded: Mapping[ExclusionReason, int],
    adjustment_basis: str | None,
) -> EntryLiquidityMeasurement:
    """Collapse the per-leg observations to what a human reads.

    ⚠ The SUMMARY is not the deliverable — the per-leg array inside
    ``_measure_namespace`` is, because that is what a later capacity rule
    consumes, in the same function, beside the returns and concurrency slices 1
    and 4 already build there. 4.2M floats are what ``LegBook``'s design exists
    to avoid persisting, which is exactly why the computation must live where
    the book does.

    ⚠ Quantile method is FROZEN as ``numpy.percentile``'s ``linear``
    interpolation. Leaving it implicit lets identical inputs produce different
    p05s across versions.

    ⚠ The counts travel with the quantiles so a p05 over a thin subset is
    visible as such. They show INCOMPLETENESS only — they cannot show which way
    the excluded legs would have moved the distribution, and no such claim is
    made.
    """
    count = len(values)
    if not (len(name_keys) == len(signal_dates) == len(exit_dates) == count):
        raise ValueError("the measured columns are positionally parallel")
    frozen: dict[ExclusionReason, int] = {
        reason: int(excluded.get(reason, 0)) for reason in EXCLUSION_PRECEDENCE if excluded.get(reason)
    }
    if count == 0:
        return EntryLiquidityMeasurement(
            rule_version=ENTRY_LIQUIDITY_RULE_VERSION,
            lookback_sessions=LOOKBACK_SESSIONS,
            adjustment_basis=adjustment_basis,
            realised_leg_count=realised_leg_count,
            measured_leg_count=0,
            excluded=frozen,
            unlinked_series_leg_count=0,
            minimum=None,
            p05=None,
            p50=None,
            binding_name_key=None,
            binding_signal_date=None,
        )
    array = np.asarray(values, dtype=np.float64)
    # ⚠ argmin alone is not deterministic under ties — numpy returns the first
    # in array order, which is corpus-sweep order. The tie-break is declared so
    # the reported binding leg is reproducible across runs.
    lowest = float(array.min())
    tied = [index for index in range(count) if array[index] == lowest]
    binding = min(tied, key=lambda index: (name_keys[index], signal_dates[index], exit_dates[index]))
    return EntryLiquidityMeasurement(
        rule_version=ENTRY_LIQUIDITY_RULE_VERSION,
        lookback_sessions=LOOKBACK_SESSIONS,
        adjustment_basis=adjustment_basis,
        realised_leg_count=realised_leg_count,
        measured_leg_count=count,
        excluded=frozen,
        unlinked_series_leg_count=sum(1 for key in name_keys if key < 0),
        minimum=_quantise(lowest),
        p05=_quantise(float(np.percentile(array, 5.0, method="linear"))),
        p50=_quantise(float(np.percentile(array, 50.0, method="linear"))),
        binding_name_key=int(name_keys[binding]),
        binding_signal_date=signal_dates[binding],
    )


def _quantise(value: float) -> Decimal:
    """Two decimal places, because the unit is a currency-shaped product.

    ⚠ NOT a claim that the unit is USD — see the module header. It is a
    presentation rule for a float that would otherwise print 17 digits of
    false precision.
    """
    return Decimal(f"{value:.2f}")


__all__ = [
    "ELIGIBLE_ADJUSTMENT_BASES",
    "ENTRY_LIQUIDITY_RULE_VERSION",
    "EXCLUSION_PRECEDENCE",
    "LOOKBACK_SESSIONS",
    "ArchivePolicy",
    "EntryLiquidityMeasurement",
    "ExclusionReason",
    "archive_policy_for",
    "bar_index_for",
    "causal_close_volume_means",
    "summarise",
    "window_spans_break",
]
