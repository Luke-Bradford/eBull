"""Per-leg session-gap observation for #3104 slice 9 (``worst_gap_pct``).

Spec: ``docs/proposals/ta/2026-09-16-promotion-evidence-exit-gap.md``.

⚠⚠ THIS IS A DESCRIPTIVE REALISED-PATH TAIL STATISTIC AND IS NOT AN ALMOST-SURE
BOUND. It must not be used to supply Hoeffding's ``[a, b]``.
``strategy_decay_sequential_test.hoeffding_variance_proxy`` says why, two lines
above the sentence that names this field: *"THE BOUND MUST BE A TRUE ALMOST-SURE
BOUND.  Policing it afterwards does not make it one."*  A worst gap measured off
a realised book is a HISTORICAL EXTREMUM -- the next trade may gap worse -- and a
single non-positive number cannot bound the upper tail at all, which Hoeffding
also needs.  (A long can gap THROUGH its target: entry 100, target 120, prior
close 119, an open at 150 is a 26.1% gap and a 30-point overshoot.)  So
``docs/proposals/ta/2026-09-16-2500-sequential-decay-test.md:88``'s
``-(stop_barrier + worst_gap)`` has no historical-extremum producer, and this
module is not one.  Recorded on #3104 and #2500; not fixable by measuring harder.

Its standing is that of its neighbours in the same record: ``max_drawdown_pct``
and ``expected_shortfall_5_pct`` are historical extrema too.

⚠ WHAT THIS MEASURES, EXACTLY: a GROSS price return across one session boundary.
It excludes the half-spread ``_absorb`` charges both fills, any execution
slippage, and the terminal-value haircut a ``series_termination`` close applies
(``backtest_run.py:1070``) -- none of which is a price gap.  The fields are named
``min_gap_pct`` / ``max_gap_pct`` for their ARITHMETIC and not "worst" / "best",
because a one-sided population makes those words wrong: an all-adverse run has a
negative maximum and an all-favourable one a positive minimum.
"""

from __future__ import annotations

from array import array
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_CEILING, ROUND_FLOOR, Context, Decimal, InvalidOperation
from typing import Final, Literal, get_args

import numpy as np

from app.services.price_quarantine import params_for
from app.services.research_corpus_ingest import ASSET_CLASS as RESEARCH_ASSET_CLASS

#: ⚠ BUMPED ON A RULE CHANGE, NEVER ON A COMMENT -- a literal rather than a hash
#: of this module's source, for the reason slice 7a recorded against
#: ``ENTRY_LIQUIDITY_RULE_VERSION``: a source hash moves the stamp on a re-worded
#: docstring, which makes a version change stop meaning anything.
#:
#: ⚠⚠ IT PINS THIS MODULE'S RULES AND NOT ITS INPUTS.  The arithmetic, the
#: population, the exclusion precedence, the conversion and rounding policy and
#: the tie-break are all pinned here.  ``HOLE_DAYS`` and the archive's
#: ``provisional_from`` are IMPORTED, so a change to ``price_quarantine`` or to
#: an archive literal moves the result WITHOUT moving this string -- which is why
#: ``ExitGapMeasurement`` reports both values rather than leaving them implicit.
EXIT_GAP_RULE_VERSION: Final[str] = "exit-gap-2026-09-16-v1"

#: T2's threshold, verbatim.  ``price_quarantine.evaluate_transitions``: *"T2 --
#: calendar gap wider than the per-class hole threshold … A ratio spanning a
#: series HOLE is not a same-scale comparison at all"*.  The class is the one
#: ``research_corpus_ingest`` pins for this corpus, imported rather than
#: restated, so the two cannot drift.
#:
#: ⚠ A NAMED LIMIT: this excludes only LONG holes.  A single missing session
#: inside ten calendar days still yields a multi-session return counted as one
#: boundary, and the threshold cannot distinguish an archive hole from a genuine
#: trading suspension.  ``boundary_calendar_days`` travels with each binding
#: observation so a reader can see which they are looking at.
HOLE_DAYS: Final[int] = params_for(RESEARCH_ASSET_CLASS).hole_days

ExclusionReason = Literal[
    "not_instrumented",
    "no_session_boundary",
    "provenance_unknown",
    "off_axis",
    "session_hole_spanned",
    "scale_break_spanned",
    "provisional_bar",
    "open_unusable",
    "close_unusable",
    "non_finite",
]

#: ⚠ FROZEN PRECEDENCE, and it is load-bearing.  A leg routinely fails several
#: rules across its boundaries; counting every hit would break ``measured +
#: excluded == realised``, which is the one invariant that makes these counts
#: auditable.
#:
#: ⚠⚠ "EARLIEST MATCH" MEANS LOWEST RANK IN THIS TUPLE, NOT CHRONOLOGICALLY
#: FIRST.  A leg whose second boundary is ``off_axis`` and whose fifth is
#: ``open_unusable`` reports ``off_axis``; chronological order would report the
#: other and is not reproducible under a re-ordered corpus sweep.
EXCLUSION_PRECEDENCE: Final[tuple[ExclusionReason, ...]] = get_args(ExclusionReason)

_RANK: Final[Mapping[str, int]] = {reason: index for index, reason in enumerate(EXCLUSION_PRECEDENCE)}

#: Two decimal places would be a currency rule.  This is a percentage of a price,
#: and 4dp is the resolution at which a sub-basis-point gap stays visible.
_EXPONENT: Final = Decimal("0.0001")

#: ⚠ QUANTISING A FINITE VALUE CAN RAISE.  ``Decimal(repr(1e100)).quantize(
#: Decimal("0.0001"))`` is ``InvalidOperation`` under the default 28-digit
#: context, because the result needs 105 digits.  A local context large enough
#: for any float's exact decimal expansion (~767 digits for a subnormal) removes
#: the failure mode instead of catching it after the fact.
_QUANTISE_CONTEXT: Final = Context(prec=800)


def worse_of(left: str | None, right: str) -> str:
    """The LOWEST-RANKED of two exclusion reasons under the frozen precedence."""
    if left is None:
        return right
    return left if _RANK[left] <= _RANK[right] else right


@dataclass(frozen=True)
class GapObservation:
    """One boundary, with everything needed to find it again.

    ⚠ A COUNT CANNOT IDENTIFY THE BINDING OBSERVATION.  Equal extrema can sit on
    different names, different close sources and spans of different length, so
    the provenance travels with the value rather than being looked up later --
    which is impossible anyway, because the book dies inside
    ``_measure_namespace``.

    ⚠ BOTH ends of the boundary are carried.  ``prior_bar_date`` is not derivable
    from ``bar_date`` and the calendar span without assuming the subtraction, and
    a reader chasing a suspect gap needs the two rows.
    """

    value: float
    name_key: int
    fill_date: date
    exit_date: date
    prior_bar_date: date
    bar_date: date
    boundary_calendar_days: int
    close_source: str

    @property
    def order_key(self) -> tuple[int, date, date, date]:
        """The declared tie-break.

        ⚠ ``argmin`` alone returns corpus-sweep order, which is not reproducible.
        """
        return (self.name_key, self.fill_date, self.exit_date, self.bar_date)

    def is_lower_than(self, other: GapObservation | None) -> bool:
        if other is None:
            return True
        if self.value != other.value:
            return self.value < other.value
        return self.order_key < other.order_key

    def is_higher_than(self, other: GapObservation | None) -> bool:
        if other is None:
            return True
        if self.value != other.value:
            return self.value > other.value
        return self.order_key < other.order_key


def boundary_gaps(
    *,
    dates: Sequence[date],
    opens: Sequence[Decimal | float | None],
    offsets: Sequence[int | None],
    raw_closes: Sequence[float],
    wealth_closes: Sequence[float],
    provisional_from: date,
    unresolved_breaks: Sequence[date] = (),
    hole_days: int = HOLE_DAYS,
) -> tuple[tuple[float, ...], tuple[ExclusionReason | None, ...]]:
    """Per SERIES index ``i``: the gap across the boundary ``(i-1, i)``, or why none.

    ⚠⚠ THE ARITHMETIC IS TWO RATIOS AND NOT ONE PRODUCT, and that is a
    correctness rule rather than a style::

        gap% = ((open[i] / raw_close[i]) * (wealth_close[i] / wealth_close[i-1]) - 1) * 100

    The algebraically identical ``(open * wealth / raw) / prior_wealth`` UNDERFLOWS.
    Codex checkpoint 1 reproduced it: with all four prices ``1e-200`` the product
    form returns **-100% for a flat boundary**, and with all four ``1e200`` it
    returns ``inf`` and withholds a boundary that is exactly flat.  Each ratio
    above is near 1 for a normal bar whatever the price level, so neither
    intermediate leaves float range.

    ⚠⚠ TWO INDEX DOMAINS, AND CONFUSING THEM IS SILENT.  ``opens`` and ``dates``
    are on the INSTRUMENT'S SERIES.  ``raw_closes`` and ``wealth_closes`` are
    dense arrays over its PANEL SPAN (``_dense_price_history`` fills
    ``[NaN] * (last_axis_index - first_axis_index + 1)`` only at
    ``axis_pos[when]``).  Indexing both with one integer reads different bars
    wherever the panel carries a date this instrument did not trade, which is the
    normal case.  So ``offsets`` maps EACH series index to its own dense offset,
    and both ends of a boundary are mapped independently.

    ⚠ ``wealth_close / raw_close`` IS NOT A DIVIDEND-ONLY FACTOR.  It is whatever
    separates the run's total-return close from the stored close, and that
    depends on the archive: ``sql/251``'s *"The OHLC columns carry only the split
    adjustment"* describes ONE archive, and ``icyDenev/Intrader`` -- the eligible
    one -- stores ``unadjusted`` OHLC, so its factor carries splits as well as
    distributions.  Carrying the open by its own bar's factor and dividing by the
    prior total-return close is what makes both corrections cancel, and it is the
    same treatment ``_absorb`` gives the leg return, which is the commensurability
    requirement.

    ⚠ THIS FUNCTION NEVER RAISES ON DATA, only on misaligned inputs, so a leg's
    LEG-level verdict is never pre-empted by arithmetic on a bar it did not hold.

    ⚠ Index 0 ends no boundary.  It carries ``no_session_boundary`` rather than a
    ``None`` reason, so ``measurable <=> reason is None`` holds at every index and
    a misuse is counted rather than silently measured.
    """
    count = len(dates)
    if not (len(opens) == len(offsets) == count):
        raise ValueError("dates, opens and offsets must be positionally parallel")
    span = len(raw_closes)
    if len(wealth_closes) != span:
        raise ValueError(f"raw ({span}) and wealth ({len(wealth_closes)}) close arrays must be the same length")
    ordered_breaks = sorted(unresolved_breaks)
    values = [float("nan")] * count
    reasons: list[ExclusionReason | None] = [None] * count
    if count:
        reasons[0] = "no_session_boundary"
    for index in range(1, count):
        prior_offset, offset = offsets[index - 1], offsets[index]
        if prior_offset is None or offset is None or not (0 <= prior_offset < span) or not (0 <= offset < span):
            reasons[index] = "off_axis"
            continue
        if (dates[index] - dates[index - 1]).days > hole_days:
            reasons[index] = "session_hole_spanned"
            continue
        # ⚠⚠ A REALISED LEG *CAN* SPAN AN UNRESOLVED BREAK, and an earlier draft
        # asserted it could not.  `segment_end_index` guards the LEVEL-resolved
        # path only (`backtest_run.py:953-957`); a `signal_pair` / calendar /
        # max-hold exit never goes through the resolver, and Codex checkpoint 1
        # built one that crossed a supplied break and booked -50.72%.  Left
        # unexcluded, a 1:2 rescale prints a -50% "gap" that never happened --
        # and because this statistic is an EXTREMUM, one such boundary does not
        # bias the estimate, it BECOMES it.
        #
        # `price_segments.py:1-6`: *"`price_series_break.break_date` is the first
        # date at the new scale.  Bars on either side remain usable inside their
        # own segment; indicators and positions must not span the boundary."*
        if bisect_right(ordered_breaks, dates[index]) > bisect_right(ordered_breaks, dates[index - 1]):
            reasons[index] = "scale_break_spanned"
            continue
        if dates[index] >= provisional_from or dates[index - 1] >= provisional_from:
            # ⚠ Withheld because a bar inside the archive's pinned correction
            # window may still be REVISED -- not because it is necessarily a part
            # session.  `PROVISIONAL_WINDOW_DAYS` covers completed sessions too,
            # and the earlier claim that it does not was too strong.  The cutoff
            # is the archive's own `quarantine_as_of`, never today: a diagnostic
            # whose value moves with the wall clock is not reproducible.
            reasons[index] = "provisional_bar"
            continue
        bar_open = _finite_positive(opens[index])
        if not np.isfinite(bar_open):
            reasons[index] = "open_unusable"
            continue
        prior_wealth = wealth_closes[prior_offset]
        wealth = wealth_closes[offset]
        raw = raw_closes[offset]
        if not (_usable(prior_wealth) and _usable(wealth) and _usable(raw)):
            reasons[index] = "close_unusable"
            continue
        gap = ((bar_open / raw) * (wealth / prior_wealth) - 1.0) * 100.0
        # ⚠ The FINAL PERCENTAGE is checked, not the ratio: a finite ratio can
        # leave float range when scaled, and the scaled value is what is read.
        if not np.isfinite(gap):
            reasons[index] = "non_finite"
            continue
        values[index] = gap
    return tuple(values), tuple(reasons)


def _finite_positive(value: Decimal | float | int | None) -> float:
    """``price_quarantine.rule_b1``'s open clause, on this module's inputs.

    ⚠ NOT "reused verbatim" -- ``price_quarantine``'s helpers take a ``Bar`` and
    return ``Decimal | None``, while ``_absorb`` holds ``OHLCVRow`` mappings.  The
    PREDICATE is identical (``None`` or ``<= 0`` is unusable); the carrier is not.
    ``synthetic_control_run.py:582`` applies the same test to the same field.

    ⚠ The ``Decimal -> float`` conversion is GUARDED.  A signalling NaN raises
    rather than answering, and ``float()`` on an over-range ``Decimal`` gives an
    infinity; both become ``nan`` here, which the caller turns into
    ``open_unusable``, rather than escaping as an exception from a diagnostic.
    """
    if value is None:
        return float("nan")
    try:
        numeric = float(value)
    except ValueError, ArithmeticError:
        return float("nan")
    if not np.isfinite(numeric) or numeric <= 0.0:
        return float("nan")
    return numeric


def _usable(value: float) -> bool:
    """``synthetic_control_run._usable`` -- finite and strictly positive."""
    return bool(np.isfinite(value)) and value > 0.0


@dataclass(frozen=True)
class ExitGapMeasurement:
    """One namespace's session-gap observations, or the reason there are none.

    ⚠⚠ ALWAYS PRESENT, never ``None``.  Withholding is expressed INSIDE this
    object so that ``measured + excluded == realised`` survives it -- a ``None``
    measurement would take the exclusion counts with it, which is precisely the
    accounting the diagnostic exists to provide.  ⚠ That includes the
    never-instrumented book: slice 7a returns ``None`` when nothing was recorded,
    which silently bypasses its own equality, and here that state is the named
    ``not_instrumented`` exclusion instead.
    """

    rule_version: str
    hole_days: int
    #: The run's resolved basis, or ``None`` where it could not be resolved.
    #: ⚠ REPORTED, NOT A GATE.  Unlike slice 7a this measurement is a RETURN on
    #: the basis the backtest already prices every leg on, so ``sql/305``'s
    #: unadjusted-level requirement -- which governs price and dollar-volume
    #: ATTRIBUTION -- does not apply.  What the label does not establish is that
    #: corporate-action treatment is consistent across every series, so it is
    #: carried rather than inferred.
    adjustment_basis: str | None
    #: ⚠ LOAD-BEARING, NOT DECORATION.  ``_dense_price_history`` substitutes the
    #: RAW close for the wealth close under ``LEGACY_RETURN_BASIS``, which makes
    #: the carry exactly 1 and this statistic a PRICE-ONLY gap rather than a
    #: total-return one.  Two runs reporting the same number mean different
    #: things unless this says which.
    return_basis: str
    realised_leg_count: int
    measured_leg_count: int
    excluded: Mapping[ExclusionReason, int]
    #: Boundaries actually measured, across the measured legs -- the denominator
    #: ``unmeasurable_boundary_count`` needs to be read as a rate.
    measured_boundary_count: int
    #: ⚠ COVERAGE, because the accounting equality does NOT establish it.  A
    #: measured leg can still have unmeasurable boundaries and the excluded one
    #: could have been its most extreme.  Counted ONCE PER (leg, boundary): a
    #: boundary held by two overlapping legs counts for each, and
    #: ``measured_boundary_count + unmeasurable_boundary_count`` is every
    #: boundary visited -- the denominator a coverage RATE needs, across
    #: measured and wholly excluded legs alike.
    #:
    #: ⚠⚠ MISSINGNESS HAS A KNOWN DIRECTION HERE, unlike an ordinary mean.
    #: Restoring any withheld boundary can only LOWER the minimum and RAISE the
    #: maximum, so the measured subset systematically understates tail magnitude
    #: or leaves it unchanged.  It is never an overstatement.
    partial_coverage_leg_count: int
    unmeasurable_boundary_count: int
    #: Measured legs on a series ``price_series_break`` cannot describe at all
    #: (#2721 step 3's negative name key), denominated on the measured
    #: population.  ⚠ A CAVEAT with no conservatism claim attached: "no break
    #: record" is not evidence of no break, and a positive key is not evidence of
    #: full coverage either -- the break table describes LIVE prices, while a
    #: research archive can carry historical scale defects of its own.
    unlinked_series_leg_count: int
    #: Realised legs this namespace held OPEN at the window end, which this
    #: statistic does not cover at all.  ⚠ Reported so the omitted exposure is
    #: visible: an open leg can hold the most extreme boundary in the run.
    open_leg_count: int
    #: ⚠ NULLABLE TOGETHER.  Undefined when nothing was measured, and an invented
    #: zero is what #2505's "missing is not zero" rule exists to prevent -- a
    #: measured zero is a run that gapped not at all, which is a different state.
    #:
    #: ⚠ NAMED FOR THE ARITHMETIC.  On an all-adverse population ``max_gap_pct``
    #: is negative and on an all-favourable one ``min_gap_pct`` is positive;
    #: "worst" and "best" would both be wrong there.
    min_gap_pct: Decimal | None
    max_gap_pct: Decimal | None
    p05_gap_pct: Decimal | None
    p50_gap_pct: Decimal | None
    binding_min: GapObservation | None
    binding_max: GapObservation | None

    def __post_init__(self) -> None:
        if self.realised_leg_count < 0 or self.measured_leg_count < 0 or self.open_leg_count < 0:
            raise ValueError("leg counts cannot be negative")
        unknown = set(self.excluded) - set(EXCLUSION_PRECEDENCE)
        if unknown:
            raise ValueError(f"unknown exclusion reasons {sorted(unknown)}")
        if any(value < 0 for value in self.excluded.values()):
            raise ValueError("exclusion counts cannot be negative")
        counted = self.measured_leg_count + sum(self.excluded.values())
        if counted != self.realised_leg_count:
            raise ValueError(
                f"{self.measured_leg_count} measured + {sum(self.excluded.values())} excluded = {counted} "
                f"against {self.realised_leg_count} realised legs — every leg carries exactly one verdict"
            )
        for name in ("partial_coverage_leg_count", "unlinked_series_leg_count"):
            value = getattr(self, name)
            if not 0 <= value <= self.measured_leg_count:
                raise ValueError(f"{name} counts a SUBSET of the {self.measured_leg_count} measured legs, got {value}")
        if self.measured_boundary_count < 0 or self.unmeasurable_boundary_count < 0:
            raise ValueError("boundary counts cannot be negative")
        if self.measured_boundary_count < self.measured_leg_count:
            raise ValueError(
                f"{self.measured_boundary_count} measured boundaries against {self.measured_leg_count} measured "
                "legs — a measured leg has at least one"
            )
        if self.partial_coverage_leg_count > self.unmeasurable_boundary_count:
            raise ValueError(
                f"{self.partial_coverage_leg_count} partially covered legs against "
                f"{self.unmeasurable_boundary_count} unmeasurable boundaries — each such leg has at least one"
            )
        summary = (
            self.min_gap_pct,
            self.max_gap_pct,
            self.p05_gap_pct,
            self.p50_gap_pct,
            self.binding_min,
            self.binding_max,
        )
        if self.measured_leg_count == 0:
            if any(field is not None for field in summary):
                raise ValueError("a measurement with no measured legs cannot carry a summary")
        elif any(field is None for field in summary):
            raise ValueError("a measurement with measured legs must carry a complete summary")
        if self.max_gap_pct is not None and self.min_gap_pct is not None and self.max_gap_pct < self.min_gap_pct:
            raise ValueError("the maximum gap cannot lie below the minimum")


def summarise(
    *,
    min_per_leg: Sequence[float],
    max_per_leg: Sequence[float],
    realised_leg_count: int,
    excluded: Mapping[ExclusionReason, int],
    measured_boundary_count: int,
    partial_coverage_leg_count: int,
    unmeasurable_boundary_count: int,
    unlinked_series_leg_count: int,
    open_leg_count: int,
    binding_min: GapObservation | None,
    binding_max: GapObservation | None,
    adjustment_basis: str | None,
    return_basis: str,
) -> ExitGapMeasurement:
    """Collapse the per-leg observations to what a human reads.

    ⚠ Quantile method is FROZEN as ``numpy.percentile``'s ``linear``
    interpolation, over the per-leg MINIMA.  Leaving it implicit lets identical
    inputs produce different p05s across versions.

    ⚠⚠ ROUNDING DIRECTION IS PART OF THE MEASUREMENT, and it is OUTWARD FROM THE
    INTERVAL, not away from zero: the minimum floors and the maximum ceilings, so
    a rounded pair always contains the raw one.  ⚠ The claim is about the
    QUANTISATION only.  Codex reproduced the limit: an exact gap of
    ``-0.1000000000000000000100%`` is already ``-0.09999999999998899`` as a float
    before any rounding happens, and flooring that gives ``-0.1000%`` — above the
    truth.  Float error is not controlled here and is not claimed to be.

    ⚠ Binding selection happens on the RAW floats, before any rounding.
    """
    count = len(min_per_leg)
    if len(max_per_leg) != count:
        raise ValueError("the per-leg minimum and maximum columns are positionally parallel")
    # ⚠ VALIDATED BEFORE FILTERING, not after. The comprehension below keeps
    # only known reasons, so an unknown one would be silently DROPPED and would
    # then break `measured + excluded == realised` with a message blaming the
    # counts rather than the typo. Found by its own test.
    unknown = set(excluded) - set(EXCLUSION_PRECEDENCE)
    if unknown:
        raise ValueError(f"unknown exclusion reasons {sorted(unknown)}")
    frozen: dict[ExclusionReason, int] = {
        reason: int(excluded.get(reason, 0)) for reason in EXCLUSION_PRECEDENCE if excluded.get(reason)
    }
    common = {
        "rule_version": EXIT_GAP_RULE_VERSION,
        "hole_days": HOLE_DAYS,
        "adjustment_basis": adjustment_basis,
        "return_basis": return_basis,
        "realised_leg_count": realised_leg_count,
        "excluded": frozen,
        "unmeasurable_boundary_count": unmeasurable_boundary_count,
        "open_leg_count": open_leg_count,
    }
    if count == 0:
        return ExitGapMeasurement(
            measured_leg_count=0,
            measured_boundary_count=0,
            partial_coverage_leg_count=0,
            unlinked_series_leg_count=0,
            min_gap_pct=None,
            max_gap_pct=None,
            p05_gap_pct=None,
            p50_gap_pct=None,
            binding_min=None,
            binding_max=None,
            **common,
        )
    if binding_min is None or binding_max is None:
        raise ValueError("a measured population must carry both binding observations")
    lows = np.asarray(min_per_leg, dtype=np.float64)
    highs = np.asarray(max_per_leg, dtype=np.float64)
    return ExitGapMeasurement(
        measured_leg_count=count,
        measured_boundary_count=measured_boundary_count,
        partial_coverage_leg_count=partial_coverage_leg_count,
        unlinked_series_leg_count=unlinked_series_leg_count,
        min_gap_pct=_quantise(float(lows.min()), ROUND_FLOOR),
        max_gap_pct=_quantise(float(highs.max()), ROUND_CEILING),
        p05_gap_pct=_quantise(float(np.percentile(lows, 5.0, method="linear")), ROUND_FLOOR),
        p50_gap_pct=_quantise(float(np.percentile(lows, 50.0, method="linear")), ROUND_FLOOR),
        binding_min=binding_min,
        binding_max=binding_max,
        **common,
    )


def _quantise(value: float, rounding: str) -> Decimal:
    """4dp, with the rounding direction supplied rather than defaulted.

    ⚠ ``repr`` and not ``Decimal(float)``: the shortest round-tripping literal,
    so the stored number reads as the float it came from rather than as its exact
    binary expansion.
    """
    try:
        return Decimal(repr(value)).quantize(_EXPONENT, rounding=rounding, context=_QUANTISE_CONTEXT)
    except InvalidOperation:  # pragma: no cover - 800 digits covers every finite float
        raise ValueError(f"gap percentage {value!r} cannot be quantised") from None


def new_leg_column() -> array[float]:
    """The per-leg float column shape, so the book and the tests cannot disagree.

    ⚠ ``array('d')`` and not ``list[float]``: a Python list of 4.2M floats boxes
    every one of them (~32 bytes against 8), twice over -- which is the memory
    ``LegBook``'s own design exists to avoid.
    """
    return array("d")


__all__ = [
    "EXCLUSION_PRECEDENCE",
    "EXIT_GAP_RULE_VERSION",
    "HOLE_DAYS",
    "ExclusionReason",
    "ExitGapMeasurement",
    "GapObservation",
    "boundary_gaps",
    "new_leg_column",
    "summarise",
    "worse_of",
]
