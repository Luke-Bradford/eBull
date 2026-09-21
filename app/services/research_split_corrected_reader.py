"""The split-corrected RATIO basis for a research series — #2834 §7 item 2, slice C.

WHAT THIS IS
------------
`4e9a121d` derived the correction (`research_split_adjustment`) and stored
nothing. This module is the CONSUMER that slice left open: it reads one series'
stored split stamps, builds the scale, and hands a strategy a second
``BarSeries`` to compute RATIOS on, beside the as-traded bars it already reads.

    ratio_basis(d) = as_traded(d) / scale(d),  scale(d) = ∏ factors after d

⚠⚠ A CORRECTED LEVEL CARRIES ITS ANCHOR'S FUTURE. A CORRECTED RATIO DOES NOT.
THAT ASYMMETRY IS THE WHOLE DESIGN, SO IT IS STATED BEFORE ANYTHING ELSE.

This module anchors every series at the END of the loaded window, so for any
bar ``d`` before the last one ``scale(d)`` is a product over events AFTER ``d``
and the corrected level embeds information no observer had at ``d`` —
including that the name would later reverse-split, which is the single most
informative thing you can know about a distressed penny stock. Compare such a
level against a constant and you have built a look-ahead filter.

⚠ Stated precisely because a looser version of it is false. It is not that a
restated level is inherently unavailable: a level restated using only events
known by ``t`` IS available at ``t``, and at ``t`` itself ``scale(t) = 1`` when
the anchor is ``t``. The claim is about the anchor THIS MODULE uses — the
window's last bar — under which every earlier level is a function of that bar's
past, not of ``t``'s.

A RATIO of two corrected levels is a different object, and the difference is
arithmetic rather than a judgement. For ``a`` LATER than ``b``::

    ratio_basis(a)   as_traded(a) / scale(a)   as_traded(a)
    ------------- =  ----------------------- = ------------ x  ∏ factors in (b, a]
    ratio_basis(b)   as_traded(b) / scale(b)   as_traded(b)

Every factor after ``a`` appears in ``scale(a)`` and in ``scale(b)`` and
cancels. What survives is the product over the HALF-OPEN interval ``(b, a]``,
which **excludes ``b``'s own stamp and INCLUDES ``a``'s** — every one of them in
the past at ``a``. (For ``a`` earlier than ``b`` the multiplier is the
reciprocal of that product; for ``a = b`` it is 1.)

⚠ "Strictly between the two bars" is the WRONG reading and an earlier draft of
this paragraph used it. ``a``'s own factor is the bar that first prints the
post-split level, so it is part of the re-denomination the window needs, not an
event after it.

So the ratio is invariant to the anchor, is computable from information
available at ``max(a, b)``, and is exactly the re-denomination a return across a
split needs.

⚠⚠ THREE LIMITS ON THAT, NAMED BECAUSE THE ALGEBRA INVITES OVERREADING IT:

1. **It is denomination invariance, not a point-in-time certificate.** The
   cancellation says nothing about backdated or revised stamps, revised closes,
   coverage selection or survivor selection. Nothing here reads an observation
   timestamp.
2. **It is exact in rational arithmetic, not necessarily in the caller's.**
   ``corrected_price`` divides and the quotient is deliberately not trapped
   (``research_split_adjustment``), so two separately-rounded quotients can
   leave a residue of the common factor. At ``prec=4``, ``1/2`` is ``0.5`` but
   ``(1/3)/(2/3)`` is ``0.4999``. At this codebase's default context the
   residue is far below any decile cut, but it is a tolerance rather than an
   identity, and an exact score TIE at a cut is counted rather than resolved
   by it.
3. **A future factor can still fail the READ even where it cancels.** Every
   factor in the window is validated and multiplied, so an invalid stamp after
   ``a`` raises for the whole series rather than returning a ratio that would
   have been correct.

Two consequences this module is built around:

* the ratio series is safe to slice at an in-sample boundary — the anchor moves,
  every ratio inside is unchanged — which is why ``through_date`` is honoured
  here rather than worked around;
* a strategy must NOT compare a level off it against a CONSTANT. ``$1`` floors
  and tick-size reasoning belong on the as-traded bars. The field is named
  ``ratio_basis`` and not ``corrected_close`` for that reason: the name is the
  only enforcement available once the object is handed over.
  ⚠ Not every cross-bar test is a level test — a proportional gap check is a
  ratio and is fine here. The line is "compared against a constant", not
  "reads more than one bar".

WHY THIS CORPUS NEEDS IT AT ALL
-------------------------------
``research_price_daily`` is MIXED-BASIS and that is not a defect of this module
but the thing it exists to remove. Measured 2026-09-21 over the §4.0 validated
universe (6,776 instruments, 5,890 with a series):

    paperswithbacktest/Stocks-Daily-Price   split_adjusted   5,264 series
    icyDenev/Intrader                       unadjusted       5,151 series

A ratio taken across a split on the `unadjusted` half is not a return — it is
the split. Applying the correction there brings both halves onto one
split-consistent basis for ratio purposes, so this makes the corpus MORE
single-basis, not less.

⚠ It does not make them one corpus. The two vendors still differ on the floor
basis (the `split_adjusted` half has no as-traded close at all, so §9 Q3's
floor stays restated there and this module cannot fix it), on depth and on
survivorship. See the s2 module's own note.

THE CORRECTION RUNS AFTER THE QUARANTINE, NEVER BEFORE IT
---------------------------------------------------------
#2834 §7 contract (a), settled here — ``load_ratio_basis`` takes bars that
``load_masked_series`` has already masked, and ``price_quarantine`` keeps
reading the corpus as printed. The question was live because correcting first
would remove the split-shaped level breaks T3 contains. Four reasons, in
decreasing order of what reversing them would cost:

1. **It would make containment quality covary with vendor coverage.** The
   quarantine evaluates 30,572 research series; 22,879 carry stamps and the
   other 7,693 are ``corporate_action_stamps = 'absent'`` (measured
   2026-09-21). A corrected-basis quarantine therefore runs one rule on 74.8%
   of the corpus and another on the rest.
   ⚠ TWO HONEST WEAKENINGS (Codex ckpt-1). First, `absent` is not one
   population: most of it is the ALREADY-adjusted vendor, for which the
   as-printed bars are the split-consistent ones — so the split is not simply
   "corrected vs uncorrected". Second, the analogy to `d15e680e`'s refusal of
   ``apply_only_corroborated`` is a SHAPE and not its evidence: that refusal
   rested on a measured delisting-rate difference (1.67% vs 17.33%), and no
   such measurement has been taken across these two vendor groups. The
   argument that survives is that the change would introduce a per-vendor
   difference whose selection effect is unmeasured — which is a reason to
   measure before moving, not a proof that moving is worse.
2. **It contradicts what the quarantine IS.** Its header: *"A split and a bad
   print produce the same defect, so the rules never have to tell them apart —
   which is exactly why they work independently of the unbuilt #2231 split
   detector."* Reading corrected bars spends that property rather than keeping
   it.
3. **Most of its rules could not tell — but NOT all, and the exceptions are
   named rather than rounded off (Codex ckpt-1).** B1-B3 and W1 are WITHIN-bar
   tests, and a per-bar scale divides every OHLC field of a bar alike, so a
   test that is homogeneous under positive scaling (an ordering, a ratio) has
   scale-invariant inputs. Three carve-outs:
   * **B4 is not within-bar.** It reads the neighbouring bar across a calendar
     gap, and a split inside that gap changes the corrected level ratio
     ``scale_1/scale_2``. Its CALENDAR predicate (``hole_days``) is invariant;
     a price comparison across the gap is not.
   * **Homogeneity is a property of the predicate, not of the rule family.** A
     dollar-denominated tolerance, a strict equality or a volume predicate is
     not scale-invariant even inside one bar.
   * **Rounding can flip an equality.** The division is not exact, so a
     corrected comparison can separate or collapse values a raw one did not.
   The narrowed claim that survives: the T-transition rules are where the
   BULK of the sensitivity is, not the whole of it. This ground is therefore
   the weakest of the four and is not load-bearing on its own.
4. **The change would be invisible in the evidence as the schema stands.**
   ``RULE_SET_VERSION`` hashes the quarantine MODULE, not the bars it read, so
   verdicts produced on two bases would carry one version and read as the same
   evidence. ⚠ This is a MIGRATION constraint, not a data-treatment argument:
   rotating the rule id and recording the input basis on the coverage row would
   make it visible. It is listed last because it bounds the cost of reversing
   the decision, not its correctness — and the same hole already lets a
   raw-bar revision pass unnoticed.

⚠ The cost is accepted rather than waved away: T3 keeps quarantining
transitions a stamp could have explained, which is over-containment on exactly
the population that has stamps. That is the direction the quarantine already
publishes a bias for.

⚠⚠ AND THE DECISION IS RECORDED HERE RATHER THAN IN ``price_quarantine`` ON
PURPOSE. That module's ``RULE_SET_VERSION`` hashes its own source, and
``load_masked_series`` joins ``coverage.rule_set_version`` — so adding this
paragraph THERE would rotate the version, match zero stored coverage rows and
return zero bars for every series in the corpus until a full re-run. A
docstring that documents a decision not to change behaviour must not be the
thing that changes it.

Refs #2834, #2437.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Literal

import psycopg

from app.services.indicator_series import BarSeries
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.research_split_adjustment import (
    CORRECTABLE_STAMP_MARKERS,
    SPLIT_ADJUSTMENT_RULE_VERSION,
    StampsUnavailable,
    corrected_price,
    corrected_volume,
    require_correctable,
    split_scales,
)
from app.services.technical_analysis import OHLCVRow, ReadOnlyOHLCVRow

#: ⚠⚠ THE VALUE A STRATEGY IDENTITY MUST HASH — not
#: ``SPLIT_ADJUSTMENT_RULE_VERSION`` alone (Codex ckpt-2, 2026-09-21). That
#: constant covers the POLICY and the arithmetic in
#: ``research_split_adjustment``; it says nothing about this module, which
#: independently decides score-affecting behaviour: which series get corrected
#: at all (:func:`ratio_basis_method_for`), which bars the factors are read
#: over (``_FACTOR_SQL``'s coverage and ``through_date`` predicates) and what a
#: corrected row contains (``_corrected_row``). A later edit to any of those
#: would change every corrected score while
#: ``SPLIT_ADJUSTMENT_RULE_VERSION`` stood still, and new results would reuse
#: the old strategy identity — the exact silent-staleness
#: ``strategy_registry.INPUT_RULE_SETS`` exists to prevent.
#:
#: Composed as id + this module's source + the adjustment version, so it moves
#: when EITHER half moves. Same shape as ``SPLIT_ADJUSTMENT_RULE_VERSION``
#: itself and ``price_basis_carrier``'s.
SPLIT_CORRECTED_READER_RULE_VERSION: Final[str] = (
    f"split-corrected-reader-v1+{hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]}"
    f"+{SPLIT_ADJUSTMENT_RULE_VERSION}"
)

__all__ = [
    "SPLIT_ADJUSTMENT_RULE_VERSION",
    "SPLIT_CORRECTED_READER_RULE_VERSION",
    "CorrectedSeries",
    "RatioBasisMethod",
    "load_ratio_basis",
    "load_split_factors",
    "ratio_basis_method_for",
    "ratio_basis_series",
]

#: How one series reached a ratio basis. ⚠ RECORDED, NOT INFERRED: "the two
#: bases are identical" is true both when a vendor already ships adjusted bars
#: and when the correction was never applied, and those are opposite facts.
RatioBasisMethod = Literal["split_corrected", "vendor_already_adjusted"]

# ⚠ A NARROW MIRROR of `research_price_structure_store._LOAD_SQL`'s JOINs, not a
# second copy of the masked read. It returns two columns the masked loader does
# not carry, over exactly the bars that loader returns, so the two results align
# one-for-one and `_aligned_factors` can ASSERT that rather than trust it.
#
# The coverage JOIN is repeated verbatim for that alignment reason and for the
# fail-closed one it was written for: a bar outside an evaluated range is
# unchecked, not clean. `through_date` is honoured at the query boundary, never
# by an after-fetch slice — see the module docstring's first consequence.
_FACTOR_SQL: Final = """
    SELECT d.bar_date,
           d.split_factor
    FROM research_price_daily d
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    WHERE d.series_id = %(series_id)s
      AND (%(through_date)s::date IS NULL OR d.bar_date <= %(through_date)s::date)
    ORDER BY d.bar_date
"""

_MARKER_SQL: Final = "SELECT corporate_action_stamps FROM research_price_series WHERE series_id = %(series_id)s"


@dataclass(frozen=True)
class CorrectedSeries:
    """One series on both bases, aligned bar-for-bar.

    ⚠ THE PAIR IS THE POINT. A consumer needs both — the ratio on
    ``ratio_basis`` and every level test on ``as_traded`` — and handing them
    back separately is how a caller ends up with one of them twice. They are
    validated equal-length and equal-dated here so a mis-wire is an exception
    rather than a plausible number.
    """

    series_id: int
    #: The bars as the archive printed them. Levels, floors, fills, spreads.
    as_traded: BarSeries
    #: The same OHLC re-denominated to the loaded window's terminal unit.
    #: ⚠⚠ RATIOS ONLY — see the module docstring. A level off this series is a
    #: look-ahead read.
    #:
    #: ⚠⚠ ITS ``volume`` IS THE AS-TRADED ONE, UNCORRECTED, AND THAT IS WHY THE
    #: FIELD BELOW EXISTS. ``OHLCVRow.volume`` is ``int | None``, while a
    #: restated share count is genuinely fractional under a reverse split (1:10
    #: turns 5 shares into 0.5) and ``research_split_adjustment`` refuses to
    #: round it — rounding destroys the ``close * volume`` invariance the pair
    #: exists to preserve. Writing a rounded int into this row would be a
    #: quiet lie in the field a volume consumer reads, so the corrected count
    #: is carried BESIDE the bars instead, as ``MaskedSeries.wealth_closes``
    #: already carries the dividend-adjusted level beside its own.
    ratio_basis: BarSeries
    #: Corrected share counts, aligned one-for-one with ``ratio_basis``' bars.
    #: ⚠ The published PAIR of ``ratio_basis``' prices — a consumer reading one
    #: without the other breaks ``close * volume`` invariance. Absent where the
    #: archive stored no volume.
    ratio_basis_shares: tuple[Decimal | None, ...]
    #: How many of the loaded bars carry a scale other than 1, i.e. how many the
    #: correction actually moved. ⚠ Reported rather than inferred: on a series
    #: with no stamped event the two bases are byte-identical, and a caller
    #: cannot otherwise tell "correctly unchanged" from "never applied".
    moved_bars: int

    def __post_init__(self) -> None:
        if self.as_traded.dates != self.ratio_basis.dates:
            raise ValueError(
                f"series {self.series_id}: the two bases do not carry the same dates "
                f"({len(self.as_traded.dates)} vs {len(self.ratio_basis.dates)}); they are not the same bars"
            )
        if len(self.ratio_basis_shares) != len(self.ratio_basis.dates):
            raise ValueError(
                f"series {self.series_id}: {len(self.ratio_basis_shares)} corrected share counts against "
                f"{len(self.ratio_basis.dates)} bars; they do not align"
            )
        if not 0 <= self.moved_bars <= len(self.as_traded.dates):
            raise ValueError(
                f"series {self.series_id}: moved_bars {self.moved_bars} is outside [0, {len(self.as_traded.dates)}]"
            )


def load_split_factors(
    conn: psycopg.Connection[Any],
    series_id: int,
    *,
    through_date: date | None = None,
) -> tuple[tuple[date, ...], tuple[Decimal | None, ...], str | None]:
    """The stamped factors of one series' evaluated bars, plus its stamp marker.

    Returns ``(dates, factors, marker)`` in ascending bar-date order over
    exactly the bars ``research_price_structure_store.load_masked_series``
    returns for the same ``series_id`` and ``through_date``.

    ⚠ The marker is returned rather than checked here. ``require_correctable``
    is the gate and it belongs at the point of division, not at the point of
    reading — a caller that only wants to know WHETHER a series is correctable
    should not have to catch an exception to find out.
    """
    rows = conn.execute(
        _FACTOR_SQL,
        {"series_id": series_id, "quarantine_version": QUARANTINE_RULE_SET_VERSION, "through_date": through_date},
    ).fetchall()
    marker_row = conn.execute(_MARKER_SQL, {"series_id": series_id}).fetchone()
    dates = tuple(row[0] for row in rows)
    factors = tuple(row[1] for row in rows)
    return dates, factors, (marker_row[0] if marker_row else None)


def _corrected_row(row: ReadOnlyOHLCVRow, scale: Decimal) -> tuple[OHLCVRow, Decimal | None]:
    """One bar re-denominated: prices divide, volume multiplies.

    ⚠ BOTH HALVES, ALWAYS, AND RETURNED TOGETHER. ``research_split_adjustment``
    publishes the two appliers as a pair precisely because correcting one and
    not the other breaks ``close * volume`` invariance. Returning a tuple is
    this module's share of that enforcement: the corrected share count cannot be
    dropped without deleting a binding the type checker is watching.
    """
    volume = row["volume"]
    return (
        {
            "open": corrected_price(row["open"], scale),  # type: ignore[typeddict-item]
            "high": corrected_price(row["high"], scale),  # type: ignore[typeddict-item]
            "low": corrected_price(row["low"], scale),  # type: ignore[typeddict-item]
            "close": corrected_price(row["close"], scale),  # type: ignore[typeddict-item]
            # ⚠ AS-TRADED, DELIBERATELY — see ``CorrectedSeries.ratio_basis``.
            "volume": volume,
        },
        corrected_volume(volume, scale),
    )


def ratio_basis_series(
    as_traded: BarSeries,
    *,
    series_id: int,
    factor_dates: Sequence[date],
    factors: Sequence[Decimal | None],
    stamps_marker: str | None,
) -> CorrectedSeries:
    """Pair one loaded series with its split-corrected ratio basis.

    ``factor_dates``/``factors`` come from :func:`load_split_factors` on the
    SAME ``series_id`` and ``through_date``. The dates are compared against the
    bars rather than assumed equal, which closes caller obligations 1 and 2 of
    ``split_scales``' docstring — order and completeness — at the only place
    where both sequences are in scope. ``split_scales`` itself receives bare
    factors and cannot check either.

    Refuses an uncorrectable series through ``require_correctable``: a marker of
    ``absent`` means the archive's corporate actions are UNKNOWN, and a scale of
    1 derived from that would convert a vendor with unknown splits into one with
    none.
    """
    require_correctable(stamps_marker)
    if tuple(factor_dates) != as_traded.dates:
        raise ValueError(
            f"the factor read returned {len(factor_dates)} dates against {len(as_traded.dates)} bars, or they "
            "differ in order or content; the scale of a bar is a claim about every later event in the SAME "
            "window, so a mismatched read silently re-anchors the result (split_scales, obligation 2)"
        )
    scales = split_scales(factors, stamps_marker=stamps_marker)
    corrected = [_corrected_row(row, scale) for row, scale in zip(as_traded.rows, scales, strict=True)]
    one = Decimal(1)
    return CorrectedSeries(
        series_id=series_id,
        as_traded=as_traded,
        ratio_basis=BarSeries(dates=as_traded.dates, rows=tuple(row for row, _ in corrected)),
        ratio_basis_shares=tuple(shares for _, shares in corrected),
        # ⚠ `!= 1` and not `> 1`: a reverse split stamps a factor BELOW one, so
        # a magnitude test would count only the forward half and report the
        # corpus as half-corrected.
        moved_bars=sum(1 for scale in scales if scale != one),
    )


def ratio_basis_method_for(adjustment_basis: str | None, stamps_marker: str | None) -> RatioBasisMethod:
    """How this series reaches a split-consistent ratio basis, or raise.

    The corpus has exactly three shapes and only one of them divides. Measured
    over ``research_price_series`` on 2026-09-21:

    ===========================  ==================  =======  ==================
    vendor                       adjustment_basis    stamps   method
    ===========================  ==================  =======  ==================
    icyDenev/Intrader            unadjusted          vendor_  split_corrected
                                                     supplied
    paperswithbacktest/…         split_adjusted      absent   vendor_already_…
    etoro/etoro-comparators-…    split_adjusted      absent   vendor_already_…
    cboe                         unadjusted          absent   REFUSED
    ===========================  ==================  =======  ==================

    ⚠⚠ THE FOURTH ROW IS THE REASON THIS IS A FUNCTION AND NOT AN ``if``.
    ``unadjusted`` + ``absent`` is a series whose bars are on the as-traded
    basis and whose corporate actions are UNKNOWN. There is no ratio basis for
    it — not the as-traded bars (a split inside a window makes that ratio the
    split) and not a corrected one (no stamps). It refuses rather than
    defaulting either way, because both defaults are wrong and one of them is
    silent.

    ⚠ ``split_adjusted`` + ``vendor_supplied`` is not in the table because the
    corpus has no such series today. It refuses too: dividing bars that are
    already adjusted would apply the correction twice, and this function will
    not guess which of the two the stamps describe.
    """
    if adjustment_basis == "split_adjusted" and stamps_marker not in CORRECTABLE_STAMP_MARKERS:
        return "vendor_already_adjusted"
    if adjustment_basis == "unadjusted" and stamps_marker in CORRECTABLE_STAMP_MARKERS:
        return "split_corrected"
    raise StampsUnavailable(
        f"a series on adjustment_basis {adjustment_basis!r} with corporate_action_stamps {stamps_marker!r} "
        "has no defined ratio basis: 'unadjusted' + 'absent' means the bars are as-traded and the corporate "
        "actions unknown, so neither the stored ratio nor a corrected one is a return; 'split_adjusted' + a "
        "stamp marker would apply the correction twice"
    )


def load_ratio_basis(
    conn: psycopg.Connection[Any],
    series_id: int,
    as_traded: BarSeries,
    *,
    through_date: date | None = None,
) -> tuple[CorrectedSeries, RatioBasisMethod]:
    """One loaded series paired with its ratio basis, and how it got one.

    ``as_traded`` is what ``research_price_structure_store.load_masked_series``
    returned for the same ``series_id`` and ``through_date``, already masked.
    The correction is applied to the MASKED bars deliberately: a bar the
    quarantine condemned must not re-enter through the adjusted basis, and a
    masked ``None`` divides to ``None``.

    ⚠ ``vendor_already_adjusted`` returns the same object on both sides. That
    is the declaration ``s2_member``'s docstring asks a caller to make, not a
    shortcut — and ``method`` is returned beside it so a census can report how
    many series took which path rather than inferring it from an equality that
    has two meanings.
    """
    basis_row = conn.execute(
        "SELECT adjustment_basis, corporate_action_stamps FROM research_price_series WHERE series_id = %(series_id)s",
        {"series_id": series_id},
    ).fetchone()
    if basis_row is None:
        raise StampsUnavailable(f"series {series_id} has no research_price_series row; its basis is undeclared")
    adjustment_basis, stamps_marker = basis_row
    method = ratio_basis_method_for(adjustment_basis, stamps_marker)
    if method == "vendor_already_adjusted":
        return (
            CorrectedSeries(
                series_id=series_id,
                as_traded=as_traded,
                ratio_basis=as_traded,
                # ⚠ `is None`, NOT a falsy test — a zero-volume bar is real and
                # `row["volume"] and Decimal(...)` would hand back a bare `int`
                # 0 where the field is declared `Decimal | None`. The prevention
                # log's "a falsy check is not a non-positive check" (2026-09-21)
                # is the same defect one type over.
                ratio_basis_shares=tuple(
                    None if row["volume"] is None else Decimal(row["volume"]) for row in as_traded.rows
                ),
                moved_bars=0,
            ),
            method,
        )
    dates, factors, marker = load_split_factors(conn, series_id, through_date=through_date)
    return (
        ratio_basis_series(
            as_traded,
            series_id=series_id,
            factor_dates=dates,
            factors=factors,
            stamps_marker=marker,
        ),
        method,
    )
