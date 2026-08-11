"""
Fail-closed reader that turns research-corpus bars into masked ``StructureBar``
inputs for ``price_structure`` (#2279, phase 2b of #2240).

SEPARATE MODULE FROM THE PRIMITIVES, for the same reason
``price_quarantine_store`` is separate from ``price_quarantine``:
``price_structure.RULE_SET_VERSION`` is a hash of its own source, so an
unrelated SQL tweak living in that file would look like a rule change and
invalidate every dependent signal.

WHY THE RESEARCH CORPUS NEEDS ITS OWN READER. ``price_quarantine_store``
already has ``usable_bar_filter_sql``, and it cannot be reused here for two
independent reasons:

1. It is keyed on ``instrument_id``. The research corpus is keyed on
   ``series_id`` precisely because 2,424 of its 7,693 series have no
   ``instruments`` row — that population is the eToro-listing-bias measurement
   (sql/249), so it must not be dropped by a join.
2. It gates on ``return_usable`` **only**. Price structure reads highs and lows,
   so ``range_usable`` governs most of it. A reader that checked only the return
   verdict would admit exactly the bars whose wicks are known to be wrong —
   which is the population swing detection is most sensitive to.

The *shape* is reused verbatim: coverage row, current rule-set version, bar
inside the evaluated range, ``COALESCE(..., TRUE)`` for the sparse-absence case.
All four are required. The COALESCE alone is NOT fail-closed — on its own it
makes a series that was never evaluated read as clean, which is the opposite of
the intent.

⚠⚠ TWO ARMS SINCE STAGE 5e-5a, AND THE DEFAULT IS UNCHANGED.
---------------------------------------------------------------------------
Criterion 9 requires a **sensitivity arm with conservative handling**, defined
by ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §9 C9 as *"re-run with
quarantined bars admitted at their stored values rather than masked"*. That arm
is this loader with ``arm="admitted"``; ``arm="masked"`` is the shipped default
and every existing caller keeps byte-identical behaviour.

⚠ The arm changes **masking only**. Series-level fail-closed is NOT an arm — a
series with no coverage row, or one evaluated at a stale rule-set version, still
returns zero bars under both arms. That exclusion is not a masked field whose
stored value could be admitted; it is a series the rules have never seen, and
"admit it anyway" would be a different decision from the one C9 names. It is
counted separately by the census instead (``quarantine_sensitivity``).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, Literal, get_args

import psycopg

from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.price_structure import StructureBar

# Bars, with their verdicts attached. LEFT JOIN because the verdict tables are
# SPARSE — a row exists only when it says something — so most bars have no
# match and read as clean, but ONLY inside an evaluated range.
_LOAD_SQL = """
    SELECT d.bar_date,
           d.open,
           d.high,
           d.low,
           d.close,
           d.adj_close,
           d.volume,
           COALESCE(q.range_usable, TRUE)  AS range_usable,
           COALESCE(q.return_usable, TRUE) AS return_usable
    FROM research_price_daily d
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN research_bar_quarantine q
      ON q.series_id = d.series_id
     AND q.bar_date = d.bar_date
     AND q.rule_set_version = %(quarantine_version)s
    WHERE d.series_id = %(series_id)s
    ORDER BY d.bar_date
"""


#: Criterion 9's declared pair. ``masked`` is what every strategy reads and is
#: the default everywhere; ``admitted`` is the sensitivity arm — the same bars
#: with the quarantined fields left at their STORED values.
#:
#: ⚠ NOT a third "drop the whole bar" arm. C9 asks what the exclusion COST, and
#: the only handling whose delta answers that is the one that stops excluding.
QuarantineArm = Literal["masked", "admitted"]
QUARANTINE_ARMS: Final[frozenset[str]] = frozenset(get_args(QuarantineArm))


@dataclass(frozen=True)
class MaskedSeries:
    """Bars ready for ``price_structure``, plus what masking cost.

    The counts are not diagnostics — they are the denominator for any claim
    about how much structure the quarantine suppressed. Reporting the masked
    bars without reporting the total would make an over-masking regression
    invisible.

    ⚠⚠ ``*_masked`` AND ``*_flagged`` ARE DIFFERENT QUANTITIES, and they differ
    exactly in the ``admitted`` arm. ``*_flagged`` is what the QUARANTINE said —
    arm-invariant, and criterion 9's census figure. ``*_masked`` is what this
    loader DID, which is zero under ``admitted``. Collapsing them would make the
    sensitivity arm report its own census as empty, i.e. an arm that measures
    the exclusion would print "nothing was excluded".
    """

    series_id: int
    bars: tuple[StructureBar, ...]
    #: Split-and-dividend-adjusted closes aligned one-for-one with ``bars``.
    #: These are portfolio WEALTH observations, never candle levels: signals,
    #: fills, TP/SL and spread bands continue to read raw ``bars`` OHLC.
    wealth_closes: tuple[Decimal | None, ...]
    range_masked: int
    return_masked: int
    #: The quarantine's verdict counts, independent of the arm. ⚠ NO DEFAULT:
    #: a caller that omits them would silently claim the quarantine flagged
    #: nothing, which is the favourable direction.
    range_flagged: int
    return_flagged: int
    #: Bars carrying EITHER verdict. ⚠ Not ``range_flagged + return_flagged``:
    #: the two overlap (measured on this corpus — every return-flagged bar is
    #: also range-flagged), so the sum double-counts and criterion 9 asks for a
    #: SHARE of bars, whose numerator has to be a bar count.
    bars_flagged: int
    arm: QuarantineArm = "masked"

    def __post_init__(self) -> None:
        # ⚠ Asserted rather than commented: the two pairs are equal in one arm
        # and maximally unequal in the other, so a future edit that wires the
        # wrong one through is caught here instead of in a printed figure that
        # looks plausible at any magnitude.
        if self.arm not in QUARANTINE_ARMS:
            raise ValueError(f"unknown quarantine arm {self.arm!r}; must be one of {sorted(QUARANTINE_ARMS)}")
        if len(self.wealth_closes) != len(self.bars):
            raise ValueError(
                f"wealth closes ({len(self.wealth_closes)}) do not align one-for-one with bars ({len(self.bars)})"
            )
        if self.range_masked > self.range_flagged or self.return_masked > self.return_flagged:
            raise ValueError(
                f"masked ({self.range_masked}, {self.return_masked}) exceeds flagged "
                f"({self.range_flagged}, {self.return_flagged}): a field can only be masked on a flagged bar"
            )
        if not max(self.range_flagged, self.return_flagged) <= self.bars_flagged <= len(self.bars):
            raise ValueError(
                f"bars_flagged {self.bars_flagged} is outside "
                f"[{max(self.range_flagged, self.return_flagged)}, {len(self.bars)}]: a bar count cannot be below "
                "either field's count nor above the series"
            )
        expected_masked = (0, 0) if self.arm == "admitted" else (self.range_flagged, self.return_flagged)
        if (self.range_masked, self.return_masked) != expected_masked:
            raise ValueError(
                f"arm {self.arm!r} masked ({self.range_masked}, {self.return_masked}) against flagged "
                f"({self.range_flagged}, {self.return_flagged}): the masked arm masks every flagged field and "
                "the admitted arm masks none"
            )

    @property
    def evaluated(self) -> bool:
        return bool(self.bars)


def load_masked_series(
    conn: psycopg.Connection[Any],
    series_id: int,
    *,
    arm: QuarantineArm = "masked",
) -> MaskedSeries:
    """Load one research series with quarantined fields masked to ``None``.

    Fail-closed at the SERIES level: a series with no coverage row, or one
    evaluated at a stale ``rule_set_version``, returns **zero bars** rather than
    its raw bars. That is deliberate and is the opposite of the convenient
    default — an unevaluated series is not "clean", it is unchecked, and letting
    it through would silently admit precisely the population the rules have not
    seen. ⚠ Unchanged by ``arm``; see the module header.

    Masking is per FIELD, not per bar, because the two verdicts mean different
    things: ``range_usable = False`` is a bad wick (masks high/low),
    ``return_usable = False`` is a bad close (masks close). Masking the whole bar
    on either verdict would discard good data and shift every N-bar window.

    ⚠⚠ THE OPEN IS MASKED ON ITS VALUE, NOT ON A VERDICT (#2354). It is the one
    OHLC field the quarantine has no axis for, and this loader used to carry it
    through untouched — so a stored ``open = 0`` reached the fill path and
    ``signal_ledger.resolve_fills`` booked a fill at price 0. The rule applied
    here is not invented: it is ``price_quarantine.rule_b1``'s own clause,
    *"any of open/high/low/close NULL or <= 0"*, applied to the field the
    two-axis masking cannot reach. Measured on the full corpus 2026-08-08, every
    non-positive open in ``research_price_daily`` (16 bars / 9 series) and in
    ``price_daily`` (154 bars / 14 instruments) carries ``rules = ['B1']`` with
    BOTH axes false — so this masks no bar the quarantine had not already
    condemned, and adds nothing to criterion 9's census.

    ``arm="admitted"`` is criterion 9's sensitivity arm: the same rows, the same
    verdicts counted, and every flagged field passed through at its **stored**
    value. It is a measurement of what masking cost, never a production read.
    """
    rows = conn.execute(
        _LOAD_SQL,
        {"series_id": series_id, "quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    return _apply_arm(series_id, rows, arm=arm)


def _apply_arm(
    series_id: int,
    rows: list[Any],
    *,
    arm: QuarantineArm,
) -> MaskedSeries:
    """The masking rule, separated from the fetch so both arms read ONE query.

    ⚠ Split for correctness before efficiency. A verifier running both arms with
    two `load_masked_series` calls would read the corpus twice, and two reads are
    two chances for the arms to differ by something other than the arm.
    """
    if arm not in QUARANTINE_ARMS:
        raise ValueError(f"unknown quarantine arm {arm!r}; must be one of {sorted(QUARANTINE_ARMS)}")
    admit = arm == "admitted"

    bars: list[StructureBar] = []
    range_flagged = 0
    return_flagged = 0
    bars_flagged = 0
    wealth_closes: list[Decimal | None] = []
    for bar_date, open_, high, low, close, adj_close, volume, range_usable, return_usable in rows:
        if not range_usable:
            range_flagged += 1
        if not return_usable:
            return_flagged += 1
        if not (range_usable and return_usable):
            bars_flagged += 1
        bars.append(
            StructureBar(
                bar_date=bar_date,
                # ⚠ Value-keyed, and it follows the ARM like every other field:
                # `admitted` is criterion 9's "stored values rather than masked"
                # and an exception for the open would make the arm no longer the
                # thing C9 names. The admitted arm is safe to run because
                # `resolve_fills` refuses a non-positive open independently and
                # reports `unusable_fill_price` rather than crashing.
                open=open_ if (admit or (open_ is not None and open_ > 0)) else None,
                high=high if (range_usable or admit) else None,
                low=low if (range_usable or admit) else None,
                close=close if (return_usable or admit) else None,
                volume=volume,
            )
        )
        wealth_closes.append(adj_close if (return_usable or admit) else None)

    return MaskedSeries(
        series_id=series_id,
        bars=tuple(bars),
        wealth_closes=tuple(wealth_closes),
        range_masked=0 if admit else range_flagged,
        return_masked=0 if admit else return_flagged,
        range_flagged=range_flagged,
        return_flagged=return_flagged,
        bars_flagged=bars_flagged,
        arm=arm,
    )


def load_arms(
    conn: psycopg.Connection[Any],
    series_id: int,
) -> dict[QuarantineArm, MaskedSeries]:
    """Both criterion-9 arms of one series, off a SINGLE fetch.

    ⚠ The point is that the two arms are guaranteed to describe the same rows.
    A caller looping over 5,266 series with two queries each would also be
    reading twice as much, but the reason this exists is the first one.
    """
    rows = conn.execute(
        _LOAD_SQL,
        {"series_id": series_id, "quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    return {arm: _apply_arm(series_id, rows, arm=arm) for arm in ("masked", "admitted")}


__all__ = [
    "QUARANTINE_ARMS",
    "QUARANTINE_RULE_SET_VERSION",
    "MaskedSeries",
    "QuarantineArm",
    "load_arms",
    "load_masked_series",
]
