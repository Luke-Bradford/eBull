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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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


@dataclass(frozen=True)
class MaskedSeries:
    """Bars ready for ``price_structure``, plus what masking cost.

    The two counts are not diagnostics — they are the denominator for any claim
    about how much structure the quarantine suppressed. Reporting the masked
    bars without reporting the total would make an over-masking regression
    invisible.
    """

    series_id: int
    bars: tuple[StructureBar, ...]
    range_masked: int
    return_masked: int

    @property
    def evaluated(self) -> bool:
        return bool(self.bars)


def load_masked_series(
    conn: psycopg.Connection[Any],
    series_id: int,
) -> MaskedSeries:
    """Load one research series with quarantined fields masked to ``None``.

    Fail-closed at the SERIES level: a series with no coverage row, or one
    evaluated at a stale ``rule_set_version``, returns **zero bars** rather than
    its raw bars. That is deliberate and is the opposite of the convenient
    default — an unevaluated series is not "clean", it is unchecked, and letting
    it through would silently admit precisely the population the rules have not
    seen.

    Masking is per FIELD, not per bar, because the two verdicts mean different
    things: ``range_usable = False`` is a bad wick (masks high/low),
    ``return_usable = False`` is a bad close (masks close). Masking the whole bar
    on either verdict would discard good data and shift every N-bar window.
    """
    rows = conn.execute(
        _LOAD_SQL,
        {"series_id": series_id, "quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()

    bars: list[StructureBar] = []
    range_masked = 0
    return_masked = 0
    for bar_date, open_, high, low, close, volume, range_usable, return_usable in rows:
        if not range_usable:
            range_masked += 1
        if not return_usable:
            return_masked += 1
        bars.append(
            StructureBar(
                bar_date=bar_date,
                open=open_,
                high=high if range_usable else None,
                low=low if range_usable else None,
                close=close if return_usable else None,
                volume=volume,
            )
        )

    return MaskedSeries(
        series_id=series_id,
        bars=tuple(bars),
        range_masked=range_masked,
        return_masked=return_masked,
    )


__all__ = [
    "QUARANTINE_RULE_SET_VERSION",
    "MaskedSeries",
    "load_masked_series",
]
