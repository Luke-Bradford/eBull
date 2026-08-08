"""Fail-closed reader that turns LIVE-corpus bars into masked ``BarSeries``.

Spec: ``docs/proposals/ta/2026-08-08-strategy-signal-scan.md`` §4. Refs #2240,
#2394, #2354.

⚠⚠ WHY THIS EXISTS AT ALL: EVERY PHASE-5 FIGURE CAME OFF A DIFFERENT CORPUS.
``research_price_structure_store`` reads ``research_price_daily``, a frozen
archive keyed on ``series_id`` — last bar 2026-07-08, and only 5,269 of its
7,709 series carry an ``instrument_id`` at all. ``strategy_signals.instrument_id``
is a foreign key to ``instruments``, so **a daily scan cannot run on the research
corpus**. It reads ``price_daily``, and until this module there was no masked
reader for it.

⚠⚠ ``price_quarantine_store.usable_bar_filter_sql`` IS NOT THIS AND MUST NOT BE
PRESSED INTO SERVICE. It is a row **filter**; the strategies need a per-field
**mask**. ``research_price_structure_store.load_masked_series`` says why:
*"Masking the whole bar on either verdict would discard good data and shift
every N-bar window."* A filter silently shortens every warm-up window, which on
a corpus whose median depth is 613 bars is the difference between evaluating
``sma_200`` and reporting ``insufficient_warmup``.

⚠ ONE ARM, NOT TWO. ``research_price_structure_store`` carries criterion 9's
``masked``/``admitted`` pair because it feeds a sensitivity measurement. This is
a production read: *"criterion 9's ``admitted`` arm is a sensitivity measurement
that has no place in a scan"*. Adding the arm here would put a switch on the
production path whose only correct setting is the default.

⚠ IT RETURNS A ``BarSeries``, NOT ``StructureBar``s, WHICH IS THE OTHER HALF OF
WHY THE RESEARCH READER COULD NOT BE REUSED. ``price_structure`` consumes
``StructureBar``; the strategy modules consume ``BarSeries``. Same masking rule,
different output type and a different key column, so the SQL is mirrored field
for field rather than shared — and the mirror is checkable, because
``scripts/verify_2394_signal_scan_cost.py`` imports THIS module rather than
carrying its own copy.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from app.services.indicator_series import BarSeries
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.technical_analysis import OHLCVRow

#: What a masked field means in the strategies' own closed vocabulary. Every
#: caller of this loader owes ``strategy_registry`` this reason code, because
#: ``indicator_series`` knows THAT a value is absent and not why — *"Quarantine
#: and adjustment basis are the CALLER's gate."*
MASKED_REASON = "quarantined_bar"

# ⚠ A FIELD-FOR-FIELD MIRROR of `research_price_structure_store._LOAD_SQL`
# against the live corpus. The JOIN to the coverage table — not a LEFT JOIN — is
# the fail-closed rule: bars outside an evaluated range are not returned, because
# an unevaluated bar is unchecked rather than clean. The LEFT JOIN to the verdict
# table is right for the opposite reason: that table is SPARSE, a row exists only
# when it says something, so a missing row IS a clean bar inside a covered range.
#
# All four elements are required. The COALESCE alone is NOT fail-closed — on its
# own it makes an instrument that was never evaluated read as clean, which is the
# opposite of the intent.
_LOAD_SQL = """
    SELECT d.price_date,
           d.open,
           d.high,
           d.low,
           d.close,
           d.volume,
           COALESCE(q.range_usable, TRUE)  AS range_usable,
           COALESCE(q.return_usable, TRUE) AS return_usable
    FROM price_daily d
    JOIN price_quarantine_coverage cov
      ON cov.instrument_id = d.instrument_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN price_bar_quarantine q
      ON q.instrument_id = d.instrument_id
     AND q.price_date = d.price_date
     AND q.rule_set_version = %(quarantine_version)s
    WHERE d.instrument_id = %(instrument_id)s
    ORDER BY d.price_date
"""

#: The same coverage predicate as ``_LOAD_SQL``, aggregated. ⚠ It must stay the
#: same predicate: a population counted off raw ``price_daily`` counts bars the
#: fail-closed loader never returns, so the scan would report a universe it does
#: not reach. That exact error was caught by Codex in the measurement script and
#: is the reason this lives here beside the loader rather than in the caller.
_LAST_BAR_SQL = """
    SELECT d.instrument_id, max(d.price_date), count(*)
    FROM price_daily d
    JOIN price_quarantine_coverage cov
      ON cov.instrument_id = d.instrument_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
    WHERE d.instrument_id = ANY(%(instrument_ids)s)
    GROUP BY d.instrument_id
"""

_UNION_CALENDAR_SQL = """
    SELECT DISTINCT d.price_date
    FROM price_daily d
    JOIN price_quarantine_coverage cov
      ON cov.instrument_id = d.instrument_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
    WHERE d.instrument_id = ANY(%(instrument_ids)s)
    ORDER BY 1
"""


@dataclass(frozen=True)
class MaskedBars:
    """One instrument's live bars, ready for a strategy, plus what masking cost.

    The counts are not diagnostics — they are criterion 9's denominator. A caller
    reporting how many bars were masked without reporting how many there were
    makes an over-masking regression invisible.
    """

    instrument_id: int
    series: BarSeries
    #: Bars whose HIGH/LOW were masked (``range_usable = FALSE``).
    range_masked: int
    #: Bars whose CLOSE was masked (``return_usable = FALSE``).
    return_masked: int
    #: Bars carrying EITHER verdict. ⚠ Not the sum of the two: they overlap, and
    #: a share-of-bars figure needs a bar count as its numerator.
    bars_masked: int

    def __len__(self) -> int:
        return len(self.series)


@dataclass(frozen=True)
class InstrumentBarSpan:
    """How much of an instrument the loader would return, without returning it."""

    last_bar: date
    bars: int


def load_masked_bars(conn: psycopg.Connection[Any], instrument_id: int) -> MaskedBars:
    """Load one instrument's live bars with quarantined fields masked to ``None``.

    Fail-closed at the INSTRUMENT level: an instrument with no coverage row, or
    one evaluated at a stale ``rule_set_version``, returns **zero bars** rather
    than its raw bars. That is the opposite of the convenient default and is
    deliberate — an unevaluated instrument is not "clean", it is unchecked.

    Masking is per FIELD, because the two verdicts mean different things:
    ``range_usable = False`` is a bad wick (masks high/low), ``return_usable =
    False`` is a bad close (masks close).

    ⚠⚠ THE OPEN IS MASKED ON ITS VALUE, NOT ON A VERDICT (#2354). It is the one
    OHLC field the quarantine has no axis for. The rule applied is not invented:
    it is ``price_quarantine.rule_b1``'s own clause, *"any of open/high/low/close
    NULL or <= 0"*, applied to the field the two-axis masking cannot reach. Left
    unmasked, a stored ``open = 0`` reaches ``signal_ledger.resolve_fills`` and
    books a fill at price zero — which every reader downstream refuses.
    """
    rows = conn.execute(
        _LOAD_SQL,
        {"instrument_id": instrument_id, "quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()

    dates: list[date] = []
    bars: list[OHLCVRow] = []
    range_masked = 0
    return_masked = 0
    bars_masked = 0
    for bar_date, open_, high, low, close, volume, range_usable, return_usable in rows:
        if not range_usable:
            range_masked += 1
        if not return_usable:
            return_masked += 1
        if not (range_usable and return_usable):
            bars_masked += 1
        dates.append(bar_date)
        bars.append(
            {
                # ⚠ `type: ignore` on all four because `OHLCVRow` declares
                # non-optional Decimals while every field here is maskable. The
                # ignores are the standing record that the TypedDict is wrong
                # about nullability, not a shortcut — `research_price_structure_
                # store` carries the same shape through `StructureBar`, whose
                # fields ARE optional, and #2354 is the ticket where the
                # difference cost a fill at price 0.
                "open": open_ if (open_ is not None and open_ > 0) else None,  # type: ignore[typeddict-item]
                "high": high if range_usable else None,  # type: ignore[typeddict-item]
                "low": low if range_usable else None,  # type: ignore[typeddict-item]
                "close": close if return_usable else None,  # type: ignore[typeddict-item]
                "volume": volume,
            }
        )

    return MaskedBars(
        instrument_id=instrument_id,
        series=BarSeries(dates=tuple(dates), rows=tuple(bars)),
        range_masked=range_masked,
        return_masked=return_masked,
        bars_masked=bars_masked,
    )


def load_bar_spans(
    conn: psycopg.Connection[Any],
    instrument_ids: Sequence[int],
) -> dict[int, InstrumentBarSpan]:
    """Last bar date and bar count per instrument, through the LOADER's predicate.

    ⚠ The point of this function is that it does NOT load bars. The scan needs
    every instrument's last bar date to compute the frontier before it decides
    which instruments to evaluate, and loading 3.6M bars to read 6,547 dates
    would be the whole cost of the scan paid twice.

    ⚠ An instrument absent from the result loads as ZERO bars — that is the
    fail-closed exclusion, not a missing row to be defaulted. Callers must count
    it, because "the loader returns nothing for it" is one of the two reasons a
    validated-universe member produces no signal at all (spec §9).
    """
    rows = conn.execute(
        _LAST_BAR_SQL,
        {"instrument_ids": list(instrument_ids), "quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    return {
        int(instrument_id): InstrumentBarSpan(last_bar=last_bar, bars=int(bars))
        for instrument_id, last_bar, bars in rows
    }


def load_union_calendar(conn: psycopg.Connection[Any], instrument_ids: Sequence[int]) -> tuple[date, ...]:
    """Every date on which ANY of these instruments has a loadable bar, ascending.

    ⚠⚠ THE PANEL'S UNION CALENDAR IS NOT ANY ONE MEMBER'S, AND THE DIFFERENCE IS
    LOAD-BEARING. ``s2_cross_sectional_momentum.rebalance_dates`` fires on *"the
    first bar whose calendar month differs from the previous bar's"*. Handed a
    per-member calendar it fires on that member's own first bar of each month, so
    names that resumed on different days rank against nobody and the
    cross-section collapses — measured as ``thin_cross_section (1 < 10)`` by an
    earlier version of the cost arm, which was a measurement of the bug and not
    of S-2.
    """
    rows = conn.execute(
        _UNION_CALENDAR_SQL,
        {"instrument_ids": list(instrument_ids), "quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    return tuple(row[0] for row in rows)


__all__ = [
    "MASKED_REASON",
    "QUARANTINE_RULE_SET_VERSION",
    "InstrumentBarSpan",
    "MaskedBars",
    "load_bar_spans",
    "load_masked_bars",
    "load_union_calendar",
]
