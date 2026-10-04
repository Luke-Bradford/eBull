"""#3619 slice 2 — monthly total return per admitted name, Intrader spliced onto PWB.

Spec: ``docs/proposals/etl/2026-10-04-3619-total-return-splice.md``. The unit is the
``survivorship_free`` selection's ``name_key``; this module never changes membership, it only
decides which vendor supplies each month's return for a name the selection admitted.

Source rule: none exists for splicing two vendor archives. Every rule here is fixed by
construction from #3619 slice 1's measurements and frozen by this module's source hash, the
``universe_selection`` idiom. Inputs with a documented rule are cited in the spec: Intrader
``close`` is raw and ``adj_close`` carries splits and dividends; PWB ``close`` is split-adjusted
and ``adj_close`` adds dividends; bar validity is the #2261 quarantine, fail-closed outside a
current coverage range.

⚠ Returns chain, levels never splice: each vendor back-adjusts ``adj_close`` to its own capture
date, so a return is only ever computed between two month-ends of the SAME series.
"""

from __future__ import annotations

import hashlib
import statistics
from bisect import bisect_right
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Final, Literal

import psycopg

from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.universe_selection import (
    INTRADER_CAPTURE_DATE,
    SURVIVORSHIP_FREE_VENDOR,
    UNIVERSE_SELECTION_RULE_VERSION,
    UniverseSelection,
)

_RULE_ID: Final = "total-return-splice-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


TOTAL_RETURN_SPLICE_VERSION: Final[str] = f"{_RULE_ID}+{_code_hash()}"

Month = tuple[int, int]

INTRADER_VENDOR: Final = SURVIVORSHIP_FREE_VENDOR
#: The 2026-09-09 PWB capture (#3619 slice 3), a separate vendor from the 2026-07-08 load that
#: ``universe_selection.SURVIVOR_ONLY_VENDOR`` still names. A literal, not an import of
#: ``research_corpus_ingest.HF_ARCHIVE_2026_09_09.vendor``, so the capture this reads is inside this
#: module's source hash and moving it is a new splice version; a test pins the two equal.
PWB_VENDOR: Final = "paperswithbacktest/Stocks-Daily-Price@2026-09-09"

#: PWB's freeze date. Declared and asserted equal to ``max(last_bar)`` at load, as
#: ``universe_selection._assert_capture`` does for Intrader: a re-loaded capture must refuse here
#: until the constant is re-frozen deliberately.
PWB_CAPTURE_DATE: Final = date(2026, 9, 9)

#: First month PWB supplies a spliced name's return. Slice 1's XBRL dividend miss rates cross
#: here: Intrader 1.2% vs PWB 1.6% in 2021, 2.7% vs 1.5% in 2022.
SWITCH_MONTH: Final[Month] = (2022, 1)

#: Identity gate: median |Intrader close return − PWB close return| over the months the splice
#: uses PWB while Intrader is still available (``SWITCH_MONTH`` .. ``LAST_SURVIVORSHIP_FREE_MONTH``)
#: must be below this, over at least ``IDENTITY_MIN_MONTHS`` paired months. Both values are a
#: policy fixed by construction at the measured distribution (spec §"Why these thresholds").
IDENTITY_MAX_MEDIAN: Final = 0.0005
IDENTITY_MIN_MONTHS: Final = 12


def month_of(day: date) -> Month:
    return (day.year, day.month)


def add_months(month: Month, k: int) -> Month:
    n = month[0] * 12 + (month[1] - 1) + k
    return (n // 12, n % 12 + 1)


#: The last month Intrader supplies, for every series: its capture month (2024-09) is partial for a
#: series alive at capture, and from that month the panel can no longer see names that die, so every
#: later month is survivor-only.
LAST_SURVIVORSHIP_FREE_MONTH: Final[Month] = add_months(month_of(INTRADER_CAPTURE_DATE), -1)
#: The last month PWB supplies: its capture month (2026-09) is partial.
LAST_PWB_MONTH: Final[Month] = add_months(month_of(PWB_CAPTURE_DATE), -1)


class SpliceVerdict(StrEnum):
    """One per admitted name, assigned in this precedence order."""

    TERMINATING = "terminating"
    NO_PWB_SERIES = "no_pwb_series"
    REFUSED_INSUFFICIENT_OVERLAP = "refused_insufficient_overlap"
    REFUSED_PRICE_DISAGREEMENT = "refused_price_disagreement"
    SPLICED = "spliced"


@dataclass(frozen=True, slots=True)
class MonthEnd:
    """The last quarantine-usable bar of a calendar month in one series."""

    bar_date: date
    adj_close: float
    close: float


@dataclass(frozen=True, slots=True)
class PeriodReturn:
    value: float
    #: The month-end bars the return runs between: ``(start_bar, end_bar]``.
    start_bar: date
    end_bar: date


@dataclass(frozen=True, slots=True)
class MonthlyTotalReturn:
    name_key: int
    #: First day of the calendar month the return is assigned to.
    month: date
    total_return: float
    #: The actual bars the return runs between. ``end_bar`` is the month's last usable bar, which is
    #: earlier than the calendar month-end when later bars are missing or quarantined, and is a
    #: series' last bar in its final month — an observed return, never a realised termination.
    start_bar: date
    end_bar: date
    vendor: str
    series_id: int
    #: An Intrader-sourced month from ``SWITCH_MONTH`` on: a source-risk indicator (its dividend
    #: capture degrades), not proof that this month missed a payment.
    dividend_capture_degraded: bool
    #: A month after ``LAST_SURVIVORSHIP_FREE_MONTH``: names that died then are invisible.
    survivor_only: bool


@dataclass(frozen=True, slots=True)
class IdentityCheck:
    paired_months: int
    median_abs_diff: float | None
    passed: bool


@dataclass(frozen=True)
class NameSplice:
    verdict: SpliceVerdict
    identity: IdentityCheck | None
    rows: tuple[MonthlyTotalReturn, ...]


@dataclass(frozen=True)
class TotalReturnPanel:
    #: This module's version plus the quarantine and selection rule versions it read under. The
    #: capture assertions are endpoint checks; prices, links and verdicts can still move beneath them.
    version: str
    rows: tuple[MonthlyTotalReturn, ...]
    verdicts: Mapping[int, SpliceVerdict]
    identity: Mapping[int, IdentityCheck]
    #: ``name_key`` → the PWB series considered for it (spliced or refused).
    pwb_series: Mapping[int, int]

    def census(self) -> Counter[SpliceVerdict]:
        return Counter(self.verdicts.values())


# ---------------------------------------------------------------------------
# Pure rules
# ---------------------------------------------------------------------------


def monthly_returns(
    month_ends: Mapping[Month, MonthEnd],
    *,
    field: Literal["adj_close", "close"],
    through: Month,
) -> dict[Month, PeriodReturn]:
    """Simple return for each month up to ``through`` whose previous calendar month has a level.

    Chain-linked period-end returns, the conventional construction. A month with no usable level
    breaks the chain: neither it nor the month after it has a return, and no row is emitted for
    either — a missing return is an absent row, never a zero.
    """
    out: dict[Month, PeriodReturn] = {}
    for month, end in month_ends.items():
        if month > through:
            continue
        previous = month_ends.get(add_months(month, -1))
        if previous is None:
            continue
        out[month] = PeriodReturn(getattr(end, field) / getattr(previous, field) - 1.0, previous.bar_date, end.bar_date)
    return out


def identity_check(
    intrader: Mapping[Month, MonthEnd],
    pwb: Mapping[Month, MonthEnd],
    intrader_split_dates: Sequence[date],
) -> IdentityCheck:
    """Do the two series trace the same traded price over the months the splice would use PWB?

    A cross-archive CONSISTENCY check, not corroboration: both archives are Yahoo derivatives, so
    agreement cannot show either is right — only that they describe the same security.

    - Close returns, not ``adj_close`` returns: Intrader's missing dividends from 2022 are the
      defect the splice fixes, so an ``adj_close`` comparison refuses exactly the monthly payers it
      should splice.
    - Only ``SWITCH_MONTH`` .. ``LAST_SURVIVORSHIP_FREE_MONTH``: earlier months come from Intrader
      whatever the verdict, so identity before the switch decides nothing.
    - Only months whose two returns run between the SAME bar dates, so a difference measures the
      price path and not two different holding periods.
    - Not across an Intrader split stamp inside the return interval: Intrader's raw close jumps
      there and PWB's split-adjusted close does not. Every stamp counts, quarantined bar or not.
    """
    splits = sorted(intrader_split_dates)
    ours = monthly_returns(intrader, field="close", through=LAST_SURVIVORSHIP_FREE_MONTH)
    theirs = monthly_returns(pwb, field="close", through=LAST_SURVIVORSHIP_FREE_MONTH)
    diffs: list[float] = []
    for month, mine in ours.items():
        other = theirs.get(month)
        if month < SWITCH_MONTH or other is None:
            continue
        if (mine.start_bar, mine.end_bar) != (other.start_bar, other.end_bar):
            continue
        if bisect_right(splits, mine.end_bar) > bisect_right(splits, mine.start_bar):
            continue
        diffs.append(abs(mine.value - other.value))
    if len(diffs) < IDENTITY_MIN_MONTHS:
        return IdentityCheck(paired_months=len(diffs), median_abs_diff=None, passed=False)
    median = statistics.median(diffs)
    return IdentityCheck(paired_months=len(diffs), median_abs_diff=median, passed=median < IDENTITY_MAX_MEDIAN)


def _row(name_key: int, month: Month, ret: PeriodReturn, vendor: str, series_id: int) -> MonthlyTotalReturn:
    return MonthlyTotalReturn(
        name_key=name_key,
        month=date(month[0], month[1], 1),
        total_return=ret.value,
        start_bar=ret.start_bar,
        end_bar=ret.end_bar,
        vendor=vendor,
        series_id=series_id,
        dividend_capture_degraded=vendor == INTRADER_VENDOR and month >= SWITCH_MONTH,
        survivor_only=month > LAST_SURVIVORSHIP_FREE_MONTH,
    )


def splice_name(
    *,
    name_key: int,
    intrader_series_id: int,
    intrader: Mapping[Month, MonthEnd],
    intrader_split_dates: Sequence[date],
    terminating: bool,
    pwb_series_id: int | None,
    pwb: Mapping[Month, MonthEnd] | None,
) -> NameSplice:
    """Assign every month of one admitted name to at most one vendor (spec §"Contract").

    A terminating series never continues on PWB. Its archive endpoint does not prove the security
    died, but a PWB series still trading at 2026 under the same instrument has unverified
    continuity with it, so the conservative policy is to stop where Intrader stops.
    """
    intrader_returns = monthly_returns(intrader, field="adj_close", through=LAST_SURVIVORSHIP_FREE_MONTH)
    identity: IdentityCheck | None = None
    if terminating:
        verdict = SpliceVerdict.TERMINATING
    elif pwb_series_id is None or pwb is None:
        verdict = SpliceVerdict.NO_PWB_SERIES
    else:
        identity = identity_check(intrader, pwb, intrader_split_dates)
        if identity.passed:
            verdict = SpliceVerdict.SPLICED
        elif identity.median_abs_diff is None:
            verdict = SpliceVerdict.REFUSED_INSUFFICIENT_OVERLAP
        else:
            verdict = SpliceVerdict.REFUSED_PRICE_DISAGREEMENT

    rows: list[MonthlyTotalReturn] = []
    if verdict is SpliceVerdict.SPLICED:
        assert pwb is not None and pwb_series_id is not None
        pwb_returns = {
            month: ret
            for month, ret in monthly_returns(pwb, field="adj_close", through=LAST_PWB_MONTH).items()
            if month >= SWITCH_MONTH
        }
        for month in sorted(set(intrader_returns) | set(pwb_returns)):
            if month in pwb_returns:
                rows.append(_row(name_key, month, pwb_returns[month], PWB_VENDOR, pwb_series_id))
            elif month in intrader_returns:
                rows.append(_row(name_key, month, intrader_returns[month], INTRADER_VENDOR, intrader_series_id))
    else:
        rows = [
            _row(name_key, month, intrader_returns[month], INTRADER_VENDOR, intrader_series_id)
            for month in sorted(intrader_returns)
        ]
    return NameSplice(verdict=verdict, identity=identity, rows=tuple(rows))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

#: Month-end level per series: the last quarantine-usable bar of each calendar month, inside a
#: current-rule-set coverage range, with a finite positive close and adj_close. The ``factor_validation``
#: semantics exactly: a bar outside its series' coverage row (one per series, sql/251 PK) is
#: unusable; inside it, a bar with no ``research_bar_quarantine`` row is clean (the verdict table
#: is sparse) and one with a row is usable only if ``return_usable``. The finite filter sits before
#: ``DISTINCT ON`` so a non-finite last bar falls back to an earlier bar: numeric ``'NaN'`` sorts above
#: ``'Infinity'``, so ``> 0`` alone admits both and ``< 'Infinity'`` excludes both (PG 17, measured).
_MONTH_END_SQL = """
SELECT DISTINCT ON (d.series_id, date_trunc('month', d.bar_date))
       d.series_id, d.bar_date, d.adj_close::float8, d.close::float8
FROM research_price_daily d
JOIN research_price_quarantine_coverage cov
  ON cov.series_id = d.series_id
 AND cov.rule_set_version = %(quarantine_version)s
 AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
LEFT JOIN research_bar_quarantine q
  ON q.series_id = d.series_id
 AND q.bar_date = d.bar_date
 AND q.rule_set_version = %(quarantine_version)s
WHERE d.series_id = ANY(%(series_ids)s::bigint[])
  AND COALESCE(q.return_usable, TRUE)
  AND d.adj_close > 0 AND d.adj_close < 'Infinity'::numeric
  AND d.close > 0 AND d.close < 'Infinity'::numeric
ORDER BY d.series_id, date_trunc('month', d.bar_date), d.bar_date DESC
"""

#: Every Intrader split stamp, quarantined bar or not: a split moves the raw close regardless.
_SPLIT_DATES_SQL = """
SELECT series_id, bar_date
FROM research_price_daily
WHERE series_id = ANY(%(series_ids)s::bigint[])
  AND split_factor IS NOT NULL
  AND split_factor <> 1
"""

#: At most one PWB series per instrument: ``uq_research_price_series_vendor_instrument`` (sql/249).
_PWB_SERIES_SQL = """
SELECT instrument_id, series_id
FROM research_price_series
WHERE vendor = %(vendor)s
  AND instrument_id = ANY(%(instrument_ids)s::bigint[])
  AND bar_count IS NOT NULL
"""


def load_month_ends(conn: psycopg.Connection[Any], series_ids: Sequence[int]) -> dict[int, dict[Month, MonthEnd]]:
    out: dict[int, dict[Month, MonthEnd]] = {}
    params = {"series_ids": list(series_ids), "quarantine_version": QUARANTINE_RULE_SET_VERSION}
    with conn.cursor(name="total_return_month_ends") as cur:
        cur.itersize = 50_000
        cur.execute(_MONTH_END_SQL, params)
        for series_id, bar_date, adj_close, close in cur:
            out.setdefault(int(series_id), {})[month_of(bar_date)] = MonthEnd(bar_date, adj_close, close)
    return out


def load_split_dates(conn: psycopg.Connection[Any], series_ids: Sequence[int]) -> dict[int, list[date]]:
    out: dict[int, list[date]] = {}
    for series_id, bar_date in conn.execute(_SPLIT_DATES_SQL, {"series_ids": list(series_ids)}).fetchall():
        out.setdefault(int(series_id), []).append(bar_date)
    return out


def _assert_pwb_capture(conn: psycopg.Connection[Any]) -> None:
    row = conn.execute(
        "SELECT max(last_bar) FROM research_price_series WHERE vendor = %(vendor)s", {"vendor": PWB_VENDOR}
    ).fetchone()
    measured = row[0] if row else None
    if measured != PWB_CAPTURE_DATE:
        raise RuntimeError(
            f"declared PWB capture {PWB_CAPTURE_DATE} but max(last_bar) measures {measured} — the archive moved "
            "under the frozen constant; re-freeze it deliberately (a new splice version) before reading"
        )


def load_total_return_panel(conn: psycopg.Connection[Any], selection: UniverseSelection) -> TotalReturnPanel:
    """Monthly total returns for every name ``selection`` admitted, per the splice contract."""
    if selection.vendor != INTRADER_VENDOR or selection.capture_date != INTRADER_CAPTURE_DATE:
        raise ValueError(
            f"the splice is defined over the survivorship_free selection ({INTRADER_VENDOR}, capture "
            f"{INTRADER_CAPTURE_DATE}); got {selection.vendor!r} captured {selection.capture_date}"
        )
    _assert_pwb_capture(conn)
    alive_linked = sorted(
        {s.instrument_id for s in selection.admitted if s.termination is None and s.instrument_id is not None}
    )
    pwb_by_instrument = {
        int(instrument_id): int(series_id)
        for instrument_id, series_id in conn.execute(
            _PWB_SERIES_SQL, {"vendor": PWB_VENDOR, "instrument_ids": alive_linked}
        ).fetchall()
    }
    intrader_ids = [s.series_id for s in selection.admitted]
    month_ends = load_month_ends(conn, [*intrader_ids, *pwb_by_instrument.values()])
    splits = load_split_dates(conn, intrader_ids)

    rows: list[MonthlyTotalReturn] = []
    verdicts: dict[int, SpliceVerdict] = {}
    identity: dict[int, IdentityCheck] = {}
    pwb_by_name: dict[int, int] = {}
    for admitted in selection.admitted:
        terminating = admitted.termination is not None
        pwb_series_id = (
            pwb_by_instrument.get(admitted.instrument_id)
            if admitted.instrument_id is not None and not terminating
            else None
        )
        result = splice_name(
            name_key=admitted.name_key,
            intrader_series_id=admitted.series_id,
            intrader=month_ends.get(admitted.series_id, {}),
            intrader_split_dates=splits.get(admitted.series_id, []),
            terminating=terminating,
            pwb_series_id=pwb_series_id,
            pwb=month_ends.get(pwb_series_id, {}) if pwb_series_id is not None else None,
        )
        verdicts[admitted.name_key] = result.verdict
        if result.identity is not None and pwb_series_id is not None:
            identity[admitted.name_key] = result.identity
            pwb_by_name[admitted.name_key] = pwb_series_id
        rows.extend(result.rows)
    return TotalReturnPanel(
        version=f"{TOTAL_RETURN_SPLICE_VERSION}|{QUARANTINE_RULE_SET_VERSION}|{UNIVERSE_SELECTION_RULE_VERSION}",
        rows=tuple(rows),
        verdicts=verdicts,
        identity=identity,
        pwb_series={key: pwb_by_name[key] for key in identity},
    )


__all__ = [
    "IDENTITY_MAX_MEDIAN",
    "IDENTITY_MIN_MONTHS",
    "INTRADER_VENDOR",
    "PWB_CAPTURE_DATE",
    "PWB_VENDOR",
    "SWITCH_MONTH",
    "TOTAL_RETURN_SPLICE_VERSION",
    "LAST_PWB_MONTH",
    "LAST_SURVIVORSHIP_FREE_MONTH",
    "IdentityCheck",
    "MonthEnd",
    "MonthlyTotalReturn",
    "NameSplice",
    "PeriodReturn",
    "SpliceVerdict",
    "TotalReturnPanel",
    "identity_check",
    "load_month_ends",
    "load_total_return_panel",
    "monthly_returns",
    "splice_name",
]
