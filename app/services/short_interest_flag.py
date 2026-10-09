"""#3621 slice 5a: the FINRA short-interest (SI) flag for one formation.

Spec: ``docs/research/2026-10-09-3621-slice5-short-interest.md`` §"Source rules" (PR #3734). Pure; the run supplies
the calendar rows, each settlement's raw FINRA payload, the SPY sessions, and each admitted name's symbol,
``me.shares``, daily bars and split stamps.

Per formation M the settlement used is the latest calendar settlement whose publication is strictly before s(M);
before ``COVERAGE_START`` the formation is ``no_settlement`` and nothing is flagged. A covered formation whose
settlement is not month M's mid-month (first) calendar settlement refuses. The settlement's rows are used as stored,
whatever their ``revisionFlag``: FINRA's flag announces a revision of the PRIOR settlement (Regulatory Notice 21-19,
note 10), and the next file's ``previousShortPositionQuantity`` is never consumed. ``revision_check`` measures whether
a stored prior file holds the figure the next file announces as revised.

Per admitted name, in precedence order: ``ambiguous`` (the normalised symbol is empty, shared by two admitted names at
M, or carried by two rows of the file used); ``unmatched``; ``unverifiable`` (a session of FINRA's ADV window has no
usable bar); ``identity_fail`` (|ln(FINRA ADV / our ADV)| > ln ``ADV_TOLERANCE``, or exactly one of the two is zero);
``valid``: SIR = ``float(currentShortPositionQuantity x split_product(splits, settlement, s(M))) / float(shares)``.

Our ADV follows FINRA's glossary: over the SPY sessions after the previous calendar settlement through the row's
settlement, the sum of each session's volume x ``split_product(splits, session, settlement)``, divided by the session
count. A session's volume counts when its bar is ``usable`` with a non-null, finite, non-negative volume.

``flagged``: SIR >= q, q the ``ceil(0.9 N)``-th smallest of the N valid values at M over all admitted names (ties at
q all flag). A covered formation with N = 0, or q <= 0, refuses. Values above 1 are kept as reported.

``scripts/measure_3621_short_interest_premise.py`` is an independent implementation of the same rule; slice 5c
requires this module to reproduce its counts table and per-name file, after the declaration and access row.
"""

from __future__ import annotations

import csv
import io
import math
from collections import Counter
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Final

from app.services.factor_panel import SplitStamp, split_product
from app.services.factor_panel_prices import DailyBar
from app.services.finra_short_interest_ingest import normalise_symbol, parse_body_settlement_date

#: FINRA's file catalog: before June 2021 the files "do not reflect short interest data in exchange-listed securities".
COVERAGE_START: Final = date(2021, 6, 15)
#: The ADV-window predecessor of ``COVERAGE_START``; the calendar's fixed span starts here.
CALENDAR_FIRST: Final = date(2021, 5, 28)
CALENDAR_LAST_MONTH: Final = (2024, 8)
#: §"Source rules", threshold: the top decile, ``ceil(0.9 N)``-th smallest (1-based).
SI_DECILE: Final = 0.9
#: §"Source rules", identity: FINRA's ADV and ours agree within a factor of 1.2.
ADV_TOLERANCE: Final = 1.2
#: The ``filing_raw_documents`` kind FINRA's bimonthly payloads are stored under.
DOCUMENT_KIND: Final = "finra_short_interest_csv"
#: The header columns ``parse_file`` reads.
FINRA_COLUMNS: Final = frozenset(
    {
        "symbolCode",
        "issueName",
        "currentShortPositionQuantity",
        "previousShortPositionQuantity",
        "averageDailyVolumeQuantity",
        "revisionFlag",
        "settlementDate",
    }
)


class ShortInterestError(RuntimeError):
    pass


class SiState(StrEnum):
    NO_SETTLEMENT = "no_settlement"
    AMBIGUOUS = "ambiguous"
    UNMATCHED = "unmatched"
    UNVERIFIABLE = "unverifiable"
    IDENTITY_FAIL = "identity_fail"
    VALID = "valid"


Calendar = Sequence[tuple[date, date]]


def accession_for(settlement: date) -> str:
    """The raw-store accession FINRA's refresh job writes (``finra_short_interest_refresh``)."""
    return f"FINRA_SI_{settlement:%Y%m%d}"


def next_month(month: tuple[int, int]) -> tuple[int, int]:
    """(year, month) of the following month; December rolls to January of the next year."""
    return (month[0] + month[1] // 12, month[1] % 12 + 1)


def validate_calendar(rows: Sequence[tuple[date, date]]) -> list[tuple[date, date]]:
    """(settlement, publication) pairs, refused unless they start at ``CALENDAR_FIRST``, hold exactly two settlements
    in every month from the next through ``CALENDAR_LAST_MONTH`` and none after, with unique ascending settlements,
    strictly increasing publications, and each publication after its settlement."""
    days = [s for s, _p in rows]
    if not days or days[0] != CALENDAR_FIRST:
        raise ShortInterestError(f"calendar must start at {CALENDAR_FIRST}")
    if days != sorted(set(days)) or any(p1 <= p0 for (_s0, p0), (_s1, p1) in zip(rows, rows[1:], strict=False)):
        raise ShortInterestError(
            "calendar settlements not unique and ascending, or publications not strictly increasing"
        )
    if any(p <= s for s, p in rows):
        raise ShortInterestError("a publication date not after its settlement")
    per_month = Counter((s.year, s.month) for s in days[1:])
    month = next_month((CALENDAR_FIRST.year, CALENDAR_FIRST.month))
    required: list[tuple[int, int]] = []
    while month <= CALENDAR_LAST_MONTH:
        required.append(month)
        month = next_month(month)
    if outside := sorted(set(per_month) - set(required)):
        raise ShortInterestError(f"calendar settlements outside the fixed span: {outside}")
    for month in required:
        if per_month[month] != 2:
            raise ShortInterestError(f"calendar month {month} holds {per_month[month]} settlements, not 2")
    return list(rows)


def read_calendar_csv(text: str) -> list[tuple[date, date]]:
    """``docs/research/3621-finra-si-calendar.csv`` parsed and validated."""
    rows = [
        (date.fromisoformat(r["settlement_date"]), date.fromisoformat(r["publication_date"]))
        for r in csv.DictReader(io.StringIO(text))
    ]
    return validate_calendar(rows)


def settlement_for(calendar: Calendar, s_m: date) -> date | None:
    """The latest settlement whose publication is strictly before s(M); ``None`` unless it is on or after
    ``COVERAGE_START``. A covered settlement that is not M's mid-month settlement refuses."""
    published = [s for s, p in calendar if p < s_m]
    if not published or published[-1] < COVERAGE_START:
        return None
    used = published[-1]
    in_month = [s for s, _p in calendar if (s.year, s.month) == (s_m.year, s_m.month)]
    if not in_month or used != min(in_month):
        raise ShortInterestError(f"s(M) {s_m}: settlement {used} is not the month's mid-month settlement")
    return used


def adv_window(calendar: Calendar, settlement: date, sessions: Sequence[date]) -> list[date]:
    """SPY sessions after the previous calendar settlement, through ``settlement`` (FINRA's ADV denominator)."""
    days = [s for s, _p in calendar]
    if settlement not in days:
        raise ShortInterestError(f"{settlement}: not a calendar settlement")
    i = days.index(settlement)
    if i == 0:
        raise ShortInterestError(f"{settlement}: no previous calendar settlement for the ADV window")
    return [d for d in sessions if days[i - 1] < d <= settlement]


def usable_volume(bars: Iterable[DailyBar]) -> dict[date, float]:
    """Volume by session for bars that count toward our ADV: ``usable``, volume non-null, finite and >= 0."""
    out: dict[date, float] = {}
    for bar in bars:
        if bar.usable and bar.volume is not None:
            v = float(bar.volume)
            if math.isfinite(v) and v >= 0:
                out[bar.bar_date] = v
    return out


def our_adv(
    volume: Mapping[date, float], window: Sequence[date], splits: Sequence[SplitStamp], settlement: date
) -> float | None:
    """FINRA's ADV on our bars: the window's split-carried volume over its session count; ``None`` when a window
    session has no usable bar."""
    if not window or any(d not in volume for d in window):
        return None
    return sum(volume[d] * float(split_product(splits, d, settlement)) for d in window) / len(window)


def identity_ok(finra_adv: int, ours: float, tolerance: float = ADV_TOLERANCE) -> bool:
    if finra_adv == 0 or ours == 0:
        return finra_adv == 0 and ours == 0
    return abs(math.log(finra_adv / ours)) <= math.log(tolerance)


@dataclass(frozen=True)
class FinraRow:
    short: int
    previous: int | None
    adv: int
    revised: bool
    issue_name: str


@dataclass(frozen=True)
class FinraFile:
    rows: dict[str, FinraRow]
    #: Normalised symbols carried by two or more physical rows; ``rows`` keeps the last, which is ambiguous.
    twice: frozenset[str]
    physical_rows: int
    physical_revised: int
    zero_short: int
    blank_previous: int


def parse_file(payload: bytes, settlement: date) -> FinraFile:
    """One FINRA bimonthly file. Refuses a body ``settlementDate`` other than ``settlement``, a non-integer or
    negative short count, prior count or ADV (a blank ADV is FINRA's NULL, read as zero per its glossary)."""
    rows: dict[str, FinraRow] = {}
    twice: set[str] = set()
    physical = revised = zero = blanks = 0
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8")), delimiter="|")
    if absent := sorted(FINRA_COLUMNS - set(reader.fieldnames or ())):
        raise ShortInterestError(f"{settlement}: header lacks {absent}")
    for row in reader:
        if parse_body_settlement_date(row.get("settlementDate")) != settlement:
            raise ShortInterestError(f"{settlement}: a body row carries settlementDate {row.get('settlementDate')!r}")
        try:
            short, adv = int(row["currentShortPositionQuantity"]), int(row["averageDailyVolumeQuantity"] or 0)
            blank = not (row["previousShortPositionQuantity"] or "").strip()
            previous = None if blank else int(row["previousShortPositionQuantity"])
        except ValueError as exc:
            raise ShortInterestError(f"{settlement}: non-integer short count or ADV for {row['symbolCode']!r}") from exc
        if short < 0 or adv < 0 or (previous is not None and previous < 0):
            raise ShortInterestError(f"{settlement}: negative short count or ADV for {row['symbolCode']!r}")
        flagged = bool((row["revisionFlag"] or "").strip())
        physical += 1
        revised += flagged
        zero += short == 0
        blanks += previous is None
        key = normalise_symbol(row["symbolCode"] or "")
        if key in rows:
            twice.add(key)
        rows[key] = FinraRow(short, previous, adv, flagged, row["issueName"] or "")
    if physical == 0:
        raise ShortInterestError(f"{settlement}: payload has a header and no rows")
    return FinraFile(rows, frozenset(twice), physical, revised, zero, blanks)


@dataclass(frozen=True)
class SiName:
    """One admitted name at M: the panel row's vendor symbol and ``me.shares``, its usable volume and split stamps."""

    symbol: str
    shares: float
    volume: Mapping[date, float]
    splits: Sequence[SplitStamp]


@dataclass(frozen=True)
class Reading:
    state: SiState
    used: date | None = None
    sir: float | None = None
    #: A valid name with any split stamp in (settlement, s(M)], whatever the net product.
    split_carried: bool = False
    issue_name: str | None = None
    log_ratio: float | None = None
    adv_finra: int | None = None
    adv_ours: float | None = None


def read_name(key: str, file: FinraFile, used: date, window: Sequence[date], name: SiName, s_m: date) -> Reading:
    """One name whose normalised symbol ``key`` is non-empty and unique among the admitted names at M."""
    if key in file.twice:
        return Reading(SiState.AMBIGUOUS)
    if key not in file.rows:
        return Reading(SiState.UNMATCHED)
    hit = file.rows[key]
    ours = our_adv(name.volume, window, name.splits, used)
    if ours is None:
        return Reading(SiState.UNVERIFIABLE, used, issue_name=hit.issue_name)
    ratio = math.log(hit.adv / ours) if hit.adv > 0 and ours > 0 else None
    if not identity_ok(hit.adv, ours):
        return Reading(
            SiState.IDENTITY_FAIL, used, issue_name=hit.issue_name, log_ratio=ratio, adv_finra=hit.adv, adv_ours=ours
        )
    carried = any(used < stamp.day <= s_m for stamp in name.splits)
    sir = float(hit.short * split_product(name.splits, used, s_m)) / name.shares
    return Reading(SiState.VALID, used, sir, carried, hit.issue_name, ratio, hit.adv, ours)


def read_formation[K: Hashable](
    names: Mapping[K, SiName], file: FinraFile | None, used: date | None, window: Sequence[date], s_m: date
) -> dict[K, Reading]:
    """Every admitted name's reading at M. ``used`` is ``settlement_for``'s answer and ``file`` its parsed payload;
    both ``None`` for an uncovered formation."""
    for k, name in names.items():
        if not (math.isfinite(name.shares) and name.shares > 0):
            raise ShortInterestError(f"{s_m} {k!r}: admitted with shares {name.shares!r}")
    if used is None:
        return dict.fromkeys(names, Reading(SiState.NO_SETTLEMENT))
    if file is None:
        raise ShortInterestError(f"{s_m}: settlement {used} selected but no payload supplied")
    keys = {k: normalise_symbol(name.symbol) for k, name in names.items()}
    shared = Counter(keys.values())
    return {
        k: Reading(SiState.AMBIGUOUS)
        if not keys[k] or shared[keys[k]] > 1
        else read_name(keys[k], file, used, window, name, s_m)
        for k, name in names.items()
    }


def si_cutoff(values: Sequence[float]) -> float:
    """q: the ``ceil(0.9 N)``-th smallest valid SIR. Refuses N = 0 or q <= 0."""
    if not values:
        raise ShortInterestError("covered formation with no valid SIR")
    ordered = sorted(values)
    q = ordered[math.ceil(SI_DECILE * len(ordered)) - 1]
    if q <= 0:
        raise ShortInterestError(f"SIR decile cutoff {q} is not positive")
    return q


def flag_formation[K: Hashable](readings: Mapping[K, Reading], used: date | None) -> tuple[float | None, frozenset[K]]:
    """(q, flagged names). ``used`` is ``settlement_for``'s answer: an uncovered formation has no q and flags nothing;
    a covered one refuses with N = 0, including when it has no admitted names."""
    if used is None:
        return None, frozenset()
    q = si_cutoff([r.sir for r in readings.values() if r.sir is not None])
    return q, frozenset(k for k, r in readings.items() if r.sir is not None and r.sir >= q)


@dataclass
class RevisionTally:
    """Consecutive files compared on the PRIOR settlement's figure. If a stored prior file had been revised in place,
    a flagged row's ``previousShortPositionQuantity`` would equal that file's current figure."""

    #: (row flagged, previous == stored prior figure) -> rows.
    compared: Counter[tuple[bool, bool]] = field(default_factory=Counter)
    #: Rows with a prior count whose symbol is missing from, or duplicated in, either file; by flag.
    uncompared: Counter[bool] = field(default_factory=Counter)
    #: Rows with a blank prior count; by flag.
    blank_previous: Counter[bool] = field(default_factory=Counter)
    #: (settlement, symbol, flagged, previous, stored prior figure) where flagged == agree.
    residue: list[tuple[date, str, bool, int | None, int]] = field(default_factory=list)


def revision_check(files: Sequence[tuple[date, FinraFile]]) -> RevisionTally:
    """Tally each consecutive (prior, next) pair of ``files``, ascending by settlement."""
    out = RevisionTally()
    for (_prior_day, before), (day, now) in zip(files, files[1:], strict=False):
        for key, row in now.rows.items():
            if row.previous is None:
                out.blank_previous[row.revised] += 1
            elif key in before.rows and key not in now.twice and key not in before.twice:
                agree = row.previous == before.rows[key].short
                out.compared[(row.revised, agree)] += 1
                if row.revised == agree:
                    out.residue.append((day, key, row.revised, row.previous, before.rows[key].short))
            else:
                out.uncompared[row.revised] += 1
    return out
