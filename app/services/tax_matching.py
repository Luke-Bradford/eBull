"""Statutory share identification from the effective generation of tax evidence (#3616 slice 3d-2).

Spec: docs/specs/metrics/2026-10-14-3616-slice3d2-statutory-matching.md. Consideration classes:
docs/specs/metrics/2026-10-14-3616-slice3d1-consideration-basis.md ("Consideration classes").

- ``classify_lot`` is 3d-1's class table with slice 3d-3b's A8 (``consideration_classes_v2``).
- ``tax_security_of`` maps a component of instruments to its tax security (Design 2, ``tax_security_v1``).
- ``identify_security`` is the identification of one security's admitted lots (Designs 5-8): pure, exact, and
  refusing step 5 (s105(2)) rather than implementing it.
- ``compute_matching`` applies all of it to one generation's inputs, withholding what the evidence cannot establish
  (Design 3).
- ``run_tax_matching`` stores one sealed snapshot per (generation, policy ids, ``inputs_digest``) (Designs 9-10), and
  ``effective_tax_matching`` selects exactly the snapshot of the current inputs, with no fallback.
- Each security's tax character (slice 3d-3a, ``app/services/tax_character.py``) is decided before identification: a
  security that is not established as a share is withheld. ``refresh_cover_instances`` fetches the covers it reads.
- ``fund_status`` and ``identify_fund`` are slice 3d-3c-3's rules for a non-reporting fund (relevant securities):
  a fund whose status is ``non_reporting_assumed`` is ``non_reporting_fund`` and is identified with them, not pooled.

Nothing here is shown to the operator: the tax-year view is slice 3d-4's.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Context, Decimal, Inexact, Rounded, localcontext
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

import httpx
import psycopg
import psycopg.rows
from psycopg import IsolationLevel
from psycopg.types.json import Jsonb

from app.db.snapshot import snapshot_read
from app.services.def14a_recipients import instance_url
from app.services.tax_character import (
    ACCESSION,
    ETF_TYPE_ID,
    IDENTITY_UNESTABLISHED,
    ISSUER_BINDING_ASSUMED,
    OFFSHORE_FUND_EXCLUSION_ASSUMED,
    STOCKS_TYPE_ID,
    TAX_CHARACTER_POLICY_ID,
    FetchedInstance,
    SecurityEvidence,
    classify,
    document_entry,
    fetch_instance,
    is_cik,
    read_evidence,
    select_covers,
    structurally_sound,
)
from app.services.tax_fund_determination import (
    Decision,
    Determination,
    ReportingStatus,
    RunState,
    reporting_status,
)
from app.services.tax_ledger import MatchingRule

#: The frozen local policy (spec "Local policy"); changing any part of it is a new id.
TAX_MATCHING_POLICY_ID = "identification_v2"
#: 3d-1's class table with slice 3d-3b's A8 (``distribution_character_assumed``).
CONSIDERATION_CLASS_POLICY_ID = "consideration_classes_v2"
#: The one evidence policy ``consideration_classes_v2`` was reviewed against (slice 3d-3b Design 2). A generation
#: under any other policy is refused, not matched.
SUPPORTED_EVIDENCE_POLICY_ID = "broker_evidence_v3"
#: The version of the canonical inputs document (Design 9): v3 added each subject's A8 inputs, v4 each fund's Design 1
#: status (slice 3d-3c-3 Design 6).
INPUTS_VERSION = "matching_inputs_v4"
#: Session advisory lock held for a whole run (Design 10): int.from_bytes(b"taxmatch", "big").
TAX_MATCHING_LOCK_KEY = 8386116392322360168

_UK_TZ = ZoneInfo("Europe/London")
_ZERO = Decimal(0)

# Reasons (3d-1 "Consideration classes"; Designs 7-8).
SPREAD_INCLUSION_ASSUMED = "spread_inclusion_assumed"
EXECUTION_ASSUMED = "execution_assumed"
LOOKUP_EVIDENCE_UNOBSERVED = "lookup_evidence_unobserved"
DISPOSAL_FORMULA_ASSUMED = "disposal_formula_assumed"
SYNC_CHARGES_UNOBSERVED = "sync_charges_unobserved"
#: A8 (slice 3d-3b): every distribution in a position's ``totalFees`` is a separately paid cash income distribution.
DISTRIBUTION_CHARACTER_ASSUMED = "distribution_character_assumed"
WINDOW_OPEN = "window_open"
HISTORY_COVERAGE_UNESTABLISHED = "history_coverage_unestablished"
#: Carried by every disposal and pool, all of shares (slice 3d-3a Design 3).
SHARE_REASONS = frozenset({OFFSHORE_FUND_EXCLUSION_ASSUMED, ISSUER_BINDING_ASSUMED})

# The fields whose failure makes a lot ``unavailable`` (3d-1; Design 3 ``lot_unavailable``).
EXECUTION_EVIDENCE_MISMATCH = "execution_evidence_mismatch"
TOTAL_FEES_UNOBSERVED_NULL = "total_fees_unobserved_null"
TOTAL_FEES_NONZERO_INVALID = "total_fees_nonzero_invalid"
DIRECTION_INVALID = "direction_invalid"

LotClass = Literal["final_eligible", "provisional", "unavailable"]
WithholdReason = Literal[
    "unresolved_subject",
    "identity_unestablished",
    "lot_unavailable",
    "subsequent_acquisition_rule",
    "unplaceable",
    "character_unestablished",
    # A fund's Design 2 refusals (slice 3d-3c-3).
    "time_order_unestablished",
    "close_fragments_unestablished",
    "pre_2009_rights",
    "identification_incomplete",
]
#: ``tax_character_v2``: 3d-3a's characters and slice 3d-3c-3's ``non_reporting_fund``.
MatchingCharacter = Literal["share", "unestablished", "non_reporting_fund"]
Direction = Literal["acquisition", "disposal"]

#: Column capacities (Design 6): units NUMERIC(24,8), sterling NUMERIC(44,14).
UNITS_PRECISION, UNITS_SCALE = 24, 8
GBP_PRECISION, GBP_SCALE = 44, 14


# --- inputs -------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class InstrumentRow:
    instrument_id: int
    symbol: str
    canonical_instrument_id: int | None


@dataclass(frozen=True)
class SubjectRow:
    subject: str
    outcome: str
    instrument_id: int | None
    execution_evidence: str | None
    total_fees_unobserved_position_ids: tuple[int, ...] | None
    #: Slice 3d-3b: positions with a non-zero ``totalFees`` (A8's predicate), and the subject's positions (the keys
    #: of its ``held_positions``) the array must be drawn from. NULL on runs before ``broker_evidence_v3``.
    total_fees_nonzero_position_ids: tuple[int | None, ...] | None
    #: None when a ``held_positions`` key is not a position id, which makes the lot unavailable.
    position_ids: frozenset[int] | None


@dataclass(frozen=True)
class LotRow:
    lot_key: str
    subject: str
    instrument_id: int
    direction: str
    uk_date: date
    tax_year: str
    units: Decimal
    #: NULL while the lot's FX is pending (3c; 3d-1 precedence 1).
    amount_gbp: Decimal | None
    execution_evidence: str | None
    fingerprint: str
    #: Enters the digest through ``fingerprint`` (3c Design 4.10); orders a fund's lots (3d-3c-3 Design 2).
    executed_at: datetime


@dataclass(frozen=True)
class MatchingInputs:
    """One generation's inputs as read in one snapshot (Design 1)."""

    evidence_run_id: int
    evidence_policy_id: str
    #: ``started_at`` of the generation's ``history_fetch_ok_id`` fetch; None with no ``ok`` fetch.
    history_fetch_started_at: datetime | None
    subjects: tuple[SubjectRow, ...]
    lots: tuple[LotRow, ...]
    #: Every ``instruments`` row, by id.
    instruments: Mapping[int, InstrumentRow]
    #: The clock reading taken right after the snapshot started (3d-3a Design 5).
    as_of: datetime
    #: Each security's character inputs, keyed as ``security_members`` keys them.
    characters: Mapping[int, SecurityEvidence]
    #: What 3d-3c-3's Design 1 reads for each security whose members are all ETFs, keyed likewise. A security absent
    #: here is read as having no determination run.
    funds: Mapping[int, FundInputs] = field(default_factory=dict)


# --- consideration classes (3d-1) ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class LotClassification:
    lot_class: LotClass
    #: Sorted. For ``unavailable``, the failing fields; otherwise the consideration reasons.
    reasons: tuple[str, ...]


def _nonzero_ids_valid(subject: SubjectRow) -> bool:
    """Slice 3d-3b Design 2: a sorted, duplicate-free array of positive ids, each one of the subject's positions."""
    ids = subject.total_fees_nonzero_position_ids
    if ids is None or subject.position_ids is None:
        return False
    if any(not isinstance(p, int) or isinstance(p, bool) or p <= 0 for p in ids):
        return False
    positive = cast(tuple[int, ...], ids)
    return list(positive) == sorted(set(positive)) and set(positive) <= subject.position_ids


def _position_keys(keys: Iterable[str]) -> frozenset[int] | None:
    """The subject's positions from its ``held_positions`` keys; None when one is not a decimal id."""
    keys = list(keys)
    if any(not (isinstance(k, str) and k.isascii() and k.isdigit()) for k in keys):
        return None
    return frozenset(int(k) for k in keys)


def classify_lot(lot: LotRow, subject: SubjectRow) -> LotClassification:
    """3d-1's table, from the lot's own row and its subject's row in the same generation."""
    failing: list[str] = []
    if lot.direction not in ("acquisition", "disposal"):
        failing.append(DIRECTION_INVALID)
    if lot.execution_evidence not in ("lookup", "sync_only") or subject.execution_evidence != lot.execution_evidence:
        failing.append(EXECUTION_EVIDENCE_MISMATCH)
    if subject.total_fees_unobserved_position_ids is None:
        failing.append(TOTAL_FEES_UNOBSERVED_NULL)
    if not _nonzero_ids_valid(subject):
        failing.append(TOTAL_FEES_NONZERO_INVALID)
    if failing:
        return LotClassification("unavailable", tuple(sorted(failing)))
    reasons: set[str] = set()
    if lot.direction == "acquisition":
        reasons.add(SPREAD_INCLUSION_ASSUMED)
        if lot.execution_evidence == "sync_only":
            reasons |= {EXECUTION_ASSUMED, LOOKUP_EVIDENCE_UNOBSERVED}
        if subject.total_fees_nonzero_position_ids:
            reasons.add(DISTRIBUTION_CHARACTER_ASSUMED)
    else:
        reasons.add(DISPOSAL_FORMULA_ASSUMED)
    if subject.total_fees_unobserved_position_ids:
        reasons.add(SYNC_CHARGES_UNOBSERVED)
    return LotClassification("provisional" if reasons else "final_eligible", tuple(sorted(reasons)))


# --- tax security (Design 2) --------------------------------------------------------------------------------------

_RTH = ".RTH"


def components(instruments: Mapping[int, InstrumentRow], named: Iterable[int]) -> dict[int, frozenset[int]]:
    """The component of each named instrument that has a row: the connected set under the canonical pointer and the
    ``X`` / ``X.RTH`` symbol pair, each edge followed both ways, symbols compared upper-cased."""
    by_symbol: dict[str, list[int]] = defaultdict(list)
    pointed_from: dict[int, list[int]] = defaultdict(list)
    for row in instruments.values():
        by_symbol[row.symbol.upper()].append(row.instrument_id)
        if row.canonical_instrument_id is not None:
            pointed_from[row.canonical_instrument_id].append(row.instrument_id)

    def neighbours(iid: int) -> set[int]:
        row = instruments[iid]
        symbol = row.symbol.upper()
        out = set(pointed_from.get(iid, ()))
        if row.canonical_instrument_id is not None and row.canonical_instrument_id in instruments:
            out.add(row.canonical_instrument_id)
        out.update(by_symbol.get(symbol + _RTH, ()))
        if symbol.endswith(_RTH):
            out.update(by_symbol.get(symbol[: -len(_RTH)], ()))
        out.discard(iid)
        return out

    found: dict[int, frozenset[int]] = {}
    for start in named:
        if start not in instruments or start in found:
            continue
        seen, todo = {start}, [start]
        while todo:
            for v in neighbours(todo.pop()):
                if v not in seen:
                    seen.add(v)
                    todo.append(v)
        member_set = frozenset(seen)
        for member in member_set:
            found[member] = member_set
    return {iid: found[iid] for iid in named if iid in found}


def tax_security_of(component: frozenset[int], instruments: Mapping[int, InstrumentRow]) -> int | None:
    """The component's base when it is established (exactly one member with no pointer, every other member pointing
    to it with the base's symbol plus ``.RTH``), else None: *unestablished*."""
    bases = [m for m in component if instruments[m].canonical_instrument_id is None]
    if len(bases) != 1:
        return None
    base = bases[0]
    variant_symbol = instruments[base].symbol.upper() + _RTH
    for member in component:
        row = instruments[member]
        if member != base and (row.canonical_instrument_id != base or row.symbol.upper() != variant_symbol):
            return None
    return base


# --- exact arithmetic (Design 6) ----------------------------------------------------------------------------------


def _scaled(value: Decimal, places: int) -> int:
    """``value × 10^places`` as an exact integer; raises if ``value`` has more than ``places`` decimal places."""
    numerator, denominator = value.as_integer_ratio()
    quotient, remainder = divmod(numerator * 10**places, denominator)
    if remainder:
        raise ValueError(f"{value} has more than {places} decimal places")
    return quotient


def share_quanta(u: int, c: int, v: int) -> int:
    """``u × c / v`` rounded half-even to an integer, on integers only (Design 6): ``q, r = divmod(u × c, v)``, and
    ``q`` goes up when ``2r > v``, or when ``2r = v`` and ``q`` is odd. A decimal division at a fixed precision can
    round a value just below a midpoint onto it and then round again."""
    if u < 0 or c < 0 or v <= 0:
        raise ValueError(f"share_quanta needs u, c >= 0 and v > 0: {u}, {c}, {v}")
    q, r = divmod(u * c, v)
    if 2 * r > v or (2 * r == v and q % 2 == 1):
        q += 1
    return q


def apportion(units: Decimal, cost_left: Decimal, units_left: Decimal) -> Decimal:
    """The cost of a partial withdrawal, ``units × cost_left / units_left`` half-even at ``1E-14`` (CG51575)."""
    quanta = share_quanta(_scaled(units, UNITS_SCALE), _scaled(cost_left, GBP_SCALE), _scaled(units_left, UNITS_SCALE))
    return Decimal(f"{quanta}E-{GBP_SCALE}")


_CAPACITY_CONTEXT = Context(prec=100)


def fits(value: Decimal, precision: int, scale: int) -> bool:
    """Whether ``value`` is stored exactly by ``NUMERIC(precision, scale)``."""
    quantum = Decimal(1).scaleb(-scale, context=_CAPACITY_CONTEXT)
    exact = value.quantize(quantum, context=_CAPACITY_CONTEXT) == value
    return exact and abs(value) < Decimal(1).scaleb(precision - scale, context=_CAPACITY_CONTEXT)


def window_elapses_at(disposal_uk_date: date) -> datetime:
    """London midnight beginning ``d + 31``: the 30 days after the disposal have passed (Design 7)."""
    return datetime.combine(disposal_uk_date + timedelta(days=31), time(0), tzinfo=_UK_TZ)


# --- identification (Designs 5-8) ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchLot:
    lot_key: str
    direction: Direction
    uk_date: date
    tax_year: str
    units: Decimal
    amount_gbp: Decimal | None
    reasons: frozenset[str]


@dataclass(frozen=True)
class Identification:
    rule: MatchingRule
    disposal_uk_date: date
    #: None for the pool.
    acquisition_uk_date: date | None
    acquisition_lot_keys: tuple[str, ...]
    units: Decimal
    #: None iff ``missing_lot_keys`` is non-empty.
    cost_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class StatutoryDisposal:
    uk_date: date
    tax_year: str
    lot_keys: tuple[str, ...]
    units: Decimal
    proceeds_gbp: Decimal | None
    cost_gbp: Decimal | None
    gain_gbp: Decimal | None
    status: Literal["computed", "amount_missing"]
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]
    identifications: tuple[Identification, ...]


@dataclass(frozen=True)
class Pool:
    units: Decimal
    #: None iff ``missing_lot_keys`` is non-empty.
    cost_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]
    window_open_since: date | None


@dataclass(frozen=True)
class Identified:
    disposals: tuple[StatutoryDisposal, ...]
    pool: Pool


@dataclass(frozen=True)
class StepFiveReached:
    """A disposal not fully identified by steps 1-4: s105(2), which this policy withholds (Design 3)."""

    disposal_uk_date: date
    units: Decimal


@dataclass
class _Day:
    lot_keys: list[str]
    units: Decimal
    #: None when any lot of the day has a missing amount.
    amount: Decimal | None
    missing: set[str]
    reasons: set[str]
    tax_years: set[str]


def _days(lots: Iterable[MatchLot]) -> dict[date, _Day]:
    days: dict[date, _Day] = {}
    for lot in sorted(lots, key=lambda x: x.lot_key):
        day = days.setdefault(lot.uk_date, _Day([], _ZERO, _ZERO, set(), set(), set()))
        day.lot_keys.append(lot.lot_key)
        day.units += lot.units
        if lot.amount_gbp is None:
            day.amount = None
            day.missing.add(lot.lot_key)
        elif day.amount is not None:
            day.amount += lot.amount_gbp
        day.reasons |= lot.reasons
        day.tax_years.add(lot.tax_year)
    return days


def _sorted(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


def identify_security(
    lots: Sequence[MatchLot], history_fetch_started_at: datetime | None
) -> Identified | StepFiveReached:
    """Identify one security's admitted lots under TCGA 1992 ss104-106A, steps 1-4 (spec Source rules, Design 5).

    1. Each London date is one acquisition and one disposal (s105(1)(a)), summing its lots.
    2. Same day, on every day first (s105(1)(b)).
    3. The following 30 days, earlier disposals first, each against the earliest remaining acquisition days in
       ``(d, d+30]`` (s106A(5)).
    4. The s104 pool, walked in date order, each acquisition day's remainder entering on its own day.

    A disposal left with units after step 4 returns ``StepFiveReached``. Every sum is exact (precision 60, ``Inexact``
    and ``Rounded`` trapped); a partial withdrawal of cost is ``apportion``; a withdrawal of all remaining units takes
    the exact remaining cost. The Design 5 invariants are checked before returning.
    """
    with localcontext() as ctx:
        ctx.prec = 60
        ctx.traps[Inexact] = True
        ctx.traps[Rounded] = True
        return _identify(lots, history_fetch_started_at)


def _identify(lots: Sequence[MatchLot], history_fetch_started_at: datetime | None) -> Identified | StepFiveReached:
    acq = _days(x for x in lots if x.direction == "acquisition")
    disp = _days(x for x in lots if x.direction == "disposal")
    acq_units_left = {d: day.units for d, day in acq.items()}
    acq_cost_left: dict[date, Decimal | None] = {d: day.amount for d, day in acq.items()}
    acq_withdrawn_units = dict.fromkeys(acq, _ZERO)
    acq_withdrawn_cost: dict[date, Decimal | None] = dict.fromkeys(acq, _ZERO)
    disp_units_left = {d: day.units for d, day in disp.items()}
    identified: dict[date, list[Identification]] = {d: [] for d in disp}

    def take(rule: MatchingRule, disposal_day: date, acq_day: date, units: Decimal) -> None:
        day = acq[acq_day]
        cost_left = acq_cost_left[acq_day]
        if cost_left is None:
            cost = None
        elif units == acq_units_left[acq_day]:
            cost = cost_left
        else:
            cost = apportion(units, cost_left, acq_units_left[acq_day])
        acq_units_left[acq_day] -= units
        acq_cost_left[acq_day] = None if cost_left is None or cost is None else cost_left - cost
        acq_withdrawn_units[acq_day] += units
        withdrawn = acq_withdrawn_cost[acq_day]
        acq_withdrawn_cost[acq_day] = None if withdrawn is None or cost is None else withdrawn + cost
        disp_units_left[disposal_day] -= units
        identified[disposal_day].append(
            Identification(
                rule=rule,
                disposal_uk_date=disposal_day,
                acquisition_uk_date=acq_day,
                acquisition_lot_keys=tuple(day.lot_keys),
                units=units,
                cost_gbp=cost,
                missing_lot_keys=_sorted(day.missing) if cost is None else (),
                reasons=_sorted(day.reasons | {HISTORY_COVERAGE_UNESTABLISHED}),
            )
        )

    # Step 2: same day, on every day first.
    for d in sorted(disp.keys() & acq.keys()):
        units = min(disp_units_left[d], acq_units_left[d])
        if units > 0:
            take("same_day", d, d, units)

    # Step 3: the following 30 days, earlier disposals first, earliest acquisitions first.
    acq_days = sorted(acq)
    for d in sorted(disp):
        window_end = d + timedelta(days=30)
        for a in acq_days:
            if disp_units_left[d] <= 0:
                break
            if a <= d or a > window_end or acq_units_left[a] <= 0:
                continue
            take("bed_and_breakfast", d, a, min(disp_units_left[d], acq_units_left[a]))

    # Step 4: the s104 pool, in date order. A day with an acquisition remainder had its disposal fully identified
    # same-day, so the order of the two within one day cannot matter.
    pool_units: Decimal = _ZERO
    pool_cost: Decimal | None = _ZERO
    pool_missing: set[str] = set()
    pool_reasons: set[str] = set()
    entered_units = dict.fromkeys(acq, _ZERO)
    entered_cost: dict[date, Decimal | None] = dict.fromkeys(acq, _ZERO)
    for d in sorted(acq.keys() | disp.keys()):
        if d in acq and acq_units_left[d] > 0:
            remainder_cost = acq_cost_left[d]
            entered_units[d] = acq_units_left[d]
            entered_cost[d] = remainder_cost
            pool_units += acq_units_left[d]
            pool_cost = None if pool_cost is None or remainder_cost is None else pool_cost + remainder_cost
            if remainder_cost is None:
                pool_missing |= acq[d].missing
            pool_reasons |= acq[d].reasons
            acq_units_left[d] = _ZERO
        if d not in disp or disp_units_left[d] <= 0:
            continue
        units = min(disp_units_left[d], pool_units)
        if units > 0:
            if pool_cost is None:
                cost = None
            elif units == pool_units:
                cost = pool_cost
            else:
                cost = apportion(units, pool_cost, pool_units)
            identified[d].append(
                Identification(
                    rule="s104_pool",
                    disposal_uk_date=d,
                    acquisition_uk_date=None,
                    acquisition_lot_keys=(),
                    units=units,
                    cost_gbp=cost,
                    missing_lot_keys=_sorted(pool_missing) if cost is None else (),
                    reasons=_sorted(pool_reasons | {HISTORY_COVERAGE_UNESTABLISHED}),
                )
            )
            pool_units -= units
            pool_cost = None if pool_cost is None or cost is None else pool_cost - cost
            disp_units_left[d] -= units
            if pool_units < 0 or (pool_cost is not None and pool_cost < 0):
                raise RuntimeError(f"s104 pool negative on {d}: units {pool_units}, cost {pool_cost}")
            if pool_units == _ZERO:
                # Remainder reasons and an undefined cost leave only when the units reach zero (3d-1 "Propagation").
                pool_cost, pool_missing, pool_reasons = _ZERO, set(), set()
        if disp_units_left[d] > 0:
            return StepFiveReached(d, disp_units_left[d])

    # The Design 5 invariants.
    for d, day in acq.items():
        if acq_withdrawn_units[d] + entered_units[d] != day.units:
            raise RuntimeError(f"acquisition day {d}: units not conserved")
        withdrawn, entered = acq_withdrawn_cost[d], entered_cost[d]
        if day.amount is not None and (withdrawn is None or entered is None or withdrawn + entered != day.amount):
            raise RuntimeError(f"acquisition day {d}: cost not conserved")

    disposals: list[StatutoryDisposal] = []
    for d in sorted(disp):
        day = disp[d]
        idents = identified[d]
        if sum((i.units for i in idents), _ZERO) != day.units:
            raise RuntimeError(f"disposal day {d}: identified units differ from the disposal's")
        if len(day.tax_years) != 1:
            raise RuntimeError(f"disposal day {d}: lots disagree on the tax year {sorted(day.tax_years)}")
        elapsed = history_fetch_started_at is not None and history_fetch_started_at >= window_elapses_at(d)
        window_open = any(i.rule != "same_day" for i in idents) and not elapsed
        if window_open:
            idents = [
                i if i.rule == "same_day" else _with_reason(i, WINDOW_OPEN)  # a later acquisition could displace it
                for i in idents
            ]
        costs = [i.cost_gbp for i in idents]
        cost = None if any(c is None for c in costs) else sum((c for c in costs if c is not None), _ZERO)
        proceeds = day.amount
        gain = None if proceeds is None or cost is None else proceeds - cost
        missing = set(day.missing)
        for i in idents:
            missing |= set(i.missing_lot_keys)
        reasons = set(day.reasons) | {HISTORY_COVERAGE_UNESTABLISHED} | SHARE_REASONS
        for i in idents:
            reasons |= set(i.reasons)
        if window_open:
            reasons.add(WINDOW_OPEN)
        disposals.append(
            StatutoryDisposal(
                uk_date=d,
                tax_year=next(iter(day.tax_years)),
                lot_keys=tuple(day.lot_keys),
                units=day.units,
                proceeds_gbp=proceeds,
                cost_gbp=cost,
                gain_gbp=gain,
                status="computed" if gain is not None else "amount_missing",
                missing_lot_keys=_sorted(missing),
                reasons=_sorted(reasons),
                identifications=tuple(idents),
            )
        )

    open_dates = [x.uk_date for x in disposals if WINDOW_OPEN in x.reasons]
    window_open_since = min(open_dates) if open_dates else None
    final_reasons = pool_reasons | {HISTORY_COVERAGE_UNESTABLISHED} | SHARE_REASONS
    if window_open_since is not None:
        final_reasons.add(WINDOW_OPEN)
    return Identified(
        disposals=tuple(disposals),
        pool=Pool(
            units=pool_units,
            cost_gbp=pool_cost,
            missing_lot_keys=_sorted(pool_missing) if pool_cost is None else (),
            reasons=_sorted(final_reasons),
            window_open_since=window_open_since,
        ),
    )


def _with_reason(ident: Identification, reason: str) -> Identification:
    return Identification(
        rule=ident.rule,
        disposal_uk_date=ident.disposal_uk_date,
        acquisition_uk_date=ident.acquisition_uk_date,
        acquisition_lot_keys=ident.acquisition_lot_keys,
        units=ident.units,
        cost_gbp=ident.cost_gbp,
        missing_lot_keys=ident.missing_lot_keys,
        reasons=_sorted((*ident.reasons, reason)),
    )


# --- non-reporting funds (slice 3d-3c-3) --------------------------------------------------------------------------
#
# Spec: docs/specs/metrics/2026-10-15-3616-slice3d3c3-fund-disposals.md. Pure: ``fund_status`` is Design 1's status
# rule, ``identify_fund`` Designs 2-4 for one security whose status is ``non_reporting_assumed``, and
# ``fund_digest_entry`` its ``matching_inputs_v4`` entry (Design 6). ``compute_matching`` applies them; ``sql/494``
# stores the result.

#: Carried by every row of a non-reporting fund (Design 4), with ``HISTORY_COVERAGE_UNESTABLISHED``.
NON_REPORTING_STATUS_ASSUMED = "non_reporting_status_assumed"
#: A disposal with an acquisition later on its own UK date, where F2's reading of the period could matter.
THIRTY_DAY_PERIOD_ASSUMED = "thirty_day_period_assumed"
#: Determinations whose fund-level exceptions (regs 28, 31A-31C) the spec reviewed (F7); no other fund is computed.
EXCEPTIONS_REVIEWED = frozenset({"fund_determination_v1:0000884394:1"})
#: Rights acquired before this date are protected (regs 30 and 43): such a security is refused.
OFFSHORE_FUNDS_COMMENCEMENT = date(2009, 12, 1)
#: The detail of 3d-3a's ETF line, which ``tax_character_v2`` replaces (Design 1).
ETF_LINE = "exchange_traded_fund"

FundStatusCode = ReportingStatus | Literal["exceptions_unreviewed"]
FundRefusalReason = Literal[
    "time_order_unestablished", "close_fragments_unestablished", "pre_2009_rights", "identification_incomplete"
]
GainCharacter = Literal["capital_loss", "nil", "offshore_income_gain", "oig_or_capital_unresolved"]
_STATUS_REASON_CODES = frozenset({"list_match", "search_unavailable"})


@dataclass(frozen=True)
class FundInputs:
    """What Design 1 reads for one type-6 security, in the matching run's snapshot."""

    #: The newest determination run visible in the snapshot; None with no run.
    run: RunState | None
    #: That run's observation of the security; None when it has none.
    observed: Decision | None
    #: The observation's determination body and its file hash; None without a determination.
    determination: Determination | None
    file_sha256: str | None
    #: The fund observation's reporting-fund list search.
    search_outcome: str | None
    #: The run's id, stored as provenance on a ``non_reporting_fund`` character row.
    run_id: int | None = None


@dataclass(frozen=True)
class FundStatus:
    """Design 1's result. A failing status carries only its code, structured reason and (``outside_interval`` only)
    date; ``non_reporting_assumed`` carries the determination, its transparency and the assumptions it rests on."""

    status: FundStatusCode
    reason: str | None = None
    status_date: date | None = None
    determination_id: str | None = None
    file_sha256: str | None = None
    transparent_fund: str | None = None
    transparent_basis: str | None = None
    assumptions: tuple[str, ...] | None = None


def fund_status(fund: FundInputs, lot_dates: Iterable[date], as_of: datetime) -> FundStatus:
    """Design 1: ``reporting_status`` over the newest run, evaluated at ``as_of``.

    1. ``no_run``, ``run_failed`` and ``not_observed`` do not depend on the date and are returned without one.
    2. Otherwise it is evaluated at each distinct lot date, ascending, then at the holding date (the run's cutoff as a
       UTC date); the first other result is the status, dated only when it is ``outside_interval``.
    3. A determination not in ``EXCEPTIONS_REVIEWED`` is ``exceptions_unreviewed``.
    4. Otherwise ``non_reporting_assumed``, with the sorted union of every result's assumptions.
    """
    first = reporting_status(fund.run, fund.observed, fund.determination, fund.search_outcome, date.min, as_of)
    if first.status in ("no_run", "run_failed", "not_observed"):
        return FundStatus(first.status)
    if fund.run is None or fund.run.cutoff is None or fund.observed is None:
        raise RuntimeError(f"reporting_status returned {first.status} without a succeeded run and an observation")
    dates = [*sorted(set(lot_dates)), fund.run.cutoff.astimezone(UTC).date()]
    assumptions: set[str] = set()
    for d in dates:
        result = reporting_status(fund.run, fund.observed, fund.determination, fund.search_outcome, d, as_of)
        if result.status != "non_reporting_assumed":
            return FundStatus(
                result.status,
                result.reason if result.reason in _STATUS_REASON_CODES else None,
                d if result.status == "outside_interval" else None,
            )
        assumptions |= set(result.assumptions)
    determination_id = fund.observed.determination_id
    if determination_id not in EXCEPTIONS_REVIEWED:
        return FundStatus("exceptions_unreviewed")
    if fund.determination is None or fund.file_sha256 is None:
        raise RuntimeError(f"determination {determination_id} without its body or file hash")
    (finding,) = [f for f in fund.determination["findings"] if f["question"] == "transparent_fund"]
    return FundStatus(
        "non_reporting_assumed",
        determination_id=determination_id,
        file_sha256=fund.file_sha256,
        transparent_fund=finding["answer"],
        transparent_basis=finding["basis"],
        assumptions=tuple(sorted(assumptions)),
    )


def fund_digest_entry(status: FundStatus) -> list[Any]:
    """``[status_code, status_reason_code, status_date, determination_id, file_sha256, transparent_fund,
    transparent_basis, assumptions]`` (Design 6): structured codes only, so an equivalent determination run with the
    same results gives the same entry."""
    return [
        status.status,
        status.reason,
        None if status.status_date is None else status.status_date.isoformat(),
        status.determination_id,
        status.file_sha256,
        status.transparent_fund,
        status.transparent_basis,
        None if status.assumptions is None else list(status.assumptions),
    ]


def gain_character(
    computed_gain: Decimal, transparent_fund: str
) -> tuple[GainCharacter, Decimal, Decimal | None, Decimal | None]:
    """Design 3: ``(gain_character, basic_gain, offshore_income_gain, chargeable_gain)`` for one disposal. A loss is
    capital whatever the fund's transparency (reg 42 floors the basic gain at nil); only an opaque fund's gain is an
    offshore income gain (reg 38), whose chargeable gain is nil (reg 45); otherwise the split is unresolved."""
    if computed_gain < 0:
        return "capital_loss", _ZERO, _ZERO, computed_gain
    if computed_gain == 0:
        return "nil", _ZERO, _ZERO, _ZERO
    if transparent_fund == "opaque":
        return "offshore_income_gain", computed_gain, computed_gain, _ZERO
    return "oig_or_capital_unresolved", computed_gain, None, None


@dataclass(frozen=True)
class FundLot:
    lot_key: str
    direction: Direction
    #: The order that opened the position (3c's subject): two closes of one order may be fragments of one bargain.
    subject: str
    executed_at: datetime
    uk_date: date
    tax_year: str
    units: Decimal
    amount_gbp: Decimal | None
    reasons: frozenset[str]


@dataclass(frozen=True)
class FundIdentification:
    rule: Literal["thirty_day", "lifo"]
    acquisition_lot_key: str
    acquisition_executed_at: datetime
    acquisition_uk_date: date
    units: Decimal
    #: None iff ``missing_lot_keys`` (then the acquisition's key) is non-empty.
    cost_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class FundDisposal:
    #: Identification order, from 1.
    seq: int
    lot_key: str
    executed_at: datetime
    uk_date: date
    tax_year: str
    units: Decimal
    proceeds_gbp: Decimal | None
    cost_gbp: Decimal | None
    computed_gain_gbp: Decimal | None
    basic_gain_gbp: Decimal | None
    offshore_income_gain_gbp: Decimal | None
    chargeable_gain_gbp: Decimal | None
    status: Literal["computed", "amount_missing"]
    gain_character: GainCharacter | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]
    identifications: tuple[FundIdentification, ...]


@dataclass(frozen=True)
class FundBalance:
    lot_key: str
    units_left: Decimal
    #: None iff ``missing_lot_keys`` (then the acquisition's key) is non-empty.
    cost_left_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class FundIdentified:
    disposals: tuple[FundDisposal, ...]
    balances: tuple[FundBalance, ...]
    #: The holding: the balances' totals (``pool`` holds them, as the security row's five pool columns).
    pool: Pool


@dataclass(frozen=True)
class FundRefused:
    """A Design 2 refusal: the security is withheld with this reason."""

    reason: FundRefusalReason
    detail: str


def identify_fund(
    lots: Sequence[FundLot], history_fetch_started_at: datetime | None, transparent_fund: str
) -> FundIdentified | FundRefused:
    """Identify one non-reporting fund's admitted lots as relevant securities (TCGA 1992 s106A(4)-(6); Design 2), then
    compute each disposal's gain and character (Design 3) and every row's reasons (Design 4).

    No same-day rule and no s104 pool (s104(3)): each disposal lot is its own disposal, taken in ``(uk_date,
    executed_at, lot_key)`` order, first against acquisitions on UK dates ``d+1 .. d+30``, earliest first (F2), then
    against acquisitions before its instant, latest first. Arithmetic is exact as in ``identify_security``; the
    invariants and column capacities are checked before returning, raising on a failure.
    """
    with localcontext() as ctx:
        ctx.prec = 60
        ctx.traps[Inexact] = True
        ctx.traps[Rounded] = True
        return _identify_fund(lots, history_fetch_started_at, transparent_fund)


def _identify_fund(
    lots: Sequence[FundLot], history_fetch_started_at: datetime | None, transparent_fund: str
) -> FundIdentified | FundRefused:
    instants = sorted(x.executed_at for x in lots)
    shared = next((a for a, b in zip(instants, instants[1:], strict=False) if a == b), None)
    if shared is not None:
        return FundRefused("time_order_unestablished", f"two lots at {shared.isoformat()}")
    closes = sorted(x.subject for x in lots if x.direction == "disposal")
    fragment = next((a for a, b in zip(closes, closes[1:], strict=False) if a == b), None)
    if fragment is not None:
        return FundRefused("close_fragments_unestablished", f"two disposal lots of {fragment}")
    acqs = sorted((x for x in lots if x.direction == "acquisition"), key=lambda x: (x.executed_at, x.lot_key))
    early = next((a for a in acqs if a.uk_date < OFFSHORE_FUNDS_COMMENCEMENT), None)
    if early is not None:
        return FundRefused("pre_2009_rights", f"{early.lot_key} acquired {early.uk_date.isoformat()}")
    disps = sorted((x for x in lots if x.direction == "disposal"), key=lambda x: (x.uk_date, x.executed_at, x.lot_key))

    units_left = {a.lot_key: a.units for a in acqs}
    cost_left: dict[str, Decimal | None] = {a.lot_key: a.amount_gbp for a in acqs}
    taken: dict[str, list[FundIdentification]] = {d.lot_key: [] for d in disps}
    for d in disps:
        need = d.units
        last_day = d.uk_date + timedelta(days=30)
        thirty_day = [a for a in acqs if d.uk_date < a.uk_date <= last_day]
        lifo = [a for a in reversed(acqs) if a.executed_at < d.executed_at]
        for rule, pool in (("thirty_day", thirty_day), ("lifo", lifo)):
            for a in pool:
                if need == 0:
                    break
                if units_left[a.lot_key] == 0:
                    continue
                units = min(need, units_left[a.lot_key])
                left = cost_left[a.lot_key]
                if left is None:
                    cost = None
                elif units == units_left[a.lot_key]:
                    cost = left
                else:
                    cost = apportion(units, left, units_left[a.lot_key])
                units_left[a.lot_key] -= units
                cost_left[a.lot_key] = None if left is None or cost is None else left - cost
                need -= units
                taken[d.lot_key].append(
                    FundIdentification(
                        cast(Literal["thirty_day", "lifo"], rule),
                        a.lot_key,
                        a.executed_at,
                        a.uk_date,
                        units,
                        cost,
                        () if cost is not None else (a.lot_key,),
                        (),
                    )
                )
        if need != 0:
            return FundRefused("identification_incomplete", f"{d.lot_key} leaves {need} units")

    # Design 4: reasons carried from a disposal onwards, in identification order.
    acq_reasons = {a.lot_key: a.reasons for a in acqs}
    base = {HISTORY_COVERAGE_UNESTABLISHED, NON_REPORTING_STATUS_ASSUMED}
    carried: set[str] = set()
    open_since: date | None = None
    disposals: list[FundDisposal] = []
    for seq, d in enumerate(disps, start=1):
        if history_fetch_started_at is None or history_fetch_started_at < window_elapses_at(d.uk_date):
            carried.add(WINDOW_OPEN)
            open_since = d.uk_date if open_since is None else open_since
        if any(a.uk_date == d.uk_date and a.executed_at > d.executed_at for a in acqs):
            carried.add(THIRTY_DAY_PERIOD_ASSUMED)
        idents = tuple(
            replace(i, reasons=_sorted(base | acq_reasons[i.acquisition_lot_key] | carried)) for i in taken[d.lot_key]
        )
        costs = [i.cost_gbp for i in idents]
        cost = None if any(c is None for c in costs) else sum((c for c in costs if c is not None), _ZERO)
        missing = {k for i in idents for k in i.missing_lot_keys} | ({d.lot_key} if d.amount_gbp is None else set())
        if d.amount_gbp is None or cost is None:
            status: Literal["computed", "amount_missing"] = "amount_missing"
            gain = basic = oig = chargeable = None
            character: GainCharacter | None = None
        else:
            status, gain = "computed", d.amount_gbp - cost
            character, basic, oig, chargeable = gain_character(gain, transparent_fund)
        disposals.append(
            FundDisposal(
                seq=seq,
                lot_key=d.lot_key,
                executed_at=d.executed_at,
                uk_date=d.uk_date,
                tax_year=d.tax_year,
                units=d.units,
                proceeds_gbp=d.amount_gbp,
                cost_gbp=cost,
                computed_gain_gbp=gain,
                basic_gain_gbp=basic,
                offshore_income_gain_gbp=oig,
                chargeable_gain_gbp=chargeable,
                status=status,
                gain_character=character,
                missing_lot_keys=_sorted(missing),
                reasons=_sorted(
                    base | d.reasons | {r for i in idents for r in acq_reasons[i.acquisition_lot_key]} | carried
                ),
                identifications=idents,
            )
        )
    balances = tuple(
        FundBalance(
            a.lot_key,
            units_left[a.lot_key],
            cost_left[a.lot_key],
            () if cost_left[a.lot_key] is not None else (a.lot_key,),
            _sorted(base | a.reasons | carried),
        )
        for a in acqs
        if units_left[a.lot_key] > 0
    )
    pool_costs = [b.cost_left_gbp for b in balances]
    pool = Pool(
        units=sum((b.units_left for b in balances), _ZERO),
        cost_gbp=None if any(c is None for c in pool_costs) else sum((c for c in pool_costs if c is not None), _ZERO),
        missing_lot_keys=_sorted(k for b in balances for k in b.missing_lot_keys),
        reasons=_sorted(base | carried | {r for b in balances for r in b.reasons}),
        window_open_since=open_since,
    )
    out = FundIdentified(tuple(disposals), balances, pool)
    _check_fund_invariants(lots, out, transparent_fund)
    return out


def _check_fund_invariants(lots: Sequence[FundLot], out: FundIdentified, transparent_fund: str) -> None:
    """Design 2's invariants and every column capacity; raises on the first failure."""
    acqs = [x for x in lots if x.direction == "acquisition"]
    proceeds = {x.lot_key: x.amount_gbp for x in lots if x.direction == "disposal"}

    def require(holds: bool, what: str) -> None:
        if not holds:
            raise RuntimeError(f"fund identification invariant: {what}")

    def fit(value: Decimal | None, precision: int, scale: int, what: str) -> None:
        require(
            value is None or fits(value, precision, scale), f"{what} {value} does not fit NUMERIC({precision},{scale})"
        )

    balance = {b.lot_key: b for b in out.balances}
    for a in acqs:
        idents = [i for x in out.disposals for i in x.identifications if i.acquisition_lot_key == a.lot_key]
        b = balance.get(a.lot_key)
        left_units = _ZERO if b is None else b.units_left
        require(sum((i.units for i in idents), _ZERO) + left_units == a.units, f"{a.lot_key}: units not conserved")
        if a.amount_gbp is not None:
            costs = [i.cost_gbp for i in idents]
            left_cost = _ZERO if b is None else b.cost_left_gbp
            require(all(c is not None for c in costs) and left_cost is not None, f"{a.lot_key}: cost undefined")
            total = sum((c for c in costs if c is not None), _ZERO) + (left_cost or _ZERO)
            require(total == a.amount_gbp, f"{a.lot_key}: cost not conserved")
    for x in out.disposals:
        require(sum((i.units for i in x.identifications), _ZERO) == x.units, f"{x.lot_key}: identified units")
        for i in x.identifications:
            if i.rule == "thirty_day":
                require(x.uk_date < i.acquisition_uk_date <= x.uk_date + timedelta(days=30), f"{x.lot_key}: period")
            else:
                require(i.acquisition_executed_at < x.executed_at, f"{x.lot_key}: LIFO acquisition after the disposal")
            require(i.cost_gbp is None or i.cost_gbp >= 0, f"{x.lot_key}: negative cost")
            fit(i.units, UNITS_PRECISION, UNITS_SCALE, f"{x.lot_key} identification units")
            fit(i.cost_gbp, GBP_PRECISION, GBP_SCALE, f"{x.lot_key} identification cost")
        costs = [i.cost_gbp for i in x.identifications]
        cost = None if any(c is None for c in costs) else sum((c for c in costs if c is not None), _ZERO)
        require(x.cost_gbp == cost, f"{x.lot_key}: cost is not its identifications' sum")
        require(x.proceeds_gbp == proceeds[x.lot_key], f"{x.lot_key}: proceeds are not its lot's amount")
        if x.proceeds_gbp is None or cost is None:
            got: tuple[Any, ...] = (x.status, x.computed_gain_gbp, x.gain_character, x.basic_gain_gbp)
            require(got == ("amount_missing", None, None, None), f"{x.lot_key}: an amount is missing")
            require(x.offshore_income_gain_gbp is None and x.chargeable_gain_gbp is None, f"{x.lot_key}: split")
        else:
            gain = x.proceeds_gbp - cost
            got = (x.status, x.computed_gain_gbp, x.gain_character, x.basic_gain_gbp, x.offshore_income_gain_gbp)
            want = ("computed", gain, *gain_character(gain, transparent_fund))
            require(got + (x.chargeable_gain_gbp,) == want, f"{x.lot_key}: gain and character")
        fit(x.units, UNITS_PRECISION, UNITS_SCALE, f"{x.lot_key} units")
        for value in (
            x.proceeds_gbp,
            x.cost_gbp,
            x.computed_gain_gbp,
            x.basic_gain_gbp,
            x.offshore_income_gain_gbp,
            x.chargeable_gain_gbp,
        ):
            fit(value, GBP_PRECISION, GBP_SCALE, f"{x.lot_key} sterling")
    for b in out.balances:
        require(b.cost_left_gbp is None or b.cost_left_gbp >= 0, f"{b.lot_key}: negative balance cost")
        fit(b.units_left, UNITS_PRECISION, UNITS_SCALE, f"{b.lot_key} balance units")
        fit(b.cost_left_gbp, GBP_PRECISION, GBP_SCALE, f"{b.lot_key} balance cost")
    costs = [b.cost_left_gbp for b in out.balances]
    require(out.pool.units == sum((b.units_left for b in out.balances), _ZERO), "holding units")
    require(
        out.pool.cost_gbp
        == (None if any(c is None for c in costs) else sum((c for c in costs if c is not None), _ZERO)),
        "holding cost",
    )
    fit(out.pool.units, UNITS_PRECISION, UNITS_SCALE, "holding units")
    fit(out.pool.cost_gbp, GBP_PRECISION, GBP_SCALE, "holding cost")


# --- one generation (Designs 2-4) ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class InstrumentPlacement:
    instrument_id: int
    #: None when the id has no ``instruments`` row.
    symbol: str | None
    canonical_instrument_id: int | None
    component_id: int
    #: None when unestablished or unplaceable.
    tax_security_id: int | None


@dataclass(frozen=True)
class Dependency:
    #: None: every security (``unplaceable``).
    tax_security_id: int | None
    reason: WithholdReason
    subject: str | None = None
    lot_key: str | None = None
    instrument_id: int | None = None
    detail: str | None = None


@dataclass(frozen=True)
class CharacterRow:
    """A security's ``tax_matching_characters`` row (3d-3a Design 4; 3d-3c-3 Design 5)."""

    character: MatchingCharacter
    #: None for a share or a ``non_reporting_fund``.
    detail: str | None
    #: The single CIK, else None.
    cik: str | None
    #: The accessions selected, else None.
    latest_cover: str | None
    start_cover: str | None
    #: Design 1's result for a fund that reached the ETF line, else None: a failing status (``exchange_traded_fund``)
    #: or ``non_reporting_assumed`` (``non_reporting_fund``), with the determination run it was read from.
    fund: FundStatus | None = None
    fund_run_id: int | None = None


def decide_character(ev: SecurityEvidence, as_of: datetime, *, identity_established: bool) -> CharacterRow:
    """3d-3a Design 2: a member of an unestablished identity component is ``identity_unestablished`` without
    evaluation; every other security is ``classify``'d."""
    character, detail = classify(ev, as_of) if identity_established else ("unestablished", IDENTITY_UNESTABLISHED)
    selection = select_covers(ev.filings, ev.first_acquisition)
    return CharacterRow(
        character,
        detail,
        next(iter(ev.ciks)) if len(ev.ciks) == 1 and is_cik(next(iter(ev.ciks))) else None,
        None if selection.latest is None else selection.latest.accession,
        None if selection.start is None else selection.start.accession,
    )


@dataclass(frozen=True)
class SecurityResult:
    tax_security_id: int
    instrument_ids: tuple[int, ...]
    status: Literal["computed", "withheld"]
    character: CharacterRow
    #: None when withheld. A computed fund's holding totals (Design 2 step 5).
    pool: Pool | None
    disposals: tuple[StatutoryDisposal, ...]
    #: A computed ``non_reporting_fund``'s disposals and balances; its ``disposals`` are then empty.
    fund: FundIdentified | None = None

    @property
    def holding_basis(self) -> Literal["s104_pool", "relevant_securities"] | None:
        if self.status == "withheld":
            return None
        return "relevant_securities" if self.character.character == "non_reporting_fund" else "s104_pool"


@dataclass(frozen=True)
class MatchingResult:
    placements: tuple[InstrumentPlacement, ...]
    lot_classes: Mapping[str, LotClassification]
    dependencies: tuple[Dependency, ...]
    securities: tuple[SecurityResult, ...]
    withheld_all: bool

    def counts(self) -> dict[str, int]:
        computed = [s for s in self.securities if s.status == "computed"]
        funds = [s.fund for s in computed if s.fund is not None]
        return {
            "lots": len(self.lot_classes),
            "securities_computed": len(computed),
            "securities_withheld": len(self.securities) - len(computed),
            "statutory_disposals": sum(len(s.disposals) for s in computed),
            "identifications": sum(len(x.identifications) for s in computed for x in s.disposals),
            "fund_disposals": sum(len(f.disposals) for f in funds),
            "fund_identifications": sum(len(x.identifications) for f in funds for x in f.disposals),
            "fund_balances": sum(len(f.balances) for f in funds),
            "dependencies": len(self.dependencies),
        }


def _named_instruments(inputs: MatchingInputs) -> set[int]:
    named = {lot.instrument_id for lot in inputs.lots}
    named |= {s.instrument_id for s in inputs.subjects if s.instrument_id is not None}
    return named


def placements(inputs: MatchingInputs) -> tuple[InstrumentPlacement, ...]:
    """Every member of every component the run's evidence names, and each named id with no row (Design 9)."""
    named = _named_instruments(inputs)
    comps = components(inputs.instruments, named)
    out: dict[int, InstrumentPlacement] = {}
    for component in set(comps.values()):
        security = tax_security_of(component, inputs.instruments)
        for member in component:
            row = inputs.instruments[member]
            out[member] = InstrumentPlacement(member, row.symbol, row.canonical_instrument_id, min(component), security)
    for iid in named - out.keys():
        out[iid] = InstrumentPlacement(iid, None, None, iid, None)
    return tuple(out[iid] for iid in sorted(out))


def security_members(inputs: MatchingInputs) -> dict[int, tuple[int, ...]]:
    """Every security a run has a row for, with its members (Designs 2-3): the security of each unresolved subject's
    and each lot's instrument. An established component is keyed by its base; a member of an unestablished one is its
    own security; an id with no ``instruments`` row is unplaceable and has none."""
    placed = placements(inputs)
    placement_of = {p.instrument_id: p for p in placed}
    named = [s.instrument_id for s in inputs.subjects if s.outcome == "unresolved" and s.instrument_id is not None]
    named += [lot.instrument_id for lot in inputs.lots]
    out: dict[int, tuple[int, ...]] = {}
    for iid in named:
        placement = placement_of.get(iid)
        if placement is None or placement.symbol is None:
            continue
        if placement.tax_security_id is None:
            out[iid] = (iid,)
        else:
            out[placement.tax_security_id] = tuple(
                sorted(p.instrument_id for p in placed if p.component_id == placement.component_id)
            )
    return out


def fund_statuses(inputs: MatchingInputs) -> dict[int, FundStatus]:
    """3d-3c-3 Design 1's status of every security that reaches it: an established identity whose character inputs
    reach 3d-3a's ``exchange_traded_fund`` line. A security decided by an earlier line (``member_conflict`` among
    them) or of unestablished identity is never evaluated as a fund. Dates are the UK dates of its lots."""
    established = {p.tax_security_id for p in placements(inputs) if p.tax_security_id is not None}
    members = security_members(inputs)
    security_of = {m: key for key, ms in members.items() for m in ms}
    lot_dates: dict[int, set[date]] = defaultdict(set)
    for lot in inputs.lots:
        key = security_of.get(lot.instrument_id)
        if key is not None:
            lot_dates[key].add(lot.uk_date)
    no_run = FundInputs(None, None, None, None, None)
    return {
        key: fund_status(inputs.funds.get(key, no_run), lot_dates[key], inputs.as_of)
        for key in sorted(members)
        if key in established and classify(inputs.characters[key], inputs.as_of) == ("unestablished", ETF_LINE)
    }


def inputs_document(inputs: MatchingInputs, funds: Mapping[int, FundStatus]) -> str:
    """The canonical inputs document (Design 9; 3d-3a Design 4), serialized as 3c's fingerprint. Tuple positions are
    fixed; only the outer lists are sorted. ``funds`` is ``fund_statuses(inputs)``: ``matching_inputs_v4`` is v3's
    document plus each fund's Design 1 status (3d-3c-3 Design 6)."""
    document: dict[str, Any] = {
        "version": INPUTS_VERSION,
        "evidence": sorted([lot.lot_key, lot.fingerprint] for lot in inputs.lots),
        "subjects": sorted(
            [
                s.subject,
                s.outcome,
                s.instrument_id,
                s.execution_evidence,
                None if s.total_fees_unobserved_position_ids is None else sorted(s.total_fees_unobserved_position_ids),
                None if s.total_fees_nonzero_position_ids is None else list(s.total_fees_nonzero_position_ids),
                None if s.position_ids is None else sorted(s.position_ids),
            ]
            for s in inputs.subjects
        ),
        "instruments": [[p.instrument_id, p.symbol, p.canonical_instrument_id] for p in placements(inputs)],
        "character": [document_entry(key, inputs.characters[key], inputs.as_of) for key in sorted(inputs.characters)],
    }
    document["funds"] = [[key, fund_digest_entry(funds[key])] for key in sorted(funds)]
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def inputs_digest(document: str) -> str:
    return hashlib.sha256(document.encode("ascii")).hexdigest()


def compute_matching(inputs: MatchingInputs) -> MatchingResult:
    """Classify, map, withhold and identify one generation in memory (Designs 2-8), then check every stored value
    against its column (Design 6). Raises on a broken invariant or capacity: the run then writes nothing."""
    placed = placements(inputs)
    placement_of = {p.instrument_id: p for p in placed}
    subjects = {s.subject: s for s in inputs.subjects}
    classes = {lot.lot_key: classify_lot(lot, subjects[lot.subject]) for lot in inputs.lots}

    dependencies: list[Dependency] = []
    withheld_all = False
    security_keys: dict[int, tuple[int, ...]] = {}
    unestablished_identity: set[int] = set()
    own_reasons: dict[int, list[Dependency]] = defaultdict(list)

    def security_for(instrument_id: int | None) -> int | None:
        """The security's key, registering its row; None when unplaceable."""
        placement = None if instrument_id is None else placement_of.get(instrument_id)
        if placement is None or placement.symbol is None:
            return None
        if placement.tax_security_id is not None:
            component = tuple(sorted(p.instrument_id for p in placed if p.component_id == placement.component_id))
            security_keys[placement.tax_security_id] = component
            return placement.tax_security_id
        if placement.instrument_id not in security_keys:
            security_keys[placement.instrument_id] = (placement.instrument_id,)
            unestablished_identity.add(placement.instrument_id)
            members = sorted(p.instrument_id for p in placed if p.component_id == placement.component_id)
            own_reasons[placement.instrument_id].append(
                Dependency(
                    placement.instrument_id,
                    "identity_unestablished",
                    instrument_id=placement.instrument_id,
                    detail=f"component {members}",
                )
            )
        return placement.instrument_id

    for s in sorted(inputs.subjects, key=lambda x: x.subject):
        if s.outcome != "unresolved":
            continue
        key = security_for(s.instrument_id)
        if key is None:
            withheld_all = True
            dependencies.append(Dependency(None, "unplaceable", subject=s.subject, instrument_id=s.instrument_id))
        else:
            own_reasons[key].append(
                Dependency(key, "unresolved_subject", subject=s.subject, instrument_id=s.instrument_id)
            )

    lots_by_security: dict[int, list[LotRow]] = defaultdict(list)
    for lot in sorted(inputs.lots, key=lambda x: x.lot_key):
        key = security_for(lot.instrument_id)
        if key is None:
            withheld_all = True
            dependencies.append(Dependency(None, "unplaceable", lot_key=lot.lot_key, instrument_id=lot.instrument_id))
            continue
        lots_by_security[key].append(lot)
        cls = classes[lot.lot_key]
        if cls.lot_class == "unavailable":
            own_reasons[key].append(
                Dependency(
                    key,
                    "lot_unavailable",
                    lot_key=lot.lot_key,
                    instrument_id=lot.instrument_id,
                    detail=",".join(cls.reasons),
                )
            )

    missing = sorted(security_keys.keys() - inputs.characters.keys())
    if missing:
        raise RuntimeError(f"character inputs missing for securities {missing}")

    funds = fund_statuses(inputs)
    securities: list[SecurityResult] = []
    for key in sorted(security_keys):
        # Character is decided before identification: a security not established as a share or a non-reporting fund
        # is never identified.
        character = decide_character(
            inputs.characters[key], inputs.as_of, identity_established=key not in unestablished_identity
        )
        if (key in funds) != (character.detail == ETF_LINE):
            raise RuntimeError(f"security {key}: fund status and the ETF line disagree")
        if key in funds:  # tax_character_v2's ETF line (3d-3c-3 Design 1)
            status = funds[key]
            if status.status == "non_reporting_assumed":
                run_id = inputs.funds[key].run_id
                character = replace(
                    character, character="non_reporting_fund", detail=None, fund=status, fund_run_id=run_id
                )
            else:
                character = replace(character, fund=status)
        reasons = list(own_reasons.get(key, []))
        if character.character == "unestablished":
            reasons.append(Dependency(key, "character_unestablished", detail=character.detail))
        result: Identified | StepFiveReached | None = None
        fund: FundIdentified | FundRefused | None = None
        if not withheld_all and not reasons and character.character == "non_reporting_fund":
            transparent = character.fund.transparent_fund if character.fund else None
            if transparent is None:
                raise RuntimeError(f"security {key}: non_reporting_fund without its transparency")
            fund = identify_fund(
                [
                    FundLot(
                        lot.lot_key,
                        "acquisition" if lot.direction == "acquisition" else "disposal",
                        lot.subject,
                        lot.executed_at,
                        lot.uk_date,
                        lot.tax_year,
                        lot.units,
                        lot.amount_gbp,
                        frozenset(classes[lot.lot_key].reasons),
                    )
                    for lot in lots_by_security.get(key, [])
                ],
                inputs.history_fetch_started_at,
                transparent,
            )
            if isinstance(fund, FundRefused):
                reasons = [Dependency(key, fund.reason, detail=fund.detail)]
        elif not withheld_all and not reasons:
            result = identify_security(
                [
                    MatchLot(
                        lot.lot_key,
                        "acquisition" if lot.direction == "acquisition" else "disposal",
                        lot.uk_date,
                        lot.tax_year,
                        lot.units,
                        lot.amount_gbp,
                        frozenset(classes[lot.lot_key].reasons),
                    )
                    for lot in lots_by_security.get(key, [])
                ],
                inputs.history_fetch_started_at,
            )
            if isinstance(result, StepFiveReached):
                reasons = [
                    Dependency(
                        key,
                        "subsequent_acquisition_rule",
                        detail=f"disposal {result.disposal_uk_date.isoformat()}: {result.units} units unidentified",
                    )
                ]
        dependencies.extend(reasons)
        if isinstance(result, Identified):
            securities.append(
                SecurityResult(key, security_keys[key], "computed", character, result.pool, result.disposals)
            )
        elif isinstance(fund, FundIdentified):
            securities.append(SecurityResult(key, security_keys[key], "computed", character, fund.pool, (), fund))
        else:
            securities.append(SecurityResult(key, security_keys[key], "withheld", character, None, ()))

    result_set = MatchingResult(
        placements=placed,
        lot_classes=classes,
        dependencies=tuple(dependencies),
        securities=tuple(securities),
        withheld_all=withheld_all,
    )
    _check_capacities(result_set)
    return result_set


def _check_capacities(result: MatchingResult) -> None:
    def check(value: Decimal | None, precision: int, scale: int, what: str) -> None:
        if value is not None and not fits(value, precision, scale):
            raise ValueError(f"{what} {value} does not fit NUMERIC({precision},{scale})")

    for s in result.securities:
        if s.pool is not None:
            check(s.pool.units, UNITS_PRECISION, UNITS_SCALE, f"security {s.tax_security_id} pool units")
            check(s.pool.cost_gbp, GBP_PRECISION, GBP_SCALE, f"security {s.tax_security_id} pool cost")
        for x in s.disposals:
            where = f"security {s.tax_security_id} disposal {x.uk_date}"
            check(x.units, UNITS_PRECISION, UNITS_SCALE, f"{where} units")
            for name, value in (("proceeds", x.proceeds_gbp), ("cost", x.cost_gbp), ("gain", x.gain_gbp)):
                check(value, GBP_PRECISION, GBP_SCALE, f"{where} {name}")
            for i in x.identifications:
                check(i.units, UNITS_PRECISION, UNITS_SCALE, f"{where} identification units")
                check(i.cost_gbp, GBP_PRECISION, GBP_SCALE, f"{where} identification cost")


# --- storage and selection (Designs 9-10) -------------------------------------------------------------------------


def _clock(conn: psycopg.Connection[Any]) -> datetime:
    """The run's ``as_of`` (3d-3a Design 5): the database clock, read right after the snapshot starts."""
    clock = conn.execute("SELECT clock_timestamp()").fetchone()
    if clock is None:
        raise RuntimeError("clock_timestamp() returned no row")
    return clock[0]


def _read_inputs(conn: psycopg.Connection[Any], *, bodies: bool = True) -> MatchingInputs | None:
    """The effective generation and everything matching reads (Design 1; 3d-3a Design 5; 3d-3c-3 Design 6); the caller
    holds one snapshot. ``as_of`` is read first, right after the snapshot starts. ``bodies=False`` leaves the cover
    instances unread (``refresh_cover_instances`` needs only the selection)."""
    as_of = _clock(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        run = cur.execute(
            """
            SELECT r.run_id, r.evidence_policy_id, f.started_at AS fetch_started_at
            FROM tax_evidence_runs r
            JOIN trade_ledger_scope s ON s.environment = r.environment AND s.account_id = r.account_id
            LEFT JOIN trade_history_fetches f ON f.fetch_id = r.history_fetch_ok_id AND f.outcome = 'ok'
            WHERE r.state = 'published'
            ORDER BY r.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        if run is None:
            return None
        subjects = tuple(
            SubjectRow(
                r["subject"],
                r["outcome"],
                r["instrument_id"],
                r["execution_evidence"],
                None if r["unobserved"] is None else tuple(r["unobserved"]),
                None if r["nonzero"] is None else tuple(r["nonzero"]),
                _position_keys(r["positions"]),
            )
            for r in cur.execute(
                """
                SELECT subject, outcome, instrument_id, execution_evidence,
                       total_fees_unobserved_position_ids AS unobserved,
                       total_fees_nonzero_position_ids AS nonzero,
                       ARRAY(SELECT jsonb_object_keys(held_positions)) AS positions
                FROM tax_evidence_subjects WHERE run_id = %(run)s ORDER BY subject
                """,
                {"run": run["run_id"]},
            ).fetchall()
        )
        lots = tuple(
            LotRow(**r)
            for r in cur.execute(
                """
                SELECT lot_key, subject, instrument_id, direction, uk_date, tax_year, units, amount_gbp,
                       execution_evidence, fingerprint, executed_at
                FROM tax_evidence_lots WHERE run_id = %(run)s ORDER BY lot_key
                """,
                {"run": run["run_id"]},
            ).fetchall()
        )
        instruments = {
            r["instrument_id"]: InstrumentRow(r["instrument_id"], r["symbol"], r["canonical_instrument_id"])
            for r in cur.execute("SELECT instrument_id, symbol, canonical_instrument_id FROM instruments").fetchall()
        }
    inputs = MatchingInputs(
        evidence_run_id=run["run_id"],
        evidence_policy_id=run["evidence_policy_id"],
        history_fetch_started_at=run["fetch_started_at"],
        subjects=subjects,
        lots=lots,
        instruments=instruments,
        as_of=as_of,
        characters={},
    )
    members = security_members(inputs)
    security_of = {m: key for key, ms in members.items() for m in ms}
    first_acquisitions: dict[int, date] = {}
    for lot in lots:
        key = security_of.get(lot.instrument_id)
        if key is not None and lot.direction == "acquisition":
            first_acquisitions[key] = min(first_acquisitions.get(key, lot.uk_date), lot.uk_date)
    characters = read_evidence(conn, members, first_acquisitions, bodies=bodies)
    funds = _read_funds(conn, sorted(key for key, ev in characters.items() if ev.types == {ETF_TYPE_ID}))
    return replace(inputs, characters=characters, funds=funds)


def _read_funds(conn: psycopg.Connection[Any], keys: Sequence[int]) -> dict[int, FundInputs]:
    """3d-3c-3 Design 1's inputs for each all-ETF security: the newest determination run visible in the snapshot
    (whatever its status), its observation of the security, the determination body and file hash, and the fund
    observation's list search."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        run = cur.execute(
            "SELECT run_id, status, cutoff FROM tax_fund_determination_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if run is None:
            return {key: FundInputs(None, None, None, None, None) for key in keys}
        observed = {
            r["tax_security_id"]: r
            for r in cur.execute(
                """
                SELECT o.tax_security_id, o.outcome, o.refusal, o.determination_id, d.body, d.file_sha256,
                       f.search_outcome
                FROM tax_fund_determination_observations o
                LEFT JOIN tax_fund_determinations d ON d.determination_id = o.determination_id
                LEFT JOIN tax_fund_observations f ON f.observation_id = o.fund_observation_id
                WHERE o.run_id = %(run)s AND o.tax_security_id = ANY(%(keys)s)
                """,
                {"run": run["run_id"], "keys": list(keys)},
            ).fetchall()
        }
    state = RunState(run["status"], run["cutoff"])
    out: dict[int, FundInputs] = {}
    for key in keys:
        r = observed.get(key)
        if r is None:
            out[key] = FundInputs(state, None, None, None, None, run["run_id"])
        else:
            out[key] = FundInputs(
                state,
                Decision(r["outcome"], r["refusal"], r["determination_id"]),
                r["body"],
                r["file_sha256"],
                r["search_outcome"],
                run["run_id"],
            )
    return out


def _existing_run(
    conn: psycopg.Connection[Any], evidence_run_id: int, digest: str
) -> tuple[int, str, str | None] | None:
    row = conn.execute(
        """
        SELECT matching_run_id, state, reason FROM tax_matching_runs
        WHERE evidence_run_id = %s AND matching_policy_id = %s AND class_policy_id = %s AND character_policy_id = %s
          AND inputs_digest = %s
        """,
        (evidence_run_id, TAX_MATCHING_POLICY_ID, CONSIDERATION_CLASS_POLICY_ID, TAX_CHARACTER_POLICY_ID, digest),
    ).fetchone()
    return None if row is None else (int(row[0]), str(row[1]), row[2])


@dataclass(frozen=True)
class EffectiveMatching:
    matching_run_id: int
    evidence_run_id: int


def effective_tax_matching(conn: psycopg.Connection[Any]) -> EffectiveMatching | None:
    """Exactly the published run of the effective generation, the current policy ids and the current
    ``inputs_digest``, resolved in one snapshot. None means *unavailable*: there is no fallback to an older run."""
    with snapshot_read(conn):
        return _effective_in_snapshot(conn)


def _effective_in_snapshot(conn: psycopg.Connection[Any]) -> EffectiveMatching | None:
    inputs = _read_inputs(conn)
    if inputs is None:
        return None
    existing = _existing_run(
        conn, inputs.evidence_run_id, inputs_digest(inputs_document(inputs, fund_statuses(inputs)))
    )
    if existing is None or existing[1] != "published":
        return None
    return EffectiveMatching(existing[0], inputs.evidence_run_id)


TaxRunUnavailable = Literal["ledger_scope_not_demo", "no_effective_run"]


@dataclass(frozen=True)
class TaxRunResolution:
    """The effective run and the ledger scope's environment, or why there is none (3d-4a Design 1)."""

    environment: str | None
    run: EffectiveMatching | None
    reason: TaxRunUnavailable | None


def require_snapshot(conn: psycopg.Connection[Any]) -> None:
    """Raise unless ``conn`` holds an active REPEATABLE READ or SERIALIZABLE transaction, read from the server: the
    connection's ``isolation_level`` describes the next transaction, not necessarily the open one."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
        raise RuntimeError("tax run resolution needs the caller's open REPEATABLE READ transaction; none is active")
    row = conn.execute("SELECT current_setting('transaction_isolation')").fetchone()
    if row is None or row[0] not in ("repeatable read", "serializable"):
        raise RuntimeError(f"tax run resolution needs REPEATABLE READ or SERIALIZABLE, not {row and row[0]}")


def resolve_tax_run(conn: psycopg.Connection[Any]) -> TaxRunResolution:
    """3d-4a Design 1. Runs inside the caller's snapshot (``require_snapshot``); never commits, never changes the
    isolation level and opens no transaction, so it is safe on a connection the caller is using."""
    require_snapshot(conn)
    scope = conn.execute("SELECT environment FROM trade_ledger_scope").fetchone()
    environment = None if scope is None else str(scope[0])
    if environment is None:
        return TaxRunResolution(None, None, "no_effective_run")
    if environment != "demo":
        return TaxRunResolution(environment, None, "ledger_scope_not_demo")
    run = _effective_in_snapshot(conn)
    return TaxRunResolution(environment, run, None if run is not None else "no_effective_run")


@dataclass(frozen=True)
class MatchingRunReport:
    outcome: Literal["published", "already_published", "refused", "no_generation"]
    matching_run_id: int | None = None
    evidence_run_id: int | None = None
    reason: str | None = None
    counts: Mapping[str, int] = field(default_factory=dict)


def run_tax_matching(conn: psycopg.Connection[Any]) -> MatchingRunReport:
    """One run (Design 10) on one autocommit connection.

    The session lock is taken outside any transaction, then one REPEATABLE READ transaction reads the inputs, so its
    snapshot sees every earlier run's commit. A run already stored for the key is returned as it stands (a refusal
    stays visible). A refusal is a decided outcome, stored in this transaction. Any exception rolls the whole
    transaction back, leaving no row of the run, and propagates. The lock is released in every case.
    """
    if not conn.autocommit:
        raise ValueError("run_tax_matching needs an autocommit connection: the lock is taken outside any transaction")
    conn.execute("SELECT pg_advisory_lock(%s)", [TAX_MATCHING_LOCK_KEY])
    prior = conn.isolation_level
    try:
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            inputs = _read_inputs(conn)
            if inputs is None:
                return MatchingRunReport("no_generation")
            document = inputs_document(inputs, fund_statuses(inputs))
            digest = inputs_digest(document)
            existing = _existing_run(conn, inputs.evidence_run_id, digest)
            if existing is not None:
                run_id, state, reason = existing
                outcome: Literal["already_published", "refused"] = (
                    "already_published" if state == "published" else "refused"
                )
                return MatchingRunReport(outcome, run_id, inputs.evidence_run_id, reason)
            if inputs.evidence_policy_id != SUPPORTED_EVIDENCE_POLICY_ID:
                run_id = _insert_run(conn, inputs, document, digest, withheld_all=False)
                _seal(conn, run_id, "refused", "evidence_policy_unsupported", {})
                return MatchingRunReport("refused", run_id, inputs.evidence_run_id, "evidence_policy_unsupported")
            result = compute_matching(inputs)
            run_id = _insert_run(conn, inputs, document, digest, withheld_all=result.withheld_all)
            _insert_rows(conn, run_id, inputs.evidence_run_id, result)
            _check_one_character_per_security(conn, run_id)
            counts = result.counts()
            _seal(conn, run_id, "published", None, counts)
        return MatchingRunReport("published", run_id, inputs.evidence_run_id, None, counts)
    finally:
        conn.isolation_level = prior
        conn.execute("SELECT pg_advisory_unlock(%s)", [TAX_MATCHING_LOCK_KEY])


def _insert_run(
    conn: psycopg.Connection[Any], inputs: MatchingInputs, document: str, digest: str, *, withheld_all: bool
) -> int:
    row = conn.execute(
        """
        INSERT INTO tax_matching_runs
            (evidence_run_id, matching_policy_id, class_policy_id, character_policy_id, inputs_digest, inputs_document,
             state, computed_at, as_of, history_fetch_started_at, withheld_all)
        VALUES (%s, %s, %s, %s, %s, %s, 'building', clock_timestamp(), %s, %s, %s)
        RETURNING matching_run_id
        """,
        (
            inputs.evidence_run_id,
            TAX_MATCHING_POLICY_ID,
            CONSIDERATION_CLASS_POLICY_ID,
            TAX_CHARACTER_POLICY_ID,
            digest,
            document,
            inputs.as_of,
            inputs.history_fetch_started_at,
            withheld_all,
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("tax_matching_runs insert returned no matching_run_id")
    return int(row[0])


def _seal(
    conn: psycopg.Connection[Any], run_id: int, state: str, reason: str | None, counts: Mapping[str, int]
) -> None:
    conn.execute(
        "UPDATE tax_matching_runs SET state = %s, reason = %s, counts = %s WHERE matching_run_id = %s",
        (state, reason, Jsonb(dict(counts)), run_id),
    )


def _insert_rows(conn: psycopg.Connection[Any], run_id: int, evidence_run_id: int, result: MatchingResult) -> None:
    computed = [s for s in result.securities if s.status == "computed"]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO tax_matching_instruments (matching_run_id, instrument_id, symbol, canonical_instrument_id, "
            "component_id, tax_security_id) VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (run_id, p.instrument_id, p.symbol, p.canonical_instrument_id, p.component_id, p.tax_security_id)
                for p in result.placements
            ],
        )
        cur.executemany(
            "INSERT INTO tax_matching_lot_classes (matching_run_id, lot_key, evidence_run_id, class, reasons) "
            "VALUES (%s, %s, %s, %s, %s)",
            [
                (run_id, lot_key, evidence_run_id, c.lot_class, list(c.reasons))
                for lot_key, c in sorted(result.lot_classes.items())
            ],
        )
        cur.executemany(
            "INSERT INTO tax_matching_securities (matching_run_id, tax_security_id, instrument_ids, status, "
            "pool_units, pool_cost_gbp, pool_missing_lot_keys, pool_reasons, window_open_since, holding_basis) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    s.tax_security_id,
                    list(s.instrument_ids),
                    s.status,
                    *(
                        (None, None, None, None, None)
                        if s.pool is None
                        else (
                            s.pool.units,
                            s.pool.cost_gbp,
                            list(s.pool.missing_lot_keys),
                            list(s.pool.reasons),
                            s.pool.window_open_since,
                        )
                    ),
                    s.holding_basis,
                )
                for s in result.securities
            ],
        )
        cur.executemany(
            "INSERT INTO tax_matching_characters (matching_run_id, tax_security_id, character, detail, cik, "
            "latest_cover, start_cover, fund_status, fund_status_reason, fund_status_date, fund_determination_id, "
            "fund_determination_run_id, transparent_fund, transparent_basis, fund_assumptions) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    s.tax_security_id,
                    s.character.character,
                    s.character.detail,
                    s.character.cik,
                    s.character.latest_cover,
                    s.character.start_cover,
                    *_fund_columns(s.character),
                )
                for s in result.securities
            ],
        )
        cur.executemany(
            "INSERT INTO tax_matching_dependencies (matching_run_id, seq, tax_security_id, reason, subject, lot_key, "
            "instrument_id, detail) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (run_id, seq, d.tax_security_id, d.reason, d.subject, d.lot_key, d.instrument_id, d.detail)
                for seq, d in enumerate(result.dependencies, start=1)
            ],
        )
        cur.executemany(
            "INSERT INTO tax_statutory_disposals (matching_run_id, tax_security_id, uk_date, tax_year, lot_keys, "
            "units, proceeds_gbp, cost_gbp, gain_gbp, status, missing_lot_keys, reasons) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    s.tax_security_id,
                    x.uk_date,
                    x.tax_year,
                    list(x.lot_keys),
                    x.units,
                    x.proceeds_gbp,
                    x.cost_gbp,
                    x.gain_gbp,
                    x.status,
                    list(x.missing_lot_keys),
                    list(x.reasons),
                )
                for s in computed
                for x in s.disposals
            ],
        )
        cur.executemany(
            "INSERT INTO tax_identifications (matching_run_id, tax_security_id, disposal_uk_date, seq, rule, "
            "acquisition_uk_date, acquisition_lot_keys, units, cost_gbp, missing_lot_keys, reasons) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    s.tax_security_id,
                    x.uk_date,
                    seq,
                    i.rule,
                    i.acquisition_uk_date,
                    list(i.acquisition_lot_keys),
                    i.units,
                    i.cost_gbp,
                    list(i.missing_lot_keys),
                    list(i.reasons),
                )
                for s in computed
                for x in s.disposals
                for seq, i in enumerate(x.identifications, start=1)
            ],
        )
        funds = [(s.tax_security_id, s.fund) for s in computed if s.fund is not None]
        cur.executemany(
            "INSERT INTO tax_fund_disposals (matching_run_id, tax_security_id, lot_key, seq, executed_at, uk_date, "
            "tax_year, units, proceeds_gbp, cost_gbp, computed_gain_gbp, basic_gain_gbp, offshore_income_gain_gbp, "
            "chargeable_gain_gbp, status, gain_character, missing_lot_keys, reasons) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    key,
                    x.lot_key,
                    x.seq,
                    x.executed_at,
                    x.uk_date,
                    x.tax_year,
                    x.units,
                    x.proceeds_gbp,
                    x.cost_gbp,
                    x.computed_gain_gbp,
                    x.basic_gain_gbp,
                    x.offshore_income_gain_gbp,
                    x.chargeable_gain_gbp,
                    x.status,
                    x.gain_character,
                    list(x.missing_lot_keys),
                    list(x.reasons),
                )
                for key, f in funds
                for x in f.disposals
            ],
        )
        cur.executemany(
            "INSERT INTO tax_fund_identifications (matching_run_id, tax_security_id, disposal_lot_key, seq, rule, "
            "acquisition_lot_key, acquisition_executed_at, acquisition_uk_date, units, cost_gbp, missing_lot_keys, "
            "reasons) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    key,
                    x.lot_key,
                    seq,
                    i.rule,
                    i.acquisition_lot_key,
                    i.acquisition_executed_at,
                    i.acquisition_uk_date,
                    i.units,
                    i.cost_gbp,
                    list(i.missing_lot_keys),
                    list(i.reasons),
                )
                for key, f in funds
                for x in f.disposals
                for seq, i in enumerate(x.identifications, start=1)
            ],
        )
        cur.executemany(
            "INSERT INTO tax_fund_acquisition_balances (matching_run_id, tax_security_id, lot_key, units_left, "
            "cost_left_gbp, missing_lot_keys, reasons) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [
                (run_id, key, b.lot_key, b.units_left, b.cost_left_gbp, list(b.missing_lot_keys), list(b.reasons))
                for key, f in funds
                for b in f.balances
            ],
        )


def _fund_columns(c: CharacterRow) -> tuple[Any, ...]:
    """The character row's eight fund columns (3d-3c-3 Design 5's truth table): all NULL unless the security reached
    the ETF line; a failing status alone, or the determination, run, transparency and assumptions alone."""
    f = c.fund
    if f is None:
        return (None,) * 8
    if c.character != "non_reporting_fund":
        return (f.status, f.reason, f.status_date, None, None, None, None, None)
    if f.assumptions is None or list(f.assumptions) != sorted(set(f.assumptions)):
        raise RuntimeError(f"fund assumptions {f.assumptions} are not the sorted union")
    return (
        None,
        None,
        None,
        f.determination_id,
        c.fund_run_id,
        f.transparent_fund,
        f.transparent_basis,
        list(f.assumptions),
    )


def _check_one_character_per_security(conn: psycopg.Connection[Any], run_id: int) -> None:
    """Publication requires exactly one character row per security row (3d-3a Design 4), and a computed security's
    basis to be ``relevant_securities`` exactly when its character is ``non_reporting_fund`` (3d-3c-3 Design 5),
    checked before the seal."""
    row = conn.execute(
        """
        SELECT (SELECT count(*) FROM tax_matching_securities WHERE matching_run_id = %(r)s),
               (SELECT count(*) FROM tax_matching_characters WHERE matching_run_id = %(r)s),
               (SELECT count(*) FROM tax_matching_securities s WHERE s.matching_run_id = %(r)s AND NOT EXISTS (
                    SELECT 1 FROM tax_matching_characters c
                    WHERE c.matching_run_id = s.matching_run_id AND c.tax_security_id = s.tax_security_id))
        """,
        {"r": run_id},
    ).fetchone()
    if row is None or row[0] != row[1] or row[2] != 0:
        raise RuntimeError(f"run {run_id}: character rows do not match security rows one to one: {row}")
    mismatched = conn.execute(
        """
        SELECT s.tax_security_id FROM tax_matching_securities s
        JOIN tax_matching_characters c USING (matching_run_id, tax_security_id)
        WHERE s.matching_run_id = %s AND s.status = 'computed'
          AND (s.holding_basis = 'relevant_securities') <> (c.character = 'non_reporting_fund')
        """,
        (run_id,),
    ).fetchall()
    if mismatched:
        raise RuntimeError(f"run {run_id}: holding basis and character disagree for {[r[0] for r in mismatched]}")


# --- cover instances (3d-3a Design 1) -----------------------------------------------------------------------------


@dataclass(frozen=True)
class CoverRefreshReport:
    #: Selected covers of Stocks securities with one primary CIK.
    selected: int
    already_stored: int
    stored: int
    #: Selected covers whose primary document is not ``.htm``: no instance URL, not fetched, not a failure.
    no_instance_url: int
    #: ``accession: error`` per failed cover, sorted.
    failures: tuple[str, ...]


def refresh_cover_instances(
    conn: psycopg.Connection[Any], fetch: Callable[[str], FetchedInstance] | None = None
) -> CoverRefreshReport:
    """Store the instance of every cover the effective generation's characters read and ``tax_cover_instances``
    lacks (3d-3a Design 1), on an autocommit connection; the selection is read in one snapshot, then each missing
    cover is fetched and committed on its own. Only a structurally sound 200 body is stored; every other outcome is
    a failed cover, retried by the next run. Every cover is tried before returning."""
    if not conn.autocommit:
        raise ValueError("refresh_cover_instances needs an autocommit connection: each stored cover commits alone")
    if fetch is None:
        with httpx.Client(timeout=60, follow_redirects=False) as client:
            return refresh_cover_instances(conn, lambda url: fetch_instance(client, url))
    wanted: dict[str, str | None] = {}
    with snapshot_read(conn):
        inputs = _read_inputs(conn, bodies=False)
        if inputs is None:
            return CoverRefreshReport(0, 0, 0, 0, ())
        for ev in inputs.characters.values():
            if ev.types != {STOCKS_TYPE_ID} or len(ev.ciks) != 1:
                continue
            selection = select_covers(ev.filings, ev.first_acquisition)
            for filing in (selection.latest, selection.start):
                if filing is not None:
                    wanted[filing.accession] = filing.primary_document_url
        stored = {
            r[0]
            for r in conn.execute(
                "SELECT accession_number FROM tax_cover_instances WHERE accession_number = ANY(%s)", (sorted(wanted),)
            ).fetchall()
        }
    added = no_url = 0
    failures: list[str] = []
    for accession in sorted(wanted.keys() - stored):
        if not ACCESSION.fullmatch(accession):  # the table would refuse it; a failed cover, not a raised step
            failures.append(f"{accession}: malformed accession")
            continue
        document = wanted[accession]
        url = instance_url(document) if document else None
        if url is None:
            no_url += 1
            continue
        got = fetch(url)
        if got.body is None:
            failures.append(f"{accession}: {got.error}")
            continue
        if not structurally_sound(got.body):
            failures.append(f"{accession}: not an XBRL instance")
            continue
        conn.execute(
            "INSERT INTO tax_cover_instances (accession_number, source_url, fetched_at, body, body_sha256) "
            "VALUES (%s, %s, now(), %s, %s) ON CONFLICT (accession_number) DO NOTHING",
            (accession, url, got.body, hashlib.sha256(got.body).hexdigest()),
        )
        added += 1
    return CoverRefreshReport(len(wanted), len(stored), added, no_url, tuple(failures))
