"""Statutory share identification from the effective generation of tax evidence (#3616 slice 3d-2).

Spec: docs/specs/metrics/2026-10-14-3616-slice3d2-statutory-matching.md. Consideration classes:
docs/specs/metrics/2026-10-14-3616-slice3d1-consideration-basis.md ("Consideration classes").

- ``classify_lot`` is 3d-1's class table (``consideration_classes_v1``).
- ``tax_security_of`` maps a component of instruments to its tax security (Design 2, ``tax_security_v1``).
- ``identify_security`` is the identification of one security's admitted lots (Designs 5-8): pure, exact, and
  refusing step 5 (s105(2)) rather than implementing it.
- ``compute_matching`` applies all of it to one generation's inputs, withholding what the evidence cannot establish
  (Design 3).
- ``run_tax_matching`` stores one sealed snapshot per (generation, policy ids, ``inputs_digest``) (Designs 9-10), and
  ``effective_tax_matching`` selects exactly the snapshot of the current inputs, with no fallback.

Nothing here is shown to the operator: the tax-year view is slice 3d-4's.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Context, Decimal, Inexact, Rounded, localcontext
from typing import Any, Literal
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from psycopg import IsolationLevel
from psycopg.types.json import Jsonb

from app.db.snapshot import snapshot_read
from app.services.tax_ledger import MatchingRule

#: The frozen local policy (spec "Local policy"); changing any part of it is a new id.
TAX_MATCHING_POLICY_ID = "share_identification_v1"
#: 3d-1's class table.
CONSIDERATION_CLASS_POLICY_ID = "consideration_classes_v1"
#: The one evidence policy ``consideration_classes_v1`` was reviewed against (3d-1 "Consideration classes"). A
#: generation under any other policy is refused, not matched.
SUPPORTED_EVIDENCE_POLICY_ID = "broker_evidence_v2"
#: The version of the canonical inputs document (Design 9).
INPUTS_VERSION = "matching_inputs_v1"
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
WINDOW_OPEN = "window_open"
HISTORY_COVERAGE_UNESTABLISHED = "history_coverage_unestablished"
CHARACTER_PENDING = "character_pending"

# The fields whose failure makes a lot ``unavailable`` (3d-1; Design 3 ``lot_unavailable``).
EXECUTION_EVIDENCE_MISMATCH = "execution_evidence_mismatch"
TOTAL_FEES_UNOBSERVED_NULL = "total_fees_unobserved_null"
DIRECTION_INVALID = "direction_invalid"

LotClass = Literal["final_eligible", "provisional", "unavailable"]
WithholdReason = Literal[
    "unresolved_subject", "identity_unestablished", "lot_unavailable", "subsequent_acquisition_rule", "unplaceable"
]
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


# --- consideration classes (3d-1) ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class LotClassification:
    lot_class: LotClass
    #: Sorted. For ``unavailable``, the failing fields; otherwise the consideration reasons.
    reasons: tuple[str, ...]


def classify_lot(lot: LotRow, subject: SubjectRow) -> LotClassification:
    """3d-1's table, from the lot's own row and its subject's row in the same generation."""
    failing: list[str] = []
    if lot.direction not in ("acquisition", "disposal"):
        failing.append(DIRECTION_INVALID)
    if lot.execution_evidence not in ("lookup", "sync_only") or subject.execution_evidence != lot.execution_evidence:
        failing.append(EXECUTION_EVIDENCE_MISMATCH)
    if subject.total_fees_unobserved_position_ids is None:
        failing.append(TOTAL_FEES_UNOBSERVED_NULL)
    if failing:
        return LotClassification("unavailable", tuple(sorted(failing)))
    reasons: set[str] = set()
    if lot.direction == "acquisition":
        reasons.add(SPREAD_INCLUSION_ASSUMED)
        if lot.execution_evidence == "sync_only":
            reasons |= {EXECUTION_ASSUMED, LOOKUP_EVIDENCE_UNOBSERVED}
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
        reasons = set(day.reasons) | {HISTORY_COVERAGE_UNESTABLISHED, CHARACTER_PENDING}
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
    final_reasons = pool_reasons | {HISTORY_COVERAGE_UNESTABLISHED}
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
class SecurityResult:
    tax_security_id: int
    instrument_ids: tuple[int, ...]
    status: Literal["computed", "withheld"]
    #: None when withheld.
    pool: Pool | None
    disposals: tuple[StatutoryDisposal, ...]


@dataclass(frozen=True)
class MatchingResult:
    placements: tuple[InstrumentPlacement, ...]
    lot_classes: Mapping[str, LotClassification]
    dependencies: tuple[Dependency, ...]
    securities: tuple[SecurityResult, ...]
    withheld_all: bool

    def counts(self) -> dict[str, int]:
        computed = [s for s in self.securities if s.status == "computed"]
        return {
            "lots": len(self.lot_classes),
            "securities_computed": len(computed),
            "securities_withheld": len(self.securities) - len(computed),
            "statutory_disposals": sum(len(s.disposals) for s in computed),
            "identifications": sum(len(x.identifications) for s in computed for x in s.disposals),
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


def inputs_document(inputs: MatchingInputs) -> str:
    """The canonical inputs document (Design 9), serialized as 3c's fingerprint. Tuple positions are fixed; only the
    outer lists are sorted."""
    document = {
        "version": INPUTS_VERSION,
        "evidence": sorted([lot.lot_key, lot.fingerprint] for lot in inputs.lots),
        "subjects": sorted(
            [
                s.subject,
                s.outcome,
                s.instrument_id,
                s.execution_evidence,
                None if s.total_fees_unobserved_position_ids is None else sorted(s.total_fees_unobserved_position_ids),
            ]
            for s in inputs.subjects
        ),
        "instruments": [[p.instrument_id, p.symbol, p.canonical_instrument_id] for p in placements(inputs)],
    }
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

    securities: list[SecurityResult] = []
    for key in sorted(security_keys):
        reasons = own_reasons.get(key, [])
        result: Identified | StepFiveReached | None = None
        if not withheld_all and not reasons:
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
            securities.append(SecurityResult(key, security_keys[key], "computed", result.pool, result.disposals))
        else:
            securities.append(SecurityResult(key, security_keys[key], "withheld", None, ()))

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


def _read_inputs(conn: psycopg.Connection[Any]) -> MatchingInputs | None:
    """The effective generation and everything matching reads (Design 1); the caller holds one snapshot."""
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
            )
            for r in cur.execute(
                """
                SELECT subject, outcome, instrument_id, execution_evidence,
                       total_fees_unobserved_position_ids AS unobserved
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
                       execution_evidence, fingerprint
                FROM tax_evidence_lots WHERE run_id = %(run)s ORDER BY lot_key
                """,
                {"run": run["run_id"]},
            ).fetchall()
        )
        instruments = {
            r["instrument_id"]: InstrumentRow(r["instrument_id"], r["symbol"], r["canonical_instrument_id"])
            for r in cur.execute("SELECT instrument_id, symbol, canonical_instrument_id FROM instruments").fetchall()
        }
    return MatchingInputs(
        evidence_run_id=run["run_id"],
        evidence_policy_id=run["evidence_policy_id"],
        history_fetch_started_at=run["fetch_started_at"],
        subjects=subjects,
        lots=lots,
        instruments=instruments,
    )


def _existing_run(
    conn: psycopg.Connection[Any], evidence_run_id: int, digest: str
) -> tuple[int, str, str | None] | None:
    row = conn.execute(
        """
        SELECT matching_run_id, state, reason FROM tax_matching_runs
        WHERE evidence_run_id = %s AND matching_policy_id = %s AND class_policy_id = %s AND inputs_digest = %s
        """,
        (evidence_run_id, TAX_MATCHING_POLICY_ID, CONSIDERATION_CLASS_POLICY_ID, digest),
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
        inputs = _read_inputs(conn)
        if inputs is None:
            return None
        existing = _existing_run(conn, inputs.evidence_run_id, inputs_digest(inputs_document(inputs)))
    if existing is None or existing[1] != "published":
        return None
    return EffectiveMatching(existing[0], inputs.evidence_run_id)


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
            document = inputs_document(inputs)
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
            (evidence_run_id, matching_policy_id, class_policy_id, inputs_digest, inputs_document, state,
             computed_at, history_fetch_started_at, withheld_all)
        VALUES (%s, %s, %s, %s, %s, 'building', clock_timestamp(), %s, %s)
        RETURNING matching_run_id
        """,
        (
            inputs.evidence_run_id,
            TAX_MATCHING_POLICY_ID,
            CONSIDERATION_CLASS_POLICY_ID,
            digest,
            document,
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
            "pool_units, pool_cost_gbp, pool_missing_lot_keys, pool_reasons, window_open_since) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
