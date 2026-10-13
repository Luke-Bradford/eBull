"""Deriving the account's broker record into tax evidence, pure (#3616 slice 3c, Design 4 and 5).

Spec: docs/specs/metrics/2026-10-13-3616-slice3c-broker-tax-evidence.md. Nothing here reads the database or the
broker: ``app/services/tax_broker_evidence.py`` loads one snapshot and publishes what this module returns.

- ``partition`` splits the environment's events into subjects: one per order, plus ``position:<id>`` subjects for
  positions whose order cannot be established (Design 4, "Partition").
- ``derive_order`` runs the Design 4 checks over one order and returns its outcome, every failing reason and, when
  admitted, its lot drafts (Design 4.10).
- ``price_lots`` attaches the 3b sterling values (Design 5); ``lot_fingerprint`` is the Design 4.10 canonical hash.
- ``lookup_field_failures`` is the field check of one ``found`` body alone, which the candidate rules need.

Reason labels name the field as the source spells it in the spec's field table (``orderID`` on a sync payload,
``orderId`` on a history row or lookup); a lookup field below the root carries its path (``asset.settlementType``,
``openingData.fees``, ``positionExecutions.state``).

Every decimal arrives as ``int`` or ``Decimal``: stored payloads are read from their JSONB text and lookup bodies with
``parse_lookup_body``. A ``float`` means a loader decoded through binary doubles, which the spec forbids (Design 1,
"Precision"); it raises rather than being read.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, Inexact, Rounded, localcontext
from typing import Any, Literal, cast

from app.services.tax_fx import TaxFxState, resolve_tax_fx
from app.services.tax_ledger import _compute_tax_year, _to_uk_date
from app.services.trade_events import EventSource, canonical_decimal, classify_observation, normalise_payload

#: Names the checks, field table, product mapping, normalisation, tolerances, candidate rules and keys (Design 4.11).
#: Changing any of them is a new policy id.
TAX_EVIDENCE_POLICY_ID = "broker_evidence_v1"

#: The fingerprint's canonical form (Design 4.10).
CANONICAL_VERSION = 1

#: The plausibility tolerance (spec "Local policy"): |a - b| <= 0.001 * b, computed with no division.
TOLERANCE = Decimal("0.001")

BIGINT_MAX = 2**63 - 1

Product = Literal["real", "cfd", "real_futures", "margin_trade", "swap", "crypto_margin"]
SubjectOutcome = Literal["admitted", "excluded_non_share_product", "unresolved"]
ExecutionEvidence = Literal["lookup", "sync_only"]
Direction = Literal["acquisition", "disposal"]

#: The closed product mapping (Design 4.4). Lookup values are compared case-insensitively.
LOOKUP_PRODUCTS: Mapping[str, Product] = {
    "real": "real",
    "cfd": "cfd",
    "realfutures": "real_futures",
    "margintrade": "margin_trade",
}
SYNC_PRODUCTS: Mapping[int, Product] = {1: "real", 0: "cfd", 4: "real_futures", 2: "swap", 3: "crypto_margin"}

SYNC = "etoro_sync"
HISTORY = "etoro_history"


@contextmanager
def evidence_context() -> Iterator[None]:
    """3c's own arithmetic (Design 1, "Precision"): 60 digits, and a rounding raises instead of passing silently.

    ⚠ Never call 3b (``resolve_tax_fx``) inside it: 3b rounds its reciprocal rate by design, and a context copied
    from this one would trap that rounding.
    """
    with localcontext() as ctx:
        ctx.prec = 60
        ctx.traps[Inexact] = True
        ctx.traps[Rounded] = True
        yield


def parse_lookup_body(body_text: str) -> Any:
    """A lookup body with every decimal kept as the broker's text (Design 1). ``NaN`` and ``Infinity`` become
    non-finite ``Decimal`` values, which fail every numeric domain."""
    return json.loads(body_text, parse_float=Decimal, parse_constant=Decimal)


# --- inputs -----------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Scope:
    environment: str
    account_id: int


@dataclass(frozen=True)
class LedgerEvent:
    """One ``trade_events`` row of the run's environment."""

    event_id: int
    position_id: int
    etoro_instrument_id: int
    instrument_id: int | None
    event_kind: Literal["open", "close"]
    units: Decimal
    executed_at: datetime
    order_id: int | None
    source: str
    raw_payload: Mapping[str, Any]


@dataclass(frozen=True)
class RetainedObservation:
    """One ``trade_event_observations`` row (Design 2). ``event_id`` is None exactly when ``kind`` is ``unscoped``."""

    observation_id: int
    event_id: int | None
    position_id: int
    event_kind: Literal["open", "close"]
    executed_at: datetime | None
    kind: Literal["supplemental", "amendment", "lifecycle", "unscoped"]
    source: str
    observed_normalised: Mapping[str, Any]
    observed_raw: Mapping[str, Any]


@dataclass(frozen=True)
class FoundLookup:
    """One ``found`` attempt of ``tax_order_lookups``, its body parsed by ``parse_lookup_body``."""

    lookup_id: int
    requested_at: datetime
    policy_id: str
    body: Any


@dataclass(frozen=True)
class HeldPosition:
    """One real-id ``broker_positions`` row."""

    position_id: int
    units: Decimal
    amount: Decimal
    payload: Mapping[str, Any]


# --- outputs ----------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class LotDraft:
    """One admitted acquisition or disposal before its sterling value (Design 4.10)."""

    lot_key: str
    subject: str
    order_id: int
    instrument_id: int
    etoro_instrument_id: int
    direction: Direction
    position_ids: tuple[int, ...]
    trade_event_ids: tuple[int, ...]
    executed_at: datetime
    units: Decimal
    usd_amount: Decimal
    execution_evidence: ExecutionEvidence
    consideration_status: str = "broker_stated_unreconciled"

    @property
    def uk_date(self) -> date:
        return _to_uk_date(self.executed_at)

    @property
    def tax_year(self) -> str:
        return _compute_tax_year(self.uk_date)


@dataclass(frozen=True)
class SubjectEvidence:
    """One ``tax_evidence_subjects`` row, and the lot drafts of an admitted order."""

    subject: str
    order_id: int | None
    etoro_instrument_id: int | None
    instrument_id: int | None
    product: Product | None
    outcome: SubjectOutcome
    reasons: tuple[str, ...]
    trade_event_ids: tuple[int, ...]
    lookup_id: int | None
    held_positions: Mapping[int, HeldPosition | None]
    first_event_at: datetime | None
    last_event_at: datetime | None
    execution_evidence: ExecutionEvidence | None
    total_fees_unobserved_position_ids: tuple[int, ...]
    lots: tuple[LotDraft, ...] = ()


@dataclass(frozen=True)
class EvidenceLot:
    """One ``tax_evidence_lots`` row: a lot draft with the fields of 3b's consumer contract (Design 5)."""

    draft: LotDraft
    fx_rate_date: date
    fx_status: str
    fx_observation_date: date | None
    fx_usd_per_gbp: Decimal | None
    fx_gbp_per_usd: Decimal | None
    fx_policy_id: str
    fx_run_id: int | None
    amount_gbp: Decimal | None
    fingerprint: str


# --- field domains ----------------------------------------------------------------------------------------------

_INVALID = object()
_ABSENT = object()

Domain = Callable[[Any], Any]


def _number(value: Any) -> Decimal | None:
    if isinstance(value, float):
        raise TypeError("a payload number was decoded as a float; read JSON with parse_float=Decimal")
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        return None
    number = Decimal(value)
    return number if number.is_finite() else None


def _fits(number: Decimal, places: int, integer_digits: int) -> bool:
    """Within the column's range and with no more decimal places than it holds; never rounded (Design 1)."""
    # Decided on the digits alone: arithmetic on an extreme exponent (``1e1000000``) would overflow the context.
    if number.is_zero():
        return True
    if number.adjusted() >= integer_digits:
        return False
    digits, exponent = number.as_tuple().digits, int(number.as_tuple().exponent)
    dropped = -places - exponent
    return dropped <= 0 or (dropped < len(digits) and not any(digits[-dropped:]))


def _decimal_domain(places: int, integer_digits: int, sign: Literal["positive", "non_negative", "any"]) -> Domain:
    def check(value: Any) -> Any:
        number = _number(value)
        if number is None or not _fits(number, places, integer_digits):
            return _INVALID
        if (sign == "positive" and number <= 0) or (sign == "non_negative" and number < 0):
            return _INVALID
        return number

    return check


def _id(value: Any) -> Any:
    return value if type(value) is int and 0 < value <= BIGINT_MAX else _INVALID


def _positive_int(value: Any) -> Any:
    return value if type(value) is int and 0 < value <= BIGINT_MAX else _INVALID


def _non_negative_int(value: Any) -> Any:
    return value if type(value) is int and 0 <= value <= BIGINT_MAX else _INVALID


def _boolean(value: Any) -> Any:
    return value if isinstance(value, bool) else _INVALID


def _text(value: Any) -> Any:
    return value if isinstance(value, str) and value.strip() else _INVALID


def _timestamp(value: Any) -> Any:
    """ISO 8601 with an offset or ``Z``, normalised to UTC."""
    if not isinstance(value, str):
        return _INVALID
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return _INVALID
    return _INVALID if parsed.tzinfo is None else parsed.astimezone(UTC)


def _object(value: Any) -> Any:
    return value if isinstance(value, dict) else _INVALID


def _non_empty_array(value: Any) -> Any:
    return value if isinstance(value, list) and value else _INVALID


def _lookup_product(value: Any) -> Any:
    return LOOKUP_PRODUCTS.get(value.lower(), _INVALID) if isinstance(value, str) else _INVALID


def _sync_product(value: Any) -> Any:
    return SYNC_PRODUCTS.get(value, _INVALID) if type(value) is int else _INVALID


def _execution_state(value: Any) -> Any:
    return value.lower() if isinstance(value, str) and value.lower() in ("open", "closed") else _INVALID


def _non_negative_decimal(value: Any) -> Any:
    number = _number(value)
    return number if number is not None and number >= 0 else _INVALID


_MONEY = _decimal_domain(4, 16, "any")
_MONEY_POSITIVE = _decimal_domain(4, 16, "positive")
_MONEY_NON_NEGATIVE = _decimal_domain(4, 16, "non_negative")
_UNITS = _decimal_domain(8, 12, "positive")
_UNITS_OR_ZERO = _decimal_domain(8, 12, "non_negative")
_RATE = _decimal_domain(10, 10, "positive")


def _comparable(value: Any) -> Any:
    """One value in comparable form: numbers canonical (``1`` and ``1.0`` are one value), booleans never numbers."""
    if isinstance(value, bool) or value is None:
        return ("bool", value)
    if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
        number = Decimal(value)
        return ("number", canonical_decimal(number) if number.is_finite() else str(number))
    if isinstance(value, float):
        raise TypeError("a payload number was decoded as a float; read JSON with parse_float=Decimal")
    if isinstance(value, (dict, list)):
        return ("json", json.dumps(value, sort_keys=True, default=str))
    return ("text", value)


def _spellings(payload: Mapping[str, Any], key: str) -> list[Any]:
    """Every value under ``key``, matched case-insensitively (Design 4.3, fact 15)."""
    lowered = key.lower()
    return [v for k, v in payload.items() if isinstance(k, str) and k.lower() == lowered]


def _peek(payload: Any, key: str, domain: Domain) -> Any:
    """The field's value when present, unambiguous and in its domain; else None. Records nothing: the check that
    owns the field reports it."""
    if not isinstance(payload, Mapping):
        return None
    values = _spellings(payload, key)
    if not values or len({_comparable(v) for v in values}) > 1:
        return None
    parsed = domain(values[0])
    return None if parsed is _INVALID else parsed


class _Reasons:
    """Every failing check, once each, in the order the checks ran."""

    def __init__(self, initial: Iterable[str] = ()) -> None:
        self.items: list[str] = []
        for reason in initial:
            self.add(reason)

    def add(self, reason: str) -> None:
        if reason not in self.items:
            self.items.append(reason)

    def __bool__(self) -> bool:
        return bool(self.items)


def _require(payload: Any, key: str, label: str, domain: Domain, reasons: _Reasons) -> Any:
    """Check 3: present (absent is never zero), one value across spellings, in its domain. None on failure."""
    values = _spellings(payload, key) if isinstance(payload, Mapping) else []
    if not values:
        reasons.add(f"missing_field:{label}")
        return None
    if len({_comparable(v) for v in values}) > 1:
        reasons.add(f"field_conflict:{label}")
        return None
    parsed = domain(values[0])
    if parsed is _INVALID:
        reasons.add(f"invalid_field:{label}")
        return None
    return parsed


# --- the field table (Design 4) ---------------------------------------------------------------------------------

FieldSpec = tuple[str, Domain]

_SYNC_BIND: tuple[FieldSpec, ...] = (
    ("CID", _id),
    ("orderID", _id),
    ("positionID", _id),
    ("instrumentID", _id),
    ("settlementTypeID", _sync_product),
)
_HISTORY_BIND: tuple[FieldSpec, ...] = (("orderId", _id), ("positionId", _id), ("instrumentId", _id))
_HISTORY_CLOSE_BIND: tuple[FieldSpec, ...] = (("units", _UNITS), ("closeTimestamp", _timestamp))
_SYNC_REAL: tuple[FieldSpec, ...] = (
    ("isBuy", _boolean),
    ("isPartiallyAltered", _boolean),
    ("leverage", _positive_int),
    ("mirrorID", _non_negative_int),
    ("parentPositionID", _non_negative_int),
    ("initialUnits", _UNITS),
    ("initialAmountInDollars", _MONEY_POSITIVE),
    ("totalExternalFees", _MONEY_NON_NEGATIVE),
    ("totalExternalTaxes", _MONEY_NON_NEGATIVE),
    ("totalFees", _MONEY),
    ("openRate", _RATE),
    ("openConversionRate", _RATE),
    ("openDateTime", _timestamp),
)
_HISTORY_REAL: tuple[FieldSpec, ...] = (
    ("isBuy", _boolean),
    ("leverage", _positive_int),
    ("socialTradeId", _non_negative_int),
    ("parentPositionId", _non_negative_int),
    ("initialInvestment", _MONEY_POSITIVE),
    ("openRate", _RATE),
    ("openTimestamp", _timestamp),
)
_HISTORY_CLOSE_REAL: tuple[FieldSpec, ...] = (
    ("investment", _MONEY_POSITIVE),
    ("netProfit", _MONEY),
    ("fees", _MONEY_NON_NEGATIVE),
    ("closeRate", _RATE),
)
_LOOKUP_ROOT_REAL: tuple[FieldSpec, ...] = (
    ("transaction", _text),
    ("action", _text),
    ("orderCurrency", _text),
    ("totalCosts", _MONEY_NON_NEGATIVE),
)
_LOOKUP_ASSET_REAL: tuple[FieldSpec, ...] = (("side", _text), ("currency", _text), ("leverage", _positive_int))
_LOOKUP_EXECUTION_REAL: tuple[FieldSpec, ...] = (
    ("state", _execution_state),
    ("remainingUnits", _UNITS_OR_ZERO),
    ("addedFunds", _MONEY),
)
_LOOKUP_OPENING_REAL: tuple[FieldSpec, ...] = (
    ("units", _UNITS),
    ("avgPrice", _RATE),
    ("avgConversionRate", _RATE),
    ("fees", _MONEY_NON_NEGATIVE),
    ("taxes", _MONEY_NON_NEGATIVE),
    ("markup", _MONEY_NON_NEGATIVE),
    ("marketSpread", _non_negative_decimal),
    ("executionTime", _timestamp),
    ("openTime", _timestamp),
)
#: The openingData fields a single execution is identified by (Design 4.8).
_OPENING_IDENTITY = ("executionTime", "openTime", "units", "avgPrice", "avgConversionRate", "fees", "taxes", "orderId")


def _check_fields(payload: Any, specs: Sequence[FieldSpec], reasons: _Reasons, prefix: str = "") -> dict[str, Any]:
    return {key: _require(payload, key, prefix + key, domain, reasons) for key, domain in specs}


@dataclass
class _Lookup:
    """*F*'s fields as the checks read them; a value is None when its field check failed."""

    root: dict[str, Any] = field(default_factory=dict)
    asset: dict[str, Any] = field(default_factory=dict)
    status_name: str | None = None
    executions: list[tuple[dict[str, Any], dict[str, Any], Any]] = field(default_factory=list)


def _lookup_bind(body: Any, reasons: _Reasons) -> _Lookup:
    parsed = _Lookup()
    parsed.root = _check_fields(body, (("orderId", _id), ("accountId", _id)), reasons)
    asset = _require(body, "asset", "asset", _object, reasons)
    if asset is not None:
        parsed.asset = _check_fields(
            asset, (("instrumentId", _id), ("settlementType", _lookup_product)), reasons, "asset."
        )
    executions = _require(body, "positionExecutions", "positionExecutions", _non_empty_array, reasons)
    for execution in executions or []:
        if _object(execution) is _INVALID:
            reasons.add("invalid_field:positionExecutions")
            continue
        fields = _check_fields(execution, (("positionId", _id),), reasons, "positionExecutions.")
        opening = _require(execution, "openingData", "openingData", _object, reasons)
        opening_fields = (
            _check_fields(opening, (("orderId", _id),), reasons, "openingData.") if opening is not None else {}
        )
        parsed.executions.append((fields, opening_fields, opening))
    return parsed


def _lookup_real(body: Any, parsed: _Lookup, reasons: _Reasons) -> None:
    parsed.root.update(_check_fields(body, _LOOKUP_ROOT_REAL, reasons))
    status = _require(body, "status", "status", _object, reasons)
    if status is not None:
        parsed.status_name = _require(status, "name", "status.name", _text, reasons)
    asset = _peek(body, "asset", _object)
    if asset is not None:
        parsed.asset.update(_check_fields(asset, _LOOKUP_ASSET_REAL, reasons, "asset."))
    raw_executions = _peek(body, "positionExecutions", _non_empty_array) or []
    for (fields, opening_fields, opening), execution in zip(
        parsed.executions, [e for e in raw_executions if isinstance(e, dict)], strict=False
    ):
        fields.update(_check_fields(execution, _LOOKUP_EXECUTION_REAL, reasons, "positionExecutions."))
        if opening is not None:
            opening_fields.update(_check_fields(opening, _LOOKUP_OPENING_REAL, reasons, "openingData."))


def lookup_field_failures(body: Any) -> list[str]:
    """The Design 4 field checks of one ``found`` body alone: its *bind* fields, and its *real* fields when its own
    ``asset.settlementType`` maps to ``real``. The candidate rules re-ask a failing *F* after 7 days (Design 3)."""
    reasons = _Reasons()
    parsed = _lookup_bind(body, reasons)
    if parsed.asset.get("settlementType") == "real":
        _lookup_real(body, parsed, reasons)
    return reasons.items


# --- partition (Design 4) ---------------------------------------------------------------------------------------


def _order_ids(payload: Mapping[str, Any]) -> tuple[set[int], bool]:
    """The valid order ids under every spelling, and whether some spelling holds a value that is not an id."""
    values = _spellings(payload, "orderid")
    return {v for v in values if _id(v) is not _INVALID}, any(_id(v) is _INVALID for v in values)


@dataclass
class SubjectInput:
    """One subject's evidence, as ``partition`` assigns it."""

    subject: str
    order_id: int | None
    events: list[LedgerEvent] = field(default_factory=list)
    observations: list[RetainedObservation] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    orphan_held: list[HeldPosition] = field(default_factory=list)


def partition(
    events: Sequence[LedgerEvent],
    observations: Sequence[RetainedObservation],
    held: Mapping[int, HeldPosition],
) -> tuple[list[SubjectInput], list[RetainedObservation]]:
    """Every event in exactly one subject; returns the subjects and the unscoped observations attached to none.

    A position's order is the set of order ids its events and their observations name, from the column and every
    payload spelling: one id joins that order; none makes it ``position:<id>``; two or more make it ``position:<id>``
    with ``identity_conflict:order_id`` and quarantine every order it names. A held position with no event is an
    orphan. Unscoped observations attach to the subject holding their position, else to every order their payload
    names, else to nothing (Design 2).
    """
    events_by_position: dict[int, list[LedgerEvent]] = {}
    for event in events:
        events_by_position.setdefault(event.position_id, []).append(event)
    event_ids = {e.event_id: e.position_id for e in events}
    retained_by_position: dict[int, list[RetainedObservation]] = {}
    unscoped: list[RetainedObservation] = []
    for obs in observations:
        if obs.kind == "unscoped":
            unscoped.append(obs)
        elif obs.event_id in event_ids:
            retained_by_position.setdefault(event_ids[obs.event_id], []).append(obs)

    orders: dict[int, SubjectInput] = {}
    positions: dict[int, SubjectInput] = {}
    owner: dict[int, SubjectInput] = {}
    quarantined: list[set[int]] = []
    for position_id in sorted(events_by_position):
        position_events = events_by_position[position_id]
        retained = retained_by_position.get(position_id, [])
        ids: set[int] = set()
        invalid = False
        for event in position_events:
            found, bad = _order_ids(event.raw_payload)
            ids |= found
            invalid |= bad
            if event.order_id is not None:
                ids.add(event.order_id)
        for obs in retained:
            found, bad = _order_ids(obs.observed_raw)
            ids |= found
            invalid |= bad
        if len(ids) == 1:
            (order_id,) = ids
            subject = orders.setdefault(order_id, SubjectInput(subject=f"order:{order_id}", order_id=order_id))
        else:
            subject = SubjectInput(subject=f"position:{position_id}", order_id=None)
            positions[position_id] = subject
            if ids:
                subject.reasons.append("identity_conflict:order_id")
                quarantined.append(ids)
            else:
                subject.reasons.append("invalid_field:order_id" if invalid else "missing_field:order_id")
        subject.events.extend(position_events)
        subject.observations.extend(retained)
        owner[position_id] = subject

    for ids in quarantined:
        for order_id in sorted(ids):
            if order_id in orders:
                orders[order_id].reasons.append("position_quarantined")

    for position_id in sorted(held):
        if position_id in events_by_position:
            continue
        holding = held[position_id]
        named = [orders[o] for o in sorted(_order_ids(holding.payload)[0]) if o in orders]
        if named:
            for subject in named:
                subject.reasons.append("orphan_position")
                subject.orphan_held.append(holding)
        else:
            subject = SubjectInput(subject=f"position:{position_id}", order_id=None, reasons=["orphan_position"])
            subject.orphan_held.append(holding)
            positions[position_id] = subject

    unattached: list[RetainedObservation] = []
    for obs in unscoped:
        if obs.position_id in owner:
            owner[obs.position_id].observations.append(obs)
            continue
        named = [orders[o] for o in sorted(_order_ids(obs.observed_raw)[0]) if o in orders]
        for subject in named:
            subject.observations.append(obs)
        if not named:
            unattached.append(obs)

    subjects = [orders[o] for o in sorted(orders)] + [positions[p] for p in sorted(positions)]
    return subjects, unattached


# --- derivation (Design 4) --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Payload:
    event_id: int | None  # the stored row's id; None for an observation
    position_id: int
    source: str
    event_kind: str
    payload: Mapping[str, Any]
    observation_kind: str | None  # None for the stored row


def _canonical_ts(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _within_tolerance(a: Decimal, b: Decimal) -> bool:
    with evidence_context():
        return abs(a - b) <= TOLERANCE * b


_TIMESTAMP_KEYS = frozenset({"executiontime", "opentime"})


def _projected(container: Any, key: str) -> Any:
    """One field in comparable form: absent is a value of its own, timestamps are compared as UTC instants and
    numbers canonically, so a re-rendering of the same value is not a difference."""
    if not isinstance(container, Mapping):
        return ("absent",)
    values = _spellings(container, key)
    if not values:
        return ("absent",)
    if key.lower() in _TIMESTAMP_KEYS and len(values) == 1 and isinstance(moment := _timestamp(values[0]), datetime):
        return ("ts", _canonical_ts(moment))
    # Every text field projected is an enum, compared case-insensitively (Design 4.3).
    return tuple(sorted((_comparable(v.lower() if isinstance(v, str) else v) for v in values), key=repr))


def _lookup_projection(body: Any) -> tuple[tuple[Any, ...], Counter[str]]:
    """A ``found`` body on every lookup field of the field table except the lifecycle ones (an execution's ``state``
    and ``remainingUnits``, and the execution list, which may grow). Absent fields are part of the projection;
    fields outside the field table (``priceId``, ``contracts``, ...) are not."""
    status = _peek(body, "status", _object)
    asset = _peek(body, "asset", _object)
    root = (
        tuple(
            _projected(body, k)
            for k in ("orderId", "accountId", "transaction", "action", "orderCurrency", "totalCosts")
        )
        + (_projected(status, "name"),)
        + tuple(_projected(asset, k) for k in ("instrumentId", "settlementType", "side", "currency", "leverage"))
    )
    executions: Counter[str] = Counter()
    raw = _spellings(body, "positionExecutions") if isinstance(body, Mapping) else []
    for listing in raw:
        for execution in listing if isinstance(listing, list) else [listing]:
            opening = _peek(execution, "openingData", _object)
            projection = (
                _projected(execution, "positionId"),
                _projected(execution, "addedFunds"),
                opening is not None,
                *(_projected(opening, k) for k, _ in _LOOKUP_OPENING_REAL),
                _projected(opening, "orderId"),
            )
            executions[repr(projection)] += 1
    return root, executions


def _lookups_amended(lookups: Sequence[FoundLookup]) -> bool:
    """Any two ``found`` bodies differing outside the lifecycle fields, or a later body missing an execution an
    earlier one listed (Design 3, "Which body is used"). Sticky: every pair is compared, not just the latest two."""
    projections = [_lookup_projection(lookup.body) for lookup in lookups]
    for i, (root_i, executions_i) in enumerate(projections):
        for root_j, executions_j in projections[i + 1 :]:
            # Every execution of the earlier body, unchanged, in the later one: only a growing list passes.
            if root_i != root_j or executions_i - executions_j:
                return True
    return False


def _opening_identity(opening: Any) -> tuple[Any, ...]:
    return tuple(_projected(opening, key) for key in _OPENING_IDENTITY)


def derive_order(
    scope: Scope,
    order_id: int,
    events: Sequence[LedgerEvent],
    observations: Sequence[RetainedObservation],
    lookups: Sequence[FoundLookup],
    held: Mapping[int, HeldPosition | None],
    partition_reasons: Sequence[str] = (),
) -> SubjectEvidence:
    """Run the Design 4 checks over one order, recording every failing one in Design 4 order.

    ``observations`` are every retained observation of the order's events plus the ``unscoped`` ones attached to it;
    ``lookups`` its ``found`` attempts oldest first (*F* last); ``held`` the ``broker_positions`` row of each of its
    positions, or None. ``partition_reasons`` are the reasons the partition gave it (``position_quarantined``,
    ``orphan_position``). A check whose inputs an earlier failure left undefined is skipped, so the reasons name
    causes, not their consequences.
    """
    reasons = _Reasons(partition_reasons)
    subject = f"order:{order_id}"
    position_ids = sorted({e.position_id for e in events})
    retained = [o for o in observations if o.kind != "unscoped"]
    unscoped = [o for o in observations if o.kind == "unscoped"]
    f = lookups[-1] if lookups else None
    body = f.body if f is not None else None

    payloads = [_Payload(e.event_id, e.position_id, e.source, e.event_kind, e.raw_payload, None) for e in events]
    payloads += [_Payload(None, o.position_id, o.source, o.event_kind, o.observed_raw, o.kind) for o in retained]
    sync_payloads = [p for p in payloads if p.source == SYNC]
    history_payloads = [p for p in payloads if p.source == HISTORY]
    close_payloads = [p for p in history_payloads if p.event_kind == "close"]
    closes = sorted((e for e in events if e.event_kind == "close"), key=lambda e: (e.position_id, e.executed_at))
    opens = [e for e in events if e.event_kind == "open"]
    synced_positions = {p.position_id for p in sync_payloads}

    # Every event and observation of the order names it, in the column or a payload spelling (partition).
    for event in events:
        if event.order_id is None and not _order_ids(event.raw_payload)[0]:
            reasons.add("missing_field:order_id")
    for obs in retained:
        if not _order_ids(obs.observed_raw)[0]:
            reasons.add("missing_field:order_id")

    # 1. Identity.
    for event in events:
        if _peek(event.raw_payload, "positionid", _id) not in (None, event.position_id):
            reasons.add("identity_conflict:position_id")
        if _peek(event.raw_payload, "instrumentid", _id) not in (None, event.etoro_instrument_id):
            reasons.add("identity_conflict:instrument_id")
        if event.event_kind == "close":
            payload_units = _peek(event.raw_payload, "units", _UNITS)
            if payload_units is not None and payload_units != event.units:
                reasons.add("identity_conflict:units")
            payload_closed = _peek(event.raw_payload, "closeTimestamp", _timestamp)
            if payload_closed is not None and payload_closed != event.executed_at:
                reasons.add("identity_conflict:executed_at")
    events_by_id = {e.event_id: e for e in events}
    for obs in retained:
        if _peek(obs.observed_raw, "positionid", _id) not in (None, obs.position_id):
            reasons.add("identity_conflict:position_id")
        observed_event = events_by_id.get(obs.event_id) if obs.event_id is not None else None
        if observed_event is not None and _peek(obs.observed_raw, "instrumentid", _id) not in (
            None,
            observed_event.etoro_instrument_id,
        ):
            reasons.add("identity_conflict:instrument_id")
    broker_instruments = {e.etoro_instrument_id for e in events}
    if len(broker_instruments) > 1:
        reasons.add("instrument_conflict")
    etoro_instrument_id = next(iter(broker_instruments)) if len(broker_instruments) == 1 else None
    if {e.position_id for e in opens} != set(position_ids):
        reasons.add("open_event_missing")

    # 2. Account.
    accounts = {_peek(p.payload, "CID", _id) for p in sync_payloads}
    if body is not None:
        accounts.add(_peek(body, "accountId", _id))
    accounts |= {_peek(h.payload, "CID", _id) for h in held.values() if h is not None}
    accounts.discard(None)
    if not accounts:
        reasons.add("account_unestablished")
    elif accounts != {scope.account_id}:
        reasons.add("account_conflict")

    # 3. Fields (bind).
    for p in sync_payloads:
        _check_fields(p.payload, _SYNC_BIND, reasons)
    for p in history_payloads:
        _check_fields(p.payload, _HISTORY_BIND, reasons)
    for p in close_payloads:
        _check_fields(p.payload, _HISTORY_CLOSE_BIND, reasons)
    lookup = _lookup_bind(body, reasons) if body is not None else None
    for holding in held.values():
        if holding is not None:
            _require(holding.payload, "CID", "CID", _id, reasons)

    # 4. Product.
    products: set[Product] = {
        product for p in sync_payloads if (product := _peek(p.payload, "settlementTypeID", _sync_product)) is not None
    }
    if lookup is not None and lookup.asset.get("settlementType") is not None:
        products.add(lookup.asset["settlementType"])
    product: Product | None = None
    if len(products) > 1:
        reasons.add("settlement_conflict")
    elif f is None and synced_positions != set(position_ids):
        reasons.add("settlement_unestablished")
    elif products:
        (product,) = products

    # 5. Lookup binding.
    if lookup is not None and body is not None:
        opening_orders = [o.get("orderId") for _, o, _ in lookup.executions]
        if any(v != order_id for v in _order_ids(body)[0]) or any(
            v is not None and v != order_id for v in opening_orders
        ):
            reasons.add("lookup_mismatch:order_id")
        asset_instrument = lookup.asset.get("instrumentId")
        if asset_instrument is not None and etoro_instrument_id is not None and asset_instrument != etoro_instrument_id:
            reasons.add("lookup_mismatch:instrument_id")
        execution_positions = [fields.get("positionId") for fields, _, _ in lookup.executions]
        if None not in execution_positions and (
            len(set(execution_positions)) != len(execution_positions) or set(execution_positions) != set(position_ids)
        ):
            reasons.add("lookup_mismatch:positions")

    # 6. Amendments, and unscoped observations against their baselines (Design 2).
    if any(o.kind == "amendment" for o in retained):
        reasons.add("source_amended")
    if _lookups_amended(lookups):
        reasons.add("lookup_amended")
    for obs in unscoped:
        _check_unscoped(obs, events, retained, set(position_ids), reasons)

    first_event_at = min((e.executed_at for e in events), default=None)
    last_event_at = max((e.executed_at for e in events), default=None)
    unobserved = tuple(p for p in position_ids if p not in synced_positions)
    own_held = {p: held.get(p) for p in position_ids}
    # An orphan holding the partition attached to this order is recorded, never conserved (it has no event).
    held_positions = own_held | {p: h for p, h in held.items() if p not in own_held}
    app_instruments = {e.instrument_id for e in events}

    def result(
        outcome: SubjectOutcome, evidence: ExecutionEvidence | None = None, lots: tuple[LotDraft, ...] = ()
    ) -> SubjectEvidence:
        return SubjectEvidence(
            subject=subject,
            order_id=order_id,
            etoro_instrument_id=etoro_instrument_id,
            instrument_id=next(iter(app_instruments)) if len(app_instruments) == 1 else None,
            product=product,
            outcome=outcome,
            reasons=tuple(reasons.items) if outcome == "unresolved" else (),
            trade_event_ids=tuple(sorted(e.event_id for e in events)),
            lookup_id=f.lookup_id if f is not None else None,
            held_positions=held_positions,
            first_event_at=first_event_at,
            last_event_at=last_event_at,
            execution_evidence=evidence,
            total_fees_unobserved_position_ids=unobserved,
            lots=lots,
        )

    if product != "real":
        if not reasons and product is not None:
            return result("excluded_non_share_product")
        return result("unresolved")

    # Real-asset fields (the rows marked *real*), checked as in check 3.
    sync_values = [(p, _check_fields(p.payload, _SYNC_REAL, reasons)) for p in sync_payloads]
    history_values = [(p, _check_fields(p.payload, _HISTORY_REAL, reasons)) for p in history_payloads]
    close_values = [(p, _check_fields(p.payload, _HISTORY_CLOSE_REAL, reasons)) for p in close_payloads]
    if lookup is not None:
        _lookup_real(body, lookup, reasons)
    held_values: dict[int, tuple[Any, Any]] = {}
    for position_id, holding in own_held.items():
        if holding is not None:
            held_values[position_id] = (
                _require({"units": holding.units}, "units", "broker_positions.units", _UNITS_OR_ZERO, reasons),
                _require({"amount": holding.amount}, "amount", "broker_positions.amount", _MONEY_NON_NEGATIVE, reasons),
            )

    # 7. Shape: a filled, long, unleveraged, uncopied buy.
    if lookup is not None:
        expected = (
            ("status.name", lookup.status_name, "filled"),
            ("transaction", lookup.root.get("transaction"), "buy"),
            ("action", lookup.root.get("action"), "open"),
            ("asset.side", lookup.asset.get("side"), "long"),
            ("orderCurrency", lookup.root.get("orderCurrency"), "usd"),
        )
        for label, value, wanted in expected:
            if value is not None and value.lower() != wanted:
                reasons.add(f"unsupported_shape:{label}")
        if lookup.asset.get("leverage") not in (None, 1):
            reasons.add("unsupported_shape:asset.leverage")
    for _, values in sync_values:
        for label, wanted in (("isBuy", True), ("leverage", 1), ("mirrorID", 0), ("parentPositionID", 0)):
            if values[label] is not None and values[label] != wanted:
                reasons.add(f"unsupported_shape:{label}")
    for _, values in history_values:
        for label, wanted in (("isBuy", True), ("leverage", 1), ("socialTradeId", 0), ("parentPositionId", 0)):
            if values[label] is not None and values[label] != wanted:
                reasons.add(f"unsupported_shape:{label}")

    # 8. Executions.
    close_units: dict[int, Decimal] = {}
    with evidence_context():
        for close in closes:
            close_units[close.position_id] = close_units.get(close.position_id, Decimal(0)) + close.units
    opening: dict[str, Any] = {}
    if lookup is not None:
        identities = {_opening_identity(raw) for _, _, raw in lookup.executions}
        if len(identities) > 1:
            reasons.add("multiple_executions")
        elif lookup.executions:
            opening = lookup.executions[0][1]
        states = [fields.get("state") for fields, _, _ in lookup.executions]
        if len(states) > 1 and any(s not in (None, "closed") for s in states):
            reasons.add("unsupported_shape:partially_open_split")
        for fields, _, _ in lookup.executions:
            position_id, state, remaining = fields.get("positionId"), fields.get("state"), fields.get("remainingUnits")
            if position_id is None or state is None or remaining is None:
                continue
            holding = held.get(position_id)
            if state == "closed":
                bound = remaining > 0 and holding is None and close_units.get(position_id) == remaining
            else:
                bound = holding is not None and held_values.get(position_id, (None, None))[0] == remaining
            if not bound:
                reasons.add("piece_coverage_incomplete")
    else:
        # A failed isPartiallyAltered field check is its own reason; the remaining values decide.
        altered = [values["isPartiallyAltered"] for _, values in sync_values]
        if len(position_ids) != 1 or position_ids[0] not in synced_positions or True in altered:
            reasons.add("executions_unestablished")

    acquired_at: datetime | None = opening.get("executionTime")
    if lookup is None:
        # Every retained sync payload, stored or observed: a history-first open's sync evidence is an observation.
        sync_times = {values["openDateTime"] for _, values in sync_values} - {None}
        acquired_at = next(iter(sync_times)) if len(sync_times) == 1 else None
        if len(sync_times) > 1:
            reasons.add("acquisition_date_conflict")
    candidates = [opening.get("executionTime"), opening.get("openTime")]
    if lookup is not None:
        for _, opening_fields, _ in lookup.executions:
            candidates += [opening_fields.get("executionTime"), opening_fields.get("openTime")]
    candidates += [values["openDateTime"] for _, values in sync_values]
    candidates += [values["openTimestamp"] for _, values in history_values]
    if len({_to_uk_date(c) for c in candidates if c is not None}) > 1:
        reasons.add("acquisition_date_conflict")
    if acquired_at is not None and any(c.executed_at < acquired_at for c in closes):
        reasons.add("close_before_open")

    # 9. Quantities and money.
    indicators: list[tuple[str, Any]] = []
    if lookup is not None:
        indicators.append(("totalCosts", lookup.root.get("totalCosts")))
        for fields, opening_fields, _ in lookup.executions:
            indicators.append(("positionExecutions.addedFunds", fields.get("addedFunds")))
            for key in ("fees", "taxes", "markup"):
                indicators.append((f"openingData.{key}", opening_fields.get(key)))
    for _, values in sync_values:
        indicators += [(k, values[k]) for k in ("totalExternalFees", "totalExternalTaxes", "totalFees")]
    indicators += [("fees", values["fees"]) for _, values in close_values]
    for label, value in indicators:
        if value is not None and value != 0:
            reasons.add(f"charges_nonzero:{label}")

    sync_units = {values["initialUnits"] for _, values in sync_values if values["initialUnits"] is not None}
    execution_units: Decimal | None = opening.get("units") if lookup is not None else None
    if lookup is not None:
        if execution_units is not None and sync_units - {execution_units}:
            reasons.add("opening_units_conflict")
    elif len(sync_units) > 1:
        reasons.add("opening_units_conflict")
    elif sync_units:
        (execution_units,) = sync_units

    open_rates = {v["openRate"] for _, v in sync_values + history_values if v["openRate"] is not None}
    conversions = {v["openConversionRate"] for _, v in sync_values if v["openConversionRate"] is not None}
    rate: Decimal | None = None
    conversion: Decimal | None = None
    if lookup is not None:
        rate, conversion = opening.get("avgPrice"), opening.get("avgConversionRate")
        if (rate is not None and open_rates - {rate}) or (conversion is not None and conversions - {conversion}):
            reasons.add("opening_rate_conflict")
    elif len(open_rates) > 1 or len(conversions) > 1:
        reasons.add("opening_rate_conflict")
    else:
        rate = next(iter(open_rates), None)
        conversion = next(iter(conversions), None)

    held_units = [held_values[p][0] for p in held_values]
    held_amounts = [held_values[p][1] for p in held_values]
    stored_close_values = {p.event_id: values for p, values in close_values if p.observation_kind is None}
    with evidence_context():
        if execution_units is not None and None not in held_units:
            if execution_units != sum(close_units.values(), Decimal(0)) + sum(held_units, Decimal(0)):
                reasons.add("units_not_conserved")

        investments = {v["initialAmountInDollars"] for _, v in sync_values} | {
            v["initialInvestment"] for _, v in history_values
        }
        consideration: Decimal | None = None
        if len(investments - {None}) > 1:
            reasons.add("consideration_conflict")
        elif investments - {None}:
            (consideration,) = investments - {None}

        close_investments = [v["investment"] for v in stored_close_values.values()]
        if consideration is not None and None not in close_investments and None not in held_amounts:
            if sum(close_investments, Decimal(0)) + sum(held_amounts, Decimal(0)) != consideration:
                reasons.add("investment_not_conserved")

        if None not in (execution_units, rate, conversion, consideration):
            assert execution_units is not None and rate is not None and conversion is not None
            assert consideration is not None
            if not _within_tolerance(execution_units * rate * conversion, consideration):
                reasons.add("consideration_implausible")

    currency = lookup.asset.get("currency") if lookup is not None else None
    usd_priced = currency is not None and currency.upper() == "USD"
    if closes:
        if lookup is None:
            reasons.add("pricing_currency_unestablished")
        elif currency is not None and not usd_priced:
            reasons.add("close_conversion_unestablished")

    proceeds: dict[int, Decimal] = {}
    for close in closes:
        values = stored_close_values.get(close.event_id)
        if values is None or values["investment"] is None or values["netProfit"] is None:
            continue
        with evidence_context():
            amount = values["investment"] + values["netProfit"]
        if not _fits(amount, 4, 16):
            reasons.add("invalid_field:proceeds")
            continue
        proceeds[close.event_id] = amount
        # ``closeRate`` is in the pricing currency: only a USD-priced piece compares with USD proceeds.
        if usd_priced and values["closeRate"] is not None:
            with evidence_context():
                implied = close.units * values["closeRate"]
            if not _within_tolerance(implied, amount):
                reasons.add("proceeds_implausible")

    if len(app_instruments) != 1 or None in app_instruments:
        reasons.add("instrument_unresolved")

    if not reasons and (acquired_at is None or execution_units is None or consideration is None):
        # Unreachable while every check above holds; a defect here refuses the order rather than aborting the run.
        reasons.add("executions_unestablished")
    if reasons:
        return result("unresolved")

    assert acquired_at is not None and execution_units is not None and consideration is not None
    assert etoro_instrument_id is not None
    instrument_id = next(iter(app_instruments))
    assert instrument_id is not None
    evidence: ExecutionEvidence = "lookup" if f is not None else "sync_only"
    lots = [
        LotDraft(
            lot_key=f"acq:{scope.environment}:{order_id}",
            subject=subject,
            order_id=order_id,
            instrument_id=instrument_id,
            etoro_instrument_id=etoro_instrument_id,
            direction="acquisition",
            position_ids=tuple(position_ids),
            trade_event_ids=tuple(sorted(e.event_id for e in opens)),
            executed_at=acquired_at,
            units=execution_units,
            usd_amount=consideration,
            execution_evidence=evidence,
        )
    ]
    for close in closes:
        lots.append(
            LotDraft(
                lot_key=f"dis:{scope.environment}:{close.position_id}:{_canonical_ts(close.executed_at)}",
                subject=subject,
                order_id=order_id,
                instrument_id=instrument_id,
                etoro_instrument_id=etoro_instrument_id,
                direction="disposal",
                position_ids=(close.position_id,),
                trade_event_ids=(close.event_id,),
                executed_at=close.executed_at,
                units=close.units,
                usd_amount=proceeds[close.event_id],
                execution_evidence=evidence,
            )
        )
    return result("admitted", evidence, tuple(lots))


def _check_unscoped(
    obs: RetainedObservation,
    events: Sequence[LedgerEvent],
    retained: Sequence[RetainedObservation],
    position_ids: set[int],
    reasons: _Reasons,
) -> None:
    """An unscoped observation against the latest retained payload of its event and source (Design 2).

    Attached by order id alone (its position is not the order's), or with no event or no same-source payload to
    compare against: ``unscoped_unmatched``. Different by the amendment rule: ``unscoped_conflict``.
    """
    if obs.position_id not in position_ids:
        reasons.add("unscoped_unmatched")
        return
    event = next(
        (
            e
            for e in events
            if e.position_id == obs.position_id
            and e.event_kind == obs.event_kind
            and (obs.event_kind == "open" or e.executed_at == obs.executed_at)
        ),
        None,
    )
    if event is None:
        reasons.add("unscoped_unmatched")
        return
    same_source = sorted(
        (o for o in retained if o.event_id == event.event_id and o.source == obs.source),
        key=lambda o: o.observation_id,
    )
    if same_source:
        baseline: Mapping[str, Any] = same_source[-1].observed_normalised
    elif event.source == obs.source:
        baseline = normalise_payload(cast(EventSource, event.source), event.event_kind, dict(event.raw_payload))
    else:
        reasons.add("unscoped_unmatched")
        return
    if classify_observation(dict(baseline), dict(obs.observed_normalised)) == "amendment":
        reasons.add("unscoped_conflict")


def derive_position(subject: SubjectInput, held: Mapping[int, HeldPosition]) -> SubjectEvidence:
    """A position whose order cannot be established: always ``unresolved``, with the partition's reasons."""
    position_ids = sorted({e.position_id for e in subject.events} | {h.position_id for h in subject.orphan_held})
    instruments = {e.etoro_instrument_id for e in subject.events}
    app_instruments = {e.instrument_id for e in subject.events}
    event_positions = sorted({e.position_id for e in subject.events})
    synced = {e.position_id for e in subject.events if e.source == SYNC} | {
        o.position_id for o in subject.observations if o.source == SYNC and o.kind != "unscoped"
    }
    return SubjectEvidence(
        subject=subject.subject,
        order_id=None,
        etoro_instrument_id=next(iter(instruments)) if len(instruments) == 1 else None,
        instrument_id=next(iter(app_instruments)) if len(app_instruments) == 1 else None,
        product=None,
        outcome="unresolved",
        reasons=tuple(_Reasons(subject.reasons).items),
        trade_event_ids=tuple(sorted(e.event_id for e in subject.events)),
        lookup_id=None,
        held_positions={p: held.get(p) for p in position_ids},
        first_event_at=min((e.executed_at for e in subject.events), default=None),
        last_event_at=max((e.executed_at for e in subject.events), default=None),
        execution_evidence=None,
        total_fees_unobserved_position_ids=tuple(p for p in event_positions if p not in synced),
    )


def derive_all(
    scope: Scope,
    events: Sequence[LedgerEvent],
    observations: Sequence[RetainedObservation],
    lookups: Mapping[int, Sequence[FoundLookup]],
    held: Mapping[int, HeldPosition],
) -> list[SubjectEvidence]:
    """Every subject of the snapshot: the partition, then ``derive_order`` per order and ``derive_position`` per
    position subject. ``lookups`` maps an order id to its ``found`` attempts, oldest first."""
    subjects, _unattached = partition(events, observations, held)
    derived: list[SubjectEvidence] = []
    for subject in subjects:
        if subject.order_id is None:
            derived.append(derive_position(subject, held))
            continue
        positions = {e.position_id for e in subject.events}
        derived.append(
            derive_order(
                scope,
                subject.order_id,
                subject.events,
                subject.observations,
                sorted(lookups.get(subject.order_id, ()), key=lambda lookup: lookup.lookup_id),
                {p: held.get(p) for p in positions} | {h.position_id: h for h in subject.orphan_held},
                partition_reasons=subject.reasons,
            )
        )
    return derived


# --- sterling and fingerprints (Design 5, 4.10) -----------------------------------------------------------------


@dataclass(frozen=True)
class FxRefusal:
    """A lot whose date the 3b state cannot answer: the run is refused and publishes nothing (Design 5)."""

    reason: str
    lot_key: str


def price_lots(
    drafts: Sequence[LotDraft],
    state: TaxFxState,
    evidence_policy_id: str = TAX_EVIDENCE_POLICY_ID,
) -> list[EvidenceLot] | FxRefusal:
    """Each lot's 3b rate for its UK date, and ``amount_gbp`` as the exact product ``usd_amount × fx_gbp_per_usd``.

    ``pending``: stored with ``amount_gbp`` NULL; ``unavailable`` or ``out_of_range`` for any lot: an ``FxRefusal``.
    ``resolve_tax_fx`` runs outside the evidence context, because 3b rounds its reciprocal rate by design.
    """
    lots: list[EvidenceLot] = []
    for draft in drafts:
        rate = resolve_tax_fx(state, draft.uk_date)
        if rate.status in ("unavailable", "out_of_range"):
            return FxRefusal(reason=f"fx_{rate.status}", lot_key=draft.lot_key)
        amount_gbp: Decimal | None = None
        if rate.status != "pending":
            if rate.gbp_per_usd is None:
                raise RuntimeError(f"3b answered {rate.status} for {draft.uk_date} with no rate")
            with evidence_context():
                amount_gbp = draft.usd_amount * rate.gbp_per_usd
        lot = EvidenceLot(
            draft=draft,
            fx_rate_date=rate.rate_date,
            fx_status=rate.status,
            fx_observation_date=rate.observation_date,
            fx_usd_per_gbp=rate.usd_per_gbp,
            fx_gbp_per_usd=rate.gbp_per_usd,
            fx_policy_id=rate.policy_id,
            fx_run_id=rate.run_id,
            amount_gbp=amount_gbp,
            fingerprint="",
        )
        lots.append(replace(lot, fingerprint=lot_fingerprint(lot, evidence_policy_id)))
    return lots


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        with evidence_context():
            return canonical_decimal(value)
    if isinstance(value, datetime):
        return _canonical_ts(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (tuple, list)):
        return sorted(_canonical_value(v) for v in value)
    raise TypeError(f"no canonical form for {type(value).__name__}")


def fingerprint_document(lot: EvidenceLot, evidence_policy_id: str = TAX_EVIDENCE_POLICY_ID) -> dict[str, Any]:
    """Exactly the Design 4.10 keys; never ``run_id``, ``fx_run_id`` or the fingerprint itself."""
    d = lot.draft
    document = {
        "canonical_version": CANONICAL_VERSION,
        "evidence_policy_id": evidence_policy_id,
        "lot_key": d.lot_key,
        "subject": d.subject,
        "order_id": d.order_id,
        "instrument_id": d.instrument_id,
        "etoro_instrument_id": d.etoro_instrument_id,
        "direction": d.direction,
        "position_ids": d.position_ids,
        "trade_event_ids": d.trade_event_ids,
        "executed_at": d.executed_at,
        "uk_date": d.uk_date,
        "tax_year": d.tax_year,
        "units": d.units,
        "usd_amount": d.usd_amount,
        "consideration_status": d.consideration_status,
        "execution_evidence": d.execution_evidence,
        "fx_rate_date": lot.fx_rate_date,
        "fx_status": lot.fx_status,
        "fx_observation_date": lot.fx_observation_date,
        "fx_usd_per_gbp": lot.fx_usd_per_gbp,
        "fx_gbp_per_usd": lot.fx_gbp_per_usd,
        "fx_policy_id": lot.fx_policy_id,
        "amount_gbp": lot.amount_gbp,
    }
    return {key: _canonical_value(value) for key, value in document.items()}


def fingerprint_bytes(lot: EvidenceLot, evidence_policy_id: str = TAX_EVIDENCE_POLICY_ID) -> bytes:
    return json.dumps(
        fingerprint_document(lot, evidence_policy_id), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def lot_fingerprint(lot: EvidenceLot, evidence_policy_id: str = TAX_EVIDENCE_POLICY_ID) -> str:
    return hashlib.sha256(fingerprint_bytes(lot, evidence_policy_id)).hexdigest()
