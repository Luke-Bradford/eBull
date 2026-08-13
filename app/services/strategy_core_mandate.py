"""The core/cash mandate authority (#2603 item 1).

Hold a benchmark instrument and cash to operator-declared weights, rebalance only
outside a declared band.  Event-shaped like the other capital authorities in
``strategy_control_plane``: one append-only row per operator change.

⚠ This module AUTHORISES NOTHING.  Nothing reads ``strategy_core_mandate_events``
yet -- the allocator (#2603 item 3) is its first consumer, the eligibility proof
(item 2) owns its own evidence shape, and no endpoint is wired.  It is state, not
a gate, and must not be cited as one until something calls it.

``CoreMandateError`` is deliberately standalone rather than a
``StrategyControlError`` subclass: no endpoint maps this path yet, so inheriting
would buy coupling to a 1000-line module for an integration that does not exist.

Spec: ``docs/proposals/ta/2026-08-13-core-cash-mandate.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any

import psycopg

CORE_MANDATE_POLICY_VERSION = "core-mandate-v1"

# Locked to USD by this module's own schema CHECK (sql/336:26), and deliberately NOT
# bound to `strategy_base_currency.DEPLOYMENT_CURRENCY` (#2603 item 4): the core mandate
# and the paper deployment are separate capital authorities behind separate constraints,
# so aliasing would let one widen and silently drive the other past its own CHECK.
# Lifting THIS one lifts sql/336:26 with it. The deployment side's coordinated site list
# lives on `SUPPORTED_DEPLOYMENT_CURRENCIES`; the pool's is sql/290:96 +
# strategy_control_plane.py:313. Three locks, three lists, none of them one edit.
CORE_MANDATE_BASE_CURRENCY = "USD"

# (ticket, sub) per strategy_control_plane's PAPER_ALLOCATOR_ADVISORY_LOCK.
CORE_MANDATE_ADVISORY_LOCK = (2603, 1)

_HUNDRED = Decimal("100")
# NUMERIC(8,4) percentages and NUMERIC(18,6) amounts, matching sql/311.  Both
# halves of each column type are enforced: scale, so a value cannot be silently
# rounded, and precision, so an oversized one raises CoreMandateError instead of
# Postgres' NumericValueOutOfRange.
_PCT_PLACES = 4
_PCT_PRECISION = 8
_AMOUNT_PLACES = 6
_AMOUNT_PRECISION = 18


class CoreMandateError(ValueError):
    """A rejected core/cash mandate revision."""


@dataclass(frozen=True)
class CoreMandate:
    """One stored core/cash mandate revision.

    Every percentage is a share of a single core-sleeve denominator.  What that
    denominator is -- the whole account, or a carve-out of the paper pool -- is
    #2525's question; each invariant here is scale-free in it.
    """

    event_id: int
    revision: int
    enabled: bool
    base_currency: str
    core_instrument_id: int | None
    core_target_pct: Decimal
    liquidity_reserve_pct: Decimal
    rebalance_band_pct: Decimal
    min_rebalance_amount: Decimal
    policy_version: str

    @property
    def cash_target_pct(self) -> Decimal:
        """Cash is the complement, never a stored second column."""
        return _HUNDRED - self.core_target_pct


@dataclass(frozen=True)
class CoreMandateValues:
    """The operator-supplied half of a revision, validated and normalised."""

    enabled: bool
    base_currency: str
    core_instrument_id: int | None
    core_target_pct: Decimal
    liquidity_reserve_pct: Decimal
    rebalance_band_pct: Decimal
    min_rebalance_amount: Decimal


def _require_text(value: str, field: str, limit: int) -> None:
    if not value or not value.strip():
        raise CoreMandateError(f"{field} is required")
    if len(value) > limit:
        raise CoreMandateError(f"{field} exceeds {limit} characters")


def _require_finite(value: Decimal, field: str) -> None:
    """Reject NaN/Infinity before anything compares the value.

    Ordering constraint, not style: ``Decimal("NaN") < 0`` raises
    ``InvalidOperation`` rather than returning False, so every range check below
    is only safe once this has run over all four fields.
    """
    if not value.is_finite():
        raise CoreMandateError(f"{field} must be a finite decimal")


def _require_storable(value: Decimal, field: str, places: int, precision: int) -> None:
    """Reject a value the column cannot hold exactly, in either direction.

    Scale: Postgres rounds silently to the column scale, which would store a
    mandate the operator did not ask for.  Refusing is the only way the stored row
    and the request stay the same number.  The test is representability, not the
    exponent: ``Decimal("60.00000")`` is exact at four places and must pass.

    Precision: ``NUMERIC(p,s)`` holds ``p - s`` integer digits, and exceeding that
    raises ``NumericValueOutOfRange`` from the driver.  Bounding it here is what
    keeps every rejection a named ``CoreMandateError``.

    ⚠ Assumes ``_require_finite`` has already run: ``quantize`` on a NaN raises
    ``InvalidOperation``, which this would report as "too large to store".

    For the percentages the precision half is unreachable through
    ``validate_core_mandate``, whose range checks bound them to [0,100] first.  It
    stays because this helper is generic over ``NUMERIC(p,s)`` and coupling it to
    one caller's ranges would be the worse trade -- a backstop that costs a
    comparison.
    """
    try:
        rounded = value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_DOWN)
    except InvalidOperation as exc:
        raise CoreMandateError(f"{field} is too large to store") from exc
    if value != rounded:
        raise CoreMandateError(f"{field} carries more precision than {places} decimal places")
    if abs(value) >= Decimal(1).scaleb(precision - places):
        raise CoreMandateError(f"{field} exceeds {precision - places} integer digits")


def validate_core_mandate(
    *,
    enabled: bool,
    base_currency: str,
    core_instrument_id: int | None,
    core_target_pct: Decimal,
    liquidity_reserve_pct: Decimal,
    rebalance_band_pct: Decimal,
    min_rebalance_amount: Decimal,
) -> CoreMandateValues:
    """Validate one proposed revision, returning it normalised.

    Pure: no connection, no clock.  The DB CHECKs in sql/336 are the backstop for
    every rule here; this exists so an operator gets a named error instead of a
    raw constraint violation, and so the rules are table-testable without a DB.
    """
    normalised_currency = base_currency.strip().upper()
    if normalised_currency != CORE_MANDATE_BASE_CURRENCY:
        # #2603 item 4: the deferral is explicit and total, not partial.
        raise CoreMandateError(
            f"core mandate base currency must be {CORE_MANDATE_BASE_CURRENCY}; "
            "non-USD support lifts six sites in one change"
        )

    percentages = (
        ("core_target_pct", core_target_pct),
        ("liquidity_reserve_pct", liquidity_reserve_pct),
        ("rebalance_band_pct", rebalance_band_pct),
    )
    # Finiteness first over every field, because the range checks below cannot
    # compare a NaN; then ranges, so an out-of-range percentage reports its range
    # rather than its digit count; then storability.
    for field, value in percentages:
        _require_finite(value, field)
    _require_finite(min_rebalance_amount, "min_rebalance_amount")

    if core_target_pct < 0 or core_target_pct > _HUNDRED:
        raise CoreMandateError("core_target_pct must be between 0 and 100")
    if liquidity_reserve_pct < 0 or liquidity_reserve_pct >= _HUNDRED:
        raise CoreMandateError("liquidity_reserve_pct must be at least 0 and below 100")
    if rebalance_band_pct <= 0 or rebalance_band_pct > _HUNDRED:
        # Zero authorises a rebalance on any drift at all, and turnover is the
        # first-order cost filter.
        raise CoreMandateError("rebalance_band_pct must be above 0 and at most 100")
    if min_rebalance_amount <= 0:
        raise CoreMandateError("min_rebalance_amount must be above 0")

    for field, value in percentages:
        _require_storable(value, field, _PCT_PLACES, _PCT_PRECISION)
    _require_storable(min_rebalance_amount, "min_rebalance_amount", _AMOUNT_PLACES, _AMOUNT_PRECISION)

    if enabled and core_instrument_id is None:
        raise CoreMandateError("an enabled core mandate requires a core instrument")
    if core_instrument_id is not None and core_instrument_id <= 0:
        raise CoreMandateError("core_instrument_id must be a positive instrument id")

    if core_target_pct - rebalance_band_pct < 0:
        raise CoreMandateError("rebalance_band_pct wider than core_target_pct leaves the lower trigger unreachable")
    if _HUNDRED - (core_target_pct + rebalance_band_pct) < liquidity_reserve_pct:
        raise CoreMandateError("rebalance_band_pct would authorise drifting through liquidity_reserve_pct")

    return CoreMandateValues(
        enabled=enabled,
        base_currency=normalised_currency,
        core_instrument_id=core_instrument_id,
        core_target_pct=core_target_pct,
        liquidity_reserve_pct=liquidity_reserve_pct,
        rebalance_band_pct=rebalance_band_pct,
        min_rebalance_amount=min_rebalance_amount,
    )


def load_core_mandate(conn: psycopg.Connection[Any]) -> CoreMandate | None:
    """The latest revision, or None when no mandate has ever been configured.

    None is a state, not a default: there is no implied core allocation.
    """
    row = conn.execute(
        """
        SELECT core_mandate_event_id,revision,enabled,base_currency,core_instrument_id,
               core_target_pct,liquidity_reserve_pct,rebalance_band_pct,
               min_rebalance_amount,policy_version
        FROM strategy_core_mandate_events
        ORDER BY revision DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return CoreMandate(
        event_id=int(row[0]),
        revision=int(row[1]),
        enabled=bool(row[2]),
        base_currency=str(row[3]),
        core_instrument_id=None if row[4] is None else int(row[4]),
        core_target_pct=Decimal(str(row[5])),
        liquidity_reserve_pct=Decimal(str(row[6])),
        rebalance_band_pct=Decimal(str(row[7])),
        min_rebalance_amount=Decimal(str(row[8])),
        policy_version=str(row[9]),
    )


def _is_material_change(current: CoreMandate, values: CoreMandateValues) -> bool:
    return (
        current.enabled != values.enabled
        or current.base_currency != values.base_currency
        or current.core_instrument_id != values.core_instrument_id
        or current.core_target_pct != values.core_target_pct
        or current.liquidity_reserve_pct != values.liquidity_reserve_pct
        or current.rebalance_band_pct != values.rebalance_band_pct
        or current.min_rebalance_amount != values.min_rebalance_amount
    )


def configure_core_mandate(
    conn: psycopg.Connection[Any],
    *,
    enabled: bool,
    core_instrument_id: int | None,
    core_target_pct: Decimal,
    liquidity_reserve_pct: Decimal,
    rebalance_band_pct: Decimal,
    min_rebalance_amount: Decimal,
    changed_by: str,
    reason: str,
    base_currency: str = CORE_MANDATE_BASE_CURRENCY,
) -> CoreMandate:
    """Append one material core/cash mandate revision.

    No parameter can record an eligibility proof: item 2 owns that evidence shape
    and gets its own table, so a mandate cannot claim proof it does not have.
    """
    _require_text(changed_by, "changed_by", 200)
    _require_text(reason, "reason", 1000)
    values = validate_core_mandate(
        enabled=enabled,
        base_currency=base_currency,
        core_instrument_id=core_instrument_id,
        core_target_pct=core_target_pct,
        liquidity_reserve_pct=liquidity_reserve_pct,
        rebalance_band_pct=rebalance_band_pct,
        min_rebalance_amount=min_rebalance_amount,
    )
    # Serialise revision allocation; the UNIQUE on revision is the backstop, not
    # the mechanism.
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", CORE_MANDATE_ADVISORY_LOCK)
    current = load_core_mandate(conn)
    if current is not None and not _is_material_change(current, values):
        raise CoreMandateError("core mandate change must alter at least one mandate value")
    revision = 1 if current is None else current.revision + 1
    row = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING core_mandate_event_id
        """,
        (
            revision,
            values.enabled,
            values.base_currency,
            values.core_instrument_id,
            values.core_target_pct,
            values.liquidity_reserve_pct,
            values.rebalance_band_pct,
            values.min_rebalance_amount,
            CORE_MANDATE_POLICY_VERSION,
            changed_by,
            reason,
        ),
    ).fetchone()
    assert row is not None
    return CoreMandate(
        event_id=int(row[0]),
        revision=revision,
        enabled=values.enabled,
        base_currency=values.base_currency,
        core_instrument_id=values.core_instrument_id,
        core_target_pct=values.core_target_pct,
        liquidity_reserve_pct=values.liquidity_reserve_pct,
        rebalance_band_pct=values.rebalance_band_pct,
        min_rebalance_amount=values.min_rebalance_amount,
        policy_version=CORE_MANDATE_POLICY_VERSION,
    )


__all__ = [
    "CORE_MANDATE_ADVISORY_LOCK",
    "CORE_MANDATE_BASE_CURRENCY",
    "CORE_MANDATE_POLICY_VERSION",
    "CoreMandate",
    "CoreMandateError",
    "CoreMandateValues",
    "configure_core_mandate",
    "load_core_mandate",
    "validate_core_mandate",
]
