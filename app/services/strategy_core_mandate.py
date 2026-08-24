"""The core/cash mandate authority (#2603 item 1).

Hold a benchmark instrument and cash to operator-declared weights, rebalance only
outside a declared band.  Event-shaped like the other capital authorities in
``strategy_control_plane``: one append-only row per operator change.

⚠ This module AUTHORISES NOTHING.  Nothing reads ``strategy_core_mandate_events``
to act -- the allocator (#2603 item 3) is its first such consumer and no endpoint
is wired.  It is state, not a gate, and must not be cited as one.

What it now DOES do is refuse to record an unproved authority: as of item 2,
``configure_core_mandate`` requires a fresh account-specific proof that an
enabled mandate's core instrument is the underlying product and not a CFD.  That
is a constraint on WRITES, not a control on trading -- see its docstring.

``CoreMandateError`` is deliberately standalone rather than a
``StrategyControlError`` subclass: no endpoint maps this path yet, so inheriting
would buy coupling to a 1000-line module for an integration that does not exist.

Spec: ``docs/proposals/ta/2026-08-13-core-cash-mandate.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg

from app.services.strategy_control_plane import PAPER_ALLOCATOR_ADVISORY_LOCK
from app.services.strategy_core_eligibility import require_core_eligibility
from app.services.strategy_core_selection import require_selected_core_instrument
from app.services.strategy_engine_capital import EngineCapitalObservationError, load_engine_capital_authority

# v2 as of #2670, which made both band triggers REACHABLE rather than merely in
# range.  Bumped even though the table held 0 rows: a version denotes a rule set,
# not a row population, and `CoreMandate` is publicly constructible, so a v1
# mandate can exist without ever having been stored and changes validity across
# that tightening.  Leaving the stamp would make one string mean two arithmetics.
CORE_MANDATE_POLICY_VERSION = "core-mandate-v2"

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

# The core arm's identity in operator-facing series that key on `strategy_id`
# (#2603 item 3, step 2).  A core position has no strategy, but the P&L history
# and the owned-positions list both group by one, so the arm needs a stable key
# rather than each endpoint inventing a literal.
#
# ⚠ Deliberately NOT a value that could collide with a real strategy id: every
# manifest id is a bare slug, and this one is namespaced.  It is a presentation
# key only -- nothing joins on it and nothing stores it.
CORE_MANDATE_SERIES_ID = "core:mandate"
CORE_MANDATE_SERIES_TITLE = "Core / cash mandate"

# The execution mode this authority DECLARES (sql/349), CHECK-pinned to 'paper'.
#
# ⚠ Required on insert with no column default, deliberately: a writer that forgets
# it must fail rather than inherit safety it never asked for.
#
# ⚠⚠ It records the AUTHORITY'S DECLARATION and nothing more. It does not record
# which account, environment or broker credentials a resulting trade actually
# used -- the real backstop stays the demo-only credential configuration and
# `app/security/unattended_guard.py`. Do not cite this as the demo gate.
CORE_MANDATE_MODE = "paper"

PERCENT_BASIS = Decimal("100")
# NUMERIC(8,4) percentages and NUMERIC(18,6) amounts, matching sql/311.  Both
# halves of each column type are enforced: scale, so a value cannot be silently
# rounded, and precision, so an oversized one raises CoreMandateError instead of
# Postgres' NumericValueOutOfRange.
#
# The amount pair and the two validators below are PUBLIC because
# `strategy_core_allocator` sizes a rebalance in the same NUMERIC(18,6) shape
# (#2603 item 3).  Re-declaring `6` and `18` there would be two constants that
# must agree and nothing making them.  The percentage pair stays private: no
# second consumer writes a NUMERIC(8,4) column.
_PCT_PLACES = 4
_PCT_PRECISION = 8
AMOUNT_PLACES = 6
AMOUNT_PRECISION = 18


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
        return PERCENT_BASIS - self.core_target_pct


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


def require_finite(value: Decimal, field: str) -> None:
    """Reject NaN/Infinity before anything compares the value.

    Ordering constraint, not style: ``Decimal("NaN") < 0`` raises
    ``InvalidOperation`` rather than returning False, so every range check below
    is only safe once this has run over all four fields.
    """
    if not value.is_finite():
        raise CoreMandateError(f"{field} must be a finite decimal")


def require_storable(value: Decimal, field: str, places: int, precision: int) -> None:
    """Reject a value the column cannot hold exactly, in either direction.

    Scale: Postgres rounds silently to the column scale, which would store a
    mandate the operator did not ask for.  Refusing is the only way the stored row
    and the request stay the same number.  The test is representability, not the
    exponent: ``Decimal("60.00000")`` is exact at four places and must pass.

    Precision: ``NUMERIC(p,s)`` holds ``p - s`` integer digits, and exceeding that
    raises ``NumericValueOutOfRange`` from the driver.  Bounding it here is what
    keeps every rejection a named ``CoreMandateError``.

    ⚠ Assumes ``require_finite`` has already run: ``quantize`` on a NaN raises
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
        require_finite(value, field)
    require_finite(min_rebalance_amount, "min_rebalance_amount")

    if core_target_pct < 0 or core_target_pct > PERCENT_BASIS:
        raise CoreMandateError("core_target_pct must be between 0 and 100")
    if liquidity_reserve_pct < 0 or liquidity_reserve_pct >= PERCENT_BASIS:
        raise CoreMandateError("liquidity_reserve_pct must be at least 0 and below 100")
    if rebalance_band_pct <= 0 or rebalance_band_pct > PERCENT_BASIS:
        # Zero authorises a rebalance on any drift at all, and turnover is the
        # first-order cost filter.
        raise CoreMandateError("rebalance_band_pct must be above 0 and at most 100")
    if min_rebalance_amount <= 0:
        raise CoreMandateError("min_rebalance_amount must be above 0")

    for field, value in percentages:
        require_storable(value, field, _PCT_PLACES, _PCT_PRECISION)
    require_storable(min_rebalance_amount, "min_rebalance_amount", AMOUNT_PLACES, AMOUNT_PRECISION)

    if enabled and core_instrument_id is None:
        raise CoreMandateError("an enabled core mandate requires a core instrument")
    if core_instrument_id is not None and core_instrument_id <= 0:
        raise CoreMandateError("core_instrument_id must be a positive instrument id")

    # Both band bounds are STRICT (#2670).  `core_pct` is bounded to [0,100] for a
    # non-negative sleeve and the allocator's triggers are strict, so `lower == 0`
    # and `upper == 100` are comparators that cannot become true -- a declared
    # two-sided band that is silently one-sided.  Equality is the dead point on
    # each side, not a boundary case, which is why `<= 0` and `>= PERCENT_BASIS`
    # rather than `< 0` and `> PERCENT_BASIS`.
    if core_target_pct - rebalance_band_pct <= 0:
        raise CoreMandateError("rebalance_band_pct must be below core_target_pct or the lower trigger is unreachable")
    if core_target_pct + rebalance_band_pct >= PERCENT_BASIS:
        raise CoreMandateError(
            "core_target_pct + rebalance_band_pct must be below 100 or the upper trigger is unreachable"
        )
    # Kept separate from the bound above and deliberately NOT strict: at equality
    # the reserve is exactly satisfied at the worst case the band authorises,
    # which is a legitimate (pre-cost) mandate.  Not implied by the upper bound
    # either -- that buys only positive worst-case cash, not cash of at least a
    # positive reserve.
    if PERCENT_BASIS - (core_target_pct + rebalance_band_pct) < liquidity_reserve_pct:
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
    operator_id: UUID,
    provider: str,
    environment: str,
    base_currency: str = CORE_MANDATE_BASE_CURRENCY,
) -> CoreMandate:
    """Append one material core/cash mandate revision.

    No parameter can record an eligibility proof: item 2 owns that evidence shape
    and gets its own table, so a mandate cannot claim proof it does not have.
    What it must do instead is REQUIRE one that exists independently -- an
    ENABLED mandate is refused unless its ``core_instrument_id`` has a fresh,
    passing, same-account proof that the instrument is the underlying product and
    not a CFD (``strategy_core_eligibility.require_core_eligibility``).

    ``operator_id`` / ``provider`` / ``environment`` are required because a gate
    that cannot name the account cannot select the right proof; eligibility is
    per-account regulatory state, not an instrument attribute.

    ⚠ A DISABLED mandate is not gated, and MAY still name an instrument --
    ``strategy_core_mandate_enabled_has_instrument`` is one-directional. It is
    ungated because it authorises nothing; re-enabling it passes the gate like
    any other enable.

    ⚠ This is a write-time gate only. It does not keep an already-enabled mandate
    proved: a stored mandate outlives its proof, and item 3 re-proves at
    execution time.
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
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", CORE_MANDATE_ADVISORY_LOCK)
    if values.enabled:
        # Inside the lock and inside this transaction, so the freshness test and
        # the INSERT see the same `now()` -- transaction-start time is constant,
        # which is what removes the check-then-write window.
        # ⚠ NOT an `assert`. `validate_core_mandate` does guarantee this, but
        # `python -O` strips asserts, and the surviving code would then call the
        # gate with `instrument_id=None` -- which fails closed only because no
        # proof can match NULL. A check standing between a caller and an
        # authorisation must not be removable by an interpreter flag.
        if values.core_instrument_id is None:
            raise CoreMandateError("an enabled core mandate must name a core instrument")
        require_selected_core_instrument(conn, instrument_id=values.core_instrument_id)
        require_core_eligibility(
            conn,
            instrument_id=values.core_instrument_id,
            operator_id=operator_id,
            provider=provider,
            environment=environment,
        )
    current = load_core_mandate(conn)
    if current is not None and not _is_material_change(current, values):
        raise CoreMandateError("core mandate change must alter at least one mandate value")
    if current is not None and current.core_instrument_id != values.core_instrument_id:
        try:
            capital = load_engine_capital_authority(conn)
        except EngineCapitalObservationError as exc:
            raise CoreMandateError("core instrument cannot change while capital ownership is incomplete") from exc
        if capital is not None and capital.core_active_position_ids:
            raise CoreMandateError("core instrument cannot change while an owned core position is active")
    revision = 1 if current is None else current.revision + 1
    row = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason,mode
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING core_mandate_event_id
        """,
        # ⚠ THREE lists that must stay aligned: the column list, the placeholder
        # count, and this tuple. #2623 shipped a value into the wrong column by
        # appending at a different ordinal in one of them. `mode` is appended
        # LAST in all three; check all three when adding a field here.
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
            CORE_MANDATE_MODE,
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
    "AMOUNT_PLACES",
    "AMOUNT_PRECISION",
    "CORE_MANDATE_ADVISORY_LOCK",
    "CORE_MANDATE_MODE",
    "CORE_MANDATE_SERIES_ID",
    "CORE_MANDATE_SERIES_TITLE",
    "CORE_MANDATE_BASE_CURRENCY",
    "CORE_MANDATE_POLICY_VERSION",
    "PERCENT_BASIS",
    "CoreMandate",
    "CoreMandateError",
    "CoreMandateValues",
    "configure_core_mandate",
    "load_core_mandate",
    "require_finite",
    "require_storable",
    "validate_core_mandate",
]
