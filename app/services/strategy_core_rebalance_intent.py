"""Persist one core/cash rebalance evaluation (#2603 item 3, execution half, step 1).

`evaluate_core_rebalance` is pure and, until this module, had no caller.  This is
that caller: load the live mandate, evaluate against an observed sleeve, write one
append-only row.

⚠⚠ CORRECTED 2026-08-22 (#2603 step 3b-3).  This said "AUTHORISES NOTHING, and
mechanically so rather than by promise: no table has a foreign key to
``strategy_core_rebalance_intents`` and no other module reads it".  Both halves are
now false -- ``sql/349`` added ``strategy_trades.core_rebalance_intent_id``, and
``app/services/strategy_core_submission_gate.py`` reads the table.  The reading side
arrived as promised; the promise was not re-read when it did.  ⚠ It also cited
``tests/test_core_rebalance_intent_has_no_readers.py`` as asserting both halves --
**that file does not exist** anywhere in the repo, so the claim was never enforced by
anything.  A named enforcement is only as good as its existence.

What holds instead, weaker on purpose: a row here is submission-gate INPUT, not
authority.  The gate has no acting caller in ``app/`` or ``scripts/``, so no path runs
from a row to an order.  That is a fact about today's call graph, not a mechanical
impossibility -- and saying so is the point, because the previous wording survived the
change that falsified it by sounding structural.

The producer is ``app/workers/scheduler.py::core_rebalance_observation``.

⚠ What this does NOT provide, so it is an owed obligation rather than a silence:
no in-flight suppression (the allocator is stateless and will re-recommend a trade
already in flight), no expiry, and no eligibility proof -- ``sql/346``'s gate is
write-time on ``configure_core_mandate`` and explicitly not an execution control.
The executor re-proves at submission, bounds ``evaluated_at``, and keys one trade
per intent.

Spec: ``docs/proposals/ta/2026-08-14-core-rebalance-intent.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any, Final

import psycopg

from app.services.strategy_core_allocator import (
    CoreRebalanceDecision,
    CoreSleeveState,
    evaluate_core_rebalance,
)
from app.services.strategy_core_mandate import (
    AMOUNT_PLACES,
    AMOUNT_PRECISION,
    CORE_MANDATE_POLICY_VERSION,
    load_core_mandate,
)

#: Exclusive magnitude bound of the NUMERIC(18,6) amount shape: 12 integer digits.
#: Deliberately the SAME bound ``_state_refusal`` applies to each sleeve component,
#: which is what makes "a non-refused verdict is always storable" true by
#: construction rather than by hope.
_MAX_AMOUNT: Final = Decimal(1).scaleb(AMOUNT_PRECISION - AMOUNT_PLACES)
_AMOUNT_QUANTUM: Final = Decimal(1).scaleb(-AMOUNT_PLACES)

#: The observed-currency column's length bound, mirroring ``sql/348``.  A value
#: over it is not an observation worth keeping verbatim; it is a caller bug, and
#: the reason code still records that the currency was the thing wrong with it.
_CURRENCY_LIMIT: Final = 16

#: The INSERT column list, in order.  ⚠ ONE tuple, from which both the column list
#: and the ``%(name)s`` block are generated, because #2623 shipped a value into the
#: wrong block by maintaining those two lists (and a reader tuple) separately:
#: psycopg binds by NAME, which makes the order feel irrelevant, but the order that
#: matters is against the column list.  Adding a column here cannot desynchronise
#: them.
_INTENT_COLUMNS: Final[tuple[str, ...]] = (
    "core_mandate_event_id",
    "allocator_policy_version",
    "recorded_by",
    "core_instrument_id",
    "currency",
    "core_market_value",
    "cash_balance",
    "state_as_of",
    "action",
    "reason_code",
    "amount",
    "core_pct",
    "target_pct",
    "lower_pct",
    "upper_pct",
    "effective_floor",
    "floor_source",
    "reserve_breached",
    "reserve_margin_pct",
)

_INSERT_INTENT: Final = (
    "INSERT INTO strategy_core_rebalance_intents ("
    + ", ".join(_INTENT_COLUMNS)
    + ") VALUES ("
    + ", ".join(f"%({column})s" for column in _INTENT_COLUMNS)
    + ") RETURNING core_rebalance_intent_id, evaluated_at"
)


@dataclass(frozen=True)
class CoreRebalanceIntent:
    """One stored evaluation: the verdict, plus where and when it landed."""

    core_rebalance_intent_id: int
    evaluated_at: datetime
    core_mandate_event_id: int | None
    decision: CoreRebalanceDecision


def _storable_or_none(value: Decimal) -> Decimal | None:
    """The observed value in the column's shape, or None when it has none.

    NULL is not "missing" here -- it is "the observation could not be represented
    in NUMERIC(18,6), and ``reason_code`` says what was wrong with it".  The
    refusals that produce such a value (``sleeve_valuation_invalid``, and the
    currency/instrument mismatches that are checked BEFORE it) are precisely the
    ones whose evidence would otherwise be the single row that cannot be written.

    Rounding rather than refusing on excess SCALE is the deliberate half.
    ``_state_refusal`` bounds finiteness and magnitude but says nothing about
    decimal places, so an ordinary broker valuation carrying nine of them reaches
    a perfectly valid ``buy_core`` -- refusing it would make a successful verdict
    unstorable, which is the bug this function exists to avoid, inverted.
    ``ROUND_DOWN`` matches the allocator's own rounding direction.
    """
    if not value.is_finite():
        return None
    if abs(value) >= _MAX_AMOUNT:
        return None
    return value.quantize(_AMOUNT_QUANTUM, rounding=ROUND_DOWN)


def _storable_currency_or_none(value: str) -> str | None:
    """The observed currency in the column's shape, or None when it has none.

    The same rule as ``_storable_or_none``, on the column where it is easiest to
    forget it applies. ``_state_refusal`` compares
    ``state.currency.strip().upper()`` to the mandate's base currency, so a BLANK
    or absurdly long observed currency is a reachable
    ``sleeve_currency_mismatch`` -- and storing it into a NOT NULL non-blank
    column would make that refusal the one row that cannot be written.

    Not stripped or upper-cased on the way in: what was OBSERVED is the evidence,
    and normalising it here would hide the difference between a caller sending
    ``"usd"`` and one sending ``" USD "``.
    """
    if not value.strip():
        return None
    if len(value) > _CURRENCY_LIMIT:
        return None
    return value


def record_core_rebalance_intent(
    conn: psycopg.Connection[Any],
    *,
    state: CoreSleeveState,
    recorded_by: str,
) -> CoreRebalanceIntent:
    """Evaluate the live mandate against ``state`` and store the verdict.

    Writes inside the caller's transaction and does not commit: the evaluation and
    its record are one fact, and a caller that has more to do in the same unit of
    work must be able to roll both back together.

    A refusal is a RETURNED verdict, never a raise -- the allocator's own posture,
    so a caller never has to catch in order to learn that the mandate is disabled.

    ⚠ ``broker_minimum`` is deliberately not a parameter, though
    ``evaluate_core_rebalance`` accepts one.  This slice performs no broker I/O and
    so cannot SOURCE a minimum; accepting one would store a caller assertion with
    no provenance and no record of which provider rule was applied -- and whether
    eToro's ``min_position_amount`` even governs an incremental buy or a partial
    sell is unsettled (the allocator's docstring flags it).  The executor holds the
    eligibility response and can answer it with evidence.  Consequence, so it is
    not later read as a defect: ``floor_source`` can only be ``mandate`` here, and
    ``broker_minimum_invalid`` is unreachable.
    """
    mandate = load_core_mandate(conn)
    decision = evaluate_core_rebalance(mandate, state)

    params: dict[str, Any] = {
        "core_mandate_event_id": None if mandate is None else mandate.event_id,
        "allocator_policy_version": CORE_MANDATE_POLICY_VERSION,
        "recorded_by": recorded_by,
        "core_instrument_id": state.core_instrument_id,
        "currency": _storable_currency_or_none(state.currency),
        "core_market_value": _storable_or_none(state.core_market_value),
        "cash_balance": _storable_or_none(state.cash_balance),
        "state_as_of": state.as_of,
        "action": decision.action,
        "reason_code": decision.reason_code,
        "amount": decision.amount,
        "core_pct": decision.core_pct,
        "target_pct": decision.target_pct,
        "lower_pct": decision.lower_pct,
        "upper_pct": decision.upper_pct,
        "effective_floor": decision.effective_floor,
        "floor_source": decision.floor_source,
        "reserve_breached": decision.reserve_breached,
        "reserve_margin_pct": decision.reserve_margin_pct,
    }
    row = conn.execute(_INSERT_INTENT, params).fetchone()
    if row is None:
        raise RuntimeError("strategy_core_rebalance_intents INSERT did not return a row")

    return CoreRebalanceIntent(
        core_rebalance_intent_id=int(row[0]),
        evaluated_at=row[1],
        core_mandate_event_id=None if mandate is None else mandate.event_id,
        decision=decision,
    )


__all__ = [
    "CoreRebalanceIntent",
    "record_core_rebalance_intent",
]
