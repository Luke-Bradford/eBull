"""Observe the core sleeve from one broker snapshot (#2704, #2603 item 3 step 3b).

``evaluate_core_rebalance`` needs a ``CoreSleeveState`` and nothing in ``app/`` could
supply one.  This module is that supplier, and it is the whole of it.

⚠ NO CALLER, deliberately -- same posture as every other piece of item 3, restated here
rather than left to be inferred.  The acting caller is the executor, whose acceptance
needs the operator-attended session #2603 reserves.

Pure: no connection, no clock, no broker call.  Reads one already-fetched snapshot.

Spec: ``docs/proposals/ta/2026-08-14-core-sleeve-observer.md``
"""

from __future__ import annotations

from decimal import Decimal

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerInstrumentInvestment
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.strategy_core_allocator import CoreSleeveState

__all__ = ["CoreSleeveObservationError", "observe_core_sleeve"]

_ZERO = Decimal("0")


class CoreSleeveObservationError(ValueError):
    """The core sleeve cannot be observed from this snapshot.

    Raised rather than returned as a verdict, unlike everything in
    ``strategy_core_allocator``.  The distinction is the layer: the allocator never
    raises because a caller that must catch to learn the mandate is disabled will
    eventually catch too broadly, whereas every condition here is INPUT DRIFT -- a
    payload that cannot describe the sleeve at all.  ``AccountEquityEvidenceError``
    is the settled in-repo precedent for that class.
    """


def _core_row(snapshot: BrokerAccountRiskSnapshot, core_instrument_id: int) -> BrokerInstrumentInvestment | None:
    """The one row for this instrument, or None when the account holds none.

    Refuses a DUPLICATE rather than picking.  ``_parse_account_risk_snapshot`` keys
    its accumulator by instrument id so it cannot emit two, but
    ``BrokerAccountRiskSnapshot`` is publicly constructible and a test double or a
    future producer can.  First-match silently drops a holding and summing silently
    double-counts one; neither is a number worth having in a weight computation.
    """
    rows = [row for row in snapshot.instrument_investments if row.instrument_id == core_instrument_id]
    if len(rows) > 1:
        raise CoreSleeveObservationError(
            f"snapshot reports {len(rows)} rows for instrument {core_instrument_id}; the core sleeve is ambiguous"
        )
    return rows[0] if rows else None


def observe_core_sleeve(snapshot: BrokerAccountRiskSnapshot, *, core_instrument_id: int) -> CoreSleeveState:
    """Build the allocator's sleeve state from ONE broker snapshot.

    ⚠⚠ What "one snapshot" buys, precisely.  Both money components come from a single
    HTTP payload, so they are MUTUALLY CONSISTENT -- whatever instant the broker
    computed them for, it is the same instant for both, which is the property
    ``CoreSleeveState`` actually depends on and cannot check.  Sourcing cash from this
    call and market value from ``get_portfolio()`` would breach it on every call, with
    both figures arriving as plain Decimals and the breach undetectable downstream.

    ⚠ ``as_of`` is ``snapshot.observed_at``, which is our RECEIPT time -- assigned with
    ``datetime.now(UTC)`` after the response returns.  It is not a broker valuation
    stamp, and the payload carries none (measured 2026-08-14).  Calling it a valuation
    instant would be the same defect this module exists to remove, one layer up.

    ⚠⚠ One clause of ``CoreSleeveState``'s cash warranty is NOT established by the
    source.  It asks for cash "settled and unreserved, with pending orders, unsettled
    proceeds and accrued charges already deducted".  eToro's published available-cash
    formula deducts PENDING ORDERS and nothing else; unsettled proceeds and accrued
    charges are not separately identifiable anywhere in this payload.  That half is a
    known limitation of the source, carried openly rather than quietly assumed, and it
    is not a reason to prefer a second call -- ``get_portfolio().available_cash``
    establishes less and breaks the one-snapshot property outright.

    Raises ``CoreSleeveObservationError`` in a declared precedence, so a snapshot with
    several defects fails the same way every time: naive timestamp, then unreported
    currency, then undocumented currency, then a duplicate row, then a direct short.

    Finiteness and magnitude are deliberately NOT re-checked -- ``_state_refusal``
    already refuses ``sleeve_valuation_invalid`` on non-finite, negative and
    out-of-range components, and repeating it here would change no outcome.
    """
    if snapshot.observed_at.tzinfo is None:
        raise CoreSleeveObservationError("snapshot observed_at must be timezone-aware")
    if snapshot.account_currency_id is None:
        # #2602 item 2: an absence to refuse on, never a licence to assume USD.
        raise CoreSleeveObservationError("account currency was not reported; refusing to assume one")
    currency = DOCUMENTED_ACCOUNT_CURRENCIES.get(snapshot.account_currency_id)
    if currency is None:
        # Inferring the code from the id is the same defect wearing a lookup.
        raise CoreSleeveObservationError(
            f"account currency id {snapshot.account_currency_id} is undocumented; refusing to infer its code"
        )

    row = _core_row(snapshot, core_instrument_id)
    if row is not None and row.direct_short_positions > 0:
        # The sleeve's value is a LONG one.  Folding a short in misstates it and
        # dropping it misstates it the other way -- and shorts are unobserved on this
        # payload, so there is no measured basis for either.
        raise CoreSleeveObservationError(
            f"instrument {core_instrument_id} carries {row.direct_short_positions} direct short "
            "position(s); a long core sleeve cannot be valued through them"
        )

    return CoreSleeveState(
        core_instrument_id=core_instrument_id,
        # Absent row and all-mirror row are the same true statement: no direct long
        # holding.  `core_sleeve_empty` handles the state that results.
        core_market_value=row.direct_long_market_value if row is not None else _ZERO,
        cash_balance=snapshot.available_cash,
        currency=currency,
        as_of=snapshot.observed_at,
    )
