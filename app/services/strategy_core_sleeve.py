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

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.strategy_core_allocator import CoreSleeveState

__all__ = ["CoreSleeveObservationError", "observe_core_sleeve"]


class CoreSleeveObservationError(ValueError):
    """The core sleeve cannot be observed from this snapshot.

    Raised rather than returned as a verdict, unlike everything in
    ``strategy_core_allocator``.  The distinction is the layer: the allocator never
    raises because a caller that must catch to learn the mandate is disabled will
    eventually catch too broadly, whereas every condition here is INPUT DRIFT -- a
    payload that cannot describe the sleeve at all.  ``AccountEquityEvidenceError``
    is the settled in-repo precedent for that class.
    """


def observe_core_sleeve(
    snapshot: BrokerAccountRiskSnapshot,
    *,
    core_instrument_id: int,
    exact_owned_market_value: Decimal,
    assigned_cash_available: Decimal,
) -> CoreSleeveState:
    """Build the allocator's sleeve state from ONE broker snapshot.

    ``exact_owned_market_value`` is resolved by ``strategy_engine_capital`` from this
    snapshot's exact direct rows and immutable ownership IDs. ``assigned_cash_available``
    is the lesser of this snapshot's broker cash and the assigned-pot headroom. Thus both
    still share one broker observation while DB authority can only narrow the cash term.

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

    return CoreSleeveState(
        core_instrument_id=core_instrument_id,
        # Manual holdings never reach these inputs: only exact-owned ids were joined.
        core_market_value=exact_owned_market_value,
        cash_balance=assigned_cash_available,
        currency=currency,
        as_of=snapshot.observed_at,
    )
