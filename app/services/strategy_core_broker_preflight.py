"""May a core rebalance be submitted against the BROKER right now? (#2603 item 3, step 3b-2).

Step 3b-1 (:mod:`app.services.strategy_core_preflight`) is the DB-and-clock half of the
submission refusal vocabulary, and it closes by naming what it leaves out: *"account-risk
availability, broker minimums, cost assessment and broker rejection"*.  The broker
minimum landed separately (``broker_settlement_arms.effective_open_minimum``).  This
module is the other two.

The split between 3b-1 and this one is "does it need a broker": 3b-1 holds no broker
handle and every refusal it names is provable in a pure test; every refusal here is
observable only against a live account.

⚠⚠ AUTHORISES NOTHING TODAY.  No caller in ``app/`` or ``scripts/``; it writes nothing.
Named plainly, as every step of this arc has been, because #2437's R4 comment records *a
control that exists, is tested, and sits on a path the decision does not take* nine times
over on this ticket alone.

⚠⚠ BUY ONLY.  ``sell_core`` refuses before any broker call --
``core_close_side_cost_quote_unavailable``.  See :data:`CoreBrokerPreflightRefusal`.

⚠ Both broker calls are INFORMATIONAL and are deliberately not covered by
``refuse_broker_mutation_if_unattended`` (#2645): ruling informational work out was the
other half of that error.

⚠ What this still does NOT carry, so the reader does not take 3b-1 + this for the whole
set: broker REJECTION and the uncertain-submission resume path (both outcomes of
submission, not preconditions); ``maxUnitsPerOrder`` (quoted in units while this sizes in
currency, so it needs the price 3b-1 holds); and any coherence guarantee ACROSS the five
reads that inform a submission -- the advisory lock serialises our actors, not the
broker's.  The ticket owes "eligibility read and submission in ONE transaction" to the
submitter, and this module does not discharge it.

Spec: ``docs/proposals/ta/2026-08-23-core-broker-preflight.md``
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final, Literal

from app.providers.broker import (
    BrokerProvider,
    BrokerWhatIfOrder,
)
from app.services.broker_settlement_arms import (
    UNDERLYING_SETTLEMENT_TYPE,
    UNLEVERAGED_LEVERAGE,
    effective_open_minimum,
)
from app.services.strategy_core_allocator import (
    CoreMandate,
    CoreRebalanceDecision,
    CoreRebalanceReasonCode,
    CoreSleeveState,
    evaluate_core_rebalance,
)
from app.services.strategy_core_sizing import (
    CoreSizingRefusalCode,
    QuotedTradeCost,
    decode_quoted_trade_cost,
    resolve_core_trade_size,
)
from app.services.strategy_core_sleeve import CoreSleeveObservationError, observe_core_sleeve

__all__ = [
    "CORE_BROKER_PREFLIGHT_POLICY_VERSION",
    "CORE_MAX_ACCOUNT_RISK_AGE_SECONDS",
    "CoreBrokerPreflightRefusal",
    "CoreBrokerPreflightVerdict",
    "StrategyCoreBrokerPreflightError",
    "assess_core_broker_preflight",
]


class StrategyCoreBrokerPreflightError(RuntimeError):
    """A caller contract breach -- not a refusal, and deliberately not returnable.

    The same distinction ``StrategyCorePreflightError`` and ``CoreSleeveObservationError``
    draw: a refusal is a verdict about the WORLD and belongs in the returned vocabulary,
    whereas asking to preflight a decision that is not a trade is a defect in the code
    doing the asking.  Returning it would put a caller bug and a broker condition behind
    one ``reason_code``, and the caller would learn about its own bug by catching a code
    that means the broker said something.
    """


#: Frozen with the rule set it stamps.  v1 fixes, BY CONSTRUCTION: the account-risk age
#: bound below, the buy-only restriction, and the refusal precedence order.
#:
#: ⚠⚠ Widening the bound is a VERSION BUMP, never an edit to the constant.  Editing it in
#: place silently re-verdicts every past comparison, including ones already read -- the
#: rule ``account_equity_evidence.RECONCILIATION_RULE_VERSION`` states for the same
#: reason.
CORE_BROKER_PREFLIGHT_POLICY_VERSION: Final = "core-broker-preflight-v1"

#: One write-lane throttle wait, ``etoro_broker._ETORO_WRITE_INTERVAL_S`` = 3.5 s.
#: ``get_what_if_costs`` posts on ``_http_write``, NOT the 1.1 s read lane.
_NOMINAL_WRITE_THROTTLE_SECONDS: Final = Decimal("3.5")

#: One HTTP round trip, from ``etoro_broker``'s ``httpx.Client(timeout=30.0)``.
#:
#: ⚠ ``httpx`` applies that value to connect, read, write and pool INDIVIDUALLY; it is
#: not a total wall-clock cap on one attempt.  It is used here as a single-phase proxy
#: for the nominal round trip, which is what "nominal" means, and is not claimed as a
#: hard bound.
_NOMINAL_HTTP_ROUND_TRIP_SECONDS: Final = Decimal("30.0")

#: How old the account snapshot may be when this verdict is reached.
#:
#: There is no producer cadence to derive from -- the snapshot is a LIVE read stamped at
#: receipt (``_parse_account_risk_snapshot`` takes ``observed_at`` as a parameter; the
#: payload carries no broker valuation stamp).  So ``strategy_core_preflight
#: ._freshness_bound``, which derives from a producer's nominal PERIOD, does not apply
#: and is deliberately not reused.
#:
#: ⚠⚠ A first draft derived this from the WORST-CASE duration of the one intervening
#: call.  That is unsound and is recorded so it is not re-attempted: the throttle is paid
#: once per ATTEMPT (``resilient_client.py`` ``_request``), and on a 429 ``Retry-After``
#: OVERRIDES the backoff schedule with no upper cap (``_retry_delay``; only a 0.1 s
#: floor).  The worst case is therefore unbounded, and a constant derived from it could
#: never fire -- decoration wearing a derivation.
#:
#: Fixed instead at the NOMINAL single-attempt duration of that call: one write-lane
#: throttle wait plus one round trip, rounded up.  A retrying call will typically exceed
#: it and refusing is the INTENDED outcome -- a submission whose cost quote needed
#: retries is one whose account view we no longer trust, and the caller re-runs from a
#: fresh snapshot.  This bound is meant to bite.
#:
#: ⚠ It bounds a STALL, not market movement.  Nothing here makes the snapshot and the
#: what-if simultaneous, and no second snapshot is taken after the cost call.  The
#: sleeve-vs-cost coherence question is separately bounded, by ``decode_quoted_trade_cost``
#: refusing ``cost_quote_stale`` against ``CoreSleeveState.as_of``.
CORE_MAX_ACCOUNT_RISK_AGE_SECONDS: Final = int(
    (_NOMINAL_WRITE_THROTTLE_SECONDS + _NOMINAL_HTTP_ROUND_TRIP_SECONDS).to_integral_value(rounding="ROUND_CEILING")
)

#: Tolerated clock skew on the snapshot stamp, matching ``strategy_core_preflight
#: ._FUTURE_SKEW``.  A stamp further into the future than this is refused rather than
#: treated as maximally fresh -- without it a corrupted future timestamp never ages out.
_FUTURE_SKEW: Final = timedelta(seconds=5)

CoreBrokerPreflightRefusal = Literal[
    "core_close_side_cost_quote_unavailable",
    "core_account_risk_unavailable",
    "core_account_risk_stale",
    "core_account_risk_unobservable",
    "core_sleeve_moved_since_decision",
    "core_minimum_currency_unsupported",
    "core_broker_open_minimum_unquoted",
    "core_cost_assessment_unavailable",
]
"""The closed vocabulary of reasons the BROKER refuses a core submission.

A ``Literal`` rather than ``str`` so pyright checks every ``return`` site and a code
cannot be introduced without appearing here -- step 3a's device, and the allocator's
before it.

⚠ PRECEDENCE IS THE DECLARATION ORDER and it is load-bearing: an input can be a sell,
stale AND uncostable at once, and without a fixed order the recorded explanation moves
with a refactor.

``core_close_side_cost_quote_unavailable`` leads because it is decided before any broker
call.  ⚠⚠ It is a REAL limitation of the core arm and not a formality -- a rebalance that
can only buy is half an allocator.  ``BrokerWhatIfOrder``'s docstring carries the two
measurements that force it (#2712, 2026-08-14): the close arm REQUIRES ``position_ids``
(400 without them) and an open-arm quote does NOT bound the close-arm cost -- measured
dearer on 4 of 5 held positions by 5.7x, 8.5x, 13.0x and 18.5x.  Neither
``CoreSleeveState`` nor ``BrokerInstrumentInvestment`` carries a position id, so no close
quote is constructible from this module's inputs, and the one substitution available
would under-state a cost bound by an order of magnitude -- the single direction a cost
bound must never be wrong in.  What unblocks it: position ids threaded from
``broker_positions``, plus a close-side floor rule, which the portal does NOT document
(``effective_open_minimum`` states both minimums for OPENING only).

``core_broker_open_minimum_unquoted`` fails CLOSED, and the distinction matters:
``effective_open_minimum`` returning ``None`` means the broker quoted no usable
threshold, whereas ``evaluate_core_rebalance``'s ``broker_minimum=None`` means THE CALLER
HAS NO APPLICABLE MINIMUM TO SUPPLY.  Passing the first through as the second would read
an unanswered question as an answered one.
"""

#: Codes this module passes through UNCHANGED rather than re-coding.  Re-coding would
#: put two names on one condition and lose which layer decided it.
CoreBrokerPreflightReason = CoreBrokerPreflightRefusal | CoreRebalanceReasonCode | CoreSizingRefusalCode


@dataclass(frozen=True)
class CoreBrokerPreflightVerdict:
    """The broker's answer: a cost-adjusted size, or one named refusal.

    ``amount`` is ``0`` on a refusal and is otherwise the currency amount to trade in the
    mandate's base currency -- the same contract ``CoreSizingResult.amount`` carries,
    because it IS that number.

    ``snapshot_observed_at`` stays populated on a refusal wherever it is known: an
    operator diagnosing ``core_account_risk_stale`` needs the stamp that was too old, and
    blanking it would ship the refusal without its evidence.
    """

    admitted: bool
    reason_code: CoreBrokerPreflightReason | None
    amount: Decimal
    cost_rate: Decimal | None
    snapshot_observed_at: datetime | None
    max_account_risk_age_seconds: int = CORE_MAX_ACCOUNT_RISK_AGE_SECONDS
    policy_version: str = CORE_BROKER_PREFLIGHT_POLICY_VERSION


_ZERO: Final = Decimal("0")


def _refused(
    reason_code: CoreBrokerPreflightReason,
    *,
    snapshot_observed_at: datetime | None = None,
) -> CoreBrokerPreflightVerdict:
    return CoreBrokerPreflightVerdict(
        admitted=False,
        reason_code=reason_code,
        amount=_ZERO,
        cost_rate=None,
        snapshot_observed_at=snapshot_observed_at,
    )


def _age_within_bound(observed_at: datetime, *, now: datetime) -> bool:
    """Is the snapshot young enough, and not implausibly future-stamped?"""
    delta = now - observed_at
    if delta < -_FUTURE_SKEW:
        return False
    return delta.total_seconds() <= CORE_MAX_ACCOUNT_RISK_AGE_SECONDS


def assess_core_broker_preflight(
    broker: BrokerProvider,
    *,
    mandate: CoreMandate,
    decision: CoreRebalanceDecision,
    core_instrument_id: int,
    eligibility_response_currency: str,
    eligibility_min_position_exposure: Decimal | None,
    eligibility_min_position_amount: Decimal | None,
    clock: Callable[[], datetime],
) -> CoreBrokerPreflightVerdict:
    """Re-prove a recorded rebalance against the broker, and size it after cost.

    ⚠⚠ A CLOCK, not a ``now``, and the difference is the whole point of the age bound.
    An earlier draft took one ``now`` and tested the snapshot's age BEFORE the what-if
    call -- i.e. before the one thing whose duration the bound exists to cover.  Since
    that call is the only wall clock in the assembly, the bound could never fire: a
    request delayed by throttling, retries or an uncapped ``Retry-After`` still returned
    an ADMITTED verdict on an hour-old snapshot.  The clock is therefore read twice and
    **the second reading is the binding one**; the first only avoids spending a write-lane
    request on a snapshot that is already too old.

    ``decision`` is the verdict already recorded on the intent.  It is NOT trusted as a
    sizing input: step 4 below re-derives it from a freshly observed sleeve, so a
    decision computed from a sleeve that has since moved is REFUSED here rather than
    raising later out of ``resolve_core_trade_size._assert_decision_describes``.

    ⚠ ``CoreSizingContractError`` is allowed to propagate rather than being caught into a
    refusal -- a decision that does not describe its own ``(mandate, state)`` is a CALLER
    DEFECT, the distinction ``CoreSleeveObservationError`` and
    ``StrategyCorePreflightError`` both draw.  The re-derivation makes it unreachable
    through this module's own path, which is why it is a contract breach and not a
    verdict.

    The eligibility figures are the caller's ALREADY-PROVED ones (``strategy_core_
    eligibility``); this module does not re-fetch eligibility, and the request it does
    make echoes back neither direction, settlement type, leverage nor account -- see
    ``QuotedTradeCost``, which says so of the response it decodes.
    """
    if decision.action not in ("buy_core", "sell_core"):
        # A hold or a refusal is not a submission, so there is nothing to preflight.
        # Raised rather than returned for the reason `StrategyCorePreflightError` gives:
        # this is a CALLER CONTRACT BREACH, not a verdict about the world, and folding it
        # into the refusal vocabulary would let a caller learn it by catching a code that
        # means something else entirely.
        raise StrategyCoreBrokerPreflightError(
            f"decision.action is {decision.action!r}; only a trade can be submission-preflighted"
        )
    if decision.action == "sell_core":
        # Decided before any broker call: a request we know returns 400, or a quote we
        # know does not bound the cost, is not worth spending against the write lane.
        return _refused("core_close_side_cost_quote_unavailable")

    try:
        snapshot = broker.get_account_risk_snapshot()
    except Exception:
        # Broad on purpose: transport, auth, parse and shape failures are one condition
        # to a submission -- the broker's view of the account is not available.
        return _refused("core_account_risk_unavailable")

    if snapshot.observed_at.tzinfo is None:
        # `observe_core_sleeve` refuses this too, but the age test runs first and a naive
        # stamp cannot be subtracted from an aware clock reading without raising.
        return _refused("core_account_risk_unobservable")
    if not _age_within_bound(snapshot.observed_at, now=clock()):
        # Cheap pre-check only: it saves a write-lane request on a snapshot already too
        # old.  The check that BINDS is the one after the cost call.
        return _refused("core_account_risk_stale", snapshot_observed_at=snapshot.observed_at)

    try:
        state: CoreSleeveState = observe_core_sleeve(snapshot, core_instrument_id=core_instrument_id)
    except CoreSleeveObservationError:
        return _refused("core_account_risk_unobservable", snapshot_observed_at=snapshot.observed_at)

    if eligibility_response_currency.strip().upper() != "USD":
        # Runs BEFORE `effective_open_minimum` so its documented `ValueError` on non-USD
        # stays unreachable: its docstring already contracts that "both callers refuse a
        # mismatch first".  #2603 scope item 4 is the change that revisits this.
        return _refused("core_minimum_currency_unsupported", snapshot_observed_at=snapshot.observed_at)
    broker_minimum = effective_open_minimum(
        response_currency=eligibility_response_currency,
        min_position_exposure=eligibility_min_position_exposure,
        min_position_amount=eligibility_min_position_amount,
    )
    if broker_minimum is None:
        return _refused("core_broker_open_minimum_unquoted", snapshot_observed_at=snapshot.observed_at)

    # The floor is applied INSIDE the allocator, which already owns the precedence
    # between it and the mandate's own minimum and reports which one bound
    # (`floor_source`).  A second comparison here would be a copy of that rule.
    fresh = evaluate_core_rebalance(mandate, state, broker_minimum=broker_minimum)
    if fresh.action != "buy_core":
        # Includes `below_min_rebalance_amount` -- the broker floor biting, which is the
        # whole reason the minimum is passed in.  A refusal keeps ITS code; a fresh
        # verdict that merely wants a different trade is drift.
        return _refused(
            fresh.reason_code if fresh.reason_code is not None else "core_sleeve_moved_since_decision",
            snapshot_observed_at=snapshot.observed_at,
        )
    if fresh.amount != decision.amount:
        return _refused("core_sleeve_moved_since_decision", snapshot_observed_at=snapshot.observed_at)

    try:
        response = broker.get_what_if_costs(
            BrokerWhatIfOrder(
                instrument_id=core_instrument_id,
                transaction="buy",
                settlement_type=UNDERLYING_SETTLEMENT_TYPE,
                amount=fresh.amount,
                leverage=UNLEVERAGED_LEVERAGE,
            )
        )
    except Exception:
        return _refused("core_cost_assessment_unavailable", snapshot_observed_at=snapshot.observed_at)

    if not _age_within_bound(snapshot.observed_at, now=clock()):
        # THE BINDING CHECK.  The call above is the assembly's only wall clock, and it can
        # run arbitrarily long -- the throttle is paid per attempt and a 429's
        # `Retry-After` overrides the backoff with no upper cap.  Refusing here is the
        # behaviour the bound is FOR: a submission whose cost quote needed retries is one
        # whose account view we no longer trust, and the caller re-runs from a fresh
        # snapshot.  Same condition as the pre-check, so deliberately the same code.
        return _refused("core_account_risk_stale", snapshot_observed_at=snapshot.observed_at)

    cost = decode_quoted_trade_cost(
        response,
        instrument_id=core_instrument_id,
        ticket_amount=fresh.amount,
        base_currency=mandate.base_currency,
        valuation_as_of=state.as_of,
    )
    if not isinstance(cost, QuotedTradeCost):
        return _refused(cost, snapshot_observed_at=snapshot.observed_at)

    sized = resolve_core_trade_size(mandate, state, fresh, cost)
    if not sized.sized:
        assert sized.refusal_code is not None
        return _refused(sized.refusal_code, snapshot_observed_at=snapshot.observed_at)

    return CoreBrokerPreflightVerdict(
        admitted=True,
        reason_code=None,
        amount=sized.amount,
        cost_rate=sized.cost_rate,
        snapshot_observed_at=snapshot.observed_at,
    )
