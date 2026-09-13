"""Prospective portfolio feasibility screen (#2947).

Assigned pot plus planned weights plus recorded broker eligibility in, one dry-run
report out.  Pure: no connection, no clock, no broker, no persistence.

**Advisory, never authority.**  ``strategy_core_eligibility.require_core_eligibility``
and ``strategy_core_broker_preflight.assess_core_broker_preflight`` are the execution
gates; this screen exists to reject an operationally impossible portfolio BEFORE
expensive outcome evaluation, and its verdict authorises no trade.  A ``feasible``
verdict means "not refused on the dimensions checked here", never "deployable" --
see :class:`FeasibilityReport`, whose named-unknown fields say so on the artifact
itself rather than in a docstring the reader may not reach.

It is also not a selection filter (#2947 step 4).  It returns a verdict per leg of
the caller's OWN list and never a subset, so screening cannot silently shrink a
candidate portfolio or license a post-result top-N cut.

Spec: ``docs/proposals/execution/2026-09-13-portfolio-feasibility-screen.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from app.services.broker_settlement_arms import effective_open_minimum
from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_PASS_VERDICT,
    UNRESOLVED_REASONS,
    CoreEligibilityVerdict,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

#: Frozen so a later change to the precedence order or either vocabulary is visible
#: in a stored artifact instead of silently reinterpreting one.
FEASIBILITY_POLICY_VERSION = "portfolio-feasibility-v1"

#: The currency ``minPositionExposure`` is documented in (live portal,
#: ``trading--demo/check-instrument-trading-eligibility``): *"The exposure is always
#: calculated in USD"*.  Mirrors ``broker_settlement_arms._MINIMUM_QUOTE_CURRENCY``;
#: not imported because that one is private to the helper's own raise.
_MINIMUM_QUOTE_CURRENCY = "USD"

#: Absolute tolerance on ``sum(weights) == 1``.  Exact equality is unreachable in
#: ``Decimal`` -- ``1/4034`` summed 4,034 times is not 1 -- so a tolerance is
#: required.  Deliberately tight: accepting ``sum > 1`` spends past investable
#: capital and accepting ``sum < 1`` invents undeclared cash, so this covers
#: representation error only, never intent.
DEFAULT_WEIGHT_TOLERANCE = Decimal("1e-6")

#: How stale a recorded eligibility proof may be.  A recorded verdict is a
#: measurement of an account on a date, never a permanent capability.
DEFAULT_MAX_PROOF_AGE = timedelta(hours=24)

#: Tolerated clock skew on a future-stamped proof, matching the posture
#: ``strategy_core_broker_preflight._age_within_bound`` takes.
_FUTURE_SKEW = timedelta(minutes=5)

Disposition = Literal["feasible", "refused", "indeterminate"]

#: Per-leg codes, in evaluation order.  ``Literal`` rather than ``str`` so pyright
#: checks every ``return`` site against the single source.
LegReasonCode = Literal[
    "feasible",
    "duplicate_leg",
    "weight_not_positive",
    "weight_above_unity",
    "eligibility_unrecorded",
    "eligibility_account_mismatch",
    "eligibility_proof_stale",
    "eligibility_unresolved",
    "eligibility_projection_inconsistent",
    "eligibility_currency_unsupported",
    "arm_selection_ambiguous",
    "not_underlying_product",
    "open_permission_unrecorded",
    "open_not_permitted",
    "open_minimum_unrecorded",
    "below_open_minimum",
]

PortfolioReasonCode = Literal[
    "capital_not_positive",
    "reserve_out_of_range",
    "capital_currency_unconvertible",
    "weights_do_not_sum",
    "leg_refused",
    "leg_indeterminate",
]

#: Which codes withhold a verdict versus assert impossibility.  Missing or stale
#: evidence justifies withholding feasibility but does NOT establish that a
#: portfolio cannot be held -- reporting "cannot be held" when the truth is "we have
#: not looked recently" is the distinction this mapping exists to keep.
_INDETERMINATE_CODES: frozenset[str] = frozenset(
    {
        "eligibility_unrecorded",
        "eligibility_account_mismatch",
        "eligibility_proof_stale",
        "eligibility_unresolved",
        "eligibility_projection_inconsistent",
        "eligibility_currency_unsupported",
        "arm_selection_ambiguous",
        "open_permission_unrecorded",
        "open_minimum_unrecorded",
    }
)

#: Codes for which more capital is the cure, and therefore the only ones carrying a
#: ``minimum_viable_capital``.  No pot size fixes a CFD-only listing, a prohibited
#: open, a duplicate leg or an unknown floor.
_CAPITAL_CURABLE_CODES: frozenset[str] = frozenset({"feasible", "below_open_minimum"})


class PortfolioFeasibilityError(Exception):
    """A caller contract breach, not a verdict about the world."""


@dataclass(frozen=True)
class AccountScope:
    """The account identity a recorded proof must match to be evidence for it.

    Without this, a ``demo`` proof, another operator's account or a superseded
    credential could supply a passing verdict.  The field set mirrors #2833's own
    ``eligibility_account_rule``: *"sole operator, provider etoro, environment demo,
    and the current non-revoked api_key and user_key credential ids"*.
    """

    provider: str
    environment: str
    operator_id: UUID
    api_key_credential_id: UUID
    user_key_credential_id: UUID


@dataclass(frozen=True)
class FeasibilityLeg:
    """One planned holding: ``weight`` is a fraction of INVESTABLE capital."""

    instrument_id: int
    symbol: str
    weight: Decimal


@dataclass(frozen=True)
class LegEligibility:
    """A recorded ``strategy_core_eligibility_proofs`` row, projected.

    ``allow_open_position`` is ``bool | None`` because the column is nullable:
    ``None`` is "not recorded", which is not "false" and certainly not "true".

    ``qualifying_arm_count`` is ``select_underlying_long_arms``' own count, so this
    screen inherits that function's arm semantics -- including that it does not read
    the response's ``isPotential`` flag.  That is the recorder's decision (#2603);
    named here rather than silently adopted.

    ⚠⚠ ``verdict`` and ``reason_code`` are REQUIRED, and dropping them was a real
    defect caught at Codex checkpoint 2.  ``evaluate_core_eligibility`` leaves
    ``qualifying_arm_count`` at its default ``0`` for an ``unresolved`` proof --
    ``instrument_not_resolved``, ``eligibility_row_ambiguous`` -- because it never
    evaluated any arm.  A projection carrying only the count cannot tell that apart
    from a broker that answered "no", so it reports a definitive ``refused`` where the
    truth is "the response did not answer the question".  The recorder already draws
    the line (``strategy_core_eligibility.py:74-76``: *"`unresolved` means the response
    did not answer the question; `not_underlying` means it answered and the answer is
    no.  Only the second is a fact about the instrument."*) and this screen reuses it
    rather than re-deriving one from the field values.
    """

    instrument_id: int
    scope: AccountScope
    observed_at: datetime
    verdict: CoreEligibilityVerdict
    reason_code: str | None
    response_currency: str
    qualifying_arm_count: int
    allow_open_position: bool | None
    min_position_exposure: Decimal | None
    min_position_amount: Decimal | None


@dataclass(frozen=True)
class LegFeasibility:
    leg: FeasibilityLeg
    code: LegReasonCode
    disposition: Disposition
    open_minimum_usd: Decimal | None
    allocated_notional_usd: Decimal | None
    minimum_viable_capital: Decimal | None
    proof_age: timedelta | None


@dataclass(frozen=True)
class FeasibilityReport:
    """The dry-run artifact.

    The provenance fields are not decoration: a reader must be able to judge the
    numbers without a second read, and a stored report whose account scope, FX rate
    or age bound is implicit cannot be re-checked later.

    The three ``Literal``-typed trailing fields are NAMED UNKNOWNS.  The eligibility
    row exposes dimensions this screen does not test (``maxUnitsPerOrder``,
    fractional-unit capability, order types, the close side), and costs and unit
    rounding are excluded because a fee buffer would be a threshold with no source
    rule.  Carrying them as fields is what stops "not refused" reading as "checked".
    """

    verdict: Disposition
    portfolio_code: PortfolioReasonCode | None
    legs: tuple[LegFeasibility, ...]
    minimum_viable_capital: Decimal | None
    legs_with_unknown_minimum: int
    investable_usd: Decimal | None
    weights_sum: Decimal
    evaluated_at: datetime
    scope: AccountScope
    capital_currency: str
    usd_per_capital_unit: Decimal | None
    cash_reserve_fraction: Decimal
    weight_tolerance: Decimal
    max_proof_age: timedelta
    policy_version: str
    close_side_floor: Literal["unknown"] = "unknown"
    costs_and_rounding_excluded: Literal[True] = True
    max_units_per_order_checked: Literal[False] = False


def _is_usable(value: Decimal | None) -> bool:
    """Finite and not NaN.

    ⚠ Checked BEFORE any comparison, never after.  Python raises
    ``InvalidOperation`` on ``Decimal("NaN") <= 0`` rather than returning False, so a
    comparison-first guard raises where it must refuse.
    """
    return value is not None and value.is_finite()


def _scope_matches(recorded: AccountScope, declared: AccountScope) -> bool:
    """Case-insensitive on the two string fields, exact on the identifiers.

    The provider echoes its own vocabulary in whatever case it likes (the demo
    eligibility response already answers ``currency`` lower-case where the request
    sent upper), so casing is not evidence of a different account.  A credential id
    is either the declared one or it is not.
    """
    return (
        recorded.provider.strip().lower() == declared.provider.strip().lower()
        and recorded.environment.strip().lower() == declared.environment.strip().lower()
        and recorded.operator_id == declared.operator_id
        and recorded.api_key_credential_id == declared.api_key_credential_id
        and recorded.user_key_credential_id == declared.user_key_credential_id
    )


def _proof_age_within_bound(age: timedelta, *, max_proof_age: timedelta) -> bool:
    if age < -_FUTURE_SKEW:
        return False
    return age <= max_proof_age


def _minimum_viable_capital(
    *,
    open_minimum_usd: Decimal,
    weight: Decimal,
    investable_fraction: Decimal,
    usd_per_capital_unit: Decimal,
) -> Decimal | None:
    """``m_i / (w_i * (1 - r) * q)``, in the POT currency.

    ``q`` is load-bearing: ``m_i`` is USD and the pot is not, so a version without it
    compares two currencies -- the #2947 error this screen exists to prevent.

    Returns ``None`` rather than raising when the denominator is not usable.  The
    caller has already refused a non-positive weight, reserve and rate, so this is
    unreachable through this module; it is a guard against a future caller, not a
    verdict.
    """
    denominator = weight * investable_fraction * usd_per_capital_unit
    if not _is_usable(denominator) or denominator <= 0:
        return None
    return open_minimum_usd / denominator


def _classify_leg(
    leg: FeasibilityLeg,
    *,
    seen_instrument_ids: set[int],
    eligibility: Mapping[int, LegEligibility],
    scope: AccountScope,
    now: datetime,
    max_proof_age: timedelta,
    investable_usd: Decimal,
    investable_fraction: Decimal,
    usd_per_capital_unit: Decimal,
) -> LegFeasibility:
    """One leg, first matching code wins.

    The order is the spec's and is the whole reason overlapping conditions have a
    defined answer: a leg that is simultaneously stale and zero-arm reports the
    staleness, because we do not know what a fresh proof would have said about its
    arms.
    """

    def verdict(
        code: LegReasonCode,
        *,
        open_minimum_usd: Decimal | None = None,
        allocated_notional_usd: Decimal | None = None,
        minimum_viable_capital: Decimal | None = None,
        proof_age: timedelta | None = None,
    ) -> LegFeasibility:
        return LegFeasibility(
            leg=leg,
            code=code,
            disposition=(
                "feasible" if code == "feasible" else "indeterminate" if code in _INDETERMINATE_CODES else "refused"
            ),
            open_minimum_usd=open_minimum_usd,
            allocated_notional_usd=allocated_notional_usd,
            minimum_viable_capital=(minimum_viable_capital if code in _CAPITAL_CURABLE_CODES else None),
            proof_age=proof_age,
        )

    if leg.instrument_id in seen_instrument_ids:
        return verdict("duplicate_leg")
    if not _is_usable(leg.weight) or leg.weight <= 0:
        return verdict("weight_not_positive")
    if leg.weight > 1:
        return verdict("weight_above_unity")

    proof = eligibility.get(leg.instrument_id)
    if proof is None:
        return verdict("eligibility_unrecorded")
    if not _scope_matches(proof.scope, scope):
        return verdict("eligibility_account_mismatch")

    age = now - proof.observed_at
    if not _proof_age_within_bound(age, max_proof_age=max_proof_age):
        return verdict("eligibility_proof_stale", proof_age=age)
    # ⚠⚠ THE RECORDER'S VERDICT IS READ BEFORE ANY FIELD ON THE ROW.  An `unresolved`
    # proof never evaluated an arm, so its `qualifying_arm_count` is a DEFAULT and not
    # a measurement; interpreting it would convert "the response did not answer" into
    # "the answer is no".  Only `not_underlying` is a fact about the instrument.
    if proof.verdict != CORE_ELIGIBILITY_PASS_VERDICT:
        if proof.verdict == "unresolved" or (proof.reason_code or "") in UNRESOLVED_REASONS:
            return verdict("eligibility_unresolved", proof_age=age)
        # `not_underlying` — the broker answered, and the answer is no.
        return verdict("not_underlying_product", proof_age=age)
    # Defence in depth from here down: a projection that did NOT come from
    # `evaluate_core_eligibility` can claim the pass verdict while carrying field
    # values that contradict it, and a claim is not a measurement.
    #
    # Refused BEFORE `effective_open_minimum`, which RAISES on a non-USD response by
    # design (`broker_settlement_arms.py:219-234`): `minPositionAmount` carries no
    # documented currency, and its docstring records that every caller refuses a
    # mismatch first.  Reaching that raise would be a defect here, not a verdict.
    if proof.response_currency.strip().upper() != _MINIMUM_QUOTE_CURRENCY:
        return verdict("eligibility_currency_unsupported", proof_age=age)
    if proof.qualifying_arm_count > 1:
        return verdict("arm_selection_ambiguous", proof_age=age)
    # The pass verdict and zero arms cannot both be true of one recorded row, so this
    # is an inconsistent PROJECTION, not a fact about the instrument -- indeterminate
    # rather than refused, because we do not know which half is wrong.
    if proof.qualifying_arm_count <= 0:
        return verdict("eligibility_projection_inconsistent", proof_age=age)
    if proof.allow_open_position is None:
        return verdict("open_permission_unrecorded", proof_age=age)
    if not proof.allow_open_position:
        return verdict("open_not_permitted", proof_age=age)

    open_minimum = effective_open_minimum(
        response_currency=proof.response_currency,
        min_position_exposure=proof.min_position_exposure,
        min_position_amount=proof.min_position_amount,
    )
    if open_minimum is None:
        # `None` means the broker quoted no usable threshold -- NOT that any size is
        # permitted (`effective_open_minimum`'s own docstring). Fail closed.
        return verdict("open_minimum_unrecorded", proof_age=age)

    allocated = investable_usd * leg.weight
    required_pot = _minimum_viable_capital(
        open_minimum_usd=open_minimum,
        weight=leg.weight,
        investable_fraction=investable_fraction,
        usd_per_capital_unit=usd_per_capital_unit,
    )
    # `>=` and not `>`: a notional sitting exactly on the floor clears it. The floor
    # is the broker's minimum, and `effective_open_minimum` already takes the SAFE
    # side (`max` of two different quantities), so no further margin is added here.
    code: LegReasonCode = "feasible" if allocated >= open_minimum else "below_open_minimum"
    return verdict(
        code,
        open_minimum_usd=open_minimum,
        allocated_notional_usd=allocated,
        minimum_viable_capital=required_pot,
        proof_age=age,
    )


def screen_portfolio_feasibility(
    *,
    assigned_capital: Decimal,
    capital_currency: str,
    usd_per_capital_unit: Decimal | None,
    cash_reserve_fraction: Decimal,
    scope: AccountScope,
    legs: Sequence[FeasibilityLeg],
    eligibility: Mapping[int, LegEligibility],
    now: datetime,
    max_proof_age: timedelta = DEFAULT_MAX_PROOF_AGE,
    weight_tolerance: Decimal = DEFAULT_WEIGHT_TOLERANCE,
) -> FeasibilityReport:
    """Screen a candidate portfolio against the pot and the recorded broker map.

    ``usd_per_capital_unit`` is required for a non-USD pot and must be exactly ``1``
    for a USD one -- any other value on a USD pot is a caller defect, not a rate,
    because it would silently restate the pot's own size.

    Raises :class:`PortfolioFeasibilityError` on an EMPTY leg list.  An empty
    portfolio has no defined feasibility and returning ``feasible`` for one would be
    the wrong answer in the dangerous direction; it is a caller defect, the
    distinction ``strategy_core_broker_preflight`` draws between a contract breach
    and a verdict.
    """
    if not legs:
        raise PortfolioFeasibilityError("a portfolio with no legs has no feasibility")
    if now.tzinfo is None:
        raise PortfolioFeasibilityError("`now` must be timezone-aware")

    def refused(
        code: PortfolioReasonCode,
        *,
        legs_out: tuple[LegFeasibility, ...] = (),
        weights_sum: Decimal = Decimal(0),
        investable_usd: Decimal | None = None,
        minimum_viable_capital: Decimal | None = None,
        legs_with_unknown_minimum: int = 0,
        verdict: Disposition = "refused",
    ) -> FeasibilityReport:
        return FeasibilityReport(
            verdict=verdict,
            portfolio_code=code,
            legs=legs_out,
            minimum_viable_capital=minimum_viable_capital,
            legs_with_unknown_minimum=legs_with_unknown_minimum,
            investable_usd=investable_usd,
            weights_sum=weights_sum,
            evaluated_at=now,
            scope=scope,
            capital_currency=capital_currency,
            usd_per_capital_unit=usd_per_capital_unit,
            cash_reserve_fraction=cash_reserve_fraction,
            weight_tolerance=weight_tolerance,
            max_proof_age=max_proof_age,
            policy_version=FEASIBILITY_POLICY_VERSION,
        )

    # Portfolio-level domains first: every one of them makes the per-leg arithmetic
    # meaningless rather than merely wrong, and `r = 1` would divide by zero.
    if not _is_usable(assigned_capital) or assigned_capital <= 0:
        return refused("capital_not_positive")
    if not _is_usable(cash_reserve_fraction) or cash_reserve_fraction < 0 or cash_reserve_fraction >= 1:
        return refused("reserve_out_of_range")

    is_usd_pot = capital_currency.strip().upper() == _MINIMUM_QUOTE_CURRENCY
    # A USD pot may omit the rate entirely; anything else must supply one. `None` on a
    # non-USD pot is the #2947 error itself, so it refuses rather than defaulting to 1.
    supplied = Decimal(1) if is_usd_pot and usd_per_capital_unit is None else usd_per_capital_unit
    if supplied is None or not _is_usable(supplied) or supplied <= 0:
        return refused("capital_currency_unconvertible")
    rate: Decimal = supplied
    if is_usd_pot and rate != 1:
        raise PortfolioFeasibilityError(
            f"a {_MINIMUM_QUOTE_CURRENCY} pot must be supplied a rate of exactly 1, got {rate}"
        )

    # A non-finite weight is excluded from the SUM -- including it would make the sum
    # itself NaN and mask every other weight -- but it still reaches `_classify_leg`,
    # which refuses that leg by name. Both levels therefore report it.
    weights_sum = Decimal(0)
    for leg in legs:
        if _is_usable(leg.weight):
            weights_sum += leg.weight

    investable_fraction = Decimal(1) - cash_reserve_fraction
    investable_usd = assigned_capital * investable_fraction * rate

    seen: set[int] = set()
    results: list[LegFeasibility] = []
    for leg in legs:
        results.append(
            _classify_leg(
                leg,
                seen_instrument_ids=seen,
                eligibility=eligibility,
                scope=scope,
                now=now,
                max_proof_age=max_proof_age,
                investable_usd=investable_usd,
                investable_fraction=investable_fraction,
                usd_per_capital_unit=rate,
            )
        )
        seen.add(leg.instrument_id)

    legs_out = tuple(results)
    known = [r.minimum_viable_capital for r in legs_out if r.minimum_viable_capital is not None]
    # A max over the legs whose floor is KNOWN, reported alongside the count of legs
    # whose floor is not, so a reader cannot mistake it for a max over all of them.
    portfolio_minimum = max(known) if known else None
    unknown_minimum = sum(1 for r in legs_out if r.minimum_viable_capital is None)

    # Weight conservation is a portfolio-level refusal and outranks the leg codes: a
    # portfolio that spends past its pot -- or invents undeclared cash -- must not be
    # reported as though the allocation were sound, whatever the individual legs say.
    # The legs are carried anyway so the reader can see WHICH weight is wrong.
    if abs(weights_sum - 1) > weight_tolerance:
        return refused(
            "weights_do_not_sum",
            legs_out=legs_out,
            weights_sum=weights_sum,
            investable_usd=investable_usd,
            minimum_viable_capital=portfolio_minimum,
            legs_with_unknown_minimum=unknown_minimum,
        )

    # `refused` outranks `indeterminate`: one leg that genuinely cannot be opened
    # sinks the portfolio regardless of what is unknown elsewhere.
    if any(r.disposition == "refused" for r in legs_out):
        verdict: Disposition = "refused"
        portfolio_code: PortfolioReasonCode | None = "leg_refused"
    elif any(r.disposition == "indeterminate" for r in legs_out):
        verdict, portfolio_code = "indeterminate", "leg_indeterminate"
    else:
        verdict, portfolio_code = "feasible", None

    return FeasibilityReport(
        verdict=verdict,
        portfolio_code=portfolio_code,
        legs=legs_out,
        minimum_viable_capital=portfolio_minimum,
        legs_with_unknown_minimum=unknown_minimum,
        investable_usd=investable_usd,
        weights_sum=weights_sum,
        evaluated_at=now,
        scope=scope,
        capital_currency=capital_currency,
        usd_per_capital_unit=rate,
        cash_reserve_fraction=cash_reserve_fraction,
        weight_tolerance=weight_tolerance,
        max_proof_age=max_proof_age,
        policy_version=FEASIBILITY_POLICY_VERSION,
    )


__all__ = [
    "DEFAULT_MAX_PROOF_AGE",
    "DEFAULT_WEIGHT_TOLERANCE",
    "FEASIBILITY_POLICY_VERSION",
    "AccountScope",
    "Disposition",
    "FeasibilityLeg",
    "FeasibilityReport",
    "LegEligibility",
    "LegFeasibility",
    "LegReasonCode",
    "PortfolioFeasibilityError",
    "PortfolioReasonCode",
    "screen_portfolio_feasibility",
]
