"""The account-specific core-instrument eligibility proof (#2603 item 2).

Does a candidate core instrument exist on THIS account as the underlying product,
or only as a CFD?  The question cannot be answered from anything we store about
the instrument: SPY (3000) and SPY.RTH (3417) are the same fund -- identical
``company_name``, identical ``maxUnitsPerOrder`` -- and only SPY.RTH carries a
``real``/``long``/x1 arm.  Eligibility is per-account regulatory state that
changes without notice, so the answer belongs to a dated observation.

⚠⚠ **This is a WRITE-TIME gate and must never be cited as an execution control.**
``require_core_eligibility`` refuses a mandate WRITE that names an unproved core
instrument.  It does not, and cannot, stop anything trading: an enabled mandate
stays enabled after its proof ages out or is superseded by a failing observation.
Item 3 re-proves at execution time.  #2437's R4 comment records *a control that
exists, is tested, and sits on a path the decision does not take* nine times over,
so the boundary is named here rather than left to be inferred.

⚠ Sizing is recorded, not decided.  ``min_position_amount`` /
``min_position_exposure`` / ``max_units_per_order`` are stored as observed FACTS
and this module derives no floor from them, because a missing floor is an
order-sizing gap rather than evidence about what the product IS.

⚠ The rule for combining the two minimums IS now settled and cited --
``broker_settlement_arms.effective_open_minimum``, from the portal's own field
definitions (2026-08-23).  What it settles is narrow and worth reading there
before relying on it: an OPEN only, at x1, in a USD response, and as a safe upper
bound rather than an exact reproduction of the broker's rule.  It settles NOTHING
about a close or partial close, which the portal does not document at all -- so a
rebalance sell still has no broker floor from this source.

Spec: ``docs/proposals/ta/2026-08-13-core-eligibility-proof.md``
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

import psycopg

from app.providers.broker import BrokerEligibilityResponse
from app.services.broker_settlement_arms import select_underlying_long_arms

CORE_ELIGIBILITY_POLICY_VERSION = "core-eligibility-v1"

# Fixed BY CONSTRUCTION: no published formulation governs it and the eligibility
# response documents no freshness field of its own (verified on the live portal
# 2026-08-13 -- the what-if COST endpoint has `lastUpdated`, eligibility does
# not).  So the only measurable age is the age of OUR observation, and 24h is the
# coarsest window under which a proof cannot outlive more than one intervening
# trading session.  Re-proving is one request against a dedicated 20/min
# endpoint, so this is a CEILING on staleness, not a target.
CORE_ELIGIBILITY_MAX_AGE = timedelta(hours=24)

# Everything here quotes and requests USD.  Held as a constant rather than a
# literal so the #2603 item 4 currency lift has one place to look; note this is
# the currency the PROOF was requested in, which is a different question from the
# deployment-currency equality `_eligibility_reason` enforces.
CORE_ELIGIBILITY_REQUEST_CURRENCY = "USD"

CoreEligibilityVerdict = Literal["underlying", "not_underlying", "unresolved"]

# The one verdict that says "this instrument is the underlying product".  Named
# because a second reader arrived in #2833: `quotes_refresh`'s candidate scope arm
# is raw SQL, and a literal there would be a third copy of a vocabulary the CHECK
# in sql/346 already owns.
CORE_ELIGIBILITY_PASS_VERDICT: CoreEligibilityVerdict = "underlying"

# Closed vocabulary, mirrored by the CHECK in sql/346.  `unresolved` means the
# response did not answer the question; `not_underlying` means it answered and
# the answer is no.  Only the second is a fact about the instrument.
UNRESOLVED_REASONS = frozenset(
    {
        "instrument_not_resolved",
        "eligibility_row_ambiguous",
        "eligibility_currency_mismatch",
        "eligibility_arm_ambiguous",
    }
)
NOT_UNDERLYING_REASONS = frozenset({"instrument_not_open", "no_underlying_arm"})
CORE_ELIGIBILITY_REASONS = UNRESOLVED_REASONS | NOT_UNDERLYING_REASONS


class CoreEligibilityError(RuntimeError):
    """A core instrument has no usable eligibility proof."""


@dataclass(frozen=True)
class CoreEligibilityAssessment:
    """One evaluated eligibility response, before it is stored.

    Pure product of ``evaluate_core_eligibility``; carries every column
    ``record_core_eligibility_proof`` writes so the evaluation can be tested
    without a database and the INSERT holds no second opinion.
    """

    verdict: CoreEligibilityVerdict
    reason_code: str | None
    response_currency: str
    settlement_type: str | None
    direction: str | None
    leverage_values: tuple[int, ...] | None
    qualifying_arm_count: int
    allow_open_position: bool | None
    allow_close_position: bool | None
    allow_partial_close_position: bool | None
    min_position_amount: Decimal | None
    min_position_exposure: Decimal | None
    max_units_per_order: Decimal | None
    response_digest: str


@dataclass(frozen=True)
class CoreEligibilityProof:
    """A stored observation, as read back."""

    proof_id: int
    instrument_id: int
    operator_id: UUID
    provider: str
    environment: str
    api_key_credential_id: UUID
    user_key_credential_id: UUID
    observed_at: Any
    verdict: CoreEligibilityVerdict
    reason_code: str | None
    age_seconds: Decimal
    response_currency: str
    min_position_amount: Decimal | None
    min_position_exposure: Decimal | None
    max_units_per_order: Decimal | None
    #: ⚠ READ BACK SINCE #2603 step 3a, and NOT part of ``verdict``.  A passing
    #: verdict requires ``allow_open_position`` (``evaluate_core_eligibility``) and says
    #: nothing about closing.  A rebalance SELL is a partial close -- never a full
    #: one, because ``validate_core_mandate`` requires
    #: ``core_target_pct - rebalance_band_pct > 0`` and the allocator sells only to
    #: the lower band edge -- so the submission gate tests
    #: ``allow_partial_close_position`` separately.  ``None`` means the response did
    #: not say, which is why the gate compares ``IS TRUE`` rather than truthiness.
    allow_close_position: bool | None
    allow_partial_close_position: bool | None
    policy_version: str


def response_digest(raw: dict[str, Any]) -> str:
    """SHA-256 over the canonicalised WHOLE response.

    The whole response and not the instrument row, because response-level facts
    (``currency``, ``notFoundInstrumentIds``) are part of the evidence.  That is
    only sound because a proof requests exactly one instrument.

    ``allow_nan=False`` turns a non-finite number into an error rather than
    invalid JSON.  Sorted keys make the digest immune to field REORDERING but not
    to field ADDITION -- deliberately: an added field is drift this provider has
    shipped before, and re-proving costs one request.
    """
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _positive_or_none(value: Decimal | None) -> Decimal | None:
    """Keep a broker-quoted bound only when it is finite and positive.

    The provider may OMIT a minimum; it may not assert a non-positive or
    non-finite one, and sql/346 refuses to store one either.  Dropping it to NULL
    keeps the observation storable while recording "not quoted", which is what a
    nonsense value actually tells us.
    """
    if value is None or not value.is_finite() or value <= 0:
        return None
    return value


def evaluate_core_eligibility(
    response: BrokerEligibilityResponse,
    *,
    instrument_id: int,
    requested_currency: str = CORE_ELIGIBILITY_REQUEST_CURRENCY,
) -> CoreEligibilityAssessment:
    """Decide whether *instrument_id* is offered as the underlying product.

    Pure: no connection, no clock, no broker.  Order of tests is the order in
    which a response stops being able to answer -- not-found before row count,
    row count before currency, currency before arms -- so a row that is both
    resolved and listed as not-found reports the more specific failure.
    """
    digest = response_digest(response.raw_payload)
    base: dict[str, Any] = {
        "response_currency": response.currency,
        "settlement_type": None,
        "direction": None,
        "leverage_values": None,
        "qualifying_arm_count": 0,
        "allow_open_position": None,
        "allow_close_position": None,
        "allow_partial_close_position": None,
        "min_position_amount": None,
        "min_position_exposure": None,
        "max_units_per_order": None,
        "response_digest": digest,
    }

    matches = [row for row in response.eligibilities if row.instrument_id == instrument_id]
    if instrument_id in response.not_found_instrument_ids or not matches:
        return CoreEligibilityAssessment(verdict="unresolved", reason_code="instrument_not_resolved", **base)
    if len(matches) > 1:
        return CoreEligibilityAssessment(verdict="unresolved", reason_code="eligibility_row_ambiguous", **base)

    row = matches[0]
    base.update(
        allow_open_position=row.allow_open_position,
        allow_close_position=row.allow_close_position,
        allow_partial_close_position=row.allow_partial_close_position,
        min_position_exposure=_positive_or_none(row.min_position_exposure),
        max_units_per_order=_positive_or_none(row.max_units_per_order),
    )

    if response.currency.strip().upper() != requested_currency.strip().upper():
        return CoreEligibilityAssessment(verdict="unresolved", reason_code="eligibility_currency_mismatch", **base)

    arms = select_underlying_long_arms(row)
    base["qualifying_arm_count"] = len(arms)
    if not row.allow_open_position:
        return CoreEligibilityAssessment(verdict="not_underlying", reason_code="instrument_not_open", **base)
    if len(arms) != 1:
        # Zero and many are DIFFERENT answers: "not offered as the underlying" is
        # a fact about the instrument, "more than one qualifying arm" is a
        # response we cannot read. Collapsing them is what makes the executor's
        # single `eligibility_arm_ambiguous` misleading for the SPY case.
        if arms:
            return CoreEligibilityAssessment(verdict="unresolved", reason_code="eligibility_arm_ambiguous", **base)
        return CoreEligibilityAssessment(verdict="not_underlying", reason_code="no_underlying_arm", **base)

    arm = next(iter(arms))
    base.update(
        settlement_type=arm.settlement_type.strip().lower(),
        direction=arm.direction.strip().lower(),
        leverage_values=tuple(int(value) for value in arm.leverage_values),
        min_position_amount=_positive_or_none(arm.min_position_amount),
    )
    return CoreEligibilityAssessment(verdict="underlying", reason_code=None, **base)


def record_core_eligibility_proof(
    conn: psycopg.Connection[Any],
    *,
    assessment: CoreEligibilityAssessment,
    instrument_id: int,
    operator_id: UUID,
    provider: str,
    environment: str,
    api_key_credential_id: UUID,
    user_key_credential_id: UUID,
    recorded_by: str,
    requested_currency: str = CORE_ELIGIBILITY_REQUEST_CURRENCY,
) -> int:
    """Append one observation and return its id.

    ⚠ A transport failure writes NOTHING.  The caller must not funnel a provider
    exception into this function: absence of evidence stored as an observation
    turns "we could not ask" into "the broker said".

    ``observed_at`` is deliberately absent from the signature -- it is the
    database's ``now()``, so no caller can extend a proof's validity by declaring
    its age.
    """
    row = conn.execute(
        """
        INSERT INTO strategy_core_eligibility_proofs (
            instrument_id, operator_id, provider, environment,
            api_key_credential_id, user_key_credential_id,
            verdict, reason_code, requested_currency, response_currency,
            settlement_type, direction, leverage_values, qualifying_arm_count,
            allow_open_position, allow_close_position, allow_partial_close_position,
            min_position_amount, min_position_exposure, max_units_per_order,
            response_digest, policy_version, recorded_by
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING core_eligibility_proof_id
        """,
        (
            instrument_id,
            operator_id,
            provider,
            environment,
            api_key_credential_id,
            user_key_credential_id,
            assessment.verdict,
            assessment.reason_code,
            requested_currency,
            assessment.response_currency,
            assessment.settlement_type,
            assessment.direction,
            list(assessment.leverage_values) if assessment.leverage_values is not None else None,
            assessment.qualifying_arm_count,
            assessment.allow_open_position,
            assessment.allow_close_position,
            assessment.allow_partial_close_position,
            assessment.min_position_amount,
            assessment.min_position_exposure,
            assessment.max_units_per_order,
            assessment.response_digest,
            CORE_ELIGIBILITY_POLICY_VERSION,
            recorded_by,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def load_latest_core_eligibility_proof(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    operator_id: UUID,
    provider: str,
    environment: str,
) -> CoreEligibilityProof | None:
    """The newest observation for this instrument on this account, or None.

    Age is computed in SQL against the same ``now()`` the caller's transaction
    sees, so a freshness decision cannot straddle two clocks.
    """
    row = conn.execute(
        """
        SELECT core_eligibility_proof_id, instrument_id, operator_id, provider, environment,
               api_key_credential_id, user_key_credential_id, observed_at, verdict, reason_code,
               EXTRACT(EPOCH FROM (now() - observed_at))::numeric AS age_seconds, policy_version,
               allow_close_position, allow_partial_close_position,
               response_currency, min_position_amount, min_position_exposure,
               max_units_per_order
        FROM strategy_core_eligibility_proofs
        WHERE instrument_id = %s AND operator_id = %s AND provider = %s AND environment = %s
        ORDER BY observed_at DESC, core_eligibility_proof_id DESC
        LIMIT 1
        """,
        (instrument_id, operator_id, provider, environment),
    ).fetchone()
    if row is None:
        return None
    return CoreEligibilityProof(
        proof_id=int(row[0]),
        instrument_id=int(row[1]),
        operator_id=row[2],
        provider=str(row[3]),
        environment=str(row[4]),
        api_key_credential_id=row[5],
        user_key_credential_id=row[6],
        observed_at=row[7],
        verdict=str(row[8]),  # type: ignore[arg-type]
        reason_code=None if row[9] is None else str(row[9]),
        age_seconds=Decimal(str(row[10])),
        policy_version=str(row[11]),
        # Nullable in sql/346 and left as None rather than coerced: False means the
        # broker said no, None means the response did not answer, and collapsing
        # them would let an unanswered question read as a refusal (or worse, the
        # other way round once someone writes `not proof.allow_partial_close`).
        allow_close_position=None if row[12] is None else bool(row[12]),
        allow_partial_close_position=None if row[13] is None else bool(row[13]),
        response_currency=str(row[14]),
        min_position_amount=None if row[15] is None else Decimal(str(row[15])),
        min_position_exposure=None if row[16] is None else Decimal(str(row[16])),
        max_units_per_order=None if row[17] is None else Decimal(str(row[17])),
    )


def _live_credential_ids(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    provider: str,
    environment: str,
) -> tuple[UUID, UUID]:
    """The live ``(api_key, user_key)`` credential ids for this account.

    ``broker_credentials_unique_active`` (sql/019) is unique on
    ``(operator_id, provider, label, environment) WHERE revoked_at IS NULL``, so
    at most one of each exists and this cannot return an arbitrary pick.

    ⚠ ``FOR SHARE``, and it is load-bearing rather than defensive.  The caller's
    advisory lock serialises MANDATE writers against each other; it says nothing
    about a concurrent credential swap.  Without the row lock, a replacement that
    commits between this read and the caller's INSERT would leave an enabled
    mandate authorised by a proof belonging to the account that just went away --
    exactly the inheritance the credential columns exist to prevent.  ``FOR
    SHARE`` blocks the revoking UPDATE until the caller commits, and since the
    partial unique index refuses a second live row per label, blocking the revoke
    blocks the whole swap.  Shared, not exclusive: concurrent readers of the same
    account are not a hazard, only a writer is.
    """
    rows = conn.execute(
        """
        SELECT label, id FROM broker_credentials
        WHERE operator_id = %s AND provider = %s AND environment = %s
          AND revoked_at IS NULL AND label IN ('api_key','user_key')
        FOR SHARE
        """,
        (operator_id, provider, environment),
    ).fetchall()
    live = {str(label): value for label, value in rows}
    if "api_key" not in live or "user_key" not in live:
        raise CoreEligibilityError(
            f"no live {provider} {environment} credential pair for this operator; "
            "an eligibility proof cannot be attributed to an account that has none"
        )
    return live["api_key"], live["user_key"]


def require_core_eligibility(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    operator_id: UUID,
    provider: str,
    environment: str,
) -> CoreEligibilityProof:
    """Raise unless this instrument has a fresh, passing, same-account proof.

    Four separate ways to fail, each named, because "ineligible" would hide which
    one happened:

    * no observation at all;
    * the newest observation says the instrument is not the underlying product,
      or could not be resolved;
    * the newest observation was taken under DIFFERENT credentials -- a swap can
      put another eToro account behind the same operator/environment triple, and
      inheriting the old account's proof is exactly the silent failure the
      credential columns exist to prevent;
    * the newest observation has aged past ``CORE_ELIGIBILITY_MAX_AGE``.  A proof
      observed EXACTLY ``MAX_AGE`` ago is still fresh; the comparison is ``<=``.
    """
    proof = load_latest_core_eligibility_proof(
        conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        provider=provider,
        environment=environment,
    )
    if proof is None:
        raise CoreEligibilityError(
            f"instrument {instrument_id} has no {provider} {environment} eligibility proof; "
            "a core instrument must be proved to be the underlying product, not a CFD"
        )
    if proof.verdict != CORE_ELIGIBILITY_PASS_VERDICT:
        raise CoreEligibilityError(
            f"instrument {instrument_id} eligibility proof {proof.proof_id} is {proof.verdict} ({proof.reason_code})"
        )
    api_key_id, user_key_id = _live_credential_ids(
        conn, operator_id=operator_id, provider=provider, environment=environment
    )
    if (proof.api_key_credential_id, proof.user_key_credential_id) != (api_key_id, user_key_id):
        raise CoreEligibilityError(
            f"instrument {instrument_id} eligibility proof {proof.proof_id} was observed under "
            "credentials that are no longer live; re-prove against the current account"
        )
    max_age_seconds = Decimal(int(CORE_ELIGIBILITY_MAX_AGE.total_seconds()))
    if proof.age_seconds > max_age_seconds:
        raise CoreEligibilityError(
            f"instrument {instrument_id} eligibility proof {proof.proof_id} is "
            f"{proof.age_seconds:.0f}s old, past the {max_age_seconds}s maximum"
        )
    return proof


__all__ = [
    "CORE_ELIGIBILITY_MAX_AGE",
    "CORE_ELIGIBILITY_POLICY_VERSION",
    "CORE_ELIGIBILITY_REASONS",
    "CORE_ELIGIBILITY_REQUEST_CURRENCY",
    "CoreEligibilityAssessment",
    "CoreEligibilityError",
    "CoreEligibilityProof",
    "evaluate_core_eligibility",
    "load_latest_core_eligibility_proof",
    "record_core_eligibility_proof",
    "require_core_eligibility",
    "response_digest",
]
