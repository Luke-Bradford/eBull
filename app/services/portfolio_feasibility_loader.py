"""Feed the feasibility screen from ``strategy_core_eligibility_proofs`` (#2947).

``portfolio_feasibility`` is pure by design -- the loader lives here so the screen
stays table-testable.  This module owns three things and nothing else: resolving
the account scope, selecting the right proof row, and projecting it.

⚠⚠ **"Latest wins" is a rule WITHIN ONE ACCOUNT.**  The obvious loader takes the
newest row per instrument regardless of credentials and lets the screen report a
scope mismatch.  That is wrong: a row observed under a *revoked* credential pair
is not a newer failure for this account, it is an observation of a DIFFERENT
account, and letting it shadow a valid in-scope proof is cross-account
interference rather than freshness.  ``prove_2603_core_eligibility.py`` caches
credentials, so a late write carrying old credential ids is plausible.  So the
query filters on the FULL scope and selects the latest within it.

The diagnosis that filtering would otherwise destroy is preserved separately:
:class:`OutOfScopeEvidence` reports, per requested instrument with no in-scope
proof, whether rows exist under OTHER credentials.  It never enters the screen's
input mapping -- it goes in the artifact, so the operator can tell "run a census"
apart from "investigate a credential swap".

Spec: ``docs/proposals/execution/2026-09-13-feasibility-screen-caller.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import psycopg

from app.services.operators import OperatorLookupError, sole_operator_id
from app.services.portfolio_feasibility import AccountScope, LegEligibility
from app.services.strategy_core_eligibility import (
    CoreEligibilityError,
    CoreEligibilityVerdict,
    live_credential_ids_unlocked,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime


class FeasibilityLoaderError(RuntimeError):
    """The scope or the request could not be resolved.

    Distinct from a feasibility VERDICT: a missing credential pair is not "this
    portfolio is infeasible", it is "the question could not be asked".
    """


@dataclass(frozen=True)
class ProofEvidence:
    """The raw row behind one leg, for the artifact's evidence envelope.

    ``FeasibilityReport`` carries the screen's CONCLUSIONS.  A reader re-checking
    the artifact later needs the inputs too -- the proof id above all, because it
    is the only field that identifies WHICH row was selected, and therefore the
    only way to tell a correct selection from a plausible-looking wrong one.
    """

    instrument_id: int
    proof_id: int
    observed_at: datetime
    verdict: CoreEligibilityVerdict
    reason_code: str | None
    response_currency: str
    qualifying_arm_count: int
    allow_open_position: bool | None
    min_position_exposure: Decimal | None
    min_position_amount: Decimal | None
    policy_version: str


@dataclass(frozen=True)
class OutOfScopeEvidence:
    """Rows for a requested instrument that exist under a DIFFERENT account.

    Only populated where the instrument has no in-scope proof, because that is the
    only case where it changes what the operator should do next.
    """

    instrument_id: int
    row_count: int
    newest_observed_at: datetime


@dataclass(frozen=True)
class LoadedEligibility:
    """What the caller feeds the screen, plus what it records alongside."""

    #: Screen input.  Keyed by the ROW's own ``instrument_id`` -- see the identity
    #: note on :func:`load_leg_eligibility`.
    by_instrument: dict[int, LegEligibility]
    evidence: dict[int, ProofEvidence]
    out_of_scope: dict[int, OutOfScopeEvidence]


def resolve_account_scope(
    conn: psycopg.Connection[Any],
    *,
    provider: str,
    environment: str,
) -> AccountScope:
    """The account a proof must have been observed under to be evidence for it.

    Mirrors #2833's ``eligibility_account_rule``: *"sole operator, provider etoro,
    environment demo, and the current non-revoked api_key and user_key credential
    ids"*.

    ⚠ Uses the UNLOCKED credential reader.  This screen authorises nothing and
    records the scope it used in its artifact, so it has no business holding a
    ``FOR SHARE`` that blocks a concurrent revoke -- and ``FOR SHARE`` cannot run
    in a read-only transaction at all.

    Raises :class:`FeasibilityLoaderError` when the scope cannot be resolved --
    zero or several operators, or a missing/revoked key.  That is deliberately NOT
    a feasibility verdict: rendering "we have no credentials" as ``refused`` would
    report a portfolio problem where there is an account problem.
    """
    # ⚠ NARROW deliberately: `OperatorLookupError` (zero or several operators) and
    # `CoreEligibilityError` (no live credential pair) are the two ways the account
    # can legitimately fail to resolve.  A bare `except Exception` here would
    # reclassify a programming bug -- a `TypeError`, a renamed column -- as a
    # config error, which is the quietest possible way to hide a real defect.
    # A `psycopg.Error` deliberately propagates: the caller distinguishes a
    # database failure from an account that cannot be resolved.
    try:
        operator_id = sole_operator_id(cast("psycopg.Connection[object]", conn))
        api_key_credential_id, user_key_credential_id = live_credential_ids_unlocked(
            conn, operator_id=operator_id, provider=provider, environment=environment
        )
    except (OperatorLookupError, CoreEligibilityError) as exc:
        raise FeasibilityLoaderError(f"cannot resolve the {provider} {environment} account scope: {exc}") from exc
    return AccountScope(
        provider=provider,
        environment=environment,
        operator_id=operator_id,
        api_key_credential_id=api_key_credential_id,
        user_key_credential_id=user_key_credential_id,
    )


#: ⚠ ``DISTINCT ON`` requires the leading ``ORDER BY`` expressions to MATCH its
#: list, so the single-row reader's ``ORDER BY observed_at DESC, proof_id DESC``
#: cannot be copied verbatim -- ``instrument_id`` has to lead.  ``environment``
#: leaves the key because it is a fixed equality predicate here.
#:
#: ⚠ The ``core_eligibility_proof_id DESC`` tiebreak matches
#: ``load_latest_core_eligibility_proof`` exactly.  ``observed_at`` defaults to
#: ``now()``, which is TRANSACTION-START time, so a single-transaction batch
#: writer would stamp every row identically and the timestamp alone could not
#: order them.  The current recorder commits per instrument and produces distinct
#: timestamps, so the tiebreak resolves no tie that exists today -- it is
#: insurance against a writer shape that is not a contract.
#:
#: ⚠ Deterministic is NOT the same as chronologically latest: because
#: ``observed_at`` is transaction START, a transaction that began earlier and
#: committed later can record a genuinely later observation that still loses.
#: That is inherited from #2603's clock choice (a caller must not be able to
#: extend a proof's validity by supplying its own time), named not fixed.
_LATEST_IN_SCOPE_SQL = """
    SELECT DISTINCT ON (p.instrument_id)
           p.instrument_id, p.core_eligibility_proof_id, p.observed_at,
           p.verdict, p.reason_code, p.response_currency, p.qualifying_arm_count,
           p.allow_open_position, p.min_position_exposure, p.min_position_amount,
           p.policy_version
      FROM strategy_core_eligibility_proofs p
     WHERE p.instrument_id = ANY(%(instrument_ids)s::bigint[])
       AND p.operator_id = %(operator_id)s
       AND p.provider = %(provider)s
       AND p.environment = %(environment)s
       AND p.api_key_credential_id = %(api_key_credential_id)s
       AND p.user_key_credential_id = %(user_key_credential_id)s
     ORDER BY p.instrument_id, p.observed_at DESC, p.core_eligibility_proof_id DESC
"""

#: Rows under the same operator/provider/environment but DIFFERENT credentials.
#: Restricted to the instruments that had no in-scope row, because elsewhere it
#: would answer a question nobody asked.
_OUT_OF_SCOPE_SQL = """
    SELECT p.instrument_id, count(*), max(p.observed_at)
      FROM strategy_core_eligibility_proofs p
     WHERE p.instrument_id = ANY(%(instrument_ids)s::bigint[])
       AND p.operator_id = %(operator_id)s
       AND p.provider = %(provider)s
       AND p.environment = %(environment)s
       AND (p.api_key_credential_id <> %(api_key_credential_id)s
            OR p.user_key_credential_id <> %(user_key_credential_id)s)
     GROUP BY p.instrument_id
"""


def project_leg_eligibility(row: Sequence[Any], scope: AccountScope) -> tuple[LegEligibility, ProofEvidence]:
    """One ``_LATEST_IN_SCOPE_SQL`` row into the screen's input plus its evidence.

    Pure over the row tuple so the column-by-column contract is table-testable
    without a database.

    ⚠⚠ ``verdict`` and ``reason_code`` are projected, never inferred from the
    counts.  ``evaluate_core_eligibility`` leaves ``qualifying_arm_count`` at its
    default ``0`` for an ``unresolved`` proof because it never evaluated an arm, so
    a projection carrying only the count cannot tell that apart from a broker that
    answered "no" -- and would report a definitive refusal where the truth is "the
    response did not answer the question".

    ⚠ ``allow_open_position`` stays ``bool | None``.  ``None`` is "not recorded",
    which is neither false nor true; coercing it either way is a wrong answer in
    one of the two dangerous directions.

    ⚠ The two minimums stay SEPARATE and un-merged.  The
    ``arm.min_position_amount or row.min_position_exposure`` precedence has no
    citation in the provider's documentation; ``effective_open_minimum`` owns that
    call and the screen delegates to it.
    """
    instrument_id = int(row[0])
    evidence = ProofEvidence(
        instrument_id=instrument_id,
        proof_id=int(row[1]),
        observed_at=row[2],
        verdict=cast("CoreEligibilityVerdict", str(row[3])),
        reason_code=None if row[4] is None else str(row[4]),
        response_currency=str(row[5]),
        qualifying_arm_count=int(row[6]),
        allow_open_position=None if row[7] is None else bool(row[7]),
        min_position_exposure=None if row[8] is None else Decimal(str(row[8])),
        min_position_amount=None if row[9] is None else Decimal(str(row[9])),
        policy_version=str(row[10]),
    )
    leg = LegEligibility(
        instrument_id=instrument_id,
        scope=scope,
        observed_at=evidence.observed_at,
        verdict=evidence.verdict,
        reason_code=evidence.reason_code,
        response_currency=evidence.response_currency,
        qualifying_arm_count=evidence.qualifying_arm_count,
        allow_open_position=evidence.allow_open_position,
        min_position_exposure=evidence.min_position_exposure,
        min_position_amount=evidence.min_position_amount,
    )
    return leg, evidence


def load_leg_eligibility(
    conn: psycopg.Connection[Any],
    *,
    instrument_ids: Sequence[int],
    scope: AccountScope,
) -> LoadedEligibility:
    """Latest in-scope proof per instrument, plus the out-of-scope diagnosis.

    ⚠⚠ Keys are taken from the ROW's own ``instrument_id``, never from the
    requested list.  The screen looks its legs up by key and does NOT verify that
    the mapping key matches the proof it holds -- a mapping
    ``{1: proof(instrument_id=999)}`` returns ``feasible`` today.  Keying off the
    row is what makes that unreachable from this path.

    An instrument with no in-scope proof is simply ABSENT from ``by_instrument``.
    It is never given a fabricated ``unresolved`` projection: that would invent an
    observation that never happened, and ``unresolved`` is a real recorded verdict
    meaning "the broker's response did not answer", not "we did not ask".
    """
    if not instrument_ids:
        raise FeasibilityLoaderError("no instruments requested")
    requested = list(dict.fromkeys(int(i) for i in instrument_ids))

    params: dict[str, Any] = {
        "instrument_ids": requested,
        "operator_id": scope.operator_id,
        "provider": scope.provider,
        "environment": scope.environment,
        "api_key_credential_id": scope.api_key_credential_id,
        "user_key_credential_id": scope.user_key_credential_id,
    }

    by_instrument: dict[int, LegEligibility] = {}
    evidence: dict[int, ProofEvidence] = {}
    for row in conn.execute(_LATEST_IN_SCOPE_SQL, params).fetchall():
        leg, proof = project_leg_eligibility(row, scope)
        by_instrument[leg.instrument_id] = leg
        evidence[proof.instrument_id] = proof

    missing = [i for i in requested if i not in by_instrument]
    out_of_scope: dict[int, OutOfScopeEvidence] = {}
    if missing:
        for row in conn.execute(_OUT_OF_SCOPE_SQL, {**params, "instrument_ids": missing}).fetchall():
            instrument_id = int(row[0])
            out_of_scope[instrument_id] = OutOfScopeEvidence(
                instrument_id=instrument_id,
                row_count=int(row[1]),
                newest_observed_at=row[2],
            )

    return LoadedEligibility(by_instrument=by_instrument, evidence=evidence, out_of_scope=out_of_scope)


__all__ = [
    "FeasibilityLoaderError",
    "LoadedEligibility",
    "OutOfScopeEvidence",
    "ProofEvidence",
    "load_leg_eligibility",
    "project_leg_eligibility",
    "resolve_account_scope",
]
