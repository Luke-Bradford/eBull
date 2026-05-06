"""Credential health state machine + write-through helpers (#974 / #975).

Every auth-using path (HTTP request handlers, WebSocket subscriber,
batch-job adapters) reports its outcome through this module. The module
owns:

  * Row-level health state on ``broker_credentials`` (untested / valid /
    rejected). Computed aggregate at operator level (worst-of, with
    REJECTED dominating MISSING per locked precedence).
  * Side-transactional write-through: ``record_health_outcome`` opens
    its own connection from the pool, commits, then ``pg_notify``. A
    caller's transaction rolling back can never lose a health write.
  * REJECTED-stickiness: only an explicit validation-probe success
    (``source='probe'``) can promote REJECTED → VALID. Incidental 2xx
    from any other auth path cannot clear an explicit rejection.
  * NOTIFY on aggregate movement only (idempotent). Subscribers (the
    LISTEN listener at ``app/jobs/credential_health_listener.py`` —
    ticket #976) wake on the channel and re-read DB truth.

Design background: ``docs/superpowers/specs/2026-05-06-credential-health-precondition-design.md``.
Codex review chain: ``.claude/codex-974-r{1,2,3,4,5}-review.txt``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

import psycopg
import psycopg.rows
from psycopg_pool import ConnectionPool, PoolTimeout

logger = logging.getLogger(__name__)


# LISTEN/NOTIFY channel for aggregate-level operator health changes.
# Subscribers run in the API process, the jobs process, and the WS
# subscriber; cross-process delivery is the reason for using
# pg_notify rather than an in-process pub-sub (settled-decision:
# Postgres-first; no Redis). The channel is wake-up only — payload
# carries operator_id + before/after aggregate but consumers MUST
# re-read DB truth on receipt rather than trusting the payload alone.
NOTIFY_CHANNEL = "ebull_credential_health"


# Required label set per provider. Operator-level aggregate health
# is computed against this list — a missing required label means
# the operator's pair is incomplete (MISSING).
REQUIRED_LABELS_BY_PROVIDER: dict[str, tuple[str, ...]] = {
    "etoro": ("api_key", "user_key"),
}


HealthState = Literal["untested", "valid", "rejected"]


class CredentialHealth(StrEnum):
    """Operator-level aggregate credential health.

    UNTESTED / VALID / REJECTED come from the row-level column.
    MISSING is derived from the absence of a required label row for
    the operator and is never stored at row level.

    Aggregate precedence (locked, REJECTED-first per Codex r3.2):
        REJECTED > MISSING > UNTESTED > VALID

    REJECTED dominates MISSING because if the operator has saved at
    least one rejected key, they have a concrete fix to make; reporting
    MISSING for the other label would mask that.
    """

    UNTESTED = "untested"
    VALID = "valid"
    REJECTED = "rejected"
    MISSING = "missing"


# ---------------------------------------------------------------------------
# Operator-level aggregate computation
# ---------------------------------------------------------------------------


def get_operator_credential_health(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    provider: str = "etoro",
    environment: str = "demo",
) -> CredentialHealth:
    """Compute the operator's aggregate credential health.

    Joins a synthetic required-labels CTE against ``broker_credentials``
    and returns the worst-of state per the locked precedence above.

    Scoped to a single (provider, environment) pair (Codex pre-push r1.2).
    Active uniqueness on broker_credentials is
    ``(operator_id, provider, label, environment)``, so demo and real rows
    are independent. Without the environment filter, a demo api_key VALID
    + real user_key VALID would falsely report VALID for either env. v1
    only uses ``demo`` but the parameter is plumbed so the same helper
    works when ``real`` arrives.

    Returns:
        CredentialHealth — REJECTED / MISSING / UNTESTED / VALID.

    Raises:
        KeyError — provider not in REQUIRED_LABELS_BY_PROVIDER.
        RuntimeError — aggregate query produced no decision (logical
            impossibility under the schema CHECK constraints; surfaced
            rather than silently defaulting per Codex r2.2).
    """
    required = REQUIRED_LABELS_BY_PROVIDER[provider]

    sql_query = """
        WITH required(label) AS (
            SELECT * FROM unnest(%(required_labels)s::text[])
        ),
        observed AS (
            SELECT label, health_state
              FROM broker_credentials
             WHERE operator_id = %(op)s
               AND provider    = %(prov)s
               AND environment = %(env)s
               AND revoked_at IS NULL
        ),
        label_join AS (
            SELECT r.label, obs.health_state
              FROM required r
              LEFT JOIN observed obs USING (label)
        )
        SELECT
            bool_or(health_state IS NULL)        AS any_missing,
            bool_or(health_state = 'rejected')   AS any_rejected,
            bool_or(health_state = 'untested')   AS any_untested,
            bool_and(health_state = 'valid')     AS all_valid
        FROM label_join
    """

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            sql_query,
            {
                "required_labels": list(required),
                "op": operator_id,
                "prov": provider,
                "env": environment,
            },
        )
        row = cur.fetchone()

    # Aggregate-only SELECT against a non-empty required CTE always
    # returns one row. The dict_row factory pins the shape; tuple/scalar
    # row_factory regressions are caught by tests in
    # test_credential_health.py::test_aggregate_row_factory_pinned.
    if row is None:
        raise RuntimeError("get_operator_credential_health: aggregate query returned no row")

    # Decision tree (REJECTED-first per locked precedence).
    if row["any_rejected"]:
        return CredentialHealth.REJECTED
    if row["any_missing"]:
        return CredentialHealth.MISSING
    if row["any_untested"]:
        return CredentialHealth.UNTESTED
    if row["all_valid"]:
        return CredentialHealth.VALID

    raise RuntimeError(
        f"get_operator_credential_health: unreachable — aggregate query produced no decision branch: {row!r}"
    )


# ---------------------------------------------------------------------------
# Side-transactional write-through
# ---------------------------------------------------------------------------


def record_health_outcome(
    *,
    credential_id: UUID,
    success: bool,
    source: Literal["probe", "incidental"],
    error_detail: str | None,
    pool: ConnectionPool[psycopg.Connection[Any]],
) -> None:
    """Write the outcome of an auth-using call through to credential health.

    Public entry point for auth-using paths (HTTP handlers, WS subscriber,
    batch-job adapters). Translates the (success, source) pair into a
    target row state and dispatches to ``record_row_health_transition``.

    Behaviour matrix:
        success=True,  source='probe'      -> row VALID (clears REJECTED)
        success=True,  source='incidental' -> row VALID iff old=='untested'; never overwrites rejected
        success=False, source=any          -> row REJECTED (sticky)

    error_detail is recorded on the row regardless; the column is
    overwritten on every call (best-effort surface for the most recent
    failure message).
    """
    new_state: HealthState = "valid" if success else "rejected"
    record_row_health_transition(
        credential_id=credential_id,
        new_state=new_state,
        source=source,
        error_detail=error_detail if not success else None,
        pool=pool,
    )


def record_row_health_transition(
    *,
    credential_id: UUID,
    new_state: HealthState,
    source: Literal["probe", "incidental"],
    error_detail: str | None,
    pool: ConnectionPool[psycopg.Connection[Any]],
) -> None:
    """Update one row's health and pg_notify if operator aggregate moves.

    Side-transaction contract:
      * Acquires its own connection from the pool. Never takes a conn
        argument — caller's transaction lifecycle is independent.
      * UPDATE under FOR UPDATE row lock so concurrent transitions
        serialize.
      * REJECTED-sticky: ``new_state='valid'`` on a row whose
        ``health_state='rejected'`` only proceeds when ``source='probe'``.
        Incidental 2xx from any other auth path returns without modifying
        the row.
      * Always touches ``last_health_check_at`` and ``last_health_error``
        (reflects "we checked"). These updates do NOT trigger NOTIFY
        — only an aggregate-level transition does.
      * On VALID transition that flips the operator aggregate from
        REJECTED to VALID: UPSERT ``operator_credential_health_transitions``
        with ``last_recovered_at = NOW()``. The AUTH_EXPIRED suppression
        query (#977) filters job_runs failures with
        ``failed_at < last_recovered_at``.
      * Idempotent: if the operator-level aggregate before and after
        the row update is identical, no NOTIFY fires.

    Pool exhaustion (Codex r2.1): ``PoolTimeout`` from the pool's
    acquisition is logged at ERROR with credential_id + intended
    new_state and re-raised. Caller is responsible for catching and
    deciding whether to fail the user-facing request — the spec says
    auth bookkeeping is best-effort beyond this contract.
    """
    try:
        with pool.connection() as conn:
            with conn.transaction():
                _do_health_transition(
                    conn,
                    credential_id=credential_id,
                    new_state=new_state,
                    source=source,
                    error_detail=error_detail,
                )
    except PoolTimeout:
        logger.error(
            "credential_health write-through pool timeout: credential_id=%s new_state=%s source=%s — DROPPED",
            credential_id,
            new_state,
            source,
        )
        raise


def _do_health_transition(
    conn: psycopg.Connection[Any],
    *,
    credential_id: UUID,
    new_state: HealthState,
    source: Literal["probe", "incidental"],
    error_detail: str | None,
) -> None:
    """Inner implementation of the transition under a caller-supplied tx.

    Split out so tests can drive it directly with a connection without
    going through the pool path. Production callers go through
    ``record_row_health_transition``, which provides the side-tx
    + PoolTimeout semantics.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT id, operator_id, provider, environment, health_state
              FROM broker_credentials
             WHERE id = %(id)s
               AND revoked_at IS NULL
             FOR UPDATE
            """,
            {"id": credential_id},
        )
        row = cur.fetchone()

        if row is None:
            # Row doesn't exist or is revoked. Not an error — a delete-
            # then-write race or a stale credential_id from a long-
            # running consumer is benign here. Log and bail.
            logger.warning(
                "credential_health: row %s not found (revoked or deleted); skipping write-through",
                credential_id,
            )
            return

        old_state: HealthState = row["health_state"]
        operator_id: UUID = row["operator_id"]
        provider: str = row["provider"]
        environment: str = row["environment"]

        # Decide whether this call mutates health_state.
        will_change_health = _should_change_state(
            old_state=old_state,
            new_state=new_state,
            source=source,
        )

        # Always reflect that we checked. last_health_check_at gives the
        # admin UI "last checked Nm ago" without depending on a state
        # transition. last_health_error always reflects the most recent
        # failure detail (or NULL on success).
        cur.execute(
            """
            UPDATE broker_credentials
               SET last_health_check_at = NOW(),
                   last_health_error = %(err)s
             WHERE id = %(id)s
            """,
            {"id": credential_id, "err": error_detail},
        )

        if not will_change_health:
            # No transition. last_health_check_at was bumped; no NOTIFY.
            return

        # Snapshot the operator aggregate BEFORE the row update.
        # Reads against the same connection within the same tx so the
        # FOR UPDATE row is visible. Environment-scoped per Codex
        # pre-push r1.2.
        old_aggregate = get_operator_credential_health(
            conn,
            operator_id=operator_id,
            provider=provider,
            environment=environment,
        )

        cur.execute(
            """
            UPDATE broker_credentials
               SET health_state = %(new)s,
                   health_state_updated_at = NOW()
             WHERE id = %(id)s
            """,
            {"id": credential_id, "new": new_state},
        )

        # Recompute aggregate after the row update.
        new_aggregate = get_operator_credential_health(
            conn,
            operator_id=operator_id,
            provider=provider,
            environment=environment,
        )

        # Record the recovery timestamp on ANY move OUT of REJECTED at
        # operator level — VALID, UNTESTED, or MISSING — not only the
        # direct REJECTED -> VALID transition. The realistic operator
        # flow is: REJECTED -> (PUT /replace) UNTESTED -> (validate-
        # stored) VALID. If we only stamped on REJECTED -> VALID we
        # would miss the recovery moment entirely (Codex pre-push
        # r2.1). The suppression query filters AUTH_EXPIRED rows with
        # failed_at < last_recovered_at, and any subsequent REJECTED
        # cycle generates new rows with failed_at > last_recovered_at
        # so they surface normally.
        _maybe_record_recovery(
            cur,
            operator_id=operator_id,
            old_aggregate=old_aggregate,
            new_aggregate=new_aggregate,
        )

        # Idempotent: if the aggregate didn't move, no NOTIFY. A row-
        # level transition that doesn't move the aggregate (e.g.
        # one of two REJECTED rows clears, but the other is still
        # REJECTED) leaves subscribers alone — the operator's situation
        # hasn't changed from their perspective.
        if old_aggregate == new_aggregate:
            return

        payload = json.dumps(
            {
                "operator_id": str(operator_id),
                "provider": provider,
                "old_aggregate": old_aggregate.value,
                "new_aggregate": new_aggregate.value,
                "at": datetime.now(UTC).isoformat(),
            }
        )
        # pg_notify inside the same tx; Postgres delivers on commit.
        # Committing first means the notify carries the durably-stored
        # state — a subscriber that re-reads will see the post-update row.
        cur.execute(
            "SELECT pg_notify(%(channel)s, %(payload)s)",
            {"channel": NOTIFY_CHANNEL, "payload": payload},
        )


def _should_change_state(
    *,
    old_state: HealthState,
    new_state: HealthState,
    source: Literal["probe", "incidental"],
) -> bool:
    """Apply REJECTED-stickiness + same-state idempotence rules.

    Returns True iff the row's health_state should be updated.

    Truth table:
        old=untested,  new=valid,    *           -> True
        old=untested,  new=rejected, *           -> True
        old=valid,     new=valid,    *           -> False (idempotent)
        old=valid,     new=rejected, *           -> True
        old=rejected,  new=valid,    probe       -> True
        old=rejected,  new=valid,    incidental  -> False (sticky)
        old=rejected,  new=rejected, *           -> False (idempotent)
    """
    if old_state == new_state:
        return False
    if old_state == "rejected" and new_state == "valid":
        return source == "probe"
    return True


# ---------------------------------------------------------------------------
# Recovery-timestamp lookup (used by AUTH_EXPIRED suppression query in #977)
# ---------------------------------------------------------------------------


def notify_aggregate_if_changed(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    provider: str,
    environment: str,
    old_aggregate: CredentialHealth,
) -> None:
    """Emit pg_notify if the operator aggregate has moved since old_aggregate.

    For callers that mutate broker_credentials rows directly (e.g. PUT
    /broker-credentials/replace) and need to wake subscribers without
    going through ``record_row_health_transition``. PUT /replace
    revokes a possibly-VALID row and inserts an UNTESTED replacement;
    the aggregate may move VALID → UNTESTED, and subscribers must
    observe that transition or they'll keep treating creds as valid
    (Codex pre-push r1.3).

    Also records the operator-level recovery timestamp when the
    aggregate moves OUT of REJECTED — covers the realistic
    REJECTED -> UNTESTED transition from PUT /replace before the
    operator runs validate-stored (Codex pre-push r2.1).

    Caller is expected to have:
      1. Snapshotted ``old_aggregate`` before any row mutations.
      2. Performed the mutations on ``conn`` inside a transaction.
      3. Called this helper inside the same transaction. The pg_notify
         fires when that transaction commits.

    Idempotent: if the aggregate hasn't moved, no NOTIFY fires and no
    recovery timestamp is written.
    """
    new_aggregate = get_operator_credential_health(
        conn,
        operator_id=operator_id,
        provider=provider,
        environment=environment,
    )
    if old_aggregate == new_aggregate:
        return

    with conn.cursor() as cur:
        _maybe_record_recovery(
            cur,
            operator_id=operator_id,
            old_aggregate=old_aggregate,
            new_aggregate=new_aggregate,
        )

        payload = json.dumps(
            {
                "operator_id": str(operator_id),
                "provider": provider,
                "old_aggregate": old_aggregate.value,
                "new_aggregate": new_aggregate.value,
                "at": datetime.now(UTC).isoformat(),
            }
        )
        cur.execute(
            "SELECT pg_notify(%(channel)s, %(payload)s)",
            {"channel": NOTIFY_CHANNEL, "payload": payload},
        )


def _maybe_record_recovery(
    cur: psycopg.Cursor[Any],
    *,
    operator_id: UUID,
    old_aggregate: CredentialHealth,
    new_aggregate: CredentialHealth,
) -> None:
    """UPSERT operator_credential_health_transitions when leaving REJECTED.

    Marks "the moment this operator stopped being REJECTED at
    aggregate level" — used by the AUTH_EXPIRED suppression query
    (failed_at < last_recovered_at) so cascade rows from the rejected
    window stop surfacing in the operator-visible streak count.

    Fires on REJECTED -> {VALID, UNTESTED, MISSING}. A subsequent
    cycle back into REJECTED is fine: the new failures' failed_at
    will be after last_recovered_at and will surface correctly.
    """
    if old_aggregate != CredentialHealth.REJECTED:
        return
    if new_aggregate == CredentialHealth.REJECTED:
        return
    cur.execute(
        """
        INSERT INTO operator_credential_health_transitions
            (operator_id, last_recovered_at)
        VALUES (%(op)s, NOW())
        ON CONFLICT (operator_id) DO UPDATE
            SET last_recovered_at = EXCLUDED.last_recovered_at
        """,
        {"op": operator_id},
    )


def get_last_recovered_at(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
) -> datetime | None:
    """Return the operator's most recent REJECTED -> VALID timestamp, or None.

    Missing row OR ``last_recovered_at IS NULL`` -> None. Callers must
    treat None as "no filter applied" — i.e. all AUTH_EXPIRED rows
    remain operator-visible.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT last_recovered_at
              FROM operator_credential_health_transitions
             WHERE operator_id = %(op)s
            """,
            {"op": operator_id},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row["last_recovered_at"]
