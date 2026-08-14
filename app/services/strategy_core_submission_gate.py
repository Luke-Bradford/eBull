"""May this recorded core rebalance verdict become an order? (#2603 item 3, step 3a).

Step 3 is the executor.  This is the half of it that REFUSES: one admission
decision over one stored ``strategy_core_rebalance_intents`` row.  Step 3b builds
the observe -> record -> submit path that consumes it, and ships after this one
deliberately -- the opposite order produces a writer whose preconditions are a
comment.

⚠⚠ AUTHORISES NOTHING TODAY.  It has no caller in ``app/`` or ``scripts/`` and it
writes nothing.  Named plainly, as steps 1 and 2 named it, because #2437's R4
comment records *a control that exists, is tested, and sits on a path the decision
does not take* nine times over on this ticket alone.

⚠⚠ THE VOCABULARY BELOW IS NOT THE COMPLETE SUBMISSION REFUSAL VOCABULARY.  The
kill switch, ``enable_auto_trading``, the execution block, market-session state,
quote availability and staleness, account-risk availability, broker minimums,
cost assessment and broker rejection are all real refusals of a core submission
and NONE of them is here -- they belong to 3b, which is the code that holds a
quote and a broker.  A reader who takes this module for the full set will
conclude the core arm has no kill-switch check.  It has none YET.

Spec: ``docs/proposals/ta/2026-08-14-core-submission-gate.md``
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, Literal
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus

from app.services.strategy_core_eligibility import (
    CoreEligibilityError,
    require_core_eligibility,
)
from app.services.strategy_core_mandate import (
    CORE_MANDATE_ADVISORY_LOCK,
    CORE_MANDATE_MODE,
)

#: Frozen with the rule set it stamps.  v1 fixes, BY CONSTRUCTION and with no
#: published formulation to cite (see the spec's source-rule section): intent
#: freshness is SUPERSESSION rather than an age threshold; in-flight suppression
#: is sourced from ``strategy_order_reconciliation_state``'s own terminal set
#: rather than from ``strategy_trades.status`` names; and a rebalance sell
#: requires ``allow_partial_close_position``.
CORE_SUBMISSION_POLICY_VERSION: Final = "core-submission-v1"

#: Submissions against each other.  ``(2603, 1)`` is the mandate writers' key and
#: is taken FIRST by :func:`core_submission_lock` -- see its docstring.
CORE_SUBMISSION_ADVISORY_LOCK: Final = (2603, 3)

#: ``sql/285``'s own terminal set: the backlog index is defined
#: ``WHERE state NOT IN ('resolved','rejected')`` and the
#: ``strategy_order_reconciliation_resolved_shape`` CHECK ties exactly these two
#: to a non-NULL ``reconciled_at``.  Quoted from the migration rather than
#: re-derived, because "is this order's broker effect known" is that table's
#: entire purpose and inferring it from ``strategy_trades.status`` names is what
#: the first draft of this module got wrong in both directions.
_RECONCILIATION_TERMINAL_STATES: Final = ("resolved", "rejected")

CoreSubmissionRefusal = Literal[
    "core_intent_missing",
    "core_intent_not_actionable",
    "core_intent_superseded",
    "core_mandate_revision_stale",
    "core_mandate_not_paper",
    "core_mandate_disabled",
    "core_intent_already_submitted",
    "core_trade_in_flight",
    "core_eligibility_unproved",
    "core_partial_close_unproved",
]
"""The closed vocabulary of reasons admission is refused.

A ``Literal`` rather than ``str``, so pyright checks every ``return`` site against
it and a code cannot be introduced without appearing here (the allocator's own
device).  ⚠ PRECEDENCE IS THE DECLARATION ORDER and it is load-bearing: an input
can be superseded, already submitted AND ineligible at once, and without a fixed
order the recorded explanation moves with a refactor.
"""


class StrategyCoreSubmissionError(RuntimeError):
    """A caller contract breach -- not a refusal, and deliberately not returnable.

    Raised only when the serialising advisory lock is not held.  An unserialised
    caller is a BUG rather than a state of the world; returning it as a verdict
    would let the caller log it and carry on into the race the lock exists to
    prevent.
    """


@dataclass(frozen=True)
class CoreSubmissionAdmission:
    """One admission verdict, carrying what the caller would otherwise re-read."""

    admitted: bool
    reason_code: CoreSubmissionRefusal | None
    detail: str | None
    """⚠ NOT decoration.  Three refusals collapse materially different causes that
    the caller cannot re-derive: ``core_eligibility_unproved`` covers four
    (``require_core_eligibility`` distinguishes them and only in its message),
    ``core_intent_already_submitted`` hides whether the prior submission is
    uncertain and therefore needs reconciliation rather than a retry, and
    ``core_trade_in_flight`` hides WHICH trade blocks and in what state."""

    intent_id: int
    action: Literal["buy_core", "sell_core"] | None
    amount: Decimal | None
    core_instrument_id: int | None
    core_mandate_event_id: int | None
    eligibility_proof_id: int | None


# ⚠⚠ ONE STATEMENT, and that is the correctness property rather than a tidiness
# one.  Under READ COMMITTED a sequence of statements sees a sequence of
# snapshots, so "this is the newest intent" and "no trade cites it yet" could each
# be true of a different instant and false together.  Every table fact the gate
# decides on is read here; eligibility is the one read that follows, because it
# takes FOR SHARE on the credential rows and answers a different question.
_ADMISSION_SQL: Final = """
SELECT
    intent.action,
    intent.amount,
    intent.core_instrument_id,
    intent.core_mandate_event_id,
    mandate.mode                       AS mandate_mode,
    mandate.revision                   AS mandate_revision,
    current_mandate.revision           AS current_mandate_revision,
    current_mandate.enabled            AS current_mandate_enabled,
    newer.core_rebalance_intent_id     AS newer_intent_id,
    existing.strategy_trade_id         AS existing_trade_id,
    existing.status                    AS existing_trade_status,
    blocker.strategy_trade_id          AS blocking_trade_id,
    blocker.blocking_state             AS blocking_state
FROM strategy_core_rebalance_intents intent
LEFT JOIN strategy_core_mandate_events mandate
       ON mandate.core_mandate_event_id = intent.core_mandate_event_id
LEFT JOIN LATERAL (
    SELECT revision, enabled FROM strategy_core_mandate_events
    ORDER BY revision DESC LIMIT 1
) current_mandate ON TRUE
LEFT JOIN LATERAL (
    SELECT core_rebalance_intent_id FROM strategy_core_rebalance_intents newer_intent
    WHERE newer_intent.core_rebalance_intent_id > intent.core_rebalance_intent_id
    ORDER BY newer_intent.core_rebalance_intent_id DESC LIMIT 1
) newer ON TRUE
LEFT JOIN strategy_trades existing
       ON existing.core_rebalance_intent_id = intent.core_rebalance_intent_id
LEFT JOIN LATERAL (
    -- A non-terminal core trade blocks unless EVERY linked order has reached
    -- sql/285's terminal set -- and a trade with no linked order at all blocks
    -- too.
    --
    -- ⚠⚠ A MISSING reconciliation row blocks exactly as a non-terminal one does,
    -- and getting this wrong is a fail-OPEN.  An earlier draft filtered on
    -- `recon.state IS NOT NULL AND state <> ALL (terminal)`, so a linked order
    -- with no reconciliation row satisfied neither arm: not blocking by state
    -- (NULL), not blocking by absence (the order exists).  That admits a second
    -- rebalance while the FIRST order's broker effect is unknown, which is the
    -- one outcome this predicate exists to prevent.  Nothing in the schema
    -- requires a linked order to have a reconciliation row.
    SELECT t.strategy_trade_id,
           -- FILTERed, so the reported state is one that ACTUALLY blocks: a bare
           -- min() over every linked order would report 'rejected' (alphabetically
           -- first) for a trade blocked by an 'unresolved' sibling order.
           coalesce(
               min(coalesce(recon.state, 'no_reconciliation_row')) FILTER (
                   WHERE link.order_id IS NOT NULL
                     AND (recon.state IS NULL
                          OR recon.state <> ALL (%(terminal_reconciliation_states)s))
               ),
               'no_order'
           ) AS blocking_state
    FROM strategy_trades t
    LEFT JOIN strategy_trade_orders link ON link.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN strategy_order_reconciliation_state recon ON recon.order_id = link.order_id
    WHERE t.core_rebalance_intent_id IS NOT NULL
      AND t.status <> ALL (%(terminal_trade_statuses)s)
    GROUP BY t.strategy_trade_id
    HAVING count(*) FILTER (
               WHERE link.order_id IS NOT NULL
                 AND (recon.state IS NULL
                      OR recon.state <> ALL (%(terminal_reconciliation_states)s))
           ) > 0
        -- No linked order at all.  Unreachable through the executor (trade, order
        -- and link are one transaction) and kept because this repo's tests and
        -- fixtures insert rows directly -- sql/349's own rule: the invariant
        -- changes at the migration, not at the writer.
        OR count(link.order_id) = 0
    ORDER BY t.strategy_trade_id
    LIMIT 1
) blocker ON TRUE
WHERE intent.core_rebalance_intent_id = %(intent_id)s
"""

#: Trade statuses that cannot block: nothing is outstanding on either of them, so
#: a stranded reconciliation row against a closed or failed trade must not deny
#: every later rebalance for ever.  ⚠ NOT a claim about broker-snapshot
#: reflection -- that question is answered by the reconciliation state, above.
_TERMINAL_TRADE_STATUSES: Final = ("closed", "failed")


def _lock_held(conn: psycopg.Connection[Any], key: tuple[int, int]) -> bool:
    """Does THIS backend currently hold ``key`` as a two-int4 advisory lock?

    ⚠ ``objsubid = 2`` is required, not tidiness.  Postgres encodes a two-``int4``
    advisory key as ``classid``/``objid`` with ``objsubid = 2``, and a one-``int8``
    key as the high/low halves of the bigint with ``objsubid = 1``.  Without it an
    unrelated ``pg_advisory_lock(bigint)`` whose halves happen to match satisfies
    the check.

    ⚠ ``classid``/``objid`` are OID-width (unsigned) while
    ``pg_advisory_lock(int, int)`` takes signed ``int4``, so a NEGATIVE key
    component would not compare equal without conversion.  Both keys used here are
    positive literals; a future negative one breaks this silently.

    What this proves, exactly: that this backend currently holds a matching lock.
    NOT that this call acquired it, that the context manager owns it, or that no
    reentrant acquisition is outstanding -- ``pg_locks`` does not expose the
    reference count.  That is enough for the property the gate needs (the critical
    section is open) and short of exclusive ownership, so no such claim is made.
    """
    row = conn.execute(
        """
        SELECT 1 FROM pg_locks
        WHERE locktype='advisory' AND classid=%s AND objid=%s AND objsubid=2
          AND pid=pg_backend_pid() AND granted
        LIMIT 1
        """,
        key,
    ).fetchone()
    return row is not None


@contextmanager
def core_submission_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Serialise the core arm's observe-record-admit-submit-insert section.

    Takes TWO locks, in this order:

    1. ``CORE_MANDATE_ADVISORY_LOCK`` -- the same key ``configure_core_mandate``
       takes as an *xact* lock.  Without it the gate's
       ``core_mandate_revision_stale`` check is a TOCTOU: a revision appended
       between the check and the trade INSERT leaves a trade citing a mandate the
       operator has replaced.  Session and transaction advisory locks share one
       lock manager, so holding it blocks mandate writes for the duration of a
       submission -- which is the intended behaviour, not a side effect.
    2. ``CORE_SUBMISSION_ADVISORY_LOCK`` -- submissions against each other.

    One acquisition order, and ``configure_core_mandate`` takes only the first, so
    there is no deadlock cycle.  Shape follows ``_allocator_lock``
    (``strategy_paper_executor``), unlock-ownership assertion included: a lost
    lock means the critical section was not what it claimed to be, and that must
    be loud.
    """
    keys = (CORE_MANDATE_ADVISORY_LOCK, CORE_SUBMISSION_ADVISORY_LOCK)
    for key in keys:
        conn.execute("SELECT pg_advisory_lock(%s, %s)", key)
    conn.commit()
    try:
        yield
    finally:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        # Released in reverse acquisition order, and EVERY key is attempted before
        # anything raises: bailing on the first lost lock would leak the other for
        # the life of the session.
        lost = [
            key
            for key in reversed(keys)
            if conn.execute("SELECT pg_advisory_unlock(%s, %s)", key).fetchone() != (True,)
        ]
        conn.commit()
        if lost:
            raise StrategyCoreSubmissionError(f"core submission advisory lock ownership was lost: {lost}")


def _refused(
    intent_id: int,
    reason_code: CoreSubmissionRefusal,
    detail: str | None = None,
    *,
    action: Literal["buy_core", "sell_core"] | None = None,
    amount: Decimal | None = None,
    core_instrument_id: int | None = None,
    core_mandate_event_id: int | None = None,
) -> CoreSubmissionAdmission:
    return CoreSubmissionAdmission(
        admitted=False,
        reason_code=reason_code,
        detail=detail,
        intent_id=intent_id,
        action=action,
        amount=amount,
        core_instrument_id=core_instrument_id,
        core_mandate_event_id=core_mandate_event_id,
        eligibility_proof_id=None,
    )


def admit_core_rebalance_intent(
    conn: psycopg.Connection[Any],
    *,
    intent_id: int,
    operator_id: UUID,
    provider: str,
    environment: str,
) -> CoreSubmissionAdmission:
    """Decide whether this stored rebalance verdict may become an order now.

    Returns a verdict for every input and NEVER raises to signal a refusal -- the
    allocator's posture, for the allocator's reason: a caller that must catch in
    order to learn the mandate is disabled will eventually catch too broadly.  The
    single raise is :class:`StrategyCoreSubmissionError` for an unheld lock, which
    is a caller contract breach rather than a state of the world.

    ⚠ Reads only.  Writes nothing, calls no broker, and the whole table half is one
    statement so every fact it decides on comes from one snapshot.

    ⚠ The freshness rule is SUPERSESSION, not an age threshold, and the scope of
    "newest" is the whole table -- correct only because the mandate is a singleton
    (``load_core_mandate`` takes no account argument and
    ``strategy_core_rebalance_intents`` carries no operator/provider/environment
    column).  If a second mandate is ever introduced this predicate silently starts
    refusing unrelated intents.

    ⚠ Supersession bounds RELATIVE staleness only.  An intent that is newest, under
    the newest mandate, and two hours old is admitted: no server-side fact
    distinguishes it from one recorded two seconds ago.  The absolute bound is the
    caller's -- observe, record, admit and submit inside ONE hold of
    :func:`core_submission_lock` -- and this function cannot verify it.

    ⚠ ``require_core_eligibility`` takes ``FOR SHARE`` on the live credential rows
    and that lock ends at the next commit.  A caller that commits between admission
    and submission reopens the credential-swap window; keep the two in one
    transaction or re-admit afterwards.
    """
    if not _lock_held(conn, CORE_SUBMISSION_ADVISORY_LOCK):
        raise StrategyCoreSubmissionError(
            "core submission admission requires core_submission_lock to be held; "
            "without it the UNIQUE index refuses the second INSERT only after both callers reached the broker"
        )
    if not _lock_held(conn, CORE_MANDATE_ADVISORY_LOCK):
        raise StrategyCoreSubmissionError(
            "core submission admission requires the core mandate advisory lock to be held; "
            "without it a mandate revision can be appended between this check and the trade INSERT"
        )

    row = conn.execute(
        _ADMISSION_SQL,
        {
            "intent_id": intent_id,
            "terminal_trade_statuses": list(_TERMINAL_TRADE_STATUSES),
            "terminal_reconciliation_states": list(_RECONCILIATION_TERMINAL_STATES),
        },
    ).fetchone()
    if row is None:
        return _refused(intent_id, "core_intent_missing")
    (
        action,
        amount,
        core_instrument_id,
        core_mandate_event_id,
        mandate_mode,
        mandate_revision,
        current_mandate_revision,
        current_mandate_enabled,
        newer_intent_id,
        existing_trade_id,
        existing_trade_status,
        blocking_trade_id,
        blocking_state,
    ) = row

    if action not in ("buy_core", "sell_core"):
        # A hold or a refusal is stored as evidence (sql/348) and the FK alone
        # would let one back a trade.  Same requirement `core_arm_authorised`
        # encodes on the load path, applied here before anything reaches a broker.
        return _refused(intent_id, "core_intent_not_actionable", f"action={action}")

    # Narrowed for the caller's benefit; every field below is non-NULL on an
    # actionable row by `core_rebalance_intent_shape_matches_action` plus
    # `core_rebalance_intent_event_absent_only_when_mandate_absent`.
    verdict_action: Literal["buy_core", "sell_core"] = action
    verdict_amount = Decimal(str(amount))
    instrument_id = int(core_instrument_id)
    mandate_event_id = int(core_mandate_event_id)
    context: dict[str, Any] = {
        "action": verdict_action,
        "amount": verdict_amount,
        "core_instrument_id": instrument_id,
        "core_mandate_event_id": mandate_event_id,
    }

    if newer_intent_id is not None:
        return _refused(
            intent_id,
            "core_intent_superseded",
            f"intent {int(newer_intent_id)} is a later evaluation of the same sleeve",
            **context,
        )
    if current_mandate_revision is None or int(current_mandate_revision) != int(mandate_revision):
        return _refused(
            intent_id,
            "core_mandate_revision_stale",
            f"evaluated under revision {mandate_revision}, current is {current_mandate_revision}",
            **context,
        )
    if mandate_mode != CORE_MANDATE_MODE:
        return _refused(intent_id, "core_mandate_not_paper", f"mode={mandate_mode!r}", **context)
    if not current_mandate_enabled:
        # Reachable precisely because the check above proved the revision current:
        # the allocator's own `core_mandate_disabled` belongs to a revision this
        # intent predates, so a mandate disabled AFTER the evaluation passes every
        # other check here.
        return _refused(intent_id, "core_mandate_disabled", None, **context)
    if existing_trade_id is not None:
        return _refused(
            intent_id,
            "core_intent_already_submitted",
            f"strategy_trade {int(existing_trade_id)} status={existing_trade_status!r}",
            **context,
        )
    if blocking_trade_id is not None:
        return _refused(
            intent_id,
            "core_trade_in_flight",
            f"strategy_trade {int(blocking_trade_id)} reconciliation={blocking_state!r}",
            **context,
        )

    try:
        proof = require_core_eligibility(
            conn,
            instrument_id=instrument_id,
            operator_id=operator_id,
            provider=provider,
            environment=environment,
        )
    except CoreEligibilityError as exc:
        # Four materially different causes -- no observation, a non-`underlying`
        # verdict, credentials no longer live, and an aged-out proof -- and only
        # the message distinguishes them, so it is carried rather than dropped.
        return _refused(intent_id, "core_eligibility_unproved", str(exc), **context)

    if verdict_action == "sell_core" and proof.allow_partial_close_position is not True:
        # A rebalance sell can never be a full close: `validate_core_mandate`
        # requires `core_target_pct - rebalance_band_pct > 0` and the allocator
        # sells only down to the lower band edge, so post-trade core value is
        # strictly positive.  `is not True` rather than `not ...`: the column is
        # nullable and None means the response did not say.
        return _refused(
            intent_id,
            "core_partial_close_unproved",
            f"proof {proof.proof_id} allow_partial_close_position={proof.allow_partial_close_position!r}",
            **context,
        )

    return CoreSubmissionAdmission(
        admitted=True,
        reason_code=None,
        detail=None,
        intent_id=intent_id,
        action=verdict_action,
        amount=verdict_amount,
        core_instrument_id=instrument_id,
        core_mandate_event_id=mandate_event_id,
        eligibility_proof_id=proof.proof_id,
    )


__all__ = [
    "CORE_SUBMISSION_ADVISORY_LOCK",
    "CORE_SUBMISSION_POLICY_VERSION",
    "CoreSubmissionAdmission",
    "CoreSubmissionRefusal",
    "StrategyCoreSubmissionError",
    "admit_core_rebalance_intent",
    "core_submission_lock",
]
