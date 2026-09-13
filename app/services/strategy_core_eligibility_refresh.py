"""Which recorded eligibility proofs need re-asking? (#2603 item 2, revalidation half).

#2603 item 2 asked for an eligibility proof *"with a declared freshness/revalidation
rule -- eligibility is not immutable."*  The freshness rule shipped
(``CORE_ELIGIBILITY_MAX_AGE``, and ``require_core_eligibility`` refusing past it); the
revalidator did not.  Until this module the ONLY writer of
``strategy_core_eligibility_proofs`` was ``scripts/prove_2603_core_eligibility.py``, so
more than 24h after a hand-run ``prove`` the two age-bounded consumers -- the attended
executor and #2947's feasibility screen -- could not pass at all.

⚠ Narrowly: *the two age-bounded consumers*.  ``QUOTES_REFRESH_SCOPE_SQL`` deliberately
does NOT bound proof age, so quote scope is unaffected by staleness, and
``core_rebalance_observation`` requires no proof.  "Every consumer was failing" would be
overbroad.

This module is the SELECTION half and is pure-ish: one read, no broker, no writes.  The
job in ``app/workers/scheduler.py`` owns the broker calls and the recording.

Spec: ``docs/proposals/execution/2026-09-13-core-eligibility-revalidation.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg

from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_MAX_AGE,
    CORE_ELIGIBILITY_REQUEST_INTERVAL_S,
    CoreEligibilityVerdict,
)

#: DERIVED from ``CORE_ELIGIBILITY_MAX_AGE``, never chosen independently.  With the
#: job's hourly cadence a proof is refreshed at the first tick after it passes this,
#: so worst-case age is ``REFRESH_AGE + one tick`` and there are 11 NOMINAL ticks of
#: margin before ``MAX_AGE``.
#:
#: ⚠ "Nominal" is load-bearing: that margin counts scheduler ticks, not wall-clock.
#: Lane contention, request spacing, HTTP retries and position within the batch all
#: consume real time this inequality does not model.  It is a design margin, not a
#: guarantee -- which is why ``tests/test_2603_core_eligibility_refresh.py`` pins
#: ``REFRESH_AGE + the REGISTERED tick < MAX_AGE`` against the scheduler's own
#: ``ScheduledJob`` rather than against a literal hour.
CORE_ELIGIBILITY_REFRESH_AGE = CORE_ELIGIBILITY_MAX_AGE / 2

#: Per-run WORK bound, fixed by construction from the runtime budget:
#: ``100 * CORE_ELIGIBILITY_REQUEST_INTERVAL_S`` is about 5.3 minutes, comfortably
#: inside an hourly tick.
#:
#: ⚠⚠ It is NOT the endpoint's 100-ids-per-request ceiling wearing a different hat.
#: That figure bounds ONE request and is no source rule at all for a job making
#: singleton requests.  ⚠⚠ And it neither raises nor truncates silently: raising
#: would make a >100 backlog a permanent outage that never makes progress, so the
#: selection takes the OLDEST ``limit`` and the caller reports how many were
#: deferred to the next tick.
CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN = 100

#: Re-exported so the job has one import for its spacing.
REQUEST_INTERVAL_S = CORE_ELIGIBILITY_REQUEST_INTERVAL_S


@dataclass(frozen=True)
class StaleProof:
    """One instrument due for re-asking, with the observation it supersedes.

    ``prior_verdict`` and ``prior_proof_id`` are carried so the job can report
    verdict TRANSITIONS in its ``job_runs`` note.  An ``underlying ->
    not_underlying`` flip moves ``quotes_refresh`` arm-5 membership, and finding
    that out needs to cost a glance at the note rather than a query.
    """

    instrument_id: int
    symbol: str | None
    prior_proof_id: int
    prior_verdict: CoreEligibilityVerdict
    prior_age: timedelta
    #: True when the prior proof's credential pair is not the live one.  Such a
    #: proof is unusable to ``require_core_eligibility`` (it compares the pair)
    #: however fresh it is, so it is selected regardless of age.
    credentials_superseded: bool


@dataclass(frozen=True)
class RevalidationScope:
    """The selection, plus the two facts needed to describe an empty one."""

    due: tuple[StaleProof, ...]
    #: Distinct instruments with any proof for this account triple.  Zero means
    #: "never proved"; non-zero with an empty ``due`` means "all fresh".  The two
    #: are different operator-facing situations and must not share a message.
    proved_instrument_count: int
    #: How many due instruments the per-run cap deferred to the next tick.
    deferred_count: int


# ⚠ `DISTINCT ON (instrument_id)` with the leading `ORDER BY` matching it, then
# `observed_at DESC, core_eligibility_proof_id DESC` -- the SAME ordering as
# `load_latest_core_eligibility_proof` and `QUOTES_REFRESH_SCOPE_SQL`.  Three
# readers of "the latest proof" that break a tie differently are three
# definitions, and the disagreement only ever shows up on the row that matters.
#
# ⚠ Age is `now() - observed_at` computed IN SQL, deliberately, not from a
# caller-supplied clock.  `record_core_eligibility_proof` omits `observed_at`
# from its signature precisely "so no caller can extend a proof's validity by
# declaring its age"; a Python-side comparison would hand that back.
#
# ⚠ LEFT JOIN on `instruments`, and honestly: `sql/346` line 32-33 makes
# `instrument_id` a `REFERENCES instruments(instrument_id) ON DELETE RESTRICT`,
# so an orphan proof cannot exist and LEFT vs INNER is behaviourally identical
# TODAY.  It is LEFT so that a future FK change cannot quietly convert this
# selector into one that drops rows -- the symbol is a label for the log, never
# a filter.  Do not read the LEFT as evidence that orphans are handled; it is
# insurance against a schema change, and no test can demonstrate a case the
# constraint forbids.
_SELECT_LATEST_PROOFS_SQL = """
SELECT DISTINCT ON (p.instrument_id)
       p.instrument_id,
       i.symbol,
       p.core_eligibility_proof_id,
       p.verdict,
       EXTRACT(EPOCH FROM (now() - p.observed_at))::numeric AS age_seconds,
       (p.api_key_credential_id, p.user_key_credential_id)
           IS DISTINCT FROM (%(api_key_credential_id)s::uuid, %(user_key_credential_id)s::uuid)
           AS credentials_superseded
  FROM strategy_core_eligibility_proofs p
  LEFT JOIN instruments i ON i.instrument_id = p.instrument_id
 WHERE p.operator_id = %(operator_id)s
   AND p.provider = %(provider)s
   AND p.environment = %(environment)s
 ORDER BY p.instrument_id, p.observed_at DESC, p.core_eligibility_proof_id DESC
"""


def select_proofs_to_revalidate(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    provider: str,
    environment: str,
    live_credential_ids: tuple[UUID, UUID],
    refresh_age: timedelta = CORE_ELIGIBILITY_REFRESH_AGE,
    limit: int = CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN,
) -> RevalidationScope:
    """Latest proof per instrument on this account triple, filtered to those due.

    ⚠⚠ THE NON-WIDENING INVARIANT.  The candidate set is the instruments that
    ALREADY appear in ``strategy_core_eligibility_proofs`` for this triple, so this
    can never introduce an instrument id the table does not carry.  That matters
    beyond eligibility: ``QUOTES_REFRESH_SCOPE_SQL`` arm 5 admits an instrument to
    the quote-refresh scope on its latest passing proof, and its comment states the
    bound -- *"only `prove` writes proofs ... so this arm cannot widen to the
    universe on its own"*.  A scheduled prover has to preserve that, and this is
    how.

    ⚠ It does NOT follow that quote-scope membership can only shrink, and claiming
    so was a first-draft error.  Two different sets:

    * instrument ids in the proofs table -- this job cannot grow it;
    * instruments passing ``quotes_refresh`` arm 5 -- this job CAN grow it, because
      a ``not_underlying``/``unresolved`` proof superseded by an ``underlying`` one
      ADMITS that instrument with no operator action.

    The second is bounded by the first and is the correct direction (the broker has
    said the instrument now qualifies), but it is a behaviour change and is named
    rather than left to be discovered.

    ⚠ Nothing here interprets a verdict.  In particular a non-passing verdict does
    NOT mean "the instrument stopped qualifying": ``unresolved`` is not evidence
    about the instrument at all (``strategy_core_eligibility`` line 74-76), and
    ``not_underlying`` covers ``instrument_not_open`` -- an OPEN-PERMISSION fact --
    as well as ``no_underlying_arm``.  Ownership is ``settlementType``, which
    ``broker_settlement_arms`` owns.  What moves membership is mechanical: the
    latest observation no longer satisfies the predicate its readers key on.

    Two selection arms, either sufficient:

    1. the latest proof is older than ``refresh_age``;
    2. the latest proof was observed under credential ids that are not the live
       pair.  Without arm 2 a credential rotation leaves every proof unusable to
       ``require_core_eligibility`` -- which compares the pair -- for up to
       ``refresh_age`` while the age rule calls it fresh.  A proof that cannot be
       used is not fresh in any sense its consumers care about.

    Selection keys on ``(operator_id, provider, environment)`` and NOT on the
    credential pair, so the rotation case is SELECTED rather than hidden behind an
    empty result.  ⚠ The pair passed here should come from the UNLOCKED reader --
    this is advisory, deciding what to ask.  The WRITE attributes under the locked
    ``live_credential_ids``, as that reader's own docstring requires.

    Ordered oldest-first: after downtime the most-expired instruments go first, so
    the per-run cap cannot permanently starve the same tail.
    """
    if limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}")
    if refresh_age < timedelta(0):
        raise ValueError(f"refresh_age must not be negative, got {refresh_age}")

    api_key_credential_id, user_key_credential_id = live_credential_ids
    rows = conn.execute(
        _SELECT_LATEST_PROOFS_SQL,
        {
            "operator_id": operator_id,
            "provider": provider,
            "environment": environment,
            "api_key_credential_id": api_key_credential_id,
            "user_key_credential_id": user_key_credential_id,
        },
    ).fetchall()

    due: list[StaleProof] = []
    for instrument_id, symbol, proof_id, verdict, age_seconds, credentials_superseded in rows:
        age = timedelta(seconds=float(Decimal(str(age_seconds))))
        superseded = bool(credentials_superseded)
        # `>` and not `>=`: a proof AT the trigger is not yet due, matching the
        # direction `require_core_eligibility` already settled for MAX_AGE ("a
        # proof observed EXACTLY MAX_AGE ago is still fresh; the comparison is
        # <=").  One boundary convention across both, not two.
        if not superseded and age <= refresh_age:
            continue
        due.append(
            StaleProof(
                instrument_id=int(instrument_id),
                symbol=None if symbol is None else str(symbol),
                prior_proof_id=int(proof_id),
                prior_verdict=str(verdict),  # type: ignore[arg-type]
                prior_age=age,
                credentials_superseded=superseded,
            )
        )

    due.sort(key=lambda stale: stale.prior_age, reverse=True)
    return RevalidationScope(
        due=tuple(due[:limit]),
        proved_instrument_count=len(rows),
        deferred_count=max(0, len(due) - limit),
    )


__all__ = [
    "CORE_ELIGIBILITY_REFRESH_AGE",
    "CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN",
    "REQUEST_INTERVAL_S",
    "RevalidationScope",
    "StaleProof",
    "select_proofs_to_revalidate",
]
