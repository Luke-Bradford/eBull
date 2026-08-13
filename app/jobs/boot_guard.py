"""Stream A PR-A T1.8 (#1233): jobs-process boot guard for operator existence.

The jobs daemon has no useful work to do until ``/auth/setup`` has
created the operator row. Pre-#1233 the jobs process would happily
boot against an unprepared DB and every scheduled fire would reject
silently with ``bootstrap_not_complete`` — operator sees nothing
ingested and has to grep stderr to discover the root cause.

This module owns the **pure check**:

    check_operator_exists(conn, *, skip_env_set=False) -> BootGuardOutcome

The wrapper that performs the DB connection lifecycle + cleanup-on-fail
+ ``SystemExit(2)`` lives at
``app/jobs/__main__.py::_check_operator_exists_with_cleanup`` to match
the existing ``_ensure_*_with_cleanup`` chain conventions. The pure
function here is testable without driving the boot path
(``tests/test_jobs_boot_guard.py``).

Semantically distinct from sibling ``_ensure_*_with_cleanup`` helpers,
which RE-SEED missing default singletons (kill_switch, bootstrap_state,
runtime_config, budget_config, transaction_cost_config). Operator
absence cannot be re-seeded — the operator must invoke
``POST /auth/setup`` manually with a desired master-key + password.
Hence ``_check_*`` (verify; fail if missing) not ``_ensure_*`` (create
if missing).

Spec: docs/proposals/etl/stream-a-run-8-fixes.md §1 T1.8 + §13
(Stream A v2.3, 2026-05-24).
"""

from __future__ import annotations

from enum import Enum
from typing import Any

import psycopg


class BootGuardOutcome(Enum):
    """Closed-set result of the operator-existence boot check.

    Closed-set per CLAUDE.md ``rows_skipped`` discipline — every call
    site exhaustively matches all three members; an unmatched outcome
    is a defect, not a default.
    """

    OPERATOR_PRESENT = "operator_present"
    OPERATOR_ABSENT = "operator_absent"
    SKIPPED_BY_ENV = "skipped_by_env"


def check_operator_exists(
    conn: psycopg.Connection[Any],
    *,
    skip_env_set: bool,
) -> BootGuardOutcome:
    """Pure check: does the ``operators`` table have ≥ 1 row?

    Honours the ``EBULL_JOBS_SKIP_OPERATOR_CHECK=1`` escape hatch via
    the ``skip_env_set`` arg (caller reads the env var; this function
    stays pure).

    Performs ONE SELECT against the supplied connection. Owns no
    connection lifecycle, no cleanup, no env var read — every side
    effect is in the caller (``_check_operator_exists_with_cleanup``
    in ``app/jobs/__main__.py``).
    """
    if skip_env_set:
        return BootGuardOutcome.SKIPPED_BY_ENV
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM operators LIMIT 1")
        if cur.fetchone() is None:
            return BootGuardOutcome.OPERATOR_ABSENT
    return BootGuardOutcome.OPERATOR_PRESENT


def is_cold_start_state(conn: psycopg.Connection[Any]) -> bool:
    """Pure check: is the application in pre-/auth/setup cold-start state?

    Returns ``True`` iff ``bootstrap_state.status = 'pending'`` — meaning
    first-install bootstrap has never been triggered. Operator absence
    in this state is the expected initial condition (``/auth/setup``
    has not been run yet), NOT a defect.

    Returns ``False`` when:
      * ``bootstrap_state.status`` is anything other than ``'pending'``
        (``'running' | 'complete' | 'partial_error' | 'cancelled'``) —
        post-install state where operator absence IS a defect.
      * The ``bootstrap_state`` singleton row is missing entirely
        (should not happen given the earlier
        ``_ensure_bootstrap_state_singleton_with_cleanup`` guard; fail
        closed to preserve hard-fail behaviour rather than mask the
        anomaly).

    Issue #1363: kept as a stand-alone primitive for direct callers
    + test coverage. The wrapper at
    ``app/jobs/__main__.py::_check_operator_exists_with_cleanup``
    instead uses :func:`read_boot_gate_snapshot` to read operator
    presence + cold-start signal in ONE SELECT — necessary because
    READ COMMITTED + autocommit make two separate SELECTs vulnerable
    to a stale-decision interleaving (Codex 2 P2: /auth/setup + Re-run
    all can commit between the two probes and flip both facts under
    the wrapper's nose).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM bootstrap_state WHERE id = 1")
        row = cur.fetchone()
    if row is None:
        return False
    return row[0] == "pending"


def read_boot_gate_snapshot(conn: psycopg.Connection[Any]) -> tuple[bool, bool]:
    """Atomic single-SELECT read of (operator_present, is_cold_start).

    Issue #1363 (Codex 2 P2 fold rounds 1 + 2):

    Round 1 (atomicity): the wrapper previously called
    :func:`check_operator_exists` + :func:`is_cold_start_state` as two
    separate SELECT statements. Under PostgreSQL's default READ
    COMMITTED isolation each statement sees the latest committed
    state, so even on the same connection the two probes can sample
    different points in time — a concurrent ``/auth/setup`` + Re-run
    all sequence can commit between the probes and produce a stale
    verdict. Folding all facts into one SELECT means PostgreSQL
    evaluates the sub-queries in a single statement-level snapshot —
    atomic by construction without needing REPEATABLE READ.

    Round 2 (cold-start composition): ``bootstrap_state.status =
    'pending'`` is necessary but NOT sufficient to identify the
    pre-setup window. ``/auth/setup`` does NOT mutate
    ``bootstrap_state`` — only the first Re-run-all click flips status
    away from ``'pending'``. So a post-setup-but-pre-bootstrap state
    where the operator row was accidentally deleted is
    indistinguishable from a true cold-start using status alone. The
    ``operator_audit`` table records every ``'setup'`` event
    historically (insert-only, no delete path); the **absence** of any
    ``'setup'`` row is the load-bearing signal that ``/auth/setup``
    has never been run on this DB.

    Returns:
        (operator_present, is_cold_start) where:
          * ``operator_present`` = ``EXISTS(SELECT 1 FROM operators)``
          * ``is_cold_start``    = ``status = 'pending'``
            AND ``NOT EXISTS(setup event in operator_audit)``.
            False when ``bootstrap_state`` row missing (fail closed).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            "EXISTS(SELECT 1 FROM operators) AS operator_present, "
            "(COALESCE((SELECT status FROM bootstrap_state WHERE id = 1), '') = 'pending' "
            " AND NOT EXISTS(SELECT 1 FROM operator_audit WHERE event_type = 'setup')) "
            "AS is_cold_start",
        )
        row = cur.fetchone()
    if row is None:
        # Defensive — SELECT … always returns a single row in PG; this
        # branch exists for type-checker happiness + future-proofing.
        return (False, False)
    return (bool(row[0]), bool(row[1]))


OPERATOR_MISSING_ERROR_MESSAGE: str = (
    "jobs boot blocked: no operators row in DB. "
    "Run POST /auth/setup via the API process to create one. "
    "Cold-start override: set EBULL_JOBS_SKIP_OPERATOR_CHECK=1 in the jobs-process env."
)
"""Operator-actionable string persisted to ``bootstrap_state.last_jobs_boot_error``
on hard-fail. Surfaced via ``GET /system/status``.

Kept here (rather than inline in ``__main__.py``) so the test layer can
assert the exact wording without importing ``__main__`` (which has
side-effectful imports — APScheduler etc.).
"""
