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
