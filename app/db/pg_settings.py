"""PostgreSQL configuration guards (#1187, #1472).

Two fail-fast boot guards live here, both run in BOTH processes
(FastAPI lifespan + jobs entrypoint):

- ``max_locks_per_transaction`` floor (#1187) — below.
- connection budget (#1472 PR1) — refuses to boot a configuration whose
  steady-state connection demand cannot fit ``max_connections``.

eBull's ownership schema partitions 8 observation tables quarterly
(85 partitions × 3-5 indexes per parent). An unpruned SELECT against
any partitioned parent reserves ~431 distinct relation locks
(empirically measured against PG17, 2026-05-17). With the PG default
``max_locks_per_transaction=64``, bootstrap and ingest paths exhaust
the shared lock table → ``OutOfMemory: out of shared memory``.

This module's helpers run at boot in BOTH processes (FastAPI lifespan
and jobs entrypoint) and HARD-FAIL the boot if the floor is breached.
The ``EBULL_ALLOW_LOW_PG_LOCKS=1`` env var is an explicit operator
override for niche dev/CI environments where the cluster setting is
out of the operator's control; every boot logs a loud WARNING so the
bypass stays visible.

Spec: ``docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md``.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Final, Literal

import psycopg

from app.db.pool import (
    AUDIT_POOL_MAX_SIZE,
    BACKGROUND_POOL_MAX_SIZE,
    DB_POOL_MAX_SIZE,
    JOBS_POOL_MAX_SIZE,
)

logger = logging.getLogger(__name__)


PG_LOCKS_FLOOR: Final[int] = 1024
"""Minimum acceptable ``max_locks_per_transaction``.

2× the measured worst-case single-parent unpruned-SELECT lock count
(431) plus headroom for future growth (post-2030q4 partitions, new
partitioned ownership tables). Spec §5.1.
"""

PG_LOCKS_OVERRIDE_ENV: Final[str] = "EBULL_ALLOW_LOW_PG_LOCKS"
"""Operator escape hatch. Setting this env var to ``"1"`` bypasses the
hard-fail. Spec §5.2 + Risk row in §7.
"""


class PgLocksFloorBreached(RuntimeError):
    """Raised at boot when ``max_locks_per_transaction < PG_LOCKS_FLOOR``.

    The lifespan / jobs entrypoint propagates this exception so the
    process exits non-zero with a clear operator-actionable message.
    """

    def __init__(self, value: int, floor: int) -> None:
        super().__init__(
            f"max_locks_per_transaction={value} < floor={floor} — "
            f"eBull's partitioned ownership tables routinely reserve "
            f"~431 locks per unpruned-parent statement. Run "
            f"`ALTER SYSTEM SET max_locks_per_transaction = {floor};` "
            f"then restart Postgres. Set {PG_LOCKS_OVERRIDE_ENV}=1 to "
            f"bypass (development only, expect OOM under load)."
        )
        self.value = value
        self.floor = floor


def check_max_locks_per_transaction(
    conn: psycopg.Connection[Any],
    *,
    floor: int = PG_LOCKS_FLOOR,
) -> tuple[bool, int]:
    """Probe ``max_locks_per_transaction``; return ``(passes, value)``.

    Fail-open on SHOW exception (returns ``(True, 0)``): the probe is
    informational; a transient SHOW failure must not block startup —
    the downstream OOM (if it materialises) would surface anyway.
    """
    try:
        row = conn.execute("SHOW max_locks_per_transaction").fetchone()
    except Exception:
        logger.warning(
            "pg_settings: SHOW max_locks_per_transaction failed; skipping guard",
            exc_info=True,
        )
        return True, 0
    if row is None:
        return True, 0
    value = int(row[0])
    return value >= floor, value


def enforce_max_locks_floor(conn: psycopg.Connection[Any]) -> None:
    """Hard-fail wrapper. Raises ``PgLocksFloorBreached`` when the
    cluster setting is below the floor and the operator has not set
    the explicit override env var.

    Operator override: ``EBULL_ALLOW_LOW_PG_LOCKS=1`` skips the raise
    + logs a loud WARNING so the bypass stays visible. Use only in
    dev / CI where the cluster setting is fixed.
    """
    passes, value = check_max_locks_per_transaction(conn)
    if passes:
        return
    if os.environ.get(PG_LOCKS_OVERRIDE_ENV) == "1":
        logger.warning(
            "pg_settings: max_locks_per_transaction=%d below floor=%d; running anyway because %s=1 is set",
            value,
            PG_LOCKS_FLOOR,
            PG_LOCKS_OVERRIDE_ENV,
        )
        return
    raise PgLocksFloorBreached(value=value, floor=PG_LOCKS_FLOOR)


# ---------------------------------------------------------------------------
# Connection-budget guard (#1472 PR1)
# ---------------------------------------------------------------------------
#
# The dev box is a single-user co-deployment: the FastAPI process, the
# jobs process, and one Postgres all run together. ``max_connections=30``
# (``superuser_reserved_connections=3`` → 27 usable) is NOT too low for
# that workload — the #1472 failure mode was undisciplined *demand* (a
# cadence-boundary raw-connection herd), not an undersized ceiling.
#
# This guard is the fail-fast mirror of ``check_max_locks_per_transaction``
# above: it refuses to boot a *configuration* whose steady-state
# connection demand cannot mathematically fit the usable budget, so a
# self-defeating pool size can never again silently degrade at runtime.
# It is deliberately a STATIC check of configured demand — the per-fire
# burst is removed by later #1472 PRs (PR2 audit+shrink, PR4 bounded
# background pool), NOT absorbed by a large reserve here.

API_FIXED_LONGLIVED_CONNS: Final[int] = 1
"""Long-lived non-pool connections the API process holds for its whole
lifetime, by EXPECTED topology: 1 ``ebull_credential_health`` LISTEN
(``app/main.py`` credential-health thread). The ``db_pool`` + ``audit_pool``
slots are counted via their max-size constants, not here."""

JOBS_FIXED_LONGLIVED_CONNS: Final[int] = 3
"""Long-lived non-pool connections the jobs process holds for its whole
lifetime, by EXPECTED topology: the singleton-fence advisory-lock conn
(``JOBS_PROCESS_LOCK_KEY``) + 1 ``ebull_job_request`` LISTEN + 1
``ebull_credential_health`` LISTEN. Heartbeat writers open a short-lived
conn per beat (not held) and ``BackgroundScheduler`` uses no persistent
jobstore, so neither adds a long-lived slot.

EXPECTED, not observed: #1472's RCA saw ``credential_health`` LISTEN ×3
(a duplicate-instance bug PR3 fixes). The budget models the intended
topology so it never blesses that bug."""

JOBS_STEADY_STATE_EXEC_CONNS: Final[int] = 1
"""Models the ONE always-running NON-sec_rate job's ``JobLock`` advisory-lock
connection.  The jobs process almost always has ≥1 non-sec_rate scheduled
job executing (``orchestrator_high_frequency_sync`` every 5 min being the
canonical example), and each such job holds one ``JobLock`` advisory-lock
connection for its whole body (``app/jobs/locks.py`` ``JobLock.__enter__``
opens it, ``__exit__`` closes it).

``sec_rate`` jobs are NOT counted here — after #1542 they route through the
in-process ``SecLaneGate`` and open **zero** ``JobLock`` connections.  Their
concurrent bodies are charged separately via ``SEC_LANE_MAX_CONCURRENCY``
(``app/jobs/sec_lane_gate.py``), so the two terms are additive and
non-overlapping (no double-count).

This term is per-execution, not process-lifetime — it is NOT a
``JOBS_FIXED_LONGLIVED_CONNS`` member — but ≥1 is live at steady state
(the RCA idle snapshot caught exactly 1), so the budget counts one.
Concurrent execution beyond 1 is part of the cadence-boundary burst that
PR2/PR4 bound, NOT PR1 (Codex ckpt-2)."""

ORCHESTRATOR_GATE_CHECK_CONN: Final[int] = 1
"""The sync orchestrator's ``_run_layers_loop`` holds ONE run-scoped
autocommit connection for the per-layer read gate-checks (cancel poll,
credential health, layer init, dependency lookup) for the whole walk
(#1472 PR4a — replaces the prior fresh-connect-per-check-per-layer herd).
The orchestrator high-frequency sync fires every 5 min and is frequently
the steady-state executing job, so the budget counts this slot as held
alongside ``JOBS_STEADY_STATE_EXEC_CONNS`` (the same job's JobLock conn).
Conservative: the slot is released between walks (``finally: gate_conn.close``),
so counting it always over-estimates. Single sync run at a time (advisory
lock + sync_runs single-running unique index) bounds it to exactly one."""

CONNECTION_BUDGET_RESERVE: Final[int] = 3
"""Headroom over the steady-state baseline for transient connections
that briefly coexist with it: serialized boot singleton-probes, the
fence-reaper probe, concurrent heartbeat beats. NOT headroom for the
cadence-boundary per-fire herd — that demand is removed by PR2/PR4 of
#1472, not absorbed here. Sized so the real dev config passes with margin
while a genuinely over-budget pool change still trips the guard."""

CONNECTION_BUDGET_OVERRIDE_ENV: Final[str] = "EBULL_ALLOW_OVER_BUDGET_CONNS"
"""Operator escape hatch for niche dev/CI clusters with an atypically low
``max_connections`` (e.g. a shared CI Postgres). Setting it to ``"1"``
downgrades the hard-fail to a loud WARNING. It does NOT mean 'raise
max_connections' — that is rejected in #1472 (adds backend RAM + WAL
pressure on the OOM-fragile box); the remediation is to SHRINK demand."""


# ---------------------------------------------------------------------------
# Listener connection labels + expected cardinality (#1472 PR3)
# ---------------------------------------------------------------------------
#
# Each LISTEN connection eBull opens is stamped with a distinct
# ``application_name`` so ``pg_stat_activity`` shows ownership and a
# cardinality probe (``/system/postgres-health``) can detect a
# duplicate-instance listener. The #1472 RCA observed
# ``ebull_credential_health`` LISTEN ×3 where the intended topology is 2
# (one per subscribing process: API + jobs). That over-count was a
# reconnect-overlap artifact of the restart-herd crash-loop
# (``listener_restarts=1439``), not a genuine double-start — PR0's
# connect-timeout already removed the crash-loop. PR3 makes the count
# observable + asserts it so a real future duplicate surfaces immediately.

JOB_REQUEST_LISTENER_APPLICATION_NAME: Final[str] = "ebull-jobs-job-request-listener"
"""``application_name`` for the jobs-process ``LISTEN ebull_job_request``
connection (``app/jobs/listener.py``). Exactly one, in the jobs process."""

JOBS_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME: Final[str] = "ebull-jobs-credential-health-listener"
"""``application_name`` for the jobs-process ``LISTEN ebull_credential_health``
connection (``app/jobs/credential_health_listener.py`` started from
``app/jobs/__main__.py``). Exactly one, in the jobs process."""

API_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME: Final[str] = "ebull-api-credential-health-listener"
"""``application_name`` for the API-process ``LISTEN ebull_credential_health``
connection (started from ``app/main.py`` lifespan). Exactly one, in the API
process."""

LISTENER_APPLICATION_NAMES: Final[frozenset[str]] = frozenset(
    {
        JOB_REQUEST_LISTENER_APPLICATION_NAME,
        JOBS_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME,
        API_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME,
    }
)
"""Every labelled LISTEN connection. In a healthy single-instance topology
each name appears AT MOST ONCE in ``pg_stat_activity`` (0 when that process
is not running, e.g. the jobs labels when only the API is up). A count > 1
for any name is a duplicate-instance listener — surfaced warn-only via
``/system/postgres-health`` (a transient reconnect can momentarily show 2,
so the probe is informational, not a hard gate)."""


def _dev_profile_connection_demand() -> int:
    """Steady-state connection demand of the dev single-box profile
    (API + jobs co-deployed), excluding the transient reserve.

    Conservative co-deployment model (Codex ckpt-1 #1): counts BOTH
    processes' configured pool maxes + their fixed long-lived conns,
    regardless of which process is booting. The peer term is bounded and
    documented, so counting it always over-estimates rather than failing
    one process for capacity the other is not currently using.

    #1542 — the ``sec_rate`` lane is now an in-process semaphore (no per-job
    JobLock connection). Up to ``SEC_LANE_MAX_CONCURRENCY`` sec_rate job
    BODIES run concurrently, each holding ONE raw body connection. Charge all
    N explicitly: before #1542 only one job ran at a time (its body conn
    absorbed by the reserve); now N>1 sec bodies are first-class demand.

    #1484 — the SEC rate gate (PostgresFloorGate) borrows ONE pooled conn per
    acquire for ~1 ms under a process-local lock, then releases before
    sleeping. Sync and async acquires share the SAME threading.Lock inside
    _reserve_sync (the async path runs it via run_in_executor), so at most ONE
    gate conn is held per process at a time (<=2 across API+jobs). This is
    transient pool usage, not steady-state demand: it does not raise any pool
    max, so the returned total is unchanged, well within the reserve.
    """
    from app.jobs.sec_lane_gate import SEC_LANE_MAX_CONCURRENCY

    return (
        DB_POOL_MAX_SIZE
        + AUDIT_POOL_MAX_SIZE
        + API_FIXED_LONGLIVED_CONNS
        + JOBS_POOL_MAX_SIZE
        + BACKGROUND_POOL_MAX_SIZE
        + JOBS_FIXED_LONGLIVED_CONNS
        + JOBS_STEADY_STATE_EXEC_CONNS
        + ORCHESTRATOR_GATE_CHECK_CONN
        + SEC_LANE_MAX_CONCURRENCY
    )


class ConnectionBudgetExceeded(RuntimeError):
    """Raised at boot when configured connection demand exceeds the usable
    budget (``max_connections − superuser_reserved_connections``) and the
    operator has not set the override env var.

    The message steers the operator to the ONLY sanctioned remediation —
    shrink configured demand — and explicitly names raising
    ``max_connections`` as diagnostic-only (#1472)."""

    def __init__(self, *, process: Literal["api", "jobs"], demand: int, usable: int) -> None:
        super().__init__(
            f"connection budget exceeded at {process} boot: dev-profile demand "
            f"{demand} > usable {usable} (max_connections − "
            f"superuser_reserved_connections). The dev box co-deploys API + jobs "
            f"+ Postgres; the configured pools cannot fit. Primary fix: SHRINK "
            f"pool sizes (app/db/pool.py: DB_POOL_MAX_SIZE / AUDIT_POOL_MAX_SIZE "
            f"/ JOBS_POOL_MAX_SIZE / BACKGROUND_POOL_MAX_SIZE) or stop duplicate "
            f"processes. Raising "
            f"max_connections is DIAGNOSTIC-ONLY, not a remediation — it adds "
            f"backend RAM + WAL pressure on the OOM-fragile dev box (#1472). Set "
            f"{CONNECTION_BUDGET_OVERRIDE_ENV}=1 to boot anyway (niche "
            f"low-max_connections CI only; expect saturation under load)."
        )
        self.process = process
        self.demand = demand
        self.usable = usable


def check_connection_budget(
    conn: psycopg.Connection[Any],
    *,
    process: Literal["api", "jobs"],
) -> tuple[bool, int, int]:
    """Probe the usable budget; return ``(passes, demand, usable)``.

    ``usable = max_connections − superuser_reserved_connections`` (live
    SHOW). ``demand = dev-profile steady-state demand + reserve``.

    Fail-open on SHOW exception (returns ``(True, 0, 0)``), mirroring
    ``check_max_locks_per_transaction``: a transient SHOW failure must not
    block startup — the probe is informational, and genuine saturation
    would surface at runtime anyway.

    ``process`` only labels the diagnostics/message; the asserted
    inequality is the same co-deployment total in both boot paths.
    """
    try:
        max_row = conn.execute("SHOW max_connections").fetchone()
        reserved_row = conn.execute("SHOW superuser_reserved_connections").fetchone()
    except Exception:
        logger.warning(
            "pg_settings: SHOW max_connections/superuser_reserved_connections failed; skipping connection-budget guard",
            exc_info=True,
        )
        return True, 0, 0
    if max_row is None or reserved_row is None:
        return True, 0, 0
    usable = int(max_row[0]) - int(reserved_row[0])
    demand = _dev_profile_connection_demand() + CONNECTION_BUDGET_RESERVE
    return demand <= usable, demand, usable


def enforce_connection_budget(
    conn: psycopg.Connection[Any],
    *,
    process: Literal["api", "jobs"],
) -> None:
    """Hard-fail wrapper. Raises ``ConnectionBudgetExceeded`` when the
    configured demand cannot fit the usable budget and the operator has
    not set ``EBULL_ALLOW_OVER_BUDGET_CONNS=1`` (loud-WARNING bypass)."""
    passes, demand, usable = check_connection_budget(conn, process=process)
    if passes:
        return
    if os.environ.get(CONNECTION_BUDGET_OVERRIDE_ENV) == "1":
        logger.warning(
            "pg_settings: connection demand %d > usable %d at %s boot; running anyway because %s=1 is set",
            demand,
            usable,
            process,
            CONNECTION_BUDGET_OVERRIDE_ENV,
        )
        return
    raise ConnectionBudgetExceeded(process=process, demand=demand, usable=usable)
