"""PostgreSQL configuration guards (#1187).

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
from typing import Any, Final

import psycopg

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
