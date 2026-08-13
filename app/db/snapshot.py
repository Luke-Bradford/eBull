"""Snapshot-consistent multi-query reads (#395).

The ``get_conn`` pool yields connections in READ COMMITTED isolation
(see ``docs/review-prevention-log.md`` — every statement re-reads the
latest committed snapshot). Multi-query read handlers therefore see
a fresh snapshot per statement, which lets a concurrent writer slip
between query 1 and query 2 and produces brief count/list drift.

``snapshot_read(conn)`` opens a single REPEATABLE READ transaction so
all reads inside the block run against one consistent snapshot. The
helper is for read-only handlers; writers should use
``conn.transaction()`` directly so the default READ COMMITTED applies
(REPEATABLE READ would force serialization-failure retries on
concurrent writes, which is not what most write handlers want).

Usage::

    from app.db.snapshot import snapshot_read

    @router.get("/example")
    def example(conn: psycopg.Connection = Depends(get_conn)):
        with snapshot_read(conn):
            row1 = conn.execute("SELECT ...").fetchone()
            row2 = conn.execute("SELECT ...").fetchone()
        return ...

The connection's prior ``isolation_level`` is captured and restored on
exit so the pool returns it to its default state. psycopg3 sets
isolation as a connection-level property that takes effect on the
next transaction; we set it before opening the transaction and
restore it afterward.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import IsolationLevel


@contextmanager
def snapshot_read(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Run a block of reads inside one REPEATABLE READ transaction.

    Pool connections from ``get_conn`` are autocommit-off, so any
    statement issued before this helper (e.g. an operator-id lookup
    in a FastAPI dependency) silently opens an implicit READ
    COMMITTED transaction. Entering ``conn.transaction()`` while one
    is already active produces a SAVEPOINT — the
    ``isolation_level`` change applies only to the *next* top-level
    transaction, so the snapshot would silently inherit READ
    COMMITTED. Codex review on PR for #395.

    Mitigation: commit any pending implicit transaction before
    setting isolation. Read-only by contract: the caller MUST NOT
    issue writes inside the block, and prior reads on the same
    connection are committable no-ops, so the upfront commit is
    safe.

    The connection's prior isolation level is restored on exit so
    pool reuse does not leak REPEATABLE READ. psycopg3's
    isolation_level is a connection-level setting that persists
    across transactions until changed.
    """
    # Commit (no-op for read-only state) any implicit transaction
    # opened by an earlier statement on this connection — without
    # this, conn.transaction() below opens a savepoint instead of a
    # new top-level transaction, and the isolation change never
    # takes effect.
    conn.commit()
    prior = conn.isolation_level
    conn.isolation_level = IsolationLevel.REPEATABLE_READ
    try:
        with conn.transaction():
            yield
    finally:
        conn.isolation_level = prior
