"""Postgres health metrics collection (#1208 Phase 4 Sub 4).

Surfaces the five operator signals that Phases 1-3 of #1208 lacked a
live readout for:

- `pg_database_size('ebull')` against a 10 GB warn threshold (matches
  the pre-push hook gate at `.githooks/pre-push`).
- Leaked `ebull_test_*` DB count + names (Phase 2 enforced zero leaks
  but had no operator surface).
- WAL: `wal_dir_bytes` (size of pg_wal/ on disk) against a 4 GB
  absolute bloat alarm (effective `max_wal_size=1 GB` via the
  docker-compose `command:` flag, #1410), plus
  `wal_since_checkpoint_bytes` as the informational burst-pressure
  signal.
- Autovacuum top-10 by `n_dead_tup` (Phase 3's partition + retention
  motivation).
- `financial_facts_raw_default` row count against the 5000-row alarm
  (Phase 3 §4.1.1 — parser-junk growth detector).

Service-no-commit invariant + autocommit-conn contract: the service
opens its OWN connection with `autocommit=True` so each of the seven
metric queries runs in its own implicit tx. Without autocommit, a
single SQL error (e.g. `pg_monitor` role required for `pg_ls_waldir`
on a non-superuser DB) puts the entire tx in `ABORTED` state and
every subsequent query fails with `current transaction is aborted`.

See `docs/superpowers/specs/2026-05-19-phase4-postgres-health.md` for
the full design + Codex 1a/1b iteration history.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg

from app.config import settings

logger = logging.getLogger(__name__)


# ── Thresholds — single source of truth ──────────────────────────
#
# DB_SIZE_WARN_BYTES is also pinned in `.githooks/pre-push`. The
# `tests/test_pre_push_hook_bloat_warn.py::test_pre_push_hook_threshold_matches_db_size_warn`
# test asserts the two stay aligned by parsing the hook file.

DB_SIZE_WARN_BYTES: int = 10 * 1024 * 1024 * 1024  # 10 GB
# 4 GB absolute pg_wal disk-bloat alarm. Effective max_wal_size is 1 GB
# (docker-compose `command:` flag, #1410), so steady-state pg_wal sits
# well below this even during bootstrap write bursts; crossing 4 GB
# signals a stuck WAL archiver / replication slot or runaway WAL, not
# normal checkpoint pressure.
WAL_WARN_BYTES: int = 4 * 1024 * 1024 * 1024  # 4 GB
DEFAULT_PARTITION_WARN_ROWS: int = 5000


# ── Data shapes ──────────────────────────────────────────────────


@dataclass(frozen=True)
class AutovacuumTableLag:
    relname: str
    last_autovacuum: datetime | None
    last_analyze: datetime | None
    n_dead_tup: int
    n_live_tup: int
    dead_fraction: float | None  # dead / (dead + live), bounded [0, 1]; null when both zero


@dataclass(frozen=True)
class PostgresHealthSnapshot:
    db_size_bytes: int | None
    db_size_pretty: str | None
    db_size_warn_threshold_bytes: int
    db_size_breached_warn: bool | None
    leaked_test_db_count: int | None
    leaked_test_db_names: list[str] | None
    # #1444 — total on-disk size of leaked ``ebull_test_*`` DBs. Bloat
    # visibility: a large value warns the operator BEFORE the next crash
    # recovery has to fsync-walk those files (the multi-hour-stall risk).
    # Size is the connection-free proxy for file/relation count —
    # counting relations would require connecting to each leaked DB,
    # which can hang on a ``datconnlimit=-2`` corpse (#1393).
    leaked_test_db_total_bytes: int | None
    leaked_test_db_total_pretty: str | None
    wal_dir_bytes: int | None
    wal_dir_pretty: str | None
    wal_since_checkpoint_bytes: int | None
    wal_warn_threshold_bytes: int
    wal_breached_warn: bool | None  # keys on wal_dir_bytes
    last_checkpoint_at: datetime | None
    autovacuum_top10: list[AutovacuumTableLag] | None
    financial_facts_raw_default_rows: int | None
    financial_facts_raw_default_warn_threshold: int
    financial_facts_raw_default_breached_warn: bool | None
    metric_errors: list[str]
    collected_at: datetime


# ── Per-metric query callables ───────────────────────────────────


def _safe[T](
    conn: psycopg.Connection[tuple],
    name: str,
    fn: Callable[[psycopg.Connection[tuple]], T],
    errors: list[str],
) -> T | None:
    """Run one metric probe under autocommit isolation. On any failure,
    append a `<name>: <ExceptionClassName>` line to `errors` and
    return None. The autocommit conn keeps a failure local to this
    single query — the next probe still works.

    Catches `Exception` (not just `psycopg.Error`) so a defensive
    `assert row is not None` inside a probe, a `KeyError` on an
    unexpected column shape, or any other non-DB exception still
    flows through the isolation wrapper instead of escaping to the
    API handler as an unhandled 500 (bot review #1216).
    """
    try:
        return fn(conn)
    except Exception as exc:  # noqa: BLE001 — isolation wrapper, see docstring
        msg = f"{name}: {type(exc).__name__}"
        logger.warning("postgres_health probe %s failed: %s", name, exc)
        errors.append(msg)
        return None


def _q_db_size(conn: psycopg.Connection[tuple]) -> tuple[int, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_database_size(current_database()),        pg_size_pretty(pg_database_size(current_database()))"
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def _q_leaked_test_dbs(conn: psycopg.Connection[tuple]) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            # ``_`` is a LIKE wildcard; escape it (ESCAPE '!') so the
            # predicate matches the literal ``ebull_test_`` prefix only —
            # consistent with force_drop_invalid_test_dbs (bot #1446).
            "SELECT datname FROM pg_database "
            " WHERE datname LIKE 'ebull!_test!_%' ESCAPE '!' "
            "   AND datname != 'ebull_test_template' "
            " ORDER BY datname"
        )
        return [str(r[0]) for r in cur.fetchall()]


def _q_leaked_test_db_bytes(conn: psycopg.Connection[tuple]) -> tuple[int, str]:
    """Total on-disk size of leaked ``ebull_test_*`` DBs (#1444).

    Guarded by a short ``statement_timeout`` so a wedged
    ``pg_database_size`` on a ``datconnlimit=-2`` corpse (the #1393 hang)
    surfaces as a caught timeout via the ``_safe`` wrapper instead of
    blocking the whole health endpoint. The conn is autocommit + reused
    across probes, so the timeout is reset in a ``finally``.
    """
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '2s'")
        try:
            cur.execute(
                # ESCAPE '!' so the literal ``_`` are matched, not LIKE
                # wildcards (bot #1446; mirrors _q_leaked_test_dbs above).
                "SELECT COALESCE(sum(pg_database_size(datname)), 0)::bigint, "
                "       pg_size_pretty(COALESCE(sum(pg_database_size(datname)), 0)::bigint) "
                "  FROM pg_database "
                " WHERE datname LIKE 'ebull!_test!_%' ESCAPE '!' "
                "   AND datname != 'ebull_test_template'"
            )
            row = cur.fetchone()
        finally:
            # RESET (not ``SET = 0``) restores the role/cluster default
            # rather than hard-disabling the timeout for the autocommit
            # conn's later probes (Codex #1444).
            cur.execute("RESET statement_timeout")
    assert row is not None
    return int(row[0]), str(row[1])


def _q_wal_dir(conn: psycopg.Connection[tuple]) -> tuple[int, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(sum(size), 0)::bigint, "
            "       pg_size_pretty(COALESCE(sum(size), 0)::bigint) "
            "  FROM pg_ls_waldir()"
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def _q_wal_since_checkpoint(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), redo_lsn)::bigint   FROM pg_control_checkpoint()")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _q_last_checkpoint(conn: psycopg.Connection[tuple]) -> datetime:
    with conn.cursor() as cur:
        cur.execute("SELECT checkpoint_time FROM pg_control_checkpoint()")
        row = cur.fetchone()
    assert row is not None
    return row[0]  # type: ignore[no-any-return]


def _q_autovacuum_top10(conn: psycopg.Connection[tuple]) -> list[AutovacuumTableLag]:
    # ORDER BY ... LIMIT 10 still requires a full scan + top-K sort
    # over pg_stat_user_tables. Cheap on our ~100-table dev cluster;
    # revisit if the user-table count balloons (Codex 1b LOW #1).
    with conn.cursor() as cur:
        cur.execute(
            "SELECT relname, last_autovacuum, last_analyze, "
            "       n_dead_tup, n_live_tup "
            "  FROM pg_stat_user_tables "
            " ORDER BY n_dead_tup DESC NULLS LAST "
            " LIMIT 10"
        )
        rows = cur.fetchall()
    out: list[AutovacuumTableLag] = []
    for relname, last_av, last_an, n_dead, n_live in rows:
        n_dead_i = int(n_dead) if n_dead is not None else 0
        n_live_i = int(n_live) if n_live is not None else 0
        denom = n_dead_i + n_live_i
        ratio = (n_dead_i / denom) if denom > 0 else None
        out.append(
            AutovacuumTableLag(
                relname=str(relname),
                last_autovacuum=last_av,
                last_analyze=last_an,
                n_dead_tup=n_dead_i,
                n_live_tup=n_live_i,
                dead_fraction=ratio,
            )
        )
    return out


def _q_default_partition_rows(conn: psycopg.Connection[tuple]) -> int:
    # Full seq scan on the DEFAULT partition. Current size ~1055 rows
    # post-Phase-3; scan stays cheap until the 5000-row alarm fires,
    # at which point the operator has bigger problems than scan cost.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM financial_facts_raw_default")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


# ── Public entrypoint ────────────────────────────────────────────


def collect_postgres_health(
    *,
    database_url: str | None = None,
    db_size_warn_threshold_bytes: int = DB_SIZE_WARN_BYTES,
    wal_warn_threshold_bytes: int = WAL_WARN_BYTES,
    default_partition_warn_rows: int = DEFAULT_PARTITION_WARN_ROWS,
) -> PostgresHealthSnapshot:
    """Collect every PG health metric into one snapshot.

    Opens its own `psycopg.connect(url, autocommit=True)` so each of
    the seven metric queries runs in its own implicit tx — a SQL
    error on one metric never aborts the others.

    Thresholds are injectable for tests (above-threshold assertions
    on a small test DB need a low threshold). Production callers pass
    no overrides; defaults are the module-level constants.

    A connection-level failure (psycopg cannot open the conn at all)
    propagates as `psycopg.Error` — the API layer translates it into
    a 503 per the fail-closed posture documented at
    `app/api/system.py:24`.
    """
    url = database_url or settings.database_url
    errors: list[str] = []

    db_size_bytes: int | None = None
    db_size_pretty: str | None = None
    leaked_names: list[str] | None = None
    leaked_bytes: int | None = None
    leaked_pretty: str | None = None
    wal_dir_bytes: int | None = None
    wal_dir_pretty: str | None = None
    wal_since_ckpt: int | None = None
    last_ckpt: datetime | None = None
    top10: list[AutovacuumTableLag] | None = None
    default_rows: int | None = None

    with psycopg.connect(url, autocommit=True) as conn:
        result = _safe(conn, "db_size", _q_db_size, errors)
        if result is not None:
            db_size_bytes, db_size_pretty = result

        leaked_names = _safe(conn, "leaked_test_dbs", _q_leaked_test_dbs, errors)

        leaked_size = _safe(conn, "leaked_test_db_bytes", _q_leaked_test_db_bytes, errors)
        if leaked_size is not None:
            leaked_bytes, leaked_pretty = leaked_size

        wal_result = _safe(conn, "wal_dir", _q_wal_dir, errors)
        if wal_result is not None:
            wal_dir_bytes, wal_dir_pretty = wal_result

        wal_since_ckpt = _safe(conn, "wal_since_checkpoint", _q_wal_since_checkpoint, errors)
        last_ckpt = _safe(conn, "last_checkpoint", _q_last_checkpoint, errors)
        top10 = _safe(conn, "autovacuum_top10", _q_autovacuum_top10, errors)
        default_rows = _safe(conn, "default_partition_rows", _q_default_partition_rows, errors)

    db_size_breached: bool | None = None if db_size_bytes is None else db_size_bytes > db_size_warn_threshold_bytes
    wal_breached: bool | None = None if wal_dir_bytes is None else wal_dir_bytes > wal_warn_threshold_bytes
    default_breached: bool | None = None if default_rows is None else default_rows > default_partition_warn_rows
    leaked_count: int | None = None if leaked_names is None else len(leaked_names)

    return PostgresHealthSnapshot(
        db_size_bytes=db_size_bytes,
        db_size_pretty=db_size_pretty,
        db_size_warn_threshold_bytes=db_size_warn_threshold_bytes,
        db_size_breached_warn=db_size_breached,
        leaked_test_db_count=leaked_count,
        leaked_test_db_names=leaked_names,
        leaked_test_db_total_bytes=leaked_bytes,
        leaked_test_db_total_pretty=leaked_pretty,
        wal_dir_bytes=wal_dir_bytes,
        wal_dir_pretty=wal_dir_pretty,
        wal_since_checkpoint_bytes=wal_since_ckpt,
        wal_warn_threshold_bytes=wal_warn_threshold_bytes,
        wal_breached_warn=wal_breached,
        last_checkpoint_at=last_ckpt,
        autovacuum_top10=top10,
        financial_facts_raw_default_rows=default_rows,
        financial_facts_raw_default_warn_threshold=default_partition_warn_rows,
        financial_facts_raw_default_breached_warn=default_breached,
        metric_errors=errors,
        collected_at=datetime.now(UTC),
    )
