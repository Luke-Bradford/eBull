"""Migration runner.

Scans sql/ for files named NNN_*.sql in lexicographic order.
Tracks applied migrations in a schema_migrations table.

Each migration file is applied using psycopg.ClientCursor, which uses the
simple query protocol and allows multi-statement files (CREATE TABLE + CREATE
INDEX, etc.). The default psycopg3 cursor uses the extended query protocol,
which rejects multiple statements in a single execute() call.
"""

import hashlib
import logging
from pathlib import Path

import psycopg

from app.config import settings

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql"

CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename       TEXT PRIMARY KEY,
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_sha256 TEXT
);
"""

# #1333 — ``CREATE TABLE IF NOT EXISTS`` does NOT add columns to a
# pre-existing table (prevention log), so existing installs need an
# explicit, idempotent ALTER alongside the CREATE.
ADD_CONTENT_SHA_COLUMN = """
ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS content_sha256 TEXT;
"""

# Per-file directive: when a migration's first non-blank line is exactly this
# string, the runner opens an autocommit connection so each statement runs in
# its own implicit transaction. Required for migrations containing commands
# Postgres forbids inside a transaction block (notably ``ALTER SYSTEM``).
# Strict line-1 check (Codex 1b LOW): looser parsing could false-positive on
# a migration whose body literally contains the directive string.
AUTOCOMMIT_DIRECTIVE = "-- runner: autocommit"


def _wants_autocommit(sql: str) -> bool:
    """Return True iff the migration file declares autocommit on line 1."""
    lines = sql.lstrip().splitlines()
    return bool(lines) and lines[0].strip() == AUTOCOMMIT_DIRECTIVE


def _split_autocommit_statements(sql_text: str) -> list[str]:
    """Split an autocommit migration into single statements.

    Under autocommit mode, ``psycopg.ClientCursor.execute`` on a
    multi-statement string still wraps the batch in an implicit
    transaction (PG raises ``ALTER SYSTEM cannot run inside a
    transaction block``). So autocommit migrations must execute each
    statement separately.

    Strategy: strip ``--`` line comments first (so any ``;`` characters
    inside English prose comments don't false-split), then naive
    ``;``-split. Autocommit migrations MUST NOT contain dollar-quoted
    (``$$ ... $$``) blocks or string literals containing semicolons.
    The reference migration ``sql/155_postgres_runtime_tuning.sql`` is
    plain ``ALTER SYSTEM`` + ``SELECT pg_reload_conf()`` lines, so this
    constraint is easy to meet; document the constraint in any new
    autocommit migration.
    """
    # Strip ``--`` line comments first. A ``--`` inside a string literal
    # would be miscategorised here, but autocommit migrations are
    # forbidden from containing string literals with semicolons anyway,
    # and the small target surface (ALTER SYSTEM, SELECT pg_reload_conf)
    # never needs string literals with ``--``.
    cleaned_lines: list[str] = []
    for line in sql_text.splitlines():
        idx = line.find("--")
        if idx >= 0:
            line = line[:idx]
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)

    statements: list[str] = []
    for chunk in cleaned.split(";"):
        stripped = chunk.strip()
        if stripped:
            statements.append(stripped)
    return statements


def _migration_files() -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        logger.warning("No SQL migration files found in %s -- is the sql/ directory missing?", MIGRATIONS_DIR)
    return files


def _already_applied(conn: psycopg.Connection) -> dict[str, str | None]:  # type: ignore[type-arg]
    """Map of applied filename → stored content_sha256 (None on legacy rows)."""
    return {row[0]: row[1] for row in conn.execute("SELECT filename, content_sha256 FROM schema_migrations")}


def _content_sha256(path: Path) -> str:
    """SHA-256 of the migration file's raw bytes.

    Raw bytes (not normalised text) deliberately: a line-ending or
    encoding change to an applied file is still a content change the
    operator must resolve via a new ``NNN+1`` file or a manual replay.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_migrations() -> list[str]:
    """Apply all pending migrations synchronously. Returns list of applied filenames.

    #1333 — content-drift guard: every applied row stores the file's
    SHA-256. On a later run, a hash mismatch on an already-applied file
    raises instead of silently skipping — editing an applied migration
    leaves the DB permanently diverged from the file (the runner keys
    on filename only), which surfaced as the sql/171 partial-application
    WARN. Legacy rows (NULL hash) are backfilled one-time from the
    current file content.
    """
    # Bootstrap: ensure tracking table exists. ClientCursor for consistency.
    with psycopg.connect(settings.database_url) as bootstrap:
        with psycopg.ClientCursor(bootstrap) as cur:
            cur.execute(CREATE_TRACKING_TABLE)
            cur.execute(ADD_CONTENT_SHA_COLUMN)
        bootstrap.commit()

    applied: list[str] = []
    files = _migration_files()

    if not files:
        return applied

    # #1333 — single pre-pass over every already-applied file on one
    # connection: backfill legacy NULL hashes, and fail on ANY drift
    # BEFORE applying anything new (a later pending migration must not
    # half-apply on a run that is about to abort on a drifted earlier
    # file).
    with psycopg.connect(settings.database_url) as reader:
        done = _already_applied(reader)
        for path in files:
            if path.name not in done:
                continue
            current_sha = _content_sha256(path)
            stored_sha = done[path.name]
            if stored_sha is None:
                # One-time backfill of rows recorded before the drift
                # guard existed. Trusts the current file content — the
                # guard starts enforcing from the next run.
                reader.execute(
                    "UPDATE schema_migrations SET content_sha256 = %s WHERE filename = %s AND content_sha256 IS NULL",
                    (current_sha, path.name),
                )
                logger.info("Backfilled content_sha256 for legacy migration row: %s", path.name)
            elif stored_sha != current_sha:
                logger.critical(
                    "Migration content drift: %s changed since it was applied (stored sha256 %s, file sha256 %s)",
                    path.name,
                    stored_sha,
                    current_sha,
                )
                raise RuntimeError(
                    f"Migration {path.name} content changed since applied. Either bump the change "
                    "to a new NNN+1 file, or manually replay the edited file and reset the "
                    "schema_migrations row (UPDATE schema_migrations SET content_sha256 = NULL "
                    f"WHERE filename = '{path.name}') so the next run re-records it."
                )
        reader.commit()

    for path in files:
        if path.name in done:
            logger.debug("Migration already applied: %s", path.name)
            continue

        logger.info("Applying migration: %s", path.name)
        sql = path.read_text(encoding="utf-8")

        # ClientCursor uses the simple query protocol, which allows multiple
        # statements in one execute() call. This is the correct way to run
        # real migration files in psycopg3.
        #
        # autocommit-directive (#1208 T0): migrations whose first non-blank
        # line is ``-- runner: autocommit`` are applied with autocommit=True
        # so each statement runs in its own implicit transaction. Required
        # for ALTER SYSTEM (forbidden inside a tx block). On partial failure
        # (body succeeds, schema_migrations INSERT fails) the next boot
        # re-runs the body; safe because such migrations must be idempotent.
        autocommit = _wants_autocommit(sql)
        with psycopg.connect(settings.database_url, autocommit=autocommit) as conn:
            try:
                with psycopg.ClientCursor(conn) as cur:
                    if autocommit:
                        # Multi-statement execute under autocommit STILL
                        # opens an implicit transaction at the protocol
                        # level. Split and run each statement separately
                        # so non-transactional commands like ALTER
                        # SYSTEM are accepted.
                        for stmt in _split_autocommit_statements(sql):
                            cur.execute(stmt)  # type: ignore[call-overload]
                    else:
                        cur.execute(sql)  # type: ignore[call-overload]
                    cur.execute(  # type: ignore[call-overload]
                        "INSERT INTO schema_migrations (filename, content_sha256) VALUES (%s, %s)",
                        (path.name, _content_sha256(path)),
                    )
                if not autocommit:
                    conn.commit()
                logger.info("Applied: %s%s", path.name, " (autocommit)" if autocommit else "")
                applied.append(path.name)
            except Exception:
                if not autocommit:
                    conn.rollback()
                logger.exception(
                    "Migration failed: %s -- %s",
                    path.name,
                    "partial in autocommit mode (re-run will replay body; ensure migration is idempotent)"
                    if autocommit
                    else "rolled back",
                )
                raise

    return applied


def migration_status(conn: psycopg.Connection[object] | None = None) -> list[dict[str, str]]:
    """Return status of every migration file: applied or pending.

    If *conn* is provided, uses that connection.  Otherwise opens a raw
    connection (for CLI/startup contexts where no pool exists yet).

    Raises psycopg.OperationalError if the database is unreachable.
    Callers are responsible for handling connection failures.
    """
    files = _migration_files()

    def _query_applied(c: psycopg.Connection[object]) -> dict[str, str]:
        try:
            return {
                row[0]: row[1].isoformat() if row[1] else ""  # type: ignore[index]  # TupleRow
                for row in c.execute("SELECT filename, applied_at FROM schema_migrations ORDER BY filename")
            }
        except psycopg.errors.UndefinedTable:
            return {}

    if conn is not None:
        applied = _query_applied(conn)
    else:
        with psycopg.connect(settings.database_url) as fallback_conn:
            applied = _query_applied(fallback_conn)

    return [
        {
            "file": p.name,
            "status": "applied" if p.name in applied else "pending",
            "applied_at": applied.get(p.name, ""),
        }
        for p in files
    ]
