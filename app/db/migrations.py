"""Migration runner.

Scans sql/ for files named NNN_*.sql in lexicographic order.
Tracks applied migrations in a schema_migrations table.

Each migration file is applied using psycopg.ClientCursor, which uses the
simple query protocol and allows multi-statement files (CREATE TABLE + CREATE
INDEX, etc.). The default psycopg3 cursor uses the extended query protocol,
which rejects multiple statements in a single execute() call.
"""

import logging
from pathlib import Path

import psycopg

from app.config import settings

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql"

CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _migration_files() -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        logger.warning("No SQL migration files found in %s -- is the sql/ directory missing?", MIGRATIONS_DIR)
    return files


def _already_applied(conn: psycopg.Connection) -> set[str]:  # type: ignore[type-arg]
    return {row[0] for row in conn.execute("SELECT filename FROM schema_migrations")}


def run_migrations() -> list[str]:
    """Apply all pending migrations synchronously. Returns list of applied filenames."""
    # Bootstrap: ensure tracking table exists. ClientCursor for consistency.
    with psycopg.connect(settings.database_url) as bootstrap:
        with psycopg.ClientCursor(bootstrap) as cur:
            cur.execute(CREATE_TRACKING_TABLE)
        bootstrap.commit()

    applied: list[str] = []
    files = _migration_files()

    if not files:
        return applied

    with psycopg.connect(settings.database_url) as reader:
        done = _already_applied(reader)

    for path in files:
        if path.name in done:
            logger.debug("Migration already applied: %s", path.name)
            continue

        logger.info("Applying migration: %s", path.name)
        sql = path.read_text(encoding="utf-8")

        # ClientCursor uses the simple query protocol, which allows multiple
        # statements in one execute() call. This is the correct way to run
        # real migration files in psycopg3.
        with psycopg.connect(settings.database_url) as conn:
            try:
                with psycopg.ClientCursor(conn) as cur:
                    cur.execute(sql)  # type: ignore[call-overload]
                    cur.execute(  # type: ignore[call-overload]
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (path.name,),
                    )
                conn.commit()
                logger.info("Applied: %s", path.name)
                applied.append(path.name)
            except Exception:
                conn.rollback()
                logger.exception("Migration failed: %s -- rolled back", path.name)
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
