"""Migration runner.

Scans sql/ for files named NNN_*.sql in lexicographic order.
Tracks applied migrations in a schema_migrations table.

Each migration file is applied via the simple query protocol (autocommit=True
connection with explicit BEGIN/COMMIT) so multi-statement files work correctly.
psycopg3's default extended-query protocol rejects multiple statements in a
single execute() call, which would silently break real migration files.
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
    # Bootstrap: ensure the tracking table exists (DDL, autocommit safe).
    with psycopg.connect(settings.database_url, autocommit=True) as bootstrap:
        bootstrap.execute(CREATE_TRACKING_TABLE)

    applied: list[str] = []
    files = _migration_files()

    if not files:
        return applied

    with psycopg.connect(settings.database_url, autocommit=True) as reader:
        done = _already_applied(reader)

    for path in files:
        if path.name in done:
            logger.debug("Migration already applied: %s", path.name)
            continue

        logger.info("Applying migration: %s", path.name)
        sql = path.read_text(encoding="utf-8")

        # autocommit=True + explicit BEGIN/COMMIT uses the simple query protocol,
        # which allows multi-statement files (CREATE TABLE + CREATE INDEX, etc.).
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            conn.execute("BEGIN")
            try:
                conn.execute(sql)  # type: ignore[call-overload]
                conn.execute(  # type: ignore[call-overload]
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (path.name,),
                )
                conn.execute("COMMIT")
                logger.info("Applied: %s", path.name)
                applied.append(path.name)
            except Exception:
                conn.execute("ROLLBACK")
                logger.exception("Migration failed: %s -- rolled back", path.name)
                raise

    return applied


def migration_status() -> list[dict[str, str]]:
    """Return status of every migration file: applied or pending."""
    files = _migration_files()

    try:
        with psycopg.connect(settings.database_url) as conn:
            applied: dict[str, str] = {
                row[0]: row[1].isoformat() if row[1] else ""
                for row in conn.execute("SELECT filename, applied_at FROM schema_migrations ORDER BY filename")
            }
    except psycopg.errors.UndefinedTable:
        applied = {}

    return [
        {
            "file": p.name,
            "status": "applied" if p.name in applied else "pending",
            "applied_at": applied.get(p.name, ""),
        }
        for p in files
    ]
