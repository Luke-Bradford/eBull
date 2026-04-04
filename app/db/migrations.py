"""Migration runner.

Scans sql/ for files named NNN_*.sql in lexicographic order.
Tracks applied migrations in a schema_migrations table.
Each migration runs inside its own transaction — if it fails the
transaction is rolled back and the error is re-raised immediately.
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
        logger.warning("No SQL migration files found in %s", MIGRATIONS_DIR)
    return files


def run_migrations() -> list[str]:
    """Apply all pending migrations synchronously. Returns list of applied filenames."""
    applied: list[str] = []

    with psycopg.connect(settings.database_url) as conn:
        conn.autocommit = True
        conn.execute(CREATE_TRACKING_TABLE)

        already_applied: set[str] = {row[0] for row in conn.execute("SELECT filename FROM schema_migrations")}

        for path in _migration_files():
            if path.name in already_applied:
                logger.debug("Migration already applied: %s", path.name)
                continue

            logger.info("Applying migration: %s", path.name)
            sql = path.read_text(encoding="utf-8")

            conn.autocommit = False
            try:
                conn.execute(sql)  # type: ignore[call-overload]
                conn.execute(  # type: ignore[call-overload]
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (path.name,),
                )
                conn.commit()
                logger.info("Applied: %s", path.name)
                applied.append(path.name)
            except Exception:
                conn.rollback()
                logger.exception("Migration failed: %s — rolled back", path.name)
                raise
            finally:
                conn.autocommit = True

    return applied


def migration_status() -> list[dict[str, str]]:
    """Return status of every migration file: applied or pending."""
    files = _migration_files()

    try:
        with psycopg.connect(settings.database_url) as conn:
            applied: dict[str, str] = {
                row[0]: row[1].isoformat()
                for row in conn.execute("SELECT filename, applied_at FROM schema_migrations ORDER BY applied_at")
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
