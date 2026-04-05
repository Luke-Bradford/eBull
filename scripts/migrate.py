"""Apply pending database migrations. Run via: uv run python scripts/migrate.py"""

from app.db.migrations import run_migrations

applied = run_migrations()
if applied:
    print(f"Applied {len(applied)} migration(s): {applied}")
else:
    print("No pending migrations.")
