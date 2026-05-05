"""Operator-driven cleanup helper for leaked test databases.

Drops every ``ebull_test_*`` database **except** ``ebull_test_template``
on the configured Postgres cluster. Used after a crashed pytest run
leaves a worker DB orphaned (the session-finish hook would normally
clean up, but a SIGKILL'd worker skips it).

Usage:

    uv run python -m tests.fixtures.cleanup_test_dbs

The template stays so the next pytest invocation re-uses it for free.
"""

from __future__ import annotations

import psycopg

from tests.fixtures.ebull_test_db import (
    TEMPLATE_DB_NAME,
    _admin_database_url,
    _drop_database_force,
)


def main() -> int:
    dropped: list[str] = []
    skipped: list[str] = []
    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database WHERE datname LIKE 'ebull_test_%' ORDER BY datname")
            names = [row[0] for row in cur.fetchall()]
        for name in names:
            if name == TEMPLATE_DB_NAME:
                skipped.append(name)
                continue
            try:
                _drop_database_force(admin, name)
                dropped.append(name)
            except Exception as exc:
                print(f"  [warn] could not drop {name}: {type(exc).__name__}: {exc}")

    if dropped:
        print("Dropped:")
        for name in dropped:
            print(f"  - {name}")
    else:
        print("No leaked test databases found.")
    if skipped:
        print("Preserved (template):")
        for name in skipped:
            print(f"  - {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
