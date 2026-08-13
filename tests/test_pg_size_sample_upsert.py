"""#1564 — DB-tier: the pg_size_sample daily upsert is idempotent.

The genuinely-new SQL mechanism is the ``ON CONFLICT (sampled_on)`` upsert in
``app.workers.scheduler.pg_size_sample`` (sql/206). A same-day re-run / boot
catch-up must refresh the row's value, never insert a duplicate. Exercised
directly through the test conn (not via the job function, which opens its own
``connect_job`` against the operator dev DB).

Requires migration 206 in the test template — rebuild ``ebull_test_template``
after this lands (the operator runbook step).
"""

from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.db

_UPSERT = (
    "INSERT INTO pg_size_sample (sampled_on, db_size_bytes) "
    "VALUES (%(d)s, %(b)s) "
    "ON CONFLICT (sampled_on) DO UPDATE "
    "  SET db_size_bytes = EXCLUDED.db_size_bytes, sampled_at = NOW()"
)


def test_same_day_upsert_updates_not_duplicates(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute("DELETE FROM pg_size_sample WHERE sampled_on = '2026-06-28'")

    ebull_test_conn.execute(_UPSERT, {"d": "2026-06-28", "b": 45_000_000_000})
    ebull_test_conn.execute(_UPSERT, {"d": "2026-06-28", "b": 47_000_000_000})
    ebull_test_conn.commit()

    row = ebull_test_conn.execute(
        "SELECT COUNT(*), MAX(db_size_bytes) FROM pg_size_sample WHERE sampled_on = '2026-06-28'"
    ).fetchone()
    assert row is not None
    count, latest = row
    assert count == 1, "same-day re-run must not duplicate the daily sample"
    assert latest == 47_000_000_000, "second run's value must win"
