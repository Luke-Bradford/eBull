"""#1333 — migration content-drift guard.

The runner keys "already applied" on filename only; editing an applied
``sql/NNN_*.sql`` after its ``schema_migrations`` row was recorded left
the DB permanently diverged from the file (the sql/171 partial-application
WARN). The guard stores ``content_sha256`` per applied row and raises on
mismatch with an operator-actionable message.

Tests run the REAL ``run_migrations()`` against the per-worker test DB,
with ``MIGRATIONS_DIR`` monkeypatched to a tmp dir so only the probe
files are considered (the real applied rows are never iterated).
Probe rows are cleaned out of ``schema_migrations`` in teardown.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from app.db import migrations
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")

_PROBE = "900_test1333_drift_probe.sql"


@pytest.fixture
def probe_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point the runner at a tmp migrations dir + the test DB; clean up
    the probe's schema_migrations row afterwards."""
    from app.config import settings as app_settings

    url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", url)
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", tmp_path)
    yield tmp_path
    with psycopg.connect(url) as conn:
        conn.execute("DELETE FROM schema_migrations WHERE filename = %s", (_PROBE,))
        conn.commit()


def _stored_sha(filename: str) -> str | None:
    with psycopg.connect(test_database_url()) as conn:
        row = conn.execute("SELECT content_sha256 FROM schema_migrations WHERE filename = %s", (filename,)).fetchone()
    assert row is not None, f"no schema_migrations row for {filename}"
    return row[0]


def test_apply_records_content_sha(probe_dir: Path) -> None:
    probe = probe_dir / _PROBE
    probe.write_text("SELECT 1;\n", encoding="utf-8")

    applied = migrations.run_migrations()

    assert applied == [_PROBE]
    assert _stored_sha(_PROBE) == hashlib.sha256(probe.read_bytes()).hexdigest()
    # Unchanged re-run is a clean no-op.
    assert migrations.run_migrations() == []


def test_drift_on_applied_file_raises_with_operator_message(probe_dir: Path) -> None:
    probe = probe_dir / _PROBE
    probe.write_text("SELECT 1;\n", encoding="utf-8")
    migrations.run_migrations()

    probe.write_text("SELECT 2;  -- edited after apply\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match=rf"{_PROBE} content changed since applied"):
        migrations.run_migrations()


def test_legacy_null_sha_backfilled_once_then_enforced(probe_dir: Path) -> None:
    probe = probe_dir / _PROBE
    probe.write_text("SELECT 1;\n", encoding="utf-8")
    migrations.run_migrations()

    # Simulate a legacy row recorded before the guard existed.
    with psycopg.connect(test_database_url()) as conn:
        conn.execute("UPDATE schema_migrations SET content_sha256 = NULL WHERE filename = %s", (_PROBE,))
        conn.commit()

    # Backfill pass: trusts current content, no raise, idempotent.
    assert migrations.run_migrations() == []
    expected = hashlib.sha256(probe.read_bytes()).hexdigest()
    assert _stored_sha(_PROBE) == expected
    assert migrations.run_migrations() == []
    assert _stored_sha(_PROBE) == expected

    # The guard enforces from the next run on.
    probe.write_text("SELECT 3;\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="content changed since applied"):
        migrations.run_migrations()
