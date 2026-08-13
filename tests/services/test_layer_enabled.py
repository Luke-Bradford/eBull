import psycopg
import pytest

from app.services.layer_enabled import (
    is_layer_enabled,
    read_all_enabled,
    set_layer_enabled,
)
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


@pytest.mark.integration
def test_default_missing_row_is_enabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM layer_enabled WHERE layer_name = %s", ("candles",))
        conn.commit()
        assert is_layer_enabled(conn, "candles") is True


@pytest.mark.integration
def test_set_and_read_back() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        set_layer_enabled(conn, "candles", enabled=False)
        conn.commit()
        assert is_layer_enabled(conn, "candles") is False
        set_layer_enabled(conn, "candles", enabled=True)
        conn.commit()
        assert is_layer_enabled(conn, "candles") is True


@pytest.mark.integration
def test_read_all_enabled_batched() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM layer_enabled WHERE layer_name = ANY(%s)", (["news", "thesis", "fx_rates"],))
        conn.commit()
        set_layer_enabled(conn, "news", enabled=False, reason="batched-read test", changed_by="pytest")
        conn.commit()
        result = read_all_enabled(conn, ["news", "thesis", "fx_rates"])
    assert result == {"news": False, "thesis": True, "fx_rates": True}


@pytest.mark.integration
def test_set_layer_enabled_persists_reason_and_changed_by() -> None:
    """#346: latest-state row carries reason + changed_by for hot-path
    reads (no second query needed to surface 'why is this disabled?')."""
    with psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM layer_enabled WHERE layer_name = %s", ("candles",))
        conn.execute("DELETE FROM layer_enabled_audit WHERE layer_name = %s", ("candles",))
        conn.commit()
        set_layer_enabled(conn, "candles", enabled=False, reason="provider 5xx storm", changed_by="ops@example.com")
        conn.commit()
        row = conn.execute(
            "SELECT is_enabled, reason, changed_by FROM layer_enabled WHERE layer_name = %s",
            ("candles",),
        ).fetchone()
    assert row == (False, "provider 5xx storm", "ops@example.com")


@pytest.mark.integration
def test_set_layer_enabled_writes_audit_row_per_toggle() -> None:
    """#346: every toggle appends a layer_enabled_audit row so the
    full sequence is queryable, not just the most-recent state."""
    with psycopg.connect(_test_database_url()) as conn:
        conn.execute("DELETE FROM layer_enabled WHERE layer_name = %s", ("candles",))
        conn.execute("DELETE FROM layer_enabled_audit WHERE layer_name = %s", ("candles",))
        conn.commit()
        set_layer_enabled(conn, "candles", enabled=False, reason="r1", changed_by="op1")
        conn.commit()
        set_layer_enabled(conn, "candles", enabled=True, reason="r2", changed_by="op2")
        conn.commit()
        rows = conn.execute(
            """
            SELECT is_enabled, reason, changed_by
            FROM layer_enabled_audit
            WHERE layer_name = %s
            ORDER BY changed_at ASC, audit_id ASC
            """,
            ("candles",),
        ).fetchall()
    assert rows == [(False, "r1", "op1"), (True, "r2", "op2")]
