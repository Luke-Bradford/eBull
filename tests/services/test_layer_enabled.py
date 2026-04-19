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
        set_layer_enabled(conn, "news", enabled=False)
        conn.commit()
        result = read_all_enabled(conn, ["news", "thesis", "fx_rates"])
    assert result == {"news": False, "thesis": True, "fx_rates": True}
