"""Tests for the bundled company_tickers_mf.json ingest (#1171, T4)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services import mf_directory
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} fund"),
    )


def _fake_provider(payload: dict[str, Any]) -> Any:
    provider = MagicMock()
    provider.fetch_document_text.return_value = __import__("json").dumps(payload)
    provider.__enter__ = MagicMock(return_value=provider)
    provider.__exit__ = MagicMock(return_value=False)
    return provider


def test_refresh_mf_directory_first_run(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=4001, symbol="VFIAX")
    ebull_test_conn.commit()
    payload = {
        "fields": ["cik", "seriesId", "classId", "symbol"],
        "data": [
            [36405, "S000002839", "C000010048", "VFIAX"],
            [36405, "S000002839", "C000010049", "VFINX"],  # not in instruments
            [819118, "S000006027", "C000016700", "FXAIX"],  # not in instruments
        ],
    }
    provider = _fake_provider(payload)

    result = mf_directory.refresh_mf_directory(ebull_test_conn, provider=provider)

    assert result["fetched"] == 3
    assert result["directory_rows"] == 3
    assert result["external_identifier_rows"] == 1  # only VFIAX is in instruments

    # Directory populated.
    cur = ebull_test_conn.execute(
        "SELECT trust_cik, symbol FROM cik_refresh_mf_directory WHERE class_id = %s",
        ("C000010048",),
    )
    row = cur.fetchone()
    assert row[0] == "0000036405"  # zero-padded
    assert row[1] == "VFIAX"

    # external_identifier created for in-universe symbol.
    cur = ebull_test_conn.execute(
        "SELECT instrument_id FROM external_identifiers "
        "WHERE provider='sec' AND identifier_type='class_id' AND identifier_value=%s",
        ("C000010048",),
    )
    assert cur.fetchone()[0] == 4001


def test_refresh_mf_directory_idempotent_second_run(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=4002, symbol="VOO")
    ebull_test_conn.commit()
    payload = {
        "fields": ["cik", "seriesId", "classId", "symbol"],
        "data": [[36405, "S000002839", "C000200001", "VOO"]],
    }
    provider = _fake_provider(payload)

    first = mf_directory.refresh_mf_directory(ebull_test_conn, provider=provider)
    second = mf_directory.refresh_mf_directory(ebull_test_conn, provider=provider)

    assert first["external_identifier_rows"] == 1
    # Second run: ON CONFLICT DO NOTHING → no new ext_ids.
    assert second["external_identifier_rows"] == 0


def test_refresh_mf_directory_handles_malformed_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        "fields": ["cik", "seriesId", "classId", "symbol"],
        "data": [
            [36405, "S000002839", "C000300001", "VTSAX"],
            "this is not a list",  # malformed
            [None, "S000002839", "C000300002", "VTI"],  # null CIK — still accepted with NULL trust_cik
        ],
    }
    provider = _fake_provider(payload)

    result = mf_directory.refresh_mf_directory(ebull_test_conn, provider=provider)
    # 2 valid rows + 1 malformed skipped.
    assert result["directory_rows"] == 2


def test_refresh_mf_directory_empty_body_raises(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    provider = MagicMock()
    provider.fetch_document_text.return_value = None
    provider.__enter__ = MagicMock(return_value=provider)
    provider.__exit__ = MagicMock(return_value=False)

    with pytest.raises(RuntimeError, match="Empty body"):
        mf_directory.refresh_mf_directory(ebull_test_conn, provider=provider)
