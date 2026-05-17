"""Tests for the bundled company_tickers_exchange.json ingest (G8, Phase
2 PR 4 of the US-ETL completion plan).

Mirrors ``tests/test_mf_directory.py`` shape: ``MagicMock`` provider
with a stubbed ``fetch_document_text`` returns a constructed JSON
payload; the real service writes rows to the test DB; SELECT
back asserts the snapshot.

Spec: ``docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md``.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services import exchange_directory
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _fake_provider(payload: dict[str, Any] | str) -> Any:
    """Stub provider whose ``fetch_document_text`` returns the supplied
    payload (dict → JSON-encoded; str → returned verbatim, e.g. for the
    empty-body test).
    """
    provider = MagicMock()
    body = payload if isinstance(payload, str) else json.dumps(payload)
    provider.fetch_document_text.return_value = body
    provider.__enter__ = MagicMock(return_value=provider)
    provider.__exit__ = MagicMock(return_value=False)
    return provider


def _read_directory(conn: psycopg.Connection[tuple]) -> list[tuple[Any, ...]]:
    return list(
        conn.execute(
            "SELECT cik, ticker, name, exchange FROM cik_refresh_exchange_directory ORDER BY cik, ticker"
        ).fetchall()
    )


# ---------------------------------------------------------------------------
# T1 — happy path with three rows + three exchanges
# ---------------------------------------------------------------------------


def test_happy_path_three_rows_three_exchanges(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            [19617, "JPMORGAN CHASE & CO", "JPM", "NYSE"],
            [2070829, "Foreign ADR Co", "CYATY", "OTC"],
        ],
    }
    provider = _fake_provider(payload)

    result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result == {"fetched": 3, "directory_rows": 3}
    rows = _read_directory(ebull_test_conn)
    assert rows == [
        ("0000019617", "JPM", "JPMORGAN CHASE & CO", "NYSE"),
        ("0000320193", "AAPL", "Apple Inc.", "Nasdaq"),
        ("0002070829", "CYATY", "Foreign ADR Co", "OTC"),
    ]


# ---------------------------------------------------------------------------
# T2 — CIK zero-padding from integer
# ---------------------------------------------------------------------------


def test_cik_integer_zero_padded(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
    }
    provider = _fake_provider(payload)

    exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    row = ebull_test_conn.execute(
        "SELECT cik FROM cik_refresh_exchange_directory WHERE ticker = %s",
        ("AAPL",),
    ).fetchone()
    assert row is not None
    assert row[0] == "0000320193"


# ---------------------------------------------------------------------------
# T3 — multi-ticker CIK preserved (Codex 1a HIGH 2 regression guard)
# ---------------------------------------------------------------------------


def test_multi_ticker_cik_preserves_all_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Empirically, the 2026-05-17 live payload has 1,446 CIKs with
    multiple ticker variants (BAC=17, JPM=9, BABA=3). PK (cik, ticker)
    must preserve every (ticker, exchange) mapping."""
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [70858, "BANK OF AMERICA CORP", "BAC", "NYSE"],
            [70858, "BANK OF AMERICA CORP", "BAC-PB", "NYSE"],
            [70858, "BANK OF AMERICA CORP", "BACRP", "OTC"],
        ],
    }
    provider = _fake_provider(payload)

    result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result["directory_rows"] == 3
    tickers = ebull_test_conn.execute(
        "SELECT ticker, exchange FROM cik_refresh_exchange_directory WHERE cik = %s ORDER BY ticker",
        ("0000070858",),
    ).fetchall()
    assert tickers == [("BAC", "NYSE"), ("BAC-PB", "NYSE"), ("BACRP", "OTC")]


# ---------------------------------------------------------------------------
# T4 — empty / null exchange normalised to SQL NULL
# ---------------------------------------------------------------------------


def test_empty_and_null_exchange_normalised_to_null(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [111, "Empty-string exchange Co", "EMPTY", ""],
            [222, "Null exchange Co", "NULL", None],
        ],
    }
    provider = _fake_provider(payload)

    exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    rows = ebull_test_conn.execute(
        "SELECT ticker, exchange FROM cik_refresh_exchange_directory ORDER BY ticker"
    ).fetchall()
    assert rows == [("EMPTY", None), ("NULL", None)]


# ---------------------------------------------------------------------------
# T5 — empty / null ticker skipped with warning (PK forbids NULL)
# ---------------------------------------------------------------------------


def test_empty_and_null_ticker_skipped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [111, "Empty Co", "", "Nasdaq"],
            [222, "Null Co", None, "Nasdaq"],
            [333, "Valid Co", "VALID", "Nasdaq"],
        ],
    }
    provider = _fake_provider(payload)

    with caplog.at_level(logging.WARNING, logger="app.services.exchange_directory"):
        result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result == {"fetched": 3, "directory_rows": 1}
    rows = _read_directory(ebull_test_conn)
    assert [r[1] for r in rows] == ["VALID"]
    warnings = [r for r in caplog.records if "empty/non-string ticker" in r.message]
    assert len(warnings) == 2


# ---------------------------------------------------------------------------
# T6 — malformed row variants skipped, valid stored
# ---------------------------------------------------------------------------


def test_malformed_rows_skipped_valid_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],  # valid
            ["not-a-number", "Bad CIK Co", "BAD", "NYSE"],  # cik non-numeric
            [111, "Short row"],  # len < max_idx
        ],
    }
    provider = _fake_provider(payload)

    with caplog.at_level(logging.WARNING, logger="app.services.exchange_directory"):
        result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result == {"fetched": 3, "directory_rows": 1}
    rows = _read_directory(ebull_test_conn)
    assert len(rows) == 1
    assert rows[0][1] == "AAPL"
    # Two distinct warnings: one for non-numeric cik, one for short row.
    assert any("non-numeric cik" in r.message for r in caplog.records)
    assert any("malformed row" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# T7 — upsert idempotency: stale last_seen advances on re-run
# ---------------------------------------------------------------------------


def test_upsert_idempotency_advances_last_seen(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Avoid time-flakiness from psycopg3's transaction-stable NOW()
    by staling the first-pass last_seen before the second pass, then
    asserting the second pass advanced it."""
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
    }
    provider = _fake_provider(payload)

    # First pass.
    first = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    # Force a deterministic clock advance via a stale UPDATE so the
    # next NOW() in the refresh transaction is unambiguously later.
    ebull_test_conn.execute("UPDATE cik_refresh_exchange_directory SET last_seen = last_seen - INTERVAL '1 minute'")
    ebull_test_conn.commit()

    # Read the stale value AFTER the UPDATE, BEFORE the second refresh.
    # This is the value the second refresh MUST advance past. Comparing
    # against a Python-reconstructed `ts1 - delta` would be vacuously
    # true (Codex review #1194 WARNING): even if the UPSERT's DO UPDATE
    # never ran, ts2 would still equal ts1 ≈ NOW() which trivially
    # exceeds `ts1 - 1 minute`. The assertion must read what is in the
    # row right now and assert the next refresh strictly advances it.
    stale_row = ebull_test_conn.execute(
        "SELECT last_seen FROM cik_refresh_exchange_directory WHERE cik = %s AND ticker = %s",
        ("0000320193", "AAPL"),
    ).fetchone()
    assert stale_row is not None
    stale_ts = stale_row[0]

    # Second pass.
    second = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)
    row2 = ebull_test_conn.execute(
        "SELECT last_seen FROM cik_refresh_exchange_directory WHERE cik = %s AND ticker = %s",
        ("0000320193", "AAPL"),
    ).fetchone()
    assert row2 is not None
    ts2 = row2[0]

    # Row count stable.
    assert first == second == {"fetched": 1, "directory_rows": 1}
    # Second pass strictly advanced last_seen past the staled value the
    # second refresh actually observed at start. If the UPSERT's
    # DO UPDATE never fired, ts2 would equal stale_ts and this would
    # fail loudly.
    assert ts2 > stale_ts


# ---------------------------------------------------------------------------
# T8 — empty data list returns 0 / 0
# ---------------------------------------------------------------------------


def test_empty_data_list_returns_zero(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    payload = {"fields": ["cik", "name", "ticker", "exchange"], "data": []}
    provider = _fake_provider(payload)

    result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result == {"fetched": 0, "directory_rows": 0}
    assert _read_directory(ebull_test_conn) == []


# ---------------------------------------------------------------------------
# T9 — missing entire fields key returns 0 / 0 without raise
# ---------------------------------------------------------------------------


def test_missing_fields_key_returns_zero(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    payload = {"data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]]}
    provider = _fake_provider(payload)

    result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result == {"fetched": 0, "directory_rows": 0}
    assert _read_directory(ebull_test_conn) == []


# ---------------------------------------------------------------------------
# T10 — missing single required field name in `fields` (Codex 1a MED 5)
# ---------------------------------------------------------------------------


def test_missing_single_field_returns_zero_with_warning(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = {
        # No "exchange" field.
        "fields": ["cik", "name", "ticker"],
        "data": [[320193, "Apple Inc.", "AAPL"]],
    }
    provider = _fake_provider(payload)

    with caplog.at_level(logging.WARNING, logger="app.services.exchange_directory"):
        result = exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    assert result == {"fetched": 0, "directory_rows": 0}
    assert any("missing required field" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# T11 — field reordering tolerated
# ---------------------------------------------------------------------------


def test_field_reordering_tolerated(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    payload = {
        "fields": ["exchange", "cik", "name", "ticker"],
        "data": [["Nasdaq", 320193, "Apple Inc.", "AAPL"]],
    }
    provider = _fake_provider(payload)

    exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)

    row = ebull_test_conn.execute("SELECT cik, ticker, name, exchange FROM cik_refresh_exchange_directory").fetchone()
    assert row == ("0000320193", "AAPL", "Apple Inc.", "Nasdaq")


# ---------------------------------------------------------------------------
# T12 — empty body raises (MF parity)
# ---------------------------------------------------------------------------


def test_empty_body_raises(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    provider = _fake_provider("")  # empty string body

    with pytest.raises(RuntimeError, match="Empty body fetching"):
        exchange_directory.refresh_exchange_directory(ebull_test_conn, provider=provider)
