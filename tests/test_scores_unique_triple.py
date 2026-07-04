"""Migration 212 — UNIQUE(instrument_id, model_version, scored_at) on ``scores``.

Structural (index exists + is unique + covers the right columns) + behaviour
(a second row with the same triple is rejected; varying any key member is
allowed). Guards the #1918 invariant that ``GET /rankings`` COUNT(*) can never
diverge from ``GET /rankings/coverage`` COUNT(DISTINCT instrument_id).
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401

_INSTRUMENT_ID = 9_333_001
_MODEL = "test-model-v1"
_SCORED_AT = datetime(2026, 7, 4, 12, 0, 0, tzinfo=UTC)


def _seed_instrument(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (_INSTRUMENT_ID, "UQ1933", "Unique Triple Co"),
        )
    conn.commit()


def _insert_score(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int = _INSTRUMENT_ID,
    model_version: str = _MODEL,
    scored_at: datetime = _SCORED_AT,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scores (instrument_id, scored_at, model_version, total_score) VALUES (%s, %s, %s, 1.0)",
            (instrument_id, scored_at, model_version),
        )
    conn.commit()


def test_unique_index_exists_and_covers_triple(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = 'uq_scores_instrument_model_scored'")
        row = cur.fetchone()
    ebull_test_conn.commit()
    assert row is not None, "migration 212 unique index missing"
    indexdef = row[0]
    assert "UNIQUE INDEX" in indexdef
    # Column set (order-independent assertion is enough for the invariant).
    for col in ("instrument_id", "model_version", "scored_at"):
        assert col in indexdef, f"{col} not covered by uq_scores_instrument_model_scored"


def test_duplicate_triple_rejected(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_instrument(ebull_test_conn)
    _insert_score(ebull_test_conn)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert_score(ebull_test_conn)


def test_varying_any_key_member_is_allowed(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _seed_instrument(ebull_test_conn)
    _insert_score(ebull_test_conn)
    # Different scored_at (the normal multi-run case) — allowed.
    _insert_score(ebull_test_conn, scored_at=datetime(2026, 7, 4, 13, 0, 0, tzinfo=UTC))
    # Different model_version at the same instant — allowed.
    _insert_score(ebull_test_conn, model_version="test-model-v2")
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM scores WHERE instrument_id = %s", (_INSTRUMENT_ID,))
        row = cur.fetchone()
    ebull_test_conn.commit()
    assert row is not None
    assert row[0] == 3
