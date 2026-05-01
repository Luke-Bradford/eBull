"""Unit + integration tests for the filer-type classifier (#730 PR 3).

Pins:
  * Default classification — unflagged CIKs land as 'INV'.
  * ETF-list cross-reference — CIKs on the curated list land as 'ETF'.
  * Active-only — a CIK marked ``active=FALSE`` no longer counts as ETF.
  * Idempotence — re-running classify on the same CIK is stable.
  * Ingester wiring — _upsert_filer writes filer_type on insert and
    on the ON CONFLICT UPDATE branch, so a seed-list change
    propagates on the next ingest cycle without a backfill.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.providers.implementations.sec_13f import ThirteenFFilerInfo
from app.services.institutional_holdings import (
    _upsert_filer,
    classify_filer_type,
    seed_etf_filer,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _info(*, cik: str, name: str = "TEST FILER INC") -> ThirteenFFilerInfo:
    return ThirteenFFilerInfo(
        cik=cik,
        name=name,
        period_of_report=date(2024, 12, 31),
        filed_at=datetime(2025, 2, 14, tzinfo=UTC),
        table_value_total_usd=Decimal("1000000000"),
    )


class TestClassifyFilerType:
    def test_unflagged_cik_defaults_to_inv(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        assert classify_filer_type(conn, "0001067983") == "INV"

    def test_seeded_cik_is_etf(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        seed_etf_filer(conn, cik="0000102909", label="VANGUARD GROUP")
        conn.commit()
        assert classify_filer_type(conn, "0000102909") == "ETF"

    def test_inactive_seed_falls_back_to_inv(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A seed marked ``active=FALSE`` is preserved (audit trail
        of prior state) but does not count toward classification."""
        conn = ebull_test_conn
        seed_etf_filer(
            conn,
            cik="0000102909",
            label="VANGUARD",
            active=False,
            notes="paused — operator-initiated 2026-05-03",
        )
        conn.commit()
        assert classify_filer_type(conn, "0000102909") == "INV"

    def test_classification_normalises_unpadded_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Both the seed table and the classify call accept short
        CIKs and zero-pad on read so a caller doesn't have to
        remember the convention."""
        conn = ebull_test_conn
        seed_etf_filer(conn, cik="102909", label="VANGUARD")
        conn.commit()
        # Lookups work regardless of padding.
        assert classify_filer_type(conn, "102909") == "ETF"
        assert classify_filer_type(conn, "0000102909") == "ETF"


class TestUpsertFilerWritesType:
    def test_insert_writes_filer_type(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        seed_etf_filer(conn, cik="0000102909", label="VANGUARD")
        filer_id = _upsert_filer(conn, _info(cik="0000102909", name="VANGUARD"))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT filer_type FROM institutional_filers WHERE filer_id = %s", (filer_id,))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "ETF"

    def test_unflagged_filer_lands_as_inv(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        filer_id = _upsert_filer(conn, _info(cik="0001067983", name="BERKSHIRE"))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT filer_type FROM institutional_filers WHERE filer_id = %s", (filer_id,))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "INV"

    def test_on_conflict_updates_filer_type_after_seed_change(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Operator promotes a CIK from 'INV' to 'ETF' by adding it
        to the seed list. The next ingest cycle re-runs _upsert_filer
        and the ON CONFLICT UPDATE branch must propagate the new
        label without requiring a backfill migration."""
        conn = ebull_test_conn
        # Phase 1: filer arrives without the ETF flag.
        _upsert_filer(conn, _info(cik="0000102909", name="VANGUARD"))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT filer_type FROM institutional_filers WHERE cik = '0000102909'")
            assert cur.fetchone() == ("INV",)

        # Phase 2: operator seeds the ETF list and re-runs the
        # ingester.
        seed_etf_filer(conn, cik="0000102909", label="VANGUARD")
        _upsert_filer(conn, _info(cik="0000102909", name="VANGUARD"))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT filer_type FROM institutional_filers WHERE cik = '0000102909'")
            assert cur.fetchone() == ("ETF",)


class TestSeedEtfFilerHelper:
    def test_idempotent_seed_update(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        seed_etf_filer(conn, cik="102909", label="VANGUARD")
        seed_etf_filer(conn, cik="0000102909", label="VANGUARD GROUP", notes="updated")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT label, notes, active FROM etf_filer_cik_seeds WHERE cik = '0000102909'")
            row = cur.fetchone()
        assert row == ("VANGUARD GROUP", "updated", True)
