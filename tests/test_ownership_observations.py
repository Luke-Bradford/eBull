"""Tests for the ownership observations + _current pattern (#840.A).

Covers the foundation sub-PR: provenance shape, insiders
observations/current round-trip, two-axis dedup, refresh
idempotency, advisory-lock contract.

Subsequent sub-PRs (institutions / blockholders / treasury / def14a)
add their own tests; this module establishes the patterns.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


# ---------------------------------------------------------------------------
# Schema-shape uniformity (Codex plan-review finding #4)
# ---------------------------------------------------------------------------


class TestProvenanceBlockUniformity:
    """Every ``ownership_*_observations`` table must carry the EXACT
    provenance block (column names, types, nullability, source CHECK).
    Drift across categories is a real risk; this test fails CI on any
    deviation.

    For #840.A only ``ownership_insiders_observations`` exists. As
    sub-PRs B-D add institutions/blockholders/treasury/def14a tables,
    they auto-enroll into this test via the
    ``information_schema.tables`` LIKE pattern."""

    _PROVENANCE_COLS: tuple[str, ...] = (
        "source",
        "source_document_id",
        "source_accession",
        "source_field",
        "source_url",
        "filed_at",
        "period_start",
        "period_end",
        "known_from",
        "known_to",
        "ingest_run_id",
    )

    def test_every_observations_table_carries_full_provenance_block(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name LIKE 'ownership_%%_observations'
                  AND table_name NOT LIKE 'ownership_%%_observations_%%'
                """
            )
            tables = [str(row["table_name"]) for row in cur.fetchall()]

        assert tables, "no ownership_*_observations tables found"

        for table in tables:
            with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT column_name, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    (table,),
                )
                cols = {str(row["column_name"]): str(row["is_nullable"]) for row in cur.fetchall()}

            for col in self._PROVENANCE_COLS:
                assert col in cols, f"{table} missing provenance column {col!r}"

            # Required-non-null provenance fields:
            for required in ("source", "source_document_id", "filed_at", "period_end", "known_from", "ingest_run_id"):
                assert cols[required] == "NO", f"{table}.{required} must be NOT NULL"

            # Optional fields:
            for nullable in ("source_accession", "source_field", "source_url", "period_start", "known_to"):
                assert cols[nullable] == "YES", f"{table}.{nullable} must be NULLABLE"

    def test_default_partition_is_empty_post_backfill(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex plan-review finding #1: the default partition catches
        any row whose ``period_end`` falls outside the explicit ranges.
        On a healthy install this stays empty — anything landing here
        signals a partition floor / ceiling miss that the operator
        must investigate."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ownership_insiders_observations_default")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0


# ---------------------------------------------------------------------------
# Insider observation round-trip + dedup
# ---------------------------------------------------------------------------


class TestInsiderObservations:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=840_001, symbol="GME")
        conn.commit()
        return conn

    def test_record_then_refresh_round_trip(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """One observation → one row in _current. Verifies the basic
        record/refresh cycle works end-to-end."""
        conn = _setup
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik="0001767470",
            holder_name="Cohen Ryan",
            ownership_nature="direct",
            source="form4",
            source_document_id="0001234567-26-000001",
            source_accession="0001234567-26-000001",
            source_field=None,
            source_url="https://www.sec.gov/.../form4.xml",
            filed_at=datetime(2026, 1, 21, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 21),
            ingest_run_id=uuid4(),
            shares=Decimal("38347842"),
        )
        conn.commit()

        n = refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()
        assert n == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT holder_cik, ownership_nature, source, shares
                FROM ownership_insiders_current WHERE instrument_id = %s
                """,
                (840_001,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["holder_cik"] == "0001767470"
        assert rows[0]["ownership_nature"] == "direct"
        assert rows[0]["source"] == "form4"
        assert rows[0]["shares"] == Decimal("38347842")

    def test_dedup_within_nature_form4_outranks_form3(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Two observations for the same (CIK, nature) — Form 4 wins
        Form 3 per the source-priority chain."""
        conn = _setup
        cik = "0009990001"
        run_id = uuid4()
        # Older Form 3.
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Officer A",
            ownership_nature="direct",
            source="form3",
            source_document_id="ACC-F3",
            source_accession="ACC-F3",
            source_field=None,
            source_url=None,
            filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2024, 5, 1),
            ingest_run_id=run_id,
            shares=Decimal("10000"),
        )
        # Newer Form 4 — should win.
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Officer A",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-F4",
            source_accession="ACC-F4",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 15),
            ingest_run_id=run_id,
            shares=Decimal("12500"),
        )
        conn.commit()

        refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source, shares FROM ownership_insiders_current
                WHERE instrument_id = %s AND holder_cik = %s
                """,
                (840_001, cik),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["source"] == "form4"
        assert rows[0]["shares"] == Decimal("12500")

    def test_dual_render_across_natures_for_same_cik(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Cohen-on-GME shape: same CIK with TWO different
        ``ownership_nature`` values — direct (Form 4, 38M) and
        beneficial (13D, 75M). Both must surface in ``_current`` —
        cross-nature dedup is forbidden under the two-axis spec."""
        conn = _setup
        cik = "0001767470"
        run_id = uuid4()
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Cohen Ryan",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-F4-COHEN",
            source_accession="ACC-F4-COHEN",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 21, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 21),
            ingest_run_id=run_id,
            shares=Decimal("38347842"),
        )
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik=cik,
            holder_name="Cohen Ryan",
            ownership_nature="beneficial",
            source="13d",
            source_document_id="ACC-13D-COHEN",
            source_accession="ACC-13D-COHEN",
            source_field=None,
            source_url=None,
            filed_at=datetime(2025, 1, 29, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 1, 29),
            ingest_run_id=run_id,
            shares=Decimal("75000000"),
        )
        conn.commit()

        refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT ownership_nature, source, shares
                FROM ownership_insiders_current
                WHERE instrument_id = %s AND holder_cik = %s
                ORDER BY ownership_nature
                """,
                (840_001, cik),
            )
            rows = cur.fetchall()
        # Two rows, both surface.
        assert len(rows) == 2
        natures = {r["ownership_nature"]: (r["source"], r["shares"]) for r in rows}
        assert natures["direct"] == ("form4", Decimal("38347842"))
        assert natures["beneficial"] == ("13d", Decimal("75000000"))

    def test_refresh_is_idempotent(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Running ``refresh_insiders_current`` twice with no new
        observations leaves the same set of rows. Refresh atomicity
        guard exercised."""
        conn = _setup
        record_insider_observation(
            conn,
            instrument_id=840_001,
            holder_cik="0001234567",
            holder_name="Test Holder",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-1",
            source_accession="ACC-1",
            source_field=None,
            source_url=None,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 1, 1),
            ingest_run_id=uuid4(),
            shares=Decimal("100"),
        )
        conn.commit()

        first = refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()
        second = refresh_insiders_current(conn, instrument_id=840_001)
        conn.commit()
        assert first == 1
        assert second == 1

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_insiders_current WHERE instrument_id = %s",
                (840_001,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_record_is_idempotent_on_natural_key(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Recording the same observation twice (re-running the
        ingester on the same accession) does NOT duplicate the
        observations row — ON CONFLICT DO UPDATE refreshes in place."""
        conn = _setup
        run_id = uuid4()
        for _ in range(2):
            record_insider_observation(
                conn,
                instrument_id=840_001,
                holder_cik="0007770007",
                holder_name="Idempotent Holder",
                ownership_nature="direct",
                source="form4",
                source_document_id="ACC-IDEMP",
                source_accession="ACC-IDEMP",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 2, 1),
                ingest_run_id=run_id,
                shares=Decimal("500"),
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM ownership_insiders_observations
                WHERE instrument_id = %s AND holder_cik = %s
                """,
                (840_001, "0007770007"),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
