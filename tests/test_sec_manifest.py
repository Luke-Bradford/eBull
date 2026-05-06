"""Tests for ``sec_filing_manifest`` + the manifest service (#864).

Covers:

- Schema integrity: PK, CHECK constraints, indexes, trigger
- ``record_manifest_entry`` round-trip + idempotent UPSERT
- Subject / instrument cross-check (CHECK constraint + service guard)
- ``transition_status`` allowed + illegal transitions
- ``iter_pending`` / ``iter_retryable`` filters and ordering
- ``map_form_to_source`` / ``is_amendment_form`` helpers
- ``ingested_at`` column on every ``ownership_*_observations`` table
  (the v3 spec finding #1 addition)
- ``ingested_at`` bumps on UPSERT (DO UPDATE) for every record_*_observation
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services.ownership_observations import (
    record_blockholder_observation,
    record_def14a_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
)
from app.services.sec_manifest import (
    ManifestSource,
    get_manifest_row,
    is_amendment_form,
    iter_pending,
    iter_retryable,
    map_form_to_source,
    record_manifest_entry,
    transition_status,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str, cik: str | None = None) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )
    if cik is not None:
        # CIK lives in ``instrument_sec_profile``, not on ``instruments``.
        conn.execute(
            """
            INSERT INTO instrument_sec_profile (instrument_id, cik)
            VALUES (%s, %s)
            ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
            """,
            (iid, cik),
        )


def _seed_institutional_filer(conn: psycopg.Connection[tuple], *, cik: str, name: str = "BlackRock") -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO institutional_filers (cik, name, filer_type, fetched_at)
            VALUES (%s, %s, 'INV', NOW())
            ON CONFLICT (cik) DO UPDATE SET name = EXCLUDED.name
            RETURNING filer_id
            """,
            (cik, name),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


# ---------------------------------------------------------------------------
# Schema shape
# ---------------------------------------------------------------------------


class TestSchema:
    """Migration 118 + 119 produced the expected table shape."""

    def test_manifest_table_exists_with_pk(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT column_name, is_nullable, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'sec_filing_manifest'
                """
            )
            cols = {row["column_name"]: row for row in cur.fetchall()}

        assert "accession_number" in cols
        assert cols["accession_number"]["is_nullable"] == "NO"

        for required in (
            "cik",
            "form",
            "source",
            "subject_type",
            "subject_id",
            "filed_at",
            "ingest_status",
            "raw_status",
            "created_at",
            "updated_at",
        ):
            assert cols[required]["is_nullable"] == "NO", f"{required} must be NOT NULL"

        for nullable in (
            "instrument_id",
            "accepted_at",
            "primary_document_url",
            "amends_accession",
            "parser_version",
            "last_attempted_at",
            "next_retry_at",
            "error",
        ):
            assert cols[nullable]["is_nullable"] == "YES", f"{nullable} must be NULLABLE"

    def test_issuer_constraint_rejects_null_instrument_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # CHECK chk_manifest_issuer_has_instrument: subject_type='issuer'
        # MUST have a non-null instrument_id.
        with pytest.raises(psycopg.errors.CheckViolation):
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sec_filing_manifest (
                        accession_number, cik, form, source,
                        subject_type, subject_id, instrument_id,
                        filed_at
                    ) VALUES (
                        '0000000000-00-000001', '0000320193', '4', 'sec_form4',
                        'issuer', '999', NULL,
                        NOW()
                    )
                    """
                )
        ebull_test_conn.rollback()

    def test_non_issuer_constraint_rejects_non_null_instrument_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=42, symbol="X")
        with pytest.raises(psycopg.errors.CheckViolation):
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sec_filing_manifest (
                        accession_number, cik, form, source,
                        subject_type, subject_id, instrument_id,
                        filed_at
                    ) VALUES (
                        '0000000000-00-000002', '0001364742', '13F-HR', 'sec_13f_hr',
                        'institutional_filer', '0001364742', 42,
                        NOW()
                    )
                    """
                )
        ebull_test_conn.rollback()


# ---------------------------------------------------------------------------
# record_manifest_entry round-trip
# ---------------------------------------------------------------------------


class TestRecordManifestEntry:
    def test_round_trip_issuer(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        _seed_instrument(ebull_test_conn, iid=1701, symbol="AAPL", cik="0000320193")

        record_manifest_entry(
            ebull_test_conn,
            "0000320193-26-000001",
            cik="0000320193",
            form="DEF 14A",
            source="sec_def14a",
            subject_type="issuer",
            subject_id="1701",
            instrument_id=1701,
            filed_at=datetime(2026, 1, 15, tzinfo=UTC),
            primary_document_url="https://www.sec.gov/Archives/.../proxy.htm",
        )
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "0000320193-26-000001")
        assert row is not None
        assert row.cik == "0000320193"
        assert row.source == "sec_def14a"
        assert row.subject_type == "issuer"
        assert row.instrument_id == 1701
        assert row.ingest_status == "pending"
        assert row.raw_status == "absent"
        assert row.is_amendment is False

    def test_round_trip_institutional_filer(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        record_manifest_entry(
            ebull_test_conn,
            "0001364742-26-000001",
            cik="0001364742",
            form="13F-HR",
            source="sec_13f_hr",
            subject_type="institutional_filer",
            subject_id="0001364742",
            instrument_id=None,
            filed_at=datetime(2026, 2, 14, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "0001364742-26-000001")
        assert row is not None
        assert row.subject_type == "institutional_filer"
        assert row.instrument_id is None

    def test_upsert_is_idempotent(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        for _ in range(3):
            record_manifest_entry(
                ebull_test_conn,
                "0000000001-26-000001",
                cik="0000000001",
                form="4",
                source="sec_form4",
                subject_type="issuer",
                subject_id="1",
                instrument_id=1,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number = %s",
                ("0000000001-26-000001",),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 1

    def test_upsert_does_not_overwrite_lifecycle_state(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        record_manifest_entry(
            ebull_test_conn,
            "ACC-1",
            cik="0000000001",
            form="4",
            source="sec_form4",
            subject_type="issuer",
            subject_id="1",
            instrument_id=1,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        transition_status(ebull_test_conn, "ACC-1", ingest_status="parsed", parser_version="v2")
        ebull_test_conn.commit()

        # A second discovery (Atom feed re-emits) should NOT downgrade
        # the state from ``parsed`` back to ``pending``.
        record_manifest_entry(
            ebull_test_conn,
            "ACC-1",
            cik="0000000001",
            form="4",
            source="sec_form4",
            subject_type="issuer",
            subject_id="1",
            instrument_id=1,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.parser_version == "v2"

    def test_service_guard_rejects_issuer_without_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(ValueError, match="subject_type='issuer' requires instrument_id"):
            record_manifest_entry(
                ebull_test_conn,
                "BAD",
                cik="0000000001",
                form="4",
                source="sec_form4",
                subject_type="issuer",
                subject_id="1",
                instrument_id=None,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
            )

    def test_service_guard_rejects_non_issuer_with_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        with pytest.raises(ValueError, match="must have instrument_id=None"):
            record_manifest_entry(
                ebull_test_conn,
                "BAD",
                cik="0001364742",
                form="13F-HR",
                source="sec_13f_hr",
                subject_type="institutional_filer",
                subject_id="0001364742",
                instrument_id=1,
                filed_at=datetime(2026, 2, 14, tzinfo=UTC),
            )


# ---------------------------------------------------------------------------
# transition_status state machine
# ---------------------------------------------------------------------------


class TestTransitionStatus:
    def _seed(self, conn: psycopg.Connection[tuple]) -> str:
        _seed_instrument(conn, iid=1, symbol="X", cik="0000000001")
        record_manifest_entry(
            conn,
            "ACC-1",
            cik="0000000001",
            form="4",
            source="sec_form4",
            subject_type="issuer",
            subject_id="1",
            instrument_id=1,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        return "ACC-1"

    def test_pending_to_fetched_to_parsed(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        accession = self._seed(ebull_test_conn)
        transition_status(ebull_test_conn, accession, ingest_status="fetched", raw_status="stored")
        transition_status(ebull_test_conn, accession, ingest_status="parsed", parser_version="v3")
        row = get_manifest_row(ebull_test_conn, accession)
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "stored"
        assert row.parser_version == "v3"
        assert row.error is None

    def test_failed_clears_on_retry(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        accession = self._seed(ebull_test_conn)
        transition_status(
            ebull_test_conn,
            accession,
            ingest_status="failed",
            error="HTTP 503",
            next_retry_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
        row = get_manifest_row(ebull_test_conn, accession)
        assert row is not None
        assert row.error == "HTTP 503"
        assert row.next_retry_at == datetime(2026, 1, 2, tzinfo=UTC)

        # Worker re-tries: failed -> fetched -> parsed
        transition_status(ebull_test_conn, accession, ingest_status="fetched", raw_status="stored")
        transition_status(ebull_test_conn, accession, ingest_status="parsed")
        row = get_manifest_row(ebull_test_conn, accession)
        assert row is not None
        assert row.error is None
        assert row.next_retry_at is None

    def test_illegal_transition_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        accession = self._seed(ebull_test_conn)
        # parsed cannot jump straight to failed
        transition_status(ebull_test_conn, accession, ingest_status="fetched", raw_status="stored")
        transition_status(ebull_test_conn, accession, ingest_status="parsed")
        with pytest.raises(ValueError, match="illegal transition"):
            transition_status(ebull_test_conn, accession, ingest_status="failed")
        ebull_test_conn.rollback()

    def test_rebuild_path_parsed_to_pending(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        accession = self._seed(ebull_test_conn)
        transition_status(ebull_test_conn, accession, ingest_status="fetched", raw_status="stored")
        transition_status(ebull_test_conn, accession, ingest_status="parsed", parser_version="v1")
        # Rebuild flips it back; parser_version stays so the rewash
        # detector can compare.
        transition_status(ebull_test_conn, accession, ingest_status="pending")
        row = get_manifest_row(ebull_test_conn, accession)
        assert row is not None
        assert row.ingest_status == "pending"
        assert row.parser_version == "v1"

    def test_missing_accession_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(ValueError, match="manifest row missing"):
            transition_status(ebull_test_conn, "DOES-NOT-EXIST", ingest_status="parsed")
        ebull_test_conn.rollback()


# ---------------------------------------------------------------------------
# iter_pending / iter_retryable
# ---------------------------------------------------------------------------


class TestIterators:
    def _seed_n(self, conn: psycopg.Connection[tuple], n: int, source: ManifestSource = "sec_form4") -> None:
        _seed_instrument(conn, iid=1, symbol="X", cik="0000000001")
        for i in range(n):
            record_manifest_entry(
                conn,
                f"ACC-{i:03d}",
                cik="0000000001",
                form="4",
                source=source,
                subject_type="issuer",
                subject_id="1",
                instrument_id=1,
                filed_at=datetime(2026, 1, 1 + i, tzinfo=UTC),
            )

    def test_iter_pending_returns_only_pending(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        self._seed_n(ebull_test_conn, 3)
        # Mark one as parsed
        transition_status(ebull_test_conn, "ACC-001", ingest_status="fetched", raw_status="stored")
        transition_status(ebull_test_conn, "ACC-001", ingest_status="parsed")
        rows = list(iter_pending(ebull_test_conn, source="sec_form4", limit=10))
        accessions = {r.accession_number for r in rows}
        assert accessions == {"ACC-000", "ACC-002"}

    def test_iter_pending_orders_by_filed_at_asc(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        self._seed_n(ebull_test_conn, 4)
        rows = list(iter_pending(ebull_test_conn, source="sec_form4", limit=4))
        assert [r.accession_number for r in rows] == ["ACC-000", "ACC-001", "ACC-002", "ACC-003"]

    def test_iter_pending_filters_source(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        record_manifest_entry(
            ebull_test_conn,
            "ACC-FORM4",
            cik="0000000001",
            form="4",
            source="sec_form4",
            subject_type="issuer",
            subject_id="1",
            instrument_id=1,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        record_manifest_entry(
            ebull_test_conn,
            "ACC-DEF14A",
            cik="0000000001",
            form="DEF 14A",
            source="sec_def14a",
            subject_type="issuer",
            subject_id="1",
            instrument_id=1,
            filed_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
        form4_rows = list(iter_pending(ebull_test_conn, source="sec_form4", limit=10))
        def14a_rows = list(iter_pending(ebull_test_conn, source="sec_def14a", limit=10))
        assert {r.accession_number for r in form4_rows} == {"ACC-FORM4"}
        assert {r.accession_number for r in def14a_rows} == {"ACC-DEF14A"}

    def test_iter_retryable_excludes_future_retry(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        self._seed_n(ebull_test_conn, 2)
        # one failed, retry in past = eligible
        transition_status(
            ebull_test_conn,
            "ACC-000",
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # one failed, retry in future = NOT eligible
        transition_status(
            ebull_test_conn,
            "ACC-001",
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2099, 1, 1, tzinfo=UTC),
        )
        rows = list(iter_retryable(ebull_test_conn, source="sec_form4", limit=10))
        assert {r.accession_number for r in rows} == {"ACC-000"}

    def test_iter_retryable_includes_null_retry(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        self._seed_n(ebull_test_conn, 1)
        transition_status(ebull_test_conn, "ACC-000", ingest_status="failed", error="x", next_retry_at=None)
        rows = list(iter_retryable(ebull_test_conn, source="sec_form4", limit=10))
        assert {r.accession_number for r in rows} == {"ACC-000"}


# ---------------------------------------------------------------------------
# Form mapping helpers
# ---------------------------------------------------------------------------


class TestFormMapping:
    def test_known_forms_map_to_expected_sources(self) -> None:
        assert map_form_to_source("4") == "sec_form4"
        assert map_form_to_source("4/A") == "sec_form4"
        assert map_form_to_source("13F-HR") == "sec_13f_hr"
        assert map_form_to_source("13F-HR/A") == "sec_13f_hr"
        assert map_form_to_source("SC 13D") == "sec_13d"
        assert map_form_to_source("SC 13D/A") == "sec_13d"
        assert map_form_to_source("SC 13G") == "sec_13g"
        assert map_form_to_source("DEF 14A") == "sec_def14a"
        assert map_form_to_source("10-K") == "sec_10k"

    def test_unknown_form_maps_to_none(self) -> None:
        assert map_form_to_source("S-1") is None
        assert map_form_to_source("424B5") is None
        assert map_form_to_source("CORRESP") is None
        assert map_form_to_source("") is None

    def test_whitespace_tolerant(self) -> None:
        assert map_form_to_source("13F-HR  ") == "sec_13f_hr"
        assert map_form_to_source("  4  ") == "sec_form4"

    def test_is_amendment_form(self) -> None:
        assert is_amendment_form("4/A") is True
        assert is_amendment_form("SC 13D/A") is True
        assert is_amendment_form("4") is False
        assert is_amendment_form("DEF 14A") is False

    def test_is_amendment_form_recognises_non_suffix_amendments(self) -> None:
        # Claude bot review on PR #878 (PREVENTION): DEFA14A and DEFR14A
        # are amendment forms that don't carry the ``/A`` suffix.
        # Without explicit handling, discovery callers would leave
        # those rows with ``is_amendment=False`` + null amends_accession,
        # silently breaking the amendment chain.
        assert is_amendment_form("DEFA14A") is True
        assert is_amendment_form("DEFR14A") is True
        # And both still map to sec_def14a (the parent source).
        # Codex pre-push review #939 caught the DEFR14A mapping gap:
        # without an entry in ``_FORM_TO_SOURCE`` the discovery callers
        # would skip the row at ``record_manifest_entry`` even though
        # ``is_amendment_form`` recognised it.
        assert map_form_to_source("DEFA14A") == "sec_def14a"
        assert map_form_to_source("DEFR14A") == "sec_def14a"

    def test_self_transition_explicit(self) -> None:
        # Bot review WARNING: ``failed -> failed`` should be EXPLICITLY
        # legal (re-fail records new error), not silently no-op'd by
        # a same-status short-circuit. Same for pending -> pending
        # (re-discovery). parsed/tombstoned have NO self-loop — those
        # must go through ``pending`` for the rebuild gate.
        from app.services.sec_manifest import _ALLOWED_TRANSITIONS

        assert "failed" in _ALLOWED_TRANSITIONS["failed"]
        assert "pending" in _ALLOWED_TRANSITIONS["pending"]
        assert "parsed" not in _ALLOWED_TRANSITIONS["parsed"]
        assert "tombstoned" not in _ALLOWED_TRANSITIONS["tombstoned"]
        # fetched is transient — no self-loop
        assert "fetched" not in _ALLOWED_TRANSITIONS["fetched"]


# ---------------------------------------------------------------------------
# ingested_at on observations (migration 119)
# ---------------------------------------------------------------------------


class TestIngestedAtOnObservations:
    """Spec v3 finding #1: every ``ownership_*_observations`` table
    needs ``ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` AND every
    UPSERT (including DO UPDATE) must bump the column so the repair
    sweep in #873 keys on max-of-``ingested_at`` cleanly."""

    _OBS_TABLES: tuple[str, ...] = (
        "ownership_insiders_observations",
        "ownership_institutions_observations",
        "ownership_blockholders_observations",
        "ownership_treasury_observations",
        "ownership_def14a_observations",
    )

    def test_every_observations_table_has_ingested_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        for table in self._OBS_TABLES:
            with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_name = 'ingested_at'
                    """,
                    (table,),
                )
                row = cur.fetchone()
            assert row is not None, f"{table} missing ingested_at column"
            assert row["is_nullable"] == "NO", f"{table}.ingested_at must be NOT NULL"
            # Default ``clock_timestamp()`` per Codex pre-push finding —
            # ``NOW()`` (= transaction_timestamp) would lock all rows in
            # a batch INSERT to the transaction-start time, defeating
            # the per-row repair sweep watermark.
            default = (row["column_default"] or "").lower()
            assert "clock_timestamp()" in default, (
                f"{table}.ingested_at must default to clock_timestamp(), got {row['column_default']!r}"
            )

    def test_insider_upsert_bumps_ingested_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        run_id = uuid4()

        def _record() -> None:
            record_insider_observation(
                ebull_test_conn,
                instrument_id=1,
                holder_cik="0000000001",
                holder_name="Alice",
                ownership_nature="direct",
                source="form4",
                source_document_id="DOC-1",
                source_accession="ACC-1",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 1, 1),
                ingest_run_id=run_id,
                shares=Decimal("100"),
            )

        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_insiders_observations WHERE source_document_id = %s",
                ("DOC-1",),
            )
            row = cur.fetchone()
            assert row is not None
            t1 = row[0]

        time.sleep(0.05)
        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_insiders_observations WHERE source_document_id = %s",
                ("DOC-1",),
            )
            row = cur.fetchone()
            assert row is not None
            t2 = row[0]

        assert t2 > t1, "ingested_at should bump on every UPSERT (DO UPDATE)"

    def test_institution_upsert_bumps_ingested_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        run_id = uuid4()

        def _record() -> None:
            record_institution_observation(
                ebull_test_conn,
                instrument_id=1,
                filer_cik="0001364742",
                filer_name="BlackRock",
                filer_type="INV",
                ownership_nature="economic",
                source="13f",
                source_document_id="DOC-13F",
                source_accession="ACC-13F",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 14, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 3, 31),
                ingest_run_id=run_id,
                shares=Decimal("1000000"),
                market_value_usd=Decimal("123456789"),
                voting_authority="SOLE",
            )

        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_institutions_observations WHERE source_document_id = %s",
                ("DOC-13F",),
            )
            row = cur.fetchone()
            assert row is not None
            t1 = row[0]

        time.sleep(0.05)
        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_institutions_observations WHERE source_document_id = %s",
                ("DOC-13F",),
            )
            row = cur.fetchone()
            assert row is not None
            t2 = row[0]

        assert t2 > t1

    def test_blockholder_upsert_bumps_ingested_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        run_id = uuid4()

        def _record() -> None:
            record_blockholder_observation(
                ebull_test_conn,
                instrument_id=1,
                reporter_cik="0001234567",
                reporter_name="Cohen",
                ownership_nature="beneficial",
                submission_type="SCHEDULE 13D",
                status_flag="active",
                source="13d",
                source_document_id="DOC-13D",
                source_accession="ACC-13D",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 3, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 3, 1),
                ingest_run_id=run_id,
                aggregate_amount_owned=Decimal("75000000"),
                percent_of_class=Decimal("12.5"),
            )

        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_blockholders_observations WHERE source_document_id = %s",
                ("DOC-13D",),
            )
            row = cur.fetchone()
            assert row is not None
            t1 = row[0]

        time.sleep(0.05)
        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_blockholders_observations WHERE source_document_id = %s",
                ("DOC-13D",),
            )
            row = cur.fetchone()
            assert row is not None
            t2 = row[0]

        assert t2 > t1

    def test_treasury_upsert_bumps_ingested_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        run_id = uuid4()

        def _record() -> None:
            record_treasury_observation(
                ebull_test_conn,
                instrument_id=1,
                source="xbrl_dei",
                source_document_id="DOC-XBRL",
                source_accession="ACC-XBRL",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 1, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=run_id,
                treasury_shares=Decimal("100000000"),
            )

        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_treasury_observations WHERE source_document_id = %s",
                ("DOC-XBRL",),
            )
            row = cur.fetchone()
            assert row is not None
            t1 = row[0]

        time.sleep(0.05)
        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_treasury_observations WHERE source_document_id = %s",
                ("DOC-XBRL",),
            )
            row = cur.fetchone()
            assert row is not None
            t2 = row[0]

        assert t2 > t1

    def test_def14a_upsert_bumps_ingested_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        run_id = uuid4()

        def _record() -> None:
            record_def14a_observation(
                ebull_test_conn,
                instrument_id=1,
                holder_name="Vanguard",
                holder_role=None,
                ownership_nature="beneficial",
                source="def14a",
                source_document_id="DOC-DEF14A",
                source_accession="ACC-DEF14A",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 4, 1, tzinfo=UTC),
                period_start=None,
                period_end=date(2026, 3, 31),
                ingest_run_id=run_id,
                shares=Decimal("20000000"),
                percent_of_class=Decimal("8.5"),
            )

        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_def14a_observations WHERE source_document_id = %s",
                ("DOC-DEF14A",),
            )
            row = cur.fetchone()
            assert row is not None
            t1 = row[0]

        time.sleep(0.05)
        _record()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT ingested_at FROM ownership_def14a_observations WHERE source_document_id = %s",
                ("DOC-DEF14A",),
            )
            row = cur.fetchone()
            assert row is not None
            t2 = row[0]

        assert t2 > t1


# ---------------------------------------------------------------------------
# Backfill from existing tombstone tables
# ---------------------------------------------------------------------------


class TestBackfillFromTombstones:
    def test_backfill_def14a_log(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
        _seed_instrument(ebull_test_conn, iid=1701, symbol="AAPL", cik="0000320193")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO def14a_ingest_log (accession_number, issuer_cik, status, fetched_at)
                VALUES ('ACC-DEF', '0000320193', 'success', NOW())
                """
            )
        ebull_test_conn.commit()

        from scripts.backfill_864_sec_manifest import backfill_def14a

        n = backfill_def14a(ebull_test_conn, dry_run=False)
        ebull_test_conn.commit()
        assert n == 1

        row = get_manifest_row(ebull_test_conn, "ACC-DEF")
        assert row is not None
        assert row.source == "sec_def14a"
        assert row.subject_type == "issuer"
        assert row.instrument_id == 1701
        assert row.ingest_status == "parsed"

    def test_backfill_def14a_partial_becomes_tombstoned(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO def14a_ingest_log (accession_number, issuer_cik, status, error, fetched_at)
                VALUES ('ACC-PARTIAL', '0000000001', 'partial', 'no table', NOW())
                """
            )
        ebull_test_conn.commit()

        from scripts.backfill_864_sec_manifest import backfill_def14a

        backfill_def14a(ebull_test_conn, dry_run=False)
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "ACC-PARTIAL")
        assert row is not None
        assert row.ingest_status == "tombstoned"
        assert row.error == "no table"

    def test_backfill_institutional_log(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_institutional_filer(ebull_test_conn, cik="0001364742", name="BlackRock")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO institutional_holdings_ingest_log (
                    accession_number, filer_cik, period_of_report, status, fetched_at
                )
                VALUES ('ACC-13F', '0001364742', '2026-03-31', 'success', NOW())
                """
            )
        ebull_test_conn.commit()

        from scripts.backfill_864_sec_manifest import backfill_institutional_holdings

        n = backfill_institutional_holdings(ebull_test_conn, dry_run=False)
        ebull_test_conn.commit()
        assert n == 1

        row = get_manifest_row(ebull_test_conn, "ACC-13F")
        assert row is not None
        assert row.subject_type == "institutional_filer"
        assert row.instrument_id is None
        assert row.source == "sec_13f_hr"
        assert row.ingest_status == "parsed"

    def test_backfill_insider_filings(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1701, symbol="AAPL", cik="0000320193")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_filings (
                    accession_number, instrument_id, document_type,
                    issuer_cik, primary_document_url, fetched_at, parser_version, is_tombstone
                ) VALUES (
                    'ACC-FORM4-1', 1701, '4',
                    '0000320193', 'https://sec.gov/...', NOW(), 2, FALSE
                ),
                (
                    'ACC-FORM4-2', 1701, '4',
                    '0000320193', NULL, NOW(), 2, TRUE
                )
                """
            )
        ebull_test_conn.commit()

        from scripts.backfill_864_sec_manifest import backfill_insider_filings

        n = backfill_insider_filings(ebull_test_conn, dry_run=False)
        ebull_test_conn.commit()
        assert n == 2

        parsed_row = get_manifest_row(ebull_test_conn, "ACC-FORM4-1")
        assert parsed_row is not None
        assert parsed_row.ingest_status == "parsed"
        assert parsed_row.source == "sec_form4"

        tomb_row = get_manifest_row(ebull_test_conn, "ACC-FORM4-2")
        assert tomb_row is not None
        assert tomb_row.ingest_status == "tombstoned"

    def test_backfill_dry_run_makes_no_writes(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1701, symbol="AAPL", cik="0000320193")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO def14a_ingest_log (accession_number, issuer_cik, status, fetched_at)
                VALUES ('ACC-DRY', '0000320193', 'success', NOW())
                """
            )
        ebull_test_conn.commit()

        from scripts.backfill_864_sec_manifest import backfill_def14a

        n = backfill_def14a(ebull_test_conn, dry_run=True)
        # dry-run does not commit; rollback to clear any speculative state
        ebull_test_conn.rollback()
        assert n == 1  # counted

        row = get_manifest_row(ebull_test_conn, "ACC-DRY")
        assert row is None  # no actual write
