"""#1233 PR-1a / #1349 — bulk-path unresolved-CUSIP capture.

Verifies (post-#1349 per-(cusip, source) grain, sql/189):

* Migration ``sql/189`` applies cleanly to ``ebull_test_template``
  (verified implicitly — the per-worker DB is built from the
  template, so a broken migration would prevent ``ebull_test_conn``
  from connecting). Schema shape asserted explicitly.
* ``record_unresolved_cusip_from_bulk`` upserts ONE row per
  ``(cusip, source)``. Repeat sightings bump ``observation_count``
  (best-effort heuristic) and widen the
  ``first_period_end``/``last_period_end`` range monotonically.
* The writer-side retention gate drops sightings with
  ``period_end < cutoff`` (spec §4).
* Bulk 13F / N-PORT ingest fixtures land one row per unresolved
  CUSIP.
* Legacy ``_record_unresolved_cusip`` still works (writes with
  ``source=NULL``) and shares the table without colliding with
  bulk rows.
* The legacy resolver / extid sweep (``cusip_resolver.py``) ONLY
  reads/mutates legacy rows (``source IS NULL``); bulk rows for
  the same CUSIP are untouched. Codex BLOCKING + HIGH on PR-1a
  pre-push review.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.cusip_resolver import (
    flush_unresolved_cusips_bulk,
    record_unresolved_cusip_from_bulk,
    resolve_unresolved_cusips,
    sweep_resolvable_unresolved_cusips,
)
from app.services.institutional_holdings import _record_unresolved_cusip
from app.services.sec_13f_dataset_ingest import ingest_13f_dataset_archive
from app.services.sec_nport_dataset_ingest import ingest_nport_dataset_archive
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# Constants — picked so all test data passes the 8-quarter retention gate
# (PR6/PR7) regardless of when the suite runs in 2026.
# ---------------------------------------------------------------------------


# Latest completed quarter as of 2026-05-22 = 2026-03-31. The retention
# gate admits the 8 most recent quarter-ends inclusive — 2024Q2 onward.
# Using the latest completed quarter end means the tests stay in-window
# for the next ~24 months without churn.
_PERIOD_END = date(2026, 3, 31)
_FILED_AT = "2026-05-15"  # post-2023-01-03 dollars cutover

# Writer-side retention floor passed to the bulk writers (#1349 spec §4).
# Sits safely below every in-window period the fixtures use, and above
# the deliberately-stale period in the retention-gate tests.
_CUTOFF = date(2024, 6, 30)
_STALE_PERIOD = date(2020, 3, 31)  # < _CUTOFF → writer gate drops it


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _count_bulk_rows(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM unresolved_13f_cusips WHERE source IS NOT NULL")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _count_legacy_rows(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM unresolved_13f_cusips WHERE source IS NULL")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _build_13f_zip(
    *,
    submissions: list[dict[str, str]],
    coverpages: list[dict[str, str]],
    infotable: list[dict[str, str]],
) -> bytes:
    def _to_tsv(rows: list[dict[str, str]]) -> str:
        if not rows:
            return ""
        fieldnames = sorted({k for row in rows for k in row.keys()})
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return buf.getvalue()

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("SUBMISSION.tsv", _to_tsv(submissions))
        zf.writestr("COVERPAGE.tsv", _to_tsv(coverpages))
        zf.writestr("INFOTABLE.tsv", _to_tsv(infotable))
    return out.getvalue()


def _build_nport_zip(
    *,
    submissions: list[dict[str, str]],
    registrants: list[dict[str, str]],
    fund_info: list[dict[str, str]],
    holdings: list[dict[str, str]],
) -> bytes:
    def _to_tsv(rows: list[dict[str, str]]) -> str:
        if not rows:
            return ""
        fieldnames = sorted({k for row in rows for k in row.keys()})
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return buf.getvalue()

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("SUBMISSION.tsv", _to_tsv(submissions))
        zf.writestr("REGISTRANT.tsv", _to_tsv(registrants))
        zf.writestr("FUND_REPORTED_INFO.tsv", _to_tsv(fund_info))
        zf.writestr("FUND_REPORTED_HOLDING.tsv", _to_tsv(holdings))
    return out.getvalue()


# ---------------------------------------------------------------------------
# Schema / migration smoke
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestMigrationSchema:
    def test_partial_unique_indexes_present(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Both partial UNIQUE indexes (recreated by sql/189) exist on
        the worker's private DB (built from ``ebull_test_template``
        which has all migrations applied)."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'unresolved_13f_cusips'
                  AND indexname IN (
                    'unresolved_13f_cusips_bulk_idx',
                    'unresolved_13f_cusips_legacy_idx'
                  )
                ORDER BY indexname
                """,
            )
            names = [r[0] for r in cur.fetchall()]
        assert names == [
            "unresolved_13f_cusips_bulk_idx",
            "unresolved_13f_cusips_legacy_idx",
        ]

    def test_legacy_pk_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """The legacy PRIMARY KEY on ``cusip`` is gone after sql/164;
        partial UNIQUE indexes take over."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'unresolved_13f_cusips'::regclass
                  AND contype = 'p'
                """,
            )
            assert cur.fetchone() is None

    def test_per_cusip_grain_columns(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """sql/189 shape: ``first_period_end``/``last_period_end``
        present, per-(filer, period) columns GONE, legacy columns
        nullable (bulk rows leave issuer name / accession empty).

        No re-run idempotency test for sql/189: the table swap reads
        the dropped ``period_end`` column from the pre-189 shape, so a
        replay against a migrated DB is structurally impossible — the
        runner's ``schema_migrations`` + content-sha guard (#1333) is
        the re-application protection.
        """
        expected = {
            "source": ("text", "YES"),
            "first_period_end": ("date", "YES"),
            "last_period_end": ("date", "YES"),
            "name_of_issuer": ("text", "YES"),
            "last_accession_number": ("text", "YES"),
        }
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'unresolved_13f_cusips'
                """,
            )
            all_cols = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        assert {k: v for k, v in all_cols.items() if k in expected} == expected
        # #1349 — the fine-grain columns must be gone.
        assert "filer_cik" not in all_cols
        assert "period_end" not in all_cols


# ---------------------------------------------------------------------------
# Helper unit behaviour
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestRecordUnresolvedCusipFromBulk:
    def test_first_insert_creates_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip="00BULK0001",
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, source, observation_count,
                       first_period_end, last_period_end,
                       name_of_issuer, last_accession_number
                FROM unresolved_13f_cusips
                WHERE cusip = %s
                """,
                ("00BULK0001",),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == "00BULK0001"
        assert row[1] == "bulk_13f_dataset"
        assert row[2] == 1
        assert row[3] == _PERIOD_END
        assert row[4] == _PERIOD_END
        # Bulk path leaves issuer name + accession blank; OpenFIGI
        # sweep (PR-1b) fills name_of_issuer.
        assert row[5] is None
        assert row[6] is None

    def test_repeat_sightings_bump_count_not_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """#1349 — same cusip, ANY (filer, period) combination: still
        ONE row; observation_count accumulates; period range widens."""
        for filer, period in (
            ("0001234567", _PERIOD_END),
            ("0009999999", _PERIOD_END),  # different filer
            ("0001234567", date(2025, 12, 31)),  # different period
        ):
            record_unresolved_cusip_from_bulk(
                ebull_test_conn,
                cusip="00BULK0002",
                filer_cik=filer,
                period_end=period,
                source="bulk_13f_dataset",
                cutoff=_CUTOFF,
            )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT observation_count, first_period_end, last_period_end
                FROM unresolved_13f_cusips
                WHERE cusip = '00BULK0002'
                """,
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (3, date(2025, 12, 31), _PERIOD_END)

    def test_different_source_creates_separate_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip="00BULK0005",
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip="00BULK0005",
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_nport_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert _count_bulk_rows(ebull_test_conn) == 2

    def test_out_of_retention_sighting_is_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Writer-side retention gate (#1349 spec §4): a sighting with
        ``period_end < cutoff`` never reaches the table."""
        before = _count_bulk_rows(ebull_test_conn)
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip="00BULK0006",
            filer_cik="0001234567",
            period_end=_STALE_PERIOD,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert _count_bulk_rows(ebull_test_conn) == before


# ---------------------------------------------------------------------------
# #1295 — COPY-based bulk flush helper unit behaviour
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestFlushUnresolvedCusipsBulk:
    """Regression coverage for :func:`flush_unresolved_cusips_bulk`.

    Pre-#1295 the bulk ingesters drained their unresolved-CUSIP
    buffer via a per-row INSERT + SAVEPOINT loop. Post-#1295 they
    call :func:`flush_unresolved_cusips_bulk`, which streams the
    whole buffer into a TEMP staging table via ``COPY`` then drains
    via an aggregated ``INSERT...SELECT...ON CONFLICT DO UPDATE``
    onto the per-(cusip, source) grain (#1349, sql/189).
    """

    def test_empty_buffer_returns_zero(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        before = _count_bulk_rows(ebull_test_conn)
        written = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            [],
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert written == 0
        assert _count_bulk_rows(ebull_test_conn) == before

    def test_multi_row_buffer_inserts_one_row_per_cusip(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        before = _count_bulk_rows(ebull_test_conn)
        buffer = [
            ("00FLUSH001", "0001111111", _PERIOD_END),
            ("00FLUSH002", "0001111111", _PERIOD_END),
            ("00FLUSH003", "0002222222", _PERIOD_END),
        ]
        written = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert written == 3
        assert _count_bulk_rows(ebull_test_conn) == before + 3

    def test_reflush_same_buffer_keeps_row_identity(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Row identity stays stable under re-flush. NOTE (#1349):
        the return now counts groups touched (inserted OR updated) —
        a re-flush returns the group count, not 0 — and
        ``observation_count`` accumulates (best-effort heuristic,
        spec §4 conscious tradeoff)."""
        buffer = [
            ("00FLUSH010", "0001111111", _PERIOD_END),
            ("00FLUSH011", "0001111111", _PERIOD_END),
        ]
        first = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        second = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert first == 2
        assert second == 2  # groups updated, not newly inserted
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), SUM(observation_count)
                FROM unresolved_13f_cusips
                WHERE cusip IN ('00FLUSH010', '00FLUSH011')
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert (int(row[0]), int(row[1])) == (2, 4)

    def test_same_cusip_aggregates_to_one_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """#1349 — different (filer, period) sightings of one cusip
        collapse to ONE row with the count + period range."""
        cusip = "00FLUSH020"
        buffer = [
            (cusip, "0001111111", _PERIOD_END),
            (cusip, "0002222222", _PERIOD_END),
            (cusip, "0001111111", date(2025, 12, 31)),
        ]
        written = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT observation_count, first_period_end, last_period_end
                FROM unresolved_13f_cusips WHERE cusip = %s
                """,
                (cusip,),
            )
            rows = cur.fetchall()
        assert rows == [(3, date(2025, 12, 31), _PERIOD_END)]

    def test_out_of_retention_rows_filtered_at_writer(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Writer-side retention gate (#1349 spec §4): the 13F walk
        buffers markers BEFORE its retention gate, so the flush must
        drop stale periods. A cusip with ONLY stale sightings gets no
        row; a mixed-period cusip keeps only the in-window range."""
        buffer = [
            ("00FLUSH050", "0001111111", _STALE_PERIOD),  # stale-only → no row
            ("00FLUSH051", "0001111111", _STALE_PERIOD),  # mixed → in-window only
            ("00FLUSH051", "0001111111", _PERIOD_END),
        ]
        written = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, observation_count, first_period_end, last_period_end
                FROM unresolved_13f_cusips
                WHERE cusip IN ('00FLUSH050', '00FLUSH051')
                """
            )
            rows = cur.fetchall()
        assert rows == [("00FLUSH051", 1, _PERIOD_END, _PERIOD_END)]

    def test_whitespace_and_case_normalisation(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Helper strips + upper-cases CUSIP so downstream lookups
        (e.g. by the OpenFIGI sweep) match the canonical form
        regardless of caller hygiene."""
        buffer = [
            ("  00flush030  ", "  0003333333  ", _PERIOD_END),
        ]
        written = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_nport_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, source
                FROM unresolved_13f_cusips
                WHERE cusip = '00FLUSH030'
                """
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == ("00FLUSH030", "bulk_nport_dataset")

    def test_helper_failure_isolated_by_wrapper_savepoint(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The wrapper :func:`_flush_unresolved_buffer` in each
        ingester catches helper raises and increments
        ``parse_errors`` so a flush failure must NOT poison the
        outer archive transaction.

        Codex 2 pre-push BLOCKING finding on #1295: without the
        wrapper's savepoint, a CHECK / FK / OOM raise inside the
        helper leaves the connection in ``InFailedSqlTransaction``;
        the next observation-table query in the archive flow fails
        and the entire archive rolls back. The fix wraps the
        helper call in ``with conn.transaction():`` so a raise
        unwinds to the savepoint and the archive tx survives.
        """
        from app.services import sec_13f_dataset_ingest as ingest_mod
        from app.services.sec_13f_dataset_ingest import Form13FIngestResult

        def _boom(*_args: object, **_kwargs: object) -> int:
            raise RuntimeError("forced helper failure for test")

        monkeypatch.setattr(ingest_mod, "flush_unresolved_cusips_bulk", _boom)

        result = Form13FIngestResult()
        ingest_mod._flush_unresolved_buffer(
            ebull_test_conn,
            buffer=[("00FLUSH900", "0009000000", _PERIOD_END)],
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
            result=result,
        )

        # Wrapper recorded the failure but did NOT raise.
        assert result.parse_errors == 1

        # Outer tx is alive: a plain SELECT runs without
        # InFailedSqlTransaction. Pre-fix this would raise.
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

        # Commit succeeds — the failed flush did not poison the tx.
        ebull_test_conn.commit()

        # And the helper failed cleanly: no row reached the table.
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM unresolved_13f_cusips WHERE cusip = '00FLUSH900'")
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 0

    def test_drops_rows_with_missing_required_field(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Buffer rows with empty cusip, empty filer_cik, or null
        period_end are silently skipped by the helper — the caller
        is expected to have filtered already, but the helper is
        defensive so a malformed triple never aborts the flush.
        Matches the pre-#1295 per-row SAVEPOINT semantic without
        spending a SAVEPOINT to do it.
        """
        before = _count_bulk_rows(ebull_test_conn)
        buffer = [
            ("", "0001111111", _PERIOD_END),
            ("00FLUSH040", "", _PERIOD_END),
            ("00FLUSH041", "0001111111", None),  # type: ignore[arg-type]
            ("00FLUSH042", "0001111111", _PERIOD_END),
        ]
        written = flush_unresolved_cusips_bulk(
            ebull_test_conn,
            buffer,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()
        assert written == 1
        assert _count_bulk_rows(ebull_test_conn) == before + 1


# ---------------------------------------------------------------------------
# Legacy / bulk coexistence
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestLegacyWriterCoexistence:
    def test_legacy_writer_still_works(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Legacy ``_record_unresolved_cusip`` still upserts one row
        per CUSIP under the new partial UNIQUE
        ``unresolved_13f_cusips_legacy_idx``."""
        _record_unresolved_cusip(
            ebull_test_conn,
            cusip="00LEGACY01",
            name_of_issuer="Legacy Issuer Inc",
            accession_number="0000000-00-000001",
        )
        _record_unresolved_cusip(
            ebull_test_conn,
            cusip="00LEGACY01",
            name_of_issuer="Legacy Issuer Inc Updated",
            accession_number="0000000-00-000002",
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, name_of_issuer, last_accession_number,
                       observation_count, source
                FROM unresolved_13f_cusips
                WHERE cusip = %s
                """,
                ("00LEGACY01",),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        cusip, name, accn, obs_count, source = rows[0]
        assert cusip == "00LEGACY01"
        assert name == "Legacy Issuer Inc Updated"
        assert accn == "0000000-00-000002"
        assert obs_count == 2
        assert source is None

    def test_legacy_and_bulk_coexist_for_same_cusip(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """A CUSIP can have ONE legacy row (source IS NULL) AND
        multiple bulk rows (source IS NOT NULL) simultaneously —
        the partial indexes don't cross-collide."""
        cusip = "00MIX00001"
        _record_unresolved_cusip(
            ebull_test_conn,
            cusip=cusip,
            name_of_issuer="Mixed Issuer Inc",
            accession_number="0000000-00-000099",
        )
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0009999999",
            period_end=_PERIOD_END,
            source="bulk_nport_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM unresolved_13f_cusips WHERE cusip = %s",
                (cusip,),
            )
            row = cur.fetchone()
            assert row is not None
        assert int(row[0]) == 3


# ---------------------------------------------------------------------------
# 13F bulk-ingest integration — five unresolved CUSIPs land five rows
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestBulk13FIngestUnresolvedCapture:
    def test_five_unresolved_cusips_yield_five_bulk_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """A 13F archive with 5 unresolved CUSIPs writes 5 rows into
        ``unresolved_13f_cusips`` with ``source='bulk_13f_dataset'``.
        Zero resolved rows are emitted because we seed no
        ``external_identifiers``."""
        accession = "0001234567-26-000001"
        submissions = [
            {
                "ACCESSION_NUMBER": accession,
                "CIK": "1234567",
                "FILING_DATE": _FILED_AT,
            },
        ]
        coverpages = [
            {
                "ACCESSION_NUMBER": accession,
                "FILINGMANAGER_NAME": "Test Fund",
                "REPORTCALENDARORQUARTER": _PERIOD_END.isoformat(),
            },
        ]
        unresolved_cusips = [
            "U0000001A1",
            "U0000002B2",
            "U0000003C3",
            "U0000004D4",
            "U0000005E5",
        ]
        infotable = [
            {
                "ACCESSION_NUMBER": accession,
                "CUSIP": cusip,
                "VALUE": "1000",
                "SSHPRNAMT": "100",
                "VOTING_AUTH_SOLE": "100",
            }
            for cusip in unresolved_cusips
        ]
        archive_path = tmp_path / "form13f_unresolved.zip"
        archive_path.write_bytes(
            _build_13f_zip(
                submissions=submissions,
                coverpages=coverpages,
                infotable=infotable,
            )
        )

        result = ingest_13f_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_skipped_unresolved_cusip == 5
        assert result.rows_written == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, observation_count, first_period_end,
                       last_period_end, source
                FROM unresolved_13f_cusips
                WHERE source = 'bulk_13f_dataset'
                ORDER BY cusip
                """,
            )
            rows = cur.fetchall()
        assert len(rows) == 5
        assert [r[0] for r in rows] == sorted(unresolved_cusips)
        # One sighting each, period range collapsed to the single
        # cover period (single-submission fixture).
        assert all(r[1] == 1 for r in rows)
        assert all(r[2] == _PERIOD_END for r in rows)
        assert all(r[3] == _PERIOD_END for r in rows)
        assert all(r[4] == "bulk_13f_dataset" for r in rows)


# ---------------------------------------------------------------------------
# N-PORT bulk-ingest integration — five unresolved CUSIPs land five rows
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestBulkNPortIngestUnresolvedCapture:
    def test_five_unresolved_cusips_yield_five_bulk_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """A N-PORT archive with 5 EC/Long/NS unresolved CUSIPs writes
        5 rows into ``unresolved_13f_cusips`` with
        ``source='bulk_nport_dataset'``."""
        accession = "0007654321-26-000001"
        submissions = [
            {
                "ACCESSION_NUMBER": accession,
                "FILING_DATE": _FILED_AT,
                "SUB_TYPE": "NPORT-P",
                "REPORT_DATE": _PERIOD_END.isoformat(),
            },
        ]
        registrants = [
            {
                "ACCESSION_NUMBER": accession,
                "CIK": "7654321",
                "REGISTRANT_NAME": "Test Trust",
            },
        ]
        fund_info = [
            {
                "ACCESSION_NUMBER": accession,
                "SERIES_ID": "S000004310",
                "SERIES_NAME": "Test Equity Series",
            },
        ]
        unresolved_cusips = [
            "N0000001A1",
            "N0000002B2",
            "N0000003C3",
            "N0000004D4",
            "N0000005E5",
        ]
        holdings = [
            {
                "ACCESSION_NUMBER": accession,
                "HOLDING_ID": str(i),
                "ISSUER_CUSIP": cusip,
                "ASSET_CAT": "EC",
                "PAYOFF_PROFILE": "Long",
                "UNIT": "NS",
                "BALANCE": "1000",
                "CURRENCY_CODE": "USD",
                "CURRENCY_VALUE": "10000",
            }
            for i, cusip in enumerate(unresolved_cusips, start=1)
        ]
        archive_path = tmp_path / "nport_unresolved.zip"
        archive_path.write_bytes(
            _build_nport_zip(
                submissions=submissions,
                registrants=registrants,
                fund_info=fund_info,
                holdings=holdings,
            )
        )

        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_skipped_unresolved_cusip == 5
        assert result.rows_written == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, observation_count, first_period_end,
                       last_period_end, source
                FROM unresolved_13f_cusips
                WHERE source = 'bulk_nport_dataset'
                ORDER BY cusip
                """,
            )
            rows = cur.fetchall()
        assert len(rows) == 5
        assert [r[0] for r in rows] == sorted(unresolved_cusips)
        assert all(r[1] == 1 for r in rows)
        assert all(r[2] == _PERIOD_END for r in rows)
        assert all(r[3] == _PERIOD_END for r in rows)
        assert all(r[4] == "bulk_nport_dataset" for r in rows)


# ---------------------------------------------------------------------------
# Resolved rows MUST NOT land in the unresolved table — guards against the
# capture branch leaking past the cusip_map.get() check.
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestResolvedCusipDoesNotCapture:
    def test_resolved_cusip_in_13f_archive_is_not_captured(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """If the CUSIP resolves via external_identifiers, the ingester
        writes the observation and DOES NOT add a row to
        ``unresolved_13f_cusips``."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
                "VALUES (%s, %s, %s, 'USD', TRUE)",
                (90001, "RES1", "Resolved Co"),
            )
            cur.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
                (90001, "RESOLVED01"),
            )
        ebull_test_conn.commit()

        accession = "0001234567-26-000099"
        archive_path = tmp_path / "form13f_resolved.zip"
        archive_path.write_bytes(
            _build_13f_zip(
                submissions=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "CIK": "1234567",
                        "FILING_DATE": _FILED_AT,
                    }
                ],
                coverpages=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "FILINGMANAGER_NAME": "Test Fund",
                        "REPORTCALENDARORQUARTER": _PERIOD_END.isoformat(),
                    }
                ],
                infotable=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "CUSIP": "RESOLVED01",
                        "VALUE": "1000",
                        "SSHPRNAMT": "100",
                        "VOTING_AUTH_SOLE": "100",
                    }
                ],
            )
        )

        bulk_before = _count_bulk_rows(ebull_test_conn)
        legacy_before = _count_legacy_rows(ebull_test_conn)
        result = ingest_13f_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 1
        assert result.rows_skipped_unresolved_cusip == 0
        assert _count_bulk_rows(ebull_test_conn) == bulk_before
        assert _count_legacy_rows(ebull_test_conn) == legacy_before

    def test_resolved_cusip_in_nport_archive_is_not_captured(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """N-PORT mirror of the 13F resolved-CUSIP no-capture invariant.
        Codex MED on PR-1a pre-push: 13F path had this, N-PORT didn't.
        """
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
                "VALUES (%s, %s, %s, 'USD', TRUE)",
                (90002, "RES2", "Resolved N-PORT Co"),
            )
            cur.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
                (90002, "RESOLVED02"),
            )
        ebull_test_conn.commit()

        accession = "0007654321-26-000099"
        archive_path = tmp_path / "nport_resolved.zip"
        archive_path.write_bytes(
            _build_nport_zip(
                submissions=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "FILING_DATE": _FILED_AT,
                        "SUB_TYPE": "NPORT-P",
                        "REPORT_DATE": _PERIOD_END.isoformat(),
                    }
                ],
                registrants=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "CIK": "7654321",
                        "REGISTRANT_NAME": "Test Trust",
                    }
                ],
                fund_info=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "SERIES_ID": "S000004310",
                        "SERIES_NAME": "Test Equity Series",
                    }
                ],
                holdings=[
                    {
                        "ACCESSION_NUMBER": accession,
                        "HOLDING_ID": "1",
                        "ISSUER_CUSIP": "RESOLVED02",
                        "ASSET_CAT": "EC",
                        "PAYOFF_PROFILE": "Long",
                        "UNIT": "NS",
                        "BALANCE": "1000",
                        "CURRENCY_CODE": "USD",
                        "CURRENCY_VALUE": "10000",
                    }
                ],
            )
        )

        bulk_before = _count_bulk_rows(ebull_test_conn)
        legacy_before = _count_legacy_rows(ebull_test_conn)
        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 1
        assert result.rows_skipped_unresolved_cusip == 0
        assert _count_bulk_rows(ebull_test_conn) == bulk_before
        assert _count_legacy_rows(ebull_test_conn) == legacy_before


# ---------------------------------------------------------------------------
# Partition isolation — the legacy resolver / extid sweep must NOT touch
# bulk rows. Codex BLOCKING + HIGH on PR-1a pre-push review.
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestLegacyResolverIgnoresBulkRows:
    def test_resolve_unresolved_cusips_skips_bulk_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """``resolve_unresolved_cusips`` reads only legacy rows. A bulk
        row with NULL ``name_of_issuer`` would otherwise be picked up
        and ``_normalise_name(None)`` would crash or silently
        tombstone the bulk row as ``unresolvable``."""
        # Seed one bulk row only.
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip="BULKONLY01",
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()

        # No legacy rows + bulk rows are filtered → report sees zero
        # candidates.
        report = resolve_unresolved_cusips(ebull_test_conn)
        assert report.candidates_seen == 0
        assert report.tombstoned_unresolvable == 0

        # The bulk row is untouched: status still NULL.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s AND source = %s",
                ("BULKONLY01", "bulk_13f_dataset"),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is None

    def test_extid_sweep_skips_bulk_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """``sweep_resolvable_unresolved_cusips`` reads only legacy
        rows. A bulk row with NULL ``last_accession_number`` would
        otherwise be picked up and the rewash call would target
        accession ``"None"``."""
        # Seed instrument + extid mapping so the sweep would otherwise
        # pick the row.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
                "VALUES (%s, %s, %s, 'USD', TRUE)",
                (90003, "BULK3", "Bulk Test Inc"),
            )
            cur.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
                (90003, "BULKSWEEP1"),
            )
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip="BULKSWEEP1",
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()

        report = sweep_resolvable_unresolved_cusips(ebull_test_conn)
        assert report.candidates_seen == 0
        assert report.promoted == 0

        # Bulk row stays pending; status untouched.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s AND source = %s",
                ("BULKSWEEP1", "bulk_13f_dataset"),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is None

    def test_legacy_tombstone_does_not_mutate_bulk_rows_for_same_cusip(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """A legacy tombstone (e.g. ``unresolvable`` when no candidate
        crosses the fuzzy threshold) MUST mutate only the legacy row.
        Bulk rows sharing the same CUSIP keep ``resolution_status =
        NULL`` so PR-1b's OpenFIGI sweep can still pick them up.

        Codex MED follow-up on PR-1a: previous tests covered DELETE
        isolation but not UPDATE isolation."""
        cusip = "TOMBSHARE1"

        # Two rows: one legacy + one bulk, same CUSIP.
        _record_unresolved_cusip(
            ebull_test_conn,
            cusip=cusip,
            name_of_issuer="Z-Some Weird Unmatchable Name Inc",
            accession_number="0000000-26-000100",
        )
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        ebull_test_conn.commit()

        # Run the legacy resolver. With no matching instrument, the
        # legacy row gets tombstoned ``unresolvable``.
        report = resolve_unresolved_cusips(ebull_test_conn)
        assert report.tombstoned_unresolvable == 1
        assert report.promotions == 0

        # Legacy row status is now ``unresolvable``.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s AND source IS NULL",
                (cusip,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "unresolvable"

        # Bulk row status stays NULL — UNTOUCHED.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s AND source = %s",
                (cusip, "bulk_13f_dataset"),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is None

    def test_legacy_promotion_does_not_delete_bulk_rows_for_same_cusip(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """A legacy row whose CUSIP gets promoted to
        ``external_identifiers`` is DELETEd, but bulk rows sharing
        that CUSIP MUST survive (different lifecycle owned by PR-1b
        sweep)."""
        cusip = "SHARED0001"

        # Bulk row for the CUSIP.
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0001234567",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
            cutoff=_CUTOFF,
        )
        # Legacy row for the same CUSIP.
        _record_unresolved_cusip(
            ebull_test_conn,
            cusip=cusip,
            name_of_issuer="Shared Co Inc",
            accession_number="0000000-26-000099",
        )
        # Instrument + name to allow the legacy resolver to fuzzy-match.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
                "VALUES (%s, %s, %s, 'USD', TRUE)",
                (90004, "SHRD", "Shared Co Inc"),
            )
        ebull_test_conn.commit()

        report = resolve_unresolved_cusips(ebull_test_conn)
        # The legacy row resolved (similarity 1.0).
        assert report.promotions == 1

        # Bulk row survives.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM unresolved_13f_cusips WHERE cusip = %s AND source = %s",
                (cusip, "bulk_13f_dataset"),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 1
        # Legacy row is gone.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM unresolved_13f_cusips WHERE cusip = %s AND source IS NULL",
                (cusip,),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 0
