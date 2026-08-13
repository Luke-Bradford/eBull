"""#1294 — sec_companyfacts_ingest rows_processed counts UPSERTs not INSERTs.

Pre-#1294: ``sec_companyfacts_ingest_job`` recorded only
``facts_upserted`` as the archive's ``rows_written``. On a re-run where
every (instrument, concept, period, accession) row already existed
with unchanged values, psycopg's ``ON CONFLICT DO UPDATE WHERE IS
DISTINCT FROM`` filter returns rowcount=0 for the idempotent re-upsert
— so ``facts_upserted=0`` and ``rows_written=0``.

The strict-gate cap ``fundamentals_raw_seeded`` (`_CAPABILITY_MIN_ROWS`
floor 1) then false-blocks S25 ``fundamentals_sync`` even though
``financial_facts_raw`` is fully populated.

Post-#1294: ``rows_written = facts_upserted + facts_skipped``
(=total rows seen by the upsert path). Skipped here means "ON CONFLICT
fired but value unchanged" — the row IS present, just idempotent.

Regression sentinel: a future edit that reverts the accounting back to
``facts_upserted`` alone would re-introduce the false-block bug
silently.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.bootstrap_state import StageSpec, start_run
from app.services.sec_companyfacts_ingest import CompanyFactsIngestResult
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable"),
]


def _bind_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    monkeypatch.setattr(app_settings, "database_url", test_database_url())


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status='pending', last_run_id=NULL, last_completed_at=NULL
         WHERE id=1
        """
    )
    conn.commit()


def test_rows_written_counts_upsert_total_not_just_inserts(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Idempotent re-run scenario from #1294: every fact already exists
    in financial_facts_raw with the same value, so ``facts_upserted=0``
    but ``facts_skipped=100``. The archive result MUST record 100 as
    ``rows_written`` so the strict-gate cap reads non-zero.
    """
    _reset_state(ebull_test_conn)
    _bind_settings(monkeypatch)

    # Seed an in-flight bootstrap run so ``_current_running_bootstrap_run_id``
    # returns a value — without that the job follows the standalone
    # path and never writes an archive_result row.
    specs = (
        StageSpec(
            stage_key="sec_companyfacts_ingest",
            stage_order=1,
            lane="db",
            job_name="sec_companyfacts_ingest",
        ),
    )
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    # Fake the archive file existence + bypass preconditions (we only
    # want to exercise the row-count plumbing).
    fake_archive = tmp_path / "companyfacts.zip"
    fake_archive.write_bytes(b"placeholder; not actually opened by the fake")
    monkeypatch.setattr(
        "app.services.sec_bulk_orchestrator_jobs._archive_path",
        lambda name: fake_archive,
    )

    def _noop_preconditions(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(
        "app.services.bootstrap_preconditions.assert_c2_preconditions",
        _noop_preconditions,
    )

    # Inject the re-run scenario: upserted=0, skipped=100.
    def _fake_ingest(*, conn: psycopg.Connection[tuple], archive_path) -> CompanyFactsIngestResult:
        return CompanyFactsIngestResult(
            archive_entries_seen=100,
            instruments_matched=100,
            facts_upserted=0,
            facts_skipped=100,
        )

    monkeypatch.setattr(
        "app.services.sec_bulk_orchestrator_jobs.ingest_companyfacts_archive",
        _fake_ingest,
    )

    # Avoid the post-success archive delete touching tmp_path semantics.
    monkeypatch.setattr(
        "app.services.sec_bulk_orchestrator_jobs._delete_archive_after_success",
        lambda _archive: None,
    )

    from app.services.sec_bulk_orchestrator_jobs import sec_companyfacts_ingest_job

    sec_companyfacts_ingest_job()

    # bootstrap_archive_results must record rows_written = upserted + skipped.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_written
              FROM bootstrap_archive_results
             WHERE bootstrap_run_id = %s
               AND stage_key = 'sec_companyfacts_ingest'
               AND archive_name = 'companyfacts.zip'
            """,
            (run_id,),
        )
        row = cur.fetchone()
    assert row is not None, "no archive_result row recorded — job path broken"
    assert row[0] == 100, (
        f"rows_written={row[0]} on re-run scenario; expected 100 "
        "(facts_upserted=0 + facts_skipped=100). #1294 regression — "
        "fundamentals_raw_seeded cap will false-block S25."
    )


def test_rows_written_counts_fresh_inserts_normally(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Fresh-install scenario: facts_upserted=50, facts_skipped=0
    (everything is a fresh INSERT). The new accounting must not double-
    count or change the fresh-install number.
    """
    _reset_state(ebull_test_conn)
    _bind_settings(monkeypatch)

    specs = (
        StageSpec(
            stage_key="sec_companyfacts_ingest",
            stage_order=1,
            lane="db",
            job_name="sec_companyfacts_ingest",
        ),
    )
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    fake_archive = tmp_path / "companyfacts.zip"
    fake_archive.write_bytes(b"placeholder")
    monkeypatch.setattr(
        "app.services.sec_bulk_orchestrator_jobs._archive_path",
        lambda name: fake_archive,
    )
    monkeypatch.setattr(
        "app.services.bootstrap_preconditions.assert_c2_preconditions",
        lambda *a, **k: None,
    )

    def _fake_ingest(*, conn: psycopg.Connection[tuple], archive_path) -> CompanyFactsIngestResult:
        return CompanyFactsIngestResult(
            archive_entries_seen=50,
            instruments_matched=50,
            facts_upserted=50,
            facts_skipped=0,
        )

    monkeypatch.setattr(
        "app.services.sec_bulk_orchestrator_jobs.ingest_companyfacts_archive",
        _fake_ingest,
    )
    monkeypatch.setattr(
        "app.services.sec_bulk_orchestrator_jobs._delete_archive_after_success",
        lambda _archive: None,
    )

    from app.services.sec_bulk_orchestrator_jobs import sec_companyfacts_ingest_job

    sec_companyfacts_ingest_job()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_written
              FROM bootstrap_archive_results
             WHERE bootstrap_run_id = %s
               AND stage_key = 'sec_companyfacts_ingest'
               AND archive_name = 'companyfacts.zip'
            """,
            (run_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == 50, f"rows_written={row[0]} on fresh-install; expected 50"
