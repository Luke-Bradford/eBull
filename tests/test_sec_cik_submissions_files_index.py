"""Tests for ``app.services.sec_submissions_ingest.refresh_cik_sidecar``
(Stream A PR-B T1.3, #1233) — the sidecar populate path that writes
``sec_cik_submissions_files_index`` rows per CIK.

Covers:
  * Agent-CIK filter — sidecar is "real-filer-only" index.
  * Sentinel-row pattern when files[] is empty.
  * Real-page rows when files[] has overflow entries.
  * Per-CIK DELETE + INSERT idempotency (re-running on same CIK
    leaves the sidecar in the same shape, even if new pages dropped).
  * Malformed page-entry handling — skips bad entries, still
    populates the good ones; fully-malformed list yields sentinel.
  * ``populate_origin`` discriminator: ``bootstrap`` when run_id
    present; ``steady_state`` when None.
  * Per-CIK transaction atomicity — INSERT failure rolls back the
    DELETE (prior committed sidecar state SURVIVES).
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import psycopg.rows
import pytest

from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS
from app.services.sec_submissions_ingest import (
    SubmissionsIngestResult,
    refresh_cik_sidecar,
)
from tests.fixtures.ebull_test_db import test_database_url

_TEST_CIK = "0001234567"


def _empty_result() -> SubmissionsIngestResult:
    return SubmissionsIngestResult(
        archive_entries_seen=0,
        instruments_matched=0,
        filings_upserted=0,
        profiles_upserted=0,
    )


def _read_sidecar_rows(conn: psycopg.Connection[tuple], cik: str) -> list[dict[str, object]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT cik, page_name, filing_from, filing_to, bootstrap_run_id, populate_origin "
            "FROM sec_cik_submissions_files_index "
            "WHERE cik = %s "
            "ORDER BY page_name",
            (cik,),
        )
        return list(cur.fetchall())


def _wipe_test_cik(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (_TEST_CIK,))
    conn.commit()


@pytest.fixture
def guard_conn() -> Iterator[psycopg.Connection[tuple]]:
    """Autocommit connection to the per-worker test DB — DIVERGES FROM
    PRODUCTION semantics (per PR #1308 review bot iter 2 WARNING).

    Production callers always wrap ``refresh_cik_sidecar`` in
    ``with conn.transaction()`` (sec_submissions_ingest.py:148-176) so
    DELETE + INSERT are atomic per-CIK: an INSERT failure rolls back
    the DELETE and prior committed sidecar rows for that CIK SURVIVE.

    Under THIS fixture's autocommit, the DELETE commits before the
    INSERT runs; an INSERT failure leaves the CIK with zero rows.
    The atomicity contract is therefore NOT exercised here — the
    dedicated test ``TestSidecarPerCikSavepointAtomicity`` uses the
    transactional ``ebull_test_conn`` fixture for that.

    This fixture is fine for tests that only verify the WRITER's
    output shape under successful execution (sentinel vs real-pages,
    populate_origin, idempotent re-runs) — NOT for atomicity tests.
    """
    conn = psycopg.connect(test_database_url(), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


class TestRefreshCikSidecar:
    """Pure-function contract over the sidecar writer."""

    @pytest.mark.integration
    def test_agent_cik_filter_skips_sidecar_write(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """KNOWN_FILING_AGENT_CIKS rows MUST NOT be written. Sidecar is
        a real-filer-only index per sql/172 header + §0.8 grep proof.

        Cleanup wipes the agent-CIK row explicitly (NOT _TEST_CIK) so a
        regression that writes a row leaves no flake-vector for the
        next test in this worker (Reviewer + Architect IMPORTANT — both
        lenses caught the prior cleanup mismatch).
        """
        agent_cik = next(iter(KNOWN_FILING_AGENT_CIKS))
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (agent_cik,))
        ebull_test_conn.commit()

        try:
            result = _empty_result()
            refresh_cik_sidecar(
                ebull_test_conn,
                cik=agent_cik,
                payload={
                    "filings": {
                        "files": [
                            {
                                "name": f"CIK{agent_cik}-submissions-001.json",
                                "filingFrom": "2020-01-01",
                                "filingTo": "2020-12-31",
                            }
                        ]
                    }
                },
                bootstrap_run_id=None,
                result=result,
            )
            ebull_test_conn.commit()

            rows = _read_sidecar_rows(ebull_test_conn, agent_cik)
            assert rows == [], "agent CIK must produce zero sidecar rows"
            assert result.ciks_sidecared == 0
            assert result.sidecar_pages_indexed == 0
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM sec_cik_submissions_files_index WHERE cik = %s",
                    (agent_cik,),
                )
            ebull_test_conn.commit()

    @pytest.mark.integration
    def test_writes_real_page_rows_when_files_present(
        self,
        guard_conn: psycopg.Connection[tuple],
    ) -> None:
        _wipe_test_cik(guard_conn)
        payload = {
            "filings": {
                "files": [
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-001.json",
                        "filingFrom": "2010-01-15",
                        "filingTo": "2012-06-30",
                    },
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-002.json",
                        "filingFrom": "2012-07-01",
                        "filingTo": "2015-03-31",
                    },
                ],
            },
        }
        result = _empty_result()

        refresh_cik_sidecar(
            guard_conn,
            cik=_TEST_CIK,
            payload=payload,
            bootstrap_run_id=None,
            result=result,
        )

        rows = _read_sidecar_rows(guard_conn, _TEST_CIK)
        assert len(rows) == 2
        assert {r["page_name"] for r in rows} == {
            f"CIK{_TEST_CIK}-submissions-001.json",
            f"CIK{_TEST_CIK}-submissions-002.json",
        }
        assert all(r["filing_from"] is not None and r["filing_to"] is not None for r in rows)
        assert all(r["populate_origin"] == "steady_state" for r in rows)  # run_id was None
        assert all(r["bootstrap_run_id"] is None for r in rows)
        assert result.ciks_sidecared == 1
        assert result.sidecar_pages_indexed == 2

    @pytest.mark.integration
    def test_writes_sentinel_when_files_empty(
        self,
        guard_conn: psycopg.Connection[tuple],
    ) -> None:
        _wipe_test_cik(guard_conn)
        payload: dict[str, object] = {"filings": {"recent": {}, "files": []}}
        result = _empty_result()

        refresh_cik_sidecar(
            guard_conn,
            cik=_TEST_CIK,
            payload=payload,
            bootstrap_run_id=None,
            result=result,
        )

        rows = _read_sidecar_rows(guard_conn, _TEST_CIK)
        assert len(rows) == 1
        assert rows[0]["page_name"] == "__no_overflow_pages__"
        assert rows[0]["filing_from"] is None
        assert rows[0]["filing_to"] is None
        assert rows[0]["populate_origin"] == "steady_state"
        assert result.ciks_sidecared == 1
        # Sentinel rows do NOT count toward sidecar_pages_indexed.
        assert result.sidecar_pages_indexed == 0

    @pytest.mark.integration
    def test_writes_sentinel_when_filings_block_missing(
        self,
        guard_conn: psycopg.Connection[tuple],
    ) -> None:
        """Defensive: payload with no ``filings`` key (rare but possible
        for placeholder responses) still produces a sentinel — the CIK
        was processed even though there was nothing to index."""
        _wipe_test_cik(guard_conn)
        result = _empty_result()

        refresh_cik_sidecar(
            guard_conn,
            cik=_TEST_CIK,
            payload={},
            bootstrap_run_id=None,
            result=result,
        )

        rows = _read_sidecar_rows(guard_conn, _TEST_CIK)
        assert len(rows) == 1
        assert rows[0]["page_name"] == "__no_overflow_pages__"

    @pytest.mark.integration
    def test_per_cik_delete_then_insert_rebuilds_idempotently(
        self,
        guard_conn: psycopg.Connection[tuple],
    ) -> None:
        """Second call with different files[] entries replaces the
        first call's rows wholesale (not appends)."""
        _wipe_test_cik(guard_conn)
        first_payload = {
            "filings": {
                "files": [
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-001.json",
                        "filingFrom": "2010-01-15",
                        "filingTo": "2012-06-30",
                    },
                ],
            },
        }
        second_payload = {
            "filings": {
                "files": [
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-002.json",
                        "filingFrom": "2012-07-01",
                        "filingTo": "2015-03-31",
                    },
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-003.json",
                        "filingFrom": "2015-04-01",
                        "filingTo": "2018-12-31",
                    },
                ],
            },
        }
        result = _empty_result()
        refresh_cik_sidecar(guard_conn, cik=_TEST_CIK, payload=first_payload, bootstrap_run_id=None, result=result)
        refresh_cik_sidecar(guard_conn, cik=_TEST_CIK, payload=second_payload, bootstrap_run_id=None, result=result)

        rows = _read_sidecar_rows(guard_conn, _TEST_CIK)
        assert {r["page_name"] for r in rows} == {
            f"CIK{_TEST_CIK}-submissions-002.json",
            f"CIK{_TEST_CIK}-submissions-003.json",
        }
        assert f"CIK{_TEST_CIK}-submissions-001.json" not in {r["page_name"] for r in rows}

    @pytest.mark.integration
    def test_malformed_page_entries_skipped_well_formed_kept(
        self,
        guard_conn: psycopg.Connection[tuple],
    ) -> None:
        _wipe_test_cik(guard_conn)
        payload = {
            "filings": {
                "files": [
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-001.json",
                        "filingFrom": "2010-01-15",
                        "filingTo": "2012-06-30",
                    },
                    {"name": None},  # malformed
                    "not a dict",  # malformed
                    {"name": f"CIK{_TEST_CIK}-submissions-002.json"},  # missing dates
                ],
            },
        }
        result = _empty_result()

        refresh_cik_sidecar(
            guard_conn,
            cik=_TEST_CIK,
            payload=payload,
            bootstrap_run_id=None,
            result=result,
        )

        rows = _read_sidecar_rows(guard_conn, _TEST_CIK)
        assert {r["page_name"] for r in rows} == {f"CIK{_TEST_CIK}-submissions-001.json"}
        assert result.sidecar_pages_indexed == 1

    @pytest.mark.integration
    def test_populate_origin_bootstrap_when_run_id_present(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """`populate_origin='bootstrap'` when bootstrap_run_id is not
        None; FK to bootstrap_runs(id) must be valid. We seed a real
        bootstrap_runs row so the FK insert succeeds."""
        _wipe_test_cik(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            # Seed a bootstrap_runs row with explicit status='running' so the helper
            # has a real FK target. Default columns are nullable.
            cur.execute(
                "INSERT INTO bootstrap_runs (status, triggered_by_operator_id) VALUES ('running', NULL) RETURNING id",
            )
            row = cur.fetchone()
            assert row is not None
            run_id = int(row[0])
        ebull_test_conn.commit()

        try:
            result = _empty_result()
            refresh_cik_sidecar(
                ebull_test_conn,
                cik=_TEST_CIK,
                payload={"filings": {"files": []}},
                bootstrap_run_id=run_id,
                result=result,
            )
            ebull_test_conn.commit()

            rows = _read_sidecar_rows(ebull_test_conn, _TEST_CIK)
            assert len(rows) == 1
            assert rows[0]["populate_origin"] == "bootstrap"
            assert rows[0]["bootstrap_run_id"] == run_id
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (_TEST_CIK,))
                cur.execute("DELETE FROM bootstrap_runs WHERE id = %s", (run_id,))
            ebull_test_conn.commit()


class TestSidecarPerCikSavepointAtomicity:
    """Per spec §14 + DE BLOCKING: on per-CIK transaction rollback
    (e.g. an INSERT failure inside the outer ``with conn.transaction()``
    block), the DELETE rolls back too — prior committed sidecar rows
    for that CIK SURVIVE.

    These tests exercise the contract end-to-end by simulating the
    OUTER transaction wrapper around refresh_cik_sidecar."""

    @pytest.mark.integration
    def test_prior_rows_survive_when_sibling_write_raises_inside_outer_tx(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        # Seed a "prior committed" sidecar state for the test CIK.
        _wipe_test_cik(ebull_test_conn)
        prior_payload = {
            "filings": {
                "files": [
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-001.json",
                        "filingFrom": "2010-01-15",
                        "filingTo": "2012-06-30",
                    },
                ],
            },
        }
        refresh_cik_sidecar(
            ebull_test_conn,
            cik=_TEST_CIK,
            payload=prior_payload,
            bootstrap_run_id=None,
            result=_empty_result(),
        )
        ebull_test_conn.commit()
        assert len(_read_sidecar_rows(ebull_test_conn, _TEST_CIK)) == 1

        # Now simulate the production shape: an OUTER per-CIK
        # ``with conn.transaction()`` that calls refresh_cik_sidecar
        # AND then raises in a sibling code path. The savepoint rollback
        # MUST restore the prior committed sidecar row.
        new_payload = {
            "filings": {
                "files": [
                    {
                        "name": f"CIK{_TEST_CIK}-submissions-999.json",
                        "filingFrom": "2020-01-01",
                        "filingTo": "2020-12-31",
                    },
                ],
            },
        }
        try:
            with ebull_test_conn.transaction():
                refresh_cik_sidecar(
                    ebull_test_conn,
                    cik=_TEST_CIK,
                    payload=new_payload,
                    bootstrap_run_id=None,
                    result=_empty_result(),
                )
                # Simulate a sibling-instrument write failure.
                raise RuntimeError("simulated sibling write failure")
        except RuntimeError:
            pass

        rows = _read_sidecar_rows(ebull_test_conn, _TEST_CIK)
        assert len(rows) == 1
        assert rows[0]["page_name"] == f"CIK{_TEST_CIK}-submissions-001.json", (
            "rollback must restore the PRIOR committed row, not the in-flight new row"
        )

        # Cleanup.
        _wipe_test_cik(ebull_test_conn)
