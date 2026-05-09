"""Endpoint contract tests for ``/system/processes`` (#1071, PR3).

DB-backed: the trigger / cancel endpoints insert into
``pending_job_requests`` + read ``bootstrap_state`` + ``job_runs``, so
mocking the cursor would lose the partial-unique fence guarantee.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from app.db import get_conn
from app.main import app
from app.workers.scheduler import JOB_RETRY_DEFERRED

client = TestClient(app)


@pytest.fixture
def conn_override(
    ebull_test_conn: psycopg.Connection[tuple],
) -> Iterator[None]:
    """Wire the FastAPI ``get_conn`` dependency to the test DB connection.

    Codex round 7 fix: the trigger handler now writes its
    ``pending_job_requests`` row inside the request's tx (atomic with
    fence-check under the per-process advisory lock). That tx uses
    ``conn`` from the FastAPI dep — overriding ``get_conn`` to yield
    the test conn means INSERTs land in the test DB and precondition
    re-reads see them; no separate ``publish_manual_job_request``
    monkeypatch needed.

    Always reset the override on teardown so the next test's fixture
    (or the smoke test) starts clean. The auth no-op override is
    preserved by the conftest autouse fixture.
    """

    def _yield_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _yield_conn
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _ensure_kill_switch_off(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO UPDATE
        SET is_active = FALSE, activated_at = NULL, activated_by = NULL, reason = NULL
        """
    )


def _seed_bootstrap_state(conn: psycopg.Connection[tuple], status: str) -> None:
    conn.execute("UPDATE bootstrap_state SET status = %s WHERE id = 1", (status,))


def test_list_processes_returns_envelope_shape(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.get("/system/processes")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert "rows" in payload and "partial" in payload
    assert payload["partial"] is False
    assert isinstance(payload["rows"], list)
    process_ids = {r["process_id"] for r in payload["rows"]}
    assert "bootstrap" in process_ids
    assert JOB_RETRY_DEFERRED in process_ids


def test_get_process_unknown_returns_404(conn_override: None) -> None:
    resp = client.get("/system/processes/not_a_real_thing")
    assert resp.status_code == 404


def test_trigger_bootstrap_iterate_from_pending_returns_409(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Iterate = retry-failed; from 'pending' there is nothing to resume."""
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "iterate"})
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "bootstrap_not_resumable"


def test_trigger_bootstrap_full_wash_inserts_fence_row(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "full_wash"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mode"] == "full_wash"
    assert isinstance(body["request_id"], int)

    # Fence row exists with mode='full_wash' + process_id='bootstrap'.
    row = ebull_test_conn.execute(
        """
        SELECT process_id, mode, status FROM pending_job_requests
        WHERE request_id = %s
        """,
        (body["request_id"],),
    ).fetchone()
    assert row is not None
    assert row[0] == "bootstrap"
    assert row[1] == "full_wash"
    assert row[2] == "pending"


def test_trigger_kill_switch_active_returns_409(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, TRUE, now(), 'test', 'paused')
        ON CONFLICT (id) DO UPDATE
        SET is_active = TRUE, activated_at = now(), activated_by = 'test', reason = 'paused'
        """
    )
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "full_wash"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "kill_switch_active"


def test_trigger_scheduled_iterate_dedup_409(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Two iterate triggers in a row → second one 409s on the
    iterate_already_pending precondition."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    first = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert first.status_code == 200, first.text

    second = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert second.status_code == 409
    assert second.json()["detail"]["reason"] == "iterate_already_pending"


def test_trigger_scheduled_full_wash_blocks_subsequent_iterate(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    first = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "full_wash"},
    )
    assert first.status_code == 200, first.text

    second = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert second.status_code == 409
    # Fence check runs FIRST in `_check_scheduled_job_preconditions`
    # (PR #1072 review WARNING fix), so the iterate POST during an
    # active full-wash always reports the spec-aligned fence reason —
    # never `iterate_already_pending`. Pin the exact reason so a
    # future precondition reorder shows up as a test diff.
    assert second.json()["detail"]["reason"] == "full_wash_already_pending"


def test_trigger_invalid_mode_returns_422(conn_override: None) -> None:
    resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "NUKE"},
    )
    assert resp.status_code == 422


def test_cancel_no_active_run_returns_409(conn_override: None, ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/cancel",
        json={"mode": "cooperative"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "no_active_run"


def test_cancel_invalid_mode_returns_422(conn_override: None) -> None:
    resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/cancel",
        json={"mode": "halt"},
    )
    assert resp.status_code == 422


def test_full_wash_resets_bootstrap_stages_before_enqueue(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """PR4 §Full-wash semantics step 5 — bootstrap full-wash flips
    every non-pending stage on the latest run back to ``pending`` AND
    inserts the durable fence row, all inside the same advisory-lock-
    held transaction.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    run_row = ebull_test_conn.execute(
        """
        INSERT INTO bootstrap_runs (status, completed_at)
        VALUES ('partial_error', now())
        RETURNING id
        """
    ).fetchone()
    assert run_row is not None
    run_id = int(run_row[0])
    ebull_test_conn.execute(
        """
        INSERT INTO bootstrap_stages
            (bootstrap_run_id, stage_key, stage_order, lane, job_name,
             status, started_at, completed_at, last_error)
        VALUES (%s, 'init', 0, 'init', 'job_x', 'success', now(), now(), NULL),
               (%s, 'sec_form4', 5, 'sec', 'job_x', 'error', now(), now(),
                'EDGAR 503')
        """,
        (run_id, run_id),
    )
    _seed_bootstrap_state(ebull_test_conn, "partial_error")
    ebull_test_conn.execute(
        "UPDATE bootstrap_state SET last_run_id = %s WHERE id = 1",
        (run_id,),
    )
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "full_wash"})
    assert resp.status_code == 200, resp.text

    statuses = {
        row[0]: row[1]
        for row in ebull_test_conn.execute(
            "SELECT stage_key, status FROM bootstrap_stages WHERE bootstrap_run_id = %s",
            (run_id,),
        ).fetchall()
    }
    assert statuses == {"init": "pending", "sec_form4": "pending"}
    last_error = ebull_test_conn.execute(
        "SELECT last_error FROM bootstrap_stages WHERE stage_key = 'sec_form4'"
    ).fetchone()
    assert last_error is not None
    assert last_error[0] is None


def test_bootstrap_full_wash_blocked_while_running(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Review bot BLOCKING rebuttal: bootstrap's active-run gate is
    ``bootstrap_state.status='running'`` (not ``_has_active_job_run``).
    The check happens before ``_apply_full_wash_reset`` so the running
    orchestrator's bootstrap_stages cannot be reset under it. Pin the
    behaviour with an explicit test so the symmetry with scheduled-job
    full-wash protection is auditable.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO bootstrap_runs (status, completed_at)
        VALUES ('running', NULL)
        """
    )
    _seed_bootstrap_state(ebull_test_conn, "running")
    ebull_test_conn.commit()

    resp = client.post("/system/processes/bootstrap/trigger", json={"mode": "full_wash"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "bootstrap_already_running"


def test_trigger_active_scheduled_run_returns_409(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """PR4 Codex BLOCKING: a scheduled trigger landing while a worker
    is mid-run must 409 with ``active_run_in_progress`` so full-wash
    cannot reset watermarks under the running worker's feet AND a
    second iterate cannot double-enqueue."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, status)
        VALUES (%s, now(), 'running')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()

    iterate_resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "iterate"},
    )
    assert iterate_resp.status_code == 409
    assert iterate_resp.json()["detail"]["reason"] == "active_run_in_progress"

    full_wash_resp = client.post(
        f"/system/processes/{JOB_RETRY_DEFERRED}/trigger",
        json={"mode": "full_wash"},
    )
    assert full_wash_resp.status_code == 409
    assert full_wash_resp.json()["detail"]["reason"] == "active_run_in_progress"


def test_full_wash_clears_freshness_filing_id_and_makes_rows_immediately_due(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """PR4 Codex BLOCKING: clearing only ``last_known_filed_at`` is
    not a real epoch reset. The full-wash must also clear
    ``last_known_filing_id``, ``expected_next_at``, and
    ``next_recheck_at`` so the post-reset rows qualify for
    ``idx_freshness_due_for_poll`` immediately AND the next poll
    cannot skip historical filings against a stale filing_id pointer.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9200099, 'TST_FW2', 'TST_FW2 Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, last_known_filed_at,
             last_known_filing_id, expected_next_at, next_recheck_at,
             state, instrument_id)
        VALUES ('issuer', '9200099', 'sec_form3',
                '2026-05-08T12:00:00Z',
                '0000320193-26-000042',
                '2026-06-01T00:00:00Z',
                '2026-06-15T00:00:00Z',
                'never_filed', 9200099)
        """
    )
    ebull_test_conn.commit()

    resp = client.post(
        "/system/processes/sec_form3_ingest/trigger",
        json={"mode": "full_wash"},
    )
    assert resp.status_code == 200, resp.text

    row = ebull_test_conn.execute(
        """
        SELECT last_known_filed_at, last_known_filing_id, expected_next_at,
               next_recheck_at, state
          FROM data_freshness_index
         WHERE source = 'sec_form3'
           AND subject_id = '9200099'
        """
    ).fetchone()
    assert row is not None
    assert row[0] is None  # last_known_filed_at cleared
    assert row[1] is None  # last_known_filing_id cleared
    assert row[2] is None  # expected_next_at cleared
    assert row[3] is None  # next_recheck_at cleared
    assert row[4] == "unknown"


def test_mixed_covered_and_uncovered_failed_rows_stays_failed(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """PR4 Codex BLOCKING: covered-check must prove EVERY failed row
    has coverage. One uncovered row keeps the status at ``failed`` so
    operator-visible errors are not auto-hidden by a single due retry
    when other rows have no retry within the window.
    """
    from app.services.processes import scheduled_adapter

    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, finished_at, status,
                              error_classes, rows_errored)
        VALUES ('sec_form3_ingest', now() - interval '5 minutes', now(),
                'failure', %s, 2)
        """,
        (
            Jsonb(
                {
                    "RateLimited": {
                        "count": 2,
                        "sample_message": "429",
                        "last_subject": "AAPL",
                        "last_seen_at": "2026-05-09T11:00:00+00:00",
                    }
                }
            ),
        ),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9200201, 'TST_MIX1', 'TST_MIX1 Co', 'TEST', 'USD', TRUE),
               (9200202, 'TST_MIX2', 'TST_MIX2 Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, state, next_recheck_at,
             instrument_id)
        VALUES
            -- Covered: retry due in 5 minutes (well within next fire)
            ('issuer', '9200201', 'sec_form3', 'error',
             now() + interval '5 minutes', 9200201),
            -- Uncovered: NULL next_recheck_at means no scheduled retry
            ('issuer', '9200202', 'sec_form3', 'error', NULL, 9200202)
        """
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id="sec_form3_ingest")
    assert row is not None
    # Mixed coverage → status stays failed, errors visible.
    assert row.status == "failed"
    assert len(row.last_n_errors) == 1


def test_multi_source_covered_check_requires_all_sources_covered(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """Codex pre-push round 2 BLOCKING: a job with BOTH freshness +
    manifest sources (e.g. ``sec_filing_documents_ingest``) must keep
    ``status='failed'`` when ANY applicable source has uncovered
    failures. Coverage on one source does not mask uncovered failures
    on the other.
    """
    from app.services.processes import scheduled_adapter

    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, finished_at, status,
                              error_classes, rows_errored)
        VALUES ('sec_filing_documents_ingest', now() - interval '5 minutes',
                now(), 'failure', %s, 1)
        """,
        (
            Jsonb(
                {
                    "ParseError": {
                        "count": 1,
                        "sample_message": "malformed XML",
                        "last_subject": None,
                        "last_seen_at": "2026-05-09T11:00:00+00:00",
                    }
                }
            ),
        ),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9200301, 'TST_MULTI', 'TST_MULTI Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    # Freshness side: ALL covered (one error row with retry due soon).
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, state, next_recheck_at,
             instrument_id)
        VALUES ('issuer', '9200301', 'sec_form4', 'error',
                now() + interval '5 minutes', 9200301)
        """
    )
    # Manifest side: one UNCOVERED failed row (next_retry_at = NULL).
    ebull_test_conn.execute(
        """
        INSERT INTO sec_filing_manifest
            (accession_number, cik, form, source, subject_type, subject_id,
             instrument_id, filed_at, ingest_status, next_retry_at)
        VALUES ('0000000099-26-000001', '0000123', '4', 'sec_form4',
                'issuer', '9200301', 9200301, now() - interval '1 day',
                'failed', NULL)
        """
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(
        ebull_test_conn,
        process_id="sec_filing_documents_ingest",
    )
    assert row is not None
    assert row.status == "failed"
    assert len(row.last_n_errors) == 1


def test_full_wash_resets_freshness_index_for_sec_ingest(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """PR4 §Full-wash semantics step 5 — SEC ingest jobs reset
    ``data_freshness_index`` for the source: ``last_known_filed_at``
    flips to NULL and ``state`` flips to ``unknown``."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9200001, 'TST_FW', 'TST_FW Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, last_known_filed_at, state,
             instrument_id)
        VALUES ('issuer', '9200001', 'sec_form3', '2026-05-08T12:00:00Z',
                'current', 9200001)
        """
    )
    ebull_test_conn.commit()

    resp = client.post(
        "/system/processes/sec_form3_ingest/trigger",
        json={"mode": "full_wash"},
    )
    assert resp.status_code == 200, resp.text

    row = ebull_test_conn.execute(
        """
        SELECT last_known_filed_at, state
          FROM data_freshness_index
         WHERE source = 'sec_form3'
           AND subject_id = '9200001'
        """
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] == "unknown"


def test_iterate_does_not_reset_freshness_index(
    conn_override: None, ebull_test_conn: psycopg.Connection[tuple]
) -> None:
    """PR4 §Iterate semantics — Iterate never resets the watermark.

    Idempotency is at the ingest layer (ON CONFLICT). Confirm the
    handler does NOT mutate ``data_freshness_index`` on iterate.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9200002, 'TST_ITR', 'TST_ITR Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    original_filed_at = "2026-05-08T12:00:00+00:00"
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, last_known_filed_at, state,
             instrument_id)
        VALUES ('issuer', '9200002', 'sec_form3', %s, 'current', 9200002)
        """,
        (original_filed_at,),
    )
    ebull_test_conn.commit()

    resp = client.post(
        "/system/processes/sec_form3_ingest/trigger",
        json={"mode": "iterate"},
    )
    assert resp.status_code == 200, resp.text

    row = ebull_test_conn.execute(
        """
        SELECT last_known_filed_at, state
          FROM data_freshness_index
         WHERE source = 'sec_form3'
           AND subject_id = '9200002'
        """
    ).fetchone()
    assert row is not None
    assert row[0] is not None  # untouched
    assert row[1] == "current"


def test_partial_flag_when_adapter_throws(
    conn_override: None,
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec §Failure-mode invariants: an adapter raising must omit its
    rows, NOT 500 the page. The envelope flips ``partial=true``."""
    _ensure_kill_switch_off(ebull_test_conn)
    _seed_bootstrap_state(ebull_test_conn, "pending")
    ebull_test_conn.commit()

    def _explode(_conn: object) -> list[object]:
        raise RuntimeError("adapter exploded")

    from app.services.processes import scheduled_adapter

    monkeypatch.setattr(scheduled_adapter, "list_rows", _explode)

    resp = client.get("/system/processes")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["partial"] is True
    process_ids = {r["process_id"] for r in payload["rows"]}
    # bootstrap survived; scheduled_jobs are absent.
    assert "bootstrap" in process_ids
    assert JOB_RETRY_DEFERRED not in process_ids
