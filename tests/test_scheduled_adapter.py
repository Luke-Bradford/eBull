"""Scheduled adapter round-trip tests (#1071, umbrella #1064 PR3).

DB-backed against the worker ``ebull_test`` template.
"""

from __future__ import annotations

import psycopg
from psycopg.types.json import Jsonb

from app.services.processes import scheduled_adapter
from app.workers.scheduler import JOB_FUNDAMENTALS_SYNC, JOB_RETRY_DEFERRED


def _ensure_kill_switch_off(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO UPDATE
        SET is_active = FALSE, activated_at = NULL, activated_by = NULL, reason = NULL
        """
    )


def _make_run(
    conn: psycopg.Connection[tuple],
    *,
    job_name: str,
    status: str = "running",
    error_classes: dict[str, dict[str, object]] | None = None,
    rows_skipped_by_reason: dict[str, int] | None = None,
    rows_errored: int = 0,
    finished: bool = False,
    cancel_requested: bool = False,
    processed_count: int = 0,
    target_count: int | None = None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, finished_at, status, row_count,
                error_classes, rows_skipped_by_reason, rows_errored,
                cancel_requested_at, processed_count, target_count)
        VALUES (%s, now() - interval '5 minutes',
                CASE WHEN %s THEN now() ELSE NULL END,
                %s, NULL, %s, %s, %s,
                CASE WHEN %s THEN now() ELSE NULL END,
                %s, %s)
        RETURNING run_id
        """,
        (
            job_name,
            finished,
            status,
            Jsonb(error_classes or {}),
            Jsonb(rows_skipped_by_reason or {}),
            rows_errored,
            cancel_requested,
            processed_count,
            target_count,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_no_history_yields_pending_first_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.process_id == JOB_RETRY_DEFERRED
    assert row.mechanism == "scheduled_job"
    assert row.status == "pending_first_run"
    assert row.last_run is None
    assert row.active_run is None
    # Cron string + next_fire_at always populated for a registered job.
    assert row.cadence_cron is not None
    assert row.next_fire_at is not None


def test_latest_success_yields_ok(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "ok"
    assert row.last_run is not None
    assert row.last_run.status == "success"
    assert row.can_iterate is True
    assert row.last_n_errors == ()


def test_latest_failure_no_retry_in_flight_shows_failed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        status="failure",
        finished=True,
        rows_errored=2,
        error_classes={
            "ConnectionTimeout": {
                "count": 2,
                "sample_message": "timed out",
                "last_subject": "ECB",
                "last_seen_at": "2026-05-09T11:00:00+00:00",
            }
        },
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "failed"
    assert len(row.last_n_errors) == 1
    assert row.last_n_errors[0].error_class == "ConnectionTimeout"
    assert row.last_n_errors[0].count == 2


def test_failure_with_retry_in_flight_auto_hides_errors(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """spec §Auto-hide-on-retry rule: a retry currently in flight covers
    the failed scope, so the row renders as `running` with empty errors."""
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        status="failure",
        finished=True,
        rows_errored=5,
        error_classes={
            "RateLimited": {
                "count": 5,
                "sample_message": "429",
                "last_subject": None,
                "last_seen_at": "2026-05-09T11:00:00+00:00",
            }
        },
    )
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', %s, %s, 'iterate', 'pending')
        """,
        (JOB_RETRY_DEFERRED, JOB_RETRY_DEFERRED),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "running"
    assert row.last_n_errors == ()


def test_active_run_with_progress(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(
        ebull_test_conn,
        job_name=JOB_FUNDAMENTALS_SYNC,
        status="running",
        processed_count=312,
        target_count=1547,
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_FUNDAMENTALS_SYNC)
    assert row is not None
    assert row.status == "running"
    assert row.active_run is not None
    assert row.active_run.rows_processed_so_far == 312
    assert row.active_run.progress_units_done == 312
    assert row.active_run.progress_units_total == 1547


def test_kill_switch_active_disables_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, TRUE, now(), 'test', 'pause everything')
        ON CONFLICT (id) DO UPDATE
        SET is_active = TRUE, activated_at = now(), activated_by = 'test',
            reason = 'pause everything'
        """
    )
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "disabled"
    assert row.can_iterate is False
    assert row.can_full_wash is False


def test_full_wash_fence_disables_iterate_and_full_wash(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', %s, %s, 'full_wash', 'dispatched')
        """,
        (JOB_RETRY_DEFERRED, JOB_RETRY_DEFERRED),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.can_iterate is False
    assert row.can_full_wash is False


def test_list_rows_returns_one_per_scheduled_job(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    from app.workers.scheduler import SCHEDULED_JOBS

    rows = scheduled_adapter.list_rows(ebull_test_conn)
    assert len(rows) == len(SCHEDULED_JOBS)
    process_ids = {r.process_id for r in rows}
    assert {j.name for j in SCHEDULED_JOBS} == process_ids


def test_unknown_process_id_returns_none(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    row = scheduled_adapter.get_row(ebull_test_conn, process_id="not_a_real_job")
    assert row is None


def test_list_runs_returns_terminal_history(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="failure", finished=True)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="running")  # excluded
    ebull_test_conn.commit()

    runs = scheduled_adapter.list_runs(ebull_test_conn, process_id=JOB_RETRY_DEFERRED, days=7)
    assert len(runs) == 2
    statuses = {r.status for r in runs}
    assert statuses == {"success", "failure"}


def test_watermark_surfaces_filed_at_for_sec_ingest_job(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR4: SEC freshness-driven jobs surface a ``filed_at`` watermark
    on the ProcessRow."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9100001, 'TST_F3', 'TST_F3 Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, last_known_filed_at, state,
             instrument_id)
        VALUES ('issuer', '9100001', 'sec_form3', '2026-05-08T12:00:00Z',
                'current', 9100001)
        """
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id="sec_form3_ingest")
    assert row is not None
    assert row.watermark is not None
    assert row.watermark.cursor_kind == "filed_at"
    assert row.watermark.cursor_value.startswith("2026-05-08")


def test_pending_retry_status_when_freshness_recheck_covers_failed_scope(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR4: spec §"Auto-hide-on-retry rule" / "Covered" check.

    Latest terminal run is failure, no inflight Iterate, but
    data_freshness_index has an error-state subject with
    ``next_recheck_at`` <= next_fire_at — the next scheduled fire will
    reattempt the failed scope. Status flips from ``failed`` to
    ``pending_retry`` with empty errors.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(
        ebull_test_conn,
        job_name="sec_form3_ingest",
        status="failure",
        finished=True,
        rows_errored=1,
        error_classes={
            "RateLimited": {
                "count": 1,
                "sample_message": "429",
                "last_subject": "AAPL",
                "last_seen_at": "2026-05-09T11:00:00+00:00",
            }
        },
    )
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9100002, 'TST_RC', 'TST_RC Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, state, next_recheck_at,
             instrument_id)
        VALUES ('issuer', '9100002', 'sec_form3', 'error',
                now() + interval '5 minutes', 9100002)
        """
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id="sec_form3_ingest")
    assert row is not None
    assert row.status == "pending_retry"
    assert row.last_n_errors == ()


def test_failed_status_when_no_freshness_recheck_covers_failed_scope(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The covered-check is conservative — without an error-state
    freshness row inside the next-fire window, the row stays ``failed``."""
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(
        ebull_test_conn,
        job_name="sec_form3_ingest",
        status="failure",
        finished=True,
        rows_errored=1,
        error_classes={
            "RateLimited": {
                "count": 1,
                "sample_message": "429",
                "last_subject": "AAPL",
                "last_seen_at": "2026-05-09T11:00:00+00:00",
            }
        },
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id="sec_form3_ingest")
    assert row is not None
    assert row.status == "failed"
    assert len(row.last_n_errors) == 1


def test_list_run_errors_decodes_jsonb(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    run_id = _make_run(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        status="failure",
        finished=True,
        error_classes={
            "X": {
                "count": 3,
                "sample_message": "boom",
                "last_subject": "subj",
                "last_seen_at": "2026-05-09T11:00:00+00:00",
            }
        },
    )
    ebull_test_conn.commit()

    errors = scheduled_adapter.list_run_errors(ebull_test_conn, process_id=JOB_RETRY_DEFERRED, run_id=run_id)
    assert len(errors) == 1
    assert errors[0].error_class == "X"
    assert errors[0].count == 3
    assert errors[0].sample_subject == "subj"


# ---------------------------------------------------------------------------
# PR8 (#1083) — four-case stale model, scheduled adapter integration.
# ---------------------------------------------------------------------------


def test_stale_reasons_default_empty_on_healthy_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.stale_reasons == ()


def test_watermark_gap_surfaces_when_freshness_overdue(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """SEC freshness-driven scheduled job with an
    ``expected_next_at`` past the gap tolerance → ``watermark_gap``."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (9100009, 'TST_WG', 'TST_WG Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, state, expected_next_at,
             instrument_id)
        VALUES ('issuer', '9100009', 'sec_form3', 'current',
                now() - interval '10 minutes', 9100009)
        """
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id="sec_form3_ingest")
    assert row is not None
    assert "watermark_gap" in row.stale_reasons


def test_queue_stuck_surfaces_when_dispatched_row_aged(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``status='dispatched'`` row whose ``claimed_at`` is older than
    the 30-min queue_stuck threshold → ``queue_stuck``."""
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status,
             claimed_at, claimed_by)
        VALUES ('manual_job', %s, %s, 'iterate', 'dispatched',
                now() - interval '45 minutes', 'test-boot-id')
        """,
        (JOB_RETRY_DEFERRED, JOB_RETRY_DEFERRED),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert "queue_stuck" in row.stale_reasons


def test_mid_flight_stuck_fires_on_aged_heartbeat(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Running job whose ``last_progress_at`` is older than the
    per-process threshold → ``mid_flight_stuck``."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, status, processed_count, last_progress_at)
        VALUES (%s, now() - interval '20 minutes', 'running', 5,
                now() - interval '10 minutes')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "running"
    assert "mid_flight_stuck" in row.stale_reasons
    assert row.active_run is not None
    assert row.active_run.last_progress_at is not None


def test_mid_flight_stuck_does_not_fire_on_first_tick_lag(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A run that just started (10s ago, no first tick) is NOT stale —
    first-tick lag is benign on unbounded jobs."""
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, status)
        VALUES (%s, now() - interval '10 seconds', 'running')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert "mid_flight_stuck" not in row.stale_reasons


def test_schedule_missed_when_terminal_run_predates_cadence_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``retry_deferred_recommendations`` runs every 5 minutes. A
    successful terminal run from 2 hours ago means we should have
    fired ~24 more times since — schedule_missed must surface.

    Codex pre-push BLOCKING: the rule must compare against the
    cadence-occurrence after the latest run, not the strictly-future
    ``next_fire_at`` (which compute_next_run anchors to ``now`` and so
    can never be in the past).
    """
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, finished_at, status)
        VALUES (%s, now() - interval '2 hours',
                now() - interval '2 hours' + interval '30 seconds',
                'success')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert "schedule_missed" in row.stale_reasons


def test_schedule_missed_does_not_fire_when_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An active run that's been going for 2 hours has not "missed" its
    fire — the cadence is suppressed by the in-flight run."""
    _ensure_kill_switch_off(ebull_test_conn)
    # Seed a 2h-old terminal AND a currently-running row.
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, finished_at, status)
        VALUES (%s, now() - interval '3 hours',
                now() - interval '3 hours' + interval '30 seconds',
                'success')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, status, last_progress_at)
        VALUES (%s, now() - interval '2 hours', 'running',
                now() - interval '30 seconds')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "running"
    assert "schedule_missed" not in row.stale_reasons


def test_queue_stuck_with_null_claimed_at_falls_back_to_requested_at(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A buggy ``status='dispatched'`` row with NULL claimed_at must
    still surface queue_stuck — fallback to ``requested_at`` keeps the
    rule honest under dispatcher misbehaviour."""
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status,
             requested_at, claimed_at, claimed_by)
        VALUES ('manual_job', %s, %s, 'iterate', 'dispatched',
                now() - interval '60 minutes', NULL, NULL)
        """,
        (JOB_RETRY_DEFERRED, JOB_RETRY_DEFERRED),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert "queue_stuck" in row.stale_reasons


def test_queue_stuck_does_not_fire_on_recently_claimed_dispatched(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A dispatched row whose ``claimed_at`` is only 5 minutes old is
    well within the 30-min queue_stuck threshold — not stale."""
    _ensure_kill_switch_off(ebull_test_conn)
    _make_run(ebull_test_conn, job_name=JOB_RETRY_DEFERRED, status="success", finished=True)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status,
             claimed_at, claimed_by)
        VALUES ('manual_job', %s, %s, 'iterate', 'dispatched',
                now() - interval '5 minutes', 'test-boot-id')
        """,
        (JOB_RETRY_DEFERRED, JOB_RETRY_DEFERRED),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert "queue_stuck" not in row.stale_reasons


# ---------------------------------------------------------------------------
# PR2 #1064 — params_metadata surfacing for the Advanced disclosure tab
# ---------------------------------------------------------------------------


def test_params_metadata_surfaces_for_sec_13f_quarterly_sweep(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """ScheduledJob.params_metadata flows verbatim onto ProcessRow.

    Pins the foundation that the FE Advanced tab depends on: a job that
    declares ``params_metadata`` surfaces its full tuple on the row,
    enabling the drill-in to render one form field per entry. Drift
    here silently breaks the renderer.
    """
    from app.workers.scheduler import JOB_SEC_13F_QUARTERLY_SWEEP, SCHEDULED_JOBS

    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_SEC_13F_QUARTERLY_SWEEP)
    assert row is not None
    job = next(j for j in SCHEDULED_JOBS if j.name == JOB_SEC_13F_QUARTERLY_SWEEP)
    assert row.params_metadata == job.params_metadata
    assert len(row.params_metadata) == 1
    assert row.params_metadata[0].name == "min_period_of_report"
    assert row.params_metadata[0].field_type == "date"


def test_params_metadata_default_empty_for_jobs_without_declarations(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Jobs that do not declare ``params_metadata`` surface ``()``.

    Today every scheduled job except ``sec_13f_quarterly_sweep`` falls
    in this bucket. The empty tuple is what the FE keys off to hide
    the Advanced tab.
    """
    _ensure_kill_switch_off(ebull_test_conn)
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.params_metadata == ()
