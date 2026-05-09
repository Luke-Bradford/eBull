"""Ingest-sweep adapter round-trip tests (#1078, umbrella #1064 PR6).

DB-backed against the worker ``ebull_test`` template. Each test seeds
the relevant manifest / freshness / per-source log rows then asserts
the adapter renders the spec-aligned ``ProcessRow`` shape.

Pinned invariants (Codex pre-impl review fixes):

* H2: ``pending_retry`` is NOT emitted by sweep rows — sweeps don't
  carry ``next_fire_at``; the underlying scheduled_job row is the
  retry surface.
* H3: freshness-only sweeps read ``last_n_errors`` from
  ``data_freshness_index.state_reason`` (NOT manifest.error). Manifest
  + log sweeps prefer the per-source log when populated.
"""

from __future__ import annotations

import psycopg

from app.services.processes import ingest_sweep_adapter

_TEST_INSTRUMENT_ID = 9201078  # PR6 fixture instrument; pinned per-test to keep cohorts isolated.


def _seed_instrument(conn: psycopg.Connection[tuple]) -> int:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (%s, 'TST_SW6', 'TST_SW6 Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (_TEST_INSTRUMENT_ID,),
    )
    return _TEST_INSTRUMENT_ID


def _ensure_kill_switch_off(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, FALSE, NULL, NULL, NULL)
        ON CONFLICT (id) DO UPDATE
        SET is_active = FALSE, activated_at = NULL, activated_by = NULL, reason = NULL
        """
    )


def _wipe_test_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute("DELETE FROM sec_filing_manifest WHERE source IN ('sec_form4', 'sec_13f_hr', 'sec_n_port')")
    conn.execute("DELETE FROM data_freshness_index WHERE source IN ('sec_form3', 'sec_form4', 'sec_def14a', 'sec_8k')")
    conn.execute("DELETE FROM n_port_ingest_log")
    conn.execute("DELETE FROM institutional_holdings_ingest_log")
    conn.execute(
        "DELETE FROM job_runs WHERE job_name IN ("
        "'sec_form3_ingest','sec_filing_documents_ingest','sec_def14a_ingest',"
        "'sec_8k_events_ingest','sec_13f_quarterly_sweep','sec_n_port_ingest')"
    )
    conn.execute(
        "DELETE FROM pending_job_requests WHERE job_name IN ("
        "'sec_form3_ingest','sec_filing_documents_ingest','sec_def14a_ingest',"
        "'sec_8k_events_ingest','sec_13f_quarterly_sweep','sec_n_port_ingest')"
    )


def _insert_freshness_row(
    conn: psycopg.Connection[tuple],
    *,
    subject_id: str,
    source: str,
    state: str,
    state_reason: str | None = None,
    instrument_id: int | None = None,
) -> None:
    if instrument_id is None:
        instrument_id = _seed_instrument(conn)
    conn.execute(
        """
        INSERT INTO data_freshness_index (
            subject_type, subject_id, source, state, state_reason, instrument_id
        ) VALUES ('issuer', %s, %s, %s, %s, %s)
        ON CONFLICT (subject_type, subject_id, source) DO UPDATE
        SET state = EXCLUDED.state,
            state_reason = EXCLUDED.state_reason,
            instrument_id = EXCLUDED.instrument_id
        """,
        (subject_id, source, state, state_reason, instrument_id),
    )


def _insert_manifest_row(
    conn: psycopg.Connection[tuple],
    *,
    accession_number: str,
    cik: str,
    source: str,
    form: str,
    ingest_status: str,
    error: str | None = None,
    instrument_id: int | None = None,
) -> None:
    # Manifest CHECK requires instrument_id non-null on issuer rows.
    # Seed a deterministic test instrument so cohorts stay isolated
    # across tests without depending on prior dev-DB state.
    if instrument_id is None:
        instrument_id = _seed_instrument(conn)
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (
            accession_number, cik, form, source, subject_type, subject_id,
            instrument_id, filed_at, ingest_status, error, last_attempted_at
        ) VALUES (%s, %s, %s, %s, 'issuer', %s, %s, now(), %s, %s, now())
        """,
        (
            accession_number,
            cik,
            form,
            source,
            str(instrument_id),
            instrument_id,
            ingest_status,
            error,
        ),
    )


def _insert_n_port_log(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    status: str,
    error: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO n_port_ingest_log (
            accession_number, filer_cik, status, holdings_inserted, holdings_skipped, error
        ) VALUES (%s, '0001234567', %s, 0, 0, %s)
        """,
        (accession, status, error),
    )


def test_sweep_registry_has_six_canonical_sweeps(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Pin the v1 sweep registry shape — adding/removing sweeps is a deliberate change."""
    ids = ingest_sweep_adapter.sweep_process_ids()
    assert set(ids) == {
        "sec_form3_sweep",
        "sec_form4_sweep",
        "sec_def14a_sweep",
        "sec_8k_sweep",
        "sec_13f_sweep",
        "nport_sweep",
    }
    for pid in ids:
        assert ingest_sweep_adapter.is_sweep(pid)
    assert not ingest_sweep_adapter.is_sweep("not_a_sweep")


def test_freshness_only_sweep_failed_reads_state_reason(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex H3: freshness-only sweep ``last_n_errors`` MUST come from
    ``data_freshness_index.state_reason`` (not manifest.error)."""
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    _insert_freshness_row(
        ebull_test_conn,
        subject_id="42",
        source="sec_form3",
        state="error",
        state_reason="HTTP 429 rate-limited",
    )
    _insert_freshness_row(
        ebull_test_conn,
        subject_id="43",
        source="sec_form3",
        state="error",
        state_reason="HTTP 429 rate-limited",
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_form3_sweep")
    assert row is not None
    assert row.process_id == "sec_form3_sweep"
    assert row.mechanism == "ingest_sweep"
    assert row.status == "failed"
    # Sweeps don't carry next_fire_at — H2 invariant.
    assert row.next_fire_at is None
    assert row.cadence_cron is None
    # READ-ONLY surface — no triggering / cancelling.
    assert row.can_iterate is False
    assert row.can_full_wash is False
    assert row.can_cancel is False
    # Errors come from the freshness scheduler, NOT the manifest.
    assert len(row.last_n_errors) == 1
    assert row.last_n_errors[0].error_class == "HTTP 429 rate-limited"
    assert row.last_n_errors[0].count == 2


def test_running_sweep_reflects_underlying_job_in_flight(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Spec §"running" — sweep flips to ``running`` when the underlying
    scheduled_job has a manual_job request in flight OR a job_runs
    row in ``status='running'``."""
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    # Underlying job is sec_filing_documents_ingest for sec_form4_sweep.
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, status)
        VALUES ('sec_filing_documents_ingest', now(), 'running')
        """
    )
    # Even if there are failures, ``running`` wins.
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="0000000000-00-000001",
        cik="0000000001",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        error="parser exploded",
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_form4_sweep")
    assert row is not None
    assert row.status == "running"
    # Auto-hide-on-retry: errors empty when status='running'.
    assert row.last_n_errors == ()


def test_nport_sweep_reads_log_errors_first(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """N-PORT sweep prefers ``n_port_ingest_log.error`` over manifest
    error when log rows exist (parser failures land on the log)."""
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="0000000000-00-000099",
        cik="0001111111",
        source="sec_n_port",
        form="N-PORT",
        ingest_status="failed",
        error="manifest-shape parse error",
        instrument_id=None,
    )
    _insert_n_port_log(
        ebull_test_conn,
        accession="0000000000-00-000100",
        status="failed",
        error="NPortMissingSeriesError: missing seriesId for fund X",
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="nport_sweep")
    assert row is not None
    assert row.status == "failed"
    # log dominates — first error_class is the log's
    assert any("NPortMissingSeriesError" in e.error_class for e in row.last_n_errors)


def test_ok_sweep_when_no_failures_and_no_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    # Seed a healthy freshness row (state='current') and no errors.
    _insert_freshness_row(
        ebull_test_conn,
        subject_id="100",
        source="sec_def14a",
        state="current",
        state_reason=None,
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_def14a_sweep")
    assert row is not None
    assert row.status == "ok"
    assert row.last_n_errors == ()


def test_disabled_when_kill_switch_active(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _wipe_test_state(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO kill_switch (id, is_active, activated_at, activated_by, reason)
        VALUES (TRUE, TRUE, now(), 'test', 'kill switch sweep')
        ON CONFLICT (id) DO UPDATE
        SET is_active = TRUE, activated_at = now(), activated_by = 'test', reason = 'kill switch sweep'
        """
    )
    ebull_test_conn.commit()
    try:
        row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_form4_sweep")
        assert row is not None
        assert row.status == "disabled"
    finally:
        _ensure_kill_switch_off(ebull_test_conn)
        ebull_test_conn.commit()


def test_list_rows_emits_one_per_registered_sweep(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    ebull_test_conn.commit()
    rows = ingest_sweep_adapter.list_rows(ebull_test_conn)
    pids = {r.process_id for r in rows}
    assert pids == set(ingest_sweep_adapter.sweep_process_ids())
    for r in rows:
        assert r.mechanism == "ingest_sweep"
        # Universal sweep invariants (PR6 Codex H2 / READ-ONLY surface).
        assert r.next_fire_at is None
        assert r.cadence_cron is None
        assert r.can_iterate is False
        assert r.can_full_wash is False
        assert r.can_cancel is False
        # ``pending_retry`` is NEVER emitted by sweeps in PR6.
        assert r.status != "pending_retry"


def test_list_runs_returns_log_history_for_log_backed_sweep(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    _insert_n_port_log(
        ebull_test_conn,
        accession="0000000000-00-000200",
        status="success",
        error=None,
    )
    _insert_n_port_log(
        ebull_test_conn,
        accession="0000000000-00-000201",
        status="failed",
        error="parse error",
    )
    ebull_test_conn.commit()
    runs = ingest_sweep_adapter.list_runs(ebull_test_conn, process_id="nport_sweep", days=7)
    assert len(runs) == 2
    statuses = {r.status for r in runs}
    assert statuses == {"success", "failure"}


def test_list_runs_empty_for_freshness_only_sweep(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Freshness-only sweeps have no log table — list_runs returns []."""
    runs = ingest_sweep_adapter.list_runs(ebull_test_conn, process_id="sec_form3_sweep", days=7)
    assert runs == []


def test_list_runs_unknown_process_id_returns_empty(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    runs = ingest_sweep_adapter.list_runs(ebull_test_conn, process_id="not_a_sweep", days=7)
    assert runs == []


def test_list_runs_rejects_non_positive_days(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    import pytest

    with pytest.raises(ValueError):
        ingest_sweep_adapter.list_runs(ebull_test_conn, process_id="nport_sweep", days=0)


# ---------------------------------------------------------------------------
# PR8 (#1083) — four-case stale model, ingest_sweep adapter integration.
# ---------------------------------------------------------------------------


def _insert_freshness_with_expected_next_at(
    conn: psycopg.Connection[tuple],
    *,
    subject_id: str,
    source: str,
    state: str,
    expected_next_at_offset_minutes: int,
    instrument_id: int,
) -> None:
    """Helper for PR8 stale-detection tests — sets
    ``expected_next_at`` to ``now() + offset`` so the watermark_gap
    rule can find an overdue row.

    Offset is parameterised through ``make_interval`` rather than
    f-string interpolated to keep the SQL a literal string for
    pyright (LiteralString constraint on `psycopg.execute`).
    """
    conn.execute(
        """
        INSERT INTO data_freshness_index (
            subject_type, subject_id, source, state, expected_next_at,
            instrument_id
        ) VALUES ('issuer', %s, %s, %s,
                  now() + make_interval(mins => %s),
                  %s)
        ON CONFLICT (subject_type, subject_id, source) DO UPDATE
        SET state            = EXCLUDED.state,
            expected_next_at = EXCLUDED.expected_next_at,
            instrument_id    = EXCLUDED.instrument_id
        """,
        (subject_id, source, state, expected_next_at_offset_minutes, instrument_id),
    )


def test_sweep_watermark_gap_fires_when_freshness_overdue(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A freshness-driven sweep with at least one
    ``data_freshness_index`` row in ``expected_next_at`` past the gap
    tolerance surfaces ``watermark_gap``."""
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn)
    _insert_freshness_with_expected_next_at(
        ebull_test_conn,
        subject_id=str(instrument_id),
        source="sec_form3",
        state="current",
        expected_next_at_offset_minutes=-30,
        instrument_id=instrument_id,
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_form3_sweep")
    assert row is not None
    assert "watermark_gap" in row.stale_reasons


def test_sweep_never_schedule_misses_or_queue_stucks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Sweeps have no own cron + no own pending_job_requests rows in
    v1. Even with stale freshness fixtures, the sweep row must NOT
    surface schedule_missed or queue_stuck."""
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn)
    _insert_freshness_with_expected_next_at(
        ebull_test_conn,
        subject_id=str(instrument_id),
        source="sec_form3",
        state="current",
        expected_next_at_offset_minutes=-120,
        instrument_id=instrument_id,
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_form3_sweep")
    assert row is not None
    assert "schedule_missed" not in row.stale_reasons
    assert "queue_stuck" not in row.stale_reasons
    # No active_run on a sweep — mid_flight_stuck cannot fire either.
    assert "mid_flight_stuck" not in row.stale_reasons


def test_sweep_running_suppresses_watermark_gap(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When the underlying scheduled job is in flight the sweep is
    ``running``; watermark_gap is suppressed because we ARE the
    catch-up."""
    _ensure_kill_switch_off(ebull_test_conn)
    _wipe_test_state(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn)
    _insert_freshness_with_expected_next_at(
        ebull_test_conn,
        subject_id=str(instrument_id),
        source="sec_form3",
        state="current",
        expected_next_at_offset_minutes=-30,
        instrument_id=instrument_id,
    )
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, status)
        VALUES ('sec_form3_ingest', now(), 'running')
        """
    )
    ebull_test_conn.commit()

    row = ingest_sweep_adapter.get_row(ebull_test_conn, process_id="sec_form3_sweep")
    assert row is not None
    assert row.status == "running"
    assert "watermark_gap" not in row.stale_reasons
