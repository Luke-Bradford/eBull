"""Watermark resolver tests (#1073, umbrella #1064 PR4).

Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §"Watermark + resume contract" + §PR4 test plan.

Each ``cursor_kind`` round-trips against the worker ``ebull_test``
template DB. Mocking psycopg cursors loses the SQL-shape guarantees
the resolver relies on (MAX(...) FILTER, JSONB column types, the
freshness-index partial indexes); spec test plan also explicitly asks
for per-cursor-kind round-trips so the regression risk catches schema
drift downstream.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg

from app.services.processes.watermarks import (
    atom_etag_target_for,
    freshness_source_for,
    manifest_source_for,
    resolve_watermark,
)

# ---------------------------------------------------------------------------
# Bootstrap — stage_index cursor
# ---------------------------------------------------------------------------


def _seed_bootstrap_run(conn: psycopg.Connection[tuple]) -> int:
    row = conn.execute(
        """
        INSERT INTO bootstrap_runs (status, completed_at)
        VALUES ('running', NULL)
        RETURNING id
        """
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_bootstrap_stage(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    stage_order: int,
    lane: str,
    status: str,
    completed_offset_seconds: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO bootstrap_stages
               (bootstrap_run_id, stage_key, stage_order, lane, job_name,
                status, started_at, completed_at)
        VALUES (%s, %s, %s, %s, 'job_x',
                %s,
                CASE WHEN %s != 'pending' THEN now() - make_interval(secs => %s) ELSE NULL END,
                CASE WHEN %s = 'success' THEN now() - make_interval(secs => %s) ELSE NULL END)
        """,
        (
            run_id,
            stage_key,
            stage_order,
            lane,
            status,
            status,
            completed_offset_seconds,
            status,
            completed_offset_seconds,
        ),
    )


def test_resolve_bootstrap_stage_index_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _seed_bootstrap_run(ebull_test_conn)
    _seed_bootstrap_stage(
        ebull_test_conn, run_id=run_id, stage_key="init", stage_order=0, lane="init", status="success"
    )
    _seed_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="etoro_meta",
        stage_order=1,
        lane="etoro",
        status="success",
        completed_offset_seconds=10,
    )
    _seed_bootstrap_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_form4",
        stage_order=5,
        lane="sec",
        status="success",
        completed_offset_seconds=5,
    )
    _seed_bootstrap_stage(
        ebull_test_conn, run_id=run_id, stage_key="sec_def14a", stage_order=6, lane="sec", status="pending"
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(ebull_test_conn, process_id="bootstrap", mechanism="bootstrap")
    assert wm is not None
    assert wm.cursor_kind == "stage_index"
    # Lane order is stable: init (0), etoro (1), sec (5).
    assert wm.cursor_value == "etoro:1,init:0,sec:5"
    assert "Resume after stages" in wm.human


def test_resolve_bootstrap_returns_none_when_no_runs(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.commit()
    wm = resolve_watermark(ebull_test_conn, process_id="bootstrap", mechanism="bootstrap")
    assert wm is None


def test_resolve_bootstrap_returns_none_when_no_success_stage(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _seed_bootstrap_run(ebull_test_conn)
    _seed_bootstrap_stage(
        ebull_test_conn, run_id=run_id, stage_key="init", stage_order=0, lane="init", status="running"
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(ebull_test_conn, process_id="bootstrap", mechanism="bootstrap")
    # A run exists but no stage has reached 'success'; nothing to resume from.
    assert wm is None


# ---------------------------------------------------------------------------
# Universe sync — epoch cursor
# ---------------------------------------------------------------------------


def test_resolve_universe_sync_epoch_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, status, requested_at)
        VALUES ('manual_job', 'nightly_universe_sync', 'completed',
                now() - interval '1 hour')
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, status, requested_at)
        VALUES ('manual_job', 'nightly_universe_sync', 'completed', now())
        """
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="nightly_universe_sync",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "epoch"
    # cursor_value is stringified request_id; specific value depends
    # on sequence state across the test run, so just shape-check.
    assert wm.cursor_value.isdigit()
    assert "Resume after universe epoch" in wm.human


def test_resolve_universe_sync_epoch_returns_none_when_no_completed_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, status)
        VALUES ('manual_job', 'nightly_universe_sync', 'pending')
        """
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="nightly_universe_sync",
        mechanism="scheduled_job",
    )
    assert wm is None


# ---------------------------------------------------------------------------
# Candle refresh — instrument_offset cursor
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (%s, %s, %s, 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Co"),
    )


def test_resolve_candle_offset_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid = 9_000_001
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TST_WM")
    ebull_test_conn.execute(
        """
        INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume)
        VALUES (%s, '2026-05-01', 1, 1, 1, 1, 1),
               (%s, '2026-05-08', 2, 2, 2, 2, 2)
        ON CONFLICT DO NOTHING
        """,
        (iid, iid),
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="daily_candle_refresh",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "instrument_offset"
    # MAX(price_date) is at least 2026-05-08; could be later if other
    # tests insert newer dates against the shared template.
    assert wm.cursor_value >= "2026-05-08"
    assert "Resume from candles" in wm.human


# ---------------------------------------------------------------------------
# NPORT — accession cursor (n_port_ingest_log)
# ---------------------------------------------------------------------------


def test_resolve_n_port_accession_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO n_port_ingest_log (accession_number, filer_cik, status, fetched_at)
        VALUES ('0000000001-26-000001', '0000123', 'success', now() - interval '1 day'),
               ('0000000002-26-000001', '0000123', 'success', now())
        """
    )
    ebull_test_conn.execute(
        """
        INSERT INTO n_port_ingest_log (accession_number, filer_cik, status, fetched_at)
        VALUES ('0000000003-26-000001', '0000123', 'failed', now())
        """
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="sec_n_port_ingest",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "accession"
    # Failed accession (000003) must NOT be the cursor — only success
    # rows count for "last processed".
    assert wm.cursor_value == "0000000002-26-000001"


# ---------------------------------------------------------------------------
# SEC submissions — filed_at cursor (data_freshness_index)
# ---------------------------------------------------------------------------


def _seed_freshness_row(
    conn: psycopg.Connection[tuple],
    *,
    source: str,
    subject_id: str,
    last_known_filed_at: datetime,
    state: str = "current",
    next_recheck_at: datetime | None = None,
    instrument_id: int | None = None,
    subject_type: str = "issuer",
) -> None:
    conn.execute(
        """
        INSERT INTO data_freshness_index
            (subject_type, subject_id, source, last_known_filed_at, state,
             next_recheck_at, cik, instrument_id)
        VALUES (%s, %s, %s, %s, %s, %s, NULL, %s)
        """,
        (subject_type, subject_id, source, last_known_filed_at, state, next_recheck_at, instrument_id),
    )


def test_resolve_filed_at_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid_a = 9_000_010
    iid_b = 9_000_011
    _seed_instrument(ebull_test_conn, iid=iid_a, symbol="TST_F3A")
    _seed_instrument(ebull_test_conn, iid=iid_b, symbol="TST_F3B")
    older = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    newer = datetime(2026, 5, 8, 12, 0, 0, tzinfo=UTC)
    _seed_freshness_row(
        ebull_test_conn,
        source="sec_form3",
        subject_id=str(iid_a),
        last_known_filed_at=older,
        instrument_id=iid_a,
    )
    _seed_freshness_row(
        ebull_test_conn,
        source="sec_form3",
        subject_id=str(iid_b),
        last_known_filed_at=newer,
        state="expected_filing_overdue",
        instrument_id=iid_b,
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="sec_form3_ingest",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "filed_at"
    assert wm.cursor_value == newer.isoformat()
    assert wm.last_advanced_at == newer
    # 1 of 2 awaiting (the overdue one); subjects_total counts both.
    assert "1 of 2 subjects awaiting next poll" in wm.human


def test_resolve_filed_at_returns_none_when_source_empty(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.commit()
    wm = resolve_watermark(
        ebull_test_conn,
        process_id="sec_form3_ingest",
        mechanism="scheduled_job",
    )
    assert wm is None


# ---------------------------------------------------------------------------
# Manifest worker — accession cursor (sec_filing_manifest)
# ---------------------------------------------------------------------------


def _seed_manifest_row(
    conn: psycopg.Connection[tuple],
    *,
    accession_number: str,
    source: str,
    instrument_id: int,
    filed_at: datetime,
    ingest_status: str = "parsed",
    next_retry_at: datetime | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO sec_filing_manifest
            (accession_number, cik, form, source,
             subject_type, subject_id, instrument_id, filed_at,
             ingest_status, next_retry_at)
        VALUES (%s, '0000123', '4', %s, 'issuer', %s, %s, %s, %s, %s)
        """,
        (
            accession_number,
            source,
            str(instrument_id),
            instrument_id,
            filed_at,
            ingest_status,
            next_retry_at,
        ),
    )


def test_resolve_manifest_accession_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid = 9_000_020
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TST_MAN")
    older = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    newer = datetime(2026, 5, 8, 12, 0, 0, tzinfo=UTC)
    _seed_manifest_row(
        ebull_test_conn,
        accession_number="0000000001-26-000001",
        source="sec_form4",
        instrument_id=iid,
        filed_at=older,
    )
    _seed_manifest_row(
        ebull_test_conn,
        accession_number="0000000002-26-000002",
        source="sec_form4",
        instrument_id=iid,
        filed_at=newer,
        ingest_status="pending",
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="sec_filing_documents_ingest",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "accession"
    assert wm.cursor_value == "0000000002-26-000002"
    assert "1 accessions awaiting drain" in wm.human


# ---------------------------------------------------------------------------
# Atom ETag — external_data_watermarks
# ---------------------------------------------------------------------------


def test_resolve_atom_etag_round_trip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO external_data_watermarks
            (source, key, watermark, watermark_at, fetched_at, response_hash)
        VALUES ('sec.tickers', 'global', 'W/"abc123"',
                now() - interval '2 hours', now() - interval '5 minutes', 'hash')
        """
    )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="daily_cik_refresh",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "atom_etag"
    assert wm.cursor_value == 'W/"abc123"'
    assert "company_tickers.json" in wm.human


def test_resolve_atom_etag_returns_none_when_no_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    ebull_test_conn.commit()
    wm = resolve_watermark(
        ebull_test_conn,
        process_id="daily_cik_refresh",
        mechanism="scheduled_job",
    )
    assert wm is None


# ---------------------------------------------------------------------------
# Unmapped jobs / unknown mechanism
# ---------------------------------------------------------------------------


def test_resolve_returns_none_for_unmapped_job(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    wm = resolve_watermark(
        ebull_test_conn,
        process_id="heartbeat",
        mechanism="scheduled_job",
    )
    assert wm is None


def test_resolve_returns_none_for_ingest_sweep_mechanism(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    wm = resolve_watermark(
        ebull_test_conn,
        process_id="anything",
        mechanism="ingest_sweep",
    )
    assert wm is None


# ---------------------------------------------------------------------------
# Source-helper registry consistency
# ---------------------------------------------------------------------------


def test_source_helpers_round_trip() -> None:
    assert freshness_source_for("sec_form3_ingest") == "sec_form3"
    assert freshness_source_for("sec_insider_transactions_ingest") == "sec_form4"
    assert freshness_source_for("sec_def14a_ingest") == "sec_def14a"
    assert freshness_source_for("sec_8k_events_ingest") == "sec_8k"
    assert freshness_source_for("daily_financial_facts") == "sec_xbrl_facts"
    assert freshness_source_for("fundamentals_sync") == "sec_xbrl_facts"
    # sec_business_summary_ingest retired post-#1155; manifest worker +
    # sec_10k.py parser (#1152) carry 10-K Item 1 writes.
    assert freshness_source_for("sec_business_summary_ingest") is None
    assert freshness_source_for("sec_n_port_ingest") == "sec_n_port"
    # Unmapped jobs must return None — the trigger handler treats None
    # as "no freshness reset to issue".
    assert freshness_source_for("heartbeat") is None
    assert freshness_source_for("daily_candle_refresh") is None  # uses custom resolver


def test_manifest_source_helpers_round_trip() -> None:
    assert manifest_source_for("sec_filing_documents_ingest") == "sec_form4"
    assert manifest_source_for("sec_form3_ingest") is None  # freshness-only
    assert manifest_source_for("heartbeat") is None


def test_atom_etag_helper_round_trip() -> None:
    assert atom_etag_target_for("daily_cik_refresh") == ("sec.tickers", "global")
    assert atom_etag_target_for("sec_form3_ingest") is None
    assert atom_etag_target_for("heartbeat") is None


# ---------------------------------------------------------------------------
# Resolver swallows exceptions
# ---------------------------------------------------------------------------


def test_resolve_swallows_unexpected_exception(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A per-row resolver failure must not 500 the snapshot.

    Spec §"Failure-mode invariants". Inject a resolver that raises and
    confirm the public ``resolve_watermark`` returns None instead of
    propagating.
    """

    class _BadConn:
        # Minimal stub that lets the resolver get past type-narrowing
        # but raises on cursor() access — typical SQLSTATE-08006 shape.
        def cursor(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise psycopg.OperationalError("connection broken")

    wm = resolve_watermark(
        _BadConn(),  # type: ignore[arg-type]
        process_id="sec_form3_ingest",
        mechanism="scheduled_job",
    )
    assert wm is None


# ---------------------------------------------------------------------------
# Resume-after-cancel guarantee (idempotent replay, NOT transactional)
# ---------------------------------------------------------------------------


def test_resume_after_cancel_via_idempotent_manifest_cohort(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Spec §"Resume after cancel" + post-Codex B6.

    The watermark advance is per-accession on commit. Cancel mid-drain
    means the next Iterate sees the same ``WHERE ingest_status='pending'``
    cohort minus the accessions already ingested. We model this by:
      1. Seeding a 3-accession cohort, all pending.
      2. Marking the first as parsed (= ingester succeeded on it before
         cancel landed).
      3. Asserting the manifest watermark surfaces the pending cohort
         count = 2 (one drained, two awaiting).
    The Iterate path itself does no SQL; the pending-cohort-minus-drained
    invariant lives in the manifest worker's existing ON CONFLICT
    upsert (sql/118 idx_manifest_status_retry).
    """
    iid = 9_000_030
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TST_RC")
    base = datetime(2026, 5, 8, 12, 0, 0, tzinfo=UTC)
    for offset, accession, status in [
        (0, "0000000001-26-000001", "parsed"),
        (1, "0000000002-26-000001", "pending"),
        (2, "0000000003-26-000001", "pending"),
    ]:
        _seed_manifest_row(
            ebull_test_conn,
            accession_number=accession,
            source="sec_form4",
            instrument_id=iid,
            filed_at=base + timedelta(seconds=offset),
            ingest_status=status,
        )
    ebull_test_conn.commit()

    wm = resolve_watermark(
        ebull_test_conn,
        process_id="sec_filing_documents_ingest",
        mechanism="scheduled_job",
    )
    assert wm is not None
    assert wm.cursor_kind == "accession"
    # pending count is the cohort the next iterate has yet to drain.
    assert "2 accessions awaiting drain" in wm.human
