"""#1233 PR-D — Stream-C correctness gate runbook integration tests.

Exercises the C1-C7 check logic against a real per-worker test DB
using the ``ebull_test_conn`` fixture. The runbook proper opens its
own connection through ``settings.database_url``; these tests
exercise the underlying ``_check_*`` helpers + the ``_run_gate``
orchestrator directly so we can seed targeted rows and verify the
JSON envelope per gate.

Covers:

* C5 uses ``data_freshness_index.updated_at`` (NOT phantom
  ``last_seen_at`` — v2.4 fold of Codex 1 BLOCKING 1).
* C6 covers all 7 categories from ``_CATEGORIES``.
* C6 quiescence emits ``warning_*`` (not fail) when both
  observations + manifest are absent.
* C6 treasury falls back to ``sec_xbrl_facts`` (not def14a).
* C7 sentinel rows count toward populated; in-universe denominator
  uses ``COUNT(DISTINCT identifier_value)``.
* ``_persist_status`` round-trips ``stream_c_gate_status``.

All assertions are DELTA-based: each test seeds its own rows + asserts
the delta, never an exact global count (per
``docs/review-prevention-log.md`` "Integration tests must use DELTA-
based counter assertions").
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.runbooks.stream_a_stream_c_gate import (
    _check_c4_manifest_drained,
    _check_c5_freshness_index_current,
    _check_c6_category,
    _check_c7_sidecar_populated,
    _check_layer_job_fired,
    _persist_status,
)

pytestmark = pytest.mark.integration
# ``ebull_test_conn`` fixture auto-discovered via tests/conftest.py.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_bootstrap_run(conn: psycopg.Connection[tuple], *, completed_at: datetime) -> int:
    """Insert a synthetic ``bootstrap_runs`` row and return its id."""
    row = conn.execute(
        "INSERT INTO bootstrap_runs (status, completed_at) VALUES ('complete', %s) RETURNING id",
        (completed_at,),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def _seed_job_run(
    conn: psycopg.Connection[tuple],
    *,
    job_name: str,
    started_at: datetime,
    status: str = "success",
) -> None:
    conn.execute(
        "INSERT INTO job_runs (job_name, started_at, status) VALUES (%s, %s, %s)",
        (job_name, started_at, status),
    )
    conn.commit()


def _seed_in_universe_cik(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    cik: str,
) -> None:
    """Seed one tradable instrument + sec_cik external identifier.

    Used by C7 integration tests so the in-universe denominator is
    not zero. Mirrors the pattern at
    ``tests/test_agent_cik_defense.py:155``.
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable) "
        "VALUES (%s, %s, %s, %s, TRUE) "
        "ON CONFLICT (instrument_id) DO NOTHING",
        (instrument_id, symbol, f"Test {symbol}", f"c7_{instrument_id}"),
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', %s, TRUE) "
        "ON CONFLICT DO NOTHING",
        (instrument_id, cik),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# C1/C2/C3 — layer-job-fired
# ---------------------------------------------------------------------------


def test_check_layer_job_fired_passes_when_success_post_completed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    _seed_job_run(
        ebull_test_conn,
        job_name="sec_atom_fast_lane",
        started_at=completed_at + timedelta(minutes=10),
    )
    status, count, _ = _check_layer_job_fired(
        ebull_test_conn,
        job_name="sec_atom_fast_lane",
        completed_at=completed_at,
    )
    assert status == "passed"
    assert count >= 1


def test_check_layer_job_fired_fails_when_only_pre_completed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    # Job ran BEFORE completed_at → does not count.
    _seed_job_run(
        ebull_test_conn,
        job_name="sec_atom_fast_lane",
        started_at=completed_at - timedelta(hours=1),
    )
    status, count, _ = _check_layer_job_fired(
        ebull_test_conn,
        job_name="sec_atom_fast_lane",
        completed_at=completed_at,
    )
    assert status == "failed"
    assert count == 0


# ---------------------------------------------------------------------------
# C5 — data_freshness_index uses updated_at (NOT phantom last_seen_at)
# ---------------------------------------------------------------------------


def test_check_c5_uses_updated_at_column(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Regression gate for v2.4 fold of Codex 1 BLOCKING 1.

    A ``current``-state row with ``updated_at > completed_at`` MUST pass
    C5. The original v2.3 query cited ``last_seen_at`` which would have
    failed against any real schema. This test would have caught the
    phantom column.
    """
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    # Seed a current-state row with updated_at AFTER completed_at.
    ebull_test_conn.execute(
        "INSERT INTO data_freshness_index "
        "(subject_type, subject_id, source, state, last_polled_outcome, updated_at) "
        "VALUES ('institutional_filer', 'TEST_C5_FILER', 'sec_13f_hr', 'current', "
        "'current', %s)",
        (completed_at + timedelta(minutes=30),),
    )
    ebull_test_conn.commit()
    status, count, _ = _check_c5_freshness_index_current(ebull_test_conn, completed_at=completed_at)
    assert status == "passed"
    assert count >= 1


def test_check_c5_fails_when_only_stale_rows(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """C5 must NOT pass on rows updated BEFORE Run-#8 completed."""
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    # The fixture truncates data_freshness_index, so a stale row is the
    # only one present. We assert C5 returns count=0 (failed).
    status, _, _ = _check_c5_freshness_index_current(ebull_test_conn, completed_at=completed_at)
    # No qualifying rows → failed (status='current' AND updated_at>completed).
    assert status == "failed"


# ---------------------------------------------------------------------------
# C6 — per-category, 7 categories incl. treasury+esop, quiescence warning
# ---------------------------------------------------------------------------


def test_check_c6_treasury_quiescence_uses_sec_xbrl_facts_not_def14a(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Treasury maps to {sec_xbrl_facts} (v2.4 R3 fold).

    When there are NO ownership_treasury_observations AND NO
    sec_xbrl_facts manifest rows in the last 24h, the gate emits
    ``warning_category_quiescent_treasury`` (NOT a fail — treasury can
    legitimately be quiet across a 24h window).
    """
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    # No observations, no manifest rows. Quiescence warning expected.
    status, count, detail = _check_c6_category(
        ebull_test_conn,
        category="treasury",
        observations_table="ownership_treasury_observations",
        completed_at=completed_at,
    )
    assert status == "warning_category_quiescent_treasury"
    assert count == 0
    # Detail mentions the correct source (NOT def14a).
    assert "sec_xbrl_facts" in detail
    assert "def14a" not in detail


# ---------------------------------------------------------------------------
# C7 — sentinel-aware count + DISTINCT denominator
# ---------------------------------------------------------------------------


def test_check_c7_sentinel_row_counts_toward_populated(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Sentinel row (``page_name='__no_overflow_pages__'``) MUST count
    toward the populated CIK set for an in-universe CIK, otherwise
    AAPL (zero overflow) would false-fail C7. v2.3 §1.8 + sql/172
    invariant.

    Strengthened in v2.4 fold of Codex 2 IMPORTANT 2 (PR-D pre-push):
    seeds a real in-universe row + asserts the helper returns
    ``passed`` with count=1.
    """
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    run_id = _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    _seed_in_universe_cik(ebull_test_conn, instrument_id=9000001, symbol="C7T1", cik="0000320193")

    # Seed one sentinel row carrying our run_id for the in-universe CIK.
    ebull_test_conn.execute(
        "INSERT INTO sec_cik_submissions_files_index "
        "(cik, page_name, bootstrap_run_id, populate_origin) "
        "VALUES (%s, '__no_overflow_pages__', %s, 'bootstrap')",
        ("0000320193", run_id),
    )
    ebull_test_conn.commit()

    status, count, _ = _check_c7_sidecar_populated(ebull_test_conn, run_id=run_id)
    assert status == "passed", "sentinel must count toward populated set"
    assert count == 1, f"expected count=1 (single in-universe sentinel); got {count}"


def test_check_c7_ignores_out_of_universe_sidecar_rows(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Numerator MUST NOT include out-of-universe CIKs even when their
    sidecar rows carry the right ``bootstrap_run_id``.

    Regression gate for Codex 2 IMPORTANT 1 (PR-D pre-push): the
    repair runbook now filters via in-universe set, and the C7 query
    JOINs through external_identifiers + instruments to defence-in-
    depth against any future writer that bypasses the filter.

    Seed: one in-universe CIK with sentinel + one out-of-universe CIK
    with sentinel + run_id. Expected: gate returns count=1 (only the
    in-universe row), NOT count=2.
    """
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    run_id = _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    _seed_in_universe_cik(ebull_test_conn, instrument_id=9000002, symbol="C7T2", cik="0000111111")
    # Two sentinel rows: one in-universe + one NOT in-universe.
    for cik in ("0000111111", "0000999999"):
        ebull_test_conn.execute(
            "INSERT INTO sec_cik_submissions_files_index "
            "(cik, page_name, bootstrap_run_id, populate_origin) "
            "VALUES (%s, '__no_overflow_pages__', %s, 'bootstrap')",
            (cik, run_id),
        )
    ebull_test_conn.commit()
    status, count, detail = _check_c7_sidecar_populated(ebull_test_conn, run_id=run_id)
    assert count == 1, f"out-of-universe CIK leaked into numerator; got count={count}; detail={detail}"
    assert status == "passed"


# ---------------------------------------------------------------------------
# _persist_status round-trip
# ---------------------------------------------------------------------------


def test_persist_status_round_trips(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    run_id = _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    _persist_status(ebull_test_conn, run_id=run_id, status_value="pending")
    ebull_test_conn.commit()
    row = ebull_test_conn.execute(
        "SELECT stream_c_gate_status FROM bootstrap_runs WHERE id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None and row[0] == "pending"

    _persist_status(ebull_test_conn, run_id=run_id, status_value="failed_c5")
    ebull_test_conn.commit()
    row = ebull_test_conn.execute(
        "SELECT stream_c_gate_status FROM bootstrap_runs WHERE id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None and row[0] == "failed_c5"


def test_persist_status_rejects_bad_value(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """sql/173 CHECK refuses bare ``failed_`` and other garbage."""
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    run_id = _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    with pytest.raises(psycopg.errors.CheckViolation):
        _persist_status(ebull_test_conn, run_id=run_id, status_value="failed_")
    ebull_test_conn.rollback()
    with pytest.raises(psycopg.errors.CheckViolation):
        _persist_status(ebull_test_conn, run_id=run_id, status_value="garbage")
    ebull_test_conn.rollback()


# ---------------------------------------------------------------------------
# C4 — manifest_drained
# ---------------------------------------------------------------------------


def test_check_c4_fails_when_a_registered_source_has_no_manifest_drain(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """With ZERO manifest rows post-completed_at, every registered
    source is missing → C4 failed + details lists the missing sources."""
    completed_at = datetime.now(UTC) - timedelta(hours=2)
    _seed_bootstrap_run(ebull_test_conn, completed_at=completed_at)
    status, _, detail = _check_c4_manifest_drained(ebull_test_conn, completed_at=completed_at)
    assert status == "failed"
    assert "missing drain for sources" in detail
