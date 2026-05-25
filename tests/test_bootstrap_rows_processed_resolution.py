"""#1225 — rows_processed resolution hardening.

Two test classes:

* Layer A: ``_run_one_stage`` retry-once + contained-fail on persistent
  ``_resolve_stage_rows`` exception. Pre-fix: any DB error during
  resolution silently produced ``bootstrap_stages.rows_processed=NULL``
  which then tripped the downstream strict-gate floor and blocked S23/S24.
  Post-fix: retry once, then mark stage ``error`` + return contained
  ``_StageOutcome(success=False, error="rows_processed_resolution_failed
  after 2 attempts: ...")``.

* Layer B: contract pin for the 5 bulk ingester stages. The source
  contract is structurally asymmetric for these stages (sources 2 + 3
  of ``_resolve_stage_rows`` are dead by design — see updated docstring
  at ``app/services/bootstrap_orchestrator.py::_resolve_stage_rows``).
  Source 1 (``bootstrap_archive_results`` non-``__job__`` rows) is the
  load-bearing source. Parametrized regression: for each of the 5
  stage_keys, simulate a ``_record_archive_result`` write and assert
  ``_resolve_stage_rows`` returns the expected SUM (non-None).

Forensic context: ``docs/proposals/etl/audits/1225-rows-processed-null.md``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import psycopg
import pytest

from app.services import bootstrap_orchestrator
from app.services.bootstrap_orchestrator import (
    _resolve_stage_rows,
    _run_one_stage,
    _StageOutcome,
)
from app.services.bootstrap_preconditions import record_archive_result_if_absent
from app.services.bootstrap_state import StageSpec, start_run
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable"),
    # _run_one_stage uses _adapt_zero_arg invokers + opens own psycopg
    # connections; pin to one xdist worker so the per-test DB isn't
    # racing with a parallel test on the same fixture.
    pytest.mark.xdist_group(name="rows_processed_resolution"),
]


# ---------------------------------------------------------------------------
# Layer B fixtures
# ---------------------------------------------------------------------------

# 5 bulk ingester stage_keys per Phase 0 audit. Each writes
# bootstrap_archive_results non-__job__ rows via _record_archive_result.
# Source 1 is load-bearing because sources 2+3 are structurally dead
# (see _resolve_stage_rows docstring).
BULK_INGEST_STAGE_KEYS = (
    "sec_submissions_ingest",
    "sec_companyfacts_ingest",
    "sec_13f_ingest_from_dataset",
    "sec_insider_ingest_from_dataset",
    "sec_nport_ingest_from_dataset",
)


def _seed_run(conn: psycopg.Connection[tuple], stage_key: str) -> int:
    """Create a minimal bootstrap_runs + bootstrap_stages row for the test
    stage. Returns run_id.

    Resets ``bootstrap_state.status`` to ``'pending'`` first — the
    singleton is NOT truncated by ``_PLANNER_TABLES`` (intentionally;
    the row is the boot-state lock). A prior test leaving it at
    ``'running'`` would cause ``start_run`` to raise
    ``BootstrapAlreadyRunning``. ``'pending'`` is the post-init fresh
    state allowed by ``bootstrap_state_status_check`` (sql/136).
    """
    with conn.transaction():
        # Valid status values per sql/136 CHECK: pending, running, complete,
        # partial_error, cancelled. 'pending' is the post-init fresh state.
        conn.execute("UPDATE bootstrap_state SET status = 'pending' WHERE id = 1")
    return start_run(
        conn,
        operator_id=None,
        stage_specs=[
            StageSpec(
                stage_key=stage_key,
                stage_order=1,
                lane="sec_rate",  # any valid Lane literal
                job_name=stage_key,
            )
        ],
    )


# ---------------------------------------------------------------------------
# Layer B — source-1 contract pin (parametrized over 5 bulk stages)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stage_key", BULK_INGEST_STAGE_KEYS)
def test_resolver_returns_sum_when_source1_populated(
    stage_key: str,
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1225 Layer B contract pin — for each of 5 bulk stages, a populated
    source 1 (bootstrap_archive_results non-__job__ rows) MUST resolve to
    the SUM.

    Pure resolver-side regression: prevents a future refactor from breaking
    the only functional source for these 5 stages.
    """
    run_id = _seed_run(ebull_test_conn, stage_key)
    ebull_test_conn.commit()

    # Simulate ingester writes via _record_archive_result (the same path
    # the 5 bulk jobs use at sec_bulk_orchestrator_jobs.py:211/280/386/579/744).
    # Two synthetic archives with non-zero rows_written.
    record_archive_result_if_absent(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key=stage_key,
        archive_name=f"{stage_key}_archive_a.zip",
        rows_written=42,
    )
    record_archive_result_if_absent(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key=stage_key,
        archive_name=f"{stage_key}_archive_b.zip",
        rows_written=8,
    )
    ebull_test_conn.commit()

    resolved = _resolve_stage_rows(
        ebull_test_conn,
        bootstrap_run_id=run_id,
        stage_key=stage_key,
        job_name=stage_key,
        job_runs_id_before=0,
        job_runs_id_after=0,
    )

    assert resolved == 50, (
        f"Source 1 resolution broken for {stage_key!r}: expected SUM=50 "
        f"(42+8), got {resolved!r}. Source 1 is the only functional source "
        f"for the 5 bulk jobs — see _resolve_stage_rows docstring."
    )


@pytest.mark.parametrize("stage_key", BULK_INGEST_STAGE_KEYS)
def test_orchestrated_run_one_stage_persists_rows_processed_via_invoker_archive_writes(
    stage_key: str,
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1225 Layer B end-to-end — exercise the full _run_one_stage chain
    for each of the 5 bulk stages with a fake invoker that calls
    _record_archive_result (the same path real bulk ingesters use at
    sec_bulk_orchestrator_jobs.py:211/280/386/579/744).

    Asserts the orchestrated flow lands a non-NULL bootstrap_stages.rows_processed
    via the source-1 path. Catches writer-path regression classes:
    - bulk job stops calling _record_archive_result → archive_results empty → NULL
    - stage_key drift between writer and resolver → resolver finds nothing → NULL
    - _run_one_stage resolver-output-to-mark_stage_success wiring breaks
      (e.g. NULL passed even when resolver returned an int)

    Out of scope (separate failure mode): _current_running_bootstrap_run_id()
    drift in production bulk-job wrappers. This test uses a fake invoker that
    closes over the run_id; the helper's wrong-run failure mode is its own
    test surface (and is gated upstream by the singleton-running invariant
    at sql/129).
    """
    run_id = _seed_run(ebull_test_conn, stage_key)
    ebull_test_conn.commit()
    db_url = test_database_url()

    # Fake invoker that mirrors what real bulk ingesters do at their
    # _record_archive_result call sites: write a non-__job__ archive
    # results row keyed to the orchestrated run_id + stage_key.
    invoker_call_count = {"n": 0}

    def _fake_bulk_invoker(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        invoker_call_count["n"] += 1
        # Open own connection (mirrors _record_archive_result pattern at
        # sec_bulk_orchestrator_jobs.py:124).
        with psycopg.connect(db_url) as conn:
            record_archive_result_if_absent(
                conn,
                bootstrap_run_id=run_id,
                stage_key=stage_key,
                archive_name=f"{stage_key}_e2e_archive.zip",
                rows_written=123,
            )
            conn.commit()

    outcome = _run_one_stage(
        run_id=run_id,
        stage_key=stage_key,
        job_name=stage_key,
        invoker=_fake_bulk_invoker,
        database_url=db_url,
        params={},
    )

    # Invoker actually ran
    assert invoker_call_count["n"] == 1, f"Fake invoker called {invoker_call_count['n']} times; expected 1"

    # Stage succeeded with non-NULL rows_processed (the bug class fix)
    assert outcome.success is True, f"Expected success outcome; got {outcome!r}"
    assert outcome.rows_processed == 123, (
        f"Expected rows_processed=123 from orchestrated invocation; got "
        f"{outcome.rows_processed!r}. This is the run_id=3 symptom — "
        f"writer wrote source-1 but resolver returned NULL anyway."
    )

    # DB stage row reflects the same (no silent NULL on success)
    with psycopg.connect(db_url) as verify_conn, verify_conn.cursor() as cur:
        cur.execute(
            "SELECT status, rows_processed FROM bootstrap_stages WHERE bootstrap_run_id = %s AND stage_key = %s",
            (run_id, stage_key),
        )
        row = cur.fetchone()
        assert row is not None
        status, rows_processed = row
        assert status == "success"
        assert rows_processed == 123, f"DB stage rows_processed mismatch: expected 123, got {rows_processed!r}"


# ---------------------------------------------------------------------------
# Layer A — resolver exception handling
# ---------------------------------------------------------------------------


def test_run_one_stage_returns_contained_fail_when_resolver_raises_twice(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1225 Layer A — when _resolve_stage_rows raises on BOTH attempts,
    _run_one_stage must:

    1. NOT silently swallow + write NULL (pre-fix behaviour that
       produced run_id=3 S23/S24 blocked-on-success).
    2. Mark stage 'error' in DB BEFORE returning.
    3. Return _StageOutcome(success=False, error="rows_processed_resolution_failed
       after 2 attempts: ...").
    4. NOT raise raw — exception must NOT escape to future.result() at
       _phase_batched_dispatch:2040 (would tear through the orchestrator).
    """
    stage_key = "sec_submissions_ingest"
    run_id = _seed_run(ebull_test_conn, stage_key)
    ebull_test_conn.commit()

    # Fake invoker returns immediately (the stage "succeeds" before
    # rows_processed resolution kicks in).
    def _fake_invoker(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        return None

    # _run_one_stage signature: (run_id, stage_key, job_name, invoker, database_url, params)
    # We need _resolve_stage_rows to raise on both attempts.
    resolve_call_count = {"n": 0}

    def _always_raise(*args: Any, **kwargs: Any) -> int | None:  # noqa: ARG001
        resolve_call_count["n"] += 1
        raise psycopg.errors.SerializationFailure("synthetic SerializationFailure for #1225 Layer A regression test")

    # ``conn.info.dsn`` masks the password; use the fixture helper which
    # constructs the full URL with credentials so the test can open
    # additional connections inside ``_run_one_stage``.
    db_url = test_database_url()

    with patch.object(bootstrap_orchestrator, "_resolve_stage_rows", side_effect=_always_raise):
        outcome = _run_one_stage(
            run_id=run_id,
            stage_key=stage_key,
            job_name=stage_key,
            invoker=_fake_invoker,
            database_url=db_url,
            params={},
        )

    # Assertion 1 — resolver was called twice (retry once)
    assert resolve_call_count["n"] == 2, f"Expected 2 resolver attempts (retry-once); got {resolve_call_count['n']}"

    # Assertion 2 — outcome is contained-fail
    assert isinstance(outcome, _StageOutcome)
    assert outcome.success is False, f"Expected contained-fail _StageOutcome; got success={outcome.success!r}"
    assert outcome.error is not None
    assert outcome.error.startswith("rows_processed_resolution_failed after 2 attempts:"), (
        f"Error message must start with prefix; got: {outcome.error!r}"
    )
    assert outcome.rows_processed is None

    # Assertion 3 — DB persists stage as 'error' (NOT 'running' or 'success')
    with psycopg.connect(db_url) as verify_conn, verify_conn.cursor() as cur:
        cur.execute(
            "SELECT status, last_error FROM bootstrap_stages WHERE bootstrap_run_id = %s AND stage_key = %s",
            (run_id, stage_key),
        )
        row = cur.fetchone()
        assert row is not None
        status, last_error = row
        assert status == "error", (
            f"DB stage status must be 'error' after resolver-failure; got {status!r}. "
            f"Pre-fix bug: status would have stayed 'running' (process gone, no recovery)."
        )
        assert last_error is not None
        assert "rows_processed_resolution_failed" in last_error
