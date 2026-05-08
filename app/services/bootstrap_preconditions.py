"""Pre-write validation for first-install bootstrap stages (#1020).

Every Phase C / D / E invoker calls into this module BEFORE any DB
write to verify that:

  1. **Provenance** — required upstream B-stage / C-stage rows exist
     in ``bootstrap_archive_results`` for the current
     ``bootstrap_run_id``. The row's existence proves the upstream
     stage ran in THIS bootstrap run, not a prior one.

  2. **Coverage adequacy** (Phase C only) — the producer reference
     table has populated enough rows in the current A1 universe
     cohort. Stale partial mappings are caught here before downstream
     ingest writes nothing.

A precondition failure raises ``BootstrapPreconditionError`` so the
orchestrator marks the stage ``error`` with a clear operator-visible
message. Without this guard, the older path silently no-op'd
(empty lookup table → 100 % rows_skipped_unresolved → stage marked
``success`` with zero writes).

Spec: docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


# Default coverage ratios. Operator overrides via env vars.
DEFAULT_MIN_CIK_COVERAGE_RATIO = float(os.environ.get("BOOTSTRAP_MIN_CIK_COVERAGE_RATIO", "0.50"))
DEFAULT_MIN_CUSIP_COVERAGE_RATIO = float(os.environ.get("BOOTSTRAP_MIN_CUSIP_COVERAGE_RATIO", "0.50"))


class BootstrapPreconditionError(RuntimeError):
    """Raised when a Phase C / D / E precondition fails.

    Distinct exception type so the orchestrator's error message reads
    "PRECONDITION: {detail}" instead of being mistaken for a
    runtime ingester failure.
    """


# ---------------------------------------------------------------------------
# Provenance — bootstrap_archive_results row existence
# ---------------------------------------------------------------------------


def assert_archive_result_exists(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    stage_key: str,
    archive_name: str,
) -> None:
    """Raise unless ``bootstrap_archive_results`` has a row for this triple.

    Used by C-stages to verify the matching B-stage (archive_name='__job__')
    or upstream C-stage (archive_name=<zip>) ran in the current run.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM bootstrap_archive_results
            WHERE bootstrap_run_id = %s
              AND stage_key = %s
              AND archive_name = %s
            """,
            (bootstrap_run_id, stage_key, archive_name),
        )
        if cur.fetchone() is None:
            raise BootstrapPreconditionError(
                f"PRECONDITION: bootstrap_archive_results row missing for "
                f"run_id={bootstrap_run_id}, stage_key={stage_key!r}, "
                f"archive_name={archive_name!r}; upstream stage did not "
                f"complete in the current bootstrap run."
            )


def assert_stage_succeeded_in_run(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    stage_key: str,
) -> None:
    """Raise unless ``bootstrap_stages`` shows ``status='success'`` for
    the given (run_id, stage_key)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status FROM bootstrap_stages
            WHERE bootstrap_run_id = %s AND stage_key = %s
            """,
            (bootstrap_run_id, stage_key),
        )
        row = cur.fetchone()
        if row is None:
            raise BootstrapPreconditionError(
                f"PRECONDITION: bootstrap_stages row missing for run_id={bootstrap_run_id}, stage_key={stage_key!r}."
            )
        if row[0] != "success":
            raise BootstrapPreconditionError(
                f"PRECONDITION: stage {stage_key!r} status={row[0]!r} in run_id={bootstrap_run_id}; required 'success'."
            )


# ---------------------------------------------------------------------------
# Coverage adequacy — mapped/cohort ratio
# ---------------------------------------------------------------------------


@dataclass
class CoverageRatio:
    mapped: int
    cohort: int

    @property
    def ratio(self) -> float:
        if self.cohort == 0:
            return 0.0
        return self.mapped / self.cohort


def compute_cik_coverage(conn: psycopg.Connection[Any]) -> CoverageRatio:
    """Compute CIK coverage ratio against the producer cohort.

    Producer cohort matches ``daily_cik_refresh`` (verified at
    ``app/workers/scheduler.py:1501``):
    ``is_tradable = TRUE AND exchanges.asset_class = 'us_equity'``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
             WHERE i.is_tradable = TRUE
               AND e.asset_class = 'us_equity'
            """,
        )
        row = cur.fetchone()
        cohort = int(row[0]) if row else 0

        cur.execute(
            """
            SELECT COUNT(*) FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
             WHERE i.is_tradable = TRUE
               AND e.asset_class = 'us_equity'
            """,
        )
        row = cur.fetchone()
        mapped = int(row[0]) if row else 0
    return CoverageRatio(mapped=mapped, cohort=cohort)


def compute_cusip_coverage(conn: psycopg.Connection[Any]) -> CoverageRatio:
    """Compute CUSIP coverage ratio against the producer cohort.

    Producer cohort matches ``backfill_cusip_coverage`` (verified at
    ``app/services/sec_13f_securities_list.py``):
    ``is_tradable = TRUE AND company_name IS NOT NULL AND company_name <> ''``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM instruments
             WHERE is_tradable = TRUE
               AND company_name IS NOT NULL
               AND company_name <> ''
            """,
        )
        row = cur.fetchone()
        cohort = int(row[0]) if row else 0

        cur.execute(
            """
            SELECT COUNT(*) FROM instruments i
              JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cusip'
             WHERE i.is_tradable = TRUE
               AND i.company_name IS NOT NULL
               AND i.company_name <> ''
            """,
        )
        row = cur.fetchone()
        mapped = int(row[0]) if row else 0
    return CoverageRatio(mapped=mapped, cohort=cohort)


def assert_cik_coverage(
    conn: psycopg.Connection[Any],
    *,
    min_ratio: float = DEFAULT_MIN_CIK_COVERAGE_RATIO,
) -> None:
    """Raise unless CIK coverage in the US-equity cohort meets the floor."""
    coverage = compute_cik_coverage(conn)
    if coverage.cohort == 0:
        raise BootstrapPreconditionError(
            "PRECONDITION: CIK cohort is empty (no tradable us_equity instruments); A1 universe_sync may have not run."
        )
    if coverage.ratio < min_ratio:
        raise BootstrapPreconditionError(
            f"PRECONDITION: CIK coverage {coverage.mapped}/{coverage.cohort} "
            f"= {coverage.ratio:.2%} below floor {min_ratio:.0%}; "
            f"daily_cik_refresh has not adequately mapped the current universe."
        )


def assert_cusip_coverage(
    conn: psycopg.Connection[Any],
    *,
    min_ratio: float = DEFAULT_MIN_CUSIP_COVERAGE_RATIO,
) -> None:
    """Raise unless CUSIP coverage in the named-tradable cohort meets the floor."""
    coverage = compute_cusip_coverage(conn)
    if coverage.cohort == 0:
        raise BootstrapPreconditionError(
            "PRECONDITION: CUSIP cohort is empty (no tradable named instruments); A1 universe_sync may have not run."
        )
    if coverage.ratio < min_ratio:
        raise BootstrapPreconditionError(
            f"PRECONDITION: CUSIP coverage {coverage.mapped}/{coverage.cohort} "
            f"= {coverage.ratio:.2%} below floor {min_ratio:.0%}; "
            f"cusip_universe_backfill has not adequately mapped the current universe."
        )


# ---------------------------------------------------------------------------
# Per-stage precondition bundles
# ---------------------------------------------------------------------------


def assert_archives_in_manifest(
    target_dir: Any,
    expected_names: list[str],
    *,
    bootstrap_run_id: int,
) -> None:
    """Raise unless every name in ``expected_names`` is in the current
    run's manifest.

    Catches both stale-archive (prior run's leftover) and partial-set
    (some quarterly archives failed to land) failure modes.
    """
    from app.services.sec_bulk_download import assert_archive_belongs_to_run

    for name in expected_names:
        assert_archive_belongs_to_run(target_dir, name, bootstrap_run_id=bootstrap_run_id)


def assert_c1a_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    bulk_dir: Any | None = None,
) -> None:
    """C1.a (sec_submissions_ingest): B4 invocation + CIK coverage + manifest provenance."""
    assert_archive_result_exists(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="cik_refresh",
        archive_name="__job__",
    )
    assert_cik_coverage(conn)
    if bulk_dir is not None:
        assert_archives_in_manifest(bulk_dir, ["submissions.zip"], bootstrap_run_id=bootstrap_run_id)


def assert_c2_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    bulk_dir: Any | None = None,
) -> None:
    """C2 (sec_companyfacts_ingest): B4 invocation + CIK coverage + manifest provenance."""
    assert_archive_result_exists(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="cik_refresh",
        archive_name="__job__",
    )
    assert_cik_coverage(conn)
    if bulk_dir is not None:
        assert_archives_in_manifest(bulk_dir, ["companyfacts.zip"], bootstrap_run_id=bootstrap_run_id)


def assert_c3_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    bulk_dir: Any | None = None,
    expected_archive_names: list[str] | None = None,
) -> None:
    """C3 (13F): B1 invocation + CUSIP coverage + ALL 4 quarterly archives landed."""
    assert_archive_result_exists(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="cusip_universe_backfill",
        archive_name="__job__",
    )
    assert_cusip_coverage(conn)
    if bulk_dir is not None and expected_archive_names is not None:
        assert_archives_in_manifest(bulk_dir, expected_archive_names, bootstrap_run_id=bootstrap_run_id)


def assert_c4_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    bulk_dir: Any | None = None,
    expected_archive_names: list[str] | None = None,
) -> None:
    """C4 (insider): B4 invocation + CIK coverage + ALL 8 quarterly archives landed."""
    assert_archive_result_exists(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="cik_refresh",
        archive_name="__job__",
    )
    assert_cik_coverage(conn)
    if bulk_dir is not None and expected_archive_names is not None:
        assert_archives_in_manifest(bulk_dir, expected_archive_names, bootstrap_run_id=bootstrap_run_id)


def assert_c5_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    bulk_dir: Any | None = None,
    expected_archive_names: list[str] | None = None,
) -> None:
    """C5 (N-PORT): B1 invocation + CUSIP coverage + ALL 4 quarterly archives landed."""
    assert_archive_result_exists(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="cusip_universe_backfill",
        archive_name="__job__",
    )
    assert_cusip_coverage(conn)
    if bulk_dir is not None and expected_archive_names is not None:
        assert_archives_in_manifest(bulk_dir, expected_archive_names, bootstrap_run_id=bootstrap_run_id)


def assert_c1b_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
) -> None:
    """C1.b: C1.a succeeded in current run AND wrote ≥ 1 row.

    Without the rows-written check, a zero-row C1.a (e.g. universe
    had no SEC-mapped instruments) would pass this gate and let
    C1.b proceed to walk an empty CIK list. Codex review
    BLOCKING for #1020.
    """
    assert_stage_succeeded_in_run(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="sec_submissions_ingest",
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_written FROM bootstrap_archive_results
            WHERE bootstrap_run_id = %s
              AND stage_key = 'sec_submissions_ingest'
              AND archive_name = 'submissions.zip'
            """,
            (bootstrap_run_id,),
        )
        row = cur.fetchone()
        if row is None or int(row[0]) <= 0:
            raise BootstrapPreconditionError(
                f"PRECONDITION: sec_submissions_ingest wrote 0 rows in run_id={bootstrap_run_id}; C1.b cannot proceed."
            )


def assert_d_preconditions(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
) -> None:
    """D1/D2/D3: C1.a + C1.b both succeeded in current run."""
    assert_stage_succeeded_in_run(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="sec_submissions_ingest",
    )
    assert_stage_succeeded_in_run(
        conn,
        bootstrap_run_id=bootstrap_run_id,
        stage_key="sec_submissions_files_walk",
    )


# ---------------------------------------------------------------------------
# Audit-trail writer for B-stage / C-stage invokers
# ---------------------------------------------------------------------------


def record_archive_result(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    stage_key: str,
    archive_name: str,
    rows_written: int,
    rows_skipped: dict[str, int] | None = None,
) -> None:
    """Insert/upsert one row in ``bootstrap_archive_results``.

    Each Phase C ingester calls this AFTER each archive's writes
    commit. Each B-stage wrapper calls it with ``archive_name='__job__'``
    after the underlying scheduler job completes.

    Idempotent on ``(bootstrap_run_id, stage_key, archive_name)`` so a
    re-run within the same orchestrator re-records cleanly.
    """
    import json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bootstrap_archive_results
                (bootstrap_run_id, stage_key, archive_name, rows_written, rows_skipped, completed_at)
            VALUES
                (%(run_id)s, %(stage)s, %(archive)s, %(rows_written)s, %(rows_skipped)s::jsonb, NOW())
            ON CONFLICT (bootstrap_run_id, stage_key, archive_name) DO UPDATE SET
                rows_written = EXCLUDED.rows_written,
                rows_skipped = EXCLUDED.rows_skipped,
                completed_at = EXCLUDED.completed_at
            """,
            {
                "run_id": bootstrap_run_id,
                "stage": stage_key,
                "archive": archive_name,
                "rows_written": rows_written,
                "rows_skipped": json.dumps(rows_skipped or {}),
            },
        )
