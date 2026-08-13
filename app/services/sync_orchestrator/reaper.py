"""Boot-time orphaned-sync reaper.

Runs before the scheduler starts. Transitions `sync_runs` rows stuck
in `status='running'` longer than `timeout` to `status='failed'` with
`error_category='orchestrator_crash'` so the partial unique index
gate releases and the next sync can start.

Also recomputes aggregate counts (layers_done/failed/skipped) from the
authoritative `sync_layer_progress` rows so `GET /sync/runs` shows
truthful numbers for crash-reaped runs.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import psycopg

from app.config import settings

logger = logging.getLogger(__name__)


def reap_orphaned_syncs(
    timeout: timedelta = timedelta(hours=1),
    *,
    reap_all: bool = False,
) -> int:
    """Transition stale-running sync_runs rows to failed; clean up their
    unfinished sync_layer_progress rows; recompute aggregate counts.

    When ``reap_all=True`` the ``timeout`` predicate is bypassed and
    EVERY ``status='running'`` row is reaped regardless of age. This is
    the correct choice at lifespan startup, where the orchestrator runs
    in-process and any running row must be from a prior dead process —
    the age-based predicate can miss same-clock-tick rows when ``timeout``
    collapses to zero.

    Returns count of sync_runs rows reaped.
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            # Step 1: reap sync_runs + cascade-fail their pending/running
            # progress rows. CTE returns reaped sync_run_ids for step 2.
            # The `OR %(reap_all)s` branch reaps unconditionally when the
            # caller explicitly opts in — avoids the <=/< boundary bug
            # of a timedelta=0 age check.
            reaped_rows = conn.execute(
                """
                UPDATE sync_runs
                SET status = 'failed',
                    finished_at = now(),
                    error_category = 'orchestrator_crash'
                WHERE status = 'running'
                  AND (%(reap_all)s OR started_at < now() - %(timeout)s::interval)
                RETURNING sync_run_id
                """,
                {"timeout": timeout, "reap_all": reap_all},
            ).fetchall()
            reaped_ids = [r[0] for r in reaped_rows]

            # #645 split — two SEPARATE statements (NOT a single WITH
            # with both updates). PostgreSQL CTE semantics let
            # concurrent data-modifying CTEs see the same row snapshot,
            # so a single WITH containing both the cancel and the fail
            # UPDATE would race: the fail CTE could clobber rows the
            # cancel CTE just transitioned. Sequencing them inside the
            # transaction guarantees the fail UPDATE only sees rows
            # the cancel UPDATE left behind.
            if reaped_ids:
                # Step A: never-started pending rows become 'cancelled'
                # so the consecutive-failure streak in the admin banner
                # is not inflated by reaper noise (uvicorn --reload
                # cycles during dev iteration were the dominant source).
                conn.execute(
                    """
                    UPDATE sync_layer_progress
                    SET status      = 'cancelled',
                        finished_at = now(),
                        skip_reason = 'worker died before adapter dispatched'
                    WHERE sync_run_id = ANY(%s)
                      AND status      = 'pending'
                      AND started_at IS NULL
                    """,
                    (reaped_ids,),
                )
                # Step B: any remaining pending/running rows had
                # `started_at` populated, meaning the adapter actually
                # began work — those are real mid-flight failures.
                conn.execute(
                    """
                    UPDATE sync_layer_progress
                    SET status         = 'failed',
                        finished_at    = now(),
                        error_category = 'orchestrator_crash'
                    WHERE sync_run_id = ANY(%s)
                      AND status IN ('pending', 'running')
                    """,
                    (reaped_ids,),
                )

            # Step 2: recompute aggregate counts. Inside the SAME
            # transaction so status='failed' and counts either both
            # land or neither does — the non-atomic split in the prior
            # draft left sync_runs marked failed with stale pre-crash
            # layers_done/failed/skipped if the process died between
            # the two statements.
            if reaped_ids:
                conn.execute(
                    """
                    UPDATE sync_runs sr
                    SET layers_done    = agg.done,
                        layers_failed  = agg.failed,
                        layers_skipped = agg.skipped
                    FROM (
                        SELECT sync_run_id,
                               COUNT(*) FILTER (WHERE status IN ('complete','partial')) AS done,
                               COUNT(*) FILTER (WHERE status = 'failed')                AS failed,
                               -- `cancelled` (#645 reaper split) rolled
                               -- into the skipped bucket so layers_done +
                               -- layers_failed + layers_skipped continues
                               -- to equal the row count for the run.
                               COUNT(*) FILTER (WHERE status IN ('skipped','cancelled')) AS skipped
                        FROM sync_layer_progress
                        WHERE sync_run_id = ANY(%s)
                        GROUP BY sync_run_id
                    ) agg
                    WHERE sr.sync_run_id = agg.sync_run_id
                    """,
                    (reaped_ids,),
                )

    count = len(reaped_ids)
    if count:
        logger.warning(
            "orchestrator reaper: transitioned %d stale 'running' sync_runs row(s) "
            "to 'failed' (error_category=orchestrator_crash)",
            count,
        )
    return count
