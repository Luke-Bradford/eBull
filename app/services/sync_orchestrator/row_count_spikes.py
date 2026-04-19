"""Row-count spike detection for scheduled jobs.

Moved from app/services/ops_monitor.py in chunk 7 so the orchestrator
owns its own spike logic and ops_monitor can shrink to audit writers
+ kill-switch machinery only.

Signature and semantics are preserved exactly — this is a relocation,
not a rewrite. Callers:
  - app/workers/scheduler.py inside _tracked_job's success branch.
  - tests/test_ops_monitor.py (to be migrated to this module in a
    later cleanup PR).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# Minimum expected row count ratio.  If a job run produces fewer than
# (previous_count * threshold), it is flagged as a potential broken source.
_SPIKE_RATIO_THRESHOLD: float = 0.5


@dataclass(frozen=True)
class SpikeResult:
    job_name: str
    flagged: bool
    current_count: int
    previous_count: int | None = None
    detail: str = ""


def check_row_count_spike(
    conn: psycopg.Connection[Any],
    job_name: str,
    current_count: int,
    *,
    exclude_run_id: int | None = None,
) -> SpikeResult:
    """
    Compare current_count against the previous successful run's row_count.

    Flags when current_count < previous_count * _SPIKE_RATIO_THRESHOLD.
    This detects broken data sources that silently return fewer rows than
    expected.

    exclude_run_id: if provided, excludes this run from the comparison query
    so the current run does not compare against itself.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT row_count
            FROM job_runs
            WHERE job_name = %(name)s
              AND status = 'success'
              AND row_count IS NOT NULL
              AND (%(exclude_id)s IS NULL OR run_id != %(exclude_id)s)
            ORDER BY started_at DESC
            LIMIT 1
            """,
            {"name": job_name, "exclude_id": exclude_run_id},
        )
        row = cur.fetchone()

    if row is None or row["row_count"] is None:
        # No prior successful run with a row count — nothing to compare.
        return SpikeResult(
            job_name=job_name,
            flagged=False,
            current_count=current_count,
            detail=f"{job_name}: no prior row_count to compare",
        )

    previous_count: int = int(row["row_count"])
    if previous_count == 0:
        # Previous run also had zero rows — not a spike.
        return SpikeResult(
            job_name=job_name,
            flagged=False,
            current_count=current_count,
            previous_count=previous_count,
            detail=f"{job_name}: previous count was 0, skip comparison",
        )

    ratio = current_count / previous_count
    if ratio < _SPIKE_RATIO_THRESHOLD:
        return SpikeResult(
            job_name=job_name,
            flagged=True,
            current_count=current_count,
            previous_count=previous_count,
            detail=(
                f"{job_name}: row_count dropped from {previous_count} to "
                f"{current_count} (ratio={ratio:.2f} < threshold={_SPIKE_RATIO_THRESHOLD})"
            ),
        )

    return SpikeResult(
        job_name=job_name,
        flagged=False,
        current_count=current_count,
        previous_count=previous_count,
    )
