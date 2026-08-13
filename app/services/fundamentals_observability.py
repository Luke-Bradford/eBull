"""Admin-UI observability helpers for the SEC fundamentals ingest
pipeline (#414, #418).

Two surfaces:

- ``get_cik_timing_summary(conn)`` — p50/p95/count per mode
  (seed/refresh) for the most recent ingestion run, plus the top-5
  slowest CIKs. Answers "did the ADR 0004 Shape-B bench ratios hold
  in prod?" without tailing logs.
- ``get_seed_progress(conn)`` — submissions/master-index seed ratios
  (N of M CIKs watermarked), latest run state, operator pause flag.
  Answers "how far through the seed are we, ETA, and is ingest paused?"

Both are read-only helpers that accept an externally-managed
connection so the caller (the HTTP handler) controls transaction
boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg

TimingMode = Literal["seed", "refresh"]


@dataclass(frozen=True)
class CikTimingPercentiles:
    mode: TimingMode
    count: int
    p50_seconds: float | None
    p95_seconds: float | None
    max_seconds: float | None
    facts_upserted_total: int


@dataclass(frozen=True)
class SlowCikEntry:
    cik: str
    mode: TimingMode
    seconds: float
    facts_upserted: int
    outcome: str
    finished_at: datetime


@dataclass(frozen=True)
class CikTimingSummary:
    ingestion_run_id: int | None
    run_source: str | None
    run_started_at: datetime | None
    run_finished_at: datetime | None
    run_status: str | None
    modes: list[CikTimingPercentiles]
    slowest: list[SlowCikEntry]


def get_cik_timing_summary(conn: psycopg.Connection[Any]) -> CikTimingSummary:
    """Return timing percentiles for the most recent ingestion run.

    If no timing rows exist yet (pre-#418 deploy or empty table),
    returns an empty summary with ``ingestion_run_id=None`` — the
    caller renders "no data yet" rather than a 500.
    """
    latest = conn.execute(
        """
        SELECT ingestion_run_id
        FROM cik_upsert_timing
        WHERE ingestion_run_id IS NOT NULL
        ORDER BY ingestion_run_id DESC
        LIMIT 1
        """
    ).fetchone()

    if latest is None:
        return CikTimingSummary(
            ingestion_run_id=None,
            run_source=None,
            run_started_at=None,
            run_finished_at=None,
            run_status=None,
            modes=[],
            slowest=[],
        )

    run_id = int(latest[0])

    run_row = conn.execute(
        """
        SELECT source, started_at, finished_at, status
        FROM data_ingestion_runs
        WHERE ingestion_run_id = %s
        """,
        (run_id,),
    ).fetchone()

    # percentile_cont returns DOUBLE PRECISION; cast the NUMERIC(10,3)
    # ``seconds`` column to float on the Python side so the JSON
    # response is a plain number, not a Decimal.
    rows = conn.execute(
        """
        SELECT mode,
               COUNT(*)                                                AS n,
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY seconds)   AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds)   AS p95,
               MAX(seconds)                                            AS mx,
               SUM(facts_upserted)                                     AS total_facts
        FROM cik_upsert_timing
        WHERE ingestion_run_id = %s
        GROUP BY mode
        ORDER BY mode
        """,
        (run_id,),
    ).fetchall()

    modes: list[CikTimingPercentiles] = []
    for r in rows:
        mode_str = str(r[0])
        if mode_str not in ("seed", "refresh"):
            continue
        modes.append(
            CikTimingPercentiles(
                mode=mode_str,  # type: ignore[arg-type]
                count=int(r[1]),
                p50_seconds=float(r[2]) if r[2] is not None else None,
                p95_seconds=float(r[3]) if r[3] is not None else None,
                max_seconds=float(r[4]) if r[4] is not None else None,
                facts_upserted_total=int(r[5] or 0),
            )
        )

    slow_rows = conn.execute(
        """
        SELECT cik, mode, seconds, facts_upserted, outcome, finished_at
        FROM cik_upsert_timing
        WHERE ingestion_run_id = %s
        ORDER BY seconds DESC
        LIMIT 5
        """,
        (run_id,),
    ).fetchall()

    slowest: list[SlowCikEntry] = []
    for r in slow_rows:
        mode_str = str(r[1])
        if mode_str not in ("seed", "refresh"):
            continue
        slowest.append(
            SlowCikEntry(
                cik=str(r[0]),
                mode=mode_str,  # type: ignore[arg-type]
                seconds=float(r[2]),
                facts_upserted=int(r[3]),
                outcome=str(r[4]),
                finished_at=r[5],
            )
        )

    return CikTimingSummary(
        ingestion_run_id=run_id,
        run_source=str(run_row[0]) if run_row else None,
        run_started_at=run_row[1] if run_row else None,
        run_finished_at=run_row[2] if run_row else None,
        run_status=str(run_row[3]) if run_row else None,
        modes=modes,
        slowest=slowest,
    )


@dataclass(frozen=True)
class SeedSourceProgress:
    source: str
    key_description: str
    seeded: int
    total: int


@dataclass(frozen=True)
class LatestIngestionRun:
    ingestion_run_id: int
    source: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    rows_upserted: int
    rows_skipped: int


@dataclass(frozen=True)
class SeedProgressSummary:
    sources: list[SeedSourceProgress]
    latest_run: LatestIngestionRun | None
    ingest_paused: bool


# Bound the total-CIK count to the universe of tradable instruments
# that have a primary SEC CIK mapping. Matches the phase-2/phase-1b
# audit query so the denominator cannot silently drift between the
# scheduler and the admin UI.
_TOTAL_CIKS_SQL = """
    SELECT COUNT(*)
    FROM instruments i
    JOIN external_identifiers ei
        ON ei.instrument_id = i.instrument_id
       AND ei.provider = 'sec'
       AND ei.identifier_type = 'cik'
       AND ei.is_primary = TRUE
    WHERE i.is_tradable = TRUE
"""


def get_seed_progress(conn: psycopg.Connection[Any]) -> SeedProgressSummary:
    """Return seed progress ratios + latest run state + pause flag.

    ``total`` is the count of CIK-mapped tradable instruments. A
    per-source watermark exists once that CIK has been successfully
    ingested, so ``seeded / total`` is the literal seed-progress
    ratio the operator expects.
    """
    total_row = conn.execute(_TOTAL_CIKS_SQL).fetchone()
    total = int(total_row[0]) if total_row is not None else 0

    # Two watermark keys drive SEC XBRL ingest: sec.submissions
    # (per-CIK top-accession) and sec.master-index (per-day crawl).
    # Only sec.submissions is per-CIK — master-index is per-day, so
    # reporting it as a seed ratio would be misleading. We surface
    # sec.submissions as the canonical progress bar.
    #
    # ``seeded`` must be scoped to the SAME cohort as ``total``
    # (primary SEC-mapped tradable instruments). Counting every
    # sec.submissions watermark would over-report completion when a
    # previously-seeded CIK falls out of the tradable universe —
    # worst case, seeded > total and the progress bar exceeds 100%.
    seeded_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM external_data_watermarks w
        JOIN external_identifiers ei
            ON ei.provider = 'sec'
           AND ei.identifier_type = 'cik'
           AND ei.is_primary = TRUE
           AND ei.identifier_value = w.key
        JOIN instruments i
            ON i.instrument_id = ei.instrument_id
           AND i.is_tradable = TRUE
        WHERE w.source = 'sec.submissions'
        """
    ).fetchone()
    seeded_submissions = int(seeded_row[0]) if seeded_row is not None else 0

    sources = [
        SeedSourceProgress(
            source="sec.submissions",
            key_description="SEC submissions.json (per-CIK top accession)",
            seeded=seeded_submissions,
            total=total,
        ),
    ]

    latest_row = conn.execute(
        """
        SELECT ingestion_run_id, source, started_at, finished_at, status,
               COALESCE(rows_upserted, 0), COALESCE(rows_skipped, 0)
        FROM data_ingestion_runs
        WHERE source = 'sec_edgar'
        ORDER BY ingestion_run_id DESC
        LIMIT 1
        """
    ).fetchone()

    latest_run: LatestIngestionRun | None = None
    if latest_row is not None:
        latest_run = LatestIngestionRun(
            ingestion_run_id=int(latest_row[0]),
            source=str(latest_row[1]),
            started_at=latest_row[2],
            finished_at=latest_row[3],
            status=str(latest_row[4]),
            rows_upserted=int(latest_row[5]),
            rows_skipped=int(latest_row[6]),
        )

    # Reuse the existing layer_enabled table — ``fundamentals_ingest``
    # is the runtime toggle landed in #421. Absent row counts as
    # enabled; False means the operator paused the scheduled job.
    paused_row = conn.execute(
        "SELECT is_enabled FROM layer_enabled WHERE layer_name = 'fundamentals_ingest'"
    ).fetchone()
    ingest_paused = paused_row is not None and not bool(paused_row[0])

    return SeedProgressSummary(
        sources=sources,
        latest_run=latest_run,
        ingest_paused=ingest_paused,
    )
