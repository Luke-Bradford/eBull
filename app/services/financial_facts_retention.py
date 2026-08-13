"""Retention sweep for ``financial_facts_raw`` (#1208 Sub 3).

Enforces the per-source horizons documented at
`.claude/skills/data-engineer/SKILL.md` §13:

- 10-K family (10-K, 10-K/A): keep the latest 3 distinct accessions per
  instrument.
- 10-Q family (10-Q, 10-Q/A): keep the latest 8 distinct accessions per
  instrument.

Other form_types (8-K, DEF 14A, 13F-HR, ...) are NOT swept here — each
source has its own discovery-layer horizon.

Service-no-commit invariant (Phase 1 lesson, prevention-log §"psycopg3
service-no-commit invariant"): :func:`sweep_retention_for_instrument`
takes a connection and does NOT enter its own ``with conn.transaction()``
block. The orchestrator :func:`sweep_retention_all_instruments` owns
transaction boundaries; the per-instrument call runs inside the
orchestrator's tx so its DELETE is committed when the tx exits.

Autocommit is mandatory for the orchestrator (Codex 1b BLOCKING #2):
in default-mode psycopg conns the initial SELECT opens an implicit tx,
making every subsequent ``with conn.transaction()`` a savepoint rather
than a fresh top-level tx. The 12k-instrument loop would commit exactly
once, on function exit, defeating per-instrument WAL bounding. Hence
the orchestrator opens an autocommit connection — ``connect_job(autocommit=True)``
on the scheduled path (#1693: the active job's ``statement_timeout`` binds it),
or a raw ``psycopg.connect(database_url, autocommit=True)`` when a caller passes
an explicit ``database_url`` (tests / isolated cluster).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

from app.jobs.job_connection import connect_job

logger = logging.getLogger(__name__)


KEEP_10K = 3
KEEP_10Q = 8

ANNUAL_FORMS = ("10-K", "10-K/A")
QUARTERLY_FORMS = ("10-Q", "10-Q/A")
SWEPT_FORMS = ANNUAL_FORMS + QUARTERLY_FORMS


@dataclass(frozen=True)
class RetentionSummary:
    """Outcome of one orchestrator run."""

    instruments: int
    rows_deleted: int


_DELETE_SQL = """
WITH distinct_accessions AS (
    -- One row per (instrument, accession). GROUP BY collapses duplicate
    -- metadata rows for the same accession to a single ranking entry.
    -- MAX(filed_date) picks the latest reported filed_date if a parser
    -- bug ever emitted divergent values for the same accession.
    SELECT instrument_id,
           accession_number,
           MAX(filed_date) AS filed_date,
           MAX(
               CASE WHEN form_type IN ('10-K', '10-K/A') THEN 'ANNUAL'
                    WHEN form_type IN ('10-Q', '10-Q/A') THEN 'QUARTERLY'
               END
           ) AS family
    FROM financial_facts_raw
    WHERE instrument_id = %(iid)s
      AND form_type IN ('10-K', '10-K/A', '10-Q', '10-Q/A')
    GROUP BY instrument_id, accession_number
),
ranked AS (
    SELECT instrument_id, accession_number, family,
           ROW_NUMBER() OVER (
             PARTITION BY instrument_id, family
             ORDER BY filed_date DESC, accession_number DESC
           ) AS rn
    FROM distinct_accessions
),
to_evict AS (
    SELECT instrument_id, accession_number
    FROM ranked
    WHERE (family = 'ANNUAL'    AND rn > %(keep_10k)s)
       OR (family = 'QUARTERLY' AND rn > %(keep_10q)s)
)
DELETE FROM financial_facts_raw f
USING to_evict e
WHERE f.instrument_id    = e.instrument_id
  AND f.accession_number = e.accession_number
"""


def sweep_retention_for_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    keep_10k: int = KEEP_10K,
    keep_10q: int = KEEP_10Q,
) -> int:
    """Delete ``financial_facts_raw`` rows whose accession is outside the
    per-family retention horizon for ``instrument_id``.

    Returns the deleted row count.

    Service-no-commit: caller owns the transaction. Do not enter
    ``with conn.transaction()`` inside this function — it would become
    a SAVEPOINT in the caller's tx, masking failures and breaking the
    "commit per instrument" promise the orchestrator depends on.
    """
    with conn.cursor() as cur:
        cur.execute(
            _DELETE_SQL,
            {
                "iid": instrument_id,
                "keep_10k": keep_10k,
                "keep_10q": keep_10q,
            },
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


def sweep_retention_all_instruments(
    *,
    database_url: str | None = None,
    keep_10k: int = KEEP_10K,
    keep_10q: int = KEEP_10Q,
) -> RetentionSummary:
    """Iterate every instrument with swept-family facts, sweep each in
    its own real top-level transaction.

    ``autocommit=True`` on the conn is non-negotiable — see module
    docstring. Each ``with conn.transaction()`` then issues a fresh
    BEGIN/COMMIT pair so a failure on instrument N does not roll back
    the work done for instruments 1..N-1.
    """
    total_deleted = 0
    iids: list[int] = []

    # #1693 — the scheduled-job body (financial_facts_retention_sweep) passes no
    # database_url, so connect_job binds the active job's statement_timeout
    # (ContextVar set by _tracked_job): a wedged sweep statement self-aborts
    # instead of stranding the job_runs row 'running' (#1689 mode). An explicit
    # database_url (tests, isolated 5433 cluster) takes the raw path — connect_job
    # hardcodes settings.database_url and would escape test isolation onto dev.
    connect_cm = (
        connect_job(autocommit=True) if database_url is None else psycopg.connect(database_url, autocommit=True)
    )
    with connect_cm as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT instrument_id FROM financial_facts_raw "
                "WHERE form_type IN ('10-K', '10-K/A', '10-Q', '10-Q/A')"
            )
            iids = [row[0] for row in cur.fetchall()]

        for iid in iids:
            with conn.transaction():
                deleted = sweep_retention_for_instrument(
                    conn,
                    instrument_id=iid,
                    keep_10k=keep_10k,
                    keep_10q=keep_10q,
                )
                total_deleted += deleted

    logger.info(
        "financial_facts_retention_sweep: instruments=%d rows_deleted=%d",
        len(iids),
        total_deleted,
    )
    return RetentionSummary(instruments=len(iids), rows_deleted=total_deleted)
