"""One-shot retroactive cleanup of skip-tier ``filing_events`` rows (#1013).

`#1011`/`#1012` introduced the SEC three-tier form allow-list
(``SEC_INGEST_KEEP_FORMS`` in :mod:`app.services.filings`). New ingest
filters to that set, but existing DBs still hold pre-allow-list skip-tier
rows. This service deletes them in bounded batches.

Scope guards (correctness, not cosmetic):

* ``provider = 'sec'`` — the allow-list is SEC-specific. Other providers
  (e.g. Companies House) MUST NOT be judged against SEC form names.
* ``filing_type IS NOT NULL`` — never delete a row we cannot classify.

``filing_documents`` (sql/062, ``ON DELETE CASCADE``) is the only FK child
of ``filing_events`` and is removed automatically. ``filing_raw_documents``
is accession-keyed with no ``filing_event_id`` FK and is NOT touched;
skip-tier accessions have no raw bodies anyway (no parser → nothing stored).

Connection ownership (prevention-log §"orchestrator-of-N autocommit"):
this is an orchestrator of independent batch units, so it OWNS its
connection — opens ``autocommit=True`` and wraps each batch in
``with conn.transaction()`` (a real top-level BEGIN/COMMIT per batch).
Bounded batches keep WAL bursts and lock churn small over the ~189k-row
delete. Mirror of
:func:`app.services.financial_facts_retention.sweep_retention_all_instruments`.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field

import psycopg

from app.config import settings
from app.services.filings import SEC_INGEST_KEEP_FORMS

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 5000


@dataclass(frozen=True)
class SkipTierCleanupSummary:
    """Outcome of one cleanup run."""

    total_deleted: int
    batches: int
    # Exact count of rows actually deleted, keyed by filing_type.
    by_form_type: dict[str, int] = field(default_factory=dict)


# Bounded delete: page the skip-tier candidates by PK (deterministic /
# auditable progress) and delete that page, returning the filing_type of
# each removed row so the caller can tally exactly what landed. The
# candidate set strictly shrinks each batch (deleted rows can't re-match),
# so the loop terminates without an explicit iteration cap.
_DELETE_BATCH_SQL = """
DELETE FROM filing_events
WHERE filing_event_id IN (
    SELECT filing_event_id
    FROM filing_events
    WHERE provider = 'sec'
      AND filing_type IS NOT NULL
      AND filing_type <> ALL(%(keep)s::text[])
    ORDER BY filing_event_id
    LIMIT %(batch)s
)
RETURNING filing_type
"""


def cleanup_skip_tier_filing_events(
    *,
    database_url: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> SkipTierCleanupSummary:
    """Delete SEC ``filing_events`` rows whose ``filing_type`` is not in
    ``SEC_INGEST_KEEP_FORMS``, in bounded batches.

    Idempotent: a re-run after a full drain selects zero candidates and
    returns ``total_deleted=0``.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    url = database_url or settings.database_url
    keep_forms = sorted(SEC_INGEST_KEEP_FORMS)

    tally: Counter[str] = Counter()
    total_deleted = 0
    batches = 0

    with psycopg.connect(url, autocommit=True) as conn:
        while True:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        _DELETE_BATCH_SQL,
                        {"keep": keep_forms, "batch": batch_size},
                    )
                    deleted = cur.fetchall()
            if not deleted:
                break
            batches += 1
            total_deleted += len(deleted)
            for (filing_type,) in deleted:
                tally[filing_type] += 1

    logger.info(
        "filing_events_skip_tier_cleanup: deleted=%d batches=%d by_form_type=%s",
        total_deleted,
        batches,
        dict(sorted(tally.items(), key=lambda kv: kv[1], reverse=True)),
    )
    return SkipTierCleanupSummary(
        total_deleted=total_deleted,
        batches=batches,
        by_form_type=dict(tally),
    )
