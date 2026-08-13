"""Bulk pre-retention tombstone sweep for ``sec_filing_manifest`` (#1686).

~43% of the manifest pending backlog is **pre-retention**: filings older
than their source's retention horizon. For the sources whose parser gates
retention BEFORE the HTTP fetch (Form 4, 13D, 13G), the worker already
tombstones these rows without contacting SEC — but it does so 100-rows-a-
tick through the full per-row parser dispatch, burning the scarce per-tick
budget that fetchable rows need.

This sweep performs the IDENTICAL ``pending -> tombstoned`` transition in
bulk SQL (no HTTP, no parser dispatch), clearing the standing backlog in
minutes and keeping the rolling-cutoff boundary swept daily.

Scope is deliberately narrow — ONLY sources where a ``pending`` row's
out-of-retention status is PROVABLE from ``filed_at`` alone:

  * ``sec_form4`` — ``form4_retention_cutoff()`` (today - 3y), gate at
    ``manifest_parsers/insider_345.py``.
  * ``sec_form5`` — ``form5_retention_cutoff()`` (today - 18 months), same
    parser module (Form 5 18-month pre-fetch cap).
  * ``sec_13d`` / ``sec_13g`` — ``blockholders_retention_cutoff()``
    (``max(today - 3y, 2024-12-18 XML-mandate)``), gate at
    ``manifest_parsers/sec_13dg.py``.
  * ``sec_13f_hr`` — ``thirteen_f_retention_cutoff()`` (#1703). Its parser
    gate is POST-parse on ``period_of_report``
    (``manifest_parsers/sec_13f_hr.py:332``), NOT a pre-fetch ``filed_at``
    gate — but SEC Rule 13f-1(a) (file within 45 days AFTER the quarter end)
    means ``filed_at >= period_of_report`` ALWAYS, so a row with
    ``filed_at < thirteen_f_retention_cutoff()`` necessarily has
    ``period_of_report < cutoff`` ⟹ out-of-retention. Sweeping on the
    period cutoff (the MAXIMUM-safe ``filed_at`` cutoff — any larger value
    could catch an in-retention boundary filing) is therefore a
    provably-safe SUBSET of the parser's period gate: zero false tombstones
    (verified — 0 / 62,374 filings have ``filed_at < period_of_report``).
    This is NOT the 13F-NT period-vs-``filed_at`` hazard (#1639): that
    compares TWO filings' periods across amendments; this bounds ONE
    filing's OWN period. Spec
    ``docs/specs/ingest/2026-06-21-manifest-13f-pre-retention-sweep.md``.

EXCLUDED and why (see spec ``docs/specs/ingest/2026-06-20-manifest-drain-capacity.md``):

  * ``sec_10q`` / ``sec_10k`` / ``sec_form3`` / ``sec_def14a`` — no
    retention gate at all (the parser keeps every fetched row). A bulk
    ``filed_at`` tombstone would destroy fetchable, in-scope rows = silent
    data loss.

Reversible: ``POST /jobs/sec_rebuild/run`` re-pends tombstoned rows.

Concurrency: the live worker selects rows non-locking and only takes a row
``FOR UPDATE`` inside ``transition_status``. ``FOR UPDATE SKIP LOCKED``
below keeps the sweep from blocking on a row the worker is mid-transition
on; the residual race where the sweep tombstones a row the worker already
selected is absorbed by the idempotent ``tombstoned -> tombstoned`` no-op
in ``transition_status`` (#1686).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from app.jobs.job_connection import connect_job
from app.services.sec_manifest import ManifestSource

logger = logging.getLogger(__name__)


def gated_cutoffs() -> dict[ManifestSource, Callable[[], date]]:
    """Per-source pre-fetch retention cutoff resolver.

    The SINGLE source of truth is each source's own chokepoint function
    (never a copied literal). A source appears here IFF a ``pending`` row's
    out-of-retention status is provable from ``filed_at`` alone — either the
    parser has a pre-fetch ``filed_at`` gate (``form4``/``form5``/``13d``/
    ``13g``), or ``filed_at`` provably bounds the parser's period gate
    (``sec_13f_hr``: ``filed_at >= period_of_report`` by SEC Rule 13f-1, so
    the period cutoff doubles as a safe ``filed_at`` cutoff). See module
    docstring for the inclusion/exclusion rule. ``13d`` and ``13g`` share
    the blockholders cutoff; ``form4`` (3y) and ``form5`` (18-month) each
    carry their own insider cutoff; ``sec_13f_hr`` reuses the 8-quarter
    period cutoff (couples to ``THIRTEEN_F_HR_RETENTION_QUARTERS``).

    Imports are LAZY (inside the function) so importing this module never
    forces ``insider_transactions`` cold — that module participates in a
    pre-existing ``insider_transactions <-> manifest_parsers`` import cycle
    that only resolves when ``manifest_parsers`` is loaded first. Resolving
    at call time sidesteps any import-order dependence.
    """
    from app.services.blockholders import blockholders_retention_cutoff
    from app.services.insider_transactions import form4_retention_cutoff, form5_retention_cutoff
    from app.services.institutional_holdings import thirteen_f_retention_cutoff

    return {
        "sec_form4": form4_retention_cutoff,
        "sec_form5": form5_retention_cutoff,
        "sec_13d": blockholders_retention_cutoff,
        "sec_13g": blockholders_retention_cutoff,
        "sec_13f_hr": thirteen_f_retention_cutoff,
    }


# Batch size for the bulk UPDATE loop. Each batch is its own top-level
# transaction (autocommit conn + ``with conn.transaction()``), so a 600k-row
# sweep never holds one giant row-lock set / WAL burst.
_BATCH_SIZE = 5000

# Mirrors the parser's tombstone ``error`` string semantics. The parsers
# write ``"retention floor"``; the bulk path tags itself so an operator
# reading the manifest can tell a bulk-swept row from a parser-tombstoned
# one.
_TOMBSTONE_ERROR = "retention floor (bulk pre-fetch sweep)"


@dataclass(frozen=True)
class PreRetentionSweepSummary:
    """Per-source tombstoned counts + total for one sweep run."""

    by_source: dict[str, int]
    total: int


def sweep_pre_retention(*, database_url: str | None = None, batch_size: int = _BATCH_SIZE) -> PreRetentionSweepSummary:
    """Bulk-tombstone every pre-retention ``pending`` row for the gated sources.

    Opens its OWN autocommit connection (each ``with conn.transaction()``
    is then a real BEGIN/COMMIT pair, so a failure on one batch does not
    roll back earlier batches — the ``financial_facts_retention`` sister
    pattern). The service does not accept a caller conn; it owns the
    lifecycle.

    ``database_url`` (tests, isolated 5433 cluster) takes the raw
    ``psycopg.connect`` path — ``connect_job`` hardcodes
    ``settings.database_url`` and would escape test isolation onto dev
    (#1693 prevention-log).
    """
    by_source: dict[str, int] = {}

    # #1693 — scheduled-job body passes no database_url, so connect_job binds
    # the active job's statement_timeout (ContextVar from _tracked_job); an
    # explicit database_url (tests) takes the raw isolated-cluster path.
    connect_cm = (
        connect_job(autocommit=True) if database_url is None else psycopg.connect(database_url, autocommit=True)
    )
    with connect_cm as conn:
        for source, cutoff_fn in gated_cutoffs().items():
            cutoff = cutoff_fn()
            swept = _sweep_one_source(conn, source=source, cutoff=cutoff, batch_size=batch_size)
            by_source[source] = swept

    total = sum(by_source.values())
    logger.info(
        "manifest_pre_retention_sweep: tombstoned=%d by_source=%s",
        total,
        dict(sorted(by_source.items())),
    )
    return PreRetentionSweepSummary(by_source=by_source, total=total)


def _sweep_one_source(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource,
    cutoff: date,
    batch_size: int,
) -> int:
    """Tombstone all ``pending`` rows of ``source`` with ``filed_at`` (UTC date)
    strictly before ``cutoff``, in committed batches. Returns rows tombstoned.

    Reproduces ``transition_status``'s tombstone column writes exactly:
    ``ingest_status='tombstoned'``, ``error=<reason>``, ``next_retry_at=NULL``,
    ``last_attempted_at=clock_timestamp()``; ``raw_status`` is left untouched
    (no #948 evidence-downgrade concern — the bulk path never writes it).

    ``(filed_at AT TIME ZONE 'UTC')::date < cutoff`` is the swept predicate.
    For the ``filed_at``-gated sources (form4/5/13d/g) it matches the
    parser's pre-fetch ``filed_at.date()`` gate exactly; for ``sec_13f_hr``
    it is a provably-safe SUBSET of the parser's POST-parse
    ``period_of_report`` gate (``filed_at >= period_of_report`` by SEC Rule
    13f-1, so ``filed_at < cutoff`` ⟹ ``period < cutoff``). ``FOR UPDATE
    SKIP LOCKED`` skips a row the live worker is mid-``transition_status``
    on rather than blocking; that row is tombstoned by the worker's own
    gate (idempotent no-op absorbs the double-tombstone).
    """
    total = 0
    while True:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sec_filing_manifest
                   SET ingest_status = 'tombstoned',
                       error = %(error)s,
                       next_retry_at = NULL,
                       last_attempted_at = clock_timestamp()
                 WHERE accession_number IN (
                     SELECT accession_number
                       FROM sec_filing_manifest
                      WHERE source = %(source)s
                        AND ingest_status = 'pending'
                        AND (filed_at AT TIME ZONE 'UTC')::date < %(cutoff)s
                      ORDER BY accession_number
                      LIMIT %(batch)s
                      FOR UPDATE SKIP LOCKED
                 )
                """,
                {
                    "error": _TOMBSTONE_ERROR,
                    "source": source,
                    "cutoff": cutoff,
                    "batch": batch_size,
                },
            )
            updated = cur.rowcount
        total += updated
        if updated < batch_size:
            break
    return total
