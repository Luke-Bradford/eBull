"""Universe-issuer-CIK-driven SC 13D / SC 13G discovery layer (#1233 PR11).

This module activates the dormant SEC Schedule 13D / 13G blockholder
pipeline by walking the universe of US tradable issuer CIKs and asking
EDGAR full-text search (``efts.sec.gov/LATEST/search-index``) for every
SC 13D / SC 13D/A / SC 13G / SC 13G/A filing against each CIK in a
bounded date window. Each hit becomes:

  1. One row in ``sec_filing_manifest`` (``subject_type='blockholder_filer'``,
     ``instrument_id=NULL`` per the table's CHECK constraint) for the
     existing ``sec_manifest_worker`` + ``manifest_parsers/sec_13dg.py``
     to drain.
  2. One row per universe-member sibling instrument in
     ``sec_13dg_discovery_issuer_hint`` so the parser can later
     cross-validate its CUSIP-resolved instrument_id and fall back to
     the hint for single-class issuers with unresolvable CUSIPs.

Cross-references
----------------
- Spec ``docs/superpowers/specs/2026-05-21-pr11-blockholders-activation-design.md``
  §3.1 (discovery responsibilities) + §3.5 (watermark helper + bootstrap stage).
- Hint table schema: ``sql/159_create_sec_13dg_discovery_issuer_hint.sql``.
- Manifest helper contract: ``app/services/sec_manifest.py:194-300``
  (returns ``None``; unconditional ``ON CONFLICT DO UPDATE``).
- Filing-agent defence: ``app/providers/implementations/sec_edgar.py``
  ``KNOWN_FILING_AGENT_CIKS`` — agent CIKs MUST be excluded from both
  the manifest ``cik`` field AND from ``blockholder_filers`` auto-seeding.
- Retention floor: ``app/services/blockholders.py::blockholders_retention_cutoff``
  — ``max(today - 3y, 2024-12-18)`` (SEC XBRL mandate effective date).

Why one file (not split discovery + ingest module)
--------------------------------------------------
The discovery layer is pure HTTP + SELECT + INSERT and does NOT call
the parser. It enqueues manifest rows; the existing
``sec_manifest_worker`` drains them. Keeping discovery in
``sec_13dg_discovery.py`` keeps the load-bearing live module
(``blockholders.py``) focused on parse + write helpers. Mirrors the
shape of the N-CSR discovery introduced under PR8.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Literal

import psycopg

from app.providers.implementations.sec_edgar import (
    KNOWN_FILING_AGENT_CIKS,
    SecFilingsProvider,
    _zero_pad_cik,
)
from app.services.blockholders import (
    _upsert_filer,
    blockholders_retention_cutoff,
)
from app.services.sec_manifest import record_manifest_entry

__all__ = [
    "DiscoveryResult",
]


# Silence "imported but unused" — these symbols are intentionally re-exported
# so future Task 4.3 additions in this module can reach them without a
# fresh edit to the import block. The lint guard in Phase 10 invariant B
# requires the module body to reference ``blockholders_retention_cutoff()``
# explicitly (already true via ``_resolve_discovery_startdt`` below); the
# discovery walker in Task 4.3 will consume the remaining names.
_REEXPORTS_FOR_NEXT_TASK = (
    KNOWN_FILING_AGENT_CIKS,
    SecFilingsProvider,
    _zero_pad_cik,
    _upsert_filer,
    record_manifest_entry,
)


# 7-day safety overlap so a steady-state pass after a short outage still
# re-covers any filings whose ``filed_at`` predates the previous run's
# completion (SEC accepts filings 24/7; ``filed_at`` lags wall-clock by
# up to a business day on amendment chains).
_WATERMARK_SAFETY_OVERLAP_DAYS = 7


def _resolve_discovery_startdt(
    conn: psycopg.Connection[Any],
    *,
    mode: Literal["bootstrap", "steady_state"],
    issuer_cik: str | None = None,
) -> date:
    """Pick the discovery window start, with the 3y floor as the hard ceiling.

    Per spec §3.5:

    * ``mode='bootstrap'`` always returns the floor — the bootstrap
      stage performs the full-cohort 3y scan regardless of any prior
      ingest state.
    * ``mode='steady_state'`` narrows to
      ``MAX(blockholder_filings.filed_at) WHERE issuer_cik = ?`` minus
      a 7-day safety overlap, CLAMPED to the floor so a missing
      watermark (issuer with zero prior 13D/G ingest) does not
      silently shrink coverage. An ``issuer_cik`` of ``None`` is
      defensive — also falls back to the floor.

    Watermark source: the raw chain's own
    ``blockholder_filings.filed_at`` keyed by ``issuer_cik``. We do
    NOT consult ``data_freshness_index`` because DFI for
    ``sec_13d``/``sec_13g`` is keyed by
    ``(subject_type='blockholder_filer', subject_id=filer_cik)`` —
    that grain is filer-side and would not match the per-issuer scan
    PR11's discovery performs (Codex 1b HIGH watermark coherence).
    """
    floor = blockholders_retention_cutoff()
    if mode == "bootstrap":
        return floor
    if issuer_cik is None:
        return floor
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(filed_at)::date
            FROM blockholder_filings
            WHERE issuer_cik = %s
              AND filed_at IS NOT NULL
            """,
            (issuer_cik,),
        )
        row = cur.fetchone()
    watermark = row[0] if row and row[0] else floor
    return max(floor, watermark - timedelta(days=_WATERMARK_SAFETY_OVERLAP_DAYS))


@dataclass(frozen=True)
class DiscoveryResult:
    """Counters returned by :func:`discover_sec_13dg_for_universe`.

    Mirrors spec §3.1 step 6 field-for-field so the surrounding
    scheduler job (``sec_blockholders_discovery_job``) can populate a
    ``JobResult`` payload without translation. Frozen so the result is
    safe to hand to logging / metrics without defensive copies.

    Fields
    ------
    issuers_scanned
        Distinct universe issuer CIKs queried (one search-index call
        per issuer; one issuer CIK can map to multiple instruments
        e.g. GOOG + GOOGL on CIK 1652044).
    accessions_discovered
        Total ``hit._source`` records returned across all pages.
    manifest_rows_inserted
        New ``sec_filing_manifest`` rows written (i.e. accession not
        previously present). The helper ``record_manifest_entry`` uses
        an unconditional ``ON CONFLICT DO UPDATE``; insert-vs-update
        is decided by a ``SELECT 1 FROM sec_filing_manifest WHERE
        accession_number = %s`` pre-check inside the same
        ``conn.transaction()`` block.
    manifest_rows_skipped_existing
        Re-discoveries (accession already present). Bumps to confirm
        idempotency without re-fetching.
    filers_upserted
        Total ``blockholder_filers`` UPSERT invocations. Counts every
        seed call, not just net-new rows; the resolver semantic is
        idempotent on existing rows.
    hints_written
        New ``sec_13dg_discovery_issuer_hint`` rows. Idempotent UPSERT
        per the hint table comment in ``sql/159``; this counter
        increments only on NEW ``(accession_number, instrument_id)``
        pairs (detected via the ``RETURNING (xmax = 0)`` predicate),
        NOT on every UPSERT that refreshed ``discovered_at``.
    rows_skipped_outside_cap
        Accessions returned by efts whose ``file_date`` falls outside
        ``blockholders_retention_cutoff()``. Always ``0`` in normal
        operation because the discovery query is already bounded by
        ``startdt = _resolve_discovery_startdt(...)`` which is itself
        clamped to the cutoff. Surfaced explicitly as a tripwire so a
        future helper drift becomes operator-visible.
    elapsed_seconds
        Wall-clock duration of the whole universe sweep
        (``time.monotonic`` delta), useful for the bootstrap stage's
        runtime budget audit.
    """

    issuers_scanned: int
    accessions_discovered: int
    manifest_rows_inserted: int
    manifest_rows_skipped_existing: int
    filers_upserted: int
    hints_written: int
    rows_skipped_outside_cap: int
    elapsed_seconds: float
