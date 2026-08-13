"""Stub seeder for ``sec_filing_manifest`` (floor 1,000,000).

Phase 0 NEW-C stub per docs/proposals/etl/phase-0-instrumentation.md §2.8.
Not implemented in Phase 0.

Plan (grep-verified schema, sql/118_sec_filing_manifest.sql:30):

* **Primary key**: ``accession_number`` (TEXT).
* **Foreign keys**:
  * ``instrument_id REFERENCES instruments(instrument_id) ON DELETE
    CASCADE`` — present only for issuer-scoped rows; institutional-filer
    rows have ``instrument_id`` NULL, so sentinel strategy works for
    institutional rows only.
  * ``amends_accession`` self-FK ``ON DELETE SET NULL`` — leave NULL
    for synthetic rows.
* **CHECK constraints**: ``source`` ∈ allowlist; ``subject_type`` ∈
  allowlist; ``ingest_status`` ∈ allowlist. Verify exact sets at
  implementation time.

Implementation outline (do NOT extrapolate):

1. **Issuer-scoped rows**: use real ``instrument_id`` (FK fires); pair
   with companion cleanup script tagged by a distinctive synthetic
   ``accession_number`` prefix (e.g. ``SYN-...``).
2. **Institutional-filer-scoped rows**: ``instrument_id`` NULL;
   ``filer_cik`` synthetic (e.g. ``SYN`` + sequence). Sentinel
   approach effectively applies via the synthetic ``accession_number``
   namespace rather than ``instrument_id``.
3. Synthetic ``accession_number`` shape: ``'SYN-' || generate_series``
   so cleanup is ``DELETE WHERE accession_number LIKE 'SYN-%'``.
4. Implementation PR MUST grep readers of ``sec_filing_manifest`` to
   confirm none silently aggregate without a source filter — synthetic
   rows tagged ``source = 'synthetic'`` (add to CHECK allowlist? or
   reuse 'derived'?) so callers can exclude.

**Cross-impact note**: the SEC manifest worker reads this table; any
synthetic row whose ``ingest_status = 'pending'`` will be picked up by
the worker on the next tick. Synthetic rows MUST land with
``ingest_status = 'success'`` or equivalent terminal state.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    raise NotImplementedError(
        "seed_sec_filing_manifest: not implemented in Phase 0. See module "
        "docstring; mixed sentinel + real-id strategy required."
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
