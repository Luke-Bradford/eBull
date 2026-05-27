"""Stub seeder for ``filing_events`` (floor 2,000,000).

Phase 0 NEW-C stub per docs/proposals/etl/phase-0-instrumentation.md §2.8.
Not implemented in Phase 0.

Plan (grep-verified schema, sql/001_init.sql:46):

* **Primary key**: ``filing_event_id`` (BIGSERIAL).
* **Foreign key**: ``instrument_id REFERENCES instruments(instrument_id)``
  — FK fires; sentinel ``instrument_id`` will NOT work here.
* **No partitioning** — single table.
* The schema uses ``filing_date`` (DATE), NOT ``filed_at``.

Implementation outline (do NOT extrapolate):

1. **Real-instrument replication**: pick N real ``instrument_id`` values;
   replicate each ``num_copies`` times with different ``filing_date``
   values spread across a multi-year range.
2. Let DB assign ``filing_event_id`` via BIGSERIAL default.
3. Tag synthetic rows with a distinctive ``event_type`` literal (e.g.
   ``'synthetic_perf_bench'``) for trivially scoped cleanup.
4. Implementation PR MUST grep readers of ``filing_events`` and either
   filter the synthetic ``event_type`` at the reader level or document
   the bench DB as unsafe for any operator query touching filing
   timelines.

**Cross-impact note**: the ``coverage_status_transition_log`` and any
scheduled-job that reads ``filing_events`` for downstream signals
should be inventoried before the implementation PR ships.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    raise NotImplementedError(
        "seed_filing_events: not implemented in Phase 0. See module "
        "docstring; FK to instruments means sentinel strategy doesn't apply."
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
