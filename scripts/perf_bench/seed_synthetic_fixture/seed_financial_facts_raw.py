"""Stub seeder for ``financial_facts_raw`` (floor 10,000,000).

Phase 0 NEW-C stub per docs/proposals/etl/phase-0-instrumentation.md §2.8.
Not implemented in Phase 0. Likely first needed by Phase 4 (S22 work).

Plan (grep-verified schema, sql/156_financial_facts_raw.sql:22):

* **Primary key**: ``(fact_id, period_end)``; ``fact_id BIGSERIAL DEFAULT
  nextval(...)``.
* **Foreign keys**:
  * ``instrument_id REFERENCES instruments(instrument_id)`` — FK fires
    on INSERT, so sentinel ``instrument_id >= 1_000_000_000`` will NOT
    work here.
  * ``ingestion_run_id REFERENCES data_ingestion_runs(ingestion_run_id)`` —
    requires a pre-existing ``data_ingestion_runs`` row, or the seeder
    must insert one first.
* **Partitioning**: ``PARTITION BY RANGE (period_end)``; quarterly
  2010-Q1..2030-Q4 + ``pre2010`` + DEFAULT.

Implementation outline (do NOT extrapolate):

1. **Real-instrument replication strategy** (sentinel approach broken
   by FK): pick N real ``instrument_id`` values that operator confirms
   are safe to attach synthetic facts to (e.g. the perf panel
   AAPL/GME/MSFT/JPM/HD); replicate each ``num_copies`` times with
   different ``period_end`` values so partitions are exercised.
2. Insert a single ``data_ingestion_runs`` parent row tagged
   ``run_label = 'perf_bench_synthetic'`` so the synthetic facts are
   trivially identifiable + bulk-deletable post-bench.
3. Let DB assign ``fact_id`` via BIGSERIAL default.
4. **MUST** document a paired cleanup path: the data attaches to real
   instruments, so the implementation PR also lands a
   ``delete_synthetic_facts.py`` companion that DELETEs by
   ``ingestion_run_id``.

**Cross-impact note**: any code path that aggregates from
``financial_facts_raw`` (rollups, fundamentals computations, screen
queries) will see the synthetic rows. Implementation PR must grep all
readers and either (a) filter out the synthetic ``ingestion_run_id`` at
the reader level, or (b) document that the bench DB is unsafe for any
operator query touching fundamentals.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    raise NotImplementedError(
        "seed_financial_facts_raw: not implemented in Phase 0. See module "
        "docstring; FK to instruments means sentinel strategy doesn't apply "
        "— see plan."
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
