"""Stub seeder for ``ownership_institutions_observations`` (floor 2,000,000).

Phase 0 NEW-C stub per docs/proposals/etl/phase-0-instrumentation.md §2.8.
Not implemented in Phase 0. The first phase whose perf claim needs this
table seeded must implement per the plan below.

Plan (grep-verified schema, sql/114_ownership_institutions_observations.sql):

* **Primary key**: ``(instrument_id, filer_cik, ownership_nature, period_end,
  source_document_id, exposure_kind)``.
* **No FK** to ``instruments`` (observation rows can outlive any
  particular instrument lifecycle).
* **Partitioning**: ``PARTITION BY RANGE (period_end)``; quarterly
  partitions 2010-Q1..2030-Q4 + a DEFAULT partition. Partition routing
  is handled by Postgres on INSERT — the seeder only needs to spread
  ``period_end`` across existing partitions.

Implementation outline (do NOT extrapolate beyond this when implementing):

1. Use sentinel ``instrument_id`` (``>= 1_000_000_000``) via
   :func:`scripts.perf_bench.seed_synthetic_fixture.sentinel_instrument_id`.
2. Generate rows whose ``period_end`` is spread across the last 8 quarters
   so multiple partitions are exercised by EXPLAIN.
3. Direct INSERT via ``COPY``; partition routing is transparent.
4. Required NOT NULL fields per DDL: ``filer_cik``, ``filer_name``,
   ``ownership_nature``, ``source``, ``source_document_id``, ``filed_at``,
   ``period_end``, ``exposure_kind``. Match the CHECK-constrained
   enums (filer_type / ownership_nature / source / voting_authority /
   exposure_kind) — see ``sql/114_*.sql`` for the full set.

**Cross-impact note (writer-safety)**: seeding ``_observations`` will
populate the drifted-set query in
``app/jobs/ownership_observations_repair.py`` for sentinel ids. The
implementing PR MUST also seed ``ownership_refresh_state`` with a
matching ``last_drained_observations_max_ingested_at`` so the
refresh-sweep never wakes up on sentinel rows. Without this paired
write the next refresh-sweep run will attempt to recompute
``ownership_institutions_current`` for every sentinel id and trigger
the ``WHEN NOT MATCHED BY SOURCE ... DELETE`` clause.

Refusals (inherit from :mod:`scripts.perf_bench.seed_synthetic_fixture`):

* ``EBULL_BENCH_DB_URL`` must contain 'bench'; must not contain 'dev' or 'prod'.
* :func:`assert_sentinel_range_clear` must pass.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    raise NotImplementedError(
        "seed_ownership_institutions_observations: not implemented in Phase 0. "
        "See module docstring for the implementation plan; implement when "
        "first downstream perf claim needs this table seeded."
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
