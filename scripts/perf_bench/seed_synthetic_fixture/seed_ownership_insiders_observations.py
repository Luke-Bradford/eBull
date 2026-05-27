"""Stub seeder for ``ownership_insiders_observations`` (floor 500,000).

Phase 0 NEW-C stub per docs/proposals/etl/phase-0-instrumentation.md §2.8.
Not implemented in Phase 0.

Plan (grep-verified schema, sql/113_ownership_insiders_observations.sql:77):

* **Primary key**: ``(instrument_id, holder_identity_key, ownership_nature,
  source, source_document_id, period_end)``.
* ``holder_identity_key`` is a ``GENERATED ALWAYS`` column derived from
  ``holder_cik`` (when present) else a hash of ``holder_name`` + filer.
  The seeder MUST NOT write to this column; let the DB derive it.
* **No FK** to ``instruments``.
* **Partitioning**: ``PARTITION BY RANGE (period_end)``; quarterly
  2010-Q1..2030-Q4 + DEFAULT.

Implementation outline:

1. Sentinel ``instrument_id`` (``>= 1_000_000_000``).
2. Synthesise ``holder_cik`` (e.g. ``SYNH00000001``) so
   ``holder_identity_key`` deterministically derives from a unique value.
3. ``source`` ∈ Form 3/4/5 allowlist (verify exact set in DDL CHECK
   constraint at implementation time).
4. Spread ``period_end`` across last 8 quarters; route via COPY into
   existing partitions.

**Cross-impact note (writer-safety)**: same as institutions — paired
``ownership_refresh_state`` write is required to suppress refresh-sweep
of sentinel ids.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    raise NotImplementedError(
        "seed_ownership_insiders_observations: not implemented in Phase 0. "
        "See module docstring; implement when first downstream perf claim "
        "needs this table seeded."
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
