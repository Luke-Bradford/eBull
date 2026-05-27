"""Stub seeder for ``ownership_funds_observations`` (floor 200,000).

Phase 0 NEW-C stub per docs/proposals/etl/phase-0-instrumentation.md §2.8.
Not implemented in Phase 0.

Plan (grep-verified schema, sql/123_ownership_funds_observations.sql:89):

* **Primary key**: ``(instrument_id, fund_series_id, period_end,
  source_document_id)``.
* **No FK** to ``instruments``.
* **Partitioning**: ``PARTITION BY RANGE (period_end)``; partitions are
  N-PORT-era dominant (2018-2030).

Implementation outline:

1. Sentinel ``instrument_id``.
2. Synthetic ``fund_series_id`` (TEXT, e.g. ``SYNF000000123``).
3. ``payoff_profile = 'Long'`` and ``asset_category = 'EC'`` (both
   CHECK-pinned per DDL — verify at implementation time).
4. Spread ``period_end`` across last 8 N-PORT quarters.

**Cross-impact note (writer-safety)**: same paired refresh-state write
caveat as institutions / insiders.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    raise NotImplementedError(
        "seed_ownership_funds_observations: not implemented in Phase 0. "
        "See module docstring; implement when first downstream perf claim "
        "needs this table seeded."
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
