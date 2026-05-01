"""One-shot ``financial_periods`` re-projection (#735).

Re-derives every issuer's canonical ``financial_periods`` row from
``financial_facts_raw`` so newly-added TRACKED_CONCEPTS (treasury_shares,
shares_authorized, shares_issued, retained_earnings — migration 088 /
issue #731) project onto the canonical table for issuers whose facts
were ingested *before* the alias map carried those concepts.

Why a backfill is needed: ``app.workers.scheduler`` runs
``normalize_financial_periods`` only on instruments whose facts changed
that day (``touched_ciks``). Issuers whose raw facts pre-date migration
088 already have their canonical rows; the daily pass skips them; the
new columns stay NULL until the next time SEC ships a fresh filing for
that issuer (which, for slow-filers, can be a quarter+ away). Without
this backfill, the ownership reporting card (#729) renders treasury as
"not on file" for almost every ticker.

Idempotent: ``normalize_financial_periods`` upserts via ON CONFLICT DO
UPDATE WHERE IS DISTINCT FROM, so re-running over already-projected
rows is a no-op for unchanged values. Safe to run repeatedly.

Bootstrap-only: this is intended as a once-per-install / once-per-new-
TRACKED_CONCEPTS-migration operation. Not a nightly job. The daily
cadence handles new data.

Run from repo root:

    uv run python -m scripts.backfill_xbrl_normalization
    uv run python -m scripts.backfill_xbrl_normalization --apply
    uv run python -m scripts.backfill_xbrl_normalization --apply --limit 100

Defaults to dry-run (counts the affected cohort, no writes). The
``--limit`` flag caps the cohort for sample / staging runs.

Rate limit: no SEC HTTP calls — this is pure DB-side normalization.
Wall clock dominated by per-instrument transaction overhead;
~50-200 ms / instrument depending on facts cardinality.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import psycopg

from app.config import settings
from app.services.fundamentals import normalize_financial_periods

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# Migration-088 SEC us-gaap concepts that need re-projection. Mirrors
# the ``TRACKED_CONCEPTS`` aliases in
# ``app.providers.implementations.sec_fundamentals`` for the four
# columns added in ``sql/088_xbrl_ownership_columns.sql``. Kept here as
# a literal so the cohort SELECT can run without importing the provider
# module (which pulls in HTTP client deps).
MIGRATION_088_CONCEPTS: tuple[str, ...] = (
    "TreasuryStockShares",
    "TreasuryStockCommonShares",
    "CommonStockSharesAuthorized",
    "CommonStockSharesIssued",
    "RetainedEarningsAccumulatedDeficit",
)


def select_cohort(
    conn: psycopg.Connection[tuple],
    *,
    only_unprojected_088: bool,
    limit: int | None,
) -> list[int]:
    """Return instrument_ids that need re-projection.

    When ``only_unprojected_088`` is True (default), narrow to issuers
    that have raw facts for ANY of the four migration-088 concepts and
    still have at least one of the four target canonical columns NULL.
    This is the minimum cohort that materially benefits from the
    backfill — full no-op runs still update zero rows but the
    per-instrument tx overhead adds up across 5000+ issuers.

    The first cohort definition (PR review, codex high) only checked
    treasury_shares and missed issuers that needed shares_authorized /
    shares_issued / retained_earnings projection but had no treasury
    facts at all. Broadened here to track all four migration-088
    columns.

    When ``only_unprojected_088`` is False (``--all-instruments``),
    process every instrument with any raw facts. Use this after adding
    NEW TRACKED_CONCEPTS in a future migration that affects every
    issuer regardless of which subset of facts they file.
    """
    if only_unprojected_088:
        sql = """
            SELECT DISTINCT i.instrument_id
            FROM instruments i
            WHERE EXISTS (
                SELECT 1 FROM financial_facts_raw f
                WHERE f.instrument_id = i.instrument_id
                  AND f.concept = ANY(%(concepts)s)
            )
            AND EXISTS (
                SELECT 1 FROM financial_periods p
                WHERE p.instrument_id = i.instrument_id
                  AND (
                      p.treasury_shares IS NULL
                      OR p.shares_authorized IS NULL
                      OR p.shares_issued IS NULL
                      OR p.retained_earnings IS NULL
                  )
            )
            ORDER BY i.instrument_id
        """
        params: dict[str, object] = {"concepts": list(MIGRATION_088_CONCEPTS)}
    else:
        sql = """
            SELECT DISTINCT instrument_id
            FROM financial_facts_raw
            ORDER BY instrument_id
        """
        params = {}
    if limit is not None:
        sql += "\nLIMIT %(limit)s"
        params["limit"] = limit
    rows = conn.execute(sql, params).fetchall()
    return [int(r[0]) for r in rows]


def count_unprojected_088(
    conn: psycopg.Connection[tuple],
    instrument_ids: list[int],
) -> int:
    """Count instruments in cohort that STILL have an unprojected 088
    column despite having raw facts for it.

    ``normalize_financial_periods`` swallows per-instrument exceptions
    (logs + continues) and reports ``instruments_processed=len(ids)``
    regardless. For a one-shot backfill that masks partial failure.
    Run this post-normalize as a verification probe — if any rows come
    back, the operator knows specific instruments rolled back and can
    investigate the per-instrument exception in the application log.
    """
    if not instrument_ids:
        return 0
    rows = conn.execute(
        """
        SELECT COUNT(DISTINCT i.instrument_id)
        FROM instruments i
        WHERE i.instrument_id = ANY(%(ids)s)
          AND EXISTS (
              SELECT 1 FROM financial_facts_raw f
              WHERE f.instrument_id = i.instrument_id
                AND f.concept = ANY(%(concepts)s)
          )
          AND EXISTS (
              SELECT 1 FROM financial_periods p
              WHERE p.instrument_id = i.instrument_id
                AND (
                    p.treasury_shares IS NULL
                    OR p.shares_authorized IS NULL
                    OR p.shares_issued IS NULL
                    OR p.retained_earnings IS NULL
                )
          )
        """,
        {"ids": instrument_ids, "concepts": list(MIGRATION_088_CONCEPTS)},
    ).fetchall()
    return int(rows[0][0]) if rows else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually run the normaliser. Default is dry-run (cohort report only).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap cohort size for sample / staging runs.",
    )
    parser.add_argument(
        "--all-instruments",
        action="store_true",
        help=(
            "Process every instrument with raw facts, not only those "
            "missing migration-088 columns. Use after adding NEW "
            "TRACKED_CONCEPTS in a future migration that affects every "
            "issuer (not just ones with treasury / capital-structure "
            "data on file)."
        ),
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        cohort = select_cohort(
            conn,
            only_unprojected_088=not args.all_instruments,
            limit=args.limit,
        )
        # Close the implicit read transaction before per-instrument
        # transactions inside ``normalize_financial_periods`` run.
        # Same pattern the per-CIK path in app/services/fundamentals.py
        # and scripts/force_refresh_fundamentals.py use.
        conn.commit()

        cohort_label = "all-instruments" if args.all_instruments else "unprojected-088"
        logger.info(
            "backfill_xbrl_normalization: cohort=%s size=%d limit=%s",
            cohort_label,
            len(cohort),
            args.limit,
        )

        if not cohort:
            logger.info("backfill_xbrl_normalization: nothing to do")
            return 0

        if not args.apply:
            logger.info("backfill_xbrl_normalization: DRY-RUN — pass --apply to project the canonical rows")
            return 0

        started = time.monotonic()
        summary = normalize_financial_periods(conn, instrument_ids=cohort)
        conn.commit()
        elapsed = time.monotonic() - started

        # Post-normalize verification probe (codex review medium).
        # ``normalize_financial_periods`` catches per-instrument
        # exceptions and counts every input as ``processed``, so a
        # partial failure looks identical to a clean run in its
        # summary. Re-query the cohort: any instrument still missing a
        # migration-088 column (despite having raw facts for it) is a
        # rollback signal — surface that as a non-zero exit so a CI
        # wrapper / shell pipeline can distinguish a clean
        # backfill from one that needs the operator to read the log.
        unresolved = count_unprojected_088(conn, cohort) if not args.all_instruments else 0

        logger.info(
            "backfill_xbrl_normalization: done in %.1fs — instruments=%d "
            "raw_upserted=%d canonical_upserted=%d unresolved=%d",
            elapsed,
            summary.instruments_processed,
            summary.periods_raw_upserted,
            summary.periods_canonical_upserted,
            unresolved,
        )

        if unresolved > 0:
            logger.warning(
                "backfill_xbrl_normalization: %d instrument(s) still have NULL migration-088 columns "
                "despite raw facts on file — likely per-instrument exceptions during normalize. "
                "Check the application log for 'Failed to normalize instrument N' lines.",
                unresolved,
            )
            return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
