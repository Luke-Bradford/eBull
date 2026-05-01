"""Bulk SEC ``companyfacts`` re-fetch (#759).

Re-fetches every primary-SEC-CIK issuer's full XBRL fact corpus from
SEC ``companyfacts`` so the unfiltered concept set lands in
``financial_facts_raw``. Resolves the gap left by the daily change-
driven planner: pre-#451-Phase-A the extractor filtered concepts on a
narrow editorial allowlist, and the daily refresher only re-fetches
CIKs that ship a new filing. Issuers without recent filings keep stale
narrow facts forever; their post-088 canonical columns (treasury_shares,
shares_authorized, shares_issued, retained_earnings) stay NULL.

Bootstrap-only — once-per-install, or after a TRACKED_CONCEPTS-affecting
migration. Not a nightly job.

Pipeline:

  1. Select every instrument with a primary SEC CIK in
     ``external_identifiers``.
  2. Re-fetch ``companyfacts`` for each via
     ``refresh_financial_facts``. SEC public-key tier is 10 req/sec —
     provider's internal throttle handles spacing. ~5000 issuers ≈
     10 min wall clock.
  3. Re-derive canonical ``financial_periods`` from the refreshed raw
     store via ``normalize_financial_periods``.

Both writes are idempotent: ``upsert_facts_for_instrument`` and the
canonical merge both use ON CONFLICT DO UPDATE WHERE IS DISTINCT FROM,
so re-runs over already-current data are no-ops for unchanged values.

Resumable: ``--start-from N`` skips instrument_ids ≤ N (useful after a
mid-run network blip). ``--limit N`` caps the cohort for staging runs.

Run from repo root:

    uv run python -m scripts.backfill_company_facts
    uv run python -m scripts.backfill_company_facts --apply
    uv run python -m scripts.backfill_company_facts --apply --limit 50
    uv run python -m scripts.backfill_company_facts --apply --start-from 1500

Defaults to dry-run (lists cohort, no fetches, no DB writes).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass

import psycopg

from app.config import settings
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.fundamentals import (
    normalize_financial_periods,
    refresh_financial_facts,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CohortRow:
    symbol: str
    instrument_id: int
    cik: str


def select_cohort(
    conn: psycopg.Connection[tuple],
    *,
    start_from: int,
    limit: int | None,
) -> list[CohortRow]:
    """Return primary-SEC-CIK instruments to fetch in this run.

    Ordered by ``instrument_id`` ASC so ``--start-from`` produces a
    deterministic resume point. Filters on ``is_primary = TRUE`` so an
    issuer with multiple historical CIKs (e.g. spin-offs, mergers)
    gets its current canonical CIK only.
    """
    sql = """
        SELECT i.symbol, i.instrument_id, ei.identifier_value
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.is_primary = TRUE
        WHERE i.instrument_id > %(start_from)s
        ORDER BY i.instrument_id ASC
    """
    params: dict[str, object] = {"start_from": start_from}
    if limit is not None:
        sql += "\nLIMIT %(limit)s"
        params["limit"] = limit
    rows = conn.execute(sql, params).fetchall()
    return [CohortRow(symbol=str(r[0]), instrument_id=int(r[1]), cik=str(r[2])) for r in rows]


def select_all_primary_sec_instrument_ids(
    conn: psycopg.Connection[tuple],
) -> list[int]:
    """Return EVERY primary-SEC-CIK instrument_id, ignoring
    ``--start-from`` / ``--limit``.

    Used by the normalize phase to re-derive the canonical store for
    the entire SEC universe, not just this invocation's fetch cohort
    (codex review High #1). Without this, a script run with
    ``--start-from N`` would fetch IDs > N but only normalize IDs > N,
    leaving a prior run's fetched-but-unnormalized prefix stranded
    under a stale canonical row.

    Idempotent — ``normalize_financial_periods`` no-ops on rows whose
    derived values are unchanged, so re-normalizing IDs that were
    already current adds no DB churn.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT i.instrument_id
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.is_primary = TRUE
        ORDER BY i.instrument_id ASC
        """,
    ).fetchall()
    return [int(r[0]) for r in rows]


def count_facts_without_periods(
    conn: psycopg.Connection[tuple],
    instrument_ids: list[int],
) -> int:
    """Count cohort members that have raw facts on file but no
    canonical ``financial_periods`` row at all — an approximate signal
    for "normalize raised an exception and rolled the row back".

    ``normalize_financial_periods`` swallows per-instrument exceptions
    (logs and continues), and ``NormalizationSummary`` reports
    ``instruments_processed = len(input_ids)`` regardless of whether
    each one actually committed (codex review High #2). For a one-shot
    backfill that masks partial failure. This probe is cheap to run
    (one indexed SELECT) and surfaces the "completely failed
    instrument" case at the exit boundary.

    Limitation: an instrument that committed *some* canonical rows but
    rolled back *others* shows up as healthy here. The schema-level
    invariant we can cheaply assert is "raw facts AND zero canonical
    rows" — anything finer-grained needs a per-period probe which
    ``backfill_xbrl_normalization.py`` already provides for the
    migration-088 column subset.
    """
    if not instrument_ids:
        return 0
    rows = conn.execute(
        """
        SELECT COUNT(DISTINCT i.instrument_id)
        FROM instruments i
        WHERE i.instrument_id = ANY(%(ids)s)
          AND EXISTS (SELECT 1 FROM financial_facts_raw f WHERE f.instrument_id = i.instrument_id)
          AND NOT EXISTS (SELECT 1 FROM financial_periods p WHERE p.instrument_id = i.instrument_id)
        """,
        {"ids": instrument_ids},
    ).fetchall()
    return int(rows[0][0]) if rows else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually fetch + write. Default is dry-run (cohort report only).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap cohort size for staging runs.",
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help=(
            "Skip instrument_ids ≤ this value. Use to resume after a "
            "mid-run network blip without re-fetching the already-done "
            "prefix."
        ),
    )
    parser.add_argument(
        "--skip-normalize",
        action="store_true",
        help=(
            "Run only the SEC re-fetch phase. Use when chaining with a "
            "separate normalize-only run, or when the cohort is being "
            "split across multiple invocations."
        ),
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        cohort = select_cohort(conn, start_from=args.start_from, limit=args.limit)
        # Close the implicit read transaction opened by ``select_cohort``
        # before the multi-minute SEC fetch begins. Without this, the
        # cohort-SELECT cursor's read transaction stays open across the
        # whole fetch loop. Same pattern ``force_refresh_fundamentals``
        # uses.
        conn.commit()

        logger.info(
            "backfill_company_facts: cohort=%d (start_from=%d limit=%s)",
            len(cohort),
            args.start_from,
            args.limit,
        )

        if not cohort:
            logger.info("backfill_company_facts: nothing to do")
            return 0

        if not args.apply:
            logger.info(
                "backfill_company_facts: DRY-RUN — pass --apply to actually fetch + write. "
                "Estimated wall clock at 10 req/sec: ~%.1f minutes.",
                len(cohort) / 600.0,
            )
            return 0

        symbols_for_refresh: list[tuple[str, int, str]] = [(r.symbol, r.instrument_id, r.cik) for r in cohort]

        started = time.monotonic()
        with SecFundamentalsProvider(user_agent=settings.sec_user_agent) as provider:
            facts_summary = refresh_financial_facts(provider, conn, symbols_for_refresh)
        # Commit the fetch phase before normalize so the per-instrument
        # transactions inside ``normalize_financial_periods`` are
        # top-level (not nested savepoints under any outer ledger tx
        # that ``refresh_financial_facts`` may still hold open). Same
        # boundary pattern ``force_refresh_fundamentals`` uses (codex
        # review #2 on PR #680).
        conn.commit()
        fetch_elapsed = time.monotonic() - started

        logger.info(
            "backfill_company_facts: fetch done in %.1fs — upserted=%d skipped=%d failed=%d",
            fetch_elapsed,
            facts_summary.facts_upserted,
            facts_summary.facts_skipped,
            facts_summary.symbols_failed,
        )

        if args.skip_normalize:
            logger.info("backfill_company_facts: --skip-normalize — leaving canonical re-derive for a separate run")
            return _exit_code_from_failures(facts_summary.symbols_failed, len(cohort), normalize_failed=0)

        # Normalize EVERY primary-SEC-CIK instrument — not just this
        # run's fetch cohort. With ``--start-from`` users can split a
        # multi-hour fetch across runs; scoping normalize to the
        # current cohort would strand prior-run fetches under stale
        # canonical rows. Re-normalizing already-current rows is a
        # no-op (ON CONFLICT DO UPDATE WHERE IS DISTINCT FROM), so
        # the only cost is per-instrument tx overhead — a few minutes
        # for the full universe (codex review High #1).
        norm_ids = select_all_primary_sec_instrument_ids(conn)
        conn.commit()

        norm_started = time.monotonic()
        norm_summary = normalize_financial_periods(conn, instrument_ids=norm_ids)
        conn.commit()
        norm_elapsed = time.monotonic() - norm_started

        # Approximate normalize-failure probe: count instruments with
        # raw facts on file but no canonical rows at all. Catches the
        # "every period rolled back" case that
        # ``NormalizationSummary`` hides (codex review High #2).
        normalize_failed = count_facts_without_periods(conn, norm_ids)

        logger.info(
            "backfill_company_facts: normalize done in %.1fs — instruments=%d "
            "raw_periods_upserted=%d canonical_upserted=%d normalize_failed=%d",
            norm_elapsed,
            norm_summary.instruments_processed,
            norm_summary.periods_raw_upserted,
            norm_summary.periods_canonical_upserted,
            normalize_failed,
        )
        if normalize_failed > 0:
            logger.warning(
                "backfill_company_facts: %d instrument(s) have raw facts on file but no canonical "
                "financial_periods row — normalize likely raised an exception. "
                "Check the application log for 'Failed to normalize instrument N' lines.",
                normalize_failed,
            )

        return _exit_code_from_failures(facts_summary.symbols_failed, len(cohort), normalize_failed=normalize_failed)


def _exit_code_from_failures(
    fetch_failed: int,
    fetch_total: int,
    *,
    normalize_failed: int,
) -> int:
    """Return shell-friendly exit status: 0 clean, 1 total fetch
    failure, 2 partial / normalize failure.

    Three input states feed into the decision:
      * ``fetch_failed`` — count of CIKs whose SEC fetch raised.
      * ``fetch_total`` — cohort size at fetch phase.
      * ``normalize_failed`` — instruments with raw facts but no
        canonical rows after normalize (apparent rollback).

    Codex review High #2: prior version derived exit code only from
    fetch failures and missed normalize rollbacks entirely. Now any
    normalize failure also flips the exit to 2 so the operator's
    automation wrapper can distinguish a clean backfill from one that
    needs the application log read.
    """
    if fetch_failed == 0 and normalize_failed == 0:
        return 0
    if fetch_failed == fetch_total and fetch_total > 0:
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
