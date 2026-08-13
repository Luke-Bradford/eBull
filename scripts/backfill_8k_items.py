"""One-shot backfill of ``filing_events.items`` for SEC 8-K rows (#675).

Pre-#675 the executor's 8-K items[] apply lived inside an ``else``
branch that only fired on the seed / first-time-refresh path; the
planner-driven refresh path (the steady-state common case) silently
discarded the submissions body and skipped the items extraction. As
a result, 480k+ existing 8-K rows have ``items=NULL`` even though
the parser is correct and the column type is non-NULLable in spirit.

This script re-fetches ``submissions.json`` for every covered CIK and
applies items to every existing 8-K ``filing_events`` row. Same logic
as the executor's #431 path, just iterated once across the universe.
The same script also re-applies the entity profile extraction (#427)
which had the same staleness defect. Idempotent — re-running is safe
because the UPDATE matches by ``(provider, provider_filing_id)`` and
``upsert_entity_profile`` is upsert-based.

Run from the repo root::

    uv run python scripts/backfill_8k_items.py --apply

Dry-run by default. ``--apply`` does the real fetches and writes.
``--limit N`` caps CIKs processed in this invocation (resumable —
the script always processes CIKs in deterministic identifier order
so a follow-up run with the same ``--limit`` after ``--offset N``
covers the next slice).

Rate limit: SEC fair-use is 10 req/s per UA. The provider's shared
process-wide throttle handles this; with ~5k covered CIKs and 100ms
per fetch the full sweep is ~8 minutes.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.sec_entity_profile import parse_entity_profile, upsert_entity_profile
from app.services.sec_filing_items import (
    apply_8k_items_to_filing_events,
    parse_8k_items_by_accession,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _load_covered_ciks(
    conn: psycopg.Connection[tuple],
    *,
    limit: int | None,
    offset: int,
) -> list[tuple[int, str, str]]:
    """Return ``(instrument_id, symbol, cik)`` for every tradable
    instrument with a primary SEC CIK identifier. Ordered by CIK so
    ``--offset`` / ``--limit`` produce stable slices across runs.

    Uses ``LIMIT ALL`` when ``limit is None`` so the same parameterised
    SQL handles both bounded and unbounded slices — keeps the query a
    PEP 675 ``LiteralString`` (psycopg + pyright reject f-string SQL).
    """
    cur = conn.execute(
        """
        SELECT i.instrument_id, i.symbol, ei.identifier_value
        FROM instruments i
        JOIN external_identifiers ei
          ON ei.instrument_id = i.instrument_id
         AND ei.provider = 'sec'
         AND ei.identifier_type = 'cik'
         AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        ORDER BY ei.identifier_value
        OFFSET %(offset)s
        LIMIT %(limit)s
        """,
        {"offset": int(offset), "limit": limit},
    )
    return [(int(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()]


def _process_cik(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cik: str,
    provider: SecFilingsProvider,
    apply: bool,
) -> tuple[int, int, str]:
    """Fetch submissions and apply items + profile for one CIK.

    Returns ``(items_updated, profile_updated, status)`` where
    ``items_updated`` is the rowcount of the UPDATE and
    ``profile_updated`` is 1 if the profile upsert ran (always 1 on
    success, 0 on failure). ``status`` is one of ``ok``,
    ``no_submissions``, ``items_failed``, ``profile_failed``,
    ``both_failed``.
    """
    submissions = provider.fetch_submissions(cik)
    if submissions is None:
        return (0, 0, "no_submissions")

    items_updated = 0
    profile_updated = 0
    items_status = "ok"
    profile_status = "ok"

    if apply:
        try:
            items_map = parse_8k_items_by_accession(submissions)
            if items_map:
                with conn.transaction():
                    items_updated = apply_8k_items_to_filing_events(conn, items_map)
        except Exception:
            logger.warning("backfill: items apply failed for cik=%s", cik, exc_info=True)
            items_status = "failed"

        try:
            profile = parse_entity_profile(submissions, instrument_id=instrument_id, cik=cik)
            with conn.transaction():
                upsert_entity_profile(conn, profile)
            profile_updated = 1
        except Exception:
            logger.warning("backfill: profile upsert failed for cik=%s", cik, exc_info=True)
            profile_status = "failed"
    else:
        # Dry-run: parse only, count what *would* change.
        items_map = parse_8k_items_by_accession(submissions)
        items_updated = sum(1 for _ in items_map)  # would-touch accession count
        profile_updated = 1  # would always upsert

    if items_status == "failed" and profile_status == "failed":
        status = "both_failed"
    elif items_status == "failed":
        status = "items_failed"
    elif profile_status == "failed":
        status = "profile_failed"
    else:
        status = "ok"
    return (items_updated, profile_updated, status)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Perform fetches and writes (default: dry-run).")
    parser.add_argument("--limit", type=int, default=None, help="Cap CIKs processed this run.")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N CIKs (for resumable runs).")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Log progress every N CIKs (default: 50).",
    )
    args = parser.parse_args(argv)

    if not args.apply:
        logger.info("DRY RUN — no fetches, no writes. Use --apply to commit.")

    # autocommit=True so every per-CIK ``with conn.transaction()`` is a
    # real BEGIN/COMMIT pair rather than a savepoint nested inside the
    # implicit outer transaction that ``conn.execute(SELECT)`` would
    # otherwise open. Without this, a process crash mid-loop would roll
    # back every preceding CIK's writes when the outer transaction
    # aborts. Codex checkpoint-2 flagged this.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        covered = _load_covered_ciks(conn, limit=args.limit, offset=args.offset)
        if not covered:
            logger.info("backfill: no covered CIKs found in slice (offset=%d, limit=%s)", args.offset, args.limit)
            return 0

        logger.info(
            "backfill: %d CIKs to process (offset=%d, limit=%s, apply=%s)",
            len(covered),
            args.offset,
            args.limit,
            args.apply,
        )

        if not args.apply:
            # Dry-run: only count, do not invoke the provider — keep
            # SEC traffic to zero.
            logger.info("backfill: dry-run skips submissions fetches; pass --apply to test the real path")
            return 0

        items_total = 0
        profile_total = 0
        ok_count = 0
        no_submissions = 0
        failed_items = 0
        failed_profile = 0
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            for idx, (instrument_id, symbol, cik) in enumerate(covered, start=1):
                # Trap per-CIK provider exceptions (network, 5xx
                # bursts, malformed JSON) so one bad CIK cannot abort
                # the rest of the sweep. With autocommit=True, every
                # prior CIK's writes are already durable.
                try:
                    items_updated, profile_updated, status = _process_cik(
                        conn,
                        instrument_id=instrument_id,
                        cik=cik,
                        provider=provider,
                        apply=args.apply,
                    )
                except Exception:
                    logger.warning(
                        "backfill: _process_cik raised for cik=%s — skipping",
                        cik,
                        exc_info=True,
                    )
                    failed_items += 1
                    continue
                items_total += items_updated
                profile_total += profile_updated
                if status == "ok":
                    ok_count += 1
                elif status == "no_submissions":
                    no_submissions += 1
                elif status == "items_failed":
                    failed_items += 1
                elif status == "profile_failed":
                    failed_profile += 1
                elif status == "both_failed":
                    failed_items += 1
                    failed_profile += 1

                if idx % args.progress_every == 0:
                    logger.info(
                        "backfill: %d/%d processed (symbol=%s cik=%s items=%d profile=%d status=%s)",
                        idx,
                        len(covered),
                        symbol,
                        cik,
                        items_updated,
                        profile_updated,
                        status,
                    )

        logger.info(
            "backfill: complete. ok=%d no_submissions=%d failed_items=%d failed_profile=%d "
            "items_rows_updated=%d profiles_upserted=%d",
            ok_count,
            no_submissions,
            failed_items,
            failed_profile,
            items_total,
            profile_total,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
