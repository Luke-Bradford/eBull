"""One-shot backfill of ``filing_events.red_flag_score`` (#1748).

The score was added to all three write paths (NT at INSERT, 8-K via
``apply_8k_items_to_filing_events``), so a *fresh* bootstrap populates it
natively. This script fills the existing corpus, where every input is
already in the DB — no SEC fetch needed.

Candidate rows: ``red_flag_score IS NULL`` AND (a Form NT, or an 8-K with
``items[]`` already applied). The score is computed by the SAME pure fn
the write paths use (``app.services.filings_risk.score_filing_red_flag``)
— one source of truth, no SQL re-encoding of the rule. Rows that compute
to ``None`` (e.g. an 8-K with no critical item) are left NULL.

Run from the repo root::

    uv run python scripts/backfill_red_flag_score.py --apply

Dry-run by default (counts what would change). ``--batch N`` sets the
keyset page size (default 5000). Idempotent — re-running only touches
still-NULL candidates.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict

import psycopg

from app.config import settings
from app.services.filings_risk import load_severity_by_code, score_filing_red_flag

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit writes (default: dry-run count).")
    parser.add_argument("--batch", type=int, default=5000, help="Keyset page size (default 5000).")
    args = parser.parse_args(argv)

    if not args.apply:
        logger.info("DRY RUN — counts only, no writes. Use --apply to commit.")

    # autocommit so each per-batch ``with conn.transaction()`` is a real
    # BEGIN/COMMIT pair (durable incrementally; a crash keeps prior
    # batches) rather than a savepoint under an implicit outer txn.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        severity_by_code = load_severity_by_code(conn)

        last_id = 0
        scanned = 0
        none_total = 0
        # score value -> rows written, so totals stay correct no matter what
        # values the pure fn returns (no re-encoding of the constants here).
        written: defaultdict[float, int] = defaultdict(int)
        while True:
            rows = conn.execute(
                """
                SELECT filing_event_id, filing_type, items
                FROM filing_events
                WHERE red_flag_score IS NULL
                  AND filing_event_id > %(last_id)s
                  AND (filing_type ~* '^NT' OR items IS NOT NULL)
                ORDER BY filing_event_id
                LIMIT %(batch)s
                """,
                {"last_id": last_id, "batch": int(args.batch)},
            ).fetchall()
            if not rows:
                break

            # Group ids by the score the pure fn returns. The SQL only writes
            # the value the rule produced — never a hardcoded bucket — so the
            # backfill follows the scorer's constants automatically.
            ids_by_score: defaultdict[float, list[int]] = defaultdict(list)
            for fid, filing_type, items in rows:
                score = score_filing_red_flag(filing_type, items, severity_by_code)
                if score is None:
                    none_total += 1
                else:
                    ids_by_score[score].append(int(fid))
            scanned += len(rows)
            last_id = int(rows[-1][0])

            if args.apply:
                with conn.transaction():
                    for score, ids in ids_by_score.items():
                        conn.execute(
                            "UPDATE filing_events SET red_flag_score = %s WHERE filing_event_id = ANY(%s)",
                            (score, ids),
                        )
            for score, ids in ids_by_score.items():
                written[score] += len(ids)
            logger.info(
                "backfill: scanned=%d written=%s none=%d (last_id=%d)",
                scanned,
                {k: written[k] for k in sorted(written)},
                none_total,
                last_id,
            )

    logger.info(
        "backfill: complete. scanned=%d written=%s none(left NULL)=%d apply=%s",
        scanned,
        {k: written[k] for k in sorted(written)},
        none_total,
        args.apply,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
