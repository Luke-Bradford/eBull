"""One-shot retirement of issuer-CIK blockholder observations (#1638).

Pre-#1638 the 13D/G write-through + legacy mirror stamped
``ownership_blockholders_observations.reporter_cik`` with the subject
company's own issuer CIK (post-#1628 the manifest/filer key became the
issuer). #1638 corrects the write paths to use the reporting person's
own CIK (or the document filer of record). But the observation natural
key includes ``reporter_cik``, so a corrected re-drain INSERTs a NEW row
and leaves the stale issuer-CIK row valid (``known_to IS NULL``). The
``_current`` MERGE is ``DISTINCT ON (reporter_cik, ownership_nature)`` —
it would then keep BOTH → a fix that doubles the row. Blockholder
observations have no ``known_to`` supersession wired, so re-drain alone
does not retire the stale rows.

This script retires exactly the bug signature — an active 13D/G
observation whose ``reporter_cik`` equals the instrument's own issuer
``(sec, cik)`` — and immediately refreshes ``_current`` for the affected
instruments so the stale current rows are deleted
(``WHEN NOT MATCHED BY SOURCE THEN DELETE``). A 13D/G is never filed by
the issuer on itself, so this predicate cannot retire a legitimate row.

Run from repo root (dry-run first):

    uv run python -m scripts.backfill_1638_retire_issuer_cik_blockholders
    uv run python -m scripts.backfill_1638_retire_issuer_cik_blockholders --apply

After ``--apply``, trigger the re-drain so the corrected parser
repopulates the observations under the bumped parser_version:

    POST /jobs/sec_rebuild/run  {"params": {"source": "sec_13d"}}
    POST /jobs/sec_rebuild/run  {"params": {"source": "sec_13g"}}

Idempotent: re-running after a clean re-drain retires 0 rows.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.services.ownership_observations import refresh_blockholders_current

logger = logging.getLogger("backfill_1638")

# An active 13D/G observation whose reporter_cik == the instrument's own
# issuer (sec, cik). lpad both sides so a stored unpadded CIK still matches.
_STALE_PREDICATE = """
    o.known_to IS NULL
    AND o.source IN ('13d', '13g')
    AND ei.provider = 'sec'
    AND ei.identifier_type = 'cik'
    AND lpad(ei.identifier_value, 10, '0') = lpad(o.reporter_cik, 10, '0')
"""


def _count_stale(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    row = conn.execute(
        f"""
        SELECT count(*) AS rows, count(DISTINCT o.instrument_id) AS instruments
        FROM ownership_blockholders_observations o
        JOIN external_identifiers ei ON ei.instrument_id = o.instrument_id
        WHERE {_STALE_PREDICATE}
        """
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def _retire(conn: psycopg.Connection[tuple]) -> list[int]:
    """Soft-delete the stale rows (set known_to) and bump ingested_at so
    the refresh / repair-sweep watermark (MAX(ingested_at)) advances past
    the expiry. Returns the distinct affected instrument_ids."""
    rows = conn.execute(
        f"""
        UPDATE ownership_blockholders_observations o
        SET known_to = clock_timestamp(),
            ingested_at = clock_timestamp()
        FROM external_identifiers ei
        WHERE ei.instrument_id = o.instrument_id
          AND {_STALE_PREDICATE}
        RETURNING o.instrument_id
        """
    ).fetchall()
    return sorted({int(r[0]) for r in rows})


def _stale_current_instruments(conn: psycopg.Connection[tuple]) -> list[int]:
    """Instruments whose ``ownership_blockholders_current`` still holds an
    issuer-CIK blockholder row. On a clean run this equals the retired set;
    after a partial run (observations retired but a refresh failed) it
    surfaces the un-refreshed remainder so a rerun still drops them even
    though there are no observations left to retire — makes the script
    idempotent under partial failure (Codex ckpt-2)."""
    rows = conn.execute(
        """
        SELECT DISTINCT bc.instrument_id
        FROM ownership_blockholders_current bc
        JOIN external_identifiers ei ON ei.instrument_id = bc.instrument_id
        WHERE ei.provider = 'sec'
          AND ei.identifier_type = 'cik'
          AND bc.source IN ('13d', '13g')
          AND lpad(ei.identifier_value, 10, '0') = lpad(bc.reporter_cik, 10, '0')
        """
    ).fetchall()
    return sorted({int(r[0]) for r in rows})


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Retire the stale rows + refresh _current. Without it, dry-run counts only.",
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        n_rows, n_instruments = _count_stale(conn)
        stale_current = _stale_current_instruments(conn)
        logger.info(
            "backfill_1638: %d stale issuer-CIK observations across %d instruments; "
            "%d instruments still hold a stale _current row",
            n_rows,
            n_instruments,
            len(stale_current),
        )
        if not args.apply:
            logger.info("backfill_1638: DRY-RUN — pass --apply to retire + refresh")
            return 0
        if n_rows == 0 and not stale_current:
            logger.info("backfill_1638: nothing to retire or refresh")
            return 0

        # Retire the stale observations, then refresh the UNION of the
        # instruments whose observations we just retired AND any instrument
        # still carrying a stale _current row (heals a prior partial run).
        retired = _retire(conn) if n_rows else []
        conn.commit()
        affected = sorted(set(retired) | set(stale_current))
        logger.info("backfill_1638: retired %d rows; refreshing _current for %d instruments", n_rows, len(affected))
        for instrument_id in affected:
            refresh_blockholders_current(conn, instrument_id=instrument_id)
        logger.info(
            "backfill_1638: DONE — %d rows retired, %d instruments refreshed. "
            "Now trigger sec_rebuild {source: sec_13d} + {source: sec_13g} to repopulate.",
            n_rows,
            len(affected),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
