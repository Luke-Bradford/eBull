"""Backfill: clean the dual-pipeline insider collision out of ``_current`` (#1805).

#1804 fixed the operator-visible double-count at the read path; #1805 relocates the
same-accession de-collision into ``refresh_insiders_current`` so ``ownership_insiders_current``
stores ONE authoritative row set per ``(holder, accession)`` instead of leaving both
the XML-manifest row and the bulk-dataset ``:NDT:``/``:NDH:`` row in place.

Existing ``_current`` rows are only cleaned when an instrument is re-refreshed. The
``ownership_observations_repair`` sweep will NOT pick these up on its own — no new
observations landed, so each instrument's ``ownership_refresh_state`` watermark is
already current. This script forces a refresh for every instrument that currently
carries a dual-pipeline collision row, deleting the redundant rows via the MERGE's
``WHEN NOT MATCHED BY SOURCE → DELETE``.

Idempotent: re-running refreshes the same instruments; observations are unchanged, so a
second run is a no-op (and re-running the WHOLE script after new ingest just cleans any
newly-collided instruments).

Usage:
    uv run python scripts/backfill_1805_insider_decollision.py            # refresh affected
    uv run python scripts/backfill_1805_insider_decollision.py --dry-run  # count only
"""

from __future__ import annotations

import argparse
import logging

import psycopg

from app.config import settings
from app.services.ownership_observations import refresh_insiders_current_batch

logger = logging.getLogger("backfill_1805")

# Affected = instruments where a ``:NDT:``/``:NDH:`` row in ``_current`` has a
# plain-accession (XML) sibling ALSO in ``_current`` for the same
# ``(holder_cik, source_accession)``. This is exactly the set the refresh predicate
# removes — it filters the post-DISTINCT-ON ``winners`` set (the future ``_current``)
# keyed on ``holder_cik IS NOT DISTINCT FROM`` + accession, mirroring #1804 read-path
# fix B. So scanning ``_current`` finds precisely the instruments a refresh would
# change. (An observations-keyed scan would over-select: it flags rows whose plain
# sibling was superseded out of ``_current`` within its ``ownership_nature`` slot —
# rows the predicate correctly KEEPS, since dropping them changes operator figures.)
_AFFECTED_SQL = """
    SELECT DISTINCT oc.instrument_id
      FROM ownership_insiders_current oc
     WHERE oc.source_document_id ~ ':(NDT|NDH):'
       AND EXISTS (
           SELECT 1 FROM ownership_insiders_current x
            WHERE x.instrument_id = oc.instrument_id
              AND x.holder_cik IS NOT DISTINCT FROM oc.holder_cik
              AND x.source_accession = oc.source_accession
              AND x.source_document_id !~ ':(NDT|NDH):'
       )
     ORDER BY oc.instrument_id
"""

_CHUNK = 200


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="count affected instruments, do not refresh")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(_AFFECTED_SQL)
            ids = [int(r[0]) for r in cur.fetchall()]
        logger.info("affected instruments: %d", len(ids))
        if args.dry_run:
            return 0

        done = 0
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            refresh_insiders_current_batch(conn, instrument_ids=chunk)
            conn.commit()
            done += len(chunk)
            logger.info("refreshed %d / %d", done, len(ids))

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM (
                    SELECT 1 FROM ownership_insiders_current
                     WHERE source IN ('form4', 'form3') AND shares > 0 AND holder_cik IS NOT NULL
                     GROUP BY instrument_id, holder_cik, source_accession
                    HAVING bool_or(source_document_id ~ ':(NDT|NDH):')
                       AND bool_or(source_document_id !~ ':(NDT|NDH):')
                       AND count(DISTINCT ownership_nature) > 1
                ) g
                """
            )
            residual = cur.fetchone()
        logger.info("residual dual-pipeline collision groups: %s (target 0)", residual[0] if residual else "?")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
