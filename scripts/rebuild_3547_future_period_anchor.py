"""#3547 backfill — re-derive instruments carrying future-context facts or rows.

Scope: every instrument with a ``financial_facts_raw`` fact or a current
``financial_periods`` row whose period ends after its filing. For each,
``normalize_financial_periods`` re-derives periods (the derivation now ignores
such facts), runs the canonical merge (Phase B3 drops persisted rows of that
shape) and rewashes ``fundamentals_snapshot``. Normalization skips an instrument
with no facts left, so any still holding a future row then gets the merge and
snapshot rewash directly.

Treasury: ``_record_treasury_observations_for_instrument`` is additive, so an
``xbrl_dei`` observation recorded from a phantom period survives the rewash. It is
closed (``known_to``, per I6 — never a hard delete) and the current row refreshed.
``record_treasury_observation`` revives a closed row on reassert, so a real
quarter later ending on the same date is not swallowed.

No SEC HTTP calls. Read-only by default; ``--apply`` executes and then asserts
zero future rows remain (normalization swallows per-instrument errors, so the
assertion, not the summary, is the success signal).

    PYTHONPATH=. uv run python -m scripts.rebuild_3547_future_period_anchor
    PYTHONPATH=. uv run python -m scripts.rebuild_3547_future_period_anchor --apply
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.services.fundamentals import (
    _canonical_merge_instrument,
    _write_snapshots_from_periods,
    normalize_financial_periods,
)
from app.services.ownership_observations import refresh_treasury_current

logger = logging.getLogger(__name__)

_SCOPE_SQL = """
SELECT instrument_id FROM financial_facts_raw WHERE period_end > filed_date
UNION
SELECT instrument_id FROM financial_periods
WHERE superseded_at IS NULL AND source = 'sec_edgar' AND period_end_date > filed_date
ORDER BY 1
"""

_COUNTS_SQL = """
SELECT
    (SELECT count(*) FROM financial_periods
     WHERE superseded_at IS NULL AND source = 'sec_edgar' AND period_end_date > filed_date),
    (SELECT count(*) FROM fundamentals_snapshot WHERE as_of_date > current_date),
    (SELECT count(*) FROM ownership_treasury_observations
     WHERE source = 'xbrl_dei' AND known_to IS NULL AND period_end > (filed_at AT TIME ZONE 'UTC')::date),
    (SELECT count(*) FROM ownership_treasury_current WHERE period_end > (filed_at AT TIME ZONE 'UTC')::date)
"""

# Instruments normalization skipped: it ``continue``s when no facts remain, so
# Phase B3 and the snapshot rewash never ran for them.
_LEFTOVER_SQL = """
SELECT instrument_id FROM financial_periods
WHERE superseded_at IS NULL AND source = 'sec_edgar' AND period_end_date > filed_date
UNION
SELECT instrument_id FROM fundamentals_snapshot WHERE as_of_date > current_date
ORDER BY 1
"""

_PHANTOM_TREASURY_SQL = """
UPDATE ownership_treasury_observations
SET known_to = now()
WHERE source = 'xbrl_dei' AND known_to IS NULL AND period_end > (filed_at AT TIME ZONE 'UTC')::date
RETURNING instrument_id
"""


def _counts(conn: psycopg.Connection[tuple]) -> tuple[int, ...]:
    row = conn.execute(_COUNTS_SQL).fetchone()
    if row is None:
        raise RuntimeError("count query returned no row")
    return tuple(row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with psycopg.connect(settings.database_url) as conn:
        ids = [r[0] for r in conn.execute(_SCOPE_SQL)]
        before = _counts(conn)
        logger.info(
            "scope=%d instruments; future periods=%d, future snapshots=%d, open phantom treasury obs=%d, "
            "phantom treasury current=%d",
            len(ids),
            *before,
        )
        if not args.apply:
            logger.info("dry-run — pass --apply to execute")
            return 0

        summary = normalize_financial_periods(conn, ids)
        conn.commit()
        logger.info(
            "normalized %d instruments: %d raw periods, %d canonical",
            summary.instruments_processed,
            summary.periods_raw_upserted,
            summary.periods_canonical_upserted,
        )

        leftovers = [r[0] for r in conn.execute(_LEFTOVER_SQL)]
        for iid in leftovers:
            with conn.transaction():
                _canonical_merge_instrument(conn, iid)
                _write_snapshots_from_periods(conn, instrument_id=iid)
        conn.commit()
        logger.info("merged + snapshot-rewashed %d instruments normalization skipped: %s", len(leftovers), leftovers)

        with conn.transaction():
            closed = sorted({r[0] for r in conn.execute(_PHANTOM_TREASURY_SQL)})
            for iid in closed:
                refresh_treasury_current(conn, instrument_id=iid)
        logger.info("closed phantom treasury observations on %d instruments: %s", len(closed), closed)

        after = _counts(conn)
        logger.info(
            "after: future periods=%d, future snapshots=%d, open phantom treasury obs=%d, phantom treasury current=%d",
            *after,
        )
        if any(after):
            logger.error("future rows remain — inspect the normalization log for failed instruments")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
