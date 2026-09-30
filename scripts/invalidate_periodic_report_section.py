"""Withdraw one ``periodic_report_sections`` row (#3518 spec §3) — operator-run, never called by the job.

Writes ONE append-only ``invalidated`` row naming the withdrawn row; nothing is updated or deleted. The target's
identity becomes due again once its effective history is empty, so the next producer run re-extracts it under the
current extractor. The sql/441 trigger refuses a missing target, a cross-identity target, a self-reference and an
invalidation of an invalidation; ``UNIQUE(invalidates_row_id)`` refuses a second withdrawal.

    PYTHONPATH=. uv run python -m scripts.invalidate_periodic_report_section --row-id 123 \
        --reason "transient error page recorded as caption_absent"
"""

from __future__ import annotations

import argparse
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings

_INSERT_SQL = """
    INSERT INTO periodic_report_sections
        (instrument_id, accession_number, section_id, extractor, status, detail, retryable, invalidates_row_id)
    SELECT instrument_id, accession_number, section_id, extractor, 'invalidated', %(reason)s, false, row_id
      FROM periodic_report_sections
     WHERE row_id = %(row_id)s
    RETURNING row_id, instrument_id, accession_number, section_id, extractor, invalidates_row_id, fetched_at
"""


def invalidate(conn: psycopg.Connection[Any], row_id: int, reason: str) -> dict[str, Any]:
    if not reason.strip():
        raise ValueError("--reason must say why the row is withdrawn")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_INSERT_SQL, {"row_id": row_id, "reason": reason.strip()})
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"periodic_report_sections row {row_id} does not exist")
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--row-id", type=int, required=True)
    ap.add_argument("--reason", required=True)
    a = ap.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        row = invalidate(conn, a.row_id, a.reason)
        conn.commit()
    print(row)


if __name__ == "__main__":
    main()
