"""#2788 — read-only census of the insider ``_current`` rows the Form 4 retention
bound removes, and of where they came from.

The ticket asked why an ``ownership_insiders_current`` row freezes and warned that
"the holder ceased to be a reporting person" was the OBVIOUS story and an UNMEASURED
one. It is not the story. The freeze is dominated by a single ingest event, and this
script is what says so — every figure it prints is computed at run time against the
dev DB, so a later ingest moves the numbers instead of leaving a stale copy in prose.

Sections:

``provenance``
    ``_current`` split by writer. The discriminator is the one
    ``data-sources/sec-edgar.md`` §2.3 gives: a ``source_document_id`` carrying an
    ``:NDT:`` / ``:NDH:`` marker came from the DERA bulk insider dataset
    (``sec_insider_dataset_ingest``); a bare accession came from the XML manifest
    parse.

``retention``
    Rows on either side of ``form4_retention_cutoff()``, which is the boundary
    ``form4_within_retention`` already enforces on every Form 4 WRITER (#1233 §4.3)
    and which the rollup read path now honours too.

``ingest_day``
    The retention split attributed to the day the backing observation was written.
    This is the section that identifies the cause: the #2701 research ingest ran
    ``ingest_insider_dataset_archive`` with ``retention_cutoff_override`` and wrote
    pre-retention Form 4 rows into ``ownership_insiders_observations`` — the table
    ``ownership_insiders_current`` projects.

⚠ Form 3 is reported but never gated. #1233 §4.3 exempts it deliberately ("Form 3
rows are NOT gated here"), because the initial statement is the only evidence for a
holder who has never transacted. Its stale rows are a SEPARATE, pre-existing
question; this script prints them so they are not mistaken for part of the fix.

Usage:

    PYTHONPATH=. uv run python -m scripts.audit_2788_insider_retention --census
"""

from __future__ import annotations

import argparse
from typing import Any

import psycopg

from app.config import settings
from app.services.insider_transactions import form4_retention_cutoff

PROVENANCE_SQL = """
SELECT (source_document_id ~ ':(NDT|NDH):') AS dera_dataset,
       source,
       count(*)                              AS rows,
       min(period_end)                       AS min_period_end,
       max(period_end)                       AS max_period_end,
       count(DISTINCT instrument_id)         AS instruments,
       sum(shares)                           AS shares
  FROM ownership_insiders_current
 WHERE shares IS NOT NULL
 GROUP BY 1, 2
 ORDER BY 1, 2
"""

RETENTION_SQL = """
SELECT source,
       (source = 'form4' AND filed_at::date < %(cutoff)s) AS beyond_retention,
       count(*)                      AS rows,
       count(DISTINCT instrument_id) AS instruments,
       min(filed_at)::date           AS min_filed,
       max(filed_at)::date           AS max_filed,
       sum(shares)                   AS shares
  FROM ownership_insiders_current
 WHERE shares IS NOT NULL
 GROUP BY 1, 2
 ORDER BY 1, 2
"""

# The observations PK is the join key; ``_current`` is a projection of exactly one
# observation row per (instrument, holder identity, nature).
INGEST_DAY_SQL = """
SELECT date_trunc('day', o.known_from)::date AS ingest_day,
       (c.source = 'form4' AND c.filed_at::date < %(cutoff)s) AS beyond_retention,
       count(*)                        AS rows,
       count(DISTINCT c.instrument_id) AS instruments,
       sum(c.shares)                   AS shares
  FROM ownership_insiders_current c
  JOIN ownership_insiders_observations o
    ON o.instrument_id        = c.instrument_id
   AND o.holder_identity_key  = c.holder_identity_key
   AND o.ownership_nature     = c.ownership_nature
   AND o.source               = c.source
   AND o.source_document_id   = c.source_document_id
   AND o.period_end           = c.period_end
 WHERE c.shares IS NOT NULL
 GROUP BY 1, 2
HAVING count(*) >= %(min_rows)s
 ORDER BY 3 DESC
"""

#: Ingest days contributing fewer rows than this are folded away — the tail is one
#: row per day across months of steady-state ingest and drowns the signal. Printed
#: with the threshold so a reader can see what was dropped rather than infer that
#: the listed days are everything (the "no silent caps" rule).
INGEST_DAY_MIN_ROWS = 500


def _print(title: str, cur: psycopg.Cursor[Any]) -> None:
    cols = [d.name for d in cur.description or ()]
    rows = cur.fetchall()
    print(f"\n=== {title} ({len(rows)} rows)")
    print("  " + " | ".join(cols))
    for row in rows:
        print("  " + " | ".join("" if v is None else str(v) for v in row))


def census() -> int:
    cutoff = form4_retention_cutoff()
    print(f"form4_retention_cutoff() = {cutoff}  (INSIDER_FORM4_RETENTION_YEARS, #1233 §4.3)")
    with psycopg.connect(settings.database_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = '900s'")
        cur.execute(PROVENANCE_SQL)
        _print("provenance of ownership_insiders_current", cur)
        cur.execute(RETENTION_SQL, {"cutoff": cutoff})
        _print("retention split (form3 shown but NEVER gated — #1233 §4.3)", cur)
        cur.execute(INGEST_DAY_SQL, {"cutoff": cutoff, "min_rows": INGEST_DAY_MIN_ROWS})
        _print(f"backing-observation ingest day (days with >= {INGEST_DAY_MIN_ROWS} rows only)", cur)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--census", action="store_true", help="print the three tables")
    args = ap.parse_args()
    if not args.census:
        ap.error("--census is required (this script has no other mode)")
    return census()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
