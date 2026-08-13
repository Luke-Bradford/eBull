"""Operator audit for agent-CIK contamination in ``external_identifiers`` (#752).

Reports any instrument whose primary SEC CIK is a known filing-agent CIK
(EdgarOnline, GlobeNewswire, Donnelley etc.). Such rows would route
``Archives/edgar/data/{CIK}/{accession}/`` fetches under the agent's
non-existent archive directory and produce 100% fetch_errors.

Original #752 ticket hypothesised that the daily master-index ingest
(``app/services/fundamentals.py:1769``) was bucketing accessions by
column-1 CIK (the filer-of-record, often the agent for agent-filed
accessions) and propagating that into ``external_identifiers``.

Diagnostic run on 2026-05-02 against a populated dev DB returned
**zero contaminated rows** — even with ``is_primary=FALSE`` rows
included. So the data plane is clean; the production 404s the user
observed at 2026-05-01 20:51:44 came from a long-running ``app.jobs``
worker process started 2026-05-01 01:12:14, holding pre-#745+#748
in-memory code that fell through to the legacy CIK-prefix fallback.

This script remains valuable as:

  1. **Boot-time / scheduled audit**: confirms the data plane stays
     clean on every install / migration.
  2. **Operator-facing remediation tool**: exits non-zero with a CSV
     dump if a future bug reintroduces contamination.

Run from repo root:

    uv run python -m scripts.audit_agent_cik_contamination
    uv run python -m scripts.audit_agent_cik_contamination --include-secondary

Exit codes:
  0 — clean
  1 — contamination found (CSV emitted to stdout)
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def find_contaminated(
    conn: psycopg.Connection[tuple],
    *,
    include_secondary: bool,
) -> list[tuple[int, str, str, str, bool]]:
    """Return ``(instrument_id, symbol, company_name, agent_cik, is_primary)``
    for every external_identifier row whose CIK matches a known agent.

    ``include_secondary`` toggles whether ``is_primary=FALSE`` rows
    appear in the report. Default off — primary rows are the ones that
    drive the production fetch URL routing.
    """
    sql = """
        SELECT i.instrument_id, i.symbol, i.company_name,
               ei.identifier_value AS agent_cik, ei.is_primary
        FROM external_identifiers ei
        JOIN instruments i USING (instrument_id)
        WHERE ei.provider = 'sec'
          AND ei.identifier_type = 'cik'
          AND ei.identifier_value = ANY(%(agents)s)
    """
    if not include_secondary:
        sql += "\n          AND ei.is_primary = TRUE"
    sql += "\n        ORDER BY ei.identifier_value, i.symbol"
    rows = conn.execute(sql, {"agents": list(KNOWN_FILING_AGENT_CIKS)}).fetchall()
    return [(int(r[0]), str(r[1]), str(r[2]), str(r[3]), bool(r[4])) for r in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-secondary",
        action="store_true",
        help=(
            "Include is_primary=FALSE rows in the audit. These do not "
            "drive production routing but a non-empty list signals the "
            "ingest path leaked agent CIKs into the table at some point."
        ),
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        rows = find_contaminated(conn, include_secondary=args.include_secondary)

    logger.info(
        "audit_agent_cik_contamination: agents=%d rows_found=%d include_secondary=%s",
        len(KNOWN_FILING_AGENT_CIKS),
        len(rows),
        args.include_secondary,
    )

    if not rows:
        logger.info("audit_agent_cik_contamination: clean — no agent-CIK contamination found")
        return 0

    writer = csv.writer(sys.stdout)
    writer.writerow(["instrument_id", "symbol", "company_name", "agent_cik", "is_primary"])
    for r in rows:
        writer.writerow(r)
    logger.warning(
        "audit_agent_cik_contamination: %d contaminated row(s) — see CSV above. "
        "Remediation: demote affected rows to is_primary=FALSE and re-resolve "
        "the issuer's true CIK via SEC's company_tickers.json.",
        len(rows),
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
