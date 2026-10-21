"""Backfill #3948: fill ``blockholder_filings.date_of_event`` from the stored 13D/G bodies.

The adapter parsed the cover's event date as ISO only, so the column is NULL on every row. This re-parses each stored
``primary_doc_13dg`` body through the real edgartools ``parse_xml`` and the fixed adapter, and for each reporting
person sets ``date_of_event`` on the row with the same ``(accession, COALESCE(reporter_cik, ''), reporter_name)`` key,
only while it is NULL. No other column is written and no row is inserted or deleted, so it is narrower than the
DELETE + re-INSERT rewash (which would also re-derive every other column, e.g. the legacy ``member_of_group``).

v4 of ``_PARSER_VERSION_13DG`` differs from v3 only in this parse, so each body whose rows are filled (or that has
no date to fill) is bumped to v4. A body that fails to parse is left at its version and counted.

Idempotent. Commits per accession. ``--dry-run`` writes nothing.

Usage: ``PYTHONPATH=. uv run python -m scripts.backfill_3948_13dg_date_of_event [--dry-run] [--limit N]``
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter

import psycopg
from edgar.beneficial_ownership.schedule13 import Schedule13D, Schedule13G

from app.config import settings
from app.services.blockholders import _PARSER_VERSION_13DG
from app.services.manifest_parsers._schedule13_adapter import build_filing_from_edgartools_dict
from app.services.raw_filings import stored_body
from app.services.rewash_filings import _bump_parser_version
from scripts.ab_3948_13dg_date_of_event import _FORMS

_FILL_SQL = """
    UPDATE blockholder_filings SET date_of_event = %(d)s
     WHERE accession_number = %(acc)s AND COALESCE(reporter_cik, '') = %(cik)s AND reporter_name = %(name)s
       AND date_of_event IS NULL
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    st: Counter[str] = Counter()
    # Autocommit: each accession's ``conn.transaction()`` is its own BEGIN/COMMIT.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        cohort = conn.execute(
            """SELECT r.accession_number, min(bf.submission_type)
                 FROM filing_raw_documents r JOIN blockholder_filings bf USING (accession_number)
                WHERE r.document_kind = 'primary_doc_13dg' AND r.parser_version IS DISTINCT FROM %s
                GROUP BY 1 ORDER BY 1""",
            (_PARSER_VERSION_13DG,),
        ).fetchall()
        if args.limit:
            cohort = cohort[: args.limit]
        st["cohort"] = len(cohort)
        for n, (acc, form) in enumerate(cohort):
            if n % 5000 == 0:
                print("progress", n, len(cohort), dict(st), flush=True)
            source = _FORMS[form]
            try:
                body = stored_body(conn, accession_number=acc, document_kind="primary_doc_13dg")
                if body is None:
                    raise ValueError("no stored body")
                parsed = (Schedule13D if source == "sec_13d" else Schedule13G).parse_xml(body)
                filing = build_filing_from_edgartools_dict(
                    parsed, source=source, manifest_form=form, manifest_filer_cik="1", raw_xml=body
                )
            except Exception as exc:  # noqa: BLE001 — counted; the body stays at its version
                st["parse failed"] += 1
                print("FAILED", acc, repr(exc), flush=True)
                continue
            with conn.transaction():
                if filing.date_of_event is None:
                    st["bodies with no date"] += 1
                for p in filing.reporting_persons:
                    if filing.date_of_event is None:
                        continue
                    cur = conn.execute(
                        _FILL_SQL, {"d": filing.date_of_event, "acc": acc, "cik": p.cik or "", "name": p.name}
                    )
                    st["rows filled"] += cur.rowcount
                _bump_parser_version(
                    conn, accession_number=acc, document_kind="primary_doc_13dg", new_version=_PARSER_VERSION_13DG
                )
                st["bodies bumped"] += 1
                if args.dry_run:
                    raise psycopg.Rollback
    print({k: v for k, v in sorted(st.items())})
    return 1 if st["parse failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
