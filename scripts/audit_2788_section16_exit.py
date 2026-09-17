"""Read-only census of the Section 16 exit-box release (#2788 / #2226 M1).

Every figure quoted in ``docs/proposals/ownership/2026-09-17-2788-section16-exit-box-release.md``
and in PR #2788's description comes from here, and the script prints the SQL it ran next to
each result so a reader can re-run it rather than trust a transcript. Nothing is hand-copied
— the repo rule is that a derived statistic written into prose goes stale silently in the
place a reader trusts most.

    PYTHONPATH=. uv run python -m scripts.audit_2788_section16_exit --census

⚠ Read-only. No write path exists in this module, deliberately: #2790's session applied a
correction from an audit script before its checkpoint-2 review had run, and the removal
turned out to lack an exemption the gate kept.
"""

from __future__ import annotations

import argparse
from typing import Any

import psycopg
import psycopg.rows

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services import ownership_rollup

# ⚠ IMPORTED, not restated. An earlier revision copied the tip definition into this file
# and kept the copies in step with a test — which measured the TIP RULE rather than the
# shipped predicate, and so quoted a population 115 holders larger than anything that
# actually ships (the guards refuse those). A census that measures a proxy for the code it
# describes reports numbers that were never true of it. Both the correlation name (``oc``)
# and the guards therefore come from the service.
_TIP = ownership_rollup._INSIDER_TIP_PERIOD_SQL
_EXIT = ownership_rollup._INSIDER_SECTION16_EXIT_SQL

QUERIES: tuple[tuple[str, str], ...] = (
    (
        "flag census on insider_filings",
        """
        SELECT count(*) AS filings,
               count(*) FILTER (WHERE not_subject_to_section_16 IS TRUE)  AS flag_true,
               count(*) FILTER (WHERE not_subject_to_section_16 IS FALSE) AS flag_false,
               count(*) FILTER (WHERE not_subject_to_section_16 IS NULL)  AS flag_null,
               count(*) FILTER (WHERE is_tombstone)                       AS tombstoned,
               count(*) FILTER (WHERE period_of_report IS NULL)           AS period_null,
               count(*) FILTER (WHERE period_of_report IS NULL
                                  AND NOT is_tombstone)                   AS period_null_LIVE
          FROM insider_filings
        """,
    ),
    (
        "readable-status coverage on form4 ownership_insiders_current",
        """
        SELECT count(*) AS form4_rows,
               count(*) FILTER (WHERE f.accession_number IS NULL)              AS no_filing_row,
               count(*) FILTER (WHERE f.accession_number IS NULL
                                  AND c.source_document_id ~ ':(NDT|NDH):')    AS no_filing_and_dera,
               count(*) FILTER (WHERE f.accession_number IS NOT NULL
                                  AND f.not_subject_to_section_16 IS NULL)     AS filing_flag_null,
               count(*) FILTER (WHERE f.not_subject_to_section_16 IS FALSE)    AS flag_false,
               count(*) FILTER (WHERE f.not_subject_to_section_16 IS TRUE)     AS flag_true
          FROM ownership_insiders_current c
          LEFT JOIN insider_filings f ON f.accession_number = c.source_accession
         WHERE c.source = 'form4'
        """,
    ),
    (
        "form3 has no exit box - corpus side of the source-rule claim",
        """
        SELECT count(*) AS form3_rows,
               count(f.accession_number) AS joined_a_filing,
               count(*) FILTER (WHERE f.not_subject_to_section_16 IS TRUE) AS flag_true
          FROM ownership_insiders_current c
          LEFT JOIN insider_filings f ON f.accession_number = c.source_accession
         WHERE c.source = 'form3'
        """,
    ),
    (
        "SHIPPED predicate - what the reader actually excludes",
        f"""
        SELECT count(*)                        AS rows_released,
               sum(oc.shares)                  AS shares_released,
               count(DISTINCT oc.instrument_id) AS instruments,
               count(DISTINCT (oc.instrument_id, oc.holder_identity_key)) AS holders,
               count(*) FILTER (WHERE oc.source = 'form3') AS form3_rows_carried_along
          FROM ownership_insiders_current oc
         WHERE ({_EXIT})
        """,
    ),
    (
        "tip rule ALONE, before the guards - the gap is what arms 1/4/5 refuse",
        f"""
        WITH tip AS (
            SELECT oc.instrument_id, oc.holder_identity_key, oc.holder_cik,
                   bool_and(f.not_subject_to_section_16 IS TRUE) AS all_flagged,
                   bool_or (f.not_subject_to_section_16 IS TRUE) AS any_flagged
              FROM ownership_insiders_current oc
              LEFT JOIN insider_filings f ON f.accession_number = oc.source_accession
             WHERE oc.period_end = {_TIP}
             GROUP BY 1, 2, 3
        )
        SELECT count(*) FILTER (WHERE all_flagged)                     AS holders_passing_tip,
               count(*) FILTER (WHERE any_flagged AND NOT all_flagged) AS holders_tip_disagrees,
               count(*) FILTER (WHERE all_flagged
                                  AND holder_cik IS NULL)              AS tip_holder_cik_null
          FROM tip
        """,
    ),
    (
        "row-level counterfactual - siblings the REJECTED row-level design left behind",
        """
        SELECT count(*) AS row_level_candidates,
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM ownership_insiders_current s
                    WHERE s.instrument_id = c.instrument_id
                      AND s.holder_cik IS NOT DISTINCT FROM c.holder_cik
                      AND (s.ownership_nature, s.source)
                          IS DISTINCT FROM (c.ownership_nature, c.source))) AS left_a_sibling,
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM ownership_insiders_current s
                    WHERE s.instrument_id = c.instrument_id
                      AND s.holder_cik IS NOT DISTINCT FROM c.holder_cik
                      AND s.source = 'form3'
                      AND (s.ownership_nature, s.source)
                          IS DISTINCT FROM (c.ownership_nature, c.source))) AS left_a_form3_sibling
          FROM ownership_insiders_current c
          JOIN insider_filings f ON f.accession_number = c.source_accession
         WHERE f.not_subject_to_section_16 IS TRUE
        """,
    ),
    (
        "residual fail-open surface the guards close",
        f"""
        WITH tip AS (
            SELECT oc.instrument_id, oc.holder_identity_key, oc.holder_cik,
                   max(oc.period_end)                            AS tip_period,
                   bool_and(f.not_subject_to_section_16 IS TRUE) AS all_flagged,
                   max(f.issuer_cik)                             AS issuer_cik
              FROM ownership_insiders_current oc
              LEFT JOIN insider_filings f ON f.accession_number = oc.source_accession
             WHERE oc.period_end = {_TIP}
             GROUP BY 1, 2, 3
        )
        SELECT count(*) AS released_holders,
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM insider_filers lfl
                     JOIN insider_filings lf ON lf.accession_number = lfl.accession_number
                    WHERE lfl.filer_cik    = tip.holder_cik
                      AND lf.instrument_id = tip.instrument_id
                      AND NOT lf.is_tombstone
                      AND lf.period_of_report > tip.tip_period)) AS later_filing_beyond_tip,
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM sec_form25_common_equity_delistings d
                    WHERE LPAD(d.issuer_cik, 10, '0')
                        = LPAD(tip.issuer_cik, 10, '0')))        AS issuer_has_form25
          FROM tip
         WHERE all_flagged
        """,
    ),
    (
        "subsequent filing activity after a declared exit (NOT a re-entry measure)",
        """
        WITH flagged AS (
            SELECT f.accession_number, f.instrument_id, f.period_of_report, fl.filer_cik,
                   (SELECT count(*) FROM insider_filers x
                     WHERE x.accession_number = f.accession_number) AS n_owners
              FROM insider_filings f
              JOIN insider_filers fl ON fl.accession_number = f.accession_number
             WHERE f.not_subject_to_section_16 IS TRUE AND NOT f.is_tombstone
        )
        SELECT CASE WHEN n_owners = 1 THEN 'single' ELSE 'joint' END AS shape,
               count(*)                        AS owner_rows,
               count(DISTINCT accession_number) AS accessions,
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM insider_filers l2
                     JOIN insider_filings l ON l.accession_number = l2.accession_number
                    WHERE l2.filer_cik    = flagged.filer_cik
                      AND l.instrument_id = flagged.instrument_id
                      AND NOT l.is_tombstone
                      AND l.period_of_report > flagged.period_of_report)) AS files_again
          FROM flagged
         GROUP BY 1
         ORDER BY 1
        """,
    ),
)


def _census() -> int:
    # Row factory is set per-cursor, not on the connection: ``psycopg.connect`` is typed
    # for ``TupleRow`` and the repo's own readers take the per-cursor form.
    with psycopg.connect(settings.database_url) as conn:
        # One REPEATABLE READ snapshot for the whole census: the tables move under ordinary
        # ingest, and two figures taken minutes apart cannot be quoted in the same table.
        with snapshot_read(conn):
            for title, sql in QUERIES:
                print(f"\n=== {title} ===")
                print(sql.strip())
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(sql)  # type: ignore[arg-type]
                    rows: list[dict[str, Any]] = cur.fetchall()
                for row in rows:
                    for key, value in row.items():
                        print(f"  {key:<34} {value}")
                    if len(rows) > 1:
                        print("  --")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--census", action="store_true", help="print every census query and its result")
    args = ap.parse_args()
    if not args.census:
        ap.error("nothing to do: pass --census")
    return _census()


if __name__ == "__main__":
    raise SystemExit(main())
