"""#3227 — Form 4 indirect balances collapsed to one vehicle (the MNSO supersession residual).

    PYTHONPATH=. uv run python -m scripts.census_3227_form4_vehicle_balance
    PYTHONPATH=. uv run python -m scripts.census_3227_form4_vehicle_balance --examples 10

Read-only.  Nothing is written, nothing is refreshed.  Spec:
docs/specs/ownership/2026-10-17-3227-form4-vehicle-balance.md, whose conditions this script
applies one by one, in the spec's order, so every count the spec quotes comes from here.

## What is being measured

Both Form 4 XML observation writers (``insider_transactions._record_form4_observations_for_filing``
and ``ownership_observations_sync.sync_insiders``) keep ONE line per ``(filer, D/I)``: the
latest by ``(txn_date, txn_row_num)``, its column 5 "Amount of Securities Beneficially Owned
Following Reported Transaction(s)".  Form 4 General Instruction 4(b)(iii) puts each form of
indirect ownership on its own line, so where column 5 is that form's own balance, the other
vehicles' balances are dropped.  MNSO ``0001104659-26-069242`` (Ye Guofu): the writer keeps "by
Mini Investment Limited" 315,740,482 and drops "by YGF MN LIMITED" 195,115,382.

## Why a discriminator is needed

Instruction 4(a)(i) requires the class total after the transactions but not how it is laid out
across the separate ownership-form lines, and filers do both.  Each line's pre-transaction
balance is ``post - signed shares`` (``A`` adds, ``D`` subtracts).  Where column 5 is the form's
own balance, a line chains to the previous line OF THE SAME FORM; where it is a running total,
a line chains to an earlier line of ANOTHER form.  MNSO is the first shape; Abdiel
(``0000950170-23-052960``) and Pentwater (``0000902664-26-002383``) are the second, and a
per-form sum there counts one block once per fund.

## What it cannot establish

* Not that a ``sums`` group is per-vehicle beyond its own arithmetic.  A running total passes
  every test only if no line starts from an earlier line of another form, and a group with one
  line per form has no within-form chain to test.  Both are counted (``single_line_forms``).
* Not the DERA ``:NDT:`` path: those rows carry no D/I, nature or title.
* Not the cross-filing balance of vehicles a later Form 4 does not mention.  ``mnso_shape``
  counts CANDIDATES for that (an earlier multi-vehicle Form 3 exists), not a verified loss.
"""

from __future__ import annotations

import argparse
from typing import LiteralString

import psycopg

from app.config import settings

# Every non-derivative line of a Form 4 or 4/A, whatever the writers would keep, so the
# completeness test (condition 2) sees the lines the writers drop.  ``nat`` is the spec's
# normalised nature: ASCII alphanumerics only, lower case; '' is "blank".  D lines are one
# series whatever their nature text (the spec's definition of a form).
_LINES = """
CREATE TEMP TABLE c_lines AS
SELECT t.instrument_id, t.accession_number, nullif(btrim(t.filer_cik), '') AS filer_cik,
       t.txn_row_num, t.txn_date, t.txn_code,
       t.txn_date_invalid, t.shares, t.acquired_disposed_code AS ad,
       t.post_transaction_shares AS post, t.direct_indirect AS di, t.security_title,
       CASE WHEN t.direct_indirect = 'I'
            THEN lower(regexp_replace(coalesce(t.nature_of_ownership, ''), '[^a-zA-Z0-9]', '', 'g'))
            ELSE '' END AS nat,
       t.post_transaction_shares
         - CASE t.acquired_disposed_code WHEN 'A' THEN t.shares WHEN 'D' THEN -t.shares END AS pre
  FROM insider_transactions t
  JOIN insider_filings f ON f.accession_number = t.accession_number
 WHERE NOT t.is_derivative
   AND f.document_type IN ('4', '4/A')
   AND NOT f.is_tombstone
"""

# Candidates (condition 1): the filer has I lines under more than one normalised nature, blank
# counted as a nature here so a blank line reaches condition 2 and refuses there.  A null or
# blank CIK keeps today's grouping and value, so it is excluded and counted separately.
_CANDIDATES = """
CREATE TEMP TABLE c_cand AS
SELECT instrument_id, accession_number, filer_cik
  FROM c_lines
 WHERE di = 'I' AND filer_cik IS NOT NULL
 GROUP BY 1, 2, 3
HAVING count(DISTINCT nat) > 1
"""

# One row per candidate, every condition evaluated over the filer's WHOLE non-derivative table
# in the accession (D and I lines), as the spec requires.  A "form" is (D/I, normalised nature).
_CONDITIONS = """
CREATE TEMP TABLE c_eval AS
WITH x AS (
    SELECT l.*,
           lag(l.post) OVER wn AS prev_post_same_form,
           row_number() OVER (PARTITION BY l.instrument_id, l.accession_number, l.filer_cik, l.di, l.nat
                              ORDER BY l.txn_date DESC, l.txn_row_num DESC) AS rn_form
      FROM c_lines l
      JOIN c_cand c USING (instrument_id, accession_number, filer_cik)
    WINDOW wn AS (PARTITION BY l.instrument_id, l.accession_number, l.filer_cik, l.di, l.nat
                  ORDER BY l.txn_row_num)
)
SELECT x0.instrument_id, x0.accession_number, x0.filer_cik,
       coalesce((SELECT array_agg(DISTINCT btrim(f.filer_cik)) FROM insider_filers f
                  WHERE f.accession_number = x0.accession_number)
                = ARRAY[x0.filer_cik], FALSE) AS one_owner,
       -- IS TRUE per line: a NULL predicate must refuse, not drop out of bool_and.
       bool_and((x0.post IS NOT NULL AND x0.post >= 0 AND x0.shares IS NOT NULL AND x0.shares >= 0
                 AND x0.pre >= 0
                 AND x0.ad IN ('A', 'D') AND x0.txn_date IS NOT NULL AND NOT x0.txn_date_invalid
                 AND x0.di IN ('D', 'I') AND (x0.di = 'D' OR x0.nat <> '')
                 AND coalesce(x0.security_title, '') !~ '^[[:space:]]*$') IS TRUE) AS complete,
       count(DISTINCT x0.security_title) = 1 AS one_title,
       bool_and(x0.prev_post_same_form IS NULL OR x0.pre = x0.prev_post_same_form) AS forms_chain,
       NOT EXISTS (
           SELECT 1 FROM x a JOIN x b
             ON b.instrument_id = a.instrument_id AND b.accession_number = a.accession_number
            AND b.filer_cik = a.filer_cik
            AND (b.di, b.nat) <> (a.di, a.nat)
            AND a.pre = b.post AND b.post <> 0
          WHERE a.instrument_id = x0.instrument_id AND a.accession_number = x0.accession_number
            AND a.filer_cik = x0.filer_cik
       ) AS no_cross_chain,
       count(DISTINCT x0.post) FILTER (WHERE x0.rn_form = 1 AND x0.post <> 0)
         = count(*) FILTER (WHERE x0.rn_form = 1 AND x0.post <> 0) AS distinct_finals,
       NOT bool_or(x0.prev_post_same_form IS NOT NULL) AS single_line_forms,
       sum(x0.post) FILTER (WHERE x0.rn_form = 1 AND x0.di = 'I') AS vehicle_sum,
       (SELECT y.post FROM x y
         WHERE y.instrument_id = x0.instrument_id AND y.accession_number = x0.accession_number
           AND y.filer_cik = x0.filer_cik AND y.di = 'I' AND y.post IS NOT NULL
           AND y.txn_date IS NOT NULL AND NOT y.txn_date_invalid
         ORDER BY y.txn_date DESC, y.txn_row_num DESC LIMIT 1) AS writer_value
  FROM x x0
 GROUP BY x0.instrument_id, x0.accession_number, x0.filer_cik
"""

# Condition 5c, per I form of every candidate.  The form's first line (lowest txn_row_num)
# opens at a non-zero balance.  Evidence is any EARLIER filing of the filer on the instrument
# (manifest filed_at strictly earlier), restricted to the spec's sources: a non-tombstoned Form
# 4/4A transaction line or Form 3/3A holding line, non-derivative, of the unit's title.  A
# match on another form (an I line of another nature, or a D line) and on no line of its own
# form is a running total carried across filings.  A unit with no filing time cannot be checked
# and refuses (``no_time``).
_HIST = """
CREATE TEMP TABLE c_hist AS
SELECT t.instrument_id, nullif(btrim(t.filer_cik), '') AS filer_cik, m.filed_at, t.security_title,
       t.direct_indirect AS di, t.post_transaction_shares AS bal,
       CASE WHEN t.direct_indirect = 'I'
            THEN lower(regexp_replace(coalesce(t.nature_of_ownership, ''), '[^a-zA-Z0-9]', '', 'g')) END AS nat
  FROM insider_transactions t
  JOIN insider_filings f ON f.accession_number = t.accession_number
  JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
 WHERE NOT t.is_derivative AND f.document_type IN ('4', '4/A') AND NOT f.is_tombstone
   AND t.direct_indirect IN ('D', 'I') AND t.post_transaction_shares IS NOT NULL
   AND (t.instrument_id, nullif(btrim(t.filer_cik), '')) IN (SELECT instrument_id, filer_cik FROM c_cand)
UNION ALL
SELECT h.instrument_id, nullif(btrim(h.filer_cik), ''), m.filed_at, h.security_title,
       h.direct_indirect, h.shares,
       CASE WHEN h.direct_indirect = 'I'
            THEN lower(regexp_replace(coalesce(h.nature_of_ownership, ''), '[^a-zA-Z0-9]', '', 'g')) END
  FROM insider_initial_holdings h
  JOIN insider_filings f ON f.accession_number = h.accession_number
  JOIN sec_filing_manifest m ON m.accession_number = h.accession_number
 WHERE NOT h.is_derivative AND f.document_type IN ('3', '3/A') AND NOT f.is_tombstone
   AND h.direct_indirect IN ('D', 'I') AND h.shares IS NOT NULL
   AND (h.instrument_id, nullif(btrim(h.filer_cik), '')) IN (SELECT instrument_id, filer_cik FROM c_cand)
"""

_CROSS_FILING = """
CREATE TEMP TABLE c_xf AS
WITH firsts AS (
    SELECT DISTINCT ON (l.instrument_id, l.accession_number, l.filer_cik, l.nat)
           l.instrument_id, l.accession_number, l.filer_cik, l.nat, l.pre, l.security_title, m.filed_at
      FROM c_lines l
      JOIN c_cand c USING (instrument_id, accession_number, filer_cik)
      LEFT JOIN sec_filing_manifest m ON m.accession_number = l.accession_number
     WHERE l.di = 'I'
     ORDER BY l.instrument_id, l.accession_number, l.filer_cik, l.nat, l.txn_row_num
)
SELECT f.*,
       f.filed_at IS NULL AS no_time,
       EXISTS (SELECT 1 FROM c_hist h
                WHERE h.instrument_id = f.instrument_id AND h.filer_cik = f.filer_cik
                  AND h.filed_at < f.filed_at AND h.security_title = f.security_title
                  AND h.bal = f.pre AND h.di = 'I' AND h.nat = f.nat) AS same_form,
       EXISTS (SELECT 1 FROM c_hist h
                WHERE h.instrument_id = f.instrument_id AND h.filer_cik = f.filer_cik
                  AND h.filed_at < f.filed_at AND h.security_title = f.security_title
                  AND h.bal = f.pre AND (h.di = 'D' OR h.nat <> f.nat)) AS other_form
  FROM firsts f
 WHERE f.pre <> 0
"""

_VERDICT = """
CASE WHEN NOT complete THEN '2 incomplete'
     WHEN NOT one_title THEN '3 multi_title'
     WHEN NOT one_owner THEN '4 joint_or_owner_mismatch'
     WHEN NOT forms_chain THEN '5a form_breaks_chain'
     WHEN NOT no_cross_chain THEN '5b running_total'
     WHEN EXISTS (SELECT 1 FROM c_xf z WHERE z.instrument_id = e.instrument_id
                     AND z.accession_number = e.accession_number AND z.filer_cik = e.filer_cik
                     AND (z.no_time OR (z.other_form AND NOT z.same_form))) THEN '5c running_total_across_filings'
     WHEN NOT distinct_finals THEN '6 equal_finals'
     ELSE 'sums' END
"""

_WINNER = """
EXISTS (SELECT 1 FROM ownership_insiders_current oc
         WHERE oc.instrument_id = e.instrument_id
           AND oc.source_accession = e.accession_number
           AND oc.holder_cik = e.filer_cik
           AND oc.ownership_nature = 'indirect'
           AND oc.source = 'form4'
           AND oc.source_document_id !~ ':')
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--examples", type=int, default=0)
    args = ap.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        # One snapshot (one REPEATABLE READ transaction) for every statement below, so the
        # candidate tables and the live _current reads see the same committed state.
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SET LOCAL statement_timeout = '900s'")
        conn.execute(_LINES)
        conn.execute(_CANDIDATES)
        conn.execute(_CONDITIONS)
        conn.execute(_HIST)
        conn.execute(_CROSS_FILING)

        units = conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT instrument_id, accession_number, filer_cik FROM c_lines) s"
        ).fetchone()
        no_cik = conn.execute(
            """SELECT count(*) FROM (SELECT instrument_id, accession_number FROM c_lines
                WHERE di = 'I' AND filer_cik IS NULL
                GROUP BY 1, 2 HAVING count(DISTINCT nat) > 1) s"""
        ).fetchone()
        cand = conn.execute("SELECT count(*), count(DISTINCT instrument_id) FROM c_cand").fetchone()
        assert units is not None and no_cik is not None and cand is not None
        print(f"Form 4 / 4/A (instrument, accession, filer) units: {units[0]:,}")
        print(f"candidates (I lines under >1 normalised nature): {cand[0]:,} over {cand[1]:,} instruments")
        print(f"  excluded, null/blank filer CIK (keep today's grouping): {no_cik[0]:,}\n")

        print("verdict (first failed condition)   groups  instruments  live XML winner keys  winner instruments")
        for row in conn.execute(
            f"""SELECT {_VERDICT} AS verdict, count(*), count(DISTINCT instrument_id),
                       count(*) FILTER (WHERE {_WINNER}),
                       count(DISTINCT instrument_id) FILTER (WHERE {_WINNER})
                  FROM c_eval e GROUP BY 1 ORDER BY 1"""
        ):
            print(f"{row[0]:<34} {row[1]:>6,} {row[2]:>12,} {row[3]:>21,} {row[4]:>19,}")

        s = conn.execute(
            f"""SELECT count(*) FILTER (WHERE vehicle_sum > writer_value),
                       count(*) FILTER (WHERE vehicle_sum = writer_value),
                       count(*) FILTER (WHERE vehicle_sum < writer_value),
                       count(*) FILTER (WHERE single_line_forms),
                       count(*) FILTER (WHERE accession_number IN
                           (SELECT accession_number FROM insider_filings WHERE document_type = '4/A'))
                  FROM c_eval e WHERE {_VERDICT} = 'sums'"""
        ).fetchone()
        assert s is not None
        print(
            f"\nsums: vehicle sum > writer value {s[0]:,}; equal {s[1]:,}; below {s[2]:,}; "
            f"every form (D included) a single line {s[3]:,}; of the sums, 4/A {s[4]:,}"
        )

        # Over EVERY candidate's I forms (not a sample): how a form's opening balance relates to
        # the filer's earlier filings.  Same-form matches are the per-form reading's evidence.
        xf_cuts: list[tuple[str, LiteralString]] = [("all candidates", "TRUE"), ("sums", f"{_VERDICT} = 'sums'")]
        for label, where in xf_cuts:
            xf = conn.execute(
                f"""SELECT count(*), count(*) FILTER (WHERE z.same_form AND NOT z.other_form),
                           count(*) FILTER (WHERE z.same_form AND z.other_form),
                           count(*) FILTER (WHERE z.other_form AND NOT z.same_form),
                           count(*) FILTER (WHERE NOT z.same_form AND NOT z.other_form),
                           count(*) FILTER (WHERE z.no_time)
                      FROM c_xf z JOIN c_eval e USING (instrument_id, accession_number, filer_cik)
                     WHERE {where}"""
            ).fetchone()
            assert xf is not None
            print(
                f"{label}: I forms with a non-zero opening balance {xf[0]:,}; equal to an earlier balance of only "
                f"the same form {xf[1]:,}; of both {xf[2]:,}; of only another form {xf[3]:,}; of neither {xf[4]:,}; "
                f"no filing time {xf[5]:,}"
            )

        dera = conn.execute(
            f"""SELECT count(DISTINCT (e.instrument_id, e.accession_number, e.filer_cik)),
                       count(DISTINCT (e.instrument_id, e.accession_number, e.filer_cik)) FILTER (
                           WHERE d.source_document_id ~ ':NDH:' AND d.ownership_nature = 'direct'
                             AND o.direct_indirect = 'I')
                  FROM c_eval e
                  JOIN ownership_insiders_current d
                    ON d.instrument_id = e.instrument_id AND d.holder_cik = e.filer_cik
                   AND d.source_document_id ~ ':(NDT|NDH):'
                  LEFT JOIN ownership_insiders_observations o
                    ON o.instrument_id = d.instrument_id AND o.holder_identity_key = d.holder_identity_key
                   AND o.ownership_nature = d.ownership_nature AND o.source = d.source
                   AND o.source_document_id = d.source_document_id AND o.period_end = d.period_end
                   AND o.known_to IS NULL
                 WHERE {_VERDICT} = 'sums' AND {_WINNER}"""
        ).fetchone()
        assert dera is not None
        print(
            f"sums XML winners whose holder also has a DERA _current row on the instrument: {dera[0]:,}; "
            f"of which that row is an :NDH: 'I' line on the additive 'direct' key: {dera[1]:,}"
        )

        mnso = conn.execute(
            """SELECT count(*), count(DISTINCT oc.instrument_id)
                 FROM ownership_insiders_current oc
                 JOIN insider_filings w ON w.accession_number = oc.source_accession
                 JOIN sec_filing_manifest wm ON wm.accession_number = oc.source_accession
                WHERE oc.ownership_nature = 'indirect' AND oc.source = 'form4'
                  AND oc.source_document_id !~ ':' AND w.document_type IN ('4', '4/A')
                  AND EXISTS (
                      SELECT 1 FROM insider_initial_holdings h
                        JOIN insider_filings f ON f.accession_number = h.accession_number
                        JOIN sec_filing_manifest fm ON fm.accession_number = h.accession_number
                       WHERE f.instrument_id = oc.instrument_id
                         AND f.document_type IN ('3', '3/A')
                         AND fm.filed_at < wm.filed_at
                         AND h.filer_cik = oc.holder_cik
                         AND NOT h.is_derivative AND h.direct_indirect = 'I'
                         AND lower(regexp_replace(coalesce(h.nature_of_ownership, ''),
                                                  '[^a-zA-Z0-9]', '', 'g')) <> ''
                       GROUP BY h.accession_number
                      HAVING count(DISTINCT lower(regexp_replace(h.nature_of_ownership,
                                                                 '[^a-zA-Z0-9]', '', 'g'))) > 1)"""
        ).fetchone()
        assert mnso is not None
        print(
            f"mnso_shape candidates — live Form 4 XML 'indirect' winners whose owner FILED a Form 3 before it "
            f"naming >1 indirect vehicle: {mnso[0]:,} keys over {mnso[1]:,} instruments"
        )

        prov = conn.execute(
            """SELECT count(*), count(*) FILTER (WHERE source_document_id ~ ':NDT:')
                 FROM ownership_insiders_current WHERE source IN ('form3', 'form4')"""
        ).fetchone()
        assert prov is not None
        print(f"Form 3/4 _current rows: {prov[0]:,}; DERA :NDT: winners among them: {prov[1]:,}")

        if args.examples:
            example_cuts: list[tuple[str, LiteralString]] = [
                ("sums", f"{_VERDICT} = 'sums'"),
                ("sums, every form a single line", f"{_VERDICT} = 'sums' AND single_line_forms"),
                ("5b running_total refusals", f"{_VERDICT} = '5b running_total'"),
            ]
            for label, where in example_cuts:
                print(f"\n{label} (md5 order):")
                for row in conn.execute(
                    f"""SELECT i.symbol, e.accession_number, e.filer_cik, e.writer_value, e.vehicle_sum
                          FROM c_eval e JOIN instruments i ON i.instrument_id = e.instrument_id
                         WHERE {where}
                         ORDER BY md5(e.accession_number || e.filer_cik) LIMIT %(n)s""",
                    {"n": args.examples},
                ):
                    print(row)


if __name__ == "__main__":
    main()
