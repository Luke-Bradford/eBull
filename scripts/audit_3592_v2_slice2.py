"""#3592 slice 2: the spec's Appendix A full-population audits (read-only).

    PYTHONPATH=. uv run python -m scripts.audit_3592_v2_slice2 [--purchase-year 2026]

Prints, over the whole dev corpus (no sample):

- (28) open-market purchase rows by form: NULL owner / issuer CIKs, invalid dates, CIK shape, reporting owners per
  accession (Table I rows are attributed to the first-listed owner, ``sec-edgar.md`` §2.3), owner roles;
- (30, 35) amendment exposure: 4/A / 5/A purchase rows, how many repeat an original's same-day purchase, and
  original purchase keys an amendment recodes away (``ranking_pot_v2`` takes rows as filed);
- (34) ``filed_at`` − ``txn_date`` lag percentiles per form;
- history depth: open-market rows per month from the corpus start;
- (31) CMP sensitivity for pairs with a purchase in ``--purchase-year``: classification counts as if the corpus were
  complete and under the floor-aware rule (``ranking_pot_v2.classify`` with the measured coverage start), routine
  pairs that turn opportunistic when ONE history accession is missing, and opportunistic pairs whose classification
  rests on a truncated year (a calendar month carrying a trade in every covered year whose cell in the first year lies
  before the corpus's coverage start, so it is unobservable);
- (41, 48) FINRA days-to-cover conventions and values at the latest three settlement dates.

Nothing is written.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.services import ranking_pot_v2 as v2

_OPEN_MARKET: Final = """
    NOT t.is_derivative AND NOT t.txn_date_invalid
    AND ((t.txn_code = 'P' AND t.acquired_disposed_code = 'A') OR (t.txn_code = 'S' AND t.acquired_disposed_code = 'D'))
"""


def _print(conn: psycopg.Connection[Any], title: str, sql: LiteralString, params: Any = None) -> list[tuple[Any, ...]]:
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    print(f"\n## {title}")
    assert cur.description is not None
    print(" | ".join(d.name for d in cur.description))
    for r in rows:
        print(" | ".join(str(x) for x in r))
    return rows


def form4_audits(conn: psycopg.Connection[Any]) -> None:
    _print(
        conn,
        "(28) open-market purchase rows (P, non-derivative) by form",
        """
        SELECT f.document_type, count(*) AS rows,
               count(*) FILTER (WHERE t.acquired_disposed_code IS DISTINCT FROM 'A') AS not_a,
               count(*) FILTER (WHERE t.filer_cik IS NULL) AS null_owner,
               count(*) FILTER (WHERE f.issuer_cik IS NULL) AS null_issuer,
               count(*) FILTER (WHERE t.filer_cik !~ '^[0-9]{10}$' OR f.issuer_cik !~ '^[0-9]{10}$') AS bad_cik_shape,
               count(*) FILTER (WHERE t.txn_date_invalid) AS invalid_date
          FROM insider_transactions t JOIN insider_filings f USING (accession_number)
         WHERE t.txn_code = 'P' AND NOT t.is_derivative
         GROUP BY 1 ORDER BY 2 DESC
        """,
    )
    _print(
        conn,
        "(28) reporting owners per accession carrying a purchase",
        """
        WITH p AS (
            SELECT DISTINCT accession_number FROM insider_transactions WHERE txn_code = 'P' AND NOT is_derivative
        )
        SELECT LEAST(coalesce(n, 0), 3) AS owners_capped_at_3, count(*) AS accessions
          FROM p LEFT JOIN (SELECT accession_number, count(DISTINCT filer_cik) AS n FROM insider_filers GROUP BY 1) o
               USING (accession_number)
         GROUP BY 1 ORDER BY 1
        """,
    )
    _print(
        conn,
        "(28) purchase rows by the attributed owner's role flags",
        """
        SELECT coalesce(fl.is_director, false) OR coalesce(fl.is_officer, false) AS director_or_officer,
               coalesce(fl.is_ten_percent_owner, false) AS ten_pct, coalesce(fl.is_other, false) AS other, count(*)
          FROM insider_transactions t
          JOIN insider_filers fl ON fl.accession_number = t.accession_number AND fl.filer_cik = t.filer_cik
         WHERE t.txn_code = 'P' AND NOT t.is_derivative
         GROUP BY 1, 2, 3 ORDER BY 4 DESC
        """,
    )
    _print(
        conn,
        "(30, 35) amendment purchase rows vs originals",
        """
        WITH a AS (
            SELECT f.issuer_cik, t.filer_cik, t.txn_date, t.shares
              FROM insider_transactions t JOIN insider_filings f USING (accession_number)
             WHERE f.document_type IN ('4/A', '5/A') AND t.txn_code = 'P' AND t.acquired_disposed_code = 'A'
               AND NOT t.is_derivative
        ), o AS (
            SELECT f.issuer_cik, t.filer_cik, t.txn_date, t.shares
              FROM insider_transactions t JOIN insider_filings f USING (accession_number)
             WHERE f.document_type IN ('4', '5') AND t.txn_code = 'P' AND NOT t.is_derivative
        )
        SELECT count(*) AS amendment_rows,
               count(*) FILTER (WHERE EXISTS (SELECT 1 FROM o WHERE (o.issuer_cik, o.filer_cik, o.txn_date)
                                               = (a.issuer_cik, a.filer_cik, a.txn_date))) AS original_same_day,
               count(*) FILTER (WHERE EXISTS (SELECT 1 FROM o WHERE (o.issuer_cik, o.filer_cik, o.txn_date, o.shares)
                                               = (a.issuer_cik, a.filer_cik, a.txn_date, a.shares))) AS identical
          FROM a
        """,
    )
    _print(
        conn,
        "(30, 35) heuristic: original qualifying-purchase keys whose same-date amendment lines include none that "
        "qualifies (an indicator of a correction, NOT a measured correction count: owner/issuer/date cannot tell which "
        "line an amendment corrects)",
        """
        WITH o AS (
            SELECT DISTINCT f.issuer_cik, t.filer_cik, t.txn_date
              FROM insider_transactions t JOIN insider_filings f USING (accession_number)
             WHERE f.document_type IN ('4', '5') AND t.txn_code = 'P' AND t.acquired_disposed_code = 'A'
               AND NOT t.is_derivative AND NOT t.txn_date_invalid
        ), am AS (
            SELECT f.issuer_cik, t.filer_cik, t.txn_date,
                   bool_or(t.txn_code = 'P' AND t.acquired_disposed_code = 'A' AND NOT t.is_derivative
                           AND NOT t.txn_date_invalid) AS has_p,
                   bool_or(NOT (t.txn_code = 'P' AND t.acquired_disposed_code = 'A' AND NOT t.is_derivative
                                AND NOT t.txn_date_invalid)) AS has_other
              FROM insider_transactions t JOIN insider_filings f USING (accession_number)
             WHERE f.document_type IN ('4/A', '5/A') GROUP BY 1, 2, 3
        )
        SELECT count(*) AS original_keys,
               count(*) FILTER (WHERE am.has_other AND NOT am.has_p) AS recoded_by_amendment
          FROM o LEFT JOIN am USING (issuer_cik, filer_cik, txn_date)
        """,
    )
    _print(
        conn,
        "(34) filed_at - txn_date (days) for qualifying purchases, by form",
        """
        SELECT f.document_type,
               percentile_disc(ARRAY[0.5, 0.9, 0.95, 0.99])
                   WITHIN GROUP (ORDER BY m.filed_at::date - t.txn_date) AS p50_90_95_99,
               count(*) AS n, count(*) FILTER (WHERE m.filed_at::date - t.txn_date > 6) AS over_6d,
               count(*) FILTER (WHERE m.filed_at::date < t.txn_date) AS negative
          FROM insider_transactions t
          JOIN sec_filing_manifest m USING (accession_number) JOIN insider_filings f USING (accession_number)
         WHERE t.txn_code = 'P' AND t.acquired_disposed_code = 'A' AND NOT t.is_derivative AND NOT t.txn_date_invalid
         GROUP BY 1 ORDER BY 3 DESC
        """,
    )
    _print(
        conn,
        "history depth: open-market rows per month (first 15 months with any row since 2022)",
        f"""
        SELECT to_char(date_trunc('month', t.txn_date), 'YYYY-MM') AS month, count(*) AS rows
          FROM insider_transactions t WHERE {_OPEN_MARKET} AND t.txn_date >= '2022-01-01'
         GROUP BY 1 ORDER BY 1 LIMIT 15
        """,
    )


def _coverage_start(conn: psycopg.Connection[Any]) -> date:
    """§8's ``history_floor`` (``ranking_pot_v2.history_floor``) over usable history rows, freeze month = this month."""
    rows = conn.execute(
        f"""
        SELECT date_trunc('month', t.txn_date)::date, count(*)
          FROM insider_transactions t
          JOIN insider_filings f USING (accession_number) JOIN sec_filing_manifest m USING (accession_number)
         WHERE {_OPEN_MARKET} AND t.filer_cik IS NOT NULL AND f.issuer_cik IS NOT NULL AND m.filed_at IS NOT NULL
         GROUP BY 1
        """
    ).fetchall()
    floor = v2.history_floor({m: n for m, n in rows}, freeze_month=date.today().replace(day=1))
    if isinstance(floor, str):
        raise SystemExit(floor)
    return floor


def cmp_sensitivity(conn: psycopg.Connection[Any], year: int) -> None:
    start = _coverage_start(conn)
    print(f"\n## (31) CMP classification of pairs with a {year} purchase; corpus coverage starts {start}")
    rows = conn.execute(
        f"""
        WITH pairs AS (
            SELECT DISTINCT t.filer_cik, f.issuer_cik
              FROM insider_transactions t JOIN insider_filings f USING (accession_number)
             WHERE t.txn_code = 'P' AND t.acquired_disposed_code = 'A' AND NOT t.is_derivative
               AND NOT t.txn_date_invalid AND extract(year FROM t.txn_date) = %(y)s
               AND t.filer_cik IS NOT NULL AND f.issuer_cik IS NOT NULL
        )
        SELECT t.id, t.accession_number, t.instrument_id, t.filer_cik, f.issuer_cik, t.txn_date, t.txn_code,
               t.acquired_disposed_code, t.is_derivative, t.txn_date_invalid, m.filed_at
          FROM pairs p
          JOIN insider_filings f ON f.issuer_cik = p.issuer_cik
          JOIN insider_transactions t ON t.accession_number = f.accession_number AND t.filer_cik = p.filer_cik
          JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
         WHERE {_OPEN_MARKET} AND t.txn_date >= make_date(%(y)s - 3, 1, 1) AND t.txn_date < make_date(%(y)s, 1, 1)
        """,
        {"y": year},
    ).fetchall()
    pair_ids = conn.execute(
        """
        SELECT DISTINCT t.filer_cik, f.issuer_cik
          FROM insider_transactions t JOIN insider_filings f USING (accession_number)
         WHERE t.txn_code = 'P' AND t.acquired_disposed_code = 'A' AND NOT t.is_derivative
           AND NOT t.txn_date_invalid AND extract(year FROM t.txn_date) = %(y)s
           AND t.filer_cik IS NOT NULL AND f.issuer_cik IS NOT NULL
        """,
        {"y": year},
    ).fetchall()
    history: dict[tuple[str, str], list[v2.InsiderRow]] = defaultdict(list)
    for r in rows:
        history[(r[3], r[4])].append(v2.InsiderRow(*r))
    counts: dict[str, int] = defaultdict(int)
    flips_one_filing = 0
    truncated_risk = 0
    for filer, issuer in pair_ids:
        hist = history.get((filer, issuer), [])
        key: v2.PairKey = (filer, issuer, year)
        cells = v2.pair_cells(hist, key)
        cls = v2.classify(cells, year, history_floor=date(year - v2.CMP_HISTORY_YEARS, 1, 1))
        counts[cls] += 1
        counts[f"floor-aware {v2.classify(cells, year, history_floor=start)}"] += 1
        if cls == "routine":
            accessions = {h.accession for h in hist}
            if any(
                v2.classify(
                    v2.pair_cells([h for h in hist if h.accession != a], key),
                    year,
                    history_floor=date(year - v2.CMP_HISTORY_YEARS, 1, 1),
                )
                == "opportunistic"
                for a in accessions
            ):
                flips_one_filing += 1
        elif cls == "opportunistic":
            first, *later = range(year - v2.CMP_HISTORY_YEARS, year)
            common_later = set.intersection(*({m for y, m in cells if y == yy} for yy in later))
            hidden = {m for m in range(1, 13) if date(first, m, 1) < start}
            if common_later & hidden:
                truncated_risk += 1
    print(f"pairs {len(pair_ids)} · " + " · ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    print(f"routine pairs that turn opportunistic when one history accession is missing: {flips_one_filing}")
    print(
        "opportunistic pairs whose later years share a month that is unobservable in the first year "
        f"(could be routine on full history): {truncated_risk}"
    )


def dtc_audits(conn: psycopg.Connection[Any]) -> None:
    _print(
        conn,
        "(41) DTC vs short interest / average daily volume, whole series",
        """
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE days_to_cover IS NULL) AS null_dtc,
               count(*) FILTER (WHERE days_to_cover < 0) AS negative,
               count(*) FILTER (WHERE days_to_cover = 0) AS zero,
               count(*) FILTER (WHERE days_to_cover = 0 AND current_short_interest = 0) AS zero_with_zero_si,
               count(*) FILTER (WHERE current_short_interest = 0) AS zero_si,
               count(*) FILTER (WHERE current_short_interest = 0 AND days_to_cover IS DISTINCT FROM 1) AS zero_si_not_1,
               count(*) FILTER (WHERE days_to_cover = 1) AS at_floor_1,
               count(*) FILTER (WHERE days_to_cover = 999.99) AS at_cap_999_99,
               count(*) FILTER (WHERE days_to_cover > 999.99) AS above_cap,
               count(*) FILTER (WHERE average_daily_volume = 0) AS zero_adv,
               count(*) FILTER (WHERE average_daily_volume = 0
                                  AND days_to_cover IS DISTINCT FROM 999.99) AS zero_adv_not_999_99,
               count(*) FILTER (WHERE average_daily_volume IS NULL OR current_short_interest IS NULL) AS null_inputs,
               count(*) FILTER (WHERE average_daily_volume > 0 AND current_short_interest > 0
                                  AND days_to_cover IS DISTINCT FROM LEAST(999.99, GREATEST(1, round(
                                      current_short_interest::numeric / average_daily_volume, 2)))) AS off_rule
          FROM finra_short_interest_observations
        """,
    )
    _print(
        conn,
        "(48) latest three settlement dates (bounded to these dates only)",
        """
        WITH d AS (SELECT DISTINCT settlement_date FROM finra_short_interest_observations ORDER BY 1 DESC LIMIT 3)
        SELECT o.settlement_date, count(*) AS rows, count(DISTINCT o.instrument_id) AS names,
               count(*) FILTER (WHERE days_to_cover IS NULL) AS null_dtc,
               count(*) FILTER (WHERE days_to_cover < 0) AS negative,
               count(*) FILTER (WHERE days_to_cover = 0) AS zero,
               count(*) FILTER (WHERE days_to_cover = 1) AS at_floor_1,
               count(*) FILTER (WHERE days_to_cover = 999.99) AS at_cap,
               count(*) FILTER (WHERE revision_flag = 'R') AS revised,
               count(*) - count(DISTINCT o.instrument_id) AS extra_revisions,
               max(known_from) AS max_known_from
          FROM finra_short_interest_observations o JOIN d USING (settlement_date)
         GROUP BY 1 ORDER BY 1 DESC
        """,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--purchase-year", type=int, default=2026)
    args = parser.parse_args()
    with psycopg.connect(settings.database_url) as conn, conn.transaction(force_rollback=True):
        conn.execute("SET TRANSACTION READ ONLY")
        form4_audits(conn)
        cmp_sensitivity(conn, args.purchase_year)
        dtc_audits(conn)


if __name__ == "__main__":
    main()
