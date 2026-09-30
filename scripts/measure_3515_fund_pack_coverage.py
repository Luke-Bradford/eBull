"""#3515 premise measurement: the fundamentals block over the shortlist-ELIGIBLE population.

Read-only, one REPEATABLE READ snapshot, one ``as_of`` (= the snapshot's ``now()``).
Population = the production §3.1 eligibility: ``ai_trial_pack_reader.read_scores_run`` at
``as_of``, the ``read_shortlist`` candidate query, ``drop_symbol_collisions`` and
``ai_trial_pack.is_eligible`` — the same functions the decision job calls.

Applies the spec §3 report rule (candidate = an original-form ``filing_events`` row, exactly one per
accession, ``created_at <= as_of``, ``report_date`` set and not after ``as_of``, with >= 1 finite
us-gaap K fact ``fetched_at <= as_of`` whose ``period_end`` is the report date; ordered by report
date, filing date, accession) and reports:
- names with >= 1 selected report; report_date coverage of the candidate accessions;
- facts per selected report (max / p99 — the ``FACTS_PER_REPORT_MAX`` premise);
- names per delivered concept, and names with either revenue concept;
- non-finite ``val`` among K rows;
- ingestion lag ``min(fetched_at) - filed_date`` over the selected reports filed in the last 120 days;
- whether each name's latest original 10-K/10-Q ``filing_events`` row (the text target) is the
  newest selected report.

    PYTHONPATH=. uv run python -m scripts.measure_3515_fund_pack_coverage
"""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.ai_trial_pack import ShortlistCandidate, is_eligible
from app.services.ai_trial_pack_reader import _decimal, drop_symbol_collisions, read_scores_run

K = [
    ("us-gaap", "Revenues"),
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
    ("us-gaap", "GrossProfit"),
    ("us-gaap", "OperatingIncomeLoss"),
    ("us-gaap", "NetIncomeLoss"),
    ("us-gaap", "EarningsPerShareDiluted"),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
    ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
    ("us-gaap", "Assets"),
    ("us-gaap", "Liabilities"),
    ("us-gaap", "StockholdersEquity"),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
    ("us-gaap", "LongTermDebtNoncurrent"),
    ("us-gaap", "CommonStockSharesOutstanding"),
    ("dei", "EntityCommonStockSharesOutstanding"),
]
ANNUAL = ["10-K", "10-KT"]
QUARTERLY = ["10-Q", "10-QT"]


def _eligible_ids(conn: psycopg.Connection, as_of) -> list[int]:  # type: ignore[type-arg]
    run = read_scores_run(conn, as_of=as_of)
    assert run is not None, "no scores run at as_of"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, i.symbol, q.bid, q.ask, q.quoted_at, s.total_score
              FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              JOIN scores s ON s.instrument_id = i.instrument_id
                           AND s.model_version = %(mv)s AND s.scored_at = %(scored_at)s
              LEFT JOIN quotes q ON q.instrument_id = i.instrument_id
             WHERE i.is_tradable AND e.asset_class = 'us_equity'
            """,
            {"mv": run.model_version, "scored_at": run.scored_at},
        )
        candidates = [
            ShortlistCandidate(
                instrument_id=int(r["instrument_id"]),
                symbol=str(r["symbol"]),
                bid=_decimal(r["bid"]),
                ask=_decimal(r["ask"]),
                quoted_at=r["quoted_at"],
                total_score=float(r["total_score"]) if r["total_score"] is not None else None,
                market_cap_usd=None,
            )
            for r in cur.fetchall()
        ]
    print("scores run:", run.model_version, run.scored_at)
    return [c.instrument_id for c in drop_symbol_collisions(candidates) if is_eligible(c, as_of=as_of)]


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        conn.execute("SET statement_timeout = '900s'")
        as_of = conn.execute("SELECT now()").fetchone()[0]  # type: ignore[index]
        print("as_of:", as_of)
        ids = _eligible_ids(conn, as_of)
        print("eligible names:", len(ids))
        conn.execute("CREATE TEMP TABLE elig AS SELECT unnest(%s::bigint[]) AS instrument_id", (ids,))
        conn.execute(
            "CREATE TEMP TABLE k (taxonomy text, concept text)",
        )
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO k VALUES (%s, %s)", K)
        conn.execute(
            """
            CREATE TEMP TABLE ev AS
            SELECT fe.instrument_id, fe.provider_filing_id AS accession_number,
                   count(*) AS n_rows, min(fe.filing_type) AS form_type, min(fe.filing_date) AS filing_date,
                   min(fe.report_date) AS report_date, count(DISTINCT fe.report_date) AS n_report_dates
              FROM filing_events fe JOIN elig USING (instrument_id)
             WHERE fe.filing_type = ANY(%(forms)s) AND fe.created_at <= %(as_of)s
             GROUP BY 1, 2
            """,
            {"forms": ANNUAL + QUARTERLY, "as_of": as_of},
        )
        print(
            "original-form events: accessions, >1 event row, NULL report_date, report_date after as_of:",
            conn.execute(
                "SELECT count(*), count(*) FILTER (WHERE n_rows > 1), count(*) FILTER (WHERE report_date IS NULL), "
                "count(*) FILTER (WHERE report_date > %(d)s::date) FROM ev",
                {"d": as_of},
            ).fetchone(),
        )
        print(
            "K concept names stored under another taxonomy (collision check):",
            conn.execute(
                "SELECT count(*) FROM financial_facts_raw r JOIN elig USING (instrument_id) "
                "WHERE r.concept IN (SELECT concept FROM k) AND (r.taxonomy, r.concept) NOT IN (SELECT * FROM k)"
            ).fetchone(),
        )
        conn.execute(
            """
            CREATE TEMP TABLE kf AS
            SELECT r.fact_id, r.instrument_id, r.taxonomy, r.concept, r.period_end, r.accession_number,
                   r.fetched_at, r.val
              FROM financial_facts_raw r
              JOIN k USING (taxonomy, concept)
              JOIN ev ON ev.instrument_id = r.instrument_id AND ev.accession_number = r.accession_number
             WHERE r.fetched_at <= %(as_of)s
            """,
            {"as_of": as_of},
        )
        print(
            "non-finite val among K rows (NaN, +/-Infinity):",
            conn.execute("SELECT count(*) FROM kf WHERE val IN ('NaN', 'Infinity', '-Infinity')").fetchone(),
        )
        conn.execute(
            """
            CREATE TEMP TABLE sel AS
            WITH cand AS (
              SELECT ev.*, CASE WHEN ev.form_type = ANY(%(annual)s) THEN 'a' ELSE 'q' END AS kind,
                     (SELECT count(*) FROM kf WHERE kf.instrument_id = ev.instrument_id
                         AND kf.accession_number = ev.accession_number AND kf.period_end <= ev.report_date
                         AND kf.val NOT IN ('NaN', 'Infinity', '-Infinity'))
                       AS n_facts
                FROM ev
               WHERE n_rows = 1 AND report_date IS NOT NULL AND report_date <= %(d)s::date
                 AND EXISTS (SELECT 1 FROM kf WHERE kf.instrument_id = ev.instrument_id
                               AND kf.accession_number = ev.accession_number AND kf.taxonomy = 'us-gaap'
                               AND kf.period_end = ev.report_date
                               AND kf.val NOT IN ('NaN', 'Infinity', '-Infinity'))),
            ranked AS (
              SELECT c.*, row_number() OVER (PARTITION BY instrument_id, kind
                                             ORDER BY report_date DESC, filing_date DESC, accession_number DESC) AS rn
                FROM cand c)
            SELECT r.* FROM ranked r
             WHERE rn = 1
               AND (kind = 'a' OR NOT EXISTS (
                      SELECT 1 FROM ranked a2 WHERE a2.instrument_id = r.instrument_id AND a2.kind = 'a'
                         AND a2.rn = 1 AND a2.report_date >= r.report_date))
            """,
            {"annual": ANNUAL, "d": as_of},
        )
        print(
            "names with >=1 selected report; annual shown; quarterly shown:",
            conn.execute(
                "SELECT count(DISTINCT instrument_id), count(*) FILTER (WHERE kind = 'a'), "
                "count(*) FILTER (WHERE kind = 'q') FROM sel"
            ).fetchone(),
        )
        print(
            "facts per selected report: p50, p99, max:",
            conn.execute(
                "SELECT percentile_cont(ARRAY[0.5, 0.99]) WITHIN GROUP (ORDER BY n_facts), max(n_facts) FROM sel"
            ).fetchone(),
        )
        for kind in ("a", "q"):
            print(
                f"report age days, kind={kind}: p50, p90, max:",
                conn.execute(
                    "SELECT percentile_cont(ARRAY[0.5, 0.9]) WITHIN GROUP (ORDER BY %(d)s::date - report_date), "
                    "max(%(d)s::date - report_date) FROM sel WHERE kind = %(k)s",
                    {"d": as_of, "k": kind},
                ).fetchone(),
            )
        print(
            "K facts in shown reports with period_end after report_date, by taxonomy:",
            conn.execute(
                "SELECT kf.taxonomy, count(*), max(kf.period_end - sel.report_date) FROM kf "
                "JOIN sel USING (instrument_id, accession_number) "
                "WHERE kf.period_end > sel.report_date GROUP BY 1 ORDER BY 1"
            ).fetchall(),
        )
        print(
            "max facts for one concept in one shown report:",
            conn.execute(
                "SELECT max(n) FROM (SELECT count(*) n FROM kf JOIN sel USING (instrument_id, accession_number) "
                "WHERE kf.period_end <= sel.report_date GROUP BY kf.instrument_id, kf.accession_number, kf.concept) x"
            ).fetchone(),
        )
        print(
            "names flagged newer_report_without_facts (per kind: newest valid event of the kind is not the shown one):",
            conn.execute(
                """
                WITH newest AS (
                  SELECT DISTINCT ON (instrument_id, kind) instrument_id, kind, accession_number FROM (
                    SELECT ev.*, CASE WHEN form_type = ANY(%(annual)s) THEN 'a' ELSE 'q' END AS kind FROM ev
                     WHERE n_rows = 1 AND report_date IS NOT NULL AND report_date <= %(d)s::date) e
                   ORDER BY instrument_id, kind, report_date DESC, filing_date DESC, accession_number DESC)
                SELECT count(DISTINCT n.instrument_id) FROM newest n
                  LEFT JOIN sel s ON s.instrument_id = n.instrument_id AND s.kind = n.kind
                 WHERE s.accession_number IS DISTINCT FROM n.accession_number
                   AND NOT (n.kind = 'q' AND EXISTS (SELECT 1 FROM sel a WHERE a.instrument_id = n.instrument_id
                            AND a.kind = 'a' AND a.report_date >= (SELECT report_date FROM ev
                             WHERE ev.instrument_id = n.instrument_id AND ev.accession_number = n.accession_number)))
                """,
                {"annual": ANNUAL, "d": as_of},
            ).fetchone(),
        )
        for taxonomy, concept in K:
            n = conn.execute(
                "SELECT count(DISTINCT r.instrument_id) FROM financial_facts_raw r "
                "JOIN sel USING (instrument_id, accession_number) "
                "WHERE r.taxonomy = %s AND r.concept = %s AND r.fetched_at <= %s",
                (taxonomy, concept, as_of),
            ).fetchone()
            print(f"  delivered {taxonomy}:{concept}: {n}")
        print(
            "names with either revenue concept delivered; with both:",
            conn.execute(
                """
                WITH r AS (SELECT r.instrument_id, count(DISTINCT r.concept) c
                             FROM financial_facts_raw r JOIN sel USING (instrument_id, accession_number)
                            WHERE r.taxonomy = 'us-gaap' AND r.fetched_at <= %s
                              AND r.concept IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax')
                            GROUP BY 1)
                SELECT count(*), count(*) FILTER (WHERE c = 2) FROM r
                """,
                (as_of,),
            ).fetchone(),
        )
        print(
            "ingestion lag days, selected reports filed <= 120d (first surviving fetched_at): count, [p50, p90, p99]:",
            conn.execute(
                """
                SELECT count(*), percentile_cont(ARRAY[0.5, 0.9, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM
                       (SELECT min(kf.fetched_at) FROM kf WHERE kf.instrument_id = sel.instrument_id
                           AND kf.accession_number = sel.accession_number)
                       - (sel.filing_date::timestamp AT TIME ZONE 'America/New_York')) / 86400)
                  FROM sel WHERE filing_date >= %(d)s::date - 120
                """,
                {"d": as_of},
            ).fetchone(),
        )
        print(
            "text target (newest original event) equals newest shown report: names with target, equal:",
            conn.execute(
                """
                WITH t AS (
                  SELECT DISTINCT ON (instrument_id) instrument_id, accession_number FROM ev
                   WHERE n_rows = 1 AND report_date IS NOT NULL AND report_date <= %(d)s::date
                   ORDER BY instrument_id, report_date DESC, filing_date DESC, accession_number DESC),
                n AS (
                  SELECT DISTINCT ON (instrument_id) instrument_id, accession_number FROM sel
                   ORDER BY instrument_id, report_date DESC)
                SELECT count(*), count(*) FILTER (WHERE n.accession_number = t.accession_number)
                  FROM t LEFT JOIN n USING (instrument_id)
                """,
                {"d": as_of},
            ).fetchone(),
        )
        conn.rollback()


if __name__ == "__main__":
    main()
