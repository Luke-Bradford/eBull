"""#2842 v2 premise: which candidate factors have point-in-time data over the pot's universe (read-only).

    PYTHONPATH=. uv run python -m scripts.measure_2842_v2_factor_premise [--as-of 2026-10-02]

The universe is S₀ as the v1 freeze defines it: every instrument in the latest committed ``v1.5-balanced`` scores
run at or before ``as_of``, restricted to tradable ``us_equity`` names. For each candidate v2 factor (the
2026-10-01 supervisor queue on #2437: insider purchases, earnings surprise, crowd, news, short volume) it prints how
many S₀ names carry an input known at ``as_of``, with the as-of column each source's own rule fixes:

- insider purchases: Form 4 open-market purchases (Table I, ``txn_code = 'P'``, non-derivative, acquired), known
  at ``sec_filing_manifest.filed_at`` (Exchange Act §16(a)(2)(C): filed within two business days of the trade).
  Also the subset whose insider traded in each of the three preceding calendar years, the population Cohen,
  Malloy & Pomorski (2012) classify as routine (same calendar month in each of those years) or opportunistic;
- earnings surprise: a seasonal-random-walk SUE input (latest fiscal quarter's diluted EPS and the same quarter a
  year earlier, plus at least eight quarters for the scale), known at ``financial_periods.filed_date``;
- crowd: the latest complete ``etoro_crowd_snapshots`` row (observed, never back-filled — history is the snapshot
  count);
- news: a sentiment-scored ``news_events`` row in the 30 days to ``as_of`` (the v1.5 sentiment family's window);
- short interest: the latest FINRA settlement date at or before ``as_of`` (``finra_short_interest_observations``);
- short volume: a RegSHO daily row on the last stored trade date (FINRA's daily flow; not short interest).

Every figure prints with its denominator. Nothing is written.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import psycopg

from app.config import settings
from app.services.scoring import _DEFAULT_MODEL_VERSION

INSIDER_WINDOWS_DAYS = (90, 180)
NEWS_WINDOW_DAYS = 30
SUE_MIN_QUARTERS = 8
SUE_FRESH_DAYS = 120
QUARTERS = ("Q1", "Q2", "Q3", "Q4")


def _universe(conn: psycopg.Connection[Any], as_of: datetime) -> tuple[datetime, list[int]]:
    row = conn.execute(
        "SELECT max(scored_at) FROM scores WHERE model_version = %s AND scored_at <= %s",
        (_DEFAULT_MODEL_VERSION, as_of),
    ).fetchone()
    if row is None or row[0] is None:
        raise SystemExit(f"no {_DEFAULT_MODEL_VERSION} scores run at or before {as_of}")
    ids = conn.execute(
        """
        SELECT DISTINCT s.instrument_id
          FROM scores s
          JOIN instruments i ON i.instrument_id = s.instrument_id
          JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE s.model_version = %s AND s.scored_at = %s AND i.is_tradable AND e.asset_class = 'us_equity'
         ORDER BY 1
        """,
        (_DEFAULT_MODEL_VERSION, row[0]),
    ).fetchall()
    return row[0], [r[0] for r in ids]


def _count(conn: psycopg.Connection[Any], sql: str, params: dict[str, Any]) -> int:
    row = conn.execute(sql, params).fetchone()  # type: ignore[arg-type]
    return 0 if row is None else int(row[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args()
    as_of = datetime.combine(args.as_of, time(23, 59, 59), tzinfo=UTC)

    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        scored_at, ids = _universe(conn, as_of)
        n = len(ids)
        p: dict[str, Any] = {"ids": ids, "as_of": as_of, "d": args.as_of}
        print(f"S0: {n} tradable us_equity names in the {_DEFAULT_MODEL_VERSION} run scored_at {scored_at}")
        print(f"as_of: {as_of}\n")

        print("insider purchases (Form 4 P, non-derivative, acquired; known at manifest filed_at)")
        for days in INSIDER_WINDOWS_DAYS:
            p["since"] = as_of - timedelta(days=days)
            names = _count(
                conn,
                """
                SELECT count(DISTINCT t.instrument_id)
                  FROM insider_transactions t
                  JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
                 WHERE t.instrument_id = ANY(%(ids)s) AND t.txn_code = 'P' AND NOT t.is_derivative
                   AND t.acquired_disposed_code = 'A' AND NOT t.txn_date_invalid
                   AND m.filed_at > %(since)s AND m.filed_at <= %(as_of)s
                """,
                p,
            )
            classifiable = _count(
                conn,
                """
                WITH buys AS (
                    SELECT t.instrument_id, t.filer_cik, t.txn_date
                      FROM insider_transactions t
                      JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
                     WHERE t.instrument_id = ANY(%(ids)s) AND t.txn_code = 'P' AND NOT t.is_derivative
                       AND t.acquired_disposed_code = 'A' AND NOT t.txn_date_invalid
                       AND m.filed_at > %(since)s AND m.filed_at <= %(as_of)s
                )
                SELECT count(DISTINCT b.instrument_id)
                  FROM buys b
                 WHERE (SELECT count(DISTINCT extract(year FROM h.txn_date))
                          FROM insider_transactions h
                          JOIN sec_filing_manifest hm ON hm.accession_number = h.accession_number
                         WHERE h.filer_cik = b.filer_cik AND NOT h.txn_date_invalid
                           AND h.txn_code IN ('P', 'S') AND hm.filed_at <= %(as_of)s
                           AND extract(year FROM h.txn_date) BETWEEN extract(year FROM b.txn_date) - 3
                                                                AND extract(year FROM b.txn_date) - 1) = 3
                """,
                p,
            )
            print(
                f"  {days}d: names with >=1 purchase {names}/{n}; "
                f"with a 3-prior-year-history purchaser {classifiable}/{n}"
            )

        print("\nearnings surprise (seasonal random walk; known at financial_periods.filed_date)")
        p["fresh"] = args.as_of - timedelta(days=SUE_FRESH_DAYS)
        p["quarters"] = list(QUARTERS)
        p["min_q"] = SUE_MIN_QUARTERS
        sue = conn.execute(
            """
            WITH q AS (
                SELECT DISTINCT ON (instrument_id, fiscal_year, period_type)
                       instrument_id, fiscal_year, period_type, eps_diluted, filed_date
                  FROM financial_periods
                 WHERE instrument_id = ANY(%(ids)s) AND period_type = ANY(%(quarters)s)
                   AND filed_date <= %(d)s
                 ORDER BY instrument_id, fiscal_year, period_type, filed_date DESC
            ), latest AS (
                SELECT DISTINCT ON (instrument_id) instrument_id, fiscal_year, period_type, eps_diluted, filed_date
                  FROM q ORDER BY instrument_id, filed_date DESC, fiscal_year DESC, period_type DESC
            )
            SELECT count(*) FILTER (WHERE l.filed_date > %(fresh)s),
                   count(*) FILTER (WHERE l.filed_date > %(fresh)s AND l.eps_diluted IS NOT NULL AND EXISTS (
                       SELECT 1 FROM q y WHERE y.instrument_id = l.instrument_id AND y.period_type = l.period_type
                          AND y.fiscal_year = l.fiscal_year - 1 AND y.eps_diluted IS NOT NULL)),
                   count(*) FILTER (WHERE l.filed_date > %(fresh)s AND l.eps_diluted IS NOT NULL AND (
                       SELECT count(*) FROM q z WHERE z.instrument_id = l.instrument_id
                          AND z.eps_diluted IS NOT NULL) >= %(min_q)s + 4)
              FROM latest l
            """,
            p,
        ).fetchone()
        assert sue is not None
        print(f"  latest quarter filed within {SUE_FRESH_DAYS}d: {sue[0]}/{n}")
        print(f"  ... with the same quarter a year earlier: {sue[1]}/{n}")
        print(f"  ... with >= {SUE_MIN_QUARTERS + 4} quarters of diluted EPS (8 seasonal differences): {sue[2]}/{n}")

        print("\ncrowd (eToro; observed snapshots only)")
        snaps = conn.execute(
            "SELECT count(*), min(started_at), max(snapshot_id) FROM etoro_crowd_snapshots "
            "WHERE status = 'complete' AND started_at <= %(as_of)s",
            p,
        ).fetchone()
        assert snaps is not None
        p["snap"] = snaps[2]
        crowd = conn.execute(
            """
            SELECT count(DISTINCT instrument_id) FILTER (WHERE holding_pct IS NOT NULL),
                   count(DISTINCT instrument_id) FILTER (WHERE popularity_uniques_7d IS NOT NULL)
              FROM etoro_crowd_observations WHERE snapshot_id = %(snap)s AND instrument_id = ANY(%(ids)s)
            """,
            p,
        ).fetchone()
        assert crowd is not None
        print(f"  complete snapshots: {snaps[0]}, first {snaps[1]}")
        print(f"  latest snapshot {snaps[2]}: holding_pct {crowd[0]}/{n}; popularity_uniques_7d {crowd[1]}/{n}")

        print(f"\nnews (sentiment-scored, {NEWS_WINDOW_DAYS}d)")
        p["news_since"] = as_of - timedelta(days=NEWS_WINDOW_DAYS)
        news_first = conn.execute("SELECT min(event_time) FROM news_events").fetchone()
        news = _count(
            conn,
            """
            SELECT count(DISTINCT instrument_id) FROM news_events
             WHERE instrument_id = ANY(%(ids)s) AND sentiment_score IS NOT NULL
               AND event_time > %(news_since)s AND event_time <= %(as_of)s
            """,
            p,
        )
        print(f"  names with >=1 scored event: {news}/{n} (corpus starts {news_first[0] if news_first else None})")

        print("\nshort interest (FINRA bimonthly; latest settlement date <= as_of)")
        settle = conn.execute(
            "SELECT max(settlement_date), min(settlement_date) FROM finra_short_interest_observations "
            "WHERE settlement_date <= %(d)s",
            p,
        ).fetchone()
        assert settle is not None
        p["settle"] = settle[0]
        si = conn.execute(
            """
            SELECT count(DISTINCT instrument_id), count(DISTINCT instrument_id) FILTER (WHERE days_to_cover IS NOT NULL)
              FROM finra_short_interest_observations
             WHERE settlement_date = %(settle)s AND instrument_id = ANY(%(ids)s)
            """,
            p,
        ).fetchone()
        assert si is not None
        print(f"  settlement {settle[0]} (corpus from {settle[1]}): names {si[0]}/{n}; days_to_cover {si[1]}/{n}")

        print("\nshort volume (FINRA RegSHO daily)")
        sv_day = conn.execute(
            "SELECT max(trade_date), min(trade_date) FROM finra_regsho_daily_observations WHERE trade_date <= %(d)s",
            p,
        ).fetchone()
        assert sv_day is not None
        p["sv_day"] = sv_day[0]
        sv = _count(
            conn,
            "SELECT count(DISTINCT instrument_id) FROM finra_regsho_daily_observations "
            "WHERE trade_date = %(sv_day)s AND instrument_id = ANY(%(ids)s)",
            p,
        )
        print(f"  trade date {sv_day[0]} (corpus from {sv_day[1]}): names {sv}/{n}")


if __name__ == "__main__":
    main()
