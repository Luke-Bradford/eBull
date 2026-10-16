"""#3841 premise measurements: share-count coverage and the split problem on the momentum universe.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` ("Problem", "The primitive"). Read-only; it reads
live dev-DB state, so every run prints its own as-of time and figures.

Prints, for the momentum book's formation-1 universe (``momentum_book_universes``):

1. share-count coverage by concept (cover ``dei:EntityCommonStockSharesOutstanding``, balance-sheet
   ``us-gaap:CommonStockSharesOutstanding``), and the names with neither;
2. every consecutive cover-count jump with ratio >= 1.25 or <= 0.8 since 2024-06, beside the
   ``StockholdersEquityNoteStockSplitConversionRatio1`` facts companyfacts.zip holds for that CIK;
   (the spec v1 evidence: these jumps cannot separate a split from a merger);
3. the eToro close either side of three known splits (price_daily is back-adjusted);
4. the 13F anchor factor distribution per quarter, over every instrument with >= 20 13F rows;
5. the price-revision heal ledger and session-close capture spans.

    PYTHONPATH=. uv run python scripts/measure_3841_premise.py [--companyfacts <companyfacts.zip>]
"""

from __future__ import annotations

import argparse
import json
import zipfile
from datetime import date, timedelta
from pathlib import Path

import psycopg

from app.config import settings
from app.services.market_calendar import us_market_status

DEFAULT_COMPANYFACTS = Path.home() / "Library/Application Support/eBull/sec/bulk/companyfacts.zip"
RATIO_TAGS = ("StockholdersEquityNoteStockSplitConversionRatio1", "StockholdersEquityNoteStockSplitConversionRatio")

UNIVERSE = """
    SELECT (jsonb_array_elements_text(universe))::bigint AS iid FROM momentum_book_universes WHERE formation_id = 1
"""


def _quarter_ends(first: date) -> list[date]:
    """Calendar quarter ends from ``first`` through the end of the current year."""
    out: list[date] = []
    year, month = first.year, first.month
    while year <= date.today().year:
        end = date(year, month, 30 if month in (6, 9) else 31)
        if end >= first:
            out.append(end)
        month += 3
        if month > 12:
            year, month = year + 1, 3
    return out


def _last_session_on_or_before(day: date) -> date:
    """The last NYSE session on or before ``day`` (``nyse-market-calendar-v1``)."""
    while us_market_status(day) == "closed":
        day -= timedelta(days=1)
    return day


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--companyfacts", type=Path, default=DEFAULT_COMPANYFACTS)
    args = parser.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        as_of = conn.execute("SELECT now()").fetchone()
        assert as_of is not None
        print(f"as of {as_of[0].isoformat()}")
        coverage = conn.execute(
            f"""
            WITH u AS ({UNIVERSE}),
            c AS (
                SELECT u.iid,
                       EXISTS (SELECT 1 FROM financial_facts_raw f WHERE f.instrument_id = u.iid
                               AND f.taxonomy = 'dei' AND f.concept = 'EntityCommonStockSharesOutstanding') AS cover,
                       EXISTS (SELECT 1 FROM financial_facts_raw f WHERE f.instrument_id = u.iid
                               AND f.taxonomy = 'us-gaap' AND f.concept = 'CommonStockSharesOutstanding'
                               AND f.filed_date >= CURRENT_DATE - 365) AS sheet_12m,
                       EXISTS (SELECT 1 FROM financial_facts_raw f WHERE f.instrument_id = u.iid) AS any_fact,
                       EXISTS (SELECT 1 FROM instrument_class_shares_outstanding s WHERE s.instrument_id = u.iid)
                           AS curated_multiclass
                FROM u
            )
            SELECT count(*), count(*) FILTER (WHERE cover),
                   count(*) FILTER (WHERE NOT cover AND sheet_12m),
                   count(*) FILTER (WHERE NOT any_fact),
                   count(*) FILTER (WHERE curated_multiclass)
            FROM c
            """
        ).fetchone()
        assert coverage is not None
        print(
            "universe {} | cover count {} | no cover, balance-sheet count filed in 12m {} | no facts at all {} | "
            "curated multi-class {}".format(*coverage)
        )
        no_cover = conn.execute(
            f"""
            WITH u AS ({UNIVERSE})
            SELECT i.symbol FROM u JOIN instruments i ON i.instrument_id = u.iid
            WHERE NOT EXISTS (SELECT 1 FROM financial_facts_raw f WHERE f.instrument_id = u.iid
                              AND f.taxonomy = 'dei' AND f.concept = 'EntityCommonStockSharesOutstanding')
            ORDER BY 1
            """
        ).fetchall()
        print(f"no cover count ({len(no_cover)}):", " ".join(r[0] for r in no_cover))
        live = conn.execute(
            f"""
            WITH u AS ({UNIVERSE})
            SELECT count(*) FROM u JOIN instrument_valuation v ON v.instrument_id = u.iid
            WHERE v.market_cap_live IS NOT NULL
            """
        ).fetchone()
        assert live is not None
        print(f"instrument_valuation.market_cap_live present: {live[0]}")

        jumps = conn.execute(
            f"""
            WITH u AS ({UNIVERSE}),
            s AS (
                SELECT DISTINCT ON (f.instrument_id, f.period_end) f.instrument_id, f.period_end, f.filed_date, f.val
                FROM financial_facts_raw f JOIN u ON u.iid = f.instrument_id
                WHERE f.taxonomy = 'dei' AND f.concept = 'EntityCommonStockSharesOutstanding'
                  AND f.period_end >= '2024-06-01' AND f.val > 0
                ORDER BY f.instrument_id, f.period_end, f.filed_date
            ),
            j AS (
                SELECT instrument_id, period_end, filed_date, val / lag(val) OVER w AS r, lag(period_end) OVER w AS prev
                FROM s WINDOW w AS (PARTITION BY instrument_id ORDER BY period_end)
            )
            SELECT i.symbol, x.identifier_value, j.prev, j.period_end, j.filed_date, round(j.r, 4)
            FROM j JOIN instruments i USING (instrument_id)
            LEFT JOIN external_identifiers x ON x.instrument_id = j.instrument_id
                 AND x.provider = 'sec' AND x.identifier_type = 'cik' AND x.is_primary
            WHERE j.r >= 1.25 OR j.r <= 0.8
            ORDER BY j.period_end
            """
        ).fetchall()
        archive = zipfile.ZipFile(args.companyfacts)
        print(f"\ncover-count jumps outside (0.8, 1.25) since 2024-06: {len(jumps)}")
        print("symbol | previous count date | count date | filed | ratio | split-ratio facts (context end, value)")
        for symbol, cik, prev, end, filed, ratio in jumps:
            facts: set[tuple[str, float]] = set()
            if cik is not None:
                try:
                    doc = json.loads(archive.read(f"CIK{int(cik):010d}.json"))
                except KeyError:
                    doc = {}
                for tag in RATIO_TAGS:
                    for values in doc.get("facts", {}).get("us-gaap", {}).get(tag, {}).get("units", {}).values():
                        facts.update((v["end"], float(v["val"])) for v in values if str(prev) < v["end"] <= str(end))
            print(f"{symbol} | {prev} | {end} | {filed} | {ratio} | {sorted(facts) or '-'}")

        print("\neToro close either side of a known split (back-adjusted series):")
        for symbol, start, stop in (
            ("NFLX", "2025-11-12", "2025-11-20"),
            ("ORLY", "2025-06-05", "2025-06-13"),
            ("FAST", "2025-05-19", "2025-05-27"),
        ):
            bars = conn.execute(
                """
                SELECT p.price_date, p.close FROM price_daily p JOIN instruments i USING (instrument_id)
                WHERE i.symbol = %s AND p.price_date BETWEEN %s AND %s ORDER BY 1
                """,
                (symbol, start, stop),
            ).fetchall()
            print(f"  {symbol}:", ", ".join(f"{d} {float(c):.2f}" for d, c in bars))

        # The spec's anchor ("Construction" / "Anchors"), measured exactly: s(q) is the last NYSE session on or before
        # q; one value per filer from its latest-filed report for (instrument, q); >= 20 filers; coherence = share of
        # filers within m <= 1.02 of the median; bands on m(F^) = max(F^, 1/F^). Every instrument, not only formation 1.
        quarter_ends = [q for q in _quarter_ends(date(2024, 6, 30)) if q <= date.today()]
        sessions = [_last_session_on_or_before(q) for q in quarter_ends]
        anchors = conn.execute(
            f"""
            WITH u AS ({UNIVERSE}),
            sq AS (SELECT * FROM unnest(%(qs)s::date[], %(ss)s::date[]) AS t(q, s)),
            latest AS (
                SELECT h.filer_id, h.instrument_id, h.period_of_report, max(h.filed_at) AS filed_at
                FROM institutional_holdings h JOIN sq ON sq.q = h.period_of_report
                GROUP BY 1, 2, 3
            ),
            per_filer AS (
                SELECT h.instrument_id, h.period_of_report AS q, h.filer_id,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY h.market_value_usd / h.shares) AS unit_value
                FROM institutional_holdings h
                JOIN latest l USING (filer_id, instrument_id, period_of_report, filed_at)
                WHERE h.is_put_call IS NULL AND h.shares > 0 AND h.market_value_usd > 0
                  AND h.shares <> 'NaN'::numeric AND h.market_value_usd <> 'NaN'::numeric
                GROUP BY 1, 2, 3
            ),
            agg AS (
                SELECT instrument_id, q, count(*) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_value) AS med
                FROM per_filer GROUP BY 1, 2 HAVING count(*) >= 20
            ),
            coh AS (
                SELECT a.instrument_id, a.q, a.n, a.med,
                       avg((greatest(p.unit_value / a.med, a.med / p.unit_value) <= 1.02)::int) AS coherent
                FROM agg a JOIN per_filer p USING (instrument_id, q)
                GROUP BY 1, 2, 3, 4
            )
            SELECT c.q, i.symbol, c.n, c.coherent, c.med / d.close AS f, c.instrument_id IN (SELECT iid FROM u)
            FROM coh c
            JOIN sq ON sq.q = c.q
            JOIN price_daily d ON d.instrument_id = c.instrument_id AND d.price_date = sq.s AND d.close > 0
            JOIN instruments i ON i.instrument_id = c.instrument_id
            ORDER BY 1, 5
            """,
            {"qs": quarter_ends, "ss": sessions},
        ).fetchall()
        print("\nanchors per quarter: >= 20 filers and a close on exactly s(q); bands on m = max(F^, 1/F^)")
        print(
            "quarter | s(q) | anchors | incoherent | ok | ok within m<=1.02 (share) | snapped m<=1.10 | "
            "ambiguous 1.10<m<1.25 | split m>=1.25 | max m-1 among snapped"
        )
        totals: list[int] = []
        for q, s in zip(quarter_ends, sessions, strict=True):
            rows = [row for row in anchors if row[0] == q]
            ok = [float(row[4]) for row in rows if float(row[3]) >= 0.75]
            m = [max(f, 1 / f) for f in ok]
            snapped = [x for x in m if x <= 1.10]
            within = sum(1 for x in m if x <= 1.02)
            share = within / len(ok) if ok else 0.0
            totals.append(len(rows))
            print(
                f"{q} | {s} | {len(rows)} | {len(rows) - len(ok)} | {len(ok)} | {within} ({share:.1%}) | "
                f"{len(snapped)} | {sum(1 for x in m if 1.10 < x < 1.25)} | {sum(1 for x in m if x >= 1.25)} | "
                f"{max(snapped, default=1.0) - 1:.4f}"
            )
        print(f"anchors per quarter range: {min(totals)}..{max(totals)}")
        print("formation-1 ok anchors with m > 1.02, per quarter:")
        for q in quarter_ends:
            outside = [
                (sym, n, round(float(f), 4))
                for qq, sym, n, coherent, f, in_universe in anchors
                if qq == q and in_universe and float(coherent) >= 0.75 and max(float(f), 1 / float(f)) > 1.02
            ]
            print(f"  {q}: {outside}")
        print("named series, F^ (distinct filers): splits, reverse splits and a spin-off (FTV, 2025-06-28)")
        for symbol in ("NFLX", "BKNG", "KLAC", "APH", "DD", "HON", "FTV", "CMCSA", "GE"):
            series = conn.execute(
                """
                WITH per_filer AS (
                    SELECT h.instrument_id, h.period_of_report AS q, h.filer_id,
                           percentile_cont(0.5) WITHIN GROUP (ORDER BY h.market_value_usd / h.shares) AS unit_value
                    FROM institutional_holdings h JOIN instruments i USING (instrument_id)
                    WHERE i.symbol = %s AND h.is_put_call IS NULL AND h.shares > 0 AND h.market_value_usd > 0
                      AND h.period_of_report >= '2024-06-30'
                    GROUP BY 1, 2, 3
                )
                SELECT f.q, count(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY f.unit_value) / max(p.close)
                FROM per_filer f
                JOIN LATERAL (
                    SELECT close FROM price_daily d
                    WHERE d.instrument_id = f.instrument_id AND d.price_date <= f.q AND d.price_date > f.q - 7
                    ORDER BY d.price_date DESC LIMIT 1
                ) p ON p.close > 0
                GROUP BY 1 ORDER BY 1
                """,
                (symbol,),
            ).fetchall()
            print(f"  {symbol}:", ", ".join(f"{q} {float(f):.3f} ({n})" for q, n, f in series))

        heal = conn.execute(
            """
            SELECT count(*), count(DISTINCT instrument_id), min(revised_at), max(revised_at)
            FROM price_daily_revision WHERE cause = 'adjustment_heal'
            """
        ).fetchone()
        captures = conn.execute(
            "SELECT count(DISTINCT instrument_id), min(session_date), max(session_date) "
            "FROM etoro_session_close_captures"
        ).fetchone()
        print(f"\nadjustment_heal revisions: rows, instruments, first, last = {heal}")
        print(f"session-close captures: instruments, first session, last session = {captures}")


if __name__ == "__main__":
    main()
