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
from pathlib import Path

import psycopg

from app.config import settings

DEFAULT_COMPANYFACTS = Path.home() / "Library/Application Support/eBull/sec/bulk/companyfacts.zip"
RATIO_TAGS = ("StockholdersEquityNoteStockSplitConversionRatio1", "StockholdersEquityNoteStockSplitConversionRatio")

UNIVERSE = """
    SELECT (jsonb_array_elements_text(universe))::bigint AS iid FROM momentum_book_universes WHERE formation_id = 1
"""


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

        # Anchor factor F^(q) = median 13F value/shares / eToro close at the last session <= q (spec "The primitive").
        # Every instrument with >= 20 13F rows for q, not only the momentum universe.
        anchors = conn.execute(
            f"""
            WITH u AS ({UNIVERSE}),
            per_filer AS (
                SELECT instrument_id, period_of_report AS q, filer_id,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY market_value_usd / shares) AS unit_value
                FROM institutional_holdings
                WHERE is_put_call IS NULL AND shares > 0 AND market_value_usd > 0 AND period_of_report >= '2024-06-30'
                GROUP BY 1, 2, 3
            ),
            h AS (
                SELECT instrument_id, q, count(*) AS n,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_value) AS raw
                FROM per_filer GROUP BY 1, 2 HAVING count(*) >= 20
            )
            SELECT h.q, i.symbol, h.n, h.raw / p.close AS f, h.instrument_id IN (SELECT iid FROM u) AS in_universe
            FROM h
            JOIN instruments i ON i.instrument_id = h.instrument_id
            JOIN LATERAL (
                SELECT close FROM price_daily d
                WHERE d.instrument_id = h.instrument_id AND d.price_date <= h.q AND d.price_date > h.q - 7
                ORDER BY d.price_date DESC LIMIT 1
            ) p ON p.close > 0
            ORDER BY 1, 4
            """
        ).fetchall()
        print("\nanchor F^ per quarter, every instrument with >= 20 distinct 13F filers and a close:")
        print("quarter | anchors | |F-1|<=2% | <=10% | ambiguous (10-20%) | split-sized | max |F-1| inside 10%")
        quarters = sorted({row[0] for row in anchors})
        for q in quarters:
            fs = [float(row[3]) for row in anchors if row[0] == q]
            dev = [abs(f - 1) for f in fs]
            ambiguous = sum(1 for f in fs if 1.1 < max(f, 1 / f) < 1.2)
            split = sum(1 for f in fs if max(f, 1 / f) >= 1.2)
            inside = max((d for d in dev if d <= 0.10), default=0.0)
            print(
                f"{q} | {len(fs)} | {sum(d <= 0.02 for d in dev)} | {sum(d <= 0.10 for d in dev)} | {ambiguous} | "
                f"{split} | {inside:.4f}"
            )
        print("formation-1 names outside 2%, per quarter:")
        for q in quarters:
            outside = [(s, n, round(float(f), 4)) for qq, s, n, f, u in anchors if qq == q and u and abs(f - 1) > 0.02]
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
