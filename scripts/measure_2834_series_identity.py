"""#2834 part 2 — feed agreement across the two research vendors, read-only.

For every instrument carrying BOTH an ``icyDenev/Intrader`` and a
``paperswithbacktest/Stocks-Daily-Price`` series, compare the two series' daily
close-to-close returns over their common dates and report the MEDIAN absolute
difference. Pairs split into ``feeds_agree`` (median ~0) and ``feeds_disagree``
(median of the order of a daily move).

⚠ THIS IS FEED AGREEMENT, NOT AN IDENTITY CERTIFICATE. Both archives are Yahoo
derivatives (``research_corpus_ingest`` provenance), so their agreement is
circular and cannot prove one issuer. Disagreement is a flag to adjudicate, and
the adjudicator must be independent: for a series with a Form 25 link,
``sec_form25_register.issuer_cik`` against the instrument's SEC CIK. On
2026-09-22 that SEC witness showed reused tickers among the two-vendor pairs (AVX,
CAI, JCAP, NP, TGE: no overlap, the Form 25 issuer delisted 1-5 years before HF's series
starts, under a different CIK).

WHY THIS IS A MEASUREMENT AND NOT A SELECTION RULE. The engine never reads both
vendors for one name: #2721's ``universe_selection`` admits series vendor-pinned
per universe label. The reuse cases are why the two must NOT be spliced into one
instrument history. Both vendors resolve ``symbol_exact`` on the CURRENT eToro
symbol, so any instrument-keyed join from an Intrader series (shares outstanding,
fundamentals, CIK) inherits a wrong issuer where the ticker changed hands.

Usage: ``PYTHONPATH=. uv run python -m scripts.measure_2834_series_identity``
"""

from __future__ import annotations

import psycopg

from app.config import settings
from app.services.universe_selection import SURVIVOR_ONLY_VENDOR, SURVIVORSHIP_FREE_VENDOR

#: Reporting partition only (see the module docstring).
MEDIAN_ABS_DIFF_CUT = 0.005
MIN_OVERLAP_RETURNS = 20

_SQL = """
WITH pair AS (
    SELECT a.instrument_id, a.series_id AS si, b.series_id AS sh
    FROM research_price_series a
    JOIN research_price_series b USING (instrument_id)
    WHERE a.vendor = %(i)s AND b.vendor = %(h)s
), j AS (
    SELECT p.instrument_id, di.bar_date, di.close AS ci, dh.close AS ch
    FROM pair p
    JOIN research_price_daily di ON di.series_id = p.si
    JOIN research_price_daily dh ON dh.series_id = p.sh AND dh.bar_date = di.bar_date
    WHERE di.close > 0 AND dh.close > 0
), r AS (
    SELECT instrument_id,
           ci / lag(ci) OVER w - 1 AS ri,
           ch / lag(ch) OVER w - 1 AS rh
    FROM j
    WINDOW w AS (PARTITION BY instrument_id ORDER BY bar_date)
)
SELECT p.instrument_id, i.symbol, count(r.ri) AS n,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(r.ri - r.rh)) AS med
FROM pair p
JOIN instruments i USING (instrument_id)
LEFT JOIN r ON r.instrument_id = p.instrument_id AND r.ri IS NOT NULL
GROUP BY 1, 2
"""


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        # Parallel hash joins over two 76M-row reads exhaust /dev/shm on the
        # dev box (DiskFull on a shared-memory resize); serial is fine.
        conn.execute("SET max_parallel_workers_per_gather = 0")
        rows = conn.execute(_SQL, {"i": SURVIVORSHIP_FREE_VENDOR, "h": SURVIVOR_ONLY_VENDOR}).fetchall()

    thin = [r for r in rows if r[2] < MIN_OVERLAP_RETURNS]
    measured = [r for r in rows if r[2] >= MIN_OVERLAP_RETURNS]
    same = sorted((float(r[3]) for r in measured if float(r[3]) <= MEDIAN_ABS_DIFF_CUT), reverse=True)
    diff = sorted((r for r in measured if float(r[3]) > MEDIAN_ABS_DIFF_CUT), key=lambda r: float(r[3]))

    print(f"two-vendor instruments             {len(rows):,}")
    empty = sum(r[2] == 0 for r in thin)
    print(f"  insufficient overlap (n < {MIN_OVERLAP_RETURNS})   {len(thin):,}  (of which n = 0: {empty})")
    largest_same = f"{same[0]:.5f}" if same else "n/a"
    smallest_diff = f"{float(diff[0][3]):.5f}" if diff else "n/a"
    print(f"  feeds_agree (median <= {MEDIAN_ABS_DIFF_CUT})   {len(same):,}  largest median {largest_same}")
    print(f"  feeds_disagree                   {len(diff):,}  smallest median {smallest_diff}")
    for iid, symbol, n, med in diff:
        print(f"    {iid:>8} {symbol:<10} n={n:<5} median={float(med):.4f}")


if __name__ == "__main__":
    main()
