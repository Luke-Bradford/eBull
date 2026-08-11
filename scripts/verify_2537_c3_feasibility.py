"""Read-only point-in-time data census for candidate C-3 (#2537).

Run from the repository root:

    PYTHONPATH=. uv run python scripts/verify_2537_c3_feasibility.py

This script deliberately reads no forward returns and performs no factor sort.  It
asks whether the existing free corpus can support an honest historical
quality-plus-momentum trial before a specification is frozen.  The quality count
is an optimistic upper bound: it accepts either reported gross profit or
revenue-minus-cost-of-revenue and does not yet impose value validity, common-stock,
liquidity, sector, or portfolio eligibility gates.
"""

from __future__ import annotations

import sys
from datetime import date

import psycopg

from app.config import settings

DECISION_DATES = tuple(date(year, 6, 30) for year in range(2020, 2027))

_ARCHIVE_TOTAL = """
    SELECT count(*) AS series,
           count(instrument_id) AS mapped,
           count(cik) AS cik,
           count(delisting_date) AS dated_delistings,
           min(first_bar) AS first_bar,
           max(last_bar) AS last_bar
    FROM research_price_series
    WHERE comparator_snapshot_id IS NULL
"""

_ARCHIVE_BY_LAST_YEAR = """
    SELECT extract(year FROM last_bar)::int AS last_year,
           count(*) AS series,
           count(instrument_id) AS mapped
    FROM research_price_series
    WHERE comparator_snapshot_id IS NULL
    GROUP BY 1
    ORDER BY 1
"""

_MEMBERSHIP = """
    SELECT count(*) AS rows,
           count(DISTINCT instrument_id) AS instruments,
           min(effective_from) AS first_effective_from,
           max(effective_from) AS last_effective_from,
           count(*) FILTER (WHERE source_event = 'imported') AS imported
    FROM instrument_universe_membership
"""

_ANNUAL_RETENTION = """
    SELECT count(*) AS instruments,
           max(accessions) AS maximum_accessions,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY accessions) AS median_accessions,
           count(*) FILTER (WHERE accessions > 3) AS above_retention_cap
    FROM (
        SELECT instrument_id, count(DISTINCT accession_number) AS accessions
        FROM financial_facts_raw
        WHERE form_type IN ('10-K', '10-K/A')
        GROUP BY instrument_id
    ) retained
"""

# This is intentionally permissive.  Facts only need to have been filed by the
# decision date, belong to an annual filing/period ending before it, and be no
# older than 548 days.  Grouping after the decision-date predicate permits an
# earlier original filing even when a later amendment exists.  Any production
# quality measure would reject more rows, never add rows to this upper bound.
_DECISION_DATE_COVERAGE = """
    WITH decision_dates(decision_date) AS (
        SELECT unnest(%(decision_dates)s::date[])
    ),
    active AS (
        SELECT d.decision_date, s.series_id, s.instrument_id
        FROM decision_dates d
        JOIN research_price_series s
          ON s.comparator_snapshot_id IS NULL
         AND s.first_bar <= d.decision_date
         AND s.last_bar >= d.decision_date
    ),
    annual AS (
        SELECT d.decision_date,
               f.instrument_id,
               f.period_end,
               bool_or(f.concept = 'Assets') AS has_assets,
               bool_or(f.concept = 'GrossProfit')
                   OR (
                       bool_or(f.concept = ANY(%(revenue_concepts)s::text[]))
                       AND bool_or(f.concept = ANY(%(cost_concepts)s::text[]))
                   ) AS has_gross_profit
        FROM decision_dates d
        JOIN financial_facts_raw f
          ON f.filed_date <= d.decision_date
         AND f.filed_date > d.decision_date - INTERVAL '548 days'
         AND f.period_end < d.decision_date
        WHERE f.form_type IN ('10-K', '10-K/A')
          AND f.concept = ANY(%(quality_concepts)s::text[])
        GROUP BY d.decision_date, f.instrument_id, f.period_end
    ),
    quality_ready AS (
        SELECT DISTINCT decision_date, instrument_id
        FROM annual
        WHERE has_assets AND has_gross_profit
    )
    SELECT a.decision_date,
           count(*) AS active_archive,
           count(a.instrument_id) AS identity_mapped,
           count(q.instrument_id) AS quality_ready
    FROM active a
    LEFT JOIN quality_ready q
      ON q.decision_date = a.decision_date
     AND q.instrument_id = a.instrument_id
    GROUP BY a.decision_date
    ORDER BY a.decision_date
"""

_REVENUE_CONCEPTS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
)
_COST_CONCEPTS = ("CostOfGoodsAndServicesSold", "CostOfRevenue")
_QUALITY_CONCEPTS = ("Assets", "GrossProfit", *_REVENUE_CONCEPTS, *_COST_CONCEPTS)


def _pct(numerator: int, denominator: int) -> str:
    return "—" if denominator == 0 else f"{numerator / denominator:.1%}"


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        archive = conn.execute(_ARCHIVE_TOTAL).fetchone()
        last_years = conn.execute(_ARCHIVE_BY_LAST_YEAR).fetchall()
        membership = conn.execute(_MEMBERSHIP).fetchone()
        retention = conn.execute(_ANNUAL_RETENTION).fetchone()
        coverage = conn.execute(
            _DECISION_DATE_COVERAGE,
            {
                "decision_dates": list(DECISION_DATES),
                "revenue_concepts": list(_REVENUE_CONCEPTS),
                "cost_concepts": list(_COST_CONCEPTS),
                "quality_concepts": list(_QUALITY_CONCEPTS),
            },
        ).fetchall()

    if archive is None or membership is None or retention is None:
        print("REFUSED: a feasibility census returned no aggregate row", file=sys.stderr)
        return 2

    series, mapped, cik, dated_delistings, first_bar, last_bar = archive
    membership_rows, membership_instruments, membership_first, membership_last, imported = membership
    retained_instruments, maximum_accessions, median_accessions, above_cap = retention

    print("#2537 C-3 POINT-IN-TIME FEASIBILITY — NO OUTCOMES READ")
    print("\nPRICE / IDENTITY")
    print(f"archive series:                 {series:>10,}")
    print(f"identity mapped:                {mapped:>10,}  ({_pct(mapped, series)})")
    print(f"unresolved:                     {series - mapped:>10,}  ({_pct(series - mapped, series)})")
    print(f"archive CIK present:            {cik:>10,}")
    print(f"dated delistings:               {dated_delistings:>10,}")
    print(f"bar envelope:                   {first_bar} through {last_bar}")
    print("last-bar-year mapping:")
    for year, year_series, year_mapped in last_years:
        print(f"  {year}: {year_mapped:>5,}/{year_series:<5,} ({_pct(year_mapped, year_series)})")

    print("\nPOINT-IN-TIME MEMBERSHIP")
    print(f"rows / instruments:             {membership_rows:,} / {membership_instruments:,}")
    print(f"effective-from envelope:        {membership_first} through {membership_last}")
    print(f"imported (true start unknown):  {imported:,}")

    print("\nRETAINED SEC ANNUAL HISTORY")
    print(f"instruments with 10-K family:   {retained_instruments:,}")
    print(f"accessions median / maximum:    {median_accessions:g} / {maximum_accessions}")
    print(f"instruments above 3-accession cap: {above_cap:,}")

    print("\nOPTIMISTIC QUALITY INPUT UPPER BOUND")
    print(f"{'date':<12}{'active':>9}{'mapped':>10}{'quality':>10}{'archive %':>12}{'mapped %':>11}")
    for decision_date, active_count, mapped_count, quality_count in coverage:
        print(
            f"{decision_date!s:<12}{active_count:>9,}{mapped_count:>10,}{quality_count:>10,}"
            f"{_pct(quality_count, active_count):>12}{_pct(quality_count, mapped_count):>11}"
        )

    print("\nDISPOSITION: DEFER / DO NOT BACKTEST")
    print("- C-3 is a cross-sectional rank; unresolved/dead names alter every surviving rank.")
    print("- Membership begins in 2026 and imported rows explicitly do not reveal their true start.")
    print("- The hot SEC table is capped at three annual accessions, so 2020-2023 quality coverage")
    print("  is not a historical point-in-time population even under this deliberately loose upper bound.")
    print("- Today’s mapped identities or current universe must not be backfilled into prior dates.")
    print("- No parameter, return, factor sort, or portfolio outcome was inspected by this census.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
