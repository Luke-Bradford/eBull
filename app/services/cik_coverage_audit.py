"""CIK coverage audit (#1067 — PR10 leftover from #1064).

Operator-visible report of which us_equity tradable instruments are
missing a primary SEC CIK in ``external_identifiers``. Useful when:

  * A new universe sync introduces tickers the daily CIK refresh
    didn't pick up.
  * The operator suspects a high-profile ticker (TSLA, GOOGL) is
    missing — historical context for #1067, fixed post-#1102 share-
    class CIK uniqueness.
  * Diagnosing why an instrument's chart / ownership / fundamentals
    render empty: no CIK = no SEC filings ingest.

The audit splits unmapped instruments into two buckets so the
operator can ignore the noise and focus on real gaps:

  * ``suffix_variants`` — symbol contains ``.`` (e.g. ``AAPL.RTH``,
    ``ABT.US``, ``ACLX.CVR``). These are operational duplicates;
    once #819 lands the canonical-redirect mechanism, they should
    NOT have their own CIK row — the canonical row carries it.
    Legitimate share-class siblings like ``BRK.B`` DO have CIK rows
    (post-#1102) so they don't show here.
  * ``other`` — everything else. ETFs, funds, merger CVRs, and any
    genuine gap. Operator must triage one by one.

The bridge itself (ticker → CIK via SEC's ``company_tickers.json``)
already ships in ``daily_cik_refresh`` (#475); this audit surface is
the operator-side telemetry the issue body asked for.

Runbook: ``docs/wiki/runbooks/runbook-diagnosing-missing-cik.md``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CikGapRow:
    """One unmapped instrument in the audit detail."""

    instrument_id: int
    symbol: str
    company_name: str | None
    category: str  # "suffix_variant" | "other"


@dataclass(frozen=True)
class CikCoverageGapReport:
    """Aggregate report for the audit endpoint."""

    cohort_total: int
    mapped: int
    unmapped: int
    unmapped_suffix_variants: int
    unmapped_other: int
    sample: list[CikGapRow]  # capped at sample_limit


def compute_cik_gap_report(
    conn: psycopg.Connection[Any],
    *,
    sample_limit: int = 200,
) -> CikCoverageGapReport:
    """Audit query: us_equity tradable instruments without primary SEC CIK.

    Cohort matches the daily_cik_refresh producer cohort
    (``is_tradable AND e.asset_class='us_equity'``) so the gap count
    correlates 1:1 with what the bridge tried + missed. ``sample_limit``
    bounds the per-row payload so the endpoint stays fast on a large
    universe; the aggregate counters reflect the full gap regardless
    of the sample cap.

    Caller owns ``conn`` (no commit needed — read-only).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
             WHERE i.is_tradable = TRUE
               AND e.asset_class = 'us_equity'
            """,
        )
        row = cur.fetchone()
        cohort_total = int(row[0]) if row else 0

        # ``is_primary = TRUE`` is load-bearing: demoted historical
        # CIK rows must not count as "mapped" (Codex pre-push round 1
        # — without this an instrument with N historical CIKs would
        # inflate mapped past cohort_total). Mirrors the producer-side
        # primary filter in #540 (daily_cik_refresh).
        cur.execute(
            """
            SELECT COUNT(*) FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
             WHERE i.is_tradable = TRUE
               AND e.asset_class = 'us_equity'
            """,
        )
        row = cur.fetchone()
        mapped = int(row[0]) if row else 0

        # Two aggregate counters for the unmapped split. Cheaper than
        # streaming the full unmapped set just to bucket them. The
        # sample query below caps payload size.
        cur.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE i.symbol LIKE '%%.%%') AS suffix_variants,
              COUNT(*) FILTER (WHERE i.symbol NOT LIKE '%%.%%') AS other
              FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              LEFT JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
             WHERE i.is_tradable = TRUE
               AND e.asset_class = 'us_equity'
               AND ei.identifier_value IS NULL
            """,
        )
        row = cur.fetchone()
        if row is None:
            unmapped_suffix_variants = 0
            unmapped_other = 0
        else:
            unmapped_suffix_variants = int(row[0] or 0)
            unmapped_other = int(row[1] or 0)
        unmapped = unmapped_suffix_variants + unmapped_other

        # Sample: prioritise "other" rows over suffix variants so the
        # operator sees the genuinely interesting gaps first. Cap at
        # sample_limit to keep the JSON response bounded.
        cur.execute(
            """
            SELECT i.instrument_id, i.symbol, i.company_name,
                   CASE WHEN i.symbol LIKE '%%.%%' THEN 'suffix_variant'
                        ELSE 'other' END AS category
              FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              LEFT JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
             WHERE i.is_tradable = TRUE
               AND e.asset_class = 'us_equity'
               AND ei.identifier_value IS NULL
             ORDER BY CASE WHEN i.symbol LIKE '%%.%%' THEN 1 ELSE 0 END,
                      i.symbol
             LIMIT %s
            """,
            (sample_limit,),
        )
        sample = [
            CikGapRow(
                instrument_id=int(r[0]),
                symbol=str(r[1]),
                company_name=r[2] if r[2] else None,
                category=str(r[3]),
            )
            for r in cur.fetchall()
        ]

    logger.info(
        "cik_coverage_audit: cohort=%d mapped=%d unmapped=%d (suffix_variants=%d other=%d) sample=%d",
        cohort_total,
        mapped,
        unmapped,
        unmapped_suffix_variants,
        unmapped_other,
        len(sample),
    )
    return CikCoverageGapReport(
        cohort_total=cohort_total,
        mapped=mapped,
        unmapped=unmapped,
        unmapped_suffix_variants=unmapped_suffix_variants,
        unmapped_other=unmapped_other,
        sample=sample,
    )
