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

The audit splits unmapped instruments into three buckets so the
operator can ignore the noise and focus on real gaps:

  * ``fund_series_covered`` — instrument holds a primary ``(sec,
    class_id)`` row whose class_id is known to
    ``cik_refresh_mf_directory``. ETF/fund identity flows through
    series/class by design (#1577 — trust CIK is deliberately never
    stamped on instruments), so the missing CIK row is not a gap.
    The directory join is load-bearing: a stale/orphaned class_id
    must NOT hide a real CIK gap.
  * ``suffix_variants`` — symbol contains ``.`` (e.g. ``AAPL.RTH``,
    ``ABT.US``, ``ACLX.CVR``). These are operational duplicates;
    once #819 lands the canonical-redirect mechanism, they should
    NOT have their own CIK row — the canonical row carries it.
  * ``other`` — everything else. Funds without class bindings,
    merger CVRs, and any genuine gap. Operator must triage one by
    one.

Share-class exception (#1102): a dotted symbol ending in a single
uppercase class letter (``BRK.B``, ``CWEN.A`` — ``\\.[A-Z]$``) is a
distinct security expected to carry its own CIK, not an operational
variant. Those bucket as ``other`` so a residual miss (SEC's ticker
file lacking the dashed form) stays operator-visible instead of
hiding in the ignorable bucket. Multi-letter class suffixes
(``AXIA.PC``) are rare enough that they stay in ``suffix_variants``
— triage via the sample list if coverage stalls.

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

# True for dotted symbols that are operational variants (AAPL.RTH,
# DOW.OLD) — false for bare tickers and for share-class dots
# (BRK.B, CWEN.A: ``\.[A-Z]$``), which are expected CIK-bearing
# securities post-#1102. Static SQL fragment, single source for the
# three queries below.
_SUFFIX_VARIANT_SQL = r"(i.symbol LIKE '%%.%%' AND i.symbol !~ '\.[A-Z]$')"

# cik_refresh_mf_directory has observed-ever semantics — daily_cik_refresh
# upserts bump last_seen daily for classes still in company_tickers_mf.json,
# so a row whose last_seen stops advancing is a class SEC has dropped.
# 30 days tolerates refresh outages while still catching drops (Codex
# ckpt-2: without this, a dropped class would hide a real CIK gap in
# the by-design bucket forever).
_MF_DIRECTORY_FRESHNESS_DAYS = 30

# True when the instrument's fund identity flows through the
# series/class mechanism (#1577): a PRIMARY (sec, class_id) row whose
# class_id the mf directory has seen RECENTLY. All three predicates
# are load-bearing (Codex spec + ckpt-2 reviews): a demoted class_id,
# or one SEC has dropped from the directory, must not hide a real
# CIK gap. Freshness arrives as the ``mf_fresh_days`` query param —
# parameterising (vs interpolating the constant) keeps the composed
# query a LiteralString for pyright, per repo convention
# (def14a_drift.py / coverage.py precedents).
_FUND_SERIES_COVERED_SQL = """EXISTS (
  SELECT 1 FROM external_identifiers cl
  JOIN cik_refresh_mf_directory mf ON mf.class_id = cl.identifier_value
  WHERE cl.instrument_id = i.instrument_id
    AND cl.provider = 'sec'
    AND cl.identifier_type = 'class_id'
    AND cl.is_primary = TRUE
    AND mf.last_seen >= NOW() - make_interval(days => %(mf_fresh_days)s)
)"""


@dataclass(frozen=True)
class CikGapRow:
    """One unmapped instrument in the audit detail."""

    instrument_id: int
    symbol: str
    company_name: str | None
    category: str  # "fund_series_covered" | "suffix_variant" | "other"


@dataclass(frozen=True)
class CikCoverageGapReport:
    """Aggregate report for the audit endpoint."""

    cohort_total: int
    mapped: int
    unmapped: int
    unmapped_fund_series_covered: int
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
            f"""
            SELECT
              COUNT(*) FILTER (WHERE {_FUND_SERIES_COVERED_SQL}) AS fund_series_covered,
              COUNT(*) FILTER (WHERE NOT {_FUND_SERIES_COVERED_SQL}
                               AND {_SUFFIX_VARIANT_SQL}) AS suffix_variants,
              COUNT(*) FILTER (WHERE NOT {_FUND_SERIES_COVERED_SQL}
                               AND NOT {_SUFFIX_VARIANT_SQL}) AS other
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
            {"mf_fresh_days": _MF_DIRECTORY_FRESHNESS_DAYS},
        )
        row = cur.fetchone()
        if row is None:
            unmapped_fund_series_covered = 0
            unmapped_suffix_variants = 0
            unmapped_other = 0
        else:
            unmapped_fund_series_covered = int(row[0] or 0)
            unmapped_suffix_variants = int(row[1] or 0)
            unmapped_other = int(row[2] or 0)
        unmapped = unmapped_fund_series_covered + unmapped_suffix_variants + unmapped_other

        # Sample: prioritise "other" rows, then suffix variants, then
        # fund_series_covered (by-design, least interesting) so the
        # operator sees the genuinely interesting gaps first. Cap at
        # sample_limit to keep the JSON response bounded.
        cur.execute(
            f"""
            SELECT i.instrument_id, i.symbol, i.company_name,
                   CASE WHEN {_FUND_SERIES_COVERED_SQL} THEN 'fund_series_covered'
                        WHEN {_SUFFIX_VARIANT_SQL} THEN 'suffix_variant'
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
             ORDER BY CASE WHEN {_FUND_SERIES_COVERED_SQL} THEN 2
                           WHEN {_SUFFIX_VARIANT_SQL} THEN 1
                           ELSE 0 END,
                      i.symbol
             LIMIT %(sample_limit)s
            """,
            {
                "mf_fresh_days": _MF_DIRECTORY_FRESHNESS_DAYS,
                "sample_limit": sample_limit,
            },
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
        "cik_coverage_audit: cohort=%d mapped=%d unmapped=%d "
        "(fund_series_covered=%d suffix_variants=%d other=%d) sample=%d",
        cohort_total,
        mapped,
        unmapped,
        unmapped_fund_series_covered,
        unmapped_suffix_variants,
        unmapped_other,
        len(sample),
    )
    return CikCoverageGapReport(
        cohort_total=cohort_total,
        mapped=mapped,
        unmapped=unmapped,
        unmapped_fund_series_covered=unmapped_fund_series_covered,
        unmapped_suffix_variants=unmapped_suffix_variants,
        unmapped_other=unmapped_other,
        sample=sample,
    )
