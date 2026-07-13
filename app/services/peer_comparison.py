"""
Peer-comparison data layer (#1751; SIC re-key #2023) — unblocks #594.

Derives, per instrument, the radar factors (P/E, ROE, revenue growth YoY,
operating margin, debt/equity, net margin), their cohort medians, and a peer
set — entirely from EXISTING tables (no new ingest):

  * price-free factors from ``financial_periods_ttm`` (is_complete_ttm),
  * ``pe_ratio`` from the ``instrument_valuation`` view (price-gated → thin on
    dev; flagged ``dev_limited`` by the caller),
  * ``revenue_growth_yoy`` from the two most recent canonical FY rows in
    ``financial_periods``, with a consecutive-year day-gap guard,
  * cohort medians via ``percentile_cont`` over the SIC cohort's complete-TTM set,
  * peer set = same SIC cohort, nearest by size proximity (``total_assets``;
    market cap is price-gated so unusable broadly).

Cohort key (#2023): SEC SIC walked 4->3->2 to the narrowest level with
``MIN_COHORT`` peers — the SAME key + walk as ``fair_value_band.peer_pct_for``
(``fair_value_band.py:604-707``), replacing the old eToro ``instruments.sector``
exact-match (9 opaque codes, 26% missing). SIC lives in
``instrument_sec_profile`` (``sic`` + generated ``sic3``/``sic2`` STORED cols,
sql/221). Unlike the band (which goes comparator-absent under threshold),
peer_comparison is a DISCLOSURE surface: under threshold it widens to SIC-2 and
renders thin (``cohort_sic_level == 0``), never absent.

Factor formulas MIRROR ``instrument_valuation`` (sql/201) — do not re-derive.
The view's ~32-row ceiling is its live-price join; bypassed here by reading
``financial_periods_ttm`` directly for the price-free factors.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import LiteralString, cast

import psycopg
from psycopg.rows import dict_row

_PEER_LIMIT = 8

# Narrowest SIC cohort must hold >= MIN_COHORT PEERS (self already excluded) to
# resolve at that granularity; else the walk widens. Matches fair_value_band
# MIN_PEERS=8 so the two keys agree on walk philosophy.
MIN_COHORT = 8

# SIC column per walk level — a FROZEN whitelist, never interpolated from input
# (injection-safe, same pattern as fair_value_band._MEMBER_SQL). sic = full
# 4-digit; sic3/sic2 = generated STORED prefixes (sql/221).
_SIC_COL: dict[int, str] = {4: "sp.sic", 3: "sp.sic3", 2: "sp.sic2"}


def resolve_sic_level(n4: int, n3: int, n2: int, min_cohort: int) -> tuple[int, int]:
    """
    Pick the cohort SIC granularity from the three self-excluded peer counts.

    Pure policy (no I/O) — table-tested. ``n4``/``n3``/``n2`` are the peer counts
    (self already excluded) at SIC-4 / SIC-3 / SIC-2. Returns
    ``(column_level, disclosure_marker)``:

      * ``column_level`` ∈ {4, 3, 2} — which ``_SIC_COL`` to filter the cohort on.
      * ``disclosure_marker`` ∈ {4, 3, 2, 0} — the value surfaced as
        ``cohort_sic_level``. 4/3/2 = that granularity cleared ``min_cohort``;
        **0** = none cleared, widened to SIC-2 (render thin), mirroring
        ``fair_value_band``'s ``sic_level=0``.
    """
    if n4 >= min_cohort:
        return 4, 4
    if n3 >= min_cohort:
        return 3, 3
    if n2 >= min_cohort:
        return 2, 2
    return 2, 0  # widened SIC-2 fallback, below threshold → thin


# Factor keys in #594 radar order. Each: (key, label, better_when).
FACTOR_KEYS: tuple[str, ...] = (
    "pe_ratio",
    "roe",
    "revenue_growth_yoy",
    "operating_margin",
    "debt_equity_ratio",
    "net_margin",
)
FACTOR_LABELS: dict[str, str] = {
    "pe_ratio": "P/E (TTM)",
    "roe": "ROE",
    "revenue_growth_yoy": "Revenue growth YoY",
    "operating_margin": "Operating margin",
    "debt_equity_ratio": "Debt / equity",
    "net_margin": "Net margin",
}
# "higher" = a larger value is better for a long investor; "lower" = smaller is
# better (cheaper / less levered).
FACTOR_BETTER_WHEN: dict[str, str] = {
    "pe_ratio": "lower",
    "roe": "higher",
    "revenue_growth_yoy": "higher",
    "operating_margin": "higher",
    "debt_equity_ratio": "lower",
    "net_margin": "higher",
}
# pe_ratio is price-gated (instrument_valuation live-price join) → structurally
# thin on dev regardless of how many sector members exist.
DEV_LIMITED_FACTORS: frozenset[str] = frozenset({"pe_ratio"})

# A factor is "thin" (greyed + ⚠ in the UI, its sector median read as noisy) when
# it is either structurally dev-limited OR its sector coverage is below this
# fraction of the complete-TTM sector base. Below ~20% the median rests on too
# small a non-null base to be meaningful (#1836). The cut is a visual-taste call
# the operator pre-approved at ~20%; the dev DB separates cleanly — thin factors
# (pe_ratio, revenue_growth_yoy) peak at 12.5% coverage, healthy factors floor at
# 24.6%, so 0.20 flags the former without catching the latter.
THIN_COVERAGE_RATIO: float = 0.20


def is_factor_thin(key: str, sector_n: int, sector_member_count: int) -> bool:
    """
    True when a factor should be disclosed as thin/unreliable for a sector.

    Pure policy (no I/O) — table-tested. ``sector_n`` is the count of sector
    members with a non-null value for the factor; ``sector_member_count`` is the
    complete-TTM sector base (the median denominator). Structurally dev-limited
    factors are always thin; an empty base is treated as thin (no signal).
    """
    if key in DEV_LIMITED_FACTORS:
        return True
    if sector_member_count <= 0:
        return True
    return sector_n / sector_member_count < THIN_COVERAGE_RATIO


# Per-instrument factor CTE template, parameterised by ``%(sic_prefix)s`` with the
# SIC column filled from ``_SIC_COL`` (frozen whitelist) via ``_factors_cte``.
# Mirrors the instrument_valuation formulas (sql/201) for the price-free factors;
# LEFT JOINs instrument_valuation for pe_ratio and the YoY CTE for revenue growth.
_FACTORS_CTE_TEMPLATE = """
WITH yoy AS (
    SELECT instrument_id,
           (cur_rev - prev_rev) / prev_rev AS revenue_growth_yoy
    FROM (
        SELECT instrument_id,
               revenue AS cur_rev,
               period_end_date AS cur_end,
               LEAD(revenue) OVER w AS prev_rev,
               LEAD(period_end_date) OVER w AS prev_end,
               ROW_NUMBER() OVER w AS rn
        FROM financial_periods
        WHERE period_type = 'FY'
          AND revenue > 0
          AND superseded_at IS NULL
          AND normalization_status = 'normalized'
        WINDOW w AS (PARTITION BY instrument_id ORDER BY period_end_date DESC)
    ) s
    WHERE rn = 1
      AND prev_rev IS NOT NULL
      AND prev_rev > 0
      -- consecutive-year guard: the two most recent FY ends must be ~12mo
      -- apart, else the "YoY" spans a multi-year gap (e.g. GME: FY2025/FY2020).
      AND (cur_end - prev_end) BETWEEN 300 AND 430
),
factors AS (
    SELECT
        t.instrument_id,
        i.symbol,
        i.company_name,
        t.total_assets,
        CASE WHEN t.revenue_ttm > 0
             THEN t.operating_income_ttm / t.revenue_ttm END AS operating_margin,
        CASE WHEN t.revenue_ttm > 0
             THEN t.net_income_ttm / t.revenue_ttm END AS net_margin,
        CASE WHEN t.shareholders_equity > 0
             THEN t.net_income_ttm / t.shareholders_equity END AS roe,
        CASE WHEN t.shareholders_equity > 0
             THEN (COALESCE(t.long_term_debt, 0) + COALESCE(t.short_term_debt, 0))
                  / t.shareholders_equity END AS debt_equity_ratio,
        v.pe_ratio,
        y.revenue_growth_yoy
    FROM financial_periods_ttm t
    JOIN instruments i USING (instrument_id)
    JOIN instrument_sec_profile sp USING (instrument_id)
    LEFT JOIN instrument_valuation v USING (instrument_id)
    LEFT JOIN yoy y ON y.instrument_id = t.instrument_id
    WHERE t.is_complete_ttm = TRUE
      AND {sic_col} = %(sic_prefix)s
)
"""


def _factors_cte(column_level: int) -> LiteralString:
    """Build the factor CTE for a walk level. ``sic_col`` comes from the frozen
    ``_SIC_COL`` whitelist (never input) → injection-safe; the prefix VALUE is
    bound as ``%(sic_prefix)s``. The cast is sound because both the template and
    every ``_SIC_COL`` value are literals (same pattern as
    ``fair_value_band._MEMBER_SQL: dict[int, LiteralString]``)."""
    return cast(LiteralString, _FACTORS_CTE_TEMPLATE.format(sic_col=_SIC_COL[column_level]))


@dataclass(frozen=True)
class PeerRow:
    instrument_id: int
    symbol: str
    company_name: str | None
    total_assets: float | None
    factors: dict[str, float | None]


@dataclass(frozen=True)
class FactorMedian:
    median: float | None
    n: int


@dataclass(frozen=True)
class PeerComparisonResult:
    instrument_id: int
    symbol: str
    cohort_sic: str  # the instrument's own full 4-digit SIC
    cohort_sic_label: str | None  # sic_description of that SIC
    cohort_sic_level: int  # 4/3/2 = cleared MIN_COHORT at that granularity; 0 = SIC-2 fallback (thin)
    cohort_member_count: int  # cohort peer count incl. self row (median base)
    self_factors: dict[str, float | None]
    medians: dict[str, FactorMedian]
    peers: list[PeerRow]


def _row_factors(row: dict[str, object]) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for key in FACTOR_KEYS:
        val = row.get(key)
        out[key] = float(val) if val is not None else None  # type: ignore[arg-type]
    return out


def _rank_peers(
    rows: list[dict[str, object]],
    *,
    self_id: int,
    self_total_assets: float,
    limit: int,
) -> list[PeerRow]:
    """
    Pick the ``limit`` nearest same-cohort peers by log-size proximity.

    Pure (no I/O): excludes self, drops rows with non-positive ``total_assets``,
    orders by ``|ln(peer.total_assets) - ln(self_total_assets)|``. Ties break on
    instrument_id for determinism.
    """
    candidates = [
        r
        for r in rows
        if int(r["instrument_id"]) != self_id  # type: ignore[arg-type]
        and r["total_assets"] is not None
        and float(r["total_assets"]) > 0  # type: ignore[arg-type]
    ]
    target = math.log(self_total_assets)
    candidates.sort(
        key=lambda r: (abs(math.log(float(r["total_assets"])) - target), int(r["instrument_id"]))  # type: ignore[arg-type]
    )
    return [
        PeerRow(
            instrument_id=int(r["instrument_id"]),  # type: ignore[arg-type]
            symbol=str(r["symbol"]),
            company_name=(str(r["company_name"]) if r["company_name"] is not None else None),
            total_assets=float(r["total_assets"]),  # type: ignore[arg-type]
            factors=_row_factors(r),
        )
        for r in candidates[:limit]
    ]


def compute_peer_comparison(
    conn: psycopg.Connection[object],
    instrument_id: int,
) -> PeerComparisonResult | None:
    """
    Build the peer-comparison payload, or ``None`` when the instrument has no
    SIC classification or no complete-TTM fundamentals (caller 404s).
    """
    with conn.cursor(row_factory=dict_row) as cur:
        # 1. target SIC + label + the three candidate PEER counts (self-excluded,
        #    so MIN_COHORT means N peers). None / null SIC → no classification.
        cur.execute(
            """
            WITH tgt AS (
                SELECT sic, sic3, sic2, sic_description
                FROM instrument_sec_profile WHERE instrument_id = %(id)s
            )
            SELECT t.sic, t.sic3, t.sic2, t.sic_description,
              (SELECT count(*) FROM financial_periods_ttm f
                 JOIN instrument_sec_profile p USING (instrument_id)
                 WHERE f.is_complete_ttm AND p.sic  = t.sic  AND f.instrument_id <> %(id)s) AS n4,
              (SELECT count(*) FROM financial_periods_ttm f
                 JOIN instrument_sec_profile p USING (instrument_id)
                 WHERE f.is_complete_ttm AND p.sic3 = t.sic3 AND f.instrument_id <> %(id)s) AS n3,
              (SELECT count(*) FROM financial_periods_ttm f
                 JOIN instrument_sec_profile p USING (instrument_id)
                 WHERE f.is_complete_ttm AND p.sic2 = t.sic2 AND f.instrument_id <> %(id)s) AS n2
            FROM tgt t
            """,
            {"id": instrument_id},
        )
        srow = cur.fetchone()
        if srow is None or srow["sic"] is None:
            return None
        cohort_sic = str(srow["sic"])
        cohort_sic_label = str(srow["sic_description"]) if srow["sic_description"] is not None else None
        column_level, cohort_sic_level = resolve_sic_level(
            int(srow["n4"] or 0), int(srow["n3"] or 0), int(srow["n2"] or 0), MIN_COHORT
        )
        sic_prefix = {4: srow["sic"], 3: srow["sic3"], 2: srow["sic2"]}[column_level]
        cte = _factors_cte(column_level)

        # 2. all complete-TTM factor rows in the cohort (self + candidates).
        cur.execute(
            cte + "SELECT instrument_id, symbol, company_name, total_assets, "
            "operating_margin, net_margin, roe, debt_equity_ratio, pe_ratio, revenue_growth_yoy "
            "FROM factors",
            {"sic_prefix": sic_prefix},
        )
        rows = cur.fetchall()
        by_id = {int(r["instrument_id"]): r for r in rows}
        self_raw = by_id.get(instrument_id)
        # Self must have a complete-TTM row with positive total_assets (factor
        # anchor + peer-proximity reference). Else: no fundamentals.
        if self_raw is None or self_raw["total_assets"] is None or float(self_raw["total_assets"]) <= 0:
            return None
        self_ta = float(self_raw["total_assets"])

        # 3. cohort medians + non-null counts, one pass over the factors CTE.
        median_select = ", ".join(
            f"percentile_cont(0.5) WITHIN GROUP (ORDER BY {k}) AS {k}_med, count({k}) AS {k}_n" for k in FACTOR_KEYS
        )
        # median_select is built only from the frozen FACTOR_KEYS constant — no
        # user input — so the dynamic SELECT is injection-safe.
        cur.execute(cte + f"SELECT {median_select} FROM factors", {"sic_prefix": sic_prefix})  # type: ignore[arg-type]
        mrow = cur.fetchone() or {}

    medians = {
        k: FactorMedian(
            median=(float(mrow[f"{k}_med"]) if mrow.get(f"{k}_med") is not None else None),
            n=int(mrow.get(f"{k}_n") or 0),
        )
        for k in FACTOR_KEYS
    }

    # 4. peer set: same SIC cohort, exclude self, total_assets>0, nearest by
    #    log-size proximity. Ranking in Python over the already-fetched cohort
    #    rows (hundreds at most) — no extra query. Best-effort: may be <8 after
    #    the total_assets filter (peer_comparison does not gate on peer count).
    peers = _rank_peers(list(by_id.values()), self_id=instrument_id, self_total_assets=self_ta, limit=_PEER_LIMIT)

    return PeerComparisonResult(
        instrument_id=instrument_id,
        symbol=str(self_raw["symbol"]),
        cohort_sic=cohort_sic,
        cohort_sic_label=cohort_sic_label,
        cohort_sic_level=cohort_sic_level,
        cohort_member_count=len(rows),
        self_factors=_row_factors(self_raw),
        medians=medians,
        peers=peers,
    )
