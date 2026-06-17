-- 201_instrument_valuation_dual_class_suppress.sql
--
-- #1664: the instrument_valuation VIEW (sql/080) computes the
-- shares-derived valuation columns as ``price × shares_outstanding``,
-- where ``shares_outstanding`` is the issuer's COMBINED all-class count
-- (financial_periods_ttm / fundamentals_snapshot hold only the combined
-- us-gaap figure — companyfacts strips the per-class dimensional facts,
-- #1646). For a dual-class issuer whose share classes share one SEC CIK
-- (GOOG/GOOGL, HEI/HEI.A, METC/METCB) this prices EVERY class at ONE
-- class's quote against ALL classes' shares → a structurally-wrong, and
-- per-sibling-different, "market cap" for one company (dev: GOOG $4.340T
-- vs GOOGL $4.473T; correct #1662 total = $4.4476T, identical).
--
-- The view cannot compute the correct total-company cap: that needs
-- per-class prices + untraded-residual imputation + the fail-closed
-- guards in app/services/xbrl_derived_stats.py::_assemble_total_company_cap
-- (Σ-overage, residual cap, freshness, future-period). Reproducing that
-- multi-step policy in SQL would duplicate the load-bearing source of
-- truth and drift. So this migration takes the #1662 ``multiclass_unavailable``
-- posture for the VIEW: SUPPRESS (NULL) the shares-distorted columns for
-- curated dual-class issuers, for ALL view readers (scoring + operator BI).
-- The decision-grade consumer (app/services/scoring.py) overlays the
-- CORRECT total-company figure in Python via resolve_market_cap_basis.
--
-- Dual-class detection mirrors resolve_market_cap_basis: the instrument's
-- primary SEC CIK is present in the #1623 curated per-class FSDS table
-- (instrument_class_shares_outstanding.source_cik), CIK zero-padded to 10
-- to match how the SEC pipeline stores it. This is the CURATED,
-- CUSIP-disambiguated oracle — NOT a raw shared-CIK sibling count (which
-- is dominated by .US dual-listings, ETF trusts, warrants, preferreds).
--
-- Suppressed (every column whose value carries a ``price × combined-shares``
-- term): market_cap_live, enterprise_value, pb_ratio (= price×shares/equity
-- in both CTEs), price_sales, p_fcf_ratio, fcf_yield, ev_revenue, ev_ebitda.
-- KEPT (no shares×price term — correct regardless of class): pe_ratio,
-- debt_equity_ratio (debt/total-equity), net/gross/operating_margin, roa,
-- roe, dividend_yield, and all raw TTM/snapshot figures.
--
-- View recreate only — no data backfill. Idempotent (DROP VIEW IF EXISTS).

BEGIN;

DROP VIEW IF EXISTS instrument_valuation;

CREATE VIEW instrument_valuation AS
WITH dual_class AS (
    -- Curated dual-class instruments: primary SEC CIK present in the
    -- #1623 per-class FSDS table. Both share-class siblings of a covered
    -- CIK appear (the table is keyed per sibling instrument_id).
    SELECT DISTINCT ei.instrument_id
    FROM external_identifiers ei
    JOIN instrument_class_shares_outstanding c
      ON c.source_cik = lpad(ei.identifier_value, 10, '0')
    WHERE ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
      AND ei.is_primary = TRUE
),
priced AS (
    SELECT instrument_id,
           COALESCE(
               NULLIF(GREATEST(last, 0), 0),
               CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
           )                       AS price,
           quoted_at
    FROM quotes
),
new_pipeline AS (
    SELECT
        p.instrument_id,
        p.price,
        p.quoted_at,
        ttm.revenue_ttm,
        ttm.net_income_ttm,
        ttm.operating_income_ttm,
        ttm.gross_profit_ttm,
        ttm.depreciation_amort_ttm,
        ttm.operating_cf_ttm,
        ttm.capex_ttm,
        ttm.dps_declared_ttm,
        ttm.is_complete_ttm,
        ttm.eps_diluted_ttm,
        ttm.total_assets,
        ttm.total_liabilities,
        ttm.shareholders_equity,
        ttm.cash,
        ttm.long_term_debt,
        ttm.short_term_debt,
        ttm.shares_outstanding,
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
             THEN p.price * ttm.shares_outstanding
        END AS market_cap_live,
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
             THEN p.price * ttm.shares_outstanding
                  + COALESCE(ttm.long_term_debt, 0)
                  + COALESCE(ttm.short_term_debt, 0)
                  - COALESCE(ttm.cash, 0)
        END AS enterprise_value,
        CASE WHEN ttm.eps_diluted_ttm > 0
             THEN p.price / ttm.eps_diluted_ttm
        END AS pe_ratio,
        CASE WHEN ttm.shareholders_equity > 0 AND ttm.shares_outstanding > 0
             THEN p.price / (ttm.shareholders_equity / ttm.shares_outstanding)
        END AS pb_ratio,
        CASE WHEN ttm.revenue_ttm > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding) / ttm.revenue_ttm
        END AS price_sales,
        CASE WHEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) > 0
                  AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding)
                  / (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
        END AS p_fcf_ratio,
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
                  AND (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) IS NOT NULL
             THEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
                  / (p.price * ttm.shares_outstanding)
        END AS fcf_yield,
        CASE WHEN ttm.shareholders_equity > 0
             THEN (COALESCE(ttm.long_term_debt, 0) + COALESCE(ttm.short_term_debt, 0))
                  / ttm.shareholders_equity
        END AS debt_equity_ratio,
        CASE WHEN ttm.revenue_ttm > 0 AND p.price > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding
                   + COALESCE(ttm.long_term_debt, 0)
                   + COALESCE(ttm.short_term_debt, 0)
                   - COALESCE(ttm.cash, 0))
                  / ttm.revenue_ttm
        END AS ev_revenue,
        CASE WHEN (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0)) > 0
                  AND p.price > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding
                   + COALESCE(ttm.long_term_debt, 0)
                   + COALESCE(ttm.short_term_debt, 0)
                   - COALESCE(ttm.cash, 0))
                  / (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0))
        END AS ev_ebitda,
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.net_income_ttm / ttm.revenue_ttm
        END AS net_margin,
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.gross_profit_ttm / ttm.revenue_ttm
        END AS gross_margin,
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.operating_income_ttm / ttm.revenue_ttm
        END AS operating_margin,
        CASE WHEN ttm.total_assets > 0
             THEN ttm.net_income_ttm / ttm.total_assets
        END AS roa,
        CASE WHEN ttm.shareholders_equity > 0
             THEN ttm.net_income_ttm / ttm.shareholders_equity
        END AS roe,
        CASE WHEN p.price > 0 AND ttm.dps_declared_ttm > 0
             THEN ttm.dps_declared_ttm / p.price * 100
        END AS dividend_yield,
        ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0) AS ebitda_ttm,
        ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)) AS fcf_ttm
    FROM priced p
    JOIN financial_periods_ttm ttm USING (instrument_id)
    WHERE ttm.is_complete_ttm = TRUE
),
legacy AS (
    SELECT DISTINCT ON (fs.instrument_id)
        p.instrument_id,
        p.price,
        p.quoted_at,
        NULL::numeric AS revenue_ttm,
        NULL::numeric AS net_income_ttm,
        NULL::numeric AS operating_income_ttm,
        NULL::numeric AS gross_profit_ttm,
        NULL::numeric AS depreciation_amort_ttm,
        NULL::numeric AS operating_cf_ttm,
        NULL::numeric AS capex_ttm,
        NULL::numeric AS dps_declared_ttm,
        NULL::boolean AS is_complete_ttm,
        NULL::numeric AS eps_diluted_ttm,
        NULL::numeric AS total_assets,
        NULL::numeric AS total_liabilities,
        NULL::numeric AS shareholders_equity,
        fs.cash,
        fs.debt AS long_term_debt,
        NULL::numeric AS short_term_debt,
        fs.shares_outstanding,
        CASE WHEN p.price > 0 AND fs.shares_outstanding > 0
             THEN p.price * fs.shares_outstanding
        END AS market_cap_live,
        NULL::numeric AS enterprise_value,
        CASE WHEN p.price > 0 AND fs.eps > 0
             THEN p.price / fs.eps
        END AS pe_ratio,
        CASE WHEN p.price > 0 AND fs.book_value > 0
             THEN p.price / fs.book_value
        END AS pb_ratio,
        NULL::numeric AS price_sales,
        CASE WHEN p.price > 0 AND fs.shares_outstanding > 0 AND fs.fcf > 0
             THEN (p.price * fs.shares_outstanding) / fs.fcf
        END AS p_fcf_ratio,
        CASE WHEN p.price > 0 AND fs.shares_outstanding > 0
             THEN fs.fcf / (p.price * fs.shares_outstanding)
        END AS fcf_yield,
        CASE WHEN fs.book_value > 0 AND fs.shares_outstanding > 0
             THEN fs.debt / (fs.book_value * fs.shares_outstanding)
        END AS debt_equity_ratio,
        NULL::numeric AS ev_revenue,
        NULL::numeric AS ev_ebitda,
        NULL::numeric AS net_margin,
        fs.gross_margin,
        fs.operating_margin,
        NULL::numeric AS roa,
        NULL::numeric AS roe,
        NULL::numeric AS dividend_yield,
        NULL::numeric AS ebitda_ttm,
        fs.fcf AS fcf_ttm
    FROM priced p
    JOIN fundamentals_snapshot fs USING (instrument_id)
    WHERE NOT EXISTS (
        SELECT 1 FROM financial_periods_ttm ttm
        WHERE ttm.instrument_id = p.instrument_id
          AND ttm.is_complete_ttm = TRUE
    )
    ORDER BY fs.instrument_id, fs.as_of_date DESC
)
SELECT
    v.instrument_id,
    v.price              AS current_price,
    v.quoted_at          AS price_as_of,
    v.revenue_ttm,
    v.net_income_ttm,
    v.operating_income_ttm,
    v.gross_profit_ttm,
    v.depreciation_amort_ttm,
    v.operating_cf_ttm,
    v.capex_ttm,
    v.dps_declared_ttm,
    v.is_complete_ttm,
    v.total_assets,
    v.total_liabilities,
    v.shareholders_equity,
    v.cash,
    v.long_term_debt,
    v.short_term_debt,
    v.shares_outstanding,
    -- #1664: NULL the shares-distorted columns for curated dual-class
    -- issuers (combined-shares × one-class-price is structurally wrong).
    -- The correct total-company figure is sourced in Python by the
    -- decision-grade consumer via resolve_market_cap_basis.
    CASE WHEN dc.instrument_id IS NULL THEN v.market_cap_live END   AS market_cap_live,
    CASE WHEN dc.instrument_id IS NULL THEN v.enterprise_value END  AS enterprise_value,
    v.pe_ratio,
    CASE WHEN dc.instrument_id IS NULL THEN v.pb_ratio END          AS pb_ratio,
    CASE WHEN dc.instrument_id IS NULL THEN v.price_sales END       AS price_sales,
    CASE WHEN dc.instrument_id IS NULL THEN v.p_fcf_ratio END       AS p_fcf_ratio,
    CASE WHEN dc.instrument_id IS NULL THEN v.fcf_yield END         AS fcf_yield,
    v.debt_equity_ratio,
    CASE WHEN dc.instrument_id IS NULL THEN v.ev_revenue END        AS ev_revenue,
    CASE WHEN dc.instrument_id IS NULL THEN v.ev_ebitda END         AS ev_ebitda,
    v.net_margin,
    v.gross_margin,
    v.operating_margin,
    v.roa,
    v.roe,
    -- forward_pe always NULL: source (analyst_estimates) dropped in
    -- sql/080. Column retained for shape compatibility with any external
    -- (operator-curated SQL, BI tool) reader.
    NULL::numeric AS forward_pe,
    v.dividend_yield,
    v.ebitda_ttm,
    v.fcf_ttm
FROM (
    SELECT * FROM new_pipeline
    UNION ALL
    SELECT * FROM legacy
) v
LEFT JOIN dual_class dc USING (instrument_id);

COMMIT;
