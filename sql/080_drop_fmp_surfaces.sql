-- 080_drop_fmp_surfaces.sql
--
-- #539 stage 2: drop FMP-only data surfaces from the schema.
--
-- Stage 1 (#532, sql/072 / sql/073) already dropped FMP from the
-- capabilities matrix and TRUNCATEd the orphan rows. Stage 2 drops
-- the source tables outright now that #539 retires the writers
-- (app/services/enrichment.py, app/providers/implementations/fmp.py)
-- and the readers (analyst_estimates lookup in scoring/thesis,
-- earnings_events lookup in thesis/reporting).
--
-- ``instrument_valuation`` VIEW depends on ``analyst_estimates`` for
-- its ``forward_pe`` column (sql/032 line 514-520). Recreating the
-- VIEW without the latest_estimates CTE preserves column shape with
-- ``forward_pe = NULL`` so any future caller still resolves the
-- column. Zero existing callers reference forward_pe (verified via
-- grep across app/ frontend/src/ tests/).
--
-- Idempotent: ``IF EXISTS`` guards every drop. Safe on a fresh DB
-- (where the tables/columns may already be absent) and safe to
-- re-run on a partially-applied state.

BEGIN;

-- ── 1. Recreate instrument_valuation without analyst_estimates ──
DROP VIEW IF EXISTS instrument_valuation;

CREATE VIEW instrument_valuation AS
WITH priced AS (
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
    v.market_cap_live,
    v.enterprise_value,
    v.pe_ratio,
    v.pb_ratio,
    v.price_sales,
    v.p_fcf_ratio,
    v.fcf_yield,
    v.debt_equity_ratio,
    v.ev_revenue,
    v.ev_ebitda,
    v.net_margin,
    v.gross_margin,
    v.operating_margin,
    v.roa,
    v.roe,
    -- forward_pe always NULL: source (analyst_estimates) dropped in
    -- this migration. Column retained for shape compatibility with
    -- any external (operator-curated SQL, BI tool) reader.
    NULL::numeric AS forward_pe,
    v.dividend_yield,
    v.ebitda_ttm,
    v.fcf_ttm
FROM (
    SELECT * FROM new_pipeline
    UNION ALL
    SELECT * FROM legacy
) v;

-- ── 2. Drop FMP-only tables ─────────────────────────────────────
DROP TABLE IF EXISTS analyst_estimates;
DROP TABLE IF EXISTS earnings_events;
DROP TABLE IF EXISTS instrument_profile;

-- ── 3. Drop FMP-only currency-enrichment column ────────────────
ALTER TABLE instruments DROP COLUMN IF EXISTS currency_enriched_at;

COMMIT;
