-- 032_financial_data_enrichment_p1.sql
--
-- Phase 1 of Financial Data Enrichment (spec: 2026-04-16-financial-data-enrichment-design.md)
--
-- Creates:
--   data_ingestion_runs         — audit trail for every provider batch
--   financial_facts_raw         — individual XBRL facts from SEC companyfacts
--   financial_periods_raw       — wide period rows per source (SEC, FMP, etc.)
--   financial_periods           — canonical one-row-per-period (best source wins)
--   financial_periods_ttm       — VIEW: trailing 12 months from last 4 quarters
--   instrument_valuation        — VIEW: expanded (replaces 024 version)

-- ── 1. data_ingestion_runs ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS data_ingestion_runs (
    ingestion_run_id     BIGSERIAL PRIMARY KEY,
    source               TEXT NOT NULL,
    endpoint             TEXT,
    instrument_count     INTEGER,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at          TIMESTAMPTZ,
    status               TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'partial', 'failed')),
    rows_upserted        INTEGER DEFAULT 0,
    rows_skipped         INTEGER DEFAULT 0,
    error                TEXT
);

-- ── 2. financial_facts_raw ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS financial_facts_raw (
    fact_id              BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    taxonomy             TEXT NOT NULL DEFAULT 'us-gaap',
    concept              TEXT NOT NULL,
    unit                 TEXT NOT NULL,
    period_start         DATE,
    period_end           DATE NOT NULL,
    val                  NUMERIC(30,6) NOT NULL,
    frame                TEXT,
    accession_number     TEXT NOT NULL,
    form_type            TEXT NOT NULL,
    filed_date           DATE NOT NULL,
    fiscal_year          INTEGER,
    fiscal_period        TEXT,
    decimals             INTEGER,
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id, concept, unit, period_end, accession_number)
);

CREATE INDEX IF NOT EXISTS idx_facts_raw_instrument_concept
    ON financial_facts_raw(instrument_id, concept, period_end DESC);

-- ── 3. financial_periods_raw ───────────────────────────────────

CREATE TABLE IF NOT EXISTS financial_periods_raw (
    raw_period_id        BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    period_end_date      DATE NOT NULL,
    period_type          TEXT NOT NULL CHECK (period_type IN (
        'Q1','Q2','Q3','Q4','FY','H1','H2','9M','STUB'
    )),
    fiscal_year          INTEGER NOT NULL,
    fiscal_quarter       INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    period_start_date    DATE,
    months_covered       SMALLINT,

    -- Income Statement
    revenue              NUMERIC(20,4),
    cost_of_revenue      NUMERIC(20,4),
    gross_profit         NUMERIC(20,4),
    operating_income     NUMERIC(20,4),
    net_income           NUMERIC(20,4),
    eps_basic            NUMERIC(12,4),
    eps_diluted          NUMERIC(12,4),
    research_and_dev     NUMERIC(20,4),
    sga_expense          NUMERIC(20,4),
    depreciation_amort   NUMERIC(20,4),
    interest_expense     NUMERIC(20,4),
    income_tax           NUMERIC(20,4),
    shares_basic         NUMERIC(20,0),
    shares_diluted       NUMERIC(20,0),
    sbc_expense          NUMERIC(20,4),

    -- Balance Sheet
    total_assets         NUMERIC(20,4),
    total_liabilities    NUMERIC(20,4),
    shareholders_equity  NUMERIC(20,4),
    cash                 NUMERIC(20,4),
    long_term_debt       NUMERIC(20,4),
    short_term_debt      NUMERIC(20,4),
    shares_outstanding   NUMERIC(20,0),
    inventory            NUMERIC(20,4),
    receivables          NUMERIC(20,4),
    payables             NUMERIC(20,4),
    goodwill             NUMERIC(20,4),
    ppe_net              NUMERIC(20,4),

    -- Cash Flow
    operating_cf         NUMERIC(20,4),
    investing_cf         NUMERIC(20,4),
    financing_cf         NUMERIC(20,4),
    capex                NUMERIC(20,4),
    dividends_paid       NUMERIC(20,4),
    dps_declared         NUMERIC(12,4),
    buyback_spend        NUMERIC(20,4),

    -- Provenance
    source               TEXT NOT NULL,
    source_ref           TEXT NOT NULL,
    reported_currency    TEXT NOT NULL,
    fx_rate_to_usd       NUMERIC(20,10),
    fx_rate_date         DATE,
    form_type            TEXT,
    filed_date           DATE,
    is_restated          BOOLEAN NOT NULL DEFAULT FALSE,
    is_derived           BOOLEAN NOT NULL DEFAULT FALSE,
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(instrument_id, period_end_date, period_type, source, source_ref)
);

-- ── 4. financial_periods (canonical) ───────────────────────────

CREATE TABLE IF NOT EXISTS financial_periods (
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    period_end_date      DATE NOT NULL,
    period_type          TEXT NOT NULL CHECK (period_type IN (
        'Q1','Q2','Q3','Q4','FY','H1','H2','9M','STUB'
    )),
    fiscal_year          INTEGER NOT NULL,
    fiscal_quarter       INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    period_start_date    DATE,
    months_covered       SMALLINT,

    -- Income Statement
    revenue              NUMERIC(20,4),
    cost_of_revenue      NUMERIC(20,4),
    gross_profit         NUMERIC(20,4),
    operating_income     NUMERIC(20,4),
    net_income           NUMERIC(20,4),
    eps_basic            NUMERIC(12,4),
    eps_diluted          NUMERIC(12,4),
    research_and_dev     NUMERIC(20,4),
    sga_expense          NUMERIC(20,4),
    depreciation_amort   NUMERIC(20,4),
    interest_expense     NUMERIC(20,4),
    income_tax           NUMERIC(20,4),
    shares_basic         NUMERIC(20,0),
    shares_diluted       NUMERIC(20,0),
    sbc_expense          NUMERIC(20,4),

    -- Balance Sheet
    total_assets         NUMERIC(20,4),
    total_liabilities    NUMERIC(20,4),
    shareholders_equity  NUMERIC(20,4),
    cash                 NUMERIC(20,4),
    long_term_debt       NUMERIC(20,4),
    short_term_debt      NUMERIC(20,4),
    shares_outstanding   NUMERIC(20,0),
    inventory            NUMERIC(20,4),
    receivables          NUMERIC(20,4),
    payables             NUMERIC(20,4),
    goodwill             NUMERIC(20,4),
    ppe_net              NUMERIC(20,4),

    -- Cash Flow
    operating_cf         NUMERIC(20,4),
    investing_cf         NUMERIC(20,4),
    financing_cf         NUMERIC(20,4),
    capex                NUMERIC(20,4),
    dividends_paid       NUMERIC(20,4),
    dps_declared         NUMERIC(12,4),
    buyback_spend        NUMERIC(20,4),

    -- Provenance
    source               TEXT NOT NULL,
    source_ref           TEXT NOT NULL,
    reported_currency    TEXT NOT NULL,
    fx_rate_to_usd       NUMERIC(20,10),
    form_type            TEXT,
    filed_date           DATE,
    is_restated          BOOLEAN NOT NULL DEFAULT FALSE,
    is_derived           BOOLEAN NOT NULL DEFAULT FALSE,
    normalization_status TEXT NOT NULL DEFAULT 'raw'
        CHECK (normalization_status IN ('raw', 'normalized', 'verified')),
    superseded_at        TIMESTAMPTZ,

    PRIMARY KEY (instrument_id, period_end_date, period_type)
);

CREATE INDEX IF NOT EXISTS idx_financial_periods_instrument_date
    ON financial_periods(instrument_id, period_end_date DESC)
    WHERE superseded_at IS NULL;

-- ── 5. financial_periods_ttm (VIEW) ────────────────────────────

CREATE OR REPLACE VIEW financial_periods_ttm AS
WITH ranked_quarters AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY instrument_id
               ORDER BY period_end_date DESC
           ) AS rn
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
      AND superseded_at IS NULL
      AND normalization_status = 'normalized'
),
latest_4 AS (
    SELECT * FROM ranked_quarters WHERE rn <= 4
)
SELECT
    instrument_id,
    COUNT(*) AS quarter_count,
    SUM(COALESCE(months_covered, 3)) AS ttm_months_covered,
    COUNT(*) = 4 AS is_complete_ttm,
    MIN(period_end_date) AS ttm_start,
    MAX(period_end_date) AS ttm_end,

    -- Flow items: SUM of 4 quarters
    SUM(revenue) AS revenue_ttm,
    SUM(cost_of_revenue) AS cost_of_revenue_ttm,
    SUM(gross_profit) AS gross_profit_ttm,
    SUM(operating_income) AS operating_income_ttm,
    SUM(net_income) AS net_income_ttm,
    SUM(research_and_dev) AS research_and_dev_ttm,
    SUM(sga_expense) AS sga_expense_ttm,
    SUM(depreciation_amort) AS depreciation_amort_ttm,
    SUM(interest_expense) AS interest_expense_ttm,
    SUM(income_tax) AS income_tax_ttm,
    SUM(sbc_expense) AS sbc_expense_ttm,
    SUM(operating_cf) AS operating_cf_ttm,
    SUM(investing_cf) AS investing_cf_ttm,
    SUM(financing_cf) AS financing_cf_ttm,
    SUM(capex) AS capex_ttm,
    SUM(dividends_paid) AS dividends_paid_ttm,
    SUM(dps_declared) AS dps_declared_ttm,
    SUM(buyback_spend) AS buyback_spend_ttm,

    -- Stock items: latest quarter only (rn=1)
    MAX(total_assets) FILTER (WHERE rn = 1) AS total_assets,
    MAX(total_liabilities) FILTER (WHERE rn = 1) AS total_liabilities,
    MAX(shareholders_equity) FILTER (WHERE rn = 1) AS shareholders_equity,
    MAX(cash) FILTER (WHERE rn = 1) AS cash,
    MAX(long_term_debt) FILTER (WHERE rn = 1) AS long_term_debt,
    MAX(short_term_debt) FILTER (WHERE rn = 1) AS short_term_debt,
    MAX(shares_outstanding) FILTER (WHERE rn = 1) AS shares_outstanding,
    MAX(inventory) FILTER (WHERE rn = 1) AS inventory,
    MAX(receivables) FILTER (WHERE rn = 1) AS receivables,
    MAX(payables) FILTER (WHERE rn = 1) AS payables,
    MAX(goodwill) FILTER (WHERE rn = 1) AS goodwill,
    MAX(ppe_net) FILTER (WHERE rn = 1) AS ppe_net,

    -- Derived
    MAX(reported_currency) FILTER (WHERE rn = 1) AS reported_currency,
    -- EPS: latest quarter values (not summed — TTM EPS derived below in valuation view)
    MAX(eps_basic) FILTER (WHERE rn = 1) AS eps_basic_latest,
    MAX(eps_diluted) FILTER (WHERE rn = 1) AS eps_diluted_latest,
    -- TTM EPS as sum of 4 quarters
    SUM(eps_basic) AS eps_basic_ttm,
    SUM(eps_diluted) AS eps_diluted_ttm,
    MAX(shares_basic) FILTER (WHERE rn = 1) AS shares_basic,
    MAX(shares_diluted) FILTER (WHERE rn = 1) AS shares_diluted

FROM latest_4
GROUP BY instrument_id;

-- ── 6. instrument_valuation (expanded VIEW, replaces 024 version) ──
--
-- Backward-compatible: keeps pe_ratio, pb_ratio, p_fcf_ratio,
-- fcf_yield, debt_equity_ratio, market_cap_live, current_price.
-- Adds: price_sales, ev_revenue, ev_ebitda, net_margin, gross_margin,
-- operating_margin, roa, roe, forward_pe, dividend_yield, ebitda_ttm, fcf_ttm,
-- enterprise_value, and the raw TTM components.
--
-- Falls back to the old fundamentals_snapshot-based calculation if no
-- financial_periods data exists yet (migration transition period).
--
-- Must DROP first: the column types changed (UNION ALL widens
-- NUMERIC(18,6) to NUMERIC), and CREATE OR REPLACE cannot change types.
-- No dependent views exist — checked before writing this migration.

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
-- New pipeline: TTM from financial_periods
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

        -- Market cap
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
             THEN p.price * ttm.shares_outstanding
        END AS market_cap_live,

        -- Enterprise value
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
             THEN p.price * ttm.shares_outstanding
                  + COALESCE(ttm.long_term_debt, 0)
                  + COALESCE(ttm.short_term_debt, 0)
                  - COALESCE(ttm.cash, 0)
        END AS enterprise_value,

        -- P/E
        CASE WHEN ttm.eps_diluted_ttm > 0
             THEN p.price / ttm.eps_diluted_ttm
        END AS pe_ratio,

        -- P/B
        CASE WHEN ttm.shareholders_equity > 0 AND ttm.shares_outstanding > 0
             THEN p.price / (ttm.shareholders_equity / ttm.shares_outstanding)
        END AS pb_ratio,

        -- P/S
        CASE WHEN ttm.revenue_ttm > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding) / ttm.revenue_ttm
        END AS price_sales,

        -- P/FCF
        CASE WHEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) > 0
                  AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding)
                  / (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
        END AS p_fcf_ratio,

        -- FCF yield (backward compat)
        CASE WHEN p.price > 0 AND ttm.shares_outstanding > 0
                  AND (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) IS NOT NULL
             THEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
                  / (p.price * ttm.shares_outstanding)
        END AS fcf_yield,

        -- Debt/equity (backward compat)
        CASE WHEN ttm.shareholders_equity > 0
             THEN (COALESCE(ttm.long_term_debt, 0) + COALESCE(ttm.short_term_debt, 0))
                  / ttm.shareholders_equity
        END AS debt_equity_ratio,

        -- EV/Revenue
        CASE WHEN ttm.revenue_ttm > 0 AND p.price > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding
                   + COALESCE(ttm.long_term_debt, 0)
                   + COALESCE(ttm.short_term_debt, 0)
                   - COALESCE(ttm.cash, 0))
                  / ttm.revenue_ttm
        END AS ev_revenue,

        -- EV/EBITDA
        CASE WHEN (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0)) > 0
                  AND p.price > 0 AND ttm.shares_outstanding > 0
             THEN (p.price * ttm.shares_outstanding
                   + COALESCE(ttm.long_term_debt, 0)
                   + COALESCE(ttm.short_term_debt, 0)
                   - COALESCE(ttm.cash, 0))
                  / (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0))
        END AS ev_ebitda,

        -- Margins
        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.net_income_ttm / ttm.revenue_ttm
        END AS net_margin,

        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.gross_profit_ttm / ttm.revenue_ttm
        END AS gross_margin,

        CASE WHEN ttm.revenue_ttm > 0
             THEN ttm.operating_income_ttm / ttm.revenue_ttm
        END AS operating_margin,

        -- Returns on capital
        CASE WHEN ttm.total_assets > 0
             THEN ttm.net_income_ttm / ttm.total_assets
        END AS roa,

        CASE WHEN ttm.shareholders_equity > 0
             THEN ttm.net_income_ttm / ttm.shareholders_equity
        END AS roe,

        -- Dividend yield
        CASE WHEN p.price > 0 AND ttm.dps_declared_ttm > 0
             THEN ttm.dps_declared_ttm / p.price * 100
        END AS dividend_yield,

        -- EBITDA
        ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0) AS ebitda_ttm,

        -- FCF
        ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)) AS fcf_ttm

    FROM priced p
    JOIN financial_periods_ttm ttm USING (instrument_id)
),
-- Legacy fallback: old fundamentals_snapshot (used until new pipeline is populated)
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
    )
    ORDER BY fs.instrument_id, fs.as_of_date DESC
),
-- Forward P/E from analyst_estimates (shared by both pipelines)
latest_estimates AS (
    SELECT DISTINCT ON (instrument_id)
        instrument_id,
        consensus_eps_fy
    FROM analyst_estimates
    ORDER BY instrument_id, as_of_date DESC
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
    CASE WHEN ae.consensus_eps_fy > 0
         THEN v.price / ae.consensus_eps_fy
    END AS forward_pe,
    v.dividend_yield,
    v.ebitda_ttm,
    v.fcf_ttm
FROM (
    SELECT * FROM new_pipeline
    UNION ALL
    SELECT * FROM legacy
) v
LEFT JOIN latest_estimates ae USING (instrument_id);
