# Financial Data Enrichment — Phase 1: SEC EDGAR Expansion & Canonical Pipeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 10-concept FundamentalsSnapshot pipeline with a full XBRL fact store (~30 concepts), quarterly financial periods, TTM computation, and expanded valuation ratios — all from SEC EDGAR (free, uncapped).

**Architecture:** Three-layer pipeline: `financial_facts_raw` (individual XBRL facts) → `financial_periods_raw` (wide period rows per source) → `financial_periods` (canonical best-source-wins merge). TTM and valuation are PostgreSQL VIEWs that auto-update. The existing `fundamentals_snapshot` table remains populated for backward compatibility until consumers migrate.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL 16, httpx, pytest, SEC EDGAR XBRL API

**Spec:** `docs/superpowers/specs/2026-04-16-financial-data-enrichment-design.md`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `sql/032_financial_data_enrichment_p1.sql` | Create | Migration: `data_ingestion_runs`, `financial_facts_raw`, `financial_periods_raw`, `financial_periods` tables; `financial_periods_ttm` and updated `instrument_valuation` views |
| `app/providers/implementations/sec_fundamentals.py` | Modify | Expand XBRL tag list to ~30 concepts; add `extract_facts()` method returning `list[XbrlFact]` |
| `app/providers/fundamentals.py` | Modify | Add `XbrlFact` dataclass |
| `app/services/financial_facts.py` | Create | Service: fetch facts via provider, upsert into `financial_facts_raw`, track ingestion runs |
| `app/services/financial_normalization.py` | Create | Service: derive `financial_periods_raw` from facts, canonical merge into `financial_periods` |
| `app/workers/scheduler.py` | Modify | Wire `refresh_financial_facts` + `normalize_financial_periods` into `daily_research_refresh` |
| `tests/test_xbrl_fact_extraction.py` | Create | Unit tests for expanded fact extraction |
| `tests/test_financial_facts_service.py` | Create | Unit tests for fact storage service |
| `tests/test_financial_normalization.py` | Create | Unit tests for normalization pipeline |

---

### Task 1: Migration — Create Phase 1 Tables and Views

**Files:**
- Create: `sql/032_financial_data_enrichment_p1.sql`

- [ ] **Step 1: Write the migration file**

```sql
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

CREATE OR REPLACE VIEW instrument_valuation AS
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
```

- [ ] **Step 2: Verify migration applies cleanly**

Run: `uv run python -c "from app.db.migrations import migration_status; print(migration_status())"`
Expected: `032_financial_data_enrichment_p1.sql` shows as `pending`

Then start the app or run:
```bash
uv run python -c "from app.db.migrations import run_migrations; run_migrations()"
```
Expected: Migration applies without errors.

- [ ] **Step 3: Verify tables and views exist**

```bash
uv run python -c "
import psycopg
from app.config import settings
with psycopg.connect(settings.database_url) as conn:
    for t in ['data_ingestion_runs','financial_facts_raw','financial_periods_raw','financial_periods']:
        conn.execute(f'SELECT 1 FROM {t} LIMIT 0')
        print(f'{t}: OK')
    for v in ['financial_periods_ttm','instrument_valuation']:
        conn.execute(f'SELECT 1 FROM {v} LIMIT 0')
        print(f'{v} (view): OK')
"
```
Expected: All tables/views print OK.

- [ ] **Step 4: Verify backward compatibility — scoring still works**

```bash
uv run pytest tests/test_scoring.py -v -x
```
Expected: All scoring tests pass (the valuation view still returns the columns scoring expects: `pe_ratio`, `pb_ratio`, `p_fcf_ratio`, `fcf_yield`, `debt_equity_ratio`, `market_cap_live`, `current_price`).

- [ ] **Step 5: Commit**

```bash
git add sql/032_financial_data_enrichment_p1.sql
git commit -m "feat: add Phase 1 financial data enrichment tables and views

Creates data_ingestion_runs, financial_facts_raw, financial_periods_raw,
financial_periods tables. Adds financial_periods_ttm view and expands
instrument_valuation with full ratio suite while maintaining backward
compatibility with fundamentals_snapshot fallback."
```

---

### Task 2: XbrlFact Dataclass and Expanded Tag List

**Files:**
- Modify: `app/providers/fundamentals.py`
- Modify: `app/providers/implementations/sec_fundamentals.py`

- [ ] **Step 1: Write failing test for XbrlFact dataclass**

Create file `tests/test_xbrl_fact_extraction.py`:

```python
"""Tests for expanded XBRL fact extraction from SEC companyfacts."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.providers.fundamentals import XbrlFact


class TestXbrlFactDataclass:
    def test_create_duration_fact(self) -> None:
        fact = XbrlFact(
            concept="Revenues",
            taxonomy="us-gaap",
            unit="USD",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 3, 31),
            val=Decimal("1000000.00"),
            frame="CY2024Q1",
            accession_number="0000320193-24-000042",
            form_type="10-Q",
            filed_date=date(2024, 5, 1),
            fiscal_year=2024,
            fiscal_period="Q1",
            decimals=-3,
        )
        assert fact.concept == "Revenues"
        assert fact.period_start == date(2024, 1, 1)
        assert fact.val == Decimal("1000000.00")

    def test_create_instant_fact(self) -> None:
        fact = XbrlFact(
            concept="Assets",
            taxonomy="us-gaap",
            unit="USD",
            period_start=None,
            period_end=date(2024, 3, 31),
            val=Decimal("500000000.00"),
            frame=None,
            accession_number="0000320193-24-000042",
            form_type="10-Q",
            filed_date=date(2024, 5, 1),
            fiscal_year=2024,
            fiscal_period="Q1",
            decimals=-6,
        )
        assert fact.period_start is None
        assert fact.frame is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_xbrl_fact_extraction.py::TestXbrlFactDataclass -v`
Expected: FAIL with `ImportError: cannot import name 'XbrlFact'`

- [ ] **Step 3: Add XbrlFact dataclass to fundamentals.py**

Add after the existing `FundamentalsSnapshot` dataclass in `app/providers/fundamentals.py`:

```python
@dataclass(frozen=True)
class XbrlFact:
    """Single XBRL fact extracted from SEC companyfacts response."""

    concept: str  # e.g. 'Revenues', 'Assets'
    taxonomy: str  # e.g. 'us-gaap'
    unit: str  # 'USD', 'USD/shares', 'shares', 'pure'
    period_start: date | None  # None for instant (balance sheet) items
    period_end: date
    val: Decimal
    frame: str | None  # e.g. 'CY2024Q1', None for YTD/cumulative
    accession_number: str
    form_type: str  # '10-K', '10-Q', '8-K'
    filed_date: date
    fiscal_year: int | None
    fiscal_period: str | None  # 'FY', 'Q1', 'Q2', 'Q3', 'Q4'
    decimals: int | None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_xbrl_fact_extraction.py::TestXbrlFactDataclass -v`
Expected: PASS

- [ ] **Step 5: Add expanded XBRL tag configuration to sec_fundamentals.py**

Replace the existing tag tuples section (lines 56-78) and add the new ones below them. Keep the existing tags and add new ones:

```python
# ── Expanded XBRL tags for financial_facts_raw pipeline ──────────
# Maps a canonical concept name to XBRL tag priority lists.
# Key = column name in financial_periods_raw; value = (tag1, tag2, ...) in priority order.
# The first matching tag with data is used.

TRACKED_CONCEPTS: dict[str, tuple[str, ...]] = {
    # Income Statement
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
    ),
    "cost_of_revenue": ("CostOfGoodsAndServicesSold", "CostOfRevenue"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss",),
    "eps_basic": ("EarningsPerShareBasic",),
    "eps_diluted": ("EarningsPerShareDiluted",),
    "research_and_dev": ("ResearchAndDevelopmentExpense",),
    "sga_expense": ("SellingGeneralAndAdministrativeExpense",),
    "depreciation_amort": (
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
    ),
    "interest_expense": ("InterestExpense", "InterestExpenseDebt"),
    "income_tax": ("IncomeTaxExpenseBenefit",),
    "shares_basic": ("WeightedAverageNumberOfSharesOutstandingBasic",),
    "shares_diluted": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
    "sbc_expense": ("AllocatedShareBasedCompensationExpense", "ShareBasedCompensation"),
    # Balance Sheet
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "shareholders_equity": (
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ),
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
    ),
    "long_term_debt": ("LongTermDebt", "LongTermDebtNoncurrent"),
    "short_term_debt": ("ShortTermBorrowings", "CommercialPaper"),
    "shares_outstanding": ("CommonStockSharesOutstanding",),
    "inventory": ("InventoryNet",),
    "receivables": ("AccountsReceivableNetCurrent",),
    "payables": ("AccountsPayableCurrent",),
    "goodwill": ("Goodwill",),
    "ppe_net": ("PropertyPlantAndEquipmentNet",),
    # Cash Flow
    "operating_cf": ("NetCashProvidedByUsedInOperatingActivities",),
    "investing_cf": ("NetCashProvidedByUsedInInvestingActivities",),
    "financing_cf": ("NetCashProvidedByUsedInFinancingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpenditures"),
    "dividends_paid": ("PaymentsOfDividends",),
    "dps_declared": ("CommonStockDividendsPerShareDeclared",),
    "buyback_spend": ("PaymentsForRepurchaseOfCommonStock",),
}

# All unique XBRL tag names (for efficient lookup during extraction)
_ALL_TRACKED_TAGS: frozenset[str] = frozenset(
    tag for tags in TRACKED_CONCEPTS.values() for tag in tags
)
```

- [ ] **Step 6: Commit**

```bash
git add app/providers/fundamentals.py app/providers/implementations/sec_fundamentals.py tests/test_xbrl_fact_extraction.py
git commit -m "feat: add XbrlFact dataclass and expanded XBRL tag configuration

Adds XbrlFact frozen dataclass for individual XBRL facts.
Adds TRACKED_CONCEPTS dict mapping 30 canonical concept names to
XBRL tag priority lists (revenue, cost_of_revenue, gross_profit,
operating_income, net_income, EPS, R&D, SG&A, D&A, interest, tax,
shares, SBC, total_assets, total_liabilities, equity, cash, debt,
inventory, receivables, payables, goodwill, PP&E, operating_cf,
investing_cf, financing_cf, capex, dividends, DPS, buybacks)."
```

---

### Task 3: Fact Extraction Method on SecFundamentalsProvider

**Files:**
- Modify: `app/providers/implementations/sec_fundamentals.py`
- Test: `tests/test_xbrl_fact_extraction.py`

- [ ] **Step 1: Write failing tests for extract_facts**

Add to `tests/test_xbrl_fact_extraction.py`:

```python
from app.providers.implementations.sec_fundamentals import (
    _extract_facts_from_gaap,
)


def _make_companyfacts(gaap_facts: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal SEC companyfacts JSON structure."""
    return {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": gaap_facts,
        },
    }


def _make_xbrl_entry(
    *,
    end: str,
    val: float,
    form: str = "10-Q",
    fp: str = "Q1",
    fy: int = 2024,
    filed: str = "2024-05-01",
    accn: str = "0000320193-24-000042",
    start: str | None = "2024-01-01",
    frame: str | None = "CY2024Q1",
    decimals: int = -3,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "end": end,
        "val": val,
        "form": form,
        "fp": fp,
        "fy": fy,
        "filed": filed,
        "accn": accn,
        "frame": frame,
    }
    if start is not None:
        entry["start"] = start
    if decimals is not None:
        entry["decimals"] = decimals
    return entry


class TestExtractFactsFromGaap:
    def test_extracts_revenue_facts(self) -> None:
        gaap = {
            "Revenues": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(end="2024-03-31", val=50_000_000.0),
                        _make_xbrl_entry(
                            end="2024-06-30", val=55_000_000.0,
                            fp="Q2", frame="CY2024Q2",
                            accn="0000320193-24-000050",
                        ),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 2
        assert facts[0].concept == "Revenues"
        assert facts[0].val == Decimal("50000000")
        assert facts[0].period_end == date(2024, 3, 31)
        assert facts[0].frame == "CY2024Q1"

    def test_uses_priority_tag_order(self) -> None:
        """If ASC 606 tag exists, it's used over legacy Revenues."""
        gaap = {
            "RevenueFromContractWithCustomerExcludingAssessedTax": {
                "units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=50_000_000.0)]}
            },
            "Revenues": {
                "units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=49_000_000.0)]}
            },
        }
        facts = _extract_facts_from_gaap(gaap)
        revenue_facts = [f for f in facts if f.concept == "RevenueFromContractWithCustomerExcludingAssessedTax"]
        legacy_facts = [f for f in facts if f.concept == "Revenues"]
        # Both tags are extracted — the priority logic is in normalization, not extraction.
        # extract_facts stores ALL matching tags.
        assert len(revenue_facts) == 1
        assert len(legacy_facts) == 1

    def test_handles_instant_items(self) -> None:
        """Balance sheet items have no start date."""
        gaap = {
            "Assets": {
                "units": {
                    "USD": [
                        _make_xbrl_entry(
                            end="2024-03-31", val=300_000_000_000.0,
                            start=None, frame=None,
                        ),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].period_start is None
        assert facts[0].concept == "Assets"

    def test_handles_shares_unit(self) -> None:
        gaap = {
            "CommonStockSharesOutstanding": {
                "units": {
                    "shares": [
                        _make_xbrl_entry(
                            end="2024-03-31", val=15_334_000_000.0,
                            start=None, frame=None,
                        ),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].unit == "shares"

    def test_handles_per_share_unit(self) -> None:
        gaap = {
            "EarningsPerShareDiluted": {
                "units": {
                    "USD/shares": [
                        _make_xbrl_entry(end="2024-03-31", val=1.53),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
        assert facts[0].unit == "USD/shares"

    def test_skips_untracked_tags(self) -> None:
        gaap = {
            "SomeRandomTag": {
                "units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=999.0)]}
            },
            "Assets": {
                "units": {"USD": [_make_xbrl_entry(end="2024-03-31", val=100.0, start=None, frame=None)]}
            },
        }
        facts = _extract_facts_from_gaap(gaap)
        concepts = {f.concept for f in facts}
        assert "SomeRandomTag" not in concepts
        assert "Assets" in concepts

    def test_empty_gaap_returns_empty(self) -> None:
        facts = _extract_facts_from_gaap({})
        assert facts == []

    def test_missing_required_fields_skips_entry(self) -> None:
        """Entries without 'end' or 'val' are skipped."""
        gaap = {
            "Revenues": {
                "units": {
                    "USD": [
                        {"form": "10-Q", "fp": "Q1"},  # missing end and val
                        _make_xbrl_entry(end="2024-03-31", val=50_000_000.0),
                    ]
                }
            }
        }
        facts = _extract_facts_from_gaap(gaap)
        assert len(facts) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_xbrl_fact_extraction.py::TestExtractFactsFromGaap -v`
Expected: FAIL with `ImportError: cannot import name '_extract_facts_from_gaap'`

- [ ] **Step 3: Implement _extract_facts_from_gaap**

Add to `app/providers/implementations/sec_fundamentals.py`, after the `_zero_pad_cik` function and before the `SecFundamentalsProvider` class:

```python
from app.providers.fundamentals import XbrlFact

# Unit type priority: monetary → per-share → share count → pure (ratio)
_UNIT_PRIORITY = ("USD", "USD/shares", "shares", "pure")


def _extract_facts_from_gaap(gaap: dict[str, Any]) -> list[XbrlFact]:
    """Extract all tracked XBRL facts from a companyfacts us-gaap section.

    Returns one XbrlFact per (tag, unit, entry) combination for every tag
    in ``_ALL_TRACKED_TAGS`` that appears in the gaap dict.  Entries missing
    required fields (``end``, ``val``, ``accn``, ``form``, ``filed``) are
    skipped with a debug log.
    """
    facts: list[XbrlFact] = []
    for tag_name, fact_data in gaap.items():
        if tag_name not in _ALL_TRACKED_TAGS:
            continue
        units = fact_data.get("units", {})
        for unit_key in _UNIT_PRIORITY:
            entries = units.get(unit_key)
            if not entries:
                continue
            for entry in entries:
                try:
                    end_str = entry["end"]
                    val = entry["val"]
                    accn = entry["accn"]
                    form = entry["form"]
                    filed_str = entry["filed"]
                except KeyError:
                    logger.debug(
                        "Skipping XBRL entry for %s: missing required field", tag_name
                    )
                    continue

                start_str = entry.get("start")
                try:
                    period_end = date.fromisoformat(end_str)
                    period_start = date.fromisoformat(start_str) if start_str else None
                    filed_date = date.fromisoformat(filed_str)
                except (ValueError, TypeError):
                    logger.debug(
                        "Skipping XBRL entry for %s: bad date format", tag_name
                    )
                    continue

                facts.append(
                    XbrlFact(
                        concept=tag_name,
                        taxonomy="us-gaap",
                        unit=unit_key,
                        period_start=period_start,
                        period_end=period_end,
                        val=Decimal(str(val)),
                        frame=entry.get("frame"),
                        accession_number=accn,
                        form_type=form,
                        filed_date=filed_date,
                        fiscal_year=entry.get("fy"),
                        fiscal_period=entry.get("fp"),
                        decimals=entry.get("decimals"),
                    )
                )
            # Don't break after first unit type — extract all unit types for this tag.
            # A tag like EPS appears under USD/shares, while shares_outstanding under shares.

    return facts
```

- [ ] **Step 4: Add extract_facts method to SecFundamentalsProvider**

Add to the `SecFundamentalsProvider` class, after the `get_snapshot_history_by_cik` method:

```python
    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        """Extract all tracked XBRL facts from SEC companyfacts.

        Returns a list of XbrlFact objects for all tracked concepts.
        Returns empty list if companyfacts not available for this CIK.
        """
        raw = self._fetch_company_facts(cik)
        if raw is None:
            return []
        gaap = raw.get("facts", {}).get("us-gaap", {})
        if not gaap:
            logger.info("No us-gaap facts for %s (CIK %s)", symbol, cik)
            return []
        return _extract_facts_from_gaap(gaap)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_xbrl_fact_extraction.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add app/providers/implementations/sec_fundamentals.py tests/test_xbrl_fact_extraction.py
git commit -m "feat: add XBRL fact extraction from SEC companyfacts

Adds _extract_facts_from_gaap() pure function that extracts all tracked
XBRL concepts from a companyfacts us-gaap section. Returns list[XbrlFact]
with full provenance (accession_number, form_type, filed_date, frame).
Adds extract_facts() method to SecFundamentalsProvider."
```

---

### Task 4: Financial Facts Service — Fetch and Store

**Files:**
- Create: `app/services/financial_facts.py`
- Test: `tests/test_financial_facts_service.py`

- [ ] **Step 1: Write failing test for ingestion run tracking**

Create `tests/test_financial_facts_service.py`:

```python
"""Tests for financial facts service — XBRL fact storage and ingestion tracking."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.providers.fundamentals import XbrlFact
from app.services.financial_facts import (
    FactsRefreshSummary,
    _start_ingestion_run,
    _finish_ingestion_run,
    _upsert_facts,
)


def _make_fact(
    *,
    concept: str = "Revenues",
    val: Decimal = Decimal("50000000"),
    period_end: date = date(2024, 3, 31),
    period_start: date | None = date(2024, 1, 1),
    frame: str | None = "CY2024Q1",
    accession_number: str = "0000320193-24-000042",
    form_type: str = "10-Q",
    filed_date: date = date(2024, 5, 1),
    fiscal_year: int | None = 2024,
    fiscal_period: str | None = "Q1",
    unit: str = "USD",
) -> XbrlFact:
    return XbrlFact(
        concept=concept,
        taxonomy="us-gaap",
        unit=unit,
        period_start=period_start,
        period_end=period_end,
        val=val,
        frame=frame,
        accession_number=accession_number,
        form_type=form_type,
        filed_date=filed_date,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        decimals=-3,
    )


class TestStartIngestionRun:
    def test_returns_run_id(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (42,)
        conn.execute.return_value = cursor
        run_id = _start_ingestion_run(conn, source="sec_edgar", endpoint="/api/xbrl/companyfacts", instrument_count=5)
        assert run_id == 42
        conn.execute.assert_called_once()


class TestFinishIngestionRun:
    def test_updates_run_status(self) -> None:
        conn = MagicMock()
        _finish_ingestion_run(conn, run_id=42, status="success", rows_upserted=100, rows_skipped=3)
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args
        # Verify the SQL updates finished_at and status
        sql = call_args[0][0]
        assert "finished_at" in sql
        assert "status" in sql


class TestUpsertFacts:
    def test_upserts_single_fact(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.rowcount = 1
        conn.execute.return_value = cursor
        facts = [_make_fact()]
        upserted, skipped = _upsert_facts(conn, instrument_id=1, facts=facts, ingestion_run_id=42)
        assert upserted == 1
        assert skipped == 0

    def test_handles_empty_facts(self) -> None:
        conn = MagicMock()
        upserted, skipped = _upsert_facts(conn, instrument_id=1, facts=[], ingestion_run_id=42)
        assert upserted == 0
        assert skipped == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_financial_facts_service.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.services.financial_facts'`

- [ ] **Step 3: Implement financial_facts service**

Create `app/services/financial_facts.py`:

```python
"""Financial facts service — fetch XBRL facts and store in financial_facts_raw.

Orchestrates:
  1. Start an ingestion run (audit trail)
  2. For each instrument, call provider.extract_facts() to get XBRL facts
  3. Upsert facts into financial_facts_raw
  4. Finish the ingestion run with summary counts
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence

import psycopg

from app.providers.fundamentals import XbrlFact

if TYPE_CHECKING:
    from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FactsRefreshSummary:
    symbols_attempted: int
    facts_upserted: int
    facts_skipped: int
    symbols_failed: int


def _start_ingestion_run(
    conn: psycopg.Connection[tuple],
    *,
    source: str,
    endpoint: str | None = None,
    instrument_count: int | None = None,
) -> int:
    """Insert a new data_ingestion_runs row with status='running'. Returns the run ID."""
    cur = conn.execute(
        """
        INSERT INTO data_ingestion_runs (source, endpoint, instrument_count)
        VALUES (%(source)s, %(endpoint)s, %(instrument_count)s)
        RETURNING ingestion_run_id
        """,
        {"source": source, "endpoint": endpoint, "instrument_count": instrument_count},
    )
    row = cur.fetchone()
    assert row is not None
    return row[0]  # type: ignore[no-any-return]


def _finish_ingestion_run(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    status: str,
    rows_upserted: int = 0,
    rows_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Update an ingestion run with final status and counts."""
    conn.execute(
        """
        UPDATE data_ingestion_runs
        SET finished_at = NOW(),
            status = %(status)s,
            rows_upserted = %(rows_upserted)s,
            rows_skipped = %(rows_skipped)s,
            error = %(error)s
        WHERE ingestion_run_id = %(run_id)s
        """,
        {
            "run_id": run_id,
            "status": status,
            "rows_upserted": rows_upserted,
            "rows_skipped": rows_skipped,
            "error": error,
        },
    )


def _upsert_facts(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    facts: Sequence[XbrlFact],
    ingestion_run_id: int,
) -> tuple[int, int]:
    """Upsert XBRL facts into financial_facts_raw.

    Returns (upserted_count, skipped_count).
    Uses ON CONFLICT DO UPDATE so restatements overwrite prior values.
    """
    if not facts:
        return 0, 0

    upserted = 0
    skipped = 0
    for fact in facts:
        cur = conn.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit,
                period_start, period_end, val, frame,
                accession_number, form_type, filed_date,
                fiscal_year, fiscal_period, decimals,
                ingestion_run_id
            ) VALUES (
                %(instrument_id)s, %(taxonomy)s, %(concept)s, %(unit)s,
                %(period_start)s, %(period_end)s, %(val)s, %(frame)s,
                %(accession_number)s, %(form_type)s, %(filed_date)s,
                %(fiscal_year)s, %(fiscal_period)s, %(decimals)s,
                %(ingestion_run_id)s
            )
            ON CONFLICT (instrument_id, concept, unit, period_end, accession_number)
            DO UPDATE SET
                val = EXCLUDED.val,
                frame = EXCLUDED.frame,
                form_type = EXCLUDED.form_type,
                filed_date = EXCLUDED.filed_date,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_period = EXCLUDED.fiscal_period,
                decimals = EXCLUDED.decimals,
                ingestion_run_id = EXCLUDED.ingestion_run_id,
                fetched_at = NOW()
            WHERE financial_facts_raw.val IS DISTINCT FROM EXCLUDED.val
               OR financial_facts_raw.frame IS DISTINCT FROM EXCLUDED.frame
            """,
            {
                "instrument_id": instrument_id,
                "taxonomy": fact.taxonomy,
                "concept": fact.concept,
                "unit": fact.unit,
                "period_start": fact.period_start,
                "period_end": fact.period_end,
                "val": fact.val,
                "frame": fact.frame,
                "accession_number": fact.accession_number,
                "form_type": fact.form_type,
                "filed_date": fact.filed_date,
                "fiscal_year": fact.fiscal_year,
                "fiscal_period": fact.fiscal_period,
                "decimals": fact.decimals,
                "ingestion_run_id": ingestion_run_id,
            },
        )
        if cur.rowcount > 0:
            upserted += 1
        else:
            skipped += 1

    return upserted, skipped


def refresh_financial_facts(
    provider: SecFundamentalsProvider,
    conn: psycopg.Connection[tuple],
    symbols: Sequence[tuple[str, int, str]],
) -> FactsRefreshSummary:
    """Fetch and store XBRL facts for all given symbols.

    Parameters
    ----------
    symbols:
        List of (symbol, instrument_id, cik) tuples.
    """
    run_id = _start_ingestion_run(
        conn,
        source="sec_edgar",
        endpoint="/api/xbrl/companyfacts",
        instrument_count=len(symbols),
    )

    total_upserted = 0
    total_skipped = 0
    failed = 0

    for symbol, instrument_id, cik in symbols:
        try:
            with conn.transaction():
                facts = provider.extract_facts(symbol, cik)
                if not facts:
                    logger.info("No XBRL facts for %s (CIK %s)", symbol, cik)
                    continue
                upserted, skipped = _upsert_facts(
                    conn,
                    instrument_id=instrument_id,
                    facts=facts,
                    ingestion_run_id=run_id,
                )
                total_upserted += upserted
                total_skipped += skipped
                logger.info(
                    "SEC facts for %s: %d upserted, %d skipped",
                    symbol, upserted, skipped,
                )
        except Exception:
            failed += 1
            logger.exception("Failed to refresh SEC facts for %s", symbol)

    status = "success" if failed == 0 else ("partial" if total_upserted > 0 else "failed")
    _finish_ingestion_run(
        conn,
        run_id=run_id,
        status=status,
        rows_upserted=total_upserted,
        rows_skipped=total_skipped,
        error=f"{failed} symbols failed" if failed > 0 else None,
    )

    return FactsRefreshSummary(
        symbols_attempted=len(symbols),
        facts_upserted=total_upserted,
        facts_skipped=total_skipped,
        symbols_failed=failed,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_financial_facts_service.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/financial_facts.py tests/test_financial_facts_service.py
git commit -m "feat: add financial facts service for XBRL fact storage

Adds refresh_financial_facts() service that fetches XBRL facts via
SecFundamentalsProvider.extract_facts() and upserts into financial_facts_raw.
Includes ingestion run tracking in data_ingestion_runs for audit trail.
Per-symbol savepoint isolation so one failure doesn't block others."
```

---

### Task 5: Normalization Service — Facts to Period Rows

**Files:**
- Create: `app/services/financial_normalization.py`
- Test: `tests/test_financial_normalization.py`

This is the core of the pipeline: reading raw XBRL facts from `financial_facts_raw` and deriving wide `financial_periods_raw` rows with YTD disambiguation, Q4 derivation, and period metadata.

- [ ] **Step 1: Write failing test for period derivation from facts**

Create `tests/test_financial_normalization.py`:

```python
"""Tests for financial normalization — facts_raw → periods_raw → canonical."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from app.services.financial_normalization import (
    _derive_periods_from_facts,
    FactRow,
    PeriodRow,
)


def _fact(
    *,
    concept: str = "Revenues",
    val: Decimal = Decimal("50000000"),
    period_end: str = "2024-03-31",
    period_start: str | None = "2024-01-01",
    frame: str | None = "CY2024Q1",
    form_type: str = "10-Q",
    fiscal_year: int = 2024,
    fiscal_period: str = "Q1",
    accession_number: str = "accn-q1",
    filed_date: str = "2024-05-01",
    unit: str = "USD",
) -> FactRow:
    return FactRow(
        concept=concept,
        unit=unit,
        period_start=date.fromisoformat(period_start) if period_start else None,
        period_end=date.fromisoformat(period_end),
        val=val,
        frame=frame,
        form_type=form_type,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        accession_number=accession_number,
        filed_date=date.fromisoformat(filed_date),
    )


class TestDerivePeriodsFromFacts:
    def test_single_quarter_revenue(self) -> None:
        """A single Q1 revenue fact produces one period row with revenue populated."""
        facts = [_fact()]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        assert p.period_type == "Q1"
        assert p.fiscal_year == 2024
        assert p.fiscal_quarter == 1
        assert p.revenue == Decimal("50000000")
        assert p.period_end_date == date(2024, 3, 31)
        assert p.period_start_date == date(2024, 1, 1)
        assert p.months_covered == 3
        assert p.source == "sec_edgar"
        assert not p.is_derived

    def test_multiple_concepts_same_period(self) -> None:
        """Multiple concepts for the same period merge into one period row."""
        facts = [
            _fact(concept="Revenues", val=Decimal("100000000")),
            _fact(concept="GrossProfit", val=Decimal("40000000")),
            _fact(concept="NetIncomeLoss", val=Decimal("20000000")),
            _fact(
                concept="Assets", val=Decimal("500000000"),
                period_start=None, frame=None,
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        assert p.revenue == Decimal("100000000")
        assert p.gross_profit == Decimal("40000000")
        assert p.net_income == Decimal("20000000")
        assert p.total_assets == Decimal("500000000")

    def test_fy_period_type(self) -> None:
        facts = [
            _fact(
                fiscal_period="FY", fiscal_year=2024,
                period_end="2024-12-31", period_start="2024-01-01",
                frame="CY2024", form_type="10-K",
                accession_number="accn-fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        assert periods[0].period_type == "FY"
        assert periods[0].months_covered == 12

    def test_derives_q4_from_fy_minus_quarters(self) -> None:
        """Q4 = FY - Q1 - Q2 - Q3 when Q4 not directly filed."""
        facts = [
            _fact(fiscal_period="Q1", val=Decimal("100"), period_end="2024-03-31",
                  period_start="2024-01-01", frame="CY2024Q1", accession_number="q1"),
            _fact(fiscal_period="Q2", val=Decimal("120"), period_end="2024-06-30",
                  period_start="2024-04-01", frame="CY2024Q2", accession_number="q2"),
            _fact(fiscal_period="Q3", val=Decimal("110"), period_end="2024-09-30",
                  period_start="2024-07-01", frame="CY2024Q3", accession_number="q3"),
            _fact(fiscal_period="FY", fiscal_year=2024, val=Decimal("500"),
                  period_end="2024-12-31", period_start="2024-01-01",
                  frame="CY2024", form_type="10-K", accession_number="fy"),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4_periods = [p for p in periods if p.period_type == "Q4"]
        assert len(q4_periods) == 1
        q4 = q4_periods[0]
        assert q4.revenue == Decimal("170")  # 500 - 100 - 120 - 110
        assert q4.is_derived is True
        assert q4.fiscal_year == 2024
        assert q4.fiscal_quarter == 4

    def test_skips_ytd_entries(self) -> None:
        """Entries without frame (YTD cumulative) are excluded — we only want
        standalone quarterly or annual values identified by frame."""
        facts = [
            _fact(frame="CY2024Q1"),  # standalone quarter — include
            _fact(
                frame=None, period_end="2024-06-30",
                period_start="2024-01-01", fiscal_period="Q2",
                accession_number="ytd-q2",
            ),  # YTD Q1+Q2 cumulative — exclude
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        # Only the framed Q1 should produce a period
        assert len(periods) == 1
        assert periods[0].period_type == "Q1"

    def test_tag_priority_picks_first_match(self) -> None:
        """When multiple tags map to the same concept (e.g. revenue),
        the highest-priority tag's value is used."""
        facts = [
            _fact(concept="RevenueFromContractWithCustomerExcludingAssessedTax",
                  val=Decimal("100")),
            _fact(concept="Revenues", val=Decimal("95")),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        # ASC 606 tag has priority
        assert periods[0].revenue == Decimal("100")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_financial_normalization.py::TestDerivePeriodsFromFacts -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement normalization service with FactRow, PeriodRow, and _derive_periods_from_facts**

Create `app/services/financial_normalization.py`:

```python
"""Financial normalization service.

Derives financial_periods_raw from financial_facts_raw, then merges
into canonical financial_periods.

Pipeline:
  financial_facts_raw → _derive_periods_from_facts() → financial_periods_raw
  financial_periods_raw → _canonical_merge() → financial_periods
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field, fields
from datetime import date
from decimal import Decimal
from typing import Any, Sequence

import psycopg

from app.providers.implementations.sec_fundamentals import TRACKED_CONCEPTS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FactRow:
    """Minimal fact representation for normalization (subset of DB columns)."""

    concept: str
    unit: str
    period_start: date | None
    period_end: date
    val: Decimal
    frame: str | None
    form_type: str
    fiscal_year: int
    fiscal_period: str
    accession_number: str
    filed_date: date


# Build reverse map: XBRL tag → (canonical_column, priority_index)
# Lower priority_index = higher priority.
_TAG_TO_COLUMN: dict[str, tuple[str, int]] = {}
for col_name, tags in TRACKED_CONCEPTS.items():
    for idx, tag in enumerate(tags):
        _TAG_TO_COLUMN[tag] = (col_name, idx)

# Financial columns that are flow items (income/CF — get summed in TTM).
# Balance sheet items are point-in-time (latest value used in TTM).
_FLOW_COLUMNS: frozenset[str] = frozenset({
    "revenue", "cost_of_revenue", "gross_profit", "operating_income",
    "net_income", "eps_basic", "eps_diluted", "research_and_dev",
    "sga_expense", "depreciation_amort", "interest_expense", "income_tax",
    "sbc_expense", "operating_cf", "investing_cf", "financing_cf",
    "capex", "dividends_paid", "dps_declared", "buyback_spend",
    # shares_basic/shares_diluted are weighted averages for a period, so they
    # belong to the period, but TTM uses latest rather than sum.
})

_BALANCE_SHEET_COLUMNS: frozenset[str] = frozenset({
    "total_assets", "total_liabilities", "shareholders_equity", "cash",
    "long_term_debt", "short_term_debt", "shares_outstanding",
    "inventory", "receivables", "payables", "goodwill", "ppe_net",
})

# All financial columns on PeriodRow
_ALL_FINANCIAL_COLUMNS: frozenset[str] = _FLOW_COLUMNS | _BALANCE_SHEET_COLUMNS | {
    "shares_basic", "shares_diluted",
}

# Fiscal period label → (period_type, fiscal_quarter)
_FP_MAP: dict[str, tuple[str, int | None]] = {
    "Q1": ("Q1", 1),
    "Q2": ("Q2", 2),
    "Q3": ("Q3", 3),
    "Q4": ("Q4", 4),
    "FY": ("FY", None),
}


@dataclass
class PeriodRow:
    """Wide period row ready for insertion into financial_periods_raw."""

    period_end_date: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    period_start_date: date | None
    months_covered: int | None

    # Financial columns — all optional
    revenue: Decimal | None = None
    cost_of_revenue: Decimal | None = None
    gross_profit: Decimal | None = None
    operating_income: Decimal | None = None
    net_income: Decimal | None = None
    eps_basic: Decimal | None = None
    eps_diluted: Decimal | None = None
    research_and_dev: Decimal | None = None
    sga_expense: Decimal | None = None
    depreciation_amort: Decimal | None = None
    interest_expense: Decimal | None = None
    income_tax: Decimal | None = None
    shares_basic: Decimal | None = None
    shares_diluted: Decimal | None = None
    sbc_expense: Decimal | None = None

    total_assets: Decimal | None = None
    total_liabilities: Decimal | None = None
    shareholders_equity: Decimal | None = None
    cash: Decimal | None = None
    long_term_debt: Decimal | None = None
    short_term_debt: Decimal | None = None
    shares_outstanding: Decimal | None = None
    inventory: Decimal | None = None
    receivables: Decimal | None = None
    payables: Decimal | None = None
    goodwill: Decimal | None = None
    ppe_net: Decimal | None = None

    operating_cf: Decimal | None = None
    investing_cf: Decimal | None = None
    financing_cf: Decimal | None = None
    capex: Decimal | None = None
    dividends_paid: Decimal | None = None
    dps_declared: Decimal | None = None
    buyback_spend: Decimal | None = None

    # Provenance
    source: str = "sec_edgar"
    source_ref: str = ""
    reported_currency: str = "USD"
    form_type: str | None = None
    filed_date: date | None = None
    is_restated: bool = False
    is_derived: bool = False


def _months_between(start: date | None, end: date) -> int | None:
    """Approximate months between two dates."""
    if start is None:
        return None
    delta_days = (end - start).days
    if delta_days <= 0:
        return None
    return round(delta_days / 30.44)


def _derive_periods_from_facts(
    facts: Sequence[FactRow],
    reported_currency: str = "USD",
) -> list[PeriodRow]:
    """Derive wide period rows from individual XBRL facts.

    Groups facts by (fiscal_year, fiscal_period) and merges values into
    PeriodRow objects. Uses tag priority to pick the best value when
    multiple XBRL tags map to the same canonical column.

    YTD disambiguation: only facts with a non-null ``frame`` field are
    included for duration items (income/CF). Instant items (balance sheet)
    are always included regardless of frame.

    Q4 derivation: if FY exists but Q4 does not, derives Q4 = FY - Q1 - Q2 - Q3
    for all flow columns.
    """
    # Group facts by (fiscal_year, fiscal_period)
    grouped: dict[tuple[int, str], list[FactRow]] = defaultdict(list)
    for fact in facts:
        fp = fact.fiscal_period
        if fp not in _FP_MAP:
            continue  # skip unknown periods (e.g. 'H1', '9M')

        is_instant = fact.period_start is None
        is_duration = not is_instant

        # YTD disambiguation: for duration items, require frame to be set.
        # Entries without frame are YTD cumulative — exclude them.
        if is_duration and fact.frame is None:
            continue

        grouped[(fact.fiscal_year, fp)].append(fact)

    # Build period rows
    periods: list[PeriodRow] = []
    for (fy, fp), period_facts in grouped.items():
        period_type, fiscal_quarter = _FP_MAP[fp]

        # Determine period dates from facts
        period_end = max(f.period_end for f in period_facts)
        starts = [f.period_start for f in period_facts if f.period_start is not None]
        period_start = min(starts) if starts else None
        months = _months_between(period_start, period_end)

        # Collect accession numbers for source_ref
        accession_numbers = sorted({f.accession_number for f in period_facts})
        source_ref = accession_numbers[0] if len(accession_numbers) == 1 else ",".join(accession_numbers)

        # Find the most recent filed_date and form_type
        latest_filing = max(period_facts, key=lambda f: f.filed_date)

        row = PeriodRow(
            period_end_date=period_end,
            period_type=period_type,
            fiscal_year=fy,
            fiscal_quarter=fiscal_quarter,
            period_start_date=period_start,
            months_covered=months,
            source="sec_edgar",
            source_ref=source_ref,
            reported_currency=reported_currency,
            form_type=latest_filing.form_type,
            filed_date=latest_filing.filed_date,
        )

        # Apply values with tag priority
        # Track which columns have been set and at what priority
        col_priority: dict[str, int] = {}
        for fact in period_facts:
            mapping = _TAG_TO_COLUMN.get(fact.concept)
            if mapping is None:
                continue
            col_name, priority = mapping
            current_priority = col_priority.get(col_name)
            if current_priority is not None and priority >= current_priority:
                continue  # existing value has higher or equal priority
            setattr(row, col_name, fact.val)
            col_priority[col_name] = priority

        periods.append(row)

    # Q4 derivation: if FY exists but Q4 does not, derive Q4 = FY - Q1 - Q2 - Q3
    fy_periods = {p.fiscal_year: p for p in periods if p.period_type == "FY"}
    existing_quarters: dict[int, dict[str, PeriodRow]] = defaultdict(dict)
    for p in periods:
        if p.period_type in ("Q1", "Q2", "Q3", "Q4"):
            existing_quarters[p.fiscal_year][p.period_type] = p

    for fy_year, fy_row in fy_periods.items():
        quarters = existing_quarters.get(fy_year, {})
        if "Q4" in quarters:
            continue  # Q4 already exists
        if not all(q in quarters for q in ("Q1", "Q2", "Q3")):
            continue  # need all three quarters to derive Q4

        q1, q2, q3 = quarters["Q1"], quarters["Q2"], quarters["Q3"]

        # Determine Q4 period dates
        q3_end = q3.period_end_date
        q4_start = date(q3_end.year, q3_end.month + 1, 1) if q3_end.month < 12 else date(q3_end.year + 1, 1, 1)
        q4_end = fy_row.period_end_date

        q4 = PeriodRow(
            period_end_date=q4_end,
            period_type="Q4",
            fiscal_year=fy_year,
            fiscal_quarter=4,
            period_start_date=q4_start,
            months_covered=_months_between(q4_start, q4_end),
            source="sec_edgar",
            source_ref=fy_row.source_ref,
            reported_currency=reported_currency,
            form_type=fy_row.form_type,
            filed_date=fy_row.filed_date,
            is_derived=True,
        )

        # Derive flow columns: Q4 = FY - Q1 - Q2 - Q3
        for col in _FLOW_COLUMNS:
            fy_val = getattr(fy_row, col)
            if fy_val is None:
                continue
            q1_val = getattr(q1, col) or Decimal(0)
            q2_val = getattr(q2, col) or Decimal(0)
            q3_val = getattr(q3, col) or Decimal(0)
            derived = fy_val - q1_val - q2_val - q3_val
            setattr(q4, col, derived)

        # Balance sheet columns: use FY values (they're the same date as Q4 end)
        for col in _BALANCE_SHEET_COLUMNS:
            fy_val = getattr(fy_row, col)
            if fy_val is not None:
                setattr(q4, col, fy_val)

        # Weighted average shares: use FY values for Q4 (approximate)
        for col in ("shares_basic", "shares_diluted"):
            fy_val = getattr(fy_row, col)
            if fy_val is not None:
                setattr(q4, col, fy_val)

        periods.append(q4)

    return periods


def _upsert_period_raw(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period: PeriodRow,
    ingestion_run_id: int | None = None,
) -> bool:
    """Upsert a single period row into financial_periods_raw.

    Returns True if a row was inserted/updated, False if skipped (unchanged).
    """
    cur = conn.execute(
        """
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, period_start_date, months_covered,
            revenue, cost_of_revenue, gross_profit, operating_income,
            net_income, eps_basic, eps_diluted, research_and_dev,
            sga_expense, depreciation_amort, interest_expense, income_tax,
            shares_basic, shares_diluted, sbc_expense,
            total_assets, total_liabilities, shareholders_equity, cash,
            long_term_debt, short_term_debt, shares_outstanding,
            inventory, receivables, payables, goodwill, ppe_net,
            operating_cf, investing_cf, financing_cf, capex,
            dividends_paid, dps_declared, buyback_spend,
            source, source_ref, reported_currency,
            form_type, filed_date, is_restated, is_derived,
            ingestion_run_id
        ) VALUES (
            %(instrument_id)s, %(period_end_date)s, %(period_type)s,
            %(fiscal_year)s, %(fiscal_quarter)s, %(period_start_date)s, %(months_covered)s,
            %(revenue)s, %(cost_of_revenue)s, %(gross_profit)s, %(operating_income)s,
            %(net_income)s, %(eps_basic)s, %(eps_diluted)s, %(research_and_dev)s,
            %(sga_expense)s, %(depreciation_amort)s, %(interest_expense)s, %(income_tax)s,
            %(shares_basic)s, %(shares_diluted)s, %(sbc_expense)s,
            %(total_assets)s, %(total_liabilities)s, %(shareholders_equity)s, %(cash)s,
            %(long_term_debt)s, %(short_term_debt)s, %(shares_outstanding)s,
            %(inventory)s, %(receivables)s, %(payables)s, %(goodwill)s, %(ppe_net)s,
            %(operating_cf)s, %(investing_cf)s, %(financing_cf)s, %(capex)s,
            %(dividends_paid)s, %(dps_declared)s, %(buyback_spend)s,
            %(source)s, %(source_ref)s, %(reported_currency)s,
            %(form_type)s, %(filed_date)s, %(is_restated)s, %(is_derived)s,
            %(ingestion_run_id)s
        )
        ON CONFLICT (instrument_id, period_end_date, period_type, source, source_ref)
        DO UPDATE SET
            revenue = EXCLUDED.revenue,
            cost_of_revenue = EXCLUDED.cost_of_revenue,
            gross_profit = EXCLUDED.gross_profit,
            operating_income = EXCLUDED.operating_income,
            net_income = EXCLUDED.net_income,
            eps_basic = EXCLUDED.eps_basic,
            eps_diluted = EXCLUDED.eps_diluted,
            research_and_dev = EXCLUDED.research_and_dev,
            sga_expense = EXCLUDED.sga_expense,
            depreciation_amort = EXCLUDED.depreciation_amort,
            interest_expense = EXCLUDED.interest_expense,
            income_tax = EXCLUDED.income_tax,
            shares_basic = EXCLUDED.shares_basic,
            shares_diluted = EXCLUDED.shares_diluted,
            sbc_expense = EXCLUDED.sbc_expense,
            total_assets = EXCLUDED.total_assets,
            total_liabilities = EXCLUDED.total_liabilities,
            shareholders_equity = EXCLUDED.shareholders_equity,
            cash = EXCLUDED.cash,
            long_term_debt = EXCLUDED.long_term_debt,
            short_term_debt = EXCLUDED.short_term_debt,
            shares_outstanding = EXCLUDED.shares_outstanding,
            inventory = EXCLUDED.inventory,
            receivables = EXCLUDED.receivables,
            payables = EXCLUDED.payables,
            goodwill = EXCLUDED.goodwill,
            ppe_net = EXCLUDED.ppe_net,
            operating_cf = EXCLUDED.operating_cf,
            investing_cf = EXCLUDED.investing_cf,
            financing_cf = EXCLUDED.financing_cf,
            capex = EXCLUDED.capex,
            dividends_paid = EXCLUDED.dividends_paid,
            dps_declared = EXCLUDED.dps_declared,
            buyback_spend = EXCLUDED.buyback_spend,
            form_type = EXCLUDED.form_type,
            filed_date = EXCLUDED.filed_date,
            is_restated = EXCLUDED.is_restated,
            is_derived = EXCLUDED.is_derived,
            ingestion_run_id = EXCLUDED.ingestion_run_id,
            fetched_at = NOW()
        """,
        {
            "instrument_id": instrument_id,
            "period_end_date": period.period_end_date,
            "period_type": period.period_type,
            "fiscal_year": period.fiscal_year,
            "fiscal_quarter": period.fiscal_quarter,
            "period_start_date": period.period_start_date,
            "months_covered": period.months_covered,
            "revenue": period.revenue,
            "cost_of_revenue": period.cost_of_revenue,
            "gross_profit": period.gross_profit,
            "operating_income": period.operating_income,
            "net_income": period.net_income,
            "eps_basic": period.eps_basic,
            "eps_diluted": period.eps_diluted,
            "research_and_dev": period.research_and_dev,
            "sga_expense": period.sga_expense,
            "depreciation_amort": period.depreciation_amort,
            "interest_expense": period.interest_expense,
            "income_tax": period.income_tax,
            "shares_basic": period.shares_basic,
            "shares_diluted": period.shares_diluted,
            "sbc_expense": period.sbc_expense,
            "total_assets": period.total_assets,
            "total_liabilities": period.total_liabilities,
            "shareholders_equity": period.shareholders_equity,
            "cash": period.cash,
            "long_term_debt": period.long_term_debt,
            "short_term_debt": period.short_term_debt,
            "shares_outstanding": period.shares_outstanding,
            "inventory": period.inventory,
            "receivables": period.receivables,
            "payables": period.payables,
            "goodwill": period.goodwill,
            "ppe_net": period.ppe_net,
            "operating_cf": period.operating_cf,
            "investing_cf": period.investing_cf,
            "financing_cf": period.financing_cf,
            "capex": period.capex,
            "dividends_paid": period.dividends_paid,
            "dps_declared": period.dps_declared,
            "buyback_spend": period.buyback_spend,
            "source": period.source,
            "source_ref": period.source_ref,
            "reported_currency": period.reported_currency,
            "form_type": period.form_type,
            "filed_date": period.filed_date,
            "is_restated": period.is_restated,
            "is_derived": period.is_derived,
            "ingestion_run_id": ingestion_run_id,
        },
    )
    return cur.rowcount > 0


# ── Source priority for canonical merge ─────────────────────────
_SOURCE_PRIORITY = {"sec_edgar": 1, "companies_house": 2, "fmp": 3}


def _canonical_merge_instrument(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> int:
    """Merge financial_periods_raw into financial_periods for one instrument.

    For each (period_end_date, period_type), picks the row from the
    highest-priority source.  Returns count of rows upserted.
    """
    cur = conn.execute(
        """
        WITH best_source AS (
            SELECT DISTINCT ON (period_end_date, period_type)
                *
            FROM financial_periods_raw
            WHERE instrument_id = %(iid)s
            ORDER BY period_end_date, period_type,
                     CASE source
                         WHEN 'sec_edgar' THEN 1
                         WHEN 'companies_house' THEN 2
                         WHEN 'fmp' THEN 3
                         ELSE 99
                     END,
                     filed_date DESC NULLS LAST
        )
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, period_start_date, months_covered,
            revenue, cost_of_revenue, gross_profit, operating_income,
            net_income, eps_basic, eps_diluted, research_and_dev,
            sga_expense, depreciation_amort, interest_expense, income_tax,
            shares_basic, shares_diluted, sbc_expense,
            total_assets, total_liabilities, shareholders_equity, cash,
            long_term_debt, short_term_debt, shares_outstanding,
            inventory, receivables, payables, goodwill, ppe_net,
            operating_cf, investing_cf, financing_cf, capex,
            dividends_paid, dps_declared, buyback_spend,
            source, source_ref, reported_currency,
            form_type, filed_date, is_restated, is_derived,
            normalization_status
        )
        SELECT
            %(iid)s, period_end_date, period_type,
            fiscal_year, fiscal_quarter, period_start_date, months_covered,
            revenue, cost_of_revenue, gross_profit, operating_income,
            net_income, eps_basic, eps_diluted, research_and_dev,
            sga_expense, depreciation_amort, interest_expense, income_tax,
            shares_basic, shares_diluted, sbc_expense,
            total_assets, total_liabilities, shareholders_equity, cash,
            long_term_debt, short_term_debt, shares_outstanding,
            inventory, receivables, payables, goodwill, ppe_net,
            operating_cf, investing_cf, financing_cf, capex,
            dividends_paid, dps_declared, buyback_spend,
            source, source_ref, reported_currency,
            form_type, filed_date, is_restated, is_derived,
            'normalized'
        FROM best_source
        ON CONFLICT (instrument_id, period_end_date, period_type)
        DO UPDATE SET
            fiscal_year = EXCLUDED.fiscal_year,
            revenue = EXCLUDED.revenue,
            cost_of_revenue = EXCLUDED.cost_of_revenue,
            gross_profit = EXCLUDED.gross_profit,
            operating_income = EXCLUDED.operating_income,
            net_income = EXCLUDED.net_income,
            eps_basic = EXCLUDED.eps_basic,
            eps_diluted = EXCLUDED.eps_diluted,
            research_and_dev = EXCLUDED.research_and_dev,
            sga_expense = EXCLUDED.sga_expense,
            depreciation_amort = EXCLUDED.depreciation_amort,
            interest_expense = EXCLUDED.interest_expense,
            income_tax = EXCLUDED.income_tax,
            shares_basic = EXCLUDED.shares_basic,
            shares_diluted = EXCLUDED.shares_diluted,
            sbc_expense = EXCLUDED.sbc_expense,
            total_assets = EXCLUDED.total_assets,
            total_liabilities = EXCLUDED.total_liabilities,
            shareholders_equity = EXCLUDED.shareholders_equity,
            cash = EXCLUDED.cash,
            long_term_debt = EXCLUDED.long_term_debt,
            short_term_debt = EXCLUDED.short_term_debt,
            shares_outstanding = EXCLUDED.shares_outstanding,
            inventory = EXCLUDED.inventory,
            receivables = EXCLUDED.receivables,
            payables = EXCLUDED.payables,
            goodwill = EXCLUDED.goodwill,
            ppe_net = EXCLUDED.ppe_net,
            operating_cf = EXCLUDED.operating_cf,
            investing_cf = EXCLUDED.investing_cf,
            financing_cf = EXCLUDED.financing_cf,
            capex = EXCLUDED.capex,
            dividends_paid = EXCLUDED.dividends_paid,
            dps_declared = EXCLUDED.dps_declared,
            buyback_spend = EXCLUDED.buyback_spend,
            source = EXCLUDED.source,
            source_ref = EXCLUDED.source_ref,
            form_type = EXCLUDED.form_type,
            filed_date = EXCLUDED.filed_date,
            is_restated = EXCLUDED.is_restated,
            is_derived = EXCLUDED.is_derived,
            normalization_status = 'normalized'
        """,
        {"iid": instrument_id},
    )
    return cur.rowcount


@dataclass(frozen=True)
class NormalizationSummary:
    instruments_processed: int
    periods_raw_upserted: int
    periods_canonical_upserted: int


def normalize_financial_periods(
    conn: psycopg.Connection[tuple],
    instrument_ids: Sequence[int] | None = None,
) -> NormalizationSummary:
    """Full normalization pipeline: facts_raw → periods_raw → canonical.

    If ``instrument_ids`` is None, processes all instruments that have
    facts in financial_facts_raw.
    """
    # Determine which instruments to process
    if instrument_ids is None:
        cur = conn.execute(
            "SELECT DISTINCT instrument_id FROM financial_facts_raw"
        )
        instrument_ids = [row[0] for row in cur.fetchall()]

    total_raw = 0
    total_canonical = 0

    for iid in instrument_ids:
        try:
            with conn.transaction():
                # Step 1: Read facts for this instrument
                cur = conn.execute(
                    """
                    SELECT concept, unit, period_start, period_end, val,
                           frame, form_type, fiscal_year, fiscal_period,
                           accession_number, filed_date
                    FROM financial_facts_raw
                    WHERE instrument_id = %(iid)s
                    ORDER BY period_end, concept
                    """,
                    {"iid": iid},
                )
                fact_rows = [
                    FactRow(
                        concept=r[0], unit=r[1], period_start=r[2],
                        period_end=r[3], val=r[4], frame=r[5],
                        form_type=r[6], fiscal_year=r[7], fiscal_period=r[8],
                        accession_number=r[9], filed_date=r[10],
                    )
                    for r in cur.fetchall()
                ]

                if not fact_rows:
                    continue

                # Determine reported currency (always USD for SEC)
                reported_currency = "USD"

                # Step 2: Derive periods from facts
                periods = _derive_periods_from_facts(fact_rows, reported_currency)

                # Step 3: Upsert into financial_periods_raw
                raw_count = 0
                for period in periods:
                    if _upsert_period_raw(conn, instrument_id=iid, period=period):
                        raw_count += 1
                total_raw += raw_count

                # Step 4: Canonical merge
                canonical_count = _canonical_merge_instrument(conn, iid)
                total_canonical += canonical_count

                logger.info(
                    "Normalized instrument %d: %d raw periods, %d canonical",
                    iid, raw_count, canonical_count,
                )
        except Exception:
            logger.exception("Failed to normalize instrument %d", iid)

    return NormalizationSummary(
        instruments_processed=len(instrument_ids),
        periods_raw_upserted=total_raw,
        periods_canonical_upserted=total_canonical,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_financial_normalization.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/financial_normalization.py tests/test_financial_normalization.py
git commit -m "feat: add financial normalization service

Implements the three-layer pipeline:
- _derive_periods_from_facts(): groups XBRL facts by fiscal period,
  applies tag priority, handles YTD disambiguation via frame field,
  derives Q4 as FY - Q1 - Q2 - Q3 when Q4 not directly filed.
- _upsert_period_raw(): stores wide period rows in financial_periods_raw.
- _canonical_merge_instrument(): picks best source per period
  (sec_edgar > companies_house > fmp) into financial_periods.
- normalize_financial_periods(): orchestrates the full pipeline."
```

---

### Task 6: Wire Into Scheduler

**Files:**
- Modify: `app/workers/scheduler.py`

- [ ] **Step 1: Add job name constant**

Add after the existing job name constants (around line 191):

```python
JOB_DAILY_FINANCIAL_FACTS = "daily_financial_facts"
```

- [ ] **Step 2: Add to SCHEDULED_JOBS list**

Find the `SCHEDULED_JOBS` list and add a new entry. The financial facts job should run after `daily_research_refresh` (which still populates the legacy `fundamentals_snapshot`), so schedule it slightly later:

```python
    ScheduledJob(
        name=JOB_DAILY_FINANCIAL_FACTS,
        description="Fetch SEC XBRL facts and normalize into financial periods",
        cadence=Cadence.daily(hour=4, minute=0),
        prerequisite=_has_any_coverage,
    ),
```

- [ ] **Step 3: Add job function**

Add the job function after the existing `daily_research_refresh` function:

```python
def daily_financial_facts() -> None:
    """Fetch expanded XBRL facts from SEC and run normalization pipeline."""
    with _tracked_job(JOB_DAILY_FINANCIAL_FACTS) as tracker:
        from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
        from app.services.financial_facts import refresh_financial_facts
        from app.services.financial_normalization import normalize_financial_periods

        with psycopg.connect(settings.database_url) as conn:
            # Build symbol → CIK mapping
            cur = conn.execute(
                """
                SELECT i.symbol, i.instrument_id, ei.identifier_value
                FROM instruments i
                JOIN external_identifiers ei
                    ON ei.instrument_id = i.instrument_id
                    AND ei.provider = 'sec'
                    AND ei.identifier_type = 'cik'
                    AND ei.is_primary = TRUE
                WHERE i.is_tradable = TRUE
                ORDER BY i.symbol
                """
            )
            symbols = [(row[0], row[1], row[2]) for row in cur.fetchall()]

            if not symbols:
                logger.info("No instruments with SEC CIK — skipping financial facts")
                return

            # Phase 1: Fetch and store raw XBRL facts
            with SecFundamentalsProvider(user_agent=settings.sec_user_agent) as provider:
                summary = refresh_financial_facts(provider, conn, symbols)
                logger.info(
                    "Financial facts: %d attempted, %d upserted, %d skipped, %d failed",
                    summary.symbols_attempted,
                    summary.facts_upserted,
                    summary.facts_skipped,
                    summary.symbols_failed,
                )

            # Phase 2: Normalize facts → periods_raw → canonical
            instrument_ids = [iid for _, iid, _ in symbols]
            norm_summary = normalize_financial_periods(conn, instrument_ids)
            logger.info(
                "Normalization: %d instruments, %d raw periods, %d canonical",
                norm_summary.instruments_processed,
                norm_summary.periods_raw_upserted,
                norm_summary.periods_canonical_upserted,
            )

            tracker.row_count = summary.facts_upserted + norm_summary.periods_canonical_upserted
```

- [ ] **Step 4: Register the job function in the dispatch map**

Find the job dispatch map (where job names are mapped to functions) and add:

```python
JOB_DAILY_FINANCIAL_FACTS: daily_financial_facts,
```

- [ ] **Step 5: Run checks**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -x
```
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py
git commit -m "feat: wire daily_financial_facts job into scheduler

Runs daily at 04:00 UTC after daily_research_refresh. Fetches expanded
XBRL facts via SecFundamentalsProvider.extract_facts(), stores in
financial_facts_raw, then runs normalization pipeline to populate
financial_periods_raw and financial_periods."
```

---

### Task 7: Smoke Test — End-to-End Pipeline Verification

**Files:**
- Test: `tests/test_financial_normalization.py` (add integration-style test)

- [ ] **Step 1: Add a comprehensive normalization test with Q4 derivation and canonical merge**

Add to `tests/test_financial_normalization.py`:

```python
class TestDeriveQ4EdgeCases:
    def test_no_q4_derivation_without_all_three_quarters(self) -> None:
        """If Q1+Q2 exist but Q3 is missing, no Q4 is derived."""
        facts = [
            _fact(fiscal_period="Q1", val=Decimal("100"), period_end="2024-03-31",
                  period_start="2024-01-01", frame="CY2024Q1", accession_number="q1"),
            _fact(fiscal_period="Q2", val=Decimal("120"), period_end="2024-06-30",
                  period_start="2024-04-01", frame="CY2024Q2", accession_number="q2"),
            _fact(fiscal_period="FY", fiscal_year=2024, val=Decimal("500"),
                  period_end="2024-12-31", period_start="2024-01-01",
                  frame="CY2024", form_type="10-K", accession_number="fy"),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4_periods = [p for p in periods if p.period_type == "Q4"]
        assert len(q4_periods) == 0

    def test_no_q4_derivation_when_q4_exists(self) -> None:
        """If Q4 is directly filed, no derivation needed."""
        facts = [
            _fact(fiscal_period="Q1", val=Decimal("100"), period_end="2024-03-31",
                  period_start="2024-01-01", frame="CY2024Q1", accession_number="q1"),
            _fact(fiscal_period="Q2", val=Decimal("120"), period_end="2024-06-30",
                  period_start="2024-04-01", frame="CY2024Q2", accession_number="q2"),
            _fact(fiscal_period="Q3", val=Decimal("110"), period_end="2024-09-30",
                  period_start="2024-07-01", frame="CY2024Q3", accession_number="q3"),
            _fact(fiscal_period="Q4", val=Decimal("170"), period_end="2024-12-31",
                  period_start="2024-10-01", frame="CY2024Q4", accession_number="q4"),
            _fact(fiscal_period="FY", fiscal_year=2024, val=Decimal("500"),
                  period_end="2024-12-31", period_start="2024-01-01",
                  frame="CY2024", form_type="10-K", accession_number="fy"),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4_periods = [p for p in periods if p.period_type == "Q4"]
        assert len(q4_periods) == 1
        assert q4_periods[0].revenue == Decimal("170")
        assert q4_periods[0].is_derived is False

    def test_derived_q4_balance_sheet_uses_fy(self) -> None:
        """Derived Q4 balance sheet = FY balance sheet (same point-in-time)."""
        facts = [
            _fact(fiscal_period="Q1", val=Decimal("100"), period_end="2024-03-31",
                  period_start="2024-01-01", frame="CY2024Q1", accession_number="q1"),
            _fact(fiscal_period="Q2", val=Decimal("120"), period_end="2024-06-30",
                  period_start="2024-04-01", frame="CY2024Q2", accession_number="q2"),
            _fact(fiscal_period="Q3", val=Decimal("110"), period_end="2024-09-30",
                  period_start="2024-07-01", frame="CY2024Q3", accession_number="q3"),
            _fact(fiscal_period="FY", fiscal_year=2024, val=Decimal("500"),
                  period_end="2024-12-31", period_start="2024-01-01",
                  frame="CY2024", form_type="10-K", accession_number="fy"),
            # Balance sheet fact on FY
            _fact(
                concept="Assets", fiscal_period="FY", fiscal_year=2024,
                val=Decimal("999"), period_end="2024-12-31",
                period_start=None, frame=None,
                form_type="10-K", accession_number="fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4 = next(p for p in periods if p.period_type == "Q4")
        assert q4.total_assets == Decimal("999")


class TestMultiYearNormalization:
    def test_multiple_fiscal_years(self) -> None:
        """Facts from FY2023 and FY2024 produce separate periods."""
        facts = [
            _fact(fiscal_period="Q1", fiscal_year=2023, val=Decimal("80"),
                  period_end="2023-03-31", period_start="2023-01-01",
                  frame="CY2023Q1", accession_number="q1-2023"),
            _fact(fiscal_period="Q1", fiscal_year=2024, val=Decimal("100"),
                  period_end="2024-03-31", period_start="2024-01-01",
                  frame="CY2024Q1", accession_number="q1-2024"),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 2
        years = {p.fiscal_year for p in periods}
        assert years == {2023, 2024}
```

- [ ] **Step 2: Run all tests**

```bash
uv run pytest tests/test_financial_normalization.py -v
uv run pytest tests/test_xbrl_fact_extraction.py -v
uv run pytest tests/test_financial_facts_service.py -v
```
Expected: All pass

- [ ] **Step 3: Run full pre-push checks**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_financial_normalization.py
git commit -m "test: add Q4 derivation edge cases and multi-year normalization tests

Covers: no derivation without 3 quarters, no derivation when Q4 exists,
derived Q4 balance sheet uses FY values, multi-fiscal-year separation."
```

---

### Task 8: Format, Lint, Final Checks

- [ ] **Step 1: Fix any remaining lint/type issues**

```bash
uv run ruff check . --fix
uv run ruff format .
uv run pyright
```

- [ ] **Step 2: Run full test suite including smoke test**

```bash
uv run pytest -x
```
Expected: All pass, including `tests/smoke/test_app_boots.py` (validates migration applies and app starts).

- [ ] **Step 3: Commit any fixes**

```bash
git add -u
git commit -m "chore: fix lint and type issues from Phase 1 implementation"
```

---

## Post-Plan Notes

### Backward Compatibility

The expanded `instrument_valuation` view has a **dual-path** design:
- Instruments with data in `financial_periods` get the new pipeline (TTM from 4 quarters, full ratio suite)
- Instruments without `financial_periods` data fall back to the legacy `fundamentals_snapshot` path
- Both paths produce the same column set, including `fcf_yield` and `debt_equity_ratio` consumed by `scoring.py`

### What This Phase Does NOT Include

- **Phases 2-6** from the spec (computed metrics, short interest, insider transactions, FRED) — each gets its own plan after Phase 1 ships
- Frontend changes to display the new data — separate ticket
- Historical backfill beyond what SEC companyfacts returns — the API includes full filing history, so backfill is automatic on first fetch

### Testing Strategy

- Unit tests cover the pure normalization functions (`_derive_periods_from_facts`, tag priority, Q4 derivation)
- Service tests mock DB connections for upsert logic
- The smoke test (`test_app_boots.py`) validates the migration applies and views are queryable
- Real data validation happens by running the `daily_financial_facts` job against the dev DB after deployment
