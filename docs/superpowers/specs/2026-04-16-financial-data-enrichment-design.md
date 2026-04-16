# Financial Data Enrichment — Design Spec

**Date:** 2026-04-16
**Issue:** TBD (will be created from this spec)
**Goal:** Comprehensive per-instrument financial data from free, uncapped, ToS-compliant sources — with full quarterly history, source provenance, and derived valuation metrics.

---

## 1. Problem Statement

eBull currently extracts ~10 financial concepts from SEC EDGAR's companyfacts response, which contains hundreds of XBRL tags. There is no quarterly history tracking — only TTM snapshots. Missing: net income, total assets, total liabilities, R&D, SG&A, D&A, EBITDA, dividends, insider transactions, short interest, and most valuation ratios. The instrument page has no meaningful financial profile.

FMP was used for enrichment but its free tier covers only ~87 symbols at 250 calls/day — effectively a demo, not a data source for a 150+ instrument universe.

## 2. Data Sources — Verified Free and Uncapped

### Tier 1: Unlimited, no restrictions

| Source | Auth | Rate Limit | Daily Cap | Coverage |
|--------|------|-----------|-----------|----------|
| **SEC EDGAR** (data.sec.gov) | User-Agent header | 10 req/s | None | All US public companies |
| **FINRA CDN** (cdn.finra.org) | None | None stated | None | All US equities (short interest) |
| **FCA** (fca.org.uk) | None | None stated | None | UK-regulated securities (short positions) |
| **Self-computed** | N/A | N/A | N/A | All instruments with price data |
| **FRED** (fred.stlouisfed.org) | Free API key | 120 req/min | None | Macro/rates/yield curves |

### Tier 2: Free but constrained

| Source | Auth | Rate Limit | Constraint | Coverage |
|--------|------|-----------|------------|----------|
| **Finnhub** (finnhub.io) | Free API key | 60/min | Personal use ToS | US fundamentals; US+UK+EU insider txns |
| **Companies House** | Free API key | 600/5min | UK companies only | Already integrated |

### Tier 3: Demo-grade fallback

| Source | Auth | Rate Limit | Constraint | Coverage |
|--------|------|-----------|------------|----------|
| **FMP** | Free API key | 250/day | ~87 symbols only | Forward estimates, price targets where covered |

### Not available (free)

- Consensus forward estimates (full universe) — no free uncapped source exists
- Analyst price targets (full universe) — same
- Real-time institutional ownership — Finnhub premium
- UK normalized fundamentals API — no UK equivalent of SEC CompanyFacts

## 3. What Each Source Provides

### SEC EDGAR — companyfacts endpoint

One call per company returns full XBRL quarterly/annual history. We currently extract 10 concepts; expanding to ~30:

**Income Statement:**
`Revenues` (4 tag variants), `CostOfGoodsAndServicesSold`, `GrossProfit`, `OperatingIncomeLoss`, `NetIncomeLoss`, `EarningsPerShareBasic`, `EarningsPerShareDiluted`, `ResearchAndDevelopmentExpense`, `SellingGeneralAndAdministrativeExpense`, `DepreciationDepletionAndAmortization`, `InterestExpense`, `IncomeTaxExpenseBenefit`, `WeightedAverageNumberOfSharesOutstandingBasic`, `WeightedAverageNumberOfDilutedSharesOutstanding`, `AllocatedShareBasedCompensationExpense`

**Balance Sheet:**
`Assets`, `Liabilities`, `StockholdersEquity`, `CashAndCashEquivalentsAtCarryingValue` (2 variants), `LongTermDebt` (3 variants), `CommercialPaper`/`ShortTermBorrowings`, `CommonStockSharesOutstanding`, `InventoryNet`, `AccountsReceivableNetCurrent`, `AccountsPayableCurrent`, `Goodwill`, `PropertyPlantAndEquipmentNet`

**Cash Flow:**
`NetCashProvidedByUsedInOperatingActivities`, `NetCashProvidedByUsedInInvestingActivities`, `NetCashProvidedByUsedInFinancingActivities`, `PaymentsToAcquirePropertyPlantAndEquipment`, `PaymentsOfDividends`, `CommonStockDividendsPerShareDeclared`, `PaymentsForRepurchaseOfCommonStock`, `ShareBasedCompensation`

**Computed (not XBRL tags):**
- EBITDA = OperatingIncome + D&A
- FCF = OperatingCF - |CapEx|
- Net debt = LT debt + ST debt - Cash
- Book value/share = Equity / Shares outstanding

**Other SEC endpoints:**
- `submissions/{CIK}.json` — fiscal year end (`fiscalYearEnd` MMDD), filing history
- Form 4 bulk TSV — quarterly insider transaction dumps (NONDERIV_TRANS.tsv, DERIV_TRANS.tsv)
- 13F data sets — quarterly institutional holdings (future phase)

**Critical XBRL parsing notes:**
- Duration items (income/CF): filter by `frame` field to get standalone quarterly values, not YTD cumulative
- Instant items (balance sheet): use most recent `end` date entry
- Q4 often not filed directly — derive as FY minus Q1+Q2+Q3
- Tag priority: try ASC 606 tags first, fall back to legacy
- Values in raw units (not thousands) — `val` field is the actual number

### Finnhub free tier

| Endpoint | What we use it for |
|----------|-------------------|
| `/stock/insider-transactions` | Insider buys/sells — US, UK, CA, AU, IN, EU coverage |
| `/stock/metric?metric=all` | Beta, 52-week high/low, dividend yield, margins (US only) |
| `/stock/recommendation` | Analyst buy/hold/sell/strongBuy/strongSell monthly history |
| `/stock/insider-sentiment` | Monthly Share Purchase Ratio (MSPR) — US only |

### FINRA CDN

- Biweekly short interest CSV: `cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv`
- Pipe-delimited (not comma). Fields: symbolCode, issueName, currentShortPositionQuantity, previousShortPositionQuantity, avgDailyVolumeQuantity, daysToCoverQuantity
- ~11 calendar day delay from settlement date
- Pre-June 2021 data covers OTC securities only

### FCA short positions

- Daily XLSX: `fca.org.uk/publication/data/short-positions-daily-update.xlsx`
- Fields: position holder, company name, ISIN, net short %, position date
- Threshold disclosure: only positions ≥0.5% of issued share capital are reported
- Absence below 0.5% is NOT zero — must be treated as unknown/nullable
- File is overwritten daily — must self-archive for history

### FRED

- Macro/rates context for valuation: Treasury yields, Fed Funds rate, CPI, credit spreads, yield curve
- Free API key, 120 req/min
- Useful for discount rate inputs and macro regime context

### Self-computed metrics

| Metric | Source data | Method |
|--------|-----------|--------|
| Beta | `price_daily` returns vs benchmark returns | Covariance / variance, 252-day window |
| 52-week high/low | `price_daily.high`, `price_daily.low` | MAX/MIN over 252 trading days |
| Avg volume (30d) | `price_daily.volume` | Rolling 30-day average |
| YTD/3yr/5yr returns | `price_daily.close` | Same pattern as existing 1w-1y |
| All valuation ratios | `quotes` + `financial_periods` | P/E, P/B, P/S, EV/EBITDA, etc. |
| Dividend yield | DPS from SEC + price | Annual DPS / current price × 100 |
| EBITDA | Financial periods | Operating income + D&A |

**Benchmark mapping for beta:**
- US stocks → SPY (S&P 500)
- UK stocks → ISF.L (iShares FTSE 100)
- EU stocks → instrument-specific or broad EU index
- Stored in `instrument_profile.benchmark_symbol`

## 4. Schema Design

### 4a. `financial_facts_raw` — fact-level XBRL storage (NEW)

Stores individual XBRL facts exactly as they come from SEC CompanyFacts. This is the lowest-level raw data — if normalization has bugs, we re-derive from here without re-fetching.

```sql
CREATE TABLE financial_facts_raw (
    fact_id              BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    taxonomy             TEXT NOT NULL DEFAULT 'us-gaap',
    concept              TEXT NOT NULL,           -- e.g. 'NetIncomeLoss'
    unit                 TEXT NOT NULL,           -- 'USD', 'USD/shares', 'shares', 'pure'
    period_start         DATE,                    -- NULL for instant (balance sheet) items
    period_end           DATE NOT NULL,
    val                  NUMERIC(30,6) NOT NULL,  -- raw value, full precision
    frame                TEXT,                    -- e.g. 'CY2024Q4', NULL for YTD/cumulative
    accession_number     TEXT NOT NULL,
    form_type            TEXT NOT NULL,           -- '10-K', '10-Q', '8-K'
    filed_date           DATE NOT NULL,
    fiscal_year          INTEGER,
    fiscal_period        TEXT,                    -- 'FY', 'Q1', 'Q2', 'Q3', 'Q4'
    decimals             INTEGER,                 -- precision indicator from XBRL
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id, concept, unit, period_end, accession_number)
);

CREATE INDEX idx_facts_raw_instrument_concept
    ON financial_facts_raw(instrument_id, concept, period_end DESC);
```

### 4b. `financial_periods_raw` — wide period rows per source (NEW)

Provider-shaped wide rows. For SEC, derived from `financial_facts_raw`. For FMP/Companies House, populated directly from API responses.

```sql
CREATE TABLE financial_periods_raw (
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
    source_ref           TEXT NOT NULL,      -- accession_number (SEC) or hash (FMP/Finnhub)
    reported_currency    TEXT NOT NULL,
    fx_rate_to_usd       NUMERIC(20,10),
    fx_rate_date         DATE,
    form_type            TEXT,
    filed_date           DATE,
    is_restated          BOOLEAN NOT NULL DEFAULT FALSE,
    is_derived           BOOLEAN NOT NULL DEFAULT FALSE,  -- Q4 = FY - Q1 - Q2 - Q3
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(instrument_id, period_end_date, period_type, source, source_ref)
);
```

### 4c. `financial_periods` — canonical one-row-per-period (NEW)

The single source of truth for valuation, TTM, and trend queries. Populated by a normalization step that picks the best source per period.

```sql
CREATE TABLE financial_periods (
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    period_end_date      DATE NOT NULL,
    period_type          TEXT NOT NULL CHECK (period_type IN (
        'Q1','Q2','Q3','Q4','FY','H1','H2','9M','STUB'
    )),
    fiscal_year          INTEGER NOT NULL,
    fiscal_quarter       INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    period_start_date    DATE,
    months_covered       SMALLINT,

    -- All financial columns same as financial_periods_raw
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

    operating_cf         NUMERIC(20,4),
    investing_cf         NUMERIC(20,4),
    financing_cf         NUMERIC(20,4),
    capex                NUMERIC(20,4),
    dividends_paid       NUMERIC(20,4),
    dps_declared         NUMERIC(12,4),
    buyback_spend        NUMERIC(20,4),

    -- Provenance (which source won)
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
    superseded_at        TIMESTAMPTZ,     -- set when a restatement replaces this row

    PRIMARY KEY (instrument_id, period_end_date, period_type)
);

CREATE INDEX idx_financial_periods_instrument_date
    ON financial_periods(instrument_id, period_end_date DESC)
    WHERE superseded_at IS NULL;
```

**Source priority for canonical merge:**
1. `sec_edgar` (authoritative, as-filed)
2. `companies_house` (UK equivalent)
3. `fmp` (vendor-normalized, fallback)

### 4d. `financial_periods_ttm` — trailing twelve months (VIEW)

```sql
CREATE VIEW financial_periods_ttm AS
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
    -- Completeness flags
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
    MAX(reported_currency) FILTER (WHERE rn = 1) AS reported_currency

FROM latest_4
GROUP BY instrument_id;
```

### 4e. `instrument_profile` — expand existing table

```sql
ALTER TABLE instrument_profile
    ADD COLUMN IF NOT EXISTS dividend_yield    NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS last_dividend     NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS ex_dividend_date  DATE,
    ADD COLUMN IF NOT EXISTS fiscal_year_end   TEXT,          -- MMDD format
    ADD COLUMN IF NOT EXISTS week_52_high      NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS week_52_low       NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS week_52_high_date DATE,
    ADD COLUMN IF NOT EXISTS week_52_low_date  DATE,
    ADD COLUMN IF NOT EXISTS benchmark_symbol  TEXT;          -- SPY, ISF.L, etc.
```

### 4f. `dividends` — dividend history (NEW)

```sql
CREATE TABLE dividends (
    dividend_id          BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    ex_dividend_date     DATE,
    record_date          DATE,
    payment_date         DATE,
    declaration_date     DATE,
    amount               NUMERIC(20,6) NOT NULL,
    currency             TEXT NOT NULL,
    frequency            TEXT,         -- 'quarterly', 'semiannual', 'annual', 'special'
    source               TEXT NOT NULL,
    source_ref           TEXT,
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id, ex_dividend_date, amount, source)
);
```

Source: SEC XBRL `CommonStockDividendsPerShareDeclared` per period for amount/declaration. FMP `/profile` for ex-dividend dates where covered. Finnhub `/stock/metric` for current dividend yield (US only).

### 4g. `corporate_actions` — splits and ticker changes (NEW)

```sql
CREATE TABLE corporate_actions (
    action_id            BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    action_date          DATE NOT NULL,
    effective_date       DATE,
    action_type          TEXT NOT NULL CHECK (action_type IN (
        'split', 'reverse_split', 'spinoff', 'ticker_change', 'merger'
    )),
    ratio                NUMERIC(20,10),    -- 2.0 for 2:1 split
    old_symbol           TEXT,              -- for ticker_change
    new_symbol           TEXT,
    cash_amount          NUMERIC(20,6),     -- for spinoff/merger cash component
    currency             TEXT,
    source               TEXT NOT NULL,
    source_ref           TEXT,
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id, action_date, action_type)
);
```

### 4h. `insider_transactions` (NEW)

```sql
CREATE TABLE insider_transactions (
    insider_transaction_id BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    transaction_date     DATE NOT NULL,
    filed_date           DATE,
    insider_name         TEXT NOT NULL,
    insider_title        TEXT,
    security_title       TEXT,           -- 'Common Stock', 'RSU', 'Option'
    transaction_type     TEXT NOT NULL,   -- P, S, M, F, G, A, D, J, C, W
    acquired_disposed    TEXT,            -- 'A' or 'D'
    shares               NUMERIC(20,4) NOT NULL,
    price_per_share      NUMERIC(18,6),
    shares_after         NUMERIC(20,0),
    ownership_type       TEXT,            -- 'D' direct, 'I' indirect
    is_derivative        BOOLEAN NOT NULL DEFAULT FALSE,
    -- Source identity
    source               TEXT NOT NULL,   -- 'sec_edgar' or 'finnhub'
    source_ref           TEXT NOT NULL,   -- accn+line (SEC) or hash (Finnhub)
    accession_number     TEXT,
    line_number          INTEGER,
    reporting_owner_cik  TEXT,
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source, source_ref)
);

CREATE INDEX idx_insider_txns_instrument_date
    ON insider_transactions(instrument_id, transaction_date DESC);
```

### 4i. `short_interest` — FINRA US aggregate (NEW)

```sql
CREATE TABLE short_interest (
    short_interest_id    BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    settlement_date      DATE NOT NULL,
    short_shares         NUMERIC(20,0),
    previous_short       NUMERIC(20,0),
    avg_daily_volume     NUMERIC(20,0),
    days_to_cover        NUMERIC(10,2),
    source               TEXT NOT NULL DEFAULT 'finra',
    source_ref           TEXT,            -- FINRA file name
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id, settlement_date)
);
```

Derived in queries (not stored):
- `short_pct_of_float = short_shares / instrument_profile.public_float`
- `change_shares = short_shares - previous_short`
- `change_pct = change_shares / NULLIF(previous_short, 0) * 100`

### 4j. `fca_short_positions` — UK per-holder (NEW)

```sql
CREATE TABLE fca_short_positions (
    fca_position_id      BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    position_holder      TEXT NOT NULL,
    isin                 TEXT,
    net_short_pct        NUMERIC(10,4) NOT NULL,
    position_date        DATE NOT NULL,
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id, position_holder, position_date)
);
```

Note: absence below 0.5% threshold is NOT zero. UI must display "no disclosed positions" not "0%".

### 4k. `data_ingestion_runs` — audit trail (NEW)

```sql
CREATE TABLE data_ingestion_runs (
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
```

### 4l. `instrument_valuation` — expanded VIEW

```sql
-- Drop and recreate the existing view with new columns
CREATE OR REPLACE VIEW instrument_valuation AS
SELECT
    q.instrument_id,
    q.last AS current_price,
    q.quoted_at AS price_as_of,

    -- From TTM
    ttm.revenue_ttm,
    ttm.net_income_ttm,
    ttm.operating_income_ttm,
    ttm.depreciation_amort_ttm,
    ttm.operating_cf_ttm,
    ttm.capex_ttm,
    ttm.dps_declared_ttm,
    ttm.is_complete_ttm,

    -- Balance sheet (latest)
    ttm.total_assets,
    ttm.total_liabilities,
    ttm.shareholders_equity,
    ttm.cash,
    ttm.long_term_debt,
    ttm.short_term_debt,
    ttm.shares_outstanding,

    -- Market cap
    CASE WHEN q.last > 0 AND ttm.shares_outstanding > 0
         THEN q.last * ttm.shares_outstanding
    END AS market_cap_live,

    -- Enterprise value
    CASE WHEN q.last > 0 AND ttm.shares_outstanding > 0
         THEN q.last * ttm.shares_outstanding
              + COALESCE(ttm.long_term_debt, 0)
              + COALESCE(ttm.short_term_debt, 0)
              - COALESCE(ttm.cash, 0)
    END AS enterprise_value,

    -- Valuation ratios (all with zero-division guards)
    CASE WHEN ttm.net_income_ttm > 0 AND ttm.shares_outstanding > 0
         THEN q.last / (ttm.net_income_ttm / ttm.shares_outstanding)
    END AS pe_ratio,

    CASE WHEN ttm.shareholders_equity > 0 AND ttm.shares_outstanding > 0
         THEN q.last / (ttm.shareholders_equity / ttm.shares_outstanding)
    END AS pb_ratio,

    CASE WHEN ttm.revenue_ttm > 0 AND ttm.shares_outstanding > 0
         THEN (q.last * ttm.shares_outstanding) / ttm.revenue_ttm
    END AS price_sales,

    CASE WHEN (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0))) > 0
              AND ttm.shares_outstanding > 0
         THEN (q.last * ttm.shares_outstanding)
              / (ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)))
    END AS p_fcf_ratio,

    -- EV ratios
    CASE WHEN ttm.revenue_ttm > 0
         THEN (q.last * ttm.shares_outstanding
               + COALESCE(ttm.long_term_debt, 0)
               + COALESCE(ttm.short_term_debt, 0)
               - COALESCE(ttm.cash, 0))
              / ttm.revenue_ttm
    END AS ev_revenue,

    CASE WHEN (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0)) > 0
         THEN (q.last * ttm.shares_outstanding
               + COALESCE(ttm.long_term_debt, 0)
               + COALESCE(ttm.short_term_debt, 0)
               - COALESCE(ttm.cash, 0))
              / (ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0))
    END AS ev_ebitda,

    -- Profitability
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

    -- Forward P/E (from analyst_estimates if available)
    CASE WHEN ae.consensus_eps_fy > 0
         THEN q.last / ae.consensus_eps_fy
    END AS forward_pe,

    -- Dividend yield
    CASE WHEN q.last > 0 AND ttm.dps_declared_ttm > 0
         THEN ttm.dps_declared_ttm / q.last * 100
    END AS dividend_yield,

    -- EBITDA
    ttm.operating_income_ttm + COALESCE(ttm.depreciation_amort_ttm, 0) AS ebitda_ttm,

    -- FCF
    ttm.operating_cf_ttm - ABS(COALESCE(ttm.capex_ttm, 0)) AS fcf_ttm

FROM quotes q
LEFT JOIN financial_periods_ttm ttm USING (instrument_id)
LEFT JOIN LATERAL (
    SELECT consensus_eps_fy
    FROM analyst_estimates
    WHERE instrument_id = q.instrument_id
    ORDER BY as_of_date DESC
    LIMIT 1
) ae ON TRUE;
```

## 5. Provider Architecture

### 5a. Existing providers (modify)

**SecEdgarFinancialsProvider** (expand)
- Widen XBRL extraction from 10 to ~30 concepts
- Store individual facts in `financial_facts_raw`
- Derive quarterly period rows into `financial_periods_raw`
- Handle YTD disambiguation using `frame` field
- Derive Q4 as FY - Q1 - Q2 - Q3 where Q4 not directly filed
- Track `accession_number`, `form_type`, `period_start_date`
- Preserve raw JSON in `data/raw/sec_fundamentals/` (existing pattern)

**FmpEnrichmentProvider** (demote to fallback)
- Use only for symbols confirmed in FMP's free tier
- Keep for: forward estimates, price targets, earnings calendar (deeper history)
- UK fundamentals if symbol covered
- Drop: US fundamentals calls (SEC handles these now)

### 5b. New providers

**SecEdgarInsiderProvider**
- Download quarterly bulk TSV from `sec.gov/files/structureddata/data/insider-transactions-data-sets/`
- Parse `NONDERIV_TRANS.tsv` and `DERIV_TRANS.tsv`
- `source_ref` = `{accession_number}:{line_number}`
- Daily incremental: check `submissions/{CIK}.json` for new Form 4 filings

**FinnhubInsiderProvider**
- `/stock/insider-transactions?symbol=X` for Tier 1 instruments
- Covers US + UK + CA + AU + IN + EU
- `source_ref` = deterministic hash of `(transaction_date, insider_name, shares, price, transaction_type)`
- Dedup against SEC data: skip if matching `(instrument_id, transaction_date, insider_name, shares)` already exists from SEC

**FinnhubMetricsProvider**
- `/stock/metric?symbol=X&metric=all` — beta, 52-week, dividend yield, margins (US only)
- `/stock/recommendation?symbol=X` — analyst buy/hold/sell trends
- `/stock/insider-sentiment?symbol=X` — MSPR score
- Updates `instrument_profile` and `analyst_estimates` tables

**FinraShortInterestProvider**
- Download biweekly CSV from `cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv`
- Parse pipe-delimited format
- Match `symbolCode` to `instruments.symbol`
- `source_ref` = FINRA file name

**FcaShortPositionsProvider**
- Download daily XLSX from `fca.org.uk/publication/data/short-positions-daily-update.xlsx`
- Parse with openpyxl
- Match ISIN to `external_identifiers`
- Archive each daily file to `data/raw/fca/short-positions-{YYYY-MM-DD}.xlsx`

**ComputedMetricsService**
- Beta: covariance of instrument returns vs benchmark returns over 252 trading days
- 52-week high/low: `MAX(high)`, `MIN(low)` from `price_daily` over 252 days
- Avg volume: rolling 30-day and 90-day average from `price_daily`
- Updates `instrument_profile` cached fields

**FredMacroProvider** (future phase)
- Treasury yields, Fed Funds rate, CPI, credit spreads
- Stored in a `macro_indicators` table (not designed in this spec — separate issue)

### 5c. Normalization pipeline

```
SEC companyfacts JSON
    ↓
financial_facts_raw (fact-level, one row per XBRL fact)
    ↓
[Normalization service]
    - Filter by frame to get standalone quarterly values
    - Derive Q4 = FY - Q1 - Q2 - Q3
    - Set months_covered, period_start_date
    - Detect and flag restatements (multiple accessions for same period)
    - Apply tag priority (ASC 606 > legacy)
    ↓
financial_periods_raw (wide rows, source = 'sec_edgar')
    ↓
[Canonical merge]
    - Pick best source per (instrument, period_end, period_type)
    - Priority: sec_edgar > companies_house > fmp
    - Set normalization_status = 'normalized'
    ↓
financial_periods (canonical, one row per period)
    ↓
financial_periods_ttm (VIEW — auto-updates)
    ↓
instrument_valuation (VIEW — auto-updates)
```

## 6. Refresh Cadence

| Source | Job | Cadence | Trigger |
|--------|-----|---------|---------|
| SEC EDGAR financials | `refresh_sec_financials` | Daily check | New filings appear quarterly |
| SEC Form 4 bulk | `refresh_sec_insiders` | Quarterly bulk + daily incremental | After quarterly ZIP published |
| Finnhub insider txns | `refresh_finnhub_insiders` | Daily for Tier 1 | After market close |
| Finnhub metrics | `refresh_finnhub_metrics` | Weekly | Weekend batch |
| FINRA short interest | `refresh_finra_shorts` | Biweekly | Match FINRA publication schedule |
| FCA short positions | `refresh_fca_shorts` | Daily | After FCA publishes (~8am UK) |
| Computed metrics | `refresh_computed_metrics` | Daily | After price_daily refresh |
| FMP enrichment | `refresh_fmp_enrichment` | Weekly for covered symbols | Weekend batch |
| Normalization | `normalize_financial_periods` | After SEC/FMP refresh | Triggered by upstream |

## 7. Source Provenance — UI Display Pattern

Every metric displayed on an instrument page should show provenance:

```
Revenue (Q3 FY2025)              $3.63B
  SEC EDGAR · 10-Q filed 2025-11-14 · fetched 2026-04-16

Enterprise Value                 $6.46B
  Computed · market cap + debt - cash · as of 2026-04-16

Short Interest                   2.1M shares (1.8% of float)
  FINRA · settled 2026-03-31 · fetched 2026-04-11

Insider: Deirdre O'Brien sold 20,338 shares @ $255.12
  SEC Form 4 · filed 2026-04-03 · accn 0000320193-26-000042
```

This requires the `source`, `filed_date`, `fetched_at`, and `source_ref` columns present on every data table.

## 8. Migration Strategy

### From `fundamentals_snapshot`

- Do NOT backfill `financial_periods` from `fundamentals_snapshot` — it stores TTM values that cannot be decomposed into quarters
- Populate `financial_periods` fresh from SEC EDGAR and FMP
- Create a compatibility view that mimics `fundamentals_snapshot` from the new tables
- Deprecate reads from old table gradually
- Keep old table for archived data integrity

### Migration order

1. Create `data_ingestion_runs` first (other tables reference it)
2. Create `financial_facts_raw` and `financial_periods_raw`
3. Create `financial_periods` and TTM view
4. Expand `instrument_profile`
5. Create `dividends` and `corporate_actions`
6. Create `insider_transactions`
7. Create `short_interest` and `fca_short_positions`
8. Expand `instrument_valuation` view
9. Create compatibility view for `fundamentals_snapshot`

## 9. Build Order (implementation phases)

### Phase 1: SEC EDGAR financials expansion + canonical pipeline
- Expand SecEdgarFinancialsProvider to ~30 XBRL concepts
- Create `financial_facts_raw`, `financial_periods_raw`, `financial_periods` tables
- Build normalization service (YTD disambiguation, Q4 derivation, canonical merge)
- Create `financial_periods_ttm` view
- Expand `instrument_valuation` view with all new ratios
- Create `data_ingestion_runs` audit table

### Phase 2: Computed metrics + profile expansion
- Build ComputedMetricsService (beta, 52-week, avg volume)
- Expand `instrument_profile` with new columns
- Add benchmark mapping per instrument
- Add `dividends` and `corporate_actions` tables
- Extract dividends from SEC XBRL

### Phase 3: Short interest (FINRA + FCA)
- Build FinraShortInterestProvider
- Build FcaShortPositionsProvider
- Create `short_interest` and `fca_short_positions` tables
- Daily FCA archival job

### Phase 4: Insider transactions
- Build SecEdgarInsiderProvider (bulk TSV parsing)
- Build FinnhubInsiderProvider
- Create `insider_transactions` table
- Cross-source deduplication logic

### Phase 5: Forward estimates + Finnhub supplementary
- FinnhubMetricsProvider for recommendations, MSPR
- FMP fallback for forward estimates (covered symbols only)
- Wire forward_pe into valuation view

### Phase 6: FRED macro context (future)
- FredMacroProvider
- `macro_indicators` table
- Yield curve, rates, CPI for valuation context

## 10. Explicit Non-Goals

- No scraping of Yahoo Finance, TipRanks, MarketBeat, or any ToS-violating source
- No real-time institutional ownership (Finnhub premium, no free alternative)
- No full-universe consensus forward estimates (no free source exists)
- No UK normalized fundamentals API equivalent to SEC CompanyFacts (doesn't exist)
- No intraday financial data
- No options chain data
- No ESG scores (Finnhub premium)

## 11. Risk Register

| Risk | Mitigation |
|------|-----------|
| SEC XBRL tag inconsistency across companies | Tag priority lists with fallbacks; log missing tags |
| YTD vs quarterly disambiguation errors | Strict `frame` field filtering; `is_derived` flag on Q4 |
| FX misalignment in valuation ratios | `reported_currency` + `fx_rate_to_usd` on every financial period |
| FINRA symbol mismatch | Match via `instruments.symbol`; log unmatched |
| FCA ISIN mismatch | Match via `external_identifiers`; log unmatched |
| Finnhub ToS change | Supplementary only; core pipeline works without Finnhub |
| FMP free tier further restricted | Already demo-grade; design works without FMP |
| Restatements corrupt TTM | `superseded_at` + `is_restated` flags; TTM view filters |
| Stock splits corrupt per-share metrics | `corporate_actions` table; adjust historical EPS/DPS |
| Fact-level raw table grows large | Partition by instrument_id or year if needed |
