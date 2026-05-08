# eBull metrics analyst — what we measure, where it comes from, where it renders

> Read this when answering "what is X?", "where does Y come from?", "where does Z render?", or when choosing what to expose on a new operator panel. Every metric below is anchored `path:line` so an agent can verify before quoting. All paths are repo-relative.
>
> **Naming**: numbers labelled "TTM" come from `financial_periods_ttm` and require `is_complete_ttm = TRUE`. Numbers labelled "as-filed" come from a single `financial_periods` row.
>
> **Scope discipline**: catalog is descriptive. Metrics marked `(planned: #N)` are filed but not rendered. Metrics marked `(deferred)` are not yet specced. Don't invent values.
>
> Per-metric template:
> ```
> Definition / Formula / Source data / Storage / Service / Endpoint / Chart / Cadence / Caveats / Validation
> ```

## Master index

| Metric | Category | Storage | Endpoint |
|---|---|---|---|
| 13F holdings change (last quarter) | Filings + events | `ownership_institutions_observations` | `/instruments/{symbol}/institutional-holdings` |
| 52-week range | Market data | (deferred) | `/instruments/{symbol}/summary` returns NULL |
| 8-K event categorisation | Filings + events | `eight_k_structured_events.items[].severity` | `/instruments/{symbol}/eight_k_filings` |
| ATR-14 | Market data | `price_daily.atr_14` | (TA scoring) |
| AUM | Risk + portfolio | positions × quotes × cash_ledger | `/portfolio` |
| Available for deployment | Risk + portfolio | `budget_config` + computed | `/budget` |
| Backfill / SEC manifest pending count | Pipeline | `sec_filing_manifest`, `data_freshness_index` | `/system/jobs`, `/system/bootstrap/status` |
| Beta vs SPY | Market data | (deferred) | — |
| Blockholder ownership % | Ownership | `ownership_blockholders_current` | `/instruments/{symbol}/ownership-rollup` |
| Bollinger bands (20, 2σ) | Market data | `price_daily.bb_upper / bb_lower` | (TA scoring) |
| Bootstrap state + stage status | Pipeline | `bootstrap_state`, `bootstrap_stages`, `bootstrap_archive_results` | `/system/bootstrap/status` |
| Buyback authorisation | Capital returns | (deferred) | — |
| Capital event (deposit / withdraw) | Risk + portfolio | `capital_events` | `/budget/events` |
| Cash + equivalents | Fundamentals | `financial_periods.cash` | `/instruments/{symbol}/financials?statement=balance` |
| Cash balance (operator) | Risk + portfolio | `cash_ledger` SUM | `/portfolio` |
| Cash buffer reserve | Risk + portfolio | `budget_config.cash_buffer_pct` | `/budget`, `/budget/config` |
| Coverage tier | Pipeline | `coverage.coverage_tier` (1/2/3) | `/instruments`, `/instruments/{symbol}/summary` |
| Coverage % per source | Pipeline | `coverage_audit`, ownership rollup `coverage` block | `/coverage/summary`, `/coverage/insufficient` |
| Credential health (eToro) | Pipeline | `broker_credentials_health_state` | `/system/status.credential_health` |
| CGT / estimated tax | Risk + portfolio | `tax_disposals` + `budget_config.cgt_scenario` | `/budget` |
| Daily candle (OHLCV) | Market data | `price_daily` | `/instruments/{symbol}/candles?range=...` |
| DEF 14A beneficial holdings | Ownership | `def14a_beneficial_holdings` → rollup `def14a_unmatched` | `/instruments/{symbol}/ownership-rollup` |
| DEF 14A vote summary | Filings + events | (planned) | — |
| Dividend per share (latest) | Capital returns | `instrument_dividend_summary.latest_dps` | `/instruments/{symbol}/dividends` |
| Dividend streak (Q) | Capital returns | `instrument_dividend_summary.dividend_streak_q` | `/instruments/{symbol}/dividends` |
| Dividend yield (TTM) | Capital returns | `instrument_dividend_summary.ttm_yield_pct` | `/instruments/{symbol}/summary`, `/instruments/{symbol}/dividends` |
| EBITDA TTM | Fundamentals | `instrument_valuation.ebitda_ttm` | (scoring) |
| EPS basic / diluted | Fundamentals | `financial_periods.eps_basic / eps_diluted` | `/instruments/{symbol}/financials?statement=income` |
| ETL freshness per source | Pipeline | `data_freshness_index` | (admin via DB) |
| Exit recommendation | Portfolio | `trade_recommendations.action='EXIT'` | `/recommendations` |
| FCF (period) | Fundamentals | derived `operating_cf - capex` | `/instruments/{symbol}/financials?statement=cashflow` (FE-derived) |
| FCF TTM | Fundamentals | `instrument_valuation.fcf_ttm` | (scoring) |
| FCF yield | Fundamentals | `instrument_valuation.fcf_yield` | (scoring; planned operator surface #671) |
| Float concentration info chip | Ownership | rollup `concentration.pct_outstanding_known` | `/instruments/{symbol}/ownership-rollup` |
| Fund ownership % (memo overlay) | Ownership | `ownership_funds_current`, `denominator_basis="institution_subset"` | `/instruments/{symbol}/ownership-rollup` |
| Gross margin | Fundamentals | derived FE; `instrument_valuation.gross_margin` | `/instruments/{symbol}/financials?statement=income` |
| Insider 90d net / counts | Filings + events | `insider_transactions` rollup | `/instruments/{symbol}/insider_summary` |
| Insider baseline (Form 3) | Ownership | `insider_initial_holdings` | `/instruments/{symbol}/insider_baseline` |
| Insider ownership % | Ownership | `ownership_insiders_current` | `/instruments/{symbol}/ownership-rollup` |
| Institutional ownership % | Ownership | `ownership_institutions_current` | `/instruments/{symbol}/ownership-rollup` |
| Job runs success rate | Pipeline | `job_runs` | `/system/status`, `/system/jobs` |
| Kill switch state | Pipeline | `runtime_config` | `/system/status.kill_switch` |
| Last close | Market data | `quotes.last`, fallback `price_daily.close` | `/portfolio`, `/instruments/{symbol}/summary` |
| Last 10-K / 10-Q / 8-K date | Filings + events | `filing_events.filing_date` | `/filings`, `/instruments/{symbol}/ten_k_history`, `/instruments/{symbol}/eight_k_filings` |
| Live volume V2 | Market data | (planned: #608) | — |
| MACD line / signal / histogram | Market data | `price_daily.macd_*` | (TA scoring) |
| Market cap (live) | Fundamentals / Market | `instrument_share_count_latest` × quote midpoint | `/instruments/{symbol}/summary.identity.market_cap` |
| Net buyback rate | Capital returns | `instrument_dilution_summary` derived | `/instruments/{symbol}/dilution` |
| Net debt | Fundamentals | `fundamentals_snapshot.net_debt` (legacy) | `/instruments/{symbol}/summary` |
| Net dilution % YoY | Capital returns | `instrument_dilution_summary.net_dilution_pct_yoy` | `/instruments/{symbol}/dilution` |
| Net income | Fundamentals | `financial_periods.net_income` | `/instruments/{symbol}/financials?statement=income` |
| News sentiment score | News | `news_events.sentiment_score` | `/news/{instrument_id}` |
| News volume (last N days) | News | COUNT(*) `news_events` | `/news/{instrument_id}` |
| Operating income | Fundamentals | `financial_periods.operating_income` | `/instruments/{symbol}/financials?statement=income` |
| Operating margin | Fundamentals | derived | `instrument_valuation.operating_margin` |
| Ownership freshness chips | Ownership | `data_freshness_index` per source | `/instruments/{symbol}/ownership-rollup` |
| P/E (TTM) | Fundamentals | `key_stats.pe_ratio`, `instrument_valuation.pe_ratio` | `/instruments/{symbol}/summary.key_stats` |
| P/B | Fundamentals | `key_stats.pb_ratio` | `/instruments/{symbol}/summary.key_stats` |
| Payout ratio (Div/FCF) | Capital returns | derived FE | `/instrument/{symbol}/dividends` page |
| Position cost basis | Risk + portfolio | `positions.cost_basis` | `/portfolio` |
| Position market value | Risk + portfolio | derived | `/portfolio` |
| Public float (residual) | Ownership | rollup `residual.shares` | `/instruments/{symbol}/ownership-rollup` |
| Recommendation history | Portfolio | `trade_recommendations` | `/recommendations`, `/recommendations/{id}` |
| Residual % | Ownership | rollup `residual.pct_outstanding` | `/instruments/{symbol}/ownership-rollup` |
| Return windows (1d/1w/1m) | Risk + portfolio | derived from `price_daily.close` | `/portfolio/rolling-pnl` |
| Revenue (period / TTM / FY) | Fundamentals | `financial_periods.revenue` / `financial_periods_ttm.revenue_ttm` | `/instruments/{symbol}/financials?statement=income` |
| ROA | Fundamentals | `instrument_valuation.roa`, `key_stats.roa` | `/instruments/{symbol}/summary.key_stats` |
| ROE | Fundamentals | `instrument_valuation.roe`, `key_stats.roe` | `/instruments/{symbol}/summary.key_stats` |
| ROIC | Fundamentals | derived FE `buildRoic()` | `/instrument/{symbol}/fundamentals` page only |
| Rolling P&L (1d/1w/1m) | Risk + portfolio | computed `price_daily` deltas vs anchor | `/portfolio/rolling-pnl` |
| RSI-14 | Market data | `price_daily.rsi_14` | (TA scoring) |
| Sentiment score (per ranking) | Scoring | `scores.sentiment_score` | `/rankings` |
| SMA-20/50/200 | Market data | `price_daily.sma_20/50/200` | (TA scoring) |
| Total assets / liabilities / equity | Fundamentals | `financial_periods.{total_assets, total_liabilities, shareholders_equity}` | `/instruments/{symbol}/financials?statement=balance` |
| Total debt | Fundamentals | derived `LTD + STD` | `/instruments/{symbol}/financials?statement=balance` |
| Treasury shares | Ownership / Fundamentals | `financial_periods.treasury_shares` → rollup top wedge | `/instruments/{symbol}/ownership-rollup`, balance |
| TTM dividends paid | Capital returns | `instrument_dividend_summary.ttm_dividends_paid` | `/instruments/{symbol}/dividends` |
| TTM DPS | Capital returns | `instrument_dividend_summary.ttm_dps` | `/instruments/{symbol}/dividends` |
| Universe coverage banner | Ownership | rollup `coverage.state` | `/instruments/{symbol}/ownership-rollup` |
| Unrealised P&L (per position) | Risk + portfolio | derived from `quotes.last` | `/portfolio` |
| Volume (daily) | Market data | `price_daily.volume` | `/instruments/{symbol}/candles` |
| VWAP | Market data | (deferred) | — |
| Yield-on-cost | Capital returns | derived FE | dividends drilldown |

## 1. Ownership metrics (Phase 1–3 of #788)

The ownership card is the cleanest example of "one fetch, one snapshot, one denominator". Every slice comes from `app/services/ownership_rollup.py:get_ownership_rollup` and renders as one wedge in `frontend/src/components/instrument/OwnershipPanel.tsx`. The only denominator is XBRL-DEI `shares_outstanding`; treasury renders as additive top wedge and is **not** in the denominator.

### Insider ownership %
- **Definition**: percentage of `shares_outstanding` held by SEC Form 3/4 filers (officers, directors, 10% owners) after cross-channel dedup.
- **Formula**: `Σ slice.holders.shares / shares_outstanding × 100`. Each holder's `shares` is highest-priority surviving row across `(form4 > form3)` per `(filer_cik, ownership_nature)`.
- **Source data**: SEC Form 3 + Form 4 → [app/services/insider_transactions.py](../../../app/services/insider_transactions.py), [app/services/insider_form3_ingest.py](../../../app/services/insider_form3_ingest.py).
- **Storage**: `ownership_insiders_observations` (write-through, partitioned quarterly) → `ownership_insiders_current`.
- **Service**: [app/services/ownership_rollup.py:347](../../../app/services/ownership_rollup.py#L347) (insiders block) + dedup priority at `:326` (`_PRIORITY_RANK`).
- **Endpoint**: `GET /instruments/{symbol}/ownership-rollup` slice `category="insiders"`.
- **Chart**: `OwnershipPanel.tsx:38` + `OwnershipSunburst.tsx:133` (ring 2 wedge, blue).
- **Cadence**: write-through on every Form 3/4 ingest; backfill `POST /jobs/ownership_observations_backfill/run` (Sun 03:00 UTC).
- **Caveats**: dedup identity key `(filer_cik | NAME-fallback, ownership_nature)`. Direct + indirect surface as separate rows on per-officer ring 3 — JPM moved 1.29% → 6.16% when this landed. Form 3 baseline-only filers (no Form 4 history) surface via `/insider_baseline` and merge into per-officer ring on FE.
- **Validation**: cross-source against gurufocus / openinsider; smoke `AAPL`, `GME`, `MSFT`, `JPM`, `HD` per CLAUDE.md panel.
- **denominator_basis**: `pie_wedge`.

### Institutional ownership %
- **Definition**: percentage held by 13F-HR filers (≥ $100M AUM) after cross-channel dedup.
- **Formula**: `Σ surviving 13F holdings / shares_outstanding × 100`, equity-only (PUT/CALL exposures dropped).
- **Source**: SEC 13F-HR XML + quarterly 13F Securities List. Parse: [app/services/sec_13f_dataset_ingest.py](../../../app/services/sec_13f_dataset_ingest.py), filer directory: [app/services/sec_13f_filer_directory.py](../../../app/services/sec_13f_filer_directory.py), CUSIP resolution: [app/services/cusip_resolver.py](../../../app/services/cusip_resolver.py). EdgarTools 13F drop-in via #925.
- **Storage**: `ownership_institutions_observations` (partitioned, `is_put_call IS NULL` filter at read time) → `ownership_institutions_current` with `exposure_kind = 'EQUITY'` filter.
- **Service**: [app/services/ownership_rollup.py:434-460](../../../app/services/ownership_rollup.py#L434-L460).
- **Endpoint**: `GET /instruments/{symbol}/ownership-rollup` slices `category="institutions"` and `category="etfs"`; flat list at `GET /instruments/{symbol}/institutional-holdings` ([app/api/instruments.py:3262](../../../app/api/instruments.py#L3262)).
- **Chart**: `OwnershipPanel.tsx` ring 2 (institutions+etfs).
- **Cadence**: `sec_13f_quarterly_sweep` Sat 02:00 UTC, 6h deadline. Filer directory walks last 4 closed quarters.
- **Caveats**: AAPL was historically under-counted ~10× until #840-A through #840-F landed full decomposition. Universe expansion via #841 still pending — institutional totals can lag reality. CUSIP coverage gates the join: 7.4% on dev as of #914 vs 80% target.
- **Validation**: cross-check vs WhaleWisdom / SEC EDGAR direct; track `unresolved_13f_cusips` count.
- **denominator_basis**: `pie_wedge`. ETF filer-type holdings come from same 13F data but render as separate slice for legibility.

### Fund ownership % (memo overlay, NOT in residual)
- **Definition**: per-fund-series equity positions from N-PORT-P. Renders as memo wedge alongside institutional pie, NOT inside it (parent advisor's 13F-HR aggregate already counts the same shares).
- **Formula**: `Σ fund_series_holdings.shares / shares_outstanding × 100`, with `denominator_basis="institution_subset"`.
- **Source**: SEC Form N-PORT-P parsed via [app/services/n_port_ingest.py](../../../app/services/n_port_ingest.py) (stdlib ElementTree; EdgarTools rewrite punted #932).
- **Storage**: `ownership_funds_observations` (partitioned) → `ownership_funds_current` keyed `(instrument_id, fund_series_id)`.
- **Service**: [app/services/ownership_rollup.py:465](../../../app/services/ownership_rollup.py#L465) (`_collect_funds_from_current`). Slice constructed at `:850-870` with `denominator_basis="institution_subset"`.
- **Endpoint**: same `/ownership-rollup`, slice `category="funds"`.
- **Chart**: `OwnershipPanel.tsx` renders memo overlay separately (filtered out of pie math at `:1366-1367`).
- **Cadence**: `sec_n_port_ingest` monthly day 22 03:00 UTC. Filer directory walker `sec_nport_filer_directory_sync` seeds CIK-trust set (#963).
- **Caveats**: per `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md`, funds are strict subset of institutional pie wedge and must NEVER add to it. Enforced via `denominator_basis` checks in residual/concentration sums (`:927`, `:948`, `:1366-1367`). PR #962 cutover specifically.
- **Validation**: cross-check Vanguard 500's AAPL position separately against Vanguard 13F-HR aggregate — should NOT sum.
- **denominator_basis**: `institution_subset`.

### Blockholder ownership %
- **Definition**: percentage held by Schedule 13D (active >5%) / 13G (passive >5%) filers.
- **Formula**: `Σ blockholder.aggregate_amount_owned / shares_outstanding × 100`. Latest amendment per filer wins.
- **Source**: SEC 13D/G/D-A/G-A. Parser: [app/services/blockholders.py](../../../app/services/blockholders.py).
- **Storage**: `ownership_blockholders_observations` → `ownership_blockholders_current`.
- **Service**: [app/services/ownership_rollup.py:401](../../../app/services/ownership_rollup.py#L401) (sources `'13d'` / `'13g'`).
- **Endpoint**: `/ownership-rollup` slice `category="blockholders"`; flat list at `/instruments/{symbol}/blockholders` ([app/api/instruments.py:3512](../../../app/api/instruments.py#L3512)).
- **Chart**: `OwnershipPanel.tsx` ring 2.
- **Cadence**: write-through on each 13D/G ingest; daily filings sync.
- **Caveats**: 13D and 13G compete with Form 4 / Form 3 in dedup; Form 4 wins (`_PRIORITY_RANK`). Cohen-on-GME bug fix (audit example at `app/services/ownership_rollup.py:18-23`): without dedup, Form 4 + 13D/A double-counted same holder.
- **denominator_basis**: `pie_wedge`.

### Treasury %
- **Definition**: shares the issuer holds itself — additive on top of `shares_outstanding` for chart, NOT a holders slice.
- **Formula**: `treasury_shares / shares_outstanding × 100` (rendered separately as top wedge).
- **Source**: SEC XBRL `us-gaap:TreasuryStockCommonShares` + tag variants.
- **Storage**: `financial_periods.treasury_shares` (sql/088); mirrored to `ownership_treasury_observations` and `ownership_treasury_current` (sql/116).
- **Service**: [app/services/ownership_rollup.py](../../../app/services/ownership_rollup.py) `_read_treasury_from_current`.
- **Endpoint**: `/ownership-rollup.treasury_shares` + `treasury_as_of` (top-level fields, NOT inside `slices`); also balance sheet endpoint.
- **Chart**: `OwnershipPanel.tsx` renders treasury wedge above the ring; legend has treasury swatch.
- **Cadence**: companyfacts daily sync.
- **Caveats**: excluded from numerator of `concentration.pct_outstanding_known` — issuer doesn't "invest" in itself (`:158-164`). Treasury IS allowed to push chart "oversubscribed" if `Σ pie_wedges + treasury > shares_outstanding` (stale-13F + fresh Form 4 mix); residual clamps to zero, banner flags it.
- **Validation**: check `shares_authorized ≥ shares_issued ≥ treasury_shares` invariant.

### Public float / residual
- **Definition**: shares not attributable to any known SEC filing or treasury — by construction includes retail, undeclared institutional below 13F threshold, and any filer outside coverage cohort.
- **Formula**: `residual = shares_outstanding − Σ (slices where denominator_basis="pie_wedge") − treasury_shares`. Clamped ≥ 0; `oversubscribed` flag fires when negative (`:128-140`).
- **Storage**: not stored — computed at read time.
- **Service**: [app/services/ownership_rollup.py:850-948](../../../app/services/ownership_rollup.py#L850-L948).
- **Endpoint**: `/ownership-rollup.residual.{shares, pct_outstanding, oversubscribed}`.
- **Chart**: `OwnershipPanel.tsx ResidualLine` (`:380`); always rendered as grey "Public / unattributed" wedge.
- **Caveats**: residual is NOT a free signal — 90% residual on a small-cap usually means coverage is incomplete, not that retail owns 90%. Coverage banner is the right cue.
- **Validation**: `Σ all wedges + residual + treasury ≈ shares_outstanding` exactly when `oversubscribed=false`.

### denominator_basis explained per kind

`DenominatorBasis = Literal["pie_wedge", "institution_subset"]` ([ownership_rollup.py:68](../../../app/services/ownership_rollup.py#L68)).

| Slice | denominator_basis | Why |
|---|---|---|
| insiders | pie_wedge | Beneficial ownership; sums into pie |
| blockholders | pie_wedge | 13D/G; sums into pie |
| institutions | pie_wedge | 13F-HR equity; sums into pie |
| etfs | pie_wedge | 13F-HR by filer-type ETF; sums into pie |
| def14a_unmatched | pie_wedge | DEF 14A holders unresolved to Form 4 / 13F; conservative addition |
| funds | institution_subset | N-PORT positions are strict subset of parent advisor's 13F-HR aggregate; memo overlay only |
| treasury | (special) | Top-level field; rendered above pie; excluded from concentration numerator |

Future overlays (ESOP #843 / DRS / short-interest #961) will land as additional `institution_subset` rows.

### Share-class collapse (GOOGL+GOOG)
- GOOGL and GOOG share CIK 1652044 (Alphabet) but separate CUSIPs and separate `instrument_id`. Each class has its own card.
- Each class's `shares_outstanding` fetched per-class from `instrument_share_count_latest`; insider/13F filings whose CUSIP matches one class are routed to that class only.
- **No merged view today.** If you need combined Alphabet ownership, sum the two endpoints client-side.

## 2. Fundamentals

All US fundamentals come from SEC XBRL via Company Facts API (settled in `docs/settled-decisions.md` Provider strategy). `app/providers/implementations/sec_fundamentals.py` has the XBRL tag → column map; `app/services/fundamentals.py` is storage layer.

### Revenue
- **Formula**: rolling sum of last four quarterly rows for TTM (gated `is_complete_ttm = TRUE`); single row for quarter / FY.
- **Source**: XBRL `us-gaap:Revenues` + tag-set fallbacks.
- **Storage**: `financial_periods.revenue` per-period; `financial_periods_ttm.revenue_ttm`.
- **Endpoint**: `GET /instruments/{symbol}/financials?statement=income&period=quarterly|annual`.
- **Chart**: `FundamentalsPane.tsx` 8-quarter AreaChart top-left; `fundamentalsCharts.tsx:212` YoY growth.
- **Cadence**: daily via `daily_financial_facts`.
- **Caveats**: superseded rows filtered via `superseded_at IS NULL`. Some MLPs/partnerships file `IncomeLossFromContinuingOperations` not `Revenues` — TTM may be NULL; per-cell null filter on FE keeps chart rendering.

### Operating income / Net income
- Same pattern. Tags: `us-gaap:OperatingIncomeLoss` (MLP variant `IncomeLossFromContinuingOperations`); `us-gaap:NetIncomeLoss`.
- Storage: `financial_periods.{operating_income, net_income}` + TTM views.
- Charts: `FundamentalsPane.tsx` cells 2 + 3; YoY growth bars.

### EBITDA
- **Formula**: `operating_income_ttm + depreciation_amort_ttm` (`sql/080:99-106` `instrument_valuation`).
- **Storage**: `instrument_valuation.ebitda_ttm` VIEW.
- **Endpoint**: not directly exposed; visible to scoring engine only.
- **Caveats**: depreciation/amort can be sparse on small issuers — falls back to operating income only.

### FCF (period) / FCF TTM / FCF yield
- **FCF formula**: `operating_cf - capex` where capex = `PaymentsToAcquirePropertyPlantAndEquipment` (positive outflow in XBRL; subtracting is correct, see [fundamentalsMetrics.ts:246-251](../../../frontend/src/lib/fundamentalsMetrics.ts#L246-L251)).
- **TTM**: `instrument_valuation.fcf_ttm`.
- **FCF yield**: `(operating_cf_ttm - |capex_ttm|) / (current_price × shares_outstanding)` (sql/080:83-87) → `instrument_valuation.fcf_yield`.
- **Endpoint**: per-period FCF computed FE from `/instruments/{symbol}/financials?statement=cashflow`. TTM via `instrument_valuation` (scoring only). FCF yield NOT operator-visible today — **planned operator surface: #671** (needs price-join exposure on L2 fundamentals).
- **Chart**: `fundamentalsCharts.tsx:507` (FCF line chart).
- **Caveats**: `capex` null on issuers without explicit PPE filing → empty-state hint. Negative FCF returns 0 from `_value_score`. Quote-derived; stale quote = stale yield.

### Margins
- **Definitions**: each = `(line / revenue) × 100`.
- **Source**: SEC XBRL.
- **Storage**: `instrument_valuation.{gross_margin, operating_margin, net_margin}` (sql/080:107-116) + per-period derivation.
- **Service**: VIEW + [fundamentalsMetrics.ts:210](../../../frontend/src/lib/fundamentalsMetrics.ts#L210) (`buildMargins`).
- **Caveats**: gross margin requires `cost_of_revenue` which is sparse on financial firms — chart drops gross line.

### Total debt / Net debt / Debt-to-equity / Cash + equivalents
- Total debt = `long_term_debt + short_term_debt`.
- Net debt = `total_debt − cash`.
- Debt-to-equity = `total_debt / shareholders_equity` (NULL when equity ≤ 0).
- Cash = `us-gaap:CashAndCashEquivalentsAtCarryingValue` + tag fallbacks.
- Storage: `financial_periods.{long_term_debt, short_term_debt, cash}`; legacy `fundamentals_snapshot.{net_debt, debt}` for `key_stats`.
- Endpoint: `/instruments/{symbol}/financials?statement=balance` + `/summary.key_stats.debt_to_equity`.
- Chart: `FundamentalsPane.tsx` cell 4 (Total Debt); `fundamentalsCharts.tsx buildDebtStructure` (LTD + STD + interest coverage).

### Total assets / liabilities / equity
- Tags: `us-gaap:Assets`, `us-gaap:Liabilities`, `us-gaap:StockholdersEquity`.
- Storage: `financial_periods.{total_assets, total_liabilities, shareholders_equity}`.
- Chart: `latestBalanceStructure` ([fundamentalsMetrics.ts:326](../../../frontend/src/lib/fundamentalsMetrics.ts#L326)) — two horizontal stacked bars to verify `assets ≈ liab + equity`.

### ROE / ROA / ROIC
- ROE = `net_income / shareholders_equity`. ROA = `net_income / total_assets`.
- ROIC = `NOPAT / invested_capital` where NOPAT ≈ `operating_income × (1 − effective_tax_rate)`; invested capital = `LTD + STD + equity`.
- ROE/ROA per period: [app/api/instruments.py:2863-2864](../../../app/api/instruments.py#L2863-L2864) for `key_stats`; TTM at `instrument_valuation.{roe,roa}`.
- ROIC: [fundamentalsMetrics.ts:415-441](../../../frontend/src/lib/fundamentalsMetrics.ts#L415-L441) (FE derivation; uses 21% US-statutory placeholder when effective tax rate undefined; skips lease liabilities + minority interest — "good enough for trend-watching, not absolute valuation").
- Endpoint: `/summary.key_stats.{roe,roa}`. ROIC: not on API; FE-only on L2 fundamentals.
- Chart: `KeyStatsPane.tsx` ROE/ROA cells; `fundamentalsCharts.tsx` ROIC + DuPont.
- Caveats: when `current_price` unavailable but EPS/book_value present, source label flips `sec_xbrl_price_missing` so FE renders "price missing".

### EPS / P/E / P/B
- EPS: `us-gaap:EarningsPerShareBasic / Diluted` → `financial_periods.{eps_basic, eps_diluted}`; TTM `eps_diluted_ttm`.
- P/E: `current_price / eps_diluted_ttm` (sql/080:69-71); per-period fallback `current_price / eps`. NULL for negative EPS.
- P/B: `current_price / (shareholders_equity / shares_outstanding)`.
- Endpoint: `/summary.key_stats.{pe_ratio, pb_ratio}`.

### Dilution / share-count metrics
- Fields: `latest_shares`, `yoy_shares`, `net_dilution_pct_yoy`, `ttm_shares_issued`, `ttm_buyback_shares`, `ttm_net_share_change`, `dilution_posture` ∈ `{stable, dilutive, buyback_heavy}`.
- Source: XBRL `StockIssuedDuringPeriodSharesNewIssues`, `StockRepurchasedDuringPeriodShares`, `CommonStockSharesOutstanding`, `dei:EntityCommonStockSharesOutstanding`.
- Storage: `instrument_share_count_history`, `instrument_dilution_summary` (sql/052).
- Service: `app/services/dilution.py:get_dilution_summary`.
- Endpoint: `/instruments/{symbol}/dilution`.

## 3. Market data

### Last close / current price
- Source: eToro WebSocket → `quotes.last`; fallback `(bid+ask)/2` (`app/services/xbrl_derived_stats.py:73-76`).
- Storage: `quotes` (1:1 by `instrument_id`, current snapshot, overwritten).
- Endpoint: `/instruments/{symbol}/summary.price`, `/portfolio` positions.
- Chart: hero ticker every L2 page; `PriceChart.tsx`.
- Cadence: live (WS); 60s candle-window backstop poll.

### Day range / 52-week range
- (Deferred.) Would derive from `price_daily.high / low`. `/summary.price.{week_52_high, week_52_low, day_change, day_change_pct}` returns NULL today; FE renders "—". Operator-visible follow-up exists in CLAUDE-md task list.

### Daily candles (OHLCV)
- Source: eToro candles refresh job → `price_daily`.
- Storage: `price_daily` per-day per-instrument.
- Endpoint: `/instruments/{symbol}/candles?range=1w|1m|3m|6m|ytd|1y|5y|max` ([app/api/instruments.py:700](../../../app/api/instruments.py#L700)).
- Chart: `PriceChart.tsx`. Range mapping at `:672-680`.
- Cadence: daily after market close.

### Volume (live volume V2 — planned: #608)
- Today: `price_daily.volume` per day; live-tick volume not aggregated.
- Planned: rolling intraday volume against running average.

### VWAP / Realised volatility / Beta vs SPY
- All **deferred**. eToro candles do not include intraday VWAP. ATR-14 stored at `price_daily.atr_14` (sql/025) consumed by scoring as proxy.

### Total return windows
- 1d / 1w / 1m: dashboard via `/portfolio/rolling-pnl` ([app/api/portfolio.py:710](../../../app/api/portfolio.py#L710)). Anchor uses each position's `latest_close` `price_date` — never wall-clock — so stale candle doesn't collapse the bucket (Codex #387 phase 2 finding).
- 3m / 6m / 1y: consumed internally by scoring (`return_3m`, `return_6m`); not surfaced as own panel today.
- 5y / since-IPO: not surfaced.

### Technical indicators (TA scoring layer)
- Stored on latest `price_daily` row only:
  - `sma_20`, `sma_50`, `sma_200`.
  - `ema_12`, `ema_26`, `macd_line`, `macd_signal`, `macd_histogram`.
  - `rsi_14`, `stoch_k`, `stoch_d`.
  - `bb_upper`, `bb_lower`, `atr_14`.
- Formulas: sql/025:7-14. Implementations: `app/services/technical_analysis.py`.
- Used by `_momentum_score` ([scoring.py:322](../../../app/services/scoring.py#L322)) for v1.1-balanced.
- Operator-visible: NOT in v1.

### Market cap (live)
- Formula: SEC XBRL share count × current price ([xbrl_derived_stats.py:43-95](../../../app/services/xbrl_derived_stats.py#L43-L95)). Share count from `instrument_share_count_latest` (DEI > us-gaap; newest restated).
- Endpoint: `/summary.identity.market_cap`.
- Caveats: NULL for non-SEC instruments — no third-party fallback per #498/#499 settled decision.

## 4. Capital returns (dividends + buybacks)

### Dividend per share (latest announced)
- Source: XBRL `us-gaap:CommonStockDividendsPerShareDeclared` per period; 8-K parser for forward calendar (`app/services/dividends.py:177-237`).
- Storage: historical `financial_periods.dps_declared` → `dividend_history` view (sql/050). Forward `dividend_events` (sql/054) populated by 8-K parser.
- Service: `dividends.py` `get_dividend_summary:84`, `get_dividend_history:125`, `get_upcoming_dividends:177`.
- Endpoint: `/instruments/{symbol}/dividends`.
- Chart: `DividendsPanel.tsx`; full L2 `DividendsPage.tsx`.
- Caveats: latest = newest QUARTER WITH POSITIVE DPS (zero/null rows excluded). Tie-break Q4 > FY same period_end.

### Dividend yield (TTM)
- Formula: `(ttm_dps / price) × 100`.
- Storage: `instrument_dividend_summary.ttm_yield_pct`.
- Endpoint: `/summary.key_stats.dividend_yield` and `/dividends.summary.ttm_yield_pct`.
- Caveats: NULL when `ttm_dps` null/zero or price null/zero. `dividend_streak_q` counts consecutive positive-DPS quarters.

### TTM DPS / TTM dividends paid
- `instrument_dividend_summary.ttm_dps`, `ttm_dividends_paid`.

### Payout ratio (Dividends/FCF)
- Formula: `dividends_paid / FCF × 100`. Computed FE on **annual** data only (quarterly too noisy). FCF-negative years → NULL clamped.
- Service: [dividendsMetrics.ts:174](../../../frontend/src/lib/dividendsMetrics.ts#L174) (`buildPayoutRatio`).
- Caveats: SEC XBRL `PaymentsOfDividends` is positive outflow; helper `Math.abs()` to normalise issuers under-reporting negative.

### Buyback authorisation outstanding
- **Deferred** — `dividend_events` parses dividend calendars from 8-K but no buyback-authorisation parser exists. Operator must consult 8-K Item 8.01 / 1.01 directly via `/instruments/{symbol}/eight_k_filings`.

### Net buyback rate
- Derives from `instrument_dilution_summary.{ttm_buyback_shares, ttm_shares_issued}`. Surfaces `dilution_posture` ∈ `{stable, dilutive, buyback_heavy}`.
- Endpoint: `GET /instruments/{symbol}/dilution` ([app/api/instruments.py:1577](../../../app/api/instruments.py#L1577)).

### Yield-on-cost
- An income investor's "what's the yield on what I originally paid?". Derived FE only.
- Service: [dividendsMetrics.ts:200+](../../../frontend/src/lib/dividendsMetrics.ts#L200) (`buildYieldOnCost`); requires `/portfolio/instruments/{id}` for entry price.
- Chart: dividends drilldown.

## 5. Filings + events

### Last 10-K / 10-Q / 8-K date
- Storage: `filing_events.filing_date` (per filing per provider), typed by `filing_events.form_type`.
- Service: `app/services/filings.py`; api `app/api/filings.py:27`.
- Endpoint: `GET /filings?instrument_id=...&form_types=...`.
- Chart: `FilingsPane.tsx` on L1 instrument page.
- Caveats: settled — `filing_events` stores metadata + summary + risk score + provider payload + canonical document link. Full raw filing text out of scope. Raw documents land in `filing_raw_documents`.

### 10-K Item 1 subsections
- Source: SEC 10-K HTML body parser (`app/services/business_summary.py`, `app/services/filing_documents.py`).
- Storage: `instrument_business_summary` + `instrument_business_summary_sections` (sql/059).
- Endpoint: `/instruments/{symbol}/business_sections` ([app/api/instruments.py:1358](../../../app/api/instruments.py#L1358)).
- Parse states: `not_attempted` / `parse_failed` / `no_item_1` / `sections_pending`.

### 8-K filings (structured items + exhibits)
- Source: SEC 8-K parser (`app/services/eight_k_events.py`).
- Storage: `eight_k_structured_events` (sql/061), `filing_documents` (sql/062 exhibit pointers).
- Endpoint: `/instruments/{symbol}/eight_k_filings?limit=...` ([app/api/instruments.py:1195](../../../app/api/instruments.py#L1195)).
- Per-item severity: `frontend/src/components/instrument/eightKSeverity.ts`.
- Chart: `EightKEventsPanel.tsx`, `EightKDetailPanel.tsx`, `EightKListPage.tsx`.

### 8-K event severity
- FE-only mapping (not regulatory). Item 1.01 (material agreement), 4.02 (non-reliance / restatement), 5.02 (departure of officer) typically high severity.

### Insider 90-day net / counts
- Two lenses:
  - **Open-market**: only discretionary purchases/sales (codes P/S).
  - **Total-activity**: every non-derivative txn classified by `acquired_disposed_code`.
- Fields: `open_market_net_shares_90d`, `open_market_buy_count_90d`, `open_market_sell_count_90d`, `total_acquired_shares_90d`, `total_disposed_shares_90d`, `acquisition_count_90d`, `disposition_count_90d`, `unique_filers_90d`, `latest_txn_date`.
- Service: [insider_transactions.py:get_insider_summary](../../../app/services/insider_transactions.py).
- Storage: `insider_transactions` + `insider_initial_holdings` (Form 3) + tombstones via sql/058.
- Endpoint: `/instruments/{symbol}/insider_summary`; detail `/insider_transactions?limit=...`; baseline `/insider_baseline`.
- Chart: compact `InsiderActivitySummary.tsx`; L2 `InsiderPage.tsx` → `InsiderNetByMonth`, `InsiderByOfficer`, `InsiderTransactionsTable`, `InsiderPriceMarkers`.
- Caveats: derivative grants and option exercises EXCLUDED from `open_market_*`. Tombstones excluded.
- Validation: cross-reference openinsider.com.

### 13F holdings change (last quarter)
- Storage: `ownership_institutions_observations` (immutable per-quarter rows, sql/114).
- Endpoint: `/institutional-holdings` (flat list); rollup at `/ownership-rollup`.
- Today the L2 ownership page shows per-filer current holdings without explicit "delta last quarter" panel.

### DEF 14A vote summary
- **Planned.** DEF 14A parser exists for beneficial holdings (`def14a_ingest.py`, `def14a_drift.py`) and surfaces `def14a_unmatched` slice on rollup, but vote tabulation (board elections / shareholder proposals) is not parsed.

## 6. News + sentiment

### News volume (last N days)
- COUNT of `news_events` rows in lookback for an instrument.
- Storage: `news_events` (sql/005). Unique key `(instrument_id, url_hash)` per settled-decisions.
- Service: `app/services/news.py`.
- Endpoint: `GET /news/{instrument_id}?since=...&limit=...`. Default 30-day window.
- Chart: `RecentNewsPane.tsx`.

### News sentiment score
- `[-1, +1]` signed numeric (no labels — settled).
- Storage: `news_events.sentiment_score` FLOAT.
- Aggregated into ranking `sentiment_score` (`scores.sentiment_score`).
- Caveats: settled — persist as signed numeric only (no label columns in v1). Out-of-range logged as warning and clipped on aggregation.

## 7. Risk + portfolio

### AUM
- Definition: total operator capital marked-to-market.
- Source: `positions × quotes` + `cash_ledger` SUM + mirrors.
- Endpoint: `/portfolio`.
- Settled: AUM uses mark-to-market first; cost-basis fallback; never unrealised P&L.

### Cash balance / Cash buffer / Available for deployment / Capital event
- Cash balance: `cash_ledger` SUM. Sign: + inflow / − outflow.
- Cash buffer: `budget_config.cash_buffer_pct`.
- Capital events: `capital_events` table.
- Endpoints: `/portfolio`, `/budget`, `/budget/events`.
- Settled: unknown cash tolerated in PM, hard-blocked in execution guard.

### Position market value / cost basis / unrealised P&L
- `positions.cost_basis`; market value derived from `quotes.last`; unrealised P&L = `(quotes.last − cost_basis) × shares`.
- Endpoint: `/portfolio`.

### Position alerts
- `position_alerts` table → `/alerts/position-alerts`.

### Rolling P&L (1d / 1w / 1m)
- Computed from `price_daily` deltas vs anchor (each position's own `latest_close.price_date`, not wall-clock).
- Endpoint: `/portfolio/rolling-pnl`.

### CGT / estimated tax
- `tax_disposals` table + `budget_config.cgt_scenario`. Computed via `tax_ledger`.
- Endpoint: `/budget` (`estimated_tax_gbp` / `_usd`).

### Recommendation history / Exit / HOLD
- `trade_recommendations` table.
- Endpoint: `/recommendations`, `/recommendations/{id}`.
- Settled: append-oriented persistence, no spam HOLDs. Default HOLD unless EXIT fires.

## 8. Pipeline / system metrics (operator-visible)

### Bootstrap state + stage status
- `bootstrap_state` (singleton, `pending/running/complete/partial_error`).
- `bootstrap_runs` (per-click); `bootstrap_stages` (per-stage detail; lane ∈ init/etoro/sec; status ∈ pending/running/success/error/skipped).
- `bootstrap_archive_results` (per-archive audit).
- Endpoint: `GET /system/bootstrap/status`.

### Backfill / SEC manifest pending count
- `sec_filing_manifest WHERE ingest_status IN ('pending','failed')`.
- `data_freshness_index` for cadence-due subjects.
- Endpoints: `/system/jobs`, `/system/bootstrap/status`.

### ETL freshness per source
- `data_freshness_index`. State `unknown/current/expected_filing_overdue/never_filed/error`. Cadence map at [data_freshness.py:69-100](../../../app/services/data_freshness.py#L69-L100).
- No public read endpoint — admin via DB.

### Coverage tier / Coverage % per source
- `coverage.coverage_tier` (1/2/3) per instrument.
- `coverage_audit` history; ownership rollup `coverage` block.
- Endpoints: `/instruments`, `/coverage/summary`, `/coverage/insufficient`, `/instruments/{symbol}/ownership-rollup`.
- Settled: coverage = telemetry, not a gate.

### Credential health (eToro)
- `broker_credentials_health_state` (sql/128).
- Endpoint: `/system/status.credential_health`.

### Job runs success rate
- `job_runs` history; aggregated by `app/services/ops_monitor.py:check_job_health`.
- Endpoint: `/system/status`, `/system/jobs`.

### Kill switch state
- `runtime_config` row keyed by name. Read by `get_kill_switch_status` ([ops_monitor.py](../../../app/services/ops_monitor.py)).
- Toggle: `POST /system/config/kill-switch` ([app/api/config.py:233](../../../app/api/config.py#L233)).
- Endpoint: `/system/status.kill_switch`.

## 9. Validation + golden panel

CLAUDE.md ETL clauses 8-12 mandate verification on the canonical 5-instrument panel for any change touching ownership / fundamentals / observations:

- **AAPL** — large-cap, dense 13F filer set, multi-decade history.
- **GME** — high retail residual, Cohen direct + beneficial split, activist 13D/A history.
- **MSFT** — broad institutional coverage, dividend issuer, frequent buybacks.
- **JPM** — financial sector (no gross margin), large insider direct + indirect totals.
- **HD** — REIT-style cash-flow profile, dilution-stable.

Cross-source for at least one fixture against an independent reputable source (gurufocus, marketbeat, EdgarTools golden file, SEC EDGAR direct). Record the source + figure in the PR description.

After backfill, hit the relevant rollup endpoint and confirm the figure renders correctly on the live chart. PR description records the verification step + commit SHA for each clause.

## 10. Specs to consult

- `docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md`
- `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md` (Phase 1 + Phase 3)
- `docs/superpowers/specs/2026-05-04-etl-coverage-model.md`
- `docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md` (#843 ESOP)
- `docs/superpowers/specs/2026-05-07-first-install-bootstrap.md` (#993)
- `docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md`

`docs/settled-decisions.md` — every numbered decision constraining a metric.

## 11. Cross-link

- `.claude/skills/data-sources/sec-edgar.md` — where the source data comes from.
- `.claude/skills/data-sources/edgartools.md` — the parsing library.
- `.claude/skills/ebull/data-engineer.md` — schema invariants + write/read patterns.
