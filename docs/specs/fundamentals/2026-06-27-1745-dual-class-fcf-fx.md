# #1745 — per-period dual-class FCF yield + cross-currency FX normalisation

Parent #585. Predecessor #671 (FCF yield trend). Operator decision 2026-06-27: build full (A) now + (B).

## Problem
`fcf_yield_series` (app/services/fcf_yield.py) **fail-closed suppresses** two issuer classes:
- **`multiclass`** — any curated multi-class issuer (`resolve_market_cap_basis().basis != "not_multiclass"`). `close × combined_shares` is the structurally-wrong cap #1662 retired; v1 did no per-period per-class reconstruction.
- **`currency_mismatch`** — `financial_periods.reported_currency != instruments.currency`, with no FX normaliser (sql/024 caveat).

Dev: 6 multi-class instruments (GOOG/GOOGL, HEI/HEI.A, METC/METCB); **0** currency-mismatch.

## Source rule
- **Per-class shares** = SEC DERA FSDS `num.txt`, `tag == "CommonStockSharesOutstanding"` AND `version` starts `us-gaap/`, single `ClassOfStock=<member>` segment, `uom=shares qtrs=0`, current period = `ddate == sub.period` (the settled rules already encoded in `app/services/fsds_class_shares.py`, spec `docs/specs/etl/2026-06-17-per-class-shares-denominator.md`). The companyfacts JSON API strips dimensional facts, so `financial_facts_raw` holds only the COMBINED count.
- **Total-company cap** = `Σ class_price × class_shares + residual×impute_price`, all fail-closed guards in `xbrl_derived_stats._assemble_total_company_cap` (settled `docs/specs/etl/2026-06-17-per-class-market-cap.md`).
- **FX** — period-end rate from `fx_rates_daily` (sql/196) via `app/services/fx_history.load_fx_rates_for_date` (USD-base, carry-forward). FCF is reporting-currency; price (hence cap) is trading-currency.

## Full-population verification (dev — premise of the original handoff falsified)
A per-period cap **trend** needs per-class shares **at each historical period_end**. That data did **not** exist:
- `instrument_class_shares_outstanding`: **latest snapshot only** — 1 period_end per instrument (6 rows total), because the orchestrator ingests only `last_n_quarters(n_fsds=4)` and only `fsds_2025q1.zip` is cached.
- `instrument_dimensional_facts`: segment/geo/product **revenue/income/assets** only — no share metric, no ClassOfStock axis.
- `financial_facts_raw`: **consolidated only** (no dimensional column); GOOG `CommonStockSharesOutstanding` = 12.1B combined.

**Key structural finding:** the fix is *not* a new parser/table. `instrument_class_shares_outstanding` PK is **`(instrument_id, period_end)`** — it already holds history; `ingest_fsds_class_shares_archive` already derives `period_end = ddate` per class. The gap is purely that **only one FSDS quarter was ever ingested**. Ingesting historical quarters fills the per-period rows with the existing, settled parser.

## Design

### (A1) Historical FSDS ingest backfill
`scripts/backfill_fsds_class_shares_history.py`: build `BulkArchive`s for the last N FSDS quarters (`last_n_quarters(N)` → `files/dera/data/financial-statement-data-sets/{q}.zip`), `download_bulk_archives(archives=…)` into the bulk dir, then run the existing `ingest_fsds_class_shares_archive` per quarter (no-demotion upsert handles overlap/restatement). `--quarters N` (default e.g. 20 ≈ 5y), `--keep`/delete-after. Reuses every settled primitive; output is tiny (per-class rows over the 3 curated issuers × N quarters). Each ZIP ~530 MB streamed.

### (A2) Per-period total-company cap read
`xbrl_derived_stats.total_company_cap_at_period(conn, *, cik, period_end) -> TotalCompanyMarketCap | None`: refactor `_build_total_company_cap` so the period is a parameter, not `MAX(period_end)`. For a given `period_end`:
- per-class shares = the `instrument_class_shares_outstanding` row for the CIK at the FSDS period **nearest** `period_end` (exact-or-nearest, same `ABS(period_end − target)` ordering as `_read_combined_shares_near`);
- per-class **price = `price_daily.close` at ≤ period_end** (NOT `_latest_price`) — new `_price_at(conn, iid, period_end)`;
- combined = `_read_combined_shares_near(conn, …, period_end)`.
Defer to the unchanged pure `_assemble_total_company_cap` (its guards: ≥2 siblings, future-date, combined-delta ≤ `_MAX_COMBINED_FSDS_DELTA_DAYS`, `class_shares_usable` staleness, Σ ≤ combined×1.005, residual ≤ 25%). The existing latest-pinned `_build_total_company_cap` becomes a thin `total_company_cap_at_period(…, period_end=MAX)` wrapper so `resolve_market_cap_basis` is unchanged.

### (A3) fcf_yield multiclass path → per-period, drop suppression
Replace the whole-series `multiclass` suppression: for a multi-class issuer, compute `market_cap` per financial_periods row via `total_company_cap_at_period`; a period whose cap fails its guards → `market_cap=None` → that point shows absolute FCF but **NULL yield** (per-point fail-closed, the FE already gaps NULL `fcf_yield_pct`). Single-class path unchanged. The `multiclass` suppression literal is retired.

### (B) FX normalisation, drop currency_mismatch suppression
When `reported_currency != trading currency`, convert `fcf_ttm` (reporting ccy) → trading ccy at the **period-end** rate (`load_fx_rates_for_date`; USD-base, so cross rate = `rate[trading]/rate[reported]`) before `fcf_yield_pct`. A period with no FX rate on/near its date → NULL yield (per-point fail-closed). Retire the `currency_mismatch` suppression. 0 dev instruments → covered by unit tests + a synthetic fixture; forward safety net.

## Out of scope / kept
The fail-closed guards stay for genuinely-uncomputable periods. `resolve_market_cap_basis` / ownership rollup unchanged. Untraded residual imputation unchanged. New curated dual-class issuers still need a `_CLASS_MEMBER_TO_CUSIP` entry (settled).

## Tests
- Pure: `total_company_cap_at_period` period selection (nearest FSDS period, price-at-period) + FX cross-rate math, table-tested in `_assemble`-style harness.
- FX cross-rate helper (USD-base → trading/reported) pure test.
- db-tier: ingest two FSDS quarters → two period rows → per-period cap differs by period.
- FE: FcfChart already handles NULL yield + multi-point; verify no `suppressed_reason` path break (the literal set shrinks).

## DoD (ETL clauses 8–12)
- Backfill executed on dev: N historical FSDS quarters ingested → `instrument_class_shares_outstanding` gains per-period rows for the 3 issuers (record counts).
- Smoke: `/instruments/GOOG/fcf-yield?period=quarterly` now returns **points (not suppressed)** with per-period `market_cap`/`fcf_yield_pct`; same on GOOGL (identical company total). HEI/METC likewise.
- Cross-source one period's per-class shares vs the issuer's 10-Q cover (e.g. Alphabet Class A vs Class C).
- Record commit SHA + the GOOG figure.
