# Data housekeeping + richness audit (2026-04-18)

**Context:** Post-Chunk-H session. User flagged two concerns:
1. `data/` folder ballooning with apparently-repeated downloads.
2. Are we extracting the full richness from ingested files? Gaps?

**Findings from Explore audit:**

## Disk survey (`du -sh data/`)

```
data/raw/sec_fundamentals/   225 GB    59,789 files   sec_facts_<cik>_<ts>.json  (7.5 MB each)   NO retention
data/raw/sec/                 12 GB    55,082 files   sec_submissions_<cik>_<ts>.json + sec_filing_*.json   NO retention
data/raw/etoro/              184 MB     1,388 files   candles_<iid>_<ts>.json                    NO retention
data/raw/etoro_broker/        71 MB       252 files   etoro_portfolio_<ts>.json                  NO retention
data/raw/fmp/                  1 MB       762 files   fmp_*_<symbol>_<ts>.json                   NO retention
TOTAL                        237 GB   117,273 files   (zero housekeeping anywhere)
```

**Concrete waste:** spot-check of CIK `0000320193` (Apple) in `sec_fundamentals/` shows **15 byte-identical copies** (same md5 `2ee9730e...`), each 7.5 MB. Every CIK exhibits the same 13–15× duplication. Root cause: `_persist_raw(tag, payload)` always writes a fresh timestamped file; no content-hash check, no retention sweep anywhere in `app/`.

## Ingestion-completeness survey

**SEC fundamentals path — 10% extraction of raw XBRL:**
- Raw `sec_facts_<cik>.json` contains **503 XBRL concepts × ~48 historical periods = ~24,500 facts** per company, going back to 2009.
- `app/providers/implementations/sec_fundamentals.py:81` defines `TRACKED_CONCEPTS` — **34 canonical concepts × ~50 aliases**. Everything else discarded before `financial_facts_raw` write.
- **Discarded 90% of raw data:** segment reporting, tax detail, off-balance-sheet leases, contingent liabilities, related-party transactions, warranty/environmental reserves, geographic revenue breakdown, etc.

**eToro candles — downloaded, never persisted:**
- `data/raw/etoro/candles_*.json` — 1,388 hourly OHLCV snapshots. **No DB table consumes them.**
- `quotes` table stores only latest price. Historical OHLCV lost for every instrument.

**SEC submissions — 3 fields extracted of 20+:**
- `sec_submissions_<cik>.json` includes LEI, SIC, website, address, insider-transaction flags, business addresses, former names.
- `_normalise_filings` takes only accession + form + filing_date. Rest discarded.

**FMP — fallback only, minimal use:**
- 762 files, 1 MB. Only triggered when SEC returns nothing. Rich analyst data (estimates, price targets, earnings surprises) fetched but largely unused.

## Gap categories (no current pipeline)

- **Insider transactions** (Forms 3/4/5) — zero ingestion. CFO share sales, CEO buybacks invisible.
- **Institutional ownership** (13F) — quarterly snapshots of who holds what. Zero ingestion.
- **Segment reporting** — consolidated only; geographic/business-unit P&L ignored.
- **Corporate actions** — dividend history, splits, spinoffs. No systematic capture.
- **Earnings call transcripts / MD&A text** — sentiment signal missed.

---

## Proposed workstreams

### Plan A — Disk rescue (1 day, 1 PR)

**Goal:** reclaim ~180 GB; bound future growth to ~6 GB/month.

1. **Add `_persist_raw` dedup:** compute `sha256(payload)`; check latest file matching `{tag}_*.json` in the target directory for same hash; if hash match, skip write and log "cache hit". Saves 90 MB per sync run. Low risk: byte-identical duplicates provide zero audit value.
2. **New scheduler job `raw_data_retention_sweep`:** runs daily at 02:00 UTC before `orchestrator_full_sync`. Walks `data/raw/**`, deletes files older than `RAW_DATA_RETENTION_DAYS` (default 30, configurable via settings). Logs counts per subdirectory.
3. **One-shot cleanup on first run:** existing 225 GB of `sec_fundamentals/` duplicates removed by the sweep job's initial execution. No separate migration script.
4. **Unit tests + integration test:** hash-collision, retention boundary (exactly 30d), permissions failure handling.

**Risks to validate with Codex:**
- Does any code path elsewhere depend on "every fetch = new file" (e.g. forensic replay, audit trail)?
- 30d arbitrary — should insider-sensitive files (future 3/4/5) have a longer retention?
- Dedup skips the `_persist_raw` write but still persists to DB; does anything reconstruct from raw files instead of DB?

### Plan B — Richness expansion (3-5 days, multiple PRs)

**Goal:** extract more value from raw we already pull.

1. **Expand `TRACKED_CONCEPTS`** from 34 → ~100 canonical concepts. Adds cash flow detail (working capital changes, stock comp, capex breakdown), balance sheet granularity (current vs. non-current split, lease liability, deferred tax), income statement detail (operating vs. non-operating, discontinued ops, restructuring, litigation, acquisition amortization). Re-normalizes historical `financial_facts_raw` → `financial_periods` via a backfill job.
2. **Ingest eToro candles → `market_data_candles`:** new table, batch upsert from `data/raw/etoro/candles_*.json` during `daily_market_data`. Unlocks: volatility, Sharpe, drawdown analysis, entry-timing backtests.
3. **Extract company metadata from `sec_submissions_*.json`:** upsert LEI, SIC code, website, business address to `instruments` table via an `enrich_from_sec_submissions` helper inside the existing filings refresh.

### Plan C — New pipelines (separate issues, ~2 days each)

1. **Insider transactions (Forms 3/4/5):** new `insider_transactions` table, new provider method `fetch_insider_transactions(cik)`, nightly scheduler job.
2. **Institutional ownership (13F):** new `institutional_holdings` table, quarterly refresh keyed on quarter-end dates.
3. **Segment reporting:** SEC `SegmentReporting` taxonomy parse during XBRL normalization; new `financial_segments` table.

### Separate workstream — Chunk L

Master-plan-deferred filings-fetch dedupe. Static audit + feature flag instead of "wait for prod time" per prior discussion. Revisit after Plan A lands.

### Separate workstream — Admin UX redesign

User's hands-off expectation vs current jobs-grid admin page. Fund-vision territory (MCP chat + overnight-summary feed). Separate ticket.

---

## Priority recommendation

**Ship Plan A first** (disk rescue — immediate operational win, low risk).
Then **Plan B.1 + B.2 together** (SEC concepts expansion + eToro candles — high leverage, same DB-ingestion pattern).
Then **Plan C.1** (insider transactions — highest signal-to-effort among new pipelines).

Everything else (Plan B.3, C.2, C.3, Chunk L, admin UX) as separate follow-up tickets.

## Codex checkpoints

Per CLAUDE.md:
1. **Pre-plan review (this doc)** — run before implementing anything from Plan A. Focus: is dedup-on-write safe, retention-duration choice, richness priority order, are there audit/compliance reasons to retain raw files longer than 30 days?
2. Per-PR pre-push review as usual.
