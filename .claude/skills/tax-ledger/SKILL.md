---
name: tax-ledger
description: eBull UK tax + disposal-matching engine — HMRC same-day/30-day/s104 matching in app/services/tax_ledger.py, the tax_lots/disposal_matches/s104_pool tables, and the passive /tax read API.
---

# tax-ledger

## When to use

Any change to `app/services/tax_ledger.py` (ingest, matching, or the read
surface), the `tax_lots` / `disposal_matches` / `s104_pool` / `fx_rates` tables
(sql/001_init.sql, sql/013_tax_disposal_matching.sql), the `/tax/*` endpoints
(`app/api/tax.py`), the `daily_tax_reconciliation` job
(`app/workers/scheduler.py`), or `frontend/src/pages/TaxPage.tsx` /
`frontend/src/api/tax.ts`. This is build priority 8 (Ledger and tax engine) —
the read API + engine shipped (#11, #1905 PR1); dev tax data is currently EMPTY.

## What it is

A deterministic UK CGT engine. Fills → tax lots → HMRC disposal matches → a
tax-year view. No ML, no estimation in the matching path — every gain is
traceable to a lot.

**Ingest** — `ingest_tax_events(conn)` → `IngestionResult`. `_ingest_fills`
reads `fills` JOIN `orders` JOIN `instruments` for fills not yet in `tax_lots`,
converts to GBP, writes one `tax_lots` row per fill. `BUY`/`ADD` →
`direction='acquisition'` (cost = gross + fees); `EXIT` → `'disposal'`
(proceeds = gross − fees); unknown action → `logger.warning` + skip. Idempotent:
`ON CONFLICT (reference_fill_id) DO NOTHING`. `_ingest_cash_events` is a **stub
returning 0** — dividend/fee lots are not ingested yet, so `dividend_total_gbp`
is always 0 today (#11 follow-up: cash-event dedup key).

**Match** — `_match_disposals_for_instrument(acquisitions, disposals)` is a
**pure function** applying HMRC rules in order: (1) same-day, (2) 30-day bed-and-
breakfast (`d+1..d+30`), (3) Section 104 pool. All comparisons on `uk_date`
(Europe/London calendar date), all money in GBP. `run_disposal_matching(conn,
instrument_id=None)` orchestrates: per instrument it DELETEs prior
`disposal_matches` and recomputes (idempotent delete-and-recompute), then upserts
`s104_pool` (`ON CONFLICT (instrument_id)`).

**Read** — `tax_year_summary`, `disposals_for_tax_year`, `disposal_audit_trail`,
`s104_pool_rows`, `available_tax_years`, `current_tax_year`, `valid_tax_year`.
The engine owns every CGT treatment: `_CGT_RATE_PERIODS` (tax_ledger.py:43 —
2024-04-06→10-29 basic 0.10/higher 0.20, then 0.18/0.24), `ANNUAL_EXEMPT = £3000`
(tax_ledger.py:58), and the basic/higher scenario estimates. Earliest priceable
disposal is 2024-04-06; `_MIN_SUPPORTED_TAX_YEAR_START` tracks the rate table.

**Tables** (verified): `tax_lots` (sql/001:179, extended sql/013:14 —
`direction`, `original_currency`, `fx_rate_to_gbp`, `amount_gbp`; unique index on
`reference_fill_id`); `disposal_matches` (sql/013:25); `s104_pool` (sql/013:46,
`UNIQUE (instrument_id)`); `fx_rates` (sql/013:5, PK `(rate_date, from_currency,
to_currency)`). Reads `fills`, `orders`, `instruments`.

**Endpoints** (`app/api/tax.py`, prefix `/tax`, registered app/main.py:560, all
`require_session_or_service_token`, each wrapped in `snapshot_read`): `GET
/tax/summary`, `GET /tax/disposals`, `GET /tax/pools`, `GET /tax/tax-years`.
Handlers only shape engine output — no tax logic lives in the API.

**Job** — `daily_tax_reconciliation` (scheduler.py:4390): sole writer of
`tax_lots` / `disposal_matches` (`db` lane, sources.py:535). Ingest then match on
two separate connections so a match failure does not roll back committed ingest.
⚠ Despite its docstring ("Runs daily"), it is **on-demand only** — registered in
`_INVOKERS` (runtime.py:296), NOT on an APScheduler cadence (scheduler.py:1787).
Trigger via the Admin "Run now"; it does not self-run.

## Invariants

- Raw events immutable; views derived from stored events. `tax_lots` is
  append-only (one row per fill via the unique index); `disposal_matches` /
  `s104_pool` are the delete-and-recompute derived layer.
- Matching-rule provenance preserved on every `disposal_matches` row
  (`matching_rule`, both lot ids); `disposal_audit_trail` gives full per-lot
  provenance (auditability — every trade path must be auditable).
- All matching + gains in GBP. CGT treatment (rates, £3,000 exempt, scenario
  estimates) is owned by the engine, never the API handler.
- `estimated_cgt_*` are **scenario** estimates only — actual CGT depends on the
  taxpayer's income and band; never present as a definitive figure.
- Long-only v1: only `BUY`/`ADD` (acquisition) and `EXIT` (disposal) map to lots;
  no shorting.
- The tax `fx_rates` table is **distinct** from `fx_rates_daily` — see
  settled-decisions "Own EOD NAV-snapshot table (#1594)": dropping ECB rows into
  the tax table would silently change the safety-critical USD tax-disposal path
  (`tax_ledger._load_fx_rate`). Cash-sign convention: settled-decisions
  "Portfolio manager semantics → Cash semantics" (positive = inflow).

## Failure conditions

Missing critical source data, stale/out-of-range dates, and impossible states
surface as explicit signals — never a neutral default:

- Missing FX rate for a non-GBP disposal → `_load_fx_rate` raises RuntimeError
  (tax_ledger.py:275), halting ingest. Never fabricate a rate.
- Disposal before 2024-04-06 → `_cgt_rates_for_disposal` raises RuntimeError;
  `valid_tax_year` rejects the year → API 422 (never a 500, never a silently
  empty summary).
- Unmatched disposal units → `logger.warning "incomplete acquisition data"`
  (tax_ledger.py:612) — surfaced, not absorbed.
- Negative pool units or cost → RuntimeError invariant check (tax_ledger.py:631).
- Unknown fill action → `logger.warning` + skip, not a silent acquisition.
