---
name: universe-sync
description: eBull tradable-universe sync — the nightly eToro instrument-list upsert (sync_universe in app/services/universe.py), the instruments table + is_tradable flag that gates scoring eligibility, and the canonical-redirect / symbol-history bookkeeping it drives.
---

# universe-sync

## When to use

Any change to `app/services/universe.py`, the `instruments` table
(`sql/001_init.sql` core + `sql/070` instrument_type_id + `sql/145`
canonical_instrument_id), the `nightly_universe_sync` job
(`app/workers/scheduler.py`), the provider contract
`MarketDataProvider.get_tradable_instruments` (`app/providers/market_data.py`)
/ its eToro impl (`app/providers/implementations/etoro.py`),
`reconcile_symbol_history` (`app/services/instrument_history.py`), or the
canonical-redirect populate (`app/services/canonical_instrument_redirects.py`).
Also read it before touching any consumer of `is_tradable` (scoring
eligibility, portfolio, coverage).

## What it is

Build priority 1 (`.claude/CLAUDE.md` → Build priorities). Entry point:
`sync_universe(provider, conn) -> SyncSummary(inserted, updated, deactivated)`
in `app/services/universe.py`. Pulls the full tradable list via
`provider.get_tradable_instruments()` (returns `list[InstrumentRecord]`) and
upserts into `instruments`, keyed on `instrument_id` (the eToro provider id,
stable across renames).

- New rows insert `is_tradable=TRUE`; changed metadata (symbol,
  company_name, exchange, sector, industry, instrument_type_id) updates in
  place under a guarded `WHERE ... IS DISTINCT FROM` (no redundant rewrites).
- Instruments no longer in the feed → `is_tradable=FALSE`, `last_seen_at=NOW()`;
  **never DELETE** — the row and its history are retained (auditability).
- `country` (ISO 3166-1 alpha-2) and `currency` (ISO 4217) are NOT from the
  provider (the eToro instruments endpoint exposes neither). Both derive from
  the operator-curated `exchanges` table via
  `instruments.exchange = exchanges.exchange_id`. Single source of truth is the
  exchanges curator — no instrument-level override (#1233 §6.1). A missing
  exchange row preserves the prior value rather than wiping to NULL. One-shot
  backfills: `sql/158` (country), `sql/159` (currency).
- `reconcile_symbol_history(conn)` (`app/services/instrument_history.py`, #794)
  runs in the same transaction: a ticker change (FB→META) lands as a plain
  symbol UPDATE, then this closes the prior `instrument_symbol_history` chain
  link and opens the new one.

Table `instruments` (`sql/001_init.sql`): `instrument_id BIGINT PK, symbol,
company_name, exchange, currency, sector, industry, country, is_tradable
BOOLEAN NOT NULL DEFAULT TRUE, first_seen_at, last_seen_at`;
`idx_instruments_tradable`. `instrument_type_id INTEGER` added `sql/070`;
`canonical_instrument_id BIGINT NULL` self-FK + `instruments_canonical_not_self_chk`
CHECK added `sql/145`.

Job: `nightly_universe_sync` (`app/workers/scheduler.py:2149`, constant
`JOB_NIGHTLY_UNIVERSE_SYNC = "nightly_universe_sync"`) — nightly, idempotent,
under `JobLock`. Sequence: `sync_universe` → `reclassify_unknown_exchanges`
(#1055) → `seed_coverage` (first-run Tier-3 seed) → `bootstrap_missing_coverage_rows`
(#292, gives late-joining instruments a coverage row). Registered in
`app/jobs/runtime.py`.

Canonical redirect: `populate_canonical_redirects_job`
(`app/services/canonical_instrument_redirects.py`, constant
`JOB_POPULATE_CANONICAL_REDIRECTS`) is a separate idempotent job the operator
triggers after a sync introduces new `.RTH` variants; it sets
`canonical_instrument_id`.

Reads / consumers: `GET /instruments` list + `/instruments/{symbol}/summary`
(router prefix `/instruments`, `app/api/instruments.py`; summary exposes
`canonical_symbol`). `is_tradable=TRUE` is a hard eligibility gate in
`compute_rankings` (`app/services/scoring.py`: `WHERE i.is_tradable = TRUE`
+ coverage `filings_status='analysable'` + ≥1 of thesis / fundamentals_snapshot
/ price_daily); also consumed by portfolio + coverage.

## Invariants

- **Provider boundary** (settled "General engineering decisions → Provider
  boundary"): keep domain logic out of the provider; the provider persists its
  raw payload before `sync_universe` runs.
- **Identifier strategy** (settled "Identifier strategy"): provider-native ids
  live in `external_identifiers`, resolved service-side before provider calls;
  `symbol` is NOT the universal filing key (SEC uses CIK). Never fuzzy-resolve
  tickers on the normal path.
- **CIK = entity, CUSIP = security** (settled "CIK = entity, CUSIP = security
  (#1102)"): share-class siblings (GOOG/GOOGL, BRK.A/BRK.B) legitimately share
  an issuer CIK — distinct securities, distinct `instruments` rows; MUST NOT be
  collapsed.
- **Canonical-instrument redirect** (settled "Canonical-instrument redirect
  (#819)"): `.RTH`-style operational duplicates are the same security → linked
  via `canonical_instrument_id` (NULL = this row is canonical). Scope is
  operational duplicates ONLY — never share-class siblings.
- Deactivate, never delete: `is_tradable=FALSE` retains the row + history.
- `is_tradable=TRUE` confirmed by CURRENT eToro data is the only route into the
  scored/tradable set (long-only v1, deterministic execution) — nothing tradable
  that eToro does not currently confirm.

## Failure conditions

- **Provider returns zero instruments** → skip the deactivation sweep, log a
  warning, return `SyncSummary(0,0,0)`. An empty feed is treated as missing
  critical source data, never as "everything delisted" — the universe is never
  wiped on a transient API error.
- Missing critical source data, stale `last_seen_at` beyond threshold, or
  contradictory metadata must surface as explicit signals (skip + warn, the
  deactivation guard, preserve-prior-value branches) — never papered over with a
  neutral default or a silent full-universe rewrite.
