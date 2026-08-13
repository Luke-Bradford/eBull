# Instruments list + summary: day-change column (#1924 part 2)

## Problem
Instruments list has no day-change. Per-instrument summary nulls `day_change`
(`app/api/instruments.py:3756`, stale placeholder comment). #1924 part 1
(all-dashes treatment) shipped in PR #1929.

## Source rule
Day-change = close-to-close **fractional** change (e.g. `-0.015` = −1.5%, NOT
`-1.5`) between an instrument's two most recent **strictly-positive** closes in
`price_daily`.
- `price_daily` PK `(instrument_id, price_date)`; `close` is the native-currency
  session close, stamped to `MAX(price_date)` per settled-decisions.md:767-768
  ("latest closed session", data-anchored not wall-clock).
- **Strictly-positive filter (full-pop verified, not extrapolated):** dev DB has
  **137 real `close = 0` rows** in `price_daily` — a persisted zero is a
  non-trade sentinel, not a price (the same eBull cross-surface invariant that
  prevention-log #1428 documents for `quotes.last = 0.00`; here re-verified
  directly against `price_daily`, not carried over by assumption). The window
  ranks over `close > 0` only, so a zero day is skipped, never a −100% artefact.
- **Two-close invariant:** the change is over an instrument's two most-recent
  *valid* (positive) closes — normally two adjacent trading sessions. Full-pop:
  4,628/5,196 are ≤1 calendar day apart, 564 are 2–4 days (weekends/holidays),
  only **4** span >4 days (a data hole). No gap-guard warranted; the as-of date
  is the honesty mechanism.
- **Full-population verification (dev DB, 2026-07-04):** **5,196** instruments
  have ≥2 positive closes (vs `quotes` ~69). Staleness of the latest close is
  severe and widespread in the loop (eToro market-data unreachable →
  `daily_candle_refresh` under-writes): buckets by `CURRENT_DATE − latest_close`
  = {≤1d: 6, 2–3d: 721, 4–7d: 9, 8–30d: 4,225, >30d: 235, max 1,870d}. This is
  exactly why the metric is **stamped with its close date** — an operator reads
  "as of 12 Jun" and judges freshness; in production (fresh candles) the ≤1d
  bucket dominates and it reads as a normal day-change. No hard staleness
  suppression: an arbitrary cut would blank the feature in this known-broken
  market-data env and misrepresent production; the date label is the settled
  (line-767) as-of convention.

## Not touched (boundary)
- **#1857 (operator-gated):** the value sub-score gate is the *scoring model*
  (`instrument_valuation` view COALESCE → `model_version` bump). This change is a
  pure display read path — no scoring, no `model_version`, no migration.
- **#1906 (native price primary):** the list/summary displayed price stays
  quote-sourced. Day-change is a separate `price_daily` metric; the quote-sourced
  "Last price" column is unchanged. Where a row has no live quote, price still
  shows "—" while day-change renders — the as-of close date keeps this honest.
  A quote→price_daily *display* fallback is deliberately left to #1857 de-gating.

## Design
Shared helper in `app/services/market_data.py`:
- `compute_day_change(last_close, prior_close) -> Decimal | None` — pure;
  `(last-prior)/prior`, `None` when `prior <= 0`.
- `load_day_changes(conn, instrument_ids) -> dict[int, DayChange]` — one batched
  window query (rn≤2 over positive closes), builds `DayChange(as_of, last_close,
  prior_close, change_abs, change_pct)` only where both closes present.

Wiring:
- **List** (`list_instruments`): batch-load for the page's instrument_ids; add
  `day_change_pct: Decimal | None`, `day_change_as_of: date | None` to
  `InstrumentListItem`.
- **Summary** (`get_instrument_summary`): populate `InstrumentPrice.day_change` /
  `day_change_pct` from the helper; add `day_change_as_of: date | None`.
  **Invariant fix (Codex HIGH):** `price_block` currently is `None` when there is
  no quote-derived `current_price`, so day-change would silently vanish on the
  detail page for the ~5k quote-less names while the list shows it —
  list↔detail divergence. Build `InstrumentPrice(current=None, day_change=…,
  day_change_pct=…, day_change_as_of=…)` when day-change is present even if the
  quote price is absent, so both surfaces agree. `SummaryStrip` already renders
  its price/change row whenever `price` is truthy and shows "—" for a null
  price.

Frontend:
- `types.ts`: mirror new fields (`string | null` — dates serialize as ISO
  strings; `day_change_pct` is a fraction, fed straight to `formatPct`). Update
  existing `InstrumentPrice` / `InstrumentListItem` test fixtures
  (`SummaryStrip.test.tsx`, `InstrumentsPage.test.tsx`, `ChartPage.test.tsx`) —
  additive optional fields, so fixtures without them still typecheck, but the
  new column/label assertions need fixtures that set them.
- `InstrumentsPage`: new "Day change" column — colored pct + muted as-of date,
  sortable (client-side, mirrors `last`); "—" when unavailable.
- `SummaryStrip`: render the as-of close date next to the existing day-change so
  it is never misread as "today".

## Tests
- Pure `compute_day_change`: positive/negative change, `prior<=0 → None`, zero
  change. Table-test (no DB).
- One db-tier test for `load_day_changes` (skips <2 closes, ignores non-positive,
  picks two most recent).

## Labeling decision (was operator-open)
Rendered as a day-change **stamped with its close date** (e.g. "as of 12 Jun"),
matching the line-767 as-of convention. Decisive per autonomy mandate: honest
under the loop's staleness, normal day-change in production.
