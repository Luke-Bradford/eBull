# Resolve eToro industry id → name at the remaining display read sites (#1599)

## Problem (verified live, dev 2026-06-28)
`#1598` fixed the eToro mapper casing, so `instruments.sector` now populates with
eToro's **numeric** industry id as text. Full-population dev query:
```sql
SELECT count(*) total, count(sector) with_sector, count(DISTINCT sector) FROM instruments;
-- total=12598, with_sector=9271, distinct=9
SELECT sector, count(*) FROM instruments WHERE sector IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;
-- distinct values are exactly '1'..'9'
```
`etoro_stocks_industries` holds ids 1–9 all named, so every populated id resolves
cleanly — no "Unknown" leakage on the current population.

Two read sites still surface the **raw id** to the operator (FE renders it):
- `app/api/watchlist.py` — `WatchlistItem.sector` selects `i.sector` verbatim; FE
  `WatchlistPanel.tsx:56` renders `{item.sector ?? "—"}` → operator sees **"8"**.
- `app/api/instruments.py` `InstrumentIdentity.sector` — FE `SummaryStrip.tsx:184`
  falls back `gics_sector ?? sector` → for a **non-SEC** instrument (no GICS) the
  raw id "8" shows.

Not leaks (already correct, confirmed this session):
- Instruments-list / scores items carry raw `sector` for the **deprecated** filter
  back-compat (`#1675`); FE displays `gics_sector`, never raw `sector`.
- Reports holdings (`reporting.py:1288`) already use `h.sector_name` (resolved).
- The deprecated `sector` exact-match filter is documented (`#1675`) and superseded
  by `sector_spdr` — out of scope.

## Source rule
eToro provider contract, documented at `sql/070_etoro_lookup_tables.sql:1-8`
("universe ingest stores `stocksIndustryId` as a raw integer in
`instruments.sector`") + the `etoro_stocks_industries` table comment
(`sql/070_etoro_lookup_tables.sql:49-54`: "Maps numeric industryID … to the
human-readable industry name rendered as the instrument-page sector label").
`instruments.sector` = eToro's numeric industry id (text); the id→name dictionary
is `etoro_stocks_industries (industry_id int, name text)`.
Settled resolution pattern already in `app/services/valuation.py`
(`_POSITIONS_SQL`, `HoldingValuation.sector` + `.sector_name`):
```sql
LEFT JOIN etoro_stocks_industries esi ON esi.industry_id::text = i.sector
```
Keep the raw `sector` field (provider contract); ADD a resolved `sector_name`.

## Change
1. `WatchlistItem`: add `sector_name: str | None`. Both queries (list GET, add POST)
   LEFT JOIN `etoro_stocks_industries`; populate `sector_name`. Keep `sector` raw.
2. `InstrumentIdentity`: add `sector_name: str | None`. The identity lookup SQL
   (both branches) LEFT JOIN `etoro_stocks_industries`; populate it. Keep `sector`.
3. FE types: add `sector_name: string | null` to `WatchlistItem`
   (`frontend/src/api/watchlist.ts:3`) and to the `InstrumentIdentity` type
   (`frontend/src/api/types.ts`).
4. FE `WatchlistPanel.tsx`: render `{item.sector_name ?? "—"}`.
5. FE `SummaryStrip.tsx`: fallback chain `gics_sector ?? sector_name ?? "—"`
   (drop the raw-id fallback). Keep the `(sector_spdr)` suffix logic on gics.
6. Tests:
   - watchlist API: `sector_name` resolves id → name; null when id unmapped/NULL.
   - instruments identity API: `sector_name` populated on BOTH lookup branches
     (`instrument_id` path + symbol-only path), incl. unmapped id → null.
   - FE component tests render the name, not the id.

`sector_name` is `None` when `sector` is NULL or the id has no dictionary row —
display falls back to "—" (safe; not a hard-rule check). Nullable, no default, to
mirror TS `string | null` exactly (prevention-log identity-nullable convention).

## Out of scope (follow-up)
- `app/services/portfolio.py` BUY/ADD guard reason strings emit `sector {id!r}`
  ("sector '8' would reach…"). Grouping math is correct (id-keyed); only the
  human string leaks. Guard-internal cosmetic → focused follow-up ticket, not this
  read-path display PR.

## Verification
- Unit: watchlist resolution test; FE component tests.
- Dev endpoint: GET `/watchlist` + GET `/instruments/<sym>` (a non-SEC instrument)
  and confirm `sector_name` renders the eToro name, raw `sector` unchanged.
- No schema change, no backfill, no daemon restart (read-path only).
