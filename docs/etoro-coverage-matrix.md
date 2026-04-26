# eToro instruments-endpoint field coverage matrix

Date: 2026-04-26
Source: live `/api/v1/market-data/instruments` capture against `settings.etoro_env` demo creds.
Samples: `docs/research/etoro-instrument-samples/{tag}_{exchange_id}_{symbol}.json`.

This matrix is workstream 1's deliverable for [`#515`](https://github.com/Luke-Bradford/eBull/pull/515) PR 1. It compares fields eToro returns against what eBull's universe ingest currently captures in the `instruments` table.

## Captured samples

| Tag | Symbol | Exchange ID | Description |
|---|---|---|---|
| us_equity_stock | AAPL | 4 | Nasdaq stock |
| us_equity_etf | ARKK | 20 | CBOE ETF |
| crypto | BTC | 8 | Digital Currency |
| uk_equity | BARC.L | 7 | LSE listing |
| eu_equity_de | 0B2.DE | 6 | Frankfurt listing |
| asia_equity_tyo | 7203.T | 56 | Tokyo Stock Exchange (Toyota) |

Plus three lookup-catalogue captures:

- `lookup_instrument-types.json` — `{instrumentTypes: [{instrumentTypeID, instrumentTypeDescription}]}`
- `lookup_stocks-industries.json` — `{stocksIndustries: [{industryID, industryName}]}`
- `lookup_exchanges.json` — `{exchangeInfo: [{exchangeID, exchangeDescription}]}`

## Field × asset class matrix

Legend:

- ✅ persisted — field captured by `_normalise_instrument` and written to the `instruments` table.
- 📦 available, dropped — eToro returns it on at least one asset class but eBull doesn't capture it. Cell text states the impact.
- ⊘ not returned — field absent on this capture.

| Field | us_equity (Stock) | us_equity (ETF) | crypto | uk_equity | eu_equity | asia_equity | Notes |
|---|---|---|---|---|---|---|---|
| `instrumentID` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | PK |
| `symbolFull` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | |
| `instrumentDisplayName` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | `instruments.company_name` |
| `exchangeID` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | `instruments.exchange` |
| `stocksIndustryID` | ✅ | ✅ | ⊘ | ✅ | ✅ | ✅ | Captured as `instruments.sector` (raw int). PR 1 adds `etoro_stocks_industries` lookup so frontend can render the name. |
| `instrumentTypeID` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Captured in `instruments.instrument_type_id` (#515 PR 1). The numeric FK joins to the new `etoro_instrument_types` lookup so the frontend renders the description. Note: the eToro instruments endpoint does NOT return `instrumentTypeName` despite migration 068's docstring suggesting otherwise — every sample under `docs/research/etoro-instrument-samples/` confirms only the int is present. The text `instrument_type` column from migration 068 stays NULL across the universe; `instrument_type_id` + the lookup join is the canonical path. |
| `priceSource` | 📦 | 📦 | 📦 | 📦 | 📦 | 📦 | Free-text source venue ("Nasdaq", "LSE-Vendor"). Operator-page-relevant: tells the trader where the displayed price actually comes from. Defer to a follow-up — not blocking spec workstream 2. |
| `hasExpirationDate` | 📦 | 📦 | 📦 | 📦 | 📦 | 📦 | Boolean. Always `false` in v1 captures (no futures/options in eBull yet). Capture when eBull starts trading dated instruments. |
| `isInternalInstrument` | ✅* | ✅* | ✅* | ✅* | ✅* | ✅* | * Used as a **filter** during normalisation (skip-if-true), not persisted as a column. |
| `images` | 📦 | 📦 | 📦 | 📦 | 📦 | 📦 | List of logo URLs at multiple resolutions. Frontend renders no instrument logo today; defer until UI design calls for it. |
| `instrumentTypeSubCategoryID` | ⊘ | ⊘ | ✅* | ⊘ | ⊘ | ⊘ | * Crypto-only subcategory id (e.g. token type). Sample shows int. Defer until a use case surfaces. |

## Lookup catalogues (PR 1 ships ingest)

Both endpoints land in dedicated reference tables via `app/services/etoro_lookups.py`:

| Endpoint | Table | Sample row |
|---|---|---|
| `/api/v1/market-data/instrument-types` | `etoro_instrument_types(instrument_type_id, description)` | `(5, "Stocks")` |
| `/api/v1/market-data/stocks-industries` | `etoro_stocks_industries(industry_id, name)` | `(5, "Healthcare")` |

Frontend renders the human-readable label by joining on these tables instead of showing a raw integer.

## Audit gaps to file separately

Each "📦 available, dropped" row above has its own follow-up question for the operator. None block PR 2 (workstream 2 keys on `exchange_id`, not on these fields), so we file them as separate tickets after PR 1 lands rather than expanding PR 1's scope:

- `priceSource` — capture as `instruments.price_source` so the page can label "via LSE-Vendor".
- `images` — defer until visual design calls for instrument logos.
- `instrumentTypeSubCategoryID` — defer until a crypto-token-type use case surfaces.
- `hasExpirationDate` + `expirationDate` — capture once eBull adds dated-instrument support.

## Acceptance

- [x] Lookup-table migration (070) ships with PR 1.
- [x] Provider methods + normalisers + service ship with PR 1.
- [x] Weekly cron `etoro_lookups_refresh` registered (Sunday 04:30 UTC).
- [x] Sample fixtures committed under `docs/research/etoro-instrument-samples/`.
- [x] Field-coverage matrix above operator-reviewable.
- [ ] Operator sign-off on which "📦 available, dropped" rows to promote to "must persist" (separate tickets, out of PR 1 scope).
