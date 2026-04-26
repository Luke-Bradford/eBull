# Complete-coverage spec — every asset, every exchange, capability-driven UI

Date: 2026-04-26
Author: Luke / Claude
Status: Draft v3.9 (post-Codex round 11; pending operator sign-off)

## Goal

Every tradable instrument on eToro renders a complete, asset-class-
appropriate view in eBull. No "no price data" placeholders on a real
ticker; no "filings" tab on a crypto coin; no missing dividend
history on an LSE blue-chip. The exchange the operator already curates
in the `exchanges` table drives both *what* we ingest and *how* the
frontend presents it.

Three problems to solve in lockstep:

1. **eToro field audit.** The universe ingest captures only a
   subset of what eToro returns. `instrumentTypeID`, stocks-industries
   lookup, and instrument-types lookup are all "Not used — IDs
   stored raw" per `docs/etoro-api-reference.md:130-132`, so the
   frontend renders numeric ids where Bloomberg would render
   "Pharmaceuticals — biotech R&D". Separately from this audit, BTC
   and LRC instrument pages render "no price data" — that is a
   *candle-ingest / price-API-handler* bug (PR 0 below), NOT a
   universe-field-audit downstream.
2. **Per-region data sources.** US equities have SEC EDGAR coverage
   end-to-end. UK / EU / Asia / MENA / crypto / FX / commodity /
   index have nothing equivalent yet. We need to know what's
   available, what's free, and what each looks like compared to
   Bloomberg's per-ticker coverage so the operator can make an
   informed build vs. defer call per region.
3. **UI capability flags.** The instrument page already gates two
   tabs (`has_sec_cik`, `has_filings_coverage`) post-#506. We need
   a generalised capability matrix so a crypto page never offers a
   "Filings" tab, an ASX page shows "Announcements" not "8-K", and
   an FX pair page shows TA + macro only.

## Why

Operator vision (verbatim):

> Always go back to, what would a Bloomberg terminal do, what
> information helps a trader know about an asset and make the needed
> call on a good investment. What do other solutions do like this is
> trying to be.

eBull is positioning as an autonomous-pocket-hedge-fund (#198 vision
memo). A hedge fund's analyst doesn't accept a half-populated security
master — every position carries a complete dossier of price,
fundamentals, news, regulatory disclosures, ownership, and analyst
view appropriate to its venue. eBull's thesis engine downstream is
only as good as the universe data it's reasoning over.

Today's gaps observable in the dev DB:

- BTC / LRC instrument pages render "no price data" — candle ingest
  has an unintentional gate that excludes non-US-classified exchanges
  from `daily_candle_refresh` (a hypothesis to verify in PR 0).
- 30+ exchanges classified by #503 PR 4's auto-classifier, but the
  scheduler / API / frontend only treat `us_equity` rows as
  first-class. Every other classification is a label without a
  consumer.
- The eToro instruments endpoint returns `stocksIndustryId`,
  `instrumentTypeID`, `priceSource`, plus several other fields not
  documented in this audit's scope. Currency is enriched separately
  via FMP. Industry name (`stocks-industries` lookup) is never
  resolved — frontend shows a numeric id where Bloomberg would show
  "Pharmaceuticals — biotech R&D".

## Non-goals (locked)

- **Implementing per-region data sources.** This spec files
  investigation tickets only. Concrete UK/EU/Asia/MENA integrations
  land in their own PRs once the operator has signed off on which
  source per region. No new HTTP clients in the PR sequence below.
- **Operator-curated `country` / `asset_class` columns.** The
  classifier from #503 PR 4 stays the source of truth. This spec
  consumes it; it doesn't second-guess it.
- **News providers.** Tracked separately under #198. Out of scope.
- **Bloomberg API, Refinitiv API, FactSet API.** Paid feeds. Spec
  benchmarks against them; doesn't propose integrating them.
- **Changing the eToro WebSocket live-tick pipeline.** That work
  shipped in #274 / #498 / #501. Live ticks already cover every
  visible instrument regardless of asset class.

## Architecture invariants (post-spec)

1. **Exchange row is the capability *defaults* oracle.** Every code
   path that needs to ask "should this instrument's venue support
   filings? dividends? ownership data?" reads
   `exchanges.capabilities` (a new JSONB column added in PR 3, keyed
   by exchange_id — NOT by asset_class). One row per eToro
   exchange_id (HKEX vs ASX vs TDnet are distinct rows), so the
   asia_equity / mena_equity / eu_equity wide buckets do NOT collapse
   their distinct sources into a single default. The classifier from
   #503 PR 4 fed asset_class; capabilities are a finer per-exchange
   layer above it.
2. **Per-instrument capability resolution = exchange default ∪
   per-instrument identifier facts.** A given instrument's effective
   capability set is the union of (a) its exchange row's
   `capabilities` AND (b) any extra coverage signalled by
   `external_identifiers` rows on that instrument (e.g. an LSE-listed
   Chinese ADR with both a Companies House number AND a SEC CIK
   resolves to filings sourced from both). The resolution function
   lives in one helper at API summary time —
   `resolve_capabilities(exchange_id, instrument_id) -> Capabilities`
   — so frontend / thesis-engine consumers see one shape regardless
   of which way the data was discovered. The wire format below
   carries `providers: list[CapabilityProvider]` (NOT a scalar
   `provider`) per capability so multi-source coverage is
   first-class.
3. **eToro is the universe + price source. Period.** Every instrument
   we render is one eToro returns. No yfinance fallback (#500
   retired it), no synthetic instruments. Other data sources *enrich*
   an eToro row; they never *gate* its existence.
4. **Operator-decision ≠ implementation-status ≠ data-present.**
   Three distinct concerns, each with one canonical encoding:
   - **`providers: list[CapabilityProvider]`** carries the
     operator's *decided source list* — set during PR 2 sign-off,
     possibly before any of those sources are wired. Empty list =
     "no source picked".
   - **Implementation status** (whether a provider is wired) is
     NOT a separate field. The signal is `data_present[provider]`
     below: a provider that's been wired and has ingested at least
     one row reports `true`; a provider in the operator's
     decision list that hasn't been wired yet (or has been wired
     but has no rows for this instrument) reports `false`.
   - **`data_present: dict[CapabilityProvider, bool]`** carries
     per-provider data-presence. Keyed identically to `providers`.
     A multi-source venue (LSE-listed ADR with Companies House +
     SEC) has `data_present: {"companies_house": true,
     "sec_edgar": false}` when only one source has landed rows.
   Render rule below uses the AND of "any provider in the list"
   AND "at least one of those providers has data_present=true".
5. **Frontend never queries source tables directly.** Per-source
   panels (filings, fundamentals, dividends, …) gate on
   `providers` + `data_present` returned in the instrument
   summary, not on `EXISTS (SELECT 1 FROM filing_events …)` joins
   from the frontend.

## Bloomberg / competitor reference matrix (research input)

Before writing the per-region matrix, the spec needs a grounded
benchmark for "what should an instrument page show." This list is
the research input — operator and Codex review BEFORE we commit to
any specific data type as in-scope.

**Bloomberg Terminal per-ticker functions (representative subset):**

| Function | What it shows | eBull current state |
|----------|---------------|---------------------|
| DES | Security description, exchange, sector, business summary | partial — SEC business summary US-only |
| FA | Fundamentals (IS, BS, CF + ratios) | partial — FMP for US only |
| EQS | Equity screener (cross-instrument) | not built |
| RV | Relative valuation vs peers | not built |
| GIP | Intraday + historical price chart | partial — eToro candles, has the BTC/LRC gap |
| DVD | Dividend history + forward calendar | partial — SEC 8-K 8.01 ingest US-only |
| ANR | Analyst recommendations / target price | partial — FMP US analyst rec ingested but barely surfaced |
| ESG | ESG scores + controversies | not built |
| OWN | Major holders, insider holdings | partial — SEC Form 4 US-only |
| OPT | Options chain | not built |
| HP | Historical prices in tabular form | partial — same as GIP |
| EVTS | Corporate event calendar | not built (future #198 thesis driver) |
| RELS | Related securities (ADR ↔ ordinary, share class) | not built |
| MGMT | Officers + directors | not built |

**Free / freemium competitor coverage benchmark:**

| Tool | Strong on | Weak on | Free? |
|------|-----------|---------|-------|
| Yahoo Finance | UK/EU/Asia equity prices + headline fundamentals | Form 4 / insider depth | Yes |
| Stockanalysis.com | US fundamentals UI quality | Non-US | Yes |
| TIKR Terminal | 10-yr fundamentals US/intl | Asia depth | Freemium |
| Simply Wall St | Visual snapshots, Bloomberg-DES-like | Real-time, depth | Freemium |
| Koyfin | Intl coverage, charts | Form 4 / insider | Freemium |
| CoinGecko | Crypto: market cap, on-chain, exchange listings | Equities | Yes |
| OpenCorporates | Cross-border company registry | No financials | Free with limits |

**Goal of this matrix:** for each non-US `exchange_id` row in the
`exchanges` table, identify the free public source that delivers
the best Bloomberg-DES / FA / DVD equivalent. Operator picks one
provider set per exchange_id as the v1 enrichment target; the rest
are future PRs.

## Workstreams

### Workstream 1 — eToro field audit

Goal: a per-asset-class matrix of every field eToro returns vs. every
field eBull persists, with explicit gap reasons.

**Deliverable:** `docs/etoro-coverage-matrix.md` — matrix doc with:

- Rows: each field name as eToro names it (`instrumentID`,
  `symbolFull`, `instrumentDisplayName`, `exchangeID`,
  `instrumentTypeID`, `instrumentTypeName`, `stocksIndustryId`,
  `priceSource`, `isInternalInstrument`, `hasExpirationDate`,
  `expirationDate`, `holdingsLeverage`, `tradeOpenLeverage`,
  `tradePnLLeverage`, `precision`, `isActive`, … exhaustive list
  from one live API capture per asset class).
- Columns: each `asset_class` value (`us_equity`, `eu_equity`, …).
- Cell: one of:
  - ✅ **persisted**: eToro provides + eBull writes a column for it.
  - 📦 **available, dropped**: eToro provides but eBull doesn't
    capture. Cell text states the impact (e.g. "industry name
    rendered as numeric id on instrument page").
  - ⊘ **N/A for this asset class**: eToro doesn't provide it (e.g.
    `tradeOpenLeverage` not meaningful for crypto).

**Method:**

1. Live API capture per `instrument_type` value (NOT per `asset_class`),
   since field presence varies inside an asset class — `us_equity`
   lumps Stocks + ETFs which expose different fields. Capture set:
   one Stock, one ETF, one Crypto, one Index, one Currency, one
   Commodity, one ADR, one leveraged-product (if eToro tags any).
2. Per-exchange equity capture in addition: pull one sample from
   each active exchange_id (BARC.L for LSE, 0700.HK for HKEX,
   7203.T for TSE, BHP.AX for ASX, AAPL for NASDAQ, …) — different
   exchanges may decorate instruments with different fields
   (e.g. ISIN, MIC code).
3. Save raw JSON snapshots to `docs/research/etoro-instrument-samples/`
   one file per capture, named `{instrument_type}_{exchange_id}_{symbol}.json`,
   so the audit is reproducible without re-hitting eToro.
4. Diff against `_normalise_instrument` field set.
5. **Implementation PRs split out separately** for any "📦 available,
   dropped" rows the operator promotes to "must persist" — those are
   their own tickets, not part of this spec's PR sequence.
6. **Lookup-endpoint ingest scoped here:** PR 1 of this spec adds the
   lookup tables for `instrument-types` + `stocks-industries`
   ("Not used — IDs stored raw" today per
   `docs/etoro-api-reference.md:131-132`). Currently the universe
   ingest stores numeric ids that the frontend can't render
   meaningfully. Migration + small ingest service in PR 1.

**Exit criteria:** matrix is complete, operator signs off on the
"📦 available, dropped" rows that should become "must persist" before
we ship per-region UI.

### Workstream 2 — Per-exchange_id data capability matrix

Goal: per `exchange_id` row in the dev DB (51 rows post-#513),
identify the canonical source for each data type a Bloomberg-grade
page would show, and what we'd see on a free or freemium tier.

**Granularity is per-`exchange_id`, NOT per-`asset_class`.** HKEX
(`21`) and Tokyo Stock Exchange (`13` / `56`) and Sydney (`31`) are
all `asia_equity` (post-#514 reclassification) but their data
sources are entirely different and so are their
mapping primitives (HKEX stock code vs. JP-securities code vs. ASX
ticker). PR 3 seeds defaults onto `exchanges.capabilities` keyed by
exchange_id; the operator overrides per row.

**Deliverable:** `docs/per-exchange-capability-matrix.md` — matrix with:

- Rows: each data type — capped at the v1 set of 11 capabilities:
  - `filings` — corporate filings index
  - `fundamentals` — IS / BS / CF + ratios
  - `dividends` — dividend history + forward calendar
  - `insider` — corporate-insider transactions (officers /
    directors / 10%+ holders; SEC source = Form 4)
  - `analyst` — analyst recommendations / target prices
  - `ratings` — credit / quality ratings (separate from analyst)
  - `esg` — ESG scores + controversies
  - `ownership` — institutional + 5%+ beneficial owners
    (distinct from `insider`; SEC source = 13F-HR / 13D / 13G,
    NOT Form 4)
  - `corporate_events` — calendar of structured events (8-K-class)
  - `business_summary` — narrative description (10-K Item 1 class)
  - `officers` — directors + officers list
  **News is excluded** — tracked separately under #198. **Options
  chain** and **short interest** are explicitly deferred to a
  follow-up spec; they don't appear in the v1 capability set, the
  `CAPABILITY_PROVIDERS` enum, the JSONB schema keys, or the
  summary endpoint.
- Columns: each `exchange_id` row currently in the `exchanges`
  table (51 today, exact list from `SELECT exchange_id, description
  FROM exchanges ORDER BY exchange_id::int`).
- Cell:
  - **Provider tag** (one of the `CAPABILITY_PROVIDERS` enum values
    defined in workstream 3, or empty list).
  - **URL pattern** (e.g.
    `https://api.companieshouse.gov.uk/company/{number}/filing-history`).
  - **Auth** (free / API key required / login required).
  - **Coverage depth** (full vs. headline-only).
  - **Refresh cadence** of the source (daily / event-driven).
  - **Mapping primitive** — what eBull needs to know per instrument
    to query the source (Companies House number, ISIN, HKEX stock
    code, …). Captured under workstream 1 if `instruments` already
    carries the field via eToro; otherwise files an enrichment
    ticket separate from this spec's PR sequence.
  - **(implicit) Decision relevance:** the operator's "is this data
    type useful for the thesis engine on this venue?" judgment is
    encoded by the provider list itself, NOT a separate flag. An
    empty provider list means "no source picked" — that includes
    both "no source available" AND "available but not
    decision-relevant on this venue, defer". The matrix doc body
    can carry a free-text rationale per cell. The single canonical
    render rule is in workstream 3 (`providers.length > 0 AND any
    data_present[provider] is true`); workstream 2's role is only
    to populate the `providers` list per cell. This avoids a third
    state the `CAPABILITY_PROVIDERS` enum doesn't model.

**Granularity contract — one rule, applied everywhere:**

| Artefact | Granularity |
|----------|-------------|
| Matrix rows | per-data-type (filings, fundamentals, …) |
| Matrix columns | per-`exchange_id` row in `exchanges` table |
| Sample JSON capture | one file per `exchange_id` per source |
| Investigation tickets | one ticket per region (a region groups all `exchange_id`s sharing a regulator / data landscape) |
| Operator sign-off | per-`exchange_id` provider list — operator picks the providers list for each row |

The ticket level is REGION because matrix research is mostly portal
scanning — fragmenting "investigate HKEX vs ASX" into two tickets
adds tracking overhead without buying parallelism. The investigation
*outcome* is per-`exchange_id` (one matrix column), but the
investigation *unit* is regional.

**Regions and the exchange_ids each ticket covers:**

| Region (one ticket each) | exchange_ids in scope (live `exchanges` table) | Source candidates to evaluate |
|--------------------------|----------------------------------------------|-------------------------------|
| US | `4` (Nasdaq), `5` (NYSE), `19` (OTC Markets), `20` (CBOE), `33` (Regular Trading Hours) | Already covered by SEC EDGAR + FMP — document only, no investigation |
| _(matrix-only row, no separate ticket)_ | `38` (Xetra ETFs) — investigated as part of the EU ticket; listed here as its own matrix row so PR 2 cannot accidentally drop it from the per-`exchange_id` enumeration | Same as EU row above |
| UK | `7` (LSE), `42` (LSE_AIM), `43` (LSE AIM Auction), `44` (LSE Auction) | Companies House + LSE RNS |
| EU | `6` (FRA), `9` (Euronext Paris), `10` (Bolsa De Madrid), `11` (Borsa Italiana), `12` (SIX Switzerland), `14` (Oslo), `15` (Stockholm), `16` (Copenhagen), `17` (Helsinki), `22` (Euronext Lisbon), `23` (Euronext Brussels), `30` (Euronext Amsterdam), `32` (Vienna), `34` (Dublin EN), `35` (Prague), `36` (Warsaw), `37` (Budapest), `50` (Nasdaq Iceland), `51` (Nasdaq Tallinn), `52` (Nasdaq Vilnius), `53` (Nasdaq Riga) | ESMA + national regulators (BaFin, AMF, CONSOB, …) — one ticket investigates the matrix for ALL EU venues |
| Asia | `13` (TYO), `21` (Hong Kong Exchanges), `31` (Sydney), `45` (Shenzhen), `46` (Shanghai), `47` (NSE India), `49` (Singapore), `54` (Korea Exchange), `55` (Taiwan SE), `56` (Tokyo Stock Exchange — distinct from `13`) | HKEX disclosure, TDnet/EDINET (Japan), ASX announcements (AU), KRX/KIND (Korea), TWSE/MOPS (Taiwan), SSE/SZSE (China — Shanghai/Shenzhen), NSE/BSE India, SGX (Singapore) — one ticket investigates ALL Asian venues' matrices |
| MENA | `24` (Tadawul / Saudi), `39` (Dubai Financial Market), `41` (Abu Dhabi) | Tadawul + ADX + DFM |
| Crypto | `8` (Digital Currency) | CoinGecko + on-chain (Glassnode free tier) |
| Commodity | `2` (post-#514 reclassification), `40` (CME) | CME + LME |
| FX | `1` (post-#514 reclassification) | ECB / Fed / BoE — most already covered by Frankfurter (#275) |
| Canada | `18` (Toronto), `48` (TSX Venture) | TMX Group + SEDAR+ — one ticket covering both Canadian venues |
| CFD | `3` (post-#514 reclassification) | Defer — cross-asset wrapper, no separate data source |

The exact `exchange_id` list above will be verified against the
live `exchanges` table when PR 2 is opened — **#514 must merge
first** so the `asset_class` values are corrected (today's seed has
e.g. id `7` as `us_equity` but description "LSE").

**Method per ticket:**

1. Manual scan of each regulator / exchange portal in scope,
   documenting publicly available APIs.
2. Sample API call for one well-known instrument **per
   `exchange_id`** (e.g. BARC.L for LSE id 7, 0700.HK for HKEX,
   7203.T for TYO id 13, BHP.AX for ASX). Each capture is saved
   per-exchange_id even though the ticket spans a region.
3. Save raw responses as fixtures under
   `docs/research/per-exchange-samples/{exchange_id}_{provider}_{symbol}.json`
   (provider tag included so a venue with multiple candidate sources
   doesn't collide on a single filename).
4. Populate one matrix column per `exchange_id` covered by this
   ticket.
5. Flag rate limits, auth complexity, terms-of-service constraints.

**Exit criteria:** every `exchange_id` in the dev DB has a
provider-list value picked by the operator (possibly empty,
possibly multi-source) for each capability row. Each non-empty
provider that isn't already wired becomes its own implementation
PR (out of scope for this spec).

### Workstream 3 — UI capability flags

Goal: the instrument page renders only the panels the instrument's
exchange row supports (per `exchanges.capabilities` resolved
through the union with `external_identifiers` per invariant 2),
with content sourced from the matrix in workstream 2.

**Deliverable: schema + endpoint + frontend changes.**

Schema:

```sql
ALTER TABLE exchanges ADD COLUMN capabilities JSONB NOT NULL DEFAULT '{}';
-- shape (controlled-vocabulary values, see CAPABILITY_PROVIDERS below).
-- Each capability key maps to a *list* of provider tags so a venue
-- with multiple sources for one capability is first-class:
--   {"filings":["sec_xbrl"],
--    "fundamentals":["sec_xbrl", "fmp"],
--    "dividends":["sec_8k_item_801"],
--    "insider":["sec_form4"],
--    "analyst":["fmp"],
--    "ratings":[],
--    "esg":[],
--    "ownership":["sec_13f", "sec_13d_13g"],
--    "corporate_events":["sec_8k_events"],
--    "business_summary":["sec_10k_item1"],
--    "officers":[]}
-- v1 = 11 keys. ownership is sourced from 13F/13D/13G (NOT Form 4 —
-- Form 4 is insider transactions, a different capability). options
-- + short_interest are deferred (see workstream 2 row spec).
-- An empty list means "no source picked" — covers BOTH "no public
-- source available on this venue" AND "available but not
-- decision-relevant on this venue, operator deferred". The matrix
-- doc body carries the free-text rationale per cell. Frontend
-- treats empty list as "panel hidden" regardless of which
-- sub-reason applies (#503 PR 4 follow-up — see CAPABILITY_PROVIDERS
-- comment below).
```

**Capability provider enum** (Python `Literal` + DB CHECK constraint
on the JSON values, prevents free-text drift the frontend would
have to typo-match):

```python
# CAPABILITY_PROVIDERS values map 1:1 to existing
# external_identifiers.provider tags AND to the source-attribution
# tags already present in summary provenance. The mapping is
# explicit (not invented) so data_present[provider] can join to
# existing SEC tables without a translation layer.
#
# SEC family naming is intentionally split: the existing
# `external_identifiers.provider='sec'` row stays canonical for
# CIK linkage, but per-data-type tags (`sec_xbrl`,
# `sec_dividend_summary`, etc.) are surfaced separately because
# they correspond to different SEC ingest jobs and different
# tables. PR 3 is responsible for confirming the exact tag list
# against current provenance strings before locking the enum.
CAPABILITY_PROVIDERS = Literal[
    # US — SEC family. Tags align with existing provenance strings
    # in app/api/instruments.py (sec_xbrl, sec_dividend_summary,
    # …). PR 3 audits the full set during the schema migration.
    "sec_xbrl",            # 10-K/Q XBRL → financial_periods (fundamentals)
    "sec_8k_item_801",     # 8-K Item 8.01 → dividend_events
    "sec_8k_events",       # 8-K structured events → eight_k_filings
    "sec_10k_item1",       # 10-K Item 1 → instrument_business_summary
    "sec_form4",           # Form 4 → insider_transactions (insider; NOT ownership)
    "sec_13f",             # 13F-HR → ownership (institutional holdings)
    "sec_13d_13g",         # 13D/G → ownership (>5% beneficial owners)
    # US — non-SEC enrichment
    "fmp",                 # Fundamentals + analyst estimates / ratings (non-SEC)
    # UK
    "companies_house", "lse_rns",
    # EU
    "esma", "bafin", "amf", "consob",
    # Asia
    "hkex", "tdnet", "edinet", "asx",
    "krx", "kind",         # Korea: KRX disclosure + KIND filings portal
    "twse", "mops",        # Taiwan: TWSE + MOPS (Market Observation Post)
    "sse", "szse",         # China: Shanghai + Shenzhen exchange portals
    "nse_india", "bse_india",  # India: NSE + BSE
    "sgx",                 # Singapore Exchange disclosures
    # MENA
    "tadawul", "adx", "dfm",
    # Crypto
    "coingecko", "glassnode",
    # Commodity / FX
    "cme", "lme", "ecb", "fed", "boe",
    # Canada
    "tmx_group", "sedar_plus",
]
# Insider vs ownership: `sec_form4` covers ONLY transactions by
# corporate insiders (officers/directors/10%+ shareholders).
# Major-holder / institutional ownership is a different SEC source
# (13F-HR / 13D / 13G) and gets its own provider tags. PR 3 wires
# both into the matrix as separate capability rows
# (`insider` ← sec_form4; `ownership` ← sec_13f, sec_13d_13g).
# Empty list (= "no source picked") is the canonical absence-of-
# provider state — covers BOTH "no public source available on this
# venue" AND "available but not decision-relevant on this venue,
# operator deferred". The matrix doc body carries the free-text
# rationale per cell so the operator review stays honest. Frontend
# treats empty list identically: panel hidden. Never use `[null]`,
# `"none"`, or any sentinel string — list emptiness is the only
# absence signal.
#
# The enum stays a closed set of real provider tags; new values
# require an explicit migration when a future PR wires a new
# source.
```

Populated per-`exchange_id` (NOT per-asset_class) via a one-shot
data migration sourced from the matrix in workstream 2. Operator
can override any row directly; PR 3 ships an admin "show overrides"
page that diffs current row state against the migration's seed so
divergence is visible.

API:

`GET /instruments/{symbol}/summary` (the existing handler at
[`app/api/instruments.py`](../../app/api/instruments.py) — current
file path; verify the line number when implementing) gains an
`exchange` block plus per-capability resolution:

```json
{
  ...,
  "exchange": {
    "id": "7",
    "country": "GB",
    "asset_class": "uk_equity",
    "description": "LSE"
  },
  "capabilities": {
    "filings":     {"providers": ["companies_house", "sec_edgar"], "data_present": {"companies_house": true, "sec_edgar": false}},
    "fundamentals":{"providers": [],                               "data_present": {}},
    "dividends":   {"providers": ["lse_rns"],                      "data_present": {"lse_rns": false}}
    // ... one entry per data type from workstream 2
  }
  // Two fields per capability, no third. `providers` is the
  // operator's decided source list; `data_present` is a dict
  // keyed identically to `providers`, one bool per provider from
  // a per-instrument SQL EXISTS check. Empty `providers` →
  // empty `data_present` (no keys to key by).
}
```

Render rule (single source of truth — applied client-side, no
server-side `supported` flag):

```ts
const present = Object.values(capability.data_present)
const visible = capability.providers.length > 0 && present.some(v => v)
// Panel header lists ONLY providers with data_present=true:
const activeProviders = capability.providers.filter(
  p => capability.data_present[p]
)
// e.g. activeProviders=["companies_house"] → header reads
// "Filings — Companies House" even though sec_edgar is in the
// operator's decision list (because SEC EDGAR ingest hasn't
// landed for this instrument yet).
```

Frontend:

- `frontend/src/components/instrument/RightRail.tsx` and
  `InstrumentPage.tsx` already gate on `has_filings_coverage`.
  Replace per-flag gates with a per-capability lookup driven by
  `summary.capabilities[type]`. The existing #506 flags become a
  *thin shim* over the new shape during the migration window — both
  read paths are wired in PR 3 so a single PR doesn't reshape every
  consumer at once.
- Tab labels per capability provider — UK shows "Filings (Companies
  House)"; ASX shows "Announcements (ASX)". Provider enum →
  human-readable label table is part of PR 3 scope.

**PR 3 explicit scope (must ship in PR 3):**

1. Schema migration adding `exchanges.capabilities` JSONB + enum
   CHECK constraint on JSON values. The CHECK lists the
   `CAPABILITY_PROVIDERS` enum values verbatim; PR 3 audits the
   actual provenance strings already in `app/api/instruments.py`
   and reconciles any drift in the same migration.
2. Data migration seeding per-exchange_id capability defaults
   from the workstream 2 matrix.
3. `resolve_capabilities()` helper + summary endpoint plumbing
   (returns `providers` + per-provider `data_present` dict per
   capability — the `data_present` keys must match existing
   `external_identifiers.provider` and per-table source-attribution
   strings so SQL EXISTS checks land truthy on existing SEC data
   without a translation layer).
4. **Frontend panel refactor — provider-agnostic.** The current
   panels (`DividendsPanel`, `InsiderActivityPanel`,
   `EightKEventsPanel` in `frontend/src/components/instrument/`,
   wired off SEC gates in `ResearchTab.tsx`) are SEC-specific by
   accident, not design. PR 3 splits each panel into a
   provider-agnostic shell + per-provider data hook, so a
   non-SEC integration in a future PR only adds a hook and a
   capability default — no panel code change. The shells render
   from a normalised shape returned by API endpoints that take
   `provider` as a query param (e.g. `GET /instruments/{symbol}/dividends?provider=sec_8k_item_801`).
5. Per-capability gating in `ResearchTab.tsx` + `RightRail.tsx`
   reads the new `summary.capabilities[type]` shape; existing
   `has_*_coverage` flags become a thin shim that resolves to
   `data_present.values().any()` for the SEC-only case during
   the migration window, then the shim is removed.
6. Provider → human-readable label table in frontend (e.g.
   `sec_8k_item_801` → "SEC 8-K Item 8.01").
7. **Admin "capability overrides" page** showing rows where the
   operator's value differs from the seed default (Codex round 1
   risk-section requirement promoted into scope).

**Exit criteria:** every instrument page on the dev DB renders a
coherent layout with no empty panels and no "no data" placeholders
where data should exist. A crypto page never shows a Filings tab; a
US page shows everything that ships post-#506.

## PRs (sequenced)

PR 0 ships immediately — the BTC/LRC bug is independent of the
audits. PRs 1–2 are research deliverables; PR 3 ships the schema +
UI plumbing once the matrices are operator-signed-off. Per-region
integrations (Companies House, HKEX, …) are out of scope here and
get their own specs.

### PR 0 — Fix BTC/LRC "no price data"

**Independent of the audits.** Investigates the candle ingest path,
not the universe-instruments-endpoint field set. Likely root cause
locations to verify (in priority order):

- `app/workers/scheduler.py` → `daily_candle_refresh` —
  asset-class / `coverage_tier` filter on the candidate query that
  excludes crypto / non-US instruments.
- T3 bootstrap path from the live-pricing spec — instruments without
  a coverage row never enter the candle ingest set.
- `app/api/instruments.py` → price-history handler — venue-class
  gate that 404s on instruments without a specific table row even
  when `price_daily` has data.

**Method:** start by checking `price_daily` directly for BTC's
instrument_id on the dev DB. If rows exist but the API returns
empty, the bug is in the API handler. If `price_daily` is empty
despite eToro returning candles for the symbol, the bug is in the
scheduler / ingest gate. This frames the fix before code changes.

PR 0 is small (likely <50 lines of code + tests). Ships before the
research PRs because operator wants the obvious bug gone first.

### PR 1 — eToro field-audit matrix + lookup ingest

Research deliverable PLUS a small code change for the
`instrument-types` / `stocks-industries` lookup tables (so this
spec's NITPICK 1 doesn't drift into a phantom future PR).

- `docs/research/etoro-instrument-samples/{instrument_type}_{exchange_id}_{symbol}.json`
  — one raw API response per `instrument_type` value AND per
  active exchange_id (see workstream 1 method).
- `docs/etoro-coverage-matrix.md` — the matrix doc.
- Migration adding `etoro_instrument_types` + `etoro_stocks_industries`
  reference tables, seeded from the lookup endpoints.
- Service `app.services.etoro_lookups.refresh_etoro_lookups` (weekly
  cron, same shape as `refresh_exchanges_metadata` from #503 PR 4).
- Frontend instrument-page resolves numeric `industry`
  / `instrument_type` to human labels via the new tables.
- Tickets filed per "📦 available, dropped" row the operator
  promotes to "must persist". Each becomes its own PR — out of scope
  here.

### PR 2 — Per-exchange_id capability matrix + sample capture

Pure research / docs PR.

- `docs/research/per-exchange-samples/{exchange_id}_{provider}_{symbol}.json`
  — one raw response **per (`exchange_id`, provider) pair**. Provider
  segment matches the workstream 2 method block (single source of
  truth for filename pattern); ensures no collision when one venue
  has multiple candidate providers.
- `docs/per-exchange-capability-matrix.md` — the matrix doc.
- Investigation tickets filed per region (per the workstream 2
  region table). No code changes.

### PR 3 — Capability schema + summary API plumbing

The schema + UI-plumbing PR. Six concrete deliverables enumerated
in the workstream 3 "PR 3 explicit scope" block above. No new
external integrations — capability values for non-US exchanges
initially resolve to providers that aren't yet wired (panel hidden
because `data_present=false`) until each region's integration PR
ships.

This PR is *enabling*: it lets future per-region work flip data
into the `data_present=true` state without touching frontend / API
code each time.

## Investigation tickets to file (workstream 2)

The canonical region → exchange_ids → source-candidates table is
the **"Regions and the exchange_ids each ticket covers" table in
workstream 2**. This section exists only to record issue numbers
once filed.

Filing order (one ticket per region row in the workstream 2 region
table; US is excluded — already covered by SEC EDGAR + FMP, no
investigation needed; CFD is excluded — deferred per the region
table):

1. UK — `gh issue create` after operator sign-off → #TBD
2. EU → #TBD
3. Asia (one ticket covering HKEX + TDnet/EDINET + ASX +
   Shenzhen/Shanghai + KRX + TWSE + NSE India + SGX) → #TBD
4. MENA (one ticket covering Tadawul + ADX + DFM) → #TBD
5. Crypto → #TBD
6. Commodity → #TBD
7. FX (mostly documents existing #275 Frankfurter coverage) → #TBD
8. Canada (one ticket covering TMX + TSX Venture) → #TBD

Each ticket links back here. Source candidates and exchange_id
scope per ticket are defined in the workstream 2 region table —
do NOT re-list them here (single source of truth).

## Acceptance criteria

This spec is "done" when:

1. PR 0 has shipped: BTC and LRC instrument pages show price data.
2. PR 1 matrix doc exists and is operator-approved; lookup tables
   ship in the same PR with weekly refresh cron registered.
3. PR 2 matrix doc exists with a provider list (possibly empty
   for "no source picked", possibly multi-source) decided **for
   every capability row in the v1 set × every `exchange_id` row**
   — every cell of the per-`exchange_id` matrix has an explicit
   value, not just one cell per exchange. Empty cells carry a
   free-text rationale in the matrix body.
4. PR 3 has shipped: `exchanges.capabilities` JSONB column,
   `resolve_capabilities()` helper, summary endpoint plumbing,
   frontend per-capability panel gating, admin overrides page.
5. One investigation ticket per *non-excluded* region row in the
   workstream 2 region table is filed against this spec.
   **Excluded by definition (no ticket):** US (already covered by
   SEC EDGAR + FMP), CFD (cross-asset wrapper, deferred), and the
   matrix-only row for `38` Xetra ETFs (covered by the EU ticket).
   The remaining rows (UK, EU, Asia, MENA, Crypto, Commodity, FX,
   Canada — 8 tickets) each cover every `exchange_id` in that
   region in one investigation pass; matrix research is mostly
   portal scanning, so per-venue tickets would fragment the work
   without buying parallelism.

The downstream per-region integrations are NOT acceptance criteria
for this spec — they're the work this spec unlocks.

## Risks

- **Investigation rabbit-holes.** Each region's data landscape is
  different and free APIs come and go. Cap workstream 2 at *one
  week of investigation per region in parallel*, then operator
  decides which to act on. Better to ship a documented "deferred"
  than burn weeks chasing a flaky source.
- **Capability flag explosion.** If the JSON shape grows to 30+
  flags, maintenance pain bites. Cap the v1 set at the 11
  capability keys enumerated in workstream 2's "Rows" bullet
  (filings, fundamentals, dividends, insider, analyst, ratings,
  esg, ownership, corporate_events, business_summary, officers).
  options + short_interest are deferred to a follow-up spec once
  the core 11 are wired.
- **Operator-curated overrides drifting from defaults.** Once the
  per-`exchange_id` default capabilities ship, an operator override
  can silently get out of sync with a future default change. PR 3
  must include an admin "show overrides" page so divergence is
  visible — already promoted into PR 3 explicit scope (item 6).
- **Bloomberg envy.** "What would Bloomberg show" is a useful
  benchmark, not a build target. The thesis engine doesn't need
  every Bloomberg field — it needs the ones that change a hold/buy/
  sell call. Encoded via the workstream 2 contract: a
  not-decision-relevant data type for a venue gets an empty
  provider list (no separate flag). The matrix doc carries free-
  text rationale per cell so the operator review stays honest.
