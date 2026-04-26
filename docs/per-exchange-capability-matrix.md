# Per-exchange-id capability matrix

Date: 2026-04-26
Status: Skeleton (PR 2 of [#515](https://github.com/Luke-Bradford/eBull/pull/515)). Region cells populated by each per-region investigation ticket.

This matrix is workstream 2's deliverable. One column per `exchange_id` row in the live `exchanges` table, one row per v1 capability. Cells carry the operator-decided `providers: list[CapabilityProvider]` value plus a free-text rationale. Empty list = "no source picked" (covers BOTH "no public source available" AND "available but not decision-relevant on this venue, defer").

## Granularity contract

Per [`docs/superpowers/specs/2026-04-26-complete-coverage-spec.md`](superpowers/specs/2026-04-26-complete-coverage-spec.md):

| Artefact | Granularity |
|----------|-------------|
| Matrix rows | 11 v1 capabilities |
| Matrix columns | per-`exchange_id` row in the `exchanges` table (51 venues) |
| Sample fixtures | per (`exchange_id`, provider) pair under `docs/research/per-exchange-samples/` |
| Investigation tickets | one per region (8 tickets — UK / EU / Asia / MENA / Crypto / Commodity / FX / Canada) |
| Operator sign-off | per-`exchange_id` provider list — one cell per (capability × venue) |

## v1 capabilities (11 rows per region)

`filings` · `fundamentals` · `dividends` · `insider` · `analyst` · `ratings` · `esg` · `ownership` · `corporate_events` · `business_summary` · `officers`

Excluded: `news` (#198), `options`, `short_interest` (deferred follow-up).

## Provider enum

See `CAPABILITY_PROVIDERS` in the spec. Empty list (= `[]`) is the canonical absence-of-provider state.

## Provider-tag drift note

The provider tags below (`sec_xbrl`, `sec_dividend_summary`, `sec_8k_item_801`, etc.) include both:

- Tags ALREADY present in eBull's live provenance strings (`sec_xbrl`, `sec_dividend_summary` per `app/api/instruments.py`).
- Tags spec-proposed by `CAPABILITY_PROVIDERS` that don't yet match a live provenance string (`sec_8k_item_801`, `sec_form4`, `sec_13f`, `sec_13d_13g`, `sec_10k_item1`).

Per the #515 spec, PR 3 does the explicit reconciliation: the schema migration audits the live set and aligns the enum so `data_present[provider]` joins to the right SQL EXISTS check without a translation layer. PR 2 surfaces the operator's *decided* provider list; PR 3 makes those values match the implementation reality.

## US (already covered — document only)

Investigation: NONE — SEC EDGAR + FMP already wired since pre-#515.

| Capability | `4` Nasdaq | `5` NYSE | `19` OTC Markets | `20` CBOE | `33` RTH |
|------------|------------|----------|------------------|-----------|----------|
| filings | `["sec_xbrl"]` | `["sec_xbrl"]` | `["sec_xbrl"]` | `["sec_xbrl"]` | `["sec_xbrl"]` |
| fundamentals | `["sec_xbrl", "fmp"]` | `["sec_xbrl", "fmp"]` | `["sec_xbrl", "fmp"]` | `["sec_xbrl", "fmp"]` | `["sec_xbrl", "fmp"]` |
| dividends | `["sec_8k_item_801"]` | `["sec_8k_item_801"]` | `["sec_8k_item_801"]` | `["sec_8k_item_801"]` | `["sec_8k_item_801"]` |
| insider | `["sec_form4"]` | `["sec_form4"]` | `["sec_form4"]` | `["sec_form4"]` | `["sec_form4"]` |
| analyst | `["fmp"]` | `["fmp"]` | `["fmp"]` | `["fmp"]` | `["fmp"]` |
| ratings | `[]` | `[]` | `[]` | `[]` | `[]` |
| esg | `[]` | `[]` | `[]` | `[]` | `[]` |
| ownership | `["sec_13f", "sec_13d_13g"]` | `["sec_13f", "sec_13d_13g"]` | `["sec_13f", "sec_13d_13g"]` | `["sec_13f", "sec_13d_13g"]` | `["sec_13f", "sec_13d_13g"]` |
| corporate_events | `["sec_8k_events"]` | `["sec_8k_events"]` | `["sec_8k_events"]` | `["sec_8k_events"]` | `["sec_8k_events"]` |
| business_summary | `["sec_10k_item1"]` | `["sec_10k_item1"]` | `["sec_10k_item1"]` | `["sec_10k_item1"]` | `["sec_10k_item1"]` |
| officers | `[]` | `[]` | `[]` | `[]` | `[]` |

Notes:

- `ratings` / `esg` / `officers` rows empty across US: not currently ingested. Defer to follow-up specs (out of #515 scope).
- `13F-HR` / `13D` / `13G` ingest is NOT yet wired (no `sec_13f` table exists today). Listed in `providers` per the spec contract that `providers` = "operator's decision list, possibly before wiring"; `data_present[provider]` will report `false` until each is implemented in its own PR.

## UK — investigation ticket #516

Venues: `7` LSE, `42` LSE_AIM, `43` LSE AIM Auction, `44` LSE Auction.

| Capability | `7` LSE | `42` LSE_AIM | `43` LSE AIM Auction | `44` LSE Auction |
|------------|---------|--------------|----------------------|------------------|
| filings | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| fundamentals | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| dividends | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| insider | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| analyst | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| ratings | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| esg | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| ownership | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| corporate_events | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| business_summary | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |
| officers | _pending #516_ | _pending #516_ | _pending #516_ | _pending #516_ |

Source candidates to evaluate: Companies House (free, full filings + officers + accounts), LSE RNS (free announcements). Working hypothesis: filings/officers via Companies House; dividends + corporate_events via LSE RNS; fundamentals possibly via FMP if Companies House XBRL coverage is thin.

## EU — investigation ticket #517

Venues: 22 EU venues across `6` FRA / `9` Paris / `10` Madrid / `11` Borsa Italiana / `12` SIX / `14` Oslo / `15` Stockholm / `16` Copenhagen / `17` Helsinki / `22` Lisbon / `23` Brussels / `30` Amsterdam / `32` Vienna / `34` Dublin EN / `35` Prague / `36` Warsaw / `37` Budapest / `38` Xetra ETFs / `50` Nasdaq Iceland / `51` Tallinn / `52` Vilnius / `53` Riga.

Source candidates: ESMA register (pan-EU regulatory data), national regulators (BaFin/AMF/CONSOB/AFM/CMVM/...), Euronext announcements (Paris/Amsterdam/Brussels/Lisbon/Dublin), FMP for fundamentals where coverage exists.

One per-venue capability table follows; every cell `_pending #517_` until investigation lands.

### `6` — FRA

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `9` — Euronext Paris

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `10` — Bolsa De Madrid

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `11` — Borsa Italiana

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `12` — SIX Switzerland

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `14` — Oslo

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `15` — Stockholm

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `16` — Copenhagen

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `17` — Helsinki

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `22` — Euronext Lisbon

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `23` — Euronext Brussels

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `30` — Euronext Amsterdam

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `32` — Vienna

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `34` — Dublin EN

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `35` — Prague

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `36` — Warsaw

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `37` — Budapest

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `38` — Xetra ETFs

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `50` — Nasdaq Iceland

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `51` — Nasdaq Tallinn

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `52` — Nasdaq Vilnius

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

### `53` — Nasdaq Riga

| Capability | Providers |
|------------|-----------|
| filings | _pending #517_ |
| fundamentals | _pending #517_ |
| dividends | _pending #517_ |
| insider | _pending #517_ |
| analyst | _pending #517_ |
| ratings | _pending #517_ |
| esg | _pending #517_ |
| ownership | _pending #517_ |
| corporate_events | _pending #517_ |
| business_summary | _pending #517_ |
| officers | _pending #517_ |

## Asia — investigation ticket #518

Venues: `13` TYO, `21` Hong Kong Exchanges, `31` Sydney, `45` Shenzhen, `46` Shanghai, `47` NSE India, `49` Singapore, `54` Korea Exchange, `55` Taiwan SE, `56` Tokyo Stock Exchange.

Source candidates: HKEX disclosure, TDnet/EDINET (Japan), ASX announcements (AU), KRX/KIND (Korea), TWSE/MOPS (Taiwan), SSE/SZSE (China), NSE/BSE India, SGX (Singapore). One per-venue capability table follows.

### `13` — TYO

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `21` — Hong Kong Exchanges

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `31` — Sydney

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `45` — Shenzhen

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `46` — Shanghai

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `47` — NSE India

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `49` — Singapore

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `54` — Korea Exchange

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `55` — Taiwan SE

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

### `56` — Tokyo Stock Exchange

| Capability | Providers |
|------------|-----------|
| filings | _pending #518_ |
| fundamentals | _pending #518_ |
| dividends | _pending #518_ |
| insider | _pending #518_ |
| analyst | _pending #518_ |
| ratings | _pending #518_ |
| esg | _pending #518_ |
| ownership | _pending #518_ |
| corporate_events | _pending #518_ |
| business_summary | _pending #518_ |
| officers | _pending #518_ |

## MENA — investigation ticket #519

Venues: `24` Tadawul, `39` Dubai Financial Market, `41` Abu Dhabi.

Source candidates: Tadawul disclosure portal (Saudi Arabia), ADX disclosure (Abu Dhabi), DFM disclosure (Dubai). One per-venue capability table follows.

### `24` — Tadawul

| Capability | Providers |
|------------|-----------|
| filings | _pending #519_ |
| fundamentals | _pending #519_ |
| dividends | _pending #519_ |
| insider | _pending #519_ |
| analyst | _pending #519_ |
| ratings | _pending #519_ |
| esg | _pending #519_ |
| ownership | _pending #519_ |
| corporate_events | _pending #519_ |
| business_summary | _pending #519_ |
| officers | _pending #519_ |

### `39` — Dubai Financial Market

| Capability | Providers |
|------------|-----------|
| filings | _pending #519_ |
| fundamentals | _pending #519_ |
| dividends | _pending #519_ |
| insider | _pending #519_ |
| analyst | _pending #519_ |
| ratings | _pending #519_ |
| esg | _pending #519_ |
| ownership | _pending #519_ |
| corporate_events | _pending #519_ |
| business_summary | _pending #519_ |
| officers | _pending #519_ |

### `41` — Abu Dhabi

| Capability | Providers |
|------------|-----------|
| filings | _pending #519_ |
| fundamentals | _pending #519_ |
| dividends | _pending #519_ |
| insider | _pending #519_ |
| analyst | _pending #519_ |
| ratings | _pending #519_ |
| esg | _pending #519_ |
| ownership | _pending #519_ |
| corporate_events | _pending #519_ |
| business_summary | _pending #519_ |
| officers | _pending #519_ |

## Crypto — investigation ticket #520

Venues: `8` Digital Currency.

Source candidates: CoinGecko (market cap, exchange listings, on-chain summary), Glassnode (on-chain metrics, free tier).

| Capability | `8` Digital Currency |
|------------|----------------------|
| filings | `[]` (no regulator-style filings; rationale per #520) |
| fundamentals | `[]` (no IS/BS/CF for crypto) |
| dividends | `[]` (n/a — staking yields are a separate primitive) |
| insider | `[]` (no Form-4 equivalent) |
| analyst | _pending #520_ |
| ratings | _pending #520_ |
| esg | _pending #520_ |
| ownership | _pending #520_ (large-holder concentration via on-chain — distinct from SEC-style 13F) |
| corporate_events | _pending #520_ (token-level events: forks, halvings, governance proposals) |
| business_summary | _pending #520_ (project description from CoinGecko) |
| officers | `[]` (n/a) |

Pre-decided empties reflect "no SEC-style equivalent on this venue, no plan to ingest". Pending cells need operator decision per #520 investigation.

## Commodity — investigation ticket #521

Venues: `2` Commodity, `40` CME.

Source candidates: CME Group reference data (futures spec), LME (metals).

| Capability | `2` Commodity | `40` CME |
|------------|---------------|----------|
| filings | `[]` | `[]` |
| fundamentals | `[]` | `[]` |
| dividends | `[]` | `[]` |
| insider | `[]` | `[]` |
| analyst | _pending #521_ | _pending #521_ |
| ratings | `[]` | `[]` |
| esg | `[]` | `[]` |
| ownership | `[]` (CFTC COT — separate from equity ownership; defer) | `[]` |
| corporate_events | _pending #521_ (contract roll dates, expiries) | _pending #521_ |
| business_summary | _pending #521_ (contract spec) | _pending #521_ |
| officers | `[]` | `[]` |

Most cells legitimately empty — commodities don't carry equity-style data. Pending cells are the few that map.

## FX — investigation ticket #522

Venues: `1` FX.

Source candidates: ECB Frankfurter (already wired #275 for daily rates), Fed (DXY + USD pairs), BoE (GBP pairs).

| Capability | `1` FX |
|------------|--------|
| filings | `[]` |
| fundamentals | `[]` |
| dividends | `[]` |
| insider | `[]` |
| analyst | _pending #522_ (rate-direction sell-side?) |
| ratings | `[]` |
| esg | `[]` |
| ownership | `[]` |
| corporate_events | _pending #522_ (central bank meeting calendar) |
| business_summary | `[]` (n/a — pairs, not entities) |
| officers | `[]` |

FX is heavily "no-source" by nature. Document existing #275 coverage in #522 and identify the few rate-meta cells worth filling.

## Canada — investigation ticket #523

Venues: `18` Toronto, `48` TSX Venture.

Source candidates: TMX Group reference data (TSX + TSX Venture), SEDAR+ (Canadian regulatory filings — successor to SEDAR).

| Capability | `18` Toronto | `48` TSX Venture |
|------------|--------------|------------------|
| filings | _pending #523_ | _pending #523_ |
| fundamentals | _pending #523_ | _pending #523_ |
| dividends | _pending #523_ | _pending #523_ |
| insider | _pending #523_ | _pending #523_ |
| analyst | _pending #523_ | _pending #523_ |
| ratings | _pending #523_ | _pending #523_ |
| esg | _pending #523_ | _pending #523_ |
| ownership | _pending #523_ | _pending #523_ |
| corporate_events | _pending #523_ | _pending #523_ |
| business_summary | _pending #523_ | _pending #523_ |
| officers | _pending #523_ | _pending #523_ |

Note: Canada also pending an `asset_class` vocabulary extension (currently rows stay `unknown` per #514; #523 may propose `na_equity` or similar).

## CFD — out of scope

Venue `3` CFD: cross-asset wrapper, no native data source. Excluded from investigation per the spec's region table.

## Acceptance

PR 2 ships:

- [x] This matrix doc — skeleton with US row populated, every non-US cell either pre-decided empty (with rationale) or marked `_pending #51X_`.
- [x] `docs/research/per-exchange-samples/` directory with README pinning the filename pattern.
- [x] All 8 region tickets filed (#516–#523) with their venue list + source candidates.

PR 2 does NOT ship:

- Filled-in non-US cells (those are each region ticket's deliverable).
- External-source sample fixtures (those land per region ticket as the operator/agent investigates).
- Code changes — capability schema lands in PR 3.

## Operator sign-off

Required: confirm the US row + the pre-decided `[]` cells (crypto / commodity / fx) before each region ticket investigates the rest. Region tickets land their cells against this skeleton.
