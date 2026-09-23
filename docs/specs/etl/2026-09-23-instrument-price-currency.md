# Per-instrument price currency from the broker's conversion rate (#3322)

## Problem

`instruments.currency` is copied from the exchange's curated currency (`sql/159`,
`app/services/universe.py` upsert). That is wrong wherever a venue quotes lines in more
than one currency. Most of the LSE is quoted in **pence**, and the LSE also lists lines
quoted in USD and EUR. Every native-to-display conversion keyed on the label is wrong for
those lines: 100× too high for pence, and the wrong FX for USD/EUR lines.

## Source rule

- eToro's metadata endpoints expose **no** currency field. Checked on the live portal
  2026-09-23: `search-for-instruments.md` (73 fields, none currency) and
  `get-instrument-display-data.md` (only `priceSource`). `app/providers/implementations/etoro.py`
  already records this (`currency=None`).
- The rates endpoint (`get-instrument-market-rates.md`) documents
  `conversionRateAsk` / `conversionRateBid` as *"Current conversion rate … from
  instrument's currency to USD, used for position value calculations"*. That makes it the
  broker's own statement of the instrument's price currency, and the only one the API
  exposes. `app/services/strategy_core_quote_observation.py` already treats it as
  *"the ONLY per-instrument denomination signal the API exposes"*.
- Attended witness: on the IUSA.L close (#3007 comment 5791270221) the conversion rate was
  0.013303 = GBP/USD ÷ 100, and it reconciles `rate × conversionRate × units = proceeds`.
- **Pence has no ISO 4217 code.** The market convention is `GBX` (1 GBX = 0.01 GBP). We
  store `GBX`. It fits the existing `^[A-Z]{3}$` shape and is not a real ISO code, so
  anything that needs ISO must normalise it first.

## Full-population verification

Run on 2026-09-23 over every tradable instrument with a currency: 12,134 instruments,
12,128 quoted. Script: `scripts/census_3322_instrument_price_currency.py` (committed with this
PR, read-only). For each instrument the script finds the nearest currency whose USD rate
matches `conversion_rate` within 3%, and reports the result split by quote freshness.

- **Fresh quotes (≤ 7 days old):** every quote matches exactly one currency. Rows whose
  match differs from the label:
  - LSE, labelled GBP: 547 GBX, 268 USD, 6 EUR, 3 GBP. So only 3 of 824 fresh
    GBP-labelled lines are actually quoted in GBP.
  - EUR venues: 40 USD, 1 GBP. SEK venue: 1 EUR.
  - US venues: 6,362 of 6,362 USD.
- **Stale quotes (> 7 days old; dead listings with a frozen FX rate):** these are
  ambiguous. For example, 21 NOK lines match SEK and 82 GBP lines match nothing, because
  the frozen rate has drifted from today's FX. Stale quotes must never drive a
  reclassification.

## Design (rev 2, after Codex ckpt-1)

1. **Scope: GBP and EUR venues only.** An instrument is reclassified only when its
   exchange currency is `GBP` or `EUR`. For those venues, the label currency's own rate
   is in `live_fx_rates` (which holds EUR, GBP and USD only), so "exactly one candidate
   matches" is well-defined. On any other venue, an unpriced currency could uniquely match
   the wrong candidate, so those venues are not reclassified. This forgoes 1 fresh
   mismatch (VSURE.ST, SEK venue, quoted in EUR). US venues matched USD 6,362 of 6,362 on
   the census.
2. **Schema (`sql/415`).**
   - `instruments.currency_source TEXT NOT NULL DEFAULT 'exchange'
     CHECK (currency_source IN ('exchange','broker_rate'))`.
   - `instruments.currency_derived_at TIMESTAMPTZ` (evidence timestamp: the quote time
     used).
   - `'broker_rate'` means the currency came from the conversion rate and differs from the
     exchange default.
3. **Classifier (pure).**
   `classify_price_currency(conversion_rate, quoted_at, fx_usd_per_unit, fx_quoted_at, now)`
   returns `(currency | None, reason)`.
   - Candidates: `USD` = 1, `EUR` and `GBP` from `live_fx_rates` (`USD→X`, inverted), and
     `GBX` = GBP ÷ 100.
   - Returns a currency only when **all** of these hold:
     - `−5min ≤ now − quoted_at ≤ 7d`, where `now` is read AFTER each batch fetch (a quote
       stamped during the fetch is not future-dated; 5 min absorbs broker clock skew);
     - every FX row used is ≤ 7d old;
     - `conversion_rate` is positive and finite;
     - **exactly one** candidate has `|conv / candidate − 1| ≤ 0.02`.
   - Otherwise returns `None` with a reason: `stale_quote`, `stale_fx`, `bad_rate`,
     `no_match` or `ambiguous`.
   - Constants, fixed by construction from the census below and frozen in code:
     - `TOLERANCE = 0.02`: about 3× the largest fresh residual (0.64%) and below 1/7 of
       the smallest candidate spacing (EUR/USD 14.6%).
     - `MAX_AGE = 7d`: every quote ≤ 7d old matched uniquely.
4. **Writer.** `derive_instrument_price_currencies(conn, provider)` runs inside
   `nightly_universe_sync`, after the upsert, in its own transaction.
   - For each classified row: set `currency`, `currency_source`
     (`'broker_rate'` iff it differs from the exchange currency) and
     `currency_derived_at`.
   - `None` leaves the row untouched.
   - Returns counts by reason. The job logs them, so a mass `stale_fx` is visible rather
     than a silent no-op.
5. **Universe upsert.** Keep the existing currency when `currency_source = 'broker_rate'`
   **and** the exchange is unchanged. If the exchange changes, reset to the new exchange
   default with `currency_source = 'exchange'`; the writer re-derives it the same night.
   The change-detection `WHERE` clause uses the same predicate, so protected rows do not
   rewrite every sync.
6. **FX chokepoint.** `fx.convert` and `fx.convert_quote_fields` handle `GBX`
   explicitly:
   - from `GBX`: amount ÷ 100, then continue as `GBP`;
   - to `GBX`: convert to `GBP`, then × 100;
   - `GBX ↔ GBP` needs no FX row;
   - `GBX → GBX` is identity.
7. **Tax ledger.** `_load_fx_rate` treats `GBX` as `GBP` for the FX key. The code comments
   there say fill amounts are USD, not native, so the price unit is irrelevant to that
   path. This keeps LSE lines exactly as today and prevents a GBX fill from aborting the
   whole batch.

### Why GBX in `currency` rather than a separate price-scale column

A consumer that does not know about the new unit must **fail closed**:
- `GBX` has no FX pair, so `convert` raises, `convert_quote_fields` returns `None`, and
  fair value and FCF report `currency_mismatch`.
- A `price_scale` column next to `currency = 'GBP'` fails **open**: every consumer that
  does not read it stays 100× wrong silently. That is exactly today's defect.
- Market-data vendors quote LSE pence lines as `GBX` / `GBp`.

Amounts that are already in a major unit must not be labelled with the instrument's
currency. The consumers audited below either convert price-unit amounts
(`units × native price`) or, in tax, do not use the unit at all.

### Residual risks (accepted, stated)

- `price_daily` history is assumed to be in the current quote unit. eToro serves one
  candle series per instrument. A listing that changed its quote currency mid-history
  would be mis-scaled before the change. No such case is known.
- A USD relabel can move `fair_value_band` from `currency_mismatch` to enabled for
  USD-reporting LSE lines. That is correct: price and fundamentals are both USD.

## Full-population verification of the production rule

Script: `scripts/census_3322_instrument_price_currency.py`, which imports the production
classifier. Run on 2026-09-23 against the GBP and EUR venues (3,000 quoted instruments).

- Largest match residual by quote age:
  - ≤ 1d: 0.0064 (n = 2,567)
  - ≤ 4d: 0.0006 (n = 30)
  - ≤ 7d: 0.0019 (n = 3)
  - ≤ 30d: 0.0183 (n = 20)
  - > 30d: 0.3374 (n = 379)
- At tolerance 0.01, 0.02 and 0.03, **0** instruments match more than one candidate.
- Derivations at 0.02 with age ≤ 7d:

  | Venue | Derived currency | Count |
  | --- | --- | --- |
  | GBP venues | GBX | 546 |
  | GBP venues | USD | 268 |
  | GBP venues | EUR | 6 |
  | GBP venues | GBP | 3 |
  | EUR venues | EUR | 1,736 |
  | EUR venues | USD | 40 |
  | EUR venues | GBP | 1 |

## Consumers (`grep -rn "i\.currency\|instruments\.currency" app`)

| Consumer | Effect of GBX / USD relabel |
| --- | --- |
| `app/api/portfolio.py` `get_portfolio` trades | Was adding eToro's `amount` (USD cost basis, `app/providers/broker.py` `BrokerPosition` docstring) to a native price delta. Fixed here: `amount / open_conversion_rate` to native first, the same arithmetic `/portfolio/instruments/{id}` already used. Otherwise a GBX label turns a 34% error into a 100× one. |
| `app/services/portfolio_eod.py::compute_eod_equity` | Had the same USD-amount-plus-native-delta mix; fixed the same way (Codex ckpt-2 P1). |
| `app/api/portfolio.py::get_instrument_positions` | Computes every money field in USD on purpose (`amount + delta × open_conversion_rate`), so the relabel does not change its numbers. It labels them with the instrument currency, which is wrong under GBP today and equally wrong under GBX. Pre-existing and out of scope. |
| `app/api/portfolio.py::_build_fx_rates_used` | Reports the GBP pair scaled by 0.01 for GBX (Codex ckpt-2 P2). |
| `app/services/valuation.py`, `app/api/sse_quotes.py` | Go through `fx.convert` / `convert_quote_fields`, so they become correct. |
| `app/services/fair_value_band.py::currency_coherent`, `app/services/fcf_yield.py` | Compare against `reported_currency` (GBP). With `GBX` they become mismatched: fair value fails closed, and fcf_yield looks up `fx_history` for GBX, finds none, and returns a NULL point. Today they silently divide GBP fundamentals by a pence price (100× off). Failing closed is the intended outcome. |
| `app/services/tax_ledger.py::_ingest_fills` | GBX maps to GBP (design item 7), so LSE lines behave as today. There are 0 non-USD fills today (`fills ⋈ orders ⋈ instruments where currency <> 'USD'` returns 0 rows). Pre-existing, out of scope: the code comments say `gross_amount` is USD, yet it is converted using the instrument currency. |
| `app/services/canonical_instrument_redirects.py` | Equality filter restricted to `us_equity` exchanges, which are skipped by the writer, so no change. |
| `frontend/src/lib/format.ts` | `getFormatter` already falls back when `Intl` rejects a non-ISO code (see `format.test.ts:166`). A `GBX` label renders through the fallback without throwing. |

## Rollback

Remove the writer call from `nightly_universe_sync`, set `currency_source = 'exchange'`,
and re-run `sql/159` step 3's backfill. The migration is additive.

## Out of scope

- Price series (`price_daily`) stay in native pence. They are consistent with the broker,
  and conversion happens on read.
- Tax-ledger currency semantics.
