# Data sources

eBull is a **free regulated-source-only** project (#532, settled).
No paid third-party fundamentals provider. Every data source below
is either contractually-clean public data or covered by an explicit
operator-credentialed API.

## Source matrix

| Source | Coverage | Cadence | Rate limit | Raw store | Auth |
|---|---|---|---|---|---|
| **eToro** | Quotes / candles / portfolio / orders | Real-time on demand | per eToro API ToS | n/a (transient) | Operator credentials |
| **SEC EDGAR** | US filings (10-K, 10-Q, 8-K, 13F-HR, 13D/G, NPORT-P, N-CSR, DEF 14A, Form 4) | Continuous; 60-day lag for NPORT-P, T+45 for 13F-HR | 10 req/s shared (User-Agent required) | `filing_raw_documents` / `cik_raw_documents` / `sec_reference_documents` | None (public) |
| **FINRA Equity Short Interest** | US short interest by symbol | Bimonthly (settlement-date snapshots) | n/a (CDN file) | `provider_short_interest_raw_documents` (planned #915) | None (public) |
| **FINRA RegSHO Daily** | US daily short sale volume | Daily (EOD) | n/a (CDN file) | reuses #915 raw store | None (public) |
| **Companies House (UK)** | UK filings + company metadata | Per filing | per OGL terms | `filing_raw_documents` (UK adapter) | None (public) |
| **Anthropic API** | Thesis writing, critic review, narrative generation | On demand | per Anthropic API ToS | n/a (request/response) | Operator API key |

## Identifier strategy

Provider-native identifiers are stored in `external_identifiers`.
The service layer resolves these before any provider call.
**Providers do not fuzzy-resolve tickers as a normal path.**

Filing-lookup keys differ by jurisdiction:
- SEC uses **CIK** (10-digit zero-padded).
- Companies House uses **company_number**.

CUSIP is the issuer + share-class identifier (9 chars) for US
securities. eBull joins CUSIP → instrument_id via
`external_identifiers (provider='sec', identifier_type='cusip')`.

## Raw-payload-first rule

Any new job that fetches an external HTTP payload must `INSERT` the
raw bytes / text into the appropriate raw-payload table **before**
calling any parser / normaliser. This is a PREVENTION rule (see
`docs/review-prevention-log.md` for the original incident on #914).

Reason: re-wash after parser-bug discovery must not re-fetch from the
upstream — both because of rate limits and because the upstream may
have amended the data since.

## Cadence and freshness

The freshness model lives in `app/services/data_freshness.py`. Each
source has an expected refresh window (e.g. `sec_13f_hr` = 120 days,
`sec_form4` = 5 days). Stale data shows up on the operator
freshness dashboard (see [`runbooks/runbook-data-freshness.md`](runbooks/runbook-data-freshness.md)).

## What's NOT in scope

- yfinance / scraped feeds / unofficial API wrappers — not in scope
  per memory `feedback_data_source_constraints.md`.
- Paid fundamentals (S&P, FactSet, Bloomberg, Refinitiv) — not in
  scope per #532 settled decision.
- Short borrow rate / utilisation — vendor-paid, never in scope per
  #915 ticket body.
