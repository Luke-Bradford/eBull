# Settled decisions

This file records **live, repo-level decisions that are already settled**.

## Product name

The project is called **eBull**. Use `eBull` in all documentation, code comments, and PR descriptions.
The previous working name `trader-os` is retired.

Its purpose is simple:

- stop re-deciding things that are already decided
- keep implementation aligned across issues and PRs
- reduce semantic drift between modules

Do **not** put broad architecture prose here.
Do **not** put coding-style rules here.
Do **not** put unresolved ideas here.

Only record decisions that are:
- already agreed
- currently active
- likely to affect future implementation choices

---

## How to use this file

Before designing or coding for an issue:

1. Identify which decisions in this file apply.
2. State how your implementation preserves them.
3. If you think one must change, surface that explicitly before coding.
4. Do not silently reinterpret or override settled decisions.

---

## Provider strategy

### Market / execution source of truth
- eToro is the source of truth for:
  - tradable universe
  - quotes and candles in v1
  - portfolio/account data
  - execution

### Fundamentals provider
- FMP is the normalized fundamentals provider in v1.

### Official filings providers
- SEC EDGAR is the official filings source for US issuers.
- Companies House is the official filings source for UK issuers.

### Conflict rule
- If official filings and normalized provider data conflict, prefer the official filing.

### Provider design rule
- providers are thin adapters
- providers do not own DB lookups
- service layer resolves provider-native identifiers
- provider code should stay free of domain orchestration logic

---

## Identifier strategy

### External identifiers
- provider-native identifiers are stored in `external_identifiers`
- service layer resolves these before provider calls
- providers do not fuzzy-resolve tickers as a normal path

### Filing lookup rule
- do not use `symbol` as the universal filing lookup key
- SEC uses CIK
- Companies House uses `company_number`

---

## Filing and fundamentals storage

### Filing event storage
- `filing_events` stores metadata, extracted summary, risk score, provider payload, and canonical document link
- full raw filing text is out of scope for v1
- if full text is needed later, use a separate table, not `filing_events`

### Filing dedupe
- filing identity is provider-scoped
- provider filing identity must be stable and idempotent

### Fundamentals snapshot semantics
- `as_of_date` means financial statement period end date
- it does not mean fetch time
- when combining TTM + balance-sheet values, use the balance-sheet period end as the canonical snapshot date in v1

---

## News and sentiment

### News event storage
- `news_events` stores:
  - `url`
  - `url_hash`
  - `snippet`
  - sentiment and importance values
  - raw provider payload

### News dedupe
- exact dedupe is per `(instrument_id, url_hash)`
- near-duplicate detection is per instrument, not global

### Sentiment storage
- persist sentiment as a signed numeric score
- do not add separate label columns in v1

### News provider shape
- production code depends on a `NewsProvider` abstraction
- tests use fakes/stubs of that abstraction
- do not shape production APIs around test convenience

---

## Thesis semantics

### Thesis versioning
- each thesis generation inserts a new row
- do not overwrite prior thesis rows

### Critic output
- critic output is stored separately in `critic_json`
- do not append critic text into `memo_markdown`

### Allowed thesis types
Use this constrained set in application code:
- `compounder`
- `value`
- `turnaround`
- `speculative`

### Allowed stances
Use this constrained set in application code:
- `buy`
- `hold`
- `watch`
- `avoid`

### Thesis freshness
- thesis freshness is based on the latest thesis row `created_at`
- freshness window comes from `coverage.review_frequency`
- `coverage.last_reviewed_at` is operational metadata, not primary truth for freshness

### Review frequency mapping
- `daily` = 1 day
- `weekly` = 7 days
- `monthly` = 30 days

### Thesis prompt budget
Use capped context in v1:
- latest 1 prior thesis
- latest 3 filing events
- latest snapshot + up to 4 prior fundamental snapshots
- latest 10 news items from the last 30 days

### Critic invocation
- run the critic call for every thesis generation in v1

---

## Scoring and ranking

### Scoring model style
- v1 scoring is heuristic, explicit, and auditable
- do not use ML
- do not use cohort-relative normalization
- do not hide weighting logic

### Penalty style
- penalties are additive in v1
- do not use multiplicative penalties in v1

### Score auditability
- each score row should carry enough detail to explain how it was produced
- rank and rank delta belong with the score row in v1
- no separate rankings table in v1

### Model versioning
- `model_version` includes the scoring mode
- default scoring mode is `v1-balanced`

### Rank delta comparison
- compare rank delta only against the most recent prior run using the same model version / mode

---

## Portfolio manager semantics

### Cash semantics
- `cash_ledger.amount` uses:
  - positive = cash inflow
  - negative = cash outflow

### Unknown cash rule
- in the portfolio manager, empty / unknown cash does not hard-block recommendations
- unknown cash should be recorded in the explanation
- hard cash enforcement belongs to the execution guard (see "Cash enforcement" under Execution guard semantics below)

### AUM basis
- AUM and concentration should use mark-to-market first
- if no current quote exists, fall back to cost basis
- do not use unrealized P&L as the primary AUM source

### ADD rule
- `ADD` requires more than a new thesis version
- conviction must have improved materially via thesis confidence and/or score improvement

### EXIT rule in portfolio manager
In v1, `EXIT` is supported for:
- thesis break
- severe risk event
- valuation target achieved

Do not implement superior-rotation-driven exits in v1.

### Held but unranked instruments
- held instruments that fall out of ranking still need a view
- default to `HOLD` unless an `EXIT` rule fires

### Recommendation persistence
- recommendation history is append-oriented
- do not spam identical `HOLD` rows every run

---

## Execution guard semantics

### Kill switch
- kill switch is a DB-backed runtime flag
- it is separate from deployment config flags

### Config controls
- `enable_auto_trading` is not the same as `enable_live_trading`
- both may be checked
- neither replaces the kill switch

### Guard input
- v1 execution guard consumes `recommendation_id`
- it then builds current-state evaluation internally

### Guard auditability
- write one `decision_audit` row per guard invocation
- store per-rule results inside `evidence_json`

### Guard re-check rule
- the execution guard must re-check critical constraints against current state
- never trust old recommendation state as proof that execution is still valid

### Action-specific behaviour
For `BUY` / `ADD`, the guard checks things like:
- kill switch
- config flags
- fresh thesis
- Tier 1 coverage
- spread/cash/concentration

For `EXIT`:
- do not block just because thesis is stale
- do not block just because coverage is no longer Tier 1
- do not block just because spread is wide

### Cash enforcement
- unknown cash may be tolerated in recommendation generation
- unknown cash must fail executable `BUY` / `ADD` in the execution guard

---

## General engineering decisions

### Provider boundary
- keep providers thin
- keep domain logic in services
- keep DB access out of HTTP clients

### Auditability
- persist structured evidence where it matters
- do not leave critical model / recommendation / execution paths unexplained

### Deferrals
- if a real issue is intentionally left out of scope, open tech debt and record it explicitly
- do not silently ignore warnings or nitpicks

---

## Operator auth and broker-secret storage

- Governed by [`docs/adr/0001-operator-auth-and-broker-secrets.md`](adr/0001-operator-auth-and-broker-secrets.md).

---

## Maintenance rule

When a new repo-level decision is agreed and is likely to affect future implementation:
- add it here
- keep it short
- keep it concrete
- remove or update stale decisions when they no longer apply
