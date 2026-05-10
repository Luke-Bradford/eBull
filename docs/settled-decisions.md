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

### Fundamentals provider posture

- Free regulated-source-only (#532). No paid third-party fundamentals provider.
- US: SEC XBRL via EDGAR Company Facts API.
- UK / EU / Asia / MENA / Canada: per-region integration PRs land their own
  free regulated-source providers (Companies House, ESMA, etc.).

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
- default scoring mode is `v1.1-balanced` (v1.1 = TA-enhanced momentum)

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

- Governed by [`ADR-0001`](adr/0001-operator-auth-and-broker-secrets.md) and
  [`ADR-0003`](adr/0003-local-secret-bootstrap-and-recovery.md) (amended
  2026-05-07: phrase-based recovery removed in favour of operator-driven
  re-entry; stale-cipher soft-revoke runs at boot when key material is
  missing or mismatches existing ciphertext).

---

## Product-visibility pivot (2026-04-18, lifted 2026-04-18)

Infra-track pause for filings cascade, raw housekeeping, and
fundamentals expansion was scoped to "until #313 + #314 ship". Both
shipped on 2026-04-18 (and #315 on 2026-04-21), so the pause is no
longer in force. Plan B.1 (TRACKED_CONCEPTS expansion), Plan B.3
(company metadata), Plan C.1/C.2/C.3 (insider/13F/segment), Chunk L
flag-flip, and raw-retention dry-run-off may proceed once their own
issues are prioritised.

The product-test that lives on independent of the pause: every new
ticket should still answer yes to *"Would the operator feel this
moves the product closer to 'I can manage my fund from this
screen'?"* — else rewrite or drop. Plan + backlog still at
[`docs/superpowers/plans/2026-04-18-product-visibility-pivot.md`](superpowers/plans/2026-04-18-product-visibility-pivot.md).

---

## Process topology (#719, settled 2026-04-30)

- The FastAPI process (`app.main`) serves HTTP only. No APScheduler, no
  manual-trigger executor, no orchestrator executor, no reaper, no
  boot freshness sweep.
- The jobs process (`python -m app.jobs`) owns APScheduler, the
  manual-trigger executor, the sync orchestrator's executor, the
  reaper, the queue dispatcher, the boot-time freshness sweep, and
  the heartbeat writer.
- Inter-process communication is Postgres-only: durable rows in
  `pending_job_requests`, `pg_notify('ebull_job_request', ...)` as a
  wakeup hint. No HTTP, no Redis, no shared memory.
- Both processes use the hardened `_open_pool` helper at `app/db/pool.py`.
- Triggers are durable: every `POST /jobs/{name}/run` and `POST /sync`
  writes a row before NOTIFY, so a trigger sent while the jobs process
  is restarting is replayed on boot rather than lost.
- A session-scoped Postgres advisory lock on a dedicated long-lived
  connection (`JOBS_PROCESS_LOCK_KEY` in `app/jobs/locks.py`) enforces
  the singleton: starting `python -m app.jobs` while another instance
  is alive is a hard FATAL exit. Boot recovery's "claimed by stale
  boot id" reset is only safe under that invariant.

Do not re-introduce in-process scheduling in the API. Do not add a third
pool with raw `ConnectionPool(...)` — use `open_pool`.

## Cancel UX (#1064, settled 2026-05-09)

- **Decision:** Cancel is cooperative-with-checkpoints, never faked
  hard-kill. The Cancel button writes a row into
  `process_stop_requests`; the worker observes the signal at a
  well-defined checkpoint, completes the in-flight item (writes are
  idempotent), then transitions the run row to `cancelled`. The next
  Iterate reads the watermark and re-fetches anything not committed.
- **Why:** Hard-kill mid-write leaves partial rows on disk, and the
  next run reads a watermark that incorrectly suggests "we got that
  far" — masking the gap. Cooperative cancel + watermark-aware resume
  guarantees the next iterate reads a clean cursor and re-fetches
  anything not committed. The hard-kill failure mode was identified
  in #1064 design discussion (operator quote §3.5: "restarting jobs
  but the jobs are still running").
- **Originally:** "Cancel out of scope for v1" (placeholder noted in
  the umbrella spec at line 982). AMENDED 2026-05-09 by #1064 as
  cooperative cancel becomes a v1 affordance — watermarks ensure
  resume is safe by construction.
- **Cancel-mode choice is at cancel time, not an upgrade path:** the
  modal exposes "Cooperative" (default) and "Terminate (mark for
  cleanup)" via the More disclosure. A second cancel against the same
  in-flight run is rejected by the partial-unique active-stop index
  (sql/135) — the escape hatch for a wedged worker is the
  jobs-process restart + boot-recovery sweep, not a re-cancel.
- **Enforced in:** `app/services/process_stop.py` (request / observe /
  complete state machine with partial-unique active-stop slot,
  cooperative + terminate `StopMode` Literal); `app/services/sync_orchestrator/executor.py::_check_cancel_signal`
  (in-tx late-cancel probe); FE `CancelConfirmDialog` (cooperative
  default; terminate is a controlled disclosure, not a primary
  affordance).
- **Spec:** `docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md`
  §"Cancel semantics — cooperative" + Codex round 1 amendment B4 +
  round 2 R2-W2.

## CIK = entity, CUSIP = security (#1102, settled 2026-05-10)

Share-class siblings (GOOG/GOOGL, BRK.A/BRK.B, …) legitimately share an SEC
CIK — the CIK identifies the issuer (legal entity), not the security. The
CUSIP identifies the security (per-share-class). Every reputable feed (CRSP,
Bloomberg, Yahoo, IEX, OpenFIGI, SEC EDGAR itself) encodes this shape.

`external_identifiers` enforces this in two partial unique indexes
(migration `sql/143`):

- `uq_external_identifiers_provider_value_non_cik` — global UNIQUE on
  `(provider, identifier_type, identifier_value)` for every NON-CIK
  identifier. CUSIP / symbol / accession_no remain globally unique.
- `uq_external_identifiers_cik_per_instrument` — UNIQUE on `(provider,
  identifier_type, identifier_value, instrument_id)` for `(sec, cik)` rows.
  Multiple instruments may share a CIK; each (CIK, instrument) pair is
  unique.

`upsert_cik_mapping` (`app/services/filings.py`) claims the CIK
independently per instrument — there is no flap. Pre-#1102 the global
constraint forced ON CONFLICT to rewrite the row's `instrument_id` to
the last writer, so `daily_cik_refresh` ping-ponged the binding between
siblings on every run, leaving one without 10-K / fundamentals.

Postgres ON CONFLICT inference against partial unique indexes requires
the predicate be supplied on the upsert. Empirically verified against
Postgres 17 — without the predicate, the insert fails with
"no unique or exclusion constraint matching the ON CONFLICT specification".
All `INSERT ... ON CONFLICT (provider, identifier_type, identifier_value) DO ...`
sites must attach the matching predicate (CIK target gets the 4-tuple
+ `WHERE provider='sec' AND identifier_type='cik'`; non-CIK gets the
3-tuple + `WHERE NOT (provider='sec' AND identifier_type='cik')`).

Entity-level data (10-K text, business summary, financial facts) is
denormalised across siblings — acceptable for the small share-class
population (~10 known instruments). If the population grows to 50+, file
a follow-up to introduce a proper `entities` layer (Option B from the
#1094 design discussion).

`canonical_instrument_id` (#819) is a **different** mechanism for `.RTH`
operational duplicates — same security, two ticker variants. Don't
conflate. See "Canonical-instrument redirect (#819, settled 2026-05-11)"
below for the operational-duplicate redirect semantics.

- **PR-A:** sql/143 migration + filings.py upsert + ON CONFLICT predicate
  sweep across ~25 production + test sites + `tests/test_upsert_cik_mapping.py`
  flips.
- **PR-B (deferred):** fan-out CIK→instrument multimap in
  `sec_companyfacts_ingest.py`, `sec_submissions_ingest.py`,
  `sec_insider_dataset_ingest.py` so share-class siblings BOTH receive
  bulk-ingest data. Until PR-B lands, only one of two siblings has
  fundamentals / submissions / insider data — but the binding is stable
  rather than flapping (strict improvement).

**Spec:** `docs/superpowers/specs/2026-05-10-share-class-cik-uniqueness.md`.

## Canonical-instrument redirect (#819, settled 2026-05-11)

Operational-duplicate ticker variants (e.g. `AAPL` vs `AAPL.RTH`,
eToro's regular-trading-hours suffix) are stored as separate
`instruments` rows but represent the same security. SEC filings,
dividends, ownership, fundamentals all live under the base
instrument's CIK; the variant has no CIK row (cik_discovery resolves
to the underlying, the partial-unique CIK index in `sql/143` blocks a
second instrument from claiming the same CIK).

`instruments.canonical_instrument_id` (migration `sql/145`) is a
nullable FK to self:

- NULL = this row IS canonical (the default for every row).
- Non-NULL = this row is an operational duplicate; the FE should
  render the canonical row's page instead.

The redirect mechanic is **client-side `<Navigate replace>`** at
`InstrumentPage`'s mount, gated on `identity.canonical_symbol`
differing from the URL slug. Server-side 307 was rejected because
the per-stock research page hits ~20 endpoints; routing each through
a redirect layer is more surface area than a single FE check.
`useEffect`-based navigation was rejected because it flashes an
empty variant page before redirecting. The pattern mirrors the
existing `InstrumentDetailRedirect` shim.

CHECK constraint `instruments_canonical_not_self_chk` (in `sql/145`)
rules out self-loops at the DB layer — guards the FE redirect from
infinite-loop on a programming bug.

**Scope clarification:** `canonical_instrument_id` is for
**operational duplicates only** (.RTH and any future similar suffix
variants). **Share-class siblings** (GOOG/GOOGL, BRK.A/BRK.B) MUST
NOT use this mechanism — those are distinct securities (distinct
CUSIPs) that legitimately share an issuer CIK. See "CIK = entity,
CUSIP = security (#1102)" above.

Population: `populate_canonical_redirects_job` (registered job,
idempotent). Operator triggers after a universe sync introduces new
`.RTH`-style variants. Match rule:

- Variant symbol ends in `.RTH` (case-insensitive).
- Base symbol == variant minus suffix.
- Base lives on a DIFFERENT exchange (RTH variants live on eToro's
  operational-duplicate exchange).
- Single base, OR exactly one with `is_primary_listing=TRUE`.
- Multi-primary-listing matches are skipped with a warning; operator
  hand-binds via `UPDATE instruments` from the runbook.

**Spec:** issue #819 + `sql/145_canonical_instrument_id.sql` header.

## Maintenance rule

When a new repo-level decision is agreed and is likely to affect future implementation:
- add it here
- keep it short
- keep it concrete
- remove or update stale decisions when they no longer apply
