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

### Source priority for fund metadata (#1171)
- Within `(instrument_id, period_end)`, the winning N-CSR / N-CSRS observation is selected by:
  `ORDER BY period_end DESC, filed_at DESC, source_accession DESC LIMIT 1` (filter `known_to IS NULL`).
- N-CSR (annual, fiscal-year-end) and N-CSRS (semi-annual, mid-year) have disjoint `period_end` values per SEC rule §31a-29 so they do NOT compete at the same period_end.
- At the same period_end, amendments (N-CSR/A, N-CSRS/A) naturally win because they are filed later than the original.
- `source_accession DESC` is the final deterministic tie-break for unlikely same-filed_at collisions.
- Parser-version bump is orthogonal — rewash flows through `known_to` supersession (immutable observations + soft-delete) and the priority chain re-evaluates against the new currently-valid row set.
- **Scope:** applies to `fund_metadata_observations → fund_metadata_current` only. Does NOT apply to holdings (N-CSR holdings are not ingested; spike #918 §10.5 stands).

### Filing dedupe
- filing identity is provider-scoped
- provider filing identity must be stable and idempotent

### Fundamentals snapshot semantics
- `as_of_date` means financial statement period end date
- it does not mean fetch time
- when combining TTM + balance-sheet values, use the balance-sheet period end as the canonical snapshot date in v1

### Raw-payload retention (#1617, settled 2026-06-13)
- A stored raw filing payload (`filing_raw_documents.payload`) is legitimate only if its retention is justified by exactly one of three classes:
  - **re-read** — a rewash parser reads the stored body. Registered in `rewash_filings.registered_specs()`.
  - **housekept-and-negligible** — born-compacted at source (payload NULL + `payload_sha256` + `payload_swept_at`, rehydratable from `source_url`). Listed in `raw_filings.SWEPT_DOCUMENT_KINDS` (#1615).
  - **kept-and-negligible** — small, write-only, no payload reader; kept uncompacted with an explicit justification. Listed in `raw_filings.KEPT_NEGLIGIBLE_DOCUMENT_KINDS` (kind → reason).
- Every `raw_filings.DocumentKind` member MUST fall into exactly one class. The partition is CI-enforced by `tests/test_raw_payload_retention.py::test_every_document_kind_is_classified` — a new write-only kind fails CI until an operator deliberately buckets it (grep its payload readers first; existence-only `COUNT(*)` diagnostics do not count as a re-read).
- Adding a rewash parser for a kind currently in `KEPT_NEGLIGIBLE_DOCUMENT_KINDS` (e.g. a future Form 5 parser) MUST remove it from that map in the same change — the pairwise-disjoint test fails if it lands in two classes.

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
Use capped context in v2 (#1987):
- latest 1 prior thesis
- latest 3 filing events
- latest snapshot + up to 4 prior fundamental snapshots
- latest 10 news items from the last 30 days
- risk-evidence block (#1632): instrument_risk_metrics_current scalars, statused, as-of-stamped
- price anchor (#1987): latest price_daily close (native currency) + 52w range + persisted returns
- valuation block (#1987): instrument_valuation row when present; structurally-absent otherwise
  (quotes-gated view — absence is statused, not an error)
- analytics evidence (#1987): latest scores.analytics_json, shaped compact, scored_at-stamped
- TA state (#1987): latest price_daily indicator columns + derived sma-cross/price-vs-200d signals

All blocks follow the #1632 evidence discipline: statuses verbatim, as-of stamps, missing data
stays missing. Context-shape changes bump `_PROMPT_VERSION`.

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
- the realized-risk penalty (#1633, v1.2) uses tiered ADDITIVE deductions (high realized
  vol / deep drawdown), not multiplicative; thresholds are explicit constants calibrated
  to the universe tail, applied identically every run — NOT cohort-relative normalization
  (which stays banned)
- market-beta-vs-SPY is deliberately excluded from the risk penalty (full-population scan:
  r²≥0.30 for only ~3.4% of instruments → noise for this universe)
- return-ratio reward (Calmar) SHIPPED in scoring v1.3 (#1635 / #1633-vnext): the
  SEC-derived total-return series (price + reinvested per-share dividends from
  financial_facts_raw) feeds a 3y tr_calmar, and v1.3 adds an additive, mode-scaled
  Calmar REWARD gated on tr_status ∈ {ok, no_dividends} (tr_incomplete falls back to
  price-return Calmar + caveat). Thresholds calibrated to the universe tr_calmar tail
  (p75/p90). The earlier "Calmar excluded pending #1635" deferral is superseded.

### Score auditability
- each score row should carry enough detail to explain how it was produced
- rank and rank delta belong with the score row in v1
- no separate rankings table in v1

### Model versioning
- `model_version` includes the scoring mode
- default scoring mode is `v1.2-balanced` (#1633): v1.1 TA-enhanced momentum + an
  additive realized-risk penalty (high realized vol / deep drawdown, from risk_v1
  3y metrics). v1.2 keeps v1.1's family weights and TA momentum unchanged; the only
  difference is the penalty block, so v1/v1.1 score history is preserved (append-only;
  rank_delta compares only within a model_version)

### Rank delta comparison
- compare rank delta only against the most recent prior run using the same model version / mode

### Risk-metrics evidence layer (#591, #1674)
- `instrument_risk_metrics` is a DISPLAY/EVIDENCE layer (RiskPage + thesis evidence),
  versioned by `metric_version` (`risk_v1`). It is NOT a scoring input — sector-relative
  beta/excess (#1674), like SPY beta, is evidence-only; adding either to the scoring
  penalty would need its own full-population r² justification + sign-off (sector full-pop:
  median r²≈SPY's, but 13.6% of names clear r²≥0.30 vs 3.2% for SPY — strong tail, weak
  median).
- **Additive-nullable evidence under a stable `metric_version` is blessed — do NOT bump the
  version to add new evidence columns.** New nullable metric columns (e.g. #1674 `sector_*`)
  land under the SAME `risk_v1`: existing metrics are byte-identical, so bumping would force
  a full-universe recompute of unchanged data and orphan the append-only history. Pre-#1674
  rows keep NULL; the per-metric nullable `*_status` distinguishes "not computed then"
  (status NULL) from "computed, no benchmark" (`benchmark_missing`). Bump the version only
  when an EXISTING metric's computation changes.

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
[`docs/_archive/2026-04-18-product-visibility-pivot.md`](superpowers/plans/2026-04-18-product-visibility-pivot.md).

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
- **Spec:** `docs/proposals/ui/admin-control-hub-rewrite.md`
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
Issue #1094 design discussion).

`canonical_instrument_id` (#819) is a **different** mechanism for `.RTH`
operational duplicates — same security, two ticker variants. Don't
conflate. See "Canonical-instrument redirect (#819, settled 2026-05-11)"
below for the operational-duplicate redirect semantics.

- **PR-A:** sql/143 migration + filings.py upsert + ON CONFLICT predicate
  sweep across ~25 production + test sites + `tests/test_upsert_cik_mapping.py`
  flips.
- **PR-B (landed 65660911, PR #1118):** fan-out CIK→instrument multimap
  in `sec_companyfacts_ingest.py`, `sec_submissions_ingest.py`,
  `sec_insider_dataset_ingest.py` so share-class siblings BOTH receive
  bulk-ingest data. Per-filing manifest parsers fan out via
  `app/services/manifest_parsers/_siblings.py::resolve_siblings` /
  `app/services/sec_identity.py::siblings_for_issuer_cik` (PR #1152
  onward). Data ingested BEFORE PR-B stays single-sibling until a
  scoped `sec_rebuild` re-ingest — re-run it when a sibling shows
  missing per-source data despite a bound CIK.

**Spec:** `docs/proposals/etl/share-class-cik-uniqueness.md`.

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

## Universal bootstrap-state gate (#1064 PR1b-2, settled 2026-05-09)

`check_bootstrap_state_gate` at `app/services/processes/bootstrap_gate.py`
is the install-state gate that runs BEFORE any per-job
`ScheduledJob.prerequisite` in three call sites:

- `app/jobs/runtime.py::JobRuntime._wrap_invoker` (scheduled fire).
- `app/jobs/runtime.py::JobRuntime._run_catchup` (boot catch-up loop).
- `app/jobs/listener.py::_dispatch` (manual-queue).

The gate blocks every job whose registered `ScheduledJob` is not
exempt while `bootstrap_state.status != 'complete'`. On block, the
operator-visible reason `bootstrap_not_complete` is what the operator
sees + can fix (retry/iterate bootstrap from admin).

**Override semantics:**

- Scheduled fires + catch-up: NEVER override. There is no operator
  at the keyboard for a cron tick.
- Manual-queue dispatch: override via the
  `{control:{override_bootstrap_gate:true}}` envelope. On override, a
  `decision_audit` row with `stage='bootstrap_gate_override'` records
  the bypass with the operator id.
- Bootstrap-internal jobs (`bootstrap_orchestrator` + its stage jobs)
  are NOT registered in `SCHEDULED_JOBS`, so they bypass the gate
  unconditionally — the orchestrator MUST be able to run while
  `bootstrap_state.status='running'` or it would deadlock itself.

Adding a new `ScheduledJob` is to opt-in to the gate by default.
Opting out requires the carve-out below.

## Safety-net catch-up gate carve-out (#1181, settled 2026-05-16)

A `ScheduledJob` may set `exempt_from_universal_bootstrap_gate=True`
to bypass the universal gate above on ALL three dispatch paths.
Exempt jobs are an "unaudited design bypass" — no `decision_audit`
row is written; the static registry allow-list is the audit trail.

**Eligibility (enforced by `tests/test_universal_gate_carve_out.py`
allow-list + invariant assertions):**

1. `catch_up_on_boot=True` AND ONE of two admissible motivations:
   (a) safety-net where a missed cadence window is lost forever
   (the boot-time-only `catch_up` evaluation trap — a prereq-blocked
   catch-up cannot re-fire when bootstrap completes later); or
   (b) operator-visible live state that bootstrap would otherwise hide
   for hours (#1435 — broker portfolio / FX). Recoverable per cadence,
   but blanking the dashboard for the whole bootstrap is unacceptable;
   `catch_up_on_boot=True` fires it at boot, not the first tick.
2. `prerequisite is None` — carve-out rests on body-safe-against-
   empty-DB; a non-None prereq creates two opinions on the same
   install-state question.
3. Body is empty-DB safe (natural no-op against an empty/partial DB,
   no destructive write, no expensive fetch loop). For composite
   layer-runners (b), each constituent layer must itself self-skip
   cleanly until its own init/credential preconditions hold.
4. Bounded cost per fire (single-digit MB fetch max).

**Current carve-out members:**

- `sec_daily_index_reconcile` (motivation a) — Layer 2 of the #863-#873
  ETL freshness redesign. Daily 04:00 UTC reads yesterday's ~1MB
  daily-index master.idx; `subject_resolver` filters every unknown CIK
  so an empty/partial-bootstrap universe is a natural no-op. Missed
  cadence = lost-forever reconcile.
- `orchestrator_high_frequency_sync` (motivation b, #1435) — every-5-min
  `portfolio_sync` + `fx_rates` refresh. `portfolio_sync` carries
  `requires_layer_initialized=("universe",)`, so the executor cleanly
  `PREREQ_SKIP`s it (FK-safe once `nightly_universe_sync` commits the
  universe transaction atomically; otherwise skipped) until the universe
  stage lands early in bootstrap; `fx_rates` is independent. `prerequisite
  =None`; bounded cost (broker positions + FX). FK safety stays with the
  layer-init gate, NOT the universal bootstrap gate — so the dashboard
  populates as soon as `universe` is initialized instead of after the
  full multi-hour SEC bootstrap. NOTE: this job writes `sync_runs`, not
  `job_runs`, so the job_runs-based boot catch-up always treats it as
  never-run — `catch_up_on_boot=True` therefore fires it on every
  controller boot (intended for motivation b: populate dashboard at boot;
  bounded cost). The layer-init guard fails CLOSED on a transient
  init-check DB error (#1442): a DB failure during the check treats the
  layer as not-yet-initialized → `PREREQ_SKIP`, so it never runs before
  its FK dependency is proven visible; it retries on the next dispatch.

**Adding a new carve-out requires:**

- New spec entry + Codex 1a-equivalent review.
- Update to the allow-list assertion in
  `tests/test_universal_gate_carve_out.py::test_exempt_allowlist_is_explicit`.
- Update to this settled-decisions section.

Unilateral flag-flip is mechanically forbidden by the CI invariant
test.

## OpenFIGI as approved external CUSIP-resolver fallback (2026-05-22)

**Decision:** OpenFIGI v3 API at `https://api.openfigi.com/v3/mapping`
is approved as a CUSIP-resolution fallback for the eBull universe.
Lands as PR-0 (empirical probe + fixtures + skill) of issue #1233
ahead of the PR-1b resolver wiring.

**Constraints:**

- Free tier: 25 req/min unkeyed × max 10 jobs/POST = 250 mappings/min.
- Keyed tier: 25 req/6s × max 100 jobs/POST = 25,000 mappings/min.
- Operator-keyed mode requires `OPENFIGI_API_KEY` env var; default is unkeyed.
- The response **does not contain CUSIP**. Approved usage is
  CUSIP→ticker (`idType=ID_CUSIP, idValue=<cusip>`); the response
  includes ticker which resolves against `instruments.symbol` via
  the parallel-array contract (`zip(request, response, strict=True)`).
- Forbidden: ticker→CUSIP flow (response shape does not return CUSIP).
- Forbidden: scheduling OpenFIGI calls on the `sec_rate` Lane — PR-1b
  introduces a dedicated `openfigi` Lane (see spec v3 §5) so OpenFIGI
  throughput does not cannibalise SEC's 10 req/s shared budget.

**Empirical findings (probed 2026-05-22, see PR-0):**

- 429 body is plain text (`"Too many requests, please try again later."`),
  NOT JSON. Resolvers MUST branch on `status_code == 429` before
  attempting `.json()`.
- IETF draft RateLimit headers (`ratelimit-limit`, `ratelimit-remaining`,
  `ratelimit-reset`, `ratelimit-policy`) are emitted on EVERY response;
  `retry-after` is emitted on 429.
- Per-row "not found" entry shape is `{"warning": "..."}` — single
  key, no `error` key for unknown CUSIPs.
- Successful `data` arrays can be large (AAPL: 255 entries across
  cross-listings); always filter to `exchCode='US' AND securityType='Common Stock'`
  before binding to `instruments.symbol`.

**ToS posture:** OpenFIGI free tier permits programmatic use within
rate limits. Operator approved 2026-05-22 prior to PR-1b merge.

**Fixtures + skill:** `tests/fixtures/openfigi/`,
`.claude/skills/data-sources/openfigi.md`.

---

## Bulk archive reuse keyed on SEC ETag + SHA-256 (2026-05-22)

**Decision:** The Codex review BLOCKING for #1020 prohibited reusing a
prior-run `.zip`. With SEC's stable S3-backed ETag, reuse is permitted
when ALL of:
(1) local `.zip.etag` sidecar matches SEC's HEAD response,
(2) SHA-256 of local file matches `.zip.sha256` sidecar.
The run-manifest records `reuse_reason: 'etag_match_sha256_verified'`.
Forced override: `BOOTSTRAP_FORCE_REDOWNLOAD=1` env var.
**Empirical:** SEC ignores `If-None-Match` / `If-Modified-Since` (both
return 200 + full body regardless, probed 2026-05-22). Reuse therefore
uses client-side header comparison: HEAD → compare ETag → conditional
GET. The run-manifest is stamped fresh every run (download OR reuse),
so `assert_archive_belongs_to_run` still gates Phase C on current
`bootstrap_run_id` provenance regardless of reuse path.

---

## ETF identity = series/class; trust CIK never stamped (#1577, settled 2026-06-11)

ETF/mutual-fund instruments do NOT get a `(sec, cik)` row in
`external_identifiers` — their SEC tickers live in
`company_tickers_mf.json` keyed to the **trust** CIK (iShares Trust
`0001100663` alone covers ~300 of our ETFs), and stamping that
through the #1102 shared-CIK mechanism would explode parse-time
sibling fan-out ×300 and mis-route subject resolution (single-winner
`default_subject_resolver`). The #1102 indexes were sized for ~10
share-class siblings.

Instead, ETF identity flows through the **series/class mechanism**:
`(sec, class_id)` rows in `external_identifiers` +
`cik_refresh_mf_directory` (sql/149), which stores `trust_cik` per
class row for any consumer that needs it. N-CSR proves the pattern:
it walks trust CIKs from the directory and resolves
class_id → instrument before any instrument write. N-PORT keys
holdings by `fund_series_id`. The instrument-level CIK gap for ETFs
is **by-design** — `cik_coverage_audit` buckets them
`fund_series_covered` (primary class_id + directory join, both
load-bearing) so they don't pollute the actionable gap list.

Caveat: `cik_refresh_mf_directory` has observed-ever semantics —
future trust-CIK consumers must apply a freshness predicate
(`last_seen`).

**Tripwire for the entities layer (#1102 Option B):** file it when a
trust-CIK-keyed consumer cannot resolve to series/class before
instrument writes (the N-CSR pattern stops fitting), or when a
durable entity-level trust page/join is needed. Trust-level content
per se is NOT the tripwire. No half-step through non-primary
trust-CIK rows.

**Spec:** `docs/proposals/etl/2026-06-11-etf-trust-cik-design.md`.

---

## Ownership coverage banner = 5-state server-driven machine (#840, reaffirmed #923 2026-06-11)

The coverage banner renders the backend's 5-state machine — `no_data` /
`red` / `unknown_universe` / `amber` / `green`
(`app/services/ownership_rollup.py::CoverageState`) — with headline,
body, and color variant all SERVER-owned (`_banner_for_state`) and
rendered verbatim by `OwnershipCoverageBanner.tsx`. The frontend adds
only a per-state glyph (disambiguates `no_data` vs `red`, which share
`variant="error"`).

The Phase-1 6-state vocabulary (`partial_identifier_coverage`,
`stale_category`, `issuer_does_not_disclose`,
`complete_source_universe`) that issue #923 cited is **superseded** —
it was never implemented; #840 shipped this machine instead, and its
Codex review separately pinned the coverage-vs-concentration split.
Do not re-litigate from the old spec. Adding a coverage state is a
backend change first (new spec + ticket), never an FE-only remap.

**Spec:** `docs/specs/ui/2026-06-11-ownership-coverage-banner-v2.md`.

---

## Own EOD NAV-snapshot table (#1594, settled 2026-06-13)

eBull persists its own daily portfolio-equity snapshot (`portfolio_eod_snapshots`
+ `portfolio_eod_position_snapshots`, sql/196) plus a dated FX table
(`fx_rates_daily`). This **reverses the earlier informal "no NAV snapshot table"
posture** (#393, ~2026-04-21): operator-approved via the 2026-06-12 reporting
roadmap, and forced by empirics — eToro's `/api/v1/balances/history` returns
`403 InsufficientPermissions` on the demo key (#1593 step 1), so an external
equity-history source is unavailable.

Rules:
- Snapshot is **forward-only capture**, not reconstruction: the daily job records
  current `broker_positions × price_daily.close` + `cash_ledger` SUM in display
  currency, stamped to the latest closed session (`MAX(price_daily.price_date)`,
  data-anchored — NOT wall-clock). Idempotent (`ON CONFLICT (snapshot_date)`).
- Per-day FX lives in `fx_rates_daily` (USD-base ECB rows from Frankfurter
  time-series), **distinct from the tax `fx_rates` table** (sql/013) — dropping
  ECB rows into the tax table would silently change the safety-critical
  USD tax-disposal path (`tax_ledger._load_fx_rate`). Mirrors the live
  `live_fx_rates` USD-base convention so `fx.convert` (direct+inverse only) has
  parity.
- Own `db_eod_snapshot` JobLock lane (write-disjoint; #1527 starvation class).

**Spec:** `docs/proposals/etl/2026-06-13-portfolio-value-v2-fx-eod.md` (PR-A).

---

## Coverage tier → review_frequency assignment (#1996, settled 2026-07-16)

`coverage.review_frequency` is ASSIGNED from the coverage tier, single
mapping: **T1='weekly', T2='monthly', T3='monthly'**
(`TIER_REVIEW_FREQUENCY` in `app/services/coverage.py` — the only writer
source). Every path that sets `coverage_tier` (seed, bootstrap
gap-filler, promote/demote/override via `_apply_tier_change`) writes the
frequency in the same statement; sql/233 backfills pre-writer NULL rows.

Rationale: filing-event triggers (#273) cover real-news regen instantly;
the `review_frequency` age window is only a drift catch-all, and the
long-horizon posture (not day-trading) needs no daily rewrites. The
VALUE mapping (daily=1/weekly=7/monthly=30 days) was already settled;
this fixes the ASSIGNMENT. Matches the 2026-07-10 interim dev seed.

---

## Thesis staleness v2 thresholds (#1988, settled 2026-07-16)

Three structural data-driven regen triggers in `find_stale_instruments`
(additive; #273 semantics + existing reason order preserved; ordered
break_fired → price_move → band_exit → news_spike → cadence):

- **price_move: |move since mint| ≥ 0.30** — universe-derived
  (~5.7% 30d exceedance). **PROVISIONAL**: the 7-day-old corpus has a
  degenerate own distribution; MUST re-verify the actual fire rate
  ~30d post-ship (target ~2-8%/month; fvb R-retune precedent).
- **band_exit** — close outside [bear, bull] having minted INSIDE
  (arm-at-mint per #2012 Design 5; the 15/60 minted-outside class is
  premise and never fires). No state table: mint close is deterministic
  history.
- **news_spike: 7d importance-mass rate ≥ 3× prior-23d baseline rate
  AND 7d mass ≥ 2.0** — the absolute floor kills tiny-baseline ratio
  explosions; baseline-less names are not evaluated.
- **Price-input guards:** both closes > 0, latest close ≤ 10d old
  (#2012 price freshness bound) — else the price rules are NOT
  evaluated (#1632 NULL-never-0).

**Spec:** `docs/specs/thesis/2026-07-16-thesis-staleness-v2.md`.

---

## v1 strategy capital universe is US-only (#2605, settled 2026-08-12)

**A v1 strategy capital candidate is validated on, and eligible to hold capital
only in, the §4.0 validated universe: US listing venue + eToro `Stocks` type +
tradable.** Not issuer domicile, not quote currency — ADRs and US-listed foreign
private issuers are IN, a UK-listed issuer is OUT.

Already measured reality, recorded here because it was leaking implicitly: the
implicit version already produced one wrong "exhaustively tested" claim (#2597).

⚠ **"Eligible to hold capital" is a policy statement, not a live pre-trade
rule.** No order gate enforces it —
`app/services/strategies/validated_universe.py` says so itself (*"NO ORDER GATE
LIVES HERE"*; §4.0 puts the hard pre-trade rule in `execution_guard`, which is
phase 7 and unbuilt). What is enforced today is the evidence side, below.

**Where it binds — and where it does not.** ⚠ The two halves are different
paths and only the first is enforced today:

- **Result production — enforced.** `check_promotable`
  (`app/services/strategy_result.py:850`) refuses
  `instrument_outside_validated_universe` when
  `evaluated_instrument_ids - validated_universe_ids` is non-empty, and the sole
  writer of `strategy_results_store` fills that set from
  `load_validated_universe` (`app/services/backtest_run.py:1204` →
  `_Corpus.universe` → the candidate at `:2837`). The scope definition itself is
  `app/services/strategies/validated_universe.py`, pinned by
  `tests/test_validated_universe.py`.
- **The promotion transition — enforced since #2621 (2026-08-12).**
  `run_backtest` freezes each result's universe inputs in
  `strategy_result_universe` (evaluated ids + the validated universe as the run
  loaded it, immutable + hashed, written in the pair's own transaction), and
  `promote_strategy` replays `evaluated ⊆ validated` from that record for every
  pinned result at the evidence stages. A pinned result without a record —
  every pre-#2621 row, and anything a non-`run_backtest` writer inserts —
  refuses `evaluated_universe_unrecorded`. ⚠ The replay is against the universe
  FROZEN AT RESULT TIME, deliberately: today's `is_tradable` would let a later
  delisting retroactively invalidate a passing result, and the order-time rule
  against the CURRENT universe is the execution guard's (phase 7), not
  promotion's. The decision and its reasons live in
  `app/services/strategy_result_universe.py`'s module docstring.

**Why the restriction exists.** Every survivorship-free price source found *so
far* is US-only, and Form 25 delisting evidence is US-only
(`.claude/skills/data-sources/research-price-corpus.md`, whose measured landscape
is dated #2284 2026-08-05 / #2346 2026-08-07). Non-US tradable instruments
therefore cannot currently be validated survivorship-free at all. ⚠ That is a
claim about the *searched set* on those dates, not about the world — which is
why lifting the restriction is a data event, below, and not a re-argument.

⚠ The venue axis is eToro's `exchanges.asset_class`, a provider-maintained
classification with no foreign key behind it — §4.0's "necessary, not
sufficient" warning applies to the venue half as much as the type half.

**Reproduce the population — do not quote a figure from this file.** These move
with every `sync_universe` run, and `us_equity` tradable is a WIDER set than the
validated universe (the script prints the type split that accounts for the
difference; conflating the two sets is the prevention-log entry on §5.1's M23).
The script also ASSERTS, and exits non-zero on, the two properties this decision
leans on — that the venue axis is coherent, and that the validated universe is
uniformly USD-quoted:

```bash
PYTHONPATH=. uv run python scripts/measure_2605_universe_scope.py
```

Out of scope for the restriction:

- **core allocation (#2603)** — the core instrument is a mandate/eligibility
  question, not a strategy-validation one. A non-US-listed core instrument is
  permitted if its eligibility proof passes.
- **advisory/manual surfaces** (v1.5 scoring → thesis → portfolio manager) —
  unchanged, because the strategy/mandate path is the sole autonomous executor
  (#2437's 2026-08-12 requirements comment, decision 1). That requirement is
  cited here, not established here.

**Lifting requires ALL of** a survivorship-free non-US source, its licence
review, and broker eligibility/FX/tax review for the venue. A data event AND a
review event, not either alone.

**Spec:** `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §4.0
(#2289). Refs #2437, #2597, #2363.

---

## Live-gate evidence windows are single-entry (#2612, settled 2026-08-13)

**A `(strategy_id, strategy_version)` pair arrives at `forward_observation` at
most once, and at `paper_enabled` at most once.** The forward-evidence window and
the paper window therefore each have exactly one start, and the
splice-versus-accumulate question #2612 raised **does not currently exist** —
there is no second arrival to splice from.

⚠ **This is the rule, in place of the one #2612 asked for.** The ticket asked us
to choose splice or accumulate and implement it. Implementing either would have
encoded "re-entry is expected" into a lifecycle that forbids it, and left dead
policy guarding an unreachable state.

**Where it is enforced — two independent barriers, both required:**

- `app/services/strategy_control_plane.py::_NEXT_STAGE` is a DAG with no
  back-edge. Only `historical_validated` has an edge into `forward_observation`,
  only `forward_observation` has one into `paper_enabled`, and the exits run
  `paused` → `retired` → nothing. `promote_strategy` checks it under the
  per-version advisory lock before any evidence work.
- The partial UNIQUE index `idx_strategy_promotions_one_successor` on
  `(strategy_id, strategy_version, from_stage)`
  (`sql/281_strategy_promotion_ownership.sql:46`) — each stage may be departed
  exactly once, so the bound holds even when the service check is bypassed. It
  routinely is: five test modules INSERT into `strategy_promotions` directly.

**What reads it.** `assess_live_gate` anchors both windows on
`max(promoted_at) FILTER (WHERE to_stage=...)`, which is the true window start
only under this rule. `strategy_live_gate` cannot see `_NEXT_STAGE`, so the
coupling is held by `LiveGateFacts.forward_observation_entries` /
`paper_enabled_entries` (counted in the same scan), the fail-closed refusals
`forward_window_ambiguous` / `paper_window_ambiguous`, and a pure coupling guard
in `tests/test_2612_forward_window_single_entry.py`.

**Changing this is a real decision, not a refactor.** Adding a re-entry edge
(`paused` → `forward_observation` is the plausible one) makes the
splice-versus-accumulate question live for the first time. The guard test fails
the moment such an edge is added and names `assess_live_gate` as the thing to
revisit; #2599's contract-frozen forward-shadow floor reads `forward_days` and
`forward_decision_dates` off these windows, so the choice binds capital
authority. Decide it there, and update this entry.

**Blast radius when settled:** `strategy_promotions` held **0 rows** on dev
(measured 2026-08-13) — no strategy has ever been promoted, so nothing existing
depends on either reading.

**Refs** #2612, #2599, #2621.

---

## The promotion transition's replay policy (#2625, settled 2026-08-13; completed by #2639)

`check_promotable` runs at RESULT PRODUCTION. `promote_strategy` cannot call it
— it has no `StrategyResult` to hand it, and two of the gate's inputs cost an
audited read — so it re-derives each input individually from a persisted record.
**Which input replays against what is now fixed, and keyed on
`PromotionCandidate`'s FIELDS rather than on refusal codes** (codes are a
many-to-many projection of the inputs, so a code-keyed rule is satisfiable while
an input goes unclassified).

Three rules, and no fourth:

- **`frozen`** — replayed from a record written at result time, never re-derived
  from today's world. The default. Covers the universe (#2621), the §3.4
  ambiguity comparison (#2625) and the row's own structural stamps.
- **`today`** — re-evaluated against the current world, and legitimate **ONLY**
  in one of **three declared shapes**. A today-check outside them is an
  undeclared freshness rule — the reason #2621 froze the universe instead of
  re-loading it. **A test pins the exact member set**, so a fourth cannot be
  added quietly.
  1. the record DECLARES its own validity window — `promotion_evidence`, whose
     `cost_observed_on` / `cost_valid_through` say when executable costs go stale;
  2. the record is explicitly supersedable;
  3. **(#2639)** the record is an append-only AUDIT LOG, the clause is a
     comparison between that log and the rows it audits, and the criterion the
     clause serves requires the comparison to be CURRENT — criterion 5's
     `holdout_evaluations` / `recorded_accesses` against
     `strategy_holdout_accesses`. ⚠ Worded this narrowly on purpose: the draft
     form "a ledger of our own conduct" would admit any mutable operational
     counter, which is most of the database.
- **`not_re_read`** — neither persisted nor re-derived. ⚠⚠ **THIS NAMES A GAP,
  NOT COVERAGE.** The transition does not enforce that clause at all and still
  trusts a write-time verdict that died with `WrittenRow`. ⚠ **The set is EMPTY
  since #2639** and `unenforced_candidate_fields()` returns `frozenset()`; the
  rule stays in the vocabulary so the next unclassifiable input has an honest
  label to land on, and the assertion stays so a newly-classified-but-unwired
  input fails a test rather than arriving as a clause nobody applies.

The policy lives in `app/services/strategy_promotion_replay.py` with a reason on
every entry. **Do not add an input to the transition without classifying it
there** — `tests/test_strategy_promotion_replay.py` fails on an unclassified
`PromotionCandidate` field, in both inclusion directions.

⚠ **`replayed_at_transition` is a separate axis from the rule, and the two must
not be conflated.** "This input's rule is frozen" and "the transition checks it"
are different claims; every gap is a field where the first is true and the
second is false. Before #2625, `grep` for
`universe_basis|carry_unmodelled|fx_unmodelled` in `strategy_control_plane.py`
returned **nothing** — the stamps were persisted and never read, so Tier 1's
refusals could all close and promotion still would not consult them.

⚠ Why not just replay the whole gate: the gate has no single as-of — most
inputs are frozen and three are today — which one `check_promotable(candidate)`
call cannot express. #2639 does rebuild the row through
`result_ledger._result_from_row`, but behind
`stored_result_promotion_refusals`, which returns **refusal codes and never a
`StrategyResult`**: a public `load_result_by_id` would be a new unaudited door
to the withheld side, and 300 of the 324 stored results are `hold_out`.
`read_holdout_results` stays the sanctioned door and still records first.

### What #2639 added (2026-08-13)

- **Criterion 5's two counts replay against TODAY**, because **frozen defeats
  the criterion**: both counts are scoped to `(strategy_id, strategy_version)`,
  so a pair frozen at result time is blind to a later unrecorded look at the
  same version's hold-out — which is the leak criterion 5 exists to catch. The
  clause is strategy-version-wide (one unrecorded evaluation blocks every result
  of that version) and it heals as well as blocks. `holdout_access_counts` is
  now ONE statement with two scalar subqueries, so the pair comes from one
  snapshot. ⚠ It is **not** atomic with the promotion INSERT — the hold-out
  writers do not take `promote_strategy`'s advisory lock — and that bound is
  stated rather than assumed away.
- **Criterion 9's arm pair is RE-DERIVED from the identity hash**, not recorded.
  `result_ledger.quarantine_arm_pair_present` does the count and records
  nothing; `quarantine_arms_compared` records and then calls it, so the door
  that writes a `read` access stays the one criterion 5 governs and the
  transition does not write into the log it is auditing. ⚠ **A stored
  `sibling_result_id` pointer was the first design and was killed at Codex
  checkpoint 1**: a pointer is chosen by the writer and can name a compatible
  row that is not the one the identity admits, whereas
  `ResultIdentity.version` is a hash and admits exactly one sibling.
- **The row's own purpose, deflation, effective-sample-size and §9 clauses**
  replay through four pure functions — `purpose_promotion_refusals`,
  `holdout_count_promotion_refusals`, `deflation_promotion_refusals`,
  `synthetic_control_promotion_refusals` — which are the copies
  `check_promotable` itself calls, the `structural_promotion_refusals` move.
- ⚠ **The row's own `purpose` was a latent gap.** `promote_strategy` refuses on
  `registered_strategy_purpose` — the MANIFEST's — and never compared it to the
  row's stamp. Measured 2026-08-13: all 324 stored rows and all four registered
  strategies are `harness_validation`, so they agree today; the moment a
  manifest entry becomes `capital_candidate`, its older harness-stamped rows
  become pinnable. Same M9 shape — the control exists on a path the decision
  does not take.
- ⚠ **`result` stays `frozen`.** `trial_register_superseded` compares a frozen
  column against the CURRENT `TRIAL_REGISTER` constant; a frozen-field-versus-
  constant comparison does not make a field `today` (what makes
  `promotion_evidence` today is the current DATE).
- ⚠ **The transition keeps its OWN read of the structural stamps.**
  `_result_from_row` coerces `carry_unmodelled` with `bool(...)`, so a NULL
  would read as *modelled* — fail-open on a Tier 1 refusal — while the
  transition's read coerces NULL to `True`. Both columns are `NOT NULL`
  (`sql/262`, `sql/335`), so this is defence in depth; the two coercions must
  not be collapsed onto the weaker one.
- ⚠ **A corrupt row RAISES rather than refusing**, per `load_result_ambiguity`'s
  precedent, which aborts before the remaining refusals are gathered and so
  MASKS them. Verified on the full population: 324 of 324 rows reconstruct.

**Blast radius when settled:** `strategy_promotions` held **0 rows** and
`strategy_result_ambiguity` **0 rows** against 324 results (measured 2026-08-13),
so all 324 refuse `ambiguity_verdict_unrecorded` and nothing existing was
promotable to begin with.

**Blast radius of #2639**, measured on dev over the full population with
`PYTHONPATH=. uv run python scripts/verify_2639_promotion_replay.py --all`:
324 of 324 rows reconstruct; the new row-level census is 324
`harness_validation_only`, 324 `synthetic_control_not_run`, 204
`trial_register_superseded`, 56 `deflated_sharpe_not_computed`, 56
`trial_count_undeclared`, and **0 `quarantine_arms_not_compared`** — every row's
flipped-arm sibling is stored. **0 of 324 rows become less refused**, which is
the direction that matters: a replay closing a gap must only ever ADD refusals.
Criterion 5's clause passes on all eight `(strategy_id, strategy_version)` pairs
(evaluations equal accesses on every one).

**Refs** #2639, #2625, #2621, #2505, #2599, #2437.

---

## A refused outcome-access attempt is audited from a SEPARATE transaction (#2611)

`record_holdout_access` / `require_outcome_access` used to raise
`PreregDeclarationRefused` and write nothing. `sql/340`
(`strategy_holdout_access_refusals`) records the attempt; `_refuse_access` is the
single exit that writes it.

**The rule, and it is an asymmetry — not an inconsistency with #2599:**

- An **access** record is a claim about DATA. It stays in the caller's
  transaction, because `sql/264`'s trigger must see it alongside the hold-out row
  it authorises and a rolled-back evaluation did not happen.
  `record_holdout_access`'s docstring is unchanged and still correct.
- A **refusal** record is a claim about an ACT OF THE CALLER. It completes when
  the exception is constructed; the caller rolling back does not un-attempt it,
  and a caller that retries N times attempted N times. Postgres has no autonomous
  transaction, so it is written on a second connection — otherwise it would be
  lost in every case it exists for, the refusal being an exception.

**Consequences, each load-bearing and none of them cosmetic:**

- ⚠⚠ **Its own relation, never `strategy_holdout_accesses`.** That table is read
  as *looks that happened* by `holdout_access_counts` (criterion 5),
  `supersede_preregistration` (`supersession_trial_already_exposed`),
  `app/api/strategies.py`, `trial_register`, `scripts/sealed_rerun_gate.py` and
  `sql/264`'s own write trigger. A refusal row there would inflate criterion 5
  AND permanently strand the trial from #2634's repair over a look that returned
  nothing.
- ⚠⚠ **No advisory lock in the audit write, and no FK on `declaration_id`.**
  Measured 2026-08-13: `pg_advisory_xact_lock` **blocks across connections**.
  `record_holdout_access` holds the trial lock when it refuses, so an audit that
  took it would block until the caller's transaction ended. An FK is the same
  hazard quieter: it locks the parent row and cannot see a declaration the caller
  froze in its own open transaction.
- ⚠ **The audit DSN is derived from the caller's connection**
  (`make_conninfo(conn.info.dsn, password=conn.info.password)` — `dsn` strips the
  password, measured on psycopg 3.3.3), never from the process settings. The
  settings URL would write a DB test's refusal into the operator's dev database.
- ⚠ **Best effort, and it never masks the refusal.** A failed audit logs at ERROR
  **with the codes inline** and the refusal still raises. A gate that can be
  disabled by breaking its audit is not a gate.
- **Out of scope on purpose:** `freeze_preregistration` /
  `supersede_preregistration` refusals (attempts to WRITE a declaration, which
  open nothing), `_refuse_declared_stamp_substitution` (a result-write refusal),
  and `verify_outcome_access_provenance` (already requires a real `access_id`, so
  the attempt it re-checks is recorded). A `RuntimeError` from a malformed
  declaration chain also denies access and is deliberately NOT audited — this
  table records POLICY refusals, not every way a look can fail.

**Blast radius, measured on dev over the full population with
`PYTHONPATH=. uv run python scripts/verify_2611_refusal_audit.py`:** 8 trials,
304 access rows, 324 results (300 `hold_out`), **0 declarations** and 0 refusal
rows. All 8 trials refuse `preregistration_not_frozen` at `require_outcome_access`
and are permitted at `record_holdout_access` — **the same decision this branch and
`origin/main` both make.** The PR adds a row after the decision and changes no
decision.

**Refs** #2611, #2599, #2634, #2614, #2437.

---

## 2026-08-22 — Live-capital approval is a mandate FLAG, not a person-gate (operator, reversing the prior settled decision)

Operator, verbatim intent: *"I would want the 'check' on live capital to be a flag …
I want this in a place where it can run on its own based on the evidence it has …
I want a hands off system, not a regular check in."* Raised as a safety concern by the
agent; operator reaffirmed. The reversal is theirs to make and is made.

- Mandate field `approval_mode: manual | autonomous` (#2843). Default `manual`.
- Under `autonomous`, promotion and allocation execute when **every evidence bar**
  passes (frozen preregistration, deflation, forward-shadow floor, no structural
  refusals, sandbox invariant). The flag flips WHO approves — never WHAT qualifies.
- The execution guard is untouched: fail-closed, EXIT never blocked, kill switch live.
  "No silent bypass of failed checks" stands in full.
- Operator alerts become refusal-surfaces with a validity contract (one-sentence
  decision + complete evidence + recommendation + safe default). No routine check-ins.

⚠ **Where the flag actually landed, and what the person-gate actually was** (#2843,
implemented 2026-08-22). `approval_mode` is a column on `strategy_paper_pool_events`,
beside `capital_mode` and the `sql/311` mandate columns — the same table #2844 declined
to duplicate. Append-only, so the authority in force at any promotion is the latest
event at or before it.

The gate it replaces was **one place**: `advance_strategy` assembles all of its own
evidence and its only production caller was `POST /strategies/{id}/advance`, behind
`require_session`, stamping `advanced_by=session.username`. So the flag touches no gate.
It supplies a second caller — `app/services/strategy_autonomous_promotion.py`, run daily
by the `strategy_autonomous_promotion` job — which stamps
`promoted_by = policy@autonomy-v1` instead of a username.

| what | value |
| --- | --- |
| approver stamp | `AUTONOMOUS_APPROVER` = `policy@autonomy-v1` |
| actions the policy may take | exactly `_EVIDENCE_ACTIONS`, asserted by equality so the two cannot drift. `register_research_candidate` carries no evidence and stays manual |
| cycle-level refusals | `approval_mode_manual`, `mandate_unconfigured`, `paper_pool_disabled` |
| furthest stage reachable | `paper_enabled`. `live_enabled` is refused by `promote_strategy` itself |

⚠ **`approval_mode` omitted on `PUT /strategies/paper-pool` means UNCHANGED, not
`manual`** (`resolve_approval_mode`). Reading omission as a reset would let an unrelated
capital-limit edit silently revoke autonomy and return 200.

⚠ **No minimum stage-dwell constant was introduced, deliberately.** A draft proposed
one; it has no construction, because all six `RECENT_EVIDENCE_WINDOWS` end at or before
`INTRADER_CAPTURE_DATE` (2024-09-27) and the historical matrix therefore cannot change
with elapsed time. What bounds forward observation is
`prospective_assessment_predates_forward_observation`, which already existed. ⚠ That
rule permits an assessment computed one second after the promotion, so the effective
floor is one assessment-job cadence — pre-existing, identical on the manual path, and
noted on #2843 rather than fixed there.

## 2026-08-22 — The allocation boundary is the ONLY safety net (operator)

*"This won't be on the total pot … either an expanding pot or always limited to the
amount assigned. That is the only safety net I'm interested in."* Engine exposure is
bounded by `assigned_capital` (`capped`) or `assigned_capital` + cumulative realised
engine P&L from OUR ledger (`expanding`) — enforced at the execution guard with a named
refusal (`sandbox_exceeded`), reconciled against broker equity per #2602, never inferred
from broker balance (the account is shared with non-engine holdings). #2844.

⚠ **The operator's two words are already stored under different names, and no column was
added for them** (#2844, implemented 2026-08-22). `strategy_paper_pool_events.capital_mode`
predates this decision and its CHECK enumerates `('fixed', 'compound')`:

| operator's word | stored value |
| --- | --- |
| `capped` | `fixed` |
| `expanding` | `compound` |

`assigned_capital` is `strategy_paper_pool_events.capital_limit`, append-only and versioned
by the events table itself. Minting an `assigned_capital` / `capped|expanding` pair would
have created a **fourth** capital-limit surface — alongside this pool,
`strategy_deployments.capital_limit` and the core mandate's percentages — and a second
vocabulary for one concept. The bound arithmetic now lives once, in
`app/services/strategy_capital_sandbox.py::sandbox_bound`; it had been hand-written three
times (control plane, `/strategies` card, paper executor), so the panel promising the
operator headroom and the control enforcing it could drift apart with both internally
consistent. ⚠ The enforcement point is `strategy_paper_executor`, NOT
`execution_guard.evaluate_recommendation` — the latter serves the portfolio-manager path
and creates no funding decisions, so a bound check there would be a second, unsynchronised
opinion on a population it does not participate in.

## 2026-08-22 — Price-only steer: event-form strategy families are CUT (operator, twice affirmed)

Insider/13D/merger/PEAD/shock-event families closed with lessons and revisit conditions
(#2701 #2835 #2836 #2839 #2493 #2484 #2485 #2507). AUDITED periodic accounts
(10-K/10-Q fundamentals) remain in scope — the objection was event-TIMING forms, and
the valuation engine is blind without accounts (assumption recorded on #2832; if the
operator ever extends the steer to all filings data, #2842 dies honestly). The dead
eight TA strategies leave the manifest (#2845); s4/s8 remain as the substrate of the one
price-only research seat (#2840). The operative queue is the R5b comment on #2437.

## Maintenance rule

When a new repo-level decision is agreed and is likely to affect future implementation:
- add it here
- keep it short
- keep it concrete
- remove or update stale decisions when they no longer apply
