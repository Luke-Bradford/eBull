# Filings coverage + event-driven refresh cascade — master plan (v3)

**Covers issues:** #268 (filings coverage bar), #273 (thesis event-driven), #276 (dependency-driven rescoring)
**Depends on:** #272 (SEC incremental fetch — shipped)
**Date:** 2026-04-17
**Revision:** v3 — incorporates Codex corrections across three review rounds.

---

## Why this is a master plan, not a single spec

Initial attempt tried to pack both #268 and #273+#276 into two specs. Codex review caught 27 real issues across two rounds; several were architectural. Root cause: wrong mental model of the current repo. The corrected mapping below reflects verified HEAD state.

---

## Repo reality map (verified against HEAD 2026-04-17)

### Who writes `filing_events`?

`app/services/filings.py::refresh_filings` → `_upsert_filing` is the only writer. Called from `daily_research_refresh` (scheduler.py:1049 for SEC, 1073 for Companies House). `sec_incremental.execute_refresh` (shipped in #272) does NOT write `filing_events`; it writes `financial_facts_raw` + watermarks.

**Implication:** event-driven triggers querying `filing_events` for "new filings since last thesis" see filings only after `daily_research_refresh` runs — ~24h lag from accession to event-signal availability.

### Scoring persistence

`scoring.compute_rankings`:
1. Loads "eligible" set via `coverage` JOIN + "has at least some data" (thesis OR fundamentals OR price). No tier gate. **No `filings_status` gate today.**
2. Scores all eligible.
3. Sorts globally by `total_score`; assigns `rank` = position.
4. Reads prior ranks for same `model_version`, computes `rank_delta`.
5. Writes all rows inside one `with conn.transaction():`.

`rank` is a global ordinal — subset recompute cannot assign meaningful rank without the full pool. Any "rescore after cascade" must be full-pool.

### Normalization timing

`daily_financial_facts` body:
1. `plan_refresh` + `execute_refresh` → commits facts + watermarks.
2. If seeded + refreshed > 0: `normalize_financial_periods` for touched instrument_ids — uses nested `conn.transaction()` as a savepoint; does NOT itself commit. The outer `with psycopg.connect(...) as conn:` commits on context exit.
3. `tracker.row_count` set.

Cascade must run AFTER normalization AND AFTER an explicit `conn.commit()` in the scheduler body. Chunk K.1 adds this `conn.commit()` between normalization and the cascade call — otherwise cascade's fresh reads see an uncommitted write state and thesis context assembly races with the outer commit.

### `generate_thesis` transaction pattern

`thesis.py::generate_thesis`:
1. `_assemble_context(conn, ...)` — SELECTs. On non-autocommit connection this opens an implicit tx.
2. `_call_writer(client, context)` — Claude call (2-5s).
3. `_call_critic(client, ...)` — Claude call (2-5s).
4. `with conn.transaction(): _insert_thesis_atomic + _update_last_reviewed`.

The implicit tx from step 1 is NOT closed before steps 2-3. Docstring's "Claude calls are made outside the transaction" is misleading — only the explicit `with` is outside; the implicit one is inside.

### `daily_thesis_refresh` cohort

scheduler.py:1188 — iterates `find_stale_instruments(conn, tier)` for tier=1 then tier=2 only. Tier 3 and untiered instruments never processed by DTR.

### `coverage` row lifecycle

- `seed_coverage(conn)` (coverage.py:699) — bootstrap: inserts Tier 3 rows for all tradable instruments only if the `coverage` table is empty. Called from `nightly_universe_sync`.
- **Gap:** once `coverage` is non-empty, newly-added tradables do NOT get rows automatically. A `WHERE NOT EXISTS` insert is missing from the post-bootstrap path.
- Chunk B fixes this gap.

### `external_identifiers` primary semantics

Non-primary rows exist (e.g. historic CIKs after issuer CIK change). Queries MUST filter `is_primary = TRUE` to avoid double-counting or stale-mapping confusion.

### SEC fetch duplication

`daily_research_refresh` fetches filings (`refresh_filings` → `list_filings_by_identifier` → `submissions.json`). `daily_financial_facts` (#272) separately fetches `submissions.json` + `companyfacts.json` via its own provider instance. Two separate rate-limit budgets (different `shared_ts` lists). Redundant but not currently rate-limit-breaking. Dedupe deferred to Chunk L (separate ticket).

### Existing schedule slots

- `orchestrator_full_sync` — daily 03:00 UTC.
- `orchestrator_high_frequency_sync` — every 5 min.
- `weekly_coverage_review` — Monday 05:00 UTC.
- `attribution_summary` — Sunday 06:00 UTC.
- `execute_approved_orders` — daily 06:30 UTC.
- Morning candidate review — hourly on :00.
- Position monitor — hourly on :15.
- Retry deferred — hourly on :30.

Free slots for a new weekly job: Tuesday 04:00 UTC works (no collisions).

### `country_code` / non-US detection

`instruments` table does not reliably carry a `country_code` today. Primary SEC CIK presence is the closest-available US-issuer proxy. FPIs (20-F/40-F filers) DO have SEC CIKs despite being non-US. Correct modelling:

- **`no_primary_sec_cik`** — no `external_identifiers` row with provider=sec AND is_primary=TRUE. Can be UK issuer, crypto, ETF, etc.
- **`fpi`** — has SEC CIK, zero 10-K/10-Q, at least one 20-F / 40-F / 6-K → Foreign Private Issuer.
- **US domestic issuer** — has SEC CIK, filings include at least one 10-K or 10-Q.

No `filings_status` value named `non_us` because "non-US" isn't something we can prove from current data without a country column; use the above three concrete status values instead.

---

## Decomposition

### Decision log (resolved in this revision)

- **Scoring approach:** Option α — full-pool re-rank after cascade thesis refresh, once per `daily_financial_facts` cycle. Option β ("unranked subset scores") deferred to a separate future ticket if rank freshness becomes an operator pain point. Chunk J (filings_status gating) remains a separate ticket since it touches producers + consumers across multiple files.
- **FPI handling:** FPIs get their own `fpi` status. Event-driven thesis trigger (Chunk I) omits 20-F / 40-F / 6-K for v1 — they fall to #279's UK-equivalent-bar / international scope. FPIs therefore don't fire cascade thesis today.
- **New instrument time-to-analysable:** up to 7 days (weekly audit run picks up `unknown`-status rows). Hands-off operating model. Fast-lane queue is itself a tech-debt follow-up.
- **Weekly audit slot:** Tuesday 04:00 UTC.
- **Amendments (10-K/A, 10-Q/A, 8-K/A, 20-F/A, 40-F/A):** trigger cascade thesis refresh the same as their base forms. SEC incremental (#272) already treats amendments as fundamentals.

### Chunks

Size estimates include implementation + tests + PR polish. Not including review cycles.

#### Prerequisite chunks (unblock everything else)

**Chunk A — `execute_refresh` writes `filing_events` + plan carries full filing metadata** (~1.5 days)

- Enrich `RefreshPlan`: `refreshes` and `submissions_only_advances` carry FULL filing list per CIK, not just top accession. Shape: `list[tuple[cik, list[FilingMetadata]]]` where `FilingMetadata` includes `accession`, `form_type`, `filing_date`. `primary_document_url` is NOT in `MasterIndexEntry` today — derive it at upsert time from the canonical archive URL pattern: `https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dashes}/{accession}-index.htm`. No extra HTTP fetch needed.
- Planner already parses master-index (which has CIK, company, form, date, accession) — extend `plan_refresh` to accumulate ALL entries per covered CIK for the cycle, not just the top one.
- Executor upserts each new filing row into `filing_events` per-CIK via existing `filings._upsert_filing` (writes `provider='sec'`, `provider_filing_id=accession` per the existing uniqueness constraint). Inside the same transaction as watermark advance.
- Seed path: seeds have no master-index metadata (no hit → no entry). Seeds do NOT write `filing_events`; they rely on Chunk E's backfill to populate history.
- Handles multiple same-day filings per CIK (a 10-K + 8-K on the same day both get rows).

**Chunk B — `coverage` row bootstrap for newly-added instruments** (~0.5 day)

- Modify `nightly_universe_sync` to post-`seed_coverage`, also run `INSERT INTO coverage (instrument_id, coverage_tier) SELECT instrument_id, 3 FROM instruments i WHERE i.is_tradable = TRUE AND NOT EXISTS (SELECT 1 FROM coverage c WHERE c.instrument_id = i.instrument_id)`.
- One-time data migration in migration file 035: same statement, runs on first deploy to backfill any existing gaps.

**Chunk C — `generate_thesis` commits read tx before Claude** (~0.5 day)

- In `thesis.py::generate_thesis`, after `_assemble_context` returns, call `conn.commit()` to close the implicit read tx.
- Test: run `generate_thesis` against `ebull_test`, check `pg_stat_activity` during the (mocked) Claude call shows connection state != `idle in transaction`.
- Documentation: update docstring to reflect the explicit commit.

#### Coverage-bar track (#268)

**Chunk D — `filings_status` schema + audit v1** (~2 days)

- Migration `sql/035_coverage_filings_status.sql`:
  ```sql
  ALTER TABLE coverage ADD COLUMN filings_status TEXT
      CHECK (filings_status IN (
          'analysable',
          'insufficient',
          'fpi',
          'no_primary_sec_cik',
          'structurally_young',
          'unknown'
      ));
  ALTER TABLE coverage ADD COLUMN filings_audit_at TIMESTAMPTZ;
  ALTER TABLE coverage ADD COLUMN filings_backfill_attempts INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE coverage ADD COLUMN filings_backfill_last_at TIMESTAMPTZ;
  ALTER TABLE coverage ADD COLUMN filings_backfill_reason TEXT;
  -- Invariant: NULL filings_status MUST have NULL filings_audit_at.
  -- Enforced at runtime by audit service; no DB constraint to avoid
  -- blocking existing rows during migration.
  CREATE INDEX IF NOT EXISTS idx_coverage_filings_status ON coverage(filings_status);
  ```
  Plus the Chunk B coverage-row backfill.
- New service `app/services/coverage_audit.py`:
  - `audit_all_instruments(conn)`:
    - Single GROUP BY over filings, joined to cohort. Query filters `fe.provider = 'sec'` AND `ei.provider = 'sec'` AND `ei.identifier_type = 'cik'` AND `ei.is_primary = TRUE`. The `fe.provider = 'sec'` filter prevents Companies House filings from counting toward SEC form thresholds for instruments that carry both providers. Produces `(instrument_id, form_type, count, earliest_filing_date, latest_filing_date)`.
    - Single SELECT of cohort (tradable instruments + primary SEC CIK presence).
    - Per-instrument status derivation rules (applied in Python after pulling counts):
      - No primary SEC CIK → `no_primary_sec_cik`.
      - Has primary SEC CIK, zero 10-K, zero 10-Q, ≥1 of {20-F, 40-F, 6-K} → `fpi`.
      - Has primary SEC CIK, 10-K count (in last 3 years) ≥ 2 AND 10-Q count (in last 18 months) ≥ 4 → `analysable` (subject to 8-K gap check below).
      - Otherwise → `insufficient`. **Never `structurally_young` during initial audit** — our DB may only contain recent filings for a mature issuer; calling them "young" before backfill is a misclassification. `structurally_young` is assigned ONLY by the backfill service (Chunk E) after exhausting SEC's own `submissions.json` history AND confirming the issuer's earliest-ever SEC filing is <18 months old. Audit's output space is therefore `{analysable, insufficient, fpi, no_primary_sec_cik, unknown}` only; `structurally_young` is set by backfill.
    - 8-K gap check: audit does NOT do external-truth comparison (DB-only count is provably insufficient per Codex; full external check needs SEC submissions.json fetch which is Chunk E backfill territory). v1 audit marks `analysable` based on 10-K/10-Q alone; 8-K gaps detected during backfill pass (Chunk E), which updates status on return.
    - Bulk `UPDATE coverage SET filings_status = … FROM (VALUES …)` construct; one roundtrip.
    - Updates `filings_audit_at = NOW()` on every row touched.
  - `audit_instrument(conn, instrument_id)` — same rules scoped to one instrument.

**Chunk E — historical filings backfill + 8-K gap verification** (~2 days)

- New service `app/services/filings_backfill.py`:
  - `backfill_filings(conn, provider, cik, instrument_id) -> BackfillOutcome`.
  - Fetches `submissions.json` for the CIK (re-uses `SecFilingsProvider.fetch_submissions`).
  - Iterates `filings.files[]` pagination **recent-first** (most-recent page first). Upserts into `filing_events` via `_upsert_filing`. Stops once the bar is met OR all pages consumed.
  - 8-K gap check: compiles list of 8-K accessions from SEC's 365-day window, compares to our DB rows, fetches + inserts any missing.
  - Classifies reason on return: `COMPLETE_OK`, `STILL_INSUFFICIENT_STRUCTURALLY_YOUNG`, `STILL_INSUFFICIENT_HTTP_ERROR`, `STILL_INSUFFICIENT_PARSE_ERROR`, `SKIPPED_ATTEMPTS_CAP`, `SKIPPED_BACKOFF_WINDOW`.
- Retry management:
  - `coverage.filings_backfill_attempts` cap at 3 for `HTTP_ERROR` / `PARSE_ERROR` only.
  - `STRUCTURALLY_YOUNG` outcome does NOT count toward attempts — waits for the issuer to actually file more.
  - 7-day minimum between retry attempts.
  - Successful backfill resets attempts to 0.
- Weekly audit re-includes `structurally_young` in backfill eligibility (not just `insufficient` + `unknown`) so that issuers crossing the 18-month threshold get re-audited and promoted to `analysable` once they've filed enough base forms. Without this, a young issuer that ages and files 2 × 10-K + 4 × 10-Q over time stays stuck at `structurally_young` forever.

**Chunk F — weekly coverage audit job** (~0.5 day)

- New scheduler job `weekly_coverage_audit`:
  - Runs Tuesday 04:00 UTC (free slot, no collisions).
  - `_tracked_job` wrap.
  - Body: `audit_all_instruments` → identify `insufficient`, `unknown`, AND `structurally_young` instruments → for each within retry budget, `backfill_filings` → `audit_all_instruments` re-run to settle. Including `structurally_young` in the eligibility set is what lets an aging young issuer eventually promote to `analysable` once they've filed enough base forms past the 18-month window.
  - Registered in `SCHEDULED_JOBS` + `_INVOKERS`.
  - Per-run log: counts per status, attempts consumed, total SEC requests.

**Chunk G — universe-sync hook: mark newly-added as `unknown`** (~0.25 day)

- After `nightly_universe_sync` adds coverage rows for new tradables (via Chunk B), set `filings_status = 'unknown'` for those new rows.
- They get picked up by the next weekly audit run automatically.
- No inline audit/backfill in universe sync — avoids HTTP inside universe-sync tx and keeps the job fast.
- Time-to-analysable: up to 7 days (weekly cadence). Acceptable given hands-off operating model.

**Chunk H — admin coverage surface** (~1 day)

- Backend: new GET endpoint `/admin/coverage` returning `{analysable: N, insufficient: N, fpi: N, ...}` counts + drill-down rows.
- Frontend: `AdminPage.tsx` adds "Filings coverage" card with counts + a link to `/admin/coverage/insufficient` (new route) showing drill-down list: symbol, CIK, attempts used, last reason, earliest SEC filing date.
- No write actions yet; read-only surface.

#### Cascade track (#273 + #276)

**Chunk I — thesis event-driven trigger + gating** (~1 day)

- Modify `thesis.py::find_stale_instruments`:
  - `tier` becomes optional; when `None` AND `instrument_ids` provided, bypass tier filter (used by cascade). Existing call sites (DTR tier=1/2) unchanged.
  - Add `instrument_ids: Sequence[int] | None = None` parameter — when provided, limits scope to those instruments.
  - Add event predicate:
    ```sql
    OR EXISTS (
      SELECT 1 FROM filing_events f
      WHERE f.instrument_id = i.instrument_id
        AND f.filing_type IN (
            '10-K', '10-K/A',
            '10-Q', '10-Q/A',
            '8-K',  '8-K/A'
        )
        AND f.created_at > COALESCE(t.created_at, '-infinity'::timestamptz)
    )
    ```
    (20-F/40-F/6-K NOT included in v1 — FPIs are not `analysable` yet. Revisits in #279.)
  - Add status gate: `AND coverage.filings_status = 'analysable'`. Strict equality — NULL (never), `unknown`, `insufficient`, `fpi`, `no_primary_sec_cik`, `structurally_young` all excluded.
  - Reason codes add: `event_new_10k`, `event_new_10q`, `event_new_8k` (amendments share the base reason code).

**Chunk J — scoring gates on `filings_status` (producers + consumers)** (~0.75 day)

- Modify `scoring.compute_rankings` eligibility query: add `AND coverage.filings_status = 'analysable'` to the JOIN predicate.
- **Gating at the producer side is necessary but not sufficient.** Score rows already exist in the DB for now-ineligible instruments; consumers that load "latest score by instrument_id" would still surface stale ineligible scores into recommendations / promotions. Enumerate consumers and update each:
  - `app/workers/scheduler.py::_has_scoreable_instruments` (~line 272) — the scheduler prerequisite that mirrors `compute_rankings`'s eligibility set. Must add `coverage.filings_status = 'analysable'` so the scoring job doesn't report "has work" for instruments the rankings query will immediately filter out.
  - `app/services/portfolio.py` (~line 357) — latest-score lookups for recommendation building must add the `filings_status = 'analysable'` join predicate so stale ineligible scores don't drive recommendations.
  - `app/services/coverage.py` (~line 191) — coverage review loader that reads latest scores for tier promotion decisions. Needs the same gate so a demoted-to-`insufficient` instrument doesn't get promoted on an old pre-gate score.
  - Any other per-instrument latest-score consumers found during implementation get the same treatment.
- Do NOT purge existing score rows. Gating at read time is the safer pattern — preserves audit history and lets operators inspect why an instrument was or wasn't considered.
- Test surface: insert instrument with `filings_status = 'insufficient'` + existing score row → portfolio recommendation does NOT surface it, compute_rankings next run does NOT include it.

**Chunk K — cascade service + wiring + retry outbox + advisory locking** (split into 4 sub-chunks, ~3 days total)

**K.1 — cascade service skeleton + basic wiring** (~1 day)

- New `app/services/refresh_cascade.py`:
  - `cascade_refresh(conn, client, changed_instrument_ids) -> CascadeOutcome` — iterates instrument_ids, calls `generate_thesis` per. After the thesis loop, if any thesis was refreshed this cycle, triggers a single full-pool `compute_rankings`.
  - `changed_instruments_from_outcome(conn, plan, outcome)` — maps (plan.refreshes − outcome.failed) ∪ (plan.submissions_only_advances − outcome.failed) CIKs to instrument_ids. Does NOT distinguish refreshes vs submissions-only at this layer — downstream thesis gating (Chunk I) decides whether each instrument actually needs a new thesis (via event-predicate + status gate). 8-K thesis changes affect scoring too because `compute_rankings` reads thesis fields (per Codex), therefore any successful thesis refresh → full rerank.
- `daily_financial_facts` hook: AFTER `normalize_financial_periods` returns, explicit `conn.commit()` to flush the outer tx's normalization writes, THEN call `cascade_refresh`. The explicit commit is required — `normalize_financial_periods` uses nested savepoints, not its own commit; without the explicit commit, cascade's fresh reads see uncommitted state.

**K.2 — retry outbox for cascade failures** (~1 day)

- Why needed: SEC watermarks commit before cascade, so a cascade failure never triggers a re-plan of the same CIK on the next `daily_financial_facts` run. Without an outbox, a failed cascade = permanently stale thesis/score for that instrument until the thesis naturally goes stale on its review_frequency window.
- New table `cascade_retry_queue (instrument_id BIGINT, enqueued_at TIMESTAMPTZ, attempt_count INTEGER, last_error TEXT, PRIMARY KEY (instrument_id))`.
- Cascade's per-instrument failure path: insert-or-update row.
- Cascade's per-instrument success path: delete row.
- `cascade_refresh` starts by draining the queue (retry-first), then does new work. Attempt cap 5; after that, manual intervention flag surfaces in admin.
- Ships before K.3 because K.3's lock-skip path needs the queue to exist.

**K.3 — advisory locking (session-level, held across Claude calls)** (~0.5 day)

- Use `pg_try_advisory_lock(<instrument_id>)` (session-level, NOT xact-level — xact-level releases at Chunk C's early commit).
- Acquired at start of per-instrument cascade iteration. Released in `finally` via `pg_advisory_unlock`.
- `daily_thesis_refresh` acquires the same lock before per-instrument generation.
- If lock unavailable → skip that instrument with reason `LOCKED_BY_SIBLING`. Two invariants:
  - Logged as an informational event, NOT a failure.
  - The `cascade_retry_queue` row (introduced in K.2, so this table is guaranteed to exist) is PRESERVED — not cleared, not incremented. `LOCKED_BY_SIBLING` is neither success nor failure; the sibling job will complete its own work and remove the queue row if successful. If sibling also fails, its own failure path enqueues/preserves the queue row. This guarantees "will retry next cycle" holds even when watermarks have committed.
  - If there is no queue row yet (fresh event), K.3 writes one with `last_error = 'LOCKED_BY_SIBLING'` and `attempt_count` unchanged from initial 0, so the next cycle re-attempts without retry-budget consumption.

**K.4 — orchestrator freshness + observability wiring** (~0.5 day)

- `sync_orchestrator/freshness.py` `thesis` layer freshness predicate: update to include cascade run timestamps (not just `daily_thesis_refresh` tracker).
- `scoring` layer freshness: similarly pick up cascade-run compute_rankings.
- `tracker.row_count` for `daily_financial_facts` conflates cascade work. Solution: cascade work logged under its own `cascade_refresh` sync layer or its own `data_ingestion_runs` row. Keep `daily_financial_facts` tracker scoped to fundamentals only.

#### Follow-up / deferred

**Chunk L — dedupe SEC filing fetches between `daily_research_refresh` and `daily_financial_facts`** (~1 day, separate ticket)

- Long-term: delete the SEC filings fetch from `daily_research_refresh` entirely once Chunk A guarantees `filing_events` population from `execute_refresh`. Companies House side of `daily_research_refresh` stays.
- Holds until Chunks A + D have been in production long enough to confirm `filing_events` completeness.

---

## Dependency graph (corrected)

```
PREREQS (parallel, any order):
  Chunk A (execute_refresh → filing_events)
  Chunk B (coverage row bootstrap)
  Chunk C (thesis commit-before-Claude)

COVERAGE-BAR STREAM:
  B → D (filings_status schema + audit)
           ├─→ E (backfill)
           │     └─→ F (weekly audit job, exercises E)
           ├─→ G (universe hook: mark unknown)
           ├─→ H (admin surface)
           └─→ J (scoring gates on filings_status)

CASCADE STREAM:
  A + D → I (event-driven trigger needs filing_events AND filings_status gate)
  C → K.1 (cascade service needs tx-clean thesis path)
  D → J (scoring gate — K.1 wants J already in)
  I + J + K.1 → K.2 (retry outbox — creates cascade_retry_queue table)
             → K.3 (advisory lock — depends on K.2's table)
             → K.4 (freshness/observability)

SHIPPABLE MERGE ORDER (one reasonable path):
  A, B, C (any order, parallel-mergeable)
  → D (enables I, J, E, G, H)
  → parallel: E + F, G, H, J, I
  → K.1 → K.2 (outbox) → K.3 (lock) → K.4
```

**Critical paths:**
- Cascade earliest = A + C + D + I + K.1 + K.2 + K.3 + K.4 = ~8-9 days sequential.
- With two engineers: cascade stream ~5-6 days elapsed.

---

## Ticket plan

Rescope existing + file new:

| # | Ticket | Chunks | Est |
|---|---|---|---|
| NEW | execute_refresh populates filing_events | A | 1.5d |
| NEW | coverage row bootstrap in universe sync | B | 0.5d |
| NEW | generate_thesis commits before Claude | C | 0.5d |
| #268 | filings coverage bar + audit + backfill + admin | D, E, F, G, H, J | 6.25d |
| #273 | thesis event-driven trigger | I | 1d |
| #276 | cascade refresh service + wiring | K.1, K.2, K.3, K.4 | 3d |
| #279 | Companies House streaming + UK analysability | (separate track) | - |
| NEW | dedupe SEC fetches | L | 1d (follow-up) |

---

## Pre-implementation gates (per CLAUDE.md Codex checkpoints)

1. **This master plan** — Codex review this revision before user approves.
2. **Each ticket's spec** — written when its prerequisites are close to landing. Codex-reviewed before implementation.
3. **Each ticket's implementation plan** — Codex-reviewed before first task dispatch.

---

## What this plan makes explicit (vs prior broken specs)

- `execute_refresh` does NOT currently write `filing_events` (Codex §Repo map). Chunk A fixes.
- Scoring can't do subset recompute (Codex Blocker). Fold #276 into full-pool re-rank after cascade (Option α).
- Cascade must run after normalization commits (Codex Blocker).
- `generate_thesis` has implicit read tx across Claude calls (Codex Blocker). Chunk C fixes.
- `coverage` rows don't auto-create for newly-added instruments (Codex, verified). Chunk B fixes.
- DTR is T1/T2-only; cascade Chunk K covers anything that misses the DTR cohort.
- Pagination must be recent-first (Codex).
- Retry cap needs distinguishing HTTP error vs structurally-young (Codex).
- 8-K gap detection requires SEC external truth (Codex); handled via Chunk E backfill pass, not DB-internal audit.
- FPIs get `fpi` status, blocked from analysable (Codex); 20-F/40-F cascade triggers deferred to #279.
- `external_identifiers` queries must filter `is_primary = TRUE` (Codex).
- `filings_status = NULL` is error state post-first-audit, never "assume analysable" (Codex).
- `scoring.compute_rankings` needs `filings_status = 'analysable'` gate too (Codex).
- Advisory lock must be session-level, not xact (Codex).
- Cascade failure needs retry outbox — not implicit re-plan (Codex).
- `RefreshPlan` payload needs enrichment to carry full filing metadata (Codex).
- Cascade writes shouldn't conflate `daily_financial_facts` row_count metrics (Codex §Chunk K).
- Schedule slot Tuesday 04:00 UTC avoids existing collisions (Codex caught Sunday 04:30 wrong).
