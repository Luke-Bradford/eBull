# #1343 — S18/S21 lazy-on-click: defer 10-K Item 1 + 8-K item bodies to first user view

Status: **proposal v2.1** (Phase 3 PR2 of bootstrap-sub-1h). Pre-branch.
Review path: committee-review (8-lens, done 2026-05-29) → Codex checkpoint-1 (done; 1 BLOCKING + 4 IMPORTANT + 1 NIT folded) → **user sign-off (pending)**.
v2 corrected a v1 premise error the committee caught (see §0.0); v2.1 folded Codex ckpt1 (S18 weekly-skip, S16 initial-status param, #938 scoping, dynamic constraint name, lazy/force-drain race). Findings: [[project-1343-consolidated-findings]].

---

## 0.0 v1→v2 correction (why this was rewritten)

v1 asserted "the gate-EXEMPT `sec_manifest_worker` fetches bodies during bootstrap → it's the lever." **FALSE** (committee BLOCKING, re-verified): the worker has NO exempt flag (scheduler.py:1033-1049) → it is GATED during bootstrap. The `exempt=True` at :1101 belongs to `JOB_SEC_DAILY_INDEX_RECONCILE`; `test_universal_gate_carve_out.py:15` pins exempt = `{sec_daily_index_reconcile}`.

**Corrected mechanism** — the worker is gated *during* bootstrap, but `catch_up_on_boot=True` (:1048) means it eagerly drains the backlog *after* bootstrap. So the lever is NOT a worker hook; it is:
1. **S16 seeds sec_10k/sec_8k manifest rows as `'deferred'`** (not `'pending'`). `iter_pending`/`iter_retryable` select only pending/failed → the backlog is never eagerly drained, during OR after bootstrap.
2. **S18/S21 bootstrap stages seed typed metadata only** (no HTTP). They — not the worker — are the bootstrap fetchers today, so making them metadata-only stops the bootstrap body fetch.
3. **Lazy API** fills bodies on first view, flipping `'deferred'→'parsed'`.

This DROPS v1's non-existent `ParserSpec.metadata_seed_fn` + worker `bootstrap_state` read, and PRESERVES #1347 (its recency bound now scopes the metadata-seed cohort — no dead code).

---

## 0. Grep proof

> Generated 2026-05-29 against `main` @ `0aa3196`. Verbatim.

### 0.1 Manifest worker is GATED during bootstrap (corrected)
```
app/workers/scheduler.py:1033-1049  ScheduledJob(name=JOB_SEC_MANIFEST_WORKER ... catch_up_on_boot=True)  # NO exempt field → gated
app/workers/scheduler.py:1101  exempt_from_universal_bootstrap_gate=True   # belongs to JOB_SEC_DAILY_INDEX_RECONCILE (:1073)
tests/test_universal_gate_carve_out.py:15  pins exempt set = {sec_daily_index_reconcile}
```

### 0.2 Bootstrap body fetchers today = S18/S21 (+ S16 seeds the manifest, no HTTP)
```
app/services/business_summary.py:1739   html = fetcher.fetch_document_text(url)   # S18 (gated context = bootstrap stage)
app/services/eight_k_events.py:782       html = fetcher.fetch_document_text(url)   # S21
app/jobs/sec_first_install_drain.py:490  seed_manifest_from_filing_events(...)     # S16 seeds rows, no HTTP, comment :167
app/services/sec_manifest.py:925 "10-K"→"sec_10k"; :929 "8-K"→"sec_8k"
```

### 0.3 Manifest state machine (cited §4)
```
app/services/sec_manifest.py:145 IngestStatus = Literal["pending","fetched","parsed","tombstoned","failed"]
app/services/sec_manifest.py:153-176 _ALLOWED_TRANSITIONS (closed dict)
app/services/sec_manifest.py:419-475 transition_status SET-clause branches (parsed/fetched/failed/tombstoned/pending — NO deferred)
app/services/sec_manifest.py:207 record_manifest_entry(...)  # ":227 metadata fields without touching ingest_status"
app/services/sec_manifest.py:532 iter_pending WHERE ingest_status='pending'   # 'deferred' auto-excluded ✓
sql/118_sec_filing_manifest.sql:85 inline CHECK (ingest_status IN (...))  # UNNAMED → PG auto-name sec_filing_manifest_ingest_status_check (VERIFY at impl via \d)
app/jobs/sec_manifest_worker.py:94 ParseStatus = Literal["parsed","tombstoned","failed"]
app/jobs/sec_manifest_worker.py:357-398 #938 guard (parsed ⇒ raw stored) lives in WORKER dispatch, NOT transition_status
```

### 0.4 Typed sinks + readers + capabilities (cited §4/§8/§I-fixes)
```
sql/055_instrument_business_summary.sql:27 body TEXT NOT NULL DEFAULT ''
sql/061_eight_k_structured_events.sql:115 eight_k_items.body TEXT NOT NULL DEFAULT ''
sql/061:68-73 idx_eight_k_filings_instrument (instrument_id, date_of_report DESC) [no WHERE — incl NULL]; idx_eight_k_filings_report_date WHERE date_of_report IS NOT NULL
app/services/business_summary.py:1122 get_business_summary SELECT body (no body_deferred); :1183 get_parse_status SELECT body,reason,retry,parsed (no body_deferred)
app/services/business_summary.py:1216 fallthrough → parse_failed  # a deferred row would hit this
app/services/capabilities.py:191 ("business_summary","sec_10k_item1"): EXISTS(instrument_business_summary)  # overclaims on deferred
app/services/capabilities.py:174 ("corporate_events","sec_8k_events"): EXISTS(eight_k_filings ...)            # overclaims on deferred
app/services/eight_k_events.py:585-592 list_8k_filings EXISTS(filing_events) + ORDER BY date_of_report DESC NULLS LAST
```

### 0.5 8-K item codes + reportDate are bulk metadata (no body needed)
```
app/services/sec_filing_items.py:32 items_col = recent.get("items")  # → filing_events.items[]
app/providers/implementations/sec_edgar.py:850 report_dates = block.get("reportDate")  # parsed but DROPPED (no filing_events column)
```

### 0.6 #1347 recency floor (reused, NOT superseded)
```
app/services/filings.py: BOOTSTRAP_FILINGS_RECENCY_DAYS=396 + bootstrap_filings_recency_floor()
app/services/business_summary.py:1453 bootstrap_business_summaries(min_filing_date) gated on progress_ctx
```

---

## 1. Decisions

During first-install bootstrap, 10-K Item 1 (`instrument_business_summary` + `_sections`) and 8-K item bodies (`eight_k_items.body` + exhibits) are **seeded as metadata only — body NOT fetched**. Bodies are fetched on the **first user view** (business panel auto-load / 8-K detail row-select), cached, instant thereafter.

Two levers, both bootstrap-scoped by construction:
- **S16** (`seed_manifest_from_filing_events`, bootstrap-only) seeds sec_10k/sec_8k manifest rows as **`'deferred'`** — a new terminal status the worker's `iter_pending` skips, so the post-bootstrap `catch_up_on_boot` worker never eagerly drains the backlog.
- **S18/S21** bootstrap stages (`progress_ctx`-gated, the #1347 pattern) **seed typed metadata** (`body_deferred=TRUE`) instead of fetching.

Steady-state is unchanged-eager for NEW filings: a never-before-seen 10-K/8-K is seeded `'pending'` by Layer 1/2/3 and drained eagerly; a NEWER filing supersedes a deferred placeholder eagerly (the latest-per-instrument selector re-selects on accession change). Only the bootstrap-era backlog stays lazy-until-clicked.

Operator effect: S18/S21 do ~0 HTTP + seconds of DB work at bootstrap; no post-bootstrap fetch storm; `sec_rate` freed for S14/S16. First viewer waits ~0.5-1 s once. **#1347 preserved** (its 396 d floor now bounds the metadata-seed cohort). **DEF 14A NOT deferred** (S17 untouched — beneficial ownership is load-bearing). **#449 already shipped + closed — out of scope.**

---

## 2. Identifiers + identity-drift
`instrument_id` (typed-table + lazy-fetch key) + SEC `accession_number` (manifest PK + 8-K PK). No new identifier. Share-class: 10-K/8-K are issuer-level; reads bridge per-instrument via `filing_events`. ⚠ §10 EXISTS→JOIN for `filing_date` must `DISTINCT`/aggregate to avoid sibling fan-out. No CIK/CUSIP/FIGI drift in this path.

## 3. Endpoint surface
No new SEC endpoint (reuses `fetch_document_text`). Internal routes:

| Route | Method | Purpose | Returns |
|---|---|---|---|
| `/instruments/{symbol}/business_sections` | GET | EXISTING (instruments.py:1366). Extend: if `body_deferred`, fetch+cache inline, then return sections. | existing `BusinessSectionsResponse` (+`parse_status.state='deferred'` transient) |
| `/instruments/{symbol}/eight_k_filings/{accession}/body` | GET | NEW. Fetch+cache one filing's bodies+exhibits. | the **full `EightKFilingModel`** (instruments.py:1152) so the FE swaps the rail object wholesale |

Side-effecting GET (issue's "fetch on access"): idempotent (ON CONFLICT). `Cache-Control: no-store`. **Error contract (committee I11):** parse-miss / 404 / 410 → **2xx with empty body** (FE renders empty-state, NOT an error toast); 429/5xx → `503` (FE "try again"). dashed accession is a valid FastAPI path param (repo's first accession-in-path; add a routing test).

## 4. Schema

**Migration `sql/179_lazy_body_deferred.sql`** (`-- runner: <default transactional>`; NOT autocommit — DDL is transactional, autocommit is only for `ALTER SYSTEM`):

```sql
ALTER TABLE instrument_business_summary ADD COLUMN IF NOT EXISTS body_deferred BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE eight_k_filings           ADD COLUMN IF NOT EXISTS body_deferred BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE filing_events             ADD COLUMN IF NOT EXISTS report_date   DATE;   -- §10 (reportDate capture, in-scope per committee I8)

-- new terminal manifest status 'deferred' — matches the PROVEN house pattern for
-- widening this table's CHECK enums (sql/153 widened sec_filing_manifest_source_check
-- identically). Plain transactional DROP IF EXISTS + ADD. The inline CHECK at
-- sql/118:85 is unnamed → PG auto-names it sec_filing_manifest_ingest_status_check
-- (the convention sql/153 already relies on for the source check; DROP IF EXISTS
-- tolerates it). New list is a strict SUPERSET → all rows pass. NO lock_timeout
-- (a timeout in the boot-time run_migrations would turn transient contention into a
-- boot failure — worse than a brief wait) and NO NOT VALID/VALIDATE (zero benefit
-- inside the runner's single transaction — superseded the v2 DO-block; reuse>reinvent).
ALTER TABLE sec_filing_manifest DROP CONSTRAINT IF EXISTS sec_filing_manifest_ingest_status_check;
ALTER TABLE sec_filing_manifest ADD CONSTRAINT sec_filing_manifest_ingest_status_check
    CHECK (ingest_status IN ('pending','fetched','parsed','tombstoned','failed','deferred'));
```

State semantics (`body_deferred`): `TRUE` = metadata seeded, body not fetched (≠ `body=''` tombstone ≠ `body=text`). `'deferred'` manifest status: terminal, `raw_status` stays `'absent'`; flipped → `'parsed'`+`raw_status='stored'` on lazy fill.

Type/code updates (committee B3): `IngestStatus` +`'deferred'`; `ParseStatus` (worker) +`'deferred'`; `_ALLOWED_TRANSITIONS` — `pending`+=`deferred`, `failed`+=`deferred`, NEW key `"deferred": frozenset({"pending","parsed","tombstoned"})` (no self-loop); NEW `'deferred'` SET-branch in `transition_status` clearing `error`+`next_retry_at`, leaving `raw_status` untouched. `ParseStatus.state` (business_summary) + API `BusinessSectionsParseStatus.state` + TS Literal (instruments.ts:220) all +`'deferred'`.

Index Budget: no new index (partial `WHERE ingest_status IN ('pending','failed')` correctly omits `'deferred'`). `filing_events.report_date` adds a column, no index (rail reads via instrument bridge). ✓

## 5. Fetch strategy + rate-limit composition
| Path | Bootstrap | Steady-state |
|---|---|---|
| S16 manifest seed | `derive` (rows seeded `'deferred'`, 0 HTTP) | n/a (bootstrap-only stage) |
| S18/S21 stages | `derive` (metadata seed, `body_deferred=TRUE`, 0 HTTP), `progress_ctx`-gated, #1347 396d cohort | weekly safety-net: eager for NEW/superseding filings; **skips `body_deferred` rows** (see §13) |
| manifest worker | gated (no run) | drains `'pending'` (new filings) eagerly; never selects `'deferred'` |
| lazy API | allowed (user read, not a gated job) — see §13 | 1 doc/click via SEC provider limiter |

Rate limit (committee I10): `fetch_document_text` shares one process-wide lock (`_PROCESS_RATE_LIMIT_LOCK`, sec_edgar.py:72, ~9 req/s). A lazy click serializes behind the worker → "<1s" is best-effort; under heavy steady-state drain a click can wait seconds. Bound the lazy path with a tighter client-visible timeout; document degraded latency rather than promising <1s unconditionally.

## 6. Conditional-GET semantics
Unchanged. A `'deferred'` row carries no conditional-GET state (never fetched); first lazy/steady fetch establishes ETag/If-Modified-Since via the provider.

## 7. Retry posture per error-class
Lazy fetch must not loop (prevention-log §1265/§1271):
- 10-K: reuse sql/074 backoff cols (`attempt_count`/`last_failure_reason`/`next_retry_at`) — a failed lazy fetch records failure exactly as steady-state; `get_parse_status` → `parse_failed`/`no_item_1`; panel shows `ParseStatusEmptyState` (#648), no refetch.
- 8-K: reuse `eight_k_filings.is_tombstone`; failed lazy fetch tombstones; `list_8k_filings` excludes it.
- Transient (429/5xx): `503` to FE, no tombstone, click retries.

## 8. Multi-writer sink registry
- **`sec_filing_manifest`**: new transitions `→'deferred'` (S16 seed) and `'deferred'→'parsed'` (lazy API). Conflict key unchanged (triple UPSERT). **S16 seed mechanism (Codex ckpt1):** add an `initial_ingest_status` param to `record_manifest_entry` applied ONLY on INSERT (preserving conflict-lifecycle status, sec_manifest.py:255/266) for sec_10k/sec_8k — cleaner than a post-seed `pending→deferred` UPDATE (which would need careful `WHERE ingest_status IN ('pending','failed')` scoping + same-tx). `pending→deferred` + `failed→deferred` still added to `_ALLOWED_TRANSITIONS` for the lazy/runbook paths.
- **`instrument_business_summary`**: writers = S18 seed (metadata), lazy API (full body), weekly S18 (eager new). Option C gate (business_summary.py:951-956): a deferred row holds the latest accession → lazy fill matches incumbent → write proceeds (verified). Seed must set `body_deferred=TRUE` and NOT stamp `last_parsed_at` (it hasn't parsed); lazy fill clears `body_deferred` + sets `last_parsed_at`.
- **`eight_k_filings`/`_items`/`_exhibits`**: seed needs a **metadata-only writer** — `upsert_8k_filing` (eight_k_events.py:400) requires a full `Parsed8KFiling` (body-derived fields). Add a seed path constructing a sentinel `Parsed8KFiling` (document_type/is_amendment from `filing_events.form`; `date_of_report` from `filing_events.report_date`; items from `filing_events.items[]`+`sec_8k_item_codes`; bodies `''`; no exhibits; `body_deferred=TRUE`). Lazy fill's DELETE-then-INSERT replaces the empty items + adds exhibits + clears `body_deferred`.
- **#938 (committee B4 + Codex ckpt1):** the lazy `'deferred'→'parsed'` flip MUST `store_raw` + pass `raw_status='stored'`. Do NOT move the worker's guard wholesale into `transition_status` — synth/no-payload parsers legitimately return `parsed` with no raw (`sec_xbrl_facts.py:76`, `finra_regsho_daily.py:79`; the worker guard is conditional on `spec.requires_raw_payload`, sec_manifest_worker.py:371-374) and `transition_status` has no parser context. Enforce raw-stored ONLY in the lazy sec_10k/sec_8k endpoints (keep the worker's existing flag for the drain path).

## 9. Watermark + retry-budget
No new watermark. Idempotent: seed via UPSERT + non-`pending` exclusion; re-run is no-op. **Concurrency (committee I9):** the business panel auto-loads on mount + React StrictMode double-invokes → concurrent identical lazy fetches are COMMON, not rare. A per-`(instrument_id)`/`(accession)` **advisory lock around the lazy fetch is DEFAULT (not optional)**; the holder fetches, the waiter re-reads the now-filled row. The lazy writer fetches FIRST, then opens a short write tx (committee M4 — do NOT hold `FOR UPDATE` across the network fetch, or a force-drain blocks behind the HTTP). **Race with force-drain/worker (Codex ckpt1):** the runbook flips `deferred→pending` and the worker selects `pending` WITHOUT the lazy advisory lock, so a concurrent click + force-drain can double-fetch + race `pending→parsed` vs `deferred→parsed`. Not a deadlock. The lazy writer RE-READS `ingest_status` inside the write tx and no-ops if already `parsed` — single-fetch is best-effort, idempotent on the race (ON CONFLICT). Acceptable; force-drain is a rare operator escape hatch.

## 10. Encoding / precision / NULL / timezone
- `body_deferred` BOOLEAN NOT NULL DEFAULT FALSE.
- **8-K date (committee I8):** capture `submissions.json reportDate` → `filing_events.report_date` at S8 ingest (the field is parsed at sec_edgar.py:850 but dropped — a latent §903 drop, fixed here). Seed writes `eight_k_filings.date_of_report = filing_events.report_date` → deferred rows have the TRUE event date at seed, no fetch, correct ordering. Interim before backfill: `list_8k_filings` orders `COALESCE(date_of_report, filing_date)` (EXISTS→`LEFT JOIN LATERAL (SELECT ... LIMIT 1)` to avoid share-class fan-out). UTC unchanged.

## 11. Backfill horizon + retention
Metadata-seed cohort = #1347 `bootstrap_filings_recency_floor` (396d): latest 10-K/instrument + 8-K/8-K-A within 396d. Older filings: manifest `'deferred'` (worker skips); a click on an out-of-cohort filing still lazily fetches + creates the typed row. 8-K row budget (committee I6, quantified): ~10-16 filings/issuer ×396d × ~3-5k issuers ≈ 30-80k `eight_k_filings` + 75-250k `eight_k_items` rows — bounded, far below partition thresholds. Recorded (no silent cap). No sweep — `'deferred'` rows persist (they ARE the coverage record).

## 12. Partition strategy + extension deadline
N/A — no partitioned table touched.

## 13. Bootstrap vs steady-state mode
**Bootstrap (`bootstrap_state.status ≠ 'complete'`):**
- S16 `seed_manifest_from_filing_events`: seed sec_10k/sec_8k rows `'deferred'`. (S16 runs only in bootstrap — inherently scoped; no bootstrap_state read needed.)
- S18/S21 stages (`progress_ctx`-gated via `resolve_progress_context()`, the #1347 discriminator that WORKS for stages): seed typed metadata, 0 HTTP, within the 396d cohort.
- Manifest worker: gated (does not run).
- **Expected 10-K/8-K body HTTP during bootstrap = 0.**

**Steady-state (`status='complete'`):**
- Worker drains `'pending'` (new filings, Layer 1/2/3) eagerly; never selects `'deferred'`.
- Weekly S18/S21 safety-net (`progress_ctx` None): eager for filings with NO row / a NEWER accession; **must skip `body_deferred=TRUE` incumbents** so it does not re-fetch the lazy backlog.
  - **S21 confirmed (Codex ckpt1):** the selector requires `ekf.accession_number IS NULL` after the join (eight_k_events.py:737/742) → an existing (deferred) `eight_k_filings` row is naturally skipped. ✓
  - **S18 needs work (Codex ckpt1 BLOCKING):** the S18 metadata seed MUST write the parent `instrument_business_summary` row ONLY — **zero `instrument_business_summary_sections` child rows** (there is no body → no sections to extract). Otherwise the #560 section-backfill reselect (`tables_json IS NULL`, business_summary.py:1702) re-selects the row and re-fetches the backlog. AND add `AND bs.body_deferred = FALSE` to the retry/backfill reparse branches (business_summary.py:1689/1691/1693/1702) so a deferred incumbent is never re-fetched. A NEWER accession still supersedes (accession-mismatch arm fires → eager fetch).

**Lazy fetch (any mode):** a user read on a `body_deferred` row fetches + caches. This is a READ endpoint, not a gated job → allowed during bootstrap (a user mid-bootstrap gets their one body). The ONE sanctioned per-resource HTTP outside the carve-out table — user-paced, single-doc, not a sweep. **Add to `bootstrap-mode-discipline` skill as the "user-triggered lazy fill" exception (skill edit in this PR).**

Net vs bootstrap-mode-discipline: REMOVES a non-carve-out per-resource-HTTP path (S18/S21 bootstrap fetch) — a win for the invariant.

## 14. Tombstones + soft-delete
`'deferred'` ≠ tombstone (it's "not yet fetched"; distinct status + `body_deferred=TRUE`). Existing tombstones unchanged; a failed lazy fetch transitions `'deferred'→'tombstoned'`. State space (non-overlapping IFF readers check `body_deferred` BEFORE `failure_reason` — committee I1): `deferred` / `tombstone` (body='',reason set) / `no_item_1` (reason marker) / `real` (body=text). Never hard-delete.

## 15. `rows_skipped` closed-set + other
Worker `ParseOutcome`/stats: `'deferred'` is a first-class terminal (not a skip); add `deferred_by_source` to worker stats. **Operator audit (committee H2):** `/coverage/manifest-parsers` (`manifest_parser_audit.py:155-168`) MUST add `rows_deferred` to the dataclass + response model + GROUP-BY bucket, else sec_10k/sec_8k "vanish" from the card. **Coverage-hole visibility (committee H3):** add a dashboard line "N business-summary/8-K bodies deferred (fill on view)" so the never-clicked backlog is not a silent hole. Cohort bound recorded (#1273, no silent cap).

## 16. Schema-evolution migration path
- `'deferred'` additive (superset CHECK) — no dual-parser window, no back-compat read hazard.
- **#1347 PRESERVED** (corrected from v1): S18 stays a bootstrap stage (now metadata-only), still `progress_ctx`-gated + 396d-bounded — the bound now scopes the SEED cohort. No dead code. S17 DEF 14A untouched.
- `body_deferred` DEFAULT FALSE → existing rows read as not-deferred (correct).
- **Capability fix (committee I2):** capabilities.py:191 (`sec_10k_item1`) + :174 (`sec_8k_events`) currently go true on row existence → MUST add `AND body_deferred = FALSE` so a deferred placeholder does not overclaim body-readiness.

## 17. Operator runbooks
`app/runbooks/sec_lazy_body_backfill.py` (flat layout + `safety.py`): `--dry-run` reports `body_deferred=TRUE` counts per source/recency; `--apply --source {sec_10k,sec_8k}` flips `'deferred'→'pending'` so the steady-state worker eagerly fills (escape hatch). **Assert `bootstrap_state.status='complete'`** (committee L6 — a mid-bootstrap force-drain is a no-op: the worker is gated). `assert_dev_env`+`assert_dev_db` guarded.

## 18. Smoke matrix
Panel `AAPL, GME, MSFT, JPM, HD`. Post-bootstrap: each has `body_deferred=TRUE` rows; **0 bodies fetched** during bootstrap (assert manifest/`job_runs` counts). Lazy: `GET /…/business_sections` fills <1-2 s, second call instant + `body_deferred=FALSE`; `GET /…/eight_k_filings/{accn}/body` fills items+exhibits. Rail shows codes+dates (true `report_date`) pre-fetch. **DoD 8-12 (committee I14): batched to the operator-driven end-of-ETL clean bootstrap** (per the #1337 P1 precedent) — disclosed here + in the PR body; merge gates on unit+integration green, with the "0 fetch under `bootstrap_state='running'`" integration test as a HARD blocker.

## 19. Cross-source verification
AAPL latest 10-K: lazily-fetched Item 1 first paragraph matches SEC-hosted doc; pre-fetch 8-K item codes match `submissions.json items[]`. Recorded in PR (clause 9).

## 20. Test placement
- **Unit**: seed writes `body_deferred=TRUE` + 0 HTTP (patch `fetch_document_text` to **raise** — committee I13 — so a fetch-then-defer regression trips); `get_parse_status`/`get_business_summary` read `body_deferred` → `'deferred'` not `parse_failed`; capabilities exclude deferred; `transition_status` `'deferred'` edges + #938-on-parsed guard; `list_8k_filings` COALESCE ordering with a NULL-date fixture + a real-date sibling.
- **Integration** (dev DB, seed `bootstrap_state`): S18/S21 stage under `status='running'` → 0 fetch + `body_deferred=TRUE` + manifest `'deferred'`; under `'complete'` → eager. Lazy API end-to-end fill + concurrent-double-fetch idempotency.
- **Contract**: `test_bootstrap_orchestrator_source_registry.py` (REAL name — committee B7; the v1-cited `*_catalogue_invariants.py` does NOT exist) green after the S18/S21 lane move; `_ALLOWED_TRANSITIONS` test (test_sec_manifest.py:707) extended incl. `assert "deferred" not in _ALLOWED_TRANSITIONS["deferred"]`.
- **FE** (new files — committee B6/I3: both components are test-less + `EightKDetailPanel` is prop-only today): lift 8-K fetch to `EightKEventsPanel` (rail parent) — on row-select, if `filing.body_deferred`, call the body endpoint, swap the filing; `BusinessSectionsPanel` inline-fill (skeleton covers wait). Each asserts deferred⇒fetch, non-deferred⇒no fetch, transient⇒retry-not-spinner.
- Flakiness: distinct accessions per lazy test (not shared rows); `xdist_group` secondary.

## 21. Rationale log
**Decision:** S16-seed-`'deferred'` + S18/S21 metadata-seed. **Rejected:** hook the manifest worker (v1) — it's gated during bootstrap (can't hook) and the backlog danger is post-bootstrap, solved by the seed status.
**Decision:** new terminal `'deferred'`. **Rejected:** `'parsed'` (violates #938); leave `'pending'` (post-bootstrap eager drain).
**Decision:** KEEP S18/S21 as metadata-only stages. **Rejected:** retire (v1) — they ARE the bootstrap typed-seeders (worker gated); retiring loses the seed.
**Decision:** capture `reportDate→filing_events.report_date` IN-SCOPE. **Rejected:** COALESCE-only follow-up (v1 §22.2) — a disguised punt; the index `WHERE date_of_report IS NOT NULL` + correct ordering want the real column (committee I8).
**Decision:** advisory lock DEFAULT for lazy fetch. **Rejected:** accept-rare-double-fetch (v1) — auto-load+StrictMode makes it COMMON (committee I9).
**Decision:** #1347 preserved. **Rejected:** v1's supersession/dead-bound removal — wrong once S18 stays a (metadata-only) stage.
**Decision:** DEF 14A NOT deferred. **Rejected:** defer it — beneficial ownership feeds the rollup + cap-gate.

## 22. Open questions
1. **PR split (committee PM/Codex):** PR-A = sql/179 + S16-defer-seed + S18/S21 metadata-seed + `'deferred'` status/transitions + readers/capabilities + `reportDate` capture + **lazy BACKEND fill endpoints** + audit-card + runbook (operator-curl-verifiable, no empty-panel window). PR-B = FE wiring + §18 smoke + §19 cross-source. Never ship A without the backend fill. Lean: A→B sequence. Confirm at sign-off.
2. **Advisory-lock scope:** per-instrument vs per-accession. Lean per-`(instrument_id)` for business, per-`accession` for 8-K.
3. **`'deferred'` vs `'metadata_deferred'` name** (Codex CTO LOW): lean `'deferred'` (terser); not load-bearing.

## 23. References
Plan `bootstrap-sub-1h-plan.md` §Phase 3 · [[project-1343-consolidated-findings]] · [1347](1347-s17-s18-recency-bound.md) (preserved) · worker `app/jobs/sec_manifest_worker.py` · parsers `manifest_parsers/{sec_10k,eight_k}.py` · prevention §903/§1265/§1271/§937/§946.
