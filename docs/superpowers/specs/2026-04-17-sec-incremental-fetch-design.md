# SEC incremental fetch — design spec

**Issue:** #272
**Parent plan:** `docs/superpowers/plans/2026-04-17-lightweight-etl-audit.md`
**Depends on:** #269 (watermarks infra — shipped, PR #283)
**Date:** 2026-04-17
**Status:** Design approved, implementation plan next.

---

## Goal

Replace the 45-minute `daily_research_refresh` full-pull with a change-driven fetch that:

- uses the SEC daily master-index as the change signal,
- pulls per-CIK `submissions.json` + `companyfacts.json` only when something actually filed,
- self-seeds on first sight (fresh install or newly-promoted ticker) with no migration step,
- closes the day-boundary / missed-run gap with a fixed 7-day lookback window,
- keeps existing XBRL upsert + normalization logic untouched.

Target: 45 min → 2-5 min typical wall time, ~98% fewer SEC requests, ~99% fewer bytes.

---

## Scope

**In scope**

- New service module `app/services/sec_incremental.py`: planner + executor for the delta fetch.
- New provider function in `app/providers/implementations/sec_edgar.py`: `fetch_master_index(date, if_modified_since)`.
- Rewire `daily_financial_facts` (sync-orchestrator composite) to consume the new plan.
- Per-CIK seed path triggered by absence of `sec.submissions` watermark row — covers fresh install and new universe tickers.
- Three new `source` identifiers in `external_data_watermarks`: `sec.master-index`, `sec.submissions`, `sec.companyfacts`.
- Form-type gate: only `{10-K, 10-K/A, 10-Q, 10-Q/A, 20-F, 20-F/A, 40-F}` trigger companyfacts refresh.
- Unit + integration tests; no new migration (existing watermark table reused).

**Out of scope**

- Changes to XBRL concept coverage or normalization math (existing code unchanged).
- Changes to the CIK mapping refresh job (that was #270, shipped).
- Companies House filings (#279 — separate track).
- Filings coverage bar enforcement (#268 — orthogonal).
- Thesis event-driven refresh (#273 — depends on this shipping first).

---

## Architecture

Provider stays thin HTTP. Orchestration lives in a new service module. Existing XBRL upsert reused as-is.

```
app/providers/implementations/sec_edgar.py
  ├─ fetch_master_index(date, if_modified_since) -> (status, last_modified, body) | None    # NEW
  ├─ fetch_submissions(cik)                                                                  # EXISTING
  └─ fetch_companyfacts(cik)                                                                 # EXISTING

app/services/sec_incremental.py                                                              # NEW
  ├─ plan_refresh(conn) -> RefreshPlan
  └─ execute_refresh(conn, plan, progress) -> RefreshOutcome

app/services/financial_facts.py                                                              # EXISTING
  └─ full_backfill(cik) / refresh_companyfacts(cik)    # thin wrappers over existing XBRL upsert

app/workers/scheduler.py::daily_financial_facts                                              # REWIRED
  1. plan = plan_refresh(conn)
  2. progress.total = len(plan.seeds) + len(plan.refreshes)
  3. execute_refresh(conn, plan, progress)
```

### `plan_refresh`

1. Load `covered_us_ciks` from the existing covered-instruments query (CIK-mapped, coverage tier ≥ 1).
2. For each day in `[today - 7 days, today]` (ET):
   - `wm = get_watermark("sec.master-index", date)`
   - Fetch `master.YYYYMMDD.idx` with `If-Modified-Since: wm.watermark` (if present).
   - 304 → skip.
   - 200 + body hash matches stored → refresh `fetched_at` only, no parse.
   - 200 + new body → parse entries into `(cik, accession, form_type)` tuples.
3. Union all parsed entries from the window into `master_hits`, indexed by CIK.
4. For each covered CIK:
   - `wm = get_watermark("sec.submissions", cik)`
   - If `wm is None` → add to `plan.seeds`.
   - Else if CIK not in `master_hits` → skip (no fetch).
   - Else fetch `submissions.json`, read `filings.recent.accessionNumber[0]`:
     - If equal to `wm.watermark` → skip (amendment or duplicate master-index listing).
     - If different and any filing in `master_hits[cik]` has a fundamentals-relevant form type → add to `plan.refreshes`, advance both `sec.submissions` and (pending execute) `sec.companyfacts`.
     - If different but form types are all non-fundamentals (e.g. 8-K only) → advance `sec.submissions` watermark only, do NOT queue companyfacts refresh.

Returns `RefreshPlan { seeds: list[str], refreshes: list[str], submissions_only_advances: list[(cik, accession)] }`.

### `execute_refresh`

- For each `cik in plan.seeds`: inside one `with conn.transaction()`:
  - Fetch `submissions.json` + `companyfacts.json` (full backfill).
  - Run existing XBRL upsert + normalization.
  - `set_watermark("sec.submissions", cik, top_accession)`.
  - `set_watermark("sec.companyfacts", cik, top_accession)`.
- For each `cik in plan.refreshes`: inside one `with conn.transaction()`:
  - Fetch `companyfacts.json`.
  - Run XBRL upsert + normalization.
  - Advance both watermarks.
- For each `(cik, accession) in plan.submissions_only_advances`: set `sec.submissions` watermark alone.
- Per-CIK failure isolated — one CIK's exception does not fail the layer; failed CIK's watermarks are NOT advanced, so next run re-plans it.
- `progress.report_progress(done, total)` after each CIK.

Returns `RefreshOutcome { seeded: int, refreshed: int, submissions_advanced: int, failed: list[(cik, error_class)] }`.

---

## Data flow

**Typical day (steady state, 300 US cohort):**

1. `plan_refresh` fetches 7 master-index files. Typical: 1 × 200 (~67 KB new file for today) + 6 × 304 (~1 KB headers total). Parse today's file: ~20-200 filing rows.
2. Intersect with covered cohort: typical 1-5 hits.
3. For each hit: fetch `submissions.json` (~30 KB) to read top accession. Compare against watermark.
4. If changed and form-type is fundamentals-relevant: fetch `companyfacts.json` (~100 KB), upsert, advance watermarks.
5. Typical: 1-5 companyfacts pulls. Wall time 5-30 seconds.

**Fresh install (no watermarks yet):**

1. `plan_refresh`: `seeds = [300 CIKs]`, `refreshes = []`, master-index still fetched for future lookback but not consulted for seed path.
2. `execute_refresh` backfills all 300. At SEC's 10 rps cap with XBRL parse + DB writes: realistic 3-5 min.
3. From run 2 onward: `seeds = []`, steady state.

**New ticker promoted to universe:**

- Universe sync + coverage upgrade land the CIK mapping in `cik_mapping` + `coverage`.
- Next `daily_financial_facts` run: `plan_refresh` sees CIK has no `sec.submissions` watermark → `seeds = [cik]`.
- Full backfill happens for that CIK alone. Steady state thereafter.

**Missed run / downtime recovery:**

- Lookback window covers up to 7 prior days independently.
- Each lookback day is conditional-GET'd on its own Last-Modified watermark.
- Previously-seen days return 304 forever (master-index files are immutable once past).
- Any day fetched for the first time (because the stored watermark is older than the day) returns 200, parses, intersects, feeds the plan.

---

## Watermark schema

Existing `external_data_watermarks` table (no migration). Three new `source` identifiers:

| source | key | watermark | watermark_at | response_hash |
|---|---|---|---|---|
| `sec.master-index` | `YYYY-MM-DD` | HTTP `Last-Modified` value | parsed HTTP date | sha256 of body |
| `sec.submissions` | CIK (10-digit zero-padded) | top `accessionNumber` from `filings.recent` | `acceptedDate` of that accession | — |
| `sec.companyfacts` | CIK (10-digit zero-padded) | top accession at last successful XBRL pull | same as above | sha256 of body |

**Why three keys, not one:**

- `sec.master-index` per-day: one row per calendar day; bounded growth (~252/yr). `watermark` = Last-Modified for conditional-GET; `response_hash` lets us short-circuit re-parse if body identical.
- `sec.submissions` per-CIK: records the authoritative "what accession have we seen for this CIK." Comparison point for triggering companyfacts.
- `sec.companyfacts` per-CIK: separate from `sec.submissions` because submissions may advance on 8-K without triggering XBRL pull. Keeps 8-K-only filers from causing 100 KB XBRL re-fetches.

### Atomicity

`set_watermark` enforces `INTRANS` at runtime (existing behaviour from #269). Each CIK's refresh wraps provider calls, XBRL upsert, and watermark writes inside one `with conn.transaction()`. Crash mid-ingest rolls back both — next run's planner sees unchanged watermark and replans that CIK.

### Retention

`sec.master-index` rows: unbounded-ish but cheap (~252/yr). No prune scheduled.
`sec.submissions` / `sec.companyfacts`: one row per covered US CIK. When a CIK drops from the universe, the watermark row is retained (harmless; zero cost).

---

## Error handling + edge cases

### HTTP

| Condition | Behaviour |
|---|---|
| 304 on master-index | No body read. No DB write. Move to next lookback day. |
| 200, body hash matches stored | Skip parse; refresh `fetched_at` only. |
| 200, new body | Parse, intersect, update watermark + hash atomically. |
| 429 / 503 / `Retry-After` | Respect via existing `resilient_client.py` backoff. 10 rps SEC cap already enforced there. |
| 5xx from `data.sec.gov` | Retry 3x with exponential backoff. On exhaustion, fail that CIK only; layer continues. |
| 403 / missing `User-Agent` | Hard-fail the layer. Existing SEC provider config enforces UA with email; regression surfaces at boot. |
| Network timeout | Per-CIK isolation — one CIK's timeout does not fail the layer. |

### Data

| Condition | Behaviour |
|---|---|
| Master-index entry for non-covered CIK | Ignored during intersect. Zero cost. |
| `filings.recent.accessionNumber` empty | Seed with sentinel watermark so we don't refetch every run. |
| CIK in master-index hits but top accession unchanged | No companyfacts fetch (amendments or duplicate listings). |
| Form-type not in fundamentals set | Advance `sec.submissions` watermark only. No companyfacts fetch. |
| CIK dropped from universe mid-cycle | Skipped from `covered_us_ciks`. Watermark rows retained. |
| `companyfacts.json` 404 (private / de-registered) | Watermark gets sentinel `accession#404`; stop retrying. Surface in coverage audit. |
| Malformed JSON | Transaction rolls back; per-CIK fail; retry next run. |
| `acceptedDate` in future (clock skew) | Accept as-is — audit field only, not a gate. |

### Concurrency

- `daily_financial_facts` already holds a job lock via `_run_with_lock`. Concurrent runs no-op at planner entry.
- Mid-run crash: partial commits safe — watermarks only advance with successful data writes. Next run replans from watermark state alone. No resume file, no checkpoint.

### Observability

- `sync_layer_progress.items_processed` counts seeded + refreshed + submissions-only advances.
- `sync_runs.notes` records per-CIK failure class + count.
- Structured log per plan: `plan.seeds.count`, `plan.refreshes.count`, `plan.submissions_only.count`, `master_index.days_fetched`, `master_index.days_304`.

---

## Testing strategy

### Unit — `tests/test_sec_incremental.py`

| Scenario | Assertion |
|---|---|
| `plan_refresh` — zero watermarks, non-empty cohort | All covered CIKs in `seeds`; `refreshes` empty. |
| `plan_refresh` — watermarks present, master-index all 304 | `seeds=[]`, `refreshes=[]`. Zero submissions calls. |
| `plan_refresh` — master-index hit, accession changed, form 10-Q | CIK in `refreshes`. |
| `plan_refresh` — master-index hit, accession unchanged | CIK NOT in `refreshes`; watermark `fetched_at` still advances. |
| `plan_refresh` — master-index hit, form 8-K only | CIK in `submissions_only_advances`; NOT in `refreshes`. |
| `plan_refresh` — master-index hit, non-covered CIK | Ignored. |
| `plan_refresh` — partial lookback (6 days 304, 1 day 200) | Only the 200 day parsed. |
| `execute_refresh` — seed happy path | `full_backfill` called; both watermarks set; one commit. |
| `execute_refresh` — refresh happy path | `companyfacts` called; both watermarks advanced. |
| `execute_refresh` — one CIK fails mid-loop | Other CIKs still processed; failed CIK's watermarks NOT advanced. |
| `execute_refresh` — crash after upsert before watermark | Rolled back together; next run replans same CIK. |

### Mocking discipline

- Provider HTTP = fakes returning recorded fixtures.
- DB = real `ebull_test` Postgres — never mock cursor. Watermark atomicity verified in real transaction.
- Time = `freezegun` for lookback-window math.
- Internal helpers (`get_watermark`, `set_watermark`) NOT mocked — call through.

### Fixtures — `tests/fixtures/sec/`

- `master_20260415.idx` — trimmed real sample (public SEC data).
- `submissions_AAPL.json` — trimmed `filings.recent` top 3.
- `companyfacts_AAPL.json` — trimmed to 2-3 concepts.

### Integration — `tests/test_sync_orchestrator_financial_facts_incremental.py`

- Seed covered US cohort of 3 CIKs, no watermarks.
- Stub provider with fixtures.
- Run `daily_financial_facts` through scheduler entrypoint.
- Assert: 3 CIKs seeded; 3 watermarks exist; fundamentals rows land; `sync_run` marked success.
- Re-run same job → planner returns empty plan → zero provider calls.

### Pre-push gates (per `.claude/CLAUDE.md`)

- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run pyright`
- `uv run pytest`

No frontend changes.

---

## Expected impact

| Metric | Today | After | Change |
|---|---|---|---|
| Daily SEC requests | ~600 (300 submissions + 300 companyfacts) | ~15 (7 master-index + 1-5 submissions + 1-5 companyfacts) | -98% |
| Daily SEC bytes | ~50-100 MB | ~750 KB | -99% |
| Research refresh wall time | 45 min | 5-30 sec typical; 3-5 min on fresh install | -90%+ |
| Initial backfill for a new ticker | entire universe pass | single CIK backfill | scoped |

---

## Settled decisions preserved

- **Provider boundary** (settled-decisions.md §Provider design rule): new `fetch_master_index` added to SEC provider, stays thin HTTP — no DB access, no domain orchestration.
- **Filing lookup rule** (§Identifier strategy): SEC lookups use CIK, not symbol. Watermark keys are CIK-indexed (zero-padded 10 digits).
- **Fundamentals snapshot semantics** (§Fundamentals snapshot semantics): `as_of_date` = period end date. Unchanged. XBRL upsert logic untouched.
- **Auditability** (§General engineering decisions): every plan logs seed/refresh/skip counts + per-CIK failure detail via `sync_layer_progress`.

No settled decision altered.

---

## Review-prevention log hits

- Atomic-transaction pattern: watermark write + data upsert in same `with conn.transaction()` — enforced by existing `set_watermark` INTRANS check (#269).
- SQL correctness: positional-access `ORDER BY` patterns followed in cohort load.
- Test quality: real DB for integration; real transactions in atomicity test; no cursor mocks.
- Python hygiene: `from __future__ import annotations`; `Sequence` for read-only inputs; `Literal` for form-type set.

---

## Open follow-ups (out of scope for this ticket)

- **#273 thesis event-driven trigger** — consumes the `sec.submissions` watermark advance signal after this ships.
- **#268 filings coverage bar** — separate enforcement; orthogonal to this fetch path.
- **Per-CIK rate shaping inside seed path** — if fresh install of 300 CIKs strains SEC 10 rps cap beyond acceptable, revisit with per-CIK throttling. Current expectation: existing `resilient_client.py` backoff is sufficient.
