# PR11 — SEC Schedule 13D / 13G activation + 3y retention cap design

> Created: **2026-05-21** during PR11 brainstorming under #1233 (data-retention rubric umbrella).
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
> Parent spec: `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` §4.8.
>
> Status: **DRAFT** — pending Codex 1a/1b spec review, then operator sign-off, then implementation plan.

## 0. Why this design exists

PR11 sits inside the #1233 data-retention rubric as **§4.8 13D/G blockholders** — described in the parent spec as "activate dormant pipeline + 3y historical + current-state cap at the parser." The parent spec's volume estimate (`0 ingested; pipeline not yet active`) is materially correct: every code surface (schema, manifest parser, observations sync, API endpoint, tests) is wired and exercised by unit tests, but **no discovery path enqueues universe-issuer-relevant SC 13D/G filings into `sec_filing_manifest`**, so the worker never fires and the observations / current tables stay empty universe-wide.

The chicken-and-egg root cause (audited 2026-05-21):

1. SEC SC 13D/G is filed BY the 5%+ holder, NOT by the issuer. AAPL's `data.sec.gov/submissions/CIK0000320193.json` does NOT contain SC 13D filings against AAPL.
2. Existing daily-index reconcile (`app/jobs/sec_daily_index_reconcile.py`) catches SC 13D/G by filer CIK and tries to resolve subject via `default_subject_resolver` (`app/jobs/sec_atom_fast_lane.py:57-101`), which checks `blockholder_filers` (auto-populated by parser on first successful ingest — chicken-and-egg with no bootstrap seed) + `blockholder_filer_seeds` (operator-curated, **currently empty universe-wide**).
3. With both tables empty on bootstrap, the resolver returns no subject; daily-index discovery silently drops the row; worker is never invoked; `blockholder_filers` is never auto-populated; the bootstrap path is forever dormant.

PR11 unblocks this by adding a **universe-issuer-CIK-driven discovery path** (no hardcoded filer seed list, per operator mandate "the data we pull is dictated based on what instruments eToro provide; everything else should populate downstream of that as a filter") + a 3y `filed_at` cap at every writer chokepoint following the PR5–PR8 placement-invariant lint-guard pattern + a same-PR retirement of the dormant filer-seed code path so no tech debt is left behind.

## 1. Mandate

This spec is delivered under the following operator-stated constraints (2026-05-21):

1. **100% complete in PR11** — no follow-up tickets for in-scope work, no "we'll come back to it later".
2. **No tech debt** — dormant code paths retired in the same PR that ships the live one.
3. **No hardcoded lists** — the data we pull is dictated by what eToro provides; the SEC universe filters downstream of `instruments.country='US' AND is_tradable=TRUE` (PR1, settled).
4. **Sensible depth** — 3y per parent spec §4.8; no "century of data".

These constraints are load-bearing for the design calls below.

## 2. Audit baseline

### 2.1 Code surface as of 2026-05-21

| Layer | File:line | Status |
|---|---|---|
| Schema — filer registry | `sql/095_blockholder_filers_filings.sql:71` | LIVE (auto-populated by parser on successful ingest) |
| Schema — raw chain | `sql/095_blockholder_filers_filings.sql:78` | LIVE (one row per reporter-per-accession; populated by live manifest parser) |
| Schema — seed table | `sql/096_blockholder_filer_seeds_and_log.sql:30` | DORMANT (empty universe-wide; read by dormant ingester) |
| Schema — attempt log | `sql/096_blockholder_filer_seeds_and_log.sql:63` | LIVE (written by both live manifest parser + dormant ingester) |
| Schema — observations | `sql/115_ownership_blockholders_observations.sql:27` | LIVE (append-only; written by manifest parser via `_record_13dg_observation_for_filing`) |
| Schema — current snapshot | `sql/115_ownership_blockholders_observations.sql:102` | LIVE (write-through via `refresh_blockholders_current`) |
| Provider | `app/providers/implementations/sec_13dg.py:455` | LIVE (`parse_primary_doc()` — pure XML→BlockholderFiling) |
| Manifest parser | `app/services/manifest_parsers/sec_13dg.py:95` | LIVE (registered to `sec_13d` + `sec_13g`; uses lower-level helpers from `blockholders.py`) |
| Service — live helpers | `app/services/blockholders.py` (`_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing`, `_resolve_cusip_to_instrument_id`, `_archive_file_url`, `_record_ingest_attempt`, etc.) | LIVE (used by manifest parser) |
| Service — dormant entrypoints | `app/services/blockholders.py:869` (`ingest_all_active_filers`), `:804` (`ingest_filer_blockholders`) | DORMANT (per-filer walker; reads seed table; never called in production) |
| Observation sync | `app/services/ownership_observations_sync.py:414` (`sync_blockholders`) | LIVE (called by `sync_all`); pre-existing path may need cap gate per PR11 |
| API endpoint | `app/api/instruments.py:3563` (`GET /{symbol}/blockholders`), `:4054` (rollup category) | LIVE (renders empty for every instrument today) |
| Bootstrap stage | `app/workers/scheduler.py` `_BOOTSTRAP_STAGE_SPECS` | **MISSING — no 13D/G bootstrap stage exists** |
| Scheduler cron stage | `app/workers/scheduler.py` | **MISSING — no scheduled 13D/G discovery job** |
| Atom fast-lane resolver | `app/jobs/sec_atom_fast_lane.py:57-101` | LIVE (drops 13D/G when filer unknown — gap above) |
| Daily-index reconcile | `app/jobs/sec_daily_index_reconcile.py:73-99` | LIVE (drops 13D/G when filer unknown — same gap) |
| Tests | `tests/test_sec_13dg_parser.py` (443 LOC), `tests/test_manifest_parser_sec_13dg.py` (627 LOC), `tests/test_blockholders_ingester.py` (955 LOC), `tests/test_api_blockholders.py` (516 LOC) | Mixed — parser + manifest + API tests stay live; ~50% of ingester tests exercise dormant entrypoints (retire in PR11) |
| Operator ingest rebuild | `app/api/operator_ingest.py:312` | LIVE entry in payload allow-list (already exposes `blockholder_filings` for `POST /jobs/sec_rebuild/run`) |

### 2.2 Universe baseline (post-PR1)

```text
universe   = 12,417 instruments
US filers  =  5,174 (country='US' + is_tradable=TRUE + has primary CIK in external_identifiers)
```

PR11's discovery cohort is the 5,174 US-tradable issuers. The same filter is applied at the discovery seed (no implicit dependency on PR1's lint guard; the SELECT here is explicit + grep-friendly for the PR11 lint guard).

### 2.3 Empirical SEC API verification (2026-05-21)

Run before this spec was written, per "verify before asserting":

```text
$ curl -sS -H "User-Agent: eBull luke.bradford@hotmail.co.uk" \
    "https://efts.sec.gov/LATEST/search-index?q=&forms=SC%2013D&ciks=0001326380&dateRange=custom&startdt=2023-05-21&enddt=2026-05-21"
{ hits.total.value = 3, hits.hits = [
    SC 13D/A 2024-06-11 0000921895-24-001394 ciks=['0001326380','0001822844'] names=['GameStop Corp. (GME)', 'RC Ventures LLC'],
    SC 13D/A 2024-05-24 0001193805-24-000707 ciks=['0001326380','0001822844'] names=['GameStop Corp. (GME)', 'RC Ventures LLC'],
    SC 13D/A 2023-06-13 0000921895-23-001480 ciks=['0001326380','0001822844'] names=['GameStop Corp. (GME)', 'RC Ventures LLC'],
] }

$ curl ... "...forms=SC%2013G&ciks=0001326380" (no date filter)
{ hits.total.value = 110, hits.hits[0:3] = [
    SC 13G/A 2024-02-13 ... names=['GameStop Corp. (GME)', 'VANGUARD GROUP INC'],
    SC 13G/A 2024-01-26 ... names=['GameStop Corp. (GME)', 'BlackRock Inc. (BLK)'],
    ...
] }
```

Confirmed:

- `efts.sec.gov/LATEST/search-index` is a **public JSON endpoint** (no auth, returns JSON, honours `User-Agent`). Same back-end as the EDGAR full-text-search UI at `efts.sec.gov`.
- `ciks={cik}` param matches **any CIK on the filing** — for SC 13D/G this includes the subject (issuer) AND the filer (5%+ holder). For our universe-CIK seed, the issuer is always `ciks[0]` (first position is the queried CIK) and the filer CIK(s) are `ciks[1:]`.
- `dateRange=custom&startdt=YYYY-MM-DD&enddt=YYYY-MM-DD` filters server-side on `file_date` — this is the 3y cap chokepoint A (discovery query).
- Default page size = 100. Active-blockholder-heavy issuers (e.g. GME has 110 SC 13G all-time) may need pagination via `&from={N}&size=100`. With the 3y window, the median issuer fits one page; long-tail outliers may need 2-3 pages.
- `display_names[]` carries entity labels for auto-seeding `blockholder_filers` without a follow-up fetch.

This endpoint is the load-bearing discovery primitive for PR11.

## 3. Design

### 3.1 Discovery layer (NEW)

**New file**: `app/services/sec_13dg_discovery.py`

**Responsibilities**:

1. Read the universe-issuer-CIK cohort via the canonical SELECT (post-PR1):
   ```sql
   SELECT DISTINCT i.instrument_id, ei.identifier_value AS cik
   FROM instruments i
   INNER JOIN external_identifiers ei
       ON ei.instrument_id = i.instrument_id
      AND ei.identifier_type = 'cik'
      AND ei.is_primary = TRUE
   WHERE i.country = 'US'
     AND i.is_tradable = TRUE
   ORDER BY i.instrument_id
   ```
2. For each CIK, page through `efts.sec.gov/LATEST/search-index` with:
   - `forms = SC 13D,SC 13D/A,SC 13G,SC 13G/A` (URL-encoded as `SC%2013D,SC%2013D%2FA,SC%2013G,SC%2013G%2FA`)
   - `ciks = {zero-padded 10-digit CIK}`
   - `dateRange = custom`, `startdt = blockholders_retention_cutoff().date().isoformat()`, `enddt = today.isoformat()`
   - `from = 0; size = 100`; advance `from` by 100 until `len(hits) < size`
3. For each `hit._source`:
   - Extract `accession = adsh`, `form = form`, `file_date = file_date`, `cik_list = ciks`, `name_list = display_names`
   - Identify filer CIK(s): `[c for c in cik_list if c != issuer_cik_padded]` (typically 1; joint filings can be 2+)
   - For each filer CIK + matching display name: UPSERT `blockholder_filers (cik, name)` so the subject resolver can find them (`_upsert_filer` already exists in `blockholders.py`)
   - INSERT (or no-op on conflict) into `sec_filing_manifest`:
     ```
     subject_type   = 'blockholder_filer'
     subject_id     = filer_cik (the standalone CIK, not the joint-filing concatenation)
     instrument_id  = issuer_instrument_id (the universe-resolved issuer)
     source         = 'sec_13d' if form starts with 'SC 13D' else 'sec_13g'
     accession_number = adsh
     filed_at       = file_date (parsed as date at UTC midnight; manifest schema uses TIMESTAMPTZ)
     primary_document_url = built from accession via _archive_file_url (or left NULL — parser rebuilds canonically)
     status         = 'pending'
     ```
   - Joint-filing handling: enqueue ONE manifest row per filer CIK per accession (matches `blockholder_filings` PK of `(accession, COALESCE(reporter_cik,''), reporter_name)`). The worker parser handles the per-reporter expansion downstream from one fetch of `primary_doc.xml`.
4. Rate-limit: shared SEC 10 req/s budget via the existing `app/services/sec_rate_limit.py` shared semaphore. Per-issuer query cost = 1 search-index request + 0-2 pagination requests for outlier issuers. Bootstrap = ~5,200 + ~200 outlier-page = ~5,400 requests = **~9 min wall-clock** under shared budget.
5. Return a `DiscoveryResult` dataclass:
   ```python
   @dataclass(frozen=True)
   class DiscoveryResult:
       issuers_scanned: int
       accessions_discovered: int
       manifest_rows_inserted: int
       filers_upserted: int
       rows_skipped_outside_cap: int  # always 0 since discovery query is already capped; surfaces explicit invariant
       elapsed_seconds: float
   ```

**Why one file** (not split discovery + ingest module): the discovery layer is pure HTTP+SELECT+INSERT and does NOT call the parser. It enqueues manifest rows; the existing `sec_manifest_worker` drains them. Putting discovery in `sec_13dg_discovery.py` keeps the load-bearing live module (`blockholders.py`) focused on parse + write helpers; `sec_13dg_discovery.py` mirrors the shape of `sec_n_csr_discovery` introduced under PR8.

### 3.2 Cap chokepoints (3y `filed_at`)

Helper module additions in `app/services/blockholders.py` (canonical module; matches PR8 N-CSR helper placement):

```python
INSIDER_BLOCKHOLDERS_RETENTION_YEARS = 3

def blockholders_retention_cutoff() -> datetime:
    """Sliding cutoff anchored to UTC midnight today minus 3 years."""
    return datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0) \
         - timedelta(days=365 * INSIDER_BLOCKHOLDERS_RETENTION_YEARS)

def blockholders_within_retention(filed_at: datetime | None) -> bool:
    """Inclusive predicate; treats NULL filed_at as outside retention (defensive)."""
    if filed_at is None:
        return False
    return filed_at >= blockholders_retention_cutoff()
```

Chokepoint matrix:

| # | Chokepoint | File / function | Gate kind | Test pin |
|---|---|---|---|---|
| **A** | Discovery query | `app/services/sec_13dg_discovery.py::_build_query_params` | `&startdt = blockholders_retention_cutoff().date().isoformat()` | `test_discovery_query_uses_helper_cutoff` |
| **B** | Manifest worker pre-fetch | `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` (BEFORE `fetch_document_text`) | If not `blockholders_within_retention(row.filed_at)` → tombstone with `error="retention floor"` and skip fetch | `test_parse_13dg_tombstones_pre_cap_accession` |
| **C** | Observations sync | `app/services/ownership_observations_sync.py::sync_blockholders` | LEFT JOIN `filing_events fe ON fe.provider_filing_id = bf.accession_number AND fe.provider = 'sec'`; add `WHERE fe.filing_date >= blockholders_retention_cutoff().date()` predicate (LEFT not INNER — Codex 1b PR10b lesson — so rows missing manifest entry still sync) | `test_sync_blockholders_excludes_pre_cap_rows` |
| **D** | Refresh-current | `app/services/ownership_observations.py::refresh_blockholders_current` | **UNCAPPED** — per parent spec §6.3 "refresh-current is exempt from the cap; capping it would actively delete pre-wipe pre-cap rows" (mirror of §4.5 `refresh_institutions_current` precedent) | `test_refresh_current_keeps_pre_cap_observations_intact` |
| **E** | Bulk dataset | n/a — SEC publishes no 13D/G bulk archive | no gate needed | n/a |
| **F** | Rewash | n/a — no `_apply_13dg_*` rewash function exists | no gate needed | n/a |
| **G** | One-shot single accession | `_ingest_single_accession` is not 13D/G-aware; operator-rebuild via `POST /jobs/sec_rebuild/run` re-enqueues manifest rows which then pass through gate B | no separate gate | covered by B |

The cap is **filed-at based** (matches PR8 N-CSR precedent), not `period_of_report` based, because:

- 13D/G has no `period_of_report` — the cover page records `date_of_event` (the day the 5% threshold was crossed) which lags `filed_at` by up to 10 calendar days.
- Discovery uses `file_date` from efts.sec.gov which IS the `filed_at` source of truth.
- Cap helper and discovery query speak the same vocabulary; gate B is defensive (catches any future writer that bypasses discovery — e.g. a manual `POST /jobs/sec_rebuild/run` against `sec_13d` source that re-enqueues pre-cap rows from a stale freshness index).

### 3.3 Manifest worker integration

No new worker code. Existing `sec_manifest_worker` already drains pending `sec_13d` + `sec_13g` rows via `_parse_13dg` (registered at `app/services/manifest_parsers/__init__.py:59`). PR11 changes inside `_parse_13dg`:

1. ADD: pre-fetch retention gate (chokepoint B above). Reuses `blockholders_within_retention(row.filed_at)` import from `app.services.blockholders`.
2. UNCHANGED: store_raw, parse, CUSIP resolution, upsert, observation write-through, refresh-current.
3. UNCHANGED: `subject_type='blockholder_filer'` semantics — the discovery layer guarantees `blockholder_filers` is auto-seeded BEFORE the manifest row is INSERTed, so `_upsert_filer` inside the parser body becomes a no-op for filers seeded by discovery (still functional for the legacy daily-index path where the resolver succeeds via `_upsert_filer`'s ON CONFLICT clause).

### 3.4 Cleanup — dormant code retirement (in same PR)

The operator mandate "no tech debt, no coming back later" requires retiring the dormant filer-seed-driven path in the same PR that activates the live one. Concretely:

**DELETE**:

- `app/services/blockholders.py::ingest_all_active_filers` (entry-point that walks `blockholder_filer_seeds`)
- `app/services/blockholders.py::ingest_filer_blockholders` (per-filer walker; dependency-only of `ingest_all_active_filers` — no other caller surfaces in the repo)
- `app/services/blockholders.py::_list_active_filer_seeds` (helper that reads the seed table)
- The corresponding test cases in `tests/test_blockholders_ingester.py` that exercise the deleted entrypoints (estimated ~400-500 LOC of the 955; the lower-level helper tests — `_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing` — stay because the live manifest path uses them)

**DROP** (new migration `sql/15X_drop_blockholder_filer_seeds.sql`):

```sql
DROP INDEX IF EXISTS idx_blockholder_filer_seeds_active;
DROP TABLE IF EXISTS blockholder_filer_seeds;
```

**KEEP**:

- `app/services/blockholders.py` lower-level helpers (`_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing`, `_resolve_cusip_to_instrument_id`, `_archive_file_url`, `_record_ingest_attempt`, etc.) — actively used by the live manifest parser at `manifest_parsers/sec_13dg.py:60-69`.
- `blockholder_filers` table — auto-populated by discovery + parser via `_upsert_filer`; required for resolver lookup; required for `blockholder_filings` FK.
- `blockholder_filings` table — raw chain; populated by parser; read by `sync_blockholders` and ownership rollup.
- `blockholder_filings_ingest_log` table — written by parser via `_record_ingest_attempt`; required for operator audit.
- All ownership_blockholders_* tables and refresh paths.
- All parser, provider, API, and rollup code paths.

**Test impact**:

- `tests/test_blockholders_ingester.py`: ~50% of cases reference `ingest_all_active_filers` or `ingest_filer_blockholders`; those cases are deleted. The remaining cases (lower-level helper tests + sync tests) stay.
- `tests/test_manifest_parser_sec_13dg.py`: unchanged shape; adds one new test case for the retention-gate tombstone branch (chokepoint B).
- `tests/test_sec_13dg_parser.py`: unchanged.
- `tests/test_api_blockholders.py`: unchanged.
- NEW `tests/test_sec_13dg_discovery.py`: covers the new discovery module — fake efts.sec.gov response fixtures + assertions on filer/manifest seeding + invariant on no out-of-window rows + pagination behaviour.
- NEW `tests/test_ownership_observations_sync_blockholders_cap.py` (or fold into existing): covers chokepoint C gate.

### 3.5 Bootstrap stage + scheduler wiring

**NEW bootstrap stage** in `app/workers/scheduler.py` `_BOOTSTRAP_STAGE_SPECS`:

```python
"sec_blockholders_discovery": StageSpec(
    stage_key="sec_blockholders_discovery",
    lane="sec_rate",                # shared 10 req/s SEC budget
    job_name="sec_blockholders_discovery_job",
    order=...,                       # post-PR1, post-PR5 (DEF14A discovery); pre-N-PORT? — set during impl
    description="Universe-issuer-CIK-driven SC 13D/G discovery via efts.sec.gov, 3y cap",
    prerequisite="sec_universe_sync",  # depends on country='US' + is_tradable being populated
    params={},                       # full-cohort scan; no dynamic resolution
    max_runtime_seconds=3600,        # 1h default — should drain in ~10min for 5,174 issuers under shared budget
)
```

**NEW scheduler job** `sec_blockholders_discovery_job` (nightly, parallel to other SEC discovery jobs):

```python
@register_job("sec_blockholders_discovery_job", JobLane.SEC_RATE)
def sec_blockholders_discovery_job(params: dict[str, Any]) -> JobResult:
    """Walk universe US-tradable issuer CIKs; enqueue SC 13D/G within 3y to manifest."""
    result = discover_sec_13dg_for_universe(  # NEW in app/services/sec_13dg_discovery.py
        sliding_window_days=14 if params.get("steady_state") else 365 * 3,
        ...
    )
    return JobResult(...)
```

Steady-state cron uses a tight 14d sliding window (catches new filings + amendments); bootstrap uses full 3y. The discriminator is the explicit `steady_state` flag in params (operator-controllable via admin UI).

**`data_freshness_index` integration**: NEW entries for `sec_13d` and `sec_13g` lanes per #863 — three-tier polling (fresh / stale / dormant). The discovery job + worker drain update `data_freshness_index.last_observed_at` automatically.

### 3.6 Lint guard

NEW `scripts/check_13dg_retention.sh` (pre-push hook; PR5-style awk block walker; mirror of `check_n_csr_retention.sh`):

Invariants:

- **A — helpers present**: `app/services/blockholders.py` defines exactly one `def blockholders_retention_cutoff(` and one `def blockholders_within_retention(`. (Greps for the literal `def ` prefix on both names; fails if count ≠ 1 or if defined outside `blockholders.py`.)
- **B — discovery query uses helper**: `app/services/sec_13dg_discovery.py` contains `startdt = blockholders_retention_cutoff()` (or equivalent assignment whose RHS is the helper call). Empty-grep `wc -l` guard per PR10a Codex iter 1 lesson.
- **C — manifest gate placed BEFORE fetch**: `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` calls `blockholders_within_retention(` on a line whose line number precedes the first `fetch_document_text(` call inside the same function block. Awk-based block walker (PR4 Codex 1c lesson).
- **D — sync uses helper + LEFT JOIN**: `app/services/ownership_observations_sync.py::sync_blockholders` contains both `LEFT JOIN filing_events` and `blockholders_retention_cutoff()` within the same function body (Codex 1b PR10b lesson — LEFT not INNER, so rows missing manifest entry still sync).
- **E — refresh-current is exempt**: forbid any reference to `blockholders_retention_cutoff` or `blockholders_within_retention` inside `refresh_blockholders_current(` function block (the §4.5 13F-HR precedent: capping refresh would actively delete pre-wipe rows from `_current`).
- **F — no append writers outside the helper-gated chokepoints**: forbid raw `INSERT INTO ownership_blockholders_observations` and raw `INSERT INTO blockholder_filings` outside (a) `app/services/blockholders.py` lower-level helpers and (b) the manifest parser. Catches future PRs that add a side-path writer skipping the cap.
- **G — dormant entrypoints stay deleted**: forbid the literal symbols `ingest_all_active_filers` and `ingest_filer_blockholders` from re-appearing anywhere in `app/`. Catches accidental resurrection.

Wired into `.githooks/pre-push` after the existing PR10b `check_form3_latest_per_pair.sh` and `check_form5_retention.sh` invocations.

### 3.7 Migration

NEW `sql/15X_drop_blockholder_filer_seeds.sql`:

```sql
-- Drop dormant filer-seed table; PR11 retires the operator-curated
-- seed mechanism in favour of universe-issuer-CIK-driven discovery
-- via efts.sec.gov. The seed table was empty universe-wide (never
-- populated). All downstream consumers of the seed list
-- (ingest_all_active_filers, ingest_filer_blockholders, the atom
-- fast-lane subject resolver's seed-list lookup branch) are removed
-- in the same PR. `_PLANNER_TABLES` updated to remove the dropped
-- table.
DROP INDEX IF EXISTS idx_blockholder_filer_seeds_active;
DROP TABLE IF EXISTS blockholder_filer_seeds;
```

`tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` updated to remove `blockholder_filer_seeds` (per the "When a migration adds OR drops any table with a FK relationship, update `_PLANNER_TABLES`" prevention-log entry).

The atom fast-lane resolver and daily-index reconcile resolver are also edited: the seed-list lookup branch is removed; the `blockholder_filers` lookup branch stays (now the SOLE resolution path; populated by PR11's discovery layer upstream of the manifest insert).

### 3.8 Acceptance criteria (per CLAUDE.md ETL clauses 8-12)

1. **Smoke-tested against panel** (AAPL, GME, MSFT, JPM, HD) on dev DB after merge. Expected: GME has ≥3 SC 13D/A (RC Ventures activist trail) + multiple SC 13G (Vanguard/BlackRock); AAPL likely has 0-1 SC 13D + handful of SC 13G; MSFT/JPM/HD have institutional 13G filings.
2. **Cross-source verified**: At least one observation row's `(filer_cik, filed_at, percent_of_class)` cross-checked against SEC EDGAR direct (`https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<issuer>&type=SC+13`).
3. **Backfill executed**: `POST /jobs/sec_rebuild/run` with `{"source": "sec_13d"}` and `{"source": "sec_13g"}` after merge. Manifest worker drains. PR description records the job invocations + final manifest pending counts.
4. **Operator-visible figure verified**: `GET /instruments/GME/blockholders` and `/ownership-rollup?category=blockholders` render the RC Ventures + Vanguard/BlackRock entries post-backfill. PR description records the rendered figures.
5. **PR description records the verification step + commit SHA** for each of clauses 1-4.

### 3.9 Provenance + parent spec amendments

Same PR amends `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`:

- §4.8 — replace "Current volume: 0 ingested (table exists; pipeline not yet active)" with "Volume: backfilled in PR11 (#TBD) via efts.sec.gov universe-issuer-CIK discovery + 3y cap"
- §4.8 — replace "Ingest depth cap: 3y historical at the parser + current state always" with the concrete chokepoint matrix (mirror format from §4.5/§4.6/§4.7/§4.8 PR-shipped sections)
- §7 PR11 entry — replace "13D/G activate dormant pipeline with 3y historical + current-state cap at the parser" with the SHIPPED summary (chokepoint coverage, dormant-code retirement, lint guard, migration)
- §11 Codex review gate — add PR11 entry to the cadence log
- §12 Handover — mark PR11 SHIPPED; PR12 (`ownership_*_current` size audit) remains as the final spec PR

## 4. Risks + mitigations

| Risk | Mitigation |
|---|---|
| efts.sec.gov endpoint changes shape / undocumented | Fallback to `data.sec.gov/submissions/CIK{filer}.json` per-filer walk via a `blockholder_filers` view of known filers; if endpoint flat-out unavailable, discovery is no-op and operator paged via #863 freshness index |
| Pagination edge case — issuer with >100 results in 3y window | Loop until `len(hits) < size`; smoke-test confirms 99th-percentile fits in 2 pages |
| Joint-filing identification — `ciks[]` may have >2 entries | Filer set = `set(cik_list) - {issuer_cik_padded}`; enqueue one manifest row per filer; parser's per-reporter expansion downstream handles the joint case via existing `BlockholderFiling.reporting_persons` walker |
| SEC rate-limit budget contention with other lanes during bootstrap | Stage uses `lane="sec_rate"` (shared semaphore); 5,400-request bootstrap discovery scan = ~9min wall-clock; fits inside default `max_runtime_seconds=3600` with 5x headroom |
| Auto-populated `blockholder_filers.name` is sometimes joint-filing label (e.g. "RC Ventures LLC and Ryan Cohen") | Accept name as-is from `display_names[i]`; the resolver only joins on CIK; name is operator-visible label only — no functional impact |
| Resurrection of dormant seed-driven path by future PR | Lint guard G forbids re-introduction of the deleted symbols; migration drops the seed table so the resurrection would also need to re-add the schema (loud, not silent) |
| Existing `blockholder_filings` rows from prior smoke tests (if any) survive PR11 with stale parser_version | Per parent spec §6.3, existing rows untouched until pre-wipe; PR11 introduces no `parser_version` bump; if prior smoke-test rows exist they're either correct (live manifest path semantics preserved) or rebuilt by post-merge `POST /jobs/sec_rebuild/run` |

## 5. Out of scope

- **Filer-side 13D/G discovery** (per-FILER-CIK submissions walk, e.g. "every SC 13D Carl Icahn ever filed regardless of which issuer"). The retired dormant path attempted this; it's incomplete coverage by design (only the seeded filer set) and is replaced by the complete universe-issuer-CIK-driven discovery in PR11. If a future ticket genuinely needs filer-side coverage (e.g. surfacing every activist filing by Pershing Square across all issuers, including non-universe ones), that's a separate epic.
- **Form 144 / Form D / NT 10-Q** etc. (other metadata-only forms — parent spec §4.14 covers them under `filing_events` 10y cap).
- **Active blockholder alerts** (parent spec §5.3 — future alert epic). PR11 lands the data; the alert wire-up is its own ticket.
- **Frontend chart redesign** (parent spec §10 — separate epic).
- **#1010-style filer-recency cohort bound**. 13D/G filers are not a curated cohort (no `blockholder_filers.last_active_at` parallel to `institutional_filers.last_13f_hr_at`); discovery is universe-issuer-CIK-driven so the "shed inactive filers" cohort bound doesn't apply.

## 6. Implementation sequencing (preview — full plan in writing-plans output)

1. Schema migration `sql/15X_drop_blockholder_filer_seeds.sql` + `_PLANNER_TABLES` update.
2. Helper additions in `app/services/blockholders.py` (`blockholders_retention_cutoff` + `blockholders_within_retention`).
3. NEW `app/services/sec_13dg_discovery.py` (discovery module + `discover_sec_13dg_for_universe` entry-point).
4. Cap gate in `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` (chokepoint B, pre-fetch).
5. Sync gate in `app/services/ownership_observations_sync.py::sync_blockholders` (chokepoint C, LEFT JOIN + filed_at filter).
6. Resolver edits in `app/jobs/sec_atom_fast_lane.py` + `app/jobs/sec_daily_index_reconcile.py` (remove seed-list lookup branch; keep `blockholder_filers` lookup).
7. DELETE dormant code from `app/services/blockholders.py` (`ingest_all_active_filers`, `ingest_filer_blockholders`, `_list_active_filer_seeds`).
8. NEW scheduler job + bootstrap stage in `app/workers/scheduler.py`.
9. NEW lint guard `scripts/check_13dg_retention.sh` + wire into `.githooks/pre-push`.
10. NEW + UPDATED tests (parser cap, discovery, sync cap, refresh-current uncapped, dormant-symbol absence).
11. Amend parent spec (`docs/superpowers/specs/2026-05-19-data-retention-rubric.md` §4.8, §7, §11, §12).
12. PR description with the ETL clause 8-12 evidence + Codex 2 pre-push review.
