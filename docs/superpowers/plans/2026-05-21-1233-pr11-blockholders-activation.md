# PR11 — SEC SC 13D/G activation + 3y cap implementation plan (v3)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Activate the dormant SEC Schedule 13D/13G pipeline. Universe-issuer-CIK-driven discovery via `efts.sec.gov/LATEST/search-index`. Retention floor = `max(today − 3y, 2024-12-18)` (SEC XBRL mandate). Retire dormant filer-seed code path in the same PR.

**Architecture:** New `app/services/sec_13dg_discovery.py` walks `instruments WHERE country='US' AND is_tradable=TRUE`, queries efts per issuer CIK, writes `sec_filing_manifest` rows + multi-row `sec_13dg_discovery_issuer_hint` side-table in one transaction. Existing `manifest_parsers/sec_13dg.py::_parse_13dg` swapped to `edgartools.beneficial_ownership.schedule13.Schedule13D.parse_xml` via a new `_schedule13_adapter.py` (dict → repo `BlockholderFiling`). Cap chokepoints A/B/C/F at discovery query / manifest pre-fetch / sync / rewash rescue-path. Refresh-current EXEMPT.

**Tech stack:** Python 3.14 / psycopg3 / FastAPI / PostgreSQL 17 / edgartools 5.30.2 / pytest + pytest-testmon / awk-based pre-push lint.

**Spec:** `docs/superpowers/specs/2026-05-21-pr11-blockholders-activation-design.md` v7.2 (Codex 1g APPROVED 2026-05-21; v7.2 = 2024-12-18 mandate alignment).

## v3 strategy note

Plan v1/v2 inlined verbatim production code (test bodies, adapter modules, SQL fixtures). Each Codex round caught a code-block detail bug (wrong XML element names, wrong schema columns, wrong function signatures, self-grep against own forbidden symbols). Root cause: writing fixture code without running it.

v3 pivots to imperative shape: each task lists files + cites canonical source paths (`sql/115:60-65`, `.venv/.../schedule13.py:167`, `app/services/rewash_filings.py:185`) + acceptance criteria for tests. Subagent executor reads the canonical sources at code-time, writes tests that match the live contract, runs them, iterates. Plan no longer ages out when downstream symbols rename.

## Canonical source-of-truth references (read once before any task)

- **Spec**: `docs/superpowers/specs/2026-05-21-pr11-blockholders-activation-design.md` v7.2 — every design decision; cite §X.Y in task notes
- **Parent spec**: `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` §4.8 — overall rubric position for SC 13D/G
- **Manifest schema**: `sql/118_sec_filing_manifest.sql` — CHECK constraints; PK; subject_type enum
- **Manifest helper**: `app/services/sec_manifest.py:194-300` — `record_manifest_entry` contract (returns `None`; unconditional `ON CONFLICT DO UPDATE`)
- **Blockholders schema**: `sql/095_blockholder_filers_filings.sql` (raw chain) + `sql/115_ownership_blockholders_observations.sql` (observations + current)
- **Filing-agent CIKs**: `app/providers/implementations/sec_edgar.py:83-104` (`KNOWN_FILING_AGENT_CIKS`) + the `_zero_pad_cik` helper
- **SEC rate-limit clock**: `app/providers/implementations/sec_edgar.py:55-80` (`_MIN_REQUEST_INTERVAL_S`, `_PROCESS_RATE_LIMIT_CLOCK`, `_PROCESS_RATE_LIMIT_LOCK`)
- **edgartools Schedule13D/G**: `.venv/lib/python3.14/site-packages/edgar/beneficial_ownership/schedule13.py` (`Schedule13D.parse_xml` / `Schedule13G.parse_xml`) + `.venv/lib/.../models.py` (`IssuerInfo` / `SecurityInfo` / `ReportingPerson` / `Signature`)
- **edgartools skill**: `.claude/skills/data-sources/edgartools.md` G11 (HTML pre-mandate gap) + G15 (dict-shape contract)
- **Existing live parser**: `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` — current `parse_primary_doc` call site; downstream `_upsert_filing_row` / `_record_13dg_observation_for_filing` consumers in `blockholders.py`
- **Internal dataclass shape**: `app/providers/implementations/sec_13dg.py:77-180` (`BlockholderReportingPerson` + `BlockholderFiling` field-by-field)
- **Rewash dispatcher**: `app/services/rewash_filings.py:185` — calls `spec.apply_fn(conn, raw_doc)` (2-arg contract); `_apply_blockholders` signature MUST match
- **Bootstrap stage spec**: `app/services/bootstrap_orchestrator.py:859` (`_BOOTSTRAP_STAGE_SPECS`) + `:1961` (hard `len == 26` assertion — bump to 27)
- **Universe filter**: post-PR1 `instruments.country='US' AND is_tradable=TRUE` + `external_identifiers` primary `cik` join
- **Cleanup PR #1251 precedent**: `scripts/check_archive_url_agent_guard.sh` (same lint shape) + `app/services/manifest_parsers/sec_13f_hr.py` agent-CIK guard added there (mirror pattern)

## Settled decisions honoured

- PR1 universe filter (`country='US' AND is_tradable=TRUE`)
- `sec_filing_manifest` CHECK: `subject_type='blockholder_filer' → instrument_id IS NULL`
- Refresh-current EXEMPT per parent spec §6.3
- `KNOWN_FILING_AGENT_CIKS` defense per PR #1251
- 2024-12-18 SEC Schedule 13 XBRL mandate
- Two-layer ownership model #788

## Prevention-log entries honoured

- Grep `KNOWN_FILING_AGENT_CIKS` before designing archive-URL flows
- Read manifest CHECK constraints before designing manifest semantics
- edgartools `parse_xml` returns top-level dict; nested values are dataclasses (attr access)
- Bootstrap recency constants must be namespaced per source
- Pre-push xdist + Postgres-lock-OOM → `--no-verify` justified when impacted-files clean + Codex green

---

## Phase 1 — Schema migrations (ADD only)

> **Codex 1a HIGH ordering**: drop migration `sql/161_drop_blockholder_filer_seeds.sql` lands in Phase 8 Task 8.5 AFTER all live references are removed.

### Task 1.1: New migration `sql/159_create_sec_13dg_discovery_issuer_hint.sql`

**Files:** Create `sql/159_create_sec_13dg_discovery_issuer_hint.sql`.

**Reference:** spec §3.4 hint-table block (multi-row PK `(accession_number, instrument_id)`); `sql/118_sec_filing_manifest.sql` for CASCADE pattern.

- [ ] **Step 1**: Write the migration. Schema: `accession_number TEXT NOT NULL`, `instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE`, `issuer_cik TEXT NOT NULL`, `discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, PK `(accession_number, instrument_id)`. Two indexes: `(accession_number)` and `(instrument_id)`. Header comment cites #1233 PR11 + Codex 1b BLOCKING #2 sibling rationale.
- [ ] **Step 2**: Apply locally via `docker exec -i ebull-postgres psql -U postgres -d ebull < sql/159_*.sql`. Verify with `\d sec_13dg_discovery_issuer_hint`.
- [ ] **Step 3**: Commit `feat(#1233): add sec_13dg_discovery_issuer_hint table (PR11)`.

### Task 1.2: Add `sec_13dg_discovery_issuer_hint` to `_PLANNER_TABLES`

**Files:** Modify `tests/fixtures/ebull_test_db.py`.

> Add only; the `blockholder_filer_seeds` row stays until Task 8.5 (post-cleanup drop).

- [ ] **Step 1**: Grep `_PLANNER_TABLES` definition.
- [ ] **Step 2**: Add `"sec_13dg_discovery_issuer_hint"` in the alphabetical/grouped position matching surrounding entries.
- [ ] **Step 3**: Verify both names are present: `uv run python -c "from tests.fixtures.ebull_test_db import _PLANNER_TABLES; assert 'sec_13dg_discovery_issuer_hint' in _PLANNER_TABLES; assert 'blockholder_filer_seeds' in _PLANNER_TABLES"`.
- [ ] **Step 4**: Commit.

---

## Phase 2 — Retention helpers + constants

### Task 2.1: Add helpers in `app/services/blockholders.py`

**Files:** Modify `app/services/blockholders.py`; create `tests/test_blockholders_retention_helpers.py`.

**Reference:** spec §3.2 helper block; canonical mandate date 2024-12-18 per SEC EDGAR Release 23.4.

- [ ] **Step 1**: Write failing test `tests/test_blockholders_retention_helpers.py` asserting: `INSIDER_BLOCKHOLDERS_RETENTION_YEARS == 3`; `SEC_SCHEDULE_13_XML_MANDATE_DATE == date(2024, 12, 18)`; `blockholders_retention_cutoff()` returns `date` (not `datetime`); cutoff clamps to mandate when `today − 3y < mandate`; cutoff uses `today − 3y` once `today − 3y >= mandate`; `blockholders_within_retention(None)` is `False`; inclusive at cutoff; rejects strictly-before.
- [ ] **Step 2**: Run, verify ImportError.
- [ ] **Step 3**: Implement helpers per spec §3.2 code block. Helper returns `date` (NOT `datetime`); predicate accepts `datetime | None` and compares `.date()`.
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

---

## Phase 3 — Provider efts.sec.gov method

### Task 3.1: Add `SecFilingsProvider.fetch_search_index_json`

**Files:** Modify `app/providers/implementations/sec_edgar.py`; create `tests/test_sec_edgar_search_index.py`.

**Reference:** spec §3.5 rate-limit surface + sec-edgar skill §1 endpoint shape; `_http_tickers` already shares the process-wide throttle.

- [ ] **Step 1**: Write failing test asserting (a) URL constructed correctly with `efts.sec.gov/LATEST/search-index?` + URL-encoded `forms` + `ciks` + `dateRange=custom` + `startdt` + `enddt` + `from` + `size`; (b) pagination `from_offset=100` round-trips; (c) 404 → `None`; (d) provider call routes through the existing `_http_tickers` client (monkeypatch at provider instance).
- [ ] **Step 2**: Run, verify AttributeError.
- [ ] **Step 3**: Implement method. Signature `fetch_search_index_json(*, ciks: str, forms: tuple[str, ...], startdt: date, enddt: date, from_offset: int = 0, size: int = 100) -> dict | None`. Mirror `fetch_filing_index` error-handling shape: 404 → `None`, other errors raise via `resp.raise_for_status()`. Use `urllib.parse.urlencode` for params.
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

---

## Phase 4 — Discovery module

### Task 4.1: Stub `app/services/sec_13dg_discovery.py` + `DiscoveryResult` dataclass

**Files:** Create `app/services/sec_13dg_discovery.py`.

**Reference:** spec §3.1 discovery responsibilities + §3.5 metric counters.

- [ ] **Step 1**: Write module docstring + imports (`KNOWN_FILING_AGENT_CIKS`, `SecFilingsProvider`, `_zero_pad_cik`, `_upsert_filer`, `blockholders_retention_cutoff`, `record_manifest_entry`).
- [ ] **Step 2**: Define `DiscoveryResult` dataclass with fields per spec §3.1 step 6: `issuers_scanned`, `accessions_discovered`, `manifest_rows_inserted`, `manifest_rows_skipped_existing`, `filers_upserted`, `hints_written`, `rows_skipped_outside_cap`, `elapsed_seconds`.
- [ ] **Step 3**: Commit skeleton.

### Task 4.2: `_resolve_discovery_startdt` watermark helper

**Files:** Modify `app/services/sec_13dg_discovery.py`; create `tests/test_sec_13dg_discovery.py`.

**Reference:** spec §3.5 helper code block (watermark = `MAX(blockholder_filings.filed_at) WHERE issuer_cik = ?`, clamped to retention floor; `mode='bootstrap'` → floor regardless of watermark).

- [ ] **Step 1**: Write 3 failing tests: (a) bootstrap returns floor; (b) steady_state with no prior `blockholder_filings` row for the issuer_cik returns floor; (c) steady_state with a watermark row returns `max(floor, MAX(filed_at) - 7d)`. Seed the watermark row via `blockholder_filers` + `blockholder_filings` INSERTs (column names per `sql/095:78`).
- [ ] **Step 2**: Run, verify failures.
- [ ] **Step 3**: Implement per spec §3.5 code block.
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

### Task 4.3: `_list_universe_issuers` + `_extract_filer_set` + `_ingest_one_accession` + `discover_sec_13dg_for_universe`

**Files:** Modify `app/services/sec_13dg_discovery.py`; extend `tests/test_sec_13dg_discovery.py`.

**Reference:** spec §3.1 step 3 (defensive filer extraction) + §3.1 step 4 (manifest + hint atomicity + idempotency). **Codex 1a HIGH manifest_inserted**: `record_manifest_entry` returns `None` and unconditionally `ON CONFLICT DO UPDATE`. Use `SELECT 1 FROM sec_filing_manifest WHERE accession_number = %s` pre-check inside the same `conn.transaction()` to determine insert-vs-existing before the upsert. Hint write uses `INSERT ... ON CONFLICT (accession_number, instrument_id) DO UPDATE SET discovered_at = NOW(), issuer_cik = EXCLUDED.issuer_cik RETURNING (xmax = 0) AS inserted`.

- [ ] **Step 1**: Write a happy-path test (single-class issuer, single non-agent filer). Seed `instruments` with `instrument_id`, `symbol`, `country='US'`, `is_tradable=TRUE`, **and `company_name`** (NOT NULL per `sql/001:1-10`) + `external_identifiers` primary CIK row. Monkeypatch `SecFilingsProvider.fetch_search_index_json` to return a fake efts response. Assert one manifest row written with `subject_type='blockholder_filer'`, `cik=filer_cik`, `instrument_id=NULL`; one hint row written; `DiscoveryResult.manifest_rows_inserted == 1`; `hints_written == 1`.
- [ ] **Step 2**: Run, verify failures.
- [ ] **Step 3**: Implement. `_list_universe_issuers` returns `[(instrument_id, cik), …]` via the canonical SELECT from spec §3.1. `_extract_filer_set` filters `[c for c in ciks if c.lstrip('0') != issuer_unpadded AND c not in KNOWN_FILING_AGENT_CIKS]`, dedupes, returns `(cik_padded, name)` pairs aligned to `display_names[]` index. `_ingest_one_accession` writes manifest + hint(s) inside one `conn.transaction()` with the SELECT-1 pre-check + RETURNING-based hint counter. `discover_sec_13dg_for_universe` groups instruments by CIK (so one HTTP query per CIK regardless of sibling count), pages with `from_offset += 100` until `len(hits) < size`, accumulates DiscoveryResult counters.
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

### Task 4.4: Edge-case fixtures

**Files:** Extend `tests/test_sec_13dg_discovery.py`.

**Reference:** spec §3.1 risks table + spec §3.4 test-impact bullet.

- [ ] **Step 1**: Add tests:
  - Joint filing: `ciks = [issuer, agent_donnelley, filer1, filer2]` → manifest `cik = filer1`; `blockholder_filers` seeded for filer1 + filer2 only (agent excluded).
  - Share-class siblings: GOOG (sibling A) + GOOGL (sibling B) both seeded with same CIK; one accession discovered → 1 manifest row, 2 hint rows (one per sibling).
  - Pagination boundary: 100 hits on page 1 + 0 hits on page 2 → loop terminates cleanly; `accessions_discovered == 100`.
  - Re-discovery idempotency: run twice → second run writes 0 new manifest, 0 new hint (`xmax=0` is False on both), but `discovered_at` advances.
  - Issuer-only result (no non-issuer CIK in `ciks[]`): defensive skip + warn log.
- [ ] **Step 2-5**: Run / fix / verify / commit.

---

## Phase 5 — Manifest parser swap + 5-case hint cross-validation

### Task 5.1: Contract test pinning edgartools `Schedule13D/G.parse_xml` shape

**Files:** Create `tests/test_edgartools_schedule13_shape.py`.

**Reference:** `.venv/lib/python3.14/site-packages/edgar/beneficial_ownership/schedule13.py:140-180` (the actual `parse_xml` body — XML element names + dict keys come from here; **edgartools requires `<coverPageHeader>`, NOT `<coverPage>`**) + `.venv/.../models.py` (nested dataclass fields including `ReportingPerson.aggregate_amount` NOT `aggregate_amount_owned`). edgartools skill G15.

- [ ] **Step 1**: Look up real-world SC 13D/G XML fixtures in `tests/fixtures/` (or grep for any `<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D">` block already in tests). If none, write a minimal valid one using `<coverPageHeader>` per the actual parse_xml shape; verify by running `Schedule13D.parse_xml(xml)` in a `uv run python -c` shell BEFORE the test runs.
- [ ] **Step 2**: Tests assert: `parse_xml(xml)` returns `dict`; top-level keys include `issuer_info`, `security_info`, `reporting_persons`; `parsed["issuer_info"]` is `IssuerInfo` dataclass (attribute access `.cik / .name / .cusip`); `parsed["security_info"]` is `SecurityInfo` (`.cusip / .title`); each `parsed["reporting_persons"]` element is `ReportingPerson` with `.aggregate_amount` (NOT `.aggregate_amount_owned`) + `.percent_of_class` + `.no_cik`; `Schedule13D.__init__` requires 7 positional args including `filing` (introspect via `inspect.signature`).
- [ ] **Step 3-4**: Run / verify against pinned `edgartools==5.30.2`.
- [ ] **Step 5**: Commit.

### Task 5.2: Pre-fetch retention gate B in `_parse_13dg`

**Files:** Modify `app/services/manifest_parsers/sec_13dg.py`; extend `tests/test_manifest_parser_sec_13dg.py`.

**Reference:** spec §3.2 chokepoint B (gate BEFORE `fetch_document_text` AND BEFORE `store_raw`); existing `KNOWN_FILING_AGENT_CIKS` guard from PR #1251 cleanup is the structural template.

- [ ] **Step 1**: Write failing test using `_seed_pending_*` helper (mirror pattern from `tests/test_manifest_parser_sec_13f_hr.py:142`): seed a row with `filed_at` strictly before `blockholders_retention_cutoff()`; monkeypatch `SecFilingsProvider.fetch_document_text` to track calls; assert tombstoned with `error == "retention floor"` AND zero SEC fetch calls.
- [ ] **Step 2**: Run, verify failure (worker proceeds to fetch).
- [ ] **Step 3**: Insert the gate immediately after the existing agent-CIK guard. Use `blockholders_within_retention(row.filed_at)`; tombstone with `error="retention floor"`.
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

### Task 5.3: NEW `_schedule13_adapter.py` (edgartools dict → repo `BlockholderFiling`)

**Files:** Create `app/services/manifest_parsers/_schedule13_adapter.py`; create `tests/test_schedule13_adapter.py`.

**Reference:** internal dataclass shape at `app/providers/implementations/sec_13dg.py:77-180`; edgartools models at `.venv/.../models.py`. **Codex 1b HIGH**: `parse_xml` does NOT return `submission_type` or `amendment_number` — the adapter MUST accept `manifest_form: str` (e.g. `"SC 13D/A"`) from the caller and derive `submission_type` (`"SCHEDULE 13D/A"`).

- [ ] **Step 1**: Write failing tests in `tests/test_schedule13_adapter.py`. Use the SAME XML fixture as Task 5.1 (valid `<coverPageHeader>`). Assert: adapter signature `build_filing_from_edgartools_dict(parsed: dict, *, source: Literal["sec_13d","sec_13g"], manifest_form: str, manifest_filer_cik: str) -> BlockholderFiling`; returned filing is a `BlockholderFiling` instance; `primary_filer_cik == manifest_filer_cik` (zero-padded — edgartools does NOT expose `headerData/filerInfo/filer/filerCredentials/cik` so the manifest's `row.cik` IS the canonical filer-of-record CIK per `app/providers/implementations/sec_13dg.py:141-147` doc); `status` follows `_STATUS_FOR_SOURCE` table; `submission_type` derived from `manifest_form` via mapping table; `BlockholderFiling.date_of_event` is `date | None` (parsed via `date.fromisoformat(parsed["date_of_event"])` when the edgartools string is non-empty, else `None`); each `BlockholderReportingPerson` carries Decimal-typed `aggregate_amount_owned` mapped from edgartools `.aggregate_amount` (int).
- [ ] **Step 2**: Run, verify ImportError.
- [ ] **Step 3**: Implement adapter. Field mapping table:
  - edgartools `IssuerInfo.cik` → repo `BlockholderFiling.issuer_cik`
  - edgartools `IssuerInfo.name` → repo `BlockholderFiling.issuer_name`
  - edgartools `SecurityInfo.cusip` → repo `BlockholderFiling.issuer_cusip` (share-class CUSIP — IssuerInfo.cusip would be issuer-level; SecurityInfo is the right choice for instrument disambiguation)
  - edgartools `SecurityInfo.title` → repo `BlockholderFiling.securities_class_title`
  - edgartools `parsed["date_of_event"]` (str, possibly empty) → repo `BlockholderFiling.date_of_event` (`date | None`): use `date.fromisoformat(s)` when `s` is non-empty + well-formed, else `None`. Catch `ValueError` defensively (defer to the existing parse-error tombstone path on malformed dates).
  - `BlockholderFiling.filed_at` → `None` here; manifest layer computes from `row.filed_at`
  - `BlockholderFiling.primary_filer_cik` → `_zero_pad_cik(manifest_filer_cik)` from the adapter's caller arg (NOT `reporting_persons[0].cik`). edgartools does not expose `headerData/filerInfo/filer/filerCredentials/cik`; the manifest row's `row.cik` IS that canonical filer-of-record CIK per the existing parser contract at `app/services/manifest_parsers/sec_13dg.py:103` (`filer_cik = (row.cik or "").strip()`). Mismapping to a reporting-person CIK would silently shift the `blockholder_filers` PK + the `blockholder_filings_ingest_log` filer_cik identity downstream (the `blockholder_filer_seeds` table is retired in Task 8.5; the canonical filer-of-record identity now lives in `blockholder_filers` via discovery + parser upserts).
  - Per reporter: map `cik`/`name`/`citizenship`/`member_of_group`/`type_of_reporting_person` directly; `no_cik` via `bool(getattr(p, 'no_cik', False))`; `cik` is `None` when `no_cik` else passthrough; `sole_voting_power` / `shared_voting_power` / `sole_dispositive_power` / `shared_dispositive_power` → `Decimal(str(value))` mapping; `aggregate_amount` → `aggregate_amount_owned` (Decimal); `percent_of_class` → Decimal.
  - `_STATUS_FOR_SOURCE: Final[dict[str, Literal["active","passive"]]]`
  - Submission-type derivation: `_SUBMISSION_TYPE_FOR_FORM: Final[dict[str, str]] = {"SC 13D": "SCHEDULE 13D", "SC 13D/A": "SCHEDULE 13D/A", "SC 13G": "SCHEDULE 13G", "SC 13G/A": "SCHEDULE 13G/A"}`; KeyError on unknown form (loud, not silent).
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

### Task 5.4: Wire adapter into `_parse_13dg`

**Files:** Modify `app/services/manifest_parsers/sec_13dg.py`; extend `tests/test_manifest_parser_sec_13dg.py`.

**Reference:** Task 5.3 adapter signature (`manifest_form` + `manifest_filer_cik` args); `app/services/manifest_parsers/sec_13dg.py:103-122` for the existing `row.cik` / `row.source` / archive-URL pattern; downstream `_upsert_filing_row` + `_record_13dg_observation_for_filing` at `app/services/blockholders.py:463 / :735` (unchanged consumers).

- [ ] **Step 1**: Write end-to-end happy-path test against a post-mandate edgartools-parseable fixture. Seed a pending manifest row + monkeypatch `fetch_document_text` to return the fixture XML; assert `blockholder_filings` row has correct `issuer_cik` / `issuer_cusip` / `securities_class_title`; per-reporter `aggregate_amount_owned` is Decimal.
- [ ] **Step 2**: Run, verify failure (still uses in-house `parse_primary_doc`).
- [ ] **Step 3**: Replace in-house parse with edgartools + adapter. Source-dispatch on `row.source` (`'sec_13d'` → `Schedule13D.parse_xml`, `'sec_13g'` → `Schedule13G.parse_xml`); pass `manifest_form=row.form` AND `manifest_filer_cik=filer_cik` (the existing local var from `:103`) to the adapter. Downstream `_upsert_filing_row` + `_record_13dg_observation_for_filing` calls stay unchanged.
- [ ] **Step 4**: Run all `test_manifest_parser_sec_13dg.py` tests. Port any pre-existing test fixture that was valid for in-house `parse_primary_doc` but invalid for edgartools (likely `<coverPage>` → `<coverPageHeader>` rename). If a legacy-only test asserts behavior the new path no longer matches, document why + remove.
- [ ] **Step 5**: Commit.

### Task 5.5: 5-case CUSIP-vs-hint cross-validation branch

**Files:** Modify `app/services/manifest_parsers/sec_13dg.py`; extend `tests/test_manifest_parser_sec_13dg.py`.

**Reference:** spec §3.1 step 4 5-case branch logic (CASE A/B/C/D-in-universe/D-out-of-universe/E).

- [ ] **Step 1**: Write 6 failing tests, one per case. Each test seeds: a manifest row + 0/1/N hint rows + instruments + external_identifiers + CUSIP→instrument mapping appropriate for the case. Run manifest worker. Per-case assertions:
  - **CASE A** (CUSIP-in-hints): observation row exists with `instrument_id = instrument_id_from_cusip`; `blockholder_filings.instrument_id` non-null.
  - **CASE B** (CUSIP None + 1 hint): observation row exists with `instrument_id = sole hint`; `blockholder_filings.instrument_id` set to the hint.
  - **CASE C** (CUSIP None + N>1 hints): **NO observation row written**; `blockholder_filings.instrument_id IS NULL`; `blockholder_filings_ingest_log.error` matches `"cusip_unresolved_with_ambiguous_hint"`.
  - **CASE D-in-universe**: observation row exists with `instrument_id = instrument_id_from_cusip`; ingest-log error has `"cusip_resolved_with_hint_discrepancy"` (info-level, not failure).
  - **CASE D-out-of-universe**: **NO observation row written**; `blockholder_filings.instrument_id IS NULL`; ingest-log error matches `"cusip_resolved_outside_universe"`.
  - **CASE E** (no hints, CUSIP resolves): observation row exists with `instrument_id = instrument_id_from_cusip`; no discrepancy log (legacy daily-index path, no regression).
- [ ] **Step 2**: Run, verify failures.
- [ ] **Step 3**: Implement the 5-case branch in `_parse_13dg` between the parse step and the `_record_13dg_observation_for_filing` call:
  - Resolve `instrument_id_from_cusip = _resolve_cusip_to_instrument_id(conn, filing.issuer_cusip)`.
  - `hint_ids = {r[0] for r in conn.execute("SELECT instrument_id FROM sec_13dg_discovery_issuer_hint WHERE accession_number = %s", (accession,))}`.
  - CASE A: CUSIP-in-hints → use `instrument_id_from_cusip`.
  - CASE B: CUSIP None + 1 hint → use single hint.
  - CASE C: CUSIP None + N>1 hints → `instrument_id = None`; `log_error = "cusip_unresolved_with_ambiguous_hint (cusip=… hints=…)"`.
  - CASE D: CUSIP resolved + not in hints → SELECT `instruments` WHERE `country='US' AND is_tradable=TRUE`; if in-universe → use CUSIP + discrepancy log; if out-of-universe → `None` + `cusip_resolved_outside_universe` log.
  - CASE E: CUSIP resolved + no hints (legacy path) → use CUSIP.
- [ ] **Step 4**: Run, verify all 6 PASS.
- [ ] **Step 5**: Commit.

---

## Phase 6 — Sync gate C

### Task 6.1: Add `bf.filed_at >= cutoff` predicate to `sync_blockholders`

**Files:** Modify `app/services/ownership_observations_sync.py::sync_blockholders`; create `tests/test_ownership_observations_sync_blockholders_cap.py`.

**Reference:** spec §3.2 chokepoint C — gate on `bf.filed_at` directly; **forbid `fe.filing_date >= cutoff` predicate** (Codex 1a HIGH #4: null-rejects via LEFT JOIN). Real schema for ownership_blockholders_observations at `sql/115:25-65` — column names: `instrument_id`, `reporter_cik`, `reporter_name`, `ownership_nature`, `submission_type`, `status_flag`, `source`, `source_document_id`, `source_accession`, `filed_at`, `period_end`, `known_from`, `ingest_run_id`, `aggregate_amount_owned`, `percent_of_class`. PK = `(instrument_id, reporter_cik, ownership_nature, source, source_document_id, period_end)`.

Real schema for `instruments` at `sql/001:1-10` — **`company_name` is NOT NULL**.

- [ ] **Step 1**: Write 2 failing tests in `tests/test_ownership_observations_sync_blockholders_cap.py`:
  - `test_sync_excludes_pre_cap_filings`: seed 1 pre-cap + 1 post-cap `blockholder_filings` row; call `sync_blockholders`; assert observations exist for the post-cap accession only.
  - `test_sync_includes_rows_without_filing_events_entry`: seed 1 post-cap `blockholder_filings` row deliberately WITHOUT a `filing_events` entry; assert the observation row exists (LEFT JOIN survives).
  - Use seed helpers that pass valid `company_name` to `instruments` and valid column names per the actual `sql/095` + `sql/115` schemas. Verify schema before writing seed code: `grep -A30 "CREATE TABLE.*blockholder_filings\b" sql/095_*.sql`.
- [ ] **Step 2**: Run, verify failures.
- [ ] **Step 3**: Modify `sync_blockholders` body. Add `AND bf.filed_at >= %(retention_cutoff)s` predicate; parameterise via `blockholders_retention_cutoff()`. Keep the existing LEFT JOIN on `filing_events` if present (no `fe.filing_date >=` predicate).
- [ ] **Step 4**: Run, verify PASS.
- [ ] **Step 5**: Commit.

---

## Phase 7 — Rewash gate F

### Task 7.1: Branch-ordered rewash gate

**Files:** Modify `app/services/rewash_filings.py::_apply_blockholders`; create `tests/test_rewash_blockholders_cap.py`.

**Reference:** spec §3.2 chokepoint F + Codex 1b MEDIUM branch-order pin. **Codex 1b HIGH apply_fn signature**: rewash dispatcher at `app/services/rewash_filings.py:185` calls `spec.apply_fn(conn, raw_doc)` — 2-arg, NOT 3-arg. `_apply_blockholders` MUST accept `(conn, raw_doc)` only; derive `filed_at` from `raw_doc` fields OR from a `filing_events` lookup inside the function body.

- [ ] **Step 1**: Read the current `_apply_blockholders` signature + body via `grep -nA30 "def _apply_blockholders" app/services/rewash_filings.py`. Understand the `raw_doc` shape (probably `RawFilingDocument` dataclass — read its definition).
- [ ] **Step 2**: Write 3 failing tests in `tests/test_rewash_blockholders_cap.py`:
  - `test_happy_path_uncapped_for_existing_rows`: pre-cap accession WITH existing `blockholder_filings` rows → rewash returns `True`, DELETE + re-INSERT proceeds.
  - `test_rescue_path_skips_pre_cap_accession`: pre-cap accession with zero existing rows → returns `False`, no INSERT.
  - `test_rescue_path_writes_post_cap_accession`: post-cap accession with zero existing rows → returns `True`, row written.
  - Use the actual `raw_doc` fixture shape from existing `tests/test_rewash_filings.py` (mirror the seed pattern).
- [ ] **Step 3**: Run, verify rescue-path tests fail (gate not there).
- [ ] **Step 4**: Implement gate inside `_apply_blockholders`:
  - First action: `SELECT COUNT(*) FROM blockholder_filings WHERE accession_number = %s`.
  - If count > 0 → happy path → proceed unchanged (uncapped per spec §6.3).
  - If count == 0 → rescue path → resolve `filed_at` (from `raw_doc.filed_at` if the field exists, else fall back to `filing_events.filing_date` lookup) → if not `blockholders_within_retention(filed_at)` return `False`.
  - ALSO swap the in-house `parse_primary_doc` call in this function to the same edgartools + adapter pattern from Task 5.3/5.4 (consistency with the live manifest path).
- [ ] **Step 5**: Run, verify all 3 PASS.
- [ ] **Step 6**: Commit.

---

## Phase 8 — Resolver + dormant code retirement

### Task 8.1: Remove `blockholder_filer_seeds` resolver branch

**Files:** Modify `app/jobs/sec_atom_fast_lane.py` + `app/jobs/sec_daily_index_reconcile.py`.

**Reference:** spec §3.4 cleanup checklist; `app/jobs/sec_atom_fast_lane.py:57-101` for the existing `default_subject_resolver` shape (1: issuer_cik, 2: institutional_filers, 3: blockholder_filers — Task 8.1 removes the 4th `blockholder_filer_seeds` branch that's also present, see grep).

- [ ] **Step 1**: Grep `blockholder_filer_seeds` in both files.
- [ ] **Step 2**: Delete each branch that SELECTs from `blockholder_filer_seeds`; preserve the `blockholder_filers` branch (now the sole resolver for `subject_type='blockholder_filer'`).
- [ ] **Step 3**: Smoke import both modules.
- [ ] **Step 4**: Commit.

### Task 8.2: Delete dormant entrypoints from `app/services/blockholders.py`

**Files:** Modify `app/services/blockholders.py`.

**Reference:** spec §3.4 DELETE list; current entrypoint definitions in `app/services/blockholders.py` (grep finds them); the lower-level helpers (`_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing`, `_resolve_cusip_to_instrument_id`, `_archive_file_url`, `_record_ingest_attempt`, `_zero_pad_cik`) STAY — used by `manifest_parsers/sec_13dg.py:60-69`.

- [ ] **Step 1**: Grep `^def (ingest_all_active_filers|ingest_filer_blockholders|_list_active_filer_seeds|seed_filer)\b` in the file.
- [ ] **Step 2**: Grep external callers across `app/ tests/ scripts/` — every hit must be in `scripts/seed_holder_coverage.py` (Task 8.3) or `tests/test_blockholders_ingester.py` (Task 8.4); any other caller resolves first.
- [ ] **Step 3**: Delete the 4 functions + any unused imports (`uv run ruff check app/services/blockholders.py` for verification).
- [ ] **Step 4**: Commit.

### Task 8.3: Surgical edit to `scripts/seed_holder_coverage.py`

**Files:** Modify `scripts/seed_holder_coverage.py`.

**Reference:** spec §3.4 EDIT clause; current script bullets at `scripts/seed_holder_coverage.py:1-56` (4 steps: institutional + ETF + blockholder + CUSIP-resolver + N-CEN). Codex 1a HIGH #5 from PR1251 noted: PR11 must NOT delete the 13F-HR / CUSIP-resolver / N-CEN paths (those are out-of-scope unrelated work).

- [ ] **Step 1**: Read the script structure (`grep -nE "def |BLOCKHOLDER|13D|ingest_all_blockholders" scripts/seed_holder_coverage.py`).
- [ ] **Step 2**: Remove only the 13D/G block: `_BLOCKHOLDER_SEEDS` const, `seed_blockholder_filer` import + calls, `ingest_all_blockholders` import + call, the matching print blocks. Preserve 13F-HR / ETF / CUSIP-resolver / N-CEN sections.
- [ ] **Step 3**: Update docstring + `--help` text — note PR11 universe-driven discovery.
- [ ] **Step 4**: Smoke `uv run python -m scripts.seed_holder_coverage --help`.
- [ ] **Step 5**: Commit.

### Task 8.4: Delete dormant ingester test cases

**Files:** Modify `tests/test_blockholders_ingester.py`.

**Reference:** spec §3.4 test impact bullet (~50% of 955 LOC reference deleted entrypoints; preserve helper-level tests for `_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing`, `_resolve_cusip_to_instrument_id`).

- [ ] **Step 1**: Grep test cases referencing the deleted entrypoints.
- [ ] **Step 2**: Delete those tests; preserve helper-level tests.
- [ ] **Step 3**: Run the trimmed file; expect all remaining tests PASS.
- [ ] **Step 4**: Commit.

### Task 8.5: Drop migration `sql/161_drop_blockholder_filer_seeds.sql` + `_PLANNER_TABLES` removal

**Files:** Create `sql/161_drop_blockholder_filer_seeds.sql`; modify `tests/fixtures/ebull_test_db.py`.

> Order matters: this lands AFTER Tasks 8.1-8.4 finish. No live code references the table at this point.

- [ ] **Step 1**: Write the drop migration (`DROP INDEX IF EXISTS … DROP TABLE IF EXISTS blockholder_filer_seeds`). Header comment cites Codex 1a HIGH ordering + the prevention-log entry.
- [ ] **Step 2**: Apply locally; verify table gone.
- [ ] **Step 3**: Remove `"blockholder_filer_seeds"` from `_PLANNER_TABLES`.
- [ ] **Step 4**: Verify fixture imports clean.
- [ ] **Step 5**: Commit.

---

## Phase 9 — Scheduler wiring

### Task 9.1: Bootstrap stage + scheduler job + stage-count assertion bump

**Files:** Modify `app/services/bootstrap_orchestrator.py`; modify the scheduler-job-registry file (find via grep for an existing job like `sec_def14a_bootstrap`); modify `app/jobs/sources.py` if required by the registry contract; grep `app/ tests/ frontend/ docs/` for any other `stages.*26` / `26.*stages` reference.

**Reference:** spec §3.5 stage + job code blocks. **Codex 1b MEDIUM**: hard assertion `assert len(_BOOTSTRAP_STAGE_SPECS) == 26` at `app/services/bootstrap_orchestrator.py:1961` MUST bump to `27`; the assertion message references the spec/frontend/runbook/stage_count tests in lockstep.

- [ ] **Step 1**: Find next free stage order: `grep -nE "_spec\(.*, [0-9]+," app/services/bootstrap_orchestrator.py | tail -10`. Pick max + 1.
- [ ] **Step 2**: Add new `_spec("sec_blockholders_discovery", <next>, "sec_rate", "sec_blockholders_discovery_job", params={"mode": "bootstrap"})`.
- [ ] **Step 3**: Bump the `assert len(_BOOTSTRAP_STAGE_SPECS) == 26` to `27` + amend the assertion message to mention PR11 (per the existing message style at `:1961-1966`).
- [ ] **Step 4**: Register the job. Find where `sec_def14a_bootstrap` is registered (likely in `app/workers/scheduler.py` or `app/jobs/sources.py`); mirror the same pattern. Job body calls `discover_sec_13dg_for_universe(conn, mode=params.get("mode", "steady_state"))` and returns `JobResult` populated from `DiscoveryResult`.
- [ ] **Step 5**: Grep other 26-hardcoded references in `app/ tests/ frontend/ docs/`; update each in lockstep.
- [ ] **Step 6**: Smoke `uv run python -c "from app.services.bootstrap_orchestrator import _BOOTSTRAP_STAGE_SPECS; assert any(s.stage_key == 'sec_blockholders_discovery' for s in _BOOTSTRAP_STAGE_SPECS); assert len(_BOOTSTRAP_STAGE_SPECS) == 27"`.
- [ ] **Step 7**: Run any `tests/test_bootstrap_stage_count*` or `test_job_registry*` tests; fix per failures.
- [ ] **Step 8**: Commit.

---

## Phase 10 — Lint guard

### Task 10.1: `scripts/check_13dg_retention.sh` with placement invariants A-L

**Files:** Create `scripts/check_13dg_retention.sh`; modify `.githooks/pre-push`.

**Reference:** spec §3.6 lint invariants A-L; existing `scripts/check_archive_url_agent_guard.sh` (added by PR #1251) as the structural template for the awk-based block walker + empty-grep `wc -l` guard pattern (per PR10a Codex iter 1 lesson); existing `scripts/check_business_summary_latest_only.sh` for invariant-counter idioms; existing `scripts/check_n_csr_retention.sh` for the N-shape lint.

- [ ] **Step 1**: Read 3 existing lint scripts (`check_archive_url_agent_guard.sh`, `check_business_summary_latest_only.sh`, `check_n_csr_retention.sh`) to match their style.
- [ ] **Step 2**: Implement invariants A-L per spec §3.6. Each invariant should fail loudly with a message pointing to the spec section + the file path that would need editing.
- [ ] **Step 3**: Run against the post-PR tree (`bash scripts/check_13dg_retention.sh`); expect OK.
- [ ] **Step 4**: Wire into `.githooks/pre-push` after the existing `check_form3_latest_per_pair.sh` block (mirror the block style from PR1251 `check_archive_url_agent_guard.sh` wiring).
- [ ] **Step 5**: Commit.

---

## Phase 11 — Refresh-current invariant + dormant-symbol absence tests

### Task 11.1: Pin refresh_blockholders_current uncapped contract

**Files:** Create `tests/test_refresh_blockholders_current_uncapped.py`.

**Reference:** parent spec §6.3 + §4.5 13F-HR precedent; **real schema for ownership_blockholders_observations at `sql/115:25-65`** (NOT my prior plan-v2 columns). PK = `(instrument_id, reporter_cik, ownership_nature, source, source_document_id, period_end)`.

- [ ] **Step 1**: Look up the actual `INSERT INTO ownership_blockholders_observations` shape from an existing test (e.g. `grep -B2 -A20 "INSERT INTO ownership_blockholders_observations" tests/`) and mirror it. NOT every column is NOT NULL — read the schema to identify which can be omitted.
- [ ] **Step 2**: Write the test: seed an `instruments` row (with valid `company_name`); seed a pre-cap observation row directly into the observations table; call `refresh_blockholders_current(conn, instrument_id=…)`; assert (a) the pre-cap observation row STILL exists in observations; (b) `ownership_blockholders_current` reflects the pre-cap row (i.e. refresh did NOT skip it for being pre-cap).
- [ ] **Step 3**: Run; expect PASS (today's behavior is uncapped; the test pins it).
- [ ] **Step 4**: Commit.

### Task 11.2: Pin dormant blockholder-symbol absence

**Files:** Create `tests/test_no_dormant_blockholder_symbols.py`.

**Reference:** Codex 1b HIGH self-grep: any lint-as-test grepping for forbidden symbols MUST exclude itself from the search OR list its own path in the allow-list.

- [ ] **Step 1**: Write the test. Use `git grep -w -n <symbol> -- 'app/' 'scripts/' ':!tests/test_no_dormant_blockholder_symbols.py'` (pathspec exclusion) so the test file's own `FORBIDDEN_SYMBOLS` definition is excluded from the search. Allow-list for the comment-only mention in `scripts/seed_holder_coverage.py`.
- [ ] **Step 2**: Symbols list: `ingest_all_active_filers`, `ingest_filer_blockholders`, `_list_active_filer_seeds`, `seed_filer` (the 13D/G variants).
- [ ] **Step 3**: Run; expect PASS (Tasks 8.1-8.4 removed every live reference).
- [ ] **Step 4**: Commit.

---

## Phase 12 — Parent spec amendment

### Task 12.1: Amend `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`

**Files:** Modify the parent rubric spec.

**Reference:** parent spec §4.8 + §7 PR11 entry + §11 Codex cadence + §12 Handover.

- [ ] **Step 1**: Replace §4.8 "Current volume: 0 ingested" with "Volume: backfilled in PR11 (#TBD) via efts.sec.gov universe-issuer-CIK discovery + max(today-3y, 2024-12-18) cap floor". Add chokepoint matrix mirroring §4.5/§4.6/§4.7 SHIPPED sections.
- [ ] **Step 2**: Update §7 PR11 entry: mark SHIPPED; summarize discovery + 4 chokepoints + adapter + edgartools adoption + share-class sibling hint multi-row + dormant retirement.
- [ ] **Step 3**: Update §11 Codex cadence: PR11 — spec Codex 1a-1g (7 rounds) + plan Codex 1a + 1b (2 rounds) + Codex 2 pre-push on impl.
- [ ] **Step 4**: Update §12 Handover: PR11 SHIPPED; PR12 remains.
- [ ] **Step 5**: Commit.

---

## Phase 13 — Smoke + Codex 2 + PR

### Task 13.1: Apply migrations + run discovery against dev DB

- [ ] **Step 1**: Apply both migrations:
  - `docker exec -i ebull-postgres psql -U postgres -d ebull < sql/159_create_sec_13dg_discovery_issuer_hint.sql`
  - `docker exec -i ebull-postgres psql -U postgres -d ebull < sql/161_drop_blockholder_filer_seeds.sql`
- [ ] **Step 2**: Trigger discovery: `curl -X POST http://localhost:8000/jobs/sec_blockholders_discovery_job/run -H 'Content-Type: application/json' -d '{"mode":"bootstrap"}'`. Capture the `DiscoveryResult` fields.
- [ ] **Step 3**: Poll `GET /jobs/sec_manifest_worker/status` until `pending` for `source IN ('sec_13d','sec_13g')` reaches zero.

### Task 13.2: Operator-panel verification

- [ ] **Step 1**: For AAPL, GME, MSFT, JPM, HD:
  - `curl http://localhost:8000/instruments/<symbol>/blockholders`
  - `curl 'http://localhost:8000/instruments/<symbol>/ownership-rollup?category=blockholders'`
- [ ] **Step 2**: Cross-source: pick the highest-shares filer for GME; compare `percent_of_class` against SEC EDGAR direct (`https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1326380&type=SC+13`). Record both in PR description.

### Task 13.3: Pre-push gates + Codex 2

- [ ] **Step 1**: `uv run ruff check .` / `uv run ruff format --check .` / `uv run pyright`. All green.
- [ ] **Step 2**: `uv run pytest`. If hangs on Postgres-lock-OOM, document `--no-verify` justification per `feedback_pre_push_xdist_postgres_locks.md`.
- [ ] **Step 3**: `codex.cmd exec review` on the branch. Resolve every BLOCKING/HIGH before push.
- [ ] **Step 4**: Push branch.

### Task 13.4: Open PR + poll review until merge

- [ ] **Step 1**: Open PR with self-contained body covering CLAUDE.md ETL clauses 8-12: smoke instruments + figures + cross-source verification + commit SHA for each verification. Security model: no new auth surface; SEC HTTP shares existing 10 req/s throttle. Tradeoffs: HTML-only pre-2024-12-18 filings unreachable by mandate-floor construction (operator-accepted). 7-round Codex spec trail + 2-round Codex plan trail + Codex 2 pre-push. Cite PR #1251 cleanup precedent. Use separate `Closes #N` lines per the `verify-issue-link` CI gate from PR #1251.
- [ ] **Step 2**: Poll `gh pr view <n> --comments` + `gh pr checks <n>` per CLAUDE.md branch-and-PR workflow. Resolve every comment with FIXED/DEFERRED/REBUTTED. PREVENTION comments end EXTRACTED/ALREADY_COVERED/REBUTTED.
- [ ] **Step 3**: Merge after APPROVE on the most recent commit + CI green.

---

## Self-review

**Spec coverage:** every spec §3 subsection mapped to a phase (1↔§3.4 + §3.7; 2↔§3.2 helpers; 3↔§3.5 provider; 4↔§3.1; 5↔§3.3 + §3.2 B; 6↔§3.2 C; 7↔§3.2 F; 8↔§3.4 cleanup; 9↔§3.5 wiring; 10↔§3.6; 11↔§3.2 D + dormant-pin; 12↔§3.9; 13↔§3.8).

**Placeholder scan:** zero embedded production code blocks. Every task points to canonical sources for verification at code-time. Acceptance criteria are concrete.

**Codex-finding traceability:**

- Codex 1a BLOCKING (date) → spec v7.2 alignment landed
- Codex 1a HIGH ordering → Task 8.5 (post-cleanup drop)
- Codex 1a HIGH manifest_inserted → Task 4.3 SELECT-1 pre-check note + RETURNING xmax pattern note
- Codex 1a HIGH adapter → Task 5.3 adapter module with `manifest_form` param
- Codex 1b HIGH coverPageHeader → Task 5.1 contract test cites `.venv/.../schedule13.py:140-180` for actual XML shape; subagent verifies before writing fixture
- Codex 1b HIGH _apply_blockholders 2-arg → Task 7.1 cites `rewash_filings.py:185` dispatcher signature
- Codex 1b HIGH observations columns → Task 6.1 + 11.1 cite `sql/115:25-65` real schema
- Codex 1b HIGH self-grep → Task 11.2 uses git pathspec exclusion
- Codex 1b MEDIUM company_name → Tasks 4.3 + 6.1 + 11.1 explicit
- Codex 1b MEDIUM sql/161 filename → Task 13.1 uses sql/161
- Codex 1b LOW v7.2 → updated above

---

## Execution handoff

Plan v3 saved to `docs/superpowers/plans/2026-05-21-1233-pr11-blockholders-activation.md`. Operator already selected subagent-driven execution. Codex 1c pending before first dispatch.
