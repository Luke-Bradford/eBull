# Bootstrap ETL — comprehensive optimisation spec **v3**

**Date:** 2026-05-22 (v3 after v2 review converged on 4 CRITICAL + 5 HIGH new defects)
**Status:** DRAFT v3 — incorporates v2 review (`/tmp/spec-v2-review.md`) + Codex 2 spec review.
**Supersedes:** v1, v2. Both retained for audit trail.

---

## Changelog v2 → v3

| v2 defect | v2 framing | v3 fix |
|---|---|---|
| H4 `partial_complete` enum | demote to non-existent status | v3: keep `complete` + new column `bootstrap_runs.coverage_floor_met BOOLEAN`. CHECK constraint untouched. Operator panel reads the column. |
| H5(a) reaper hash mismatch | `hashtextextended` ≠ JobLock's `hashtext` | v3: use `hashtext('job_source:' || %(source)s)::int` matching JobLock at `app/jobs/locks.py:224`. |
| H5(c) heartbeat per-subsystem | not per-stage | v3: drop the heartbeat check. Reaper criterion = lock-NOT-held + stage `started_at > 5 min ago`. Re-entrancy edge accepted as residual risk; documented. |
| N1 PR-1a writer signature | `_record_unresolved_cusip` needs name_of_issuer | v3: new helper `record_unresolved_cusip_from_bulk(conn, *, cusip, filer_cik_or_null, period_end, source)`. Sidesteps name_of_issuer requirement (it's optional in v3's schema). |
| C2 schema mismatch | ON CONFLICT against columns that don't exist | v3: SQL migration `sql/<N>_unresolved_13f_cusips_bulk_columns.sql` adds nullable `filer_cik TEXT`, `period_end DATE`, `source TEXT`; adds UNIQUE INDEX `unresolved_13f_cusips_bulk_idx` on `(cusip, filer_cik, period_end, source) WHERE source IS NOT NULL`. Backwards-compat: existing legacy writes (`source IS NULL`) keep using the original `cusip` PK. |
| Codex critical: `_load_cusip_map` only reads `provider='sec'` | OpenFIGI promotions invisible | v3: extend `_load_cusip_map` (`sec_13f_dataset_ingest.py:67-97`, `sec_nport_dataset_ingest.py:153-180`) to `WHERE provider IN ('sec', 'openfigi') AND identifier_type='cusip'`. `bootstrap_preconditions.compute_cusip_coverage` (`:188`) same change. **30 extra LoC in PR-1b.** |
| N6 S12.5 lane misuse | `sec_rate` budget conflated | v3: NEW `openfigi` lane. Registered in `Lane` (`app/jobs/sources.py`), `_LANE_MAX_CONCURRENCY` adds entry, `bootstrap_stages.lane` CHECK migration adds value. PR-1b ships the lane migration. |
| N8 stage_order SMALLINT collision | no room for "12.5" | v3: renumber. Migration shifts S13-S26 → S14-S27. S13 (new) = cusip_resolver_post_bulk_sweep. PR-1b ships the renumber migration. |
| H9 GOOGL acceptance | unconditional but PR-B deferred | v3: assertion = `GET /instruments/GOOGL/ownership-rollup` returns 200 with EITHER non-empty body OR `partial_data_reason='share_class_sibling_pre_PR_B'`. Endpoint change scoped in PR-1b. |
| Codex H "12k / 250 = 48 min" | spec said 30 | v3: budget S13 (new ordering) = **up to 48 min unkeyed / 5 min keyed**. 5-instrument panel ≠ universe; the 48-min figure assumes all unresolved CUSIPs sweep. Realistic: bootstrap-time unresolved set ~7-12k CUSIPs. |
| C3 TEMP TABLE timing | ON COMMIT DROP ambiguity | v3: PR-3 spec MANDATES "CREATE TEMP TABLE inside the same transaction as the COPY+INSERT+commit", re-created each per-archive iteration. Test fixture invariant. |
| C4 ETag multipart stability | -184 part-count suffix | v3: ETag-keyed reuse augmented with content SHA-256 verification on reuse path (defence-in-depth: ETag = freshness check, SHA-256 = local integrity check). Per-night ETag stability accepted as empirical observation; if SEC repartitions, ETag changes, reuse correctly skips. |
| H2 OpenFIGI rate limits unverified | doc-derived | v3: PR-0 (NEW) — empirical OpenFIGI probe. Land BEFORE PR-1b. Records fixtures with actual headers + per-row error shapes + 429 retry behaviour. |
| C5 dispatcher test rework | budgeted but unspecified | v3: explicitly lists tests to rewrite. |
| N3 ThreadPoolExecutor lifetime | 8+ pools alive 90 min | v3: register pools in a single `LaneExecutorRegistry` context manager scoped to `run_bootstrap_orchestrator`. Test fixture injects a sync `MockExecutor` for deterministic unit tests. |
| Codex M S12.5 cancel-mid-sweep | undocumented | v3: §12 robustness assertion + test added. |
| Codex M S9 budget | 10 min unverified | v3: budget bumped to 15-20 min OR ship a S9 COPY refactor as PR-3.b (split). v3 picks the latter — S9 is in scope. |
| Adversarial §7 90-min over-conservative | real 47-57 min | v3: target **≤ 60 min Tier 1** (was 90); hard ceiling 90. Tier 2 ≤ 45. |
| Codex M S15 rewash | bulk rows skipped before sweep | v3: PR-1b's sweep triggers `_rewash_originating_filings` for bulk-source rows (currently only legacy). Spec includes `rewash_bulk_source_filings` extension. |

---

## 1. Goals (revised)

| Target | v2 | v3 |
|---|---|---|
| Cold-install wall-clock Tier 1 | ≤ 90 min | **≤ 60 min** (45 min target with OpenFIGI key) |
| Cold-install wall-clock Tier 2 | ≤ 45 min | **≤ 30 min** |
| Daily-refresh wall-clock | ≤ 5 min | ≤ 5 min |
| CUSIP coverage at run COMPLETION | ≥ 80% | ≥ 80% (record-only, `coverage_floor_met` column) |
| Ownership categories populated at "complete" | 10 of 10 | 10 of 10 |
| Operator-visible drop telemetry | admin panel | admin panel + `coverage_floor_met` boolean |
| Process-crash recovery | reaper | reaper (lock-probe + grace window) |
| Wasted re-download | 0 bytes | 0 bytes (ETag-keyed reuse + SHA-256) |

---

## 2. Pre-requisite work (must land before PR-1b)

### SD-1 settled-decisions entry: OpenFIGI

Concrete patch (paste-ready):

```markdown
## OpenFIGI as approved external CUSIP-resolver fallback (2026-05-22)

**Decision:** OpenFIGI v3 API at `https://api.openfigi.com/v3/mapping`
is approved as a CUSIP-resolution fallback for the eBull universe.

**Constraints:**
- Free tier: 25 req/min unkeyed × max 10 jobs/POST = 250 mappings/min.
- Keyed tier: 25 req/6s × max 100 jobs/POST = 25,000 mappings/min.
- Operator-keyed mode requires `OPENFIGI_API_KEY` env var; default is unkeyed.
- The response **does not contain CUSIP**. Approved usage is CUSIP→ticker
  (idType=ID_CUSIP, idValue=<cusip>); the response includes ticker which
  resolves against `instruments.symbol`.
- Forbidden: ticker→CUSIP flow (response shape does not return CUSIP).

**ToS posture:** OpenFIGI free tier permits programmatic use within rate
limits. Operator approves prior to PR-1b merge.
```

### SD-2 settled-decisions entry: ETag-keyed reuse (PR-5b prerequisite)

```markdown
## Bulk archive reuse keyed on SEC ETag + SHA-256 (2026-05-22)

**Decision:** The Codex review BLOCKING for #1020 prohibited reusing a
prior-run .zip. With SEC's stable S3-backed ETag (probed 2026-05-22 against
`submissions.zip` returning `etag: "504b124e9474334e889e9e525db95c14-184"`),
reuse is permitted when ALL of:
(1) local `.zip.etag` sidecar matches SEC's HEAD response,
(2) SHA-256 of local file matches `.zip.sha256` sidecar.
The run-manifest records `reuse_reason: 'etag_match_sha256_verified'`.
Forced override: `BOOTSTRAP_FORCE_REDOWNLOAD=1` env var.
**Empirical:** SEC ignores If-None-Match / If-Modified-Since; reuse uses
client-side header comparison.
```

### PR-0 (NEW): OpenFIGI empirical probe + recorded fixtures

**Scope (~150 LoC):**
- `tests/integration/test_openfigi_live_probe.py` — gated by `OPENFIGI_LIVE_PROBE=1` env var.
- Records response headers + bodies for: (a) successful CUSIP→ticker for 5 known CUSIPs (AAPL, MSFT, GOOG, BRK.A, JPM), (b) batch of 10 with 1 invalid CUSIP, (c) rate-limit-saturation 429 response shape, (d) `Retry-After` + `ratelimit-remaining` header presence.
- Writes fixtures to `tests/fixtures/openfigi/` for use by PR-1b unit tests.
- Operator runs probe once; commits fixtures with the PR-0 merge.

**Why first:** v2's rate-limit numbers came from docs, not measurement. PR-1b needs verified contract.

---

## 3. Revised PR ordering + LoC budgets

```
SD-1 + SD-2 settled-decisions  → PR-0 OpenFIGI empirical probe
                                          ↓
                                  PR-1a unresolved-CUSIP capture + schema migration
                                          ↓
                                  PR-1b OpenFIGI resolver + Phase D sweep stage + cusip_map fix
                                          ↓
                                  PR-2 dispatcher parallelism
                                          ↓
                                  PR-3 COPY refactor (13F, NPORT, insider, companyfacts)
                                          ↓
                                  PR-4 batched _current refresh
                                          ↓
                                  PR-5a manifest reset + PR-5b ETag reuse (parallel)
                                          ↓
                                  PR-6 reaper + PR-8 daily refresh + PR-7 cap floor (conditional)
```

| PR | Service LoC | Test LoC | Schema LoC | Skill LoC | Total |
|---|---|---|---|---|---|
| PR-0 | 100 | 150 | - | - | 250 |
| PR-1a | 250 | 150 | 50 | 50 | 500 |
| PR-1b | 600 | 350 | 100 (lane migration + stage renumber) | 150 | 1200 |
| PR-2 | 300 | 300 | - | 100 | 700 |
| PR-3 | 800 | 500 | - | 50 | 1350 |
| PR-4 | 200 | 200 | - | 50 | 450 |
| PR-5a | 150 | 100 | - | 30 | 280 |
| PR-5b | 350 | 250 | - | 50 | 650 |
| PR-6 | 250 | 200 | - | 50 | 500 |
| PR-7 | 30 | 50 | - | 20 | 100 |
| PR-8 | 250 | 200 | - | 50 | 500 |
| **Total** | **3280** | **2450** | **150** | **600** | **~6480** |

---

## 4. PR-1a — Bulk-path unresolved-CUSIP capture

### Schema migration (~50 LoC)

```sql
-- sql/<N>_unresolved_13f_cusips_bulk_columns.sql
ALTER TABLE unresolved_13f_cusips
  ADD COLUMN filer_cik TEXT,
  ADD COLUMN period_end DATE,
  ADD COLUMN source TEXT;

CREATE UNIQUE INDEX unresolved_13f_cusips_bulk_idx
  ON unresolved_13f_cusips (cusip, COALESCE(filer_cik, ''), COALESCE(period_end, '0001-01-01'::date), COALESCE(source, ''))
  WHERE source IS NOT NULL;
-- The original (cusip)-PK path stays for legacy per-filing writes
-- where filer_cik/period_end/source are NULL. The new bulk path
-- writes with all four populated.
```

### Service (~250 LoC)

New helper in `app/services/cusip_resolver.py`:

```python
def record_unresolved_cusip_from_bulk(
    conn: psycopg.Connection[Any],
    *,
    cusip: str,
    filer_cik: str,
    period_end: date,
    source: Literal['bulk_13f_dataset', 'bulk_nport_dataset'],
) -> None:
    """Bulk-path-specific unresolved-CUSIP write.

    Distinct from the legacy ``_record_unresolved_cusip`` (which needs
    ``name_of_issuer + accession_number`` only available on per-filing
    path). The bulk dataset carries period_end + filer_cik but not the
    issuer name; we store None for ``name_of_issuer`` and let the sweep
    fill it from OpenFIGI's response (``name`` field).

    Idempotent on the new partial UNIQUE INDEX.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO unresolved_13f_cusips
              (cusip, name_of_issuer, last_accession_number, filer_cik, period_end, source)
            VALUES (%(cusip)s, NULL, NULL, %(filer_cik)s, %(period_end)s, %(source)s)
            ON CONFLICT (cusip, COALESCE(filer_cik, ''), COALESCE(period_end, '0001-01-01'::date), COALESCE(source, ''))
            WHERE source IS NOT NULL DO NOTHING
        """, {"cusip": cusip, "filer_cik": filer_cik, "period_end": period_end, "source": source})
```

Call sites:
- `sec_13f_dataset_ingest.py:294-297` — replace counter-only increment with batched call to the new helper (accumulate 1000 rows then INSERT).
- `sec_nport_dataset_ingest.py:341-345` — same.

---

## 5. PR-1b — OpenFIGI resolver + sweep + Phase D stage + cusip_map fix

### Lane migration (`sql/<N>_bootstrap_stages_lane_openfigi.sql`)

```sql
ALTER TABLE bootstrap_stages DROP CONSTRAINT bootstrap_stages_lane_check;
ALTER TABLE bootstrap_stages ADD CONSTRAINT bootstrap_stages_lane_check
  CHECK (lane IN ('init', 'etoro', 'sec', 'sec_rate', 'sec_bulk_download',
                  'db', 'db_filings', 'db_fundamentals_raw', 'db_ownership_inst',
                  'db_ownership_insider', 'db_ownership_funds',
                  'openfigi'));
```

### Stage renumber migration (`sql/<N>_bootstrap_stages_insert_cusip_sweep.sql`)

```sql
-- Renumber S13-S26 to S14-S27 in any in-flight run + the spec catalogue
UPDATE bootstrap_stages SET stage_order = stage_order + 1
 WHERE stage_order >= 13;
-- New S13 = cusip_resolver_post_bulk_sweep — inserted by the orchestrator
-- when run_bootstrap_orchestrator scaffolds stages from the spec.
```

### Source/Lane registration

`app/jobs/sources.py` — add `'openfigi'` to `Lane` Literal.
`app/services/bootstrap_orchestrator.py:233-259` — `_LANE_MAX_CONCURRENCY['openfigi'] = 1`.

### OpenFIGI resolver (~400 LoC)

New module `app/services/openfigi_resolver.py`:

```python
class OpenFigiResolver:
    BASE = "https://api.openfigi.com/v3/mapping"
    UNKEYED_PER_MIN = 25
    UNKEYED_BATCH = 10
    KEYED_PER_6S = 25
    KEYED_BATCH = 100

    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key
        self.rate_limiter = _RateLimiter(
            per_window=self.KEYED_PER_6S if api_key else self.UNKEYED_PER_MIN,
            window_seconds=6 if api_key else 60,
        )

    def resolve_cusips(self, cusips: Iterable[str]) -> dict[str, OpenFigiMapping]:
        """Batch resolve CUSIPs → mappings. Returns only successful resolutions."""
        batch_size = self.KEYED_BATCH if self.api_key else self.UNKEYED_BATCH
        results: dict[str, OpenFigiMapping] = {}
        for chunk in _batched(cusips, batch_size):
            with self.rate_limiter:
                resp = self._post(chunk)
            for cusip, mapping in zip(chunk, self._parse_response(resp), strict=True):
                if mapping is not None:
                    results[cusip] = mapping
        return results

    def _post(self, cusips: list[str]) -> httpx.Response:
        body = [{"idType": "ID_CUSIP", "idValue": c} for c in cusips]
        headers = {}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        resp = httpx.post(self.BASE, json=body, headers=headers, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "60"))
            time.sleep(retry_after)
            return self._post(cusips)  # one retry
        resp.raise_for_status()
        return resp

    def _parse_response(self, resp: httpx.Response) -> list[OpenFigiMapping | None]:
        # OpenFIGI returns array parallel to input. Each entry is either
        # {'data': [...]} or {'warning': '...'} or {'error': '...'}.
        out: list[OpenFigiMapping | None] = []
        for entry in resp.json():
            if "data" in entry and entry["data"]:
                first = entry["data"][0]
                out.append(OpenFigiMapping(
                    ticker=first.get("ticker"),
                    name=first.get("name"),
                    exch_code=first.get("exchCode"),
                    share_class_figi=first.get("shareClassFIGI"),
                ))
            else:
                out.append(None)
        return out
```

### Sweep extension (`cusip_resolver.py`)

Extend `sweep_resolvable_unresolved_cusips`:

1. Pre-existing path: name-fuzzy via SEC 13F List (0.92 threshold).
2. NEW path: for remaining unresolved, call `OpenFigiResolver.resolve_cusips()`.
3. For each successful resolution: match `mapping.ticker` against `instruments.symbol` (exact); if match → write `external_identifiers (provider='openfigi', identifier_type='cusip', instrument_id, identifier_value, is_primary=FALSE)`.
4. Trigger `rewash_bulk_source_filings(conn, cusip)` — NEW helper that finds bulk-source unresolved rows and removes them so subsequent bulk re-ingest picks them up; OR (lighter) materialises the now-resolvable rows into `ownership_*_observations` directly.

### `_load_cusip_map` extension

```python
# sec_13f_dataset_ingest.py:67-97  (and sec_nport equivalent at :153-180)
def _load_cusip_map(conn: psycopg.Connection[Any]) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT identifier_value, instrument_id
              FROM external_identifiers
             WHERE provider IN ('sec', 'openfigi')
               AND identifier_type = 'cusip'
        """)
        return dict(cur.fetchall())

# bootstrap_preconditions.py:188 (compute_cusip_coverage):
# SELECT COUNT(DISTINCT instrument_id) FROM external_identifiers
#   WHERE provider IN ('sec', 'openfigi') AND identifier_type='cusip'
```

### Phase D stage S13 (new ordering)

`_BOOTSTRAP_STAGE_SPECS`:

```python
StageSpec(
    stage_key="cusip_resolver_post_bulk_sweep",
    stage_order=13,
    lane="openfigi",
    job_name=JOB_CUSIP_RESOLVER_POST_BULK_SWEEP,
    params={"max_batches": 200},
    requires=CapRequirement(all_of=("institutional_inputs_seeded", "nport_inputs_seeded")),
    provides=(),  # No new cap; writes to existing external_identifiers
)
```

### Floor-gate without `partial_complete`

`bootstrap_runs` migration adds `coverage_floor_met BOOLEAN DEFAULT NULL`. `finalize_run` (`app/services/bootstrap_state.py`) sets it based on post-sweep `compute_cusip_coverage`. Admin panel renders an amber badge if `coverage_floor_met=FALSE`; run still transitions to `complete`.

---

## 6. PR-2 — Dispatcher parallelism

(Unchanged from v2 §6 except for:)
- Persistent pool registry context-manager: `with LaneExecutorRegistry() as registry: ...`
- Test fixture injects `MockExecutor` for deterministic ordering.
- Tests to rewrite: `tests/test_bootstrap_orchestrator.py`, `tests/test_bootstrap_orchestrator_source_registry.py`, `tests/test_bootstrap_adapter.py`, `tests/test_bootstrap_atomic_enqueue.py`.

---

## 7. PR-3 — COPY refactor (NOW includes S9 companyfacts)

(v2 §7 + S9 added based on Codex M finding):

| Stage | LoC delta | Wall-clock target |
|---|---|---|
| S10 13F | 250 | 80 min → 5 min |
| S11 insider | 200 | 10 min → 2 min |
| S12 NPORT | 250 | 46 min → 4 min |
| S9 companyfacts | 200 | 20 min → 5 min |

Pattern: per-archive `BEGIN; CREATE TEMP TABLE _stg ON COMMIT DROP; COPY ... INTO _stg; INSERT INTO target SELECT ... FROM _stg ON CONFLICT ...; COMMIT;` — TEMP table dropped at commit, freshly recreated next iteration.

Lint: `scripts/check_bulk_ingest_copy_pattern.sh` (positive name per v2 review N2).

---

## 8-13. (PR-4 through PR-8)

(v2 §8-§13 with minor fixes per v2 review.)

---

## 14. Honest wall-clock budget (post-Codex re-derivation)

Per-lane critical path (with PR-2 cross-lane parallelism + dedicated `openfigi` lane):

```
init:       S1 (10s)
etoro:      S2 (4 min)
sec_rate:   S3+S4+S5+S6+S7 = 6 min  →  S14+S15+S16+S17+S18+S19+S20+S21+S22 (post-renumber) = ~30 min total
sec_bulk_download: S7 = 5 min (or 30s if PR-5b reuse)
db_filings: S8 = 10 min
db_fundamentals_raw: S9 = 5 min (post PR-3.b)
db_ownership_inst:  S10 = 5 min
db_ownership_insider: S11 = 2 min
db_ownership_funds: S12 = 4 min
openfigi:   S13 (sweep) = up to 48 min unkeyed / 5 min keyed
db:         S23-S27 = ~10 min combined
```

**Critical path with PR-2 + parallel `openfigi` lane:**
- Phase A: max(10s, 4 min, 6 min) = 6 min (parallel init / etoro / sec_rate)
- Phase C (parallel db lanes + sec_rate S14+): max(10, 9, 5, 5, 2, 4, ~30) = max(10, 30) = 30 min (sec_rate dominates)
- Phase D: max(openfigi 48 min unkeyed, db ~10 min) = 48 min unkeyed / 10 min keyed

**Total Tier 1: ~6 + 30 + 10 = 46 min** (keyed) or **~6 + 30 + 48 = 84 min** (unkeyed).

With operator-supplied OpenFIGI key → **Tier 1 ≤ 45 min target achievable.**

Without key → **Tier 1 ≤ 90 min ceiling.**

Tier 2 (catalogue collapse + S15 replacement + parallel sec_rate within lane):
- sec_rate critical drops to ~15 min
- openfigi keyed = 5 min
- **Tier 2 ≤ 30 min.**

---

## 15. Acceptance criteria (revised)

```
ASSERT bootstrap_runs.status = 'complete'
ASSERT bootstrap_runs.coverage_floor_met = TRUE
   OR (coverage_floor_met = FALSE AND CUSIP coverage between 50% and 80%)
   -- below 50%: hard refuse via existing bootstrap_preconditions floor
ASSERT bootstrap_runs elapsed ≤ 60 min (Tier 1 keyed) / ≤ 90 min (Tier 1 unkeyed)
ASSERT every stage IN ('success', 'skipped')
ASSERT external_identifiers (provider IN ('sec','openfigi'), identifier_type='cusip') count ≥ 0.50 × tradable instruments
ASSERT ownership_*_current rows ≥ _CAPABILITY_MIN_ROWS[corresponding cap]
ASSERT bootstrap_archive_results aggregate drop rate < 25% per archive
ASSERT financial_facts_raw rows > 0
ASSERT instrument_share_count_latest has rows for {AAPL, GME, MSFT, JPM, HD}
ASSERT GET /instruments/AAPL/ownership-rollup non-empty within 1s
ASSERT GET /instruments/GOOGL/ownership-rollup returns 200 with EITHER
   non-empty body OR partial_data_reason='share_class_sibling_pre_PR_B'
ASSERT data_freshness_index has rows for all (subject, source) triples touched
```

Robustness (CI-testable):

```
ASSERT mid-archive operator cancel observed within 60s
ASSERT process crash mid-stage: reaper resets to pending within 6 min (5 min grace + 1 min poll)
ASSERT OpenFIGI 429 with Retry-After: resolver backs off + completes batch
ASSERT OpenFIGI 5xx extended outage: bootstrap completes with coverage_floor_met=FALSE
ASSERT two operators trigger bootstrap simultaneously: partial-unique index blocks second
ASSERT S13 cancel mid-sweep: buffer rows remain unresolved for next sweep
```

---

## 16-20. (Migration safety, out-of-scope, skill updates, decision log, review gate)

(Carried from v2 with adjustments noted in changelog.)

---

## 21. v3 review gate

Before any implementation:

1. **Codex 2 spec review on v3** (re-run).
2. **Third clean-agent adversarial pass on v3** — must report ≤ 2 HIGH findings to converge.
3. **Operator sign-off** on SD-1, SD-2 + Tier 1 plan + 60-min target (keyed) / 90-min ceiling (unkeyed).

If v3 review still finds ≥ 3 HIGH defects: v4. Per iterative-refinement memory: keep going until convergence.

---

## Closing

v3 addresses every CRITICAL + HIGH from v2 review. The architectural insight from the adversarial pass that 90 min is too conservative — actual is 47-57 min with `openfigi` lane separated — moves the Tier 1 target to **60 min keyed / 90 min unkeyed**, retiring the 45-min Tier 2 stretch goal as actually achievable in Tier 1 with operator-provided API key.

Spec growing larger reflects honest design — not bloat. Code remains minimal: target Tier 1 net repo delta = +6480 LoC across 11 PRs, of which 2450 LoC is tests.
