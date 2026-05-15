# N-CSR / N-CSRS fund-scoped bootstrap drain (T8)

> Status: **DRAFT 2026-05-15** — pending Codex pre-spec 1a + operator signoff.
>
> Issue: **#1174**. Parent ticket: **#1171** (SHIPPED 2026-05-15, merge `3c31b5e`).
> Branch: `feature/1174-n-csr-bootstrap-drain`.

## 1. Problem

#1171 landed the real N-CSR / N-CSRS iXBRL → fund-metadata parser. End-to-end pipeline works (Vanguard accession `0001104659-26-021519` parsed into `fund_metadata_observations` for VOO / VTV / VUG; cross-source verified against Vanguard factsheet).

The parser has **nothing to drain** for the ~28k mutual-fund / ETF trust CIKs catalogued in `cik_refresh_mf_directory`. Two reasons:

1. The first-install drain at `app/jobs/sec_first_install_drain.py:167` explicitly excludes `sec_n_csr` from the issuer-scoped seed (issuer rows must carry an instrument_id; N-CSR is trust-scoped + multi-series).
2. Layer 1/2/3 steady-state discovery (#1155) does fire for trust CIKs that publish into the getcurrent atom or daily-index feed AFTER the first-install drain runs — but on a fresh install with no in-flight filings, the parser sees zero N-CSR work.

T8 closes the gap: a dedicated bootstrap pass walks trust CIKs from `cik_refresh_mf_directory` and enqueues last-2-years N-CSR + N-CSRS accessions to `sec_filing_manifest` so the manifest worker can drain them.

Post-merge state (captured 2026-05-15 ~01:10 UTC):

| Table / source | Count | Note |
|---|---|---|
| `cik_refresh_mf_directory` | 28,308 rows | classId → (series, symbol, trust_cik) map populated by Stage 6 (#1171 T4) |
| `external_identifiers (provider='sec', identifier_type='class_id')` | 436 rows | classes whose symbol matches a universe instrument |
| `sec_filing_manifest` `source='sec_n_csr'` | 154 rows | all 8 non-universe trust CIKs; all tombstoned (`instrument_not_in_universe` / pre-iXBRL era) |
| `fund_metadata_observations` | 3 rows | VOO / VTV / VUG, seeded by hand to prove pipeline |
| `fund_metadata_current` | 3 rows | same |

## 2. Goals

1. On a fresh install, after the dedicated S25 `mf_directory_sync` stage populates `cik_refresh_mf_directory`, the T8 stage (S26) enqueues N-CSR + N-CSRS accessions for every trust the directory advertises, bounded to a 2-year horizon (default 730 days).
2. The manifest worker then drains those accessions via the #1171 parser. For accessions whose iXBRL carries at least one in-universe class, `fund_metadata_observations` + `fund_metadata_current` populate. For accessions with zero in-universe classes, the parser tombstones with `instrument_not_in_universe` (existing behaviour; no change).
3. Re-runs are idempotent. The existing per-accession `record_manifest_entry` UPSERT plus the worker's parsed-state gate handle this.
4. Cancel-cooperative: the stage polls `bootstrap_cancel_requested()` at a coarse cadence (every N trusts) so an operator-cancel completes in <5s of observed latency.

Non-goals (explicit out-of-scope, file as follow-ups if needed):

- Universe-class-id growth (#841 — CUSIP + ETF universe expansion). Today only 436 classes resolve; that's a coverage gap not addressable here.
- Layer 1/2/3 steady-state firing verification (Lane B — separate session). T8 is the one-shot bootstrap; the steady-state path is orthogonal.
- `data_freshness_index` seeding cadence verification (Lane C — separate session). The inline scheduler seed inside `record_manifest_entry` handles freshness rows; that's verified separately.
- Filings older than 2 years. The retention horizon mirrors the existing `filings_history_seed` stage (also 730 days). Historical backfill is a tech-debt ticket if operator demand emerges.

## 3. Design

### 3.1 Function shape

New free function in `app/jobs/sec_first_install_drain.py`:

```python
def bootstrap_n_csr_drain(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    horizon_days: int = 730,
) -> "NCsrDrainStats":
    """Walk fund-trust CIKs from cik_refresh_mf_directory + enqueue
    last-`horizon_days` N-CSR + N-CSRS accessions per trust to
    sec_filing_manifest.

    Pre-condition: ``class_id_mapping_ready`` capability (S25
    ``mf_directory_sync`` populates ``cik_refresh_mf_directory``).

    Cancel-cooperative: polls ``bootstrap_cancel_requested()`` every
    ``_CANCEL_POLL_EVERY_N`` trusts; raises
    ``BootstrapStageCancelled`` on observed cancel.
    """
```

`NCsrDrainStats` is a dataclass mirroring `DrainStats`:

```python
@dataclass(frozen=True)
class NCsrDrainStats:
    trusts_processed: int
    trusts_skipped: int           # submissions.json 404 / fetch error
    secondary_pages_fetched: int  # `filings.files[]` walked for deep history
    manifest_rows_upserted: int
    accessions_outside_horizon: int  # observed but skipped (sanity counter)
    errors: int
```

### 3.2 Trust-CIK iterator

Cohort: `SELECT DISTINCT trust_cik FROM cik_refresh_mf_directory WHERE trust_cik IS NOT NULL ORDER BY trust_cik` (deterministic order for crash-resume + test reproducibility; manifest UPSERT idempotency carries actual safety).

`cik_refresh_mf_directory.trust_cik` is already 10-digit zero-padded by the Stage 6 writer (`mf_directory.py:101`), so the iterator is a plain text walk.

The cohort universe is ~28k rows but `DISTINCT trust_cik` collapses to ~600-800 trusts (Vanguard, iShares, Fidelity, BlackRock, etc.). One HTTP fetch per distinct trust + optional secondary-page walks. At the shared SEC pool rate (~10 r/s), expected wall-clock is **~5-15 minutes** including pagination — corrected downward from the issue's first-draft ~47 min estimate (which incorrectly assumed one fetch per row).

### 3.3 Per-trust enqueue

Reuse `check_freshness(http_get, cik=trust_cik, last_known_filing_id=None, sources={'sec_n_csr'})` to fetch + parse the primary submissions.json page. This:

- shares the existing rate-limited client + retry plumbing,
- filters at parse-time to `source='sec_n_csr'` (the form-to-source map already routes `N-CSR` + `N-CSR/A` + `N-CSRS` + `N-CSRS/A` to `sec_n_csr`),
- returns the `filings.files[]` list for secondary-page pagination so deep history beyond the ~1000-most-recent inline cap is reachable.

For each filing row from `delta.new_filings`:

1. Skip if `row.source != 'sec_n_csr'` (defensive belt-and-braces; `check_freshness`'s `sources` filter already does this).
2. Skip if `row.filed_at < (now() - horizon_days)`. Bump `accessions_outside_horizon` counter so the sanity check is auditable.
3. Call `record_manifest_entry(...)` with:
   - `subject_type='institutional_filer'` — matches N-PORT precedent (`app/services/manifest_parsers/sec_n_port.py:28`), satisfies the `chk_manifest_issuer_has_instrument` CHECK (institutional_filer must have `instrument_id IS NULL`).
   - `subject_id=trust_cik`.
   - `instrument_id=None`.
   - `is_amendment=row.is_amendment`.
   - everything else passes through from the row.

Pagination contract (Codex 1a WARNING addressed): if `delta.has_more_in_files and delta.files_pages`, walk **every** secondary page returned by the SEC. Submissions.json is keyed by accession (not by date) and secondary pages are appended chronologically, so the only way to know the page is fully outside the horizon is to parse it. We accept the full secondary walk per trust (typically 0-3 pages) and apply the horizon filter at the row level on every page. The new bootstrap drain implements a trust-scoped local helper rather than reusing `_drain_secondary_pages` directly because:

1. It must filter rows to `source='sec_n_csr'` only (the existing helper writes every mapped source — would over-enqueue if reused as-is). Codex 1a WARNING explicit.
2. It must enforce `subject_type='institutional_filer' + subject_id=trust_cik + instrument_id=None` consistently; the existing helper takes a `ResolvedSubject` and writes whatever the caller supplied.
3. It must apply the horizon filter on every row, including secondary-page rows.

The implementation plan should consider whether to refactor `_drain_secondary_pages` to accept a row predicate + subject-supplier closure (DRY win) versus duplicating a small loop body (simpler diff). Either acceptable.

**Subject-type decision rationale**: at enqueue time we know only `trust_cik`, not `series_id`. The manifest accession is PK so one row per (accession, *) — we cannot pre-fan-out to per-series rows. The parser fans out at parse time to per-(series, class) `fund_metadata_observations` rows. `institutional_filer` is the existing subject_type that represents "filer-scoped row with `instrument_id IS NULL`"; it matches the N-PORT precedent + CHECK constraint. The plan §2.T8 text mentioning `subject_type='fund_series'` was a misread — `fund_series` requires a `subject_id` matching `S\d{9}` regex AND would force pre-fan-out (impossible without first parsing the iXBRL). Explicitly noting the decision here so Codex 1a + the implementation plan stay aligned.

### 3.4 Bootstrap stage entry — dedicated MF-directory stage

**Source cadence ground-truth** (edgar skill §1, live header check 2026-05-15):

- `company_tickers_mf.json` is republished **nightly** by SEC. Live `last-modified` header confirms one update per day.
- Our `daily_cik_refresh` already runs daily — matched cadence. Under normal operation `cik_refresh_mf_directory.last_seen` is always within 24h.
- There is no slower upstream that would warrant a multi-day tolerance window.

The implication: a freshness "staleness threshold" is not the right tool. Either the daily refresh ran (table is fresh) or it didn't (table is stale or empty, and operator has an ops incident — the daily cron failed for N nights). A capability gate that maps directly onto "did the directory refresh succeed on the most recent run" is sufficient. No timer needed.

**Design** — split the MF directory refresh into its own bootstrap stage, separate from `cik_refresh`:

1. New capability `class_id_mapping_ready` added to the `Capability = Literal[...]` union.
2. New bootstrap stage `mf_directory_sync` at S25 (appended after `fundamentals_sync` S24). Lane: `sec_rate`. Job: `JOB_MF_DIRECTORY_SYNC` (new constant). `_STAGE_REQUIRES_CAPS` entry: `CapRequirement(all_of=('universe_seeded',))` (we need instruments in place so the symbol → instrument_id JOIN populates external_identifiers). `_STAGE_PROVIDES` entry: `('class_id_mapping_ready',)`.
3. The new stage's invoker (in `app/workers/scheduler.py`) calls the existing `refresh_mf_directory(conn, provider=...)` — same function, same fetch, same writes. **No fail-soft** on the bootstrap stage path: if the fetch fails, the stage transitions to `error`, the capability is not advertised, and T8 transitions to `blocked` (existing orchestrator semantics: provider `error` → downstream `blocked`, not `skipped`). Operator sees the failed S25 in the Timeline + the blocked T8 row pointing at it, and remediates by re-running S25 or the bootstrap as a whole.
4. New stage `sec_n_csr_bootstrap_drain` at S26 (T8 proper). Lane: `sec_rate`. `_STAGE_REQUIRES_CAPS` entry: `CapRequirement(all_of=('class_id_mapping_ready',))`. `_STAGE_PROVIDES`: empty (terminal). `params={'horizon_days': 730}`.

**Daily cron path is unchanged** — `daily_cik_refresh` keeps the bundled `refresh_mf_directory` call at `app/workers/scheduler.py:1769` (with its fail-soft `try/except` retained). The cron is a daily safety net + drift detection: if the bootstrap-side stage ever produces stale data (e.g. ops failed to re-run bootstrap), the daily cron heals it on the next nightly run. Belt-and-braces; same code path; one source of truth (`refresh_mf_directory`).

**Stage 6 304/hash-skip relocation** — REMOVED from this PR's scope. The bug is real but it only matters for fresh-install where Stage 6's 304 short-circuit could leave the bundled MF refresh un-called on day 1. The new dedicated `mf_directory_sync` stage in bootstrap covers this — bootstrap explicitly calls `refresh_mf_directory` regardless of the equity-CIK 304/hash-skip path. The cron-side bug becomes a follow-up tech-debt issue (low priority — only affects "MF directory is stale on a long-running install where neither bootstrap re-runs nor the daily cron's 200-body has fired in months", which is a separate ops alert anyway).

**No consumer-side staleness check in T8** — the capability is now truthful: stage success ⇒ MF directory refreshed THIS bootstrap run. T8 trusts the capability. The only defensive check at T8 entry is a row-count assertion (`SELECT COUNT(*) FROM cik_refresh_mf_directory > 0`) for a friendly error message if an operator manually triggers T8 on a fresh install where the dedicated stage hasn't run yet — that path is the capability-gate's job to prevent, but the assertion documents the precondition at the consumer site.

**Rationale for new capability** (vs reusing `cik_mapping_ready`): the two artifacts have different invariants. `cik_mapping_ready` ("equity issuer CIK → instrument_id") is satisfied by `external_identifiers` `(provider='sec', identifier_type='cik')` having rows. `class_id_mapping_ready` is satisfied by `cik_refresh_mf_directory` having rows + `external_identifiers (identifier_type='class_id')` having rows for in-universe symbols. They populate from different SEC files, can fail independently, and have different downstream consumers.

### 3.5 Invoker wiring

Add to `app/jobs/runtime.py` `_INVOKERS`:

```python
_INVOKERS[JOB_SEC_N_CSR_BOOTSTRAP_DRAIN] = sec_n_csr_bootstrap_drain
```

`sec_n_csr_bootstrap_drain(params: Mapping[str, Any]) -> None` lives in `app/workers/scheduler.py`. Body mirrors the existing `sec_first_install_drain` wrapper at `:4275`:

1. Open `SecFilingsProvider` + `psycopg.connect(...)`.
2. Adapt the provider's ResilientClient to `HttpGet` via `_make_sec_http_get(sec)`.
3. Call `bootstrap_n_csr_drain(conn, http_get=..., horizon_days=params.get('horizon_days', 730))`.
4. Record `manifest_rows_upserted` to the tracker.

### 3.6 Idempotency

`record_manifest_entry` UPSERTs on `(accession_number)` (PK). Re-running the drain is safe — the UPSERT's `ON CONFLICT DO UPDATE` clause refreshes `updated_at` plus discovery-side metadata (`primary_document_url`, `accepted_at`, `is_amendment`) when SEC has emitted a richer field set since the original write, but does NOT flip lifecycle state (`ingest_status`, `parser_version`, `raw_status`, `next_retry_at`). The worker's drain progress is preserved across re-enqueue (Codex 1a NIT — phrasing tightened from "identical content modulo updated_at" to reflect the actual UPSERT scope).

(Implementation plan must verify this against the actual `record_manifest_entry` body before coding — this spec phase only asserts the contract.)

## 4. Implementation files

- `app/jobs/sec_first_install_drain.py` — add `bootstrap_n_csr_drain` function + `NCsrDrainStats` dataclass. The existing N-CSR exclusion at `:167` stays (issuer-scoped seed path remains correct).
- `app/services/bootstrap_orchestrator.py` — add capability literal (`class_id_mapping_ready`), TWO new stage specs (S25 `mf_directory_sync` + S26 `sec_n_csr_bootstrap_drain`), `_STAGE_PROVIDES` entry for S25 only, `_STAGE_REQUIRES_CAPS` entries for both, job-name constants (`JOB_MF_DIRECTORY_SYNC`, `JOB_SEC_N_CSR_BOOTSTRAP_DRAIN`).
- `app/workers/scheduler.py` — add TWO wrappers: `mf_directory_sync(params)` (calls existing `refresh_mf_directory` with NO fail-soft) and `sec_n_csr_bootstrap_drain(params)` (adapts provider → `HttpGet` and dispatches).
- `app/jobs/runtime.py` — register both invokers.
- `tests/test_sec_first_install_drain.py` — add the 13 test cases enumerated in §6.
- `.claude/skills/data-engineer/etl-endpoint-coverage.md` — restate row 47 (`sec_n_csr`) to "real parser + bootstrap drain landed" with this PR link.

No schema migration. No new tables.

## 5. Source-priority + freshness interactions

T8 enqueues manifest rows. It does NOT write `fund_metadata_observations` directly. The parser owns the source-priority chain established by #1171 (`period_end DESC, filed_at DESC, source_accession DESC`). The drain only seeds work for the parser.

`data_freshness_index` rows are seeded inline by `record_manifest_entry` (#956); the drain inherits that path for free. No bespoke freshness seed.

## 6. Test plan

`tests/test_sec_first_install_drain.py` extends with the following cases (golden submissions.json fixtures live under `tests/fixtures/sec_submissions/`):

| # | Case | Setup | Assert |
|---|---|---|---|
| 1 | First-run writes manifest rows | Seed `cik_refresh_mf_directory` with one trust (e.g. CIK 36405 → series/class rows). Fake `http_get` returns a golden submissions.json with 3 N-CSR + 2 N-CSRS + 5 non-N-CSR filings within horizon. | `sec_filing_manifest` has exactly 5 rows, all `source='sec_n_csr'`, `subject_type='institutional_filer'`, `subject_id='0000036405'`, `instrument_id IS NULL`. `stats.manifest_rows_upserted == 5`. |
| 2 | Idempotent re-run | Run case 1 twice. | Second run: same row count, no duplicates, `stats.manifest_rows_upserted == 5` (UPSERT counts each touch). No row's `ingest_status` flipped back from non-`pending` to `pending`. |
| 3 | 2-year horizon truncation (primary page) | Golden fixture includes 1 N-CSR filed `now() - 800d`. | That row absent from manifest; `stats.accessions_outside_horizon == 1`. |
| 4 | CHECK constraint honored | Inspect rows from case 1 directly. | All `chk_manifest_issuer_has_instrument` constraint conditions satisfied (`subject_type='institutional_filer' AND instrument_id IS NULL`). |
| 5 | Cancel signal observed (Codex 1a fix) | Seed 200 trusts. Monkeypatch `bootstrap_cancel_requested` to return `True` after the first trust. | `pytest.raises(BootstrapStageCancelled)` fires. The exception's `stage_key` is `'sec_n_csr_bootstrap_drain'` (or empty if no active dispatch context). Mock `http_get` was called at most `_CANCEL_POLL_EVERY_N + 1` times — asserts bounded HTTP issuance under cancel. (No `stats` object — cancel raises before return.) |
| 6 | Secondary-page pagination + horizon (Codex 1a WARNING) | Golden primary submissions.json has `has_more_in_files=True` + 2 `files[]` pages. Page 1 carries 5 N-CSR rows in horizon; page 2 carries 3 N-CSR + 2 N-PORT all >730d old. | `stats.manifest_rows_upserted == 5`. No N-PORT rows enqueued (source filter). `accessions_outside_horizon == 3`. `secondary_pages_fetched == 2`. |
| 7 | Secondary-page source filter (Codex 1a WARNING) | Golden secondary page mixes N-CSR + 10-K + 13F-HR rows. | Only `sec_n_csr` rows enqueued. Other rows absent (the drain does NOT delegate writes for non-matching sources — explicit guard). |
| 8 | 404 submissions.json | Fake `http_get` returns `(404, b'')` for one trust. | `stats.trusts_skipped == 1`, `stats.errors == 0`. No exception bubbled. |
| 9 | Fetch exception | Fake `http_get` raises `RuntimeError` for one trust. | `stats.errors == 1`. Drain continues to the next trust. |
| 10 | Empty trust cohort (manual-trigger guard) | `cik_refresh_mf_directory` has zero rows. | `bootstrap_n_csr_drain` raises `BootstrapPhaseSkipped` with reason `class_id_mapping_ready unsatisfied — cik_refresh_mf_directory empty`. No HTTP calls. |
| 11 | Scheduler / freshness side-effect (#956 contract) | After case 1, query `data_freshness_index` for the `(institutional_filer, trust_cik, sec_n_csr)` triple. | Exactly 1 row exists, inline-seeded by `record_manifest_entry`. Confirms the inline-seed contract carries to the new subject identity. |
| 12 | `mf_directory_sync` stage success advertises capability | Fire the new stage's invoker against a fake provider that returns a golden mf.json with 5 trusts. | After invoker returns: `cik_refresh_mf_directory` has 5 rows; bootstrap_orchestrator catalogue test confirms stage advertises `class_id_mapping_ready` in `_STAGE_PROVIDES`. |
| 13 | `mf_directory_sync` stage failure surfaces (no fail-soft) | Fake provider raises `RuntimeError` on fetch. | Invoker propagates the exception; stage transitions to `error`; capability NOT advertised. Catalogue-level test asserts T8 `(S26)`'s computed status under `_classify_dead_cap('class_id_mapping_ready', ...)` is `blocked` (provider in `error` → downstream `blocked`, not `skipped`). |

Smoke-time (post-push, on dev DB) per CLAUDE.md DoD clauses 8-12. Smoke panel:

| Instrument | Trust CIK | Expected outcome | Note |
|---|---|---|---|
| VFIAX | 36405 (Vanguard) | manifest rows enqueued; drained; `fund_metadata_current` row appears with ER ≈ 0.04% | |
| VOO | 36405 (Vanguard) | already populated (#1171 seed); rerun confirms idempotency | |
| IVV | 1100663 (iShares) | manifest rows enqueued; drained; ER ≈ 0.03% | |
| AGG | 1100663 (iShares) | manifest rows enqueued; drained; bond fund → `credit_quality_allocation` populated | |
| FXAIX | 819118 (Fidelity) | manifest rows enqueued; drained; ER ≈ 0.015% | if Fidelity TSR coverage holds — else expected tombstone with explanatory log |

Cross-source for VFIAX: expense_ratio_pct vs Vanguard factsheet (exact match), net_assets_amt within ±1%.

## 7. Risks

| Risk | Mitigation |
|---|---|
| `cik_refresh_mf_directory` is small enough today (28k rows; ~600-800 distinct trusts) that the SEC budget impact is moot. But if SEC adds dozens of new trusts, drain wall-clock grows. | Time-bounded by `horizon_days` + per-trust pagination cap. Acceptable to land "drain takes 30 min on full universe" — bootstrap is a once-per-install operation. |
| Some trusts file N-CSR/A amendments that supersede the original by `period_end` tie. | #1171 source-priority chain already handles this (amendments file later → win on `filed_at DESC` tie-break). |
| Submission.json 404 for a trust CIK that's in `cik_refresh_mf_directory` but missing from EDGAR | `check_freshness` returns empty `FreshnessDelta` on 404 (existing behaviour at `sec_submissions.py:246`). Increment `trusts_skipped` counter; no error. |
| Layer 2 daily-index reconciler not yet firing in prod (Lane B blocker) | T8 lands the bootstrap drain — it does NOT replace the steady-state path. After Lane B fires, both paths coexist and the manifest UPSERT idempotency handles re-discovery. |
| `instrument_not_in_universe` flood from non-universe trusts that file N-CSR | Parser tombstones cleanly with explicit reason (#1171 behaviour). Manifest rows accumulate as `tombstoned`; that's expected coverage signal — operators see "X trust filings observed but no in-universe classes". |

## 8. Settled-decisions

No new settled-decisions. Confirms / preserves:

- #1171 source-priority chain (`period_end DESC, filed_at DESC, source_accession DESC` within `(instrument_id, period_end)`).
- Filing event storage: no raw N-CSR body retained; parser is `requires_raw_payload=False` and re-fetches on rewash.
- External_identifiers as canonical classId resolver.

## 9. Acceptance criteria

1. `bootstrap_n_csr_drain` lands with unit tests covering all scenarios in §6 (cases 1-13).
2. Stage entry visible in `/admin/bootstrap` Timeline with lane `sec_rate`.
3. Capability gating: stage skips with cascade reason if `class_id_mapping_ready` is unsatisfied.
4. Dev-DB smoke confirms VFIAX / IVV / AGG accessions enqueue → drain → `fund_metadata_current` populates with operator-visible ER + NAV.
5. PR body embeds smoke-panel table + cross-source check per DoD clauses 8-12.
6. `.claude/skills/data-engineer/etl-endpoint-coverage.md` row 47 restated to reflect bootstrap-drain landed.

## 10. Review-prevention-log applicability

- **#86 (fixed-phrase 503 detail)** — N/A; T8 surfaces no public endpoint.
- **#1172 `is_primary` resolver entry (PR #1170)** — relevant for the upstream T4 write path; T8 reads via `resolve_class_id_to_instrument` indirectly through the parser. T8 itself does not write `external_identifiers`, so no new exposure. Implementation plan re-verifies the resolver continues to filter `is_primary = TRUE`.
- **"Service accepting `conn` must not commit"** — relevant. `bootstrap_n_csr_drain(conn, ...)` accepts a caller-owned connection. The function MUST NOT call `conn.commit()` directly; it should write inside `with conn.transaction()` blocks where atomicity is needed (per-row `record_manifest_entry` already operates this way) and let the wrapper at `app/workers/scheduler.py` own the outer commit boundary via `with psycopg.connect(...) as conn` context-manager close.
- **#1131 transient-vs-deterministic upsert exception classification** — relevant if `record_manifest_entry` ever raises. The existing helper at `app/services/sec_manifest.py` already discriminates (`ValueError` on row-validation failures only). The new drain catches `ValueError` and logs (mirroring `seed_manifest_from_filing_events`); does not bubble.
- **#1290 catalogue-resolved indirection for `failing_jobs`** — relevant for the cancel test (case 5). Use `{spec.stage_key: spec.job_name for spec in get_bootstrap_stage_specs()}` lookup rather than hardcoding `JOB_SEC_N_CSR_BOOTSTRAP_DRAIN` as a string in the test set.
- **#1296 transaction-aborted-by-CheckViolation** — relevant for the CHECK-constraint test (case 4). Wrap any malformed-INSERT assertion in `ebull_test_conn.transaction()` so the parent fixture stays usable.

## 11. Codex pre-spec 1a checklist (self-review hints)

- Subject-type rationale: institutional_filer + `instrument_id=NULL` + trust_cik as subject_id matches CHECK + N-PORT precedent. The plan's mention of `fund_series` is reconciled in §3.3.
- Capability is additive — provided exclusively by the new dedicated S25 `mf_directory_sync` stage (per §3.4 final design). Independent provider; Stage 6 `cik_refresh` continues to provide `cik_mapping_ready` only. The Stage 6 304/hash-skip relocation is OUT OF SCOPE; the dedicated bootstrap stage covers the truthfulness invariant without touching the existing equity-CIK refresh path.
- Horizon mirrors `filings_history_seed.days_back=730`. Tweakable via params.
- Idempotency phrasing tightened against `record_manifest_entry`'s UPSERT scope (lifecycle state preserved; discovery metadata refreshed).
- No new schema; no new tables.
- Secondary-page walk is full-walk (not horizon-fetch-bounded); horizon filter applies row-by-row on every page. Source filter applied at every level.

## 12. Sign-off

- Codex 1a round 1: 1 BLOCKING + 4 WARNING + 3 NIT. All addressed:
  - BLOCKING (Stage 6 304 path) → fix-in-scope per §3.4.
  - WARNING (horizon row vs fetch-bounded) → §3.3 explicit full-walk + row-filter.
  - WARNING (secondary-page source filter) → §3.3 explicit + test case 7.
  - WARNING (cancel test exception shape) → §6 case 5 rewritten.
  - WARNING (pagination / 404 / freshness side-effect coverage) → §6 cases 6-11 added.
  - NIT (idempotency phrasing) → §3.6 tightened.
  - NIT (prevention-log applicability) → §10 added.
  - NIT (subject-type correct) → acknowledged in §11.
- Codex 1a round 2: 1 BLOCKING (fail-soft → stale capability) + 1 NIT (test count mismatch). First-pass fix used 7d staleness check; operator pushback (cadence-aware design) prompted Round 3 re-design.
- Codex 1a round 3: CLEAN (cached at /tmp/codex_1174_1a_round3.txt) on the 7d-staleness-check shape.
- **Round 4 operator pushback** (this revision): "Why 7d? Source updates daily — why scheduled-freshness instead of fixing upstream?" Spec re-designed per §3.4:
  - Split MF directory refresh into a dedicated bootstrap stage (S25 `mf_directory_sync`) with no fail-soft on the bootstrap path. Capability advertised iff stage succeeded this run.
  - Daily cron `daily_cik_refresh` keeps the bundled call (drift-heal safety net; fail-soft preserved for cron robustness).
  - Dropped Stage 6 304/hash-skip relocation + the 7d staleness check entirely. T8 trusts the capability; only a row-count assertion at entry for manual-trigger friendliness.
  - Test cases 12-13 rewritten to cover the new stage's success / failure paths.
- Codex 1a round 4: 1 BLOCKING (cascade-blocked vs skip) + 1 WARNING + 2 NIT. Addressed.
- Codex 1a round 5: 1 WARNING + 1 NIT (stale references to dropped co-declaration). Addressed.
- Codex 1a round 6: 1 NIT (residual `co-declared` verbiage). Addressed.
- Codex 1a round 7: **CLEAN** (cached at `/tmp/codex_1174_1a_round7.txt`).
- Operator: pending.
- Implementation plan: pending (drafted after spec signoff).
