# Bulk-archive cache lifecycle — fetch / consume / evict contract (#3113, stage 1)

Scope: the **filesystem** archive cache at `resolve_data_dir()/sec/bulk`. Not
`filing_raw_documents` payload compaction (#2774), not per-company discovery dedupe (#2126).

⚠ **This spec's first draft proposed a different stage 1 — moving the two daily bulk
refreshes to demand-fetch — and Codex checkpoint 1 killed it on evidence.** What is
specced below is what survived. The reversal is recorded in §5 rather than deleted,
because the ordering it establishes is the main design output of this stage.

## 1. Source rule

| rule | source |
| --- | --- |
| `submissions.zip` and `companyfacts.zip` are **rebuilt nightly ~03:00 ET** | SEC EDGAR APIs page; `.claude/skills/data-sources/sec-edgar.md` §1 ("Bulk over per-filing whenever possible") and the bulk-archive inventory rows for both ZIPs |
| SEC ignores `If-None-Match` / `If-Modified-Since` on bulk `.zip` — 200 + full body regardless | same skill, §4 "Bulk-archive reuse contract"; empirical probe 2026-05-22 |
| Reuse of a local `.zip` is permitted only on `.zip.etag` match **AND** `.zip.sha256` match | `docs/settled-decisions.md` "Bulk archive reuse keyed on SEC ETag + SHA-256 (2026-05-22)" |
| A cheap check may DECLINE to certify, never certify | `docs/review-prevention-log.md`, #3112 entry (2026-09-16) |

⚠ The publication rule bounds how often the bytes *can* change; it does **not** establish
that the `ETag` changes nightly, and this spec does not infer that. Transfer frequency is a
measurement (§2.1), not a deduction from the cadence.

This spec does not touch the reuse contract. ETag + SHA-256 remains the identity test on the
preflight path; §4 tightens one path that was **weaker** than the settled decision, and
changes nothing else about it.

## 2. Measurement

Re-measured from this branch on 2026-09-16. Each figure states the query that produced it.

### 2.1 Producers

`select job_name, count(*), count(*) filter (where coalesce(row_count,0) > 0), sum(coalesce(row_count,0)), min(started_at)::date, max(started_at) from job_runs where job_name in (…) group by 1`
— these jobs record `row_count = bytes_downloaded`:

| job | runs | rows with bytes>0 | rows with bytes=0 | recorded bytes | first | last |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `sec_submissions_bulk_refresh` | 86 | 56 | 30 | **86.94 GB** | 2026-06-04 | 2026-09-16 08:00Z |
| `sec_companyfacts_bulk_refresh` | 83 | 56 | 27 | **78.04 GB** | 2026-06-04 | 2026-09-16 08:30Z |
| `sec_quarterly_datasets_bulk_refresh` | 5 | **0** | 5 | 0 | 2026-06-05 | 2026-08-05 06:00Z |

Total **164.98 GB**. The span 2026-06-04 → 2026-09-16 is 104 elapsed days / 105 inclusive
daily fire dates; on the elapsed denominator that is 1.586 GB/day, ≈ **580 GB/yr**
(164.98 / 104 × 365 = 579.0).

⚠ **What this table does and does not say.**

- `row_count` is **recorded publication bytes**, not bytes pulled off the wire. A fetch whose
  body is downloaded and then rejected (bad digest, weak ETag, publish failure) records zero.
  So 580 GB/yr is a **lower bound** on transfer.
- A zero-byte row does **not** identify why: ETag match, bootstrap fence, HEAD failure and
  missing telemetry are indistinguishable in this query. The split is not attributed here
  and no claim rests on attributing it.
- 86 runs across 105 fire dates is a shortfall of 19, cause not established by this query.
- ⚠ The #3113 evidence comment extrapolated "≈1.4 TB/yr". That figure does not reproduce from
  these rows and is superseded by the ≈580 GB/yr line above.

### 2.2 Consumers

⚠⚠ The consumption oracle is **`bootstrap_archive_results`, not `job_runs`** — the bulk ingest
jobs only get a `job_runs` row when dispatched through `app/jobs/runtime.py`, and the bootstrap
orchestrator calls them directly (`bootstrap_orchestrator.py:1356-1373`).

`select stage_key, count(distinct archive_name), sum(rows_written), max(completed_at) from bootstrap_archive_results group by 1`
returns five archive-consuming stages (`sec_companyfacts_ingest`, `sec_submissions_ingest`,
`sec_13f_ingest_from_dataset`, `sec_nport_ingest_from_dataset`, `sec_insider_ingest_from_dataset`)
whose `max(completed_at)` is 2026-06-03 for every one.
⚠ `count(distinct archive_name)` includes each stage's `__job__` provenance row, so these counts
run one above the #3113 evidence comment's per-archive counts, and `max(completed_at)` bounds the
latest row, not every row.

Readers of the bulk directory, from `rg -n "_bulk_dir\(\)|bulk_dir|submissions\.zip|companyfacts\.zip"`:

| reader | kind | freshness check before reading |
| --- | --- | --- |
| `sec_*_ingest*` jobs (`app/services/sec_bulk_orchestrator_jobs.py`) | bootstrap phase C / operator-invoked | **existence only** (`:175` and siblings) — no ETag, no digest |
| `sec_submissions_files_walk.py:613` | bootstrap C1b precondition assert only | n/a — its secondary pages come over HTTP |
| `app/jobs/sec_first_install_drain.py` (`use_bulk_zip=True`) | operator-triggered; hybrid local-zip path documented bootstrap-only | none |
| `app/runbooks/stream_a_t13_sidecar_repair.py` | operator runbook — **replay** | none; takes `--archive-path` |
| `scripts/backfill_*`, `scripts/verify_2476_*`, `scripts/verify_2493_*` | ad-hoc | none |

**No scheduled job reads `submissions.zip` or `companyfacts.zip`.** Re-verified on this branch.

⚠ *No scheduled consumer* ≠ *no consumer*: the runbook and the ad-hoc scripts are real consumers.
What the measurement rules out is **steady-state** value, not **replay** value. And the right-hand
column is the finding that decides §5: **no consumer verifies archive identity at consume time.**

### 2.3 Inventory

`resolve_data_dir()/sec/bulk`: 232 files, 10,520,061,139 B. 110 `.zip`, 120 `.sha256`, 2 `.etag`,
0 `.partial`, 0 files with a bare `.tmp` suffix.

- **10 orphan `.sha256`** (9 × `fsds_2023q2…2025q2`, 1 × `form13f_01mar2025-31may2025`) — every
  eviction path unlinks the `.zip` only.
- **2 `.etag` sidecars for 110 ZIPs.** Only the two nightly archives carry one, so only they can
  take the ETag-keyed reuse path at all. ⚠ This is consistent with #3112's measurement that the
  quarterly dataset URLs probed there returned no `ETag`, but 2 sidecars and 5 refresh runs are
  **not** a full-population census of SEC's quarterly URLs; "SEC never serves an ETag on a
  quarterly dataset" is not established and is not assumed anywhere below.
- ⚠ The `0 .tmp` count is a **suffix census and misses PID-suffixed sidecar temporaries**
  (`_SIDECAR_TMP_TAG = f".tmp.{os.getpid()}"`, so real debris is `<name>.zip.sha256.tmp.<pid>`).
  §4.3 removes that class at eviction; the census figure above should be read as "no bare-`.tmp`
  debris", nothing wider.

⚠ An orphan `.sha256` is **hygiene, not a correctness bug**:
`sec_bulk_download._preflight_archive_reuse_decision` tests `zip_path.exists()` before reading any
sidecar and returns `local_missing`, so a stale sidecar cannot cause a false reuse. Checked before
writing it up.

## 3. The contract

> An archive in `sec/bulk` is a **working input fetched for a declared consumer**, not a mirror
> kept in sync with SEC.

1. **A fetch call owns only the archives it names.** `archives=` means "fetch these". It does not
   license deleting anything else in the directory.
2. **Identity is proved at consume time, never inferred from structure or from currency.** The
   evidence is a content digest against a sidecar written by the code that fetched those exact
   bytes. A structural check (size, ZIP round-trip) may decline, never certify.
3. **Evict the artefact set, not the `.zip`** — `.partial`, `.sha256`, `.etag` and the PID-suffixed
   sidecar temporaries go with it, through one helper.

Rules 1 and 3 are implemented in §4. Rule 2 is implemented for the one path that violated it
(§4.4); making it hold for the *consumers* in §2.2's right-hand column is stage 2, and §5 explains
why that ordering is not optional.

## 4. Stage 1 — implemented in this PR

### 4.1 ⛔ A filtered `download_bulk_archives` call deletes every other archive in the directory

`_preflight_etag_keyed_reuse` ends with a sweep over `target_dir`: every `.zip`/`.partial` whose
name is not in the inventory it was handed is purged (`sec_bulk_download.py:755-766`), and the
run manifest is unlinked up front (`:711-719`). `archives=` therefore carries **two** contracts at
once — "fetch these" *and* "this is the complete inventory, delete anything else" — and every
caller except the bootstrap stage passes a filtered list.

**This has already fired.** `scripts/backfill_2701_insider_corpus.py`'s own docstring records it:
*"A filtered list of insider archives deleted companyfacts.zip, submissions.zip and 14 fsnds
archives on the first attempt (#2701, 2026-08-14)."* The fix applied then was a docstring warning
on that one caller.

⚠ **`scripts/backfill_fsds_class_shares_history.py` still passes a filtered list of 20 FSDS
archives and carries no such warning.** On today's cache, one `--apply` run deletes the other 90
ZIPs — `submissions.zip`, `companyfacts.zip`, the 74-archive #2701 insider research corpus, 16
`fsnds_*`, 4 `nport_*`, 4 `form13f_*` — about 9.6 GB, and the bootstrap run manifest with them.
That is a direct contradiction of this ticket's own constraint that archives referenced by
research snapshots must remain reproducible.

Fix: `download_bulk_archives(..., prune_strays: bool = False)`, threaded into
`_preflight_etag_keyed_reuse`. Both directory-owning operations — the stray sweep and the manifest
unlink — run only under the flag. The per-archive purge of a *named* archive that failed its reuse
check is unchanged; that one is inside the caller's declared scope.

The only caller that owns the directory is the bootstrap stage (`sec_bulk_download_job`, which
passes no `archives=` and is the sole caller of `write_run_manifest`). It opts in. Every other
caller gets the safe default, so a future caller that forgets the parameter deletes nothing. An
`ast`-based test pins the invariant that exactly one call site prunes and that it passes no
inventory — a docstring is what failed last time.

⚠ **Keeping the manifest is not the same as leaving it alone, and Codex checkpoint 2 caught the
difference.** A filtered inventory can OVERLAP a paused bootstrap's archives. If the remote ETag
has moved, the preflight purges and re-downloads one of them — and the surviving manifest entry
still says "run 99 downloaded these bytes". `assert_archive_belongs_to_run` checks run id, name and
`reuse_reason` and never the content, so the resumed bootstrap would ingest the backfill's bytes as
its own input. Before this PR the same scenario was loud (the manifest was unlinked outright, so
phase C raised "manifest missing"); preserving it naively would have made it silent, which is
strictly worse.

So a filtered preflight drops the entry for each archive it is about to replace
(`_invalidate_manifest_entries`) and keeps every other entry. Ordering is the #3112 rule: the
marker every consumer gates on goes BEFORE the new bytes, so an interruption leaves the archive
uncertified rather than certified-wrong. A *reused* archive keeps its entry — reuse means the bytes
did not change. If the rewrite fails for any reason the manifest is unlinked: one we cannot correct
is worse than none, because "missing" fails closed at the precondition and a stale entry fails open.

⚠ **What the new default REJECTS relative to today**: nothing is deleted that was previously
deleted *by a directory-owning call*; the bootstrap path is byte-for-byte unchanged. What stops
happening is collateral deletion by filtered callers. The cost is that a rolled-off archive (an old
13F window) now survives until a bootstrap run prunes it — it accumulates rather than disappearing,
which is the safe direction and is visible in the §2.3 inventory.

`scripts/backfill_2701_insider_corpus.py` is switched to the filtered `insider` list it already
computes and never used, now that filtering is safe; its docstring warning is replaced by the new
contract.

### 4.2 Eviction removes the artefact set — at all three sites

Three paths delete an ingested archive by unlinking the `.zip` alone:

| site | context |
| --- | --- |
| `sec_bulk_orchestrator_jobs._delete_archive_after_success` | phase-C ingesters |
| `scheduler._cleanup_submissions_zip_after_drain` | S16 first-install drain; its own docstring says it "mirrors `_delete_archive_after_success`" |
| `scripts/backfill_fsds_class_shares_history.py:119` | the default delete-after-ingest |

All three delegate to the existing `sec_bulk_download._purge_archive_artifacts`, which the download
preflight already uses. One rule, one expression — fixing only the first would leave two copies
free to drift, which is how the pair in #3110 drifted.

The 10 orphan sidecars already on disk are **not** removed by this PR (~0 bytes, and removing them
is a deletion this ticket has not authorised). The fix stops new ones.

### 4.3 `_purge_archive_artifacts` also removes PID-suffixed sidecar temporaries

`_atomic_write_sidecar` writes `<sidecar>.tmp.<pid>` then renames. A crash between the two leaves
debris that no purge path removes and that a bare-`.tmp` census does not see. The helper now globs
`<name>.sha256.tmp.*` and `<name>.etag.tmp.*` alongside the four fixed paths.

### 4.4 A ZIP round-trip may not certify an archive as downloaded-in-run

`_download_one` short-circuits on `final_path.exists() and _zip_round_trip(final_path)` and returns
`reuse_reason="downloaded_in_run"` — stamping bytes it did not fetch with the manifest provenance
that `assert_archive_belongs_to_run` accepts. A ZIP central-directory round-trip is a structural
check; the #3112 prevention entry (merged 2026-09-16, one day before this spec) states the rule
verbatim: *a cheap check may DECLINE to certify, never certify.*

Reachable whenever the preflight's purge fails — `_purge_archive_artifacts` logs and swallows
`OSError`, so a permission glitch leaves the stale `.zip` in place and this branch then certifies
it. The branch now additionally requires the `.zip.sha256` sidecar to match the file's actual
digest; without that it falls through to a normal download. Incidental to §4.1 but the same class
and the same file.

### 4.5 Not changed

- **Both refresh cadences stay exactly as they are** — see §5.
- `sec_quarterly_datasets_bulk_refresh` stays scheduled and untouched. It is inert rather than
  wasteful (5 runs, 0 recorded bytes), so it costs 36 HEADs a month; whether its no-ETag condition
  is permanent is not established (§2.3) and retiring it is a separate scope decision.
- No archive is deleted by this PR. No retention rule changes.

## 5. ⛔ The cadence change is blocked, and the blocker is the point

The #3113 evidence comment's first recommendation — stop proactively refreshing the two archives
with no scheduled consumer, fetch on demand — was drafted as this spec's stage 1. Two independent
findings killed it.

**(a) Demand-fetch is only safe if consumers check freshness, and none do.** §2.2's right-hand
column: the standalone ingest jobs gate on `path.exists()`, the runbook takes an operator-supplied
path, the verify scripts read whatever is cached, and the bootstrap's own resume path accepts a
prior run manifest after an arbitrary pause without re-verifying ETag or digest. Today the daily
refresh is, in effect, what keeps those unchecked reads roughly current. Removing it converts a
currently-harmless gap into a live one: the same code reads bytes that are now months old and says
nothing. ⚠ It is *not* true that the archive is guaranteed ≤24 h old today either — missed fires
and retain-on-failure already allow arbitrary staleness — but the change would widen that from an
edge case to the normal case.

**(b) Removing a job from `SCHEDULED_JOBS` removes it from the job substrate, not just from the
clock.** `scheduled_adapter.get_row()` resolves through `SCHEDULED_JOBS`, so both `/processes`
detail endpoints 404 and the job leaves the process list — the opposite of deliverable 6.
`_tracked_job` falls back to an **unbounded** statement timeout for a name absent from the
registry. `jobs_retry_sweeper` only re-enqueues scheduled jobs, so transient failures stop being
retried. And the `prerequisite=_bootstrap_complete` gate is a `ScheduledJob` field that manual
dispatch does not apply. The in-repo precedent (`sec_manifest_tombstone_stale`, #1614) was a job
that was *drained* — no work, no retries needed — which is not this case.

So the ordering is: **a consume-time freshness contract first, cadence second.** That is stage 2,
and it is the same artefact deliverables 1 and 6 already asked for.

### 5.1 ⛔ "Give `sec_fsnds_notes_ingest_job` a `_record_archive_result` call" — withdrawn

The evidence comment recommended this as prerequisite to any byte budget. It does not work as
stated. `bootstrap_archive_results.bootstrap_run_id` is `BIGINT NOT NULL REFERENCES
bootstrap_runs(id)` (`sql/130_bootstrap_archive_results.sql:20`) and every existing call site is
guarded by `if run_id is not None`, from `_current_running_bootstrap_run_id()`.
`sec_fsnds_notes_ingest_job` is documented operator-invokable and is not a bootstrap stage, so at a
typical invocation there is no running bootstrap run and the call records nothing. ⚠ The converse
is also true and is worse: that helper reads **global** state, not the invocation's own lineage, so
an fsnds run that happened to overlap an unrelated bootstrap would record itself against that run.

The table is a **per-bootstrap-run freshness proof** — `sql/130`'s comment: *"The row's existence
proves 'the stage ran in the current bootstrap run'"* — not a general consumption ledger. Adding a
writer that either never fires or attributes to the wrong run is the defect class #3111 hit three
times.

### 5.2 Stage 2 — consume-time identity, and a ledger keyed on content

Deliverable 1 asks for "downloaded version/hash, consumed version/hash, last use"; deliverable 6
asks to surface "fetched-versus-consumed versions". Neither is answerable today:
`bootstrap_archive_results` records an archive **name**, never a digest, so it cannot say whether
the ZIP on disk is the one that was consumed. Same identity-vs-proxy shape as #3112.

Two halves, in this order:

1. **A consume-time check** every archive reader passes through, returning the archive's digest and
   its age, so a reader either verifies or is on record as not having verified. This is what
   unblocks §5's cadence change.
2. **A ledger** that is not FK-bound to `bootstrap_runs`. ⚠ The key cannot be
   `(archive_name, sha256)` with a single stage field, as the evidence comment's recommendation 4
   implied — one archive is consumed by several stages, repeatedly, under different parser
   versions. Grain is `(archive_name, sha256, stage_key, run_identity)`. ⚠ And consumption alone
   proves neither "unreferenced" nor "safe to evict": an active reader and a pinned replay
   requirement both have to be representable before any eviction decision reads from it.

⚠ **Replay and currency are different contracts and stage 2 must carry both.** A pinned historical
replay (`scripts/verify_2493_pead_feasibility.py` is explicit about exact historical reproduction)
wants the digest it was pinned to, and "must match live SEC" would invalidate it. A current-data
backfill wants the opposite. One flag, declared by the caller, not inferred.

No byte budget can be set before stage 2 exists: 40 % of the cache (`fsnds_*`, 4.206 GB) has no
consumption record of any kind.

### 5.3 Stage 3 — the ETL UI (deliverable 6)

Must read `bootstrap_archive_results` plus stage 2's ledger, **not** `job_runs`, or it reports five
consumers as never having run.

## 6. Acceptance for stage 1

1. A filtered `download_bulk_archives` call leaves unrelated archives and the run manifest intact;
   a `prune_strays=True` call still removes both. Pinned by tests over a tmp dir.
1a. A filtered call that REPLACES an archive named in the manifest drops that entry and keeps every
   other one, so the resumed bootstrap raises rather than accepting foreign bytes; a reused archive
   keeps its entry. Pinned by two tests.
2. Each of the three eviction sites removes `.zip`, `.partial`, `.sha256`, `.etag` and
   `<sidecar>.tmp.<pid>`. Pinned by a test per site.
3. `_download_one` does not return `reuse_reason="downloaded_in_run"` for an existing ZIP whose
   `.sha256` sidecar is missing or mismatched. Pinned by a test.
4. `uv run pytest -m "not db" tests/test_sec_bulk_download.py tests/test_sec_bulk_refresh.py
   tests/test_job_registry.py` green; full fast tier + smoke green at the pre-push gate.

No migration, no backfill, no parser change, no scheduler-registry change: nothing stored changes
shape and no job's cadence or registration moves, so Definition-of-Done clauses 8-12 (smoke panel /
cross-source / `sec_rebuild`) do not apply. Dev-verify is the jobs-daemon respawn plus confirming
the two refresh jobs still appear and still fire on their existing schedule.
