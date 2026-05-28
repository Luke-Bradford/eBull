# #1277 — S16 first-install drain: local-zip path for non-issuer primary pages

**Status**: draft 1.1 · 2026-05-28 · Phase 2 of [`bootstrap-sub-1h-plan.md`](./bootstrap-sub-1h-plan.md) §7.

**Parent**: master plan v5.2 §7 Phase 2 row 2 (S16 local-zip parse, folds into #1337 P2 already partial via #1366).

**Changelog**:
- v1.0 — 2026-05-28 — initial draft.
- v1.1 — 2026-05-28 — Codex 1 fold: 3 BLOCKING + 3 IMPORTANT + 2 NIT.
  - BLOCKING-1 (archive deleted by S8): move `_delete_archive_after_success(archive)` out of S8 for `submissions.zip` specifically; defer deletion to S16's exit so the drain still sees the archive. Other bulk archives (companyfacts, etc.) keep existing post-ingest deletion.
  - BLOCKING-2 (secondary pages not in bulk archive — confirmed by `app/services/sec_submissions_files_walk.py:16-23`): zip path covers PRIMARY `CIK<10>.json` only. Secondary `CIK<10>-submissions-NNN.json` pages stay on HTTP via a **hybrid** `HttpGet` (primary → zip, secondary → real HTTP fallback).
  - BLOCKING-3 (full-history regression): hybrid wrapper resolves; T4 reshaped to assert primary-routed-to-zip AND secondary-routed-to-HTTP.
  - IMPORTANT-1 (archive provenance): add `assert_archive_belongs_to_run(target_dir, "submissions.zip", bootstrap_run_id=run_id)` before zip-walk. Mismatch → log + fall back to HTTP.
  - IMPORTANT-2 (strict bool): coerce `use_bulk_zip` via `isinstance(val, bool)`; non-bool → log + treat False.
  - IMPORTANT-3 (real-archive test shape): drop synthetic-secondary fixture; assert primary-only zip namelist; cite SEC docs.
  - NIT-1: `_make_zip_http_get(archive_path, *, fallback_http_get) -> tuple[HttpGet, zipfile.ZipFile]`; ZipFile opened immediately, returned to caller for explicit close via `try/finally` (cleaner ownership than lazy-open hidden behind callable).
  - NIT-2: secondary-primary-fetch redundancy in `_drain_secondary_pages` not in scope here.
- v1.2 — 2026-05-28 — Codex 1 v1.1 re-pass fold: 1 BLOCKING + 2 IMPORTANT + 1 NIT (consistency).
  - BLOCKING-4: `_current_running_bootstrap_run_id()` is private to `sec_bulk_orchestrator_jobs.py`, not a scheduler symbol. Use `resolve_progress_context()` from `app.services.bootstrap_state` (returns `BootstrapProgressContext(run_id, stage_key)` or `None` outside a bootstrap dispatch). Already used by every progress-instrumented stage. Updates §3.3 invoker snippet.
  - IMPORTANT-4 (rollback orphans archive): code-only rollback (flip `use_bulk_zip=False` at orchestrator) leaves `archive_path=None` so the previous v1.1 cleanup did NOT fire. Fix: cleanup is **always unconditional** — scheduler invoker resolves `candidate = target_dir / "submissions.zip"` regardless of `use_bulk_zip`; calls `_cleanup_submissions_zip_after_drain(candidate)` on drain SUCCESS regardless of which path the drain took. Decouples cleanup from zip-path activation. §3.3 + §3.4 + §8 updated.
  - IMPORTANT-5 (T4 incomplete): T4 now asserts BOTH directions — primary `CIK<10>.json` URL routed to zip (200 from zip bytes; HTTP transport NOT called) AND secondary `CIK<10>-submissions-NNN.json` URL routed to `fallback_http_get` (HTTP transport called with that URL).
  - NIT-3 (contract consistency): v1.1 changelog claimed lazy open + `HttpGet` return; design code already showed immediate open + `(HttpGet, ZipFile)` tuple. Tuple shape kept; changelog NIT-1 text fixed to match.
  - NIT-4 (§2 wording lag): §2 in scope still described the lazy-open + `HttpGet`-only contract — updated to match §3.1's tuple/immediate-open shape.

**Pre-#1366 baseline**: S16 ~85 min observed Run #8.
**Post-#1366 baseline**: S16 ~17 min (issuer cohort short-circuits to `filing_events` fast-path; institutional + blockholder cohorts still HTTP-walk).
**Phase 2 target (this PR)**: S16 < 5 min. Achieved by eliminating non-issuer **primary** `submissions.json` HTTP fetches (~11k requests at 10 req/s shared = ~18 min budget) and reading them from the local `submissions.zip` that S7 already landed. Non-issuer **secondary** pages (~1-2k requests for filers with >1000 filings) still walk HTTP at sec_rate — these are not in the bulk archive per `app/services/sec_submissions_files_walk.py:16-23`. Net: ~17 min → ~3 min projected.

---

## 1. Problem

`app/workers/scheduler.py:4801` hardcodes `use_bulk_zip=False`. The drain's own bulk-zip branch at `app/jobs/sec_first_install_drain.py:290` raises `NotImplementedError`. The flag is structurally inert today.

Post-#1366 state:

- S7 `sec_bulk_download` (lane `sec_bulk_download`) lands `submissions.zip` (~1.54 GB).
- S8 `sec_submissions_ingest_job` (`app/services/sec_bulk_orchestrator_jobs.py:162`) ingests the zip into `filing_events` + `sec_cik_submissions_files_index` sidecar, **then calls `_delete_archive_after_success(archive)` at line 222 — the zip is gone by S16 today**.
- S14 `sec_submissions_files_walk` (sec_rate lane) walks sidecar page names + fetches each secondary page over HTTP. Issuer-only by sidecar shape.
- S16 `sec_first_install_drain` waits on `submissions_processed` (#1366), runs `seed_manifest_from_filing_events` (~15 s issuer fast-path, no HTTP), then per-CIK HTTP-walks institutional + blockholder filers (~17 min observed = primary `CIK<10>.json` for ~11k filers + secondary `CIK<10>-submissions-NNN.json` for ~5-10% of them).

The local `submissions.zip` contains **only** primary `CIK<10>.json` entries — secondary pages are NOT in the archive (per the canonical comment block in `app/services/sec_submissions_files_walk.py:16-23` + reflected by the sidecar-based design: the bulk path writes a `sec_cik_submissions_files_index` row per CIK with sidecar **page names**, never their bytes). Reading those primaries locally eliminates ~11k of the ~12k HTTP requests S16 incurs.

## 2. Scope

In:

1. **Archive lifecycle change** (`app/services/sec_bulk_orchestrator_jobs.py:162-222`): remove `_delete_archive_after_success(archive)` from the end of `sec_submissions_ingest_job` **for `submissions.zip` only**. The other bulk archives (`companyfacts.zip`, etc.) keep their existing post-ingest deletion via the other call sites at `sec_bulk_orchestrator_jobs.py:290, 501, 661, 849`. A new helper `_cleanup_submissions_zip_after_drain()` is called at the end of `sec_first_install_drain` to delete it post-drain.
2. New helper `_make_zip_http_get(archive_path, *, fallback_http_get) -> tuple[HttpGet, zipfile.ZipFile]` in `app/jobs/sec_first_install_drain.py`. **Hybrid** routing:
   - URL matches `https://data.sec.gov/submissions/CIK<10digits>.json` (primary) → read from zip; (200, bytes) on hit, (404, b"") on miss.
   - URL matches `https://data.sec.gov/submissions/CIK<10digits>-submissions-<NNN>.json` (secondary) OR any other pattern → delegate to `fallback_http_get`.
   - `ZipFile` opened immediately + returned to caller as the second tuple element so the drain owns close via `try/finally` (no hidden lifecycle behind the callable).
3. Drain branch (`app/jobs/sec_first_install_drain.py:273+`): add `archive_path: Path | None = None` param. When `use_bulk_zip=True` AND archive present + provenance-verified, wrap `http_get` with the hybrid above for the per-CIK loop. The fast-path issuer short-circuit is unchanged.
4. Scheduler invoker `sec_first_install_drain` (`app/workers/scheduler.py:4771`): read `use_bulk_zip` param (strict bool, default False). Resolve `target_dir = resolve_data_dir() / "sec" / "bulk"`, then `archive_path = target_dir / "submissions.zip"` (no need to promote the private `_resolve_archive_path` — single-name join). If `not archive_path.exists()` OR `assert_archive_belongs_to_run` fails for the current bootstrap run → log warn + downgrade to HTTP.
5. Bootstrap dispatch: `_spec("sec_first_install_drain", ..., params={"max_subjects": None, "use_bulk_zip": True})` at `app/services/bootstrap_orchestrator.py:1117`.
6. `JOB_INTERNAL_KEYS["sec_first_install_drain"]` extended to `frozenset({"max_subjects", "use_bulk_zip"})` at `app/services/processes/param_metadata.py:180`. Manual API path keeps rejecting `use_bulk_zip`.
7. Tests covering: hybrid `HttpGet` routing (primary→zip, secondary→fallback, unknown→fallback); drain with archive present routes per-CIK loop through zip for primaries; drain with archive present routes secondary pages through `http_get` even when zip path is active; drain with missing archive falls back to HTTP; drain with mismatched provenance falls back to HTTP; bootstrap dispatch sets `use_bulk_zip=True`; operator API rejects `use_bulk_zip`; S8 no longer deletes `submissions.zip`; S16 cleanup deletes `submissions.zip` after drain.

Out:

- No schema change.
- No frontend change.
- No new lane / cap.
- Scheduled-cron operator-trigger keeps `use_bulk_zip=False` default (no archive freshness guarantee outside bootstrap window; daily-refresh PR #1286 freshness opt-in is a separate ticket).
- Issuer cohort secondary-page coverage is unaffected — S14 sec_submissions_files_walk owns that path. S16's `_drain_secondary_pages` is only relevant to the non-issuer cohort (issuers short-circuit before it). Secondary pages for non-issuers continue via HTTP (the sidecar in `sec_cik_submissions_files_index` is issuer-scoped — `repair_cik_sidecar_from_archive` filters by `_load_cik_to_instrument` per `app/services/sec_submissions_ingest.py:474`).
- Refactor of `_drain_secondary_pages` redundant primary fetch (NIT-2): tracked separately.
- Perf-bench artifact: deferred. Wall-clock claim verified via Phase 0.5 R3 measurement run (operator-driven, post-merge).

## 3. Design

### 3.1 Hybrid `_make_zip_http_get`

```python
import re
import zipfile
from pathlib import Path

from app.providers.implementations.sec_submissions import HttpGet

_PRIMARY_URL_RE = re.compile(r"^https://data\.sec\.gov/submissions/CIK(\d{10})\.json$")

def _make_zip_http_get(
    archive_path: Path,
    *,
    fallback_http_get: HttpGet,
) -> tuple[HttpGet, zipfile.ZipFile]:
    """Return (hybrid HttpGet, open ZipFile) for caller-managed lifecycle.

    Routes:
      * Primary submissions URL ``data.sec.gov/submissions/CIK<10>.json``
        → read from ``archive_path``; (200, bytes) on hit, (404, b"")
        on miss (CIK not in archive — caller treats as ``not_found``).
      * Anything else (secondary pages, other paths) → ``fallback_http_get``.

    The bulk ``submissions.zip`` contains ONLY primary ``CIK<10>.json``
    entries. Secondary ``CIK<10>-submissions-<NNN>.json`` pages are
    NOT in the archive (per ``app/services/sec_submissions_files_walk.py``
    canonical reference). The hybrid wrapper preserves correctness by
    routing secondaries to the real transport.

    Caller is responsible for closing the returned ZipFile via
    ``try/finally``.
    """
    zf = zipfile.ZipFile(archive_path)

    def _get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        match = _PRIMARY_URL_RE.match(url)
        if match is None:
            return fallback_http_get(url, headers)
        entry_name = f"CIK{match.group(1)}.json"
        try:
            with zf.open(entry_name) as fh:
                return (200, fh.read())
        except KeyError:
            return (404, b"")

    return _get, zf
```

Returning `(callable, ZipFile)` lets the caller (drain) own the close. No magic context-managers crossing function boundaries.

### 3.2 Drain branch

`run_first_install_drain(conn, *, http_get, use_bulk_zip=False, follow_pagination=True, max_subjects=None, archive_path=None)`:

```python
# replace the existing NotImplementedError branch at L290 with:
zip_handle: zipfile.ZipFile | None = None
effective_http_get = http_get
if use_bulk_zip and archive_path is not None and archive_path.exists():
    effective_http_get, zip_handle = _make_zip_http_get(
        archive_path, fallback_http_get=http_get
    )

try:
    # existing per-CIK loop runs with effective_http_get bound where
    # check_freshness + _drain_secondary_pages currently use http_get.
    # (Only the local binding swaps — function bodies unchanged.)
    ...
finally:
    if zip_handle is not None:
        zip_handle.close()
```

`check_freshness` consumes `effective_http_get` for the primary `CIK<10>.json` (routed to zip). `_drain_secondary_pages` consumes `effective_http_get` for secondary `CIK<10>-submissions-NNN.json` URLs (routed back through `fallback_http_get` by the regex predicate). The existing `if status != 200: continue` guards both call sites cleanly when the zip happens to miss a CIK (404 → drain skips it; no semantic change).

### 3.3 Scheduler invoker

```python
def sec_first_install_drain(params: Mapping[str, Any]) -> None:
    ...
    max_subjects_param = params.get("max_subjects")
    max_subjects = int(max_subjects_param) if max_subjects_param is not None else None

    use_bulk_zip_param = params.get("use_bulk_zip", False)
    # IMPORTANT-2 fold: strict bool — reject "false" / 0 / None coercion.
    if isinstance(use_bulk_zip_param, bool):
        use_bulk_zip = use_bulk_zip_param
    else:
        logger.warning(
            "sec_first_install_drain: use_bulk_zip must be bool, got %r — treating as False",
            use_bulk_zip_param,
        )
        use_bulk_zip = False

    # Resolve candidate UNCONDITIONALLY so the post-drain cleanup fires
    # regardless of which drain path was taken. This avoids orphaning
    # submissions.zip on the "use_bulk_zip=False" rollback path (IMPORTANT-4
    # fold).
    from app.config import resolve_data_dir
    from app.services.bootstrap_state import resolve_progress_context
    from app.services.sec_bulk_download import assert_archive_belongs_to_run
    target_dir = resolve_data_dir() / "sec" / "bulk"
    candidate = target_dir / "submissions.zip"

    archive_path: Path | None = None
    if use_bulk_zip:
        if not candidate.exists():
            logger.warning(
                "sec_first_install_drain: use_bulk_zip=True but %s missing — HTTP fallback",
                candidate,
            )
            use_bulk_zip = False
        else:
            ctx = resolve_progress_context()
            if ctx is None:
                logger.warning(
                    "sec_first_install_drain: use_bulk_zip=True outside bootstrap dispatch — HTTP fallback"
                )
                use_bulk_zip = False
            else:
                try:
                    assert_archive_belongs_to_run(
                        target_dir, "submissions.zip", bootstrap_run_id=ctx.run_id
                    )
                    archive_path = candidate
                except Exception as exc:  # noqa: BLE001 — downgrade safety
                    logger.warning(
                        "sec_first_install_drain: archive provenance mismatch (%s) — HTTP fallback",
                        exc,
                    )
                    use_bulk_zip = False

    with _tracked_job(JOB_SEC_FIRST_INSTALL_DRAIN) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = run_first_install_drain(
                conn,
                http_get=_make_sec_http_get(sec),
                follow_pagination=True,
                use_bulk_zip=use_bulk_zip,
                archive_path=archive_path,
                max_subjects=max_subjects,
            )
        # Unconditional post-drain disk-hygiene on the SUCCESS path. Runs
        # even when the HTTP fallback path was taken — once S16 has
        # exhausted whatever the drain needed from the zip, no other
        # stage consumes submissions.zip, so it's safe to drop. Idempotent
        # missing-ok unlink — operator-triggered re-runs see no zip and
        # the drain downgrades cleanly. Bypassed when the drain raises
        # (control flow leaves the with-block before reaching here).
        _cleanup_submissions_zip_after_drain(candidate)
        ...
```

`_cleanup_submissions_zip_after_drain` mirrors `_delete_archive_after_success` (try/unlink/log/swallow) — extract or inline. Decide at code-review.

### 3.4 S8 lifecycle change

`app/services/sec_bulk_orchestrator_jobs.py:162` `sec_submissions_ingest_job`:

Remove **only** the L222 `_delete_archive_after_success(archive)` call. The other bulk-archive ingester jobs (`sec_companyfacts_ingest_job` etc. — lines 290, 501, 661, 849) keep theirs. Add a comment block at the removed site explaining the deferral + cross-linking to `sec_first_install_drain`.

Standalone manual S8 invocation (`run_id is None` path at L181) currently still deletes after success — this PR removes the unconditional deletion. Disk-hygiene impact: ~1.54 GB stays on disk per manual `sec_submissions_ingest` invocation until either S16 fires or an operator manually deletes. Acceptable — manual S8 invocations are rare + bootstrap-relative.

Test sentinel: `tests/test_sec_bulk_disk_hygiene.py` already covers `_delete_archive_after_success` semantics. Extend it with `test_sec_submissions_ingest_does_not_delete_submissions_zip_on_run_path` + `test_sec_first_install_drain_deletes_submissions_zip_after_success`.

### 3.5 Bootstrap dispatch

```python
_spec(
    "sec_first_install_drain",
    16,
    "sec_rate",
    JOB_SEC_FIRST_INSTALL_DRAIN,
    params={"max_subjects": None, "use_bulk_zip": True},
),
```

`CapRequirement` already pinned at `bootstrap_orchestrator.py:575` (`("cik_mapping_ready", "submissions_processed")`). `submissions_processed` is provided by S8 on SUCCESS (bulk path → zip on disk after this PR's deferral) AND on SKIP (cascade-skip / slow-connection fallback → no zip). The drain's downgrade chain (§3.3) handles both: present + provenanced → zip; absent OR mismatched → HTTP.

### 3.6 Operator API rejection

`JOB_INTERNAL_KEYS["sec_first_install_drain"]` extends `{"max_subjects"}` → `{"max_subjects", "use_bulk_zip"}`. `validate_job_params(allow_internal_keys=False)` rejects unlisted keys for the manual API + cron path. Regression sentinel test in `tests/test_job_registry.py` (existing JOB_INTERNAL_KEYS test surface).

## 4. Tests

| ID | Test | Where |
|---|---|---|
| T1a | `_make_zip_http_get` returns `(200, payload)` for primary CIK URL hit | `tests/jobs/test_sec_first_install_drain.py` |
| T1b | `_make_zip_http_get` returns `(404, b"")` for primary CIK URL miss | T1 ditto |
| T1c | `_make_zip_http_get` delegates secondary `-submissions-001.json` URL to `fallback_http_get` (asserts mock fallback called with the URL; zip not consulted) | T1 ditto |
| T1d | `_make_zip_http_get` delegates non-submissions URL to fallback | T1 ditto |
| T2 | Drain with `use_bulk_zip=True` + archive present + non-issuer cohort: primary HTTP transport sees ZERO `CIK<10>.json` calls; manifest rows upserted from zip | T2 ditto |
| T3 | Drain with `use_bulk_zip=True` + archive absent: warns + uses provided `http_get` for everything | T3 ditto |
| T4 | Drain with `use_bulk_zip=True` + secondary page expected (`has_more_in_files=True`): **primary** `CIK<10>.json` URL served from zip bytes (HTTP transport NOT called with that URL) AND **secondary** `CIK<10>-submissions-NNN.json` URL routed through `fallback_http_get` (HTTP transport called with the secondary URL). Verifies both halves of the hybrid contract in one fixture — non-regression of full-history seeding (IMPORTANT-5 fold). | T4 ditto |
| T5 | Drain with `use_bulk_zip=True` + issuer cohort + `filing_events` seeded: still short-circuits issuers (no zip read either; no HTTP call) | T5 ditto |
| T6 | Scheduler invoker passes `use_bulk_zip=True` + resolves archive path + provenance check fires | `tests/workers/test_scheduler_sec_first_install_drain.py` (new or extension) |
| T6b | Scheduler invoker downgrades to HTTP when `assert_archive_belongs_to_run` raises | T6 ditto |
| T6c | Scheduler invoker treats non-bool `use_bulk_zip` param as False + logs warn | T6 ditto |
| T7 | Bootstrap StageSpec for S16 has `use_bulk_zip=True` (regression sentinel sibling to #1366's `test_sec_first_install_drain_requires_submissions_processed`) | `tests/services/test_bootstrap_orchestrator.py` |
| T8 | `validate_job_params(allow_internal_keys=False)` rejects `use_bulk_zip` | `tests/test_job_registry.py` |
| T9 | `validate_job_params(allow_internal_keys=True)` accepts `use_bulk_zip` for `sec_first_install_drain` | T8 ditto |
| T10 | S8 `sec_submissions_ingest_job` no longer calls `_delete_archive_after_success(submissions.zip)` on the run path | `tests/test_sec_bulk_disk_hygiene.py` |
| T11 | S16 `sec_first_install_drain` deletes `submissions.zip` after successful zip-path drain (`use_bulk_zip=True` + archive present) | T10 ditto OR T1-T5 ditto |
| T11b | S16 `sec_first_install_drain` ALSO deletes `submissions.zip` after successful HTTP-fallback drain (`use_bulk_zip=False` AND candidate file present on disk). Locks the IMPORTANT-4 invariant — code-only rollback path stays disk-clean. | T11 ditto |
| T12 | S16 `sec_first_install_drain` does NOT delete `submissions.zip` when drain raises | T11 ditto |

Fixture: in-memory `submissions.zip` via `zipfile.ZipFile(BytesIO, "w")`. Contents: 2-3 entries shaped `CIK<10>.json` only — **NO synthetic secondary pages**, matching the real archive layout (per `app/services/sec_submissions_files_walk.py:16-23`).

## 5. Performance verification

Per [`.claude/skills/engineering/etl-perf-claims.md`](../../../.claude/skills/engineering/etl-perf-claims.md):

- **No `var/perf_baselines/1277-<sha>.*` artifact this PR.** Wall-clock claim ("17 min → ~3 min") cannot be reproduced in `EBULL_BENCH_DB_URL` because the savings come from network-IO elimination, not SQL plan shape. The §4 protocol's EXPLAIN/SQL-medians fixtures don't model this workload.
- **PR body omits the `perf` label + `## Performance impact` header**, so `perf-claim-lint` exits 0 (no claim detected). PR body still cites the claim under a different header so it shows in the description, just outside the lint trigger.
- **Wall-clock verification deferred to Phase 0.5 R3 measurement run** (operator-driven, master plan §7 Phase 0.5). PR body records this trade-off explicitly under "Performance verification deferred to R3".
- **Structural sentinel**: `test_use_bulk_zip_zero_primary_http_calls_for_non_issuers` asserts the primary HTTP transport receives ZERO calls when the zip path fires. A future revert that re-routed primaries through HTTP would have to defeat this test.

## 6. Engineering discipline cross-references

- [`.claude/skills/data-engineer/SKILL.md`](../../../.claude/skills/data-engineer/SKILL.md): no schema change, no observation-table write, no rollup endpoint change → CLAUDE.md §"ETL clauses 8-12" do not apply (this PR seeds the **manifest**; downstream parsers + observations are unchanged).
- [`.claude/skills/data-sources/sec-edgar.md`](../../../.claude/skills/data-sources/sec-edgar.md): submissions.zip is the canonical bulk archive at `/Archives/edgar/daily-index/bulkdata/submissions.zip`. Per the inline comment block in `app/services/sec_submissions_files_walk.py:16-23` (the canonical project reference): contents = primary `CIK<10>.json` only; secondary `CIK<10>-submissions-NNN.json` pages are NOT in the archive and remain HTTP-fetched. This PR is consistent with that invariant.
- [`docs/review-prevention-log.md`](../../review-prevention-log.md): `run_first_install_drain` opens its own connection — the prevention-log entry on commit-ownership is preserved. The S8-deletion-move does not change connection ownership in either function.

## 7. Risks + mitigations

| Risk | Mitigation |
|---|---|
| Archive absent at S16 dispatch despite `submissions_processed` cap | Invoker checks `candidate.exists()` + downgrades + warns + runs HTTP fallback. T3 covers. |
| Wrong-run archive on disk (stale from a previous bootstrap) | `assert_archive_belongs_to_run` provenance check before zip-walk. T6b covers. |
| Secondary page name not present in zip namelist (always true today) | Hybrid wrapper routes secondary URLs to `fallback_http_get` via regex predicate. T1c + T4 cover. |
| Archive deletion deferral causes disk pressure | ~1.54 GB extra disk for the duration between S8 and S16 SUCCESS. Bootstrap-window-only. Bench target machines have GB-scale free disk; smoke against `check_disk_space` at S7 stays accurate. |
| Manual S8 run no longer cleans up archive | Documented in S8 docstring; operator follow-up = manual rm. Acceptable — manual S8 is rare. |
| Drain crash mid-loop leaves archive undeleted | Intentional. Operator-visible state for triage; next bootstrap pre-flight re-downloads via the existing `_purge_archive_artifacts` path. |
| ZipFile open across the per-CIK loop holds one file handle | Acceptable. Closed via `try/finally`. |
| Operator manual-triggers `sec_first_install_drain` with `use_bulk_zip=True` against stale on-disk archive | Manual API rejects the key via `JOB_INTERNAL_KEYS`. T8 regression sentinel. |

## 8. Rollback

Layered:

1. **Code-only rollback (preserves S8 lifecycle change)**: flip `params={"use_bulk_zip": False}` at `bootstrap_orchestrator.py:1117`. S16 reverts to HTTP per-CIK loop. **S16's unconditional cleanup (§3.3) still deletes `submissions.zip` post-drain**, so the S8 deferral does NOT orphan the archive on this rollback path — disk hygiene preserved end-to-end (IMPORTANT-4 fold).
2. **Full rollback (restore S8 deletion)**: revert the S8 change too — `_delete_archive_after_success(archive)` returns at L222. Then this PR is fully undone.

Both rollbacks single-file in scope.

## 9. Out-of-scope follow-ups

1. **Daily-cron operator-trigger using bulk zip** — gated on freshness telemetry from PR #1286.
2. **#1337 P2 fold** — confirm at #1337 implementation time whether #1277 + #1366 + #1337 P2 collapse into a single dispatch path or stay separate. Closes #1277 standalone.
3. **`_drain_secondary_pages` redundant primary fetch** — NIT-2 from Codex 1 review; refactor separately.
4. **Perf-bench harness chicken-egg fix** — PR #1371 known-issue.
5. **`sec_cik_submissions_files_index` for non-issuer filers** — if S14-like sidecar coverage were extended to institutional + blockholder filers, S16 could drop secondary HTTP too. Out of scope (touches sidecar schema + S14 ingest semantics).

## 10. Acceptance

1. All four pre-push gates pass (`ruff check`, `ruff format --check`, `pyright`, `pytest`).
2. T1a-T12 pass.
3. Codex 1 v1.1 re-review APPROVE or fold further BLOCKING.
4. Codex 2 reviews the diff pre-push; BLOCKING items folded.
5. Bot review iter-1 APPROVE or rebuttal-only round with Codex 3 sign-off.
6. Operator R3 measurement run records S16 wall-clock < 5 min on a clean-DB bootstrap (post-merge; reported back via Phase 0.5 measurement memo).
