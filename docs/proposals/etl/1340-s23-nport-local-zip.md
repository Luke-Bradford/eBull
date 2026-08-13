# #1340 — S23 NPORT trust ingest: local-zip enumeration + bulk-seeded skip

Status: PROPOSAL (Codex 1 pending)
Phase: bootstrap-sub-1h master plan v5.2 §7 Phase 2 row 4
Sibling precedent: #1277 (S16 local-zip), #1341 (S14 pipelined)

## 1. Problem

Bootstrap stage **S23 `sec_n_port_ingest`** (HTTP-walks `sec_nport_filer_directory`
trust cohort, ingests pending NPORT-P / NPORT-P/A into
`ownership_funds_observations`) is a long pole. Acceptance: S23 < 10 min.

The dominant cost is **one per-trust-CIK HTTP fetch of the primary
`https://data.sec.gov/submissions/CIK<10>.json`** to enumerate accessions,
fired *before* any skip check:

- `app/services/n_port_ingest.py:732` — `submissions_payload = sec.fetch_document_text(_submissions_url(cik))`
- skip-vs-`n_port_ingest_log` happens *after*, at `n_port_ingest.py:745-749`.

Cohort ≈ 3-4k active trusts (post-#1010 `min_last_seen_filed_at` bound). At the
shared `sec_rate` ≈ 7-10 req/s ceiling, ~3.5k enumeration fetches ≈ 6-8 min
*before* any document body is fetched.

Secondary redundancy: **S12 `sec_nport_ingest_from_dataset`** already loaded the
current-quarter NPORT-P holdings from the bulk `nport.zip` into
`ownership_funds_observations` — but it does **not** write `n_port_ingest_log`,
so S23 re-fetches + re-parses those same current-quarter accessions over HTTP.

## 2. Why the original ticket design is retargeted

Issue #1340 prescribed (a) `ManifestSubjectType` enum addition (`nport_trust`),
(b) NPORT-trust manifest extraction from `submissions.zip`, (c) S23 fast-path
`if subject_type=='nport_trust' and _bulk_already_seeded(conn, cik): continue`.

Grep-verified against the code, all three premises fail (same class as #1341):

| Ticket premise | Reality (file:line) |
|---|---|
| Requires `ManifestSubjectType` enum addition | `fund_series` already in the Literal (`sec_manifest.py:137`) + DB CHECK (`sql/118:50`). N-PORT manifest rows already carry `institutional_filer`/`fund_series` (`manifest_parsers/sec_n_port.py:28-30`). No new value needed. |
| S23 fast-path keys on `sec_filing_manifest.subject_type` | S23 never reads the manifest; it skips via `n_port_ingest_log` (`n_port_ingest.py:621-634`). `_bulk_already_seeded` does not exist (grep: 0 matches). |
| `submissions.zip` *manifest* extraction is the upstream half | `sec_submissions_files_walk.py` emits **no** NPORT rows and walks only tradable *issuer* CIKs (`:181 WHERE …is_tradable=TRUE`), never trust CIKs. |

Retarget approved by operator 2026-05-28 → **local-zip enumeration + S12 bulk-seeded
skip** (this spec). Mirrors the established #1277/#1341 local-zip pattern; no
manifest, no enum.

## 3. Design

Two complementary changes + the supporting cap-gate and zip-lifecycle move.

### 3.1 Local-zip submissions enumeration (the dominant win)

S23's per-CIK primary `submissions/CIK<10>.json` reads route to the already-landed
local `submissions.zip` (1.54 GB; contains **every** CIK incl. trusts) instead of
HTTP — identical to #1277's S16 fix, adapted to S23's `SecArchiveFetcher` protocol.

**New shared module `app/services/sec_submissions_zip.py`** — single source of
truth for the primary-URL contract, so S16 and S23 cannot drift:

```python
PRIMARY_SUBMISSIONS_URL_RE = re.compile(r"^https://data\.sec\.gov/submissions/CIK(\d{10})\.json$")

def match_primary_submissions_cik(url: str) -> str | None:
    """Return the zero-padded 10-digit CIK if url is a primary submissions URL, else None."""

def read_zip_entry(zf: zipfile.ZipFile, entry_name: str) -> bytes | None:
    """Return entry bytes, or None on KeyError (member absent). Raises on BadZipFile/OSError."""

class ZipBackedArchiveFetcher:
    """Wrap a SecArchiveFetcher; route primary submissions URLs to a local ZipFile.
    fetch_document_text(url) -> str | None:
      - primary URL, zip HIT → decode utf-8, return str
      - primary URL, zip MISS or any zip/decode error → DELEGATE to wrapped fetcher
      - non-primary URL (NPORT-P doc bodies, secondary pages) → delegate to wrapped fetcher
    Caller owns the open ZipFile lifecycle (try/finally), mirroring _make_zip_http_get."""
```

- **`app/jobs/sec_first_install_drain.py`** changes ONE line: its local
  `_PRIMARY_SUBMISSIONS_URL_RE = re.compile(...)` becomes
  `from app.services.sec_submissions_zip import PRIMARY_SUBMISSIONS_URL_RE`. Body
  of `_make_zip_http_get` otherwise unchanged → #1277's tests still pin S16
  behaviour. (Minimal-risk reuse, no logic refactor of merged code.)
- **Zip is a pure accelerator, never a coverage reducer (Codex 1 BLOCKING-1).**
  Unlike S16's `_make_zip_http_get` (returns `404` on miss because the drain
  records `not_found` via the manifest), S23's `fetch_document_text` contract maps
  `None` → "404/error → **no work for this filer**" (`n_port_ingest.py:732-735`).
  A trust newly present in `sec_nport_filer_directory` but absent/stale in the
  local `submissions.zip` would then be silently dropped. So `ZipBackedArchiveFetcher`
  **delegates to the wrapped HTTP fetcher on ANY non-hit**: `KeyError` (member
  absent), `UnicodeDecodeError` (bad bytes), `BadZipFile`/`OSError` (corrupt/
  truncated member). Only a clean zip HIT short-circuits HTTP. `read_zip_entry`
  returns `None` on `KeyError` and *raises* on `BadZipFile`/`OSError`; the fetcher
  catches both → delegate. utf-8 decode failure → delegate.
- Net: the common case (trust in zip) does zero enumeration HTTP; the rare
  miss/stale case falls through to one real HTTP fetch — preserving coverage.

### 3.2 S23 invoker wiring (`scheduler.py::sec_n_port_ingest`, :5441)

Mirror S16's invoker (`scheduler.py:4804-4881`) exactly:

- New bootstrap-only param `use_bulk_zip` (bool, strict — non-bool → warn + False,
  per #1277 IMPORTANT-2). Added to `JOB_INTERNAL_KEYS["sec_n_port_ingest"]`
  (`param_metadata.py:159`) so the operator API path rejects it.
- Resolve `candidate = resolve_data_dir()/"sec"/"bulk"/"submissions.zip"`
  unconditionally (so cleanup fires on every path).
- If `use_bulk_zip` AND `candidate.exists()` AND `resolve_progress_context() is not None`
  AND `assert_archive_belongs_to_run(target_dir, "submissions.zip", bootstrap_run_id=ctx.run_id)`
  passes → open zip, wrap `sec` in `ZipBackedArchiveFetcher`. Any failure → HTTP
  fallback (downgrade-safe, log a warning), `use_bulk_zip=False`.
- The `zipfile.ZipFile(archive_path)` open is **guarded** (Codex 2 BLOCKING):
  a corrupt/truncated archive that passed `exists()` + provenance raises
  `BadZipFile`/`OSError` → log + fall back to the raw HTTP fetcher (`zip_handle=None`,
  `fetcher=sec`). The zip is a pure accelerator; an open failure never fails S23.
- `try/finally`: close the ZipFile; then `_cleanup_submissions_zip_after_drain(candidate)`
  on the SUCCESS path, **gated to bootstrap dispatch only** (see §3.4).
- Bootstrap stage spec (`bootstrap_orchestrator.py:1180`) gains
  `"use_bulk_zip": True` alongside the existing `min_last_seen_filed_at` sentinel.

### 3.3 S12 writes `n_port_ingest_log` (skip current-quarter doc re-fetch)

`sec_nport_dataset_ingest.py` — after the staging drain + series-upsert loop
(`:674-689`), **still inside the orchestrator's per-archive transaction, before
commit, and before any `series_upsert_buffer` mutation/clear** (Codex 1 finding 3),
write one `n_port_ingest_log` row per distinct accession it loaded:

- Source the `(accession, filer_cik, period_end, series_id)` tuples from the
  existing `series_upsert_buffer` (already collected at `:598-606`), deduped to
  distinct accession. The buffer is iterated read-only by the series loop and is
  NOT cleared — confirm at implementation time (grep `series_upsert_buffer` for any
  `.clear()`/reassignment) and place the log write after the loop while the list is
  still populated.
- Per-accession `holdings_inserted` count: `SELECT source_accession, COUNT(*) FROM
  _stg_nport GROUP BY source_accession` issued **before commit** while `_stg_nport`
  (`ON COMMIT DROP`) is still alive; it carries `source_accession` (the COPY column
  written at `:630`).
- `status='success'` (CHECK allows `success|partial|failed`; `_existing_accessions_for_fund_filer`
  reads accessions *regardless of status* (`n_port_ingest.py:628`), so any value
  triggers the S23 skip — `success` is the honest label for a seeded accession).
- One set-based `INSERT … SELECT … FROM _stg_nport GROUP BY source_accession …
  ON CONFLICT (accession_number) DO UPDATE` — no per-row round-trips, no buffer
  dependency. `MAX(fund_series_id)` picks one of the accession's series for the
  informational column (every staged value already satisfies `^S[0-9]{9}$` — it
  passed the observations drain). `COUNT(*)` is the staged-row count (upper bound;
  the DISTINCT-ON drain may collapse a few duplicate holding-ids — Codex 2 NIT,
  documented inline; informational only).

**Correctness — only fully-resolved accessions are seeded (Codex 2 BLOCKING-1)**:
S12 must NOT mark an accession done if it held back any **recoverable** holding —
a valid CUSIP not yet in the cusip_map, buffered for the S13 OpenFIGI sweep. If
seeded, S23 would skip that accession forever and the held-back holdings would
never be ingested even after the CUSIP resolves. So S12 tracks `unresolved_accns`
(accessions with ≥1 valid-but-unmapped CUSIP holding) and the seed SQL excludes
them: `AND source_accession <> ALL(%(unresolved_accns)s::text[])`. Empty-CUSIP
holdings are permanently unresolvable for everyone, so an accession that only
loses empty-CUSIP rows stays eligible. A seeded (clean) accession yields nothing
new on S23 re-fetch — S23 applies identical equity/long/unit gates and reads a
subset of the resolvers — so no coverage is lost. Only **current-quarter**
accessions are in `nport.zip`; older quarters of S23's ~4-quarter horizon are not
bulk-covered → S23 still HTTP-fetches those bodies (bounded; the dominant
enumeration HTTP is eliminated by §3.1).

### 3.4 Zip lifecycle: defer cleanup S16 → S23

`submissions.zip` is deleted at **S16's exit** today
(`scheduler.py:4870 _cleanup_submissions_zip_after_drain`). S23 runs *after* S16
(stage_order 16 < 23, both `sec_rate` lane → serialised). So the zip is gone
before S23 unless cleanup moves:

- **S16**: remove the `_cleanup_submissions_zip_after_drain(candidate)` call
  (`scheduler.py:4870`); update the docstring + the helper's comment
  (`:4889-4891` "no other stage consumes the zip" is now false → "S23 consumes it").
- **S23**: add the unconditional-on-success `_cleanup_submissions_zip_after_drain(candidate)`
  at its exit (same shape S16 had).
- **Helper sharing**: `_cleanup_submissions_zip_after_drain` is module-private in
  `scheduler.py`; S23's invoker is in the same module → call directly.
- **Cleanup fires on the SUCCESS path only, and only in bootstrap dispatch**
  (Codex 2 IMPORTANT). Unlike S16 (bootstrap-only job), `sec_n_port_ingest` ALSO
  runs monthly + via Admin "Run now" — outside bootstrap the `submissions.zip`
  lifecycle is owned by the bulk-download path, so S23 must not delete an archive
  it never managed. Gate: `if in_bootstrap` (`resolve_progress_context() is not None`).
  In bootstrap, an S23 *error* still leaves the zip on disk (cleanup is after the
  `with` block) so the dispatcher's retry can reuse it; only a clean success deletes.
- **Skip-safety backstop (Codex 1 finding 2 — corrected)**: `raw_data_retention_sweep`
  is scoped to `data/raw/**` only (`scheduler.py:839-844, 3864-3905`) — it does
  **NOT** clean `<data>/sec/bulk/submissions.zip`. The real backstops are: (a) S23's
  ordering cap `nport_dataset_processed` terminalises on *any* terminal status of
  S12, so S23's body runs in every bootstrap that reaches stage 23 — the
  never-runs case requires the run to be cancelled/abandoned before S23; (b) the
  next bootstrap's `sec_bulk_download` re-download **overwrites the file in place**
  (single 1.54 GB file, bounded — not unbounded growth) and provenance-stamps it to
  the new run. A cancelled-before-S23 run therefore leaves at most one stale 1.54 GB
  archive until the next bootstrap, which is the same residual #1277 accepted.
- **Non-bootstrap S23** (monthly steady-state, empty params): `use_bulk_zip`
  defaults False, `candidate` typically absent → `unlink(missing_ok=True)` no-ops.
  No behaviour change.

### 3.5 Ordering cap-gate S12 → S23 (correctness + latent contention fix)

S12 (`db` lane) and S23 (`sec_rate` lane) run on **different lanes** → the
dispatcher may run them concurrently. Both write `ownership_funds_observations`.
This is the same latent row-lock contention the §6.5.10 audit fixed for
S22↔S10 / S19↔S11 / S20↔S11 — and S23 now additionally needs S12's
`n_port_ingest_log` rows *visible* before its skip check. One cap-gate solves both.

Add `nport_dataset_processed` (ordering-only cap), mirroring
`institutional_dataset_processed` exactly:

- `_STAGE_PROVIDES["sec_nport_ingest_from_dataset"]` += `nport_dataset_processed`
- `_STAGE_PROVIDES_ON_SKIP["sec_nport_ingest_from_dataset"]` += `nport_dataset_processed`
  (slow-connection-fallback parity: bulk skip still unblocks S23)
- `_STAGE_REQUIRES_CAPS["sec_n_port_ingest"]` = `all_of=("cik_mapping_ready", "nport_dataset_processed")`
- add `"nport_dataset_processed"` to `_ORDERING_ONLY_CAPS` (terminalises on any
  terminal status — "no concurrent writer remains")
- register the new capability name in the `Capability` set / catalogue invariant.

## 4. Files touched

| File | Change |
|---|---|
| `app/services/sec_submissions_zip.py` | NEW — `PRIMARY_SUBMISSIONS_URL_RE`, `match_primary_submissions_cik`, `read_zip_entry`, `ZipBackedArchiveFetcher` |
| `app/jobs/sec_first_install_drain.py` | import `PRIMARY_SUBMISSIONS_URL_RE` from shared module (drop local copy) |
| `app/services/sec_nport_dataset_ingest.py` | write `n_port_ingest_log` per bulk-loaded accession (pre-commit) |
| `app/workers/scheduler.py` | S23 invoker: `use_bulk_zip` + zip-fetcher + cleanup-at-exit; S16: drop cleanup call + fix comment |
| `app/services/bootstrap_orchestrator.py` | S23 stage params `use_bulk_zip=True`; `nport_dataset_processed` cap (provides/provides_on_skip/requires/ordering-only/catalogue) |
| `app/services/processes/param_metadata.py` | `JOB_INTERNAL_KEYS["sec_n_port_ingest"]` += `use_bulk_zip` |

## 5. Tests

- `sec_submissions_zip`: primary URL hit → zip str (utf-8 decoded); non-primary URL
  → delegate; `read_zip_entry` returns None on KeyError, raises on BadZipFile/OSError.
  `ZipBackedArchiveFetcher.fetch_document_text`: **every non-hit delegates to the
  wrapped fetcher** — assert delegate called for (a) member-absent KeyError, (b)
  UnicodeDecodeError on bad bytes, (c) BadZipFile/OSError, (d) non-primary URL; and
  the wrapped result (incl. its `None`) is returned verbatim. A clean hit must NOT
  call the wrapped fetcher.
- S12 log-write: after a bulk ingest, `n_port_ingest_log` has one `success` row per
  distinct loaded accession, correct `filer_cik`/`period_end`/`holdings_inserted`;
  idempotent on re-run.
- S23 invoker: `use_bulk_zip=True` + present provenance-valid zip → no per-CIK
  primary HTTP (assert wrapped-fetcher routing via monkeypatched module attr,
  #1341 seam style); zip absent → HTTP fallback; provenance mismatch → fallback;
  cleanup fires on success path; non-bool `use_bulk_zip` → False.
- S16: `_cleanup_submissions_zip_after_drain` no longer called by drain invoker
  (zip survives to S23). #1277 zip-routing tests unaffected.
- Cap-gate: catalogue invariant accepts `nport_dataset_processed`; S23 requires it;
  S12 provides on success+skip; ordering-only set membership.
- End-to-end skip: bulk-load accession A for trust T (writes log) → S23 over T with
  zip enumeration skips A's body fetch (in `already` set), still ingests
  un-bulk-covered older accession B.

## 6. Floors / perf-claims

This retarget touches `n_port_ingest_log` + `ownership_funds_observations` + the S23
invoker — it does **NOT** touch `sec_filing_manifest` and runs **no** perf-bench
against it. The deferred `sec_filing_manifest` floor seeder STUB obligation
(handoff) therefore does not bind here; it stays deferred to a ticket that actually
benches that table. `etl-perf-claims`: the < 10 min S23 claim is verified by the
R3 measurement run wall-clock, not a synthetic bench.

## 7. Acceptance

- S23 bootstrap wall-clock < 10 min (eliminate ~3.5k per-CIK enumeration HTTP).
- Smoke panel funds slice unchanged (AAPL/GME/MSFT/JPM/HD `ownership-funds-current`
  row count within ±1% of pre-change).
- Pre-push gate green (ruff/format/pyright + unit; PG-dependent tests via CI if dev
  PG is in WAL-recovery — per #1346/#1277/#1341 precedent).

## 8. Risks / conscious tradeoffs

- **Disk window**: `submissions.zip` (1.54 GB) now on disk S8→S23 instead of S8→S16.
  ~7 extra stages of residency. Accepted (#1277 already extended S8→S16).
- **S27 N-CSR** runs after S23 and currently HTTP-walks (does not use the zip).
  Deleting the zip at S23 does not regress N-CSR (it never read the zip). A future
  ticket making N-CSR zip-aware would move cleanup further; out of #1340 scope.
- **Status='success' on partial bulk loads**: a bulk accession with some
  unresolved-CUSIP holdings is logged `success`. By §3.3 this is correct (no S23
  re-fetch recovers them). Honest label; informational `holdings_inserted` reflects
  what landed.
