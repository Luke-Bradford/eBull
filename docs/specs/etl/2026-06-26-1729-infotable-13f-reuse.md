# #1729 — Reuse stored infotable_13f on re-drain (extend #1591 PR1 to 13F-HR)

Status: spec (live). Branch `feature/1729-reuse-infotable-13f-redrain`.
Follow-up to #1591 PR1 (`86d42e5c`, #1727) which wired stored-body reuse into the
4 single-primary retained parsers (def14a/form4/form3/13dg). 13F-HR was deferred
because it is multi-doc.

## Problem

`_parse_13f_hr` (`app/services/manifest_parsers/sec_13f_hr.py`) fetches, per accession:
1. `index.json` (not stored — directory listing)
2. `primary_doc.xml` → kind `primary_doc`, **SWEPT/born-compacted** (#1617) → always re-fetched
3. `infotable.xml` → kind `infotable_13f`, **RETAINED**, the largest doc (avg 199 KB)

On a re-drain (`sec_rebuild` resets the manifest row to `pending` → manifest worker
re-runs the parser) the parser refetches the infotable even though the body is
already stored. The infotable is the dominant per-row byte cost.

## Source rule

- **One Information Table per 13F-HR accession** (SEC Form 13F General
  Instruction; the EDGAR `<informationTable>` XML attachment, mandated 2013).
  `parse_archive_index` resolves exactly one `infotable_name`, and `store_raw`
  UPSERTs it under `(accession, "infotable_13f")` — so the key
  `(accession, "infotable_13f")` uniquely + permanently identifies that one
  attachment. There is no second infotable candidate to disambiguate, so reuse by
  `(accession, kind)` cannot select the wrong document (Codex ckpt-1 HIGH #1).
  This matches the PR1 reuse contract exactly — form4/form3/def14a/13dg all reuse
  by `(accession, kind)` with no `source_url` re-validation; adding one here would
  diverge from `stored_body`'s settled helper contract for zero benefit (the
  attachment is structurally singular). The body parser `parse_infotable`
  (versioned `_PARSER_VERSION_13F_INFOTABLE`) still runs on every reuse, so a
  body-parser change IS picked up; only the structurally-deterministic attachment
  *selection* (`parse_archive_index`) is skipped, which is safe because the
  archive is frozen (next bullet).
- **A stored body never goes stale for a fixed accession** — this is the settled
  **#1591-family internal invariant** (already stated verbatim in
  `raw_filings.stored_body`'s docstring + the #1591 proposal), resting on the
  EDGAR mechanism that an amendment is filed as a *new* accession (13F-HR/A) with
  its own accession number while the original accession's archive directory is
  immutable. "Present" == "reusable", no freshness check. (Codex ckpt-1 HIGH #2:
  cited as an internal invariant + named the EDGAR amendment mechanism, not
  inferred from first principles.)
- **Raw-payload retention is partitioned by #1617** (settled-decisions, data-eng
  §13.F). `infotable_13f` is already in the **REWASH** bucket
  (`rewash_filings.registered_specs()` → `_apply_13f_infotable`, since #836), so a
  stored-body reader is already sanctioned for it — adding a second reader (the
  manifest-parser fast path) needs NO bucket change and keeps
  `tests/test_raw_payload_retention.py` green. `primary_doc` stays SWEPT
  (re-fetched); only `infotable_13f` is reused.

## Full-population verification (dev DB, 2026-06-26)

Two populations, both full (not samples):

**(a) `filing_raw_documents` — are stored infotable bodies reusable?**

| document_kind | rows | with_payload | swept | avg_bytes | reusable? |
|---|---:|---:|---:|---:|---|
| infotable_13f | 18,131 | 18,131 | 0 | 199 K | ✅ 100% |
| primary_doc | 113,393 | 38,413 | 74,980 | 74 K | ❌ 66% swept → re-fetch |

**(b) rebuild candidates — `sec_filing_manifest` source=`sec_13f_hr` LEFT JOIN the
infotable raw row (Codex ckpt-1 MED #3 — the reuse-hit denominator is manifest
rows, not raw rows):**

| ingest_status | manifest rows | with stored infotable | reuse-hit |
|---|---:|---:|---|
| parsed | 17,396 | 17,396 | **100%** |
| pending | 50,445 | 749 | partial (mid-drain) |
| tombstoned | 254,965 | 0 | n/a — tombstoned pre-infotable, re-fetch index + re-tombstone (no infotable fetch wasted) |
| failed | 14 | 0 | n/a |

Every **parsed** 13F-HR accession (the re-drain reuse target — store-before-parse
#938 guarantees a parsed row has a stored infotable) reuses on re-drain: 17,396 /
17,396 = 100%. Tombstoned rows have no infotable to reuse and never fetched one, so
reuse is correctly inert for them.

## Change

`_parse_13f_hr` Step 3 (the infotable fetch site, AFTER the index-walk + primary
fetch/store/parse + `thirteen_f_within_retention` gate — reuse replaces ONLY the
fetch, never a gate):

```python
infotable_xml = stored_body(conn, accession_number=accession, document_kind="infotable_13f")
if infotable_xml is None:                 # first ingest, or absent → fetch + store
    <existing fetch via provider.fetch_document_text(infotable_url)>
    if not infotable_xml: <existing empty-body tombstone, raw_status="stored">
    <existing store_raw(... document_kind="infotable_13f" ...)>
# parse_infotable(infotable_xml) — unchanged; reuse joins the flow here
```

Import `stored_body` from `app.services.raw_filings` (alongside the existing
`store_raw` import).

Invariants preserved (identical to PR1):
- **#938 store-before-parse**: on reuse the row already exists with a payload, so
  `effective_raw_status='stored'`; no `store_raw` call needed.
- **fetched_at not churned** on reuse (no re-store) — preserves the
  operator-visible last-published timestamp.
- **raw_status="stored"** on EVERY post-infotable outcome — already true today:
  parse-failure (`:439`), upsert-transient (`:549`), upsert-deterministic
  (`:577`/`:581`), success (`:584`), and the empty-body / store_raw-failure
  tombstones all return `raw_status="stored"`. The reuse branch joins at
  `parse_infotable`, so these returns hold for the reuse path identically (the
  Codex ckpt-1 #2 audit from PR1 — verified, none slipped).
- Empty-body guard + store_raw run on the fetch path only; stored bodies are
  non-empty by construction (`store_raw` rejects empty).

## Prefetch-hook mirror (prevention-log #1956) — verify-only, NO code change

The rule: a prefetch hook that front-runs a gated consumer must mirror EVERY
pre-fetch gate (reuse included) or it burns the budget it exists to save. Applied
to 13F-HR's two hooks:

- `_thirteen_f_index_url` (pass 1) prefetches `index.json` — never stored, never
  reused. Nothing to mirror.
- `_thirteen_f_expand` (pass 2) prefetches **`primary_doc.xml` ONLY** — which is
  SWEPT (always re-fetched), never reused; AND it deliberately does NOT prefetch
  the infotable (the `thirteen_f_within_retention` gate is post-primary-parse, so
  prefetching the often-large infotable for a row the parser tombstones would
  itself violate the mirror contract — Codex ckpt-1 HIGH on #1700; pinned by
  `test_two_phase_prefetch_out_of_retention_never_fetches_infotable`).

Because **no hook prefetches the infotable**, the new infotable reuse gate has no
hook to mirror — adding one would be wrong (it would re-introduce the
prefetch-the-infotable-for-a-row-that-tombstones bug). Document the reasoning in
`_thirteen_f_expand`'s docstring so a future reader doesn't "fix" the asymmetry.

## Tests (DB-tier — mirror existing `test_manifest_parser_sec_13f_hr.py` harness)

The 13F parser is DB-coupled (filer/holdings/observations upserts); the existing
suite is integration/db tier via `ebull_test_conn` + `_patch_fetch_map`'s `calls`
list. Add:

- **reuse SUCCESS**: pre-seed `filing_raw_documents` with the `infotable_13f`
  body; run the worker; assert (a) `base+"infotable.xml" NOT in calls` (no fetch),
  (b) `base+"primary_doc.xml" IN calls` (primary still SWEPT-fetched), (c) row
  reaches `parsed` + `raw_status="stored"`, (d) holdings upserted identically.
- **reuse FAILURE**: pre-seed a stored infotable whose `parse_infotable` raises
  (monkeypatch) → returns `failed`/`raw_status="stored"` (proves the reuse path
  repairs `effective_raw_status` even on a parse crash).
- **MISS**: no stored infotable → fetches + stores (existing happy-path behaviour
  unchanged — already covered by `test_happy_path_*`; add an explicit assert that
  `filing_raw_documents` has the infotable row after).

## Dev-verify (ETL DoD 8-12)

Scoped `POST /jobs/sec_rebuild/run` `{"source":"sec_13f_hr","instrument_id":...}`
on a known 13F filer's accession already on file: assert the infotable URL is NOT
re-fetched (0-HTTP for the infotable on re-drain) and the parsed holdings are
byte-identical to the pre-drain rows (parity ⇒ no data change). Smoke
`/instruments/AAPL/ownership-rollup` institutions slice unchanged. No `sec_rebuild`
backfill needed for correctness (output row-identical). Restart the jobs daemon
onto new main post-merge (parser-touching).

## Skill ownership

Document the **"manifest-parser reuse-on-redrain + prefetch-hook-mirrors-the-gate"**
pattern in `.claude/skills/data-engineer/SKILL.md` in this PR (operator directive):
the manifest-parser `stored_body` fast path is a SECOND reader on a REWASH kind
(distinct from the `rewash_filings` bulk spec); a prefetch hook must mirror every
pre-fetch gate including reuse, BUT only for kinds the hook actually prefetches —
13F's infotable is reused yet never prefetched, so its reuse gate has no hook
mirror.

## Non-goals

`primary_doc` reuse (#1617 SWEPT); 10-K XBRL prefetch (#1730); form5/nport reuse
(#1731).
