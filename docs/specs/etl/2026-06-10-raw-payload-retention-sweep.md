# Raw-payload retention sweep — `primary_doc` drop-on-success + SHA-256 guard (#1014)

Author: claude (autonomous)
Date: 2026-06-10
Status: Draft (post-Codex round 1 — 2 High, 2 Medium, 1 Low all addressed inline)
Issue: #1014 (re-scoped 2026-06-10) — part of the #1011 retention plan.
Parent spec: [`retention.md`](retention.md) §"Raw-payload retention policy".

## Problem

`filing_raw_documents` is the #1 table in the dev DB: **5.77 GB on disk
(TOAST-compressed), 16 GB of raw payload bytes**, DB total 20 GB
(measured 2026-06-10, `bootstrap_state=complete`). Per-kind split:

| document_kind | rows | raw bytes |
|---|---|---|
| `primary_doc` | 28,102 | **16 GB** |
| `form4_xml` | 102,700 | 719 MB |
| `def14a_body` | 5,859 | 593 MB |
| others | ~13k | ~140 MB |

`primary_doc` by manifest form:

| form | rows | raw bytes | avg/doc |
|---|---|---|---|
| 10-K (+/A) | 3,776 | 12.3 GB | ~3.5 MB |
| 8-K (+/A) | 8,997 | 4.4 GB | ~500 KB |
| 13F-HR (+/A) | 15,329 | 33 MB | ~2 KB |

10-K/8-K primary-document HTML is ~99.8% of the kind's bytes and the
single largest open-ended growth path in the DB (grew 14 GB → 16 GB
within one day of ongoing ingest).

## Prior decisions this implements (and two deviations)

The parent spec's policy table already pre-decided:

> (future) 10-K body → **drop-on-success**. (future) 8-K body →
> **keep-on-fail**.

Sweeping only `ingest_status='parsed'` accessions implements both:
parsed bodies are dropped; `failed` / `tombstoned` / `pending` rows
keep their payload for forensics and retry.

Two deviations from the original #1014 issue body (pre-rescope):

1. **Hash at sweep time, not at ingest time.** The original plan added
   `payload_sha256` at every `store_raw` + a 16 GB backfill migration.
   Computing the hash server-side inside the sweep's UPDATE gives the
   identical guard property (hash recorded before bytes destroyed) with
   zero ingest-path change and no mass backfill. Rows that are never
   swept don't need a stored hash — it is derivable from the payload at
   any time.
2. **No per-row `retention_mode` column.** The policy is per
   `(document_kind, manifest.source)`, not per row. It lives in one code
   constant the sweep reads (`SWEPT_MANIFEST_SOURCES`), the same shape
   as `SEC_PARSE_AND_RAW`. A column would denormalise a global policy
   into 230k rows.

Settled decisions check: "full raw filing text … separate table, not
`filing_events`" (holds — this is that table); #719 process topology
(job lands in the jobs process via the manual-trigger queue); universal
bootstrap gate (manual-only triangle job, same bypass as
`filing_events_skip_tier_cleanup` #1013). No settled decision changes.

Prevention-log entries applied: #1013 allow-list-superset lesson
(§Structural guard below); mega-table sweep shape (autocommit conn +
per-batch `with conn.transaction()`); `str(row[N])` NULL-coercion
(§read-path); ON CONFLICT must cover all columns (§store_raw).

## Design

### Safety by construction

A swept payload is recoverable because, for these forms, the re-parse
path never reads the stored body:

- **No rewash parser exists for `primary_doc`** — the rewash registry
  (`app/services/rewash_filings.py::_REGISTRY`) covers `form4_xml`,
  `form3_xml`, `def14a_body`, `primary_doc_13dg`, `infotable_13f` only.
  `primary_doc` payload is write-only today (verified: the only readers
  of `filing_raw_documents.payload` are `read_raw`/`iter_raw`, called
  solely by rewash; all other queries are COUNT-style diagnostics).
- **The manifest rebuild path re-fetches.** `sec_rebuild` flips
  `parsed → pending`; the manifest worker re-runs the parser, which
  fetches from `primary_document_url` and calls `store_raw`
  unconditionally. A swept row is transparently re-hydrated by any
  re-parse.
- **SEC archive is durable** for accepted filings; the SHA-256 guard
  detects the (theoretically possible) silent-replacement case.

### Scope — opt-in destruction list

Eligible rows = all of:

```
r.document_kind = 'primary_doc'
AND r.payload IS NOT NULL
AND m.accession_number = r.accession_number
AND m.source IN ('sec_10k', 'sec_8k')          -- SWEPT_MANIFEST_SOURCES
AND m.ingest_status = 'parsed'
AND m.raw_status IN ('stored', 'compacted')
```

The `raw_status` predicate (Codex round 1, High): a split row
`parsed + raw_status='absent' + payload present` is already an
invariant violation (raw_status must reflect the table) — the sweep
must NOT compound it by destroying the payload while the manifest says
`absent`. Such rows are excluded and surface as a dry-run-vs-total
discrepancy for operator triage. `'compacted'` is eligible so a row
re-stored by `store_raw` without a parser transition (manifest still
`compacted`) re-sweeps cleanly; the manifest flag update below is a
no-op for it (already correct).

Keyed on `sec_filing_manifest.source` (CHECK-constrained enum that
collapses amendments: `sec_10k` ⊇ {10-K, 10-K/A}) rather than the
free-text `form` column. Anything NOT listed is kept — a new form
(10-Q, etc.) defaults to keep-always until explicitly added here.
13F-HR `primary_doc` rows (33 MB) are excluded: not worth touching, and
`raw_status` is accession-scoped so compacting one of a 13F's two kinds
would make the manifest flag ambiguous.

`def14a_body` (593 MB) is consciously out of scope: it HAS a registered
rewash parser that reads stored bodies; sweeping it requires a
re-fetch-capable rewash path first. Revisit if it grows materially
(parent-spec threshold language stands).

Eligible on dev today: **12,184 rows / ~14.4 GB raw** (3,173 `sec_10k`
+ 9,011 `sec_8k`).

### Schema — `sql/190_filing_raw_documents_sweepable.sql`

```sql
ALTER TABLE filing_raw_documents
    ALTER COLUMN payload DROP NOT NULL;

ALTER TABLE filing_raw_documents
    ADD COLUMN payload_sha256 TEXT
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ADD COLUMN payload_swept_at TIMESTAMPTZ;

ALTER TABLE filing_raw_documents
    ADD CONSTRAINT chk_swept_rows_carry_hash CHECK (
        payload IS NOT NULL
        OR (payload_sha256 IS NOT NULL AND payload_swept_at IS NOT NULL)
    );
```

- `payload_sha256` TEXT hex (not BYTEA): greppable, comparable in SQL
  and Python without encode/decode asymmetry. CHECK pins shape.
- `chk_swept_rows_carry_hash`: a payload-less row MUST carry hash +
  sweep timestamp — the DB cannot represent "bytes gone, no proof".
- `byte_count` is `GENERATED ALWAYS AS (octet_length(payload)) STORED`
  → becomes NULL on swept rows. This is correct: the operator storage
  chip (`storage_summary`) then reports live payload bytes. Reclaimed
  bytes are reported in the sweep job summary instead.
- No new index: candidates resolve via existing
  `idx_filing_raw_documents_kind_fetched` + manifest PK join; the sweep
  is rare + manual, seq-scan acceptable at 28k rows.

Hash semantics: **SHA-256 of the UTF-8 encoding of the TEXT payload as
stored** — server-side `encode(sha256(convert_to(payload, 'UTF8')), 'hex')`,
Python-side `hashlib.sha256(text.encode('utf-8')).hexdigest()`.
Empirically verified identical on dev PG 17 (accession
`0001291422-13-000007`). Hashing the decoded text (not raw HTTP bytes)
makes the re-fetch comparison immune to transfer-encoding/charset
variance.

### Manifest flag — `raw_status = 'compacted'`

`sec_filing_manifest.raw_status` already reserves `'compacted'`
("future hot-storage eviction path" — sql/118) and the
`transition_status` guard already treats `('stored','compacted')` as
sticky-non-absent. The sweep bulk-updates
`raw_status = 'compacted' WHERE accession IN (batch) AND raw_status = 'stored'`
in the same per-batch transaction as the payload UPDATE. No CHECK
migration, no Literal change (`RawStatus` already includes it). A later
re-parse writes `raw_status='stored'` via the existing parser-outcome
path — a legal transition.

### Sweep service — `app/services/raw_payload_retention.py`

Mirror of `filing_events_cleanup.py` (#1013) / mega-table prevention
entry:

- `sweep_raw_payloads(*, database_url=None, batch_size=100, dry_run=True) -> RawPayloadSweepSummary`
  — `dry_run` defaults TRUE at the service entrypoint too (Codex
  ckpt-2): a destructive service must be opt-in even for REPL / test
  callers, not just at the manual-trigger layer.
- Owns its connection: `psycopg.connect(url, autocommit=True)`; each
  batch in `with conn.transaction()` (real top-level commit per batch).
- Per batch, one statement (chained data-modifying CTEs, all in one
  snapshot + tx):

```sql
WITH batch AS (
    SELECT r.accession_number, r.byte_count, m.source
    FROM filing_raw_documents r
    JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
    WHERE r.document_kind = 'primary_doc'
      AND r.payload IS NOT NULL
      AND m.source = ANY(%(sources)s)
      AND m.ingest_status = 'parsed'
      AND m.raw_status IN ('stored', 'compacted')
    ORDER BY r.accession_number
    LIMIT %(batch)s
    FOR UPDATE OF r SKIP LOCKED
),
swept AS (
    UPDATE filing_raw_documents r
    SET payload_sha256   = encode(sha256(convert_to(r.payload, 'UTF8')), 'hex'),
        payload_swept_at = NOW(),
        payload          = NULL
    FROM batch b
    WHERE r.accession_number = b.accession_number
      AND r.document_kind = 'primary_doc'
      AND r.payload IS NOT NULL
    RETURNING r.accession_number
),
flagged AS (
    UPDATE sec_filing_manifest m
    SET raw_status = 'compacted'
    FROM swept s
    WHERE m.accession_number = s.accession_number
      AND m.raw_status = 'stored'
    RETURNING m.accession_number
)
SELECT b.accession_number, b.source, b.byte_count
FROM batch b
JOIN swept s ON s.accession_number = b.accession_number
```

  Notes pinned for the implementer (Codex round 1):
  - RETURNING evaluates the NEW row — the OLD `byte_count` must come
    from the `batch` CTE, never from the UPDATE's RETURNING.
  - The `swept`/`flagged` CTEs target different tables, so the
    one-snapshot rule for data-modifying CTEs is safe here.
  - Lock scope is honest: `FOR UPDATE OF r` locks only the raw row.
    The manifest is NOT locked; a concurrent `sec_rebuild` can flip
    `parsed → pending` between snapshot and commit. Consequence is
    benign by construction: the pending row's worker re-parse fetches
    from `primary_document_url` unconditionally and `store_raw`
    re-populates the payload — it never reads the stored body. The
    raw-row side IS re-checked under lock (`payload IS NOT NULL` in
    the `swept` UPDATE), so a `store_raw` that won the race is never
    clobbered by a stale hash-of-nothing.
- `dry_run=True`: one aggregate SELECT (count + SUM(byte_count)), no
  writes.
- Idempotent: `payload IS NOT NULL` predicate → drained DB sweeps 0.
- Terminates: candidate set strictly shrinks per batch.
- `batch_size=100` default: bounds per-tx detoast cost (10-K avg
  3.5 MB → ~350 MB worst-case detoast per batch) and WAL burst.
- Summary: `rows_swept, batches, bytes_reclaimed, by_source`.

### Re-hydrate helper + hash guard — same module

`rehydrate_raw_document(conn, *, accession_number, document_kind, fetch_text) -> RehydrateOutcome`

1. Read row. `payload IS NOT NULL` → no-op (`already_present`).
   Missing row → raise (nothing to rehydrate).
2. `text = fetch_text(source_url)` — injected callable; production
   caller passes the SEC-rate-limited provider fetch. No new HTTP code.
3. `hashlib.sha256(text.encode('utf-8')).hexdigest()` vs stored
   `payload_sha256`. **Mismatch → log warning + raise
   `RawPayloadIntegrityError`** (SEC silently changed the document —
   operator must adjudicate; never auto-overwrite).
4. Match → `UPDATE ... SET payload = %s, payload_swept_at = NULL
   WHERE ... payload IS NULL AND payload_sha256 = <verified hash>`
   (keep `payload_sha256` — still true; keep `fetched_at` — original
   SEC-publication-era timestamp, mirrors `_bump_parser_version`
   rationale) + manifest `raw_status='stored'` where `'compacted'`.
   The hash re-pin in the WHERE (Codex ckpt-2): a concurrent
   `store_raw` + re-sweep during the fetch can install a NEWER hash;
   writing the stale-verified bytes under it would pair bytes with a
   hash they were never checked against. `rowcount == 0` → report
   `already_present`, write nothing.

Helper-only in this PR (operator invokes via Python / future admin
endpoint). The hot re-parse path doesn't need it — parsers re-fetch +
`store_raw` regardless.

### `store_raw` change (prevention #732)

A fresh body invalidates sweep state. ON CONFLICT SET gains:

```sql
payload_sha256 = NULL,
payload_swept_at = NULL
```

Fresh-or-amended bytes are authoritative; a stale hash must not linger
to fail a future verify against a legitimately re-stored body.

Concurrency invariant (Codex round 1): whatever the interleaving of
sweep and `store_raw` on the same row, the terminal state is one of
exactly two legal shapes — `payload IS NULL + sha256 + swept_at`
(sweep won) or `payload IS NOT NULL + both sweep columns NULL`
(store_raw won). Both writers take the row lock (single-statement
UPDATE / upsert), the sweep re-checks `payload IS NOT NULL` under the
lock, and `store_raw` unconditionally clears the sweep columns — so no
third state is reachable. The DB CHECK `chk_swept_rows_carry_hash`
forbids the dangerous fourth shape (payload gone, no hash) outright.
Tested sequentially (both orders) in the DB tier; a two-session lock
test adds no coverage beyond what the row lock already guarantees.

### Read-path NULL safety (prevention #960)

`RawFilingDocument.payload: str | None`, `byte_count: int | None`;
`read_raw` / `iter_raw` stop coercing via `str(...)` (which would
produce the literal string `"None"`). `run_rewash` gains a
belt-and-braces guard: `raw_doc.payload is None → rows_skipped += 1`
(unreachable for currently-swept kinds — see structural guard — but
cheap and future-proof).

### Job triangle — `raw_payload_retention_sweep`

Manual-trigger-only, #1013 shape:

- `app/workers/scheduler.py`: `JOB_RAW_PAYLOAD_RETENTION_SWEEP` +
  thin `_tracked_job` wrapper (`tracker.row_count = rows_swept`).
- `app/jobs/runtime.py::_INVOKERS` entry.
- `app/jobs/sources.py`: **own lane `db_raw_sweep`** — NOT the
  catch-all `db` lane. A 12k-row sweep holds its lane for minutes;
  on `db` it would starve `orchestrator_high_frequency_sync`
  (every-5-min, same lane) — the exact #1526/#1527 starvation class.
  Manual-only job ⇒ no `ScheduledJob` row, no bootstrap-stage CHECK
  migration (precedent #1540).
- `app/services/processes/param_metadata.py`: `dry_run` bool param
  (mirrors `raw_data_retention_sweep`).
- NOT in `SCHEDULED_JOBS` — no cadence, no bootstrap gate interplay.

### Structural guard (#1013 lesson, inverted)

The #1013 trap was a keep-list that under-enumerated → delete destroyed
real data. Here destruction is opt-in (drop-list), so the residual risk
is different: **a rewash parser registered for a swept kind later**
would read NULL payloads. Pure-logic test pins the invariant:

```python
def test_swept_kinds_have_no_rewash_parser():
    from app.services import rewash_filings
    from app.services.raw_payload_retention import SWEPT_DOCUMENT_KINDS
    assert SWEPT_DOCUMENT_KINDS.isdisjoint(rewash_filings.registered_specs())
```

plus `SWEPT_MANIFEST_SOURCES ⊆` the manifest `source` vocabulary
(import the Literal/constant from `sec_manifest.py`, not a hand copy).
Registering a `primary_doc` rewash parser then fails this test until
the author reconciles it with the sweep (re-fetch-capable rewash or
de-scoping the kind).

## Storage reclaim mechanics (honesty section)

Nulling TOASTed payloads marks TOAST tuples dead; **plain autovacuum
makes the space reusable but does not shrink the 5.77 GB file**.
Operator follow-up on dev: run `VACUUM (ANALYZE) filing_raw_documents`
after the sweep (space reuse), then a one-time
`VACUUM FULL filing_raw_documents` (exclusive lock, ~minutes, needs
~2× transient disk) to actually return ~4 GB to the OS before the
issue-1556 threshold re-measure. Expected post-sweep steady state: table
~1.5–1.8 GB on disk (form4/def14a/others remain).

## Tests (lean tier policy)

Pure-logic (fast tier):
1. Structural: swept kinds ∩ rewash registry = ∅; swept sources ⊆
   manifest source vocabulary.
2. `rehydrate` hash guard: fake `fetch_text`; mismatch raises
   `RawPayloadIntegrityError`, match returns payload restore plan
   (mock-cursor level).
3. `read_raw` NULL-payload mapping (no `"None"` literal).

DB tier (`-m db`, two genuinely-new SQL mechanisms):
4. Sweep integration: seed 3 raw rows (parsed/failed/parsed-other-kind)
   + manifest rows → sweep with `batch_size=1` → only the parsed
   `primary_doc` row swept; hash equals `hashlib` reference; manifest
   `compacted`; `chk_swept_rows_carry_hash` satisfied; re-run sweeps 0;
   `dry_run` writes nothing.
5. Rehydrate integration: swept row + matching fake fetch → payload
   restored, `payload_swept_at` NULL, manifest back to `stored`;
   `store_raw` over a swept row clears both sweep columns, and
   sweeping a re-stored row re-sweeps it cleanly (both interleaving
   orders → one of the two legal terminal states).

## Dev-verify (DoD 8–12)

1. Migration applies (drift-guard hash recorded).
2. Manual trigger `dry_run=true` → counts match the eligibility query.
3. Real run → `rows_swept ≈ 12k+`, bytes_reclaimed ≈ 14+ GB raw.
4. `VACUUM (ANALYZE)`, then `VACUUM FULL` → record
   `pg_total_relation_size` before/after + `pg_database_size`.
5. Panel smoke: `/instruments/{AAPL,GME,MSFT,JPM,HD}/ownership-rollup`
   200 + figures unchanged (sweep touches no typed tables).
6. Cross-source: pick one swept 10-K accession → fetch
   `primary_document_url` from SEC EDGAR direct → SHA-256 matches the
   stored `payload_sha256` (this is the reproducibility guard exercised
   end-to-end, and the independent-source check in one step).
7. 8-K page + business-summary endpoint still render (no payload
   readers on those paths).

## Out of scope / follow-ups

- `def14a_body` sweep (needs re-fetch-capable rewash) — revisit on
  growth.
- Admin endpoint / UI for rehydrate — operator-driven Python for now.
- #1556 threshold rebase lands as its own PR after step 4 measurement.

## Rollback

Migration is additive (two nullable columns + constraints); the sweep
is the only writer of NULL payloads. Roll back = stop triggering the
job. Swept payloads are recoverable per-accession via `rehydrate` or
any manifest re-parse; bulk restore = `sec_rebuild` scope
`{source: sec_10k|sec_8k}` (re-fetch at 10 req/s shared, ~12k requests
≈ 20 min of budget).
