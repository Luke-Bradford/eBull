# Insider Form 4/3: transient fetch errors must not permanently tombstone (#1698)

Status: proposal (unshipped). Fixes a data-loss regression of #1684/#1683.

## Source rule

Discriminate **transient** failures (retry) from **deterministic/permanent** ones (tombstone) before permanently dropping a filing. Established in:
- `docs/review-prevention-log.md` ~line 1288 — "Manifest parser upsert exception must discriminate transient vs deterministic"; canonical `app/services/manifest_parsers/_classify.py::is_transient_upsert_error`.
- `app/providers/concurrent_fetch.py:58-63` — `None` from `concurrent_map` is a **lossy** sentinel (conflates "fn raised + caught" with "fn returned None / 404"); callers needing the distinction must wrap the fetcher in a **discriminated result type**.
- `app/jobs/sec_manifest_worker.py:351-377` (#1686 Ph2, "Codex ckpt-1 HIGH") — the correct model: concurrent prefetch returns successful bodies only; a `None` is dropped and the serial path re-fetches, preserving tombstone discrimination.

`fetch_document_text` (`sec_edgar.py:619-623`) returns `None` **only** on 404/410, returns `""` on an empty 200, and **raises** on every other non-2xx (429/5xx, after ResilientClient's 3 retries) and on transport errors. So the discriminator needs no HTTP-status parsing: the permanent signals are explicit *return values*; any *raised* exception is transient.

## Full-population verification (done, dev) — reproducible queries

429 burst correlated to insider tombstone spike:
```
grep "per-future failure" /tmp/ebull_jobs.log | grep -c "429"        # 441 in 01:15–01:21Z
docker exec ebull-postgres psql -U postgres -d ebull -tA -c \
  "SELECT date_trunc('minute',created_at), count(*) FROM insider_filings \
   WHERE is_tombstone AND created_at > now()-interval '6 hours' GROUP BY 1 ORDER BY 1;"
  # → 2026-06-21 01:21:00Z = 411 (tail of the burst); baseline ≈2/min
```
Tombstone population by form × era:
```
docker exec ebull-postgres psql -U postgres -d ebull -tA -c \
  "SELECT document_type, (created_at >= timestamptz '2026-06-20 00:00:00+00') AS post_1684, count(*) \
   FROM insider_filings WHERE is_tombstone GROUP BY 1,2 ORDER BY 1,2;"
  # → F4: 1763 pre / 558 post ; F3: 0 / 228 ; F5: 583 / 0 (lane not re-instated → not bleeding)
```
- Regression-era (post-#1684) = **786** (558 F4 + 228 F3). Pre-#1684 = 2346 (old oldest-first path; out of scope).
- Bug confirmed in two call sites: `insider_transactions.py:1811-1835` (Form 4), `insider_form3_ingest.py:656-674` (Form 3). Both: `if not xml: _write_tombstone(...); continue`.
- Upsert path (`insider_transactions.py:1871-1911`) already discriminates (#1131) → only the fetch path is wrong.

## Change

### 1. `app/providers/concurrent_fetch.py` — discriminated classified fetch (shared home)
Add `FetchOutcome` (enum: `OK | MISSING | EMPTY | TRANSIENT`) and:
```
def fetch_document_texts_classified(fetcher, urls) -> dict[str, tuple[FetchOutcome, str | None]]
```
Per-URL classifier wraps `fetcher.fetch_document_text`:
- raises (any `Exception`) → `(TRANSIENT, None)`  ← the fix; no httpx import needed. **Log the exception class + message (+ status if `HTTPStatusError`) at WARNING per URL** (Codex ckpt-1 MED) so a *deterministic* fetcher/config bug — which would otherwise retry forever under the new "never tombstone on raise" rule — is operator-visible, not silent.
- returns `None` → `(MISSING, None)`  (404/410)
- returns `""` → `(EMPTY, None)`  (empty-200 poison)
- returns body → `(OK, body)`

De-dup URLs (as `fetch_document_texts` does). Two lossy-result safety rules (Codex ckpt-1 MED): (a) a `None` from `concurrent_map._safe` (classifier itself died) maps to `(TRANSIENT, None)`; (b) callers MUST look up with `outcomes.get(url, (FetchOutcome.TRANSIENT, None))` so a URL filtered by de-dup (falsey) or otherwise **absent from the map defaults to TRANSIENT, never tombstone**. Keep the existing `fetch_document_texts` (manifest worker still uses it; it handles `None` itself).

### 2. Both insider loops — branch on outcome
Replace `bodies = fetch_document_texts(...)` + `if not xml:` with classified outcomes:
- `OK` → store_raw + parse + upsert (unchanged)
- `MISSING` / `EMPTY` → `_write_tombstone` + commit + continue (unchanged behaviour; preserves #1683 empty-body wedge fix)
- `TRANSIENT` → `fetch_errors += 1`, WARNING "transient fetch, retry next run", `continue` — **no tombstone, row stays selectable**

Form 4: `insider_transactions.py::_process_candidates`. Form 3: `insider_form3_ingest.py` loop. Grep for any other `if not xml`/`if not body` tombstone shape (e.g. a Form 5 path) and fix if it shares the helper.

### 3. No schema change. No manifest-worker change.

## Backfill (after fix merges; runs with insider jobs quiesced)

Codex ckpt-1 HIGH×2 + MED: a bare date-predicate DELETE is not reproducible (created_at is destroyed by the delete; re-tombstones get fresh timestamps) and is rerun-unsafe (would re-delete fixed-code tombstones). Fix = **materialize the suspect set ONCE into a persistent staging table**; every step references that fixed list, never the live `created_at` predicate.

1. **Stage once** (scoped by form type; complete regression-era superset):
```sql
CREATE TABLE IF NOT EXISTS _insider_tombstone_backfill_1698 AS
SELECT accession_number, instrument_id, document_type, created_at AS tombstoned_at
FROM insider_filings
WHERE is_tombstone
  AND created_at >= timestamptz '2026-06-20 00:00:00+00'
  AND document_type IN ('3','3/A','4','4/A');   -- ~786; excludes Form 5 + pre-#1684
```
Over-inclusion is safe, not a correctness risk: the fixed code re-tombstones the legit-permanent subset (real 404/empty/parse-miss), so the cost of an over-broad superset is bounded re-fetch, not data corruption. Staging into a table (not a live predicate) makes the set FIXED → the whole backfill is rerun-idempotent.

2. **Quiesce + delete** (Codex MED — no job race): with the insider lanes not running (delete during the daemon-restart window onto fixed main), delete only the staged accessions:
```sql
DELETE FROM insider_filings f USING _insider_tombstone_backfill_1698 s
WHERE f.accession_number = s.accession_number AND f.is_tombstone;
```

3. **Re-ingest on fixed code**: start the daemon on merged main, run `sec_insider_transactions_ingest` + `sec_form3_ingest` as a controlled pass (manually invoke until the staged set is drained; limit=500/run). Accounting is by the staged table's FINAL state, so concurrent hourly runs only help (idempotent upsert) — they cannot corrupt the count.

4. **Account** (proves recovery): for the staged set, count now-real rows (recovered) vs fresh tombstones (legit-permanent) vs still-absent:
```sql
SELECT
  count(*) FILTER (WHERE f.accession_number IS NOT NULL AND NOT f.is_tombstone) AS recovered,
  count(*) FILTER (WHERE f.is_tombstone) AS re_tombstoned,
  count(*) FILTER (WHERE f.accession_number IS NULL) AS still_absent
FROM _insider_tombstone_backfill_1698 s
LEFT JOIN insider_filings f ON f.accession_number = s.accession_number;
```
Expect recovered ≫ re_tombstoned (the 411-spike were transient). Drop the staging table when done. Must run on FIXED code or transient ones re-tombstone immediately.

## Tests (pure-logic)
- classifier table-test: stub fetcher returning body / `""` / `None` / raising → asserts `OK/EMPTY/MISSING/TRANSIENT`.
- loop branch: TRANSIENT outcome writes NO tombstone (assert `_write_tombstone` not called) and leaves a fetch_error; MISSING/EMPTY DO tombstone. Use a fake conn/spy or extract the branch into a pure decision fn.

## Risks / edge cases
- A permanently-broken URL (e.g. malformed `primary_document_url` → raises) now retries every run instead of tombstoning → bounded waste, visible in logs, not data loss. Acceptable for a data-loss fix; separate DQ ticket if it becomes noise.
- EMPTY stays a tombstone. This is an **empirical local policy** (not a SEC source rule): established by #1683 — the `if not xml` empty-body guard at `insider_transactions.py:1814-1821` / `insider_form3_ingest.py:656-660`, added to stop empty-200 poison filings re-wedging the backfill (eBay 3-mo freeze). This fix preserves it unchanged; a *transient* empty-200 from SEC under load is unverified and out of scope (would need its own full-pop evidence to reclassify).
- `store_raw` (insider loop, post-OK) is still bare/uncaught (prevention-log 1278 shape) — pre-existing, out of scope; note in PR.

## Prevention-log
Extend the ~line-1288 entry: the transient-vs-permanent discrimination applies to the **fetch** phase too, not only the upsert phase — a caught concurrent-fetch `None` must not be tombstoned without distinguishing 404/empty (permanent) from a raised 429/timeout (transient).
