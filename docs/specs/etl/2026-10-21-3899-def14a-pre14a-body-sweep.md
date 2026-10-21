# #3899 slice 2 — sweep the DEF 14A and PRE 14A raw bodies

> Issue: **#3899** (child of the #3117 storage umbrella). Slice 1 (#3931 `63461266`,
> #3933 `3186fbf9`) made a re-fetch byte-reproducible again by stripping the SEC edge's
> injected `<script>` at both raw-body boundaries.
> Prior art: #1014 (sweep + `rehydrate_raw_document`), #1615 (born-compaction), #1617
> (the three-class partition), #2774 (`pre14a_body` verdict,
> `docs/specs/etl/2026-09-17-pre14a-body-retention-verdict.md`).
> Codex checkpoint 1, round 1: 25 findings, NOT SOUND. The first draft deleted the
> DEF 14A rewash and lost capabilities the re-drain does not have. This version keeps
> the rewash and makes it re-fetch-capable instead.

## 1. Source rules

- **Retention class.** `docs/settled-decisions.md:108-114` ("Raw-payload retention,
  #1617"): a stored payload is legitimate only as exactly one of **re-read** ("a rewash
  parser reads the stored body", registered in `rewash_filings.registered_specs()`),
  **housekept-and-negligible** (born-compacted at source, rehydratable from `source_url`,
  `raw_filings.SWEPT_DOCUMENT_KINDS`) or **kept-and-negligible**. The partition is per
  `DocumentKind` and CI-enforced.
- **The sanctioned reconciliation of a rewash with a sweep.**
  `tests/test_raw_payload_retention.py::test_swept_kinds_have_no_rewash_parser` names
  it: registering a rewash parser for a swept kind "requires reconciling it with the
  sweep first (re-fetch-capable rewash, or de-scoping the kind here)". This spec takes
  the first option. A re-fetch-capable rewash does not read a stored body, so it is not
  a re-read justification. The kind's payloads are justified as housekept, and the kind
  stays in exactly one class.
- **Identity of a filing's bytes.** Every EDGAR submission gets its own accession
  number, and a proxy is revised by a new submission (DEFR14A, DEFA14A) rather than by
  editing the accepted one. That is the premise `raw_filings.stored_body` reuses a body
  on. It is **not** a guarantee that `www.sec.gov` serves the same bytes forever: slice 1
  found the edge rewriting HTML from 2026-09-20. No safety claim here rests on
  immutability. Every swept body is re-fetched and hash-matched first (D2).
- **Hash.** SHA-256 of the UTF-8 encoding of the stored TEXT, computed server-side
  (`raw_payload_retention` module docstring).

## 2. Premise, measured on the full population (dev DB, 2026-10-21 ~03:00Z)

All figures come from the census in Appendix A. A row count is a count of
`filing_raw_documents` rows. "Live" means `payload IS NOT NULL`, and bytes are
`sum(byte_count)` over live rows (logical, i.e. before TOAST compression).

| kind | manifest status / raw_status / reason | live rows | live bytes |
|---|---|---|---|
| `def14a_body` | tombstoned / stored / no Item 403 table (post-fetch) | 39,267 | 18.52 GB |
| `def14a_body` | parsed / stored | 3,627 | 16.22 GB |
| `def14a_body` | tombstoned / absent / cap (split rows) | 588 | 0.41 GB |
| `def14a_body` | tombstoned / stored / cap | 88 | 0.08 GB |
| `def14a_body` | no manifest row | 106 | 0.05 GB |
| `def14a_body` | tombstoned / stored / missing instrument_id | 13 | 0.02 GB |
| `def14a_body` | tombstoned / absent / missing instrument_id (split) | 4 | 0.01 GB |
| `pre14a_body` | tombstoned / stored | 222 | 2.35 GB |
| `pre14a_body` | parsed / stored | 285 | 1.46 GB |

- **D2's eligibility predicate** (Appendix A, C2) selects **43,502 rows / 38.64 GB
  logical**: `def14a_body` 42,995 / 34.84 GB and `pre14a_body` 507 / 3.80 GB. 33
  `def14a_body` rows carry a filing-index `source_url` (C3, the #3590 class) and are
  excluded on top of that (D2).
- **Physical:** `filing_raw_documents` is 30 GB of the 88 GB dev DB (C5), TOAST-compressed.
  Logical and physical are different numbers. §6 item 7 reports both after the sweep.
- **The tombstoned DEF 14A cohort is not dead weight.** Item 402(c) executive
  compensation runs on the ownership-tombstone path (`def14a_ingest.py` v3 note, #2086),
  and 3,248 tombstoned accessions carry `def14a_exec_compensation` rows (C6).
- **175 populated accessions are refreshable only by rewash** (C4): 171 "latest-N
  primary cap" and 4 "missing instrument_id" tombstones still carry typed holdings or
  comp rows. `_parse_def14a` returns before fetching for both (`def14a.py:162-223`).
  `_apply_def14a` re-parses populated accessions and applies the cap only to its
  rescue branch (`rewash_filings.py:680-714`).

### Every payload reader of the two kinds

Grep over `app/` for `def14a_body`, `pre14a_body`, `stored_body(`, `read_raw(` and
`require_payload()`:

| reader | behaviour on a NULL payload today | after this change |
|---|---|---|
| `manifest_parsers/def14a.py:238` `_parse_def14a` | `stored_body` → `None` → fetch | unchanged (born-compacts on store) |
| `manifest_parsers/def14a.py:567` prefetch gate | `None` → prefetch | unchanged |
| `def14a_ingest.py:1082` legacy ingester | stores, then parses the in-memory body | unchanged (born-compacts) |
| `rewash_filings.py:174` `run_rewash` → `_apply_def14a` | row skipped | **verified fetch** (D1) |
| `def14a_recipients.py:710` `load_row_locations` | returns `{}` | **cache + verified fetch** (D3) |
| `api/instruments.py:3738`, `ownership_drillthrough.py:566` | `COUNT(DISTINCT accession)` of rows | unchanged (rows survive) |
| `manifest_parsers/sec_pre14a.py` | writer only; born-compacted since #2774 | unchanged |
| `scripts/census_*` / `scripts/ab_*def14a*` research scripts | read `payload` in bulk | D4 |

`pre14a_body` has no payload reader in `app/`; it has been housekept since #2774.

### The naive sweep would silently regress #2351

`load_row_locations` returns `{}` for a NULL body (`def14a_recipients.py:710-712`).
`compute_desired` then makes no class-column or class-row decision for that accession
(:814-831), and the reconcile DELETEs stored suppressions that are neither desired nor
kept (:869-900). The rows at risk are those whose reason depends on body evidence: 10
`other_common_class_column` accession suppressions and 159 `other_common_class_row` row
suppressions (C7). This census counts reasons; it does not compute a deletion set, and
§6 item 2 measures the actual sets.

## 3. Rejected alternatives

1. **Keep the bodies, compressed.** This was measured on a 1% sample, so it is a cost
   estimate rather than a safety claim. Python 3.14's stdlib `compression.zstd` on 424
   bodies (`abs(hashtext(accession_number)) % 100 = 7`) took 402 MB raw to 141 MB at level
   9. That is 2.8× over raw and 1.7× over the sample's current TOAST size of 242 MB. Level
   19 gains 2% at 20× the CPU. Extrapolated, it leaves roughly 40% of today's stored bytes
   in place. Rejected.
2. **Keep only the multi-class issuers' bodies.** One `DocumentKind` would then sit in two
   retention classes, which the per-kind #1617 partition forbids.
3. **Delete the DEF 14A rewash and rely on `sec_rebuild` re-drain** (draft 1). Rejected
   in round 1: it loses the refresh of the 175 populated out-of-cap accessions (C4); the
   manifest-independent cohort resolution (`rewash_filings.py:651-679`, which covers the
   106 manifestless rows); the historical-sibling unions (:897-961, :984-989); and the
   holdings and comp regression brakes, stale-exec clearing, withheld-row clearing and
   NULL-instrument cleanup (:1041-1139).
4. **Re-fetch the recipient bodies every day.** That is ≤106 requests/day (§D3), but about
   410 MB/day of unchanged documents. The job already caches its derived cover evidence
   (`sec_cover_12b_fetches` / `_pairs`) for the same reason.

## 4. Design

### D0 — one verified-body helper

`raw_payload_retention.verified_body(conn, *, accession_number, document_kind,
fetch_text) -> VerifiedBody`. It returns exactly one of:

- `present(text)`: a live payload. It goes through `raw_filings.stored_body`, so a
  filing-index `source_url` (#3590) is refused rather than reused.
- `fetched(text)`: the row is swept, `source_url` is fetched, and the SHA-256 equals
  `payload_sha256`.
- `unavailable(reason)`, with reason one of:
  - `no_row`;
  - `no_source_url` or `index_url`;
  - `no_hash`;
  - `fetch_error`, a transport error or exception;
  - `not_found`, a 404 or empty body;
  - `mismatch`.

It never writes. It never restores a payload, because a restore would only be re-swept.

### D1 — `def14a_body` becomes housekept-and-negligible; its rewash becomes re-fetch-capable

- Add `def14a_body` to `raw_filings.SWEPT_DOCUMENT_KINDS`. `store_raw` then born-compacts
  every new body, and both writers already pass a non-NULL `source_url`
  (`def14a.py:159-163`, `def14a_ingest.py:1050-1057`).
- Move the `def14a_body` `ParserSpec` from `register_parser` to a second registry,
  `register_fetch_backed_parser` / `fetch_backed_specs()`. The specs and their
  `apply_fn` are unchanged. `run_rewash` serves both registries, and for a fetch-backed
  spec it takes an injected `fetch_text`, which is required.
  - The body comes from `verified_body`.
  - On `present` or `fetched`, the runner passes `dataclasses.replace(raw_doc,
    payload=text)` to the unchanged `_apply_def14a`.
  - On `unavailable` it counts `rows_failed` and never parses, so a `mismatch` needs
    adjudication exactly as in `rehydrate_raw_document`.
  - `scripts/rewash.py` wires the SEC provider and installs the cross-process
    `PostgresFloorGate` before fetching, following the
    `scripts/measure_3518_mdna_extraction_census.py:374` precedent and
    `app/jobs/__main__.py:1058`. The `sec_edgar.py:64-77` comment calling the gate
    per-process is stale, and the build corrects it.
- Partition tests:
  - `registered_specs()` and `SWEPT_DOCUMENT_KINDS` stay disjoint;
  - `fetch_backed_specs() ⊆ SWEPT_DOCUMENT_KINDS`;
  - the classification test unions the fetch-backed registry under "swept", not
    "re-read";
  - `retention_class_for("def14a_body")` is `"swept"`.
- **Cost:** a DEF 14A parser-version bump rewashed through this path fetches its stale
  cohort. Today that is 43,020 rows (C3): ≈ 79 min of the SEC budget if it ran alone at
  the 0.11 s floor (`rate_gate.py:23`), and about 35 GB downloaded. Today the same bump
  costs 0 fetches. That is the price of the reclaim, and it is paid only on a bump.

### D2 — the retroactive sweep verifies every body before nulling it

- **Scope is an explicit `(source, kind)` map, not the cross-product.** A new
  `VERIFIED_SWEEP_TARGETS: dict[ManifestSource, DocumentKind]` =
  `{"sec_def14a": "def14a_body", "sec_pre14a": "pre14a_body"}`.
  `SWEPT_MANIFEST_SOURCES` (`{sec_10k, sec_8k}`, the unverified #1014 arm) is unchanged.
  The manual job gains `verified: bool`. `verified=true` runs only the verified arm, so a
  verified run never touches the 10-K/8-K backlog that #2774 §7 flagged. `dry_run`
  stays the default.
- **Eligibility** (Appendix A, C2), per target:
  - `payload IS NOT NULL`;
  - `ingest_status IN ('parsed', 'tombstoned')` and `raw_status IN ('stored',
    'compacted')`;
  - not a filing-index URL.

  Excluded and left live:
  - split rows (`raw_status = 'absent'`, the #1014 rule);
  - manifestless rows;
  - `pending` and `failed` rows, which are mid-flight.

  Tombstoned rows are included for every reason. For "no Item 403 table", a
  `sec_rebuild` re-drain re-fetches. For the cap and missing-instrument reasons, the
  manifest parser never fetches. Those bodies stay recoverable via
  `rehydrate_raw_document` and D1's rewash, which is the reader that refreshes them.
- **Per row:**
  1. Read `(accession_number, source_url)` with a keyset cursor `accession_number >
     %(after)s ORDER BY accession_number LIMIT n`.
  2. Fetch through `SecFilingsProvider.fetch_document_text`, which strips the edge
     script, and hash the text.
  3. Run one statement that refuses a changed payload, a changed URL or a
     concurrently-swept row:

     ```sql
     WITH swept AS (
         UPDATE filing_raw_documents
            SET payload_sha256 = %(sha)s, payload_swept_at = NOW(), payload = NULL
          WHERE accession_number = %(acc)s AND document_kind = %(kind)s
            AND payload IS NOT NULL
            AND source_url = %(url)s
            AND encode(sha256(convert_to(payload, 'UTF8')), 'hex') = %(sha)s
         RETURNING accession_number)
     UPDATE sec_filing_manifest m SET raw_status = 'compacted'
       FROM swept s WHERE m.accession_number = s.accession_number AND m.raw_status = 'stored'
     ```

     A `mismatch`, `fetch_error`, `not_found` or 0-row update (a race) leaves the row
     live and is counted under its outcome, with every non-swept accession returned in
     the summary.
- **Concurrency contract**, adopted from `_SWEEP_BATCH_SQL`'s comment
  (`raw_payload_retention.py:91-103`): the manifest is not locked. A concurrent
  `sec_rebuild` flipping a row to `pending` is benign. The re-parse finds no stored body
  and re-fetches, and the body it would have reused was just proven identical to the
  fetch. The verifier's hash pin means `store_raw`, `rehydrate_raw_document` and a
  concurrent sweep can never pair bytes with a hash they were not checked against.
- **Termination and bounds.** One run is one keyset pass, bounded by `max_rows` (attempted
  rows) and `max_seconds` (wall clock, checked between rows). The summary returns the
  last cursor. The next run resumes from `after`. Failed rows are passed over, never
  re-attempted within a pass, so a failure cannot monopolise a batch. Done means one pass
  attempted every eligible row, and the residual is the listed unresolved set.
- **Runs in the jobs process** through the manual job `raw_payload_retention_sweep`.
  `MANUAL_TRIGGER_JOB_METADATA` declares `verified`, `max_rows`, `max_seconds` and
  `after`, and the wrapper validates and forwards them. The jobs process already holds
  the cross-process SEC gate.

### Why this is the agent's call, not the operator's

#2774 §7 (`2026-09-17-pre14a-body-retention-verdict.md:267-287`) presented an
unverified `pre14a_body` sweep for operator approval and left the tombstoned cohort to
"a separate decision". The operator's list in `.claude/CLAUDE.md` covers irreversible
data loss and settled-decision reversals; nothing else needs the operator.

- **Not irreversible.** Every swept body is byte-identical, at sweep time, to what
  EDGAR serves for its accession, and `rehydrate_raw_document` restores it from
  `source_url`. Parsed and tombstoned rows are equally recoverable, so the tombstone
  question #2774 left open has the same answer as the parsed one.
- **Not a settled-decision reversal.** It uses the reconciliation the partition test
  itself names.
- **Directed.** #3899 was filed on 2026-10-19 under the operator's request to minimise
  retained data, and running the sweep on dev is in its acceptance. #3117 requires a
  declared use for retained bytes, and no consumer reads these bodies from storage once
  D1 and D3 land.

The **residual is stated, not dismissed.** Verification proves reproducibility now. If
EDGAR later removes or rewrites a document, `rehydrate_raw_document` and the rewash fail
loud (`RawPayloadIntegrityError` / `mismatch`), and the original bytes are gone. That is
the contract every swept kind already runs under: 10-K/8-K `primary_doc`, prospectus,
tender, and `pre14a_body` since #2774. The PR records this paragraph so the operator can
contest it.

### D3 — the recipient job caches its Item 403 table evidence

- **New table** `def14a_item403_tables`:
  - columns `accession_number`, `parser_version`, `body_sha256`, `tables text[]`,
    `computed_at`;
  - PK `(accession_number, parser_version, body_sha256)`;
  - `tables` is `sec_def14a.item403_table_htmls(body)`, the parser's own table
    selection.

  The key carries the DEF 14A parser version, because table selection changes with the
  parser. Any change to `_select_item403_tables` must bump `_PARSER_VERSION_DEF14A`,
  which every DEF 14A parser change already does. The key also carries the body hash, so
  a re-stored body under the same version is a miss. The current hash is
  `payload_sha256` on a swept row, or the server-side hash of a live payload. The insert
  is `ON CONFLICT DO NOTHING`.
- **`load_row_locations` returns `available(locations)` or `unavailable(reason)`:**
  - No holdings rows for the accession: `available({})`, as today.
  - Cache hit: compute `share_locations` from the cached tables and the current holdings
    rows. Holdings are never cached, because they change on re-parse.
  - Miss: call `verified_body` with the job's injected `fetch_text`. On `present` or
    `fetched`, extract the tables, write the cache and compute.
  - `unavailable` from `verified_body`: return `unavailable`, and never cache it.
  - Extraction or location exception: `available({})`, logged, as today. Parsing is
    deterministic over verified bytes, and this outcome is not cached either.
- **Blocking point.** `compute_desired` calls `load_row_locations` before `decide()`.
  On `unavailable`, the accession goes to `blocked_accessions` and the loop
  `continue`s. That is the same path an unresolved cover takes (:805-807): the
  accession contributes no point-in-time, class-column or class-row decision; its
  instruments join `keep_instruments`; and all their stored rows in both ledgers are
  kept, with nothing extended. Today's `return {}` on a missing body, which deletes, is
  removed with it.
- **Population.** A body is read only for accessions with holdings rows under the
  population's instruments (`def14a_recipients.py:699-709`). That is ≤106 accessions
  across 72 issuers / 157 instruments (C8), and fewer in practice, because the accession
  must also be dated and have a resolved cover. Warm-cache steady state makes **zero
  body fetches**; cover fetches are unchanged. A parser bump costs one fetch per such
  accession.

### D4 — research scripts

The `scripts/census_def14a_*` / `scripts/ab_*def14a*` scripts read stored payloads and
will see the swept cohort as NULL. They are evidence for merged PRs and are not
rewritten. A future DEF 14A full-population A/B reads through `verified_body`, and pays
D1's fetch cost.

## 5. Tests

- **Partition (pure):**
  - `def14a_body` is `"swept"`;
  - `fetch_backed_specs() ⊆ SWEPT_DOCUMENT_KINDS`;
  - `registered_specs()` and `SWEPT_DOCUMENT_KINDS` are disjoint;
  - `VERIFIED_SWEEP_TARGETS` keys are valid `ManifestSource`s and values valid
    `DocumentKind`s;
  - `SWEPT_MANIFEST_SOURCES` is unchanged.
- **`store_raw` (pure):** it refuses a `def14a_body` with no `source_url`.
- **`verified_body` (db):** each outcome, including that a filing-index URL is never
  `present`.
- **Verified sweep (db):**
  - a match nulls the payload and flips `stored → compacted`;
  - a mismatch, fetch error or 404 leaves both untouched and is listed;
  - a payload or `source_url` changed between fetch and UPDATE is not nulled;
  - `pending`, `failed`, split, manifestless and index-URL rows are never selected;
  - `max_rows` and the cursor resume without re-attempting;
  - the `sec_10k` / `sec_8k` rows are untouched by `verified=true`.
- **Fetch-backed rewash (db):**
  - a swept `def14a_body` with a stale parser version, rewashed with a fake fetch
    returning the original body, writes holdings and comp rows identical to a
    stored-body rewash of the same accession, and bumps `parser_version`;
  - a mismatching fetch fails the row and writes nothing;
  - no `fetch_text` for a fetch-backed kind raises.
- **Re-drain (db):** a swept row parsed by `_parse_def14a` fetches exactly once and writes
  the same rows as a stored-body parse.
- **Recipients (pure + db):**
  - a hit makes no fetch;
  - a miss with a live body makes no fetch and fills the cache;
  - a miss with a swept body fetches, verifies and fills;
  - an old parser version or another `body_sha256` is a miss;
  - `unavailable` blocks the accession, keeps every stored row of its instruments in
    both ledgers and extends nothing;
  - nothing `unavailable` is ever cached.

## 6. Acceptance (corpus change; evidence and SHAs on the build PR)

All baselines are captured **before** anything is swept. Deploy enables born-compaction
only for bodies stored after it, and the re-drain reuse path (`stored_body` → present)
never calls `store_raw`.

1. **Dry run:** the verified arm's eligible rows and bytes per target equal C2 run at the
   same moment, with no other comparison.
2. **Recipient A/B, full population, before the sweep.** Use a read-only connection, in a
   rolled-back transaction, with `def14a_recipient_suppressions` paused for the window.
   Compare complete desired records (every `_LEDGER_COLS` / `_ROW_LEDGER_COLS` field) plus
   the `keep` and `keep_instruments` sets across three arms:
   - (a) `main`'s code;
   - (b) the new code, cold cache, live bodies;
   - (c) the new code, cold cache, with `verified_body` forced to fetch and verify.

   (a) = (b) = (c) exactly. After the sweep, (d) runs the new code with a cold cache and
   must equal (a). The cache filled by (b) is kept.
3. **Verified sweep, full population.** Run `max_rows=5000` passes until one complete pass
   has attempted every eligible row. Report swept / mismatch / fetch_error / not_found /
   race by kind × status × reason, list every unswept accession, and give the before and
   after live bytes.
4. **Smoke panel** (AAPL, GME, MSFT, JPM, HD).
   - Before the sweep: dump each panel instrument's complete `def14a_beneficial_holdings`
     and `def14a_exec_compensation` rows, and its
     `/instruments/<symbol>/ownership-rollup` DEF 14A figures.
   - After the sweep: rewash the panel's swept accessions through D1, then
     `sec_rebuild` `{"instrument_id": N, "source": "sec_def14a"}` each. Wait for pending
     = 0, and check every reset row reached `parsed`/`tombstoned` with the same error
     class as before.
   - Both dumps and the rollups must be identical.
5. **Cross-source:** one panel Item 403 figure checked against the filing on SEC EDGAR.
6. **Born-compaction live:** a `def14a_body` row first stored after the deploy has a
   NULL payload and a hash.
7. **Reclaim:** report the logical reclaim from item 3. Physical reclaim is not this
   ticket's: ordinary autovacuum makes the dead TOAST space reusable. Returning it to the
   OS takes a rewrite (`VACUUM FULL filing_raw_documents`), which is a separate supervisor
   maintenance decision (#3899 "Reclaiming the space").

## 7. Deploy order

1. Merge the build. Confirm the jobs child PID changed.
2. §6 item 2 arms (a)–(c), and §6 item 4's "before" dumps.
3. §6 item 1, then verified passes (§6 item 3).
4. §6 item 2 arm (d), then items 4–6.

## Appendix A — census queries

Run them with `PYTHONPATH=. uv run python` and a psycopg connection to `settings.database_url`.

```sql
-- C1: every row, by kind x manifest state x tombstone reason class
SELECT r.document_kind, coalesce(m.source,'(no manifest)'), coalesce(m.ingest_status,'-'),
       coalesce(m.raw_status,'-'),
       CASE WHEN m.error LIKE 'no beneficial-ownership table%' THEN 'no_403_table'
            WHEN m.error = 'latest-N primary cap' THEN 'cap'
            WHEN m.error = 'missing instrument_id' THEN 'no_instrument'
            WHEN m.error IS NULL THEN '-' ELSE 'other' END,
       count(*), count(*) FILTER (WHERE r.payload IS NOT NULL),
       coalesce(sum(r.byte_count) FILTER (WHERE r.payload IS NOT NULL), 0)
  FROM filing_raw_documents r LEFT JOIN sec_filing_manifest m USING (accession_number)
 WHERE r.document_kind IN ('def14a_body','pre14a_body')
 GROUP BY 1,2,3,4,5;
-- C2: D2 eligibility (before the index-URL exclusion, which C3 counts)
SELECT r.document_kind, m.ingest_status, count(*), sum(r.byte_count)
  FROM filing_raw_documents r JOIN sec_filing_manifest m USING (accession_number)
 WHERE ((r.document_kind = 'def14a_body' AND m.source = 'sec_def14a')
     OR (r.document_kind = 'pre14a_body' AND m.source = 'sec_pre14a'))
   AND r.payload IS NOT NULL
   AND m.ingest_status IN ('parsed','tombstoned') AND m.raw_status IN ('stored','compacted')
 GROUP BY ROLLUP (1,2);
-- C3: rewash cohort at def14a-v16, and recovery-locator shape
SELECT count(*) FILTER (WHERE parser_version IS DISTINCT FROM 'def14a-v16'),
       count(*) FILTER (WHERE source_url IS NULL),
       count(*) FILTER (WHERE source_url LIKE '%-index.htm%')
  FROM filing_raw_documents WHERE document_kind = 'def14a_body';
-- C4: populated accessions the re-drain cannot refresh
SELECT m.error, count(DISTINCT m.accession_number) FROM sec_filing_manifest m
 WHERE m.source = 'sec_def14a' AND m.ingest_status = 'tombstoned'
   AND m.error IN ('latest-N primary cap','missing instrument_id')
   AND (EXISTS (SELECT 1 FROM def14a_beneficial_holdings h WHERE h.accession_number = m.accession_number)
     OR EXISTS (SELECT 1 FROM def14a_exec_compensation c WHERE c.accession_number = m.accession_number))
 GROUP BY 1;
-- C5: physical sizes
SELECT pg_size_pretty(pg_total_relation_size('filing_raw_documents')),
       pg_size_pretty(pg_database_size(current_database()));
-- C6: tombstoned accessions carrying exec comp
SELECT count(DISTINCT c.accession_number) FROM def14a_exec_compensation c
  JOIN sec_filing_manifest m USING (accession_number) WHERE m.ingest_status = 'tombstoned';
-- C7: suppressions by reason, both ledgers
SELECT reason, count(*) FROM def14a_recipient_suppressions GROUP BY 1;
SELECT reason, count(*) FROM def14a_recipient_row_suppressions GROUP BY 1;
```

C8 (the recipient population) needs the job's own population function:
`ids = [s.instrument_id for v in def14a_recipients.load_population(conn).values() for s
in v]`, then `WITH acc AS (SELECT DISTINCT accession_number FROM def14a_beneficial_holdings WHERE
instrument_id = ANY(ids)) SELECT count(*), sum(r.byte_count) FROM acc JOIN
filing_raw_documents r ON r.accession_number = acc.accession_number AND r.document_kind =
'def14a_body'`.
That gives 106 accessions and 409,695,538 bytes, across 72 issuers and 157 instruments.
