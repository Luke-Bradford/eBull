# #3899 slice 2 — sweep the DEF 14A and PRE 14A raw bodies

> Issue: **#3899** (child of the #3117 storage umbrella). Slice 1 (#3931 `63461266`,
> #3933 `3186fbf9`) made a re-fetch byte-reproducible again by stripping the SEC edge's
> injected `<script>` at both raw-body boundaries.
> Prior art: #1014 (sweep + `rehydrate_raw_document`), #1615 (born-compaction), #1617
> (the three-class partition), #2774 (`pre14a_body` verdict,
> `docs/specs/etl/2026-09-17-pre14a-body-retention-verdict.md`, "V" below).
>
> **Codex checkpoint 1.**
> - Round 1: 25 findings, NOT SOUND. The first draft deleted the DEF 14A rewash and
>   lost capabilities the re-drain lacks; the rewash is now kept and made
>   re-fetch-capable.
> - Round 2: 13 new findings, NOT SOUND. It confirmed that the second registry is a
>   legitimate reconciliation of #1617. It rejected the agent deciding the tombstoned
>   cohort, so that cohort is now gated (§4 "Authority").

## 1. Source rules

- **Retention class.** `docs/settled-decisions.md:108-114` ("Raw-payload retention,
  #1617"): a stored payload is legitimate only as exactly one of three classes:
  - **re-read**: "a rewash parser reads the stored body", registered in
    `rewash_filings.registered_specs()`;
  - **housekept-and-negligible**: born-compacted at source, rehydratable from
    `source_url` (`raw_filings.SWEPT_DOCUMENT_KINDS`);
  - **kept-and-negligible**.

  The partition is per `DocumentKind` and CI-enforced.
- **The sanctioned reconciliation of a rewash with a sweep.**
  `tests/test_raw_payload_retention.py:45-50` (`test_swept_kinds_have_no_rewash_parser`)
  says that registering a rewash parser for a swept kind "requires reconciling it with
  the sweep first (re-fetch-capable rewash, or de-scoping the kind here)". This spec takes
  the first option. A re-fetch-capable rewash does not **depend on retaining** a stored
  body: it reuses a live one if present and re-fetches otherwise. So the kind's payloads
  are justified as housekept, and the kind stays in exactly one class.
- **Identity of a filing's bytes.** Every EDGAR submission gets its own accession
  number, and a proxy is revised by a new submission (DEFR14A, DEFA14A) rather than by
  editing the accepted one. That is the premise on which `raw_filings.stored_body` reuses a
  body. It is **not** a guarantee that `www.sec.gov` serves the same bytes forever: slice 1
  found the edge rewriting HTML from 2026-09-20. No safety claim here rests on
  immutability. Every swept body is re-fetched and hash-matched first (D2).
- **Hash.** SHA-256 of the UTF-8 encoding of the stored TEXT, computed server-side
  (`raw_payload_retention` module docstring).

## 2. Premise, measured on the full population (dev DB, 2026-10-21 ~03:00Z)

Figures come from Appendix A.
- A row count counts `filing_raw_documents` rows.
- "Live" means `payload IS NOT NULL`.
- Bytes are `sum(byte_count)` over live rows: logical, before TOAST compression, with
  GB = 10⁹ bytes.
- `pg_size_pretty` sizes (C5) are physical and binary.

The table rows are C1's groups one-to-one. `source` is constant per kind, so it is not
shown.

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

- **D2's eligibility predicate**, exactly as the code will apply it (C2, including the
  filing-index URL exclusion), selects these cohorts:

  | kind | status | rows | GB |
  |---|---|---|---|
  | `def14a_body` | parsed | 3,627 | 16.22 |
  | `def14a_body` | tombstoned | 39,368 | 18.62 |
  | `pre14a_body` | parsed | 285 | 1.46 |
  | `pre14a_body` | tombstoned | 222 | 2.35 |

  None of the 33 filing-index-URL `def14a_body` rows (C3) is in it; they are all split or
  manifestless rows. The exclusion is a guard, not a subtraction.
- **Physical:** `filing_raw_documents` is 30 GB of the 88 GB dev DB (C5).
- **The tombstoned DEF 14A cohort is not dead weight.** Item 402(c) executive
  compensation runs on the ownership-tombstone path (`def14a_ingest.py` v3 note, #2086).
  3,248 tombstoned accessions carry `def14a_exec_compensation` rows (C6).
- **Populated accessions the re-drain cannot refresh** (C4) number 175 in all. Of these:
  - 172 carry holdings: 168 "latest-N primary cap" and 4 "missing instrument_id"
    tombstones;
  - 3 cap tombstones carry comp only.

  `_parse_def14a` returns before fetching for both reasons (`def14a.py:162-223`).
  `_apply_def14a` re-parses an accession with existing **holdings** and applies the cap
  only to its rescue branch (`rewash_filings.py:651-716`). So the 172 are refreshable by
  rewash. The 3 comp-only ones go through the rescue branch's gates, as they do today. The
  106 manifestless rows likewise get whatever rewash's resolution gives them today. This
  spec makes no coverage claim for them.

### Every payload reader of the two kinds

The readers were found by grepping `app/` for `def14a_body`, `pre14a_body`,
`stored_body(`, `read_raw(` and `require_payload()`.

| reader | behaviour on a NULL payload today | after this change |
|---|---|---|
| `manifest_parsers/def14a.py:238` `_parse_def14a` | `stored_body` → `None` → fetch | unchanged (born-compacts on store) |
| `manifest_parsers/def14a.py:567` prefetch gate | `None` → prefetch | unchanged |
| `def14a_ingest.py:1082` legacy ingester | stores, then parses the in-memory body | unchanged (born-compacts) |
| `rewash_filings.py:174` `run_rewash` → `_apply_def14a` | row skipped | **verified body** (D1) |
| `def14a_recipients.py:710` `load_row_locations` | returns `{}` | **cache + verified body** (D3) |
| `api/instruments.py:3738`, `ownership_drillthrough.py:566` | `COUNT(DISTINCT accession)` of rows | unchanged (rows survive) |
| `manifest_parsers/sec_pre14a.py` | writer only; born-compacted since #2774 | unchanged |
| `scripts/census_*` / `scripts/ab_*def14a*` research scripts | read `payload` in bulk | D4 |

`pre14a_body` has no payload reader in `app/`.

### The naive sweep would silently regress #2351

1. `load_row_locations` returns `{}` for a NULL body (`def14a_recipients.py:710-712`).
2. `compute_desired` then makes no class-column or class-row decision for that accession
   (:814-831).
3. The reconcile DELETEs stored suppressions that are neither desired nor kept
   (:869-900).

The rows at risk are those whose reason depends on body evidence: 10
`other_common_class_column` accession suppressions and 159 `other_common_class_row` row
suppressions (C7). That is a reason count, not a computed deletion set. §6 item 2
measures the actual sets.

## 3. Rejected alternatives

1. **Keep the bodies, compressed.** Measured on a 1% sample, so this is a cost estimate,
   not a safety claim. Python 3.14's stdlib `compression.zstd` was run on 424 bodies
   (`abs(hashtext(accession_number)) % 100 = 7`).
   - At level 9 it took 402 MB raw to 141 MB: 2.8× over raw, and 1.7× over the sample's
     current TOAST size of 242 MB.
   - Level 19 gains 2% at 20× the CPU.

   Extrapolated, roughly 40% of today's stored bytes would remain. Rejected.
2. **Keep only the multi-class issuers' bodies.** One `DocumentKind` would then sit in two
   retention classes, which the per-kind #1617 partition forbids.
3. **Delete the DEF 14A rewash and rely on `sec_rebuild` re-drain** (draft 1). It loses:
   - the refresh of the 172 populated out-of-cap accessions (C4);
   - the manifest-independent cohort resolution (`rewash_filings.py:651-679`);
   - the historical-sibling unions (:897-961, :984-989);
   - the holdings and comp regression brakes, stale-exec clearing, withheld-row clearing
     and NULL-instrument cleanup (:1041-1139).
4. **Re-fetch the recipient bodies every day.** That is up to 106 requests/day, about
   410 MB/day of unchanged documents (C8). The job already caches its derived cover
   evidence (`sec_cover_12b_fetches` / `_pairs`) for the same reason.

## 4. Design

### D0 — one verified-body helper

`raw_payload_retention.verified_body(conn, *, accession_number, document_kind,
fetch_text) -> VerifiedBody` returns exactly one of:

- **`present(text, sha256)`**: a live payload, read via `raw_filings.stored_body`, so a
  filing-index `source_url` (#3590) is refused rather than reused. `sha256` is computed
  in Python over exactly the text returned.
- **`fetched(text, sha256)`**: the row is swept, `source_url` was fetched, and the
  fetched text's SHA-256 equals the row's `payload_sha256`.
- **`unavailable(reason)`**, where `reason` is one of `no_row`, `no_source_url`,
  `index_url`, `no_hash`, `fetch_error` (transport error or exception), `not_found` (404
  or empty) or `mismatch`.

The helper never writes. It never restores a payload, because a restored payload would
only be swept again. Every consumer keys whatever it does on the returned `sha256`, the
hash of the exact bytes it parsed, and never on a separate read of the row.

### D1 — `def14a_body` becomes housekept-and-negligible; its rewash becomes re-fetch-capable

- **Born-compaction.** Add `def14a_body` to `raw_filings.SWEPT_DOCUMENT_KINDS`.
  `store_raw` then born-compacts every body stored from now on. Both writers already
  pass a non-NULL `source_url` (`def14a.py:159-163`, `def14a_ingest.py:1050-1057`).
  Update `raw_filings.py:108-115`: the comment calls the swept kinds write-only, and it
  should name re-fetch-capable rewash. Update `stored_body`'s "payload reader on a SWEPT
  kind is barred" note (:446-448) the same way.
- **Second registry.** Move the `def14a_body` `ParserSpec` from `register_parser` to
  `register_fetch_backed_parser` / `fetch_backed_specs()`, with the spec and its
  `apply_fn` unchanged. `run_rewash` serves both registries; a fetch-backed spec requires
  an injected `fetch_text` and raises without one. For each row:
  1. Take the body from `verified_body`. On `unavailable`, count `rows_failed` and never
     parse. A `mismatch` therefore needs adjudication, exactly as in
     `rehydrate_raw_document`.
  2. Otherwise pass `dataclasses.replace(raw_doc, payload=text)` to the unchanged
     `_apply_def14a`.
  3. **Revalidate before commit.** The parser-version bump for a fetch-backed kind is
     conditional on the row still being the verified bytes:
     `... WHERE accession_number = %s AND document_kind = %s AND (payload_sha256 =
     %(sha)s OR (payload IS NOT NULL AND encode(sha256(convert_to(payload,'UTF8')),'hex')
     = %(sha)s))`. Rowcount 0 means a concurrent ingest replaced the body after the
     fetch. The runner then rolls back the accession's whole transaction, typed changes
     included, and counts a `race`. `_apply_def14a` never commits; the runner does
     (`rewash_filings.py:186-227`).
- **Rate gate.** `scripts/rewash.py` wires the SEC provider and first installs the
  cross-process `PostgresFloorGate`. The precedents are
  `scripts/measure_3518_mdna_extraction_census.py:374` and `app/jobs/__main__.py:1058`.
  The comment at `sec_edgar.py:64-77`, which calls the gate per-process, is stale and is
  corrected.
- **Partition tests.**
  - `registered_specs()` and `SWEPT_DOCUMENT_KINDS` stay disjoint.
  - `fetch_backed_specs() ⊆ SWEPT_DOCUMENT_KINDS`.
  - The classification test counts the fetch-backed registry as "swept", not "re-read".
  - `retention_class_for("def14a_body")` is `"swept"`.
- **Cost.** A rewash run fetches every stale row that has no live body. Today the stale
  cohort is 43,020 rows and 32.96 GB, all of it live (C3b), so before any sweep its first
  run fetches nothing.
  - After the sweep, a version bump costs up to 43,020 fetches: about 79 minutes of the
    SEC budget if it ran alone at the 0.11 s floor (`rate_gate.py:23`), and about 33 GB
    downloaded.
  - Rows that `apply_fn` skips (returns `False`) or fails stay stale. Each re-run fetches
    them again; the runner already reports them as `rows_skipped` / `rows_failed`, and
    `since` still scopes a run.

  This is the price of the reclaim. Today it is zero.

### D2 — the retroactive sweep verifies every body before nulling it

- **Scope is an explicit `(source, kind)` map, not the cross-product.**
  - A new `VERIFIED_SWEEP_TARGETS: dict[ManifestSource, DocumentKind]` =
    `{"sec_def14a": "def14a_body", "sec_pre14a": "pre14a_body"}`.
  - `SWEPT_MANIFEST_SOURCES` (`{sec_10k, sec_8k}`, the unverified #1014 arm) is unchanged.
  - The manual job gains `verified: bool`; `verified=true` runs only the verified arm. A
    verified run therefore never touches the 10-K/8-K backlog that V §7 flagged.
  - `dry_run` stays the default.
- **Eligibility** (C2): every condition below holds.
  - `payload IS NOT NULL`;
  - `raw_status IN ('stored', 'compacted')`;
  - `ingest_status = ANY(%(statuses)s)`;
  - `source_url` is not a filing-index URL (`sec_manifest.is_filing_index_url`).

  `statuses` is a job param that defaults to `['parsed']`; `tombstoned` is accepted only
  when named explicitly (see "Authority"). Never eligible: split rows (`raw_status =
  'absent'`, the #1014 rule), manifestless rows, and `pending`/`failed` rows, which are
  in flight.
- **Per row:** the run reads `(accession_number, source_url)` in keyset order
  (`accession_number > %(after)s ORDER BY accession_number LIMIT n`). It fetches through
  `SecFilingsProvider.fetch_document_text`, which strips the edge script, and hashes the
  text. It then runs the existing three-CTE shape (`_SWEEP_BATCH_SQL`,
  `raw_payload_retention.py:109-145`):

  ```sql
  WITH swept AS (
      UPDATE filing_raw_documents
         SET payload_sha256 = %(sha)s, payload_swept_at = NOW(), payload = NULL
       WHERE accession_number = %(acc)s AND document_kind = %(kind)s
         AND payload IS NOT NULL
         AND source_url = %(url)s
         AND encode(sha256(convert_to(payload, 'UTF8')), 'hex') = %(sha)s
      RETURNING accession_number),
  flagged AS (
      UPDATE sec_filing_manifest m SET raw_status = 'compacted'
        FROM swept s WHERE m.accession_number = s.accession_number AND m.raw_status = 'stored')
  SELECT accession_number FROM swept
  ```

  - **Success** means exactly one row returned from `swept`; an already-`compacted`
    manifest is correct as it stands.
  - **Zero rows** means "this invocation did not sweep" (`race`): the payload, URL or
    hash moved, or a concurrent sweep or `store_raw` got there first.
  - `mismatch`, `fetch_error` and `not_found` never reach the statement.

  Each outcome is counted, and every unswept accession is listed in the summary.
- **Concurrency contract**, adopted from `_SWEEP_BATCH_SQL`'s comment
  (`raw_payload_retention.py:91-103`). The manifest is not locked.
  - A concurrent `sec_rebuild` flipping a row to `pending` is benign. The re-parse finds
    no stored body and re-fetches, and the body it would have reused was just proven
    identical to the fetch.
  - The `source_url` + hash pin means the sweep never pairs a hash with bytes it did not
    check.
  - `rehydrate_raw_document` already pins its restore to `payload_sha256`
    (:318-347), so a restore racing a sweep either sees a NULL payload with the matching
    hash, or finds the row moved on and writes nothing.
- **Termination and bounds.** One run is one keyset pass.
  - **`max_rows`** bounds the rows attempted.
  - **`max_seconds`** is a *soft* limit, checked before each row: no new row starts after
    it. The overrun is bounded by one fetch (the provider's HTTP timeout plus one
    rate-gate wait) and one single-row statement.
  - The summary returns the last cursor, and the next run resumes from `after`.
  - Failed rows are passed over and never re-attempted within a pass.
  - "Done" means one pass attempted every eligible row. The residual is the listed
    unswept set.
- **Runs in the jobs process** through the manual job `raw_payload_retention_sweep`,
  which already holds the cross-process SEC gate. `MANUAL_TRIGGER_JOB_METADATA`
  declares `verified`, `statuses`, `max_rows`, `max_seconds` and `after`; the wrapper
  validates them and forwards them.

### Authority — what this ticket sweeps, and what waits for the operator

- **Parsed cohorts (3,912 rows / 17.68 GB logical): swept by this ticket.** #3899
  ("Build" step 2 and "Acceptance") orders exactly this: "add both kinds to the sweep, on
  parsed accessions only, with the sha256 recorded first", then "run the sweep on dev".
  It was filed on 2026-10-19 under the operator's request to minimise retained data.
- **Tombstoned cohorts (39,590 rows / 20.96 GB logical): not swept until the operator
  says so.** #3899's text excludes them, and V §7 (:280-282) reserved them for "a
  separate decision".
  - Verification proves that each body is recoverable from EDGAR **at verification
    time**. It cannot prove EDGAR will keep serving it (V:262-264). Deleting our copy is
    therefore irreversible in that tail, and irreversible data loss is on the operator's
    list in `.claude/CLAUDE.md`.
  - The build ships the mechanism with `statuses=['parsed']` as the default.
  - The question goes on #3899 as one sentence, with the evidence, a recommendation
    (sweep: the same verified contract as the parsed cohort, with no consumer depending
    on a retained body after D1/D3) and the safe default (keep them).
- **Born-compaction of new bodies is not gated.** It deletes nothing stored, which is the
  #2774 precedent for `pre14a_body` (V:24-27, rubric §3.3). It applies equally to
  bodies that later tombstone (V:153-155, consequence 3).

### D3 — the recipient job caches its Item 403 table evidence

- **New table `def14a_item403_tables`.**
  - Columns: `accession_number`, `parser_version`, `body_sha256`, `tables text[]`,
    `computed_at`; PK `(accession_number, parser_version, body_sha256)`.
  - `tables` holds `sec_def14a.item403_table_htmls(body)`, the parser's own table
    selection.
  - `body_sha256` is the `sha256` that `verified_body` returned for exactly the bytes
    the tables were extracted from (D0), so a cache row can never name other bytes.
  - `parser_version` is `_PARSER_VERSION_DEF14A`, because table selection changes with
    the parser. A change to `_select_item403_tables` must bump it, which every DEF 14A
    parser change already does.
  - The insert is `ON CONFLICT DO NOTHING`: identical key, identical content.
- **`load_row_locations` returns `available(locations)` or `unavailable(reason)`:**
  - No holdings rows for the accession: `available({})`, as today.
  - **Swept row:** look the cache up on the stored `payload_sha256`, before any fetch.
    A hit is trustworthy, because that hash names bytes that were verified when they
    were swept or born-compacted, and a new body re-hashes the row. On a miss,
    `verified_body` fetches. Its `fetched` sha equals the stored hash by construction.
  - **Live row:** `verified_body` returns `present(text, sha256)`, a single read plus a
    hash in Python. The cache is then looked up on that sha.
  - On a hit: compute `share_locations` from the cached tables and the current holdings
    rows. Holdings are never cached, because they change on re-parse.
  - On a miss: extract from the verified text, write the cache under its sha, and
    compute the same way.
  - `unavailable` from `verified_body` returns `unavailable` and is never cached.
  - An extraction or location exception returns `available({})` and is logged, as today
    (parsing is deterministic over verified bytes). It is not cached.
  - Warm steady state makes **zero body fetches**. Cover fetches are unchanged.
- **Blocking point.** `compute_desired` calls `load_row_locations` immediately after
  `resolve_cover` returns a non-`None` cover, and **before** `covers.append`, `decide()`
  and every `extend`.
  - On `unavailable`, the accession goes to `blocked_accessions` and the loop
    `continue`s. That is the same position and effect as the unresolved-cover path
    (`def14a_recipients.py:805-807`): no cover, point-in-time, class-column or class-row
    contribution.
  - The accession's instruments join `keep_instruments` (:833-845), and their stored rows
    in both ledgers are kept, with nothing extended.
  - Today's `return {}` on a missing body, which deletes, goes away with this change.
- **Population.** A body is read only for accessions with holdings rows under the
  population's instruments (`def14a_recipients.py:699-709`). Today that is 106
  accessions (C8, counted before the join to raw rows, all 106 of which have a live
  `def14a_body`), across 72 issuers / 157 instruments (C8, from `load_population`). Fewer
  reach it, because the accession must also be dated and have a resolved cover. A parser
  bump costs at most one fetch per such accession.

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
  - `VERIFIED_SWEEP_TARGETS` keys are valid `ManifestSource`s and its values valid
    `DocumentKind`s;
  - `SWEPT_MANIFEST_SOURCES` is unchanged.
- **`store_raw` (pure):** it refuses a `def14a_body` with no `source_url`.
- **`verified_body` (db):** every outcome; a filing-index URL is never `present`; and
  `sha256` is the hash of the returned text.
- **Verified sweep (db):**
  - a match nulls the payload and flips `stored → compacted`;
  - a match on an already-`compacted` manifest counts as swept;
  - a mismatch, fetch error or 404 leaves both untouched and is listed;
  - a payload or `source_url` changed between fetch and UPDATE is a `race` and is not
    nulled;
  - a rehydrate between fetch and UPDATE is a `race`;
  - `pending`, `failed`, split, manifestless and index-URL rows are never selected;
  - `tombstoned` is selected only when named in `statuses`;
  - `max_rows` plus the cursor resumes without re-attempting;
  - `verified=true` never touches `sec_10k` / `sec_8k` rows.
- **Fetch-backed rewash (db):**
  - a swept stale `def14a_body`, rewashed with a fake fetch returning the original body,
    writes holdings and comp rows identical to a stored-body rewash of the same accession,
    and bumps `parser_version`;
  - a mismatching fetch fails the row and writes nothing;
  - a body replaced between fetch and bump rolls back every typed change and counts a
    `race`;
  - no `fetch_text` for a fetch-backed kind raises.
- **Re-drain (db):** a swept row parsed by `_parse_def14a` fetches exactly once and writes
  the same rows as a stored-body parse.
- **Recipients (pure + db):**
  - a swept-row cache hit makes no fetch;
  - a miss with a live body makes no fetch and fills the cache;
  - a miss with a swept body fetches, verifies and fills it;
  - an old parser version or another hash is a miss;
  - `unavailable` blocks the accession before `covers.append`;
  - a reconcile test seeds both ledgers with a blocked and an unblocked sibling, and
    asserts that the blocked instrument's stored rows survive the apply while the
    unblocked one's are reconciled normally;
  - nothing `unavailable` is ever cached.

## 6. Acceptance (corpus change; evidence and SHAs on the build PR)

Baselines are captured **before the deploy**, from the base SHA (`3186fbf9` or the
merge-base at build time), so no new code has run against the corpus when they are
taken.

1. **Before deploy: recipient arm (a) and panel dumps.**
   - Arm (a) runs `compute_desired` from a worktree at the base SHA, inside a writable
     transaction that is then rolled back: cover-cache writes roll back too, and the
     production ledgers are never applied. It dumps complete desired records (every
     `_LEDGER_COLS` / `_ROW_LEDGER_COLS` field) plus `keep` and `keep_instruments` as JSON.
   - Panel dumps (AAPL, GME, MSFT, JPM, HD): every `def14a_beneficial_holdings` and
     `def14a_exec_compensation` row, the `/instruments/<symbol>/ownership-rollup` DEF 14A
     figures, and the stored `def14a_recipient_suppressions` /
     `def14a_recipient_row_suppressions` ledgers in full.
2. **After deploy, before any sweep: recipient arms (b) and (c).** Both run from the
   build SHA, each in its own rolled-back transaction with an empty
   `def14a_item403_tables`.
   - (b) uses live bodies.
   - (c) forces `verified_body` down the fetch-and-verify path.

   (a) = (b) = (c) exactly. Corpus writes between (a) and (b), such as a new proxy
   ingested, are listed by accession and excluded from the comparison only with that
   listing.
3. **Dry run**, then the verified sweep with `statuses=['parsed']`.
   - The dry run's eligible rows and bytes equal C2's parsed rows, run at the same moment.
   - Then `max_rows=5000` passes run until one complete pass has attempted every eligible
     row.
   - The report gives swept / mismatch / fetch_error / not_found / race by kind × status,
     lists every unswept accession, and states logical live bytes before and after.
4. **After the parsed sweep: recipient arm (d)** runs from the build SHA, cold cache, and
   must equal (a). Then the real job runs once. Its stored ledgers must equal item 1's
   ledger dump, with any listed corpus changes excepted.
5. **Fetch-backed rewash on the panel.**
   - Set the panel's swept accessions' raw `parser_version` to `def14a-v15`. That is an
     explicit stale-version fixture on dev; the rewash itself restores `def14a-v16`.
   - Run `scripts/rewash.py` for `def14a_body`, restricted with `since` or an accession
     list.
   - Assert: fetched = the number of accessions, applied = the expected count, failed = 0,
     race = 0, and every row is back at `def14a-v16`.
   - Compare against item 1's typed dumps **immediately**, then separately after a
     `sec_rebuild` `{"instrument_id": N, "source": "sec_def14a"}` per panel instrument has
     drained to pending = 0 with every row back in its prior terminal state and error
     class. Both comparisons must be identical, and the rollup figures unchanged.
6. **Cross-source:** one panel Item 403 figure checked against the filing on SEC EDGAR.
7. **Born-compaction live:** a `def14a_body` row first stored after the deploy has a
   NULL payload and a hash.
8. **Reclaim:** this ticket reports the **logical** reclaim from item 3 and the
   **physical** `pg_total_relation_size` before and after (C5). The physical figure is
   not expected to fall: autovacuum makes the dead TOAST space reusable, and returning
   it to the OS takes a rewrite (`VACUUM FULL filing_raw_documents`). That rewrite is a
   separate supervisor maintenance decision (#3899 "Reclaiming the space").
9. **Tombstoned cohorts:** they run only after the operator's answer on #3899. If the
   answer is yes, rerun item 3 with `statuses=['tombstoned']`, then item 4.

## 7. Deploy order

1. §6 item 1 (base SHA, pre-deploy).
2. Merge the build. Confirm the jobs child PID changed.
3. §6 item 2, then item 3 (parsed), then items 4–8.
4. Item 9 when the operator answers.

## Appendix A — census queries

Run with `PYTHONPATH=. uv run python` against `settings.database_url`.

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
-- C2: D2 eligibility; the filing-index exclusion is applied in Python with
-- sec_manifest.is_filing_index_url(source_url), the production predicate
SELECT r.document_kind, m.ingest_status, r.accession_number, r.byte_count, r.source_url
  FROM filing_raw_documents r JOIN sec_filing_manifest m USING (accession_number)
 WHERE ((r.document_kind = 'def14a_body' AND m.source = 'sec_def14a')
     OR (r.document_kind = 'pre14a_body' AND m.source = 'sec_pre14a'))
   AND r.payload IS NOT NULL
   AND m.ingest_status IN ('parsed','tombstoned') AND m.raw_status IN ('stored','compacted');
-- C3: recovery-locator shape over every def14a_body row (pre14a_body: 0 index URLs)
SELECT count(*) FILTER (WHERE source_url IS NULL),
       count(*) FILTER (WHERE source_url LIKE '%-index.htm%')
  FROM filing_raw_documents WHERE document_kind = 'def14a_body';
-- C3b: the rewash cohort at def14a-v16 — rows, logical bytes, live rows
SELECT count(*), coalesce(sum(byte_count),0), count(*) FILTER (WHERE payload IS NOT NULL)
  FROM filing_raw_documents
 WHERE document_kind = 'def14a_body' AND parser_version IS DISTINCT FROM 'def14a-v16';
-- C4: populated accessions the re-drain cannot refresh, split holdings vs comp-only
SELECT m.error,
       count(DISTINCT m.accession_number) FILTER (WHERE EXISTS (
           SELECT 1 FROM def14a_beneficial_holdings h WHERE h.accession_number = m.accession_number)),
       count(DISTINCT m.accession_number) FILTER (WHERE NOT EXISTS (
           SELECT 1 FROM def14a_beneficial_holdings h WHERE h.accession_number = m.accession_number)
         AND EXISTS (SELECT 1 FROM def14a_exec_compensation c WHERE c.accession_number = m.accession_number))
  FROM sec_filing_manifest m
 WHERE m.source = 'sec_def14a' AND m.ingest_status = 'tombstoned'
   AND m.error IN ('latest-N primary cap','missing instrument_id')
 GROUP BY 1;
-- C5: physical sizes (binary units)
SELECT pg_size_pretty(pg_total_relation_size('filing_raw_documents')),
       pg_size_pretty(pg_database_size(current_database()));
-- C6: tombstoned accessions carrying exec comp
SELECT count(DISTINCT c.accession_number) FROM def14a_exec_compensation c
  JOIN sec_filing_manifest m USING (accession_number) WHERE m.ingest_status = 'tombstoned';
-- C7: suppressions by reason, both ledgers
SELECT reason, count(*) FROM def14a_recipient_suppressions GROUP BY 1;
SELECT reason, count(*) FROM def14a_recipient_row_suppressions GROUP BY 1;
```

C8 is the recipient population. `pop = def14a_recipients.load_population(conn)` gives
72 issuers (`len(pop)`) and 157 instruments (`ids = [s.instrument_id for v in
pop.values() for s in v]`). Then:

```sql
WITH acc AS (SELECT DISTINCT accession_number FROM def14a_beneficial_holdings
              WHERE instrument_id = ANY(%(ids)s))
SELECT count(*), count(r.accession_number), count(r.payload), coalesce(sum(r.byte_count),0)
  FROM acc LEFT JOIN filing_raw_documents r
    ON r.accession_number = acc.accession_number AND r.document_kind = 'def14a_body';
```

That returns 106 accessions, 106 with a raw row, 106 live, and 409,695,538 bytes.
