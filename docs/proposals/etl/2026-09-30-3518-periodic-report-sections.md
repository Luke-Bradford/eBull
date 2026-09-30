# `periodic_report_sections`: MD&A text producer for the fund-v1 trial (#3518)

Refs #3518, #3515, #3471. Status: spec, v3 (after Codex ckpt-1 rounds 1 and 2). Consumer: the fund-v1 `mdna` pack block,
whose interface is fixed by `docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md` §4 ("F" below).
This spec fills that interface. It changes no v1 or F term and touches none of v1's hashed `POLICY_MODULES` (F §0),
so it can land while v1 runs.

## 1. Problem

No store of periodic-report narrative exists beyond 10-K Item 1 (`instrument_business_summary_sections`). The
`sec_10q` manifest parser is a synth no-op because "narrative HTML has no v1 consumer" (sec-edgar skill §11.5 /
§11.5.1). F is that consumer: it shows the first 6,000 normalised characters of the target report's MD&A.

## 2. Source rule

Skill sections read first: `.claude/skills/data-sources/sec-edgar.md` §4 (10 req/s per IP across processes;
`sec_rate_gate`), §11.5 / §11.5.1 (`sec_10q` synth no-op); `.claude/skills/data-sources/edgartools.md` (10-K /
10-Q objects, version pinning).

- **What the section is.** 10-K Item 7 and 10-Q Part I Item 2 are both MD&A under **Reg S-K Item 303** (303(b)
  annual, 303(c) interim). A 10-Q's Part II also has an Item 2 (unregistered sales), so the 10-Q key carries the
  Part.
- **Captions.** **Exchange Act Rule 12b-13**: a report "shall contain the numbers and captions of all items of the
  appropriate form", and the text of an item may be omitted only where the form or a rule permits, in which case
  the item number and caption still appear. The MD&A caption is "Management's Discussion and Analysis of Financial
  Condition and Results of Operations". §4.3 uses this: text the accessor returns as the item, with no trace of
  the caption at its start, is not demonstrably the item. The rule establishes what a *filing* contains, not what
  a parser returns; §4.3's window and variants are by construction, and §8.3 measures their effect.
- **Incorporation by reference.** Form 10-K **General Instruction G(2)** lets Item 7 be incorporated from the annual
  report to security holders, filed as an exhibit under **Reg S-K Item 601(b)(13)**; **Rule 12b-23** allows it "in
  answer or partial answer to any item" of any report. Such an Item 7 is a pointer under its caption (JPM 10-K
  `0001628280-26-008131`, 396 characters). The producer records it as filed and **does not follow exhibits** (F
  §10). A pointer is indistinguishable from a short MD&A by structure; F's 1,000-character floor keeps a short one
  from counting as coverage, and a long partial incorporation is a residual (§6).
- **Originals only.** **Rule 12b-15**: an amendment sets forth "the complete text of each item as amended", so it
  need not carry Item 7. The producer extracts `10-K`, `10-KT`, `10-Q`, `10-QT` only (F §2). Transition reports
  (**Rules 13a-10 / 15d-10**) are the same forms for a changed fiscal period and are parsed as their base form.
- **Which document.** The submission's primary document, `filing_events.primary_document_url` (the submissions
  index `primaryDocument`).
- **Extractor: edgartools 5.30.2 public item accessor, fed our HTML offline.** Reuse check in F §2 against our only
  in-house extractor (`business_summary.extract_business_sections`, Item 1, regex-bounded, patched per case).
  `TenK` / `TenQ` read only `form`, `html()`, `accession_number` and (legacy fallback) `base_dir` from the filing
  object, so a stub carrying our HTML runs with DNS and socket connects disabled (§8.2, §8.3: 0 network attempts).
  Accessor behaviour we inherit and do not patch: `TenK['Item 7']` tries Part I before Part II and accepts
  combined-item keys (`part_*_items_7_and_*`); `TenQ.get_item_with_part` returns a present-but-empty new-parser key
  without trying its fallbacks (→ `item_absent`); the legacy cleanup `rstrip(last_line)` strips a character set.
  §8.3 measures the outcomes; changing any of this is a new extractor id (§4.3), not a patch.

## 3. Table

`sql/NNN_periodic_report_sections.sql`:

| column | type / constraint |
| --- | --- |
| `row_id` | `bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY` |
| `instrument_id` | `bigint NOT NULL REFERENCES instruments` |
| `accession_number` | `text NOT NULL` (`filing_events.provider_filing_id`) |
| `section_id` | `text NOT NULL CHECK (section_id IN ('10-K:Item 7', '10-Q:Part I, Item 2'))` |
| `extractor` | `text NOT NULL` (§4.3) |
| `status` | `text NOT NULL CHECK (status IN ('extracted','item_absent','fetch_failed','parse_failed','invalidated'))` |
| `body` | `text`; `CHECK ((status = 'extracted') = (body IS NOT NULL))`, `CHECK (body IS NULL OR length(btrim(body)) > 0)` |
| `full_chars` | `integer`; `CHECK ((status = 'extracted') = (full_chars IS NOT NULL))`, `CHECK (full_chars > 0)`; equality with `normalise_mdna(body)` is the writer's, pinned by test (SQL cannot run F's normalisation) |
| `detail` | `text`; `CHECK ((status = 'extracted') = (detail IS NULL))` |
| `retryable` | `boolean NOT NULL`; `CHECK (NOT retryable OR status IN ('fetch_failed','parse_failed'))` |
| `invalidates_row_id` | `bigint REFERENCES periodic_report_sections(row_id)`; `CHECK ((status = 'invalidated') = (invalidates_row_id IS NOT NULL))`; `UNIQUE` |
| `source_url` | `text`; `CHECK (status = 'invalidated' OR coalesce(detail, '') = 'no_url' OR source_url IS NOT NULL)` |
| `source_text_sha256` | `text CHECK (source_text_sha256 ~ '^[0-9a-f]{64}$')`; `CHECK ((source_text_sha256 IS NULL) = (source_chars IS NULL))` |
| `source_chars` | `integer CHECK (source_chars >= 0)`; `CHECK (status <> 'extracted' OR (source_url IS NOT NULL AND source_text_sha256 IS NOT NULL))` |
| `fetched_at` | `timestamptz NOT NULL` — set by the INSERT to `clock_timestamp()` |

- **Append-only.** `BEFORE UPDATE OR DELETE` row trigger and `BEFORE TRUNCATE` statement trigger both raise. The
  migration grants no UPDATE/DELETE/TRUNCATE to any role beyond the owner (the dev stack runs as the owner, so the
  triggers are the enforcement).
- **Invalidation integrity** (a `BEFORE INSERT` trigger, since a CHECK cannot read another row): the target row
  exists, has the same `(instrument_id, accession_number, section_id, extractor)`, is not itself `invalidated`,
  and is not the new row. `UNIQUE (invalidates_row_id)` makes a row invalidatable once. Invalidation rows are
  written only by an operator script (`scripts/invalidate_periodic_report_section.py --row-id N --reason …`),
  never by the job.
- **Ordering.** The job is the only writer of non-invalidation rows, and the `sec_rate` lane runs at most one
  instance per job name (`app/jobs/sec_lane_gate.py`), so rows of one identity are inserted by one transaction at
  a time and identity order equals commit order among them. F's row choice depends on `row_id` order among rows
  of one identity and on invalidation *references*. An invalidation row's own position matters only when no
  usable extraction remains: F then reports the latest row's status, which is `invalidated` or the job's newer
  outcome, both true.
- **Knowledge time.** `fetched_at = clock_timestamp()` at INSERT; the job commits once per accession (§4.1), a
  few statements later. A row stamped `≤ as_of` but committed after F's snapshot is invisible to that run
  (conservative); no row can carry a stamp earlier than its own outcome.
- **Provenance.** `source_url` + `source_text_sha256` (sha256 of the decoded body's UTF-8 encoding, because the
  house client returns `response.text`; not a hash of the wire bytes) + `source_chars`. The HTML is not retained
  (`raw_filings` unused, no new `DocumentKind`). Residual, stated: the hash verifies a re-fetch decoded the same
  way; if EDGAR removes or changes the document, or the client's decoding changes, the extraction cannot be
  reproduced from the row.
- Index `(instrument_id, accession_number, section_id, extractor, row_id DESC)`.

## 4. Extraction

### 4.1 Selection

- **Job as-of** = the job transaction's `now()` at start. The selector runs in one REPEATABLE READ transaction that
  materialises the whole worklist (targets, due state, accession groups) and commits; per-accession write
  transactions follow.
- **Scope**: every `is_tradable` instrument on a `us_equity` exchange (the shortlist candidate universe F draws
  from; 4,263 names have ≥ 1 original periodic event, §8.1, an upper bound on names with a target).
- **Target per instrument** = F §4's rule evaluated at the job as-of: the newest original event (`10-K`, `10-KT`,
  `10-Q`, `10-QT`) with `created_at ≤` job as-of, exactly one such row per `(instrument_id, accession_number)`,
  `report_date` set and `≤` the job as-of's UTC date, ordered by (`report_date`, `filing_date`, accession)
  descending. The rule mirrors F exactly (including counting only original-form rows, finding 12); F slice 2
  must use the UTC date too.
- **Why only the current target.** F's pack runs live: its `as_of` is the claim time and a run is never retried for
  a past session (v1 §3). The pack's target is F's rule at `as_of`, the producer's is the same rule at its own
  as-of. They differ when an event is created between the two (F: `not_yet_extracted`) or when the rule's inputs
  change between them (a UTC date boundary, a corrected or duplicated event); whatever the target is at a
  producer run, it is extracted if due. A report that is not the target at any run is never extracted.
- **Due.** The target's *effective history* is its rows under the current extractor, minus invalidation rows and
  the rows they invalidate. The target is due when its effective history is empty (never attempted, or every
  attempt invalidated — the operator's repair path) or its latest effective row is `retryable` and the backoff
  for its attempt count (§4.4) has elapsed.
- **Work unit = accession.** Due instruments are grouped by accession. If the instruments whose target is that
  accession (due or not) disagree on form family or `primary_document_url`, the accession is skipped and counted
  `inconsistent_accession`. One
  fetch and one parse per accession; one row per **due** instrument of it (an instrument whose own state is not due
  gets nothing), all rows of an accession in one transaction.
- **Order and cap.** Accessions ordered by (every due instrument has an empty effective history first, then the
  accession's max `filing_date` desc, then accession); the first
  `MAX_ACCESSIONS_PER_RUN` are processed while the run is inside `MAX_RUN_SECONDS`; the rest are counted
  `deferred_by_cap`. Newest-first serves the live trial; a retry-exhausted backlog cannot starve new reports.

### 4.2 Fetch

`SecFilingsProvider.fetch_document_text` under the process's `sec_rate_gate` (installed by the jobs composition
root; sec-edgar §4). Transient failures are never terminal (prevention log #1698):

| outcome | row |
| --- | --- |
| `primary_document_url` NULL | `fetch_failed`, `detail='no_url'`, retryable (the URL may be backfilled) |
| `None` (404/410) | `fetch_failed`, `detail='missing'`, not retryable |
| `""` or whitespace-only 200 | `fetch_failed`, `detail='empty'`, retryable |
| raised `httpx.HTTPError` subclass (429/5xx/other 4xx/timeout/transport, after the client's retries) | `fetch_failed`, `detail='<class>[:<status>]'`, retryable |
| any other exception | not caught: the run fails (a code or config defect, visible as a failed job) |
| body longer than `MAX_SOURCE_CHARS` | `parse_failed`, `detail='too_large'`, not retryable |

A non-empty 200 that is not the filing (an error page) reaches the parser and ends as `item_absent` or
`caption_absent`, which are terminal: a transient error page served as 200 is the one transient that can be
recorded as terminal (residual; the repair is invalidation). The client downloads the body into memory before the
size check. One accession is in flight at a time: memory is bounded by one document in the parent, its copy in
the child, the parse tree and the returned text.

### 4.3 Parse

- **Isolation.** One long-lived child process (`multiprocessing`, spawn) with DNS and socket connects disabled,
  fed one document at a time over a pipe. The parent waits `PARSE_TIMEOUT_S`; on timeout it **terminates** the
  child (`Process.terminate`, then `kill` after 5 s) and starts a new one. Outcomes, in precedence order: timeout →
  `parse_failed`, `detail='timeout'`, retryable; child death → `detail='worker_died'`, retryable; `MemoryError`
  anywhere → `detail='memory'`, retryable. Retryable parse failures share the fetch backoff (§4.4).
- **Accessor.** 10-K family: `TenK(stub)['Item 7']`; 10-Q family: `TenQ(stub).get_item_with_part('Part I',
  'Item 2', markdown=False)`; the stub's `form` is the base form. `None` or whitespace → `item_absent`. Any
  other exception inside the accessor → `parse_failed`, `detail='<class>: <message[:200]>'`, not retryable
  (deterministic for the same text and extractor).
- **Normalisation** is F §4's, in F's stated order, implemented once in the producer module
  (`normalise_mdna`) and imported by F slice 2: horizontal whitespace (Unicode whitespace other than line
  separators) → one space each; strip remaining control characters (Unicode category Cc) except `\n`; collapse
  runs of spaces; collapse three or more newlines to two; trim. `full_chars` = normalised length.
- **Caption rule.** Collapse all whitespace in the normalised text to single spaces; if the census script's
  `CAPTION` regex (moved verbatim into the producer module: `discussion\s*(and|&|&amp;)\s*analysis` or
  `financial\s+condition\s*(and|&|&amp;)\s*results\s+of\s+operations`, case-insensitive, no word boundaries)
  finds nothing in the first 1,000 characters → `parse_failed`, `detail='caption_absent'`, not retryable. Otherwise
  `extracted`, `body` = the accessor text as returned. Basis: §2 (Rule 12b-13) for the caption's presence; the
  window and variants are by construction. It establishes only that a caption phrase occurs in the
  first 1,000 characters: not that it is a heading, nor the right item, nor the end; a TOC or pointer carrying the
  phrase passes (§6).
- **Extractor id** = `edgartools==<installed version>/mdna-<MDNA_EXTRACTOR_REVISION>`. `MDNA_EXTRACTOR_REVISION`
  is an integer in the producer module, bumped by any change to the accessor call, stub, normalisation, caption
  rule or outcome mapping; a test pins the sha256 of the whole extraction module's source (functions, regexes,
  constants) against the revision, so an edit without a bump fails CI (the v1 policy-hash pattern). Transitive
  dependencies (lxml etc.) are pinned by `uv.lock` but not in the id: a lock change that alters output is a
  residual, caught only by re-running the census. A new id makes every target due again; old rows stay.
- **Freeze hand-off.** F freezes the id it reads. Until F's declaration exists nothing consumes the table. F slice
  3's policy guard must refuse an edgartools pin change or a revision bump while a fund-v1 declaration is
  non-terminal; this spec records that as a slice-3 requirement, and the job logs its extractor id on every run.

### 4.4 Bounds (by construction; constants in the job module)

| constant | value | basis |
| --- | --- | --- |
| `MAX_ACCESSIONS_PER_RUN` | 500 (distinct accessions fetched) | > 2× the largest measured day (219 new original events, 45 days) |
| `MAX_RUN_SECONDS` | 2,700 | measured from the job's own start: no new accession after 45 minutes. No completion deadline is claimed or needed: a row committed after F's snapshot is invisible to that run (`not_yet_extracted`), never inconsistent |
| `PARSE_TIMEOUT_S` | 300 | ~2.7× the measured max, 112.9 s (§8.3) |
| `MAX_SOURCE_CHARS` | 64,000,000 | ~1.6× the measured max, 40,848,703 (§8.3) |
| backoff (retryable rows) | attempt 1→1 d, 2→2 d, 3→4 d, 4→8 d, 5→16 d, ≥ 6→30 d | doubling capped at 30 d; `business_summary._next_retry_days` uses 1/7/30/365, whose one-year quarantine would silence a live trial's target |

Attempt count = the number of consecutive retryable rows for the identity under the current extractor, counting
back from the latest row until a non-retryable, `extracted` or invalidated row; an invalidation resets it.

## 5. Job

`sec_periodic_report_sections` on the `sec_rate` lane, daily at 22:30 UTC, plus manual trigger. Basis: over 60 days
original periodic events were created at 02:30–03:40 UTC (routine), 06:38–07:16 UTC (catch-up) and once 23:18–23:25
UTC (ad hoc backfill) (§8.1); F decides at 23:30. The daily volume (p50 7) is minutes of work. **Bootstrap**
(at most 4,263 targets, about two hours of parse, §8.3) runs as manual triggers until a run reports no due accession apart from
retryable rows inside their backoff and `inconsistent_accession`; it runs outside 22:30–23:30. The lane's per-name lock prevents a scheduled
and a manual instance overlapping.

Counters (instrument units unless stated): `in_scope`, `no_target`, `not_due_terminal`, `not_due_backoff`, `due`;
accession units: `accessions_due`, `inconsistent_accession`, `deferred_by_cap`, `fetched`; outcome rows by
`status` / `detail`; `rows_written`. Every branch that skips increments one of them (prevention log #1698 corollary).

## 6. Quality and residuals (findings 40, 44–51)

No published formulation exists for validating MD&A extraction boundaries; the caption rule is the only gate.
Everything below is measured and stated.

- **Start boundary** — gated by the caption rule; the census's rejected bodies are listed in §8.3 with their
  classification (wrong section vs MD&A opening with boilerplate). The rule does not measure accepted-body
  precision; §8.3's other signals are the evidence on the accepted side.
- **TOC shapes and pointers** — a TOC or pointer carrying the caption is `extracted`. §8.3 counts TOC-shaped bodies
  and how many reach the 1,000-character floor.
- **End boundary — not validated.** Known overruns: JPM 10-Q `0001628280-26-054343` (the consolidated statements
  and notes begin 43% into the body; JPM files statements after MD&A without an Item 1 heading between them), MSFT
  10-K `0001193125-26-323660` (ends in the signature block). §8.3 counts line-start next-item headings inside the
  body and inside the displayed 6,000 characters; a heading-less transition (JPM) escapes that count, so it is
  neither an upper nor a lower bound on overruns. **`full_chars` is the length of the extracted text, not of the
  MD&A**; F slice 2 must label it that way in the prompt.
- **Duplicated content** — §8.3 reports the excess-copy share of long paragraphs in the body and in the displayed
  prefix. Exact-paragraph matching misses reformatted or partial repeats.
- **Tables (finding 47)** — sections found by edgartools' TOC detector take text from the HTML between boundaries
  and ignore `TextExtractor` table options, so cells can run together ("Three Months EndedJune 30,
  2026Three Months Ended…", "Profit$4,558 $7.77 $2,818"). §8.3 counts bodies whose first 6,000 characters have ≥ 1
  and ≥ 5 such hits: a signal, not a bound (it misses other shapes and can match prose such as "US$100"). Normalisation cannot restore
  cell separators; a table-aware renderer is a new extractor id.
- **Accessor path** — §8.3 reports how many bodies equal the part-qualified new-parser section text and how many
  legacy-fallback warnings were logged; the remainder is unattributed (text equality does not prove the path). The v6.0 removal of the legacy fallback would be a new id with its own census.

## 7. Tests

- Pure (fast tier): the §4.2 outcome table row by row; §4.3 outcome mapping (`None`, whitespace, exception,
  caption present/absent incl. `&amp;` and split-line captions, caption at char 999 vs 1,001 after collapsing);
  `normalise_mdna` against F §4's rule (tabs, NBSP, CR, VT/FF, control chars, 3+ newlines, trim); extractor id
  and the revision source-hash pin; due predicate over every row history (none; each terminal status; retryable
  inside/past backoff; invalidated; older extractor); attempt counting incl. reset by invalidation; accession
  grouping incl. inconsistent metadata and partially-due instruments; ordering and both caps.
- Parse worker: untrimmed real primary documents stored under `tests/fixtures/sec/mdna/` (AAPL 10-K, GME 10-Q, and
  one each of the classes §8.3 found: fallback path, TOC-shaped, caption_absent wrong section, JPM-style overrun),
  asserting outcome, `full_chars` and the first 200 characters; a timeout test (a stub accessor that sleeps) that
  asserts the child is terminated and replaced; a network test asserting a DNS lookup inside the worker raises.
- DB (one module): every CHECK rejects its invalid combination; UPDATE, DELETE and TRUNCATE are refused; the
  invalidation trigger refuses cross-identity, self and double invalidation; the selector on seeded events
  (duplicate-event accession excluded, amendment excluded, newer report supersedes, `created_at` after job as-of
  excluded); per-accession transaction atomicity (a failure mid-accession writes no row for that accession).

## 8. Full-population verification

### 8.1 Scope and arrival (dev DB, 2026-09-30 ~16:30Z)

- Tradable `us_equity` instruments with ≥ 1 original periodic event: **4,263**. Of their 130,895 original periodic
  events, 0 lack `primary_document_url` (a snapshot; §4.2 still defines the NULL case). Transition forms in
  `filing_events` (all instruments): `10-KT` 6, `10-QT` 3.
- New original periodic events per day (tradable US equity, `created_at` date, last 45 days): p50 7, max 219.
- `created_at` UTC over 60 days: routine 02:30–03:40, catch-up 06:38–07:16, one ad hoc 2026-09-12 23:18–23:25 (219
  events). Query: `filing_events` originals grouped by `created_at::date` and hour, min/max time.
- These are finite windows. The per-run caps and time budget (§4.4) hold whatever the future volume; volume only
  changes how much is deferred.

### 8.2 Offline feasibility (golden panel)

Latest 10-K and 10-Q of AAPL, GME, MSFT, JPM, HD, fetched by the house client, parsed with sockets blocked; 10 of
10 parsed:

| filing | raw accessor chars |
| --- | --- |
| AAPL 10-K `0000320193-25-000079` / 10-Q `0000320193-26-000020` | 18,018 / 21,464 |
| GME 10-K `0001326380-26-000013` / 10-Q `0001326380-26-000055` | 37,086 / 38,501 |
| MSFT 10-K `0001193125-26-323660` / 10-Q `0001193125-26-191507` | 53,403 / 61,409 |
| JPM 10-K `0001628280-26-008131` / 10-Q `0001628280-26-054343` | 396 (G(2) pointer) / 601,221 (tail overrun, §6) |
| HD 10-K `0001628280-26-019436` / 10-Q `0001628280-26-058715` | 31,267 / 26,285 |

### 8.3 Extraction census

`PYTHONPATH=. uv run python -m scripts.measure_3518_mdna_extraction_census --out /tmp/c3518.jsonl --bodies /tmp/c3518`
(re-print with `--summarise-only`; metadata in `/tmp/c3518.meta.json`). Run: `as_of` 2026-09-30 16:35:51Z,
edgartools 5.30.2, 4 workers, wall 1,079 s. Population: **1,430 eligible names, all with a target; 1,424
accessions** (6 shared by two instruments; 0 of 6 with inconsistent form/URL across them, by a direct
`filing_events` query; 0 targets with a non-original `filing_events` row for the same accession created by
`as_of`). No document hit a timeout, so the census's timeout handling was not exercised. Fetch: 1,423 ok, 1 empty 200 (CUBI `0001488813-26-000095`). Parse
errors 0; network attempts 0.

| | 10-K family | 10-Q family |
| --- | --- | --- |
| parsed | 70 (all `10-K`) | 1,353 (all `10-Q`) |
| `extracted` / `caption_absent` / `item_absent` | 70 / 0 / 0 | 1,330 / 8 / 15 |
| text equals the part-qualified new-parser section / legacy fallback logged | 59 / 2 | 1,135 / 0 (remainder unattributed; includes the 15 `item_absent`) |
| extracted chars p10 / p50 / p90 / max | 29,630 / 51,303 / 74,844 / 166,903 | 29,957 / 54,157 / 101,933 / 601,000 |
| extracted below 1,000 chars | 0 | 5 |
| next-item line-start heading in body / in first 6,000 chars | 1 / 0 | 48 / 2 |
| excess duplicate share > 5%: body / first 6,000 chars | 0 / 0 | 23 / 0 |
| table-glue hits in first 6,000 chars: ≥ 1 / ≥ 5 | 9 / 3 | 163 / 41 |
| TOC-shaped: all / ≥ 1,000 chars | 0 / 0 | 4 / 3 |

Flagged bodies, each read:
- **caption_absent (8)**: wrong start 5 (3 not MD&A at all, 2 mid-MD&A) — ENS `0001628280-26-056208` (financial-statement note "2. Revenue
  Recognition"), GEV `0001996810-26-000148` ("NOTE 2. SUMMARY OF SIGNIFICANT ACCOUNTING POLICIES"), HIW
  `0000921082-26-000045` (balance sheet), DAR `0000916540-26-000023` and ACTG `0000934549-26-000034` (start mid-MD&A);
  MD&A opening with forward-looking boilerplate 3 — HCA `0001193125-26-321077`, UBSI `0001193125-26-339565`, WYY
  `0001654954-26-007588`. Rejections that are wrong starts: **5 of 8** (3 of 8 are text that is not MD&A).
- **TOC-shaped ≥ 1,000 (3)**: THG `0000944695-26-000014`, KEY `0001628280-26-052671`, PIPR `0001230245-26-000032` —
  real MD&A (79k–140k chars) opening with its own section index, which uses part of the displayed prefix.
- **next-item heading in the first 6,000 (2)**: BAC `0000070858-26-000394` (925 chars: the MD&A's own index only,
  below the floor) and KEY (its index lists "Item 3"); neither is an overrun.
- Sizes: source chars p50 2,126,560, p99 13,026,046, max 40,848,703. Parse seconds p50 0.92, p90 2.61, p99 12.37,
  max 112.9; sum 2,412 s over 1,423 documents, so the 4,263-name bootstrap is about two hours of single-worker parse.
- **Coverage**: eligible names with extracted, captioned text ≥ 1,000 normalised chars: **1,401 of 1,430 (98.0%)**,
  against F's per-run floor of 80%. The eligible count moves with quote freshness (1,410 at 16:08Z, 1,430 at 16:35Z).

### 8.4 Scope of the evidence

- The census population is F's **consumer** population (eligible names at one `as_of`), not the producer's
  4,263-name scope. The build PR's bootstrap re-runs the same summary over the stored rows of the whole scope
  (the job writes `full_chars`, `detail`; the census signals are recomputable from `body`), and records it.
- Annual targets are few in a late-September snapshot (most names' newest report is a 10-Q); the bootstrap summary
  is split by form family. Transition forms: 0 in the population; unmeasured.
- Definition of Done 8–12 bind the **build** PR: the golden-panel rows written by the job, at least one body per
  form family cross-checked against the filing on EDGAR (start and end of the MD&A), the bootstrap run and its
  counters, and the stored rows read back for the panel.

## 8a. Ckpt-1 disposition

**Round 0 (handed over from F's ckpt-1).** R1-26 bootstrap: §5 (no placeholder, manual batches). R1-27 amendments:
§2. R1-28 KT/QT: §4.3 (base form), §8.4 (unmeasured). R1-29 key vs extractor, R1-32 re-extraction: §4.3. R1-31
retry: §4.2, §4.4. R1-33 outages: §4.1 (state-based). R1-34 selection ambiguity: §4.1 (F's rule verbatim). R1-35
status/body: §3. R1-36, R2-48 provenance and row identity: §3. R1-37 bounds: §4.4. R1-38 multi-instrument: §4.1.
R1-39 table semantics: §3. R1-40, R2-51 correctness and quality: §4.3, §6, §8.3. R1-42 risk factors: §9. R2-43
tie ordering: §3. R2-47 tables: §6.

**Round 1 (71 findings).**
- §3: 1 (body NULL unless extracted), 2, 3 (invalidation trigger + UNIQUE), 4 (invalidated → due), 5 (ordering
  argument: identity order among job rows; invalidation by reference), 6 (TRUNCATE trigger; grants), 7 (CHECKs),
  8 (`clock_timestamp()`, per-accession commit), 9 (renamed `source_text_sha256`, stated), 10 (residual stated).
- §4.1: 11 (job as-of, UTC date), 12 (rebutted: F counts original-form rows only, and the producer must match F;
  census counts targets with a non-original row for the same accession: §8.3), 13, 14 (live-only argument stated;
  historical targets are never read), 15 (`inconsistent_accession`), 16, 17 (due instruments only; one
  transaction per accession), 18 (ordering), 19 (cap in accessions).
- §4.2: 20 (`no_url` row), 21 (empty → retryable), 22 (caption rule rejects error pages; stated), 23 (only
  `httpx.HTTPError` caught; others fail the run), 30 (one document in memory).
- §4.3: 24 (timeout / worker death / memory retryable), 25 (residual: EDGAR document change not detected), 29
  (terminate + replace), 34 (revision constant + source-hash pin), 35 (slice-3 requirement), 36 (normalisation
  per F, census re-run with it), 37, 38 (census and production apply the rule to the same normalised text), 39, 40
  (§2 wording: the rule is about filings; window by construction), 41, 43 (stated: start-only validation), 42
  (`&` variants on both halves; census counts), 52–54 (accessor behaviour stated, not patched).
- §4.4/§5: 26, 27 (citation fixed; one schedule), 28 (attempt count), 31 (`MAX_RUN_SECONDS`), 32 (bootstrap
  outside the decision window; termination rule), 33 (counters).
- §6/§8: 44–51 (measured in §8.3; wording no longer claims bounds), 55–57 (§8.4), 58–68 (census rewritten: public
  accessor only, path evidence, raw text saved, F normalisation, per-name denominator from eligible names,
  missing-file count, `None` vs empty split, DNS + `connect_ex` blocked with attempt count, metadata file, bounded
  pending queue and per-document timeout), 63 (F's gate counts `mdna.text` ≥ 1,000; the displayed text is a
  whitespace cut; round 2 showed this is false for a text with no whitespace between characters 1,000 and 6,000,
  where F shows less; F's gate is authoritative and the census count is an upper bound for that case), 69, 70 (§7: untrimmed fixtures, edge cases listed), 71 (§8.4: one cross-check per family,
  bootstrap summary over the whole scope).

**Round 2 (40 findings).** §3: 1, 2 (provenance CHECKs), 3 (`full_chars > 0`; equality pinned by test), 5
(invalidation position stated). §4.1: 4 (effective history), 6 (worklist materialised), 7 (target-change wording),
8 (consistency over all instruments of the accession), 9 (ordering defined), 40 (upper bound). §4.2: 13 (memory
wording), 14 (transient error page residual). §4.3: 12 (precedence), 15 (wording), 16 (regex quoted verbatim), 17
(whole-module hash), 18 (transitive deps residual). §4.4/§5: 10 (termination), 11 (no deadline claimed). §6/§8.3:
19 (disposition 63 corrected), 23 (not a bound), 24 (wrong start vs not MD&A), 25 (unattributed remainder), 26
(direct query), 27 (census filter fixed; count still 0). Census script: 21 (heading index), 22 (unrounded
threshold), 33 (handlers removed), 37 (raw text preserved); re-summarised, §8.3 numbers unchanged. 20, 28–32,
34–36, 38 (census-only robustness: no timeout, error or missing file occurred in the run; prefix metrics use the
first 6,000 characters, not F's whitespace cut) and 39 (stored rows cannot reproduce the rejected-body reading;
the build PR's bootstrap summary covers accepted bodies and `detail` counts) stated, not changed.

## 9. Out of scope

Risk factors (F §4: 68,163 and 112,862 characters, 11–19× the text cap); following G(2) exhibits; amendments;
20-F / 40-F; retaining raw HTML; end-boundary repair; any reader or pack code (F slice 2).

## 10. Security model

Reads public SEC documents through the shared rate-limited client with the configured User-Agent; no credential,
no broker surface. Filer-written text is stored as data, never executed or rendered as HTML; F treats it as
untrusted input inside its escaped pack delimiters. The parse runs in a child process with DNS and socket connects
disabled and a hard timeout. The invalidation script is operator-run, writes one append-only row, and cannot alter
or delete existing rows.
