# ETL coverage + freshness model — spec

**Status:** draft v3 (post-Codex re-review 2026-05-04).
**v1 → v2 changes:** 8 Codex findings applied.
**v2 → v3 changes:** 5 additional Codex findings applied — see "v3 changes" below.

## v3 changes summary

1. **`ownership_*_observations` need an `ingested_at` (or monotonic `revision`) column.** v2 repair sweep predicate referenced `o.created_at` which doesn't exist on those tables — known_from is valid-time, not system-time. Migration #863 (extended) adds `ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` to every `ownership_*_observations` table and `record_*_observation` bumps it on every UPSERT (including parser-rewash re-INSERTs). Repair sweep keys on max-of-`ingested_at`.
2. **Targeted rebuild explicitly runs a discovery pass.** v2 said "trigger Mode 3 polling" but Mode 3 only consumes existing manifest rows; if the manifest never knew an accession existed, rebuild couldn't repair that gap. #871 now runs a full per-CIK history scan (recent array + `filings.files[]`) for the rebuild scope BEFORE handing work to the manifest worker.
3. **First-install fetches submissions.json once per CIK, fans out by source.** v2 implied `check_freshness` per (cik, source) — multiplies provider calls unnecessarily. Corrected to: one submissions.json fetch per CIK, parse all supported forms in the response, INSERT manifest rows per (accession, source).
4. **`subjects_due_for_poll` explicitly includes `state='unknown'`.** v2 partial index excluded `unknown`, so reset-by-rebuild rows could sit forever. The due contract now: `state IN ('unknown', 'current', 'expected_filing_overdue') AND expected_next_at <= NOW()`. Partial index covers all three states.
5. **Enum name aligned: `failed` everywhere.** v2 summary line referenced `fetch_error` which doesn't exist. Fixed.



## v2 changes summary

1. **Scheduler is subject-polymorphic, not always (instrument_id, source).** 13F-HR is filer-centric (BlackRock files; AAPL doesn't), so the scheduler row carries a `subject_type` (`issuer` | `institutional_filer` | `fund_series`) + `subject_id` plus a denormalised `cik`. `instrument_id` is non-null only when the subject is an issuer.
2. **Per-accession manifest split out from scheduler.** `data_freshness_index` becomes the poll scheduler only. New `sec_filing_manifest` table tracks every accession we know exists with `ingest_status`, `parser_version`, `raw_status`, `last_attempted_at`, `error` — answers "is filing X already on file" cleanly across amendments, failed tombstones, retries.
3. **submissions.json pagination acknowledged.** SEC docs (https://www.sec.gov/search-filings/edgar-application-programming-interfaces) confirm the main object holds recent filings; older filings live in `filings.files[]`. Steady-state polling uses the recent array; first-install drain + targeted rebuild + historical backfill must follow `filings.files[]` or use quarterly/daily indexes.
4. **Drain progress tracked separately from steady-state cursor.** `last_known_filing_id` is the steady-state newest-observed. Drain progress goes into the manifest (`ingest_status` per accession) — crash-resume picks up by scanning manifest rows where `ingest_status IN ('pending', 'fetch_error')`, NOT by reading the freshness index newest pointer.
5. **`last_polled_at` is nullable.** Rows seeded from tombstones get `state='unknown'`, `last_polled_outcome='never'`, `last_polled_at=NULL`. No fake timestamps.
6. **Rebuild explicitly resets `expected_next_at = NOW()` + `state = 'unknown'`** so the rebuild scope drains immediately rather than sitting in the future-poll queue.
7. **`check_freshness` takes `last_known_filing_id` as an argument.** Provider stays pure; DB lookup happens in the freshness service.
8. **Repair sweep uses observation `ingested_at` / monotonic revision**, not `known_from` (which is valid-time and doesn't capture re-ingests, parser-version rewashes, or amendments).


**Author:** Claude (Opus 4.7) on 2026-05-04 after operator audit of #840 sub-PR cycle.
**Trigger:** Operator flagged that current ETL hammers SEC unnecessarily (re-fetches data we already have; nightly periodic re-scans of unchanged data; no programmatic answer to "what do I have / what do I need").

## Problem statement

Today's ingest model is dumb-bulk:

- `sec_def14a_ingest` hits SEC for 100 untouched accessions every day; no per-CIK freshness check.
- `sec_insider_transactions_ingest` hits 500 newest filings every hour; no scope dedup.
- `ownership_observations_sync` re-scans 90 days of typed-table rows nightly even when nothing changed.
- `cusip_extid_sweep` runs daily even with zero candidates.
- Bootstrap-mode jobs run weekly "as a safety net" — perpetual bandwidth tax.
- No table answers "is filing X already on file" without a per-source bespoke join.
- No table answers "should I even ASK SEC if this CIK has new filings".

Result: every job hits SEC regardless of whether new data exists. New install vs steady-state are not distinguished. Targeted backfill ("rebuild AAPL from scratch") is not a first-class operation.

## Constraints

1. SEC fair-use cap: 10 req/s shared across the process.
2. SEC bulk archives ARE available — `Archives/edgar/Feed/`, `submissions.zip` (~5 GB), `companyfacts.zip` (~2 GB).
3. SEC RSS / `getcurrent` Atom feed lists every just-accepted filing across the entire SEC universe within seconds of acceptance.
4. SEC per-CIK `submissions.json` (≤50 KB) lists every filing for that CIK with `accession`, `filingDate`, `form`, `acceptedDateTime`. One call answers "what's new for this CIK".
5. SEC per-form quarterly indexes (`full-index/YYYY/QTRn/form.gz`) list every filing of a given form for a quarter — used by EdgarTools for backfill scoping.
6. Operator wants three modes: **first-install drain**, **targeted rebuild** (one CIK or one form), **steady-state polling** that pulls only new data.

## Reference: how EdgarTools handles this

Per investigation of `dgunning/edgartools`:

- **Bulk-zip-then-incremental** is the canonical pattern. EdgarTools downloads `submissions.zip` + `companyfacts.zip` once on first install (`download_edgar_data()`), then uses `getcurrent` Atom + per-CIK submissions.json + quarterly full-index for incremental.
- **Filesystem-based watermark** — `Path.exists()` per accession. No DB, no manifest. Identified gap; we shouldn't clone it.
- **No per-CIK rebuild API.** Identified gap; we should ship it as a first-class operation.
- **Token-bucket rate limiter** at 9 req/s default. Hishel HTTP cache (30s submissions, 30min indexes, infinite archives).
- **Hard 1000-filings-per-CIK cap** in EdgarTools' local store. We should avoid replicating this.

## Target model

### Two tables — scheduler + per-accession manifest

Codex review v2: the scheduler answers "should I ask SEC for this subject's source"; the manifest answers "is this specific accession already on file / parsed / failed". Two separate concerns, two tables.

#### `data_freshness_index` — poll scheduler (subject-polymorphic)

One row per `(subject_type, subject_id, source)` tracking when to next check.

```sql
CREATE TABLE data_freshness_index (
    subject_type            TEXT NOT NULL CHECK (subject_type IN (
        'issuer',                  -- subject_id = instrument_id; sources: form4, def14a, 10k, 10q, 8k, xbrl_facts
        'institutional_filer',     -- subject_id = institutional_filer.filer_id; source: 13f_hr
        'blockholder_filer',       -- subject_id = blockholder_filer.filer_id; source: 13d, 13g
        'fund_series',             -- subject_id = fund_series_id; source: n_port, n_csr (Phase 3)
        'finra_universe'           -- singleton; source: short_interest
    )),
    subject_id              TEXT NOT NULL,                   -- string for portability across PK types
    cik                     TEXT,                            -- denormalised for fast filtering
    instrument_id           INTEGER,                         -- non-null when subject_type='issuer'
    source                  TEXT NOT NULL CHECK (source IN (
        'sec_form4', 'sec_form3', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest'
    )),
    last_known_filing_id    TEXT,                            -- newest accession observed in steady-state
    last_known_filed_at     TIMESTAMPTZ,                     -- corresponding filed_at
    last_polled_at          TIMESTAMPTZ,                     -- nullable: NULL = never polled
    last_polled_outcome     TEXT NOT NULL DEFAULT 'never' CHECK (last_polled_outcome IN ('current', 'new_data', 'error', 'never')),
    new_filings_since       INTEGER NOT NULL DEFAULT 0,
    expected_next_at        TIMESTAMPTZ,                     -- predicted next-filing time
    next_recheck_at         TIMESTAMPTZ,                     -- explicit recheck for never_filed/error states
    state                   TEXT NOT NULL DEFAULT 'unknown' CHECK (state IN (
        'unknown',                  -- never polled
        'current',                  -- last poll = no new data + within expected cadence
        'expected_filing_overdue',  -- past expected_next_at without new
        'never_filed',              -- inferred from history; re-checked at next_recheck_at
        'error'                     -- last poll failed (rate limit, 404, parse miss)
    )),
    state_reason            TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subject_type, subject_id, source)
);

CREATE INDEX idx_freshness_due_for_poll
    ON data_freshness_index (expected_next_at, source)
    WHERE state IN ('current', 'expected_filing_overdue');

CREATE INDEX idx_freshness_recheck
    ON data_freshness_index (next_recheck_at)
    WHERE state IN ('never_filed', 'error');

CREATE INDEX idx_freshness_by_instrument
    ON data_freshness_index (instrument_id) WHERE instrument_id IS NOT NULL;
```

#### `sec_filing_manifest` — per-accession truth

```sql
CREATE TABLE sec_filing_manifest (
    accession_number        TEXT PRIMARY KEY,
    cik                     TEXT NOT NULL,
    form                    TEXT NOT NULL,                   -- '4', '13F-HR', 'DEF 14A', etc.
    source                  TEXT NOT NULL,                   -- maps to data_freshness_index.source
    subject_type            TEXT NOT NULL,                   -- which scheduler row 'owns' the freshness signal
    subject_id              TEXT NOT NULL,
    instrument_id           INTEGER,                         -- non-null when source is issuer-scoped
    filed_at                TIMESTAMPTZ NOT NULL,
    accepted_at             TIMESTAMPTZ,                     -- SEC accepted timestamp (when known)
    primary_document_url    TEXT,
    is_amendment            BOOLEAN NOT NULL DEFAULT FALSE,
    amends_accession        TEXT,                            -- self-reference when is_amendment
    ingest_status           TEXT NOT NULL DEFAULT 'pending' CHECK (ingest_status IN (
        'pending',     -- discovered but not fetched
        'fetched',     -- raw body downloaded
        'parsed',      -- typed-table rows + observations recorded
        'tombstoned',  -- intentionally not parseable (not-on-file / no-table / partial)
        'failed'       -- fetch / parse error; eligible for retry per backoff
    )),
    parser_version          TEXT,                            -- bumps trigger rewash
    raw_status              TEXT NOT NULL DEFAULT 'absent' CHECK (raw_status IN ('absent', 'stored', 'compacted')),
    last_attempted_at       TIMESTAMPTZ,
    next_retry_at           TIMESTAMPTZ,                     -- backoff for failed
    error                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_manifest_subject ON sec_filing_manifest (subject_type, subject_id, form, filed_at DESC);
CREATE INDEX idx_manifest_status_retry ON sec_filing_manifest (ingest_status, next_retry_at)
    WHERE ingest_status IN ('pending', 'failed');
CREATE INDEX idx_manifest_parser_version ON sec_filing_manifest (source, parser_version)
    WHERE ingest_status = 'parsed';  -- supports rewash by parser_version bump
CREATE INDEX idx_manifest_instrument ON sec_filing_manifest (instrument_id, form, filed_at DESC)
    WHERE instrument_id IS NOT NULL;
```

The manifest is the single source of truth for "do I have accession X". Every existing tombstone table (`def14a_ingest_log`, `institutional_holdings_ingest_log`, the tombstone bits in `unresolved_13f_cusips`, etc.) becomes a derived view of the manifest after the migration lands. Existing tombstones backfill into manifest on the migration's first run.

The `expected_next_at` cadence per source is hard-coded:

- Form 4: 24h after first known officer (event-driven; usually quarterly clusters)
- Form 3: 30 days after appointment trigger (very rare per CIK)
- 13D/G: 10 days after threshold cross (event-driven; rarely cyclic)
- 13F-HR: 45 days after quarter-end
- DEF 14A: 365 days from last filed_at
- N-PORT: 60 days after month-end
- 10-K: 60-90 days after fiscal year-end
- 10-Q: 40-45 days after quarter-end
- 8-K: 4 business days after triggering event (event-driven)
- XBRL facts: piggybacks on 10-K / 10-Q
- FINRA short interest: bimonthly settlement schedule

### Three operator modes

**Mode 1 — First-install drain.** Two paths:

```
POST /jobs/sec_first_install_drain/run            # default: in-universe-only
POST /jobs/sec_first_install_drain/run            # body: { "use_bulk_zip": true } for full SEC bulk
```

Default path (dev laptops, most operators): per-CIK submissions.json for every CIK in the tradable universe (~12k requests at 10 req/s = ~20 min). Cheap; precise; respects the universe scope. Codex review: bulk-zip is appropriate for production bootstrap or operator-explicit drain, NOT the default local path.

Bulk-zip path (production / operator-explicit): downloads `submissions.zip` (~5 GB) + `companyfacts.zip` (~2 GB) once, extracts per-CIK records into the manifest.

Steps either path:

1. Populate `data_freshness_index` rows for every `(subject, source)` pair that historical data implies.
2. For every CIK, walk full filing history (`recent` array + `filings.files[]` for older — Codex review caught that submissions.json paginates).
3. INSERT every accession into `sec_filing_manifest` with `ingest_status='pending'`.
4. Iterate manifest rows where `ingest_status='pending'`, fetch + parse + persist via the per-form pipeline, transitioning to `parsed` / `tombstoned` / `failed` per outcome.
5. Crash-resume: just re-run; iteration picks up at next pending row.

Idempotent — re-running is a no-op once every accession has a terminal status.

**Mode 2 — Targeted rebuild.** Operator-triggered clean sweep.

```
POST /jobs/sec_rebuild/run    body: { "instrument_id": 1701 }                     # all issuer-scoped sources for AAPL
POST /jobs/sec_rebuild/run    body: { "filer_cik": "0000102909", "source": "sec_13f_hr" }   # Vanguard's 13F history
POST /jobs/sec_rebuild/run    body: { "source": "sec_def14a" }                    # all DEF 14A universe-wide
POST /jobs/sec_rebuild/run    body: { "instrument_id": 1701, "source": "sec_def14a" }       # narrow
```

Steps:

1. Resolve scope to a set of `(subject_type, subject_id, source)` triples.
2. For every triple:
   - Reset scheduler row: `state='unknown'`, `expected_next_at=NOW()`, `last_known_filing_id=NULL`.
   - Set every manifest row in scope to `ingest_status='pending'` (NOT delete — preserves accession history; allows re-parse without losing the original).
3. Trigger Mode 3 polling against the scope; drains naturally because every row is pending + every scheduler row is past `expected_next_at`.

Codex review v2: explicit `expected_next_at = NOW()` reset on rebuild — without it, rebuild-resetted rows could sit in the future-poll queue and never drain.

**Mode 3 — Steady-state polling.** Three layers, cheapest first.

**Layer 1 — `getcurrent` Atom feed (every 5 min)** — fast lane for low-latency event-driven sources (8-K, Form 4, 13D/G).

```
GET https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom
→ list of (cik, accession, form, accepted) for the last ~24h
→ filter to CIKs in our universe + sources we care about
→ for each match: insert manifest row pending; downstream worker picks up
```

One HTTP request per 5-min cycle. Codex review v2: keep this as fast lane, NOT only lane — feed lossy on outage / very-old amendments.

**Layer 2 — Daily-index reconciliation (daily 04:00 UTC)** — completeness safety net.

```
GET https://www.sec.gov/Archives/edgar/daily-index/{YYYY}/QTR{q}/master.{YYYYMMDD}.idx
→ ~1 MB plain-text list of every filing accepted that day, all CIKs, all forms
→ filter to (cik IN universe) + (form IN sources we care about)
→ insert manifest rows missing from yesterday's getcurrent stream
```

Codex review v2 confirmed: daily index is cheaper than per-CIK polling for universe-wide "what changed today". One ~1MB download covers every CIK. Acts as a safety net for any filing the Atom feed missed.

**Layer 3 — Per-CIK submissions.json (scheduled per-CIK, only at predicted-next-filing windows)** — for amendments + historical backfill.

```
for each row in data_freshness_index WHERE expected_next_at <= NOW():
    delta = check_freshness(http, cik, source, last_known_filing_id=row.last_known_filing_id)
    update scheduler row outcome
    if delta.has_new:
        manifest INSERT … ON CONFLICT DO NOTHING for each accession
        # downstream worker picks up pending manifest rows
```

Layer 3 only runs when a scheduler row's `expected_next_at` is past — i.e. AAPL's DEF 14A poll fires once a year, not nightly. AAPL's 13F poll never fires (issuer subject; AAPL doesn't file 13F).

For pagination: when a CIK has more filings than fit in the recent array, layer 3 walks `filings.files[]` paths. This only matters for first-install / targeted rebuild — steady-state polling against the recent array catches every new filing.

**Manifest-driven worker** (separate from polling): scans `sec_filing_manifest WHERE ingest_status IN ('pending') AND (next_retry_at IS NULL OR next_retry_at <= NOW())`, fetches + parses each, transitions status. Bounded by SEC rate budget. Decouples discovery (poll) from materialisation (parse) — operator can throttle each independently.

### Eliminate periodic re-scan jobs

Current `ownership_observations_sync` re-reads typed tables daily. Replace with **write-through hooks**: every successful per-form ingest call records the observation + queues `refresh_*_current(instrument_id)` immediately. No nightly scan.

Add a **weekly self-healing repair sweep** that runs ONLY against rows where
`ownership_*_current.refreshed_at < (SELECT MAX(known_from) FROM ownership_*_observations WHERE instrument_id = ...)` — i.e. only instruments where drift exists. On a healthy install this finds zero rows and exits in <100ms.

### Definition of done

A change to ETL / parser code is not done until:

1. Lint + format + pyright + pytest pass locally.
2. Smoke-run against 3-5 known instruments (AAPL / GME / MSFT / JPM / HD) in dev DB.
3. Cross-checked against an independent reputable source for at least one fixture (gurufocus / marketbeat / EdgarTools golden file).
4. **Backfill executed** (NOT "queued for nightly"). For schema/parser changes affecting ownership: run `POST /jobs/sec_rebuild/run` with appropriate scope, verify operator-visible figure on rollup endpoint.
5. Operator-visible figure verified on the live chart.
6. PR description records the verification step + commit SHA.

This addition lands in `.claude/CLAUDE.md`.

## Tickets

### #863 — `sec_filing_manifest` table + backfill from existing tombstones

**Scope:**

- Migration adds `sec_filing_manifest` per the schema above.
- `app/services/sec_manifest.py` with:
  - `record_manifest_entry(conn, accession, *, cik, form, source, subject_type, subject_id, instrument_id, filed_at, ...)` — UPSERT.
  - `transition_status(conn, accession, *, ingest_status, parser_version=None, error=None)` — atomic state machine transitions.
  - `iter_pending(conn, *, source, limit)` — manifest rows ready for fetch.
  - `iter_retryable(conn, *, source, limit)` — failed rows past `next_retry_at`.
- One-shot backfill script: ingests `def14a_ingest_log`, `institutional_holdings_ingest_log`, `insider_filings`, `blockholder_filings`, raw documents under `filing_raw_documents` into the manifest. Idempotent via primary key.
- Tests: round-trip, status transitions, retry filter, backfill from each tombstone source.

**Out of scope:** scheduler table; ingester behaviour change.

### #864 — `data_freshness_index` scheduler table

**Scope:**

- Migration adds `data_freshness_index` per the schema above (subject-polymorphic).
- `app/services/data_freshness.py` with:
  - `seed_scheduler_from_manifest(conn)` — derives one row per `(subject, source)` from manifest history.
  - `record_poll_outcome(conn, *, subject_type, subject_id, source, outcome, last_known_filing_id, expected_next_at)`.
  - `subjects_due_for_poll(conn, *, source, limit)` and `subjects_due_for_recheck(conn, *, source, limit)`.
  - Per-source cadence calculator (predicted next filing time).
- Tests: round-trip, due-for-poll filter, never-filed recheck filter, polymorphic-subject seeding.

**Depends on:** #863 (manifest must exist for the seeder to read).

**Out of scope:** ingest behaviour change.

### #865 — submissions.json + daily-index readers

**Scope:**

- `app/providers/implementations/sec_submissions.py`:
  - `check_freshness(http, cik, source, *, last_known_filing_id) -> FreshnessDelta` — pure provider; takes the watermark as an argument (Codex finding 7).
  - Fetches submissions.json `recent` array. Returns `(new_accessions, last_filed_at, has_more_in_files)`.
- `app/providers/implementations/sec_daily_index.py`:
  - `read_daily_index(http, when) -> Iterator[FilingIndexRow]` — streams `Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx`.
- Per-form filter mapping (form_type → source enum).
- HTTP cache: 5-min TTL on submissions.json, 1-day TTL on daily-index.
- Tests with recorded SEC fixtures (AAPL / GME / known new-filing case / no-new-filings case / paginated submissions).

**Depends on:** none (pure providers).

### #866 — `getcurrent` Atom fast lane (every 5 min)

**Scope:**

- New job `sec_recent_filings_poll` runs every 5 minutes.
- Reads Atom feed; filters by `cik IN data_freshness_index.cik AND form IN our sources`.
- For each match: UPSERT `sec_filing_manifest` row with `ingest_status='pending'`.
- Dedup is the manifest PK (accession_number).
- Tests: empty feed, populated feed, dedup against existing manifest, lossy-feed simulation.

**Depends on:** #863 (manifest), #864 (universe filter via scheduler).

### #867 — daily-index reconciliation safety-net job

**Scope:**

- New job `sec_daily_index_reconcile` runs daily 04:00 UTC.
- Reads yesterday's daily-index. Filters to (cik IN universe) + (form IN sources).
- UPSERT manifest rows that the Atom stream missed.
- Tests: index-only filing (Atom missed), Atom-already-saw filing (no-op).

**Depends on:** #863, #864, #865.

### #868 — manifest-driven worker (replaces per-form ingester batch logic)

**Scope:**

- New job `sec_manifest_worker` runs every 15 minutes.
- Scans `sec_filing_manifest WHERE ingest_status IN ('pending') OR (ingest_status='failed' AND next_retry_at <= NOW())`.
- For each row, dispatches to the existing per-form parser by `source`.
- Transitions status: `pending → fetched → parsed`, with `tombstoned`/`failed` per outcome.
- Bounded by SEC rate budget.
- Tests: state-machine transitions, retry backoff, parser-dispatch coverage per form.

**Depends on:** #863. Replaces the batch-limit logic in `sec_def14a_ingest`, `sec_form4_ingest` etc. — those become NOOPS or are deleted.

### #869 — per-CIK scheduled polling (Layer 3)

**Scope:**

- New job `sec_per_cik_poll` runs hourly.
- Reads `subjects_due_for_poll` from `data_freshness_index`.
- For each due subject: call `check_freshness`, UPSERT manifest rows, update scheduler outcome.
- Pagination: when `has_more_in_files` is true (i.e. recent array is full), walk `filings.files[]` paths (only happens for first-install + rebuild paths in practice).
- Tests: due-window filter, pagination follow, scheduler outcome update.

**Depends on:** #863, #864, #865.

### #870 — first-install drain (two paths: in-universe-only + bulk-zip)

**Scope:**

- New job `sec_first_install_drain` with payload `{ "use_bulk_zip": false }` (default).
- Default path: iterate every CIK in the tradable universe, call `check_freshness` for every supported source, UPSERT manifest. ~12k requests at 10 req/s = ~20 min total.
- Bulk-zip path: download `submissions.zip` + `companyfacts.zip`, stream-parse, populate manifest + `financial_facts_raw`.
- Crash-resume: re-running re-scans the universe; manifest UPSERTs are idempotent; pending rows pick up via #868.
- Tests: in-universe path on small fixture set, bulk-zip path with mocked archive.

**Depends on:** #863, #864, #865, #868.

### #871 — targeted rebuild

**Scope:**

- New job `sec_rebuild` with payload validators:
  - `{ "instrument_id": int }` → all issuer sources for that instrument.
  - `{ "filer_cik": str, "source": str }` → all filings under that filer's CIK for the source.
  - `{ "source": str }` → universe-wide for that source.
  - `{ "instrument_id": int, "source": str }` → narrow.
- Resolves scope to (subject_type, subject_id, source) triples.
- Resets scheduler rows: `state='unknown'`, `expected_next_at=NOW()`, `last_known_filing_id=NULL`.
- Sets manifest rows in scope to `ingest_status='pending'` (preserves history).
- Triggers #868 worker.
- Tests: per-CIK rebuild, per-form rebuild, scope-empty no-op, both-null rejected.

**Depends on:** #863, #864, #868.

### #872 — write-through ingest hooks + retire periodic sync

**Scope:**

- Wire `record_*_observation` + queue-refresh-of-`_current` calls inline at every `upsert_filing` callsite (5 ingesters + fundamentals normaliser). Manifest transition `parsed` triggers the observations write.
- `ownership_observations_sync` retired. Replace with `ownership_observations_repair` weekly job that scans:

  ```sql
  WHERE c.refreshed_at < (
      SELECT MAX(o.created_at) FROM ownership_*_observations o
      WHERE o.instrument_id = c.instrument_id
  )
  ```

  Codex finding 8: use `o.created_at` (or a monotonic revision column) NOT `known_from` (valid-time, doesn't capture re-ingests / parser rewashes / amendments).

- Tests: write-through wires per ingester; repair sweep finds drift; healthy = zero rows refreshed.

**Depends on:** #863, #868. Single biggest blast radius — Codex flagged this as scope-too-wide for one PR. Sub-split if needed:

- 872.A — write-through for insiders (Form 3 + Form 4).
- 872.B — institutions (13F).
- 872.C — blockholders (13D/G).
- 872.D — treasury + DEF 14A.
- 872.E — retire periodic sync, ship repair sweep.

### #873 — definition-of-done update

**Scope:**

- Update `.claude/CLAUDE.md` Definition of done: add smoke-against-3-5-instruments, cross-source-verification, backfill-executed clauses.
- Add operator-runbook for "after schema/parser change → run `POST /jobs/sec_rebuild/run` with appropriate scope and verify operator-visible figure".

**Depends on:** none (docs-only).

### Sequence

1. **#873** docs-only — ships first to lock the standard.
2. **#863** manifest schema + backfill from tombstones (foundation).
3. **#864** scheduler schema (depends on #863).
4. **#865** providers — submissions.json + daily-index readers (parallel to #863/#864).
5. **#868** manifest-driven worker (depends on #863) — replaces existing batch-limit logic; ingesters become parsers only.
6. **#866** Atom fast lane (depends on #863, #864).
7. **#867** daily-index reconcile (depends on #863, #864, #865).
8. **#869** per-CIK scheduled polling (depends on #863, #864, #865).
9. **#870** first-install drain (depends on #863-#868).
10. **#871** targeted rebuild (depends on #863, #864, #868).
11. **#872** write-through + retire periodic sync (depends on #863, #868). Sub-split A-E for blast radius.

After #872 lands, the ETL is event-driven from end to end:

- New filing accepted by SEC → Atom feed picks it up within 5 min OR daily-index catches it next morning → manifest row INSERT pending → worker picks it up → parser writes typed table + observation + queues `_current` refresh.
- No periodic re-scans. No daily blanket polls. No bandwidth tax for already-have data.

## Open questions for Codex

1. Is the `data_freshness_index` shape sufficient, or are we missing a cardinality concern? (E.g. `(instrument_id, source)` doesn't carry a per-form-amendment tracker. Do we need one?)
2. Should `getcurrent` Atom be the primary stream-sources path, or is it lossy enough that we still need the daily-index files as a safety net?
3. What's the right way to express "AAPL has never filed DEF 14C" without polling SEC every cycle to confirm absence? (`state='never_filed'` is a guess from history; should we re-check annually?)
4. Are there cheaper alternatives to per-CIK submissions.json — e.g. SEC's daily-index files (`Archives/edgar/daily-index/YYYYMMDD.idx`) which list every filing accepted that day in one ~1MB file? If we poll daily and parse the day's index, we get every CIK's new filings without per-CIK calls.
5. Does the bulk-zip approach (`submissions.zip` ~5 GB) make sense in a dev-laptop context? Should `sec_first_install_drain` have a "fast path" that only downloads the in-universe CIKs' submissions.json individually instead of the whole zip?
6. What did EdgarTools intentionally NOT do that we should also avoid? (E.g. the 1000-filings-per-CIK cap is clearly wrong for us; what else?)
