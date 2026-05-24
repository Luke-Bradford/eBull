# G8 — `company_tickers_exchange.json` directory ingest

> **Status:** SPEC v2 2026-05-17 (post-Codex 1a). Closes Phase 2 PR 4 in `docs/_archive/2026-05-17-us-etl-completion.md`.
>
> **Gap closed:** §7 G8 in `.claude/skills/data-engineer/etl-endpoint-coverage.md`. Also adds the matching row to §4 (reference endpoints) and extends §2's `daily_cik_refresh` row.

## 1. Problem statement

SEC publishes `https://www.sec.gov/files/company_tickers_exchange.json` daily. eBull does not consume it. §1 of `.claude/skills/data-sources/sec-edgar.md` enumerates it as the canonical "ticker → CIK bridge **with exchange enum**". G8 closes the source-coverage gap by landing a directory snapshot.

### Cohort observation (empirical, 2026-05-17)

| Metric | `company_tickers.json` | `company_tickers_exchange.json` |
|---|---|---|
| Total rows | 10,353 | 10,353 |
| Unique CIKs | 10,353 | **7,996** |
| Multi-ticker CIKs | 0 (de-duped on `ticker_str`) | **1,446** |
| Exchange enum | absent | Nasdaq=4273, NYSE=3289, OTC=2549, CBOE=27, NULL=215 |

The exchange file is **ticker-grain, not CIK-grain**. Single-CIK examples in the live payload: `BAC` (17 ticker variants — common + 15 preferred series + 1 OTC), `JPM` (9 variants), `BABA` (3 variants — NYSE ADR + 2 OTC), `GOOG/GOOGL` (share-class siblings). The earlier plan-§2-PR-4 prose claimed the exchange file widens the CIK cohort; that is incorrect (CIK cohort is narrower because the file emits multiple rows per CIK). The real value-add is the (ticker, exchange) mapping — preferred series, OTC ADR siblings, share-class siblings.

There is no v1 consumer of the exchange enum. Snapshot persistence is the entire scope of this PR. Consumers (e.g. operator-visible exchange tag on `/instruments`, out-of-universe CIK classification for filings discovery, preferred-series resolution) land via separate tickets when downstream use cases are identified. This matches the MF directory precedent (#1171 → #1174) where `cik_refresh_mf_directory` landed before `_fund_class_resolver` consumed it.

## 2. Design

### 2.1 Job wiring — restructure `daily_cik_refresh` so sibling enrichments always fire

#### 2.1.1 The early-return bug (latent on MF, blocking for G8)

Today's `daily_cik_refresh` (`app/workers/scheduler.py:1665-1815`) has two early returns BEFORE Stage 6 (MF) is reached:

- Line 1728 — `return` after SEC responds `304 Not Modified`.
- Line 1747 — `return` after SEC responds 200 with an identical body hash to the stored watermark.

On either path, neither MF (current Stage 6) nor a new Stage 7 would fire. The MF latent bug was papered over in #1174 by adding a dedicated bootstrap stage `mf_directory_sync` (S25) with `daily_cik_refresh`'s bundled call retained as a "drift-heal safety net" — but the bundled call still fails to fire on warm-watermark days. Under steady-state operation, the dedicated job is the actual write path. This works for MF because S25 is on the bootstrap orchestrator AND is dispatched daily via the scheduler.

For G8, we have **no downstream capability requirement**: no bootstrap stage depends on `cik_refresh_exchange_directory` being populated, and the directory has no v1 consumer. Adding a dedicated `daily_cik_exchange_refresh` job would cost ~7 cross-cutting wiring changes (job const + invoker + bootstrap stage + sources + watermarks + scheduled_adapter + tests) for a feature with no downstream pressure to justify them.

Instead: **fix the early-return bug in-scope**. Restructure `daily_cik_refresh` so sibling enrichments always run, regardless of the equity-side decision. This is the smallest change that (a) makes Stage 7 (G8) correct under warm watermark and (b) tightens the existing MF Stage 6 from a known-latent-skip into an authoritative daily write path.

#### 2.1.2 Restructured shape

```python
def daily_cik_refresh() -> None:
    upserted = 0
    mapping_size = 0
    skip_equity_upsert = False

    with _tracked_job(JOB_DAILY_CIK_REFRESH) as tracker:
        with (SecFilingsProvider(...) as provider, psycopg.connect(...) as conn):
            # --- Equity-side fetch + decide whether to upsert ---
            dest_empty = _cik_destination_is_empty(conn)
            prior = get_watermark(conn, SOURCE, WATERMARK_KEY)
            if_modified_since = None if dest_empty else (prior.watermark if (prior and prior.watermark) else None)

            result = provider.build_cik_mapping_conditional(if_modified_since=if_modified_since)

            if result is None:
                if dest_empty:
                    raise RuntimeError(...)  # existing invariant
                logger.info("daily_cik_refresh: 304 Not Modified, skipping upsert")
                skip_equity_upsert = True
            elif not dest_empty and prior and prior.response_hash == result.body_hash:
                logger.info("daily_cik_refresh: 200 but body hash unchanged, skipping upsert")
                with conn.transaction():
                    set_watermark(conn, source=SOURCE, key=WATERMARK_KEY,
                                  watermark=result.last_modified or prior.watermark,
                                  response_hash=result.body_hash)
                skip_equity_upsert = True

            if not skip_equity_upsert:
                assert result is not None  # narrowed for type-checker; covered by branches above
                mapping_size = len(result.mapping)
                if dest_empty:
                    logger.warning("daily_cik_refresh: destination empty — forcing full upsert.")
                rows = conn.execute(<existing cohort query>).fetchall()
                instrument_symbols = [(row[0], row[1]) for row in rows]
                with conn.transaction():
                    upserted = upsert_cik_mapping(conn, result.mapping, instrument_symbols)
                    set_watermark(conn, source=SOURCE, key=WATERMARK_KEY,
                                  watermark=result.last_modified or "",
                                  response_hash=result.body_hash)

            # --- Sibling enrichments — ALWAYS fire, fail-soft each. ---
            # Stage 6 (#1171, fixes the latent warm-watermark skip):
            try:
                mf_result = refresh_mf_directory(conn, provider=provider)
                logger.info("mf_directory refresh: ...", ...)
            except Exception:  # noqa: BLE001
                logger.exception("mf_directory refresh failed; equity CIK refresh result preserved")

            # Stage 7 (G8) — new:
            try:
                exch_result = refresh_exchange_directory(conn, provider=provider)
                logger.info("exchange_directory refresh: fetched=%s directory_rows=%s",
                            exch_result["fetched"], exch_result["directory_rows"])
            except Exception:  # noqa: BLE001
                logger.exception("exchange_directory refresh failed; equity CIK refresh result preserved")

        tracker.row_count = upserted
    logger.info("CIK refresh complete: mapping_size=%d upserted=%d", mapping_size, upserted)
```

**Transaction structure — accurate description.** psycopg3's `with psycopg.connect(url) as conn:` opens an implicit transaction on first command and COMMITs at context-manager exit (ROLLBACKs on uncaught exception). Inner `with conn.transaction():` blocks therefore create SAVEPOINTs, not top-level commits — equity + MF + exchange writes all land in one transaction that COMMITs at the `with psycopg.connect(...)` exit.

What actually protects equity writes from sibling failure is the **fail-soft `try / except Exception: logger.exception(...)` wrapper around each enrichment**, combined with psycopg3 SAVEPOINT semantics: when `refresh_mf_directory` (or `refresh_exchange_directory`) raises inside its inner `with conn.transaction():`, the SAVEPOINT rolls back, the exception propagates to the wrapper, the wrapper catches + logs, and the outer transaction CONTINUES — equity's savepoint and watermark-touch savepoint remain intact and commit on context exit.

This is the same structural contract `refresh_mf_directory` has had since #1171, validated in production. **No explicit `conn.commit()` is added** — doing so would deviate from the MF precedent and could mask conn-level error rollback under unhandled provider errors. Sibling enrichments open their OWN inner `conn.transaction()` (savepoint) so per-sibling failure rolls back only that sibling's writes.

**Caveat the wrapper does NOT cover.** If a conn-level error propagates out of a sibling enrichment AND escapes the catch-all (e.g. the connection itself becomes unusable), the outer `with psycopg.connect(...)` block exits via exception → T1 ROLLBACK → equity writes also lost. The MF code has carried this caveat since #1171; G8 inherits it. Mitigations: the catch-all `except Exception` covers all expected SEC-side failures (HTTP error, parse error, savepoint-rollback-by-SQL-error); conn-level catastrophe (network drop mid-transaction) is rare and tombstones cleanly via tracker's `failed` state.

**MF semantics shift.** Today MF fires only on full-upsert path (~rare; only on body change). After this PR, MF fires on every `daily_cik_refresh` invocation. This is idempotent (UPSERT advances `last_seen` only) and removes the latent dependency on body-change-coupled refresh. `mf_directory_sync` (S25) retains its dedicated-job role for bootstrap.

**Why not Option C (dedicated `daily_cik_exchange_refresh` job).** Rejected: no downstream capability requires it, and the wiring overhead is ~7 cross-cutting changes (job const + `_INVOKERS` + bootstrap stage spec + `MANUAL_TRIGGER_JOB_SOURCES` + `watermarks.py::_JobSpec` + `scheduled_adapter.py` mapping + tests). The restructure-and-bundle path costs one site (`daily_cik_refresh` body) and fixes the parallel MF latent bug in scope.

**Why not Option D (separate `cik_refresh_exchange_directory` cron at a different cadence).** Rejected: `company_tickers_exchange.json` refreshes once daily on SEC's side (same as `company_tickers.json`); decoupling cadence buys nothing.

### 2.2 Persistence — new `cik_refresh_exchange_directory` table

```sql
-- sql/150_cik_refresh_exchange_directory.sql
BEGIN;

-- ---------------------------------------------------------------------
-- cik_refresh_exchange_directory — parsed snapshot of
-- company_tickers_exchange.json (G8, Phase 2 PR 4 of US-ETL plan).
-- ---------------------------------------------------------------------
-- Populated by daily_cik_refresh (Stage 7 sibling enrichment, mirrors
-- Stage 6 cik_refresh_mf_directory pattern at sql/149:278-290).
--
-- Ticker-grain. The SEC payload emits MULTIPLE rows per CIK for share-
-- class siblings (GOOG/GOOGL), preferred-series tickers (BAC has 17),
-- and ADR + OTC siblings (BABA/BABAF/BBAAY). PK MUST be (cik, ticker)
-- to preserve every (ticker, exchange) mapping the payload carries.
-- Empirical: 2026-05-17 live payload has 7,996 unique CIKs across
-- 10,353 rows; 1,446 CIKs have multiple ticker variants.
--
-- Snapshot semantics: "observed-ever". UPSERT advances last_seen on
-- every observed row; rows SEC drops from the payload remain in the
-- table with an older last_seen. Consumers needing a freshness gate
-- filter on last_seen >= cutoff. No DELETE / mark-stale in v1 — add
-- when a consumer needs strict authority over the live cohort.

CREATE TABLE IF NOT EXISTS cik_refresh_exchange_directory (
    -- 10-digit zero-padded canonical form; CHECK enforces the
    -- invariant at the DB level (mirrors cik_raw_documents at
    -- sql/109:42-44) so a direct-SQL writer cannot bypass the
    -- application-layer normaliser.
    cik          TEXT NOT NULL
        CHECK (cik ~ '^[0-9]{10}$'),
    ticker       TEXT NOT NULL,
    name         TEXT,
    -- SEC's exchange enum — stored verbatim. Observed values in live
    -- payload: 'Nasdaq', 'NYSE', 'OTC', 'CBOE', NULL. Nullable: 215
    -- rows in the 2026-05-17 sample emit no exchange. No CHECK
    -- constraint so a new SEC enum value lands without a migration.
    exchange     TEXT,
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, ticker)
);

COMMENT ON TABLE cik_refresh_exchange_directory IS
    'Parsed snapshot of company_tickers_exchange.json keyed by (cik, ticker). Populated by daily_cik_refresh (Stage 7 sibling enrichment, G8). Ticker-grain — single CIK may have multiple rows (share-class siblings, preferred-series tickers, ADR+OTC siblings). Observed-ever semantics: last_seen advances on UPSERT; rows SEC drops remain.';

-- Per-CIK rollup lookup for future operator-visible "list all tickers
-- for this issuer" reads. Composite index covers (cik) prefix queries
-- and ticker-by-CIK lookups without sequential scan.
CREATE INDEX IF NOT EXISTS idx_cik_refresh_exchange_directory_cik
    ON cik_refresh_exchange_directory (cik);

-- Exchange-bucket reporting / classification reads.
CREATE INDEX IF NOT EXISTS idx_cik_refresh_exchange_directory_exchange
    ON cik_refresh_exchange_directory (exchange);

COMMIT;
```

**PK on (cik, ticker).** Codex 1a HIGH 2 — empirical 1,446 of 7,996 unique CIKs have multiple tickers in the payload (BAC=17, JPM=9, MS=10). A PK on `(cik)` alone would collapse all variants into one row, dropping ~2,357 of 10,353 source rows on every refresh. The composite PK preserves the full ticker-grain mapping.

**Ticker column NOT NULL.** Empirically verified against the 2026-05-17 live payload: zero null/empty tickers in 10,353 rows. SEC's schema treats `ticker` as required. Defensive normalisation (`strip()` empty → `NULL`) would push such a row through `ON CONFLICT (cik, ticker)` with a NULL PK component — Postgres rejects NULL in PK columns by default. Make the constraint explicit: skip rows with absent / empty / non-string ticker; log warning.

**`exchange TEXT` no CHECK constraint.** A new SEC enum value (e.g. SEC starts emitting `'IEX'`) lands without a migration. Downstream consumers handle unknown values.

**`name` nullable.** Empirically non-null today but defensive.

**Indexes.** `(cik)` prefix index for per-issuer rollup reads (composite PK already covers this for ORDER BY but a dedicated index keeps unrelated query plans honest). `(exchange)` for bucket classification reads.

**Why not `cik_raw_documents`.** That table is per-CIK-per-document (kind ∈ `submissions_json`, `companyfacts_json` enforced by CHECK) for archived raw fetches. A reference directory aggregate-file does not fit. The MF precedent established the dedicated directory-table pattern; G8 follows it. Codex 1a LOW 7: do NOT describe this table as a "raw-payload sink" — it is a **parsed snapshot**. The raw bytes are not retained; if exact-bytes retention is needed downstream, that lands as a separate consumer-driven ticket against `cik_raw_documents` with an expanded `document_kind` enum.

**Idempotency.** `INSERT ... ON CONFLICT (cik, ticker) DO UPDATE SET name = EXCLUDED.name, exchange = EXCLUDED.exchange, last_seen = NOW()`. Re-running on the same day overwrites with identical values + a fresh `last_seen`. The watermark IS the table.

### 2.3 Service module — `app/services/exchange_directory.py`

Single public function, signature mirrors `refresh_mf_directory`:

```python
def refresh_exchange_directory(
    conn: psycopg.Connection[Any],
    *,
    provider: SecFilingsProvider | None = None,
) -> dict[str, int]:
    """Refresh ``cik_refresh_exchange_directory`` from
    ``company_tickers_exchange.json``.

    Returns counts: ``{fetched, directory_rows}``.
    """
```

**Fetch path.** `provider.fetch_document_text(_EXCHANGE_URL)` — same SEC rate-limited pool as MF and equity refresh. No conditional GET in v1 (file is ~1 MB; daily fetch cost acceptable; matches MF decision documented at `mf_directory.py:24-26`).

**Parser shape.** Payload is `{"fields": ["cik","name","ticker","exchange"], "data": [[1045810,"NVIDIA CORP","NVDA","Nasdaq"], ...]}`. Field-name index lookups (not positional) — defensive against SEC field reordering.

**Field-resolution tolerance (Codex 1a MED 5).** Don't trust `fields.index("...")` to succeed:

```python
REQUIRED_FIELDS = ("cik", "name", "ticker", "exchange")
field_idx: dict[str, int] = {}
for fld in REQUIRED_FIELDS:
    try:
        field_idx[fld] = fields.index(fld)
    except ValueError:
        logger.warning("exchange_directory: SEC payload missing required field %r; skipping refresh", fld)
        return {"fetched": 0, "directory_rows": 0}
```

This makes "missing one field" symmetric with "missing entire fields list" — both no-op safely.

**Per-row validation.**

- `len(row) <= max(field_idx.values())` → log warning, skip.
- `raw_cik` is None / not int-coercible → log warning, skip.
- `ticker` is None / empty after strip → log warning, skip (PK forbids NULL).
- `name` empty after strip → stored as `NULL`.
- `exchange` None / empty after strip → stored as `NULL` (observed for ~215 rows in live payload).

**Transaction.** Single outer `with conn.transaction(), conn.cursor() as cur:` block. All ~10k rows in one transactional UPSERT loop. Failure raises; the caller (`daily_cik_refresh` Stage 7) catches via the fail-soft wrapper. The transaction opens AFTER the equity-side transaction has exited (see §2.1.2 invariant).

**No own connection.** Caller passes `conn`. Provider auto-instantiates only if absent (mirrors MF; useful for unit tests that mock the provider but supply a test conn).

### 2.4 `fetch_document_text` allow-list update

`tests/test_fetch_document_text_callers.py::_ALLOWED_CALLER_FILES` is the writer-discipline regression guard (#453): every legitimate caller of `SecFilingsProvider.fetch_document_text` must be allow-listed alongside a comment naming the SQL normalisation path. Without an allow-list update, the test fails on first lint with `New caller(s) of fetch_document_text detected outside the allow-list`.

Add two entries mirroring the MF precedent (lines 122-128 / 136 of that test file):

- **`app/services/exchange_directory.py`** — calls `fetch_document_text(_EXCHANGE_URL)`. Normalises every `(cik, name, ticker, exchange)` row into `cik_refresh_exchange_directory` (the structured SQL surface for future consumers).
- **`tests/test_exchange_directory.py`** — references / monkeypatches `fetch_document_text` to inject stub payloads. Test-only; no disk persistence.

The integration test `tests/test_daily_cik_refresh_sibling_enrichments.py` does NOT need an allow-list entry: it monkeypatches the higher-level `refresh_mf_directory` / `refresh_exchange_directory` service functions, not `fetch_document_text` directly. (Matches the MF wrapper precedent — `test_mf_directory_sync_wrapper.py` is on the allow-list because the wrapper-effect test goes through the lower-level call; the equivalent G8 wrapper test does the same and SHOULD be added if implementation routes through `fetch_document_text`. Decide at impl time: if the integration test monkeypatches the higher service function only, no allow-list entry; if it monkeypatches `fetch_document_text` directly, add entry.)

### 2.5 Acceptance criteria

1. **Migration applied.** `sql/150_cik_refresh_exchange_directory.sql` lands in dev DB; smoke test passes (lifespan + migration applied at boot).
2. **`daily_cik_refresh` populates the table on EVERY run.** After one invocation, regardless of whether the equity side took the 304 / hash-unchanged / full-upsert path: `SELECT COUNT(*) FROM cik_refresh_exchange_directory` returns ~10,000 rows (one per ticker), `SELECT COUNT(DISTINCT cik) FROM cik_refresh_exchange_directory` returns ~8,000 (Codex 1a HIGH 1).
3. **Idempotent re-runs.** A second invocation: `last_seen` advances on every row, row count unchanged. No duplicate-key errors.
4. **Multi-ticker CIKs preserved.** `SELECT cik, ARRAY_AGG(ticker ORDER BY ticker) FROM cik_refresh_exchange_directory GROUP BY cik HAVING COUNT(*) > 1 LIMIT 5` returns rows for BAC / JPM / BABA / etc. (Codex 1a HIGH 2).
5. **Fail-soft.** If `refresh_exchange_directory` raises (simulated via fetch monkeypatch), the equity-side CIK upsert still commits — verified by integration test.
6. **Fail-soft on MF parity.** Same contract for `refresh_mf_directory` — fixing the early-return skip MUST NOT introduce a path where MF failure cascades into equity failure.
7. **Coverage matrix updated.** `.claude/skills/data-engineer/etl-endpoint-coverage.md`:
   - §2 `daily_cik_refresh` row notes the Stage 7 addition.
   - §4 reference-endpoint row for `company_tickers_exchange.json` → ✅ WIRED with path to `app/services/exchange_directory.py`.
   - §7 G8 row → ✅ CLOSED 2026-05-17.

## 3. Test plan

### 3.1 Service-level tests — `tests/test_exchange_directory.py`

1. **Happy path with stub provider.** Stub `provider.fetch_document_text` to return canonical SEC payload with 3 rows (Nasdaq / NYSE / OTC). Assert: 3 rows in `cik_refresh_exchange_directory`, each with the expected `exchange`, zero-padded CIK, stripped ticker.
2. **CIK zero-padding.** Stub returns `{"cik": 320193, ...}` as integer. Assert row's `cik = '0000320193'`.
3. **Multi-ticker CIK preserved.** Stub returns 3 rows with the SAME `cik=70858` but different `ticker` values (`BAC` / `BAC-PB` / `BACRP`). Assert 3 rows in the table (Codex 1a HIGH 2 regression-guard).
4. **Empty / null exchange normalised.** Stub returns one row with `exchange=""` and one with `exchange=None`. Assert both stored as SQL `NULL`.
5. **Empty / null ticker skipped.** Stub returns 2 rows with `ticker=""` and `ticker=None`. Assert 0 rows + 2 warning logs.
6. **Malformed row skipped, valid stored.** Stub returns 3 rows: one valid, one with `cik="not-a-number"`, one with `len(row) == 2`. Assert 1 row + 2 warning logs.
7. **Upsert idempotency.** Run refresh twice with identical stub data; assert row count unchanged. Avoid time-based `last_seen` strict-monotonicity assertions (psycopg3 `NOW()` may yield identical values inside a transaction batch — `now()` is transaction-stable; `clock_timestamp()` is per-call). Instead, mark the first pass's `last_seen` via a controlled value, run the second pass, and assert that the row's `last_seen` is now `!=` the first pass's recorded value (or fetch `clock_timestamp()` via the table writer if a strict-increase semantic is needed in a future consumer).
8. **Empty payload data list.** Stub returns `{"fields": [...], "data": []}`. Assert 0 rows + `{"fetched": 0, "directory_rows": 0}`.
9. **Missing entire fields list.** Stub returns `{"data": [...]}` (no `fields` key). Assert `{"fetched": 0, "directory_rows": 0}` without raise.
10. **Missing single field name in `fields`.** Stub returns `fields = ["cik","name","ticker"]` (no `exchange`). Assert `{"fetched": 0, "directory_rows": 0}` + warning log (Codex 1a MED 5).
11. **Field reordering tolerated.** Stub returns `fields = ["exchange","cik","name","ticker"]` with a matching row; assert row stored correctly.
12. **Empty body raises.** Stub `fetch_document_text` returns `""`. Assert `RuntimeError("Empty body fetching ...")` (MF parity).

### 3.2 Integration test — `tests/test_daily_cik_refresh_sibling_enrichments.py`

Codex 1a HIGH 1 — explicit coverage for the early-return-skip bug fix.

1. **Sibling enrichments fire on 304 path.** Stub `provider.build_cik_mapping_conditional` to return `None` (simulating 304). Pre-seed `external_identifiers (sec, cik)` so the empty-destination invariant doesn't trip. Stub both directory refresh fetches. Run `daily_cik_refresh`. Assert: `cik_refresh_mf_directory` populated (1+ rows from stub), `cik_refresh_exchange_directory` populated (1+ rows from stub), equity-side `external_identifiers` unchanged.
2. **Sibling enrichments fire on hash-unchanged path.** Stub the provider to return a `CikMappingResult` whose `body_hash` matches a pre-seeded watermark. Assert: equity upsert skipped (watermark touched only), MF + exchange both populated.
3. **Sibling enrichments fire on full-upsert path.** Standard happy-path. Assert all three: equity, MF, exchange all wrote.
4. **Stage 7 fail-soft.** Stub `refresh_exchange_directory` to raise `RuntimeError("simulated SEC outage")`. Assert: equity + MF still write; `cik_refresh_exchange_directory` empty; `logger.exception` called once.
5. **Stage 6 fail-soft (MF parity regression guard).** Stub `refresh_mf_directory` to raise. Assert: equity + exchange still write; `cik_refresh_mf_directory` empty; equity-side `external_identifiers` upsert preserved.
6. **Both stages fail-soft.** Both raise. Assert: equity-side `external_identifiers` upsert preserved; both directories empty; two `logger.exception` calls.

### 3.3 Test fixture cleanup (Codex 1a LOW 6)

Add `cik_refresh_exchange_directory` to `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES`. Cross-test leak protection.

### 3.4 Pre-flight gates

- `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest` all green.
- `tests/smoke/test_app_boots.py` green (migration applied at lifespan boot).
- Pre-push hook runs first three gates.

### 3.5 Codex checkpoints

- **Checkpoint 1a (spec):** `codex.cmd exec` against this spec — addressed in this v2 revision.
- **Checkpoint 1b (plan):** `codex.cmd exec` against the implementation plan that follows this spec.
- **Checkpoint 2 (pre-push):** `codex.cmd exec review` against the branch diff before first push.

### 3.6 ETL clauses #8-#12 — applicability statement

The PR adds a snapshot table + ETL fetch but does NOT touch ownership / fundamentals / observations data paths. Clauses #8-#12:

- **Clause 8 (3-5 instrument smoke):** N/A — no per-instrument operator-visible figure changes. Documented in PR body as "G8 is a reference-table snapshot — no per-instrument figure changes."
- **Clause 9 (cross-source verify):** Spot-check `cik_refresh_exchange_directory` against SEC's `company_tickers_exchange.json` directly (`curl | jq`) for AAPL + GME + MSFT + JPM + HD. PR body records the exact rows compared.
- **Clause 10 (backfill):** N/A — no manifest scope, no ownership/fundamentals rebuild. PR body records: "first `daily_cik_refresh` run on dev DB after merge populates the directory; verified via `\\dt+ cik_refresh_exchange_directory` row count post-trigger."
- **Clause 11 (operator-visible figure):** N/A — no consumer in v1. Deferred until a downstream ticket consumes the directory.
- **Clause 12 (commit SHA per clause):** PR body records the verification commands run + their output, with the commit SHA at which each was observed.

## 4. Out of scope

- **Conditional GET (If-Modified-Since / ETag).** Daily ~1 MB fetch cost is acceptable. Add if SEC adds bandwidth pressure or a consumer needs sub-daily freshness.
- **Consumers.** No `/instruments` endpoint extension; no resolver reads the new table. Land consumers in separate PRs when a use case is identified.
- **`external_identifiers` schema change.** Operator design call selected the snapshot directory (Option A); the `external_identifiers` column extension (Option B) was rejected.
- **Stale-row deletion.** Observed-ever semantics — rows SEC drops remain with stale `last_seen`. Consumers gate on freshness.
- **Raw-bytes retention.** Directory holds parsed rows, not raw bytes. If exact-bytes retention is needed by a future consumer, land via expanded `cik_raw_documents.document_kind` enum + new migration.
- **`mf_directory_sync` (S25) deletion.** The dedicated bootstrap stage remains the bootstrap-side authoritative provider for MF; this PR only fixes the bundled fail-soft path's warm-watermark skip.
- **Bootstrap stage for exchange directory.** No `cik_exchange_directory_sync` analogous to S25 — no downstream capability requires it.

## 5. Risk register

| Risk | Mitigation |
|---|---|
| SEC field reordering breaks positional indexing | Fields are looked up by name; never positional. |
| SEC adds a new exchange enum value | `exchange TEXT` (no CHECK) accepts any string. |
| Stage 7 failure cascades into equity refresh | Fail-soft wrapper around the call; integration test #3.2.4 asserts the contract. |
| Stage 6 fail-soft regression (MF) caused by restructure | Integration test #3.2.5 explicitly verifies MF parity. |
| Migration runs on fresh-install boot before any `daily_cik_refresh` fires | Table created empty; no v1 consumer reads it. |
| Settled-decision §"CIK = entity" violated by PK choice | PK is `(cik, ticker)` — does not assert per-CIK uniqueness; preserves issuer-grain interpretation. |
| Raw-payload prevention rule (#1171 doc line) | Directory holds **parsed** rows. Rule applies to per-filing ingest writers; reference-directory aggregates use the dedicated directory-table pattern (MF precedent at sql/149). |
| MF semantics shift (now fires on every `daily_cik_refresh` run) | UPSERT is idempotent. Bootstrap-side authority remains `mf_directory_sync` (S25). Test #3.2.1-3 explicitly verifies new fire-on-every-path behaviour. |
| `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` leak | New table added to `_PLANNER_TABLES`. |

## 6. Settled-decisions cross-check

- **§"CIK = entity, CUSIP = security" (#1102).** Preserved. PK is `(cik, ticker)` — does not make per-CIK assertions; preserves issuer-grain interpretation in consumer queries (`SELECT DISTINCT cik FROM ...`).
- **§"Identifier mapping lives in `external_identifiers`".** Not violated — `external_identifiers` untouched. Directory is enrichment, not identifier mapping.
- **§"Providers are pure HTTP".** Preserved. `refresh_exchange_directory` is a service-layer function calling `provider.fetch_document_text`; no DB access from the provider.

## 7. Prevention-log cross-check

- **"Writer-vs-resolver `is_primary` mismatch on `external_identifiers`" (line 1316-1319).** Not applicable — `external_identifiers` not written by this PR.
- **"Raw-payload persistence BEFORE parser" (line 1171).** The directory table is a **parsed snapshot** for reference directories. Same pattern as MF (sql/149); does not displace the prevention rule's scope (per-filing ingest writers).
- **"psycopg3 transaction inside open tx is SAVEPOINT not COMMIT".** §2.1.2 explicitly addresses this: equity + sibling enrichments commit together at `with psycopg.connect(...)` exit, NOT after each `with conn.transaction()`. Fail-soft try/except + SAVEPOINT rollback semantics are what preserve equity writes under sibling failure — not top-level commit ordering. The conn-level catastrophe caveat is documented.
- **"Counter increment under ON CONFLICT".** Counter advances unconditionally because UPSERT always advances `last_seen` (no DO NOTHING branch). Documented at the increment site.
- **"Conditional-branch test coverage" (lines 992-1001).** Integration tests #3.2.1-3 explicitly cover the three equity-side branches × the sibling-enrichment fire-or-fail-soft outcome matrix.

## 8. Changes from spec v1 (Codex 1a)

- **HIGH 1 (early-return skip):** Restructured `daily_cik_refresh` so sibling enrichments fire after the equity branch regardless of which branch was taken. MF latent bug fixed in-scope.
- **HIGH 2 (PK granularity):** PK is `(cik, ticker)`. Empirical 1,446 multi-ticker CIKs documented in §1. Tests #3.1.3 and #3.2 acceptance #4 regression-guard.
- **MED 3 (transaction ordering):** §2.1.2 now describes the **actual** psycopg3 transaction structure (single implicit outer txn, inner `conn.transaction()` blocks = SAVEPOINTs, COMMIT lands at `with psycopg.connect(...)` exit). What protects equity from sibling failure is fail-soft try/except + SAVEPOINT rollback semantics, NOT a top-level commit boundary between equity and siblings. Conn-level catastrophe caveat documented (inherited from MF #1171). Codex round-2 HIGH addressed.
- **MED 4 (stale-row semantics):** §2.2 documents "observed-ever". §4 lists "stale-row deletion" as out-of-scope.
- **MED 5 (per-field missing tolerance):** §2.3 enumerates per-field tolerance; test #3.1.10 covers missing single field.
- **LOW 6 (test fixture):** §3.3 adds `cik_refresh_exchange_directory` to `_PLANNER_TABLES`.
- **LOW 7 ("raw-payload sink" wording):** §2.2 + §7 + §4 reworded — table is a "parsed snapshot" not a raw sink.
