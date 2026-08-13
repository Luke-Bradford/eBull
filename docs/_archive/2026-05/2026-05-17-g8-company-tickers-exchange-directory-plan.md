# G8 — implementation plan

> **Status:** PLAN 2026-05-17.
>
> **Spec:** `docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md` (v3, Codex 1a CLEAN).
>
> **Scope:** one PR, branch `feat/g8-company-tickers-exchange-directory`.

## 0. Task DAG

```
T1 (migration sql/150)
T2 (service app/services/exchange_directory.py)
  ↓
T3 (scheduler.py — imports refresh_exchange_directory from T2)
  ↓
T4 (service tests — imports refresh_exchange_directory from T2)
T5 (integration tests — imports daily_cik_refresh which imports T2)
T6 (allow-list — references T2 file path string + test path)
T7 (_PLANNER_TABLES — references migration table name)
  ↓
T8 (matrix + handover — doc-only)
T9 (local gates)
T10 (Codex 2 + push)
```

**Dependency facts** (Codex 1b HIGH 1):

- T3 imports from T2 → T2 MUST land before T3 (or pyright + import-time checks fail).
- T4 imports from T2 → T2 before T4.
- T5 imports from `app.workers.scheduler` (which imports T2 via T3) → T2 + T3 before T5.
- T6 references T2 by file-path string only (not import) — can land in any order relative to T2, but the allow-list test fails until T2 exists. Net: land T6 alongside T2 commit OR in the same diff.
- T7 references the migration table-name string only — independent of T1's file existence, but the migration must run before any test that touches the table. Land T7 in the same diff as T1.
- T1 has no Python import dependencies — independent. Land first or alongside T2.

**Practical commit order:** one diff. T1 + T2 land first (foundational), T3 second (imports T2), T4-T7 third (test surface), T8-T10 doc + gates + push. Within the single PR diff this ordering is enforced naturally by file presence at import-time.

## 1. T1 — Migration `sql/150_cik_refresh_exchange_directory.sql`

Single-table migration with two indexes. Verbatim per spec §2.2:

```sql
BEGIN;

CREATE TABLE IF NOT EXISTS cik_refresh_exchange_directory (
    cik          TEXT NOT NULL
        CHECK (cik ~ '^[0-9]{10}$'),
    ticker       TEXT NOT NULL,
    name         TEXT,
    exchange     TEXT,
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, ticker)
);

COMMENT ON TABLE cik_refresh_exchange_directory IS
    'Parsed snapshot of company_tickers_exchange.json keyed by (cik, ticker). Populated by daily_cik_refresh (Stage 7 sibling enrichment, G8). Ticker-grain — single CIK may have multiple rows (share-class siblings, preferred-series tickers, ADR+OTC siblings). Observed-ever semantics: last_seen advances on UPSERT; rows SEC drops remain.';

CREATE INDEX IF NOT EXISTS idx_cik_refresh_exchange_directory_cik
    ON cik_refresh_exchange_directory (cik);

CREATE INDEX IF NOT EXISTS idx_cik_refresh_exchange_directory_exchange
    ON cik_refresh_exchange_directory (exchange);

COMMIT;
```

**Verification.** `tests/smoke/test_app_boots.py` exercises the lifespan migration path and runs `sql/*.sql` in order. No additional `tests/test_migration_*.py` file is required for the table-creation case (existing pattern: dedicated migration tests exist only for migrations that mutate existing data — sql/072/073/095/etc.).

## 2. T2 — Service module `app/services/exchange_directory.py`

Mirror `app/services/mf_directory.py` shape. Public surface:

```python
def refresh_exchange_directory(
    conn: psycopg.Connection[Any],
    *,
    provider: SecFilingsProvider | None = None,
) -> dict[str, int]:
    """Refresh cik_refresh_exchange_directory from company_tickers_exchange.json.

    Returns: {"fetched": <total rows in payload>, "directory_rows": <successfully upserted>}
    """
```

**Internal flow:**

1. Lazy-construct provider if absent (mirrors MF; supports unit tests that supply `conn` only).
2. `body = provider.fetch_document_text(_EXCHANGE_URL)` — raises `RuntimeError("Empty body fetching …")` if empty (MF parity).
3. `payload = json.loads(body)`.
4. Field index lookup with per-field tolerance (spec §2.3): if any of `{"cik","name","ticker","exchange"}` is absent from `fields[]`, log + return `{"fetched": 0, "directory_rows": 0}`.
5. Single outer `with conn.transaction(), conn.cursor() as cur:` block. Per row:
   - Skip if `len(row) <= max(field_idx.values())` (short row) — log warning.
   - Skip if `raw_cik` is None / non-int-coercible — log warning.
   - Zero-pad CIK via `str(int(raw_cik)).zfill(10)` (matches MF; provider's `_zero_pad_cik` lives on the wrong side of the service/provider boundary).
   - **Per-field non-string guard (Codex 1b MED 5).** Apply `isinstance(value, str)` BEFORE `.strip()`. Pattern:
     ```python
     def _coerce_text(v: object) -> str | None:
         if not isinstance(v, str):
             return None
         stripped = v.strip()
         return stripped or None
     ```
     Apply to ticker / name / exchange. Skip the row with a warning if the *required* `ticker` returns `None` (non-string OR empty); store `name` and `exchange` as `NULL` if `None`. Without the `isinstance` guard, a SEC payload that emits a numeric in a string column would raise `AttributeError: 'int' object has no attribute 'strip'`.
   - `INSERT ... ON CONFLICT (cik, ticker) DO UPDATE SET name=EXCLUDED.name, exchange=EXCLUDED.exchange, last_seen=NOW()`.
   - Increment `directory_rows` unconditionally (UPSERT always advances `last_seen`; no DO NOTHING branch).
6. Return counts.

**Imports:** `json`, `logging`, `typing.Any`, `psycopg`, `app.config.settings`, `app.providers.implementations.sec_edgar.SecFilingsProvider`.

**Module constants:** `_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"`.

**Module docstring:** copy MF directory's header style; note the spec reference + ticker-grain PK rationale + parsed-snapshot wording (Codex 1a LOW 7 — avoid "raw-payload sink").

## 3. T3 — Restructure `daily_cik_refresh` in `app/workers/scheduler.py`

**Current code (lines 1665-1815).** Spec §2.1 enumerates the early returns. The restructure:

1. Hoist `upserted = 0` + `mapping_size = 0` pre-bindings (already done).
2. Introduce `skip_equity_upsert: bool = False`.
3. Replace the two `return` statements in the 304 / hash-unchanged branches with `skip_equity_upsert = True` flag-sets.
4. Wrap the full-upsert path in `if not skip_equity_upsert:`.
5. Move the existing `try / except for refresh_mf_directory(...)` block OUT of any if-branch — sits at the same indent level as the equity-side path, fires unconditionally.
6. Append the new `try / except for refresh_exchange_directory(...)` block immediately after the MF block.
7. `tracker.row_count = upserted` remains at the end.

**Exact diff shape (anchored on current scheduler.py):**

```python
# OLD (1726-1728):
                logger.info("daily_cik_refresh: 304 Not Modified, skipping upsert")
                tracker.row_count = 0
                return

# NEW:
                logger.info("daily_cik_refresh: 304 Not Modified, skipping equity upsert")
                skip_equity_upsert = True
            elif not dest_empty and prior and prior.response_hash == result.body_hash:
                ...
                skip_equity_upsert = True
            # Drop both `return` paths. Equity branch becomes a single
            # if-elif-else with the full-upsert as the else.

# Wrap full-upsert in:
            if not skip_equity_upsert:
                assert result is not None  # narrowed by the elif above; satisfies pyright
                mapping_size = len(result.mapping)
                ...
                with conn.transaction():
                    upserted = upsert_cik_mapping(...)
                    set_watermark(...)

# Sibling enrichments — ALWAYS fire:
            try:
                mf_result = refresh_mf_directory(conn, provider=provider)
                logger.info("mf_directory refresh: ...", ...)
            except Exception:  # noqa: BLE001
                logger.exception("mf_directory refresh failed; equity CIK refresh result preserved")

            try:
                exch_result = refresh_exchange_directory(conn, provider=provider)
                logger.info(
                    "exchange_directory refresh: fetched=%s directory_rows=%s",
                    exch_result["fetched"],
                    exch_result["directory_rows"],
                )
            except Exception:  # noqa: BLE001
                logger.exception("exchange_directory refresh failed; equity CIK refresh result preserved")

        tracker.row_count = upserted
```

**Type-checker note.** Pyright requires `result is not None` narrowing inside the `if not skip_equity_upsert:` branch. Use an explicit `assert result is not None` with a brief comment naming the elif that proves it.

**Import addition.** Top of `app/workers/scheduler.py`:

```python
from app.services.exchange_directory import refresh_exchange_directory
```

(MF's `refresh_mf_directory` import is already present.)

**Watermark unchanged.** No new watermark row for exchange — the directory IS the watermark (spec §2.2). The `set_watermark(...)` call for the equity 304-path-hash-unchanged branch retains its current location inside the `with conn.transaction():` block.

**dest_empty path.** Unchanged — `dest_empty` short-circuits the `if_modified_since=None` send AND the 304 path raises `RuntimeError` (existing #1056 invariant). Sibling enrichments still fire after the raise propagates from the equity branch — wait, no: a raise from the equity branch exits the function via context-manager rollback, so MF + exchange DO NOT fire. This is correct: an empty-destination 304 is a "must investigate" state, and the failure mode rightly tombstones the whole run.

## 4. T4 — Service-level tests `tests/test_exchange_directory.py`

12 tests per spec §3.1. Implementation pattern follows `tests/test_mf_directory.py` (will need to inspect that file at impl time for fixture conventions). Each test injects a stub `SecFilingsProvider` whose `fetch_document_text` returns a constructed payload.

**Stub provider shape:**

```python
class _StubProvider:
    def __init__(self, payload: str | None) -> None:
        self._payload = payload
    def fetch_document_text(self, url: str) -> str:
        if self._payload is None:
            return ""
        return self._payload
    # __enter__ / __exit__ for context-manager compat
    def __enter__(self) -> "_StubProvider": return self
    def __exit__(self, *args) -> None: pass
```

**Per-test assertions** use direct SQL against the test DB:

```python
def _read_directory(conn):
    return [dict(zip(("cik","ticker","name","exchange","last_seen"), r))
            for r in conn.execute("SELECT cik, ticker, name, exchange, last_seen FROM cik_refresh_exchange_directory ORDER BY cik, ticker").fetchall()]
```

**Test list (matches spec §3.1):** happy-path (3 rows, 3 exchanges) → CIK zero-pad → multi-ticker preserved (BAC variant trio) → empty/null exchange → empty/null ticker skip → malformed row skip → upsert idempotency (compare last_seen value between two runs, not strict-monotonicity) → empty data list → missing fields key → missing single field → field reordering → empty body raises.

**Idempotency test (spec §3.1.7 detail).** Avoid time-flakiness:

```python
# First pass.
res1 = refresh_exchange_directory(conn, provider=stub)
ts1 = conn.execute("SELECT last_seen FROM cik_refresh_exchange_directory WHERE cik = %s AND ticker = %s", ('0000320193', 'AAPL')).fetchone()[0]

# Force a deterministic clock advance via a stale UPDATE.
conn.execute("UPDATE cik_refresh_exchange_directory SET last_seen = last_seen - INTERVAL '1 minute'")
conn.commit()  # commit so the next refresh's NOW() is definitely later

# Second pass.
res2 = refresh_exchange_directory(conn, provider=stub)
ts2 = conn.execute("SELECT last_seen FROM cik_refresh_exchange_directory WHERE cik = %s AND ticker = %s", ('0000320193', 'AAPL')).fetchone()[0]

assert ts2 > ts1 - timedelta(minutes=1)  # second pass advanced past the staled value
assert res1 == res2  # row count stable
```

(Codex 1a round-2 minor — neutralises `NOW()` per-transaction-stable behaviour.)

## 5. T5 — Integration test `tests/test_daily_cik_refresh_sibling_enrichments.py`

6 tests per spec §3.2. Pattern follows the existing `tests/test_mf_directory_sync_wrapper.py` (parallel "fires regardless" contract).

**Use existing repo conventions from `tests/test_daily_cik_refresh_scope.py`** (Codex 1b round-2 corrections):

**Database URL monkeypatch (Codex 1b HIGH 2 + round-2 §1).** `daily_cik_refresh()` opens its OWN connection via `psycopg.connect(settings.database_url)`. Use the existing pattern from `test_daily_cik_refresh_scope.py:220-227`:

```python
@staticmethod
def _patch_db_url(monkeypatch):
    """daily_cik_refresh hardcodes settings.database_url; redirect to
    ebull_test DB for the test."""
    from app.config import settings
    from tests.fixtures.ebull_test_db import test_database_url
    monkeypatch.setattr(settings, "database_url", test_database_url())
```

Call `self._patch_db_url(monkeypatch)` at the top of each test (mirror the precedent file's class-method style).

**Foreign-key seed (Codex 1b HIGH 3 + round-2 §2).** Reuse the existing seed helpers from `tests/test_daily_cik_refresh_scope.py`:

- `_seed_instrument(conn, instrument_id=N, symbol="X", exchange="4")` — at module-level line 33 of the precedent file.
- `_seed_aapl_us_equity(conn)` — class-method at line 212 of the precedent file. Sets `exchanges.asset_class='us_equity'` for `exchange_id='4'` + seeds AAPL instrument_id=4901 + clears `external_identifiers (sec, cik)`.

Do NOT hand-roll a `seeded_apple_cik(conn)` fixture (Codex 1b round-2 §2). Either:

- Import `_seed_instrument` from `tests.test_daily_cik_refresh_scope` (cross-test imports are an established pattern in this repo — `tests/test_upsert_cik_mapping.py` does similar), OR
- Extract `_seed_instrument` to `tests/fixtures/copy_mirrors.py` if the cross-test import looks like coupling. Lift-and-share is preferred since two test files now exercise the same daily_cik_refresh path.

**Recommendation:** extract to `tests/fixtures/copy_mirrors.py` as `seed_us_equity_instrument(conn, instrument_id, symbol)` (mirrors existing `copy_mirrors.py:make_*` helpers) + re-export. Both `test_daily_cik_refresh_scope.py` and the new `test_daily_cik_refresh_sibling_enrichments.py` import from there.

**Provider stub (top-level).** Monkeypatch `app.workers.scheduler.SecFilingsProvider` to a class whose `build_cik_mapping_conditional` returns the desired result:

```python
class _StubProvider:
    def __init__(self, result, *, body=None) -> None:
        self._result = result
        self._body = body
    def build_cik_mapping_conditional(self, *, if_modified_since=None):
        return self._result
    def fetch_document_text(self, url: str) -> str:
        return self._body or ""
    def __enter__(self): return self
    def __exit__(self, *args): pass
```

**Watermark seed for hash-unchanged path:**

```python
from app.services.watermarks import set_watermark
with conn.transaction():
    set_watermark(conn, source="sec.tickers", key="global",
                  watermark="Wed, 17 May 2026 02:00:00 GMT",
                  response_hash="<matches stub.body_hash>")
conn.commit()
```

**Sibling stubs that WRITE rows (Codex 1b MED 4).** Spec §3.2 acceptance #1-3 assert the directory TABLES are populated, not just that the wrapper called the sibling. Two approaches:

**Approach A (stub the service function with a row-writing fake):**

```python
def _fake_refresh_exchange(conn, *, provider):
    """Stub that mirrors the real service contract:
    writes ONE row + returns the counts dict."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cik_refresh_exchange_directory (cik, ticker, name, exchange, last_seen)
            VALUES ('0000320193', 'AAPL', 'Apple Inc.', 'Nasdaq', NOW())
            ON CONFLICT (cik, ticker) DO UPDATE SET last_seen = NOW()
            """
        )
    return {"fetched": 1, "directory_rows": 1}

monkeypatch.setattr("app.workers.scheduler.refresh_exchange_directory", _fake_refresh_exchange)
# Mirror for MF:
def _fake_refresh_mf(conn, *, provider):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cik_refresh_mf_directory (class_id, series_id, symbol, trust_cik, last_seen)
            VALUES ('C000000001', 'S000000001', 'TEST', '0000000001', NOW())
            ON CONFLICT (class_id) DO UPDATE SET last_seen = NOW()
            """
        )
    return {"fetched": 1, "directory_rows": 1, "external_identifier_rows": 0}

monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _fake_refresh_mf)
```

**Approach B (let the real services run with stubbed `fetch_document_text`):**

The `_StubProvider.fetch_document_text` returns a constructed JSON payload; the real service writes rows. Cleaner but couples the integration test to the service-level field-parsing logic — better isolation by Approach A.

**Adopted approach: A** (row-writing stubs). The service-level field parsing is already covered by T4 unit tests.

**Failure-mode stubs (spec §3.2 acceptance #4-6):**

```python
def _raise_exchange(conn, *, provider):
    raise RuntimeError("simulated SEC outage — exchange directory")
monkeypatch.setattr("app.workers.scheduler.refresh_exchange_directory", _raise_exchange)
```

Same shape for MF in scenarios that test MF fail-soft.

**Assertion utility:**

```python
def _both_directories_populated(conn) -> tuple[bool, bool]:
    mf = conn.execute("SELECT COUNT(*) FROM cik_refresh_mf_directory").fetchone()[0]
    exch = conn.execute("SELECT COUNT(*) FROM cik_refresh_exchange_directory").fetchone()[0]
    return mf > 0, exch > 0
```

**`logger.exception` assertion:** use `caplog` with `caplog.set_level(logging.ERROR, logger="app.workers.scheduler")` and assert one record per simulated failure:

```python
exchange_errors = [r for r in caplog.records
                   if r.levelname == "ERROR" and "exchange_directory" in r.message]
assert len(exchange_errors) == 1
```

## 6. T6 — Allow-list update `tests/test_fetch_document_text_callers.py`

Two entries added near the MF #1171 / #1174 block (lines 122-128 / 136 of the current test file):

```python
# G8 — bundled company_tickers_exchange.json ingest. Fetches the
# exchange directory JSON via SecFilingsProvider.fetch_document_text
# and normalises every (cik, name, ticker, exchange) row into
# cik_refresh_exchange_directory (the structured SQL surface for
# future consumers). Spec: docs/superpowers/specs/2026-05-17-g8-...
"app/services/exchange_directory.py",
"tests/test_exchange_directory.py",
```

The integration test `tests/test_daily_cik_refresh_sibling_enrichments.py` does NOT need an entry IF it stubs `refresh_exchange_directory` / `refresh_mf_directory` (higher-level service functions). At impl time, verify via `grep -l fetch_document_text tests/test_daily_cik_refresh_sibling_enrichments.py` — if the grep matches, add the test file to the allow-list. If not, leave it out.

**Sentinel `>= 9` bound update (line 197):** the test file's `_caller_scan_finds_expected_minimum` asserts `len(hits) >= 9`. Verify with `grep -lc fetch_document_text app/ tests/` at impl time. If the existing bound still holds with 2 additions, leave it. If `>= 9` becomes too low after adding 2 more allow-listed files (and the grep hits both), the assertion still passes — `>= 9` is a floor, not equality.

## 7. T7 — `_PLANNER_TABLES` update `tests/fixtures/ebull_test_db.py`

Add `"cik_refresh_exchange_directory"` to the tuple at line ~86. **Not alphabetically sorted** — the tuple is grouped by cleanup / FK order (Codex 1b LOW 6). Place near the existing `cik_refresh_mf_directory` entry to keep cik-refresh siblings co-located:

```python
_PLANNER_TABLES: tuple[str, ...] = (
    ...
    "cik_refresh_mf_directory",
    "cik_refresh_exchange_directory",  # G8
    ...
)
```

(Codex 1a LOW 6 — prevents cross-test row leak.)

## 8. T8 — Matrix + handover updates

### 8.1 `.claude/skills/data-engineer/etl-endpoint-coverage.md`

Three edits:

1. **§2 (manifest source matrix).** `daily_cik_refresh` row at line 130 — update the "Notes" column to add: "Stage 7 sibling enrichment fires `refresh_exchange_directory` (G8, sql/150)."
2. **§4 (reference endpoints).** Line 95 `company_tickers_exchange.json` row:
   - Implementation column: `NOT CONSUMED` → `app/services/exchange_directory.py:refresh_exchange_directory`
   - Watermark column: `—` → `cik_refresh_exchange_directory.last_seen MAX`
   - Status column: `❌ GAP` → `✅ WIRED — daily_cik_refresh Stage 7 (G8, 2026-05-17)`
3. **§7 (gap register).** Line 175 G8 row:
   - Status column: `OPEN (low)` → `✅ CLOSED 2026-05-17 (PR #<n>)`
   - PR column: `—` → `#<n>`
   - Notes: append "Closed via `cik_refresh_exchange_directory` snapshot table; observed-ever semantics; consumers TBD."

### 8.2 `docs/superpowers/plans/2026-05-17-us-etl-completion.md`

Append handover block per the plan's template (line 167-177). Includes:

- Phase: 2
- Gap / ticket closed: **G8**
- Branch: `feat/g8-company-tickers-exchange-directory`
- Merge SHA: filled in post-merge
- Tests added: 12 service + 6 integration + 2 allow-list entries + 1 fixture update
- Scope discoveries handled in-scope:
  - **Cohort observation (empirical 2026-05-17):** company_tickers_exchange.json shares the same row cohort COUNT as company_tickers.json (10,353) but is ticker-grain not CIK-grain — 7,996 unique CIKs / 1,446 multi-ticker CIKs. Plan's pre-cohort framing of "closes pink-sheet/OTC/foreign-without-ADR cohort gap" is corrected in spec §1 / matrix §4 notes. The real value-add is the (ticker, exchange) mapping for preferred series, ADR + OTC siblings, share-class siblings.
  - **MF latent bug fixed:** Stage 6 MF refresh previously only fired on the full-upsert path (skipped on 304 / hash-unchanged). The restructure makes it fire on every `daily_cik_refresh` invocation. Bootstrap-side authority remains `mf_directory_sync` (S25 #1174); this PR only fixes the daily-cron drift-heal path.
  - **PK granularity correction:** initial design was (cik); corrected to (cik, ticker) after re-counting the live payload. Documented in spec §2.2 + integration test #3.1.3.
  - **`fetch_document_text` allow-list entries added** per #453 contract.
- Matrix delta: §2 `daily_cik_refresh` row + §4 exchange-row + §7 G8 → all updated to CLOSED.
- ETL clauses #8-#12: applicability per spec §3.6 (most N/A — no per-instrument figure changes; cross-source verify spot-checked AAPL/GME/MSFT/JPM/HD via curl).

### 8.3 `.claude/skills/data-sources/sec-edgar.md` correction

Two edits in §1 (lines 22-32):

- Line 22 row stays as-is (`+ exchange (Nasdaq / NYSE / Cboe / OTC)`).
- Line 32 "Coverage gap" paragraph — clarify: "`company_tickers_exchange.json` shares the same CIK row cohort as `company_tickers.json` but is **ticker-grain**: a single CIK can produce multiple (ticker, exchange) rows for preferred series, share-class siblings, and ADR + OTC variants. eBull's `daily_cik_refresh` Stage 7 (#G8) lands the snapshot in `cik_refresh_exchange_directory`."

## 9. T9 — Local gates

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest tests/test_exchange_directory.py tests/test_daily_cik_refresh_sibling_enrichments.py tests/test_fetch_document_text_callers.py tests/smoke/test_app_boots.py -x
uv run pytest  # full suite
```

**Smoke gate.** `tests/smoke/test_app_boots.py` exercises the migration via FastAPI lifespan. The new migration `sql/150_*.sql` lands at boot.

## 10. T10 — Codex 2 pre-push + push + monitor

1. **Codex 2:** `codex exec` against the branch diff. Address findings.
2. Push. PR title: `feat(g8): company_tickers_exchange.json directory snapshot + Stage 6/7 fail-soft restructure`.
3. Monitor PR:
   - `gh pr view <n> --comments` for Claude review bot.
   - `gh pr checks <n>` for CI.
   - Wait until both visible on the LATEST commit before any follow-up.
4. Resolve every review comment per the FIXED / DEFERRED / REBUTTED contract.
5. Merge after APPROVE on the most recent commit + CI green.
6. Update memory `[[us-source-coverage]]` to reflect G8 closure.

## 11. Risk callouts for impl time

- **Pyright strict-narrowing on `result is not None` after elif.** May require `assert result is not None` even though the elif proves it. MF restructure pattern reference.
- **Existing `tests/test_daily_cik_refresh_scope.py` may live-fetch SEC after T3 (Codex 1b round-2 §3).** Three tests there (`test_empty_dest_omits_if_modified_since`, `test_empty_dest_with_matching_hash_still_upserts`, `test_empty_dest_with_none_result_raises`) drive the real `daily_cik_refresh()` with stubbed `build_cik_mapping_conditional` ONLY — they do NOT stub `fetch_document_text`. Pre-T3, MF Stage 6 doesn't fire on 304 / hash-unchanged / empty-dest-raises paths, so those tests never reach MF. **Post-T3, MF + exchange Stage 6/7 fire on every path** (modulo `_seed_aapl_us_equity` not seeding a `dest_empty`-raise scenario — though `test_empty_dest_with_none_result_raises` DOES raise before sibling enrichments, so it's safe). The two non-raising tests would attempt a real SEC `fetch_document_text` call against the MF + exchange URLs unless stubbed.
  - **Required addition to T3 + T5 scope:** add a `monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", lambda conn, *, provider: {...})` + same for `refresh_exchange_directory` in the two affected existing tests. Or extract a fixture `_patch_sibling_enrichments(monkeypatch)` and apply to all three tests for consistency.
  - **Alternative:** mark the existing tests `pytest.mark.integration` already (line 202) — pytest's network-isolated CI run may already block live SEC fetches. But the local test run could still hit SEC. Patch defensively.
  - **Decision:** patch in-scope. The fix lives in `test_daily_cik_refresh_scope.py` as part of this PR's diff.
- **MF integration tests may break.** Existing tests in `tests/test_workers_scheduler.py` (if any) that assert "MF refresh did NOT fire on 304 path" will now fail. Search for such assertions at impl time; update assertions to match the new behaviour (MF DOES fire on every run).
- **`scheduled_adapter.py` / `watermarks.py` mappings unchanged.** Only the body of `daily_cik_refresh` changes; the job name, lane, source, and watermark contract are all unchanged.
- **`bootstrap_orchestrator.py` unchanged.** S6 `cik_refresh` continues to dispatch `daily_cik_refresh` as today.
- **Database test-fixture cleanup.** With `_PLANNER_TABLES` updated, the new table is TRUNCATED between tests. Confirm at impl time that this matches the table's per-test-isolation expectation (it does — observed-ever semantics are per-run, not per-test).

## 12. Out-of-scope reminders (per spec §4)

- No conditional GET.
- No consumers.
- No `external_identifiers` schema change.
- No stale-row deletion.
- No raw-bytes retention.
- No `mf_directory_sync` deletion.
- No bootstrap stage for exchange directory.
- No `cik_exchange_directory_sync` dedicated job.

If any of these surface as Codex 2 findings, the answer is REBUTTED with reference to spec §4.

## 13. PR description skeleton

```markdown
## Summary

Closes G8 in the US-ETL endpoint-coverage matrix. Adds:

- `sql/150_cik_refresh_exchange_directory.sql` — new directory snapshot
  table, ticker-grain PK `(cik, ticker)`, observed-ever semantics.
- `app/services/exchange_directory.py` — service helper mirroring
  `app/services/mf_directory.py` shape.
- Restructured `app/workers/scheduler.py::daily_cik_refresh` so Stage 6
  (MF) AND new Stage 7 (exchange) sibling enrichments fire on every
  invocation regardless of equity-side watermark state. Fixes a
  latent MF Stage 6 skip on 304 / hash-unchanged paths.
- `tests/test_exchange_directory.py` (12 tests) +
  `tests/test_daily_cik_refresh_sibling_enrichments.py` (6 tests).
- `_ALLOWED_CALLER_FILES` allow-list entries for the new caller +
  test file.
- `_PLANNER_TABLES` entry for cross-test cleanup.
- Matrix updates: §2 + §4 + §7 G8 → CLOSED.

## Why

`company_tickers_exchange.json` is a SEC reference bridge enumerated
in `.claude/skills/data-sources/sec-edgar.md` §1 but not consumed.
The plan's pre-cohort framing claimed it widens the CIK cohort —
empirically the cohort COUNT is identical (10,353), but the file is
ticker-grain (7,996 unique CIKs / 1,446 multi-ticker CIKs). Real
value-add is the (ticker, exchange) mapping for preferred series,
ADR + OTC siblings, share-class siblings.

No v1 consumer. Snapshot persistence closes the source-coverage gap;
consumers land via separate tickets when downstream use cases are
identified. Matches MF directory precedent (#1171 → #1174 → consumers).

## Security model

Reference data fetched from `https://www.sec.gov/files/`. SEC's
shared 10 req/s pool. No user-controlled input. Schema migration is
additive (new table only); no data migration. Fail-soft wrappers
prevent sibling-enrichment failures from cascading into the equity
side.

## Conscious tradeoffs

- **Bundle into `daily_cik_refresh` vs dedicated `daily_cik_exchange_refresh`
  job.** Bundled: no downstream capability requires a dedicated job;
  wiring overhead for a dedicated job is ~7 cross-cutting changes for
  no operational benefit. Trade-off doc: spec §2.1.1.
- **PK `(cik, ticker)` vs `(cik)`.** Live payload has 1,446 CIKs with
  multiple tickers (BAC=17, JPM=9). A `(cik)`-only PK would drop ~2,357
  rows on every refresh. Trade-off doc: spec §2.2.
- **No conditional GET in v1.** ~1 MB daily fetch is acceptable; MF
  precedent. Add if SEC adds bandwidth pressure.
- **Observed-ever semantics.** Rows SEC drops remain with stale
  `last_seen`. Consumers filter on freshness. Trade-off doc: spec §2.2.

## ETL clauses #8-#12 applicability

- **Clause 8 (3-5 instrument smoke):** N/A — reference-table snapshot;
  no per-instrument figure changes.
- **Clause 9 (cross-source verify):** spot-check vs SEC live for
  AAPL/GME/MSFT/JPM/HD — output recorded below.
- **Clause 10 (backfill):** N/A — no manifest scope.
- **Clause 11 (operator-visible figure):** N/A — no v1 consumer.
- **Clause 12 (per-clause commit SHA):** see verification block below.

## Test plan

- [x] `uv run pytest tests/test_exchange_directory.py` — 12 tests pass.
- [x] `uv run pytest tests/test_daily_cik_refresh_sibling_enrichments.py` — 6 tests pass.
- [x] `uv run pytest tests/smoke/test_app_boots.py` — migration applied at lifespan boot.
- [x] `uv run pytest` — full suite green.
- [x] `uv run ruff check .` / `format --check` / `pyright` — all green.
- [x] Codex 2 pre-push — CLEAN.
- [ ] Claude review bot APPROVE on latest commit.
- [ ] CI green on latest commit.

## Verification (Clause 12)

[recorded per impl-time]
```
