# FINRA RegSHO daily short volume ingest + schema (#916) — spec

> Phase: 6 of `docs/superpowers/plans/2026-05-17-us-etl-completion.md` (PR 12).
> Issue: #916 (OPEN). Parent #845 CLOSED. Predecessor: #915 PR #1207 merged 2026-05-18.
> Spike: `docs/superpowers/spikes/2026-05-18-finra-regsho-daily-feasibility.md` (verdict: SHIP).
> Architectural siblings: G6/#915 bimonthly (`app/services/finra_short_interest_ingest.py`) — same `finra` Lane, shared throttle clock, same synth-noop-manifest pattern. G7 `sec_xbrl_facts` ScheduledJob+synth-parser. G12 `sec_master_idx_quarterly_sweep` preloaded-resolver shape.
> Closure framing: **OBSERVATIONS PRIMITIVE** (mirror G10/G11 Phase 4). Ingest layer wired end-to-end; chart memo-overlay UI (issue acceptance #2 daily sparkline) deferred per plan §1 autonomy contract UI carve-out.

## 1. Goal

Ingest FINRA's daily **RegSHO Short Sale Volume** publication into a new `finra_regsho_daily_observations` table, partitioned by trade date quarterly. Operator can:

- Query per-instrument daily short volume + total volume + short-exempt volume across every trade date in the partition window via `SELECT * FROM finra_regsho_daily_observations WHERE instrument_id=<id> ORDER BY trade_date DESC`.
- Cross-reference the per-facility breakdown (CNMS aggregate + FNQC / FNRA / FNSQ / FNYX / FORF facility-specific rows) for the same `(instrument, trade_date)` via the composite PK.
- Trigger backfill via `POST /jobs/finra_regsho_daily_refresh/run` (no body — fixed 30-day window in v1; extended-window backfill via REPL runbook §13).

The daily ingest complements #915 bimonthly (which gives a snapshot of total short interest open positions every 2 weeks) — daily RegSHO gives intra-cycle momentum signal. Both share the `finra` Lane.

**No `_current` snapshot** — the daily file IS the snapshot. Per-instrument latest-trade-date queries land on the partitioned observations table directly (PK `(instrument_id, trade_date DESC, …)` index keeps it fast).

Memo overlay (FE) — deferred. No new UI ticket opened; closure framing documents the deferral.

## 2. Settled-decisions check

| Decision | Relevance | Preservation |
|---|---|---|
| §"Free regulated-source-only" (#532) | FINRA is an SEC-overseen SRO; RegSHO daily publication is mandated under SEC Rule 200(g) + FINRA Rule 4560. CDN is free + anonymous. | PRESERVED. |
| §"Provider design rule" (providers thin, DB lookups in services) | New `app/providers/implementations/finra_regsho.py` is HTTP-only (URL builder + `ResilientClient` GET + decode). Service layer at `app/services/finra_regsho_ingest.py` owns symbol resolver + DB writes. | PRESERVED. |
| §"Filing event storage" (#1168 — raw payload before parse) | Pipe-delim bytes stored in `filing_raw_documents` keyed by synthetic accession `FINRA_REGSHO_{PREFIX}_{YYYYMMDD}` BEFORE any parse step. | PRESERVED. |
| §"Manifest source-of-truth" (#864 / sql/118) | New `finra_regsho_daily` source added to the `sec_filing_manifest.source` CHECK enum + the `data_freshness_index.source` CHECK enum + `ManifestSource` Literal + `_CADENCE` cadence map. Subject_type `finra_universe` is reused; subject_id `FINRA_REGSHO` is a new singleton. | EXTENDED with a new singleton — see §5.1. |
| §"Data freshness index" (#865 / sql/120) | `app/services/data_freshness.py::_CADENCE` (the cadence map consumed by `cadence_for(source)`) gets a new entry `"finra_regsho_daily": timedelta(days=2)` (daily publication, allow 1 weekend + 1 holiday slack). | EXTENDED. |
| §"Disjoint-lane invariant" (universal-gate carve-out, settled 2026-05-16) | `finra_regsho_daily_refresh` reuses the existing `finra` Lane; no new Lane needed. Prerequisite `_bootstrap_complete` → opts INTO the universal gate. Not added to the exempt allow-list. | PRESERVED. |

## 3. Prevention-log check

| Entry | How spec honours it |
|---|---|
| ResilientClient shared throttle (#726) | New provider module IMPORTS `_FINRA_RATE_LIMIT_CLOCK` + `_FINRA_RATE_LIMIT_LOCK` from `finra_short_interest.py` (module-globals already enforce single-per-process throttle for the FINRA CDN). Bimonthly + daily jobs share the 1 req/s budget by construction. |
| Raw-payload-before-parse (#1168) | Per-file ordering: `store_raw` → `conn.commit()` (raw durable) → `with conn.transaction():` { parse → bulk-UPSERT observations → manifest UPSERT 'parsed' } → `with` exits → commit. Mirrors `finra_short_interest_refresh.py` Phase 1 / Phase 2 split. |
| psycopg3 savepoint-vs-commit | **JOB body** opens `with conn.transaction():` around each per-file parse-and-upsert; service emits SQL only — NEVER enters its own transaction context (would silently top-level-commit under autocommit conn — re-occurrence of the #915 Codex 1b r1 HIGH 2 lesson). |
| Universal-gate supersession | New job `finra_regsho_daily_refresh` is NOT added to the exempt allow-list. Its prerequisite is `_bootstrap_complete`. The disjoint-lane invariant test (`tests/test_universal_gate_carve_out.py`) is extended to confirm. |
| Skills must own integrity | `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 FINRA row + §7 G6 row updated to reflect the daily wiring. `.claude/skills/data-sources/finra.md` §1 updated (daily row flips to `WIRED 2026-05-18`); §3 (rate-limit posture) confirms shared throttle; new §2.5 documents the decimal-volume + multi-prefix Market shape. |
| Documentation layering | Spec doc (this file) is the architecture-decision-of-record; settled-decisions + prevention-log get cross-references; `.claude/skills/data-sources/finra.md` is the per-source operator runbook. |
| Manifest UPSERT must seed freshness (#915 Codex 2 r1 HIGH 1) | Service explicitly calls `seed_freshness_for_manifest_row` after the manual manifest UPSERT (same shape as bimonthly: re-fetch path can't go through `record_manifest_entry` because the `parsed → parsed` transition is disallowed). |

## 4. Architecture

```
Cron: finra_regsho_daily_refresh (daily 23:00 UTC, lane=finra, prerequisite=_bootstrap_complete)
  │
  ▼
ScheduledJob body — app/workers/scheduler.py::finra_regsho_daily_refresh()
  │   -- opens its OWN conn via psycopg.connect(settings.database_url)
  │   -- owns ALL conn.commit() / conn.rollback() calls
  │
  ├─ resolver = build_preloaded_symbol_resolver(conn)   # reuse #915 helper via import
  │
  ├─ candidate_dates = _trade_dates_to_fetch(
  │       now=datetime.now(UTC),
  │       backfill_window_days=30,   # default v1 steady-state window
  │   )                              # weekday-only enumeration
  ├─ already_parsed = SELECT accession FROM sec_filing_manifest
  │                   WHERE source='finra_regsho_daily' AND ingest_status='parsed'
  ├─ # Revision window — always re-probe the 2 most-recent trade dates × 6 prefixes
  ├─ revision_window = {(d, prefix) for d in sorted(candidate_dates)[-2:] for prefix in PREFIXES}
  ├─ all_targets = {(d, prefix) for d in candidate_dates for prefix in PREFIXES}
  ├─ targets = sorted(all_targets - already_parsed_tuples | revision_window)
  │
  ├─ FOR EACH (trade_date, prefix) in targets:
  │      try:
  │          raw_bytes = provider.fetch_regsho_daily_file(trade_date, prefix)
  │      except FinraNotFound:
  │          continue                      # 404 = file not yet published / holiday
  │      except (HTTPStatusError, Timeout, ConnectError) as exc:
  │          stats.append(per-file failure)
  │          continue
  │
  │      # Phase 1: durable raw-payload store BEFORE parse (#1168).
  │      # Wrapped in try-except so a UnicodeDecodeError / store_raw DB
  │      # failure records a per-file failure + continues to the next
  │      # (trade_date, prefix) pair rather than poisoning the connection
  │      # for subsequent iterations (Codex 1a MED — mirrors #915
  │      # finra_short_interest_refresh.py:235-255).
  │      try:
  │          raw_filings.store_raw(
  │              conn,
  │              accession_number=f"FINRA_REGSHO_{prefix}_{YYYYMMDD}",
  │              document_kind='finra_regsho_daily_txt',
  │              payload=raw_bytes.decode('utf-8'),
  │              source_url=file_url,
  │          )
  │          conn.commit()                 # raw durable BEFORE parse
  │      except Exception as exc:
  │          conn.rollback()
  │          stats.append(per-file failure with raw_store error_detail)
  │          continue
  │
  │      # Phase 2: parse + upserts inside JOB-owned txn.
  │      try:
  │          with conn.transaction():      # JOB owns commit/rollback
  │              per_file = ingest_regsho_daily_file(
  │                  conn, trade_date, prefix, raw_bytes, resolver, ingest_run_id,
  │              )
  │          stats.append(per_file)
  │      except Exception as exc:
  │          stats.append(per-file failure)
  ▼
ScheduledJob:
  tracker.row_count = sum(s.rows_upserted for s in stats)
  logger.info("finra_regsho_daily_refresh: files=%d upserted=%d ...")
  if total_parsed > 0 and total_resolved / total_parsed < 0.50:
      logger.warning("...match rate ...%% below 50%% — universe drift or FINRA shape regression")
  if failed_files > 0:
      raise RuntimeError("finra_regsho_daily_refresh: N of M files failed")
```

**Per-file failure isolation:** HTTP / decode / parse / DB errors on one (trade_date, prefix) pair trigger the JOB-owned `with conn.transaction()` rollback; the raw-payload store committed before is untouched; remaining (trade_date, prefix) pairs continue. Job-level `status='failure'` raised by the explicit RuntimeError if any file failed (mirror G12 + #915 contract).

**Service NEVER opens its own transaction.** `ingest_regsho_daily_file` accepts the caller-supplied connection and emits SQL only. The JOB body wraps the call site in `with conn.transaction():` — preserves the prevention-log "service-no-commit" invariant + the psycopg3 SAVEPOINT-vs-TOPLEVEL discipline (re-occurrence of the #915 Codex 1b r1 HIGH 2 lesson).

## 5. Schema

Two-migration split: extend the source CHECK enums in `sec_filing_manifest` + `data_freshness_index` + extend `filing_raw_documents.document_kind`, then create the typed observation table.

### 5.1 New migration `sql/153_finra_regsho_daily_enum.sql`

Extends three CHECK constraints in lock-step:

```sql
BEGIN;

-- Widen filing_raw_documents.document_kind for the new daily file body.
ALTER TABLE filing_raw_documents
    DROP CONSTRAINT IF EXISTS filing_raw_documents_document_kind_check;
ALTER TABLE filing_raw_documents
    ADD CONSTRAINT filing_raw_documents_document_kind_check
    CHECK (document_kind IN (
        'primary_doc',
        'infotable_13f',
        'primary_doc_13dg',
        'form4_xml',
        'form3_xml',
        'form5_xml',
        'def14a_body',
        'nport_xml',
        'finra_short_interest_csv',
        'finra_regsho_daily_txt'
    ));

-- Widen sec_filing_manifest.source so the new RegSHO daily slot accepts INSERTs.
ALTER TABLE sec_filing_manifest
    DROP CONSTRAINT sec_filing_manifest_source_check;
ALTER TABLE sec_filing_manifest
    ADD CONSTRAINT sec_filing_manifest_source_check
    CHECK (source IN (
        'sec_form3', 'sec_form4', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily'
    ));

-- Same widening for the freshness-index source enum.
ALTER TABLE data_freshness_index
    DROP CONSTRAINT data_freshness_index_source_check;
ALTER TABLE data_freshness_index
    ADD CONSTRAINT data_freshness_index_source_check
    CHECK (source IN (
        'sec_form3', 'sec_form4', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily'
    ));

COMMIT;
```

The corresponding `ManifestSource` Literal at `app/services/sec_manifest.py:106` is widened in the same PR; the `_CADENCE` cadence map at `app/services/data_freshness.py:69` (consumed by `cadence_for(source)`) gets a `"finra_regsho_daily": timedelta(days=2)` entry; `DocumentKind` Literal at `app/services/raw_filings.py:58` is widened to include `"finra_regsho_daily_txt"`.

The `subject_type` enum (`finra_universe`) is **unchanged** — RegSHO daily reuses the bimonthly's singleton subject pattern but with a different `subject_id` (`FINRA_REGSHO` vs the bimonthly's `FINRA_SI`). No migration needed for the subject-type column.

### 5.2 New migration `sql/154_finra_regsho_daily.sql`

```sql
BEGIN;

CREATE TABLE finra_regsho_daily_observations (
    instrument_id           BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    trade_date              DATE   NOT NULL,
        -- The trade date from the file body (matches the URL date). NOT
        -- a settlement date — RegSHO daily is per-trade-date.
    market                  TEXT   NOT NULL,
        -- Single-facility files: 'B' | 'Q' | 'N' | 'O' (one of FINRA's
        -- single-char facility codes).
        -- CNMS aggregate: comma-joined union (e.g. 'B,Q,N'). Each
        -- distinct value is a distinct row in the PK — CNMS aggregate
        -- is a separate fact from the per-facility breakdown for the
        -- same (instrument, trade_date). PK ordering ensures both
        -- coexist.
    source_document_id      TEXT   NOT NULL,
        -- '{PREFIX}_{YYYYMMDD}' — e.g. 'CNMS_20260515', 'FNQC_20260515'.
        -- Encodes the prefix so the audit trail distinguishes the
        -- different files. Same prefix may UPSERT into the same row
        -- on revision re-fetch.

    short_volume            NUMERIC(18, 6) NOT NULL,
    short_exempt_volume     NUMERIC(18, 6) NOT NULL,
    total_volume            NUMERIC(18, 6) NOT NULL,
        -- All three reported by FINRA to 6 decimal places. Empirically
        -- verified 2026-05-18 in spike §3.3. NOT integer.

    source                  TEXT NOT NULL CHECK (source = 'finra_regsho'),
        -- Single-element CHECK locks the table to FINRA RegSHO daily.
        -- 'finra_regsho' is the short-form column value (mirrors the
        -- bimonthly's 'finra_si' / 'finra_short_interest' short/long
        -- split). Manifest source enum uses the long form
        -- 'finra_regsho_daily'.
    source_url              TEXT NOT NULL,
        -- 'https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt'.
    filed_at                TIMESTAMPTZ NOT NULL,
        -- Trade-date midnight UTC. Publication-time anchor; FINRA
        -- publishes EOD ~6 PM ET.
    period_end              DATE NOT NULL,
        -- Same as trade_date.
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_run_id           UUID NOT NULL,

    PRIMARY KEY (instrument_id, trade_date, market, source_document_id)
) PARTITION BY RANGE (trade_date);

-- Quarterly partitions covering the public RegSHO archive window. The
-- archive is well-defined from 2008-01-02 onward; we materialise from
-- 2024-Q1 forward to bound on-disk size for v1 (~252 trading days/year ×
-- 6 prefixes × ~5k matched instruments/file ≈ 7M rows/year). Extended
-- historical backfill (pre-2024-Q1) lands via operator runbook + a
-- one-line ALTER + new partition.
--
-- Forward window: 2024-Q1 through 2030-Q1 inclusive = 25 partitions
-- (loop bound `q_start < '2030-04-01'` creates the 2030-Q1 partition
-- as its last iteration). Gives ~4 years of cron runway past current
-- date (2026-05-18). The next partition-extension migration must land
-- before 2030-Q2 (operator runbook + skill checklist tracked in
-- `.claude/skills/data-sources/finra.md` §6.4 — RegSHO daily partition
-- extension).
DO $$
DECLARE
    q_start DATE := '2024-01-01';
    q_end   DATE;
    q_name  TEXT;
BEGIN
    WHILE q_start < '2030-04-01' LOOP
        q_end := q_start + INTERVAL '3 months';
        q_name := format(
            'finra_regsho_daily_observations_p_%sq%s',
            EXTRACT(YEAR FROM q_start)::INT,
            EXTRACT(QUARTER FROM q_start)::INT
        );
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF finra_regsho_daily_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            q_name, q_start, q_end
        );
        q_start := q_end;
    END LOOP;
END$$;

-- Operator chart queries: "GME daily short volume over the last 30 days."
CREATE INDEX idx_finra_regsho_obs_instrument_trade
    ON finra_regsho_daily_observations (instrument_id, trade_date DESC);

-- Source/audit queries: "every row from CNMS_20260515".
CREATE INDEX idx_finra_regsho_obs_source_doc
    ON finra_regsho_daily_observations (source_document_id);

COMMIT;
```

### 5.3 PK + revision contract

PK `(instrument_id, trade_date, market, source_document_id)` lets the CNMS aggregate (`market='B,Q,N'`, `source_document_id='CNMS_YYYYMMDD'`) coexist with per-facility rows (`market='B'`, `source_document_id='FNQC_YYYYMMDD'`) for the same `(instrument, trade_date)`. Both are distinct facts: the CNMS aggregate is the union-of-facilities daily volume; the per-facility rows attribute volume to specific reporting facilities. Operators querying "total short volume on day X" should filter `WHERE source_document_id LIKE 'CNMS_%'`; operators wanting facility breakdown filter `WHERE source_document_id NOT LIKE 'CNMS_%'`.

Same-day re-fetch contract (revision-window path):
- `ON CONFLICT (instrument_id, trade_date, market, source_document_id) DO UPDATE SET` every non-key column. Latest fetch wins. Captures FINRA in-place revisions.

### 5.4 Why no `_current` snapshot

The daily file IS the per-day snapshot. Per-instrument latest-trade-date queries are O(log N) against the `(instrument_id, trade_date DESC)` index — same shape as a materialised `_current` table would give. A separate `_current` table would add maintenance complexity without query-speed benefit at this cardinality (~7M rows/year, indexed).

## 6. Provider — `app/providers/implementations/finra_regsho.py`

### 6.1 Public surface

```python
"""FINRA RegSHO Daily Short Volume CDN provider (#916).

Anonymous CDN access at https://cdn.finra.org/equity/regsho/daily/.
Reuses the FINRA throttle clock + lock from
``finra_short_interest`` — same host (cdn.finra.org), shared 1 req/s
polite floor budget.

Six prefixes per trading day:
  - CNMS (Consolidated NMS — aggregated across facilities)
  - FNQC (FINRA/NASDAQ TRF Chicago)
  - FNRA (ADF — legacy alt display facility, often empty)
  - FNSQ (FINRA/NASDAQ TRF Carteret)
  - FNYX (FINRA/NYSE TRF)
  - FORF (ORF — OTC reporting)
"""

PREFIXES: Final[tuple[str, ...]] = ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")


class FinraRegShoProvider:
    BASE_URL: Final[str] = "https://cdn.finra.org/equity/regsho/daily/"

    def __init__(self, http_client: ResilientClient | None = None) -> None:
        # ResilientClient injected SHARES throttle state with
        # FinraShortInterestProvider via imported _FINRA_RATE_LIMIT_CLOCK
        # + _FINRA_RATE_LIMIT_LOCK module globals.
        ...

    def regsho_daily_url(self, trade_date: date, prefix: str) -> str:
        """BASE_URL + '{PREFIX}shvol{YYYYMMDD}.txt'."""

    def fetch_regsho_daily_file(self, trade_date: date, prefix: str) -> bytes:
        """GET the daily file bytes. 404 → FinraNotFound (reused from
        bimonthly module). 5xx → HTTPStatusError.
        """
```

`PREFIXES` is module-level constant — the JOB iterates over it; tests pin the tuple identity.

### 6.2 Error contract — same as #915

| Condition | Raises |
|---|---|
| 404 | `FinraNotFound(trade_date, prefix)` — file not yet published / holiday |
| 5xx after retries | `httpx.HTTPStatusError` (re-raised) |
| Timeout / connect | `httpx.TimeoutException` / `httpx.ConnectError` |

## 7. Service — `app/services/finra_regsho_ingest.py`

### 7.1 Public surface

```python
@dataclass(frozen=True)
class RegShoDailyIngestStats:
    trade_date: date
    prefix: str
    rows_parsed: int = 0
    rows_resolved: int = 0
    rows_upserted: int = 0
    skipped_no_instrument_match: int = 0
    skipped_ambiguous_symbol: int = 0
    skipped_invalid_row: int = 0
    failed: bool = False
    error_detail: str | None = None


def ingest_regsho_daily_file(
    conn: psycopg.Connection[Any],
    trade_date: date,
    prefix: str,
    raw_bytes: bytes,
    resolver: Callable[[str], int | None],
    ingest_run_id: UUID,
) -> RegShoDailyIngestStats:
    """Parse + UPSERT + manifest write. SQL-only — caller owns txn."""
```

The `build_preloaded_symbol_resolver` + `normalise_symbol` helpers are imported verbatim from `app.services.finra_short_interest_ingest` — same shape required, no duplication.

### 7.2 Header + footer validation (spike §3.6)

```python
_EXPECTED_HEADER: tuple[str, ...] = (
    "Date", "Symbol", "ShortVolume", "ShortExemptVolume", "TotalVolume", "Market",
)
```

Algorithm:
1. Decode UTF-8.
2. Split on `\r?\n`, strip trailing empty lines.
3. First non-empty line == header (`|`-join) → must equal `_EXPECTED_HEADER` or raise `HeaderCorruptionError`.
4. Last non-empty line is the footer integer (single token, parses as `int`). Capture it.
5. Body = lines `[1 : -1]`. Parse each via **`parts = line.split('|')`** (no `maxsplit` — bare split). Assert **`len(parts) == 6`** per row; row that mismatches is a per-row defect (`skipped_invalid_row` counter, file continues). The `Market` column is comma-joined on CNMS (e.g. `B,Q,N`) but contains no `|` characters, so an extra `|` always indicates a malformed row.
6. **Body-Date validation (Codex 1a r1 MED — fixture / CDN-mismatch trap):** for every parsed row, assert `parts[0] == trade_date.strftime('%Y%m%d')`. Any mismatch is a file-level defect — raise `HeaderCorruptionError` (same severity as header / footer mismatch). Without this guard, a CDN path mistake or fixture seeded under the wrong date silently writes facts under the caller's `trade_date` while the body's date column is ignored.
7. **Footer-row-count validation:** after iterating the body, assert `body_row_count == footer_int`. Mismatch = `HeaderCorruptionError`.

### 7.3 Manifest UPSERT (per-file, inside the JOB-owned txn)

```sql
INSERT INTO sec_filing_manifest (
    accession_number, cik, form, source,
    subject_type, subject_id, instrument_id,
    filed_at, accepted_at, primary_document_url,
    is_amendment, amends_accession,
    ingest_status, parser_version, raw_status,
    last_attempted_at, next_retry_at, error
) VALUES (
    %(accession_number)s,           -- 'FINRA_REGSHO_{PREFIX}_{YYYYMMDD}'
    'FINRA_REGSHO',                 -- synthetic cik
    'REGSHO',                       -- synthetic form
    'finra_regsho_daily',
    'finra_universe',
    'FINRA_REGSHO',                 -- singleton subject_id (NOT per-prefix)
    NULL,
    %(filed_at)s,                   -- trade-date midnight UTC
    NULL,
    %(primary_document_url)s,
    FALSE, NULL,
    'parsed',
    'finra-regsho-daily-v1',
    'stored',
    NOW(), NULL, NULL
)
ON CONFLICT (accession_number) DO UPDATE SET
    filed_at = EXCLUDED.filed_at,
    primary_document_url = EXCLUDED.primary_document_url,
    ingest_status = 'parsed',
    parser_version = EXCLUDED.parser_version,
    raw_status = 'stored',
    last_attempted_at = NOW(),
    next_retry_at = NULL,
    error = NULL;
```

After the UPSERT runs, the service calls `seed_freshness_for_manifest_row(conn, subject_type='finra_universe', subject_id='FINRA_REGSHO', source='finra_regsho_daily', cik='FINRA_REGSHO', instrument_id=None, accession_number=<accession>, filed_at=<filed_at>)` — same pattern as #915 Codex 2 r1 HIGH 1 — so the freshness panel sees the daily slot.

### 7.4 Empty-file handling (FNRA — spike §3.5)

`FNRAshvol{YYYYMMDD}.txt` is consistently header + footer `0`. The parser MUST treat zero-row body as success:

- `rows_parsed = 0`, `rows_upserted = 0`, `failed = False`.
- Manifest row still written `parsed` (audit-trail completeness — file fetched, parsed, has zero rows).
- Freshness seeded.

### 7.5 Per-row defect handling

| Defect class | Granularity | Counter | Outcome |
|---|---|---|---|
| Header row absent / column-count mismatch | File-level | n/a | `HeaderCorruptionError` raised → file rejected, txn rolled back |
| Footer mismatch (parsed body row count != footer int) | File-level | n/a | `HeaderCorruptionError` raised → file rejected |
| Per-row missing field (truncated row) | Row-level | `skipped_invalid_row` | row skipped, file continues |
| Per-row malformed numeric (non-decimal in volume) | Row-level | `skipped_invalid_row` | row skipped, file continues |
| Per-row blank Symbol | Row-level | `skipped_invalid_row` | row skipped, file continues |
| Symbol resolves to no instrument | Row-level | `skipped_no_instrument_match` | row skipped, file continues |
| Symbol ambiguous (normalisation collapses 2+ instruments) | Row-level | `skipped_ambiguous_symbol` | row skipped, file continues |

## 8. ScheduledJob — `app/jobs/finra_regsho_daily_refresh.py`

### 8.1 Constants + dispatch

```python
JOB_FINRA_REGSHO_DAILY_REFRESH = "finra_regsho_daily_refresh"

PREFIXES = ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")


def _trade_dates_to_fetch(
    now: datetime,
    backfill_window_days: int = 30,
) -> list[date]:
    """Enumerate weekdays falling within ``[now - backfill_window_days, now]``.

    Weekend filter only (Saturday/Sunday excluded). US federal holidays
    are NOT filtered out at enumeration time — the 404 path returns
    FinraNotFound + the JOB skips silently. Same accepted v1 limitation
    as #915's _walk_back_to_weekday.
    """


@dataclass(frozen=True)
class RegShoDailyRefreshStats:
    daily_files: list[RegShoDailyIngestStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int: ...
    @property
    def failed_files(self) -> int: ...


_ACCESSION_PREFIX = "FINRA_REGSHO_"


def _parse_accession(accession: str, *, allowed_prefixes: tuple[str, ...]) -> tuple[date, str] | None:
    """Reverse the synth accession ``FINRA_REGSHO_{PREFIX}_{YYYYMMDD}``.

    The prefix segment is fixed-length (4 chars) but `_parse_accession`
    does NOT assume that — it rsplit-tail-parses the date instead.

    Returns ``(trade_date, prefix)`` on a clean parse; ``None`` on any
    malformation (unknown prefix, malformed date, missing prefix
    tag). Callers SKIP None results from the manifest filter — a
    malformed accession in the manifest table never causes the cron to
    re-fetch every file.

    Algorithm (Codex 1a r1 MED — sibling bimonthly accession shape
    is FINRA_SI_YYYYMMDD; this one is 4-token with prefix in the
    middle, so a naive split would break):
      1. ``startswith("FINRA_REGSHO_")`` — else None.
      2. tail = accession[len("FINRA_REGSHO_"):]
      3. ``prefix_part, date_part = tail.rsplit("_", 1)``
      4. ``prefix_part in allowed_prefixes`` — else None.
      5. ``datetime.strptime(date_part, "%Y%m%d").date()`` — None on ValueError.
    """


def run_finra_regsho_daily_refresh(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
    backfill_window_days: int = 30,
    provider: FinraRegShoProvider | None = None,
) -> RegShoDailyRefreshStats:
    """Per-fire orchestration.

    1. resolver = build_preloaded_symbol_resolver(conn)
    2. candidate_dates = _trade_dates_to_fetch(now, backfill_window_days)
    3. SELECT accession_number FROM sec_filing_manifest
         WHERE source='finra_regsho_daily' AND ingest_status='parsed'
       → already_parsed_pairs: set[tuple[date, str]]
       Build via _parse_accession with allowed_prefixes=PREFIXES; skip None.
    4. revision_window_pairs = {(d, p) for d in sorted(candidate_dates)[-2:] for p in PREFIXES}
    5. all_pairs = {(d, p) for d in candidate_dates for p in PREFIXES}
    6. targets = sorted(all_pairs - already_parsed_pairs | revision_window_pairs)
    7. For each (trade_date, prefix) in targets:
         fetch → store_raw + conn.commit() (with try-except + rollback)
               → with conn.transaction(): ingest_regsho_daily_file(...)
    8. RuntimeError on failed_files > 0.
    """
```

### 8.2 ScheduledJob registration in `app/workers/scheduler.py`

```python
JOB_FINRA_REGSHO_DAILY_REFRESH = "finra_regsho_daily_refresh"

ScheduledJob(
    name=JOB_FINRA_REGSHO_DAILY_REFRESH,
    display_name="FINRA RegSHO daily short volume refresh (#916)",
    source="finra",
    description=(
        "G6 — daily Short Sale Volume ingest. Daily 23:00 UTC probes "
        "the FINRA RegSHO daily CDN for new (trade_date, prefix) files "
        "at https://cdn.finra.org/equity/regsho/daily/. Six prefixes per "
        "trading day (CNMS / FNQC / FNRA / FNSQ / FNYX / FORF). Skips "
        "manifest-parsed (trade_date, prefix) pairs EXCEPT the two most-"
        "recent trade dates (revision window — FINRA corrects daily files "
        "in-place within 1-2 cycles). Per-file: store_raw (raw payload "
        "before parse, #1168) → parse pipe-delim → preloaded symbol "
        "resolver (~13k instruments) → bulk-UPSERT "
        "finra_regsho_daily_observations → manifest UPSERT 'parsed' "
        "(synth no-op parser dispatch shape). Per-file failure isolated. "
        "Default backfill window 30 days; extended-window backfill via "
        "REPL runbook."
    ),
    cadence=Cadence.daily(hour=23, minute=0),
    catch_up_on_boot=False,
    prerequisite=_bootstrap_complete,
),
```

### 8.3 Lane + manual-trigger registration

Already-existing `finra` Lane reused. Only `MANUAL_TRIGGER_JOB_SOURCES` entry needed at `app/jobs/sources.py`:

```python
"finra_regsho_daily_refresh": "finra",
```

No `MANUAL_TRIGGER_JOB_METADATA` entry — zero-param v1 manual surface (same rationale as #915 Codex 1b r1 HIGH 1: `_adapt_zero_arg` discards body params; extended backfill lands via REPL runbook).

### 8.4 `_INVOKERS` registration in `app/jobs/runtime.py`

```python
_INVOKERS[_scheduler.JOB_FINRA_REGSHO_DAILY_REFRESH] = _adapt_zero_arg(
    _scheduler.finra_regsho_daily_refresh
)
```

The `scheduler.finra_regsho_daily_refresh` body wraps `_tracked_job` + opens its own DB connection + calls `run_finra_regsho_daily_refresh` (mirror G6/#915 + G12 shape).

## 9. Manifest parser — `app/services/manifest_parsers/finra_regsho_daily.py`

Synth no-op per `sec_xbrl_facts` + #915 precedent. The ScheduledJob writes the manifest row directly as `parsed`; this parser exists only to satisfy the manifest-worker dispatch invariant on the rare `sec_rebuild --source=finra_regsho_daily` path.

```python
"""finra_regsho_daily manifest-worker parser — synth no-op (G6/#916).

FINRA RegSHO daily short volume data lands via the
``finra_regsho_daily_refresh`` ScheduledJob (daily 23:00 UTC). The
ScheduledJob owns the fetch + parse + UPSERT into
``finra_regsho_daily_observations``, then UPSERTs the manifest row as
``ingest_status='parsed'`` directly.

This parser exists only to satisfy the manifest-worker dispatch
invariant on the rare ``sec_rebuild --source=finra_regsho_daily`` path.
If the operator flips manifest rows back to ``pending``, the worker
needs a registered parser to mark them ``parsed`` again WITHOUT
triggering a network fetch or DB write. Actual re-ingest mechanism is
re-firing the ScheduledJob.

Architectural siblings: sec_xbrl_facts (G7) + finra_short_interest
(G6/#915).

ParseOutcome contract:
- status='parsed' always.
- parser_version='finra-regsho-daily-v1'.
- No network call.
- No DB write.
"""
```

Registration in `app/services/manifest_parsers/__init__.py::register_all_parsers`:

```python
from app.services.manifest_parsers import finra_regsho_daily as _finra_regsho_daily
# ...
_finra_regsho_daily.register()  # synth no-op (G6/#916)
```

## 10. Tests

| File | Purpose |
|---|---|
| `tests/test_finra_regsho_daily_provider.py` | Provider unit — URL builder per prefix; 404 → `FinraNotFound`; 5xx → HTTPStatusError; rate-limit clock IDENTITY-shared with bimonthly provider; back-to-back throttle smoke. |
| `tests/test_finra_regsho_daily_ingest.py` | Service integration against `ebull_test_conn` — happy path against `CNMS_panel_20260515.txt` fixture (5 panel instruments resolve); empty file (`FNRA_empty_20260515.txt`) → `rows_parsed=0, rows_upserted=0, failed=False, manifest written 'parsed'`; header corruption → `HeaderCorruptionError`; footer mismatch → `HeaderCorruptionError`; per-row defects (missing field / malformed numeric / blank Symbol) → row-skip counters; multi-prefix coexistence (CNMS aggregate + FNQC facility for same `(instrument, trade_date)` both land in the table). |
| `tests/test_finra_regsho_daily_refresh.py` | ScheduledJob integration — `_trade_dates_to_fetch` weekday filter; manifest filter excludes already-parsed `(trade_date, prefix)` pairs; revision-window unions in the 2 most-recent dates × 6 prefixes; per-file failure isolated + successful files committed; partial-failure RuntimeError raised. |
| `tests/test_finra_regsho_daily_scheduler_wiring.py` | Wiring invariants — `JOB_FINRA_REGSHO_DAILY_REFRESH` constant; `ScheduledJob` entry shape (source='finra', cadence daily 23:00 UTC, prerequisite=_bootstrap_complete); `_INVOKERS` identity; `source_for()` resolves without KeyError; `MANUAL_TRIGGER_JOB_SOURCES` entry. |
| `tests/test_finra_regsho_daily_manifest_parser.py` | Parser invariants — synth no-op contract (no network / no DB writes); `ParseOutcome(status='parsed', parser_version='finra-regsho-daily-v1')`; registry wiring after `clear_registered_parsers` + `register_all_parsers`. |
| `tests/test_layer_123_wiring.py` | Extended with Layer-4 row for `finra_regsho_daily_refresh`. |
| `tests/test_universal_gate_carve_out.py` | Positive assertion that `finra_regsho_daily_refresh` is NOT in the exempt allow-list (it has a real prerequisite). |
| `tests/test_fetch_document_text_callers.py` | Allow-list extended for the new provider + parser modules (per #453 contract). |
| `tests/fixtures/finra/regsho/CNMS_panel_20260515.txt` | Pristine — header + 5 panel rows (AAPL / GME / HD / JPM / MSFT) + footer `5`, CRLF, verbatim shape from live `CNMSshvol20260515.txt` fetch on 2026-05-18 (spike §3.8). |
| `tests/fixtures/finra/regsho/FNRA_empty_20260515.txt` | Pristine — header + footer `0`, CRLF, the empty-prefix shape from a live FNRA fetch. |
| `tests/fixtures/finra/regsho/CNMS_defects_20260515.txt` | Synthetic — header + truncated-row + malformed-decimal + blank-Symbol + footer mismatch. Hand-written; committed once. |

## 11. Operator-visible figures + ETL DoD #8-#12

| Clause | Evidence |
|---|---|
| #8 Smoke against AAPL / GME / MSFT / JPM / HD | All 5 resolve directly against `instruments` (verified in spike §3.8 against live CNMS 2026-05-15). After a single `finra_regsho_daily_refresh` fire on dev DB against the 2026-05-15 trade date, `SELECT * FROM finra_regsho_daily_observations WHERE instrument_id IN (...) AND trade_date='2026-05-15' AND source_document_id LIKE 'CNMS_%'` returns one row per panel symbol. PR body records the figures observed. |
| #9 Cross-source verify one ticker | Compare GME 2026-05-15 `short_volume` from CNMS aggregate against nasdaq.com short-volume page or marketbeat.com daily volume page. ±5% tolerance acknowledged. PR body records source + figure compared. |
| #10 Backfill | REPL invocation on dev DB: `python -c "from psycopg import connect; from app.settings import settings; from app.jobs.finra_regsho_daily_refresh import run_finra_regsho_daily_refresh; \\\nwith connect(settings.database_url) as c: print(run_finra_regsho_daily_refresh(c, backfill_window_days=90))"`. ~63 trading days × 6 prefixes = 378 fetches at 1 req/s ≈ 6 min wall-clock. PR body records invocation + outcome. |
| #11 Operator-visible figure | No live `/instruments/{symbol}/regsho-daily` endpoint in v1 (memo overlay deferred). Operator verifies via direct SQL: `SELECT trade_date, market, short_volume, total_volume FROM finra_regsho_daily_observations WHERE instrument_id=<...> AND source_document_id LIKE 'CNMS_%' ORDER BY trade_date DESC LIMIT 10`. PR body records the SELECT output for AAPL / GME. |
| #12 PR records verification + SHA | PR body explicit table. |

## 12. Out of scope

| Item | Reason |
|---|---|
| Frontend daily sparkline (issue acceptance #2) | Plan §1 autonomy contract UI carve-out. OBSERVATIONS PRIMITIVE closure framing. |
| Short borrow rate / utilisation | Vendor-paid; out of scope per settled-decisions #532. |
| Pre-2024-Q1 historical backfill | Static partition window is 2024-Q1 → 2030-Q1 inclusive (25 quarterly partitions). Extended historical lands via operator-runbook ALTER + new partition (separate ticket if needed). |
| `_current` materialised snapshot | Daily file IS the snapshot; partitioned observations table indexed on `(instrument_id, trade_date DESC)` is fast enough. |

## 13. Acceptance

1. **Schema** — `finra_regsho_daily_observations` partitioned by trade_date quarterly buckets (2024-Q1 → 2030-Q1 inclusive, 25 partitions) lands via `sql/153_finra_regsho_daily_enum.sql` + `sql/154_finra_regsho_daily.sql`. The two enum-extension migrations widen `sec_filing_manifest.source`, `data_freshness_index.source`, and `filing_raw_documents.document_kind` in lock-step.
2. **Provider** — `FinraRegShoProvider` exposes URL builder per prefix + `fetch_regsho_daily_file(date, prefix)` with throttle state IDENTITY-shared with the bimonthly provider.
3. **Service** — `ingest_regsho_daily_file(conn, trade_date, prefix, raw_bytes, resolver, ingest_run_id)` parses + UPSERTs + writes manifest + seeds freshness. Emits SQL only against caller's open transaction. Empty file (FNRA shape) → success path with zero rows + manifest still written. Header corruption / footer mismatch → `HeaderCorruptionError`. Per-row defects skipped + counters tracked.
4. **ScheduledJob** — `finra_regsho_daily_refresh` (daily 23:00 UTC, lane=finra, prereq=`_bootstrap_complete`) opens its own connection, enumerates weekday trade dates × 6 prefixes (with revision-window re-fetch), filters against manifest, per file: `store_raw + conn.commit() + with conn.transaction(): ingest_regsho_daily_file(...)`. Logs aggregated stats; logs match-rate warning <50%; raises `RuntimeError` if `failed_files > 0`.
5. **Manifest parser** — synth no-op registered alongside G7 / G6.
6. **Smoke** — AAPL / GME / MSFT / JPM / HD resolve + write observations rows on a real-fixture fire against 2026-05-15 CNMS file.
7. **Cross-source** — GME 2026-05-15 short volume from CNMS within 5% of independent source.
8. **Backfill** — REPL `run_finra_regsho_daily_refresh(conn, backfill_window_days=90)` re-drives 90-day window without error; rows land in expected quarterly partitions.
9. **Matrix delta** — `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 `finra_short_interest` row + §7 G6 row updated. The G6 row notes both bimonthly + daily wired.
10. **Skill** — `.claude/skills/data-sources/finra.md` daily row flips to `WIRED 2026-05-18`; §2.5 expanded with decimal-volume + multi-prefix Market notes; new §6.4 (daily runbook) added.
11. **Memory** — `[[us-source-coverage]]` updated to reflect G6 daily wired. New `project_916_finra_regsho_daily.md` memory note.
12. **Local gates** — `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest` all green.
13. **Codex** — 1a (spec) CLEAN; 1b (plan) CLEAN; 2 (pre-push) CLEAN.
14. **Bot review** — APPROVE on the most recent commit + CI green.
