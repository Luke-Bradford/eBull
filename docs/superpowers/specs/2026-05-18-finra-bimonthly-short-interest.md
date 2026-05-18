# FINRA bimonthly short interest ingest + schema (#915) — spec

> Phase: 6 of ``docs/superpowers/plans/2026-05-17-us-etl-completion.md`` (PR 11).
> Issue: #915 (OPEN). Parent #845 CLOSED.
> Spike: ``docs/superpowers/spikes/2026-05-18-finra-bimonthly-short-interest-feasibility.md`` (verdict: SHIP Option A).
> Architectural siblings: G7 ``sec_xbrl_facts`` ScheduledJob+synth-noop-parser pattern (PR #1190); G12 ``sec_master_idx_quarterly_sweep`` per-fire HTTP fetch + preloaded subject resolver (PR #1196).
> Closure framing: **OBSERVATIONS PRIMITIVE** (mirror G10/G11 Phase 4 "PROVIDER PRIMITIVE"). Ingest layer wired end-to-end; chart memo overlay (issue acceptance #2) deferred per plan §1 autonomy contract UI carve-out.

## 1. Goal

Ingest FINRA's bimonthly **Equity Short Interest** publication into a new ``finra_short_interest_observations`` table (history) + ``finra_short_interest_current`` snapshot (latest per instrument), under a new ``finra`` lane disjoint from ``sec_rate``. Operator can:

- Query "GME days-to-cover at any post-2021-06 settlement" against ``finra_short_interest_observations``.
- Read the latest settlement figure for any in-universe instrument from ``finra_short_interest_current``.
- Trigger backfill via ``POST /jobs/finra_short_interest_refresh/run`` (no body — fixed 400-day window in v1; extended-window backfill via REPL runbook §13).

**Cohort caveat** (spike §4.2): pre-June 2021 FINRA archive is **OTC-only**. Post-June 2021 expanded to include exchange-listed securities. Smoke target is GME at the most-recent settlement (e.g. 2026-04-30); historical reach is bounded by ``settlement_date >= 2021-07-15`` for exchange-listed cohort. Spec §13 acceptance pins the post-2021 cohort rule.

Memo overlay (FE) — deferred. No new UI ticket opened; closure framing documents the deferral.

**Naming** — tables prefix ``finra_short_interest_*`` per parent plan §2 Phase 6 PR 11. Distinguishes from the planned PR 12 ``finra_regsho_daily_observations`` sibling (#916).

## 2. Settled-decisions check

| Decision | Relevance | Preservation |
|---|---|---|
| §"Free regulated-source-only" (#532) | FINRA is an SEC-overseen SRO; Equity Short Interest publication is mandated under FINRA Rule 4560 + SEC Rule 10a-1. CDN is free + anonymous. | PRESERVED. |
| §"Provider design rule" (providers thin, DB lookups in services) | New ``app/providers/implementations/finra_short_interest.py`` is HTTP-only (URL builder + ``ResilientClient`` GET + decode). Service layer at ``app/services/finra_short_interest_ingest.py`` owns symbol resolver + DB writes. | PRESERVED. |
| §"Filing event storage" (#1168 — raw payload before parse) | Pipe-delim bytes stored in ``filing_raw_documents`` keyed by synthetic accession ``FINRA_SI_{YYYYMMDD}`` BEFORE any parse step. | PRESERVED. |
| §"Manifest source-of-truth" (#864 / sql/118) | ``finra_short_interest`` source + ``finra_universe`` subject_type + ``'FINRA_SI'`` subject_id singleton — all three pre-exist in sql/118 + sql/120. This PR populates the slot. | PRESERVED. |
| §"Data freshness index" (#865 / sql/120) | ``app/services/data_freshness.py:101`` declares ``finra_short_interest: timedelta(days=20)``. Manifest writes drive the freshness panel; no schema change. | PRESERVED. |

## 3. Prevention-log check

| Entry | File:line | How spec honours it |
|---|---|---|
| ResilientClient shared throttle (#726) | review-prevention-log.md:510-513 | **Independent host = independent pool.** New module-global ``_FINRA_RATE_LIMIT_CLOCK: list[float]`` + ``_FINRA_RATE_LIMIT_LOCK: threading.Lock`` in ``app/providers/implementations/finra_short_interest.py``. ``min_request_interval_s=1.0`` (polite default). No SEC pool sharing by construction. |
| Raw-payload-before-parse (#1168) | review-prevention-log entry | Job ordering per settlement file: ``store_raw`` → ``conn.commit()`` (raw durable) → ``with conn.transaction():`` { parse rows → bulk-upsert observations → upsert _current → manifest UPSERT ``parsed`` } → ``with`` exits → commit. Mirrors ``n_port_ingest.py:786-798``. |
| Pydantic validation cliff | feedback_pydantic_validation_cliff.md | N/A — pipe-delim text, stdlib ``csv.DictReader``. |
| Universal-gate supersession | feedback_universal_gate_supersession.md | New ``finra`` Lane added to ``Lane`` Literal; ``test_universal_gate_carve_out.py`` extended to assert no implicit carve-out for ``finra_short_interest_refresh``. |
| Skills must own integrity | feedback_skills_must_own_integrity.md | ``.claude/skills/data-engineer/etl-endpoint-coverage.md`` §2 FINRA row + §7 G6 row updated. New ``.claude/skills/data-sources/finra.md`` source-of-truth note created — endpoints, formats, rate-limit posture, symbol-norm discipline. |
| psycopg3 savepoint-not-commit | feedback_psycopg3_savepoint_commit.md | **JOB body** opens ``with conn.transaction():`` around each per-file parse-and-upsert; service emits SQL only — never enters its own transaction context (would silently commit at top-level under autocommit conn). Raw-payload commit happens BEFORE the per-file ``with conn.transaction():`` block (explicit ``conn.commit()`` between Phase 1 store_raw + Phase 2 service call). |
| Universal-gate supersession + universal-gate carve-out (G12 precedent) | tests/test_universal_gate_carve_out.py | New job ``finra_short_interest_refresh`` is added to the positive allowlist; the disjoint-lane invariant test asserts ``finra`` lane shares no jobs with ``sec_rate`` / ``db`` / other lanes. |
| Documentation layering | feedback_documentation_layering.md | Spec doc (this file) is the **architecture-decision-of-record**; settled-decisions and prevention-log get cross-references; the new data-sources skill is the **per-source operator runbook**. |

## 4. Architecture (Option A from spike §5.1)

```
Cron: finra_short_interest_refresh (daily 12:00 UTC, lane=finra, prerequisite=_bootstrap_complete)
  │
  ▼
ScheduledJob body — app/workers/scheduler.py::finra_short_interest_refresh()
  │   -- opens its OWN conn via psycopg.connect(settings.database_url)
  │   -- owns ALL conn.commit() / conn.rollback() calls (G12 precedent)
  │
  ├─ resolver = build_preloaded_symbol_resolver(conn)   # one-shot SELECT
  │
  ├─ candidate_dates = _settlement_dates_to_fetch(
  │       now=datetime.now(UTC),
  │       backfill_window_days=400,
  │   )
  ├─ already_parsed_set = SELECT settlement_date_from_accession FROM sec_filing_manifest
  │                       WHERE source='finra_short_interest' AND ingest_status='parsed'
  ├─ # Revision window — re-fetch the most-recent N=2 settlements
  ├─ # unconditionally. FINRA publishes in-place revisions (revisionFlag='Y')
  ├─ # within 1-2 cycles of the original snapshot; the manifest's 'parsed'
  ├─ # status alone would otherwise mask in-place corrections.
  ├─ revision_window = sorted(candidate_dates)[-2:]
  ├─ targets = sorted(set(candidate_dates) - already_parsed_set | set(revision_window))
  │
  ├─ FOR EACH settlement_date in targets:
  │      try:
  │          raw_bytes = provider.fetch_settlement_file(settlement_date)
  │      except FinraNotFound:
  │          continue                       # 404 = file not yet published
  │      except (HTTPStatusError, Timeout, ConnectError) as exc:
  │          stats.append(SettlementIngestStats(settlement_date=date, failed=True,
  │                                              error_detail=str(exc)))
  │          continue                       # next file
  │
  │      # Phase 1: durable raw-payload store BEFORE parse (#1168).
  │      raw_filings.store_raw(
  │          conn,
  │          accession_number=f"FINRA_SI_{YYYYMMDD}",
  │          document_kind='finra_short_interest_csv',
  │          payload=raw_bytes.decode('utf-8'),
  │          source_url=file_url,
  │      )
  │      conn.commit()                      # raw durable BEFORE parse
  │
  │      # Phase 2: parse + upserts inside JOB-owned txn (NO commit
  │      # inside service). Job wraps service call in
  │      # `with conn.transaction():`. On clean exit it commits; on
  │      # exception it rolls back. Raw payload stays durable from
  │      # earlier Phase 1 commit.
  │      try:
  │          with conn.transaction():       # JOB owns commit/rollback
  │              per_file = ingest_settlement_file(
  │                  conn, settlement_date, raw_bytes, resolver, ingest_run_id,
  │              )
  │          # `with` exited cleanly => committed
  │          stats.append(per_file)
  │      except Exception as exc:
  │          # `with` rolled back automatically on the raised exception;
  │          # raw payload persists from Phase 1 store_raw + commit.
  │          stats.append(SettlementIngestStats(settlement_date=date, failed=True,
  │                                              error_detail=str(exc)))
  ▼
ScheduledJob:
  tracker.row_count = sum(s.rows_upserted for s in stats)
  logger.info("finra_short_interest_refresh: files=%d upserted=%d skipped_no_match=%d "
              "skipped_ambiguous=%d skipped_invalid=%d failed=%d",
              len(stats), total_upserted, total_skipped_no_match,
              total_skipped_ambiguous, total_skipped_invalid, failed_files)
  if total_resolved > 0 and total_resolved / total_parsed < 0.50:
      logger.warning("finra_short_interest_refresh: match rate %.2f%% below 50%% "
                     "threshold (parsed=%d resolved=%d) — universe drift or FINRA "
                     "column-shape regression suspected",
                     100 * total_resolved / total_parsed, total_parsed, total_resolved)
  if failed_files > 0:
      raise RuntimeError(
          f"finra_short_interest_refresh: {failed_files} of {len(stats)} files failed",
      )
```

Per-file failure isolation: HTTP / decode / parse / DB errors on one settlement_date trigger the **JOB-owned** ``conn.rollback()`` of the parse-and-upsert transaction; the raw-payload store committed before is untouched (durable for later rebuild via the parser-version path). Successful files commit before the next iteration. Job-level ``status='failure'`` raised by the explicit ``RuntimeError`` if ``failed_files > 0`` (mirrors G12 ``run_master_idx_quarterly_sweep`` partial-failure visibility contract at ``app/workers/scheduler.py:4625-4646``).

**Service does NOT commit AND does NOT open its own transaction.** ``ingest_settlement_file`` accepts the caller-supplied connection and emits SQL only. The JOB body wraps the call site in ``with conn.transaction():`` so commit/rollback ownership stays with the JOB, per the prevention-log "service that accepts an external connection must not commit" invariant + the psycopg3 contract that ``with conn.transaction()`` silently top-level-commits on clean exit when no outer tx is active (Codex 1b r1 HIGH 2).

## 5. Schema

Two-migration split: extend ``filing_raw_documents.document_kind`` first (the raw-payload sink), then create the typed observation tables.

### 5.1 New migration ``sql/151_filing_raw_documents_add_finra_si.sql``

```sql
-- 151_filing_raw_documents_add_finra_si.sql
--
-- Issue #915 (Phase 6 PR 11) — FINRA bimonthly short interest ingest.
-- Adds ``finra_short_interest_csv`` document_kind to filing_raw_documents
-- so the FINRA ingester can persist the pipe-delim file body before
-- parse (raw-payload-before-parse contract, #1168). Distinct kind keeps
-- the re-wash targeting query simple (mirror sql/122 N-PORT + sql/146
-- Form 5 precedent).

BEGIN;

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
        'finra_short_interest_csv'
    ));

COMMIT;
```

The corresponding ``DocumentKind`` Literal at ``app/services/raw_filings.py`` is widened in the same PR to include ``'finra_short_interest_csv'``.

### 5.2 New migration ``sql/152_finra_short_interest.sql``

```sql
BEGIN;

CREATE TABLE finra_short_interest_observations (
    instrument_id           BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    settlement_date         DATE   NOT NULL,
    source_document_id      TEXT   NOT NULL,
        -- For finra_si bimonthly: 'YYYYMMDD' (settlement date as compact string).
        -- Same source_document_id across original + revision (revisionFlag='Y')
        -- payloads — re-ingest UPSERTs the (PK) row. Latest fetch wins.

    current_short_interest      NUMERIC(20, 0) NOT NULL,
    previous_short_interest     NUMERIC(20, 0),
    average_daily_volume        NUMERIC(20, 0),
    days_to_cover               NUMERIC(10, 4),
    change_percent              NUMERIC(10, 4),
    change_previous             NUMERIC(20, 0),
    accounting_yearmonth        INTEGER,
        -- FINRA's accountingYearMonthNumber column. YYYYMM-shaped integer.
    market_class_code           TEXT,
        -- FINRA marketClassCode: 'NYSE' | 'BZX' | 'OTC' | etc.
    exchange_code               TEXT,
        -- FINRA issuerServicesGroupExchangeCode: single letter 'A' | 'S' | 'H' | etc.
    issue_name                  TEXT,
        -- FINRA issueName free text.
    stock_split_flag            TEXT,
        -- '' or 'Y'.
    revision_flag               TEXT,
        -- '' or 'Y'. When 'Y', the row reflects a corrected FINRA snapshot;
        -- caller is expected to UPSERT (overwrite) on PK collision.

    source                  TEXT NOT NULL CHECK (source = 'finra_si'),
        -- Single-element CHECK locks the table to FINRA short interest
        -- bimonthly. The ownership observations enum at sql/113-116
        -- already includes 'finra_si' as a valid value, so the literal
        -- vocabulary is shared. ('finra_si' is the short-form column
        -- value; 'finra_short_interest' is the ManifestSource long form.)
    source_url              TEXT NOT NULL,
        -- 'https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv'.
    filed_at                TIMESTAMPTZ NOT NULL,
        -- Settlement date midnight UTC — the publication-time anchor.
    period_end              DATE NOT NULL,
        -- Same as settlement_date (the fact's valid-time end).
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_run_id           UUID NOT NULL,

    PRIMARY KEY (instrument_id, settlement_date, source_document_id)
) PARTITION BY RANGE (settlement_date);

-- Quarterly partitions covering the exchange-listed cohort (post-June 2021)
-- plus three forward quarters for steady-state writes (2026-Q3, Q4, 2027-Q1).
-- Backfill default window is 400 days (~Q1 2025 → current); maximum-operator-
-- backfill is 730 days (~Q2 2024 → current); the static range spans 2021-Q3
-- through 2027-Q1 = 23 partitions inclusive. Steady-state cron writes always
-- land in the most-recent two partitions; the next operator partition-add
-- runbook fires before end of 2027-Q1.
--
-- Future partition additions: a one-line ALTER + new partition at the
-- start of each quarter. A boot-time helper is OUT OF SCOPE for this PR
-- (sec_filing_manifest does not auto-partition either; same operator
-- runbook applies).

DO $$
DECLARE
    q_start DATE := '2021-07-01';
    q_end   DATE;
    q_name  TEXT;
BEGIN
    WHILE q_start < '2027-04-01' LOOP
        q_end := q_start + INTERVAL '3 months';
        q_name := format(
            'finra_short_interest_observations_p_%sq%s',
            EXTRACT(YEAR FROM q_start)::INT,
            EXTRACT(QUARTER FROM q_start)::INT
        );
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF finra_short_interest_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            q_name, q_start, q_end
        );
        q_start := q_end;
    END LOOP;
END$$;

-- Operator chart queries by instrument over time.
CREATE INDEX idx_finra_si_obs_instrument_settlement
    ON finra_short_interest_observations (instrument_id, settlement_date DESC);

-- Source/audit queries.
CREATE INDEX idx_finra_si_obs_source_doc
    ON finra_short_interest_observations (source_document_id);

-- Materialised _current snapshot. One row per instrument; settled by
-- the service ingester on every UPSERT.
CREATE TABLE finra_short_interest_current (
    instrument_id           BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    settlement_date         DATE   NOT NULL,
    source_document_id      TEXT   NOT NULL,

    current_short_interest      NUMERIC(20, 0) NOT NULL,
    previous_short_interest     NUMERIC(20, 0),
    average_daily_volume        NUMERIC(20, 0),
    days_to_cover               NUMERIC(10, 4),
    change_percent              NUMERIC(10, 4),
    change_previous             NUMERIC(20, 0),
    market_class_code           TEXT,
    exchange_code               TEXT,
    issue_name                  TEXT,

    source_url              TEXT NOT NULL,
    filed_at                TIMESTAMPTZ NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
```

### 5.3 Why ``check (source='finra_si')``

The observations enum at sql/113-116 already includes ``'finra_si'`` literal. Locking the value via single-element CHECK keeps the table dedicated to FINRA short-interest (no accidental cross-source mixing) while keeping the same enum vocabulary used by the ownership family. ``finra_si`` (short form for table column) vs ``finra_short_interest`` (long form for manifest source enum / data-freshness-index source) is the pre-existing pattern across the codebase (sql/116 + sql/118 + ``data_freshness.py``).

### 5.4 PK + revision contract

PK ``(instrument_id, settlement_date, source_document_id)`` mirrors the ownership-decomposition Phase 6 design table at ``docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md:316``. ``source_document_id`` is **always** the settlement_date formatted YYYYMMDD; same value across the original FINRA publication and any revision file at the same settlement date (FINRA publishes revisions in-place rather than as a separate file). The ``revision_flag`` column records whether the row was sourced from a revision (``'Y'``) or the original (``''``).

Same-settlement re-fetch contract:
- ``finra_short_interest_observations`` UPSERT — ``ON CONFLICT (instrument_id, settlement_date, source_document_id) DO UPDATE SET`` every non-key column (latest fetch wins). Captures both genuine revisions and re-ingest from raw_payload.
- ``finra_short_interest_current`` UPSERT — ``ON CONFLICT (instrument_id) DO UPDATE SET`` every non-key column **WHEN** ``excluded.settlement_date > finra_short_interest_current.settlement_date OR (excluded.settlement_date = finra_short_interest_current.settlement_date AND excluded.refreshed_at > finra_short_interest_current.refreshed_at)``. The compound predicate handles same-date revisions correctly: a fresh revision at the current settlement always wins; an older settlement-date payload from rebuild never displaces a newer one.

### 5.5 Partitioning

Range-by-settlement_date quarterly buckets per the ownership precedent (sql/113-116). The migration's ``DO`` loop (``WHILE q_start < '2027-04-01'``) iterates with ``q_start`` ∈ {2021-Q3 … 2027-Q1} — i.e. it creates **23 partitions** inclusive (2021-Q3 + 2021-Q4 + four per year for 2022–2026 + 2027-Q1). Large enough to absorb default 400-day backfill, 730-day operator-extended backfill, and three forward quarters of steady-state writes (through 2027-Q1) before the next operator partition-add runbook fires. Ballpark cardinality: 24 files/year × ~10k matched rows/file × 5 years = ~1.2M rows over the full window — modest.

## 6. Provider — ``app/providers/implementations/finra_short_interest.py``

### 6.1 Public surface

```python
class FinraShortInterestProvider:
    """FINRA Equity Short Interest CDN provider.

    Anonymous CDN access at ``https://cdn.finra.org/equity/otcmarket/biweekly/``.
    No auth. 1 req/s polite floor (FINRA publishes no explicit rate
    limit on the catalog page; CDN robots.txt is 403).
    """

    BASE_URL = "https://cdn.finra.org/equity/otcmarket/biweekly/"

    def __init__(self, http_client: ResilientClient | None = None) -> None:
        ...

    def settlement_file_url(self, settlement_date: date) -> str:
        """Return canonical URL for a settlement-date file.

        Format: BASE_URL + 'shrt{YYYYMMDD}.csv'.
        """
        ...

    def fetch_settlement_file(self, settlement_date: date) -> bytes:
        """GET the settlement file bytes. Raises FinraNotFound on 404
        (file not yet published) or HTTPStatusError on 5xx.
        """
        ...
```

Module-global rate-limit clock + lock declared at module top (mirrors ``app/providers/implementations/sec_edgar.py:54-80``). Shared across multiple ``FinraShortInterestProvider`` instances via ``ResilientClient.shared_last_request`` + ``shared_throttle_lock`` parameters.

### 6.2 Error contract

| Condition | Wrapper raises |
|---|---|
| 404 | ``FinraNotFound(settlement_date)`` — file not published yet. ScheduledJob skips silently (settlement date not yet available). |
| 5xx after 3 retries | ``httpx.HTTPStatusError`` (re-raised). ScheduledJob records the file as failed. |
| Timeout | ``httpx.TimeoutException``. Same handling as 5xx. |
| Connect error | ``httpx.ConnectError``. Same. |

## 7. Service — ``app/services/finra_short_interest_ingest.py``

### 7.1 Public surface

```python
@dataclass(frozen=True)
class SettlementIngestStats:
    settlement_date: date
    rows_parsed: int = 0
    rows_resolved: int = 0
    rows_upserted: int = 0
    skipped_no_instrument_match: int = 0
    skipped_ambiguous_symbol: int = 0
    skipped_invalid_row: int = 0
    failed: bool = False
    error_detail: str | None = None


def build_preloaded_symbol_resolver(
    conn: psycopg.Connection[Any],
) -> Callable[[str], int | None]:
    """One-shot SELECT instrument_id, symbol FROM instruments.

    Returns a closure that normalises the input symbol
    (strip-non-alnum + upper) and looks up the matching instrument_id.

    Returns ``None`` on no-match OR on ambiguous-match (multiple
    instruments collapse to the same normalised key — e.g. ``ABR.PRD``
    and ``ABRPRD`` both → ``ABRPRD``). Ambiguous-match raises an
    in-resolver counter via the closure's ``ambiguous_keys`` attribute
    so the caller can surface counter to job_runs.detail.
    """


def ingest_settlement_file(
    conn: psycopg.Connection[Any],
    settlement_date: date,
    raw_bytes: bytes,
    resolver: Callable[[str], int | None],
    ingest_run_id: UUID,
) -> SettlementIngestStats:
    """Parse + UPSERT short_interest + UPSERT manifest. SQL-only —
    no transaction context manager, no ``conn.commit()`` /
    ``conn.rollback()``. Caller MUST wrap in
    ``with conn.transaction():`` so commit/rollback ownership stays
    with the JOB.

    Reason this matters (Codex 1b r1 HIGH 2): psycopg3
    ``with conn.transaction()`` opens a TOP-LEVEL transaction when
    no outer tx is active — clean-exit commits. The service-no-commit
    invariant requires the caller to own the outer ``with
    conn.transaction()`` so the service body's writes participate in
    a SAVEPOINT (nested) or simply emit SQL into the caller's open tx.

    Pre-conditions (caller-enforced):
      - Raw payload is ALREADY stored + committed in
        ``filing_raw_documents`` (raw-payload-before-parse, #1168).
      - Caller has opened ``with conn.transaction():`` immediately
        before calling this function.
      - ``resolver`` is the preloaded symbol resolver from
        ``build_preloaded_symbol_resolver(conn)``.

    Failure invariants:
      - **Header / structural corruption** (header row missing,
        unexpected column count) → ``HeaderCorruptionError`` raised
        out of the caller's ``with conn.transaction()``; caller's tx
        rolls back. Raw payload stays durable from the earlier
        ``conn.commit()``.
      - **Empty file (0 bytes)** → caught by the JOB before calling
        the service (``store_raw`` rejects empty payloads at
        ``app/services/raw_filings.py:105``). Job records
        ``SettlementIngestStats(failed=True, error_detail='empty file')``
        and continues to the next settlement date.
      - **Per-row defects** (missing field, malformed integer, blank
        symbolCode, unresolved instrument, ambiguous symbol collision) →
        row SKIPPED + per-defect counter incremented in
        ``SettlementIngestStats``. The file as a whole continues.
      - DB integrity errors (PK collision after deduplication, FK
        violation, partition-not-found) → propagate. Caller's
        ``with conn.transaction():`` rolls back atomically: observations
        + _current + manifest writes for THIS settlement_date all
        disappear. The manifest row is NOT written (for first-ingest)
        OR remains at its prior ``parsed`` value (if this was a
        re-fetch within the revision window). Raw payload from
        the earlier Phase 1 ``store_raw`` + ``conn.commit()`` stays
        durable; next fire re-attempts via the same revision-window
        path.

    Returns ``SettlementIngestStats`` — non-failed path; failure path
    raises.
    """
```

### 7.3 Manifest write tuple (synthetic FINRA accession)

Pinned shape for the manifest UPSERT inside ``ingest_settlement_file``:

```sql
INSERT INTO sec_filing_manifest (
    accession_number, cik, form, source,
    subject_type, subject_id, instrument_id,
    filed_at, accepted_at, primary_document_url,
    is_amendment, amends_accession,
    ingest_status, parser_version, raw_status,
    last_attempted_at, next_retry_at, error
) VALUES (
    %(accession_number)s,     -- 'FINRA_SI_YYYYMMDD'
    'FINRA_SI',               -- documented synthetic cik (sql/118:62)
    'SHRT',                   -- synthetic form code (NOT a real SEC form)
    'finra_short_interest',
    'finra_universe',
    'FINRA_SI',               -- documented synthetic subject_id (sql/118:62)
    NULL,                     -- required NULL per chk_manifest_issuer_has_instrument
    %(filed_at)s,             -- settlement_date midnight UTC
    NULL,                     -- no SEC accepted-at concept
    %(primary_document_url)s, -- FINRA CDN URL for the settlement file
    FALSE, NULL,              -- no amendment chain
    'parsed',                 -- write-through; raw + observations already durable
    'finra-si-bimonthly-v1',
    'stored',                 -- raw_payload landed in Phase 1 store_raw
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

State-machine contract: re-fetch of the same settlement date (revision-window path) re-runs this UPSERT — the row stays ``ingest_status='parsed'`` but ``last_attempted_at`` advances. ``sec_rebuild --source=finra_short_interest`` flips selected rows back to ``ingest_status='pending'``; the manifest worker dispatches the synth no-op parser (§9), which re-marks them ``parsed`` (a true re-ingest still requires re-firing ``finra_short_interest_refresh`` with the relevant settlement date in scope).

### 7.4 Malformed-row discipline

| Defect class | Granularity | Counter | Outcome |
|---|---|---|---|
| Header row absent / column-count mismatch | File-level | n/a | ``HeaderCorruptionError`` raised → file rejected, txn rolled back |
| Per-row missing field (truncated row) | Row-level | ``skipped_invalid_row`` | row skipped, file continues |
| Per-row malformed numeric (non-integer in ``currentShortPositionQuantity``) | Row-level | ``skipped_invalid_row`` | row skipped, file continues |
| Per-row blank ``symbolCode`` | Row-level | ``skipped_invalid_row`` | row skipped, file continues |
| Symbol resolves to no instrument | Row-level | ``skipped_no_instrument_match`` | row skipped, file continues |
| Symbol matches multiple instruments after normalisation | Row-level | ``skipped_ambiguous_symbol`` | row skipped, file continues |

Rationale: header corruption indicates FINRA changed the column shape OR the file was truncated in transit; rejecting the whole file forces operator triage. Per-row defects are an expected long-tail (FINRA publishes ~10k rows; one bad row should not fail the whole bimonthly snapshot for the other 9,999).

### 7.2 Symbol-normaliser contract

```python
_NORMALISE_RE = re.compile(r'[^A-Z0-9]+')

def _normalise_symbol(symbol: str) -> str:
    """Strip non-alphanumerics + upper-case.

    'BRK.A' -> 'BRKA'.
    'ABRPRD' -> 'ABRPRD' (FINRA shape, idempotent).
    'goog' -> 'GOOG'.
    """
    return _NORMALISE_RE.sub('', symbol.upper())
```

### 7.3 Manifest write contract

The manifest UPSERT runs **inside** the same JOB-owned ``with conn.transaction():`` block that wraps ``ingest_settlement_file``, AFTER the observations + _current writes. ``ingest_status='parsed'`` is written directly (NOT ``'pending'`` → worker drain). This is the synth no-op manifest parser pattern from ``sec_xbrl_facts`` (G7). The manifest row exists for audit / freshness-index / sec_rebuild path coverage, but the actual data write happens inline in the ScheduledJob.

Atomicity contract: the JOB's outer ``with conn.transaction():`` commits the observations + _current + manifest writes as ONE unit. If any of them fails, the txn rolls back and the manifest row is NOT written — preserves the invariant that ``manifest.ingest_status='parsed'`` implies observations are durable.

## 8. ScheduledJob — ``app/jobs/finra_short_interest_refresh.py``

### 8.1 Constants + dispatch

```python
JOB_FINRA_SHORT_INTEREST_REFRESH = "finra_short_interest_refresh"

def _settlement_dates_to_fetch(
    now: datetime,
    backfill_window_days: int = 400,
) -> list[date]:
    """Enumerate every (year, month, 15) + (year, month, last_business_day)
    falling within ``now - backfill_window_days`` ≤ date ≤ ``now``.

    Returns the candidate set sorted ASC. Caller filters out already-
    ingested settlement dates via the manifest read.
    """


@dataclass(frozen=True)
class FinraRefreshStats:
    settlement_files: list[SettlementIngestStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int:
        return sum(s.rows_upserted for s in self.settlement_files)

    @property
    def failed_files(self) -> int:
        return sum(1 for s in self.settlement_files if s.failed)


def run_finra_short_interest_refresh(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
    backfill_window_days: int | None = None,
    provider: FinraShortInterestProvider | None = None,
) -> FinraRefreshStats:
    """Per-fire orchestration.

    1. Determine candidate settlement dates (15th + EOM within backfill window).
    2. Read ``sec_filing_manifest WHERE source='finra_short_interest' AND
       ingest_status='parsed'`` to identify already-ingested settlement dates.
    3. Compute targets = (candidates - already_parsed) ∪ revision_window,
       where ``revision_window = candidates[-2:]`` — the two most-recent
       settlement dates are always re-probed regardless of manifest status,
       to catch FINRA in-place revisions (revisionFlag='Y') that arrive
       within 1-2 cycles of the original snapshot.
    4. For each settlement_date in targets:
         - provider.fetch_settlement_file(date) → bytes
         - service.ingest_settlement_file(...) → SettlementIngestStats
    5. Return aggregated stats; raise RuntimeError on failed_files>0
       so _tracked_job records job_runs.status='failure'.
    """
```

### 8.2 ScheduledJob registration in ``app/workers/scheduler.py``

```python
JOB_FINRA_SHORT_INTEREST_REFRESH = "finra_short_interest_refresh"

# In SCHEDULED_JOBS, after the sec_master_idx_quarterly_sweep entry:
ScheduledJob(
    name=JOB_FINRA_SHORT_INTEREST_REFRESH,
    display_name="FINRA bimonthly short interest refresh (#915)",
    source="finra",
    description=(
        "G6 — bimonthly Equity Short Interest ingest. Daily 12:00 UTC "
        "probes the FINRA CDN for new settlement-date files at "
        "https://cdn.finra.org/equity/otcmarket/biweekly/ (15th + last "
        "business day of each month). Skips manifest-parsed settlement "
        "dates EXCEPT the two most-recent (revision window — FINRA "
        "publishes in-place revisionFlag='Y' corrections within 1-2 "
        "cycles). Per-file: store_raw "
        "(raw payload before parse, #1168) → parse pipe-delim → "
        "preloaded symbol resolver (~13k instruments) → bulk-UPSERT "
        "finra_short_interest_observations + finra_short_interest_current → "
        "manifest UPSERT 'parsed' (synth no-op parser dispatch shape, "
        "G7 sec_xbrl_facts precedent). Per-file failure isolated; "
        "partial success durable. Backfill window 400 days "
        "(post-bootstrap operator may extend via params)."
    ),
    cadence=Cadence.daily(hour=12, minute=0),
    catch_up_on_boot=False,
    prerequisite=_bootstrap_complete,
),
```

### 8.3 Lane addition in ``app/jobs/sources.py``

```python
Lane = Literal[
    "init",
    "etoro",
    "sec_rate",
    "sec_bulk_download",
    "db",
    "db_filings",
    "db_fundamentals_raw",
    "db_ownership_inst",
    "db_ownership_insider",
    "db_ownership_funds",
    "bootstrap",
    "finra",   # NEW — FINRA CDN, 1 req/s polite floor. Disjoint from
               # sec_rate (different host). Single-job lane in v1
               # (finra_short_interest_refresh).
]
```

Docstring expanded:

```
* ``finra`` — FINRA CDN (cdn.finra.org). 1 req/s polite floor (no
  published rate limit). Disjoint from ``sec_rate`` by construction
  (different host, no shared per-IP budget). Single ScheduledJob in
  v1 (``finra_short_interest_refresh``); FINRA RegSHO daily (#916)
  adds a second job in the same lane.
```

### 8.4 ``_INVOKERS`` registration in ``app/jobs/runtime.py``

```python
_INVOKERS[_scheduler.JOB_FINRA_SHORT_INTEREST_REFRESH] = _adapt_zero_arg(
    _scheduler.finra_short_interest_refresh
)
```

The ``scheduler.finra_short_interest_refresh`` body wraps ``_tracked_job`` + opens its own DB connection + calls ``run_finra_short_interest_refresh`` (mirrors the G12 ``sec_master_idx_quarterly_sweep`` shape at ``app/workers/scheduler.py:4599-4646``).

### 8.5 Manual-trigger entry

Add to ``MANUAL_TRIGGER_JOB_SOURCES``:

```python
"finra_short_interest_refresh": "finra",
```

No companion ``MANUAL_TRIGGER_JOB_METADATA`` entry — v1 ships with no operator-supplied params. The fixed 400-day window covers steady-state refresh + the ETL DoD clause #8 smoke. Extended-window backfill (e.g. 730 days for ETL DoD clause #10) lands via the REPL runbook (§13).

Rationale (Codex 1b r1 HIGH 1): the current ``_INVOKERS`` adapter at ``app/jobs/runtime.py:_adapt_zero_arg`` discards params. Adding a params-aware invoker is documented in ``app/jobs/sources.py:130`` as a PR1b widening but has not landed. Scoping FINRA's manual-trigger surface to zero-param keeps the v1 PR within ``finra``-lane scope and avoids dragging in the PR1b widening as a sibling change.

## 9. Manifest parser — ``app/services/manifest_parsers/finra_short_interest.py``

Synth no-op per ``sec_xbrl_facts`` precedent. The ScheduledJob writes the manifest row directly as ``parsed``; this parser exists only to satisfy the manifest-worker dispatch invariant for the rare ``sec_rebuild`` scoped tick path.

```python
"""finra_short_interest manifest-worker parser — synth no-op (G6/#915).

FINRA Equity Short Interest data lands via the
``finra_short_interest_refresh`` ScheduledJob (daily 12:00 UTC). The
ScheduledJob owns the fetch + parse + UPSERT into
``finra_short_interest_observations`` + ``finra_short_interest_current``, then
UPSERTs the manifest row as ``ingest_status='parsed'`` directly.

This parser exists only to satisfy the manifest-worker dispatch
invariant on the rare ``sec_rebuild --source=finra_short_interest``
path — if the operator flips a manifest row back to ``pending``, the
worker dispatch needs a registered parser to mark it ``parsed`` again
WITHOUT triggering a network fetch or DB write. The actual re-ingest
mechanism is re-firing the ScheduledJob (which will re-fetch the file
+ re-write the observations); the manifest-side rebuild is the
audit-tracking dimension only.

Architectural sibling: ``sec_xbrl_facts.py`` (G7) — Companyfacts data
lands via the bulk JSON ScheduledJob path; XBRL manifest rows exist
for accession-level tracking.

ParseOutcome contract — mirrors ``sec_xbrl_facts``:
- ``status='parsed'`` always.
- ``parser_version='finra-si-bimonthly-v1'``.
- No network call.
- No DB write.
"""
```

Registration in ``app/services/manifest_parsers/__init__.py::register_all_parsers``:

```python
from app.services.manifest_parsers import finra_short_interest as _finra_short_interest
# ...
_finra_short_interest.register()  # synth no-op (G6/#915)
```

## 10. Tests

| File | Purpose |
|---|---|
| ``tests/test_finra_short_interest_provider.py`` | Provider unit — URL builder (settlement date → URL); 404 → ``FinraNotFound``; 5xx → re-raise (Request attached); rate-limit clock identity (shared across multiple instances); back-to-back throttle smoke via ``httpx.MockTransport``. |
| ``tests/test_finra_short_interest_ingest.py`` | Service integration against ``ebull_test_conn`` — happy path (real shape fixture, 5 known instruments resolved); ``_normalise_symbol`` unit (BRK.A → BRKA, ABRPRD idempotent, lowercase upper'd); ambiguous-symbol collision skipped + counter incremented; no-match skipped + counter incremented; malformed row (missing field) skipped + counter incremented; per-file txn rollback on UPSERT failure (raw payload still durable from earlier explicit commit); _current row settlement-date-wins-most-recent; manifest row upserted ``parsed`` with ``parser_version='finra-si-bimonthly-v1'``. |
| ``tests/test_finra_short_interest_refresh.py`` | ScheduledJob integration — ``_settlement_dates_to_fetch`` parametrised across month-end / leap-year / month-15 cases; manifest-filter excludes already-parsed; per-file failure ``conn.rollback()`` isolated + successful files committed before failed one; partial-failure raises RuntimeError so ``job_runs.status='failure'``; explicit ``provider=`` injection for test isolation. |
| ``tests/test_finra_short_interest_scheduler_wiring.py`` | Wiring invariants — ``JOB_FINRA_SHORT_INTEREST_REFRESH`` constant value; ``ScheduledJob`` entry shape (source='finra', cadence daily 12:00 UTC, prerequisite=_bootstrap_complete); ``_INVOKERS.__wrapped__`` identity; ``source_for()`` resolves without ``KeyError``; ``MANUAL_TRIGGER_JOB_SOURCES`` entry. |
| ``tests/test_finra_short_interest_manifest_parser.py`` | Parser invariants — synth no-op contract (no network, no DB writes); ``ParseOutcome(status='parsed', parser_version='finra-si-bimonthly-v1')``; non-caller invariant (``fetch_document_text`` / ``store_raw`` / ``conn.execute`` / ``conn.cursor`` / ``conn.transaction`` never called); registry-wiring after ``clear_registered_parsers`` + ``register_all_parsers``. |
| ``tests/test_layer_123_wiring.py`` | Extended with Layer-4 row for ``finra_short_interest_refresh``. |
| ``tests/test_universal_gate_carve_out.py`` | Positive assertion that ``finra_short_interest_refresh`` is NOT in the exempt allow-list (it has a real prerequisite). |
| ``tests/test_fetch_document_text_callers.py`` | Allow-list extended for the new provider + parser modules (per #453 contract). |
| ``tests/fixtures/finra/shrt20260430_sample.csv`` | Pristine — header row + 9 verbatim rows for AAPL, GME, MSFT, JPM, HD, ABRPRD, ABRPRE, ALLPRB, ANCTF from a live ``shrt20260430.csv`` fetch. Regenerable via the plan §11.1 fetch+grep script. **No hand-edits.** |
| ``tests/fixtures/finra/shrt20260430_defects.csv`` | Synthetic — header row + one ambiguous-collapse symbol (test-seeded normalised collision against a sibling instrument) + one truncated row (missing fields after pipe 5). Hand-written; committed once; never regenerated from FINRA. |

## 11. Operator-visible figures + ETL DoD #8-#12

| Clause | Evidence |
|---|---|
| #8 Smoke against AAPL / GME / MSFT / JPM / HD | All five symbols resolve directly against ``instruments`` (no dotted form). After a single ``finra_short_interest_refresh`` fire on dev DB against the 2026-04-30 settlement file (most-recent exchange-listed cohort), ``SELECT * FROM finra_short_interest_current WHERE instrument_id IN (...)`` returns one row per panel symbol. PR body records the figures observed. |
| #9 Cross-source verify one ticker | Compare GME 2026-04-30 ``current_short_interest`` against marketbeat.com / shortsqueeze.com / nasdaq.com short-interest page for GME 2026-04-30 settlement. ±5% tolerance acknowledged (off-source reporting cadence drift). PR body records source + figure compared. |
| #10 Backfill | REPL runbook on dev DB: ``python -c "from psycopg import connect; from app.settings import settings; from app.jobs.finra_short_interest_refresh import run_finra_short_interest_refresh; \nwith connect(settings.database_url) as c: print(run_finra_short_interest_refresh(c, backfill_window_days=730))"``. ~48 settlement files at 1 req/s = ~48s wall-clock. Backfill reaches 2024-Q2; falls within the 2021-Q3 → 2027-Q1 partition window. PR body records invocation + outcome (rows upserted across the 2-year window). |
| #11 Operator-visible figure verified | No live ``/instruments/{symbol}/short-interest`` endpoint exists in v1 (memo overlay deferred per spec §1 closure framing). Operator verifies via direct SQL: ``SELECT * FROM finra_short_interest_current WHERE instrument_id=<...>`` against dev DB. PR body records the SELECT output for AAPL / GME. |
| #12 PR records verification + SHA | PR body explicit table. |

## 12. Out of scope

| Item | Reason |
|---|---|
| RegSHO daily short volume (#916) | Sequential PR in Phase 6 (after this one). |
| Frontend memo overlay (issue #915 acceptance #2) | Plan §1 autonomy contract UI carve-out. OBSERVATIONS PRIMITIVE closure framing. No new UI ticket opened. |
| ESOP / DRS / restricted overlay | Phases 4 + 5 of the ownership full-decomposition spec; separate work. |
| Short interest borrow rate / utilisation | Out of scope per issue #845 — vendor-paid, not free regulated. |
| Per-broker short interest disaggregation | Out of scope per issue #845. |
| 2014-→ historical backfill | Spec scope is N=400 days (~24 most-recent files) per default ``backfill_window_days``. Extended-window backfill (730+ days) lands via REPL invocation against ``run_finra_short_interest_refresh(conn, backfill_window_days=N)`` per ETL DoD clause #10 evidence. Deeper backfill (2014→) is operator runbook territory. |

## 13. Acceptance

1. **Schema** — ``finra_short_interest_observations`` partitioned by settlement_date quarterly buckets (2021-Q3 → 2027-Q1) + ``finra_short_interest_current`` snapshot land via ``sql/152_finra_short_interest.sql`` (and ``sql/151_filing_raw_documents_add_finra_si.sql`` widens the raw-payload sink).
2. **Provider** — ``FinraShortInterestProvider`` exposes URL builder + ``fetch_settlement_file(date)`` with shared throttle.
3. **Service** — ``ingest_settlement_file(conn, settlement_date, raw_bytes, resolver, ingest_run_id)`` parses + UPSERTs + writes manifest. Emits SQL only against the caller's open transaction; NEVER opens its own ``with conn.transaction():`` and NEVER calls ``conn.commit()`` / ``conn.rollback()`` — JOB body wraps the call in ``with conn.transaction():``. Header corruption raises ``HeaderCorruptionError``; per-row defects (incl. None-fill from truncated DictReader rows) skipped + counter-tracked.
4. **ScheduledJob** — ``finra_short_interest_refresh`` (daily 12:00 UTC, lane=finra, prerequisite=_bootstrap_complete) opens its own connection, enumerates settlement dates (with revision-window re-fetch), filters against manifest, per file: ``store_raw + conn.commit() + with conn.transaction(): ingest_settlement_file(...)``. Clean-exit of the ``with`` block commits; exception in the block triggers automatic rollback while leaving the Phase 1 raw payload durable. Logs aggregated stats at INFO; logs match-rate warning at WARNING if <50%; raises ``RuntimeError`` if ``failed_files > 0``.
5. **Manifest parser** — synth no-op registered alongside sec_xbrl_facts / sec_10q.
6. **Smoke** — AAPL / GME / MSFT / JPM / HD resolve + write observations + _current rows on a real-fixture fire against 2026-04-30 settlement.
7. **Cross-source** — GME 2026-04-30 current_short_interest within 5% of independent source (marketbeat / shortsqueeze / nasdaq).
8. **Backfill** — REPL invocation ``run_finra_short_interest_refresh(conn, backfill_window_days=730)`` re-drives 2-year window without error; rows land in expected quarterly partitions. (v1 manual-trigger surface is zero-param — params-aware invoker deferred.)
9. **Matrix delta** — ``.claude/skills/data-engineer/etl-endpoint-coverage.md`` §2 ``finra_short_interest`` row + §7 G6 row updated to ``WIRED 2026-05-18 (#915 — bimonthly portion)``; G6 row noted as "bimonthly portion closed; RegSHO daily portion #916 open".
10. **Skill** — ``.claude/skills/data-sources/finra.md`` new source-of-truth note created (endpoints, formats, rate-limit posture, symbol-norm discipline).
11. **Memory** — ``[[us-source-coverage]]`` updated to reflect G6 bimonthly closed.
12. **Local gates** — ``uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest`` all green.
13. **Codex** — 1a (spec) CLEAN; 1b (plan) CLEAN; 2 (pre-push) CLEAN.
14. **Bot review** — APPROVE on the most recent commit + CI green.
