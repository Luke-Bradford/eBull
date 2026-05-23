# N-CSR fund-metadata parser — implementation plan

> Status: **DRAFT 2026-05-14** — pending Codex pre-spec 1b + operator signoff.
>
> Spec: `docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md` (signed off; Codex 1a CLEAN).
> Issue: **#1171**. Branch: `feature/1171-n-csr-fund-metadata-parser`.
> Output preference (CLAUDE.md): schema → service logic → tests → integration glue.

## 1. Task decomposition

| # | Task | Scope (files) | Depends on | Deliverable |
|---|---|---|---|---|
| T1 | Schema migration | `sql/NNN_fund_metadata.sql` (next number); fixture `tests/fixtures/ebull_test_db.py:_PLANNER_TABLES` registers **all three** new FK-child tables (`fund_metadata_observations`, `fund_metadata_current`, `cik_refresh_mf_directory`) | — | DDL lands; `pytest tests/test_migrations.py` green |
| T2 | classId resolver helper | `app/services/manifest_parsers/_fund_class_resolver.py` | T1 | Pure-function resolver + miss-classifier + unit tests |
| T3 | iXBRL fact extractor | `app/services/n_csr_extractor.py` | T1 | Pure-function extractor + golden-file unit tests across 4 spike fixtures |
| T4 | company_tickers_mf.json ingest (Stage 6 extension) | `app/services/cik_refresh.py`; `app/services/bootstrap_orchestrator.py` capability output `class_id_mapping_ready` | T1 | Stage 6 extension fetches MF directory + populates `external_identifiers` + populates `cik_refresh_mf_directory` |
| T6 | Write-through refresh writer | `app/services/fund_metadata.py` (new) — `refresh_fund_metadata_current(conn, instrument_id)` | T1 | Atomic refresh inside advisory lock + source-priority chain |
| T7 | Form-to-source map + freshness cadence | `app/services/sec_manifest.py:_FORM_TO_SOURCE` (add `N-CSRS`, `N-CSRS/A`); `app/services/data_freshness.py:_CADENCE` (confirm 200d) | T1 | N-CSRS manifest rows now write; cadence unchanged |
| T5 | Manifest-worker parser adapter | `app/services/manifest_parsers/sec_n_csr.py` (REPLACES synth no-op); `app/services/manifest_parsers/__init__.py` registration | T1 + T2 + T3 + **T6** (parser calls `refresh_fund_metadata_current`) | Real parser with `requires_raw_payload=False`; replaces #918 no-op |
| T8 | Bootstrap fund-scoped drain | `app/jobs/sec_first_install_drain.py` (new `bootstrap_n_csr_drain` function); `app/services/bootstrap_orchestrator.py` (new stage entry; depends on `class_id_mapping_ready`) | T4 + T5 + **T7** (drain enqueues both N-CSR and N-CSRS — needs N-CSRS in _FORM_TO_SOURCE before manifest writes) | Stage runs last-2-years per fund-trust CIK; both form-variants drained |
| T9 | Read endpoints | `app/api/fund_metadata.py` (new); `app/main.py` (route mount) | T6 | 3 endpoints: `/instruments/{symbol}/fund-metadata`, `.../history`, `/coverage/fund-metadata` |
| T10 | Parser durability + extraction tests | `tests/test_manifest_parser_sec_n_csr.py` (overwrite existing no-op test); `tests/test_n_csr_extraction.py` (new); `tests/test_fund_class_resolver.py` (new); `tests/test_refresh_fund_metadata_current.py` (new); `tests/test_fund_metadata_endpoints.py` (new); update allow-list at `tests/test_fetch_document_text_callers.py` | T5 + T6 + T9 | All tests pass under `uv run pytest` — **see §2.T10 for the explicit case enumeration matching spec §13** |
| T11 | Documentation updates | per §2.T11 file list below | T5 (parser landed) | All docs reflect new state |
| T12 | param_metadata help text + wiki | `app/services/processes/param_metadata.py:259-265` (remove sec_n_csr "no parser registered yet" note); `docs/wiki/ownership-card.md` lines 27+43 (restate); `docs/wiki/glossary.md` lines 36-37 (restate) | T5 | Stale help text + wiki claims aligned |

**Total tasks: 12.** Dispatch order:

```
T1 (schema)
  ├── T2 (resolver)          ┐
  ├── T3 (extractor)         │
  ├── T4 (mf ingest)         ├── T5 (parser; needs T6)
  ├── T6 (refresh writer)    ┘
  └── T7 (form-to-source)        ── T8 (bootstrap drain; needs T4+T5+T7)
                              ── T9 (endpoints; needs T6)
T1+...+T9 → T10 (tests, incremental) → T11+T12 (docs) → self-review → Codex 2 → push
```

The corrected critical-path dependency is **T5 depends on T6** (parser calls `refresh_fund_metadata_current`) and **T8 depends on T7** (drain enqueues N-CSRS which must map to `sec_n_csr` first).

## 2. Per-task contracts

### T1 — Schema migration

**File:** `sql/NNN_fund_metadata.sql` (NNN = next sequence number; current max = `144_filings_fanout_per_instrument.sql`, so likely 145 or 146 depending on what lands in parallel).

**Contents** (per spec §6 DDL + T4 companion table):

1. `CREATE TABLE fund_metadata_observations` partitioned by `RANGE(period_end)`.
2. Quarterly partitions 2010Q1 – 2030Q4 + `_default` partition (mirror `sql/113_ownership_insiders_observations.sql` generator pattern).
3. Partial unique index `uq_fund_metadata_observations_current` `(instrument_id, source_accession, period_end) WHERE known_to IS NULL`.
4. Indexes: `class_id`, `(instrument_id, period_end DESC)`, `filed_at DESC`.
5. `CREATE TABLE fund_metadata_current` PK on `instrument_id`.
6. Indexes: `expense_ratio_pct`, `net_assets_amt DESC`.
7. `CREATE TABLE cik_refresh_mf_directory (class_id TEXT PRIMARY KEY, series_id TEXT, symbol TEXT, trust_cik TEXT, last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW())` — populated by T4 from `company_tickers_mf.json`; consumed by `_fund_class_resolver.classify_resolver_miss` (T2).
8. Index on `cik_refresh_mf_directory (symbol)` for the JOIN against `instruments.symbol` used by the EXT_ID_NOT_YET_WRITTEN classifier branch.
9. Idempotency: every DDL gated by `pg_constraint.contype + conkey` / `pg_index.indisunique + indkey` introspection (data-engineer §0 step 4).

**Test fixture:** `tests/fixtures/ebull_test_db.py:_PLANNER_TABLES` must list **all three** new tables (`fund_metadata_observations`, `fund_metadata_current`, `cik_refresh_mf_directory`) per data-engineer corollary I3.

**Definition of done:**
- `uv run pytest tests/test_migrations.py` green.
- `uv run pyright` clean (only relevant for the SQL → no python in this task).
- Re-running the migration is a no-op (`pg_constraint` introspection).

### T2 — classId resolver helper

**File:** `app/services/manifest_parsers/_fund_class_resolver.py` (new).

**Public surface:**

```python
from enum import Enum


class ResolverMissReason(str, Enum):
    """Per spec §7.4 — distinguishes transient (retry) from deterministic (tombstone) misses."""
    PENDING_CIK_REFRESH = "pending_cik_refresh"        # classId not in cik_refresh_mf_directory yet
    EXT_ID_NOT_YET_WRITTEN = "ext_id_not_yet_written"  # classId IS in directory + symbol matches an instrument, but external_identifiers row hasn't been written yet (cik_refresh race)
    INSTRUMENT_NOT_IN_UNIVERSE = "instrument_not_in_universe"  # classId IS in directory + no matching instrument (mutual fund not in eToro universe)


def resolve_class_id_to_instrument(
    conn: psycopg.Connection[Any],
    class_id: str,
) -> int | None:
    """Resolve a 10-char classId (C000NNNNNN) to instrument_id via
    external_identifiers (provider='sec', identifier_type='class_id').

    Returns None for unknown classIds (caller calls classify_resolver_miss
    to decide retry vs tombstone).
    """


def classify_resolver_miss(
    conn: psycopg.Connection[Any],
    class_id: str,
) -> ResolverMissReason:
    """Discriminate the three miss-reasons per spec §7.4.

    Decision tree:
      1. SELECT 1 FROM cik_refresh_mf_directory WHERE class_id = %s
         → no row → PENDING_CIK_REFRESH (transient, retry)
      2. JOIN instruments ON symbol → no instrument row → INSTRUMENT_NOT_IN_UNIVERSE (deterministic)
      3. instrument row exists but no external_identifiers row → EXT_ID_NOT_YET_WRITTEN (transient race)
    """
```

**Test file:** `tests/test_fund_class_resolver.py`:
- `test_resolve_hit_returns_instrument_id` — bootstrap row in external_identifiers; assert returns int.
- `test_resolve_miss_returns_none` — no row; assert None.
- `test_classify_pending_cik_refresh` — directory empty for class_id → PENDING_CIK_REFRESH.
- `test_classify_ext_id_not_yet_written` — directory has class_id + symbol matches an instrument + no external_identifiers row → EXT_ID_NOT_YET_WRITTEN.
- `test_classify_instrument_not_in_universe` — directory has class_id + symbol does not map to any instrument → INSTRUMENT_NOT_IN_UNIVERSE.
- `test_no_symbol_only_fallback` — directory has class_id with matching symbol, but no external_identifiers row → `resolve_class_id_to_instrument` returns None (no shortcut via symbol; caller must classify + retry-or-tombstone).

### T3 — iXBRL fact extractor

**File:** `app/services/n_csr_extractor.py` (new).

**Public surface:**

```python
@dataclass
class FundMetadataFacts:
    """All facts extracted for one (series_id, class_id) observation."""
    series_id: str | None
    class_id: str
    trust_cik: str
    period_end: date
    document_type: str  # 'N-CSR' | 'N-CSR/A' | 'N-CSRS' | 'N-CSRS/A'
    amendment_flag: bool
    # ... Tier 1 fields (one per spec §5 typed column)
    expense_ratio_pct: Decimal | None
    net_assets_amt: Decimal | None
    # ...
    # Tier 2 fields
    returns_pct: dict[str, Decimal]
    benchmark_returns_pct: dict[str, dict[str, Decimal]]
    sector_allocation: dict[str, Decimal]
    region_allocation: dict[str, Decimal]
    credit_quality_allocation: dict[str, Decimal]
    growth_curve: list[dict[str, Any]]
    # Tier 3 fallback
    raw_facts: dict[str, list[dict[str, Any]]]


def extract_fund_metadata_facts(ixbrl_xml: bytes) -> list[FundMetadataFacts]:
    """Parse iXBRL companion + return one FundMetadataFacts per
    (series_id, class_id) tuple, with hard context filtering per
    spec §8 step 6.c.

    Raises ValueError on malformed iXBRL (caller maps to failed_outcome).
    """
```

**Implementation steps** (per spec §8):
1. Parse with `lxml.etree.fromstring`.
2. Build context-dimension index `{context_ref: {axis_qname: member_qname, ...}}`.
3. Enumerate (series, class) tuples from contexts.
4. For each tuple, iterate facts, filter by context-tuple match, route to Tier 1 / Tier 2 / Tier 3.
5. Apply boilerplate blocklist (spec §5 table).
6. Apply axis allowlist (spec §5.A + §5.B).
7. Return list.

**Test file:** `tests/test_n_csr_extraction.py`:
- Per-concept extraction test per Tier 1 column (parameterized across the 4 spike fixtures cached at `tests/fixtures/n_csr_golden/{vanguard_a,vanguard_ncsrs,fidelity,ishares}/_htm.xml`).
- `test_axis_allowlist_route` — known period axis → Tier 2; unknown → raw_facts.
- `test_pct_of_nav_axis_routing` — IndustrySectorAxis → sector_allocation; GeographicRegionAxis → region_allocation; CreditQualityAxis → credit_quality_allocation. Same concept, different columns.
- `test_multi_series_isolation` — Vanguard accession -021519 carries 2 series × 3 classes each → 6 FundMetadataFacts. Series-A HoldingsCount ≠ Series-B HoldingsCount.
- `test_growth_curve_time_order` — points sorted by `instant` ASC.
- `test_blocklist_skipped` — HoldingsTableTextBlock, AvgAnnlRtrTableTextBlock, etc. NOT in raw_facts.
- `test_factors_affecting_perf_in_raw_facts` — included with 8 KB cap.
- `test_raw_facts_size_cap` — overflow truncated with `__truncated__` sentinel.

### T4 — company_tickers_mf.json ingest (Stage 6 extension)

**File:** `app/services/cik_refresh.py` (extend existing).

**Add function:**

```python
def refresh_mf_directory(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Fetch company_tickers_mf.json + populate:

      1. cik_refresh_mf_directory (small companion table) — all 28k classIds
         seen (used by class_id_known_to_mf_directory).
      2. external_identifiers (provider='sec', identifier_type='class_id')
         for rows whose symbol matches an instruments.symbol.

    Conditional GET via ETag (mirror existing daily_cik_refresh pattern at
    sec_edgar.py:52). No-op on 304 Not Modified.

    Returns counts: {fetched, directory_rows, external_identifier_rows}.
    """
```

**Schema add to T1:** `cik_refresh_mf_directory(class_id TEXT PRIMARY KEY, series_id TEXT, symbol TEXT, trust_cik TEXT, last_seen TIMESTAMPTZ DEFAULT NOW())`. — Update T1 to include this table.

**Capability output:** `bootstrap_orchestrator.py` stage 6 (`cik_refresh`) declares new capability `class_id_mapping_ready` once `refresh_mf_directory` has run successfully at least once. T8 depends on this capability.

**Test file:** extend `tests/test_cik_refresh.py`:
- `test_refresh_mf_directory_first_run` — empty DB + golden mf.json fixture → 28k directory rows + N external_identifiers rows.
- `test_refresh_mf_directory_etag_304` — second run with same ETag → no DB writes.
- `test_refresh_mf_directory_skips_non_universe_symbols` — symbols not in `instruments` populate directory but NOT external_identifiers.

### T5 — Manifest-worker parser adapter

**File:** `app/services/manifest_parsers/sec_n_csr.py` — **REPLACES** existing synth no-op.

**Public surface:**

```python
_PARSER_VERSION_N_CSR = "n-csr-fund-metadata-v1"


def _parse_sec_n_csr(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow
) -> Any:  # ParseOutcome
    """Real parser per spec §8.

    Steps:
      1. Validate URL + cik.
      2. Fetch iXBRL companion via SecFilingsProvider.
      3. Parse via extract_fund_metadata_facts.
      4. Per (series, class): resolve class_id → instrument_id; if hit,
         INSERT observation + refresh_fund_metadata_current.
      5. Aggregate ParseOutcome per resolver-miss rules.

    No store_raw (requires_raw_payload=False).
    """


def register() -> None:
    """Register with manifest worker. Idempotent."""
    register_parser("sec_n_csr", _parse_sec_n_csr, requires_raw_payload=False)
```

**Old synth no-op file is OVERWRITTEN** — the docstring is updated to reflect the real parser. `tests/test_manifest_parser_sec_n_csr.py` is overwritten in T10 with the real-parser test suite.

### T6 — Write-through refresh writer

**File:** `app/services/fund_metadata.py` (new).

**Public surface:**

```python
def refresh_fund_metadata_current(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> Literal['inserted', 'updated', 'suppressed']:
    """Atomic write-through refresh per spec §8.

    Steps inside one transaction:
      1. pg_advisory_xact_lock(instrument_id) — I7 invariant.
      2. SELECT winning observation per priority chain.
      3. INSERT ... ON CONFLICT (instrument_id) DO UPDATE.
    """
```

**Test file:** `tests/test_refresh_fund_metadata_current.py`:
- `test_inserted_first_observation`.
- `test_updated_newer_period_end`.
- `test_filed_at_tie_break_amendment_wins`.
- `test_source_accession_tie_break_degenerate`.
- `test_suppressed_incumbent_already_winner`.
- `test_known_to_filter_excludes_superseded` — superseded row has known_to NOT NULL; winner is the un-superseded row.
- `test_advisory_lock_serialises_concurrent_refresh` — two transactions calling refresh on the same instrument; one waits.

### T7 — Form-to-source map + freshness cadence

**File:** `app/services/sec_manifest.py:_FORM_TO_SOURCE`.

**Change:** add `'N-CSRS': 'sec_n_csr', 'N-CSRS/A': 'sec_n_csr'` (spec §9). Existing `'N-CSR': 'sec_n_csr', 'N-CSR/A': 'sec_n_csr'` stays.

**File:** `app/services/data_freshness.py:_CADENCE`. Confirm `sec_n_csr: 200` days. Leave as-is.

**Test:** extend `tests/test_sec_manifest_form_to_source.py` (or create) — assert all 4 N-CSR variants map to `sec_n_csr`.

### T8 — Bootstrap fund-scoped drain

**File:** `app/jobs/sec_first_install_drain.py`.

**Add function:**

```python
def bootstrap_n_csr_drain(
    conn: psycopg.Connection[Any],
    *,
    horizon_days: int = 730,  # 2 years
) -> dict[str, int]:
    """Walk fund-trust CIK list from cik_refresh_mf_directory + enqueue
    last-2-years N-CSR + N-CSRS accessions per trust to sec_filing_manifest.

    Pre-condition: class_id_mapping_ready capability (T4).
    """
```

**File:** `app/services/bootstrap_orchestrator.py` — add new stage entry (next available stage number), depends on `class_id_mapping_ready`. Dispatched via `_INVOKERS`.

**Test:** `tests/test_sec_first_install_drain.py` — extend with `test_bootstrap_n_csr_drain_writes_manifest_rows`.

### T9 — Read endpoints

**File:** `app/api/fund_metadata.py` (new).

**Routes:**

```python
@router.get("/instruments/{symbol}/fund-metadata", response_model=FundMetadataResponse)
async def get_fund_metadata(symbol: str, ...) -> FundMetadataResponse: ...

@router.get("/instruments/{symbol}/fund-metadata/history", response_model=list[FundMetadataObservation])
async def get_fund_metadata_history(symbol: str, since: date | None = Query(None), ...) -> list[FundMetadataObservation]: ...

@router.get("/coverage/fund-metadata", response_model=FundMetadataCoverageResponse)
async def get_fund_metadata_coverage(...) -> FundMetadataCoverageResponse: ...
```

**File:** `app/main.py` — mount the router.

**Pydantic models:** defined inline in `app/api/fund_metadata.py` per the spec §11.1 shape.

**Test file:** `tests/test_fund_metadata_endpoints.py`:
- All 3 endpoints with golden response fixtures (seed `fund_metadata_current` directly).
- 404 on unknown symbol.
- 503 on infra failure (DB down) — fixed-phrase detail per prevention-log #86.
- `since` naive datetime coerced to UTC (prevention-log #80).
- `coverage` endpoint counts per-source + resolver-miss.

### T10 — Tests

**Pattern reference** for parser tests: `tests/test_manifest_parser_sec_10k.py` (#1152 pattern). Use the same sentinel-connection + monkeypatch shape.

**Existing test that needs overwriting:** `tests/test_manifest_parser_sec_n_csr.py` — currently asserts the #918 no-op behaviour; this PR rewrites it to assert the real-parser behaviour.

**Allow-list update:** `tests/test_fetch_document_text_callers.py` — when `sec_n_csr.py` adds a call to `fetch_document_text`, the test's allow-list must include the new caller path.

**Per-spec-§13 test enumeration** (every case must land):

| Test case | Test file | Notes |
|---|---|---|
| Per-Tier-1-column extraction × 4 family fixtures | `tests/test_n_csr_extraction.py` | Vanguard A + Vanguard NCSRS + Fidelity + iShares |
| Period-axis allowlist + raw_facts fallback | `tests/test_n_csr_extraction.py` | unknown member → raw_facts; logged warning |
| `oef:PctOfNav` axis routing | `tests/test_n_csr_extraction.py` | sector / region / credit go to distinct columns |
| Multi-series hard context filter | `tests/test_n_csr_extraction.py` | Series-A HoldingsCount does NOT bleed into Series-B observation |
| Growth-curve time-order | `tests/test_n_csr_extraction.py` | `growth_curve` sorted by `instant` ASC |
| Boilerplate blocklist | `tests/test_n_csr_extraction.py` | `HoldingsTableTextBlock` NOT in raw_facts |
| `oef:FactorsAffectingPerfTextBlock` → raw_facts with 8 KB cap | `tests/test_n_csr_extraction.py` | overflow truncated with `__truncated__` sentinel |
| classId resolver — hit | `tests/test_fund_class_resolver.py` | external_identifiers row exists |
| classId resolver — miss returns None | `tests/test_fund_class_resolver.py` | |
| classify_resolver_miss × 3 reasons | `tests/test_fund_class_resolver.py` | PENDING_CIK_REFRESH / EXT_ID_NOT_YET_WRITTEN / INSTRUMENT_NOT_IN_UNIVERSE |
| No symbol-only fallback | `tests/test_fund_class_resolver.py` | |
| **Append-only supersession on parser-version rewash** | `tests/test_manifest_parser_sec_n_csr.py` | First call writes row with known_to=NULL; second call with bumped parser_version marks prior row's known_to=NOW() + INSERTs fresh row with known_to=NULL; assert exactly one row currently-valid; assert partial unique index allows both rows to coexist |
| **Partial-success resolver-miss** | `tests/test_manifest_parser_sec_n_csr.py` | 5 classes: 2 resolve, 1 miss with PENDING_CIK_REFRESH, 1 miss with EXT_ID_NOT_YET_WRITTEN, 1 miss with INSTRUMENT_NOT_IN_UNIVERSE → 2 observations written + 3 per-miss-reason log entries + ParseOutcome.status == "parsed" |
| **Zero-resolution unanimous reason → tombstoned** | `tests/test_manifest_parser_sec_n_csr.py` | All 3 classes miss with the SAME deterministic reason (INSTRUMENT_NOT_IN_UNIVERSE) → ParseOutcome.status == "tombstoned" with that reason |
| **Zero-resolution mixed reasons → failed** | `tests/test_manifest_parser_sec_n_csr.py` | All 3 classes miss with mixed reasons (some transient, some deterministic) → ParseOutcome.status == "failed" + 1h backoff (allows next tick to re-classify post-cik-refresh) |
| Missing URL tombstone | `tests/test_manifest_parser_sec_n_csr.py` | |
| Missing CIK tombstone | `tests/test_manifest_parser_sec_n_csr.py` | |
| Empty fetch tombstone | `tests/test_manifest_parser_sec_n_csr.py` | |
| Fetch exception → failed | `tests/test_manifest_parser_sec_n_csr.py` | |
| Parse exception → failed | `tests/test_manifest_parser_sec_n_csr.py` | |
| Sentinel-conn durability | `tests/test_manifest_parser_sec_n_csr.py` | Monkeypatch store_raw + fetch + DB write helpers; assert no DB writes outside fund_metadata_* + manifest_state tables |
| refresh — inserted first observation | `tests/test_refresh_fund_metadata_current.py` | |
| refresh — updated newer period_end | `tests/test_refresh_fund_metadata_current.py` | |
| refresh — filed_at tie-break (amendment wins) | `tests/test_refresh_fund_metadata_current.py` | |
| refresh — source_accession tie-break (degenerate) | `tests/test_refresh_fund_metadata_current.py` | |
| refresh — suppressed when incumbent already winner | `tests/test_refresh_fund_metadata_current.py` | |
| refresh — known_to filter excludes superseded | `tests/test_refresh_fund_metadata_current.py` | |
| refresh — advisory lock serialises concurrent | `tests/test_refresh_fund_metadata_current.py` | |
| Endpoint shape × 3 endpoints | `tests/test_fund_metadata_endpoints.py` | golden responses |
| 503 on infra failure | `tests/test_fund_metadata_endpoints.py` | DB error → fixed-phrase detail |
| Naive datetime coerce | `tests/test_fund_metadata_endpoints.py` | `since` query without tzinfo → UTC |
| 404 on unknown symbol | `tests/test_fund_metadata_endpoints.py` | |
| Coverage endpoint counts | `tests/test_fund_metadata_endpoints.py` | per-source + resolver-miss |

**New test files** (consolidated list):
- `tests/test_n_csr_extraction.py` (T3 coverage).
- `tests/test_fund_class_resolver.py` (T2 coverage).
- `tests/test_refresh_fund_metadata_current.py` (T6 coverage).
- `tests/test_fund_metadata_endpoints.py` (T9 coverage).

### T11 — Documentation updates

**Files:**
1. `.claude/skills/data-sources/sec-edgar.md` §11.5 — replace synth-no-op note with real-parser note + PR link.
2. `.claude/skills/data-sources/edgartools.md` G12 — restate to "holding-level CUSIP still absent (spike verdict stands); fund-level metadata now extracted by manifest parser".
3. `.claude/skills/data-engineer/SKILL.md` — new section "Fund metadata observations + current" mirroring §1.2 ownership-model conventions.
4. `.claude/skills/data-engineer/etl-endpoint-coverage.md` row 47 — restate `sec_n_csr` from "synth no-op landed" to "real parser landed" with this PR.
5. `.claude/skills/metrics-analyst/SKILL.md` — new rows per operator-visible fund-metadata figure: expense_ratio, net_assets_amt, returns_pct, sector_allocation, portfolio_turnover_pct.
6. `docs/settled-decisions.md` — add "Source priority for fund metadata" entry per spec §2.
7. `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` — append §10.6 "Scope narrowing — fund-metadata extraction tracked separately under #1171"; narrow §10.3 row "Product-visibility pivot test: ANSWERED" to scope it to audit-attestation lane only.
8. `docs/superpowers/specs/2026-05-08-filing-allow-list-and-raw-retention.md` — add a note clarifying that the manifest path now drains N-CSR/N-CSRS through the real fund-metadata parser (#1171). The legacy `SEC_INGEST_KEEP_FORMS` SEC_SKIP entry for N-CSR/N-CSRS STAYS (fund metadata never went through legacy `refresh_filings`; the manifest path and the legacy path are independent). This doc edit prevents future spec-readers from misinterpreting the unchanged SKIP tier as a "parser still missing" signal.

### T12 — Stale help text + wiki

**Files:**
1. `app/services/processes/param_metadata.py:259-265` — remove sec_n_csr "no parser registered yet" verbiage.
2. `docs/wiki/ownership-card.md` lines 27 + 43 — restate "N-CSR audited beats NPORT-P" claim (was moot per spike §10.4; now formally narrowed to fund_metadata table per spec §2 settled-decision).
3. `docs/wiki/glossary.md` lines 36-37 — same restate.

## 3. Pre-push gate checklist

Per `.claude/CLAUDE.md` + memory `[[checklist-pre-push]]`:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -n0 tests/test_manifest_parser_sec_n_csr.py \
                  tests/test_n_csr_extraction.py \
                  tests/test_fund_class_resolver.py \
                  tests/test_refresh_fund_metadata_current.py \
                  tests/test_fund_metadata_endpoints.py \
                  tests/test_cik_refresh.py \
                  tests/test_sec_first_install_drain.py \
                  tests/test_migrations.py \
                  tests/smoke/test_app_boots.py
```

All four (lint, format, pyright, pytest scoped) must pass. Smoke test (`test_app_boots.py`) catches lifespan failures from new endpoint mount.

If impacted-files-clean, optional full `uv run pytest` with xdist for last-mile parity.

ETL DoD clauses 8-12 require the PR description to record observed operator-visible figures, not just the names. The PR body MUST embed this table (filled with actual figures + outcomes at smoke time):

| Instrument | Endpoint hit | Expected outcome | Observed result | expense_ratio_pct | net_assets_amt | period_end | document_type | Commit SHA |
|---|---|---|---|---|---|---|---|---|
| VFIAX | `GET /instruments/VFIAX/fund-metadata` | parsed; ER ≈ 0.04% | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| VOO | `GET /instruments/VOO/fund-metadata` | parsed; ER ≈ 0.03% | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| IVV | `GET /instruments/IVV/fund-metadata` | parsed; ER ≈ 0.03% | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| AGG | `GET /instruments/AGG/fund-metadata` | parsed; bond fund — `credit_quality_allocation` populated | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| FXAIX | `GET /instruments/FXAIX/fund-metadata` | 404 (or empty rollup) — INSTRUMENT_NOT_IN_UNIVERSE tombstoned manifest row | _(fill)_ | n/a | n/a | n/a | n/a | _(fill)_ |

Cross-source verification (DoD clause 9) — required entry:

| Instrument | Field | Parser value | Independent source | Source value | Delta | Acceptable? |
|---|---|---|---|---|---|---|
| VFIAX | expense_ratio_pct | _(fill)_ | Vanguard factsheet `investor.vanguard.com/.../vfiax` | _(fill)_ | _(fill)_ | within ±0% exact match required |
| VFIAX | net_assets_amt | _(fill)_ | Vanguard factsheet | _(fill)_ | _(fill)_ | within ±1% (period_end vs publish-date snapshot OK) |
| VFIAX | 5Y annl return | _(fill)_ | Vanguard factsheet | _(fill)_ | _(fill)_ | within ±0.05% (rounding) |

Backfill (DoD clause 10) — required entry:
- Invocation: `POST /jobs/sec_rebuild/run -d '{"source": "sec_n_csr"}'` at commit `<sha>` on dev DB.
- Manifest worker drain: started at `<UTC ts>`, completed at `<UTC ts>` (`pending` count → 0).
- Observation count after drain: `<N>` rows in `fund_metadata_observations` where `known_to IS NULL`.
- `fund_metadata_current` row count: `<N>`.

Operator-visible figure verification (DoD clause 11) — already implied by the smoke-panel table above; PR description must explicitly state the figure was confirmed against hand-validated N-CSR sample for at least one instrument.

PR description (DoD clause 12) embeds all four tables/blocks above plus the commit SHA at which each verification step ran.

## 4. Codex pre-push 2

After self-review + local gates pass:

```bash
codex exec --output-last-message /tmp/codex_1171_2.txt \
  "Review the branch feature/1171-n-csr-fund-metadata-parser against /Users/lukebradford/Dev/eBull/docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md and /Users/lukebradford/Dev/eBull/docs/superpowers/specs/2026-05-14-n-csr-fund-metadata-plan.md. Focus on: parser correctness (multi-series isolation, axis routing, source-priority chain, append-only supersession); schema correctness (partition + partial unique index + idempotency); test coverage gaps; review-prevention-log applicability; ETL DoD clauses 8-12 completeness in PR body. Reply terse — BLOCKING/WARNING/NIT only." < /dev/null
```

Fix all BLOCKING + WARNING before pushing. NIT triaged: fix-now if small + coupled, else file tech-debt issue.

## 5. PR description outline

Per memory `[[feedback_pr_description_brevity]]`:

### What

- N-CSR / N-CSRS iXBRL → fund-level + class-level metadata observations + write-through current view + 3 read endpoints.
- Replaces #918 / PR #1170 synth no-op.
- Bundles `company_tickers_mf.json` ingest (classId → instrument_id bridge).

### Why

- Operator-visible figures (expense ratio, NAV, returns, sector allocation, portfolio turnover) — discriminating signals for fund-instrument ranking + filter.
- Spike #918 verdict was scoped to audit-attestation on holdings (NO). This PR addresses fund-level metadata extraction (orthogonal new surface). Reconciliation note appended to spike doc.

### Test plan

- [ ] Smoke panel: VFIAX (expense ratio ≈ 0.04%), VOO (≈ 0.03%), IVV (≈ 0.03%), AGG (bond), FXAIX (tombstone instrument_not_in_universe).
- [ ] Cross-source verify VFIAX expense ratio vs Vanguard factsheet (record both figures + delta).
- [ ] Backfill: `POST /jobs/sec_rebuild/run {"source": "sec_n_csr"}`; drain via manifest worker; record drain time + observation count.
- [ ] `GET /instruments/VFIAX/fund-metadata` returns matching values (record commit SHA).
- [ ] Local gates: ruff check, format check, pyright, pytest scoped.

### Settled-decisions

- ADDS: "Source priority for fund metadata" — `period_end DESC, filed_at DESC, source_accession DESC` within `(instrument_id, period_end)`; scoped to fund_metadata, not holdings.
- PRESERVES: filing-event-storage (no raw N-CSR body retained); free regulated-source posture; thin providers; external_identifiers as canonical resolver.

### Spike doc edit

- Appends §10.6 "Scope narrowing — fund-metadata extraction tracked separately under #1171".
- Narrows §10.3 "Product-visibility pivot test: ANSWERED" row to scope it to the audit-attestation lane.

## 6. Risks

- **iXBRL surface drift** (TSR taxonomy version change): mitigated by `raw_facts` capture-then-decide + parser-version bump + golden fixtures.
- **company_tickers_mf.json staleness**: pending-cik-refresh retry path (5 days) avoids permanent tombstone on a transient miss.
- **Multi-series fact bleed**: hard context-tuple filter per spec §8.6.c; test required.
- **Large iShares filings (115 MB primary HTML)**: parser only fetches iXBRL companion (~2 MB max in spike sample). No primary-HTML retention.
- **Concurrent refresh**: `pg_advisory_xact_lock(instrument_id)` serialises per-instrument writes.

## 7. Dispatch matrix

```
T1 (schema)
  ├── T2  (resolver) ──────┐
  ├── T3  (extractor) ─────┤
  ├── T4  (mf ingest) ─────┼─── T5 (parser; needs T2+T3+T6)
  ├── T6  (refresh writer)─┘
  └── T7  (form-to-source)
                             ├── T8 (bootstrap drain; needs T4+T5+T7)
                             └── T9 (endpoints; needs T6)

T1+T2+T3+T4+T5+T6+T7+T8+T9 → T10 (tests; incremental as each task lands) → T11+T12 (docs + cleanup; parallel)
T10+T11+T12 → self-review → Codex pre-push 2 → push
```

Critical-path dependencies (corrected from Codex 1b round 1):

- **T5 depends on T6** because the parser calls `refresh_fund_metadata_current` (T6) inside its per-class fan-out transaction.
- **T8 depends on T7** because the fund-scoped bootstrap drain enqueues both N-CSR and N-CSRS accessions, and N-CSRS must be in `_FORM_TO_SOURCE` first or the manifest insert silently drops the row.

After T1 lands: T2 / T3 / T4 / T6 / T7 parallelize. T5 lands once T2+T3+T6 are in. T8 lands once T4+T5+T7 are in. T9 lands once T6 is in. T10 lands incrementally as each task surfaces its testable contract. T11+T12 land last in parallel.

## 8. Out of scope (file follow-ups)

- Fund-comparison UI view (operator workflow surface).
- N-CEN annual census cross-source verification.
- Historical backfill beyond 2 years per fund.
- UIT-structured trust holdings disclosure.
- Frontend consumption (separate UI ticket).

## 9. Open questions

None — all design choices pinned in spec + plan. Codex 1b should flag anything missed.

---

## Sign-off

- Spec: `docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md` — operator-signed 2026-05-14.
- Codex 1a: round 3 CLEAN (cached at `/tmp/codex_1171_1a_round3.txt`).
- Codex 1b: pending.
- Operator signoff on this plan: pending.
