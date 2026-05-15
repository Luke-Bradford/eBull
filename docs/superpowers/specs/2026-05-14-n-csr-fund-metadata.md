# N-CSR fund-metadata parser — spec

> Status: **DRAFT 2026-05-14** — awaiting Codex pre-spec 1a + operator signoff.
>
> Issue: **#1171** (replaces #918 / PR #1170 synth no-op).
> Branch: `feature/1171-n-csr-fund-metadata-parser`.
> Predecessor: spike `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` (verdict scope = audit-attestation on holdings; THIS spec narrows that verdict and adds a distinct fund-metadata extraction lane).
> Sibling real parser pattern: `app/services/manifest_parsers/sec_10k.py` (#1152, PR equivalent for Option C filed_at gate + share-class fan-out + raw-payload invariant adherence).

## 1. Reconciliation with spike #918 verdict

The spike's §10.5 product-visibility-test answer (**NO**) was scoped to **audit-attestation on holdings** — badging existing N-PORT-P rows as "audited" via a 4-part name-and-shares match. This spec addresses a **different operator surface** — fund-level + class-level metadata extraction — that the spike did not evaluate.

Each of the spike's five §9.2 arguments transfers (or fails to transfer) as follows:

| Spike §9.2 argument | Holdings-attestation (spike) | Fund-metadata (this spec) |
|---|---|---|
| 1. Vanguard text directs reader to N-PORT for holdings | True for holdings | N-PORT-P does NOT publish expense ratio / NAV / returns / sector allocation / portfolio turnover. The fund itself does NOT redirect for these fields |
| 2. Data is the same as N-PORT-P | True (same year-end portfolio) | N-PORT-P has zero overlap with the fund-metadata field set. This is a NEW surface, not a duplicate |
| 3. Audit badge uniform across funds → no discriminating signal | True | Expense ratio ranges 0.03% (IVV) to ~1.5% (active funds). NAV ranges $1B-$500B. Returns / turnover / holdings count all span order-of-magnitude. Strongly discriminating per the table below |
| 4. Name-matching false-claim risk | True (required 4-part match) | `oef:ClassAxis` member → `classId` (`C000NNNNNN`) is a structured XBRL dimension with deterministic resolution. Zero name matching |
| 5. Per-family HTML-layout fragility | True (3 distinct HTML layouts) | iXBRL is SEC-versioned OEF taxonomy. Bounded by taxonomy releases (~annual cadence), not per-family layout (per-PR cadence) |

**Per-field discrimination evidence (operator-visible):**

| Field | Concept | Range across universe | Operator use case | Discriminating? |
|---|---|---|---|---|
| Expense ratio | `oef:ExpenseRatioPct` | 0.03% (IVV) – 1.5% (active) | Universe filter ER < 0.10% | Yes (50× spread) |
| Net assets | `us-gaap:AssetsNet` | $1B – $500B | Sort by AUM; capacity-constraint thesis | Yes (500× spread) |
| 1Y/5Y/10Y returns | `oef:AvgAnnlRtrPct` | -30% to +50% | Sort by 5Y; vs benchmark deviation | Yes |
| Portfolio turnover | `us-gaap:InvestmentCompanyPortfolioTurnover` | 1% (passive) – 200% (active) | Tax-efficiency screen | Yes (200× spread) |
| Holdings count | `oef:HoldingsCount` | 30 – ~9000 | Concentration thesis | Yes |
| Sector allocation | `oef:PctOfNav` × `IndustrySectorAxis` | varies | Tilt detection; cross-fund overlay | Yes |
| Material-change date | `oef:MaterialChngDate` | sparse | Strategy/manager-change alert | Yes (when present) |
| Returns vs benchmark | `oef:AvgAnnlRtrPct` × `BroadBasedIndexAxis` | varies | Alpha estimation; passive-vs-active triage | Yes |

**Spike side-finding §10.4 supports this approach.** The 2024-07-24 TSR rule (Release 33-11125) reshaped post-TSR N-CSR primary HTML to a 2-3 page shareholder report + Item 7 financial statements. The OEF iXBRL taxonomy covers exactly the post-TSR section — the fields above. The spike's "iXBRL has rich fund-level facts but no per-holding CUSIP" finding is the **green light for this spec**, not a red light.

**Net edit to spike doc:** this PR will append a §10.6 "Scope narrowing — fund-metadata extraction tracked separately under #1171" to `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` and narrow the §10.3 row "Product-visibility pivot test: ANSWERED" to specifically scope it to the audit-attestation lane. The spike's audit-attestation verdict (INFEASIBLE-CONFIRMED + synth no-op recommendation for holdings) is **PRESERVED** — this PR does not write holdings from N-CSR.

## 2. Settled-decisions check (`docs/settled-decisions.md`)

| Decision | Applies? | Preservation |
|---|---|---|
| Fundamentals provider posture (free regulated-source only #532) | Yes | PRESERVED — OEF iXBRL is free regulated SEC. No new paid provider |
| Filing event storage (full raw filing text out of scope v1) | Yes | PRESERVED — `requires_raw_payload=False`; parser fetches iXBRL companion on-the-fly to extract facts but **does not** call `store_raw`. Re-parse on parser-version bump re-fetches from SEC (acceptable cost: ~48k fetches at 10 r/s = ~80 min wall-clock for a full universe rewash) |
| Provider design rule (thin adapters) | Yes | PRESERVED — fetcher uses existing `SecFilingsProvider.fetch_document_text`. Parser logic lives in service layer (`app/services/manifest_parsers/sec_n_csr.py`) |
| External identifiers (`external_identifiers` table is the canonical resolver) | Yes | PRESERVED — new `identifier_type='class_id'` rows populated by the bundled `company_tickers_mf.json` ingest. Resolver uses the same shape as every other SEC source |
| Filing dedupe (provider-scoped + stable + idempotent) | Yes | PRESERVED — partial unique index on currently-valid rows: `(instrument_id, source_accession, period_end) WHERE known_to IS NULL`. Functional equivalence to `(instrument_id, source_accession)` because `period_end` is derived per-accession; `period_end` in the index is mandated by PostgreSQL partition-key inclusion rule |
| Identity resolution (I10: instruments BIGINT, filers TEXT cik 10-digit padded) | Yes | EXTENDED — new third identifier `classId` (`C000NNNNNN`) joins via `external_identifiers`. Instruments stay BIGINT, classId is TEXT, trust CIK stays 10-digit TEXT |

**NEW settled-decision proposal (lands as a row in `docs/settled-decisions.md` in this PR):**

> ### Source priority for fund metadata
> Within `(instrument_id, period_end)`, the winning observation is selected by:
>
>   `ORDER BY period_end DESC, filed_at DESC, source_accession DESC LIMIT 1`
>
> - **`period_end DESC`** — most recent reporting period wins. N-CSR (annual, fiscal-year-end) and N-CSRS (semi-annual, mid-year) have disjoint period_end values by SEC rule §31a-29 (annual covers full fiscal year; semi-annual covers first half) so they do not compete at the same period_end.
> - **`filed_at DESC`** — at the same period_end, amendments (N-CSR/A, N-CSRS/A) naturally win because they are filed later than the original they amend.
> - **`source_accession DESC`** — final deterministic tie-break for unlikely-but-possible same-filed_at collisions.
> - Parser-version bump is orthogonal: rewash flows through `known_to` supersession (see §6) and the priority chain re-evaluates against the new currently-valid row set.
> - **Scope**: applies to `fund_metadata_observations → fund_metadata_current` only. Does NOT apply to holdings (N-CSR holdings are not ingested — spike §10.5 stands).

## 3. Review-prevention-log applicable entries

Entries from `docs/review-prevention-log.md` that this spec must respect (cited in implementation):

| Entry | How this spec avoids it |
|---|---|
| **Missing data on hard-rule path silently passes** (#45) | classId resolver miss is a deterministic tombstone with reason `instrument_not_in_universe` or `class_id_unknown`, never a silent skip. Per-class fan-out has explicit unresolved-count logging |
| **JOIN fan-out inflates aggregate totals** (#45) | `fund_metadata_observations` has at most one currently-valid row per (instrument_id, source_accession) (enforced by partial unique index `WHERE known_to IS NULL`); `fund_metadata_current` is one row per instrument_id (PK). No JOINs that fan out on read |
| **Audit reads outside the write transaction** (#66) | Source-priority decision (which observation is "winner") reads incumbent + writes `fund_metadata_current` inside one `with conn.transaction()`. Plus `pg_advisory_xact_lock` per instrument_id (mirrors I7) |
| **Read-then-write cap enforcement outside transaction** (#66) | Same as above — filed_at gate is a read-then-write pattern inside one tx + advisory lock |
| **Single-row UPDATE silent no-op on missing row** (#70) | `refresh_fund_metadata_current` uses `INSERT ... ON CONFLICT (instrument_id) DO UPDATE` so missing row is handled correctly; `RETURNING` captures rowcount |
| **f-string SQL composition for identifiers** (#110) | All SQL uses `%(name)s` for values; no identifier interpolation. Column lists are literal strings inside `sql.SQL("...")` if ever needed |
| **Mid-transaction `conn.commit()` in service functions** (#110) | Parser receives caller's connection from the manifest worker; never calls `conn.commit()` or `conn.rollback()`. Uses `with conn.transaction()` savepoints only |
| **Naive datetime in TIMESTAMPTZ params** (#80) | `filed_at` is propagated from `row.filed_at` (already TIMESTAMPTZ from manifest); `since` query param on history endpoint is coerced to UTC if naive |
| **Health endpoint HTTP 200 on infrastructure failure** (#70) | `/instruments/{symbol}/fund-metadata` raises `HTTPException(503)` on infra failure, not `{"error": ...}` 200 |
| **Unbounded enum filters accept nonsense** (#77) | History endpoint param `since: date | None = Query(None)` is typed; no free-form string enum |
| **Internal exception text leaked into HTTP responses** (#86) | Endpoint exception handler uses fixed phrase strings; full exc text goes to `logger.exception` only |
| **`assert` as runtime guard in service code** (#109) | Parser and refresh writer use `if x is None: raise RuntimeError(...)`, never `assert` |
| **ON CONFLICT DO NOTHING counter overcount** (#69) | If any insert counters used in the bundled ingest, gated on `result.rowcount > 0` |

## 4. Scope (in / out)

### 4.1 In-scope (this PR lands)

1. **Schema migration** (`sql/NNN_fund_metadata.sql`):
   - `fund_metadata_observations` — append-only event log. Partitioned by `RANGE(period_end)` quarterly per the two-layer ownership-model convention (data-engineer §1.2 invariant I8). One *currently-valid* row per `(instrument_id, source_accession)` is enforced by a partial unique index `WHERE known_to IS NULL` on `(instrument_id, source_accession, period_end)` — `period_end` is included because PostgreSQL requires the partition key in unique constraints, but `period_end` is functionally dependent on `source_accession` (one DocumentPeriodEndDate per accession) so the partial index effectively enforces uniqueness on `(instrument_id, source_accession)` alone. Soft-deleted rows (`known_to IS NOT NULL`) are exempt so parser-version rewashes can supersede.
   - `fund_metadata_current` — write-through current state, one row per instrument_id (PK).
   - Indexes per access pattern.
2. **classId → instrument_id bridge**:
   - `company_tickers_mf.json` ingest (~28k mutual-fund + ETF rows with seriesId + classId + symbol). Ingest path: bundled in this PR (operator-chosen). Lives in `app/services/cik_refresh.py` (extends Stage 6 `daily_cik_refresh`) — populates `external_identifiers` with new `identifier_type='class_id'` rows.
   - Bootstrap stage updated to depend on `cik_refresh` completion before fund-scoped first-install drain runs.
3. **Real parser** `app/services/manifest_parsers/sec_n_csr.py` — REPLACES the #918 synth no-op:
   - Fetches iXBRL companion via existing `SecFilingsProvider`.
   - Extracts the §1 field set via lxml + XBRL context-ref resolution.
   - Resolves per-class observations via classId → instrument_id.
   - Writes per (instrument_id, accession) — multi-series + multi-class fan-out where applicable.
   - Tombstones classes not in universe (`instrument_not_in_universe`).
   - Source-priority gate at `refresh_fund_metadata_current` (read-then-write inside one tx + advisory lock).
4. **`register_all_parsers()` wire** — replace `sec_n_csr` no-op registration with the new parser (`requires_raw_payload=False`, per operator choice).
5. **Endpoints**:
   - `GET /instruments/{symbol}/fund-metadata` → current row (most recent + source-priority winner).
   - `GET /instruments/{symbol}/fund-metadata/history?since=YYYY-MM-DD` → observation timeline.
   - `GET /coverage/fund-metadata` → operator audit (per-source coverage + resolver-miss count).
6. **Tests** (per §13).
7. **Documentation updates**:
   - `.claude/skills/data-sources/sec-edgar.md` §11.5 row for sec_n_csr — flip "synth no-op" to "real parser landed".
   - `.claude/skills/data-sources/edgartools.md` G12 — restate: holding-level CUSIP still absent (spike verdict stands); fund-level facts now extracted by this parser.
   - `.claude/skills/data-engineer/SKILL.md` — new section "Fund metadata observations + current" mirroring the two-layer ownership-model conventions (§1.2 shape).
   - `.claude/skills/data-engineer/etl-endpoint-coverage.md` row 47 — restate from "synth no-op landed" to "real parser landed" with this PR reference.
   - `.claude/skills/metrics-analyst/SKILL.md` — new rows for each operator-visible fund-metadata figure (source → transform → table → endpoint → chart + validation).
   - `docs/settled-decisions.md` — new "Source priority for fund metadata" row.
   - Spike doc `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` — append §10.6 "Scope narrowing" + narrow §10.3 wording.

### 4.2 Out of scope (file follow-ups if needed)

- Fund-comparison view (side-by-side N funds).
- Cross-source verification against N-CEN annual census (different cadence + sample).
- Historical backfill beyond 2 years per fund (semi-annual cadence keeps recent state fresh).
- Per-class share-class-redemption-fee structure (deeper iXBRL dive — capture in `raw_facts` JSONB; surface later if it materialises).
- UIT-structured trusts (QQQ, SPY) — they don't file N-CSR. Tracked in `etl-endpoint-coverage.md` side-finding.
- N-CSRS form-to-source mapping. Spec adds N-CSRS to `_FORM_TO_SOURCE` in `app/services/sec_manifest.py` (currently absent — spike §2.1) because this parser handles both N-CSR + N-CSRS. Required, in-scope.

## 5. Data inventory + concept → column map

All concepts observed in `tmp/spike-918/full_concept_sweep.py` output. Three tiers per §1B:

**Tier 1 — typed columns** (1:1 or 1:N per accession × classId, reportable):

| Concept (XBRL qname) | Column | Type | Notes |
|---|---|---|---|
| `dei:EntityCentralIndexKey` | `trust_cik` | TEXT NOT NULL | 10-digit zero-padded (mirror identity invariant I10) |
| `dei:EntityRegistrantName` | `trust_name` | TEXT | |
| `dei:EntityInvCompanyType` | `entity_inv_company_type` | TEXT | typical `N-1A`; record-only for now |
| `dei:DocumentType` | `document_type` | TEXT NOT NULL | CHECK `IN ('N-CSR', 'N-CSR/A', 'N-CSRS', 'N-CSRS/A')` |
| `dei:DocumentPeriodEndDate` | `period_end` | DATE NOT NULL | partition key |
| `dei:AmendmentFlag` | `amendment_flag` | BOOLEAN NOT NULL DEFAULT FALSE | |
| `dei:SecurityExchangeName` | `exchange` | TEXT | per-class context; ETFs only |
| `dei:TradingSymbol` | `trading_symbol` | TEXT | per-class context |
| `oef:FundName` | `series_name` | TEXT | per-series context |
| `oef:ClassName` | `class_name` | TEXT | per-class context |
| `oef:PerfInceptionDate` | `inception_date` | DATE | |
| `oef:ShareholderReportAnnualOrSemiAnnual` | `shareholder_report_type` | TEXT | TSR stamp |
| `oef:ExpenseRatioPct` | `expense_ratio_pct` | NUMERIC(12,8) | per-class |
| `oef:ExpensesPaidAmt` | `expenses_paid_amt` | NUMERIC | per-class |
| `oef:AdvisoryFeesPaidAmt` | `advisory_fees_paid_amt` | NUMERIC | per-series (sometimes per-class) |
| `us-gaap:AssetsNet` | `net_assets_amt` | NUMERIC | per-series |
| `us-gaap:InvestmentCompanyPortfolioTurnover` | `portfolio_turnover_pct` | NUMERIC(12,6) | per-series |
| `oef:HoldingsCount` | `holdings_count` | INTEGER | per-series |
| `oef:MaterialChngDate` | `material_chng_date` | DATE | |
| `oef:MaterialFundChngNoticeTextBlock` | `material_chng_notice` | TEXT | HTML-stripped to plain text; bounded at 16 KB hard cap; longer → truncate + flag |
| `oef:AddlInfoPhoneNumber` | `contact_phone` | TEXT | |
| `oef:AddlInfoWebsite` | `contact_website` | TEXT | |
| `oef:AddlInfoEmail` | `contact_email` | TEXT | |
| `oef:UpdProspectusPhoneNumber` | `prospectus_phone` | TEXT | |
| `oef:UpdProspectusWebAddress` | `prospectus_website` | TEXT | |
| `oef:UpdProspectusEmailAddress` | `prospectus_email` | TEXT | |
| derived from `oef:SeriesId` context | `series_id` | TEXT | `S000NNNNNN` |
| derived from `oef:ClassAxis` context | `class_id` | TEXT NOT NULL | `C000NNNNNN`; bridge key |

**Tier 2 — bounded JSONB columns** (dimensional, many-valued). Routing is by **`(concept_qname, axis_qname)` tuple**, not concept alone — `oef:PctOfNav` reappears across sector / region / credit axes and would mis-bucket if routed by concept-name alone (Codex 1a WARNING-2):

| (Concept, Axis) tuple | Target column | Shape | Notes |
|---|---|---|---|
| (`oef:AvgAnnlRtrPct`, `srt:RangeAxis`) with member ∈ allowlist below | `returns_pct` | `{"1Y": 0.1771, "5Y": 0.1234, ...}` | Per-class. Period member must be in the §5.A allowlist |
| (`oef:AvgAnnlRtrPct`, `oef:BroadBasedIndexAxis`) | `benchmark_returns_pct["broad_based"][period]` | `{"broad_based": {"1Y": 0.1812, ...}}` | Per-class per-benchmark |
| (`oef:AvgAnnlRtrPct`, `oef:AdditionalIndexAxis`) | `benchmark_returns_pct["additional"][period]` | `{"additional": {"1Y": ...}}` | Per-class per-benchmark |
| (`oef:PctOfNav`, `oef:IndustrySectorAxis`) | `sector_allocation` | `{"Communication Services": 0.106, ...}` | Per-series |
| (`oef:PctOfNav`, `oef:GeographicRegionAxis`) | `region_allocation` | `{"United States": 0.97, ...}` | Per-series |
| (`oef:PctOfNav`, `oef:CreditQualityAxis`) | `credit_quality_allocation` | `{"AAA": 0.45, ...}` | Bond funds |
| (`oef:PctOfTotalInv`, `oef:IndustrySectorAxis` ∨ `GeographicRegionAxis` ∨ `CreditQualityAxis`) | same allocation column as `PctOfNav` for that axis, scaled to NAV using `holdings_count + net_assets_amt` denominator (DOCUMENTED in column comment as PctOfTotalInv-derived) — OR keep separately under `<col>_pct_of_total_inv` if both are present. Decision: prefer `PctOfNav` (NAV-denominated, comparable across funds); fall back to `PctOfTotalInv` only when NAV-denominated is absent for that axis | per-series | Most funds carry one OR the other, not both. Spike sample carries `PctOfNav` |
| (`oef:AccmVal`, period dim ∈ allowlist) | `growth_curve` | `[{"period_end": "...", "value": 12345.67, "axis_member": "..."}, ...]` | Reconstructed in time-order per period_end |

**Unmapped `(concept, axis)` tuples** — including any axis member NOT in the §5.A allowlist — route to `raw_facts` (Tier 3 fallback). The parser NEVER buckets an unrecognised tuple into a typed column on guess.

### 5.A Period-axis allowlist for `srt:RangeAxis` members on `AvgAnnlRtrPct`

Members observed in spike-sampled iXBRL (Vanguard / Fidelity / iShares); any member NOT in this list routes to `raw_facts`:

| Allowlist member | Period key in `returns_pct` JSONB |
|---|---|
| `oef:OneYearMember` (or trust-namespace equivalent) | `"1Y"` |
| `oef:FiveYearsMember` | `"5Y"` |
| `oef:TenYearsMember` | `"10Y"` |
| `oef:SinceInceptionMember` | `"SinceInception"` |
| `oef:LifeOfFundMember` | `"LifeOfFund"` (alias for SinceInception in some filers; recorded distinct so the parser doesn't lose provenance) |

Concrete enumeration is finalized from the per-family golden fixtures at parser implementation time. Any new member encountered in production triggers a `raw_facts` entry + a logged warning (not a failure) so the operator can audit + extend the allowlist in a follow-up.

### 5.B Period-axis allowlist for `oef:AccmVal` (growth-of-$10K curve)

Acceptable period-axis members enumerate to month-end / quarter-end / fiscal-year-end markers. Members are recovered from the context's `<xbrli:period><xbrli:instant>` element. Curve points are sorted by `instant` ASC for `growth_curve` output. Unsorted output is a bug (test required).

**Tier 3 — capture-then-decide fallback**:

- `raw_facts` JSONB column: `{"namespace:concept": [{"value": ..., "context_ref": ..., "dim_members": {axis: member}}, ...]}`.
- Captures every concept observed in the iXBRL **not** modelled by Tier 1 or Tier 2.
- Hard cap: 32 KB serialized; if exceeded, truncate with a sentinel `__truncated__: true` key.

**Boilerplate + text-block fields (NOT extracted to typed columns; routing per below):**

Spike §8.2 observed the following text-block concepts. Each must have an explicit routing decision (Codex 1a WARNING-6):

| Concept | Routing | Why |
|---|---|---|
| `oef:HoldingsTableTextBlock` | **BLOCKLIST** (not in raw_facts) | Spike §8.2: 1-30 KB sector-allocation text. Already captured structurally via `(oef:PctOfNav, oef:IndustrySectorAxis)`. Storing the HTML duplicate would 10-50× the row size |
| `oef:AvgAnnlRtrTableTextBlock` | **BLOCKLIST** | Text rendering of returns table. Already captured via `oef:AvgAnnlRtrPct` Tier 2 |
| `oef:LineGraphTableTextBlock` | **BLOCKLIST** | Text rendering of growth-of-$10K. Already captured via `oef:AccmVal` Tier 2 |
| `oef:AddlFundStatisticsTextBlock` | **BLOCKLIST** | Generic fund-stats narrative — operator-discriminating signal is zero |
| `oef:FactorsAffectingPerfTextBlock` | **route to `raw_facts`** (size cap 8 KB; truncate beyond) | Narrative on what drove fund performance — potential thesis input. Not currently surfaced through endpoints but retained for future model iteration |
| `oef:AnnlOrSemiAnnlStatementTextBlock` | **BLOCKLIST** | TSR intro boilerplate |
| `oef:PerformancePastDoesNotIndicateFuture` | **BLOCKLIST** | Standard disclaimer |
| `oef:NoDeductionOfTaxesTextBlock` | **BLOCKLIST** | Standard disclaimer |
| `oef:MaterialFundChngNoticeTextBlock` | **Tier 1 `material_chng_notice`** (HTML-stripped to plain text; 16 KB hard cap; truncate beyond with `__truncated__` sentinel) | Material change is operator-actionable |

The parser maintains an explicit `_BOILERPLATE_BLOCKLIST` constant against which every concept observed in the iXBRL is checked. Blocklist concepts are skipped entirely (NOT included in raw_facts and NOT extracted to Tier 1). New text-blocks not in the blocklist or the explicit routing map default to **`raw_facts` capture with size cap** so future iteration has the data; a logged-warning fires on each new concept to surface the gap.

## 6. Schema (DDL sketch)

Migration `sql/NNN_fund_metadata.sql` (next available number at implementation time):

```sql
-- Migration NNN: fund_metadata_observations + fund_metadata_current.
--
-- Append-only event log + write-through current state for fund-level + class-level
-- facts extracted from N-CSR / N-CSRS iXBRL. Mirrors the two-layer ownership model
-- (data-engineer §1.2).

CREATE TABLE IF NOT EXISTS fund_metadata_observations (
    observation_id          BIGSERIAL,
    instrument_id           BIGINT NOT NULL REFERENCES instruments(instrument_id),
    source_accession        TEXT NOT NULL,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_end              DATE NOT NULL,
    document_type           TEXT NOT NULL
        CHECK (document_type IN ('N-CSR', 'N-CSR/A', 'N-CSRS', 'N-CSRS/A')),
    amendment_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    parser_version          TEXT NOT NULL,

    -- Series + class identity
    trust_cik               TEXT NOT NULL,
    trust_name              TEXT,
    entity_inv_company_type TEXT,
    series_id               TEXT,
    series_name             TEXT,
    class_id                TEXT NOT NULL,
    class_name              TEXT,
    trading_symbol          TEXT,
    exchange                TEXT,
    inception_date          DATE,
    shareholder_report_type TEXT,

    -- Per-class economics
    expense_ratio_pct       NUMERIC(12, 8),
    expenses_paid_amt       NUMERIC,
    net_assets_amt          NUMERIC,
    advisory_fees_paid_amt  NUMERIC,
    portfolio_turnover_pct  NUMERIC(12, 6),
    holdings_count          INTEGER,

    -- Tier 2 — dimensional JSONB
    returns_pct             JSONB,
    benchmark_returns_pct   JSONB,
    sector_allocation       JSONB,
    region_allocation       JSONB,
    credit_quality_allocation JSONB,
    growth_curve            JSONB,

    -- Material change
    material_chng_date      DATE,
    material_chng_notice    TEXT,

    -- Contact / diligence
    contact_phone           TEXT,
    contact_website         TEXT,
    contact_email           TEXT,
    prospectus_phone        TEXT,
    prospectus_website      TEXT,
    prospectus_email        TEXT,

    -- Tier 3 fallback
    raw_facts               JSONB,

    -- Provenance (uniform with ownership_*_observations §1.2; supports
    -- append-only semantics + I6 soft-delete supersession on rewash)
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    known_to                TIMESTAMPTZ,
    ingest_run_id           UUID,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- PK includes period_end because the table is RANGE-partitioned by it
    -- (PostgreSQL requires partition key in PK / UNIQUE constraints).
    PRIMARY KEY (observation_id, period_end)
) PARTITION BY RANGE (period_end);

-- Partial unique index: at most ONE currently-valid row per
-- (instrument_id, source_accession). Soft-deleted rows (known_to IS NOT NULL)
-- are exempt so a parser-version rewash can supersede the prior row.
-- The unique index includes period_end because the table is partitioned by it.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fund_metadata_observations_current
    ON fund_metadata_observations (instrument_id, source_accession, period_end)
    WHERE known_to IS NULL;

-- Create partitions for 2010-2030 quarterly + default (mirror sql/113 ownership_*).
-- (DDL elided for brevity; same generator pattern as existing ownership partitions.)

CREATE INDEX fund_metadata_observations_class_id
    ON fund_metadata_observations (class_id);
CREATE INDEX fund_metadata_observations_period_end
    ON fund_metadata_observations (instrument_id, period_end DESC);
CREATE INDEX fund_metadata_observations_filed_at
    ON fund_metadata_observations (filed_at DESC);


-- ``fund_metadata_current`` mirrors the full Tier 1 + Tier 2 column set of
-- the observation table. The ONLY intentionally-omitted columns are:
--   - ``raw_facts`` (Tier 3 fallback — per-observation audit data, never
--     surfaced through the read endpoints).
--   - Provenance columns (``known_from``, ``known_to``, ``ingest_run_id``,
--     ``ingested_at``) — _current is a projection of the currently-valid
--     observation; provenance lookup goes through the observation table.
-- All other columns mirror their observation counterparts so the read
-- endpoint can serve the full operator-visible figure set without a JOIN.

CREATE TABLE IF NOT EXISTS fund_metadata_current (
    instrument_id           BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    source_accession        TEXT NOT NULL,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_end              DATE NOT NULL,
    document_type           TEXT NOT NULL,
    amendment_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    parser_version          TEXT NOT NULL,

    trust_cik               TEXT NOT NULL,
    trust_name              TEXT,
    entity_inv_company_type TEXT,
    series_id               TEXT,
    series_name             TEXT,
    class_id                TEXT NOT NULL,
    class_name              TEXT,
    trading_symbol          TEXT,
    exchange                TEXT,
    inception_date          DATE,
    shareholder_report_type TEXT,

    expense_ratio_pct       NUMERIC(12, 8),
    expenses_paid_amt       NUMERIC,
    net_assets_amt          NUMERIC,
    advisory_fees_paid_amt  NUMERIC,
    portfolio_turnover_pct  NUMERIC(12, 6),
    holdings_count          INTEGER,

    returns_pct             JSONB,
    benchmark_returns_pct   JSONB,
    sector_allocation       JSONB,
    region_allocation       JSONB,
    credit_quality_allocation JSONB,
    growth_curve            JSONB,

    material_chng_date      DATE,
    material_chng_notice    TEXT,

    contact_phone           TEXT,
    contact_website         TEXT,
    contact_email           TEXT,
    prospectus_phone        TEXT,
    prospectus_website      TEXT,
    prospectus_email        TEXT,

    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX fund_metadata_current_expense_ratio
    ON fund_metadata_current (expense_ratio_pct);
CREATE INDEX fund_metadata_current_net_assets
    ON fund_metadata_current (net_assets_amt DESC);
```

**Append-only invariant (data-engineer I6).** `fund_metadata_observations` is append-only with `known_to` supersession — the parser NEVER `UPDATE`s a Tier 1 or Tier 2 column on an existing row. A parser-version rewash marks the previous row(s) for the same `(instrument_id, source_accession)` as `known_to = NOW()` and INSERTs a fresh row with `known_from = NOW(), known_to = NULL`. The partial unique index (`WHERE known_to IS NULL`) enforces "one currently-valid row per (instrument_id, source_accession)" while permitting a history of superseded rows.

**Migration shape-check (data-engineer §0 step 4):** uses `pg_constraint.contype + conkey` + `pg_index.indisunique + indkey` introspection for idempotency. New migration mirrors the partitioning pattern in `sql/113_ownership_insiders_observations.sql`.

**FK cascade analysis (data-engineer §0 step 3):** `ON DELETE` posture is `RESTRICT` from `instruments(instrument_id)` (default). No `*_audit` / `*_log` shape so no cascade-destruction risk (prevention-log L350).

## 7. classId → instrument_id resolution (bundled ingest)

### 7.1 Source: `company_tickers_mf.json`

URL: `https://www.sec.gov/files/company_tickers_mf.json`. ~28k rows; shape:

```json
{
  "fields": ["cik", "seriesId", "classId", "symbol"],
  "data": [[123456, "S000003008", "C000003008", "VFINX"], ...]
}
```

Fetched via existing `SecFilingsProvider`-shape pool path (per provider design rule). Conditional GET ETag-aware (mirrors `company_tickers.json` ingest at `app/providers/implementations/sec_edgar.py:52`).

### 7.2 Ingest sites

1. `app/services/cik_refresh.py` — extend `daily_cik_refresh` (Stage 6) to fetch + parse `company_tickers_mf.json` and upsert into `external_identifiers`:
   - `(provider='sec', identifier_type='class_id', identifier_value=<classId>) → instrument_id`
   - Resolution chain: symbol → instruments.symbol (preferred) → instrument_id. Symbol-missing rows are skipped (not in eToro universe).
   - `is_primary=TRUE` per (instrument_id, provider, identifier_type='class_id') via the partial unique index already defined in `sql/003`.
2. Bootstrap: Stage 6 (`cik_refresh`) bumps its capability output to include `class_id_mapping_ready`. Fund-scoped first-install drain depends on it (per `bootstrap_orchestrator.py`).

### 7.3 Resolver helper

`app/services/manifest_parsers/_fund_class_resolver.py` (new):

```python
def resolve_class_id_to_instrument(conn, class_id: str) -> int | None:
    """Look up instrument_id via external_identifiers (provider='sec', identifier_type='class_id')."""
```

Service-layer only (per provider design rule). Used by `sec_n_csr.py` parser and any future fund-metadata reader.

### 7.4 Resolver-miss handling

**No symbol-only fallback.** Symbol fallback is unsafe: tickers get reused after delisting; share-class symbols can match non-fund instruments; ETF/mutual-fund tickers can collide across exchanges. The classId → instrument_id resolution path is **either `external_identifiers` row exists, or retry-then-tombstone** — no heuristic shortcut.

| Branch | Action |
|---|---|
| classId present in `external_identifiers` (`provider='sec', identifier_type='class_id'`) | Resolve to instrument_id; write observation |
| classId NOT present (cik_refresh hasn't run since the new mf.json row appeared) | `failed_outcome` with 24h backoff (reason: `class_id_pending_cik_refresh`). Up to 5 retries (5 days) — gives daily cik_refresh time to populate. Beyond that → tombstone permanent with `class_id_unknown_persistent` |
| classId IS in `company_tickers_mf.json` AND symbol matches an instrument, BUT NO `external_identifiers` row exists | Same as above — defer to cik_refresh to write the canonical bridge row. Per provider-design-rule (settled-decisions §"External identifiers"), service code does not bypass the resolver |
| classId IS in `company_tickers_mf.json` AND no corresponding instrument exists (mutual fund / ETF not in eToro universe) | Tombstone with `instrument_not_in_universe` (deterministic; classId is known but the security is not in our universe). Surface in `/coverage/fund-metadata` |

**Resolver-miss discrimination on retry-vs-tombstone:** the parser distinguishes "pending cik_refresh" (transient — `class_id` row absent from `external_identifiers` but classId is well-formed per regex `^C[0-9]{9}$`) from "instrument not in universe" (deterministic — `class_id` IS in `company_tickers_mf.json` but no `instrument_id` corresponds to that symbol). This requires a second probe of the bundled MF directory tracked in `external_identifiers` indirectly; implementation note: the cik_refresh extension exposes a small helper `class_id_known_to_mf_directory(class_id: str) -> bool` queried by the resolver-miss branch.

**Partial-success rule** (prevention-log "Missing data on hard-rule path silently passes"): if a multi-series filing has 5 classes and 3 resolve while 2 miss with different miss-reasons, write the 3 observations + log the 2 unresolved (each with its specific miss-reason) + return `parsed` with a degraded marker in the per-accession parser log. Do NOT tombstone the whole accession if any class resolves (3 valuable observations > none). If ALL classes miss with the same reason → tombstone the accession with that reason; if ALL miss with mixed reasons → tombstone with `class_id_unknown_persistent` priority.

## 8. Parser flow (`_parse_sec_n_csr`)

Steps (mirrors `sec_10k._parse_sec_10k` but adapted to per-class fan-out):

1. **Validate URL + cik** (tombstone on missing; no instrument_id check at row-level because the parser fans out via classId per-class, not per-issuer-cik).
2. **Fetch iXBRL companion**. Determination: from the manifest row's primary doc URL, the iXBRL companion is `<basename>_htm.xml` in the same accession folder. Spike §3.3 confirms this shape.
   - Exception → `failed_outcome` (1h backoff).
   - Empty → tombstone (`empty or non-200 fetch`).
3. **Parse iXBRL** via lxml. Build the context dimension map.
   - Exception → `failed_outcome`. After 1 retry of the transient class → tombstone.
4. **Build context-dimension index.** For every `<xbrli:context>` element in the iXBRL companion:
   - Capture every dimensional axis member: `oef:ClassAxis`, `oef:SeriesAxis` (or `oef:SeriesId` where present), `oef:IndustrySectorAxis`, `oef:GeographicRegionAxis`, `oef:CreditQualityAxis`, `oef:AdditionalIndexAxis`, `oef:BroadBasedIndexAxis`, `srt:RangeAxis` (and any other axis present).
   - Store as `{context_ref: {axis_qname: member_qname, ...}}` mapping.
5. **Enumerate series + classes from the dimension map.**
   - Series candidates: distinct `SeriesAxis` / `SeriesId` member values observed across all contexts.
   - For each series, classes belonging to it: distinct `ClassAxis` member values appearing in contexts that ALSO carry the matching series member (or contexts that carry only ClassAxis when the trust files a single series — verified by absence of SeriesAxis variation across the entire file).
   - Build `series_to_classes: {seriesId: [classId, ...]}` map.
6. **Per (seriesId, classId), HARD CONTEXT FILTER + extract.** For each tuple in the map:
   - a. Resolve classId → instrument_id via `_fund_class_resolver` (per §7.3 + §7.4).
   - b. If resolver miss → log per-miss reason + continue (partial-success rule §7.4).
   - c. **Hard context filter (BLOCKING-2 fix):**
        - For each fact required by the observation, locate facts whose `context_ref` resolves to a context with the EXACT (seriesId, classId) tuple for class-level facts, or to a context with the EXACT seriesId AND NO ClassAxis member for series-level facts.
        - A fact whose context dimensions do NOT match the target tuple is REJECTED for this observation (the fact belongs to a sibling class or sibling series and would bleed cross-series). Rejected facts remain in the index for their correct (series, class) tuple but are NOT copied.
        - Reasoning: Vanguard accession -021519 carries multiple series in one document; without this filter, sibling-series HoldingsCount or AssetsNet would silently attach to the wrong observation.
   - d. **Per-axis Tier 2 routing.** Tier 2 JSONB columns route by `(concept_qname, axis_qname)` tuple, not concept alone:
        - `(oef:PctOfNav, oef:IndustrySectorAxis)` → `sector_allocation[member_label]`.
        - `(oef:PctOfNav, oef:GeographicRegionAxis)` → `region_allocation[member_label]`.
        - `(oef:PctOfNav, oef:CreditQualityAxis)` → `credit_quality_allocation[member_label]`.
        - `(oef:AvgAnnlRtrPct, srt:RangeAxis member ∈ allowlist)` → `returns_pct[period_member_label]`.
        - `(oef:AvgAnnlRtrPct, oef:BroadBasedIndexAxis | oef:AdditionalIndexAxis)` → `benchmark_returns_pct[benchmark_label][period_label]`.
        - `(oef:AccmVal, period dim)` → `growth_curve[]` in time-order.
        - Any unmapped `(concept, axis)` tuple → `raw_facts` (Tier 3 fallback). NEVER bucketed into a typed column on guess.
   - e. **Tier 1 single-value extraction.** For facts with no dimensional axis other than (seriesId|classId): extract scalar value to the matching Tier 1 column.
   - f. Inside `with conn.transaction()`:
        - `pg_advisory_xact_lock(instrument_id)` (I7 invariant).
        - **Soft-delete supersession (BLOCKING-3 fix):** `UPDATE fund_metadata_observations SET known_to = NOW() WHERE instrument_id = %s AND source_accession = %s AND known_to IS NULL` (marks previously-valid rows for this (instrument, accession) as superseded — happens on parser-version rewash; no-op on first ingest).
        - `INSERT INTO fund_metadata_observations (..., known_from, known_to) VALUES (..., NOW(), NULL)` — append-only; respects I6.
        - Call `refresh_fund_metadata_current(conn, instrument_id)` (write-through).
7. **Aggregate ParseOutcome**:
   - At least 1 class resolved + written → `parsed` (full or partial success).
   - Zero classes resolved, all reasons consistent → `tombstoned` (reason matches the unanimous miss-reason from §7.4).
   - Zero classes resolved, mixed transient + deterministic reasons → `failed_outcome` (defer to next retry; the next retry will tombstone if state hasn't changed).

**Source-priority gate inside `refresh_fund_metadata_current(conn, instrument_id)`:**

```python
def refresh_fund_metadata_current(conn, instrument_id: int) -> str:
    """Atomic write-through refresh.

    1. pg_advisory_xact_lock(instrument_id) — I7 invariant.
    2. SELECT the winning observation across all currently-valid rows.
    3. INSERT ... ON CONFLICT (instrument_id) DO UPDATE.

    Winning observation:
      SELECT *
      FROM fund_metadata_observations
      WHERE instrument_id = %(instrument_id)s
        AND known_to IS NULL                   -- exclude soft-deleted supersessions
      ORDER BY period_end DESC,
               filed_at DESC,
               source_accession DESC
      LIMIT 1

    Returns 'inserted' | 'updated' | 'suppressed'.
    """
```

Source-priority chain rationale (matches §2 settled-decision proposal):

- `period_end DESC` — most recent reporting period wins. N-CSR (annual) and N-CSRS (semi-annual) have disjoint period_end values per SEC rule, so they do not compete at the same period_end.
- `filed_at DESC` — at the same period_end, amendments (N-CSR/A, N-CSRS/A) naturally win because their filed_at is strictly later than the original.
- `source_accession DESC` — final deterministic tie-break for unlikely same-filed_at collisions (never observed in practice but the DB schema can't rule it out).
- `known_to IS NULL` filter — excludes rows marked superseded by a parser-version rewash.

**Fan-out posture** (vs sec_10k's share-class fan-out via `_resolve_siblings`): N-CSR fan-out is **per-classId not per-issuer-cik**. Each classId IS its own instrument. No sibling fan-out needed — classId resolution is already 1:1.

**ParseOutcome contract:**

| Outcome | Trigger | parser_version | next_retry |
|---|---|---|---|
| `parsed` | At least 1 class wrote an observation (full or partial success) | `n-csr-fund-metadata-v1` | — |
| `tombstoned` | Missing URL / empty fetch / unparseable iXBRL / zero in-universe classes | `n-csr-fund-metadata-v1` | — |
| `failed` | Transient fetch error / DB error classified as transient (`_classify.is_transient_upsert_error`) | `n-csr-fund-metadata-v1` | 1h |

**Raw-payload invariant (#938):** parser registers with `requires_raw_payload=False`. Worker accepts `parsed` with `raw_status=None`. Per operator choice + settled-decision § filing-event-storage.

## 9. Cadence + freshness wiring

- Layer 1 (Atom, 5-min): catches new accessions same-day (overkill but free; bundled in #1155).
- Layer 2 (daily-index 04:00 UTC): natural fit for semi-annual filings.
- Layer 3 (per-CIK hourly): belt-and-braces.
- `data_freshness._CADENCE['sec_n_csr']` already 200 days. Confirm; leave as-is.
- Form-to-source map: add `N-CSRS` + `N-CSRS/A` to `app/services/sec_manifest.py:_FORM_TO_SOURCE` (currently absent per spike §2.1).

## 10. Bootstrap (fund-scoped first-install drain)

- N-CSR is already excluded from issuer-scoped drain at `app/jobs/sec_first_install_drain.py:167` (correct — fund-scoped). Exclusion stays.
- **New fund-scoped pass**: walks SEC mutual-fund-trust CIK list (every distinct `trust_cik` from `company_tickers_mf.json` ingest) and drains the last 2 years per CIK (~4 accessions per fund × ~6k trusts = ~24k accessions).
- Pre-condition: `class_id_mapping_ready` capability (Stage 6 cik_refresh extension §7.2).
- Implementation: separate function `bootstrap_n_csr_drain()` in `app/jobs/sec_first_install_drain.py`; gated by a new bootstrap stage entry.

## 11. Read-side surfaces

### 11.1 Endpoints

| Path | Method | Returns | Auth |
|---|---|---|---|
| `/instruments/{symbol}/fund-metadata` | GET | `FundMetadataResponse` (current row + last_observed_at) | session-or-service-token |
| `/instruments/{symbol}/fund-metadata/history?since=date` | GET | `list[FundMetadataObservation]` | session-or-service-token |
| `/coverage/fund-metadata` | GET | `FundMetadataCoverageResponse` (per-source counts; resolver-miss; freshness) | service-token |

Pydantic response models:

```python
class FundMetadataResponse(BaseModel):
    instrument_id: int
    symbol: str
    class_id: str
    series_id: str | None
    document_type: Literal['N-CSR', 'N-CSR/A', 'N-CSRS', 'N-CSRS/A']
    period_end: date
    filed_at: datetime
    parser_version: str
    expense_ratio_pct: Decimal | None
    net_assets_amt: Decimal | None
    portfolio_turnover_pct: Decimal | None
    holdings_count: int | None
    returns_pct: dict[str, Decimal] | None
    sector_allocation: dict[str, Decimal] | None
    # ... mirror fund_metadata_current
    refreshed_at: datetime
```

### 11.2 Frontend (out of scope for this PR)

Spec lists the operator-visible surfaces (instrument detail chip, universe filter) but frontend consumption lands in a follow-up — separate UI ticket. This PR ensures the API exists + is correctly populated.

## 12. Smoke panel (operator-approved)

Per CLAUDE.md ETL DoD clauses 8-12:

| Instrument | Type | Trust CIK | Expected outcome |
|---|---|---|---|
| **VFIAX** | Vanguard 500 Index Admiral (mutual) | 36405 | Resolves; expense_ratio_pct ≈ 0.04%; tested against fund factsheet |
| **VOO** | Vanguard S&P 500 ETF | 36405 | Resolves; expense_ratio_pct ≈ 0.03% |
| **IVV** | iShares Core S&P 500 ETF | 1100663 | Resolves; expense_ratio_pct ≈ 0.03% |
| **AGG** | iShares Core US Aggregate Bond ETF | 1100663 | Resolves; bond-fund credit_quality_allocation populated |
| **FXAIX** | Fidelity 500 Index (mutual) | 819118 | **NOT in eToro universe → tombstone** with `instrument_not_in_universe` |

QQQ explicitly dropped from panel: UIT structure, files no N-CSR (spike §8.1: 0 in 249 submissions). Side-finding noted in `etl-endpoint-coverage.md`.

## 13. Tests

| Category | File | Cases |
|---|---|---|
| Per-concept extraction | `tests/test_n_csr_extraction.py` | One test per Tier 1 column against a golden iXBRL fixture per family (Vanguard / Fidelity / iShares) |
| Dimensional-axis recovery | `tests/test_n_csr_extraction.py` | `AvgAnnlRtrPct` per period (allowlist + unknown → raw_facts); `PctOfNav` per sector / region / credit axis |
| classId resolution | `tests/test_fund_class_resolver.py` | hit (external_identifiers row exists) / miss-pending-cik-refresh (transient — 24h backoff) / miss-not-in-universe (deterministic tombstone). **No symbol fallback** — symbol-only resolution is explicitly removed per §7.4; the test asserts a symbol-only match without an `external_identifiers` row does NOT resolve |
| Append-only supersession | `tests/test_n_csr_parser.py` | parser_version-bump rewash: prior row known_to set + new row inserted with known_from=NOW(), known_to=NULL. Partial unique index `WHERE known_to IS NULL` permits the supersession |
| Multi-series hard context filter | `tests/test_n_csr_extraction.py` | One accession with 5 classes across 2 series → 5 observations. Series-A's HoldingsCount/AssetsNet does NOT leak into Series-B's observation (context-tuple filter rejects cross-series facts) |
| Source-priority gate (filed_at) | `tests/test_refresh_fund_metadata_current.py` | Two observations same (instrument_id, period_end) — newer filed_at wins. Covers N-CSR/A vs N-CSR amendment shape. **Note**: N-CSR vs N-CSRS at the same period_end is structurally impossible per SEC rule §31a-29 so is NOT tested — annual + semi-annual cover disjoint periods |
| Source-priority tie-break (source_accession) | `tests/test_refresh_fund_metadata_current.py` | Same period_end + same filed_at (degenerate) → source_accession DESC wins deterministically |
| Resolver-miss partial success | `tests/test_n_csr_parser.py` | 5 classes, 2 miss with mixed reasons → 3 observations + per-miss log; outcome=parsed |
| Zero-resolution tombstone | `tests/test_n_csr_parser.py` | All classes miss with unanimous deterministic reason → outcome=tombstoned (reason matches unanimous miss); all miss with mixed reasons → outcome=failed (defer to next retry) |
| Sentinel-conn durability | `tests/test_n_csr_parser.py` | Monkeypatched store_raw + fetch — assert no DB writes outside fund_metadata_* tables (mirrors §11.5.1 pattern) |
| Endpoint shape | `tests/test_fund_metadata_endpoints.py` | All 3 endpoints with golden response fixtures |
| HTTPException on infra failure | `tests/test_fund_metadata_endpoints.py` | DB error → 503 with fixed-phrase detail (prevention-log #86) |
| Naive datetime coerce | `tests/test_fund_metadata_endpoints.py` | `since` query param without tzinfo → coerced to UTC (prevention-log #80) |

## 14. Cross-source verification plan (CLAUDE.md DoD clause 9)

For **VFIAX** (Vanguard 500 Index Admiral):
- Source 1: this parser's output for the most recent N-CSR.
- Source 2: Vanguard's published fund factsheet (`investor.vanguard.com/investment-products/mutual-funds/profile/vfiax`) — expense ratio, NAV, 1Y/5Y/10Y returns.
- Acceptable delta: NAV within 1% (period_end vs publish-date snapshot), expense ratio exact match, returns within 0.05% (rounding).
- PR description records the comparison table with figures + commit SHA.

## 15. Backfill plan (CLAUDE.md DoD clause 10)

Post-merge:

1. Verify `class_id_mapping_ready` capability is up (Stage 6 cik_refresh completion).
2. `POST /jobs/sec_rebuild/run` with body `{"source": "sec_n_csr"}` on dev DB.
3. Manifest worker drains rebuilt rows (~24k accessions × ~5 classes avg = ~120k observations).
4. Monitor `/jobs/sec_manifest_worker/status` until pending=0.
5. Spot-check 5 of the smoke panel + cross-source verify VFIAX.
6. PR description records: rebuild request body, drain completion time, observation count, smoke-check results.

## 16. Risks + invariants

- **TSR rule format may evolve.** Pin against the OEF taxonomy version observed in current filings (`xsd` reference in each filing folder). Add golden-file regression covering all 4 spike-sampled families (Vanguard A + Vanguard NCSRS + Fidelity + iShares).
- **Multi-series filings**: parser MUST isolate per-(seriesId, classId) before extracting facts. Test required.
- **`company_tickers_mf.json` staleness**: rows discovered before cik_refresh completes go to `class_id_unknown` retry (24h) → `instrument_not_in_universe` permanent. Surface in coverage endpoint.
- **N-CSR + N-CSRS overlap at half-year boundary**: source-priority rule resolves deterministically; explicit test required.
- **Mutual funds outside eToro universe**: tombstone `instrument_not_in_universe`, never silently drop. Coverage endpoint surfaces count.
- **AccmVal growth curve** time-order vs file-order. Test required.
- **Concurrent reach of refresh_fund_metadata_current**: I7 advisory lock + PK conflict guard.
- **Partition coverage**: period_end partitions exist for 2010-2030 (mirror sql/113 generator). If a filing has period_end outside that range (won't happen in v1 but worth a defensive check), tombstone with `period_out_of_range`.
- **JSONB size**: hard cap raw_facts at 32 KB; sector_allocation etc. typically <2 KB. Add assertion in serializer.
- **Spike doc §10 reaffirmation**: this PR does NOT ingest holdings from N-CSR; the spike's INFEASIBLE-CONFIRMED for that lane stands. Reviewer reading this PR must understand the lane separation.

## 17. References

- **Spike**: `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` (#918 closeout).
- **Synth no-op precursor**: PR #1170 (#918) — REPLACED by this PR.
- **Sibling real parser**: `app/services/manifest_parsers/sec_10k.py` (#1152) — pattern reference for share-class fan-out + filed_at gate + raw-payload invariant.
- **Sibling synth no-op**: `app/services/manifest_parsers/sec_10q.py` (#1168 / PR #1169) — pattern reference for the no-op-to-real-parser transition.
- **OEF taxonomy**: `https://xbrl.sec.gov/oef/` — authoritative concept inventory + dimensional axis definitions. Pin version against current TSR filings.
- **DEI taxonomy**: `https://xbrl.sec.gov/dei/` — EntityInvCompanyType + DocumentType.
- **SEC TSR rule**: Release 33-11125 (Oct 2022, effective 2024-07-24).
- **EDGAR Filer Manual Vol II §5**: OEF inline-XBRL filer guidance.
- **N-CSR instructions**: `https://www.sec.gov/files/formn-csr.pdf`.
- **`.claude/skills/data-sources/sec-edgar.md`** §11 + §11.5 (manifest-worker parser onboarding + synth no-op pattern).
- **`.claude/skills/data-engineer/SKILL.md`** §1.2 (two-layer observation model + I3 / I7 / I8 invariants).
- **`.claude/skills/metrics-analyst/SKILL.md`** (source → endpoint → chart pattern for operator-visible metrics).

---

## End of spec

Next steps after this doc is committed:

1. Codex pre-spec 1a review (`codex.cmd exec ... < /dev/null`).
2. Operator signoff on spec.
3. Implementation plan doc (`docs/superpowers/specs/2026-05-14-n-csr-fund-metadata-plan.md`).
4. Codex pre-spec 1b on plan.
5. Operator signoff on plan.
6. Branch + schema + parser + tests + docs + endpoints + bundled cik_refresh extension.
7. Self-review + local gates.
8. Codex pre-push 2.
9. Push + poll review + iterate to APPROVE + merge.
10. Post-merge: trigger sec_rebuild for sec_n_csr; verify VFIAX `/fund-metadata` endpoint.
