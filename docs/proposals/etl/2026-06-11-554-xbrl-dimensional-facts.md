# #554 — XBRL dimensional facts: segments, product mix, geographic revenue

Status: PROPOSAL — Codex ckpt-1 PASSED 2026-06-11 (3 HIGH / 4 MED / 1 LOW, all folded
in: per-route axis-set rule, per-member alias arbitration, per-(axis,metric) winner,
deterministic instance discovery, decimals + duplicate arbitration, annual-duration
definition, parser_version rewash path, axis-mapping test pin). Awaiting operator
sign-off.
Issue: #554 (carve-out of #551; #551 closed 2026-06-11 — headcount half shipped PR#555/888db1af)

## 1. Premise corrections (all empirically verified 2026-06-11)

The issue body says "extend SEC XBRL provider to emit dimensional facts". Four premises
needed correction before design:

1. **The companyfacts API carries NO dimensional facts.** Verified live: AAPL
   `CIK0000320193.json`, union of fact keys across all 503 concepts =
   `{accn, end, filed, form, fp, frame, fy, start, val}`. No axis/member fields exist
   at the source. Segment data cannot come from the current `sec_fundamentals`
   provider path at all — it requires per-filing XBRL instance parsing.
2. **#551's "5 Apple segments (iPhone, Mac, iPad, Wearables, Services)" are NOT
   business segments.** They are `srt:ProductOrServiceAxis` revenue disaggregation.
   AAPL's actual reportable segments (`us-gaap:StatementBusinessSegmentsAxis`) are
   geographic: Americas / Europe / Greater China / Japan / Rest of Asia Pacific.
   Verified on AAPL FY2025 10-K (accn 0000320193-25-000079): product axis sums to
   $416.16B total revenue ✓, segment axis sums to $416.16B ✓. We must ingest
   **three axes**, not one.
3. **`country_code` is the wrong column.** Geographic members mix ISO-backed qnames
   (`country:US`, `country:CN`) with filer-custom members
   (`aapl:OtherCountriesMember`). Store member qname + label, never force ISO.
4. **Per-segment `total_assets` is optional in the wild.** AAPL reports segment
   revenue + operating income but zero segment-asset facts (ASC 280 requires assets
   only if CODM-reviewed). Column nullable; not in acceptance.

Generality probe: MSFT FY2025 10-K segment revenue = Productivity $120.81B /
Intelligent Cloud $106.27B / More Personal Computing $54.65B ✓.

## 2. Settled decisions honoured

- **Fundamentals posture (#532)**: free regulated-source-only. The instance documents
  are EDGAR archive artifacts — same source, no new provider.
- **Process topology (#719) + manifest single-writer (#869)**: extraction runs in the
  manifest worker, not the API process.
- **Raw-payload scope-narrowing (#470, prevention log §547)**: every extracted field
  lands in SQL → instance XML is parse-and-drop, NOT retained in
  `filing_raw_documents`. (At ~1–5 MB × ~25k filings, retention would re-create the
  #1014 problem.)
- **Filing lookup rule**: CIK-keyed, accession-driven; no symbol lookups.
- **Prevention log §1455 (period sanity window)**: parser validates
  `[1900-01-01, 2100-01-01)` + `period_start <= period_end`, reject + WARN with
  provenance, mirror predicate in any cleanup tool.

## 3. Design

### D1 — extraction lives in the existing `sec_10k` manifest parser, version bump `10k-v2`

`_FORM_TO_SOURCE` is one-source-per-form and `10-K`/`10-K/A` already map to `sec_10k`
(Item 1 narrative, `app/services/manifest_parsers/sec_10k.py`). A second manifest
source per form is a discovery-plumbing change with no payoff. Segment extraction is a
second extraction step inside the same parser run, savepointed independently of the
Item 1 write so a segment-extraction failure does not tombstone the narrative (and
vice versa). Failure of the XBRL step alone → `status='failed'` (transient) only for
fetch errors; structurally-absent XBRL (pre-mandate filings, ~pre-2011) → zero rows,
parsed, no tombstone.

10-Q interim segments: **deferred** (conscious tradeoff — issue scope is "most recent
fiscal year"; quarterly adds a cadence-mixing problem for the UI with no operator ask).

### D2 — fetch through our throttle + lxml, NOT `edgartools.xbrl()`

edgartools' `filing.xbrl()` issues its own HTTP (instance + linkbases) outside
`app/providers/concurrent_fetch.py`'s shared 10 req/s throttle — the edgartools-skill
hard-stop for new paths. Instead:

1. Fetch the filing `index.json` (already-throttled wrapper).
2. Locate the extracted instance with deterministic priority (Codex ckpt-1 MED;
   detection rules CORRECTED after live verification — `index.json` carries NO
   SEC exhibit-type labels, its `type` field is a content-type icon, so the
   original EX-101.INS plan is impossible from the listing): (a) files ending
   `_htm.xml` (inline era) — several → prefer primary-document stem match, else
   largest-size + WARN; (b) else standalone-era `.xml` files that are not
   linkbases (`_cal/_def/_lab/_pre/_ref`), not `R<n>.xml`/`FilingSummary.xml`,
   not index files — same tie-break. Neither → no-XBRL skip. Each branch
   table-tested.
3. Locate label (`*_lab.xml`) + definition (`*_def.xml`) linkbases; when a filer
   ships no standalone linkbases (Workiva-style — verified on MSFT FY2025), fall
   back to the filing `.xsd`, which embeds labelLink/definitionLink in its
   annotation; the parsers match by localname so the xsd is a drop-in source.
   Labels absent entirely → prettified member localname fallback.
4. Parse with lxml via the shared `xbrl_instance` helpers (extracted from
   `n_csr_extractor.py`): `context_dimensions` (context → `{axis: member}`),
   wildcard-namespace matching, `(concept, axis)` routing. Hardened parser
   (entity resolution / DTD / network off) for all SEC-fetched XML.

Cost: 3–4 throttled fetches per accession (index + instance + lab + def, the
latter two sometimes one xsd). Backfill of ~25k 10-K accessions ≈ ≤100k fetches
≈ <3h at the shared 10 req/s floor.

### D3 — axes and metrics ingested

| Axis | Metrics | Notes |
| --- | --- | --- |
| `us-gaap:StatementBusinessSegmentsAxis` | revenue, operating_income, assets (nullable) | only contexts where `srt:ConsolidationItemsAxis` is absent or `us-gaap:OperatingSegmentsMember` — excludes eliminations/corporate reconciling items so member sums match consolidated totals |
| `srt:ProductOrServiceAxis` | revenue | the #551 acceptance surface |
| `srt:StatementGeographicalAxis` | revenue | member qname + label, no ISO coercion |

Axis-set rule is per route, exact (Codex ckpt-1 HIGH — the earlier "any other axis
excluded" phrasing contradicted the ConsolidationItemsAxis allowance):

- `business_segment` route accepts contexts whose axis set is exactly
  `{StatementBusinessSegmentsAxis}` or
  `{StatementBusinessSegmentsAxis, ConsolidationItemsAxis=OperatingSegmentsMember}`.
- `product_service` route: exactly `{ProductOrServiceAxis}`.
- `geographic` route: exactly `{StatementGeographicalAxis}`.

Anything else (segment×product cross-dimensions, eliminations members, etc.) is
excluded — double-counting guard.

Revenue concept resolution reuses the existing alias tuple
(`sec_fundamentals.py:224` — `RevenueFromContractWithCustomerExcludingAssessedTax`,
`Revenues`, `SalesRevenueNet`, `RevenueFromContractWithCustomerIncludingAssessedTax`).
Arbitration is per `(axis, member, period)`, not one-concept-per-axis (Codex ckpt-1
HIGH — filers can mix concepts across members): members are unioned across aliases;
where the same member+period has facts under several aliases, the highest-priority
alias wins. Filer-extension revenue concepts are out of scope v1 (conscious
undercount caveat; `pct_of_total` is computed over returned rows so tables stay
internally consistent). Operating income = `us-gaap:OperatingIncomeLoss`; assets =
`us-gaap:Assets`.

Duplicate-fact arbitration (Codex ckpt-1 MED): when the instance repeats the same
(concept, context, unit), keep the most precise (`decimals`, with `INF` highest);
equal precision with differing values → drop that member + WARN with provenance.

**Subtotal members (added after live verification — the issue/spec originally
missed this):** member sets are NOT flat. AAPL's product axis carries
`us-gaap:ProductMember` ($307.0B) as the parent subtotal of iPhone/Mac/iPad/
Wearables; summing returned rows would double-count (723B vs 416B). Rows carry
`is_subtotal`, marked by TWO detectors: (a) definition-linkbase domain-member
nesting (filers who nest; both AAPL linkbases are FLAT, so insufficient alone);
(b) value-overage — revenue on product/geographic axes only, where ASC 606
disaggregation reconciles to the consolidated (dimensionless) revenue fact in
the same instance: `sum(members) − consolidated` is exactly the subtotal mass;
the smallest member subset (≤3) summing exactly to it is marked; ambiguity or
no-match marks nothing + WARNs. business_segment is excluded from (b) — its sum
legitimately differs from consolidated (unallocated corporate items filtered via
ConsolidationItemsAxis). Verified: AAPL Product; MSFT Product + Service,Other
(xsd-embedded linkbases); JPM 'Total International' (= EMEA+APAC+LatAm exactly).
Readers exclude subtotals; leaf sums reconcile (AAPL 416.161B on both axes).

Financials-sector caveat: banks (JPM) may emit none of the revenue aliases on these
axes → endpoint returns empty state, acceptable v1, recorded in smoke results.

### D4 — schema: ONE raw table, latest-filed-wins reader

Issue proposed two UI-shaped tables. Three axes × shared identity/provenance columns →
one table, axis-discriminated, mirroring `financial_facts_raw` semantics
(immutable per-accession rows; reader dedupes):

```sql
CREATE TABLE instrument_dimensional_facts (
    fact_id          BIGSERIAL PRIMARY KEY,
    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id),
    axis             TEXT NOT NULL CHECK (axis IN
                       ('business_segment', 'product_service', 'geographic')),
    member_qname     TEXT NOT NULL,          -- e.g. 'aapl:IPhoneMember', 'country:US'
    member_label     TEXT NOT NULL,          -- lab.xml standard label, localname fallback
    metric           TEXT NOT NULL CHECK (metric IN
                       ('revenue', 'operating_income', 'assets')),
    unit             TEXT NOT NULL,
    period_start     DATE,
    period_end       DATE NOT NULL,
    val              NUMERIC(30,6) NOT NULL,
    decimals         TEXT,                   -- XBRL precision incl. 'INF' (duplicate arbitration input)
    source_accession TEXT NOT NULL,
    form_type        TEXT NOT NULL,
    filed_at         TIMESTAMPTZ NOT NULL,
    parser_version   TEXT NOT NULL,          -- audit + rewash correction path
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX uq_dimensional_facts_identity
    ON instrument_dimensional_facts(
        instrument_id, axis, member_qname, metric,
        COALESCE(period_start, '0001-01-01'::date), period_end, source_accession);
CREATE INDEX idx_dimensional_facts_read
    ON instrument_dimensional_facts(instrument_id, axis, period_end DESC);
```

Restatements: each 10-K carries ~3 fiscal years of comparatives; 10-K/A supersedes.
Reader selects the winning accession per `(instrument, axis, metric)` (Codex ckpt-1
HIGH — a 10-K/A can carry revenue rows but omit op-income/assets; per-axis selection
would regress the omitted metric to empty) =
`ORDER BY filed_at DESC, source_accession DESC LIMIT 1` over accessions having rows
for that (axis, metric). Rows join by member for the table; the response reports the
winning accession per metric (`sources: {revenue: accn, …}`).

"Latest FY" is defined per metric kind (Codex ckpt-1 MED — max `period_end` alone can
pick a YTD/quarterly context): duration metrics (revenue, operating_income) take the
winning accession's max `period_end` among durations of 330–400 days; the instant
metric (assets) takes the instant context at exactly that `period_end`.

Re-parse correction path (Codex ckpt-1 MED): rows are immutable per accession in
normal operation, but a rewash of an accession DELETEs that accession's rows and
re-inserts inside one transaction, stamping the new `parser_version` — same semantics
as the manifest rewash contract elsewhere; no `known_to` machinery needed.

Not partitioned: ~25 rows/filing × ~25k filings ≈ 625k rows. Revisit only if 10-Q lands.

### D5 — share-class fan-out

10-K is issuer-level; fan out per `siblings_for_issuer_cik` exactly like the Item 1
narrative writer (GOOG + GOOGL both render).

### D6 — API

One endpoint, axis-parameterised (not two):

`GET /instruments/{symbol}/segments?axis=business|product|geographic`
→ `{ axis, period_end, sources: {revenue: accn, operating_income?: accn,
assets?: accn}, filed_at, rows: [{member_label, member_qname, revenue,
operating_income?, assets?, pct_of_total}], total }`

(`sources` is a per-metric map, NOT a single `source_accession` — the §D4 reader
selects winners per (axis, metric), so a 10-K/A revenue row can legitimately pair
with original-10-K op-income rows. `filed_at` = max over winning accessions.)

`pct_of_total` computed over the returned member rows (not consolidated revenue) so
the table is internally consistent even when eliminations exist.

The `axis` query values map to storage enum values
(`business→business_segment`, `product→product_service`, `geographic→geographic`);
the mapping is pinned in an API test so storage enum values never leak (Codex
ckpt-1 LOW).

### D7 — frontend

Instrument page, beside `FundamentalsPane`: `SegmentsTable` (business/product axis
toggle, revenue + op income + % columns) and `GeographicMixChart` (horizontal bars,
shares palette conventions with existing panes). Loading/error/empty per
`.claude/skills/frontend/loading-error-empty-states.md`. Empty state copy covers the
financials-sector caveat ("no segment disclosure in this filing").

## 4. Backfill + verification plan (DoD clauses 8–12)

1. Parser version `10k-v1` → `10k-v2`; rebuild via
   `POST /jobs/sec_rebuild/run {"source": "sec_10k"}`. Conscious cost: rewash re-runs
   the Item 1 narrative parse too (idempotent, filed_at-gated).
2. Drain monitored via manifest pending count for `sec_10k`.
3. Smoke panel: AAPL, MSFT, GME, JPM, HD via
   `GET /instruments/{symbol}/segments` ×3 axes. Record figures in PR.
4. Cross-source: AAPL product-axis revenue vs the 10-K as filed on EDGAR
   (already captured above: iPhone $209.586B / Mac $33.708B / iPad $28.023B /
   Wearables $35.686B / Services $109.158B, FY2025) + one independent source
   (e.g. macrotrends/stockanalysis segment page).
5. Operator-visible: SegmentsTable + GeographicMixChart render on dev for the panel.

## 5. Out of scope (conscious)

- 10-Q interim segments (cadence mixing; no ask).
- Segment history/time-series UI (one latest-FY snapshot per the issue; raw table
  retains history for a future chart).
- ISO-normalised country rollups (member qname is the honest grain).
- Headcount (#551, shipped).

## 6. Effort

M — schema (1 migration) + parser step (reuse n_csr machinery) + 1 endpoint + 2 FE
components + tests (pure-logic parser table-tests; ONE db-tier test for the
latest-accession-wins reader SQL).

## 7. Implementation plan (operator-approved 2026-06-11; single PR)

Order per repo output preference: schema → service → tests → integration glue.

- **T1 — sql/193_instrument_dimensional_facts.sql.** Table per §D4 (axis/metric
  CHECKs, identity unique index, read index). No partitioning, no backfill DML.
  Append the table to the `tests/fixtures/ebull_test_db.py` teardown list in the
  same PR (fixture contract for new FK-child tables — Codex plan-review).
- **T2a — `app/services/dimensional_facts.py`** (PURE — parse + discovery only,
  zero DB imports; Codex plan-review split):
  - `discover_xbrl_files(index_json) -> XbrlFileRefs | None` — §D2 deterministic
    priority (EX-101.INS → `*_htm.xml` stem-match → lexicographic + WARN).
  - `extract_dimensional_facts(instance_xml, lab_xml | None) -> list[DimensionalFact]`
    — lxml, context→axis-set per-route exact match (§D3), revenue alias arbitration
    per (axis, member, period), duplicate-fact precision arbitration, sanity window
    `[1900-01-01, 2100-01-01)` + `period_start <= period_end` (prevention log §1455),
    member label from lab.xml standard role, localname-prettify fallback.
- **T2b — `app/services/dimensional_facts_store.py`** (DB layer):
  `replace_accession_rows(conn, instrument_id, accession, rows)` — delete-then-insert
  in caller's tx, stamps `parser_version`; reader
  `read_segments(conn, instrument_id, axis)` — winner per (axis, metric) +
  annual-duration filter per §D4.
- **T3 — `manifest_parsers/sec_10k.py` step 2:** after Item 1 upsert, savepointed
  XBRL fetch (our throttled wrapper: index.json + instance + lab) + extract + write,
  fanned out per `siblings_for_issuer_cik`. Bump `_PARSER_VERSION_10K = "10k-v2"`.
  Failure semantics per §D1: fetch error → transient `failed` (the whole manifest
  row retries; retry is idempotent — Item 1 upsert is filed_at-gate-suppressed,
  segment write is delete-then-insert — so a narrative may be visible before
  segments land on the retry; intended); no-XBRL → parsed, zero rows; instance
  parse error → WARN + parsed-without-segments (narrative must not regress on
  segment bugs) with error recorded in parse log detail.
  **10-K/A interaction (Codex plan-review):** the dimensional step ALWAYS targets
  `row.accession_number`'s own XBRL — never the Item-1 fallback's
  `chosen_accession`/`chosen_html` (Item 1 may fall back to the prior plain 10-K
  when an amendment lacks Item 1; segments must not be attributed to the wrong
  accession). Amendment without XBRL → zero rows; the prior accession's rows
  remain and the per-metric winner reader serves them. Explicit test case.
- **T4 — reader + API:** winner-per-(axis,metric) SQL + annual-duration filter in
  `app/services/dimensional_facts.py`; `GET /instruments/{symbol}/segments?axis=…`
  in `app/api/instruments.py` next to the #551 employees endpoint. Response per §D6.
- **T5 — tests (lean):** table-tests on `extract_dimensional_facts` +
  `discover_xbrl_files` with trimmed AAPL/MSFT instance fixtures (axis-set routing,
  alias arbitration, duplicate precision, sanity window, label fallback); ONE db-tier
  test for winner-per-(axis,metric) + annual-duration reader SQL; API test pinning
  axis param→enum mapping + 404/empty states.
- **T6 — frontend:** `fetchInstrumentSegments` in `api/instruments.ts`;
  `SegmentsTable` (business/product toggle) + `GeographicMixChart`; wire into
  instrument page near `FundamentalsPane`; vitest unit tests for states + pct
  rendering. Frontend skills re-read at this step.
- **T7 — gates + Codex ckpt-2 + push + open PR** (pre-pr-fresh-agent-review
  mandatory — filings-ETL change). PR opens with implementation description;
  DoD 8–12 evidence section added by T8 BEFORE merge (Codex plan-review ordering
  fix — verification evidence cannot precede the verification).
- **T8 — dev backfill + verify, pre-merge:** restart dev jobs proc onto the branch
  (operator-approved relaunch method), `POST /jobs/sec_rebuild/run
  {"source":"sec_10k"}`, drain watch, smoke panel AAPL/MSFT/GME/JPM/HD ×3 axes,
  cross-source AAPL product split (already EDGAR-confirmed §1), operator-visible FE
  check. Update PR body with figures + commit SHA, then merge gate applies.

Dependencies: T2b→T1+T2a, T3→T2a/T2b, T4→T2b, T5→T2a/T2b/T4, T6→T4, T8→T7.
