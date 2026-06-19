# DERA FSDS bulk dimensional-facts loader (#1590)

Bulk-first initial load for dimensional XBRL facts (segment / product / geographic
revenue, opinc, assets). Quick-and-dirty tier: ~minutes for the first load vs the
~4 h per-filing drain; the precise #554 per-filing path converges on top.

Refs #554 (per-filing path + schema, merged e0e496b4), #1623 (FSDS plumbing this
clones, merged 8f52bfad).

## Operator directive

Backfill gaps like #554's must be lined up in the initial loads — a bootstrap
stage, not a manual multi-hour drain. Quick-and-dirty data in <10 min beats hours
of precise per-filing fetching for the FIRST load; the precise path converges.

## Source rule (verified empirically, 2025q1, 2026-06-18)

SEC DERA **Financial Statement Data Sets** quarterly ZIPs. The public `aqfs.pdf`
is STALE (shows no `segments` column, scopes to "primary statements only"). The
authoritative current rule is the **in-zip `readme.htm`**: *"segments — XBRL tags
used to represent axis and member reporting"*, now part of NUM's unique key. Live
`num.txt` header (verified): `adsh⇥tag⇥version⇥ddate⇥qtrs⇥uom⇥segments⇥coreg⇥value⇥footnote`.

**`segments` encoding** (verified on real 2025q1 cells): `Axis=Member;Axis2=Member2;`
— `;`-delimited, `=`-separated, **namespace stripped, axis = localname minus the
`Statement` prefix and `Axis` suffix, member = localname minus the `Member` suffix.**
Real cells: `ClassOfStock=CommonClassA;`, `BusinessSegments=SpecialtyDiagnostics;ConsolidationItems=OperatingSegments;`,
`ProductOrService=ExtendedWarranty;`, `Geographical=US;`.

**Caveat on the source rule's scope:** `readme.htm` documents only the column's
PURPOSE ("XBRL tags used to represent axis and member reporting"). The exact
encoding above (ns-strip, `Statement`/`Axis`/`Member` affix-strip, `;`/`=`
delimiters) is **empirically reverse-engineered from real cells, not formally
specified by SEC** — but it is the same encoding #1623's shipped
`parse_class_member` already relies on (`ClassOfStock=CommonClassA;`), so the
treatment is settled in-repo, not freshly guessed. The classifier therefore
matches a CLOSED set of known axis tokens and rejects everything else (fail safe),
rather than trusting the strip to be lossless for unknown tokens.

This is why the bulk path **cannot reuse #554's `_classify_context`** — that matches
FULL localnames (`StatementBusinessSegmentsAxis`, …). The bulk classifier needs its
own FSDS-token→route map (below), keeping #554's **exact-set** rule.

## Full-population verification (2025q1 num.txt — whole quarter, not a sample)

Clean exact-set route rows (pre us-gaap-concept filter — an upper bound):

| route | rows | distinct accessions |
| --- | --- | --- |
| business_segment | 128,080 | 2,765 |
| product_service | 38,986 | 2,679 |
| geographic | 27,502 | 1,948 |

~2k–2.8k issuers/quarter gain clean data via bulk, vs the ~4 h per-filing drain —
confirms the bulk-first premise at scale.

**Scope of this scan (Codex ckpt-1 MED):** this is a FULL scan of ONE quarter (2025q1
— every num.txt row, not a within-quarter sample), so the yields are exact for that
quarter; it does NOT prove the route-token set is complete across all quarters.

### Calibration results (2025q1, post all production filters — one-off scan, 2026-06-19)

Applying the REAL filters (10-K/10-K/A form + us-gaap concept + per-route metric) over
the actual num.txt (3.66M rows, 6,231 filings, 4,235 10-Ks):

| route | rows | accessions | CIKs |
| --- | --- | --- | --- |
| business_segment | 25,705 | 1,975 | 1,965 |
| product_service | 18,705 | 1,713 | 1,697 |
| geographic | 12,815 | 1,128 | 1,124 |

- **Tradable-universe intersection: 2,302 of our instruments gain data from this ONE
  quarter** (2,770 routed CIKs ∩ 5,261 universe CIKs) — confirms the bulk-first premise:
  ~2.3k instruments in one <10-min load vs the ~4 h per-filing drain.
- **Encoding verified** on real cells (samples match the affix-stripped form exactly).
- **Exact-set rule confirmed safe**: the dominant rejected cells are precisely the
  cross-dimensional ones that would double-count — `{BusinessSegments,ProductOrService}`
  (10,973), `{BusinessSegments,ConsolidationItems,ProductOrService}` (9,432),
  `{BusinessSegments,Geographical}` (4,202). `{BusinessSegments,ConsolidationItems}` with
  a non-`OperatingSegments` member (3,272, e.g. eliminations/corporate) is correctly
  excluded → the member constraint is load-bearing.
- **Truncation negligible**: 16 suspect cells in the whole quarter; the classifier
  rejects malformed cells (fail-safe). No un-stripped or missed single-axis token variant
  of the 3 routes was observed.

## Reuse map (grounded against live code)

| Concern | Reuse | Source |
| --- | --- | --- |
| Download FSDS zip | `build_bulk_archive_inventory` (already lists `fsds_{q}.zip`, 4 quarters, newest optional) | `sec_bulk_download.py:255,300-313` |
| Stream `sub.txt` | `read_fsds_sub(zf) → {adsh: FsdsSub(cik, period, form, filed)}` | `fsds_class_shares.py:144` |
| Stream `num.txt` | `iter_fsds_num(zf) → Iterator[dict[header→str]]` (latin-1, manual tab-split, header-keyed) | `fsds_class_shares.py:175` |
| Fact shape | `DimensionalFact(axis, member_qname, member_label, metric, unit, is_subtotal, period_start, period_end, val, decimals)` | `dimensional_facts.py:100` |
| Concept→metric map | `_CONCEPT_TO_METRIC` (revenue aliases, OperatingIncomeLoss, Assets) + per-route metric filter + `fasb.org/us-gaap` namespace gate | `dimensional_facts.py:64,75` |
| Subtotal (product/geo) | shared value-overage helper (EXTRACTED, see below) | `dimensional_facts.py:644-694` |
| CIK→instruments fan-out | `siblings_for_issuer_cik(conn, cik) → list[int]` | `sec_identity.py:26` |
| Bootstrap stage shape | clone `sec_fsds_class_shares_ingest` (S14, `db` lane) | `bootstrap_orchestrator.py:1110`, `sec_bulk_orchestrator_jobs.py:873` |
| Target table | `instrument_dimensional_facts` (sql/193) — **no schema change** | `sql/193` |

## Design

New module `app/services/fsds_dimensional_facts.py`. `PARSER_VERSION = "fsds_dimensional_v1"`.

### 1. Bulk classifier (own FSDS-token map, exact-set rule)

`_classify_fsds_segments(segments_cell: str) -> tuple[DimensionalAxis, str] | None`.
Parse the cell into a set of `axis → member` localname pairs. Match the **exact axis
set** (reject any cell carrying an axis outside the route's set → cross-dimensional
rows must not be routed):

| axis-token set | member constraint | route |
| --- | --- | --- |
| `{BusinessSegments}` | — | business_segment |
| `{BusinessSegments, ConsolidationItems}` | `ConsolidationItems` member == `OperatingSegments` | business_segment |
| `{ProductOrService}` | — | product_service |
| `{Geographical}` | — | geographic |

Token match is **exact** (`Geographical` ≠ `GeographicalAreas`; `BusinessSegments` ≠
`BusinessSegmentsAreas`) — not substring. Member returned = the routed axis's member
localname (for `{BusinessSegments, ConsolidationItems}`, the `BusinessSegments`
member). Empirically-rejected multi-axis cells: `ConcentrationRiskByBenchmark=…;…;Geographical=US;`,
`BusinessSegments=MedicalOffice;Geographical=HoustonTexas;`, `LegalEntity=…;ProductOrService=…;`,
`ConsolidationItems=OperatingSegments;ProductOrService=…;`.

**Parse to an ordered LIST of `(axis, member)` pairs, not a dict** (Codex ckpt-1 MED).
A repeated axis token (`Geographical=US;Geographical=Canada;`) must REJECT (→ None), never
silently collapse to one member — assert `len(pairs) == len({axis for axis,_ in pairs})`
before the exact-set match, and require the routed axis to carry exactly one member.

### 2. Metric + namespace filter

Reuse #554's `_CONCEPT_TO_METRIC`: keep only num.txt rows whose `tag` (us-gaap
concept) maps to a tracked metric AND whose `version` namespace is `us-gaap`
(num.txt `version` = `us-gaap/2024` etc.; reject filer-extension tags — v1 undercount,
matches #554). Apply the per-route metric filter: business_segment ⊇
{revenue, operating_income, assets}; product_service, geographic = {revenue} only.

### 2b. Form filter — bulk is 10-K only

FSDS includes 10-Q rows; a later-filed 10-Q can carry a `qtrs=4` (FY-comparative or
TTM) annual-duration member that would WIN the reader (`_METRIC_ROWS_SQL` orders by
`filed_at DESC` and filters annual-duration but NOT form) and outrank the real 10-K —
diverging from #554's 10-K semantics (Codex ckpt-1 HIGH). **Process only accessions whose
`sub.txt` `form ∈ {'10-K', '10-K/A'}`** (`read_fsds_sub` already returns `form`); skip all
others. This matches the #554 per-filing path (the ~3,175 rows it drains are 10-Ks) so
bulk and per-filing rows are the same annual grain and converge cleanly.

### 3. Period reconstruction

num.txt `ddate` (yyyymmdd period-end) + `qtrs` (duration in quarters; `0` = instant):
instant (assets, qtrs=0) → `period_start=NULL, period_end=ddate`; duration →
`period_end=ddate`, `period_start = ddate − qtrs×3 months`. The reader's annual-duration
filter (`period_end − period_start BETWEEN 330 AND 400`) then keeps only FY rows, so
qtrs≠4 rows are written but won't render in the segments read — consistent with #554.

### 4. Per-accession buffering + value-overage

num.txt is one flat file, not issuer-sorted. Buffer routed member rows AND the
dimensionless consolidated rows (`segments=''`, same tracked tag) **per accession**
(keyed by adsh) while streaming — bounded by the routed subset (~195k rows/quarter,
tens of MB). After the stream, per accession:

1. Build `DimensionalFact` members + a `totals` dict
   `{(metric, period_start, period_end): Decimal}` from the dimensionless rows.
2. Mark subtotals via the **shared value-overage helper** (product/geo revenue only;
   business_segment excluded exactly as #554 does — unallocated corporate items make
   the segment member-sum legitimately ≠ consolidated). business_segment ships with
   `is_subtotal=False` (quick-tier caveat: def-linkbase subtotals unavailable in bulk;
   the per-filing rewash marks them on contact).

`member_qname` = the FSDS member **localname** (e.g. `US`, `SpecialtyDiagnostics`) —
NOT the honest namespaced qname (`country:US`) the per-filing path stores; FSDS strips
namespaces. `member_label` = prettified localname (split camelCase). This divergence is
load-bearing for convergence (next section).

### Shared value-overage helper (behavior-preserving extraction from #554)

Extract `dimensional_facts.py:644-694` into a pure
`mark_value_overage_subtotals(facts, totals, *, accession) -> tuple[list[DimensionalFact], dict[str, int]]`
where `totals: Mapping[tuple[DimensionalMetric, date|None, date], Decimal]`. It returns the
marked facts AND its rejection counter (`subtotal_set_ambiguous`,
`subtotal_overage_unresolved`, `no_consolidated_revenue_anchor`) and keeps the
accession-bearing WARN logging verbatim (Codex ckpt-1 MED — preserve telemetry). The
per-filing caller merges the returned counts into its existing `rejections` dict before
its summary WARN (so the #554 log output is byte-identical); it passes
`{k: v[2] for k, v in totals.items()}` (its `totals` values are `(priority, rank, Decimal)`
tuples → `v[2]` is the Decimal, matching today's `anchor[2]`). The bulk path calls the same
helper. #554's existing value-overage tests pin the per-filing behavior → the refactor is
safe iff they stay green. ONE owner of the value-overage rule (DRY; CLAUDE.md).

### 5. Convergence — the critical correctness rule

The reader (`dimensional_facts_store._METRIC_ROWS_SQL`) is **accession-winner per
(instrument, axis, metric)**: `ORDER BY filed_at DESC, source_accession DESC LIMIT 1`.
The identity unique index includes `member_qname`. FSDS `US` ≠ per-filing `country:US`,
so for the SAME accession a bulk INSERT does **not** conflict with per-filing rows →
naive append would DOUBLE every member if that accession later wins the reader.

**Rule: the bulk loader writes an accession's rows only when NO `instrument_dimensional_facts`
row already exists for `(instrument_id, source_accession)`** — never augment/overwrite a
per-filing accession. NOT `ON CONFLICT DO NOTHING` (the differing `member_qname` doesn't
conflict, so DO NOTHING still appends).

**The guard must be race-tight** (Codex ckpt-1 HIGH). A bare `SELECT exists → INSERT`
inside one txn does NOT serialize against a concurrent per-filing
`replace_accession_rows` (`dimensional_facts_store.py:54`) — and they genuinely race:
the bootstrap bulk stage runs while the per-filing manifest worker is draining the same
10-Ks. Interleave (bulk checks "absent" → per-filing inserts `country:US` → bulk inserts
`US`) leaves both → double-count. Fix: **both writers take a transaction-scoped Postgres
advisory lock keyed on `(instrument_id, accession)` before their check/replace** —
`SELECT pg_advisory_xact_lock(%(instrument_id)s::bigint, hashtext(%(accession)s))` at the
top of the bulk write txn AND at the top of `replace_accession_rows`. The lock serializes
the two writers on the same accession; the check-then-act becomes atomic. (Adding the lock
to `replace_accession_rows` is a 1-line, behavior-preserving change pinned by #554's
existing writer tests.)

With the lock, bulk is:
- **bulk-after-per-filing**: skipped (per-filing rows present) — no double-count. ✓
- **per-filing-after-bulk**: per-filing `replace_accession_rows` DELETEs by
  `(instrument_id, source_accession)` then re-inserts → replaces bulk rows. ✓
- **bulk-after-bulk** (re-run / overlapping quarters): skipped (idempotent). ✓
- **concurrent bulk ⇄ per-filing**: serialized by the shared advisory lock — whichever
  commits first owns the accession; the other sees its rows and skips/replaces. ✓

### 6. Writer

`_write_bulk_accession(conn, *, instrument_id, accession, form_type, filed_at, facts)`:
inside a `with conn.transaction():` savepoint (prevention-log "clear-then-insert must be
atomic" — here existence-check-then-insert, still wrapped so a mid-`executemany` failure
rolls back cleanly): (1) `SELECT pg_advisory_xact_lock(%(instrument_id)s::bigint,
hashtext(%(accession)s))` — the shared lock that serializes against per-filing
`replace_accession_rows` (§5); (2) `SELECT 1 FROM instrument_dimensional_facts WHERE
instrument_id=%s AND source_accession=%s LIMIT 1` — if present, return 0 (skip); (3) else
`executemany` INSERT with `parser_version = "fsds_dimensional_v1"`. Fan-out: one such call
per sibling from `siblings_for_issuer_cik` (each sibling locks its own
`(instrument_id, accession)`). `replace_accession_rows` gets the SAME
`pg_advisory_xact_lock(instrument_id, hashtext(accession))` as its first statement.

### 7. Bootstrap stage + job

- Job constant `JOB_SEC_FSDS_DIMENSIONAL_INGEST = "sec_fsds_dimensional_ingest"` +
  zero-arg invoker `sec_fsds_dimensional_ingest_job()` in `sec_bulk_orchestrator_jobs.py`,
  cloning `sec_fsds_class_shares_ingest_job`: list cached `fsds_*.zip`, per-archive
  `ingest_fsds_dimensional_archive(conn, archive_path, fsds_qtr)`, commit per archive,
  audit row in bootstrap.
- **Archive retention (Codex ckpt-1 HIGH):** today `sec_fsds_class_shares_ingest_job`
  deletes every `fsds_*.zip` on success (`sec_bulk_orchestrator_jobs.py:966,
  _delete_archive_after_success`) — and bootstrap stage ORDER is not scheduling order, so
  it can delete a zip the dimensional stage still needs. Both stages now consume the same
  zips, so **neither deletes**: REMOVE the `_delete_archive_after_success` loop from the
  class-shares job, and the dimensional job does not add one. The 4 `fsds_{q}.zip`
  (~240 MB) then persist in the bulk cache exactly as `companyfacts.zip` (1.4 GB) and the
  `nport_*.zip` (~900 MB) already do; filenames are deterministic per quarter so re-runs
  overwrite, never accumulate. (A uniform terminal retention sweep for ALL bulk zips is a
  separate concern, out of scope.) This makes the two consumers order-independent.
- Stage `_spec("sec_fsds_dimensional_ingest", <next order>, "db", JOB_SEC_FSDS_DIMENSIONAL_INGEST)`
  in `bootstrap_orchestrator.py`. CapRequirement = `bulk_archives_ready` + the capability
  that guarantees instruments + `sec` CIK `external_identifiers` exist (the CIK fan-out
  needs them; confirm which cap provides it — likely the same universe/identity cap the
  class-shares stage's `cusip_mapping_ready` implies, but dimensional needs NO CUSIP map).
  Provides a status-only `fsds_dimensional_ingested` capability (not a strict-floor gate —
  partial coverage is valid quick-tier state).

## Schema

**No migration.** Reuse `instrument_dimensional_facts` (sql/193). `member_qname` holds
the FSDS localname for bulk rows; `decimals` = NULL (num.txt carries no decimals column);
`unit` = num.txt `uom`. All other columns map directly.

## Tests

- Pure (`_classify_fsds_segments`): each route's exact set → route; extra-axis cells →
  None (the 4 empirical rejects); `Geographical` vs `GeographicalAreas` token-exactness;
  `{BusinessSegments, ConsolidationItems=OperatingSegments}` → business_segment but
  `{BusinessSegments, ConsolidationItems=Eliminations}` → None.
- Pure (period reconstruction): instant qtrs=0 → start NULL; qtrs=4 → start = end−12mo.
- Pure (value-overage parity): the extracted helper, fed #554's AAPL Products fixture,
  marks the same subtotal — assert byte-identical to the pre-extraction behavior
  (the existing #554 test stays green = the proof).
- Pure (member_label prettify): `SpecialtyDiagnostics` → `Specialty Diagnostics`.
- DB (`-m db`): convergence guard — bulk write skips an accession that already has a
  per-filing row (assert row count unchanged, no doubled members); bulk writes a
  fresh accession; a later per-filing `replace_accession_rows` replaces bulk rows.
- DB: CIK fan-out — a 2-sibling CIK gets both instruments' rows.

## DoD (ETL clauses 8-12)

8. **Smoke 3-5 instruments** (dev DB) after backfill: dimensional segments render for
   a known multi-segment filer panel (e.g. AAPL product/geo, a segment-reporting
   industrial). Record figures + accession provenance.
9. **Cross-source** one fixture: AAPL product revenue (iPhone/Mac/…) bulk-loaded figure
   vs the 10-K / a reputable source.
10. **Backfill executed**: download ≥1 FSDS quarter zip, run the ingest job/stage on dev,
    record rows written + accessions skipped (convergence guard).
11. **Live endpoint**: hit the segments read endpoint for the smoke panel; confirm the
    pie/table renders with bulk-sourced members + that a per-filing-covered instrument
    shows per-filing (honest qname) rows, not doubled.
12. PR records the commit SHA for each of 8-11.

## Out of scope / quick-tier caveats (accepted)

- No label linkbase → bulk `member_label` = prettified localname until per-filing touches.
- business_segment subtotals unmarked in bulk (no linkbase; value-overage excluded for
  the segment axis by #554 design) — per-filing marks them on convergence.
- `segments` truncation may make some multi-axis contexts lossy → exact-set rejector is
  conservative (fails toward dropping, never mis-routing a known route).
- Steady-state go-forward is the #554 per-filing manifest worker (unchanged). Bulk is a
  bootstrap/first-load accelerator + a manual re-trigger; no recurring schedule.
