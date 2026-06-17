# Per-class shares-outstanding denominator (FSDS) — retires the #1646 dual-class caveat

Issue: follow-up under epic #788 (ownership DQ audit). Supersedes the honest-degradation
caveat shipped in #1646 with a **real per-class denominator** sourced from the SEC DERA
Financial Statement Data Sets (FSDS). When a verified per-class share count exists for an
instrument, the rollup divides by it (GOOGL 20.97% → ~43.8%); when one does not, the
existing #1646 combined-basis caveat is preserved unchanged. **Fail-closed: never fabricate
a per-class denominator.**

This is **Option A** of the 2026-06-17 3-lens committee verdict (data-eng / architect /
filings-analyst): a minimal, standalone per-class FSDS ingest. It deliberately does **NOT**
bundle #1590 business-segment/product/geo dimensional facts — those are a throughput problem
with an existing per-filing extractor (`dimensional_facts.py`) and divergent routing
(axis-member → label, not class → instrument). Bundling would be premature abstraction.

---

## 1. Problem

A multi-class issuer whose classes share one SEC CIK (GOOG/GOOGL, HEI/HEI.A, METC/METCB)
has per-class holdings resolved by CUSIP, but the only shares-outstanding figure in our
pipeline is the issuer's **combined all-class** count. Dividing per-class holdings by the
combined count understates every percentage ~2×. #1646 detects this and renders an honest
caveat ("combined-basis lower bound") but cannot produce the true figure.

### Why the combined count is all we have (verified, #1646 + this spec)

`financial_facts_raw` is sourced from the SEC `companyfacts` JSON API, which returns ONLY
the **non-dimensional** member of each concept. Per-class `EntityCommonStockSharesOutstanding`
is tagged with a `us-gaap:StatementClassOfStockAxis` member → the companyfacts/companyconcept
API strips it (Alphabet `dei/EntityCommonStockSharesOutstanding` = 404). Only the combined
us-gaap `CommonStockSharesOutstanding` (12.211B for Alphabet) survives. The per-class values
live ONLY in dimensional facts — the per-filing inline XBRL instance, or the DERA FSDS
`num.txt` `segments` column. This spec consumes the latter.

---

## 2. Verified empirical findings (FSDS 2025q1 `num.txt`, downloaded + inspected 2026-06-17)

Source row schema: `adsh⇥tag⇥version⇥ddate⇥qtrs⇥uom⇥segments⇥coreg⇥value⇥footnote`.
`sub.txt` maps `adsh → cik, period, form`.

**Alphabet 10-K `0001652044-25-000014` (period 20241231), tag `CommonStockSharesOutstanding`,
version `us-gaap/2024`, uom `shares`, qtrs `0`:**

| segments | ddate | value |
|---|---|---|
| `ClassOfStock=CommonClassA;` | **20241231** | **5,835,000,000** |
| `ClassOfStock=CommonClassA;` | 20231231 | 5,899,000,000 |
| `ClassOfStock=CapitalClassC;` | **20241231** | **5,515,000,000** |
| `ClassOfStock=CapitalClassC;` | 20231231 | 5,691,000,000 |
| `ClassOfStock=CommonClassB;` | 20241231 | 861,000,000 |
| `` (segment-less, combined) | 20241231 | 12,211,000,000 |

Confirmed findings (each corrects or grounds a plan assumption):

1. **Tag is us-gaap `CommonStockSharesOutstanding`, version `us-gaap/2024`** — NOT
   `dei:EntityCommonStockSharesOutstanding` (which had ZERO Alphabet rows; consistent with the
   companyfacts dimensional-strip). Filter `tag='CommonStockSharesOutstanding' AND version LIKE 'us-gaap/%'`.

2. **`ddate` is the balance-sheet instant date, NOT a "cover-date".** A single filing reports
   the tag at TWO ddates: the current fiscal period end + the prior-year comparative. The
   plan's "(cover-date ≠ period_end)" rationale was **wrong**. The current value is selected
   by **`ddate == sub.period`** (exact, unambiguous) — equivalently `MAX(ddate)` per
   `(adsh, member)`. Verified across all three issuers (Alphabet period 20241231 → ddate
   20241231; HEI 10-Q period 20250131 → ddate 20250131; METC 10-K period 20241231). This spec
   uses **`ddate == sub.period`** as the primary selector.

3. **GOOGL (Class A) FY2024 = 5,835M** — not 5,899M (that is the 2023 comparative; a prior
   memory note conflated them). GOOG (Class C) = 5,515M. Internal-consistency proof:
   5,835 (A) + 5,515 (C) + 861 (B) = **12,211M = the combined value exactly**.

4. **Member localnames are issuer-specific and non-uniform.** Observed: `CommonClassA`,
   `CapitalClassC`, `CommonClassB`, and **`HeicoCommonStock`** (HEICO's voting common, an
   issuer-specific localname). A `(cik, member) → security` mapping **cannot be algorithmic**
   — a curated map is mandatory.

5. **Multi-axis `ClassOfStock` rows exist** (e.g. `ClassOfStock=CommonClassAAndCommonClassB;EquityComponents=CommonStock;`,
   `...;Restatement=ScenarioPreviouslyReported;`). The parser MUST require the `segments`
   string to be **exactly one** `ClassOfStock=<member>;` component — reject any row carrying a
   second axis (those are restatement/scenario/consolidated sub-slices).

6. **The plan's "≥10% magnitude separation" gate is WRONG.** Alphabet Class A (5,835M) and
   Class C (5,515M) differ only 5.8%; a 10% gate would reject Alphabet and defeat the feature.
   Replaced by a holdings-plausibility fail-closed guard (§7).

**Other verified per-class values (for the curated map + tests):**

| issuer CIK | member | value (current) | CUSIP | instrument |
|---|---|---|---|---|
| 0001652044 | CommonClassA | 5,835,000,000 | 02079K305 | GOOGL (6434) |
| 0001652044 | CapitalClassC | 5,515,000,000 | 02079K107 | GOOG (1002) |
| 0000046619 | CommonClassA | 83,920,000 | 422806208 | HEI.A (9485) |
| 0000046619 | HeicoCommonStock | 55,025,000 | 422806109 | HEI (5606) |
| 0001687187 | CommonClassA | 43,824,999 | 75134P600 | METC (10101) |
| 0001687187 | CommonClassB | 9,549,914 | 75134P501 | METCB (11102) |

(GOOG=1002 / GOOGL=6434 reconciled against the dev DB — a prior memory note had the iids
swapped; the data-engineer committee value was correct.)

---

## 3. Decision

Build a minimal FSDS per-class ingest, standalone:

1. New table `instrument_class_shares_outstanding` (sql/200), tiny cardinality.
2. `app/services/fsds_class_shares.py`: a streaming `num.txt`/`sub.txt` reader + a consumer
   that maps `(cik, member) → instrument` via a **curated CUSIP map** and upserts current-period
   per-class rows.
3. FSDS quarters added to the bulk-archive download inventory (Phase A3) + a new Phase-C
   bootstrap stage `sec_fsds_class_shares_ingest` (`db` lane) that streams the cached zips.
4. Read-path swap in `get_ownership_rollup`: when a verified per-class row exists and passes
   the fail-closed guard, divide by it and emit a `per_class_denominator` correction; else
   keep the combined denominator + the #1646 caveat.

**Settled-decision alignment:** honors **#1102 (CIK = entity, CUSIP = security)** — the
curated map keys on `(cik, member) → CUSIP`, and CUSIP → instrument resolution is the
security-identity lookup. Does NOT use `canonical_instrument_id` (#819, operational dups
only). Source-of-truth posture (#532): regulated SEC source only.

---

## 4. Schema — `sql/200_instrument_class_shares_outstanding.sql`

```sql
CREATE TABLE instrument_class_shares_outstanding (
    instrument_id      INTEGER NOT NULL REFERENCES instruments(instrument_id),
    period_end         DATE    NOT NULL,
    shares             NUMERIC(28,4) NOT NULL CHECK (shares > 0),
    class_member       TEXT    NOT NULL,              -- FSDS ClassOfStock localname (audit)
    source_cik         TEXT    NOT NULL,              -- 10-digit, from sub.txt (audit)
    source_adsh        TEXT    NOT NULL,              -- FSDS accession (audit/provenance/edgar_url)
    source_form_type   TEXT    NOT NULL,              -- sub.txt form (10-K/10-Q) → SharesOutstandingSource
    source_fsds_qtr    TEXT    NOT NULL,              -- e.g. '2025q1' (audit/provenance)
    source_filed_at    DATE    NOT NULL,              -- sub.txt filed (no-demotion tie-break; always present in sub.txt → NOT NULL so the ON CONFLICT predicate is deterministic, Codex ckpt-1b #4)
    resolution_method  TEXT    NOT NULL CHECK (resolution_method = 'curated'),
    parser_version     TEXT    NOT NULL,              -- 'fsds_class_shares_v1'
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, period_end)
);
```

- Grain: one row per `(instrument_id, period_end)`. The read-path selects `MAX(period_end)`
  per instrument.
- `resolution_method`: v1 writes only `'curated'` — CHECK pinned to it (Codex ckpt-1 #5: a
  reserved-but-unimplemented enum value invites accidental writes). A future auto-resolver
  widens the CHECK in its own migration.
- **Restatement no-demotion** (Codex ckpt-1 #6): `source_fsds_qtr >= stored` is insufficient
  for same-quarter amendments / multiple rows at one `(instrument, period)`. Upsert
  `ON CONFLICT (instrument_id, period_end) DO UPDATE ... WHERE EXCLUDED.source_filed_at >
  current.source_filed_at OR (EXCLUDED.source_filed_at = current.source_filed_at AND
  EXCLUDED.source_adsh > current.source_adsh)`. Within one ingest run, dedup per
  `(instrument, period)` keeping `max(filed_at, adsh)` before the upsert.
- No `known_to`/observation-history complexity — small derived lookup.

---

## 5. FSDS reader + ingest — `app/services/fsds_class_shares.py`

### 5.1 Streaming readers (mirror `sec_13f_dataset_ingest._iter_tsv`)

```python
def iter_fsds_num(zf: zipfile.ZipFile) -> Iterator[dict[str, str]]:
    """Stream num.txt rows (tab-delimited, csv.DictReader). 530 MB/quarter — never
    materialised; the zip member is read as a text stream."""

def read_fsds_sub(zf: zipfile.ZipFile) -> dict[str, FsdsSub]:
    """sub.txt → {adsh: FsdsSub(cik_10digit, period, form, filed)}. sub.txt is small
    (~2 MB). Returns a structured row (Codex ckpt-1b #3): the consumer needs `form`
    (→ source_form_type) and `filed` (→ source_filed_at) in addition to cik+period."""
```

### 5.2 Consumer

For one FSDS quarter zip:
1. `sub = read_fsds_sub(zf)` → `{adsh: (cik, period)}`.
2. Stream `iter_fsds_num(zf)`; keep a row only when ALL hold:
   - `tag == 'CommonStockSharesOutstanding'` and `version` starts `'us-gaap/'`
   - `uom == 'shares'`, `qtrs == '0'` (instant)
   - `segments` is **exactly one** `ClassOfStock=<member>;` component (split on `;`, drop
     trailing empty → len 1; component starts `ClassOfStock=`) → extract `member`
   - `adsh` in `sub`, and `ddate == sub[adsh].period` (current period, not comparative)
   - `(cik, member)` in the curated map (§6) → `cusip`
3. Resolve `cusip → instrument_id` via `external_identifiers` (provider IN ('sec','openfigi'),
   identifier_type='cusip', joined to `instruments` on `is_tradable=TRUE`). **Fail-closed on
   ambiguity** (Codex ckpt-1 #5): require EXACTLY ONE eligible tradable instrument — 0 (skip,
   warn "cusip unresolved") or >1 (skip, warn "cusip ambiguous": operational dup / historical
   row) both fall through without writing. Never guess.
4. Parse `value` → `Decimal` (> 0). Carry `source_filed_at`/`source_form_type` from `sub.txt`.
   Upsert `(instrument_id, period_end=ddate)` with the no-demotion ON CONFLICT from §4.

`parser_version = 'fsds_class_shares_v1'`.

### 5.3 Curated map location

`_CLASS_MEMBER_TO_CUSIP: dict[tuple[str, str], str]` keyed `(cik_10digit, member)` → CUSIP.
Keyed on CUSIP (not instrument_id) so it is **environment-independent** (dev/prod instrument
ids differ) and ties to the settled CUSIP-is-security identity. Fail-closed: a `(cik, member)`
absent from the map is skipped. Authoring rule: every entry hand-verified against the SEC
filing's per-class CUSIP at add time (the curated map IS the correctness guarantee; the runtime
guard in §7 is a drift tripwire, not the primary mechanism).

Initial entries: the six rows in §2's value table.

---

## 6. Bootstrap wiring

### 6.1 Download (Phase A3) — `sec_bulk_download.build_bulk_archive_inventory`

Add FSDS quarters (mirror nport's `n_quarters_nport`):

```python
for q in last_n_quarters(n_quarters_fsds, today=today):   # default 4
    ArchiveSpec(
        name=f"fsds_{q}.zip",
        url=f"{SEC_BASE_URL}/files/dera/data/financial-statement-data-sets/{q}.zip",
        ...)
```

The generic `download_bulk_archives` (ETag reuse + SHA-256 + bandwidth probe) handles FSDS for
free. 4 quarters ≈ 500 MB cached zips; num.txt is stream-parsed (never extracted to disk). HEAD
verified live: `200`, `content-length 127765057` for 2025q1.

### 6.2 Ingest (Phase C) — new stage + invoker

- `app/services/sec_bulk_orchestrator_jobs.py`: `sec_fsds_class_shares_ingest_job()` zero-arg
  invoker — for each `fsds_*` archive in the inventory, open zip, run the §5 consumer. Mirrors
  `sec_nport_ingest_from_dataset_job`. Standalone-safe (operator may trigger directly).
- `app/jobs/runtime.py`: register `_INVOKERS[JOB_SEC_FSDS_CLASS_SHARES_INGEST]`.
- `app/services/bootstrap_orchestrator.py`: add stage `sec_fsds_class_shares_ingest`
  (`db` lane) **ordered AFTER `cusip_resolver_post_bulk_sweep`** (next free order ~14;
  Codex ckpt-1b #1). Cap requirement `all_of=("bulk_archives_ready", "cusip_mapping_ready")`.
  - **Why `cusip_mapping_ready` suffices** (vs needing the post-bulk openfigi sweep): FSDS
    resolves the *issuer's own per-class CUSIP* — a UNIVERSE instrument's CUSIP, written by
    S3 `cusip_universe_backfill` (which provides `cusip_mapping_ready`), present before the
    sweep. The `cusip_resolver_post_bulk_sweep` targets *13F holding* CUSIPs
    (`unresolved_13f_cusips`), a different population. Ordering after the sweep is still done
    (free, conservative) so any future dual-class instrument whose own CUSIP only resolves via
    openfigi is covered.
  - **Provides `fsds_class_shares_ingested`** (new `Capability`; Codex ckpt-1b #2) so the
    stage is a visible DAG node, not a no-provides orphan. **NOT required by
    `bootstrap_validation`**: the per-class denominator is an enhancement that fails closed to
    the #1646 caveat — a missing FSDS row is a valid state, not a broken install, so it must
    not gate terminal validation. (Mirrors the no-hard-gate posture; the provides cap is for
    visibility/future use.)
  - Tiny cost (hundreds of rows) → `db` catch-all lane, no dedicated family lane.

---

## 7. Read-path swap — `app/services/ownership_rollup.py::get_ownership_rollup`

`OwnershipSlice.pct_outstanding` is computed from the `outstanding` passed into
`_bucket_into_slices`/`_build_slice`; holder `.shares` is absolute. So swapping the
denominator before bucketing yields per-class-correct pcts with no other change.

### Not a `CorrectionApplied` — a new provenance field (Codex ckpt-1 #3)

`CorrectionApplied` is a **share-removal** record (`shares_removed: Decimal`; docstring "a
correction here REMOVES shares from the total"). A denominator swap removes NO shares — forcing
it into that contract would mean `shares_removed=0`, a semantic lie. Instead add a sibling
optional field, parallel to `dual_class_denominator` and **mutually exclusive** with it:

```python
@dataclass(frozen=True)
class PerClassDenominator:
    """The rollup divided by a VERIFIED per-class share count (FSDS), not the
    combined all-class count — supersedes the #1646 caveat. Provenance only;
    removes no shares (NOT a CorrectionApplied)."""
    cik: str
    class_member: str          # FSDS ClassOfStock localname
    period_end: date           # FSDS class period (== combined as_of, by the §7 guard)
    per_class_shares: Decimal  # the denominator used
    combined_shares: Decimal   # what #1646 would have used (transparency)
    source_adsh: str
    source_fsds_qtr: str
    note: str                  # server-owned FE copy (single source)

# on OwnershipRollup, last field with default:
per_class_denominator: PerClassDenominator | None = None
```

When set, `dual_class_denominator` is None and vice-versa. API model
`app/api/instruments.py` + FE type `frontend/src/api/ownership.ts` gain the mirror. No
`CorrectionApplied.kind` vocab change. CSV: a `__per_class_denominator__` inert memo row
(mirrors `__dual_class_denominator__`).

### Ordering in `get_ownership_rollup`

New helper `_read_class_shares_outstanding(conn, instrument_id) -> ClassShareRow | None`
(`SELECT shares, period_end, class_member, source_cik, source_adsh, source_form_type,
source_fsds_qtr ... ORDER BY period_end DESC LIMIT 1`).

Insert after dedup/reconcile, before `_bucket_into_slices`:

1. Keep combined `outstanding` + `outstanding_as_of` + staleness check unchanged (combined
   stays the conservative upper bound for `_reconcile_institutional_families`' garbage filter).
2. Compute `by_category` + `unmatched_def14a` as today — denominator-independent.
3. `max_pie_holder_shares` = max `.shares` across **pie-wedge** contributors only —
   `by_category` holders + `unmatched_def14a` (an additive pie wedge). **Exclude `funds`**
   (Codex ckpt-1 #4): N-PORT funds are an `institution_subset` memo overlay (NOT a pie wedge;
   excluded from residual/concentration) and may double-count 13F, so they must not veto the
   denominator. Denominator-independent.
4. `class_row = _read_class_shares_outstanding(conn, instrument_id)`.
5. **Fail-closed guard** (pure `_should_use_class_denominator`) — use the per-class
   denominator only when ALL hold:
   - `class_row is not None`
   - **freshness coherence** — `not _denominator_too_stale(class_row.period_end, today)`:
     the per-class period clears the SAME 548-day staleness bound (#1581
     `_STALE_DENOMINATOR_MAX_AGE_DAYS`) the combined denominator must clear. This is the
     "tightly justified tolerance" Codex ckpt-1 #2 allowed in place of exact period-equality.
     **Why NOT strict `class_period == combined_as_of` equality** (the v1 spec / Codex ckpt-1
     proposal, REVISED at Codex ckpt-2): companyfacts (the combined as_of) updates a quarter
     ahead of DERA FSDS (the per-class period), so an equality gate would make the swap
     essentially never fire on live data. Bounding the per-class period by the repo's own
     settled denominator-freshness window is the principled middle — shares-outstanding drifts
     < a few % over that window, far below the ~2× error it corrects — and is uniform with how
     the combined denominator is already checked (`_denominator_too_stale(outstanding_as_of,
     today)` upstream). A class count older than 548 days → fall back to the #1646 caveat.
   - `0 < class_shares < outstanding` (structural: a class is a strict subset of the combined
     total when ≥2 classes; rejects a stale/garbage row ≥ combined)
   - `max_pie_holder_shares <= class_shares` (holdings-plausibility: no resolved pie holder can
     own more shares than exist in the class — catches a mis-mapped too-small denominator, the
     %-inflating direction)
   On pass: `effective_outstanding = class_shares`; `effective_as_of = class_row.period_end`;
   `effective_source = SharesOutstandingSource(accession_number=class_row.source_adsh,
   concept='CommonStockSharesOutstanding', form_type=class_row.source_form_type,
   edgar_url=edgar_archive_url(class_row.source_adsh))` (synthesized FSDS source — Codex
   ckpt-1 #1: the reported source must NOT claim companyfacts when the value is FSDS; the FSDS
   adsh yields a valid EDGAR archive URL); `per_class_denominator = PerClassDenominator(...)`;
   `dual_class_denominator = None`.
   Else: `effective_* = combined`; `per_class_denominator = None`; `dual_class_denominator =
   _detect_dual_class_denominator(...)` as today (#1646 caveat preserved).
6. `_bucket_into_slices`, `_compute_residual`, `_compute_concentration`, `_compute_sanity`
   run against `effective_outstanding`. The rollup reports `shares_outstanding=effective_*`,
   `shares_outstanding_as_of=effective_as_of`, `shares_outstanding_source=effective_source`.

The correctness guarantee is the **hand-verified curated map + a per-entry fixture test**
(§9), not a runtime magnitude heuristic (Codex ckpt-1 §11.2 — avoid rank/magnitude). The
three runtime guards are drift tripwires that fail closed to the honest #1646 caveat.

### FE

When `per_class_denominator` is set, the #1646 `DualClassDenominatorCallout` is absent. Render
a small server-owned info note from `per_class_denominator.note` ("Percentages use the
per-class share count for <member> (5,835M), not the combined all-class count.") on the same
L1 + L2 surfaces (reuse `DualClassDenominatorCallout` shape with a positive variant, single
copy source). The figures are per-class-true; the note is informational, not a caveat.

---

## 8. #790 prevention comment (bundled)

Add an inline comment on `_read_universe_estimates` (`ownership_rollup.py:2472`): the all-NULL
return is **intentional** — `is_estimate=True`/`unknown_universe` is the honest state; do NOT
revive a fabricated universe denominator (filer-count coverage_ratio is size-correlated and
reads as misleading-green). The real size-debiasing value is tracked in #1660. (Committee
disposition of #790, 2026-06-17.) Same file as this PR → bundled, no separate PR.

---

## 9. Tests

Pure-logic (no DB) — extract the parse/guard decisions into pure functions:
- `parse_class_member(segments)` → member or None: single `ClassOfStock=X;` passes; multi-axis
  (`ClassOfStock=X;EquityComponents=Y;`) rejected; non-ClassOfStock rejected; empty rejected.
- row-filter: us-gaap tag/version/qtrs=0/uom=shares accept; dei tag / duration (qtrs>0) /
  wrong-uom reject.
- current-period select: `ddate == period` picks current (5,835M) over comparative (5,899M).
- fail-closed guard `_should_use_class_denominator(class_shares, class_period_end,
  combined_shares, today, max_pie_holder_shares)`: table-test — pass (Alphabet 5,835M <
  12,211M, holder ≤ class, class period fresh); reject class ≥ combined; reject class ≤ 0;
  reject holder > class; **reject stale class period** (> 548 days before `today`).
- restatement no-demotion `_pick_restatement(rows)`: later `filed_at` wins; equal filed →
  larger `adsh`; never demotes.

**Curated-map fixture test (Codex ckpt-1 §11.2 — the real correctness guarantee):** one
parametrised test over EVERY `_CLASS_MEMBER_TO_CUSIP` entry asserting `(cik, member) → CUSIP`
matches the SEC-verified value in §2's table (catches a curated typo / A↔C swap at authoring,
not via runtime heuristic).

DB-backed (one integration test, marker `db`):
- seed `instrument_class_shares_outstanding` (GOOGL 5,835M @ period == AAPL-style combined
  as_of) + an institutions holding; assert the rollup divides by 5,835M, sets
  `per_class_denominator`, `dual_class_denominator is None`, and
  `shares_outstanding_source.accession_number == source_adsh`.
- seed a too-small class row → holder > class → guard fails → #1646 caveat returns.
- seed a class row whose `period_end` is > 548 days stale → freshness guard fails → #1646
  caveat. (DB cases use periods relative to `date.today()` so the snapshot-clocked staleness
  guard is exercised deterministically, not time-bombed.)

CSV: `__per_class_denominator__` memo row + copy.

FE: `OwnershipPerClassDenominator` type present; positive callout renders from `note`; #1646
callout absent when per-class set.

---

## 10. Backfill + verification (DoD clauses 8–12)

ETL/parser/schema change → the additional DoD clauses apply.

1. Apply sql/200 on dev.
2. Download FSDS (4 quarters) via `sec_bulk_download` (or operator
   `POST /jobs/sec_bulk_download/run`), then run `sec_fsds_class_shares_ingest` on dev.
3. Smoke panel + dual-class set: `AAPL` (single-class, unchanged), `GOOGL`/`GOOG`,
   `HEI`/`HEI.A`, `METC`/`METCB`. Record the operator-visible figure:
   GOOGL institutions 20.97% → ~43.8% (÷5.835B), no caveat, `per_class_denominator` correction;
   AAPL unchanged (no class row).
4. Cross-source: FSDS Class A 5,835M vs SEC 10-K `0001652044-25-000014` per-class cover
   (independent of our pipeline) — matches.
5. Hit `/instruments/GOOGL/ownership-rollup` live after ingest; confirm the figure renders
   per-class with the correction.
6. Record each step + commit SHA in the PR.

No `sec_rebuild` (new table, not a manifest source). Jobs-process restart needed only to pick
up the new stage/invoker for steady-state (operator follow-up); the dev backfill is a direct
job invocation.

---

## 11. Codex ckpt-1 resolutions (2026-06-17)

6 findings (3 High, 3 Medium) + §11 answers — ALL adopted:

1. **(High) Synthesize FSDS source on swap** — done (§7): the reported
   `SharesOutstandingSource` is rebuilt from `source_adsh`/`source_form_type`, never left
   claiming companyfacts. FSDS adsh → valid `edgar_archive_url`.
2. **(High) Freshness-coherence guard** — added. (Codex ckpt-1 proposed strict
   `class_period_end == outstanding_as_of`; **REVISED at Codex ckpt-2** to the repo's existing
   548-day `_denominator_too_stale` bound on `class_period_end` — companyfacts updates a
   quarter ahead of FSDS, so equality would essentially never fire. See §7 step 5.) Prevents
   fresh holdings ÷ a stale class count while letting the swap fire across the normal lag.
3. **(High) Not a `CorrectionApplied`** — replaced with a new `PerClassDenominator` provenance
   field (removes no shares; mutually exclusive with `dual_class_denominator`). No correction
   vocab change.
4. **(Medium) Exclude funds from the holdings guard** — `max_pie_holder_shares` is over
   pie-wedge contributors only; N-PORT funds (`institution_subset` overlay) cannot veto.
5. **(Medium) CUSIP fail-closed on ambiguity** — require exactly one tradable instrument; 0 or
   >1 skip+warn.
6. **(Medium) Restatement no-demotion** — upsert orders by `(source_filed_at, source_adsh)`;
   `source_filed_at`/`source_form_type` added to the table.

§11 answers: keep `ddate == sub.period` (skip+telemetry when no exact current row); curated
map + per-entry fixture tests are the correctness guarantee, no magnitude heuristic; 4-quarter
window configurable + fail-visible when a curated issuer has no current-period row; drop
`magnitude_match` from the CHECK.

---

## 12. Settled-decisions / prevention-log impact

- No settled-decision change. Reinforces #1102 (CUSIP = security) and #532 (regulated source).
- Prevention-log + data-engineer skill: extend the #1646 entry — "a source fact unreachable
  through one ingest path (companyfacts dimensional-strip) may be reachable through another
  (DERA FSDS `segments`); verify reachability per-path." Add the empirical FSDS gotchas (ddate
  = balance-sheet instant with a comparative twin; issuer-specific member localnames;
  multi-axis ClassOfStock rows) to `.claude/skills/data-sources/sec-edgar.md`.
- sec-bulk-archives doc note ("FSDS largely redundant, not consumed") is now partially
  superseded — FSDS IS consumed for the single-axis ClassOfStock per-class denominator (NOT
  for segments, which keep the per-filing extractor). Update that doc.
