# #3360 — PIT fundamentals bundle from `companyfacts.zip`

Step 1 of `docs/proposals/ta/2026-09-24-selection-programme-v2.md` ("The data unlock"). Refs #2899, #2900,
#2901, #2902, #3361, #3362. Codex ckpt-1 ran three times (80 + 68 + 54 findings, 2026-09-24); see "Design history".

## Contract (deliberately narrow)
The bundle is a **point-in-time event store of raw XBRL facts** for a declared concept set, and one reader:

> For CIK C, concept K, unit U and period (start, end): the value(s) that admitted periodic reports had put on
> EDGAR, **as accepted strictly before decision session D**, or a typed absence.

It does **not** define economic fields. Aliasing (`Revenues` vs `RevenueFromContract…`), derivation (gross profit
= revenue − cost), period alignment (annual GP with year-end assets), staleness, sign screens and treatment of
non-reliance notices are each arm's **declared construction**, frozen in that arm's #2829 declaration and reviewed
at that arm's ckpt-1 (#2901 first). The programme's "declared concept-alias map / sign-unit checks / no silent
fallback" criteria bind there; this bundle supplies what they need: every candidate concept, unit and period, each
with its own public clock, and absence that is never 0.

**Not here:** field construction (arms), CIK ↔ security linkage (#3361), termination (#3362), split adjustment,
market value, IFRS reporters.

## Source rules
- **Shape** (`.claude/skills/data-sources/sec-edgar.md` §1, §7.5): per-CIK JSON; fact rows carry `start?`, `end`,
  `val`, `accn`, `form`, `filed`, `fy`, `fp`, `frame?`. Standard taxonomies and non-dimensional facts only
  (§7.17): absence means **not extracted**, never "not disclosed".
- **Clock** = `acceptanceDateTime` of the accession, from `submissions.zip` `recent` + every `filings.files` page
  (§7.4), UTC → New York (§7.8). This is an **acceptance** clock: SEC states no guaranteed dissemination lag, so
  the rule "public iff acceptance NY date < D" leaves at least the rest of the acceptance day and is labelled as
  acceptance-based, not publication-proven.
- **Extraction chokepoint** (§7.16): rows go through `sec_fundamentals._extract_facts_from_section`
  (`allowed_tags` = the concept set, `retention_cutoff=None`; window `[1900-01-01, 2100-01-01)` and
  `_UNIT_PRIORITY` inherited). It requires `filed`/`form`/`accn`/`end`/`val` and drops failures silently, so the
  builder counts raw rows per (taxonomy, concept, unit) before the call and reconciles every raw row to exactly one
  outcome (stored, or one named rejection).
- **Forms admitted** (policy, not an audit-status claim; basis: operator steer 2026-08-22 "audited periodic
  accounts stay in scope for valuation"): `10-K`, `10-KT`, `10-Q`, `10-QT`, `20-F`, `40-F` and the `/A` of each.
  Form is taken from **submissions** (authoritative for the accession); a companyfacts `form` that disagrees is
  counted `form_mismatch` and the row rejected.
- **Mandate phase-in** (§2.4.1) is not assumed: voluntary and pre-mandate XBRL is admitted on its own clock. The
  parent's "2011-06 = full mandate coverage" premise is reported by the census against an independent filer
  population, not inherited.

## Premise measurements
`uv run python scripts/measure_3360_companyfacts_acceptance.py --companyfacts <zip> --submissions <zip>`; run
2026-09-24 on companyfacts `e00a594c4716385b3af7bb8ac49b980e567bf9070025d3e3c9e76b6702d5f329`, submissions
`3f894f8c1ef56c0068f7f8a4cd994dc0fb2619946bd23f7affb90384de6fecb8` (script at this PR's head):
- 466,773 (CIK, accession) pairs across all taxonomies; **300** have no acceptance in the CIK's own submissions.
- Acceptance NY date vs `filed`: 429,377 equal, 36,815 earlier, **281 later** (e.g. CIK 1800,
  `0001104659-10-033097`, accepted 2010-06-09, `filed` 2010-05-04). Cause not asserted; on those a `filed` clock
  would call a fact public before EDGAR accepted it.
- us-gaap `(concept, unit, start, end)` keys: 67,251,204; 33,062,769 on >1 accession; 3,208,412 of those carry
  different values (exact `Decimal`) — "value disagreement", not classified as restatement. "Which copy at D" must
  therefore be a rule.

## Construction rules
1. **Concept set** (hashed): us-gaap `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`,
   `RevenueFromContractWithCustomerIncludingAssessedTax`, `SalesRevenueNet`, `CostOfRevenue`,
   `CostOfGoodsAndServicesSold`, `CostOfGoodsSold`, `GrossProfit`, `Assets`, `StockholdersEquity`,
   `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest`, `NetIncomeLoss`,
   `NetCashProvidedByUsedInOperatingActivities`, `CommonStockSharesOutstanding`; dei
   `EntityCommonStockSharesOutstanding`. Storing a concept asserts nothing about its economic scope. Units are the
   chokepoint's `_UNIT_PRIORITY` (`USD`, `USD/shares`, `shares`, `pure`); other units (non-USD reporters) are
   counted `unit_outside_policy` and are out of scope with IFRS.
2. **Input identity:** zip members unique; payload `cik` equals the member name for both archives; a mismatch
   excludes that CIK (rule 5).
3. **Key** = (taxonomy, concept, unit, start|null, end). **Event** = key + accn + value + acceptance (UTC ISO with
   `Z`) + multiplicity (raw rows collapsing to it). Value = exact `Decimal` from the JSON text; canonical string
   built from `as_tuple()` (strip trailing zeros of the coefficient, adjust exponent, render positional; zero →
   `0`), no context arithmetic. `fy`, `fp`, `frame`, `filed` not stored. `filed` still gates admission because
   the chokepoint requires it (rejection `chokepoint_reject`); it is never a clock.
4. **Row outcomes** — every raw row gets exactly one, first match wins, each with a source locator
   (cik, taxonomy, concept, unit, row index):
   `malformed_structure` (container shapes validated before the chokepoint) → `out_of_scope_form` (accession form
   not admitted; **non-blocking**, not a rejection) → `no_acceptance` → `chokepoint_reject` →
   `unit_outside_policy` → `prospective_period` (`end` > acceptance NY date) → stored. Only
   `chokepoint_reject` and `prospective_period` on an admitted accession are **blocking rejections** (dated,
   keyed); the rest are ledger entries.
5. **Snapshot integrity** (property of the archive, stated as such): a CIK is excluded whole, and listed in the
   manifest, if its submissions entry or any listed page is missing, a page lacks a required column or has column
   arrays of unequal length, or one accession carries conflicting (acceptance, form, items) across pages. So an
   answer is a function of (snapshot integrity state, public prefix).
6. **Accession index** per CIK: every admitted accession `{accn, form, acceptance}` (context for arms: annual vs
   quarterly vs transition vs amendment), gated by the same public rule.
7. **Public at D:** acceptance NY date `< D` (NY zone loaded from the pinned `tzdata` package file, not the system
   TZ path). Applies to events, blocking rejections and the accession index alike.
8. **Reader** `value_as_of(C, key, D)` → exactly one of: `after_capture`, `cik_not_in_bundle`,
   `cik_integrity_excluded`, `concept_not_in_policy`, `absent` (no public event or blocking rejection),
   `blocked_by_rejection`, `ambiguous(values, accns)`, `value(v, accns, acceptance)`. Rule: L = the maximal
   acceptance timestamp over public events ∪ public blocking rejections; if any blocking rejection is at L →
   `blocked_by_rejection` (conservative on ties); else the events at L give one value → `value`, several →
   `ambiguous`. Never falls back past L.
9. **Prefix reader** `public_events(C, concept, D)` returns every public event and blocking rejection for the
   concept (all keys, all accessions) plus the public accession index, so an arm can take several fields from one
   filing or choose its own period. Nothing in the bundle ranks or selects.
10. **Total order** of every array: key (null `start` first), acceptance, accn, canonical value, reason.

## Artefact
- **Inputs snapshotted:** both archives copied into `<bundle>/inputs/`, hashed from the copy; the build reads only
  the copies; inputs retained for byte-for-byte rebuild.
- **Identity:** `POLICY` = sha256 of canonical JSON of every constant (concept set, forms, units, parser window,
  outcome precedence, schema versions) + sha256 of the builder, reader, `sec_fundamentals.py`,
  `r6_pit_bundle.py`, plus Python and `tzdata` versions. The loader refuses a bundle whose `POLICY` ≠ its own.
- **Supported window:** `supported_through` = the earlier of the two archives' maximum acceptance NY date; reads
  with D > `supported_through` → `after_capture`.
- **Layout:** `<bundle>/shards/CIK##########.json` = `{schema, cik, events[], rejections[], accessions[]}`;
  `<bundle>/manifest.json` = `{schema: "pit-fundamentals-manifest-v1", policy, input_sha256, supported_through,
  snapshot_integrity_failures, shards: [{cik, path, sha256, events}], ledger}`, ordered by CIK, `sort_keys` +
  fixed separators. `path` must be `shards/<basename>`; anything else refuses.
- **Publish:** `mkdir <bundle>` exclusively at start (fails if it exists); shards written and `fsync`ed, then each
  re-read via `read_verified_document` and checked; the manifest is written LAST with the no-replace link of
  `build_2900_pit_bundle._write_exclusive`, then the directory is `fsync`ed. No manifest = not a bundle; a crashed
  build is deleted, never resumed.
- **Loader:** manifest and each shard via `read_verified_document` (size-bounded single read), digest compared to
  the pinned / manifest value, then: schema, `cik` = entry, path containment, total order, unique events, finite
  decimals, parseable timestamps, per-shard event count = manifest, no duplicate CIK or path. Any document over
  `MAX_EVIDENCE_BYTES` fails the BUILD; all shard sizes are reported.

## Census (descriptive; before any strategy look)
`scripts/census_3360_pit_fundamentals.py` → an evidence manifest hashing the bundle manifest, the sorted Form 25
rows read, code and parameters.
- **Formations:** last NYSE session of each June, 2011–2024 (descriptive; arms re-run with their own grid).
- **Population (independent of companyfacts):** CIKs in submissions with an admitted-form accession accepted in
  `[D − 730 days, D)` NY date; CIKs whose submissions fail integrity form a separate `eligibility_unknown` cohort.
  Two separate axes per member: **snapshot status** (`in_bundle` / `no_companyfacts_entry` / `integrity_excluded`)
  and **historical status at D** (`has_public_event` / `no_public_event`).
- **Per formation × concept:** members with ≥1 key whose reader status at D is `value` and whose `end` ≥ D − 548
  days (descriptive recency window, 18 months, stated as such), plus counts of `ambiguous` and
  `blocked_by_rejection`. Each concept separately; no pooled "GP available".
- **Size:** census-only construction — `Assets`, unit `USD`, the latest `end` among instant keys with status
  `value` at D; zero-based rank over (value, CIK), decile = ⌊10·rank/n⌋; `assets_unavailable` separate; n = 0 → no
  deciles.
- **Outcome (issuer-level, neutral labels):** `sec_form25_register` rows with `provision_class =
  'equity_delisting'` by `issuer_cik`, filed in (D, D+730]. Per issuer: an indicator per raw `rule_provision`
  label (`(b)`, `(a)(4)`, `(a)(3)`, NULL, other) and the label set of the first filing date. No economic
  interpretation here (#3362 owns it). No event: `no_form25_observed` only if [D, D+730] ⊆
  `[2013-01-02, max(filed_date)]`, else `unobserved_horizon`; completeness inside the span is not certified.
- **Ledger reconciliation:** raw rows in = Σ outcomes (rule 4) + rows excluded with an integrity-excluded CIK,
  per concept; `no_acceptance` reported per CIK, never assigned to a formation.
- Extracted-concept coverage is reported; whether the XBRL mandate was fully met at a date is NOT tested here.

## Acceptance (this ticket)
1. Pure fixtures: restatement; partial `/A`; missing original (comparative-only); same-day acceptance; `frame`/
   `fy` inert; `filed` < acceptance row public only after acceptance; ambiguity and recovery; blocking rejection at
   the latest timestamp and on a tie; rejected-only key; out-of-scope 8-K row does not block; prospective period;
   form mismatch between archives; integrity exclusion; decimal canonicalisation (`1`, `1.0`, `-0`, long
   coefficients); `after_capture`; corrupt shard, wrong-CIK shard, bad path, moved manifest, policy mismatch;
   rebuild determinism.
2. **Full-population causal reference:** for every CIK and every distinct acceptance NY date d over its events,
   rejections and accession index (plus one date before the first), rebuild the shard by an independent path —
   filter the raw companyfacts rows and submissions entries to acceptance date < d+1 first, then apply rules 3–6
   — and assert `value_as_of` for every key and `public_events` for every concept equal the full-bundle answers
   at D = d+1 (calendar cutoff; D ≤ `supported_through`).
3. Ledger reconciles; census evidence manifest written.
4. Registry (`app/services/research_point_in_time.py`): `RankingFamily.COMPANYFACTS_PIT` with **public_clock pass,
   qualified** "acceptance clock; public from the next NY date; dissemination lag not proven", **causal_transform
   pass** (item 2), **system_versions fail** — "no independent vintage witness: companyfacts is today's
   extraction, and DERA FSDS was reprocessed in 2024-12 so historical quarters are not original vintages" —
   **historical_population fail** (#3361). `FIELD_REGISTRY`, `_REASONS`, `PROBE_MATRIX` updated together; added to
   `QUALITY`, `VALUATION`, `SHAREHOLDER_YIELD` beside `FUNDAMENTAL_FACTS` (DB path, unchanged). **Nothing becomes
   admissible.** Whether an arm may proceed with `system_versions` as a stated residual is that arm's declaration.

## Design history (why this is narrow)
Rounds 1 (80 findings) and 2 (68) reviewed drafts that defined economic fields inside the bundle (alias priority,
derived gross profit, annual-period selection, staleness, compression, non-reliance windows). Most round-2 findings
were consequences of those choices, and each is a construction an arm must make against its own published
definition (Novy-Marx 2013 for GP/A), so they moved to the arm's declaration and ckpt-1. Round 3 (54) on the
narrowed draft drove: out-of-scope forms made non-blocking; a prefix reader and accession index so arms can build
same-filing fields; a complete result algebra and outcome precedence; exact decimal canonicalisation; data-derived
`supported_through`; no-replace publish with fsync; neutral Form 25 labels instead of an economic classifier;
8-K Item 4.02 headers dropped (they do not name the affected periods — an arm needing non-reliance pins its own
evidence); and the FSDS "vintage probe" dropped because SEC reprocessed FSDS in 2024-12, which leaves
`system_versions` honestly failed. Kept throughout: acceptance clock (labelled), strict admission with a per-row
ledger, ambiguity on the public prefix only, no fallback past a later blocking rejection, independent-path causal
test, snapshotted inputs, policy-bound loader, independent census population, observed-span rule.
