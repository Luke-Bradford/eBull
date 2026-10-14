# Statutory matching from the effective generation (#3616 slice 3d-2)

Source: #3616 slice 3d, split by 3d-1 (`2026-10-14-3616-slice3d1-consideration-basis.md`, "3d split"). 3d-2 owns
slice-3 checkpoint-1 round-1 findings 4, 5, 6, 7, 29, 31, 33, 42 and 43 (text in
`var/research/3616/ckpt1_slice3_r1_final.txt`, loop worktree), the rounding policy 3c left open (3c spec, Design 1
"Precision"), the implementation of 3d-1's consideration classes, and retiring the `tax_lots` write path.

**What this slice builds.** A matching run that reads the effective generation of tax evidence (3c Design 6), maps each
admitted lot to a tax security, withholds every security whose identification the evidence cannot establish,
identifies each statutory disposal under TCGA 1992 ss104–106A, computes its GBP gain with the reasons it inherits, and
stores the result append-only, one sealed snapshot per generation and input revision. Every gain is marked
`character_pending` (3d-3). Nothing is shown to the operator: the tax-year view, coverage labelling and the page are
3d-4's.

## Facts established first

"Script" is `scripts/measure_3616_slice3d2_population.py`, read-only, run on 2026-10-14 over run 2 (the effective
generation, policy `broker_evidence_v2`) in one REPEATABLE READ transaction; its sums run in a precision-60 context
with `Inexact` and `Rounded` trapped. Output: `var/research/3616/slice3d2/population.txt` (gitignored; dates,
amounts, counts and symbols, no account identifier). It groups by the mapping of Design 2.

1. **Run 2** holds 21 subjects (8 admitted, 7 unresolved, 6 `excluded_non_share_product`, all 6 with product `cfd`)
   and 11 lots (8 acquisitions, 3 disposal pieces). It read history fetch 12 (`ok`, `min_date` 2026-10-02, started
   2026-10-14 02:14:52Z). The scope's first `ok` fetch started 2026-10-14 00:24:01Z; 12 `ok` fetches up to fetch 12.
2. **Securities under Design 2.** No instrument of a subject or lot maps to *unestablished*.
   - **With admitted lots:** AAPL (1001: 1 acquisition, 2 disposal pieces, all 2026-09-23), NXH (1181: 1 acquisition,
     2025-11-14), SPY (3000: 7 lots, every one on SPY.RTH, 3417, whose canonical row is 3000).
   - **With unresolved subjects only:** GME (2, `pricing_currency_unestablished`), CSPX.L (3,
     `close_conversion_unestablished`), IUSA.L (1, same), ILMN (1, `account_unestablished`,
     `settlement_unestablished`, product NULL). No admitted security has an unresolved subject.
   - **With CFD subjects only:** SVOL, IEP (2), QQQ, SVXY, VOO.
   - 6 subjects are on an instrument with a canonical row (all SPY.RTH). The Script prints each named instrument's
     component (Design 2) with its raw rows ("[component]"): 12 components, all established; AAPL, GME and QQQ have
     bound `.RTH` variants (8754, 8761, 3418) with no subject.
3. **Days.** AAPL 2026-09-23: acquisition 0.14707100 units, £37.69317753500000; disposals 2 pieces, 0.14707100,
   £37.68563889949300. SPY 2026-09-23: acquisitions 2 pieces, 0.29220900, £169.30267621620600; disposal 1 piece,
   0.29615500, £172.44628722262500. SPY's other acquisition days: 09-18 (0.29615500, £168.43050669140000), 09-30
   (20.62672000, £11933.68460630607000), 10-01 (0.13442600, £77.30850742070600), 10-09 (0.61384700,
   £360.56710775200500). NXH 2025-11-14: 1305.05709600, £6048.55403340328000. Every lot's FX is `published`.
4. **Class inputs.** AAPL's subject has `execution_evidence` `lookup` and 2 unobserved positions; NXH's is `sync_only`;
   SPY's six are `lookup` with none unobserved. The resulting classes are 3d-1's ("Run 2 under this policy").
5. **The legacy tables** `tax_lots`, `disposal_matches` and `s104_pool` hold 0 rows on dev. Their readers are the
   `/tax/*` endpoints (`app/api/tax.py`, through `tax_ledger`'s read functions), `frontend/src/pages/TaxPage.tsx`, and
   `app/services/budget.py::_load_tax_estimates`, which sizes the budget's CGT reserve from `disposal_matches`.

## Source rules

- **Identification order** (HMRC CG51555 and CG51560, legislation.gov.uk s105 and s106A, all read 2026-10-14):
  1. all acquisitions of the same class by the same person in the same capacity on one day are one acquisition, and
     all disposals on one day one disposal (s105(1)(a));
  2. a disposal is identified first with that day's acquisition, so far as its quantity allows (s105(1)(b));
  3. then with acquisitions "within the period of thirty days after the disposal", earlier before later (s106A(5));
  4. then with the s104 holding (s104; CG51555 rule 3);
  5. finally, any excess "shall be treated as diminishing a quantity of securities subsequently acquired", earlier
     before later (s105(2); CG51555 rule 4).
  s106A(9) bars identification with later acquisitions except under s105 or s106A(5).
- **Residence.** The 30-day rule does not apply to an acquisition made while the person was not UK resident
  (s106A(5A); CG51560). Assumption **A5**: the account holder was UK resident at every acquisition. The ledger holds no
  residence evidence; the assumption is named so that 3d-4 can state it beside the figures.
- **Same class, person and capacity** (s104(1), s105(1); CG51575: "all shares of the same class in the same company"
  form one s104 holding). Identity is the security, not the broker instrument (Design 2).
- **Non-share products.** A CFD is a contract with the broker, not a security acquired (`.claude/CLAUDE.md`, risk
  posture); round-1 finding 5: "Established CFDs belong outside the share-identification population". 3c establishes
  the product (3c Design 4); `excluded_non_share_product` subjects take no part here.
- **Apportionment.** A part disposal from the holding takes cost "by reference to the number of shares sold"
  (CG51575, the practical method). The same share-count basis applies to a partly used acquisition day.
- **Conversion.** Each amount is converted at its own date (CG78310), done per lot by 3c (`amount_gbp`). Matching adds
  and apportions sterling; it never converts.
- **Rounding.** None of the sources above states a rule for rounding intermediate amounts. The scale and mode are local
  policy (Design 6), frozen under the policy id; presentation rounding is 3d-4's.

## Design

### 1. Inputs

The effective generation is the latest `published` `tax_evidence_runs` row whose `(environment, account_id)` equals
`trade_ledger_scope` (3c Design 6). No such run: nothing is written. Its `evidence_policy_id` must be
`broker_evidence_v2` (3d-1 "Consideration classes"); otherwise the run is stored `refused`
(`evidence_policy_unsupported`) with no results. The run reads, in one REPEATABLE READ snapshot taken after its lock
(Design 10): that generation's subjects and lots, its `history_fetch_ok_id` fetch row, and `(instrument_id, symbol,
canonical_instrument_id)` of every `instruments` row, in one statement. `instruments` is mutable and sql/488 does not
refuse a late insert into a published generation, so the inputs the run used are digested into its identity
(`inputs_digest`, Design 9): a later change to either makes a new run, and the earlier run stops being selected.

### 2. Tax security (`tax_security_v1`; finding 29)

Each instrument maps to a tax security id (an `instrument_id`), to *unestablished*, or, with no `instruments` row, to
*unplaceable*. Source: settled decision "Canonical-instrument redirect (#819)": `.RTH` operational duplicates
"represent the same security"; share-class siblings "MUST NOT use this mechanism". #819 binds a variant only when an
operator runs its population job after a universe sync, and allows hand-binding, so a NULL pointer may mean "not yet
bound" and a non-NULL one is re-checked.

The decision is per **component**: the connected set of instruments under two kinds of edge, each followed both ways:
the canonical pointer, and a symbol `X` with a symbol `X.RTH`. Every symbol comparison here, for edges and for the
establishment test below, is case-insensitive (upper-cased). A pointer to a missing row is
itself a defect of the component. A component is **established** when exactly one member has a NULL pointer (the
base), and every other member points to the base and has the base's symbol plus `.RTH`. Then every member maps to the
base. Otherwise every member is *unestablished*, which catches an unbound variant beside its base, a chain (`A → B →
C`), a pointer to a missing row and a symbol mismatch, whatever member carries the evidence.

Product identity stays separate: 3c's `product` decides whether a subject takes part; the tax security decides which
holding it belongs to. Changing the mapping (for example to an ISIN source) is a new policy id.

### 3. Withholding (findings 5, 7)

A security is **withheld**, with no identification, gain or pool, when any condition holds. Each is stored as a
dependency row naming the reason and its cause (Design 9):
- `unresolved_subject`: an unresolved subject's instrument maps to it. An unresolved order may be a real acquisition or
  disposal of the security and change any identification. ILMN's product is NULL, so it could be a real holding.
- `identity_unestablished`: a lot or unresolved subject is on a member of an unestablished component. Every member of
  that component is then its own withheld security row, so nothing reachable from the evidence is computed.
- `lot_unavailable`: a lot has consideration class `unavailable` (3d-1); the dependency names the failing field
  (`execution_evidence_mismatch`, `total_fees_unobserved_null`, `direction_invalid`).
- `subsequent_acquisition_rule`: identification reaches step 5 (s105(2)), which this policy does not implement.
- `unplaceable`: an unresolved subject with a NULL `instrument_id`, or a lot or unresolved subject on an *unplaceable*
  instrument. It cannot be placed, so **every** security is withheld, and the run records it at run level even when no
  security row exists. 0 in run 2.

Withholding is whole-security and not time-scoped: an unresolved subject's dates are among the things not established,
and a withheld disposal leaves every later pool figure of its security unknown (finding 7's propagation).

**Step 5 is unreachable for a computable security.** 3c admits an order whole (3c Design 4); its close units sum to at
most its execution units (units conservation, 3c Design 4.9); and every close is at or after the acquisition
(`close_before_open`, 3c Local policy), so on the London calendar too. Hence for every day *d*, the units acquired on
or before *d* are at least the units disposed of on or before *d*. Acquisitions on or before *d* are consumed only by
disposals on or before *d*: same-day consumes its own day, the 30-day rule consumes acquisitions after their disposal,
and the pool walks in date order. So when *d*'s pool step runs, the pool holds at least *d*'s remainder. An excess at
step 4 therefore means the evidence breaks those 3c properties; the security is withheld rather than identified
under s105(2). In run 2 it withholds GME, CSPX.L, IUSA.L and ILMN (`unresolved_subject`), none of which has an admitted
lot (fact 2), so it removes no computable figure today.

### 4. Lot classes

Each lot gets its consideration class and reasons from 3d-1's table (`consideration_classes_v1`), from its own row
and its subject's row in the same generation, stored per run (Design 9).

### 5. Identification (findings 4, 33)

Per computable security, from its admitted lots, in the Source-rules order, steps 1–4. The unit is the day: one
acquisition day and one disposal day per London date (`uk_date`, 3c Design 4.10), each summing its lots.
- **The statutory disposal** is the security's disposal day. Its broker pieces are kept as evidence (`lot_key`s); the
  gain is computed once on the day: its proceeds less the sum of its identified costs. Pieces that gain and lose on one
  day net inside one statutory disposal (finding 4).
- **Steps** (the 3a matcher's order, `tax_ledger._match_disposals_for_instrument`, moved and reshaped): same day on
  every day first; then the following 30 days, earlier disposal days first, each against the earliest remaining
  acquisition days in `(d, d+30]`; then the pool, walked in date order, each acquisition day's remainder entering on its
  own day.
- **Identification rows** are day-level: rule, acquisition day (NULL for the pool), units, cost. There is no per-lot
  allocation; the statutory figures are the day's (s105(1)(a)), and the lot keys identify the evidence. This replaces
  `disposal_matches`' per-lot rows and their split rounding.
- **Acquisition-only securities (finding 33)** get a pool row (NXH). A security whose pool is empty gets a pool row
  with 0 units and £0.
- **Invariants**, checked before anything is written; a failure raises and the run writes nothing:
  - per acquisition day: same-day units + 30-day units + units entering the pool = the day's units; when its amount is
    defined, the costs withdrawn + the cost entering the pool = the day's amount, exactly;
  - per statutory disposal: the units of its identification rows = its units;
  - the pool: units = Σ entered − Σ drawn ≥ 0 at every step, and likewise cost while defined.

### 6. Amounts and rounding

- `amount_gbp` is exact (`NUMERIC(40,14)`, 3c). Matching runs in `decimal.localcontext()` with `prec = 60` and
  `Inexact` and `Rounded` trapped, so every sum and difference is exact.
- The one inexact step is a partial withdrawal of cost: `units × cost_left / units_left`, from an acquisition day or
  the pool, rounded half-even to `1E-14`. It is computed on scaled integers, never by a decimal division: with `u =
  units × 10^8`, `c = cost_left × 10^14` and `v = units_left × 10^8` (all exact integers), `q, r = divmod(u × c, v)`,
  and `q` is incremented when `2r > v`, or when `2r = v` and `q` is odd. A decimal division at any fixed precision can
  round a value just below a midpoint onto it and then round again (checkpoint 1 round 2, finding 33, gives a case
  within the column capacities). A withdrawal of all remaining units takes the exact remaining cost, so every day's
  and the pool's cost is conserved.
- Units are never divided: every units figure is a sum, difference or minimum of `NUMERIC(20,8)` values.
- **Column capacity.** Stored units are `NUMERIC(24,8)` and sterling `NUMERIC(44,14)`, room for 10,000 maximal lots in
  one aggregate. Every value is checked against its column before the insert; one that does not fit raises and the
  run writes nothing.
- **Missing amounts** (3d-1 precedence 1). A lot with `fx_status = 'pending'` has a NULL `amount_gbp`. Identification
  still runs, because it depends on units and dates only.
  - A day containing such a lot has an undefined amount, and so does every cost withdrawn from it. A day fully
    consumed before the pool contributes nothing to it.
  - The pool's cost is undefined from the entry of an undefined remainder until its units reach zero, when it resets
    to a defined £0.
  - A statutory disposal with undefined proceeds or any undefined identified cost has status `amount_missing` and a
    NULL gain. Each disposal, identification and pool row stores the lot keys whose missing amounts made it undefined.

### 7. The 30-day window (finding 6)

**`window_open`.** A statutory disposal on London date *d* with any units identified other than same-day carries
`window_open` until its window has elapsed: until the `started_at` of the generation's `history_fetch_ok_id` fetch is
at or after London midnight beginning *d*+31. No `ok` fetch: open. The fetch's start is used because the history
request returns trades up to when it is made.
- A disposal fully identified on its own day does not carry it: an acquisition on a later day cannot displace a
  same-day identification (s105(1)(b) before s106A(5)). Late evidence of another same-day piece is a coverage matter,
  below.
- **The pool** carries `window_open`, with the earliest such disposal date as `window_open_since`, while any disposal of
  the security carries it. A later acquisition identified with that disposal under s106A(5) would displace units it
  now takes from a 30-day acquisition or from the pool, so the pool depends on it. Draws before `window_open_since`
  cannot change: their windows have elapsed, and the pool at their dates holds only acquisitions up to them. Depletion
  does not clear it; only elapse does.
- A 30-day acquisition can fall in a later tax year than its disposal (across 5 April). The disposal keeps its own tax
  year; every run recomputes the security's whole history, so such an acquisition changes the earlier year's figure in
  the run that first holds it.

**`history_coverage_unestablished`.** Elapsed time is not coverage: the history endpoint documents no page totals,
coverage before the first recorded `ok` fetch is unknown, and an `ok` fetch reaches back only to its `min_date` (3c
"History completeness"; fact 1). Under this policy every disposal, identification and pool carries this reason; 3d-4
defines the coverage evidence that can discharge it, as a new policy.

"Final" is relative to a generation: a later generation can change a figure. Each run is kept; 3d-4 reports a changed
figure against the previous run.

### 8. Reasons

The vocabulary: 3d-1's consideration reasons, `window_open`, `history_coverage_unestablished` and `character_pending`.
- An acquisition day's reasons are the union of all its lots' reasons; each same-day or 30-day row, and its remainder
  entering the pool, carries all of them (the day is averaged).
- The pool's reasons are the union of every remainder that entered it, plus `window_open` and
  `history_coverage_unestablished` (Design 7). Remainder reasons leave only when the units reach zero (3d-1
  "Propagation"). A pool row of an identification takes the pool's reasons when it draws.
- A statutory disposal takes the union of its lots' reasons, its identification rows' reasons, `window_open` (Design
  7), `history_coverage_unestablished`, and `character_pending`.
- Under `consideration_classes_v1` no figure is `final_eligible`: A1 and A4 always apply (3d-1).

### 9. Storage (migration `sql/489_tax_matching.sql`; finding 31)

The migration first raises unless `disposal_matches` and `s104_pool` are empty (`tax_lots` is already guarded by
sql/485).

**Sealed runs.** Triggers enforce:
- a run row is inserted with state `building`;
- it is updated exactly once, from `building` to `published` or `refused`, changing only `state`, `reason`, `counts`
  and `withheld_all`; a move to `refused` requires that it has no child row;
- a child row is inserted only while its run is `building`;
- a deferred constraint trigger refuses a commit that leaves a run `building`;
- no other update, and no delete, of a run or child row.

So a published snapshot cannot gain, lose or change a row, and a refused run has none. A run is a complete snapshot,
so a security, day or pool absent from it is absent (finding 31).

**`inputs_digest`** is the SHA-256 of a canonical JSON document, serialized as 3c's fingerprint (3c Design 4.10):
`{"version": "matching_inputs_v1", "evidence": [[lot_key, fingerprint], …] sorted by lot_key, "subjects": [[subject,
outcome, instrument_id, execution_evidence, total_fees_unobserved_position_ids], …] sorted by subject,
"instruments": [[instrument_id, symbol, canonical_instrument_id], …]}`. Tuple positions are fixed; only the outer
lists are sorted (and `total_fees_unobserved_position_ids` ascending, as stored). `instruments` holds every member of
every component the run's evidence names (Design 2), sorted by id; a named id with no row appears as
`[id, null, null]`. Exactly those instrument rows are stored in `tax_matching_instruments`. 3d-3 adds its character
and basis inputs as a new document version. The same function computes the digest at publication and at selection.

Every column below is NOT NULL unless it says NULL is allowed, and every child has an FK to its run.
- **`tax_matching_runs`**: `matching_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`; `evidence_run_id` (FK `tax_evidence_runs`);
  `matching_policy_id` (written from `TAX_MATCHING_POLICY_ID`); `class_policy_id` (from 3d-1's
  `consideration_classes_v1`); `inputs_digest`; `inputs_document TEXT` (the exact canonical bytes
  hashed, so the inputs of any run, refused ones included, are reconstructable after a later insert or `instruments`
  change); `state` `building`, `published` or `refused`; `reason`
  (NULL allowed; non-NULL iff refused); `computed_at`; `history_fetch_started_at` (NULL allowed: no `ok` fetch);
  `withheld_all BOOLEAN`; `counts JSONB` (NULL allowed while building). Unique `(evidence_run_id, matching_policy_id,
  class_policy_id, inputs_digest)`; unique `(matching_run_id, evidence_run_id)` for the composite FKs below.
- **`tax_matching_instruments`**: `(matching_run_id, instrument_id)` PK; `symbol` and `canonical_instrument_id` as
  read (NULL allowed: no row, or no pointer); `component_id` (the smallest member id); `tax_security_id` (NULL allowed:
  unestablished or unplaceable).
- **`tax_matching_lot_classes`**: `(matching_run_id, lot_key)` PK; `evidence_run_id`, with FK `(evidence_run_id,
  lot_key)` → `tax_evidence_lots (run_id, lot_key)` and FK `(matching_run_id, evidence_run_id)` → the run; `class`
  `final_eligible`, `provisional` or `unavailable`; `reasons TEXT[]`.
- **`tax_matching_dependencies`**: `(matching_run_id, seq)` PK; `tax_security_id` (NULL allowed: every security);
  `reason` (Design 3); `subject`, `lot_key`, `instrument_id`, `detail` (each NULL allowed, at least one non-NULL).
- **`tax_matching_securities`**: `(matching_run_id, tax_security_id)` PK; `instrument_ids BIGINT[]`; `status`
  `computed` or `withheld`. For `computed`: `pool_units NUMERIC(24,8) ≥ 0`; `pool_cost_gbp NUMERIC(44,14) ≥ 0`, NULL
  iff `pool_missing_lot_keys` is non-empty; `pool_missing_lot_keys TEXT[]`; `pool_reasons TEXT[]`;
  `window_open_since DATE` (NULL allowed). For `withheld`, all five are NULL. Rows: every established security with an
  admitted lot or an unresolved subject, and every member of an unestablished component that holds a lot or an
  unresolved subject, each member its own row with `instrument_ids` = `{member}`.
- **`tax_statutory_disposals`**: `(matching_run_id, tax_security_id, uk_date)` PK, FK to its security row, which must
  be `computed` (checked by trigger); `tax_year`; `lot_keys TEXT[]` non-empty; `units NUMERIC(24,8) > 0`;
  `proceeds_gbp`, `cost_gbp`, `gain_gbp` `NUMERIC(44,14)`, NULL allowed; `status` `computed` or `amount_missing`;
  `missing_lot_keys TEXT[]`; `reasons TEXT[]`. CHECKs, each written `(…) IS TRUE` so that NULL never passes:
  `computed` ⇔ all three amounts non-NULL and `missing_lot_keys` empty, with `gain_gbp = proceeds_gbp − cost_gbp`;
  `amount_missing` ⇔ `gain_gbp` NULL, at least one of `proceeds_gbp` and `cost_gbp` NULL, and `missing_lot_keys`
  non-empty.
- **`tax_identifications`**: `(matching_run_id, tax_security_id, disposal_uk_date, seq)` PK, FK to the disposal;
  `rule` (`same_day`, `bed_and_breakfast`, `s104_pool`, the existing `MatchingRule` values); `acquisition_uk_date`
  (NULL allowed) and `acquisition_lot_keys TEXT[]`: for `same_day` the date equals the disposal's, for
  `bed_and_breakfast` it is in `(d, d+30]`, both with non-empty keys; for `s104_pool` NULL and empty; `units
  NUMERIC(24,8) > 0`; `cost_gbp NUMERIC(44,14)`, NULL iff `missing_lot_keys` is non-empty; `missing_lot_keys TEXT[]`;
  `reasons TEXT[]`.

The cross-row equations (identification units sum to the disposal's; every lot key belongs to the generation and the
security; every missing lot key names a lot with `fx_status = 'pending'`) are the Design 5 invariants, checked in code
before insert and asserted in the DB tests.

**Selecting the result.** `effective_tax_matching(conn)` resolves, in one REPEATABLE READ snapshot, the effective
generation, the current policy ids and the current `inputs_digest`, and returns exactly that run. No such run, or a
refused one, means *unavailable*: there is no fallback to an older run. 3d-4's readers use it.

### 10. A run and the job

`run_tax_matching(conn)` in a new `app/services/tax_matching.py`; the identification is a pure function over one
security's lots and classes.
1. Take the session-level `pg_advisory_lock(TAX_MATCHING_LOCK_KEY)` outside any transaction, then begin a REPEATABLE
   READ transaction, so its snapshot is taken after the lock and sees every earlier run's commit. The body runs in
   `try`/`finally`; the `finally` rolls back any open transaction and releases the lock.
2. Resolve the effective generation and read the inputs (Design 1); compute `inputs_digest`. No generation: return
   `no_generation`. A run exists for the key: return its state and reason (`published` → `already_published`,
   `refused` → `refused` again, so a refusal stays visible).
3. Classify, map, withhold and identify in memory (Designs 2–8); check the invariants and capacities.
4. Insert the run `building` and its rows; seal it; commit. A refusal is a decided outcome, not an exception: the run
   is inserted `building` with no children and sealed `refused` in this same transaction, which commits, so the refusal
   is stored and visible. Any exception rolls the whole transaction back, so no parent or child row of a failed run
   remains; the exception propagates and the job records the failure (`_tracked_job`), which is how a failed seal is
   told apart from a refusal.

Matching is guaranteed only for the generation effective when a run resolves its inputs. A generation superseded
before then is never matched; while it was effective and unmatched, `effective_tax_matching` returned *unavailable*.

**The job.** `tax_broker_evidence_refresh` runs its evidence step as today, capturing the outcome instead of raising,
then runs `run_tax_matching` on a fresh connection, then raises if either failed: an evidence `refused` or exception,
or a matching `refused` or exception, with both outcomes in the message. Matching runs even when the evidence step
failed: it then matches the still-effective earlier generation if no run exists for its key, and the job stays failed.
`daily_tax_reconciliation` keeps its registrations and becomes the on-demand invoker of `run_tax_matching` alone.

### 11. Retiring the legacy write path

- Deleted from `tax_ledger.py`: `ingest_tax_events`, `_ingest_cash_events`, `run_disposal_matching`, and the matcher
  (`_match_disposals_for_instrument`, `_split_exact`, `_DayIdentification`, `TaxLot`, `DisposalMatch`,
  `MutablePoolState`, `PoolState`, `IngestionResult`, `MatchingResult`). The algorithm moves to `tax_matching.py`; the
  matcher's pure tests move with it.
- **Kept until 3d-4:** `tax_lots`, `disposal_matches`, `s104_pool`, the sql/485 insert-refusal trigger, and their
  readers (fact 5). They are empty and now have no writer, so they serve empty, as they do today. 3d-1's table gave
  "retiring `tax_lots`" to 3d-2; dropping the tables goes with replacing their read surface, which is 3d-4's.
- **The budget's CGT reserve** reads `disposal_matches` and so reserves £0 for gains computed here; it has reserved £0
  since the fills route was retired in 3c. 3d-4 must give it an explicit *unavailable* state from
  `effective_tax_matching` and define the reserve while gains are `character_pending` or provisional, before dropping
  the table. The deferral rests on the demo scope (3c Limits), the consumer being unchanged by this slice, and the
  explicit 3d-4 requirement; run 2's computed gains happen to be under £1, which bounds nothing later.

## Local policy (frozen under `TAX_MATCHING_POLICY_ID = "share_identification_v1"`)

`tax_security_v1` (Design 2); whole-security withholding and step 5 withheld (Design 3); the day as the unit and no
per-lot allocation (Design 5); the rounding scale and mode and the column capacities (Design 6); the window's elapse
instant, its same-day exemption and pool propagation (Design 7); `history_coverage_unestablished` always (Design 7);
assumption A5. Changing any is a new policy id, which the run key then admits as a new run for the same generation.

## Not in this slice

- **3d-3:** tax character (`character_pending` is removed there), OIG routing on the statutory disposal, excess
  reportable income as a basis adjustment, dividends. Its inputs enter `inputs_digest`, so a change makes a new run.
- **3d-4:** the tax-year view and page, coverage evidence, change reporting between runs, A5 stated beside the
  figures, dropping the legacy tables and re-pointing their readers including the budget reserve, no figure for a
  `real` scope.
- Short positions (3c admits long x1 only), reorganisations (s127), rights (s122–123), s105(2).

## Tests (finding 42)

Pure, on the identification function, each figure computed by hand in the test:
1. **The three 3a defects stay fixed:** same-day acquisitions averaged (£100 + £300 for 20 units, 10 sold same day:
   cost £200); same-day priority over an earlier disposal's 30-day claim; 20 bought, 10 sold twice the same day: pool
   0. The other existing matcher tests move with the algorithm.
2. **Netting (finding 4):** two pieces on one day, one above and one below cost: one disposal, one net gain.
3. **Competing disposals:** two disposals 10 days apart and one acquisition inside both windows: the earlier takes it
   first (s106A(5)).
4. **Window boundaries:** with an opening pool, an acquisition on *d*+30 is identified 30-day with *d*'s disposal; on
   *d*+31 it is not: *d*'s disposal draws from the opening pool, and the *d*+31 acquisition then enters the pool on
   its own day. Elapse: disposal 2026-09-23, fetch started
   2026-10-23 22:59:59Z open, 23:00:00Z elapsed (BST); disposal 2026-11-20 elapses 2026-12-21 00:00Z (GMT); disposal
   2026-10-10, whose window spans the 2026-10-25 clock change, elapses 2026-11-10 00:00Z; no `ok` fetch: open. A fully
   same-day disposal carries no `window_open`.
5. **Pool window:** an open-window disposal drawing on the pool, the pool depleted to 0 and replenished, all before
   elapse: the pool keeps `window_open` with `window_open_since` the disposal's date; it clears at elapse.
6. **Across 5 April:** disposal 2027-03-20, acquisition 2027-04-10: `bed_and_breakfast`; tax year 2026/27.
7. **Late evidence:** generation N without the 04-10 acquisition draws the 03-20 disposal from the pool; N+1 with it
   matches 30-day. Two runs, both kept, different figures.
8. **Missing amounts:** pending disposal proceeds; a pending acquisition inside a window; mixed defined and pending lots
   on one day; a pending day fully consumed same-day (the pool stays defined); a pending remainder in the pool (later
   pool costs undefined, `missing_lot_keys` named, a defined £0 after depletion).
9. **Reasons:** 3d-1's case (10 `execution_assumed` and 10 not, 10 sold: the pool keeps it; at 0 it clears); a mixed
   acquisition day partly same-day, partly 30-day, remainder to the pool: all three carry the day's union.
10. **Classes:** each 3d-1 predicate alone and combined; subject/lot `execution_evidence` disagreement and NULL
    `total_fees_unobserved_position_ids` give `unavailable` and withhold with the field named; a generation changing
    only a subject's unobserved positions changes the class with an unchanged lot fingerprint.
11. **Mapping:** an established component pools a bound variant with its base; unestablished components: an unbound
    `.RTH` variant beside its base, a chain `A → B → C` with the evidence on `A` and admitted lots on `C` (all three
    withheld), a pointer to a missing row, a symbol mismatch; a bound `spy.rth` variant of `SPY` (established); a lot
    on an id with no row (`unplaceable`).
12. **Step 5:** lots violating 3c's properties (a disposal with no prior acquisition) withhold the security with
    `subsequent_acquisition_rule`.
13. **Rounding:** a pool drawn in three thirds conserves cost exactly; a partial withdrawal rounds half-even at `1E-14`,
    including an exact tie and round 2 finding 33's near-tie case (`U = 10^24 − 1`, `N = (U − 1)/2`, `C = (10^20 − 1)
    × U + 1` as scaled integers: expected `(10^20 − 1) × N` quanta); a value beyond its column raises.

DB, one per mechanism:
14. **Run:** one published run per key; a second call returns `already_published`; another evidence policy gives one
    `refused` run, and the next call returns `refused` again; an `instruments` change gives a new digest and a new run.
15. **Seal:** sealing `building → published` succeeds; a move to `refused` after a child insert, a child insert into a
    published or refused run, a second state change, any other UPDATE, a DELETE, an insert not `building`, and a commit
    leaving `building` are all refused. An exception after some child inserts leaves neither run nor child.
16. **Evidence change:** a late insert into a published generation's lots, and separately its subjects (possible under
    sql/488), changes the digest; the old run is no longer selected, its `inputs_document` still reproduces its own
    digest, and the next call makes a new run. A pinned fixture (a missing instrument, a subject with no lot) fixes the
    document's bytes and SHA-256.
17. **Withholding:** an unresolved subject withholds its security and names itself; an `unplaceable` subject with no
    admitted lot anywhere sets `withheld_all` with no security rows; a security computed in run N and withheld in N+1
    has no pool or disposal in N+1; a security whose only lots leave the generation in N+1 has no row in N+1.
18. **Concurrency:** two callers on one generation produce one run; a call while a new generation publishes matches
    the generation it resolved.
19. **Selection:** `effective_tax_matching` returns the exact run, and *unavailable* when the effective generation has
    no run, or a refused one, even though an older published run exists; a published generation of another scope is
    never selected.
20. **Job:** an evidence refusal still runs matching and fails the job; a matching refusal fails it.
21. **Smoke:** `/tax/*` and the budget still serve (empty legacy tables) after the deletions.

## Acceptance (dev DB, finding 43)

After the build is deployed, `run_tax_matching` over the effective generation gives, for run 2 (for a later generation,
rerun the Script and re-derive these figures from its output the same way):

| security | broker pieces | statutory disposals | identifications | expected |
|---|---|---|---|---|
| AAPL (1001) | 2 | 1: 2026-09-23, 2026/27 | 1 `same_day` 0.14707100, cost £37.69317753500000 | proceeds £37.68563889949300, gain **−£0.00753863550700**; reasons `character_pending`, `disposal_formula_assumed`, `history_coverage_unestablished`, `spread_inclusion_assumed`, `sync_charges_unobserved` |
| SPY (3000) | 1 | 1: 2026-09-23, 2026/27 | `same_day` 0.29220900, cost £169.30267621620600; `bed_and_breakfast` against 2026-09-30, 0.00394600, cost £2.28297661753705 | proceeds £172.44628722262500, cost £171.58565283374305, gain **£0.86063438888195**; reasons as AAPL's without `sync_charges_unobserved`, plus `window_open` |
| AAPL pool | | | | 0 units, £0, reasons `history_coverage_unestablished` |
| SPY pool | | | | 21.66720200 units, £12537.70775155264395, reasons `history_coverage_unestablished`, `spread_inclusion_assumed`, `window_open`, since 2026-09-23 |
| NXH pool | 0 | 0 | 0 | 1305.05709600 units, £6048.55403340328000, reasons `execution_assumed`, `history_coverage_unestablished`, `lookup_evidence_unobserved`, `spread_inclusion_assumed` |
| GME, CSPX.L, IUSA.L, ILMN | | | | `withheld`, `unresolved_subject`, one dependency per subject (2, 3, 1, 1) |

Totals: 3 broker disposal pieces, 2 statutory disposals, 3 identification rows, 7 security rows (3 computed, 4
withheld), 7 dependency rows, `withheld_all` false. The 30-day cost is 0.00394600 × 11933.68460630607000 /
20.62672000, quantized to `1E-14` half-even. SPY's window elapses at 2026-10-23 23:00Z: the first generation whose `ok`
fetch starts at or after that instant drops `window_open` from SPY's disposal and pool.

## Full-population verification

The population is every subject (21) and lot (11) of run 2 and every instrument row they name (Script). The
grouping, mapping, withholding and day totals above come from its output, not a sample; the expected figures are
hand-derived from those totals and were recomputed independently at checkpoint 1.

## Finding dispositions

| finding | where |
|---|---|
| 4 statutory daily disposal | Design 5; test 2 |
| 5 withholding | Design 3; tests 11, 12, 17 |
| 6 window, late evidence, across tax years | Design 7; tests 4–7 |
| 7 complete quantity and its propagation | Design 3 (step 5 unreachable, else withheld); test 12 |
| 29 tax-security identity | Design 2; test 11 |
| 31 obsolete keys | Design 9 (sealed whole snapshots, exact selection) |
| 33 acquisition-only pools | Design 5; acceptance (NXH) |
| 42 expected-result tests | Tests |
| 43 per-disposal tax year and counts | Acceptance |

## Security

No order or authorisation path changes. The matching run is a DB-only derivation over stored evidence. The one capital
consumer, the budget's CGT reserve, is unchanged by this slice and its re-pointing is a stated 3d-4 requirement
(Design 11). The Script is read-only and prints no account identifier.
