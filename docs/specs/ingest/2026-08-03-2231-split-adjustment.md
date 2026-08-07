# Split adjustment for stale ownership rows (#2231)

Ownership rows are stored at the share count reported on their filing date and
compared against a present-day `shares_outstanding`. After a reverse split the
two are in different units, producing the 100x-350x insider ratios in the #2226
cohort (GWAV 266,252,068 held vs 829,631 outstanding = 32,092.8%).

> ⚠ **PREMISE FALSIFIED 2026-08-07 — the corporate-actions table already exists.**
> The sentence that stood here ("There is no splits or corporate-actions table
> anywhere in the schema") was true when this spec was written on 2026-08-03 and
> was false four days later. `sql/246_price_adjustments_and_series_breaks.sql`
> (#2261, phase 0a of #2240, from the S7 verdict on #2247 §7/§8) shipped
> **`price_adjustments`** and **`price_series_break`**, both applied to the dev
> DB. `price_adjustments` is empty (0 rows, 0 instruments) and its `source`
> column comment reserves the value **`sec_xbrl (#2231)`** for this detector.
>
> This spec therefore does **not** create a `corporate_actions` table. See
> "Storage: `price_adjustments`, not a new table" below — that adoption answers
> most of open item 2 and narrows open item 1, but it does **not** close event
> identity; see the two ⚠ blocks there.

## Source rule

**ASC 260-10-55-12** requires a splitting filer to retroactively restate
per-share and share-count disclosures for **all prior periods presented**, in
post-split units. **ASC 805-40** imposes the same retroactive restatement for
reverse acquisitions, denominated by the exchange ratio.

Consequence, and the whole basis of the detector: the same
`(instrument, concept, period_start, period_end)` reported at two different
`filed_date`s with two different values can only be a split, a
recapitalisation, or a filer error. **Issuance cannot move a past period** —
that is why this axis works where the drop-magnitude discriminator (which
compares *different* periods) was falsified on #2226.

Concepts carrying the signal, both already in `_ALL_TRACKED_TAGS`:
`CommonStockSharesOutstanding` (instant),
`WeightedAverageNumberOfSharesOutstandingBasic` (duration).

⚠ `StockholdersEquityNoteStockSplitConversionRatio` is **absent** from
`_ALL_TRACKED_TAGS` (`app/providers/implementations/sec_fundamentals.py`), so
its coverage is zero by construction. Not a candidate without an ingest change.

✅ `dei:EntityCommonStockSharesOutstanding` **is now in the detector's concept
set** (added 2026-08-07, since the rollup denominator path reads DEI/us-gaap
outstanding and a mismatch of basis reproduces the #2217 class of bug). It is
thin — 106 of 9,713 pairs over 81 instruments — because companyfacts returns no
non-dimensional value for multi-class issuers at all (sec-edgar §7.17), which is
the same limitation that bounds open item 1.

**ASC 805-40 is in this section for detection only.** What a reverse-acquisition
restatement means for an ownership row is settled in open item 7 below, and the
answer is that it must not be applied.

## Full-population verification

Scan over `financial_facts_raw`, all instruments, no sampling.

> ⚠ **The two blocks below are the 2026-08-03 measurement and are HISTORICAL,
> not normative.** They were taken before `taxonomy`/`unit` entered the partition
> key, before the materiality floor and before `dei` was added, so they disagree
> with the corrected scan (9,713 pairs / 1,466 instruments; median intersected
> bracket **207 d**, not 273 d). Retained because the GWAV cross-source check and
> the three-known-errors check still stand; every count in them does not.

**Detector (5,536 rows / 1,288 instruments, per the #2231 evidence comment):**
90.1% of ratios are round integers within 1%; 96.4% round to
integer/half/quarter. Zero fires on AVAL (1.08e9x), ALCYF (3.79e6x) and PKG
(1,001x) — the three known XBRL errors that killed the magnitude
discriminator. Cross-source: GWAV 1-for-150 eff. 2024-05-31 and 1-for-110 eff.
2025-08-22 (PRNewswire), our derived 150.001 / 110.000, FINRA `stock_split_flag`
`'S'` on settlement 2025-08-29.

**Bracketing (this spec, measured 2026-08-03):**

| bracketing method | median bracket width |
| --- | --- |
| per instrument (`min(first_filed)`→`max(last_filed)`) | 637 d |
| per consecutive filing pair | 363 d |
| **intersection across a `(instrument, ratio)` cluster** | **273 d** |

Consecutive pairs alone help less than expected, because the same period tends
to be re-filed about a year later — that is the shape of the 363 d median, not a
separately measured claim about filing habits. The gain comes from
**intersecting**: an event that genuinely describes one split must contain that
split's true effective date, so the brackets can be intersected. ⚠ "Every event"
is the *premise* of intersecting, and it is exactly what the 130 empty
intersections (item 5) put in doubt — a scale artefact or a second split merged
into the cluster breaks it. GWAV's 1-for-150 gets a 91-day bracket from
`CommonStockSharesOutstanding` (2024-05-20 → 2024-08-19) which contains the true
2024-05-31 date, against 371 days from the weighted-average concept alone.

**Blast radius, per row, after intersected bracketing, on the `period_end`
as-of date (NOT `filed_at` — see the as-of rule below). Re-measured 2026-08-07
on the corrected detector (`taxonomy`/`unit` in the partition key, ±10%
materiality floor, `dei` concept included) and now covering the overlays open
item 6 said were missing:**

| source | rows | ambiguous (fail closed) | certain pre-split (adjust) | instruments touched |
| --- | ---: | ---: | ---: | ---: |
| institutions | 2,313,145 | 34,449 | 11,526 | 572 |
| **funds** | **1,148,022** | **19,108** | **8,070** | **354** |
| insiders | 93,315 | 2,992 | 5,839 | 907 |
| def14a | 66,320 | 2,202 | 1,907 | 412 |
| blockholders | 23,045 | 1,101 | 398 | 505 |
| **treasury** | **1,847** | **53** | **76** | **129** |
| **esop** | **44** | **1** | **1** | **2** |
| **drs** | **19** | **0** | **0** | **0** |
| **total** | **3,645,757** | **59,906** | **27,817** | — |

⚠ The three bolded overlays were absent from the 2026-08-03 version of this
table. Adding them moves the row base **2,480,876 → 3,645,757 (+47.0%)** — that
half of the delta is clean, because the four original tables are unchanged to
within four days of corpus growth (2,495,825 today). `ownership_funds_current`
alone is 1,148,022 rows and contributes 27,178 touched rows, more than the whole
of insiders + blockholders + def14a combined. **The touched total is not a clean
A/B against the old 38,955**: this scan also changed the detector (floor,
partition key, `dei`), so the two deltas are mixed and neither is separable from
these numbers alone.

⚠ **`ownership_drs_observations` breaks the "every table carries `period_end`"
assumption** stated in the as-of section below — its as-of column is
`as_of_date`. It is 19 rows / 2 instruments and 0 are touched, so it costs
nothing today, but a read path that assumes the column name is uniform will
raise rather than degrade. `nonvested` has **no table**: it is a memo computed
from the dimensional/FSNDS facts, so it is out of scope by construction rather
than by omission.

⚠ Measuring on `filed_at` instead understates this by 37% (19,215 / 9,141) and
reports **DEF 14A as entirely unaffected (0 certain)** when it in fact carries
1,469 rows needing adjustment — the proxy record date precedes the filing date
by enough to cross a bracket. Any re-measurement must use `period_end`.

**Blockholder coverage is floored at 2024-12-18** — `min(filed_at)` on
`ownership_blockholders_current` is exactly the Schedule 13D/G structured-XML
mandate date (sec-edgar skill §2.4.1; pre-mandate filings are HTML-only and PR11
of #1233 chose a hard date floor). No pre-mandate blockholder row is
**represented in that table**, so its figures are complete rather than truncated
for the period it covers. ⚠ That is not the same as "cannot carry a pre-2024
split problem": a post-mandate row can still describe a stale pre-split position
through its own `period_end`, and the 2026-08-07 blast radius does find 398
certainly-pre-split blockholder rows.

**13F PRN rows are already excluded at ingest**
(`app/services/sec_13f_dataset_ingest.py:729-736`, PR #1054 — 20k PRN rows in
2026Q1 alone). This matters here because PRN rows carry bond principal in
dollars, not shares; rescaling one by a split ratio would be meaningless. No
additional guard needed in this spec, but the exclusion must not be removed.

## Design

### Storage: `price_adjustments`, not a new table

Ratios are captured **as observed**. The detector is an ongoing observer, not a
history reconstructor — retention (`app/services/financial_facts_retention.py:44-49`,
latest 3 10-K + 8 10-Q accessions per instrument) permanently removes pre-window
figures, which is why ARMP's real 998x produces no restatement row and RTB's
2018 / GNLN's 2019 factors cannot be derived. Once a ratio is seen it is the
system of record and is never re-derived.

The system of record is **`price_adjustments`** (`sql/246`), written with
`source = 'sec_xbrl'`. That table already supplies everything open items 1 and 2
asked for:

| open item asked for | `sql/246` already has |
| --- | --- |
| a declared PK and event id | `adjustment_id BIGSERIAL PRIMARY KEY` |
| correction / tombstone state | `superseded_by` + `superseded_at`, append-only, with the self-reference and paired-nullability CHECKs |
| source accessions | `source_document_id`, `source_event_date`, `evidence_json` |
| arbitration when two sources disagree | `source_priority` + the partial unique index `price_adjustments_active ON (instrument_id, effective_date) WHERE superseded_by IS NULL` |
| "captured as observed, never re-derived" vs later evidence | `observed_at` is the replay pin; a narrowed bracket SUPERSEDES rather than UPDATEs |
| detector identity | `detector_version TEXT` — rule-set id + code hash, explicitly not an int |

⚠ **Factor direction: the two conventions agree numerically and differ in
application. Do not "fix" one to match the other.** `price_adjustments.factor`
multiplies price bars strictly *before* `effective_date`; this spec's `ratio =
earliest_val / latest_val` divides ownership share counts. For a 1-for-10
reverse split both are **10**: the price bar reads 10x higher, the share count
reads 10x lower. `factor == ratio`; only the operator differs, because price and
share count move in opposite directions through the same event. Both `sql/246`
and this spec were written after Codex caught this exact inversion, and both
carry the warning — the failure is invisible on a chart because the series stays
internally consistent, off by `factor²`.

⚠ **CONTRACT GAP, and it needs the TA epic's agreement rather than a unilateral
decision here.** `price_adjustments.effective_date` is a single DATE — "first
bar at the NEW scale". This detector cannot produce a date; it produces a
**bracket** (median 207 days, measured below). There is nowhere in `sql/246` to
record that uncertainty, and `price_adjustments_active` is keyed on the exact
date, so two detectors that bracket the same split differently would not
collide and would both stay active. Proposal, to be agreed with the #2240 owner
before any write: `effective_date = bracket_end` (the first date by which the
new scale is certainly in force), `confidence = 'inferred'`, and the full
bracket in `evidence_json`. **The ownership read path must then key off the
`evidence_json` bracket, not `effective_date`** — using the point date would
silently reclassify every row inside the bracket from "ambiguous" to
"certainly pre-split", which is the failure this spec's fail-closed rule exists
to prevent.

⚠ **`kind` never filters DETECTION; it does filter APPLICATION.** The original
form of this rule ("`kind` classifies; it never filters") is too flat and open
item 7 below is why. ASC 805-40 reverse-merger exchange ratios are real
accounting restatements, so roundness must never be an *admission* gate at
detection time — and they are **also** the one class that must not be applied to
an ownership row. Detect and record everything; gate on `kind` at the read path.

⚠ `price_adjustments.kind` is unconstrained `TEXT` (`sql/246:38`) — no CHECK, no
enum. An application rule that turns on `kind = 'reverse_acquisition'` fails
open on a typo, in the direction that rescales rows it should not. Either a
CHECK constraint is added (a change to a table this spec does not own) or the
read path treats any unrecognised `kind` as non-applicable.

### Detector

1. Pair scan: `lag(val)`/`lag(filed_date)` over
   `PARTITION BY instrument_id, taxonomy, unit, concept,
   COALESCE(period_start,'0001-01-01'), period_end
   ORDER BY filed_date, accession_number`, emitting
   `(prev_val, val, prev_filed, filed_date)` where the value changed.
   ⚠ `taxonomy` and `unit` are identity columns on `financial_facts_raw` and
   **must** be in the partition key — comparing across units manufactures
   ratios out of a unit change. `accession_number` is the tie-break, because
   `filed_date` alone is nondeterministic when two accessions land the same day.
   ⚠ Needs a **materiality floor**: "value changed" with no threshold turns a
   rounding restatement near 1.0 into a corporate action that then rescales
   ownership rows. Floor to be set from the full-population ratio distribution,
   not chosen by hand.
   ⚠ `period_start` **must** be in the partition key — on `period_end` alone a
   9-month YTD weighted average is compared against a 3-month quarter and
   manufactures bogus ~2x ratios.
   ⚠ Guard `prev_val > 0 AND val > 0` — a period first reported as zero divides
   by zero and inverts.
2. Cluster by `(instrument_id, <log-scaled ratio key>)`, bracket =
   `[max(bracket_start), min(bracket_end)]`.
   ⚠ **NOT `round(ratio, 2)`.** That key collapses every ratio below 0.005 into
   one `0.00` bucket — 33 forward clusters in the 2025-04-30 → 2026-07-15 window
   alone, mixing 10⁻³/10⁻⁶ scale artefacts with genuine high-ratio events. Use
   `round(ln(ratio)/ln(10), 3)` or an equivalent relative-tolerance key. See
   open item 5.
3. **130 clusters intersect empty** (2026-08-07; the 2026-08-03 figure of 38 was
   measured on the collapsing key). The spec's reading is that these are two
   genuinely distinct splits at the same ratio and must be time-partitioned into
   separate events, never merged — ⚠ still an assertion, not a measurement, and
   it cannot be tested until the key is fixed, because a merged artefact
   manufactures the same symptom.
   ⚠ **Ordering is undefined when two brackets overlap.** "Applied in
   chronological order" (below) assumes a total order on effective dates;
   brackets give a partial one. Two overlapping brackets on the same instrument
   need an explicit rule — fail closed for any row inside the union is the
   conservative default — before the read path is written.

### Application at read time

For an ownership row with `filed_at`:

**Ratio is defined as `earliest_val / latest_val`** — pre-restatement over
post-restatement. For GWAV's 1-for-150 reverse split that is 150.001, a number
greater than 1. A stale holder row is therefore **DIVIDED** by the ratio to
reach post-split units:

```text
266,252,068 / (150 × 110) = 16,136 shares → 16,136 / 829,631 = 1.95%
```

which is the figure on the ticket. Multiplying would move the row the wrong way
by a factor of ratio².

- `as_of < bracket_start` for a cluster → **certainly pre-split**; divide by the
  product of every such cluster's ratio, applied in chronological order.
- `as_of BETWEEN bracket_start AND bracket_end` → **ambiguous**; fail closed.
- otherwise → untouched.
- A row may be certainly-pre for one cluster and ambiguous for another. Any
  ambiguous cluster in the applicable set makes the whole row ambiguous —
  a partially-adjusted count is worse than a suppressed one.

⚠ **`as_of` is `period_end`, NOT `filed_at`.** Seven of the eight sources in
the blast-radius table carry `period_end NOT NULL`, which is the date the share
count describes; the eighth, `ownership_drs_observations`, calls the same thing
`as_of_date`, so the read path cannot assume a uniform column name.
`filed_at` is when the document reached EDGAR;
`filed_at` is when the document reached EDGAR. A split landing between the two
misclassifies the row.

| source | what `period_end` holds | governing rule |
| --- | --- | --- |
| Form 3/4/5 insiders | `<periodOfReport>` / transaction date | Exchange Act §16(a); sec-edgar §2.3 |
| 13F institutions | quarter end, filed up to 45 days later | Rule 13f-1(a)(1); sec-edgar §2.1 |
| 13D/G blockholders | event date | Rule 13d-1; sec-edgar §2.4 |
| DEF 14A | record date stated in the table | Item 403 Reg S-K |

Measured on the corpus, the institutional `filed_at − period_end` gap is a
**median of 39 days and a maximum of 697**. Against a 273-day median bracket, a
39-day shift is enough to move a row between "certainly pre-split" and
"ambiguous", which is why the blast-radius table above is measured on
`period_end`.

⚠ **`no_data(reason=…)` vocabulary is closed.** The current type admits only
`absent | stale_denominator`; `split_ambiguous` is a NEW variant and is a
contract change across the Pydantic model, the API schema and the frontend
union. Per the 2026-08-03 lesson (a closed vocab in three places returned 500s
on a live endpoint with the entire suite green, because a Pydantic `Literal`
only runs at serialization), it needs the derive-by-AST contract test, not just
a type edit.

⚠ **Granularity is unresolved.** The existing `no_data` path suppresses the
whole rollup payload. Whether one ambiguous holder suppresses the row, the
category, or the payload is a product decision that must be fixed before
implementation.

⚠ **Read-time application may be too late.** If unadjusted raw counts already
influenced dedupe / winner selection upstream of the `_current` tables, adjusting
after selection preserves a wrong winner. Verify where the `_current` winner is
chosen before settling on read-time.

⚠ **A read-time metric needs a LATENCY arm** (2026-08-03, #2229): the correct
predicate ran 11-20x slower there (180ms → 3.7s) and a missing index made the
correct source measure 3x worse. Benchmark before adopting.

## ⚠ Open: the pre-retention gate premise is falsified

Decision 2 on-issue is that rows older than the derivable window fail closed,
rationale *"a 2018 holder row is also 8 years stale — split-adjusting it would
produce a precise-looking but meaningless percentage"*.

Implemented as `filed_at < min(financial_facts_raw.filed_date)` for the
instrument, that gate rejects **15,985 insider rows**, of which only **2,827**
are on an instrument with any split cluster at all. Worse, the rejected set is
not the stale tail the rationale assumes:

| median filed | p90 filed | newest filed | mean age |
| --- | --- | --- | --- |
| 2021-04-01 | 2025-05-28 | **2026-06-12** | 4.8 y |

`min(financial_facts_raw.filed_date)` is not a uniform horizon — for a thin or
recently-listed filer it is recent, so rows filed *last month* fall "before" it.
The rationale holds at the median and breaks in the tail.

This is the #2182 failure shape (a discriminator that reads correctly and
deletes 9,236 legitimate rows), so the gate is **not specced as blanket
suppression**. Proposed instead: fail closed only where the row is pre-window
**and** the instrument shows split evidence or an implausible implied stake —
which is the #2226 oversubscribed signal the ticket exists to serve. The precise
predicate needs one more full-population pass before it is fixed here.

## Verification plan

Corpus change → full-population A/B (`full-population-ab.md`) and Definition-of-Done
clauses 8-12.

- Distinct-entity metric (instruments with a corrected stake), never row count.
- Inspect the **gain** side: rows whose percentage moves, not just the count.
- Arms paired in **one snapshot** — the jobs daemon rewrites ownership rows
  between sequential arms (#2217's first A/B was invalidated exactly this way).
- Invariant on the **total pie**, not one slice — a share removed from one
  ownership class reappears in another (#2229: VEON inst −73.9M / blk +73.2M).
- Panel: GWAV (two known splits, cross-source confirmed), GPUS, AAPL, GME, JPM.
- Cross-source: GWAV against PRNewswire + FINRA `stock_split_flag`.
- Acceptance is **per-axis** — #2226 prevalence → 0 is not the bar, since most
  instruments are oversubscribed for more than one reason at once.

## Codex checkpoint 1 — accepted, still open

Fixed inline above: ratio direction (was multiply, must be divide), tracked-tags
file path, `taxonomy`/`unit` in the partition key, same-day accession tie-break,
materiality floor, `as_of`-not-`filed_at` per source, `split_ambiguous` as a
closed-vocab contract change, mixed certain/ambiguous precedence.

The seven items that stood here were worked on 2026-08-07. Every figure below
was computed in that session against the dev DB; none is carried over. The
corrected pair scan is in "Reproduction" at the end.

### Item 2 — mostly answered by adoption. Item 1 — narrowed to a shared question.

`sql/246` supplies the PK, event id, correction state and arbitration (table
above), so item 2 does not need a new table. **It is not fully closed, and two
residues matter:**

⚠ **`source_document_id` is singular; a detector cluster rests on ≥2 accessions
and often many** (600 of 2,916 clusters span more than one concept, and the
median cluster carries more than one event). Putting the rest in `evidence_json`
is not a queryable audit trail — "which filings established this factor" cannot
be answered by a join. Either the accession set is normalised into a child
table, or the spec states plainly that the audit trail is JSON-only.

⚠ **The active-row unique index does not give event identity.**
`price_adjustments_active` is keyed on the exact `(instrument_id,
effective_date)`, while this detector emits brackets. Two detectors — or two
runs of this one on a narrowed bracket — that place the same economic split on
different dates do **not** collide, both stay active, and the price read path
multiplies them together. That is the same double-apply the index exists to
prevent, arriving through the door the index does not watch.

Item 1 (multi-class / ADR-ratio granularity) is **not** closed: `sql/246` is
keyed on `instrument_id` too, so this is now a shared limitation of the
price-adjustment substrate rather than something #2231 can settle alone. Two
facts bound it: our `instruments` table carries no CIK or share-class column, so
the class dimension is not expressible today; and sec-edgar §7.17 records that
`financial_facts_raw` holds only the **combined** us-gaap count for multi-class
issuers, because companyfacts strips dimensional facts. So a per-class ratio is
not derivable from the detector's own source either. **Scope note, to be agreed
with #2240: `price_adjustments` rows are issuer-level; a multi-class issuer that
splits one class only is out of scope and must not be written.**

### Item 4 — ANSWERED. The claim is too strong, and the false-positive class is named.

Full population, 3 concepts, corrected partition key: **9,713** same-period
value-change pairs over 1,466 instruments.

| ratio band | pairs | instruments |
| --- | ---: | ---: |
| within 0.1% | 1,763 | 486 |
| 0.1–1% | 640 | 255 |
| 1–10% | 982 | 301 |
| 10–50% | 302 | 161 |
| shrink >1.5x | 4,247 | 837 |
| grow <0.667 | 1,779 | 449 |

**3,385 of 9,713 (34.8%) sit within ±10% of 1.0.** The distribution is bimodal.
Log-spaced histogram, 28 buckets of 0.05 in `ln(ratio)` over `[-0.7, +0.7]`
(`width_bucket(ln(ratio), -0.7, 0.7, 28)`, restricted to `ratio BETWEEN 0.4966
AND 2.0138`):

| approx ratio | pairs | | approx ratio | pairs |
| ---: | ---: | --- | ---: | ---: |
| 0.5092 **(2:1)** | 327 | | 0.9753 | **1778** |
| 0.5916 | 19 | | 1.0253 | **1385** |
| 0.6219 | 4 | | 1.0779 | 73 |
| 0.6538 **(3:2)** | 135 | | 1.1912 | 15 |
| 0.6873 | 28 | | 1.3165 | 9 |
| 0.7985 | 38 | | 1.4550 | 10 |
| 0.8825 | 59 | | 1.6080 | 2 |
| 0.9277 | 144 | | 1.9640 **(1-for-2)** | 45 |

The noise mode is the 3,163 pairs in the two buckets spanning 0.975–1.025; the
split modes are the 2:1, 3:2 and 1-for-2 spikes. The trough on the high side is
1.19–1.61 (2–15 per bucket); on the low side it is a plateau of 4–59 per bucket
from 0.62 to 0.88, not a clean trough. **±10% is a materiality cutoff chosen to
sit above the noise mode and below the 3:2 spike, not a source-rule
impossibility** — and it knowingly discards a class:

⚠ Under **ASC 505-20-25-3** a distribution below ~20–25% of shares outstanding
is a **stock dividend**, not a split, and ASC 260 still requires retroactive
restatement of prior-period share counts for it. So a 5% stock dividend is a
genuine unit change that lands inside ±10% and this floor drops it. The
justification is materiality, not impossibility: a 5% mis-scaling cannot produce
the 100x–350x #2226 ratios this spec exists to fix, and admitting it would mean
admitting the 3,385-pair noise mode with it. **State that trade-off in the
detector's docstring; do not let it read as "nothing real is below 10%".**

**The residue above the floor still contains a large, identifiable
false-positive class: presentation-scale restatements.** Of the 7,083 pairs
above a ±10% floor, **1,988 (28.1%) over 440 instruments are exact powers of
ten**:

| decade | pairs | instruments |
| ---: | ---: | ---: |
| −6 | 34 | 13 |
| −3 | 685 | 200 |
| −1 | 97 | 22 |
| +1 | 552 | 140 |
| +3 | 505 | 169 |
| +6 | 34 | 15 |

Decades ±3 and ±6 are the artefact. Inspected directly:

```
BRKR  WeightedAverageNumberOfSharesOutstandingBasic  FY2023
      146.4        (10-K filed 2025-03-03, millions)
      146,400,000  (10-K filed 2026-02-27, units)      ratio 1e-6
HCI   same concept, H1-2024
      9,897        (10-Q filed 2024-08-09, thousands)
      9,897,000    (10-Q filed 2025-08-08, units)      ratio 1e-3
```

Bruker and HCI Group did not do 1,000,000:1 and 1,000:1 forward splits. Two
consequences the spec had wrong:

⚠ **`unit` in the partition key does NOT catch this.** The unit string is
`shares` on both sides; the scale lives in the XBRL `decimals` attribute, not
the unit.

⚠ **`financial_facts_raw.decimals` is EMPTY on all 6,508,601 rows**, so the
XBRL-native carrier of scale is unavailable to this detector. The writer at
`app/providers/implementations/sec_fundamentals.py:509` does
`decimals=str(entry["decimals"]) if "decimals" in entry else None`, and
`data.sec.gov` companyfacts entries never carry the key. The dimensional and
FSNDS paths do populate it (`app/services/dimensional_facts.py:398`
`_decimals_rank`), so a fix has somewhere to come from — but not from the
detector's own source.

⚠ **Decades ±1 and ±2 must NOT be excluded.** 10:1 and 1-for-10 are ordinary
splits — NVDA, AVGO, LRCX, SMCI and NFLX all sit at 10⁻¹. A blanket
power-of-ten filter would delete the most common split ratio there is.

A two-sided guard was designed and is **not validated** — see item 3. (Not
"falsified": the only arm available to test it cannot see per-event truth.)

### Item 3 — CANNOT be closed as written, and the reason is structural.

The item asks for "a full-population audit against an independent
corporate-action source". **No such source is available to us.** Both candidates
were measured:

**(a) `price_series_break` — unusable.** 404 rows / 158 instruments; 39
instruments overlap the cluster set; 24 breaks fall inside a cluster bracket;
**1** of those 24 matches the cluster's ratio to within 5% in log space. Its own
population is contaminated: **120 of 404 (29.7%)** sit in an opposite-direction
pair within 3 days, i.e. a one-day round trip, which is a bad bar rather than a
unit change (MWG −10.6x on 2025-12-09 then +10.07x on 2025-12-10; SRXH −57x then
+79.6x). This is consistent with the S7 verdict (#2247): the corpus is
predominantly split-**adjusted** already, so a real split usually leaves no level
break at all.

**(b) FINRA `stock_split_flag` — carries no ratio, so it corroborates the
INSTRUMENT-WINDOW, not the EVENT.** That is the finding, and it is what makes
the item unclosable rather than merely unfinished.

Measured on the continuous-coverage window (2025-04-30 → 2026-07-15, **30 held
settlement dates against 29.0 expected bimonthly** — coverage is complete there,
unlike the corpus as a whole):

| arm | clusters | corroborated | % |
| --- | ---: | ---: | ---: |
| all | 574 | 316 | 55.1 |
| reverse-shaped | 402 | 291 | 72.4 |
| forward-shaped | 172 | 25 | 14.5 |
| **placebo — same windows, random FINRA-present instrument** | **574** | **17** | **3.0** |

The signal is real (18x the placebo). It is **not** a per-event rate in
general — but the bound is sharper than "instrument-level only", and the
distribution gives it exactly:

| clusters on the instrument, inside the window | instruments | clusters |
| ---: | ---: | ---: |
| 1 | 174 | 174 |
| 2 | 40 | 80 |
| 3 | 15 | 45 |
| 4 | 3 | 12 |
| 5 | 1 | 5 |
| **total** | **233** | **316** |

**174 clusters (30.3% of the 574) ARE individually attested** — one detected
event, one flagged instrument, one window, nothing else it could refer to. The
other **142** stand on 59 instruments carrying 2–5 distinct ratios each, and
there the flag cannot say which cluster it belongs to; an instrument that split
once and produced three wrong ratios would score 3/3. So the arm gives a firm
**lower bound of 174/574 = 30.3% attested**, an upper bound of 316/574 = 55.1%,
and no false-positive rate at all.

⚠ Precise form of the claim, since the flat version overstates it: a ratio-less
flag corroborates the **event** whenever the instrument has exactly one
candidate cluster in the window, and only the **instrument-window** otherwise.

**The discriminator built to strip the item-4 scale class is NOT VALIDATED by
this arm.** The rule — "a scale artefact leaves exactly one side inconsistent
with its own accession's sibling share-count concepts; a real restatement
rescales the whole filing, so both sides stay consistent" — labels the
population
3,503 admitted / 2,434 scale-artefact / 728 untestable / 418 both-inconsistent,
and does strip the artefact class hard (10^±3 and 10^±6 fall from 17.8% of all
pairs to **0.83%** of the admitted set, a 21x reduction). But it orders the wrong
way against FINRA:

| verdict | clusters | corroborated | % |
| --- | ---: | ---: | ---: |
| both_inconsistent | 80 | 60 | 75.0 |
| scale_artefact | 209 | 125 | 59.8 |
| **admitted** | **287** | **155** | **54.0** |
| untestable | 55 | 23 | 41.8 |

The admitted set corroborates *worst* of the three testable verdicts. Given (b),
this is not proof the rule is wrong — an instrument with a messy scale artefact
is also disproportionately an instrument that really split, and the arm scores
per instrument — but it is proof the rule is **not validated**, and it must not
ship on the strength of the 21x artefact reduction alone.

⚠ Its premise also needs a **formal sibling-eligibility rule** before it could
be trusted even with a better arm. "Both sides stay consistent" assumes the
siblings are commensurable, and they are not always: `dei` cover-page counts and
`us-gaap` balance-sheet counts have different as-of dates, an instant concept and
a duration weighted-average legitimately differ, a multi-class issuer's combined
us-gaap value is not comparable to a per-class one, and a filing may correct one
concept and not the other. The 0.7-decade tolerance hides most of that; it is
not a reason to think the premise holds.

**What DOES validate the detector: a labelled subset from public record.** Six
US forward splits, chosen from public record without reference to our data and
all outside the ratio bands above:

| symbol | real event | detector ratio | expected |
| --- | --- | ---: | ---: |
| WMT | 3-for-1, eff. 2024-02-26 | 0.33 | 0.333 |
| NVDA | 10-for-1, eff. 2024-06-10 | 0.10 | 0.100 |
| CMG | 50-for-1, eff. 2024-06-26 | 0.02 | 0.020 |
| AVGO | 10-for-1, eff. 2024-07-15 | 0.10 | 0.100 |
| SMCI | 10-for-1, eff. 2024-10-01 | 0.10 | 0.100 |
| LRCX | 10-for-1, eff. 2024-10-03 | 0.10 | 0.100 |

**6/6 at the exact contractual ratio.** ⚠ This establishes **recall on
unambiguous positives**, not precision — the six are large-cap, round-ratio,
forward-split cases chosen because they are unambiguous. They say nothing about
false positives, reverse splits, arbitrary ratios, the scale-artefact class or
ambiguous brackets, which is exactly why acceptance step 1 below demands a set
spanning both directions and both decades. ⚠ All six carry **zero** FINRA `S` flags
— and that is *not* evidence about FINRA: all six effective dates fall in our
FINRA coverage gap (we hold 2024-01-12, 2024-01-31, 2024-02-15 and then nothing
until 2025-04-30). Reading those zeros as "FINRA does not flag forward splits"
would be a coverage artefact read as a semantic one.

**Revised acceptance for item 3.** Precision is stated on a labelled subset and
the ceiling is raised by data, not by argument:

1. Extend the labelled set to ≥30 events from public record spanning both
   directions and both decades, and report precision and recall on it.
2. **Run the #2234 FINRA backfill first.** It is merged and specced; only the
   operator run is outstanding. It takes FINRA from 33 to 121 settlement dates
   back to 2021-07-15, closes the 2024-02-15 → 2025-04-30 gap that hides all six
   labelled events, and moves clusters inside the corroboration window from
   **1,730 to 2,199 (+27.1%)**. Until it runs, the instrument-level arm is the
   only one there is and it cannot attribute a flag to an event.
3. The FINRA arm is reported as an **instrument-window** corroboration rate with
   its placebo, never as an event-level false-positive rate.

### Item 5 — measured, and the clustering key is the actual defect.

**130 empty-intersection clusters over 98 instruments** (30 with exactly two
events, 100 with more; median overlap deficit 567 days). The spec asserted these
are two distinct splits at the same ratio and that they must be time-partitioned.
That is still plausible and still unvalidated — but the prior figure of 38 was
measured on a different key, and the real finding is upstream of it:

⚠ **`round(ratio, 2)` is not a usable cluster key.** Every ratio below 0.005
collapses to the single bucket `0.00`. In the 2025-04-30 → 2026-07-15 window
alone that bucket holds **33 forward clusters**, mixing the 10⁻³ and 10⁻⁶ scale
artefacts of BRKR, WAT, RGEN, CLDX, HCI and BYD with whatever genuine
high-ratio events exist. The key must be **log-scaled** — cluster on
`round(ln(ratio)/ln(10), 3)` or an equivalent relative-tolerance key — before
the empty-intersection question can even be asked, because an artefact merged
into a real cluster is exactly what manufactures an empty intersection.

### Item 6 — ANSWERED. See the revised blast-radius table above.

Funds, treasury, ESOP and DRS are now measured; `nonvested` has no table.

### Item 7 — ANSWERED with a source rule, and it REVERSES on-issue decision 3.

**Source rule: ASC 805-40-45-2(d).** In a reverse acquisition the equity
structure of the **legal subsidiary** (the accounting acquirer — the private
target) is restated using the exchange ratio in the acquisition agreement, to
express its history in the shares the **legal parent** issued. ASC 260's EPS
guidance then requires the restated weighted-average count to be presented for
every comparative period.

What that restatement does **not** touch is the legal parent's own outstanding
shares. A shareholder who held 1,000,000 shares of the shell before the merger
holds 1,000,000 shares of the surviving issuer after it. Their *percentage*
collapses; their *unit* does not change.

Our ownership rows are keyed to `instrument_id`. The instrument→registrant
bridge is **`instrument_sec_profile.cik`** (5,340 rows, one per instrument;
`instruments` itself carries no CIK, which is what open item 1 runs into), and it
follows the surviving registrant — the same CIK, with the pre-merger name in
`instrument_sec_profile.former_names`. So a pre-merger Form 4 / 13D-G / 13F row
on that instrument reports the **legal parent's** own shares, in units the ASC
805-40 restatement never re-denominated.

⚠ **Therefore the ASC 805-40 exchange ratio ALONE must not be applied to an
ownership row.** A legacy shell-holder row is a STALENESS problem (#2229's axis),
not a unit problem, and rescaling it would corrupt a row that is currently merely
old.

⚠ **Stated as the narrow rule, because the flat version is wrong.** A pre-merger
row on the surviving CIK **can** legitimately need re-denominating, in at least
two ways: (a) the registrant executes a legal reverse split or recapitalisation
around the closing — a real unit change that happens to be concurrent, and whose
factor is *not* the exchange ratio; (b) the row is not a legacy shell-holder
position at all but a target-holder position reported after exchange into
registrant shares, which is already in post-exchange units and must be left
alone for the opposite reason. The detector sees only the product of whatever
occurred, so it cannot separate (a) from the exchange ratio — which is why this
class fails closed rather than adjusting.

⚠ **This does NOT mean the recorded factor is useful to the price series
instead.** An earlier draft of this section claimed it was; that is wrong.
Public price history follows the traded legal parent, so applying an accounting
acquirer's ASC 805-40 restatement ratio to the shell's own bars would corrupt
them the same way it would corrupt an ownership row. If such a row is written to
`price_adjustments` at all, it needs a state meaning *recorded, not applicable to
either consumer* — and `sql/246` has no such state: any row with
`superseded_by IS NULL` is active for the price read path by construction. That
is a storage gap to resolve with #2240 before the detector writes anything of
this `kind`.

This **reverses decision 3** as recorded on the issue on 2026-08-03, which said
the arbitrary-ratio residue is "ASC 805-40 reverse-merger exchange ratios, not
error… equally real unit changes" and concluded that roundness must therefore
never filter. The premise is right — they are real accounting restatements, and
roundness must still not be an *admission* gate for detection. The consequence
is wrong: detected 805-40 events must be recorded with `kind` distinguishing
them and then **excluded from application** to ownership rows. Recording them
still pays, because `price_adjustments` serves the price series too, where the
805-40 restatement *is* the right factor for the accounting acquirer's history.

⚠ A de-SPAC that also executes a reverse split at closing produces a genuine
split on top of the exchange ratio. Those are two events, not one, and the
detector sees only their product. Separating them is unsolved and is the reason
this class should fail closed rather than adjust.

### Item 8 (was the standalone "Open" section) — the pre-retention gate.

Unchanged and still open; see the section above. Not re-measured this session.

### Still blocking code

- Item 1's issuer-level scope note, agreed with #2240.
- The `effective_date`-vs-bracket contract gap, agreed with #2240.
- A working scale-artefact discriminator (item 4's class is named; the candidate
  guard is unvalidated, and validating it needs a ratio-labelled negative set
  that does not exist yet).
- A log-scaled cluster key (item 5).
- The ≥30-event labelled set and the #2234 backfill (item 3).
- Item 8's predicate.

## Reproduction

Corrected pair scan (2026-08-07). Differences from the 2026-08-03 version, all
of which move numbers: `taxonomy` and `unit` in the partition key,
`accession_number` as the same-day tie-break, `dei:EntityCommonStockSharesOutstanding`
added, and `prev_val > 0 AND val > 0`. ~40 s against dev.

```sql
CREATE UNLOGGED TABLE tmp_2231b_pairs AS
WITH f AS (
  SELECT instrument_id, taxonomy, unit, concept,
         COALESCE(period_start, '0001-01-01'::date) AS pstart, period_end,
         val, filed_date, accession_number, decimals, form_type,
         lag(val)              OVER w AS prev_val,
         lag(filed_date)       OVER w AS prev_filed,
         lag(accession_number) OVER w AS prev_accession,
         lag(decimals)         OVER w AS prev_decimals,
         lag(form_type)        OVER w AS prev_form
  FROM financial_facts_raw
  WHERE concept IN ('CommonStockSharesOutstanding',
                    'WeightedAverageNumberOfSharesOutstandingBasic',
                    'EntityCommonStockSharesOutstanding')
  WINDOW w AS (PARTITION BY instrument_id, taxonomy, unit, concept,
                            COALESCE(period_start, '0001-01-01'::date), period_end
               ORDER BY filed_date, accession_number)
)
SELECT *, (prev_val / NULLIF(val, 0))::numeric(30,9) AS ratio
FROM f
WHERE prev_val IS NOT NULL AND prev_val > 0 AND val > 0 AND prev_val <> val;
```

Clusters are then `GROUP BY instrument_id, round(ratio,2)` with bracket
`[max(prev_filed), min(filed_date)]` — ⚠ but see item 5: that key is wrong below
0.005 and must be replaced with a log-scaled one before the detector ships.

⚠ **A placebo arm whose random draw sits in an uncorrelated scalar subquery is
an InitPlan and is evaluated ONCE.** The first version of the FINRA control
returned exactly **0 of 931**, which reads as perfect separation and is in fact a
constant. Drawing per row in the outer query gives **32 of 931**. A control that
is silently constant is worse than no control, because it certifies whatever it
is pointed at.

## Housekeeping

- **2026-08-03**: `tmp_2231_restatements`, `tmp_2231_pairs`, `tmp_2231_clusters`
  — UNLOGGED, write no existing row, still present in the dev DB. Drop when the
  detector lands. ⚠ They were built on the *pre-correction* key (no `taxonomy` /
  `unit`, no floor, no `dei`), so they do not reproduce any figure in this
  document.
- **2026-08-07**: `tmp_2231b_pairs`, `tmp_2231b_clusters`, `tmp_2231b_labelled`,
  `tmp_2231b_rows` — **dropped at end of session.** Every number above is
  reproducible from the scan in "Reproduction"; ~40 s for the pair scan, under a
  minute for the rest.
