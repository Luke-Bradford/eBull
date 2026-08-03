# Split adjustment for stale ownership rows (#2231)

Ownership rows are stored at the share count reported on their filing date and
compared against a present-day `shares_outstanding`. After a reverse split the
two are in different units, producing the 100x-350x insider ratios in the #2226
cohort (GWAV 266,252,068 held vs 829,631 outstanding = 32,092.8%).

There is no splits or corporate-actions table anywhere in the schema.

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

⚠ `dei:EntityCommonStockSharesOutstanding` is NOT yet in the detector's concept
set, but the rollup denominator path reads DEI/us-gaap outstanding. Confirm
whether numerator and denominator rest on the same basis before implementation;
a mismatch there reproduces the #2217 class of bug.

## Full-population verification

Scan over `financial_facts_raw`, all instruments, no sampling.

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

Consecutive pairs alone help less than expected, because the same period is
typically only re-filed a year later. The gain comes from **intersecting**: every
event describing one split must contain that split's true effective date, so the
brackets can be intersected. GWAV's 1-for-150 gets a 91-day bracket from
`CommonStockSharesOutstanding` (2024-05-20 → 2024-08-19) which contains the true
2024-05-31 date, against 371 days from the weighted-average concept alone.

**Blast radius, per row, after intersected bracketing, on the `period_end`
as-of date (NOT `filed_at` — see the as-of rule below):**

| source | rows | ambiguous (fail closed) | certain pre-split (adjust) |
| --- | --- | --- | --- |
| insiders | 93,258 | 2,352 | 4,612 |
| institutions | 2,298,518 | 21,221 | 6,304 |
| blockholders | 22,795 | 966 | 288 |
| def14a | 66,305 | 1,743 | 1,469 |
| **total** | 2,480,876 | **26,282** | **12,673** |

This replaces the 4,680 → 59,221 bracket recorded on-issue; the upper bound
collapses to 38,955 because the smearing was an artefact of per-instrument
bracketing.

⚠ Measuring on `filed_at` instead understates this by 37% (19,215 / 9,141) and
reports **DEF 14A as entirely unaffected (0 certain)** when it in fact carries
1,469 rows needing adjustment — the proxy record date precedes the filing date
by enough to cross a bracket. Any re-measurement must use `period_end`.

**Blockholder coverage is floored at 2024-12-18** — `min(filed_at)` on
`ownership_blockholders_current` is exactly the Schedule 13D/G structured-XML
mandate date (sec-edgar skill §2.4.1; pre-mandate filings are HTML-only and PR11
of #1233 chose a hard date floor). That category therefore cannot carry a
pre-2024 split problem, and its 288/966 figures are complete rather than
truncated.

**13F PRN rows are already excluded at ingest**
(`app/services/sec_13f_dataset_ingest.py:729-736`, PR #1054 — 20k PRN rows in
2026Q1 alone). This matters here because PRN rows carry bond principal in
dollars, not shares; rescaling one by a split ratio would be meaningless. No
additional guard needed in this spec, but the exclusion must not be removed.

## Design

### `corporate_actions` table

Ratios are captured **as observed**. The detector is an ongoing observer, not a
history reconstructor — retention (`app/services/financial_facts_retention.py:44-49`,
latest 3 10-K + 8 10-Q accessions per instrument) permanently removes pre-window
figures, which is why ARMP's real 998x produces no restatement row and RTB's
2018 / GNLN's 2019 factors cannot be derived. Once a ratio is seen it is the
system of record and is never re-derived.

Columns: `instrument_id`, `ratio numeric`, `bracket_start date`,
`bracket_end date`, `kind` (`round_integer` | `round_fraction` | `arbitrary`),
`n_events int`, `first_observed_at`, `source` (`xbrl_restatement`).

⚠ **`kind` classifies; it never filters.** The 3.6% arbitrary-ratio residue is
ASC 805-40 reverse-merger exchange ratios (AMCI 0.127, BBT 0.481, FOXX 0.303) —
equally real unit changes. Filtering to round ratios silently drops legitimate
conversions.

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
2. Cluster by `(instrument_id, round(ratio,2))`, bracket =
   `[max(bracket_start), min(bracket_end)]`.
3. **38 clusters intersect empty** — two genuinely distinct splits at the same
   ratio. Those must be time-partitioned into separate events, never merged.

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

⚠ **`as_of` is `period_end`, NOT `filed_at`.** All four `ownership_*_current`
tables carry `period_end NOT NULL`, which is the date the share count describes;
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

Still open, and blocking implementation rather than this spec:

1. **Key granularity.** `corporate_actions` keyed on `instrument_id` alone is
   wrong for multi-class issuers and ADR/ADS ratio changes — splits are
   security/class-level. Needs the class dimension or an explicit scope note.
2. **`corporate_actions` has no declared PK, event id, source accessions, or
   correction/tombstone state.** "Captured as observed, never re-derived"
   conflicts with later evidence narrowing a bracket or falsifying an event —
   needs append-only evidence with monotonic bracket tightening.
3. **False-positive rate is not established on the full population.** "Zero
   fires on AVAL / ALCYF / PKG" is three known errors, not a rate; the
   arbitrary-ratio-is-ASC-805-40 claim rests on three examples (AMCI, BBT,
   FOXX). Both need a full-population audit against an independent
   corporate-action source before the detector is trusted to write.
4. **"Same period, two values ⇒ split / recap / error" is too strong.**
   Amendments, decimals/unit fixes, taxonomy or context changes, and
   weighted-average corrections all move a same-period fact with no tradable
   unit change. The materiality floor and `unit` partitioning mitigate but do
   not close this.
5. **Empty intersections are asserted to be two distinct splits**, unvalidated —
   could equally be bad facts or over-lossy `round(ratio,2)` clustering. The
   time-partitioning algorithm is unspecified.
6. **Blast radius omits funds, treasury, ESOP and DRS/nonvested overlays**,
   which the rollup does read. Either measure them or state why out of scope.
7. **Reverse-merger exchange ratios** may not warrant rescaling a pre-merger
   holder into current issuer shares at all — that is a different economic
   event from a split, and needs a source rule rather than accounting
   inference.

Items 3, 4 and 7 are source-rule questions and must be answered before code, per
"Source-rule before design".

## Housekeeping

`tmp_2231_restatements` and `tmp_2231_pairs` / `tmp_2231_clusters` are UNLOGGED
analysis tables in the dev DB (write no existing row). Drop when the detector
lands.
