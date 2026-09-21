# ARM B §7.4 — SEC XBRL cannot settle the correction policy, and a control arm says why

**Ticket:** #2834 (R5 S-B, ARM B) · **Status:** measurement complete, nothing proposed
**Reproduce:** `PYTHONPATH=. uv run python -m scripts.measure_2834_armb_sec_adjudicator`
**Predecessor:** `docs/proposals/ta/2026-09-21-armb-split-only-basis-verdict.md` (§7 item 4)

## 1. The question, and the answer

The basis verdict settled on candidate 1 — derive a split-only series from the per-bar
split ratio the Intrader archive ships — and recorded that **every check available to it
is circular**. The internal check (`adj_close` honours the stamp) is derived from the stamp
and passes 364 of the 383 events a second processing rejects. The cross-vendor check is a
Yahoo redistribution, and `research_corpus_ingest.py` says so itself: *"the two are ONE
observation and agreement between them is circular, never corroborating."*

§7 item 4 named the one untried direction: a source outside the Yahoo lineage.

**Verdict: the `companyconcept` split-ratio elements cannot settle the correction policy.**
The reason is not primarily coverage — it is that **the source's own error rate, measured on
a control arm, is the same order as the effect it would have to resolve.** §5.3 is the
finding; everything before it is the funnel that gets there.

⚠ Scope of that claim: it is about **two standard-taxonomy elements read through
`companyconcept`**, not about SEC evidence in general. §6 item 4 lists what remains untested.

## 2. Source rule

The governing artefact is the US GAAP element definition, recorded by our own ingest in
`sec_facts_concept_catalog` (label and description come from SEC):

> **`StockholdersEquityNoteStockSplitConversionRatio1`** — "Ratio applied to the conversion
> of stock split, for example but not limited to, **one share converted to two or two
> shares converted to one**." Units: `Rate`, `pure`.

The definition names both orientations under one element. ⚠ **That does not prove a given
value of `2` is used both ways in practice** — it establishes only that the element does not
forbid it. §5.2 measures what filers actually do, and the two are never merged into one claim.

The deprecated twin `StockholdersEquityNoteStockSplitConversionRatio` (deprecated 2013-01-31)
carries eleven unit spellings in our catalog. It is probed as a fallback and matched
separately; it contributed **1 of 365 headline matches**.

## 3. Bounds — including one an earlier draft got wrong

### 3.1 The pre-2009 bound is NOT structural

An earlier draft of this document excluded 57% of events as *"outside XBRL coverage by
construction"* and derived an "upper bound" from it. **That is false.** The XBRL mandate
bounds when a *filing* exists, not which *event dates* it can describe — a 10-K filed in
2013 can disclose a 1987 split, and the corpus contains exactly that. The era filter has
been removed; no bound is asserted from filing-mandate dates.

⚠ Empirically the correction changes almost nothing: of 2,630 instant facts, **1** is dated
before 2009. So pre-2009 events are nearly unreachable *in practice*, which is a measurement,
while "unreachable by construction" was an invented rule. The distinction matters because
the second version would have justified never looking.

### 3.2 The CIK column is dark, and the two paths that replace it disagree

`sql/250_research_price_series_cik.sql` added `cik` + `cik_source` as a #2282 2c
prerequisite, with a CHECK-paired provenance invariant and a closed vocabulary. Its header
argues exactly the case this work needed: *"CIK is stable across delisting, ticker change
and reuse. Symbol is not."*

Measured: **0 of 30,591 rows carry a CIK**, and no writer for the column exists. So the
mapping is re-derived here through the two evidenced paths its vocabulary names.

⚠ **35 series resolve by both paths and 20 of them (57%) disagree.** The Form 25 path joins
on `resolved_symbol` equality, which sql/353 records as no longer reproducing the Q-suffix
resolution, and a CIK identifies an **issuer**, not the share class a series tracks. Both
are unguarded. 705 of 4,937 reachable events (14.3%) come through that path and inherit its
precision.

### 3.3 Dimensional dropout, which no census here can remove

sec-edgar.md §7.17: `companyfacts`/`companyconcept` return **only the non-dimensional default
member**, so a ratio tagged on `us-gaap:StatementClassOfStockAxis` is dropped entirely.
Multi-class issuers are exactly the population most likely to tag that way. **A 404 is not
evidence that the filer did not disclose the split**, and this measurement cannot separate
the two.

## 4. The funnel

```
stamped events                                8,073
  on a CIK-reachable series                   4,937   (61.2%)
      via instrument_sec_profile              4,232 events
      via sec_form25                            705 events
CIKs probed                                   2,000
  returning >= 1 split-ratio fact               609   (30.4%)
facts retrieved                               4,311   (4,221 current element, 90 deprecated)
  INSTANT context  (datable)                  2,630
  DURATION context (no event date)            1,681   -- excluded from matching
events with a dated fact in window              365   (4.52% of all stamps, +/-10d)
```

⚠ Duration facts fix the ratio to a reporting *period* and recover no event date. An earlier
draft matched on the context `end` regardless and silently admitted 1,681 of them.

### 4.1 Window and tolerance sensitivity, since both drive the headline

| window | matched | % of 8,073 | tol | direct | reciprocal | disagree |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ±3d | 336 | 4.16% | 0.01 | 288 | 19 | 28 |
| ±10d | **365** | **4.52%** | **0.01** | **308** | **22** | **34** |
| ±10d | 365 | 4.52% | 0.02 | 314 | 22 | 28 |
| ±45d | 425 | 5.26% | 0.01 | 364 | 23 | 37 |

There is no published rule fixing how far a disclosed context instant may sit from an
ex-date, so no window is defensible alone and every figure is quoted with its window.
The headline tolerance is pinned to the predecessor's `_EVENT_TOLERANCE` (1%) so that
"SEC agrees" and "the reference disagrees" are decided at the **same** threshold — an
earlier draft used 2% here against the predecessor's 1%, which flattered agreement.

⚠ **39 matched events have tied nearest facts carrying different values.** The tie-break
(prefer the current element, then the latest filing) is explicit rather than left to array
order, but ties at this rate are themselves a caution. 4 fact records each serve more than
one stamped event, so the matches are not one-to-one.

## 5. What it says

### 5.1 Against the predecessor's §3 dispute classes

All four class counts reproduce the predecessor exactly, so this is the same partition.
⚠ Those class names are **1%-ratio predicates, not established causes** — the predecessor
explicitly allows date offsets, reference errors and symbol collisions behind each.

| class | events | SEC covers | what SEC says |
| --- | ---: | ---: | --- |
| stamped factor ABSENT from prices (`no_step`) | 188 | 4 (2.1%) | 3 direct, 1 disagree |
| stamped factor of the WRONG MAGNITUDE | 163 | 30 (18.4%) | 21 direct, 4 reciprocal, 5 disagree |
| reference carries the raw step too | 32 | **0 (0.0%)** | — |
| **control: stamp and reference AGREE** | 3,824 | 251 (6.6%) | 221 direct, 10 reciprocal, 20 disagree |
| no reference at all | 3,835 | 76 (2.0%) | 60 direct, 8 reciprocal, 7 disagree, 1 unusable |

**SEC covers 34 of the 351 uncorroborated events (9.7%)**, and 34 of all 383 disagreeing
events (8.9%).

⚠ "Covers" means *a dated fact within the window whose magnitude was compared* — it does not
mean the per-bar treatment was established. A `disagree` backs **neither** the stamp nor the
reference; it is not a vote for the reference.

### 5.2 Orientation is an association, not an established convention

| stamp direction | direct | reciprocal | disagree |
| --- | ---: | ---: | ---: |
| forward (stamp > 1) | 94 | 2 | 5 |
| reverse (stamp < 1) | 214 | 20 | 29 |

Reciprocal concentrates on reverse splits (20 of 22), but 214 reverse events are *also*
direct, so this is not a consistent inversion that could be undone by rule.

⚠ **This is weaker than it looks, twice over.** "Forward"/"reverse" is read off the stamp,
which is the artefact under test — so a reciprocal match is equally consistent with a stamp
error, a tagging error, a wrong identity or a wrong date. And 263 of 365 covered events are
reverse-stamped, so the base rates (11.6% reverse vs 2.0% forward) carry the comparison, not
the raw 20-of-22. **No filing was read to confirm a convention.**

### 5.3 The control arm — the finding that decides it

The `agreeing` row above is not decoration. On **251 events where the stamp and the
reference already agree**, and which we therefore have no reason to doubt, SEC returns
**non-direct 30 times — 12.0%** (20 disagree, 10 reciprocal).

That is SEC's noise floor through this path: wrong identity, wrong date, orientation, share
class, or a genuinely different corporate action. Against it:

| arm | n | non-direct |
| --- | ---: | ---: |
| control (stamp and reference agree) | 251 | 12.0% |
| disputed (`magnitude`) | 30 | 30.0% |

The disputed arm is worse, which is the direction a real signal would point. But **n = 30
against a 12% floor cannot resolve it**, and the two arms are not comparable populations
anyway — the control is dominated by surviving filers, the disputed class by the
delisted micro-caps where every identity join is weakest.

**So the honest reading is not "SEC backs the stamp". It is that this path's own error rate
is the same order as the discrepancy it was brought in to adjudicate.** An earlier draft of
this document claimed SEC "backs the stamp over the reference 27 to 4" — that was never
computed (nothing here compares SEC to the reference's `implied_factor`), and it is withdrawn.

## 6. What follows

1. **The correction policy (§7 item 1) must be decided without a non-circular adjudicator
   from this source.** That is now measured rather than assumed, and it is the substantive
   result.
2. **Do not spend a corpus re-ingest adding these concepts to `TRACKED_CONCEPTS`.**
   `sec_fundamentals.py:412` notes that leaving a concept out costs a corpus job; the
   converse is that adding one costs a job too. 4.52% coverage at a 12% noise floor does not
   buy it, and the probe path answers the question without an ingest.
   ⚠ This is a judgement, not a measurement: no error-cost model was built. It is recorded as
   a recommendation so the next session can disagree with something specific.
3. **`research_price_series.cik` is dark and this script's mapping is throwaway.** Populating
   it is cheap and in-schema (#2282 2c's stated prerequisite) — but §3.2's 20-of-35 conflict
   rate says the two paths need a reconciliation rule first. Not proposed here; not on the
   capital path.
4. **Not tested, and therefore not covered by the verdict:** 8-K Item 5.03 charter amendments
   and narrative disclosure (unstructured, and a classifier over source text is itself a
   data-treatment decision needing its own source rule); dimensional instance facts in the
   inline-XBRL documents, which is where §3.3 says the multi-class ratios actually live;
   custom filer tags; and any non-XBRL corporate-action feed.

## 7. Scope

No persistent write, no schema change, no corpus change, nothing under `app/`, no strategy
touched. The script writes two `/tmp` JSON files and the predecessor's `check_events` creates
`ON COMMIT DROP` temp tables.

`s2_cross_sectional_momentum.py` is deliberately untouched — `_source_hash()` hashes the whole
module, so even a docstring edit there rotates the strategy identity
(`docs/review-prevention-log.md`, #2840).
