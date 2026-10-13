# #3624: LLM research component, re-sequenced by measurement (2026-10-06)

Issue: #3624. Programme: `docs/research/2026-10-04-strategy-research-sweep.md` §4 item 7. Standards:
`.claude/skills/quant/research-process.md`, `.claude/skills/quant/llm-research.md`. This is a programme-level
spec: it fixes order, scope and the gates between slices. Each slice gets its own build spec, whose required
contents are listed here.

**Decision.** Build the deterministic text and filing-item layers first: year-over-year text change on periodic
reports (slice 1) and narrow structured filing-item labels (slice 2). They are measurable on history, and they are
the deterministic factor controls `llm-research.md` §Validation asks LLM features to beat. LLM extraction (slice 3)
comes after them: its first spend is a capped, outcome-blind extraction pilot under its own registration, and no
return study runs or spends until a registration shows, with a design-specific power assessment, that the post-cutoff window can
answer its question. LLM `base_value` leaves scoring through #3609 step 2, and #3624 does not close until it has.

## 1. Premise, measured on the dev DB (2026-10-06)

The issue says v1.5's value family is driven by LLM price targets (corr 0.725 with the total score).

- **Which path each score used** is read from the score's own explanation, written at scoring time:
  `_value_score` (`app/services/scoring.py:730`) appends `base_value missing` on the fallback path. Latest
  `v1.5-balanced` score per instrument: 3,937 rows; 238 took the thesis path, 3,699 the fallback.
- **The fallback contains no price target in practice.** Its docstring names a 30% `price_target_mean`
  component, but the only call site passes `price_target_mean=None` (`app/services/scoring.py:2112`, #539: the
  FMP analyst-target source was retired). The fallback is P/E and FCF yield only.
- **Correlations:** `corr(value_score, total_score)` is 0.759 over the fallback rows, 0.306 over the thesis rows,
  0.723 pooled. Value is a component of the total, so none of these is attribution. What the counts establish is
  prevalence: 238 of 3,937 latest scores used an LLM target at all.
- **The thesis path is stale.** `max(theses.created_at)` = 2026-08-22 19:47Z; 482 instruments have any thesis.
- Reproduce: `DISTINCT ON (instrument_id) … ORDER BY scored_at DESC` over `scores` where
  `model_version = 'v1.5-balanced'`; split on `explanation NOT ILIKE '%base_value missing%'`; `corr()`, `count()`.

**Removal of LLM targets from scoring.** The 238 thesis-path rows are the ones potentially affected; actual score,
rank and portfolio differences are measured by a matched full-population A/B. v1.5's weights are being replaced by
#3609 step 2, which runs that A/B anyway. **Completion condition for #3624:** a consumer and input-lineage
inventory shows no scoring model read by any consumer takes an absolute LLM price target or anything derived from
one (`theses.bear_value`, `base_value`, `bull_value`, and any other per-share target field the inventory finds),
with a dependency test that fails if one does; the A/B is separate evidence of effect, not of absence. This lands in #3609 step 2,
or, if v1.5 is still read by any consumer when slice 1 merges, in a standalone removal PR. The memo keeps
bear/base/bull for display (`llm-research.md` §What our LLM component should produce, item 3).

## 2. The model-cutoff window

Valid evaluation of LLM output is forward-only after the pinned model's **training data** cutoff
(`llm-research.md` §Look-ahead; Lopez-Lira, Tang & Zhu 2025). The provider's retirement figure is a
"not sooner than" commitment: a floor on availability, not a shutdown date. Retirement stops new extraction; it
does not invalidate stored outputs, whose outcomes keep maturing. A successor model is a new version with its own
record (`llm-research.md` §Validation).

| model id | training data cutoff | available until at least | 10-K / 10-Q filed after the cutoff, on file today |
|---|---|---|---|
| `claude-haiku-4-5-20251001` | Jul 2025 | 2026-10-15 | not counted |
| `claude-sonnet-4-6` | Jan 2026 | 2027-02-17 | 3,904 / 8,473 (filed ≥ 2026-02-01) |
| `claude-sonnet-5-5` | Jun 2026 | 2027-09-28 | 231 / 3,963 (filed ≥ 2026-07-01) |
| `claude-opus-5-5` | Jun 2026 | 2027-09-22 | as above |
| `claude-fable-5-1` | Jun 2026 | 2027-09-01 | as above |

Evidence (fetched 2026-10-06, platform.claude.com): `about-claude/models/overview` ("Training data cutoff" row:
Jun 2026 for Fable 5.1 / Opus 5.5 / Sonnet 5.5, Jul 2025 for Haiku 4.5; "Every Claude model ID is a pinned
snapshot, including the dateless IDs used from the 4.6 generation on"); `models/sonnet-4-6/overview` ("Training
data cutoff | Jan 2026", "Retirement | Not sooner than February 17, 2027");
`about-claude/model-deprecations` (the "Tentative retirement date" column above). Filing counts: `filing_events`,
`provider = 'sec'`, `filing_type IN ('10-K','10-Q')`, by `filing_date`. They are inventory, not a power-relevant
sample size.

**Model choice is made at slice-3 registration, not here.** The registration compares every model whose
availability floor covers its planned extraction window (Haiku 4.5 included if a campaign fits before its floor),
on the registered task's eligible observations with completed outcome horizons (up to the 120-session decay
horizon) and their effective information after issuer and date dependence. Because `llm-research.md` §Validation
requires a two-writer A/B on the same post-cutoff dates, it names both writers and assesses the common window after
both cutoffs separately. On raw elapsed time, `claude-sonnet-4-6` has about 8 months against about 3 for the
Jun-2026 models; Haiku 4.5's earlier cutoff gives more, subject to its availability floor. The preference is the
registration's to make. A recall probe at the cutoff boundary (Gao, Jiang & Yan) supplements the documented
cutoff; it does not certify the population.

## 3. Why the LLM return study waits

This is a sequencing and resource decision, not a claim that an LLM feature cannot pass.

- **Track per candidate, not per family.** `research-process.md` §Two tracks classifies by hypothesis and
  evidence. Year-over-year 10-K change (Cohen, Malloy & Nguyen 2020) is a published premium and may qualify for
  Track B if post-publication independent out-of-sample evidence is cited at registration. The sector-neutral LLM
  outlook score (Lehner & Lopez-Lira 2026) is a working paper: Track A unless independent replication is cited.
- **Power is assessed per design at registration**, separately for extraction accuracy (precision/recall with
  uncertainty bounds on the labelled sample), for monthly cross-sectional rank IC, and for net executable returns,
  each with its dependence treatment (common dates, filing clustering, overlapping holdings) and, for returns, the
  deflated threshold for the trial count. The rough guide in `research-process.md` §Power,
  `T ≈ ((z_α + z_β) / IR)²` with z_α = 3.0 and z_β = 0.8416, gives 14.76, 3.69 and 1.64 years at annualised
  portfolio IR 1, 2 and 3 (`python3 -c "print([round(((3.0+0.8416)/ir)**2,2) for ir in (1,2,3)])"`). It is an
  illustration of the return-study scale only, and says nothing about extraction accuracy or IC tests.
- **The deterministic layers have history the LLM slice cannot have**, and every LLM feature is measured for
  incremental value over them.

## 4. Programme rules that bind every slice

- **One trial ledger.** Every configuration tried in slices 1–3 (sections, forms, measures, label definitions,
  residual tasks, models) is counted in one programme ledger in `app/services/trial_register.py`, with PBO/CSCV or
  an equivalent nested hold-out on any selection step (`research-process.md` §Every study is registered). A
  confirmation sample is sealed before the first slice reads outcomes and stays sealed across slice selection.
- **Gates.** Each build spec pre-registers its progression and no-go criteria: what census, survivorship and
  corpus-A/B results let the next step use its output, what triggers remediation, and what ends the slice.
  Building a corpus is not accepting it for research; the gate decision is recorded on #3624 before any downstream
  use.
- **Data-treatment hierarchy.** For every exception: the source's reporting and disclosure rules first (form
  instructions, Reg S-K, Exchange Act rules); then the published method's treatment; a declared construction choice,
  frozen in the version hash, only for a degree of freedom neither determines, cited as such.
- **Provenance.** Every output carries its evidence. Slice 1: both section records of the pair (accession, section
  id, extractor version) and the frozen pairing, preprocessing and measure version. Slice 2: accession number and
  item code, the filing itself being the evidence. Slice 3: a quote and its document id. Issue item 1d asks for a
  quote and doc id; for slices 1–2 the referenced section or item is the quote's equivalent.
- **Track B checklist** when a registration claims Track B (`research-process.md` §Two tracks): fidelity of the
  actual section and form variant against the published series; the net result against the net buy-and-hold
  benchmark and the random-basket control on the same window; a forward non-inferiority rule with margin, minimum
  information and sequential boundary. A related published premium does not by itself make a construction faithful.

## 5. Slices

### Slice 1: year-over-year periodic-report text change

**Source rule.** Reg S-K Item 105 (Risk Factors: Form 10-K Item 1A; Form 10-Q Part II Item 1A, which reports
material changes from the annual disclosure) and Item 303 (MD&A: 10-K Item 7, 10-Q Part I Item 2). Method: Cohen,
Malloy & Nguyen (2020), *Lazy Prices*, Journal of Finance 75(3), for pairing and similarity.

**Store.** `periodic_report_sections` (sql/441) gains the new section ids and historical targets. The MD&A
extractor is hashed and frozen by fund-v1 (`app/services/mdna_extraction.py:1-11`): new sections use a new
extractor id in a new module, never an edit to that file. Historical targets are a separate producer mode; the live
producer's current-target rule (#3518 spec §4.1) is unchanged.

**Population, as metadata only.** `filing_events` holds 32,209 events typed `10-K` and 98,749 typed `10-Q`,
2016-06-03 → 2026-10-05, over 4,201 / 4,266 instruments. That is filing metadata, not retrievable documents,
extracted sections, valid pairs or usable outcomes; feasibility is conditional on the census below.

**The slice-1 build spec must contain:**

1. Forms: `10-K`, `10-KT`, `10-Q`, `10-QT` per the extractor's `FORM_FAMILY`, with transition reports' pair
   eligibility and the amendment policy stated from the form rules.
2. Sections: 10-K Item 1A, 10-K Item 7, 10-Q Part I Item 2, and 10-Q Part II Item 1A, or an annual-only
   risk-factor scope with its reason. Quarterly risk-factor semantics come from the Form 10-Q instructions:
   "no material change" text, smaller-reporting-company exemptions and absence are disclosure states, kept
   separate from processing failures, and never read as zero risk.
3. Caption-only, incorporated-by-reference and omitted sections: a treatment rule from Rule 12b-13 and the form
   instructions, including whether referenced documents are resolved point-in-time or excluded.
4. The pairing algorithm, exactly, under the §4 hierarchy: issuer and fiscal-period matching, gaps, changed fiscal
   year ends, duplicate accessions, several instruments mapped to one issuer.
5. The similarity measures with the paper's equations and preprocessing (tokenisation, weighting, edit unit,
   denominators, distance-to-similarity), cited to section or appendix. A measure the paper does not define is not
   added.
6. A census at two levels: document retrieval per accession; then, per accession × section × extractor version,
   a processing status (extracted, fetch_failed, parse_failed, not attempted, with effective status after retries
   and invalidation) and a disclosure status (present, no material change, exempt, caption-only, absent, or
   `unknown` whenever processing did not establish one). The build spec states which successful evidence permits
   each substantive disclosure status. Then pair
   completeness, then pairs reconciled to point-in-time universe eligibility, executable prices and complete return
   horizons. Only that last count sets the backtestable population and span.
7. A survivorship audit: filing and pair coverage against the historical eligible universe including delisted
   names, with exclusions and missingness reported.
8. Availability: SEC acceptance time, separate from ingestion and extraction time.
9. The feature and execution rules (measure choice or combination, sign, missing-pair handling, signal date,
   holding horizon), registered in the programme ledger before outcomes are read, with the slice's gate criteria.
10. The corpus-change evidence: full-population A/B, smoke panel, cross-source check, backfill, live re-check.

### Slice 2: structured filing-item labels

A label producer, not a strategy: any factor or filter that uses its labels supplies its own hypothesis,
event-to-feature rule, persistence and expiry, horizon, track, power assessment and gate before reading outcomes.

The issue's red flags are checked for a structured source before any text rule (`.claude/CLAUDE.md` engineering
rules). Labels are named for what the item requires, not for a semantic finding:

- `non_reliance` = 8-K Item 4.02 (non-reliance on previously issued financial statements; not every restatement
  is filed under it).
- `accountant_change` = 8-K Item 4.01 (a change in certifying accountant; not by itself adverse).
- Item 2.02 (results of operations) is an event-timing marker for slice 3, not a red flag.
- Going-concern language: no structured field identified yet. This slice records the search (XBRL dei and auditor
  elements). If it finds none, going-concern detection becomes a slice-3 candidate task with its deterministic
  baseline (a rule over the auditor's report text).

Stored today (`eight_k_items` ⋈ `eight_k_filings`, 2026-10-06): Item 4.02 25 rows / 24 names from 2026-02-02;
Item 4.01 109 / 99 from 2025-12-04; Item 2.02 5,420 / 2,544 from 2025-02-25. The typed table starts in 2025.

**The slice-2 build spec must contain:** a coverage reconciliation of `filing_events.items` (the submissions
`items` array, sql/053) with a denominator (8-K events per year); explicit `unknown` for missing item metadata,
distinct from a known negative; a frozen missing-metadata policy and the periods and populations that remain
eligible after reconciliation; as-of label construction keyed to each accession's acceptance time, with the rule
for when an amendment or a metadata correction changes the effective label and how an earlier state is kept from
being rewritten; de-duplication.

**Estimand and guarantee** (added 2026-10-08 with the slice-2 build spec,
`docs/research/2026-10-08-3624-slice2-filing-item-labels.md`). No source the slice pins records when a historical
filing became public, so "keyed to each accession's acceptance time" means:
- every label belongs to one accession and is never available on or before that accession's acceptance date;
- its availability is a declared rule over the pinned sources (the documented filing-date and index rules plus a
  stated construction choice), which may be later than the real public time but never earlier than acceptance;
- a negative means no label among the rows the pinned sources hold, under that rule. Rows absent from every pinned
  source, filers named only in headers the inventory never reaches, and same-day corrections are known limits,
  stated with what is measured about them and restated by every consumer, not turned into `unknown`. The record
  that might remove them, the EDGAR Feed archive, is out of proportion for two labels.

### Slice 3: LLM extraction, sequenced last

**Step 3a, extraction pilot.** Outcome-blind: it reads no returns. It has its own registration in the ledger, a
capped spend, a stated sampling design over the target document population, independent hand labels with
adjudication, precision/recall with uncertainty bounds over attempted documents and per-label denominators, so
failed, malformed, abstaining and context-incompatible responses count against coverage under a failure and retry
policy frozen before the pilot runs, and a population census of document availability, size
and context fit used to extrapolate cost. Its permission covers extraction quality only, never a return study.

**Step 3b, return study. Wakes when** a registration exists for a named residual task (for example: guidance
raised / held / cut / withdrawn where the Item 2.02 exhibit carries no machine-readable guidance) with: the task
defined operationally; **a deterministic text baseline for that same task** (keyword or diff rules), distinct from
the slice-1/2 factor controls; the pilot's extraction results; the model comparison in §2; a design-specific power
assessment (§3); and a cost model over the full registered plan (self-consistency k, both writers, the document-size
distribution, retries, batch and cache assumptions). It does not require a slice-1 feature to pass.

**It carries every `llm-research.md` requirement unchanged:** event triggers with timestamped carry-forward;
point-in-time documents; Batches API with the rubric and schema in a cached prefix; raw outputs, prompts and model
ids persisted; (model, prompt, schema, document set) hashed into the version; precision/recall against independent
hand labels **and** the task's deterministic baseline; monthly rank IC, decay at 5/20/60/120 sessions, incremental
value over deterministic families, size splits, net returns after trading and inference cost, event-to-decision
latency; writer-model A/B by rank IC on dates after both cutoffs.

**Use.** Any output that can change a score, an exclusion, a weight or an execution is a selection input and must
clear its own forward validation. Unvalidated output never feeds those inputs. It may appear in the operator memo
only, labelled as unvalidated commentary; that bounds the system's use, not the reader's.

### Issue scope, item by item

| #3624 item | disposition |
|---|---|
| 1a. risk-factor change vs prior filing | slice 1 (deterministic); a semantic risk-change reading is a slice-3 candidate task |
| 1b. accounting red flags | slice 2: `non_reliance`, `accountant_change`, going-concern source search. Restatements outside Item 4.02 and adverse auditor findings are slice-3 candidate tasks |
| 1c. guidance change; customer, product and contract events; earnings direction | slice-3 candidate tasks |
| 1d. evidence quote and doc id per feature | §4 provenance rule, every slice |
| 2. within-industry relative outlook score | slice-3 candidate task (Track A unless replication is cited) |
| 3. self-consistency confidence | slice 3, priced in the cost model |
| 4. event-triggered Batches API, cached rubric, hashed version | slice 3 |
| 5. rank-IC validation, forward-only after cutoff | slice 3b; slices 1–2 validate on history |
| 6. absolute targets out of scoring | §1 completion condition |
| Codex 2026-10-04: precision/recall vs hand labels and a deterministic baseline; net returns after trading and inference cost; latency | slice 3a (labels) and 3b (baseline, returns, latency) |

## 6. Not done, and why

- Inference spend outside a registered slice-3a pilot (within its cap) or a slice-3b registration whose gates
  have passed (within the spend limit that registration states) (§3, §5).
- Pre-cutoff backtests of LLM output: contaminated by construction.

## Checkpoint log

- **Round 1** (Codex, 34 findings; `var/research/3624/ckpt1_round1.txt` in the loop worktree). Applied: 1–3
  (scoring-time provenance from the explanation; prevalence, not attribution; direct vs rank effects), 4 (the
  fallback's `price_target_mean` is passed as `None`), 5 (completion condition), 6–8 (availability floor;
  retirement stops extraction, not observation; one selection criterion), 9 (evidence quoted), 10–14 (track per
  candidate; power is a registration duty; the LLM slice is sequenced, not ruled out), 15–26 (slice-1 build spec
  contents), 27–28 (narrow labels; history needs reconciliation), 29–32 (wake condition; filters are selection;
  independent labels), 33–34 (item-by-item disposition; requirements carried unchanged).
- **Round 2** (Codex, 25 round-1 findings resolved, 9 open, 12 new; `ckpt1_round2.txt`). Applied: 2–3 (prevalence
  only; 238 rows potentially affected), 8 + 39 + 40 (model choice moved to registration, on eligible observations
  with completed horizons and a two-writer common window; Haiku compared if a campaign fits), 14 (separate
  assessments; illustration confined), 15 (census reaches universe, prices and horizons), 26 (one ledger, PBO/CSCV,
  sealed confirmation), 28 + 45 (slice-2 build spec: missing-metadata policy, as-of labels), 30 (a deterministic
  baseline per slice-3 task), 33 (every semantic deliverable allocated; provenance per slice), 35 (census unit is
  accession × section × extractor, processing vs disclosure status), 36 (pre-registered gates), 37–38 (outcome-blind
  pilot with its own registration and sampling design), 41 (Track B checklist), 42 (slice 2 is a label producer),
  43 (data-treatment hierarchy), 44 (lineage inventory, not an A/B, proves removal), 46 (full-plan cost model).
- **Round 3** (Codex, 3 open, 5 new; `ckpt1_round3.txt`). Applied: 8 (Haiku's earlier cutoff stated; preference
  left to registration), 33 (going-concern fallback to slice 3; provenance per slice reconciled with item 1d),
  38 (pilot denominators include failed and abstaining responses), 47 (3b spends under its own approved limit),
  48 (invariant covers every absolute target field, bull included), 49 (`unknown` disclosure status), 50 (pair
  provenance), 51 (unvalidated output bounded to labelled memo commentary).
