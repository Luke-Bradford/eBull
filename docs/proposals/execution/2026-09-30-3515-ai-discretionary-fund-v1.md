# AI-discretionary-fund-v1: the v1 trial plus point-in-time periodic-report fundamentals (#3515)

Status: spec v4. Codex ckpt-1 rounds 1–3 (82, 72, 58 findings); dispositions at the end. Refs #3471 (parent
trial, spec `docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`, "the v1 spec" below), #3515.

## 0. Binding constraint: fund-v1 runs after v1 and never edits v1's frozen code

Measured in code:
- v1 refuses every run whose code differs from its frozen policy. `ai_trial_run.declaration_refusal` compares the
  declaration's `policy_hash` with `AI_TRIAL_POLICY_HASH`, computed at import over the bytes of the ten
  `ai_trial_policy.POLICY_MODULES` (`ai_trial_decision`, `_guard`, `_intent`, `_invocation`, `_levels`, `_pack`,
  `_pack_reader`, `_plan`, `_prompt`, `_start_gate`) and the values of `FROZEN_CONSTANTS`. The jobs daemon reloads
  its child on any `app/**` change, so **an edit to those files that reaches `main` stops v1 at the next reload.**
- v1's step-0 preview refuses `trial_pool_non_trial_over_headroom` when open lifecycles other than v1's own legs
  exceed 6 (v1 spec §16.12). fund-v1's legs are designed for 12 slots each (§7), so at design load they are 24
  "non-trial" lifecycles to v1's code, 4× that headroom. Concurrency would refuse v1 as soon as fund-v1 filled, and
  removing that refusal means editing a hashed module.

Rules:
1. **No change to a `POLICY_MODULES` file or a `FROZEN_CONSTANTS` value merges until v1 is wound down** (rule 2's
   definition, not merely a terminal declaration: exits of open v1 positions still run v1 code). `scripts/ai_trial_policy_guard.py` (slice 3) prints `policy_hash()` of the working tree and
   the stored `policy_hash` of every v1 declaration not yet wound down, from the dev DB — the only deployment — and
   exits non-zero on a mismatch. Every fund-v1 slice runs it before merge and pastes the output in the PR.
   Residual: it is pre-merge evidence, not a deployment invariant; an unrelated PR can still edit a hashed file. The
   runtime backstop is v1's own `declaration_refusal`, which fails closed (v1 refuses to run) rather than running
   changed code. Scope, stated:
   the hash covers those files and constants only; a shared non-hashed module (executor, halts, readout) can still
   change v1's behaviour, so slices that touch one declare it and add a test on v1's path.
2. **fund-v1 starts only when v1 is fully wound down:** every v1 declaration terminal AND, on either v1 leg, no
   claimed-but-undecided run, no decided pair or executable intent awaiting an order, and no open or pending
   lifecycle, order or unresolved execution. fund-v1's start gate refuses `v1_not_wound_down`
   otherwise, including when a v1 declaration row is missing or in an unknown state.
3. **Resuming v1 after fund-v1 starts is not allowed.** It is a supervisor action (v1's state machine is SQL-side),
   not something fund-v1's code can block without editing v1; the rule is recorded here and in fund-v1's
   declaration, and fund-v1's start gate refuses `v1_not_wound_down` on every run, so after a v1 resumption fund-v1
   opens nothing new. Residual, stated: fund-v1 positions already open then overlap v1 until they exit, and the
   check and a concurrent supervisor action are not atomic. Both are procedural risks under the supervisor rule.
4. The comparison with v1 is therefore **descriptive only** (§1).

The build is off v1's critical path: v1 runs at least until it halts or completes 60 sessions.

## 1. What this is

**Question.** v1's pack carries price structure, crowd, the house ranking and filing *titles*
(`ai_trial_pack_reader.filing_title`); no statement figures and no report text. v1 is not a chart-only baseline:
the ranking's quality and value families already summarise fundamentals.

**Estimand (the only inferential one).** The augmented strategy against random selection: mean over jointly
executed pairs of *d* = net_arm − net_ctl, where the arm is the model reading v1's pack plus two blocks (statement
figures §3, MD&A text §4) and the control is fund-v1's own random pick under fund-v1's own terms (v1 spec §1, §7,
§16.4). Same test, stopping rules and readout code as v1. It does **not** identify the contribution of the figures
or the text, separately or jointly; nothing in the readout may say it does.

**Side by side with v1, never a contrast.** The readout prints v1's and fund-v1's readouts next to each other
(pairs, clusters, abstentions, refusals, *d*, arm net, regime cohorts, windows). No difference is tested and no
sentence may call one version better: the windows differ and neither trial is powered for a difference.

**Tier.** Demo-tier, purpose `demo_trial`, `falsification_only`, as v1.

## 2. Source rule

Skill sections read first (`.claude/skills/data-sources/sec-edgar.md`): §1 companyfacts shape, the nightly
`companyfacts.zip` refresh, submissions `recent` columns (`reportDate`, `acceptanceDateTime`); §7.16 the single
extraction chokepoint (rejects periods outside `[1900-01-01, 2100-01-01)` and `period_start > period_end`); §7.17
companyfacts carries **non-dimensional facts only**, so absence means not extracted, never not disclosed; §11.5 /
§11.5.1 `sec_10q` is a synth no-op because "narrative HTML has no v1 consumer", which an MD&A consumer replaces.
`.claude/skills/data-sources/edgartools.md` (10-K / 10-Q `filing.obj()`).

- **Report identity comes from `filing_events`, facts from `financial_facts_raw`.** `filing_events` rows are the
  submissions index (form, filing date, `report_date` = submissions `reportDate`, the period the filing reports).
  The facts table's own `form_type` / `filed_date` / `fiscal_year` / `fiscal_period` are stored but **not used**:
  the upsert (`app/services/fundamentals/__init__.py`, `_UPSERT_FACT_SQL_SUFFIX`) skips the whole update unless
  `val` or `frame` changes, so metadata-only corrections never land there.
- **Knowledge time = our ingestion time.** Facts: `financial_facts_raw.fetched_at` (default `now()`, NOT NULL,
  re-stamped when `val` or `frame` changes; a restatement in a later filing is a separate row because the conflict
  key includes `accession_number`). Events: `filing_events.created_at`. Text: `periodic_report_sections.fetched_at`.
  - **Cutoff contract: snapshot-after-claim, not claim-time.** The job claims the run at `as_of` (v1 spec §3.2),
    then opens one REPEATABLE READ transaction for both blocks and records its start as `snapshot_at` on the run;
    every read filters its knowledge column `≤ as_of`. The run refuses `snapshot_too_late` if `snapshot_at − as_of`
    exceeds `MAX_CLAIM_TO_SNAPSHOT` = 15 minutes (by construction; v1's pack build runs inside the same job). The
    base pack's reads are v1's code in their own statements, each bounded by `as_of`; they are not in this
    snapshot, so the blocks and the base pack are coherent to `as_of`, not to one snapshot. Rows committed after `as_of`
    with a stamp `≤ as_of` (Postgres `now()` is transaction start) are visible if committed before the snapshot.
    That is information from the claim-to-read window — minutes, and before the model call, which is the decision.
    The v1 claim-time contract is weakened by exactly that window, stated. Measured exposure: over the 30 days to
    2026-09-30, fact rows landed in UTC hours 02, 14, 15, 18, 22 and 23; the hour-23 rows were one day, 23:18–23:22,
    before the 23:30 job (query: `fetched_at` grouped by UTC hour and minute, last 30 days). Overlap is possible,
    not routine.
  - **Overwrites.** A value corrected in place by a transaction that began after `as_of` has a stamp `> as_of`, so
    its key is absent from the block (the old value is gone); one that began before `as_of` and committed before
    the snapshot is visible with its new value (the window above). The run record (not the pack, which must not
    carry post-cutoff information) stores `withheld_after_as_of` per shown accession — rows with `fetched_at >
    as_of`, whether overwrites or late inserts — for audit. A report all of whose qualifying facts were overwritten
    stops being a candidate; that is visible only in the audit count. Residual: a replay at an earlier `as_of`
    cannot reconstruct an overwritten value; the stored pack is the record.
  - Residuals: `filing_events` has no correction clock (`created_at` only), so a later change to any of its fields
    (form, dates, instrument association) is not detectable after the fact. A fact the SEC later drops from
    companyfacts is never deleted by the upsert and stays readable. Events and facts arrive by different jobs, so a
    report can be known as an event before its facts (§3 `newer_report_without_facts`).
- **Taxonomy.** The facts conflict key omits `taxonomy` and the update does not set it. `sec_facts_concept_catalog`
  holds 3 of K's names under `ifrs-full` as well; a collision needs one accession of one instrument tagged under
  both taxonomies for the same key, and the eligible population's stored rows carry 0 K-named rows under any
  taxonomy other than K's (§9). K is matched on `(taxonomy, concept)`. Residual: an overwrite across taxonomies
  would keep the old label and is undetectable from the row.
- **Forms: originals only** — `10-K`, `10-KT`, `10-Q`, `10-QT`. Rule 12b-15 requires an amendment to "set forth the
  complete text of each item as amended", so an amendment need carry only the items it amends and is not
  guaranteed to be a complete report. By construction this version shows originals; residuals: an amended report
  is shown as originally filed (the block flags `amendment_filed` when a later `/A` with the same `report_date` is
  known by `as_of`), and a later original may carry restated comparatives, which are shown as that report tagged
  them. Admission is a policy under the 2026-08-22 steer ("audited periodic accounts stay in scope"). Stated
  precisely: 10-Q interim statements are **reviewed, not audited** (Reg S-X Rule 10-01(a)(1), (d)); MD&A is
  unaudited narrative. Transition reports (10-KT, 10-QT) are the same forms filed for a changed fiscal period
  (Exchange Act Rules 13a-10 / 15d-10) and are selected as their base form; consequence, stated: a transition
  report can be shown in place of a full-year or full-quarter report, and its `period_start` / `period_end` show
  the shorter span. 20-F / 40-F / 6-K and every 8-K item stay out; in particular an Item 4.02 non-reliance notice
  is not seen, so an original can be shown after its statements were declared unreliable and before any
  amendment.
- **Concept names shown as filed; no alias map, no derived fields, no TTM.** #3360's ckpt-1 history is the reason:
  alias priority and derivation are constructions an arm must justify against a published definition, and this
  trial has no factor to define. Revenue is split across two concepts in the population (§9).
- **MD&A source rules.** 10-K Item 7 and 10-Q Part I Item 2 are Reg S-K Item 303. Interim MD&A (303(c)) covers
  "material changes in financial condition and results of operations" since the periods it names, so a 10-Q's MD&A
  presumes the preceding annual discussion; the pack shows the latest report's MD&A only and says so in the system
  prompt. Item 303 mandates no topic order ("any presentation that in the registrant's judgment enhances a reader's
  understanding"), so a first-N cut keeps whatever the filer put first; that is the stated bias of §4. Form 10-K
  General Instruction G(2) lets Item 7 be incorporated by reference from the annual report to security holders,
  filed as an exhibit under Reg S-K Item 601(b)(13) (G(2) Note 2), and Rule 12b-23 (cited by G(1)) allows
  incorporation by reference "in answer or partial answer to any item of a report", so a pointer can stand in any
  periodic report's MD&A. This version does not follow exhibits; §4's substance floor keeps a pointer from counting
  as coverage.
- **Text extractor: edgartools item access** (pinned `edgartools==5.30.2`). Reuse check, 2026-09-30, four latest
  filings, compared with our only in-house extractor (`business_summary.extract_business_sections`, Item 1 only,
  regex-bounded, patched per case in #550). A smoke test that the API returns items, not a population claim; the
  population census is the producer's acceptance (§4).

  | filing | item | chars |
  | --- | --- | --- |
  | AAPL 10-K `0000320193-25-000079` | Item 7 | 18,018 |
  | AAPL 10-K | Item 1A | 68,163 |
  | JPM 10-K `0001628280-26-008131` | Item 7 | 396 (G(2) incorporation by reference) |
  | JPM 10-K | Item 1A | 112,862 |
  | GME 10-Q `0001326380-26-000055` | Part I, Item 2 | 38,501 |
  | HD 10-Q `0001628280-26-058715` | Part I, Item 2 | 26,285 |

  ⚠ 10-Q keys must be part-qualified: bare `Item 2` is ambiguous with Part II Item 2.
- **Everything else is by construction** (v1 spec §2), frozen in fund-v1's policy manifest.

## 3. Pack block `fundamentals` (per name)

**Candidate report.** A `filing_events` row of the name with `filing_type` in the four original forms,
`created_at ≤ as_of` — duplicates are detected at this stage: **exactly one** such row per
`(instrument_id, accession_number)`, else excluded and counted `ambiguous_event` — then non-NULL `report_date ≤`
the `as_of` date (`filing_date` and `provider_filing_id` are NOT NULL columns); and at least one finite us-gaap K
fact of that accession with `fetched_at ≤ as_of` **whose `period_end` equals `report_date`** (a current-period
statement figure, so a report carrying only comparatives or the dei cover count cannot displace a statement
report). The event row supplies `form_type`, `filing_date`, `report_date`.

**Selection.**
- Order within a kind by (`report_date`, `filing_date`, `accession_number`), all descending; the accession number
  is a deterministic tie-break, not a chronology claim.
- `R_a` = first annual (10-K, 10-KT); `R_q` = first quarterly (10-Q, 10-QT).
- Show `R_a` if it exists; show `R_q` if it exists and (`R_a` is absent or `R_q.report_date > R_a.report_date`).
- `newer_report_without_facts`, per kind: when the newest event of a kind (same event rules and order, ignoring
  the fact condition) is not the report shown for that kind — and, for a quarter, is not already superseded by the
  shown annual — its accession and `report_date` are listed. This includes a name with no shown report. No figures
  from it; the model is told a newer report exists.
- No age bound, by choice: stale reports stay admissible and their dates are shown (§9 gives the ages).

**Facts per report.** K rows of that accession with `fetched_at ≤ as_of`: drop non-finite `val` (NUMERIC admits
`NaN` and `±Infinity`; §9: 0), counted `skipped_non_finite`; drop us-gaap facts with `period_end > report_date`
and the dei `EntityCommonStockSharesOutstanding` fact (the cover-page count) with `period_end > filing_date`, counted `excluded_future_dated` (§9: the only post-report
facts in the census are dei cover counts); then order and cap. Each fact: `fact_id`, `taxonomy:concept`, `unit`,
`period_start` (null for instants), `period_end`, `end_minus_start_days` (null for instants; named for what it is,
not an XBRL duration convention), `val` (exact NUMERIC string: the XBRL fact value in the stated unit, not scaled
to thousands or millions as a filing's HTML tables often are). `decimals` is not shown: it is stored metadata the
upsert never corrects (§2).
These are the facts companyfacts attributes to that accession as extracted by the chokepoint (current period and
any comparatives it tagged), not the filing's full tag set. Figures are as filed; we apply no adjustment, and any
retrospective adjustment the filer made stands. `dei:EntityCommonStockSharesOutstanding` is the cover-page count
as of its own `period_end`.
- Order: round-robin across concepts — rank each concept's facts by `period_end` desc, `period_start` desc nulls
  first, `unit`, then order by (rank, K's declared order). A cap therefore cuts the deepest comparatives of every
  concept before any concept's newest fact.
- Cap `FACTS_PER_REPORT_MAX` = 120 after the drops; beyond it the tail is cut, `truncated: true` and the full count
  shown. It did not bind in the 2026-09-30 census (max 118 per report, 15 for one concept); the freeze re-runs the
  census (§9).

**Per report:** `accession_number`, `form_type`, `filing_date`, `report_date`, `amendment_filed`, `known_at` = max
over the event's `created_at` and every K fact of the accession with `fetched_at ≤ as_of` (shown or not).

**`amendment_filed`** = a `filing_events` row of the same instrument with `filing_type` = the report's form + `/A`,
the same `report_date`, `created_at ≤ as_of`, and `filing_date ≥` the report's. Residual: an amendment whose event
row lacks or misstates `report_date` is not associated.

**K (frozen, 15, in this order):** us-gaap `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`,
`GrossProfit`, `OperatingIncomeLoss`, `NetIncomeLoss`, `EarningsPerShareDiluted`,
`NetCashProvidedByUsedInOperatingActivities`, `PaymentsToAcquirePropertyPlantAndEquipment`, `Assets`,
`Liabilities`, `StockholdersEquity`, `CashAndCashEquivalentsAtCarryingValue`, `LongTermDebtNoncurrent`,
`CommonStockSharesOutstanding`; dei `EntityCommonStockSharesOutstanding`. By construction: headline lines of the
three statements plus share count; no comparability claim across issuer types; delivered coverage in §9.

**Absence.** No candidate report → `fundamentals: null`, reason `no_candidate_report`.

## 4. Pack block `mdna` (per name) and its producer

**Producer: separate spec and ticket, #3518** ( corpus rung: its own ckpt-1, full-population census,
Definition-of-Done clauses 8–12). A new store `periodic_report_sections` filled from the primary document of each
eligible name's original periodic reports, parsed by the edgartools item accessor on HTML fetched through the house
SEC client (shared rate clock, sec-edgar §4). Codex round 1 findings 26–29, 31–40, 42 and round 2 findings 43, 47,
48, 51 are handed to that spec verbatim. **This spec fixes the interface the pack reads:**

| field | meaning |
| --- | --- |
| `row_id` | unique, monotone |
| `instrument_id`, `accession_number`, `section_id`, `extractor` | identity; `section_id` = `10-K:Item 7` for 10-K / 10-KT, `10-Q:Part I, Item 2` for 10-Q / 10-QT |
| `status` | `extracted` / `item_absent` / `fetch_failed` / `parse_failed` / `invalidated` (names the row it invalidates) |
| `body` | NULL unless `extracted`, then non-empty |
| `fetched_at` | when the outcome was recorded |

Rows are **append-only** (no UPDATE, no DELETE; the producer spec enforces it with a trigger), so a row's body can
never change under its original `fetched_at`. `row_id` order is the deliberate chronology for rows of one identity.

**Target.** The name's newest original event under §3's event rules (exactly one row, `created_at ≤ as_of`,
`report_date` set and not after `as_of`), same order. No target → `mdna: null`, reason `no_target_report`.

**Row choice.** Among the target's rows for the `section_id` of its form family and fund-v1's frozen `extractor`,
with `fetched_at ≤ as_of`, excluding rows an `invalidated` row (itself `fetched_at ≤ as_of`) names: the latest
`extracted` row by `row_id` whose normalised text is non-empty (a later failed retry, or a later extract that
normalises empty, never hides an earlier usable success); else the latest row by `row_id`, whose status (or
`empty_after_normalisation`) becomes the reason; no row → `not_yet_extracted`. **Never an older report's text**: stale text shown
as current is worse than a stated absence.

**Text.** Normalise the body: tabs and other horizontal whitespace → one space each; strip remaining control
characters except newline; collapse runs of spaces to one; collapse three or more newlines to two; trim.
(Separators survive, and newlines survive so a markdown row stays a row; fixed-width layout and a table cut by the
cap are residuals the producer spec's table-fidelity work does not remove.) `full_chars` = normalised length. If
`full_chars ≤ MDNA_MAX_CHARS` = 6,000, show it all; else cut at the last whitespace at or before 6,000, or at
exactly 6,000 if there is none, and set `truncated`. The bias of a first-N cut is stated in §2; `full_chars` tells
the model how much it did not see.

**Provenance:** `row_id`, `accession_number`, `form_type`, `filing_date`, `report_date`, `section_id`,
`extractor`, `known_at` (the row's `fetched_at`), `same_report_as_fundamentals` (target = the newest report §3
shows; false when §3 is null), `amendment_filed` (§3's rule applied to the target).

**Coverage gates, per run.** Evaluated once the pack-complete shortlist exists and is non-empty (an empty shortlist
follows v1's path; a run refused before the shortlist records NULL shares). With n names, a run is refused before
the model call when fewer than ⌈0.8·n⌉ (`MIN_BLOCK_SHARE` = 0.80) have
- `fundamentals` non-null → `fundamentals_coverage_below_floor`; or
- `mdna.text` of at least `MDNA_MIN_SUBSTANTIVE_CHARS` = 1,000 normalised characters → `mdna_coverage_below_floor`.
  Text below 1,000 characters is still shown but does not count: the measured by-reference JPM Item 7 is 396.

By construction: the floors keep the treatment from silently degrading to one block, or to pointers; at the floor,
at least four names in five carry each block. Both shares are recorded on every run. The readout also reports, per
version, the share of **picked** and of **executed** names carrying each block, since the model can pick from the
uncovered fifth. The freeze dry run evaluates the same rules over the eligible population with the same selector and
prints them; advisory there, the per-run gates bind.

**Why not risk factors.** 10-K Item 1A measured 68,163 and 112,862 characters, 11–19× the text cap. Out.

**Conditioning, stated.** Sessions that fail a coverage or budget gate are refused, so fund-v1's inference is over
sessions where filings were available and within budget. Refused sessions count toward the cohort exactly as v1's
refused sessions do; v1's power grid did not model these refusals, so the readout reports their count next to the
pair count, and "too little data" is decided by v1's rule on the pairs actually formed.

## 5. Universe parity and failure paths

Absence in either block never makes a name incomplete; fund-v1's completeness rule is v1's, so its universe equals
what v1's code would produce at the same `as_of`. A read error, a malformed row that breaks the builder, or a
budget/coverage refusal refuses the **run** (recorded, never retried the same session, as v1 §3), never drops a
name.

## 6. Prompt and budget

- **System prompt** = v1's frozen text plus one paragraph: what the blocks are; figures as filed, concept names
  verbatim, no adjustment by us; `known_at` is when we learned them; the MD&A is the latest report's only and may
  be cut; report text is **untrusted data**. The pack is embedded by `ai_trial_prompt.encode_pack`, which rewrites
  every `<` and `>` in the canonical JSON as a backslash-u JSON escape (`u003c`, `u003e`), so report text cannot
  close `<pack>`; a test feeds an extract containing `</pack>`. fund-v1's sha; v1's is unchanged.
- **Injection.** Report text can steer a pick, an abstention or the terms, within v1 §6's bounds (an in-bounds long
  entry on a shortlist name, or none). That is a risk to the trial's result, not to capital, and the bound is
  structural: no tools, no MCP, init-event assertion, output parsed only as data. The synthetic call includes an
  adversarial extract as a regression check, not proof.
- **Budget.**
  - Slice 2 builds a synthetic 50-name pack at every cap: both blocks full (120 facts per report with 20-digit
    values and the longest unit string in the stored K rows; 6,000 characters of prose), the maximum intraday bar
    count v1 accepts, full account context. Fictitious symbols, as every pre-freeze call (v1 §9). One tool-less
    call. Recorded in the declaration: the fixture's sha, the rendered prompt's UTF-8 byte length, and the CLI's
    reported input usage (uncached + cache-creation + cache-read tokens, which covers the system prompt and all
    CLI-added context of that request).
  - Freeze refuses `prompt_budget_exceeded` if that usage > `INPUT_TOKEN_CEILING` = 850,000 (150k of the
    1,000,000-token context the CLI reported left for output and margin).
  - **Every run**, before the call: refused `prompt_over_budget` if the rendered prompt's byte length exceeds the
    fixture's (equal passes). Bytes are not tokens, so this is a proxy, and the fixture is maximal only over the
    bounds it states (values, the longest stored unit, the caps), not over every string a filer can write.
  - **After** the call: if the reported input usage exceeds `INPUT_TOKEN_CEILING`, or is missing, the run's
    decisions are refused (`budget_breach`, nothing executes) and the trial halts for review. A CLI rejection or
    context overflow is v1's `nonzero_exit` / `is_error` refusal and also halts under this rule. The model id is
    asserted per call (v1 §4 layer 3) and the CLI version recorded; a different context capacity for the declared
    model is a new strategy version.
  - Cost is stored per run. v1 measured $0.57 for 68,908 input tokens.

## 7. Legs, capacity, declaration

- **Strategy ids** `ai-discretionary-fund-v1` / `ai-discretionary-fund-v1-control`, purpose `demo_trial`, own
  deployments and O9 policy parity. `strategy_manifest.DEMO_TRIAL_STRATEGY_IDS` (not hashed) gains them;
  `ai_trial_run`'s import-time assertion that the set equals v1's two ids becomes "contains". The readers
  (`ai_trial_intent`, which is hashed; `ai_trial_executor`; `strategy_paper_executor`) key on
  `registered_strategy_purpose(...) == "demo_trial"`, so the new ids are refused by every capital path.
- **Version identity.** v1's code binds identity in non-hashed modules: `ai_trial_run.TRIAL_ARM_STRATEGY_ID` (used
  by `load_declaration`), `declaration_refusal`'s default `policy_hash`, `ai_trial_freeze`, `ai_trial_jobs`,
  `ai_trial_halts`, `ai_trial_readout`. Slice 3 introduces a version descriptor (arm id, control id, version,
  policy-hash function, pack builder, system prompt) and threads it through those modules, with v1's descriptor
  reproducing today's values. Every query that loads declarations, runs, pairs, account context, halts, cohort
  counts or readouts keys on the declaration id or the descriptor's ids, so persisted rows of one version never
  reach the other. Hashed modules are called as they are, with the descriptor's values passed as arguments; where
  a hashed function hard-codes v1's identity, fund-v1 wraps it in a new module rather than editing it.
- **Capacity:** v1's terms, no generalisation (no overlap, §0): 12 slots and $3,000 per leg, pool concurrency
  design 2 × 12 + 6 = 30.
- **Control, halts, cohort, stopping, harm floor:** v1's functions and constants under fund-v1's declaration.
- **Declaration and manifest.** fund-v1's own #2599 row (`falsification_only`, v1's stamps), its own `DeclaredTrial`
  `ai-discretionary-fund-v1` (`searches=1`, `EXACT`), `TRIAL_REGISTER_VERSION` bumped (v1 spec O14). fund-v1's
  policy manifest hashes its new modules, v1's ten hashed modules, the non-hashed shared modules slice 3 changes,
  and its constants (K, forms, selection rule, `FACTS_PER_REPORT_MAX`, `MDNA_MAX_CHARS`,
  `MIN_BLOCK_SHARE`, `MDNA_MIN_SUBSTANTIVE_CHARS`, `MAX_CLAIM_TO_SNAPSHOT`, `INPUT_TOKEN_CEILING`, the extractor version, the budget fixture's sha and measured
  pair). The declaration also records, per v1 hashed module, whether its sha equals the sha in v1's declaration
  document, so the side-by-side readout can state whether the two versions ran the same **hashed** v1 modules —
  and only that; shared non-hashed modules changed by slice 3 differ by construction. Residual: modules outside
  both manifests (e.g. the broker provider) are not frozen by either.
- **Multiplicity, stated:** each version charges one search. If either result is ever cited as strategy evidence,
  the register holds both charges and the citing work applies the register's deflation.
- **Ordering:** freeze before any real-shortlist fund-v1 model call.

## 8. Slices

1. **Producer** — #3518, its own spec (§4). The fund-v1 freeze cannot pass without it: the per-run MD&A gate refuses
   every run until the store is filled.
2. **Blocks + prompt** — new module: pure builders and reader SQL. Tests: PIT (a fact, a value update, an event and
   an extract row stamped after `as_of` are absent, and `withheld_after_as_of` counts the update); a
   claim-then-commit interleaving inside the claim-to-read window (visible, by the §2 contract); selection
   (annual-only, quarterly-only, late annual for an old period, amendment ignored and flagged, duplicate event
   excluded, dei-only report ignored, `newer_report_without_facts`); non-finite drop before the cap; truncation
   order; MD&A row choice (failed retry after success, tie by `row_id`, empty after normalisation, no target,
   cut at whitespace and with none, invalidated success, section filter, normalisation keeps tab separators);
   coverage gates at n = 1, at ⌈0.8·n⌉ − 1 and ⌈0.8·n⌉, and a 999-character pointer; universe parity; the
   `</pack>` test; the budget fixture call; the byte gate with a prompt one byte over (refused) and equal (passes);
   `snapshot_too_late`. Rung: behavioural (ckpt-2).
3. **Version plumbing** — descriptor (§7), fund-v1 job and start gate (`v1_not_wound_down` cases: active,
   terminal with an open lifecycle, missing row, unknown state), halts/readout/status/Invest panel by version,
   `scripts/ai_trial_policy_guard.py`, and end-to-end routing tests driving a fund-v1 decision through control
   draw, capacity, execution intent and halts with v1 rows present, asserting no v1 row is read or written.
   Rung: behavioural (ckpt-2).
4. **Readout side by side, then the freeze dry run.** Expected refusals: `v1_not_wound_down` while v1 runs, which is
   correct; none after. `--apply` is the supervisor's step.

## 9. Full-population verification (premise)

`PYTHONPATH=. uv run python -m scripts.measure_3515_fund_pack_coverage` — one REPEATABLE READ snapshot, `as_of` =
2026-09-30 15:32:23Z (US market hours, so quotes were fresh), scores run `v1.5-balanced` 2026-09-30 09:08:37Z,
population by the production functions (`read_scores_run`, the `read_shortlist` candidate query,
`drop_symbol_collisions`, `is_eligible`): **1,410 eligible names**. It applies §3's event and selection rule,
including the current-period condition and the per-taxonomy date drops. The cap never bound, so delivered counts
are post-cap. (An earlier run at 14:47Z, with pre-market quotes, had 1,328 eligible names and the same shape.)

- Original-form events (`created_at ≤ as_of`): 49,734 accessions; 0 with more than one event row, 0 with NULL
  `report_date`, 0 with `report_date` after `as_of`.
- K concept names stored under another taxonomy: 0. Non-finite `val` among K rows: 0.
- Names with ≥ 1 shown report: **1,410**. An annual is shown for 1,409 and a quarter for 1,342; one name is
  quarterly-only.
- Facts per shown report after §3's drops: p50 30, p99 61.5, **max 118**, against the cap of 120. The most facts
  for one concept in one report is 15.
- K facts dated after `report_date` in shown reports: 2,373. All are dei cover counts, none is us-gaap, and 4
  are dated after the filing date (dropped).
- Report age (`as_of` − `report_date`):
  - annual: p50 273 days, p90 273, max 638;
  - quarterly: p50 92, p90 95, max 1,644 (the quarterly-only name).
- Names flagged `newer_report_without_facts`: 98.
- Delivered, names per concept:
  - Revenues 530; RevenueFromContract…Excluding 870 (either: 1,202; both: 198); GrossProfit 561;
    OperatingIncomeLoss 1,017; NetIncomeLoss 1,347; EPS diluted 1,358;
  - operating cash flow 1,399; capex 966;
  - Assets 1,408; Liabilities 1,181; StockholdersEquity 1,331; cash 1,191; LongTermDebtNoncurrent 644;
  - us-gaap shares 1,041; dei shares 1,259.
- Ingestion lag over the 1,336 shown reports filed in the last 120 days, measured as the first surviving
  `fetched_at` minus the filing date at NY midnight: p50 0.94 days, p90 1.94, p99 17.8.
  - This describes the ingest path. `fetched_at` can move on a correction, so it is the first *surviving* stamp,
    not the first arrival.
- The MD&A target equals the newest shown report for 1,315 of 1,410 names.
- The cap's headroom is small (118 against 120). Round-robin order (§3) means a cut removes the deepest
  comparatives first.
- A dev-DB snapshot is not the future population, and the eligible count moves with quote freshness within a
  day. The freeze dry run re-runs this script and records its output in the declaration. During the trial, the
  per-run gates (§4, §6) are what bind.

## 10. Out of scope

Risk factors; MD&A incorporated by reference into an exhibit; amendments as reports; 20-F / 40-F / IFRS; 8-K
content of any kind; aliasing or derived metrics; any change to v1's frozen terms, hashed modules or running trial.

## 11. Security model

No new broker surface: fund-v1 reuses v1's execution tier; every order path keeps the unattended-worktree refusal
and the execution guard; the new ids carry purpose `demo_trial`, refused by every capital path. New external input:
filer-written report text, treated as untrusted data inside the escaped `<pack>` delimiters and bounded by v1's
containment and validation (§6). The producer reads public SEC documents through the shared rate-limited client
with the configured User-Agent; no credential. The status endpoint keeps v1's auth.

## Ckpt-1 disposition

**Round 1 (82 findings).**
- Fixed by §0 (sequential, no hashed edits): 64, 69, 72–79. Fixed by §1 (side-by-side): 65–68, 70, 71, 81; 66 (the
  ranking already summarises fundamentals).
- Fixed in §2/§3: 3–13, 15, 17–22 (event-sourced identity, originals only with Rule 12b-15 cited, report-date order,
  deterministic tie-break, per-instrument commit, `days`, cover-date note, no adjustment by us, declared order with
  a cap above the measured max, non-finite drop, `fact_id`).
- 1, 2: partly rebutted, now stated as the §2 cutoff contract (claim-to-read window) with `withheld_after_as_of`.
- 14: rebutted — dates are shown; a staleness filter is an invented threshold. §9 reports ages per kind.
- 16: narrowed — K by construction, no comparability claim, delivered coverage measured.
- Text, pack side: 23–25, 30, 41, 43–45 fixed in §4. Producer side: 26–29, 31–40, 42 → producer spec.
- Budget: 46–49 fixed in §6. 50: rebutted (`encode_pack` escapes both brackets), test added. 51: framing fixed (§6).
- Measurement 52–63: fixed (production eligibility, one snapshot, §3 rule, K-restricted delivered counts, revenue
  union, NY-midnight lag). 80: §7. 82: §8.

**Round 2 (72 findings).**
- §0: 1 fixed (design load 24 ≫ 6 headroom, derived); 2 (hash scope stated, guard script, shared-module tests); 3
  (stops at the next reload); 4, 5 (guard compares with every non-terminal declaration on the only deployment); 6–8
  (wind-down gate incl. open lifecycles and unknown states); 7 (resumption rule, and the gate re-checks every run);
  9 (wording); 10 (§8 no longer says "new modules only"); 11–13 (§7 version descriptor, isolation by declaration,
  manifest covers changed shared modules); 14 (per-module sha comparison recorded).
- §1: 15 fixed (estimand is the augmented strategy vs random; no contribution claim).
- §2/§3: 16, 17 fixed as the cutoff contract; 18 (`withheld_after_as_of`); 19 (same, per accession); 20, 21
  (metadata from `filing_events`; facts' metadata columns stored but unused); 22 (0 collisions measured; matched on
  taxonomy); 23 (`known_at` over all K facts and the event); 24 (residual stated); 25 (≥ 1 us-gaap fact); 26
  (`newer_report_without_facts`); 27, 28 (rebuttal kept; ages split by kind in §9); 29–31 (12b-15 wording,
  `amendment_filed`, restated comparatives); 32 (no adjustment by us); 33 (`days` defined); 34 (companyfacts-as-
  extracted wording); 35 (`decimals`); 36 (exactly one event row, 0 duplicates measured); 37 (report date not after
  `as_of`; chokepoint rejects reversed or out-of-window periods, §7.16); 38, 39 (drop then cap; a report with no
  finite us-gaap fact is not a candidate).
- §4: 40, 41 (target rules and `no_target_report`); 42 (transition forms mapped; Rules 13a-10 / 15d-10 cited); 44
  (success wins over a later failure); 45, 46 (normalisation and cut defined); 49, 50 (Item 303(c), G(2) and Item
  601(b)(13) cited; limitation stated); 52–55 (per-run shortlist gate replaces the population floor; freeze figure
  advisory); 56 (first-N bias stated, Item 303 mandates no order); 57 (producer ticket #3518). 43, 47,
  48, 51 → producer spec (row identity, table fidelity, source-byte provenance, extraction quality).
- §6: 58 (per-run byte refusal is a stated proxy; post-call usage breach halts the trial); 59 (fixture field
  bounds); 60 (usage definition); 61 (escape description corrected); 62 (framing: steers the result, not capital).
- §9: 63–65, 67 fixed (event filters, event metadata, duplicate count, post-cap counts); 66 (±Infinity counted); 68
  (first surviving stamp, stated); 69 (wording: did not bind in the snapshot); 70 (freeze re-runs the whole
  script).
- §5: 71 fixed (failure refuses the run, never drops a name). §8: 72 fixed (tests listed).

**Round 3 (58 findings).** Codex: "sequential execution, a version descriptor, and the within-version estimand are
sound directions."
- §0: 1 (guard lasts until wind-down), 4 (claimed runs, decided pairs, intents in wind-down), 5 (residual stated;
  v1's `declaration_refusal` fails closed), 2, 3 (overlap and non-atomicity stated as procedural residuals).
- §7: 6 (end-to-end routing tests), 7 (claim limited to hashed modules), 8 (unfrozen modules stated).
- §2 cutoff: 9 (renamed snapshot-after-claim; `snapshot_at` recorded), 10 (overwrite wording by transaction
  start), 11 (`snapshot_too_late` at 15 minutes), 12 (base-pack coherence to `as_of`, stated), 13–15
  (`withheld_after_as_of` moved to the run record; the pack carries no post-cutoff count), 16, 20, 21 (residuals
  broadened: event fields, SEC-dropped facts, event/fact arrival order).
- §2/§3 data: 18 (catalog shows 3 K names under `ifrs-full`; stored collisions 0; residual stated), 19, 34
  (`decimals` dropped; `val` stated as the unscaled XBRL value), 22 (current-period fact required), 23 (future-dated
  facts dropped; census shows they are all dei cover counts), 24, 25 (duplicate detection before date validity;
  `filing_date` and accession NOT NULL), 26–28 (per-kind `newer_report_without_facts`, incl. no shown report), 29, 30
  (amendment association rule, applied to the MD&A target too), 32 (round-robin order), 33
  (`end_minus_start_days`), 35 (Item 4.02 residual), 36 (transition consequence stated), 37 (Rule 12b-23 cited).
- 31: rebuttal reworded — no age bound is a choice; stale reports stay admissible with dates shown.
- §4: 38, 40 (1,000-character substance floor; rationale corrected), 39 (fundamentals gate), 41 (picked/executed
  coverage in the readout), 42 (n, ⌈0.8·n⌉, empty shortlist, NULL on early refusal), 43, 44 (conditioning and
  power stated), 45 (section filter), 46, 49 (append-only; `row_id` chronology deliberate), 47 (empty extract never
  hides a usable one), 48 (`invalidated` status), 50 (tabs become spaces first), 51 (residual stated), 52 (#3518;
  the MD&A gate makes the freeze depend on it).
- §6: 53, 54 (proxy and fixture bounds stated), 55 (model asserted, capacity change = new version), 56, 57
  (missing usage / CLI error: decisions refused and trial halts), 58 (boundary tests specified).
