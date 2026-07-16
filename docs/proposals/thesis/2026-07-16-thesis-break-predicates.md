# Machine-checkable thesis break predicates (#2012)

Status: proposal (rev 4 — Codex ckpt-1 rounds 1-3 folded; operator-approved 2026-07-16, split
blessed). Ships as two PRs: **PR-A** = Shape 1-5 (pure core, schema, scan, stale rule, job);
**PR-B** = Shape 6 (alerts endpoint + FE). Parent: #2002 (meta-thesis epic).
Siblings: #2013 (re-thesis diff, SHIPPED), #2014 (DQ audit, SHIPPED), #2010 (prompt v3 —
writer-native predicates, follow-on), #1988 (staleness v2 — trigger semantics reconciled below).

Purpose: `break_conditions_json` is prose. A closed, trust-verified metric vocabulary lets a
nightly deterministic job evaluate the subset that IS machine-checkable, arm it against a
baseline, and emit a break event on a genuine transition → re-thesis. No LLM in the gate.

## Premise falsification (FULL population, dev 2026-07-16)

The issue's premise is substantially wrong. Verified over ALL 325 theses / 1,343 conditions /
282 latest-per-instrument ("fire population") — never a sample.

1. **"nothing evaluates it" — HALF-FALSE.** `app/services/portfolio.py:617` already reads
   `break_conditions_json` in EXIT rule 1: `if break_conditions and max_red_flag >=
   EXIT_RED_FLAG_THRESHOLD` — as a *boolean presence flag*. 325/325 theses have a non-empty
   array → the term is a **tautology**; EXIT rule 1 reduces to `max_red_flag >= threshold`.
   Latent; see "Out of scope".

2. **Break ≠ bearish.** A break condition invalidates ITS OWN thesis, so direction tracks stance:

   | stance | conditions | upside | upside % |
   | --- | --- | --- | --- |
   | avoid | 452 | 100 | **22.1%** |
   | watch | 671 | 35 | 5.2% |
   | buy | 126 | 5 | 4.0% |
   | hold | 94 | 1 | **1.1%** |

   Monotonic across stance — not noise. The issue's "feeds EXIT evaluation" would fire EXIT on
   `Altman Z-score improves to >2.99` for an `avoid` thesis. Unsafe → Design 1.

3. **The EXIT target barely exists.** 5 held names have a thesis (1 buy, 1 hold, 2 watch, 1
   avoid). EXIT wiring has ~no surface; the payoff is re-thesis triggering over 282 names.

4. **Prose ceiling.** 905/1,343 conditions contain a number; 626 have operator+number; 318 have
   no metric keyword at all ("Loss of key patents", "Key product trials fail to meet clinical
   endpoints"). No vocabulary reaches those — fail-open-to-prose is correct.

5. **The flagship example is not connectable as stated.** Of 75 threshold-bearing short-interest
   conditions, **57 name "float" / "public float"**; only 5 say "shares outstanding". Float is not
   ingested (source rules 1-3) → Design 4.

6. **⚠ Break conditions are predominantly ALREADY TRUE at write time.** Altman conditions on the
   latest thesis per instrument, joined to that instrument's CURRENT Altman band:

   | current band | altman conditions | worded "crosses into distress / <1.8" |
   | --- | --- | --- |
   | **distress** | **48** | **35** |
   | safe | 8 | 2 |
   | grey | 6 | 4 |
   | (no z) | 2 | 1 |

   35 conditions say "Altman Z-score crosses into bankruptcy territory (<1.8)" for names whose Z
   is ALREADY ≈ −16. A level-triggered evaluator emits ~35 false events on night one, each
   costing a wasted re-thesis (~300s LLM queue). **This makes the arm/baseline mechanism
   mandatory, not an optimisation** → Design 5. It is also a NEW writer-defect class for #2014.

## Source rules

1. **FINRA bimonthly Equity Short Interest file** — `.claude/skills/data-sources/finra.md:11,25`.
   Cadence: 15th + last business day. Exact 14-column pipe-delimited header (empirically verified
   2026-05-18 against `shrt20260430.csv`): `accountingYearMonthNumber|symbolCode|issueName|
   issuerServicesGroupExchangeCode|marketClassCode|currentShortPositionQuantity|
   previousShortPositionQuantity|stockSplitFlag|averageDailyVolumeQuantity|daysToCoverQuantity|
   revisionFlag|changePercent|changePreviousNumber|settlementDate`. **No float. No shares
   outstanding.** `daysToCoverQuantity` + `changePercent` are computed BY FINRA → they need no
   denominator of ours and carry zero imputation risk. Mirrored 1:1 in
   `finra_short_interest_current` (#915).
2. **Settled invariant — `app/services/instrument_analytics.py:325`**: "`short_pct` =
   current_short_interest / shares_outstanding (**public float is not ingested**, so the
   denominator is shares outstanding — caveat carried)". Module header (line 18): "Missing inputs
   are reported as missing — **NEVER imputed**." This spec REUSES that denominator decision; it
   does not re-decide it, and it does not extend it to prose that names a different denominator.
3. **SEC Form 10-K cover page** — the aggregate market value of common equity held by
   **non-affiliates** is stated as of the last business day of the registrant's most recently
   completed **second fiscal quarter**; shares outstanding is stated separately as of the latest
   practicable date. So `dei:EntityPublicFloat` (→ `financial_periods.public_float_usd`, present
   for 279/282 fire names) is (a) USD not shares, (b) up to ~18 months stale, (c) a different
   population (non-affiliate) than shares outstanding. NOT a live float denominator. Not used.
4. **Altman Z″ non-manufacturer recalibration (Altman 2000)** — cited at
   `instrument_analytics.py:16-17`; financial firms are outside the model. Code suppresses via
   `suppress_fz = gics_sector == "Financials"` (line 549).
5. **prevention-log:2158/2159** (#2043): "A predicate's INPUT COLUMNS must be trust-verified for
   the affected population — a correct concept keyed on a correlate inherits that correlate's data
   weakness." `financial_periods.revenue` is under-captured for banks/REITs (UDR $2.5M quarterly
   vs real ~$420M). Applied per-metric below.
6. **settled-decisions "EXIT rule in portfolio manager"**: EXIT for thesis break / severe risk
   event / valuation target achieved. Preserved — Design 1.
7. **settled-decisions "Thesis versioning"**: append-only, new row per generation. Supplies the
   event model's idempotency — Design 5.
8. **#2013 `app/services/thesis_diff.py`**: materiality is single-sourced there. NOT re-defined
   here; a break event is a distinct kind (pre-generation trigger) from a diff event
   (post-generation observation).

## Design

1. **A break is a TRIGGER, not a VERDICT.** Break fires → thesis stale (new reason `break_fired`)
   → `thesis_refresh` regenerates → the regenerated stance/targets drive the portfolio through the
   EXISTING EXIT path, surfaced by the shipped #2013 diff. Break events are NOT wired to EXIT.
   This dissolves finding 2 (the regenerated thesis speaks for itself — no stance-aware EXIT
   logic), preserves source rule 6 ("thesis break" still reaches EXIT, via regeneration), and
   keeps execution deterministic + gated. It is the #1988 reconciliation the issue asks for:
   break = a 5th rule in `find_stale_instruments` beside `event_new_10k/10q/8k` (#273) — same
   trigger bus, same drain, same batch limits.
2. **Closed vocabulary = trust-verified columns only.** A metric earns a slot only with a
   full-population coverage figure, an explicit freshness bound, AND an input-column trust verdict
   (source rule 5).
3. **Extractor is minimal + precision-only; #2010 owns the real fix.** Writer-native predicates
   need a prompt change → `_PROMPT_VERSION` v4→v5 → eval re-gate = #2010's pass. The extractor
   bridges that with the *durable* model: #2010 emits INTO this same `BreakPredicate` vocabulary,
   so nothing here is throwaway but ~40 lines of regex. **Recall is explicitly not a goal.**
4. **"of float" AND bare-denominator conditions FAIL-OPEN.** 57 conditions name a denominator we
   do not ingest; a further ~12 state a bare percentage. Evaluating either against
   shares-outstanding answers a *different question* and systematically under-reports (shares_out
   ≥ float always ⇒ SI%shares_out ≤ SI%float; a "10% of float" break could fire at 4% of shares
   outstanding and we would miss it). Source rule 2 forbids the substitution — and the writer's
   own qualified usage is 57 float : 5 shares-outstanding, so the *unqualified* prior is
   overwhelmingly float. Only conditions EXPLICITLY naming "shares outstanding" extract
   (Codex ckpt-1 BLOCKING). **The real fix is upstream**: `app/services/thesis.py:557` passes
   `positioning` verbatim, so the writer IS handed the caveat "% shares outstanding (public float
   not ingested)" and writes "of float" anyway — a fabrication class (#2014 family). #2010 kills
   it at source.
5. **Arm/baseline; edge-triggered; idempotent; no hysteresis.** Mandatory per finding 6.
   - On a predicate's FIRST evaluable scan, record its evaluation as the BASELINE and emit NO
     event. Baseline `true` → `already_true` (the writer's own premise, not a break) → **never
     fires**. Baseline `false` → `armed`. Fire on the first subsequent `false → true` transition.
   - **Ever-pending guard + rollout grace.** `already_true` is justified ONLY when the baseline is
     contemporaneous with thesis creation: the predicate evaluated on its FIRST scan (never
     `pending`) AND `baselined_at − thesis.created_at ≤ 48h` (grace = one nightly cadence + slack).
     Otherwise a first-`true` evaluation has **no prior false baseline**, so "writer's premise" and
     "transition missed during the unobserved gap" are indistinguishable *regardless of how short
     the pending gap was* — label it `already_true_after_gap`: operator-visible, counted
     separately, still non-firing (we cannot prove a break). NB two independent gap sources, both
     guarded: (a) ever-`pending` (stale input at first scan — NOT a lag-vs-freshness-bound
     comparison: a 3-day gap under a 10-day bound conflates just as badly, Codex ckpt-1 rev3 HIGH;
     dev has 194/282 names on stale prices); (b) the scan job simply not existing yet — at ROLLOUT
     every pre-existing thesis baselines days-to-weeks late, so the first scan lands existing
     theses in `after_gap`, honestly. Steady-state (nightly scan, theses ≤24h old at first scan)
     lands in `already_true`.
   - **Re-arm.** An `already_true` / `already_true_after_gap` predicate whose LATER scan observes
     `false` re-arms (state → `armed`): the premise has genuinely resolved, so a subsequent
     `false → true` is a REAL transition and may fire. Fires still originate only from `armed`.
   - **Accepted residual gap:** even a never-pending in-grace baseline reads CURRENT data at the
     first scan, up to 48h after thesis creation (the grace bound), so a transition inside that
     window is attributed to the premise. Bounded and documented; closing it would need
     point-in-time reads of `price_daily`/FINRA archives, which v1 declines.
   - `UNIQUE (thesis_id, predicate_index)` on the event table. Theses are immutable + append-only
     (source rule 7) and a break begets a NEW thesis with fresh predicates, so a fire happens at
     most once per predicate per thesis version. Level-triggered would re-fire nightly on
     oscillators (RSI-14 around 70). Cohort hysteresis was refuted on the full population in
     #2031 — no state machine beyond the baseline flag.
   - This subsumes Codex's "regime vs cross" finding: `sma_50_vs_sma_200` is a REGIME predicate; a
     regime already holding at arm baselines to `already_true` and cannot fire on the writer's own
     premise. Cross-EVENT wording is not modelled — it extracts as the regime or not at all.
6. **Fail-closed on stale/missing input, with an explicit bound per INPUT (not per metric).** A
   predicate whose input is absent or stale evaluates to `no_input` / `stale_input` — never
   `false`, never a fire, and never a baseline (a predicate cannot arm on data we do not have; it
   retries next scan). Distinguish permanently-absent (`sma_200` needs 200 trading days —
   structurally absent for 72/282) from transiently-stale (dev has only 88/282 fire names with
   price fresher than 10d — the known eToro-unreachable artifact, same class as #2014's
   `stale_price_anchor` 145).
   - **Per-input evidence.** A metric may be a RATIO of independently-bounded inputs
     (`short_interest_pct_shares_out` = FINRA ≤45d ÷ share count ≤6mo). A single scalar
     `observed_as_of` cannot audit a conjunctive freshness invariant (Codex ckpt-1 rev3 MED), so
     `MetricObservation` carries `inputs: {name: {value, as_of, source}}` and the event persists it
     as `inputs_json`. Follows the `fair_value_band_observations.basis_json` precedent and CLAUDE.md
     repo discipline ("persist enough structured evidence for auditability").
   - Scalar `observed_as_of` is retained as the **stalest** contributing input's as-of — one
     sortable field the acceptance gate can assert against the bound; `inputs_json` proves it.
7. **Whole-string match only — composites fail open.** The extractor anchors on the FULL condition
   string. Any residual content, conjunction or disjunction (`and`/`or`/`with`/`while`/`plus`)
   → prose. "Short interest exceeds 12% of float **with rising days-to-cover**" must never extract
   as the weaker "SI > 12%" (Codex ckpt-1 HIGH). Measured cost: 17 RSI + 20 SMA conditions drop.
   A composite expression model is explicitly out of scope for v1.
8. **FINRA in-place revisions.** `revisionFlag='Y'` corrections land within 1-2 cycles
   (`finra.md:116`). A fired event records `observed_value` + `observed_as_of` as evidence at fire
   time; a later revision does NOT retract the event (append-only, auditable). Noted, not fixed.

## Closed vocabulary (v1)

Coverage = fire population (282 latest theses), dev 2026-07-16.

| metric | source | coverage | freshness bound | trust verdict |
| --- | --- | --- | --- | --- |
| `short_interest_days_to_cover` | `finra_short_interest_current.days_to_cover` | 271 (96%) | `settlement_date` ≤ 45d (2 missed bimonthly cycles) | **Accept** — computed BY FINRA; no denominator of ours (source rule 1) |
| `short_interest_change_pct` | `finra_short_interest_current.change_percent` | 271 (96%) | `settlement_date` ≤ 45d | **Accept** — computed BY FINRA; period-over-period |
| `short_interest_pct_shares_out` | `current_short_interest / share_count_history.shares_outstanding` | 251 (89%) | **BOTH inputs bounded**: FINRA `settlement_date` ≤ 45d AND `share_count_history.latest_filed_date` ≤ 6 months | **Accept, narrow** — reuses the settled denominator (source rule 2). Extracts ONLY for conditions explicitly naming "shares outstanding" (5). Float + bare → fail-open (Design 4). The DENOMINATOR needs its own bound (Codex ckpt-1 rev2 MED): a ratio is only as fresh as its stalest input, and a stale share count mis-evaluates across splits/issuance. `dei:EntityCommonStockSharesOutstanding` is stated on EVERY 10-K/10-Q cover as of the latest practicable date (source rule 3) → quarterly cadence → 6 months = 2 missed filings. NB FINRA carries `stockSplitFlag` (source rule 1) — a split between share-count filings is the concrete hazard |
| `altman_z` / `altman_band` | `_read_latest_two_fy_facts` + `altman_z2()` (REUSED pure fn) — **NOT** `scores.analytics_json` | 2,211/3,084 non-financial scored | fact `period_end` ≤ 15 months (annual cadence + 10-K filing lag) | **Accept, sector-gated.** Sourced from FY facts, not `analytics_json`, because that blob carries no fact `as_of` — `scores.scored_at` would make stale annual facts look fresh (Codex ckpt-1 BLOCKING). `observed_as_of` = fact `period_end`. Financials suppressed upstream (584/594). **Real Estate NOT suppressed (0/224)**: GICS split Real Estate out of Financials in 2016, so `sector_classification.py:84,87` maps SIC 65xx/6798 → XLRE → "Real Estate" and the line-549 guard misses them. Altman Z″ excludes financial firms (source rule 4) → this spec ALSO gates SIC 65xx/6798. Gate applied at PREDICATE INSERT: `altman_*` predicate rows are never created for SIC 60-64 / 65xx / 6798 instruments (condition stays prose) — a permanently-unevaluable `pending` row would be a lie. NB: REIT distress rate is 46.2% vs 48.8% for Other (n=26) — the exclusion is a source-rule call, NOT a demonstrated artifact. |
| `rsi_14` | `price_daily.rsi_14` (latest) | 281 (99.6%) | `price_date` ≤ 10 calendar days | **Accept, freshness-gated** (Design 6) |
| `price_vs_sma200` | `price_daily.close` vs `sma_200` | 210 (74%) | `price_date` ≤ 10 calendar days | **Accept, freshness-gated**; 72 structurally absent (<200d history) |
| `sma_50_vs_sma_200` | `price_daily.sma_50` vs `sma_200` | 210 (74%) | `price_date` ≤ 10 calendar days | **Accept as REGIME** (Design 5); the #1989 `derive_trend_signals` regime, read-derived |

### Rejected from the issue's proposed vocabulary

| metric | why rejected |
| --- | --- |
| `revenue_ttm_yoy` | prevention-log:2159 — `financial_periods.revenue` under-captured for banks/REITs. **50/282 (18%) of the fire population is Financial/REIT.** A "revenue collapsed" break would false-fire on exactly that class. Reinstate only if the revenue-capture defect is fixed. |
| `short_interest_pct` (of float) | Float not ingested (source rules 1-3); imputing shares-outstanding is banned by source rule 2. 57 conditions → fail-open; fixed at source by #2010. |
| `insider_net_shares_90d` | **Deferred.** `get_insider_summary` exists, but #828 (7,569 stray `insider_filings`, high-blast, unimplemented) is an open DQ overhang on precisely this table. Source rule 5 requires input-column trust first. Follow-up, gated on #828. |

## Shape

1. `app/services/thesis_break.py` — pure, no DB, no `thesis` import (no cycle):
   - `extract_predicates(conditions: list[str]) -> list[BreakPredicate | None]` — whole-string,
     precision-gated (Design 7). Index-aligned with `break_conditions_json` (None = prose).
     Frozen `BreakPredicate(metric, op, threshold, unit, source_text)`.
   - `evaluate_predicate(pred, observation) -> BreakEval` — pure compare over a supplied
     `MetricObservation(value, as_of, status, inputs)` where `inputs` is the per-input evidence map
     (Design 6); returns `fired` / `not_fired` / `no_input` / `stale_input`. Table-tested.
2. `app/services/thesis_break_scan.py` — DB-facing: one read assembling vocabulary observations
   for the fire population, then pure calls; applies arm/baseline (Design 5) + freshness bounds
   (Design 6). Mirrors `thesis_dq_audit.compute_thesis_dq_report`'s split (#2014 precedent).
3. `sql/230_thesis_break_predicates.sql` (228/229 taken by fvb) — two tables:
   - `thesis_break_predicates(thesis_id, predicate_index, metric, op, threshold, unit,
     source_text, baseline_state, baselined_at)`, PK `(thesis_id, predicate_index)`, FK → `theses`.
     `baseline_state` ∈ `pending` (awaiting evaluable input) | `armed` | `already_true` |
     `already_true_after_gap` (Design 5 lag guard). Only `armed` can ever fire.
   - `thesis_break_events(break_event_id, thesis_id, instrument_id, predicate_index, metric, op,
     threshold, observed_value, observed_as_of, inputs_json, fired_at)`, **UNIQUE (thesis_id,
     predicate_index)** (Design 5), FK → `thesis_break_predicates`. `observed_as_of` = stalest
     input; `inputs_json` = per-input evidence (Design 6). Additive; auto-applies at boot.
4. `find_stale_instruments` rule 5 → `break_fired` (`StaleReason` widened). **Must key the event to
   the LATEST thesis by thesis_id equality** — join `event.thesis_id = latest_thesis.thesis_id`,
   and ONLY that. An `instrument_id`-only join would re-stale every regenerated thesis forever
   (Codex ckpt-1 BLOCKING). A `fired_at > latest_thesis.created_at` timestamp filter is NOT an
   acceptable substitute and must not be used: a delayed scan can stamp an OLD thesis's event with
   a `fired_at` later than its replacement's `created_at`, re-staling the new thesis (Codex ckpt-1
   rev2 HIGH). Ordered AFTER the filing-event rules so a break never masks a 10-K trigger, but
   BEFORE the generic cadence rule — a fired break is the more specific reason and must not be
   shadowed by mere age (Codex ckpt-2); every path regenerates either way.
5. Nightly job `thesis_break_scan` in `SCHEDULED_JOBS`, `db` lane, **NOT 5-min-aligned** (#1707
   tick-race; #2014 used 05:12 → use 05:22). `row_count` = events emitted.
6. `GET /alerts/thesis-breaks` — event feed + `POST /seen` cursor mirroring #2013's
   `/alerts/thesis-changes` (`operators.alerts_last_seen_*_id`, rank-moves cursor semantics).
   FE: `AlertsStrip` BREAK card + `ThesisPane` predicate rows (fired = amber, `already_true` =
   muted with a "premise, not trigger" tooltip).

## Out of scope (surfaced, not silently absorbed)

- **The `portfolio.py:617` tautology** (finding 1) — real, but an EXIT-path change on a 5-name
  population, and Design 1 deliberately does not touch EXIT. Follow-up: EXIT rule 1's
  `break_conditions` term should be dropped as dead, or replaced by "an unfired-but-armed →
  fired `thesis_break_events` row exists for the latest thesis".
- **Writer defects for #2010 + #2014**: "of float" fabrication against a supplied caveat (57);
  already-true-at-write conditions (~35). Both are prompt problems; this spec's censuses are their
  empirical brief.

## Full-population verification / acceptance gate

Measured over the SAME population the prediction names (prevention-log:2155).

- **Extractor precision: EVERY match on the full 1,343 hand-audited pre-merge; target 100%
  precision.** Recall is explicitly NOT a gate.
- **Day one delivers predicate STATUS, not events.** Expected: ~0 events on the first scan (every
  predicate baselines). ~106/282 latest theses (38%) carry ≥1 extracted predicate under the
  pre-Codex rules; the tightened rules (Design 4 + 7) cut this — the post-tightening figure is
  measured and recorded on the PR before merge, over the 282 fire population.
- **Premise census recorded on the PR** — the expected ~35 Altman premise-conditions land across
  `already_true` + `already_true_after_gap` COMBINED (reported split). At rollout the grace rule
  puts pre-existing theses in `after_gap` by construction (Design 5); steady-state new theses land
  in `already_true`. This census is the headline day-one operator-visible number and #2010's brief.
- `already_true_after_gap` is never merged into `already_true` in any surface. A non-trivial count
  on dev is EXPECTED (rollout + 194/282 stale prices) — an honest "cannot tell", not a defect.
- Per-metric `fired`/`not_fired`/`no_input`/`stale_input` census over the 282 fire population.
- Sector gate: 0 `altman_*` predicates evaluated for SIC 60-64 or 65xx/6798.
- Fail-closed: 0 events with a stale input, asserted **per input from `inputs_json`** against that
  input's own bound — NOT via the scalar `observed_as_of`. For `short_interest_pct_shares_out`
  that means FINRA `settlement_date` ≤45d AND `share_count_history.latest_filed_date` ≤6mo checked
  independently (Design 6; a conjunctive invariant is not provable from one collapsed field).
- Stale-rule join: a regenerated thesis with no NEW break must NOT report `break_fired`
  (regression test — Codex ckpt-1 BLOCKING 1).
