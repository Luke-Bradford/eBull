# #3046 residual 5 — the contract for verdict-aware raw price reads

Scope item 2 of #3046, re-scoped by residual 4: *"does a raw reader go through the
masked loader, or does it get its own verdict-aware helper?"* — with the masked-loader
option already struck off, because `price_masked_bars` does not carry transitions.

Read-only. Nothing here changes `app/services/price_quarantine.py`: it sits in
`INPUT_RULE_SETS` (#3031), so editing it rotates every strategy identity and empties
the 76M-bar backtest substrate, and only the first of those is visible.

⚠ This revision incorporates Codex checkpoint 1 (42 findings). The corrections that
changed the design are called out inline as **[ckpt-1]**; the headline one is that the
question's own framing of "three surfaces" was wrong — there are **four** damage kinds
and they **compose**, they are not alternatives a consumer picks between.

## Source rule

The governing rules are the repo's own, not an external reg. Cited before design, per
`.claude/CLAUDE.md`'s source-rule clause. All line numbers verified at write time.

- **`app/services/price_quarantine.py:5-6`** — *"WHAT QUARANTINE IS. It identifies 'this
  return is not a return'. It NEVER identifies a cause."* A transition verdict is a
  statement about a **ratio**, never about the level of either bar.
- **`app/services/price_quarantine.py:20-27`** (S7 §4) — two verdicts per bar, and they
  are **per field**: `B1`/`B4` set `return_usable = false`, `B2`/`B3` set
  `range_usable = false` only. Folding them over-rejected by 587 windows in the S7 draft.
  **[ckpt-1 #3]** "the bar is unusable" is therefore never the right summary; the verdict
  names a field.
- **`app/services/price_quarantine.py:28-33`** — *"CONTAINMENT, NOT CLASSIFICATION. T3
  rejects legitimate data at every threshold … An admitted transition is not-known-bad,
  never known-good."* **[ckpt-1 #4]** So T3 does **not** establish that the scale changed;
  it establishes that the ratio is not usable as a return. The contract must not upgrade
  it.
- **`sql/247_price_quarantine.sql:83-88`** (design-doc decision 10) — *"Bars either side
  of a level break are valid prices in their own unit regime — it is the ratio between
  them that is not a return. So the transition is quarantined and both bars are kept."*
  **[ckpt-1 #5]** This is scoped to a **level break**. It says nothing about the validity
  of a T1 endpoint (which a bar rule has already condemned) or of a fabricated T2
  endpoint, and is not cited here for either.
- **`sql/247_price_quarantine.sql:23-33`** — the sparse-table invariant and the
  fail-closed coverage contract: *"Absence of a row therefore has TWO possible meanings —
  'evaluated and clean' or 'never evaluated' … So the read helper is FAIL-CLOSED against
  this table."* **[ckpt-1 #7]** This governs the clause-3 loader and was missing from
  draft 1.
- **`sql/247_price_quarantine.sql:70-75`** — the same invariant on `price_bar_quarantine`:
  a row exists only if it says something. And `price_transition_quarantine` stores
  **admitted** rows with empty `rules` as census evidence, so the quarantine predicate is
  `cardinality(rules) > 0`, not row presence. **[ckpt-1 #8]** — the predicate
  `scripts/verify_3046_consumer_exposure.py:1076` already uses.
- **`sql/246_price_adjustments_and_series_breaks.sql:4-10`** — *"a quarantined transition
  that turns out to have an active adjustment row on its date is RECLASSIFIED from
  `quarantined` to `adjusted`. That is a resolution step, not an input to the
  classifier."* **[ckpt-1 #6]** `price_series_break.resolved_by` (`sql/246:110-111`:
  *"NULL = unresolved = the two sides cannot be joined"*) carries that resolution, and
  `price_segments.load_unresolved_breaks` already honours it. A W1 read over raw stored
  transitions would **not**, and would refuse correctly-adjusted crossings.
- **`app/services/price_quarantine.py:525-537`, `rule_w1`** — the crossing predicate,
  `(window_start, window_end]` on the transition's LATER date.
- **`app/services/price_quarantine.py:540-556`, `rule_w2`** — window calendar span more
  than 2× its nominal trading-day span, from `bar_count` and `ClassParams`.
- **`app/services/price_masked_bars.py:175-192`** — the masked loader's own fail-closed
  contract (*"an unevaluated instrument is not 'clean', it is unchecked"*), its per-field
  masking, and the open-by-value clause (#2354). **[ckpt-1 #15]** `volume` is returned
  **unchanged**; the loader has no volume axis.

## There are FOUR damage kinds, and they compose

This is the correction that reshapes the answer. **[ckpt-1 #1, #10]** Draft 1 said a
consumer "picks by what it computes". It does not pick — a windowed return consumer needs
all four to hold at once, and W2 fires on windows carrying **zero** quarantined
transitions.

| # | damage | rule | operand it needs | carrier today | production caller modules |
| --- | --- | --- | --- | --- | ---: |
| 1 | a bar FIELD is unusable | B1–B4 | the bar's own verdict | `price_masked_bars` | 3 |
| 2 | the two sides cannot be joined | T3 → `price_series_break` | unresolved break dates | `price_segments` | 4 |
| 3 | the window SPANS a non-return | **W1** | quarantined transition dates | **none** | **0** |
| 4 | the window's HORIZON is stretched | **W2** | window bounds, bar count, `ClassParams` | **none needed** | **0** |

Caller counts are module counts at this commit: `load_masked_bars` —
`strategy_signal_scan`, `strategy_outcome_resolution`, `strategy_forecast_outcome_resolution`;
`load_unresolved_breaks` — those three plus `backtest_run`. **[ckpt-1 #36]**

⚠⚠ **`rule_w1` and `rule_w2` have no production caller at this commit.** They are written,
unit-tested (`tests/test_price_quarantine_rules.py:417-433` — W1 at 419/423/426, W2 at
430/433), and named in the module header as part of the versioned rule set. The only
non-test caller anywhere in the tree is
`scripts/verify_3046_consumer_exposure.py:976,1153`, a measurement script.
**[ckpt-1 #39]** Stated as a property of this commit, not of execution history, which
source inspection cannot establish.

⚠ Draft 1 also claimed every `app/` import of `price_quarantine` takes `RULE_SET_VERSION`
alone. **That is false** — `app/services/research_corpus_ingest.py:69-73` imports `Bar`
and `evaluate_series`. **[ckpt-1 #37]** The surviving claim is the narrower and sufficient
one above: no production caller of the two W rules.

That inverts the residual's framing. For damage 3 the missing piece is **not a rule and
not a second masked view** — it is a **loader**. For damage 4 nothing at all is missing:
the consumer already holds the window bounds and the bar count, and `params_for` is a
pure call. Damage 4 is a **pure adoption gap**.

## A fifth axis, orthogonal to all four: WHERE the window is evaluated

**[ckpt-1 #12]** Residual 4's inventory classifies two consumer kinds — `DERIVED_COLUMN`
and `COMPOSED` — that read **one row** of `price_daily` and take a trailing-window value
(`return_6m`, `sma_200`, `rsi_14`) off it. Those consumers cannot apply any of the four
clauses: the window was evaluated elsewhere, by
`app/services/market_data.py:1147::_compute_and_store_features`, at write time.

So the contract binds **at the point the window is evaluated**, which for every stored
derived column is the writer. A read-path clause aimed at those consumers would certify
today's single bar and say nothing about the history the column was computed over.

**[ckpt-1 #13]** Composed and cross-series consumers (beta and the `excess_*` family
against a benchmark, portfolio P&L across positions) evaluate a window per **operand
series**; the clauses apply per operand, and a clean instrument can still inherit damage
from its benchmark. Residual 4 measured that specific case and found benchmark
propagation is 0 **by containment** — all 14 `BENCHMARK_SYMBOLS` are clean over their
whole history — which is a present-corpus fact, not an invariant.

## What this proposal deliberately does NOT decide

**[ckpt-1 #14, #17, #18, #29]** Named so they are not read as covered:

- **Calendar damage** — fabricated dates that alter rebalance selection, warm-up or
  annualisation while passing every B/T/W check. #2797 fixed the S-2/S-10 consumer
  (`app/services/strategies/s2_cross_sectional_momentum.py:192-218`) and left the data;
  #3046 residual 2 owns the write path.
- **The enforcement shape of a refusal** — which computation stops, what reason code it
  carries, how it propagates to the API and the frontend, and the rejection denominator
  the containment rule requires be published. That belongs with the first consumer.
- **Identity and replay for the new handling.** Leaving `price_quarantine.py` untouched
  keeps `INPUT_RULE_SETS` from rotating, but a consumer that starts refusing windows
  changes what it produces. The follow-up needs its own version/replay rule; this
  proposal only asserts that the rule module does not move.
- **Live vs research corpus.** **[ckpt-1 #16]** All of this is the LIVE corpus, keyed on
  `instrument_id`. `research_price_daily` is keyed on `series_id` with its own verdict
  tables and is out of scope.

## The damage test for T2 is NOT magnitude — a re-offence guard

⚠⚠ Do **not** classify a T2 transition by whether its ratio clears the class magnitude
threshold. Codex checkpoint 1 killed exactly that discriminator on this ticket two
sessions ago, on two counts that both still apply:

1. `price_quarantine.py:9-10` — *"Magnitude is a trigger, not a verdict."*
2. The constant is calibrated on a same-scale move between **adjacent** bars. A T2 pair
   spans a hole, so a legitimate cumulative return clears 5× with nothing wrong anywhere.

The shipped adjudicator is `scripts/verify_3046_archive_continuity.py`'s discrepancy
`d = our_ratio / archive_ratio`, and its verdict is that of **244 candidate** T2 rows only
**8** are adjudicable at all (4 agree, 4 disagree); the rest have no non-eToro series.
**[ckpt-1 #40]** — 244 is a candidate set, never an adjudicated one.

**So whether a given T2 is a real move or a scale error is, on our data, undecidable.**
The contract therefore cannot be "repair T2". It can only be what the rule set already
does: contain it, and tell the consumer.

**[ckpt-1 #30-#34]** Draft 1 proposed measuring the weekend-sentinel share of that
candidate set to sequence residual 2. **That arm is dropped.** It cannot establish
sequencing (a selected subset is not the affected-window population), its predicate
conflates fabricated with stale-observed levels where residual 1 already ships a
provenance classifier, it had no venue scope (crypto trades weekends, and #2797's rule is
US-equity scoped), and "of our own making" overstates provenance — `_upsert_candles`
stores provider bars. Residual 1's figure stands on its own and is cited rather than
restated.

## The measurement

`scripts/verify_3046_contract_decision.py` — **proposed, not yet run** **[ckpt-1 #42]**.
Full population, read-only, one `REPEATABLE READ READ ONLY` transaction, every figure
computed rather than written down. Population boundary and the stale/mixed-version split
are printed in the header, because filtering to the current rule-set version can silently
remove the population that needs examining **[ckpt-1 #25]**.

Each arm exists to decide one clause, and each has an acceptance condition stated ahead
of the run **[ckpt-1 #35]**:

- **Arm A — is T1 fully visible in the bar table?** KEY-level reconciliation in **both**
  directions **[ckpt-1 #23]**: (i) every T1 transition has ≥1 endpoint carrying a
  `price_bar_quarantine` row with `return_usable = false`; (ii) every adjacent stored bar
  pair inside coverage with such an endpoint carries a T1 transition row. Denominators
  printed for both. **Accept clause 1 as covering T1 only if both are exact.** A one-way
  count can pass vacuously.
- **Arm B — is T3 fully carried by the operand `price_segments` loads?** KEY
  reconciliation on `(instrument_id, price_date)` ↔ `(instrument_id, break_date)`, both
  set differences, **not three counts** — missing and unrelated rows cancel in a count
  comparison, which is the prevention-log rule on key reconciliation **[ckpt-1 #26]**.
  Reported in three populations: all breaks, unresolved breaks, and T3 transitions.
  **Accept clause 2 as covering T3 only if T3 \ breaks is empty**; resolved breaks
  legitimately outliving their T3 verdict is expected and reported, not a failure
  **[ckpt-1 #27]**.
- **Arm C — size clause 3 and prove its coverage contract is needed.** Quarantined
  transitions (`cardinality(rules) > 0`) by rule; how many sit on instruments with **no**
  coverage row at the current rule-set version, and how many on a **stale** version. That
  last number is the fail-closed population: a loader returning an empty tuple for them
  would report "clean" for "never checked". **Accept the fail-closed clause if that
  population is non-zero**; report it either way.
- **Arm D — is W2 a separate damage kind on OUR corpus?** Count instrument-windows that
  trip `rule_w2` while carrying **zero** quarantined transitions, over the trailing
  windows residual 4 enumerated. **[ckpt-1 #10]** Codex demonstrated this synthetically
  (20 equity bars at 3-day spacing: no T verdict, `W2 = True`); this arm establishes
  whether it is real here. **Accept the four-kind decomposition if that count is
  non-zero.** If it is zero, W2 collapses into W1 on this corpus and the contract has
  three clauses, not four.
- **Arm E — how much would a naive W1 over-refuse?** `price_series_break` rows with
  `resolved_by IS NOT NULL`, and the transitions on their dates. **[ckpt-1 #6]** That is
  the population a loader without the `sql/246` reclassification would wrongly refuse.
  **The loader must exclude them regardless of the count**; the count sizes the error.

⚠ **Dropped from draft 1: the T1 bracket arm.** It compared the last usable close before
a run of return-unusable bars with the first usable close after. Three independent
objections, all correct **[ckpt-1 #19, #20, #21, #22]**: it models **deletion** where the
loader **masks** (and `price_masked_bars.py:14-20` expressly forbids filtering because it
shifts every N-bar window); it had no defined comparator, so it could not separate a
legitimate cumulative return from a scale error — the same invented discriminator killed
above, re-derived under another name; equal endpoints do not establish window safety,
because offset errors cancel while `rule_w1` still fires; and its run arithmetic was
wrong (an interior run of three return-unusable bars yields **four** T1 transitions, not
three). Arm A replaces its role: it establishes that T1 sites are **visible**, which is
all the contract needs, and claims nothing about repair.

⚠ **Not established by any arm, and stated rather than assumed** **[ckpt-1 #24]**:
snapshot consistency is not verdict freshness. A stored verdict can be stale against a
revised bar while coverage bounds are unchanged. The arms measure what is stored.

## The result — all five arms, at `89163aee`

Reproduce with `PYTHONPATH=. uv run python -m scripts.verify_3046_contract_decision`.
7,002,478 bars, frontier 2026-09-14, rule set `price-quarantine-v1+49ff29fea766`,
`contract-decision-v2`. No figure below is written by hand anywhere in the script.

| arm | result | acceptance |
| --- | --- | --- |
| A — T1 ↔ bar table | **355/355 forward, 355/355 reverse** | ✅ clause 1 covers T1 |
| B — T3 ↔ break | 412 T3, 412 breaks, all unresolved; **both set differences 0** | ✅ clause 2 covers T3 |
| C — W1 population | **4,268** transitions / 2,471 instruments (T1 355, T2 3,501, T3 412); 72 admitted/deferred excluded; coverage 12,284/12,284 current, 0 stale, 0 missing | fail-closed half **not** evidenced by a non-zero population — see below |
| D — W2 without W1 | 142,818 windows; W2 fires 8,301; W2-only 6,968; weekend-explainable 6,783; **residue 185** | ✅ W2 is a 4th damage kind |
| E — naive-W1 over-refusal | **0** resolved breaks, 0 transitions on a resolved break date | exclusion still required by rule — see below |

### ⚠⚠ Two arms returned ZERO, and neither zero weakens its clause

**Arm C.** Every instrument with bars currently carries a coverage row at the current
rule-set version, so a loader that silently returned an empty tuple for an unchecked
instrument would, *today*, never be asked to. That does not make the fail-closed clause
optional: `sql/247:23-33` is the **source rule**, `price_masked_bars` already implements
it, and coverage completeness is a fact about this instant immediately after a quarantine
refresh — not an invariant. Residual 4's own run disclosed a 6,896-bar uncovered **tail**
(bars past an instrument's evaluated interval, which is a different and always-present
gap from an uncovered instrument). A clause justified by a count would have to be
un-justified the moment the count moved.

**Arm E.** No `price_series_break` row is resolved yet, so a naive W1 over-refuses
nothing today. The exclusion is still mandatory: `sql/246:4-10` states the
reclassification as a rule, `price_segments` already honours it, and the population can
only grow as adjustments are written. Sizing an error is not the same as licensing it.

### ⚠⚠ Arm D: the first run was wrong by 6×, and the fix exposed a property of W2

Draft 1 computed a rank window's bar count as `rank + 1`. `_SLICE_FILTER` ranks with
`row_number() OVER (… ORDER BY price_date DESC)`, so `rn = 1` is the **most recent** bar
and `[by_rank[r], win_end]` holds exactly **r** bars. Because `nominal = (bar_count − 1)
× calendar_days_per_bar`, the off-by-one *inflated* nominal and made W2 fire **less**:
1,394 firings became 8,301 once corrected.

The corrected run then showed that **6,923 of the 6,968 W2-only windows are the 2-bar
`market_data.load_day_changes` window**, and 6,783 of all 6,968 are explained by weekend
days alone. That is the rule meeting a window it was not written for:
`calendar_days_per_bar` is an **average** (7/5), so at `bar_count = 2` the gate is
`2 × 1.4 = 2.8` days — **below an ordinary Friday-to-Monday gap of 3**. `rule_w2`'s own
docstring is written around a 20-bar window, where averaging over many gaps is what the
constant is for.

The weekend share is measured **by construction**, not by choosing a bar-count floor: the
span is shortened by the Saturdays and Sundays it contains and `rule_w2` is re-asked. A
floor would be an invented constant, which is what this rule set refuses. The subtraction
is applied only to classes the rule set itself declares 5-day
(`calendar_days_per_bar != 1`) — on a 7-day class a Saturday is a session.

**The residue is 185 windows at 14, 16, 20, 31 and 50 bars** (plus the non-weekend part
of the 2-bar tranche). Non-zero, so the four-kind decomposition holds — but the honest
headline is that W2 is the *smallest* of the four kinds on this corpus, not the largest,
and any consumer adopting it at a 2-bar window needs the weekend qualifier or it will
refuse ~7,000 ordinary Monday day-changes.

## The decision

Four clauses, all four arms accepted. They **compose** — a windowed return consumer
needs all of 1–4; a level-aggregate consumer needs 1, 2 and 4.

1. **Bar fields → `price_masked_bars`.** Covers B1–B4 per field, plus the open by value.
   Does **not** cover volume. Per arm A it also makes every T1 site visible — T1 is a
   restatement of a bar verdict, not an independent surface.
2. **Joinability → `price_segments`.** Covers T3 per arm B, and already honours
   `sql/246`'s adjusted reclassification. The gap is adoption, not machinery.
3. **Window crossing → `rule_w1` behind a new fail-closed loader.** The only genuinely
   missing machinery. Shape, mirroring `price_segments.load_unresolved_breaks`:
   `load_quarantined_transitions(conn, instrument_ids) -> Mapping[int, tuple[date, ...]]`,
   returning the LATER date of each transition where `cardinality(rules) > 0`, at the
   current rule-set version, **excluding** dates carrying a resolved break, and
   **fail-closed** against `price_quarantine_coverage` — an instrument with no current
   coverage row must be distinguishable from one that is clean, exactly as
   `price_masked_bars` distinguishes them. The rule does not move; only the transport is
   new.
4. **Window horizon → `rule_w2`, with no new machinery.** The consumer holds the window
   bounds and the bar count; `params_for(asset_class)` is a pure call. Per arm D this is
   a distinct damage kind and the cheapest of the four to adopt — but it is also the
   smallest, and **a consumer adopting it at a 2-bar window must carry the weekend
   qualifier**, or it refuses ~7,000 ordinary Monday day-changes. The `bar_count` it
   passes must be the stored bar count, never a rank, never a calendar estimate.

⚠ Clauses 3 and 4 produce a **refusal or a flag, never a repair.** There is nothing to
repair: for T3 the bars are valid in their own regime (decision 10), and for T1/T2 the
cause is unknowable (containment, not classification).

## What this proposal does NOT ship

- **No consumer retrofit.** Residual 4 measured 21,872 exposed instrument/metric pairs on
  the ranked population across 73 `FROM price_daily` occurrences in 34 files. Changing
  what any of them computes is a behavioural change with data semantics, needs its own
  full-population A/B, and is filed as the follow-up rather than smuggled in here.
- **Not even the clause-3 loader.** A loader with no caller is the defect class this repo
  logged on 2026-09-15 (`docs/review-prevention-log.md`, the `report_progress` /
  `flush_to_job_run` entry): a producer nothing consumes is invisible to every test. The
  loader lands **in the same PR as its first consumer**.
- **No change to `price_quarantine.py`**, so `INPUT_RULE_SETS` identity does not rotate
  (#3031) and the backtest substrate is untouched.

## Security

No security surface. Read-path data quality; the script writes nothing and opens a
`READ ONLY` transaction.

Refs #3046. Refs #3031. Refs #2261. Refs #2354. Refs #2797.
