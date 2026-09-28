# #1822 — v1.5 ranking family ablation: spec (v8)

Refs #1822, #1815, #2437 (supervisor refill 2026-09-28 05:15Z, item 2),
`docs/proposals/ta/2026-09-25-evidence-ranking-and-instrument-report.md` §3. v2 answers Codex ckpt-1 v1 (61
findings), v3 answers round 2 (50),
v4 answers round 3, v5 settles "Prices", v6 settles slice 1b, v7 records slice 1b's build corrections, v8 fixes the
declaration's terms; see "Revision notes".

**Ask (supervisor):** ablate each v1.5 family against the matched control, on the hunt harness (#3385) and the PIT
bundles. Report each family's incremental contribution with its SE, and close no family on power alone. Weights
change only through the gated A/B in §3 of the evidence-ranking proposal.

**This is measurement.** It changes no weight, no rank and no recommendation. It declares no bar, runs no test
and makes no pass/fail call at any readout.

## Premise check (dev DB, 2026-09-28; full population unless stated)

Reproduce with `PYTHONPATH=. uv run python scripts/run_1822_ablation_readout.py --census` (slice 1 ships the census
mode). The figures below come from the one-off equivalent run while this spec was written.

| family (v1.5 weight, `scoring.family_weights`) | live input | earliest input in our corpus | historical route? |
| --- | --- | --- | --- |
| quality (.25) | operating/gross margin, FCF, net debt, debt | `financial_facts_raw`: min `filed_date` 2009-05-07, **0** instruments filed before 2009-01-01 | validation split only |
| value (.25) | thesis base/bear value when a thesis exists; else P/E, FCF yield, analyst target | theses from 2026-07-09, 482 instruments in total | thesis path: no. Fallback path: needs PIT analyst targets (unmeasured) |
| turnaround (.20) | margin/revenue trend, filing red flags, net debt | fundamentals as quality; red-flag PIT history unmeasured | validation only, if red flags are PIT |
| confidence (.15) | thesis confidence; 0.5 without a thesis | 2026-07-09 | no. Its score is 0.5 on **89.9%** of v1.5 rows |
| momentum (.10) | 1/3/6m returns + TA | prices | every split |
| sentiment (.05) | `news_events` | min `event_time` 2026-06-21 | no |

The live model itself, measured over all 133,368 v1.5 rows (34 runs, 2026-07-23 → 2026-09-27):
- **No family score is null or non-finite.**
- `raw_total` = Σ w_f · s_f with today's `family_weights('v1.5-balanced')` to within 7.5e-5 on every row. So the
  current weights are *compatible* with every stored run (the residual is at the stored scores' rounding scale).
  Provenance is the code: slice 1 cites `git log -L` on the `v1.5-balanced` entry of `_WEIGHT_MODES`, showing no
  change since the version was introduced (`bacef745`, 2026-07-23).
- `total_score` = `clip(raw_total − P + R)` **exactly**, with P and R the deductions and additions in
  `penalties_json` and `clip` = `scoring._clip` to [0, 1] (`scoring.py:2245`). 3,846 rows are clipped.

Consequences:

1. **The harness's discovery split (1990-01-02 → 2008-12-31) holds no fundamentals in our corpus.** The XBRL mandate
   dates (`sec-edgar.md` line 564; SEC Release 33-9002) bound this. The measured fact is the 0 above. A 2005
   voluntary XBRL programme existed, but none of it is in `financial_facts_raw`. A historical composite ablation
   can therefore run only through the validation door, one frozen look per declared spec.
2. **Value's thesis path, confidence and sentiment have no inputs before 2026-06** (value's fallback branch may be
   reconstructable; see Route H). A historical reconstruction
   is a different model from the live one. The nominal weight overstates the gap: confidence is a constant 0.5 on
   89.9% of rows, and value takes its fallback path for every name without a thesis. Actual branch usage is part
   of the census.
3. **Stored `scores` rows are what each v1.5 run ranked on, for all six families.** This is necessary but not
   sufficient for point-in-time use. "Timeline" below fixes when a run counts as known.

**Route choice.** Route F (forward, stored scores) measures the live model's every family from the scores each
run stored, with no input reconstruction.
It departs from the ask's "on the hunt harness", for these reasons:
- the harness's inputs are the research archive and, for fundamentals, a view that does not exist yet (#3386);
- the thesis and news inputs, feeding 0.45 of the weight, begin in 2026-06 (consequence 2);
- the live window's prices are the `price_daily` table the app shows every day, which the harness does not read.

Route F **is** registered, in the programme's preregistration register (#2829 / #2599,
`freeze_preregistration`), as one frozen construction. It is not registered in `hunt_trials`, whose `TrialSpec`
fields (a hunt signal module, a split, the archive universe identity) do not describe a stored-score book over
`price_daily`. Adapting the harness to a new price source and a new split would be a new `HUNT_HARNESS_MODEL_ID`,
built to host one descriptive measurement. Route F reuses the harness's pure evaluator, inference code and bar rules. The departure is
contestable on the PR. Route H (historical, through the harness's validation door) is specified with its
prerequisites and is not conditioned on route F's numbers (see "Route H").

## Source rule

| decision | rule | source |
| --- | --- | --- |
| overlapping holds → one return per session | h slots of 1/h | Jegadeesh & Titman 1993, *JF* 48(1) §I, as `hunt_evaluator` |
| cross-sectional dependence | calendar-time portfolio, one observation per session | Fama 1998 §4, as the harness |
| serial dependence | Newey–West HAC, Bartlett kernel, lag ⌊4(T/100)^{2/9}⌋: the NW 1994 plug-in rule-of-thumb *form*, not their automatic procedure | the harness's source-rule row; `r6_monthly_trial.newey_west_lag` / `hac_t` |
| minimum detectable effect | (z_{.975} + z_{.80}) · SE, a normal-approximation **planning** figure | Cohen 1988 ch. 1 convention; states resolution, not achieved power |
| freeze before look | frozen declaration row | #2829 / #2599, `result_ledger.freeze_preregistration` |
| rank key | `total_score` descending | `scoring.compute_rankings` (`scoring.py:2464`) |
| ablation of family f | **(construction)** below | none published for a heuristic composite |
| bar / transition validity | `price_quarantine` B1–B4, T1–T3, computed in-process | S7 verdict on #2247, `price_quarantine.py` docstring |
| cost band on a non-as-traded basis | `cost_price_basis` → `UNKNOWN_NOMINAL_PRICE_BAND` | #3238, `cost_model.py:460-559` |

**Ablation (construction).** Both keys are rebuilt symmetrically from the stored family scores, with the same
precision:
- `key_full = clip(Σ_g w_g · s_g − P + R)`;
- `key_{−f} = clip(Σ_{g≠f} w_g · s_g − P + R)`;
- P and R come from the row's own `penalties_json`, and the weights from `family_weights('v1.5-balanced')`.

Both arms select on the rebuilt keys: `arm_full` uses `key_full`, never the stored `total_score`, so Δ contains no
reconstruction asymmetry. The keys are computed in float64 from the stored numerics, summing families in the
fixed order quality, value, turnaround, momentum, sentiment, confidence. The estimand is therefore **the
reconstructed composite**, which matches `total_score` to within rounding. Slice 1 reports the number of
formations where `arm_full` differs from a `total_score` selection and the size of the difference. **There is no renormalisation.** With name-specific P − R, a
renormalised key *can* order names differently, so removing the term is the choice, and it is fixed here. This is a
**score-term intervention**, not removal of a family's information:
- f's inputs can still reach the score through penalties (low thesis confidence, high red flags, stale thesis),
  and those penalties **stay**;
- clipping adds threshold effects. Removing even a term that is constant across names can move names onto or off
  the 0/1 clip and change membership;
- confidence is 0.5 on 89.9% of rows, but the 10.1% that differ, together with penalties and clipping, can still
  make its removal consequential.

The readout states all three.

**What a Δ_f means.** It is a **conditional marginal** effect: how the top-quintile book changes when f's term leaves
and the other five stay. Families correlate, so the six Δ are not additive and must not be summed or read as shares.

## Route F — forward, stored scores

**Population.** Per v1.5 run (one distinct `scored_at` with `model_version = 'v1.5-balanced'`), every row. All
133,368 v1.5 rows have `rank IS NOT NULL` and all six scores finite, so the stored run *is* the ranked set. Any
future row failing either condition is excluded from **both** keys, and the exclusions are counted. Instruments the
run failed to score are outside the population. The readout says the result is conditional on successful scoring
and reports each run's scored count. Model versions never mix; v1.6 starts a new series.

**Timeline** (timestamp-based, NYSE sessions via `market_calendar`):
- a run is **known** at the `finished_at` of the successful `morning_candidate_review` job run whose
  [`started_at`, `finished_at`] covers its `scored_at`. `compute_rankings` has one caller
  (`app/workers/scheduler.py:5438`, inside that job). 33 of 34 v1.5 runs have this witness, the latest finishing
  60 s after `scored_at`. A run with no witness is excluded and counted. Scores carry no run id, so the witness is
  containment plus the single writer. **The commit boundary holds (v6).**
  - `compute_rankings` writes inside `with connect_job() as conn:` (`scheduler.py:5436-5437`), and `connect_job`
    returns a plain `psycopg.connect(...)` (`job_connection.py:75`).
  - On a clean exit, psycopg's `Connection.__exit__` calls `commit()` (installed `psycopg/connection.py:171-172`).
  - Both callers of `compute_morning_recommendations` run it inside `_tracked_job("morning_candidate_review")`: the
    scheduled job (`scheduler.py:5352-5353`) and the sync-orchestrator adapter (`adapters.py:403-404`). That tracker
    records `finished_at` only after its `yield` returns (`scheduler.py:3141-3149`).
  - So the rows commit before `finished_at`.
  - A `scored_at` covered by more than one successful run is ambiguous; it is excluded and counted.
- **entry session e** = the first session whose open is strictly after the known time; entry at the open of e.
  "t = e − 1" is an index label only: nothing assumes the run was available during t;
- when several runs share one entry session, the one with the latest known time is used (ties: the latest
  `scored_at`) and the others are counted as superseded. `--census` lists them. At readout date 2026-09-28 it
  found 7, six of them weekend runs sharing Monday's entry with a later run (v1–v6 said one date; that was wrong);
- exit at the close of x = e + h − 1, with h = 21 sessions. If x has no valid bar, the exit is at the last
  available close before x (the harness rule, never a later price).

Each run starts a cohort held for 21 sessions, with JT 1/h slots. This is **deliberately** not the #1822 body's
once-monthly rebalance: it uses every run rather than one in 21, and turnover is reported so the difference is
visible. Sessions with no run leave that slot idle (return 0 in every book), as in the harness.

**Books** (`hunt_evaluator.evaluate_books`, unchanged):
- control C(t) = every lane-eligible population name whose bar on t (the last session before entry) is valid
  under the control bar rule in "Prices". That bar is complete before the entry open;
- both keys are restricted to C(t) **before** selection, so A(t) ⊆ C(t);
- `arm_full` = `select_arm(total_score|C, sign=+1, fraction=0.2)`;
- `arm_{−f}` = `select_arm(key_{−f}|C, …, 0.2)` for each of the six families.

`select_arm` includes every tie at the cut. A book can therefore exceed ⌈0.2·N⌉ and differ from a rank cut; sizes
are reported. The books use equal weight, price only ("Prices" 3), the `real_stock_long_x1` tariff, and
`cost_model.cost_band_for` on the basis fixed in "Prices" 2. Termination follows `r6_exclusion_trial.PROGRAMME_POLICIES`;
`zero_recovery` is canonical and the others are reported beside it. A name whose feed stops is not terminated
unless the termination source says so (the harness rule).

**Termination source (v6).** The harness reads `series_termination.TerminationEvidence`: a Form 25 link, its
provision, and the Q suffix.
- **Linked:** a `sec_form25_register` row with `provision_class = 'equity_delisting'` whose `issuer_cik` is in the
  instrument's `instrument_cik_history` and whose `filed_date` is in [first formation, c_k]; the latest filed wins.
  (v7: a `debt_lifecycle` Form 25 delists a bond, not the stock.) The rule is uniform. It yields no link in the window, because the
  register is a harvest whose latest `filed_date` is 2024-12-31 (`select max(filed_date) from
  sec_form25_register`).
- **Q suffix:** `q_suffix =
vendor_symbol_has_bankruptcy_suffix(instruments.symbol)`, the helper (`research_corpus_ingest.py:362`) `universe_selection` already uses.
  The symbol is the one current at the readout. Its snapshot goes into the vintage identity, and the
  classification is retrospective, like the rest of route F.
- A series is **terminating** when its last stored bar is before c_k and `classify_termination` is not `UNKNOWN`.
  Its `terminal_ordinal` is that bar, and each `PROGRAMME_POLICIES` fraction applies to it. Recognising a
  termination from bars missing after the fact, and valuing it at the last bar, is the harness's ex-post valuation
  convention, not an executable exit.
  - A suspension that later resumes shows up as new bars, and the next readout reports it as a revision.
- Every other stopped series is **not terminated**. It carries V and exits at the last available close (the
  evaluator's rule). No claim is made that this close matches any takeout consideration.
- Measured: the last stored bar falls between 2026-07-20 and 2026-09-17 for 37 ranked names. None has a register
  Form 25, and 2 carry a Q suffix under the helper (NOTVQ, QVCAQ). The census recounts all of them.

⚠ **Lane eligibility.** Every population name is priced as a real-stock long. Non-stock or non-USD rows are counted
and excluded from the population. Stock means `instruments.instrument_type_id = 5` ("Stocks" in
`etoro_instrument_types`); the 30 ranked ETFs (type 6) are excluded. That removes them so from both keys **and** the control (slice 1 measures how many). Rows with a
non-finite `raw_total` or unparseable `penalties_json` are excluded the same way.

**Prices (v5).** This section settles the slice-1 go/no-go and its four residuals (#1822 comment, 2026-09-28). The
figures were measured on the window 2026-07-20 → 2026-09-25 over the ranked names; `--census` reprints them. Of the
3,924 names ever ranked by v1.5, 3,881 have `price_daily` bars in the window.

⚠ Eligibility and prices are reconstructed from the store as it stands at the readout, not as they were available
at each formation. This is part of the "retrospective" label.

**Reader.** The reader is `price_daily`. The readout **computes** the bar and transition verdicts itself, with
`price_quarantine.evaluate_series`, over each series as read. The read runs ascending from the last bar before the
first formation through c_k, and `as_of` is the readout date. Dates are unique by `price_daily`'s key. Every read,
prices and metadata alike, happens in one REPEATABLE READ transaction. Verdicts are computed before masking. Each series uses the
asset class that `price_quarantine_store` uses (its scope query, `price_quarantine_store.py:49`).
- Masking then follows `research_price_structure_store.load_masked_series`: the close on `return_usable`, high and
  low on `range_usable`, and the open on its value.
- Verdicts are never read from `price_bar_quarantine`, so a stored verdict can never be stale against the prices.
  The function is pure and its rule-set version is declared. The vintage identity covers the rows read, each
  series' asset class and `as_of`, so it detects a change; it does not replay one.
- The research archive cannot serve the full window: no archive series for a ranked name reaches 2026-09-20.

**Basis.** The canonical **treats** `price_daily` returns as split-safe. This is a methodological choice: the
harness makes the same one with its own split correction. It rests on two measurements.
- **The provider back-adjusts re-denominations.** #2840 found that 0 of 142 filing-registered re-denominations left
  a cliff (`scripts/probe_2840_confirmed_split_adjustment.py`). That evidence covers registered events only. An event
  newer than an issuer's latest periodic report is untested.
- **The store follows the provider through the #2066 heal.** `market_data.detect_adjustment_event` flags an overlap
  ratio ≥ 1.2 and triggers a full re-fetch. Three failures are not detected here: a sub-1.2 re-base, a failed heal,
  and a heal with no overlap.

The `t3_excluded` sensitivity covers a failure that leaves a step T3 flags. **A step T3 does not flag is an
unmitigated residual**: one under 5×, one already explained by T1 or T2, or one admitted back by a turnover spike.
The series is also **treated as price-only**: in the 7 instruments measured, eToro candles carried no dividend
adjustment (`etoro-api.md`, "eToro prices vs the public tape"; #2240).

1. **Jumps.** The go/no-go comment's 1.8 / 0.55 screen is not a rule. The repo's rule is `price_quarantine` (S7
   verdict on #2247).
   - T1 (an unusable endpoint) and T2 (a hole) stay with the evaluator. A session without a valid close carries V,
     and the next valid close marks from the last reference (the harness convention).
   - T3 is a magnitude trigger with no turnover spike. Magnitude is a trigger, not a verdict.
   - In the window, over the stored verdicts, there are 10 T3 transitions. Every one is `unclassifiable`, because
     both bars have NULL volume, so T3 there is magnitude alone. There are also 7 T2 holes, 1 trigger admitted back
     (`spike`) and 1 provisional trigger. `--census` recounts all of these from the computed verdicts, including T1
     and the overlaps between rules.
   - **Canonical:** a T3 transition is a return.
   - **`t3_excluded` sensitivity.** A T3 verdict is dated by its later bar. Every name with a computed T3 verdict
     dated in [the first formation's session t, c_k] leaves the population at every formation, and so leaves both
     keys and the control.
     - It uses information from after formation, so it is labelled a **look-ahead sensitivity**.
     - It is recomputed in full at each readout, and does not nest.
     - It measures how far Δ_f depends on those names, not the effect of admitting T3 itself.
   - The readout also lists, per book, the entered positions still held at the close before a T3 transition and
     at its later bar, with each ratio.
   - The #2840 classification route is not used. Its register is built from periodic-report restatements, so it
     cannot reach the recent events at issue.
2. **Cost bands.** `cost_model.cost_price_basis` (#3238) is total and fail-closed: only a stored basis that
   positively says as-traded may select a band. `price_daily` is back-adjusted.
   - **Canonical:** `cost_band_for(p, price_basis="split_adjusted")`, which is `UNKNOWN_NOMINAL_PRICE_BAND` (the
     highest measured p75 band), in every book. p is the entry fill's stored price. A position never entered is
     never charged (the evaluator). The band is selected once per
     position and reused at exit, as the harness's `HalfSpread` does.
   - `cost_model.py:460` calls this band an **adverse sensitivity, not the historical quote**, and that is the label
     it carries here. A flat band does not bound Δ in either direction.
   - **Sensitivity:** `as_traded` on the stored level. Its error is directional per event: a later reverse split
     lifts the stored level into a cheaper band, and a later forward split lowers it into a dearer one.
   - **Gross is dropped.** `hunt_evaluator.position_path` refuses a zero half-spread, and changing the evaluator
     changes `HUNT_HARNESS_MODEL_ID`. Net is reported at both bases. Entries per formation per book are reported
     descriptively; they do not decompose Δ.
3. **Dividends.** None of our stores covers the window's ex-dates:
   - `dividend_events` holds 4 ex-dates in 3 of 3,881 names. That is insufficient coverage, and no statement is
     made about its point-in-time provenance. It holds 0 ex-dates for AAPL, MSFT, JPM, HD, KO, XOM and GME over
     2026-06-01 → 2026-09-30;
   - the research archive has 0 dividend rows since 2026-01-01;
   - `dividend_history` is keyed by fiscal period and carries no ex-date.

   Route F is therefore **price-only**, and the with-dividends cell is not computed. The omission changes each
   book's path, since the evaluator compounds credited dividends and applies terminal haircuts. The sign of its
   effect on Δ_f is not known.

   The readout gives one **descriptive diagnostic**, the yield tilt. Per family and formation, it takes the mean
   `instrument_dividend_summary.ttm_yield_pct` over each book's entered positions, then the difference between the
   two books.
   - NULL or non-finite yields are counted and left out.
   - A formation where either book has no yield is skipped and counted. With none left, the diagnostic is refused.
   - The yield is read at readout time, not point-in-time, and the snapshot is stored in the vintage sidecar.
   - It neither signs nor bounds the omission's effect on Δ.
4. **The mismatched name is CTNT (1052266).** On all 90 compared bars, each of open, high, low and close live
   equals the stored value × **150.000**. The mismatch was observed on 2026-09-28; the store's last bar is
   2026-09-25. Neither the provider's re-base date nor the economic effective date is established here.
   - A uniform scale leaves every return unchanged, and so canonical cost and membership are unchanged too, because
     the verdicts are computed from ratios. Only the `as_traded` sensitivity reads the level.
   - CTNT is not excluded. The #2066 heal is meant to rewrite the series on the first refresh that overlaps the new basis. This is the
mechanism; the rewrite has not been verified. The
     vintage identity records the rewrite, and the readout reports it as a revision.

**Control bar rule.** This supersedes "the harness's bar rule" under Books. It is the harness's eligibility rule
(`2026-09-26-3385-hunt-harness.md`, "Eligibility"), applied **per formation** to the masked bar on t, with one change:
the volume clause passes when volume is NULL **or** finite and > 0.
- Stored volume is NULL on every window bar of **995** of 3,881 names (48,202 bars), and on some bars of 19 more.
  No window bar stores volume 0. A live fetch also returns `volume = None` for PLAG and YYAI (2 checked).
- A NULL volume records the absence of a figure, not evidence that nothing traded. It is also not evidence of
  liquidity; canonical cost treats every name alike.
- H > L is kept. 908 bars fail it, 855 of them in series with NULL volume.
- The readout reports the exclusion funnel per formation: ranked, lane, bars present, then each bar-rule reason.
  The harness counts it the same way.

Sensitivity: the harness rule verbatim, applied per formation. At a formation where a name's bar on t has NULL
volume, that name leaves the population, both keys and the control.

**Cells.** There are four: canonical, `t3_excluded`, `as_traded` cost, and the verbatim bar rule. Each sensitivity
changes one assumption against the canonical, and they are never combined.
- An empty control at a formation is an idle slot.
- A grid with no admitted cohort is refused as `empty_grid`. All three are frozen in the declaration and reported
beside the canonical for all six families. None is a decision.

**Grid (nested by construction).** Readout k has cutoff c_k, frozen in its vintage record. c_k is the latest
session **more than** `price_quarantine.PROVISIONAL_WINDOW_DAYS` calendar days before the readout date. (v7: v6 said
"at least", but `evaluate_bars` marks `price_date >= as_of − 5 days` provisional, so a session exactly 5 days back
is provisional and would break the next bullet.)
- It is calendar-only, so it never moves backward between readouts.
- No bar at or before it is provisional under the verdicts the readout computes.
- A bar loaded late shows up as a revision in a later readout (below). The cutoff does not wait for it.

Its reporting grid runs from the first
entry session to G_k = c_k − (h − 1) sessions:
- only cohorts with entry e ≤ G_k are admitted, so each exits by c_k;
- every admitted cohort is evaluated over its whole path through its exit, but only daily returns through G_k are
  reported;
- `ruined` is judged on the reported grid only. An evaluator refusal (`StatRefused`) anywhere on an admitted
  cohort's path is fail-closed, because the evaluator cannot return part of a path:
  - in `arm_full` or the control, it refuses all six families in that cell;
  - in `arm_{−f}`, it refuses f only.

  Cells never affect each other.

A later readout appends sessions. An earlier session's return changes only if an **input** changed (a price,
dividend, score or termination correction). Each readout's vintage record stores the input identity: a sha256
over the ordered rows of every series it read, the quarantine rule-set version, and the exclusion list with its
reasons. A later readout that finds a differing identity for an already
reported session reports the revision and its cause, never silently. The record is a JSON sidecar next to the
readout doc, and slice 1's script writes it along with the declaration. Idle slots at start-up and in sparse-run
gaps return 0. The readout reports the deployed share per session: active slots / h, and the entered share
of each active cohort. The annualised figures are read against that exposure.

**Held names.** These follow the harness evaluator's rules unchanged (`hunt_evaluator.py` docstring):
- a session without a bar carries V forward;
- an exit with no valid open uses the last available close, never a later price;
- the terminal haircut applies only when the termination source says so;
- membership is fixed at formation, so later missing prices never remove a name from a book.

Costs are charged per cohort, with no netting across overlapping cohorts (the harness convention). Between-slot
rebalancing is uncosted in both books (the harness residual), so net Δ is not fully costed. The readout says so.

**Statistics per family f,** on the grid:
- Δ_f(d) = r_{arm_full}(d) − r_{arm_{−f}}(d), paired on the same sessions (the control cancels);
- the mean of Δ_f and its HAC SE, computed **on the paired daily series**; both are ×252 annualised (an
  arithmetic annualised spread, not a compounded wealth gap); t (unannualised); T; the NW lag used; and the count
  of sessions where Δ ≠ 0. All reported **net**, in each of the four cells of "Prices" (gross is dropped there);
- MDE_f = (z_{.975} + z_{.80}) · SE_annual, reported beside Δ and Δ / SE. It is a planning figure, showing what
  this window could resolve, and it classifies nothing;
- **the lag (v6 correction).** The harness's lag is its *construction* L = max(2h, ⌊4(T/100)^{2/9}⌋)
  (`2026-09-26-3385-hunt-harness.md`, "Lag (construction)"; `hunt_inference.hunt_lag`). The 2h floor covers the
  overlap of h cohorts. v1–v5 cited only its initial-lag row, which is too small for 21-session overlapping holds.
  - Canonical: `hac_estimate(Δ_f, L)` with L = `hunt_lag(T, h)`. Sensitivity: `hac_estimate(Δ_f, 2L)`.
  - L is kept in the Bartlett weights even when L ≥ T, as the code does, and autocovariances at shifts ≥ T are 0.
    The readout flags L ≥ T and 2L ≥ T separately;
- two of the harness's `cell_statistics` floors are **reported, not applied**, because the readout is descriptive:
  - T / L against `SHORT_SAMPLE_LAGS`;
  - `sparse_arm_count` of the `arm_full` book's entered formations on the admitted grid, against
    `MIN_ARM_FORMATIONS`.

  The census gives readout 1's values. A window of about two months cannot come near either floor;
- `hac_estimate` refusals (constant series, too few observations, invalid variance) are reported as refusals, and no t or
  MDE is fabricated. A book ruined **on the reported grid** (`evaluate_books`' own flag) stops the family's
  statistics: the readout reports "ruined" and computes no SE, t or MDE for that family;
- descriptive: `arm_full − control`, both book sizes and never-entered positions per formation, held
  position-sessions marked without a valid close (carried V) per book, the symmetric membership difference
  |arm_full △ arm_{−f}|, and realised turnover per book.

Six families, two cost bases: every t is **marginal**. No multiplicity correction is applied because no inference
is drawn; the readout says this in its header.

**Declaration (v8).** Slice 1b-ii freezes one `freeze_preregistration` row before any forward return is read.
`sql/333`'s row has fixed columns, so the construction lives in a **terms sidecar** that the row pins by digest.

- **Terms sidecar.** It is canonical JSON: `sort_keys`, `separators=(",", ":")`, `ensure_ascii`, and one trailing
  newline. It is stored per digest at `docs/proposals/ta/1822-route-f/terms-<sha256>.json`, and it is never
  overwritten. It has two sections.
  - **Semantic terms**, regenerated from the code:
    - the SQL constants route F executes;
    - H, FRACTION, LAG, the tie rule (`select_arm`: every tie at the cut), FAMILY_ORDER and the weights by value;
    - the lane (`Stocks`, USD), the four cells and the three `PROGRAMME_POLICIES` (canonical `zero_recovery`);
    - the rule identities: `COST_MODEL_ID`, `HUNT_HARNESS_MODEL_ID`, `STRUCTURAL_REFUSAL_POLICY_VERSION`, and the
      quarantine, calendar and termination `RULE_SET_VERSION`s. The last three embed their module's code hash;
    - `ranking_ablation.CONSTRUCTION_REVISION`, an integer bumped with any change to selection, aggregation or
      statistics code. It is a declared revision, not auto-detected; the commit provenance below is the backstop;
    - the evaluation inventory, as explicit lists: the 8 books, the 6 evaluations and the 2 populations. `searches`
      is their product, and the readout iterates these lists from the sidecar.
  - **Frozen facts**, computed once at PR time from the dev DB and never recomputed. They are generated only
    together with new semantic terms, and a sidecar whose semantic terms equal an existing one is refused, so the
    semantic terms always select one sidecar:
    - the forward-shadow floor values, with their derivation text;
    - the grid facts they come from, and the date those facts were read.
- **Code provenance: recorded, not proven.** `--freeze` and each readout record `git rev-parse HEAD`, and they refuse
  a dirty tree (`git status --porcelain`, where ignored files do not count).
  - `--freeze` prints one JSON record: the commit, the `declaration_id`, `frozen_at`, and an outcome of `frozen` or
    `already_frozen_identical`, which tells a retry from the original. The readout-1 PR commits it as
    `1822-route-f/freeze-<declaration_id>.json`.
  - This *identifies* the code. It does not prove which bytes ran: a stale `.pyc`, an edit racing the run, or an
    environment change can defeat it. It is an accident control, like `unattended_guard`, and the readout says so.
  - The spec's "Grid" identity already takes the same stance ("detects a change; it does not replay one").
  - Semantic changes are what the gate refuses. Any rule-set change also moves its `RULE_SET_VERSION`.
- **Identity.** It uses the full digest.
  - `strategy_id = "ranking-ablation-1822-route-f"`;
  - `strategy_version = "v1.5-balanced+<sha256>"`, at most 78 characters against `sql/333`'s limit of 200;
  - `contract_version = "1822-route-f-terms-<sha256>"`;
  - `trial_id = "ranking-ablation-1822-route-f-<sha256>"`.

  Changed semantic terms or frozen facts give a new sidecar, identity, root and register entry. The earlier ones
  stay. It is never a second root on one identity (`sql/337`), and never a re-pointed entry (`freeze_preregistration`'s
  #2829 message).
- **Row.**
  - `structural_refusal_policy_version = STRUCTURAL_REFUSAL_POLICY_VERSION`;
  - `declared_by = "scripts/run_1822_ablation_readout.py --freeze (#1822)"`;
  - `prereg_purpose = "falsification_only"`: of `PreregPurpose`'s two values, the one that cannot promote;
  - `declared_universe_basis = "survivor_only"`. This is the conservative label (#2288: *"an unlabelled result is
    treated as survivor_only"*). Membership is the rows each run wrote, but the lane filter and the Q-suffix symbol
    are read at readout time, so survivorship-free is not claimed. A `falsification_only` declaration over
    survivor-only stamps is coherent (`declaration_refusals`' docstring).
  - `declared_carry_unmodelled = False` and `declared_fx_unmodelled = False`, as #2901 declares for the same lane:
    *"Long x1 US equities in a USD lane: no overnight financing exists, and USD in / held / out has no conversion
    event"*. A bespoke contract owns its stamps.
  - `expected_structural_refusals = structural_promotion_refusals(...)` =
    `("universe_basis_not_survivorship_free",)`.
  - **Floor.** It is fixed by construction, following the #2901 / #2840 precedent (*"the floor equals the evidence
    supply"*), because no power calculation exists for a descriptive readout.
    - `min_independent_decision_dates` = the number of distinct entry sessions on the grid (at the fact date) that
      have a mapped witnessed run. It comes from the calendar and `job_runs`; no price is read.
    - `min_calendar_weeks` = ⌈(G_k − first entry session) days / 7⌉.
    - If either is < 1, generation refuses `floor_not_positive` and nothing is frozen.
    - The derivation says the floor is the evidence supply **at the fact date**, not readout 1's grid (which may have
      more runs), and that it gates nothing.
- **Trial register (#2829).** There is one entry per sidecar: `DeclaredTrial(trial_id, …, declared_for=(strategy_id,
  strategy_version), exactness=EXACT, searches=96)`. Its evidence is `declaration_backed_evidence(declaration_path=…,
  declaration_sha256=…, pinned_specs=96)`, the register's format for a group pinned by one frozen declaration.
  - **96 = 8 books × 6 evaluations × 2 populations.**
    - The books are `arm_full`, the six `arm_{−f}` and the control.
    - The evaluations are the 4 cells, plus the 2 non-canonical termination policies re-evaluating the canonical
      cell.
    - The populations are the pooled series and the prospective subseries.
  - Every evaluated book and population is charged. The fragility precedents (S-E, S-H, ARM B) exempted their cells
    because a joint pass bar left no cell selectable, and route F has no pass bar. #3238 charges each population.
    #3238's control was exempt because it reproduced a stored number, and this control is new.
  - Readouts under identical terms add none. The source rule is the DSR's own: N is the number of configurations
    tried (Bailey & López de Prado 2014, *J. Portfolio Mgmt* 40(5)), and re-evaluating one configuration over more
    sessions is not a new one. #2831's charged extra read was a look whose configuration could not be established.
    An identical-terms readout's configuration is established by the gate. If the register ever adopts a
    per-look charge, the start log gives the count.
  - Looks are **listed**, best-effort. Before its first return is read, a readout appends a start record to
    `var/1822-route-f/looks.jsonl`, holding a uuid, the identity, the commit, the cutoff and the start time, and
    fsyncs it.
    - The readout holds an exclusive `flock` on that file for its whole run, so readouts serialise.
    - Each committed vintage record lists every start record whose uuid appears in no earlier vintage, aborted
      ones included.
    - Claim level: this lists looks made from this checkout only. The loop runs from one worktree. A final
      aborted run appears in the next vintage, or in none if there is no next one, and the readout says so.
  - It bumps `TRIAL_REGISTER_VERSION`, with the strand measurement the register's header prescribes. It is not a
    `hunt-` entry, so it counts in M_inh (`TRIAL_REGISTER.inherited_floor()`, printed before and after in the PR).
    Raising M is conservative.
  - Route F reports no DSR and no M.
- **Tests (slice 1b-ii).**
  - Exactly one committed sidecar matches the regenerated semantic terms, which makes the selection rule for
    `--freeze` and `--readout` unambiguous.
  - Every sidecar under `1822-route-f/` has exactly one register entry, with the matching `trial_id` and
    `declared_for`, `searches == 96`, `EXACT`, and evidence equal to `declaration_backed_evidence(…, 96)`. No sidecar
    named by an entry is missing.
  - For the **selected** sidecar only, `build_declaration(sidecar)` passes `declaration_refusals`. Historical
    sidecars are checked for integrity (entry mapping) but not coherence, since a policy bump correctly retires them.
  - Each register entry's `searches` equals the product of its sidecar's inventory lists.
  - Gate before returns: with the gate stubbed to refuse, `--readout` never calls the price reader.
  - The access boundary (below).
- **Freeze.**
  1. From `main` with a clean tree, `--freeze --dry-run` prints the full `digest_payload`.
  2. `--freeze` selects the sidecar, rebuilds the declaration from it, and freezes.
  3. On `UniqueViolation` it rolls back, reloads, and returns `already_frozen_identical` when the stored digest
     equals the rebuilt one; anything else is refused.
  - The prospective boundary is the row's `frozen_at`: a run is prospective when its `known_at > frozen_at`, both
    timestamptz.
- **Readout gate.** `--readout` refuses unless every one of these holds:
  - the tree is clean;
  - the regenerated semantic terms select exactly one sidecar;
  - `load_preregistration` returns the row;
  - the stored `declaration_sha256` equals `build_declaration(sidecar).sha256`, which is equality with the full
    intended payload, not just integrity;
  - `declaration_refusals` is empty.

  **Input** revisions after the freeze (a score, bar or termination correction) keep the declaration and are
  reported through the "Grid" input identity. **Implementation** corrections are semantic-term changes, and so a new
  declaration.
- **Sealed-outcome boundary.** The withheld side is only reachable through `backtest_run` with an
  `evidence_window_id` and `holdout_requested` (the gate test's docstring). `price_daily` is the live app table and
  holds no sealed window. Two tests pin the boundary:
  - Route F's modules, and the computation modules they import from `app.services`, import at top level only (an
    AST check: no `import` inside a function body). A subprocess then imports the script and asserts that
    `app.services.backtest_run` is absent from `sys.modules`. Residual: a lazy import in a module outside that set.
  - Every query route F issues goes through one `_execute` helper, which raises unless the SQL is a registered
    constant. The runtime path therefore cannot run an unaudited query. The ledger's freeze/load SQL is the
    exception: it runs inside `result_ledger`, and its table is in the allowlist.
  - Every relation named in a FROM or JOIN of the registered constants is in an allowlist. The constants are the
    reader's, the script's, `price_quarantine_store._SCOPE_SQL`, and the ledger's declaration SQL. The allowlist is
    `scores`, `job_runs`, `instruments`, `etoro_instrument_types`, `exchanges`, `price_daily`,
    `instrument_cik_history`, the Form 25 view, `instrument_dividend_summary`, `dividend_events`, the premise tables
    and `strategy_preregistration_declarations`. It excludes `strategy_results_store` and
    `strategy_holdout_accesses`.

⚠ **Retrospective, then prospective.** The window 2026-07-23 → cutoff already exists. Nobody has computed this
ablation over it, but the prices are not unseen. Readout 1 is labelled **retrospective**. Every later readout
extends the same frozen construction, and each reports the prospective part (formations after the freeze) beside
the pooled series.

**Repeated readouts.** Readouts come on the first weekday of each month, with c_k as defined under "Grid", each
recomputed in full on the frozen construction. With the nested grid they append, and input corrections are surfaced as above. None is a significance
decision, so no sequential correction applies. A rerun at the same cutoff over the same input identity must reproduce byte-identical statistics;
an implementation correction is a new declaration (an input revision is reported, per "Declaration").

**Prospective subseries.** It uses only cohorts formed from runs known after the declaration's freeze time, on
their own grid. Retrospective cohorts are never sliced into it.

**Implementation drift within v1.5.** `scoring.py` changed under the same model label during the window. Examples:
`ac21f90d` (2026-08-12, thesis quarantine) and `2476b4b9`/`73c61231` (2026-08-08, IAR share-count and freshness
bounds). The readout lists every commit touching `scoring.py` inside the window, from
`git log --since=2026-07-23 -- app/services/scoring.py`, as a dated marker. It does not split on them; that would be
a search.

**Scope of any claim.** The readout describes this window, this broker's scored universe and this construction.
It says nothing about persistence, regimes or risk-adjusted alpha. Per-regime reporting starts when the window spans more than one label of
`market_regime.classify_regimes` (the label `strategy_regime_evidence` uses). Labels are computed causally, from
bars ≤ t, with the classifier's code sha in the declaration.

## Route H — historical, the harness's validation door (not started here)

Measures the reconstructable subset over validation: momentum, quality, turnaround if its red flags are PIT, and
value on its fallback path if analyst targets are PIT.
- **Decision (made now, not after route F's readout):** H is built when its prerequisite 1 lands, independent of
  any route F number.
- H's spec freezes its families, transformations and constants without reference to route F's numbers, and it
  records which F readouts existed when it froze.

Its prerequisites:

1. a PIT fundamentals view in the harness (#3386), which is a new `HUNT_HARNESS_MODEL_ID`;
2. a companyfacts → v1.5-input construction under #3360's readers. Availability is the acceptance timestamp, never
   the mandate date (SEC Release 33-9002 phase-in and grace periods), with amendments, units, YTD-to-quarter and
   period alignment declared. Price-based constants are fitted on discovery. Fundamentals constructions carry no
   fitted constant, because discovery has no fundamentals to fit them on;
3. measured PIT availability of red flags, analyst targets, and the risk-deduction and Calmar-reward inputs. A
   family whose defining input is not PIT is **excluded** from H, not modified. A penalty or reward whose input
   is not PIT is dropped from every key, and this is declared;
4. momentum's own warm-up (6m returns and TA) and PIT corporate-action handling at split boundaries, measured
   before claiming "every split";
5. its own spec and ckpt-1, carrying the harness obligations: embargo and purge, `survivor_bias_direction`,
   pre/post-2013-06-21 side-by-side, and a coverage census over the phase-in years from actual filings, not a
   calendar breakpoint.

## Slices

1. **Build.** The reader fixed in "Prices", then `app/services/ranking_ablation.py`: pure key
   construction, domain restriction, timeline mapping, and the Δ series via `evaluate_books`, `hac_t` and the MDE.
   Also `scripts/run_1822_ablation_readout.py` (read-only; `--census` prints the premise table; writes only the
   declaration) and the frozen declaration. Tests: adding w_f·s_f back to the **pre-clip** ablated total and then clipping reproduces `key_full`;
   Δ ≡ 0 → refusal, not t; `key_{−f}` with name-specific P − R differs from a renormalised key (pins the "no
   renormalisation" choice); clipped keys and ties at the cut; the timeline around an in-session run, a weekend run,
   a holiday and two runs per entry session; exclusion applied to both keys; `ruined` propagation.
   "Prices" tests:
   - verdicts computed in-process equal `evaluate_series` on the same bars, and no stored verdict is read;
   - `t3_excluded` removes a T3 name at every formation, from both keys and the control; T1/T2/admitted/provisional
     do not trigger it;
   - canonical cost is exactly `UNKNOWN_NOMINAL_PRICE_BAND`, selected at entry and reused at exit;
   - a uniform ×k rescale of one series leaves canonical returns (to 1e-12), costs and membership unchanged. In a
     fixture whose rescale crosses a band threshold it moves only the `as_traded` band (the CTNT case);
   - volume NULL passes; 0, negative, NaN and ∞ fail. The verbatim arm drops a NULL-volume name at that formation
     only;
   - c_k is calendar-only and monotone in the readout date. At the `PROVISIONAL_WINDOW_DAYS` boundary,
     `evaluate_series` marks no bar ≤ c_k provisional;
   - a `StatRefused` after G_k in `arm_{−f}` refuses f only, and in `arm_full` it refuses all six; `ruined` after
     G_k refuses nothing;
   - `t3_excluded` dates a T3 by its later bar and leaves an empty formation idle.
2. **Readout 1 (retrospective):** `docs/proposals/ta/2026-09-28-1822-ablation-readout-1.md`.
3. **Route H**, per its own spec.

## Not doing
Changing or proposing a weight. Mixing model versions. Summing Δ across families. Any 6/12-month horizon, which
the 34-run window cannot feed and which exceeds the harness's 63-session cap for route H.

## Revision notes
- **v2 (Codex ckpt-1 v1, 61 findings).**
  - Fixed: the renormalisation claim was false (#11), so the ablation is now a construction, with clip, P and R
    reproduced (#12, #13).
  - Replaced sample claims with full-population measures: 133,368 rows, reconciliation, branch usage, and 0
    pre-2009 filers (#2–#8).
  - Timeline made timestamp-based, with a commit margin and collisions handled (#20–#23).
  - Added: ties, domain and exclusion rules (#15–#18); termination, lane and reader go/no-go (#32–#36);
    paired HAC, annualisation, refusals and `ruined` (#39–#46); MDE and marginal labelling (#47, #48);
    retrospective vs prospective, vintages and scope (#49–#53).
  - Route departure: justified by the holdout-split reservation (#1).
  - Route H: the build decision is taken now and is not outcome-conditioned (#60); its obligations are listed
    (#54–#59).
  - "Only horizon" dropped (#27).
- **v3 (round 2, 50 findings).**
  - The holdout-split justification is withdrawn, because it was unproved (#1, #2). The departure now rests on
    input availability, no specification search, and data the harness does not read (#3).
  - Keys are rebuilt symmetrically, and the add-back test is taken before the clip (#8–#11).
  - Weight provenance: reconciliation on every row (#12). Implementation drift is listed, not split on (#13).
  - The commit witness replaces the +1 h margin (#15, #16).
  - Readouts nest: the grid ends h − 1 sessions before the cutoff, so there are no immature cohorts and no
    revisions (#29–#34).
  - Added: held-name and cost-accounting conventions (#28, #29, v1 #37, #38), deployed share (#37), the doubled-lag
    SE sensitivity (#35, #36), and "unresolved" replacing "insignificant" (#38).
  - Ruin stops a family (#40). The prospective subseries is defined (#41), and so are the readout date and rerun
    rule (#43). The regime classifier is declared (#44).
  - Route H: the spec is frozen without F's numbers (#4). Risk and reward inputs, the exclude-don't-modify rule,
    price-constant fitting and momentum warm-up are added (#45–#48).
  - v1 items mapped here: #9/#10 → weight reconciliation; #14 → "conditional marginal"; #19 → population caveat;
    #24–#26 → daily overlapping cohorts, idle slots and deployed share; #28 → census; #29–#31 → nested grid;
    #37/#38 → held-name and cost conventions; #61 → tests.
  - Rejected: #17 (causality of stored inputs). The readout measures the model as it ran; an input that was wrong
    at scoring time is part of what the live model did. #49: the census reports actual branch usage from the
    thesis join, not from 0.5 values.
- **v4 (round 3).**
  - `arm_full` selects on `key_full`, and the estimand is the reconstructed composite (#3, #4). Float64 and the
    summation order are fixed (#6).
  - Weight provenance comes from the code history, not the reconciliation (#5).
  - Witness: the single caller is cited, and slice 1 must cite the commit boundary or stop (#7, #8). The
    1-hour-margin leftover is removed (#10). Tie rule added (#11). t is an index label only (#12).
  - Grid: admission is e ≤ G_k, paths are evaluated through the exit, and reporting runs through G_k (#13–#15).
    Revisions come only from input corrections, detected through an input-identity sidecar (#16, #17). Ruin is
    judged on the grid (#18).
  - Close-exit fallback defined (#19). The lane filter covers the control (#20). Control validity uses the bar on
    t, before entry (#21). Row validity is extended (#22). One cutoff rule for any reader (#23, #24).
  - Route F is registered in the #2829 register, and the reason `hunt_trials` does not fit is stated (#25, #26).
  - "No history at all" is corrected (#27). The overlapping-cohort choice is declared deliberate (#28). The MDE
    classifies nothing (#29).
- **v8 (the declaration's concrete terms, for slice 1b-ii; ckpt-1, three rounds: 24, 12 and 18 findings).**
  - A terms sidecar (canonical JSON, one path per digest) carries the construction, and the full digest is the
    identity and the trial id.
  - Round 3 showed that proving which bytes executed is unbounded, so code identity became recorded provenance
    with the claim level stated (r3 #1–#4, #17). The semantic terms are what the gate compares.
  - The floor is the evidence supply at the fact date, frozen in the sidecar, and refuses at < 1 (r2 #5, #6; r3 #5,
    #6).
  - The universe basis is `survivor_only`, which is conservative and coherent under `falsification_only` (r2 #3).
  - searches = 96: every book × evaluation × population, because no joint pass bar protects the cells (r1 #6–#10;
    r3 #13). Readouts under identical terms add none, per the DSR's N (r2 #10; r3 #12, which corrected the #2831
    reading).
  - Looks are listed through a git-ignored, fsynced start log (r3 #14, #15). The tests tie each sidecar to its entry
    and to the searches count (r3 #7–#10). The readout compares the full declaration payload (r3 #11). Input and
    implementation corrections are separated (r3 #16). The access boundary is an import closure plus a SQL
    allowlist (r3 #17, #18).
  - The remaining round-1 items are covered: the row fields, the stamps citations, rollback before the reload, and
    the dry-run.
  - **Round 4** (12 findings): frozen facts never change alone, so selection is unique (#1). There is a declared
    `CONSTRUCTION_REVISION` (#2). The inventory is an explicit list, and searches is its product (#3). The per-look
    note (#4). Keyword-only evidence (#5). A freeze record (#6). A best-effort look log under `flock`, deduplicated
    by uuid (#7, #8). Coherence is tested on the selected sidecar only (#9). A gate-before-returns test (#10). The
    AST scope is widened, with the residual named (#11). A runtime `_execute` allowlist (#12).
- **v7 (slice 1b build, no ckpt-1: three corrections found by building; the first two are pinned by tests).** c_k is strictly
  outside the provisional window; a Form 25 link is an equity delisting; the superseded-run figure is the census's.
- **v6 (slice 1b settlements, measured 2026-09-28; ckpt-1, 10 findings).**
  - Witness: the commit boundary is cited, with both tracked callers, and an ambiguous cover is excluded (#9, #10).
  - Termination: a uniform Form 25 link rule over `instrument_cik_history`, which yields none because the register
    ends 2024-12-31 (#5). The Q suffix uses the current symbol, as a retrospective snapshot (#6). A series
    terminates only if it stops before c_k with a non-`UNKNOWN` class. This is the harness's ex-post convention, and
    a resumption becomes a revision (#7, #8).
  - Lane: stock is type 5; the 30 ETFs are excluded.
  - Lag: corrected to the harness construction, max(2h, NW), with 2L as the sensitivity, via `hac_estimate`, L kept
    at L ≥ T (#1, #3). The short-sample and `sparse_arm` floors are reported, not applied (#2, #4).
- **v5 (the go/no-go and its four residuals, measured 2026-09-28; ckpt-1 rounds 4, 5 and 6).**
  - Reader: `price_daily`. Bar and transition verdicts are **computed in-process** with `evaluate_series`, not read
    from stored tables. This removes the stale-verdict, coverage, rule-version and interior-completeness class
    (r4 #5, #8, #9, #44; r5 #4, #5, #23, #26, #27).
  - Basis: split-safety is stated as a methodological choice, and its residuals are named (a sub-1.2 re-base, a
    failed heal, no overlap, a sub-5× unadjusted step) (r4 #1–#7; r5 #1–#3).
  - Jumps: T1 and T2 stay with the evaluator. For T3, the canonical treats it as a return. The `contained`
    segment arm (r4 v5 draft) is **withdrawn**: it removed the whole remaining path, used d's close for an open exit
    on d, clashed with termination and did not match the `Segment` model (r5 #6, #7, #18–#22). It is replaced by
    `t3_excluded`, a labelled look-ahead exclusion. The #2840 route is not used (r4 #11–#21).
  - Cost: the maximum band, labelled adverse and not a bound. Selected at entry and reused. Gross dropped; entry
    counts are descriptive, not a decomposition (r4 #22–#27; r5 #28).
  - Dividends: coverage is "insufficient", with provenance not claimed. The yield bound becomes a tilt diagnostic,
    with defined skips and refusal and a stored snapshot (r4 #28–#35; r5 #8–#11, #29).
  - CTNT: all four OHLC fields ×150.000 on 90 of 90 bars. No date is claimed. Membership is unchanged because the
    verdicts come from ratios (r4 #36–#38; r5 #12, #17, #30, #31).
  - Volume: finite > 0 or NULL, applied per formation in both arms, with a per-formation funnel. 3,924 versus
    3,881 is reconciled (r4 #39–#46; r5 #13).
  - Grid: c_k is calendar-only and monotone; late loads become revisions (r4 #49; r5 #15, #24, #25). An evaluator
    refusal anywhere refuses the family; `ruined` is judged on the grid (r4 #50; r5 #16). Deployed share counts
    entered positions (r4 #48; r5 #14). Sensitivities are one at a time (r4 #51). Tests rewritten (r4 #52).
  - Round 6 (24 findings): reader bounds, ordering and one snapshot (#8–#10); identity detects, does not replay
    (#7, #11); the T3 residual widened to "not flagged" (#6); `t3_excluded` dating, nesting and meaning (#12–#14);
    empty cases and the cell list (#15, #24); a carried-mark diagnostic (#5, #16); tests pinned (#17, #20, #21);
    reruns keyed by cutoff and identity (#18); refusal propagation (#19); heal wording (#22); never-entered cost
    (#23). Not changed: #1 (as r5 #2 below); #4, since per-family composition is the quantity the ablation itself
    measures.
  - Not changed: r4 #4 and r5 #2. Treating T3 as a return is the harness's choice, and it is labelled as a
    choice. `t3_excluded` shows what rests on it. r4 #10 is covered by the retrospective label. r5 #9: the
    dividend claim is stated as measured on 7 instruments; no population claim is made.
