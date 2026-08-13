# The daily signal scan — §3.1, settled against the live corpus

Spec for **#2394 §3.1**. Parent: `2026-08-08-strategy-runner-and-manifest.md`
(§2's manifest, §3.1's job, §3.1.1's blocking prerequisite, §3.1.2's five
underspecified items, §4's seven open questions). Above that:
`strategy-catalogue-and-backtest-validity.md` §3.5 and criteria 8/9/11. Refs #2240.

§4 of the parent says its open questions are *"to settle before implementation,
not during"*. This document settles them. Every figure below was computed by
`scripts/verify_2394_signal_scan_cost.py` on the dev database on 2026-08-08;
nothing is quoted from a prior session.

    PYTHONPATH=. uv run python scripts/verify_2394_signal_scan_cost.py --all

## 0. The population, measured

`--population`:

```text
price_daily          bars 6723014  instruments 12185  last bar 2026-08-08
research_price_daily bars 25920971  series 7709  last bar 2026-07-08
research series carrying an instrument_id: 5269/7709
validated universe 6735; loadable through the masked loader 6547
strategy_signals 0 rows; strategy_outcomes 0 rows
instrument_universe_membership 0 rows
last-bar-date histogram over the loadable universe (top 6):
  2026-08-07: 5783
  2023-02-28: 21
  2026-08-06: 9
  2026-08-08: 7
  2025-06-26: 7
  2026-02-27: 6
bars per instrument: min 1  median 613  max 1046  total 3627106
research bars per series for contrast: 25920971/7709 = 3,362
instruments with fewer than 200 bars (sma_200 cannot evaluate): 2624 (40.1%)
```

⚠ The histogram and the depth both go through the **coverage join**, not raw
`price_daily`. An earlier version joined the raw table and called the result "the
loadable universe" — which counts bars the fail-closed loader never returns, so
it would have described a population the scan does not see.

## 1. Source rules

Nothing below is reasoned from first principles where a rule already exists in
the tree. The five that bind:

- **`signal_ledger.store_signals`' INSERT carries no `ON CONFLICT`,
  deliberately**: *"A colliding key raises `UniqueViolation` and aborts the batch
  … `DO UPDATE` would let a re-run overwrite a recorded decision."* ⚠ The
  behaviour is the **writer's**, not `sql/255`'s — the migration supplies the
  `UNIQUE (strategy_id, strategy_version, instrument_id, signal_bar_date,
  signal_kind)` key that the absent clause would have needed. Either way every
  row this job writes is **terminal**, which is the constraint §2 is derived
  from, not a preference.
- **`strategy_registry.NotEvaluableReason` includes `no_fill_bar`**, flagged as
  an addition to criterion 8's seven: *"the last bar of any series has no `t+1`,
  so a signal there can never be filled … it is the edge of the series."*
- **`outcome_ledger.select_pending_fills`** is a `LEFT JOIN … WHERE o.outcome_id
  IS NULL` at one `(rule_set_version, input_rule_set_version)` pin. A signal
  that has *any* outcome row at that pin — including an `unresolved` one — is
  never returned again.
- **`position_builder.build_positions`** raises on `outcomes` supplied for a
  regime that is not `level_based`: *"they would be read by nothing, and a close
  source nobody applies is a close source nobody notices is missing."*
- **Prevention log, "Source-absence gates over a date-keyed window must tolerate
  holidays"**: *"the per-day watermark only advances on success, so the date was
  re-requested every run and wedged phase 1 indefinitely."* §3's watermark is
  shaped to make that failure impossible rather than unlikely.

## 2. The scan runs in ARREARS — one bar behind the frontier

**This is the decision the job's whole shape follows from, and it is measured.**

A signal on the final bar of a series can never be filled, so
`strategy_registry.evaluate` stamps it `not_evaluable` / `no_fill_bar`. `sql/255`
has no `ON CONFLICT`. Therefore a scan that writes *today's* bar writes a refusal
that can never be corrected when tomorrow's bar arrives — the corrected row
collides on `(strategy_id, strategy_version, instrument_id, signal_bar_date,
signal_kind)` and raises.

`--arrears` computes each series' verdict at its **second-to-last** bar twice —
once with the next bar present (what a one-day-arrears scan sees) and once with
that bar truncated away (what a same-day scan sees):

```text
per strategy, at each series' SECOND-TO-LAST bar:
  s1-time-series-momentum:            compared 13070, differ 13070, of which fired-with-next 3658
  s3-mean-reversion-in-trend:         compared 13070, differ 13070, of which fired-with-next 2323
  s4-volatility-compression-breakout: compared  6535, differ  6535, of which fired-with-next  204
TOTAL compared 32675, differ 32675, fired-and-lost 6185
```

⚠ **`differ = 100%` is not the finding and must not be quoted as one.** It is
true by construction: without bar `D+1`, bar `D` *is* the last bar, so its
verdict is `no_fill_bar` unconditionally. The load-bearing number is the
**6,185 rows whose verdict with `D+1` present is `fired`** — real decisions a
same-day scan records permanently as unevaluable. The transition census makes
the shape explicit:

```text
s1: fired/- -> not_evaluable/no_fill_bar: 3658
s3: fired/- -> not_evaluable/no_fill_bar: 2323
s4: fired/- -> not_evaluable/no_fill_bar:  204
```

**Rule.** The scan's write date is `frontier - 1 bar` on each instrument's own
calendar, where the frontier is §3's completeness condition. A scan may never
write a `signal_bar_date` equal to the last bar it can see.

## 3. The frontier, and the watermark

§4 question 3: *"'after the candle refresh' must name a concrete condition."*

⚠ **It cannot be `max(price_date)`.** The histogram in §0 is why: on the day
this was measured, 7 instruments carried a bar at `2026-08-08` and 5,783 did
not. A scan keyed on the maximum evaluates a date most of the universe is
missing and manufactures thousands of refusals out of a refresh still in flight —
and by §1 those refusals are terminal.

**Frontier = the modal last bar date across the loadable universe, ties broken on
the later date.** Measured today: `2026-08-07`, held by 5,783 of 6,547 (88.3%),
printed by the script rather than assumed.

**Completeness floor, fixed by construction: the modal share must be ≥ 2/3 of the
loadable universe, or the scan refuses the day and reports.** There is no
published rule for this and the honest form is to say so and freeze the constant
(the `.claude/CLAUDE.md` rule for exactly this case). ⚠ It is a **job** parameter,
not a strategy parameter — it selects *when* to evaluate, never *what* the
verdict is — so it is deliberately outside `StrategyIdentity` and criterion 11
does not reach it. Refusing is safe in a way that scanning is not: a skipped day
can be picked up tomorrow, a written row cannot be withdrawn.

### 3.1 The write date is per instrument, and the watermark is not

⚠ **This was wrong in the first draft and the measurement caught it.** The write
date is the bar before the frontier *on each instrument's own calendar*, and that
is a distribution, not a constant — `BarSeries` allows calendar gaps and never
interpolates them:

```text
write-date distribution across eligible series:
  2026-08-06=5779, 2026-07-31=1, 2026-08-05=1, 2026-08-04=1, 2026-07-28=1
```

So a watermark holding one `signal_bar_date` cannot describe what was written. It
holds a **calendar frontier** instead:

> **Watermark** = per `(strategy_id, strategy_version)`, the last frontier date
> the scan completed. On each run the scan writes, for every eligible instrument,
> each of its own bars strictly **after** the watermark and strictly **before**
> that instrument's last bar. Then it sets the watermark to the current frontier.

That shape settles three things at once:

- **Re-runs.** A second run in the same day has `watermark == frontier` and does
  nothing, without `ON CONFLICT` — which parent §2.1 forbids in both arms
  (`DO NOTHING` hides corpus drift, `DO UPDATE` overwrites a recorded decision).
- **The three stragglers above.** An instrument that missed sessions gets all its
  unwritten bars in the window, not just one — so a gap does not silently drop a
  day of its record.
- **Holidays.** ⚠ The first draft said *"a market holiday advances the
  watermark"*, which contradicted itself: a holiday has no bar, so there is no
  `signal_bar_date` to advance to. Against a calendar frontier there is no
  contradiction — on a holiday the frontier has not moved, `watermark ==
  frontier`, the scan writes nothing and is not stuck. The prevention-log entry
  in §1 (*"the per-day watermark only advances on success, so the date was
  re-requested every run and wedged phase 1 indefinitely"*) is avoided by
  construction rather than by tolerance: the watermark tracks a frontier the
  corpus supplies, never a date the scan hoped to find.

⚠ **"Wrote nothing" and "failed" must remain distinguishable**, and the watermark
alone cannot carry both. A run that completes with zero rows advances it; a run
that raises does not.

## 4. It reads the LIVE corpus, through a masked loader that does not exist yet

§4 question 4 says *"settle the source first, then measure"*. Settled:

Every phase-5 figure on this epic was measured on `research_price_daily` — a
frozen archive keyed on `series_id`. §0 shows it last moved on `2026-07-08`, a
month stale, and only 5,269 of its 7,709 series carry an `instrument_id` at all.
`strategy_signals.instrument_id` is a foreign key to `instruments`. **A daily
scan cannot run on the research corpus.** It reads `price_daily`, which reached
`2026-08-08` on the day of measurement.

⚠ **There is no masked loader for `price_daily` in `app/`, and the scan cannot
use raw bars.** `indicator_series`' own header: *"Quarantine and adjustment basis
are the CALLER's gate. These functions have no database access and compute over
whatever bars they are handed."* The pieces exist and the gap is small:

| piece | research corpus | live corpus |
| --- | --- | --- |
| verdict table | `research_bar_quarantine` | `price_bar_quarantine` — populated, 11,132 instruments |
| coverage table | `research_price_quarantine_coverage` | `price_quarantine_coverage` — covers all 6,547 loadable |
| masking rule | `_apply_arm` | corpus-agnostic already; names `series_id` only in its return type |
| loader | `load_masked_series` | **missing** |

⚠ `price_quarantine_store.usable_bar_filter_sql` is **not** the missing piece and
must not be pressed into service. It is a row **filter**; the strategies need a
per-field **mask**. `load_masked_series` says why: *"Masking the whole bar on
either verdict would discard good data and shift every N-bar window."* A filter
silently shortens every warm-up window.

**Rule.** Implementation adds an instrument-keyed masked loader mirroring
`research_price_structure_store._LOAD_SQL` field for field — fail-closed at the
instrument level (an instrument with no coverage row at the current
`rule_set_version` loads as zero bars), per-field masking on `range_usable` /
`return_usable`, and the #2354 value-keyed open mask. `scripts/verify_2394_
signal_scan_cost.py::_load_live_masked` is the measured prototype, not the
shipping code.

## 5. It recomputes from the series start — no trailing window

§3.1.2 item 1: *"The runner either gains single-date entry points or recomputes
history and filters — decide, and state which."*

`rsi_series` and `atr_series` are Wilder-smoothed from the series start —
recursive with unbounded memory, which `indicator_series.atr_window_series`
documents and measures. So a trailing-K-bar recompute is a **different function**,
not a cheaper one, and `StrategyIdentity` records no window: two runs at
different K are indistinguishable once stored.

`--truncation` compares the frontier verdict under full history against 250- and
750-bar trailing windows:

```text
window 250 bars — series deeper than it: 3290
  s1 0/3290   s3 0/3290   s4 0/3290   frontier verdicts differ (0.00%)
window 750 bars — series deeper than it: 2033
  s1 0/2033   s3 0/2033   s4 0/2033   frontier verdicts differ (0.00%)
```

⚠ **That is a negative result and it does not license truncation.** Zero
disagreement here is a property of this corpus's depth (median 613 bars) against
a boolean threshold comparison, not a property of the functions. It is reported
because the honest form of "we did not adopt the optimisation" is the
measurement that would have justified it.

**The reason not to truncate is §6's cost, not this arm.** A full-history
recompute of the entire universe takes 40.2 s. There is no budget pressure to
trade an unrecorded parameter for.

**Rule.** The scan recomputes each instrument's full stored history and takes the
verdicts at the write-date index. No single-date entry point is added, and no
window is introduced.

## 6. Cost, and the daily write volume

§4 question 4: *"§3.1's 'cheap' is an assertion until it is measured."* `--cost`,
over 6,547 series and 3,627,106 bars:

| stage | seconds |
| --- | --- |
| load, masked, all 6,547 instruments | 23.3 |
| `s1-time-series-momentum` | 13.8 |
| `s3-mean-reversion-in-trend` | 16.4 |
| `s4-volatility-compression-breakout` | 26.1 |
| `s2-cross-sectional-momentum` (staged) | 0.5 |
| **strategy evaluation total** | **56.8** |

Under a minute and a half end to end, at 255,420 bar-evaluations/s. §3.1's
"cheap" holds, measured. ⚠ The per-strategy seconds include `resolve_fills` and
move run to run with machine load — S-1 and S-3 swapped order between two runs of
this arm — so the figure that carries is the order of magnitude, which is
minutes-not-hours.

⚠ **The census runs through `signal_ledger.resolve_fills`, not off the raw
strategy verdicts.** Codex caught the first version taking strategy output
directly, which is not what the ledger stores: a `fired` verdict whose `t+1` open
is absent or non-positive is stored as `not_evaluable` / `unusable_fill_price`
(#2354). On this population the two agree — no `unusable_fill_price` row appears
below — but that is now a measurement rather than an assumption.

⚠ It is taken at the **write date**, not the frontier. An earlier version
censused the frontier and reported 5,783 `no_fill_bar` rows per leg —
structurally guaranteed by §2, and a measurement of the refusal rather than of
the scan.

```text
write date = frontier - 1 bar; series eligible to be written 5783

s1 entry  fired 1722   not_fired 1623   insufficient_warmup 2435   quarantined_bar 3
s1 exit   fired 1384   not_fired 1961   insufficient_warmup 2435   quarantined_bar 3
s3 entry  fired   11   not_fired 3322   insufficient_warmup 2410   quarantined_bar 40
s3 exit   fired 1946   not_fired 1387   insufficient_warmup 2410   quarantined_bar 40
s4 entry  fired  108   not_fired 5382   insufficient_warmup  100   quarantined_bar 193
```

Each leg sums to 5,783 — every eligible instrument gets a verdict, which is
criterion 9's *"exclusion is visible rather than assumed harmless"* satisfied by
construction. ⚠ The script **asserts** that sum per leg and fails the arm on a
short one, so a silently smaller population is a non-zero exit code rather than a
number a reader has to add up.

**Daily write volume**, computed by the script rather than by hand:

```text
TOTAL rows per scan day 34698 (per-series legs 28915 + s2 5783)
at 252 trading days that is 8,743,896 rows/year at one strategy_version
insufficient_warmup rows 9790 of 28915 per-series rows (33.9%)
```

Sizing, not a problem — but it is the number a retention or partitioning decision
would be made from, and it did not exist before this run.

⚠ **A third of every scan day is `insufficient_warmup`**, and for S-1 alone it is
2,435 of 5,783 — matching §0's 2,624 instruments (40.1%) holding fewer than 200
bars. The live corpus is shallow: median 613 bars per instrument against
25,920,971 / 7,709 = 3,362 per research series, both printed. So a large minority
of the tradable universe is structurally unevaluable for any `sma_200`-gated
strategy. That is a fact about `price_daily`'s depth, not a defect in the scan,
and it must appear in the operator surface rather than be absorbed into
`not_fired`.

### 6.1 S-2 needs the panel's union calendar, and a slice of it

⚠ **`rebalance_dates` must be given the panel's union calendar, never a single
member's dates.** It fires on *"the first bar whose calendar month differs from
the previous bar's — i.e. act at the start of the new month"*, which is causal
for the reason its own docstring gives: the last session of a month is not
knowable at that session. ⚠ The first draft of this spec said "the last trading
day of each month" — **wrong, and Codex caught it**; the rule is the first bar of
the new month, and reading it the other way would have specced a look-ahead into
the runner.

Because it reads only the dates it is handed, a per-member calendar makes each
name rebalance on *its own* first-bar-of-month, so names that resumed on
different days rank against nobody. An earlier version of `--cost` did exactly
that and reported `thin_cross_section (1 < 10)` — a measurement of the bug, not
of S-2. On the union calendar the panel has **73 rebalance dates over 6.6 years
(11.0/year)**, and the write date measured is not one of them: **0
decision-bar participants**.

That is correct behaviour and has two consequences the runner must encode:

1. **On a non-rebalance day S-2 still writes a verdict for every member.**
   `CrossSectionalMember`: *"Everything else is an ordinary `not_fired` … It is a
   verdict, not an absence."* The script counts that directly — `rows at the
   write date 5783` — so S-2's contribution is the whole eligible population on
   every scan day, with 11.0 days a year carrying any `fired`.
2. **The panel is a slice, not the corpus.** `evaluate_cross_sectional` holds
   every member's whole score series in memory at once. A daily scan needs one
   date's cross-section, so it stages each member, keeps that date's score, and
   calls `s2_select` once — 0.6 s, measured. Full panel materialisation is
   explicitly unsafe at corpus scale (parent §2) and is not needed here.

⚠ `s2_select` is handed the **write date**, not the frontier. Today's
implementation ignores `when` — but the manifest's `CrossSectionalSelect`
contract exists so a date-aware selector needs no signature change, and passing
the wrong date would be silent until one arrives.

## 7. It writes SIGNALS ONLY — outcome resolution is NOT this job

§3.1 as drafted says the scan also *"resolves prior open signals into
`strategy_outcomes`"*. **It cannot, for any strategy in the catalogue today**,
and the reason is structural rather than a matter of sequencing.

- `strategy_outcomes` is fed only by `outcome_resolver.resolve_outcome`, which
  requires `ExitLevels` — a take-profit, a stop and a max hold.
- Only **S-4** declares `level_based=True`, so S-1, S-2 and S-3 have nothing to
  resolve: their closes are position-layer constructs (`signal_pair`,
  `max_hold_bars`, `rebalance_dates`). ⚠ That is enforced at the **reader**, not
  in the schema: `build_positions` raises on outcomes supplied for a non-level
  regime, while `outcome_ledger.store_outcomes` only checks that the parent is a
  fired entry. So the invariant is "nothing constructs them and the consumer
  refuses them", not "the table forbids them" — which is why this section is a
  rule for the runner rather than an observation about the DDL.
- **Nothing in `app/` constructs an `ExitLevels`.** `grep -rn 'ExitLevels('
  app scripts tests` returns one verify script and two test modules. The
  manifest already states this: *"`level_based=True` says the levels EXIST;
  nothing in `app/` computes them yet."*

So the scan's outcome half has no work to do for three strategies and no inputs
for the fourth.

⚠ **And there is a second, sharper reason not to bolt it on later without
thought.** `select_pending_fills` returns fills with **no outcome row at the
pin**. An immature window — a signal whose max-hold has not elapsed — resolves to
`unresolved` / `window_truncated`. Store that today and the signal leaves the
pending set **permanently**: tomorrow's mature resolution collides on
`(signal_id, rule_set_version, input_rule_set_version)`, and there is no
`ON CONFLICT` there either. §3.1.2's *"immature windows need a policy"* has
exactly one safe answer: **do not write an outcome for a window that has not
closed.** `unresolved` is for a window that closed and could not be judged, not
for one still open.

**Rule.** §3.1 ships as `strategy_signal_scan`, signals only. Outcome resolution
is a separate job on a separate ticket, blocked on an S-4 exit-level provider,
and it must carry the immature-window rule above as an acceptance criterion.

This also settles §4 question 1 (*"does the scan resolve outcomes, or a second
job?"*): a second job, because the first one cannot.

## 8. Transaction boundary, failure isolation, concurrency

**One transaction per `(strategy_id, frontier)`** — the unit the watermark names,
which per §3.1 is a frontier and not a write date, because the write date varies
by instrument. A strategy's run is all-or-nothing, and one strategy's failure does
not stop the others: the manifest is iterated, each entry's batch commits
independently, and the watermark advances per `(strategy_id, strategy_version)`.
A half-written batch under a no-`ON CONFLICT` key is unrecoverable without a
delete, so the batch boundary and the watermark unit have to be the same thing.

**Concurrency (§4 question 2).** ⚠ Uniqueness catches a duplicate only *after*
the work is done, and under a raising key that means an aborted batch rather than
a no-op. `app/jobs/locks.py` already has the lane mechanism —
`pg_try_advisory_lock(hashtext('job_source:{source}')::int)`, session-scoped,
released in a `finally`. Reuse it with a scan-specific lane; do not invent a
second locking vocabulary.

⚠ **Take the lock in a connection that is not inside the writing transaction.**
Prevention log: *"`pg_advisory_xact_lock` acquired in a savepoint is absorbed by
the parent and held until the TOP-LEVEL transaction commits"* — with a
per-strategy commit boundary that would hold every strategy's lock until the last
one finished.

## 9. Observability (§4 question 5)

Writing rows is not enough. The job reports, per run:

- the frontier date, the write date, and the watermark it resumed from;
- per `(strategy_id, signal_kind, verdict, not_evaluable_reason)`, the row count
  — the §6 census shape, emitted by the job rather than by a script;
- instruments in the validated universe that produced **no row at all**, with
  which of the two reasons: no bars through the masked loader (188 today), or a
  series whose last bar is not the frontier (764 today).

⚠ **A strategy-date with zero rows must be visible, and no index can do that.**
The first draft pointed at `idx_strategy_signals_reason`; Codex is right that it
cannot help — it is keyed `(strategy_id, strategy_version, not_evaluable_reason)`
with no `signal_bar_date`, and more fundamentally **an absent row indexes to
nothing**. Zero-coverage is only detectable against an *expected* count, so the
job must emit the count it wrote against the eligible population it computed, per
strategy, per run — and a mismatch is a failure, not a log line. A scan that
silently covered 4,000 instruments instead of 5,783 is the manifest defect
(#2394 §2) reappearing at the population layer. `verify_2394_signal_scan_cost.py
--cost` already asserts exactly this equality and exits non-zero on a short leg;
the job inherits the check rather than the index.

## 10. The population is survivor-only, and says so

§3.1.1 named #2290 as blocking. It merged (`f234a1e3`) and
`instrument_universe_membership` is live — but §0 measures it at **0 rows**, by
design: `sync_universe` calls `reconcile_universe_membership`
(`app/services/universe.py:226`), so the table accrues from the next
`nightly_universe_sync` and not before, and #2290 carries a no-backfill clause
because the historical transitions were destroyed as they happened.

So the membership record cannot answer "was X tradable on date D" for any past
date, and will not for some time. §3.1.1 offers two arms and the second is the
one available: **the scan pins `universe = 'survivor_only'` on every row**, which
is the label #2288 already defined and which `load_validated_universe`'s own
docstring insists on: *"Do not quietly drop the filter to make a number look
better — that would widen the population without changing the label, which is
worse than the bias."*

⚠ This is not a workaround that expires quietly. The switch to a point-in-time
population is a **new `universe` value**, hence a new `strategy_version`, hence a
new track record beside the old one — not an in-place correction. Stated here so
that the day membership history is deep enough to use, nobody treats it as a bug
fix.

## 11. What this job does not do

- **It touches no broker path.** It records what would happen. Funding is a later
  phase, and §3.1's own note stands: the unfunded set is the
  **allocation-unbiased arm**, not "the unbiased control" — it still inherits
  survivor-only membership, `not_evaluable` exclusions and quarantine masking.
- **It does not backfill.** Signals are a function of what was known on the day.
  §3.1: *"Every day this job is not running is a day of validation permanently
  lost."* Deriving yesterday's signals from today's stored bars would reintroduce
  the look-ahead phase 5 spent itself removing.
- **It does not run `strategy_backtest_run`** (§3.2), and must never be gated
  behind it.

## 12. Residual risk this spec does not close

⚠ **A corrected historical bar cannot be reflected in an already-written
signal.** `LedgerRow` records `input_rule_set_versions` (the indicator and
quarantine rule sets) but **no corpus version**, and the writer has no
`ON CONFLICT`. So if `daily_candle_refresh`'s trailing-correction window revises
a bar the scan has already evaluated, the stored verdict is wrong and is
unrewritable — the corrected row collides on the uniqueness key.

This is inherent to the no-`ON CONFLICT` design and is not created by this job;
the job is simply the first thing that will encounter it, daily and at scale. It
needs its own ticket, and the shape of the fix (a corpus version in the key, or
an explicit supersede-and-record path) is a decision about the ledger, not about
the scan. Flagged here rather than absorbed.

## 13. Acceptance

1. `strategy_signals` accrues rows daily at `universe = 'survivor_only'`, with
   `signal_bar_date` never equal to any instrument's last bar.
2. A re-run on the same frontier is a **no-op via the watermark** — not via
   `ON CONFLICT`, which does not exist and must not be added.
3. A market holiday leaves the frontier unmoved, writes zero rows, and does not
   wedge: the next run with a moved frontier writes the whole gap.
4. The per-run census sums to the eligible-instrument count for every
   `(strategy_id, signal_kind)` — a shortfall fails the run rather than
   under-reporting it.
5. The scan refuses a day whose modal frontier share is below the §3 floor.
6. Every figure in §§0, 2, 3.1, 5, 6 is **printed** by
   `scripts/verify_2394_signal_scan_cost.py --all`, exit 0 — none is arithmetic
   done in this document.
