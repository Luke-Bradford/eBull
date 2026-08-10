# Strategy runner and manifest — the production step nobody owns

Spec for **#2394**. Parent: `strategy-catalogue-and-backtest-validity.md` (§5 criteria,
§9 *the harness itself*), `2026-08-07-bounded-backtester.md` (phase 5). Refs #2240.

## 0. The problem, measured

Phase 5 is nearly closed and **no operator surface can render anything**, because nothing
drives the strategy machinery on a cadence. Measured on dev, 2026-08-08:

```text
strategy_results_store   0 rows
strategy_signals         0 rows
strategy_outcomes        0 rows
```

```bash
grep -oE "'[a-z_]*strateg[a-z_]*'" app/jobs/runtime.py app/workers/scheduler.py   # no matches
```

`signal_ledger.py`, `outcome_ledger.py` and `result_ledger.py` are complete and tested.
Their only callers are `scripts/verify_2240_*.py` and `scripts/probe_*`.

**A second defect surfaced while scoping this, and it is the more expensive one.** There is
no enumeration of strategies anywhere — `app/services/strategies/__init__.py` declares no
`__all__` and no registry list. Every caller imports modules by name, so the strategy set is
whatever a given script happened to import:

```text
call sites naming a strategy module:  19
scripts importing s1: 8    s2: 2    s3: 10    s4: 2
```

That asymmetry is why every phase-5 run report carries S-1 and S-3 figures and not S-2 and
S-4 — **not a decision, an import list.** Adding S-5 or S-6 today means finding and editing
19 call sites, and forgetting one produces a silently smaller population rather than an
error. #2394 must not build a runner on top of that.

## 1. Source rules

- **Criterion 9, parent §5** — *"Report the count and share of bars/trades excluded per
  strategy … so exclusion is visible rather than assumed harmless."* A runner that skips a
  strategy because nobody imported it is an unmeasured exclusion, which is the same defect
  one layer up.
- **#2333, `strategy_registry.py:58`** — `INPUT_RULE_SETS` is deliberately a **registry-wide
  constant, not a per-strategy declaration**, with the rationale stated at length: *"a field
  they must remember to fill is the same omission with a nicer name."* The manifest below
  must inherit that stance — registration cannot be a thing an author remembers to do.
- **`STRATEGY_SET_ID = "strategy-registry-v1"`** already exists and is hashed into identity.
  Adding or removing a strategy changes the set, so the manifest is versioned by it.
- **Criterion 5 / §9** — the hold-out and the random-entry control both depend on signals
  being recorded independently of whether capital was deployed.

## 2. The manifest — enumeration that cannot be forgotten

One mapping in `strategy_registry.py`, beside `INPUT_RULE_SETS`:

```python
STRATEGY_MANIFEST: Mapping[str, StrategyEntry]   # strategy_id -> identity fn, signal fn,
                                                 # class, emitted legs, resolver shape
```

⚠⚠ **CORRECTION, made while implementing it: the manifest landed in
`app/services/strategy_manifest.py`, NOT in `strategy_registry.py`.** The shape above is
unchanged; the placement was wrong for a reason this section could not see, and the first
of the four is measurable:

1. `StrategyIdentity.version` hashes `_module_hash()` — **the bytes of
   `strategy_registry.py`**. A manifest living there would move *every stored strategy
   version every time a strategy is added*, so registering S-5 would invalidate S-1's
   entire track record for no reason of S-1's. `verify_2394_strategy_manifest.py
   --identity` asserts all five identity-bearing files are byte-identical to `origin/main`
   and prints the four versions, so the claim is checked rather than asserted.
2. The registry is imported **by** every strategy module, so it cannot import them back.
3. `ExitRegime` lives in `position_builder`; putting the manifest in the registry would
   couple the pure signal contract to the position layer.
4. ⚠ **It is not inside `app/services/strategies/` either**, which is where it was first
   written. `TestInputRuleSetsAreComplete` walks every module in that package and requires
   any imported `app.services` module carrying a `RULE_SET_VERSION` to be in
   `INPUT_RULE_SETS` — and the manifest imports `position_builder`, which has one. The
   guard failed on the first fast-tier run, correctly. Adding `position_builder` to
   `INPUT_RULE_SETS` would be reason 1 by another route; excluding the file would weaken a
   working guard. The manifest computes no verdict, so it belongs outside the package the
   guard scopes to.

⚠ **A second correction, to §1's last bullet** — *"Adding or removing a strategy changes
the set, so the manifest is versioned by it."* It does not, and must not.
`STRATEGY_SET_ID` is a literal in the registry and no field of `StrategyIdentity` is a
function of manifest membership. S-1's signals are unchanged by S-5 existing, so S-1's
version must not move when S-5 lands. The set id names the **contract** version, not the
membership.

⚠ **A third, to the field list** — the entry carries an `ExitRegimeFactory` rather than a
free-text "resolver shape". `position_builder.ExitRegime` already *is* the per-strategy
close-source contract, and §3's table already lives in its docstring as prose that every
caller hand-builds from. The manifest makes that table executable instead of adding a
second vocabulary for it.

**The completeness check is a test, not a convention**, mirroring the pattern already proven
by `tests/test_strategy_registry.py::TestInputRuleSetsAreComplete`: walk every module under
`app.services.strategies` (excluding known non-strategy modules), and fail if a module
exposing a `*_STRATEGY_ID` is absent from the manifest. So the coverage is **checked, not
promised** — the same words the registry uses about itself.

Consequences, all of them wanted:

- adding S-5/S-6/S-N is one manifest entry, not 19 edits;
- the runner iterates the manifest and therefore cannot silently under-cover;
- verify scripts iterate it too, which closes the S-2/S-4 asymmetry above as a side effect;
- a strategy present in the tree but absent from the manifest **fails CI** rather than
  quietly not running.

⚠ The manifest carries no per-strategy *tuning* — no thresholds, no windows, no cost
overrides. Anything richer on that axis reintroduces the "author remembers to fill it"
failure #2333 rejected.

⚠ **It does carry the OPERATIONAL CONTRACT, and an earlier draft of this spec wrongly
excluded it** (Codex ckpt-1). A runner cannot iterate the manifest without knowing, per
strategy: whether it is `per_series` or `cross_sectional` (S-2 needs a panel, and full
panel materialisation is explicitly unsafe); which `signal_kind`s it emits (S-1/S-3 emit
entry **and** exit, S-2 and S-4 entry only); and which outcome-resolution shape applies
(S-4 needs exit levels fixed at signal time, the others do not). These are not tuning —
they are how the caller invokes it at all, and omitting them forces the runner back into
per-strategy `if` branches, which is the 19-call-site problem in a new location.

## 2.1 ⚠ Re-runs must NOT be made idempotent — the ledger raises on purpose

An earlier draft listed "upsert-vs-skip" as an open implementation choice and said a re-run
"must be a no-op". **Both are wrong and would weaken a settled invariant.**
`signal_ledger.store_signals()` (`app/services/signal_ledger.py:279`) carries:

> ⚠⚠ **NO** `ON CONFLICT`, **deliberately.** A colliding key raises `UniqueViolation` and
> aborts the batch. […] `DO UPDATE` would let a re-run overwrite a recorded decision, which
> is the exact failure `strategy_version` is in the key to prevent.

So the runner must not reach for either arm. The correct shape is **not to re-enter a date
it has already written**: the job holds a per-`(strategy_id, strategy_version)` watermark of
the last completed `signal_bar_date`, and a re-run resumes after it. A collision that
survives that check is a **real disagreement about a recorded decision** and must keep
raising, loudly. `DO NOTHING` would hide corpus drift; a compare-every-field no-op is the
only safe equivalence test and is not worth building before a collision is ever observed.

## 3. Two jobs, deliberately separate

Splitting matters: the daily scan is cheap and must never be gated behind an expensive
full-corpus backtest.

### 3.1 `strategy_signal_scan` — daily, read-only with respect to money

Runs after the candle refresh. For every strategy in the manifest, over the validated
universe: evaluate today's bar, write `strategy_signals` with its `verdict`
(`fired` / `not_fired` / `not_evaluable` + reason), and resolve prior open signals into
`strategy_outcomes`.

- **Touches no broker path.** It records what *would* happen. Funding is a separate concern
  and a later phase.
- **Records signals whether or not capital is behind them.** Funded results are a biased
  sample: which signals get funded depends on cash being free, which is not independent of
  market conditions. Recording the unfunded ones removes **capital-allocation** bias, and
  §9's synthetic control and the surface's capture-rate both consume them.

  ⚠ **Two corrections to how this was first written** (Codex ckpt-1). First, *"funded trades
  over-sample calm periods because cash frees up after winners close"* is a **hypothesis,
  not a measurement** — it was stated as fact in the first draft and in the operator
  discussion. It is testable once signals exist (compare realised volatility on funded vs
  unfunded signal dates) and must not be cited as a finding until it is. Second, the
  unfunded set is **not "the unbiased control"** — it removes capital-allocation bias only,
  and still inherits survivor-only universe membership (§3.1.1), `not_evaluable` exclusions,
  quarantine masking and cross-sectional thinness. Call it what it is: the
  allocation-unbiased arm.
- ⚠ **The shadow track record cannot be backfilled.** Signals are a function of what was
  known on the day; re-deriving them later from stored bars would reintroduce exactly the
  look-ahead the epic spent phase 5 removing. **Every day this job is not running is a day
  of validation permanently lost**, independent of when any UI ships. That is the argument
  for landing 3.1 before 3.2 and before phase 6.

### 3.2 `strategy_backtest_run` — deliberate, not scheduled

Runs the harness for a strategy × namespace × arm and persists via `result_ledger`.
Manual-trigger only: it is expensive, and its `StrategyIdentity.version` must be stable for
a stored row to mean anything.

### 3.1.1 ⚠ BLOCKING — the daily population has no stable meaning yet

`load_validated_universe` resolves **current** membership. Its own docstring:

> this population moves with every `sync_universe` run

So two scans a month apart are over different populations with nothing recording the
difference, and today's membership is a look-ahead fact relative to a past signal date. A
shadow track record accumulated this way cannot answer "what was investable on the day",
which is the only question it exists to answer.

The source rule already exists and is not a new decision: the catalogue's own ordering table
carries **#2290, append-only universe-membership recording**, marked *"now — reinstated"*
and *"Not optional, and not 'downgraded' any more"*, precisely because `sync_universe`
overwrites the transition instead of recording it.

**This is a prerequisite, not an open question.** Either #2290's membership history is
readable at signal time, or the scan pins a `corpus_version` and declares the population
frozen — and says so on every row. Starting the daily scan before this is settled produces a
record whose population drifts silently, which is worse than no record.

### 3.1.2 Resolution, versions and the API shape — all underspecified in the first draft

Each of these was a Codex ckpt-1 finding and each needs settling before code:

- **Strategy functions emit full-series verdicts, not a single bar.** The runner either
  gains single-date entry points or recomputes history and filters — the latter risks
  accidental historical writes and unmeasured cost. Decide, and state which.
- **Only fired *entry* signals are outcome inputs.** Resolution goes through
  `outcome_ledger.select_pending_fills()`, not "prior open signals" as first written.
- **`strategy_outcomes` keys on `(rule_set_version, input_rule_set_version)`** — the daily
  scan must declare both, and the first draft named neither.
- **Immature or unorderable outcomes need a policy.** A signal whose max-hold has not elapsed
  is not an outcome; `window_truncated` / `missing_bar_data` / `quarantined_bar` /
  `series_break` / `unorderable_exit_levels` all have to resolve to something explicit rather
  than being skipped.
- **S-4 needs exit levels fixed at signal time** — a manifest of `(identity, signals)` alone
  cannot express that, which is part of why §2's operational contract exists.

## 4. Open questions — to settle before implementation, not during

1. **Does the scan resolve outcomes, or does a second job?** Written above as one job; the
   alternative is cleaner failure isolation at the cost of another moving part. Related:
   **transaction boundary** — if signals commit and resolution fails, what is the state, and
   does one strategy's failure stop the rest?
2. **Concurrency.** Two scheduler instances or a manual trigger can race; uniqueness only
   catches the duplicate *after* the work is done. Needs an advisory lock lane.
3. **Prerequisite watermark.** "After the candle refresh" must name a concrete condition —
   which date, all instruments or partial, and no concurrent refresh mutating bars mid-scan.
4. **Cost of a full daily scan is unmeasured**, and §3.1's "cheap" is an assertion until it
   is. 5,266 series × N strategies. ⚠ The rate limit is the wrong axis if the runner reads
   **stored** bars — and if it reaches for a provider instead, that breaks the causal
   stored-corpus shape phase 5 was built on. Settle the source first, then measure, per the
   corpus-cost rule in the prevention log.
5. **Observability, required by criterion 9.** Writing rows is not enough — the scan must
   emit counts and shares by strategy, reason and date, and a strategy-date with zero rows
   must be visible rather than silent.
6. **Result identity is wider than "strategy × namespace × arm"** as §3.2 first put it.
   `strategy_results_store` identity spans namespace, ambiguity arm, quarantine arm, sizing
   rule, cost model, corpus version, window, and the position/outcome/input rule versions. A
   stored row means nothing unless all of them are pinned.
7. **Version mixing on read.** Signals, outcomes and results coexist across versions. The
   operator surface must pin one set per view or it will double-count.

## 5. What this unblocks

Landing 3.1 starts the shadow track record immediately — the one clock that cannot be caught
up. Landing 3.2 puts real rows in `strategy_results_store`, which is all a **read-only**
operator surface needs. Neither depends on #2363 or #2364: those gate **trading**, not
**viewing**, so a performance page can be built and judged on real figures before either
lands.
