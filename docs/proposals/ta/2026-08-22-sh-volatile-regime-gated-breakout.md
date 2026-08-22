# S-H arm 1 — the volatile-regime-gated compression breakout (#2840)

Refs #2840, #2832, #2437. Depends on #2829 (the declaration freezes after this lands).

Contract version: `sh-volatile-regime-gate-2026-08-22`. ⚠ That string is what the frozen
declaration will carry in `PreregDeclaration.contract_version`, and it is how the readout
rule in §"Readout and abort bar" below becomes part of the declaration. The digest hashes
the STRING, not this file's bytes (`prereg_contract.digest_payload`), so once the freeze
runs this document is append-only: a correction is a new contract version and a new trial.

## What this ships

One new manifest strategy, `s11-volatile-regime-gated-breakout` — S-4's entry rule
conjoined with `regime ∈ {bear_volatile, bull_volatile}`. Nothing else. The declaration
freeze, the exploration run and arms 2/3 are separate acts, sequenced in §Sequencing.

**Name.** The R5 sweep calls this candidate S-H; the manifest numbers its strategies
`s{N}-`, and the id has to live in the manifest. `s11-` continues that sequence and the
S-H mapping is recorded here and in the module docstring. No other R5 candidate has taken
a manifest id, so there is nothing to collide with.

## Why a new strategy id and not a new version of S-4

#2840 step 1 says *"as a NEW strategy_version"*. A version bump is the wrong shape:

1. **S-4's stored evidence is the control, and a bump orphans it.** Editing
   `s4_volatility_compression_breakout.py` moves `_source_hash()` and therefore the S-4
   identity, so results 501-504 and every other stored S-4 row stop being interpretable
   against the rule they were measured under — for a change S-4's rule did not undergo.
   This is the decisive one.
2. Keeping both rules live in the manifest also lets the gated and ungated arms be run
   from one invocation over one corpus load, which the stored control alone would not give
   for any window not already measured.

So S-4 is left **byte-identical** and the gate is a new module that imports it.

## The rule

    entry(t) := s4_entry(t)  ∧  regime(t) ∈ {bear_volatile, bull_volatile}

`s4_entry` is not restated — the module imports `compression_rank_series`,
`prior_high_close_series` and every S-4 constant. The bracket, the stop/target multiples
and the 40-bar hold cap are S-4's, unchanged: the gate conditions ENTRY, not the exit.

### Source rule

Neither half is invented here.

- The compression + breakout legs are S-4's, whose construction (and its explicit
  "no published formulation, fixed by construction" note) is in its module docstring;
  parent spec `strategy-catalogue-and-backtest-validity.md` §4.
- The regime is `app/services/market_regime.py`: close vs the 200-day SMA for trend, and
  Bollinger BandWidth at its 126-trading-day extreme for volatility — the published
  Squeeze/Bulge rule (*Bollinger on Bollinger Bands*, ch. 21), the one #2279 caught being
  replaced by an invented percentile cut. ⚠ Its boundary conventions are **inherited, not
  re-decided here**: `close > SMA` classifies equality as bearish and `>= max(window)`
  classifies a tied Bulge as volatile. Both are frozen in `REGIME_RULE_VERSION`, which
  S-11's identity hashes, so a future change to either is a new S-11 and not a silent
  re-reading of this one. Re-opening them is `market_regime`'s ticket, not this one.
- The permitted-regime SET is the hypothesis under test and is not a free parameter: it is
  the two cohorts #2840 named, frozen in `S11_PARAMS` and hashed into the identity.
  Moving it mints a different strategy.

## Identity — the one thing that has to be right

`S11_PARAMS` is written out key by key (no dict merge, so there is no collision to
resolve): S-4's seven parameters, `permitted_regimes` as a sorted tuple of enum VALUES
(`s5_support_bounce.py`'s serialisation, because a `frozenset` has no canonical JSON
form), `regime_rule_version`, and `s4_source_hash`.

⚠⚠ **`s4_source_hash` is the load-bearing member.** This module imports S-4's rule rather
than copying it, so an edit to `s4_volatility_compression_breakout.py` changes what S-11
DOES. Without S-4's hash in S-11's params, S-11's own `_source_hash()` would not move and
the changed rule would silently inherit the old track record. Same reason S-5 hashes
`LEVEL_RULE_VERSION` and `REGIME_RULE_VERSION` instead of relying on its own file hash.

⚠ **S-11 computes that hash itself from S-4's module path; it does NOT import
`s4._source_hash`.** That function is private and absent from S-4's `__all__`, and adding
it there would change S-4's file bytes — i.e. bump the S-4 version, which is exactly the
orphaning this design exists to avoid. A test asserts the two agree, so the duplicated
one-line construction cannot drift silently.

## The regime is a declared INPUT, not a body check

`s11_signals` declares the `RegimeSeries` as a `StrategyInput` with
`reason="missing_market_context"`, last, exactly as `s5_signals` does — and for the reason
S-6 shipped a bug over: a bar with no benchmark observation must be recorded
`not_evaluable / missing_market_context`, not silently "did not fire".

⚠ This is why the gate CANNOT be a post-filter over `s4_signals`' output. Filtering the
returned signal list would collapse "the regime says no" (`not_fired`) and "there is no
regime" (`not_evaluable`) into the same bar, destroying criterion 8's distinction at the
last step. Declaring the regime LAST keeps S-4's own OHLC/warm-up refusals taking
precedence, which is the honest order: a bar S-4 cannot evaluate is not a bar whose regime
matters.

## Measured premise, corrected

The ticket's headline cohorts are a 4× double-count. Query:

```sql
SELECT r.ambiguity_arm, r.quarantine_arm, c.regime, c.trade_count, c.expectancy_pct,
       c.profit_factor, c.expectancy_ci_low_pct, c.expectancy_ci_high_pct
FROM strategy_result_regime_cohorts c
JOIN strategy_results_store r ON r.result_id = c.result_id
WHERE r.strategy_id = 's4-volatility-compression-breakout'
  AND r.evidence_window_id = 'primary-2022-plus'
  AND r.result_scope = 'hold_out';
```

Each `(strategy, evidence_window, result_scope)` carries **four** rows —
`ambiguity_arm ∈ {best_case, worst_case}` × `quarantine_arm ∈ {masked, admitted}` — which
are four MEASUREMENTS over one window (`strategy_result.py:488`). They are not four
disjoint trade sets: `admitted` refuses fewer bars than `masked` and is a SUPERSET of it
(`research_price_structure_store.py:86-88` — *"`masked` is what every strategy reads and
is the default everywhere; `admitted` is the sensitivity arm — the same bars with the
quarantined fields left at their STORED values"*). Summing the four `trade_count`s counts
every trade up to four times.

`trade_count` in that table is **realised trades** — resolved entries, one row per fill,
regime attributed at the entry SIGNAL date (`strategy_regime_evidence.py` module
docstring: *"that is the information the strategy consumed when it decided to fire"*).
The CI is that module's clustered block bootstrap, clustered by decision date, with the
block length, resample count and seed stored per cohort row.

`worst_case` ambiguity, both quarantine arms:

| quarantine arm | regime | trades | exp %/trade | PF | 95% CI |
| --- | --- | ---: | ---: | ---: | --- |
| masked (default) | bear_volatile | 429 | +2.092 | 1.332 | [−0.668, +4.801] |
| admitted (sensitivity) | bear_volatile | 508 | +1.261 | 1.196 | [−0.867, +3.540] |
| masked (default) | bull_volatile | 555 | +0.585 | 1.103 | [−2.489, +3.267] |
| admitted (sensitivity) | bull_volatile | 811 | **−0.172** | **0.969** | [−2.734, +2.091] |

429 + 508 + 429 + 508 = 1,874 and 555 + 811 + 555 + 811 = 2,732 — the ticket's two counts
reproduce exactly, which is what identifies the pooling as their source.

Three consequences, all carried into the declaration rather than into this diff:

- the largest independent cohort behind the bear_volatile lead is **508 trades**, not
  1,874;
- **bull_volatile flips sign in the sensitivity arm**, so that half of the lead is a claim
  about the quarantine masking as much as about the regime;
- both volatile CIs span zero on the default arm, not just the bear one the ticket flags.

The gate is still declared on BOTH volatile regimes. Narrowing it to bear_volatile now
would be fitting the hypothesis to the hold-out cohort that suggested it.

## ⚠⚠ There is no clean confirmatory corpus window for this hypothesis

#2840's step 4 asks for *"one confirmatory shot on a window the exploration never
touched"*. Applied honestly, **no stored window qualifies.**

The hypothesis was formed by reading the `primary-2022-plus` hold-out cohorts. Every
pinned window in the store — `primary-2022-plus` (2022-01-01→2024-09-27), `rolling-36m`
(2021-09-28→), `rolling-24m` (2022-09-28→), `year-2022`, `year-2023`, `year-2024` — lies
inside or across that span, so all of them contain the trades that produced the lead. A
"confirmatory" run on any of them re-reads the data that generated the hypothesis, which
is the hold-out-becomes-training-data trap #2840 was written to avoid and which the ticket
records as having already killed the flip idea.

So the design splits differently from the one filed:

- **Exploration** — in-sample pre-2022, using the purged K-fold machinery (#2240/#2823).
  Never looked at for this hypothesis, so it is legal exploration and it answers the only
  question worth asking cheaply: does the volatile-regime edge exist at all outside the
  span that suggested it? A null here kills the candidate for the cost of one run.
- **Confirmation** — **forward shadow**, not a corpus replay. It is the only instrument
  whose data did not exist when the hypothesis was formed. This is the same conclusion
  #2840's own addendum reaches for arms 2/3 (*"the clean instrument is forward paper
  trading"*); it applies to arm 1 for a different reason (adaptivity rather than cost
  bands), and the two agree.

This is a correction to the ticket's step 4, stated here rather than quietly implemented.
It costs nothing today: step 4 was always conditional on step 3 passing.

## Readout and abort bar

Frozen under this contract version, before any look:

- **Report the four cells separately** — {bear_volatile, bull_volatile} ×
  {masked, admitted} — never a pooled volatile number. A pooled figure is what produced
  the 4× count above.
- **A pass needs bear_volatile positive in BOTH quarantine arms.** A result carried by
  bull_volatile-on-masked alone is a FAIL, because that cell is the one already known to
  flip sign under the sensitivity arm.
- **Abort bar on cohort n: 508.** ⚠ This is the largest independent cohort the lead itself
  rests on, i.e. an upper bound on the evidence available, NOT a power calculation — there
  is no published floor to cite and inventing one would be the made-up constant the
  instruction set forbids. It is fixed by construction: an exploration cohort smaller than
  the cohort that generated the hypothesis cannot be more informative than the hypothesis,
  so it is reported and NOT read as a verdict either way.
- **Decision metrics: `expectancy_per_trade_pct`, `profit_factor`, deflated Sharpe.**
  CAGR, Sharpe, Sortino and win rate are banned as decision metrics
  (`.claude/skills/quant/cost-aware-viability.md`).
- **Turnover after gating is measured, not assumed** (#2840 step 5). The gate should cut
  trade count; if it does not, that is reported as a fail of the stated mechanism even if
  expectancy improves.

## Sequencing — why the declaration is NOT in this PR

`PreregDeclaration.digest_payload` includes `strategy_version`, and for a MANIFEST
strategy that string is the identity hash (`result_ledger.py:574` writes
`identity.strategy_version` on every access row, and declarations are looked up by
`(strategy_id, strategy_version)`). A declaration frozen on the branch would be
invalidated by any review comment that touches the module, and `sql/333` bars UPDATE and
DELETE — recovery would mean a new strategy version and a second charge on the shared
trial register. Order:

1. **this PR** — the strategy exists, is registered, is tested;
2. add the `DeclaredTrial` register entry, then freeze the declaration against the merged
   `strategy_version` (`falsification_only`, under the survivor-only + carry-unmodelled
   stamps it will run on, which make it structurally unpromotable and therefore a
   declaration `capital_candidate` would be refused for);
3. explore on in-sample pre-2022;
4. forward-shadow confirmation, only if 3 passes.

Steps 2-4 follow `scripts/freeze_2837_se_overlay_declaration.py`'s pattern, including its
rule that the freeze script is never the script that opens the outcomes.

## Arms 2 and 3

Out of scope here, and not by omission: #2840's addendum states the corpus cannot assign a
cost band because its prices are split-adjusted, so the nominal-price ≥ $100 gate is a
bounded sensitivity in backtest and its clean instrument is forward paper. Arm 2 is a
forward-shadow declaration in its own right and does not need this module to exist first.

## Tests

Pure tier, no DB.

*The rule*
- S-4 fires + quiet regime → does NOT fire; the same bar + volatile regime → fires.
- Every one of the four regime values is exercised, so an asymmetric permitted set cannot
  pass.
- S-11's fired set is a SUBSET of S-4's over a generated series (the gate can only remove).

*The refusal distinction — the bug this design exists to prevent*
- `None` regime on a bar S-4 would fire → `not_evaluable / missing_market_context`.
- `None` regime on a bar S-4 would NOT fire → still `not_evaluable`, never `not_fired`
  (a short-circuiting implementation gets this wrong).
- A permitted-but-not-volatile regime → `not_fired`, distinct from the above.
- Masked OHLC on a permitted volatile bar → S-4's `masked_reason` wins, not the regime's.

*Identity*
- `S11_PARAMS["s4_source_hash"]` equals S-4's live `_source_hash()` — the drift guard.
- Changing the S-4 hash changes `s11_identity(...).version` (monkeypatched, so the
  dependency is proven rather than assumed).
- Changing the permitted set changes the version.
- Blank `cost_model_id` is rejected, as on S-4.

*Manifest wiring*
- The entry is present, non-retired, `per_series`, entry-only, with the S-4 exit regime
  (40-bar cap, level-based) and BOTH `exit_levels` and `exit_levels_batch` registered.
- The manifest's signals adapter passes the regime through (a copy of `_s4_signals`, which
  discards it, would silently un-gate the strategy — asserted directly).
- Scalar and batch exit levels agree, and equal S-4's for the same request.

*Housekeeping*
- The #2845 `KEPT` set grows to include `s11-…`, so "the manifest minus the retired eight"
  stays an exact assertion rather than becoming a stale one.

## What this is not

Not a new TA idea. The standing order forbids another daily-bar variant, and this is not
one: it is a declared conditioning of an EXISTING measured rule, which is #2840 as filed
and phase 2 of the R5b queue.
