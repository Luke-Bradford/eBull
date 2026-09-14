# #3031 — put the quarantine rule set inside `strategy_version`, and close the walk's blind spot

Spec. Autonomy loop, 2026-09-14, from `151b93cc`. Revised after Codex checkpoint 1
(27 findings; the disposition of each is on the PR).

## The defect

`app/services/strategy_registry.py::INPUT_RULE_SETS` does not name
`price_quarantine`. The live signal scan's bar loader
(`app/services/price_masked_bars.py::_LOAD_SQL`) selects coverage rows with
`cov.rule_set_version = %(quarantine_version)s`, bound from that module's
`RULE_SET_VERSION`. So the quarantine rule set decides which bars the strategies
see, and today it does so under an unchanged `strategy_version`.

Every table the scan writes keys on `strategy_version`:

| table | key |
| --- | --- |
| `strategy_signals` | `UNIQUE (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)` |
| `strategy_signal_daily_counts` | `PRIMARY KEY (strategy_id, strategy_version, signal_bar_date, signal_kind, verdict, reason_code)` |
| `strategy_signal_observations` | `PRIMARY KEY (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)` |
| `strategy_scan_watermark` | `PRIMARY KEY (strategy_id, strategy_version)` |

Two scans run under different masking rules therefore write into the same key
space and are indistinguishable afterwards.

## Source rule — and the reconciliation, not a dismissal

Two conventions coexist in this schema, deliberately. `sql/257`'s header says so
in terms: *"Two different answers because the two hashes cover different things,
not because one of them is wrong."*

**The signal ledger hashes its input rule sets** —
`sql/257_strategy_signals_input_rule_sets.sql`:

> Fixed in the HASH (`strategy_registry.INPUT_RULE_SETS` is now part of the
> version payload), which is what makes the corrected row storable. This column
> is the QUERYABLE half.
>
> ⚠ DELIBERATELY NOT IN THE UNIQUENESS KEY — It is INSIDE `strategy_version`,
> exactly as `universe` is. Adding it to the key would permit one strategy
> identity to span two indicator rule sets, which criterion 11 says is not one
> strategy.

`docs/review-prevention-log.md` (#2333 entry) names the key-member alternative as
the error for this table specifically: *"Copying 4b's shape literally would have
put it in the uniqueness key and let one strategy identity span two indicator
rule sets."*

**The results ledger keys on its input version instead** — `sql/256`, `sql/262`,
`sql/267`. `sql/267`'s *"⚠ NOT hashed into `strategy_version` … this is a
property of how a RESULT was measured, which is the same distinction that puts
`input_rule_set_version` on this table rather than in the strategy hash"* is a
statement about `strategy_results_store`, and this change does not touch it or
propose to.

### Why the signal ledger takes the other convention

Two structural differences decide it, and neither is a preference:

1. **The results ledger has a SECOND hash; the signal ledger has none.**
   `ResultIdentity` produces `result_version`, which is where a measurement
   property can live without being mistaken for the strategy. There is no
   `signal_version`. On the signal ledger a rule set that is neither in
   `strategy_version` nor in the key has nowhere to go at all — which is `sql/256`'s
   own "unstorable" argument, arriving at the opposite shape because the hash it
   would sit in exists here and does not there.

2. **The scan has ONE arm, so masking is an input rather than a choice.**
   `price_masked_bars`'s docstring: *"ONE ARM, NOT TWO. `research_price_structure_store`
   carries criterion 9's `masked`/`admitted` pair because it feeds a sensitivity
   measurement. This is a production read … Adding the arm here would put a
   switch on the production path whose only correct setting is the default."*
   `sql/267` exists because two arms of one strategy collided; a scan cannot
   produce that pair. What varies on the scan path is not the handling but the
   rule set, which is the same category as `market_regime_provider`'s benchmark
   source — whose registry comment states this ticket's argument verbatim:
   *"switching the backtest source flips every pre-2023 bar of a regime-gated
   strategy from `not_evaluable` to a real verdict, which is a changed input
   under an unchanged `strategy_version` unless it is hashed here."*

⚠ `StrategyIdentity` is shared by the scan and the backtester, so hashing the
quarantine version moves `strategy_version` for backtests too. That is correct
and not a side effect: `research_price_structure_store` masks on the same rule
set, so a backtest run under a changed quarantine is a changed input there as
well. What `sql/267` settles — that the *arm* is a result property — is
unaffected, because `quarantine_arm` stays on `strategy_results_store` and stays
out of the hash.

## Change

One registry entry, its test pin, and one new completeness test. **No migration.**

1. `app/services/strategy_registry.py` — a fourth hand-maintained entry:

   ```python
   "price_quarantine": QUARANTINE_RULE_SET_VERSION,
   ```

   Precedent is exact: `market_regime_provider`, `series_termination` and
   `universe_selection` are all named by hand because the ENGINE consumes them
   and no strategy imports them, so the import walk cannot see them.

2. `tests/test_strategy_registry.py::test_the_stored_mapping_is_the_hashed_one` —
   add the entry to the pinned mapping.

3. A behavioural test rather than an identity check: two `StrategyIdentity`
   versions computed over mappings differing only in the `price_quarantine` value
   must differ, and `price_masked_bars` must bind the registry's value into
   `_LOAD_SQL`'s `quarantine_version` parameter. Object identity of a string
   constant proves nothing about the executed query, so the bind is asserted from
   the module's own SQL parameters.

4. **Close the blind spot that let this through.** `TestInputRuleSetsAreComplete`
   ast-walks `app.services.strategies` for DIRECT imports and documents that
   limit — but this defect lived in exactly that documented blind spot, and a
   documented blind spot is still a blind spot. Add a companion test that walks
   the ENGINE's import closure from `app.services.strategy_signal_scan` and
   requires every rule-set version it can reach to be covered.

   Three design points, each answering a way the naive version would pass while
   blind:

   - **Key on the VALUE, not the constant name.** `RULE_SET_VERSION` is not the
     only spelling — `LEVEL_RULE_VERSION`, `REGIME_RULE_VERSION`,
     `TERMINATION_RULE_VERSION`, `UNIVERSE_SELECTION_RULE_VERSION`,
     `AMBIGUITY_RULE_VERSION` and `METRIC_AXIS_RULE_VERSION` all exist in the
     closure. Matching `*RULE_VERSION` / `*RULE_SET_VERSION` finds the constants;
     comparing their VALUES against the covered set is what makes re-exports
     collapse automatically (`price_masked_bars.QUARANTINE_RULE_SET_VERSION`,
     `research_corpus_ingest.RULE_SET_VERSION` and
     `research_price_structure_store.QUARANTINE_RULE_SET_VERSION` are all the same
     string as `price_quarantine.RULE_SET_VERSION`).
   - **Covered means hashed ANYWHERE in the identity, not just in
     `INPUT_RULE_SETS`.** `price-levels-v1+…` and `market-regime-v1+…` are hashed
     through per-strategy `params`, not through the registry mapping, and both are
     correct as they are. The covered set is `INPUT_RULE_SETS.values()` ∪ every
     string value in every manifest entry's `params`.
   - **Fail closed.** A reachable version in neither the covered set nor the named
     exclusion list fails the test.

   Measured today: the closure is 45 modules; the covered set holds six rule
   versions; seven reachable versions are uncovered:

   | uncovered version | disposition |
   | --- | --- |
   | `price-quarantine-v1+…` | **added to `INPUT_RULE_SETS` by this change** |
   | `outcome-resolver-v1+…` | excluded — `sql/256` makes it a KEY member of `strategy_outcomes`, deliberately outside the strategy hash |
   | `position-builder-v1+…` | excluded — backtest position construction, a property of a RESULT |
   | `price-structure-v1+…` | excluded — reached only via `backtest_run` / `research_price_structure_store`; no live-scan path reads it |
   | `validated-universe-us-stocks-v1` | excluded — a universe LABEL; `universe_selection` carries the versioned admission rule and is already in `INPUT_RULE_SETS` |
   | `ambiguity-verdict-…` (v1 + v2) | excluded — result-ambiguity policy; `#2747` owns versioning it into result identity |
   | `full-namespace-panel-v1` | excluded — the result METRIC axis, a property of a result |

   ⚠ Limits of the walk, stated rather than implied: it follows
   `from app.services.x import …`, `import app.services.x` and
   `from app.services import x`; it does not resolve dynamic imports, and it
   cannot see a rule set reached through a non-`app.services` intermediary. A
   test pinning that the walk finds a known module guards the "matched nothing"
   failure, the same guard `test_the_walk_finds_the_strategies` already provides.

## What this change does and does not cover

- ✅ New scans under a changed quarantine rule set land on a new
  `strategy_version`, so all four tables separate them without a migration.
- ✅ The provenance guard #3031 §E asks for is **structurally unnecessary under
  this shape**. `strategy_scan_watermark` keys on `(strategy_id, strategy_version)`,
  so a rotated identity has no watermark to continue past;
  `write_window_indices(watermark=None, …)` writes the single frontier-minus-one
  bar, the documented cold start. Under the key-member shape the watermark would
  have been shared and the guard would have been required.
- ⚠ **Every future quarantine edit now rotates every identity, with no registry
  edit.** Accepted deliberately, and it is the point rather than a cost — it is
  what makes the corrected row storable. It is also the posture already recorded
  for `universe_selection`: *"This over-invalidates survivor-only identities on a
  survivorship-free rule change — accepted deliberately, the same global-rule-set
  over-invalidation every entry in this mapping makes."*
- ⚠ **A fresh identity has no watermark, so `refused_frontier_regressed` cannot
  fire on its first scan.** True of every rotation, including the 32 already
  taken. It is the reason #3028's ordering constraint below is not optional.
- ❌ It does not recover provenance for the existing rows. `sql/257` records that
  the producing version *"is recoverable only by guessing which historical module
  source hashes to the digest"*; run timestamps and job logs may bound some rows
  by date, so the honest claim is "not recoverable from the ledger", not "not
  recoverable at all".
- ❌ It does not version `price_masked_bars`'s own mirror SQL, nor
  `price_segments.load_unresolved_breaks`, which reads mutable break rows carrying
  no version at all — a quarantine-stamped signal can still combine bars and
  segmentation from different states. Same class of limit every entry in the
  mapping has; named here so it is not mistaken for coverage.

## Cost of the rotation — full-population census, dev DB, 2026-09-14

Adding a key to `INPUT_RULE_SETS` rotates every strategy's `strategy_version`.
Two facts bound what that costs, and both are measurements.

**1. Editing `strategy_registry.py` rotates every identity anyway.** `version`'s
payload carries `_module_hash()`, which hashes that file. The rotation is not
attributable to the mapping entry.

**2. Almost nothing is on a current identity to lose.** Every table carrying
`strategy_version` was counted against the identities the manifest produces today
(`entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version`):

| table | rows | on a CURRENT identity |
| --- | --- | --- |
| `strategy_signals` (verdict `fired`) | 58,711 | **1,572** (s4 1,450, s8 122) |
| — of those, unresolved by `strategy_outcomes` | 56,720 non-current | **1,331** |
| `strategy_results_store` | 580 | **0** |
| `strategy_preregistration_declarations` | 7 | **0** |
| `strategy_holdout_accesses` | 560 | **0** |
| `strategy_deployments` | 0 | 0 |
| `strategy_promotions` | 0 | 0 |
| `strategy_scan_watermark` | 32 | 3 |

Reproduce with `scripts/census_3031_identity_rotation.py` (added by this change):
run it at `origin/main` for the column above and on this branch for the after
state, which reads `0` on every row. It recomputes the current identities from
`STRATEGY_MANIFEST` rather than filtering stored rows by a version constant
imported from the module under test — that constant stops matching the moment the
module is edited, and the empty baseline prints `0`, which is what a pass looks
like.

So the measurable loss is **1,331 unresolved forward fills** and **three
watermarks**. Everything the rotation is feared to restart — results, folds,
declarations, hold-out exposure accounting, deployments, promotions — is already
at zero-on-current, because `strategy_version` has rotated 32 times across 11
strategies and nothing re-points those tables.

⚠ **Two consequences recorded rather than fixed, because neither is this
ticket's and both predate it:**

- `run_outcome_resolution` selects *"current-version forward fills"*, so the 1,331
  join the 56,720 already stranded. The same function's own comment refuses this
  outcome for retired strategies — *"Filtering them would strand those
  permanently — corrupting the very evidence record retirement exists to
  preserve"* — while the version filter does it by construction. Whether that is
  a decision or a defect wants its own look; noted on the PR.
- Re-resolving an ALREADY-resolved signal under a changed quarantine version is
  likewise foreclosed by that filter. Also pre-existing.

## Timing — measured, not inferred

`price_quarantine.RULE_SET_VERSION` is `price-quarantine-v1+<code hash>` and
#3028 (`7aa5f7e3`) changed `rule_b4` today, so the constant has already moved on
`main`. `max(created_at)` on `strategy_signals` is **2026-09-13 06:47 UTC**: no
scan has written a row since, so no row yet exists whose masking rule is
ambiguous. Landing this before the next deploy is what keeps the two rule sets
out of one key space, rather than fixing it after the first mixed day.

⚠ This is an argument about scan output, not about process state. Daemon uptime
would not prove which code is loaded; the row timestamps do.

## Ordering constraint inherited from #3028 — NOT discharged here

#3031 §F applies unchanged: do not let the signal scan run between the deploy and
the completion of both #3028 item-3 re-evaluations, and reconcile coverage rather
than trusting a single counter. `price_masked_bars._LOAD_SQL` INNER JOINs
`price_quarantine_coverage` on the pinned version, so an instrument whose
coverage has not been re-evaluated returns zero bars and leaves the population
silently. ⚠ `ScanReport.excluded_no_bars` is necessary but not sufficient —
partial date coverage and a shrunken frontier escape it, and a wholly empty
loader refuses with `refused_empty_universe` leaving the counter at its zero
default. Passed through to #3028's handoff.

## Security

No security surface: an in-process identity constant, one census script and two
tests. No new input, no new endpoint, no credential or authorisation path touched.
