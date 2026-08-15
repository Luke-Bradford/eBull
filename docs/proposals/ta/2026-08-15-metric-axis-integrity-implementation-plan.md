# Metric-axis integrity implementation plan (#2697)

Spec: `docs/proposals/ta/2026-08-15-metric-axis-integrity.md`.

## 1. Close the hold-out door and define durable interfaces

1. Replace `run_backtest(..., evaluation_window=...)` with
   `evidence_window_id: str | None`. Validate the complete invocation matrix
   before `load_corpus`: in-sample has no ID/audit fields; registered hold-out
   has a known ID and both non-blank audit fields; every mixed, unknown, raw or
   default/full-history hold-out form refuses.
2. Migrate scheduler jobs, recent-evidence refresh, verification scripts and
   tests. Assert every invalid combination fails before the corpus loader is
   touched and every registered window resolves to its complete observed panel
   tuple.
3. Make `RECENT_EVIDENCE_WINDOWS` and its SQL ID/date mirror append-only. Tests
   compare the complete historical Python/SQL mapping, so removing or mutating
   a used ID fails; additions require a migration.
4. Add seven nullable store columns: `metric_axis_rule_version`,
   `metric_axis_dates`, `metric_axis_start`, `metric_axis_end`,
   `metric_axis_digest`, `opportunity_set_digest`, and `evidence_window_id`.
   Update the in-sample view.
5. SQL constrains the six axis/opportunity fields all-null for legacy or
   all-present for current rows; current arrays have at least two
   strictly-increasing unique dates, endpoints equal array ends, dates inside
   the declared window, in-sample dates before the boundary, a recognised
   current rule, and hold-out ID/date pairs matching the registry. The migration
   does no backfill. DB tests prove old rows stay all-null and malformed direct
   rows fail.

## 2. Derive the axis and opportunity record before identity planning

1. During corpus load, derive the fixed namespace date tuple and the pre-mask
   opportunity name set per namespace. Membership is series-based and requires
   two dates with finite positive raw and total-return closes; it is independent
   of masks, evaluability verdicts, signals, fills, positions and costs.
2. Split each set into linked instrument IDs and unlinked series IDs and build
   the exact `ResultUniverseRecord` before any strategy pass. Its canonical
   payload hash is `opportunity_set_digest`; count semantics everywhere become
   linked plus unlinked names.
3. Move result collision planning to after corpus/axis/population derivation and
   before strategy evaluation. Planned identities include the full axis tuple,
   its canonical digest, opportunity digest and optional evidence-window ID.
4. Tests prove per-namespace membership, arm equality, firing and non-firing
   unlinked names, independence from all downstream outcomes, every member
   reaching `_signals_for`, no-opportunity versus no-trade distinction, and a
   hard failure when any trade name is absent from the opportunity set.

## 3. Build every metric on the fixed axis and population

1. Add the frozen axis digest helper and `DatedEquityCurve`. It rejects one-date,
   duplicate, unordered and curve-length-mismatch inputs. `compute_metrics`
   accepts dated strategy/benchmark values and rejects equal-length curves with
   different date tuples.
2. `NamespaceMeasurement` carries the complete tuple and opportunity record,
   not endpoint scalars alone. `build_result` accepts those exact values only.
   Fold and label-window indices remain on the full corpus axis; regression
   tests prove metric rebasing cannot move them.
3. Build strategy curves over the complete namespace tuple. Tests cover cash on
   both tails; independent CAGR/periods-per-year reconciliation; volatility,
   Sharpe, Sortino, exposure, turnover and drawdown effects; and explicit
   refusal—not clamping/dropping—for realised, open and termination legs beyond
   either boundary.
4. Replace the no-leg and null-ESS exception paths with an all-cash row when the
   opportunity set is non-empty. Pin zero metrics, null Sortino/bootstrap/ESS,
   null moments/DSR/control, empty regime cohorts, deflation handling, expected
   refusal cross-check and writer completeness. A genuinely empty opportunity
   set still produces no row and fails run completeness.
5. Plumb raw/wealth histories by signed name key so linked and unlinked series
   both reach the comparator. Build exactly one frozen close-fill,
   `UNKNOWN_NOMINAL_PRICE_BAND`, non-rebalanced leg per name. Pin reserved cash
   before listing and cash after terminal exit.
6. Benchmark tests cover missing series, zero legs, fewer than two distinct
   usable endpoints, invalid/reversed spans, non-finite/non-positive raw and
   wealth endpoints, observed-raw/invalid-wealth refusal, allowed missing-raw
   interior gaps, exact population cardinality, masked-arm refusal naming the
   series, and no silent omission.
7. Pass the real measurement's exact in-sample tuple into `run_cohort`; prohibit
   member slicing. Tests prove every member uses that tuple, changing placement
   changes only the path, and an all-cash parent launches no cohort. Revert-probe
   both old strategy and synthetic span cuts.

## 4. Bind identity, ledger and every promotion path

1. Extend `ResultIdentity` and `StrategyResult` with the seven fields. The
   legacy branch requires axis rule, opportunity digest and window ID all null,
   omits every new field and preserves every old prefix/hash byte-for-byte.
   Current rows use a new axis-aware prefix. Mixed states are invalid.
2. Mutation tests prove current identity moves with an interior axis date,
   endpoints, rule, axis digest, opportunity population and evidence-window ID.
3. Update ledger insert/select/read-back, in-sample and hold-out arms, pair and
   recent readers. Writer and reader recompute tuple digest, endpoints,
   `periods_per_year`, CAGR and result identity. After inserting the result, the
   universe child is written atomically and must reproduce the identity's
   opportunity digest and linked+unlinked count.
4. Add `metric_axis_unproven`, bump the structural-refusal policy version, and
   update the deliberately independent expected-refusal table. Share one axis
   validation helper between pure `check_promotable`, preregistration and stored
   replay without making the write-time cross-check self-referential.
5. Test pure gating, preregistration, control-plane promotion, stored replay,
   arm-pair/recent readers, legacy rows, corpus-free integrity replay, missing
   universe child and mismatched child digest. A direct row without coherent
   children refuses every entry point; the documented database-superuser threat
   boundary remains explicit.

## 5. Derivation verification and in-sample A/B

1. Add a full-population verifier that distinguishes corpus-free integrity
   replay from source-dependent derivation replay. The latter compares stored
   tuples/opportunity sets with the frozen corpus and the pre-mask query.
2. Add an in-sample-only A/B script. It makes one causal corpus pass, applies a
   script-local plainly named legacy `first_index..last_index` helper to the
   already-produced books, exposes no production legacy switch, and reads no
   stored/new hold-out outcomes.
3. Emit per-row—not aggregate-only—old/new endpoints, opportunity and comparator
   population, total return, CAGR, periods/year, volatility, Sharpe, Sortino,
   drawdown, exposure, turnover, benchmark return, excess return and synthetic
   threshold deltas.

## 6. Gate, review, merge and rerun

1. Run focused pure/DB tests and migration smoke only after worker 98349 releases
   the database lock/CPU window. Run the derivation verifier, full in-sample A/B,
   complete local gate and adversarial revert probes.
2. Self-review, run the required semantic diff review, push, read every CI/bot
   response, resolve each explicitly, re-run affected gates and merge only on
   the latest approved green commit.
3. Record run 98349 as legacy development evidence that is structurally refused
   by `metric_axis_unproven`; do not interpret its performance.
4. Start a corrected in-sample run only after the merged schema, tests, full
   derivation verifier and A/B integrity checks—not their performance—pass.
   Read no results until its atomic invocation audit succeeds.
