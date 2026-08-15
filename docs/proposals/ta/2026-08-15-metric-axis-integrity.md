# Metric-axis integrity for strategy results (#2697)

## Status and scope

This proposal replaces the result-dependent metric axis in
`backtest_run._measure_namespace`. It changes metric derivation and result
identity. It does not inspect hold-out outcomes, change a strategy, select a
candidate, or authorise capital.

Run 98349 began before this contract existed. Its output may be retained as
development evidence, but it is not promotion evidence unless it is reproduced
under the corrected identity.

## Reproduced defect

The stored `window_start` / `window_end` name the evaluation window, while the
curve and benchmark are cut to the first entry and last exit in that namespace.
The cut is chosen from realised positions, hence from the strategy's outcome.
It removes every zero-return cash period before the first trade and after the
last trade. CAGR, annualised volatility and Sharpe therefore describe an
unstored, result-dependent period.

The old long-window rows expose the contradiction: their total return and CAGR
imply roughly 5--8 years while their identity declares 64.5 years. The current
benchmark correction (#2426) makes strategy and benchmark share the shorter
axis, but it does not make the missing metric period auditable or remove the
outcome-dependent cut.

## Source rule

No published formulation was found for selecting a performance axis on this
project's unbalanced, listing-and-delisting panel. The existing benchmark rule
is already fixed by construction for the same reason. Two applicable rules do
not settle that panel question but constrain the answer:

- the existing equity engine defines uninvested capital as cash earning zero
  and uses the full allocated pot as the denominator;
- an evaluation boundary must be known without observing whether or when the
  strategy later trades. Selecting the first realised trade violates that
  causal requirement.

Accordingly the axis is fixed by construction below. It is not inferred from
the return that looks best.

## Corrected construction

### One fixed date axis

There is no inferred readiness date. Warm-up is part of the strategy and earns
zero while it prevents trading. For each
`(result_scope, strategy, namespace, ambiguity arm, quarantine arm)`:

1. The **in-sample** metric axis is every ordered, unique panel date inside the
   declared evaluation window and strictly before `HOLDOUT_BOUNDARY`.
2. A **registered recent hold-out** metric axis is every ordered, unique panel
   date inside that code-pinned window. These windows are wholly on or after
   `HOLDOUT_BOUNDARY`.
3. A default full-history hold-out request is refused. Its current population
   mixes positions opened before the boundary with positions opened after it,
   and no causal return-rebasing rule has been frozen for that mixture. The
   promotion campaign uses the registered recent windows; it must not reach the
   ambiguous path.
4. The first and final calendar dates need not contain a panel observation. The
   stored metric endpoints are the first and last dates actually present on the
   selected panel axis. Fewer than two dates is a refusal.
5. The strategy holds zero-return cash before its first entry and after its last
   exit. Warm-up, an evaluable-but-not-fired period, and a strategy that never
   fires are therefore not deleted from time. Open positions retain the existing
   mark/freeze rules through the fixed axis end.

Fold indices and label windows remain positions on the full corpus axis. Metric
indices are a separate rebased view and never overwrite fold provenance.

### One opportunity population

The opportunity set is computed once from the **pre-mask admitted-series
selection** returned by `load_universe_selection`, independently of either arm.
A name enters a namespace's set when its admitted series has at least two dates
on that namespace's fixed metric axis whose stored raw close and stored
total-return close are both finite and strictly positive. Every
such series is passed through `_signals_for`; the set does not depend on an
evaluable verdict, signal, fill, position or cost outcome.

The persisted shape reuses `strategy_result_universe`: linked names are stored
as `evaluated_instrument_ids`; admitted unlinked dead names are stored as
`evaluated_series_ids`. The in-pass comparison key remains the existing positive
instrument ID or negative `-series_id`, and the benchmark is keyed on that same
name set. `evaluated_instrument_count` equals the sum of the two stored sets.
Unlinked series are therefore neither discarded nor forced through an
instrument foreign key.

The set is accumulated independently of `_NamespaceBook` and therefore cannot
move when only a firing verdict moves. Both quarantine arms use this same
pre-mask set. A masked series still runs; if masking leaves it unable to satisfy
the benchmark endpoint rule below, that arm refuses naming the series rather
than silently removing it.

### Strategy, comparator and synthetic control

1. Every strategy curve is built on the complete fixed metric axis. Any
   realised/open/termination leg outside it raises. An excluded or uncosted
   position is reconciled by its existing census and does not create a curve
   leg.
2. The buy-and-hold comparator is built on the exact same date tuple and exact
   opportunity-set IDs. Each instrument contributes one non-rebalanced leg from
   its first usable raw/wealth close on or after the metric start to its last
   usable close on or before the metric end. Capital reserved for a later-listed
   instrument remains cash until that entry; proceeds after its terminal exit
   remain cash. This is the existing benchmark construction widened to the
   causal population.
3. The benchmark retains its frozen close-fill convention and
   `UNKNOWN_NOMINAL_PRICE_BAND` round-trip cost. Those are comparator rules,
   already named by `BENCHMARK_RULE_ID`; they are not claimed to be the
   strategy's next-open execution rule.
4. The benchmark must create exactly one leg per opportunity-set name key.
   Missing series, fewer than two usable closes, invalid spans, or a zero-leg
   comparator raises with instrument/series IDs instead of silently shrinking the
   population.
5. Every random-entry synthetic member uses the real strategy's complete fixed
   in-sample metric date tuple. Its random first/last placement cannot move its
   annualisation period. A synthetic control remains absent when the real
   strategy has no realised holds; the parent all-cash result is unpromotable
   through the existing evidence gates.
6. Introduce a `DatedEquityCurve(dates, curve)` value. Its constructor requires
   strictly increasing unique dates, at least two dates, and equal curve/date
   lengths. `compute_metrics` accepts the strategy value and an optional
   benchmark value and compares their complete date tuples before computing.
   Equal curve lengths are not proof of date identity.

For a benchmark name, a usable endpoint is a date on the metric axis where raw
and wealth closes are both finite and strictly positive. The leg uses the first
and last such endpoints and requires two distinct dates. Between them, a missing
raw close is an allowed non-trading gap; every observed raw close requires a
finite positive wealth close. Any other non-finite/non-positive observed value
refuses that name and the arm. The existing close-fill and non-rebalancing cash
semantics are unchanged.

### Durable provenance and identity

1. Store `metric_axis_dates` (`date[]`), `metric_axis_start`,
   `metric_axis_end`, and `metric_axis_digest` on every result. The tuple makes
   replay independent of a mutable database; endpoints alone cannot detect a
   missing interior session.
2. Add `metric_axis_rule_version = "full-namespace-panel-v1"` and
   `opportunity_set_digest` to `ResultIdentity`, and hash the rule, complete
   tuple, endpoints, axis digest and opportunity-set digest. The latter is the
   exact `strategy_result_universe.payload_sha256` computed before evaluation
   from the pre-mask linked/unlinked sets, validated universe and universe rule.
   The writer requires the child record to reproduce the identity digest.
   The digest is lower-case SHA-256 over UTF-8 bytes of
   `axis-v1:` followed by compact JSON (`separators=(",", ":")`) of the ordered
   ISO `YYYY-MM-DD` strings. Corrected rows
   therefore cannot collide with legacy results or with the same calendar over
   a different admitted population. Result planning occurs after `load_corpus`
   and the pre-mask population query, so both identities are known before any
   strategy pass.
3. `NamespaceMeasurement` hands its exact dates/provenance to `build_result`;
   the writer may not recompute or accept caller-provided substitutes.
4. New rows require the current rule, opportunity-set digest and all four axis
   fields. Legacy rows keep all six fields null. The SQL all-or-none check distinguishes those states;
   every all-null row is permanently unpromotable regardless of its result
   prefix. For current rows, SQL validates at least two strictly increasing
   unique dates, endpoints equal the first/last array members, containment in
   the declared window, and in-sample dates strictly before the boundary. The
   writer and reader recompute the digest and result identity. Blank/unknown
   rules, one-sided nulls, reversed/out-of-window endpoints, a digest mismatch,
   current-rule nulls, and legacy rows carrying dates are invalid states.
5. `periods_per_year` and CAGR are recomputed from the exact date tuple before
   write and must reconcile with the stored endpoints/digest.
6. The store, in-sample view, result reader, recent-evidence reader, arm-pair
   readers and promotion replay all carry the rule, opportunity digest and four axis fields. Direct/manual SQL
   rows cannot bypass the SQL shape/order/containment checks and remain
   unpromotable unless application replay verifies their digests, identity and
   frozen universe child record.
7. Add the closed refusal `metric_axis_unproven` and bump the refusal-policy
   version. Both `check_promotable` and the stored-result promotion replay use
   the same validation helper. Preregistration treats a legacy/missing axis as
   structurally unpromotable.
8. No migration backfills provenance. Reconstructing dates under current code
   would splice a new derivation onto an old result. Run 98349 therefore remains
   durable development evidence and cannot be pinned for promotion.

`strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS` is the canonical hold-out
registry. IDs are immutable and append-only: a date change gets a new ID; an ID
already used by a row is never removed. The SQL mirror is likewise append-only.
`run_backtest` receives an `evidence_window_id`, resolves it through
`recent_evidence_window`, and no longer accepts a public raw `Window`.
The ID is stored and hashed into the result identity. In-sample rows require it
to be null. Hold-out rows require the ID and exact registered start/end dates.
The SQL constraint mirrors the current closed ID/date mapping, with a coupling
test that fails when the Python registry changes without a migration.

The invocation modes are closed:

- in-sample: no evidence-window ID and neither hold-out audit field;
- registered hold-out: a known evidence-window ID plus non-blank
  `holdout_purpose` and `holdout_accessed_by`;
- every other combination—including hold-out audit fields without an ID, an ID
  without both audit fields, or any raw window—is rejected before `load_corpus`.

Legacy identity compatibility is explicit. When the axis rule,
opportunity-set digest and evidence-window ID are all null, the version method
omits every new field and preserves the existing hash/prefix byte-for-byte.
Current rows use a new axis-aware result-set prefix and include every new field.
Mixed legacy/current fields are invalid.

The stored date tuple supports **integrity replay** without corpus access:
reader and promotion can recompute its digest, endpoints, metrics and identity.
It does not independently prove that the tuple was the complete source-derived
panel axis. **Derivation replay** additionally compares it with the frozen
corpus and the pre-mask opportunity-set query. That check runs before write and
in the full-population verifier; the distinction is reported rather than
claiming the immutable row proves its own source selection.

Threat boundary: an ordinary direct writer that omits or mismatches the frozen
universe child record remains unpromotable. A database superuser can disable
triggers and fabricate a mutually consistent result, child records and hashes;
no in-database audit ledger can prove itself against that actor. The acceptance
claim does not pretend otherwise.

## Failure semantics

- Fewer than two metric-axis dates: refuse before metric computation.
- No signal-evaluated opportunity-set instrument: write no result.
- No realised trade: write an all-cash metric row over the full axis with zero
  return/CAGR/volatility/Sharpe/drawdown/exposure/turnover and zero trade
  counts; Sortino, clustered-bootstrap, ESS, DSR and synthetic-control fields are null,
  and their existing absence refusals keep promotion closed. This explicitly
  replaces `_measure_namespace`'s no-leg `None` and null-ESS exception paths.
- A trade with no independently accumulated opportunity-set membership: raise.
- Any strategy leg outside the selected axis: raise; never clamp or drop it.
- Strategy and benchmark date tuples differ: raise.
- Missing or inconsistent stored axis provenance: fail promotion closed.
- Any non-registered or full-history hold-out request: refuse before reading
  withheld metrics.

## Acceptance

1. A ten-date fixture whose first trade spans indices 5--7 measures all ten
   dates, preserves cash on both tails, and keeps the trade itself unchanged.
2. Moving only the first firing later or the last exit earlier does not move
   either metric endpoint or the digest.
3. Warm-up and a never-firing but signal-evaluated instrument remain in the
   time axis and opportunity set; changing only a firing verdict cannot remove
   that instrument from the comparator.
4. Separate cases refuse an unordered/duplicate/one-date axis, a strategy leg
   outside either boundary, and a comparator missing any opportunity-set ID.
5. CAGR and `periods_per_year` independently reconcile to total return and the
   exact stored date tuple. Cash padding is also asserted for volatility,
   Sharpe, Sortino, exposure, turnover and drawdown.
6. Strategy, comparator and every synthetic member use the same exact date
   tuple. Moving only a random member's first/last placement changes its path,
   not its annualisation span.
7. Every new ledger round-trip persists the endpoints, digest and rule. Tests
   cover blank/unknown rule, one-null date, reversed/out-of-window endpoints,
   digest mismatch, current-rule nulls and legacy-rule dates.
8. Both pure promotion and database promotion replay refuse legacy/missing or
   inconsistent provenance with `metric_axis_unproven`; arm-pair and recent
   readers cannot omit it.
9. A full-history hold-out request refuses before measurement; every registered
   recent window uses its complete panel tuple.
10. Revert probes restoring `lo = book.first_index`, `hi = book.last_index` and
    synthetic-member truncation fail named tests.
11. An **in-sample-only** full-population A/B reports per-row old/new endpoints,
    total return, CAGR, periods/year, volatility, Sharpe, Sortino, drawdown,
    exposure, turnover, comparator population/return/excess, and synthetic
    threshold deltas before corrected evidence is interpreted. It never opens
    hold-out data.
12. Additional cases cover equal-length curves with different dates, an
    all-cash ledger round-trip and refusal set, identical pre-mask name sets in
    both quarantine arms, a firing unlinked series, arbitrary raw hold-out
    window refusal, stored universe set/count equality, and replay with no
    corpus access.
13. Benchmark cases cover fewer than two endpoints, non-finite/non-positive raw
    and wealth endpoints, an observed raw close with invalid wealth, an allowed
    missing-raw interior gap, and exact one-leg-per-name enforcement.
14. Identity tests prove legacy hashes remain byte-identical, current hashes
    move with the opportunity set or evidence-window ID, and result/universe
    digest disagreement refuses both write-time validation and promotion.
15. A direct result row without the required universe child record refuses at
    every promotion entry point; the privileged-superuser threat boundary above
    is not misrepresented as an executable test.

The A/B script owns a plainly named legacy-span helper that applies the old
`book.first_index..book.last_index` slice to the already-produced in-sample
books. Production code exposes no legacy switch. The script runs both metric
constructions over the same single causal corpus pass and never reads stored or
new hold-out results.
