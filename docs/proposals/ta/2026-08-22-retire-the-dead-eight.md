# Retire the dead eight from the manifest (#2845)

Status: proposed, 2026-08-22. Phase-0 item 6 of the R5b queue, after #2843 (`ceffc769`).
Revised after Codex ckpt-1, which corrected a factual claim and found four real gaps.

## Source rule

Not a market-data or SEC decision. The governing rule is the operator's cut-and-reset
(2026-08-22) plus #2827's measurement, read off the issue rather than recalled:

| class | strategies | what #2827 measured |
| --- | --- | --- |
| gross-negative at zero cost | s1, s6, s7, s9 | negative gross expectancy per trade, PF < 1 |
| gross-negative, and **not a cost problem** | s2, s10 | same, plus cost accounts for under a fifth of the loss — the band sensitivity is far narrower than the loss itself |
| gross-positive but short of the deflation bar | s3, s5 | positive gross expectancy, gross trade Sharpe an order below the bar; s5 break-even at a realistic band, s3's edge 8× wider between ambiguity arms than the others' |
| **KEPT** | s4, s8 | the only two clearing a plausible cost band, barely; substrate of #2840 |

⚠ **No figure from that table is copied into the code.** Reason strings name the verdict
class and the ticket; the numbers live in #2827 where they are reproducible. Hardcoding a
derived statistic into a comment is the thing that goes stale in the place a reader trusts
most.

## Premise check — "the pattern already exists"

It does, one layer up from where the issue implies. `runnable_strategies`
(`app/services/backtest_run.py:702`) already returns
`tuple[runnable, tuple[ExcludedStrategy, ...]]` and `/strategies` already renders
`exclusion_reason` from it (`app/api/strategies.py:2030,2261`). Its exclusions are
**derived from capability**; retirement is a **policy** axis with no representation today.

## Measured, not assumed

```
PYTHONPATH=. uv run python -c "... _regime_for(entry, _PROBE_CALENDAR) ..."
```

| | |
| --- | --- |
| `level_based` | s4, s5, s6, s7, s8, s9 — **six**, not one |
| all six declare `exit_levels` | so `excluded` is empty today, matching `assert excluded == ()` |

⚠ A first draft of this spec said *"s4 is the only `level_based` entry"*. That was wrong,
caught at ckpt-1, and it mattered: it was the load-bearing premise of a "skip the probe for
retired entries" design that is now abandoned (below).

## Full-population state (dev, 2026-08-22)

```sql
select strategy_id, count(*) from strategy_results_store group by 1;   -- 568 rows over all 10
select strategy_id, count(*) from strategy_signals     group by 1;
```

All ten carry stored results, so retirement must not make any unresolvable. Durable signal
rows for the retired eight total **55,309** against **1,526** for s4+s8 (s2 and s10 hold
none — cross-sectional, they publish calendars). That is the recurring cost retirement
stops.

⚠ Mid-flight rule checked: no `running` row in `job_runs`, and no `strategy_backtest_runs`
relation exists — nothing is in flight.

## Design

### 1. Retirement is a manifest FIELD, not a removal

`StrategyEntry.retired_reason: str | None = None`, refused when **blank after `.strip()`**
— `""` and `" "` are the same defect and would render a blank exclusion in the UI.

⚠⚠ **The entries STAY in `STRATEGY_MANIFEST`.** Deleting them would take
`registered_strategy_purpose` to `None` (so `promote_strategy` refuses),
`current_result_versions()` would stop resolving their stored rows, and `/strategies` would
lose them — the "vanished" outcome the acceptance forbids, stranding 568 immutable result
rows at versions the code no longer produces. Membership is what makes stored evidence
readable; only *runnability* changes.

⚠ `retired_reason` must never reach the identity hash, or retirement would rotate every
version and falsify the whole "stored rows stay resolvable" claim. It cannot today —
identity comes from the `entry.identity(...)` factory, not from the dataclass — but that is
a property worth an assertion rather than a reading, so a test pins each strategy's current
version across the change.

### 2. `runnable_strategies` — validate EVERY entry, then exclude on policy

⚠⚠ **The capability probe runs for retired entries too.** The first draft checked
retirement first and skipped it. That is wrong: the probe asserts more than exclusion
honesty — for a non-level entry it refuses a stray `exit_levels`, and for a level entry it
refuses a builder that has stopped refusing. Short-circuiting on retirement would make
retirement **a way to conceal a malformed declaration**, and eight of ten entries would
stop being schema-checked at once. The probe runs on a tiny synthetic `_PROBE_CALENDAR`,
so there is no cost argument for skipping it either.

Order: derive the capability verdict for every entry → then classify. A capability
exclusion still wins over a retirement one for the *reason* text, because "this cannot
produce a result at all" is the stronger statement.

`ExcludedStrategy` gains `kind: Literal["capability", "retired"]`, so a consumer can tell a
policy exclusion from a builder refusal without parsing prose. Its docstring says the
reason *"is the message `build_positions` RAISED, not a paraphrase"* — amended rather than
quietly falsified: that stays true of `kind="capability"`.

### 3. The signal scan skips them, by name

New `StrategyScanStatus` member `refused_retired`, emitted from `run_signal_scan`'s
per-strategy loop; retired entries filtered from `_publish_decision_calendars`; the job
note reports the retired count so a 2-of-10 population is visible rather than inferred.

⚠ A skip must be a RECORDED result, not an absent one — §9's observability contract. A
scan reporting "2 strategies evaluated" with eight simply missing is the silent narrowing
criterion 9 forbids.

⚠ Not gated on anything corpus-shaped. #2811's lesson was that gating decision-calendar
publication on "has bars to write" made never-measured zeros read as measured; retirement
is a declared property of the entry, not a fact about this run.

### 4. Scan freshness must say `retired`, not `stale`

`assess_scan_freshness` gains `retired_ids` and a `retired` status, **outside**
`_ALERTING_STATUSES`.

⚠⚠ **Without this the change ships a permanent false alarm.** The eight stop scanning, so
their watermarks freeze, so every `/system/status` poll reports eight `stale` strategies
for ever. That is precisely the prevention-log defect about an alarm with a documented
"ignore this" attached — strictly worse than no alarm, because it still costs attention and
occupies the slot a working detector would have. Found by Codex ckpt-1.

### 5. A retired strategy cannot advance a lifecycle stage

`promote_strategy` refuses a retired entry into `_EXTERNAL_EVIDENCE_STAGES`, one condition
beside the two existing `purpose` checks it already makes there.

⚠ Scoped to the evidence stages deliberately: `paused` and `retired` must stay reachable
for a retired strategy, or the eight become unmanageable. This is the chokepoint every
promotion path passes, which closes the hole Codex named in `strategy_paper_runtime` (it
selects `capital_candidate` entries directly rather than through `runnable_strategies`) and
in #2843's autonomous approver.

⚠⚠ **Placed FIRST, ahead of the two purpose checks, and that ordering is load-bearing.** A
draft put it after them, and writing the test exposed why that is wrong: every manifest
entry is `harness_validation` today, so the purpose check always wins and the new guard
would be UNREACHABLE. An unreachable guard cannot be proven to work, which is how a guard
rots — and I had written a test *documenting* the unreachability, which is the tell. It is
also the strongest statement available about a strategy: "this is dead" says more than
"this is not a capital candidate".

### 6. Outcome resolution keeps processing retired strategies — deliberately

`run_outcome_resolution` iterates entries with `exit_levels`, which includes retired
s5/s6/s7/s9. **It is not filtered, and that is the decision, not an oversight.** Those
strategies have already-fired signals whose outcomes are unresolved; filtering would strand
them permanently and corrupt the evidence record that retirement exists to preserve.
Retirement stops NEW evidence, never the drain of old. Stated here and asserted by a test,
because an unexamined omission and a deliberate one look identical in a diff.

### 7. Nothing is deleted, and no migration

`strategy_results_store`, the ledgers, the trial register, declarations and the registry
modules are untouched.

## Tests

- `runnable_strategies()` returns exactly `("s4-...", "s8-...")`; the other eight are in
  `excluded` with `kind="retired"` and a non-empty reason naming #2827.
- The existing `runnable | excluded == STRATEGY_MANIFEST` test keeps passing, plus
  **disjointness** — membership alone does not prove exactly-one classification.
- **At least one strategy is runnable.** A general invariant, not this ticket's set: a
  manifest that retires everything is a system that cannot produce evidence at all.
- All ten remain in `STRATEGY_MANIFEST` and `current_result_versions()`, and **each
  strategy's current version is unchanged by the field**, pinning §1's identity claim.
- A retired entry with a malformed capability declaration STILL raises — the assertion that
  retirement did not become a concealment channel (§2).
- `StrategyEntry` refuses a blank or whitespace-only `retired_reason`.
- `assess_scan_freshness` returns `retired` (and `is_alerting is False`) for a retired
  strategy whose watermark is old enough to be `stale`, i.e. the arm that would have
  alarmed.
- `promote_strategy` refuses a retired strategy into an evidence stage and still permits
  `paused`.
- Outcome resolution still selects a retired strategy's entry (§6).
- The scan's two call sites share ONE retirement predicate (`retired_scan_ids`), asserted
  from the module source — written twice they drift, and silently in the worse direction: a
  strategy skipped by the scan but still publishing a calendar looks healthy on the card
  while producing nothing.

⚠ **No new DB test for `run_signal_scan`, and the reason is stated rather than the gap left
open.** No `run_signal_scan` DB harness exists in the tree — every current test of that
module is pure — so covering a `continue` would mean building a universe + bars + watermark
fixture from scratch. Instead the decision was EXTRACTED (`retired_scan_ids`) and
table-tested, which is this repo's stated preference, and the consequential properties
follow structurally: the retired branch `continue`s before `plans.append`, so no `_Plan`
exists, so no writer is reached. A reviewer who wants the integration test anyway should
say so — this is a judgement about proportion, not an oversight.

## Out of scope (named, not forgotten)

- **A structured retirement date/source beyond `kind` + reason.** `kind` covers the
  machine-readable need; a date field with no consumer is a declared-but-unwired symbol.
- **A UI "retired at" cutoff** distinguishing retained evidence from live production.
  Raised by ckpt-1; no consumer asks for it, and the card already shows the reason.
- **`strategy_monitoring._panel_floor`** — a defensive per-id lookup that returns 1 for an
  unknown strategy. Retirement does not change what it returns.
- **Un-retiring.** A later ticket clears the field; there is no workflow because there is
  no candidate.
