# Strategy holding period — the "expected turnaround" number

Issue: #2623 gap 1. Gap 2 shipped as `df1fa062`; gap 3 (catalog view) renders both.

Rung: corpus-adjacent — a new derived statistic on the backtest metric set, a metric-set
version bump, and three nullable result columns. ⚠ **Populate forward only.** No
backfill, no re-run.

## 1. The premise, verified — and the sizing risk it carried is gone

The ticket says *"the backtest resolves every trade and discards the duration"*. True at
the metric layer: `strategy_statistics.TradeReturns` carries `net_return_pct` and
`entry_fill_date` as positionally parallel tuples under a length invariant, and **no exit
axis**. So `compute_metrics` cannot produce a holding statistic however many columns are
added downstream — adding the result columns first would be an R4 orphan.

The 2026-08-13 triage left one thing unmeasured, and it was the one that decided the
size: *"whether the exit bar is in scope at the producer, or has already been dropped
further upstream."*

**Measured: both producers hold it, at the append site itself.**

- `app/services/backtest_run.py:1174` appends `position.entry_fill_bar_date`. The same
  `position` object's `close_bar_date` is read **three lines above** at `:1105`, and
  `exit_index` is computed at `:1164` and handed to `book.add_leg`.
- `app/services/synthetic_control_run.py:576` appends `axis[entry_index]` with
  `exit_index` already in scope on the line above.

## 2. The surface this actually touches

⚠ A first draft of this spec called it "four steps and no more". That was wrong — it
counted the data path and forgot the ledger round-trip and the view. The real list:

| # | file | change |
| --- | --- | --- |
| 1 | `strategy_statistics.TradeReturns` | third parallel tuple `exit_bar_date` + invariants |
| 2 | `strategy_statistics.compute_metrics` / `StrategyMetrics` | derive + carry three fields |
| 3 | `strategy_statistics.METRIC_SET_ID` | `criterion7-v1` → `criterion7-v2` (§4) |
| 4 | `backtest_run.py:1174` | thread `position.close_bar_date` |
| 5 | `synthetic_control_run.py:_place_member` | accumulate + **return** exit dates; its signature and caller change |
| 6 | `sql/347` | three nullable columns, view recreate, CHECKs |
| 7 | `result_ledger.py:410/436/591` | writer: column list, placeholders, params |
| 8 | `result_ledger.py:741/822` | **reader**: select list and `StrategyMetrics(...)` reconstruction |

⚠ (8) is the one a "just add a column" reading drops. Writing a column without adding it
to the read path leaves the value stored and unreadable, which looks like a writer bug
from every consumer.

## 3. Source rule

No published formulation exists for a strategy's "expected turnaround" — it is an
operator-facing descriptive statistic, not a literature quantity. Fixed by construction,
each choice anchored to something already in this repo.

### 3.1 Unit: calendar days

`exit_bar_date - entry_bar_date`. The operator asked for *"expected turn around times"* —
how long capital is tied up, which is wall clock.

⚠ **Reconciling `bars_held`, which is the competing documented unit.**
`strategy_outcomes.bars_held` stores duration in BARS. It is not dismissed as "a
different statistic" — it is a different *population* (the live signal-outcome path, per
fired signal) measured on a different axis, and the reason to diverge from it is that a
bar count is not a turnaround an operator can plan against: 5 bars is a week or a
fortnight depending on halts and holidays. The live attribution path already made this
same call — `strategy_monitoring._ATTRIBUTION_SQL`'s `median_days_to_outcome` is
`percentile_cont(0.5) WITHIN GROUP (ORDER BY (o.exit_bar_date - s.fill_bar_date))`,
Postgres date subtraction, so calendar days.

⚠ That precedent fixes the UNIT and nothing more. It is **not** the same statistic: it
measures resolved signal outcomes across funded *and* rejected entries, filtered on
`gross_return_pct IS NOT NULL`, whereas this measures costed, realised backtest positions
on net returns with different exclusions. Same unit, different population — the catalog
must not label them as one number.

### 3.2 Percentile method: linear interpolation

`np.percentile(..., method="linear")` — numpy's default, and the same definition as
Postgres `percentile_cont`. That equality is the reason to state it: §3.1's live
statistic uses `percentile_cont`, and a different method here would make two adjacent
catalog figures disagree on identical data. `strategy_statistics` has no existing
percentile to inherit from — checked — so this is chosen rather than assumed, and a test
pins the two engines against each other on even and odd sizes and on duplicates.

### 3.3 Population: realised trades only, and it is right-censored

`TradeReturns` is **realised-only by construction** (§3.4 of the bounded-backtester
spec): an open position, an `ambiguous` close and an `unresolved` outcome are each
*"excluded, counted"* from the trade-level metrics while staying on the equity curve.

⚠⚠ **The excluded set is right-censored, and the direction of the resulting bias is NOT
determinable a priori.** A first draft asserted the median is biased *downward* because
"the still-open positions are precisely the long-held ones". That is unfounded: a
position opened shortly before the window end is still open and has a *short* elapsed
duration. Censoring here is informative — whether a trade is still open correlates with
how long it runs — but informative censoring does not fix a sign. The honest statement is
that the reported median is over closed trades only and the censored set could pull it
either way.

So it is **labelled, not corrected**. Both exclusion counters already exist on
`strategy_results_store` and both must render beside the median in gap 3:
`open_trade_count` (still open at window end) and `unpriced_trade_count` (closed but
carrying no usable price). ⚠ Showing only `open_trade_count` misstates the gap — they are
separate exclusions and neither implies the other.

## 4. `METRIC_SET_ID` moves to `criterion7-v2`, and that is what makes the nulls readable

`METRIC_SET_ID` is stored per result row. Its documented job is to identify **which
definitions** a stored metric set holds, so a row carrying three metrics that
`criterion7-v1` never defined cannot truthfully keep that stamp. This is #2670's lesson
exactly: *a version denotes a rule set, not a row population.* A first draft argued the
bump was unnecessary because no existing metric changes; that confuses "no value moved"
with "the set is the same set".

**Verified safe:** nothing gates on the *value*. `grep` across `app/`, `sql/` and
`frontend/src` finds `metric_set_id` only written (`strategy_result.py:1088`,
`result_ledger.py:591`) and read back (`result_ledger.py:822`); its only constraint is
`CHECK (metric_set_id <> '')` (`sql/263:161`). No promotion rule, index or comparison
filters on it.

⚠⚠ **And the bump is not just correctness bookkeeping — it is the null-provenance
discriminator.** Without it, a null holding period on a future row is indistinguishable
from a legitimate legacy null, so a writer defect would be invisible. With it the rule is
exact and enforceable in the database:

```
CHECK (metric_set_id <> 'criterion7-v2'
       OR trade_count = 0
       OR median_hold_days IS NOT NULL)
```

- `criterion7-v1` row → three nulls, legitimate and permanent (§5).
- `criterion7-v2` row with realised trades → the triple is **required**.
- `criterion7-v2` row with `trade_count = 0` → nulls, legitimate.

⚠ Written with `<>` on a NOT NULL column, so the `NULL = 'x'` trap that bit #2603 item 2
does not apply here — `metric_set_id` is `SET NOT NULL` (`sql/263:105`). Stated because
the shape is the one that failed before.

Legacy reads are unaffected: the reader maps a null column to `None`, and
`StrategyMetrics` carries the three as `float | None`.

## 5. ⚠⚠ Populate forward only — this must not become a re-run

`strategy_results_store` holds **324 rows**, every one written without a duration.
Backfilling requires **re-running the backtests**, which mints results under current pins
— the trial-register-charging path #2599's declaration contract and #2616's pre-cutoff
rerun gate govern, and which an unattended run must not open.

So the columns ship nullable, future runs populate them, and all 324 existing rows read
`null` until someone runs a backtest. Gap 3 renders that as *"not measured for this
result version"*, naming `criterion7-v1` — which its own scope already requires, since
empty states must say **why**.

⚠ No claim is made here about what the hold distribution will look like. A first draft
argued from trade counts (min 433, median 6,005) that "the percentiles are not going to
be noise". Trade count does not establish temporal support — a large correlated
population can rest on few distinct dates — and the hold distribution is not stored,
which is the entire point of this ticket. Nothing to measure it on yet, so nothing is
asserted.

## 6. Invariants

On `TradeReturns.__post_init__`, beside the existing length check:

- all three axes the same length (extends the existing invariant to the third tuple);
- `exit_bar_date[i] >= entry_bar_date[i]` for every `i` — a same-day close is legal
  (hold 0), an exit *before* its entry is a producer bug and must not reach a statistic.

On `StrategyMetrics.__post_init__`:

- the triple is all-present or all-absent — a partial triple means a derivation half-ran;
- `p25 <= median <= p75`, and all three `>= 0`.

## 7. Verification

- Pure table tests: no realised trades → all three `None`; a known distribution → the
  three percentiles at their linear-interpolation values; **even and odd** population
  sizes; duplicate durations; a same-day hold reading `0`; a single trade giving
  `p25 == median == p75`; a weekend-spanning hold counting calendar days.
- The exit axis is **required** — constructing `TradeReturns` without it fails, mirroring
  the existing argument for `entry_fill_date`.
- Rejection tests for each §6 invariant, one per invariant so a failure names the rule.
- A parity test asserting `np.percentile(..., "linear")` equals Postgres `percentile_cont`
  on the same inputs (§3.2).
- Producer tests asserting each derived duration against its **originating** position /
  placement, not merely that the tuples are the same length — positional arrival cannot
  prove semantic correspondence.
- DB: a ledger **round-trip** (write then read reconstructs the three values), a
  view-mediated insert/select proving the recreated view exposes the columns, and both
  CHECK directions from §4.
- ⚠ **No full-population A/B arm is run and none is claimed.** The change adds a statistic
  and alters no existing one; the only stored field that changes value is
  `metric_set_id`, deliberately (§4). Stated rather than left as an unrun gate. A re-run
  to populate is out of scope per §5.

## 8. Migration notes (`sql/347`)

Follows `sql/335`'s sequence, which records the traps:

- `SET LOCAL lock_timeout = '5s'` — a pending `AccessExclusiveLock` queues *ahead* of new
  readers, so an ALTER merely waiting behind the live `strategy_backtest_run` job blocks
  every subsequent SELECT on the relation.
- `strategy_results` is a **VIEW** over the store and `SELECT *` is expanded at creation,
  so it must be recreated or the new columns are invisible through it — and
  `ALTER VIEW ... SET (check_option = 'cascaded')` restored afterwards, because
  `CREATE OR REPLACE` drops it.
- The columns are **nullable with no default**, which is what makes this rolling-safe:
  `strategy_backtest_run` is a live job, so between the migration applying and the daemon
  picking up new code an old writer inserts without them — legal against a nullable
  column, and the §4 CHECK still holds because that writer stamps `criterion7-v1`.
