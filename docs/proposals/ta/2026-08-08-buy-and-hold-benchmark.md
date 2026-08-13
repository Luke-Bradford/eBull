# The buy-and-hold benchmark — composition, and why the shipped one is not buy-and-hold

Refs #2426. Blocks #2364 (baseline-comparator refusal), which gates phase 6.
Parent: `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` criterion 7.
Amends: `docs/proposals/ta/2026-08-07-bounded-backtester.md` §5.4.

---

## 1. What is wrong

`strategy_results_store.buy_and_hold_return_pct` reads **33,706,844.28%** on the
s1 in-sample row, and `return_vs_buy_and_hold_pct` inherits it.

The premise stated in #2426 — *per-instrument hold returns are being summed
across the evaluated universe* — is **FALSIFIED** (§3.2). There is no summation
anywhere on the path.

The cause is one line of construction. `backtest_run._benchmark_book` builds the
legs correctly — one per evaluated instrument, first usable bar to last — and
`_measure_namespace` then hands them to `equity_curve.build_equity_curve`, which
applies `SIZING_RULE_ID = "equal_weight_concurrent_v1"`. **That rule re-imposes
equal weight on every event date**, so the benchmark trades: measured over the
full axis it turns over **137,477,862×** the starting pot across **3,292** of
16,236 dates.

A portfolio that re-equalises ~5,000 names on 3,292 separate dates is not
buy-and-hold. It is a periodically-rebalanced equal-weighted index, and
compounding one of those over decades is a named, published, quantified error.

⚠ The engine re-use is not itself the mistake. `_benchmark_book`'s docstring is
right that running the benchmark through the same **cost model** and the same
**fill contract** is what stops the machinery's difference being attributed to
the strategy. What it did not notice is that the engine also carries a **sizing
rule**, and that a sizing rule which rebalances is the one thing a buy-and-hold
benchmark may not have.

---

## 2. Source rule

⚠ Read `.claude/skills/data-sources/market-structure.md` first. Trap 3
(*"equal-weighted per-bar means are micro-cap means"*) is the same mechanism seen
from the cross-section; that file gains a cross-reference to this one.

### 2.1 The governing rule

**Blume, M. E. and Stambaugh, R. F. (1983), "Biases in computed returns: An
application to the size effect", *Journal of Financial Economics* 12, 387-404.**

> *"The use of quoted closing prices in computing returns on individual stocks
> imparts an upward bias. Returns computed for buy-and-hold portfolios largely
> avoid the bias induced by closing prices."*

The mechanism is bid-ask bounce: a rebalance trades **into** the noise in each
closing print, buying whatever happened to print at the bid and selling whatever
printed at the ask. Blume & Stambaugh measure it at **0.056% per day on the
small-firm decile against 0.001% on the large-firm decile — fifty times as
great** — and the published size effect **halves** when recomputed buy-and-hold.
Our panel is 5,266 series of predominantly small and delisted US names: the
population where the bias is largest.

**So the rule is not a preference: a buy-and-hold benchmark is committed once and
never rebalanced.** It is what the words mean and it is what the literature
requires of the construction.

### 2.2 A corroborating measurement, and its limits

**Canina, L., Michaely, R., Thaler, R. and Womack, K. (1998), "Caveat
Compounder: A Warning about Using the Daily CRSP Equal-Weighted Index to Compute
Long-Run Excess Returns", *Journal of Finance* 53(1), 403-416** — compounding the
**daily**-rebalanced equal-weighted index over long windows biases the benchmark
by ~0.43%/month, ~6%/year, *"large enough to reverse the conclusions of papers
using the daily tape to compute benchmark portfolio returns."*

⚠ Cited for direction and for the fact that this exact use — a benchmark
compounded over a long window to decide a verdict — is the one the literature
warns about. It is **not** a magnitude for our engine: theirs rebalances daily,
ours on event dates. The governing magnitude here is our own full-population
measurement, **23.2 percentage points per year** (§3.1).

### 2.3 Both options in the issue text need correcting against the rule

- *"equal-weighted portfolio … rebalanced on the strategy's cadence"* is the
  **biased** construction. Matching the strategy's cadence matches its turnover,
  which is the property buy-and-hold is defined by not having. **Rejected.**
- *cap-weighted SPY* cannot be criterion 7's bar. #2398 loaded SPY from
  **1993-01-29**; the in-sample axis starts **1962-01-02**, so it covers none of
  the first 31 years, and its `instrument_id IS NULL` keeps it out of the
  validated universe by construction. Viable later as a **secondary** comparator
  on the hold-out namespace only. **Not adopted here.**

### 2.4 What no source rule fixes, and the by-construction answer

CRSP's published index methodology does not govern this, and it is worth saying
why rather than leaving it unmentioned: CRSP's equal-weighted index is a
**rebalanced** index (*"once delisted, an issue is given no weight in the
portfolio [and] the returns reflect the … average of the returns of the
remaining securities"*), i.e. it redistributes to survivors, which is the
construction §2.1 rules out. Its **delisting-return** rule likewise cannot be
imported: it prices a security after its last trade from distributions or an
off-exchange print, and our corpus carries no such field — `research_price_daily`
is `(open, high, low, close, volume, adj_close)` and nothing else.

So the unbalanced-panel handling is fixed **by construction** and frozen in a
version hash, per `.claude/CLAUDE.md`. Frozen as
`BENCHMARK_RULE_ID = "equal_weight_buy_and_hold_v1"`:

1. **N = the namespace's evaluated instrument set** — precisely, the instruments
   with at least one position routed into that namespace (`_NamespaceBook.
   instruments`, populated in `_absorb`). Unchanged from today, including the
   consequence that the benchmark is **strategy-dependent**: it is the bar for
   the names *this* row claims to have measured, which is what
   `_benchmark_book`'s docstring already argues and what
   `evaluated_instrument_count` already reports.
2. **Each leg is committed exactly `starting_equity / N` at its own entry index**
   and held to its own exit index.
3. **Proceeds go to cash and stay there; cash earns 0.** Not a new decision —
   §5.4 already states *"define cash return as zero, report return on the full
   allocated pot"*, quoting criterion 7. Applying the same denominator rule to
   the benchmark is what keeps the two comparable.
4. **No rebalance, ever.** The one substantive change.
5. **Prices and costs unchanged**: entry at the first usable **close** × `(1+h)`,
   exit at the last usable **close** × `(1−h)`, `h` from `cost_model.
   half_spread_for` keyed on the entry price (§5.1's documented band rule, *"the
   only function here that reads a price to choose a band"*). One round trip, the
   same as today. No rebalance trades means no rebalance costs.

**Cash is provably sufficient and never negative**: total commitment is exactly
`N × (starting_equity / N) = starting_equity`, entries only ever debit and exits
only ever credit, so no leg can be short-funded and no reserved-sleeve accounting
is needed. This is a stronger invariant than the strategy curve's cash cap, and
it is asserted rather than assumed.

⚠ **Named consequences, so they are not discovered later.**

- **Cash drag is real and is in the honest direction.** A name first listing in
  2015 leaves its `1/N` idle from 1962. That lowers the benchmark's *annualised*
  rate but **not** its total return, which is exactly `mean(exit/entry) − 1` over
  the legs (§7 acceptance 3 asserts this identity). Report exposure alongside it,
  as §5.4 requires of the strategy.
- **Entry and exit are CLOSE fills, where the strategy fills at the OPEN.** That
  is already true today and is not changed here. It is defensible for a
  benchmark — there is no signal and so no fill bar to be late for — but it means
  the two legs are not priced off the same print, and it must not be quietly
  read as a like-for-like execution comparison.
- **Halts keep their mark.** A leg whose series has no bar on a date carries its
  previous mark forward and the carry is counted, matching §3.3 and the strategy
  curve's own treatment. A stale mark is not a return.
- **`invested` is market value**, so benchmark exposure is ~100% once the last
  name has listed, and near 0% at the axis start. That is the truthful shape of
  this construction, not a defect.

### 2.5 Price basis — corrected by #2429

This document originally left the whole engine on raw `close`, because moving
only the benchmark would credit dividends to one arm and deny them to the
strategy. #2429 completes the engine-wide correction: raw OHLC still governs
signals, fills, spread bands and TP/SL, while strategy and benchmark wealth use
`adj_close`. Historical rows remain explicitly labelled price-return v1; new
rows carry a distinct total-return v2 identity. See
`2026-08-11-total-return-accounting-result.md` for the consumer matrix and A/B.

---

## 3. Full-population verification

`scripts/verify_2426_benchmark.py --compositions`. Whole validated universe,
whole axis, the **same legs** under three compositions. Nothing sampled.

### 3.1 The measurement

```text
axis            16,236 dates   1962-01-02 -> 2026-07-08   (64.51 years)
benchmark book  5,266 legs over 5,266 instruments

A. as shipped (equal_weight_concurrent_v1, rebalanced on event dates)
     total return     1,204,631,084.15%      CAGR 28.754%/yr
     event dates      3,292 of 16,236
     traded notional  137,477,861.98x the starting pot
     rebalance costs  354,752.8807 of a 1.0 pot

B. equal_weight_buy_and_hold_v1 (1/N at each leg's own entry, held to its exit)
     total return             3,223.24%      CAGR  5.581%/yr

C. #2426's premise (sum of per-instrument hold returns)
     Sigma return_pct    16,973,576.64%      = 0.0141x of A
```

**A − B = 23.2 percentage points of annual return, manufactured by rebalancing**
— on identical legs, identical prices and an identical cost model. That is the
governing magnitude for this engine.

### 3.2 Why the summing premise is falsified

The primary falsifier is **structural, not statistical**: there is no summation
on the path. `build_equity_curve` walks the axis carrying a single `cash` scalar
and a per-leg `units` vector, and returns `cash + Σ(units × mark)` — a composed
pot. No function between `_benchmark_book` and `buy_and_hold_return_pct` adds
per-instrument return percentages together.

The measurement agrees: **A is 71× larger than C on identical legs.** If the
mechanism were summation, A would *equal* C.

Two corroborations from the stored rows, which are about the stored figures
rather than the mechanism:

1. **They do not scale with instrument count.** Within the hold-out namespace, s1
   has **4,853** instruments and reads **3,761.66%**; s3 has **2,847** and reads
   **70.44%** — more instruments, 53× less return, where a sum is ∝N. ⚠ Not
   decisive on its own: the two rows also differ in axis length (7.76 vs 5.06
   years, recovered from each row's own `cagr_pct`) and in which names they
   selected, so this narrows the explanation rather than closing it.
2. **The in-sample benchmark CAGRs agree across three different universes** —
   **24.1 / 24.2 / 24.0**%/yr for s1 / s2 / s3 over universes of 3,541 / 2,978 /
   3,001, on 58.3-year axes recovered the same way. ⚠ Also not decisive alone: a
   summing bug over correlated universes and a common window could produce
   similar figures. It is consistent with composition and is reported as such.

### 3.3 It is not the #2400 adjustment artefact either

Leg census over all 5,266 legs: median gross multiple **1.0374**, max
**11,334.20×** over 11,287 bars — about 11%/yr, an ordinary 45-year hold. The top
ten legs are 27.41% of `Σ(gross)`. No back-adjusted `3e17` monster is carrying
the figure, so #2400's level distortion is not implicated.

---

## 4. The second defect: the benchmark is not on the result identity

`ResultIdentity.version` hashes `sizing_rule` deliberately — §5.4: naming it *"is
what stops a later sizing change reading as a performance improvement."* It
hashes nothing describing the benchmark, because until now the benchmark had no
rule of its own; it silently inherited `sizing_rule`. **A comparator that can
change without the identity moving is a comparator that can be tuned invisibly.**

`benchmark_rule` therefore becomes a hashed member of `ResultIdentity` and a
stored column, and the `result_version` bump #2426 requires then happens **by
construction** rather than by forcing.

### 4.1 ⚠⚠ This is not free: readback re-hashes and refuses

`result_ledger._result_from_row` rebuilds `ResultIdentity` from the stored
columns and **raises** when the re-derived hash differs from the stored
`result_version` — *"a result whose version does not describe it is exactly the
'different strategy inherits a track record' failure criterion 11 exists to
prevent."* Adding a hashed field with no further action makes **all 24 existing
rows unreadable**.

Three ways out, and the choice:

| option | verdict |
| --- | --- |
| Store `benchmark_rule` as a column but do **not** hash it | Rejected — the version would not move, the re-run would collide with the 24 rows, and the invisible-tuning hole stays open. It is the whole point of §4. |
| Keep both payload shapes and try the legacy 14-field hash on mismatch | Rejected — it makes "which payload does this row hash?" a permanent property of a row, which is the identity ambiguity criterion 11 exists to prevent, in slow motion. |
| **Re-derive the 24 stored versions under the completed payload** | **Adopted.** |

The adopted option is a **correction, not a rewrite of history**: the benchmark
rule was always an input to those numbers, and `equal_weight_concurrent_v1` is
truthfully the value that produced them. Their stored hash was computed from an
incomplete payload; re-deriving it makes the row *more* accurate about itself,
and it preserves the only property that matters — two runs differing in the
benchmark get different versions.

**Migration order is constrained by a trigger.**
`trg_strategy_results_holdout_access` fires `BEFORE INSERT OR UPDATE` and demands
a `strategy_holdout_accesses` row matching `(strategy_id, strategy_version,
result_version)` for every `hold_out` row. So: update `strategy_holdout_accesses`
**first**, then `strategy_results_store`, in one transaction.

The 24 replacement hashes are computed in Python and emitted as literal
per-`result_id` `UPDATE`s, with the generating command recorded in the migration
header — a SQL-side re-implementation of `json.dumps(sort_keys=True,
separators=(",", ":"))` plus SHA-256 would have to byte-match Python's, and a
silent divergence there produces exactly the unreadable rows this section exists
to avoid.

---

## 5. What changes

| # | Change | File |
| --- | --- | --- |
| 1 | `BENCHMARK_RULE_ID` + `build_buy_and_hold_curve`: commit `1/N` at each leg's entry, carry marks (stale marks counted), release to cash at exit, never rebalance. Returns the same `EquityCurve` shape with `rebalance_costs = 0.0` and `event_dates = 0` | `app/services/equity_curve.py` |
| 2 | `_measure_namespace` builds the benchmark with the new function; the **strategy** curve is untouched | `app/services/backtest_run.py` |
| 3 | `benchmark_rule` on `ResultIdentity` (hashed) and `StrategyResult`; `compute_metrics`' docstring corrected — it currently states the benchmark runs under the same engine, which will no longer be true of the sizing rule | `app/services/strategy_result.py`, `app/services/strategy_statistics.py` |
| 4 | Migration: add nullable → backfill 24 rows to `equal_weight_concurrent_v1` → non-empty CHECK → `SET NOT NULL`; update `strategy_holdout_accesses` then `strategy_results_store` result versions in one transaction; **recreate the `strategy_results` view** (`SELECT *` is expanded at creation) | `sql/2xx_benchmark_rule.sql` |
| 5 | Insert + positional read of the new column | `app/services/result_ledger.py` |
| 6 | Full-population A/B harness (the §3 table is its output) | `scripts/verify_2426_benchmark.py` |
| 7 | Identity tests enumerate hashed members member-by-member and must gain `benchmark_rule`; new pure tests for the no-rebalance curve | `tests/test_strategy_result.py`, `tests/test_equity_curve.py` |

**Not in scope — filed, not folded in:**

- **The strategy sleeve carries the same bias where its turnover is high.** s1
  runs `turnover_annualised = 72.00`/yr over ~3,500 concurrent names and ends at
  `total_return_pct = −100.00` — the mirror image, where the benchmark got the
  bias and the strategy paid the spread on it. Changing that is a change to a
  **declared, hashed strategy sizing rule**, i.e. a strategy-identity change.
- **`close` vs `adj_close`** (§2.5) — engine-wide, moves every stored figure.
- The 24 existing rows are **kept**, not deleted. They are a truthful record of
  what `equal_weight_concurrent_v1` produced, and deleting them is irreversible.

---

## 6. The other two figures #2426 asked to settle

**`s2.total_return_pct = 2,617,494.16%` — NOT on the same path.** s2 runs
`turnover_annualised = 4.21`/yr in-sample against s1's 72.00, so its curve is not
churning, and the rebalancing bias needs turnover. The figure is 58.3 years
compounding at the row's own `cagr_pct = 19.055`, with `sharpe = 0.850` at
`annualised_volatility_pct = 23.76` — internally consistent. It is large because
the window is 58 years, not because the composition is wrong. Whether 19%/yr
survives is criteria 6 and 7's job, and it already does not:
`deflated_sharpe = 0.0064`. **No change.**

**`max_drawdown_pct` of −100.00 (s1) / −99.592 (s3) — already correct.**
`strategy_statistics.max_drawdown_pct` runs `np.maximum.accumulate` over the
**portfolio equity path** and returns `min(equity/peak − 1)`. That is
portfolio-level and path-dependent, exactly what criterion 7 requires (*"a
per-trade figure does not compose"*), and it never touches the trade list. Those
are real ruins on those sleeves, not an aggregation artefact. **No change.**

⚠ Both were flagged in #2426 as *"unchecked and likely on the same aggregation
path"*. They are not. The aggregation path was only ever wrong for the benchmark.

---

## 7. Acceptance

1. **The benchmark does not rebalance**: on the full population,
   `rebalance_costs == 0.0` and `Σ traded_notional` equals
   `Σ(entry allocations) + Σ(exit proceeds)` exactly — entries and exits are
   trades by definition,
   rebalances are what must be absent. ⚠ `event_dates` stays **truthful** (dates
   on which a leg opened or closed) rather than being zeroed: it is criterion 8's
   concurrency-change count, whose meaning does not depend on rebalancing.
2. **Cash never goes negative and never exceeds the pot before proceeds**, on
   every date, asserted over the full population.
3. **The composition identity holds exactly**: benchmark
   `total_return_pct == mean(exit_price / entry_price) − 1` over the legs, to
   floating tolerance. With no rebalancing this is an algebraic identity, not a
   plausibility check — it is the assertion that no path-dependence leaked in.
4. **Re-running `strategy_backtest_run` writes new `result_version`s with no
   forcing**, because `benchmark_rule` moved the hash.
5. **All 24 pre-existing rows still read back** through `result_ledger` after the
   migration, carrying `benchmark_rule = 'equal_weight_concurrent_v1'` and their
   re-derived versions; the hold-out access rows still satisfy the trigger.
6. **The view has column parity** with the store after the migration.
7. Every figure quoted in the PR is computed by the harness at run time, never
   hand-copied (`.claude/CLAUDE.md`: never hardcode a derived statistic).
