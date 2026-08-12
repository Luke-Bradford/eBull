# Phase 5 — the bounded backtester

Refs #2240. Refs #2288. Refs #2284. Refs #2277. Closes the design half of §5.

Parent spec: `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`
(execution semantics §3.5, validated universe §4.0, **acceptance criteria §5**,
allocation §7, sequencing §8).
Milestone table: `docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md` §5.

Phase 5's three gates are met: phase 2 ✅ (`indicator_series`), phase 3 ✅
(`5078c173`), and #2260 closed 2026-08-05 — ⚠ but it closed by **falsification**,
which amends the parent's harness acceptance rather than satisfying it. See §9.1.

**Every figure here was computed against the dev corpus on 2026-08-07, and each
appears beside the query that produced it (§7).** None is carried from an
earlier section of the parent spec; two of them contradict it, which is what
§5.1 and §5.2 are about.

---

## 1. Scope

Phase 5 turns **stored** signals and outcomes into performance statistics
satisfying §5's eleven criteria, with the window and eligible universe stated on
every number.

It is **not** a signal generator (phase 3, shipped), an outcome resolver
(phase 4, shipped), allocation or paper trading (phase 7), or a UI (phase 6).

The deliverable is a *result model*, the statistics computed into it, and the
gate that stops an unvalidated result being quoted as a validated one.

---

## 2. The load-bearing architectural decision

> **The backtester reads the ledger. It never recomputes a signal, and it never
> resolves a fill.**

§3.5 rule 1 — *signal on the close of bar t, fill at the open of t+1* — is
enforced **structurally** in two independent places: `StrategySignal` carries a
bar index and no fill field, so a same-bar fill cannot be expressed; and
`sql/255`'s `strategy_signals_fill_after_signal` backstops it. A backtester that
re-derived entries from a price series steps outside both, and nothing
downstream can tell a t+1 fill from a t fill by inspection.

**Measured this run** against `vectorbt==1.1.0`, the library §8 names:

| call | order timestamp | order price |
| --- | --- | --- |
| `Portfolio.from_signals(close, entries, exits)` — the documented default | 2024-01-02 | **101.0 = `close` of the signal bar** |
| entries/exits shifted +1, `price=open_` | 2024-01-03 | 92.0 = `open` of the next bar |

The signal sat on bar index 1 (2024-01-02); the default filled it at that bar's
own close. **Adopting any simulator on its defaults imports the exact look-ahead
§3.5's opening paragraph exists to prevent** — *"the most common backtest error
there is"*.

### 2.1 The contract, and the assertion that holds it

The simulator is handed events indexed on the **fill bar**, priced at the
**stored `fill_price`**. It never sees `signal_bar_date`.

- Opens come from `strategy_signals` where
  `verdict = 'fired' AND signal_kind = 'entry'`, at `(fill_bar_date, fill_price)`.
- Closes come from §3, also always a pre-resolved `(date, price)` pair.
- No `close` series is ever passed as a fill-price source. Where the simulator
  needs a close series for mark-to-market it gets one, but `price=` is always
  the explicit resolved array.

**Asserted over the whole generated trade list, not a fixture** — and the
assertion is an equality, not an inequality:

1. every order timestamp **equals** the `fill_bar_date` of the ledger row that
   produced it;
2. every order price **equals** that row's `fill_price`;
3. `fill_bar_date > signal_bar_date` for every row consumed.

⚠ An earlier draft asserted only *"no order timestamp equals a
`signal_bar_date`"*. That is too weak — it passes for a simulator filling on the
wrong future bar, which is a different error with the same sign.

---

## 3. Three exit regimes, and the pyramiding rule

`strategy_outcomes` is **not** "the exits table"; building phase 5 as though it
were would drop most of the catalogue's position history. Read from the shipped
modules, not from the parent's prose:

| strategy | exit mechanism | `max_hold_bars` | exit leg? |
| --- | --- | --- | --- |
| **S-1** | signal-pair only — `close < sma_50` (`s1_time_series_momentum.py:203-206`, `exit_`) | **none** | yes |
| **S-2** | calendar — hold until next rebalance | absent by design | no |
| **S-3** | signal-pair — `rsi_14 > 50` (`s3_mean_reversion_in_trend.py:250-253`, `exit_`) — **plus** max-hold expiry | `10` | yes |
| **S-4** | level-based — stop at `entry − 2×ATR`, target, max-hold; resolved by `outcome_resolver` | `40` | **no** |

⚠ **Only S-4 is level-based.** A draft of this table described S-3 as TP/SL and
was wrong: S-3 has no stop and no target, and inventing one would have specified
a different strategy from the one that shipped. Caught at Codex checkpoint 1.

⚠⚠ **S-3's `MAX_HOLD_BARS = 10` is currently enforceable by nothing, and phase 5
must own it.** `ExitLevels` (`outcome_resolver.py:118-131`) requires
`take_profit` **and** `stop_loss` and asserts `stop_loss < take_profit`; S-3
declares neither, so the object cannot be constructed for it. S-3's own
docstring (`s3_mean_reversion_in_trend.py:114`) nonetheless states *"Its consumer
is `outcome_resolver.ExitLevels.max_hold_bars`"*. The parameter is hashed into
S-3's identity and applied by no code path. **Consequence for §3.2: max-hold
expiry is a close source owned by position construction, not exclusively by the
resolver.** The stale docstring is filed separately (§8, incidental finding).

### 3.1 ⚠ Entries are STATES, not crossovers — so pyramiding must be ruled out

S-1's entry is `close > sma_200 AND sma_50 > sma_200` (`FAST_PERIOD = 50`,
`SLOW_PERIOD = 200`). S-3's is `rsi_14 < 30 AND close > sma_200`
(`OVERSOLD_THRESHOLD = 30.0`, `TREND_PERIOD = 200`). **Neither is
edge-triggered.** Both fire on
*every bar* the condition holds, so a naive "one position per fired entry" opens
a new position on every day of a sustained uptrend and multiplies every
statistic by the length of the run.

The ledger is *correct* to record them all — §7 requires that *"every fired
signal is recorded whether or not it was acted on"*, because recording only
taken trades biases the record toward periods of spare capacity. **The collapse
therefore belongs to position construction, and is specified here:**

> **An entry signal for an instrument with an open position in the same strategy
> version is recorded as `superseded_open_position` and opens nothing.** One
> position per instrument per strategy version at a time. No pyramiding, no
> averaging down.

This is the long-only/no-leverage posture applied consistently, and the
suppressed count is reported per criterion 9's *"measure what you reject"* — a
narrowing that is not counted is a narrowing asserted safe.

### 3.2 Position construction

One position per `(strategy_version, instrument_id, entry fill bar)` that
survives §3.1, closed at the **earliest** of these four sources. They are
evaluated together and the earliest date wins; they are not alternatives
selected by strategy.

| # | source | applies to | close date | close price |
| --- | --- | --- | --- | --- |
| C1 | **signal-pair** — the next `signal_kind = 'exit'` fill for that strategy version and instrument, **strictly after** the entry fill bar | S-1, S-3 | `fill_bar_date` | `fill_price` |
| C2 | **level-based** — the matching `strategy_outcomes` row with outcome `tp_hit` / `sl_hit` / `expired` | S-4 | `exit_bar_date` | `exit_price` |
| C3 | **max-hold expiry** — `entry fill index + max_hold_bars`, applied by position construction | S-3 (10), S-4 (40) | that bar | its **open**, per §3.5 rule 1 |
| C4 | **calendar** — the next rebalance at which the name is **not** reselected | S-2 | that rebalance's fill bar | its open |

⚠ **C3 exists because of the S-3 gap above.** For S-4 it is redundant with C2's
`expired` and must agree with it; a disagreement is a failure, not a
tie-break — it means the resolver and position construction disagree about the
window, and `outcome_resolver.py:470` (`exit_index = fill_index + max_hold_bars`)
is the reference.

Five rules that close the remaining gaps:

1. **Version pinning is mandatory.** The `strategy_outcomes` join pins **both**
   `rule_set_version` **and** `input_rule_set_version`. `sql/256` makes the pair
   the uniqueness key precisely so two resolver versions cannot be pooled; an
   unpinned join double-counts every signal once per resolver version present.
2. **`ambiguous` is a terminal close with NO price** — a fifth close source,
   listed apart from C2 because it breaks C2's `(date, price)` shape. `sql/256`'s
   `strategy_outcomes_location_matches_outcome` gives `ambiguous` an
   `exit_bar_date` and its `strategy_outcomes_booked_matches_outcome`
   deliberately withholds `exit_price` — *the bar is known, only the touch order
   is not*. So the position **closes on that bar with an unknown return**, and
   §3.4 governs what each statistic does with it. An earlier draft folded it
   into C2, which is a contradiction: C2 reads `exit_price`, and for `ambiguous`
   that column is null by constraint.
   `unresolved` is the opposite case — no location at all — and leaves the
   position open, handled by rule 5.
4. **Same-bar ordering is fixed:** exit before entry. An exit row whose fill bar
   equals a new entry's fill bar closes the *older* position; it never closes the
   position opened that bar. This is why the signal-pair clause says *strictly
   after*. §3.5 rule 1's own justification for keying on `signal_kind` is that
   *"a strategy exiting one position and entering another on the same bar for
   the same instrument is legitimate"* — so the order has to be stated, not left
   to sort stability.
5. **Open at window end** — a position with no close inside the evaluation
   window is neither a win nor a loss. It is reported with its count and an
   unrealised mark taken at **the last usable close of the evaluation window for
   that instrument, minus one side of the cost model** (the exit that has not
   happened). It is never dropped: dropping it biases toward positions that
   closed, and positions close faster in trending regimes. Positions left open
   by an `unresolved` outcome are counted separately from those open because the
   window ended, since the two say different things.

### 3.3 S-2's calendar regime, in full

The rebalance calendar is the **panel's**, fixed by `8bd51c0e`. So:

- a name selected in consecutive rebalances is **one hold, not two entries** —
  the second rebalance's entry signal hits §3.1 and opens nothing, and **C4 does
  not fire because the name WAS reselected**. This is why C4 is worded as *"the
  next rebalance at which the name is not reselected"* rather than *"the next
  rebalance"*: the latter would close and reopen the position every month,
  charging two sides of the cost model for a hold that never ended;
- a name **dropped** at a rebalance closes at that rebalance's fill even though
  no entry signal exists for it — this is why the close clause cannot be
  "the entry that supersedes it";
- a name dropped and later reselected is **two positions**, correctly;
- a name **halted** across a rebalance has no fill bar at that date; the position
  stays open to the next date on which its own series has a bar, and the
  divergence between the panel calendar and the instrument calendar is counted.

### 3.4 What ambiguity does to each statistic

Criterion 7 lists **twelve** metrics, and §3.5 rule 4 only says `ambiguous` is
*"excluded from the win rate with their count shown"*. That is under-specified
for the other eleven, so:

| outcome | win rate | expectancy / profit factor | equity curve, CAGR, drawdown | exposure |
| --- | --- | --- | --- | --- |
| `tp_hit` / `sl_hit` / `expired` | in | in | in | in |
| `ambiguous` | **excluded, counted** | **excluded, counted** | ⚠ see below — **no single treatment is honest** | **in** — capital was committed |
| `unresolved` | excluded, counted | excluded, counted | position stays open (§3.2 rule 5) | in |
| open at window end | excluded, counted | excluded, counted | unrealised mark only | in |

⚠ **The equity curve is where `ambiguous` has no good answer, and saying so is
the point.** The capital was committed and returned; the return is *unknown*,
not zero. Recording zero is a treatment — it silently asserts break-even, which
is favourable for a strategy whose ambiguous bars span a stop. Dropping the
position is also a treatment, and a worse one: it removes the capital commitment
too.

So the equity curve is computed **twice**, as a declared sensitivity pair:

- **worst-case arm** — every `ambiguous` position resolves at its stop
  (`sl_hit`);
- **best-case arm** — every one resolves at its target.

Both are reported. **If the two arms' Sharpe differ by more than the gap between
the strategy and the random cohort's 95th percentile (§9, *the harness
itself*), the result is
`ambiguity_material` and is not promotable.** ⚠ This is deliberately *not* the
"assume SL first for conservatism" rule, which §3.5 rule 4 and spike S5
explicitly reject — *"it is not conservative, it is a different bias"*. A
declared two-sided bound is not a point estimate dressed as caution.

⚠ **The ceiling is a gate, not a reassurance.** S5 measured the ambiguous class
at 0.83% of signals at a 1.0×ATR target down to 0.09% at 4.0×, which suggests
the arms will be close — **but that measurement entered at the close of bar `t`,
the same-bar fill §3.5 rule 1 forbids**, and §3.5 says its distribution *"is not
expected to reproduce"* under a t+1 open entry. So S5's figures do not bound
phase 5's, the ceiling is checked rather than assumed, and only S-4 can produce
an `ambiguous` outcome at all (§3: it is the only level-based strategy).

---

## 4. Adopting `vectorbt` — the evidence, and the residual

> ⚠⚠ **RESOLVED AT STAGE 5d, 2026-08-07: NOT ADOPTED.** §8 defers the decision to
> here, and here is the answer with the measurements that produced it. Two of
> them contradict the section below, which is kept as the design record.
>
> 1. **The metrics this section wanted REFUSE on our index — the killer.**
>    `sharpe_ratio()`, `sortino_ratio()`, `annualized_volatility()` and
>    `annualized_return()` all raise `ValueError: Index frequency is None` on a
>    `DatetimeIndex` of real trading dates. The only way through is declaring a
>    fixed `freq`, and `freq="1D"` imposes an annualisation factor of **exactly
>    365.0** — measured by dividing the library's Sharpe by the per-period one —
>    against an index carrying ~196 observations per calendar year, inflating
>    Sharpe by `sqrt(365/196) = 1.37x`. The one thing the library was to be
>    adopted FOR is the thing that does not work on this data.
> 2. **It wants a dense date x instrument panel; ours is 27.3% dense.** Measured
>    against the dev corpus 2026-08-07 (5,266 series, 23,339,583 bars, 16,236
>    trading dates → 85,498,776 cells, so 72.7% NaN padding).
>    `Portfolio.from_signals` at that exact shape completes in 5.9 s and peaks at
>    **4.05 GiB RSS**. Reproduce the density with
>    `PYTHONPATH=. uv run python scripts/verify_2240_statistics.py --panel`.
> 3. **Its default fill semantics still import the look-ahead §3.5 rule 1
>    forbids** — reproduced on the current install, a bar-1 signal fills at that
>    bar's own close (101.0).
> 4. ⚠ **"It pulls no numba" is FALSIFIED.** The resolution carries **58
>    packages** including `numba` 0.66.0 and `llvmlite`, plus `scikit-learn`,
>    `scipy`, `matplotlib`, `plotly`, `ipywidgets`, `requests` and `tqdm` —
>    against a repo running `pip-audit --strict` in CI. The packaging residual is
>    LARGER than this section recorded, not smaller.
>
> What ships instead is `app/services/equity_curve.py` +
> `app/services/strategy_statistics.py`, whose annualisation is derived from the
> evaluation window's own axis. Parent §8's *"Do not hand-roll"* is overridden on
> point 1 specifically, and the counter-argument is recorded in both modules'
> headers so the decision can be re-opened with evidence rather than re-argued.


§8 says *"Do not hand-roll"* and names `vectorbt 1.1.0` as verified on Python
3.14. Inherited premise, so it was re-tested rather than trusted.

**Confirmed.** `vectorbt==1.1.0` resolves, installs and imports against Python
3.14.4 in a throwaway venv, and `Portfolio.from_signals` returns
`total_return` / `sharpe_ratio` / `trades.count()`. It pulls **no numba**, which
is why it succeeds where `pandas-ta` was found uninstallable on 3.14.

**The residual, stated rather than waved past.** Resolution drags in ~40 packages
including `scikit-learn`, `scipy`, `requests`, `tqdm` and a notebook-widget
chain. This repo runs `pip-audit --strict` in CI and has taken four
advisory-driven floor bumps already (`pyproject.toml:12-25`), so that tree is a
standing supply-chain surface, not a one-off.

**Therefore the dependency lands in stage 5d, not stage 5a** (§8). Stages 5a–5c
are pure functions over the ledger and need nothing new, so the adoption
decision is taken against a working trade list rather than up front. What
`vectorbt` is genuinely worth buying is criterion 7's **portfolio-level,
path-dependent** max drawdown plus Sharpe / Sortino / exposure / turnover on a
cash-inclusive equity curve; hand-rolling portfolio accounting is the largest
correctness surface in the phase.

⚠ Whatever is adopted is wired per §2.1. A library's default fill semantics are
not a detail discovered during implementation; they are the defect.

---

## 5. Four decisions, with the source rule for each

Per `.claude/CLAUDE.md`, the trigger is *"am I about to pick a threshold, ratio
or window"*, and it does not care whether a regulator is involved. Each
subsection states whether a published rule exists.

### 5.1 Costs — a static model calibrated on IN-SESSION quotes

**Source rule: criterion 2**, which fixes the shape — static, conservative,
keyed on asset class and price band, calibrated at **p75 not the median**, and a
declared input to the identity hash (criterion 11).
`StrategyIdentity.cost_model_id` already exists and is already hashed into
`version` (`strategy_registry.py:204,235`); every call site passes the literal
`"undeclared-v0"`. The hook is built; the model is not.

**What is new is the calibration base, and it is a correction.** Parent §1
reports `quotes.spread_pct` at p50 0.110% / p75 0.235%. Re-measured on the same
table on 2026-08-07: **p50 0.284% / p75 0.930%** — 2.6× and 4.0× wider.

`quotes` holds **one row per instrument** (measured: 1,497 rows / 1,497
`us_equity` instruments) stamped `quoted_at`, so the table is a snapshot of
whatever session state each row was written in. Split by UTC capture hour:

| bucket | quoted | p50 | p75 | p90 |
| --- | ---: | ---: | ---: | ---: |
| in-session (14–19 UTC) | 1,151 | **0.194%** | **0.501%** | 1.107% |
| out-of-session | 346 | 1.608% | 2.670% | 3.610% |

**The confound was tested, not assumed.** Because there is one row per
instrument the two buckets are **disjoint instrument sets**, so no paired test is
possible and composition is a live alternative explanation. Stratifying by price
band, the gap survives inside every band that has both:

| band | in-session p75 | out-of-session p75 | ratio |
| --- | ---: | ---: | ---: |
| `$5–20` | 0.564% (n=237) | 2.585% (n=6) | 4.6× |
| `$20–100` | 0.509% (n=623) | 2.908% (n=103) | 5.7× |
| `>=$100` | 0.316% (n=215) | 2.576% (n=234) | 8.1× |

Composition does differ — the out-of-session set holds no `<$5` quotes and is
68% `>=$100`. **Stratification weakens composition as an explanation; it does not
eliminate it.** The `$5–20` cell rests on **6** out-of-session quotes, and the
`<$5` band cannot be tested at all because that bucket has none. The defensible
claim is the narrow one: *within the two bands that carry enough
out-of-session quotes to compare (`$20–100`, n=103, and `>=$100`, n=234), the
gap persists at 5.7× and 8.1×.* It is not *"the effect is not composition"*.

⚠ **`spread_flag` is NOT a valid control and was nearly used as one.** It is set
by `_upsert_quote` as `spread_pct > DEFAULT_MAX_SPREAD_PCT`, i.e. `> 1.0%`
(`market_data.py:147`) — derived from the very column under test. Stratifying by
it conditions on the outcome and mechanically truncates the distribution, which
is why the gap appears to shrink to 2.1× in the unflagged stratum. That number
is an artefact and must not be quoted as a confound-adjusted effect.

**The model that ships:**

⚠ **SUPERSEDED BY THE IMPLEMENTATION — `app/services/cost_model.py::BANDS` is the
authority and the table below is not it.** Stage 5b recalibrated on the §4.0
validated universe (limit 2 below says the `us_equity` set *"is not even the
right set"*, so the model is calibrated on the population it is applied to) with
the session resolved from `market_calendar`, and `quotes` had gained rows in
between — 1,528 → 1,557 total — which moves a percentile most in the thinnest
band. Shipped, quantised to 0.001pp with ROUND_CEILING: `<$5` **1.450** ·
`$5–20` **0.571** · `$20–100` **0.509** · `>=$100` **0.322**, on
n = 76 / 244 / 625 / 210. Reproduce with
`PYTHONPATH=. uv run python scripts/verify_2240_cost_model.py --calibrate`. The
table below is kept as the design record, not as a live number.

| price band (`us_equity`) | p75 spread | **half-spread per side** |
| --- | ---: | ---: |
| `<$5` | 1.600% | 0.800% |
| `$5–20` | 0.564% | 0.282% |
| `$20–100` | 0.509% | 0.254% |
| `>=$100` | 0.316% | 0.158% |

- **Arithmetic, stated so it cannot be guessed:** a buy fills at
  `fill_price × (1 + h)` and a sell at `fill_price × (1 − h)`, where `h` is the
  half-spread for the band. Net return is computed from those adjusted prices,
  never by subtracting a cost from `gross_return_pct` — `sql/256` names that
  column GROSS precisely so nothing averages it as performance.
- **The band is keyed on the ENTRY fill price only when that price is
  as-traded.** Research OHLC is split-adjusted and the corpus has no historical
  split factors, so it cannot honestly select a nominal-price band. Such rows
  use the maximum calibrated band as an adverse falsification arm (#2400),
  fixed for the life of the position.
- **Frozen as `cost_model_id =
  "static-p75-insession-v2+split-adjusted-max"`**, so the basis-policy change
  is a new strategy version rather than a silent improvement (criterion 11).
- **FX is a declared field, and setting it to zero requires a fact not yet
  established.** §4.0 restricts the validated universe to `us_equity`, which
  quotes in USD — but *"quotes in USD"* and *"needs no conversion"* are the same
  only if the account currency is USD, and the account currency has **not** been
  verified here (the eToro portal is unreachable from this environment). So
  `fx_bps` ships **NULL like `carry_bps`**, not zero, and the implementation
  ticket resolves it against the portal per the `etoro-api` skill's
  live-verification protocol. ⚠ Writing zero would be the #2286 shape: a value
  that is *present* and wrong beats a value that is absent and refused.

⚠ **Four limits, all stated on every result:**

1. **The "in-session" sample is really one hour.** 1,140 of the 1,151 in-session
   quotes sit at UTC hour 19 = 15:00–15:59 ET, the session's final hour. This is
   a closing-hour spread, not a session average, and closing-hour liquidity is
   at the favourable end of the day.
2. **The coverage denominator is a sample and its numerator is not even the
   right set.** The 1,151 in-session rows are `us_equity` quotes — that predicate
   is `exchanges.asset_class`, and it is **not** §4.0's validated universe, which
   additionally requires `instrument_type_id = Stocks` (ex-ETF). So "1,151 of
   6,735" is an *upper* bound on validated-universe coverage, not a measurement
   of it; the true overlap is smaller and was not computed. Either way the model
   is calibrated on well under a fifth of the universe and applied to all of it,
   and the `<$5` band rests on **72** quotes. This is criterion 2's own
   trade-off — `quotes` is all there is — but it is a sample, flagged as one.
3. **The session window is an approximation and its edges are wrong in both DST
   regimes.** The regular US session is 09:30–16:00 ET. Under **EDT** (UTC−4)
   that is 13:30–20:00 UTC, so `14–19 UTC` covers 10:00–15:59 ET and **misses
   the opening 30 minutes** — the most volatile and widest-spread part of the
   day, so the omission biases the model *optimistic*. Under **EST** (UTC−5) the
   session is 14:30–21:00 UTC, so the same literal would admit the 09:00–09:29
   ET pre-open and drop the 15:00–16:00 ET closing hour. The snapshot spans
   2026-06-18 → 2026-08-06, entirely EDT, so only the EDT error is live in these
   figures. **The implementation resolves the session from the exchange
   calendar, never from this literal**, and the recalibration under a correct
   window is a new `cost_model_id`.
4. **Carry is NULL, not zero.** Criterion 2 requires overnight/weekend CFD fees
   and FX, and says their magnitude *"is not established here"*. The eToro
   portal is unreachable from this environment, so it is not established here
   either. `carry_bps` is a declared field set to **NULL**; zero is a
   measurement nobody made. #2277 covers the standing re-check.

⚠ **What NULL carry does, precisely** — because every S-1/S-2 position and most
S-3/S-4 positions hold overnight, "refuse to report" would mean phase 5 reports
nothing. So: **statistics are computed and published with an explicit
`carry_unmodelled` marker; they are not promotable** (§6). Incomplete and
invisible are different states, and conflating them is how a phase ships that
cannot demonstrate it works.

### 5.2 The hold-out split is weighted by BAR, not by trading date

**The source rule for the split is criterion 5 itself** — *"the final 25% of
history is withheld"* — and it is followed. What criterion 5 does not say, and
what no published rule this environment can verify does say, is **how to weight
"history" on an unbalanced panel**. That sub-decision is fixed by construction
and frozen. On a balanced panel it would not arise. This panel is not balanced — measured, the validated
universe carries **30 series in 1970 and 5,245 in 2026** — so the phrase has two
readings eleven years apart:

| weighting | boundary | in-sample bars | hold-out bars |
| --- | --- | ---: | ---: |
| by trading date (16,236 dates) | 2010-05-18 | 9,392,777 (40.2%) | **13,946,806 (59.8%)** |
| **by bar (23,339,583 bars)** ✅ | **2021-06-29** | **17,501,058 (75.0%)** | **5,838,525 (25.0%)** |

The date reading is not a defensible alternative — it yields a hold-out **larger
than the training set**, trains on thin 1960s–2000s data and withholds the dense
modern era. Recorded so the ambiguity is not rediscovered and resolved the other
way.

**Adopted: bar-weighted.** Hold-out is `2021-06-29 → 2026-07-08`, 1,261 trading
dates, 5,266 series.

- **Inclusivity is fixed:** the boundary date is the **first hold-out bar**. A
  signal whose `signal_bar_date` is in-sample but whose `fill_bar_date` is on or
  after the boundary is **purged** — it is neither, because acting on it needs a
  price from the withheld side.
- **A position that spans the boundary belongs to the hold-out** and its entry is
  purged from the in-sample result. Splitting its return across namespaces would
  put hold-out prices into an in-sample number.
- ⚠ **The boundary is frozen as a literal, together with the corpus version and
  the evaluation end date** (`2026-07-08`). It is a function of the corpus, and
  the corpus grows; a recomputed boundary walks forward silently and re-admits
  hold-out data into training between runs. Appended data therefore sits outside
  the frozen window until a **deliberate re-freeze**, which is a corpus-version
  event that invalidates prior hold-out results and must be visible as one.
- ⚠ **The split is over corpus bars, not over each strategy's own signals.** A
  strategy whose signals cluster outside the modern era gets a hold-out that is
  25% of *bars* and some other fraction of its *trades*. The realised
  in-sample/hold-out **trade** counts are therefore reported per strategy.
  ⚠ **The minimum is not invented here.** Criterion 3 already requires an
  effective sample size from a date-clustered block bootstrap, and criterion 6's
  Deflated Sharpe consumes it — so the hold-out gate is *"the hold-out arm's
  **effective** sample size must be large enough for its own confidence interval
  to exclude the random cohort's 95th percentile"*, which is a computed
  quantity, not a threshold somebody picked. A strategy whose hold-out cannot
  meet that fails criterion 5 rather than passing on the panel-level split.

### 5.3 Purged walk-forward with an embargo

**Source rule: López de Prado, *Advances in Financial Machine Learning* ch. 7**
(purging and embargoing), which criterion 5 names by mechanism.

**The mechanism, stated in the correct direction** — an earlier draft had it
backwards:

- **Purge:** drop **training** observations whose label window overlaps the test
  fold.
- **Embargo:** drop **training** observations immediately *following* the test
  fold. Both act on the training side; serial correlation means a training
  sample drawn just after the test window still carries information from it.

⚠ **Both sides of the test fold carry training data, so this is ch. 7's purged
K-FOLD over contiguous time blocks, not a strictly anchored walk-forward.** An
anchored design has no training data after the test fold at all, which makes the
embargo — half of what criterion 5 asks for — unreachable. §2.2's own wording is
*"around each fold boundary"*, both sides.

⚠ **The embargo length is not quoted from memory.** AFML gives a proportional
rule of thumb which this environment cannot verify against the source (checked
twice: two independent secondary treatments of ch. 7 were fetched and neither
carries a numeric rule; the commonly-repeated `pctEmbargo = 0.01` appears in
discussion of the chapter's exercises). **It must not be cited.** The
construction below is mechanism-derived and available now.

#### ✅ RESOLVED at stage 5e-4 (2026-08-07). The embargo is MEASURED, per fold, on the PANEL axis.

The rule that shipped, in `app/services/walk_forward.py::training_embargo_bars`:

> `embargo(fold)` = the maximum panel-axis label-window span among the
> observations lying **wholly outside** the fold — its post-purge, pre-embargo
> training set.

Leak-free by construction (§5.3's own concession: a p100 taken on the training
side *"does not leak"*), non-circular (purge depends on the fold; embargo
depends on the purge; nothing depends on the embargo), and it needs **no
declared constant for any strategy, S-1 included**.

⚠⚠ **THE EARLIER DRAFT OF THIS SECTION WAS WRONG ON THE AXIS, and the
correction is the stage's main finding.** It said *"the embargo is
`max_hold_bars` wherever one is declared — S-3: 10 and S-4: 40"*. Those
constants count an **instrument's own bars**; folds are cut on the **panel
axis**, the union of every instrument's dates, of which each instrument's dates
are a subset. A hold of `h` instrument bars therefore spans `h` panel dates **or
more** — never fewer — so reading the constant straight onto a panel-axis window
under-covers, in the direction that leaks. Measured over 2,456,097 S-1
in-sample positions: the panel span exceeded `bars_held` on **3** of them, by up
to **374 dates**. Rare, real, and exactly the class the 5e-3 prevention-log
entry names. The measured rule subsumes `max_hold_bars` rather than
contradicting it — a strategy declaring one must measure a span at least that
large, which `verify_2240_walk_forward.py` F4 asserts.

**The two rejected candidates stay rejected as stated:** *measured p99* leaks 1%
by construction and its measurement spans the test folds; the surviving rule is
the *in-sample p100* §5.3 already conceded is leak-free, narrowed to one fold's
own training side so it does not span that fold either.

⚠ **§5.3's sole remaining objection to p100 — "unbounded above, and a single
long hold makes the embargo swallow the fold" — was a claim about a number
nobody had looked at. Measured** (`verify_2240_walk_forward.py --all`, full
population, in-sample only, 5,266 series / 14,975 dates / 17,501,058 bars,
0 property violations):

| | S-1 | S-3 (`max_hold_bars = 10`) |
| --- | ---: | ---: |
| closed in-sample positions | 2,456,097 | 22,811 |
| unlabelled at window end (excluded) | 2,661 | 166 |
| panel-axis span p50 / p95 / p99 / p100 | 1 / 13 / 60 / **931** | 10 / 10 / 10 / **10** |
| instrument `bars_held` p100 | 930 | 10 |
| positions within 10% of p100 | **1** of 2,456,097 | 17,346 of 22,811 |

| fold | dates | embargo `h` | `h / N_train` | embargoed share of the training side (S-1) |
| --- | ---: | ---: | ---: | ---: |
| 0 (1962-01-02 … 1999-09-02) | 9,486 | 615 | 615/5,489 = **11.204%** | 122,530 of 1,816,633 = 6.74% |
| 1 (1999-09-03 … 2009-03-19) | 2,399 | 931 | 931/12,576 = **7.403%** | 332,214 of 1,873,632 = 17.73% |
| 2 (2009-03-20 … 2016-03-22) | 1,764 | 931 | 931/13,211 = **7.047%** | 399,280 of 1,805,665 = 22.11% |
| 3 (2016-03-23 … 2021-06-28) | 1,326 | 931 | 931/13,649 = **6.821%** | 0 (nothing follows the last fold) |

**Verdict: the first branch of the decision rule.** The embargo costs at most
**22.11%** of one fold's training observations and leaves 78% standing; it does
not swallow any fold. So the in-sample p100 is adopted directly and **S-1 does
NOT declare a `max_hold_bars`** — its identity is untouched and no new strategy
version is minted.

⚠ **The structure of §5.3's fear is nonetheless real and is recorded rather than
waved past: the embargo IS set by a single observation.** Exactly 1 of 2,456,097
S-1 holds sits within 10% of p100, and p99 is 60 bars — **15.5× smaller** than
the 931 that binds. The bound is still the correct one (a bound must cover the
longest hold, not the typical one), and its cost is measured above rather than
assumed. What is rejected is paying for it by truncating real trades: declaring
a `max_hold_bars` would change what S-1 *does* in order to buy back training
observations in a cross-validation that fits no parameters — S-1's lookbacks are
*"fixed, never tuned"* (§4).

⚠ **Positions still open at the in-sample window end are EXCLUDED** — 2,661 for
S-1, 166 for S-3 — because their label is unresolved, not "as long as the data".
Admitting them with an end index at the axis end would set every early fold's
embargo to most of the corpus. It biases p100 **downward** (the longest holds
are the likeliest to be censored), so 931 is a lower bound and is reported as
one.

⚠ **The fold count is OURS and fixed by construction**, since AFML takes it as
an argument and fixes no value. It reuses a rule this phase already has:
criterion 5 withholds *"the final 25% of history"*, so a test fold is the same
share of the sample as the hold-out is of the corpus — **four folds**, cut
bar-weighted per §5.2 (realised: 25.00% / 24.99% / 25.00% / 25.01% of bars). It
is a module constant rather than an argument, because a fold count that can be
passed in is a fold count that can be swept, and a swept validity gate is a
search over validity gates. It is frozen in `WALK_FORWARD_MODEL_ID`.

⚠ **Nothing is STORED by 5e-4 and that is deliberate.** These strategies fit no
parameters, so the split is a validity gate rather than a training loop, and
there is no per-fold result row to write yet. Adding nullable walk-forward
columns nobody populates is precisely the defect `sql/266`'s own header records
(*"sql/262 shipped two of criterion 6's columns and none of its inputs"*). The
columns land with the writer, in 5e-5.

✅ **They landed at stage 5e-5c, as `sql/269`'s CHILD TABLE rather than as
columns** — the grain is (result, fold), and four columns per field would encode
`FOLD_COUNT` in the schema, making a future construction with a different count
a migration instead of a model-id bump. **No per-fold METRIC ships and that is
the same decision one level down**: the sentence above is why. Four per-fold
Sharpes have no use the spec names, and the obvious one — picking the best fold —
is the search criterion 6's trial count exists to bound. §8.8 records what a
fold row does carry and the two findings the writer produced.

### 5.4 Exposure, cash and the return denominator

**Source rule: §7**, quoted rather than re-derived — *"define cash return as
zero, report return on the full allocated pot, and state exposure time alongside
it"*, because *"a strategy invested 10% of the time can post a spectacular
return on almost no capital at work"*.

- **exposure time** = invested capital-days ÷ allocated capital-days over the
  window, at sleeve level. It is **not** `sum(bars_held)`; `sql/256` says
  `bars_held` *"is a bar count and NOT exposure time"*, and the difference is
  concurrency.
- **return denominator** = the full allocated pot; cash earns 0%.
- **three levels, never conflated** (§7): per-signal, per-strategy sleeve, total
  paper portfolio. Drawdown and Sharpe are computed at the latter two **only** —
  a per-trade max drawdown does not compose.
- ⚠ **Position sizing is NOT decided here.** §7 defines allocated pots, not
  slots, and equal-weight-per-signal, fixed-fraction and volatility-targeted
  sizing give materially different drawdowns from identical signals. Phase 5
  computes statistics **for a declared sizing rule, which is an input to the
  result identity**, and v1 declares **equal weight across concurrent positions,
  rebalanced only on position open/close**. Naming it as an input is what stops
  a later sizing change reading as a performance improvement.

  > ⚠⚠ **STAGE 5d TOOK THREE SUB-DECISIONS THIS SENTENCE DOES NOT.** No
  > published rule fixes them and each changes every number downstream, so they
  > are fixed by construction and frozen inside `equal_weight_concurrent_v1`
  > (`app/services/equity_curve.py::SIZING_RULE_ID`, hashed into
  > `ResultIdentity.version`):
  >
  > 1. **WHEN the equal weight is re-imposed** — only on an EVENT DATE, a date
  >    on which at least one leg opens or closes, which is the clause read
  >    literally. Between event dates the weights DRIFT. ⚠ The rejected reading
  >    is "rebalance every bar", which is a different and busier strategy and
  >    charges turnover the declared rule never incurs.
  > 2. **AT WHAT PRICE the rebalance trades** — at the event date's close.
  >    Entries and exits, the only LEDGER-DERIVED orders, transact at their
  >    stored fill prices at the open, which is what keeps §2.1's equality
  >    exact. A rebalance trade is produced by the sizing rule and has no stored
  >    fill price to equal.
  > 3. **SELLS BEFORE BUYS, buys capped by cash on hand.** ⚠ A single-pass
  >    rebalance to `equity / n` leaves cash at MINUS the cost it just charged —
  >    arithmetically small, and leverage, which the project posture forbids
  >    outright. Selling first makes `cash >= 0` hold by construction rather
  >    than by tolerance, and the under-investment it leaves is exactly the cost
  >    charged.
  >
  > ⚠ A fourth was rejected as degenerate rather than adopted: *"size a new
  > position at `equity / n` and never resize the existing ones"*. The first
  > position takes 100% of a flat pot, so the second is funded at zero and every
  > subsequent one too. It is not a viable reading of the clause.

  > **2026-08-12 attribution result (#2430):** the frozen v1 identity remains
  > necessary for reproducibility, but it must not be mistaken for a recommended
  > production allocation policy. Full-population recent-window A/B showed that
  > event-driven equalisation materially damages high-turnover S-1/S-3/S-4.
  > Entry-weight drift and calendar-month-end alternatives reduced that damage,
  > but none made the controls capital-worthy; drift also starved later signals.
  > S-2 remained below its passive hurdle under the funded monthly arm. See
  > `2026-08-12-sizing-rule-attribution-result.md`. Any replacement is a distinct
  > v2 identity and requires its own validation rather than rewriting v1 results.

---

## 6. #2288's remaining clauses land here

#2288 has four clauses. **Clause 1 is already shipped** and the ticket should
not be re-read as open: `strategy_signals.universe` is `NOT NULL` with **no
default** (`sql/255:87`), and `outcome_ledger.PendingFill` carries the label
forward rather than leaving it a join away.

Clauses 2–4 had no home until now, because they are about *results*:

2. **Fail closed on absence.** The result row's universe basis is `NOT NULL`
   with no default, exactly as `sql/255` does it. A metric whose basis cannot be
   established is not written.
3. **Surface it wherever a number is shown.** Phase 6 renders it; phase 5's
   obligation is that the field is on the row and non-null, so phase 6 *cannot*
   render a number without one.
4. **A promotion gate.** ⚠ #2288's own warning — *"a label nobody gates on is
   worse than no label"* — means the gate ships **with** the label. The hard
   pre-trade enforcement point is `execution_guard` (phase 7); what phase 5 owns
   is the **refusal at the result layer**: one function, returning a reason,
   failing closed, that phase 7's guard calls. It refuses on **any** of:
   - basis missing, or `survivor_only`;
   - `carry_unmodelled` set (§5.1);
   - instrument outside the §4.0 validated universe — §4.0's allocation
     invariant 2 is a universe rule, not only a survivorship one;
   - hold-out never evaluated, or evaluated more than once without a recorded
     access (criterion 5);
   - DSR not computed, or computed on an undeclared trial count (criterion 6).

⚠ **The binary label is not sufficient and the result model must not pretend
otherwise.** `survivor_only | survivorship_free` loses §4.0's measured nuance:
US survivorship is **partially** correctable at 86.2% issuer resolution, with
CEF/FPI-shaped residue and eToro-listing bias, and non-US is not correctable at
all. The result row therefore carries the **corpus version** alongside the basis,
and `survivorship_free` is not a value any current corpus can produce.

Today every result is `survivor_only` — measured, the corpus holds **7,693
series of which 2,424 have no `instruments` row**, and the delisted half is the
purchase that lands at the validation gate (#2284). The gate's initial state is
*"nothing is promotable"*. That is correct, not a bug to work around.

---

## 7. What was measured, and the queries that produced it

Run 2026-08-07 against the dev corpus, read-only, nothing written. Each figure
is reproduced by the query printed beside it in the run log; the labels below
are those of that run.

| # | figure | value |
| --- | --- | --- |
| M1 | §4.0 validated universe (`load_validated_universe`) | **6,735** instruments ⚠ parent §4.0 records **6,733** on 2026-08-05. Same predicate, four days apart — `is_tradable` moves with every `sync_universe`. This is drift to expect, not a discrepancy to reconcile, and it is why `validated_universe.py` returns ids rather than a count |
| M2 | research corpus, all series | 7,693 series · 25,818,944 bars · 1962-01-02 → 2026-07-08 |
| M3 | corpus ∩ validated universe | **5,266** series · **23,339,583** bars |
| M4 | per-series depth in that slice | min 3 · p25 1,294 · median 3,072 · p75 7,204 · max 16,236; **4,953** ≥273 bars, **4,018** ≥1,260 |
| M5 | trading dates in the slice | 16,236 |
| M12 | universe instruments with any corpus series | 5,266 of 6,735 (**78.2%**) |
| M13 | panel imbalance | 1970: 30 series / 7,620 bars → 2026: 5,245 series / 648,630 bars |
| M14 | bar-weighted 75/25 boundary | **2021-06-29** |
| M18 | slices at that boundary | 17,501,058 / 5,838,525 · 1,261 dates · 5,266 series |
| M19 | slices at the **date**-weighted boundary (rejected) | 9,392,777 / 13,946,806 |
| M6 | `quotes` spread, raw, all classes | n=1,528 · p50 **0.284%** · p75 **0.930%** · p99 4.829% |
| M9 | quote freshness | 2026-06-10 → 2026-08-07 · 1,492 of 1,528 within 7d · 356 `spread_flag` |
| M16 | `us_equity` in-session vs out | 0.194% / 0.501% (n=1,151) vs 1.608% / 2.670% (n=346) |
| M17 | in-session p75 by price band | 1.600% · 0.564% · 0.509% · 0.316% |
| M20 | `quotes` grain | 1,497 rows / 1,497 `us_equity` instruments — one row each, buckets disjoint |
| M21 | within-band in/out ratio | 4.6× · 5.7× · 8.1× |
| M22 | in-session rows with NULL `last` | **4** — reconciles M17's 1,147 against M16's 1,151 |
| M23 | in-session hour concentration | **1,140 of 1,151 at UTC hour 19**; span 2026-06-18 → 2026-08-06, all EDT |
| M24 | `spread_flag` stratum (⚠ invalid as a control, §5.1) | in-session 0.361% unflagged vs 2.520% flagged |
| M10 | ledger occupancy | `strategy_signals` **0** rows / 0 versions · `strategy_outcomes` **0** |
| M11 | corpus vs delisting register | 1,282 Form 25 rows · 2,424 corpus series with no `instruments` row |

M10 is why the result-model schema and the S-1 `max_hold_bars` change are cheap
**now**: there is nothing to backfill, and a `NOT NULL` basis column added after
the ledgers fill is a column somebody has to invent history for.

---

## 8. Stages

Five tickets, each independently mergeable with its own full-population
verification.

| stage | what | depends on |
| --- | --- | --- |
| **5a** | **Position construction** — §3 in full: the three regimes, the §3.1 pyramiding rule, version pinning, `ambiguous` as terminal, same-bar ordering, S-2's drop-out close. Pure function over ledger rows. | resolver version selection (an input, not new code) |
| **5b** | **Cost model** — frozen table plus an explicit price-basis policy; split-adjusted research prices use the maximum band under `static-p75-insession-v2+split-adjusted-max` (#2400). `carry_bps` remains NULL and the session is resolved from the exchange calendar. | 5a |
| **5c** | **Result model + #2288 clauses 2–4** — the result table, basis `NOT NULL` no default, corpus version, and the promotion-refusal function. | 5b |
| **5d** | **Statistics** — criterion 7's full metric set on the equity curve; the `vectorbt` adoption decision (§4) is taken here against 5a's trade list. | 5c |
| **5e** | **Validity gates** — frozen hold-out namespace with access logging (criterion 5), purged walk-forward + embargo (§5.3, including S-1's declared bound), block bootstrap clustered by date (criterion 3), Deflated Sharpe with a declared trial count (criterion 6), quarantine sensitivity arm (criterion 9), and the 1,000-strategy random-entry synthetic control. | 5d |

⚠ **5e is five tickets, not one, and the sub-stages are sequenced by what each
unblocks.** Split at stage 5e-1, which found the first item is also what the
result WRITER depends on — the row keys on `namespace`, so nothing may be stored
until the namespace has a mechanism.

| sub-stage | what | state |
| --- | --- | --- |
| **5e-1** | **The hold-out namespace, its access log, and the first `strategy_results` writer** — criterion 5's mechanical inaccessibility, `sql/264`, `app/services/result_ledger.py`. | ✅ shipped |
| **5e-2** | **Block bootstrap clustered by date** (criterion 3) → fills `effective_sample_size` and clears one of the seven standing refusals. `app/services/block_bootstrap.py`, `sql/265`. | ✅ shipped |
| **5e-3** | **Deflated Sharpe on a declared trial count** (criterion 6). Consumes 5e-2's output; §5.2 is explicit that a DSR on a nominal *n* is the number criterion 3 forbids. `app/services/deflated_sharpe.py`, `app/services/trial_register.py`, `sql/266`. | ✅ shipped |
| **5e-4** | **Purged walk-forward + embargo** (§5.3). `app/services/walk_forward.py`. ⚠ The "blocked on S-1 declaring a `max_hold_bars`" row was **struck**: the block was an unstarted measurement, not a decision, and the measurement adopted the leak-free in-sample p100 with S-1's identity untouched. | ✅ shipped |
| **5e-5a** | **Quarantine sensitivity arm** (criterion 9) — the two-arm loader, the census, the metric delta, and `quarantine_arm` on the result identity. `app/services/quarantine_sensitivity.py`, `sql/267`. | ✅ shipped |
| **5e-5b** | The **1,000-strategy random-entry control** (§9, *the harness itself*) — the permutation, both thresholds, and the three promotion refusals. `app/services/random_entry_cohort.py`, `sql/268`. ⚠ Cohort run at N = 1,000 for **S-3 only**; S-1's is compute-bound, §8.6. | ✅ shipped |
| **5e-5c** | The **per-fold walk-forward writer** 5e-4 deliberately left unwritten (`sql/269`, `walk_forward.WalkForwardFolds`), and the **per-arm result writer** — `store_in_sample_arm_pair` / `store_holdout_arm_pair` plus `quarantine_arms_compared`, which is the first thing to produce that gate input from the database. | ✅ shipped |

⚠ **5e-5b was split again at the writers**, and the reason is the same one that
split 5e at 5e-1 and 5e-5 at 5e-5a: the control CHANGES WHAT A STORED RESULT
MEANS. It adds three promotion refusals and a derived-verdict CHECK
(`sql/268`), and a schema change to the result row is cheap only while
`strategy_results_store` is empty. The two writers add ROWS and no semantics, so
they are strictly cheaper afterwards. ⚠ The control also unblocks something the
model already referenced: `PromotionCandidate.ambiguity_material` is defined
(§3.4) as *"the two ambiguity arms' Sharpe differ by more than the gap between
the strategy and the random cohort's 95th percentile"* — that gap did not exist
until this stage, so the §3.4 rule had no measurable right-hand side.

⚠ **5e-5 was split at 5e-5a**, for the reason 5e was split at 5e-1: the first
item turned out to change the RESULT IDENTITY (`quarantine_arm`, §8.5), and an
identity change is cheap only while `strategy_results_store` is empty. The
random-entry cohort writes rows; doing it second means it writes them under an
identity that can already express which arm produced them.

### 8.1 ⚠⚠ Stage 5e-1's finding: RLS is not criterion 5's mechanism on this database

C5 says the hold-out must be *"mechanically inaccessible to exploratory
queries"*, and the textbook answer is row-level security. **Measured 2026-08-07
rather than assumed**: a probe table with `ENABLE` + `FORCE ROW LEVEL SECURITY`
and a `USING (ns = 'in_sample')` policy returned **both** rows, because this app
connects as `postgres` with `rolsuper` and `rolbypassrls` both true. `FORCE`
binds the table OWNER; it does not bind a superuser.

What ships instead has no bypass bit, because a **view** filters and a
**trigger** fires for every role including a superuser:

- `strategy_results` is now a **VIEW**, `WHERE namespace = 'in_sample'`, `WITH
  CASCADED CHECK OPTION`. The obvious name — the one in every doc and every
  `select *` — cannot express a hold-out row. Storage moved to
  `strategy_results_store`, a name you have to decide to type.
- A **BEFORE INSERT OR UPDATE trigger** on the store refuses any hold-out row
  whose `(strategy_id, strategy_version, result_version)` has no `evaluate`
  record in `strategy_holdout_accesses`. An unrecorded hold-out evaluation is
  unrepresentable, not discouraged.

⚠ **The residual, stated rather than waved past.** This is not protection
against a determined reader — naming the store is one word. It is protection
against the failure mode C5 actually describes: withheld numbers arriving in a
result set nobody asked for, and a strategy iterated against them. Restoring the
read side as a hard boundary needs a **non-superuser application role**, which is
a change to how every connection in the app authenticates; the test that
measures the role FAILS if one ever appears, which is the signal to revisit.

### 8.2 Stage 5e-2: which rule fixed each constant, and which are ours

Criterion 3 names the method and rejects the shortcut it could have taken
(*"'Effective n ≈ nominal/20' is too crude"*), but leaves the block length open.
Every constant below is either taken from a published rule and cited, or
declared as ours and frozen in `BOOTSTRAP_MODEL_ID` — none is reasoned out.

| choice | fixed by |
| --- | --- |
| Block length | **Politis & White (2004)**, DOI `10.1081/ETC-120028836`, with **Patton, Politis & White (2009)**, DOI `10.1080/07474930802459016`. MEASURED off the cluster axis' own autocovariance, never declared. |
| Circular (wrap-around) blocks | **Politis & Romano (1992)**. ⚠ Also what makes `4/3` the right constant — `2` is the stationary bootstrap's, and crossing them mis-sizes every block silently. |
| Effective sample size | **Kish (1965)** §8.2 design effect: `ESS = n / deff`, `deff = Var_boot(mean) / Var_iid(mean)`. Units of TRADES, so it is commensurable with `trade_count` beside it in criterion 7. |
| Interval | **Efron & Tibshirani (1993)** ch. 13 percentile method. ⚠ First-order accurate only; BCa needs a cluster jackknife over the full population and is NOT computed. Stated, not silently omitted. |
| Resample count = 2,000 | **OURS**, above Efron & Tibshirani's 1,000 floor for interval estimation. `sql/265` enforces the floor. |
| Cluster key = **entry fill date** | **OURS**, from criterion 3's own stated reason — *"signals are correlated across instruments on the same day"* is a statement about the day they FIRED. An exit-date key would scatter one market-wide entry across as many clusters as it had holding periods. |
| Cluster axis = **active dates**, not the full trading calendar | **OURS.** Padding with zero-trade dates would make a block a fixed calendar span but fill it with dates carrying no error to cluster, diluting the correlation being corrected for. Consequence: a block of `b` clusters spans more calendar time in a sparse period. |

⚠ **What makes the ~10^6-trade population tractable** is that pooled expectancy
is a RATIO — `sum(cluster sums) / sum(cluster counts)` — so a cluster enters a
resample fully described by its `(count, sum)` pair and resampling gathers over
~10^4 dates rather than ~10^6 trades. This is exact, not an approximation of a
per-trade resample: it is the same arithmetic.

⚠ **The refusal is unchanged in shape.** `effective_sample_size` is still NULL
whenever the caller declares no `bootstrap_seed`, or the measurement is
degenerate (one cluster, zero trade variance, zero bootstrap variance), and the
promotion gate still refuses on it. Criterion 3 forbids a nominal-*n* fallback
anywhere, so a bootstrap that could not run must leave the column empty rather
than fill it with the number the criterion exists to replace.

⚠ **5b changes all four strategy versions** — `cost_model_id` is hashed into
`version`. Signals stored under `undeclared-v0` are not reusable under the new
id. This costs nothing today (M10: 0 rows) and would be expensive later, which
is an argument for doing 5b early rather than a problem with it.

**Incidental finding, filed not fixed.** `s3_mean_reversion_in_trend.py:114`
claims `MAX_HOLD_BARS`' consumer is `outcome_resolver.ExitLevels.max_hold_bars`,
which cannot be constructed for S-3 (§3). The docstring is wrong today, not once
phase 5 lands. Filed as **#2348** rather than folded into this spec — a narrow
doc fix should not wait on a phase.

### 8.3 Stage 5e-3: the source rule, and the axis decision it forced

**Source rule: Bailey, D. H. & López de Prado, M. (2014), *The Deflated Sharpe
Ratio*, Journal of Portfolio Management 40(5):94-107, SSRN `2460551`** — read at
implementation time, not recalled. Criterion 6 names the method and its four
inputs; the paper supplies every constant, so none is ours:

| choice | fixed by |
| --- | --- |
| DSR statistic | **Eq. (2)** — `Z[(SR − SR₀)√(T−1) / √(1 − γ₃SR + ((γ₄−1)/4)SR²)]`. |
| Rejection threshold `SR₀` | **Eq. (1)/(6)**, under `H₀: SR = 0`, with the Euler-Mascheroni weighting of two Normal quantiles. |
| Trials' correlation → `N̂` | **Appendix A.3, eqs. (8) and (9)** — `ρ` is the mean off-diagonal correlation, `N̂ = ρ̂ + (1 − ρ̂)M`. This is criterion 6's "their correlation" input, and it is a published rule rather than a heuristic. |
| `γ₄` convention | **RAW** fourth moment (the paper's example uses `γ₄ = 10` and states a Normal is `γ₃ = 0, γ₄ = 3`). Excess kurtosis would shrink the denominator and inflate every DSR by a silent constant. |
| Normal CDF / quantile | `statistics.NormalDist` — **stdlib, no new dependency**; scipy is absent from this project (verified). Reproduces all three of the paper's published values to 4 dp. |

⚠⚠ **The paper's inputs are PER OBSERVATION, and its own worked example is where
that is visible** — an annualised SR of 2.5 enters as `2.5/√250`, `V[{SRₙ}] = ½`
enters as `1/(2·250)`, and `T = 1250`. Mixing an annualised Sharpe into eq. (2)
would inflate the numerator by `√(periods per year)` and leave the denominator
alone.

⚠⚠ **WHICH AXIS THE FOUR INPUTS LIVE ON IS OURS, and 5e-2 fixed it.** Eq. (2)
divides a Sharpe by the standard error of that same Sharpe, so `SR`, `γ₃`, `γ₄`
and `T` must describe ONE series. §5.2 says the DSR consumes criterion 3's
effective sample size, and 5e-2's ESS is **in units of trades**. Therefore the
DSR is computed on the **trade axis** — `dsr_trade_sharpe` is a per-trade
Sharpe and is **not** criterion 7's annualised `sharpe`, which is computed on
the equity curve. The rejected alternative was to keep 5d's per-period Sharpe
and divide its period count by the design effect: that carries a design effect
measured by clustering TRADES onto a series of PERIODS, and no test on either
side could see it. `sql/266` stores the two under different names for this
reason.

⚠ **The trial register is a DECLARATION and a documented FLOOR.** No query can
produce criterion 6's count — a variant eyeballed in a session and dropped left
no row anywhere — so `app/services/trial_register.py` is hand-declared, every entry
carrying its evidence, and reviewed in git. What counts is a **search of price
data**: the four shipped strategies and every #2260 RSI arm (including the
withdrawn non-causal one — an artefact is still a search); S-5 and S-6 are
specified but never run and are therefore **absent**. Under-counting `M` lowers
`N̂`, lowers `SR₀` and **raises** the DSR, so a stored value is an **upper
bound** on the honest one. `V[{SRₙ}]` is estimated only from the trials that
carry a measured Sharpe, and those are the ones that survived to be measured —
the same flattering direction. Both are stated in the module header rather than
left for a reader to discover.

⚠ Exhibit 3.1 measures eq. (1)'s own accuracy: it OVERSTATES the empirical
expected maximum by under 0.05 for `N < 50` at `V = 1`. Our `N` is small, so we
sit at the loose end of the published range — and it errs toward a higher `SR₀`,
hence a lower DSR, which is the conservative direction.

### 8.4 Stage 5e-4: which constants are AFML's, which are ours, and the one that was a query

**Source rule: López de Prado, *Advances in Financial Machine Learning* (2018)
ch. 7** — purging and embargoing — which criterion 5 names by mechanism. It
fixes the two OPERATIONS and nothing else, so every remaining choice is declared
as ours and frozen in `WALK_FORWARD_MODEL_ID`:

| choice | fixed by |
| --- | --- |
| Purge = drop training observations whose LABEL WINDOW overlaps the test fold | **AFML ch. 7.** ⚠ Tested on the INTERVAL, not the endpoints — an observation spanning the fold entirely is the case an endpoint test calls training data. |
| Embargo = drop training observations STARTING in the window after the test fold | **AFML ch. 7.** ⚠ Keyed on the entry, closed on the right: an `h`-bar embargo covers `h` dates. |
| Purged K-fold rather than an anchored walk-forward | **AFML ch. 7 + §2.2's "around each fold boundary".** An anchored design has no training data after the test fold, so it cannot have an embargo at all. |
| Embargo LENGTH | **OURS**, mechanism-derived — the per-fold measured maximum panel-axis span over the post-purge training set. ⚠ AFML's proportional rule of thumb is **unverifiable from this environment** and is NOT cited; see §5.3. |
| Fold count = 4 | **OURS**, by construction from criterion 5's own 25%: a test fold is the same share of the sample as the hold-out is of the corpus. Not a round number chosen for looking reasonable. |
| Fold boundaries weighted by BAR | **§5.2**, reused verbatim including its "cumulative strictly exceeds" selection rule. |
| Observations = CLOSED positions only | **OURS.** An open position's label is unresolved; admitting it with an end index at the axis end makes every early fold's embargo an artefact of where the data stops. Counted and reported, both sleeves. |

⚠⚠ **The stage's finding is an AXIS error in this document's own earlier
construction**, and it is the second instance of the class §8.3 recorded. See
§5.3: `max_hold_bars` counts instrument bars, folds are cut on panel dates, and
the panel span exceeded `bars_held` on 3 of 2,456,097 S-1 positions by up to 374
dates. Measuring the embargo on the axis the window is cut on removes the
mismatch rather than correcting for it.

⚠ **Nothing is stored.** These strategies fit no parameters, so 5e-4 is a
validity gate and not a training loop; the walk-forward columns land with the
per-fold writer in 5e-5, not before. See the end of §5.3.

### 8.5 Stage 5e-5a: what the arm is, what it is NOT, and the identity it forced

**Source rule: the spec's own C9**, which is where a sensitivity arm's
"conservative handling" is defined for this project — *"re-run with quarantined
bars admitted at their stored values rather than masked, and report the delta in
every C7 metric"*. Parent criterion 9 fixes the census (*"the count and share of
bars/trades excluded per strategy"*) and the reason for it (*"bad bars correlate
with illiquidity and volatility"*). No external rule applies: the quarantine is
ours (#2261 / `price_quarantine.RULE_SET_VERSION`), so its arm is ours too, and
every choice below is declared rather than reasoned out from first principles.

| choice | fixed by |
| --- | --- |
| The arm = flagged fields at their STORED values | **C9's own wording.** ⚠ Not a third "drop the whole bar" arm: C9 asks what the exclusion COST, and the only handling whose delta answers that is the one that stops excluding. |
| Series-level fail-closed is **not** an arm | **OURS.** A series with no coverage row, or coverage at a stale rule-set version, has no stored value to admit — the rules have never seen it. It is excluded from BOTH arms and therefore never appears in a delta, so it is counted separately or it is invisible. |
| Both arms off ONE fetch (`load_arms`) | **OURS**, for correctness before efficiency: two reads are two chances for the arms to differ by something other than the arm. `QuarantineCensus` refuses a pair whose bar, series or flag counts disagree. |
| `*_flagged` counts are arm-invariant; `*_masked` are not | **OURS.** If the census followed the arm, the admitted run would report its own exclusion as empty — an arm measuring what masking cost, printing "nothing was masked". |
| A metric null in one arm has **no** delta | **OURS**, from `strategy_statistics`' own nullability rules. A zero would render "the admitted arm gained a losing trade, so `profit_factor` became computable" as "unchanged". |
| **No materiality threshold** | **Criterion 9 declares none.** It asks that the exclusion be *visible*, not small. §3.4's ambiguity pair has a materiality gate because the spec declares one; inventing a cut here would be the made-up constant `.claude/CLAUDE.md` forbids. The promotion gate refuses on `quarantine_arms_not_compared` and has no `quarantine_material` twin — asserted by test, so its absence is pinned rather than left to review. |
| `quarantine_arm` on the RESULT key, not the strategy hash | **Criterion 11 + this table's own precedent.** It is a property of how a result was MEASURED, which is where `input_rule_set_version` already sits. A masked and an admitted run are the same strategy measured two ways. |

⚠⚠ **The stage's finding is an AMPLIFICATION, and it is a decision already
written down rather than a bug.** A masked close is not a one-bar hole. For S-1
it suppresses the rolling windows that span it; for S-3 it suppresses **the rest
of the series**, because Wilder smoothing carries state forward and
`indicator_series.rsi_series` marks every index from the first NULL close onward
unevaluable — which `s3_mean_reversion_in_trend`'s header states in full. So the
delta the arm reports is not proportional to the flagged bar count, and reading
the census alone would understate the exposure by orders of magnitude.

⚠ **The arm runs S-1 and S-3 only**, for phase 5a's reason (S-2 needs its whole
panel resident, S-4 needs the resolver over the corpus). That gap bites harder
here than in earlier stages: **S-4 is the only sleeve that reads high and low**,
and the RANGE verdict is the larger half of this corpus's quarantine. The metric
delta is therefore measured against the smaller exposure, and the census reports
both verdicts so the untested one is visible rather than absent.

⚠ **Nothing is written by the sweep.** `sql/267` adds the identity column so a
stored arm is expressible; the per-arm writer lands with 5e-5b, which is the
first stage that has rows to write.

⚠ **Stage 5e is the phase, not an appendix.** The parent is explicit that
reproducing #2260 is *"necessary but not sufficient"* and pairs it with the
random-entry cohort at a stated threshold, because *"a stated threshold matters
more than the test"*. A phase 5 shipping 5a–5d is a number generator, not a
backtester.

---

### 8.6 Stage 5e-5b: what the null is, the residual that was not one, and the cohort that did not run

Criterion 9's arm asked what an exclusion cost. §9's control asks a harder
question — *is any of this distinguishable from chance* — and the answer is only
worth as much as the null it is measured against.

| choice | fixed by |
| --- | --- |
| Cohort size 1,000; Sharpe at the 95th percentile; a 95% interval; a recorded seed | **The parent, verbatim.** All four are `SPEC_` literals with a single bridge test. |
| The randomisation is a **PERMUTATION** — per series, keep the realised trade count and the multiset of holding periods, redraw only the entry ordinals | **OURS**, and the alternative is named: calibrating a Bernoulli entry rate until exposure and turnover land near the strategy's makes the match an optimisation with a tolerance nobody can source, and the tolerance becomes a free parameter of the null. Under the permutation the trade count matches by construction and is ASSERTED. |
| Placement space = the strategy's own **eligible fill bars** (usable open, inside the window, past the declared warm-up) | **OURS, from §3.5 and the strategies' own `WARMUP_BARS`.** A member must not be able to trade a bar the real strategy was structurally unable to trade. |
| The gaps are `m` iid uniform draws, SORTED | **OURS**, frozen in `COHORT_MODEL_ID`. ⚠ Uniform over the sorted DRAW, not over the legal placements — the two differ as a multiset differs from a composition. Neither is fixed by any source; the alternative is named in the code so a later reader sees it was a choice. |
| Touching permitted — a position may open on the ordinal a previous one closed on | **`position_builder`'s own rule 4.** Forbidding it would make the cohort's placement space strictly SMALLER than the real strategy's. |
| The percentile is an **ORDER STATISTIC** (`inverted_cdf`, Hyndman & Fan 1996 type 1), not NumPy's interpolation | **OURS**, and it was a real defect: the module declared the 950th order statistic and the code interpolated between the 950th and 951st — a cut at a value no member achieved. Caught at Codex checkpoint 2. ⚠ The bootstrap INTERVAL deliberately keeps NumPy's default, matching stage 5e-2's shipped convention; they are different quantities. |
| The cohort is **NOT** a trial count for criterion 6 | **OURS, and now written down** rather than left implicit. `trial_register` counts searches of price data for an edge to ship; no cohort member is a promotion candidate or can be selected into one. |

⚠⚠ **THE FINDING: A MATCHING RESIDUAL MEASURED ON A RUINED EQUITY PATH MEASURES
THE RUIN, NOT THE MATCH.**

Read off the costed run alone, the cohort looks badly mismatched on the two
things §9 names — exposure **34.65%** against S-3's **85.92%** (−51.3 points) and
turnover **12.33/yr** against **39.21** (−26.9). That reads as *"the permutation
destroyed the strategy's concurrency, so the null is mis-specified"*, and it was
the reading this stage was about to write down. The zero-cost ablation (§9.2)
falsifies it outright: the **same placements, same seeds** at `h = 0` hold
**99.87%** exposure and turn over **36.37/yr**, both within a few points of the
strategy's own. The permutation reproduces exposure and turnover; the costed
cohort does not, because **an equity path collapsing toward zero carries no
capital and therefore trades no notional**. The residual is a property of the
RESULT, not of the matching.

⚠ Generalises past this stage: exposure and turnover are RATIOS whose denominator
is the equity path. Comparing them between two runs whose paths differ by orders
of magnitude compares the paths, not the quantity named. The ablation cost ten
minutes and moved a "the null is wrong by 51 points" conclusion to "the null is
right and the cohort is ruined".

⚠⚠ **S-1's COHORT DID NOT RUN, AND THE REASON IS COMPUTE, NOT DESIGN.** Every
piece of machinery is strategy-agnostic and `--prepare` cached S-1's inputs in
the same sweep (3,133,100 realised holds, 0 series unable to carry them). What
stops it is scale: S-1's book is **3,133,100 legs**, `build_equity_curve` takes
**20.7 s** over it and a whole member **31 s** standalone — **8.6 CPU-hours** for
1,000 members, and measured **122 s/member at 37% CPU** on the shared box (the
rest is swap wait, against a 10 GB resident model server and an 8.2 GB container
VM). S-3's 27,782-leg book runs a member in **1.05 s**, which is why its cohort
and its ablation are both complete at N = 1,000. The command is in §8.7 and needs
only a machine with the RAM to hold four or five 1.6 GB members at once.

⚠ So the control exists, at full population and full cohort size, for **one** of
the four catalogued strategies. S-2 and S-4 do not run for phase 5a's standing
reason (a resident panel; the resolver over the corpus); S-1 is the one that is
merely expensive.

### 8.7 Stage 5e-5b: the remaining command

```
PYTHONPATH=. uv run python scripts/verify_2240_random_entry_cohort.py --prepare
# then, sharded across as many processes as RAM allows (~1.6 GB resident each):
PYTHONPATH=. uv run python scripts/verify_2240_random_entry_cohort.py --cohort --strategy S-1 --members 0:200
...                                                                                          --members 800:1000
PYTHONPATH=. uv run python scripts/verify_2240_random_entry_cohort.py --report
```

⚠ Member `m`'s stream is a pure function of `(COHORT_ROOT_SEED, m)`, so the shard
boundaries may be chosen freely and re-drawn between attempts without moving a
single entry. ⚠ `--report` refuses (R5) unless the member indices are exactly
`0 … 999`, so a partial cohort cannot be quoted as a §9 figure by accident.

### 8.8 Stage 5e-5c: what a stored split carries, and the shape both writers exist to make unreachable

Two writers, and the same argument produced both: **the state each one makes
unrepresentable is a HALF-WRITTEN one that reads as complete.**

- `store_in_sample_arm_pair` / `store_holdout_arm_pair` take criterion 9's two
  arms together. A lone `admitted` row is a number `sql/267` says may never be
  quoted; a lone `masked` row is the state the gate refuses as
  `quarantine_arms_not_compared`. Neither is reachable through one call, and a
  raise or a rollback leaves neither behind.
- `store_walk_forward_folds` takes a whole `WalkForwardFolds` — four contiguous
  folds counting one population — and writes them in one `executemany`. There is
  no per-fold writer, because a three-of-four split is a cross-validation that
  stopped early and nothing about the stored rows would say so.

**The pair check is one comparison, not a field sweep.** `_check_arm_pair`
rebuilds the masked identity with the admitted arm and requires it to EQUAL the
admitted one. A field-by-field sweep has to be extended by hand whenever
`ResultIdentity` gains a member, and the newest member is exactly the one a pair
is most likely to differ in — `quarantine_arm` itself was added one sub-stage
ago for that reason. This is `QuarantineCensus`' controlled-experiment check
(*"a difference means the populations differ and no delta between them is
interpretable"*) moved up from the bar counts to the identity.

**`quarantine_arms_compared` is the first thing to produce that gate input from
the database**, and its hold-out branch records a `read`. Presence is a fact
about the withheld side, so looking at it is an access — the same rule
`read_holdout_results` applies to a read that returns nothing. ⚠ A `read` and
never an `evaluate`: nothing was produced, so criterion 5's evaluation
arithmetic must not move. An in-sample identity records nothing, because an
audit trail that counts automation is not an audit trail.

**What the fold table stores and what it deliberately does not** — `sql/269`'s
header carries both in full. The short form: geometry on both axes (indices AND
dates, because an index is unreadable once the corpus axis moves), the realised
bar count, the MEASURED panel embargo, and the four-way census as four columns
rather than one `dropped` total. No per-fold metric (§5.3), and no new promotion
refusal — `check_promotable`'s vocabulary is sourced clause by clause from §6 and
§3.4, neither of which declares a walk-forward bullet, so a `walk_forward_not_run`
code invented here would be a gate semantic with no source rule behind it.

#### What was measured (full population, `verify_2240_result_writers.py --all`)

5,266 series, 0 fail-closed empties, 501.8 s, **0 property violations**, exit 0.
The split written is the one 5e-4 measured, re-derived from the corpus in this
run rather than quoted:

| | S-1 | S-3 |
| --- | ---: | ---: |
| in-sample observations (each fold classifies all of them) | 2,456,097 | 22,811 |
| embargo, folds 0-3 (panel bars) | 615 / 931 / 931 / 931 | 10 / 10 / 10 / 10 |
| purged, folds 0-3 | 0 / 597 / 111 / 606 | 0 / 20 / 1 / 3 |
| embargoed, folds 0-3 | 122,530 / 332,214 / 399,280 / 0 | 14 / 2 / 3 / 0 |

Realised bar shares 25.00% / 24.99% / 25.00% / 25.01%. ⚠ The four numbers on the
S-1 embargo row and the four in its embargoed row are §5.3's own table, arrived
at from a fresh sweep — which is the cross-check that matters here, since a
stored split is only worth as much as the measurement it froze.

⚠⚠ **The round trip is the assertion, and the magnitudes are why it needs the
full population.** W2 writes the split and reads it back as a whole object:
counts near 2.5M and an embargo of 931 exercise the `BIGINT` columns and the
13-position column mapping that a 30-observation fixture cannot. W3 then takes
the identical object and the identical statement to a **hold-out** parent and
requires `sql/269`'s trigger to refuse it — so a pass is about the parent's
namespace, not about anything in the payload. ⚠ Every write in the arm is rolled
back and the occupancy of both tables is re-counted afterwards (W4/P4): "it
rolled back" is asserted, not assumed.

⚠ **The `--pair` arm is a MECHANISM arm and says so.** The two arms' metric delta
is stage 5e-5a's measurement and is not re-derived here — that is an 83-minute
corpus sweep to re-measure a number nothing in this stage changes. What it
asserts is the storage behaviour 5e-5a could not have: both arms land under
different `result_version`s, the pair reads back as compared from EITHER arm's
identity, a lone arm does not, and storing the pair clears
`quarantine_arms_not_compared` **and nothing else** — 8 refusals still stand,
which is §6's stated initial state.

#### ⚠⚠ The atomicity finding: one of the two writers needed its own transaction and the other did not, and the difference is `executemany`

Codex raised at checkpoint 2 that both writers took their atomicity from the
CALLER's connection — and this repo opens autocommit connections (`app/main.py`'s
lifespan guards, the runbooks), where each statement commits on its own. For the
**arm pair** that is exactly right: two separate `execute` calls, so the masked
arm would commit before the admitted one was refused, leaving the lone-arm state
the API exists to make unreachable. It now owns a `conn.transaction()`, and the
revert probe removing it is **CAUGHT** by a test written for the purpose.

For the **split** the same objection does not survive measurement. On psycopg
**3.3.3** (measured 2026-08-08 — autocommit connection, temp table with a
primary key, an `executemany` whose third statement violates it) the two rows
before the failure do **not** survive: `executemany` runs its batch in a
transaction of its own. The wrapper is kept as defence in depth and is described
as that rather than as the mechanism, and no probe ships for it, because the
probe would be reporting on the driver. ⚠ The NOT CAUGHT that established this
is the evidence, not a failure: triaged selector → fixture → code, the answer was
a fourth one the prevention log now carries — *the injected defect is not a
defect, because a lower layer already provides the property.*

⚠ **Stated gap, unchanged from 5e-4:** only S-1 and S-3 sweep. S-2 needs its
whole panel resident and S-4 the resolver over the corpus (phase 5a's reason).
Neither changes a rule these writers apply — a split is a function of dates and a
pair is a function of two identities — but no S-2 or S-4 split has been stored.

---

## 9. Acceptance

One block per parent §5 criterion, in the parent's own order, so a missing
criterion is visible as a missing heading rather than inferred.

**C1 — point-in-time universe.** Every result row carries a non-null universe
basis and the corpus version (§6). Eligibility predicates are evaluated as-of
each decision date, never once against today's state (§3.5 rule 5) — asserted by
replaying one strategy at two different "today" values and requiring identical
historical signals. The promotion check refuses `survivor_only`.

**C2 — costs.** (a) Costs are non-zero on every position, applied as adjusted
fill prices, never subtracted from `gross_return_pct` (§5.1). (b) Both sides are
charged on a closed position and **one side** on an open-at-window-end mark
(§3.2 rule 5). (c) The band table is keyed on the entry fill price and pinned by
test — ⚠ to the SHIPPED figures (`app/services/cost_model.py::BANDS`), not to
§5.1's, which were measured on a `quotes` snapshot that no longer exists and
cannot be reproduced from any later database. What the pin buys is the same
either way: no silent recalibration. (d) `cost_model_id` is hashed into `strategy_version`
— asserted by changing the model and requiring every version to move. (e)
`carry_bps` and `fx_bps` are NULL, every result carries `carry_unmodelled`, and
the promotion check refuses on it.

**C3 — overlap-corrected statistics.** Effective sample size and confidence
intervals come from a **block bootstrap over calendar blocks with errors
clustered by date**. No bare percentage and no nominal *n* is reported anywhere.

**C4 — causal indicator computation.** ⚠ Not discharged by C-execution alone.
Two separate assertions: (a) the §2.1 equality — every order's timestamp and
price equal the stored `fill_bar_date` / `fill_price`, and
`fill_bar_date > signal_bar_date`, over the whole trade list; **and** (b) the
criterion-4 truncation test proper — recompute a mid-series bar from a truncated
series and assert equality against the full-series value, over the full
population, for every indicator any strategy reads. (b) is the one that catches
look-ahead *inside* an indicator, which (a) cannot see.

**C5 — out-of-sample hold-out.** The hold-out is a **separate result namespace
that is mechanically inaccessible** to exploratory queries — logging alone is
not the criterion, which says governance fails. Every access records timestamp
and strategy id. The frozen boundary literal must equal the corpus's
bar-weighted boundary or the run **fails** rather than re-splitting (§5.2).
Purge and embargo both act on the **training** side (§5.3) — asserted over the
full in-sample population by `scripts/verify_2240_walk_forward.py --folds` (F2,
F3), which restates both predicates from §5.3's wording rather than calling the
function under test. Every strategy entering walk-forward has a holding bound:
**declared** where §3 gives one (S-3: 10, S-4: 40) and **measured on the panel
axis per fold** otherwise, since a declared bound counts instrument bars and the
fold window counts panel dates (§5.3, §8.4). F4 asserts the measured bound
covers the declared one. Per-strategy in-sample and hold-out **trade** counts
are reported, gated on effective sample size (§5.2).

✅ **The split is now STORED, at stage 5e-5c** (`sql/269`,
`walk_forward.WalkForwardFolds`, `result_ledger.store_walk_forward_folds`,
`scripts/verify_2240_result_writers.py --split`). Four contiguous folds counting
one population, each carrying its block on both axes, its realised bar count,
its measured panel embargo and the four-way census — written whole or not at
all, and refused by trigger against a `hold_out` result, since every fold is cut
inside the in-sample side. ⚠ The evidence a criterion-5 auditor needs is the
census and the embargo, not a per-fold return; §8.8 records why no per-fold
metric ships and why no new promotion refusal was invented for it.

**C6 — multiple-testing control.** Deflated Sharpe computed with **all four**
parent inputs: the trial count, the trials' **correlation**, and the returns'
**skew and kurtosis**. The trial count is explicitly declared and includes
abandoned branches, manual eyeballing and discarded parameter values. An
undeclared trial count **fails**; it does not default to the number of shipped
strategies.

**C7 — a metric set that cannot flatter.** All **twelve** present on every
sleeve-level and portfolio-level result: expectancy per trade, profit factor,
CAGR, annualised volatility, Sharpe, Sortino, portfolio-level max drawdown,
exposure time, turnover, trade count, effective sample size, and return relative
to buy-and-hold. Drawdown and Sharpe are computed at **both** the sleeve and the
total-portfolio level (§5.4), never at signal level. A result missing any of the
twelve is incomplete, and a strategy failing to beat buy-and-hold after costs is
reported as not a strategy.

**C8 — `not_evaluable` reason codes.** The criterion-9 census is reported per
strategy from the ledger's closed vocabulary (`sql/255`), never collapsed to a
single total, and phase 5 adds no code of its own without flagging it as ours.
The §3.1 `superseded_open_position` count and §3.3's panel-vs-instrument
calendar divergence are reported alongside, since both are narrowings phase 5
introduces.

**C9 — quarantine exclusion is measured.** Excluded bar and trade counts and
shares are reported per strategy, **plus one sensitivity arm with conservative
handling** — defined here as: re-run with quarantined bars *admitted* at their
stored values rather than masked, and report the delta in every C7 metric. An
arm that cannot be defined is an arm nobody ran.

✅ **Shipped at stage 5e-5a** (`scripts/verify_2240_quarantine_sensitivity.py`,
arms `--census` and `--arms`, full population, exit-code gated). Two exclusion
channels are counted, not one: the masked FIELDS and the SERIES-level
fail-closed refusal, which never reaches a delta because it removes the series
from both arms. All **twelve** C7 metrics are compared per strategy, and a
metric null in one arm reports a STATE rather than a zero delta. The gate refuses
on `quarantine_arms_not_compared` and — deliberately — on nothing else: criterion
9 requires the exclusion visible, not small, and no rule anywhere fixes a
blocking magnitude (§8.5). ⚠ Measured against S-1 and S-3 only; the RANGE
verdict's exposure belongs to S-4, which does not run here, and the census
reports it so the gap is visible.

✅ **Stage 5e-5c stores the pair and reads the gate's input off it**
(`result_ledger.store_in_sample_arm_pair` / `store_holdout_arm_pair`,
`quarantine_arms_compared`). Both arms go in through one call, so a single-arm
result — the state this refusal exists for — is not reachable by writing one and
forgetting the other. ⚠ The refusal is unchanged and still fires on ABSENCE
only; nothing here introduces a magnitude.

**C10 — corporate actions declared.** `price_series_break` segments are **never
spanned** by a position — asserted, since C3's block bootstrap would otherwise
average across a discontinuity. eToro candles remain price-only execution data;
the research harness uses split-adjusted raw OHLC for signals, fills and levels,
and split-and-dividend-adjusted `adj_close` for wealth returns and buy-and-hold
(#2429). `return_basis` is part of the immutable result identity.

**C11 — strategy identity.** The identity hash covers code, params, universe,
cost model, **ranking tie-break** and **execution assumption**. Asserted by
mutating each in turn and requiring `version` to move — six assertions, not one.
⚠ The **position-sizing rule and the ambiguity arm** (§3.4, §5.4) are execution
assumptions and are hashed too; a sizing change that did not move the version
would let a different strategy inherit a track record.

**The harness itself.** The random-entry cohort is 1,000 strategies matched to
each real strategy's **universe, dates, exposure and turnover**, under the
**same cost model**, with the seed recorded. Acceptance is **both** parent
thresholds: the cohort's mean net return lies within its own 95% bootstrap CI of
zero, **and** each real strategy's Sharpe exceeds the cohort's **95th
percentile** to count as evidence at all. Plus §9.1.

✅ **Shipped at stage 5e-5b, and the cohort is run at N = 1,000 for S-3**
(`app/services/random_entry_cohort.py`,
`scripts/verify_2240_random_entry_cohort.py`, `sql/268`). The construction is a
**permutation**: per series, the realised trade count and the multiset of
holding periods are kept EXACTLY and only the entry ordinals are redrawn, inside
the same eligible-fill-bar space the real strategy was under (usable open,
inside the window, past the declared warm-up). So the trade count matches by
construction and is asserted rather than tolerated. ⚠ Both thresholds are
implemented literally, both are stored, and a strategy failing them is a RESULT
— §10 says so, and the verify script's exit code therefore gates the harness
properties and never the verdict. ⚠ §9.2 records what the first threshold turned
out to measure and why it is not satisfiable; §8.6 records the matching residual
that turned out to be the cohort's ruin rather than a mis-specified null, and
§8.7 the command for **S-1's cohort, which is compute-bound and did not run**.

### 9.1 ⚠ The parent's #2260 acceptance needs amending, and this is the amendment

Parent §5 requires: *"reproduce issue #2260's 76.8% figure, then attribute it to
criteria 1/3/4."* **That is now unsatisfiable as written.** #2260 closed
2026-08-05 because the figure **did not reproduce** — causal Wilder RSI gave
51.8% / 50.4%, and the "survivorship eliminated" claim was withdrawn.

Reproducing a figure that does not exist is not a gate. The amendment, which the
parent should carry:

> **Replaced by:** the harness must reproduce the **51.8% / 50.4%** causal-Wilder
> result on the same cohort, and must reproduce the **76.8% artefact** when
> deliberately run with the non-causal indicator that produced it. Recovering the
> bug on demand is the stronger test — it demonstrates the harness is sensitive
> to exactly the look-ahead class criterion 4 exists to catch, rather than merely
> agreeing with a number.

The random-entry synthetic control is unaffected and remains mandatory.

### 9.2 ⚠⚠ Stage 5e-5b measured the FIRST threshold, and it is not satisfiable by a cost-charged long-only null — the amendment, with the numbers

**What the threshold says.** Parent §5: *"the mean net return of the random
cohort must lie within its own 95% bootstrap CI of zero"*. Its stated purpose is
one sentence later: *"A harness that finds edge in noise is broken regardless of
what else it explains."*

**What it measures here.** Full population, 1,000 members, S-3
(`scripts/verify_2240_random_entry_cohort.py --report`; S-1's cohort did not
run — see §8.6):

```
cohort mean net return      -99.5935%     95% CI [-99.8620%, -99.0928%]
  contains zero                  NO        <- threshold 1 FAILS
cohort sharpe p95             0.0962      strategy 0.1430
  exceeds                       YES        <- threshold 2 PASSES
members >= strategy sharpe   0 of 1,000   empirical p 0.000999  (= the 1/(N+1) FLOOR)
members >= strategy return   1 of 1,000   empirical p 0.001998
```

**The cause is MEASURED, not asserted.** The obvious reading — *"a conservative
cost model doing its job"* — is a causal claim, and this repo does not get to
make one without checking it (`.claude/CLAUDE.md`; raised at Codex checkpoint 1,
which was right that the −99.59% could equally have been a placement bug, universe
drift or a mis-priced exit). The check is a **zero-cost ablation**: the identical
placements, the same seeds, the same entries and holds, with the half-spread set
to zero on both fill sides and on the rebalance
(`--cohort --zero-cost`, all 1,000 members):

| | costed cohort | **ablation `h = 0`** | real S-3 |
| --- | --- | --- | --- |
| mean net return | −99.5935% | **+48,048,234.36%** | +32.97% |
| mean Sharpe | −0.5321 (p50) | **+0.7191** | 0.1430 |
| mean exposure | 34.65% | **99.87%** | 85.92% |
| mean turnover /yr | 12.33 | **36.37** | 39.21 |

⚠ The cost model is the whole of it. ⚠⚠ And the same table settles a second
question the costed run appeared to answer wrongly — see §8.6.

⚠ **What the ablation does NOT establish.** The real sleeve was **not** re-run at
`h = 0`, so nothing here says anything about whether S-3's own edge survives its
costs, and the ablation's Sharpe being above the strategy's is not a comparison
anybody made. A zero-cost backtest violates criterion 2 outright and is never a
§9 figure; it exists to attribute one number and it is labelled as such in the
runner's own output.

**Amendment, recommended.**

> **(a) State that the null is NOT centred at zero, by construction.** A member
> is long-only, holds the strategy's own trade count, and pays a p75 round-trip
> spread at ~36 turns a year. Its mean net return is the corpus's drift over its
> exposure MINUS a cost that scales with turnover — a quantity with no reason to
> equal zero. Threshold 1 fixes the null's centre at a value the construction
> cannot produce, so it is not a test of the harness; it is a test of a
> coincidence.
>
> **(b) Replace it with a declared permutation p-value on the SHARPE**, at the
> parent's own 5%: `p = (1 + #{members >= strategy}) / (N + 1)`, refuse above
> 0.05, and report the same statistic for net return beside it. This keeps the
> parent's choice of Sharpe as the evidential statistic, uses the whole cohort
> instead of one order statistic, and is consistent with the Monte-Carlo
> permutation framing of Aronson (*Evidence-Based Technical Analysis*, 2006,
> ch. 6) and Masters (*Permutation and Randomization Tests for Trading System
> Development*, 2018). ⚠ Neither source is cited for this exact rule or for the
> placement measure — both are OURS and frozen in `COHORT_MODEL_ID`. ⚠ The
> statistic has a **resolution floor of `1/(N+1)` = 0.000999** at N = 1,000, and
> S-3 already sits on it; a smaller p is not purchasable without a larger cohort.

⚠ **Two proposals that were considered and REJECTED, with the reason, so they
are not re-proposed:**

- **A one-sided harness check** ("the cohort's mean must not lie ABOVE its
  interval of zero"). Rejected at Codex checkpoint 1 and the objection is
  correct: it would pass every harness that manufactures LOSSES — an
  over-applied cost model, a mis-priced exit, a forced bad fill, a sizing rule
  that under-invests — which is the same class of defect in the other direction
  and is not less likely. A calibration gate that only looks at one tail is not
  a calibration gate.
- **Reading the two percentile tests as a joint gate** ("Sharpe AND return each
  above p95"). It is an intersection rule with an unstated joint null and an
  effective alpha nobody declared. (b) names ONE gating statistic and reports the
  other.

**What ships at 5e-5b regardless.** The **literal** thresholds, as the gate.
`SyntheticControl.passed` is the conjunction the parent states, both halves are
stored, and the three promotion refusals fire on them. Nothing here is relaxed in
code — this section records a measurement and a recommendation, and adopting it
is a change to `passed` and to `sql/268`'s derived-verdict CHECK. ⚠ Adopting it
changes nothing operationally today: §6's stated initial state is that nothing is
promotable, and a survivor-only corpus with `carry_unmodelled` refuses every
result long before §9 is reached.

⚠ **What the cohort's bootstrap interval is.** Randomisation uncertainty
CONDITIONAL ON THE CORPUS. Every member trades the same price path; resampling
members measures how much the *placement* moves the mean, and nothing about how
much a different market would. Reading it as a confidence interval for "the
return of a random strategy in general" would attribute the corpus's own drift to
sampling noise.

---

## 10. What this document does NOT claim

- **No performance claim is made or implied.** S-1..S-4 each shipped
  deliberately without one; this spec is the machinery that would let one be
  made, and until stage 5e passes, it has not been.
- **The cost model is not validated against eToro's fee schedule.** It is
  calibrated from our own `quotes` snapshot, on 17.1% of the universe, at one
  hour of the trading day, with carry null (§5.1).
- **The corpus is survivor-only and every number inherits that** (#2284, #2288).
  The hold-out, embargo and synthetic control address *overfitting*. None
  addresses survivorship, and no arrangement of them can.
- **1962 is not 64 years of usable evidence for any strategy.** Median
  per-series depth is 3,072 bars, and **1,469 of the 6,735** universe
  instruments (6,735 − 5,266; M1 vs M12) have no corpus series at all. Depth is
  per-series, not per-corpus.
- **Nothing here establishes that any of S-1..S-4 works.** The most likely
  outcome of stage 5e, given §10 of the parent and a survivor-only corpus, is
  that some or all of them fail the random-cohort threshold. That is a result,
  not a failure of the phase.
