# S-E: 10-month-SMA overlay on the passive core — preregistration

Date: 2026-08-22
Status: FROZEN before first look. No return, drawdown or CAGR of this rule has been read.
Parent: #2837. Part of #2832. Refs #2437, #2599, #2600, #2829.

Contract version: `se-ma-overlay-2026-08-22`
Trial identity: `se-ma-overlay-drawdown-insurance` @ `se-ma-overlay-drawdown-insurance-v1`

> Revised once before freezing, after a Codex checkpoint-1 pass over the first draft
> (2026-08-22). The pass returned 85 findings; the resolutions are §12. No outcome was read
> in between — every fact used to revise is a date, a row count or a column's presence.

## 1. The claim, and the claim it is not

The alpha claim is dead and stays dead. #2837 records the reasons: Zakamulin's
moving-average timing work finds no significant out-of-sample outperformance under
realistic costs, GTAA's live record concurs, and UK CGT pushes expected excess CAGR below
zero because a flip realises a gain buy-and-hold defers indefinitely. Those attributions
are the ticket's; this document does not re-derive them and does not rest on them.

What survives on both sides of that literature is the **drawdown-reduction** property. The
only admissible claim here is: **a bounded CAGR cost, bought as drawdown insurance.**

If it passes, it books as an optional risk overlay on the passive core (S-A, #2833) — an
operator insurance-preference decision. It is **never a strategy promotion and never enters
the strategy funnel**. Excess-return significance is not testable on one index path, is not
the claim, and no test of it is declared, reported or run.

## 2. Why this trial is unpromotable before it starts, structurally

`structural_promotion_refusals` (`app/services/strategy_result.py:1187`) is the freeze-time
subset of `check_promotable`, and `PROMOTABLE_UNIVERSE_BASES`
(`strategy_result.py:216`) has exactly one member, `survivorship_free`. The honest stamps:

| stamp | value | why |
| --- | --- | --- |
| `declared_universe_basis` | `single_index_proxy` | one index chain, not a survivorship-free cross-section |
| `declared_carry_unmodelled` | `true` | cash below the SMA earns **zero**; cash yield is carry and is not modelled |
| `declared_fx_unmodelled` | `true` | the chain is USD, the account is GBP, and no frozen 1993→2026 GBP/USD series exists here |

Those produce `universe_basis_not_survivorship_free`, `carry_unmodelled` and
`fx_unmodelled`. `prereg_purpose` is therefore `falsification_only`, and the requirement
that this can never promote is obtained by **stamping honestly**, not by anyone remembering
the rule later.

⚠ `single_index_proxy` does not name the whole hazard, so it is named here. SPY is not a
cross-section, so there is no constituent survivorship in the usual sense — but SPY was
chosen in 2026 as the proxy that survived and won, and the 10-month rule is itself a
survivor of decades of published moving-average variants. Both are **selection hazards on
the proxy and on the rule**, and neither is corrected for. They are a further reason the
result cannot promote, not a reason to discount the drawdown reading.

The run still charges the shared trial register, as any look at price data must (#2600
D-0.1). Its entry is `se-ma-overlay-2026-08-22` in `app/services/trial_register.py`.

## 3. Frozen data source

`spy_chain_v1`, exactly as `app/services/market_regime_provider.py` defines it, obtained
through its public `load_research_closes` so that the merge has **one** implementation
shared with the regime classifier: the `icyDenev/Intrader` SPY segment strictly before the
frozen seam `2022-05-10`, the `etoro/etoro-comparators-2026-07-08-v1` SPY segment on and
after it, `close` only, one seam. Every refusal that module raises — basis drift, an eroded
fallback, a missing seam bar, a non-increasing date, a non-positive close — is a refusal
here. The measurement does not degrade to a shorter chain.

**The chain's extent is frozen, not derived.** Measured 2026-08-22, dates and row counts
only, no close read:

| | value |
| --- | --- |
| first chain bar | `1993-01-29` |
| last chain bar | `2026-07-08` |
| chain bars | `8391` |

The measurement **refuses** if any of the three has moved. A corpus refresh that extends
either segment changes the tested span, and a contract that silently absorbs that is a
contract that means something different each time it runs.

### 3.1 Price return, and the dividend drag that is charged for it

The chain is **price return**. `adj_close` is present on all 7,973 fallback bars (and
differs from `close` on 7,967 of them, so it is distribution-adjusted) but on **0** of the
1,018 primary bars — measured 2026-08-22 — so a total-return chain cannot be built across
the seam and is not attempted.

Both arms are measured on the same price series, so the drawdown leg is unaffected by the
choice. The **CAGR leg is not**: an overlay that sits in cash forgoes distributions a
price-only series never charges it for, which flatters it. That drag is therefore charged
rather than noted:

> `dividend_drag_pp_per_year = f × y`, deducted from the overlay's net CAGR before §8's
> second leg is evaluated, where `f` is the fraction of the arm's measured calendar span
> spent in cash and `y` is SPY's realised annual distribution yield **measured from the
> fallback segment's own `adj_close`/`close` ratio** over `1993-01-29 … 2022-05-09`.

`y` is sourced, not chosen: it is the segment's own annualised total-return-minus-price-
return spread. It is **not** computed here — the method is what is frozen; the number is
produced by the measurement run. Applying a 1993–2022 yield to the 2022–2026 tail
overstates the recent drag, because SPY's yield fell over the span; overstating the drag
makes §8's bar harder, which is the conservative direction. Buy-and-hold is charged nothing
here, because it forgoes nothing.

## 4. Frozen rule

Let `k` be the evaluation offset in **chain-bar positions**, `k ∈ {0, 5, 10}` (§5).

1. **Month-end bars.** The last chain bar of a calendar month, where a later chain bar
   exists in a **subsequent** calendar month. ⚠ The trailing partial month contributes no
   month-end: the chain ends `2026-07-08`, and treating that as July's month-end would
   invent a decision the calendar never offered.
2. **Evaluation dates.** For offset `k`, the chain bar `k` **positions** after each
   month-end bar (`k = 0` is the month-end bar itself). Positions, not trading days — the
   chain is the only calendar this measurement has, and no exchange calendar is frozen, so
   "+5" means the fifth subsequent chain row. A month-end whose shifted bar does not exist
   contributes no evaluation date. The measurement **refuses** if the shifted dates are not
   strictly increasing and unique, or if any evaluation date is also an execution date
   (§4.5) — both are structurally impossible on a chain with ≥12 bars per month, and a
   silent collision would double-count a close in the SMA.
3. **Signal.** `SMA10` is the arithmetic mean of the last 10 evaluation-date closes
   **inclusive of the current one**, in IEEE-754 float64, with no rounding at any stage.
   Hold the index when `close > SMA10`; hold cash otherwise. Strictly greater, no tolerance:
   an exact equality is cash. All ten inputs must be finite and positive or the measurement
   refuses.
4. **Warm-up.** Before 10 evaluation dates exist there is no `SMA10` and the position is
   **held**. The overlay is insurance on an already-invested core: it may only remove
   exposure the core has, never add exposure the core lacks. Warm-up creates no trades — it
   is one continuously invested state, not ten hold decisions.
5. **Execution.** The decision taken at evaluation date `t` executes at the **close of the
   next chain bar** after `t`. The return from `t`'s close to that execution close belongs
   to the **old** position; the new position owns from the execution close forward, up to
   and including the next execution close. A decision with no next chain bar is
   **discarded**, not carried. An unchanged target places no trade and charges nothing, but
   still ends one holding interval and starts the next.
6. **Both arms' clock.** The equity curve starts at the **first execution date** of the arm
   and ends at its **last execution date**. Chain bars after the last execution are not
   valued. Buy-and-hold is measured over **that same arm's span**, so each offset's ratio is
   like-for-like; a single full-chain buy-and-hold compared against three differently-
   spanned overlays is the comparison this clause exists to forbid.
7. **Valued on every chain bar in that span, not sampled monthly.** The position is
   piecewise constant between execution dates, but equity is marked on each chain bar. Two
   reasons, both material: a monthly-sampled drawdown **understates** the real one, which
   would flatter the very leg §8 tests; and the 31 January tax outflow (§6) is a calendar
   date that is usually not an execution date, so a monthly curve could not place it. A tax
   date falling on a non-trading day is charged at the first chain bar on or after it.
8. **Inception charges nothing, on either arm.** Both start invested — buy-and-hold by
   definition, the overlay by §4.4's warm-up — so the opening purchase is common to both
   and charging it would be theatre. Spread is charged only on a *change* of position
   (§6).
9. **Cash earns zero.** Declared, and stamped `carry_unmodelled`.
10. **Single frozen spec, no parameter search.** The lookback is 10 months and nothing else.
   A second lookback, a second offset set, a band, a confirmation delay or a different proxy
   is a NEW declared search charging the register again — not a refinement of this one.

## 5. Fragility

The identical measurement is repeated at three evaluation offsets — `k ∈ {0, 5, 10}` — and
the pass bar (§8) must hold on **all three**. One offset passing is not a pass.

⚠ These three are **a robustness screen, not three replications.** They are overlapping
paths through one index history and share almost all of their information; nothing here
treats them as independent evidence, and no statistic is pooled across them.

If any arm yields fewer than 10 evaluation dates, no execution, or a zero-length span, the
measurement **refuses** rather than reporting a degenerate arm.

## 6. Costs

**Spread.** 0.322% round trip, charged as 0.161% of transacted value on each side. A
held→cash transition charges once; cash→held charges once.

**CGT.** UK, £50,000 opening account, higher-rate taxpayer assumed throughout (the 24% rate
is the higher rate; a basic-rate taxpayer would pay 18% and the drag would be smaller — the
assumption is stated because the rate is not a property of the strategy).

Source rule: TCGA 1992 s.104 (the pooled §104 holding) and s.106A (share identification
order: same day, then acquisitions in the following 30 days, then the pool), and HMRC's
self-assessment payment date of 31 January following the tax year. The in-repo
implementation of the same rules is `app/services/tax_ledger.py`; `ANNUAL_EXEMPT` there is
the £3,000 used below and `_CGT_RATE_PERIODS` carries the 24% higher rate from 2024-10-30.

- A held→cash transition is a **complete disposal**. Gain = proceeds − the §104 pool cost,
  the pool being the amount invested at the preceding cash→held transition; the position is
  all-in or all-out, so every disposal empties the pool.
- Gains and losses are summed per UK tax year (6 April–5 April) by **disposal date**. Net
  losses carry forward indefinitely against later gains; the annual exempt amount does not
  carry forward.
- Taxable = `max(net gains − £3,000, 0)`, taxed at **24%** across the whole span. Applying
  today's rate to 1990s disposals is the worst case and is #2837's own declared treatment;
  it is not a claim about historical rates.
- The charge is a **cash outflow on 31 January following the tax year**, deducted from
  equity on that calendar date regardless of what the execution schedule is doing — not on
  the next execution bar, which could defer it by weeks.
- **Funding, declared:** the outflow is taken from equity directly. If the arm is invested
  on that date it is economically a partial sale, and this measurement charges **neither**
  the 0.161% spread on it nor a second CGT event on the sliver disposed. Both omissions
  understate the drag; both are bounded by 0.161% of a payment that is itself a small
  fraction of equity, i.e. single basis points over the whole span, and both are recorded
  here rather than discovered later.
- **Terminal accrual.** Any liability arising from disposals whose 31 January payment date
  falls after the arm's last execution date is **accrued and deducted at the last execution
  date**, so no tax escapes by falling off the end of the sample.
- **Buy-and-hold pays no CGT.** It never disposes. That is the asymmetry the literature
  names and it is deliberately left in: it makes the overlay's bar harder.

⚠ **The 30-day rule is NOT applied — declared here, not discovered later.** On a monthly
clock same-day matching can never fire, and 30-day matching can fire only where a cash→held
re-entry falls within 30 days of the preceding exit. Ignoring it taxes every realised gain
in full against the pool, the conservative direction for a bar that must clear a CAGR
floor. §9 reports how many exits had a re-entry inside 30 days, so the size of the
simplification is visible rather than assumed.

## 7. Declared sensitivity

One, declared here so it is preregistered rather than found: the primary comparison leaves
buy-and-hold's terminal gain unrealised (§6). The **symmetric** variant — both arms
notionally liquidated at the **last execution date** (§4.6), the resulting gain taxed under
§6's rules and accrued immediately at that date rather than the following 31 January — is
reported alongside. It does not move the pass bar; the pass bar is §8 and only §8.

## 8. Pass bar

Definitions, frozen so the comparison cannot be reversed by a sign:

- **Max drawdown** is a **non-negative loss magnitude in percent**, computed by
  `strategy_statistics.max_drawdown_pct` and negated to a magnitude, over the arm's
  **chain-bar** equity curve (§4.7) **after all costs and tax cash flows** (§6), starting
  from the opening £50,000 at the first execution date (§4.6). An unrecovered terminal
  drawdown counts at its trough to date.
- **Net CAGR** is annualised over the arm's own span in §4.6, from opening equity to
  closing equity, after costs and tax; the overlay's is then reduced by §3.1's
  `dividend_drag_pp_per_year`.

On **all three** offsets:

1. `overlay_max_drawdown ≤ (2/3) × buy_and_hold_max_drawdown` on the same arm's span; and
2. `overlay_net_CAGR − buy_and_hold_net_CAGR ≥ −1.5` percentage points per year.

⚠ If `buy_and_hold_max_drawdown` is zero on any arm, leg 1 **fails** on that arm: an
overlay cannot evidence insurance against a loss that never happened.

Fail either leg, at any one offset → **drop entirely. No re-tuning.**

## 9. Reported measures

Per offset, and reported whether the arm passes or fails:

- flip count; the fraction of span in cash (`f`, §3.1) and the resulting dividend drag;
- overlay and buy-and-hold max drawdown, and their ratio;
- **each arm's three worst drawdown episodes**, where an episode runs peak → trough →
  recovery to that prior peak, episodes are ranked by depth, nested drawdowns inside one
  unrecovered episode are one episode, and a terminal unrecovered episode is included with
  its trough to date and flagged as unrecovered;
- the count of drawdown episodes ≥15% on each arm — the class §10's floor is expressed in,
  reported here so the floor's unit is measured rather than only assumed;
- net CAGR both arms, the raw delta and the delta after the dividend drag;
- **March 2020, predeclared fields**: every evaluation date, position, execution date and
  trade in `2019-12-01 … 2020-12-31`, both arms' equity at each, and each arm's peak,
  trough and recovery date for the episode;
- the count of exits with a re-entry inside 30 days (§6);
- the count of SMA windows spanning the `2022-05-10` seam. ⚠ The two segments carry
  different `adjustment_basis` values, which the module's own header addresses — SPY has no
  split history and the 585-date overlap agrees to a max $1.76 (~0.3%) vendor close-mark
  difference — but a window straddling the seam is where any residual level step would
  enter a signal, so the count is surfaced rather than assumed away.

**Per-regime cohort readout, never one pooled number.** Each holding interval's return is
assigned to the regime `classify_regimes` gives for **the evaluation date that decided that
interval**, read through `MarketRegimeProvider.load_research` — the same classifier every
other consumer uses, not a second definition. Warm-up bars the classifier cannot verdict
are their own cohort, never folded in. ⚠ The regime is **descriptive only**: it partitions
the readout and never enters the signal, the position, the costs or the pass bar, so its
use of contemporaneous closes cannot leak into a trading decision.

## 10. Forward-shadow floor, and exactly what it claims

**This is not a power calculation and does not claim to be one.** No published power
formulation exists for a drawdown-ratio claim, and none is invented. What §10 fixes, by
construction, is the calendar length over which the readout unit §9 uses could be expected
to reappear forward — that and nothing more.

- the claim is a path statistic, so the smallest unit of forward evidence is a completed
  drawdown episode of the declared class (≥15%, §9's definition);
- §9's readout unit is **three** worst episodes, so three is the smallest forward sample
  readable the same way the primary is;
- #2837 declares, before any look, **~404 month-ends** carrying **~7 episodes ≥15%**. Those
  are the ticket's own priors, not this run's outcome.

```
min_independent_decision_dates = ceil(3 × 404 / 7)               = 174
min_calendar_weeks             = ceil(174 × 365.25 / (12 × 7))   = 757   (~14.5 years)
```

The month→week constant is the one `scripts/freeze_2616_precutoff_declarations.py` already
uses. Three honest limits, stated rather than left for a reader to find:

1. **174 is an expectation, not a guarantee.** At the ticket's declared rate three episodes
   are *expected* in that span; they are not *assured*, and a drawdown starting inside it
   may still be unrecovered when it ends.
2. **The dates are not independent.** The schema field is named
   `min_independent_decision_dates` and `ForwardShadowFloor`'s own docstring already says
   the narrow claim is only that a distinct-date count cannot be inflated by same-day
   fan-out — *"NOT that the dates are statistically independent of each other, which they
   are not"*. Consecutive 10-month SMAs share nine inputs and positions persist across
   months. The same applies to the episodes: drawdowns cluster by regime, so `404/7` is a
   long-run average and not an exchangeable arrival rate.
3. It follows that clearing this floor would be **necessary and nowhere near sufficient**
   for a forward claim.

The trial is `falsification_only` regardless, so the floor gates nothing it was not already
gating. It records honestly what forward validation would have cost.

## 11. What a fail means

A fail is terminal for S-E. Not a smaller lookback, not a confirmation band, not a
different proxy, not a different offset set — each of those is a new declared search
charging the register again, and #2827 already measured this family failing the deflation
bar by 5–20× at zero cost. The recorded lesson goes on #2837 and the ticket closes.

## 12. Codex checkpoint-1 resolutions

The pass returned 85 findings over two runs. Every one is resolved into the text above, or
rebutted here. Resolved by specification: the incomplete terminal month (§4.1); "trading
days" meaning chain positions (§4.2); shifted-date collision, uniqueness and
evaluation/execution disjointness (§4.2); float64, no tolerance, finite-input refusal
(§4.3); warm-up creating no trades (§4.4); execution price, interval ownership, discarded
tail decision, no-op trades (§4.5); the terminal valuation clock and per-arm buy-and-hold
span (§4.6); degenerate-arm refusal (§5); the higher-rate assumption, the 31 January cash
flow, tax funding, terminal accrual and the s.104/s.106A citations (§6); the symmetric
sensitivity's timing and "last execution date" (§7); drawdown sign convention, the
zero-denominator case, the after-tax curve and precision (§8); episode segmentation,
recovery, nesting and ties (§9); the ≥15% class as a reported measure (§9); March 2020's
predeclared fields (§9); the regime cohort's assignment rule and its descriptive-only
status (§9); the frozen chain extent (§3); the public accessor for the chain closes (§3);
the seam-spanning SMA windows (§9); price-vs-total return, now charged (§3.1); the
proxy- and rule-selection hazards (§2); the floor's three limits and its non-power status
(§10); the offsets as a screen rather than replication (§5).

Rebutted, with reasons:

- *"A completed month cannot be established without lookahead."* It can: §4.1 requires a
  later bar in a **subsequent** calendar month, which is a fact about bars that already
  exist at execution time. The execution bar is that later bar.
- *"No trading calendar is frozen, so missing sessions cannot be told from holidays."*
  Correct and deliberate. §4.2 defines offsets in chain positions precisely so no calendar
  is needed; the frozen bar count (§3) is what catches gross erosion.
- *"The vendor rows are not content-hashed, so history could change under the contract."*
  A full content hash is the right answer for an archive and is not in scope for this
  ticket; the frozen first bar, last bar and bar count catch extension, truncation and
  wholesale reload, which are the failure modes with a plausible mechanism here.
- *"The literature claims are not auditable."* They are #2837's, and §1 now attributes them
  there rather than restating them as findings of this document. Nothing in §8 rests on
  them.
- *"Three historical episodes do not justify three forward episodes."* Agreed, and §10 no
  longer claims it does — it claims a calendar length and names what that is not.
