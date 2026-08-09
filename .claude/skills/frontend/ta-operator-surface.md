# frontend/ta-operator-surface

## When to use

Read before adding, renaming or removing **any figure, chart, control or label on the TA
managed-trading surface** (phase 6/7). Pair it with `design-system.md` (surfaces, badges,
chart theme), `operator-ui-conventions.md` (formatters, colour semantics) and the `dataviz`
skill (chart form + palette validation).

This file exists because the TA surface has a failure mode the other frontend skills do not
cover: **it is built by someone fluent in the backtest vocabulary, for someone who is not.**
Every metric here has a precise engineering meaning and a different, blunter question the
operator is actually asking. Rendering the first and calling it the second is the defect.

---

## The governing rule — show the OUTCOME, not the MECHANISM

The operator's question is almost always *"did it work, and what did it make me?"* The
system's answer is usually a mechanism — which exit path fired, which gate refused, which
arm the row came from. **Mechanism is a drill-through, never the headline.**

Precedent (2026-08-08, concept v6). A panel titled *"How trades closed"* rendered
`strategy_outcomes.resolution_method` verbatim as four bars:

```text
Hit its target · Stopped out · Signal said exit · Ran out of time
```

Operator response: *"Not sure what 3 of these mean."* Correct response. Three of the four
are the same outcome — **the trade did not reach its goal** — separated by a mechanism only
the parser cares about. The panel answered "by what route did the position close", a
question nobody asked, and buried the one that was asked.

The fix is not better labels. It is **collapsing to the outcome the user is asking about**,
and putting the mechanism one level down:

```text
Reached its goal      34
Didn't                66      → drill through for how: stopped / exited / timed out
```

**Test: if two rows would make the operator take the same action, they are one row.**

---

## Every figure passes three gates before it ships

Apply to each number, bar, line and axis label — individually, not to the panel as a whole.

1. **The sentence test.** Can a non-quant say what it means in one plain sentence, without
   the word it is named after? "Expectancy" fails; *"what an average trade makes you after
   costs"* passes. If the only available sentence restates the label, the label is jargon.
2. **The decision test.** Does it change what the operator would do? If the answer is
   "it's interesting", it belongs in the drill-through or nowhere. A page of true,
   inert numbers is noise with good provenance.
3. **The provenance test.** Name the column or the arithmetic. Every figure is either
   a stored column (`strategy_results_store.max_drawdown_pct`), a documented derivation
   (`trade_count − losing_trade_count` for win rate), or it does not ship. A figure with no
   source is invented, and this repo's whole posture is that inventing one is the
   cardinal defect.

A figure failing 1 or 2 is not deleted by default — it is **demoted** to the drill-through.
Failing 3 is deleted.

---

## Helper hover vs drill-through — pick by what's missing

Both exist so the surface can stay quiet. They are not interchangeable.

| The operator lacks… | Give them | Shape |
| --- | --- | --- |
| the **meaning** of a word | **helper hover** on the label | one or two sentences, ≤ ~220px, dotted underline on the label |
| the **composition** of a number | **drill-through** | a panel or tab, opened deliberately |
| the **mechanism** behind an outcome | **drill-through** | never a hover — it is a second question, not a definition |
| nothing — it is self-evident | neither | most figures; resist decorating them |

**A helper hover never carries a number.** The moment it does, it is data hiding from the
layout, and it will go stale independently of the figure it explains.

**Never spell out what the design already says.** A paragraph under a chart explaining how
to read the chart means the chart is wrong. Fix the chart.

---

## The metric inventory — what we hold, and what it is called on screen

Source of truth for names. If a new figure appears on the surface, add a row here with its
column; if it has no column, it does not ship. Verify column names against
`strategy_results_store` / `strategy_signals` / `strategy_outcomes` at write time
(grep-before-cite), not from this table's memory.

| Operator-facing name | Backed by | Gate 2 — why it earns its place |
| --- | --- | --- |
| **Made you** | actual strategy arm: exact-owned `trade_events.realized_pnl_usd` + active owned mark-to-market; shadow arm: `strategy_outcomes.gross_return_pct` (gross, labelled shadow) | the only figure most sessions need; actual and shadow are never pooled |
| **Worst dip** | `max_drawdown_pct` | what the operator *feels*; a strategy can end up and have halved on the way |
| **Success rate** | `trade_count − losing_trade_count` over `trade_count` | "how often did it work" — the question actually asked |
| **Per trade** | `expectancy_per_trade_pct` | decides whether it makes money at all; pair with success rate, never alone |
| **vs buy &amp; hold** | `return_vs_buy_and_hold_pct` | the catalogue's own bar: fail it and it is not a strategy |
| **Money working** | `exposure_time_pct` | answers "is my cash idle" |
| **Turnaround** | `bars_held`, or `periods_per_year ÷ turnover_annualised` | half the return calculation — a fast small edge beats a slow big one |
| **Captured** | allocated `strategy_funding_decisions` ÷ fired entry `strategy_signals` | the missed-opportunity number; a missing legacy decision is not funded/not evaluated |
| **Why skipped** | `strategy_funding_decisions.reason_code` (or explicit missing-decision state) | tells the operator which lever to pull (pot size vs strategy choice) |
| **Confidence** | `deflated_sharpe` vs its threshold | one bar against one line; never the four DSR inputs on the surface |
| Profit factor | `profit_factor` | drill-through — duplicates success rate + per trade for most readers |
| Sharpe / Sortino / ESS / deff / block length / trial count | the DSR + bootstrap columns | **drill-through only.** These are how we know, not what we know |

⚠ **Win rate alone is banned as a headline** — the catalogue is explicit that a 76%-win
strategy at 1:4 loses money. It ships only beside expectancy, never on its own.

---

## Naming — say the outcome in the operator's words

- Prefer the verb the operator would use: *made you*, *worst dip*, *money working*.
- Never surface an internal enum verbatim (`not_evaluable`, `carry_unmodelled`,
  `survivor_only`). Translate, and keep the enum in the drill-through for support.
- A refusal says **what is missing and who fixes it**, not the rule that fired.
- ⚠ **Numbers that are unknown are not zero.** `carry` and FX are `None` in `cost_model.py`
  by deliberate design. Render unknown as unknown; a zero here is a lie that flatters.

---

## Review loop — run this before any TA-surface PR

Walk the rendered page top to bottom. For **each section**, then **each figure inside it**:

1. Read the label aloud. Does it name an outcome or a mechanism?
2. Apply the three gates. Record demote / keep / delete.
3. If it needs explanation — is that a definition (hover) or a composition (drill)?
4. Check the figure against its column. Does the arithmetic still hold?
5. Check the **chart form** against `dataviz`: right form for the job, palette validated,
   hover layer present, no dual axis.
6. Check every **static string** for drift — version labels, counts and dates written by
   hand go stale silently. (Precedent: a footer read "Concept v5" on the v6 page, in the
   same pass that shipped v6. Compute it or omit it.)

Record the pass as a table in the PR description: figure · gate result · action. A pass
with no demotions is a pass that was not run — this surface accretes engineering detail by
default, because the people building it can read it.

## Live-activation state

Never infer strategy authority from the account-wide live flag. Read
`live_strategy_activation_available` and its named blocker from the strategy
overview. On a demonstrably demo-only connection, do not spend landing-page
space repeatedly explaining that activity is paper trading; keep the real-money
blocker in the API, audit detail and runbook. If the configured connection can
place real-money orders, show one page-level outcome banner and never render a
disabled live control as though it awaits only a click.

The Strategies landing view is a money workspace: total automated P&L, trading
capital working/reserved/available, then one summary row per strategy. Do not
render an empty P&L chart before the first close. A row pairs success rate with
average return, then gives time to outcome, recent signal frequency and enabled
state. Observed results take precedence; until they exist, representative
backtest figures must be visibly labelled and must not be mixed with observed
figures in one aggregate. Expand at most one strategy's evidence windows inline.
Instrument-level events belong in a separate Activity view, filtered to one
selected strategy and bounded to 15 rows per page; evidence expansion never
loads the instrument ledger.
The overview freshness check must use the ingest-maintained series census, not
aggregate the full research bar heap or justify a large page-only index.
