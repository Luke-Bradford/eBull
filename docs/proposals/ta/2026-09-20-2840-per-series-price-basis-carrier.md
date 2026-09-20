# #2840 — the per-series price-basis carrier: the question cannot be answered ahead of the CERTIFIER

**Status:** findings. Nothing frozen, `r9` unmoved, no code path changed. One pure-logic test
lands with this doc to pin the measurement in §3.3.

**The question as inherited:** how does trusted loader metadata reach identity construction
AND per-bar evaluation, and what refuses on a mismatch?

**The answer:** it does not, because **no component certifies the delivered bars**. This
session drafted a design that routed an existing value into the rule, Codex ckpt-1 refused it
(39 findings), and the refusal holds: the three candidate sources are each disqualified for a
stated reason, two of them by measurement. What does exist is the raw evidence a certifier
would read — `strategy_intraday_bars.captured_at` — and the measurement below is the first
statement of how much of the store it would certify.

Upstream: `949b8d55` decided *where price provenance lives in the identity* (a sixth
`INPUT_RULE_SETS` entry, **not yet installed** — the mapping still has five members:
`indicator_series`, `market_regime_provider`, `series_termination`, `universe_selection`,
`price_quarantine`) and recorded the limit this doc takes up: *a rule-set hash versions the
RULE, not the DELIVERED SERIES.*

## 1. The draft, and the headline that inverted

**Drafted:** `cost_model.PriceBasis` is already resolved per run (`_Corpus.cost_price_basis`)
and threaded to four costing sites in `backtest_run`, so "the carrier is not missing, it is
mis-routed" — thread it into `s12_signals` as a required keyword and re-gate the existing
refusal on it.

**Refused, and the refusal is right.** The single sentence that kills it, verified in the
source rather than taken from the review:

> **control** — `replace(corpus, cost_price_basis="split_adjusted")`. That is exactly the
> literal #3238 removes, applied through the shipped code path, so the control is the OLD
> BEHAVIOUR REPRODUCED and not simulated.
> — `scripts/ab_3238_cost_basis.py:20-22`, and the substitution at `:355`

`cost_price_basis` is a **charging policy that is deliberately overridden**, independently of
any claim about the prices. Gate S-12's signals on it and the #3238 control arm stops
trading instead of being charged differently — the A/B would then compare *trades charged the
old way* against *no trades at all*, which is not the comparison it declares. A value whose
whole purpose is to be substitutable cannot also be the fact a refusal reads.

## 2. Three candidate certifiers, three disqualifications

| candidate | what it actually is | why it cannot certify the delivered bars |
| --- | --- | --- |
| `cost_model.cost_price_basis` (`cost_model.py:540`) | a **charging** policy, total and fail-closed over the stored label | deliberately overridden by `ab_3238_cost_basis.py:355`; and it maps adjusted, unknown, unrecognised and missing to one token, so it cannot distinguish *adjusted* from *unknown* |
| `research_price_series.adjustment_basis` (`sql/249:86-89`) | the **stored** label on a pinned research archive. ⚠ **FOUR** members — `unadjusted`, `split_adjusted`, `split_and_dividend_adjusted`, `unknown` — not two | covers pinned archives only; the composed forward series is neither of them. And a label does not validate a payload: S-12's own docstring, *"a stored `unadjusted` label on rescaled data satisfies all three"* |
| masking (`price_masked_bars` → `BarSeries` field = `None`) | a per-field **absence**, read from the quarantine verdict tables | **measured terminal, not bar-local** — §3.3. And `_LOAD_SQL` reads coverage and range/return usability; it reads no scale and no capture evidence, so a clean quarantine verdict says nothing about nominality |

## 3. What the draft got structurally wrong, in three parts

### 3.1 The delivery channel is the UNIFORM protocol, not an S-12 keyword

`strategy_manifest.PerSeriesSignals` is a uniform `__call__(series, *, universe,
masked_reason, regime)`, and the module states the rule for exactly this situation:

> ⚠⚠ `regime` IS ON THE UNIFORM CALL, NOT ON THE STRATEGIES THAT USE IT. S-1..S-4 ignore it;
> S-5..S-10 gate on it. … The alternative — a `requires_regime` flag with a runner branch —
> reintroduces exactly the per-strategy `if` this module exists to delete.

Both runners dispatch through `strategy_segmented_evaluation.segmented_signals`
(`:63`), which calls `entry.signals(segment, universe=…, masked_reason=…, regime=…)`. An
S-12-only keyword is undeliverable through that call. **If a basis argument is added, it goes
on the uniform protocol and every adapter absorbs it** — the precedent is `regime`, and it is
a wider diff than the draft priced.

### 3.2 The universe gate would still refuse the thing this is all for

`AS_TRADED_UNIVERSES = {"survivorship_free"}` and `SCAN_UNIVERSE = "survivor_only"`. A
composed forward series carrying a perfect certificate, selected under `survivor_only`, is
still refused by the outer gate before any basis is read. So "keep the declaration as the
outer half" — which the draft proposed — **blocks the motivating case**. The universe
overloading `949b8d55` identified is not relieved by adding a second gate beside it; one of
the two has to stop meaning price basis.

### 3.3 Masking is DELIBERATELY terminal, so it cannot carry a per-bar fact

**Measured** (pure logic, no DB — pinned by `tests/test_2840_masking_cannot_carry_per_bar_provenance.py`):
a 200-bar series, one close masked to `None` at index **120**, `atr_series(period=14)`:

| arm | `None` values |
| --- | --- |
| clean | 14 (indices 0-13, the warm-up) |
| one masked close at 120 | **94** |
| new refusals caused by that one bar | **80 — indices 120…199, recovering never** |

And this is a **decision, not a defect**. `atr_series` computes
`unevaluable.extend(range(first_null, len(rows)))` (`indicator_series.py:469`), and
`adx_series`' docstring states the rule in terms:

> ⚠ FAIL-CLOSED FROM THE FIRST MASKED FIELD … Wilder smoothing is recursive: a gap does not
> affect one value, it shifts every value after it. Resuming past a hole would produce
> numbers that look valid and are not, so everything from the first unusable bar is
> `not_evaluable`.

⇒ A per-bar provenance carrier **must not enter the indicator recursion**. Masking is the one
mechanism in this repo that expresses a per-bar fact, and its contract is the opposite of
bar-local. Two consequences the draft missed:

- **Masking the close does not protect the fill or the exit.** An entry on the preceding
  certified bar still fills at the next bar's unmasked `open`, and an open position still
  consumes its `high`/`low`. Field coverage is a separate obligation from signal refusal.
- **A single `masked_reason` cannot carry two causes.** A composed series can hold quarantine
  failures and uncertified levels at once; one code relabels the other.

## 4. The certifier that DOES exist, and how much it certifies

`strategy_intraday_bars` carries `captured_at`, populated on **29,234 of 29,234** rows
(`1m` 6,615 · `5m` 16,083 · `30m` 6,536). That is the evidence a certifier would read: a bar
captured while its own session was still running cannot have been retroactively adjusted,
because no adjustment had yet occurred.

**Measured, whole store, read-only (`SET TRANSACTION READ ONLY`), 2026-09-20:**

```sql
SELECT timeframe,
       CASE WHEN captured_at - bar_time <= interval '2 hour' THEN 'le_2h'
            WHEN captured_at - bar_time <= interval '1 day'  THEN 'le_1d'
            ELSE 'gt_1d' END AS bucket,
       count(*), count(DISTINCT instrument_id), count(DISTINCT bar_time::date)
FROM strategy_intraday_bars GROUP BY 1, 2 ORDER BY 1, 2;
```

| timeframe | ≤2h (contemporaneous) | ≤1d | >1d (backfilled) |
| --- | ---: | ---: | ---: |
| `1m` | 6,226 | 0 | 389 |
| `5m` | 9,651 | 38 | 6,394 |
| **`30m`** | **1,686 (25.8%)** | 8 | **4,842 (74.1%)** |

Max `30m` lag: **124 days**. Min: exactly **1,800 s** — which is itself evidence that
`bar_time` is the bar's START and the collector writes at its close.

⚠ **This is not the 16/29 figure and must not be reconciled with it.** `ff8f2311` measured
**sessions**, post-activation, on the seven liquid members, under composition ∩ nominality ∩
timeliness. This is **bars**, whole store, both panel versions, all dates, on capture lag
alone. Different denominators, different predicates.

⇒ **Per-bar certification is the majority case, not an edge case.** Three quarters of the
stored 30m corpus arrived after the fact. A series-level token cannot express that, and §3.3
shows the one per-bar mechanism that exists cannot either.

## 5. Corrections — claims withdrawn from my own draft

| claim | status |
| --- | --- |
| "the carrier is mis-routed, not missing" | **WITHDRAWN** — `cost_price_basis` is a charging policy, deliberately substituted by the #3238 A/B; and it does not reach the composed source at all |
| "`cost_model.PriceBasis` is the governing vocabulary" | **WITHDRAWN** — it collapses *adjusted*, *unknown* and *missing* into one token, the defect I attributed to the alternative |
| "`AsTradedPriceBasis` is the wrong GRAIN" | **WITHDRAWN** — a `Literal` has no inherent grain; its snapshot use does not preclude series carriage |
| "`research_price_series.adjustment_basis` has two members" | **WRONG** — four (`sql/249:86-89`), and its comment warns explicitly against treating `unknown` as adjusted |
| "the gate and the charge can never disagree" | **WRONG** — S-12 gates on `close(t)`, costing bands off `open(t+1)`; a gap across the edge changes the band whatever the vocabulary |
| "no `adjustment_basis` column ⇒ no basis can be resolved" (live path) | **OVERREACHED** — the schema absence is real (measured: only `research_price_series` and `research_comparator_snapshots` carry the column) but passing the fail-closed value is a POLICY choice, not a conclusion forced by the schema |
| "masking carries the per-bar fact" | **FALSIFIED BY MEASUREMENT** — §3.3, and it is a documented decision |
| "S-12 only; every other rule compares prices to prices" | **TOO BROAD** — uniform rescaling is invisible, but a split boundary is PIECEWISE rescaling and moves returns, ATR, compression ranks and breakouts for S-4 too |
| "`source_hash` is in `S12_PARAMS`" | **WRONG** — it is a separate `StrategyIdentity` field. The conclusion (an S-12 edit rotates S-12) still holds |
| "a rotation detaches 93 signals + 4 watermarks" | **WRONG SCOPE** — that census measured a REGISTRY-WIDE rotation, and it compares every table against scan versions while backtest result versions are deliberately disjoint (`strategy_result_identity.py:32`). It does not price an S-12-only edit |
| "before the freeze is free" | **TOO STRONG** — freeze scripts pin an identity already registered in the trial register (`freeze_2840_…:47`), so a rotation between REGISTRATION and freeze strands it too |
| "no live-scan behaviour change" | **TRUE ONLY OF VERDICT CONTENT** — a new identity gets a new watermark and cold-starts (`strategy_signal_scan.py:349`), so stored rows and catch-up calendars do change |
| "the refusal becomes causally correct" | **CONTRADICTED** by keeping the outer gate — see §3.2 |

Citation fixes: `AsTradedPriceBasis` is `strategy_decision_context.py:31`; the filter/warm-up
quote is `price_masked_bars.py:14-20`; the `BarSeries` construction split is **66 tests / 20
scripts / 8 app** of 94 on `origin/main` (95 with this branch's test file), not 59/12/23; the
127 `universe` matches are textual occurrences against **115** annotated signatures. All
three recounted by AST walk rather than `rg`, which is what produced the wrong split.

## 6. What the next session builds, in order

1. **Define the certifier, because nothing else can be specified without it.** One sentence
   it owes: *what evidence certifies that a delivered bar's level is the level it traded at.*
   The only such evidence in the store is `captured_at` relative to the bar's own session
   (§4). Its output is **per bar**, and §3.3 fixes the constraint: it must be readable by a
   gate without entering the indicator recursion.
2. **Then, and only then, the carrier** — on the uniform `PerSeriesSignals` call (§3.1), and
   a parallel per-bar structure rather than a mask.
3. **Then the universe overloading** (§3.2) — one of `AS_TRADED_UNIVERSES` / `SCAN_UNIVERSE`
   must stop meaning price basis, or the certificate is unreachable.

**Obligations recorded, not resolved:** nominality ≠ availability (a nominal but late bar must
still refuse a historical decision, and neither candidate vocabulary says so) · a composed
panel has no homogeneity invariant, unlike archive loading which withholds on mixed bases ·
the coverage join omits uncovered dates entirely, so "never dropped" needs an expected-session
policy the loader does not provide · refusal rows are stored and terminal, so a delivered
basis that turns refusal into evaluation under one identity collides on the ledger key ·
`verify_2900_point_in_time.py:252` pins `price_basis=corpus.cost_price_basis` at exactly 4
occurrences and any wiring change must revise it while keeping all four charge consumers
covered.

## 7. Not claimed

- That `captured_at` within the session IS a sufficient certificate. It is the only candidate
  evidence that exists; whether a contemporaneous capture can still be revised is untested,
  and `sql/387`'s revision mechanism is the place to look.
- That eToro back-adjusts. `91267518` left that UNVERIFIED deliberately and this doc does not
  move it.
- That the composed forward series exists. It does not.
