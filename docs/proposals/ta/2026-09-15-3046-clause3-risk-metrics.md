# #3046 clause 3 — `risk_metrics` as first consumer: SPECCED, then WITHDRAWN at checkpoint 1

Status: **withdrawn proposal, retained as the record of why.** Scope: #3046 build item 1.

Revision 2. Revision 1 proposed adopting clause 3 at `risk_metrics` by adding a
quarantine-aware break to `simple_returns`. Codex checkpoint 1 falsified its central
premise. **Do not implement revision 1.** Everything below is the corrected model; the
numbers are reproduced from the dev DB and each names its query.

## What revision 1 claimed, and why it was attractive

`risk_metrics` looked like the ideal first consumer for the contract decided in
`2026-09-15-3046-transition-verdict-contract.md`:

- `_load_closes` (`app/services/risk_metrics.py:1392-1407`) has **no lower bound** —
  `price_date <= end_date` — so every quarantined transition is inside its window.
- `RiskStatus` (`:81-90`) is an existing seven-member vocabulary with live propagation,
  so the "enforcement shape of a refusal" the contract parked was already built.
- **`simple_returns` (`:281-299`) already breaks the return chain** on an invalid close
  — *"An invalid row breaks the chain — no synthetic return spans the gap."* A
  quarantined transition is the same category of object, so clause 3 looked like a
  second reason to break an existing, tested chain.

That third point is what sold it, and it is the one that does not survive.

## ⛔⛔ Falsification 1 — `simple_returns` is not where the returns are

Verified by reading the signatures, not inferred: **the majority of the risk surface
never passes through `simple_returns` at all.**

| path | line | consumes |
| --- | ---: | --- |
| `cagr` | `:334` | **closes** — divides endpoints directly |
| `trailing_return` | `:875` | closes |
| `excess_trailing_return` | `:895` | closes |
| `excess_cagr` | `:944` | closes, aligned per operand |
| `total_return_index` | — | raw prices + per-share dividends, carries shares across breaks |
| max drawdown | — | **levels**, compared across breaks |
| `simple_returns` | `:281` | closes → returns (vol, beta, distribution) |

So revision 1 would have produced a **single stored risk row in which some metrics are
quarantine-aware and others are not** — volatility corrected, CAGR and drawdown still
computed straight through the same discontinuity, and Calmar built on the uncorrected
drawdown. An internally inconsistent row is worse than a uniformly uncorrected one,
because nothing in the output says which half you are reading.

⚠ It is also not internally consistent with the API. `app/api/instruments.py:6211-6212`
calls `simple_returns` directly with the default argument for the histogram, rolling
volatility and fitted-beta charts, so the **chart would disagree with the stored
metric** for exactly the 2,471 affected instruments.

⚠ And `compute_instrument_risk` calls `simple_returns` three times — instrument
(`:1149`), SPY (`:1180`), sector (`:1204`). **The benchmark operands need their own
verdicts**, or a clean instrument inherits contamination through beta and every
`excess_*` metric. Residual 4's finding that benchmark propagation is currently 0 is
"by containment — all 14 `BENCHMARK_SYMBOLS` are clean over their whole history", which
that proposal already labels *a present-corpus fact, not an invariant*.

## ⛔⛔ Falsification 2 — the "exactly one return" rule came from the wrong rule

Revision 1 argued that a quarantined transition suppresses **one** return, not the two
an invalid close costs, citing `sql/247_price_quarantine.sql` decision 10: *"Bars either
side of a level break are valid prices in their own unit regime — it is the ratio
between them that is not a return."*

**Decision 10 is a T3 rule, and T3 is 10% of the population.**

```sql
SELECT rules, count(*) FROM price_transition_quarantine
 WHERE cardinality(rules) > 0 GROUP BY 1 ORDER BY 2 DESC;
```

| rules | rows |
| --- | ---: |
| `{T2}` | **3,501** |
| `{T3}` | 412 |
| `{T1}` | 355 |
| **total** | **4,268** |

**82% is T2 — a series HOLE, not a level break.** A hole is not "two valid prices in
different unit regimes"; it is missing time, and the reason its ratio is not a return
is different in kind. T1 is different again — it is a restatement of an unusable
endpoint BAR, which is precisely the case where the invalid-close rule's *two*-return
cost is the right answer, and where `simple_returns` may already be breaking the chain
for its own reasons.

So a single suppression rule cannot be correct for all three. Revision 1 took a
rule governing a tenth of the population and generalised it to the whole of it — the
same shape as the "equity-only" error corrected in residual 3 earlier the same day.

## Falsification 3 — the loader must NOT mirror its sibling

The contract says the loader mirrors `price_segments.load_unresolved_breaks`. On the
fail-closed axis it must do the **opposite**: `load_unresolved_breaks` simply omits
instruments with nothing to report, so absent and clean are the same key. Clause 3
requires *"an instrument with no current coverage row must be distinguishable from one
that is clean"*. The loader therefore has to return `iid: ()` for **covered-and-clean**
and omit **unknown** — and a caller that defaults a missing key to `()` silently
defeats the whole fail-closed property.

⚠ Coverage existence is also not coverage sufficiency: `sql/247_price_quarantine.sql:23`
carries bounds, so an instrument covered over part of its history has unchecked
prefixes/tails that must stay unknown. A date-only mapping cannot express that.

## What the measurements say (each with its query, none re-used from revision 1's prose)

The blast radius revision 1 reported is not withdrawn, but its framing is. Counts:
12,262 risk-universe instruments, **2,471 (20.2%) carrying ≥1 quarantined transition**,
4,268 transitions, 2 instruments newly below `MIN_RETURNS_VOL_BETA = 60`, 6 below
`MIN_RETURNS_ANNUALIZED = 252`.

⚠ **Three caveats revision 1 did not carry**, all raised at ckpt-1 and all correct:

1. The counts come from ad-hoc queries with **no committed script**, so their predicates
   cannot be audited. Any ticket that acts on them must re-derive them in a
   `scripts/verify_*` arm first.
2. `≥ 2` valid closes does not guarantee an adjacent valid PAIR, and "within the last
   252 bars" is not the consumer's window — metrics use differing trailing and
   benchmark-aligned windows, so the 2 / 6 refusal counts are not per-metric.
3. Revision 1 wrote that a suppressed return is *"typically an `ln 20` or `ln 30`
   corporate-action factor"*. **Unsupported, and in the wrong units** — these are SIMPLE
   returns — and it is now doubly wrong given the population is 82% T2. Deleted rather
   than softened.

## The corrected model, for whoever picks this up

1. **Clause 3 cannot be adopted "at a consumer" the way the contract's wording
   suggests, if the consumer is `risk_metrics`.** It is a surface, not a function: at
   minimum `simple_returns`, `cagr`, the drawdown path, `trailing_return`,
   `excess_trailing_return`, `excess_cagr`, `total_return_index`, the three benchmark
   operands, and the `app/api/instruments.py` chart path — which must consume the
   stored verdict rather than recompute, or it will disagree by construction.
2. **The suppression rule is per-RULE, not per-transition.** T1, T2 and T3 need three
   separately source-ruled treatments. T3 has decision 10; T2 and T1 do not, and finding
   or constructing their rule is the first task, not an implementation detail.
3. **A genuinely narrower first consumer probably exists** and revision 1 never looked
   for one, having decided on `risk_metrics` from the mechanism rather than from the
   exposure. `app/api/portfolio.py`'s `COMPOSED` day-change (two single-bar LATERAL
   reads divided) is a two-bar window with one obvious clause and no benchmark operand —
   a much better candidate for proving the loader end to end.
4. **The A/B must be paired, not sequential.** Revision 1 proposed a before/after
   rebuild; `.claude/skills/engineering/full-population-ab.md` requires both arms
   receive identical inputs, and a sequential rebuild simulates the control.

## What this document is

A withdrawn spec, kept because the falsification is the expensive part and re-deriving
it costs another checkpoint-1 pass. **No code ships from it.** `price_quarantine.py` is
untouched, so `INPUT_RULE_SETS` identity does not rotate (#3031).

## Security

No security surface — nothing ships.

Refs #3046. Refs #3031. Refs #2261.
