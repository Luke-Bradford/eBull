# Phase 3 — strategy registry + signal ledger

Status: **proposal, unshipped.** Phase 3 of the TA strategy platform
(`docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md` §5), gated
on phase 2 ✅ (`35b9ab5e`).

Refs #2240, #2245, #2244, #2288, #2311.

---

## 1. Scope — narrower than it looks, because most of it is already decided

⚠ **This phase does NOT re-decide execution semantics.** The parent spec
(`strategy-catalogue-and-backtest-validity.md` §3.5) already fixes them, once,
globally, precisely because specifying them per-strategy *"invites it back one
strategy at a time"*:

1. **Signal on close of bar *t* → fill at the OPEN of bar *t+1*.** No
   exceptions. The backtester must make same-bar fills *"structurally
   impossible rather than merely discouraged"*.
2. Every indicator at *t* uses only bars ≤ *t*. ✅ Shipped and pinned in phase
   2a (`indicator_series`, ten invariants, revert-probed).
3. Pivot/swing detection carries an explicit confirmation lag.
4. Intrabar stop-and-target on one bar is `ambiguous`, never a win.
5. Eligibility filters evaluated **as-of the decision date**.

⚠ Rule 4 says *"This is spike S5 (#2245), still open"*. **It is now closed**
(#2245, 2026-08-05) and the rule is confirmed, with numbers: the ambiguous rate
over 25,559,104 full-corpus signals is **0.83%** at a 1.0×ATR take-profit,
falling to 0.09% at 4.0×. And historical intraday resolution is **structurally
impossible** — eToro's endpoint is `/history/candles/asc/{interval}/{count}`
with no date parameter and no cursor, reaching ~2 trading sessions at
`OneMinute` (measured live). So `ambiguous` is permanent for backtests, not a
placeholder awaiting better data.

**Validation universe** is likewise decided (§4.0, #2289): **US stocks ex-ETF
only**. Non-US stays tradable, is not backtest-validated, and no strategy
allocates to it on backtest evidence.

So this phase builds **two things**: a registry that makes those rules
unbreakable, and a ledger that records what fired.

## 2. The two §7 questions this phase owes

The design doc's §7 says each open question *"must be closed before the phase
that depends on it"*. Two are phase 3's.

### 2.1 Signal uniqueness key

**`(strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)`.**

- `strategy_version` is IN the key. Without it, re-running a changed strategy
  collides with or overwrites the old signal, and the ledger stops recording
  what was actually decided.
- `signal_bar_date` — the bar whose close triggered it, not the fill date and
  not wall clock.
- ⚠ **`signal_kind` (`entry` | `exit`) is in the key** — added after Codex
  checkpoint 1. Parent §3.5 applies the fill rule to *"entries and exits
  alike"*, so a strategy that exits one position and enters another on the same
  bar for the same instrument is legitimate and the first draft's key collided
  on it.
- ⚠ **`universe` is NOT in the key, deliberately** — it is inside
  `strategy_version` instead. Parent criterion 11: *"universe is part of the
  identity hash … 'S-1 on US stocks' and 'S-1 on eu_equity' are two strategies
  and always were."* Putting it in the key too would let one strategy identity
  span two universes, which criterion 11 says is not one strategy.
- ⚠ Timeframe is absent because v1 is daily-only, and `signal_bar_date` is a
  `DATE`. **Intraday is therefore NOT a "just add a timeframe column"
  migration** — it needs bar-instant identity, because every intraday bar on
  one date would collide on this key. Stated so the next person prices it
  correctly rather than discovering it.

### 2.2 Strategy versioning

**A stable id plus a SHA-256 over the strategy's full IDENTITY — not its module
source alone.**

⚠ The first draft hashed the defining module's source, copying
`indicator_series`. That is wrong here, and Codex was right to flag it: parent
criterion 11 requires identity to cover *"code, not just parameters — same
params with a changed filter, universe or cost model is a different strategy"*.
A module-source hash misses the universe and the cost model entirely, so two
genuinely different strategies would share a version and their signals would
collide on the key above.

```python
STRATEGY_SET_VERSION = f"{STRATEGY_SET_ID}+{sha256(
    module_source || canonical_json(params) || universe || cost_model_id
    || canonical_json(INPUT_RULE_SETS)          # #2333
)[:12]}"
```

⚠ **The last term was missing until #2333, and the omission was the same class
of defect one layer up.** The hash covered the strategy's own module but not
`indicator_series.RULE_SET_VERSION` — and a strategy IS its indicators: S-1 is
`sma_series(fast) > sma_series(slow)` and has no other content. So a change to
how the SMA, RSI or ATR is COMPUTED produced different signals under an
UNCHANGED `strategy_version`, and the key above then treated the old and new
rows as the same row. Criterion 11 calls changed filter logic a different
strategy; an indicator definition is that filter logic.

✅ **FIXED 2026-08-06** — `strategy_registry.INPUT_RULE_SETS` is in the payload,
and `strategy_signals.input_rule_set_versions` (`sql/257`) stores the same
mapping for querying. Three notes it settles:

- **A registry-wide constant, not a per-strategy `inputs=[…]` field.** The
  failure being fixed is an author not thinking about indicator versions at all;
  a field they must remember to fill is the same omission with a nicer name.
  Coverage is checked rather than promised —
  `tests/test_strategy_registry.py::TestInputRuleSetsAreComplete` walks every
  module in `app.services.strategies` and fails if it imports a versioned rule
  set the registry does not name. ⚠ Direct imports only; a strategy reaching a
  pipeline through a helper is not caught.
- **The column is NOT key material**, unlike `strategy_outcomes.input_rule_set_version`.
  It is *inside* `strategy_version` here, so the corrected row already has a
  distinct key — whereas 4b's input version is outside the resolver's hash,
  which is what made the corrected outcome unstorable there. Same lesson, two
  different remedies because the two hashes cover different things.
- **Added while both ledgers were empty** — measured 2026-08-06:
  `select count(*), count(distinct strategy_version) from strategy_signals`
  → `(0, 0)`, `select count(*) from strategy_outcomes` → `0`. After rows exist,
  the indicator version behind them is recoverable only by guessing which
  historical module source hashes into the digest, i.e. not recoverable.

Over-invalidation stays the deliberate, inherited trade — a comment edit
changes the version, making stored signals *visibly stale rather than silently
mixed*, exactly as `price_quarantine` argues. ⚠ #2333 widens it twice: a comment
edit in `indicator_series.py` now moves every strategy's identity, and because
the set is registry-wide, a strategy reading none of those series moves with
them. Accepted knowingly. A signal row stores the version that produced it, so
an outcome is never reinterpreted under new logic.

## 3. The registry

Strategies are **code, not rows.** A strategy is a pure function over a
`BarSeries` plus its indicator series, returning a verdict per bar.

```python
Verdict = Literal["fired", "not_fired", "not_evaluable"]

# ⚠ CLOSED vocabulary, taken verbatim from parent criterion 8. NOT free text.
NotEvaluableReason = Literal[
    "missing_volume", "missing_spread", "insufficient_warmup",
    "quarantined_bar", "series_break", "not_listed", "ambiguous_intrabar",
    "no_fill_bar",          # see below — an ADDITION, flagged as such
]

@dataclass(frozen=True)
class StrategySignal:
    verdict: Verdict
    signal_index: int
    #: REQUIRED when verdict == "not_evaluable", forbidden otherwise.
    reason: NotEvaluableReason | None = None
```

⚠ The first draft had `reason: str`. Parent criterion 8 is explicit that
*"`not_evaluable` carries a reason code … These have different bias
implications and collapsing them loses the ability to tell a data gap from a
real absence."* Free text cannot be counted, so it cannot support criterion 9's
"measure what you reject". Closed vocabulary, enforced by a CHECK.

⚠ **`no_fill_bar` is an ADDITION to the parent's seven and is flagged rather
than smuggled.** The last bar of any series has no *t+1*, so a signal there can
never be filled. None of the seven parent codes describes that, and it is not a
data gap — it is the edge of the series. If the parent's vocabulary is the
authority, this needs adopting there; recorded here so it is a decision rather
than a drift.

### 3.1 ⚠ Evaluability is decided BEFORE the condition, never by short-circuit

The hole Codex found at checkpoint 1, and it is subtle enough to be worth the
space. Given `close > sma_200 AND volume > vol_sma * 1.5`:

```python
# WRONG — returns not_fired when close <= sma_200, without ever
# discovering that `volume` was unevaluable. The rule was NOT evaluated.
if close <= sma_200:
    return not_fired
```

Python's `and`/`or` short-circuit, so the first false condition returns before
the unevaluable input is touched. The verdict is then `not_fired` for a bar the
strategy could not actually judge — which is exactly decision 5's corruption,
re-entering through the back door after being closed at the indicator layer.

**The contract:** a strategy declares the indices it requires, and the runner
checks every one for evaluability **before** the condition is evaluated at all.
A strategy body is only invoked on bars where all its inputs are evaluable.
That makes short-circuit ordering irrelevant rather than a thing each author
must remember.

## 4. Making the fill rule structurally impossible to violate

⚠⚠ **The first draft claimed "a `>` CHECK is the whole mechanism". That is
false**, and Codex refuted it at checkpoint 1: a writer can record
`signal_bar_date = t-1`, fill on `t`, and use bar `t`'s data — every constraint
passes. The CHECK proves the two stored dates are ordered. It proves nothing
about which bar the strategy actually read.

**The real mechanism is the API shape: the strategy never supplies a fill.**

```python
# The strategy returns only an INDEX into the series it was given.
StrategySignal(verdict="fired", signal_index=t)

# The writer — not the strategy — resolves the fill:
fill_index = t + 1                      # next bar IN THE SERIES
if fill_index >= len(series):
    -> not_evaluable("no_fill_bar")     # no t+1 exists
fill_bar_date  = series.dates[fill_index]
fill_price     = series.rows[fill_index]["open"]
```

A same-bar fill is **not expressible**: there is no parameter through which a
strategy could request one. That is what "structurally impossible" has to mean
— removing the capability, not detecting its misuse.

The CHECK stays as a **backstop against a buggy writer**, and is described as
that rather than as the mechanism:

```sql
CONSTRAINT signal_fill_is_after_signal CHECK (fill_bar_date > signal_bar_date)
```

⚠ Two further gaps Codex identified that the CHECK does not close, and neither
is claimed to be:

- It does not verify `fill_bar_date` is the **next** bar — any later date
  passes. Only the writer's index arithmetic guarantees that, and acceptance 2
  tests it on an instrument with a calendar gap.
- It does not enforce **fill at the open**. The fill price is stored alongside
  so a reader can check it against `price_daily`, and acceptance 7 asserts it
  equals `open(t+1)`.

⚠ `fill_bar_date` is the next bar **in that instrument's series**, never
`signal_bar_date + 1 day`. Calendar gaps are normal — S4 measured 1,204
instruments whose latest bar is over a month old — and date arithmetic would
invent a fill on a day the instrument did not trade.

## 5. Survivorship labelling — inherited, not re-litigated

Every ledger row carries `universe` (`survivor_only` | `survivorship_free`),
**required, no default**, exactly as `price_structure` and `indicator_series`
do. #2288 is the contract; this phase is where it finally reaches a stored row
rather than a returned object.

⚠ The research corpus is `survivor_only` (#2284: 0 of 259 known delisted names
served). Every v1 signal is therefore labelled `survivor_only`, and any win
rate computed from them inherits that label. This is the phase where the label
stops being theoretical.

## 6. What is NOT in this phase

- **The backtester.** Design-doc decision 3 says ledger and backtester are
  *"built together on shared execution semantics"* — §§3-4 above ARE those
  shared semantics, and phase 5 consumes them. Building the runner here would
  fork them.
- **The outcome resolver** (phase 4). The ledger records what fired; what
  happened next is `tp_hit`/`sl_hit`/`expired`/`ambiguous` and belongs there.
- **Any strategy in the catalogue.** §4 has a list; implementing them against a
  registry that does not exist yet is the wrong order.
- **Indicator computation.** Phase 2a owns it, and ⚠ it is **83.3 s** for all
  seven over the corpus against a < 60 s target — #2311. Phase 3 does not
  recompute; it consumes.

## 7. Acceptance

1. A same-bar fill is **rejected by the database**, proven by a test that
   attempts the insert and asserts the constraint violation.
2. `fill_bar_date` is the next bar present in the instrument's series, proven
   on an instrument with a calendar gap — not `signal_bar_date + 1`.
3. A strategy reading an unevaluable indicator index returns `not_evaluable`,
   never `not_fired`. Revert-probed by making the strategy read the `None` as
   falsey and asserting the test fails.
4. The uniqueness key rejects a duplicate `(strategy, version, instrument,
   signal_bar)` and ACCEPTS the same signal at a different `strategy_version`.
5. `universe` has no default; constructing a ledger row without it is a
   `TypeError`.
6. `STRATEGY_SET_VERSION` changes when ANY identity component changes — module
   source, params, universe or cost model — asserted per component, not just
   for source.
7. The stored fill price equals `open(t+1)` for that instrument, checked
   against `price_daily`.
8. A signal on the LAST bar of a series returns `not_evaluable("no_fill_bar")`,
   never a fill.
9. ⚠ A strategy whose condition short-circuits before reading an unevaluable
   input still returns `not_evaluable` — revert-probed by reordering the
   condition and asserting the test fails.

## 8. Tickets

⚠ **The first draft ordered these 3a-schema → 3b-registry, which is backwards
and Codex flagged it as cyclic**: the schema cannot define its `verdict` enum,
its reason-code CHECK, or its strategy-id format until the registry contract
exists. Corrected:

| # | scope | depends on |
| --- | --- | --- |
| 3a | registry contract — `Verdict`, `NotEvaluableReason`, `StrategySignal`, `STRATEGY_SET_VERSION` over the full identity, and §3.1's declare-inputs-first runner | — |
| 3b | schema — `strategy_signals`, the uniqueness key, the reason-code and fill-order CHECKs, `universe` | 3a |
| 3c | writer — resolves `fill_index` from the series, stores the fill price, enforces the key | 3a, 3b |

⚠ **No strategy is implemented in any of them**, and that is deliberate: a
registry shaped around its first sample strategy fits that one and nothing
else. ⚠ Codex counters that acceptance then rests on test-only strategies which
*"may not exercise actual strategy catalogue needs"* — a fair risk, accepted
knowingly: the parent's §4 catalogue is written and its S-1..S-6 shapes are the
design input, so the registry is shaped against six specified strategies rather
than zero. The first real implementation is its own ticket immediately after 3c
and will be the proof.
