# #2840 §8 obligation (a) — the carrier's source selection sits outside every identity hash

Status: proposal, REWRITTEN after Codex checkpoint 1 refused the first draft (17 findings —
the **thirteenth** ckpt-1 refusal on this ticket). §9 carries the disposition table; six of the
first draft's claims were false and are corrected in place rather than softened.

Ticket: #2840 (closed; the provenance line continues under it). Queue: #2437.
Measured at `69c17204`, branch `feature/2840-carrier-source-tripwire`.

Predecessors: `07f62f2f` (the carrier), `b9860cf2` (§8 recorded both obligations open),
`eddd4723` (the re-observation comparison).

## 0. Why this and not the queue head

`eddd4723`'s named head is *"read #2840's first real RTH session"*. Wall-clock gated:
`strategy_intraday_harvest` reports `skipped` outside its window, and it is Sunday ~20:00 ET
with Monday's open ~13h out. Not blocked — not readable tonight. The named alternative is §8's
two obligations, which is this document.

## 1. The claim, measured — with the arguments pinned

⚠ Working-order 3c. This ticket has falsified **three** inherited premises by deriving them
(`308d1e38`, `cbfd8144`, `03b6596f`). Obligation (a) is measured here, not restated.

⚠ The first draft printed an identity it labelled `survivor_only` that was actually the
`survivorship_free` one, and Codex could not reproduce it. Both are reproducible once the
argument is pinned, and the pinning is the point:

```
s12_identity(universe="survivor_only",      cost_model_id=COST_MODEL_ID).version
    = strategy-registry-v1+6b4b504f4fdf          ← SCAN_UNIVERSE, the ledger path
s12_identity(universe="survivorship_free",  cost_model_id=COST_MODEL_ID).version
    = strategy-registry-v1+e711e9312913
```

Both are unchanged by the carrier's source. The comparison, on
`tests/test_2840_cheapest_band_price_gated_breakout._above_edge()` (175 bars), at
`universe=SCAN_UNIVERSE`, `masked_reason=MASKED_REASON`, everything else held:

```
carrier rule_set_version, from_undeclared_source vs from_archive_basis("unadjusted"): IDENTICAL

from_undeclared_source : {('not_evaluable','missing_market_context'): 174,
                          ('not_evaluable','no_fill_bar'):              1}
from_archive_basis     : {('not_evaluable','insufficient_warmup'):    113,
                          ('not_fired', None):                         60,
                          ('fired', None):                              1,
                          ('not_evaluable','no_fill_bar'):              1}

verdict LABEL changes      : 61 / 175
reason-code changes        : 174 / 175
(verdict, reason) UNCHANGED:   1 / 175   ← the terminal no_fill_bar
```

⚠ The first draft said *"every verdict moves"*. False, and the repo's own rule fires on it —
a sentence containing "every" about data is a measurement. **61 of 175 bars change their verdict
label; 174 of 175 change their `(verdict, reason)` pair.** The one unchanged bar is the terminal
`no_fill_bar`, which `evaluate` returns before reading any input.

**No hashed input moves.** `corpus_generation` does not move either, by construction rather than
by measurement: it digests the bar values a pass read plus the regime, the unresolved breaks, the
quarantine rule version and the frontier date (`corpus_generation.py:107-215`). A constructor
choice is none of those.

⚠ What this demonstrates is CARRIER SENSITIVITY under a fixed identity, and not more. Swapping
constructors also changes the carrier's values and routes S-12 through its all-refused
short-circuit, so the measurement does not isolate "source authenticity" as a separable variable.
That is sufficient for the claim being made — the identity cannot distinguish the two regimes —
and it is not sufficient for any stronger one.

## 2. What the ledger actually holds, and where a certified row would go

⚠ The first draft got the storage wrong. **Fired rows go to `strategy_signals`; only
non-fired decisions reach `strategy_signal_observations`** (`strategy_observation_storage.py:336`).
So a certified scan does not overwrite the stored refusals — it writes `fired` rows to a table
S-12 has never written to, and `not_fired` rows alongside the refusals.

Measured read-only on the dev DB at `69c17204`:

```sql
select count(*), count(distinct strategy_version) from strategy_signal_observations
 where strategy_id like 's12%';                                  -- 5791, 1
select count(*), count(distinct strategy_version) from strategy_signals
 where strategy_id like 's12%';                                  -- 0, 0
select strategy_version, reason_code, count(*) ... group by 1,2; -- b18d5869092b | missing_market_context | 5791
```

This is the audit Codex asked for rather than the grep the first draft offered, and it supports
the uniformity claim: **one version, one reason code, zero fired rows.**

⚠ And it surfaces something the first draft missed. The stored version `b18d5869092b` is
**neither** current identity — the S-12 module has been edited since those rows were written, so
the 5,791 rows are already detached from today's S-12. See §4.

⚠ §2's first draft also claimed `strategy_signal_scan` is the *only* writer. False:
`store_strategy_observations` is called directly by `tests/test_signal_ledger_writer_db.py` and
`tests/test_strategy_observation_storage.py`, `sql/280_strategy_signal_negative_detail_move.sql:83`
inserts historical negative rows directly, and `scripts/verify_2394_signal_scan.py` writes
transitively through `run_signal_scan`. The defensible form is **"the sole current application
producer"**, and that is what the narrowing rests on: no `backtest_run` → observation path exists,
so that module's per-run `archive_adjustment_basis` variability does not reach this ledger.

## 3. The three candidate fixes

### Option 3 — hash the source. The first draft refused it on a FALSE cost.

The draft said hashing the source needs a per-call argument on the identity factory, which lands
on all 11 strategies and detaches their evidence across 24 identity-carrying tables. **That is
wrong** (ckpt-1 finding 7). A digest of the source-selection policy can go in `S12_PARAMS`
**alone** — no identity-factory argument, no registry change, no other strategy touched. A policy
covering several call contexts can be static even though the value it selects varies per call.

Done naively it is still wrong in a different way: digesting `strategy_signal_scan.py`'s bytes
would rotate S-12 on every unrelated scan edit, and the strategy module cannot import the scan
that imports it. The shape that works is to **extract the routing choice into its own small
module and hash that** — the idiom `PRICE_BASIS_RULE_VERSION` already uses for `RESEARCH_ARCHIVES`.

⚠ A declaration disconnected from the actual routing still needs enforcement, so Option 3 does
not remove the need for a test; it changes what the test asserts.

**Not in this diff, and the reason is the ladder rather than the cost.** It is a production change
to a module whose bytes are hashed into a stored identity — a behavioural change with data
semantics, owing an identity rotation and its evidence disposition. That is its own round.
Recorded as the named next item for obligation (a) in §8 below, with the corrected cost:
**S-12 alone, and the 5,791 stored rows are already on a third version regardless.**

### Option 2 — record the source on the ledger row (`corpus_generation`'s shape)

Write-once, non-key, nullable. Would make a stored row self-describing. Not the next step: §2's
audit shows one source across every stored row, so the column would carry one constant value and
distinguish nothing today. ⚠ And it does not substitute for Option 3 — row metadata cannot fix
an identity collision, and any real proposal must cover `strategy_signals` and the durable
daily-count aggregate too, not just the observation table.

### Option 1 — a behavioural tripwire (adopted for this diff)

The live trigger for this defect is a code edit; the proportionate guard for a code edit is a
test, and that is the scale `b9860cf2` named. It is the guard that holds until Option 3 lands.

Design, after ckpt-1 refused the first one:

* ⛔ **Not a spy.** Monkeypatching `segmented_signals` to capture the `price_basis` argument was
  the first design. Replacing the dispatcher removes the forwarding, the segment remap and S-12's
  consumption — so it can pass while an adapter discards the carrier entirely. The test drives the
  real `_scan_per_series`.
* ⚠ **Not a reason-code assertion.** `PRICE_BASIS_REFUSAL_REASON` is `"missing_market_context"`,
  the same code an absent regime emits. Asserting the reason would pass on a fixture whose regime
  was simply missing — prevention-log 2046's tautology shape. The refusal is pinned by its
  **verdict set**, the fixture supplies a regime classified on **every** bar, and a companion test
  shows the same bars and the same regime reaching `fired` when only the carrier changes.
* ⚠ **Not vacuous.** `assert out` precedes every set/`any` assertion; `all(...)` over an empty row
  list passes, and `from_archive_basis("unadjusted", n_bars=0).certifies_nothing()` is `True`.
* ⚠ **One site, not two.** `_stage_cross_sectional` is unreachable for S-12: `run_signal_scan`
  dispatches on `entry.strategy_class == "per_series"` (`strategy_signal_scan.py:882`) and S-12
  has no member function. Claiming both scan call sites would be false.

## 4. ⛔ The first draft's scope was unshippable — a comment edit rotates the identity

The draft proposed correcting two in-code comments in
`s12_cheapest_band_price_gated_breakout.py`, and asserted *"no production file that feeds an
identity hash is touched"*. **`_source_hash()` hashes the entire file** (`:254`), so a
comment-only edit rotates S-12's identity. Verified:

```
sha256(file)[:12]                         = 31a38948e10a
sha256(file + b"\n# a comment\n")[:12]    = b261c0bce572   ← rotates
```

This is deliberate, not a bug — `StrategyIdentity.source_hash` is documented as *"Source of the
module DEFINING the strategy"*, and hashing bytes is the conservative reading of criterion 11
(*"identity must cover code, not just parameters"*). A hash over bytes cannot tell a comment from
a branch.

⇒ **the comment edits are dropped from this diff.** The derivation lives here and on the issue
instead. This is also the general trap: **a doc-only edit to a strategy module is an identity
rotation**, and it is invisible at review because the diff is prose. It is why §2's 5,791 rows
sit on `b18d5869092b` rather than on either current version.

## 5. ⛔ Obligation (b) is REACHABLE today — the first draft's "not reachable" is falsified

*"A certified carrier is bound to its series only by length and rule version — nothing ties it
to an instrument, its dates or its payload."*

The first draft inventoried the four production call sites, found each builds at
`n_bars=len(series)` beside its series, and concluded the class is unreachable. Codex refused it
by **constructing the counterexample** rather than arguing, and it reproduces here:

```
s12_signals(series_A, ..., price_basis=from_archive_basis("unadjusted", n_bars=len(series_B)))
    -> Counter({'not_evaluable': 114, 'not_fired': 60, 'fired': 1})     # accepted, and FIRES
```

A same-length carrier built for another series is accepted by `s12_signals` and by
`segmented_signals`, and the strategy fires on it. `PriceBasisSeries` is public and structural;
a hand-built carrier needs no per-bar source to be foreign.

⚠ And the proposed trigger was wrong in both directions: foreign certification can arise **while**
using `len(series)` (above), and a properly bound per-bar source need not introduce it. Binding to
instrument id alone would miss changed dates; dates alone would miss another instrument or revised
prices.

Corrected guard inventory — three length checks, not two: `s12_signals`,
`PriceBasisSeries.segment`, **and both segmented dispatchers**, which check total carrier length
before slicing (`strategy_segmented_evaluation.py:48`). All three catch a WRONG-LENGTH carrier.
**None catches an equal-length foreign one.**

⇒ (b) is restated honestly: **reachable through the public API today, not reachable through any
current application call site.** It stays open, and the binding key is a real design question
(instrument, dates, payload, or a digest of all three) rather than a detail the per-bar source
will settle by itself. Not in this diff.

## 6. Scope

- One new test file, `tests/test_2840_carrier_source_selection.py`. No production change, no
  schema change, no migration, no backfill, no identity rotation.
- Review ladder: narrow diff (test-only, no data semantics) ⇒ self-review + pre-push hook +
  review bot. Checkpoint 1 ran on this document, which is where it earned.

## 7. Acceptance

1. The tripwire passes unmodified. ✅ `pytest tests/test_2840_carrier_source_selection.py` exit 0.
2. It FAILS when the scan call site is swapped to a certifying constructor, **and the failure is
   attributed to the intended assertion** rather than to an import or mock error. ✅
   `AssertionError: assert {'fired', 'not_evaluable', 'not_fired'} == {'not_evaluable'}` at
   `assert {row.verdict for row in out} == {"not_evaluable"}`. Reverted; unmodified control
   re-run at exit 0.
3. `strategy_version` is unmoved — no file whose bytes feed an identity hash is touched. ✅ by
   §4's correction (the comment edits that would have violated this are dropped).

## 8. What remains open on obligation (a) and (b)

- **(a)** — Option 3: extract the carrier's source-selection routing into its own module and hash
  it into `S12_PARAMS`. Cost, corrected: **S-12's identity alone**, not 11. Needs its own round
  (identity rotation + evidence disposition for the 5,791 stored rows, which already sit on a
  third version).
- **(b)** — bind a certified carrier to its series. Reachable today through the public API;
  the binding key is undecided and instrument-id-alone is insufficient.
- ⚠ A doc-only edit to any strategy module rotates that strategy's identity. Transferable beyond
  #2840 — every one of the 11 modules has it.

## 9. Checkpoint-1 disposition (17 findings)

| # | finding | disposition |
|---|---|---|
| 1 | "every verdict moves" overstated | **FIXED** §1 — 61/175 verdict labels, 174/175 pairs, 1 unchanged |
| 2 | table does not isolate causality | **FIXED** §1 — claim narrowed to carrier sensitivity under fixed identity |
| 3 | identity not reproducible | **FIXED** §1 — it was the UNIVERSE; both pinned, Codex's value was the scan one |
| 4 | storage described wrongly | **FIXED** §2 — fired → `strategy_signals`; no overwrite claim |
| 5 | "exactly one writer" false | **FIXED** §2 — "sole current application producer", other writers named |
| 6 | uniformity needs an audit | **FIXED** §2 — ledger query: 5,791 rows, 1 version, 1 reason, 0 fired |
| 7 | 11-identity argument wrong | **FIXED** §3 — Option 3 reinstated as the named next step, cost corrected to S-12 alone |
| 8 | Option 2 rejection does not follow | **FIXED** §3 — rejected as *next step* only; limits stated |
| 9 | spy is a wiring test | **FIXED** §3 / test — spy dropped, real `_scan_per_series` driven |
| 10 | can pass vacuously | **FIXED** test — `assert out` before every set/`any` assertion |
| 11 | mutation failure needs attribution | **FIXED** §7.2 — failing assertion quoted |
| 12 | second site unreachable for S-12 | **FIXED** §3 — one site, dispatch cited |
| 13 | §4 confuses length with provenance | **FIXED** §5 — rewritten |
| 14 | "unreachable today" too broad | **FIXED** §5 — falsified and reproduced; claim inverted |
| 15 | proposed trigger neither necessary nor sufficient | **FIXED** §5 — trigger withdrawn |
| 16 | guard inventory incomplete | **FIXED** §5 — three checks, dispatchers included |
| 17 | §5 contradicts acceptance 3 | **FIXED** §4 — comment edits dropped; rotation verified |
