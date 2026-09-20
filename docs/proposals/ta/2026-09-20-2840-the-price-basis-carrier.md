# #2840 — the per-series price-basis carrier: build it, sourced from the ARCHIVE provenance

**Status:** spec, revised after Codex checkpoint 1 (37 findings; §10 records what each one
moved). Supersedes §6 item 2 of `2026-09-20-2840-per-series-price-basis-carrier.md`, whose
steps 1 and 3 are unchanged.

## 0. The inherited conclusion, and where it is too strong

`3c6bb73f` §2 disqualified all three candidate certifiers and concluded *"there is nothing
to route, because nothing CERTIFIES"*. Re-falsified this session (working-order 3c), and one
row of that table is over-stated.

The draft it refused routed **`_Corpus.cost_price_basis`**. The refusal is right and stands:

> **control** — `replace(corpus, cost_price_basis="split_adjusted")`
> — `scripts/ab_3238_cost_basis.py:355`

But `cost_price_basis` is a **derived** field. Its root is
`_Corpus.liquidity_policy.adjustment_basis`, and the same A/B says at `:30`:

> ⚠⚠ `liquidity_policy` IS DELIBERATELY NOT REPLACED.

Verified in source, and independently re-verified at checkpoint 1, which found no other
`liquidity_policy` substitution in `app/` or `scripts/`. **The one sentence that killed the
draft does not apply to the root value.**

| property | `cost_price_basis` | `liquidity_policy.adjustment_basis` |
| --- | --- | --- |
| substituted by the #3238 A/B | **yes** (`:355`) | **no** (`:30`) |
| collapses *adjusted* / *unknown* / *missing* | **yes** — `cost_model.py:559` maps everything not `"unadjusted"` to one token | **no** — four stored members, `sql/249:86-89` |
| run-level homogeneity asserted | no (it is one token) | **yes** — `backtest_run.py:1584-1596` |
| absence is withholding, not eligibility | n/a | **yes** — `_Corpus.liquidity_policy` docstring, `sql/305` |

### 0.1 What this is NOT, corrected at checkpoint 1

⚠ **The root is itself synthesised, and agreement is internal consistency — not independent
certification.** `strategy_entry_liquidity.archive_policy_for:94` builds `ArchivePolicy` from
the `RESEARCH_ARCHIVES` literals; `research_corpus_ingest` writes the stored labels from the
*same* provenance objects. So `backtest_run.py:1584`'s check asks "do the stored labels match
the literal we ingested them from". That is a **drift detector**, and it is worth having, but
it certifies nothing about a payload.

⚠ **And it does not BLOCK anything today.** `_resolve_liquidity_policy` returns `None` on
disagreement — it *withholds the policy* and evaluation continues (`:1590-1596`). The first
draft of this spec said the check was "the only thing preventing" a split-adjusted corpus
reaching S-12's nominal gate. Wrong in the direction that matters: **nothing prevents it.**
That strengthens the motivation rather than weakening it.

⚠ **The mixed distribution is database-wide, not run-wide.** `research_price_series` holds
**22,880 `unadjusted`** and **7,711 `split_adjusted`** rows (dev DB, read-only), which shows
the two bases coexist in the store. It does **not** show a vendor-pinned run is mixed.

## 1. What is being replaced

`s12_signals` refuses every bar when `universe not in AS_TRADED_UNIVERSES`
(`s12_cheapest_band_price_gated_breakout.py:313`). That is a **series-level universe token
standing in for a price-basis fact**, and it is wrong in both directions:

- **False refusal.** `SCAN_UNIVERSE = "survivor_only"`, so the live scan refuses every S-12
  bar unconditionally — measured: all **5,791** stored S-12 observations are
  `not_evaluable / missing_market_context`, every one of them this gate.
- **False admission.** `survivorship_free` is a statement about *which names are in the
  panel*, never about *what scale their prices are on*. A `survivorship_free` run whose
  archive policy was withheld — or whose archive is `split_adjusted` — passes this gate with
  no refusal anywhere, because the resolver only withholds a diagnostic.

## 2. The carrier

New module `app/services/strategy_price_basis.py`.

```python
@dataclass(frozen=True)
class PriceBasisSeries:
    values: tuple[CertifiedPriceBasis | None, ...]
    not_evaluable_indices: tuple[int, ...] = ()
    rule_set_version: str = PRICE_BASIS_RULE_VERSION
```

**It satisfies `strategy_registry.EvaluableSeries` structurally** — `values`,
`not_evaluable_indices: tuple[int, ...]` and `__len__`, the same shape that let
`market_regime.RegimeSeries` be declared as a `StrategyInput` with no adapter
(`strategy_registry.py:336-379`). That is why this design needs no new refusal machinery:

- **per bar** — §6 item 1's requirement;
- **read by `evaluate` before the body runs** (`_unevaluable_reason_at:411`), so an
  uncertified bar is `not_evaluable` and never `not_fired`;
- **outside the indicator recursion** — §3.3's requirement, which masking fails.

`segment()` is a method and not a slice, for `RegimeSeries.segment`'s stated reason verbatim:
a raw slice type-checks and silently drops `not_evaluable_indices`. It preserves
`rule_set_version`.

### 2.1 The invariants — stricter than `RegimeSeries`, deliberately

`RegimeSeries` permits a `None` value *outside* `not_evaluable_indices`, and that is correct
there: it means **warm-up**, a real third state. **Price provenance has no warm-up state**,
so copying the invariant would create two silent defects, both found at checkpoint 1:

1. an unmarked `None` would reach `_unevaluable_reason_at`'s warm-up branch and be recorded as
   `insufficient_warmup` — a provenance gap wearing an indicator's reason code;
2. `"unknown"` is a member of `AsTradedPriceBasis`, and `evaluate` tests only `is None`, so a
   carrier of `"unknown"` values would **evaluate every bar**. A fail-open, in the vocabulary
   the spec proposed to reuse.

So `__post_init__` enforces, and raises on each:

- **complete correspondence** — `values[i] is None` **iff** `i in not_evaluable_indices`. Both
  directions, not one.
- **closed vocabulary at runtime** — every non-`None` value is in `CERTIFIED_PRICE_BASES`.
  `Literal` enforces nothing at runtime; `StrategySignal.__post_init__` already makes this
  point and is the precedent followed.
- indices in range, no duplicates, sorted.

`CertifiedPriceBasis` is `AsTradedPriceBasis` **minus `"unknown"`**, derived with `get_args`
and a subtraction rather than restated — `"unknown"` is exactly the state this carrier
expresses as `None` + a refusal index, and permitting both spellings would make one of them
a fail-open forever.

### 2.2 The one source built this session

```python
def from_archive_basis(adjustment_basis: str | None, *, n_bars: int) -> PriceBasisSeries
```

- `"unadjusted"` → every bar `"observed_unadjusted"`, `not_evaluable_indices = ()`.
- anything else, **including `None` and any member added to `sql/249` later** → every bar
  `None`, every index refused.

Fail-closed by construction, in the direction that needs saying out loud.

⚠ **`"observed_unadjusted"` — and the source rule is `sql/305`, not first principles.**
`sql/305`'s header requires *"either a directly observed unadjusted level or a reconstruction
backed by point-in-time adjustments"*. The archive stores the raw traded level rather than one
we reconstructed, so the first arm is the applicable one. The label is **measured**, not
assumed (`research_corpus_ingest.py:165-171`): the same AAPL bar reads **500.04** here against
Yahoo's split-adjusted, and the 1980-12-12 IPO bar reads **28.75** against Yahoo's **0.1283**
— neither the split nor the dividend adjustment is present, and the archive's ninth column
carries both separately.

⚠⚠ **That measurement is TWO AAPL BARS, and it is the weakest link in this spec.** It is the
archive's own recorded basis for its label and the best evidence that exists, but 22,880
stored labels are not 22,880 validated payloads, across all fields and all spans. Stated here
rather than buried: the carrier moves the gate from a universe NAME to a MEASURED-ON-A-SAMPLE
label. Strictly closer to the fact; not the fact. Validating the payload is a census, and it
is recorded as an obligation in §6 rather than claimed.

⚠ **The first draft cited `308d1e38` here. Withdrawn** — that probe measured **eToro's
delivered history** and says nothing about the Intrader archive this constructor admits. Two
different vendors; citing one for the other is the coincidence-window error this ticket
already has a prevention entry for.

⚠ **Constant across the series, and that is not a defect.** Archive provenance is per-series
by construction. The per-bar STRUCTURE is what the composed forward series will need
(`3c6bb73f` §4: 74.1% of stored 30m bars arrived >1 day late), and a structure that only
becomes per-bar once a second source lands is one nobody would have built.

### 2.3 The source NOT built this session

`bar_capture_certificate.capture_certificate` is the forward/intraday source, deliberately
absent. Both reasons measured:

1. **No consumer.** `strategy_intraday_bars` feeds no `BarSeries` reaching `segmented_signals`
   — zero call sites.
2. **It would admit nothing.** All 29,234 stored bars predate `sql/402`/`sql/403`
   (`unverifiable_capture_semantics`), and `PROVIDER_REWRITE_TIMING_VERIFIED = False`
   downgrades every certifying verdict anyway (`9246f77c`).

## 3. Delivery — the uniform call, per §3.1

`price_basis: PriceBasisSeries` joins `PerSeriesSignals.__call__` and
`MemberStager.__call__`. The precedent is `regime`, stated in the protocol's own comment:

> ⚠⚠ `regime` IS ON THE UNIFORM CALL, NOT ON THE STRATEGIES THAT USE IT. … The alternative —
> a `requires_regime` flag with a runner branch — reintroduces exactly the per-strategy `if`
> this module exists to delete.

**Delivery is uniform; gating is declared.** S-1…S-11 absorb the argument in their manifest
adapters and ignore it (`noqa: ARG001`, as they already do for `regime`). S-12 declares it.

`segmented_signals` / `segmented_member` slice with `.segment(start, end)` exactly as they
slice the regime, and validate `len(price_basis) == len(series)` with the same raise.

### 3.1 Constructed where the length cannot be wrong

⚠ Checkpoint 1 probed `evaluate` and found a **silent** failure: a carrier one element short
passes, because the last bar returns `no_fill_bar` before any input is read, so the missing
index is never looked up. A wrapper-only length check does not cover a direct
`s12_signals(...)` call.

So the runner entry points take the **basis string**, not a built carrier, and construct it
themselves at `n_bars=len(series)`:

```python
_signals_for(..., archive_adjustment_basis: str | None)   # backtest_run.py:3183
```

Length is then correct by construction on every production path, and the check inside S-12 is
a backstop for hand-built callers rather than the only guard.

| runner | source | today's value |
| --- | --- | --- |
| `backtest_run._signals_for` | `corpus.liquidity_policy.adjustment_basis`, `None` when withheld | `unadjusted` ⇒ certified |
| `strategy_signal_scan` | `None` | uncertified |

⚠ **The scan's `None` is a POLICY, stated as one.** The first draft derived it from
`price_daily` having no basis column. Checkpoint 1 is right that this repeats an inference
`9246f77c` expressly withdrew ("no column ⇒ no certificate" overreaches). The defensible form:
**no source of as-traded provenance has been declared for the live path, and an undeclared
basis is refused** — the same posture `sql/305` and `_Corpus.liquidity_policy` already take.

### 3.2 The run-wide→universal treatment decision, stated

One disagreeing series withholds the policy for the **whole run**, including correctly
labelled series. Turning that into a universal S-12 refusal is a **choice**, not a derivation,
and it is taken deliberately: the withheld state means *we do not know which series are
mis-labelled*, and a nominal-price gate under an unknown scale is the failure this rule
exists to prevent. Recorded here so it is reversible by argument rather than by discovery.

## 4. The gate — S-12 declares the input FIRST

```python
inputs = (
    StrategyInput(series=price_basis, reason=PRICE_BASIS_REFUSAL_REASON),  # FIRST
    StrategyInput(series=_close_input(...), reason=masked_reason),
    ... # atr, compression, prior_high
)
```

⚠ **Order is load-bearing and was a checkpoint-1 finding.** `_unevaluable_reason_at` returns
the **first** declared input's reason among competing data reasons. A bar that is both
quarantined and uncertified should report the basis refusal: the others say *this bar's data
is unusable*, the basis says *this rule may not read this corpus at all*, and the precondition
is the more informative answer. Declared first, with that reason written at the tuple.

`PRICE_BASIS_REFUSAL_REASON` is already `"missing_market_context"` with exactly this meaning
stated. No new `NotEvaluableReason` member, so `sql/255`'s CHECK and the three Python
restatements are untouched.

⚠ **All-refused short-circuits before `evaluate`.** `_unevaluable_reason_at` does
`index in series.not_evaluable_indices` on a **tuple**, so an all-refused carrier of `n` bars
is O(n²) — checkpoint 1's finding, and `n` is the whole corpus on the new withheld path. The
protocol types that field `tuple[int, ...]`, and widening it means editing
`strategy_registry.py`, which is hashed into **every** identity. So S-12 returns the uniform
refusal list directly when the carrier certifies no bar — the same shape
`AS_TRADED_UNIVERSES` already uses at `:313`, and the declared input still governs the mixed
case a per-bar source will create.

⚠ **`AS_TRADED_UNIVERSES` STAYS.** Removing it is §6 item 3 and a separate change.

## 5. Behaviour, enumerated

| path | universe | archive basis | today | after |
| --- | --- | --- | --- | --- |
| backtest, pinned archive | `survivorship_free` | `unadjusted` | evaluated | evaluated — **unchanged** |
| backtest, withheld policy | `survivorship_free` | `None` | **evaluated** | `not_evaluable / missing_market_context` |
| backtest, adjusted archive | `survivorship_free` | `split_adjusted` | **evaluated** | `not_evaluable / missing_market_context` |
| live scan | `survivor_only` | n/a | refused (universe) | refused (universe) — same content |
| S-1…S-11, any | any | any | unchanged | **unchanged verdicts** |

⚠ **"Unchanged" is about VERDICT CONTENT, not about operations.** Three corrections from
checkpoint 1:

- **The scan rotates.** Any edit to S-12's module moves its `_source_hash()`, so
  `write_window_indices` (`strategy_signal_scan.py:349`) cold-starts a new watermark and the
  5,791 existing observations detach. Their content is reproducible — every one is this same
  refusal — but the catch-up calendar restarts.
- **S-1…S-11 execution changes even though their identities do not.** They take a new required
  argument and a new object is constructed per series. Verdicts are unchanged; "nothing
  changes" was too broad.
- **Rows 2 and 3 would change other strategies' metrics if they fired.** `deflate_group`
  (`backtest_run.py:3417`) deflates over the group's shared measurements, so removing S-12
  trades moves the group's trial count. It does **not** fire today (row 1 is the live case),
  and that is a fact about today's archive, not a property of the design.

## 6. Boundary — what the carrier does NOT certify

Recorded as obligations, all raised at checkpoint 1 and none of them closed here:

- **It gates ENTRY VERDICTS only.** A certified signal at `t` still fills at an uncertified
  `open(t+1)`, and a held position still consumes uncertified `high`/`low`/`close` and its
  terminal mark. Entry refusal cannot certify a trade's price path.
- **Bar-local gating is not bar-local CONTAMINATION.** An uncertified bar's OHLC still enters
  ATR, compression and prior-high for later bars. The claim proven by §8's locality test is
  about *verdicts*, not about indicator inputs. Scale continuity is `price_segments` /
  `unresolved_breaks`' job, and `sql/249:82` already documents the danger. ⚠ Today's carrier
  is constant across a series, so no mixed case exists yet — the obligation lands with the
  per-bar source.
- **Costing stays run-wide.** The four charge consumers read `corpus.cost_price_basis`, which
  this does not touch — preserving the #3238 A/B's separation, and leaving mixed-basis
  charging unsolved.
- **Downstream effects are not local.** Suppressing an entry changes position occupancy, later
  entries, exits, costs and comparators; `position_builder` is stateful by design.
- **Absent sessions stay absent.** The carrier describes delivered rows. A date the coverage
  join omits produces no bar and therefore no refusal.
- **The payload is unvalidated.** §2.2 — the label rests on two AAPL bars.

## 7. Versioning — and the rotation this deliberately does NOT take

`PRICE_BASIS_RULE_VERSION` goes into **`S12_PARAMS`**, not into
`strategy_registry.INPUT_RULE_SETS`.

`INPUT_RULE_SETS` is hashed into **every** `StrategyIdentity` (`strategy_registry.py:279`), so
installing `949b8d55`'s "sixth entry" rotates all 11 strategies and detaches every stored
signal, result and watermark across 24 identity-carrying tables. This rule has exactly one
consumer, so it is versioned where it is consumed.

⚠ **This is an exception to the registry's documented preference, and it is bounded rather
than argued away.** `INPUT_RULE_SETS` exists because author-maintained per-strategy coverage
drifts: a second consumer can read the rule without hashing it and no test notices. The
exception holds only while there is one consumer, so **the sixth entry is installed as part of
§6 item 3** — the change that removes `AS_TRADED_UNIVERSES` and makes the carrier reachable
from the scan — and a test asserts S-12 is the sole importer of this module until then.

⚠ **The version is COMPOSED, not a bare constant** (checkpoint 1). A constant name is not a
versioning mechanism: the carrier's source selection can change without its module changing.
Following `bar_capture_certificate.CAPTURE_CERTIFICATE_VERSION`'s own idiom —
`f"{RULE_ID}+{module_source_hash}+archives-{RESEARCH_ARCHIVES_hash}"` — so a re-declared
archive provenance moves the version even though this module's bytes did not.

### 7.1 The rotation census — corrected

The first draft claimed **zero** on the strength of two tables. Checkpoint 1 called that
incomplete and it was. Enumerating every table carrying `strategy_id` from
`information_schema.columns` — **24 tables** — gives, for `s12-%` on the dev DB:

| table | rows |
| --- | ---: |
| `strategy_signal_observations` | **5,791** (all `y2026m09`) |
| `strategy_scan_watermark` | 1 |
| `strategy_signal_daily_counts` | 1 |
| the other 21, incl. `strategy_results`, `strategy_signals`, `strategy_preregistration_declarations`, `strategy_holdout_accesses` | 0 |

**Not zero — and the content is the mitigation, not the count.** All 5,791 are
`not_evaluable / missing_market_context` under `strategy-registry-v1+b18d5869092b`, i.e. every
one is the universe-gate refusal this change leaves in place, so the new identity re-emits
identical content on catch-up. No result row, no declaration and no holdout access exists to
strand. ⚠ Dev-only; a deployment census is the operator's, not derivable from here.

## 8. Tests

Pure-logic, no DB:

1. `PriceBasisSeries` satisfies `EvaluableSeries` — asserted by declaring one as a
   `StrategyInput` and running `evaluate`, never by `isinstance`.
2. `segment()` remaps `not_evaluable_indices` and preserves `rule_set_version`; a raw slice
   does not — the `RegimeSeries` test's shape, because that is the bug being prevented.
3. `__post_init__` refuses: an out-of-range index · an unmarked `None` · a marked index
   carrying a value · **`"unknown"` as a value** · an off-vocabulary string · duplicate and
   unsorted indices.
4. `from_archive_basis` over **`sql/249`'s CHECK members parsed from the migration file**, not
   from this module's own constant — a fifth member added only in SQL must fail the test
   rather than silently default. Plus an unrecognised future token, which must refuse.
5. **The refusal is BAR-LOCAL** — one uncertified index in a 200-bar series produces exactly
   one extra `not_evaluable`, against masking's 80 (`3c6bb73f` §3.3). Scoped to verdicts, per
   §6.
6. **Reason precedence** — a bar that is both uncertified and masked reports
   `missing_market_context`; the last bar still reports `no_fill_bar`, because `evaluate`
   returns it before reading any input.
7. S-12 on a withheld basis refuses every bar; on `unadjusted` it returns the **byte-identical
   verdict list** it returns today, asserted against the pre-change output.
8. S-12 raises on a length mismatch, in both directions (short and long).
9. `segmented_signals` and `segmented_member` raise on a length mismatch and slice per
   segment — both routes, not just the per-series one.
10. The all-refused short-circuit returns the same verdicts the declared-input path would, so
    the optimisation cannot diverge from the rule.

## 9. Migration and verification

- **38 call sites** take the new required keyword — 13 in `scripts/`, 25 in `tests/`
  (checkpoint-1 AST census). ⚠ **No default that infers certification**: a default would hide
  every un-migrated site behind a silent admission, which is the fail-open this whole module
  exists to refuse.
- `_WITHHELD_POLICY_WARNING` gains S-12 signal suppression — today it names only diagnostic
  withholding and maximum charging, and after this change a withheld policy also stops a
  strategy trading.
- Rung: **behavioural change with data semantics** — no parser, no ETL, no migration, no
  stored-row rewrite. Domain skill + Codex ckpt-2 + review bot. Not a corpus change, so no
  full-population A/B.
- Reproduce the population claims:
  `SELECT adjustment_basis, count(*) FROM research_price_series GROUP BY 1` and the 24-table
  census in §7.1.

## 10. Checkpoint-1 corrections — what each finding moved

| # | finding | disposition |
| --- | --- | --- |
| 1 | the A/B substitutes only `cost_price_basis` | **CONFIRMS §0** — independently re-verified |
| 2 | the root is synthesised from `RESEARCH_ARCHIVES` literals | **ACCEPTED** → §0.1, "drift detector, not certification" |
| 3 | the homogeneity check withholds, does not block | **ACCEPTED** — §1's "only thing preventing it" was wrong |
| 4 | run-wide withholding → universal refusal is a treatment decision | **ACCEPTED** → §3.2, stated explicitly |
| 5 | `SELECT DISTINCT` does not bind metadata to loaded OHLC | **ACCEPTED** → §0.1 |
| 6, 7 | the label rests on two AAPL bars; `308d1e38` is the wrong population | **ACCEPTED** → §2.2; the `308d1e38` citation is **withdrawn** |
| 8 | `sql/305` has a documented meaning for the member | **ACCEPTED** → §2.2 now cites the reg text, not first principles |
| 9 | "no column ⇒ no certificate" repeats a withdrawn inference | **ACCEPTED** → §3.1, restated as policy |
| 10, 11 | `"unknown"` and unmarked `None` are fail-opens | **ACCEPTED** → §2.1, the sharpest finding of the pass |
| 12 | a short carrier passes silently | **ACCEPTED** → §3.1 construct-by-length + §8.8 |
| 13 | equal length ≠ alignment | **PARTLY** — §3.1 removes the class for today's source; cross-series misalignment stays open with the per-bar source |
| 14 | `rule_set_version` overridable | **ACCEPTED** → `segment()` preserves it; §8.2 |
| 15 | constructor edge cases | **ACCEPTED** → §2.1 invariants |
| 16 | "every bar / `missing_market_context`" is false | **ACCEPTED** → §4 input order + §8.6 |
| 17 | `resolve_fills` overwrites reasons | **ACCEPTED** → §6 |
| 18 | bar-local gating ≠ bar-local contamination | **ACCEPTED** → §6, and §8.5 rescoped to verdicts |
| 19, 20, 21, 36 | fills, costing, statefulness, absent sessions | **ACCEPTED** → §6 obligations |
| 22 | "counted refusal" is unimplemented for backtests | **ACCEPTED** — the phrase is dropped from §5 |
| 23, 24 | 38 call sites; `_signals_for` has no corpus arg | **ACCEPTED** → §3.1, §9 |
| 25, 26 | "unchanged" too broad; the scan rotates | **ACCEPTED** → §5's three corrections |
| 27 | `deflate_group` couples strategies | **ACCEPTED** → §5, stated conditionally |
| 28, 30 | the `INPUT_RULE_SETS` exception needs a bound | **ACCEPTED** → §7, bounded to one consumer + a sole-importer test |
| 29 | a constant name is not a versioning mechanism | **ACCEPTED** → §7, composed version |
| 31 | delivered provenance is not persisted | **DEFERRED** — real, and it belongs with the per-bar source; noted in §6 |
| 32 | the rotation census was incomplete | **ACCEPTED** — it was wrong; §7.1 re-measured over 24 tables, answer is 5,791 not 0 |
| 33 | the SQL-domain test was circular | **ACCEPTED** → §8.4 parses `sql/249` |
| 34 | one fixture proves little | **ACCEPTED** → §8 widened to 10 cases incl. both routes |
| 35 | quadratic refusal lookup | **ACCEPTED** → §4 short-circuit |
| 37 | withholding diagnostics incomplete | **ACCEPTED** → §9 |

Refs #2840, #2437.
