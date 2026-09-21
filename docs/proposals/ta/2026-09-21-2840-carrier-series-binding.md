# #2840 §8 obligation (b) — bind a certified carrier to the bars it certifies

Status: proposal, revision 4. Checkpoint 1 refused revisions 1, 2 and 3 (43, 45 then 25 findings)
— the **fourteenth, fifteenth and sixteenth** ckpt-1 refusals on this ticket. §10 carries the
disposition. Every revision had a load-bearing claim falsified by counterexample; those are
corrected in place, not softened.

Ticket: #2840 (closed; the provenance line continues under it). Queue: #2437.
Measured at `ac107806`, branch `feature/2840-carrier-series-binding`, dev DB `ebull`,
2026-09-21 ~01:00-02:00 UTC.

## 0. Why this, and why obligation (b) ALONE

`ac107806` named four heads. Two are wall-clock gated to Monday's US open (read the first real RTH
session; then two or more pre-open re-observation fires) — `strategy_intraday_harvest` reports
`skipped` outside its window and it is Sunday evening ET. Same gate as last session.

The other two are §8's obligations. `ac107806` said they must land together because both edit
`app/services/strategy_price_basis.py`, whose sha256 is inside `PRICE_BASIS_RULE_VERSION`, inside
`S12_PARAMS` — so separately they are two identity rotations for the same 5,791 rows.

**That cost premise measures at zero on this database today (§7), so the round splits.** Obligation
(a) needs its own round because revision 1's attempt to *refuse* it was falsified (§2) and what
survives is a different mechanism with an open design question.
⚠ The zero is snapshot-specific: if an intermediate identity runs and writes before obligation (a)
lands, the second rotation stops being free. §7 records the recheck.

## 1. The defect, reproduced

```
s12_signals(series_A, …, price_basis=from_archive_basis("unadjusted", n_bars=len(series_B)))
    -> Counter({'not_evaluable': 114, 'not_fired': 60, 'fired': 1})     # accepted, and FIRES
```

A same-length carrier built for another series is accepted, and S-12 **fires** on it — the fail-open
in the only gate between S-12's nominal `>= $100` band and a history the provider back-adjusts at
fetch time.

**Guard inventory, corrected twice.** Four call sites, but only three are alignment checks:

| site | what it checks |
| --- | --- |
| `s12_signals` | `len(price_basis) == len(series)` |
| `segmented_signals` (`strategy_segmented_evaluation.py:48`) | same, pre-slice |
| `segmented_member` (`:118`) | same, pre-slice |
| `PriceBasisSeries.segment` | **slice bounds against its own length only** — not alignment to any external series. It will truncate an oversized carrier without complaint, which is why the two pre-slice checks exist. |

The three alignment checks catch a wrong-LENGTH carrier. **None catches an equal-length foreign one.**

⚠ `segmented_signals` is the S-12 path (`run_signal_scan` dispatches on
`entry.strategy_class == "per_series"`, `strategy_signal_scan.py:882`; S-12 has no member function).
`segmented_member` serves the two cross-sectional strategies, S-2 and S-10. The manifest holds
twelve strategies; the other nine per-series adapters take `price_basis` and discard it.

## 2. ⛔ Obligation (a) is OPEN — revision 1 refused it and the refusal was falsified

### What survives: the measurement that corrects Option 3's literal form

`PRICE_BASIS_RULE_VERSION` = `price-basis-carrier-v1 + sha256(strategy_price_basis.py)[:12] +
archives-<hash>`, and it IS `S12_PARAMS["price_basis_rule"]`
(`s12_cheapest_band_price_gated_breakout.py:251`). Both constructors live in that file. Probe —
append exactly `b"\n# probe: does a byte change here rotate S-12?\n"`, recompute in a FRESH
interpreter (module constants are computed at import), `git checkout --` to revert:

```
before: strategy-registry-v1+6b4b504f4fdf   price-basis-carrier-v1+f41fc699d0d4+archives-8fc73244537c
after : strategy-registry-v1+48548d417540   price-basis-carrier-v1+8b1da59ea43e+archives-8fc73244537c
```

⇒ the routing **policy** is already inside S-12's identity, so §8's Option 3 read literally
("extract the routing into its own module and hash it") buys nothing. Codex agrees:
*"extracting constructor bodies alone would indeed accomplish nothing."*

### ⛔ What was FALSE: "no hash can reach a call-site callee swap"

Revision 1 gave three impossibility arguments. All three were refused, the first by counterexample:

1. *"Hashing the scan into the strategy is an import cycle."* **False** — `Path.read_bytes()` is
   not an import. A digest of `strategy_signal_scan.py` added to S-12's params rotated the identity
   `e9d0b1304427 → 1718e47d3091` under a simulated callee swap.
2. *"Unrelated scan edits would rotate S-12."* True, but a **tradeoff, not an impossibility** — and
   a narrower digest over the AST of the `price_basis=` argument expressions also detected the swap
   without that sensitivity. ⚠ Incomplete as a construction: it misses an unchanged expression
   whose imported alias or wrapper changed meaning.
3. *"A runtime route declaration comes from the caller being edited."* Assumes away independent
   authority — a trusted run context can establish the expected route and enforce it against the
   carrier, and supporting two routes does not require accepting either in every context.

⇒ obligation (a) stays **OPEN**, with three candidate constructions (whole-file digest · AST-slice
digest · run-context route declaration) and no decided answer.

⚠ Third safety-shaped claim of mine inverted at ckpt-1 on this ticket, after `ac107806`'s "this
diff touches nothing hashed" and "obligation (b) is not reachable". **To write "X cannot happen",
try to make it happen and report the output.** Revision 1 wrote three impossibility arguments and
attempted none.

## 3. Source rule — the binding, fixed BY CONSTRUCTION

No published formulation governs "what binds a provenance certificate to the data it certifies"
— it is not an SEC/EDGAR matter and no market-data standard covers it, so per the repo rule it is
fixed by construction and frozen in `PRICE_BASIS_RULE_VERSION` (which hashes the module defining
it), not picked.

**What it must distinguish** (`ac107806` §5): another instrument · changed dates · revised prices.

### The subject is the SERIES, not the certified bar — revision 2's scoping was exploitable

Revision 2 bound only indices carrying a certification. Codex broke it: on `_above_edge()`, certify
every bar except 169, bind with `close[169] = 100.3`, substitute `100.0` — every binding passes and
certified bar **170 flips `not_fired → fired`**. A second variant moved only uncertified `date[169]`
across a break and shifted segment bounds `((0,170),(170,175)) → ((0,169),(169,175))`.

The carrier's own docstring already said why: *"An uncertified bar's OHLC still feeds ATR,
compression and prior-high for later bars."* A certified verdict is a function of the whole prefix.

⇒ **when a carrier certifies anything, it binds EVERY bar.**

### A certification requires bindings; the converse is NOT required

A carrier that certifies nothing cannot certify a foreign bar: every verdict is the basis refusal
regardless of which series produced it, and the single length-dependent difference (`no_fill_bar`
at the last index) is already covered by the three length checks. So the invariant is **one
directional**:

```
any(v is not None for v in values)  ⇒  len(bar_bindings) == len(values)
bar_bindings is otherwise () or len(values) long — both are legal
```

⚠ Revision 3 wrote this as an *iff* and Codex broke it twice. (i) Slicing the uncertified suffix of
a mixed carrier yields non-empty bindings with nothing certified — legal segmentation that the iff
would have made raise. (ii) The iff's second half was never enforceable anyway, because `values`
was only *annotated* as a tuple: construct all-`None` with `bar_bindings=()`, then mutate the list
to certify index 170 and drop its refusal index, and `_above_edge()` fires through the emptiness
shortcut. Reproduced. ⇒ `values`, `not_evaluable_indices` and `bar_bindings` are now checked to be
`tuple` at runtime, which also closes a pre-existing hole this diff did not create.

The cost still lands where certification does: the live scan (`from_undeclared_source`) binds
nothing; the archive path binds every bar.

### The binding value is a `repr`-ENCODED STRING — three earlier designs were refused

```python
bar_bindings[i] = "|".join((dates[i].isoformat(), *(repr(row.get(f)) for f in _BOUND_FIELDS)))
_BOUND_FIELDS = ("open", "high", "low", "close", "volume")
```

* ⛔ **Revision 1 carried the `BarSeries` itself.** `OHLCVRow` is a plain mutable `dict`, so carrier
  and series alias the same objects — `rows[i]["close"] = …` moves both sides and equality still
  passes. A reference is not a snapshot.
* ⛔ **Revision 2 reused `corpus_generation.encode_bar`, extracted from `add_series`.** The
  extraction as specified framed date and fields with `\x1f` where `add_series` uses `\x1e`, and
  adopting it changed a fixture stamp `1e9669b247b46873 → ea8371d1b7d613a6` — a corpus-wide
  regression §7's S-12 disposition would not have covered. `encode_value`'s injectivity proof is
  bounded to `numeric` columns and covers neither `int` volume nor float fixtures, and it renders
  `Decimal("0.1")` and `0.1` both as `"0.1"`. Worse, using it as an **enforcement gate** invalidates
  `_NOT_IDENTITY_INPUTS`' own reason for excluding `CORPUS_GENERATION_RULE_VERSION` from strategy
  identity (*"never affects what a strategy decides"*), so a passing engine-walk test would stop
  proving coverage.
* ⛔ **Revision 3 used a tuple of the raw values.** I asserted it was "type-aware" and
  scale-sensitive. Measured, in this interpreter:

  ```
  (Decimal("1.50"),) == (Decimal("1.5"),)   True     ← scale collapses
  (Decimal("1.5"),)  == (1.5,)              True     ← type collapses
  n = Decimal("NaN"); (n,) == (n,)          True     ← identity shortcut in tuple comparison
  (Decimal("NaN"),)  == (Decimal("NaN"),)   False    ← so acceptance depends on object sharing
  hash((Decimal("sNaN"),))                  TypeError
  ```

  Every one of those is a spurious MATCH or an unhashable carrier, and the NaN pair is worse than
  either: the same history binds or fails depending on whether the loader happened to share the
  object.

`repr` per field fixes all of it, measured in the same interpreter: `Decimal('1.50')` ≠
`Decimal('1.5')`, `Decimal('1.5')` ≠ `1.5`, `1` ≠ `True`, `None` ≠ `Decimal('0')`, two independently
built `Decimal("NaN")` encode equal (deterministic, no identity dependence), and the result is a
`str` — always hashable, always immutable, no mutable leaf to validate.

⚠ `row.get(f)`, never `row[f]`. S-12 tolerates a missing OHLC key today through its own `.get()`
consumers, and indexing would raise `KeyError` on a bar the strategy currently evaluates —
reproduced for each of open/high/low/close. A missing field encodes `'None'`, which is exactly what
a masked field encodes, and masked fields are `None` in production by design.

⚠ `str(Decimal)` — which `repr` wraps — is context-dependent only through `capitals`, which changes
`E` to `e` in exponent form. It cannot make two different values render alike, so a context change
can only produce a spurious MISMATCH, never a spurious match. Fail-closed.

⚠ Extra keys in a row `dict` are dropped. Checked: no inspected consumer reads a field outside
`_BOUND_FIELDS`, so a dropped key has no demonstrated effect — stated as a limit, not as a proof.

⚠ This is a deliberate DEPARTURE from *reuse > reinvent*: the encoding is defined here rather than
borrowed from `corpus_generation`, because of the rule-set coverage contradiction above. Defining
it here also versions it correctly — `PRICE_BASIS_RULE_VERSION` hashes this module, so a change to
the encoding rotates S-12's identity, which a `corpus_generation`-borrowed encoder would not.

### Stated non-guarantees — each one a Codex counterexample, kept rather than softened

1. **`BarSeries`' derived caches are not bound.** `float_closes` / `float_highs` / the numpy arrays
   are `cached_property` state outside `dates`/`rows`. Codex populated the cache, changed raw
   `close[170]` to `99`, then built bindings: the snapshot matches the raw rows and S-12 still fires
   on the cached `100.2`. ⚠ Revision 2 called this undetectable; that was **wrong** — binding the
   consumed arrays, or evaluating from the snapshot, would catch it. It is a **scope decision**:
   the fix belongs to `BarSeries`, which presents tuple fields and frozen semantics while caching
   mutable lists, and a carrier-side patch would cover S-12's three caches and leave every other
   consumer of the same object exposed. Named in §9 as open.
2. **Validation and consumption are not atomic.** Rows stay mutable after `binding_mismatch`
   returns. Closing that needs snapshot-based evaluation (non-guarantee 1's fix), not a better key.
3. **Equal payload transfers the claim.** Two different instruments with identical dates and OHLCV
   bind to each other. ⚠ Revision 2 justified this as "provenance is a claim about levels"; that is
   not a proof — an adjusted instrument can coincidentally equal another's unadjusted prices. It is
   an accepted residual of excluding instrument identity, stated as one.
4. **Instrument id is not bound.** It is available to the scan caller but not on `BarSeries`.
   Excluded because it is not the subject of the claim; recorded in §9 as the open alternative,
   not as impossible.

## 4. Scope

`app/services/strategy_price_basis.py` — the only production module whose behaviour changes.

* `PriceBasisSeries` gains `bar_bindings: tuple[str, ...] = field(default=(), repr=False)`.
  ⚠ **Defaulted, and that is fail-CLOSED rather than a convenience.** The invariant refuses a
  carrier that certifies anything without full-length bindings, so the default can only ever
  produce the harmless all-uncertified shape — which is exactly what `from_undeclared_source`
  returns. Making it required would have forced `bar_bindings=()` onto ten existing refusal-path
  constructions for no safety gain. `repr=False` so a carrier's `repr` does not print a whole
  price history.
* `__post_init__` enforces, at runtime and not by annotation: `values`, `not_evaluable_indices` and
  `bar_bindings` are each a `tuple`; every binding is a `str`; `bar_bindings` is `()` or
  `len(values)` long; and it is full-length whenever any value is certified.
* `segment(start, end)` slices `bar_bindings` when present, preserves `()` when not — legal under
  §3's one-directional invariant even when the slice certifies nothing.
* `binding_mismatch(series: BarSeries) -> str | None` — the single implementation of the invariant.
  `None` when bound or when the carrier binds nothing. Deterministic diagnostic: length first, then
  ascending index, reporting the **lowest** differing index whatever component differs. There is no
  second spelling (`binds()` does not exist).
* `from_archive_basis(adjustment_basis, *, series: BarSeries)` and
  `from_undeclared_source(*, series: BarSeries)` replace `n_bars`.
  ⚠ Removes the `n_bars < 0` raise. *Does my correction remove anything my gate keeps?* No — the
  guard existed because an `int` can be negative and a `BarSeries` cannot, so the class becomes
  unconstructible rather than refused. The ±1 alignment tests are **kept**, re-expressed against
  genuinely different-length series built directly.
* ⚠ Imports `BarSeries` / `OHLCVRow`; no cycle (`indicator_series` imports only
  `technical_analysis`), no new rule set in the scan closure (`indicator_series.RULE_SET_VERSION`
  is already reachable and covered). **No `corpus_generation` dependency** — see §3.

Enforcement, **beside** each length check and never replacing it (the length message documents a
distinct hazard: `evaluate` returns `no_fill_bar` for the last bar before reading any input, so a
one-short carrier is never looked up):

`s12_signals` · `segmented_signals` · `segmented_member`. Not `PriceBasisSeries.segment`, which has
no external series to check against.

### Call-site inventory — measured, not estimated

Production ×4: `strategy_signal_scan.py:987`, `:1030`; `backtest_run.py:3237`, `:3406`.
Scripts ×6: `verify_2394_backtest_run.py`, `verify_2394_signal_scan_cost.py` (three distinct
subjects — `series`, `same_day`, `tail`; each binds to its OWN series, no blanket substitution),
`verify_2394_strategy_manifest.py`, `verify_2437_missing_market_context.py`,
`verify_2437_s10_census.py`, `census_2840_s12_signal_supply.py`.
Tests ×9: `test_2840_price_basis_carrier.py`, `test_2840_s12_price_basis_gate.py`,
`test_2840_cheapest_band_price_gated_breakout.py`, `test_2840_carrier_source_selection.py`,
`test_2840_volatile_regime_gated_breakout.py`, `test_strategy_manifest.py`,
`test_2437_missing_market_context.py`, `test_2437_s10_relative_strength.py`,
`test_backtest_run.py`.

**No schema change, no migration, no backfill.** The identity rotation is unavoidable; §7 disposes.

## 5. What this does NOT do

Does not certify the payload · does not bind instrument id (§3 n-g 4) · does not defend against
`BarSeries` cache mutation or post-check mutation (n-g 1, 2) · does not touch the four charge
consumers reading `_Corpus.cost_price_basis` · does not close obligation (a) (§2).

## 6. Cost

Zero on the live scan (nothing certified ⇒ no bindings built, `binding_mismatch` returns after the
emptiness test). `O(bars)` tuple construction plus `O(bars)` tuple comparison on the archive path,
paid once per carrier and once per enforcement site.

⚠ Revision 1's pointer-compare claim is **withdrawn** — it rested on carrying the `BarSeries`, and
container identity does not bypass traversal in CPython. §8.6 measures instead of arguing, on both
routes, and reports the numbers whatever they are.

## 7. Identity rotation + evidence disposition

Editing `strategy_price_basis.py` rotates `PRICE_BASIS_RULE_VERSION` → `S12_PARAMS` → S-12's
`strategy_version` (§2's mechanism).

**Population** — base tables only, partitions and views resolved rather than counted as peers:

```sql
SELECT c.relname, c.relkind, i.inhparent IS NOT NULL AS is_partition
  FROM information_schema.columns col
  JOIN pg_class c ON c.relname = col.table_name
  LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
 WHERE col.column_name = 'strategy_version' AND col.table_schema = 'public'
 ORDER BY 1;
```

and per relation:

```sql
SELECT strategy_version, count(*) AS rows,
       count(*) FILTER (WHERE corpus_generation IS NULL) AS null_generation,
       count(DISTINCT reason_code) AS reasons, count(DISTINCT verdict) AS verdicts,
       min(signal_bar_date), max(signal_bar_date)
  FROM <relation> WHERE strategy_id = 's12-cheapest-band-price-gated-breakout'
 GROUP BY 1 ORDER BY 2 DESC;
```

⚠ Revision 2 reported "exactly three relations" from a column enumeration that also returns the
three `strategy_signal_observations_y2026m*` partition children and the `strategy_results` **view**.
Corrected: the parent is queried, the partitions are its rows, and `strategy_results_store` is
queried as the base relation behind the view. Both results are published in the PR's evidence
table, not summarised.

Measured at `ac107806` on dev `ebull`: all stored S-12 evidence sits on
`strategy-registry-v1+b18d5869092b` (observations 5,791 rows; daily counts 1 row, `row_count`
5,791; watermark frontier 2026-09-18), against current identities `6b4b504f4fdf` (survivor_only)
and `e711e9312913` (survivorship_free). `strategy_signals` holds no S-12 rows at any version.

⇒ **the rotation detaches nothing that is still attached** — the stored evidence was already
detached before this diff. ⚠ Snapshot-specific: re-run at merge time, because an intervening scan
that writes under `6b4b504f4fdf` would change the answer.

⚠ **Restart behaviour.** `read_watermarks` keys `(strategy_id, strategy_version)`, so a new version
has no watermark and `write_window_indices` takes its cold-start branch — `range(end - 1, end)`,
**at most one eligible bar** per instrument (empty for series under two bars or with no eligible
pre-frontier index), deliberately, because spec §11 forbids backfill. It does not catch up over the
stale version's dates. Not created here: `6b4b504f4fdf` already has no watermark and no rows.

## 8. Acceptance

1. `uv run pytest` over the nine changed test modules plus `test_strategy_registry.py`
   (engine-walk) and `test_corpus_generation.py` — exit 0, read from the exit code, never from a
   piped tail.
2. **The hole revision 2 shipped is closed, in both variants.** Partially-certified carrier with
   uncertified `close[169]` changed; and uncertified `date[169]` moved across a break. Both must
   now raise. Before: the certified verdict at 170 flips with nothing raising.
3. **The foreign-carrier fail-open is closed**, three same-length fixtures through both
   `s12_signals` and `segmented_signals`: (a) one price changed; (b) dates shifted; (c) both.
   Before: §1's `Counter({'not_evaluable': 114, 'not_fired': 60, 'fired': 1})`.
4. **New invariant tests**, since existing green tests cannot supply them: both `__post_init__`
   violations (certified-without-bindings, wrong-length bindings), non-tuple and wrong-arity
   elements, mutation after construction, mixed certified/uncertified carriers, nested and
   non-zero-offset segments, empty and singleton series, and a mismatch reaching through
   `segmented_member` (the S-2 / S-10 path).
5. **`ac107806`'s tripwire still fails under mutation** — swap the scan call site to a certifying
   constructor, confirm `test_2840_carrier_source_selection.py` fails at its intended assertion,
   revert, re-run the unmodified control green.
6. **Full population — a binding sweep, plus a REDUCTION, not a verdict-level A/B.**
   ⚠ Two earlier proposals are refused and dropped. `census_2840_s12_signal_supply.py` cannot be
   the arm (it fixes `survivorship_free`, ends before 2021-06-29, applies universe/opportunity
   filters that skip a measurable membership class, certifies through a literal `"unadjusted"`
   rather than `liquidity_policy.adjustment_basis`, and emits aggregates). Nor can a two-worktree
   verdict-tuple diff: the baseline has no `binding_mismatch` to time, `except TypeError` is an
   unsafe signature discriminator, two invocations do not share a snapshot, `segmented_member`
   emits `None` for unresolved candidates so its tuples are incomplete, and a set comparison hides
   duplicate-output regressions.

   What is actually owed is bounded, and each part is separately checkable:

   a. **The guard never fires on production data.** `scripts/ab_2840_carrier_binding.py`,
      read-only under `REPEATABLE READ`, over the full scan population loaded exactly as
      `run_signal_scan` loads it: build both production routes' carriers
      (`from_undeclared_source`, and `from_archive_basis("unadjusted", …)` for the certified
      route), call `binding_mismatch`, and also exercise `segment()` at every
      `series_segment_bounds` boundary. Report instruments expected / loaded / skipped /
      carriers built / bars bound / segments checked / mismatches, **per route**, so a zero
      cannot be vacuous. Expected: 0 mismatches. A mismatch **stops the round**.
   b. **The constructors' observable output is unchanged.** `values` and `not_evaluable_indices`
      are a pure function of `(adjustment_basis, n)`; an exhaustive table test pins the new
      constructors against the old outputs across every `CERTIFYING_ARCHIVE_BASES` member, the
      withheld case, and n ∈ {0, 1, 2, 175}. No corpus needed, and no second worktree.
   c. **Nothing else reads `bar_bindings`.** `rg` over `app/`, `scripts/` and `tests/`, published
      in the PR.

   (a) ∧ (b) ∧ (c) ⇒ verdicts are unchanged, because the only new behaviour is a raise that never
   occurs. ⚠ Stated as the reduction it is: this is **not** a verdict-level A/B, and it does not
   reproduce `backtest_run._resolve_liquidity_policy`. Recorded so the coverage claim is not
   overread.
7. **Cost, on pinned inputs and on both routes.** Time carrier construction and `binding_mismatch`
   per bar at fixed series lengths, and report (a)'s sweep wall-clock. The baseline has no
   `binding_mismatch`, so the comparison is old-construction versus new-construction-plus-check —
   stated as that, not as a like-for-like method timing. Report μs/bar and total; a regression is
   reported with its numbers, never silently accepted.
8. Identity rotation recorded from a fresh interpreter, and §7's queries re-run verbatim after the
   diff and again at merge time.

## 9. What stays open on #2840

* **Obligation (a)** — §2, three candidate constructions, none decided.
* `BarSeries` cached-property mutability (§3 n-g 1) — pre-existing, now named with its reproduction.
* Binding instrument identity (§3 n-g 3, 4) — the accepted residual.
* Read the first real RTH session (wall-clock, Monday's open); then two or more PRE-OPEN
  re-observation fires so a bracket can lie inside `(T, O)`.
* The per-bar price-basis source; today's carrier is uniform per series.
* ⚠ Still not breadth; still do not re-open "which 8-K item carries forward notice" from form text.

## 10. Checkpoint-1 disposition

Three passes, 113 findings. Verbatim output: `/tmp/ckpt1_2840_binding.txt`, `/tmp/ckpt1b_2840.txt`,
`/tmp/ckpt1c_2840.txt`; the PR carries the table. What CHANGED the design — the part worth reading:

| pass | finding | effect |
| --- | --- | --- |
| 1 | `OHLCVRow` is a mutable dict — carrying the `BarSeries` is not a snapshot | design replaced |
| 1 | three impossibility arguments for obligation (a), one refuted by counterexample | §2 reopened |
| 1 | `float_closes` cache mutation flips a verdict with nothing raising | non-guarantee 1 |
| 2 | binding only certified bars is exploitable via an uncertified prefix bar | **bind every bar** |
| 2 | the `encode_bar` extraction changes a corpus stamp; `encode_value`'s proof does not cover the carrier's inputs; reuse invalidates `_NOT_IDENTITY_INPUTS` | `corpus_generation` reuse dropped |
| 2 | the census cannot be the A/B arm (population, determinism, aggregates) | §8.6 rewritten |
| 2 | `information_schema` enumeration mixes partitions and views | §7 population corrected |
| 3 | tuple equality collapses scale and type, matches shared NaNs, and `hash` raises on `sNaN` | **encoding is `repr`-based** |
| 3 | the *iff* invariant breaks legal segmentation, and `values` was only annotated as a tuple | one-directional invariant + runtime tuple checks |
| 3 | `row[f]` raises where S-12 tolerates a missing key via `.get()` | `row.get(f)` |
| 3 | a two-worktree verdict-tuple diff is not a valid A/B (no baseline method, unsafe shim, no shared snapshot, incomplete member tuples, set-hides-duplicates) | §8.6 replaced by sweep + reduction |

Explicitly retained as open or partial, not silently dropped: pass-1 #9, #12-13, #15-16, #18,
#21-22, #25, #30-39, #41-42 · pass-2 #3, #4-5, #7, #16-18, #23-24, #27-28, #30-38, #41-43 ·
pass-3 #4 (an all-`None` carrier's refusal *locations* do move when dates move — the uniform-outcome
rationale is corrected, but a fresh refusal carrier on the changed series gives identical results,
so it is not unsafe certification), #13-24 (§8.6's replacement), #25 (checked boundary, no change).
Each is a stated non-guarantee (§3), an acceptance item (§8) or an open item (§9).
