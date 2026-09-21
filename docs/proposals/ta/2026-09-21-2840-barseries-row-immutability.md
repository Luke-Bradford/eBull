# `BarSeries` row immutability — closing `binding_mismatch`'s non-guarantee 1

Refs #2840. Refs #2437. Head named by `b9b2a1e5`'s close-out.

Predecessor: `docs/proposals/ta/2026-09-21-2840-carrier-series-binding.md` (§ "Stated
non-guarantees", item 1).

**Revision 3.** Two checkpoint-1 refusals, 28 findings then 29 — the seventeenth and
eighteenth on this ticket.

* **Revision 1** was refused on its **frame**: it claimed to make cache desynchronisation
  *impossible*, and six bypasses survive the design.
* **Revision 2** fixed the frame and then **introduced two defects of its own** — a
  `__setstate__` that destroyed a live instance on a bad payload, and a new public type
  that copied an existing false non-null contract. It also asserted three dispositions
  (`FIXED`, `MOOT`, `EMPTY DOMAIN`) that did not survive being checked.

Everything below was reproduced in this interpreter or measured against the repo before
being written. Disposition tables: §9 (28 findings) and §10 (29 findings).

---

## 1. The defect, reproduced before it was specced

`BarSeries` is `@dataclass(frozen=True)` with `rows: tuple[OHLCVRow, ...]`.
`OHLCVRow` is a `TypedDict` — a plain mutable `dict`. `frozen=True` is **shallow**: it
overrides `__setattr__` so a field cannot be rebound, and says nothing about the
objects the fields point at.

`float_closes` / `float_highs` / `float_lows` / `array_closes` / `array_highs` /
`array_lows` are `cached_property`, which writes into `instance.__dict__` directly and
therefore caches legally on a frozen instance (the module comment at lines 136-144
explains exactly this). The cache is a snapshot; the rows it was taken from are not.

```
float_closes (warm): [10.5, 10.5, 10.5]
array_closes (warm): [10.5 10.5 10.5]

s.rows[1]["close"] = Decimal("999")

closes  (live)  : [Decimal('10.5'), Decimal('999'), Decimal('10.5')]
float_closes    : [10.5, 10.5, 10.5]      <- stale
array_closes    : [10.5 10.5 10.5]        <- stale
DIVERGED        : True
```

No exception, no warning.

## 2. Why this is more than hygiene — it circumvents the guard `b9b2a1e5` shipped

`PriceBasisSeries.binding_mismatch` validates the carrier against `series.rows`. The
strategy then computes from the caches. Those are two different truths, and the
sequence that separates them is four lines:

1. build `BarSeries` whose rows hold `V_fake`;
2. read any float cache — it warms at `V_fake`;
3. mutate the rows to `V_real`;
4. build the carrier from the series (`bar_bindings` encode `V_real`) and call
   `s12_signals`.

`binding_mismatch` compares `V_real` against `V_real` and returns `None`. The strategy
evaluates `V_fake`.

⚠ **Corrected from revision 1** (ckpt-1 finding 18). Revision 1 said step 2 warms
`array_closes` and that "nothing reads `rows` for its arithmetic". Both are wrong, and
the corrections cut in opposite directions:

- S-12 consumes **`float_closes`**, not `array_closes`
  (`s12_cheapest_band_price_gated_breakout.py:188` says so in its own comment). Warming
  `array_closes` nevertheless warms `float_closes`, because `array_closes` is built
  from it — so the exploit is reachable either way, and the corrected step 2 is
  "read any float cache".
- **S-5 and S-6 do read raw `rows` for arithmetic**: `_volumes(series)`
  (`s5_support_bounce.py:148`) walks `series.rows` for volume, because there is no
  `array_volumes` cache. So volume has no staleness window at all today, and freezing
  the rows is what protects it.

`strategy_price_basis.py:389` already states this as a known non-guarantee. The defect
belongs to `BarSeries`, so the fix belongs there — a carrier-side patch would cover
S-12's three caches and leave the other eleven strategies and every non-strategy
consumer (`position_builder`, `outcome_resolver`, `residual_confluence_evaluation`,
`signal_ledger`, `strategy_outcome_resolution`) exposed.

## 3. ⚠⚠ What this does NOT do — the frame revision 1 got wrong

**This is an accident control, not a boundary.** It is the same class as
`app/security/unattended_guard.py`, whose own docstring is the precedent: *"It lives in
a repo you can edit, so it constrains a confused run, not a determined one."*

The threat model is our own code. There are seventeen consumer sites holding aliases
into a shared mutable structure; the failure this prevents is one of them writing
through an alias by mistake. It is not an adversary, and the design must not pretend to
stop one — revision 1 did, and six bypasses were built against it. Reproduced here on
Python 3.14.4:

| # | bypass | reproduced output |
| --- | --- | --- |
| 1 | `mappingproxy` hands its **backing dict** to a reflected `__eq__`: `proxy == Mutator()` where `Mutator.__eq__(self, backing)` writes to `backing` | `1.5 -> 99.9   LEAKS=True` |
| 2 | `array_closes.setflags(write=True)` — the array owns its storage, so read-only is reversible | `REVERSIBLE -> 9.0` |
| 3 | `.shape` / `.dtype` / `.strides` stay mutable on a read-only array | `.shape on RO array: MUTABLE -> (2, 2)` |
| 4 | `vars(series)["float_closes"] = fake` writes the cache slot directly; `vars(series)["rows"] = "not even a tuple"` replaces the field | `float_closes now (0.0, 0.0)`, `'not even a tuple'` |
| 5 | a `dict` subclass whose `copy()` returns `self` defeats `MappingProxyType(r.copy())` | `proxy sees 42  LEAKS=True` |
| 6 | subclassing `BarSeries` and overriding `__post_init__` without `super()` | leaves rows mutable |

| 7 | the copy is SHALLOW — a `Decimal` subclass with a stateful `__float__` stays shared | live `99` vs cached `1.5` |
| 8 | a preceding mixin whose `__init_subclass__` omits `super()` swallows the subclass refusal | `Child(Swallow, BarSeries)` builds with mutable rows |

Bypasses 1-4, 7 and 8 survive and are **named, not closed**. Bypass 5 **is** closed
(§4.1). Bypass 6 is closed only for ordinary subclassing; 8 is its residue.

⚠ **Revision 2 said each surviving bypass "needs code whose only purpose is the
bypass". That is false, and ckpt-1 was right to refuse it.** `arr.shape = (n, 1)` is
ordinary numpy — a helper that normalises shape in place would corrupt the shared
cache's dimensions with no ill intent at all. The claim is not worth repairing: a
control should be described by **the operations it refuses**, never by a guess at the
caller's intent. §7 asserts operations, and the docstring lists them.

⚠ **"Two independent gates" is also too broad.** pyright accepts
`series.array_closes[0] = 999` with no diagnostic — numpy has no static write
protection. The rows and the float tuples get two gates (pyright + runtime); **the
arrays get one** (the runtime `WRITEABLE` flag). Stated per surface, not in aggregate.

## 4. The construction

### 4.1 Rows — copy, then wrap

`BarSeries.__post_init__` replaces `rows` with

```python
tuple(MappingProxyType(dict(row)) for row in self.rows)
```

installed through `object.__setattr__` (the field is frozen). Both halves are
load-bearing:

| half | what it closes | what it leaves open alone |
| --- | --- | --- |
| `dict(row)` | the caller's retained dict can no longer reach the series | `series.rows[i][f] = v` still writes |
| `MappingProxyType` | `series.rows[i][f] = v` raises `TypeError` | a proxy is a **view**: `MappingProxyType(callers_dict)` tracks the caller's mutations |

⚠ **`dict(row)` and not `row.copy()` — changed from revision 1 on ckpt-1 finding 5.**
Revision 1 chose `.copy()` because `mappingproxy.copy()` is C-level and three times
faster from a proxy. But `mappingproxy.copy()` **delegates to the backing object's
`copy`**, so a `dict` subclass returning `self` passes straight through. Measured both
ways in this interpreter:

```
dict-subclass copy()->self : proxy sees 42  LEAKS=True
dict(e) instead            : proxy sees 42  LEAKS=False
```

The cost of choosing correctness here, min of 5 reps, N = 200,000:

| input | `MappingProxyType(dict(r))` | `MappingProxyType(r.copy())` |
| --- | --- | --- |
| plain dicts | 0.060 µs/bar | 0.058 µs/bar |
| already-frozen proxies (the slice path) | **0.275 µs/bar** | 0.131 µs/bar |

So the slice path pays ~0.14 µs/bar more than revision 1 assumed. §6 measures what that
is worth end-to-end rather than asserting it is negligible.

### 4.2 The caches are in scope too — what the excluded subset feeds

`b9b2a1e5`'s second process lesson is that *a guard scoped to "only the part that
matters" is the shape that keeps being exploitable*. Freezing `rows` alone would do
exactly that, because the strategies read the caches and the caches are handed out by
reference (`closes = series.float_closes`; `LevelScan.build(highs=series.array_highs,…)`).
`float_closes[i] = 999` and `array_closes[i] = 999` both succeed today.

- `float_closes` / `float_highs` / `float_lows` return `tuple[float | None, ...]`.
- `array_closes` / `array_highs` / `array_lows` call `setflags(write=False)`.

⚠ `setflags(write=False)` is reversible (§3 bypass 2) and leaves `.shape` mutable (§3
bypass 3). It stops `arr[i] = x`, which is the accident, and nothing more. Stated in the
code, not softened.

⚠ `closes` (the `Decimal` property) is deliberately unchanged: it is a plain `property`
rebuilt from `rows` on every access, so it is never stale, and the list it returns is a
fresh object no other reader holds.

### 4.3 `dates` is coerced, not merely annotated (ckpt-1 finding 7)

`dates: tuple[date, ...]` is unenforced at runtime — a caller can pass a list, retain it,
and mutate ordering or length after `__post_init__` has validated it. One line closes it:
`object.__setattr__(self, "dates", tuple(self.dates))`, before the ordering walk.

### 4.4 Subclassing is refused — and the refusal is itself suppressible

`__init_subclass__` raises. Nothing subclasses `BarSeries` in this repo, so refusing
ordinary subclassing costs nothing and closes §3 bypass 6.

⚠⚠ **It is NOT a guarantee, and revision 2 leaned on it as one.** `__init_subclass__`
only fires if every preceding class in the MRO cooperates. A mixin whose own hook omits
`super()` swallows it — reproduced:

```
class Swallow:              def __init_subclass__(cls, **kw): pass
class Child(Swallow, BarSeries):  def __post_init__(self): pass
-> MRO subclass bypass <class 'dict'> [Decimal('123')] (999.0,)
```

Revision 2 used this guard to justify a hardcoded two-field `__getstate__`, and ckpt-1
showed the consequence by construction: such a subclass with `extra=42` restored
`extra=7` through copy, deepcopy **and** pickle. So the guard is kept as an accident
control and **nothing else depends on it** — §4.5 enumerates `dataclasses.fields`
instead. A test pins the gap so it cannot later be read as absolute.

### 4.5 Serialization — preserved where it can be, and every loss named

Measured, on a `BarSeries` whose rows are proxies:

```
copy.copy      : OK
copy.deepcopy  : FAIL TypeError: cannot pickle 'mappingproxy' object
pickle         : FAIL TypeError: cannot pickle 'mappingproxy' object
asdict         : FAIL TypeError
astuple        : FAIL TypeError
```

`BarSeries` is picklable today. Nothing crosses a process boundary
(`synthetic_control_run`'s workers receive an index; its `initargs` carry
`_SharedMemberInputs`, which is names and offsets) and nothing in `app/` or `scripts/`
calls `pickle`, `asdict` or `astuple` on one — so every loss is latent. That is exactly
why none of them may ship silently.

`__getstate__` / `__setstate__` restore pickle **and** `deepcopy` (both route through
these hooks):

- `__getstate__` enumerates `dataclasses.fields(self)` and replaces `rows` with plain
  dicts, dropping the warm caches. Today's default ships `self.__dict__`, which includes
  whatever `cached_property` values were warm — derived data on the wire the receiver can
  recompute. ⚠ Enumerating rather than hardcoding two names is the §4.4 fix: the
  hardcoded version silently reverted a subclass field to its default.
- `__setstate__` **builds the replacement first, and only then commits**:
  `replacement = type(self)(**state)` → `self.__dict__.clear()` → copy the validated
  fields across. Clearing drops any already-warm cache, which is the original desync
  reached by another door.

⚠⚠ **Revision 2 had this backwards and it was a new defect, not an inherited one.** It
cleared `__dict__` and *then* called `__init__`, so a malformed payload raised correctly
and left an existing, previously valid instance **empty or length-inconsistent**.
`__setstate__` is reachable on a live object, so a failed restore that destroys its
target is worse than the incompatibility the hook exists to fix. Pinned by a
parametrised test over three malformed payloads, each asserting the instance is intact
afterwards.

**Losses that remain, each measured and pinned by a test:**

| loss | detail |
| --- | --- |
| `asdict` / `astuple` | raise on a non-empty series — they walk fields directly and never reach `__getstate__`. ⚠ They **succeed** on `BarSeries((), ())`: no proxy to choke on. Revision 2 claimed an unconditional raise; both halves are now pinned, as `hash` already was. |
| pickling `series.rows` or a row **directly** | still fails — only the series-owned path is restored. |
| `(series, series.array_closes)` as one graph | round-trips, but the cache **alias** is lost: the restored series recomputes. Values identical; identity not. |
| an old **warm** pickle | fails with `unexpected keyword argument 'float_closes'`. An old **cold** pickle loads fine. |

⚠ Revision 2 called old payloads an **EMPTY DOMAIN** on the strength of a grep. The grep
shows no in-repo writer; it cannot show that no payload exists anywhere. The behaviour
above is measured and accepted rather than assumed away.

### 4.6 Typing — a read-only TypedDict, not a cast (ckpt-1 finding 16)

Revision 1 would have left `rows: tuple[OHLCVRow, ...]` advertising mutable rows and
hidden the mismatch behind a `cast`. Instead the element type is a PEP 705 read-only
TypedDict, `ReadOnlyOHLCVRow`, declared in `technical_analysis.py` beside `OHLCVRow`
(that module cannot import `indicator_series` — it would be a cycle).

This moves the accident from a runtime `TypeError` to an error in the **pre-push gate**:

```
error: Could not assign item in TypedDict
    "close" is a read-only key in "ReadOnlyOHLCVRow"
```

⚠⚠ **Assignability runs ONE WAY, and revision 2 checked only the easy direction.**
`OHLCVRow` → `ReadOnlyOHLCVRow` is fine, so **no construction site changes**. The
reverse is not, so every consumer that takes `series.rows` and passes it to a
mutable-typed parameter breaks. Revision 2 asserted this was `FIXED`; running pyright
produced **8 errors**. They are all real and all one shape — readers demanding a write
capability they never use — and the fix is to widen the reader, which accepts both
types:

| site | change |
| --- | --- |
| `technical_analysis.atr` / `stochastic` / `compute_indicators` | `Sequence[OHLCVRow]` → `Sequence[ReadOnlyOHLCVRow]` (all three only read) |
| `strategy_price_basis.bind_bar` | `row: OHLCVRow` → `ReadOnlyOHLCVRow` |
| `verify_2240_s2_cross_sectional._window_usable` | `list[float \| None]` → `Sequence[float \| None]` (the float caches are tuples now) |

⚠ Widening `bind_bar` is what adds **`PRICE_BASIS_RULE_VERSION` to the permitted-change
set** (§5): that module's bytes are inside it. Repo pyright is at **0 errors** with
these applied.

⚠⚠ **The row schema keeps a contract that is KNOWN TO BE FALSE, deliberately.**
`ReadOnlyOHLCVRow` mirrors `OHLCVRow` exactly, including `open: Decimal` rather than
`Decimal | None`. Production does not honour that — `price_masked_bars` masks a
quarantined field to `None`, which is why `_floats` and `closes` have always had `None`
branches. Measured over 60 instruments / 60,626 bars from `load_masked_bars`:

```
volume None 16,661 · high 12 · low 12 · close 11 · open 2      (all five keys always present)
```

Declaring `Decimal | None` — **tried, not reasoned about** — produces **35 pyright
errors** at sites that assume non-null. Every one is pre-existing; `OHLCVRow` has always
made the same false promise. Correcting it is a real and separate round, and smuggling
it in here would mix two changes and leave the A/B unable to attribute a difference to
either. ⚠ ckpt-1 was right that copying a false contract into a new public type is a
defect; the disposition is to **name it in the type's own docstring with the
measurement**, not to pretend it is accurate.

## 5. Identity rotation and evidence disposition

`RULE_SET_VERSION` hashes this module's own bytes, and
`strategy_registry.INPUT_RULE_SETS["indicator_series"]` carries it, so **any** edit here
rotates `strategy_version` for all 12 strategies — 12x the blast radius of the
carrier-module rotation in `b9b2a1e5`. That over-invalidation is the deliberate,
inherited trade recorded at the module's lines 61-63 and required by prevention-log
entry #3017 (an indicator definition *is* the strategy's filter logic).

**The permitted-change set has three members**, each asserted by the A/B to have
*actually* rotated — a rotation that did **not** happen would mean the edit never
reached the identity, which is its own defect:

1. `strategy_version`, all 12 strategies x both universes;
2. `INPUT_RULE_SETS["indicator_series"]`;
3. `PRICE_BASIS_RULE_VERSION` — because §4.6 widens `bind_bar`'s annotation and
   `strategy_price_basis.py`'s bytes are inside it, which is inside S-12's params.
   ⚠ Revision 2 named only the first.

### ⚠⚠ The census must be taken from the BASELINE commit, and revision 2's was circular

`indicator_series` hashes its own source. So a census run from the **candidate** checkout
reports zero attachment **by construction** — the edit has already detached everything —
and cannot distinguish that from "nothing was ever attached". The question only the
baseline arm can answer is *did this rotation detach evidence that was still live*.

The census therefore travels **inside the A/B measurement**, computed per arm against
that arm's own identities and written into its JSON with its commit. It is not a loose
figure quoted in prose.

⚠ Revision 2's §5 also published `entry.identity(universe=u)` as the procedure. That is
not executable: it raises `TypeError`, because `cost_model_id` is required. The script
uses `entry.identity(universe=u, cost_model_id=COST_MODEL_ID)`, and the published
procedure is now the script plus its commit rather than a prose snippet.

Tables censused: `strategy_signals`, `strategy_signal_observations`,
`strategy_scan_watermark`. The last is there because revision 2 **inferred** watermark
state from the ledger count and ckpt-1 was right that it does not follow — it is an
independent table.

### Disposition if the baseline census is non-zero

`--compare` does **not** fail on non-zero attachment, because a rotation that detaches
live evidence is a decision, not an error. It logs a warning naming the tables and
counts, and the disposition is recorded on the PR before merge. Silence in that case
would be the defect. ⚠ Revision 2 said only "re-run at merge" and never said what a
non-zero answer would mean.

## 6. Cost — measured end to end, with a decision rule fixed in advance

ckpt-1 finding 28 is that a per-bar microbenchmark is not an end-to-end bound, because
every sub-series construction re-copies. **It was right, and by a large factor.** The
A/B counts `BarSeries.__init__` invocations; on the 3-instrument smoke:

```
3 instruments · 3,217 corpus bars  ->  147 constructions · 157,633 bars copied
```

That is ~49x the corpus size. ⚠ Most of it is the **harness**, which deliberately
evaluates 96 cells per instrument (12 strategies x 2 universes x 2 routes x 2 shapes)
where production evaluates one; the amplification is reported as the harness's, not as
production's, and the two are not conflated.

What the PR records, from the full-population run on both arms:

- constructions and bars copied, per arm;
- wall-clock per arm, same machine, back to back;
- peak RSS per arm, measured per process (not an in-process lifetime peak, which cannot
  isolate the second arm).

**The decision rule, fixed before the numbers are read** — revision 2 left "material"
undefined, so any regression would have satisfied it:

| outcome | disposition |
| --- | --- |
| candidate wall-clock ≤ 1.25x baseline | ship as is |
| 1.25x – 2x | ship, and record the figure in the module comment beside the per-bar cost |
| > 2x | do **not** ship this construction; add a sanctioned sub-series constructor that skips the re-freeze because it knows the rows are already ours (a provenance `__post_init__` cannot establish but a dedicated method can), and re-measure |

⚠ The per-bar prices remain: **0.060 µs from a dict, 0.275 µs from an already-frozen
proxy**, against a current constructor cost of 0.034 µs/bar.

⚠ Not covered, and named rather than implied: `price_segments.segment_for_index` can
copy a whole segment per lookup, and the backtest and outcome paths construct
sub-series at rates this scan-shaped sweep does not exercise. The cost conclusion is
scoped to what was measured.

## 7. Acceptance

Revision 1's acceptance was partly vacuous; revision 2's was still weaker than it read.
Corrected, with what each item is *guarding against* stated so it cannot be satisfied
trivially:

1. **Behaviour is unchanged.** Full-population A/B, `scripts/ab_2840_barseries_row_immutability.py`.
   The arm is the **commit** — baseline run from a worktree at `origin/main`, candidate
   from the branch, nothing simulated. Covers 12 strategies x both universes x
   {undeclared, certified} carrier x {flat, segmented} = **96 cells**, compared as
   **ordered digests with multiplicity** (a set would hide a duplicate-output
   regression). Cross-sectional members are encoded **structurally** over
   `dataclasses.fields`, so `scores`, `None` verdict slots, `admissible_dates` and
   `mandatory_dates` are all included — a hand-written field list would have gone
   silently incomplete.
   - ⚠ The two arms cannot share a DB snapshot. Rather than claim one, each arm digests
     **the input corpus it actually read**, and `--compare` **refuses outright** if they
     differ. A concurrent write then surfaces as a refusal rather than as a finding
     about this diff.
   - ⚠ Permitted-change set: the three of §5, each asserted to have *actually* rotated.
   - ⚠ Coverage asserted per cell, non-zero, because S-12's live route short-circuits
     before arithmetic.
   - ⚠ **Scope limit, named:** this is scan-shaped. It does not cover
     `position_builder`, `outcome_resolver`, `residual_confluence_evaluation`, the
     ledgers, or the backtest paths. Their protection is the type change (§4.6) plus
     the fast tier, not this sweep.
2. **The exploit of §2 is unreachable** — the four-step sequence on a real fixture, with
   the baseline binding acceptance pinned first so a generic "it raised" cannot pass.
3. **Consistency, not refusal.** For each surface (`rows[i][key]`, three `float_*`,
   three `array_*`): attempt the write, then assert *both* that it raised and that rows
   and every cache still agree — **cold and warm**. A refusal-only test passes under
   every surviving bypass in §3, which is what made revision 1's version vacuous.
4. **The caller's retained *row dicts* cannot reach the series** — the dicts, not the
   list. ⚠ Mutating the retained *list* could never reach the series even before this
   diff, so asserting on it would be vacuous.
5. **Reconstruction matrix**: `dataclasses.replace`, `copy.copy`, `copy.deepcopy`,
   `pickle`, `__setstate__` onto an already-warm instance, and **three malformed
   payloads** (length mismatch, non-ascending, duplicate dates). Each malformed case
   asserts the raise **and that the target instance is intact** — the defect revision 2
   introduced. Warm restoration uses *different* replacement data so a stale cache
   cannot pass unnoticed.
6. **The stated losses are pinned, with their conditions**: ordinary subclassing raises;
   the mixin-MRO bypass is pinned as a *reproducing* test so the refusal is never read as
   absolute; `asdict`/`astuple` raise on a non-empty series **and succeed on the empty
   one**.
7. **The sub-series idiom still produces a correct, INDEPENDENT sub-series.**
   "Independent" is operational: the child's row objects are `is not` the parent's, and
   a refused write on one leaves the other consistent. ⚠ Equality alone would pass for
   two proxies over the same backing dict.
8. **`hash()` asserted in both cases** — `hash(BarSeries((), ()))` succeeds, a non-empty
   one raises. Pins the comment correction below.
9. **The shapes production actually supplies**, not just the happy fixture: a masked bar
   (`None` in an OHLC field) and a row **missing keys entirely**. Measured frequencies
   in §4.6. Without these the `None` branch in `_floats` — the branch every masked bar
   takes — is never exercised by the freeze tests.

⚠ **An existing test changes meaning and is revised deliberately, not repaired.**
`tests/test_2840_s12_price_basis_gate.py::test_mutating_a_row_after_construction_is_caught`
mutated a retained row dict and asserted the carrier *rejects* the series. The mutation
can no longer reach the series, so there is nothing left to reject.

⚠⚠ The obvious replacement — "build a changed series separately, assert the carrier
rejects it" — **does not work**, and ckpt-1 refused it by construction: a carrier holding
a live **reference** to the original series rejects that second series too, so the
assertion cannot tell snapshot from reference. Revision 2 proposed exactly that. The test
now pins `carrier.bar_bindings == bindings_for(series)` — the independently recomputed
encoding — which a reference-backed carrier has nothing to satisfy.

### The comment at lines 143-144 is false and is corrected in the same diff

It reads *"the cached values live outside the declared fields, so `__eq__` and `__hash__`
still compare `(dates, rows)` alone."* `__eq__` is correct. `__hash__` is generated over
`(dates, rows)` as described but **raises** for any non-empty series —
`TypeError: unhashable type: 'dict'` — and succeeds only for the empty one. Freezing does
not change this; `MappingProxyType` is unhashable too. Nothing in the repo hashes a
`BarSeries` or uses one as a key or set member (grepped), so it is a comment defect and
not a bug — corrected because it is a false claim in the file the diff edits, in the
place a reader checking whether caching is safe would look.

## 8. Out of scope, named

- Replacing `OHLCVRow` with a frozen row type, or narrowing `OHLCVRow` itself to
  `ReadOnly`. Reaches 115 construction sites and every writer; a separate round.
- Making `BarSeries` hashable. Needs a hashable row type, i.e. the item above. Nothing
  hashes one.
- Closing §3's bypasses 1-4. Each needs a mechanism disproportionate to an accident
  control (a wrapper type with its own `__eq__`, a copy-on-read array, `__slots__`
  without the cache). Named so the next reader does not mistake silence for coverage.
- Non-guarantee 3 (equal payload transfers the claim) and 4 (instrument id is not bound)
  from the predecessor are untouched and remain open on #2840.

## 9. Checkpoint-1 disposition (revision 1 → revision 2)

28 findings, all reproduced or checked against the repo before disposition.

| # | finding | disposition |
| --- | --- | --- |
| 1 | reflected `__eq__` leaks the backing dict | **CONFIRMED** (`1.5 -> 99.9`). Frame corrected; §3 bypass 1, named not closed |
| 2 | `setflags(write=True)` reversible | **CONFIRMED**. §3 bypass 2, named |
| 3 | `.shape`/`.dtype`/`.strides` mutable on a RO array | **CONFIRMED**. §3 bypass 3, named |
| 4 | views reach the cached array's base | ACCEPTED as a corollary of 2; §3 |
| 5 | `.copy()` delegates to a subclass override | **CONFIRMED**. Design changed to `dict(r)` (§4.1) |
| 6 | shallow copy shares mutable leaves | ACCEPTED; leaves are `Decimal`/`int`/`None`, subclass leaves named in §3 |
| 7 | `dates` mutable through a retained list | **FIXED** — coerced (§4.3) |
| 8 | `vars(series)[...]` writes the cache slot | **CONFIRMED**. §3 bypass 4, named |
| 9 | subclassing bypasses both protections | **FIXED** — `__init_subclass__` raises (§4.4) |
| 10 | `__setstate__` must re-validate | **FIXED** — routes through `__init__` (§4.5) |
| 11 | `__setstate__` on a warm instance leaves caches | **FIXED** — `__dict__.clear()` first (§4.5) |
| 12 | old pickle payloads underspecified | **EMPTY DOMAIN** — nothing persists a pickle (§4.5) |
| 13 | dropping caches changes copy semantics | ACCEPTED; recomputation is identical for these leaves, stated |
| 14 | two-field state drops subclass fields | **MOOT** — subclassing refused (§4.4) |
| 15 | `asdict`/`astuple` break | **CONFIRMED**. Stated loss + pinned by test (§4.5, §7.6) |
| 16 | typing migration missing | **FIXED** — `ReadOnlyOHLCVRow`, pyright-verified (§4.6) |
| 17 | "30 call sites, all reads" is weak evidence | ACCEPTED — the A/B replaces the grep as the evidence (§7.1) |
| 18 | consumers misidentified | **CONFIRMED, both halves.** §2 corrected |
| 19 | watermark inference unqueried | **CONFIRMED as a gap.** Now the query; answer unchanged (§5) |
| 20 | disposition lacks reproducible SQL | **FIXED** — SQL inline, re-run at merge (§5) |
| 21 | "only `strategy_version` moves" is wrong | **FIXED** — permitted-change set widened (§7.1) |
| 22 | A/B can miss the relevant path | **FIXED** — per-route non-zero coverage asserted (§7.1) |
| 23 | refusal tests do not establish immutability | **FIXED** — consistency assertions (§7.3) |
| 24 | retained-list test is vacuous | **FIXED** — retained row *dicts* (§7.4) |
| 25 | reconstruction matrix missing | **FIXED** (§7.5); all five slice sites named (§7.7) |
| 26 | an existing test needs revision | **ACCEPTED** — split rather than rewritten (§7) |
| 27 | hash needs the empty case | **CONFIRMED** — `hash(BarSeries((),()))` succeeds (§7.8) |
| 28 | cost omits repeated copies | **FIXED** — no claim ahead of the measurement (§6) |

## 10. Checkpoint-1 disposition (revision 2 → revision 3)

29 findings, the eighteenth refusal on this ticket. Every one checked against the
interpreter or the repo before disposition — including the three of revision 2's own
dispositions that did not survive.

| # | finding | disposition |
| --- | --- | --- |
| 1 | `__init_subclass__` suppressible by a non-cooperative mixin — not `FIXED` | **CONFIRMED.** Guard kept as an accident control; nothing depends on it any more (§4.4) |
| 2 | two-field state drops subclass fields — not `MOOT` | **CONFIRMED.** `__getstate__` enumerates `dataclasses.fields` (§4.5) |
| 3 | typing migration not `FIXED` — 8 pyright errors | **CONFIRMED and FIXED in code.** Three readers widened; repo pyright at 0 (§4.6) |
| 4 | the new row schema copies a false non-null contract | **CONFIRMED.** Measured (`None`: volume 16,661 / high 12 / low 12 / close 11 / open 2). Accurate typing costs 35 errors ⇒ named in the type's docstring, corrected in its own round (§4.6) |
| 5 | "two gates" overclaimed — pyright allows `array_closes[0] = 999` | **CONFIRMED.** Stated per surface: rows and float tuples get two, arrays get one (§3) |
| 6 | surviving bypasses do not all need bypass-only code | **CONFIRMED.** `arr.shape` is ordinary numpy; the intent claim is withdrawn, not repaired (§3) |
| 7 | disposition 6 misstated where subclass leaves were named | **CONFIRMED.** Shallow-copy residue now listed explicitly as §3 bypass 7 |
| 8 | `__setstate__` destructive on failure — NEW in revision 2 | **CONFIRMED.** Validate-then-commit; three malformed payloads pinned (§4.5, §7.5) |
| 9 | clearing caches does not make *concurrent* restoration safe | ACCEPTED as a named limit. No concurrent caller exists; `__setstate__` is not synchronised and does not claim to be |
| 10 | `EMPTY DOMAIN` stronger than the grep supports | **CONFIRMED.** Measured instead: old cold pickles load, old warm pickles fail (§4.5) |
| 11 | pickle/deepcopy restoration limited to the series-owned path | **CONFIRMED.** Both residual losses tabulated (§4.5) |
| 12 | `asdict`/`astuple` succeed on an empty series | **CONFIRMED.** Both halves pinned, as `hash` already was (§4.5, §7.6) |
| 13 | the proposed test split cannot prove snapshot-vs-reference | **CONFIRMED by construction.** Test now pins the recomputed encoding (§7) |
| 14 | round-trip + frozen output does not prove revalidation | **FIXED** — malformed payloads, warm replacement with *different* data (§7.5) |
| 15 | the consistency oracle needs the real data shapes | **FIXED** — masked and missing-key fixtures, measured frequencies (§7.9) |
| 16 | "raises at step 3" alone under-specifies the exploit | **FIXED** — baseline binding acceptance pinned first (§7.2) |
| 17 | named fixes lack explicit regressions | **FIXED** — retained date list, `copy()->self`, mixin bypass each have one |
| 18 | "independence" undefined for sub-series | **FIXED** — operational: `is not` on row objects plus cross-consistency (§7.7) |
| 19 | non-zero route counts ≠ relevant execution | PARTIAL — per-cell coverage is asserted; post-warm-up body execution is not separately instrumented. Named, not claimed |
| 20 | a scan A/B misses the non-strategy consumers | **CONFIRMED.** Scope limit named in §7.1 rather than implied away |
| 21 | verdict equality omits `segmented_member`'s payload | **FIXED** — structural encoding over `dataclasses.fields` covers all of it (§7.1) |
| 22 | the A/B protocol was unspecified | **FIXED** — baseline SHA, corpus digest with refusal-to-compare, per-process RSS (§7.1, §6) |
| 23 | "scan, both universes" needs a concrete harness | **FIXED** — drives `STRATEGY_MANIFEST` directly; pure, writes nothing, not the live scan |
| 24 | "material" regression had no threshold | **FIXED** — three-band decision rule fixed *before* reading the numbers (§6) |
| 25 | a scan sweep does not bound backtest/outcome copy cost | **CONFIRMED.** Named as out of scope for the cost conclusion (§6) |
| 26 | `identity(universe=u)` is not executable | **CONFIRMED** — hit this exact `TypeError`. `cost_model_id=COST_MODEL_ID` required; procedure is now the script (§5) |
| 27 | a post-edit census is circular | **CONFIRMED, and the sharpest finding.** Census moved inside the A/B, per arm, per commit (§5) |
| 28 | no disposition stated for non-zero attachment | **FIXED** — warns and requires a recorded disposition; does not fail (§5) |
| 29 | permitted-change set incomplete once typing is fixed | **CONFIRMED.** `PRICE_BASIS_RULE_VERSION` added as the third member (§5) |

### The one lesson worth the queue's attention

Revision 1 was refused for claiming impossibility. Revision 2 corrected that and was
refused for a **new** defect of the same family: `__setstate__` cleared before it
validated, so the hook added to prevent a silent capability loss could destroy a live
object instead. The pattern is not "the claims were too strong" — it is that **a fix
written to close one failure mode is not itself examined for the failure modes it
introduces**, and a destructive-on-error path reads as safe because the error is
correctly raised. Three of revision 2's dispositions (`FIXED`, `MOOT`, `EMPTY DOMAIN`)
also failed for one shared reason: each asserted a negative from an inventory or a grep
rather than from an attempt. That is the rule this ticket has now re-learned at every
round — **to write "X cannot happen", try to make it happen and report the output.**
