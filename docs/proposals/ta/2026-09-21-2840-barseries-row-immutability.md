# `BarSeries` row immutability — closing `binding_mismatch`'s non-guarantee 1

Refs #2840. Refs #2437. Head named by `b9b2a1e5`'s close-out.

Predecessor: `docs/proposals/ta/2026-09-21-2840-carrier-series-binding.md` (§ "Stated
non-guarantees", item 1).

**Revision 2.** Revision 1 was refused whole at Codex checkpoint 1 (28 findings — the
seventeenth refusal on this ticket). It was refused on its **frame**, not its details:
it claimed to make cache desynchronisation *impossible*, and six independent bypasses
survive the design. Every one was reproduced in this interpreter before this revision
was written; they are in §3, with their output. The disposition table is §9.

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

Bypasses 1-4 survive this design and are **named, not closed**. Each requires writing
code whose only purpose is the bypass, which is exactly the line between an accident and
a determined act. Bypass 5 **is** closed (§4). Bypass 6 **is** closed (§4).

⚠ Consequently no test in §7 asserts "this is impossible", and no docstring will say
so. The claim is "the ordinary alias-write is refused by two independent gates".

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

### 4.4 Subclassing is refused (ckpt-1 findings 6, 9, 14)

`__init_subclass__` raises. Nothing subclasses `BarSeries` in this repo. Refusing it
closes bypass 6, closes the "subclass may set a non-field attribute" variant, and makes
§4.5's two-field state provably complete rather than a hazard.

### 4.5 Serialization — preserved where it can be, and the loss named

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

- `__getstate__` returns `dates` plus `tuple(dict(row) for row in rows)`, and
  deliberately drops the warm caches. Today's default ships `self.__dict__`, which
  includes whatever `cached_property` values were warm — derived data on the wire that
  the receiver can recompute. Two fields are exhaustive because §4.4 forbids subclasses.
- `__setstate__` does `self.__dict__.clear()` then `self.__init__(**state)`. Verified
  this works on a frozen dataclass and clears the caches
  (`__dict__.clear()+__init__ on frozen: OK, caches cleared=True`). Routing through
  `__init__` rather than hand-installing fields is what makes ckpt-1 findings 10 and 11
  answered rather than argued: the restored instance re-runs **the full length and
  ordering validation and the freeze**, and a `__setstate__` called on an already-warm
  instance cannot leave a stale cache behind.

`asdict` / `astuple` are **not** restored. They walk fields directly and would need a
`__deepcopy__`-shaped hack to fix. Named as a stated loss with the measurement that
nothing uses them.

⚠ There are no persisted pickles to be compatible with: nothing in the repo writes one
(grepped — the only `pickle` references are `multiprocessing`'s own, inside
`synthetic_control_run`'s comments). So the old-payload compatibility question ckpt-1
raised has an empty domain, stated as the grep rather than assumed.

### 4.6 Typing — a read-only TypedDict, not a cast (ckpt-1 finding 16)

Revision 1 would have left `rows: tuple[OHLCVRow, ...]` advertising mutable rows and
hidden the mismatch behind a `cast`. Instead the element type becomes a PEP 705
read-only TypedDict declared in this module:

```python
class ReadOnlyOHLCVRow(TypedDict):
    open: ReadOnly[Decimal]
    ...
```

This is strictly better than a runtime-only guard, because it moves the accident from a
`TypeError` at run time to an **error in the pre-push gate**. Verified with the repo's
own pyright:

```
error: Could not assign item in TypedDict
    "close" is a read-only key in "ReadOnlyOHLCVRow"
```

and a `tuple[OHLCVRow, ...]` argument still assigns to a `tuple[ReadOnlyOHLCVRow, ...]`
parameter with no error, so no construction site changes. `OHLCVRow` itself is **not**
touched — it is used by writers all over the repo and narrowing it is a different round.

So the accident is refused twice, by two independent mechanisms: pyright at push time,
`TypeError` at run time.

## 5. Identity rotation and evidence disposition

`RULE_SET_VERSION` hashes this module's own bytes, and
`strategy_registry.INPUT_RULE_SETS["indicator_series"]` carries it, so **any** edit here
rotates `strategy_version` for all 12 strategies — 12× the blast radius of the
carrier-module rotation in `b9b2a1e5`. That over-invalidation is the deliberate,
inherited trade recorded at lines 61-63 and required by prevention-log entry #3017 (an
indicator definition *is* the strategy's filter logic).

Measured read-only this session, with the queries:

```sql
-- ledger
select strategy_id, strategy_version, count(*) from strategy_signals group by 1,2
union all
select strategy_id, strategy_version, count(*) from strategy_signal_observations group by 1,2;
-- watermarks
select strategy_id, strategy_version, count(*) from strategy_scan_watermark group by 1,2;
```

intersected against `STRATEGY_MANIFEST[sid].identity(universe=u).version` for all 12
strategies × both universes:

```
ledger      : 982,051 rows / 39 (strategy_id, version) groups   ATTACHED to a current identity = 0
watermarks  :              39 (strategy_id, version) groups     ATTACHED to a current identity = 0
```

⚠ The watermark line is there because revision 1 **inferred** it from the ledger count
and ckpt-1 finding 19 was right that this does not follow — `strategy_scan_watermark` is
an independent table. It is now the query.

⇒ the rotation detaches nothing that was still attached, and the cold-start branch of
`write_window_indices` (at most one eligible bar per instrument, no backfill) is
*already* what the next scan of every strategy takes. This diff does not create that.

⚠ Both counts are re-run at merge time and recorded in the PR with the merge SHA — a
scan between now and then can attach evidence.

## 6. Cost — measured end to end, not extrapolated

ckpt-1 finding 28 is that a per-bar microbenchmark is not an end-to-end bound, because
every sub-series construction re-copies. The full-population A/B in §7 therefore counts
`BarSeries.__init__` invocations and bars copied, and the PR records:

- constructions and bars-copied per instrument, over all 5,797 eligible instruments;
- wall-clock for the sweep with and without the freeze;
- peak RSS for both arms.

No cost claim is made in this document ahead of that measurement. What is fixed is the
per-bar price: **0.060 µs from a dict, 0.275 µs from an already-frozen proxy**, against
a current constructor cost of 0.034 µs/bar.

If the measured end-to-end regression is material, the fallback is a sanctioned
sub-series constructor that skips the re-freeze because it knows the rows are already
ours — which is a provenance the `__post_init__` type check cannot establish, but a
dedicated method can. Not built speculatively.

## 7. Acceptance

Revision 1's acceptance was refused as partly vacuous (ckpt-1 findings 23, 24, 27): an
assignment that raises does not establish that rows and caches agree, and a test that
mutates a retained **list** proves nothing because that could never reach the series.
Corrected:

1. **Behaviour is unchanged.** Full-population A/B over the scan, both universes, the
   certified and undeclared carrier routes, and the segmented dispatchers. Every
   verdict, reason code and signal identical before and after. ⚠ The permitted-change
   set is `strategy_version` **and** `input_rule_set_versions["indicator_series"]` —
   revision 1 named only the first, which ckpt-1 finding 21 showed is literally wrong.
   Coverage must be non-zero on each route, asserted, not assumed: S-12's live path uses
   an undeclared carrier and short-circuits before arithmetic, so a sweep that only
   exercises it proves nothing (finding 22).
2. **The exploit of §2 is unreachable.** Run the four-step sequence; assert it raises at
   step 3.
3. **Consistency, not refusal.** For each of the seven surfaces (`rows[i][f]`, three
   `float_*`, three `array_*`): attempt the write, then assert *both* that it raised and
   that `rows` and every cache still agree — cold and warm. A refusal test alone passes
   even under every bypass in §3.
4. **The caller's retained *row dicts* cannot reach the series** — mutate the dicts (not
   the list), cold and warm, and assert the series is unmoved.
5. **Reconstruction matrix**: `dataclasses.replace`, `copy.copy`, `copy.deepcopy`,
   `pickle` round-trip, and `__setstate__` onto an already-warm instance. Each asserts
   the result is itself frozen and its caches agree with its rows — proving
   `__setstate__` re-validates rather than trusting the payload.
6. **Subclassing raises**, and **`asdict`/`astuple` raise** — the stated losses are
   pinned so they cannot silently return later.
7. **The slice idiom still produces a correct, independent sub-series** at all five
   production sites: `price_segments.py:70`, `strategy_signal_scan.py:1035`,
   `backtest_run.py:1044`, and both dispatchers in
   `strategy_segmented_evaluation.py`. Assert independence, not just equal values.
8. **`hash()` is asserted in both cases** — `hash(BarSeries((), ()))` succeeds (measured;
   revision 1 missed this), a non-empty one raises `TypeError`. This pins the comment
   correction below so the claim cannot rot again.

⚠ **An existing test changes meaning and is revised deliberately, not repaired.**
`tests/test_2840_s12_price_basis_gate.py:449::test_mutating_a_row_after_construction_is_caught`
mutates a retained row dict and asserts the carrier *rejects* the series. After the copy,
the mutation cannot reach the series at all, so the correct outcome becomes "series
unmoved, binding passes" — it was ckpt-1's only selected-test failure against the
prototype. Rewriting it to assert the new outcome would **defang** it: it is currently
the only proof that the carrier's binding is a snapshot and not a reference. It is
therefore split — the unreachability becomes acceptance item 4, and the carrier's
snapshot property is re-proved against a separately built series carrying one changed
value, which does not depend on aliasing.

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
