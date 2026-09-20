# #2840 — de-overload `AS_TRADED_UNIVERSES` / `SCAN_UNIVERSE`: the fact replaces the token

**Status:** spec, revised after Codex checkpoint 1 (21 findings; §10 records what each one
moved). Implements §6 item 3 of `2026-09-20-2840-per-series-price-basis-carrier.md`, whose
items 1 and 2 landed as `627a0736` (the certifier) and `07f62f2f` (the carrier).

## 0. The inherited handoff — one claim held, one is superseded, and the proof I gave was wrong

`07f62f2f`'s close-out named this item and attached two warnings.

| inherited claim | verdict |
| --- | --- |
| *"it is when `SCAN_ARCHIVE_ADJUSTMENT_BASIS = None` stops being inert"* | **HOLDS** — §3 is the part of this change that answers it |
| *"the sixth `INPUT_RULE_SETS` entry must be installed, rotating all 11 identities"* | **SUPERSEDED, on dependency analysis** — not falsified by a test |

### 0.1 The commitment, and why it is superseded rather than mistaken

`2026-09-20-2840-the-price-basis-carrier.md:311-313` does not merely hint at this; it commits:

> The exception holds only while there is one consumer, so **the sixth entry is installed as
> part of §6 item 3** — the change that removes `AS_TRADED_UNIVERSES` and makes the carrier
> reachable from the scan — and a test asserts S-12 is the sole importer of this module until
> then.

⚠ **My first draft cited the tripwire test as proof the commitment was wrong. That is not a
proof and the correction is Codex's (finding 6).** The test
(`tests/test_2840_price_basis_carrier.py:246-254`) greps `app/services/strategies/s*.py` for
the string `strategy_price_basis`, and an importer grep cannot establish a consumer set — an
adapter, the scan engine, or `segmented_*` can read the carrier without any strategy module
importing it (finding 5). The test is a **tripwire on the cheapest case**, not a dependency
analysis.

The dependency analysis, done and stated as the actual grounds:

- `INPUT_RULE_SETS` exists so that a rule read by **more than one strategy** is versioned once
  rather than per author (`strategy_registry.py:88`, and the drift argument in the prior spec).
- Every `PerSeriesSignals` / `MemberStager` adapter other than S-12's takes `price_basis` and
  **discards it** — `strategy_manifest.py` carries `# noqa: ARG001 - uniform call; see
  PerSeriesSignals` on 12 adapters (`:482`, `:493`, `:504`, `:516`, `:533`, `:552`, `:662`,
  `:785`, `:821`, `:878`, `:921`, `:962`). S-12's adapter at `:718-736` is the only one that
  forwards it.
- `segmented_signals` and `segmented_member` **slice** the carrier and pass it on
  (`strategy_segmented_evaluation.py:71`, and the `member` path) — they read no basis value and
  take no verdict from it.
- This change adds no forwarding adapter and no reader. **So the consumer set is still {S-12},
  and the condition the prior spec attached the installation to has not arrived.**

⚠ The commitment was written pointing at *this* item because it was the next one due, not
because a dependency here triggers it. The tripwire's failure message and the `S12_PARAMS`
comment both repeat "§6 item 3" and both are corrected by this change to say **on a second
consumer**.

⚠ **What is NOT claimed:** that `INPUT_RULE_SETS` will never be owed. Finding 8 stands as a
recorded obligation — the carrier's *source selection* is outside every identity hash, so
rewiring which constructor the scan calls changes verdicts without moving S-12's version or
`strategy_price_basis.py`'s bytes. That is a real gap, it is **pre-existing** (`07f62f2f`
shipped it), and closing it is not this change's scope. §7 test 4 is a tripwire over it, and
§8 records it as open.

So: S-12's identity rotates (its module bytes change), the other 10 do not.

## 1. What is being removed, and every caller whose verdict moves

```python
if universe not in AS_TRADED_UNIVERSES:
    return [StrategySignal(verdict="not_evaluable", ...) for index in range(len(series))]
```
`s12_cheapest_band_price_gated_breakout.py:344-348`.

`Universe = Literal["survivor_only", "survivorship_free"]` (`indicator_series.py:77`), so the
gate's whole behavioural content is **refuse `survivor_only`**.

⚠⚠ **My first draft concluded from that alone that removing the gate "loses nothing", and
that was wrong (finding 1).** The gate's replacement is the carrier, and whether a caller's
verdict moves depends on **what carrier that caller builds**, not on which universe it names.
Codex reproduced the error directly: a `survivor_only` series with a hand-built certified
carrier flipped all 175 fixture verdicts, including a fire. So the analysis has to be **per
caller**. Enumerated by grepping every `price_basis=` argument in `app/` and `scripts/`:

| caller | universe | carrier it builds | S-12 today | after |
| --- | --- | --- | --- | --- |
| `strategy_signal_scan.py:1001`, `:1041` (production scan) | `survivor_only` | `from_archive_basis(SCAN_ARCHIVE_ADJUSTMENT_BASIS=None)` → all-uncertified | refuse (token) | refuse (carrier) — §2 |
| `backtest_run.py:3237`, `:3406` (production backtest) | `survivorship_free` | `from_archive_basis(corpus.liquidity_policy.adjustment_basis)` | token passes; carrier decides | unchanged |
| `scripts/verify_2394_signal_scan_cost.py:428`, `:466`, `:551`, `:567` | `survivor_only` (`:103`) | literal `"unadjusted"` | refuse (token) | ⚠ **WOULD EVALUATE** |
| `scripts/verify_2394_strategy_manifest.py:200`, `:215`, `:232` | `survivor_only` (`:71`) | literal `"unadjusted"` | refuse (token) | ⚠ **WOULD EVALUATE** |
| `scripts/verify_2394_backtest_run.py:543` | `survivor_only` (via `verify_2240_position_builder:96`) | literal `"unadjusted"` | refuse (token) | ⚠ **WOULD EVALUATE** |
| `scripts/verify_2437_missing_market_context.py:91` | `SCAN_UNIVERSE` | literal `"unadjusted"` | refuse (token) | ⚠ **WOULD EVALUATE** |
| `scripts/verify_2437_s10_census.py:102` | `SCAN_UNIVERSE` | `from_archive_basis(SCAN_ARCHIVE_ADJUSTMENT_BASIS)` | refuse (token) | refuse (carrier) |
| `scripts/census_2840_s12_signal_supply.py:627` | research corpus, `cost_price_basis == "as_traded"` asserted at `:573` | literal `"unadjusted"` | evaluate | unchanged |
| `scripts/verify_2414_revision_invariance.py:171`, `scripts/ab_2797_s2_weekday_rebalance.py:82` | `SCAN_UNIVERSE` | n/a — S-4 / momentum, no S-12 | n/a | n/a |

**Four script callers pass a literal `"unadjusted"` on `survivor_only`, and the token is
currently the only thing stopping S-12 from judging a nominal `>= $100` gate on those
prices** — findings 2 and 3, both verified in source. `verify_2394_signal_scan_cost.py` is
the worst of them: its own module docstring (`:12`) says **"THIS READS THE **LIVE** CORPUS
(`price_daily`)"**, whose history the provider back-adjusts at fetch time and whose #2066
split-cliff guard *heals a mixed series onto the back-adjusted basis* (`market_data.py:751`).

⚠ **That is not a reason to keep the token. It is a reason those four callers are already
lying, and the token is hiding it.** Each names a path with **no pinned archive**, so a
literal archive basis is not a fact it holds. §3's constructor is exactly what they should
pass, and passing it **preserves their verdicts** — S-12 refuses there after the change as it
does today, by the honest route. Fixing them is part of this change (§6), not a follow-up.

What the removal GAINS: a certified series is evaluated on whichever universe delivered it.
Today no production path produces one on `survivor_only`, so this is a **precondition**
change — which is why it lands before the forward per-bar source rather than with it.

## 2. The one production verdict that changes, measured directly

`s12_signals` is called **per price-scale segment** (`strategy_segmented_evaluation.py:53-74`),
not once per series. The two gates disagree on a **segment-final bar**:

| path | segment-final bar |
| --- | --- |
| universe gate (removed) | `not_evaluable` / `missing_market_context` — uniform, including the last bar |
| carrier short-circuit (kept) | `not_evaluable` / `no_fill_bar` (`s12_…py:375`) |

The carrier path is the correct one, and the asymmetry is already written down as the token's
defect: *"The UNIVERSE gate above does refuse uniformly, including the last bar — that
asymmetry is pre-existing and pinned; it never routes through `evaluate`, so it has no path
to agree with"* (`s12_…py:362-368`). `evaluate` stamps a segment's final bar `no_fill_bar`
before reading any input (`strategy_registry.py:467-475`) and that is what **every peer
strategy already stores** there.

### 2.1 How many stored rows this moves — by anti-join, not by peer absence

⚠⚠ **My first draft argued "0 of 5,791" from the ABSENCE of peer `no_fill_bar` rows on
2026-09-17. Codex rejected that as insufficient (finding 11) and it was right**: peer absence
only transfers if the peers covered the same instruments and dates under the same
segmentation, and no joined coverage check was run. Measured directly instead.

A break date `B` cuts at `bisect_left(series.dates, B)` (`price_segments.py:54`), and
`price_series_break.break_date` is *"the first date at the new scale"* (module docstring). So
the 2026-09-17 bar at index `i` is a segment terminus **iff** some unresolved break falls in
`(dates[i], dates[i+1]]`. Anti-joined on exactly that, dev DB, read-only:

```sql
with s12 as (select distinct instrument_id from strategy_signal_observations
             where strategy_id='s12-cheapest-band-price-gated-breakout'
               and signal_bar_date = date '2026-09-17'),
nxt as (select s.instrument_id,
          (select min(p.price_date) from price_daily p
            where p.instrument_id = s.instrument_id and p.price_date > date '2026-09-17') as next_bar
        from s12 s)
select count(*) from nxt n
join price_series_break b on b.instrument_id = n.instrument_id and b.resolved_by is null
where b.break_date > date '2026-09-17' and (n.next_bar is null or b.break_date <= n.next_bar);
--  0
```

Context for that zero, same session:

- `select count(*) from price_series_break where resolved_by is null` → **415**, of which **4**
  have `break_date > 2026-09-01`.
- **77** of the 5,791 S-12 instruments carry an unresolved break somewhere in their history —
  so the zero is not "no breaks exist", it is "no break lands at that bar's boundary".
- `strategy_signal_observations` for S-12: **5,791** rows, all
  `not_evaluable` / `missing_market_context`, one per instrument, all on 2026-09-17, one
  `strategy_version`.

⚠ **Stated as a count, not a rate.** My first draft divided the 6 stored `no_fill_bar` rows by
922,821 observation rows and called it an incidence rate; that denominator is wrong (finding
13) — the table excludes fired rows and retains bounded history, so the quotient is neither a
population boundary rate nor a forward estimate. The honest statement is the two numbers: **6
`no_fill_bar` rows exist**, on 2 `(instrument, date)` pairs × 3 strategies
(`1059561`/2026-08-26 and `1052636`/2026-09-02 for S-4, S-8 and S-11). A future scan whose
window contains a segment terminus will store `no_fill_bar` for S-12 where today it would
store `missing_market_context`. That is the correction, not a regression.

⚠ The **write window already excludes the SERIES terminus** — `write_window_indices`' upper
bound is `min(frontier, n - 1)` (`strategy_signal_scan.py:322-366`) — so only segment
boundaries can produce this, never the end of history.

### 2.2 The cross-sectional path, which my first draft omitted entirely

Finding 12. `segmented_member` stages per segment too, and
`stage_cross_sectional_member` assigns a terminal refusal before inputs, decision dates or
ranking. **S-12 cannot reach it:** its manifest entry is `strategy_class == "per_series"` with
`signal_kinds == {"entry"}` and no `member` adapter
(`tests/test_2840_cheapest_band_price_gated_breakout.py::test_the_manifest_registers_s12_as_an_entry_only_per_series_strategy`),
so `segmented_member` never dispatches it. The cross-sectional adapters that DO exist (S-2,
both S-10 legs) discard `price_basis` (§0.1). §7 test 7 pins that no member adapter forwards
it, so this stays true by test rather than by inspection.

## 3. The fail-open the removal creates, and how far the fix actually goes

Today the live scan is protected **twice**: the universe token, and
`SCAN_ARCHIVE_ADJUSTMENT_BASIS = None`. After §1 only the second remains, and it is a
`str | None` module constant — **one token edit, `None` → `"unadjusted"`, certifies every
`price_daily` bar** on the back-adjusted path §1 describes. Leaving a one-token route to that
is not an acceptable trade for the de-overloading.

So the constant goes too, replaced by a constructor that cannot certify:

```python
def from_undeclared_source(*, n_bars: int) -> PriceBasisSeries:
    """No as-traded provenance source is DECLARED for this path — every bar refuses."""
    if n_bars < 0:
        raise ValueError(f"n_bars must be non-negative, got {n_bars}")
    return PriceBasisSeries(values=(None,) * n_bars, not_evaluable_indices=tuple(range(n_bars)))
```

⚠ The `n_bars < 0` guard is finding 9: without it `n_bars=-1` returns an empty carrier while
`from_archive_basis(None, n_bars=-1)` raises, so the two would not agree on the one input that
distinguishes them. §7 test 3 covers it.

### 3.1 What this does and does NOT achieve — my first draft overstated it

⚠⚠ **I wrote that "certifying the scan requires writing a constructor that reads a real
source". That is FALSE and the correction is Codex's (findings 7 and 8).** Any caller can
write `PriceBasisSeries(values=("observed_unadjusted",) * n)` inline, which certifies
everything and passes §7 test 4's grep. The dataclass is public and the protocol is
structural — that is what made the carrier cheap to wire and it is also what makes this
bounded.

What is true, stated at its actual strength:

- **It removes the one-token edit.** The dangerous change is no longer flipping a `None` in a
  `str | None` slot that a reader could take for a fact; it is writing a certifying
  constructor call, which is visible in review and in the grep.
- **It keeps the declaration and drops the inference.** `9246f77c` withdrew *"`price_daily`
  has no adjustment-basis column, therefore no certificate is possible"* — a missing column
  does not imply missing evidence. The narrower true statement is *no source has been
  declared*, and `from_undeclared_source` says exactly that where a `None` could be read
  either way.
- **It stops `from_archive_basis` being called where there is no archive.** The scan has no
  pinned archive; the function name asserts one. That mismatch is how a module constant came
  to look like a per-series fact.

⚠ **It is not a second gate and must not be sold as one.** It is the same single gate with its
fail-open direction closed by construction rather than by a comment. Re-adding a redundant
universe token under a new name would be the defect this change removes.

### 3.2 The residual invariant this change DOES close

Finding 19: `s12_signals` never checks `price_basis.rule_set_version`, so a carrier built
under an older rule executes under today's identity — and `PRICE_BASIS_RULE_VERSION` is in
`S12_PARAMS`, i.e. the identity *claims* a version the input need not match. Pre-existing in
`07f62f2f`, and it becomes load-bearing the moment the carrier is the sole gate. So
`s12_signals` gains, beside its existing length backstop:

```python
if price_basis.rule_set_version != PRICE_BASIS_RULE_VERSION:
    raise ValueError(...)
```

⚠ Length equality still does not bind a carrier to an instrument, its dates or its payload
(finding 19's remainder). Recorded in §8, not closed here — binding it needs the per-bar
source, which does not exist.

## 4. Source rule

**No new data-treatment decision is taken.** Everything this change reads is fixed elsewhere:

| decision | where it is already fixed |
| --- | --- |
| what evidence makes a level as-traded | **`sql/305`'s CHECK** — *a directly observed unadjusted level* or a PIT reconstruction. This is the governing rule, cited per finding 20 |
| the stored label that maps onto it | `CERTIFYING_ARCHIVE_BASES = {"unadjusted"}`, the one member of `sql/249`'s 4-member CHECK that `cost_model.cost_price_basis:559` also maps to `as_traded`; unchanged |
| what `None` means for a provenance | `archive_policy_for:95-100` — *"`None` is WITHHOLDING, not eligibility"*; unchanged |
| the reason code a basis refusal stores | `PRICE_BASIS_REFUSAL_REASON = "missing_market_context"`, one of `OUR_ADDITIONAL_REASON_CODES`; unchanged |
| the final-bar verdict | `evaluate` → `no_fill_bar` (`strategy_registry.py:467-475`); unchanged |
| the gate edge | `min(BANDS, key=p75_spread_pct).lower`; untouched |

⚠ **`sql/249`'s CHECK is a VOCABULARY, not an evidence standard** (finding 20). The
label→eligibility step is `sql/305`'s, and the archive-label mapping is an **existing
operational assumption**: `research_corpus_ingest.py:165-171` recorded the Intrader archive's
`unadjusted` basis from two AAPL bars. This change inherits that assumption unchanged and §8
bounds every claim that rests on it.

`from_undeclared_source` introduces **no constant**: it is the `n_bars`-shaped value
`from_archive_basis(None, …)` already returns. §7 test 3 pins the equality rather than
asserting it in a comment.

## 5. The identity rotation, re-priced — the stored rows are ALREADY detached

⚠⚠ **My first draft priced this against the 5,791 stored rows. That was wrong in the cheap
direction, and finding 15 caught it: `07f62f2f` already rotated S-12.** Measured this session:

```
s12_identity(universe="survivor_only",      …).version = strategy-registry-v1+7f860b9df89a
s12_identity(universe="survivorship_free",  …).version = strategy-registry-v1+e1a861a6f6cb
stored on all 5,791 observation rows        = strategy-registry-v1+b18d5869092b
```

So the live version is **already** not the stored one, and **this change detaches zero
currently-attached rows.** Both S-12 universe identities rotate again (module bytes; and the
carrier edit also moves `PRICE_BASIS_RULE_VERSION`, which `S12_PARAMS` carries).
`INPUT_RULE_SETS` is untouched, so the other 10 strategies do not rotate (§0).

### 5.1 The rows are not re-emitted, and that is the existing no-backfill rule

⚠⚠ **My first draft said "the next scan re-emits them under the new one". FALSE (finding
14).** `write_window_indices` on a cold start — which a new `strategy_version` is, having no
watermark — returns `range(end - 1, end)`: **exactly one bar, the one before the frontier**
(`strategy_signal_scan.py:385-386`). Its own docstring gives the rule and it is a source rule,
not a performance choice:

> With no watermark the "strictly after" bound is vacuous and the window would be the
> instrument's ENTIRE history — a backfill, which spec §11 forbids outright: *"Signals are a
> function of what was known on the day."*

So under the new version 2026-09-17 is **never written**. The new track record starts at the
next scan's frontier−1 bar and accrues forward. The old rows remain readable under the old
version and are not ambiguous (`strategy_version` is part of the key), but they are **not
replayed** and nothing should be written that implies they will be.

⚠ **Not an in-place correction.** A new `strategy_version` is a track record beside the old
one — `SCAN_UNIVERSE`'s own stated rule for a population change, and it applies to a rule
change identically.

## 6. What lands

**`app/services/strategy_price_basis.py`**
- `from_undeclared_source(*, n_bars: int)` (§3), with the `n_bars < 0` guard, exported. The
  policy paragraph currently on `SCAN_ARCHIVE_ADJUSTMENT_BASIS` moves onto it.

**`app/services/strategies/s12_cheapest_band_price_gated_breakout.py`**
- Delete `AS_TRADED_UNIVERSES`, the gate at `:344-348`, the `as_traded_universes` params key
  and the `__all__` entry.
- Add the `rule_set_version` check (§3.2).
- Rewrite the module docstring's *"THE PRICE MUST BE AS TRADED"* paragraph: enforcement is the
  declared `price_basis` input. Keep the `price_daily` back-adjustment fact — it is now the
  reason the scan's source is **undeclared**, which is where it belongs.
- Correct the `S12_PARAMS` comment: the sixth `INPUT_RULE_SETS` entry is owed **on a second
  consumer**, not on this item (§0).

**`app/services/strategy_signal_scan.py`**
- Delete `SCAN_ARCHIVE_ADJUSTMENT_BASIS`; both call sites (`:1001`, `:1041`) build
  `from_undeclared_source(n_bars=len(series))`. Drop it from `__all__` if present.

**The four misdeclaring script callers (§1) — `from_undeclared_source`, verdict-preserving**
- `scripts/verify_2394_signal_scan_cost.py` (4 sites), `scripts/verify_2394_strategy_manifest.py`
  (3 sites), `scripts/verify_2394_backtest_run.py:543`,
  `scripts/verify_2437_missing_market_context.py:91`. Each gains a one-line reason: the path
  has no pinned archive, so it has no archive basis to pass.
- ⚠ `verify_2394_strategy_manifest.py` compares `entry.signals(...)` against a `direct(...)`
  call; both sides of each comparison must move together or the script reports a mismatch that
  is its own.

**`scripts/verify_2437_s10_census.py`** — follows the constructor change (`:45`, `:102`).

**`scripts/project_2840_panel_accrual.py`**
- ⚠⚠ **The universe refusal at `:212-216` is KEPT, not deleted.** My first draft deleted it as
  "subsumed by the `cost_price_basis == 'as_traded'` check at `:199`". Findings 16 and 17:
  those two checks do **different jobs**. `:199` is a price-basis check on a report FIELD (and
  the report field is not the resolved policy — `ab_3238_cost_basis` exists precisely because
  charging basis can be overridden independently, so field-equals-policy is a property of
  today's producer and not a contract). `:212` is a **population** check: this script's rates
  are declared as *"conditional on an evaluable name-day in a survivorship-free 1962-2021
  cross-section"*, and without it the checker would accept a `survivor_only` report while
  still printing that sentence.
- So the check stays and is **re-sourced**: it reads the script's own
  `EXPECTED_UNIVERSE = BACKTEST_UNIVERSE` instead of importing a strategy constant, and its
  comment says POPULATION rather than as-tradedness — which is what it was always doing.
- Update the `CAPACITY IS NOT EXPOSURE` caveat (`:669`): S-12 still refuses the scan, now
  because no source is declared for it.

**`scripts/census_2840_forward_daily_provenance.py`** — stale `AS_TRADED_UNIVERSES` reference
in the header comment.

**Tests** — `tests/test_2840_cheapest_band_price_gated_breakout.py`
(`test_a_universe_whose_prices_are_not_as_traded_refuses_every_bar:448`,
`test_the_refusal_is_not_a_decline_even_where_s4_would_fire:465`,
`test_only_the_backtest_universe_is_declared_as_traded:479`,
`test_widening_the_declared_universes_moves_the_version:488`),
`tests/test_2840_s12_price_basis_gate.py::test_the_universe_gate_still_fires_first_on_the_scan_universe:139`,
`tests/test_2840_panel_accrual_projection.py:245` — all four of the first group assert the
removed rule (finding 18), and the projection one needs its assertion changed, not just its
docstring.

## 7. Tests

1. **A certified carrier evaluates on `survivor_only`** — the inversion, and the single test
   that reads the whole change off. Same fixture as the old
   `test_the_universe_gate_still_fires_first_…`; asserts a fire at `FIRING_INDEX`.
2. **An uncertified carrier refuses on `survivorship_free`** — the other direction, so the
   pair proves the verdict follows the BASIS and not the universe.
3. **`from_undeclared_source` certifies nothing and equals `from_archive_basis(None, …)`** at
   `n = 0, 1, 200`, **and both raise on `n_bars = -1`** (finding 9).
4. **The scan builds its carrier from the undeclared source** — `strategy_signal_scan`'s source
   contains no `from_archive_basis` call. A tripwire, and §3.1 states its limit: it does not
   stop an inline certified `PriceBasisSeries`.
5. **Segment mechanics** (finding 10): a singleton segment, two boundaries, and a break date
   falling *between* delivered dates — the `bisect_left` case — each asserting the segment-final
   bar is `no_fill_bar` and every earlier bar `missing_market_context`.
6. **`INPUT_RULE_SETS` still has five entries** — already asserted at
   `tests/test_strategy_registry.py:251`; named because §0 rests on it.
7. **No `MemberStager` adapter forwards `price_basis`** (§2.2, finding 12) — keeps the
   cross-sectional path out of scope by test rather than by inspection.
8. **A carrier built under a different `rule_set_version` raises** (§3.2).
9. The existing tripwire (`test_s12_is_the_only_consumer_…`) is UNCHANGED and must stay green.

## 8. Not claimed, and the obligations left open

- **That the scan can now be certified.** It cannot; nothing about its provenance changed.
  The forward per-bar source is still blocked on the absent forward corporate-action calendar
  (`capital_events` is a capital-pot ledger, 0 rows).
- **That the carrier's source selection is versioned.** It is not (finding 8) — rewiring which
  constructor the scan calls changes verdicts without moving any hashed input. Pre-existing in
  `07f62f2f`; §7 test 4 is a tripwire, not a fix.
- **That a certified carrier is bound to its series.** Length equality is the only check
  (finding 19's remainder): nothing ties a carrier to an instrument, its dates or its payload.
- **That an unknown universe string is rejected.** The removed token incidentally refused one
  (finding 4); `Universe` is a `Literal` enforced by pyright and nothing else, which is the
  convention every other strategy in the package already runs under. Stated as a loss, not
  repaired with a validator this package has no precedent for.
- **That the payload is validated.** The archive's `unadjusted` label rests on two AAPL bars
  (§4, finding 21). This moves the gate from a universe NAME to that measured-on-a-sample
  LABEL — one step closer to the fact, not the fact.
- **That entry certification certifies a trade.** Fills, held-position marks and costing stay
  uncertified (`07f62f2f` §6, unmoved). A certified decision can still consume indicators warmed
  on uncertified history — bar-local gating is not bar-local contamination.
- **That `no_fill_bar` at a segment terminus is desirable.** It is what segmentation already
  produces for every peer; §2 records it as a consequence, not a goal.

## 9. Not claimed about the measurements

- The `0` in §2.1 is a **dev-DB** anti-join at the 2026-09-17 bar. It bounds what a re-scan of
  *that* date would have moved; §5.1 then shows that date is never re-written anyway.
- The 5,791 / 415 / 77 / 6 figures are stored-state counts, not rates. §2.1 says why no rate is
  quoted.

## 10. What checkpoint 1 moved

| # | finding | disposition |
| --- | --- | --- |
| 1 | §1's "loses nothing" is false; a certified carrier on `survivor_only` flips 175 fixture verdicts | **CONFIRMED** — §1 rewritten as a per-CALLER table; the universe-member argument withdrawn |
| 2 | `verify_2394_signal_scan_cost.py:428` passes literal `"unadjusted"` over live `price_daily` | **CONFIRMED in source** (`:103` `UNIVERSE = "survivor_only"`, docstring `:12`) — §6 fixes 4 sites |
| 3 | `verify_2394_backtest_run.py:543` same shape | **CONFIRMED** (`UNIVERSE` from `verify_2240_position_builder:96` = `survivor_only`) — fixed |
| 4 | the token incidentally rejected unknown universe strings | **ACCEPTED as a stated loss** — §8; no validator, no precedent for one |
| 5 | §0's importer grep cannot establish the consumer set | **CONFIRMED** — §0.1 replaced with the adapter dependency analysis; the grep demoted to a tripwire |
| 6 | the prior spec explicitly COMMITS to installing the entry with item 3 | **CONFIRMED** (`the-price-basis-carrier.md:311-313`) — §0.1 says SUPERSEDED, on stated grounds |
| 7 | §3 moves the edit, it does not require real evidence | **CONFIRMED** — §3.1 added; the "requires reading a real source" claim withdrawn |
| 8 | source selection is outside every identity hash | **CONFIRMED** — recorded as pre-existing and open (§8) |
| 9 | `from_undeclared_source` mishandles `n_bars=-1` | **CONFIRMED** — guard added, §7 test 3 |
| 10 | segment mechanics undertested | **ACCEPTED** — §7 test 5 |
| 11 | "0 of 5,791" not established by peer absence | **CONFIRMED; re-measured** by direct anti-join, §2.1. Answer unchanged, evidence replaced |
| 12 | the cross-sectional path is unanalysed | **CONFIRMED** — §2.2 added; S-12 is `per_series` with no `member`, pinned by §7 test 7 |
| 13 | the incidence denominator is wrong | **CONFIRMED** — rate dropped for two counts; the `2 × 2 × 3` phrasing corrected to 2 pairs × 3 |
| 14 | §5's replay promise conflicts with the cold-start rule | **CONFIRMED** (`:385-386`) — §5.1 added; "re-emits them" withdrawn |
| 15 | the identity accounting is stale | **CONFIRMED by measurement** — the live version is `7f860b9df89a`, stored is `b18d5869092b`; §5 re-priced at **zero** detached rows |
| 16 | report field ≠ resolved policy | **CONFIRMED** — the equivalence claim withdrawn |
| 17 | deleting the projection check widens its population contract | **CONFIRMED** — the check is KEPT and re-sourced to the script's own `BACKTEST_UNIVERSE` |
| 18 | three more tests assert the removed behaviour | **CONFIRMED** — four, listed in §6 |
| 19 | `rule_set_version` unchecked; carrier unbound to series | **CONFIRMED** — §3.2 closes the version half; the binding half recorded open |
| 20 | `sql/249` is a vocabulary, not an evidence standard | **ACCEPTED** — §4 cites `sql/305` and names the label mapping as an inherited assumption |
| 21 | two AAPL bars do not validate the archive | **ALREADY IN §8** — unchanged |

Refs #2840. Refs #2437.
