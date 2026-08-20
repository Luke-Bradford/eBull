# Phase 4 — the outcome resolver

Parent design: `docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md`
§3 decision 1 (win definition), §3 decision 10 (quarantine + segments), §5 phase 4.
Execution semantics: `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`
§3.5 and §5 criteria 2/8/9/10/11. Spike S5 (#2245) is ANSWERED.
Predecessor: `docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md`.

Refs #2240, #2245, #2288.

## 1. Scope

The ledger records what fired and where it filled. This phase answers **what
happened next**, per filled entry: `tp_hit` / `sl_hit` / `expired` /
`ambiguous` — the parent's four — plus `unresolved`, which is ours and is
argued for in §3.4 rather than assumed.

Not in this phase:

- **Levels.** The resolver does not compute a take-profit or a stop-loss. They
  are inputs, because they are strategy parameters and criterion 11 puts every
  parameter inside the identity hash. ⚠ S5 corrected the parent ticket's
  premise here: `entry_timing._compute_take_profit` reads a thesis
  `base_value` — a fundamental valuation target — and returns `None` without
  one. It is not ATR-based and does not apply to a TA strategy.
- **Costs** (criterion 2). Spread, overnight carry and FX are portfolio-level
  and belong to the phase-5 backtester. The resolver classifies a level touch;
  it does not price a P&L, and its return field is named `gross_return_pct` so
  that nothing downstream can average it as performance by accident.
- **Rule-based exits.** A strategy that emits its own `exit` signal composes
  with a TP/SL bracket, and composition needs a position — which phase 5 owns.
  A `signal_kind = 'exit'` row is not an input here. ⚠ Consequence, stated so
  it is not discovered later: **a bracket outcome is not a strategy outcome**
  for any catalogue strategy that carries its own exit rule. Phase 4's
  statistics describe the bracket, and phase 6 must not label them otherwise.
- **Persistence.** Ticket 4b; see §6.
- **Bar validity.** `price_quarantine` owns it. The resolver consumes masked
  bars (§3.5); it does not re-derive verdicts.

⚠ **Long-only.** `S < E < T` is validated, and that is the repo's v1 risk
posture (`.claude/CLAUDE.md`: *"Long only in v1 · No shorting"*), not an
oversight. A short bracket inverts every comparison in §3.2 and would be a
different rule set with a different version, not a flag on this one.

## 2. What S5 settled, and what it did not

Measured on 25,559,104 signals, full corpus, five TP multiples: the ambiguous
rate runs 0.83% (TP = 1.0×ATR) down to 0.09% (4.0×). Three consequences bind
this spec:

1. **`ambiguous` is its own outcome class and is never silently assigned.**
2. **Never "assume SL first for conservatism".** S5: *"It is not conservative,
   it is a different bias — it makes TP-first strategies look systematically
   worse, and the distortion scales with how tight the TP is."*
3. **Intraday cannot rescue a historical bar.** `get_intraday_candles` hits an
   endpoint with **no date parameter, no offset, no cursor** — it returns the
   most recent `count` bars, capped at 1000 (≈ two sessions at `OneMinute`). A
   past date is not addressable. Forward-going signals are resolvable inside
   that window; historical ones never are.

So the resolver stamps `resolution_method` on every outcome. v1 has exactly one
value, `daily_bar`. That is not decoration: without the stamp a later
intraday-backed resolution mixes silently into the same statistics.

## 3. The rules

### 3.1 The window

Fill is at the OPEN of bar `f` (= signal bar + 1, resolved by phase 3c). The
holding window is bars `f … f + max_hold_bars − 1` **inclusive of `f`**.

⚠ The fill bar is INSIDE the window. We bought at its open; the rest of that
bar's range is real forward information and excluding it would understate both
TP and SL. Excluding it is the more common error and it is the flattering one
for tight stops.

### 3.2 Per-bar resolution, in order

For each bar `i` in the window, with `E` = entry price, `T` = take-profit,
`S` = stop-loss:

| # | test | outcome | exit price |
| --- | --- | --- | --- |
| 1 | `open[i] <= S` | `sl_hit` | `open[i]` |
| 2 | `open[i] >= T` | `tp_hit` | `open[i]` |
| 3 | `low[i] <= S` **and** `high[i] >= T` | `ambiguous` | none |
| 4 | `low[i] <= S` | `sl_hit` | `S` |
| 5 | `high[i] >= T` | `tp_hit` | `T` |

Rules 1 and 2 are not a tie-break heuristic. **The open is the first price of
the bar** — that is definitional in OHLC, not a chosen rule — so a bar that
gaps through a level has a *known* touch order and is not ambiguous. Rules 1
and 2 cannot both hold, because `S < T`.

⚠ **Neither rule 1 nor rule 2 can fire on the fill bar**, because the entry IS
`open[f]` and `S < E < T`. That matters for the execution assumption below: on
the fill bar the bracket is placed *at* the open, simultaneously with entry, so
there is no window in which it could have been gapped through. On every
subsequent bar the bracket is already resting when the bar opens.

The declared execution assumption behind rules 1/2 is a **resting bracket
order**: a stop-market at `S` fills at the open when the market gaps below it
(worse than `S`), and a sell-limit at `T` fills at the open when the market
gaps above it (better than `T`). Both directions follow from the same
assumption; taking only the unfavourable half would be the "conservatism is a
different bias" error S5 names.

⚠ **Rule 3 requires the whole bar.** It cannot be weakened to "resolve
favourably when the close is nearer the TP" or any other proximity argument —
§3.5.4: *"the order of touch is unknowable from OHLC … Silently resolving them
favourably is how backtests manufacture edge."*

### 3.3 Expiry

Neither level touched inside the window → `expired`.

⚠ **The expiry exit fills at the OPEN of bar `f + max_hold_bars`, not at the
close of the window's last bar.** §3.5.1 applies the fill rule to *"entries and
exits alike"*. A max-hold exit is a DECISION taken on a close, so it fills at
the next open — exactly like an entry. TP and SL are different and legitimately
fill intrabar, because they are resting orders placed in advance rather than
decisions taken on the close. Booking an expiry at the window's last close
would be a same-bar fill, which is the one thing this whole phase exists to
prevent.

⚠ This is a **construction**, not a citation: §3.5.1 speaks of signal-driven
entries and exits, and a max-hold liquidation is generated by the resolver
rather than by a signal. It is recorded in §4's table as constructed. The
alternative — booking the last close — is a same-bar fill, so the construction
is the only reading consistent with the rule it extends.

Consequence: `expired` needs `max_hold_bars + 1` bars from `f`. When the
corpus ends first, see §3.4.

### 3.4 `unresolved` — OUR addition, flagged as one

The parent's outcome vocabulary is four classes. None of them describes "we
entered but no defensible exit can be booked"; five terminal reasons make that
refusal countable:

| reason | when | whose |
| --- | --- | --- |
| `window_truncated` | the series ends before the window (or the expiry-exit bar) completes | ours |
| `series_break` | the window would cross out of the fill bar's segment | criterion 10 |
| `quarantined_bar` | the caller has declared a bar in the window unusable | criterion 8 |
| `missing_bar_data` | a field we must read is `NULL` and the caller declared no reason | ours |
| `unorderable_exit_levels` | the strategy's causal formula cannot produce a finite positive broker-orderable bracket | ours |

⚠ `expired` must NOT absorb `window_truncated`. A trade whose corpus ends on
bar 10 of a 20-bar window is not a trade that reached expiry — recording it as
`expired` books a return for a window that never ran, and the bias is
one-directional: it lands on every open trade at the corpus edge, which is the
most recent and most operator-relevant slice.

⚠ Truncation is only reached if the window was **otherwise undecided**. A TP
touched on bar 3 of a truncated window is `tp_hit`; a shorter window cannot
un-hit a level that was already hit. The walk terminates at the first decisive
bar and everything in §3.4 is what is left over. The same holds for masked
bars: a masked bar **after** the decisive one is irrelevant and the outcome
stands.

⚠ `window_truncated` mirrors phase 3a's `no_fill_bar` — an addition to
criterion 8's seven, flagged rather than smuggled in. It is a different case:
`no_fill_bar` means there was no `t+1` to enter at; `window_truncated` means we
entered and the exit is unknown. `missing_bar_data` is likewise ours, and is
kept distinct from `quarantined_bar` for exactly criterion 8's reason — a NULL
that the quarantine rules never looked at is a data gap, and calling it a
quarantine verdict would collapse the two. ⚠ Phase 3c widened `no_fill_bar` to
cover a NULL open and flagged the widening; **this spec does not repeat that
compromise**, because here the case is not measured-zero.

`unorderable_exit_levels` is also ours. It is terminal for that filled signal:
no exit price or return is invented, the refusal is counted, and the remaining
batch continues. In particular, an S-4 stop at or below zero is never clamped
to a broker minimum. The same versioned vocabulary applies to the historical
runner and the forward resolver.

**`unresolved` is excluded from the win rate exactly as `ambiguous` is, and
both counts are reported per criterion 9.** An outcome class that is excluded
without being counted is a silent narrowing gate.

### 3.5 Masking — the settled per-field treatment, not a new one

Design-doc decision 10: *"A bar can have a perfect close and a spurious wick
(XPER 2024-06-03 is `o 8.497 h 8.737 l 0.010 c 8.298`). Returns are untouched;
**every stop-loss in the phase-4 outcome resolver reads as touched**. A rule
set that only protects returns hands phantom fills to the backtester."*

⚠ The treatment for this is already settled and implemented; this spec adopts
it rather than inventing a second one. `research_price_structure_store.load_masked_series`:

> *"Masking is per FIELD, not per bar, because the two verdicts mean different
> things: `range_usable = False` is a bad wick (masks high/low),
> `return_usable = False` is a bad close (masks close). Masking the whole bar on
> either verdict would discard good data and shift every N-bar window."*

So a masked bar arrives with `high`/`low` already `None`, and the resolver's
rule is per field, matching:

- a **touch test** (rules 1-5) needs `open`, `high` AND `low` → refused if any
  is `None`. ⚠ Not just the range: without the open we cannot tell rule 1 from
  rule 4, and those disagree on the exit PRICE, nor rule 1 from rule 3, and
  those disagree on the CLASS.
- the **expiry exit** reads `open` alone → refused only if `open` is `None`.

A `range_usable = false` bar therefore still serves as a valid **expiry exit**,
which a whole-bar rejection would have thrown away. That is the difference
between honouring the source rule and inferring a "safe" one.

Two loader obligations the resolver cannot check and the caller must meet:

1. ⚠ **Fail-closed coverage.** The quarantine tables are SPARSE, so absence of
   a row means "clean" or "never evaluated" and a reader that cannot tell them
   apart admits the population the rules have not seen. `load_masked_series`
   already gates on a coverage row at the current `rule_set_version` and
   returns **zero bars** otherwise. Any caller of this resolver must use a
   loader with that property; the verification harness in §5 uses that one.
2. ⚠ **`provisional` bars.** A part-session bar's high/low are partial. It is
   the caller's job to declare them, via the same map as any other refusal.

⚠⚠ **The reason map ANNOTATES; the absent fields REFUSE.** That split is what
keeps the per-field rule intact — a map that refused whole bars would throw away
the good `open` of a range-masked bar, which is the *"discard good data"* the
loader warns against. So `masked_bar_reasons: Mapping[int, UnresolvedReason]`
supplies only the WHY, for the same reason phase 3a pairs a reason with each
`StrategyInput`: the resolver knows THAT a field is absent and can never know
why. A declared reason wins over the `missing_bar_data` fallback, so a
quarantine verdict and a data gap never collapse into one another.

⚠ Consequence, stated because it is otherwise a footgun: **annotating a bar
whose fields are all present is a no-op.** To refuse such a bar the caller masks
the field it does not trust. ⚠ `load_masked_series` masks `high`/`low`/`close`
on the two quarantine axes and used to **carry the open through unmasked** —
right for a bad wick, and a gap for a `B1`/`B4` sentinel bar, whose open is
untrustworthy too. **CLOSED 2026-08-08 by #2354**: the loader masks a NULL or
non-positive open by `rule_b1`'s own clause (*"any of open/high/low/close NULL
or `<= 0`"*), and `signal_ledger.resolve_fills` refuses one independently under
the `unusable_fill_price` code for callers that load bars by some other route.
⚠ The sentence this paragraph used to end on — *"a caller reading `B4` bars must
mask the open itself"* — was read by every caller since and discharged by none,
which is now its own prevention-log entry.

⚠ **The map and `segment_end_index` are REQUIRED arguments with no defaults.**
#2288's lesson applies unchanged: a field with a default is a field a writer can
forget. Forgetting `segment_end_index` spans a level break; forgetting the map
collapses every quarantine verdict into `missing_bar_data`, which is exactly the
countability criterion 8 exists to protect. A caller with nothing to declare
passes `{}` and `None`, visibly.

### 3.6 Series breaks are SEGMENTS, not bad bars

⚠ Modelling a break as a masked index is wrong, and the schema says so.
`price_series_break.break_date` is *"the bar at the NEW scale"*, and
`price_transition_quarantine` is *"keyed on the LATER bar"* because *"the
defect lives on a transition, not a bar … Bars either side of a level break are
valid prices in their own unit regime."* A trade **entered on or after** the
break date is entirely within the new scale and is perfectly resolvable;
masking the break bar would reject it.

Decision 10 names the right model directly — *"a per-instrument **segment**
model"* — so the resolver takes `segment_end_index: int | None`: the last index
of the fill bar's segment, or `None` when no break follows the fill. A window
(or an expiry-exit bar) that would read past it is `unresolved(series_break)`.

Required, no default, same reasoning as the mask map.

### 3.7 What is recorded

| field | note |
| --- | --- |
| `outcome` | the five classes of §1 |
| `resolution_method` | `daily_bar` in v1 (§2) |
| `reason` | required exactly when `unresolved`, forbidden otherwise |
| `exit_index`, `exit_bar_date` | ⚠ the DATE is recorded too — the ledger keys on dates, and an index is not durable across a corpus rebuild |
| `exit_price` | absent for `ambiguous` and `unresolved` |
| `bars_held` | `exit_index − fill_index`. ⚠ **0 for a same-bar TP/SL**, which is correct as a bar count and is NOT exposure time — criterion 7's exposure metric is phase 5's and must be defined there, not read off this field |
| `gross_return_pct` | `(exit_price − E) / E`. **Gross** is in the name because criterion 2 requires costs per trade and this number has none |
| `rule_set_version` | id + source hash, the construction used by `indicator_series` / `price_quarantine` / `price_structure`. Criterion 11 makes the execution assumption part of the identity, so the outcome carries the version of the assumption that produced it |

`ambiguous` and `unresolved` carry **no exit price and no return**. §3.5.4
excludes ambiguous outcomes from the win rate with their count shown; a return
column that is populated for them is a column something will eventually
average.

⚠ **The entry price is a REQUIRED argument, and is validated against
`open[fill_index]`.** Two failure modes, one guard: passing a price the caller
computed some other way is caught, and so is resolving a stored ledger row
against a corpus that has since been rebuilt or re-adjusted — where a silent
re-read would quietly reinterpret a recorded fill (spec §2.1: *"the ledger
stops being a record of what was actually decided"*). Disagreement raises.

`E > 0` is validated too, because `gross_return_pct` divides by it.

## 4. Source rules — what is cited, what is constructed

Per `.claude/CLAUDE.md`'s "source-rule before design", which binds quant
formulations and not only SEC data.

| decision | authority |
| --- | --- |
| win = TP before SL, with max-hold expiry | design-doc §3 decision 1 (operator) |
| long-only bracket (`S < E < T`) | `.claude/CLAUDE.md` risk posture |
| signal on close of `t` → fill at open of `t+1`, entries **and exits** | §3.5.1 |
| both levels in one bar → `ambiguous`, never a win | §3.5.4 + S5 (#2245) |
| per-FIELD masking, not per-bar | `research_price_structure_store.load_masked_series` |
| fail-closed against sparse quarantine coverage | sql/247 header |
| a break is a transition; segments, not bad bars | sql/246, sql/247, design-doc §3 decision 10 |
| `not_evaluable` reason codes are a closed, countable vocabulary | criterion 8 |
| exclusion is itself measured | criterion 9 |
| gap-through resolves at the open | **constructed** — see below |
| max-hold exit fills at the next open | **constructed** from §3.5.1 — see §3.3 |

⚠ **No published formulation exists for gap-through resolution**, so per the
repo rule it is fixed by construction and stated rather than left implicit: the
open is the first price of the bar, therefore a level already breached at the
open was breached first. The constants that *would* need freezing (TP multiple,
SL multiple, max hold) are **not in this module at all** — they are strategy
parameters inside the identity hash. Both constructions are covered by this
module's `RULE_SET_VERSION`, which is recorded on every outcome (§3.7).

## 5. Acceptance

1. A bar spanning both levels is `ambiguous`. Revert-probed by making the
   walk return `tp_hit` on that bar and asserting the test fails.
2. A bar that gaps below the stop resolves `sl_hit` at that bar's OPEN even
   when its high exceeds the take-profit — not `ambiguous`, and not at `S`.
3. `expired` fills at the OPEN of bar `f + max_hold_bars`. Revert-probed by
   booking the window's last close and asserting the test fails.
4. A window that runs off the end of the series is `unresolved`
   (`window_truncated`), never `expired`.
5. A level touched BEFORE the truncation point resolves normally.
6. Masking is per field: a bar with `high`/`low` `NULL` refuses a touch test
   but still serves as an expiry exit through its `open`.
7. A declared reason beats the `NULL` fallback — a masked bar the caller
   declared `quarantined_bar` never reports `missing_bar_data`.
8. A window crossing `segment_end_index` is `unresolved(series_break)`; a fill
   **at** `segment_end_index + 1` — i.e. inside the next segment — resolves
   normally.
9. Both the mask map and `segment_end_index` have no default — calling without
   either is a `TypeError`.
10. `reason` is present exactly when the outcome is `unresolved`;
    `exit_price` / `gross_return_pct` are absent for `ambiguous` and
    `unresolved`.
11. Levels are validated: `0 < S < E < T`, `max_hold_bars >= 1`, and an
    `entry_price` disagreeing with `open[fill_index]` — each raising.
12. **Full-population equivalence.** Every bar of the corpus as a hypothetical
    entry, classified by this module and independently in SQL, compared
    row-for-row. Zero mismatches. ⚠ Not a sample: the 3-series Bollinger check
    that showed a 40× margin and then failed on 193 of 7,354 series is the
    standing precedent, and #2260 is the reason this phase exists at all.

    ⚠ **What the SQL arm does and does not prove.** It is an independent
    implementation of §3.2/§3.3's *ordering* over the same masked inputs, so it
    catches a walk bug, an off-by-one window, a mis-ordered rule table. It does
    **not** cross-check the masking itself, the coverage gate or the segment
    model — both arms read the same loader — so those are covered by tests
    6/7/8 and by the loader's own harness, not by this arm. Saying which is
    the point; an equivalence run that is quietly assumed to prove everything
    is worse than none.
13. **Full-population distribution** over a grid of TP multiple × max-hold,
    with the `unresolved` share broken out by reason — criterion 9's "measure
    what you reject" applied to this module's own refusals. The SL rule is held
    fixed at `entry_timing._compute_stop_loss` (the repo's own, cited, and what
    S5 swept against) so the grid varies what this phase actually parameterises.

    ⚠ The census also asserts **which of its skip buckets are grid-invariant**
    before printing one cell's count as if it stood for all of them. That
    assertion found a real defect on the first full-corpus run: a branch
    commented "unreachable" fires on **flat price runs** (`open = high = low =
    close`, so true range is 0 and Wilder smoothing decays the ATR toward zero
    without reaching it), where `entry + tp×atr` rounds back to `entry` under
    `Decimal`'s 28-digit context. It was filed under `atr_not_positive` — a
    label that is false, since the ATR is positive — and now has its own,
    `levels_do_not_bracket`, printed per cell because it varies with the target
    multiple. ⚠ **Consequence for phase 5: `atr > 0` is not a "this instrument
    has volatility" gate.** A strategy eligibility filter or an ATR-divided
    position sizer that trusts it will admit these runs.
14. **OHLC internal consistency is measured, not enforced.** `low <= open <=
    high` is definitional, and a bar violating it makes §3.2 nonsense — but bar
    validity belongs to `price_quarantine`, not here. The harness counts
    violations on the full corpus and reports them; the resolver does not
    silently repair them.

⚠ Acceptance 12/13/14 numbers are produced by a committed script, never written
into prose. A hand-copied statistic goes stale in the place a reader trusts
most.

## 6. Tickets

| # | scope | depends on |
| --- | --- | --- |
| 4a | the pure resolver + the full-population verification arms | 3a, S5 |
| 4b | `strategy_outcomes` schema + writer, keyed to the ledger's `signal_id` | 4a, 3b, 3c |

4b is deliberately separate — but the identity question it turns on is closed
**here**, not left open, because an open one lets old outcomes be reinterpreted
under changed levels:

- **The level parameters live in `strategy_version`.** Criterion 11 is explicit
  that identity covers *"the same parameters with changed … execution
  assumption"*, and a TP multiple, an SL multiple and a max-hold are strategy
  parameters. They are therefore already inside the ledger row's version and
  must not be re-declared on the outcome row, where they could disagree with it.
- **The outcome row's key is `(signal_id, resolver rule_set_version)`.** The
  resolver's own version is NOT inside `strategy_version` — it is this module's
  source hash — so a changed execution assumption must be able to produce a
  second outcome for the same signal without overwriting the first. Same
  argument phase 3b made for putting `strategy_version` in the ledger key.

What 4b still owes: the storage shape, the backfill path, and whether
`resolution_method` belongs in the key once an intraday method exists.
