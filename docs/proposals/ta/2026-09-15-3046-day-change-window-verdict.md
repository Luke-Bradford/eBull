# #3046 build item 1 — the verdict-aware window loader, landing with the day-change consumer

Build item 1 of #3046's scope item 2. The contract was decided in
`2026-09-15-3046-transition-verdict-contract.md` (four composing clauses) and the first
attempt at a consumer — `risk_metrics` — was **withdrawn at Codex checkpoint 1** on
`e6c8d6ee`. That withdrawal named where to start instead: a two-bar window, one clause
each, no benchmark operand, no mixed-awareness row.

This proposal takes that instruction and ships the loader **with** its first consumer, per
the contract's own rule: *"The loader lands in the same PR as its first consumer."*

⚠ **Revision 2 — incorporates Codex checkpoint 1 (48 findings).** Corrections that changed
the design are marked **[ckpt-1 #n]**. The headline one is that the weekend qualifier the
contract measured with is **wrong for four asset classes**, and would have suppressed 28 of
63 FX day-changes.

## Source rule

Repo rules, not an external reg. Every line number verified at write time.

- **`docs/proposals/ta/2026-09-15-3046-transition-verdict-contract.md`** — the four
  clauses, their carriers, the shape of the missing loader. Not re-derived here.
- **`sql/247_price_quarantine.sql:23-33`** — the fail-closed coverage contract: *"a bar is
  usable only if its instrument has a coverage row at the current rule-set version AND the
  bar falls inside [first_bar, last_bar]. Everything else reads UNKNOWN, which is not the
  same as usable."* ⚠ UNKNOWN is a **third state**, not a synonym for unusable.
- **`sql/247_price_quarantine.sql:4-6`** — *"price_daily is raw vendor data and stays that
  way."* Verified at this commit: the only `UPDATE price_daily` in `app/`
  (`market_data.py:1236-1250`) writes derived feature columns; no code path rewrites OHLC.
  **[ckpt-1 #10]** This is what settles the resolved-break question below.
- **`sql/247_price_quarantine.sql:70-75`** — the sparse-table invariant. The quarantine
  predicate is `cardinality(rules) > 0`, never row presence.
- **`sql/247_price_quarantine.sql:60-68`** — *"a transition touching a provisional bar gets
  NO T3 verdict — it is deferred, not quarantined."* **[ckpt-1 #15]** Deferred is a third
  state on the transition table too, and reads as clean under a `cardinality(rules) > 0`
  predicate alone.
- **`sql/246_price_adjustments_and_series_breaks.sql:4-10,110-111`** — resolution is
  *"a resolution step, not an input to the classifier"*; `resolved_by IS NULL` means the
  two sides cannot be joined. `app/services/price_adjustments.py:5-8` — the module
  *"does NOT detect adjustments"* and only reads the table back.
- **`app/services/price_quarantine.py:525-537` (`rule_w1`)**, **`:540-556` (`rule_w2`)** —
  the two window rules: written, unit-tested, versioned, **zero production callers**.
- **`app/services/price_quarantine.py:98-163`** — `ClassParams` / `params_for`;
  `_FIVE_DAY = 1.4` is `7/5`, `_SEVEN_DAY = 1`. ⚠ `fx`, `commodity` and `index` are
  **not** five-day classes in this mapping, which is the defect in §"the qualifier".
- **`sql/247_price_quarantine.sql:40-46`** — `price_quarantine_coverage.asset_class` is
  stored *"as seen at evaluation time"*, so it is the class the stored verdicts were
  produced under. Measured fully populated: 12,284 rows, 0 NULL, 9 distinct classes.
- **`app/services/price_masked_bars.py:56-86`** — the existing fail-closed loader this one
  mirrors in *contract* (JOIN coverage, LEFT JOIN the sparse verdict table) and
  deliberately not in *transport*.
- **`app/services/price_segments.py:30-41`** — `load_unresolved_breaks`, the batch shape.
- **`app/services/market_data.py:187-256`** — `compute_day_change` / `load_day_changes`.

## Premise falsification — measured before speccing, full population

Read-only against the dev corpus at `price-quarantine-v1+49ff29fea766`, on the exact
population `load_day_changes` serves (the two most recent `close > 0` bars): **12,262
instruments**. Reproduced by the shipped script, never written by hand (§5).

| clause | reason code | fires on |
| --- | --- | ---: |
| 1 — bar field unusable (B1/B4 anywhere in the window) | `bar_return_unusable` | **0** |
| 2 — sides cannot be joined | `unresolved_break` | **5** |
| 3 — window spans a quarantined transition (W1) | `quarantined_transition` | **95** |
| 4 — horizon stretched (W2, session-qualified) | `horizon_stretched` | **203** |
| — window not inside the evaluated interval | `coverage_after_last_bar` | **6,742** |
| — no coverage row at the current version | `coverage_missing` | 0 |
| — window starts before the evaluated interval | `coverage_before_first_bar` | 0 |
| — a deferred (provisional) transition in the window | `verdict_deferred` | 0 |

Verdicts: **ok 5,312 · unverified 6,742 · quarantined 208** (clauses 3 and 4 overlap on 90).

⚠ **These are a snapshot, and the script is the source — not this table.** `daily_candle_refresh`
runs many times a day, so every figure here moves with the corpus. The A/B re-run at
2026-09-15 12:4xZ, on the same rule set, returned **ok 5,305 · unverified 6,742 ·
quarantined 215** (`horizon_stretched` 210, `quarantined_transition` 95,
`unresolved_break` 5) — seven more suppressions from seven new windows, not from a
behaviour change. Reproduce with
`PYTHONPATH=. uv run python -m scripts.verify_3046_day_change_verdict`; the acceptance
conditions in §5 are what must hold, and they are re-derived on every run.

**[ckpt-1 #22, #23] Measured, not assumed: the stored bar count between the two positive
closes is 2 for all 12,262 windows.** Codex's objection was that intervening zero/NULL
sentinel rows could make the pair non-adjacent, inflating W2's denominator. Zero exposure
on this corpus today. It is still a *rule* — `bar_count` is read from `price_daily`, never
assumed to be 2 — and clause 1 is the backstop, because a sentinel row is exactly what B1
condemns.

### ⛔ What the 208 look like — the defect is operator-visible and large

The "Day change" column on `/instruments` and the summary strip on `/instrument/<symbol>`
currently render, straight from `price_daily` with no verdict consulted:

| symbol | window | rendered as "day change" | rule |
| --- | --- | ---: | --- |
| ALNEV.PA | 2026-06-12 → 2026-06-24 | **+999,900.00%** | T2 |
| A4Y0.DE | 2025-07-03 → 2025-11-09 | **+138,885.71%** | T2 |
| ORTX.CVR | 2024-01-29 → 2024-05-21 | +49,900.00% | T2 |
| TTCFQ | 2024-11-21 → 2024-11-22 | +4,800.00% | T3, unresolved break |
| MOND | 2025-01-14 → 2025-01-15 | −99.18% | T3, unresolved break |

Two distinct failures in one column. The T3 rows are a transition the rule set has
contained; the T2 rows are a **hole** — four months of absence rendered as a day change and
stamped "as of" a date four months after its other operand. Clause 4 catches the second
kind and is the larger of the two (203 vs 95).

⚠ **[ckpt-1 #41, #42]** Stated as containment, not diagnosis: the rule set *"NEVER
identifies a cause"* (`price_quarantine.py:5-6`), and of 244 candidate T2 rows only 8 are
adjudicable against an independent archive at all. Nothing here claims these prices are
wrong — only that the **ratio is not a return**, which is the only claim the rule set
makes and the only one the suppression needs.

### ⚠⚠ The coverage state is structural, which decides the enforcement shape

`price_quarantine_refresh` (`app/workers/scheduler.py:5775`) is a 24h-cadence job.
Measured from `job_runs` over the retained window (three days — all that is retained, and
stated as such rather than generalised **[ckpt-1 #38]**):

| day | `price_quarantine_refresh` runs | `daily_candle_refresh` runs |
| --- | ---: | ---: |
| 09-13 | 1 (04:30Z) | 20 |
| 09-14 | 1 (11:28Z) | 17 |
| 09-15 (to 11:00Z) | **0** | 1 |

Candles land many times a day; verdicts once. The consequence at the moment of
measurement: every coverage row was written in one transaction at 2026-09-14 11:28:33Z
(**[ckpt-1 #39]** — that is `now()`, a transaction timestamp, not a per-instrument
evaluation time), and the lag against each instrument's frontier bar takes exactly two
values: **6,742 at 3 days, 5,542 at 0**. The 3 is Friday 09-11 → Monday 09-14 — the job
ran before Monday's US close.

So a consumer that suppressed on UNKNOWN would blank the day-change column for 55% of the
list on data that is almost certainly fine, and would do so again after every close until
the next refresh. `price_masked_bars` drops uncovered bars because its callers **trade** on
them; a display number is not a trade, and the two are entitled to different handling of
the same third state. **[ckpt-1 #1]** — this is a consumer policy, which the contract
expressly left to the first consumer (*"the enforcement shape of a refusal … belongs with
the first consumer"*), not a reinterpretation of `sql/247`. The loader's contract is
unchanged: it reports UNKNOWN and never reports it as clean.

## Enforcement shape — three states, one of which suppresses

| verdict | when | day-change behaviour |
| --- | --- | --- |
| `ok` | covered, no clause fired | value rendered, unchanged from today |
| `unverified` | any coverage reason, or `verdict_deferred` | **value rendered**; reason codes on the API; the existing "as of" caption's tooltip says which |
| `quarantined` | any of clauses 1–4 | **value suppressed** (`day_change`, `day_change_pct` → `null`); reasons on the API; FE renders its existing "—" with a tooltip naming the reason |

Four calls worth stating because they are the arguable ones:

1. **The UNKNOWN reasons are distinguished, not lumped.** **[ckpt-1 #3]** `coverage_missing`,
   `coverage_before_first_bar`, `coverage_after_last_bar` and `verdict_deferred` are
   separate codes, separately counted in the A/B. They share a *verdict* today because the
   consumer's action is the same; the distinction is preserved so a later consumer can
   split them without re-deriving it.
2. **`unverified` is visible, without a per-row badge.** **[ckpt-1 #2]** A badge on 55% of
   rows is noise carrying no action; silence is a claim the state does not exist. The
   compromise is the tooltip on the caption that is already rendered next to every day
   change.
3. **A fired clause outranks a coverage reason.** Both reason lists are returned either
   way. ⚠ **[ckpt-1 #4, #5]** The precedence is only reachable when a verdict *exists*, so
   the loader must not discard evidence in order to report UNKNOWN: an instrument with no
   coverage row still has its verdict rows loaded, and `assess_window` evaluates every
   clause before choosing a verdict. Revision 1 returned `None` for the whole struct in
   that case, which lost exactly the evidence the precedence rule is about.
4. **Resolved breaks are NOT excluded for this consumer.** **[ckpt-1 #10, #11, #12]** The
   contract's exclusion rule was written for `price_segments`, whose callers apply the
   adjustment factor. This consumer divides two **raw** closes, and no code path rewrites
   `price_daily`'s OHLC (verified above), so a resolved 1→10 break still renders +900%.
   The loader returns the resolution status as data; the consumer ignores it. Exposure
   today is 0 (no break is resolved), so this is a rule decision, not a count.

## What ships

### 1. `app/services/price_window_verdict.py` — new

Transport plus a pure assessor. **Nothing here defines a rule**: `rule_w1` and `rule_w2`
are called, never mirrored (`verify_3046_consumer_exposure` already made the mirroring
mistake once with `rule_w1` and had it deleted).

```python
@dataclass(frozen=True)
class WindowInputs:                            # one instrument
    coverage: tuple[date, date] | None         # None = no row at the current rule-set version
    quarantined_transitions: tuple[date, ...]  # LATER date, cardinality(rules) > 0
    deferred_transitions: tuple[date, ...]     # provisional with empty rules
    unresolved_breaks: tuple[date, ...]        # clause 2's own operand
    return_unusable_bars: tuple[date, ...]     # B1/B4
    weekend_bar_dates: frozenset[date]         # weekend days this instrument PRINTED on
    trades_weekends: bool                      # [ckpt-2] is an ABSENT weekend day a hole?

def load_window_inputs(conn, instrument_ids, *, since: date) -> Mapping[int, WindowInputs]
def assess_window(inputs, *, window_start, window_end, bar_count) -> WindowAssessment
```

- **[ckpt-1 #9]** `unresolved_breaks` is clause 2's operand, loaded from the same predicate
  `price_segments.load_unresolved_breaks` uses. Clause 2 is not folded into clause 3: their
  set equality (412 ↔ 412) is a present-corpus fact, not an invariant.
- **[ckpt-1 #14, #48] ONE query, not seven.** All seven fields come from a single
  `SELECT … FROM unnest(%(ids)s)` with lateral aggregates, so every field shares one
  snapshot by construction and there is no N+1. `since` bounds the verdict scan to the
  window the caller will assess.
- **[ckpt-1 #20] Omission is documented, not claimed as type-safety.** A missing key means
  "no bars evaluated"; `assess_window(None, …)` returns `unverified`, and
  `tests/test_price_window_verdict.py` asserts the consumer handles a missing key. The
  type does not force it and the spec no longer says it does.
- **[ckpt-1 #19, #21]** Closed vocabulary (the 8 reason codes in the table above), reasons
  returned sorted and deduplicated, every clause evaluated regardless of coverage, and
  `assess_window` raises `ValueError` on `window_end < window_start` or `bar_count < 1`.
  Bar checks are inclusive `[start, end]`; transition and break checks are `(start, end]`,
  which is `rule_w1`'s own convention and not re-derived here.

### 2. The session-aware weekend qualifier — the one design change from the contract

`rule_w2`'s nominal span uses `ClassParams.calendar_days_per_bar`, an **average** (`7/5`).
At `bar_count = 2` the gate is 2.8 days, below an ordinary Friday→Monday gap of 3, so raw
W2 fires on **7,008** of 12,262 windows — ~6,800 ordinary Mondays.

The contract's qualifier (`verify_3046_contract_decision.py:353-391`) strips the span's
Saturdays and Sundays and re-asks the rule in trading-day units, gated on
`calendar_days_per_bar != 1`. **[ckpt-1 #26, #27] That gate is wrong, and Codex verified
it with executed counterexamples.** The rule set declares `fx`, `commodity` and `index`
seven-day (for hole tolerance), so they get **no** exemption and an ordinary Friday→Monday
FX day-change fires W2. Measured on the full population:

| class | raw W2 | class-gated qualifier | **session-aware qualifier** |
| --- | ---: | ---: | ---: |
| fx | 28 | **28** | **0** |
| commodity | 14 | 0 | 0 |
| index | 14 | 0 | 0 |
| mena_equity | 43 | 0 | 0 |
| us_equity | 4,321 | 125 | 125 |
| eu_equity | 1,465 | 50 | 50 |
| uk_equity | 459 | 25 | 25 |
| asia_equity | 664 | 3 | 3 |
| **total** | **7,008** | 231 | **203** |

44% of the FX population would have been suppressed. The fix stops asking the class what
the venue does and asks **the instrument's own stored bars**: strip only those Saturdays
and Sundays inside the span that carry **no bar for this instrument**. Source rule: the
corpus itself — a day the venue traded has a bar, and `price_daily` is the record of what
the venue traded. It is per-window, so it is not a lifetime test and carries no
look-ahead (the direction residual 2's volume discriminator was killed on). Both sides
still move to trading-day units (`calendar_days_per_bar = 1`) or the weekend is deducted
twice — Codex checkpoint 2's correction on the original helper, preserved.

It also fixes the opposite error **[ckpt-1 #26]**: a `.24-7` weekend-tradable synthetic
typed `us_equity` **does** have weekend bars, so nothing is stripped and it gets no
exemption.

#### ⛔ **[ckpt-2 P1]** Presence alone is not enough — the qualifier needs TWO signals

Codex checkpoint 2 returned one finding and it was correct: per-window presence reads an
**absent** weekend bar as a **closed venue**. That is sound for an equity and false for a
seven-day product, where an absent Saturday is a **hole** — so the stripping deleted the
very gap W2 exists to catch and returned `ok`. It also caught the regression test proving
the rule with **impossible inputs** (weekend bars stored *and* `bar_count=2`, which cannot
co-occur — two weekend bars inside `[Fri, Mon]` make four stored bars).

The fix composes two signals, because they answer different questions:

| signal | question | wrong answer if used alone |
| --- | --- | --- |
| `trades_weekends` (habit) | is an **absent** weekend day a closure or a hole? | seven-day holes read `ok` |
| `weekend_bar_dates` (presence) | may a day the instrument **printed** on be deducted? | 6 live five-day suppressions lost |

⚠ **Both alternatives were measured, not argued.** Replacing presence with the habit was
implemented and run on the full population: it fixed the seven-day case and turned 6
Wednesday-to-Sunday windows (five-day venues, two stored bars, a Sunday print) from
suppressed back to `ok` — a larger defect than the one it bought. Composing them returns
the corpus to **215 / 210** with `fx`/`commodity`/`index`/`mena_equity` still at zero.

⚠ **Measured exposure of the ckpt-2 finding itself is 0 instruments today**, and that is
stated rather than inflated: a first pass counted 1, using a loose "any weekend bar in a
year" boolean that was itself rejected below. The clause is justified by the RULE, not the
count — residual 5's own lesson (`17638565`): *a zero count does not un-justify a
rule-derived clause*.

**The habit constant, fixed by construction.** No published formulation exists for "does
this venue treat Saturday as a session", so `WEEKEND_SESSION_RATIO` is built rather than
cited: a seven-day product trades both weekend days, so its weekend share of bars tends to
**2/7 = 0.2857**; a five-day venue tends to 0. The cut is **half** that signature —
**1/7 = 0.1429** — measured over each instrument's own last `WEEKEND_HABIT_DAYS = 365` days,
ending at **its own last bar** so a delisted name is judged on its final year.

⚠ The cut sits in a **measured empty band**. On 12,157 instruments with ≥20 bars in their
lookback: 11,802 at ratio ≤ 0.0238, 307 at ≥ 0.2256, and **nothing at all between 0.1250
and 0.2256**. `scripts/verify_3046_day_change_verdict.py --weekend-habit` prints the
histogram and **fails** if the band ever closes around the cut — the constant is defensible
only while the data keeps it isolated, and that is not a property of the code.

⚠ A boolean ("any weekend bar in a year") was measured and rejected: it admits FX names
carrying a handful of incidental Sunday bars, reintroducing the exact ckpt-1 false positive.

⚠ **[ckpt-1 #25] The qualifier does not make W2 a one-session test, and this is disclosed
rather than fixed.** `rule_w2`'s band is `> 2 ×` nominal, so on a two-bar window a single
missing session (Friday → Tuesday with Monday absent) is inside the band and is admitted.
That is the rule as written; `rule_w2` is in `INPUT_RULE_SETS` and is not this ticket's to
change.

⚠ **[ckpt-1 #29, #30] The published NYSE calendar is deliberately not used.**
`app/services/market_calendar.py` exists and would be strictly better — for US names. This
population is nine asset classes across at least five venues (ALNEV.PA, A4Y0.DE, TLY.L,
WIG1.DE, asia_equity), and applying a US holiday calendar to them would invent closures.
The weekend rule is venue-agnostic; the calendar is named as the refinement the moment a
per-venue calendar exists (#2312 is the missing session-hours column).

### 3. `app/services/market_data.load_day_changes` — the consumer

`DayChange` gains `verdict: str` and `reasons: tuple[str, ...]`; `change_abs` and
`change_pct` become `Decimal | None`, `None` on `quarantined`. `as_of` and `last_close` are
retained in every state — the close is a valid price in its own regime; it is the **ratio**
that is not a return. ⚠ **[ckpt-1 #17]** That is scoped to the level-break case
(`sql/247:83-88`, decision 10) and is **not** asserted for a T1 endpoint a bar rule has
condemned: when clause 1 fires, `last_close` is returned but the API carries
`bar_return_unusable`, and no claim is made about the level.

`bar_count` is the **stored** count between the two dates, never a rank and never a
calendar estimate — the contract's requirement, and the `by_rank[r]` off-by-one that made
W2 under-fire 6× is why it is spelled out. **[ckpt-1 #24]** The query returns the prior
date (it currently does not) so the window is explicit, and the original operand pair is
preserved: **[ckpt-1 #8]** nothing substitutes an older bar for a condemned one and
presents the result as the day change.

**[ckpt-1 #35]** Fewer than two positive closes stays what it is today — no `DayChange` row
at all, no verdict. Absent data is not an unverified window and is not conflated with one.

### 4. API + FE

**[ckpt-1 #34]** The detail carrier is `InstrumentPrice` (`app/api/instruments.py:359-364`,
consumed by `SummaryStrip`), not `InstrumentDetail`. Both it and `InstrumentListItem` gain
`day_change_verdict` and `day_change_reasons`.

**[ckpt-1 #36]** The two surfaces already differ on the null branch — the list drops its
date, `SummaryStrip` renders `— (—)` and keeps it. Left as is; the tooltip is added to the
element each surface already renders, and the reason codes are joined into one
human-readable sentence.

### 5. `scripts/verify_3046_day_change_verdict.py` — the paired A/B

**[ckpt-1 #43, #44, #45]** Full population, read-only, one `REPEATABLE READ READ ONLY`
transaction. The control is the **real old consumer** and the treatment the **real new
one** — both imported from `market_data`, run over the same snapshot; the control is never
simulated (`full-population-ab.md`). Population is recomputed, not fixed at 12,262, and
its boundary is printed. Distinct-entity metric (instruments). **[ckpt-1 #46]** It prints
the rejection denominator per reason code **and per asset class**, and enumerates the gain
side — every newly-suppressed instrument with its window, stored bar count, span and
reasons — so a legitimate value lost is visible rather than absorbed into a total.

**[ckpt-1 #47] Acceptance conditions, declared before the run:**

- every instrument whose verdict is `ok` has a byte-identical `change_pct` to the control;
- the newly-suppressed set equals the union of the clause populations and nothing else;
- no instrument gains a day change;
- `fx`, `commodity`, `index` and `mena_equity` contribute **zero** `horizon_stretched`
  rejections (the ckpt-1 regression);
- treatment latency on the list path is within 2× of control, measured, not asserted.

Clauses with a zero population today (1, 2's resolved half, `verdict_deferred`,
`coverage_missing`, `coverage_before_first_bar`) are exercised by **table-driven unit
tests** on `assess_window`, since the corpus cannot reach them.

## Identity and replay

**[ckpt-1 #32]** The assessment is **computed per request and stored nowhere** — no table,
no column, no ledger row — so there is no artefact to replay and nothing to version. The
moment a *stored* artefact or a strategy-path caller consumes this module, it needs an
`INPUT_RULE_SETS` entry; the module docstring says so. `price_quarantine.py` is untouched,
so `INPUT_RULE_SETS` does not rotate (#3031) and the 76M-bar backtest substrate is intact.

## What this does NOT ship

- **No other consumer.** 73 `FROM price_daily` occurrences across 34 files stay
  verdict-blind. `risk_metrics` in particular is untouched, and the withdrawal's per-RULE
  suppression question (T2 is 82% of the population and has no source rule yet) is not
  answered here — a two-bar window sidesteps it because there is one transition and the
  whole quantity dies with it.
- **No change to `app/services/price_quarantine.py`.**
- **No verdict freshness claim. [ckpt-1 #13, #16, #37]** Coverage bounds say what was
  evaluated, not that the evaluation still describes today's stored bars; a revised bar
  inside an unchanged interval is invisible here. B4 additionally needs a following bar,
  so the newest close cannot yet carry that verdict, and `most_recent_trading_day` permits
  a partially-formed same-day candle. All three are stated, none is fixed.

## Security

No security surface. Read-path data quality on public instrument data; the new loader
issues one read-only parameterised SELECT and writes nothing.

Refs #3046. Refs #3031. Refs #2261. Refs #2354.
