# Phase 2 — historical indicator recomputation from raw OHLCV

Status: **proposal, unshipped.** Phase 2 of the TA strategy platform
(`docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md` §5), gated on
0a ✅ and named "unblocked, next" there. The design doc gives this phase one
line; everything below is new.

Issue: TBD (minted from §9). Refs #2240, #2260, #2279, #2282.

---

## 1. What this phase is, and what it is NOT

**Is:** a **streaming, strictly causal** recompute of indicator *series* over
stored OHLCV, fast enough that phase 3's signal ledger and phase 5's backtester
can call it per strategy evaluation.

**Is NOT:** an indicator store. ⚠ **Adding indicator columns or an indicator
table would reverse two settled decisions**, and neither is reopened here:

- **sql/249** declines indicator columns on `research_price_daily` in terms:
  *"No indicator columns by design (vectorbt computes them over the series)"*,
  with the file header adding that persisting an indicator history is *"storage
  and drift for no read"*.
- **#2279's spec** (`docs/proposals/ta/2026-08-05-price-structure-primitives.md`
  §6) declines persisted swings/levels on measured cost — five primitives
  recompute over the full 25.8M-bar corpus in **89.0 s** — and cites sql/249 as
  its precedent.

What #2279 kept instead is inherited wholesale: `RULE_SET_VERSION` = a stable
rule-set id plus a SHA-256 of the module source, returned on every result, so a
stored *signal* in phase 3 can be invalidated against the rules that produced it.

## 2. Source rule

No external regulator applies — this is a quant formulation, which
`.claude/CLAUDE.md` binds anyway ("⚠ This binds QUANT/TA formulations too").
So the governing rules are the published indicator definitions, and each one
must cite its own:

| indicator | published rule | note |
| --- | --- | --- |
| RSI | Wilder, *New Concepts in Technical Trading Systems* — seed = simple average of the first `period` deltas, then Wilder smoothing | already implemented causally in `technical_analysis.py::rsi`; invariant pinned by #2308 |
| EMA / MACD | Appel — EMA seeded from SMA(`period`), MACD = EMA(12) − EMA(26), signal = EMA(9) of the line | `technical_analysis.py::ema`, `macd` |
| SMA | arithmetic mean over `period` | trivial |
| ATR | Wilder — TR = max(H−L, \|H−C_prev\|, \|L−C_prev\|), then Wilder smoothing | `technical_analysis.py::atr` |
| Bollinger | Bollinger, *Bollinger on Bollinger Bands* — SMA(20) ± 2σ (population) | `technical_analysis.py::bollinger_bands`; ⚠ see #2279's precedent on BandWidth — the **Squeeze/Bulge is a six-month (126-bar) extreme**, not a percentile cut |
| Stochastic | Lane — %K over the `period` high/low range, %D = SMA(3) of %K | `technical_analysis.py::stochastic` |

⚠ **No new indicator is introduced in this phase.** Phase 2 changes the *shape*
of the computation (single latest value → causal series), not the formulas. A new
indicator is a separate decision with its own source-rule citation.

## 3. The problem, measured

`technical_analysis.py`'s functions each return **one value for the latest bar**
and are O(n) per call. Building a historical series with them means calling on
expanding prefixes — O(n²).

Measured on the five deepest real series in the corpus, 2026-08-05:

| symbol | bars | naive O(n²) | streaming O(n) | ratio | values compared | mismatches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| XRX | 16,236 | 28.24 s | 0.0026 s | 10,678× | 16,222 | **0** |
| XOM | 16,236 | 28.05 s | 0.0027 s | 10,278× | 16,222 | **0** |
| HON | 16,235 | 28.24 s | 0.0028 s | 9,931× | 16,221 | **0** |
| GE | 16,235 | 28.81 s | 0.0028 s | 10,425× | 16,221 | **0** |
| GD | 16,235 | 28.69 s | 0.0026 s | 10,995× | 16,221 | **0** |

Corpus-wide scaling, exact rather than extrapolated:

```sql
select count(*), sum(bar_count::numeric * bar_count::numeric), sum(bar_count)
  from research_price_series where bar_count is not null;
-- 7,693 series | sum(n^2) = 193,137,130,320 | sum(n) = 25,818,944
-- quadratic work factor = 7,480x
```

At the measured 16,236-bar rate that is **~5.7 hours** for one full-corpus
single-indicator recompute, against **~4 seconds** streaming. Phase 5 evaluates
many indicators across many strategies, so the quadratic is not a tuning
question — it decides whether a backtester can exist.

**Zero mismatches across 81,107 compared values** establishes that the streaming
form is the same indicator, not an approximation of it.

## 4. Why this is the phase that matters for correctness, not just speed

#2260 recorded RSI<30 → **76.8%** 20-day hit rate. Measured 2026-08-05 with a
causal recompute on both full corpora: **51.846%** on `price_daily`
(n=311,332) and **50.371%** on the research corpus (n=1,255,230), against
unconditional baselines of 50.585% / 50.487%.

⚠ Attribution is **by elimination, not by finding the bug** — the original
computation is not preserved. Candidates 3 and 4 were measured and neither
carries it (de-overlapping *raises* the rate ~2.2 pts; the quarantine residual
is ≤0.005 pts). A Codex pass correctly refuted the stronger claim that
survivorship was eliminated. Full write-up on #2260.

The lesson phase 2 must encode: **a non-causal recompute manufactured a
27-point phantom edge that survived every arithmetic check for months.** So
causality is not a code-review preference here, it is the phase's primary
invariant, and #2308 already pins it for `rsi()`.

## 5. Contract

A pure module, `app/services/indicator_series.py`, mirroring the shape
`price_structure.py` and `price_quarantine.py` already establish:

⚠ **[C1] Bare lists were the first draft and are wrong.** §1 says
`RULE_SET_VERSION` is "returned on every result", which a `list[float | None]`
cannot carry — phase 3 would have no mechanical way to attach provenance, and
`price_structure`'s **required, defaultless `universe` field** (that spec §6:
*"the field has no default, so a caller cannot construct a result without
stating the universe"*) would be dropped by the one phase that claims to mirror
it. That field is the survivor-only labelling contract (#2288). Every result is
therefore an object:

```python
RULE_SET_ID = "indicator-series-v1"
RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"   # sha256 of module source

@dataclass(frozen=True)
class IndicatorSeries:
    values: list[float | None]          # len == len(input bars), always
    universe: Universe                  # REQUIRED, no default — #2288 / price_structure §6
    rule_set_version: str
    not_evaluable_indices: tuple[int, ...]   # distinct from warm-up None; decision 5

def rsi_series(closes: Sequence[Decimal], *, universe: Universe, period: int = 14) -> IndicatorSeries: ...
# sma / ema / macd / atr / bollinger / stochastic — same shape
```

`not_evaluable_indices` mirrors `price_structure`'s: design-doc decision 5 says a
rule returns `fired` / `not_fired` / `not_evaluable`, **never a bare boolean**,
because "could not evaluate" and "did not fire" collapsing is the vacuous-truth
class already in the prevention log. A `None` at index `i` must therefore be
distinguishable as *warm-up* versus *unevaluable input*.

Invariants, all testable:

1. **Causality.** Element `i` depends only on inputs `0..i`. Asserted by
   cross-validating against `technical_analysis`'s prefix form at every index —
   the pattern #2308 established.
   ⚠ **[C1] This proves no-future-bars relative to the CURRENT implementation,
   not that the current implementation matches the published rule.** The two are
   different claims and only the first is tested here; §2's citations are what
   carry the second, and they are currently named rather than page-cited (§10).
2. **Equivalence.** `x_series(bars)[-1] == x(bars)` for every indicator.
   ⚠ **[C1] §3's 81,107 comparisons are a SAMPLE — five series, one indicator —
   and this spec has no business resting its central technical claim on one.**
   Acceptance §8.2 therefore requires the equivalence sweep over the **full
   7,693-series corpus for all seven indicators**, which the measured streaming
   rate makes affordable (~4 s per indicator). Shares the weakness of invariant
   1: it can lock in a wrong current formula, and only §2 guards that.
3. **Warm-up is `None`, never a value.** A seeded-but-unwarmed indicator is the
   look-ahead in miniature. ⚠ Length equals the input's, so an index into the
   series is an index into the bars — an offset series is how off-by-one enters
   a backtest.
4. **Nothing is persisted.** No table, no column, no cache (§1).
5. **`RULE_SET_VERSION` derives from module source**, not from a constants
   tuple — following `price_quarantine.py`, which accepts over-invalidation
   deliberately so a stored verdict is *visibly stale rather than silently
   mixed*.
6. **[C1] Input ordering is asserted, not assumed.** Bars must arrive
   oldest-first, and the function must reject input that is not strictly
   ascending by date. ⚠ Newest-first input passes every value-equality fixture
   while inverting time in production — a look-ahead that no amount of causality
   testing on a correctly-ordered fixture can catch.
7. **[C1] Duplicate and missing bar dates are rejected, not smoothed.** A
   duplicate date is a corpus defect (`research_price_series` already counts
   them); a calendar gap is normal and must NOT be interpolated, because an
   interpolated bar is a fabricated observation.
8. **[C1] Null OHLC is `not_evaluable`, never coerced.** ATR and stochastic need
   complete high/low/close. A NULL that evaluates falsey is decision 5's exact
   failure mode.
9. **[C1] Quarantine and adjustment basis are the CALLER's gate, and the
   contract says so.** These functions compute over the bars handed to them and
   have no DB access. ⚠ That means a caller can feed them quarantined or
   unadjusted bars — so `universe` alone is insufficient provenance, and phase 3
   must record the eligibility predicate it applied. Stated here rather than
   silently assumed, because `price_structure._atr_at` currently fails CLOSED on
   masked bars and this module cannot.
10. **[C1] Parameter validity is checked.** `period > 0`, `fast < slow`,
    `signal_period > 0`, `num_std >= 0`, `d_period > 0`. Cheap, and an inverted
    MACD pair silently produces a sign-flipped histogram.

## 6. Rolling ATR — a costed finding this phase should absorb, not rediscover

#2279 §6 flagged, and declined to fix:

> level interaction / break-and-retest cost **252.3 s and 253.1 s per level** …
> The cause is identified: `_atr_at` recomputes a 15-bar Wilder window at every
> bar, making the scan O(bars × period) where a rolling ATR would make it
> O(bars). Left unoptimised here deliberately … **Phase 5 should budget for it
> or fix it; it should not discover it.**

`atr_series` is the rolling ATR that removes it. So phase 2 should ship
`atr_series` **and** rewire `price_structure._atr_at` to consume it — turning a
flagged phase-5 cost into a phase-2 deliverable.

⚠⚠ **[C1] This is NOT a pure cost change, and the first draft said it was.**
`_atr_at` currently **fails closed** on a masked high/low/close, and
`atr_series(Sequence[OHLCVRow])` has no way to represent a mask. Rewiring
naively would silently compute through bars that `price_structure` deliberately
refuses. So the rewire is conditional on proving exact equivalence **under
masks and at warm-up boundaries**, not merely on matching values in the clean
case — and if that cannot be proved, 2b ships the rolling form behind
`_atr_at`'s existing fail-closed guard rather than replacing it.

⚠ The rewire changes `price_structure.py`'s source, hence its
`RULE_SET_VERSION`. **[C1]** Blast radius is zero for *persisted phase-3
signals* (there are none yet) — not zero absolutely: golden-file tests and any
generated artefact keyed on that version also move. Do it before phase 3 stores
signals, or accept invalidating them later.

## 7. What is NOT in scope

- **Any new indicator.** §2.
- **Persistence of indicator VALUES.** §1. ⚠ **[C1]** "persistence of any kind"
  was the first draft and is too broad — it would forbid phase 3's signal
  ledger, its version vectors, and the benchmark receipts §8.5 requires. The
  decision is narrow: no indicator value is stored, anywhere, in any form,
  including a cache.
- **Adopting vectorbt.** Memory records 1.1.0 as *verified installable* on
  py3.14; it is **not installed** (`ModuleNotFoundError` for `vectorbt`,
  `talib`, `pandas_ta`, checked 2026-08-05). sql/249's comment assumes vectorbt
  computes indicators, and that assumption is currently unmet — but adding a
  dependency to produce a series that streams in 4 seconds is not justified
  ("do not add libraries casually"). ⚠ Phase 5 is where vectorbt earns its place
  (portfolio simulation, not indicator maths); revisit there, not here.
- **The overlapping-window estimand.** #2260 showed de-overlapping moves the
  number ~2.2 pts, so it is a real phase-5 decision about what a "win rate"
  counts. Not an indicator concern.

## 8. Acceptance

1. Every `*_series` function passes the causality cross-validation at **every**
   prefix index. ⚠ **[C1]** "real corpus series" was ambiguous enough to permit
   another sample-based safety claim: the sweep runs on a **stratified** set —
   the deepest series, the shortest series that clear warm-up, series with
   calendar gaps, series with NULL OHLC, and quarantined series — not just the
   long clean ones §3 benchmarked.
2. `x_series(bars)[-1] == x(bars)` for all seven indicators, to 1e-9, over the
   **FULL 7,693-series corpus** — not the five of §3. ⚠ At ~4 s per indicator
   this is affordable, so a sample here would be a choice, not a constraint.
3. Warm-up positions are `None` and `len(series) == len(bars)` for all seven.
4. ⚠ **Revert-probe each invariant test** by injecting the defect it guards
   (full-series seed; dropped warm-up; off-by-one shift) and confirming the test
   fails. #2308 needed **three** fixtures before one discriminated — a fixture
   too neutral to express the defect passes against broken code.
5. Full-corpus recompute of **all seven indicators together** completes in
   **< 60 s** (measured streaming rate implies ~4 s each; 60 s is headroom, not
   a target). ⚠ **[C1]** Per-indicator timing was the first draft and does not
   answer phase 5's actual question, which is what a multi-indicator strategy
   pass costs when several strategies each recompute the same series.
6. `price_structure`'s level-interaction and break-and-retest scans re-measured
   after the `_atr_at` rewire, and the new per-level figure recorded against
   #2279's 252.3 s / 253.1 s.
7. No new table, column or dependency in the diff. Assert by inspection of
   `sql/` and `pyproject.toml` — both unchanged.

   ⚠ **SUPERSEDED IN PART by #2311, 2026-08-06 — the dependency half only.**
   Acceptance 5 failed as written: the shipped pure-Python form measured
   **83.3 s** against this < 60 s bar, and #2311 declined to move the bar. The
   fix is `numpy` on the two window indicators, which makes `pyproject.toml`
   change — so this criterion and criterion 5 could not both be met.
   Criterion 5 wins, for two reasons stated on #2311: it is the one phase 5
   depends on, and `numpy` was **already a direct import** in
   `app/services/risk_metrics.py` resolving through `pandas`, so the diff
   declares an existing dependency rather than adding a new one.
   ⚠ **The no-new-table / no-new-column half stands unchanged** — nothing here
   is persisted, which is settled twice over (§1, sql/249, #2279 §6).

## 9. Tickets this mints

Deliberately small and sequential; each is independently reviewable.

| # | scope | depends on |
| --- | --- | --- |
| 2a | `indicator_series.py` — the seven streaming functions, `IndicatorSeries` result object, `RULE_SET_VERSION`, all ten invariants with revert-probes | — |
| 2c | full-corpus timing + equivalence harness; acceptance 2 and 5 recorded | 2a |
| 2b | `_atr_at` rewire **conditional on the masked-bar equivalence proof** (§6); re-measure the two per-level scans against #2279's 252.3 s / 253.1 s | 2a, **2c** |

⚠ **[C1] 2b depends on 2c, not just 2a** — 2b's re-measurement needs the
benchmark harness 2c owns, and the first draft had them as siblings.

⚠ **[C1] Phase 3 is gated on 2b, not 2a.** §6 argues the `_atr_at` rewire must
land before phase 3 stores signals or those signals are invalidated by a later
`RULE_SET_VERSION` change — and the first draft then gated phase 3 on 2a alone,
contradicting its own §6.

Phase 3 (strategy registry + signal ledger) consumes 2a's contract and is where
the survivor-only label (#2288) and signal versioning attach.

## 10. Outstanding from Codex checkpoint 1 — NOT yet fixed

Recorded rather than lost. None changes the contract; all are real.

- **§2's source rules are named, not page-cited.** Several are data-treatment
  choices with competing conventions and need the exact citation: EMA seeded
  from SMA vs from the first close; Bollinger **population** vs sample σ; ATR
  requiring `period + 1` bars and excluding bar 0; MACD signal-line warm-up and
  first valid index.
- **Flat-series conventions are undocumented.** `rsi()` returns 50.0 when both
  average gain and loss are zero; `stochastic()` returns 50 when high == low.
  Both are defensible local conventions and neither is currently cited or
  labelled as local.
- **Return shape for multi-value indicators.** `list[tuple | None]` for MACD /
  Bollinger / stochastic may not be what phase 5 wants (aligned named arrays are
  the vectorbt-shaped alternative). The adapter boundary is unassigned.
- **Look-ahead is broader than indicator causality.** The parent
  (`strategy-catalogue-and-backtest-validity.md` §"look-ahead") also requires
  fill timing (signal on close(t) fills at open(t+1)) and as-of eligibility.
  Phase 2's invariants cover the indicator only; phases 3-5 own the rest, and
  this spec should not be read as closing look-ahead generally.
- **§4's hit-rate figures** are quoted as motivation. They are overlapping-window
  counts without the parent's block bootstrap, and must not be read as endorsed
  estimates.

⚠ **Phase 5 remains gated on the survivorship purchase** for *validation*, which
is a different gate from #2260's anomaly. Nothing in this phase changes that.
