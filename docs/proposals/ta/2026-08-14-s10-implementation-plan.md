# S-10 relative-strength leader — implementation plan

Parent spec: `2026-08-14-strategy-set-s5-s10.md` §S-10. Refs #2437.

## Gate evidence (measured first, per the spec's own order)

`scripts/verify_2437_s10_turnover.py` on the full validated universe (6,413 of
6,774 loaded), production `MarketRegimeProvider`, NMV one-sided monthly
turnover, 42 active months:

| exit-cadence reading | steady-state mean | verdict |
| --- | ---: | --- |
| both exit legs at rebalance only | **36.2%/month** | inside the ~50% bar |
| 50-SMA exit checked daily | **83.7%/month** | disqualified |

**Consequence pinned into the design: S-10's exit legs are evaluated at
rebalance dates ONLY.** The daily reading is not a viable variant; the cadence
is a module constant hashed into the identity.

## The rule (spec-literal)

- Setup: regime ∈ {bull_quiet}; universe ranked by 63-bar return.
- Signal: enter the top decile that ALSO closes above its own 50-SMA; rebalance
  monthly. Reading pinned: the decile is cut on the FULL ranking, then the SMA
  condition filters the winners (no backfill) — the panel denominator is the
  whole rankable cross-section. The turnover measurement used exactly this.
- Exit: leaves the top three deciles, or closes below 50-SMA — at rebalance.

## Contract gaps and how each is closed

S-2 is the only cross-sectional precedent and it is one-legged (its exit is the
entry's complement; S-10's is not — band 3×N//10 ⊋ decile N//10, plus an SMA
condition selection cannot see).

1. **`CrossSectionalMember` gains two optional index sets**
   (`strategy_registry.py`), both defaulting to `None` = today's behaviour:
   - `admissible_indices` — participating bars allowed to fire when selected.
     Selected-but-inadmissible → `not_fired`; the score STILL enters the panel
     (denominator preserved — this is what makes reading A expressible).
     Entry leg: bars with `close > sma50` and regime `bull_quiet`.
   - `mandatory_indices` — participating bars that fire regardless of
     selection. Exit leg: bars with `close < sma50`.
   - Resolution rule everywhere a staged bar is decided:
     `fired iff (selected AND admissible) OR mandatory`.
   - `StagedMember` carries these as **date-keyed** sets so
     `segmented_member`'s per-segment merge needs no index remapping (the
     silent reslice trap from the 08-14 S-8 session).
   - Three resolvers honour it: `evaluate_cross_sectional` (registry),
     `_resolve_cross_section` (scan), and the backtest's cross-section pass.

2. **Cross-sectional exit leg** (`strategy_manifest.py`): `StrategyEntry`
   gains `exit_member: MemberStager | None` + `exit_select:
   CrossSectionalSelect | None` — cross_sectional only, both-or-neither,
   `"exit" ∈ signal_kinds` iff present, enforced in `__post_init__`.
   `stage_cross_sectional_member(kind="exit")` already exists; the position
   layer already pairs C1 `signal_pair` exits — no position-layer change,
   S-7's precedent.

3. **Regime for cross-sectional members**: `MemberStager` protocol gains
   `regime: RegimeSeries`, uniformly — the same absorb-in-adapters move
   `PerSeriesSignals` made for S-5..S-9. S-2's adapter ignores it.
   `segmented_member` slices it per segment via `RegimeSeries.segment` (as
   `segmented_signals` already does) and gains a `leg` selector for the exit
   member. S-10's ENTRY member declares the regime as an input
   (`missing_market_context` refusal on unclassifiable rebalance bars); the
   EXIT member deliberately does NOT — a missing benchmark session must never
   refuse the exit verdict for an open position (S-7 precedent).

4. **Scan** (`strategy_signal_scan.py`): `panel_dates` becomes per-plan (S-10's
   calendar differs from S-2's — below); `pending`/`scores` become per-leg;
   `_resolve_cross_section` runs per leg with the leg's select and kind, and
   its hardcoded `kind="entry"` refusals take the leg's kind.

5. **Backtest** (`backtest_run.py`): the cross-section pass runs per leg for a
   two-legged entry; exit verdicts join the same fill/outcome path S-1/S-3
   exits use. `ExitRegime`: `signal_pair=True` only (no levels, no max-hold,
   no calendar close — the exit leg IS the calendar, firing only on rebalance
   bars).

## The S-10 module (`app/services/strategies/s10_relative_strength_leader.py`)

Frozen constants, S-2's shape, all hashed via params + source hash:

- `LOOKBACK_BARS=63`, `ENTRY_DECILE=10`, `EXIT_BAND_DECILES=3`, `SMA_BARS=50`.
- `MIN_CLOSE=1.0` — S-2's §9 Q3 rule, same argument verbatim (a ranked
  strategy's top decile is where tick-quantised penny names concentrate).
  Entry eligibility only; **no price floor on the exit leg** — a held name
  that fell under $1 must still be exitable.
- `MIN_CROSS_SECTION=1000` — by construction, from the measurement's
  `THIN_PANEL`: a decile BAND retention rule evaluated against a sliver panel
  would liquidate a book built on thousands of names. (S-2's 10 protects a
  one-shot decile cut; a retention band needs the panel to resemble the one
  the book was built from. Raised from a first draft's 100 at Codex ckpt-1 —
  see the resolutions section below.)
- `s10_rebalance_dates(calendar)` — weekday-filtered (Sat/Sun dropped) union
  calendar, then S-2's first-bar-of-new-month rule. By construction:
  `price_daily` carries weekend rows for ~11 instruments, and the unfiltered
  rule hands entire months' rebalances to those dates (measured; noted on
  #2437 as an S-2 production quirk too).
- Entry member: score = `close(t)/close(t-63) - 1` (positive-close guards,
  S-2's); inputs = close + score + regime; decision = rebalance bars with
  `close >= MIN_CLOSE`; admissible = `close > sma50` AND `bull_quiet`.
- Entry select: top `N // 10`, S-2's frozen ordering (score desc, id asc).
- Exit member: same score; inputs = close + score (NO regime); decision =
  every rebalance bar; mandatory = `close < sma50`.
- Exit select: the complement — `ordered[3 * N // 10 :]` (leaves the band).

## Tests

- Registry: admissible/mandatory unit tests (selected-but-inadmissible →
  not_fired with score still ranking others; mandatory fires unselected;
  None = unchanged — S-2 regression pinned).
- S-10 module: score windows/refusals, both selects' cuts and tie-breaks,
  regime gating incl. missing_market_context on the entry leg only, SMA
  strictness (equality holds, never enters, never exits).
- Manifest: registration invariants (existing sweeps) + two-leg validation.
- Scan: two-leg cross-sectional staging on a small fixture.

## Codex ckpt-1 resolutions (2026-08-14)

Accepted and folded into the design above:

- **SMA is a DECLARED input on both legs** — a masked/short window refuses the
  bar (`not_evaluable`), never silently passes.
- **The exit leg is evaluable only when the bar is rankable** (score + SMA +
  close all present) — stated, not hidden: an unrankable rebalance bar for a
  held name refuses visibly and the position carries to the next evaluable
  rebalance. The below-SMA exit does not logically need the rank, but a
  second evaluability regime for one condition would put two contracts inside
  one leg; the refusal counts are censused.
- **Precedence pinned and tested**: `no_fill_bar` → unevaluable input →
  non-decision `not_fired` → `thin_cross_section` → mandatory → selected ∧
  admissible. Mandatory does NOT beat a refusal or a thin panel.
- **Regime mechanism**: `StrategyInput(series=regime, reason=
  "missing_market_context")` — S-7's exact pattern (`s7_trend_pullback.py`
  line ~240); `EvaluableSeries` is a Protocol precisely so `RegimeSeries`
  declares (#2437). No new representation needed.
- **`exit_leg` is one optional object** (`member`, `select`,
  `min_participants`), not parallel fields.
- **`min_participants = 1000` on both legs** — encodes panel resemblance; the
  observed junk dates (11-row Sundays, a 243-row corpus-hole Friday) all
  refuse, every observed real panel (2,084–5,747) passes.
- **Entry "rankable" includes the $1 floor** (S-2 §9 Q3, adjusted-price floor
  caveat included by reference); the exit panel is floor-free. Both
  denominators are therefore different and that is a stated choice, now
  MEASURED: v4 of the gate ran the exact shipped semantics — floored entry
  panel, floor-free exit panel, 1000 thin floor — steady-state mean
  **36.5%/month** monthly-pinned (daily 83.0%). Verdict unchanged.
- **Semantics pinned**: score(t) = `close(t)/close(t-63) − 1` (63 intervals,
  64 observations — S-2's interval convention); SMA = trailing 50 closes
  including t, all 50 present or the bar refuses; comparisons are strict
  float inequalities, no tolerance (a tolerance is an extra parameter).
- **Implementation order**: registry oracle + tests first, then module, then
  manifest, then scan/backtest, with a three-resolver equivalence test
  (registry `evaluate_cross_sectional` vs scan `_resolve_cross_section` vs
  the backtest resolution) on one shared fixture.
- **Census**: per-leg verdict/reason distribution, thin-date counts, and a
  mandatory-parity check (every below-SMA decision bar that was evaluable
  fired an exit).

Rebutted:

- *"Exit cadence is parameter selection on measured data"* — the cadence is
  selected by the spec's OWN pre-backtest disqualifier on a COST property,
  not by a return outcome; Novy-Marx/Velikov's screen exists to be applied
  exactly this way. It is frozen in the params and hashed.
- *"Identity too weak — registry semantics not hashed"* —
  `StrategyIdentity.version` includes `"registry": _module_hash()`
  (`strategy_registry.py` line ~266); registry changes move every version.
  Consequence stated instead: this PR bumps ALL strategy versions (accepted
  over-invalidation, precedent #2719) and the next scan rewrites the fleet.
- *"42 months is a sample"* — it is the entire regime-classifiable population
  on `price_daily` (SPY starts 2022-05-10; the 200-SMA + 126-bar warm-up
  reaches 2023-02-27). The 361 skipped instruments hold fewer than 64 bars
  and can never rank; both counts are reported, not hidden.
- *"Walk-forward / promotion evidence absent"* — queue items 6–8 (#2437 R4
  order, #2720, #2721), deliberately not this PR. Nothing here claims
  promotability; S-10 registers `harness_validation` like all nine.

## Definition-of-done evidence (PR)

Full-population census (per-leg verdict/reason distribution + fired counts +
distinct instruments, per year), the turnover gate table above (v4 = shipped
semantics), lint/typecheck/tests, Codex ckpt-2 (behavioural rung — data
semantics throughout).
