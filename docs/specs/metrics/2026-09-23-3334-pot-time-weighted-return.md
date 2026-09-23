# Strategy pot return % — time-weighted over `/strategies/wealth-history` (#3334 item 3)

## Problem

The Portfolio lens shows pot value and P&L in money only. `StrategyWealthHistoryResponse`
declares `total_return_available: false` because P&L ÷ current principal is wrong once the
ceiling (principal) has changed. The operator wants a return % "since start" and for "today".

## Source rule

GIPS 2020 for Firms (CFA Institute, `gipsstandards.org/.../2020_gips_standards_firms.pdf`):

- Glossary, *time-weighted return*: "reflects the change in value and negates the effects of
  external cash flows". *External cash flow*: "Capital (cash or investments) that enters or
  exits a portfolio."
- 2.A.23(c) value the portfolio on the date of all large cash flows; 2.B.1 (recommended) on the
  date of all external cash flows.
- 2.A.24(f) geometrically link sub-period returns; (e) treat external cash flows according to
  the firm's policy.
- GIPS Guidance Statement on Calculation Methodology (2011, `calculation_methodology_gs_2011.pdf`
  p.4-6): its Modified Dietz weights ASSUME end-of-day flows, and it states "other formulas for
  calculating approximate time-weighted rates of return are also permitted". So flow timing is a
  policy, not a mandated rule. It is fixed here **by construction** (below).

## Inputs (no new data)

`load_strategy_wealth_history` (`app/services/strategy_wealth.py:105`) already yields one point
per `portfolio_eod_snapshots` date with `pot_value = principal + total_pnl` (NULL when
`complete = false`) and `external_flow = principal_t − principal_{t−1}`. `principal` is the
latest `strategy_paper_pool_events.capital_limit` whose `changed_at` date ≤ the snapshot date.

**Invariant (synthetic funding).** The pot is a notional sleeve of one broker account. Its capital
is the assigned ceiling (#2844: exposure ≤ assigned capital), so a ceiling change is the pot's
external flow by definition of this series. The TWR inherits every attribution property of the
NAV (which positions count, `complete`, broker P&L basis incl. spread) and adds none.

## Rule

A sub-period is the interval between two consecutive valuations the walker uses. Every flow
reported on a point is treated as arriving at the **start** of the sub-period that ends at that
point (so `D = V_prev + F`). Why, by construction: (a) at inception `V_prev = 0`, and an end-of-day
treatment `(V_t − F_t)/V_prev` is undefined for the sub-period that starts the series; (b) valuations
are end-of-day snapshots and `changed_at` is intraday, so the capital is present at the valuation
it is credited to. ⚠ Limitation: a flow between snapshot dates (weekend, snapshot outage) is netted
and moved to the start of the next sub-period; GIPS 2.A.23(c) would require a valuation on the
date of a LARGE flow, which this series cannot provide. It is disclosed, not hidden.

Walker state: `base` = last complete valuation used `(V, date)`, `G` growth factor (starts 1),
`chain_started`, `broken`, `gap` = saw an incomplete point since `base`, `gap_flow` = saw a
non-zero `external_flow` on any point since `base` (occurrence, not net sum).

1. Points before the first complete point, and that point itself, return NULL. The first complete
   point becomes `base`; flows up to and including it are ignored (they are what it is valued at).
2. Point `t` incomplete → both returns NULL; set `gap`; OR its flow into `gap_flow`.
3. Point `t` complete. Let `F` = sum of `external_flow` over the points after `base` up to `t`,
   `D = V_base + F`.
   - `principal_t = 0` and `V_t = 0` and not `chain_started` → unfunded: returns NULL.
   - `gap` and (`gap_flow` or `F_t ≠ 0`) → `period_return` NULL and **break** (product policy,
     stricter than 2.A.23(c)'s large-flow rule: we cannot size "large", so any flow inside an
     unvalued span stops the chain).
   - `V_base ≤ 0` with `chain_started`, or `D ≤ 0`, or `V_t ≤ 0` → `period_return` NULL and
     **break** (a pot at or below zero has no base; −100%/sign-flipped returns are not published).
   - else `r = V_t / D − 1`, `period_return = r`, `period_start = base.date`. If not `broken`:
     `G *= 1 + r`, `cumulative_return = G − 1`; on the first step set `chain_started` and
     `return_since = base.date` if `V_base > 0`, else `t.date` (first funded valuation).
   - `base ← t`; clear `gap`, `gap_flow`.
4. `broken` nulls every later `cumulative_return` in the window (no resumption: a restarted chain
   needs a new "since" the operator did not ask for). `period_return` keeps being computed for
   later valid steps, so "last day" survives a break.

A bridged no-flow gap is exact (TWR is exact when no flow occurs inside the sub-period); its
`period_return` spans back to `period_start`.

## Contract change

- `StrategyWealthHistoryPoint` gains `period_return`, `period_start: date | None`,
  `cumulative_return` (Decimal fractions, not percent; unrounded — the UI rounds).
- `StrategyWealthHistoryResponse`: `total_return_available: bool`, `return_basis:
  Literal["time_weighted_start_of_period_flows"]`, `return_since: date | None`,
  `return_unavailable_reason: Literal["no_complete_point", "unfunded", "insufficient_history",
  "chain_broken"] | None` (`insufficient_history` = funded base but no step after it yet).
  All describe the **last complete point** — the same point the UI summary strip already reads
  (`potWealthSummary`, `frontend/src/lib/strategyAggregate.ts`).
  `StrategyPnlHistoryResponse` keeps `Literal[False]`.
- Window: the endpoint's `days` (default 365). The return is since `return_since`, which is
  inception only when inception is inside the window; a break that ages out of the window restores
  a later chain. The UI always labels the date, never "since start".

## UI

On the point `potWealthSummary` picks: "P&L since start" hint → `+x.xx% since <return_since>` when
`cumulative_return` is non-NULL; "Last day" hint → `+x.xx% since <period_start> close` when
`period_return` is non-NULL. Otherwise the existing money-only hints. No client derivation.

## Tests

Pure table tests on the walker: inception from 0 (return_since = funding date), mid-series top-up
(unchanged P&L ⇒ zero period return), withdrawal, bridged no-flow gap, gap with flow → break but a
later period_return survives, flow on the complete point closing a gap → break, V ≤ 0 → break,
leading unfunded points, leading incomplete points, empty and single-point series.

Security: read-only field additions on an existing session-guarded endpoint; no new auth surface.
