# Backtest regime benchmark from the research corpus — the chained SPY series

Narrows the gap `backtest_run.py::_signals_for` documents: the regime comes from
`price_daily` (first SPY bar 2022-05-10) while the backtest axis reaches 1962, so every
regime-gated strategy (S-5…S-10) is `not_evaluable` before ~2023-02 and §0's "per-year
and per-regime, never pooled-only" blocks cannot be produced over more than ~3.4 years.
The docstring names the fix: *"a benchmark series in the RESEARCH corpus — the same
source the bars come from."* This document is that piece of work. Refs #2437.

⚠ **Narrows, not closes**: SPY's inception is 1993-01-22, so 1962–1992 stays
`not_evaluable` under any SPY-based rule. Classifying pre-1993 conditions would need a
different benchmark (the S&P 500 index itself) and is out of scope — the regime rule
(§1 of the S-5…S-10 spec) is defined on SPY.

## Source rule

There is no published formulation for splicing two vendor series into one benchmark.
Per the repo rule for that case, the construction is fixed **by construction** and
frozen in a version id — stated, not cited.

**The chain (`spy_chain_v1`), two segments, one seam, all constants frozen:**

| segment | series (pinned by `(vendor, vendor_symbol)`) | span used |
| --- | --- | --- |
| primary | `('etoro/etoro-comparators-2026-07-08-v1', 'SPY')` | every bar **on or after the frozen seam** (2022-05-10 → 2026-07-08 today) |
| fallback | `('icyDenev/Intrader', 'SPY')` | every bar **strictly before the seam** (1993-01-29 → 2022-05-09 today) |

- **The seam is a frozen constant, `date(2022, 5, 10)`** — the primary's first bar at
  freeze time — not derived from live coverage. A corpus refresh that extends either
  series cannot silently move the seam or redefine the chain (the loader *asserts* the
  primary has a bar exactly on the seam and refuses otherwise).
- One seam only. Per-date interleaving is rejected: a hole in one vendor would flip
  vendors mid-window and create unbounded micro-seams. A hole in the primary after the
  seam is **left as a hole**, never backfilled from the fallback.
- The primary is the eToro comparator because it is **numerically identical to the live
  scan's benchmark**: `close IS DISTINCT FROM` returns **0 rows** across all 1,018
  dates shared with `price_daily` SPY (full population, exact comparison). The
  backtest's recent-window regime therefore *is* the live regime.
- The fallback (`unadjusted`, `yahoo_derivative`) and the primary (`split_adjusted`,
  `etoro`) disagree by ~0.1–0.3% on their 585-date overlap (max $1.76, avg $0.66, full
  population — different vendor close marks; SPY has no split history, which the
  overlap agreement itself evidences). The seam step is therefore real and
  **declared**: SMA-200 / BandWidth windows crossing 2022-05-10 mix the two marks. No
  rescaling — a splice factor would be an invented transformation of source data. The
  loader logs the seam step; no runtime tolerance gate, because any threshold would be
  an invented constant — the one-time full-overlap measurement above is the evidence.
- **Quarantine treatment, declared:** the chain reads `research_price_daily.close`
  without consulting `research_bar_quarantine`. The regime is market context, not a
  return computation; the quarantine's `return_usable` / `range_usable` verdicts do not
  define "close usable". Verified on the full population: the only flagged rows on
  either pinned series are 5 archive-tail `provisional` marks on the fallback
  (2024-09-23→27, empty rule list, both usability flags true) — all **after** the seam
  and therefore never read by the chain.

### Runtime invariants (the loader refuses rather than degrades)

1. Each pin resolves to exactly one series — `(vendor, vendor_symbol)` is DB-unique,
   so this is belt-and-braces, kept because the refusal message is better than a
   downstream shape error.
2. Resolved metadata matches the frozen expectation: primary
   `adjustment_basis = 'split_adjusted'`, fallback `= 'unadjusted'` — provenance drift
   under an unchanged vendor name is refused.
3. Primary has a bar exactly on the seam; fallback is non-empty and its last bar is
   within **7 calendar days** before the seam (7 = the max intra-series gap measured on
   both segments, i.e. a holiday week; a larger gap means a segment eroded).
4. Chained dates strictly increasing and unique (guaranteed by construction; asserted
   anyway).
5. Every close finite and > 0.
6. Both segment reads happen on one connection in one transaction snapshot.

All refusals raise `BenchmarkUnavailableError` — same contract as `load`: a scan that
silently produced an all-`None` regime would render an outage as a quiet market.

## Identity treatment

The pure classifier (`market_regime`, `REGIME_RULE_VERSION`) is untouched. What changes
is **which closes feed it in the backtest**, and that changes verdicts — pre-2023 bars
flip from `not_evaluable` to real verdicts — so it must move identity, or old and new
result rows silently mix under one `result_version` (the exact defect
`INPUT_RULE_SETS` / #2333 exists for).

- `market_regime_provider` gains a declared frozen constant
  `RULE_SET_VERSION = "benchmark-source-v1:live=price_daily_spy;research=spy_chain_v1"`
  naming **both** source rules. A declared string, not a module hash — the provider's
  own header freezes the design that fetch *mechanics* stay outside the hash; the
  source *semantics* now join it. Changing either source rule bumps the string. The
  chain's frozen constants (pins, seam, basis expectations) live beside it; the string
  and the constants are one frozen block, and tests pin both so they cannot drift
  apart silently.
- `strategy_registry.INPUT_RULE_SETS` gains
  `"market_regime_provider": RULE_SET_VERSION`. Consequences, both accepted:
  **every strategy_version moves**, including S-1…S-4 which never read the regime —
  that is the registry's own documented trade ("a strategy reading none of these still
  moves with them... visibly stale instead of silently mixed"), taken knowingly three
  times before, and the fleet is already scheduled to rewrite on the next scan after
  S-10's registry move; and **a live-source bump also invalidates backtest rows** —
  same over-invalidation direction, same acceptance.
- No `ResultIdentity` schema change: `strategy_version` is already an identity member,
  so stored backtest rows cannot collide with re-runs. The chain version is recorded
  indirectly, exactly as every indicator rule set already is.
- No content checksum of the bar population is added: the corpus is frozen by settled
  policy, the primary carries `comparator_snapshot_id`, and the seam + metadata
  asserts catch the mutations that would matter to the chain.

## Mechanics

- `MarketRegimeProvider.load_research(conn)` — new constructor. The date/close merge is
  a **pure function** (`_chain_closes`) so the construction is unit-testable without a
  database; `load_research` is fetch + invariants + the same classification path as
  `load` (shared helper, so the two constructors cannot classify differently).
- `backtest_run.py`'s two arm passes switch `MarketRegimeProvider.load(conn)` →
  `load_research(conn)`. The live scan keeps `load` (price_daily) unchanged.
- The `_signals_for` ⚠⚠ docstring block recording the 2022 ceiling is rewritten to
  record the chain and the 1993 bound.

## Tests

- Update `test_strategy_registry.py`'s exact-mapping pin (line ~247) to the two-entry
  mapping; keep the tamper-proof and completeness walks green.
- New unit tests on the pure chain: fallback trimmed strictly before the seam; primary
  hole after the seam stays a hole with a fallback bar available on that date; refusals
  for missing seam bar / empty fallback / fallback short of the seam window /
  non-finite close / duplicate date.
- A constants pin: the frozen seam, pins and `RULE_SET_VERSION` string asserted
  together, so an implementation edit cannot drift from the declared rule silently.
- No new DB-tier test: the loader's SQL is two straight reads; dev-verify is the
  full-population dry run recorded below, re-run against the merged code before the
  backtest re-run.

## Full-population verification (already run, dev DB)

- Chained series: 8,391 bars, 1993-01-29 → 2026-07-08, seam 2022-05-10, dates unique
  and ordered, both segments gap-free (max 7 calendar days).
- Classifiable: 8,192 days, 1993-11-11 → 2026-07-08 — 34 calendar years. All four
  regimes populated: bull_quiet 5,977, bear_quiet 1,910, bear_volatile 155,
  bull_volatile 150.
- **Strict-extension check: 0 disagreements** between the chained classification and
  the live provider on all 819 dates the live provider can classify.

## What this does NOT do

- It does not run the walk-forward (queue item 6) — it makes item 6's per-year /
  per-regime blocks producible. The backtest re-run is a follow-up invocation, not
  part of this diff.
- It does not touch the live scan's benchmark, `price_daily`, or any promotion gate.
- It does not modify the frozen corpus — read-only consumer.
- It does not resample, rescale or fill either vendor's closes.
- It does not classify 1962–1992 (SPY inception bound, above).

## Security model

No security surface: read-only queries against two research tables already read by the
same job, no user input, no new endpoint.
