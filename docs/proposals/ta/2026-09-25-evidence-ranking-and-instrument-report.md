# One evidence pipeline: instrument report → ranking → strategies

Refs #2437, #1815, #1822, #2899 (selection v2), #3383 (pattern hunt), #3381, #2855.
Operator direction (2026-09-25): eBull is at its core a Bloomberg-style terminal that says whether an
instrument is a good or bad investment. The rankings and the strategies should go hand in hand, and the ranking
should all but dictate which stocks deserve close attention, and **why**.
Framing reviewed by Codex ckpt-1 (100 findings, 2026-09-25). The first draft proposed an immediate v1.6 re-weight,
which Codex rejected as unsupported by the cited evidence and as an invented model; it is withdrawn here.

## What is true today (mapped 2026-09-25)
- **The ranking (`scoring.py`, `v1.5-balanced`) is a declared heuristic** (settled: "v1 is heuristic, explicit and
  auditable"). Six families (quality .25, value .25, turnaround .20, confidence .15, momentum .10, sentiment .05)
  plus penalties and a Calmar reward. None is tied to a validated result. Newer signals (F-score, Altman Z″,
  insider, 13F, short interest, peer grade) sit at **weight 0 until a backtest (#1815 §8)**, and **that backtest,
  #1822, was never built**. **#1822 is the missing link between research and ranking.**
- Thesis dependence is real but partial: value takes the thesis base/bear value when a usable thesis exists
  (aged or not; staleness deducts 0.15 separately); confidence defaults to 0.5 when there is no thesis; a low
  thesis confidence deducts 0.10; completeness gives the thesis 15%. The thesis writer is parked (#2855).
- The instrument page is rich (Research/Verdict/Financials/Positions/News/Filings), but:
  - the rank lives only on the Verdict tab;
  - nothing states the ranking is heuristic;
  - there is no tradability view;
  - Reg SHO short volume has no route;
  - `fair_value_band_current` has no operator surface;
  - risk metrics sit only on a separate page;
  - `/instruments/{symbol}/dilution` exists but no share-count view uses it.
- The advisory ranking and the autonomous strategy executor are **deliberately separate** (settled).
  Unification below respects that: validated research may inform **both**; neither silently drives the other.

## Principle
**Rank moves only on evidence; the page always says how much evidence stands behind what it shows.**

Evidence status has **two axes**, bound to an exact construction and version (a metric *name* never carries
validation):
- **maturity**: untested → under test → inconclusive / failed / passed-backtest → passed-forward → retired;
- **purpose**: return signal · risk/avoid signal · eligibility constraint · context.

Plus a per-row **data-usability** state (usable / missing / stale / quarantined / not applicable), so a validated
signal with no usable observation for this instrument never looks like a verdict. Every status carries its
evidence reference and promotion date (an auditable lifecycle).

## Changes, in order

### 1. Honesty first (no model change)
- Label the ranking **"v1.5 heuristic, not validated"** wherever a rank or score appears.
- **SummaryStrip:** the current rank (only if the instrument is in the latest run; otherwise "not ranked" with its
  reason), same-version Δ, and the **actual largest contributions and decisive deductions**, not the "top two factors".
- **Consolidate** the proposed "why" card into the existing Verdict family table: add maturity/purpose/usability
  badges per row, rather than building a second surface.
- **Unranked-state design:** non-analysable, non-tradable, unsupported and held-but-unranked instruments get a
  useful report, and "not ranked" never reads as "bad investment".

### 2. Instrument report: data we already hold, each with its definition and caveats
- **Risk inline:** 3y vol, max drawdown, Calmar and beta from `instrument_risk_metrics_current`, keeping basis
  (price vs total return), window, status and coverage.
- **Valuation band:** `fair_value_band_current` with `method_version`, quality/absence reasons, cohort and
  timestamps, labelled as a deterministic peer band, not validated fair value.
- **Share count & dilution:** a UI over the existing `/dilution` endpoint. Split/class/ADS basis stated; this is
  not the #2903 signal.
- **Short side:** short interest and days-to-cover (outstanding-share denominator, settlement and share-count
  freshness as the existing reader gates them), plus a new Reg SHO short-volume route. Each measure is defined
  separately; FTD joins after the hunt ingest, with its publication lag and the fact that it is a balance, not new fails.
- **Tradability:** from the #3381 recorder's persisted eligibility/quote/cost observations, **scoped to account,
  environment, direction and settlement, and timestamped**. Spread, real vs CFD, short permission, minimum size,
  and a what-if cost quote with undecodable components shown as unavailable.
- **Crowd:** after #3381 has data, eToro long/short share and trader-count change, with coverage and cohort context.

### 3. Build #1822 properly: the ranking's own backtest
Using the hunt harness (#3385) and the PIT bundles (#3360/#3361/#3362):
- **Ablate the existing v1.5 families** (each family out, matched control, the same universe and construction) to
  measure each family's incremental contribution to a ranking-driven portfolio. Evidence about the *composite*,
  which the first draft lacked.
- Measure the weight-0 signals (F-score, Z″, insider, 13F, SI, peer grade) the same way.
- **Weights change only through this**, with a full-population A/B that covers the consumer side: rankings,
  recommendations (BUY 0.35 / thesis-free 0.55 thresholds, allocation order, ADD), coverage tiers, and a publication
  and rollback sequence. Cross-version Δ is never displayed.

### 4. The promotion chain from research to ranking
A selection signal (#2901 → #2904) reaches the ranking only via: frozen construction + trial accounting → PIT
matched-control pass → the combination passes its own test (#2904) → declared forward paper passes → a new
versioned ranking model through step 3's A/B. If selection fails, the settled terminal outcome stands: **the product
is the low-cost market sleeve**. We do not invent a replacement stock ranking.

### 5. Rankings page as the watchlist
Maturity/purpose badges per column **and** usability per cell; "why" chips from actual contributions; a strategy
column only for a frozen strategy version's actual holdings (never "would buy"); tradability columns from the
recorder, scoped as above.

## Order
0. #3381 recorder (already first: perishable).
1. Honesty (1) and report panels over held data (2: risk, band, dilution, SI/Reg SHO).
2. #1822 on the hunt harness (3), once #3385 exists.
3. Tradability and crowd panels as #3381 data accrues.
4. Promotions per (4) as verdicts land.

## Not doing
Re-weighting by intuition; an interim re-normalised model; letting any family's name inherit a test it did not pass;
displaying an obsolete rank as current; letting a rank imply an EXIT (settled: no superior-rotation exits).
