# The pattern hunt: a disciplined search for edges in the data we can actually get

Refs #2437, #2899 (selection programme v2 runs in parallel), #2827, #2832, #2829, #3381.
Operator mandate (2026-09-25): inventory every tool and dataset, then **hunt for patterns ourselves**,
backtest, think outside the box. Use what already failed to know where not to look.
Framing reviewed by Codex ckpt-1 (160 findings, 2026-09-25); the structural corrections are below. Each
build ticket gets its own spec + ckpt-1.

## The rules that make a hunt trustworthy

A search over many ideas manufactures winners by chance. These rules are the whole point:

1. **Time-split, with purged boundaries** (no odd/even series split: series-id parity is not random, and
   cross-sectional ranks would cross it).
   - **Discovery**: 1990-01 → 2008-12. Search freely; nothing here is evidence.
   - **Validation**: 2009-01 → 2021-06-28, ending the day before the repo's existing `HOLDOUT_BOUNDARY`
     (2021-06-29). Only candidates frozen in discovery, each tried once, and reported separately from discovery.
   - **Holdout**: the existing post-2021-06-29 namespace under the existing access audit. ⚠ It is **already
     partly seen** (the S1–S4 primary holdout and #2908 opened 2022+), so it is a **corroboration**, not a pristine
     test. **The real confirmation is forward**: a frozen candidate trades demo paper for a declared window
     before any capital.
   - Every forward-return label is purged at each boundary (63 sessions); normalisers, ranks, winsorisation and
     regime thresholds are fitted **inside** the permitted window only.
2. **A trial is defined before anything is looked at**: sign, horizon, lag, transform, threshold, universe,
   weighting, construction, exits and costs. Registered in #2829 **before** its outcome is computed.
   Exploratory charts and abandoned screens count. The inherited count (#2827/#2832/#2840/#2908/selection v2)
   is carried forward as a floor.
3. **Numerical bar** (all three):
   - arm **minus its matched control** (the same construction, exposure, holding period and eligibility
     without the signal: the #2908 lesson) with **t > 3.0** (Harvey/Liu/Zhu), using date-clustered, overlap-aware
     inference;
   - deflated Sharpe probability ≥ 0.95 with the inherited trial count and the measured dispersion of trial Sharpes;
   - feature screens controlled with **Benjamini–Yekutieli** (valid under dependence) **across the whole
     programme**, never reset per family.
   Plus a **power statement** before each look. "Not significant" at low power is "undetermined", not "no edge".
4. **Mechanism = a directional prediction written before the look**, with the competing explanation it must
   beat. A post-hoc story is not a mechanism.
5. **Budget and a terminal outcome.** Hunt 1 gets a fixed trial budget (declared in its spec). When the families
   below are exhausted without a survivor, the programme ends with a written **"no demonstrated edge"** verdict,
   and the product is the market sleeve plus whatever selection v2 proves. No indefinite new searches.

## What we can trade (levers, measured 2026-09-25)
- Long real stock/ETF at x1; **short via CFD at x1** (settled 2026-08-09 permits shorting for research/paper).
  Easy-to-borrow short costs spread only; hard-to-borrow costs borrow/365 per night, tripled Friday.
  Borrow can be unavailable or recalled: an executable rule is needed, not an assumption.
- **Index CFD hedge** (SPX500/NSDQ100): 0.007% **per side**, plus its own overnight financing.
- **Options**: exist for UK accounts but are **not reachable via the API** (0 of 16,153 instruments are type 9;
  no endpoint). Recorded in `.claude/skills/data-sources/etoro-api.md`.
- No leverage above x1. **A long book plus an index short is 200% gross**: whether that fits the #2844
  sandbox ("exposure ≤ assigned capital") must be settled in the spec before any hedged arm is built.

## Cost model (per side, never "round trip")
Stock CFD 0.15% **per side** + the underlying market spread (the tariff does not replace it) + 2¢/unit per side
for stocks ≤ $3 + long overnight (6.4% + benchmark)/365 on notional with the weekend triple + dividend
adjustments on both legs. Index hedge: 0.007% per side + index overnight. Real stock/ETF: spread (+ any
account commission) + 0.75% FX on actual conversions only. Today's tariff applied to 1990–2024 is a **labelled
counterfactual**, run with spread/carry stress arms that widen in exactly the states the signals select
(gaps, illiquidity, short stress).

## Data: what we hold, what is free, and what each can actually test

| source | history | usable for |
| --- | --- | --- |
| Daily OHLCV + dividends + splits, incl. delisted (`research_price_daily`) | 1962 → 2024-09 | discovery + validation. ⚠ #3362: few observed terminations before 2013, so early years are near survivor-only; declared, not ignored |
| As-filed fundamentals (#3360) | 2011 → | validation window only (with selection v2) |
| FINRA short interest | 2017 → | validation-late + forward; needs rebuilt publication dates AND revision handling |
| SEC fails-to-deliver | 2004 → | discovery-late + validation; availability = actual release (2–6 weeks after settlement), balance ≠ new fails, pre-2008-09 censored |
| SEC MIDAS per-stock market structure | 2012 → | **no point-in-time discovery period** (released quarterly, after the fact) → validation only, declared low power |
| Cboe VIX/term points/SKEW/VVIX/put-call | 1990/2007–11/2006 → | regime conditioning; each series has its own start and publication time |
| FRED/Treasury/COT | 1970s → | regime conditioning via **ALFRED vintages** / actual release dates |
| Wikipedia pageviews | 2015-07 → | validation-late + forward; needs a dated article map |
| **eToro crowd positioning + top investors** | **forward only** | forward evaluation after ≥ 12 months (6 months ≈ 2 independent blocks at 63 days, too few) |

## Hypothesis families (each with a directional prediction to be frozen in its spec)
1. **Overnight vs intraday.** Where the close→open drift concentrates (it is holding compensation per
   `strategy-evidence.md` §0, so the question is **which** names and states, and whether a long-only book that
   holds only overnight beats holding all day, net of two sides of cost). Guard: the shared open print can
   manufacture reversal; test with next-day entry.
2. **Liquidity/volume shocks → subsequent weeks**, signals formed on day t, entered at t+1.
3. **Scheduled flows** where the announcement date is actually knowable: dividend ex-dates (no double counting
   against adjusted prices), turn-of-month, pre-holiday. Effective samples are small (shared dates): inference
   is clustered on the event date.
4. **Settlement/short stress (FTD, SI)** as an **avoid-list** first (which longs to skip), then as a signal.
   Not event-form trading: these are market-data aggregates, not the filings cut on 08-22.
5. **Regime conditioning** of 1–4 on VIX/VIX3M slope and credit spreads, plus a **crash overlay** for the core
   sleeve judged on drawdown reduction per unit of insurance cost, not on return prediction.
6. **MIDAS footprints** (odd-lot share, hidden share, cancel ratio), validation-only and low power, declared.
7. **eToro crowd extremes**, forward only.
8. **Interactions**, a nonlinear model trained on discovery only, nested feature selection, benchmarked against
   the linear/equal-weight baseline. No univariate pre-filter that would discard interaction-only features.

## Build order
1. **#3381 recorder**, now, with the full schema: raw payloads, request parameters, cohort membership, collection
   failures. Extend it to the other perishable evidence: quotes/spreads, eligibility, borrow status, what-if costs.
2. **Admissibility probe** (cheap, before any store): coverage by year/source/stratum, identity linkage for each
   new source, publication clocks, and a contamination inventory (which years each prior study saw).
3. **Trial registry + harness**: the freeze boundary, matched-control evaluator, cost model above,
   BY-FDR/DSR/clustered inference, power calculator, holdout-access path. **Built before any outcome-bearing
   exploration.**
4. **Feature store**: identity = raw manifests + mappings + calendars + adjustment/termination rules + costs +
   split membership + seeds. PyArrow declared as a direct dependency if used directly (today it is transitive).
5. **Hunt 1**: families 1–3 on held price/dividend data, within its declared trial budget.
6. **Ingests**: FTD → Cboe → FRED/ALFRED/COT → MIDAS → Wikipedia, each gated by the admissibility probe.
7. **Hunts 2–4**: families 4–6 and 8; family 7 at ≥ 12 months of recording.
8. Survivor → construction frozen **before** its confirmation (sizing, turnover band, hedge rule) → forward
   demo paper with a declared duration and promotion criteria → only then capital.

## What we will not do
Treat discovery as evidence; open the holdout per candidate and call it pristine; tune after a look; drop failed
trials; re-run #2827's daily chart-pattern entries; use leverage above x1; search without a budget and a terminal
verdict.
