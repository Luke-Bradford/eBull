# quant/research-process: how a strategy earns the right to trade

## When to use

Use this before declaring, building or evaluating any strategy backtest, forward trial or factor study. It sets the evidence bar by type of claim and the standards a backtest must meet. It works within the 2026-08-23 settled evidence bar:

- preregistration before the look;
- survivorship-free universe;
- costs and carry charged;
- deflated significance;
- full population;
- declared falsification;
- benchmark = net buy-and-hold on the same window, plus a random-basket control (2026-10-01).

## Every study is registered

Every study, whatever its track, is declared in `app/services/trial_register.py` before outcomes are read. The declaration counts every choice that could be tuned: universe, definitions, weights, bands, overlays, horizons, segments. A programme that searches families × segments × weightings is one large search. Its global trial count feeds deflation, and its selection step is checked for overfitting with PBO/CSCV (Bailey, Borwein, López de Prado & Zhu 2017) or an equivalent nested hold-out.

## Two tracks: what each must show

**Track A, discovery** (a signal we invented or found by search).

- Deflated Sharpe (Bailey & López de Prado 2014). It needs the return series, the effective sample size, the trial count and its dependence.
- t > 3 (Harvey, Liu & Zhu 2016) is a literature guide, not a substitute for the DSR gate.
- A genuinely sealed confirmation sample.
- The base rate is that most candidates fail (Hou, Xue & Zhang 2020).

**Track B, adoption** (a published premium with independent out-of-sample support, e.g. replication across markets or periods after publication: Jensen, Kelly & Pedersen 2023).

- **Prior.** Use the post-publication evidence, net of our costs. Do not apply a further McLean & Pontiff haircut to numbers that are already post-publication: their 58% decline is measured against in-sample returns, and 26% of it already appears out of sample. Rebuilding a signal on the same CRSP/Compustat sample verifies reproducibility, not independent future profitability.
- **Fidelity.** Our construction tracks the published factor once universe, period, weighting, accounting definitions and return basis are aligned.
  - Report correlation and tracking error against the published series.
  - Low correlation is a reason to align or investigate, not proof of a defect.
- **Net result.** The backtest, net of our costs and turnover, beats the net buy-and-hold benchmark and the random-basket control on the same window. A Track B result is still a test of our implementation, so deflate for the configurations tried.
- **Forward monitoring** uses a pre-specified non-inferiority rule:
  - a margin (how much worse than the backtest expectation is tolerated);
  - the minimum information before any stop decision;
  - a sequential error-controlled boundary (the #2500 confidence sequence).

  "No harm detected" is not a pass. The pass is "within margin with the stated confidence".

## Power, per track

Before declaring, compute whether the planned data can answer that track's question, and record it in the declaration. One-sided z-test rough guide, annualised IR, independent years:

- **Formula:** `T ≈ ((z_α + z_β) / IR)²` years.
- **Worked example:** at IR 0.5 over 15 years against a t ≈ 3 threshold, power is about 14%.
- **Caveats:** overlapping holdings and regime dependence lower the effective sample. Say how dependence is handled.
- **Track A:** the backtest must have enough power to clear the deflated bar for the effect size you expect. If it cannot, change the design (more breadth, longer history, a different track) rather than declare.
- **Track B:** power is about the non-inferiority margin, not about proving the premium exists.

## Backtest standards

**Point-in-time fundamentals.**

- Use the frozen `app/services/pit_fundamentals.py` bundle, which joins acceptance time and applies a decision-session rule.
- A raw `filed_date` is not acceptance time.
- `financial_facts_raw` is under a retention sweep (`financial_facts_retention.py`), so it is not a full history.
- `financial_periods` and `fundamentals_snapshot` are restated in place and are not admissible as backtest inputs.

**Point-in-time classifications.** Shares outstanding, SIC, listing date, share class, exchange membership and identifier mappings must carry effective dates. Current metadata attached to old prices is look-ahead.

**Survivorship-free universe** from `research_price_daily`, with delisting returns handled. Each vendor's coverage, adjustment basis and duplicate securities are documented in `data-sources/research-price-corpus.md`. One archive does not certify the table.

**Returns.**

- Use total return. The Intrader vendor's `adj_close` carries split and dividend adjustment (its OHLC are raw), so compute returns from `adj_close` and validate it against an independent total-return reference.
- For live-like accounting, specify dividend entitlement at the ex-date, payment-date cash, 15% US withholding and reinvestment.
- Price selection by nominal price band uses raw prices, never split-adjusted ones.

**Splicing archives to eToro `price_daily`** needs an explicit contract:

- identity mapping;
- adjustment basis;
- overlap reconciliation;
- vendor precedence.

Never splice comparator bases silently (`data-capability.md`).

**Costs.**

- Start from the eToro spread bands in `cost_model.py`. They are proportional spreads, calibrated on late-session quotes from nine summer dates with no stressed regime, and assume real settlement.
- Add stressed spreads, gap and slippage assumptions, minimum tickets and any fixed fee as separate notional-dependent terms.
- ETF costs are calibrated separately from stocks.

**Turnover.** Measure traded notional (not rebalance frequency) and report it. Above ~50% a month, net expected benefit must be shown explicitly (Novy-Marx & Velikov 2016). This supersedes the stop-before-backtest wording in `cost-aware-viability.md` §1 for portfolio strategies.

**Segmentation.** Predeclare the primary partition (`quant/market-segments.md`) and the minimum effective sample per cell. Report pooled and per-cell results. Do not cross every axis into sparse cells and then pick the best.

**Metrics by strategy type.** This supersedes `cost-aware-viability.md`'s single per-trade hierarchy for portfolio strategies.

- **Portfolio strategies:** net active return vs SPY total return, tracking error, IR, beta, maximum relative drawdown, turnover, and an FF5 + momentum regression.
- **Signals:** monthly rank IC, IC-IR, decay by horizon, quintile spreads.
- **Event-trade strategies:** per-trade expectancy net, profit factor, clustered t.

**Inference.** Follow `quant/measurement-discipline.md`: time clustering, non-overlapping windows or a correction, placebos.

**Hold-out.**

- The house `HOLDOUT_BOUNDARY` (`strategy_result.py`) is 2021-06-29, but post-2021 outcomes have been inspected many times. Record the access history (`strategy_holdout_accesses`) and say whether a given sample is reused validation or a genuinely sealed confirmation.
- `walk_forward.py` is purged K-fold, which trains on both sides of a test fold. Add a chronological walk-forward (train only on the past, step forward) to test implementability across regimes.

## From backtest to forward test

A forward demo test checks more than code: implementation, decay, eligibility, liquidity and execution assumptions. It cannot prove efficacy quickly. Declare it with:

- the backtest's expected return and tracking error, as a prediction interval;
- the operational failure rules (fills, rejections, reconciliation);
- the performance stopping rule (sequential, error-controlled);
- the review date.

**No strategy opens positions forward without a passing backtest** (operator rule, 2026-10-04).

**What "passing" means for a Track B demo admission:** a zero-capital demo test is admitted on a four-part preregistered historical screen. Live capital keeps the full bar. The rule text and the operator's approval live only in `docs/settled-decisions.md`, entry 2026-10-08. Read it there; do not restate it here.

## LLM-derived signals

They follow `quant/llm-research.md`. Return-prediction claims from pre-cutoff dates are not evidence. Feature collection without trading is the forward path.
