# market-technician / quant-methods

## When to use

Choosing or reviewing the mathematics that turns bars into systematic measurements — spread/liquidity estimators, range-based and forecast volatility, position-sizing formulas, trend-strength metrics. Read before implementing any of these or citing their constants.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

⚠⚠ **MEASURED (verification sweep, 2026-08-15): the constants in this file are exactly where citations drift.** Two live mis-transcriptions were found in reputable secondaries during that sweep: Abdi & Ranaldo's own *RFS* Table 1 restates Corwin-Schultz's β with a ½ factor absent from Corwin's own SAS code, and a widely-used practitioner survey garbles Yang-Zhang's k denominator (T/(T−2) for (n+1)/(n−1)). Implement from the formulas below (verified against author code/primary PDFs where stated), and reconcile any secondary against them. Every "MEASURED (ours)" figure below reproduces via `scripts/verify_2437_quant_methods_panel.py` — the script is the recipe (panel definition, bar-eligibility and aggregation rules live in its docstring, not here).

## Spread and liquidity from daily bars

All computable from OHLCV we already store. Log prices throughout (h, l, c = ln H, ln L, ln C).

- **PUBLISHED — Roll (1984), *JF* 39(4), verified against the original PDF:** spread = 2·√(−cov(Δp_t, Δp_{t+1})) — bid-ask bounce leaves first-order serial covariance = −s²/4 in price changes. ⚠ Roll himself *preserved the sign* when covariance came out positive (half his daily estimates were negative); setting positives to zero is the later convention (Harris 1990, codified in Abdi-Ranaldo Table 1: 2·√(max(−cov, 0))). On returns the estimate is the percentage spread (his footnote 5: bias negligible).
- **PUBLISHED — Corwin & Schultz (2012), *JF* 67(2), verified against the AUTHORS' OWN SAS code:** for day pair (t, t+1), log prices:
  - β = (h_t − l_t)² + (h_{t+1} − l_{t+1})²  ← plain sum, **no ½ factor**
  - γ = [ln(H_{t,t+1} / L_{t,t+1})]², where H_{t,t+1} = max(H_t, H_{t+1}) and L_{t,t+1} = min(L_t, L_{t+1})
  - α = [(√(2β) − √β) / (3 − 2√2)] − √[γ / (3 − 2√2)]
  - S = 2(e^α − 1)/(1 + e^α). Aggregation, theirs: per-PAIR estimates over overlapping day pairs, negative pair estimates set to 0 (their primary daily estimator), monthly = mean of dailies with ≥12 observations.
  - Their overnight adjustment, applied to EACH day before its pairs are formed: prior close below today's low → shift today's H and L down by (L_t − C_{t−1}); prior close above today's high → shift both up by (C_{t−1} − H_t).
- **PUBLISHED — Abdi & Ranaldo (2017), *RFS* 30(12), verified against the paper:** with η_t = (h_t + l_t)/2 (log mid-range), the base moment form is s² = 4·E[(c_t − η_t)(c_t − η_{t+1})] (their Theorem 1); the practical **two-day-corrected estimator** (their eq. 11, the one measured below) moves the max inside: ŝ = mean_t √(max(4(c_t − η_t)(c_t − η_{t+1}), 0)) — the two are NOT algebraically interchangeable. **Their horse race: CHL wins on daily data** — average cross-sectional correlation 0.74 with TAQ effective spreads vs 0.37–0.65 for Corwin-Schultz, Roll, Gibbs, EffTick, FHT (Oct 2003–Dec 2015 monthly panel), with the advantage largest in their less-liquid buckets. Hasbrouck (2009, *JF* 64(3)) is the Bayesian-Gibbs Roll variant (correlation 0.965 with transaction-level estimates). **CONVENTION (our default, not a published verdict):** start with CHL, escalate to Gibbs only if CHL proves insufficient here.
- ⚠⚠ **MEASURED (ours, 2026-08-15; reproduce: `uv run python scripts/verify_2437_quant_methods_panel.py`) — the LEVEL is unusable here, the RANKING is usable, CHL ranks best:** on 1,629 quoted instruments (trailing 60 `price_daily` bars, no overnight adjustment, vs `quotes.spread_pct`): median estimate ≈ **1.22%** for BOTH estimators vs median quoted **0.126%** — ~10× level mismatch. Candidate explanations, plausible and UNTESTED: the estimators absorb volatility; our quotes are point-in-time snapshots rather than time-matched effective spreads; eToro bars are bid-derived. The measured fact is the mismatch, not its cause. Ranking IS monotone: mean estimate 1.79% in the top quoted-spread quartile vs ~1.06–1.10% in the bottom; **Spearman 0.429 (CHL) vs 0.353 (CS)**. Verdict: admission-filter *ranking* tool for the backtest corpus, NEVER a cost-model input. **CONVENTION (policy):** where a live quote exists, use the quote.
- **Blocked (repo data fact, per `etoro-api` skill):** Kyle's λ and anything needing signed order flow — no tape, no depth (the WS gap). Amihud |r|/dollar-volume is the flow-free workhorse and already lives in `strategy-evidence` §2.11.

## Range-based volatility (same bars, more information)

- **PUBLISHED — Parkinson (1980), *J. Business* 53(1):** σ̂² = (1/(4·ln 2))·E[(ln(H/L))²]. Up to ~5× more efficient than close-to-close — under its own assumptions (driftless continuous Brownian motion, no overnight jump); the gain shrinks as those fail.
- **PUBLISHED — Garman & Klass (1980), *J. Business* 53(1):** the form most libraries implement (e.g. TTR): σ̂² = 0.5·(ln(H/L))² − (2·ln2 − 1)·(ln(C/O))². Same zero-drift/no-opening-jump assumptions. ⚠ Their actual "best analytic scale-invariant estimator" is a different formula (0.511(u−d)² − 0.019{c(u+d) − 2ud} − 0.383c², u=ln(H/O), d=ln(L/O), c=ln(C/O)) — do not attach the "best analytic" label to the simple form.
- **PUBLISHED — Rogers & Satchell (1991), *Ann. Appl. Prob.* 1(4):** σ̂²_RS = ln(H/C)·ln(H/O) + ln(L/C)·ln(L/O) — drift-independent within the day.
- **PUBLISHED — Yang & Zhang (2000), *J. Business* 73(3):** the one that incorporates overnight gaps — relevant here because gaps are our measured tail killer: σ²_YZ = σ²_overnight + k·σ²_open-close + (1−k)·σ²_RS over an n-day window (n>1), where the overnight and open-close legs are DEMEANED sample variances of ln(O_t/C_{t−1}) and ln(C_t/O_t) respectively, σ²_RS is the window mean of the Rogers-Satchell term, and k = 0.34/(1.34 + (n+1)/(n−1)). ⚠ The k denominator is the confirmed mis-transcription site — pin the (n+1)/(n−1) form. The overnight component alone is a candidate gap-risk feature (**CONVENTION** until measured as one).
- **MEASURED (ours, 2026-08-15; same panel script):** Parkinson/close-close σ ratio across the 1,629-instrument panel: median **1.104**, IQR [0.95, 1.32] — the two agree to first order on our bars. That is level agreement only; no forecasting-accuracy comparison has been run here.

## Volatility forecasting (bands describe; these predict)

- **PUBLISHED — EWMA (RiskMetrics Technical Document, 4th ed. 1996):** σ²_t = λ·σ²_{t−1} + (1−λ)·r²_{t−1}, **λ = 0.94 for daily returns** (0.97 for a monthly-return series — a different data frequency, not an aggregation of daily forecasts). One causal recursive line.
- **PUBLISHED — GARCH(1,1) (Bollerslev 1986, *J. Econometrics* 31; ancestor Engle 1982):** σ²_t = ω + α·ε²_{t−1} + β·σ²_{t−1}; covariance stationarity needs ω>0, α,β≥0 and α+β<1, giving unconditional variance ω/(1−α−β). One published worked example — an example, not portable defaults (Engle's *GARCH 101*, *JEP* 2001, Table 3; daily US portfolio returns 1990–2000): ω=1.4e−6, α=0.0772, β=0.9046. EWMA is the integrated case ω=0, α=1−λ, β=λ. **CONVENTION (our default):** start with EWMA, escalate to fitted GARCH only on evidence.
- Forecast vol exists to feed SIZING and regime context — as a trade signal it is refuted territory here (S-4's compression cut; and Cederburg's replication limits vol-scaling's benefit to momentum/profitability/BAB — `strategy-evidence` §2.4).

## Position sizing — the equations the family was missing

**MEASURED (ours, 2026-08-09, recorded in `docs/proposals/ta/2026-08-09-plan-of-attack.md`):** gaps void stops — 141 of 1,402 filled stops executed at an open beyond the stop level (1,364 event DAYS; a name can stop more than once, hence the larger denominator), worst single trade −87% *with* a 20% stop. Size is the only surviving control, so the sizing math is load-bearing:

- **PUBLISHED — Kelly (1956, *Bell System Tech. J.* 35, verified against the paper):** his form is the even-money ℓ* = q − p via log-growth maximization. ⚠ The textbook f* = p − q/b is Thorp's practitioner restatement, and the continuous f* = μ/σ² is **Thorp (1971; 2006 handbook chapter, Eq. 7.4)** — the log-utility case of Merton (1969). Attribute the continuous form to Thorp/Merton, never to Kelly's paper.
- **PUBLISHED — fractional Kelly (Thorp 2006 §7.3; MacLean/Thorp/Ziemba 2010/2011):** under the continuous log-growth approximation, a c-fraction bet earns c(2−c) of full-Kelly growth (half-Kelly keeps 75% — exact only in that model), and estimation error in μ makes full Kelly overbet — the asymmetry is the argument. Practitioner default ¼–½ Kelly is **CONVENTION** built on that published asymmetry.
- **PUBLISHED — volatility scaling, two distinct constructions (do not conflate):** Moreira & Muir (*JF* 2017) weight by inverse *variance* c/σ̂²_{t−1} (previous month's realized). Harvey et al. (*JPM* 2018, 60 assets to 1926, 10% target): Sharpe improvement concentrated in the risk assets they test (equities, credit), negligible in their bond/currency/commodity samples; left-tail severity reduced across their asset classes. Cederburg et al. (*JFE* 2020, 103 strategies): real-time versions generally underperform unmanaged — **exceptions: momentum, profitability and BAB** (matching `strategy-evidence` §2.4, the authority here).
- ⚠ Kelly-sizing from backtest μ is barred here until a strategy has forward paper history — a declared-trials-deflated μ is exactly the estimation-error case fractional Kelly exists for. `strategy-evidence` owns that bar.

## Trend strength beyond ADX

- **PUBLISHED (practitioner) — Clenow, *Stocks on the Move* (2015), DETAILS-UNCERTAIN on the constant:** OLS regression of ln(price) on time over 90 days; annualized slope × R² as the momentum rank. ⚠ A log-price slope annualizes exponentially (exp(slope)^252 − 1); the (1+slope)^252 form circulating in implementations treats the slope as a simple return — the two disagree, and neither was verified against the book's own page. Reconcile against the book before freezing one; whichever is chosen is a by-construction constant.
- **CONVENTION (by construction if used) — slope t-statistic:** t = slope/SE of the same regression is a self-contained significance measure; no canonical published treatment as a trend filter was LOCATED (2026-08-15 sweep — an absence of findings, not proof of absence) — Clenow's R² plays that role in the published relative. If gating on it, freeze the window and cut in a version hash.
- **PUBLISHED — Kaufman Efficiency Ratio (*Smarter Trading*, 1995):** ER = |P_t − P_{t−n}| / Σ|P_i − P_{i−1}| — net move over path length, bounded [0,1]; inside KAMA (fast=2, slow=30 periods) the smoothing constant is SC = [ER·(2/3 − 2/31) + 2/31]². **MEASURED (ours, 2026-08-15; same panel script):** ER(10) across 1,629 instruments — quartiles 0.14 / 0.30 / 0.49, 24.2% above 0.5. ⚠ That is a LAST-BAR cross-section: it shows a mid-range cut is selective in general, and it also means any cut chosen after seeing it is data-informed — a gating threshold must be declared ex ante against the pre-signal population (the candidates doc carries this rule).
- **PUBLISHED (negative) — Hurst/Lo:** rescaled-range from Hurst (1951); Lo (*Econometrica* 1991) modified R/S found no evidence of long memory in the index-return samples he examined (daily through annual) once short-range dependence is accounted for. **CONVENTION (our stance):** a Hurst-exponent trend claim carries no weight here unless it clears Lo's correction.

## The one-line map

Routing summary only — each row's tags, caveats and evidence live in the sections above; the table asserts nothing beyond them.

| question on a chart | equation to reach for |
| --- | --- |
| how expensive is this name to trade (no quote) | Abdi-Ranaldo CHL — *ranking only* here, level is ~10× off |
| how volatile is it right now | Yang-Zhang (gaps matter) or Parkinson (quick) |
| how volatile will it be | EWMA λ=0.94; GARCH(1,1) if evidence demands |
| how much of the move was overnight risk | Yang-Zhang σ²_overnight component |
| is it trending or chopping | ER(10) (bounded, distribution measured) or Clenow slope×R² |
| how big should the position be | vol target (declare MM-variance or Harvey-vol form); fractional Kelly only with forward history |

## Cross-links

Hub: [SKILL.md](SKILL.md). Candidate signals built from these: `docs/proposals/ta/2026-08-15-market-technician-derived-candidates.md`. Tradability bars: `quant/strategy-evidence.md` (turnover first; vol-scaling §2.4; Amihud §2.11). Implementation map + version-hash discipline for any constant frozen here: `data-sources/market-structure.md`.
