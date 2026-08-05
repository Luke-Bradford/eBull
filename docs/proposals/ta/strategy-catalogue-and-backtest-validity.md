# TA strategy catalogue + backtest validity — design

Date: 2026-08-04
Status: Proposal, pending operator sign-off
Parent: #2240 (TA strategy platform). Companion to
`docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md`, which
specifies the *machine*. This specifies **what goes in it, and what makes its
numbers trustworthy**.

Operator brief (2026-08-04):

> *"scripts that will look for opportunities based on prior examples … what do
> hedge funds apply when looking at TA … a page where I could put money aside
> into this and select which strategies I wanted to apply … strategies that have
> been backtested, success/win rates … automated in a way that can attempt these
> trades in real time, but is also still looking for when they happen and
> recording them to show the win/fail, to see if we need to refine them. All of
> this in the demo/paper trading account."*
>
> *"I can just leave money in the pot and let this take shape, keep itself
> refining."*

That last sentence is the most dangerous requirement in the brief, and §6 is
about honouring it without building an overfitting machine.

**Reviewed adversarially by Codex before operator sign-off. Seven of its
findings changed the design materially:**

1. **Every strategy had the same look-ahead bug** — signals were specified as
   conditions on bar *t* with no fill timing, i.e. trading at a close only
   knowable once the bar ended. Fixed once globally in the new §3.5 (fill at
   `open(t+1)`) rather than per strategy, so it cannot creep back one strategy
   at a time.
2. **The cost criterion was unimplementable.** It required "the spread as of the
   entry date"; `quotes` holds one row per instrument, so no spread history
   exists and the rule would have applied today's liquidity to a 2022 trade.
   Replaced with a declared static model + start recording spread history now.
3. **S-6's "last swing" was a look-ahead trap** — swing pivots need right-hand
   bars to confirm, so they are not knowable at the time the rule implied.
   Confirmation lag is now explicit and global (§3.5 rule 3).
4. **Allocation is itself an adaptive strategy.** "Move capital toward what is
   working" fits noise with money behind it. Now pre-registered, shrunk,
   capped, rate-limited and subject to false-discovery control (§6).
5. **Strategy identity must cover code, not just parameters** — same params with
   a changed filter, universe or cost model is a different strategy (criterion 11).
6. **The metric set was too thin and the overlap correction too crude** —
   win rate + expectancy is insufficient; `n/20` ignores cross-sectional
   correlation. Now a full metric set plus block bootstrap clustered by date.
7. **Reproducing #2260 is necessary but not sufficient** as harness acceptance —
   added a synthetic control: 1,000 seeded random-entry strategies matched for
   exposure and turnover, with a stated pass threshold.

Two further findings were accepted and folded in without restructuring:
`not_evaluable` reason codes (criterion 8), and measuring quarantine exclusion
as a possible selection bias (criterion 9).

**A second Codex round on the revision found five more, all fixed:** an
off-by-one between §3.5's `k+N+1` and S-6's confirmation wording (which still
permitted a decision on the confirmation bar); S-4's stop/target ATR not being
indexed to the signal bar (sizing a stop off the fill bar leaks it); S-2's
"last trading day of the month" being unknowable without future bars — now
triggered on the first bar of a new calendar month; a stale "must be
cost-gated" claim in §3 contradicting S-3's removal of the spread gate; and the
synthetic control having no tolerance or failure threshold, making it
unfalsifiable.

The pattern across both rounds is worth noting for whoever implements this:
**every single defect was a look-ahead or an unfalsifiable claim**, and none
would have been caught by a passing test suite. They are the class this spec
exists to guard against, and they were in the spec written to guard against them.

---

## 0. CORRECTION (2026-08-04, same day) — §1 and §2 were scoped to the wrong universe

**Operator challenge:** *"Are you sure you're checking for what data is
available? … you don't have to stick with what we have today … the market moves
regardless of what eToro data we have, we just have slightly different prices."*

Correct, and it invalidates the two constraints this document was built around.
§1 measured **our Postgres**, and §2 then treated eToro's API as the boundary of
what is obtainable. It is not. **eToro is the execution venue, not the data
source**, and separating those two roles removes both "hard" constraints.

Measured 2026-08-04, after the challenge:

**Survivorship is NOT unfixable.** Two free sources, both tested:

- **SEC EDGAR full-index** — `Archives/edgar/full-index/{year}/QTR{n}/form.idx`
  lists every filing by form type. Form **25** / **25-NSE** *are* the delisting
  notifications. Measured: **622 Form 25/25-NSE filings in 2023 QTR1 alone**,
  each with CIK, company name and filing date; the index runs back to 1993. Free,
  no key, and we already have SEC UA + rate-limit discipline. This is an
  authoritative, dated delisting record — better than a vendor flag.
- **Public price feeds retain delisted names.** Tested `FRCB` (First Republic
  Bank, failed May 2023): **3,933 bars, 2010-12-09 → 2026-08-03, last close
  $0.0004.** The failure is right there in the data.

> ### ⚠ 0.1 CORRECTION (2026-08-05, spike S8 / #2284) — both bullets above are wrong
>
> They are left in place because the reasoning built on them propagated into the
> consequences below, and a silent edit would hide that. **Read this block as
> superseding them.** Full measurements and method: #2284.
>
> **(a) "Public price feeds retain delisted names" is false at cohort scale.** It
> was inferred from one hand-picked example. Tested against every 2023 Form 25
> common-equity delisting — **382 tickers**, issuer resolved from the filing XML,
> symbol from the pre-delisting cover-page `dei:TradingSymbol`:
>
> | outcome on the free public feed | n | share |
> | --- | ---: | ---: |
> | no series at all | 334 | **87.4%** |
> | series *starts after* the delisting (unrelated company on the ticker) | 10 | 2.6% |
> | series *spans* the delisting (successor entity, or OTC continuation) | 38 | 9.9% |
> | **series that stops at the delisting** | **0** | **0.0%** |
>
> Nothing marks which case a symbol is. `SI` (Silvergate Capital, failed 2023)
> now returns a series **beginning 2025-07-31** — a different company. `SAFE`
> returns 9,244 bars from 1989, which is iStar's history welded to Safehold's
> ticker. **`FRCB` is the 9.9% OTC-continuation case and generalises to nothing.**
> Survivorship needs a source that is explicitly survivorship-bias-free; every
> such source is paid. See #2284 for the chosen one and its price.
>
> **(b) "622 filings in 2023 QTR1" counts index ROWS, not filings** — EDGAR
> indexes each 25-NSE under *both* the exchange CIK and the issuer CIK. QTR1 is
> **329 distinct accessions**; full-year 2023 is 2,437 rows → **1,282 filings**.
>
> **(c) Form 25 is per-SECURITY, not per-issuer, and needs a rule-provision
> filter.** Source rule: **17 CFR 240.12d2-2**. Berkshire Hathaway filed two
> 25-NSEs in 2023; `<descriptionClassSecurity>` reads *"0.625% Senior Notes due
> 2023"*. A "CIK in a Form 25 ⇒ delisted" register marks **Berkshire delisted in
> January 2023**. The discriminator is `<ruleProvision>` in the filing's
> structured XML — (a)(1) redemption-with-funds-deposited and (a)(2) redeemed/
> matured are debt lifecycle, **440 of 1,282 (34.3%) of 2023 filings**, and are
> not delistings. Only (a)(3) substitution-by-operation-of-law, (a)(4) rights
> extinguished, (b) exchange-initiated, and issuer-filed Form 25 = (c) voluntary
> withdrawal are. Filtering on provision *and* common-equity class gives **578
> filings / 443 issuers** for 2023.
>
> **(c2) A survivorship-free corpus is shallower than §0 assumed, and that reopens
> §2.2.** §0 argued that with ~45 years of history "the walk-forward objection
> largely dissolves". Depth and survivorship-freedom turn out to be *different
> purchases*. EODHD's delisted history carries **EOD prices only before 2018** — no
> splits, no dividends — and an unadjusted series with unknown splits is not a
> usable TA input, so its survivorship-free window is effectively **2018→present**.
> Sharadar is survivorship-free from **1998** with split/dividend adjustment, but
> is **US-only**. So the honest options are ~8 years global, or ~28 years US.
> Either way **§2.2's argument for few simple strategies stands on data grounds
> after all**, not only on overfitting discipline — the walk-forward objection did
> not dissolve, it moved. Decide the fork before §4's catalogue is fixed: it
> determines whether S-2 cross-sectional momentum has a survivorship-corrected
> universe to run on at all.
>
> **(c3) The free path was searched exhaustively and there is no free answer** (#2284
> close-out, 2026-08-05). Ten sources tested against the same 382-name cohort — yfinance,
> the Hugging Face bulk archive, `lse-data`, Nasdaq's own API, stockanalysis.com, Alpha
> Vantage, Marketstack, Stooq, qlib, Kaggle. **Every one returns 0 of 382.** The reason is
> structural: Yahoo is effectively the only free full-market historical endpoint, so every
> free "archive" is a snapshot of it (the HF archive matched Yahoo's first-bar date **29/29,
> including its artefacts**), and Yahoo has no delisting concept. The two sources that pass
> are **one-off purchases**, not subscriptions: FirstRateData (308/382 tagged `-DELISTED`)
> and HistoricalData.net (from $299).
> **Full landscape, the fingerprint technique and the acceptance test:
> `.claude/skills/data-sources/research-price-corpus.md` — read that, not this paragraph.**
>
> Adopted consequence: **build on free data, buy at the validation gate.** Free deep history
> on survivors is strictly better than eToro's 4-year cap and is enough for §2b primitives
> (#2279) and single-name strategy *development*. It is not enough for *validation*, and
> #2260 is a validation question. Two guards are mandatory while free data is in use — label
> every survivor-only metric in the signal ledger, and guard ticker reuse (first bar must
> precede the known listing; truncate at the Form 25 suspension date).
>
> **(d) The register carries no ticker and SEC will not supply one.**
> `submissions` JSON drops `tickers` to `[]` on delisting;
> `companyconcept/…/dei/TradingSymbol.json` 404s (the XBRL company APIs are
> numeric-facts-only). The symbol must be read from the **cover-page inline XBRL**
> of the last periodic report filed before the delisting — which resolved
> **382 of 443 (86.2%)**, the residue being closed-end funds and foreign private
> issuers. #2282 needs this as an explicit resolution step, not an assumed join.

**~4 years is eToro's ceiling, not the market's.** Tested via yfinance:
**AAPL 11,502 bars back to 1980-12-12**; MSFT 10,176; JPM 11,690; GME 6,157.
That is **~10× the history** our corpus holds, free. §2.2's "one-and-a-bit
regimes" argument applies only to the eToro corpus.

**And the two price series are interchangeable for TA — measured, not assumed.**
Comparing our stored closes against raw public closes on ~1,035 overlapping bars
per instrument:

| | AAPL | MSFT | GME | JPM | HD | KO | XOM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| daily-return correlation | 0.979 | 0.963 | 0.996 | 0.989 | 0.985 | 0.979 | 0.992 |
| mean level bias | −0.14% | −0.14% | −0.22% | −0.17% | −0.20% | −0.16% | −0.17% |
| median RSI-14 difference (0–100 scale) | 0.19 | 0.16 | 0.12 | 0.18 | 0.15 | 0.18 | 0.13 |
| SMA-200 regime agreement | **100.0%** | 99.8% | 99.8% | 99.9% | 99.4% | 99.9% | 99.8% |

The bias is **consistently negative** — eToro's close sits ~0.15–0.22% *below*
the public close, i.e. about a half-spread. That independently corroborates S3
(#2243): our bars are built from **Bid**. It is a level offset, not a shape
difference, and TA reads shape.

⚠ Note the earlier run of this comparison used dividend-adjusted public prices
and showed JPM/HD diverging ~5%. That was my error, not a data problem — it is
the dividend yield. Against **raw** closes every name agrees. It does, however,
confirm that eToro candles are unadjusted price, not total return (§5
criterion 10).

**Consequences — these supersede §2.1 and §2.2:**

1. **Split the corpus by role.** A **research corpus** from public data (deep
   history, delisted names retained, dividends and splits available) and the
   **execution venue** (eToro, whose measured ~0.15% half-spread becomes a
   calibrated cost input rather than a guess). Signals are generated and
   backtested on the research corpus; only fills, costs and live quotes come
   from eToro.
   ⚠ **Still correct as an architecture — but "from public data" meant "free"
   here, and §0.1(a) killed that.** The split stands; the research corpus comes
   from a paid, survivorship-bias-free vendor (#2284), not a free retail feed.
2. **"Start recording today" was wrong** as the primary answer on survivorship.
   Still worth doing for our own forward record, but it is now a *supplement* to
   a reconstructable history, not the only path. Downgraded accordingly.
   ⚠ **Partially reinstated by §0.1(a).** The history is reconstructable for US
   equities (Form 25, filtered per §0.1(c)) and for whatever the chosen vendor's
   delisted flag covers. Outside that — ~4,749 non-US instruments — there is no
   free dated delisting register, so forward recording *is* the only path there.
3. **§2.2's argument for few simple strategies still stands** — but on the
   grounds of overfitting discipline and multiple-testing (§5 criterion 6), not
   on data scarcity. With 45 years the walk-forward objection largely dissolves.
4. ⚠ **The source that produced every §0 measurement is not the source we will
   use.** yfinance/Yahoo proved the architecture and is disqualified from
   carrying it: Yahoo's ToS §2.4(i) prohibits automated collection and §2.4(j)
   prohibits building "any database, archive … data feed" from the Services —
   which is exactly what a stored research corpus is. That is a stronger
   objection than "unofficial scraper" and no library stability fixes it. The
   §0 *findings* survive (the two series are still interchangeable; the market
   still has decades of history); only the vendor changes. See #2284.

**Also settled the tooling question, empirically.** `vectorbt 1.1.0` installs and
runs on this repo's Python 3.14 (verified: a full MA-crossover backtest with
fees returning total return, Sharpe, trade count and max drawdown in one call).
`numba 0.66.0` and `TA-Lib 0.7.1` both publish 3.14 support. So **phases 3–5 do
not need a bespoke backtester** — the portfolio simulation, cost model, trade
accounting and performance statistics are off-the-shelf and battle-tested. What
remains genuinely ours is the strategy definitions (§4), the validity gates
(§5), the governance (§6), and the eBull-specific plumbing.

The rest of this document stands. §3 (what funds actually run), §3.5 (execution
semantics), §4 (the catalogue), §5 (validity gates) and §6 (governance) are
unaffected by the correction — if anything §5's gates matter more now, because a
45-year survivorship-inclusive corpus makes it possible to run the tests that
were previously impossible.

⚠ **One qualification on that last clause, from §0.1.** A survivorship-inclusive
corpus is now a *purchase*, not a download, and it is US-complete rather than
globally complete. §5's gates are unaffected; what changes is that the universe
they can honestly be run over may be US equities only. That is a scope decision
for the catalogue (§4), and it should be made explicitly rather than discovered
when a global backtest quietly excludes its own losers.

---

## 1. Data reality — measured 2026-08-04, not assumed

⚠ **Superseded in scope by §0** — the figures below are accurate for **our
Postgres corpus** and remain the right numbers for anything eToro-sourced
(execution, live quotes, the tradable universe). They are *not* the limit of
available market data.

Every number below is from the dev corpus post-#2262 seeding and post-0a
quarantine. These are constraints, not context: three of them kill or reshape
strategy families outright.

**History depth**

| bars per instrument | instruments |
| --- | ---: |
| < 100 | 675 |
| 100–249 | 3,800 |
| 250–499 | 1,359 |
| 500–999 | 2,786 |
| 1,000–1,059 | 3,559 |

Median **518 bars (~2 years)**. Only **4,190 instruments have ≥750 bars (~3
years)**. Corpus span is 2019-12-16 → 2026-08-04, but no single instrument
exceeds ~1,059 bars because eToro's candle endpoint caps at 1,000 per request
and has no `from_date` pagination (#603) — so **~4 years is the hard per-
instrument ceiling**, not a backfill we have not got round to.

**Tradable universe with price history**

`us_equity` 7,098 · `eu_equity` 2,764 · `uk_equity` 931 · `asia_equity` 891 ·
`crypto` 289 · `fx` 63 · `commodity` 44 · `index` 33.

**Volume coverage** (gates every volume-dependent signal)

| class | bars | with volume |
| --- | ---: | ---: |
| us_equity | 4,033,980 | **74.8%** |
| eu_equity | 1,372,311 | 76.7% |
| uk_equity | 523,417 | 76.3% |
| asia_equity | 401,074 | 77.5% |
| mena_equity | 25,561 | 97.5% |
| commodity | 34,090 | 15.3% |
| index | 19,117 | 1.2% |
| **crypto** | 221,379 | **0%** |
| **fx** | 58,724 | **0%** |

**Transaction costs** — `quotes.spread_pct`, 1,456 instruments quoted:

| percentile | spread | round trip (2×) |
| --- | ---: | ---: |
| p25 | 0.052% | 0.10% |
| p50 | 0.110% | 0.22% |
| p75 | 0.235% | 0.47% |
| p90 | 0.598% | 1.20% |
| p99 | 4.072% | **8.14%** |

**Survivorship** — 12,691 instruments, 12,684 tradable. **Only 5 non-tradable
instruments have any price history.**

---

## 2. Three hard constraints that shape everything

### 2.1 Survivorship bias is unfixable for historical backtests

Our instrument table is "what eToro lists **now**". Five delisted instruments
have price history. Any backtest replaying 2019–2026 therefore selects
exclusively from companies that survived to 2026 — the losers were never in the
corpus to be picked.

The direction of the error is always flattering, and the magnitude on a
small/micro-cap-heavy universe is not small. Recall §1: 570 of our instruments
are sub-$1, and 568 arrived with the #2262 sweep (#2266). That is precisely the
population where delisting is common and where survivorship inflates results
most.

**This is a leading candidate for #2260's unexplained RSI<30 → 76.8% against a
49.9% base rate.** "Buy oversold" is a bet on recovery; a corpus containing only
recoverers will report that it works.

Can we reconstruct it? **No.** `instruments.first_seen_at` / `last_seen_at` only
begin 2026-06-03 (when we started syncing), so there is no record of who was
listed in 2021. And `last_seen_at` is **misnamed**: `sync_universe` bumps it only
inside the `DO UPDATE … WHERE (something changed)` clause, so it is
*last-modified*, not *last-seen* — an unchanged instrument never bumps it (89 of
12,691 rows carry the most recent sync's timestamp). It cannot be used as a
presence signal. Delisting *is* captured going forward, but by a different path:
the deactivation `UPDATE … SET is_tradable = FALSE` for ids absent from the feed.

**Consequences, both mandatory:**

1. **Start recording point-in-time universe membership now** — a dated
   membership table written by `sync_universe`, appended on every run. Every day
   without it is a day of backtest history that can never be made honest. This
   is the cheapest high-value item in the whole programme and it is not in any
   phase. **It should ship before phase 2, not with phase 5**, because its value
   is purely a function of how long it has been running.
2. **Every backtest result over pre-2026-06 data carries a survivorship warning
   in the UI**, not a footnote in a doc. A win rate the operator can allocate
   capital against must not silently omit that its universe excluded the failures.

### 2.2 ~4 years of history is one-and-a-bit regimes

The window covers the 2020 crash, the 2021 melt-up, the 2022 bear and the
2023–26 recovery — but only 3,559 instruments reach back the full ~4 years, and
the median instrument has ~2.

Walk-forward with a meaningful out-of-sample hold-out inside 4 years leaves very
little to fit on. This is a genuine limit on statistical confidence and must be
stated on the surface, not buried. It also argues strongly for **few, simple,
theory-backed strategies with few free parameters** over a large search across
parameterised variants — the latter needs data we do not have and will not get.

### 2.3 Volume is equity-only

Zero volume on crypto and FX, 1.2% on index. So anchored VWAP, volume-confirmed
breakouts, OBV and relative-volume filters are **equity-only signals**, covering
~75% of even those bars. Any strategy depending on volume must declare
`not_evaluable` elsewhere rather than silently treating NULL as zero — the
vacuous-truth class already in the prevention log and in the parent design's §5.

---

## 3. What competitive systematic funds actually run

Honest framing, because the brief asks for it: the strategies with durable,
independently replicated, out-of-sample evidence are **not** the chart-pattern
vocabulary. What survives is a short list, and it is mostly boring.

| family | who runs it | evidence | our data fit |
| --- | --- | --- | --- |
| **Time-series momentum / trend following** | the entire managed-futures / CTA industry (AQR, Winton, Man AHL) | strongest and longest — replicated across a century and every asset class | **excellent** — needs only close prices |
| **Cross-sectional momentum (12-1)** | ubiquitous in quant equity | the most replicated equity anomaly there is | **excellent** — 12,179 instruments is real breadth |
| **Short-horizon mean reversion (1–5d)** | stat-arb desks | real, but capacity-limited and cost-sensitive | **conditional** — dies on our p90 spread. ⚠ v1 has **no ex-ante liquidity gate at all**: no as-of spread is stored (§5 criterion 2), so costs are charged at settlement from the static model and the strategy is simply allowed to lose on them. A real gate arrives once spread history accrues (§8) |
| **Volatility regime / compression breakout** | CTAs, vol funds | decent; overlaps trend | **good** — ATR/range only |
| **Support / resistance as liquidity** | discretionary desks; systematised rarely | weak-to-moderate; mechanism (resting orders at prior extremes) is real | **buildable** once #2279 primitives exist |
| **Fibonacci retracement** | widely drawn, rarely systematised | weak — no robust independent evidence | buildable; treat as a **hypothesis to falsify**, not a signal to trust |
| **Elliott wave** | discretionary only | not deterministic | **excluded** by parent design §6, correctly |

Two points the brief should hear plainly:

- **Trend and cross-sectional momentum are the answer to "if price was all you
  had".** They are what a competitive systematic fund actually deploys on
  price-only data. They are unglamorous and they work.
- **Fibonacci and S/R are in scope because they are deterministically
  computable, not because they are proven.** Building them is right — the whole
  point of the platform is to let a signal earn or lose its place on evidence.
  But they should enter with a prior of "probably nothing", and the surface must
  not present an unproven signal the same way it presents a validated one.

---

## 3.5 Execution semantics — global, and non-negotiable

⚠ **Codex round 1 found the same look-ahead bug in every strategy below**: each
was written as a condition on bar *t* with no statement of when the fill occurs.
"Buy when close > sma_200" filled at that same close is a trade at a price only
knowable once the bar has finished. It is the most common backtest error there
is, it inflates every result, and specifying it per-strategy invites it back one
strategy at a time. So it is specified once, here, and applies to all of them.

1. **Signal on the close of bar *t* → fill at the OPEN of bar *t+1*.** No
   exceptions, entries and exits alike. The backtester must make same-bar fills
   structurally impossible rather than merely discouraged.
2. **Every indicator at bar *t* uses only bars ≤ *t*.** Enforced by test
   (criterion 4).
3. **Any pivot/swing detection carries an explicit confirmation lag.** A swing
   high with an N-bar right window is not known until *t+N*; the strategy may
   reference it only from *t+N+1*. Unstated lag here is the single likeliest
   source of a fake edge in S-5/S-6, and it is why they are sequenced last.
4. **Intrabar stop-and-target on the same bar is `ambiguous`, never a win.**
   If one bar spans both levels, the order of touch is unknowable from OHLC.
   This is spike S5 (#2245), still open — until it is settled, such outcomes are
   recorded `ambiguous` and excluded from the win rate with their count shown.
   Silently resolving them favourably is how backtests manufacture edge.
5. **Eligibility filters are evaluated as-of the decision date**, never once
   against today's state. "Instruments with ≥273 bars" means ≥273 bars *as of
   that rebalance*, not "instruments that eventually accumulated 273 bars" —
   the latter leaks future existence into past selection.

## 4. Strategy catalogue v1

Six strategies. Deliberately few, each with ≤3 free parameters, each with a
stated economic rationale. **A strategy with no rationale is a data-mining
result waiting to fail out-of-sample**, and phase 8 (discovery) is safe only
once the harness below is proven on hypotheses chosen in advance.

Each entry: rule → parameters → rationale → data constraint.

**S-1 · Time-series momentum (trend)**
Signal: `close(t) > sma_200(t)` and `sma_50(t) > sma_200(t)`. Exit signal:
`close(t) < sma_50(t)`. Fill both at `open(t+1)` per §3.5.
Params: 2 (the two lookbacks — fixed, never tuned).
Rationale: persistence of returns at 3–12 month horizons; the CTA base case.
Data: close-only. Needs ≥200 bars as-of the decision date.
Note: `derive_trend_signals` (#1989) already exposes `price_vs_sma200` and
`sma_50_200_regime` — but on the LATEST bar only. The backtest consumes
phase-2 recomputed history, never the stored current-state columns.

**S-2 · Cross-sectional momentum (12-1)**
Rebalance trigger, defined causally: the **first bar whose calendar month
differs from the previous bar's** — i.e. act at the start of the new month.
⚠ "The last trading day of the month" is NOT knowable at that bar without
future bars (you cannot tell the 30th is the last session until the 31st fails
to appear), so it is a calendar look-ahead. Ranking uses return over
`t-252 .. t-21` (skipping the last ~month, which reverses); hold the top
decile; fill at `open(t+1)`.
Params: 3 (lookback, skip, decile).
Rationale: the most replicated equity anomaly; the skip window prevents
short-term reversal contaminating the momentum signal.
Eligibility, evaluated **as-of each rebalance date** (§3.5 rule 5): ≥273 bars
of history at that date. Roughly 7,700 instruments qualify today, but the
as-of count in 2022 was far smaller — the backtest must use the historical
count, not today's.
⚠ Rank within one asset class and one currency, or a currency move ranks
against an equity move. See §9 Q1.
⚠ Returns are **price returns, not total returns** — eToro candles carry no
dividend adjustment (`price_adjustments` is empty; see §5 criterion 10). This
systematically understates high-yield names over a 12-month lookback.

**S-3 · Mean reversion within trend**
Signal: `rsi_14(t) < 30` and `close(t) > sma_200(t)` (reversion inside an
uptrend, not a falling knife). Exit: `rsi_14(t) > 50`, or 10 bars elapsed,
whichever first. Fill at `open(t+1)`.
Params: 3.
Rationale: short-horizon overreaction. The trend filter is what distinguishes
this from catching a terminal decline.
⚠ **The original draft gated this on `spread_pct < 0.25%`. That is
unimplementable and was a look-ahead bug** — `quotes` holds exactly one row per
instrument (1,456 rows, 1,456 instruments), so there is no historical spread
and the gate would have applied *today's* liquidity to a 2022 decision. Removed.
Costs come from the static model in §5 criterion 2 instead, and spread history
starts being recorded now (§8) so a future revision can gate honestly.
⚠ This strategy is deliberately close to #2260's RSI<30 trigger. It is **not**
claimed that the trend filter explains that anomaly — #2260 is still
unattributed, and S-3's results are not to be trusted until it is. If anything,
S-3 is the test case: run it under the full criteria of §5 and see whether the
76.8% survives.

**S-4 · Volatility compression breakout**
Setup: `atr_14(t)` sits in the bottom quartile of its own trailing 100-bar
distribution, **computed on bars ≤ t**. Signal: `close(t) >` the highest close
of bars `t-20 .. t-1` (**prior** 20 bars, excluding *t* itself — including it
makes the condition partly self-referential). Fill at `open(t+1)`.
Exit: stop at `entry − 2 × atr_14(t)`, profit target at `entry + 3 × atr_14(t)`,
hard max-hold 40 bars — whichever comes first. ⚠ ATR is indexed at **`t`, the
signal bar**, never at `t+1`: sizing a stop off the fill bar's own range leaks
that bar into the decision that produced it. Levels are fixed at signal time
and do not move. If one bar spans both stop and
target, the outcome is `ambiguous` per §3.5 rule 4.
Params: 3 (compression window, breakout lookback, ATR stop multiple).
Rationale: volatility clusters and mean-reverts; compression precedes
expansion. Directly answers the brief's "when could price be volatile".
Data: OHLC only, but requires **complete** OHLC — instruments with NULL high/low
and any bar inside a `price_series_break` segment are `not_evaluable`, not
absent.

**S-5 · Support/resistance retest** *(blocked on #2279)*
Level formation: cluster swing pivots into levels using **only bars strictly
before the break**, with each pivot subject to its confirmation lag (§3.5
rule 3). A level needs ≥3 touches.
Signal: after a confirmed close-through break of such a level, the first bar
whose low re-enters the level band and whose close returns above it. Fill at
`open(t+1)`. Exit: stop at `level − 1 × atr_14(t)` and target the **most recent
confirmed swing extreme that is usable as of signal bar `t`** (§3.5 rule 3) —
both fixed at signal time, max-hold 30 bars. ⚠ Indexing matters here exactly as
in S-4: an unindexed ATR or a "prior swing extreme" resolved at implementation
time can pick a pivot confirmed *after* entry.
Params: 3 (pivot lookback, cluster tolerance, touch count).
Rationale: prior extremes are where resting orders cluster. ⚠ This is an
assumption about order-book microstructure that **we cannot observe** — eToro
gives us bid-derived CFD candles and no depth. Weakest rationale of the six;
included because the operator asked and because it is falsifiable.

**S-6 · Fibonacci retracement** *(blocked on #2279)*
Anchors: the most recent **confirmed** swing low and swing high. A pivot at
bar `k` with right-window `N` is confirmed at `k+N` and usable **from `k+N+1`
onward** — stated exactly as §3.5 rule 3, with no separate wording, because an
off-by-one here silently permits a decision on the confirmation bar itself.
"Last swing" without that lag is a look-ahead bug and was how the first draft
read.
Signal: within a confirmed S-1 uptrend, a close inside the 0.5–0.618
retracement band of that anchored leg. Fill at `open(t+1)`. Exit: invalidation
below the anchor swing low, target the anchor swing high, max-hold 40 bars.
Params: 2.
Rationale: **none defensible.** Included explicitly as a null-hypothesis test.
Evaluation must be a **paired incremental test against S-1 alone** — same
universe, same dates, same cost model, same exposure — not a raw return
comparison. If it adds nothing over the trend filter it already requires, it is
retired and we stop wondering.

⚠ S-5 and S-6 must not ship before S-1..S-4 have validated the harness. A
questionable strategy measured by an unproven backtester teaches nothing.

---

## 5. Backtest validity — hard acceptance criteria

**These are gates, not guidance.** A strategy that has not passed all of them
must not display a win rate anywhere the operator can allocate against it.
Issue #2260 exists because a plausible number met none of these and was believed.

1. **Point-in-time universe.** Selection only from instruments listed as of the
   entry date. Pre-2026-06 this is impossible (§2.1) → those results carry a
   `survivorship_unadjusted` flag in the data model, not just in prose.
2. **Costs applied per trade — with an honest model, not a fictional one.**
   ⚠ The first draft said "use the spread as of the entry date". **That is
   unimplementable**: `quotes` holds one row per instrument (1,456 rows / 1,456
   instruments), so no spread history exists and the rule would have silently
   applied today's liquidity to a 2022 trade. Instead:
   - historical backtests use a **static, conservative cost model** keyed on
     asset class and price band, calibrated from the §1 spread distribution and
     deliberately set at the pessimistic end (use p75, not the median);
   - the model is a **declared input** to the strategy identity hash (criterion
     11), so changing it is a new evaluation, not a silent improvement;
   - **spread history starts being recorded now** (§8) so a later revision can
     use real as-of spreads;
   - costs include more than spread: eToro charges **overnight/weekend fees on
     CFD positions**, and FX conversion applies on non-account-currency
     instruments. ⚠ The *magnitude* of that carry is *not established here* —
     no fee schedule has been measured, and holding-cost scale is the single
     biggest unknown for any multi-week strategy. Before S-1/S-2 are trusted,
     verify the current fee schedule against the eToro portal per the
     `etoro-api` skill's live-verification protocol (#2277 covers the standing
     re-check). Until then, treat long-hold results as provisional rather than
     assuming carry is negligible **or** that it dominates.
3. **Overlap-corrected statistics.** ⚠ "Effective n ≈ nominal/20" is too crude:
   returns are autocorrelated and cross-sectional signals are correlated *across
   instruments on the same day*. Use a **block bootstrap** over calendar blocks
   with errors clustered by date, and report the effective sample size and
   confidence interval — not a bare percentage.
4. **Causal indicator computation.** Every indicator at bar *t* uses only bars
   ≤ *t*. Enforced by a test that recomputes a mid-series bar from a truncated
   series and asserts equality against the full-series value. This is #2260
   candidate 1 and the likeliest of the four.
5. **Out-of-sample hold-out, enforced not promised.** The final 25% of history
   is withheld. ⚠ "One look" is governance and governance fails — the hold-out
   must be **mechanically inaccessible** to exploratory queries: a separate
   result namespace that records every access with a timestamp and strategy id,
   so a second look is visible in an audit trail rather than forgotten. With
   ~4 years of data (§2.2) a single hold-out is thin; prefer **purged
   walk-forward with an embargo** around each fold boundary to stop a 20-day
   holding period leaking across the split.
6. **Multiple-testing control, with a named method.** Use the **Deflated Sharpe
   Ratio** (Bailey & López de Prado), which takes the number of trials, their
   correlation, and the skew/kurtosis of returns. Its trial count must include
   *every* variant evaluated — abandoned branches, manual eyeballing, and
   parameter values tried and discarded. An honest trial count is the whole
   mechanism; an undercounted one makes the correction decorative.
7. **A metric set that cannot flatter.** ⚠ Win rate alone is actively
   misleading — a 76%-win strategy at 1:4 win/loss loses money. Required
   together: expectancy per trade, profit factor, CAGR, annualised volatility,
   Sharpe and Sortino, **portfolio-level** max drawdown (path-dependent — a
   per-trade figure does not compose), exposure time, turnover, trade count,
   effective sample size, and return relative to a buy-and-hold benchmark.
   A strategy that fails to beat buy-and-hold after costs is not a strategy.
8. **`not_evaluable` carries a reason code.** `missing_volume`,
   `missing_spread`, `insufficient_warmup`, `quarantined_bar`, `series_break`,
   `not_listed`, `ambiguous_intrabar`. These have different bias implications
   and collapsing them loses the ability to tell a data gap from a real absence.
9. **Quarantine exclusion is itself measured.** Dropping bad bars can bias
   selection, because bad bars correlate with illiquidity and volatility.
   Report the count and share of bars/trades excluded per strategy, and run one
   sensitivity arm with conservative handling, so exclusion is visible rather
   than assumed harmless. (Narrowing-gate rule: measure what you reject.)
10. **Corporate actions are declared, not assumed.** Splits appear to be
    provider-back-adjusted (S7: 320/330), but `price_adjustments` is **empty
    (0 rows)** and eToro candles are **price, not total return** — no dividend
    adjustment. So: momentum lookbacks understate high-yield names, and any
    unadjusted split would fabricate a signal. State the treatment per strategy;
    `price_series_break` segments (402 rows) are `not_evaluable`, never spanned.
11. **Strategy identity = code + config + data contract.** ⚠ Hashing parameters
    alone does not prevent overfitting: the same parameters with changed filter
    logic, universe definition, cost model, ranking tie-break or execution
    assumption is a different strategy. The identity hash covers all of them.
    Changing any → new id, fresh out-of-sample requirement, no inheritance of
    the prior track record.

**Acceptance for the harness itself, before any strategy is trusted:** reproduce
issue #2260's 76.8% figure, then attribute it to criteria 1/3/4. ⚠ That is
necessary but **not sufficient** — a harness can explain #2260 and still
mishandle costs, fill timing or portfolio accounting. Pair it with a synthetic
control: run **1,000 random-entry strategies** matched to each real strategy's
exposure and turnover, on the same universe and dates, with the same cost
model and a recorded seed. Acceptance: the mean net return of the random
cohort must lie within its own 95% bootstrap CI of zero, and each real
strategy's Sharpe must exceed the **95th percentile of the random cohort's**
to count as evidence at all. ⚠ A stated threshold matters more than the test —
"returns ~0" with no tolerance is unfalsifiable. A harness that finds edge in
noise is broken regardless of what else it explains.

---

## 6. Self-refinement — how to honour "keep itself refining" safely

The brief wants the system to improve itself. Taken naively — periodically
re-optimise parameters on recent data — this is the single most reliable way to
destroy a systematic strategy. It fits noise, reports rising backtest
performance, and degrades live. The backtest improves as the strategy worsens,
which is why it is so hard to catch from the inside.

What refinement can safely mean here:

**Allowed — evidence accumulation.** Live paper results append to the record;
confidence intervals narrow. A strategy builds a genuine out-of-sample track
record simply by running. The strategy does not change — our knowledge of it
does. This is the honest core of "let it take shape".

**Allowed — governed retirement.** ⚠ Not "falls outside its backtest CI": with
survivorship, cost and model uncertainty that CI is almost certainly
mis-calibrated, so testing against it inherits the bias. Use **live-only
sequential testing** — a pre-registered rule on the live record alone (e.g. a
sequential probability ratio test against zero expectancy, with a stated
drawdown circuit-breaker). The backtest earns a strategy its place in the
lineup; only live evidence removes it.

**Allowed — pre-registration, with immutable fields.** A strategy is registered
before evaluation, and these fields are **frozen at registration**: rule logic,
parameters, universe definition, eligibility rule, cost model, exit and stop
semantics, primary metric, and evaluation window. Editing any after seeing
preliminary results creates a new strategy id. Pre-registration that permits
post-hoc edits to *any* of these is theatre.

**Forbidden — continuous re-optimisation.** No scheduled job re-fits parameters
to recent data. Enforced by the identity hash (criterion 11), not by discipline.

⚠ **Allocation is itself an adaptive strategy — and the first draft missed
this.** "Move capital toward what is demonstrably working" is a rule that fits
the same noise, with capital behind it: it systematically rewards strategies
that got lucky live. So allocation is governed by the same discipline as
strategy selection:

- the allocation algorithm is **pre-registered and fixed** (e.g. risk-parity or
  equal-weight across strategies that pass their live gate), not discretionary;
- estimates are **shrunk toward the prior** — a strategy with 30 live trades
  does not get sized on its point estimate;
- **hard per-strategy caps** regardless of measured performance;
- allocation changes are **rate-limited** and logged with the evidence that
  triggered them, so the allocation path has the same audit trail as the trade
  path;
- **false-discovery control applies to allocation decisions too** — picking the
  best of 6 live strategies is itself a multiple comparison.

## 7. Capital allocation + paper trading

Per the parent design, all of this is demo-account paper trading. Additions:

- Allocation is **per strategy**, with an explicit pot; a strategy cannot exceed
  its allocation regardless of how many signals it fires.
- Orders route through `execution_guard` — the kill switch, hard caps and
  `decision_audit` trail come free, and the same rules that gate discretionary
  trades gate automated ones. No parallel execution path.
- The existing dev-DB kill switch is **active by design** and must stay so.
- Every fired signal is recorded whether or not it was acted on — including
  signals rejected for exceeding allocation. Only recording taken trades biases
  the record toward periods of spare capacity.
- Long-only, no leverage, no shorting (v1 non-negotiables) — which constrains
  S-1..S-6 to their long legs and roughly halves the theoretical edge of the
  momentum strategies. Worth stating plainly rather than discovering later.
- **Uninvested cash must be accounted for.** Long-only strategies sit in cash
  much of the time, and a return computed only over invested periods is not
  comparable to buy-and-hold. Define cash return as zero, report **return on the
  full allocated pot**, and state exposure time alongside it (criterion 7).
  Without this a strategy invested 10% of the time can post a spectacular
  "return" on almost no capital at work.
- **Results are reported at three levels and must not be conflated**:
  per-signal (did this trigger pay?), per-strategy sleeve (did the allocation
  pay?), and total paper portfolio (did the operator's pot grow?). Drawdown and
  Sharpe are only meaningful at the latter two.

⚠ **Paper fills are not free evidence.** Live paper results only count as
out-of-sample (§6) to the extent the fill model is realistic. The paper engine
must model: fill at next open per §3.5, the bid/ask actually quoted rather than
the mid, market hours and holidays for the instrument's exchange, rejected and
partially-filled orders, and the latency between signal computation and order
submission. A paper engine that fills everything instantly at the mid produces
an optimistic track record and then launders it as "live evidence", which is
worse than having no live record at all.

---

## 8. Sequencing

**Revised after §0.** The research corpus moves to the front, because it
unblocks everything downstream and turns the previously-impossible validity
gates into ordinary work. "Start recording now" drops from *the* survivorship
answer to a supplementary forward record.

| when | what | why |
| --- | --- | --- |
| **NEW — first** | **research corpus ingest**: deep public OHLCV + delisted names, into their own tables, separate from the eToro-sourced `price_daily` | §0 — 45 years vs 4, survivorship-inclusive. Unblocks §5 criteria 1, 3, 5 and 6, none of which were satisfiable before |
| **NEW — with it** | **delisting register from SEC Form 25 / 25-NSE** via EDGAR full-index | §0 — authoritative dated delisting record, free, and we already have the SEC fetch discipline. Gives point-in-time membership back to the 1990s rather than from today |
| **NEW — cheap** | **measured eToro cost model**: the ~0.15% half-spread from §0, per class and price band | §5 criterion 2 — replaces the static guess with a measurement, and re-links research prices to execution reality |
| now (downgraded) | point-in-time universe membership recording | still worth having as our own forward record, but no longer the only path to survivorship-free backtests |
| now (downgraded) | spread / liquidity history | still the route to *as-of* costs; the §0 measurement covers v1 |
| phase 2 | historical indicator recomputation | with criterion 4 enforced by test. ⚠ Reconsider scope: `vectorbt`/TA-Lib compute indicators over a series directly, so persisting a full indicator history may be unnecessary for backtesting and needed only for the live signals lens |
| **phase 2b** (#2279) | price-structure primitives | S-5/S-6 need them; the registry's vocabulary must be settled before phase 3 fixes it |
| phase 3 | strategy registry + signal ledger | S-1..S-4 registered. Ours to build — this is eBull-specific plumbing |
| phase 4 | outcome resolver | criterion 7's expectancy/drawdown — much of it is `vectorbt` trade/portfolio stats |
| phase 5 | backtester | ⚠ **Do not hand-roll.** `vectorbt 1.1.0` verified working on this repo's Python 3.14; portfolio simulation, fees, Sharpe/drawdown and trade accounting are off-the-shelf. Our work is wiring our data + strategies into it and enforcing §5. Still gated on #2260 being attributed |
| phase 6 | performance surface | must show expectancy + survivorship label, not bare win rate |
| phase 7 | allocation page | §7 |
| phase 8 | discovery | only with criterion 6 enforced |

**Vendor question, if free sources prove insufficient.** Free tier tested and
working today (public feed for prices, EDGAR for delistings). If coverage or
terms become a problem, the measured options are EODHD at **£19.99/mo** for 30+
years EOD across 60+ exchanges — but **delisted coverage sits on the £59.99
Fundamentals tier or the £99.99 all-in-one** — or Sharadar's US bundle via
Nasdaq Data Link, which is explicitly point-in-time and survivorship-free back
to the 1990s. Worth revisiting only once the free path is shown to be the
binding constraint; do not buy data to solve a problem we have not hit.

---

## 9. Open questions for the operator

1. **Universe breadth for S-2.** Cross-sectional momentum ranks instruments
   against each other. Rank US equities only (7,098, homogeneous), or all
   equities across regions (11,684, adds currency and session effects)?
   Recommendation: **US-only for v1**, add regions once S-4's session semantics
   (#2244) are settled.
2. **Rebalance cadence for S-2.** Monthly is standard and cheap. Weekly triples
   turnover and cost for marginal responsiveness. Recommendation: **monthly**.
3. **Minimum history and price floor for eligibility.** ≥273 bars for S-2
   excludes ~4,500 instruments today, many of them the newly-seeded sub-$1 names
   — probably a feature, given #2266 showed their p99.99 daily move runs to 800×
   on tick quantisation alone. Recommendation: **≥273 bars and close ≥ $1, both
   evaluated as-of each decision date** (§3.5 rule 5). ⚠ Applying "close ≥ $1"
   once against today's price would be look-ahead — it would exclude a stock
   that was $40 in 2021 and is $0.30 now, which is exactly the loser a survivor-
   biased backtest already struggles to see.
4. **Does a strategy failing its out-of-sample gate stay visible?** Recommend
   **yes, marked failed** — a graveyard is how criterion 6 stays honest and how
   we avoid retesting the same idea annually.

---

## 10. What "competitive" honestly means here

⚠ **Revised after §0.** The first draft said "~4 years of history and ~12k
instruments is not going to beat a CTA", and used data scarcity as the reason to
lower expectations. That reason is gone: with a survivorship-inclusive research
corpus reaching back decades, an authoritative delisting register, an
industrial-grade backtester, and a *measured* execution cost, **the research
setup here is not far off what a small systematic shop actually runs on.** The
remaining gaps are real but they are different ones — long-only, no leverage, no
shorting, and a single retail execution venue. Those cap the achievable Sharpe;
they no longer cap the quality of the research.

The honest edge statement: a fund's advantage comes from execution quality, cost
control, breadth, leverage and shorting — not from a better RSI threshold. We
give up leverage and shorting by policy. What we can genuinely compete on is
**rigour**: most retail systematic trading fails not on strategy cleverness but
on survivorship, look-ahead, overlapping windows and multiple testing — the four
things §5 gates and the four things that were, until §0, impossible for us to
test properly.

What this platform can realistically deliver, and what makes it worth building:

- an **honest** measurement of whether a given price pattern has any edge on our
  actual universe, with costs and biases accounted for — which is more than most
  retail tooling does, and more than our current TA does (today's entry-timing
  thresholds were chosen by judgement and have never been validated against
  outcomes);
- a **record** that accumulates, so a year from now the question "does this
  work?" has an evidence-based answer rather than an opinion;
- a **discipline** that makes it hard to fool ourselves — which, given #2260, is
  the binding constraint, not strategy cleverness.

That is a realistic and genuinely valuable goal, and §0 makes it a more ambitious
one than this document originally claimed.

**Lesson recorded, because it nearly cost the milestone:** every constraint in
§1–§2 was measured correctly and scoped wrongly. I queried our database and
concluded what the *market* made possible. "Survivorship is unfixable" was true
of our Postgres and false of the world; "~4 years is the hard ceiling" was
eToro's ceiling, not the market's. The operator's challenge — *"you don't have to
stick with what we have today"* — is the general rule: **before declaring a
constraint fundamental, check whether it is a property of the problem or of the
data source you happened to look at first.** A correct measurement of the wrong
population is still the wrong answer, and it is more dangerous than a wrong
measurement because the numbers all check out.
