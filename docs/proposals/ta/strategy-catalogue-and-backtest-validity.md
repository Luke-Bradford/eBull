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
| **Cross-sectional momentum (12-1)** | ubiquitous in quant equity | the most replicated equity anomaly there is | ⚠ **revised by §4.0** — 12,179 is the *tradable* count, not the *validatable* one. S-2 ranks within **6,733 US stocks**; still real breadth, but half what this cell claimed |
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
   Such outcomes are recorded `ambiguous` and excluded from the win rate with
   their count shown. Silently resolving them favourably is how backtests
   manufacture edge.

   ✅ **Spike S5 (#2245) is ANSWERED (2026-08-06) and CONFIRMED this rule
   rather than changing it.** Measured on 25,559,104 signals: the ambiguous
   class runs 0.83% of signals at a 1.0×ATR target down to 0.09% at 4.0×.
   Two additions it makes, both binding on phase 4:
   - ⚠ **Never "assume SL first for conservatism".** It is not conservative,
     it is a different bias — it penalises TP-first strategies, and the
     distortion scales with how tight the target is, so it hits hardest
     exactly the strategies the ambiguity affects most.
   - **Intraday cannot rescue a historical bar.** `get_intraday_candles` has
     no date parameter, no offset and no cursor, so a past date is not
     addressable; forward signals are resolvable inside a ~2-session window
     and historical ones never are. Hence every outcome carries a
     `resolution_method` stamp, so a later intraday-backed resolution cannot
     mix silently into daily-bar statistics.

   ⚠ S5's sweep entered at the **close of bar `t`**, which is the same-bar
   fill rule 1 forbids. That was right for sizing the ambiguous class and is
   NOT a backtest baseline — phase 4's resolver enters at the open of `t+1`
   per rule 1, so its distribution is comparable in magnitude to S5's table
   and is not expected to reproduce it. Implemented in
   `app/services/outcome_resolver.py`;
   spec `docs/proposals/ta/2026-08-06-outcome-resolver.md`.
5. **Eligibility filters are evaluated as-of the decision date**, never once
   against today's state. "Instruments with ≥273 bars" means ≥273 bars *as of
   that rebalance*, not "instruments that eventually accumulated 273 bars" —
   the latter leaks future existence into past selection.

## 4. Strategy catalogue v1

### 4.0 Validation universe — DECIDED 2026-08-05 (#2289)

**Strategies are validated on US stocks (ex-ETF) only. Non-US instruments stay tradable
but are not backtest-validated, and no strategy allocates to them on backtest
evidence.** §0.1(c3) left this as "a scope decision for §4"; this is the call.

Measured on the dev corpus 2026-08-05 (`instruments` ⋈ `exchanges.asset_class`;
`asset_class` is on `exchanges`, not on `instruments`):

| asset class | tradable | survivorship-correctable? |
| --- | ---: | --- |
| `us_equity` | 7,288 | **partially** — Form 25 register (#2282 2c) + a bought US corpus, at 86.2% issuer resolution and subject to the two biases below. Not "yes" |
| `eu_equity` | 2,805 | no |
| `uk_equity` | 989 | no |
| `asia_equity` | 894 | no |
| `mena_equity` | 61 | no |
| **non-US equity** | **4,749 (37.4%)** | **no source at any price we would pay** |

⚠ **`us_equity` is an *exchange* classification, not a security-type filter, and
the validated universe is narrower than the row above.** Of the 7,288, **6,733
are `Stocks` and 555 are `ETF`** (`etoro_instrument_types` 5 and 6; `asset_class`
lives on `exchanges`). Ranking an ETF against a common stock is precisely the
homogeneity violation S-2's own ⚠ warns about, and a fund's price path is a
basket's, not an issuer's. **The validated universe is `asset_class='us_equity'
AND instrument_type_id = 5` — 6,733 instruments.**

⚠ **That cut is *necessary, not sufficient*, and this section deliberately says
"US stocks ex-ETF" rather than "US common stocks".** eToro's `Stocks` type is a
provider classification, not a security-master one: ADRs/ADSs, US-listed foreign
private issuers, REITs, BDCs, preferreds, units, rights and SPAC remnants can all
carry it. Calling the result "common stock" would be the wrong-population error
this document's own §10 lesson is about. Two consequences:

- The residue is **partly separable, contrary to a first draft of this
  paragraph** — `ads_ratio` (`sql/240`) and the FPI/ADR valuation suppression
  (`sql/237`) already identify part of the ADR population. Narrowing the cut
  further is open work, not an impossibility; it is deferred because S-1..S-4 do
  not rank across issuer domicile and S-2 is the only one that would notice.
- **`instruments.instrument_type_id` carries no foreign key** — verified: the
  only constraints on `instruments` are the PK, the `canonical_instrument_id`
  FK and a self-reference check. The universe definition therefore rests on an
  unconstrained integer maintained by the provider sync. It is clean today
  (0 NULL across all 12,684 tradable), so this is a latent risk rather than a
  live defect: **the universe query must assert the type-id lookup resolves,
  not assume it.**

Why this had to be decided *before* the catalogue rather than caveated after it:
eToro serves all 12,684 shallowly, so a global backtest neither fails nor warns —
it simply never sees the non-US losers. Left undecided, that ends as validated on
57% of the universe and allocated across 100% of it, with nothing in the output
to say so — which is what the allocation invariant below exists to prevent.

✅ The universe query lives in `app/services/strategies/validated_universe.py`,
which resolves the type id through `etoro_instrument_types.description = 'Stocks'`
and raises when that lookup does not resolve to exactly one row — the assertion
this section asks for, rather than a hardcoded `5`.

**Adopted: #2289 option (1) + option (3).**

**(1) US-only validation universe.** Every §5 gate is run over the 6,733 US
stocks defined above. A
strategy's live universe may be wider than its validated one, but the difference
is carried in the signal ledger (#2288), and §7's allocation reads the validated
universe, not the live one.

**(3) Forward-record non-US membership from today**, so the constraint expires
instead of being permanent. This is not free-standing: `sync_universe` currently
*destroys* the transition it would need to record —
`UPDATE instruments SET is_tradable = FALSE, last_seen_at = NOW()`
([`app/services/universe.py`](../../../app/services/universe.py)) overwrites in
place, so there is no dated membership row, and `last_seen_at` ends up holding
the date we **noticed** the instrument was gone rather than the last date it was
listed. That is Form 25 trap 5 in a different costume (`sec-edgar.md` §2.6): the
detection date is never the date the consumer needs. Specified and ticketed as
**#2290**; the shape to copy is `instrument_symbol_history` (`sql/103`), which
already carries the exact temporal invariants — ordered ranges, single-current
partial unique index, GIST no-overlap.

Option (2) — global universe, survivor-biased, labelled — was rejected. It puts
the entire weight on a label holding under pressure, at the one moment (an
allocation decision) when pressure is highest.

**Allocation invariant — the part a label cannot carry.** "Not validated" has to
bind execution, not just display:

1. No backtest-derived allocation reaches an instrument outside the validated
   universe. Zero, not scaled-down.
2. **Order eligibility, not just allocation eligibility.** (1) constrains which
   *pots* are funded; it does not constrain what a funded pot then buys. A
   US-validated S-1 sleeve receiving capital and spending it on a non-US live
   signal satisfies (1) and defeats it. So: **an order funded from a strategy's
   allocation may only be placed on an instrument in that strategy's validated
   universe.** This is a hard pre-trade rule and belongs in
   [`execution_guard.py`](../../../app/services/execution_guard.py) — which is
   deterministic, fail-closed and already writes one `decision_audit` row per
   invocation — not in the signal ledger. **A ledger label is observability;
   this needs enforcement.**
3. **Live paper results on unvalidated instruments do not become allocation
   evidence either.** §6 moves capital toward what is working; without this
   clause a non-US sleeve accumulates live performance and re-enters allocation
   through the back door, having passed none of §5. If a non-US variant is ever
   to be allocated against, it registers as a **separate strategy identity** —
   universe is part of the identity hash (criterion 11), so "S-1 on US stocks"
   and "S-1 on `eu_equity`" are two strategies and always were.
4. Performance metrics are reported split by validated universe, never pooled.
   A pooled number launders (3) back in as arithmetic.

**Two biases this decision does NOT fix.** Both are survivorship-shaped and
neither was measured by #2284, so neither is covered by buying the corpus:

- **eToro-listing bias.** The research universe is *"US names eToro lists
  today, plus delisted names the corpus supplies"* — not *"US names that were
  listed as of the entry date"*. A company that traded 2005–2015 and was never
  on eToro's book is absent, and its absence looks exactly like survivorship.
  The bought corpus reduces this (it is keyed on the market, not on eToro) but
  only for names we think to ask for. Quantify it before trusting any
  cross-sectional result: count corpus symbols with no `instruments` row.
- **Form 25 resolution residue.** The register resolves **382 of 443 issuers
  (86.2%)**; the missing 13.8% is closed-end funds (N-CSR, no cover-page XBRL)
  and foreign private issuers (`sec-edgar.md` §2.6). US is "correctable" at
  86.2%, not 100%, and the residue is **not** random with respect to fund-shaped
  instruments.

**Per-strategy consequence.** An earlier draft of this table graded
"selection" against "price-path" and got it wrong, in a way worth recording
because it is the intuitive error: it labelled every non-ranking strategy
`selection = low`. That is false. A delisted name absent from the corpus never
fires its signals at all, so **the trade set is truncated for every strategy**,
not only for the one that ranks. The distinction that actually earns its place
is **what fixes it**:

- **omission** — the missing names' own trades never appear. Affects **every**
  strategy. Fixed only by a corpus that retains the losers' bars to the end,
  i.e. by the purchase.
- **rank contamination** — the missing names change which *surviving* names get
  picked, because the rule is relative. Affects **S-2 alone**. Needs
  point-in-time membership at each rebalance (criterion 1) *on top of* the
  corpus.

| strategy | validated universe | omission | rank contamination | note |
| --- | --- | --- | --- | --- |
| S-1 time-series momentum | US stocks ex-ETF | **high** | none | trades on names that later died are simply absent, so the outcome distribution is survivor-truncated |
| S-2 cross-sectional momentum | US stocks ex-ETF | **high** | **yes — only strategy affected** | a missing bankrupt loser promotes a survivor into the top decile that never belonged there. The skill calls this shape fatal |
| S-3 mean reversion in trend | US stocks ex-ETF | **highest** | none | the #2260 shape. "Oversold and kept going" is the *definition* of the missing population, so the absent signals are disproportionately the losing ones |
| S-4 volatility breakout | US stocks ex-ETF | **high** | none | distress *is* volatility expansion, so delisted names are over-represented in the setup, not incidental to it |
| S-5 / S-6 | US stocks ex-ETF | **high** | none | failed names generate many breaks and retests near distress. Blocked on #2279, but that is not the only gate |

⚠ **Nothing in this table is "safe", and omission is not a lesser problem than
contamination.** The only sequencing consequence is that S-2 additionally needs
criterion 1; every strategy is blocked on the corpus purchase before its numbers
may be allocated against.

⚠ **S-2 is not descoped, and #2289's body was one step too pessimistic on it.**
It argued S-2 needs index-membership history that even the paid US options do not
sell. S-2 as written in this section does not rank within an index — it ranks
within the *eligible listed* universe (≥273 bars, one asset class, one currency).
What it needs is point-in-time **listing** membership.
⚠ Form 25 supplies only the *end* of a listing. Criterion 1 needs both ends, and
the start has no equivalent register — it comes from the corpus's first bar, which
is a proxy and is exactly what the ticker-reuse guard (#2282 scope item 7) exists
to police. Listing membership is therefore *reconstructable* for US stocks, not
simply *available*. Restricting its ranking universe
to US stocks — which §9 Q1 already recommended on homogeneity grounds — is
sufficient. If S-2 is ever re-specified against an index, that reopens.

---

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

✅ **IMPLEMENTED 2026-08-06** — `app/services/strategies/s1_time_series_momentum.py`,
a pure function against the phase-3a registry contract. Two notes it settles that
this entry left open:

- **Both legs share ONE warm-up.** The exit reads only `close` and `sma_50`, so
  it is computable from bar 49; it is refused until bar 199 anyway, because
  declaring per-leg input sets would make the same bar live for the exit and
  warming for the entry — §3.1's branch-dependent evaluability one level up, and
  this entry gives the strategy a single data requirement (*"Needs ≥200 bars"*).
  ⚠ It is a narrowing, so it is counted, not asserted safe: **774,944 exit bars**
  over the validated universe (`--census`).
- **The exit leg is stateless** — `close < sma_50` fires whether or not an entry
  is open, because the ledger records decisions and §7 requires every fired signal
  recorded. Pairing an exit with the entry it closes is phase 5's.

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
of history at that date. ⚠ "Roughly 7,700 instruments qualify today" was the *global* count and is
superseded by §4.0: S-2 ranks within the 6,733 US stocks ex-ETF, and the as-of
count in 2022 was smaller again — the backtest must use the historical count,
not today's and not the global one.
⚠ Rank within one asset class and one currency, or a currency move ranks
against an equity move. Settled by §4.0 (§9 Q1 is resolved): US stocks, USD.
⚠ The **ranking signal** uses price returns, not total returns. That preserves
the registered S-2 rule and the executable OHLC geometry. Since #2429, realised
strategy wealth and the buy-and-hold hurdle are separately measured on the
research corpus's split-and-dividend-adjusted `adj_close`; changing the ranking
input itself would be a new strategy trial, not an accounting correction.

✅ **IMPLEMENTED 2026-08-06** — `app/services/strategies/s2_cross_sectional_momentum.py`,
against a CROSS-SECTIONAL extension of the phase-3a contract
(`strategy_registry.evaluate_cross_sectional`), specified in
`docs/proposals/ta/2026-08-06-cross-sectional-contract-and-s2.md`. Five things it
settles that this entry left open:

- ⚠ **This entry's two numbers disagree and both are honoured.** The window
  `t-252 .. t-21` needs 253 bars; the stated eligibility is 273 (= 252 + 21, i.e.
  computed as though the window ran `t-273 .. t-21`). The window is taken
  literally — it is also the published form, Fama-French's *prior (2-12)
  returns* — and so is the eligibility, which is the only reading that
  contradicts neither sentence. The 20-bar narrowing is counted: **99,469 bars**
  over the validated universe (`--census`).
- **The rebalance calendar is the PANEL's, not each member's.** Read per-series,
  a name resuming after a halt on the 4th ranks against whoever else resumed
  that day — a cross-section of two. Same rule on the union calendar: **774
  rebalance dates**, 762 with any participant.
- **§9 Q3's price floor ships** (`close ≥ $1`, as-of): a ranked strategy selects
  on extremes, so tick-quantised sub-$1 names are not a rare contaminant of the
  top decile, they *are* it. Cost measured: **31,746 decision bars rejected**.
  ⚠ On split-adjusted closes it is an *adjusted*-price floor, so a name that
  reverse-split 1-for-10 passes a floor it would have failed at the time — and
  reverse splits happen because a price fell under $1.
- **One leg, not two.** "Hold the top decile" makes an exit the exact complement
  of the entry over that bar's participants, so an exit row could never disagree
  with the entry beside it. Consequently S-2 declares **no** `max_hold_bars` —
  its hold is *"until the next rebalance"*, a calendar fact, and phase 5 owns
  both the pairing and the collapse of consecutive selections into one hold.
- **Three by-construction rules, because "top decile" has no published cut**:
  `k = N // 10` (floor); ties break on score descending then instrument id
  ascending (**5 of 762** rebalance dates land the cut on an exact tie, so it is
  load-bearing); and a cross-section below ten is
  `not_evaluable("thin_cross_section")` — a ninth reason code (`sql/260`) rather
  than a fake `not_fired`. ⚠ That code **never fires on today's validated
  universe** (smallest cross-section: 18); it ships fixture-covered and probed
  because the rule must be right before the panel narrows.

⚠ **No performance claim is attached, deliberately.** S-2 is the only strategy in
this table with **rank contamination**, and criterion 1 needs point-in-time
*listing* membership, which is reconstructable (corpus first bar + Form 25) but
not reconstructed. #2284's purchase is necessary and not sufficient.

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

✅ **IMPLEMENTED 2026-08-06** — `app/services/strategies/s3_mean_reversion_in_trend.py`,
a pure function against the phase-3a registry contract. Three notes it settles
that this entry left open:

- **The "10 bars elapsed" half of the exit is NOT a signal, and cannot be.** It is
  measured from the entry it closes, so it is position state, and a pure per-bar
  verdict function has none — S-1's stateless-exit reasoning, one step further.
  It is not dropped: `MAX_HOLD_BARS = 10` is hashed into the strategy identity
  (criterion 11) and enforced by `outcome_resolver.ExitLevels.max_hold_bars`
  (phase 4a). The revert probe that matters most on this module is the one that
  deletes it from `S3_PARAMS`, because nothing else in the code would notice.
- ⚠ **A masked close refuses the whole TAIL of the series, not a 200-bar window** —
  S-3's one structural difference from S-1, and a property of Wilder smoothing
  rather than a choice: RSI carries state across every bar, so `rsi_series` has no
  window for a hole to clear. Counted, not asserted away: over the validated
  universe, **only 29 of 5,266 series carry a masked close at all — and they cost
  80,476 bars** on which the 200-day average had recovered and the RSI never will
  (`--census`, 2026-08-06). ⚠ That ratio is the point: ~2,775 refused bars per
  affected series, where S-1 would have lost at most 200. The blast radius of a
  single masked bar is the rest of the instrument's history.
- **Both legs share ONE warm-up**, at the 200 bars `sma_200` implies. The exit
  reads only `rsi_14` and is computable from bar 14, so this is a narrowing of
  185 bars per series — wider than S-1's 150 — and it is counted: **957,878 exit
  bars** over the validated universe (`--census`).

⚠ **No performance claim is attached, deliberately.** §4's own survivorship table
grades S-3's omission bias highest of the six, and #2260 remains unattributed —
the causal Wilder RSI this module computes gives 51.8% / 50.4%, not 76.8%, so
reproducing that figure would be evidence of a bug rather than of an edge.

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

✅ **IMPLEMENTED 2026-08-06** — `app/services/strategies/s4_volatility_compression_breakout.py`,
a pure function against the phase-3a registry contract. Four notes it settles
that this entry left open:

- ⚠⚠ **"Bottom quartile" had no membership rule, and there is no published one to
  cite** — so it is fixed **by construction** and said out loud, per the
  instruction set's rule for exactly this case. Bollinger's Squeeze has a
  published formulation (BandWidth at its lowest in six months); "ATR in the
  bottom quartile of its own trailing 100 bars" is this document's own
  construction, and it states the window and the quartile but not the test.
  Sample quantiles are not one thing — NumPy ships nine interpolation methods —
  so membership is defined by **rank**, which has no interpolation and no free
  parameter: `compression(t) = #{w ∈ W : w < atr_14(t)} / |W|` over
  `W = atr_14[t-99 .. t]`, and the setup holds iff that is `< 0.25`. With 100
  distinct values the k-th smallest scores `(k-1)/100`, so exactly the bottom 25
  qualify. ⚠ Ties are **forced** favourable, not chosen: counting `w ≤ atr(t)`
  would let two bars with identical ATRs in one window rank differently by
  position, i.e. read arbitrary order as signal.
- ⚠⚠ **S-4 has NO exit signal leg, and cannot have one.** S-1 and S-3 exit on a
  per-bar price condition; all three of S-4's — stop, target, max-hold — are
  measured *from the entry*, so all three are position state and a pure per-bar
  verdict function has none. This is S-3's `MAX_HOLD_BARS` reasoning applied to a
  whole exit bracket. The parameters are not dropped: `ATR_STOP_MULTIPLE`,
  `ATR_TARGET_MULTIPLE` and `MAX_HOLD_BARS` are hashed into the identity
  (criterion 11) and consumed by `outcome_resolver.ExitLevels` (phase 4a).
  **Nothing in the module reads them**, so the identity hash is the only thing
  holding them to this rule — which is why the revert probe that drops one from
  `S4_PARAMS` is the one that matters most on this module.
- **The two windows keep DIFFERENT boundaries, deliberately.** Compression is
  *"computed on bars ≤ t"* (inclusive — today's ATR is ranked against a
  distribution it is in); the breakout excludes `t` for the reason this entry
  already gives. Making them consistent would be a spec violation, so each is
  pinned by its own test.
- ⚠ **A masked bar refuses the whole TAIL of the series**, inherited from Wilder
  smoothing exactly as S-3's RSI is, and **much larger here** — `atr_14` needs
  high, low *and* the previous close, so more fields can kill it. Counted, not
  asserted away: over the validated universe **361 of 5,266 series carry a masked
  bar, and they cost 611,092 bars** on which the 20-bar breakout frame had
  recovered and the ATR never will (`--census`, 2026-08-06). Both legs also share
  ONE warm-up at 113 bars (`atr_14`'s seed plus the 100-bar window), a narrowing
  of **484,594 bars** whose breakout leg was evaluable.

**Full-population verification** (`scripts/verify_2240_s4_volatility_breakout.py`),
2026-08-06. ⚠ The ATR and the prior-20 high are compared as VALUES, bar by bar,
not merely through the verdicts they feed — both sides compute in float64 in the
same order, so agreement is expected to be bit-for-bit and any difference is
logic rather than rounding:

| corpus | series | bars | values compared | value mismatches | verdict mismatches | ties |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `research_price_daily` | 7,693 | 25,818,944 | 51,637,888 | **0** | **0** | 0 |
| `price_daily` | 12,185 | 6,702,891 | 13,405,782 | **0** | **0** | 0 |
| | | **32,521,835** | **65,043,670** | **0** | **0** | **0** |

Census over the §4.0 validated universe on masked bars: 23,339,583 bars —
fired 699,632 (2.998%), not_fired 21,432,155 (91.827%), not_evaluable 1,207,796
(5.175%; `quarantined_bar` 613,835 · `insufficient_warmup` 588,695 ·
`no_fill_bar` 5,266). Revert probes 16/16 caught.

⚠ **No performance claim is attached, deliberately** — the same reason S-3 ships
without one. §4's survivorship table grades every strategy's *omission* bias on a
survivor-only corpus, the fired share above is a count of signals rather than of
outcomes, and §5's acceptance criteria are phase 5's work, not this module's.

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
   entry date. Results that cannot satisfy it carry a `survivorship_unadjusted`
   flag in the data model, not just in prose.
   ⚠ **Scoped by §4.0 — "pre-2026-06 this is impossible (§2.1)" is now only half
   true.** For the **resolved subset of US stocks** it is reconstructable back to
   the 1990s: the Form 25 register supplies dated delistings and a bought
   delisted-inclusive corpus supplies the bars. "Resolved subset" is load-bearing
   — 86.2% issuer resolution, CEF/FPI-shaped residue, plus eToro-listing bias
   (§4.0). It is not 100% and must not be reported as though it were. For the **4,749 non-US**
   instruments it remains impossible for all history predating #2290's forward
   record, and no purchase changes that. So the flag is not a legacy marker to
   be retired — it is the permanent state of every non-US result.
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
10. **Corporate actions are declared, not assumed.** eToro candles remain
    price-only execution observations. The research corpus supplies separate
    split-adjusted OHLC and split-and-dividend-adjusted `adj_close`: OHLC governs
    signals/fills/levels while `adj_close` governs strategy wealth and the
    buy-and-hold hurdle (#2429). `price_series_break` segments (402 rows) are
    `not_evaluable`, never spanned.
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
| **NEW — first** (#2282) | **research corpus ingest**, into its own tables, separate from the eToro-sourced `price_daily`. ⚠ **Two purchases, not one:** deep history on *survivors* is free today (the HF Parquet archive, 1962→2026); **delisted names are the paid part and land at the validation gate** | §0 — decades vs 4 years. Unblocks §5 criteria 3, 5 and 6 immediately; criterion 1 only once the delisted half lands |
| **NEW — with it** | **delisting register from SEC Form 25 / 25-NSE** via EDGAR full-index | §0 — authoritative dated delisting record, free, and we already have the SEC fetch discipline. Gives point-in-time membership back to the 1990s rather than from today |
| **NEW — cheap** | **measured eToro cost model**: the ~0.15% half-spread from §0, per class and price band | §5 criterion 2 — replaces the static guess with a measurement, and re-links research prices to execution reality |
| **now — reinstated (#2290)** | **append-only universe-membership recording** | §4.0 option (3). For the 4,749 non-US instruments this is the *only* path there will ever be, and today's `sync_universe` overwrites the transition instead of recording it. Not optional, and not "downgraded" any more |
| now (downgraded) | spread / liquidity history | still the route to *as-of* costs; the §0 measurement covers v1 |
| phase 2 | historical indicator recomputation | with criterion 4 enforced by test. ⚠ Reconsider scope: `vectorbt`/TA-Lib compute indicators over a series directly, so persisting a full indicator history may be unnecessary for backtesting and needed only for the live signals lens |
| **phase 2b** (#2279) | price-structure primitives | S-5/S-6 need them; the registry's vocabulary must be settled before phase 3 fixes it |
| phase 3 | strategy registry + signal ledger | S-1..S-4 registered. Ours to build — this is eBull-specific plumbing |
| phase 4 | outcome resolver | criterion 7's expectancy/drawdown — much of it is `vectorbt` trade/portfolio stats |
| phase 5 | backtester | ⚠ **Do not hand-roll.** `vectorbt 1.1.0` verified working on this repo's Python 3.14; portfolio simulation, fees, Sharpe/drawdown and trade accounting are off-the-shelf. Our work is wiring our data + strategies into it and enforcing §5. Still gated on #2260 being attributed |
| phase 6 | performance surface | must show expectancy + survivorship label, not bare win rate |
| phase 7 | allocation page | §7 |
| phase 8 | discovery | only with criterion 6 enforced |

⚠ **RETIRED 2026-08-05 (#2284).** This paragraph used to read *"free tier tested
and working today … do not buy data to solve a problem we have not hit"*, and
listed EODHD / Sharadar subscription tiers as the fallback. Both halves are
falsified. The free path was searched exhaustively — ten sources, **every free
one returns 0 of 382** on the delisted cohort — and the problem *has* been hit,
structurally, not contingently. The two sources that pass are **one-off
purchases** (~$299), not subscriptions. **Build free now, buy at the validation
gate (#2260).** The measured landscape, the Yahoo-fingerprint technique and the
382-name acceptance test live in
`.claude/skills/data-sources/research-price-corpus.md` — read that, not a
vendor-tier list in a spec that will go stale again.

So that a future reader knows whether they need to open it: the skill is what
carries the **vendor acceptance criteria** — delisted coverage measured against
the 382-name cohort, split/dividend adjustment basis, security-level (not
ticker-level) identity, symbol-history and ticker-reuse handling, listing and
suspension dates, exchange/currency fields, and redistribution/ToS constraints.
Any candidate is run against that cohort **before** purchase; a vendor's own
survivorship claim is not evidence.

---

## 9. Open questions for the operator

1. ✅ **RESOLVED 2026-08-05 by §4.0 / #2289 — US-only, and it now binds every
   strategy, not just S-2.** The recommendation here was US-only on *homogeneity*
   grounds (currency and session effects). #2284 supplied a harder reason:
   US stocks are the only slice of the universe whose survivorship is
   economically correctable at all — and then only partially (§4.0). "Add regions once #2244 is settled" is therefore no longer
   the unblocking condition — the condition is a non-US delisting record, which
   only #2290's forward recording will ever produce.
   ⚠ And it produces one **only for periods after recording starts**. Forward
   recording never corrects the past; it makes the constraint expire gradually
   from the day it ships, which is the whole reason it ships now rather than
   when non-US validation is wanted.
2. **Rebalance cadence for S-2.** Monthly is standard and cheap. Weekly triples
   turnover and cost for marginal responsiveness. Recommendation: **monthly**.
3. ✅ **ADOPTED AS RECOMMENDED 2026-08-06 by S-2's implementation** — both
   halves ship, as-of, and are hashed into the strategy identity, so reversing
   either is a new strategy version rather than a silent redefinition. The
   measured cost is on record (`--census`: 99,469 bars to the 273-bar gate,
   31,746 decision bars to the floor), and the ⚠ the recommendation did not
   anticipate is that on split-adjusted closes the floor is an *adjusted*-price
   test — see the S-2 implementation note in §4.
   **Minimum history and price floor for eligibility.** ≥273 bars for S-2
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
