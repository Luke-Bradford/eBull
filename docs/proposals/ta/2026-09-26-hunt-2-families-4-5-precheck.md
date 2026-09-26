# Hunt 2 pre-check: families 4 and 5 — no trial registered, ingests re-sequenced

Refs #2437 (queue rules 2026-09-26: power first, tracker comparator; queue note 15:39Z), #3387 (hunt 1
pre-check: its Bar A / Bar B and its referral of calendar timing, 3c, to family 5), #3384 (admissibility probe),
#2837 (S-E result). Programme doc: `2026-09-25-pattern-hunt-programme.md`, families 4–5, build steps 6–7.
Security: none (a research document; no code, broker, auth or order path).

**Status:** v3, after Codex ckpt-1: 75 findings on v1 and 33 on a scoped v2 round. Applied: the closure is a budget
decision, not an impossibility claim; gating cannot lower either bar; power needs a tracking error, not a published
mean; calendar timing added; reopen events are power-based and per-source; a reopen permits a spec, never a
promotion. No trial registered, no research return read, no ingest started. Inputs: the programme's data table,
the #3384 probe, hunt 1's bars, our recorded results (#2837; the Nagel reproduction), and published abstracts.
Nothing here enters M.

## Result
**A budget decision, as in hunt 1: register no family-4 or family-5 trial now.** Each construction examined has no
book to act on, or has no estimate that places its edge near the harness's Bar B, or has power ≪ 0.5 at the
declared 1.5%/yr worthwhile edge, or already failed its declared premium budget on our data (S-E). These are
screened out, not tested and rejected; a real edge remains possible.

**Ingests (build step 6) are re-sequenced, not cancelled.** FTD, Cboe, FRED/ALFRED and COT have no consumer that
can register a trial today. Family 8 (interactions) could use them as features but can start on the price data
already held; each ingest is taken up when a spec that needs it (family 8 included) passes this pre-check, still
behind its own admissibility probe, which may itself need a probe-scale ingest. A reopen event below permits a
spec; promotion still needs every programme gate (rule 3: matched control t > 3, DSR, BY-FDR, all stress cells).

## Family 4 — settlement / short stress
**As an avoid-list inside a held book.** The engine holds no long stock book: the core sleeve is one
evidence-approved instrument (`app/services/strategy_core_selection.py:1`, #2833); selection v2 has no runnable arm
(#3434); the S-I model pot is parked (#2842 comment 2026-09-23). Applied to an equal-weight universe instead, with EW
renormalisation over the remaining names, an avoid-list is the long-only selection signal below. **Reopens when** a
long stock book is a declared candidate; the avoid-list is then an arm of that book against the same book with a
declared replacement rule for skipped names.

**As a selection signal.**

| input | discovery (1990-01 → 2008-12) | validation (→ 2021-06-28) | admissibility still open | source |
| --- | --- | --- | --- | --- |
| FINRA short interest | none (starts 2017) | 2017 →, ≤ 4.5 calendar years | publication dates to rebuild; revisions | programme data table |
| SEC fails-to-deliver | 2004 → 2008, censored to balances ≥ 10,000 shares before 2008-09-16 | 2009 → | threshold re-applied after 2008-09-16 for a consistent variable; availability not recovered **by the probe's method** before 2020-12, a tail to 67 days and undated revisions after; CUSIP-keyed with no dated CUSIP↔series map, so absence mixes "no fails" with "no match" | #3384 slice 2a, "Clock" bullet and inputs 1–3 |

Power at the declared 1.5%/yr edge needs the arm's tracking error to its control, which needs data from before the
window being tested. SI has none before 2017, so its power cannot be sized; the ≤ 4.5 validation years would need
an annualised IR of **≥ 1.4** for 50% power at t > 3 (`3/√T`, unpurged; shorter usable coverage needs more),
against 0.87–0.94 for value's pre-window gross IR (`2026-09-26-2902-power-first.md`, Ken French data; a gross
figure, not a net long-only IR). FTD's tracking error could be measured in its thin 2004–2008 discovery, but only
after an ingest. What decides the budget is the mean: harness v1 exits every cohort at h (full round trips), so
Bar B needs **3.5–4.5%/yr** of active edge at h = 63 and **10.5–13.5%/yr** at h = 21 (hunt 1), and this pass
retrieved a direction, not a magnitude (two sources searched):

- Boehmer, Huszar & Jordan (2010), *JFE* 96(1), 80–97, NYSE/AMEX/NASDAQ 1988–2005: heavily traded stocks with
  **low** short interest earn *"statistically and economically significant positive abnormal returns"*, often
  larger in absolute value than the heavily shorted leg's negative returns (abstract; the hosted full-text PDF
  returned a suspended page).
- Stratmann & Welborn, *"Informed short selling, fails-to-deliver, and abnormal returns"*, *J. Empirical Finance*
  (2016): stocks with fails earn negative abnormal returns *"proportional to their FTD levels"* (abstract; SSRN
  full text 403). A high-FTD result; it gives no magnitude for a long-only FTD-screened book.

With no estimate that places the edge near Bar B, an ingest to measure tracking error buys a power figure for an arm
with no expected mean. Same disposition as #2903's insider ranking (`2026-09-26-2903-power-first.md`). A short leg
would add CFD borrow cost on exactly the stressed names the signal selects (unmeasured).

**Reopens when**, per source: an estimate for a long-only construction on that source (low SI for FINRA; an
FTD-screened long book for SEC fails), with its construction, comparator and sample stated and chosen without
validation-era data, puts the active edge at or above Bar B at its horizon; then the source's admissibility items
are resolved (probe-scale ingest allowed) and power at the declared edge is ≥ 0.5 on measured usable coverage. A
regime-conditioned family-4 construction reopens by the same event.

## Family 5 — regime conditioning, crash overlay, calendar timing
**Conditioning families 1–3** (family 4 is covered above). A gated arm holds the control's construction off-regime,
so off-regime it earns about the control's return, apart from switch trades, while still trading on the harness's
cohort schedule. Harness v1's stress cell charges **every arm position** the top band and the control its base band
(hunt 1, Bar B), so with active fraction `p` and in-regime active edge `E_in`, and turnover unchanged by the gate,
the mean condition is `p·E_in > (1.450 − base) × 252/h`: the edge needed in-regime rises to Bar B / p. Against the
tracker (Bar A) the gated arm still pays the full-year band, so `p·E_in > band × 252/h + 1.5` (hunt 1's form, with
g = 0): **gating lowers neither bar.** It can help a `PASS` in two ways only: removing an off-regime edge that is
negative, or cutting variance enough to clear t > 3. The conditioning variable the skill backs is volatility and
liquidity (`strategy-evidence.md` §2.10), and our reproduction of Nagel (2012) finds its surviving cell paying
**+40.0 bps gross per 20-day hold against a 50.9 bps round trip** (`.claude/skills/data-sources/market-structure.md`,
"Liquidity provision"; gross per hold, not arm minus control), so the one measured in-regime figure sits below the
base cost before any stress charge. VIX-slope and credit-spread conditioning are not bounded by it, and no published
in-regime magnitude for them was retrieved. **Reopens when** an estimate of `E_in` and regime occupancy `p` from data
the rule was not chosen on gives `p·E_in` at or above Bar B, with power ≥ 0.5 at the declared edge.

**Crash overlay on the core sleeve** (judged, per the programme, on drawdown reduction per unit of insurance cost).
#2837 (S-E, preregistered: 200-day MA on `spy_chain_v1`, 1993-01-29 → 2026-07-08, three offsets; result comment
2026-08-22) measured the canonical rule against its own declared bars: max-drawdown ratio **0.548–0.604**
(bar ≤ 0.667, pass) at **−2.92 to −4.65 pp/yr** CAGR (budget ≥ −1.5 pp/yr, fail; ≈ 1.9–3.1× over), 23.7–25.6% of
time in cash, with CGT and dividend drag charged and the premium **not decomposed**. That answers one rule, not every
overlay: another must cost about 51% or less of S-E's cheapest arm. Published routes checked: out-of-sample
volatility management generally earns lower Sharpe ratios than unmanaged outside momentum, profitability and BAB
(Cederburg et al. 2020, `strategy-evidence.md` §2.4; a return criterion, not drawdown per unit of premium), and its
up-scaling form needs leverage above x1 (barred). An index-CFD short instead of a sale keeps the holding but, at full
notional, makes the book 200% gross; #2844's fit for that is unsettled (programme doc, "What we can trade").
**Reopens when** a frozen rule, chosen without validation-era data, has power ≥ 0.5 to resolve the 1.5 pp/yr budget
over its test window (see the standard-error arithmetic below), or when #2844 permits a hedged book.

**Calendar timing (3c, referred by hunt 1).** McConnell & Xu (2008), *FAJ* 64(2), CRSP 1926–2005: the average daily
value-weighted return over the four-day turn of the month was **0.15%**, against **−0.001%** over the other 16
trading days; *"investors received no reward for bearing market risk except at turns of the month"* (abstract;
the sample ends before our validation window). At x1 with no leverage the arm can only hold less than full exposure
off the turn; let `w` be the fraction taken out on the 16 other days. Against the tracker (queue rule 2(b)), the
arm's active return is `w` × (cash yield minus the market's return on those days), minus its trading costs (12
round trips a year at `w`). Under the published mean the market term is about zero, so the expected edge is about
`0.8·w` × the cash rate, less costs; tax on a monthly sell-and-rebuy (30-day matching) is unmodelled. Its tracking
error is ≈ `w·√(16/20)·σ` ≈ 0.89·w·σ (assuming comparable daily variance and no serial covariance), so `w`
cancels from the IR: at σ = 15–20% (illustrative, not measured here) the IR at a 1.5%/yr edge is ≈ 0.08–0.11
against the ≈ 0.86 that ~12.2 validation years need, so **power ≪ 0.5: not run** as a return claim. As insurance
(w = 1), the standard error of its mean premium over ~12.2 years is ≈ 0.89σ/√12.2 ≈ 3.8–5.1 pp/yr, too imprecise to
resolve a 1.5 pp/yr budget near the boundary (and a CAGR premium, as S-E's bar is stated, adds compounding terms).
Pre-holiday timing uses the same exposure-switching algebra with fewer in-market days; its own returns and
frequency are not computed here. **Reopens when** a construction's tracking error to the tracker is small enough
for power ≥ 0.5 at a declared edge (at 1.5%/yr over ~12.2 years that is TE ≲ 1.75%/yr), or leverage above x1 is
permitted and an overweighting construction passes its own pre-check.

## What this leaves (programme build steps 6–7)
- Step 6: FTD, Cboe, FRED/ALFRED and COT (COT, like Cboe and FRED, serves family 5 conditioning) wait on the reopen
  events above or on a family-8 spec that needs them. Family 6 (MIDAS, validation-only and low power by the
  programme's own declaration) and Wikipedia are not assessed here.
- Family 8 (interactions trained on discovery) is expressible on the held price data; its spec runs hunt 1's
  pre-check table first. That table screens candidate constructions, not features: the programme bars univariate
  pre-filters of interaction-only features.
- Family 7 (eToro crowd extremes) waits on ≥ 12 months of #3381 recording.
- The programme's terminal verdict (rule 5) needs every family exhausted, including 6, 7 and 8 and the reopen events
  here; this document does not reach it.
