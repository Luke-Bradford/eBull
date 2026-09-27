# Re-screen of hunt families 1–4 under the corrected bar: no trial

Refs #2437 (supervisor correction 2026-09-27 21:50Z, and the 23:35Z queue note), #3448 (hunt 2, closed),
#3387 (hunt 1, closed). Programme doc: `2026-09-25-pattern-hunt-programme.md`, families 1–4.
Security: none (a research document; no code, broker, auth or order path).

**Status:** v2, after Codex ckpt-1. v1 drew 48 findings. The main correction is that v1 claimed arithmetic
impossibility; the evidence supports only a budget decision. Also applied: the grid length is traced, the band is
round trip, and the omitted constructions are added (liquidity provision, low-SI long, pre-holiday, lower-turnover
variants, combinations). No trial is registered and no research return is read. Inputs: published figures already
cited in earlier pre-checks, one abstract retrieved here, and arithmetic on the frozen cost bands and the door's
power formula. Nothing here enters M.

## Result
Families 1–4 are the ones #2437 lists as dropped at design stage: overnight/intraday, liquidity/volume shocks,
scheduled flows, and the FTD/SI avoid-list. Under the corrected bar, **no available estimate puts any of their
constructions at the bar.** Each published point estimate falls short of condition (i) below, most of them already
at the cheapest base band. Where no magnitude exists, the construction has no expected mean to fund.

**Decision (a budget choice, not a proof): no trial is registered.** This is the same disposition as hunt 1. A real
edge larger than its published estimate remains possible and unexamined. Registering a trial whose best estimate
is below the bar still adds 1 to M, and that raises the bar for every later trial.

## The corrected bar, as a pre-look screen
The bar is #2437 SC1 as defined in #3448 and evaluated by its discovery runner: `excess_ann` = 252 × mean(arm −
SPY total return) ≥ 0.08 in every base **and** stress cell (hunt 2 spec, "The bar"). ⚠ The validation-side
`hunt_gate` block is not built (`hunt_door.HUNTS_AWAITING_GATE`). Two conditions follow from the bar. Each is
evaluated here on published estimates.

**(i) Mean.** Take a book whose cohorts are fully replaced every h sessions. It makes 252/h round trips a year.
`cost_model.BANDS` holds round-trip spreads: each side pays half the band. The stress cell charges arm positions
max(base band, 1.450) (hunt 2 spec, "Net of tariff and spreads"). Its gross excess over SPY must cover roughly:

> 8 + band × 252/h (%/yr, arithmetic, full-book occupancy)

| h | base drag, ≥ $100 (0.322) | stress drag (1.450) | gross excess needed, base | gross excess needed, stress |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 81.1 | 365.4 | 89.1 | 373.4 |
| 5 | 16.2 | 73.1 | 24.2 | 81.1 |
| 20 | 4.1 | 18.3 | 12.1 | 26.3 |
| 21 | 3.9 | 17.4 | 11.9 | 25.4 |
| 63 | 1.3 | 5.8 | 9.3 | 13.8 |
| 126 † | 0.64 | 2.90 | 8.64 | 10.90 |
| 252 † | 0.32 | 1.45 | 8.32 | 9.45 |

Notes on the table:
- † Harness v1 caps h at 63. These rows are hypothetical extensions, not executable constructions.
- Every band other than ≥ $100 is dearer.
- The figures are approximate. At an unchanged price, fill accounting makes a round trip cost s/(1 + s/2); at
  h = 1 that gives 81.0 and 362.8.
- A book that retains names, rebalances partially or holds cash has lower turnover and different benchmark-relative
  returns. The table does not bound such books. They are covered under "Variants" below.

**(ii) Power.** The door's formula (`hunt_door.power_statement`) is MDE = (3 + z) · √(Ŝ_NW / T) · 252, with
z₈₀ = 0.8416 (`hunt_door.POWER_Z`). Write TE_NW = √(252 · Ŝ_NW), where Ŝ_NW is estimated on discovery data and
carried forward. Then MDE₈₀ = 3.8416 · TE_NW / √(T / 252).

T is the validation **grid**: 3,079 sessions (hunt 2 spec, line 253). The split itself has 3,143 SPY sessions
(line 103); the grid is shorter by the 63-session embargo and the lag (lines 32–33). T/252 = 12.22 years, so
MDE₈₀ ≤ 0.08 needs

> TE_NW ≤ 0.08 × √12.22 / 3.8416 ≈ **7.28%/yr** (7.35% if the full 3,143 were used).

What this implies:
- A construction at the bar with TE at the cap has a **design IR** (expected excess ÷ discovery TE) of
  8 / 7.28 ≈ 1.10. A lower TE requires a higher design IR.
- This is a property of the discovery-derived design, not a requirement on the realised validation IR.
- The door can still refuse for other reasons: missing or degenerate discovery data, or a short target sample.
- Hunt 2's excess-series MDE₈₀ was 0.210 (hunt 2 spec, "Outcome"), which implies TE ≈ 19%/yr.

**Two identities used below.** Both are exact under the evaluator's accounting of whole-session holdings (idle
cash earns 0%, hunt 2 spec "Utilisation"), before costs.
- **Timing the tracker at x1.** A book that holds SPY on the set D of sessions and cash otherwise has excess
  = Σ_D r − Σ_all r = −Σ_out r. To reach +8%/yr it must be out on sessions whose return sums to −8%/yr or less.
  Equivalently, the in-sessions must earn ≥ 8% + the tracker's own annual return.
- **Excluding names from the tracker.** A book equal to the tracker less names of weight w_t, reweighted onto the
  rest, has excess ≈ 252 · mean(w_t · u_t), where u_t is the rest's return minus the excluded names' return. At
  fixed w it needs u ≥ (0.08 + its costs) / w.

## Per family
The published figures are four-factor alphas or returns against a reference portfolio, not excess over SPY. Each is
used as the best available point estimate, as hunt 1 did. The reference-minus-SPY term is unknown, in either
direction.

| # | construction | best available figure | disposition |
| --- | --- | --- | --- |
| 1a | single-name overnight-only book (h = 1) | `strategy-evidence.md` §0 (line 114): an overnight drift of ~4–5 bps/day "in every decile", i.e. 10–13%/yr. That is a holding return, not excess over SPY | (i) at base needs 89%/yr of gross excess. §3.2's kill table already records the family: "die at our spreads" |
| 1b | index overnight-only book | Cliff, Cooper & Gulen (2008), SSRN 1004081, abstract (retrieved 2026-09-28): the US equity premium over the decade to 2008 "is solely due to overnight returns", and returns during the day are "close to zero and sometimes negative" | The timing identity gives excess ≈ −(1 + o)·i, where o is the overnight return and i the intraday return. The **SPY** form pays the base band 252 times (81%/yr). The **SPX500 CFD** form pays 0.007% per side (programme doc, "What we can trade"), i.e. 3.5%/yr, plus spread and index overnight financing, which is unpriced. It needs SPY's intraday mean ≤ ≈ −11.5%/yr. The abstract gives a direction ("close to zero"), not a bound. That is a budget decision, and harness v1 cannot express the arm either (hunt 1 pre-check, family 1) |
| 2a | high-volume premium, h = 20 | Gervais, Kaniel & Mingelgrin (2001), via hunt 1 (Table 2): 5.7%/yr small, 3.7%/yr large, against a reference portfolio | (i) at base needs 12.1 |
| 2b | high-volume premium, h = 63 | no 63-day or single-leg estimate. The nearest is ~1% per dollar long after 100 days, both legs combined, small and medium stocks (hunt 1, 2b) | no prior to fund. A 63-day long leg at 9.3 (base) would be a new hypothesis, not a replication |
| 2c | liquidity provision (short-term reversal, volatility-conditioned) | our reproduction of Nagel (2012): +40.0 bps gross per 20-day hold, against a 50.9 bps round trip (families 4–5 pre-check, citing `market-structure.md`) | about 5.0%/yr gross, below base cost before the bar. VIX-slope and credit conditioning have no published in-regime magnitude (same doc) |
| 3a | dividend-month premium, all payers, h = 21 | Hartzmark & Solomon (2013), via hunt 1: 4.9%/yr four-factor alpha | (i) at base needs 11.9 |
| 3b | dividend-month premium, semi-annual payers, h = 21 | same source: 13.8%/yr four-factor alpha | Clears base at the ≥ $100 band (11.9) by 1.9 points; the $5–20 band needs 14.9. Fails stress (25.4) by 11.6. Scenario: McLean & Pontiff's average decay, 58% across predictors (not measured for this one), leaves 5.8. H&S's estimation runs to 2011, overlapping validation from 2009, so this is not a choice free of validation-era data. Hunt 1's population, ADR and dividend-treatment caveats all carry over. TE is unmeasured and needs a look |
| 3c | turn-of-month timing of the index | McConnell & Xu (2008), via the families 4–5 pre-check: CRSP value-weighted 1926–2005, 0.15%/day over the 4-day turn against −0.001%/day over the other 16 | By the timing identity, being out on the 16 days gains ≈ 0.001% × 252 × 16/20 ≈ 0.2%/yr before costs. A CRSP estimate, not SPY; the sample ends before validation |
| 3d | pre-holiday timing | no pre-holiday magnitude retrieved (the families 4–5 pre-check did not compute one) | The timing identity needs ~9 pre-holiday sessions a year to earn ≥ 8% + SPY's return: 16.2%/yr against discovery's 8.18% (hunt 2 spec, line 245), i.e. ≈ 1.8% per pre-holiday session. No estimate funds it |
| 4a | FTD/SI avoid-list applied to the tracker | direction only: Boehmer, Huszar & Jordan (2010) and Stratmann & Welborn (2016), via the families 4–5 pre-check | By the exclusion identity, u ≥ 800 / 160 / 80 / 40%/yr at w = 1 / 5 / 10 / 20% (illustrative weights; the SPY weight of affected names is not measured). A larger w is a selection book, not an avoid-list; that is 4c. No magnitude funds it |
| 4b | short book on high SI or FTD | same | Excess = −r_names − r_SPY before costs, so the names must fall by ≥ 8% + SPY's return (8.18%/yr discovery, 15.87% validation; hunt 2 spec lines 245 and 249), plus hard-to-borrow fees on exactly these names |
| 4c | long selection on low SI, or FTD-screened | BHJ (2010): low-SI, heavily traded stocks earn positive abnormal returns, often larger than the high-SI leg's (abstract, 1988–2005). No long-only magnitude | Our FINRA series starts in 2017, so SI has no discovery period and cannot be frozen under rule 1. FTD has a thin, censored 2004–2008 discovery, which needs an ingest. Its reopen event is the families 4–5 pre-check's, unchanged |

**Variants and combinations.** Some variants cut turnover: selective overnight names or dates, retained dividend
payers, partial calendar tilts. They lower the drag toward the h = 63 row, but no published estimate exists for any
of them, so none is funded. Combining sleeves can lower tracking error, but not raise the mean: each component
still needs a gross excess meeting (i) at its own turnover, and none above has one.

## What remains
- The historical route has now been screened under both bars:
  - harness v1's Bar A/B, in hunt 1 and the families 4–8 pre-checks;
  - the corrected 8%-over-SPY bar, in this document and hunt 2.
- For context only: value's pre-window gross IR was 0.87–0.94 (`2026-09-26-2902-power-first.md`, Ken French data).
  That is a long-short factor, not a long-only book against SPY, and it sits below the 1.10 design IR.
- **Reopens when** a construction chosen without validation-era data has a published or discovery-derived gross
  excess over SPY that meets (i) at its turnover, **and** an expected discovery TE_NW to SPY ≤ 7.28%/yr.
- The earlier pre-check reopen events stand. Family 7 (eToro crowd) keeps its own wake: ≥ 12 months of #3381
  recording.
- **Next queue item** (#2437 comments of 2026-09-27 21:50Z and 21:55Z): the #3423 Invest page. The API was
  restored at 21:55Z.
