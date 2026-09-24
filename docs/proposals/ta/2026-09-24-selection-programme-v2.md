# Selection programme v2: cheap, profitable companies held for quarters, on a 15-year point-in-time test

Refs #2899, #2900–#2904, #2908, #2437. Sequences the 2026-08-23 decision ("fundamentals/ownership reopened
for SELECTION", `docs/settled-decisions.md`) into buildable steps. It does not reverse any settled
decision. Framing reviewed by Codex ckpt-1 (115 findings, 2026-09-24); the corrections are folded in below.
Each build step still gets its own spec + ckpt-1.

## Where we actually are
- #2900's PIT spine passed (2026-08-24), and #2908's exclusion screens ran on it. **Verdict: FAIL against the
  identical annual equal-weight control** (`2026-08-24-r6-exclusion-result.md`): every exclusion lost to plain
  annual 1/N on the same dates and costs. The apparent edge over buy-and-hold was **annual refresh**, not the
  screen. The R6 sleeve verdict stands at £0 (`2026-08-24-r6-sleeve-verdict.md`).
- That test had **27 months and three formations** (2022–2024), because the frozen bundle carries only
  identity, shares and red flags. **No quality, valuation or ownership arm (#2901–#2904) has been run.**

## The binding lesson from #2908 → the control, before anything else
Every arm below is judged first against **the identical construction without the signal**: the same PIT
universe, formation dates, rebalance, weighting, cost model and termination rule, but annual 1/N. Beating
literal buy-and-hold or the index is reported, **never sufficient**. Cap-weighted market and net buy-and-hold are
secondary comparators, named explicitly and never swapped.

## Arms, in the order settled on 08-23 (quality → valuation → ownership → combination)
Each arm freezes its headline construction, definition, universe, weighting, rebalance month, name count, ties,
missingness and exclusions **in its own declaration before first look** (#2829 register). The nested/combined
variants count as trials in the deflation, never as free choices.

| arm | signal (literature) | evidence caveat that binds us |
| --- | --- | --- |
| #2901 quality | gross profitability GP/A (Novy-Marx 2013) | survives HXZ at t 2.63 and NMV net at t 2.74. **Below our own t > 3 bar**, so the prior is modest |
| #2902 valuation | value alone, then value × GP/A **as the NMV summed-rank construction** | NMV "ValProf" net 0.77%/mo is the **long-short** spread (net FF4 alpha 0.49%), not long-only excess; value lost ~55% vs market 2007→2020 |
| #2903 ownership | net share issuance (Pontiff–Woodgate; HXZ t 4.47); opportunistic insider net buying (CMP 2012) **ranked at the periodic formation**, not traded on the event | CMP's 82 bp/mo is a monthly event portfolio. A quarterly selection filter is a different, untested claim |
| #2904 combination | only arms that beat their own control | preregistered combination rule; construction variants declared, not swept |

**Insider data needs no new operator decision:** "ownership as selection" was reopened on 08-23, and the DERA
insider quarterly ZIPs (2006+) are already on disk. The work is parse, normalise, and an admissible research
path. CMP's routine/opportunistic label must use strictly backward insider history.

**Dropped, with reason:** G-score (the return is on the short side), accruals (decayed), Magic Formula and
net-nets (book/practitioner evidence; capacity), Lazy Prices (weak replication), Piotroski as a standalone
(fails HXZ). **Beneish deferred**: the bundle fields cannot compute it (receivables, PPE, depreciation, SG&A
missing), and #2908 already showed an exclusion screen losing to its control.

## The data unlock, and the rules that make it honest
`companyfacts.zip` is already on disk (1.3 GB, 20,396 filers, daily refresh). Every fact carries `accn` +
`filed`; 2,970 of the 3,318 Form-25 delisted issuers (89.5%) are present.

**Construction rules** (step-1 spec must satisfy all; each maps to a Codex finding):
- Availability = the fact's own accession's acceptance, **strictly before** the decision session
  (`research_point_in_time.py:270` contract). Never `period_end`, never `filed ≤ D`.
- **Filter by availability first, then take the latest**. Never "latest winner, then backdate".
- **Never use `frame`** (it marks the last-filed copy; Apple FY2020 NI carries `frame` only on the 2022
  comparative). Never treat `fy`/`fp` as the fact's own period: they describe the containing filing.
- Key = (CIK, taxonomy, concept, unit, start, end, instant/duration, accn). Flows are rebuilt causally
  (no mixing of amended YTD with original quarters). 10-K/A / 10-Q/A get an explicit rule (partial
  amendments are not full replacements).
- Declared concept-alias map, sign/unit checks, and no silent fallback (missing GP ≠ 0).
- **Coverage census BEFORE any look**, by year × size × survival outcome (failure / acquisition / live),
  counting required-field coverage at each formation, not any-file presence. Missingness is plausibly tied to
  distress, the very trait these screens exploit, so complete-case selection must be shown not to manufacture
  the result.
- XBRL phases in 2009→2011: the test window starts **2011-06** (first formation with full-mandate coverage),
  and the price corpus ends **2024-09-27**. It excludes 2000–02 and 2008, so no claim is made about those regimes.

**Security linkage:** CIK is an entity, not a security. Dated, security-level linkage (classes, ADRs,
successors, ticker reuse). `submissions.zip` tickers are not a history. Abstentions get a census by year ×
reason × size × outcome; the universe never depends on today's broker list or eventual survival.

**Termination:** use `series_termination.py` exactly as it defines the classes (failure −55%; unknown 0% vs
−55%; conversion/Q-suffix 0%), **plus the #2908 zero-recovery worst case**, applied identically to arm and
control. The verdict binds to the worst case.

## Statistics and bar
- Headline arm and statistic frozen per declaration; monthly returns; dependence-robust inference (overlapping
  holds are not independent bets); **deflated for the whole declared trial family**, including #2908's prior
  cells; t > 3 on the headline difference versus its control (Harvey/Liu/Zhu).
- **Power statement before the look**: the minimum detectable annual excess at 13 years. Non-significance is
  reported as "undetermined at this power", not as "no edge".
- Cost model: the #2908 basis (0.725% half-spread per traded dollar) plus minimum-notional and broker-reachability
  checks at formation; turnover measured one-way on the declared denominator.

## LLM: forward-only, as a feature, never as the picker
Evidence (research notes on #2899): credible use turns text into a feature. Pre-cutoff backtests are
contaminated (memorisation; masking fails). Local 7–14B models make unit/scale errors. The flagship "LLM beats
analysts" paper was withdrawn in 2025.
- Never for numbers (XBRL has them). Never backtested before the model's cutoff.
- Candidate: an AVOID feature from NEW 10-K/10-Q text (MD&A, risk factors, controls, auditor report, legal
  proceedings), compared with the same section of the prior filing under a frozen comparison rule.
- Frozen identity = weights + tokenizer + quantisation + runtime + decoding + prompt + section extractor.
  Labelled extraction validation with evidence spans and an abstain state **before** use.
- Controls on matched portfolios: no exclusion / deterministic text rule / LLM rule. Avoided names keep shadow
  outcomes; fixed replacement rule. **The feature may only act at the periodic formation, never trigger a trade
  on a filing** (that would reintroduce event trading).
- Starts after an arm survives; it is a long, low-power experiment and is declared as such.

## Build order (tickets)
1. **PIT fundamentals bundle from companyfacts**: construction rules + census + leak tests (restatement,
   amendment, missing-original fixtures). Content-addressed like #2900.
2. **Dated security linkage** + abstention census.
3. **Termination wiring** as above.
4. Declare and run **#2901** → **#2902** → **#2903** (insider parse/normalise first) → **#2904**.
5. Any survivor → paper on demo (`strategy_deployments`), inside the #2844 sandbox, with its own exit design
   (#3284's guard is calibrated to SPY and is not transplanted blindly).
6. LLM avoid-feature, forward-only, beside step 5.

## What we will not do
LLM stock picks; pre-cutoff LLM backtests; more timing strategies; a screen judged only against buy-and-hold;
parameter sweeps after the first look; tight TP/SL turning a quarters-long hold into a timing rule.
