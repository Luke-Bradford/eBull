# Historical R6 programme verdict before point-in-time recovery (#2899)

> **SUPERSEDED 2026-08-24. Do not use this document as the current sleeve
> specification.** #2900 was subsequently repaired and passed its adversarial
> leak test, and #2908 then completed a frozen full-population cycle. The
> authoritative verdict is
> [`2026-08-24-r6-sleeve-verdict.md`](2026-08-24-r6-sleeve-verdict.md): R6 still
> receives £0, but because its exclusion overlay lost to identical annual 1/N,
> not because no arm could be measured.

The remainder records the earlier 2026-08-23 gate verdict for audit history.
Its present-tense claims must be read as historical statements at that date.

Historical verdict as of 2026-08-23: **NO DEPLOYABLE R6 SLEEVE — ALLOCATION £0**.

This is the programme's permitted no-sleeve ending, not a strategy failure or
a zero-return backtest. The point-in-time gate failed before any Tier 2 arm
could lawfully be measured. Opening an arm on the current data would use
irrecoverably overwritten historical inputs and would manufacture evidence.
R6 therefore remains off and holds no real or paper capital.

## Sleeve specification

| Property | Specification |
| --- | --- |
| Holds | Nothing. Target and maximum R6 allocation are both **£0**. |
| Excludes | Every instrument and every Tier 2/Tier 3 R6 arm from allocation. This is an evidence exclusion, not an adverse view on those instruments. |
| Account | None. The eToro Stocks & Shares ISA has no established public-API order route; the ordinary taxable eToro account is only a prospective venue after all evidence and execution gates pass. |
| Weighting | N/A. Equal weight remains the required default for any future survivor. |
| Rebalance | Never while this verdict stands. No R6 orders may be created. |
| Turnover | 0%. |
| Actual sleeve cost | **£0**, because there are no holdings or trades. The #2907 spread figures are diagnostics for a hypothetical future arm, not costs incurred by this sleeve. |
| Evidence window | N/A. No admissible historical ranking window exists under the current contracts. |
| Net return | N/A, not 0%. No arm was measured. |
| Buy-and-hold return | N/A. There is no same-window sleeve result to compare. |
| 15% haircut | N/A. No published effect was applied to an arm. |
| 58% haircut | N/A. No published effect was applied to an arm. |
| Turn-off rule | Already off. Any non-zero allocation, holding, rebalance or order is a contract breach and must fail closed. |

The separately queued low-cost market sleeve in #2437 Phase 1 is not an R6
survivor and is not authorised by this verdict. It may be evaluated under its
own mandate; it must not be relabelled as a successful R6 arm.

## Evidence ledger

### Tier 0

1. **#2915 — tax wrapper: fact established.** As of 2026-08-23, no supported
   public-API route from eBull to eToro's Moneyfarm-powered Stocks & Shares ISA
   was established. API-reachable ISA allocation is **£0** and an exact
   instrument eligibility catalogue is unavailable. The ordinary taxable
   account remains the prospective route. The pinned £50,000 sensitivity shows
   tax minus specified ISA FX ranging from **-£471.25 to +£808.75 in the first
   year**, depending on turnover and tax-rate scenario; it is not a personal
   tax estimate or return forecast.
2. **#2912 — factor construction: PASS.** On the declared overlapping windows,
   eBull versus French produced correlation `0.665267` and beta `1.044071`;
   eBull versus AQR produced correlation `0.649738` and beta `0.981939`; the
   internal control correlation was `0.917444`. This validates construction
   identity, not investability or alpha.
3. **#2900 — point-in-time spine: FAIL, no admissible historical field.** A
   genuinely post-decision insert left the pinned historical input hash
   unchanged, but overwriting a pre-decision natural key changed it from
   `f1d7469066740918f5102df79e374cdeb3c40f15f73bc97656bbecf0e10cec7d`
   to
   `6eb77456146f2514e8bf800e034aee5477028e1f40ad3e329fcb8ed275875d50`.
   Zero predecessor rows were recoverable. The leak test therefore fails
   loudly and every current R6 ranking read is refused.

The programme asked for a point-in-time path with a passing leak test. That
condition does not exist. This is the decisive reason a money-holding sleeve
is not justified.

### Tier 1

1. **#2907 — cost avenue survives only as a diagnostic.** The full population
   was 6,773 US-dollar instruments, but stored nominal-price coverage was only
   `14.262513%`. On the priced microcap subset, the p75 spread was `1.450000%`,
   one-round-trip loss `1.439563%`, and three-round-trip loss `4.256818%`.
   Applying the declared literature ceilings left `+12.263669%` at the 15%
   haircut and `+3.906246%` at the 58% haircut. That means costs alone did not
   kill the avenue; it establishes neither absolute return nor outperformance.
2. **#2914 — operational rules: PASS.** A `-3..+3` venue-session turn-of-month
   preference was installed without order authority or added turnover. The
   accepted reference corpus contained 38,017 typed observations but **zero
   genuine valuation-spread series**, so factor valuation context is explicitly
   unavailable and factor returns cannot be relabelled as valuation.

## Why there is no arm verdict

No Tier 2 arm was opened, inspected or backtested. #2900 is an admissibility
gate before #2908, the first arm. Because historical source versions have been
destroyed by same-key overwrite, neither a preregistration nor a full-population
run can make the resulting ranking point-in-time. Calling such a run an arm
cycle would violate the programme's explicit instruction not to approximate
point-in-time data.

Accordingly, the requested "at least one arm" artifact cannot be supplied
truthfully under the current data contracts. The absence is the evidence-backed
programme verdict: **no R6 sleeve is justified**. No 15% or 58% result, net
return, or buy-and-hold comparison has been inferred or manufactured.

## Conditions to reconsider

R6 remains off unless all of the following occur in order:

1. The relevant source writers retain immutable source versions (or an
   equivalent reconstructible bitemporal history) for every field consumed by
   an arm, including historical population and identity.
2. #2900 is rerun over the full declared registry. A later-data insert and a
   same-natural-key correction must both leave a frozen historical ranking and
   its input hash unchanged, while the prior version remains recoverable. Any
   movement refuses the read and keeps R6 at £0.
3. Only then is #2908 declared, frozen and hashed before its first result. It
   must use the full survivorship-free population, charge costs and carry,
   check turnover first, and report both the 15% and 58% haircuts.
4. An arm passes only with positive absolute net return and positive net return
   versus buy-and-hold on the identical pinned window. A complex construction
   must also beat equal weight on identical inputs, and overlapping signals
   must not be shipped twice.
5. A survivor must then pass paper/live promotion, sandbox, execution-guard,
   kill-switch, ordinary-account eligibility and tax-reporting gates. ISA use
   additionally requires a documented authenticated ISA writer and a complete
   instrument-level eligibility census.

Until every applicable condition passes, the machine-readable capital policy
is simply: **R6 allocation = £0; R6 trading = refused**.
