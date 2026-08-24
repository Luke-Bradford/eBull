# R6 #2908 exclusion-screen preregistration

Status: **FROZEN BEFORE THE FIRST ELIGIBLE-WINDOW RETURN OR ARM RESULT**

This declaration carries the first R6 Tier-2 ticket through one complete cycle. It is not permission to tune a
failed construction. The primary arm is dilution exclusion. Filing-risk exclusion and the union are declared
secondary outcomes and will be reported regardless of sign; they cannot replace a failed primary merely because
they look better.

One 1999 `A.csv` row was displayed while confirming that the headerless vendor schema has nine columns. It is
outside every declared formation and outcome window, was not used to calculate a return, and conveyed no #2908
result. Before this freeze, code read only filenames, date columns, SEC facts, and published documentation/header
metadata inside the eligible window.

## Frozen evidence identity

- PIT manifest SHA-256: `59e49fde33977da749310151d0f35697fa20dc8717d7fe63dd1688e7a20cf98a`
- PIT payload SHA-256: `7423e05dae3896340ccf460f5614be38901a0d6fa26bf49360f8ea92b9ec95d6`
- SEC cover census SHA-256: `17880dd452c43737a4997314bbb7a2788afb06153341ff690ea460b373d14ee4`
- retained raw SEC cover-instance TAR SHA-256:
  `19d3d217b46a501875c7e8eaae8ea878313151157e1dc614f5838f2515e99d87`
- same-filing share census SHA-256: `56b8193713f3e790940b82231fb88544611e1631053f47cd970277bd343f65bd`
- 90-day filing-risk census SHA-256: `3a00a1ca2f3ef15fddec142b651d6d8e4015371731f3cb0b5ba57028bdf5945d`
- SEC submissions ZIP SHA-256: `928d67221c6e6183bc343e7234c1391448c15cd1dd644d36b425db2f99ba4350`
- global-q investment-portfolio ZIP SHA-256:
  `b17242f741c2edf1df12084c6c040adbd2df2fa1012a92a162144cac49af0c82`
- global-q technical PDF SHA-256: `15635d76f74919d9f6f3a08fba0ec50bf2c483fcf8d3d0e71b2c5a8484d96113`
- price metadata census SHA-256: `25415c00481c2ebea9e17e828de531b40ab3ed9bfd3dffa80455af42da816b5e`
- clean Intrader mirror commit: `3dbfda5ca1ccecda443fd8979671fcfe47bc2a5c`

Frozen implementation SHA-256 values:

- `app/services/r6_pit_bundle.py`: `1366d2d018fb350372f70ad5956e9f36b114a9858d85213741084209dcfc687f`
- `app/services/r6_pit_universe.py`: `87c54e053d9a89a2a8ef63a65e3f9a14c1c56edecdc190ae13f6676e7116ae28`
- `app/services/r6_dilution_exclusion.py`: `2805c9ddaa3a0c9a22a9b20866496eaa4abec09ebdd84a7c77d461a3078bc78c`
- `app/services/r6_exclusion_trial.py`: `aaa64784507b8519f3cd8d0d4714263ee94fc6792c7dc213eca78ced2c3a6329`
- factor-gate runner: `e6ba7b8dff41d3d2d927a2a14c775b54caccfb19c0d966ac0d2420e139aecaeb`
- outcome runner: `9baaf5336814e8c631d81f27667ae4be154a1868ec8cf388dc732e2b45c10a0f`

Any change to a frozen source, classifier, signal, evaluator, cost, window, or failure rule requires a written,
hashed correction declaration before another result is opened. The original result remains evidence.

## Point-in-time and full-population contract

Formation closes are `2022-06-30 16:00`, `2023-06-30 16:00`, and `2024-06-28 16:00`, using the SEC/New-York
naive clock. The bundle loader requires the exact manifest hash, refuses symlinks, refuses changed/repointed
payloads, and rejects identity or share accessions later than formation. The real adversarial test passed at all
three formations: a later ingest left ranks byte-identical and a rewrite was refused.

The population is every unambiguous security class that satisfies all fixed rules, never a chosen sample:

1. latest fiscal-period annual SEC accession public by formation; a same-period earlier accession is used only
   when the newer accession is fetched and proves to lack a complete same-context cover identity;
2. SEC `Security12bTitle`, `TradingSymbol`, and `SecurityExchangeName` share one XBRL context;
3. supported listed venue and an ordinary/common-share title; warrants, units, rights, preferred/debt,
   depositary classes, beneficial interests, funds/trust shares, unknown titles and ambiguous symbols are excluded;
4. the exact formation session exists in the pinned price mirror; and
5. the first subsequent observed bar is within seven calendar days and all admitted names share one execution
   session. The date-only census found 5,102 / 4,993 / 4,771 executable classes. The 4 / 8 / 7 classes with no
   later bar are reported as unfillable, not silently assigned a return.

The PIT populations are 5,106 / 5,001 / 4,778. Same-filing comparative share pairs exist for
3,148 / 3,237 / 3,208; all missing pairs remain neutral holds. No current projection, inferred filing date,
unfetched fallback, arbitrary symbol substitution, or survivor-only end-date filter is allowed.

## Frozen constructions

`Nsi = log(current common shares / prior-year common shares)`, using the two non-dimensional, non-segmented
`CommonStockSharesOutstanding` facts from the same accepted annual filing. This avoids applying a later split to
only one endpoint.

Following global-q's published shape, negative Nsi receives portfolios 1-2, zero receives portfolio 3, and
positive Nsi receives portfolios 4-10, with NYSE breakpoints. The unpublished finite-sample rule is fixed as the
lower-inclusive nearest-rank order statistic. Portfolio 10 is the dilution exclusion. Missing share pairs remain
included.

The filing-risk arm excludes an issuer only when its complete retained 90-day history has
`AVG(non-null severity) > 0.60`; incomplete history is a neutral hold. Critical 8-K filings score `1.0` and NT
filings score `0.7`. The union excludes either set. Dilution/red-flag intersection and Jaccard overlap are reported
at every formation. Quality overlap is explicitly unavailable until ordered ticket #2901 exists; therefore no
survivor from this ticket can be called independent of quality or deployed before that later overlap gate.

## Published-factor identity gate

Before the arm outcome runner may execute, the factor-only runner compares eBull's monthly equal-weight Nsi
portfolio-10-minus-portfolio-1 return with global-q's published value-weight portfolio-10-minus-portfolio-1
return. Membership is fixed July-June from each June formation. The pinned comparison is July 2022 through
September 2024 (27 possible months), gross of strategy costs. Published returns are parsed as percent per the
technical document.

The construction passes only if there are at least 24 overlaps, contemporaneous Pearson correlation is at least
`+0.20`, OLS beta in `ebull = alpha + beta * reference` is positive, and absolute contemporaneous correlation is
at least both absolute ±1-month displacement correlations. Failure is a code/data alignment bug, not a market
finding; arm outcomes remain sealed until a correction is separately frozen.

## Outcome, benchmark, turnover, and cost

- Execution: equal-dollar target weights at the common first post-formation session's adjusted open:
  `raw open * adjusted close / raw close`. Rebalance annually. Final liquidation is at adjusted close on
  `2024-09-27`.
- Return basis: Intrader/Yahoo-derived split-and-dividend-adjusted wealth. Invalid/non-positive vendor rows are
  excluded mechanically and counted. A missing in-series execution session fails loudly.
- Primary: annual equal-weight full population less Nsi portfolio 10.
- Secondary: the filing-risk exclusion and union, both reported without winner selection.
- Comparators: (a) literal equal-weight buy-and-hold of the entire executable 2022 population with no intermediate
  rebalance, and (b) annual equal-weight full population on identical dates and inputs.
- Turnover: at each rebalance, trade every dollar difference between current and equal target market value.
  Removed holdings are fully sold and entrants fully bought. The evaluator solves target wealth after cost and
  asserts cash conservation. It reports traded notional, initial purchase, annual rebalances and final sale.
- Spread: frozen maximum split-adjusted band, `1.450%` quoted spread (`h = 0.725%` each traded dollar). A flat
  round trip loses `1 - (1-h)/(1+h) = 1.439563%`. Zero commission and structural-zero carry apply to long/x1 real
  USD holdings. No borrow or leverage is allowed.
- Termination: report both (best) last adjusted close frozen until the event and (worst) zero recovery when a
  series ends before valuation. Charge the sell half-spread to recoveries. The worst case governs capital.

The nominal research window is the first execution session after `2022-06-30` through `2024-09-27`. Every result
must state these dates; no single-year subwindow or recent-performance reorder is permitted.

## Haircuts and verdict

For each strategy, termination case, and haircut `d in {0.15, 0.58}`:

1. compute gross strategy return, net strategy return, gross buy-and-hold return and net buy-and-hold return;
2. set gross edge to `strategy gross - buy-and-hold gross`;
3. multiply a positive gross edge by `(1-d)`; leave a non-positive edge unchanged so decay cannot rescue it;
4. subtract the strategy's full measured cost drag from the adjusted gross return; and
5. pass only if adjusted absolute net return is strictly positive **and** strictly above net buy-and-hold.

All cells are published. The primary is `PASS_ROBUST` only if the dilution exclusion passes the worst termination
case at 58%; it is `PASS_CONTINGENT` if it fails 58% but passes worst-case 15%; otherwise it fails. A contingent
pass receives no capital. Secondary cells cannot promote themselves over a failed primary.

## Capital and turn-off boundary

#2915 established no supported public-API route into eToro's ISA, so ISA allocation is exactly **£0**. Research
returns are USD; a separate `0.70%` conversion sensitivity is **£350 each way on £50,000**, and repeated FX
conversion is forbidden.

Even a robust statistical pass is not yet deployable. Before non-zero ordinary-account capital it must also:

- map at least 95% of target weight uniquely to currently validated, BUY-enabled eToro underlying instruments;
- fit eToro's documented minimum notional at the declared equal weights for a maximum **£50,000** sleeve;
- pass the later #2901 quality-overlap gate, since the published q-factor caveat predicts shared information;
- run through the existing sandbox, execution guard and kill switch, with paper reconciliation first; and
- preserve the same annual rebalance and equal-weight rule without selecting a prettier subset.

Failure of any operational condition produces an evidence-backed **£0 sleeve**, even if the historical return cell
passes. If eventually deployed, turn it off before the next order on PIT/hash failure, source or broker mapping
drift below 95%, a kill-switch/execution-guard refusal, a missing annual rebuild, a cost assumption worse than the
frozen maximum band, or a rolling preregistered live result that loses either positive-absolute or buy-and-hold
status. Historical underperformance alone between annual decisions does not authorize discretionary abandonment.
