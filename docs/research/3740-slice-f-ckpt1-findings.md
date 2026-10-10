# #3740 slice F — Codex checkpoint 1 findings (verbatim)

Kept so the next round can check dispositions against the exact text. Spec: `2026-10-10-3740-slice-f-forward-capture.md`.

## Round 1

**The design is not ready to build.** I read the full target, all requested local ranges, and the probe script, and checked relevant primary-source documentation. I did not edit files or rerun the live probe.

1. **BLOCKING — Storage, receipts and binding: deletion detection is incomplete.** [Lines 97–112](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:97). A successor detects deletion from the middle of a chain; it cannot establish the history of a deleted tail, deleted duplicate, or discarded branch. Reading surviving comments does not retain every deletion, as invariant 5 requires. The stated weaker threat model does not replace that requirement. **Fix:** use an append-only witness or independently retained publication/edit/deletion history, with an externally anchored terminal inventory. Exercise tail and branch deletion, not just middle deletion.

2. **BLOCKING — Binding identities are not sufficiently specified to enforce uniqueness.** [Lines 89–96](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:89). The identity contains `source` and `component`, but the receipt schema lacks an explicit source field, and the capture sections do not supply the promised complete component/period registry. It is unclear whether a closing-price component is the whole response or an instrument row, and whether PWB binds by vintage, month, or both. These choices determine whether retries can bind twice. **Fix:** enumerate canonical identities, component granularity, source identifiers and database uniqueness constraints for every kind, including derived manifests and B1.

3. **BLOCKING — “First acquisition” and retry commitment are undefined.** [Lines 70–72, 85–96](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:70). There is no state machine for successful fetch → persisted bytes → validation → receipt → publication. A crash after reading bytes, an invalid response, or a partially rolled all-instrument response can therefore lead to another fetch without specifying which observation must bind. Exclusive creation under a fresh `attempt_id` does not prevent choosing among observations. **Fix:** define irreversible first-observation rules, deterministic per-component validation, durable recovery, and exactly which defects permit another acquisition.

4. **BLOCKING — Several windows are not executable.** [Lines 119–130](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:119). “When a new source vintage appears” supplies neither a polling schedule nor a missed-publication rule. PWB appears in two windows without precedence. FINRA and linkage have no acquisition windows. “Nine-month limit” lacks an explicit timestamp, and deadlines relative to acquisition can move with a delayed attempt. **Fix:** provide a per-kind schedule with timezone, absolute start/end, discovery order, retries, completion timestamp, publication deadline and final missed-window transition.

5. **BLOCKING — Late witnesses and refusal scopes conflict with the parent.** [Lines 104–110, 128–130](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:104). A late receipt “binds nothing,” potentially allowing reacquisition; elsewhere any chain break refuses the verdict. The parent distinguishes recoverable formation sealing failures from returns/B1/factor sealing failures, while duplicate identities permanently poison the trial. Those outcomes are not mapped here. **Fix:** freeze a transition table distinguishing acquisition commitment, witness validity, formation misses, terminal refusals and permanent ambiguity. Publication failure must never reopen source selection.

6. **BLOCKING — The acquisition population does not cover the parent’s required set.** [F4, F7 and Formation resolution](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:202). SEC members are captured for linked candidates and candles for “population instruments,” but the parent also requires carried holdings, controls’ holdings and both references’ constituents. A name can leave the population while still being held. SPY is an ETF and outside F1’s stock population. Candidate CIKs are also needed before the later linkage resolution. **Fix:** define a pre-close acquisition superset, including mandatory benchmark IDs and all potentially carried securities, and retain coverage after eligibility loss.

7. **BLOCKING — The dry-run-to-trial handoff contradicts itself.** [Lines 113–115, 236–250](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:113). Dry-run acquisitions are “never inputs to the trial,” but the split ledger starts at the first dry-run capture, C₀. The design also needs a pre-month MAX boundary and preceding daily captures; an immediate first month-end after declaration may lack them under the trial identity. **Fix:** specify an authorized, sealed pre-declaration lookback contract or revise the launch/declaration protocol explicitly. State how initial MAX, split history and new entrants obtain sufficient history without silently reusing prohibited inputs.

8. **BLOCKING — Formation finality and interim holdings require an explicit reconciliation with the parent.** [Lines 277–283, 321–334](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:321). The parent finalizes formations once previous-month returns are bound; this draft finalizes from statuses alone. It also classifies actual series holdings and prints book weights while forbidding interim portfolio returns. Maintaining actual weights can require the very valuation path being withheld. **Fix:** capture instrument-level inputs/statuses for a sufficient superset and defer portfolio-dependent work, or specify a rigorously separated state machine and declare the formation-finality amendment. Remove interim book-weight reporting.

9. **BLOCKING — B1’s early refusal gate is not enforced by the scheduler.** [F11 and Windows](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:295). Naming `b1_entry_close` does not ensure `B1_UNDEFINED` occurs before any holding-month read. Daily captures begin while formation resolution can remain open until the fifteenth session. **Fix:** give the B1 entry binding its own precise deadline and startup gate; prohibit holding-month acquisitions after that gate fails, irrespective of formation status.

10. **BLOCKING — F11/F12 do not establish binding at first ingestion.** [Lines 297–307](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:297). F12 later selects a database row written by the mutable daemon; it does not describe an ingestion-time receipt. F11 does not resolve multiple eligible filings within a first quarter load or specify immutable month-level selections alongside the ZIP digest. This leaves “first” dependent on ingestion and discovery behavior. **Fix:** bind source bytes and deterministic selections atomically with the first eligible ingestion, preserve rejected/earlier vintages, and prohibit later database reconstruction of an unwitnessed binding.

11. **BLOCKING — Pinned-runtime compliance remains an assertion.** [Runtime and Schema interface](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:346). The schema section promises a future table/column constant but supplies only partial table names. The executable protocol does not implement the parent’s separately fetched `origin/main` ledger, record its commit, or show how mutable upstream parsers are excluded from construction. **Fix:** specify the complete read interface and receipt fields for code commit, construction/dependency hash, schema hash and ledger commit, with their validation order and failure behavior.

12. **BLOCKING — The linkage source cannot supply the stated monthly evidence window.** [Population and tradability, lines 314–317](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:314). Quarterly Insider Transactions datasets will not contain current-quarter filings at ordinary monthly resolution. Filtering them by acceptance date does not recover missing filings or seal the mutable extracted mapping before the decision. SEC documents both quarterly publication and dataset updates. **Fix:** capture the required Form 3/4/5 evidence directly with effective dates, or declare and measure a lagged-linkage exception; freeze the source inventory and resulting link. [SEC dataset documentation](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets).

13. **BLOCKING — F1’s classifier lacks the required class coverage and validation.** [Lines 142–170](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:142). The keyword table does not explicitly resolve funds and commodity pools, and its accepted phrases omit “common shares.” Running it over every candidate and comparing name markers measures disagreements, not correctness; the spec explicitly concedes that. **Fix:** freeze a complete class taxonomy using the required filer/SIC evidence, document XBRL context matching and ambiguity handling, and adjudicate the entire candidate population against authoritative security descriptions.

14. **BLOCKING — Security identity and primary-listing selection are unsafe.** [Lines 149–163](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:149). Removing punctuation can collapse distinct symbols, while symbol-plus-CIK equality is asserted to prove one security. The source and point-in-time semantics of `is_primary_listing` are not identified. Lowest instrument ID is deterministic but does not establish equivalent instruments or pricing sessions. **Fix:** use an effective-dated security/class mapping, preserve collisions as refusals, and document the authoritative primary/RTH fields and equivalence checks.

15. **BLOCKING — F2 substitutes issuer registration acceptance for security listing age.** [Lines 174–177](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:174). The earliest registration under a CIK can concern another class. Acceptance is also not necessarily effectiveness: Form 8-A’s instructions require exchange certification and, where applicable, registration-statement effectiveness. This fails obligation 2’s effective-dated, security-level history. **Fix:** match the registered class, establish effectiveness and actual listing continuity from documented sources, and distinguish those dates from filing acceptance. [SEC Form 8-A, General Instruction A(c)](https://www.sec.gov/files/form8a.pdf).

16. **BLOCKING — F2’s missing-history and SPAC policies are unsupported estimand changes.** [Lines 178–184](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:178). An old EDGAR filing does not establish an old exchange listing of today’s common stock. Likewise, asserted PERMNO continuity does not establish that every SPAC successor inherits the relevant listing-age treatment. Neither policy appears in X1–X6. **Fix:** require security-specific evidence or retain `listing_age_unknown`; cite the precise CRSP convention for any successor rule and declare, measure and review each proxy explicitly.

17. **BLOCKING — F3’s header immutability claim is false as a general source contract.** [Lines 188–198](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:188). EDGAR documents post-acceptance corrections containing header changes. Fetching a header later and filtering only its original acceptance time does not prove the fetched SIC was available then. A tag-definition citation establishes meaning, not historical immutability. **Fix:** capture headers prospectively or reconstruct correction history as of the cutoff; define multi-filer CIK selection and empty-tag handling. Amend the parent’s knowledge-timestamp exemption where necessary. [EDGAR PDS specification, post-acceptance corrections](https://www.sec.gov/info/edgar/specifications/pds_dissemination_spec.pdf).

18. **BLOCKING — F6’s roll predicate does not date the undated official close.** [Lines 23–33, 218–221](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:218). A previous-day `daily.date` does not, without an atomic update contract, prove `officialClosingPrice` belongs to the new session. The probe instead chooses the latest database date after the modal daily date and compares all overlapping instruments, including rows with other dates. The Saturday observation therefore cannot validate this predicate for every instrument/session. **Fix:** obtain a dated authoritative close or a documented field-update contract; validate dates row by row and refuse stale/ambiguous observations. [eToro endpoint documentation](https://api-portal.etoro.com/api-reference/market-data/get-historical-closing-prices).

19. **BLOCKING — X2 does not meet the parent’s nominal-price requirement.** [Lines 222–232](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:222). Corporate-action timing is used to infer vendor price basis despite the acknowledged absence of documentation. The cited capture-certificate module explicitly disclaims a general provider nominal-price guarantee. Three quiet month-ends, 99% roll coverage and PWB overlap cannot establish that guarantee. **Fix:** obtain the source/capture contract required by obligation 6 or use a validated dated adjustment ledger and authoritative raw-price anchor. Treat failure as unresolved source validity, not an accepted vendor substitution.

20. **BLOCKING — F7 dates observation changes as economic events without justification.** [Lines 236–240](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:236). A rebase first observed on d can reflect a late correction, advance adjustment or cumulative changes across a missed capture. Three fetched candles may not provide the two required completed overlapping sessions after gaps or for entrants. There is also no event/no-event tolerance around ratio 1. **Fix:** define completed-bar selection, missing-overlap rules, numerical thresholds, correction handling and authoritative effective-date attribution; refuse ambiguous intervals instead of assigning them to d.

21. **BLOCKING — F7 treats every vendor rebase as an economic return adjustment.** [Lines 243–247](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:243). Two bars changing proportionally does not establish a split or investor distribution. Scaling nominal returns “whatever the action” can capitalize vendor corrections or mishandle reorganizations and distributions. **Fix:** classify documented action types and specify each action’s price/share/cash treatment. Unexplained rebases need a frozen invalid-input rule, not automatic return credit.

22. **BLOCKING — FINRA absence is incorrectly treated as complete negative evidence.** [Lines 248–254; X6](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:248). “No flag → no split” requires complete security and period coverage, publication availability, symbol continuity and documented flag semantics. The probe only counts `S` rows; it establishes none of these. A three-month comparison against flagged events cannot certify all unflagged securities over fifteen months. **Fix:** cite the exact FINRA dataset specification, distinguish missing rows/files from explicit negative evidence, reconcile the full required history against an independently selected event population, and leave unsupported intervals unknown.

23. **BLOCKING — FINRA confirmation does not identify a particular rebase or its ratio.** [Lines 245–253](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:245). A symbol-level flag anywhere in a settlement period can coincide with another adjustment or several rebases. It supplies neither the effective date nor the ratio. This repeats the broad-coincidence problem in the specifically cited prevention log. **Fix:** require event-level matching, define multiple-event handling, and measure coincidence rates and date clustering before treating flags as confirmation.

24. **BLOCKING — FINRA evidence itself is unsealed.** [F7 and Schema interface](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:245). FINRA is read from a live table but has no capture kind, vintage identity, publication timestamp contract, acquisition window or witness rule. Later-loaded or revised flags can therefore change formation ME retrospectively. **Fix:** add immutable FINRA vintage captures, pre-close availability filtering and fixed revision precedence; formation manifests must reference those bindings.

25. **BLOCKING — F5’s “exactly” claim omits essential split-basis mechanics.** [Lines 209–212, 245–254](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:209). The conversion from price-rebase ratio ρ to the share-count multiplier is unstated. Step 1’s full-population ME/adjusted-price reconciliation and discontinuity census are absent. Convention fixtures alone do not establish obligation 5 or 7. **Fix:** specify factor direction and boundary inclusion, implement the required basis-date fixtures and population reconciliations, and label X6’s departure directly in F5.

26. **BLOCKING — X3 does not preserve MAX’s screened-data exclusion.** [Lines 258–265](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:258). #3621 screens unexplained adjustment-ratio jumps, including consecutive usable bars across gaps; screened names are flagged before missing-value rules. Detecting only changes to two overlapping fetched bars is not that screen. Invalidating a close can instead reduce return count and leave the name unflagged. **Fix:** specify the replacement screen, across-gap behavior and precedence explicitly, including how inconsistent/unexplained rebases become exclusions.

27. **BLOCKING — X3’s estimand change is unmeasured at this checkpoint.** [Lines 261–265, 360](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:261). Price-return MAX and removal of the ratio screen change the tested filter. The historical measurement is promised rather than supplied, and “with and without dividends” alone does not isolate the screen substitution. **Fix:** provide the full formation-by-formation comparison for dividend omission and screen replacement separately, including threshold and flagged-set changes, then explicitly accept the amended estimand before planning freezes.

28. **BLOCKING — F9’s return arithmetic is ambiguous and inconsistent on combined split/dividend dates.** [Lines 270–276](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:270). The product is a gross relative, not a net return. `k_d` has no defined direction; the usual new/old adjusted-price-ratio change can produce a negative dividend. Moreover, converting D to the previous nominal-price scale and then dividing the whole expression by ρ can double-adjust split-day dividends. **Fix:** define gross/net units, ratio direction and dividend share basis algebraically; test a flat-price dividend, a split, and a simultaneous split/dividend with hand-calculated cash entitlements.

29. **BLOCKING — Dividend extraction is inferred without a sufficient vendor adjustment contract.** [Lines 273–276](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:273). Every `adj_close/close` change is assumed to be an ex-date cash dividend, then rescaled using another vendor’s price. This does not establish gross cash amounts, distribution types, rounding behavior or exclusion of other corporate actions. PWB documents adjusted closes but not the inversion contract claimed here. **Fix:** use documented cash-dividend records or document and validate the precise upstream adjustment algorithm, with explicit handling of noncash distributions and revisions. [PWB dataset card](https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price/blob/main/README.md).

30. **BLOCKING — X4 changes total returns into coverage-dependent price returns.** [Lines 277–279, 361](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:277). Missing dividend coverage is not evidence of zero dividends. The 5,247/6,664 count concerns currently tradable instruments in one stored vintage, not the full forward population or all carried holdings. Printing weights does not repair the altered estimand. **Fix:** obtain a valid total-return source under fixed precedence; otherwise retain incomplete inputs and the parent’s refusal behavior. Any price-return experiment needs an explicit parent-level estimand amendment.

31. **BLOCKING — The PWB identity gate selects on price outcomes and rejects legitimate adjusted series.** [Splicing, line 342](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:342). A median close-level ratio within 1% is neither a security identity mapping nor valid reconciliation between nominal and split-adjusted prices. A correct series with a later split can fail. Eligibility also depends on the realized month’s prices, potentially routing failures into X4. **Fix:** establish identity independently with effective dates, reconcile prices after documented basis conversion, and treat unresolved mappings as unavailable inputs rather than zero dividends.

32. **BLOCKING — F9’s “independent reference” shares the dividend source and has no rejection rule.** [Lines 280–283](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:280). PWB dividends and PWB adjusted-close returns share errors; their agreement cannot independently validate dividend completeness. The 0.5-point tolerance only causes printing, so arbitrarily large disagreements remain admissible. **Fix:** add independently sourced action/return validation, freeze the complete comparison population, and specify which disagreements invalidate inputs or prevent dry-run acceptance.

33. **BLOCKING — PWB selection does not preserve the returns evidence cutoff.** [Lines 125–126, 273–276](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:273). The first commit covering a month may arrive after the tenth-session cutoff and incorporate later corrections without knowledge timestamps. “Covers the month” is also undefined globally versus per security, leaving room to choose later vintages for incomplete names. **Fix:** separate evidence availability from delayed acquisition, define deterministic per-component vintage selection, and capture cutoff-eligible dividend evidence or declare the necessary cutoff exception.

34. **BLOCKING — F9 promises a missing-close repair that its source cannot perform.** [Lines 284–285](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:284). A missing session close is said to be completable within nine months, but F6 captures a rolling latest-close field whose daily window is final. No admissible historical recovery source is defined. **Fix:** distinguish repair of already captured immutable bytes from acquisition of permanently missed observations; specify a sealed recovery source or deterministic incompleteness/refusal.

35. **BLOCKING — F10’s CIK-level Form 25 predicate is broader than a security termination.** [Lines 289–293](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:289). Any Form 25 accepted for the CIK by the cutoff can concern a different class or an unrelated delisting. Acceptance alone does not establish the effective removal date. Form 25 explicitly identifies the security class and distinguishes filing from effective removal. **Fix:** reproduce the actual classifier’s security/event matching, effective-date and provision rules, including amendment precedence and unrelated-class fixtures. [SEC Form 25](https://www.sec.gov/files/form25.pdf).

36. **BLOCKING — Terminal and coverage-exit partial-month returns have no complete capture path.** [F9, F10 and Formation resolution](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:268). F9 and `month_returns` bind only `observed` holdings. Step 1 also requires a total return through `end_bar` for terminal and coverage-exit holdings before cash realization. Missing collector data versus actual source coverage cessation is not resolved consistently with F9’s incomplete-input rule. **Fix:** bind partial-period prices, dividends, adjustments and both terminal arms for every status; distinguish source cessation, capture failure and invalid total-return evidence explicitly.

37. **BLOCKING — The inherited JKP holding-return cutoffs have no forward source or sealing protocol.** [F9 and capture inventory](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:268). Step 1 lines 990–1016 require holding-month USD cutoff rows, consistency validation, clipping after status realization, and frozen cutoff bytes. None appears here or in X1–X6. **Fix:** add a cutoff capture kind, first-vintage/revision rule, deadlines and missing/invalid-row refusals; preserve raw and clipped arm values. Alternatively, explicitly amend the parent to remove this treatment.

38. **WARNING — The probe does not reproduce all “measured premises.”** [Measured premises, lines 18–59](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:18). The script measures closing-price response statistics, one PWB coverage query and FINRA counts. It does not print the claimed PWB commit history, SEC ZIP sizes, ADR-name count, connection budget, revision history or B1/factor-writer evidence. Its FINRA date query is restricted to August 2025 onward and cannot reproduce the stated 2021 storage start. **Fix:** provide a reproducible evidence command/artifact for each premise and distinguish measurements, documentation and code inspection.

39. **WARNING — Runtime capacity and shared-quota safety are unproven.** [Lines 73–81](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:73). A private 30/min limiter does not reserve capacity under a shared quota; likewise 2 SEC requests/s does not control other users of the shared limit. Fetching 6,664 candle series takes at least 222 minutes before retries, nearly the asserted four-hour work period. The actual post-close window also overlaps the cited 03:00 UTC daemon job. **Fix:** specify coordinated budgets, fixed work ordering and timeouts, and measure full-population completion/publication latency under contention.

40. **WARNING — F4’s bulk-file handoff is underspecified.** [Lines 202–207](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:202). “Downloaded that day” does not establish an atomic consistent read of daemon-owned files, complete historical submissions pages, or the timestamp of the bytes copied. The five-session fallback can also omit recently accepted eligible filings without a defined freshness/completeness report. **Fix:** specify immutable handoff manifests, atomic file acquisition, page/member completeness and exact fallback selection, and report the evidence lost through stale bundles.

41. **WARNING — Dry-run acceptance omits important promised evidence and outcome rules.** [Lines 369–380](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:369). The mandatory list does not explicitly require F3’s complete accession reconciliation, X3’s historical measurement, X5’s tradability parity or X6’s historical coverage validation. Presence of tables generally counts as acceptance regardless of their contents. Quiet dates can satisfy event checks vacuously. **Fix:** enumerate every required artifact and its predeclared pass/fail interpretation, including event coverage, negative controls and source-disagreement refusals.

42. **WARNING — Dry-run instructions contradict the required calculations.** [Lines 280–283, 367–377](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:367). F9 requires monthly total-return comparison, while the dry-run checklist says “no return, book or comparison computed.” The parent prohibits forming books, not all instrument-level validation. **Fix:** explicitly permit the frozen instrument-level return/action checks and prohibit portfolio formation, portfolio weights and performance comparisons.

43. **NIT — FINRA publication frequency is ambiguously named.** [Measured premise 4](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:44). “Bi-monthly” can mean twice monthly or every two months, which is particularly undesirable in an executable scheduling specification. **Fix:** say “semimonthly” and reference the settlement/publication calendar.

44. **NIT — “Slice 3 (planning)” conflicts with this document’s build-slice numbering.** [Lines 379–388](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:379). Local build slice 3 is capture implementation, not planning. **Fix:** identify “parent slice 3, planning” explicitly.

**Overall assessment: not fit to be built as the forward-accrual design.** The blockers affect source validity, the estimand, return arithmetic and irreversible sealing. They need specification-level resolution before implementation can be reviewed against a determinate contract.

## Round 2

**The revised design is still not ready to build.** I read the complete target, all 44 round-1 findings, the requested local source ranges, and the probe. I also checked primary documentation for Rekor, eToro, SEC forms and PWB. I did not edit files or run the live probe.

For task A, **APPLIED** means the original defect was resolved, including by removing the offending treatment. It does not certify the replacement design.

1. **PARTIAL** — Rekor replaces editable comments, but incomplete enumeration and the annotation-only fallback still permit undetected discarded branches.
2. **PARTIAL** — The registry specifies more granularity, but omits several logical bindings and conflicts with incremental manifests.
3. **PARTIAL** — A first-observation rule exists, but crash recovery and commitment before persistence remain unspecified.
4. **PARTIAL** — Most windows have timestamps; SEC schedules, discovery commitments, Form 25 completion and incremental manifests remain contradictory or incomplete.
5. **PARTIAL** — An outcome table exists, but chain requirements conflict with publication failure and formation-only refusal scopes.
6. **PARTIAL** — The sticky superset includes benchmarks, but construction dependencies, retirement and final-period coverage remain unresolved.
7. **PARTIAL** — A2 authorizes lookback reuse, but omits required source kinds, code compatibility and sufficient-history launch requirements.
8. **PARTIAL** — A1 explicitly defers holdings-dependent resolution and removes weights; immutable manifest and completion semantics remain inconsistent.
9. **PARTIAL** — An early stop is specified, but F6 makes successful B1 binding impossible by that deadline.
10. **PARTIAL** — Direct N-PORT/French acquisition removes mutable-daemon reconstruction; logical selection identities and revision precedence remain incomplete.
11. **PARTIAL** — Provenance and preflight improve, but the declared database interface omits actual reads and ledger validation is unspecified.
12. **PARTIAL** — X7 removes reliance on current-quarter insider datasets; its replacement lacks executable capture and effective-dated linkage.
13. **PARTIAL** — Taxonomy improves, but accepted classifications are excluded from adjudication and class-context matching remains unspecified.
14. **PARTIAL** — Symbol collision handling improves; security continuity, primary-listing equivalence and RTH pricing semantics remain asserted.
15. **PARTIAL** — Class matching improves, but certification/8-A acceptance still substitutes for effective listing history.
16. **PARTIAL** — X8 declares the archive proxy, but security continuity, relistings and successor treatment remain unresolved.
17. **PARTIAL** — Header immutability is no longer assumed; X1’s replacement comparison does not measure the relevant source effect.
18. **PARTIAL** — Dated confirmation is added, but arrives after the cutoff and depends on unavailable adjustment evidence.
19. **PARTIAL** — The anchor is a diagnostic, not the required nominal-price guarantee; admitted failures remain.
20. **PARTIAL** — Observation dates are no longer directly called event dates; F7 still lacks dated action attribution and valid boundary mechanics.
21. **PARTIAL** — Split-only treatment is stated, but established only by a selected dry-run sample.
22. **APPLIED** — FINRA absence is no longer used as negative split evidence.
23. **APPLIED** — FINRA settlement-period coincidence is no longer used to confirm rebases.
24. **APPLIED** — The unsealed FINRA dependency is removed.
25. **PARTIAL** — The share multiplier is explicit, but convention fixtures, population reconciliation and discontinuity checks remain absent.
26. **PARTIAL** — Screen precedence is restored; endpoint-factor failure does not reproduce the original within-month screen.
27. **NOT APPLIED** — The required estimand measurement is still deferred; the proposed comparison also does not measure the actual replacement.
28. **APPLIED** — The defective dividend-inversion formula is removed in favor of adjusted-close returns.
29. **APPLIED** — Undocumented dividend extraction is removed.
30. **PARTIAL** — A3 reduces one exposure, but X4 still substitutes price returns for missing total returns.
31. **PARTIAL** — Mapping language improves, but symbol identity and adjustment conversion remain unestablished and reconciliation still selects by realized prices.
32. **PARTIAL** — An independent source and thresholds are named; its acquisition, arithmetic, coverage and event tests remain undefined.
33. **PARTIAL** — Vintage selection is more explicit, but the cutoff violation is justified informally rather than resolved or properly amended.
34. **NOT APPLIED** — Missing capture is now treated as coverage cessation instead of receiving a repair/incompleteness rule.
35. **PARTIAL** — Security matching is delegated to the register, but event timing, amendments and classifier precedence remain incomplete.
36. **PARTIAL** — Partial-month returns are described, but their capture, end-bar selection and total-return completeness remain unresolved.
37. **PARTIAL** — Cutoffs are captured, but X9 substitutes unjustified carried bounds and leaves selection precedence ambiguous.
38. **APPLIED** — Evidence types are distinguished and the probe now prints the additional claimed probe outputs.
39. **PARTIAL** — Capacity measurement is promised; coordinated quotas, bounded execution and an effective acceptance gate remain missing.
40. **PARTIAL** — ZIP hashes and freshness checks improve, but immutable handoff, historical-page completeness and availability provenance remain unspecified.
41. **PARTIAL** — Acceptance criteria improve, but transport exceptions, missing measurements and incomplete event coverage still permit inadequate acceptance.
42. **APPLIED** — Instrument-level validation is explicitly permitted while portfolio calculations remain prohibited.
43. **APPLIED** — The ambiguous FINRA frequency disappears with FINRA’s removal.
44. **APPLIED** — “Parent slice 3” now identifies the planning stage.

For task B, these are the findings against the revised document, including unresolved defects in replacement mechanisms. Line references point to the revised target unless another source is named.

1. **BLOCKING — Witness: successful enumeration does not prove completeness.** [Lines 135–142](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:135). The design handles index *unavailability*, but Rekor explicitly permits incomplete results even on a successful query. Equality between an incomplete index result and the surviving chain cannot detect an omitted branch or tail. The 30-day annotation also knowingly waives invariant 5. **Fix:** require a complete, independently retained inventory or verifiable enumeration mechanism; unavailable completeness evidence must prevent a verdict. [Rekor API specification](https://raw.githubusercontent.com/sigstore/rekor/main/openapi.yaml).

2. **BLOCKING — Witness: timestamp authentication is missing.** [Lines 130–136](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:130). Verifying inclusion alone does not specify verification of the claimed `integratedTime`. Rekor separately supplies a signed entry timestamp covering that field. No log trust root, signed-checkpoint verification or key-rotation policy is pinned. **Fix:** specify and retain the receipt signature, signed entry timestamp, authenticated checkpoint and associated verification keys; reject invalid or untrusted proofs. [Rekor verification schema](https://raw.githubusercontent.com/sigstore/rekor/main/openapi.yaml).

3. **BLOCKING — Witness/Outcomes: publication failure has no coherent chain transition.** [Lines 133–145, 174–180](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:133). A successor requires the previous entry’s log index, while a publication failure can leave no index. “Every receipt” must also have an on-time entry, although late receipts are supposed to remain in the chain and merely miss a formation. **Fix:** define pending-publication recovery and late/failure records, plus verifier precedence separating chain integrity from input admissibility. Demonstrate that a formation-only publication failure neither stalls subsequent capture nor silently becomes a global refusal.

4. **BLOCKING — Attempt state machine: crashes still reopen observation selection.** [Lines 89–103](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:89). A response can be read before `persisted`, then discarded through a crash and fetched again. Conversely, persisted bytes without a receipt have no mandatory recovery path. The spec supplies no atomic relationship between files, component claims and receipts. **Fix:** persist acquisition intent and response progress durably; recover existing bytes before any retry; treat uncertain acquisition as a frozen failure where necessary. Define crash outcomes at every transition and deterministic transport-defect validation.

5. **BLOCKING — Binding registry: physical captures do not enforce logical uniqueness.** [Lines 107–126](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:107). Missing identities include `b1_entry_close`, B1 month selections, final factor selections and month-specific cutoff selections. Content-addressed French/JKP files permit multiple source versions without uniquely committing the evaluation choice. A component-level partial index is also not specified by a schema containing one receipt row per multi-component attempt. **Fix:** enumerate acquisition and selection identities separately, including normalized component constraints, and bind each logical selection irreversibly.

6. **BLOCKING — Manifests: one immutable component cannot accumulate returns.** [Lines 123–124, 160, 420–422](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:123). `month_manifest` is one component per month, yet statuses bind by session 15 and return components bind later. Updating it violates immutability; issuing another binding violates uniqueness. **Fix:** use separately identified immutable status and return-component bindings, followed by one final manifest, or define an append-only manifest protocol with explicit identities and completion rules.

7. **BLOCKING — Windows: first-seen discovery can move deadlines and source precedence.** [Lines 159–162](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:159). “The day it is first seen” is not independently committed. Missing a poll can move the acquisition deadline; multiple unseen versions have no discovery order; mutable files may change between discovery and acquisition. Content-hash periods also require reading bytes before knowing whether the version is new. **Fix:** witness every scheduled discovery result and failure, retain the discovered inventory, freeze version ordering and exact acquisition targets, and specify missed-poll and missed-version outcomes.

8. **BLOCKING — SEC capture: required source kinds contradict the registry and schedule.** [Lines 117, 155, 212–215, 237–238, 401–402](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:117). `sec_bulk` is registered as CIK JSON members on only three sessions per month. F1 instead requires accession-level XBRL on the session after acceptance; F13 requires a daily ticker map; F2 requires certification documents. These objects have no consistent identity/window. **Fix:** register and schedule each source explicitly, including historical bootstrap, accession documents, ticker maps and pagination inventories.

9. **BLOCKING — Form 25 window: cutoff-day evidence can be omitted.** [Lines 158, 369–375](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:158). A 06:00 scan of the previous calendar day cannot include filings accepted later on the final returns-cutoff day if scanning stops at that cutoff. “Daily until persisted” also has no acquisition end or delayed-index rule. **Fix:** separate acceptance eligibility from acquisition completion, run a final complete cutoff-eligible scan afterward, and freeze index completeness, retries, late-index handling and missing-evidence refusal.

10. **BLOCKING — Rates and fallback: first, latest and session-specific requirements conflict.** [Lines 99–100, 114, 154, 164–166, 292–295](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:99). First persistence fixes a rates batch, but F6 asks for the “latest” rate. Generic pre-close fallback permits a previous session’s rates, while F6 requires an in-session anchor. Batch identities have no canonical partition, and “bid or last execution” has no fixed precedence or freshness test. **Fix:** freeze batches, acquisition slots, price-field precedence and quote-age validation; enumerate which kinds permit fallback and prohibit stale-session anchors.

11. **BLOCKING — F6: formation validity depends on evidence outside invariant 1’s cutoff.** [Lines 284–291](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:284). Session d’s close is not valid until the next session’s post-close response arrives. That is after the next open, whereas all decision-price-package inputs must bind before it. Calling the first value a candidate does not make the later validation input timely. **Fix:** obtain sufficient dated evidence within the permitted window or explicitly amend and review the cutoff contract.

12. **BLOCKING — F8: all prior-session MAX prices are acquired after the permitted cutoff.** [Lines 308–309, 325–328](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:308). The month’s entire candle history is fetched after s(M)’s close. Invariant 1 requires price/MAX inputs for earlier sessions to be acquired before that close. X3 describes return and screen substitutions, not this sealing exception. **Fix:** bind prior-session inputs before the close and isolate the final-session package, or submit an explicit cutoff amendment with revision-exposure analysis.

13. **BLOCKING — B1: the required success state is unreachable by its deadline.** [Lines 176, 386–388](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:176). B1 must bind by 08:00 on the next session’s date, but F6 confirmation comes after that session closes. Even without that contradiction, the close’s witness deadline is 09:00, later than the B1 gate. **Fix:** define an independently valid B1 close and witness deadline that precede all holding-month reads; align the scheduler, registry and refusal transition.

14. **BLOCKING — Windows/F7/F9: the final holding month lacks its required candle capture.** [Lines 116, 157, 349, 354, 386–388](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:116). Candles are scheduled at formations M₀…M₂₃, but the final return’s reconciliation and fallback factor require a fetch at s(M₀+24). B1’s final close also needs next-session confirmation. **Fix:** enumerate terminal-period and post-terminal validation captures explicitly, without creating an extra formation.

15. **BLOCKING — Outcomes: refusal codes and precedence remain incomplete.** [Lines 168–181](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:168). “Reason: the kind” does not supply the promised input refusal taxonomy. An unwitnessed returns observation can be called unbound and become `INPUT_UNAVAILABLE`, or fail its witness check and become `CAPTURE_AMBIGUOUS`. `MAX_EMPTY`, invalid selection/mapping evidence and missing cutoff fallback also lack explicit transitions. **Fix:** provide one exhaustive validator-to-state/code table, with sealing failures taking the parent’s specified scope and precedence.

16. **BLOCKING — Pinned runtime: the declared read interface is false.** [Lines 70–75, 401–402, 442–445, 461–463](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:442). F13 reads `external_identifiers`, and X5 requires membership parity, but neither appears in the two-table interface. `forward_capture_receipts (all columns)` supplies no actual schema. Fetching and recording a ledger commit also does not specify validating its declaration, trial state and bindings. **Fix:** enumerate every database read and column, freeze the receipt/component schema, and specify ledger-content checks before acquisition and evaluation.

17. **BLOCKING — Superset: construction and retirement do not establish continuous required coverage.** [Lines 183–192](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:183). SEC capture starts at 09:30, before that day’s listing capture can identify new candidates. Retirement after absent closes plus a Form 25 or listing removal is not explicitly tied to the bound status that realizes every possible holding. The spec also leaves effective symbol changes and re-entry after retirement unresolved. **Fix:** define the superset at each acquisition boundary, allow deterministic completion for newly discovered dependencies, and retire only after bound evidence proves no required path can still hold the security.

18. **BLOCKING — A2: lookback reuse is incomplete and lacks a compatibility contract.** [Lines 194–201](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:194). A2 allows four capture kinds but omits pre-declaration PWB vintages needed for A3 and F2. It does not address changed validators/code between dry run and declaration, a gap in capture, or insufficient basis history. **Fix:** list all admissible lookback objects, pin their identities and construction versions, require continuous coverage through launch, and forbid silently reinterpreting or reusing inadmissible dry-run observations.

19. **BLOCKING — F1: “full adjudication” excludes accepted securities.** [Lines 209–233](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:209). Only unknown/rejected/fund/duplicate rows are hand-checked. False acceptance—the error that admits a fund or wrong class—can therefore pass. The relationship among title, symbol and exchange facts across XBRL contexts is also unspecified. **Fix:** adjudicate every candidate, including accepted rows, and freeze class-context joins, duplicate/conflicting facts, missing facts and taxonomy fixtures.

20. **BLOCKING — F1: deterministic duplicate preference does not establish instrument equivalence.** [Lines 218–228](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:218). Matching a current cover class and preferring exchange tags does not establish that instruments have identical security rights, currency, pricing sessions or effective listing history. Primary/RTH treatment remains a preference rule rather than a validated mapping. **Fix:** bind effective-dated instrument-to-security mappings and document authoritative exchange/session semantics; refuse unresolved equivalence before selecting a representative.

21. **BLOCKING — F13/X7: the latest periodic cover is not an effective-dated symbol history.** [Lines 399–407](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:399). A symbol can change or be reused between periodic filings. “No other CIK” is tested only within captured candidate data whose completeness is not established. The dry-run comparison to lagged insider data cannot resolve those gaps. **Fix:** define effective intervals and intervening corporate-action evidence, capture a complete candidate inventory, and distinguish unavailable history from proven unique identity.

22. **BLOCKING — F2: acceptance still replaces the documented effectiveness rule.** [Lines 237–241](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:237). Earliest certification acceptance, else earliest 8-A acceptance, is not the applicable effectiveness calculation and does not establish uninterrupted listing. Form 8-A explicitly uses the later/latest of specified events. **Fix:** implement the documented event combination for the matched security, then establish actual listing commencement and continuity. [SEC Form 8-A, General Instruction A(c)](https://www.sec.gov/files/form8a.pdf).

23. **BLOCKING — F2/X8: the fallback reinstates archive seasoning and tests the wrong error direction.** [Lines 242–248](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:242). A PWB first bar does not establish current-security listing age without continuity through ticker reuse, reorganizations and relistings. The pass rule rejects *understated* age, but overstated age can wrongly admit a security before 36 months. Comparison only where both sources exist does not validate the missing-history population. **Fix:** retain unknown age without security-specific continuity evidence; measure false eligibility at the 36-month boundary in both directions and explicitly govern successor/relisting treatment.

24. **BLOCKING — F3/X1: the stated “upper bound” is not established.** [Lines 256–262, 451](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:256). Current company SIC versus an older accession SIC confounds time and source; changes can create disagreements or cancel source discrepancies. It is not necessarily an upper bound. X1 also labels the parent rule as SUB SIC, although the immediate parent’s obligation 3 specifies accession-header SIC. **Fix:** state the correct replaced contract and obtain matched-date comparison evidence, including missingness and effects on REIT/fund exclusions and FF-12 assignments; otherwise label the source effect unmeasured.

25. **BLOCKING — F4: ZIP freshness does not establish a complete point-in-time bundle.** [Lines 266–273](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:266). Local mtime and before/after hashes do not specify an immutable daemon handoff, the ETag’s association with the copied bytes, or completeness of historical submissions pages. The alleged complete missing-filings inventory comes from a Form-25-specific acquisition path. **Fix:** require an atomic upstream handoff manifest, enumerate and verify all required pages/members, and seal a complete accession inventory used for freshness/missing-evidence reporting.

26. **BLOCKING — F5: required basis fixtures and population reconciliation remain absent.** [Lines 275–280](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:275). Referring to step 1 does not specify how its event-based tests apply after replacing the split product. Forward/reverse splits, context-date versus acceptance-date boundaries, whole-population ME reconciliation and discontinuity reporting are not required by the acceptance list. **Fix:** specify those fixtures and population checks for the new factor, preserving the cited source rules or declaring each departure.

27. **BLOCKING — F6/X2: the price anchor cannot certify nominal prices.** [Lines 292–304](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:292). An intraday price within ×1.15 admits small pre-adjustments and can admit larger adjustments offset by real price movement. It also rejects legitimate large moves between the anchor and close. The spec acknowledges failures yet declares qualifying observations valid. **Fix:** obtain the required documented nominal-price contract or authoritative dated action/raw-price evidence; treat the anchor as a diagnostic with explicit failure handling. The endpoint documentation identifies a most-recent official close, not the claimed nominal-basis guarantee. [eToro closing-price documentation](https://api-portal.etoro.com/api-reference/market-data/get-historical-closing-prices).

28. **BLOCKING — F6/F7: next-session rebase confirmation is not executable.** [Lines 289–291, 308–321](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:289). F6 refers to factors between “the two responses’ candles,” but closing-price responses are not candle captures and candles are scheduled only at formations. F7 itself requires valid F6 closes, creating a dependency cycle in precisely the rebase case. **Fix:** specify independently captured adjustment evidence and an acyclic validation order, including daily/non-formation sessions and numerical tolerances.

29. **BLOCKING — F7: five-session medians do not implement the required basis-date factor.** [Lines 310–321](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:310). Windows can straddle an action at b or s(M), making a valid split produce mixed ratios and `factor_unresolved` instead of the factor over `(basis, s(M)]`. Bid/trade-price bias cancels only under an unstated stability assumption. The 5% deadband also deliberately erases smaller stock distributions. **Fix:** derive factors on the exact required date basis from validated action evidence, define spread calculations and missing-session handling, and test endpoint actions and small distributions explicitly.

30. **BLOCKING — F7/X6: event validation is selected by the detector being validated.** [Lines 314–319](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:314). Adjudicating only `F ≠ 1` cannot detect actions erased by the deadband, missed actions or offsetting changes. Observing only splits in three dry-run months cannot establish that future vendor adjustments contain only splits and stock dividends. **Fix:** independently enumerate action and non-action cases, measure false negatives as well as false positives, and require a frozen forward rule for unexplained adjustments. Sample agreement must remain compatibility evidence.

31. **BLOCKING — X6: short lookback introduces an unmeasured ME-availability selection.** [Lines 279–280, 455, 467](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:279). Three dry-run month-ends do not cover the permitted 15-month share bases. Valid candidates can therefore lose ME solely because capture started recently, changing the MAX population and top 1,000. New entrants face the same problem. **Fix:** acquire sufficient validated basis history or measure and explicitly review this population restriction, including launch readiness and entrant treatment.

32. **BLOCKING — F8/X3: endpoint-factor failure does not replace the documented within-month screen.** [Lines 325–332](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:325). An unexplained interior adjustment can leave both endpoint median windows consistent, so the name passes the replacement screen. Conversely, missing anchor data can screen a name with no adjustment defect. The cited #3621 rule tests consecutive usable bars, including across gaps. **Fix:** specify and validate a daily replacement with the required across-gap behavior, or explicitly quantify and accept the different exclusion rule.

33. **BLOCKING — X3 measurement: the proposed three-way comparison does not measure the implemented change.** [Lines 333–337, 490](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:333). Removing the old ratio screen is not installing `factor_unresolved`; separate price-return and screen-removal runs do not measure their combined effect, and neither measures Bid-versus-trade prices. No results are supplied. **Fix:** provide the actual feasible comparisons, including combined treatment and threshold/flag changes, and identify unmeasurable dimensions explicitly before closing the exception.

34. **BLOCKING — F9/Splicing: identity and basis conversion remain assertions.** [Lines 347–350, 435–436](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:347). Exact symbols do not establish effective security identity. “Adjusted to near the same date” is not a conversion: an action between the candle fetch and PWB commit can move levels outside the tolerance for a correctly mapped security. This again routes realized-price disagreement into fallback treatment. **Fix:** bind identity independently, perform a documented dated basis conversion, and keep reconciliation failure distinct from identity failure and dividend absence.

35. **BLOCKING — F9: the 90% selection denominator is not frozen.** [Lines 343–346](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:343). “The superset’s mapped series” can depend on the candidate commit, realized-month reconciliation and later changes to the superset. Removing difficult mappings can make a commit qualify. A sticky population containing terminated securities can also make month-end-bar coverage structurally unattainable. **Fix:** freeze each month’s denominator and mapping inventory independently of candidate outcomes; specify zero-denominator, cessation, missing-boundary and below-threshold outcomes.

36. **BLOCKING — A3: the additional entry screen is unmeasured and incompletely scoped.** [Lines 351–357, 460](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:351). PWB membership changes entry eligibility beyond the parent’s filters, but no measurement shows its effect on eligible names or the planning assumptions. Applicability to controls and references is not stated, and the pre-close mapping algorithm cannot simply use the later holding-month reconciliation. **Fix:** define its as-of construction and application to every series, measure the population change and amend planning where applicable.

37. **BLOCKING — X4: vendor loss is not an economically neutral reason to substitute price returns.** [Lines 353–357, 376–377](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:353). The fallback omits distributions and now extends to terminal/coverage-exit partial periods, beyond X4’s table description of observed holdings. A3 does not establish that subsequent losses are rare. PWB itself documents survivorship-related coverage gaps. **Fix:** retain incomplete inputs under the parent or approve a separately measured price-return estimand with explicit status/reference scope; do not justify it as a neutral operational exception. [PWB dataset card](https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price/blob/main/README.md).

38. **BLOCKING — F9: later PWB evidence silently replaces the returns cutoff.** [Lines 362–363](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:362). The parent permits delayed acquisition of cutoff-eligible evidence, not arbitrary later revised histories. “Holding returns inform no decision” is also too broad: returns affect carried values and later path construction even when computation is deferred. **Fix:** preserve the evidence cutoff or declare a specific revised-vintage amendment, measure revision exposure and reconcile it with formation/path semantics.

39. **BLOCKING — F9 validation: the independent reference is not specified sufficiently to run.** [Lines 358–361](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:358). “Dividends from SEC records where available” supplies no document inventory, capture rule, gross-cash convention, ex-date/share basis, reinvestment formula or treatment of missing records. Aggregate monthly tolerances can pass while an entire rare action class fails; obligation 9 requires separate split, dividend, termination and daily-return tests. **Fix:** freeze those inputs and calculations, report uncovered cases separately, and require event-stratified validation and refusal rules.

40. **BLOCKING — F9/F10: fallback arithmetic and row validity remain underdefined.** [Lines 343–360, 376–377](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:343). “Scaled by F” does not specify the gross-relative formula or partial-period factor. PWB selection tests endpoint presence but does not define validity of both endpoints, duplicates, non-finite values or missing partial-period bars. **Fix:** state exact gross/net formulas and factors for every status, define deterministic row validators and retain invalid observations as incomplete inputs rather than silently changing return basis.

41. **BLOCKING — F10: collector and validation failures become economic coverage exits.** [Lines 341–342, 367–375](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:367). F6 failure can result from missing acquisition, late witness, stale confirmation or a legitimate large price move. F10 calls all of these “our coverage” and realizes the holding to cash. That defeats the parent’s distinction between actual source cessation and missing/invalid input. **Fix:** preserve separate source-coverage, acquisition and validation states; only documented source cessation may trigger `coverage_exit`.

42. **BLOCKING — F9/F10: interior-gap and last-bar rules contradict each other.** [Lines 341–342, 373–377](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:341). F9 calls a name observed whenever the final close exists; F10 includes interior gaps as exits. “Last valid close” does not say whether it is before the first gap or a later resumed bar. **Fix:** define ordered status predicates and `end_bar` selection, including resumption, no valid holding-month bar and missing initial boundary; bind the complete partial-return package.

43. **BLOCKING — F10: security matching alone does not reproduce termination classification.** [Lines 369–375](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:369). A matched Form 25 still needs an event/effective-date relationship to cessation and amendment precedence. “Terminal when a matched Form 25 exists; coverage_exit otherwise” also omits the cited classifier’s Q-suffix branch. **Fix:** specify the full `TerminationEvidence` construction and classifier precedence, including unmatched/ambiguous evidence, Q-suffix handling and amendments. Form 25 distinguishes filing from effective removal and explicitly addresses amendments. [SEC Form 25 instructions](https://www.sec.gov/files/form25.pdf).

44. **BLOCKING — F11/F12: version and acceptance precedence are incomplete.** [Lines 120–121, 381–395](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:381). A revised N-PORT ZIP for the same quarter collides with the quarter identity, despite the parent retaining later revisions as provenance. Across first-seen datasets, discovery order is not fixed. F12 says first download covering the period, whereas the parent says first **accepted** snapshot; invalid covering downloads have no clear selection transition. **Fix:** separate source-version provenance from B1/month and factor/trial selections, define acceptance before selection, and retain rejected/revised versions without replacing committed choices.

45. **BLOCKING — F14/X9: carry-forward replaces a documented source rule with an unsupported rationale.** [Lines 411–414](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:411). The cited JKP rule uses the holding month’s return distribution. Calling clipping “containment” does not establish that an earlier month’s bounds are interchangeable; the cited step-1 text explicitly notes that clipping changes real gains and losses. **Fix:** obtain the actual holding-month row within the allowed completion period, or provide an explicit estimand amendment measuring changed clips, both arms and stress-month effects.

46. **BLOCKING — F14: cutoff selection can change after the supposed decision.** [Lines 411–414](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:411). “First binding containing it” competes with fallback at the returns cutoff. The spec never says whether a later actual row replaces carried bounds, whether “latest binding” means latest as of the cutoff, or what happens when no usable fallback exists. **Fix:** bind one month-level cutoff selection with an explicit decision time, source vintage, row month, validation result and no-replacement rule; define every missing/invalid case.

47. **WARNING — Dry run: explained failure can count as acceptance.** [Lines 469–479](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:469). Item 1 accepts missed windows if explained by transport defects, undercutting the runtime claim that every formation fetch must finish. F8’s measurement and X5 parity are not explicit required artifacts. One detected F7 event plus one Form 25 does not cover the required action/error cases. **Fix:** distinguish successful mandatory captures from fault-injection drills, enumerate every required artifact, and require adequate independently selected event and negative-control coverage.

48. **WARNING — Runtime: one measured run does not reserve shared capacity.** [Lines 76–83, 152–160](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:76). Private rate limits do not coordinate either shared quota. Long candle/file work under one `flock` can delay later windows, and request timeouts, per-tick work limits and interruption/resumption are unspecified. **Fix:** define coordinated budgets or conservative bounded scheduling, durable queues and timeouts; measure completion under specified contention and superset growth.

49. **WARNING — Runtime/Dry run: disk budgeting excludes much of the retained workload.** [Lines 73–74, 93, 159, 476](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:73). A projection for “24 formations” does not explicitly include daily captures, failed-attempt bytes, all provenance vintages, the dry run and nine-month completion tail. Twice the largest due artifact is not a long-horizon capacity check. **Fix:** project the complete retention inventory and peak concurrent storage, with a deterministic low-space transition.

50. **WARNING — Witness drills: several promised failure classes have no explicit test outcome.** [Lines 143–145](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:143). Edit/revert, deleted tail/branch, incomplete enumeration and timestamp tampering are absent. Duplicate publication is listed but not distinguished from an idempotent retry returning the existing entry. **Fix:** enumerate each drill and expected state/code, including how the actual Rekor API’s duplicate behavior is handled.

51. **WARNING — X5: common source does not establish membership equivalence.** [Lines 461–463](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:461). A stateful membership table and one endpoint snapshot can differ because of update timing, disappearance and reopening rules. The same endpoint assertion is not the promised parity evidence. **Fix:** specify the forward tradability predicate, freeze effective times and produce the comparison with disagreement reasons; include X5 explicitly in the exception inventory and acceptance artifacts.

52. **NIT — Probe: empty/error responses produce incidental exceptions instead of useful evidence.** [`_closing_prices`/`main`](../../scripts/probe_3740_slice_f_sources.py). Empty closing-price data or an empty comparison set breaks `most_common()[0]`/`_quantile`; the HF response is parsed without checking HTTP status. **Fix:** validate these responses and print an explicit unavailable/empty measurement with its population.

53. **NIT — A2: the replaced rule is attributed to the wrong document.** [Lines 199–200](../../docs/research/2026-10-10-3740-slice-f-forward-capture.md:199). “The parent’s ‘never inputs to the trial’ (round 1 of this spec)” conflates the parent contract with the prior slice-F draft. **Fix:** identify the original rule’s actual source and separately state any parent amendment.

**Overall assessment: checkpoint 1 remains open; not fit to build as a determinate forward-accrual contract.** The revision resolves several original mechanisms, but invariants 1–6 remain materially unmet. Invariant 7’s prohibition is now clear, and invariant 8’s acquisition-access row is specified, but those improvements do not repair the sealing and source-validity defects. Obligations 1–12 still contain unresolved capture, validation or substitution issues. X2, X3, X4, X6, X8 and X9 require substantive justification and measurement; A2/A3 require complete launch and estimand contracts before declaration.

## Round 3 (on v3, `dbcde782`)

**Checkpoint 1 remains open.** Massive resolves the central nominal-price and dated-action problems, but v3 still has sealing, scheduling, identity, action-history and termination defects. Premise 5’s plumbing rules are insufficient in several places.

I read only the permitted local files/ranges and the allowed external documentation. For Massive, I used the HTML equivalents where the Markdown URLs failed. No files were edited, and no live API measurements were performed.

All target line references below refer to [v3](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/2026-10-10-3740-slice-f-forward-capture.md). **APPLIED** means the original finding was resolved, including by removing its mechanism; it does not certify the replacement.

**Task A — round-2 findings 1–53**

1. **PARTIAL** — Full tile scanning replaces incomplete search, but authenticated scan boundaries, shard transitions and complete enumeration verification remain unspecified (L207–216).
2. **PARTIAL** — RFC 3161 and pinned trust material are added; the verification procedure and proof of timely log publication remain incomplete (L199–219).
3. **APPLIED** — Receipt chaining no longer depends on successful publication; inadmissibility has a separate transition (L197–219).
4. **PARTIAL** — Recovery states are specified, but crashes between filesystem completion and database state changes can still permit reacquisition (L135–150).
5. **PARTIAL** — Logical selections are enumerated, but are not independently sealed, and the declared schema cannot implement the stated component uniqueness directly (L156–193, L538–547).
6. **PARTIAL** — Separate status, return and final identities exist; delayed-status completion and the month’s required security set remain unresolved (L191–193, L435–469).
7. **PARTIAL** — Discovery polls are witnessed, but mutable download targets and simultaneous-version ordering remain unresolved (L178–181, L236–248).
8. **PARTIAL** — SEC kinds are registered; new-entrant bootstrap, completion dependencies and several recovery schedules remain incomplete (L173–177, L232–241).
9. **PARTIAL** — Index acquisition can continue after the evidence cutoff, but missing filing bodies and immutable status selection are not reconciled (L235, L467–469).
10. **APPLIED** — Rates are removed and fallback is restricted to named metadata kinds (L252–254).
11. **APPLIED** — The next-session confirmation dependency is removed; the replacement price acquisition can finish before the next open (L233, L374–384).
12. **PARTIAL** — Ordinary daily captures satisfy the intended timing, but MAX recovery and action-version selection can still introduce later evidence (L242–245, L412–414).
13. **APPLIED** — The independently valid close and 09:00 witness gate replace the impossible confirmation deadline (L478–480).
14. **APPLIED** — Daily price/action capture removes the formation-only candle dependency (L233, L427–431).
15. **PARTIAL** — Outcomes are clearer, but returns-witness failures contradict the parent’s required refusal scope without an amendment (L264–273).
16. **PARTIAL** — Ledger checks and explicit tables improve the interface; binding keys, persistence metadata and witness storage still disagree with the schema (L107–116, L538–553).
17. **PARTIAL** — Retirement is removed and market-wide capture helps; new-entrant SEC dependencies and historical identity coverage remain incomplete (L275–295).
18. **PARTIAL** — Lookback kinds, bootstrap and validator checks are added; historical mapping, dry-run witness verification and semantic compatibility remain incomplete (L287–297).
19. **PARTIAL** — Accepted securities enter validation, but security-class/context matching and missing-reference outcomes remain unspecified (L316–320).
20. **PARTIAL** — FIGIs improve identity, but ticker matching and exchange preference do not establish eToro instrument equivalence or RTH pricing semantics (L313–315, L491–499).
21. **APPLIED** — Dated Massive reference captures replace periodic-cover symbol linkage (L491–496).
22. **APPLIED** — Certification/8-A acceptance is no longer used as the listing-age input (L324–334).
23. **PARTIAL** — Validation now checks false eligibility, but symbol listing dates still lack security continuity and relisting treatment (L324–334).
24. **PARTIAL** — The unsupported upper-bound claim is removed; the parent’s required accession-level source comparison is still absent (L338–346).
25. **PARTIAL** — Direct SEC downloads and page completeness improve acquisition; ZIP persistence and a complete cutoff-eligible accession inventory remain unresolved (L350–358).
26. **PARTIAL** — Convention fixtures are specified; the conditional ME discontinuity detector is not whole-population reconciliation (L362–372).
27. **APPLIED** — Documented `adjusted=false` replaces the intraday nominal-price anchor (L50–54, L376–382).
28. **APPLIED** — Independent daily prices and dated actions remove the candle/close dependency cycle (L374–399).
29. **APPLIED** — Exact dated split products replace median factors and deadbands (L363–369).
30. **PARTIAL** — Independent candidate detectors are added, but their blind spots and incomplete action enumeration remain (L393–399).
31. **PARTIAL** — The 24-month action bootstrap supplies nominal duration, but lacks the historical ticker mappings needed to use it (L289–293, L388–389).
32. **PARTIAL** — A daily replacement is defined, but it changes the screened error class and mishandles events inside gaps (L415–423).
33. **PARTIAL** — Combined historical measurement is required and the vendor change is acknowledged; the historical proxy does not cover every implemented branch (L420–423).
34. **PARTIAL** — PWB basis conversion is removed; effective action-to-security identity remains underdefined (L388–389, L403–406, L531–533).
35. **APPLIED** — The PWB 90% vintage-selection denominator is removed.
36. **APPLIED** — The additional PWB entry screen and A3 are withdrawn (L567–568).
37. **APPLIED** — Price-return substitution is removed; invalid total returns remain incomplete inputs (L432–434, L618–620).
38. **APPLIED** — F9 explicitly restricts action evidence to bindings available by the returns cutoff (L429–431).
39. **PARTIAL** — Separate validation strata exist, but the dividend reference, missing-event coverage and termination test remain incomplete (L437–446).
40. **PARTIAL** — Gross/net formulas and close checks are explicit; action-row validity and partial-period boundary cases remain incomplete (L378–379, L403–407, L466).
41. **PARTIAL** — Whole-response acquisition failure is separated from economic status; row defects, reference failure and cessation timing still lack complete treatment (L450–458).
42. **PARTIAL** — `end_bar` precedes the first gap, but missing initial boundaries and incomplete partial returns remain unresolved (L453–466).
43. **PARTIAL** — `TerminationEvidence` is named, but Q-suffix classification remains unreachable in the unmatched case and amendment precedence contradicts the cited matcher (L456–465).
44. **APPLIED** — Provenance versions and first-accepted logical selections are separated (L179–188, L473–487).
45. **PARTIAL** — X9 now requires measurement; reconstructed publication vintages and a fixed-lag substitute do not establish the historical treatment claimed (L503–513).
46. **APPLIED** — One cutoff selection is made at a specified time and cannot be replaced (L503–506).
47. **PARTIAL** — Missed mandatory captures now fail, and artifacts are enumerated; event-validation coverage remains incomplete (L580–587).
48. **PARTIAL** — Dedicated Massive quota, timeouts and tick budgets improve runtime; omitted calls, early-close windows and dependent scheduling remain unresolved (L117–127, L230–235).
49. **APPLIED** — Full retention, peak storage, margin and low-space preflight are specified (L114, L596–597).
50. **PARTIAL** — More drills exist, but edit/revert and incomplete enumeration are absent, and duplicate handling conflicts with the verifier (L588–595).
51. **APPLIED** — X5 states its replacement predicate and requires disagreement reasons in accepted dry-run artifacts (L560, L583).
52. **NOT APPLIED** — V3 documents no empty/error-response handling fix; the probe itself was outside this review’s permitted reads (L663).
53. **APPLIED** — A2 correctly attributes the replaced restriction to v1 of slice F (L299).

**Task B — defects in v3**

1. **BLOCKING — The specified signing algorithm does not implement Rekor v2’s supported workflow.**  
   **L199–202.** V3 specifies plain Ed25519 over a SHA-256 digest. Rekor v2 supports HashedRekord v0.0.2; Sigstore’s client specification excludes plain Ed25519 from that construction. **Fix:** specify a supported signing algorithm, exact signed bytes, hash algorithm, request encoding and verification procedure; exercise the actual service before accepting the witness design. [Rekor v2 specification](https://raw.githubusercontent.com/sigstore/architecture-docs/main/rekor-v2-spec.md), [Sigstore client specification](https://raw.githubusercontent.com/sigstore/architecture-docs/main/client-spec.md).

2. **BLOCKING — Logical selections are immutable only by local assertion.**  
   **L159–161, L183–193, L197–219, L546–547.** Receipts witness acquisitions, but no operation receipts or witnesses a `forward_capture_selections` row. A cutoff vintage, factor selection or formation manifest can therefore be changed locally without changing an acquisition receipt. **Fix:** give selections canonical signed receipts, identities, decision deadlines and external commitments; verify their uniqueness and referenced components.

3. **BLOCKING — The signed receipt payload is not defined sufficiently to protect acquisition meaning.**  
   **L137–140, L157–158, L197–204, L539–547.** The schema separates mutable attempt metadata, component hashes and witness material, without specifying exactly which identity, request, validation result and timestamps the receipt digest commits. Hashing the eventual row would also include fields populated after signing. **Fix:** define canonical immutable receipt bytes, including complete identity and component inventory; store publication evidence separately.

4. **BLOCKING — The declared schema cannot directly enforce the declared binding constraint.**  
   **L156–158, L539–551.** The component table lacks `trial`, `kind`, `source_id` and `period_key`, although the partial unique index is required over those fields. The attempt table also omits the component hashes said to be stored at `persisted`. Hashing only the declared column interface would not establish the required uniqueness constraints. **Fix:** provide an implementable normalized schema, transactional binding operation and constraint/index validation.

5. **BLOCKING — The crash protocol still has an observation-replacement interval.**  
   **L135–150.** Files are finalized before the database marks the attempt `persisted`. A crash between those steps leaves complete response bytes under a state whose prescribed outcome permits abandonment and another acquisition. Multi-component attempts have the same problem after some components finish. **Fix:** recover every completed file deterministically before retrying; define filesystem/database transition ordering, directory durability and component-level recovery.

6. **BLOCKING — Pagination lacks a coherent persistence and snapshot contract.**  
   **L139–144, L166–169.** Fetching `next_url` requires parsing a page, while `persisted` appears to require completion of every component. Retrying later pages can also combine pages from different mutable source states. **Fix:** persist and receipt pages independently before parsing; commit the pagination inventory and ordering; define duplicate, missing, shifting and incomplete-page outcomes.

7. **BLOCKING — A timely TSA token does not prove timely append-only publication.**  
   **L203–219.** The token timestamps the receipt digest, while admissibility only requires eventual valid log inclusion. A receipt can be timestamped before the deadline and selectively logged afterward. The verifier also does not explicitly bind and authenticate the token’s imprint to the receipt. **Fix:** verify the token and trusted chain against the exact payload, and obtain deadline evidence covering log inclusion—such as a timestamped inclusion checkpoint containing the receipt.

8. **BLOCKING — “Full scan” lacks authenticated, independently fixed boundaries.**  
   **L207–216.** A genesis index is shard-specific; subsequent shards need their own ranges. A locally selected stale final checkpoint can omit a deleted tail. Reading tiles without validating entry bundles and their hashes against authenticated checkpoints does not establish completeness. Consistency proofs also cannot connect unrelated shard trees. **Fix:** specify per-shard start/end checkpoints, independently obtain the terminal checkpoints, verify all complete and partial tiles, and define shard-transition verification. [Rekor v2 client guidance](https://github.com/sigstore/rekor-tiles/blob/main/CLIENTS.md).

9. **WARNING — Duplicate-publication acceptance contradicts one-to-one enumeration.**  
   **L209, L213–214, L589–591.** The verifier requires receipts and logged entries to match one to one, while the drill permits two entries for one receipt. **Fix:** distinguish duplicate entries for identical canonical receipt bytes from distinct receipts claiming one identity, and use the same rule in publication recovery and verification.

10. **BLOCKING — Failed and abandoned attempts are not necessarily externally committed.**  
    **L135–152, L246–248, L580–581.** A crash in `intent` or `persisting` leaves only local attempt/access rows. Yet poll failures and abandoned-attempt counts are treated as immutable evidence. Deleting an unreceipted attempt is invisible to the receipt scan. **Fix:** externally commit attempt intent or an independently retained attempt inventory, then append completion/abandonment records without overwriting it.

11. **BLOCKING — Witness failure changes the parent’s verdict without a declared amendment.**  
    **L264–273, L567–568.** V3 treats inadmissible returns/B1/factor receipts as unbound inputs potentially ending in `INPUT_UNAVAILABLE`. Parent L365 assigns failed sealing of those inputs to `CAPTURE_AMBIGUOUS`. Only A1/A2 are declared. **Fix:** preserve the parent’s outcome or explicitly amend that lifecycle row, with exact distinction between missing acquisition and failed witness.

12. **BLOCKING — Early-close reference and SEC windows are impossible.**  
    **L230–232.** Only the eToro window has an early-close start. On a 13:00 close, the reference window runs 13:30–12:40 and the SEC window 14:00–12:40. **Fix:** define executable early-close windows for every dependent kind and validate the complete pinned calendar.

13. **BLOCKING — New-security dependency discovery can finish at the SEC deadline.**  
    **L231–232, L279–282.** The ticker window and SEC window end simultaneously. A FIGI discovered near that end cannot receive the same-session SEC work promised by the superset rule. **Fix:** set explicit dependency deadlines and reserved completion intervals, or define a deterministic deferred-eligibility rule.

14. **BLOCKING — Premise 5(g)’s workload omits a potentially dominant acquisition kind.**  
    **L123–124, L171, L231, L575–576.** The estimate omits per-FIGI `massive_events` calls. If these run daily as the registry/window imply, 1,000 securities alone require at least 250 minutes at four calls/minute—longer than the ordinary reference window. Initial overview/bootstrap calls also need separate budgeting. **Fix:** specify event-query cadence and population, then gate the complete dependency workload, including bootstrap and growth.

15. **WARNING — Runtime acceptance does not cover the complete constrained system.**  
    **L117–127, L572–576.** The plumbing capacity rule addresses Massive calls, while SEC ZIPs, filing work, witness publication and shared eToro/SEC contention also consume deadlines. The eight-minute budget lacks component-level interruption/resumption rules and witness priority. **Fix:** test complete ticks under defined contention, with durable progress, deadline priorities and reserved witness capacity.

16. **BLOCKING — Recovery and bootstrap work lack complete identities and executable windows.**  
    **L163–181, L242–245, L289–293, L467–469.** `massive_grouped_recovery`, historical action bootstrap and repeated SEC-index recovery are absent from the registry or lack full start/end/witness/retry rules. “A new acquisition” does not explain its relationship to the original immutable identity. **Fix:** register each kind and its logical selection, including all deadline and collision rules.

17. **BLOCKING — Discovery does not freeze the bytes later selected.**  
    **L178–181, L236–248, L475–487.** A poll may identify a mutable URL whose contents change before the later download. French/JKP content versions cannot be identified from a URL alone. Multiple datasets discovered in one poll also lack a total ordering. **Fix:** bind content-bearing poll bytes directly, or bind immutable version locators plus expected hashes; define ordering and unavailable-version outcomes.

18. **BLOCKING — MAX recovery can violate the formation cutoff.**  
    **L242–245, L412–414.** Recovery remains open through the month’s later returns cutoff and is admitted for “MAX lookback” without restricting eligibility to the formation’s pre-close deadline. The claim that all earlier-session inputs bind in their own windows is therefore false. **Fix:** determine admissibility per use: later recovery may serve holding returns, but a formation may reference only recoveries witnessed before its applicable cutoff.

19. **BLOCKING — Monthly completeness has no frozen denominator.**  
    **L191–193, L277–285, L435–436.** The superset grows forever, books are not formed, yet `month_final` waits for “every” return selection. It is unspecified whether later entrants enlarge an earlier month’s required set or which securities need instrument-level results. **Fix:** bind a finite per-month candidate/continuing-security inventory independently of returns, and define completion against that inventory.

20. **BLOCKING — Delayed SEC evidence conflicts with cutoff-time status binding.**  
    **L191–192, L235, L435, L460–469.** Status is selected at the returns cutoff, but eligible indexes and Form 25 bodies may arrive afterward. F10 blocks on missing indexes, not missing eligible filing bodies. Amendment form variants are not explicitly included in the acquisition filter. **Fix:** freeze the evidence-eligibility cutoff separately from completion; require all eligible indexes and filing bodies before binding status, including explicit amendment handling.

21. **BLOCKING — “Latest amendment wins” contradicts the cited source rule.**  
    **L461–463.** The permitted [matcher range](/Users/lukebradford/Dev/.ebull-autonomy/app/services/research_corpus_ingest.py:1566) explicitly rejects conflicting provisions/suspension dates and says latest-filed or `/A` precedence would invent a source rule. V3 introduces exactly that precedence. **Fix:** preserve the matcher’s conflict refusal, or establish and cite the applicable documented amendment treatment before changing it.

22. **BLOCKING — The Q-suffix terminal branch is unreachable.**  
    **L456–465.** An unmatched ceased security is assigned `coverage_exit` before `classify_termination` can use `q_suffix`. Constructing that field later does not reproduce the classifier’s unmatched-Q behavior. **Fix:** integrate the complete termination-evidence classification into status selection, including unlinked Q-suffix, conflicting and unverified cases.

23. **BLOCKING — Cessation and filing evidence are not tied to the same effective event.**  
    **L453–465.** Being inactive by the cutoff can retrospectively classify an earlier missing session as cessation, even if trading resumed before a later delisting. A matched Form 25 also lacks an explicit suspension/effective-event relationship to the cessation. Massive documents `delisted_utc` as the last traded date, not a generic filing-effective date. **Fix:** construct dated trading/cessation intervals and match the terminating event to the relevant security interval. [Massive All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers).

24. **BLOCKING — Partial-month boundary cases remain undefined.**  
    **L427–434, L453–466.** There is no executable rule for no usable bar after entry, a missing initial boundary, an invalid action before `end_bar`, or a resumed series after its first gap. A status can exist while its required partial total return is still invalid. **Fix:** enumerate these cases, preserve incomplete-input status where appropriate, and require valid partial-return inputs before monthly completion.

25. **BLOCKING — The promised late return completion has no acquisition path.**  
    **L192, L242–245, L268, L429–436.** The registry permits later return completion within nine months, but grouped recovery ends at the returns cutoff and F9 permits completion only inside that returns window. **Fix:** specify which cutoff-eligible missing components can be acquired later and how their eligibility is established, or remove the contradictory completion promise and explicitly classify permanent failures.

26. **BLOCKING — Historical action bootstrap lacks the identity history required to use it.**  
    **L289–295, L388–389.** Two years of split events are fetched, but ticker snapshots begin only at the dry-run start. An older event cannot be mapped by its pre-event ticker using those captures. Literal “day before execution” also fails across weekends/holidays. **Fix:** capture historical reference mappings for required event dates and security intervals; use the prior trading session where appropriate.

27. **BLOCKING — Latest rolling action snapshots omit required historical events.**  
    **L168–169, L363–365, L429–431.** Each daily action response covers only ±30 days. F5 needs up to 15 months, while the latest snapshot at a returns cutoff can omit dividends/splits from the beginning of the holding month. The initial bootstrap does not fill later gaps indefinitely. **Fix:** maintain a cumulative, identity-deduplicated event inventory and select its versioned state as of each cutoff.

28. **BLOCKING — Action revision precedence is contradictory.**  
    **L363–365, L390–392, L429–431.** F7 binds the first event observation, while F5/F9 select latest bindings. Neither establishes cancellation, changed execution date, disappearing event or superseding-ID treatment. An announced event that is later cancelled can remain economically applied. **Fix:** define immutable event revisions and deterministic as-of selections, including cancellation and unresolved-conflict outcomes.

29. **BLOCKING — Dividend identity and deduplication are unspecified.**  
    **L169, L403–406, L429–431.** Splits have a ticker-to-FIGI rule, but dividends do not. Summing amounts without a dated mapping and event-ID selection can attach an old ticker’s dividend to a new occupant or count one event multiple times across captures. **Fix:** define effective-dated dividend mapping and exactly-once event selection before calculating `D_d`.

30. **BLOCKING — Required action fields have no deterministic validators.**  
    **L145–148, L388–407.** Calculations require event IDs, dates, tickers, positive split ratios and finite amounts/currencies. Several are optional in Massive’s schema. Missing fields, duplicate/conflicting IDs, invalid ratios and malformed amounts have no specified scope or outcome. **Fix:** freeze per-row validators and map each defect to affected security/date inputs without discarding unrelated valid rows. [Massive Splits](https://massive.com/docs/rest/stocks/corporate-actions/splits), [Massive Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends).

31. **WARNING — The claimed dividend share-basis rule is not documented.**  
    **L406–407.** Massive describes `cash_amount` as the original per-share amount in its currency; that does not establish v3’s stronger “before any split that day” assertion. Same-day cases are refused anyway, making that assertion unnecessary. **Fix:** remove the unsupported interpretation and document the basis needed for every accepted case, including intervening splits; retain explicit refusal where the source contract is insufficient. [Massive Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends).

32. **BLOCKING — First non-empty grouped data need not be a complete acceptable observation.**  
    **L75–76, L145–151, L378–384, L573–574.** The plumbing test measures first non-empty availability, while first persistence permanently binds the response. A partial successful response can therefore bind before complete data becomes available. Missing rows then become exclusions or coverage gaps. **Fix:** define response-level readiness/completeness rules, distinguish a valid empty result from unavailable data, and measure the actual first-binding policy prospectively.

33. **BLOCKING — F6 turns one row defect into a whole-response failure.**  
    **L147–150, L378–379.** A duplicate ticker produces `row_defect`, but F6 requires the response to be `valid`. Read literally, one bad ticker invalidates every otherwise usable close, contradicting scoped row handling. **Fix:** admit unaffected rows from a bound `row_defect` component and specify deterministic ticker-level defects.

34. **BLOCKING — Unadjusted aggregate close is asserted to reproduce RTH/exchange-close treatment.**  
    **L313–315, L376–382.** `adjusted=false` establishes split basis. The allowed endpoint documentation defines `c` for the aggregate period; it does not, by itself, establish the precise RTH or market-on-close print assumed by the parent. Dry-run price agreement cannot supply that contract. **Fix:** establish the documented session/eligible-trade/closing-print semantics or declare and review a pricing-session exception. [Massive Daily Market Summary](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary).

35. **BLOCKING — Missing reference identity has no complete forward outcome.**  
    **L279–285, L376–377, L453–455, L491–499.** `composite_figi` is optional, and a required daily ticker snapshot may be absent or defective. The spec handles an unmatched formation symbol, but not all consequences for existing holdings, action mapping and status classification. **Fix:** distinguish missing capture, missing source identity, ambiguous mapping and actual cessation, with immutable per-session mappings and explicit refusal/completion rules. [Massive All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers).

36. **BLOCKING — F1’s source substitution and validation joins are incompletely declared.**  
    **L305–320, L555–568.** The parent specifies SEC filer/SIC evidence after eToro type; v3 principally adopts Massive’s type taxonomy without listing that source substitution. Its cover-title comparison also lacks class/symbol/context joins and rules for multiple or conflicting titles. **Fix:** declare the classifier substitution, freeze explicit accepted codes and security-level joins, and specify unknown/conflicting validation outcomes.

37. **BLOCKING — Symbol first-listing date does not establish the required security listing history.**  
    **L324–334.** The documented field is the symbol’s first listing date. V3 has no operative continuity, successor, relisting or reused-symbol rule. Checking the dry-run population cannot validate future entrants; two years of experimental ticker-change coverage cannot establish a 36-month history. **Fix:** require security-specific continuity evidence or explicitly adopt and measure a narrower proxy with unknown-age handling. [Massive Overview](https://massive.com/docs/rest/stocks/tickers/ticker-overview), [Massive Ticker Events](https://massive.com/docs/rest/stocks/corporate-actions/ticker-events).

38. **BLOCKING — Header acquisition does not guarantee the latest eligible accession for new entrants.**  
    **L239–241, L279–285, L338–339, L357–358.** Header bootstrap occurs only at dry-run start. A later entrant’s latest filing may predate all captured indexes, while delayed indexes can conceal a newer eligible accession. **Fix:** bootstrap each newly required CIK and establish a complete eligible-accession inventory before choosing the latest header; otherwise retain `sic_unloaded`.

39. **BLOCKING — F3 omits the parent’s required source-compatibility evidence.**  
    **L342–346, L565.** The parent requires header-versus-SUB comparison over every accession in overlapping quarters, including unmatched accessions. V3 instead compares latest header SIC against current company-record SIC at dry-run formations, then claims exact reproduction. **Fix:** perform the prescribed accession-level comparison and provide the header-field documentation, or explicitly amend that obligation.

40. **BLOCKING — SEC ZIP handling conflicts with the acquisition state machine.**  
    **L137–144, L176, L350–354.** The response is a ZIP, but only extracted members bind and the ZIP is not retained. Extraction necessarily parses the response; the protocol does not specify when that parsing is permitted, how ZIP hashes are verified later, or how crash recovery preserves the same source bytes. **Fix:** define a durable ZIP acquisition followed by a receipted extraction manifest and controlled disposal, or retain the ZIP.

41. **WARNING — The ME validation gate checks only a selected error class.**  
    **L370–372.** The detector omits smaller missed distributions, offsetting errors, first formations and cases with substantial real price movement. Passing it does not establish the whole-population reconciliation required by obligations 5/7. **Fix:** reconcile every required share basis and event product, reporting the detector as supplementary evidence rather than completeness proof.

42. **WARNING — X3 changes the screened error class, beyond its stated rationale.**  
    **L415–423, L559.** #3621 screens adjustment-ratio moves without intervening stamps; v3 tests price consistency where an event exists. Unstamped errors can now pass, while a correctly stamped action with a genuine large price move can be screened. **Fix:** state those changed false-positive/false-negative classes explicitly and require the historical comparison to cover the complete replacement, including its ambiguity branches, before accepting X3.

43. **BLOCKING — The replacement MAX screen mishandles actions inside gaps.**  
    **L417–419.** It compares consecutive usable bars across gaps using only `k_d`, the later session’s split product. An action inside the gap is omitted; if no event occurs on the later session, the check may not run at all. **Fix:** use the cumulative applicable event product over the entire comparison interval and define event/currency ambiguity over that interval.

44. **BLOCKING — Independent return validation is not reproducible enough to serve as the stated gate.**  
    **L437–446, L552–553.** Mutable `price_daily` values are used without a frozen comparison snapshot or precise reference construction. Missing reference observations have no pass/fail treatment. The tests do not independently validate complete monthly and partial-period total-return construction. **Fix:** freeze reference bytes, identity/basis conversions, denominators, uncovered cases and expected daily/monthly/partial-return calculations before comparison.

45. **WARNING — Dividend validation samples supplied events and tolerates a wrong ex-date.**  
    **L443–445.** Sampling only Massive-listed dividends cannot detect omitted dividends. Allowing a one-session date error can accept incorrect MAX timing or move income across a month boundary. **Fix:** independently enumerate a frozen validation population including missing-event cases; require exact ex-date agreement or explicit adjudication of every discrepancy. Treat sample agreement only as compatibility evidence.

46. **BLOCKING — The termination validation gate points to a nonexistent test specification.**  
    **L446, L448–469, L582, L585–587.** F9 requires “F10’s table,” but F10 contains no reference table, expected results, tolerances or acceptance criteria. One observed Form 25 does not exercise amendment conflicts, unmatched Q names, interior gaps or collector failure. **Fix:** supply the promised independent termination table and deterministic fixtures/pass rules for each branch.

47. **BLOCKING — B1 adds a required exit-price dependency absent from the parent.**  
    **L186, L268, L481.** The parent uses SPY’s entry raw close for B1’s one-band transaction costs; IVV N-PORT supplies returns. Making `b1_exit_close` a mandatory selection can refuse an otherwise complete trial for an unnecessary SPY observation. **Fix:** remove that dependency or explicitly amend and justify the additional B1 requirement.

48. **BLOCKING — F14’s historical measurement can substitute invented publication history.**  
    **L509–513.** A current `Last-Modified` observation does not reconstruct historical vintages. A fixed lag inferred from one current observation is a scenario, not “the row a reader would have had” at each historical cutoff. **Fix:** use retained historical versions with actual availability evidence, or label and analyze fixed-lag scenarios separately while leaving the actual historical availability effect unmeasured.

49. **WARNING — X9’s rationale does not establish the necessity or limits of carry-forward.**  
    **L503–513, L617.** Publication lag does not by itself rule out acquiring the actual holding-month row during the parent’s completion period. Carried bounds also have no maximum age or special missing-vintage treatment. **Fix:** prefer the documented holding-month row where the completion contract permits; otherwise make the estimand change, age policy and stress-period effects explicit for acceptance.

50. **BLOCKING — Premise 5(e) has no substantive pass rule.**  
    **L81–82, L575.** “Documented” accepts any observed filter behavior, future-event coverage or non-USD frequency. Yet correct interval selection and usable action amounts directly determine ME and total-return validity. **Fix:** specify assertions for filter inclusivity, pagination, required fields, event timing/revisions, currency coverage and structural refusal exposure, with explicit failed-gate consequences.

51. **WARNING — Premise 5(a)’s timing test is underspecified and cannot establish future availability.**  
    **L75–76, L572–574.** Retrospective responses cannot reveal when data first became available. Even a prospective two-week success is operational evidence, not a future deadline guarantee. **Fix:** require timestamped prospective observations under the actual polling/binding policy, report the sample and latency distribution, and retain explicit operational failure handling.

52. **WARNING — Premise 5(c)/(d) permits unexplained coverage losses in unfrozen samples.**  
    **L78–80, L574–575.** A 95% symbol-match rate says nothing about FIGI/CIK/action/history usability. Eighteen successes from twenty delisted names permits two unexplained failures and is not a survivorship-coverage guarantee. Selection dates, seeds, strata and denominators are not frozen. **Fix:** freeze those populations, distinguish identifier matching from complete usability, adjudicate failures and quantify the resulting population restriction.

53. **WARNING — Several dry-run gates do not establish the continuing validity implied by “reproduces.”**  
    **L316–334, L370–399, L497–499, L565, L582.** The finite dry-run population can establish compatibility, but cannot guarantee future classification, listing continuity, action completeness or mapping correctness. Future entrants do not receive the same independent adjudication gate. **Fix:** distinguish documented source guarantees from sampled checks, and define deterministic forward handling for newly encountered or unresolved cases.

54. **WARNING — Witness drills still omit required or load-bearing failures.**  
    **L588–595.** Edit/revert and incomplete enumeration are absent. Several listed drills describe observations without a final expected state/code; selection tampering is also untested despite its role in evaluation. **Fix:** enumerate each mutation, truncation, stale-checkpoint, shard-transition and selection-tampering drill with the exact expected verifier result.

55. **WARNING — Invariant 8’s complete lifecycle is not operationalized.**  
    **L137, L152, L548–549.** Acquisition read rows are specified, but verdict `evaluate` timing and the parent’s abandoned-run classifier are not mapped to the new attempt states. The inherited requirement remains applicable. **Fix:** specify evaluation-access recording before evaluation reads and a deterministic classification for every failed/recovered attempt.

56. **NIT — Review status contradicts the actual round-3 premise state.**  
    **L5, L48, L672.** The document says round 3 runs after Massive premises are measured, although this review intentionally precedes those measurements. **Fix:** label this as a design review with measurement gates still pending.

57. **NIT — The disposition table misattributes finding numbers.**  
    **L637–645.** The F6 row groups findings 18 and 19 with nominal-price findings, then separately assigns those same numbers to A2 and F1. **Fix:** correct the cross-reference mapping so the table can be audited mechanically.

**Parent-contract cross-check**

| Requirement | Result against v3 |
|---|---|
| Sealing invariant 1 — cutoffs | Unmet: unsealed selections, MAX recovery and delayed-status timing; findings 2, 18–20. |
| Invariant 2 — immutability | Unmet: crash recovery, pagination and contradictory event selection; 5–6, 28. |
| Invariant 3 — unique identities | Unmet: schema, missing recovery identities and selection sealing; 2–4, 16. |
| Invariant 4 — executable windows | Unmet: early closes, dependency timing and incomplete recovery schedules; 12–16. |
| Invariant 5 — external witness | Unmet: signing workflow, timely publication and scan completeness; 1–10, 54. |
| Invariant 6 — splicing | Unmet: historical action identity, dividend mapping and session semantics; 26, 29, 34–35. |
| Invariant 7 — no interim result | Specified adequately: portfolio formation/valuation is prohibited; instrument-level checks are distinguished. |
| Invariant 8 — access rows | Acquisition requirement specified; evaluation and abandoned-attempt lifecycle need completion; 55. |
| Obligation 1 — security type | Incomplete source exception, security-level validation and RTH treatment; 34–36. |
| Obligation 2 — listing age | Symbol-date proxy does not establish security history; 37. |
| Obligation 3 — SIC | Latest-accession acquisition and required source comparison incomplete; 38–39. |
| Obligation 4 — accounting | Direct capture improves provenance; persistence/completeness protocol incomplete; 20, 38, 40. |
| Obligation 5 — shares/ME | Formula/fixtures improve; historical action coverage and reconciliation incomplete; 26–28, 41. |
| Obligation 6 — raw close | Split basis is documented; readiness, row validity and session semantics remain unresolved; 32–34. |
| Obligation 7 — split events | Dated source exists; historical mapping, cumulative inventory and revision handling incomplete; 26–30. |
| Obligation 8 — MAX | Cutoff and across-gap defects; changed screen needs explicit acceptance; 18, 42–43. |
| Obligation 9 — holding returns | Action completeness, identity, partial returns and validation remain incomplete; 24–31, 44–45. |
| Obligation 10 — terminations | Matcher precedence, Q branch, timing and validation defective; 20–24, 46. |
| Obligation 11 — B1 | First-accepted selection improves; sealing/discovery issues and extra exit dependency remain; 2, 17, 47. |
| Obligation 12 — factors | First-accepted rule is stated correctly; common selection-sealing/discovery defects remain; 2, 17. |
| Prospective dry run | Three month-ends and hard capture gates are retained; plumbing and validation acceptance are incomplete; 14–15, 44–46, 50–54. |
| Parity | Required parent parity and X5 disagreement tables are included; neither establishes source validity outside the measured population. |

**Overall assessment:** v3 is a substantial source-design improvement, but is not yet a determinate forward-accrual contract ready for implementation approval. The missing API key is not itself the defect: several proposed tests can pass while required inputs remain unusable or incorrectly sealed. Repair the protocol and source-rule conflicts first, then run the strengthened plumbing and prospective validation gates before closing checkpoint 1.

## Round 4 (on v4, `59742928`)

Checkpoint 1 should remain open. V4 makes substantial repairs, but the protocol and source rules still have unresolved defects. Premise 5 being unmeasured is acceptable at this stage; several proposed pass rules are not sufficient to gate the dependencies they support.

I read only the permitted local files/ranges and external documentation. Massive’s Markdown URLs failed, so I consulted their HTML equivalents. No files were edited and no API measurements were performed.

All **L** references below refer to [the v4 target](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/2026-10-10-3740-slice-f-forward-capture.md). **APPLIED** means the original finding was resolved; it does not certify the replacement design.

**Task A — round-3 findings 1–57**

1. **PARTIAL** — Supported ECDSA replaces Ed25519, but the signing/request encoding and leaf-to-record verification remain incomplete (L181–190).
2. **PARTIAL** — Selections are witnessed records, but most lack explicit witness deadlines and some decision times conflict (L119, L165–175, L440–441).
3. **APPLIED** — Canonical chained records commit identities, requests, component hashes, validation outcomes and selection values (L109–119).
4. **PARTIAL** — Identity columns and indexes are specified; the proposed `information_schema` hash cannot cover the declared indexes as written (L545–560).
5. **APPLIED** — Recovery adopts finalized files before permitting another attempt, with component and directory durability specified (L117, L123–126).
6. **PARTIAL** — Persist-before-pagination is fixed; retry rules contradict unit binding, and mutable-page omissions remain undetected (L122–139).
7. **APPLIED** — A timestamped including checkpoint now supplies deadline evidence for log inclusion (L185–190).
8. **PARTIAL** — Per-shard verification improves, but shard/start discovery remains locally bounded and freshness is not established by consistency (L191–197).
9. **PARTIAL** — Identical-request deduplication is adopted, but regenerated ECDSA signatures and cross-shard retries remain unspecified (L181–196).
10. **PARTIAL** — Intent and abandonment records exist, but an acquisition can precede external commitment of its intent (L94, L116, L179–190).
11. **APPLIED** — Returns-side witness failures now preserve the parent’s `CAPTURE_AMBIGUOUS` outcome (L250–256).
12. **APPLIED** — Early-close windows are executable, with calendar validation required (L211–224).
13. **APPLIED** — `sec_pending` supplies a deterministic deferred-entry outcome for unfinished SEC dependencies (L226–230).
14. **PARTIAL** — Event-query cadence and bootstrap budgeting improve; the workload still omits required validation and evidence acquisitions (L98–103, L596–597).
15. **PARTIAL** — Queue priorities and full-day testing are added, but burst workload and defined contention are not covered adequately (L94–103, L596–597).
16. **PARTIAL** — Recovery identities are added; bootstrap windows remain open-ended and some required acquisitions have no registered kind (L143–163, L221).
17. **PARTIAL** — French/JKP discovery binds bytes; N-PORT’s mutable listing-to-download gap remains (L220, L231–233).
18. **APPLIED** — Formation MAX explicitly prohibits recovery bindings (L278, L414–415).
19. **PARTIAL** — A finite monthly inventory is introduced, but missed-formation construction and completion scope remain ambiguous (L431–441).
20. **PARTIAL** — Status waits for indexes and amendment bodies, but historical/new-entrant evidence discovery is incomplete (L217–218, L471–473).
21. **APPLIED** — Invented latest-amendment precedence is removed in favor of conflict refusal (L465–470).
22. **APPLIED** — Unlinked Q-suffix evidence can now reach the termination classifier (L459–470).
23. **PARTIAL** — Cessation is dated, but missing bars can still backdate it and the filing-to-event relationship remains unspecified (L455–473).
24. **PARTIAL** — Several boundary cases are added; terminal precedence over an earlier gap still produces incorrect or incomplete treatment (L458–476).
25. **APPLIED** — The nonexistent late-price-completion promise is removed and permanent price incompleteness is stated (L436–439).
26. **PARTIAL** — Historical reference acquisitions are added, but the assumed pre-event ticker mapping is not established (L272–274, L387–389).
27. **APPLIED** — A cumulative inventory replaces selection from only the latest rolling snapshot (L379–383).
28. **PARTIAL** — Revisions and withdrawal exist; disappearance, reappearance and changed-ID semantics remain incomplete (L379–391).
29. **PARTIAL** — Dividends receive IDs and mappings, but same-date records and ticker changes can still miscount or misattribute income (L387–391).
30. **PARTIAL** — Required-field validators exist, but defects missing their own ticker/date/ID cannot receive the promised scope (L384–389).
31. **APPLIED** — The unsupported “before any split that day” assertion is removed (L405–410).
32. **PARTIAL** — Readiness is explicit, but the 90% count heuristic cannot establish the claimed completeness (L362–365).
33. **APPLIED** — Unaffected rows of a duplicate-ticker component remain usable (L366–367).
34. **APPLIED** — X2 explicitly declares the pricing-session substitution; its sampled equality test has separate limitations (L368–373, L568).
35. **PARTIAL** — Missing-FIGI handling improves; absent/defective daily reference snapshots and identity continuity remain underdefined (L501–505).
36. **PARTIAL** — X1 is declared and cover matching is added, but evidence acquisition and ambiguous class/context joins remain unresolved (L285–305).
37. **PARTIAL** — X8 states a proxy and unknown-age outcomes, but the proposed exclusions lack complete historical evidence (L309–319).
38. **PARTIAL** — Header bootstrap is added per CIK; latest-eligible-accession completeness is still not a gate (L226–230, L323–324).
39. **PARTIAL** — Accession-level comparison is added, but documentation and the restricted four-quarter comparison do not fully meet the parent (L327–331).
40. **PARTIAL** — ZIP persistence fixes extraction ordering; deletion still removes the source needed to audit derivation (L335–338).
41. **PARTIAL** — Reconciliation improves but remains conditional on supplied events, later cover counts and a 20% threshold (L353–356).
42. **PARTIAL** — Changed error classes are acknowledged, but dividend-stamp behavior and the complete replacement’s historical measurement remain incomplete (L416–427).
43. **APPLIED** — Across-gap checks now inspect the full interval `(p,q]` (L405–421).
44. **PARTIAL** — Frozen exports improve reproducibility; monthly/partial checks are not independent and missing-reference cases lack acceptance rules (L442–449).
45. **PARTIAL** — Exact sampled ex-dates are required; the declared-dividend comparison does not independently enumerate ex-date dividends (L392–401).
46. **PARTIAL** — A termination table and fixtures now exist, but independent expected results and several boundary branches remain unspecified (L477–480).
47. **APPLIED** — The unnecessary B1 exit-close dependency is removed (L488–491).
48. **APPLIED** — Historical lag calculations are correctly labeled scenarios, with actual availability effects left unmeasured (L520–523).
49. **APPLIED** — Own-month rows are preferred through the completion limit and carry age is bounded (L514–519).
50. **PARTIAL** — Filter and pagination assertions are added; required fields, timing, currency and structural exposure remain reporting-only (L590–594).
51. **APPLIED** — Actual-policy timing observations and a distribution replace retrospective availability inference; future guarantees are disclaimed (L581–582, L623–624).
52. **PARTIAL** — Populations broaden, but denominators remain source-selected and permitted losses do not establish complete usability (L584–589).
53. **PARTIAL** — Compatibility language is corrected, but several future cases still lack deterministic handling (L623–624).
54. **PARTIAL** — More drills are specified; stale-checkpoint expectations are wrong and destructive drills contaminate the reusable dry-run key (L609–617).
55. **APPLIED** — Evaluation access timing and abandoned-attempt classification are explicitly mapped (L116, L132–133).
56. **APPLIED** — Status correctly identifies an unmeasured-premise design review (L3–5).
57. **APPLIED** — The disposition numbering now corresponds to round 3 (L645–694).

**Task B — defects in v4**

1. **BLOCKING — Record deadlines and formation decision time are not fully defined.**  
   **L165–175, L203–221, L527–528.** Acquisition windows do not assign witness deadlines to most selections, `close`, `preflight`, genesis or disposal records. “End of s(M)’s last window” is ambiguous when some windows extend for days or months. **Fix:** specify a fixed formation decision timestamp and acquisition-independent creation/publication deadlines for every record type, including delayed selections.

2. **BLOCKING — Pagination retry contradicts component binding.**  
   **L123–139, L245.** If page 1 is valid and page 2 fails, L126 prohibits another attempt, while L128 requires restarting at page 1. Classifying the valid page as transport-defective instead would evade first-binding protection. **Fix:** define one coherent set-level protocol that preserves observed pages and explicitly states whether and how incomplete sets can be completed.

3. **BLOCKING — A single pagination attempt does not establish a coherent source snapshot.**  
   **L127–129, L590–594.** A mutable collection can shift between page requests, omitting an event without producing a duplicate. “Every row once” cannot be tested against the returned pages alone. **Fix:** establish snapshot/cursor semantics or an independently checkable completeness protocol; otherwise give shifting or unverifiable sets an explicit incomplete outcome.

4. **BLOCKING — Failed intents can still disappear without external evidence.**  
   **L94, L116, L179–190.** The process may fetch after writing a local intent but before publishing it. A crash and deletion of that unlogged tail leaves nothing for the Rekor scan to discover. **Fix:** externally commit the intent before the request, or externally commit an attempt inventory that makes omitted intents detectable.

5. **BLOCKING — The cryptographic verification profile remains incomplete.**  
   **L181–190.** “Sign the record id” leaves raw digest versus textual encoding and prehash handling unclear. Inclusion verification also does not explicitly require matching the logged artifact digest, signature and public key to the record. **Fix:** freeze the exact bytes, encodings and request fields, and implement Rekor’s leaf-binding checks. [Rekor verifier requirements](https://raw.githubusercontent.com/sigstore/architecture-docs/main/rekor-v2-spec.md).

6. **BLOCKING — Deduplication depends on preserving the exact signed request.**  
   **L181–184, L196, L611.** Signing the same record again with ECDSA can produce a different signature and therefore a different entry. Identical-request deduplication also does not establish deduplication across independent shards. **Fix:** persist signature, canonical request and selected shard before submission; define timeout and rotation recovery without re-signing or blindly switching shards. [Rekor request canonicalization](https://raw.githubusercontent.com/sigstore/architecture-docs/main/rekor-v2-spec.md).

7. **BLOCKING — Trust-root acquisition and validation are unspecified.**  
   **L163, L181–190, L219.** `SigningConfig` identifies services, but the registry does not specify the authenticated `TrustedRoot` material needed for shard and timestamp verification, its bootstrap anchor, or rotation handling. **Fix:** pin the initial trust anchor and specify authenticated TUF updates, retained verification material and failure outcomes. [Sigstore client guidance](https://github.com/sigstore/rekor-tiles/blob/main/CLIENTS.md).

8. **BLOCKING — Scan coverage is derived partly from the records it is meant to audit.**  
   **L191–197.** “Shards the trial used” comes from retained configuration history, and the range begins at the first trial entry without explaining how that entry is found independently. Removing an entire shard’s local history or an earlier branch can exclude it from inspection. **Fix:** independently enumerate relevant shards and establish authenticated lower bounds, scanning from shard start where necessary.

9. **WARNING — A stale checkpoint need not fail consistency.**  
   **L193–194, L615.** A stale checkpoint can be a valid prefix consistent with every retained checkpoint. The drill’s expected consistency failure is therefore incorrect, particularly after local tail deletion. **Fix:** test freshness separately through independent current/final checkpoint retrieval and explicit size/finalization checks.

10. **BLOCKING — The verdict scan has no stable closing boundary.**  
    **L191–200, L219, L618–619.** Captures may continue while the potentially seven-day scan runs. New entries can appear after the chosen checkpoint or after the local chain snapshot, making equality dependent on timing. **Fix:** define capture quiescence, a final externally committed chain head, and a fixed audit boundary before verification starts.

11. **BLOCKING — Destructive drills invalidate the dry-run key used for lookback.**  
    **L276–279, L609–617.** Duplicate identities, branches and late publication are deliberately created under the dry-run key, yet lookback requires every dry-run record to pass witness checks. Those failures cannot be erased from an append-only log. **Fix:** use separate scratch keys/trials for destructive drills and exclude them explicitly from reusable capture history.

12. **BLOCKING — ZIP disposal prevents verification of member derivation.**  
    **L335–338.** Witnessing extracted members proves their commitment, not that they were correctly extracted from the committed ZIP. After deletion, the ZIP hash alone cannot establish member completeness or derivation. The disposal record type is also absent. **Fix:** retain source ZIPs or specify an auditable derivation/retention scheme with a complete extraction manifest and explicit disposal records.

13. **BLOCKING — Verdict integrity checks omit an explicit artifact-byte check.**  
    **L130–131, L198–200, L545–555.** Comparing records with indexes does not detect a changed component file when both still contain its old hash. **Fix:** require rehashing every consumed retained component, checking size and provenance, and refusing missing or mismatched bytes before parsing.

14. **BLOCKING — The schema fingerprint cannot be implemented as stated.**  
    **L92, L549–560.** PostgreSQL’s `information_schema` does not expose the complete partial-index definitions and predicates that the fingerprint is required to cover. **Fix:** specify the relevant PostgreSQL catalog queries and a canonical representation covering indexes, predicates and constraints.

15. **WARNING — Preflight is per tick, although the parent requires per-attempt checks.**  
    **L87–96, L111–116.** A tick can start several attempts over eight minutes using an earlier ledger/schema check. A terminal ledger change during that interval would not stop subsequent acquisitions as required by parent L458–461. **Fix:** perform the required checks before each attempt, or declare and justify a bounded tick-level exception.

16. **BLOCKING — Bootstrap windows still lack final deadlines.**  
    **L221, L267–279.** “Dry-run days, leftover capacity, until complete” supplies no fixed acquisition end or unambiguous witness deadline. There is also no complete lifecycle for historical identity work newly needed after declaration. **Fix:** assign bootstrap identities, finite deadlines, retry rules and deterministic forward missing-history outcomes.

17. **WARNING — Capacity acceptance does not cover the specified workload’s peaks.**  
    **L98–103, L596–597.** The estimate omits or underdefines per-ticker close validation, cover/certification filings, both active/inactive reference pagination, weekly action refreshes and large batches of member/witness records. One normal day plus 30% growth does not test month-end, early-close and publication bursts. **Fix:** gate an enumerated workload under defined contention and worst relevant calendar combinations.

18. **BLOCKING — F1 and F2 require SEC documents with no acquisition path.**  
    **L143–163, L293–297, L312–318.** The registry captures headers, Form 25 submissions and bulk members, but does not specify acquisition of the security-cover contexts or CERT*/8-A class evidence used by these rules. **Fix:** register the required filing artifacts, parsing inputs, identities, cutoffs, windows and missing-evidence outcomes.

19. **BLOCKING — Latest-accession selection can silently choose an older filing.**  
    **L226–230, L323–324, L341–342.** A lagging submissions member plus incomplete daily indexes is treated as an accession inventory. The freshness report only finds omissions already visible in bound indexes. **Fix:** establish completeness through the evidence cutoff before selecting the latest accession; otherwise retain `sic_unloaded`/pending rather than selecting an older known filing.

20. **WARNING — F3 does not fully supply the parent’s documentation and comparison scope.**  
    **L327–331.** Naming the EDGAR PDS specification does not cite the field’s meaning and availability. Restricting comparison to four latest SUB quarters also narrows the parent’s “quarters both cover” requirement without an amendment. **Fix:** provide the precise documentation locator and either perform the required overlap comparison or declare the restriction.

21. **BLOCKING — Form 25 evidence inventory and security cohort are incomplete.**  
    **L217–218, L265, L465–473.** Daily-index collection does not bootstrap pre-start filings or clearly backfill newly required CIKs. “Every filing for the CIK” also does not define the security-class cohort; the cited code explicitly identifies issuer-filed paragraph-(c) exclusions. **Fix:** specify historical discovery, per-security cohort construction and completeness, preserving the cited exclusion/identity rules.

22. **BLOCKING — Cover-title joins still lack ambiguous-context handling.**  
    **L293–305.** Matching a trading symbol does not determine which title wins when a filing has multiple classes, contexts or conflicting matching titles. Treating absence as a pass does not resolve ambiguous evidence. **Fix:** freeze security/context joins and explicit multiple-match, conflict and missing-title outcomes.

23. **WARNING — Primary-listing and exchange scope are inconsistent.**  
    **L260–262, L288–301, L584–586.** Acquisition includes exchange 20, but the plumbing population omits it. F1 also lacks an operative primary-listing predicate despite the parent requiring one. **Fix:** freeze the complete exchange population and primary-listing rule, and use the same denominator throughout capture and validation.

24. **BLOCKING — Listing-history exclusions are not actually collected for initial securities.**  
    **L98–101, L272–274, L309–315.** `massive_events` runs only after an observed ticker change. A security with a pre-capture change can therefore pass because no history was queried. Basic’s two-year event history also cannot establish 36-month continuity. **Fix:** query initial history and explicitly handle unavailable older continuity. [Ticker Events documentation](https://massive.com/docs/rest/stocks/corporate-actions/ticker-events).

25. **WARNING — Certification acceptance is assigned unsupported security-history meaning.**  
    **L312–319.** A later CERT*/8-A acceptance for any common class of the CIK is treated as a relisting or new class relevant to this FIGI. That does not establish an effective listing event for the same security. **Fix:** require a security-level class/event match and documented date semantics, or label this explicitly as an exclusion heuristic with measured false exclusions.

26. **WARNING — ME reconciliation remains a selected-case test.**  
    **L353–356.** It excludes securities without a supplied event, without a later cover count, and with discrepancies below 20%. Missing smaller distributions or offsetting errors can pass. **Fix:** reconcile every required basis/event interval; classify uncovered cases and use the cover-count/jump tests as supplementary evidence.

27. **BLOCKING — Historical action revisions can enter after the formation’s permitted cutoff.**  
    **L347–349, L382–383, L414–415.** The entire action inventory is selected at the post-close decision time. That permits newly observed revisions affecting earlier MAX sessions or historical share adjustments after s(M)’s close, although parent invariant 1 limits the post-close exception to the decision-session price package. **Fix:** separate pre-close historical action selections from decision-session adjustment inputs, or amend the invariant explicitly.

28. **BLOCKING — Missing action captures can be mistaken for no action.**  
    **L379–410, L436–439.** A cumulative inventory can still calculate a return when a relevant action acquisition failed; no rule proves that absence of an event represents complete evidence rather than missing collection. **Fix:** bind an interval coverage/completeness manifest and make uncovered intervals incomplete inputs.

29. **BLOCKING — Disappearance is treated as cancellation without a source contract.**  
    **L379–383.** A record may disappear because its date moved outside the query range, pagination changed, or the response was incomplete. Reappearance of identical content is also not given a clear rule for clearing withdrawal. **Fix:** define explicit revision/tombstone/reinstatement semantics and refuse unresolved disappearance; do not equate query absence with economic cancellation.

30. **BLOCKING — The action-conflict key rejects legitimate combinations and misses duplicates.**  
    **L390–391, L406–407.** Distinct regular and special dividends on the same date can have different amounts without conflicting. Conversely, duplicate records with equal amounts pass and are summed twice. **Fix:** use documented event identity and distinguish independent distributions, revisions and unresolved duplicate records; tuple equality alone is insufficient.

31. **BLOCKING — Malformed action rows cannot always be scoped as promised.**  
    **L384–389.** A row missing its ticker or date cannot be assigned only to its `(ticker,date)`; a missing ID cannot enter the revision inventory. It may consequently disappear from every affected return. **Fix:** define conservative response/range-level outcomes when identity is insufficient, and distinguish them from locally attributable row defects.

32. **BLOCKING — Pre-event ticker mapping is an unsupported universal rule.**  
    **L273, L387–389.** An action effective on a rename date may carry the new ticker, which did not exist in the preceding session. Future announced events also cannot yet be mapped using their preceding-session snapshot. **Fix:** define deferred mapping and an effective-date identity rule covering renames and reused symbols, with explicit unresolved outcomes.

33. **WARNING — The revision refresh horizon is shorter than the share-basis horizon.**  
    **L215, L347–349.** Weekly refresh reaches 400 days, while share bases can be 15 months old. Stored events in the uncovered tail never receive later corrections through this refresh. **Fix:** cover the maximum basis interval with margin, or explicitly declare the older revisions frozen and measure that restriction.

34. **BLOCKING — The readiness heuristic does not prove completeness.**  
    **L362–365.** A response missing nearly 10% of normal rows can pass, including omissions concentrated in the research population. SPY presence does not prove valid closes, and the rule does not require `resultsCount` to equal the actual unique-row count. **Fix:** label the count test a heuristic and define independently checkable required coverage and unresolved-omission outcomes; remove “a partial response … never binds.”

35. **BLOCKING — The first plumbing test’s readiness baseline is circular.**  
    **L362–364, L579–582.** The first 20 sessions use the plumbing-test median, while the plumbing test measures first readiness under that same validator. The median is unavailable when those first-binding decisions occur. **Fix:** establish a prior frozen calibration baseline or specify a separate startup validator before prospective timing begins.

36. **WARNING — One malformed timestamp can discard unrelated valid rows.**  
    **L362–367.** Requiring every row’s `t` to be on d makes one bad row a whole-response transport defect, unlike the scoped duplicate-ticker treatment. **Fix:** distinguish genuinely wrong response dates from isolated row timestamp defects and preserve unaffected observations consistently.

37. **BLOCKING — The per-ticker close reference is not sufficiently specified.**  
    **L370–373.** The comparator request does not specify `adjusted=false`, although the endpoint defaults to split adjustment. Its bytes, capture timing and missing-response handling are also absent from the registry. **Fix:** freeze the query basis and validation acquisition protocol before comparison. [Daily Ticker Summary documentation](https://massive.com/docs/rest/stocks/aggregates/daily-ticker-summary).

38. **WARNING — Sample equality cannot establish pricing-session semantics.**  
    **L368–373, L568.** Fifteen pairs per session from the same vendor can agree despite different session rules; inequality can also result from revisions or defects rather than session semantics. **Fix:** keep X2 as an explicit undocumented-session substitution and describe the test solely as sampled compatibility evidence, with adjudicated disagreement categories.

39. **BLOCKING — Missing daily reference data still has no complete outcome.**  
    **L360–361, L453–457, L501–505.** F13 addresses a row losing FIGI, but not a missing/defective reference snapshot, contradictory mappings, or multiple tickers for a FIGI. Those failures affect prices, actions and cessation. Retaining identity from ticker+CIK also needs an explicit continuity policy. **Fix:** define a bound per-session mapping state and its effects on every dependent input.

40. **WARNING — F7’s “independent enumeration” does not enumerate the required event population.**  
    **L392–401.** Price-jump and changed-export detectors miss smaller or already-adjusted events. Dividends declared in a fiscal quarter are not necessarily dividends with ex-dates in that quarter, and per-share accounting facts need class and split-basis reconciliation. **Fix:** build a frozen independent event inventory with explicit coverage; classify these comparisons as candidate detectors.

41. **WARNING — The documented adjustment alternatives are omitted from the source-rule assessment.**  
    **L55–58, L405–410.** The design derives returns and imposes a blanket split/dividend refusal without assessing Massive’s documented `split_adjusted_cash_amount` and dividend `historical_adjustment_factor`. This does not prove the formula wrong, but leaves the claimed source justification incomplete. **Fix:** document why those fields do or do not support the adopted convention, with refusal retained where equivalence is unestablished. [Dividend field semantics](https://massive.com/docs/rest/stocks/corporate-actions/dividends).

42. **WARNING — X3’s changed behavior and historical comparison remain incomplete.**  
    **L416–427.** The cited old screen exempts dividend as well as split stamps. The new no-split jump screen can flag a correctly stamped large dividend. Its invalid-action branches also require evidence not necessarily present in the proposed Intrader proxy. **Fix:** enumerate these changed classes and measure each reproducible branch; label the others unmeasured rather than calling the comparison the complete treatment.

43. **BLOCKING — `month_inventory` cannot always be formed on a missed formation.**  
    **L431–433, L527–531.** A formation missed because population/ME inputs are unavailable may have no defined top 1,000, yet the next inventory depends on that formation and its manifest. **Fix:** bind the previous cumulative inventory even when the current universe is unavailable, with a precise rule for adding only determinately captured constituents.

44. **BLOCKING — Monthly completion alternates between inventory-wide and holding-only requirements.**  
    **L170–172, L250–251, L439–441.** `month_final` depends on every inventory security, while permanent incompleteness refuses only if the security is actually held. An irrelevant historical constituent can therefore block finality or trigger the broader missing-selection rule. **Fix:** distinguish complete input/status records, including explicit unavailable values, from verdict-required holdings and define finality consistently.

45. **BLOCKING — A later terminal event overrides an earlier coverage exit.**  
    **L458–476.** A security can gap, resume, then cease later in the month. Terminal precedence chooses the later cessation bar; the invalid interval then becomes incomplete instead of realizing the earlier coverage exit without resumption credit. **Fix:** order realized events chronologically and apply the first applicable exit, with explicit terminal/gap precedence.

46. **BLOCKING — Cessation timing still substitutes observed coverage for the documented date.**  
    **L455–457.** If `delisted_utc` says the last traded date is d but the captured last valid close is earlier, choosing that earlier close as the cessation session conflates missing coverage with cessation and can move the event into another month. **Fix:** preserve the source event date separately from the last usable valuation bar and apply the gap rule when coverage ends first. [All Tickers field semantics](https://massive.com/docs/rest/stocks/tickers/all-tickers).

47. **BLOCKING — Form 25 identity uses capture start instead of series start.**  
    **L465–469.** The cited matcher compares the price series’ `first_bar` with filing dates. V4 substitutes the FIGI’s first bound session, so a valid pre-capture filing becomes `identity_unverified` solely because collection started later. **Fix:** supply verified security-series history or declare a different identity rule; do not call this the unchanged matcher. [Cited matcher](/Users/lukebradford/Dev/.ebull-autonomy/app/services/research_corpus_ingest.py:1581).

48. **BLOCKING — Form 25 evidence is not explicitly associated with the terminating event.**  
    **L455–473.** Even after class filtering, a filing for an earlier listing interval can classify a later cessation unless its security interval/event relationship is established. The source matcher’s `no_suspension` result also must retain linkage without inventing a suspension date. **Fix:** define interval-level matching and explicit outcomes for dated, undated, conflicting and unrelated filings.

49. **BLOCKING — Monthly and partial return validation is not independent.**  
    **L447–449.** Recomputing by hand from the same bound bytes can detect implementation mistakes but cannot validate missing events, wrong identities or an incorrect source convention. **Fix:** freeze an independent reference construction for complete monthly and partial returns, retaining the hand calculation as an additional implementation check.

50. **WARNING — Daily validation can pass on a selectively covered denominator.**  
    **L443–446.** Missing observations are counted but have no acceptance threshold or adjudication requirement. The test can pass on easy overlapping rows while difficult securities have no independent comparison. **Fix:** freeze the required denominator, identity/basis conversion and uncovered-case pass/fail treatment.

51. **WARNING — Termination fixtures and validation strata are not fully executable gates.**  
    **L447–448, L477–480, L606–608.** Ten terminal/coverage security-months are required for one test, but the event gate guarantees only one termination. Fixtures name branches without freezing expected status, `end_bar`, partial return, class and final code for the remaining boundary cases. **Fix:** specify the full fixture matrix and an explicit extension/failure rule when a validation stratum is unavailable.

52. **BLOCKING — Same-tick N-PORT download does not freeze the discovered version.**  
    **L220, L231–233, L484–487.** A mutable ZIP can change between listing capture and download. The selected bytes then need not be the version the poll supposedly discovered. **Fix:** use immutable version locators with expected hashes, or define version discovery by the captured ZIP bytes themselves.

53. **WARNING — N-PORT revision detection and ordering are incomplete.**  
    **L220, L231–233, L484–487.** A content revision with unchanged listed size/date is invisible. Outstanding downloads also leave “first accepted by poll order” ambiguous if an earlier poll’s download completes after a later one. **Fix:** define content-change detection and a deterministic acceptance order that cannot retroactively reorder an existing selection.

54. **BLOCKING — F14’s fallback can select a future month.**  
    **L514–516.** If m is absent but later rows exist, “the latest month’s row” can be later than m; the “at most 18 months older” condition does not exclude that. **Fix:** select the greatest row date strictly before m and require `0 < age ≤ 18 months`, otherwise refuse.

55. **WARNING — Cutoff validation and scenario coverage are incomplete.**  
    **L514–523.** Per-row validation does not establish unique month keys. The historical scenarios stop at 12 months although the accepted fallback reaches 18. **Fix:** reject duplicate/conflicting month rows, freeze month/date validation, and include the maximum permitted lag in the scenario analysis.

56. **BLOCKING — Premise 5(e) still has no gate for several essential dependencies.**  
    **L590–594, L641–642.** The test can pass with unusable required fields, widespread non-USD structural failures, or unsuitable event timing. “Yes or no” future coverage also changes whether a weekly schedule suffices without specifying either resulting schedule. **Fix:** define explicit acceptable exposure, completeness and timing assertions, deterministic schedule branches and failed-gate consequences.

57. **WARNING — Premise 5(c)/(d) does not establish the claimed usable population.**  
    **L584–589.** “Fully usable” omits action, listing-history and SEC/share-basis usability. Delisted testing starts from Massive’s own returned set, excluding names the reference source omits, and allows 1% misses without an acceptance rule for their causes. **Fix:** freeze independent denominators and required-input usability, then adjudicate and quantify every resulting population restriction.

58. **NIT — MAX boundary notation can change the published screen.**  
    **L413, L420–421.** `(−0.9, 3.0)` suggests an open interval, whereas the cited rule screens strictly outside the closed interval. “Contains a pair” also does not explicitly preserve the later-bar-in-window rule. **Fix:** state exact inequalities and membership by the pair’s later bar.

59. **NIT — The probe’s stated purpose is stale.**  
    **L40–41, L628, L694; probe L1–9, L86–123.** The probe refers to a nonexistent “Measured premises” section and still measures the superseded PWB/eToro route. It does not measure premise 5. **Fix:** update its description and distinguish legacy informational outputs from the Massive plumbing test.

**Parent-contract cross-check**

| Requirement | Assessment against v4 |
|---|---|
| Sealing invariant 1 — cutoffs | **Unmet:** historical actions can enter post-close; several decision/witness deadlines are undefined. |
| Invariant 2 — immutability | **Unmet:** pagination recovery conflicts with binding; action revision/withdrawal treatment is incomplete. |
| Invariant 3 — unique identities | **Partial:** schema identities improve; signature retries, bootstrap definitions and destructive drills remain problematic. |
| Invariant 4 — executable windows | **Unmet:** bootstrap and selection deadlines are incomplete; some required acquisitions are absent. |
| Invariant 5 — external witness | **Unmet:** intent commitment, verification details, scan boundaries, freshness and audit closure need repair. |
| Invariant 6 — splicing | **Unmet:** historical/action identity, missing-reference handling and independent return reconciliation remain incomplete. |
| Invariant 7 — no interim result | **Adequately specified:** portfolio formation/valuation is prohibited; instrument checks and historical scenarios are distinguishable. |
| Invariant 8 — access rows | **Adequately specified at design level:** acquisition/evaluation timing and abandoned-attempt classification are stated. |
| Obligation 1 — security type | **Partial:** X1 exists; source acquisition, class/context ambiguity and primary-listing treatment remain incomplete. |
| Obligation 2 — listing age | **Partial:** X8 declares a proxy, but its exclusion evidence is incompletely acquired and interpreted. |
| Obligation 3 — SIC | **Partial:** latest-accession completeness, documentation and overlap scope remain unresolved. |
| Obligation 4 — accounting | **Partial:** source capture and cutoff are stated; derivation audit and accession completeness need repair. |
| Obligation 5 — shares/ME | **Partial:** formula and fixtures exist; cutoff treatment and population reconciliation remain incomplete. |
| Obligation 6 — raw close | **Partial:** split basis is documented and X2 declared; readiness and reconciliation claims exceed their tests. |
| Obligation 7 — split events | **Unmet:** interval completeness, identity, revision and malformed-row outcomes remain incomplete. |
| Obligation 8 — MAX | **Partial:** recovery cutoff and gap interval improve; action cutoff and replacement-screen measurement remain unresolved. |
| Obligation 9 — holding returns | **Unmet:** action completeness, monthly completion and independent validation remain incomplete. |
| Obligation 10 — terminations | **Unmet:** gap precedence, event dating and Form 25 identity/event association remain defective. |
| Obligation 11 — B1 | **Partial:** one-band pricing is fixed; N-PORT version discovery/ordering remains incomplete. |
| Obligation 12 — factors | **Rule preserved:** first accepted complete versions are specified; common sealing defects still apply. |
| Prospective dry run | **Partial:** three month-ends and hard capture gates remain; acceptance rules and drill isolation need repair. |
| Parity | **Included:** the parent’s table is required and remains reporting-only. |

**Overall assessment:** V4 is a meaningful improvement, but is not ready to close checkpoint 1. The missing API key is not the blocker. The design must first resolve its sealing contradictions, acquire all evidence its rules require, and strengthen the gates that currently permit incomplete or structurally unusable inputs to pass.

## Round 5 (on the route note, `docs/research/2026-10-10-3740-slice-f-forward-capture.md` lines 1-83 before revision)

**The route note is not ready to accept.** The measurements establish gaps in the current pipeline, not the claimed impossibility. The lower-bound implication is conditional for B1, fails for an incompletely credited control, and does not transfer to condition 3.

I reviewed the queries rather than rerunning the dev-DB measurements. No files changed.

References below: **R** = [route note](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/2026-10-10-3740-slice-f-forward-capture.md:9); **P** = [parent](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/2026-10-10-3740-step3-vw-book.md:18); **S** = [measurement script](/Users/lukebradford/Dev/.ebull-autonomy/scripts/probe_3740_slice_f_account_free.py:1); **D** = [dividend parser](/Users/lukebradford/Dev/.ebull-autonomy/app/services/dividend_calendar.py:1); **F** = [prior findings](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/3740-slice-f-ckpt1-findings.md:99); **E** = [eToro measurement](/Users/lukebradford/Dev/.ebull-autonomy/.claude/skills/data-sources/etoro-api.md:309).

1. **BLOCKING — Pipeline incompleteness does not establish source impossibility.**  
   **R 15–35, 49–52; S 35–81.** The SQL measures existing database contents. Missing events can arise from acquisition, filing selection, extraction, linking or source absence; the note acknowledges that ingestion completeness is unverified. Neither these counts nor failure of a price-only power test establishes “no account-free confirmation route.”  
   **Fix:** state “no compliant complete return source has yet been demonstrated,” separate those failure causes, and limit the stop conclusion to the particular frozen route tested.

2. **BLOCKING — The measured populations do not match the estimand.**  
   **R 22–23; S 39–49, 70–75; P 39–45.** These are stored instruments with qualifying XBRL facts—not all filers and not the monthly top-1,000 eligible common-stock population. There is no exchange, security-class, tradability, ME-ranking or required-held-security restriction. Broad-population gaps do not establish gaps in the required population; XBRL-selected denominators can also omit relevant dividend payers.  
   **Fix:** report the broad inventory separately, then measure coverage and unresolved events for a frozen eligible population, including carried holdings and control requirements.

3. **WARNING — “1,634 such rows” has the wrong implied subject.**  
   **R 16, 22; S 42–65.** The 443/124 counts join events to the 1,182 payers and require record date plus amount. The 1,634/29/1,371 query covers *all* event rows passing its date predicate, without that join or amount requirement. “Such” makes these appear to share a denominator.  
   **Fix:** explicitly name the second population, or apply the same payer join and validity filters. Print both denominators if both are useful.

4. **WARNING — The time windows and event counts do not measure annual ledger completeness.**  
   **R 22–23; S 20–22, 41–45, 49, 61, 71–74.** `COALESCE(ex_date, record_date, declaration_date)` selects different economic dates for different rows; it does not mean “announcements since.” There is no upper cutoff. “Last four fiscal quarters” is not implemented by a lower bound of July 2025. Three rows need not be three distinct distributions or a complete annual schedule. The XBRL `bool_or` flags can come from different periods, and cash paid need not correspond to the linked common share’s declared DPS.  
   **Fix:** freeze an as-of timestamp and explicit intervals; distinguish declaration, ex-date and fiscal-period populations; reconcile security class and distinct event identity. Label these as presence counts until independently checked.

5. **BLOCKING — A permitted eToro dividend source was missed.**  
   **R 25, 33–35.** eToro’s public [dividend calendar](https://www.etoro.com/investing/dividend-calendar/) exposes ex-dates, payment dates and periodic amounts without requiring a real position. This contradicts the source-wide “no dividend field outside … `totalFees`” claim. The page calls its information indicative, so its existence does **not** establish a complete ledger.  
   **Fix:** add it as an unqualified candidate and assess identity, amount basis, historical/forward coverage, revisions and capture availability before accepting or rejecting it.

6. **WARNING — The SEC assessment is confined to one deliberately limited parser.**  
   **R 22; D 3–16.** The parser reads Item 8.01 primary documents and targets ≥80% extraction on Dividend Aristocrats. That is not a completeness test for EDGAR. Dividend announcements can appear in other items and attached exhibits: this [SEC-filed Item 7.01 announcement](https://www.sec.gov/Archives/edgar/data/1287032/000128703224000153/psec-20240508.htm) includes common-dividend amounts and record/payment dates. The docstring’s “only” is therefore too broad.  
   **Fix:** distinguish the parser’s scope from EDGAR’s scope; audit relevant items, exhibits and periodic filings for the required securities. An unfiled press release remains unavailable through EDGAR, but that is a narrower claim.

7. **WARNING — Missing explicit ex-dates are not necessarily undatable events.**  
   **R 22, 33–35, 38.** The 29 explicit ex-dates understate potentially usable dated evidence. Documented rules relate ordinary ex-dates to record dates, with exceptions for large distributions, late information and other cases. This should be assessed through the applicable rules, not inferred solely from settlement arithmetic. [SEC explanation](https://www.investor.gov/introduction-investing/investing-basics/glossary/ex-dividend-dates-when-are-you-entitled-stock-and), [FINRA Rule 11140](https://www.finra.org/rules-guidance/rulebooks/finra-rules/11140).  
   **Fix:** classify explicit dates, dates derivable under a documented applicable rule, and unresolved exceptions. Pin historical rule versions for the 2014–2024 measurement; do not apply today’s rule throughout.

8. **WARNING — “FINRA … no dividend data” is overbroad.**  
   **R 26.** FINRA describes its Daily List as containing OTC corporate actions, including dividend ex-dates. That population does not supply the required exchange-listed large-cap ledger, and access conditions still need checking. [FINRA description](https://syndication.finra.org/content/corporate-actions-public-companies-what-you-should-know).  
   **Fix:** identify the particular public files assessed and reject them for demonstrated coverage/access limitations, rather than attributing no dividend data to FINRA generally.

9. **WARNING — The separate-ledger requirement and its citations are overstated.**  
   **R 24, 33–35; P 500–505; F 208, 575, 580, 713–714, 728–729.** Obligation 9 expressly includes captured PWB `adj_close` as a candidate; it does not require every return construction to invert prices into a separate cash ledger. Moreover, round-2 finding 29 concerns split-factor medians. Round-4 Task B finding 45 concerns terminal-event precedence; Task A finding 45 concerns dividend enumeration. The citations mix numbering schemes.  
   **Fix:** state separately the requirements for return construction and independent validation, and cite findings by round, task, number and title. Lack of an inversion contract rejects inversion, not automatically direct adjusted-return use.

10. **WARNING — The script does not reproduce the quoted PWB access evidence.**  
    **R 15, 24; S 103–112.** The README uses a multiline `extra_gated_prompt`; the script prints its key line but drops the continuation containing the subscription statement. Both HTTP requests use mutable current resources, and the script retains neither their revision nor bytes. The [current card](https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price/blob/main/README.md) supports the subscription concern, despite the reported `gated=False`, but the claimed dated measurement is not preserved by this script.  
    **Fix:** retain a pinned revision and response hashes, print the parsed complete field, and distinguish documented access policy, runtime gating and observed data lag.

11. **BLOCKING — Two lower bounds cannot establish the matched-control total-return comparison.**  
    **R 37–46; P 25.** If both book and controls omit dividends, beating the control median does not imply beating its total-return median. For example, book price return 6%, control price return 5%, book dividends 0%, control dividends 3%: the price comparison passes while the total-return comparison fails. Leaving controls on total return instead preserves the dividend-data requirement.  
    **Fix:** provide complete control total returns, a justified upper bound on them, or explicitly amend condition 4 to a price-return comparison without claiming the original implication. Apply the rule to every matched draw before calculating the median.

12. **BLOCKING — Return ordering does not preserve factor loadings or their significance.**  
    **R 38–47, 49–51; P 94–106, 140–167.** Even if each total return exceeds its price return, the omitted dividend component can have positive or negative partial factor loadings. Loadings, residual variance, standard errors and rejection probabilities are not ordered by the return lower bound. Price-only power also does not bound power for an eventual selectively credited-dividend series.  
    **Fix:** explicitly define condition 3 on the chosen observable return series and acknowledge the changed claim. Freeze that series before planning; do not treat price-only planning as validation of an unspecified partial-dividend variant.

13. **BLOCKING — The lower-bound proof does not yet cover the parent’s net wealth path.**  
    **R 38–45; P 378–398, 434–439, 500–507.** Adding nonnegative dividends establishes ordering for suitably matched gross returns. It does not by itself prove ordering after changed portfolio drift, rebalancing, transaction costs, cash treatment, terminal proceeds and coverage exits. “Parent construction and costs” does not specify which quantities stay identical or establish monotonicity when recomputed.  
    **Fix:** define both wealth recurrences and prove the ordering under the actual cost and trading rules. Preserve identical event/status conventions in each termination arm, charge entry/exit costs, and exclude double-counted terminal distributions. Scope the conclusion to the parent’s modeled terminal conventions.

14. **BLOCKING — “Receipt is certain” is not an executable dividend-credit rule.**  
    **R 38–41; P 500–505.** Certainty that a dividend exists or will be paid does not establish entitlement for the paper holding, correct security class, split basis, gross amount, economic date or absence of duplication. Crediting actual broker receipts can also substitute payment-date, post-withholding accounting for the parent’s gross ex-date convention.  
    **Fix:** either freeze zero credits throughout or define a deterministic evidence-and-entitlement rule with gross amounts, ex-date attribution, revision handling and unresolved-event treatment. Apply the same frozen policy in planning and forward evaluation.

15. **WARNING — “The cost is power, roughly … dividend yield” conflates different effects.**  
    **R 39–40; P 104–106.** Omitted yield approximates a gross return drag under simplifying assumptions. It is neither the loss of condition-3 power nor a quantified change in condition-4 beat probability. Compounding, costs and different control yields matter.  
    **Fix:** call it an approximate return drag; report loading power and beat survival as separate measured quantities.

16. **WARNING — The demonstrated comparator would be IVV, not literally SPY.**  
    **R 37–46; P 124–126, 387–391, 508–509.** The parent explicitly proxies SPY using IVV N-PORT returns. A lower-bound beat against that series proves a beat against the adopted proxy, subject to its costs; it does not establish a literal SPY beat.  
    **Fix:** consistently label B1 “IVV total return, the parent’s SPY proxy,” retain its specified charges, and disclose the historical source transition.

17. **BLOCKING — Zero holding dividends do not eliminate all dividend-dependent inputs.**  
    **R 40–41, 50–51; P 407–409, 498–505.** The parent also calls for daily total-return adjustment inputs for the last MAX observation. Changing only condition 4 leaves this requirement, obligation 9 and the return series used by condition 3 unresolved. Changing MAX to price returns can change eligibility and therefore the book itself.  
    **Fix:** enumerate the affected parent clauses before measurement. Freeze whether MAX remains unchanged or changes, and evaluate the exact proposed construction; a holding-return-only sensitivity does not validate a construction with different selection.

18. **BLOCKING — Item 1 conflicts with the planning slice’s explicit information restriction.**  
    **R 43–51; P 140–147.** The decision asks to print and use net growth and benchmark/control comparisons. Premise 5 expressly prohibits printing or using those quantities. The proposed parent amendment is deferred until after measurement and mentions only condition 4.  
    **Fix:** adopt and checkpoint the historical diagnostic amendment before running it. Specify who sees the results, what decisions they may affect, and how the family-selection rule remains protected from performance-based reselection.

19. **BLOCKING — The power continuation criterion is not fully precommitted.**  
    **R 47–52; P 143–175.** “Keeps … at the parent’s floor” leaves unclear whether the original selected family set must pass, the ordered search is rerun, or bars from a different return series are reused. The floor is specifically the **two-arm conjunction bound** `p₁ + p₂ − 1 ≥ 0.80`, not each arm individually reaching 0.80.  
    **Fix:** freeze the family-search policy and rerun the parent’s exact null/power calibration on the selected return definition: 119 months, 24-month replications, block length 3, 10,000 replications, prescribed seeds, cross-arm bars and refusal handling. Print arm powers and their conjunction bound; retain the parent’s diagnostic-only sensitivity figures.

20. **BLOCKING — Item 1’s report and its role in stopping are undefined.**  
    **R 45–52.** “How often” has no observational unit: one historical path, rolling windows, arms or random draws. No denominator, tie treatment, undefined-result treatment or paired control construction is specified. Merely “reported” allows continuation even if no original beat survives—which can be intentional, but is not a beat-survival gate.  
    **Fix:** freeze the reporting grid and paired comparisons. Report net G, both benchmark margins, valid counts, original-beat counts and surviving-beat counts per arm, including the zero-original-beats case. If using rolling 24-month windows, label their overlap. Explicitly make item 1 diagnostic-only, or precommit a numerical criterion and complete failure/refusal branches before looking.

21. **BLOCKING — Statistical success is insufficient to authorize the proposed source route.**  
    **R 20–21, 49–52; P 470–471, 490–505.** The continuation branch depends on power and reporting even though raw-close documentation remains open and split coverage is unestablished. Historical return sensitivity cannot resolve either source requirement. Conversely, failure of this sensitivity cannot rule out every compliant alternative.  
    **Fix:** distinguish “statistically worth designing” from “source-feasible” and “accepted for accrual.” Require obligations 6–9 to close, or receive explicit amendments, before acceptance; narrow the terminal conclusion to the route actually tested.

22. **BLOCKING — The raw-close route does not meet obligation 6 as written.**  
    **R 20, 35, 50; P 490–495.** The parent requires documentation and a capture-time contract establishing the own-session bar’s unadjusted basis. The route concedes that contract is absent. The current [eToro closing-price documentation](https://api-portal.etoro.com/api-reference/market-data/get-historical-closing-prices) names an official closing-price field but does not establish the required adjustment/finality semantics.  
    **Fix:** keep obligation 6 explicitly unresolved until that evidence exists, or propose a reviewed source/estimand exception. Preserve first-capture, entrant and whole-population reconciliation requirements.

23. **WARNING — Four smooth split boundaries do not measure the claimed rescaling mechanism.**  
    **R 21; S 23–28, 84–101.** The query reads one current database vintage around four known splits. It neither compares an original nominal capture with a later fetch nor measures adjustment timing, revision behavior or exact factor recovery. Smooth boundaries support compatibility with adjustment, not the claimed forward mechanism. Symbol-based joins followed by `dict` can also silently collapse multiple matching rows.  
    **Fix:** label these observations narrowly; use unique instrument identities and reject duplicates. Validate retained before/after source vintages against independently dated events, without promoting sampled agreement into a source contract.

24. **BLOCKING — Forward rescale detection cannot supply obligation 7’s historical coverage by itself.**  
    **R 21, 35, 50–51; P 402–416, 488–497.** Own nominal captures begin at capture inception; candidates and new entrants can require split history back to a share-count basis up to 15 months earlier. “SEC confirmation per event” also confirms detected events without independently establishing that none were missed. Later confirmation may miss formation cutoffs.  
    **Fix:** define an independent dated inventory over each required basis interval, exact factor products, startup/entrant coverage and unresolved-event outcomes. Specify confirmation availability before dependent cutoffs; use rescale detection as corroboration unless completeness is established.

25. **WARNING — The seven-instrument price comparison cannot establish universal safety or a return bound.**  
    **R 20, 25, 38–40; E 311–332.** Negative average bid/market level bias and high correlations in seven instruments do not prove every eToro close is below the market close, that the bias is constant, or that eToro price returns bound market total returns. Varying level discounts can reverse return ordering. They also do not establish equivalence between candles and the proposed closing-price field.  
    **Fix:** separate “no dividend adjustment” from price-basis equivalence. Validate the exact endpoint and required population, preserve an explicit basis exception where necessary, and reconcile broker marks with the parent’s cost convention.

## Round 6 (on the revised route note)

**The revised note is not ready to accept.** The proposed measurement could become a useful feasibility screen, but its reference inventory, comparison windows, coverage definition, and decision branches are not sufficiently specified. The stop rule is neither complete nor fully precommitted.

I inspected the script without running it and changed no files. An initial contextual search returned some adjacent lines outside the requested ranges; the findings below rely on the requested material and external documentation.

References: **R** = [revised route note](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/2026-10-10-3740-slice-f-forward-capture.md:10); **P** = [parent](/Users/lukebradford/Dev/.ebull-autonomy/docs/research/2026-10-10-3740-step3-vw-book.md:39); **S** = [probe](/Users/lukebradford/Dev/.ebull-autonomy/scripts/probe_3740_slice_f_account_free.py:1).

**Task A — disposition of round-5 findings**

“APPLIED” concerns the requested correction, not acceptance of the underlying source.

1. **PARTIAL** — R17–19 and R35–37 narrow the evidence correctly; R60–62 restores the unsupported source-impossibility conclusion.
2. **PARTIAL** — Broad counts are labelled; R49–52 still lacks the exact parent population, carried/control requirements, and a frozen membership manifest.
3. **APPLIED** — R25 and S71–72 explicitly distinguish the all-event denominator from the payer join.
4. **PARTIAL** — R25–26 now describe presence counts and predicates; snapshot, distinct-event, and script-label problems remain.
5. **PARTIAL** — The eToro calendar is included and measured, but its proposed assessment does not yet resolve identity, amount basis, or timely coverage.
6. **PARTIAL** — The parser limitation is acknowledged; the next EDGAR test still restricts discovery to selected 8-K items and exhibits.
7. **PARTIAL** — Applicable historical rules and exceptions are acknowledged, but no executable rule mapping or exception-evidence policy is frozen.
8. **APPLIED** — R29 correctly identifies FINRA Daily List’s OTC scope instead of claiming FINRA has no dividends.
9. **APPLIED** — R27 rejects PWB on the access rule; the separate-ledger/inversion objection and mixed citations are removed.
10. **PARTIAL** — A revision is named and a README hash is printed; mutable retrieval and omission of the multiline access statement remain.
11. **LAPSED** — The price-return matched-control route is withdrawn.
12. **LAPSED** — The proposed transfer from return ordering to loading power is withdrawn.
13. **LAPSED** — The unproved net-wealth lower-bound route is withdrawn.
14. **LAPSED** — The “certain receipt” credit policy is withdrawn.
15. **LAPSED** — The withdrawn route no longer equates omitted yield with lost power.
16. **LAPSED** — The withdrawn lower-bound claim no longer purports to establish a literal SPY beat.
17. **LAPSED** — The proposal to avoid dividend requirements through zero holding credits is withdrawn; R43 acknowledges MAX remains.
18. **LAPSED** — The prohibited historical performance measurement is withdrawn.
19. **LAPSED** — The old power continuation gate is withdrawn; the replacement coverage gate has separate defects below.
20. **LAPSED** — The old beat-survival reporting gate is withdrawn; replacement reporting and decision branches remain incomplete.
21. **PARTIAL** — R63 preserves obligations 6 and 7, but failure remains overgeneralised and the success branch is unspecified.
22. **APPLIED** — R23 explicitly leaves the documentation and capture contract unresolved.
23. **PARTIAL** — Timing claims are narrowed and multiple instrument IDs are rejected; categorical adjustment wording and duplicate-row handling remain.
24. **PARTIAL** — Historical insufficiency is acknowledged; required basis intervals, entrant coverage, confirmation deadlines, and refusal outcomes remain unspecified.
25. **LAPSED** — The seven-instrument comparison is no longer used to establish a universal return bound.

**Task B — remaining defects**

1. **BLOCKING — An adjusted-price series is not yet an event inventory.**  
   **R49–52.** Availability of total returns does not establish that the referenced data enumerate individual distributions with exact ex-dates and cash amounts. An adjusted series alone cannot supply that ledger; reverse engineering requires additional data and a documented adjustment convention, including split and other-action treatment.  
   **Fix:** name an explicit event dataset and schema, or document and validate the provider-supported extraction method. If neither exists, mark the proposed reference unavailable.

2. **BLOCKING — Reference independence, access, and coverage are unestablished.**  
   **R12–14, R27, R51–52.** A row title in another document does not identify the provider, upstream lineage, permitted access, security coverage, or available dates. Recording a vintage alone does not establish independence from either candidate. The supplied text also does not establish whether this reference differs from the excluded PWB source.  
   **Fix:** freeze the exact artifact, provenance, access basis, fields, coverage, and known omissions. Do not assume the unread cross-reference resolves these requirements.

3. **BLOCKING — “Frozen now” does not describe a frozen measurement.**  
   **R49–60.** The formation date, 12-month endpoints, capture start/end, reference revision, and membership list are absent. “Latest formation” moves when the panel changes.  
   **Fix:** record concrete dates and time zones, immutable population/reference manifests, and the extraction and scoring versions before candidate comparisons.

4. **BLOCKING — The proposed population does not fully specify the parent’s population.**  
   **R49–50; P39–45.** The parent requires eToro listing, decision-session tradability for entry, and monthly ME selection. The abbreviated definition omits the first two explicitly and substitutes one historical formation for a changing population. Required carried securities and controls are not addressed.  
   **Fix:** freeze the full eligibility construction and required security set. A single formation can be a pilot cohort, but label it accordingly and specify subsequent entrant, holding, and control validation.

5. **BLOCKING — The historical and prospective tests cannot currently share the stated denominator.**  
   **R49–61.** EDGAR is assessed against a frozen 12-month inventory; eToro is captured for the next eight weeks. Those are different event sets. If the reference is historical, new captures cannot establish historical calendar presence. If it is prospective, the complete inventory and its final vintage cannot already be frozen.  
   **Fix:** separate historical EDGAR diagnostics from a common prospective comparison. Freeze prospective membership, dates, reference-construction rules, and adjudication deadlines now; freeze the resulting event inventory later under those rules.

6. **BLOCKING — The EDGAR test introduces avoidable false negatives.**  
   **R53–55.** It limits discovery to three 8-K items and EX-99 exhibits, omitting the periodic-filing assessment requested in round 5. Requiring a record date also excludes an announcement that supplies the required amount and an explicit authoritative ex-date directly.  
   **Fix:** enumerate relevant filing and attachment coverage, and score explicit-ex-date evidence separately from rule-derived dates. Otherwise describe failure as failure of this restricted extraction route.

7. **BLOCKING — Ex-date derivation still lacks executable documented rules.**  
   **R25, R54–55.** “Exchange/FINRA rule in force” is directionally correct, but does not determine jurisdiction, applicable version, business-day calendar, distribution classification, or exception handling. Under the documented rules, distribution size, payable date, timely notice, and exchange designation can matter; SEC acceptance before ex-date does not itself establish timely notice to the exchange.  
   **Fix:** pin a venue/version decision table and required evidence for each branch. Preserve explicit dates, derivable dates, and unresolved exceptions separately. Use the documented transition rules rather than settlement arithmetic: Nasdaq expressly specifies the May 2024 transition and treats distributions of at least 25% differently. [FINRA Rule 11140](https://www.finra.org/rules-guidance/rulebooks/finra-rules/11140), [Nasdaq Rule 11140](https://listingcenter.nasdaq.com/rulebook/nasdaq/rules/nasdaq-equity-11), [Nasdaq transition notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2024-29).

8. **BLOCKING — Availability is tested asymmetrically and against an unexplained deadline.**  
   **R54, R56–58; P498–505.** EDGAR must precede the ex-date, while a calendar event can apparently qualify whenever it “appeared.” Economic attribution at ex-date does not, by itself, prescribe that acquisition deadline. Conversely, eventual appearance may be too late for the required capture or MAX input.  
   **Fix:** cite the actual deadline for each dependent input and apply it consistently. Report publication, acceptance, first capture, usable-by-deadline status, and final corrected agreement separately.

9. **BLOCKING — Reference-guided retrieval does not establish an operational discovery route.**  
   **R53–55.** Searching EDGAR for already-known events measures whether evidence can be found with an oracle. It does not demonstrate that the proposed acquisition and extraction process independently discovers those events.  
   **Fix:** label the guided audit as source-content feasibility. Separately run a frozen discovery process without reference answers, then reconcile acquisition, extraction, linking, dating, and genuine source-absence failures.

10. **BLOCKING — “Exact ex-date and amount” lacks an event and amount contract.**  
    **R50–61; P500–503.** There is no canonical security/class mapping, event identifier, currency, gross/pre-withholding definition, per-share split basis, or treatment of ordinary versus special distributions. Separate same-day distributions may be merged or duplicated. Literal numeric equality is also undefined across differing precision.  
    **Fix:** freeze event keys, distribution scope, amount semantics, decimal normalization, and justified tolerances. Unknown currency or basis must remain unresolved, not become a match.

11. **BLOCKING — Recall alone can accept a wrong ledger.**  
    **R53–61.** A candidate containing every reference event plus invented or duplicate dividends can achieve 100% coverage. “Revisions and duplicates observed” imposes no acceptance condition. Nonpayer false positives are invisible to a denominator containing only genuine distributions.  
    **Fix:** reconcile both directions over the full security population: missed, extra, duplicate, conflicting, and unmatched events, with precommitted outcomes and reference-error adjudication.

12. **BLOCKING — The combined source is not an executable policy.**  
    **R58, R60–61.** “Both combined” could mean event union, field-level assembly, or choosing whichever value retrospectively matches the reference. Conflicts, cancellations, revisions, and source precedence are undefined. An event that was briefly correct and later wrong could still count as having appeared.  
    **Fix:** freeze source precedence, permitted field combinations, revision selection, conflict refusal, and deduplication. Score the actual combined policy at its required cutoff; report an oracle union only as a diagnostic.

13. **BLOCKING — The 99% threshold has no stated basis and permits economically unbounded omissions.**  
    **R60–61; P500–505.** No parent rule or error budget supports 99%. One percent of event counts can contain the largest distributions or be concentrated in important securities. Passing does not specify what happens to the remaining events.  
    **Fix:** justify the threshold as a feasibility screen or propose a reviewed tolerance amendment. Report per-security completeness and distribution magnitude alongside counts, and precommit treatment of every unresolved required event.

14. **BLOCKING — Sample agreement cannot establish future completeness.**  
    **R49–61; P504–505.** Neither a single historical cohort nor eight weeks covers all future entrants, seasonal schedules, special distributions, and revision regimes. The parent explicitly says dry-run agreement is compatibility evidence, not a population guarantee. Neither eight weeks nor twelve months has a stated adequacy rationale.  
    **Fix:** distinguish a time-budgeted pilot from validation sufficient to advance the design. State event and exception coverage requirements, justify duration, and retain ongoing reconciliation and refusal rules even after perfect sample agreement.

15. **BLOCKING — The stop rule lacks inconclusive and failure branches.**  
    **R60–63.** It does not handle an unavailable/incomplete reference, zero events, insufficient event classes, fetch outages, failed identity mapping, unfinished adjudication, or an immature reference at week eight. These are not all evidence of source failure.  
    **Fix:** freeze separate pass, route-fail, and inconclusive branches, including bounded extension rules, deadlines, exact integer scoring without percentage-rounding ambiguity, and treatment of unresolved denominator members.

16. **BLOCKING — Failure is again promoted into source impossibility.**  
    **R35–37 versus R60–62.** Failing two implementations on a cohort cannot establish that obligation 9 “has no account-free source.” It may establish inadequate extraction, timing, reference quality, or this particular combined route. The forced amendment-or-stop choice excludes remediation without explaining that policy choice.  
    **Fix:** conclude “this frozen route did not demonstrate the required coverage.” If the supervisor chooses to stop further source work, identify that as a resource decision, not a fact proved by the measurement.

17. **BLOCKING — The success branch does not state what success authorizes.**  
    **R60–67; P470–471, P498–505.** Obligations 6 and 7 are explicitly retained, but the note does not say whether passing permits another design review, satisfies the dividend component, or authorizes accrual. Event coverage alone does not validate MAX inputs or the separate daily-return, split, and termination tests.  
    **Fix:** make passing authorize a specified next design/checkpoint step only. Preserve the parent’s full closure and accepted-dry-run requirements, with no automatic source or accrual acceptance.

18. **WARNING — Daily capture is insufficiently specified to support the measurement.**  
    **R56–58.** Bytes and a daily hash do not define capture time, time zone, page variant, successful completeness, retry limits, missed days, or first/last observation. Start/end censoring and events appearing after the eight-week boundary remain untreated.  
    **Fix:** freeze the capture schedule and metadata, completeness checks, outage treatment, and a justified observation tail. Keep capture failures distinct from absent events.

19. **WARNING — The calendar parser can silently produce a plausible partial measurement.**  
    **R16, R28; S131–143.** It scans every HTML table by positional columns, silently drops rows failing minimal shape checks, sorts date strings without validation, and never parses amounts. One surviving row defeats its only completeness check.  
    **Fix:** target and validate the intended table and headers; validate dates and amounts; count and retain rejected rows; detect truncation/schema changes and fail the measurement when completeness is unknown.

20. **WARNING — “Suffix-free (US)” is an unsupported security classifier.**  
    **R28; S136, S142.** Absence of a dot in a symbol does not establish US listing, common-stock status, or parent eligibility. The public calendar includes suffix-free ADR symbols, illustrating that the heuristic does not identify the required security class.  
    **Fix:** report “suffix-free symbols” only, then join through authoritative instrument and listing/class identities. [eToro calendar](https://www.etoro.com/investing/dividend-calendar/).

21. **WARNING — One calendar snapshot is described as a general publication policy.**  
    **R28.** The observed date ranges support a statement about the captured rows. They do not establish that the page always lists only already-ex distributions awaiting payment, or establish its lead time and retention policy.  
    **Fix:** qualify the sentence as a snapshot observation; determine publication and removal behavior through retained captures or provider documentation. The page describes upcoming payouts and explicitly allows changes. [eToro calendar](https://www.etoro.com/investing/dividend-calendar/).

22. **WARNING — The split evidence still begins with a stronger claim than it supports.**  
    **R24; S92–110.** “Is split-adjusted across four published splits” is categorical, despite the later compatibility qualification. Smooth boundaries do not uniquely identify adjustment. The script rejects multiple instrument IDs but still silently collapses repeated instrument/date rows; expected nominal ratios also ignore ordinary price movement.  
    **Fix:** say “four boundaries are compatible with split adjustment.” Validate unique instrument/date observations and finite positive prices, document the split references, and retain approximate nominal ratios as illustrations only.

23. **WARNING — The database measurement is not reproducibly frozen.**  
    **R16–19; S40–113.** The probe queries live tables without explicitly establishing a shared repeatable snapshot or retaining the contributing rows. Its printed HTTP hashes do not bind the SQL evidence. An approximate run time does not reproduce later-changing counts.  
    **Fix:** use a read-only consistent snapshot and retain the result artifact, parameters, exact timestamp, and relevant row/event identities or a reproducible database snapshot.

24. **WARNING — PWB’s quoted evidence is still not reproduced by the probe.**  
    **R16–17, R27; S33–34, S115–125.** Metadata and README use mutable endpoints; the reported dataset SHA does not bind the subsequent README request. The printer still omits the multiline subscription text. It does not test the claimed successful unauthenticated data download, preserve response bytes, or independently verify the data maximum date.  
    **Fix:** fetch the README at the reported immutable revision, preserve evidence, parse the complete field, and distinguish card assertions from separately observed download/data coverage. The current card supports the subscription concern, but not this probe’s historical reproducibility claim. [Provider README](https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price/blob/main/README.md).

25. **WARNING — The checkpoint log overstates application.**  
    **R762–766.** It presents findings 1–10 as applied although the revised stop conclusion, population definition, EDGAR test, and PWB reproduction remain incomplete. Withdrawal also should not be confused with implementing the old route’s fixes.  
    **Fix:** record individual dispositions consistent with Task A, separating applied corrections, remaining work, and withdrawn-route findings.

26. **NIT — The script’s explanatory labels are stale.**  
    **S7–8, S23, S59–60.** It now performs an eToro GET despite “No eToro call”; its lower-bounded fiscal query is not “the last four fiscal quarters,” and its coalesced event date is not an announcement date.  
    **Fix:** describe the public calendar GET explicitly and print the actual fiscal/event predicates.

27. **NIT — The round-5 severity totals are wrong.**  
    **R762; findings file’s Round 5, items 1–25.** The individual labels total **14 BLOCKING and 11 WARNING**, not 13 and 12.  
    **Fix:** correct the checkpoint-log totals.

## Round 7 (on the measurement spec v1, `docs/research/2026-10-10-3740-slice-f-dividend-measurement.md` at `da2c481b`)

**S is not ready for checkpoint-1 acceptance.** Its pass branches can accept unmeasured MAX failures, incomplete truth, and unresolved timing outcomes. The 2 bp budget does not establish the claimed safety for the parent book.

No files changed. I checked the cited AAPL CSV values and the 22,879-file count; I did not read panel outcomes or run candidate comparisons. Official source-rule checks are linked below.

References use your **S/P/F/R** notation. **APPLIED** means the correction is present in the specification, not that implementation has been verified.

**Task A — round 6 dispositions**

1. **APPLIED** — S31–44 identifies explicit dividend fields; the adjusted-series inversion problem is removed. Their general semantics still need validation.
2. **PARTIAL** — S47–55 supplies a reference contract, but independence, currency/basis compatibility, and completeness remain unsupported.
3. **PARTIAL** — S234–241 adds freezing, but leaves rule adoption, scoring details, rerun changes, and result-visibility order open.
4. **PARTIAL** — S116–123 labels the historical pilot correctly; C_P remains static and does not implement promised entrant/carried/control coverage.
5. **PARTIAL** — Historical and prospective denominators are separated, but P’s truth construction and maturity are insufficiently specified.
6. **APPLIED** — S133–147 expands filing coverage and distinguishes explicit from derived ex-dates.
7. **PARTIAL** — S79–93 adds a table, but leaves venue rules unpinned and substitutes an unsupported valuation rule.
8. **PARTIAL** — S97–107 distinguishes deadlines, but executable cutoffs, operational availability, and MAX acceptance remain incomplete.
9. **PARTIAL** — H1 is labelled oracle-guided and H2 nominally blind; run ordering and chronological detector use do not enforce blindness.
10. **PARTIAL** — S59–75 defines basic semantics, but security identity, component identity, split conversion, and precision rules remain incomplete.
11. **PARTIAL** — S188–194 adds two-way statuses, but sampled adjudication and unspecified residual treatment undermine the verdict.
12. **PARTIAL** — S167–172 fixes source precedence, but revision, conflict, deduplication, matching, and incompleteness rules are not executable.
13. **PARTIAL** — S198–218 replaces 99% with a budget and per-key bar, but neither is coherent with the claimed book-level safety.
14. **PARTIAL** — Pilot limitations are acknowledged; categorical duration claims and adequacy assertions remain unsupported.
15. **PARTIAL** — S213–223 names branches and an extension, but branches overlap, omit cases, and require unobservable outcomes.
16. **APPLIED** — S21–23 correctly limits failure to this route and identifies stopping as a resource decision.
17. **APPLIED** — S18–20 and S242–244 limit success to another design review and retain parent closure requirements.
18. **PARTIAL** — S173–179 specifies calendar captures, but start censoring, completeness, cutoff coverage, and temporal selection remain open.
19. **PARTIAL** — Headers, validation, and rejected-row retention improve matters; plausible partial tables can still pass.
20. **PARTIAL** — The suffix heuristic is removed, but instrument type plus symbol joins still does not establish common-stock/class identity.
21. **APPLIED** — R28 explicitly limits the publication-pattern observation to one snapshot.
22. **PARTIAL** — R24 fixes the categorical wording; the requested duplicate/price validation and supporting split evidence are not specified here.
23. **APPLIED** — S248–250 requires a consistent read-only snapshot and retained contributing rows; additional provenance gaps are listed below.
24. **PARTIAL** — S251–253 requires an immutable README and distinguishes claims from observations, but does not fully specify download/data-coverage reproduction.
25. **NOT APPLIED** — S259–285 again presents incomplete corrections as applied; pointing a log at this table does not cure that.
26. **PARTIAL** — S253 addresses the GET and fiscal label, but omits the coalesced-event-date versus announcement-date correction.
27. **APPLIED** — S285 states the correct 14 BLOCKING/11 WARNING totals; the claimed checkpoint-log edit is outside the permitted excerpts and unverified.

**Task B — findings**

1. **BLOCKING — H2 blindness is not enforced by the execution order.** **S113–114, S140–148, S234–237.** “H1 and H2 run” permits H1’s reference-guided discoveries to become visible before H2’s output is committed. Later hashing cannot undo that exposure. **Fix:** commit the extractor, scorer, configuration, and isolated H2 output before running or exposing H1, reference joins, or adjudication; prohibit H1-derived assistance to H2.

2. **BLOCKING — The safety argument uses performance outcomes to justify the tolerance.** **S203–205.** The 2 bp bar is defended using stage-B G values, 7.43% and 10.54%. This imports performance outcomes into a measurement-design choice, and the historical gap cannot establish that errors cannot decide a future condition. **Fix:** remove that calibration and the “cannot decide condition 4” claim; derive any safety bound independently from the parent’s permitted exposures and accounting. This is identifiable outcome use in S; I have not established that its cohort reader reads prohibited outcome columns.

3. **BLOCKING — The cohort budget and per-key bar do not bound the parent book’s error.** **S198–207, S218; P215–220.** For example, a missed 0.9% dividend on a name with 0.1% cohort weight contributes only **0.09 bp** to the cohort metric but **9 bp** at the parent’s permitted 10% book weight. It passes the 1% per-key bar. Repeated smaller errors can accumulate. Controls, references, compounding, and changes to MAX exclusions are also unbounded. **Fix:** either define this solely as a pilot screening tolerance without a safety claim, or precommit a conservative bound covering all parent exposures and decision effects.

4. **BLOCKING — The error formula is not executable as written.** **S198–207.** The index denotes both names and event keys; the weight date is unspecified; multiple distributions, shifted dates, and cross-month conflicts lack allocation rules. There is no frozen price source for P, missing/invalid-price rule, split-basis alignment, or exact annualisation convention. **Fix:** specify the complete key-level formula, month assignment, weight vintage, price source/basis, missing-data treatment, and arithmetic.

5. **BLOCKING — MAX is reported but does not decide acceptance.** **S100–107, S149, S198–218.** E_m is defined at D_RET. No explicit failure condition covers a dividend unavailable or wrong at D_MAX but correct by D_RET. Yet such an error can change the parent’s MAX ranking and exclusion. **Fix:** add a separate D_MAX decision rule and permanent missed-cutoff handling. Do not apply a holding-return delay rule to a formation input.

6. **BLOCKING — Incompleteness can make the error score artificially safe.** **S168–169, S200–211, S218–219.** Policy-incomplete keys contribute zero error, with no acceptance cap on their count, weight, duration, or affected months. A route can therefore look accurate by refusing much of the required ledger. **Fix:** retain zero as an accounting convention only alongside explicit availability requirements and worst-case unresolved-error bounds; separate “accurate when available” from “supplies the required inputs.”

7. **BLOCKING — The nine-month outcome cannot be observed on the specified schedule.** **S140–141, S163, S209–223.** P closes adjudication on 2027-03-31, well before nine months after its monthly return deadlines, yet its failure branch imports that condition. H2’s stated filing scan also ends at D_RET(2024-08), not at each event’s later completion horizon. The supplied parent excerpt does not itself establish S’s chosen clock anchor, D_RET + nine months. **Fix:** pin the parent’s actual clock and specify separate deadline and maturity observations, with sufficient capture/adjudication tails and a pending outcome until maturity.

8. **BLOCKING — Sampled adjudication cannot support the whole-cohort pass.** **S190–193, S217–219.** With 301–350 disagreements, 1–50 can remain unadjudicated without forcing inconclusive. Those keys can contain the largest errors or change truth. A random sample of 300 supplies neither their amounts nor a deterministic residual bound. **Fix:** adjudicate every verdict-relevant key or require a conservative residual bound that proves all acceptance conditions regardless of the unadjudicated answers.

9. **BLOCKING — The branch table is neither exhaustive nor mutually exclusive.** **S217–219.** An incomplete run can also contain a decisive error, with no priority rule. H2 has no zero-event or minimum-coverage branch. Missing prices, unknown identity, absent event classes, malformed reference data, and undefined truth may fall through to “none of the above.” **Fix:** define an ordered state machine with explicit prerequisites, failure precedence, and treatment of every unresolved status.

10. **BLOCKING — P’s extension starts before the decision to extend.** **S163, S217, S221.** Initial captures stop at D_RET(2027-01), but inconclusiveness may not be known until 2027-03-31. Extending ex-dates through April then requires February/March calendar observations that were never captured. **Fix:** retain captures continuously through the maximum precommitted extension and tail, or start the extension prospectively after the decision with corresponding dates.

11. **BLOCKING — A failed H2 can be repaired using its answers and still called the same test.** **S221–223, S234–237.** “After the stated cause is fixed” allows extractor, source-rule, or adjudication changes after results are visible. No calendar/resource bound is given for H2’s rerun. **Fix:** distinguish infrastructure retries from substantive changes. Preserve the first result; require a new frozen version and untouched evaluation material for substantive changes, with a finite rerun deadline.

12. **BLOCKING — Date shifting leaves dependent choices unfrozen.** **S238–241.** “Move by whole months” does not select the earliest permitted start or explicitly move adjudication, observation tails, extension dates, detector history, and cohort evidence cutoffs. It permits choosing among later windows. **Fix:** specify one deterministic shift formula for all dependent dates before any shifted-period evidence is examined.

13. **BLOCKING — P’s truth cannot establish recall or independent correctness.** **S180–184.** The union omits events missed by both candidates; the detectors do not close that gap. Agreement between candidates also need not establish truth. Calling the resulting inventory “truth” supports stronger conclusions than this construction warrants. **Fix:** obtain an independent prospective inventory or an independently conducted, complete cohort audit with a frozen protocol. Otherwise limit P to disagreement/detection diagnostics and do not use it to certify completeness.

14. **BLOCKING — Adjudication is biased toward the route being tested.** **S54–55, S184, S190–193.** EDGAR is both the tested route and the sole adjudication source. A real distribution absent from EDGAR cannot reliably be distinguished from a calendar/reference error. An issuer’s announced or expected ex-date also does not automatically override an exchange-designated date. **Fix:** freeze field-specific authority and escalation rules, including designated-date evidence, and leave source-absence disputes unresolved when independent evidence is unavailable. [FINRA Rule 11140](https://www.finra.org/rules-guidance/rulebooks/finra-rules/11140).

15. **BLOCKING — The “completeness detectors” detect only narrow absence cases.** **S145–147, S181–184.** One extracted event suppresses the XBRL trigger even if additional distributions are missing. Declaration periods do not equal ex-date windows. Annual, new, irregular, special, and suspended payers evade the four-quarter rule; “our stored history” has no source, vintage, or missing-history policy. Detector alerts may identify only a name/period, not a scoreable event key. **Fix:** specify temporal alignment, source coverage, alert resolution, and limitations; do not treat a quiet detector as evidence of complete coverage.

16. **BLOCKING — P cannot decide the calendar’s stated role.** **S151, S164–172, S215–219, S242–244.** Its detector role is already fixed, while the verdict assesses only the combined policy. There is no criterion for incremental discoveries, timely detection, false alarms, or EDGAR-only versus combined availability. Calendar trouble can stop an otherwise adequate EDGAR route without establishing that EDGAR failed. **Fix:** precommit separate EDGAR, calendar-detector, and combined-policy outcomes, with an explicit role-selection rule.

17. **BLOCKING — Acquisition windows omit declarations needed for in-window events.** **S133–147, S165, S238.** P specifies daily filings “since the last run” but no initial backlog. A November dividend announced before capture starts can be missed. H1’s 120-day limit and H2’s June start likewise censor sufficiently early declarations. **Fix:** freeze a justified bootstrap inventory/lookback and retained event state, including announcements whose ex-dates lie inside the scoring window.

18. **BLOCKING — An acceptance timestamp alone does not make H2 a chronological replay.** **S104–107, S140–147.** A batch run can use a later XBRL fact or filing to trigger a search that discovers an earlier announcement, then assign the announcement’s old acceptance time. That establishes earlier source content, not discovery by the earlier deadline. **Fix:** replay filings, detector state, searches, and revisions in knowledge-time order; timestamp discovery separately and prohibit future-triggered backdating.

19. **BLOCKING — The extractor and re-search contract leave operational discretion.** **S144–148, S165, S169.** “Parsed from the declaring sentence or table” does not define candidate generation, cross-document references, ambiguous dates/amounts, image-only documents, or negative declarations. “Re-search” specifies neither a different executable operation nor a stopping rule. Human adjudication could silently become route extraction. **Fix:** freeze these decisions and the human/automated boundary, retaining immutable initial output and separate adjudicated truth.

20. **WARNING — H1 is narrower than its source-content claim.** **S113–114, S133–136.** Failure of the frozen extractor within a bounded lookback does not establish that EDGAR lacks the evidence. **Fix:** describe H1 as this extractor’s reference-guided yield, or add a separately specified source-content audit and distinguish acquisition, extraction, identity, dating, and genuine absence.

21. **BLOCKING — The venue rule table is deliberately unfinished at freeze time.** **S79–90, S234–236.** NYSE rule-derived events change status when a script later “pins” rule text. That leaves source adoption and treatment open after the specification hash. The text also assumes NYSE and NYSE American can share the named rule without establishing applicability, and has no other/unknown-venue branch. **Fix:** resolve applicable venue/version rules before freezing, or freeze unsupported venues as unavailable for this version with an explicit verdict consequence.

22. **BLOCKING — The 25% treatment is invented where a source rule governs.** **S92–93.** “Cash amount over the close before the ex-date candidate” introduces a valuation convention, circular date dependency, and 20–30% uncertainty band that the cited rule does not establish. Outside that band, misclassification is still possible. **Fix:** use documented designation/valuation evidence; where unavailable, preserve uncertainty rather than treating the buffer as validation. [FINRA Rule 11140](https://www.finra.org/rules-guidance/rulebooks/finra-rules/11140).

23. **BLOCKING — The timely-notice prerequisite has no evidence rule.** **S85–89.** Naming `unresolved_late` does not say how timely versus late definitive information is established. Filing acceptance before a date is not proof of timely exchange notification. **Fix:** define the required notice/designation evidence and an unknown-notice branch; do not default unknown cases into normal derivation. [FINRA Rule 11140](https://www.finra.org/rules-guidance/rulebooks/finra-rules/11140).

24. **WARNING — Transition and calendar branches are incomplete or overlapping.** **S85–89.** May 25–27 record dates are uncovered. The May 28 row has no small/regular-distribution qualification and overlaps the ≥25% branch. The old-rule row lacks a lower effective-date bound and a documented non-delivery-date treatment. **Fix:** freeze precedence, valid date ranges, venue calendars, and exception branches; use the transition notice within its stated context. [Nasdaq ETA2024-29](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2024-29).

25. **BLOCKING — Current SEC metadata is treated as historical security metadata.** **S59–62, S79–81.** The submissions API supplies current entity metadata and filing history; it does not thereby supply exchange/class identity “as of the filing.” Ticker/exchange arrays also do not establish that an ambiguous declaration concerns the only listed common class. **Fix:** use dated security/class evidence and effective-dated mappings; refuse ambiguity instead of inferring class from array counts. [SEC API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

26. **BLOCKING — Revision selection contradicts conflict refusal.** **S170–172.** “Latest accession wins” and “two un-withdrawn declarations … with different amounts refuse” can both apply to an ordinary correction. The event key contains ex-date, so a corrected ex-date creates a new key rather than replacing the old event. **Fix:** introduce a stable declaration/component identity and explicit amendment, supersession, cancellation, tie, and cross-key revision rules.

27. **BLOCKING — Summation and deduplication can accept a wrong ledger.** **S70–72, S188–194, S218.** Repeated filings can be mistaken for separate components; compensating component errors can yield a matching sum. Component non-reconciliation is only reported, while `duplicate` has no explicit verdict treatment. **Fix:** deduplicate declarations before aggregation, specify allowable netting, and make unresolved component identity/basis affect availability or acceptance.

28. **BLOCKING — Nominal ex-date basis is declared but not produced.** **S67–75, S92, S199.** There is no conversion rule for a declaration followed by a split, simultaneous split/dividend events, retrospectively restated per-share disclosures, or alignment with the prior close. Currency is missing from the reference. **Fix:** specify documented amount/basis transformations and evidence requirements; unknown basis/currency must remain unresolved. This dividend-integration requirement remains even though the split-source obligation is separate.

29. **WARNING — Six-decimal equality is an unsupported tolerance convention.** **S73–75.** The asserted issuer precision is unsupported; decimal rounding necessarily introduces tolerance despite “No other tolerance.” The rounding mode and float-to-decimal conversion are unspecified. **Fix:** retain source decimals, specify exact quantisation and comparison rules, and justify the resulting tolerance without claiming universal issuer precision.

30. **WARNING — The noncash/terminal boundary is not established.** **S63–66; P441–447.** The supplied parent status rules do not demonstrate how spin-offs or in-kind distributions are handled. Cash liquidation/merger proceeds also need separation from dividend credits to prevent overlap with termination accounting. **Fix:** define an explicit handoff and refusal rule for excluded actions and mixed distributions; do not imply that generic status classification resolves them.

31. **BLOCKING — AAPL validation is promoted into dataset-wide semantics.** **S31–55, S259.** The inspected rows support the AAPL examples and existence of separate event fields. They do not establish currency, gross treatment, nominal basis, special-distribution coverage, or conventions across every series. “All scored rather than assumed” cannot hold when matching errors and shared omissions escape adjudication. **Fix:** distinguish observed schema from inferred semantics, validate required conventions independently, and keep unvalidated cases outside accepted truth.

32. **WARNING — Independence is asserted from a registry label.** **S49–55.** A Yahoo-derivative label does not establish the candidate/reference acquisition paths or absence of shared upstream errors, particularly for eToro. “Market-data vendor, not issuer filings” is not demonstrated lineage. **Fix:** document available lineage and qualify independence as unverified where necessary; distinguish separate providers from independent error mechanisms.

33. **WARNING — A database digest does not prove correspondence to the mirror commit.** **S31, S47–48, S248–250.** The manifest can bind a database result and a commit identifier without proving that the former was generated from the latter. **Fix:** retain the file-to-series mapping, input file hashes, ingest/transformation version, and a reconciliation of contributing database values to immutable mirror bytes.

34. **BLOCKING — The “no censoring” claim is unsupported.** **S127–132.** Only 970 of 1,000 series are said to reach September. A global last-bar date cannot establish coverage for earlier-ending series or interior gaps. Absence of an event row in missing coverage is not evidence of no dividend. **Fix:** build per-security/session reference coverage, adjudicate gaps and terminations, and distinguish uncovered intervals from known non-events.

35. **BLOCKING — C_P does not implement the parent’s security classifier.** **S155–159; P39–45, P473–476.** eToro’s stocks type, a US exchange string, and a 10-K/10-Q link do not specify accepted common-stock classes, ADR treatment, funds, preferreds, units, primary listings, or unknown types. **Fix:** apply the parent’s explicit classifier or declare and justify a bounded pilot exception before drawing the cohort.

36. **WARNING — The ME approximation changes documented source rules without a sufficient contract.** **S158–161; P488–489.** “Latest” cover count omits the parent’s precedence, 15-month limit, blocked-read treatment, and context-date/acceptance-date split basis. Price vintage, multiple facts, ties, and missing ME are also unspecified. **Fix:** reproduce the cited parent rule or enumerate the exact pilot substitutions and their consequences; freeze ranking and weighting rules consistently.

37. **BLOCKING — P cannot validate the entrants, carried names, and controls promised by S.** **S123, S155–163, S227, S262; P395–398, P434–439.** C_P is a single frozen membership list. It has no monthly entrant construction or required-set union and no specified carried/control/reference validation. **Fix:** add the required dynamic-set protocol, or remove these claims and explicitly defer each validation to a defined later test.

38. **WARNING — Identity joins are not effective-dated or uniquely constrained.** **S59, S155–159, S178–179.** A cohort-date CIK and current symbol join do not cover renames, reused symbols, multiple instruments/classes, successor issuers, or exchange changes. No ambiguous-match or mapping-change policy is given. **Fix:** freeze security identifiers, effective intervals, cardinality checks, and treatment of unmatched and changed identities without silently dropping denominator members.

39. **WARNING — The duration and sample-size claims overstate coverage.** **S129–132, S162, S225–230.** Eleven months do not ensure three observations for every quarterly payer; a three-month interval does not ensure exactly one. Initiations, suspensions, shifts, and terminations defeat both claims. A quarter does not resolve the acknowledged annual-payer limitation. “150 is below expected” is not an adequacy argument. **Fix:** call these scheduling expectations, specify exception/venue/event-class coverage requirements, and attach conclusions to observed coverage rather than total count alone.

40. **BLOCKING — Calendar completeness checks still permit silent partial success.** **S174–177.** One valid row can coexist with many rejected rows. A stable partial response, wrong table with matching headers, or gradual losses below 50% can pass. The first capture has no baseline. **Fix:** define intended-table identity, full retrieval/pagination checks, rejected-row consequences, baseline validation, and an explicit completeness-unknown state that propagates to scoring.

41. **WARNING — The calendar’s event/amount contract is missing.** **S166, S173–179; R28.** The page has annual and periodic amounts and no currency. S says “amounts” without selecting fields, defining units, or mapping missing/indicative values into the standalone comparison. **Fix:** freeze actual headers, field semantics, currency/basis treatment, and validity rules; score detector performance separately where amount semantics remain unknown.

42. **BLOCKING — Calendar matching and temporal state are unspecified.** **S168–179.** “Matching EDGAR event” could mean identity/date only or amount agreement as well. Multiple captures, disappearing rows, changed dates, and conflicting revisions lack an as-of selection rule. The policy can ignore a calendar disagreement whenever some EDGAR event is present. **Fix:** freeze matching fields, capture selection, freshness, revision/removal semantics, and the consequence of each disagreement at each deadline.

43. **WARNING — Capture cadence is not connected to event visibility and deadlines.** **S162–179, S238.** Starting on the first ex-date day can miss rows published and removed earlier. One successful daily capture does not establish coverage at a later same-day cutoff. “US business day,” legitimate empty tables, first/last observation, and retry-success selection remain undefined. **Fix:** specify a pre-window observation/bootstrap policy, calendar/time-zone rules, per-cutoff usable captures, and justified retention/tail assumptions.

44. **BLOCKING — EDGAR’s prospective acquisition process is too weakly specified to confirm H2.** **S165, S217.** “Run daily” leaves run times, retries, pagination/history traversal, document completeness, checkpoint advancement, and recovery unspecified. P has no explicit incomplete-EDGAR-run branch. **Fix:** freeze acquisition/completeness rules and distinguish source availability, acquired content, and operational extraction by each deadline.

45. **BLOCKING — Later evidence and revisions can violate the parent’s cutoff and immutability rules.** **S169–172, S209–211; P411–416, P441–447.** “Completed later” does not distinguish late retrieval of cutoff-eligible evidence from newly published evidence. Latest-accession selection can also replace a component already bound. **Fix:** separate immutable acquisition bindings, deterministic as-of selection, late completion of missing components, and post-cutoff truth corrections; enumerate permanently refusing defects.

46. **BLOCKING — Deadline definitions are not fully executable or faithful to all parent distinctions.** **S97–107; P402–413, P420–421.** D_RET specifies a session number but no clock time. D_MAX equates a window ending *before* the next open with “the next open.” Venue calendars, early closes, time zones, and acceptance-time boundary comparisons are absent. The parent’s EDGAR exception references step 1’s evidence cutoff, which S does not resolve. **Fix:** freeze exact timestamp functions, strict/inclusive comparisons, source-specific evidence eligibility, and acquisition versus binding deadlines.

47. **WARNING — H2 proves source-content availability more readily than operational readiness.** **S104–107, S225–226.** Historical acceptance times support knowledge-time filtering but do not show that the forward route can fetch, parse, map, and bind everything in time. **Fix:** narrow H2’s conclusion and make the operational confirmation requirement explicit rather than treating timestamps alone as sufficient point-in-time feasibility.

48. **WARNING — The measurement is not wholly unexposed before freezing.** **S3, S124–128, S228–229, S234–235.** Reference outcomes—payer counts, event frequencies, yield quantiles, and maximum yield—have already been inspected and inform design choices. This is not proof of candidate-comparison leakage, but “Nothing has been compared” does not disclose the relevant exposure boundary. **Fix:** record these as exploratory information, identify all already-seen data, and distinguish a prospective freeze from an untouched evaluation.

49. **WARNING — The earlier unverified panel read remains unreconciled.** **S118–128.** Repeating it later through the verified reader does not establish the integrity or access compliance of the earlier read. The spec promises restricted columns, but does not document the earlier access or its retained evidence. **Fix:** record the earlier read’s scope and provenance, verify the admitted planning material, and specify the applicable access logging and output restrictions. Do not claim an outcome-column violation without evidence.

50. **WARNING — Reproducibility retains hashes where replay also needs content and decisions.** **S248–253.** A document digest is not the document. The contract omits retention of EDGAR bytes, precise document locators, rule/calendar versions, parser/scorer dependencies, and detailed adjudication evidence and decisions. A transaction snapshot identifier alone is not a durable database snapshot. **Fix:** retain the contributing bytes and rows, deterministic configuration, evidence pointers, adjudication record, and executable versions sufficient to reproduce the verdict.

51. **WARNING — Report denominators and evidence classes are ambiguous.** **S79, S125–136, S188–194, S206–207.** “ME weight” can mean payer weight counted once or weight repeated per event. Non-payers belong to a name population but do not automatically create event keys. E1’s date-only wording differs from its later date-and-amount definition; date-only, amount-unknown, and unresolved identity cases lack a complete classification. **Fix:** publish separate name-, event-, and month-level denominators and a complete evidence/status schema.

52. **WARNING — The freeze omits verdict-affecting implementation choices.** **S190–192, S234–237.** The seed is only frozen with the disagreement list, after results exist; the scorer, adjudication selection, canonicalisation, and source-rule completion are not explicitly committed before inspection. **Fix:** commit all deterministic algorithms and the seed or a precommitted seed derivation before results, then bind the resulting lists without permitting reselection.

53. **WARNING — Probe corrections remain incomplete promises.** **S251–253, S281–284; F1034–1052.** The next-run paragraph does not require the split probe’s uniqueness/finite-positive-price checks, a preserved unauthenticated PWB data response and verified maximum date, or the corrected coalesced-event-date label. **Fix:** specify those requirements or mark the corresponding findings partial/deferred. Their lack of relevance to the dividend verdict is not evidence that they were applied.

54. **WARNING — The disposition table is itself an inaccurate judgement artefact.** **S259–285.** It conflates added prose, partial requirements, future work, and verified corrections. The claims about checkpoint-log edits cannot be checked within R10–75. **Fix:** replace the table with accurate individual dispositions, distinguish verified text from future implementation, and label inaccessible log claims unverified.

55. **NIT — “Exact integer counts” does not describe the decisive arithmetic.** **S213–218.** The branches also depend on annualised weighted errors and yield ratios, whose arithmetic and rounding are unspecified. **Fix:** say that count thresholds use integers and separately specify exact decimal/rational arithmetic and boundary comparisons for monetary and yield tests.
