# Codex ckpt-1 v1 findings: families 1–4 re-screen (verbatim)

Target: `2026-09-28-hunt-families-1-4-rescreen.md` v1. All 48 applied or answered in v2.

1. **L13–18: impossibility is not established.** Published estimates below thresholds are not upper bounds on achievable returns. Hunt 1 explicitly calls its closure a budget decision and leaves larger edges unexamined (L20–23).

2. **L22–23: both conditions cannot generally be checked without data.** Construction-specific excess and NW variance require evidence; L69 itself acknowledges unmeasured TE.

3. **L21: “implemented” is potentially misleading.** The permitted door code says hunt 2’s economic gate is not yet implemented and validation freezes refuse until it is (L89–93).

4. **L42–47: TE algebra is correct, conditional on T.** Defining annual `TE_NW = √(252 Ŝ_NW)`, T=3,079 gives **7.27918%**, hence IR **1.09902**. But **3,079 is not traceable in the permitted declaration excerpt**; that excerpt reports 3,143 validation sessions. Explain the 64-session difference. Using 3,143 gives 7.35444%.

5. **L49, L77: distinguish discovery from validation IR.** The cap constrains discovery NW variance carried forward. It does not impose a minimum *realised validation* IR of 1.10.

6. **L50–51: “gross IR” uses the wrong denominator unless costs are deterministic.** Gross excess divided by net-excess TE is not generally gross IR; varying costs change variance and covariance.

7. **L47, L81: low TE alone does not establish admissible power.** The door also refuses missing, degenerate, insufficient discovery data and short target samples; its lag depends on h.

8. **L51: hunt 2’s ~19% TE and 0.210 MDE are untraceable in the permitted excerpt.** They are internally consistent: 0.210 implies approximately 19.108% TE at T=3,079.

9. **L25–29: per-side wording is wrong.** BANDS contains round-trip spreads; each side pays half the band. The table correctly charges one whole band per round trip.

10. **L25–38: costs are approximations, not exact arithmetic exclusions.** Hunt 1 explicitly gives unchanged-price cost `s/(1+s/2)`. At h=1 this yields approximately **81.014% / 362.770%**, versus **81.144% / 365.400%** in the table.

11. **L38: rounding ambiguity.** Stress drag is exactly 1.450 and required gross excess 9.450. Conventional half-up rounding gives **1.5 / 9.5**; displaying **1.5 / 9.4** needs an explicit rounding convention.

12. **L25: holding period does not universally determine turnover.** `252/h` assumes fully replaced, continuously occupied cohorts. Retaining names, partial rebalancing, skipped events and netting can produce different costs.

13. **L29 versus family verdicts: utilisation is introduced but never established.** Full-book thresholds cannot reject every implementation. Lower utilisation also changes benchmark-relative returns; both sides require recalculation.

14. **L37–38: h=126/252 are outside the cited harness’s 63-session cap.** These are hypothetical extensions, not established executable constructions; corresponding lag/sample checks are absent.

15. **L26–40: bands are frozen counterfactuals, not observed historical costs.** The declaration expressly limits its census to one calm 2026 midday capture, excluding historical opens and stressed states.

16. **L54–56: timing lemma needs return-normalisation assumptions.** It is exact for whole-session SPY/cash switching under the stated accounting, not automatically for partial-session trading.

17. **L65: overnight excess omits a cross term.** With overnight return `o` and intraday return `i`, full-day return is `(1+o)(1+i)−1`; overnight-only excess is `−(1+o)i`, not exactly `−i`.

18. **L54: zero idle-cash yield is untraceable in the permitted declaration excerpt.** The cited “Utilisation” section is outside the allowed material.

19. **L57–59: exclusion hurdle omits costs.** For fixed weights it is `u ≥ (0.08 + cost)/w`, using comparable total returns.

20. **L57–59: time-varying exclusions require `252·mean(w_t u_t)`.** Average weight times average underperformance omits their covariance and can misstate a conditional screen’s edge.

21. **L64: “SPY … earns the same drift” is unsupported.** Hunt 1 quotes a generic overnight drift, not a matched SPY estimate establishing equality.

22. **L64: “§3.2 … KILLED” is not verifiable from the permitted material.** The quoted section itself was not supplied.

23. **L65: the Cliff–Cooper–Gulen quotation and sample cannot be checked.** None of the permitted context reproduces that abstract. “Prior decade” also supplies no explicit sample dates.

24. **L65: qualitative wording is not a quantitative bound.** “Close to zero and sometimes negative” does not establish a numerical lower bound for the relevant SPY intraday mean. An equity-premium statement also does not itself supply raw, dividend-inclusive SPY returns.

25. **L65: the 0.007%-per-side tariff and nightly financing claim are untraceable.** Conditional multiplication is correct: tariff drag is **3.528%**, requiring **11.528%** avoided losses before further costs.

26. **L65: the CFD tariff cannot reject the SPY ETF alternative.** These require separate spread, tariff, dividend and financing accounts. Their equivalence to the SPY total-return tracker is not demonstrated.

27. **Family 1 omissions:** selective overnight dates/names, partial exposure and longer-held selection based on overnight/intraday signals are not screened. Universal nightly full replacement does not bound these constructions.

28. **L66: GKM’s reference-relative premium is not SPY excess.** Required conversion includes reference-minus-SPY return; that term is unknown. Hunt 1 expressly warns about this comparator mismatch.

29. **L67: the withdrawn 63-day bound is effectively reinstated.** The source gives a **100-day, two-leg** result, specifically for small/medium stocks, with neither a 63-day estimate nor a leg split. It cannot establish failure of the 63-day long leg.

30. **Family 2 omissions:** liquidity provision/reversal and regime-conditioned liquidity/volume constructions are absent. The permitted family 4–5 pre-check discusses these and explicitly says its measured result does not bound VIX-slope or credit-spread conditioning.

31. **L68–69: four-factor alpha is not excess over SPY.** Factor exposures and factor/benchmark returns are missing. Consequently “clears base,” “fails stress,” and the 1.9/11.6-point margins are conditional proxy comparisons, not demonstrated book outcomes.

32. **L69: average decay is promoted into a certain haircut.** Hunt 1 explicitly labels the 58% McLean–Pontiff decline a scenario across predictors, not a measured decline for this signal.

33. **L69: “tens of names” is not supported by the cited census.** Hunt 1 reports annual series counts before filters, not simultaneous portfolio holdings, independent issuers or TE.

34. **L68–69: population and portfolio mismatches remain unresolved.** Hunt 1 flags ADR exclusions, calendar-month versus rolling-cohort holdings, dividend treatment and unmodelled foreign-dividend costs. Its alpha cannot be transferred unchanged.

35. **L68–69, L80: publication-sample overlap is omitted.** Hunt 1 says H&S’s estimation extends through 2011, overlapping validation beginning in 2009. This conflicts with the stated requirement for selection without validation-era data.

36. **Family 3 omissions:** persistent payer selection, retained holdings across events, partial calendar tilts and event-specific entry/exit rules are not screened. A fully replaced h=21 portfolio does not bound them.

37. **L70: McConnell–Xu’s figure is not a SPY-sample estimate.** The cited source describes CRSP value-weighted returns over **1926–2005**. Applying it directly to SPY’s discovery/validation returns is an unsupported transfer.

38. **L70: calendar annualisation is approximate.** `16×12` assumes 192 non-turn sessions within a 240-session year; `252×16/20` gives 201.6 sessions and **0.2016%**, versus **0.192%**. Actual calendar counts are absent.

39. **L70: pre-holiday timing is unscreened.** More days out does not imply a smaller excess; their return sum determines it. The cited pre-check explicitly says pre-holiday returns and frequency were not computed.

40. **L71: absent magnitude is not arithmetic failure.** The permitted sources establish direction only; they provide no upper bound on FTD/SI underperformance or attainable excess.

41. **L71: exclusion weights are illustrative, not measured.** No evidence establishes that affected SPY weight lies at 1/5/10/20%, or that larger/ranked exclusions are inadmissible. Abnormal-return direction also does not quantify the required rest-minus-excluded raw return.

42. **L72: FTD incorrectly inherits SI’s missing discovery history.** The cited table supplies **2004–2008 FTD discovery data**, and explicitly says its TE could be measured after ingest. Publication-clock and mapping problems are separate unresolved issues.

43. **L72: “SI starts in 2017” overgeneralises a dataset limitation.** That date applies to the available FINRA series; the same permitted source cites SI research covering 1988–2005.

44. **L72: the short-book thresholds lack traceable benchmark figures.** Discovery 8.2% and validation 15.9% are absent from the permitted excerpt; their sample and arithmetic-versus-compounded units cannot be checked. The displayed identity additionally assumes a fully short unit-notional book before costs/cash yield.

45. **Family 4 omissions:** low-SI positive selection deserves a distinct construction. The cited abstract says its positive abnormal returns often exceed the high-SI leg’s negative returns. FTD-screened long selection likewise remains unbounded.

46. **A mathematical counterexample survives the screen.** Hypothetically, exclude weight `w=50%`, with rest-minus-excluded mean **30%/yr** and NW TE **10%/yr**. Under fixed weights and deterministic h=63 stress costs, excess is **15−5.8=9.2%**, TE **5%**, MDE₈₀ **5.50%**. This is not empirical evidence of an investable strategy; it disproves the claimed arithmetic impossibility.

47. **Combinations are not screened.** Tracker-plus-signal allocations and multiple family sleeves can change turnover and covariance. Diversification cannot manufacture mean, but can let adequate-mean signals satisfy the TE cap despite standalone power failures.

48. **L74–84: remaining provenance gaps.** Value’s 0.87–0.94 is repeated in permitted context, but its underlying sample/calculation is unavailable. The programme-wide absence claim, family 4–8 coverage, recording-start/wake date, queue priority and API-restoration timestamp cannot be verified from the allowed sources.
26,318
Line references are to the [reviewed document](/Users/lukebradford/Dev/.ebull-autonomy/docs/proposals/ta/2026-09-28-hunt-families-1-4-rescreen.md). Only the permitted material was read.

1. **L13–18: impossibility is not established.** Published estimates below thresholds are not upper bounds on achievable returns. Hunt 1 explicitly calls its closure a budget decision and leaves larger edges unexamined (L20–23).

2. **L22–23: both conditions cannot generally be checked without data.** Construction-specific excess and NW variance require evidence; L69 itself acknowledges unmeasured TE.

3. **L21: “implemented” is potentially misleading.** The permitted door code says hunt 2’s economic gate is not yet implemented and validation freezes refuse until it is (L89–93).

4. **L42–47: TE algebra is correct, conditional on T.** Defining annual `TE_NW = √(252 Ŝ_NW)`, T=3,079 gives **7.27918%**, hence IR **1.09902**. But **3,079 is not traceable in the permitted declaration excerpt**; that excerpt reports 3,143 validation sessions. Explain the 64-session difference. Using 3,143 gives 7.35444%.

5. **L49, L77: distinguish discovery from validation IR.** The cap constrains discovery NW variance carried forward. It does not impose a minimum *realised validation* IR of 1.10.

6. **L50–51: “gross IR” uses the wrong denominator unless costs are deterministic.** Gross excess divided by net-excess TE is not generally gross IR; varying costs change variance and covariance.

7. **L47, L81: low TE alone does not establish admissible power.** The door also refuses missing, degenerate, insufficient discovery data and short target samples; its lag depends on h.

8. **L51: hunt 2’s ~19% TE and 0.210 MDE are untraceable in the permitted excerpt.** They are internally consistent: 0.210 implies approximately 19.108% TE at T=3,079.

9. **L25–29: per-side wording is wrong.** BANDS contains round-trip spreads; each side pays half the band. The table correctly charges one whole band per round trip.

10. **L25–38: costs are approximations, not exact arithmetic exclusions.** Hunt 1 explicitly gives unchanged-price cost `s/(1+s/2)`. At h=1 this yields approximately **81.014% / 362.770%**, versus **81.144% / 365.400%** in the table.

11. **L38: rounding ambiguity.** Stress drag is exactly 1.450 and required gross excess 9.450. Conventional half-up rounding gives **1.5 / 9.5**; displaying **1.5 / 9.4** needs an explicit rounding convention.

12. **L25: holding period does not universally determine turnover.** `252/h` assumes fully replaced, continuously occupied cohorts. Retaining names, partial rebalancing, skipped events and netting can produce different costs.

13. **L29 versus family verdicts: utilisation is introduced but never established.** Full-book thresholds cannot reject every implementation. Lower utilisation also changes benchmark-relative returns; both sides require recalculation.

14. **L37–38: h=126/252 are outside the cited harness’s 63-session cap.** These are hypothetical extensions, not established executable constructions; corresponding lag/sample checks are absent.

15. **L26–40: bands are frozen counterfactuals, not observed historical costs.** The declaration expressly limits its census to one calm 2026 midday capture, excluding historical opens and stressed states.

16. **L54–56: timing lemma needs return-normalisation assumptions.** It is exact for whole-session SPY/cash switching under the stated accounting, not automatically for partial-session trading.

17. **L65: overnight excess omits a cross term.** With overnight return `o` and intraday return `i`, full-day return is `(1+o)(1+i)−1`; overnight-only excess is `−(1+o)i`, not exactly `−i`.

18. **L54: zero idle-cash yield is untraceable in the permitted declaration excerpt.** The cited “Utilisation” section is outside the allowed material.

19. **L57–59: exclusion hurdle omits costs.** For fixed weights it is `u ≥ (0.08 + cost)/w`, using comparable total returns.

20. **L57–59: time-varying exclusions require `252·mean(w_t u_t)`.** Average weight times average underperformance omits their covariance and can misstate a conditional screen’s edge.

21. **L64: “SPY … earns the same drift” is unsupported.** Hunt 1 quotes a generic overnight drift, not a matched SPY estimate establishing equality.

22. **L64: “§3.2 … KILLED” is not verifiable from the permitted material.** The quoted section itself was not supplied.

23. **L65: the Cliff–Cooper–Gulen quotation and sample cannot be checked.** None of the permitted context reproduces that abstract. “Prior decade” also supplies no explicit sample dates.

24. **L65: qualitative wording is not a quantitative bound.** “Close to zero and sometimes negative” does not establish a numerical lower bound for the relevant SPY intraday mean. An equity-premium statement also does not itself supply raw, dividend-inclusive SPY returns.

25. **L65: the 0.007%-per-side tariff and nightly financing claim are untraceable.** Conditional multiplication is correct: tariff drag is **3.528%**, requiring **11.528%** avoided losses before further costs.

26. **L65: the CFD tariff cannot reject the SPY ETF alternative.** These require separate spread, tariff, dividend and financing accounts. Their equivalence to the SPY total-return tracker is not demonstrated.

27. **Family 1 omissions:** selective overnight dates/names, partial exposure and longer-held selection based on overnight/intraday signals are not screened. Universal nightly full replacement does not bound these constructions.

28. **L66: GKM’s reference-relative premium is not SPY excess.** Required conversion includes reference-minus-SPY return; that term is unknown. Hunt 1 expressly warns about this comparator mismatch.

29. **L67: the withdrawn 63-day bound is effectively reinstated.** The source gives a **100-day, two-leg** result, specifically for small/medium stocks, with neither a 63-day estimate nor a leg split. It cannot establish failure of the 63-day long leg.

30. **Family 2 omissions:** liquidity provision/reversal and regime-conditioned liquidity/volume constructions are absent. The permitted family 4–5 pre-check discusses these and explicitly says its measured result does not bound VIX-slope or credit-spread conditioning.

31. **L68–69: four-factor alpha is not excess over SPY.** Factor exposures and factor/benchmark returns are missing. Consequently “clears base,” “fails stress,” and the 1.9/11.6-point margins are conditional proxy comparisons, not demonstrated book outcomes.

32. **L69: average decay is promoted into a certain haircut.** Hunt 1 explicitly labels the 58% McLean–Pontiff decline a scenario across predictors, not a measured decline for this signal.

33. **L69: “tens of names” is not supported by the cited census.** Hunt 1 reports annual series counts before filters, not simultaneous portfolio holdings, independent issuers or TE.

34. **L68–69: population and portfolio mismatches remain unresolved.** Hunt 1 flags ADR exclusions, calendar-month versus rolling-cohort holdings, dividend treatment and unmodelled foreign-dividend costs. Its alpha cannot be transferred unchanged.

35. **L68–69, L80: publication-sample overlap is omitted.** Hunt 1 says H&S’s estimation extends through 2011, overlapping validation beginning in 2009. This conflicts with the stated requirement for selection without validation-era data.

36. **Family 3 omissions:** persistent payer selection, retained holdings across events, partial calendar tilts and event-specific entry/exit rules are not screened. A fully replaced h=21 portfolio does not bound them.

37. **L70: McConnell–Xu’s figure is not a SPY-sample estimate.** The cited source describes CRSP value-weighted returns over **1926–2005**. Applying it directly to SPY’s discovery/validation returns is an unsupported transfer.

38. **L70: calendar annualisation is approximate.** `16×12` assumes 192 non-turn sessions within a 240-session year; `252×16/20` gives 201.6 sessions and **0.2016%**, versus **0.192%**. Actual calendar counts are absent.

39. **L70: pre-holiday timing is unscreened.** More days out does not imply a smaller excess; their return sum determines it. The cited pre-check explicitly says pre-holiday returns and frequency were not computed.

40. **L71: absent magnitude is not arithmetic failure.** The permitted sources establish direction only; they provide no upper bound on FTD/SI underperformance or attainable excess.

41. **L71: exclusion weights are illustrative, not measured.** No evidence establishes that affected SPY weight lies at 1/5/10/20%, or that larger/ranked exclusions are inadmissible. Abnormal-return direction also does not quantify the required rest-minus-excluded raw return.

42. **L72: FTD incorrectly inherits SI’s missing discovery history.** The cited table supplies **2004–2008 FTD discovery data**, and explicitly says its TE could be measured after ingest. Publication-clock and mapping problems are separate unresolved issues.

43. **L72: “SI starts in 2017” overgeneralises a dataset limitation.** That date applies to the available FINRA series; the same permitted source cites SI research covering 1988–2005.

44. **L72: the short-book thresholds lack traceable benchmark figures.** Discovery 8.2% and validation 15.9% are absent from the permitted excerpt; their sample and arithmetic-versus-compounded units cannot be checked. The displayed identity additionally assumes a fully short unit-notional book before costs/cash yield.

45. **Family 4 omissions:** low-SI positive selection deserves a distinct construction. The cited abstract says its positive abnormal returns often exceed the high-SI leg’s negative returns. FTD-screened long selection likewise remains unbounded.

46. **A mathematical counterexample survives the screen.** Hypothetically, exclude weight `w=50%`, with rest-minus-excluded mean **30%/yr** and NW TE **10%/yr**. Under fixed weights and deterministic h=63 stress costs, excess is **15−5.8=9.2%**, TE **5%**, MDE₈₀ **5.50%**. This is not empirical evidence of an investable strategy; it disproves the claimed arithmetic impossibility.

47. **Combinations are not screened.** Tracker-plus-signal allocations and multiple family sleeves can change turnover and covariance. Diversification cannot manufacture mean, but can let adequate-mean signals satisfy the TE cap despite standalone power failures.

48. **L74–84: remaining provenance gaps.** Value’s 0.87–0.94 is repeated in permitted context, but its underlying sample/calculation is unavailable. The programme-wide absence claim, family 4–8 coverage, recording-start/wake date, queue priority and API-restoration timestamp cannot be verified from the allowed sources.
