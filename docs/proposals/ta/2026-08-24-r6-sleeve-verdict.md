# R6 deployable-sleeve verdict after #2908

Status: **AUTHORITATIVE CURRENT R6 VERDICT.** This supersedes
[`2026-08-23-r6-programme-verdict.md`](2026-08-23-r6-programme-verdict.md),
whose no-arm rationale predated the repaired #2900 point-in-time path and the
completed #2908 cycle.

Verdict: **£0 — NO ACTIVE R6 SLEEVE IS JUSTIFIED**

The research cycle found a clean point-in-time spine and an apparent robust return versus literal buy-and-hold,
but the active exclusion itself failed. The simpler annual equal-weight population beat dilution, filing risk and
their union on identical inputs. That is the control the sleeve must respect.

## Rejected sleeve specification

- **Would hold:** every executable SEC-identified listed common-equity class except Nsi portfolio 10; 4,034 names
  at the latest formation.
- **Would exclude:** 737 worst positive-issuance names at 2024 formation; missing share pairs are neutral holds;
  warrants, units, rights, preferred/debt/depositary classes, ambiguous mappings and unfillable classes never enter.
- **Weights/rebalance:** equal weight, annually after the last June session; no optimisation or intra-year
  performance reorder.
- **Execution/cost:** next-session adjusted open, final adjusted close, maximum measured spread (`1.45%` quoted,
  `0.725%` per traded dollar), zero leverage/borrow/carry. Worst measured turnover `3.289239x`; cost `2.384698%`
  (£1,192.35 per £50,000).
- **Net evidence:** worst +16.4349%; after 15% haircut +13.8793%; after 58% haircut +6.5533%; same-window net
  buy-and-hold +0.6038%; identical annual 1/N +16.8024%.
- **Why rejected:** −0.3675 pp versus the simpler 1/N control; only the refresh/rebalance component beats
  buy-and-hold. Quality overlap is untested, current broker mapping is not established, and 4,034 equal positions
  are only £12.39 each in a £50,000 pot.

ISA allocation is **£0** because #2915 found no supported public-API route. Ordinary-account allocation is also
**£0**. A 0.70% one-way FX sensitivity is £350 on £50,000 and is not included in the USD backtest; repeated
conversion is forbidden.

## What is justified instead

The evidence favours the cheapest genuinely tradable market exposure available to the operator, with low
turnover and no exclusion overlay. The research corpus's 4,771-name annual 1/N control is evidence, not itself a
deployable eToro product: current instrument-level reachability and minimum-notional fit are unknown. Selecting a
convenient subset now would be a new, post-result strategy and is prohibited.

## Conditions to reconsider / turn off

Reconsider non-zero R6 capital only after a separately preregistered, broker-reachable simple market sleeve maps at
least 95% of weight, fits minimum notionals, beats its identical buy-and-hold after the same costs and both
haircuts, and passes paper reconciliation through the existing execution guard and kill switch. If later deployed,
turn it off on PIT/hash failure, broker-map drift below 95%, cost above the frozen band, missed annual rebuild,
guard/kill-switch refusal, or loss of either positive-absolute or buy-and-hold status at a preregistered review.

Until then, the deployable specification is: hold nothing, place no orders, incur £0 cost, and allocate £0.
