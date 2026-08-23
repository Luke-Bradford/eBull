# R6 #2914 operational-rules declaration

Status: frozen before the first #2914 reference-series census or operational-rule result.

This is not a strategy arm and does not claim return. It installs two rules for trades and
declarations that another admissible process has already authorised. It must add no holding, trade,
rebalance, turnover, data source, or capital permission.

## Rule 1 — turn-of-month scheduling preference

- Version: `r6-2914-turn-of-month-preference-v1`.
- Source rule: #2914 and the R6 evidence memo on #2899, citing Xu and McConnell, *Equity Returns at
  the Turn of the Month*.
- Window: the seven venue sessions at offsets `-3, -2, -1, 0, +1, +2, +3`, where offset zero is
  the last trading session of the target calendar month.
- Input: a complete, strictly increasing, duplicate-free sequence of sessions for the instrument's
  actual venue. A US calendar must not be silently reused for a foreign venue.
- Output: the preference window only. The rule does not choose a security, create an order, alter an
  amount, change turnover, or override an existing earliest/latest execution constraint.
- Failure: refuse a missing target-month anchor, an incomplete seven-session window, duplicate or
  unordered sessions, or a window whose `-1`/`+1` members do not straddle the anchor in session order.

## Rule 2 — factor-valuation declaration record

- Version: `r6-2914-factor-valuation-record-v1`.
- Source rule: #2914 and the R6 evidence memo on #2899, citing Arnott, Beck and Kalesnik,
  *Forecasting Factor and Smart Beta Returns* and AQR's contested factor-timing position.
- The record is context, never a signal or launch veto. It cannot reorder arms, change weights, or
  time an existing factor in or out.
- A recorded value must name the factor, spread measure, unit, as-of date, immutable source snapshot
  SHA-256, dataset and series. A return series is not a valuation spread and must be refused.
- When the accepted free #2912 reference corpus contains no genuine factor-valuation-spread series,
  record `unavailable` with the full-population census and reason. Do not invent a proxy from recent
  factor performance. Per #2914, unavailability does not block an arm.

## Frozen census and verdict

- Population: every distinct series in every accepted `reference_data_snapshots` row currently
  stored by #2912, grouped by source, dataset, series and unit. No sample.
- Transaction: one read-only, repeatable-read snapshot.
- The census reports the accepted snapshot and series counts, units, observation bounds, and the
  number of genuine valuation-spread series admitted by the frozen semantic allow-list.
- The allow-list is empty because #2912 declares its current French and AQR datasets as factor-return
  validation/context sources and FRED as macro context. Adding a valuation-spread dataset requires a
  new version and source rule; similarity of a series name is not admission.
- PASS if the implementation returns the exact seven-session window, refuses malformed calendars,
  cannot authorise a trade, and records the #2912 valuation status without relabelling returns.
- FAIL if either rule adds turnover/authority, guesses a foreign calendar, or reports a return series
  as valuation spread.

## Reporting boundary

The 15% and 58% haircuts, absolute net return and same-window buy-and-hold comparison are `N/A`: this
ticket is explicitly not an arm and observes no return outcome. Any performance number attached to
this ticket would turn an operational preference into an undeclared strategy.
