# #3046 residual 2 — the weekend-bar population, and the constraints any fix must satisfy

Residual 2 as filed: *"The weekend/carry-forward write path. Unchanged and still unbuilt;
last session's operand analysis stands (a blanket weekday cut has a large uncharacterised
blast radius, and the shape underneath is carry-forward rather than weekend-ness)."*

This characterises the population and tests the premises the direction rests on. Read-only
throughout; **nothing is built and no rule is changed.**

⚠ **Revision note.** Draft 1 recommended a volume-based discriminator in a derived layer.
**That recommendation is WITHDRAWN** — Codex checkpoint 1 showed it was look-ahead, an
order of magnitude smaller than I had measured, and in direct conflict with a documented
source rule. What replaces it is a constraint set, not a different answer. The reasoning
is kept below because the way it failed is the useful part.

## Source rule

All line numbers verified at write time.

- **`sql/247_price_quarantine.sql:4-7`** — *"DERIVED, NEVER IN-PLACE … `price_daily` is
  raw vendor data and stays that way: a verdict written onto it could not be replayed at
  an older rule set."*
- **`app/services/price_quarantine.py:309/320/335/348`** — `rule_b1` (non-positive or NULL
  OHLC), `rule_b2` (containment), `rule_b3` (phantom wick), `rule_b4` (reverting spike).
- **`app/services/price_quarantine.py:113,116`** — `hole_days = 10` for both exchange
  parameter sets; the rationale is at 74-91.
- **`app/services/price_quarantine.py:430-438`**, `_corroboration` — turnover is an
  **admit-back signal and a confidence input, NEVER the gate**.
- **`app/services/strategy_registry.py:65-86,119-141`** — `INPUT_RULE_SETS` exists because
  *"a change to how the SMA is COMPUTED produces different signals — under an unchanged
  `strategy_version`"*. #3031 added `price_quarantine` as the fourth entry precisely
  because `price_masked_bars` binds its version into the engine's loader.
- **`app/services/market_data.py:645-651`** (#2572) — ingest **already** filters provider
  bars: a forming candle past the declared completed-session boundary is kept *"in its
  bounded raw audit store"* but never published to `price_daily`.
- **`app/services/market_calendar.py`** — a real NYSE trading calendar: full closures and
  13:00 ET early closes, composed from `pandas.tseries.holiday`, **source-ruled to NYSE's
  published holidays and early-closings** and verified against the 2025 and 2026 published
  calendars.

## The measurement

`scripts/verify_3046_weekend_bar_census.py`, `weekend-census-v2`, full corpus (7,002,478
bars, frontier 2026-09-14), read-only in one `REPEATABLE READ READ ONLY` transaction.

**10,756** weekend bars across **2,455** instrument rows, on the classes `params_for`
declares 5-day (`calendar_days_per_bar != 1`). `crypto` (66,188) and `fx` (149) are
excluded because the rule set declares them 7-day.

### It is three populations, not one

| shape | bars | instruments | with positive volume on a **strictly earlier** date |
| --- | ---: | ---: | ---: |
| flat `o=h=l=c`, volume NULL | **7,278** | 2,257 | 749 |
| shaped OHLC, volume NULL | **3,376** | 279 | 310 |
| shaped OHLC, volume **positive** | **102** | 11 | 102 |

### And two positions, not one

| position | bars | instruments |
| --- | ---: | ---: |
| leading — before the instrument's first weekday bar | 3,346 | 1,747 |
| interspersed — at or after it | **7,410** | 742 |

⚠ Residual 1 characterised the leading kind via its 103-name Nordic cluster. Leading bars
are **31%** of the population. ⚠ To be precise about what that does and does not say:
3,346 is *all* leading bars, not the Nordic cluster's share — establishing that would need
an intersection on the cluster's own bar keys, which is not done here. And residual 1's
provenance classifier was never confined to leading bars; it covers the whole T2 class.

## ⛔ The existing rule set does not reach these bars

| surface | reaches |
| --- | ---: |
| a `price_bar_quarantine` row of any kind | **8** of 10,756 (0.1%) |
| endpoint of a quarantined transition (`cardinality(rules) > 0`) | **1,619** (15.1%) |
| **neither** | **9,137** |

The complement is a real set operation, not `total − max(…)`: the two surfaces are
independent and `max` would silently assume one contains the other.

**Why, as mechanism:** a single Saturday or Sunday bar between Friday and Monday makes
gaps of 1 and 2 days, far under `hole_days = 10`, so T2 cannot fire; and a flat
`o = h = l = c` bar with positive prices violates none of B1-B3 — it is positive, it is
contained, and it has no wick.

⚠ **Three qualifications on that mechanism, all of which draft 1 overstated:**

1. **A flat bar CAN violate a B-rule.** An all-zero OHLC trips `rule_b1`; a flat bar that
   reverts (Friday 100 → flat Saturday 10 → Monday 100) trips `rule_b4`. The claim is that
   flatness *per se* is not a violation, not that these bars are unreachable in principle.
2. **A flat bar does NOT imply a zero return.** `_compute_volatility_30d`
   (`market_data.py:1325-1331`) takes successive **closes**, so a flat bar at 110 after a
   prior close of 100 contributes +10%. What a flat bar lacks is an intraday *range*, not
   a return.
3. **The Friday–weekend–Monday shape is the mechanism, not a measured property of every
   bar.** The census does not establish adjacency or per-bar gap lengths.

⚠⚠ **This kills the convenient answer.** The attractive conclusion was that residual 5's
clause-3 adoption subsumes this residual — a consumer honouring W1 would refuse any window
spanning these bars. At 15.1% transition reach it does not. ⚠ And even the 1,619 is an
upper bound on protection: endpoint membership is not W1 rejection (a bar can be a
window's first operand and escape `(start, end]`), and a `price_bar_quarantine` row can be
provisional-only, which condemns nothing.

## ⛔ What a blanket weekday cut would destroy, named

**102 weekend bars carry positive volume; 98 sit on `.24-7` symbols** — `META.24-7`,
`AMZN.24-7`, `AAPL.24-7`, `MSFT.24-7`, `NVDA.24-7`, `GOOG.24-7`, `SPCX.24-7`,
`TSLA.24-7`, `OIL.24-7`. eToro's weekend-tradable products, typed `us_equity` /
`commodity` by `exchanges.asset_class`, so the rule set hands them a **5-day** parameter
set. A rule keyed on "weekend + 5-day class" deletes them. That is residual 1's "large
uncharacterised blast radius", named and counted.

⚠ Positive volume on a weekend and a `.24-7` suffix do not *independently verify* that
those specific sessions traded; they are strong joint evidence and are reported as such.
`rule_w2`'s docstring mentions `SP.24-7` only as a sparse-window example. The other 4
(`GasOil.FUT` ×2, `IronOre` ×2, both Saturday) are **undetermined** and not claimed.

⚠ The cohort table groups by symbol, and symbols are **not unique** across instruments, so
it is a naming aid rather than an instrument count. ⚠ NULL-volume `.24-7` bars are not
enumerated — the cohort a weekend rule might wrongly reject is therefore only partly
sized.

### There is no structured 24/7 field, and the check was run

Per the classifier rule — check for a structured field first and record that you checked:
`instruments` has no session or calendar column; the 18 `.24-7` rows span 5 `exchange`
values and 4 `instrument_type_id` values and share both with ordinary equities
(`exchange = '4'`, `instrument_type_id = 5` holds 2 of 3,617); `exchanges.capabilities` is
a **data**-coverage map, not a trading one. So the suffix is reported as an observed
cohort and never used as a rule. ⚠ The rule requires the structured check first; it does
not categorically forbid a documented text classifier, and draft 1 overstated it as a
prohibition.

## ⛔⛔ The correction that matters most: we are not calendar-less

Draft 1 asserted, on #2312, that no venue calendar exists. **`app/services/market_calendar.py`
exists** — a real NYSE trading calendar with full closures and early closes, source-ruled
to NYSE's published holidays and verified against the 2025 and 2026 calendars.

#2312 is about `exchanges` having no timezone or session-hours **column**, which is true.
It is not the claim that the repo has no calendar. **`us_equity` is the single largest
affected class (3,769 of 10,756 bars), and for it a published-calendar answer already
exists in the tree** — which is a strictly stronger basis than any heuristic.

⚠ This is the *"absent from our model is not absent from the source"* class, on a new
axis: absent from the **schema** is not absent from the **codebase**. Draft 1 inherited a
ticket reference and did not grep for a module.

## Why draft 1's volume discriminator is withdrawn

It proposed: *a weekend bar with no volume, on an instrument that records volume
elsewhere, is a bar with no evidence a session occurred.* Four independent objections,
each sufficient on its own:

1. ⚠⚠ **Look-ahead.** "Records volume elsewhere" was a **lifetime** property, so one
   positive bar in 2026 changed how a 2022 weekend bar read — on a corpus that feeds
   backtests. It also counted the measured bar, making the column trivially true for every
   positive-volume row.
2. ⚠⚠ **It is an order of magnitude smaller than measured.** Re-run date-locally
   (positive volume on a **strictly earlier** date), the covered population falls from
   4,318 + 2,928 = 7,246 to **749 + 310 = 1,059** — about **10%** of the 10,654
   volume-NULL weekend bars, not 68%.
3. ⚠⚠ **It contradicts a documented source rule.** `_corroboration`
   (`price_quarantine.py:430-438`) states that turnover is an admit-back signal and a
   confidence input, **never the gate**. The discriminator made volume the gate.
4. ⚠ **NULL is not one thing.** It combines missing, never-reported and malformed volume,
   and the column is `NUMERIC`. A lifetime-coverage test cannot recover the distinction.

The column survives in the census as **descriptive context**, date-local and labelled as
such.

## Why the write path is not obviously right either — but the argument is narrower than draft 1's

Draft 1 argued that refusing at ingest discards vendor data, which `sql/247:4-7` forbids.
**That is too strong**, and #2572 is the counter-example already in the tree: ingest
*does* filter — a forming candle past the completed-session boundary is dropped from
`price_daily` — and the reason it is admissible is that the provider response is retained
*"in its bounded raw audit store"*. So a weekend filter is not categorically forbidden; it
would have to take that same shape.

What survives is the provenance finding, and it is narrower than draft 1 claimed:
`etoro.py:567::_normalise_candle` maps `fromDate`/`open`/`high`/`low`/`close`/`volume` and
returns `None` on a missing **or malformed** field; `_normalise_candles` flattens;
`_upsert_candles` writes what it is handed. **There is no gap-fill, carry-forward,
forward-fill or synthesis anywhere in the daily-candle path** — grepped, not assumed.

⚠ That is a statement about the **code path as it stands**, not a per-row payload
reconciliation of historical bars, and not a claim about what eToro's candle endpoint
returns today. Residual 1's own probe was one symbol (NCSM), and this is a different kind
of evidence, not more of the same kind.

## The constraints any fix must satisfy

Offered instead of a recommendation, because the measurement supports these and does not
yet support a choice among them.

1. ⚠⚠ **It may not dodge strategy identity by construction.** `strategy_registry.py:65-86`
   is explicit that a rule changing what the engine's loader returns must move identity;
   #3031 added `price_quarantine` for exactly this reason. Putting a session verdict in a
   new module *specifically so that `INPUT_RULE_SETS` does not rotate* is the defect that
   mechanism exists to close — draft 1 proposed it and was wrong to.
2. **It must be derived, not in-place** (`sql/247:4-7`), or take #2572's shape: filter at
   publish, retain the raw payload.
3. **It must not use volume as a gate** (`_corroboration`).
4. **It must not be keyed on a symbol suffix**, and it must not delete the `.24-7` cohort.
5. **It should prefer the published calendar where one exists.** `market_calendar.py`
   covers NYSE — the largest affected class — and a published rule beats a heuristic.
6. **It must state its treatment**: mask fields, remove the date, or refuse the window.
   These are different — `price_masked_bars.py:14-20` records that filtering *"silently
   shortens every warm-up window"*, while masking preserves the bar slot.
7. **It must say what happens to already-stored derived columns.** Their window was
   evaluated by `market_data._compute_and_store_features`, so adopting a loader repairs
   nothing already written (settled-decisions, residual 5 clause list).
8. **It needs its own versioning, coverage and staleness contract.** "Keyed like the other
   derived tables" does not establish replay behaviour after a bar revision.

## What this proposal does NOT do

- **Builds nothing**; the census writes no table and opens a `READ ONLY` transaction.
- **Does not edit `price_quarantine`**, so no identity rotation and no substrate recompute.
- **Does not change ingest.**
- **Does not decide the FX question.** `params_for` declares `fx` 7-day, so its 149
  weekend bars are excluded from every figure — though real FX venues close Friday to
  Sunday evening. That is a rule-module question and is reported, not silently corrected.
- **Does not quantify the downstream error.** How much an extra bar shifts `sma_200`, or a
  weekend close moves `volatility_30d`, is a separate measurement and is not asserted.
- **Does not establish carry-forward.** OHLC equality is not a comparison against the
  prior close, and this census does not run residual 1's provenance classifier.

## Security

No security surface. Read-path data quality; the script writes nothing.

Refs #3046. Refs #3031. Refs #2312. Refs #2572. Refs #2797. Refs #2261.
