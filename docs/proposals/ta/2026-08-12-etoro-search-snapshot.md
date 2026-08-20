# eToro broad snapshot — useful screening, incomplete evidence (#2523)

Date: 2026-08-12  
Status: implemented provider foundation; no strategy promoted

## Decision

Use eToro search as a cheap, ephemeral cross-sectional screening input, not as
a quote, a security master or an indicator warehouse. Fetch every reported
page, join exact provider IDs to a predeclared point-in-time cohort, measure the
known denominator and refuse breadth below the candidate's frozen coverage
threshold. Confirm every shortlisted candidate through timestamped bid/ask
rates and the normal execution gates.

The official [Search for Instruments](https://api-portal.etoro.com/api-reference/market-data/search-for-instruments)
contract exposes projected current rate, daily/weekly/monthly movement,
platform/trading state, industry/sector and eToro crowd-position fields. The
official endpoint page declares a shared 120-request/60-second market-data
pool. This materially improves on a 122-request full rates sweep, but does not
turn the fields into alpha.

## Authenticated demo findings

The following read-only checks used the configured demo account; credential
access was audited and no secrets or raw responses were retained.

| check | measured result |
|---|---:|
| projected page size requested/returned | 10,000 |
| total reported search rows | 12,187 |
| complete pages using live `page` parameter | 10,000 + 2,187 |
| adapter wall time | 8.493 seconds |
| normalised positive-ID rows | 12,186 |
| rows with current rate and daily change | 12,185 |
| current NYSE/Nasdaq common-stock IDs matched | 3,591 / 6,083 (59.03%) |
| with 20-session price/volume evidence | 3,537 / 3,994 (88.56%) |
| ≥$5 and ≥$25m recent dollar volume | 1,480 / 1,601 (92.44%) |
| ≥$5 and ≥$100m recent dollar volume | 637 / 669 (95.22%) |

The documented `pageNumber=2` returned reported page 1 again. The live
`page=2` parameter returned reported page 2. The adapter asserts response page,
stable total, full row count and unique positive IDs so this drift cannot
silently truncate the market.

The daily-change cross-section contained extreme values (full matched cohort
range -92.95% to +12,500%). Breadth uses only sign and treats below -100% as
invalid; it does not winsorise outcomes until they look convenient. Corporate
actions can still change a sign, so candidate research must reconcile its exact
cohort against known actions. The source has no per-instrument timestamp.

## Storage and runtime contract

- No new table or migration. The two-page snapshot lives for one scanner cycle.
- Routine instrument rows and not-fired evaluations are discarded.
- Retain only aggregate coverage/breadth for the evaluation and the existing
  compact immutable context of a genuinely fired/refused candidate.
- The provider records collection start/end because the rows do not share a
  source timestamp.
- Search `currentRate` never overwrites `quotes` and never authorises an order.
- A fresh timestamped, two-sided rate and broker preflight remain mandatory.

`strategy_market_snapshot.measure_daily_breadth` makes the coverage denominator,
threshold, sign rule, missing semantics and formula version explicit. The
caller supplies its frozen cohort and threshold; this shared layer does not
select a more favourable universe after seeing outcomes.

## What this does and does not unblock

This removes the quota objection to cheap broad screening and supplies a tested
prospective current-session breadth primitive for liquid declared cohorts. The
subsequent point-in-time-sector increment records the current eToro
stocks-industry assignment prospectively and refuses unknown assignments. It
is followed by a completed-session increment that freezes and fixture-tests
causal 1/3/5/10/20-session market/sector returns, percent-above-prior-trend,
cross-sectional dispersion and a precisely labelled common-variance share. It
also exposes a provider-session-calendar trap in the live corpus. #2523 is
still incomplete: a candidate-owned production loader, compact persistence,
exact event state and intraday coverage remain outstanding.

It also does not make S1–S4 viable. The corrected after-cost evidence remains
negative/weak, so the strategy catalogue must stay empty until a preregistered
candidate passes recent purged walk-forward, untouched and prospective gates.
