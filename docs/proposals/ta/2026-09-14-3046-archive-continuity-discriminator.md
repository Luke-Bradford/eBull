# #3046 — adjudicating the suppressed-transition blind spot against an independent archive

Status: spec for `scripts/verify_3046_archive_continuity.py`. Read-only. Writes nothing.

## The question this answers, and the one it does not

`scripts/verify_3046_break_minting_census.py` established the SIZE of the population
`price_segments` cannot see: transitions that clear their class magnitude threshold, carry
T1 and/or T2, and therefore never reach T3 and mint no `price_series_break` row
(`price_quarantine.py:486` — `if magnitude >= params.magnitude_threshold and not rules`).
Run it for the counts; none is repeated here, because a derived statistic written into
prose goes stale in the place a reader trusts most.

That census deliberately declines to say whether any of them is a real defect. Its own
header: *"whether a large move across a three-month hole is a scale change or a real move
is exactly the question the classifier declines to answer, and this script does not answer
it either."*

This script answers that question for the subset an **independent** source can reach, and
reports the rest as unadjudicable rather than as safe.

It does **not** propose a rule change. `price_quarantine` sits in `INPUT_RULE_SETS`
(#3031), so editing its rules rotates every strategy identity and empties the backtest
substrate. What a fix should be is downstream of knowing which half of the blind spot is
real.

## Source rule — what is compared, and why it is the discrepancy

The quantity tested is the **discrepancy**

```
d = our_stored_ratio / archive_ratio        (both across the SAME two dates)
verdict = max(d, 1/d) >= params_for(asset_class).magnitude_threshold
```

⚠⚠ **The first draft of this spec compared the two magnitudes — "does the archive ALSO
clear the threshold" — and Codex checkpoint 1 killed it on two counts, both fatal:**

1. `price_quarantine`'s own module header says **"Magnitude is a trigger, not a verdict."**
   Asking whether the archive trips the same trigger promotes that trigger to a verdict on
   the other side of the comparison, which is the thing the rule set refuses to do on ours.
2. **The threshold is calibrated on a same-scale move between ADJACENT bars.** A T2 pair
   spans a hole — months, sometimes years — so a legitimate cumulative return across it can
   clear 5× with nothing wrong anywhere. Applying a daily-move constant to a multi-year
   return compares two different quantities and calls the difference a finding.

Both failure modes are real on this corpus and both are pinned by tests:

- ours 6× / archive 4.9× — two series that moved within 22% of each other, reported as a
  finding purely because they landed on opposite sides of a daily-move trigger.
- ours 25× / archive 5× — reported as *corroborated*, i.e. a 5× scale error waved through
  on the grounds that something real also happened.

The discrepancy has neither problem. It is dimensionless, the market's own move divides
out (so there is no time dimension left to mis-calibrate), and what remains is exactly the
scale factor our series carries that the independent source does not. Direction needs no
separate clause — an archive that moved the same size the other way yields `ratio²`, not 1.

**No constant is invented.** The class threshold is applied to the quantity it actually
describes — a same-span level mismatch — instead of to one it does not.

### Which column, and why not `adj_close`

`close`. `sql/251`'s header settles this and it is not a preference: OHLC in the archive
carry the **split** adjustment and are mutually consistent, while `adj_close` is split
**and dividend** adjusted and is therefore a total-return series. Our `price_daily` close
is a price. Differencing a price series against a total-return one injects the dividend
stream into `d`, which across a months-long hole on a dividend payer is a real and wrong
drift.

### Exact dates only

The archive must carry a bar on both `prior_date` and `price_date`, from **one**
`series_id`. Nearest-date substitution is refused: our `prior_date` is the previous
*stored* bar, so a T2 pair spans a hole, and sliding the archive endpoint into that hole
computes a return over a different span than the one under test. Pairing endpoints from
two vendors would manufacture a return out of two adjustment bases.

### Provisional endpoints are refused, not adjudicated

`price_quarantine` defers a T3 verdict when either endpoint bar is provisional, because a
part-session bar is never verdict-bearing corroboration. Adjudicating one here would be
that rule broken through the side door, so `provisional_deferred` is decided **before** any
archive lookup — the refusal is a property of our bar and does not depend on what the
archive holds.

## Independence — one upstream, one observation

`research-price-corpus.md` and `sql/249`'s own comment fix this:

> two vendors that both resolve to `'yahoo'` are ONE observation, not two, and any
> cross-source agreement between them is circular.

So:

- `upstream_source = 'etoro'` series are **excluded**. They are our own feed; an agreement
  arm over them measures nothing. The exclusion is on `upstream_source`, never on `vendor`.
- Both usable vendors are `yahoo_derivative`. A transition both can reach yields **one**
  verdict, not two votes — and when the two disagree the row is reported as
  `vendor_disagreement`, never resolved by majority (there is no majority available at
  n=2 from one upstream).
- `adjustment_basis` is carried on every adjudicated row and never collapsed. ⚠ The
  asymmetry is load-bearing: our `price_daily` is **back-adjusted by the provider at fetch
  time** (`market_data.detect_adjustment_event`), so a disagreement against a
  `split_adjusted` archive points at our series, while one against an `unadjusted` archive
  can equally be the archive showing a split we healed.

## What it reports

| verdict | meaning |
| --- | --- |
| `sources_agree` | discrepancy under T — our series moved with the independent source |
| `sources_disagree` | discrepancy clears T — the shift is ours, not the market's |
| `vendor_disagreement` | two vendor series reach the pair and disagree |
| `provisional_deferred` | an endpoint bar is provisional |
| `archive_bar_unusable` | the series spans the pair and a close on it is non-positive |
| `archive_bar_missing` | the series spans the pair and lacks a bar on one or both dates |
| `pair_outside_span` | a series exists but does not span both dates |
| `no_archive_series` | no non-eToro series resolves to this instrument |

Split by **rule class** (`T1`, `T2`, `T1+T2`) and by asset class. The rule-class split is
the point of the whole exercise: T1 and T2 are different claims about the same suppression,
and pooling them is how the census's headline number reads as one population when it is
two.

- **T1** — an endpoint bar is `return_usable = false`. The classifier's own comment says
  *"the ratio across an unusable close is not a return regardless of its size"*, so
  `sources_disagree` here **corroborates the suppression**: the magnitude is an artefact of
  a bar the quarantine already condemned, and an archive showing no such move is what that
  looks like from outside.
- **T2** — the pair spans a hole and both endpoints are `return_usable`. Nothing masks it,
  nothing segments it, and `sources_disagree` here is a level shift no consumer is told
  about.
- **T1+T2** — reads as T1: an unusable endpoint makes the ratio not-a-return whatever the
  calendar did. The class is empty on today's corpus and the count is printed rather than
  assumed.

The same verdict therefore means opposite things in the two classes, which is exactly why
the script refuses to print a pooled figure.

⚠ A T1 verdict is about **break minting**, not about consumer exposure. The bar is still
stored, and `price_masked_bars` masks the close on the return axis only — B2/B3 range
defects reach `high`/`low` untouched. "Correctly suppressed" means "no `price_series_break`
is owed", never "nothing downstream is affected". Consumer exposure is the sibling
proposal's question (`2026-09-14-3046-raw-price-consumer-exposure.md`), deferred there and
not answered here.

## Acceptance

1. **Unresolved-break arm.** No blind-spot key may carry an *unresolved* `price_series_break`
   row; the definitional claim is that these mint nothing the segment model reads.
   ⚠ Scoped to `resolved_by IS NULL` deliberately — `price_quarantine_store.py:138` deletes
   only unresolved rows on refresh, so a resolved row legitimately survives a re-evaluation
   that no longer mints its transition, and keying against the whole table would cry
   mismatch on valid data. Resolved hits are printed, not failed.
2. **Version arm.** Zero `price_quarantine_coverage` rows at another `rule_set_version` —
   that table supplies the asset class that picks every threshold, so a stale row silently
   changes which transitions are blind.
3. `REPEATABLE READ`, read-only, one snapshot (`price_quarantine_refresh` rewrites all
   three quarantine tables on a 24h cadence).
4. Every printed figure is computed at run time, and every adjudicated row is printed with
   both ratios, the discrepancy, the threshold, the basis and the vendor — a verdict label
   alone cannot be reviewed. No top-N cut.

## Known limits, stated up front rather than discovered later

- **Coverage.** The archives are US-centric. Whatever fraction of the blind spot they
  cannot reach is **unadjudicated, not adjudicated-safe**; the unreachable buckets print
  beside the verdicts and never under them, and no conclusion about the whole population is
  drawn from the adjudicable subset.
- **Endpoints, not paths.** `d` compares two endpoints. An intermediate split that reverses,
  or two offsetting events inside the span, can cancel in both series and leave `d ≈ 1`.
  This measures whether the two sources end up in the same place, not that they took the
  same route.
- **Same date is not the same session.** Differing venue cutoffs, holiday calendars and
  carried stale closes all survive an exact-date match.
- **Resolution is symbol-level.** `research_price_series.instrument_id` links by symbol or
  CIK; ticker reuse, relisting and share-class changes are not re-verified here, and the
  corpus skill records that limitation.
