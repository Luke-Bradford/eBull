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


---

# Addendum — sizing the GENUINE T2 residual (#3046 residual 1)

Status: spec for the `T2 LEVEL PROVENANCE` section added to the same script. Read-only,
writes nothing, proposes no rule change — `price_quarantine` stays in `INPUT_RULE_SETS`
(#3031), so its rules are not touched here.

Revision 2. **Revision 1 is recorded below under "What Codex checkpoint 1 killed", because
three of its errors would each have produced a smaller, wronger answer.**

## The question

The close-out of the parent section left residual 1 open in these words: *"236 of 244
unadjudicable. Subtracting the weekend-sentinel cluster leaves a much smaller genuine
population; sizing it needs the cluster excluded first."*

The count that section printed is what the ticket, the board and the next session will
quote. A T2 transition is `close(price_date) / close(prior_date)` across a calendar hole,
and `prior_date` is by construction the **immediately preceding stored bar**. It is a claim
about a level shift between two observed prices only if both closes were observed. Where
the prior close is a carried-forward repeat of a bar that was itself never observed, the
ratio's operand is a number the feed emitted and the market never set — and
`sources_disagree` on it adjudicates a bar's provenance, not a level.

So the residual is not "the count minus one Nordic cluster". It is the whole T2 class
stratified by the **provenance of each endpoint's level**, which subsumes that cluster
without naming a date, a venue or a constant.

## Source rule

No published formulation exists for "is this stored bar an observation", so the rule is
fixed **by construction** over columns and verdicts we already own, and every clause below
cites the thing that fixes it rather than reasoning it out.

- **Volume must be POSITIVE, not merely non-NULL.** `price_quarantine._usable_volume`
  (`app/services/price_quarantine.py:302`) is `volume is None or volume <= 0 -> None`.
  Using `IS NOT NULL` would admit a zero as evidence of trading against our own rule set.
- **A stored NULL volume does not mean "the feed omitted the field".** eToro's normaliser
  `_int_or_none` (`app/providers/implementations/etoro.py:861`) returns `None` for a
  **zero** as well as for a missing or malformed value, so NULL and zero are already
  indistinguishable at rest. That is why the operand below never reads NULL as a positive
  claim about anything — only positive volume is evidence.
- **Range and return are separate verdict axes and must not be crossed.**
  `sql/247_price_quarantine.sql:10-17`: B2 (containment) and B3 (phantom wick) set
  `range_usable = false` and leave `return_usable` true, because "one verdict class = one
  column". So `high > low` on a bar carrying B2/B3 is a **known-bad range** and is not
  evidence that the session traded. The classifier consults
  `price_bar_quarantine.range_usable` before using range at all.
- **Our bars are Bid quotes, not trade prints** (`.claude/skills/data-sources/etoro-api.md`,
  the −0.15%..−0.22% level-bias table corroborating S3 / #2243). The classifier therefore
  says "the feed reported a distinct quote", never "somebody printed a trade".
- **eToro carries the last close forward** after a name stops quoting: a zero-range,
  volume-NULL bar repeating the previous stored close. Measured on #3046 on 2026-09-14
  against a live `OneDay` candle probe (`NCSM`). ⚠ That same session **falsified** the
  same predicate as an *ingest-refusal* operand — its weekday population is large and
  uncharacterised. Nothing here refuses, deletes or re-classifies a bar; the operand
  describes a stored ratio's operands, not a bar's right to exist.

## The classification

**Step 1 — is the endpoint bar observed?**

```
observed(bar) := (volume > 0)
              OR (range_usable IS NOT false AND high > low)
```

**Step 2 — if not observed, where did its level come from?** Walk backwards through the
instrument's **complete** stored history (never through a filtered or joined subset) while
each bar's close exactly equals its predecessor's:

| provenance | reached | meaning |
| --- | --- | --- |
| `observed` | — | the endpoint itself carries positive volume or a usable range |
| `stale_observed_level` | an observed bar inside the repeat run | the LEVEL is real; its DATE is not. The true span is longer than the transition claims |
| `fabricated_level` | the start of the series with no observed bar in the run — including a lone unobserved FIRST bar, which has nothing behind it at all | the operand is a level the market never set |
| `zero_range_new_level` | the close differs from its predecessor's | degenerate but new — a one-quote session on a thin name has this shape and so does a placeholder; **not decided** |
| `absent` | no `price_daily` row | cannot occur for a stored transition endpoint; **counted and printed**, never assumed away |

⚠ `stale_observed_level` exists **because revision 1 did not have it**, and that is the
correction that mattered most: revision 1 folded every carry-forward into "not a level
claim", which silently exonerated `observed 100 -> carried 100 -> hole -> observed 10`.
The 10x there can be a real scale error; only the DATE of the earlier operand is wrong.
The class is small on today's corpus and the script prints its size — which is the point,
because revision 1 would have made it invisible.

**Step 3 — tier, by precedence**, from the two endpoint provenances:

- **A — fabricated prior level.** Either endpoint is `fabricated_level` or `absent`.
- **B — stale level.** Not A, and either endpoint is `stale_observed_level`.
- **C — degenerate endpoint.** Not A or B, and either endpoint is `zero_range_new_level`.
- **D — both endpoints observed.** The genuine residual.

A, B and C are each **unresolved, not safe**. Only D is a claim about two observed levels,
and even D is a claim about the transition's *operands*, never about which consumer reads
it — that is the separate exposure question this ticket's step 1 owns.

## Structural invariant worth asserting, not assuming

`prior_date` is the immediately preceding stored bar, so the resume endpoint's predecessor
IS the prior endpoint. A resume endpoint classified `stale_observed_level` or
`fabricated_level` therefore requires `close(price_date) = close(prior_date)`, i.e. a ratio
of exactly 1 — which cannot clear any magnitude threshold and cannot be in the blind spot.
The script asserts zero such rows. A hit means the stored transition and `price_daily` have
drifted apart, which is a finding in its own right.

## Reconciliation — the existing arms plus two

1-3 unchanged (unresolved-break scoping, coverage rule-set version, `REPEATABLE READ`).

4. **Partition arm.** The tier counts must sum to the T2 transition count, every endpoint
   must classify, and no transition may appear in two tiers.
5. **Stored-ratio arm.** `REPEATABLE READ` gives one snapshot; it does not give agreement
   between a quarantine row computed at some earlier refresh and the `price_daily` this
   classifier reads now. For every T2 transition, recompute
   `_usable_close(price_date) / _usable_close(prior_date)` and compare against the stored
   `observed_ratio` within the column's stored precision. A mismatch means the classified
   bars are not the bars that minted the transition, and the residual counts are void.

## Confounds — reported, not assumed away

- ⚠ **Volume coverage is era-local, not lifetime.** One populated bar years later would
  turn a "never" instrument into a "partial" one and change nothing about the endpoint.
  Each endpoint therefore reports whether any bar **at or before its own date** carries
  positive volume; where none does, the volume half of `observed` was unavailable on that
  date and the classification rests on range alone. Printed per tier, so a tier that is an
  artefact of blanket volume absence is visible as one.
- ⚠ **A halted session is indistinguishable from carry-forward** on a bar with no positive
  volume whose close repeats. In both cases no new level was set, which is the property the
  tier is about — but it is a limit, not a proof.
- ⚠ **Tier D is not "confirmed defects".** It is the population whose `sources_disagree`
  rows mean what the parent section says they mean; it still contains archive-unreachable
  rows, and its disagreements still carry the parent section's adjustment-basis caveat (a
  disagreement against an `unadjusted` archive can be a split we healed).
- ⚠ **Exchange test issues are not excluded here.** `research-price-corpus.md` requires
  intersecting vendor symbols against the Nasdaq directory `Test Issue = Y` flag before
  treating an extreme return as a market observation. That exclusion belongs to the archive
  corpus, not to this stratification, and is named as an outstanding qualifier on the
  archive verdicts rather than silently inherited.

## What Codex checkpoint 1 killed in revision 1

1. **`volume IS NOT NULL` as evidence of trading** — contradicted
   `_usable_volume`'s own `<= 0` rejection, and eToro stores a zero AS NULL.
2. **`high > low` without consulting `range_usable`** — would have promoted a known B2/B3
   phantom wick to "observed", crossing the two verdict axes `sql/247` separates by design.
3. **Carry-forward treated as exonerating** — see `stale_observed_level` above.

Each of the three shrinks the "genuine" residual, which is the direction that reads as a
result. That is why they are recorded here rather than only in the commit.

## Output

Every figure is computed at run time; none is written into this spec or into the script.
Tier D is printed in full — every row with its magnitude, threshold, asset class, endpoint
provenances and archive verdict, no top-N cut. A, B and C print their own per-verdict and
per-provenance breakdowns plus a sample-free count, so the population being set aside is
auditable rather than merely subtracted.

A `STRATIFIER_VERSION` constant is printed beside `RULE_SET_VERSION`: the quarantine
version cannot move when only this classification changes, so without it two runs reporting
different residuals would be indistinguishable from a corpus change.
