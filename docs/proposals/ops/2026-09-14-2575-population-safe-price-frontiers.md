# #2575 — the last three global `MAX(price_date)` consumers

Status: v3, partially shipped. Issue: #2575. Refs #2572, #2624.

**Of the ticket's three consumers, two turn out to be already closed at the producer and
one is real and is fixed here.** Recorded in full because each conclusion took a falsified
premise to reach.

⚠ Two Codex checkpoint-1 passes, 36 and 40 findings, reshaped this twice:

- **v1** proposed a shared modal-coverage resolver with a completeness floor. Killed by a
  SOURCE-RULE gap — the calendar rule already exists in this repo, twice.
- **v2** proposed applying that calendar rule to `resolve_batch_as_of_date` and
  `scoring_is_fresh`. Killed by `2d1b4e8d`, which **already applies it at ingest**, so
  neither consumer can be handed a forming date. Both sections below are kept because the
  reasoning is what makes the third fix's rule defensible.

## Source rule — it is already written down, in the spec AND in code

`docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md` §4.6, verbatim:

> The whole batch computes at ONE **market-calendar** as-of DATE (**the newest closed
> session**, data-anchored — NOT `now()::date`; off-by-one/tz safe).

And `app/services/market_calendar.py:225` implements exactly that:

> `latest_completed_us_session(now)` — *"Return the latest NYSE session whose official
> close has passed … observes both full closures and 13:00 ET half-day closes. This is
> distinct from 'latest weekday': at 03:00 UTC on a Tuesday, Monday is the latest completed
> session while Tuesday has not opened."*

**The implementation carries the docstring and not the rule.** `resolve_batch_as_of_date`
(`fair_value_band.py:848`) says *"Newest closed market session"* and then executes
`SELECT max(price_date) FROM price_daily WHERE close IS NOT NULL AND close > 0`. There is
no calendar term anywhere in it. §4.6's "data-anchored" — which means *not* `now()::date` —
was implemented as "the data maximum", dropping the "market-calendar / newest closed
session" half of the same sentence.

**So this ticket needs no new rule and no new constant.** ⚠ And it turns out not to need
the resolver changed either — see "Bullets 1 and 2 are ALREADY CLOSED" below, where the
same rule is shown to be applied one layer upstream, at the writer. The section is kept
because the `prices` fix rests on exactly this rule.

## Premise check — one of the ticket's three claims is FALSIFIED, and the real defect is sharper

The ticket says `resolve_batch_as_of_date` is *"unsafe; one forming/partial newest date can
materialise a thin or empty whole-universe cohort."*

**Cohort thinness is not the mechanism.** Every price read in `fair_value_band.py` is a
carry-forward — `WHERE pd.price_date <= %(as_of)s … ORDER BY pd.price_date DESC` at `:889`,
`:1371`, `:1409`, with no `= as_of` predicate anywhere (12 `price_date` hits, all
accounted for). An instrument with no bar on the anchor inherits its previous close, so a
sparse anchor gives a **full cohort of mixed vintage**, not a thin one.

⚠ Narrowing my own first draft, which over-claimed *"structurally impossible"*: pass 2 can
still thin a cohort, because `peer_pct_for` walks SIC-4→3→2 to the first prefix with
`MIN_PEERS` **FRESH** eligible members (`:1104`). Moving the anchor changes which peers are
fresh. So anchor choice does reach cohort width — by a different route than the ticket
names, and one that makes the A/B below mandatory rather than a formality.

### What actually went wrong, measured on the full stored population

**1 of 23 stored `fair_value_cohort_members` as-of dates sits on a NYSE-closed date:
`2026-08-08`, a Saturday** (`us_market_status` → `closed`, `us_market_reason` → `Weekend`).
That is what `max(price_date)` does when crypto publishes over a weekend and equities do
not: **11 of the newest 40 distinct `price_date` values are non-open NYSE dates** — every
weekend plus `2026-09-07`, Labor Day.

⚠⚠ **This instance is PRE-FIX and the section below dates it.** `2026-08-08` precedes
`2d1b4e8d` (2026-08-12) by four days. The mechanism is real and is already closed; nothing
here should be read as a live defect in the fair-value anchor.

`fair_value_band_current` shows 0 of 10 as-of dates non-open, so no *current* band carries
a closed-session anchor today.

### The second, unnamed half: no coverage denominator is published

`resolve_batch_as_of_date` returns a bare date. Modal `close_date` share across the stored
cohorts ranges **19.39% → 98.19%** (23 as-of dates; 13 below 95%), and every one of those
batches is indistinguishable to a reader. The July figures are a corpus still being
backfilled, not an orphaned-sweep artefact — recomputing the anchor would not have made
them single-vintage — but the absence of the denominator is why nobody could tell.

## The watermark #2572 asked for EXISTS, and cannot serve these consumers

`daily_candle_refresh` writes the aggregate checkpoint under `contract_version =
'candle-population-watermark-v1'` (`app/workers/scheduler.py:3345-3407`) and
`processes/watermarks.py::_resolve_candle_offset` consumes it. Routing at it is the obvious
build and is wrong, for two measured reasons:

- **`population_status` is `'partial'` on 100 of 100 stored runs.** The writer is `"partial"
  if summary.candles_failed or unavailable else "complete"` (`:3407`) and something always
  fails or is unavailable. A consumer gating on `'complete'` never advances — the #2624
  "control that cannot fire" shape.
- **The denominator is the RUN's scope, not the population.** Stored `candidates_seen`
  ranges **1,539 → 12,222** on the same corpus, because hourly incremental runs and full
  sweeps declare different candidate sets. `1591/1599` for 2026-09-11 licenses nothing
  about the other ~9,400 instruments.

## ⛔ Bullets 1 and 2 are ALREADY CLOSED — at the producer, four days after #2572 was filed

Checkpoint 1 pointed at `app/workers/scheduler.py:3281` and it settles both. `git log -S`
dates the whole mechanism to one commit:

```
2d1b4e8d  2026-08-12  Fix candle completed-session population watermark (#2572) (#2573)
```

That commit introduced `latest_completed_us_session` AND wired it into ingest.
`market_data.py:644`:

```python
if fresh_through is not None:
    # never publish that forming value as a daily close in price_daily (#2572)
    bars = [bar for bar in bars if bar.price_date <= fresh_through]
```

with `fresh_through = latest_completed_us_session(datetime.now(UTC))`
(`scheduler.py:3281`). **A forming or future bar cannot reach `price_daily` at all**, so
`max(price_date)` cannot exceed the completed session, and neither
`resolve_batch_as_of_date` nor `scoring_is_fresh` can be handed one.

⚠ **And my own evidence was mis-dated.** The `2026-08-08` Saturday anchor — the one
concrete instance of the defect in the whole stored population — is **four days before
`2d1b4e8d`**. It is a pre-fix artefact, not a live defect, and presenting it as current
would have been the premise error this ticket's own history is full of. Every one of the
other 22 stored `fair_value_cohort_members` as-of dates is an open NYSE session.

### What is NOT closed, and why it is not built here

The residual is the case #2572 actually measured: a **legitimate completed session that
only 12.66% of the cohort reached**. The producer clamp bounds the future; it says nothing
about completeness. Anchoring on it is still "one instrument decides the label for 7,500".

Closing that needs a **completeness floor**, and there is no source rule for one:

- The candle watermark cannot supply it — `population_status` is `'partial'` on 100 of 100
  stored runs, and `candidates_seen` ranges **1,539 → 12,222** on the same corpus because
  hourly runs and full sweeps declare different scopes. Gating on `'complete'` is a control
  that cannot fire (#2624's shape).
- A modal-share floor is an invented constant. v1 of this spec proposed reusing
  `strategy_signal_scan`'s `FRONTIER_MODAL_SHARE_FLOOR = (2, 3)`; checkpoint 1 showed the
  two populations differ (the scan's loadable validated-US universe vs fair-value's
  tradable targets vs cohort members' complete-TTM subset), so sharing the arithmetic does
  not share the policy, and no census establishes the floor is safe over fair-value
  targets.
- A NYSE calendar cannot supply it either: the corpus holds **4,697 non-US-venue
  instruments** (#3028's eu/uk/asia/mena classes, 34.9% of `price_daily`) and `exchanges`
  carries **no per-venue calendar** (#2312). A US-session completeness rule would freeze
  them.

**Recommendation, recorded rather than built:** the honest v1 deliverable is to *publish
the denominator*, not to move the anchor — `resolve_batch_as_of_date` returns a bare date
today, and the stored modal `close_date` share across the 23 as-of dates ranges **19.39% →
98.19%** with no reader able to tell them apart. That is a stored field and a reader
contract, which is a different ticket from a frontier fix.

## The change that IS built: the `prices` layer

`_LAYER_QUERIES["prices"]` is `SELECT MAX(price_date)::timestamptz` — midnight of the last
trading date — aged against a **4-hour** threshold (`ops_monitor.py:125, :177`).

Reproduced live on dev at 2026-09-14 12:5x UTC with the price pipeline healthy
(`daily_candle_refresh` succeeded 7 times that day):

```
prices   stale   latest=2026-09-11 00:00:00+00:00   age=3 days, 13:13:27   max=4:00:00
```

⚠⚠ `_derive_overall_status` (`app/api/system.py`) maps *any* stale layer to `degraded`, so
this single verdict held `/system/status` off `ok` permanently.
`docs/review-prevention-log.md` records the same measurement from 2026-08-13 (#2624 scope
3) as a reason **not to reuse the signal**; the signal itself was never fixed.

⚠ Two wrong answers were tried first and both are recorded because each sounded right:

- **Age `daily_candle_refresh`'s job rows instead.** The job has no scheduled entry and the
  orchestrator declares `Cadence(interval=timedelta(hours=24))` for the `candles` layer
  (`sync_orchestrator/registry.py:131`), so a 4-hour SLA is unsatisfiable whichever column
  it ages. Today's seven runs are an orchestrator artefact, not a declared cadence.
- **"Session lag is self-suppressing by construction."** Backwards, and checkpoint 1 was
  right: the expected session advances with wall time whether or not prices do, so the lag
  *does* grow. The prevention-log sentence I cited is about scan lag measured *relative to
  available prices*, which is a different quantity. The grace below is what actually
  prevents the false alarm, not self-suppression.

### The rule

**`prices` is fresh when `MAX(price_date) >= latest_completed_us_session(now - grace)`**,
where `grace` is `LAYERS["candles"].cadence.interval` — read from the orchestrator, never
re-declared. No new constant: the NYSE calendar and the candle cadence are both already
declared, and the rule is their composition.

- It alerts about one cadence after the pipeline stops.
- It has **no false alarm at the closing bell**, because the just-closed session is not yet
  required — the failure mode a naive session comparison would introduce.
- Holidays and 13:00 ET half-days come from `market_calendar`, so a weekday-arithmetic bug
  is not available.

`max_age` is reported as `None` on both branches rather than as a number the verdict did
not use: an `ok` beside `age > max_age` on the admin panel reads as a bug.
`LayerHealthResponse.max_age_seconds` is already `float | None`, and no frontend component
renders it.

⚠ `_STALENESS_THRESHOLDS` no longer contains `prices`, so the lookup becomes `.get` and an
explicit `RuntimeError` covers a layer in neither set. A `RuntimeError` and not an
`assert`, which `python -O` deletes.

## Tests

`tests/test_2575_prices_layer_session_anchor.py`, 15 cases, every date a REAL calendar date
so an assertion fails if the holiday rules move:

1. The partition — every layer has **exactly one** freshness rule. This is the guard behind
   the `RuntimeError` branch, and it catches both "in neither set" and "in both".
2. `prices` has no wall-clock threshold, and the layer query still reads `MAX(price_date)`
   — pinned, because the session comparison is exact only for a DATE cast to midnight.
3. The grace equals `LAYERS["candles"].cadence.interval`, read from the registry.
4. Monday morning with Friday's bars is `ok` — the live measurement, inverted.
5. A session behind is `stale`; a week behind is `stale` (the alert still works).
6. No false alarm five minutes after the closing bell.
7. A holiday is never demanded: the Tuesday after Labor Day requires the preceding Friday.
8. The grace is **subtracted, not added** — a sign error would demand a session that has
   not closed and invert the defect. Pinned against the ungraced session at an instant
   where the two differ.
9. `max_age` is `None` on both verdicts; the real `age` is still reported.
10. An empty `price_daily` stays `empty`, not `stale` — `BootstrapProgress` keys on it.
11. A naive bar timestamp is read as UTC.
12. A naive `now` raises rather than being guessed a timezone.
13. An unpartitioned layer raises `RuntimeError` rather than defaulting.

**Revert-probed, not merely observed green** — three probes, each caught:
dropping the grace subtraction (3 failures), `held <= required` (4), and removing `prices`
from the session set (14).

## Out of scope, with reasons

- **No new modal/coverage resolver and no new floor constant.** v1 proposed both; the
  calendar is the source rule and a mode measures agreement, not closure. This also removes
  the v1 plan to relocate `modal_bar_date` / `FRONTIER_MODAL_SHARE_FLOOR` out of
  `strategy_signal_scan`, which checkpoint 1 showed had at least three unlisted importers
  and a caller (`app/api/strategies.py:1640`'s `_corpus_frontier`) with a deliberately
  *different* contract — it returns the mode even below the scan floor.
- **Changing `resolve_batch_as_of_date` and `scoring_is_fresh` at all.** Both are already
  protected by the producer clamp; changing them would alter stored valuation output for no
  defect. See the bullets-1-and-2 section.
- **Publishing the coverage denominator.** Real (the 19.39% batch) and the honest remaining
  deliverable, but it is a new stored field and a reader contract, not a frontier fix.
  Named on the issue, not built here.
- **Repairing the 13 historical mixed-vintage cohorts** and the one 2026-08-08 batch.
  Backfill artefacts; nothing here recomputes them.
- **`strategy_scan_freshness._RECENT_TRADING_DATES`**, which also starts from a global
  maximum. It enumerates a calendar rather than anchoring a computation, and the module
  docstring already records why it does not use the `prices` layer. Listed so the inventory
  is complete.
- The remaining `MAX(price_date)` sites, each already per-instrument or already routed:
  `market_data.py:146`, `thesis_outcomes.py` (maturity; the thesis engine is off the product
  path), `processes/watermarks.py` (reads the declared checkpoint with a legacy fallback).
