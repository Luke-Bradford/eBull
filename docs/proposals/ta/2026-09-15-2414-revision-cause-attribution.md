# #2414 — attribute a bar revision to the BRANCH that wrote it

Status: proposal. Scope: telemetry only. No schema change, no new I/O, no new gate, and
**no decision on #2414's fix.**

## What this does and does not claim

It records, per `daily_candle_refresh` run, which of the refresh loop's write branches
produced each bar revision. That is a **fact about control flow**, and it is all that is
claimed.

⚠⚠ **It does NOT answer #2414's gating question** — *which corpus revisions make a stored
verdict wrong.* A single fetch can rewrite bars for more than one underlying reason at
once, including inside an adjustment heal, so the branch is an upper bound on attribution
and not the economic cause. Stated here at the top because the first draft of this spec
claimed otherwise and Codex checkpoint 1 killed it. The counters narrow the population a
later investigation has to look at; they do not classify it.

## Why the existing axis is not enough

The previous pass shipped a revision **rate** and an **age** histogram, with its reason
written at `app/workers/scheduler.py:3473-3479`:

> The rate alone cannot choose the fix: an embargo is sufficient iff revisions never reach
> past the correction buffer, and a corpus stamp in the signal key is required if they do.

and at `app/services/market_data.py:1129-1131`:

> ⚠ MAGNITUDE is deliberately NOT captured. […] Age does [discriminate]: an embargo works
> only if revisions never reach past the correction buffer.

### The measurement, on the full stored population

`PYTHONPATH=. uv run python scripts/verify_2414_revision_causes.py --census`, which prints
its own window so nothing below is hand-maintained:

```
runs with a 'context' payload             : 106
runs carrying `bars_revised`              : 39
window (first -> last such run)           : 2026-08-23 14:44:24Z -> 2026-09-14 19:18:31Z
runs with >= 1 revision                   : 17
bars revised, total                       : 4080
runs ALSO carrying `bars_revised_age_days`: 18
bars revised inside that window           : 4
bars revised BEFORE the histogram shipped : 4076
deepest revision (calendar days)          : 275
age buckets (histogram window only)       : {'31_365': 4}
histogram conservation (sum == revised)   : 4 == 4
```

⚠ **The counter and the histogram do not disagree; they have different start dates.**
`bars_revised` shipped first, so 4,076 of the 4,080 revisions carry `bars_revised_age_days`
as **absent**, which the script counts separately from **present-and-empty**. Summing the
two totals reports a 1,000× contradiction that does not exist.

### What the age axis settles, and what it does not

**Settled, and it is one observation because that is all an existence claim needs: a
revision reached 275 calendar days back.** `_INCREMENTAL_FETCH_BARS` is 3. So the
correction-buffer-sized embargo the code names is not sufficient.

⚠ That is strictly weaker than "no embargo works". A 275-day observation refuses a 3-day
embargo; it does not bound the distribution, and no embargo length is derived from it.
It also does not establish that the revision **changed a verdict** — only that a value a
verdict could have read was overwritten.

**Not settled: age does not identify the branch.** Each of the three ordinary write paths
can produce a deep revision, so depth cannot be inverted to a cause:

- **Incremental is bounded in BARS, not in days.** `_candles_fetch_count`
  (`market_data.py:959-985`) returns `_INCREMENTAL_FETCH_BARS` when `gap_days <= 3` on the
  **newest** stored bar. The provider then returns that instrument's three most recent
  candles — whose *older* two are separated by the instrument's own trading cadence, not
  by the calendar. A thinly-traded or sparsely-ingested name whose newest bar is two days
  old can have a third-newest bar hundreds of days old, and revising it is an ordinary
  incremental write.
- **A heal can revise an arbitrarily small number of bars.** `detect_adjustment_event`
  needs one qualifying overlap mismatch to fire, and `_upsert_candles` counts only rows
  whose stored value actually differs (`IS DISTINCT FROM`). A short series, a partly
  re-based series or a provider response missing older dates all produce a heal with a
  handful of revisions.
- **A stale re-observation rewrites up to `lookback_days` of history** for any instrument
  more than three days behind. `_upsert_candles`'s own docstring measured the exposure on
  2026-08-23: 1,183 of 12,230 instruments (9.7%) were in that state, holding 544,799 bars.

⚠ **And a run aggregate cannot locate a revision anyway.** `bars_revised` and
`bars_revised_max_age_days` are summed across every instrument in the run, so a run
reporting two revisions and a 275-day maximum does not say that either revision was 275
days deep, nor that both came from the same instrument or the same branch. An earlier
draft of this spec reasoned from exactly that shape and was wrong to. **An aggregate
counter carries no per-entity identity, so its sample size for any per-entity claim is
zero, not `n`.**

## Source rule

The taxonomy is not invented. It is the enumeration of the branches `refresh_market_data`
already takes before it writes, each with its rationale recorded in the tree:

| cause | governing decision |
| --- | --- |
| `adjustment_heal` | #2066 split-cliff guard, `market_data.py:725-750` |
| `incremental` | #271 `_candles_fetch_count`, `market_data.py:959-985` — the 3-bar window |
| `stale_reobservation` | #271, same function — *"prior candles exist but the most recent is older than the incremental window"* |
| `initial_backfill` | #271, same function — *"no prior candles at all"* |
| `force_backfill` | operator-forced, `market_data.py:697-698` |

⚠ **No published provider rule for fetch-time back-adjustment has been located, and this
spec does not cite one.** `.claude/skills/data-sources/etoro-api.md` contains no
back-adjustment section (checked — it mentions adjustment only to warn that a cross-source
check must compare against RAW closes, at `:295-312`). The behaviour rests on #2066's
in-tree empirical detector and its threshold comment at `market_data.py:988-993`, which is
a repo decision, not a vendor contract. A first draft cited the skill for it; that citation
was wrong and is withdrawn. Nothing in this proposal depends on the provider's adjustment
policy — the recorded cause is *which branch ran*, which is observable regardless.

⚠ **`detect_adjustment_event` stays a TRIGGER, not a verdict.** Its 1.2 ratio decides
whether to heal; it is never consulted to decide whether a stored verdict stands. That is
the discriminator Codex killed twice on #3046 — *"magnitude is a trigger, not a verdict"* —
and it is killed here too.

## The change

### 1. `_candles_fetch_count` returns its REASON alongside the count

Today the caller re-derives the reason by comparing `fetch_count` against
`_INCREMENTAL_FETCH_BARS`. That inference is wrong in two ways and both are removed by
having the deciding function say what it decided:

- **It collapses when `lookback_days == 3`.** A stale-gap fallback returns 3, which the
  comparison then reads as `incremental`.
- **It races.** Re-deriving `had_prior_bars` from a second read (`_last_bar`, `:715`) can
  disagree with the read `_candles_fetch_count` already did, so the reported cause need not
  be the reason the fetch size was chosen.

New return: `(count, reason)` where `reason` is one of `initial_backfill`,
`stale_reobservation`, `incremental`.

### 2. `revision_cause()` — pure, table-testable, no DB

```python
def revision_cause(
    *, adjustment_detected: bool, force_backfill: bool, fetch_reason: FetchReason | None
) -> RevisionCause:
```

1. `adjustment_detected` → `adjustment_heal`
2. `force_backfill` → `force_backfill`
3. `fetch_reason is None` → `unknown`
4. otherwise → `fetch_reason`

⚠ **Both ends are `Literal`, not `str`** (review nitpick on PR #3080). These strings become
KEYS in `bars_revised_by_cause`, and an unrecognised key is indistinguishable on the admin
surface from a cause that genuinely did not fire — so a typo would be invisible rather than
loud. Typing them makes pyright the detector; probed by mistyping one constant, which is an
error at the assignment.

⚠ The forced path passes `None`, not `""`. An empty string is a value that can reach a
counter key; `None` is one the type checker forces every consumer to handle. `unknown` is
then a NAMED refusal rather than a guess: it is unreachable from the one call site today,
and a future caller that forgets the reason must surface in the census as an unattributed
revision instead of being folded into a real cause.

⚠ Order is load-bearing. A heal is reachable only from the incremental branch — its
precondition at `:733` is `not force_backfill and fetch_count == _INCREMENTAL_FETCH_BARS` —
so rule 3 would claim every heal if it ran first. Rule 2 sits above rule 3 because
`force_backfill` bypasses `_candles_fetch_count` entirely (`:697`), leaving no reason to
report. `adjustment_detected and force_backfill` is unreachable today by that same
precondition; the helper resolves it to `adjustment_heal` rather than raising, because a
telemetry helper that can abort a refresh is a worse failure than a mislabelled counter.

### 3. Carry it at the write site

All inputs are already local at `market_data.py:751-756`: `adjustment_detected` (`:706`,
`:750`), `force_backfill` (parameter), and the new `fetch_reason` from step 1. No query is
added. Two running totals accumulate under the existing #1293 rule — after the transaction
commits, in the same block as `candle_revision_age_days`:

- `candle_revisions_by_cause: dict[str, int]`
- `candle_revision_max_age_by_cause: dict[str, int]`

Both become fields on `MarketRefreshSummary` (`market_data.py:354`) with independent
`default_factory=dict`, and are returned at `:867` beside `adjustment_refetches`.

⚠ "After a clean commit" is the #1293 rule as it already exists and this change does not
strengthen it: `conn.transaction()` on a non-autocommit connection releases a savepoint
rather than committing, so these counters inherit exactly the same exposure as
`candle_rows_revised` does today. Not re-litigated here.

### 4. Persist and log them

Into the existing `context` mapping in `app/workers/scheduler.py` beside `bars_revised`:

- `bars_revised_by_cause`
- `bars_revised_max_age_days_by_cause`
- `adjustment_refetches` — already on the summary and **never persisted anywhere**; a grep
  returns its definition, its increment and one log line. Same writer-with-no-reader shape
  that `bars_revised` had before this ticket instrumented it.
  ⚠ Its exact meaning, stated because it is narrower than its name: **heals whose
  re-fetch returned at least one in-window bar AND whose per-instrument transaction then
  committed** (`:748-750`, `:779-780`). A detected adjustment whose re-fetch came back
  empty, or whose write later raised, counts zero.

Bounded: five causes, so at most 5 + 5 + 1 keys. `context` is documented as *"Optional
bounded provenance for aggregate population/session counters"* and the degradation verdict
ignores it, so no job-health signal moves.

⚠ **`job_runs` is not the whole population and the census says so.** The scheduler is the
only caller that persists these counters; `scripts/rebackfill_candles_5y.py` calls
`refresh_market_data(force_backfill=True)` and persists nothing, so a `force_backfill`
count can never appear in `job_runs`. The by-cause totals are therefore also logged from
**inside `refresh_market_data`**, which every caller reaches — the existing
"Market refresh complete" line lives in the scheduler and does not. Gated on a non-zero
revision count so a quiet run stays quiet. Runs that die before the final progress
assignment lose their counters entirely — a pre-existing property of the transport,
recorded as a census limitation rather than fixed here.

### Invariants, asserted in tests rather than documented

- `sum(bars_revised_by_cause.values()) == bars_revised`.
- Every cause with a positive count has an entry in `bars_revised_max_age_days_by_cause`,
  and no cause without one does.
- `max(bars_revised_max_age_days_by_cause.values()) == bars_revised_max_age_days`.

⚠ **Conservation cannot validate attribution** — labelling every revision
`stale_reobservation` satisfies the sum. The labels are therefore pinned by per-branch
tests that drive the real loop with a fake provider and assert the key, not by the sum.

⚠ `initial_backfill` is expected to contribute ~0 by construction, and a non-zero count is
**not** declared an invariant violation: one provider response carrying the same
`price_date` twice with different values inserts and then revises within a single call, and
a concurrent writer can insert between the fetch decision and the upsert. It is enumerated
so that such a count is visible rather than absorbed into `stale_reobservation`.

## What this deliberately does NOT do

- **No decision on #2414's fix.** The correction-buffer embargo is refused by the 275-day
  observation; the choice between a corpus stamp in the key and an explicit
  supersede-and-record path stays open and is not prejudged.
- **No claim that a split leaves a verdict valid, or that a correction invalidates it.**
  That mapping is #2414's open question. It is not asserted anywhere in this document, and
  these counters do not test it.
- **No magnitude.** `RETURNING OLD.close` is PostgreSQL 18 and this cluster is 17.9
  (recorded at `market_data.py:1125-1131`). Unchanged.
- **No per-instrument identity, dates or affected fields.** Those are what a later
  investigation of verdict impact needs, and `context` is bounded provenance, not a log.
  Naming the limit is the point.
- **No schema change and no backfill.** `price_daily` retains no prior value, so the 4,076
  already-counted revisions cannot be attributed from stored data. ⚠ Not *nothing*: the
  heal path logs instrument, symbol and ratio at WARNING (`:737-744`), so the subset that
  healed is partially recoverable from retained logs. No old value is recoverable anywhere.
- **No new refusal, gate or threshold.** Nothing reads these counters to decide anything.

## Acceptance

1. `revision_cause` table-tested over every branch and both ordering traps — a heal at
   `fetch_reason == "incremental"`, and a `force_backfill` whose reason is absent.
2. `_candles_fetch_count`'s three reasons tested directly, including `lookback_days == 3`
   (the case that made the old inference wrong) and a `gap_days` exactly at the boundary.
   ⚠ **And the same case driven through `refresh_market_data`.** The direct test pins the
   FUNCTION; it does not stop the caller throwing the reason away and re-deriving one
   from the count, which is what the code did before this change. A revert-probe
   re-introduced exactly that with every pure test still green — so the caller-side test
   is not redundant coverage, it is the only test of the layer the defect lived at.
3. Per-branch attribution driven through `refresh_market_data` with a fake provider, one
   test per cause, asserting the emitted key — not the sum.
4. The three invariants asserted, and revert-probed.
5. A mixed run (two instruments, two different causes) asserting both keys appear.
6. `daily_candle_refresh` run on the dev stack after deploy; `--causes` prints a non-empty
   window with zero conservation breaches.
   ⚠ A dev run with zero revisions satisfies every invariant vacuously, so the run is
   reported with its revision count and is not treated as evidence of attribution if that
   count is zero.

Refs #2414. Refs #2066. Refs #271. Refs #1293.
