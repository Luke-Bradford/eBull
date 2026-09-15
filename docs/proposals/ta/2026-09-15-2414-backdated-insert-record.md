# #2414 item 2 — record the bar that appeared BEHIND the frontier

Shipped 2026-09-16 (`sql/388`). Sibling of `2026-09-15-2414-bar-revision-record.md`
(`sql/387`, `22464e00`) and `2026-09-15-2414-revision-cause-attribution.md`
(`412997fc`).

## The gap, in `sql/387`'s own words

> ⚠⚠ WHAT THIS IS NOT: "THE CORPUS MOVED". It records exactly one mutation class —
> an OHLCV overwrite. A decision's consumed inputs also change when: a HISTORICAL
> BAR IS INSERTED (stale re-observation, interior gap fill). `_upsert_candles`
> counts those as `inserted` and they produce NO row here, yet they move every
> later recursive indicator, the segment calendar and next-bar fill identity.

So the shipped table is a partial answer to its own question, by design and on the
record. This closes the other half.

## Source rule

⚠ An earlier draft of this document asserted *"there is no published or external
rule here"*. **That is false and Codex checkpoint 1 caught it.** The recursion this
whole slice rests on is sourced in the code already:
`app/services/indicator_series.py::adx_series` carries
**`SOURCE RULE: J. Welles Wilder Jr., *New Concepts in Technical Trading Systems*
(1978), ch. 4`**, and `atr_series` implements the same author's smoothing. What is
*not* published is any cut-off radius, and that is the thing this document has to
fix by construction.

Governing rules, cited rather than reasoned out:

1. **No structural cut-off bounds how far a corpus change can reach.**
   `atr_series` / `adx_series` are Wilder recursions seeded once at bar `period`,
   then `current = (current × (period − 1) + tr) / period`
   (`app/services/indicator_series.py:435-481`). Dependence decays geometrically
   and never reaches zero.
2. ⚠ **The stronger claim — "an insert changes EVERY later value" — is wrong and
   was in the first draft.** Segment restarts, bounded windows (SMA, Bollinger),
   masked fields, identical values and float convergence all defeat it; `sql/387`
   already documents several of those exceptions. The defensible statement is
   *"can reach arbitrarily far, with no derivable bound"* — which is exactly what
   makes recording the event necessary and scoping it impossible.
3. ⚠ **The measured radii are a SAMPLE and are not the argument.** `3a12cfc0`
   measured S-4 at 151 bars against a `WARMUP_BARS` of 113 and S-8 at 81 against
   27, over 20 instruments. That falsifies "scope it by a bar count" for those two
   rules; it establishes nothing about other strategies or series. The structural
   recursion argument is what generalises.
4. **An audit row lives in the bar write's transaction** (`sql/387` header; #1293
   for the inverse rule on counters). Unchanged here.

## ⚠⚠ The discriminator is NOT "interior"

`force_backfill` deepens an instrument from 400 stored bars to 1000 — 600 inserts
**below** the existing minimum, each of which re-seeds the Wilder recursion. So the
test is **"not a forward extension"**:

```
backdated  ⟺  bar.price_date < frontier_before
```

- `frontier_before is None` (no prior history) ⇒ **no bar is backdated**. A
  1000-bar initial seed produces zero rows, which is right: nothing had been
  committed, so nothing had been decided against.
- Equality is unreachable in production — a bar on the frontier conflicts, so it is
  a revision or an `IS DISTINCT FROM` no-op — and the comparison is strict anyway,
  pinned by a test that drives the impossible case.

### The frontier is FIXED within a call, and READ inside the transaction

Two separate decisions, and the first draft got one of them wrong.

**Fixed within the call.** The frontier is not advanced as bars land. A payload
ordered `[F+3, F+1]` must record nothing: within one transaction nothing is visible
to any reader, so a bar arriving second was never behind *committed* history.
Advancing it would record provider **payload order**, which is not a property of
the corpus.

⚠ The test for this must be `[F+3, F+1]`, **not** `[F+3, F−1]`. Codex checkpoint 1
found the draft's `[F+3, F−1]` case cannot detect the bug — the second bar is
backdated under both policies. Revert-probed after the fix: an advancing
implementation fails exactly
`test_the_frontier_is_fixed_and_not_advanced_by_bars_landing_in_this_call` and
`test_the_frontier_is_not_advanced_from_an_empty_series_either`, **and no other
test in the file**, which is the measurement that the `[F+3, F−1]` shape would have
passed against the bug.

**Read inside the transaction.** ⚠⚠ The draft reused `last_bar_before`
(`market_data.py:741`) on the stated grounds that it is "the frontier at
transaction start". It is not — it is read **before `BEGIN`**, deliberately, for
#2262's supply marker. Codex's counter-example: writer A reads frontier 10, writer
B commits 15, a decision consumes B's history, A then inserts 12 — A calls 12 a
forward extension and it is not. So `_observed_frontier` is a separate
in-transaction read.

⚠ That makes the window **tight, not empty**. Under Read Committed a concurrent
commit inside the remaining window still misclassifies. Which is why
`frontier_before` is **stored on the row**: the classification is a property of
what one writer observed, not of the corpus, so two honest writers can disagree
about the same date and the row says which frontier it used. Without the stored
frontier the classification is unreconstructible — and `--census`'s
`price_date < frontier_before` self-consistency check would not exist.

## Premise checks

**1. The class is reachable, and the first draft overstated the evidence.** The
draft said *"closing a gap IS a backdated insert"*. ⚠ Not necessarily: if stored
history ends at d10 and the provider supplies d11–d15, `stale_reobservation` closes
the ingestion gap entirely through forward extensions. `_candles_fetch_count`'s
docstring (*"a 3-bar incremental fetch here would silently leave a history gap;
falling back to `default` closes the gap"*) establishes **possibility, not
occurrence** — which is why the census below is the load-bearing evidence and not a
sizing footnote.

**2. The input is nearly free.** One extra indexed `MAX(price_date)` per instrument
per fetch, on a path that makes up to 3 eToro calls per instrument.

**3. Prevention-log neighbour, same class, reached from the scan side.**
`docs/review-prevention-log.md:3844` — `strategy_signal_scan` reads the frontier in
one aggregate then loads per instrument, so *"an instrument can gain a bar between
the two reads"*, and *"under a ledger with no `ON CONFLICT` there is no repair short
of a version bump"*.

## Full-population verification

⚠⚠ **The first census this document carried was wrong in two ways, and Codex
checkpoint 1 caught both.** It reported **449,533** interior gaps over 12,284
instruments and called that an upper bound. It was neither a gap count nor a bound:

- **The subtraction was not a gap count.** It computed
  `(calendar dates in span) − (total bars)`. `price_daily` holds bars on dates that
  are not sessions — 3,669 weekend bars across 389 instruments are already in the
  prevention log — and every such bar **cancels a genuinely missing session date**.
- **The calendar was invented.** ">= 500 instruments have a bar" is a heuristic; the
  repo already owns the documented rule (`app/services/market_calendar.py`, NYSE
  holiday calendar + observed extraordinary closures, sourced to nyse.com), and the
  instrument side resolves through `exchanges.asset_class` — the join
  `exchanges.exchange_id = instruments.exchange` is used by
  `scripts/verify_3046_weekend_bar_census.py` already. The draft claimed that map
  did not exist.

Both figures are now computed by
`scripts/verify_2414_backdated_insert_census.py --gap-census` — ANTI-JOIN, real
calendar, `us_equity` scope — so nothing here can go stale silently. Run
2026-09-16:

```
scope           : asset_class=us_equity
corpus range    : 2019-12-16 .. 2026-09-15
bars in scope   : 4,216,888 across 7,151 instruments
NYSE sessions   : 1,695
instruments     : 7,151
with >=1 gap    : 5,848 (81.8%)
interior gaps   : 186,318   worst instrument 1,160   median of affected 28
```

**Named exclusions**, because a bound that hides them is not a bound:

- **Below-minimum deepening is not counted.** Provider reach beyond our oldest bar
  is not knowable from stored state, so the real population is strictly larger.
- **Non-US-equity instruments are excluded** — no authoritative session calendar
  for LSE / Euronext / Tokyo, and `asset_class = 'crypto'` trades 24/7. Their
  interior gaps are real; this declines to guess rather than inflating the figure
  with foreign holidays, which is what the superseded version did.
- **A missing session date is capacity, not a pending write.** The provider may
  genuinely have no bar (halt, IPO mid-span, relisting).

⚠ **No comparison is drawn against `price_daily_revision`'s rate.** The draft set
449,533 beside "4,080 revisions over 23 days" as a ~100× ratio. Those are unlike
quantities — hypothetical capacity against observed events — and the 4,080 figure
predates the revision table's existence (it is currently empty), so it has no
reproducible provenance here. Sizing claim withdrawn with it: census cardinality is
not a write-cost benchmark, and indexes, WAL and repeated insertion are unaccounted
for.

## Shape — a sibling table, NOT a rename

The draft specced `price_daily_revision` → `price_daily_mutation` + a
`mutation_kind` column, reasoning that `sql/387`'s objection was to the **name** and
a rename answers it.

⚠⚠ **Codex checkpoint 1 killed that on a deployment ground, and it is the single
most valuable finding of the pass.** The audit write is inside the bar write's
transaction. Migrations apply at API boot; the jobs child runs OLD code until it
respawns, and `dev_reload` can legitimately **defer** a respawn while a long sweep
is in flight — observed on 2026-09-15, #2274's heartbeat holding a deploy off run
131002 for over an hour. A rename therefore opens a window in which the old writer
INSERTs into a table that no longer exists, its audit write raises, and **it takes
the bar write down with it.** An audit row must never be able to destroy the thing
it audits. Adding a table has no such window.

`price_daily_corpus_mutation` (a `UNION ALL` view) is the completeness surface, so
the "a consumer always needs both classes" requirement that motivated the rename is
met without the deployment hazard. Its per-table surrogate keys are deliberately
**not exposed**: they are separate sequences and `mutated_at` is a shared
transaction start time, so nothing in the view establishes cross-kind write or
commit order.

`cause` reuses `RevisionCause` — it is the **write branch**, identical for both
classes. ⚠ `initial_backfill` + `backdated_insert` is **not** impossible, only
race-impossible: `_candles_fetch_count` and the frontier read are separate
statements, so a concurrent writer committing history between them produces exactly
that pair. There is **no cross-column CHECK** forbidding it, deliberately — a
constraint violation here would abort a price write over a telemetry disagreement.

## What this does NOT do

- It does **not** locate affected `strategy_signals` rows. `sql/387`'s header
  enumerates the ten reasons that join is neither an upper nor a lower bound;
  adding a mutation class repairs none of them. The missing piece is a read-set
  record, and `created_at` is the ledger **write** time.
- **The raw stored maximum is not the frontier a scan uses.**
  `strategy_signal_scan` derives a population frontier, masks
  (`load_masked_bars`), applies eligibility and segments. A future-dated or
  quarantined bar can be the stored maximum and be consumed by nothing.
- **A forward extension is not universally irrelevant either.** A cross-sectional
  or benchmark-gated decision moves when some *other* instrument gains a bar. This
  table is per-instrument and says nothing about it (#2414 item 3 — latent, both
  cross-sectional strategies retired under #2845).
- It does **not** cover `price_bar_quarantine` verdict changes, `price_series_break`
  resolution, rule-set bumps, or **deletions** (`price_daily` has no delete path
  outside test fixtures; the "same date across calls" case rests on that).
- It does **not** touch `strategy_signals_unique`, the ledger key, or any
  supersede-and-record path. #2414 items 1, 3 and 4 stay open and unprejudged.

## Verification

`--gap-census` is the population; `--census` is what was recorded, and it prints its
own refusal to be interpreted when empty (an empty table cannot distinguish "no bar
appeared behind a frontier" from "the writer has not run" — and hourly
`daily_candle_refresh` passes are ~9s with `items_done=0`). It also asserts
`price_date < frontier_before` on every stored row, which is possible only because
the frontier is stored.

⚠ **That is a smoke test plus a self-consistency check, not a reconciliation.**
Proving the writer saw every qualifying insert would need the provider payloads,
which are not retained. Stated rather than implied.
