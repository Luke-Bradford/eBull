# A superseded fill price is a recorded refusal, not an exception that aborts the batch

#2414, the supersession half. Narrow slice: the outcome-resolution path only.

## The defect

`strategy_signals.fill_price` is a stored copy of one bar value. `resolve_fills` prices
every fill at `open(signal_index + 1)` and `load_masked_bars` masks fields to `None`
without rescaling anything (`app/services/price_masked_bars.py:174`), so

```
strategy_signals.fill_price == price_daily.open at (instrument_id, fill_bar_date)
```

is an **invariant**, and any inequality is proof the bar moved after the verdict was
stored. `resolve_outcome` already knows this and enforces it
(`app/services/outcome_resolver.py:390-396`):

```python
if entry_price != fill_open:
    raise ValueError(
        f"entry_price {entry_price} disagrees with open[{fill_index}] = {fill_open} on "
        f"{series.dates[fill_index]} — the fill and the series must come from the same corpus"
    )
```

That guard is right — a silent re-read would quietly reinterpret a recorded decision — but
**nothing catches it.** `_resolve_fill` (`app/services/strategy_outcome_resolution.py:100`)
calls it bare, inside a `for fill in instrument_fills` loop, inside a
`for instrument_id …` loop, inside the per-strategy loop of `run_outcome_resolution`. The
raise therefore:

- aborts the current strategy **before** its `conn.transaction()` block, so no outcome row
  and no cursor advance is committed for it;
- aborts every **alphabetically later** strategy in the same batch, which never runs;
- leaves the cursor where it was, so the next tick re-selects the same batch and raises
  again — a permanent wedge, not a transient failure.

The job's whole run is marked `failure`. That is observed, not predicted: `job_runs` holds
one `strategy_outcome_resolution` failure, 2026-08-15 03:53:05Z, `row_count = NULL`,
`error_msg = "stop 50.296543131568406 is not below the entry 48.010000: a stop at or above
entry triggers immediately and is not a trade"`.

⚠ **That failure is NOT this defect, and the first draft of this spec said it was.** The
attribution was checked rather than assumed: the entry price identifies PAAS
(`instrument_id` 6495), fill bar 2026-08-13, and that bar's `open` is **still 48.010000**
today with **0** `price_daily_revision` rows. So its cause was an unorderable bracket
reaching `resolve_outcome`, not a moved bar. What it establishes — and all this spec uses
it for — is the **mechanism**: one escaped `ValueError` from this unguarded region has
already taken a whole run down, so the class is live even though this instance was a
different member of it. The unorderable-bracket instance is recorded as a separable defect
rather than fixed here; a catch-all `except ValueError` is precisely what #2489's own
commit (*"classify exit refusals without message matching"*) rejected.

## Source rule

Not reasoned from first principles. The governing precedent is this repo's own settled
treatment of the identical shape, `sql/296_strategy_outcomes_unorderable_exit_levels.sql`
(#2489, merged `dbc18d22`), whose header states the rule:

> one unorderable S-4 bracket must not abort the evidence or forward outcome batch. The
> existing bounded ledger gets one new closed refusal code; no rows, metrics or time-series
> stores are added.

and is pinned by `tests/test_strategy_outcome_resolution.py::test_unorderable_exit_levels_are_terminal_without_aborting_the_batch`.

The vocabulary rule is `app/services/outcome_resolver.py:75-79`: *"CLOSED vocabulary, for
criterion 9's 'measure what you reject': free text cannot be counted"*, with ours kept as
an explicit subtraction (`OUR_ADDITIONAL_REASONS`) so adopting a parent code later cannot
land silently on our side of the line.

So the treatment is fixed by precedent: **one new closed `UnresolvedReason`, one terminal
row, no new table, no new column.**

## Full-population verification

Dev DB, read-only, 2026-09-18. `strategy_signals` holds 59,069 rows.

| measurement | value |
| --- | ---: |
| stored `fill_price` ≠ `price_daily.open` at the fill bar | **221** (0.374%) |
| … `signal_kind = 'entry'`, `verdict = 'fired'` | **87** |
| … `signal_kind = 'exit'` | **134** |
| … already carrying a `strategy_outcomes` row | **4** |
| … at a **current** strategy_version (i.e. selectable by `select_pending_fills`) | **0** |
| fired entries at current level-based versions (the re-resolution set) | **358** (s4 292, s8 66) |

Reproduce: `scripts/census_2414_decided_bar_revisions.py` (part 1) for the 221; the
version intersection is one join against `STRATEGY_MANIFEST[…].identity(…).version`.

Two things follow, and both are load-bearing:

1. **The resolver is not wedged today, and this is not a repair.** Every one of the 87
   stale fired entries sits at a superseded `strategy_version`, and
   `select_pending_fills` filters on the current one — so none is selectable. The change
   is a guard against a recurrence that the corpus makes near-certain, not a fix for a
   live stall. Stated plainly so nobody reads a wedge into the job history.
2. **The recurrence is structural, not incidental.** The scan decides on bar `t` and
   prices the fill at `open(t+1)`, so every fresh verdict's stored price sits on the
   frontier-adjacent bar that the next `incremental` candle pass rewrites. #2414's census
   found every one of the 221 disagreements to be an **exact** corporate-action ratio
   (2:1, 3:2, and reverse splits up to 125:1), and `price_adjustments` holds 0 rows, so no
   adjustment layer intervenes. A split on any instrument holding a current-version
   pending fill produces the wedge.

### The neighbouring raise classes, measured rather than assumed away

Three other `ValueError`s live in the same unguarded region. Each was measured on the full
ledger before being left out of scope, so "not handled" is a stated zero rather than an
omission:

| raise | population check | rows |
| --- | --- | ---: |
| `locate_fill_index` — fill date gone from the series | fired entries with no `price_daily` row at `fill_bar_date` | **0** |
| `_locate_signal_index` — signal date gone | fired entries with no `price_daily` row at `signal_bar_date` | **0** |
| `resolve_outcome` — fill bar has no `open` | fired entries whose fill bar's `open` is NULL | **0** |

The fourth — an unorderable bracket reaching `resolve_outcome` — is the 2026-08-15 failure
above and is the one member of the class that has actually fired. It is a different cause
with a different fix and is recorded, not taken here.

## The change

1. **`fill_price_superseded`** joins `UnresolvedReason` and `OUR_ADDITIONAL_REASONS`
   (`app/services/outcome_resolver.py`).
2. **`sql/396_strategy_outcomes_fill_price_superseded.sql`** widens
   `strategy_outcomes_reason_check` to the existing five plus the new one, in the same
   drop-and-recreate shape as `sql/296`, with the live constraint's current membership
   recorded in the header. `tests/test_outcome_ledger.py::TestMigrationVocabularyContract`
   re-points at 396 and keeps asserting set-equality with `UNRESOLVED_REASONS`.
3. **`_resolve_fill` refuses instead of raising.** After the series-break test and
   **before** the exit-levels factory, it compares `fill.fill_price` with
   `series.rows[fill_index]["open"]` and returns a terminal
   `unresolved / fill_price_superseded` row on disagreement.
4. **`_resolve_forecast` does the same** (`app/services/strategy_forecast_outcome_resolution.py`).
   Both paths are in scope because both read the **same** stored
   `strategy_signals.fill_price` — the forecast selector's own SQL is `SELECT … s.fill_price
   FROM … strategy_signals s`. The forecast path is the worse of the two: it sizes both
   barriers as a percentage of that price, so a superseded fill puts the entire bracket on
   the old scale before `resolve_outcome` rejects the pair. Shipping the guard on one path
   would leave the claim false on the other.
5. **The invariant is written once**, as `outcome_ledger.fill_price_is_superseded` — the
   module both callers already import for `locate_fill_index`, which is the other half of
   the same "same corpus" contract. Two spellings of one invariant is the drift the closed
   vocabulary rules exist to prevent.

### Why that placement, precisely

- **Before the levels factory**, because the factory is the first consumer to mix the
  stored entry with the reloaded series, and it is what raised on 2026-08-15. Detecting
  after it would leave that failure mode open.
- **After the series-break test**, so that no row which resolves today changes its label.
  A break between signal and fill keeps `series_break`.
- **Only when the bar's `open` is present.** A masked open at the fill bar keeps its
  existing behaviour (`resolve_outcome`'s own `no open` raise, argued unreachable because
  phase 3c records `no_fill_bar` instead). Firing `fill_price_superseded` on a `None` open
  would assert a cause we have not observed.

The change is therefore **strictly additive**: a fill whose stored price still matches its
bar is untouched, and the only rows whose behaviour changes are the ones that today raise.
There is no arm in which an existing outcome is reclassified, which is why this carries no
full-population A/B — the A/B's distinct-entity metric would be identical by construction
on every row that can be written today.

### The rule-set bump is real and is sized

`RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"` hashes this module's own source, so
editing `outcome_resolver.py` bumps it, and `select_pending_fills` makes every fill pending
again at the new version pair with the old outcomes intact beside it. That is the designed
behaviour and `sql/296` did the same. **Measured cost: 358 fired entries** (s4 292, s8 66),
i.e. one `batch_limit = 1000` tick. `strategy_outcome_cursor` is keyed on the version pair,
so its rows are replaced rather than migrated. The forecast path's
`RESOLVER_VERSION` embeds `path-{PATH_RULE_SET_VERSION}` and so bumps too; its cost is
**0**, because `strategy_opportunity_forecasts` holds 0 rows.

Three things that might have made the bump expensive, checked:

- **Strategy identity does NOT move.** `s4_identity` and its siblings hash their own
  module's source (`_source_hash()`); `docs/proposals/ta/2026-09-14-3031-…` records the
  outcome rule set as *"excluded — `sql/256` makes it a KEY member of `strategy_outcomes`,
  deliberately outside the strategy hash"*. Had it moved, today's 358 current-version
  signals would have become 0 and the whole ledger would have stranded.
- **No golden file pins the live version.** Every test reference is a dummy hash
  (`outcome-resolver-v1+abc123` and friends); `tests/test_outcome_resolver.py:471` asserts
  the prefix only.
- **`scripts/audit_2745_in_sample_run.py:78` pins `outcome-resolver-v1+54aa83427048` and is
  deliberately left alone.** It is an `EXPECTED_IDENTITY` for a frozen in-sample run — it
  audits stored rows against what was actually run, so updating it would be falsifying the
  record, not maintaining it. It already pins superseded strategy versions for the same
  reason.

## What this slice deliberately does NOT do

- **No `superseded_at` on `strategy_signals`.** #2414's own prior comment is the reason:
  the table is read at 19 sites across 12 modules, and a column those readers ignore is
  worse than none — the row would still feed paper execution and the live gate while the
  column asserts it does not. The state is expressed where it is consumed and where the
  resolver is the only writer.
- **The 134 stale exit-side signals are untouched.** `select_pending_fills` filters
  `signal_kind = 'entry'`, so they never reach this path. Who reads a stale exit price, and
  whether it matters, is a separate question and is not answered here.
- **The 4 outcomes already written against a corpus that no longer holds those bars are
  not repaired or flagged.** Their `gross_return_pct` is internally consistent (entry and
  exit both came from the era they were computed in); what is lost is reproducibility.
  Repairing that needs a read-set record, which #2414's census already names as the
  unbuilt prerequisite.
- **No claim about affected VERDICTS.** `sql/387`'s header kills that inference; every
  figure above is a claim about bars.

## ⚠ Codex checkpoint 1 could not run

`codex exec` returns `You've hit your usage limit … try again at Sep 19th, 2026 11:57 PM`
for this whole session, so the mandated spec pass did not happen and is not silently
missing. The checkpoint's own question list was run against this spec by hand instead, and
three of its questions changed the document rather than confirming it:

1. *Is a causal claim in here unverified?* — yes, the 2026-08-15 attribution, now corrected
   above after checking PAAS's bar.
2. *Does the identity hash you are bumping reach further than you think?* — the forecast
   resolver embeds it, which is how the second unguarded path was found. It was not in the
   first draft's scope at all.
3. *Are the edge cases you excluded measured or assumed?* — the three neighbouring raise
   classes are now a table of measured zeros.

Two it confirmed without change: the migration's two constraint names were read from
`pg_constraint` on the live DB rather than inferred from the DDL, and `refusalLabel`
(`frontend/src/pages/StrategiesPage.tsx:705`) falls back to the raw code, so a new refusal
string cannot break a closed frontend union.

## Acceptance

- A `_resolve_fill` over a series whose fill-bar open disagrees with the stored
  `fill_price` returns `("unresolved", "fill_price_superseded", None)` and does not raise.
- The same case with an intervening series break still returns `series_break`.
- A masked (`None`) fill-bar open does not return `fill_price_superseded`.
- The migration's CHECK membership equals `UNRESOLVED_REASONS`.
- Dev-verify: `strategy_outcome_resolution` runs to `success` on the bumped version pair
  and the 358 re-resolutions drain.

### Acceptance result (dev DB, 2026-09-18)

`sql/396` applied; new pair `outcome-resolver-v1+1231a0239ea5` / `price-quarantine-v1+…`.
Three ticks of `run_outcome_resolution`, no exception:

| tick | selected | written | immature |
| ---: | ---: | ---: | ---: |
| 1 | 232 | 47 | 185 |
| 2 | 215 | 14 | 201 |
| 3 | 215 | 0 | 215 |

**61 terminal outcomes written + 297 still pending = 358**, which is the predicted
re-resolution set exactly. The 297 are immature forward windows, the normal steady state,
retried daily.

⚠ **`fill_price_superseded` count at the new pair: 0** — and that is the predicted result,
not a failed verification. Every stale row sits at a superseded `strategy_version` and is
therefore unselectable; the refusal is a guard on the path, and its discrimination is
established by revert-probe rather than by a dev row. Recording the zero so nobody later
reads it as "the code never fires".
