# Research-corpus quarantine refresh job (#3040)

Unshipped proposal. Target: the 76M-bar research corpus regains the self-healing property
`price_daily` already has when `price_quarantine.RULE_SET_VERSION` moves, and the `as_of` that
produced a coverage row stops being unrecorded.

Codex checkpoint 1 returned 31 findings on the first draft and reshaped it twice over. The
verified ones are folded in below; §7 records what was rebutted and why.

## 1. Problem, measured

`RULE_SET_VERSION` moved `price-quarantine-v1+d0423dbd9cb5` → `…+49ff29fea766` when #3028's
`rule_b4` fix landed. Both masked readers inner-join their coverage table on that version and
are fail-closed — missing coverage yields **zero bars**, never unmasked bars:

- `app/services/price_masked_bars.py:76-78` — `price_quarantine_coverage`
- `app/services/research_price_structure_store.py:62-78` — `research_price_quarantine_coverage`

On the 2026-09-14 deploy the `price_daily` half repaired itself at 11:28:33–11:29:59Z via
`price_quarantine_refresh`. The research half did not and could not:
`app/services/research_corpus_ingest.py:927::run_quarantine` has two callers, both operator
scripts. It sat at zero coverage rows at the current version — every `load_arms` read empty —
until run by hand ~2 h later.

⚠ #3031 (`eae4f507`) added `price_quarantine` to `INPUT_RULE_SETS`, so this module's version is
load-bearing for `strategy_version`. Every future edit necessarily rotates strategy identity
**and** empties the research corpus; only the first is visible.

### The existing staleness signal is too weak to be the alarm

`app/services/research_price_read_canary.py:36-38` computes `coverage_current` per series and
`:121` consumes it (`eligible_rows = [row for row in rows if bool(row[4])]`). The bar is
**5 eligible series**, so a corpus 99.98% stale passes; and its only caller is
`scripts/verify_2772_research_read_canary.py`, which nothing schedules.

### The second defect, found by checkpoint 1: `as_of` is an unrecorded input

`run_quarantine(conn, *, vendor, as_of)` takes `as_of`, and `price_quarantine.py:389`
(`provisional_from = as_of - timedelta(days=PROVISIONAL_WINDOW_DAYS)`, window 5) makes it an
input to the stored verdicts. `provisional` is not cosmetic — `price_quarantine.py:487-501`
shows it suppressing T3's volume read and propagating through transition verdicts.

`research_price_quarantine_coverage` records `rule_set_version` and **not** `as_of`. So two
runs under one version can store different verdicts and nothing can tell. That is the defect
prevention-log lines 2898-2900 name in terms: *"a versioned key on a derived table where the
pipeline feeding it is ALSO versioned and that version is not in the key … ask 'what else, if
it changed, would change this row?'"*.

Measured, current corpus:

```
icyDenev/Intrader                      provisional 50,563   non-provisional 14,997
paperswithbacktest/Stocks-Daily-Price  provisional      0   non-provisional  1,327
```

## 2. Source rule

No external regulator governs this. The governing rules are our own, cited rather than derived:

- **`sql/251`** owns `research_price_quarantine_coverage` and its `rule_set_version` column.
- **`research_price_structure_store._LOAD_SQL`** owns the fail-closed contract.
- **#3028's addendum (2026-09-14)**: *"a coverage RECONCILIATION before any scan is allowed,
  not a counter read afterwards"* — coverage at exactly one version, count matching the census.
  Adopted as the predicate, with the series-containment form the canary already uses.
- **Prevention log 2898-2900**: a versioned derived table must key on every versioned input.
  This is why `as_of` becomes a stored column rather than a convention.
- **Prevention log 1725-1726**: a job in `_INVOKERS` absent from
  `SCHEDULED_JOBS ∪ _BOOTSTRAP_STAGE_SPECS ∪ MANUAL_TRIGGER_JOB_SOURCES` raises `KeyError`
  from `source_for()` at the next `JobLock` acquisition.
- **Prevention log 2163**: a plan-level invariant needs one db-tier twin; a mocked connection
  asserts the decision while the write raises in production.

## 3. Design

### 3.1 The work condition lives in the JOB, not only in a freshness predicate

⚠⚠ **This is the checkpoint-1 finding that reshaped the design, and it is verified in code, not
inferred.** The first draft put the work condition in `is_fresh` alone. That does not hold:

- `app/services/sync_orchestrator/registry.py:66-69` marks `is_fresh` the *legacy* combined
  predicate and adds `content_predicate` beside it for the new state machine.
- `app/services/sync_orchestrator/layer_state.py:87-89` — **Rule 9**: `age_seconds >
  cadence_seconds * grace_multiplier` ⇒ `DEGRADED`, *regardless of content*. A frozen corpus
  at the correct version degrades on the clock alone.
- `app/services/sync_orchestrator/planner.py:44` —
  `bypass_freshness_for_all = scope.kind == "behind"`, and a `behind` walk selects precisely
  the DEGRADED layers. So `is_fresh` is **not consulted** on the path that would fire it.

So a content-only `is_fresh` would have been bypassed and the 8-minute pass would have re-run
nightly anyway. The fix is to make the job idempotent-by-condition:

> `refresh_research_quarantine(conn)` resolves, per archive vendor, whether coverage is complete
> at the current `(RULE_SET_VERSION, quarantine_as_of)`. It calls `run_quarantine` only for
> vendors that are not, and returns a per-vendor census naming which it skipped and why.

That holds on every path — cron, forced scope, `behind` scope, manual "Run now" — because it
does not depend on the planner agreeing with it.

`content_predicate` then carries the same reconciliation as a *detector*: it makes an
incomplete corpus visible as DEGRADED on the admin surface and causes the `behind` walk to
select the layer.

⚠⚠ **`is_fresh` keeps its audit-age term, and the first draft was wrong to drop it.** Making
it content-only looked right — frozen archives cannot go stale on a clock — but `is_fresh` is
not the only thing reading the clock. `layer_state.py:87-89` degrades a layer at
`age > cadence × grace` (30 h) regardless of content, and a layer the planner *skips* writes
no new `sync_layer_progress` row. A perfectly covered corpus would therefore age past 30 h, be
skipped for being fresh, and sit falsely DEGRADED until a reboot — `SyncScope.behind()` has
exactly two callers, `app/jobs/boot_sweep.py:37` and the operator API. Caught by checkpoint 2.

The age term is affordable *because* the expensive guard moved into the service: a daily fire
against a current corpus costs two queries. The corpus rewrite is gated by coverage; the audit
timestamp is what keeps the health model honest.

### 3.2 The layer

New orchestrator layer `research_price_quarantine`, registered like `price_quarantine` except:

| | `price_quarantine` | `research_price_quarantine` |
| --- | --- | --- |
| `dependencies` | `("candles",)` | `()` — two frozen archives loaded by script; no layer produces them |
| `requires_layer_initialized` | `("candles",)` | `()` — same reason, and so no `INIT_CHECKS` entry is required (`registry.py:92-99` raises on a named dep with no entry) |
| `content_predicate` | none | the coverage reconciliation |
| work when fresh | full recompute | **skip**, ~2 queries |

⚠ **Repair latency is bounded by the walk, not by this layer.** `JOB_ORCHESTRATOR_FULL_SYNC` is
daily at 03:00 UTC with `catch_up_on_boot=False`, and `orchestrator_high_frequency_sync` is
scoped to portfolio/FX. So a version bump can leave the corpus dark until the next 03:00 walk
or a manual trigger. This ticket does not change that; it changes "dark until a human runs a
script, with no alarm" into "dark until the next walk, and DEGRADED on the admin surface
meanwhile". Stated rather than implied, because the first draft claimed self-healing without
naming the window.

### 3.3 `as_of` becomes a declared constant AND a stored column

Two separate changes, both required:

**(a) Declared.** `ArchiveProvenance` gains `quarantine_as_of: date`, a literal:

- `INTRADER_ARCHIVE` → `date(2024, 9, 27)` — the archive's capture date. Unchanged from what
  `scripts/ingest_2597_intrader_archive.py` resolves by measurement today.
- `HF_ARCHIVE` → `date(2026, 7, 14)` — capture (2026-07-08) + `PROVISIONAL_WINDOW_DAYS` + 1,
  the smallest date that marks nothing provisional. This is what
  `scripts/ingest_2282_research_archive.py`'s `date.today()` default produces for an archive
  whose last bar is two months old, without reading a clock.

⚠ Literals, not `max(last_bar)`. Checkpoint 1: `max(last_bar)` is *mutable data* — one
later-ending or erroneously future-dated series silently re-dates the whole vendor's policy
while existing coverage still reads fresh. A capture date is provenance and belongs beside
`licence` and `adjustment_basis`, which are also pinned facts about a frozen file.

⚠ The two vendors' policies genuinely differ and this **preserves** rather than resolves that.
Making both `capture_date` would mark HF's 2026-07-03→08 bars provisional across 7,693 series
— a corpus change needing its own A/B, contradicting what is stored now, and out of scope.
Recorded as a declared divergence rather than an accident of two scripts.

⚠ Boundary, corrected from the first draft: `provisional_from = as_of − 5` and the test is
`bar.price_date >= provisional_from`, so the Intrader provisional band is
**2024-09-22 → 09-27 inclusive**, not 09-23. The five marks
`app/services/market_regime_provider.py:327` names are the bars that *exist* on those two
pinned series, which is an observation about that series pair and not the rule.

**(b) Stored.** Migration adds `quarantine_as_of date NOT NULL` to
`research_price_quarantine_coverage`. `run_quarantine` writes the `as_of` it used; the
reconciliation matches on `(rule_set_version, quarantine_as_of)`.

This is what closes the operator-override hole: `--as-of 2020-01-01` currently writes divergent
verdicts under an unchanged version and nothing — predicate, census or canary — can see it.
With the column, an override leaves coverage that does not match the declared policy, the
layer reads DEGRADED, and the next run restores it.

⚠ Consequence, stated so it is not a surprise: HF's currently-stored rows were written with
`as_of = 2026-09-14` (the script's `date.today()`), not the declared `2026-07-14`. Both produce
**identical verdicts** (nothing is provisional under either), but the stored value differs from
the declaration, so the first run after this lands re-evaluates HF once — 165 s, measured — and
Intrader skips. That is the mechanism working, not a defect.

### 3.4 Transaction shape — inherited, not imposed

⚠ The first draft cited `refresh_price_quarantine`'s whole-run transaction. **That citation was
wrong for this writer.** `research_corpus_ingest.py:1057` calls `conn.commit()` per series, so
`run_quarantine` publishes incrementally by construction. The job must therefore **not** wrap it
in a transaction — doing so would fight the existing commits.

The consequence is that a failed run leaves a partially re-evaluated vendor. That is safe under
the fail-closed reader: series already at the new version read normally, series not yet reached
read empty. It is the *partially refreshed* state #3028's addendum warns about for the signal
scan, which is why the coverage reconciliation — not a counter — remains the gate before a scan.

### 3.5 Registration surfaces

| file | change |
| --- | --- |
| `sql/<n>_research_quarantine_as_of.sql` | `ALTER TABLE research_price_quarantine_coverage ADD COLUMN quarantine_as_of date NOT NULL` (backfilled from the declared per-vendor constants) |
| `app/services/research_corpus_ingest.py` | `quarantine_as_of` on `ArchiveProvenance`; `RESEARCH_ARCHIVES` tuple; `refresh_research_quarantine(conn)`; `run_quarantine` writes the column |
| `app/workers/scheduler.py` | `JOB_RESEARCH_PRICE_QUARANTINE_REFRESH` + job body in the `_tracked_job` + `connect_job` shape |
| `app/jobs/runtime.py` | `_INVOKERS` entry via `_adapt_zero_arg` |
| `app/jobs/sources.py` | lane `research_price_quarantine`; job→lane mapping |
| `app/services/sync_orchestrator/registry.py` | `DataLayer` + `JOB_TO_LAYERS` entry (⚠ that symbol, not `_JOB_LAYERS`) |
| `app/services/sync_orchestrator/adapters.py` | `refresh_research_price_quarantine` via `_wrap_single` |
| `app/services/sync_orchestrator/freshness.py` | `research_price_quarantine_is_fresh` |
| `app/services/sync_orchestrator/content_predicates.py` | `research_price_quarantine_content_ok` |
| `app/services/processes/param_metadata.py` | empty params entry |
| both `scripts/ingest_*_archive.py` | `--as-of` default resolves through the declared constant |

Own lane, not the catch-all `db`: a multi-minute full-corpus pass must not starve the db-lane
orchestrator sync — the #1526/#1527 class both `risk_metrics_refresh` and
`price_quarantine_refresh` cite.

`row_count` counts **series evaluated**. Zero is a legitimate success here (everything already
at the declared policy), which is a second reason not to count rows: the verdict tables are
sparse (66,887 rows over 75.97M bars) and a row count would read NO_WORK on a complete run.

⚠ Accepted, not fixed: `scheduler.py:2613` resolves `statement_timeout_ms` from
`_JOBS_BY_NAME`, so a manual job absent from `SCHEDULED_JOBS` gets `None` (unbounded). That is
exactly `price_quarantine_refresh`'s situation today, so this is parity rather than a new hole;
naming it here so the next reader does not rediscover it as a finding.

## 4. Tests

- Pure, table-driven over `RESEARCH_ARCHIVES`: every declared archive has a `quarantine_as_of`,
  so a third archive cannot be added without one.
- Pure: the reconciliation is stale when a series has no coverage row; stale when the row's
  `(version, as_of)` does not match the declaration; stale when the row's range does not
  contain the series range; fresh only when every archive-vendor series is contained and
  matched. ⚠ Written against SQL, not a mock — a `LEFT JOIN` miss tested with plain negation
  disappears through three-valued logic, which is checkpoint-1 finding 10.
- Pure: `refresh_research_quarantine` skips a complete vendor and runs an incomplete one, with
  the per-vendor reason in the census.
- Registration: extend the existing `tests/test_job_registry.py` / `tests/test_jobs_runtime.py`
  contracts, which already assert `source_for()` resolves every `_INVOKERS` name. Not a
  name-coupled twin (prevention log 1734).
- ⚠ One **db-tier** test, because the reconciliation is a property of the query plan and a
  mocked connection cannot catch it (prevention log 2163).

## 5. Acceptance

```sql
-- one version AND one declared as_of per vendor, covering every eligible series
select s.vendor, c.rule_set_version, c.quarantine_as_of, count(*)
from research_price_quarantine_coverage c join research_price_series s using (series_id)
group by 1,2,3 order by 1;
-- expect: icyDenev/Intrader  …49ff29fea766  2024-09-27  22,879
--         paperswithbacktest …49ff29fea766  2026-07-14   7,693
```

Then `research_price_quarantine_content_ok` returns `(True, …)`, and a second job invocation
evaluates **0** series.

## 6. Non-goals

- **No change to any quarantine RULE.** This job re-evaluates; it does not re-decide. The B4
  counts #3028 accepted (`price_daily` 105, research scoped 292) must be unchanged after the
  HF re-evaluation, and that is an acceptance check.
- **The 19 series in `cboe` (×1) and `etoro/etoro-comparators-2026-07-08-v1` (×18) stay out of
  scope, and the first draft's reason for that was wrong.** It claimed they are "not dark".
  `docs/proposals/ta/2026-08-16-backtest-scale-readiness.md:264` already records them as
  `30,572 / 19` eligible-vs-fail-closed and calls them *"a visible coverage-refresh gap [that]
  decode[s] zero bars until evaluated"* — so on the masked path they **are** dark, and only the
  two direct readers (`cboe_vix.py`, `research_comparator_snapshot.py`, both reading
  `research_price_daily` without the masked store) are unaffected. The real reason to exclude
  them is that `research_corpus_ingest.py:102` hardcodes `_ASSET_CLASS = "us_equity"`, and
  quarantining a VIX index series under equity thresholds is a data-treatment decision needing
  its own source rule. Named as a follow-on rather than smuggled in.

## 7. Checkpoint-1 findings rebutted

- *"No actual pre-scan gate"* — correct, and out of scope by construction. #3028's addendum
  places that gate on the **scan**, not on the refresh. A refresh-selection predicate was never
  claimed to be one; §3.4 restates where the gate belongs.
- *"False fresh after interior changes / corrected OHLCV"* — real in general, not reachable
  here: both archives are frozen files and `--load` is a separate, operator-run stage. A
  reconciliation that detected interior edits would have to hash 76M bars on every walk. If the
  archives ever stop being frozen, `bar_count` + endpoint containment is the wrong check and
  this decision must be retaken — recorded in the layer docstring rather than defended here.
- *"Dedicated lane does not isolate the scheduled execution"* — true of every orchestrator
  layer, including the two this one is modelled on. Not a property of this change.
