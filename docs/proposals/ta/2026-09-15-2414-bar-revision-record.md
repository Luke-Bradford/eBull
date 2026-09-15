# #2414 — record the bar revision at the moment the writer makes it

Status: proposal. Scope: one append-only table, its writer, and a census. **No decision on
#2414's fix shape**, no change to `strategy_signals_unique`, no supersede path, no
re-decide, and — after checkpoint 1 — **no ledger-exposure join**. See §"The join this spec
does not ship".

## The question, and the two dead ends already signposted

#2414 item 2 is *"detecting that a stored row's corpus has moved"*. Two passes have tried to
answer it by **comparison**, and both are recorded as dead:

- `d086db51` — `strategy_scan_watermark.corpus_generation` cannot carry it.
  `CorpusGenerationBuilder.finish()` hashes `frontier_date` and `_ADVANCE_WATERMARK` only
  updates on a strictly greater frontier, so every advance rotates the digest and the
  comparison is a constant `TRUE`.
- The same comment's general result: **no pass-scoped digest can answer it**, because
  stripping `frontier_date` still leaves the pass reading one more day of bars than the
  previous one. It priced the alternative — a **prefix-scoped** digest folded over bars
  `<= previous_frontier` — against the pass's measured 3,364,930 bars.

Both framings assume detection must be a comparison performed later. It does not have to be.
`_upsert_candles` already separates an INSERT from a revision per bar
(`RETURNING (xmax = 0) AS was_insert`, `market_data.py:1335`), and at that instant it holds
`(instrument_id, bar.price_date)` — the same "already in hand" property the age histogram
was built on (`market_data.py:1266-1270`):

> ⚠ The age histogram is FREE: the revised bar's `price_date` is already in hand, so it
> costs no query and no column.

So **one** mutation class becomes recorded rather than inferred, at no extra read.

## ⚠⚠ What this table is, stated narrowly on purpose

`price_daily_revision` records: *this instrument's stored OHLCV for this bar date was
overwritten at this transaction time, by this write branch.* That is all.

It is **not** "the corpus moved". Checkpoint 1 enumerated at least three other ways a
decision's consumed inputs change with no revision row:

1. **INSERTS of historical bars.** A stale re-observation or gap closure adds a bar
   *interior* to an existing series, which moves every later recursive indicator, the
   segment calendar and next-bar fill identity. `_upsert_candles` counts these as
   `inserted`, and this table does not record them.
2. **Quarantine and segmentation changes.** `price_bar_quarantine` verdicts and
   `price_series_break` resolution change what `load_masked_bars` yields with the OHLCV
   untouched.
3. **Rule-set changes** — already covered by `input_rule_set_versions`, and named here so
   the three are not conflated.

(1) is a genuine gap in #2414's coverage and is recorded on the ticket rather than absorbed
here: capturing it is a different decision with a different volume profile, and widening
this table to carry it would make its name a lie.

## Source rule

No external source rule governs this table — it is our own write-path telemetry about our
own table, not an SEC/EDGAR treatment or a published quant formulation. Per `.claude/CLAUDE.md`
("where a published formulation genuinely does NOT exist … say so explicitly and fix the
rule by construction"), the construction is below and the one free parameter (append-only vs
collapsed) is decided with its reason.

⚠ The **closed vocabulary** for `cause` is not free: it is `RevisionCause`
(`market_data.py:1076-1090`), imported rather than restated, so the `CHECK` and the `Literal`
cannot drift.

⚠ No indicator formulation is relied on by this spec. The first draft leaned on Wilder's
recursion to claim an exact ledger join; that claim is withdrawn (below), and with it the
need to cite the formulation as a governing rule.

## Premise checks — run before this spec was written

- **No per-bar revision record exists.** `price_adjustments` is a corporate-action ledger
  holding **0** rows; `price_series_break` (**412**) records a *shape detected in the data*,
  not a write event; `price_bar_quarantine` (**34,890**) records usability verdicts.
  `price_daily` has no audit column, stated at `market_data.py:1281-1284`: *"`price_daily`
  has no audit column, so after this statement nothing can establish that the bar ever held
  a different number."* (Counts as of 2026-09-15; the census arm below recomputes the table's
  own figures at run time so none of them become hand-maintained.)
- **Write placement is safe.** `refresh_market_data` requests `autocommit=True` and **warns**
  rather than refuses when it does not get it (`market_data.py:654`). Under autocommit each
  `with conn.transaction()` is a genuine `BEGIN`/`COMMIT`; under an already-open outer
  transaction the block degrades to a savepoint. ⚠ Both are atomic with respect to the bar
  write, which is the only property this spec needs — the revision row cannot outlive a
  rolled-back bar in either mode.
- **Duplicate bar dates in one payload are reachable.** `_normalise_candles`
  (`etoro.py:537`) flattens every group's inner array with no date dedup and no check that
  the group's `instrumentId` matches the one requested. One `_upsert_candles` call can
  therefore revise the same date twice, including an A→B→A pair with no net change.
  Append-only records both events; the census names the count so it cannot be mistaken for
  two distinct bars.

## Schema

`sql/387_price_daily_revision.sql`:

```sql
CREATE TABLE IF NOT EXISTS price_daily_revision (
    revision_id   BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    price_date    DATE        NOT NULL,
    revised_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    cause         TEXT        NOT NULL CHECK (cause IN (
                      'initial_backfill', 'stale_reobservation', 'incremental',
                      'adjustment_heal', 'force_backfill', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_price_daily_revision_instrument_date
    ON price_daily_revision (instrument_id, price_date);
```

### Decisions, each with its reason

1. **Append-only; no `UNIQUE (instrument_id, price_date)`.** A bar can be overwritten more
   than once and each overwrite is a distinct event with its own time and branch. Collapsing
   to one row per bar with `last_revised_at` would answer a single-threshold question
   equally well and destroy the per-event history. Append-only is chosen because the
   question set is open, not for generality's sake.
2. **`revision_id` surrogate PK, no natural key.** Duplicate dates within one call are
   reachable (above), so a natural key would raise on a payload the writer must tolerate.
3. **No magnitude, no prior value, no changed-field set.** ⚠ Corrected from the first draft:
   `RETURNING OLD.*` is PostgreSQL 18 and this cluster is 17.9, **but that is not the only
   mechanism** — a row trigger exposes `OLD` on any version, and this repo already uses one
   (`sql/047_coverage_status_events.sql:40`). The reason is cost, not availability: a row
   trigger fires per updated row on a path that can write 500k rows in one stale sweep, and
   a prior-value `SELECT` or CTE doubles the index probes there. The row records **that** the
   value changed and **when**. ⚠ The consequence is explicit: a **volume-only** revision is
   indistinguishable from a close revision in this table, and the `IS DISTINCT FROM` guard
   in `_upsert_candles` includes `volume`.
4. **`cause` is a BRANCH and an UPPER BOUND on attribution.** It reuses `RevisionCause`
   verbatim and carries `revision_cause()`'s warning into the migration header: one fetch can
   rewrite bars for more than one underlying reason at once, including inside a heal.
   `adjustment_heal` must not be read as "this revision was a split". The `CHECK` is the
   closed-vocabulary guard — an unrecognised cause must be loud, not folded into a real one.
5. **FK is `NO ACTION`, deliberately unlike `strategy_signals`.** `strategy_signals`
   cascades on instrument delete; `price_bar_quarantine` — the closest sibling, per-bar and
   instrument-keyed in the same layer — does not. An audit record of a write that happened
   should not be silently erased by a later delete; if an instrument delete ever blocks on
   this table, that is the correct signal to look at.
6. **Append-only is a CONVENTION, not an enforced constraint.** The DDL permits `UPDATE` and
   `DELETE`. Stated rather than implied: no trigger or grant enforces it, and nothing in this
   spec claims otherwise.
7. **`revised_at` defaults to `now()`** — transaction start time under PostgreSQL. ⚠ It is
   the time of the WRITE TRANSACTION and is **not** a read-visibility oracle: it cannot tell
   any reader which version of a row that reader observed. See the join section below, which
   is where that mattered and why it is not shipped.
8. **No retention policy.** Growth tracks real change, not fetch volume, because
   `IS DISTINCT FROM` suppresses identical re-observations. ⚠ That bounds it against
   *re-fetching*, not against mass heals, outages or oscillation, and no such bound is
   claimed. The census prints the table's own size and window so the decision can be made on
   measurement if it ever needs making.

## Writer

`_upsert_candles` gains one field on `CandleUpsertOutcome`:

```python
#: The `price_date` of every bar this call OVERWROTE, in the order seen (#2414).
#: Empty when none. May contain the same date twice — see `_normalise_candles`.
#: The caller writes these with the branch that produced them; `_upsert_candles`
#: does not know the branch.
revised_bar_dates: tuple[date, ...]
```

The caller (`refresh_market_data`) writes them **inside** the existing
`with conn.transaction():` block, immediately after `_upsert_candles` returns. All three
`revision_cause` inputs are already final at that point: `adjustment_detected` is set earlier
in the same block, and `force_backfill` / `fetch_reason` are computed before it.

### ⚠ Inside the transaction, unlike every existing counter — deliberately

The running totals merge **after** the `with` block, and the code says why
(`market_data.py:786-793`, #1293): an instrument whose feature-compute or commit later
raised did not revise anything, and a counter incremented inside the block would over-report
it. A table row does not have that problem — it rolls back with the write it describes.
Inside is therefore **stronger** than the counters, not inconsistent with them, and is the
only placement under which the table cannot claim a revision `price_daily` does not have.

⚠ One residual, from checkpoint 1: a connection loss *while receiving* `COMMIT`'s result
raises in Python on a transaction the server committed. Atomicity between the two tables
still holds — they commit or roll back together — so the table stays consistent with
`price_daily`; it is the in-process counters that can then disagree with both. That is
pre-existing and unchanged by this spec.

### Fewer round trips, not one statement

`_record_bar_revisions` uses `executemany`. ⚠ Corrected from the first draft: `executemany`
executes the command repeatedly (pipelined where supported) — it reduces **round trips**, not
per-row server execution, index maintenance or WAL. The saving is the round trips, which is
what matters inside a per-instrument transaction on a sweep that already runs 17-34 minutes
and where a heal can revise up to `lookback_days` (1000) bars.

## The join this spec does not ship

The first draft proposed an "exact" ledger-exposure query:

```sql
-- NOT SHIPPED. Retained because the reasons are the result of this pass.
SELECT count(*) FROM strategy_signals s JOIN price_daily_revision r
  ON r.instrument_id = s.instrument_id
 AND s.signal_bar_date >= r.price_date
 AND s.created_at      <  r.revised_at;
```

It rested on `3a12cfc0`'s finding that `atr_series` / `adx_series` are Wilder recursions with
no finite window, so a revision at bar `D` reaches every later decision. **Checkpoint 1
falsified the exactness in both directions**, and a number that is neither an upper nor a
lower bound is worse than no number:

**Overcounts** — it would claim rows the revision cannot have touched:

- `strategy_segmented_evaluation` restarts indicator state per segment, so a revision before
  a signal's segment need not reach it.
- `load_masked_bars` excludes uncovered dates and masks fields; revising an already-masked
  value leaves the consumed input unchanged.
- The table cannot say **which field** changed (decision 3), so a volume-only revision looks
  identical to a close revision.
- `signal_bar_date >= price_date` does not establish the bar **existed** when the signal ran;
  a bar backfilled and later revised matches anyway.
- Not every strategy has unbounded memory — the Wilder argument covers ATR/ADX, not the
  SMA-window strategies also present in the ledger's 8 ids.

**Undercounts** — it would miss real exposure:

- Regime is computed on the **benchmark**, so a benchmark revision reaches every
  regime-gated instrument. Equality on `instrument_id` excludes all of them. ⚠ This one is
  self-inflicted: `3a12cfc0`'s own design note says the regime is a benchmark property, and
  the join was written as if it were not.
- Cross-sectional ranking evaluates instruments together, so revising A can change B.
- `resolve_fills` reads the **next** bar's open, so a revision at `signal_bar_date + 1` moves
  a stored fill — and `signal_bar_date >= price_date` excludes it by construction.
- Historical INSERTs move consumed inputs with no revision row at all (§"What this table is").

**And the time predicate is not sound regardless.** `created_at` is the ledger **write**
time, not the bar **read** time — the scan loads and computes before opening its write
transaction. Both orderings are reachable: read-old → revision commits → signal inserted
(missed, `created_at > revised_at`), and revision begins → scanner reads old committed bars →
signal inserted → revision commits (missed again). Under Read Committed a transaction
timestamp does not encode which row versions a reader observed, so no wall-clock column on
either table fixes this. It needs a **read-set** record, which is the prefix-digest shape
`d086db51` already priced — not a timestamp comparison.

**So the population stays unlocatable, and the reason is now specific rather than "an
aggregate carries no identity".** The per-entity identity was the missing *input*; it was not
sufficient on its own. That is the narrowing this pass contributes, and it is recorded on the
ticket.

## Read

`scripts/verify_2414_revision_exposure.py --census`, joining the existing `verify_2414_*`
family. **Census only** — the table's own state, no ledger join:

- row count, distinct instruments, distinct `(instrument_id, price_date)` pairs (so repeat
  revisions of one bar are visible as repeats, not as breadth);
- the observed window (`min`/`max` of `revised_at`) and the elapsed span, with rows-per-day
  **computed from those two** rather than quoted;
- the by-cause split;
- the count of bars revised more than once, and the largest repeat count.

Read inside one `REPEATABLE READ` read-only transaction so every figure describes one
snapshot, matching `verify_2414_revision_invariance.py`.

⚠ **An empty table does not prove "no revisions occur".** Before the first post-deploy sweep
the window is undefined, and an empty table cannot distinguish *no changes* from *writer not
running*. The census therefore prints the window explicitly and states that a zero is
uninterpretable without it — the `d6f506b3` lesson (*a zero is evidence of non-occurrence
only after you have shown the writer exists and runs*) applied to the surface it came from.
⚠ Nor is a zero guaranteed on shipping day: `daily_candle_refresh` runs hourly, so a sweep
may populate the table within the hour.

## What this deliberately does NOT do

- **No supersession.** No ledger row is rewritten, retracted or marked.
  `strategy_signals_unique` is untouched; `store_signals` still has no `ON CONFLICT`.
- **No re-decide.** Spec §11's cold-start rule is not engaged.
- **No backfill.** Bars revised before this ships left no trace anywhere and never can. ⚠
  Corrected from the first draft: that is a gap in the **revision history**, not in the
  signals — a row written in 2026-01 can perfectly well be reached by a revision recorded
  tomorrow.
- **No gate, no refusal, no threshold.** Nothing consumes the table in a decision path. Both
  candidate fix shapes in #2414's body stay open.

## Tests

- `_upsert_candles` returns exactly the revised dates — an INSERT and a no-op (blocked by
  `IS DISTINCT FROM`) each contribute nothing, and `len(revised_bar_dates) == revised`.
- A duplicate date within one payload yields two entries, in order.
- One row per revised bar carries the caller's cause, and the heal branch reports
  `adjustment_heal` rather than `incremental` (the ordering `revision_cause()` pins).
- A transaction that raises **after** the upsert leaves no `price_daily_revision` row and no
  `price_daily` change — the property the placement decision rests on, revert-probed.
- A cause outside the closed vocabulary is rejected by the `CHECK`.
- `RevisionCause`'s members and the `CHECK`'s vocabulary are asserted equal, so adding one in
  Python without the migration fails a test rather than a production insert.
