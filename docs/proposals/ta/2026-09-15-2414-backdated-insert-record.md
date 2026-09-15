# #2414 item 2 — record the bar that appeared BEHIND the frontier

Status: proposal, 2026-09-15. Sibling of `2026-09-15-2414-bar-revision-record.md`
(shipped `22464e00`, `sql/387`) and `2026-09-15-2414-revision-cause-attribution.md`
(shipped `412997fc`).

## The gap, in `sql/387`'s own words

`sql/387_price_daily_revision.sql`'s header names this slice and declines it:

> ⚠⚠ WHAT THIS IS NOT: "THE CORPUS MOVED". It records exactly one mutation class —
> an OHLCV overwrite. A decision's consumed inputs also change when: a HISTORICAL
> BAR IS INSERTED (stale re-observation, interior gap fill). `_upsert_candles`
> counts those as `inserted` and they produce NO row here, yet they move every
> later recursive indicator, the segment calendar and next-bar fill identity.

So the shipped table is a partial answer to its own question, by design and on the
record. This closes the other half.

## Source rule

There is **no published or external rule** here and this document says so rather
than inventing a citation. The governing rules are our own, and they are cited
rather than reasoned out:

1. **The radius of a corpus move is unbounded.** `atr_series` and `adx_series` are
   Wilder recursions — seeded once with a simple average at bar `period`, then
   `current = (current × (period − 1) + tr) / period` for every later bar
   (`app/services/indicator_series.py:435-481`). The value at bar *t* depends on
   **every** bar from the series start, with geometric decay, not on a rolling
   window. Measured on #2414 (`3a12cfc0`): S-4's largest observed radius is 151
   bars against a `WARMUP_BARS` of 113, S-8's 81 against 27. **There is no
   structural cut-off.**
2. **Therefore a bar appearing anywhere at or before a decision's bar date is a
   corpus move for that decision** — including one appearing *below* the oldest
   stored bar, which re-seeds the recursion.
3. **A revision row must live in the bar write's transaction** (`sql/387` header,
   and #1293 for the inverse rule on counters). Unchanged here.

## ⚠⚠ The discriminator is NOT "interior"

"Interior gap fill" (the phrase in `sql/387`'s header, and in this ticket's own
prose) is too narrow. `force_backfill` deepens an instrument from 400 stored bars
to 1000 — 600 inserts **below** the existing minimum. By rule 1 above every one of
them changes every later indicator value, because they change the Wilder seed.

The correct test is **"not a forward extension"**:

```
backdated  ⟺  bar.price_date < last_bar_before
```

- `last_bar_before is None` (no prior history) ⇒ **no bar is backdated**. Initial
  backfill of 1000 bars produces zero rows, which is right: nothing had been
  decided against.
- Equality is unreachable. A bar on exactly `last_bar_before` conflicts, so it is a
  revision or an `IS DISTINCT FROM` no-op, never an insert.

⚠ **The frontier is the one held at TRANSACTION START, and must not be advanced
inside the call.** Within one `_upsert_candles` call the stored maximum moves as
bars land, so a provider payload ordered `[09-15, 09-12]` would classify `09-12`
as backdated under a moving frontier and as an extension under a fixed one. The
fixed one is correct: the question the table answers is *"did a bar appear behind
the frontier that already-written decisions were made against"*, and decisions read
**committed** state. Nothing inside an uncommitted transaction was ever decided
against, so intra-call ordering must be invisible. `last_bar_before` is already
read before the `with conn.transaction():` block (`market_data.py:741`), so the
fixed value is the one we have.

## Premise checks (run before this document was written)

**1. The class is real by construction.** `_candles_fetch_count`'s docstring
(`market_data.py:1048-1099`) names it: `stale_reobservation` exists because *"a
3-bar incremental fetch here would silently leave a history gap; falling back to
`default` closes the gap"*. Closing a gap is a backdated insert. The ordinary
`incremental` path reaches it too — the same docstring: *"the incremental window is
a CALENDAR-gap test on the NEWEST stored bar only … an incremental fetch is bounded
in BARS and not in days"*, so a sparse name's second or third provider bar can be a
date absent locally.

**2. The input costs nothing.** `last_bar_before = _last_bar(conn, instrument_id)`
already exists at `market_data.py:741` (added for #2262's supply marker) and is not
passed into `_upsert_candles`. Same "already in hand" property that made the age
histogram and `price_daily_revision` free.

**3. Prevention-log neighbour, same class, reached from the scan side.**
`docs/review-prevention-log.md:3844` — `strategy_signal_scan` reads the frontier in
one aggregate then loads per instrument, so *"an instrument can gain a bar between
the two reads"*, and *"under a ledger with no `ON CONFLICT` there is no repair short
of a version bump"*.

## Full-population verification

Run 2026-09-15 against the dev corpus. **Not a sample.**

```sql
with cal as (
  select price_date from price_daily group by price_date having count(*) >= 500
), span as (
  select instrument_id, min(price_date) lo, max(price_date) hi, count(*) n
  from price_daily group by instrument_id
), g as (
  select s.instrument_id,
         (select count(*) from cal where cal.price_date between s.lo and s.hi) - s.n as gaps
  from span s
)
select (select count(*) from cal) as calendar_days,
       count(*) as instruments,
       count(*) filter (where gaps > 0) as with_gaps,
       coalesce(sum(gaps) filter (where gaps > 0), 0) as total_gaps,
       max(gaps) as worst
from g;
-- 1131 | 12284 | 11287 | 449533 | 878
```

`price_daily` holds **7,012,719 bars across 12,284 instruments**, 2019-12-16 →
2026-09-15. **11,287 instruments (91.9%)** have at least one gap against a
≥500-instrument-coverage derived calendar; **449,533** gap-dates in total; worst
single instrument 878.

⚠⚠ **That is an UPPER BOUND and it over-counts.** A foreign listing legitimately
has no bar on a US-only session and the provider will never supply one. It is the
right figure for **sizing** the table (~450k rows in a hypothetical total closure
wave, tens of MB, one-off) and the **wrong** figure to quote as "gaps we are
missing". `instruments.exchange` stores numeric codes (`'4'`, `'5'`, `'7'`…), not
names, so an exchange-scoped refinement needs the code map first.

For contrast, `price_daily_revision`'s own measured rate was **4,080 revisions over
2026-08-23 → 2026-09-14**. The two classes differ by roughly two orders of
magnitude in a first closure wave. `sql/387`'s *"different volume profile"* is
therefore **correct as measured** — and it is a **retention** question, not a
schema-identity one.

## Shape — rename and widen, not a sibling table

`sql/387`'s header objects to widening on one specific ground: *"widening this table
to carry it would make its name a lie"*. That objection is about the **name**, and a
rename answers it directly.

**`price_daily_revision` → `price_daily_mutation`, plus `mutation_kind`.**

Decided this way rather than as a sibling table because **the only consumer that
will ever read either class needs both, always, together**. #2414's remaining
supersession half asks "did anything under decision *D* move?", and a two-table
shape guarantees every future reader either writes a `UNION` or forgets one class —
and forgetting one class is the exact defect this slice exists to close. When a
consumer always unions two tables, they are one table.

The `cause` vocabulary is unchanged and applies identically to both kinds: it is the
**write branch**, not an economic cause. ⚠ `initial_backfill` can never co-occur
with `backdated_insert` (no prior history ⇒ no frontier ⇒ no backdated bar). That is
asserted in a **test**, deliberately **not** a cross-column `CHECK`: this table is
written inside the bar-write transaction, so a constraint violation here would roll
back a **price write**. An audit row must never be able to destroy the thing it
audits.

```sql
-- sql/388
ALTER TABLE price_daily_revision RENAME TO price_daily_mutation;
ALTER TABLE price_daily_mutation RENAME COLUMN revision_id TO mutation_id;
ALTER TABLE price_daily_mutation RENAME COLUMN revised_at  TO mutated_at;

ALTER TABLE price_daily_mutation
  ADD COLUMN mutation_kind TEXT NOT NULL DEFAULT 'revision'
  CHECK (mutation_kind IN ('revision', 'backdated_insert'));
ALTER TABLE price_daily_mutation ALTER COLUMN mutation_kind DROP DEFAULT;
```

The `DEFAULT` then `DROP DEFAULT` is deliberate and is not redundant: the table is
empty **on this box today**, but a sweep can write to it between `sql/387` and
`sql/388` on any other checkout, and `ADD COLUMN NOT NULL` with no default fails on
a non-empty table. Dropping it afterwards means a writer that forgets to set the
kind fails loudly instead of silently labelling an insert a revision.

Index: existing indexes follow the rename automatically. The supersession consumer's
access path is `(instrument_id, price_date)`, which `sql/387` already provides, and
`mutation_kind` is a low-cardinality filter on top of it — no new index until a
consumer exists (KISS; `sql/345`'s precedent is that an index gets added when a
measured plan asks for it).

## Writer change

`_upsert_candles` gains `last_bar_before: date | None` and returns
`backdated_insert_dates: tuple[date, ...]` on `CandleUpsertOutcome`, classified at
the same `row[0]` branch that already separates insert from revision. Cost: one
comparison per bar, no query, no column read.

`_record_bar_revisions` becomes `_record_bar_mutations(conn, instrument_id, *,
revised_bar_dates, backdated_insert_dates, cause)` — one `executemany` per kind,
inside the same transaction, same rollback property as today.

⚠ `backdated_insert_dates` **cannot repeat a date within one call** (the second
occurrence of a duplicated date conflicts and becomes a revision or a no-op),
unlike `revised_bar_dates`, which can. The table stays append-only with no
`UNIQUE`, because across calls the same date can be inserted, deleted and inserted
again.

## What this does NOT do

- It does **not** locate affected `strategy_signals` rows. The join that would
  consume it was specced, taken to Codex ckpt-1 and **killed** on ten grounds,
  enumerated in `sql/387`'s header. Adding a second mutation class does not revive
  it: the missing piece is a **read-set record**, and `created_at` is the ledger
  **write** time.
- It does **not** cover the two non-OHLCV classes `sql/387` also names —
  `price_bar_quarantine` verdict changes and `price_series_break` resolution, both
  of which change what `load_masked_bars` yields with the OHLCV untouched.
- It does **not** touch `strategy_signals_unique`, the ledger key, or any
  supersede-and-record path. #2414's items 1, 3 and 4 stay open and unprejudged.
- It records a **row deletion** nowhere. `price_daily` has no delete path today
  (grepped: no `DELETE FROM price_daily` outside test fixtures), so this is a named
  absence rather than an oversight.

## Tests

Pure-logic first, per the repo's test-tiering rule:

1. `last_bar_before is None` ⇒ 1000 inserts, **zero** backdated rows.
2. A bar strictly below `last_bar_before` ⇒ one backdated row; the same bar again
   with a changed value ⇒ one **revision** row, not a second insert.
3. A bar strictly above ⇒ zero rows of either kind.
4. Payload ordered `[frontier + 3, frontier − 1]` ⇒ the second is backdated and the
   first is not, proving the frontier is **fixed** and not advanced mid-call.
5. `initial_backfill` never co-occurs with `backdated_insert`.
6. `mutation_kind`'s Python vocabulary equals the SQL `CHECK` set — the same shape
   as the existing `RevisionCause` test, so a member added in one place fails a test
   rather than a production INSERT.
7. One DB-tier test that the mutation row rolls back with a failed bar write, for
   the insert kind (the revision kind already has one in
   `tests/test_market_data_bar_revision_record_db.py`).

## Acceptance

A `daily_candle_refresh` sweep on new code writes `mutation_kind =
'backdated_insert'` rows for at least one instrument, **or** the census reports zero
and says why (`price_daily_mutation` empty cannot distinguish "no bar appeared
behind a frontier" from "the writer has not run" — the same refusal
`verify_2414_revision_exposure.py --census` already prints).
