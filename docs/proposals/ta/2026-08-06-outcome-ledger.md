# Phase 4b — the outcome ledger

Parent design: `docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md`
§5 phase 4. Resolver: `docs/proposals/ta/2026-08-06-outcome-resolver.md` (4a) —
its §6 is the ticket split and closes the identity question this phase turns on.
Ledger: `docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md`
(3a/3b/3c). Execution semantics:
`docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §3.5 and §5
criteria 2/8/9/11.

Refs #2240, #2245, #2288.

## 1. Scope

4a classifies one filled entry and returns an `Outcome`. Nothing stores it. This
phase is the storage boundary: the `strategy_outcomes` table, the writer, the
selector that finds which stored fills still need resolving, and the bridge from
a stored fill date to a series index.

Not in this phase:

- **Computing outcomes.** 4a owns the walk. This phase must not re-derive a
  class, a level or a return; it stores what the resolver returned.
- **Levels, costs, exposure.** Unchanged from 4a §1.
- **The runner.** Loading a series, computing indicators and calling the
  resolver per pending fill is phase 5's loop. ⚠ It cannot live here: the loop
  needs the strategy's parameters, and those exist only in code — the ledger
  stores `strategy_version`, a hash, not the params. 4b supplies the two ends
  and the bridge; §5's harness drives them with a fixed rule set, which is a
  test double of that loop and lives in `scripts/`, not `app/`.

## 2. The key, and the question 4a left open

4a §6 settled two things, and they are not re-argued here:

- **Level parameters live in `strategy_version`** and must not be re-declared on
  the outcome row, where they could disagree with it.
- **The resolver's own `rule_set_version` is in the key** — it is this module's
  source hash and is NOT inside `strategy_version`, so a changed execution
  assumption must be able to produce a second outcome for the same signal
  without overwriting the first.

### 2.1 A third key member the resolver cannot see

⚠ **`rule_set_version` alone is not enough, because the resolver is not the only
thing that decides an outcome.** Its inputs do too: `load_masked_series` masks
`high`/`low`/`close` according to `price_quarantine`'s rule set, and 4a's whole
masking contract (§3.5) is that an absent field REFUSES. Re-run the quarantine
under a changed rule set and the same signal can resolve differently with
`outcome_resolver.py` byte-identical.

Under a two-part key that is not merely unrecorded, it is **unfixable**: the
re-resolution collides on `(signal_id, rule_set_version)`, and with no
`ON CONFLICT` the operator cannot store the corrected outcome at all without
touching the resolver's source to move its hash.

So the key is **`(signal_id, rule_set_version, input_rule_set_version)`**, where
`input_rule_set_version` is the version stamp of the pipeline that produced the
BARS — `price_quarantine`'s `RULE_SET_VERSION` for a masked read.

⚠ It is `TEXT NOT NULL` with **no CHECK vocabulary**, exactly as
`strategy_version` is: a version string is open by nature, and a CHECK would
have to be widened by every producer. ⚠ It is a **required writer argument with
no default** (#2288's rule: a field with a default is a field a writer can
forget), and a caller reading bars from an unversioned path must say so
explicitly rather than pass nothing.

### 2.2 Does `resolution_method` join the key?

4a §6 left this open. ⚠ **v1 answer: no**, and the trigger to revisit is stated
rather than left to memory. The argument is not "there is only one method
today" — that is true and is not a reason, since the point of a key is to
survive the second case. It is that the two designs fail in different
directions:

| design | what happens when an intraday method arrives |
| --- | --- |
| method NOT in key (v1) | a second resolution of the same signal, at the same resolver and input versions, **collides**. With no `ON CONFLICT` that is a `UniqueViolation` — loud, at the moment of the write |
| method IN key | both rows store cleanly, and **every aggregate over the table double-counts** unless it filters on method. A win rate summing a daily and an intraday resolution of one trade is wrong and looks fine |

Loud beats silent, so v1 excludes it. The collision is reachable rather than
hypothetical: S5 (#2245) established that a forward-going signal is resolvable
intraday inside a ~2-session window and a historical one never is, so one module
could legitimately pick per signal.

⚠ **The loudness only holds within one pinned key.** A new intraday-capable
resolver has a different source hash, so its rows coexist with the old daily
ones and an unpinned aggregate double-counts across versions. That is not
specific to `resolution_method` — it is true of ANY two resolver versions, and
it is the same property `strategy_version` has in the ledger. **Every aggregate
over this table must pin one `(rule_set_version, input_rule_set_version)` pair.**
Stated here because it is the standing consumer obligation phase 6 inherits.

**Trigger to revisit:** the first resolver version that can emit a
`resolution_method` other than `daily_bar`. The choice then is either (a) the
method joins the key AND every consumer gains a method filter, or (b) the
resolver picks one method per signal deterministically and the key stays. Both
are real decisions; neither is a migration written in passing.

`resolution_method` is stored regardless — 4a §2: *"without the stamp a later
intraday-backed resolution mixes silently into the same statistics."* Storing it
and keying on it are different questions.

## 3. Storage shape

One row per `(signal_id, rule_set_version, input_rule_set_version)`. Columns are
4a §3.7's recorded fields, minus §3.1's two, plus §2.1's.

⚠ Everything except the four nullable payload fields (`reason`,
`exit_bar_date`, `exit_price`, `bars_held`, `gross_return_pct`) is `NOT NULL`:
`signal_id`, both version columns, `outcome`, `resolution_method`, `created_at`.
A nullable key member would make the uniqueness constraint stop discriminating,
since `NULL <> NULL` in a unique index.

### 3.1 What is deliberately NOT stored

| field | why not |
| --- | --- |
| `exit_index` | 4a §3.7: *"an index is not durable across a corpus rebuild"*. The date is. Storing both would let them disagree after a re-adjustment, and the index is the one that would be wrong |
| `entry_price` / levels / `max_hold_bars` | `strategy_signals.fill_price` already holds the entry, and the levels are inside `strategy_version` (§2). Re-declaring either creates a second source of truth |
| `universe` | on the signal row (#2288), one join away. Duplicating a labelling column is how the label and the data drift apart |
| `instrument_id`, `signal_bar_date` | on the signal row. `signal_id` is the FK |

⚠ Consequence, stated because it is the cost of the above: **every read of this
table is a join.** "The outcome distribution for strategy X version Y" cannot be
answered from `strategy_outcomes` alone. That is the right trade — a
denormalised copy of the identity columns is a copy that can disagree with the
ledger — but a phase-6 surface wanting a wide row should build a VIEW, not add
columns.

### 3.2 The constraint set

Mirrors 4a's `Outcome.__post_init__`, in the same direction 3b/3c mirror
`sql/255`: the Python validator fails at construction naming the field, the
CHECKs are the backstop for any writer that bypasses it.

1. `outcome` ∈ the five classes; `resolution_method` ∈ `{daily_bar}`; `reason` ∈
   the four `unresolved` reasons. Closed vocabularies, restated in SQL because
   SQL cannot import a `Literal` — and **pinned by a contract test** that parses
   each CHECK out of the migration text and compares it to the Python constants.
   ⚠ Not optional: prevention log, *"A closed vocabulary declared in three
   places is validated in none of them"* and *"A widened CHECK constraint
   applies clean while silently dropping a member"*.
2. `reason` present **exactly** when `outcome = 'unresolved'`.
3. `exit_bar_date` and `bars_held` both present **exactly** when
   `outcome <> 'unresolved'`, and move together.
4. `exit_price` and `gross_return_pct` both present **exactly** when
   `outcome IN ('tp_hit','sl_hit','expired')`, and move together. ⚠ `ambiguous`
   carries a location but no price: 4a §3.7 — *"a return column that is
   populated for them is a column something will eventually average."*
5. `bars_held >= 0`. ⚠ **0 is legal** — a TP or SL touched on the fill bar. Not
   exposure time; criterion 7's exposure metric is phase 5's.

⚠ Constraints 3 and 4 are written as `(a IS NULL) = (b IS NULL)` plus a
predicate on `outcome`, not as `a IS NOT NULL AND b IS NOT NULL`. The Python
mirror COUNTS the fields for the same reason: `A IS NOT NULL AND B IS NOT NULL`
is three states in SQL and two in Python, which is exactly the mirror defect
Codex caught at 3c's checkpoint 2 (prevention log, #2240 3c).

**Two invariants SQL is NOT given, stated rather than assumed:**

- ⚠ `bars_held` cannot be re-derived without the series (trading days are not
  calendar days, and `exit_index` is deliberately not stored). SQL bounds it at
  `>= 0` and no more.
- ⚠ `gross_return_pct` is not re-derived from
  `(exit_price − strategy_signals.fill_price) / fill_price` in a CHECK. The
  resolver computes it in `Decimal` under a 28-digit context; a NUMERIC
  re-derivation would differ in scale, and a CHECK that is *nearly* true is
  worse than none. It is instead cross-checked **on the full population** by
  §5's acceptance 7, which is a measurement rather than a guess about tolerance.

No CHECK bounds `exit_price` or `gross_return_pct` from below in v1. Both have
an obvious-looking floor (`exit_price > 0`, `gross_return_pct > -1`) that holds
only while every bar's open is positive — which is `price_quarantine`'s
business, not this table's, and which a CHECK would silently start enforcing on
a code path nobody watches (#2218's lesson). §5 acceptance 7 reports the minimum
of each over the full population instead; adding the CHECK is a decision for
whoever reads that number, not a default.

### 3.3 The FK, and closing the gap it leaves

`signal_id BIGINT NOT NULL REFERENCES strategy_signals(signal_id) ON DELETE CASCADE`.

⚠ CASCADE is right here and does not contradict the prevention-log rule against
cascading into audit tables: an outcome is **derived from** its signal, not an
independent record of an action. A signal deleted with its outcomes left behind
is an orphan no query can interpret.

⚠ **A plain FK does not prove the parent was a FIRED ENTRY.** 4a §1 excludes
`signal_kind = 'exit'` rows, and a `not_fired` / `not_evaluable` signal has no
fill to resolve — but the FK admits all of them, and a CHECK cannot read the
parent row.

Three ways to close it:

| option | cost |
| --- | --- |
| composite FK — carry `signal_kind` + `verdict` on the outcome row, reference `(signal_id, signal_kind, verdict)`, CHECK them to `('entry','fired')` | two redundant TEXT columns on every row of a table that scales with bars × strategies |
| writer validates, constraint does not exist | a check-then-write, and the invariant lives only in one function's prose |
| **the INSERT is a SELECT from the parent, with the predicate in it** | one join per row, no extra columns |

**Taken: the third.** The writer inserts
`… SELECT … FROM strategy_signals s WHERE s.signal_id = %(signal_id)s AND
s.signal_kind = 'entry' AND s.verdict = 'fired' AND (exit_bar_date IS NULL OR
exit_bar_date >= s.fill_bar_date)`. A non-qualifying parent inserts **zero
rows** rather than a bad one, and it is race-free — the predicate is evaluated
inside the writing statement, not before it.

⚠ **Zero rows is silent, so the writer must make it loud**: `store_outcomes`
compares the batch rowcount to `len(rows)` and raises on any shortfall, then
runs a diagnostic query naming the offending `signal_id`s and why. A count that
merely returns short is the same silent-narrowing failure the ledger's
`no ON CONFLICT` rule exists to avoid.

That predicate also closes the second cross-table invariant for free: **an exit
cannot precede its fill.** `exit_bar_date >= fill_bar_date` — `>=` because
`bars_held = 0` is legal (a level touched on the fill bar).

## 4. The writer

`app/services/outcome_ledger.py`, mirroring `signal_ledger.py`'s shape.

### 4.1 `OutcomeRow` and `store_outcomes`

`OutcomeRow.from_outcome(signal_id, outcome, input_rule_set_version)` drops
`exit_index` and carries everything else through unchanged. ⚠ `rule_set_version`
and `resolution_method` come **from the `Outcome`**, never re-stamped by the
writer: a writer that stamped its own version could store an outcome under a
version that did not produce it, which is the one thing the key exists to
prevent.

`store_outcomes(conn, rows) -> int`, `executemany` over §3.3's INSERT-SELECT,
**no `ON CONFLICT`** — the argument is `store_signals`', unchanged: `DO UPDATE`
lets a re-run overwrite a recorded classification; `DO NOTHING` keeps the old
row when the new one *disagrees*, and given a fixed key a disagreement means the
corpus moved under us, which is the one case worth hearing about.

⚠ `rowcount` is checked for psycopg3's `-1` sentinel before being used as a
count (prevention log), and every parameter in the SELECT list carries an
explicit cast — an untyped `NULL` parameter in that position is psycopg3's
`AmbiguousParameter` (prevention log, #1961).

⚠ The writer raises rather than returning a short count, so the caller's
transaction is left to be rolled back by the caller. `store_outcomes` does not
own the transaction, exactly as `store_signals` does not.

### 4.2 The bridge: a stored fill is a DATE, the resolver wants an INDEX

`locate_fill_index(series, fill_bar_date) -> int` raises when the date is not in
the series, naming the date.

⚠ This is a guard, not a convenience. `strategy_signals.fill_bar_date` is
durable; a bar index is not. A fill date no longer in the corpus means the
corpus was rebuilt, re-adjusted or re-segmented under a recorded decision — and
the resolver's own entry-price check (4a §3.7) catches only the case where the
date still exists but the open moved. Together they cover both halves. Silently
re-reading "whatever bar is at that position now" is how a ledger stops being a
record of what was actually decided.

⚠ Duplicate dates need no handling here: `BarSeries.__post_init__` already
rejects duplicate and non-ascending dates at construction, so the lookup is
unambiguous by the input type rather than by a rule stated in this module.

### 4.3 The backfill path

`select_pending_fills(conn, *, strategy_id, strategy_version, rule_set_version,
input_rule_set_version)` returns every fired entry for that strategy version
with **no outcome at that (resolver, input) version pair**, in
`(instrument_id, signal_bar_date)` order, carrying `signal_id`,
`instrument_id`, `signal_bar_date`, `fill_bar_date`, `fill_price` and
`universe`.

⚠ `universe` is returned even though it is a join away, because it is #2288's
labelling contract and this selector is the bridge every phase-5 consumer will
use. A label the consumer has to fetch separately is a label it will omit.

```sql
SELECT s.signal_id, s.instrument_id, s.signal_bar_date,
       s.fill_bar_date, s.fill_price, s.universe
FROM strategy_signals s
LEFT JOIN strategy_outcomes o
       ON o.signal_id = s.signal_id
      AND o.rule_set_version = %(rule_set_version)s
      AND o.input_rule_set_version = %(input_rule_set_version)s
WHERE s.strategy_id = %(strategy_id)s
  AND s.strategy_version = %(strategy_version)s
  AND s.signal_kind = 'entry'
  AND s.verdict = 'fired'
  AND o.outcome_id IS NULL
ORDER BY s.instrument_id, s.signal_bar_date
```

⚠ **Both version predicates are in the JOIN, not the WHERE.** In the `WHERE`
they would turn the outer join into an inner one and return nothing — the
standing anti-join trap. Pinned by a test, because the wrong form returns *zero
pending fills* and reads as "nothing to do".

⚠ The selector is **scoped to one strategy version and required to be**: no
defaults, no "all strategies" mode. A backfill that fans out across every
strategy in the table is a backfill nobody can size before starting it.

⚠ **This is a re-resolution path, not only a first-run path.** Bump the resolver
— or the quarantine rule set — and every fill becomes pending again under the
new pair, with the old outcomes intact beside it. That is the same shape as
bumping `strategy_version` in the ledger, and it is why both versions are in the
key.

## 5. Acceptance

1. The constraint set rejects exactly what `OutcomeRow` rejects — **checked
   exhaustively, not by example.** Every combination of the five outcome classes
   × {no reason, each of the four reasons} × each of the four nullable payload
   fields present/absent is offered to both the Python validator and to
   Postgres, and the two verdicts agree on every one. The unknown-member cases
   (an out-of-vocabulary `outcome` / `reason` / `resolution_method`) are
   included. ⚠ This arm catches the 3c mirror defect *by construction*: the
   half-populated states the writer cannot emit are enumerated here rather than
   remembered.
2. A second insert on the same `(signal_id, rule_set_version,
   input_rule_set_version)` raises `UniqueViolation`; the same outcome under a
   different `rule_set_version` **or** a different `input_rule_set_version`
   inserts cleanly, and all rows survive.
3. The writer refuses a parent that is not a fired entry — `signal_kind='exit'`,
   `verdict='not_fired'` and `verdict='not_evaluable'` each raise and write
   nothing — and refuses an `exit_bar_date` before the parent's `fill_bar_date`.
4. `locate_fill_index` raises on a date absent from the series, naming it.
5. `select_pending_fills` returns a fill with no outcome, does not return one
   already resolved at that version pair, **does** return one resolved only at a
   different resolver version or a different input version, and never returns a
   `not_fired`, `not_evaluable` or `exit` row.
6. Deleting a signal removes its outcomes (CASCADE), so no orphan survives.
7. **Full-population round trip.** Over EVERY research series that can carry a
   ledger row: signals written through 3c's writer, read back through
   `select_pending_fills`, resolved by 4a, written through `store_outcomes`, and
   read back out of the database and compared **field for field on the persisted
   fields** against the in-memory `Outcome` (`exit_index` is not stored, so it
   is not compared). Zero mismatches. The outcome census is reported per class
   and per `unresolved` reason (criterion 9), with the minimum `exit_price` and
   `gross_return_pct` (§3.2), and the stored return is re-derived in SQL from
   `(exit_price − fill_price) / fill_price` and compared to the stored value at
   a stated tolerance.

   ⚠ **Bars come from `load_masked_series`**, the fail-closed loader 4a's
   contract requires, at `price_quarantine`'s current `RULE_SET_VERSION` — which
   is also what is stored as `input_rule_set_version`. Resolving the round trip
   over raw unmasked bars would make the harness a caller that violates the
   resolver's own documented obligation.

   ⚠ **The population is every research series with an `instrument_id`.** The
   rest cannot carry a ledger row at all — `strategy_signals.instrument_id` is a
   FK — so the exclusion is structural rather than a sample. Its size is
   reported, not assumed.

   ⚠ What this arm proves and does not: it proves the **round trip** — writer,
   selector, constraint set, date→index bridge — over the full eligible
   population against real bars. It does **not** re-prove the resolver, which is
   4a's equivalence arm.

   ⚠ The harness WRITES to the dev database and must delete its own rows,
   asserting the **whole-table** row count returns to its pre-run value, not
   merely the count for its own strategy id.
8. The migration's CHECK vocabularies equal the Python `Literal`s, parsed out of
   the migration text (§3.2 constraint 1).

⚠ Acceptance 7's figures are produced by a committed script, never written into
prose.
