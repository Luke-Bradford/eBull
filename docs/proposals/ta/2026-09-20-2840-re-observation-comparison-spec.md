# #2840 — the re-observation comparison: build spec

Status: **spec for a build.** Successor to
`docs/proposals/ta/2026-09-20-2840-re-observation-is-not-capture.md`, whose §5 is the
constraint list this design must satisfy and whose §5.6 named four unsolved items.

⚠ **Revision 2.** Revision 1 was refused at Codex checkpoint 1 with 50 findings — the
thirteenth refusal on this ticket. Four of its factual premises were wrong and are
corrected in §2; the surviving design is **simpler** than revision 1, not larger: two
relations instead of three, no advisory lock, no per-bar state cache. §8 tabulates every
finding and its disposition.

Nothing here is operator-gated. No broker call. No change to any promotion gate.

## 1. What is being built, in one sentence

A producer that compares each delivered intraday bar against the value we already hold for
that same bar, records **one durable row per compared bar per provider call** with that
call's own request/response bounds, and marks the rows where the price changed — so that a
bracket `(last agreement, first divergence)` exists as two real rows instead of being
reconstructible from nothing.

It is **not** a new fetch, a new job, a new cadence or a new universe. The evidence is
already fetched on every fire and discarded at
`app/services/strategy_intraday_harvest.py:301`.

## 2. Four premises corrected before anything is designed on them

Revision 1 asserted these; all four are wrong, and all four were verified against the
source, not accepted from the reviewer.

1. **OHLCV is not compared as floats end-to-end.** The provider constructs
   `Decimal(str(raw_open))` (`app/providers/implementations/etoro.py:706-709`); the
   harvester passes `Decimal` into `IntradayBar`; storage is `DOUBLE PRECISION` — and by
   `sql/277_strategy_intraday_storage_compaction.sql:17-21`, **not** `sql/276` as revision 1
   cited. So a naive comparison is `Decimal` vs `float`, which Python evaluates exactly:
   `Decimal("101.23") == 101.23` is `False`. Revision 1's "exact equality is well-defined"
   would have manufactured a divergence on **every** bar of **every** call.
   ⇒ §4.3 fixes the comparison at **storage precision**.
2. **Volume is already lossy upstream and cannot be a divergence trigger.**
   `_int_or_none` (`etoro.py:978-985`) evaluates `int(float(str(value)))` and returns
   `None` for a result of zero — so a genuinely zero-volume bar and a missing one are the
   same value by the time we see it. ⇒ §4.3 makes the divergence test **price-only**;
   volume is recorded and flagged, never decisive.
3. **`strategy_intraday_bars` has no primary key.**
   `sql/278_strategy_intraday_watermark_and_brin.sql:10-11` dropped it deliberately (index
   size); duplicate protection is the watermark, which is a convention and not a
   constraint. ⇒ §4.4 refuses a bar identity that returns conflicting stored rows rather
   than picking one.
4. **The measured premise was stated imprecisely.** "Every tier's `max(bar_time)` is
   19:5x UTC" is wrong for 30m, whose last completed RTH bar *starts* at 19:30. The actual
   per-tier figures are in §3, with the query that produces them.

## 3. Measured premise — the denominator is counted, never assumed

Dev DB, 2026-09-20.

```sql
SELECT timeframe, count(*), min(bar_time), max(bar_time)
FROM strategy_intraday_bars GROUP BY 1 ORDER BY 1;
```

| timeframe | rows | min `bar_time` | max `bar_time` |
| --- | --- | --- | --- |
| `1m` | 6,615 | 2026-08-21 13:30Z | 2026-09-18 **19:59Z** |
| `5m` | 16,083 | 2026-07-06 13:30Z | 2026-09-18 **19:55Z** |
| `30m` | 6,536 | 2026-04-07 17:00Z | 2026-09-18 **19:30Z** |

Each tier's maximum is its own last completed RTH bar of 2026-09-18, the last session
before this spec. The active universe has **18 members** (8 × 30m, 8 × 5m, 2 × 1m); the
per-member listing with each watermark is reproduced by the query in §9, and every one of
the 18 has a watermark equal to its tier's maximum above.

⚠ **Overlap is reachable but not guaranteed, and the 1m tier is the counterexample.**
`_fetch_count` requests a COUNT. From Friday's close to Monday 09:35 ET the elapsed
interval is ~66h, so the 1m tier asks for `min(1000, 3963) = 1000` bars — and 1000
one-minute candles reach back 1000 **bars**, not 66 hours. Whether that spans a weekend
depends on whether the provider emits non-session candles, which we have **not measured**.

⇒ This design does not resolve that and does not need to. `no_overlap` is a first-class
recorded outcome with an executable definition (§4.5), so a 1m tier that records
`no_overlap` every Monday is a finding the table states rather than a silence a reader has
to interpret.

## 4. Design

**Two relations.** Both unpartitioned. Neither holds a foreign key to
`strategy_intraday_bars`: a FK would make a retention partition drop either block on the
evidence or cascade it away, and the evidence must outlive the horizon that produced it.
Bar identity and both compared value sets are **copied**.

### 4.1 `strategy_intraday_reobservations` — one row per provider call per selected member

The denominator of *calls*, and the durable record of calls that produced no comparison.

| column | note |
| --- | --- |
| `reobservation_id bigserial` | PK |
| `universe_version text`, `ordinal int`, `timeframe text`, `symbol text` | member identity as selected — **`ordinal` and `symbol`, not just `instrument_id`**, so an unresolved member is identifiable |
| `instrument_id bigint NULL` | NULL exactly when the member did not resolve |
| `requested_at timestamptz` | stamped **immediately before** the provider call |
| `received_at timestamptz` | stamped **immediately after**; `CHECK (received_at >= requested_at)` |
| `outcome text` | see §4.5 |
| `overlap_bars`, `compared_bars`, `agreed_bars`, `diverged_bars`, `missing_baseline_bars`, `invalid_baseline_bars int` | all `>= 0`; `CHECK (agreed + diverged = compared)` and `CHECK (compared + missing_baseline + invalid_baseline = overlap)` |
| `failure_class text NULL` | exception **class name only** — the existing `HarvestFailure` discipline (`strategy_intraday_harvest.py:311-314`): provider messages can carry request URLs and upstream fragments |

`UNIQUE (universe_version, ordinal, requested_at)` — **no nullable column in the key**, so
ordinary PostgreSQL uniqueness applies (a key containing NULL would never conflict, which
is what an `instrument_id`-based key would have produced for unresolved members).

⚠ `requested_at`/`received_at` bound the **logical call including `ResilientClient`'s
internal retries**. That is wider than one HTTP attempt and is the conservative direction
for a bracket. Stated here because the column names do not say it.

### 4.2 `strategy_intraday_reobserved_bars` — one row per compared bar per call

This is the denominator **carrying bar identity** (predecessor §5.3), and it replaces
revision 1's per-bar state cache. A durable row per comparison, rather than an overwritten
"latest agreement" pointer, is what makes the bracket's left edge survivable.

| column | note |
| --- | --- |
| `reobservation_id bigint` | `REFERENCES strategy_intraday_reobservations ON DELETE CASCADE` |
| `bar_time timestamptz` | PK is `(reobservation_id, bar_time)` |
| `timeframe text`, `instrument_id bigint` | copied bar identity, so the table is queryable without a join |
| `observed_open/high/low/close double precision NOT NULL`, `observed_volume double precision NULL` | what the provider delivered, at storage precision |
| `baseline_open/high/low/close`, `baseline_volume` | what it was compared against — always stored, never inferred from `price_changed` |
| `baseline_source text` | `stored_bar` · `prior_reobservation` |
| `baseline_reobservation_id bigint NULL` | **the bracket's left edge**; non-null exactly when `baseline_source = 'prior_reobservation'` |
| `baseline_captured_at timestamptz NULL` | the stored bar's own `captured_at`; non-null exactly when `baseline_source = 'stored_bar'`. Copied so contemporaneity (predecessor §3) is judgeable at read time without rejoining a partition that may since have been dropped |
| `price_changed boolean NOT NULL` | the transition marker (§4.3) |
| `volume_changed boolean NOT NULL` | recorded, never decisive (§2.2) |

**The bracket.** For a bar with a divergence, the right edge is that row's parent
`received_at`; the left edge is `baseline_reobservation_id`'s parent `requested_at`. Both
are real committed rows, and the conservative side of each bound is used because those are
the only sides that *are* bounds.

⚠ A first divergence against `baseline_source = 'stored_bar'` has **no left edge** —
`captured_at` is an upper bound on when the baseline was observed, not a lower one. Such a
row is evidence of a rewrite but **not a bracket**, and the read-time predicate in §6 must
exclude it rather than treat `captured_at` as an observation instant.

### 4.3 The comparison, exactly

The current known value for a bar is the `observed_*` of its most recent
`strategy_intraday_reobserved_bars` row ordered by the parent's `requested_at`; if there is
none, it is the stored `strategy_intraday_bars` row.

```
price_changed  = any of (open, high, low, close) differ
volume_changed = observed_volume IS DISTINCT FROM baseline_volume
```

compared **at storage precision, by PostgreSQL**. Each delivered value is cast with
`%(x)s::numeric::double precision` — the identical conversion `store_intraday_bars`
triggers by passing a `Decimal` into a `double precision` column — and the result is
compared to the stored `double precision`.

⚠ **Revision 2a correction.** This section originally said "convert with `float(d)`", and
that is a *second* conversion path rather than the writer's own: psycopg adapts `Decimal`
to `numeric` and **PostgreSQL** does the narrowing. The two agree across the ordinary price
domain (verified on 3,006 values, zero mismatches) but not at the boundaries — and
`float(Decimal("1e1000"))` silently returns `inf` where PostgreSQL raises. Delegating the
cast means there is exactly one conversion in the system, and an out-of-range value becomes
a caught error instead of a stored infinity.

It follows that two Decimals differing below float resolution compare equal; that is a
stated limit of the detector, not a bug, and it is recorded in the table comment.

Non-finite guard: a value that fails the cast, or a stored baseline that is itself
non-finite, is refused for that bar and counted in `invalid_baseline_bars`. **Both** sides
are validated — `strategy_intraday_bars` CHECKs `open > 0`, which excludes NaN but not
`Infinity`, so a stored baseline can in principle be non-finite too.

⚠⚠ **A recorded divergence is not a re-basing.** A price correction, a late exchange
adjustment and a split re-base all produce the same row. This table records *that the
provider's answer moved*; attributing a movement to a corporate action is the consumer's
job and no attribution rule is implied here.

### 4.4 Baseline selection

Seed from `strategy_intraday_bars` by `(timeframe, instrument_id, bar_time)`. Because that
table has **no primary key** (§2.3), the read must tolerate more than one row: identical
duplicates collapse, and **conflicting** duplicates are refused for that bar and counted in
`invalid_baseline_bars`. Choosing one arbitrarily would silently pick which history is true.

**No advisory lock is taken.** Revision 1 took `pg_advisory_xact_lock(2448, tier)` to guard
the seed read against a concurrent retention partition drop. That was wrong twice over: the
drop already takes `ACCESS EXCLUSIVE`, so ordinary relation locking and MVCC make the read
safe without it; and the actual hazard revision 1 was reaching for — evidence dangling on a
dropped partition — is removed by copying values rather than referencing them. Adding the
advisory lock would have introduced a deadlock edge against retention's own tier order
(`sorted()` over the timeframe strings yields `1m → 30m → 5m`,
`strategy_observation_storage.py:525`) in exchange for nothing.

### 4.5 Outcomes, with executable definitions

Let the **overlap set** be the delivered completed-RTH bars with
`bar_time <= watermark` (empty when the watermark is NULL). `overlap_bars` is its size.

| outcome | condition |
| --- | --- |
| `unresolved_member` | the member failed universe resolution; **no provider call**, `requested_at = received_at` |
| `fetch_failed` | `provider.get_intraday_candles` raised; `failure_class` set |
| `invalid_response` | the response was fetched but `_completed_rth_bars` raised (conflicting duplicate, naive timestamp); `failure_class` set |
| `no_overlap` | `overlap_bars = 0` |
| `no_baseline` | `overlap_bars > 0` and `compared_bars = 0` |
| `compared` | `compared_bars > 0` |

Every **selected** member writes exactly one row per fire on every one of these paths.

⚠ Absence of a row means "no completed observation record", **not** "the member was not
selected": a crash between the fetch and the insert leaves nothing behind, and no durable
selection manifest exists. `job_runs` bounds the fire; it does not enumerate its members.

### 4.6 Ordering against the existing capture, and blast radius

The comparison runs **after** `store_intraday_bars`, inside its own `try`/`except`:

- Order is safe because the overlap set is defined by `bar_time <= watermark`, which
  storing strictly-newer bars does not change.
- A comparison failure therefore **cannot** suppress a healthy capture. Revision 1 placed
  the comparison before the write inside the same `try`, where a new-table error would have
  skipped valid bars — the one way this ticket could have damaged something that works.
- The comparison's call row and all its bar rows are written in **one** `conn.transaction()`,
  so a retry either hits the §4.1 unique constraint having written nothing else, or writes
  the complete record. Partial counters are unreachable.

### 4.7 No upsert on `strategy_intraday_bars`

`store_intraday_bars` is called unchanged with the unchanged `new` list. The comparison
reads the bar table and writes only to the two new relations. Bar immutability is
load-bearing for `captured_at` semantics (`bar_capture_certificate.py:27-37`).

### 4.8 The claim the evidence licenses

"No price change was detected **between these two observations**." Never "the provider did
not rewrite in that interval": `A→B→A` between two polls is invisible to any polling scheme.
Fixed in the table comments, not only here.

Two further limits, recorded because the column names imply more than they deliver:

- `requested_at`/`received_at` are **client-side** bounds. A cached or replica-stale
  response bounds when this client received an assertion, not when the provider's history
  changed. Clock skew between this host and the database is not corrected.
- `app/providers/implementations/etoro.py` flattens response groups without checking the
  returned `instrumentId` against the request. That is a pre-existing capture-path gap, not
  one this ticket introduces, and it is noted because a wrong-series response would surface
  here as an apparent revision.

### 4.9 Retention and growth — deferred, with the rate stated

No retention is added for the new relations, and revision 1's claim that revisions are
"bounded by construction" is **withdrawn**: an oscillating value produces unbounded rows.

⚠ Revision 1 also estimated "≲36 bar rows per fire" from `_OVERLAP_BARS = 3`. That is
wrong, and the measurement in §10 is the counterexample: `_OVERLAP_BARS` is an **addend to
a count estimate, not a cap on overlap**, so a stale watermark delivers far more. Measured
on the first real slices: overlap per call ranged **35 to 351** bars, because the watermark
sat at the previous Friday's close. In steady state (a fire every five minutes) the elapsed
term collapses and overlap falls back toward `_OVERLAP_BARS`, but the honest statement is
that the rate is **a function of watermark lag** and these tables now measure it directly.

Retention is a follow-up, named here rather than half-built — a comparison table that reaps
its own negative evidence is worse than one that grows, and revision 1's asymmetric
state-reaping was the mechanism by which "this bar was checked and did not move" would have
been deleted. `baseline_reobservation_id` is `ON DELETE RESTRICT` precisely so that a future
retention has to confront the bracket edges rather than silently nulling them.

## 5. What this does NOT do

- Does not add a pre-open fire. Two or more pre-open re-observations are still needed before
  a bracket can lie inside `(T, O)` (predecessor §2); this build is the **producer** that
  makes such a fire mean something. Adding the cadence before the producer existed was the
  error `03b6596f` corrected.
- Does not change `PROVIDER_REWRITE_TIMING_VERIFIED`, which stays `False`.
- Does not filter to contemporaneous baselines. Eligibility (predecessor §3, and the
  `intraday_capture_semantics` cutover) is a **read-time** predicate over recorded
  provenance, not a producer-side filter; filtering at write time would discard the evidence
  that the filter was ever needed.
- Does not widen the universe, touch `_fetch_count`, `_OVERLAP_BARS` or the request budget.
  The comparison issues **no additional provider request** — which is all "free" means; the
  write cost is §4.9.

## 6. Read-time eligibility for a decisive bracket

Stated here so the producer is not later credited with guarantees it does not make. For a
corporate action effective at `T` with following open `O`, a bracket is decisive only when
**all** hold:

1. the divergent row has `baseline_source = 'prior_reobservation'` (§4.2 — a `stored_bar`
   baseline has no lower bound);
2. `T <= prior.requested_at` and `detected.received_at <= O`, using the parents' bounds;
3. neither call's `[requested_at, received_at]` straddles the session open;
4. the baseline chain is contemporaneous and post-cutover per
   `intraday_capture_semantics`;
5. `price_changed` is true — `volume_changed` alone is not evidence (§2.2).

## 7. Acceptance

1. Every selected member writes exactly one `strategy_intraday_reobservations` row per fire
   on all six outcome paths of §4.5, with `received_at >= requested_at`, and with
   `requested_at` strictly after the job's own `observed_at` on every row that made a call
   — which is what proves the `sql/402` defect is not reproduced.
2. Re-observing an unchanged bar N times writes N bar rows, all `price_changed = false`,
   and **zero** rows whose baseline is not the immediately preceding observation.
3. A perturbed close produces exactly one `price_changed` row whose
   `baseline_reobservation_id` is the preceding call, and the following unchanged
   observation produces `price_changed = false` against the **new** value.
4. `A → B → C → B → A` records four `price_changed` rows, including the two reversions — the
   case a digest-keyed design loses.
5. A first divergence against a stored baseline sets `baseline_source = 'stored_bar'` and
   `baseline_reobservation_id IS NULL`, and is excluded by §6.1.
6. A `Decimal` that round-trips to the identical `double precision` is **not** a divergence
   (§2.1 — the failure revision 1 would have shipped).
7. A provider raise writes `fetch_failed`; a conflicting duplicate writes `invalid_response`;
   an empty overlap writes `no_overlap`; an unresolved member writes `unresolved_member`
   with `instrument_id IS NULL`.
8. A comparison that raises does not reduce `HarvestReport.written` (§4.6).
9. Conflicting duplicate stored rows for one bar identity increment
   `invalid_baseline_bars` and write no bar row.

## 8. Checkpoint-1 findings and their disposition

50 findings. Grouped; every one accounted for.

| findings | disposition |
| --- | --- |
| 21, 22, 23, 25 | **FIXED — premise corrected.** §2.1, §2.2, §4.3. The float claim was wrong and would have made every comparison diverge |
| 1, 2, 3, 4, 5, 6 | **FIXED by redesign.** The per-bar state cache is gone; §4.2's per-bar-per-call row makes the left edge a durable row. §4.2 records that a `stored_bar` baseline is not a bracket |
| 14, 15, 16, 17 | **FIXED by removal.** §4.4 — no advisory lock; the lock was guarding a hazard already removed by copying values |
| 7, 8, 9, 46 | **FIXED by removal.** §4.9 — no state table and no retention to race; the growth claim is withdrawn and the rate stated |
| 19, 20 | **FIXED.** §4.6 — comparison runs after the write, in its own `try` |
| 26, 27 | **FIXED.** §4.1 — `ordinal` + `symbol` recorded; the unique key contains no nullable column |
| 32, 33, 34, 35 | **FIXED.** §4.5 — executable outcome definitions; six counters including `missing_baseline_bars` and `invalid_baseline_bars` |
| 45 | **FIXED.** §2.3, §4.4 — conflicting stored duplicates refused, not arbitrated |
| 24 | **FIXED.** §4.3 non-finite guard |
| 48 | **FIXED.** §2.4, §3 — the per-tier figures restated correctly |
| 44 | **FIXED.** §4.1, §4.2 — explicit nullability, non-negative and consistency CHECKs |
| 49, 50 | **FIXED.** §7 rewritten around the counterexamples, including A→B→C→B→A and the Decimal round trip |
| 39, 40, 41, 42, 47 | **ACCEPTED as stated limits.** §4.5, §4.1, §4.8, §5 — recorded in prose and table comments rather than claimed away |
| 10, 11 | **DEFERRED to read time by design.** §5, §6.4 — filtering at write time would discard the evidence that the filter is needed |
| 13 | **FIXED as a stated limit.** §4.3 — a divergence is not a re-basing |
| 37 | **ACCEPTED, pre-existing.** §4.8 — a capture-path gap this ticket neither introduces nor fixes |
| 12 | **REBUTTED in part.** Baseline source and provenance ARE recorded (§4.2). Immutable provider/session context is not, and is not needed for a bracket |
| 18 | **ACCEPTED.** §4.2 — the analysis orders by `requested_at`, not by insertion, so a reversed commit cannot invert a bracket; a stale response can still seed a stale baseline |
| 28 | **REBUTTED.** Collision needs the same `(universe_version, ordinal)` twice in one microsecond. A clock rollback causes a refusal, which is the safe direction |
| 29, 30, 31 | **PART-FIXED.** §4.6 — one transaction makes the record atomic. A `job_runs` fire id is deliberately not plumbed through: it adds a scheduler coupling and a column for an analysis nobody has planned |
| 36, 38 | **ACCEPTED, out of scope.** Provider-side skipping of malformed candles and a raise inside `_advance_cursor` are pre-existing harvester behaviour |
| 43 | **FIXED.** §6 — the full read-time eligibility predicate, not just the 09:30 exclusion |

## 9. Checkpoint-1 revision-2 findings (7) and their disposition

| finding | disposition |
| --- | --- |
| 1 — an earlier call committing late can be mis-ordered as a baseline | **PART-FIXED.** The baseline query filters `r.requested_at < <this call>`, which excludes a *later* call. It cannot exclude an earlier uncommitted one; the harvest job holds a per-process advisory lock, so overlapping fires need two daemons, and the residual effect is a duplicate transition rather than a corrupted one. Stated in the query's own comment |
| 2 — `float(d)` is not the writer's conversion | **FIXED.** §4.3 — PostgreSQL now does the cast. This was the right catch: my 3,006-value agreement check sampled the right domain with the wrong *path* |
| 3 — `Decimal("1e1000").is_finite()` is true but `float()` is `inf`; stored baselines unchecked | **FIXED.** §4.3 — the DB cast raises instead of returning `inf`, and both sides are validated |
| 4 — storage/construction/comparison failures still had no outcome | **FIXED.** Two outcomes added: `not_attempted` (no call was made — today, a watermark read that raised) and `comparison_skipped` (the call succeeded but capture or comparison raised). Eight outcomes, every `except` in the member path mapped to one |
| 5 — the growth rate is an estimate, not a measurement | **FIXED.** §4.9, §10 — measured, and revision 1's figure was wrong by an order of magnitude |
| 6 — two of revision 2's own corrections overstated | **FIXED.** "every bar would diverge" → only values not exactly representable in binary (`Decimal("100") == 100.0` is True); the clock-rollback claim is withdrawn rather than defended |
| 7 — no index design stated | **REBUTTED in part.** The index exists in the migration (`idx_strategy_intraday_reobserved_bars_identity`, plus a partial index on `price_changed`); the spec omitted it. Now stated here |

## 10. Dev-verify — the real path, on the dev DB, against the live provider

`sql/404` applied to dev; four real harvest slices of 6 members run with the live eToro
provider (`get_intraday_candles` is informational — no broker mutation, and the unattended
guard permits it). 2026-09-21, market closed since the 18th.

| slice | **written** | **compared** | diverged | failures |
| --- | --- | --- | --- | --- |
| 1 | 0 | 1,322 | 0 | 0 |
| 2 | 0 | 2,106 | **5** | 0 |
| 3 | 0 | 359 | 0 | 0 |
| 4 | 0 | 1,322 | 0 | 0 |

**`written = 0` on every slice against `compared = 5,109` is the whole finding** — nothing
new existed to capture, and 5,109 bars of re-observation evidence were delivered and,
before this change, discarded. All 24 calls returned `outcome = 'compared'`;
`missing_baseline` and `invalid_baseline` were zero throughout.

⚠ These counts are **not** stable across runs and must not be quoted as a fixed figure:
which bars get compared depends on where the round-robin cursor sits and how far each
member's watermark lags. The tables measure it; this row is one sample of it.

**A real bracket exists.** 1,322 rows carry `baseline_source = 'prior_reobservation'`, so
their left edge is a prior call's `requested_at` rather than a `captured_at` upper bound.
Per-call `received_at - requested_at` ranged 163ms to 1.25s — real bounds, not the job clock.

### ⚠⚠ The provider does silently rewrite stored intraday bars

Five divergences, all CENN `5m`, and the **shape is close-dominant**:

| bar | moved | detail |
| --- | --- | --- |
| 2026-08-31 19:35Z | H, C | `H 3.97→4.00`, `C 3.97→4.00`; O, L, volume unchanged |
| 2026-09-02 17:35Z | C only | `C 3.90→3.95`; volume identical at 15,968 |
| 2026-09-02 18:00Z | C only | `C 4.04→3.96`; volume identical at 23,939 |
| 2026-09-02 18:10Z | C only | `C 3.93→4.05`; volume identical at 24,135 |
| 2026-09-02 18:15Z | C only | `C 3.93→4.05`; volume identical at 19,388 |

⚠ **This is emphatically NOT a re-basing, and the distinction is the reason §4.3 insists on
it.** A split re-base scales **all** of O/H/L/C together and changes volume. Here volume is
**identical to the unit** on all four close-only cases while the close moves in both
directions. That is the shape of a closing-print or consolidated-tape correction.

⚠ All five baselines are `stored_bar`, so by §6.1 they are **evidence of a rewrite and not
decisive brackets** — `captured_at` bounds the baseline observation from above only. The
design records them as exactly that rather than dressing them up.

⇒ The substantive result for #2840: **re-observation detects real provider rewrites on this
panel at a usable rate**, so the pre-open re-observation fires the experiment needs are now
worth running. They were not, while nothing recorded a comparison at all.

### Incidental, not this ticket

Three consecutive CENN 5m bars (17:35, 18:00, 18:10) carry an identical `high = 4.10` and
`low = 3.86`. That may be genuine for a volatile microcap, or it may be a session-level
extremum leaking into per-bar fields. **Not measured, not acted on here** — noted so it is
not lost, and it touches the capture path rather than this comparison.

## 11. Reproduction

```sql
-- §3 per-member listing
SELECT m.timeframe, m.symbol, i.instrument_id, w.last_bar_time
FROM strategy_intraday_universe_members m
JOIN strategy_intraday_universe_versions v
  ON v.universe_version = m.universe_version AND v.status = 'active'
LEFT JOIN instruments i ON i.symbol = m.symbol AND i.is_tradable
LEFT JOIN strategy_intraday_watermarks w
  ON w.timeframe = m.timeframe AND w.instrument_id = i.instrument_id
ORDER BY m.ordinal;
```

Refs #2840. Refs #2437.
