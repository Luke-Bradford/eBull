# #3109 slices 1-3 — a poll-eligibility clock that advances

Status: proposal, **v2 after Codex checkpoint 1 (36 findings)**. Slice 0 (evidence)
and the CIK batching (`4e894cc3`) are already on `main`.

Corrections forced by ckpt-1 are marked **[ckpt-1]** where they changed a claim.
Three of them falsified something the v1 draft asserted, so they are recorded
rather than quietly edited.

## 1. The defect, stated once

`data_freshness_index.expected_next_at` is used for two incompatible jobs:

1. a **reconciliation deadline derived from the last filing** —
   `last_known_filed_at + _CADENCE[source]` (`app/services/data_freshness.py:424`);
2. the **eligibility clock for our own request budget** — the poll selector's
   `WHERE expected_next_at <= now()` / `ORDER BY expected_next_at ASC`
   (`_ciks_due`, `app/services/data_freshness.py:726-746`).

For a filer whose last filing was 1994, (1) is permanently in the past. It wins
the `ORDER BY` on every tick, forever, and nothing else is reached. A successful
no-change poll does not help: `record_poll_outcome` recomputes the deadline from
the same unchanged `last_known_filed_at`, returning the row to the head.

**[ckpt-1]** Two precision fixes to how v1 described this:

- The `elif outcome in ("current", "new_data")` fallback at
  `data_freshness.py:425` — which *does* push the deadline forward — branches on
  the `last_known_filed_at` **argument passed to the function**, not on the
  stored column. The starving rows all supply one, so the fallback never fires
  for them.
- `expected_next_at` was never a clean "filing prediction" to begin with.
  `_CADENCE`'s own comments call it a reconcile cadence — *"Layer 3 reconcile
  cadence: 30d ceiling — Atom feed catches the individual events; this is the
  per-CIK safety-net poll"* (`data_freshness.py:77`). It is a scheduling
  heuristic anchored to a filing date, which is precisely why anchoring it to a
  1994 date starves the queue. This spec does **not** claim `_CADENCE` encodes
  SEC filing deadlines, and does not need it to.

## 2. Full-population measurement (dev, 2026-09-16T23:30Z)

Reproduced by `scripts/measure_3109_batching.py --rotation`, added by this
change. **[ckpt-1]** v1 cited that flag before writing it; it is now implemented,
and its CIK counters use the selector's own `cik ~ '^[0-9]{1,10}$'` shape filter
and `lpad(cik,10,'0')` normalisation rather than raw strings.

| figure | value |
| --- | ---: |
| `data_freshness_index` rows | 55,775 |
| ├ `state='current'` | 42,175 |
| ├ `state='unknown'` | 13,600 |
| └ `state='expected_filing_overdue'` | 0 |
| recheck-lane rows (`never_filed` / `error`) | 0 |
| distinct pollable CIKs in the poll lane | 15,483 |
| rows due | 27,157 |
| CIKs due | 7,399 |
| rows with a NULL poll deadline | 0 |
| rows with a non-NULL `last_polled_at` | 95 |
| distinct CIKs with a non-NULL `last_polled_at` | 67 |

### 2.1 The discriminator, not the inference **[ckpt-1]**

v1 claimed "the same 66 rows every hour" from three surviving `last_polled_at`
timestamps. That is an inference an overwrite-column cannot support, and ckpt-1
was right to reject it. Replaced with a direct current-state test:

> Run the production selector now. Compare its CIK set to the CIKs actually
> polled in the most recent run.

```
selector now: 66 CIKs | polled at 23:00: 48 CIKs
intersection: 48   selector-only: 18   polled-only: 0
```

**Every CIK the last run polled is still in the queue.** `polled-only = 0` — a
completed poll removed nothing from the head. That is the defect in one
measurement, and unlike the histogram it is checkable at any instant. (The extra
18 are the budget-unit change in `4e894cc3`: 66 CIKs now, where 66 *rows*
previously covered 48 CIKs.)

⚠ `sec_per_cik_poll` reports `status='success'` every one of those hours.
**[ckpt-1]** `row_count` is the count of successful manifest-upsert calls
(including rediscoveries), not subjects reached — so total starvation is
indistinguishable from a quiet week in the job's own health surface. Not fixed
here; noted because it is why this ran unnoticed since 2026-06-05.

### 2.2 Throughput headroom, measured not assumed **[ckpt-1]**

ckpt-1 flagged that "100 CIKs/hour is sustainable" was a claim about a
population we have never reached. Measured over the last 7 days (97 runs):
**avg 16.6 s, max 31.1 s** for a 66-CIK budget. Runtime is dominated by the
shared SEC rate gate, so it scales roughly linearly in fetches: ~25 s avg /
~47 s max at 100. Both are far inside the hourly tick.

## 3. Source rule, and its honest status **[ckpt-1]**

`.claude/skills/data-sources/sec-edgar.md` §"Strategies" item 4:

> - **Hot**: Atom `getcurrent` for 8-K. Poll every 5–10 min during business hours.
> - **Warm**: `daily-index/master.{YYYYMMDD}.idx` early-morning batch.
> - **Cold**: per-CIK submissions JSON **re-pull weekly or per-event**.

`sec_per_cik_poll` IS the cold tier (`app/jobs/sec_per_cik_poll.py:6`;
skill line 1187). So the re-poll interval is **7 days** — taken, not invented.

ckpt-1 correctly notes this is **an in-repo settled convention, not an SEC
requirement**. SEC publishes a rate ceiling (10 req/s) and no re-poll cadence at
all. Per `.claude/CLAUDE.md` ("where a published formulation genuinely does NOT
exist, say so explicitly and fix the rule by construction"), that is stated here
rather than dressed up as a regulation, and the constant is frozen in code with
this citation on it.

The rule's "**or per-event**" arm is not omitted — it is the hot and warm tiers,
which already exist (`sec_atom_fast_lane`, the daily-index walker). This layer is
the *weekly* arm and is explicitly a safety net behind them.

**One constant, not per-source.** `submissions.json` is entity-wide: one fetch
answers every source of that CIK, and since `4e894cc3` the budget is denominated
in CIKs. A per-source interval would be incoherent with the unit the request is
spent in.

## 4. Design

### 4.1 Schema

```sql
ALTER TABLE data_freshness_index
    ADD COLUMN next_poll_at TIMESTAMPTZ NOT NULL DEFAULT now();

DROP INDEX idx_freshness_due_for_poll;

CREATE INDEX idx_freshness_due_for_poll
    ON data_freshness_index (next_poll_at, source)
    WHERE state IN ('unknown', 'current', 'expected_filing_overdue');
```

`NOT NULL DEFAULT now()` is load-bearing, not tidiness — see §6. The backfill IS
the default: every existing row becomes eligible at migration time, which is
correct (we have never asked about 99.83% of them) and makes the first cycle one
complete deterministic sweep. **[ckpt-1]** It also resets the 66 rows polled in
the last hour; at 66 rows out of 55,775 that is accepted, not engineered around.

**[ckpt-1] The old index is DROPPED, not kept.** v1 justified keeping it as the
"overdue-filing visibility" surface, citing
`app/services/processes/stale_detection.py:146`. That citation is stale prose in
a dataclass docstring. The actual producer is
`scheduled_adapter.py:548 _source_watermark_behind`, and its docstring says the
opposite in as many words:

> Deliberately NOT ``not _source_watermark_fresh`` … and NOT the per-subject
> ``expected_next_at`` timing probe / ``state='expected_filing_overdue'``
> (event-form jitter — an issuer simply not filing is not an ingest problem).

So **no production reader of `expected_next_at` exists today.** An index with no
reader costs write amplification on a column this change updates on every poll.
Dropping it is part of the change, not scope creep. Verified by grep: the only
remaining references are `sec_rebuild` (write), `api/processes` full-wash
(write), the measurement script, and tests.

Consequence to state plainly: the ticket's "preserve overdue-filing visibility"
requirement has **no existing implementation to preserve**. What this change
preserves is the column's *meaning* — `expected_next_at` keeps being computed
exactly as today, so any future surface can still use it. It does not build that
surface.

### 4.2 `next_poll_at` is written in exactly four places

| writer | value | why |
| --- | --- | --- |
| `record_poll_outcome` | `NOW() + POLL_REPOLL_INTERVAL` | we just spent a request; unconditional, covering the 200-new, 200-empty, 304 and error paths |
| `sec_rebuild._reset_scheduler_rows` | `NOW()` | a rebuild must force an immediate re-poll, like its existing `expected_next_at = NOW()` |
| `app/api/processes.py` full-wash | `NOW()` | same, alongside its existing `expected_next_at = NULL` |
| the `DEFAULT` | `now()` | a newly seeded row is eligible at its arrival instant |

Unconditional advance is deliberate and differs from `expected_next_at`'s
conditional `CASE`: a losing or stale poll still consumed the fetch, so the
clock must move. `seed_freshness_for_manifest_row`'s `ON CONFLICT` does not
mention the column, so a discovery-layer sighting correctly leaves it alone.

**[ckpt-1] SQL `NOW()`, not the Python clock.** ckpt-1 flagged the ambiguity.
The column's sibling `last_polled_at` is already written as SQL `NOW()` in the
same statement, so using the DB clock keeps one clock for both and removes
app/DB skew from the comparison. (The prevention-log rule requiring SQL-side age
comparison applies to windows under ~10 minutes; at 7 days skew is immaterial,
but consistency with the sibling column is free.)

```python
POLL_REPOLL_INTERVAL: Final = timedelta(days=7)   # skill §Strategies item 4, "Cold"
ERROR_RECHECK_INTERVAL: Final = timedelta(hours=1)
```

### 4.3 **[ckpt-1, DESIGN CHANGE] The recheck lane has the identical bug**

`_record_subject_error` (`sec_per_cik_poll.py:153`) calls `record_poll_outcome`
with **no `next_recheck_at`**, so the UPSERT writes `NULL`. The recheck selector
treats `next_recheck_at IS NULL` as immediately due. **An errored row is
therefore pinned to the head of the recheck lane forever** — the same defect in
the other lane, and it is why the recheck lane cannot be assumed to stay empty.

This is not an optional extra: §4.4's budget rollover makes the poll lane's
throughput depend on the recheck lane draining, so shipping the rollover without
this would build the margin on a lane that jams on its first error.

Fix: on `outcome='error'` with no explicit deadline, set
`next_recheck_at = NOW() + ERROR_RECHECK_INTERVAL`. One hour is **by
construction, not chosen**: it is the job's own tick, the smallest interval at
which a retry can actually happen. Any finite advance converts starvation into
rotation — with 34 slots and a deadline that moves, N errored CIKs rotate by
oldest-deadline instead of the lowest CIK monopolising every slot.

### 4.4 Selector — an EXCLUSION, not an ordering

Both readers move together (`_POLL_LANE_STATES` exists so they cannot diverge):

```
WHERE state = ANY(_POLL_LANE_STATES)
  AND next_poll_at <= now()          -- exclusion: a polled row is GONE for 7 days
ORDER BY next_poll_at ASC, cik_padded ASC
```

`_ciks_due` keeps its CIK-grouped shape; only the poll lane's `deadline_column`
changes. The `COALESCE(<deadline>, '-infinity')` inside the `MIN` window stays —
unreachable for the poll lane now (`NOT NULL`), still required by the recheck
lane, whose `next_recheck_at` is nullable.

### 4.5 Residual budget rollover

Today: `poll_budget = max_ciks * 2 // 3` (66), `recheck_budget = 34`. The recheck
lane holds 0 rows, so 34 of 100 slots buy nothing every hour.

Change: select the recheck lane FIRST at its guaranteed cap, then give the poll
lane `max_ciks - len(recheck_due)`.

- #1155 G13's guarantee is preserved exactly: recheck still gets up to 34, still
  read before any write.
- **[ckpt-1] Dedupe.** The two SELECTs are separate statements under READ
  COMMITTED, so discovery can promote a `never_filed` row to `current` between
  them and the same triple can appear in both lists. `new_filings_since` is
  additive in the UPSERT (`data_freshness.py:481`), so a double-apply
  double-counts. The poll list is filtered against the triples already claimed
  by the recheck list.
- **[ckpt-1] The rollover is one-way, deliberately.** Spare poll capacity is not
  returned to recheck. With 15,483 CIKs in the poll lane against 0 in recheck,
  the useful direction is the only one implemented; symmetric rollover needs a
  second SELECT for a case the measurement says does not occur.
- **[ckpt-1] Degenerate budgets** (`max_ciks` ∈ {0, 1}) keep today's behaviour —
  at `max_ciks=1` with recheck work pending, poll gets 0. Bounded, pinned by test.

### 4.6 Rollout and rollback **[ckpt-1]**

- Migration first; it is additive with a default, so pre-deploy code simply never
  writes the column and every row stays eligible — no worse than today.
- Post-deploy, the new writer advances it. Reader and writer ship in one commit.
- Rollback to the prior image leaves the column in place and unread; the old
  selector returns to `expected_next_at`, i.e. the old behaviour. Safe both ways.
- `ALTER TABLE` takes `ACCESS EXCLUSIVE`. On PG17 a non-volatile `DEFAULT` is a
  catalogue-only backfill (no rewrite), and the table is 55,775 rows, so the
  window is milliseconds. The index rebuild is ordinary (not `CONCURRENTLY`)
  because the repo's migration runner wraps each file in one transaction.

## 5. Reconciliation-latency derivation (ticket item 1)

**[ckpt-1] v1 conflated three different quantities.** Separated:

| quantity | value | what it is |
| --- | --- | --- |
| **revisit interval** | 7 days | the exclusion window; a *minimum*, not a bound |
| **initial sweep** | 15,483 / 100 = 155 ticks = **6.45 days** | one-off, to reach every CIK once from the backfill |
| **steady-state drain capacity** | 2,400 CIKs/day | 100 × 24 |
| **steady-state eligibility arrivals** | 15,483 / 7 = 2,212/day | every CIK returning on its own clock |
| **worst-case wait once due** | ≤ 1 tick (1 h) + queue depth | tick granularity, which v1 omitted |

At today's 66/hour the initial sweep is 235 ticks = **9.77 days**, which fails the
7-day rule; that is what §4.5's rollover buys.

Arrivals (2,212/day) < drain (2,400/day), so the queue drains rather than
saturates and the achieved revisit is ≈ 7 days + at most one tick.

⚠ The margin is **8%** and it is not structural. It assumes the recheck lane
stays near-empty — which §4.3 is what makes plausible, and which one persistent
error class could still break. **[ckpt-1] The stated mitigation in v1 was wrong:**
`max_ciks` is NOT an operator-tunable parameter. `_INVOKERS['sec_per_cik_poll']`
is registered through `_adapt_zero_arg`, which discards the params dict
(`app/jobs/runtime.py:290`), and `scheduler.sec_per_cik_poll()` passes no
override. Raising it is a code change, not an operator action. Recorded as a
known limit rather than a lever we do not have.

Do **not** shorten `POLL_REPOLL_INTERVAL` to manufacture margin: an interval
below the achievable cycle makes the interval meaningless and hides which
constraint is binding.

**[ckpt-1] Rate-limit note.** The 10 req/s SEC ceiling is a *rate*, not a daily
allowance, so dividing by 86,400 proves nothing. The real answer is that this
job's fetches pass through the shared cross-process GCRA gate (`sec_rate_gate`,
#1484) which enforces the ceiling regardless of what this budget asks for — and
the measured effect of that gate is §2.2's 16.6 s for 66 fetches.

## 6. Starvation argument (ticket item 2)

The recorded question was whether the selector survives "continuous NULL
arrivals". Under this design **there are no NULLs** — `next_poll_at` is
`NOT NULL DEFAULT now()`. That is the whole reason for the declaration, rather
than a nullable column with `NULLS FIRST`.

- A newly seeded subject enters stamped with its arrival instant and sorts
  **behind** everything already eligible. It cannot preempt the tail.
- A polled row is **excluded** for 7 days, not merely sorted later.
- Worst-case wait for any row = one cycle (§5), bounded and measurable.
- The migration's tie (15,483 rows at one instant) breaks on `cik_padded ASC` —
  the deterministic tie-break the ticket requires — so the first cycle is a
  single ordered sweep with no repeats.

**[ckpt-1] What this does NOT guarantee.** v1 claimed "the head cannot be the
same CIK twice inside a cycle". That is false and is withdrawn:

- Exclusion is **per row**, and `_ciks_due` returns only a CIK's *due* rows. A
  sibling row of the same CIK that becomes due later re-fetches that CIK. After
  the migration backfill all rows of a CIK are due together and advance together,
  so they run in lockstep; drift comes only from new seeds and lane crossings.
- The two lanes are selected independently, so a CIK with both poll-lane and
  recheck-lane rows costs two fetches. That is the pre-existing documented
  contract (`data_freshness.py:813`) and is unchanged here.

Both are bounded duplicate *fetches*, not starvation. Stated rather than fixed.

## 7. Explicitly NOT in scope

- **Per-CIK commits** (the previous session's item 3). **[ckpt-1] Confirmed
  worse than "unreachable": `connect_job()` defaults `autocommit=False`
  (`app/jobs/job_connection.py:54`) and `scheduler.sec_per_cik_poll` passes no
  override, so the entire run is ONE transaction and the nested
  `conn.transaction()` blocks are savepoints. A DB-level failure at the end rolls
  back every advanced clock.** Per-CIK fetch errors are caught and recorded
  individually, so this needs a database failure, not a network one. Unchanged by
  this work and not fixed by it.
- The recorded-not-fixed list on #3109: the swallowed `ValueError` truncating a
  rejected accession; manifest ownership last-writer-wins on collision;
  `sec_rebuild` not clearing HTTP validators; 404/malformed classifying as
  `current`; `sec_xbrl_facts` having no form mapping.
  **[ckpt-1]** Each of those can now advance a 7-day clock on a request that
  reconciled nothing. That is not a regression — they equally advanced
  `expected_next_at` before — but it is the reason **"a CIK was reached" is not
  "a CIK was reconciled"**, and §8 does not claim otherwise.
- **[ckpt-1] Historical completeness.** `submissions.json`'s `recent` array is
  capped (skill §7.4) and processing stops at the newest known accession, so
  rotation cannot repair holes below the watermark, corrections, or deletions.
  This change is about *reaching* subjects; the bulk/archive paths own coverage.
- **[ckpt-1]** `seed_scheduler_from_manifest` flips `error` → `current`
  unconditionally; a future `next_poll_at` then delays the first poll of a
  re-seeded row by up to the interval. Pre-existing shape, noted.
- `expected_filing_overdue` has **no writer** — `record_poll_outcome` produces
  only `current` / `error` / `never_filed`. It stays in `_POLL_LANE_STATES`
  because removing it would be a silent narrowing.

## 8. Acceptance

| criterion | how met | verification |
| --- | --- | --- |
| two successive cycles reach later subjects | the poll advances `next_poll_at` past `now()` | DB test, population **larger than the budget**, asserting run 2 is non-empty AND disjoint from run 1 **[ckpt-1: disjointness alone passes on an empty second run]** |
| 200-empty AND 304 both advance eligibility | both go through `record_poll_outcome` | DB test per path reading the column back from SQL **[ckpt-1: a mock cannot verify persistence]** |
| new filings still supersede old watermarks | watermark logic untouched | existing #1534 test stays green |
| old inactive filers do not monopolise | exclusion window | DB test: a 1994-dated row is not re-selected immediately after its poll |
| deterministic tie-break | `ORDER BY next_poll_at, cik_padded` | existing `_ciks_due` rank test extended |
| unused recheck budget rolls into due work | §4.5 | pure test at recheck-empty, recheck-full, and `max_ciks` ∈ {0,1} |
| errored rows rotate rather than pin | §4.3 | DB test: error → `next_recheck_at` is in the future, not NULL |
| no invented filing date to force progress | `last_known_filed_at` is never written here | the column is absent from the diff |

Post-merge live check: `scripts/measure_3109_batching.py --rotation` re-run over
consecutive hours — the discriminator of §2.1 (`polled-only`) must become
non-zero, i.e. polling now removes CIKs from the queue head.
