# Account reconciliation countdown — #2844 acceptance clause 3

*"Reconciliation job green for 5 consecutive days on demo before any live enablement."*

## Problem

Measured 2026-09-13 on the full stored dev population, before any design:

1. **There is no job.** `load_account_equity_evidence` has exactly one caller,
   `app/api/strategies.py:2285`. The verdict is computed on page load and discarded.
   Nothing persists a day, so the countdown has no record to count, no way to fail, and
   nothing to audit.
2. **Nothing counts.** `AccountEquityEvidence.days_collected` is `count(*) OVER ()` over
   `broker_account_equity_snapshots` — broker snapshot days collected, not reconciled
   days. There is no consecutive-green quantity anywhere in the repo.
3. **Nothing reads it.** No live-enablement path consults a reconciliation streak, so
   even a hand-counted five days would authorise nothing.

Two structural facts the clause depends on, both measured on the full population:

4. **The two comparands are stamped on different calendars.**
   `broker_account_equity_snapshots.snapshot_date = observed_at.date()`
   (`account_equity_evidence.py:231`) — a UTC calendar day, weekends included.
   `portfolio_eod_snapshots.snapshot_date = MAX(price_daily.price_date)` over held
   instruments (`portfolio_eod.py:317`) — a trading session. Of 17 stored demo broker
   days, 5 are Sat/Sun and **none** has a same-day local row. A calendar-day reading of
   "5 consecutive days" is unsatisfiable by construction.
5. **A session is decided a day late, sometimes three.** `portfolio_eod_snapshot` fires
   22:30 UTC, but `MAX(price_daily.price_date)` only advances at `orchestrator_full_sync`
   03:00 UTC, so it routinely stamps the previous session.

   ```sql
   select (computed_at at time zone 'UTC')::date - snapshot_date as lag_days, count(*)
     from portfolio_eod_snapshots group by 1 order by 1;
   -- 0:32  1:6  2:10  3:1  18:1
   ```

   The 18 is the 2026-09-12 recovery burst, which ran `portfolio_eod_snapshot` at
   22:48:13 before `daily_candle_refresh` at 22:48:55 and so re-stamped the 08-25 row.
   Excluding it, the observed maximum lag is **3**.

   Consequence: a job that evaluates only "the latest broker day" records a refusal every
   single day and the count never starts. The current live verdict is exactly that —
   `2026-09-12 refused (same_day_local_eod_snapshot_missing)`, a Saturday.

## Source rule

There is no published rule fixing a broker-reconciliation cadence, and none is borrowed.
That search was already done for the tolerance and recorded at
`account_equity_evidence.RECONCILIATION_RULE_VERSION = "f0-reconcile-v1"` (an earlier
draft's citation of SEC Reg NMS Rule 612 was withdrawn as off-point). The same finding
holds for the countdown: no regulator specifies how a firm must count consecutive days of
agreement between two internal valuations.

The countdown rule is therefore fixed **by construction** and frozen in
`COUNTDOWN_RULE_VERSION = "f0-countdown-v1"`. Every constant below is derived from a stated
measurement, not chosen.

Settled decisions consulted: `docs/settled-decisions.md` §"2026-08-22 — The allocation
boundary is the ONLY safety net" (names #2602 reconciliation as the boundary's evidence,
and fixes `capped ≡ fixed` / `expanding ≡ compound`). Nothing there constrains the
countdown; this spec extends it and reverses nothing.

## Scope: demo only

`portfolio_eod_snapshots` carries **no environment column** — it is the operator's one
local book. So a `real` verdict would silently consume the demo comparand. The countdown
is therefore defined for `environment = 'demo'` only, and `consecutive_reconciled_days`
refuses any other environment rather than returning a number. The live gate reads the demo
streak explicitly, never "the current runtime environment".

## Threat model

This gate exists to catch a **broken or drifting pipeline**, and it is fail-closed against
that. It is not, and cannot be, a control against an adversary who already holds write
access to the database or the repository: such an actor can `DELETE` the ledger, edit the
constants, or bypass the gate entirely. Where a finding below reduces to "someone with
write access could forge it", it is recorded as out of model rather than defended against.

## The countdown day

A **countdown day** is a date on which the held book had a closing mark — i.e. a date `D`
for which `price_daily` holds a row at `D` for at least one instrument held under the same
predicate `_resolve_snapshot_date` uses, verbatim: `position_id >= 0 AND units > 0` on
`broker_positions`.

⚠ **The obvious discriminator is wrong and was falsified before use.** `EXISTS (SELECT 1
FROM price_daily WHERE price_date = D)` — unrestricted — returns true for **16 of the 17**
stored broker days, Saturdays included, because ~308 instruments (crypto) carry weekend
bars:

```sql
select extract(isodow from price_date)::int dow, count(*), count(distinct instrument_id)
  from price_daily where price_date >= '2026-08-01' group by 1 order by 1;
-- 1:44938/11078  2:45567/11073  3:35911/11073  4:35862/11068  5:35773/11045
-- 6:616/308      7:1220/307
```

Restricting to the currently-held instruments returns **12 of 17**, every weekday and no
weekend — and it is the *same* population `portfolio_eod._resolve_snapshot_date` uses to
stamp the local comparand, so the countdown calendar and the comparand calendar are the
same object by construction rather than by coincidence.

⚠ **The calendar is the UNION of the derived sessions and every date already recorded in
the ledger for the current version** — a set, deduplicated. Both halves are required and
neither alone is sufficient:

- Derived sessions alone would let **calendar contraction delete failures**. Selling the
  one crypto holding removes the weekend sessions from `price_daily`-over-held, which
  silently deletes the refused weekend rows sitting between two greens and splices them
  into one run. A day that was once recorded is a day that happened; unioning the ledger's
  own dates in means a recorded failure can never be un-recorded by a change of book.
- The ledger alone would let **a day on which nothing ran be invisible**. The
  2026-08-26 → 2026-09-12 host outage produced neither broker rows nor verdict rows; only
  the independently derived side notices that those sessions existed and were never judged.

Two further consequences, stated rather than engineered around:

- **A held crypto makes weekends countdown days.** That is correct, not a loophole: if the
  book holds an instrument that marks on Saturday, Saturday has a local snapshot and is
  genuinely reconcilable. It does mean five countdown days need not be five business days.
  ⚠ A mixed crypto/equity book makes every weekend a *mandatory* day on which the equity
  legs are marked from a carried-forward Friday close; whether that reconciles is a
  question for the tolerance, not for this calendar.
- **Calendar expansion is fail-closed by construction.** Buying a weekend-marking
  instrument turns past weekends into countdown days with no verdict row, which breaks a
  streak. Selling the whole book empties the derived side, leaving only recorded dates —
  and a cash-only or empty book therefore cannot start a new countdown at all. That is a
  stated limitation, not an oversight: `_resolve_snapshot_date` falls back to *today* when
  no held instrument has a price, so a cash-only book's local snapshots are stamped on a
  calendar this rule deliberately does not recognise.

## Storage — `account_reconciliation_days`

Primary key `(environment, reconciliation_rule_version, snapshot_date)`.

| column | notes |
| --- | --- |
| `environment` | `demo` / `real` |
| `reconciliation_rule_version` | part of the key — see "version bump" below |
| `snapshot_date` | the broker snapshot date |
| `reconciliation_state` | `refused` / `reconciled` / `diverged`. `unavailable` is never stored — it means no broker day exists, so there is no day to record |
| `comparable` | the loader's single load-bearing flag |
| `countdown_rule_version` | the rule that produced this row's evaluation |
| `difference`, `tolerance` | evidence; may be populated on a refusal (the loader's implication, not biconditional) |
| `incomplete_reasons` | `text[]`, the refusal reasons verbatim |
| `first_observed_at` | never moves |
| `decided_at` | set exactly when `comparable` first becomes true; never moves after |
| `revision_count` | incremented on every undecided re-record, so a laundered day is visible |

Every column above is `NOT NULL` except `difference`, `tolerance` and `decided_at`. CHECK
constraints then make the state machine structural, not conventional:

- `comparable` ⟹ `reconciliation_state IN ('reconciled','diverged')`, `decided_at IS NOT NULL`,
  and `difference IS NOT NULL AND tolerance IS NOT NULL` (the loader's own implication)
- `NOT comparable` ⟹ `reconciliation_state = 'refused'`, `decided_at IS NULL`, and
  `cardinality(incomplete_reasons) > 0` — a refusal with no reason is not a refusal
- `revision_count >= 0`

⚠ **`tolerance >= 0` does not exclude NaN.** Postgres `NUMERIC 'NaN'` is not IEEE: it
compares greater than every number, so `'NaN' >= 0` is TRUE, and `NaN = NaN` is also TRUE,
which makes the usual `col <> col` test useless here. The constraint is written
`tolerance IS NULL OR (tolerance >= 0 AND tolerance <> 'NaN'::numeric)`, and the same
`<> 'NaN'` guard is applied to `difference`. A `NOT NULL` column under a one-sided CHECK is
exactly how a NaN gets in.

**Immutability rule: a decided verdict is frozen; an undecided one stays upgradeable.**

Enforced by a `BEFORE UPDATE` trigger that raises when the OLD row is `comparable`, not by
an `ON CONFLICT … WHERE` predicate alone. The predicate protects one write path; the
invariant has to hold against every path, including a hand-run `UPDATE`.

Both halves are load-bearing:

- **Freezing a decided day** is what makes the countdown mean anything. The loader's own
  docstring records that it re-loads FX rates at the local snapshot's `fx_rate_date`, so a
  later revision of a rate row moves the number; without the freeze a re-run could turn a
  `diverged` day green and nobody would see it.
- **Leaving an undecided day open** is what makes the measured lag survivable. The job on
  day `D+1` finds session `D` still missing its local row 11 times in 49 (lags 2 and 3
  above); freezing that refusal would make those days permanently red.

**Version bump.** `reconciliation_rule_version` is in the primary key, so bumping it does
not overwrite anything — it starts a parallel series. The counter reads only the *current*
version, so **a bump resets the countdown to zero**. That is the intended price: it removes
the "bump the version and re-verdict only the red days" move entirely, because a bump
invalidates the greens too.

## The job — `account_reconciliation_check`

Daily, 22:45 UTC, own lane, after `portfolio_eod_snapshot` at 22:30.

It does **not** evaluate only the latest day. Candidates are the broker snapshot dates for
`demo` that satisfy all of:

- `snapshot_date < (now() AT TIME ZONE 'UTC')::date` — **strictly in the past**. A
  same-UTC-day broker row is still mutable: `record_account_equity_snapshot`'s `ON CONFLICT`
  updates it while `snapshot_date = (now() AT TIME ZONE 'UTC')::date`. Deciding a day whose
  evidence can still change is how a green freezes before the divergent observation lands.
- `snapshot_date >= as_of - MAX_REPLAY_DAYS` where `MAX_REPLAY_DAYS = 30` calendar days —
  a bound on the scan, not a semantic. Older days can never count (see the due window), so
  nothing is lost by not re-visiting them.
- no row yet for the current `reconciliation_rule_version` with `comparable = true`.

Ordered oldest-first, **each day in its own transaction with its own `try`**, so a
permanent refusal — or an exception — at the oldest candidate cannot starve every newer day
behind it. A run with no candidates is a successful no-op and asserts nothing about
reconciliation; `row_count` is days recorded.

`load_account_equity_evidence` gains an optional `snapshot_date` parameter (`None` = latest,
preserving the existing API caller's behaviour verbatim). ⚠ The parameter is cast
(`%(snapshot_date)s::date`) — an uncast nullable filter raises psycopg3 `AmbiguousParameter`.
`days_collected` keeps its present meaning: computed over the environment-filtered set
*before* the date filter, so the `/strategies` panel does not change.

## The counter — `consecutive_reconciled_days`

A pure function over `(rows, countdown_days, as_of)` plus a thin loader, per the repo's
"prefer pure policy over real DBs" convention. `countdown_days` is the independently
derived calendar; `rows` is the stored ledger keyed by date.

Constants, each from a stated measurement:

| constant | value | derivation |
| --- | --- | --- |
| `REQUIRED_GREEN_DAYS` | 5 | the clause's own number |
| `MAX_DECISION_LAG_DAYS` | 4 | max observed local-snapshot lag (3, excluding the recovery artefact) + 1 for the job's own next fire |
| `MAX_EVIDENCE_AGE_DAYS` | 12 | `MAX_DECISION_LAG_DAYS` + a full week of market closure + 1 |
| `MAX_STREAK_SPAN_DAYS` | 14 | 5 countdown days + 2 weekends + a holiday |

A countdown day `D` is **due** once `as_of - D > MAX_DECISION_LAG_DAYS`.

```
1. countdown_days := sorted set of dates <= as_of, descending.   # deduplicated, never future
2. due            := [D in countdown_days if as_of - D > MAX_DECISION_LAG_DAYS]
3. not_yet_due    := countdown_days \ due
   if any D in not_yet_due has a DECIDED row that is not 'reconciled'  -> 0
4. if due is empty                                     -> 0
5. if as_of - due[0] > MAX_EVIDENCE_AGE_DAYS           -> 0   # the tail is stale
6. run := 0
   for D in due:                                             # contiguous, descending
       row := rows.get(D)
       if row is None                                  -> stop   # a day nothing recorded
       if row.countdown_rule_version != COUNTDOWN_RULE_VERSION -> stop
       if not row.comparable                           -> stop   # still refused past its due date
       if row.state != 'reconciled'                    -> stop   # diverged
       if row.decided_at.date() < D                    -> stop   # decided before the day it judges
       if row.decided_at.date() - D > MAX_DECISION_LAG_DAYS -> stop   # backfilled, not observed
       run += 1
       if run == REQUIRED_GREEN_DAYS: break
7. if run > 0 and due[0] - D_last_counted > MAX_STREAK_SPAN_DAYS -> 0
8. green := run >= REQUIRED_GREEN_DAYS
```

Each stop condition closes a specific bypass, and they were not all obvious:

- **There is no "skip the pending prefix" rule**, deliberately. An earlier draft skipped
  leading `comparable = false` rows as "not yet decided". That is a refusal bypass: a
  *permanent* refusal — an undocumented account currency, an unvalued short book, an
  incomplete local valuation — would sit at the head forever while five old greens kept the
  gate green. The due window replaces it: a day gets exactly `MAX_DECISION_LAG_DAYS` to
  decide, and after that a refusal is a failure like any other.
- **Step 3 is why the grace window is not itself a hole.** Days inside the window are
  excluded from *counting*, but a day already decided `diverged` inside it is a known,
  current failure, and live eligibility must not coexist with one.
- **The `decided_at` clause is what stops replay.** Without it, a first install could
  evaluate thirty historical days in one execution and be green immediately, which is not
  "the job ran green for five days" in any sense. Its practical bound: at most
  `MAX_DECISION_LAG_DAYS` days can be decided by one execution, so five green days need at
  least two executions on two distinct days. ⚠ **The countdown counts days of EVIDENCE, not
  job executions** — that is the claim it makes, and it is bounded rather than unlimited.
- **Step 7 stops a sparse calendar from splicing.** Without it, five timely greens scattered
  across five months pass the moment the newest is five days old, because "consecutive"
  would mean consecutive *in a calendar with holes*.
- **A missing row is a stop, not a skip**, which is what makes the calendar's independence
  from the ledger load-bearing. It is also why a dead job resets the count without any
  heartbeat check: `price_daily` keeps advancing, so new countdown days keep appearing with
  no row, and the run breaks at the newest of them.
- **`decided_at.date() < D` is rejected** — a verdict cannot predate the day it judges.

Return type is a dataclass, not a bare int: `green_days`, `required_days`,
`newest_counted_date`, `green`. ⚠ The loader does **not** catch database errors and return
0 — swallowing an exception inside a caller's transaction leaves that transaction aborted,
which is a worse failure than propagating.

## Readers — the counter must not be a writer with no reader

1. **Live gate.** `LiveGateFacts` gains `account_reconciliation_green_days: int`;
   `live_gate_refusals` gains `account_reconciliation_streak_insufficient`. Appended
   **after** `live_strategy_broker_contract_not_validated`, which is unconditional — so the
   new code can never be `refusal_codes[0]` and the function's documented order contract is
   untouched. (An earlier draft put it before that line and claimed the order was preserved;
   it is not, when every earlier check passes.) A read error yields 0, not a pass.
2. **Operator panel.** The `/strategies` account-equity evidence block gains
   `reconciliation_green_days`, `reconciliation_required_days` and the date of the newest
   counted day; `StrategiesPage` renders `n / 5`. ⚠ The panel's existing verdict is the
   *latest* day recomputed live, while the streak is *frozen history* — they can legitimately
   disagree, so the countdown is labelled with its own as-of date rather than sitting
   unlabelled next to the verdict.

## Known limitations, stated not engineered around

- **`environment` is not an account identifier.** Replacing the demo credentials with a
  different demo account splices two books into one streak. Nothing in the schema
  identifies an account; recording this rather than inventing one.
- **The local snapshot is not a faithful per-session observation.** `_read_positions` reads
  *current* `broker_positions`, so a snapshot for session D is D's closes applied to the
  book at run time. Already recorded on #2844 and a #2602 item. It means a green day says
  "the two valuations of today's book agree", which is the honest claim the clause needs,
  and not "the book on day D reconciled".
- **A static book is a weak test.** Five green days on a book that never trades exercises
  valuation agreement, not trade attribution.
- **Aggregate agreement is not per-holding agreement.** Two offsetting errors of equal value
  net to `reconciled`. The position-count check in the loader is the only structural guard
  and it is a count, not a match. Inherited from #2602, not introduced here.
- **A cash-only or empty book cannot start a countdown** — see the calendar section.
- **Later corrections to the local snapshot or FX cannot revoke a frozen green.** The freeze
  is what the countdown is for; correcting a decided day is a
  `RECONCILIATION_RULE_VERSION` bump, which resets the whole count.
- **Out of model** (see Threat model): direct `DELETE`/`TRUNCATE` of the ledger, backdated
  `decided_at` written by hand, constant edits under a reused version label, and swapping
  the demo credentials for a different demo account.

## Tests

- Pure table tests for the streak: empty calendar; all-green-5; green-4; a `diverged` in the
  middle; a missing countdown day; the outage gap; a permanent refusal at the head; a
  not-yet-due head above five greens; a `diverged` day inside the grace window; a backfilled
  `decided_at`; a `decided_at` before its own day; a stale tail; a sparse calendar spanning
  more than `MAX_STREAK_SPAN_DAYS`; duplicate calendar dates; future-dated evidence; a stale
  `countdown_rule_version` in the run.
- DB tests for the freeze: a decided row does not move on re-record; a decided row cannot be
  moved by a direct `UPDATE` (the trigger raises); an undecided one upgrades and increments
  `revision_count`; a weekend day is absent from the countdown calendar.
- Live-gate test: the refusal fires below 5 and clears at 5.

## Not in scope

- The tolerance constant and `RECONCILIATION_RULE_VERSION`. Widening either to start the
  count is the shortcut the standing prompt names explicitly.
- Ordering `portfolio_eod_snapshot` after `daily_candle_refresh`. The 2026-09-12 stale
  re-stamp is a catch-up-burst artefact that the next ordinary fire self-heals; recorded on
  the issue, not fixed here.
