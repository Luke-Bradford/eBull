# #3118 — the core preflight's quote freshness, and the window the cadence cannot reach

Status: shipped. Revised twice — after Codex checkpoint 1 (25 findings) and again after the
repo's own `check_pooled_conn_across_http` guard refused the ticket's preferred fix. Target:
`app/workers/scheduler.py`, `app/jobs/runtime.py`, `app/jobs/sources.py`,
`scripts/census_3118_core_quote_freshness.py`.

## The ticket's premise is falsified, and the defect is on a different axis

#3118 says the bound is 1 hour against an hourly producer, so the margin is zero.

Measured at `ab62dde0` (`PYTHONPATH=. uv run python -c "from app.services import
strategy_core_preflight as p; print(p.CORE_MAX_QUOTE_AGE_SECONDS, p.CORE_MAX_HALT_FEED_AGE_SECONDS)"`):

| constant | ticket says | actual |
| --- | --- | --- |
| `CORE_MAX_QUOTE_AGE_SECONDS` | 3600 | **5400** |
| `CORE_MAX_HALT_FEED_AGE_SECONDS` | 300 | **450** |

Both are `_freshness_bound(period) = period * 3 // 2` (`strategy_core_preflight.py:88-107`,
landed `f5a3347c`). Its docstring states the usable interval `[period, 2*period)`, picks the
midpoint so the bound tolerates **half a period of scheduler lateness**, and records that the
position within that interval is a choice frozen in `CORE_PREFLIGHT_POLICY_VERSION`
(`core-preflight-v2`). `tests/test_2603_core_preflight.py:300-327` asserts the interval against
the scheduler's own registered cadence, so the coupling is already enforced. The producer is
healthy too: `quotes_refresh` posted **72 `success` runs in the last 72 h**, inter-arrival
exactly 3600 s.

So the ticket's fix direction 2 ("decouple the two numbers") is half done already, and its
warning against widening the bound to 2 h points at a bound that is already 1.5 h.

**The defect is real but it is not a cadence-margin defect.** `quoted_at` is a *price*
timestamp, not a *fetch* timestamp, so no fetch cadence can make it fresher than the price the
provider is willing to quote — but a fetch cadence CAN fail to be running when the price first
becomes fresh, and that is what happens at every US open.

## Source rule

`https://api-portal.etoro.com/api-reference/market-data/get-instrument-market-rates.md`,
re-read live 2026-09-16 per `.claude/skills/data-sources/etoro-api.md`. The rates response
carries **`date` (string, date-time) — "The date-time of the price in the system"**, and the
endpoint's quota is **120 requests / 60 s, shared across 11 endpoints**.

⚠ That sentence is the whole of the documented semantics, and it is weaker than it is tempting
to read. It does **not** say `date` is an exchange execution stamp, that it freezes at the
closing bell, or that it advances only on trades. Everything beyond those words below is an
observation of our own corpus, labelled as such.

`app/providers/implementations/etoro.py:708-715` parses that field into `Quote.timestamp`, and
`market_data._upsert_quote` (`:1786`) writes it to `quotes.quoted_at` under an `ON CONFLICT`
whose `WHERE` makes the write **monotonic in `quoted_at`** — a REST snapshot cannot clobber a
fresher WS tick.

### Two pre-existing provider-parsing hazards this change does not fix

1. **`now()` fallback.** When `date` is absent or unparseable, `etoro.py:713`/`:715` substitute
   `datetime.now(UTC)` — retrieval time standing in for price time, which manufactures exactly
   the freshness a bound exists to test. `market_session_support.py:172` already records it.
   Not disprovable from stored state (a fallback stamp and a genuinely current one are
   indistinguishable once written), and not fixed here: `Quote` has no field that can express
   "the provider omitted the date", the same `has_*`-flag problem the eToro skill records for
   `LastExecution`.
2. **Naive timestamps.** `datetime.fromisoformat` on an offset-less string yields a naive
   datetime, which `_upsert_quote` hands to a `timestamptz` column and Postgres localises to
   the session TimeZone. The observation lane refuses those explicitly
   (`strategy_core_quote_observation.py:117-118`, `quote_timestamp_naive`); the `quotes` writer
   does not.

Neither is introduced or worsened in kind — the new job runs the same parser the hourly one
does — and both are one-line-locatable if a later ticket takes them.

## Measurement — the stored lane, and what it is a population OF

`strategy_core_quote_observations` (sql/366) is the hourly immutable lane, written from
`quotes_refresh`'s own tick: **1,590 rows, 10 core candidates, 159 ticks each, since
2026-08-23**. Reproduce with
`PYTHONPATH=. uv run python -m scripts.census_3118_core_quote_freshness --instrument-id 3417`.

⚠ This is the complete stored population of that lane, which is **not** the population of
session minutes. The lane samples once an hour at `:23`; it can say what each fetch returned
and nothing about any moment between two fetches.

Observed:

- Of the ten candidates, exactly **two are `us_equity`** — SPY.RTH (3417) and QQQ.RTH (3418) —
  and `SESSION_SUPPORTED_ASSET_CLASSES == frozenset({"us_equity"})`, so only those two can pass
  the session gate at all. Which of them a mandate selects is #2833's question, not this one.
- Both read **28 of 159** ticks `observed`; the seven non-US candidates read 45-48 of 159.
- **Every** non-`observed` row across all ten is `observation_status='invalid'`,
  `refusal_reason='quote_stale'` — 1,590 of 1,590 ticks accounted for, not one
  `provider_omitted_quote`. So eToro returns a quote for these ids around the clock; what it
  does not return outside a venue session is a *recent* `date`.
- For 3417 the `observed` ticks are exactly the **14:00–20:00 UTC** buckets, 7 of 7 on each of
  the four days the lane covered it (2026-08-24, 08-25, 09-14, 09-15); the 13:00 bucket is
  `observed` on **zero** of them. CSPX.L (3434) shows the same shape on LSE hours
  (07:00–16:00 UTC), which is the rule seen from a second venue rather than a second finding.

⚠ The lane's own age bound is `MAX_QUOTE_AGE = 1 h`
(`strategy_core_quote_observation.py:57`), **not** the preflight's 5400 s, and an `invalid` row
discards the timestamp. So `quote_stale` in the lane does **not** by itself establish that
`decide_core_preflight` would refuse. What does: the last `observed` row of a US day — the
2026-09-15 20:00 bucket carries `quote_at = 19:59:54Z`, and `quotes.quoted_at` for 3417 still
read that stamp at 11:25Z the next morning, an age of **55,542 s** against a 5400 s bound,
measured directly off the row the preflight reads.

### The blind window, derived

DERIVED from two published rules, not sampled — the census prints it rather than asserting it:

- NYSE regular open is 09:30 America/New_York (`market_session_support.py:67`) = 13:30 UTC on
  2026-09-16.
- `quotes_refresh`'s registered cadence is `hourly(minute=23)`, read from `SCHEDULED_JOBS`.

The last fetch at or before the open is 13:23 UTC — seven minutes early, when the newest price
eToro will quote for 3417 is still the previous session's — and the next is 14:23 UTC. Census
output: `nyse: open 13:30Z on 2026-09-16 — first fetch at or after it 14:23Z — uncovered 3180s
(53 min), bound 5400s`.

⚠ Conditionality, stated rather than hidden:

- The gap is **3180 s for any `:30` open against a `:23` cadence**, so DST moves the clock time
  (14:30Z open in winter) and not the gap. Early closes move the close, not the open.
- It assumes no other writer fills the hole. `etoro_websocket.upsert_quote` is the only other
  writer and is visibility-driven — nothing unless an operator has that instrument on screen —
  so it is not a producer for an unattended path. If an operator does, the hole can close by
  accident, which is not a control.
- It assumes eToro's `date` for 3417 advances at the open. The lane has **no sample** between
  13:23 and 14:23, by construction, so that is unobserved in either direction.

## Why the ticket's preferred fix is NOT available

Direction 1 is "fetch a fresh quote on demand immediately before a core evaluation". It was
built first, and `scripts/check_pooled_conn_across_http.py` refused it:

```
app/api/strategies.py: route `rebalance_core_sleeve` holds a pooled conn (Depends(get_conn))
across an external-provider HTTP call
```

`EtoroMarketDataProvider` is an `EXTERNAL_MARKERS` name and the route takes `Depends(get_conn)`.
The three ways out are all closed:

- **Waive it** — `tests/lint/test_check_pooled_conn_across_http.py::test_allowlist_is_empty`
  asserts `ALLOWLIST == frozenset()`, deliberately re-armed by #1492. Editing that test is
  disarming a gate.
- **Move the construction into a service** so the route body stops naming the marker — the
  guard's own scope note says it does no transitive analysis, so this changes the hazard not at
  all and only hides it.
- **Stop taking `Depends(get_conn)`** (the #1492 fix) — impossible for this route: it holds
  SESSION advisory locks (`core_submission_lock`, `reconciliation_order_lock`) across the broker
  I/O by design, which requires one connection held for the whole request.

⚠ Note the route already holds the pooled conn across external HTTP via `EtoroBrokerProvider`,
which is simply not in `EXTERNAL_MARKERS`. That is an under-modelling in the guard, not a
licence; it is recorded here and not acted on.

The refusal pointed at the better fix anyway: an on-demand fetch would only have helped the
**attended endpoint**, while the hole is in the data and would still be there for every future
unattended rebalance.

## Design — a second, narrow producer at a cadence that can reach an open

The ticket's direction 2, done on the producer side: "refresh at a period materially shorter
than the bound and record why the ratio is what it is."

`core_candidate_quote_refresh` — `Cadence.every_n_minutes(interval=5)`, lane `etoro_quotes`,
scope = arms 4 and 5 of `QUOTES_REFRESH_SCOPE_SQL` and nothing else. Ratio: bound 5400 s,
producer period 300 s, **18×**.

- **Scope is COMPOSED, not copied.** The arm-4 (enabled mandate's instrument) and arm-5 (proved
  candidate) predicates are extracted to `_CORE_MANDATE_INSTRUMENT_PREDICATE_SQL` and
  `_CORE_CANDIDATE_PREDICATE_SQL`, and both queries are f-strings over them. Verified on the
  full dev population: the refactored `QUOTES_REFRESH_SCOPE_SQL` returns **1,608 rows, 10 core
  candidates — byte-identical result set to the pre-refactor query**. The narrow query returns
  exactly those 10.
- **Same lane as `quotes_refresh`, deliberately.** #2934 gave that lane to the hourly job so the
  core-sleeve population could not wait behind the multi-hour candle sweep; a job that is one
  batched GET and ten upserts cannot reintroduce that. The #1526/#1527 same-lane tick race needs
  an ALIGNED slot, and `:23` against multiples of five never aligns.
- **No session prerequisite.** A session predicate on the producer is the exact coupling #3118
  is about. Cost of running around the clock: the cohort is ten instruments against
  `quote_batch_size` 100, so one upstream request per fire — **288/day** against a 120-per-60 s
  shared budget.
- **It does not write `strategy_core_quote_observations`.** `observe_instrument_ids` is not
  passed: that lane's primary key is an hourly `sample_bucket` and `quotes_refresh` stays its
  sole writer, because #2833's pass bar reads it.
- **`batch_error` re-raised inside `_tracked_job`**, the #2218 shape — a total upstream failure
  must not report as a clean run that found no quotes.

### ⚠⚠ The bound does NOT move, and the ordering is the decision

`CORE_MAX_QUOTE_AGE_SECONDS` stays 5400 s, still derived from `quotes_refresh`'s hourly period.
Re-deriving it from 300 s would tighten it 18-fold — safer in steady state and **strictly more
dangerous on the first attended evaluation**, because a bound tightened ahead of any run history
for the producer it now depends on can refuse for a reason nobody has observed. The attended
demo session is 2026-09-18.

Consequence, stated rather than left to be rediscovered: **until that second step, the bound is
LOOSE against the new producer.** A stopped `core_candidate_quote_refresh` is not detected for
up to 5400 s, exactly as today. The realised age improves; the guarantee does not. Tightening it
is a `CORE_PREFLIGHT_POLICY_VERSION` bump, never an edit to the constant, and the coupling test
in `tests/test_2603_core_preflight.py` would have to read this job's cadence instead.

### What this does NOT improve

**Sizing.** `amount = broker_verdict.amount` (`strategy_core_executor.py:665`) comes from the
broker what-if, not from `quotes`. The DB quote is a **gate**, not a sizing input, so a fresher
row removes a false refusal and changes no order's size (Codex 25).

## Codex checkpoint 2 — the one P1, and the constraint that decided the fix

> *"`execution_lane_for()` reserves quote capacity only for `JOB_QUOTES_REFRESH`; the new job
> therefore uses the default executor and the single `general_non_sec` semaphore despite its
> `etoro_quotes` source. This can delay or discard its fires throughout the opening window."*

Real, and it defeats the whole change: the general lane has ONE permit shared by ~50 jobs
including multi-hour backfills, so a five-minute fire can queue for hours and be discarded by
the 1-second `job_defaults` grace.

The obvious fix — a reserved lane of its own — is **not available, and that was measured rather
than assumed**. A new lane costs `1 permit × JOBS_NON_SEC_CONNECTIONS_PER_EXECUTION = 2`
connections, and on dev:

```
usable = max_connections − superuser_reserved_connections = 27
demand = _dev_profile_connection_demand() + CONNECTION_BUDGET_RESERVE = 27   → headroom 0
```

Boot would raise `ConnectionBudgetExceeded`, whose own message rejects raising
`max_connections` as a remediation.

So the job **joins the existing quote lane**, and `build_scheduler_executors` is re-sized from
`permits` to `max(permits, members)`:

- **Permits stay at 1**, so the two quote jobs serialise on the semaphore exactly as they
  already do on the `etoro_quotes` source lock, and the connection budget is byte-for-byte
  unchanged (verified: still 27).
- **The pool grows to 2 threads**, which is what the second member actually needs — permits
  bound how many bodies may RUN, members bound how many fires may be waiting to DISPATCH.
- `misfire_grace_seconds` is half the interval (150 s), derived so a fire can absorb a wait
  behind `quotes_refresh`'s 40-90 s body but can never outlive its own slot.

⚠ `test_exactly_one_registered_job_per_reserved_lane` (#2985) is REPLACED, not deleted, by
`test_every_reserved_lane_has_a_dispatch_thread_per_member`. The old assertion pinned the count;
the property its own docstring named was *"one worker per reserved lane is sufficient ONLY while
one job uses it"*. Stated as `workers >= members` that condition is explicit, and a second
member can only be admitted together with the thread that stops it queueing against the first.
The concurrency bound is not relaxed.

⚠ `test_grace_opt_in_is_an_explicit_allow_list` also had to be edited — deliberately, which is
what it exists to force. The set is now three names.

## Tests

Six added to `tests/test_2603_core_preflight_db.py`, the module that already owns the scope
arms:

1. Both predicate constants appear in BOTH queries — the agreement, not a transcription.
2. Arm 4 alone (enabled mandate, no proof) admits to the narrow scope.
3. Arm 5 alone (passing proof, no mandate) admits.
4. **What it REJECTS**: a Tier-1 instrument is in the hourly scope and NOT in the five-minute
   one.
5. A disabled latest revision drops out, by the same constant rather than a second copy of the
   rule.
6. The narrow scope is a subset of the wide one.

Three more in `tests/test_jobs_runtime.py`:

7. The reserved-lane invariant, re-stated as `workers >= members`.
8. The new job is on the reserved quote lane and did not take the hourly job's reservation
   (permits still 1).
9. Its misfire grace is strictly inside its own period, derived from the interval constant.

## Acceptance

The ticket asks that "a core evaluation run at a randomly chosen minute within the US session
does not refuse `core_quote_stale`, and the reason it does not is recorded (fresh fetch, or
measured margin), not assumed."

- **Measured margin** — supplied, and negative: the census shows the hourly lane cannot cover
  13:30–14:23 UTC, so that half of the ticket's own framing is answered rather than assumed.
- **Fresh fetch** — supplied by a producer whose period is 18× under the bound, so a randomly
  chosen session minute is at most 300 s (plus scheduler lateness) from a fetch.
- ⚠ Not claimed: the end-to-end observation. It needs a live evaluation inside a US session,
  which is the attended run and is `loop-ineligible`. If eToro's first `date` of the session
  lands later than 13:30, the 13:35 fetch still returns a stale stamp and the preflight still
  refuses — correctly, there being no fresh price to gate on. What this removes is *our*
  contribution to the age, not the venue's.

## Codex checkpoint 1 — what it changed

Two findings changed the first design and survive into this one:

1. **Finding 1-2 — an implicit transaction spanning the fetch.** The on-demand helper's symbol
   lookup, as a bare `SELECT` on a non-autocommit connection, opened a transaction that spanned
   the provider round trip and degraded `refresh_quotes`' own `conn.transaction()` to a
   savepoint. Reproduced against the built implementation before it was dropped (the regression
   test read `INTRANS` at call time). It is why `core_candidate_quote_refresh` opens its
   connection with `autocommit=True`, the same reason `quotes_refresh` does.
2. **Finding 25 — the sizing overclaim**, carried into "What this does NOT improve" above.

The rest are absorbed as stated non-guarantees and labelled conditionality (7, 8, 9, 11, 12, 14,
15, 16, 17, 18, 19, 20, 22, 23) or were already true of the checkout (21).

Refs #3118. Refs #2603. Refs #2437.
