# Scheduled core-eligibility revalidation (#2603 item 2, the revalidation half)

Status: proposal — **post-Codex-ckpt-1 revision.** Codex returned 41 findings; the ones
that changed the design are listed in the table at the end, and several first-draft
claims below are reversed rather than softened. Refs #2603 (the proofs table, its recorder
and the freshness rule), #2947 (the feasibility screen this unblocks), #2833 (the sleeve
whose candidates are the current proved set), #2946 (the write-lane finding this inherits
and does not fix).

## The gap, measured

#2603 item 2 asked for an eligibility proof *"with a declared freshness/revalidation rule
— eligibility is not immutable."* The **freshness** rule shipped; the **revalidation**
never did.

- `CORE_ELIGIBILITY_MAX_AGE = timedelta(hours=24)`
  (`app/services/strategy_core_eligibility.py:58`), fixed by construction with its
  reasoning inline.
- `require_core_eligibility` (`:454`) raises on a proof past it, and the attended
  executor calls it twice — `strategy_core_executor.py:438` (pre-lock) and `:548` (the
  binding re-read inside `core_submission_lock`).
- The **only** writer of `strategy_core_eligibility_proofs` is
  `scripts/prove_2603_core_eligibility.py` (`prove` mode; `census` records nothing and
  says so). `rg 'strategy_core_eligibility_proofs' app/jobs app/workers` returns two
  hits, both READS inside `QUOTES_REFRESH_SCOPE_SQL`.

So more than 24 hours after a hand-run `prove`, the two consumers that bound freshness are
failing: `require_core_eligibility` raises, and #2947's feasibility screen returns
`indeterminate` / `eligibility_proof_stale` — reproduced on #2833's three candidates on
2026-09-13, twice, either side of a manual re-census.

⚠ **Precision the first draft lacked** (Codex 39). This is not "every consumer is
failing". `QUOTES_REFRESH_SCOPE_SQL` deliberately does **not** bound proof age, so quote
scope is unaffected by staleness; `core_rebalance_observation` requires no proof at all.
The claim is narrow and is the whole of it: *the two age-bounded consumers cannot pass
without a hand-run script.*

⚠ This is a gap, not a decision. The module docstring's *"Item 3 re-proves at execution
time"* describes re-**checking** the stored proof, which is what `require_core_eligibility`
does; nothing re-**fetches** from the broker on any scheduled path.

## Source rule

- **Freshness bound:** `CORE_ELIGIBILITY_MAX_AGE`, already fixed by construction in
  `strategy_core_eligibility.py:52-58` and cited to the live portal (2026-08-13: the
  eligibility response carries no freshness field of its own, unlike the what-if cost
  endpoint's `lastUpdated`). This proposal introduces **no new staleness constant** — its
  refresh trigger is derived from that one.
- **Request budget:** eToro documents the eligibility endpoint at **20 requests/minute
  dedicated**, at most **100 ids per request** (live portal 2026-08-13, recorded at
  `scripts/prove_2603_core_eligibility.py:52-56`).
- **Observation clock:** `observed_at` is the database's `now()`, deliberately —
  `record_core_eligibility_proof`'s signature omits it *"so no caller can extend a proof's
  validity by declaring its age."* Age is therefore computed **in SQL against `now()`**,
  never from a caller-supplied clock (Codex 22).
- **Transport failure must not be stored:** the recorder's own contract — *"A transport
  failure writes NOTHING. The caller must not funnel a provider exception into this
  function: absence of evidence stored as an observation turns 'we could not ask' into
  'the broker said'."*
- **Verdict vocabulary:** `strategy_core_eligibility.py:74-76` — *"`unresolved` means the
  response did not answer the question; `not_underlying` means it answered and the answer
  is no. Only the second is a fact about the instrument."*
- **Informational, not a mutation:** `check_instrument_eligibility` posts to
  `_v2_info_prefix`/`eligibility` and is deliberately **not** covered by
  `refuse_broker_mutation_if_unattended`; the prevention-log entry on #2645 records that
  guarding the transport instead of the five mutating methods would have re-imposed the
  other half of that error. Nothing here changes that.

## Scope of the refresh — what is and is not bounded

⚠⚠ **Proof-table membership is load-bearing beyond eligibility.**
`QUOTES_REFRESH_SCOPE_SQL` arm 5 admits an instrument to the quote-refresh scope when its
latest proof per environment passes, and the comment there states the bound explicitly:
*"Bounded by deliberate action: only `prove` writes proofs (`census` records nothing), so
this arm cannot widen to the universe on its own."*

> **Selection rule: the job re-proves only `instrument_id`s that already appear in
> `strategy_core_eligibility_proofs` for this `(operator_id, provider, environment)`. It
> can never introduce an instrument id the table does not already carry.**

⚠ **The first draft then claimed "membership can therefore shrink or stay, never grow".
That is FALSE for quote scope and Codex was right to refuse it (9, 10).** Two separate
sets, and conflating them was the error:

| set | can this job grow it? |
| --- | --- |
| `instrument_id`s present in the proofs table | **No.** That is the invariant above, and the quotes comment's "cannot widen to the universe" survives it. |
| instruments passing `quotes_refresh` arm 5 | **Yes.** An instrument whose latest proof is `not_underlying`/`unresolved` and whose next is `underlying` ENTERS quote scope. |

The second is a genuine behaviour change: a previously rejected name can re-enter quote
scope with no operator action. It is bounded by the first — only ever within the
already-proved set — and it is the correct direction, since the broker has said the
instrument now qualifies. Named here rather than discovered by an operator.

⚠ **Do not read a non-passing verdict as "the instrument stopped qualifying"** (Codex
14, 15). `unresolved` is not evidence about the instrument at all, per the vocabulary rule
above; and `not_underlying` covers `instrument_not_open` as well as `no_underlying_arm`
(`NOT_UNDERLYING_REASONS`), the first of which is an **open-permission** fact, not an
ownership one — ownership is `settlementType`, which `broker_settlement_arms` owns. The
accurate statement is mechanical: *the latest observation no longer satisfies the
predicate that quote scope and `require_core_eligibility` both key on.* That, and not an
interpretation, is why membership moves.

⚠ **Consequence for #2833's sample** (Codex 16): quote-scope departure interrupts the
spread-percentile accrual its acceptance bar reads, so an interruption conditions that
sample on broker responsiveness. This job does not evaluate that bar and takes no view on
it; the interruption is recorded in `job_runs` and in the proof row, so #2833 can see it
rather than inherit a silent hole. Flagged to #2833, not resolved here.

⚠ **Historical membership is permanent enrolment** (Codex 18): an instrument proved once
is refreshed forever, with no withdrawal mechanism. Accepted for now at a 20-instrument
population; a withdrawal path is a separate ticket if the set grows.

### Selection predicate, and why credential ids appear in it

An instrument is selected when **either**:

1. its latest in-triple proof is older than `CORE_ELIGIBILITY_REFRESH_AGE`; **or**
2. its latest in-triple proof was observed under credential ids that are **not** the
   current live pair.

Arm 2 is Codex 2: without it a credential rotation leaves every proof unusable to
`require_core_eligibility` (which compares the pair, `:496`) for up to 13 hours while the
age rule says "fresh". A proof that cannot be used is not fresh in any sense the
consumers care about, so re-asking is free of the staleness argument.

Selection keys on `(operator_id, provider, environment)` and **not** on the credential
pair, so the rotation case is selected rather than hidden by an empty result. Arm 2 reads
the live pair with `live_credential_ids_unlocked` — advisory, deciding what to ask. The
**write** attributes under the LOCKED `live_credential_ids`, which the unlocked reader's
own docstring requires: it is *"NOT acceptable for anything that writes a proof-backed
authority."*

⚠ **Residual race, pre-existing and not closed here** (Codex 1): `_load_etoro_credentials`
returns plaintext keys only, loaded and committed separately from any id read, so a
rotation between the key load and the id lock can stamp one account's evidence with the
other's ids. `scripts/prove_2603_core_eligibility.py::_credentials` has the identical
shape. The job **reuses that helper rather than writing a third** version, and the race is
named here. Closing it needs the credential loader to return ids with the plaintext — a
change to a shared auth path, out of scope.

## Cadence and the derived trigger

`Cadence` offers `every_n_minutes`, `hourly` and `daily`; there is no `every_n_hours` and
this proposal does not add one.

- Cadence: **`hourly`**.
- Trigger: `CORE_ELIGIBILITY_REFRESH_AGE = CORE_ELIGIBILITY_MAX_AGE / 2` (12h). **Derived
  from the existing constant**, not chosen independently.

By construction, with an hourly tick, an instrument's proof is refreshed at the first tick
after it passes 12h — so worst-case age is `REFRESH_AGE + 1 tick` and there are **11
nominal ticks** of margin before it crosses `MAX_AGE`.

⚠ **"Nominal" is load-bearing** (Codex 19). That margin counts scheduler ticks, not
wall-clock: lane contention, request spacing, HTTP retries and position in the batch all
consume real time the inequality does not model. It is a design margin, not a guarantee.
The invariant a test pins is `REFRESH_AGE + one registered tick < MAX_AGE`, read from the
**registered** `ScheduledJob.cadence`, not from a literal `timedelta(hours=1)` — a literal
would keep passing if the job were later moved to `daily` (Codex 20).

**Oldest-first ordering** is the starvation control (Codex 21): the selection orders by
proof age descending, so after downtime the most-expired instruments are refreshed first
and a per-run work cap cannot permanently starve the same tail.

⚠ **Accepted asymmetry, named rather than fixed** (Codex 17): a transient `unresolved`
response writes a fresh row and therefore suppresses another attempt for ~13h, whereas a
transport exception writes nothing and retries on the next tick. This is deliberate — an
`unresolved` proof authorises nothing, so 13h of one costs exactly what 13h of no proof
costs, and re-asking hourly on an ambiguous answer is the unbounded polling #2946's lane
work exists to prevent. If it proves wrong, the fix is a separate trigger arm, not a
shorter global constant.

Request volume at the current proved set (20 instruments, measured): each refreshed about
twice a day, ~40 requests/day against a 20/minute dedicated budget.

⚠ **Inherited, not fixed here (#2946 finding 2):** `check_instrument_eligibility` posts
via `_http_write`. ⚠ Codex 26 corrects the first draft's description of that: it is a
per-`EtoroBrokerProvider`-instance shared timestamp, **not** a global order-write throttle,
so independent instances or processes can exceed the endpoint quota while each spaces
correctly. That is precisely #2946's finding and its lane map's job to fix; this job's
contribution to the pressure is ~40 requests/day.

## Shape

**One request per instrument, never batched.** The endpoint accepts 100 ids, but
`response_digest` digests the WHOLE response and its docstring states that is *"only sound
because a proof requests exactly one instrument."* Batching would need a digest rule
change; at 20 instruments there is nothing to buy with it.

**Request spacing** moves to `CORE_ELIGIBILITY_REQUEST_INTERVAL` in
`strategy_core_eligibility.py`, imported by both the job and
`prove_2603_core_eligibility.py`, which currently owns the only copy
(`REQUEST_INTERVAL_S = 3.2`). One definition, its portal citation beside it. ⚠ The job
sleeps **after every attempt, successful or not** (Codex 27) — a `continue` that skips the
sleep turns a failing endpoint into an unspaced retry storm.

**Service:** `app/services/strategy_core_eligibility_refresh.py`

- `select_proofs_to_revalidate(conn, *, operator_id, provider, environment, refresh_age, live_credential_ids, limit) -> tuple[StaleProof, ...]`
  — `DISTINCT ON (instrument_id)` over the triple, latest by
  `observed_at DESC, core_eligibility_proof_id DESC` (the same ordering as
  `load_latest_core_eligibility_proof` and `QUOTES_REFRESH_SCOPE_SQL`, so no third
  definition of "the latest proof" is created), filtered by the two-arm predicate above,
  **ordered oldest-first**, limited. Age comes from `now() - observed_at` computed in SQL.
  Returns instrument id, symbol, prior age, prior verdict and prior proof id — the job
  note reports verdict transitions, so an `underlying → not_underlying` flip is visible in
  `job_runs` without a query.
- `CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN = 100`.

⚠ **The cap is a per-run WORK bound and neither raises nor silently truncates** — both of
the first draft's options were wrong. Raising (Codex 23) makes >100 stale instruments a
permanent outage that never makes progress, and the 100-id figure is the endpoint's
**per-request** ceiling, which is no source rule at all for a job making singleton
requests (Codex 24). So: take the oldest 100, **log the number deferred to the next tick**
and carry it in `tracker.note`. Fixed by construction from the runtime budget —
100 × `REQUEST_INTERVAL` ≈ 5.3 min, comfortably inside an hourly tick — and stated as
that, not as a broker limit.

**Job:** `core_eligibility_refresh` in `app/workers/scheduler.py`, modelled on
`core_rebalance_observation` (lane `source="etoro"`, `catch_up_on_boot=False`,
`prerequisite=_bootstrap_complete`).

⚠⚠ **A `ScheduledJob` entry alone does not make a job run** (Codex 35). `JobRuntime.start`
skips any job absent from `app/jobs/runtime.py::_INVOKERS` with a bare `continue`. The
invoker registration is part of this change, and a test asserts the name is in both.

Prerequisite skips, each recorded from the body so manual dispatch reaches them:

1. `settings.etoro_env != "demo"` → skip. ⚠ The first draft justified this as "a real
   refresh has no consumer", which is **false** (Codex 40): quote scope reads proofs in
   any environment and `screen_portfolio_feasibility.py` accepts `--environment real`. The
   honest justification is narrower — the credentials this job loads are for
   `settings.etoro_env`, every other core-path job is demo-gated the same way, and
   scheduling unattended eligibility traffic against a real account is not this change's
   to decide.
2. selection empty → skip. ⚠ Two distinct causes with two distinct notes (Codex 34):
   *"no instrument has ever been proved"* versus *"all N proofs are fresh"*. The first
   draft emitted the first message for both, which would have made the dev-verify re-run
   report a false explanation.
3. credentials missing → skip.

⚠ The DB predicates run **before** `_load_etoro_credentials`, for the reason already
recorded on `core_rebalance_observation`: the credential load decrypts two secrets and
appends two audit rows, so ordering it after the cheap checks keeps every no-op tick out
of secrets storage.

### Transactions, locking and the executor

⚠⚠ **Connection posture is explicit** (Codex 3, 4): the job runs on an **autocommit**
connection, so `with conn.transaction():` around each record is a real transaction and not
a savepoint inside an accidental outer one. That is the trap `prove_2603_core_eligibility`
avoids by committing before the loop and the one `scheduler.py` already documents
elsewhere. Per-instrument commits are what keep `observed_at` distinct per row, and no
DB connection is held across an HTTP call.

⚠⚠ **Each record is written inside `core_submission_lock`** (Codex 8). Without it a proof
can commit between the executor's binding re-read (`:548`) and its trade INSERT, which is
the window the lock exists to close. Taken per instrument around one INSERT only — never
across the broker round-trip — so it cannot hold the three advisory keys for the length of
the batch.

⚠ **Residual, accepted and named** (Codex 7): the executor reads a proof at `:438`,
**outside** the lock, and requires the binding read at `:548` to return the *same
`proof_id`*. A refresh landing in that window makes the attended execution refuse with
`core_credential_provenance_changed`. This is not a new defect class — a hand-run `prove`
does the same, and the identity check was written knowing proofs can land concurrently —
but this job raises its frequency from "never" to ~2 write instants per day per
instrument. The outcome is a **specific fail-closed refusal** on an attended path, which
the operator retries past. Recorded on #2603 so it is triaged as known, not rediscovered.

### Failures

Per-instrument exceptions are **caught, counted, logged with the exception type, and the
loop continues**; the recorder is never reached for them.

⚠ `except Exception` here is deliberate and is the shape the prevention log permits: it
does **not** rename the failure into a domain vocabulary — nothing synthesises an
eligibility verdict from an exception (Codex 30 asks for the boundary to be explicit;
evaluation and digest errors are inside the same guard, DB errors inside the record
transaction abort that instrument only).

⚠ If **every** attempted instrument failed, the job **re-raises the last exception**, not
a fresh `RuntimeError` (Codex 31): `_tracked_job`'s classifier inspects the cause, and a
generic wrapper turns an actionable auth or rate-limit failure into `INTERNAL_ERROR`. A
job that no-ops and reports success is invisible to every automated check this repo has.

⚠ **A partial failure still reports success** (Codex 32), which is correct for the run and
wrong as a freshness signal — a permanently failing instrument expires while runs stay
green. `tracker.note` carries written / failed / deferred counts and the failing
instrument ids so the condition is legible in `job_runs`; a freshness alarm belongs with
the other ops-monitor staleness checks and is not built here.

⚠ `tracker.row_count` and `tracker.note` are set **as the loop progresses**, not only at
the end (Codex 33) — `_tracked_job`'s failure branch does not populate them, so a late DB
failure would otherwise hide proofs already committed.

## Tests

**Pure** (fast tier):

- `REFRESH_AGE + one registered tick < MAX_AGE`, reading the cadence from the registered
  `ScheduledJob`, not a literal.
- `REFRESH_AGE` is derived from `MAX_AGE`, not an independent literal.
- `core_eligibility_refresh` appears in **both** `SCHEDULED_JOBS` and `_INVOKERS`.
- the per-run cap defers rather than raising or truncating silently, and the deferred
  count is reported.
- `prove_2603_core_eligibility.REQUEST_INTERVAL_S` is the imported constant, not a second
  copy (retires the duplicate rather than leaving two).

**DB tier** (`-m db`, one file):

- boundary: a latest proof at exactly `REFRESH_AGE` is **not** selected, at
  `REFRESH_AGE + 1s` **is** (matching the `<=`-is-fresh direction
  `require_core_eligibility` already settled for `MAX_AGE`);
- a fresh latest proof with an older stale one behind it is **not** selected — the
  predicate is on the LATEST row, not on any row;
- tied `observed_at` → the higher proof id decides which row is read as latest;
- a different `environment` is not selected;
- a different `operator_id` is not selected;
- **rotation arm**: a *fresh* proof under superseded credential ids **is** selected;
- an instrument with no proof at all is absent — the non-widening invariant, asserted
  directly;
- ordering is oldest-first across a mixed-age set;
- the cap returns the oldest N and reports the remainder.

**Behavioural** (fake broker, no network):

- a transport exception on one instrument writes no row for it, is counted, and does not
  stop the others;
- an all-fail run re-raises the original exception type;
- a partial run sets `row_count` to the number actually written;
- `check_instrument_eligibility` is the only provider method the job calls — asserted, so
  the job cannot drift onto a mutating one.

## Dev-verify

⚠ **Manual dispatch does not prove scheduled operation** (Codex 41; the prevention log
records this deployment failure mode). The PR records all of:

1. the jobs daemon restarted onto the new main, and the registered next-fire time for
   `core_eligibility_refresh` read back from the runtime — not just a manual dispatch;
2. the selected set with each instrument's prior age and prior verdict, **population-wide**
   (every proved instrument, not the three #2833 candidates — Codex 37);
3. the proofs written with ids and verdicts, and any verdict transitions;
4. the `job_runs` row: status, `row_count`, note;
5. an immediate re-run showing zero selected **and** the "all N proofs are fresh" note —
   valid only because step 3 shows every selected instrument succeeded;
6. a before/after count of `quotes_refresh` arm-5 membership, since this run can move it
   in both directions.

⚠ **Acceptance is correct revalidation, not a favourable verdict** (Codex 38). Re-running
#2947's screen on #2833's candidates is recorded as evidence that the screen is no longer
answering `eligibility_proof_stale` **without a hand-run `prove`**; whether it then says
`feasible` is the broker's business and is not a pass condition here.

## Out of scope, stated so it is not read as covered

- Re-homing `check_instrument_eligibility` off `_http_write`, and the cross-instance quota
  exposure Codex 26 describes (#2946).
- Batched requests and the `response_digest` rule that would need.
- Closing the credential plaintext/id attribution race (a shared auth-path change).
- Closing the executor's `:438`→`:548` proof-identity window.
- A per-instrument freshness alarm, a withdrawal path for permanently enrolled
  instruments, and a circuit-break for endpoint-wide failure (Codex 29) — each a separate
  ticket if the population grows past the point where oldest-first ordering suffices.
- Any change to `require_core_eligibility`, the 24h bound, or the attended executor. This
  adds a producer; it weakens no gate.
- `environment='real'` proofs.

## Codex ckpt-1 corrections

| first-draft claim | verdict |
| --- | --- |
| quote-scope membership "can shrink or stay, never grow" | **false** — a `not_underlying → underlying` flip ADMITS an instrument; two sets were conflated |
| a non-passing verdict means the instrument "stopped qualifying" | **wrong treatment** — `unresolved` is not evidence, and `not_underlying` includes an open-permission reason, not only an ownership one |
| selector takes a caller-supplied `now` | **reversed** — `observed_at` is DB `now()` by deliberate design; age is computed in SQL |
| cap raises above 100 | **permanent outage** — defer oldest-first and log the remainder; 100 is a runtime bound, not the endpoint's per-request rule |
| empty selection means "no instrument has a proof" | **two causes** — "never proved" vs "all fresh"; one message would misreport the dev-verify re-run |
| credential rotation heals on the next tick | **it does not** under an age-only rule — added the credential-mismatch selection arm |
| a `ScheduledJob` entry is the registration | **incomplete** — `_INVOKERS` absence silently skips the job |
| cadence test on a literal `timedelta(hours=1)` | **cannot catch a cadence change** — read the registered cadence |
| "every consumer is failing" past 24h | **overbroad** — quote scope does not bound age; two age-bounded consumers do |
| a real-environment refresh "has no consumer" | **false** — quote scope and the #2947 screen both read real proofs |
| `_http_write` is the order-write throttle | **per-provider-instance shared timestamp** — cross-instance quota exposure stands (#2946) |
| all-fail raises a generic `RuntimeError` | **destroys failure classification** — re-raise the cause |
| proof writes need no lock | **they do** — `core_submission_lock` per record, or a proof lands between the binding read and the trade INSERT |
| dev-verify = manual dispatch on three candidates | **insufficient** — daemon restart, registered next-fire, population-wide before/after |
